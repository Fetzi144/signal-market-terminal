from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import websockets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import (
    get_active_pilot_config,
    get_open_pilot_run,
    record_control_plane_incident,
)
from app.execution.polymarket_gateway import GatewayUnavailableError, PolymarketGateway
from app.execution.polymarket_live_reconciler import PolymarketLiveReconciler
from app.execution.polymarket_live_state import (
    mark_user_stream_message,
    set_user_stream_connection_state,
)
from app.ingestion.polymarket_common import parse_polymarket_timestamp, utcnow
from app.metrics import polymarket_user_stream_reconnects
from app.models.polymarket_live_execution import LiveOrder, PolymarketUserEventRaw

logger = logging.getLogger(__name__)


async def _record_disconnect_incident(
    session: AsyncSession,
    *,
    reason: str,
) -> None:
    config = await get_active_pilot_config(session)
    if config is None:
        return
    run = await get_open_pilot_run(session, pilot_config_id=config.id)
    await record_control_plane_incident(
        session,
        severity="warning",
        incident_type="user_stream_disconnect",
        details={"reason": reason},
        pilot_run=run,
        strategy_family=config.strategy_family,
    )


class PolymarketUserStreamService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        gateway: PolymarketGateway | None = None,
        reconciler: PolymarketLiveReconciler | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway or PolymarketGateway()
        self._reconciler = reconciler or PolymarketLiveReconciler(gateway=self._gateway)

    async def close(self) -> None:
        return None

    async def active_condition_ids(self) -> list[str]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(LiveOrder.condition_id)
                    .where(
                        LiveOrder.status.not_in({"canceled", "rejected", "failed", "validation_failed", "confirmed"}),
                    )
                    .distinct()
                )
            ).scalars().all()
            return [str(row) for row in rows if row]

    async def append_raw_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
        stream_session_id: str | None,
    ) -> PolymarketUserEventRaw:
        row = PolymarketUserEventRaw(
            stream_session_id=stream_session_id,
            condition_id=str(payload.get("market") or payload.get("condition_id")) if payload.get("market") or payload.get("condition_id") else None,
            asset_id=str(payload.get("asset_id")) if payload.get("asset_id") is not None else None,
            event_type=str(payload.get("event_type") or payload.get("type") or "unknown"),
            event_ts_exchange=parse_polymarket_timestamp(payload.get("timestamp") or payload.get("matchtime")),
            recv_ts_local=utcnow(),
            source_payload_json=payload,
        )
        session.add(row)
        await session.flush()
        return row

    async def consume_message(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any] | list[Any],
        stream_session_id: str | None,
    ) -> dict[str, int]:
        fills_created = 0
        messages = payload if isinstance(payload, list) else [payload]
        for message in messages:
            if not isinstance(message, dict):
                continue
            raw_row = await self.append_raw_payload(session, payload=message, stream_session_id=stream_session_id)
            await mark_user_stream_message(
                session,
                session_id=stream_session_id,
                message_at=raw_row.event_ts_exchange or raw_row.recv_ts_local,
            )
            result = await self._reconciler.process_raw_user_event_row(session, row=raw_row)
            fills_created += result.get("fills_created", 0)
        return {"fills_created": fills_created}

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_user_stream_enabled:
            logger.info("Polymarket user stream disabled; skipping worker startup")
            return

        reconnect_delay = settings.polymarket_user_stream_reconnect_base_seconds
        while not stop_event.is_set():
            condition_ids = await self.active_condition_ids()
            if not condition_ids:
                await asyncio.sleep(1)
                continue

            stream_session_id = f"user-{int(utcnow().timestamp())}"
            try:
                subscribe_payload = self._gateway.user_stream_subscription_payload(condition_ids)
            except GatewayUnavailableError as exc:
                async with self._session_factory() as session:
                    await set_user_stream_connection_state(
                        session,
                        connected=False,
                        error=str(exc),
                    )
                    await _record_disconnect_incident(session, reason=str(exc))
                    await session.commit()
                await asyncio.sleep(reconnect_delay)
                continue

            try:
                async with websockets.connect(
                    settings.polymarket_user_stream_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=256,
                ) as websocket:
                    async with self._session_factory() as session:
                        await set_user_stream_connection_state(
                            session,
                            connected=True,
                            session_id=stream_session_id,
                            started_at=utcnow(),
                        )
                        await session.commit()
                    await websocket.send(json.dumps(subscribe_payload))
                    reconnect_delay = settings.polymarket_user_stream_reconnect_base_seconds

                    while not stop_event.is_set():
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        message = json.loads(raw_message)
                        async with self._session_factory() as session:
                            await self.consume_message(
                                session,
                                payload=message,
                                stream_session_id=stream_session_id,
                            )
                            await session.commit()
            except asyncio.TimeoutError:
                polymarket_user_stream_reconnects.inc()
                async with self._session_factory() as session:
                    await set_user_stream_connection_state(
                        session,
                        connected=False,
                        session_id=stream_session_id,
                        error="recv_timeout",
                    )
                    await _record_disconnect_incident(session, reason="recv_timeout")
                    await session.commit()
                reconnect_delay = min(
                    reconnect_delay * 2,
                    settings.polymarket_user_stream_reconnect_max_seconds,
                )
                await asyncio.sleep(reconnect_delay)
            except Exception as exc:
                polymarket_user_stream_reconnects.inc()
                async with self._session_factory() as session:
                    await set_user_stream_connection_state(
                        session,
                        connected=False,
                        session_id=stream_session_id,
                        error=str(exc),
                    )
                    await _record_disconnect_incident(session, reason=str(exc))
                    await session.commit()
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * 2,
                    settings.polymarket_user_stream_reconnect_max_seconds,
                )
