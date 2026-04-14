from __future__ import annotations

import hashlib
import json
import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_capital_reservation import PolymarketCapitalReservationService
from app.execution.polymarket_control_plane import is_restart_window_error, register_restart_pause
from app.execution.polymarket_gateway import GatewayUnavailableError, PolymarketGateway
from app.execution.polymarket_live_state import (
    LIVE_ORDER_TERMINAL_STATUSES,
    fetch_live_state_row,
    mark_reconcile_finished,
    mark_reconcile_started,
    set_gateway_status,
)
from app.ingestion.polymarket_common import parse_polymarket_timestamp, utcnow
from app.metrics import polymarket_live_fills_observed, polymarket_live_reconcile_failures, polymarket_live_reconcile_runs
from app.models.polymarket_live_execution import (
    LiveFill,
    LiveOrder,
    LiveOrderEvent,
    PolymarketUserEventRaw,
)

ZERO = Decimal("0")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


class PolymarketLiveReconciler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        *,
        gateway: PolymarketGateway | None = None,
        reservation_service: PolymarketCapitalReservationService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway or PolymarketGateway()
        self._reservations = reservation_service or PolymarketCapitalReservationService()

    async def run(self, stop_event: asyncio.Event) -> None:
        if self._session_factory is None:
            return
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(1, settings.polymarket_reconcile_interval_seconds))
            except asyncio.TimeoutError:
                async with self._session_factory() as session:
                    try:
                        await self.reconcile_once(session, reason="scheduled")
                        await session.commit()
                    except Exception:
                        await session.rollback()

    async def reconcile_once(
        self,
        session: AsyncSession,
        *,
        reason: str = "manual",
    ) -> dict[str, Any]:
        await mark_reconcile_started(session)
        polymarket_live_reconcile_runs.labels(reason=reason).inc()

        processed_raw_event_count = 0
        processed_fill_count = 0
        repaired_order_count = 0
        last_user_event_id = None

        try:
            state = await fetch_live_state_row(session)
            last_reconciled_id = state.last_reconciled_user_event_id if state is not None else None
            query = select(PolymarketUserEventRaw).order_by(PolymarketUserEventRaw.id.asc())
            if last_reconciled_id is not None:
                query = query.where(PolymarketUserEventRaw.id > last_reconciled_id)
            raw_rows = (await session.execute(query.limit(1000))).scalars().all()
            for row in raw_rows:
                counts = await self.process_raw_user_event_row(session, row=row)
                processed_raw_event_count += 1
                processed_fill_count += counts.get("fills_created", 0)
                last_user_event_id = row.id

            live_orders = (
                await session.execute(
                    select(LiveOrder).where(
                        LiveOrder.venue_order_id.is_not(None),
                        LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES),
                        LiveOrder.dry_run.is_(False),
                    )
                )
            ).scalars().all()

            for order in live_orders:
                try:
                    snapshot = await self._gateway.fetch_order_status(order.venue_order_id)
                    await set_gateway_status(session, reachable=True, error=None)
                    updated = await self._apply_order_snapshot(session, order=order, payload=snapshot)
                    repaired_order_count += 1 if updated else 0
                    trade_rows = await self._gateway.fetch_user_trades(limit=20, asset_id=order.asset_id)
                    for trade_payload in trade_rows:
                        result = await self.process_user_event_payload(
                            session,
                            payload=trade_payload,
                            raw_user_event_id=None,
                            source_kind="rest_reconcile",
                        )
                        processed_fill_count += result.get("fills_created", 0)
                except GatewayUnavailableError as exc:
                    await set_gateway_status(session, reachable=False, error=str(exc))
                except Exception as exc:
                    if is_restart_window_error(exc):
                        await register_restart_pause(session, error=str(exc), live_order=order)
                    await set_gateway_status(session, reachable=False, error=str(exc))

            await mark_reconcile_finished(
                session,
                success=True,
                last_user_event_id=last_user_event_id,
            )
            return {
                "status": "completed",
                "reason": reason,
                "processed_raw_event_count": processed_raw_event_count,
                "processed_fill_count": processed_fill_count,
                "repaired_order_count": repaired_order_count,
                "last_user_event_id": last_user_event_id,
            }
        except Exception as exc:
            polymarket_live_reconcile_failures.inc()
            await mark_reconcile_finished(session, success=False, error=str(exc), last_user_event_id=last_user_event_id)
            raise

    async def process_raw_user_event_row(
        self,
        session: AsyncSession,
        *,
        row: PolymarketUserEventRaw,
    ) -> dict[str, int]:
        payload = row.source_payload_json
        if not isinstance(payload, dict):
            return {"fills_created": 0}
        return await self.process_user_event_payload(
            session,
            payload=payload,
            raw_user_event_id=row.id,
            source_kind="user_ws",
        )

    async def process_user_event_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
        raw_user_event_id: int | None,
        source_kind: str,
    ) -> dict[str, int]:
        event_type = str(payload.get("event_type") or payload.get("type") or "").lower()
        if event_type == "order" or str(payload.get("type") or "").upper() in {"PLACEMENT", "UPDATE", "CANCELLATION"}:
            await self._apply_order_payload(
                session,
                payload=payload,
                raw_user_event_id=raw_user_event_id,
                source_kind=source_kind,
            )
            return {"fills_created": 0}
        if event_type == "trade":
            fills_created = await self._apply_trade_payload(
                session,
                payload=payload,
                raw_user_event_id=raw_user_event_id,
                source_kind=source_kind,
            )
            return {"fills_created": fills_created}
        return {"fills_created": 0}

    async def _apply_order_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
        raw_user_event_id: int | None,
        source_kind: str,
    ) -> None:
        order = await self._find_order_for_order_payload(session, payload=payload)
        if order is None:
            return

        venue_order_id = str(payload.get("id") or payload.get("order_id") or "")
        if venue_order_id and not order.venue_order_id:
            order.venue_order_id = venue_order_id

        original_size = _to_decimal(payload.get("original_size") or payload.get("size"))
        size_matched = _to_decimal(payload.get("size_matched")) or ZERO
        if original_size is not None and original_size > ZERO:
            order.submitted_size = original_size

        venue_message_type = str(payload.get("type") or "").upper()
        venue_status = str(payload.get("status") or "").lower()
        new_status = "live"
        if venue_status in {"rejected"}:
            new_status = "rejected"
        elif venue_status in {"expired"}:
            new_status = "expired"
        elif venue_status in {"cancelled", "canceled"} or venue_message_type == "CANCELLATION":
            new_status = "canceled"
        elif original_size is not None and size_matched >= original_size > ZERO:
            new_status = "matched"
        elif size_matched > ZERO:
            new_status = "partially_filled"

        await self._record_order_event(
            session,
            order=order,
            raw_user_event_id=raw_user_event_id,
            source_kind=source_kind,
            event_type=f"order_{venue_message_type.lower() or 'update'}",
            venue_status=venue_status or None,
            new_status=new_status,
            payload=payload,
        )

        if new_status == "canceled":
            await self._reservations.release_on_cancel(
                session,
                order=order,
                details={"source_kind": source_kind, "raw_user_event_id": raw_user_event_id},
            )

    async def _apply_trade_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
        raw_user_event_id: int | None,
        source_kind: str,
    ) -> int:
        order = await self._find_order_for_trade_payload(session, payload=payload)
        order_id = order.id if order is not None else None
        trade_status = self._map_trade_status(payload.get("status"))
        trade_id = str(payload.get("id") or payload.get("trade_id") or "")
        transaction_hash = (
            str(payload.get("transaction_hash"))
            if payload.get("transaction_hash") is not None
            else None
        )
        fingerprint_payload = {
            "trade_id": trade_id or None,
            "status": trade_status,
            "order_id": str(order_id) if order_id is not None else None,
            "transaction_hash": transaction_hash,
            "asset_id": str(payload.get("asset_id") or ""),
            "condition_id": str(payload.get("market") or payload.get("condition_id") or ""),
            "price": str(_to_decimal(payload.get("price")) or ZERO),
            "size": str(_to_decimal(payload.get("size")) or ZERO),
            "side": str(payload.get("side") or "BUY").upper(),
        }
        if not trade_id and transaction_hash is None:
            fingerprint_payload["raw_user_event_id"] = raw_user_event_id
        fingerprint = _stable_hash(
            fingerprint_payload
        )
        existing = (
            await session.execute(select(LiveFill).where(LiveFill.fingerprint == fingerprint))
        ).scalar_one_or_none()
        if existing is not None:
            return 0

        fill = LiveFill(
            live_order_id=order_id,
            condition_id=str(payload.get("market") or payload.get("condition_id") or ""),
            asset_id=str(payload.get("asset_id") or ""),
            trade_id=trade_id or None,
            transaction_hash=transaction_hash,
            fill_status=trade_status,
            side=str(payload.get("side") or "BUY").upper(),
            price=_to_decimal(payload.get("price")) or ZERO,
            size=_to_decimal(payload.get("size")) or ZERO,
            fee_paid=_to_decimal(payload.get("fee_paid") or payload.get("fee")),
            fee_currency=str(payload.get("fee_currency")) if payload.get("fee_currency") is not None else None,
            maker_taker="maker" if self._is_maker_fill(payload) else "taker",
            event_ts_exchange=parse_polymarket_timestamp(payload.get("timestamp") or payload.get("matchtime")),
            raw_user_event_id=raw_user_event_id,
            details_json=_json_safe(payload),
            fingerprint=fingerprint,
        )
        session.add(fill)
        await session.flush()
        polymarket_live_fills_observed.labels(fill_status=trade_status).inc()

        if order is not None:
            previous_filled = order.filled_size or ZERO
            await self._refresh_order_fill_summary(session, order=order)
            fill_delta = max((order.filled_size or ZERO) - previous_filled, ZERO)
            if fill_delta > ZERO:
                await self._reservations.apply_fill_update(
                    session,
                    order=order,
                    fill_size_delta=fill_delta,
                    fill_price=fill.price,
                    details={"source_kind": source_kind, "trade_id": fill.trade_id},
                )
            await self._record_order_event(
                session,
                order=order,
                raw_user_event_id=raw_user_event_id,
                source_kind=source_kind,
                event_type=f"trade_{trade_status}",
                venue_status=str(payload.get("status")) if payload.get("status") is not None else None,
                new_status=self._order_status_after_fill(order=order, fill_status=trade_status),
                payload=payload,
            )
        return 1

    async def _apply_order_snapshot(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        payload: dict[str, Any],
    ) -> bool:
        venue_status = str(payload.get("status") or payload.get("order_status") or "").lower()
        if payload.get("order_id") is not None and not order.venue_order_id:
            order.venue_order_id = str(payload.get("order_id"))
        size_matched = _to_decimal(payload.get("size_matched") or payload.get("filled_size"))
        original_size = _to_decimal(payload.get("original_size") or payload.get("size"))
        if original_size is not None and original_size > ZERO:
            order.submitted_size = original_size
        if size_matched is not None:
            order.filled_size = size_matched
        mapped_status = self._map_order_snapshot_status(venue_status=venue_status, order=order)
        await self._record_order_event(
            session,
            order=order,
            raw_user_event_id=None,
            source_kind="rest_reconcile",
            event_type="order_snapshot",
            venue_status=venue_status or None,
            new_status=mapped_status,
            payload=payload,
        )
        if mapped_status == "canceled":
            await self._reservations.release_on_cancel(
                session,
                order=order,
                details={"source_kind": "rest_reconcile", "snapshot": True},
                source_kind="reconcile",
            )
        return True

    async def _find_order_for_order_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
    ) -> LiveOrder | None:
        venue_order_id = payload.get("id") or payload.get("order_id")
        if venue_order_id is not None:
            row = (
                await session.execute(
                    select(LiveOrder).where(LiveOrder.venue_order_id == str(venue_order_id))
                )
            ).scalar_one_or_none()
            if row is not None:
                return row
        asset_id = payload.get("asset_id")
        condition_id = payload.get("market") or payload.get("condition_id")
        if asset_id is None or condition_id is None:
            return None
        rows = (
            await session.execute(
                select(LiveOrder)
                .where(
                    LiveOrder.asset_id == str(asset_id),
                    LiveOrder.condition_id == str(condition_id),
                    LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES),
                )
                .order_by(LiveOrder.created_at.desc())
                .limit(2)
            )
        ).scalars().all()
        return rows[0] if len(rows) == 1 else None

    async def _find_order_for_trade_payload(
        self,
        session: AsyncSession,
        *,
        payload: dict[str, Any],
    ) -> LiveOrder | None:
        order_ids: list[str] = []
        if payload.get("taker_order_id") is not None:
            order_ids.append(str(payload["taker_order_id"]))
        maker_orders = payload.get("maker_orders")
        if isinstance(maker_orders, list):
            for maker_order in maker_orders:
                if isinstance(maker_order, dict) and maker_order.get("order_id") is not None:
                    order_ids.append(str(maker_order["order_id"]))
        for venue_order_id in order_ids:
            row = (
                await session.execute(
                    select(LiveOrder).where(LiveOrder.venue_order_id == venue_order_id)
                )
            ).scalar_one_or_none()
            if row is not None:
                return row
        asset_id = payload.get("asset_id")
        condition_id = payload.get("market") or payload.get("condition_id")
        if asset_id is None or condition_id is None:
            return None
        rows = (
            await session.execute(
                select(LiveOrder)
                .where(
                    LiveOrder.asset_id == str(asset_id),
                    LiveOrder.condition_id == str(condition_id),
                    LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES),
                )
                .order_by(LiveOrder.created_at.desc())
                .limit(2)
            )
        ).scalars().all()
        return rows[0] if len(rows) == 1 else None

    async def _refresh_order_fill_summary(self, session: AsyncSession, *, order: LiveOrder) -> None:
        rows = (
            await session.execute(
                select(LiveFill)
                .where(LiveFill.live_order_id == order.id)
                .order_by(LiveFill.observed_at_local.desc(), LiveFill.id.desc())
            )
        ).scalars().all()
        latest_by_trade: dict[str, LiveFill] = {}
        for row in rows:
            key = row.trade_id or row.transaction_hash or row.fingerprint
            if key not in latest_by_trade:
                latest_by_trade[key] = row
        active_rows = [
            row for row in latest_by_trade.values() if row.fill_status in {"matched", "mined", "confirmed"}
        ]
        total_size = sum((row.size for row in active_rows), ZERO)
        weighted_notional = sum((row.size * row.price for row in active_rows), ZERO)
        order.filled_size = total_size
        order.avg_fill_price = (weighted_notional / total_size) if total_size > ZERO else None

    async def _record_order_event(
        self,
        session: AsyncSession,
        *,
        order: LiveOrder,
        raw_user_event_id: int | None,
        source_kind: str,
        event_type: str,
        venue_status: str | None,
        new_status: str | None,
        payload: dict[str, Any],
    ) -> LiveOrderEvent:
        fingerprint = _stable_hash(
            {
                "live_order_id": str(order.id),
                "raw_user_event_id": raw_user_event_id,
                "source_kind": source_kind,
                "event_type": event_type,
                "venue_status": venue_status,
                "payload": payload,
            }
        )
        existing = (
            await session.execute(select(LiveOrderEvent).where(LiveOrderEvent.fingerprint == fingerprint))
        ).scalar_one_or_none()
        if existing is not None:
            return existing

        observed_at = utcnow()
        if new_status is not None:
            order.status = new_status
        order.last_event_at = observed_at
        if new_status in LIVE_ORDER_TERMINAL_STATUSES:
            order.completed_at = observed_at

        row = LiveOrderEvent(
            live_order_id=order.id,
            raw_user_event_id=raw_user_event_id,
            source_kind=source_kind,
            event_type=event_type,
            venue_status=venue_status,
            event_ts_exchange=parse_polymarket_timestamp(payload.get("timestamp") or payload.get("matchtime")),
            observed_at_local=observed_at,
            payload_json=_json_safe(payload),
            details_json=None,
            fingerprint=fingerprint,
        )
        session.add(row)
        await session.flush()
        return row

    def _map_trade_status(self, value: Any) -> str:
        normalized = str(value or "matched").lower()
        if normalized in {"matched", "mined", "confirmed", "failed"}:
            return normalized
        if normalized in {"retrying"}:
            return "matched"
        return "matched"

    def _map_order_snapshot_status(self, *, venue_status: str, order: LiveOrder) -> str:
        if venue_status in {"cancelled", "canceled"}:
            return "canceled"
        if venue_status in {"rejected"}:
            return "rejected"
        if venue_status in {"expired"}:
            return "expired"
        if order.submitted_size is not None and order.filled_size >= order.submitted_size > ZERO:
            return "matched"
        if order.filled_size > ZERO:
            return "partially_filled"
        return "live"

    def _order_status_after_fill(self, *, order: LiveOrder, fill_status: str) -> str:
        target_size = order.submitted_size or order.requested_size
        if target_size is not None and target_size > ZERO and order.filled_size >= target_size:
            if fill_status == "confirmed":
                return "confirmed"
            if fill_status == "mined":
                return "mined"
            return "matched"
        return "partially_filled"

    def _is_maker_fill(self, payload: dict[str, Any]) -> bool:
        maker_orders = payload.get("maker_orders")
        if isinstance(maker_orders, list):
            return len(maker_orders) > 0
        return False
