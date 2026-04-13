from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Select, and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_common import parse_polymarket_timestamp, utcnow
from app.ingestion.polymarket_raw_storage import PolymarketRawStorageService
from app.ingestion.polymarket_stream import (
    PolymarketResyncService,
    list_watched_polymarket_assets,
)
from app.metrics import (
    polymarket_book_recon_assets_degraded,
    polymarket_book_recon_auto_resync_runs,
    polymarket_book_recon_drift_incidents,
    polymarket_book_recon_last_successful_resync_timestamp,
    polymarket_book_recon_live_books,
    polymarket_book_recon_manual_resync_runs,
    polymarket_book_recon_rows_applied,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketParamHistory
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketTradeTape,
)
from app.models.polymarket_reconstruction import (
    PolymarketBookReconIncident,
    PolymarketBookReconState,
)
from app.models.polymarket_stream import PolymarketIngestIncident

logger = logging.getLogger(__name__)

RECON_POLL_SECONDS = 5
SNAPSHOT_SOURCE_PRIORITY = {
    "ws_book": 0,
    "rest_resync_snapshot": 1,
    "rest_periodic_snapshot": 2,
    "rest_seed_snapshot": 3,
    "rest_manual_snapshot": 4,
}
AUTHORITATIVE_SNAPSHOT_KINDS = tuple(SNAPSHOT_SOURCE_PRIORITY.keys())
DRIFT_INCIDENT_TYPES = {
    "missing_seed",
    "delta_without_seed",
    "bbo_mismatch",
    "stale_book",
    "stream_gap_suspected",
    "reconnect_reseed",
    "invalid_delta",
    "replay_error",
}
STREAM_INCIDENT_MAP = {
    "gap_suspected": "stream_gap_suspected",
    "reconnect": "reconnect_reseed",
}
DEGRADED_STATUSES = ("drifted", "resyncing", "stale", "error")


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coalesce_time(*values: datetime | None) -> datetime | None:
    for value in values:
        if value is not None:
            return _normalize_datetime(value)
    return None


def _book_levels_from_json(levels: Any) -> dict[Decimal, Decimal]:
    book: dict[Decimal, Decimal] = {}
    if not isinstance(levels, list):
        return book
    for level in levels:
        price: Decimal | None = None
        size: Decimal | None = None
        if isinstance(level, dict):
            price = _to_decimal(level.get("price"))
            size = _to_decimal(level.get("size"))
        elif isinstance(level, (list, tuple)) and level:
            price = _to_decimal(level[0])
            size = _to_decimal(level[1]) if len(level) > 1 else None
        if price is None or size is None or size <= 0:
            continue
        book[price] = size
    return book


def _best_bid(book: dict[Decimal, Decimal]) -> Decimal | None:
    return max(book) if book else None


def _best_ask(book: dict[Decimal, Decimal]) -> Decimal | None:
    return min(book) if book else None


def _spread(best_bid: Decimal | None, best_ask: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None:
        return None
    return best_ask - best_bid


def _prices_equal(expected: Decimal | None, observed: Decimal | None, tolerance: Decimal) -> bool:
    if expected is None and observed is None:
        return True
    if expected is None or observed is None:
        return False
    return abs(expected - observed) <= tolerance


def _incident_targets_asset(incident: PolymarketIngestIncident, asset_id: str) -> bool:
    if incident.asset_id == asset_id:
        return True
    details = incident.details_json or {}
    asset_ids = details.get("asset_ids")
    if isinstance(asset_ids, list):
        return asset_id in {str(value) for value in asset_ids if value is not None}
    return incident.incident_type == "gap_suspected" and incident.asset_id is None


@dataclass(slots=True)
class AssetContext:
    asset_id: str
    condition_id: str
    market_dim_id: int | None
    asset_dim_id: int | None


@dataclass(slots=True)
class InMemoryBook:
    asset_id: str
    condition_id: str
    market_dim_id: int | None
    asset_dim_id: int | None
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    expected_tick_size: Decimal | None = None
    last_snapshot_id: int | None = None
    last_snapshot_source_kind: str | None = None
    last_snapshot_hash: str | None = None
    last_snapshot_exchange_ts: datetime | None = None
    last_exchange_ts: datetime | None = None
    last_received_at_local: datetime | None = None

    def top_of_book(self) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        best_bid = _best_bid(self.bids)
        best_ask = _best_ask(self.asks)
        return best_bid, best_ask, _spread(best_bid, best_ask)


def _serialize_recon_state(row: PolymarketBookReconState) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "status": row.status,
        "last_snapshot_id": row.last_snapshot_id,
        "last_snapshot_source_kind": row.last_snapshot_source_kind,
        "last_snapshot_hash": row.last_snapshot_hash,
        "last_snapshot_exchange_ts": row.last_snapshot_exchange_ts,
        "last_applied_raw_event_id": row.last_applied_raw_event_id,
        "last_applied_delta_raw_event_id": row.last_applied_delta_raw_event_id,
        "last_applied_delta_index": row.last_applied_delta_index,
        "last_bbo_raw_event_id": row.last_bbo_raw_event_id,
        "last_trade_raw_event_id": row.last_trade_raw_event_id,
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "spread": _serialize_decimal(row.spread),
        "depth_levels_bid": row.depth_levels_bid,
        "depth_levels_ask": row.depth_levels_ask,
        "expected_tick_size": _serialize_decimal(row.expected_tick_size),
        "last_exchange_ts": row.last_exchange_ts,
        "last_received_at_local": row.last_received_at_local,
        "last_reconciled_at": row.last_reconciled_at,
        "last_resynced_at": row.last_resynced_at,
        "drift_count": row.drift_count,
        "resync_count": row.resync_count,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_recon_incident(row: PolymarketBookReconIncident) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "incident_type": row.incident_type,
        "severity": row.severity,
        "raw_event_id": row.raw_event_id,
        "snapshot_id": row.snapshot_id,
        "capture_run_id": row.capture_run_id,
        "exchange_ts": row.exchange_ts,
        "observed_at_local": row.observed_at_local,
        "expected_best_bid": _serialize_decimal(row.expected_best_bid),
        "observed_best_bid": _serialize_decimal(row.observed_best_bid),
        "expected_best_ask": _serialize_decimal(row.expected_best_ask),
        "observed_best_ask": _serialize_decimal(row.observed_best_ask),
        "expected_hash": row.expected_hash,
        "observed_hash": row.observed_hash,
        "details_json": row.details_json,
        "created_at": row.created_at,
    }


class PolymarketBookReconstructionService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        resync_service: PolymarketResyncService | None = None,
        raw_storage_service: PolymarketRawStorageService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._resync_service = resync_service or PolymarketResyncService(session_factory)
        self._raw_storage_service = raw_storage_service or PolymarketRawStorageService(session_factory)
        self._books: dict[str, InMemoryBook] = {}
        self._bbo_tolerance = Decimal(str(settings.polymarket_book_recon_bbo_tolerance))

    async def close(self) -> None:
        await self._resync_service.close()
        await self._raw_storage_service.close()

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_book_recon_enabled:
            logger.info("Polymarket book reconstruction disabled; skipping worker startup")
            return

        if settings.polymarket_book_recon_on_startup:
            try:
                await self.sync_scope(reason="startup", trigger_resync_for_missing_seed=True)
            except Exception:
                logger.warning("Polymarket book reconstruction startup sequence failed", exc_info=True)

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=RECON_POLL_SECONDS)
                continue
            except asyncio.TimeoutError:
                pass

            try:
                await self.sync_scope(reason="scheduled")
            except Exception:
                logger.warning("Polymarket book reconstruction catch-up failed", exc_info=True)

    async def sync_scope(
        self,
        *,
        asset_ids: list[str] | None = None,
        reason: str,
        trigger_resync_for_missing_seed: bool = False,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            target_asset_ids = await self._resolve_scope(session, asset_ids)
        live_count = 0
        degraded_count = 0
        results: list[dict[str, Any]] = []
        for asset_id in target_asset_ids:
            result = await self.sync_asset(
                asset_id,
                reason=reason,
                allow_auto_resync=settings.polymarket_book_recon_auto_resync_enabled,
                force_resync_for_missing_seed=trigger_resync_for_missing_seed,
            )
            results.append(result)
            if result["status"] == "live":
                live_count += 1
            elif result["status"] in DEGRADED_STATUSES:
                degraded_count += 1
        return {
            "asset_ids": target_asset_ids,
            "asset_count": len(target_asset_ids),
            "live_count": live_count,
            "degraded_count": degraded_count,
            "results": results,
        }

    async def sync_asset(
        self,
        asset_id: str,
        *,
        reason: str,
        allow_auto_resync: bool,
        force_resync_for_missing_seed: bool = False,
    ) -> dict[str, Any]:
        normalized_asset_id = str(asset_id)
        outcome: dict[str, Any] = {
            "asset_id": normalized_asset_id,
            "condition_id": None,
            "status": "unseeded",
            "resynced": False,
        }
        for _attempt in range(2):
            pending_resync: dict[str, Any] | None = None
            async with self._session_factory() as session:
                result = await self._sync_asset_once(
                    session,
                    normalized_asset_id,
                    reason=reason,
                    allow_auto_resync=allow_auto_resync,
                    force_resync_for_missing_seed=force_resync_for_missing_seed,
                )
                pending_resync = result.get("pending_resync")
                await session.commit()
            outcome = result
            if pending_resync is not None:
                await self._trigger_resync(
                    pending_resync["asset_ids"],
                    reason=pending_resync["reason"],
                    manual=bool(pending_resync.get("manual", False)),
                )
                outcome["resynced"] = True
            if not result.get("retry"):
                break
        return outcome

    async def manual_resync(
        self,
        *,
        asset_ids: list[str] | None,
        reason: str,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            target_asset_ids = await self._resolve_scope(session, asset_ids)
        resync_result = await self._trigger_resync(target_asset_ids, reason=reason, manual=True)
        catch_up = await self.sync_scope(asset_ids=target_asset_ids, reason="manual_resync_followup")
        resync_result["reconstruction"] = catch_up
        return resync_result

    async def _resolve_scope(self, session: AsyncSession, asset_ids: list[str] | None) -> list[str]:
        target_asset_ids = [str(value) for value in (asset_ids or []) if value]
        if not target_asset_ids:
            target_asset_ids = await list_watched_polymarket_assets(session)
        max_assets = max(1, settings.polymarket_book_recon_max_watched_assets)
        return target_asset_ids[:max_assets]

    async def _sync_asset_once(
        self,
        session: AsyncSession,
        asset_id: str,
        *,
        reason: str,
        allow_auto_resync: bool,
        force_resync_for_missing_seed: bool,
    ) -> dict[str, Any]:
        state = await self._get_or_create_state(session, asset_id)
        context = await self._resolve_asset_context(session, asset_id, fallback_condition_id=state.condition_id)
        state.market_dim_id = context.market_dim_id
        state.asset_dim_id = context.asset_dim_id
        state.condition_id = context.condition_id
        runtime = self._books.get(asset_id)
        details = dict(state.details_json or {})
        state.details_json = details

        stream_signal = await self._process_stream_incidents(
            session,
            asset_id=asset_id,
            state=state,
            allow_auto_resync=allow_auto_resync,
        )
        if stream_signal.get("retry"):
            return {
                "asset_id": asset_id,
                "condition_id": state.condition_id,
                "status": state.status,
                "retry": True,
                "pending_resync": stream_signal.get("pending_resync"),
            }
        reseed_required = stream_signal.get("reseed", False)

        snapshot = await self._select_authoritative_snapshot(session, asset_id=asset_id)
        if snapshot is None:
            if force_resync_for_missing_seed or allow_auto_resync:
                await self._mark_incident(
                    session,
                    state=state,
                    incident_type="missing_seed",
                    severity="warning",
                    details_json={"reason": reason},
                    increment_drift=True,
                    status="unseeded",
                )
                if force_resync_for_missing_seed or await self._can_resync(state):
                    state.status = "resyncing"
                    return {
                        "asset_id": asset_id,
                        "condition_id": state.condition_id,
                        "status": state.status,
                        "retry": True,
                        "pending_resync": {
                            "asset_ids": [asset_id],
                            "reason": "missing_seed",
                            "manual": False,
                        },
                    }
            self._books.pop(asset_id, None)
            return {
                "asset_id": asset_id,
                "condition_id": state.condition_id,
                "status": state.status,
                "retry": False,
            }

        if runtime is None or reseed_required or state.last_snapshot_id != snapshot.id:
            runtime = await self._seed_from_snapshot(session, state=state, snapshot=snapshot)
            self._books[asset_id] = runtime

        delta_result = await self._apply_pending_deltas(
            session,
            state=state,
            runtime=runtime,
            snapshot=snapshot,
            allow_auto_resync=allow_auto_resync,
        )
        if delta_result.get("retry"):
            return {
                "asset_id": asset_id,
                "condition_id": state.condition_id,
                "status": state.status,
                "retry": True,
                "pending_resync": delta_result.get("pending_resync"),
            }

        bbo_result = await self._reconcile_bbo(
            session,
            state=state,
            runtime=runtime,
            snapshot=snapshot,
            allow_auto_resync=allow_auto_resync,
        )
        if bbo_result.get("retry"):
            return {
                "asset_id": asset_id,
                "condition_id": state.condition_id,
                "status": state.status,
                "retry": True,
                "pending_resync": bbo_result.get("pending_resync"),
            }

        await self._advance_trade_cursor(session, state=state, snapshot=snapshot)
        await self._check_staleness(session, state=state)
        self._persist_runtime_to_state(state, runtime)
        self._books[asset_id] = runtime
        return {
            "asset_id": asset_id,
            "condition_id": state.condition_id,
            "status": state.status,
            "best_bid": _serialize_decimal(state.best_bid),
            "best_ask": _serialize_decimal(state.best_ask),
            "retry": False,
        }

    async def _resolve_asset_context(
        self,
        session: AsyncSession,
        asset_id: str,
        *,
        fallback_condition_id: str,
    ) -> AssetContext:
        row = (
            await session.execute(
                select(PolymarketAssetDim)
                .where(PolymarketAssetDim.asset_id == asset_id)
                .limit(1)
            )
        ).scalar_one_or_none()
        if row is not None:
            return AssetContext(
                asset_id=row.asset_id,
                condition_id=row.condition_id,
                market_dim_id=row.market_dim_id,
                asset_dim_id=row.id,
            )
        latest_snapshot = (
            await session.execute(
                select(PolymarketBookSnapshot)
                .where(PolymarketBookSnapshot.asset_id == asset_id)
                .order_by(PolymarketBookSnapshot.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if latest_snapshot is not None:
            return AssetContext(
                asset_id=asset_id,
                condition_id=latest_snapshot.condition_id,
                market_dim_id=latest_snapshot.market_dim_id,
                asset_dim_id=latest_snapshot.asset_dim_id,
            )
        return AssetContext(
            asset_id=asset_id,
            condition_id=fallback_condition_id or asset_id,
            market_dim_id=None,
            asset_dim_id=None,
        )

    async def _get_or_create_state(self, session: AsyncSession, asset_id: str) -> PolymarketBookReconState:
        row = (
            await session.execute(
                select(PolymarketBookReconState)
                .where(PolymarketBookReconState.asset_id == asset_id)
                .limit(1)
            )
        ).scalar_one_or_none()
        if row is not None:
            return row
        row = PolymarketBookReconState(
            asset_id=asset_id,
            condition_id=asset_id,
            status="unseeded",
            details_json={},
        )
        session.add(row)
        await session.flush()
        return row

    async def _select_authoritative_snapshot(
        self,
        session: AsyncSession,
        *,
        asset_id: str,
    ) -> PolymarketBookSnapshot | None:
        boundary = utcnow() - timedelta(hours=settings.polymarket_book_recon_bootstrap_lookback_hours)
        effective_time = func.coalesce(PolymarketBookSnapshot.event_ts_exchange, PolymarketBookSnapshot.observed_at_local)
        source_rank = case(
            *[(PolymarketBookSnapshot.source_kind == kind, rank) for kind, rank in SNAPSHOT_SOURCE_PRIORITY.items()],
            else_=100,
        )
        return (
            await session.execute(
                select(PolymarketBookSnapshot)
                .where(
                    PolymarketBookSnapshot.asset_id == asset_id,
                    PolymarketBookSnapshot.source_kind.in_(AUTHORITATIVE_SNAPSHOT_KINDS),
                    effective_time >= boundary,
                )
                .order_by(effective_time.desc(), source_rank.asc(), PolymarketBookSnapshot.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _latest_tick_size(
        self,
        session: AsyncSession,
        *,
        asset_id: str,
        condition_id: str,
    ) -> Decimal | None:
        row = (
            await session.execute(
                select(PolymarketMarketParamHistory.tick_size)
                .where(
                    PolymarketMarketParamHistory.condition_id == condition_id,
                    or_(
                        PolymarketMarketParamHistory.asset_id == asset_id,
                        PolymarketMarketParamHistory.asset_id.is_(None),
                    ),
                    PolymarketMarketParamHistory.tick_size.is_not(None),
                )
                .order_by(
                    PolymarketMarketParamHistory.asset_id.is_(None),
                    PolymarketMarketParamHistory.observed_at_local.desc(),
                    PolymarketMarketParamHistory.id.desc(),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        return row

    async def _seed_from_snapshot(
        self,
        session: AsyncSession,
        *,
        state: PolymarketBookReconState,
        snapshot: PolymarketBookSnapshot,
    ) -> InMemoryBook:
        expected_tick_size = snapshot.tick_size or await self._latest_tick_size(
            session,
            asset_id=snapshot.asset_id,
            condition_id=snapshot.condition_id,
        )
        runtime = InMemoryBook(
            asset_id=snapshot.asset_id,
            condition_id=snapshot.condition_id,
            market_dim_id=snapshot.market_dim_id,
            asset_dim_id=snapshot.asset_dim_id,
            bids=_book_levels_from_json(snapshot.bids_json),
            asks=_book_levels_from_json(snapshot.asks_json),
            expected_tick_size=expected_tick_size,
            last_snapshot_id=snapshot.id,
            last_snapshot_source_kind=snapshot.source_kind,
            last_snapshot_hash=snapshot.book_hash,
            last_snapshot_exchange_ts=snapshot.event_ts_exchange,
            last_exchange_ts=_coalesce_time(snapshot.event_ts_exchange, snapshot.observed_at_local),
            last_received_at_local=_coalesce_time(snapshot.recv_ts_local, snapshot.observed_at_local),
        )
        best_bid, best_ask, spread = runtime.top_of_book()
        state.last_snapshot_id = snapshot.id
        state.last_snapshot_source_kind = snapshot.source_kind
        state.last_snapshot_hash = snapshot.book_hash
        state.last_snapshot_exchange_ts = snapshot.event_ts_exchange
        state.last_applied_raw_event_id = snapshot.raw_event_id
        state.last_applied_delta_raw_event_id = None
        state.last_applied_delta_index = None
        state.last_bbo_raw_event_id = None
        state.best_bid = best_bid
        state.best_ask = best_ask
        state.spread = spread
        state.depth_levels_bid = len(runtime.bids)
        state.depth_levels_ask = len(runtime.asks)
        state.expected_tick_size = expected_tick_size
        state.last_exchange_ts = runtime.last_exchange_ts
        state.last_received_at_local = runtime.last_received_at_local
        state.last_reconciled_at = utcnow()
        state.status = "live"
        polymarket_book_recon_rows_applied.labels(kind="seed").inc()
        return runtime

    def _delta_query(
        self,
        *,
        asset_id: str,
        state: PolymarketBookReconState,
        snapshot: PolymarketBookSnapshot,
    ) -> Select[Any]:
        query: Select[Any] = select(PolymarketBookDelta).where(PolymarketBookDelta.asset_id == asset_id)
        if state.last_applied_delta_raw_event_id is not None:
            last_delta_index = state.last_applied_delta_index or -1
            query = query.where(
                or_(
                    PolymarketBookDelta.raw_event_id > state.last_applied_delta_raw_event_id,
                    and_(
                        PolymarketBookDelta.raw_event_id == state.last_applied_delta_raw_event_id,
                        PolymarketBookDelta.delta_index > last_delta_index,
                    ),
                )
            )
        elif snapshot.raw_event_id is not None:
            query = query.where(PolymarketBookDelta.raw_event_id > snapshot.raw_event_id)
        else:
            boundary = _coalesce_time(snapshot.observed_at_local, snapshot.event_ts_exchange)
            if boundary is not None:
                query = query.where(
                    func.coalesce(
                        PolymarketBookDelta.event_ts_exchange,
                        PolymarketBookDelta.recv_ts_local,
                        PolymarketBookDelta.ingest_ts_db,
                    )
                    > boundary
                )
        return query.order_by(
            PolymarketBookDelta.raw_event_id.asc(),
            PolymarketBookDelta.delta_index.asc(),
            PolymarketBookDelta.id.asc(),
        )

    async def _apply_pending_deltas(
        self,
        session: AsyncSession,
        *,
        state: PolymarketBookReconState,
        runtime: InMemoryBook,
        snapshot: PolymarketBookSnapshot,
        allow_auto_resync: bool,
    ) -> dict[str, Any]:
        rows = (
            await session.execute(self._delta_query(asset_id=runtime.asset_id, state=state, snapshot=snapshot))
        ).scalars().all()
        for row in rows:
            target_book = None
            if row.side in {"BUY", "BID"}:
                target_book = runtime.bids
            elif row.side in {"SELL", "ASK"}:
                target_book = runtime.asks
            if target_book is None:
                await self._mark_incident(
                    session,
                    state=state,
                    incident_type="invalid_delta",
                    severity="error",
                    raw_event_id=row.raw_event_id,
                    details_json={"side": row.side, "delta_id": row.id},
                    increment_drift=True,
                    status="drifted",
                )
                pending_resync = await self._auto_resync_after_drift(
                    state,
                    [runtime.asset_id],
                    reason="invalid_delta",
                    allow_auto_resync=allow_auto_resync,
                )
                if pending_resync is not None:
                    return {"retry": True, "pending_resync": pending_resync}
                return {"retry": False}

            if row.size <= 0:
                target_book.pop(row.price, None)
            else:
                target_book[row.price] = row.size

            runtime.last_exchange_ts = _coalesce_time(row.event_ts_exchange, runtime.last_exchange_ts)
            runtime.last_received_at_local = _coalesce_time(row.recv_ts_local, runtime.last_received_at_local)
            state.last_applied_delta_raw_event_id = row.raw_event_id
            state.last_applied_delta_index = row.delta_index
            state.last_applied_raw_event_id = row.raw_event_id
            polymarket_book_recon_rows_applied.labels(kind="delta").inc()

        return {"retry": False}

    async def _reconcile_bbo(
        self,
        session: AsyncSession,
        *,
        state: PolymarketBookReconState,
        runtime: InMemoryBook,
        snapshot: PolymarketBookSnapshot,
        allow_auto_resync: bool,
    ) -> dict[str, Any]:
        query: Select[Any] = select(PolymarketBboEvent).where(PolymarketBboEvent.asset_id == runtime.asset_id)
        if state.last_bbo_raw_event_id is not None:
            query = query.where(PolymarketBboEvent.raw_event_id > state.last_bbo_raw_event_id)
        elif snapshot.raw_event_id is not None:
            query = query.where(PolymarketBboEvent.raw_event_id > snapshot.raw_event_id)
        else:
            boundary = _coalesce_time(snapshot.observed_at_local, snapshot.event_ts_exchange)
            if boundary is not None:
                query = query.where(
                    func.coalesce(
                        PolymarketBboEvent.event_ts_exchange,
                        PolymarketBboEvent.recv_ts_local,
                        PolymarketBboEvent.ingest_ts_db,
                    )
                    > boundary
                )
        rows = (
            await session.execute(
                query.order_by(
                    PolymarketBboEvent.raw_event_id.asc(),
                    PolymarketBboEvent.id.asc(),
                )
            )
        ).scalars().all()
        for row in rows:
            best_bid, best_ask, _ = runtime.top_of_book()
            if not _prices_equal(best_bid, row.best_bid, self._bbo_tolerance) or not _prices_equal(
                best_ask,
                row.best_ask,
                self._bbo_tolerance,
            ):
                await self._mark_incident(
                    session,
                    state=state,
                    incident_type="bbo_mismatch",
                    severity="warning",
                    raw_event_id=row.raw_event_id,
                    snapshot_id=state.last_snapshot_id,
                    exchange_ts=row.event_ts_exchange,
                    expected_best_bid=best_bid,
                    observed_best_bid=row.best_bid,
                    expected_best_ask=best_ask,
                    observed_best_ask=row.best_ask,
                    expected_hash=state.last_snapshot_hash,
                    details_json={"bbo_event_id": row.id},
                    increment_drift=True,
                    status="drifted",
                )
                pending_resync = await self._auto_resync_after_drift(
                    state,
                    [runtime.asset_id],
                    reason="bbo_mismatch",
                    allow_auto_resync=allow_auto_resync,
                )
                if pending_resync is not None:
                    return {"retry": True, "pending_resync": pending_resync}
                return {"retry": False}
            state.last_bbo_raw_event_id = row.raw_event_id
            state.last_applied_raw_event_id = max(filter(None, [state.last_applied_raw_event_id, row.raw_event_id]))
            state.last_reconciled_at = utcnow()
            runtime.last_exchange_ts = _coalesce_time(row.event_ts_exchange, runtime.last_exchange_ts)
            runtime.last_received_at_local = _coalesce_time(row.recv_ts_local, runtime.last_received_at_local)
            polymarket_book_recon_rows_applied.labels(kind="bbo").inc()
        return {"retry": False}

    async def _advance_trade_cursor(
        self,
        session: AsyncSession,
        *,
        state: PolymarketBookReconState,
        snapshot: PolymarketBookSnapshot,
    ) -> None:
        query: Select[Any] = select(func.max(PolymarketTradeTape.raw_event_id)).where(
            PolymarketTradeTape.asset_id == state.asset_id,
            PolymarketTradeTape.raw_event_id.is_not(None),
        )
        if state.last_trade_raw_event_id is not None:
            query = query.where(PolymarketTradeTape.raw_event_id > state.last_trade_raw_event_id)
        elif snapshot.raw_event_id is not None:
            query = query.where(PolymarketTradeTape.raw_event_id > snapshot.raw_event_id)
        max_trade_raw_event_id = (await session.execute(query)).scalar_one_or_none()
        if max_trade_raw_event_id is not None:
            state.last_trade_raw_event_id = int(max_trade_raw_event_id)
            state.last_applied_raw_event_id = max(
                filter(None, [state.last_applied_raw_event_id, state.last_trade_raw_event_id])
            )
            polymarket_book_recon_rows_applied.labels(kind="trade").inc()

    async def _check_staleness(self, session: AsyncSession, *, state: PolymarketBookReconState) -> None:
        reference_time = _coalesce_time(state.last_reconciled_at, state.last_resynced_at, state.updated_at)
        if reference_time is None:
            return
        if (utcnow() - reference_time).total_seconds() < settings.polymarket_book_recon_stale_after_seconds:
            if state.status == "stale":
                state.status = "live"
            return
        if state.status == "stale":
            return
        await self._mark_incident(
            session,
            state=state,
            incident_type="stale_book",
            severity="warning",
            details_json={"stale_after_seconds": settings.polymarket_book_recon_stale_after_seconds},
            increment_drift=False,
            status="stale",
        )

    async def _process_stream_incidents(
        self,
        session: AsyncSession,
        *,
        asset_id: str,
        state: PolymarketBookReconState,
        allow_auto_resync: bool,
    ) -> dict[str, Any]:
        details = dict(state.details_json or {})
        last_seen_at = _normalize_datetime(parse_polymarket_timestamp(details.get("last_stream_incident_seen_at"))) or (
            datetime.fromtimestamp(0, tz=timezone.utc)
        )
        incidents = (
            await session.execute(
                select(PolymarketIngestIncident)
                .where(
                    PolymarketIngestIncident.created_at > last_seen_at,
                    PolymarketIngestIncident.incident_type.in_(tuple(STREAM_INCIDENT_MAP.keys())),
                )
                .order_by(PolymarketIngestIncident.created_at.asc())
            )
        ).scalars().all()
        reseed_required = False
        newest_seen_at = last_seen_at
        for incident in incidents:
            if not _incident_targets_asset(incident, asset_id):
                continue
            incident_created_at = _normalize_datetime(incident.created_at) or last_seen_at
            newest_seen_at = max(newest_seen_at, incident_created_at)
            incident_type = STREAM_INCIDENT_MAP[incident.incident_type]
            await self._mark_incident(
                session,
                state=state,
                incident_type=incident_type,
                severity="warning",
                raw_event_id=incident.raw_event_id,
                exchange_ts=incident_created_at,
                details_json={"source_incident_id": str(incident.id)},
                increment_drift=incident.incident_type == "gap_suspected",
                status="drifted",
            )
            if not await self._has_snapshot_since(session, asset_id=asset_id, since=incident_created_at):
                if allow_auto_resync and await self._can_resync(state):
                    state.status = "resyncing"
                    details["last_stream_incident_seen_at"] = newest_seen_at.isoformat()
                    state.details_json = details
                    return {
                        "retry": True,
                        "pending_resync": {
                            "asset_ids": [asset_id],
                            "reason": incident.incident_type,
                            "manual": False,
                        },
                    }
            reseed_required = True
        if newest_seen_at > last_seen_at:
            details["last_stream_incident_seen_at"] = newest_seen_at.isoformat()
            state.details_json = details
        return {"retry": False, "reseed": reseed_required}

    async def _has_snapshot_since(self, session: AsyncSession, *, asset_id: str, since: datetime) -> bool:
        effective_time = func.coalesce(PolymarketBookSnapshot.event_ts_exchange, PolymarketBookSnapshot.observed_at_local)
        count = int(
            (
                await session.execute(
                    select(func.count(PolymarketBookSnapshot.id)).where(
                        PolymarketBookSnapshot.asset_id == asset_id,
                        PolymarketBookSnapshot.source_kind.in_(AUTHORITATIVE_SNAPSHOT_KINDS),
                        effective_time >= since,
                    )
                )
            ).scalar_one()
            or 0
        )
        return count > 0

    async def _mark_incident(
        self,
        session: AsyncSession,
        *,
        state: PolymarketBookReconState,
        incident_type: str,
        severity: str,
        raw_event_id: int | None = None,
        snapshot_id: int | None = None,
        capture_run_id: Any | None = None,
        exchange_ts: datetime | None = None,
        expected_best_bid: Decimal | None = None,
        observed_best_bid: Decimal | None = None,
        expected_best_ask: Decimal | None = None,
        observed_best_ask: Decimal | None = None,
        expected_hash: str | None = None,
        observed_hash: str | None = None,
        details_json: dict[str, Any] | None = None,
        increment_drift: bool,
        status: str,
    ) -> PolymarketBookReconIncident:
        incident = PolymarketBookReconIncident(
            market_dim_id=state.market_dim_id,
            asset_dim_id=state.asset_dim_id,
            condition_id=state.condition_id,
            asset_id=state.asset_id,
            incident_type=incident_type,
            severity=severity,
            raw_event_id=raw_event_id,
            snapshot_id=snapshot_id,
            capture_run_id=capture_run_id,
            exchange_ts=_normalize_datetime(exchange_ts),
            observed_at_local=utcnow(),
            expected_best_bid=expected_best_bid,
            observed_best_bid=observed_best_bid,
            expected_best_ask=expected_best_ask,
            observed_best_ask=observed_best_ask,
            expected_hash=expected_hash,
            observed_hash=observed_hash,
            details_json=details_json,
        )
        session.add(incident)
        if increment_drift:
            state.drift_count += 1
        state.status = status
        if incident_type in DRIFT_INCIDENT_TYPES:
            polymarket_book_recon_drift_incidents.labels(incident_type=incident_type).inc()
        return incident

    async def _can_resync(self, state: PolymarketBookReconState) -> bool:
        if not settings.polymarket_book_recon_auto_resync_enabled:
            return False
        if state.last_resynced_at is None:
            return True
        return (utcnow() - state.last_resynced_at).total_seconds() >= settings.polymarket_book_recon_resync_cooldown_seconds

    async def _auto_resync_after_drift(
        self,
        state: PolymarketBookReconState,
        asset_ids: list[str],
        *,
        reason: str,
        allow_auto_resync: bool,
    ) -> dict[str, Any] | None:
        if not allow_auto_resync or not await self._can_resync(state):
            return None
        state.status = "resyncing"
        return {
            "asset_ids": asset_ids,
            "reason": reason,
            "manual": False,
        }

    async def _trigger_resync(self, asset_ids: list[str], *, reason: str, manual: bool) -> dict[str, Any]:
        if not asset_ids:
            return {
                "run_id": None,
                "asset_ids": [],
                "requested_asset_count": 0,
                "succeeded_asset_count": 0,
                "failed_asset_count": 0,
                "succeeded_asset_ids": [],
                "failed_asset_ids": [],
                "events_persisted": 0,
                "reason": reason,
                "status": "completed",
            }
        async with self._session_factory() as session:
            states = (
                await session.execute(
                    select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id.in_(asset_ids))
                )
            ).scalars().all()
            for state in states:
                await self._mark_incident(
                    session,
                    state=state,
                    incident_type="resync_started",
                    severity="info",
                    details_json={"reason": reason},
                    increment_drift=False,
                    status="resyncing",
                )
            await session.commit()
        try:
            result = await self._resync_service.resync_assets(asset_ids, reason=reason)
            projector_result = await self._raw_storage_service.project_until_idle(reason=f"recon_{reason}", max_batches=10)
            result["projector"] = projector_result
        except Exception as exc:
            status = "failed"
            if manual:
                polymarket_book_recon_manual_resync_runs.labels(status=status).inc()
            else:
                polymarket_book_recon_auto_resync_runs.labels(status=status).inc()
            async with self._session_factory() as session:
                states = (
                    await session.execute(
                        select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id.in_(asset_ids))
                    )
                ).scalars().all()
                for state in states:
                    await self._mark_incident(
                        session,
                        state=state,
                        incident_type="resync_failed",
                        severity="error",
                        details_json={"reason": reason, "error": str(exc)},
                        increment_drift=False,
                        status="error",
                    )
                await session.commit()
            raise

        status = str(result.get("status") or "failed")
        succeeded_asset_ids = {str(asset_id) for asset_id in result.get("succeeded_asset_ids") or []}
        failed_asset_ids = {str(asset_id) for asset_id in result.get("failed_asset_ids") or []}
        if status == "completed" and not succeeded_asset_ids and not failed_asset_ids:
            succeeded_asset_ids = {str(asset_id) for asset_id in asset_ids}
        elif status == "failed" and not failed_asset_ids:
            failed_asset_ids = {str(asset_id) for asset_id in asset_ids}
        unresolved_asset_ids = {
            str(asset_id) for asset_id in asset_ids if str(asset_id) not in succeeded_asset_ids | failed_asset_ids
        }
        if status == "partial":
            failed_asset_ids |= unresolved_asset_ids
        elif status == "completed":
            succeeded_asset_ids |= unresolved_asset_ids
        else:
            failed_asset_ids |= unresolved_asset_ids
        if manual:
            polymarket_book_recon_manual_resync_runs.labels(status=status).inc()
        else:
            polymarket_book_recon_auto_resync_runs.labels(status=status).inc()
        async with self._session_factory() as session:
            states = (
                await session.execute(
                    select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id.in_(asset_ids))
                )
            ).scalars().all()
            for state in states:
                asset_succeeded = state.asset_id in succeeded_asset_ids
                await self._mark_incident(
                    session,
                    state=state,
                    incident_type="resync_succeeded" if asset_succeeded else "resync_failed",
                    severity="info" if asset_succeeded else "warning",
                    details_json={
                        "reason": reason,
                        "run_id": str(result.get("run_id")),
                        "status": status,
                    },
                    increment_drift=False,
                    status="live" if asset_succeeded else "drifted",
                )
                if asset_succeeded:
                    state.last_resynced_at = utcnow()
                    state.resync_count += 1
            await session.commit()
        if status in {"completed", "partial"}:
            completed_at = utcnow()
            polymarket_book_recon_last_successful_resync_timestamp.set(completed_at.timestamp())
        return result

    def _persist_runtime_to_state(self, state: PolymarketBookReconState, runtime: InMemoryBook) -> None:
        best_bid, best_ask, spread = runtime.top_of_book()
        state.market_dim_id = runtime.market_dim_id
        state.asset_dim_id = runtime.asset_dim_id
        state.condition_id = runtime.condition_id
        state.best_bid = best_bid
        state.best_ask = best_ask
        state.spread = spread
        state.depth_levels_bid = len(runtime.bids)
        state.depth_levels_ask = len(runtime.asks)
        state.expected_tick_size = runtime.expected_tick_size
        state.last_snapshot_id = runtime.last_snapshot_id
        state.last_snapshot_source_kind = runtime.last_snapshot_source_kind
        state.last_snapshot_hash = runtime.last_snapshot_hash
        state.last_snapshot_exchange_ts = runtime.last_snapshot_exchange_ts
        state.last_exchange_ts = runtime.last_exchange_ts
        state.last_received_at_local = runtime.last_received_at_local
        if state.status not in DEGRADED_STATUSES:
            state.status = "live"


async def fetch_polymarket_book_recon_status(session: AsyncSession) -> dict[str, Any]:
    recent_incidents = (
        await session.execute(
            select(PolymarketBookReconIncident)
            .order_by(PolymarketBookReconIncident.observed_at_local.desc())
            .limit(10)
        )
    ).scalars().all()
    state_counts = (
        await session.execute(
            select(PolymarketBookReconState.status, func.count(PolymarketBookReconState.id))
            .group_by(PolymarketBookReconState.status)
        )
    ).all()
    counts = {status: int(count or 0) for status, count in state_counts}
    watched_assets = await list_watched_polymarket_assets(session)
    live_count = counts.get("live", 0)
    drifted_count = counts.get("drifted", 0)
    resyncing_count = counts.get("resyncing", 0)
    degraded_count = sum(counts.get(status, 0) for status in DEGRADED_STATUSES)
    last_successful_resync_at = (
        await session.execute(select(func.max(PolymarketBookReconState.last_resynced_at)))
    ).scalar_one_or_none()
    recent_incident_count = int(
        (
            await session.execute(
                select(func.count(PolymarketBookReconIncident.id)).where(
                    PolymarketBookReconIncident.observed_at_local >= utcnow() - timedelta(hours=24)
                )
            )
        ).scalar_one()
        or 0
    )
    polymarket_book_recon_live_books.set(live_count)
    for status in DEGRADED_STATUSES:
        polymarket_book_recon_assets_degraded.labels(status=status).set(counts.get(status, 0))
    if last_successful_resync_at is not None:
        polymarket_book_recon_last_successful_resync_timestamp.set(last_successful_resync_at.timestamp())
    return {
        "enabled": settings.polymarket_book_recon_enabled,
        "on_startup": settings.polymarket_book_recon_on_startup,
        "auto_resync_enabled": settings.polymarket_book_recon_auto_resync_enabled,
        "stale_after_seconds": settings.polymarket_book_recon_stale_after_seconds,
        "resync_cooldown_seconds": settings.polymarket_book_recon_resync_cooldown_seconds,
        "max_watched_assets": settings.polymarket_book_recon_max_watched_assets,
        "bbo_tolerance": settings.polymarket_book_recon_bbo_tolerance,
        "watched_asset_count": len(watched_assets),
        "live_book_count": live_count,
        "drifted_asset_count": drifted_count,
        "resyncing_asset_count": resyncing_count,
        "degraded_asset_count": degraded_count,
        "last_successful_resync_at": last_successful_resync_at,
        "recent_incident_count": recent_incident_count,
        "status_counts": counts,
        "recent_incidents": [_serialize_recon_incident(row) for row in recent_incidents],
    }


async def lookup_polymarket_book_recon_state(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketBookReconState)
    if asset_id:
        query = query.where(PolymarketBookReconState.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookReconState.condition_id == condition_id)
    if status:
        query = query.where(PolymarketBookReconState.status == status)
    rows = (
        await session.execute(
            query.order_by(PolymarketBookReconState.updated_at.desc(), PolymarketBookReconState.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_recon_state(row) for row in rows]


async def list_polymarket_book_recon_incidents(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    status: str | None,
    incident_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query: Select[Any] = (
        select(PolymarketBookReconIncident)
        .join(PolymarketBookReconState, PolymarketBookReconState.asset_id == PolymarketBookReconIncident.asset_id)
    )
    if asset_id:
        query = query.where(PolymarketBookReconIncident.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookReconIncident.condition_id == condition_id)
    if status:
        query = query.where(PolymarketBookReconState.status == status)
    if incident_type:
        query = query.where(PolymarketBookReconIncident.incident_type == incident_type)
    rows = (
        await session.execute(
            query.order_by(
                PolymarketBookReconIncident.observed_at_local.desc(),
                PolymarketBookReconIncident.created_at.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [_serialize_recon_incident(row) for row in rows]


async def get_polymarket_reconstructed_top_of_book(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
) -> dict[str, Any] | None:
    query: Select[Any] = select(PolymarketBookReconState)
    if asset_id:
        query = query.where(PolymarketBookReconState.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookReconState.condition_id == condition_id)
    row = (
        await session.execute(
            query.order_by(PolymarketBookReconState.updated_at.desc(), PolymarketBookReconState.id.desc()).limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    serialized = _serialize_recon_state(row)
    return {
        "asset_id": serialized["asset_id"],
        "condition_id": serialized["condition_id"],
        "status": serialized["status"],
        "best_bid": serialized["best_bid"],
        "best_ask": serialized["best_ask"],
        "spread": serialized["spread"],
        "depth_levels_bid": serialized["depth_levels_bid"],
        "depth_levels_ask": serialized["depth_levels_ask"],
        "last_snapshot_id": serialized["last_snapshot_id"],
        "last_snapshot_source_kind": serialized["last_snapshot_source_kind"],
        "last_reconciled_at": serialized["last_reconciled_at"],
        "last_resynced_at": serialized["last_resynced_at"],
        "updated_at": serialized["updated_at"],
    }


async def trigger_manual_polymarket_book_recon_resync(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    asset_ids: list[str] | None,
    reason: str,
) -> dict[str, Any]:
    service = PolymarketBookReconstructionService(session_factory)
    try:
        return await service.manual_resync(asset_ids=asset_ids, reason=reason)
    finally:
        await service.close()


async def trigger_manual_polymarket_book_recon_catchup(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    asset_ids: list[str] | None,
    reason: str,
) -> dict[str, Any]:
    service = PolymarketBookReconstructionService(session_factory)
    try:
        return await service.sync_scope(asset_ids=asset_ids, reason=reason, trigger_resync_for_missing_seed=True)
    finally:
        await service.close()
