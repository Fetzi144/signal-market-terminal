from __future__ import annotations

import asyncio
import logging
import uuid
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_book_reconstruction import AUTHORITATIVE_SNAPSHOT_KINDS, DRIFT_INCIDENT_TYPES
from app.ingestion.polymarket_common import utcnow
from app.ingestion.polymarket_stream import list_watched_polymarket_assets
from app.metrics import (
    polymarket_feature_last_success_timestamp,
    polymarket_feature_rows_inserted,
    polymarket_feature_run_failures,
    polymarket_feature_runs,
    polymarket_incomplete_bucket_count,
    polymarket_label_last_success_timestamp,
    polymarket_label_rows_inserted,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketParamHistory
from app.models.polymarket_microstructure import (
    PolymarketAlphaLabel,
    PolymarketBookStateTopN,
    PolymarketFeatureRun,
    PolymarketMicrostructureFeature1s,
    PolymarketMicrostructureFeature100ms,
    PolymarketPassiveFillLabel,
)
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketTradeTape,
)
from app.models.polymarket_reconstruction import PolymarketBookReconIncident, PolymarketBookReconState

logger = logging.getLogger(__name__)

RUN_TYPE_BOOK_STATE = "book_state_materialize"
RUN_TYPE_FEATURE = "feature_materialize"
RUN_TYPE_LABEL = "label_materialize"
SUCCESS_RUN_STATUSES = {"completed", "partial"}
FEATURE_TABLE_BY_BUCKET = {
    100: PolymarketMicrostructureFeature100ms,
    1000: PolymarketMicrostructureFeature1s,
}
FEATURE_TABLE_NAME_BY_BUCKET = {
    100: "polymarket_microstructure_features_100ms",
    1000: "polymarket_microstructure_features_1s",
}
LABEL_SIDES = ("buy_post_best_bid", "sell_post_best_ask")


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coalesce_time(*values: datetime | None) -> datetime | None:
    for value in values:
        normalized = _normalize_datetime(value)
        if normalized is not None:
            return normalized
    return None


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


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


def _mid(best_bid: Decimal | None, best_ask: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / Decimal("2")


def _sorted_levels(book: dict[Decimal, Decimal], *, descending: bool, limit: int) -> list[tuple[Decimal, Decimal]]:
    return sorted(book.items(), key=lambda item: item[0], reverse=descending)[:limit]


def _depth(book: dict[Decimal, Decimal], *, descending: bool, limit: int) -> Decimal | None:
    levels = _sorted_levels(book, descending=descending, limit=limit)
    if not levels:
        return None
    return sum((size for _, size in levels), start=Decimal("0"))


def _imbalance(bid_depth: Decimal | None, ask_depth: Decimal | None) -> Decimal | None:
    if bid_depth is None or ask_depth is None:
        return None
    denominator = bid_depth + ask_depth
    if denominator == 0:
        return None
    return (bid_depth - ask_depth) / denominator


def _microprice(best_bid: Decimal | None, best_ask: Decimal | None, bid_size: Decimal | None, ask_size: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None or bid_size is None or ask_size is None:
        return None
    denominator = bid_size + ask_size
    if denominator == 0:
        return None
    return ((best_ask * bid_size) + (best_bid * ask_size)) / denominator


def _bucket_floor(value: datetime, width_ms: int) -> datetime:
    normalized = _normalize_datetime(value) or value.replace(tzinfo=timezone.utc)
    millis = int(normalized.timestamp() * 1000)
    floored = millis - (millis % width_ms)
    return datetime.fromtimestamp(floored / 1000, tz=timezone.utc)


def _bucket_starts(start: datetime, end: datetime, width_ms: int) -> list[datetime]:
    if end < start:
        return []
    bucket = _bucket_floor(start, width_ms)
    result: list[datetime] = []
    step = timedelta(milliseconds=width_ms)
    while bucket <= end:
        result.append(bucket)
        bucket += step
    return result


def _serialize_levels(levels: list[tuple[Decimal, Decimal]]) -> list[dict[str, str]]:
    return [{"price": str(price), "size": str(size)} for price, size in levels]


def _effective_time_for_snapshot(row: PolymarketBookSnapshot) -> datetime:
    return _coalesce_time(row.event_ts_exchange, row.observed_at_local, row.created_at) or utcnow()


def _effective_time_for_delta(row: PolymarketBookDelta) -> datetime:
    return _coalesce_time(row.event_ts_exchange, row.recv_ts_local, row.ingest_ts_db, row.created_at) or utcnow()


def _effective_time_for_bbo(row: PolymarketBboEvent) -> datetime:
    return _coalesce_time(row.event_ts_exchange, row.recv_ts_local, row.ingest_ts_db, row.created_at) or utcnow()


def _effective_time_for_trade(row: PolymarketTradeTape) -> datetime:
    return _coalesce_time(row.event_ts_exchange, row.observed_at_local, row.created_at) or utcnow()


def _effective_time_for_param(row: PolymarketMarketParamHistory) -> datetime:
    return _coalesce_time(row.effective_at_exchange, row.observed_at_local, row.created_at) or utcnow()


def _effective_time_for_incident(row: PolymarketBookReconIncident) -> datetime:
    return _coalesce_time(row.exchange_ts, row.observed_at_local, row.created_at) or utcnow()


def _flags_with_partial(flags: dict[str, Any]) -> dict[str, Any]:
    flagged = dict(flags)
    flagged["source_coverage_partial"] = bool(
        flagged.get("source_coverage_partial")
        or not flagged.get("trustworthy_seed", False)
        or flagged.get("affected_by_drift", False)
        or flagged.get("crossed_snapshot_boundary", False)
        or flagged.get("crossed_resync_boundary", False)
        or flagged.get("missing_trade_side", False)
        or flagged.get("delta_without_seed", False)
    )
    return flagged


def _feature_flags(row: Any) -> dict[str, Any]:
    value = row.completeness_flags_json
    return dict(value) if isinstance(value, dict) else {}


@dataclass(slots=True)
class AssetContext:
    asset_id: str
    condition_id: str
    market_dim_id: int | None
    asset_dim_id: int | None
    recon_state_id: int | None


@dataclass(slots=True)
class ReplayMarker:
    exchange_time: datetime
    observed_at_local: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    mid: Decimal | None
    microprice: Decimal | None
    tick_size: Decimal | None
    bid_levels: list[tuple[Decimal, Decimal]]
    ask_levels: list[tuple[Decimal, Decimal]]
    bid_depth_top1: Decimal | None
    bid_depth_top3: Decimal | None
    bid_depth_top5: Decimal | None
    ask_depth_top1: Decimal | None
    ask_depth_top3: Decimal | None
    ask_depth_top5: Decimal | None
    imbalance_top1: Decimal | None
    imbalance_top3: Decimal | None
    imbalance_top5: Decimal | None
    trustworthy_seed: bool
    affected_by_drift: bool
    last_snapshot_id: int | None
    last_snapshot_hash: str | None
    last_applied_raw_event_id: int | None


@dataclass(slots=True)
class DeltaFlowObservation:
    exchange_time: datetime
    observed_at_local: datetime
    raw_event_id: int
    side: str
    add_volume: Decimal
    remove_volume: Decimal


@dataclass(slots=True)
class TradeObservation:
    exchange_time: datetime
    observed_at_local: datetime
    raw_event_id: int | None
    side: str | None
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class BboObservation:
    exchange_time: datetime
    observed_at_local: datetime
    raw_event_id: int | None
    best_bid: Decimal | None
    best_ask: Decimal | None


@dataclass(slots=True)
class SnapshotBoundary:
    exchange_time: datetime
    source_kind: str


@dataclass(slots=True)
class ReplayData:
    context: AssetContext
    markers: list[ReplayMarker]
    marker_times: list[datetime]
    delta_flows: list[DeltaFlowObservation]
    trades: list[TradeObservation]
    bbo_events: list[BboObservation]
    snapshot_boundaries: list[SnapshotBoundary]
    drift_times: list[datetime]
    partial_event_times: list[datetime]
    latest_observed_time: datetime | None


def _marker_from_runtime(
    *,
    exchange_time: datetime,
    observed_at_local: datetime,
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
    tick_size: Decimal | None,
    trustworthy_seed: bool,
    affected_by_drift: bool,
    last_snapshot_id: int | None,
    last_snapshot_hash: str | None,
    last_applied_raw_event_id: int | None,
) -> ReplayMarker:
    bid_levels = _sorted_levels(bids, descending=True, limit=5)
    ask_levels = _sorted_levels(asks, descending=False, limit=5)
    best_bid = bid_levels[0][0] if bid_levels else None
    best_ask = ask_levels[0][0] if ask_levels else None
    bid_depth_top1 = _depth(bids, descending=True, limit=1)
    bid_depth_top3 = _depth(bids, descending=True, limit=3)
    bid_depth_top5 = _depth(bids, descending=True, limit=5)
    ask_depth_top1 = _depth(asks, descending=False, limit=1)
    ask_depth_top3 = _depth(asks, descending=False, limit=3)
    ask_depth_top5 = _depth(asks, descending=False, limit=5)
    spread = _spread(best_bid, best_ask)
    mid = _mid(best_bid, best_ask)
    microprice = _microprice(best_bid, best_ask, bid_depth_top1, ask_depth_top1)
    return ReplayMarker(
        exchange_time=exchange_time,
        observed_at_local=observed_at_local,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        mid=mid,
        microprice=microprice,
        tick_size=tick_size,
        bid_levels=bid_levels,
        ask_levels=ask_levels,
        bid_depth_top1=bid_depth_top1,
        bid_depth_top3=bid_depth_top3,
        bid_depth_top5=bid_depth_top5,
        ask_depth_top1=ask_depth_top1,
        ask_depth_top3=ask_depth_top3,
        ask_depth_top5=ask_depth_top5,
        imbalance_top1=_imbalance(bid_depth_top1, ask_depth_top1),
        imbalance_top3=_imbalance(bid_depth_top3, ask_depth_top3),
        imbalance_top5=_imbalance(bid_depth_top5, ask_depth_top5),
        trustworthy_seed=trustworthy_seed,
        affected_by_drift=affected_by_drift,
        last_snapshot_id=last_snapshot_id,
        last_snapshot_hash=last_snapshot_hash,
        last_applied_raw_event_id=last_applied_raw_event_id,
    )


def _serialize_feature_run(row: PolymarketFeatureRun) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_type": row.run_type,
        "reason": row.reason,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
        "status": row.status,
        "scope_json": row.scope_json,
        "cursor_json": row.cursor_json,
        "rows_inserted_json": row.rows_inserted_json,
        "error_count": row.error_count,
        "details_json": row.details_json,
    }


def _serialize_book_state(row: PolymarketBookStateTopN) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "bucket_start_exchange": row.bucket_start_exchange,
        "bucket_width_ms": row.bucket_width_ms,
        "observed_at_local": row.observed_at_local,
        "recon_state_id": row.recon_state_id,
        "last_snapshot_id": row.last_snapshot_id,
        "last_snapshot_hash": row.last_snapshot_hash,
        "last_applied_raw_event_id": row.last_applied_raw_event_id,
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "spread": _serialize_decimal(row.spread),
        "mid": _serialize_decimal(row.mid),
        "microprice": _serialize_decimal(row.microprice),
        "bid_levels_json": row.bid_levels_json,
        "ask_levels_json": row.ask_levels_json,
        "bid_depth_top1": _serialize_decimal(row.bid_depth_top1),
        "bid_depth_top3": _serialize_decimal(row.bid_depth_top3),
        "bid_depth_top5": _serialize_decimal(row.bid_depth_top5),
        "ask_depth_top1": _serialize_decimal(row.ask_depth_top1),
        "ask_depth_top3": _serialize_decimal(row.ask_depth_top3),
        "ask_depth_top5": _serialize_decimal(row.ask_depth_top5),
        "imbalance_top1": _serialize_decimal(row.imbalance_top1),
        "imbalance_top3": _serialize_decimal(row.imbalance_top3),
        "imbalance_top5": _serialize_decimal(row.imbalance_top5),
        "completeness_flags_json": row.completeness_flags_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_feature_row(row: Any, *, table_name: str) -> dict[str, Any]:
    return {
        "id": row.id,
        "table_name": table_name,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "bucket_start_exchange": row.bucket_start_exchange,
        "bucket_end_exchange": row.bucket_end_exchange,
        "observed_at_local": row.observed_at_local,
        "source_book_state_id": row.source_book_state_id,
        "run_id": row.run_id,
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "spread": _serialize_decimal(row.spread),
        "mid": _serialize_decimal(row.mid),
        "microprice": _serialize_decimal(row.microprice),
        "tick_size": _serialize_decimal(row.tick_size),
        "bid_depth_top1": _serialize_decimal(row.bid_depth_top1),
        "ask_depth_top1": _serialize_decimal(row.ask_depth_top1),
        "bid_depth_top3": _serialize_decimal(row.bid_depth_top3),
        "ask_depth_top3": _serialize_decimal(row.ask_depth_top3),
        "bid_depth_top5": _serialize_decimal(row.bid_depth_top5),
        "ask_depth_top5": _serialize_decimal(row.ask_depth_top5),
        "imbalance_top1": _serialize_decimal(row.imbalance_top1),
        "imbalance_top3": _serialize_decimal(row.imbalance_top3),
        "imbalance_top5": _serialize_decimal(row.imbalance_top5),
        "bid_add_volume": _serialize_decimal(row.bid_add_volume),
        "ask_add_volume": _serialize_decimal(row.ask_add_volume),
        "bid_remove_volume": _serialize_decimal(row.bid_remove_volume),
        "ask_remove_volume": _serialize_decimal(row.ask_remove_volume),
        "buy_trade_volume": _serialize_decimal(row.buy_trade_volume),
        "sell_trade_volume": _serialize_decimal(row.sell_trade_volume),
        "buy_trade_count": row.buy_trade_count,
        "sell_trade_count": row.sell_trade_count,
        "trade_notional": _serialize_decimal(row.trade_notional),
        "last_trade_price": _serialize_decimal(row.last_trade_price),
        "last_trade_side": row.last_trade_side,
        "book_update_count": row.book_update_count,
        "bbo_update_count": row.bbo_update_count,
        "completeness_flags_json": row.completeness_flags_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_alpha_label(row: PolymarketAlphaLabel) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "anchor_bucket_start_exchange": row.anchor_bucket_start_exchange,
        "horizon_ms": row.horizon_ms,
        "source_feature_table": row.source_feature_table,
        "source_feature_row_id": row.source_feature_row_id,
        "start_mid": _serialize_decimal(row.start_mid),
        "end_mid": _serialize_decimal(row.end_mid),
        "mid_return_bps": _serialize_decimal(row.mid_return_bps),
        "mid_move_ticks": _serialize_decimal(row.mid_move_ticks),
        "best_bid_change": _serialize_decimal(row.best_bid_change),
        "best_ask_change": _serialize_decimal(row.best_ask_change),
        "up_move": row.up_move,
        "down_move": row.down_move,
        "flat_move": row.flat_move,
        "completeness_flags_json": row.completeness_flags_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_passive_fill_label(row: PolymarketPassiveFillLabel) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "anchor_bucket_start_exchange": row.anchor_bucket_start_exchange,
        "horizon_ms": row.horizon_ms,
        "side": row.side,
        "posted_price": _serialize_decimal(row.posted_price),
        "touch_observed": row.touch_observed,
        "trade_through_observed": row.trade_through_observed,
        "best_price_improved_against_order": row.best_price_improved_against_order,
        "adverse_move_after_touch_bps": _serialize_decimal(row.adverse_move_after_touch_bps),
        "source_feature_table": row.source_feature_table,
        "source_feature_row_id": row.source_feature_row_id,
        "completeness_flags_json": row.completeness_flags_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


class PolymarketMicrostructureService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def close(self) -> None:
        return None

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_features_enabled:
            logger.info("Polymarket microstructure features disabled; skipping worker startup")
            return

        if settings.polymarket_features_on_startup:
            try:
                await self.materialize_scope(reason="startup")
            except Exception:
                logger.warning("Polymarket microstructure startup run failed", exc_info=True)

        interval_seconds = max(1, settings.polymarket_features_interval_seconds)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                continue
            except asyncio.TimeoutError:
                pass

            try:
                await self.materialize_scope(reason="scheduled")
            except Exception:
                logger.warning("Polymarket microstructure scheduled run failed", exc_info=True)

    async def materialize_scope(
        self,
        *,
        reason: str,
        asset_ids: list[str] | None = None,
        condition_ids: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            contexts = await self._resolve_scope(session, asset_ids=asset_ids, condition_ids=condition_ids)
        if end is None:
            end = utcnow()
        if start is None:
            start = end - timedelta(hours=settings.polymarket_features_lookback_hours)
        start = _normalize_datetime(start) or start.replace(tzinfo=timezone.utc)
        end = _normalize_datetime(end) or end.replace(tzinfo=timezone.utc)
        if end < start:
            raise ValueError("end must be >= start")

        scope_json = {
            "asset_ids": [context.asset_id for context in contexts],
            "condition_ids": [context.condition_id for context in contexts],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "bucket_widths_ms": settings.polymarket_feature_bucket_values_ms,
            "label_horizons_ms": settings.polymarket_label_horizon_values_ms,
        }

        book_run = await self._start_run(run_type=RUN_TYPE_BOOK_STATE, reason=reason, scope_json=scope_json)
        feature_run = await self._start_run(run_type=RUN_TYPE_FEATURE, reason=reason, scope_json=scope_json)
        label_run = await self._start_run(run_type=RUN_TYPE_LABEL, reason=reason, scope_json=scope_json)

        book_rows = {"polymarket_book_state_topn": 0}
        feature_rows = {
            "polymarket_microstructure_features_100ms": 0,
            "polymarket_microstructure_features_1s": 0,
        }
        label_rows = {
            "polymarket_alpha_labels": 0,
            "polymarket_passive_fill_labels": 0,
        }
        errors: list[dict[str, str]] = []

        for context in contexts:
            try:
                async with self._session_factory() as session:
                    replay = await self._build_replay(session, context=context, start=start, end=end)
                    row_counts = await self._materialize_asset(
                        session,
                        replay=replay,
                        start=start,
                        end=end,
                        feature_run_id=feature_run["id"],
                    )
                    await session.commit()
                book_rows["polymarket_book_state_topn"] += row_counts["book_state"]
                feature_rows["polymarket_microstructure_features_100ms"] += row_counts["features_100ms"]
                feature_rows["polymarket_microstructure_features_1s"] += row_counts["features_1s"]
                label_rows["polymarket_alpha_labels"] += row_counts["alpha_labels"]
                label_rows["polymarket_passive_fill_labels"] += row_counts["passive_fill_labels"]
            except Exception as exc:
                logger.warning("Polymarket microstructure materialization failed for %s", context.asset_id, exc_info=True)
                errors.append({"asset_id": context.asset_id, "error": str(exc)})

        successful_assets = max(0, len(contexts) - len(errors))
        if errors and successful_assets:
            status = "partial"
        elif errors:
            status = "failed"
        else:
            status = "completed"

        details_json = {
            "asset_count": len(contexts),
            "successful_asset_count": successful_assets,
            "failed_asset_count": len(errors),
            "errors": errors or None,
        }

        book_run = await self._finish_run(
            run_id=book_run["id"],
            run_type=RUN_TYPE_BOOK_STATE,
            reason=reason,
            status=status,
            rows_inserted_json=book_rows,
            error_count=len(errors),
            details_json=details_json,
        )
        feature_run = await self._finish_run(
            run_id=feature_run["id"],
            run_type=RUN_TYPE_FEATURE,
            reason=reason,
            status=status,
            rows_inserted_json=feature_rows,
            error_count=len(errors),
            details_json=details_json,
        )
        label_run = await self._finish_run(
            run_id=label_run["id"],
            run_type=RUN_TYPE_LABEL,
            reason=reason,
            status=status,
            rows_inserted_json=label_rows,
            error_count=len(errors),
            details_json=details_json,
        )

        if feature_rows["polymarket_microstructure_features_100ms"]:
            polymarket_feature_rows_inserted.labels(table_name="polymarket_microstructure_features_100ms").inc(
                feature_rows["polymarket_microstructure_features_100ms"]
            )
        if feature_rows["polymarket_microstructure_features_1s"]:
            polymarket_feature_rows_inserted.labels(table_name="polymarket_microstructure_features_1s").inc(
                feature_rows["polymarket_microstructure_features_1s"]
            )
        if label_rows["polymarket_alpha_labels"]:
            polymarket_label_rows_inserted.labels(label_type="polymarket_alpha_labels").inc(
                label_rows["polymarket_alpha_labels"]
            )
        if label_rows["polymarket_passive_fill_labels"]:
            polymarket_label_rows_inserted.labels(label_type="polymarket_passive_fill_labels").inc(
                label_rows["polymarket_passive_fill_labels"]
            )

        await self._refresh_incomplete_bucket_gauge()

        return {
            "status": status,
            "scope_json": scope_json,
            "book_state_run": book_run,
            "feature_run": feature_run,
            "label_run": label_run,
        }

    async def _resolve_scope(
        self,
        session: AsyncSession,
        *,
        asset_ids: list[str] | None,
        condition_ids: list[str] | None,
    ) -> list[AssetContext]:
        normalized_asset_ids = [str(value) for value in (asset_ids or []) if value]
        normalized_condition_ids = [str(value) for value in (condition_ids or []) if value]
        if not normalized_asset_ids and not normalized_condition_ids:
            normalized_asset_ids = await list_watched_polymarket_assets(session)

        query: Select[Any] = (
            select(
                PolymarketAssetDim.asset_id,
                PolymarketAssetDim.condition_id,
                PolymarketAssetDim.market_dim_id,
                PolymarketAssetDim.id,
                PolymarketBookReconState.id,
            )
            .outerjoin(PolymarketBookReconState, PolymarketBookReconState.asset_id == PolymarketAssetDim.asset_id)
            .order_by(PolymarketAssetDim.asset_id.asc())
        )
        if normalized_asset_ids:
            query = query.where(PolymarketAssetDim.asset_id.in_(normalized_asset_ids))
        if normalized_condition_ids:
            query = query.where(PolymarketAssetDim.condition_id.in_(normalized_condition_ids))

        rows = (await session.execute(query)).all()
        contexts = [
            AssetContext(
                asset_id=str(asset_id),
                condition_id=str(condition_id),
                market_dim_id=market_dim_id,
                asset_dim_id=asset_dim_id,
                recon_state_id=recon_state_id,
            )
            for asset_id, condition_id, market_dim_id, asset_dim_id, recon_state_id in rows
        ]

        seen_asset_ids = {context.asset_id for context in contexts}
        fallback_assets = [asset_id for asset_id in normalized_asset_ids if asset_id not in seen_asset_ids]
        for asset_id in fallback_assets:
            snapshot = (
                await session.execute(
                    select(PolymarketBookSnapshot, PolymarketBookReconState.id)
                    .outerjoin(PolymarketBookReconState, PolymarketBookReconState.asset_id == PolymarketBookSnapshot.asset_id)
                    .where(PolymarketBookSnapshot.asset_id == asset_id)
                    .order_by(PolymarketBookSnapshot.id.desc())
                    .limit(1)
                )
            ).first()
            if snapshot is None:
                continue
            row, recon_state_id = snapshot
            contexts.append(
                AssetContext(
                    asset_id=asset_id,
                    condition_id=row.condition_id,
                    market_dim_id=row.market_dim_id,
                    asset_dim_id=row.asset_dim_id,
                    recon_state_id=recon_state_id,
                )
            )

        max_assets = max(1, settings.polymarket_features_max_watched_assets)
        return contexts[:max_assets]

    async def _start_run(self, *, run_type: str, reason: str, scope_json: dict[str, Any]) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = PolymarketFeatureRun(
                run_type=run_type,
                reason=reason,
                started_at=utcnow(),
                status="running",
                scope_json=scope_json,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return _serialize_feature_run(row)

    async def _finish_run(
        self,
        *,
        run_id: uuid.UUID,
        run_type: str,
        reason: str,
        status: str,
        rows_inserted_json: dict[str, int],
        error_count: int,
        details_json: dict[str, Any],
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = await session.get(PolymarketFeatureRun, run_id)
            if row is None:
                raise ValueError(f"Feature run {run_id} not found")
            row.status = status
            row.completed_at = utcnow()
            row.rows_inserted_json = rows_inserted_json
            row.error_count = error_count
            row.details_json = details_json
            await session.commit()
            await session.refresh(row)

        polymarket_feature_runs.labels(run_type=run_type, reason=reason, status=status).inc()
        if status not in SUCCESS_RUN_STATUSES:
            polymarket_feature_run_failures.labels(run_type=run_type).inc()
        if run_type == RUN_TYPE_FEATURE and row.completed_at is not None and status in SUCCESS_RUN_STATUSES:
            polymarket_feature_last_success_timestamp.set(row.completed_at.timestamp())
        if run_type == RUN_TYPE_LABEL and row.completed_at is not None and status in SUCCESS_RUN_STATUSES:
            polymarket_label_last_success_timestamp.set(row.completed_at.timestamp())
        return _serialize_feature_run(row)

    async def _build_replay(
        self,
        session: AsyncSession,
        *,
        context: AssetContext,
        start: datetime,
        end: datetime,
    ) -> ReplayData:
        snapshot_effective = func.coalesce(PolymarketBookSnapshot.event_ts_exchange, PolymarketBookSnapshot.observed_at_local)
        seed_snapshot = (
            await session.execute(
                select(PolymarketBookSnapshot)
                .where(
                    PolymarketBookSnapshot.asset_id == context.asset_id,
                    PolymarketBookSnapshot.source_kind.in_(AUTHORITATIVE_SNAPSHOT_KINDS),
                    snapshot_effective <= start,
                )
                .order_by(snapshot_effective.desc(), PolymarketBookSnapshot.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        snapshots = (
            await session.execute(
                select(PolymarketBookSnapshot)
                .where(
                    PolymarketBookSnapshot.asset_id == context.asset_id,
                    PolymarketBookSnapshot.source_kind.in_(AUTHORITATIVE_SNAPSHOT_KINDS),
                    snapshot_effective > start,
                    snapshot_effective <= end,
                )
                .order_by(snapshot_effective.asc(), PolymarketBookSnapshot.id.asc())
            )
        ).scalars().all()

        delta_effective = func.coalesce(
            PolymarketBookDelta.event_ts_exchange,
            PolymarketBookDelta.recv_ts_local,
            PolymarketBookDelta.ingest_ts_db,
        )
        delta_query = (
            select(PolymarketBookDelta)
            .where(
                PolymarketBookDelta.asset_id == context.asset_id,
                delta_effective <= end,
            )
            .order_by(
                delta_effective.asc(),
                PolymarketBookDelta.raw_event_id.asc(),
                PolymarketBookDelta.delta_index.asc(),
                PolymarketBookDelta.id.asc(),
            )
        )
        if seed_snapshot is not None and seed_snapshot.raw_event_id is not None:
            delta_query = delta_query.where(PolymarketBookDelta.raw_event_id > seed_snapshot.raw_event_id)
        deltas = (await session.execute(delta_query)).scalars().all()

        bbo_effective = func.coalesce(
            PolymarketBboEvent.event_ts_exchange,
            PolymarketBboEvent.recv_ts_local,
            PolymarketBboEvent.ingest_ts_db,
        )
        bbo_rows = (
            await session.execute(
                select(PolymarketBboEvent)
                .where(
                    PolymarketBboEvent.asset_id == context.asset_id,
                    bbo_effective > start,
                    bbo_effective <= end,
                )
                .order_by(bbo_effective.asc(), PolymarketBboEvent.id.asc())
            )
        ).scalars().all()

        trade_effective = func.coalesce(PolymarketTradeTape.event_ts_exchange, PolymarketTradeTape.observed_at_local)
        trade_rows = (
            await session.execute(
                select(PolymarketTradeTape)
                .where(
                    PolymarketTradeTape.asset_id == context.asset_id,
                    trade_effective > start,
                    trade_effective <= end,
                )
                .order_by(trade_effective.asc(), PolymarketTradeTape.id.asc())
            )
        ).scalars().all()

        param_effective = func.coalesce(PolymarketMarketParamHistory.effective_at_exchange, PolymarketMarketParamHistory.observed_at_local)
        param_rows = (
            await session.execute(
                select(PolymarketMarketParamHistory)
                .where(
                    or_(
                        PolymarketMarketParamHistory.asset_id == context.asset_id,
                        (
                            PolymarketMarketParamHistory.asset_id.is_(None)
                            & (PolymarketMarketParamHistory.condition_id == context.condition_id)
                        ),
                    ),
                    param_effective <= end,
                )
                .order_by(param_effective.asc(), PolymarketMarketParamHistory.id.asc())
            )
        ).scalars().all()

        incident_rows = (
            await session.execute(
                select(PolymarketBookReconIncident)
                .where(
                    PolymarketBookReconIncident.asset_id == context.asset_id,
                    PolymarketBookReconIncident.incident_type.in_(tuple(DRIFT_INCIDENT_TYPES)),
                    func.coalesce(PolymarketBookReconIncident.exchange_ts, PolymarketBookReconIncident.observed_at_local) > start,
                    func.coalesce(PolymarketBookReconIncident.exchange_ts, PolymarketBookReconIncident.observed_at_local) <= end,
                )
                .order_by(
                    func.coalesce(PolymarketBookReconIncident.exchange_ts, PolymarketBookReconIncident.observed_at_local).asc(),
                    PolymarketBookReconIncident.created_at.asc(),
                )
            )
        ).scalars().all()

        event_items: list[tuple[tuple[Any, ...], str, Any]] = []
        if seed_snapshot is not None:
            event_items.append(
                (self._event_sort_key(_effective_time_for_snapshot(seed_snapshot), seed_snapshot.raw_event_id, 0, 0, seed_snapshot.id), "snapshot", seed_snapshot)
            )
        for row in snapshots:
            event_items.append(
                (self._event_sort_key(_effective_time_for_snapshot(row), row.raw_event_id, 0, 0, row.id), "snapshot", row)
            )
        for row in deltas:
            event_items.append(
                (self._event_sort_key(_effective_time_for_delta(row), row.raw_event_id, row.delta_index, 1, row.id), "delta", row)
            )
        for row in bbo_rows:
            event_items.append(
                (self._event_sort_key(_effective_time_for_bbo(row), row.raw_event_id, 0, 2, row.id), "bbo", row)
            )
        for row in trade_rows:
            event_items.append(
                (self._event_sort_key(_effective_time_for_trade(row), row.raw_event_id, 0, 3, row.id), "trade", row)
            )
        for row in param_rows:
            event_items.append(
                (self._event_sort_key(_effective_time_for_param(row), row.raw_event_id, 0, 4, row.id), "param", row)
            )
        for row in incident_rows:
            event_items.append(
                (self._event_sort_key(_effective_time_for_incident(row), row.raw_event_id, 0, 5, str(row.id)), "incident", row)
            )

        event_items.sort(key=lambda item: item[0])

        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        trustworthy_seed = False
        drift_active = False
        tick_size = None
        last_snapshot_id: int | None = None
        last_snapshot_hash: str | None = None
        last_snapshot_raw_event_id: int | None = None
        last_applied_raw_event_id: int | None = None
        latest_observed_time: datetime | None = None
        markers: list[ReplayMarker] = []
        delta_flows: list[DeltaFlowObservation] = []
        trades: list[TradeObservation] = []
        bbo_events: list[BboObservation] = []
        snapshot_boundaries: list[SnapshotBoundary] = []
        drift_times: list[datetime] = []
        partial_event_times: list[datetime] = []

        for _, event_kind, row in event_items:
            if event_kind == "param":
                effective_time = _effective_time_for_param(row)
                tick_size = row.tick_size or tick_size
                latest_observed_time = effective_time
                if effective_time >= start:
                    markers.append(
                        _marker_from_runtime(
                            exchange_time=effective_time,
                            observed_at_local=_coalesce_time(row.observed_at_local, row.created_at) or effective_time,
                            bids=bids,
                            asks=asks,
                            tick_size=tick_size,
                            trustworthy_seed=trustworthy_seed,
                            affected_by_drift=drift_active,
                            last_snapshot_id=last_snapshot_id,
                            last_snapshot_hash=last_snapshot_hash,
                            last_applied_raw_event_id=last_applied_raw_event_id,
                        )
                    )
                continue

            if event_kind == "snapshot":
                effective_time = _effective_time_for_snapshot(row)
                bids = _book_levels_from_json(row.bids_json)
                asks = _book_levels_from_json(row.asks_json)
                trustworthy_seed = True
                drift_active = False
                tick_size = row.tick_size or tick_size
                last_snapshot_id = row.id
                last_snapshot_hash = row.book_hash
                last_snapshot_raw_event_id = row.raw_event_id
                last_applied_raw_event_id = row.raw_event_id
                latest_observed_time = effective_time
                snapshot_boundaries.append(SnapshotBoundary(exchange_time=effective_time, source_kind=row.source_kind))
                markers.append(
                    _marker_from_runtime(
                        exchange_time=effective_time,
                        observed_at_local=_coalesce_time(row.observed_at_local, row.recv_ts_local, row.created_at) or effective_time,
                        bids=bids,
                        asks=asks,
                        tick_size=tick_size,
                        trustworthy_seed=trustworthy_seed,
                        affected_by_drift=drift_active,
                        last_snapshot_id=last_snapshot_id,
                        last_snapshot_hash=last_snapshot_hash,
                        last_applied_raw_event_id=last_applied_raw_event_id,
                    )
                )
                continue

            if event_kind == "delta":
                effective_time = _effective_time_for_delta(row)
                latest_observed_time = effective_time
                observed_at_local = _coalesce_time(row.recv_ts_local, row.ingest_ts_db, row.created_at) or effective_time
                if last_snapshot_raw_event_id is not None and row.raw_event_id <= last_snapshot_raw_event_id:
                    continue
                if not trustworthy_seed:
                    if effective_time >= start:
                        partial_event_times.append(effective_time)
                        markers.append(
                            _marker_from_runtime(
                                exchange_time=effective_time,
                                observed_at_local=observed_at_local,
                                bids=bids,
                                asks=asks,
                                tick_size=tick_size,
                                trustworthy_seed=False,
                                affected_by_drift=drift_active,
                                last_snapshot_id=last_snapshot_id,
                                last_snapshot_hash=last_snapshot_hash,
                                last_applied_raw_event_id=last_applied_raw_event_id,
                            )
                        )
                    continue

                book = bids if str(row.side).upper() == "BUY" else asks
                previous_size = book.get(row.price, Decimal("0"))
                new_size = row.size
                add_volume = Decimal("0")
                remove_volume = Decimal("0")
                if new_size > previous_size:
                    add_volume = new_size - previous_size
                elif new_size < previous_size:
                    remove_volume = previous_size - new_size
                if new_size <= 0:
                    book.pop(row.price, None)
                else:
                    book[row.price] = new_size
                last_applied_raw_event_id = row.raw_event_id
                if effective_time >= start:
                    delta_flows.append(
                        DeltaFlowObservation(
                            exchange_time=effective_time,
                            observed_at_local=observed_at_local,
                            raw_event_id=row.raw_event_id,
                            side=str(row.side).upper(),
                            add_volume=add_volume,
                            remove_volume=remove_volume,
                        )
                    )
                    markers.append(
                        _marker_from_runtime(
                            exchange_time=effective_time,
                            observed_at_local=observed_at_local,
                            bids=bids,
                            asks=asks,
                            tick_size=tick_size,
                            trustworthy_seed=True,
                            affected_by_drift=drift_active,
                            last_snapshot_id=last_snapshot_id,
                            last_snapshot_hash=last_snapshot_hash,
                            last_applied_raw_event_id=last_applied_raw_event_id,
                        )
                    )
                continue

            if event_kind == "bbo":
                effective_time = _effective_time_for_bbo(row)
                latest_observed_time = effective_time
                observed_at_local = _coalesce_time(row.recv_ts_local, row.ingest_ts_db, row.created_at) or effective_time
                bbo_events.append(
                    BboObservation(
                        exchange_time=effective_time,
                        observed_at_local=observed_at_local,
                        raw_event_id=row.raw_event_id,
                        best_bid=row.best_bid,
                        best_ask=row.best_ask,
                    )
                )
                markers.append(
                    _marker_from_runtime(
                        exchange_time=effective_time,
                        observed_at_local=observed_at_local,
                        bids=bids,
                        asks=asks,
                        tick_size=tick_size,
                        trustworthy_seed=trustworthy_seed,
                        affected_by_drift=drift_active,
                        last_snapshot_id=last_snapshot_id,
                        last_snapshot_hash=last_snapshot_hash,
                        last_applied_raw_event_id=last_applied_raw_event_id,
                    )
                )
                continue

            if event_kind == "trade":
                effective_time = _effective_time_for_trade(row)
                latest_observed_time = effective_time
                observed_at_local = _coalesce_time(row.observed_at_local, row.recv_ts_local, row.created_at) or effective_time
                trades.append(
                    TradeObservation(
                        exchange_time=effective_time,
                        observed_at_local=observed_at_local,
                        raw_event_id=row.raw_event_id,
                        side=str(row.side).upper() if row.side else None,
                        price=row.price,
                        size=row.size,
                    )
                )
                markers.append(
                    _marker_from_runtime(
                        exchange_time=effective_time,
                        observed_at_local=observed_at_local,
                        bids=bids,
                        asks=asks,
                        tick_size=tick_size,
                        trustworthy_seed=trustworthy_seed,
                        affected_by_drift=drift_active,
                        last_snapshot_id=last_snapshot_id,
                        last_snapshot_hash=last_snapshot_hash,
                        last_applied_raw_event_id=last_applied_raw_event_id,
                    )
                )
                continue

            if event_kind == "incident":
                effective_time = _effective_time_for_incident(row)
                latest_observed_time = effective_time
                drift_active = True
                drift_times.append(effective_time)
                markers.append(
                    _marker_from_runtime(
                        exchange_time=effective_time,
                        observed_at_local=_coalesce_time(row.observed_at_local, row.created_at) or effective_time,
                        bids=bids,
                        asks=asks,
                        tick_size=tick_size,
                        trustworthy_seed=trustworthy_seed,
                        affected_by_drift=True,
                        last_snapshot_id=last_snapshot_id,
                        last_snapshot_hash=last_snapshot_hash,
                        last_applied_raw_event_id=last_applied_raw_event_id,
                    )
                )

        markers.sort(key=lambda item: item.exchange_time)
        return ReplayData(
            context=context,
            markers=markers,
            marker_times=[item.exchange_time for item in markers],
            delta_flows=delta_flows,
            trades=trades,
            bbo_events=bbo_events,
            snapshot_boundaries=snapshot_boundaries,
            drift_times=drift_times,
            partial_event_times=partial_event_times,
            latest_observed_time=latest_observed_time,
        )

    def _event_sort_key(
        self,
        exchange_time: datetime,
        raw_event_id: int | None,
        delta_index: int,
        kind_priority: int,
        stable_id: Any,
    ) -> tuple[Any, ...]:
        return (
            exchange_time,
            raw_event_id is None,
            raw_event_id if raw_event_id is not None else 2**63 - 1,
            delta_index,
            kind_priority,
            stable_id,
        )

    def _marker_as_of(self, replay: ReplayData, point_in_time: datetime) -> ReplayMarker | None:
        index = bisect_right(replay.marker_times, point_in_time) - 1
        if index < 0:
            return None
        return replay.markers[index]

    async def _materialize_asset(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        start: datetime,
        end: datetime,
        feature_run_id: uuid.UUID,
    ) -> dict[str, int]:
        latest_observed_time = replay.latest_observed_time
        if latest_observed_time is None:
            return {
                "book_state": 0,
                "features_100ms": 0,
                "features_1s": 0,
                "alpha_labels": 0,
                "passive_fill_labels": 0,
            }

        book_state_count = 0
        feature_100ms_count = 0
        feature_1s_count = 0
        feature_rows_by_table: dict[str, list[Any]] = {name: [] for name in FEATURE_TABLE_NAME_BY_BUCKET.values()}

        for bucket_width_ms in settings.polymarket_feature_bucket_values_ms:
            book_state_rows, inserted_count = await self._materialize_book_state_rows(
                session,
                replay=replay,
                bucket_width_ms=bucket_width_ms,
                start=start,
                end=latest_observed_time,
            )
            book_state_count += inserted_count
            feature_rows, inserted_feature_count = await self._materialize_feature_rows(
                session,
                replay=replay,
                book_state_rows=book_state_rows,
                bucket_width_ms=bucket_width_ms,
                start=start,
                end=latest_observed_time,
                feature_run_id=feature_run_id,
            )
            feature_rows_by_table[FEATURE_TABLE_NAME_BY_BUCKET[bucket_width_ms]] = feature_rows
            if bucket_width_ms == 100:
                feature_100ms_count += inserted_feature_count
            elif bucket_width_ms == 1000:
                feature_1s_count += inserted_feature_count

        label_counts = await self._materialize_labels(
            session,
            replay=replay,
            feature_rows_by_table=feature_rows_by_table,
        )
        return {
            "book_state": book_state_count,
            "features_100ms": feature_100ms_count,
            "features_1s": feature_1s_count,
            "alpha_labels": label_counts["alpha"],
            "passive_fill_labels": label_counts["passive_fill"],
        }

    async def _materialize_book_state_rows(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        bucket_width_ms: int,
        start: datetime,
        end: datetime,
    ) -> tuple[dict[datetime, PolymarketBookStateTopN], int]:
        contexts = replay.context
        rows: dict[datetime, PolymarketBookStateTopN] = {}
        inserted_count = 0
        for bucket_start in _bucket_starts(start, end, bucket_width_ms):
            marker = self._marker_as_of(replay, bucket_start)
            if marker is None:
                continue
            flags = _flags_with_partial(
                {
                    "trustworthy_seed": marker.trustworthy_seed,
                    "affected_by_drift": marker.affected_by_drift,
                    "crossed_snapshot_boundary": False,
                    "crossed_resync_boundary": False,
                    "delta_without_seed": any(time <= bucket_start for time in replay.partial_event_times),
                }
            )
            existing = (
                await session.execute(
                    select(PolymarketBookStateTopN)
                    .where(
                        PolymarketBookStateTopN.asset_id == contexts.asset_id,
                        PolymarketBookStateTopN.bucket_start_exchange == bucket_start,
                        PolymarketBookStateTopN.bucket_width_ms == bucket_width_ms,
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            row = existing or PolymarketBookStateTopN(
                market_dim_id=contexts.market_dim_id,
                asset_dim_id=contexts.asset_dim_id,
                condition_id=contexts.condition_id,
                asset_id=contexts.asset_id,
                bucket_start_exchange=bucket_start,
                bucket_width_ms=bucket_width_ms,
                recon_state_id=contexts.recon_state_id,
                bid_levels_json=[],
                ask_levels_json=[],
            )
            if existing is None:
                session.add(row)
                inserted_count += 1
            row.market_dim_id = contexts.market_dim_id
            row.asset_dim_id = contexts.asset_dim_id
            row.condition_id = contexts.condition_id
            row.asset_id = contexts.asset_id
            row.observed_at_local = marker.observed_at_local
            row.recon_state_id = contexts.recon_state_id
            row.last_snapshot_id = marker.last_snapshot_id
            row.last_snapshot_hash = marker.last_snapshot_hash
            row.last_applied_raw_event_id = marker.last_applied_raw_event_id
            row.best_bid = marker.best_bid
            row.best_ask = marker.best_ask
            row.spread = marker.spread
            row.mid = marker.mid
            row.microprice = marker.microprice
            row.bid_levels_json = _serialize_levels(marker.bid_levels)
            row.ask_levels_json = _serialize_levels(marker.ask_levels)
            row.bid_depth_top1 = marker.bid_depth_top1
            row.bid_depth_top3 = marker.bid_depth_top3
            row.bid_depth_top5 = marker.bid_depth_top5
            row.ask_depth_top1 = marker.ask_depth_top1
            row.ask_depth_top3 = marker.ask_depth_top3
            row.ask_depth_top5 = marker.ask_depth_top5
            row.imbalance_top1 = marker.imbalance_top1
            row.imbalance_top3 = marker.imbalance_top3
            row.imbalance_top5 = marker.imbalance_top5
            row.completeness_flags_json = flags
            rows[bucket_start] = row
        await session.flush()
        return rows, inserted_count

    async def _materialize_feature_rows(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        book_state_rows: dict[datetime, PolymarketBookStateTopN],
        bucket_width_ms: int,
        start: datetime,
        end: datetime,
        feature_run_id: uuid.UUID,
    ) -> tuple[list[Any], int]:
        model = FEATURE_TABLE_BY_BUCKET.get(bucket_width_ms)
        if model is None:
            return [], 0
        contexts = replay.context
        inserted_count = 0
        rows: list[Any] = []
        bucket_end_limit = end - timedelta(milliseconds=bucket_width_ms)
        for bucket_start in _bucket_starts(start, bucket_end_limit, bucket_width_ms):
            bucket_end = bucket_start + timedelta(milliseconds=bucket_width_ms)
            marker = self._marker_as_of(replay, bucket_start)
            if marker is None:
                continue
            flow_summary = self._flow_summary(replay=replay, bucket_start=bucket_start, bucket_end=bucket_end)
            book_state = book_state_rows.get(bucket_start)
            flags = _flags_with_partial(
                {
                    "trustworthy_seed": marker.trustworthy_seed,
                    "affected_by_drift": marker.affected_by_drift or flow_summary["affected_by_drift"],
                    "crossed_snapshot_boundary": flow_summary["crossed_snapshot_boundary"],
                    "crossed_resync_boundary": flow_summary["crossed_resync_boundary"],
                    "delta_without_seed": flow_summary["delta_without_seed"],
                    "missing_trade_side": flow_summary["missing_trade_side"],
                }
            )
            existing = (
                await session.execute(
                    select(model)
                    .where(
                        model.asset_id == contexts.asset_id,
                        model.bucket_start_exchange == bucket_start,
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            row = existing or model(
                market_dim_id=contexts.market_dim_id,
                asset_dim_id=contexts.asset_dim_id,
                condition_id=contexts.condition_id,
                asset_id=contexts.asset_id,
                bucket_start_exchange=bucket_start,
            )
            if existing is None:
                session.add(row)
                inserted_count += 1
            row.market_dim_id = contexts.market_dim_id
            row.asset_dim_id = contexts.asset_dim_id
            row.condition_id = contexts.condition_id
            row.asset_id = contexts.asset_id
            row.bucket_start_exchange = bucket_start
            row.bucket_end_exchange = bucket_end
            row.observed_at_local = max(marker.observed_at_local, flow_summary["observed_at_local"])
            row.source_book_state_id = book_state.id if book_state is not None else None
            row.run_id = feature_run_id
            row.best_bid = marker.best_bid
            row.best_ask = marker.best_ask
            row.spread = marker.spread
            row.mid = marker.mid
            row.microprice = marker.microprice
            row.tick_size = marker.tick_size
            row.bid_depth_top1 = marker.bid_depth_top1
            row.ask_depth_top1 = marker.ask_depth_top1
            row.bid_depth_top3 = marker.bid_depth_top3
            row.ask_depth_top3 = marker.ask_depth_top3
            row.bid_depth_top5 = marker.bid_depth_top5
            row.ask_depth_top5 = marker.ask_depth_top5
            row.imbalance_top1 = marker.imbalance_top1
            row.imbalance_top3 = marker.imbalance_top3
            row.imbalance_top5 = marker.imbalance_top5
            row.bid_add_volume = flow_summary["bid_add_volume"]
            row.ask_add_volume = flow_summary["ask_add_volume"]
            row.bid_remove_volume = flow_summary["bid_remove_volume"]
            row.ask_remove_volume = flow_summary["ask_remove_volume"]
            row.buy_trade_volume = flow_summary["buy_trade_volume"]
            row.sell_trade_volume = flow_summary["sell_trade_volume"]
            row.buy_trade_count = flow_summary["buy_trade_count"]
            row.sell_trade_count = flow_summary["sell_trade_count"]
            row.trade_notional = flow_summary["trade_notional"]
            row.last_trade_price = flow_summary["last_trade_price"]
            row.last_trade_side = flow_summary["last_trade_side"]
            row.book_update_count = flow_summary["book_update_count"]
            row.bbo_update_count = flow_summary["bbo_update_count"]
            row.completeness_flags_json = flags
            rows.append(row)
        await session.flush()
        return rows, inserted_count

    def _flow_summary(self, *, replay: ReplayData, bucket_start: datetime, bucket_end: datetime) -> dict[str, Any]:
        bid_add_volume = Decimal("0")
        ask_add_volume = Decimal("0")
        bid_remove_volume = Decimal("0")
        ask_remove_volume = Decimal("0")
        buy_trade_volume = Decimal("0")
        sell_trade_volume = Decimal("0")
        buy_trade_count = 0
        sell_trade_count = 0
        trade_notional = Decimal("0")
        last_trade_price: Decimal | None = None
        last_trade_side: str | None = None
        book_raw_events: set[int] = set()
        bbo_count = 0
        affected_by_drift = False
        crossed_snapshot_boundary = False
        crossed_resync_boundary = False
        delta_without_seed = False
        missing_trade_side = False
        observed_at_local = bucket_start

        for observation in replay.delta_flows:
            if not (bucket_start < observation.exchange_time <= bucket_end):
                continue
            observed_at_local = max(observed_at_local, observation.observed_at_local)
            book_raw_events.add(observation.raw_event_id)
            if observation.side == "BUY":
                bid_add_volume += observation.add_volume
                bid_remove_volume += observation.remove_volume
            elif observation.side == "SELL":
                ask_add_volume += observation.add_volume
                ask_remove_volume += observation.remove_volume

        for observation in replay.trades:
            if not (bucket_start < observation.exchange_time <= bucket_end):
                continue
            observed_at_local = max(observed_at_local, observation.observed_at_local)
            trade_notional += observation.price * observation.size
            last_trade_price = observation.price
            last_trade_side = observation.side
            if observation.side == "BUY":
                buy_trade_volume += observation.size
                buy_trade_count += 1
            elif observation.side == "SELL":
                sell_trade_volume += observation.size
                sell_trade_count += 1
            else:
                missing_trade_side = True

        for observation in replay.bbo_events:
            if not (bucket_start < observation.exchange_time <= bucket_end):
                continue
            observed_at_local = max(observed_at_local, observation.observed_at_local)
            bbo_count += 1

        for boundary in replay.snapshot_boundaries:
            if bucket_start < boundary.exchange_time <= bucket_end:
                crossed_snapshot_boundary = True
                if boundary.source_kind != "ws_book":
                    crossed_resync_boundary = True

        for drift_time in replay.drift_times:
            if bucket_start < drift_time <= bucket_end:
                affected_by_drift = True
                break

        for partial_time in replay.partial_event_times:
            if bucket_start < partial_time <= bucket_end:
                delta_without_seed = True
                break

        return {
            "bid_add_volume": bid_add_volume,
            "ask_add_volume": ask_add_volume,
            "bid_remove_volume": bid_remove_volume,
            "ask_remove_volume": ask_remove_volume,
            "buy_trade_volume": buy_trade_volume,
            "sell_trade_volume": sell_trade_volume,
            "buy_trade_count": buy_trade_count,
            "sell_trade_count": sell_trade_count,
            "trade_notional": trade_notional,
            "last_trade_price": last_trade_price,
            "last_trade_side": last_trade_side,
            "book_update_count": len(book_raw_events),
            "bbo_update_count": bbo_count,
            "affected_by_drift": affected_by_drift,
            "crossed_snapshot_boundary": crossed_snapshot_boundary,
            "crossed_resync_boundary": crossed_resync_boundary,
            "delta_without_seed": delta_without_seed,
            "missing_trade_side": missing_trade_side,
            "observed_at_local": observed_at_local,
        }

    async def _materialize_labels(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        feature_rows_by_table: dict[str, list[Any]],
    ) -> dict[str, int]:
        alpha_count = 0
        passive_fill_count = 0
        for table_name, rows in feature_rows_by_table.items():
            for row in rows:
                for horizon_ms in settings.polymarket_label_horizon_values_ms:
                    horizon_end = row.bucket_start_exchange + timedelta(milliseconds=horizon_ms)
                    if replay.latest_observed_time is None or replay.latest_observed_time < horizon_end:
                        continue
                    end_marker = self._marker_as_of(replay, horizon_end)
                    alpha_count += await self._upsert_alpha_label(
                        session,
                        replay=replay,
                        row=row,
                        table_name=table_name,
                        horizon_ms=horizon_ms,
                        end_marker=end_marker,
                    )
                    passive_fill_count += await self._upsert_passive_fill_labels(
                        session,
                        replay=replay,
                        row=row,
                        table_name=table_name,
                        horizon_ms=horizon_ms,
                        end_time=horizon_end,
                    )
        await session.flush()
        return {"alpha": alpha_count, "passive_fill": passive_fill_count}

    async def _upsert_alpha_label(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        row: Any,
        table_name: str,
        horizon_ms: int,
        end_marker: ReplayMarker | None,
    ) -> int:
        existing = (
            await session.execute(
                select(PolymarketAlphaLabel)
                .where(
                    PolymarketAlphaLabel.asset_id == replay.context.asset_id,
                    PolymarketAlphaLabel.anchor_bucket_start_exchange == row.bucket_start_exchange,
                    PolymarketAlphaLabel.horizon_ms == horizon_ms,
                    PolymarketAlphaLabel.source_feature_table == table_name,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        label = existing or PolymarketAlphaLabel(
            market_dim_id=replay.context.market_dim_id,
            asset_dim_id=replay.context.asset_dim_id,
            condition_id=replay.context.condition_id,
            asset_id=replay.context.asset_id,
            anchor_bucket_start_exchange=row.bucket_start_exchange,
            horizon_ms=horizon_ms,
            source_feature_table=table_name,
            source_feature_row_id=row.id,
        )
        if existing is None:
            session.add(label)

        flags = dict(_feature_flags(row))
        flags["future_observed"] = end_marker is not None
        flags["source_coverage_partial"] = bool(flags.get("source_coverage_partial") or end_marker is None)

        label.market_dim_id = replay.context.market_dim_id
        label.asset_dim_id = replay.context.asset_dim_id
        label.condition_id = replay.context.condition_id
        label.asset_id = replay.context.asset_id
        label.anchor_bucket_start_exchange = row.bucket_start_exchange
        label.horizon_ms = horizon_ms
        label.source_feature_table = table_name
        label.source_feature_row_id = row.id
        label.start_mid = row.mid
        label.end_mid = end_marker.mid if end_marker is not None else None
        label.best_bid_change = (end_marker.best_bid - row.best_bid) if end_marker is not None and row.best_bid is not None and end_marker.best_bid is not None else None
        label.best_ask_change = (end_marker.best_ask - row.best_ask) if end_marker is not None and row.best_ask is not None and end_marker.best_ask is not None else None
        label.mid_return_bps = None
        if row.mid is not None and end_marker is not None and end_marker.mid is not None and row.mid != 0:
            label.mid_return_bps = ((end_marker.mid - row.mid) / row.mid) * Decimal("10000")
        label.mid_move_ticks = None
        if row.tick_size not in (None, Decimal("0")) and row.mid is not None and end_marker is not None and end_marker.mid is not None:
            label.mid_move_ticks = (end_marker.mid - row.mid) / row.tick_size
        label.up_move = None if end_marker is None or row.mid is None or end_marker.mid is None else end_marker.mid > row.mid
        label.down_move = None if end_marker is None or row.mid is None or end_marker.mid is None else end_marker.mid < row.mid
        label.flat_move = None if end_marker is None or row.mid is None or end_marker.mid is None else end_marker.mid == row.mid
        label.completeness_flags_json = flags
        return 1 if existing is None else 0

    async def _upsert_passive_fill_labels(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        row: Any,
        table_name: str,
        horizon_ms: int,
        end_time: datetime,
    ) -> int:
        inserted = 0
        flags = dict(_feature_flags(row))
        start_mid = row.mid
        relevant_trades = [trade for trade in replay.trades if row.bucket_start_exchange < trade.exchange_time <= end_time]
        relevant_bbo = [bbo for bbo in replay.bbo_events if row.bucket_start_exchange < bbo.exchange_time <= end_time]
        relevant_markers = [marker for marker in replay.markers if row.bucket_start_exchange < marker.exchange_time <= end_time]

        for side in LABEL_SIDES:
            posted_price = row.best_bid if side == "buy_post_best_bid" else row.best_ask
            if posted_price is None:
                continue
            existing = (
                await session.execute(
                    select(PolymarketPassiveFillLabel)
                    .where(
                        PolymarketPassiveFillLabel.asset_id == replay.context.asset_id,
                        PolymarketPassiveFillLabel.anchor_bucket_start_exchange == row.bucket_start_exchange,
                        PolymarketPassiveFillLabel.horizon_ms == horizon_ms,
                        PolymarketPassiveFillLabel.side == side,
                        PolymarketPassiveFillLabel.source_feature_table == table_name,
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            label = existing or PolymarketPassiveFillLabel(
                market_dim_id=replay.context.market_dim_id,
                asset_dim_id=replay.context.asset_dim_id,
                condition_id=replay.context.condition_id,
                asset_id=replay.context.asset_id,
                anchor_bucket_start_exchange=row.bucket_start_exchange,
                horizon_ms=horizon_ms,
                side=side,
                posted_price=posted_price,
                source_feature_table=table_name,
                source_feature_row_id=row.id,
            )
            if existing is None:
                session.add(label)

            touch_time: datetime | None = None
            touch_observed = False
            trade_through_observed = False
            improved_against_order = False

            for bbo in relevant_bbo:
                if side == "buy_post_best_bid":
                    if bbo.best_ask is not None and bbo.best_ask <= posted_price:
                        touch_observed = True
                        touch_time = touch_time or bbo.exchange_time
                    if bbo.best_bid is not None and bbo.best_bid > posted_price and not touch_observed:
                        improved_against_order = True
                else:
                    if bbo.best_bid is not None and bbo.best_bid >= posted_price:
                        touch_observed = True
                        touch_time = touch_time or bbo.exchange_time
                    if bbo.best_ask is not None and bbo.best_ask < posted_price and not touch_observed:
                        improved_against_order = True

            for trade in relevant_trades:
                if side == "buy_post_best_bid":
                    if trade.price <= posted_price:
                        touch_observed = True
                        touch_time = touch_time or trade.exchange_time
                    if trade.price < posted_price:
                        trade_through_observed = True
                else:
                    if trade.price >= posted_price:
                        touch_observed = True
                        touch_time = touch_time or trade.exchange_time
                    if trade.price > posted_price:
                        trade_through_observed = True

            for marker in relevant_markers:
                if touch_time is not None and marker.exchange_time >= touch_time:
                    continue
                if side == "buy_post_best_bid" and marker.best_bid is not None and marker.best_bid > posted_price:
                    improved_against_order = True
                elif side == "sell_post_best_ask" and marker.best_ask is not None and marker.best_ask < posted_price:
                    improved_against_order = True

            adverse_move_after_touch_bps: Decimal | None = None
            if touch_time is not None and start_mid not in (None, Decimal("0")):
                adverse_values: list[Decimal] = []
                for marker in relevant_markers:
                    if marker.exchange_time < touch_time or marker.mid is None:
                        continue
                    if side == "buy_post_best_bid" and marker.mid < start_mid:
                        adverse_values.append(((start_mid - marker.mid) / start_mid) * Decimal("10000"))
                    elif side == "sell_post_best_ask" and marker.mid > start_mid:
                        adverse_values.append(((marker.mid - start_mid) / start_mid) * Decimal("10000"))
                adverse_move_after_touch_bps = max(adverse_values) if adverse_values else Decimal("0")

            fill_flags = dict(flags)
            fill_flags["future_observed"] = True
            fill_flags["source_coverage_partial"] = bool(fill_flags.get("source_coverage_partial"))

            label.market_dim_id = replay.context.market_dim_id
            label.asset_dim_id = replay.context.asset_dim_id
            label.condition_id = replay.context.condition_id
            label.asset_id = replay.context.asset_id
            label.anchor_bucket_start_exchange = row.bucket_start_exchange
            label.horizon_ms = horizon_ms
            label.side = side
            label.posted_price = posted_price
            label.touch_observed = touch_observed
            label.trade_through_observed = trade_through_observed
            label.best_price_improved_against_order = improved_against_order
            label.adverse_move_after_touch_bps = adverse_move_after_touch_bps
            label.source_feature_table = table_name
            label.source_feature_row_id = row.id
            label.completeness_flags_json = fill_flags
            inserted += 1 if existing is None else 0
        return inserted

    async def _refresh_incomplete_bucket_gauge(self) -> None:
        since = utcnow() - timedelta(hours=24)
        incomplete_count = 0
        async with self._session_factory() as session:
            for model in (PolymarketMicrostructureFeature100ms, PolymarketMicrostructureFeature1s):
                rows = (
                    await session.execute(
                        select(model.completeness_flags_json).where(model.bucket_start_exchange >= since)
                    )
                ).scalars().all()
                incomplete_count += sum(
                    1 for value in rows if isinstance(value, dict) and value.get("source_coverage_partial")
                )
        polymarket_incomplete_bucket_count.set(incomplete_count)


async def fetch_polymarket_feature_status(session: AsyncSession) -> dict[str, Any]:
    latest_feature_success = (
        await session.execute(
            select(PolymarketFeatureRun)
            .where(
                PolymarketFeatureRun.run_type == RUN_TYPE_FEATURE,
                PolymarketFeatureRun.status.in_(SUCCESS_RUN_STATUSES),
            )
            .order_by(PolymarketFeatureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    latest_label_success = (
        await session.execute(
            select(PolymarketFeatureRun)
            .where(
                PolymarketFeatureRun.run_type == RUN_TYPE_LABEL,
                PolymarketFeatureRun.status.in_(SUCCESS_RUN_STATUSES),
            )
            .order_by(PolymarketFeatureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    recent_runs = (
        await session.execute(
            select(PolymarketFeatureRun)
            .order_by(PolymarketFeatureRun.started_at.desc())
            .limit(10)
        )
    ).scalars().all()

    since = utcnow() - timedelta(hours=24)
    recent_feature_rows = int(
        (
            await session.execute(
                select(func.count(PolymarketMicrostructureFeature100ms.id)).where(
                    PolymarketMicrostructureFeature100ms.bucket_start_exchange >= since
                )
            )
        ).scalar_one()
        or 0
    )
    recent_feature_rows += int(
        (
            await session.execute(
                select(func.count(PolymarketMicrostructureFeature1s.id)).where(
                    PolymarketMicrostructureFeature1s.bucket_start_exchange >= since
                )
            )
        ).scalar_one()
        or 0
    )
    recent_label_rows = int(
        (
            await session.execute(
                select(func.count(PolymarketAlphaLabel.id)).where(PolymarketAlphaLabel.anchor_bucket_start_exchange >= since)
            )
        ).scalar_one()
        or 0
    )
    recent_label_rows += int(
        (
            await session.execute(
                select(func.count(PolymarketPassiveFillLabel.id)).where(
                    PolymarketPassiveFillLabel.anchor_bucket_start_exchange >= since
                )
            )
        ).scalar_one()
        or 0
    )
    incomplete_count = 0
    for model in (PolymarketMicrostructureFeature100ms, PolymarketMicrostructureFeature1s):
        values = (
            await session.execute(select(model.completeness_flags_json).where(model.bucket_start_exchange >= since))
        ).scalars().all()
        incomplete_count += sum(
            1 for value in values if isinstance(value, dict) and value.get("source_coverage_partial")
        )
    polymarket_incomplete_bucket_count.set(incomplete_count)
    if latest_feature_success is not None and latest_feature_success.completed_at is not None:
        polymarket_feature_last_success_timestamp.set(latest_feature_success.completed_at.timestamp())
    if latest_label_success is not None and latest_label_success.completed_at is not None:
        polymarket_label_last_success_timestamp.set(latest_label_success.completed_at.timestamp())
    return {
        "enabled": settings.polymarket_features_enabled,
        "on_startup": settings.polymarket_features_on_startup,
        "interval_seconds": settings.polymarket_features_interval_seconds,
        "lookback_hours": settings.polymarket_features_lookback_hours,
        "bucket_widths_ms": settings.polymarket_feature_bucket_values_ms,
        "label_horizons_ms": settings.polymarket_label_horizon_values_ms,
        "max_watched_assets": settings.polymarket_features_max_watched_assets,
        "last_successful_feature_run_at": latest_feature_success.completed_at if latest_feature_success is not None else None,
        "last_successful_label_run_at": latest_label_success.completed_at if latest_label_success is not None else None,
        "recent_feature_rows_24h": recent_feature_rows,
        "recent_label_rows_24h": recent_label_rows,
        "incomplete_bucket_count_24h": incomplete_count,
        "recent_runs": [_serialize_feature_run(row) for row in recent_runs],
    }


async def list_polymarket_feature_runs(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    total = int((await session.execute(select(func.count(PolymarketFeatureRun.id)))).scalar_one() or 0)
    rows = (
        await session.execute(
            select(PolymarketFeatureRun)
            .order_by(PolymarketFeatureRun.started_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    return [_serialize_feature_run(row) for row in rows], total


def _query_with_time_bounds(query: Select[Any], *, time_column: Any, start: datetime | None, end: datetime | None) -> Select[Any]:
    if start is not None:
        query = query.where(time_column >= start)
    if end is not None:
        query = query.where(time_column <= end)
    return query


async def lookup_polymarket_book_state_topn(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    bucket_width_ms: int | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketBookStateTopN)
    if asset_id:
        query = query.where(PolymarketBookStateTopN.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookStateTopN.condition_id == condition_id)
    if bucket_width_ms is not None:
        query = query.where(PolymarketBookStateTopN.bucket_width_ms == bucket_width_ms)
    query = _query_with_time_bounds(
        query,
        time_column=PolymarketBookStateTopN.bucket_start_exchange,
        start=start,
        end=end,
    )
    rows = (
        await session.execute(
            query.order_by(PolymarketBookStateTopN.bucket_start_exchange.desc(), PolymarketBookStateTopN.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_book_state(row) for row in rows]


async def lookup_polymarket_microstructure_features(
    session: AsyncSession,
    *,
    bucket_width_ms: int,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    model = FEATURE_TABLE_BY_BUCKET.get(bucket_width_ms)
    table_name = FEATURE_TABLE_NAME_BY_BUCKET.get(bucket_width_ms)
    if model is None or table_name is None:
        return []
    query: Select[Any] = select(model)
    if asset_id:
        query = query.where(model.asset_id == asset_id)
    if condition_id:
        query = query.where(model.condition_id == condition_id)
    query = _query_with_time_bounds(query, time_column=model.bucket_start_exchange, start=start, end=end)
    rows = (
        await session.execute(query.order_by(model.bucket_start_exchange.desc(), model.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_feature_row(row, table_name=table_name) for row in rows]


async def lookup_polymarket_alpha_labels(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    horizon_ms: int | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketAlphaLabel)
    if asset_id:
        query = query.where(PolymarketAlphaLabel.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketAlphaLabel.condition_id == condition_id)
    if horizon_ms is not None:
        query = query.where(PolymarketAlphaLabel.horizon_ms == horizon_ms)
    query = _query_with_time_bounds(
        query,
        time_column=PolymarketAlphaLabel.anchor_bucket_start_exchange,
        start=start,
        end=end,
    )
    rows = (
        await session.execute(
            query.order_by(PolymarketAlphaLabel.anchor_bucket_start_exchange.desc(), PolymarketAlphaLabel.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_serialize_alpha_label(row) for row in rows]


async def lookup_polymarket_passive_fill_labels(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    horizon_ms: int | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketPassiveFillLabel)
    if asset_id:
        query = query.where(PolymarketPassiveFillLabel.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketPassiveFillLabel.condition_id == condition_id)
    if horizon_ms is not None:
        query = query.where(PolymarketPassiveFillLabel.horizon_ms == horizon_ms)
    query = _query_with_time_bounds(
        query,
        time_column=PolymarketPassiveFillLabel.anchor_bucket_start_exchange,
        start=start,
        end=end,
    )
    rows = (
        await session.execute(
            query.order_by(
                PolymarketPassiveFillLabel.anchor_bucket_start_exchange.desc(),
                PolymarketPassiveFillLabel.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [_serialize_passive_fill_label(row) for row in rows]


async def trigger_manual_polymarket_feature_materialization(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    asset_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, Any]:
    service = PolymarketMicrostructureService(session_factory)
    try:
        return await service.materialize_scope(
            reason=reason,
            asset_ids=asset_ids,
            condition_ids=condition_ids,
            start=start,
            end=end,
        )
    finally:
        await service.close()
