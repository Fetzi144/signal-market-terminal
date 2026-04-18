from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

import httpx
from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_common import (
    REST_RESYNC_CHANNEL,
    RESYNC_PROVENANCE,
    STATUS_VENUE,
    STREAM_PROVENANCE,
    parse_polymarket_timestamp,
    unique_preserving_order,
    utcnow,
)
from app.ingestion.polymarket_metadata import seed_registry_from_book_snapshot
from app.ingestion.polymarket_stream import list_watched_polymarket_assets
from app.metrics import (
    polymarket_book_snapshot_failures,
    polymarket_book_snapshot_last_success_timestamp,
    polymarket_book_snapshot_runs,
    polymarket_oi_poll_failures,
    polymarket_oi_poll_last_success_timestamp,
    polymarket_oi_poll_runs,
    polymarket_raw_projected_rows,
    polymarket_raw_projector_failures,
    polymarket_raw_projector_lag,
    polymarket_raw_projector_last_success_timestamp,
    polymarket_raw_projector_runs,
    polymarket_trade_backfill_failures,
    polymarket_trade_backfill_last_success_timestamp,
    polymarket_trade_backfill_runs,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketOpenInterestHistory,
    PolymarketRawCaptureRun,
    PolymarketTradeTape,
)
from app.models.polymarket_stream import PolymarketMarketEvent

logger = logging.getLogger(__name__)

RUN_TYPE_RAW_PROJECTOR = "raw_projector"
RUN_TYPE_BOOK_SNAPSHOT = "book_snapshot"
RUN_TYPE_TRADE_BACKFILL = "trade_backfill"
RUN_TYPE_OI_POLL = "oi_poll"

SOURCE_KIND_WS_BOOK = "ws_book"
SOURCE_KIND_REST_PERIODIC_SNAPSHOT = "rest_periodic_snapshot"
SOURCE_KIND_REST_RESYNC_SNAPSHOT = "rest_resync_snapshot"
SOURCE_KIND_REST_MANUAL_SNAPSHOT = "rest_manual_snapshot"
SOURCE_KIND_REST_SEED_SNAPSHOT = "rest_seed_snapshot"
SOURCE_KIND_WS_LAST_TRADE_PRICE = "ws_last_trade_price"
SOURCE_KIND_DATA_API_TRADES = "data_api_trades"
SOURCE_KIND_DATA_API_OI_POLL = "data_api_oi_poll"

PROJECTABLE_MESSAGE_TYPES = {"book", "price_change", "best_bid_ask", "last_trade_price"}
SUCCESSFUL_RUN_STATUSES = {"completed", "partial"}
RAW_PROJECTOR_POLL_SECONDS = 5
BOOKS_BATCH_SIZE = 500
MARKET_BATCH_SIZE = 50
DATA_API_MAX_LIMIT = 500
DATA_API_MAX_OFFSET = 1000


@dataclass(slots=True)
class RegistryRefs:
    market_dim_id: int | None
    asset_dim_id: int | None
    condition_id: str
    asset_id: str | None
    outcome_name: str | None = None
    outcome_index: int | None = None


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _normalize_side(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text.upper() if text else None


def _normalize_payload_dict(payload: Any) -> dict[str, Any] | None:
    return payload if isinstance(payload, dict) else None


def _extract_top_level(levels: Any) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(levels, list) or not levels:
        return None, None
    level = levels[0]
    if isinstance(level, dict):
        return _to_decimal(level.get("price")), _to_decimal(level.get("size"))
    if isinstance(level, (list, tuple)) and level:
        price = _to_decimal(level[0])
        size = _to_decimal(level[1]) if len(level) > 1 else None
        return price, size
    return None, None


def _compute_spread(best_bid: Decimal | None, best_ask: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None:
        return None
    return best_ask - best_bid


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _chunked(values: Sequence[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [list(values)]
    return [list(values[index:index + size]) for index in range(0, len(values), size)]


def _boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _relevant_raw_event_filter() -> Any:
    return or_(
        and_(
            PolymarketMarketEvent.provenance == STREAM_PROVENANCE,
            PolymarketMarketEvent.message_type.in_(PROJECTABLE_MESSAGE_TYPES),
        ),
        and_(
            PolymarketMarketEvent.provenance == RESYNC_PROVENANCE,
            PolymarketMarketEvent.channel == REST_RESYNC_CHANNEL,
        ),
    )


async def _resolve_registry_refs(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
) -> RegistryRefs:
    normalized_asset_id = str(asset_id) if asset_id else None
    normalized_condition_id = str(condition_id) if condition_id else None

    if normalized_asset_id is not None:
        row = (
            await session.execute(
                select(PolymarketAssetDim, PolymarketMarketDim)
                .outerjoin(PolymarketMarketDim, PolymarketAssetDim.market_dim_id == PolymarketMarketDim.id)
                .where(PolymarketAssetDim.asset_id == normalized_asset_id)
                .limit(1)
            )
        ).first()
        if row is not None:
            asset_dim, market_dim = row
            return RegistryRefs(
                market_dim_id=market_dim.id if market_dim is not None else asset_dim.market_dim_id,
                asset_dim_id=asset_dim.id,
                condition_id=asset_dim.condition_id or normalized_condition_id or normalized_asset_id,
                asset_id=asset_dim.asset_id,
                outcome_name=asset_dim.outcome_name,
                outcome_index=asset_dim.outcome_index,
            )

    if normalized_condition_id is not None:
        market_dim = (
            await session.execute(
                select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == normalized_condition_id).limit(1)
            )
        ).scalar_one_or_none()
        return RegistryRefs(
            market_dim_id=market_dim.id if market_dim is not None else None,
            asset_dim_id=None,
            condition_id=normalized_condition_id,
            asset_id=normalized_asset_id,
        )

    fallback_condition_id = normalized_asset_id or "unknown"
    return RegistryRefs(
        market_dim_id=None,
        asset_dim_id=None,
        condition_id=fallback_condition_id,
        asset_id=normalized_asset_id,
    )


async def _resolve_condition_ids_for_scope(
    session: AsyncSession,
    *,
    asset_ids: list[str] | None,
    condition_ids: list[str] | None,
) -> tuple[list[str], list[str]]:
    explicit_condition_ids = unique_preserving_order([str(value) for value in (condition_ids or []) if value])
    if explicit_condition_ids:
        return explicit_condition_ids, []

    target_asset_ids = unique_preserving_order([str(value) for value in (asset_ids or []) if value])
    if not target_asset_ids:
        target_asset_ids = await list_watched_polymarket_assets(session)
    if not target_asset_ids:
        return [], []

    rows = (
        await session.execute(
            select(PolymarketAssetDim.asset_id, PolymarketAssetDim.condition_id).where(
                PolymarketAssetDim.asset_id.in_(target_asset_ids)
            )
        )
    ).all()
    condition_by_asset = {str(asset_id): str(condition_id) for asset_id, condition_id in rows}
    resolved_condition_ids = unique_preserving_order(
        [condition_by_asset[asset_id] for asset_id in target_asset_ids if asset_id in condition_by_asset]
    )
    unresolved_assets = [asset_id for asset_id in target_asset_ids if asset_id not in condition_by_asset]
    return resolved_condition_ids, unresolved_assets


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _serialize_capture_run(run: PolymarketRawCaptureRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "run_type": run.run_type,
        "reason": run.reason,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "status": run.status,
        "scope_json": run.scope_json,
        "cursor_json": run.cursor_json,
        "rows_inserted_json": run.rows_inserted_json,
        "error_count": run.error_count,
        "details_json": run.details_json,
    }


def _serialize_book_snapshot(row: PolymarketBookSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "source_kind": row.source_kind,
        "event_ts_exchange": row.event_ts_exchange,
        "recv_ts_local": row.recv_ts_local,
        "ingest_ts_db": row.ingest_ts_db,
        "observed_at_local": row.observed_at_local,
        "stream_session_id": row.stream_session_id,
        "raw_event_id": row.raw_event_id,
        "capture_run_id": row.capture_run_id,
        "book_hash": row.book_hash,
        "bids_json": row.bids_json,
        "asks_json": row.asks_json,
        "min_order_size": _serialize_decimal(row.min_order_size),
        "tick_size": _serialize_decimal(row.tick_size),
        "neg_risk": row.neg_risk,
        "last_trade_price": _serialize_decimal(row.last_trade_price),
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "spread": _serialize_decimal(row.spread),
        "fingerprint": row.fingerprint,
        "source_payload_json": row.source_payload_json,
        "created_at": row.created_at,
    }


def _serialize_book_delta(row: PolymarketBookDelta) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "event_ts_exchange": row.event_ts_exchange,
        "recv_ts_local": row.recv_ts_local,
        "ingest_ts_db": row.ingest_ts_db,
        "stream_session_id": row.stream_session_id,
        "raw_event_id": row.raw_event_id,
        "delta_index": row.delta_index,
        "price": _serialize_decimal(row.price),
        "size": _serialize_decimal(row.size),
        "side": row.side,
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "delta_hash": row.delta_hash,
        "source_payload_json": row.source_payload_json,
        "created_at": row.created_at,
    }


def _serialize_bbo_event(row: PolymarketBboEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "event_ts_exchange": row.event_ts_exchange,
        "recv_ts_local": row.recv_ts_local,
        "ingest_ts_db": row.ingest_ts_db,
        "stream_session_id": row.stream_session_id,
        "raw_event_id": row.raw_event_id,
        "best_bid": _serialize_decimal(row.best_bid),
        "best_ask": _serialize_decimal(row.best_ask),
        "spread": _serialize_decimal(row.spread),
        "source_payload_json": row.source_payload_json,
        "created_at": row.created_at,
    }


def _serialize_trade_tape(row: PolymarketTradeTape) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "source_kind": row.source_kind,
        "event_ts_exchange": row.event_ts_exchange,
        "recv_ts_local": row.recv_ts_local,
        "ingest_ts_db": row.ingest_ts_db,
        "observed_at_local": row.observed_at_local,
        "stream_session_id": row.stream_session_id,
        "raw_event_id": row.raw_event_id,
        "capture_run_id": row.capture_run_id,
        "transaction_hash": row.transaction_hash,
        "side": row.side,
        "price": _serialize_decimal(row.price),
        "size": _serialize_decimal(row.size),
        "fee_rate_bps": _serialize_decimal(row.fee_rate_bps),
        "event_slug": row.event_slug,
        "outcome_name": row.outcome_name,
        "outcome_index": row.outcome_index,
        "proxy_wallet": row.proxy_wallet,
        "details_json": row.details_json,
        "source_payload_json": row.source_payload_json,
        "fingerprint": row.fingerprint,
        "fallback_fingerprint": row.fallback_fingerprint,
        "created_at": row.created_at,
    }


def _serialize_open_interest(row: PolymarketOpenInterestHistory) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "condition_id": row.condition_id,
        "source_kind": row.source_kind,
        "observed_at_local": row.observed_at_local,
        "capture_run_id": row.capture_run_id,
        "value": _serialize_decimal(row.value),
        "source_payload_json": row.source_payload_json,
        "created_at": row.created_at,
    }


async def _insert_book_snapshot_row(
    session: AsyncSession,
    *,
    refs: RegistryRefs,
    source_kind: str,
    event_ts_exchange: datetime | None,
    recv_ts_local: datetime | None,
    ingest_ts_db: datetime,
    observed_at_local: datetime,
    stream_session_id: uuid.UUID | None,
    raw_event_id: int | None,
    capture_run_id: uuid.UUID | None,
    payload: dict[str, Any],
) -> bool:
    if raw_event_id is not None:
        existing = (
            await session.execute(
                select(PolymarketBookSnapshot.id).where(PolymarketBookSnapshot.raw_event_id == raw_event_id).limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return False

    bids = payload.get("bids") if isinstance(payload.get("bids"), list) else []
    asks = payload.get("asks") if isinstance(payload.get("asks"), list) else []
    best_bid, _ = _extract_top_level(bids)
    best_ask, _ = _extract_top_level(asks)
    spread = _compute_spread(best_bid, best_ask)

    fingerprint = None
    if raw_event_id is None:
        fingerprint = _hash_payload(
            {
                "source_kind": source_kind,
                "condition_id": refs.condition_id,
                "asset_id": refs.asset_id,
                "event_ts_exchange": event_ts_exchange.isoformat() if event_ts_exchange is not None else None,
                "book_hash": payload.get("hash"),
                "bids": bids,
                "asks": asks,
                "tick_size": payload.get("tick_size"),
                "min_order_size": payload.get("min_order_size"),
                "neg_risk": payload.get("neg_risk"),
                "last_trade_price": _coalesce(payload.get("last_trade_price"), payload.get("lastTradePrice")),
            }
        )
        existing = (
            await session.execute(
                select(PolymarketBookSnapshot.id).where(PolymarketBookSnapshot.fingerprint == fingerprint).limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return False

    session.add(
        PolymarketBookSnapshot(
            market_dim_id=refs.market_dim_id,
            asset_dim_id=refs.asset_dim_id,
            condition_id=refs.condition_id,
            asset_id=refs.asset_id or refs.condition_id,
            source_kind=source_kind,
            event_ts_exchange=event_ts_exchange,
            recv_ts_local=recv_ts_local,
            ingest_ts_db=ingest_ts_db,
            observed_at_local=observed_at_local,
            stream_session_id=stream_session_id,
            raw_event_id=raw_event_id,
            capture_run_id=capture_run_id,
            book_hash=str(payload["hash"]) if payload.get("hash") is not None else None,
            bids_json=bids,
            asks_json=asks,
            min_order_size=_to_decimal(_coalesce(payload.get("min_order_size"), payload.get("minOrderSize"))),
            tick_size=_to_decimal(_coalesce(payload.get("tick_size"), payload.get("tickSize"))),
            neg_risk=_boolish(_coalesce(payload.get("neg_risk"), payload.get("negRisk"))),
            last_trade_price=_to_decimal(_coalesce(payload.get("last_trade_price"), payload.get("lastTradePrice"))),
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            fingerprint=fingerprint,
            source_payload_json=payload,
        )
    )
    return True


async def _insert_book_deltas_from_raw_event(
    session: AsyncSession,
    *,
    raw_event: PolymarketMarketEvent,
    payload: dict[str, Any],
) -> int:
    price_changes = payload.get("price_changes")
    if not isinstance(price_changes, list) or not price_changes:
        return 0

    existing_indices = set(
        (
            await session.execute(
                select(PolymarketBookDelta.delta_index).where(PolymarketBookDelta.raw_event_id == raw_event.id)
            )
        ).scalars().all()
    )

    inserted = 0
    for delta_index, change in enumerate(price_changes):
        if delta_index in existing_indices or not isinstance(change, dict):
            continue

        asset_id_value = _coalesce(change.get("asset_id"), change.get("assetId"), raw_event.asset_id, payload.get("asset_id"))
        condition_id_value = _coalesce(change.get("market"), payload.get("market"), raw_event.market_id)
        if asset_id_value is None or condition_id_value is None:
            continue

        refs = await _resolve_registry_refs(
            session,
            asset_id=str(asset_id_value),
            condition_id=str(condition_id_value),
        )
        price = _to_decimal(change.get("price"))
        size = _to_decimal(change.get("size"))
        side = _normalize_side(change.get("side"))
        if price is None or size is None or side is None:
            continue

        session.add(
            PolymarketBookDelta(
                market_dim_id=refs.market_dim_id,
                asset_dim_id=refs.asset_dim_id,
                condition_id=refs.condition_id,
                asset_id=refs.asset_id or str(asset_id_value),
                event_ts_exchange=raw_event.event_time,
                recv_ts_local=raw_event.received_at_local,
                ingest_ts_db=raw_event.created_at,
                stream_session_id=raw_event.connection_id,
                raw_event_id=raw_event.id,
                delta_index=delta_index,
                price=price,
                size=size,
                side=side,
                best_bid=_to_decimal(change.get("best_bid")),
                best_ask=_to_decimal(change.get("best_ask")),
                delta_hash=str(change["hash"]) if change.get("hash") is not None else None,
                source_payload_json=change,
            )
        )
        inserted += 1
    return inserted


async def _insert_bbo_from_raw_event(
    session: AsyncSession,
    *,
    raw_event: PolymarketMarketEvent,
    payload: dict[str, Any],
) -> bool:
    existing = (
        await session.execute(
            select(PolymarketBboEvent.id).where(PolymarketBboEvent.raw_event_id == raw_event.id).limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False

    asset_id_value = _coalesce(payload.get("asset_id"), payload.get("assetId"), raw_event.asset_id)
    condition_id_value = _coalesce(payload.get("market"), payload.get("condition_id"), raw_event.market_id)
    if asset_id_value is None or condition_id_value is None:
        return False

    refs = await _resolve_registry_refs(
        session,
        asset_id=str(asset_id_value),
        condition_id=str(condition_id_value),
    )
    session.add(
        PolymarketBboEvent(
            market_dim_id=refs.market_dim_id,
            asset_dim_id=refs.asset_dim_id,
            condition_id=refs.condition_id,
            asset_id=refs.asset_id or str(asset_id_value),
            event_ts_exchange=raw_event.event_time,
            recv_ts_local=raw_event.received_at_local,
            ingest_ts_db=raw_event.created_at,
            stream_session_id=raw_event.connection_id,
            raw_event_id=raw_event.id,
            best_bid=_to_decimal(_coalesce(payload.get("best_bid"), payload.get("bid"))),
            best_ask=_to_decimal(_coalesce(payload.get("best_ask"), payload.get("ask"))),
            spread=_to_decimal(payload.get("spread")),
            source_payload_json=payload,
        )
    )
    return True


def _trade_fingerprints(
    *,
    condition_id: str,
    asset_id: str | None,
    side: str | None,
    price: Decimal,
    size: Decimal,
    event_ts_exchange: datetime | None,
    transaction_hash: str | None,
) -> tuple[str, str]:
    fallback = _hash_payload(
        {
            "condition_id": condition_id,
            "asset_id": asset_id,
            "side": side,
            "price": str(price),
            "size": str(size),
            "event_ts_exchange": event_ts_exchange.isoformat() if event_ts_exchange is not None else None,
        }
    )
    if transaction_hash:
        return f"tx:{str(transaction_hash).lower()}", fallback
    return f"fallback:{fallback}", fallback


async def _trade_exists(
    session: AsyncSession,
    *,
    raw_event_id: int | None,
    transaction_hash: str | None,
    fingerprint: str,
    fallback_fingerprint: str,
) -> bool:
    clauses = [
        PolymarketTradeTape.fingerprint == fingerprint,
        PolymarketTradeTape.fallback_fingerprint == fallback_fingerprint,
    ]
    if raw_event_id is not None:
        clauses.append(PolymarketTradeTape.raw_event_id == raw_event_id)
    if transaction_hash:
        clauses.append(PolymarketTradeTape.transaction_hash == transaction_hash)
    existing = (
        await session.execute(select(PolymarketTradeTape.id).where(or_(*clauses)).limit(1))
    ).scalar_one_or_none()
    return existing is not None


async def _insert_trade_row(
    session: AsyncSession,
    *,
    refs: RegistryRefs,
    source_kind: str,
    event_ts_exchange: datetime | None,
    recv_ts_local: datetime | None,
    ingest_ts_db: datetime,
    observed_at_local: datetime,
    stream_session_id: uuid.UUID | None,
    raw_event_id: int | None,
    capture_run_id: uuid.UUID | None,
    transaction_hash: str | None,
    side: str | None,
    price: Decimal,
    size: Decimal,
    fee_rate_bps: Decimal | None,
    event_slug: str | None,
    outcome_name: str | None,
    outcome_index: int | None,
    proxy_wallet: str | None,
    details_json: dict[str, Any] | None,
    source_payload_json: dict[str, Any],
) -> bool:
    fingerprint, fallback_fingerprint = _trade_fingerprints(
        condition_id=refs.condition_id,
        asset_id=refs.asset_id,
        side=side,
        price=price,
        size=size,
        event_ts_exchange=event_ts_exchange,
        transaction_hash=transaction_hash,
    )
    if await _trade_exists(
        session,
        raw_event_id=raw_event_id,
        transaction_hash=transaction_hash,
        fingerprint=fingerprint,
        fallback_fingerprint=fallback_fingerprint,
    ):
        return False

    session.add(
        PolymarketTradeTape(
            market_dim_id=refs.market_dim_id,
            asset_dim_id=refs.asset_dim_id,
            condition_id=refs.condition_id,
            asset_id=refs.asset_id,
            source_kind=source_kind,
            event_ts_exchange=event_ts_exchange,
            recv_ts_local=recv_ts_local,
            ingest_ts_db=ingest_ts_db,
            observed_at_local=observed_at_local,
            stream_session_id=stream_session_id,
            raw_event_id=raw_event_id,
            capture_run_id=capture_run_id,
            transaction_hash=transaction_hash,
            side=side,
            price=price,
            size=size,
            fee_rate_bps=fee_rate_bps,
            event_slug=event_slug,
            outcome_name=outcome_name,
            outcome_index=outcome_index,
            proxy_wallet=proxy_wallet,
            details_json=details_json,
            source_payload_json=source_payload_json,
            fingerprint=fingerprint,
            fallback_fingerprint=fallback_fingerprint,
        )
    )
    return True


async def _insert_trade_from_raw_event(
    session: AsyncSession,
    *,
    raw_event: PolymarketMarketEvent,
    payload: dict[str, Any],
    capture_run_id: uuid.UUID | None,
) -> bool:
    asset_id_value = _coalesce(payload.get("asset_id"), payload.get("assetId"), raw_event.asset_id)
    condition_id_value = _coalesce(payload.get("market"), payload.get("condition_id"), raw_event.market_id)
    if condition_id_value is None:
        return False

    refs = await _resolve_registry_refs(
        session,
        asset_id=str(asset_id_value) if asset_id_value is not None else None,
        condition_id=str(condition_id_value),
    )
    price = _to_decimal(_coalesce(payload.get("price"), payload.get("last_trade_price")))
    size = _to_decimal(_coalesce(payload.get("size"), payload.get("trade_size")))
    if price is None or size is None:
        return False

    return await _insert_trade_row(
        session,
        refs=refs,
        source_kind=SOURCE_KIND_WS_LAST_TRADE_PRICE,
        event_ts_exchange=raw_event.event_time,
        recv_ts_local=raw_event.received_at_local,
        ingest_ts_db=raw_event.created_at,
        observed_at_local=raw_event.received_at_local,
        stream_session_id=raw_event.connection_id,
        raw_event_id=raw_event.id,
        capture_run_id=capture_run_id,
        transaction_hash=str(payload["transaction_hash"]) if payload.get("transaction_hash") is not None else None,
        side=_normalize_side(payload.get("side")),
        price=price,
        size=size,
        fee_rate_bps=_to_decimal(payload.get("fee_rate_bps")),
        event_slug=None,
        outcome_name=refs.outcome_name,
        outcome_index=refs.outcome_index,
        proxy_wallet=None,
        details_json=None,
        source_payload_json=payload,
    )


def _query_with_time_bounds(
    query: Select[Any],
    *,
    time_column: Any,
    start: datetime | None,
    end: datetime | None,
) -> Select[Any]:
    if start is not None:
        query = query.where(time_column >= start)
    if end is not None:
        query = query.where(time_column <= end)
    return query


class PolymarketRawStorageService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        client_factory: Any | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._client_factory = client_factory or self._default_client_factory
        self._client: httpx.AsyncClient | None = None

    def _default_client_factory(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=settings.connector_timeout_seconds)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = self._client_factory()
        return self._client

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def _create_run(
        self,
        *,
        run_type: str,
        reason: str,
        scope_json: dict[str, Any] | None = None,
        cursor_json: dict[str, Any] | None = None,
    ) -> uuid.UUID:
        run_id = uuid.uuid4()
        async with self._session_factory() as session:
            session.add(
                PolymarketRawCaptureRun(
                    id=run_id,
                    run_type=run_type,
                    reason=reason,
                    status="running",
                    scope_json=scope_json,
                    cursor_json=cursor_json,
                )
            )
            await session.commit()
        return run_id

    async def _finalize_run(
        self,
        *,
        run_id: uuid.UUID,
        status: str,
        cursor_json: dict[str, Any] | None = None,
        rows_inserted_json: dict[str, Any] | None = None,
        error_count: int = 0,
        details_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            run = await session.get(PolymarketRawCaptureRun, run_id)
            assert run is not None
            run.completed_at = utcnow()
            run.status = status
            run.cursor_json = cursor_json
            run.rows_inserted_json = rows_inserted_json
            run.error_count = error_count
            run.details_json = details_json
            await session.commit()
            return _serialize_capture_run(run)

    async def _latest_successful_run(
        self,
        session: AsyncSession,
        *,
        run_type: str,
    ) -> PolymarketRawCaptureRun | None:
        return (
            await session.execute(
                select(PolymarketRawCaptureRun)
                .where(
                    PolymarketRawCaptureRun.run_type == run_type,
                    PolymarketRawCaptureRun.status.in_(SUCCESSFUL_RUN_STATUSES),
                )
                .order_by(PolymarketRawCaptureRun.started_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _normalize_books_response(self, data: Any, asset_ids: list[str]) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                return [row for row in data["data"] if isinstance(row, dict)]
            rows: list[dict[str, Any]] = []
            for asset_id in asset_ids:
                row = data.get(asset_id)
                if isinstance(row, dict):
                    row.setdefault("asset_id", asset_id)
                    rows.append(row)
            return rows
        return []

    async def _fetch_books_batch(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        client = await self._get_client()
        response = await client.post(
            f"{settings.polymarket_api_base}/books",
            json=[{"token_id": asset_id} for asset_id in asset_ids],
        )
        response.raise_for_status()
        return await self._normalize_books_response(response.json(), asset_ids)

    async def _fetch_trade_page(
        self,
        *,
        condition_ids: list[str],
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        client = await self._get_client()
        response = await client.get(
            f"{settings.polymarket_data_api_base}/trades",
            params={
                "market": ",".join(condition_ids),
                "limit": min(max(limit, 1), DATA_API_MAX_LIMIT),
                "offset": min(max(offset, 0), DATA_API_MAX_OFFSET),
                "takerOnly": "true",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []

    async def _fetch_oi_batch(self, *, condition_ids: list[str]) -> list[dict[str, Any]]:
        client = await self._get_client()
        response = await client.get(
            f"{settings.polymarket_data_api_base}/oi",
            params={"market": ",".join(condition_ids)},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []

    async def project_pending_events(
        self,
        *,
        reason: str,
        after_raw_event_id: int | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        run_id = await self._create_run(
            run_type=RUN_TYPE_RAW_PROJECTOR,
            reason=reason,
            cursor_json={"after_raw_event_id": after_raw_event_id},
        )
        try:
            async with self._session_factory() as session:
                if after_raw_event_id is None:
                    latest_successful = await self._latest_successful_run(session, run_type=RUN_TYPE_RAW_PROJECTOR)
                    if latest_successful is not None and isinstance(latest_successful.cursor_json, dict):
                        after_raw_event_id = int(latest_successful.cursor_json.get("last_projected_raw_event_id") or 0)
                    else:
                        after_raw_event_id = 0

                query = (
                    select(PolymarketMarketEvent)
                    .where(
                        PolymarketMarketEvent.venue == STATUS_VENUE,
                        PolymarketMarketEvent.id > after_raw_event_id,
                        _relevant_raw_event_filter(),
                    )
                    .order_by(PolymarketMarketEvent.id.asc())
                    .limit(max(1, limit))
                )
                raw_events = (await session.execute(query)).scalars().all()
                rows_inserted = {
                    "book_snapshots": 0,
                    "book_deltas": 0,
                    "bbo_events": 0,
                    "trade_tape": 0,
                }
                last_projected_raw_event_id = after_raw_event_id

                for raw_event in raw_events:
                    payload = _normalize_payload_dict(raw_event.payload)
                    if payload is None:
                        continue

                    if raw_event.provenance == RESYNC_PROVENANCE and raw_event.channel == REST_RESYNC_CHANNEL:
                        asset_id_value = _coalesce(payload.get("asset_id"), payload.get("assetId"), raw_event.asset_id)
                        condition_id_value = _coalesce(payload.get("market"), payload.get("condition_id"), raw_event.market_id)
                        if asset_id_value is not None and condition_id_value is not None:
                            refs = await _resolve_registry_refs(
                                session,
                                asset_id=str(asset_id_value),
                                condition_id=str(condition_id_value),
                            )
                            inserted = await _insert_book_snapshot_row(
                                session,
                                refs=refs,
                                source_kind=SOURCE_KIND_REST_RESYNC_SNAPSHOT,
                                event_ts_exchange=raw_event.event_time or parse_polymarket_timestamp(payload.get("timestamp")),
                                recv_ts_local=raw_event.received_at_local,
                                ingest_ts_db=raw_event.created_at,
                                observed_at_local=raw_event.received_at_local,
                                stream_session_id=raw_event.connection_id,
                                raw_event_id=raw_event.id,
                                capture_run_id=run_id,
                                payload=payload,
                            )
                            rows_inserted["book_snapshots"] += int(inserted)
                            if inserted:
                                polymarket_raw_projected_rows.labels(
                                    table_name="polymarket_book_snapshots",
                                    source_kind=SOURCE_KIND_REST_RESYNC_SNAPSHOT,
                                ).inc()
                    elif raw_event.message_type == "book":
                        asset_id_value = _coalesce(payload.get("asset_id"), payload.get("assetId"), raw_event.asset_id)
                        condition_id_value = _coalesce(payload.get("market"), payload.get("condition_id"), raw_event.market_id)
                        if asset_id_value is not None and condition_id_value is not None:
                            refs = await _resolve_registry_refs(
                                session,
                                asset_id=str(asset_id_value),
                                condition_id=str(condition_id_value),
                            )
                            inserted = await _insert_book_snapshot_row(
                                session,
                                refs=refs,
                                source_kind=SOURCE_KIND_WS_BOOK,
                                event_ts_exchange=raw_event.event_time,
                                recv_ts_local=raw_event.received_at_local,
                                ingest_ts_db=raw_event.created_at,
                                observed_at_local=raw_event.received_at_local,
                                stream_session_id=raw_event.connection_id,
                                raw_event_id=raw_event.id,
                                capture_run_id=run_id,
                                payload=payload,
                            )
                            rows_inserted["book_snapshots"] += int(inserted)
                            if inserted:
                                polymarket_raw_projected_rows.labels(
                                    table_name="polymarket_book_snapshots",
                                    source_kind=SOURCE_KIND_WS_BOOK,
                                ).inc()
                    elif raw_event.message_type == "price_change":
                        inserted = await _insert_book_deltas_from_raw_event(session, raw_event=raw_event, payload=payload)
                        rows_inserted["book_deltas"] += inserted
                        if inserted:
                            polymarket_raw_projected_rows.labels(
                                table_name="polymarket_book_deltas",
                                source_kind="price_change",
                            ).inc(inserted)
                    elif raw_event.message_type == "best_bid_ask":
                        inserted = await _insert_bbo_from_raw_event(session, raw_event=raw_event, payload=payload)
                        rows_inserted["bbo_events"] += int(inserted)
                        if inserted:
                            polymarket_raw_projected_rows.labels(
                                table_name="polymarket_bbo_events",
                                source_kind="best_bid_ask",
                            ).inc()
                    elif raw_event.message_type == "last_trade_price":
                        inserted = await _insert_trade_from_raw_event(
                            session,
                            raw_event=raw_event,
                            payload=payload,
                            capture_run_id=run_id,
                        )
                        rows_inserted["trade_tape"] += int(inserted)
                        if inserted:
                            polymarket_raw_projected_rows.labels(
                                table_name="polymarket_trade_tape",
                                source_kind=SOURCE_KIND_WS_LAST_TRADE_PRICE,
                            ).inc()

                    last_projected_raw_event_id = raw_event.id

                await session.commit()

            result = await self._finalize_run(
                run_id=run_id,
                status="completed",
                cursor_json={
                    "after_raw_event_id": after_raw_event_id,
                    "last_projected_raw_event_id": last_projected_raw_event_id,
                    "processed_raw_event_count": len(raw_events),
                },
                rows_inserted_json=rows_inserted,
                details_json={"processed_raw_event_count": len(raw_events)},
            )
            polymarket_raw_projector_runs.labels(reason=reason, status="completed").inc()
            if result["completed_at"] is not None:
                polymarket_raw_projector_last_success_timestamp.set(result["completed_at"].timestamp())
            return result
        except Exception as exc:
            polymarket_raw_projector_runs.labels(reason=reason, status="failed").inc()
            polymarket_raw_projector_failures.inc()
            return await self._finalize_run(
                run_id=run_id,
                status="failed",
                error_count=1,
                details_json={"error": str(exc)},
            )

    async def project_until_idle(
        self,
        *,
        reason: str,
        after_raw_event_id: int | None = None,
        limit: int = 1000,
        max_batches: int = 50,
    ) -> dict[str, Any]:
        runs: list[dict[str, Any]] = []
        next_after = after_raw_event_id
        for _ in range(max_batches):
            result = await self.project_pending_events(reason=reason, after_raw_event_id=next_after, limit=limit)
            runs.append(result)
            cursor_json = result.get("cursor_json") if isinstance(result, dict) else None
            rows_inserted = result.get("rows_inserted_json") if isinstance(result, dict) else None
            processed_count = int((cursor_json or {}).get("processed_raw_event_count") or 0)
            next_after = int((cursor_json or {}).get("last_projected_raw_event_id") or (next_after or 0))
            if result.get("status") != "completed" or processed_count == 0:
                break
            if not any(int(value or 0) > 0 for value in (rows_inserted or {}).values()) and processed_count < limit:
                break
        return {
            "run_count": len(runs),
            "last_run": runs[-1] if runs else None,
            "runs": runs,
        }

    async def capture_book_snapshots(
        self,
        *,
        reason: str,
        asset_ids: list[str] | None = None,
        source_kind: str,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            target_asset_ids = unique_preserving_order([str(value) for value in (asset_ids or []) if value])
            if not target_asset_ids:
                target_asset_ids = await list_watched_polymarket_assets(session)
        run_id = await self._create_run(
            run_type=RUN_TYPE_BOOK_SNAPSHOT,
            reason=reason,
            scope_json={"asset_ids": target_asset_ids, "source_kind": source_kind},
        )
        rows_inserted = {"book_snapshots": 0, "param_rows_inserted": 0}
        try:
            if not target_asset_ids:
                result = await self._finalize_run(
                    run_id=run_id,
                    status="completed",
                    rows_inserted_json=rows_inserted,
                    details_json={"asset_ids": []},
                )
                polymarket_book_snapshot_runs.labels(reason=reason, status="completed").inc()
                if result["completed_at"] is not None:
                    polymarket_book_snapshot_last_success_timestamp.set(result["completed_at"].timestamp())
                return result

            for batch in _chunked(target_asset_ids, BOOKS_BATCH_SIZE):
                payloads = await self._fetch_books_batch(batch)
                payload_by_asset = {
                    str(_coalesce(payload.get("asset_id"), payload.get("assetId"))): payload
                    for payload in payloads
                    if isinstance(payload, dict) and _coalesce(payload.get("asset_id"), payload.get("assetId")) is not None
                }
                observed_at_local = utcnow()
                async with self._session_factory() as session:
                    for asset_id in batch:
                        payload = payload_by_asset.get(asset_id)
                        if payload is None:
                            continue
                        condition_id_value = _coalesce(payload.get("market"), payload.get("condition_id"))
                        if condition_id_value is None:
                            continue
                        refs = await _resolve_registry_refs(
                            session,
                            asset_id=asset_id,
                            condition_id=str(condition_id_value),
                        )
                        inserted = await _insert_book_snapshot_row(
                            session,
                            refs=refs,
                            source_kind=source_kind,
                            event_ts_exchange=parse_polymarket_timestamp(payload.get("timestamp")),
                            recv_ts_local=observed_at_local,
                            ingest_ts_db=observed_at_local,
                            observed_at_local=observed_at_local,
                            stream_session_id=None,
                            raw_event_id=None,
                            capture_run_id=run_id,
                            payload=payload,
                        )
                        rows_inserted["book_snapshots"] += int(inserted)
                        if inserted:
                            polymarket_raw_projected_rows.labels(
                                table_name="polymarket_book_snapshots",
                                source_kind=source_kind,
                            ).inc()
                        rows_inserted["param_rows_inserted"] += await seed_registry_from_book_snapshot(
                            session,
                            payload=payload,
                            observed_at_local=observed_at_local,
                            sync_run_id=None,
                            raw_event_id=None,
                            source_kind=source_kind,
                        )
                    await session.commit()

            result = await self._finalize_run(
                run_id=run_id,
                status="completed",
                rows_inserted_json=rows_inserted,
                details_json={"asset_ids": target_asset_ids, "source_kind": source_kind},
            )
            polymarket_book_snapshot_runs.labels(reason=reason, status="completed").inc()
            if result["completed_at"] is not None:
                polymarket_book_snapshot_last_success_timestamp.set(result["completed_at"].timestamp())
            return result
        except Exception as exc:
            polymarket_book_snapshot_runs.labels(reason=reason, status="failed").inc()
            polymarket_book_snapshot_failures.inc()
            return await self._finalize_run(
                run_id=run_id,
                status="failed",
                rows_inserted_json=rows_inserted,
                error_count=1,
                details_json={"asset_ids": target_asset_ids, "source_kind": source_kind, "error": str(exc)},
            )

    async def backfill_trades(
        self,
        *,
        reason: str,
        asset_ids: list[str] | None = None,
        condition_ids: list[str] | None = None,
        lookback_hours: int | None = None,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            target_condition_ids, unresolved_assets = await _resolve_condition_ids_for_scope(
                session,
                asset_ids=asset_ids,
                condition_ids=condition_ids,
            )
        resolved_lookback_hours = lookback_hours or settings.polymarket_trade_backfill_lookback_hours
        run_id = await self._create_run(
            run_type=RUN_TYPE_TRADE_BACKFILL,
            reason=reason,
            scope_json={
                "asset_ids": unique_preserving_order([str(value) for value in (asset_ids or []) if value]),
                "condition_ids": target_condition_ids,
                "lookback_hours": resolved_lookback_hours,
            },
        )
        rows_inserted = {"trade_tape": 0}
        try:
            cutoff = utcnow() - timedelta(hours=resolved_lookback_hours)
            page_size = min(max(settings.polymarket_trade_backfill_page_size, 1), DATA_API_MAX_LIMIT)

            for condition_batch in _chunked(target_condition_ids, MARKET_BATCH_SIZE):
                offset = 0
                while True:
                    payloads = await self._fetch_trade_page(
                        condition_ids=condition_batch,
                        limit=page_size,
                        offset=offset,
                    )
                    if not payloads:
                        break

                    oldest_timestamp: datetime | None = None
                    async with self._session_factory() as session:
                        observed_at_local = utcnow()
                        for payload in payloads:
                            condition_id_value = _coalesce(payload.get("conditionId"), payload.get("market"))
                            if condition_id_value is None:
                                continue
                            event_ts_exchange = parse_polymarket_timestamp(payload.get("timestamp"))
                            if event_ts_exchange is not None:
                                oldest_timestamp = (
                                    event_ts_exchange
                                    if oldest_timestamp is None
                                    else min(oldest_timestamp, event_ts_exchange)
                                )
                                if event_ts_exchange < cutoff:
                                    continue

                            asset_id = str(payload["asset"]) if payload.get("asset") is not None else None
                            refs = await _resolve_registry_refs(
                                session,
                                asset_id=asset_id,
                                condition_id=str(condition_id_value),
                            )
                            price = _to_decimal(payload.get("price"))
                            size = _to_decimal(payload.get("size"))
                            if price is None or size is None:
                                continue

                            inserted = await _insert_trade_row(
                                session,
                                refs=refs,
                                source_kind=SOURCE_KIND_DATA_API_TRADES,
                                event_ts_exchange=event_ts_exchange,
                                recv_ts_local=None,
                                ingest_ts_db=observed_at_local,
                                observed_at_local=observed_at_local,
                                stream_session_id=None,
                                raw_event_id=None,
                                capture_run_id=run_id,
                                transaction_hash=str(payload["transactionHash"]) if payload.get("transactionHash") else None,
                                side=_normalize_side(payload.get("side")),
                                price=price,
                                size=size,
                                fee_rate_bps=None,
                                event_slug=str(payload["eventSlug"]) if payload.get("eventSlug") is not None else None,
                                outcome_name=str(payload["outcome"]) if payload.get("outcome") is not None else refs.outcome_name,
                                outcome_index=int(payload["outcomeIndex"]) if payload.get("outcomeIndex") is not None else refs.outcome_index,
                                proxy_wallet=str(payload["proxyWallet"]) if payload.get("proxyWallet") is not None else None,
                                details_json={
                                    "title": payload.get("title"),
                                    "slug": payload.get("slug"),
                                    "name": payload.get("name"),
                                    "pseudonym": payload.get("pseudonym"),
                                },
                                source_payload_json=payload,
                            )
                            rows_inserted["trade_tape"] += int(inserted)
                            if inserted:
                                polymarket_raw_projected_rows.labels(
                                    table_name="polymarket_trade_tape",
                                    source_kind=SOURCE_KIND_DATA_API_TRADES,
                                ).inc()
                        await session.commit()

                    if len(payloads) < page_size:
                        break
                    if oldest_timestamp is not None and oldest_timestamp < cutoff:
                        break
                    if offset + page_size > DATA_API_MAX_OFFSET:
                        break
                    offset += page_size

            result = await self._finalize_run(
                run_id=run_id,
                status="completed",
                rows_inserted_json=rows_inserted,
                details_json={
                    "condition_ids": target_condition_ids,
                    "unresolved_assets": unresolved_assets,
                    "lookback_hours": resolved_lookback_hours,
                },
            )
            polymarket_trade_backfill_runs.labels(reason=reason, status="completed").inc()
            if result["completed_at"] is not None:
                polymarket_trade_backfill_last_success_timestamp.set(result["completed_at"].timestamp())
            return result
        except Exception as exc:
            polymarket_trade_backfill_runs.labels(reason=reason, status="failed").inc()
            polymarket_trade_backfill_failures.inc()
            return await self._finalize_run(
                run_id=run_id,
                status="failed",
                rows_inserted_json=rows_inserted,
                error_count=1,
                details_json={
                    "condition_ids": target_condition_ids,
                    "unresolved_assets": unresolved_assets,
                    "lookback_hours": resolved_lookback_hours,
                    "error": str(exc),
                },
            )

    async def poll_open_interest(
        self,
        *,
        reason: str,
        asset_ids: list[str] | None = None,
        condition_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            target_condition_ids, unresolved_assets = await _resolve_condition_ids_for_scope(
                session,
                asset_ids=asset_ids,
                condition_ids=condition_ids,
            )
        run_id = await self._create_run(
            run_type=RUN_TYPE_OI_POLL,
            reason=reason,
            scope_json={
                "asset_ids": unique_preserving_order([str(value) for value in (asset_ids or []) if value]),
                "condition_ids": target_condition_ids,
            },
        )
        rows_inserted = {"open_interest_history": 0}
        try:
            if target_condition_ids:
                for condition_batch in _chunked(target_condition_ids, MARKET_BATCH_SIZE):
                    payloads = await self._fetch_oi_batch(condition_ids=condition_batch)
                    payload_by_condition = {
                        str(payload.get("market")): payload
                        for payload in payloads
                        if isinstance(payload, dict) and payload.get("market") is not None
                    }
                    async with self._session_factory() as session:
                        observed_at_local = utcnow()
                        for condition_id in condition_batch:
                            payload = payload_by_condition.get(condition_id)
                            if payload is None:
                                continue
                            value = _to_decimal(payload.get("value"))
                            if value is None:
                                continue
                            market_dim = (
                                await session.execute(
                                    select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == condition_id).limit(1)
                                )
                            ).scalar_one_or_none()
                            existing = (
                                await session.execute(
                                    select(PolymarketOpenInterestHistory.id).where(
                                        PolymarketOpenInterestHistory.capture_run_id == run_id,
                                        PolymarketOpenInterestHistory.condition_id == condition_id,
                                    )
                                )
                            ).scalar_one_or_none()
                            if existing is not None:
                                continue
                            session.add(
                                PolymarketOpenInterestHistory(
                                    market_dim_id=market_dim.id if market_dim is not None else None,
                                    condition_id=condition_id,
                                    source_kind=SOURCE_KIND_DATA_API_OI_POLL,
                                    observed_at_local=observed_at_local,
                                    capture_run_id=run_id,
                                    value=value,
                                    source_payload_json=payload,
                                )
                            )
                            rows_inserted["open_interest_history"] += 1
                        await session.commit()

            result = await self._finalize_run(
                run_id=run_id,
                status="completed",
                rows_inserted_json=rows_inserted,
                details_json={
                    "condition_ids": target_condition_ids,
                    "unresolved_assets": unresolved_assets,
                },
            )
            if rows_inserted["open_interest_history"]:
                polymarket_raw_projected_rows.labels(
                    table_name="polymarket_open_interest_history",
                    source_kind=SOURCE_KIND_DATA_API_OI_POLL,
                ).inc(rows_inserted["open_interest_history"])
            polymarket_oi_poll_runs.labels(reason=reason, status="completed").inc()
            if result["completed_at"] is not None:
                polymarket_oi_poll_last_success_timestamp.set(result["completed_at"].timestamp())
            return result
        except Exception as exc:
            polymarket_oi_poll_runs.labels(reason=reason, status="failed").inc()
            polymarket_oi_poll_failures.inc()
            return await self._finalize_run(
                run_id=run_id,
                status="failed",
                rows_inserted_json=rows_inserted,
                error_count=1,
                details_json={
                    "condition_ids": target_condition_ids,
                    "unresolved_assets": unresolved_assets,
                    "error": str(exc),
                },
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_raw_storage_enabled:
            logger.info("Polymarket raw storage disabled; skipping worker startup")
            return

        try:
            await self.project_until_idle(reason="startup")
            await self.capture_book_snapshots(reason="startup", source_kind=SOURCE_KIND_REST_SEED_SNAPSHOT)
            if settings.polymarket_trade_backfill_enabled and settings.polymarket_trade_backfill_on_startup:
                await self.backfill_trades(reason="startup")
            if settings.polymarket_oi_poll_enabled:
                await self.poll_open_interest(reason="startup")
        except Exception:
            logger.warning("Polymarket raw storage startup sequence failed", exc_info=True)

        loop = asyncio.get_running_loop()
        next_projector_at = loop.time() + RAW_PROJECTOR_POLL_SECONDS
        next_book_snapshot_at = loop.time() + settings.polymarket_book_snapshot_interval_seconds
        next_trade_backfill_at = loop.time() + settings.polymarket_trade_backfill_interval_seconds
        next_oi_poll_at = loop.time() + settings.polymarket_oi_poll_interval_seconds

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1)
                continue
            except asyncio.TimeoutError:
                pass

            now = loop.time()
            if now >= next_projector_at:
                try:
                    await self.project_until_idle(reason="scheduled")
                except Exception:
                    logger.warning("Polymarket raw projector scheduled run failed", exc_info=True)
                next_projector_at = now + RAW_PROJECTOR_POLL_SECONDS

            if now >= next_book_snapshot_at:
                try:
                    await self.capture_book_snapshots(
                        reason="scheduled",
                        source_kind=SOURCE_KIND_REST_PERIODIC_SNAPSHOT,
                    )
                except Exception:
                    logger.warning("Polymarket periodic book snapshot run failed", exc_info=True)
                next_book_snapshot_at = now + settings.polymarket_book_snapshot_interval_seconds

            if settings.polymarket_trade_backfill_enabled and now >= next_trade_backfill_at:
                try:
                    await self.backfill_trades(reason="scheduled")
                except Exception:
                    logger.warning("Polymarket scheduled trade backfill failed", exc_info=True)
                next_trade_backfill_at = now + settings.polymarket_trade_backfill_interval_seconds

            if settings.polymarket_oi_poll_enabled and now >= next_oi_poll_at:
                try:
                    await self.poll_open_interest(reason="scheduled")
                except Exception:
                    logger.warning("Polymarket scheduled OI poll failed", exc_info=True)
                next_oi_poll_at = now + settings.polymarket_oi_poll_interval_seconds


async def fetch_polymarket_raw_storage_status(session: AsyncSession) -> dict[str, Any]:
    latest_relevant_raw_event_id = int(
        (
            await session.execute(
                select(func.max(PolymarketMarketEvent.id)).where(
                    PolymarketMarketEvent.venue == STATUS_VENUE,
                    _relevant_raw_event_filter(),
                )
            )
        ).scalar_one()
        or 0
    )
    latest_projector_run = (
        await session.execute(
            select(PolymarketRawCaptureRun)
            .where(PolymarketRawCaptureRun.run_type == RUN_TYPE_RAW_PROJECTOR)
            .order_by(PolymarketRawCaptureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    latest_projector_success = (
        await session.execute(
            select(PolymarketRawCaptureRun)
            .where(
                PolymarketRawCaptureRun.run_type == RUN_TYPE_RAW_PROJECTOR,
                PolymarketRawCaptureRun.status.in_(SUCCESSFUL_RUN_STATUSES),
            )
            .order_by(PolymarketRawCaptureRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    last_projected_raw_event_id = 0
    if latest_projector_success is not None and isinstance(latest_projector_success.cursor_json, dict):
        last_projected_raw_event_id = int(latest_projector_success.cursor_json.get("last_projected_raw_event_id") or 0)

    projector_lag = max(0, latest_relevant_raw_event_id - last_projected_raw_event_id)
    polymarket_raw_projector_lag.set(projector_lag)
    if latest_projector_success is not None and latest_projector_success.completed_at is not None:
        polymarket_raw_projector_last_success_timestamp.set(latest_projector_success.completed_at.timestamp())

    async def _latest_success_at(run_type: str) -> datetime | None:
        return (
            await session.execute(
                select(func.max(PolymarketRawCaptureRun.completed_at)).where(
                    PolymarketRawCaptureRun.run_type == run_type,
                    PolymarketRawCaptureRun.status.in_(SUCCESSFUL_RUN_STATUSES),
                )
            )
        ).scalar_one_or_none()

    last_book_snapshot_at = await _latest_success_at(RUN_TYPE_BOOK_SNAPSHOT)
    last_trade_backfill_at = await _latest_success_at(RUN_TYPE_TRADE_BACKFILL)
    last_oi_poll_at = await _latest_success_at(RUN_TYPE_OI_POLL)
    now = utcnow()
    book_snapshot_freshness_seconds = (
        max(0, int((now - last_book_snapshot_at).total_seconds()))
        if last_book_snapshot_at is not None
        else None
    )
    trade_backfill_freshness_seconds = (
        max(0, int((now - last_trade_backfill_at).total_seconds()))
        if last_trade_backfill_at is not None
        else None
    )
    oi_poll_freshness_seconds = (
        max(0, int((now - last_oi_poll_at).total_seconds()))
        if last_oi_poll_at is not None
        else None
    )

    if last_book_snapshot_at is not None:
        polymarket_book_snapshot_last_success_timestamp.set(last_book_snapshot_at.timestamp())
    if last_trade_backfill_at is not None:
        polymarket_trade_backfill_last_success_timestamp.set(last_trade_backfill_at.timestamp())
    if last_oi_poll_at is not None:
        polymarket_oi_poll_last_success_timestamp.set(last_oi_poll_at.timestamp())

    recent_runs = (
        await session.execute(
            select(PolymarketRawCaptureRun)
            .order_by(PolymarketRawCaptureRun.started_at.desc())
            .limit(10)
        )
    ).scalars().all()

    since = now - timedelta(hours=24)
    recent_rows = {
        "book_snapshots": int(
            (
                await session.execute(
                    select(func.count(PolymarketBookSnapshot.id)).where(PolymarketBookSnapshot.observed_at_local >= since)
                )
            ).scalar_one()
            or 0
        ),
        "book_deltas": int(
            (
                await session.execute(
                    select(func.count(PolymarketBookDelta.id)).where(
                        func.coalesce(
                            PolymarketBookDelta.event_ts_exchange,
                            PolymarketBookDelta.recv_ts_local,
                            PolymarketBookDelta.ingest_ts_db,
                        )
                        >= since
                    )
                )
            ).scalar_one()
            or 0
        ),
        "bbo_events": int(
            (
                await session.execute(
                    select(func.count(PolymarketBboEvent.id)).where(
                        func.coalesce(
                            PolymarketBboEvent.event_ts_exchange,
                            PolymarketBboEvent.recv_ts_local,
                            PolymarketBboEvent.ingest_ts_db,
                        )
                        >= since
                    )
                )
            ).scalar_one()
            or 0
        ),
        "trade_tape": int(
            (
                await session.execute(
                    select(func.count(PolymarketTradeTape.id)).where(PolymarketTradeTape.observed_at_local >= since)
                )
            ).scalar_one()
            or 0
        ),
        "open_interest_history": int(
            (
                await session.execute(
                    select(func.count(PolymarketOpenInterestHistory.id)).where(
                        PolymarketOpenInterestHistory.observed_at_local >= since
                    )
                )
            ).scalar_one()
            or 0
        ),
    }

    return {
        "enabled": settings.polymarket_raw_storage_enabled,
        "book_snapshot_interval_seconds": settings.polymarket_book_snapshot_interval_seconds,
        "trade_backfill_enabled": settings.polymarket_trade_backfill_enabled,
        "trade_backfill_on_startup": settings.polymarket_trade_backfill_on_startup,
        "trade_backfill_interval_seconds": settings.polymarket_trade_backfill_interval_seconds,
        "trade_backfill_lookback_hours": settings.polymarket_trade_backfill_lookback_hours,
        "trade_backfill_page_size": settings.polymarket_trade_backfill_page_size,
        "oi_poll_enabled": settings.polymarket_oi_poll_enabled,
        "oi_poll_interval_seconds": settings.polymarket_oi_poll_interval_seconds,
        "retention_days": settings.polymarket_raw_retention_days,
        "projector_last_run_status": latest_projector_run.status if latest_projector_run is not None else None,
        "projector_last_run_started_at": latest_projector_run.started_at if latest_projector_run is not None else None,
        "projector_last_run_completed_at": latest_projector_run.completed_at if latest_projector_run is not None else None,
        "last_projected_raw_event_id": last_projected_raw_event_id,
        "latest_relevant_raw_event_id": latest_relevant_raw_event_id,
        "projector_lag": projector_lag,
        "last_successful_book_snapshot_at": last_book_snapshot_at,
        "last_successful_trade_backfill_at": last_trade_backfill_at,
        "last_successful_oi_poll_at": last_oi_poll_at,
        "book_snapshot_freshness_seconds": book_snapshot_freshness_seconds,
        "trade_backfill_freshness_seconds": trade_backfill_freshness_seconds,
        "oi_poll_freshness_seconds": oi_poll_freshness_seconds,
        "rows_inserted_24h": recent_rows,
        "recent_capture_runs": [_serialize_capture_run(run) for run in recent_runs],
    }


async def list_polymarket_raw_capture_runs(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    total = int((await session.execute(select(func.count(PolymarketRawCaptureRun.id)))).scalar_one() or 0)
    runs = (
        await session.execute(
            select(PolymarketRawCaptureRun)
            .order_by(PolymarketRawCaptureRun.started_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    return [_serialize_capture_run(run) for run in runs], total


async def lookup_polymarket_book_snapshots(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    source_kind: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    after_id: int | None,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketBookSnapshot)
    if asset_id:
        query = query.where(PolymarketBookSnapshot.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookSnapshot.condition_id == condition_id)
    if source_kind:
        query = query.where(PolymarketBookSnapshot.source_kind == source_kind)
    if after_id is not None:
        query = query.where(PolymarketBookSnapshot.id > after_id)
    query = _query_with_time_bounds(
        query,
        time_column=func.coalesce(PolymarketBookSnapshot.event_ts_exchange, PolymarketBookSnapshot.observed_at_local),
        start=start,
        end=end,
    )
    rows = (
        await session.execute(query.order_by(PolymarketBookSnapshot.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_book_snapshot(row) for row in rows]


async def lookup_polymarket_book_deltas(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    after_id: int | None,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketBookDelta)
    if asset_id:
        query = query.where(PolymarketBookDelta.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBookDelta.condition_id == condition_id)
    if after_id is not None:
        query = query.where(PolymarketBookDelta.id > after_id)
    query = _query_with_time_bounds(
        query,
        time_column=func.coalesce(
            PolymarketBookDelta.event_ts_exchange,
            PolymarketBookDelta.recv_ts_local,
            PolymarketBookDelta.ingest_ts_db,
        ),
        start=start,
        end=end,
    )
    rows = (
        await session.execute(query.order_by(PolymarketBookDelta.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_book_delta(row) for row in rows]


async def lookup_polymarket_bbo_events(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    after_id: int | None,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketBboEvent)
    if asset_id:
        query = query.where(PolymarketBboEvent.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketBboEvent.condition_id == condition_id)
    if after_id is not None:
        query = query.where(PolymarketBboEvent.id > after_id)
    query = _query_with_time_bounds(
        query,
        time_column=func.coalesce(
            PolymarketBboEvent.event_ts_exchange,
            PolymarketBboEvent.recv_ts_local,
            PolymarketBboEvent.ingest_ts_db,
        ),
        start=start,
        end=end,
    )
    rows = (
        await session.execute(query.order_by(PolymarketBboEvent.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_bbo_event(row) for row in rows]


async def lookup_polymarket_trade_tape(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    source_kind: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    after_id: int | None,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketTradeTape)
    if asset_id:
        query = query.where(PolymarketTradeTape.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketTradeTape.condition_id == condition_id)
    if source_kind:
        query = query.where(PolymarketTradeTape.source_kind == source_kind)
    if after_id is not None:
        query = query.where(PolymarketTradeTape.id > after_id)
    query = _query_with_time_bounds(
        query,
        time_column=func.coalesce(PolymarketTradeTape.event_ts_exchange, PolymarketTradeTape.observed_at_local),
        start=start,
        end=end,
    )
    rows = (
        await session.execute(query.order_by(PolymarketTradeTape.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_trade_tape(row) for row in rows]


async def lookup_polymarket_open_interest_history(
    session: AsyncSession,
    *,
    condition_id: str | None,
    source_kind: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    after_id: int | None,
) -> list[dict[str, Any]]:
    query: Select[Any] = select(PolymarketOpenInterestHistory)
    if condition_id:
        query = query.where(PolymarketOpenInterestHistory.condition_id == condition_id)
    if source_kind:
        query = query.where(PolymarketOpenInterestHistory.source_kind == source_kind)
    if after_id is not None:
        query = query.where(PolymarketOpenInterestHistory.id > after_id)
    query = _query_with_time_bounds(
        query,
        time_column=PolymarketOpenInterestHistory.observed_at_local,
        start=start,
        end=end,
    )
    rows = (
        await session.execute(query.order_by(PolymarketOpenInterestHistory.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_open_interest(row) for row in rows]


async def trigger_manual_polymarket_raw_projector(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    after_raw_event_id: int | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    service = PolymarketRawStorageService(session_factory)
    try:
        return await service.project_until_idle(
            reason=reason,
            after_raw_event_id=after_raw_event_id,
            limit=limit,
        )
    finally:
        await service.close()


async def trigger_manual_polymarket_book_snapshot(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    asset_ids: list[str] | None = None,
) -> dict[str, Any]:
    service = PolymarketRawStorageService(session_factory)
    try:
        return await service.capture_book_snapshots(
            reason=reason,
            asset_ids=asset_ids,
            source_kind=SOURCE_KIND_REST_MANUAL_SNAPSHOT,
        )
    finally:
        await service.close()


async def trigger_manual_polymarket_trade_backfill(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    asset_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
    lookback_hours: int | None = None,
) -> dict[str, Any]:
    service = PolymarketRawStorageService(session_factory)
    try:
        return await service.backfill_trades(
            reason=reason,
            asset_ids=asset_ids,
            condition_ids=condition_ids,
            lookback_hours=lookback_hours,
        )
    finally:
        await service.close()


async def trigger_manual_polymarket_oi_poll(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    asset_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
) -> dict[str, Any]:
    service = PolymarketRawStorageService(session_factory)
    try:
        return await service.poll_open_interest(
            reason=reason,
            asset_ids=asset_ids,
            condition_ids=condition_ids,
        )
    finally:
        await service.close()
