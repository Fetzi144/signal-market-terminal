from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_common import (
    STATUS_VENUE,
    extract_asset_ids,
    parse_json_if_string,
    parse_listish,
    parse_polymarket_timestamp,
    unique_preserving_order,
    utcnow,
)
from app.ingestion.polymarket_maker_economics import (
    FEE_SOURCE_KIND,
    REWARD_SOURCE_KIND,
    insert_reward_history_if_changed,
    insert_token_fee_history_if_changed,
    normalize_reward_history_payload,
)
from app.metrics import (
    polymarket_meta_assets_upserted,
    polymarket_meta_events_upserted,
    polymarket_meta_last_successful_sync_age_seconds,
    polymarket_meta_last_successful_sync_timestamp,
    polymarket_meta_markets_upserted,
    polymarket_meta_param_rows_inserted,
    polymarket_meta_registry_stale_rows,
    polymarket_meta_sync_failures,
    polymarket_meta_sync_runs,
)
from app.models.market import Outcome
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketEventDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
    PolymarketMetaSyncRun,
)
from app.models.polymarket_stream import PolymarketMarketEvent, PolymarketWatchAsset

logger = logging.getLogger(__name__)

GAMMA_EVENT_SOURCE = "gamma_event"
GAMMA_MARKET_SOURCE = "gamma_market"
STREAM_NEW_MARKET_SOURCE = "stream_new_market"
STREAM_TICK_SIZE_SOURCE = "stream_tick_size_change"
STREAM_RESOLUTION_SOURCE = "stream_market_resolved"
BOOK_SEED_SOURCE = "rest_book_seed"


@dataclass(slots=True)
class MetaSyncCounters:
    events_seen: int = 0
    markets_seen: int = 0
    assets_upserted: int = 0
    events_upserted: int = 0
    markets_upserted: int = 0
    param_rows_inserted: int = 0
    fee_rows_inserted: int = 0
    reward_rows_inserted: int = 0
    error_count: int = 0


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _normalize_tags(value: Any) -> list[Any] | None:
    parsed = parse_json_if_string(value)
    if not isinstance(parsed, list):
        return None
    normalized: list[Any] = []
    for item in parsed:
        if isinstance(item, dict):
            normalized.append(
                {
                    "id": item.get("id"),
                    "label": item.get("label"),
                    "slug": item.get("slug"),
                }
            )
        else:
            normalized.append(item)
    return normalized or None


def _normalize_fee_schedule(value: Any) -> dict[str, Any] | None:
    parsed = parse_json_if_string(value)
    if not isinstance(parsed, dict):
        return None
    normalized = {
        "exponent": str(parsed["exponent"]) if parsed.get("exponent") is not None else None,
        "rate": str(parsed["rate"]) if parsed.get("rate") is not None else None,
        "taker_only": _to_bool(_coalesce(parsed.get("taker_only"), parsed.get("takerOnly"))),
        "rebate_rate": str(parsed["rebate_rate"]) if parsed.get("rebate_rate") is not None else None,
    }
    rebate_rate = parsed.get("rebateRate")
    if normalized["rebate_rate"] is None and rebate_rate is not None:
        normalized["rebate_rate"] = str(rebate_rate)
    return normalized


def _normalize_outcomes(payload: dict[str, Any]) -> list[str]:
    outcomes = parse_listish(payload.get("outcomes"))
    if outcomes:
        return [str(item) for item in outcomes]
    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        values: list[str] = []
        for token in tokens:
            if isinstance(token, dict) and token.get("outcome") is not None:
                values.append(str(token["outcome"]))
        return values
    winning_outcome = payload.get("winning_outcome")
    if winning_outcome is not None:
        return [str(winning_outcome)]
    return []


def _normalize_token_ids(payload: dict[str, Any]) -> list[str]:
    token_ids = parse_listish(
        _coalesce(payload.get("clobTokenIds"), payload.get("clob_token_ids"), payload.get("assets_ids"))
    )
    if token_ids:
        return [str(item) for item in token_ids]
    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        values: list[str] = []
        for token in tokens:
            if isinstance(token, dict) and token.get("token_id") is not None:
                values.append(str(token["token_id"]))
        return values
    return extract_asset_ids(payload)


def _market_effective_at(payload: dict[str, Any]) -> datetime | None:
    return parse_polymarket_timestamp(
        _coalesce(
            payload.get("updatedAt"),
            payload.get("updated_at"),
            payload.get("closedTime"),
            payload.get("closed_time"),
            payload.get("timestamp"),
            payload.get("createdAt"),
            payload.get("creationDate"),
            payload.get("created_at"),
        )
    )


def _normalize_resolution_state(payload: dict[str, Any]) -> str | None:
    value = _coalesce(
        payload.get("resolution_state"),
        payload.get("umaResolutionStatus"),
        payload.get("uma_resolution_status"),
    )
    if value is not None:
        return str(value)
    if payload.get("event_type") == "market_resolved" or payload.get("winning_asset_id") is not None:
        return "resolved"
    return None


def _winner_from_payload(payload: dict[str, Any], asset_ids: list[str]) -> str | None:
    winning_asset_id = _coalesce(payload.get("winning_asset_id"), payload.get("winningAssetId"))
    if winning_asset_id is not None:
        return str(winning_asset_id)

    winner = payload.get("winner")
    if winner is not None:
        winner_text = str(winner)
        if winner_text in asset_ids:
            return winner_text

    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if isinstance(token, dict) and _to_bool(token.get("winner")) and token.get("token_id") is not None:
                return str(token["token_id"])

    return None


def _resolved_from_payload(payload: dict[str, Any], winning_asset_id: str | None) -> bool | None:
    explicit = _coalesce(payload.get("resolved"), payload.get("isResolved"))
    explicit_bool = _to_bool(explicit)
    if explicit_bool is not None:
        return explicit_bool
    if payload.get("event_type") == "market_resolved" or winning_asset_id is not None:
        return True
    return _to_bool(payload.get("closed"))


def _stable_json(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _build_fingerprint(
    *,
    condition_id: str,
    asset_id: str | None,
    tick_size: Decimal | None,
    min_order_size: Decimal | None,
    neg_risk: bool | None,
    fees_enabled: bool | None,
    fee_schedule_json: dict[str, Any] | None,
    maker_base_fee: Decimal | None,
    taker_base_fee: Decimal | None,
    resolution_state: str | None,
    winning_asset_id: str | None,
) -> str:
    payload = {
        "condition_id": condition_id,
        "asset_id": asset_id,
        "tick_size": str(tick_size) if tick_size is not None else None,
        "min_order_size": str(min_order_size) if min_order_size is not None else None,
        "neg_risk": neg_risk,
        "fees_enabled": fees_enabled,
        "fee_schedule_json": fee_schedule_json,
        "maker_base_fee": str(maker_base_fee) if maker_base_fee is not None else None,
        "taker_base_fee": str(taker_base_fee) if taker_base_fee is not None else None,
        "resolution_state": resolution_state,
        "winning_asset_id": winning_asset_id,
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _serialize_sync_run(run: PolymarketMetaSyncRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "status": run.status,
        "reason": run.reason,
        "include_closed": run.include_closed,
        "events_seen": run.events_seen,
        "markets_seen": run.markets_seen,
        "assets_upserted": run.assets_upserted,
        "events_upserted": run.events_upserted,
        "markets_upserted": run.markets_upserted,
        "param_rows_inserted": run.param_rows_inserted,
        "error_count": run.error_count,
        "details_json": run.details_json,
    }


def _serialize_param_history(row: PolymarketMarketParamHistory) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "source_kind": row.source_kind,
        "effective_at_exchange": row.effective_at_exchange,
        "received_at_local": row.received_at_local,
        "observed_at_local": row.observed_at_local,
        "sync_run_id": row.sync_run_id,
        "raw_event_id": row.raw_event_id,
        "tick_size": _serialize_decimal(row.tick_size),
        "min_order_size": _serialize_decimal(row.min_order_size),
        "neg_risk": row.neg_risk,
        "fees_enabled": row.fees_enabled,
        "fee_schedule_json": row.fee_schedule_json,
        "maker_base_fee": _serialize_decimal(row.maker_base_fee),
        "taker_base_fee": _serialize_decimal(row.taker_base_fee),
        "resolution_state": row.resolution_state,
        "winning_asset_id": row.winning_asset_id,
        "fingerprint": row.fingerprint,
        "details_json": row.details_json,
        "created_at": row.created_at,
    }


def _serialize_event_dim(event_dim: PolymarketEventDim) -> dict[str, Any]:
    return {
        "id": event_dim.id,
        "gamma_event_id": event_dim.gamma_event_id,
        "event_slug": event_dim.event_slug,
        "event_ticker": event_dim.event_ticker,
        "title": event_dim.title,
        "subtitle": event_dim.subtitle,
        "category": event_dim.category,
        "subcategory": event_dim.subcategory,
        "active": event_dim.active,
        "closed": event_dim.closed,
        "archived": event_dim.archived,
        "neg_risk": event_dim.neg_risk,
        "neg_risk_market_id": event_dim.neg_risk_market_id,
        "neg_risk_fee_bips": event_dim.neg_risk_fee_bips,
        "start_date": event_dim.start_date,
        "end_date": event_dim.end_date,
        "created_at_source": event_dim.created_at_source,
        "updated_at_source": event_dim.updated_at_source,
        "last_gamma_sync_at": event_dim.last_gamma_sync_at,
        "last_stream_event_at": event_dim.last_stream_event_at,
        "source_payload_json": event_dim.source_payload_json,
        "created_at": event_dim.created_at,
        "updated_at": event_dim.updated_at,
    }


def _serialize_market_dim(
    market_dim: PolymarketMarketDim,
    latest_params: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "id": market_dim.id,
        "gamma_market_id": market_dim.gamma_market_id,
        "condition_id": market_dim.condition_id,
        "market_slug": market_dim.market_slug,
        "question": market_dim.question,
        "description": market_dim.description,
        "event_dim_id": market_dim.event_dim_id,
        "enable_order_book": market_dim.enable_order_book,
        "active": market_dim.active,
        "closed": market_dim.closed,
        "archived": market_dim.archived,
        "accepting_orders": market_dim.accepting_orders,
        "resolved": market_dim.resolved,
        "resolution_state": market_dim.resolution_state,
        "winning_asset_id": market_dim.winning_asset_id,
        "clob_token_ids_json": market_dim.clob_token_ids_json,
        "outcomes_json": market_dim.outcomes_json,
        "tags_json": market_dim.tags_json,
        "fees_enabled": market_dim.fees_enabled,
        "fee_schedule_json": market_dim.fee_schedule_json,
        "maker_base_fee": _serialize_decimal(market_dim.maker_base_fee),
        "taker_base_fee": _serialize_decimal(market_dim.taker_base_fee),
        "last_gamma_sync_at": market_dim.last_gamma_sync_at,
        "last_stream_event_at": market_dim.last_stream_event_at,
        "source_payload_json": market_dim.source_payload_json,
        "created_at": market_dim.created_at,
        "updated_at": market_dim.updated_at,
        "latest_params": latest_params,
    }


def _serialize_asset_dim(
    asset_dim: PolymarketAssetDim,
    latest_params: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "id": asset_dim.id,
        "asset_id": asset_dim.asset_id,
        "condition_id": asset_dim.condition_id,
        "market_dim_id": asset_dim.market_dim_id,
        "outcome_id": asset_dim.outcome_id,
        "outcome_name": asset_dim.outcome_name,
        "outcome_index": asset_dim.outcome_index,
        "active": asset_dim.active,
        "winner": asset_dim.winner,
        "last_gamma_sync_at": asset_dim.last_gamma_sync_at,
        "last_stream_event_at": asset_dim.last_stream_event_at,
        "source_payload_json": asset_dim.source_payload_json,
        "created_at": asset_dim.created_at,
        "updated_at": asset_dim.updated_at,
        "latest_params": latest_params,
    }


async def _latest_param_history_for_asset_or_market(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str | None,
) -> PolymarketMarketParamHistory | None:
    query = select(PolymarketMarketParamHistory).where(
        PolymarketMarketParamHistory.condition_id == condition_id,
    )
    if asset_id is not None:
        query = query.where(PolymarketMarketParamHistory.asset_id == asset_id)
    else:
        query = query.where(PolymarketMarketParamHistory.asset_id.is_(None))
    query = query.order_by(
        PolymarketMarketParamHistory.observed_at_local.desc(),
        PolymarketMarketParamHistory.id.desc(),
    ).limit(1)
    return (await session.execute(query)).scalar_one_or_none()


async def _latest_param_payload(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str | None,
) -> dict[str, Any] | None:
    row = await _latest_param_history_for_asset_or_market(session, condition_id=condition_id, asset_id=asset_id)
    return _serialize_param_history(row) if row is not None else None


def _apply_updates(model: Any, updates: dict[str, Any]) -> bool:
    changed = False
    for field, value in updates.items():
        if value is None:
            continue
        current = getattr(model, field)
        if current != value:
            setattr(model, field, value)
            changed = True
    return changed


async def _lookup_event_dim(
    session: AsyncSession,
    *,
    gamma_event_id: str | None,
    event_slug: str | None,
) -> PolymarketEventDim | None:
    if gamma_event_id is not None:
        result = await session.execute(
            select(PolymarketEventDim).where(PolymarketEventDim.gamma_event_id == gamma_event_id)
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            return existing
    if event_slug is not None:
        result = await session.execute(
            select(PolymarketEventDim).where(PolymarketEventDim.event_slug == event_slug)
        )
        return result.scalar_one_or_none()
    return None


async def _upsert_event_dim(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    observed_at_local: datetime,
    source_kind: str,
) -> tuple[PolymarketEventDim | None, bool]:
    gamma_event_id = str(payload["id"]) if payload.get("id") is not None else None
    event_slug = payload.get("slug")
    if event_slug is not None:
        event_slug = str(event_slug)

    if gamma_event_id is None and event_slug is None:
        return None, False

    event_dim = await _lookup_event_dim(session, gamma_event_id=gamma_event_id, event_slug=event_slug)
    created = False
    if event_dim is None:
        event_dim = PolymarketEventDim(
            gamma_event_id=gamma_event_id,
            event_slug=event_slug,
        )
        session.add(event_dim)
        created = True

    changed = _apply_updates(
        event_dim,
        {
            "gamma_event_id": gamma_event_id,
            "event_slug": event_slug,
            "event_ticker": str(payload["ticker"]) if payload.get("ticker") is not None else None,
            "title": str(payload["title"]) if payload.get("title") is not None else None,
            "subtitle": str(payload["subtitle"]) if payload.get("subtitle") is not None else None,
            "category": str(payload["category"]) if payload.get("category") is not None else None,
            "subcategory": str(payload["subcategory"]) if payload.get("subcategory") is not None else None,
            "active": _to_bool(payload.get("active")),
            "closed": _to_bool(payload.get("closed")),
            "archived": _to_bool(payload.get("archived")),
            "neg_risk": _to_bool(_coalesce(payload.get("negRisk"), payload.get("neg_risk"))),
            "neg_risk_market_id": (
                str(_coalesce(payload.get("negRiskMarketID"), payload.get("neg_risk_market_id")))
                if _coalesce(payload.get("negRiskMarketID"), payload.get("neg_risk_market_id")) is not None
                else None
            ),
            "neg_risk_fee_bips": (
                int(_coalesce(payload.get("negRiskFeeBips"), payload.get("neg_risk_fee_bips")))
                if _coalesce(payload.get("negRiskFeeBips"), payload.get("neg_risk_fee_bips")) is not None
                else None
            ),
            "start_date": parse_polymarket_timestamp(payload.get("startDate")),
            "end_date": parse_polymarket_timestamp(payload.get("endDate")),
            "created_at_source": parse_polymarket_timestamp(
                _coalesce(payload.get("creationDate"), payload.get("createdAt"), payload.get("created_at"))
            ),
            "updated_at_source": parse_polymarket_timestamp(
                _coalesce(payload.get("updatedAt"), payload.get("updated_at"))
            ),
            "source_payload_json": payload,
        },
    )

    if source_kind.startswith("gamma"):
        if event_dim.last_gamma_sync_at != observed_at_local:
            event_dim.last_gamma_sync_at = observed_at_local
            changed = True
    elif source_kind.startswith("stream"):
        if event_dim.last_stream_event_at != observed_at_local:
            event_dim.last_stream_event_at = observed_at_local
            changed = True

    await session.flush()
    return event_dim, created or changed


async def _lookup_market_dim(
    session: AsyncSession,
    *,
    condition_id: str,
    gamma_market_id: str | None,
) -> PolymarketMarketDim | None:
    result = await session.execute(
        select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == condition_id)
    )
    existing = result.scalar_one_or_none()
    if existing is not None:
        return existing
    if gamma_market_id is not None:
        result = await session.execute(
            select(PolymarketMarketDim).where(PolymarketMarketDim.gamma_market_id == gamma_market_id)
        )
        return result.scalar_one_or_none()
    return None


async def _upsert_market_dim(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    event_dim: PolymarketEventDim | None,
    observed_at_local: datetime,
    source_kind: str,
) -> tuple[PolymarketMarketDim | None, bool]:
    condition_id_value = _coalesce(payload.get("conditionId"), payload.get("condition_id"), payload.get("market"))
    if condition_id_value is None:
        return None, False

    condition_id = str(condition_id_value)
    gamma_market_id = str(payload["id"]) if payload.get("id") is not None else None
    token_ids = _normalize_token_ids(payload)
    winning_asset_id = _winner_from_payload(payload, token_ids)
    resolution_state = _normalize_resolution_state(payload)
    market_dim = await _lookup_market_dim(session, condition_id=condition_id, gamma_market_id=gamma_market_id)
    created = False
    if market_dim is None:
        market_dim = PolymarketMarketDim(
            condition_id=condition_id,
            gamma_market_id=gamma_market_id,
        )
        session.add(market_dim)
        created = True

    changed = _apply_updates(
        market_dim,
        {
            "gamma_market_id": gamma_market_id,
            "market_slug": str(payload["slug"]) if payload.get("slug") is not None else None,
            "question": str(payload["question"]) if payload.get("question") is not None else None,
            "description": str(payload["description"]) if payload.get("description") is not None else None,
            "event_dim_id": event_dim.id if event_dim is not None else None,
            "enable_order_book": _to_bool(_coalesce(payload.get("enableOrderBook"), payload.get("enable_order_book"))),
            "active": _to_bool(payload.get("active")),
            "closed": _to_bool(payload.get("closed")),
            "archived": _to_bool(payload.get("archived")),
            "accepting_orders": _to_bool(_coalesce(payload.get("acceptingOrders"), payload.get("accepting_orders"))),
            "resolved": _resolved_from_payload(payload, winning_asset_id),
            "resolution_state": resolution_state,
            "winning_asset_id": winning_asset_id,
            "clob_token_ids_json": token_ids or None,
            "outcomes_json": _normalize_outcomes(payload) or None,
            "tags_json": _normalize_tags(payload.get("tags")),
            "fees_enabled": _to_bool(_coalesce(payload.get("feesEnabled"), payload.get("fees_enabled"))),
            "fee_schedule_json": _normalize_fee_schedule(_coalesce(payload.get("feeSchedule"), payload.get("fee_schedule"))),
            "maker_base_fee": _to_decimal(_coalesce(payload.get("makerBaseFee"), payload.get("maker_base_fee"))),
            "taker_base_fee": _to_decimal(_coalesce(payload.get("takerBaseFee"), payload.get("taker_base_fee"))),
            "source_payload_json": payload,
        },
    )

    if source_kind.startswith("gamma"):
        if market_dim.last_gamma_sync_at != observed_at_local:
            market_dim.last_gamma_sync_at = observed_at_local
            changed = True
    elif source_kind.startswith("stream"):
        if market_dim.last_stream_event_at != observed_at_local:
            market_dim.last_stream_event_at = observed_at_local
            changed = True

    await session.flush()
    return market_dim, created or changed


async def _upsert_asset_dims(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim,
    payload: dict[str, Any],
    observed_at_local: datetime,
    source_kind: str,
) -> tuple[list[PolymarketAssetDim], int]:
    asset_ids = unique_preserving_order(_normalize_token_ids(payload))
    if not asset_ids:
        return [], 0

    outcomes = _normalize_outcomes(payload)
    winning_asset_id = _winner_from_payload(payload, asset_ids)
    result = await session.execute(select(Outcome).where(Outcome.token_id.in_(asset_ids)))
    outcomes_by_token = {
        str(outcome.token_id): outcome
        for outcome in result.scalars().all()
        if outcome.token_id is not None
    }

    upserted_count = 0
    asset_dims: list[PolymarketAssetDim] = []
    active = _to_bool(payload.get("active"))

    for index, asset_id in enumerate(asset_ids):
        result = await session.execute(
            select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == asset_id)
        )
        asset_dim = result.scalar_one_or_none()
        created = False
        if asset_dim is None:
            asset_dim = PolymarketAssetDim(
                asset_id=asset_id,
                condition_id=market_dim.condition_id,
            )
            session.add(asset_dim)
            created = True

        outcome = outcomes_by_token.get(asset_id)
        changed = _apply_updates(
            asset_dim,
            {
                "condition_id": market_dim.condition_id,
                "market_dim_id": market_dim.id,
                "outcome_id": outcome.id if outcome is not None else None,
                "outcome_name": outcomes[index] if index < len(outcomes) else None,
                "outcome_index": index,
                "active": active,
                "winner": asset_id == winning_asset_id if winning_asset_id is not None else None,
                "source_payload_json": {
                    "asset_id": asset_id,
                    "condition_id": market_dim.condition_id,
                    "outcome_name": outcomes[index] if index < len(outcomes) else None,
                    "outcome_index": index,
                },
            },
        )

        if source_kind.startswith("gamma"):
            if asset_dim.last_gamma_sync_at != observed_at_local:
                asset_dim.last_gamma_sync_at = observed_at_local
                changed = True
        elif source_kind.startswith("stream"):
            if asset_dim.last_stream_event_at != observed_at_local:
                asset_dim.last_stream_event_at = observed_at_local
                changed = True

        if created or changed:
            upserted_count += 1
        asset_dims.append(asset_dim)

    await session.flush()
    return asset_dims, upserted_count


async def _insert_param_history_if_changed(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim | None,
    asset_dim: PolymarketAssetDim | None,
    condition_id: str,
    asset_id: str | None,
    source_kind: str,
    effective_at_exchange: datetime | None,
    received_at_local: datetime | None,
    observed_at_local: datetime,
    sync_run_id: uuid.UUID | None,
    raw_event_id: int | None,
    tick_size: Decimal | None = None,
    min_order_size: Decimal | None = None,
    neg_risk: bool | None = None,
    fees_enabled: bool | None = None,
    fee_schedule_json: dict[str, Any] | None = None,
    maker_base_fee: Decimal | None = None,
    taker_base_fee: Decimal | None = None,
    resolution_state: str | None = None,
    winning_asset_id: str | None = None,
    details_json: dict[str, Any] | None = None,
) -> bool:
    latest = await _latest_param_history_for_asset_or_market(
        session,
        condition_id=condition_id,
        asset_id=asset_id,
    )
    resolved_tick_size = tick_size if tick_size is not None else (latest.tick_size if latest is not None else None)
    resolved_min_order_size = (
        min_order_size if min_order_size is not None else (latest.min_order_size if latest is not None else None)
    )
    resolved_neg_risk = neg_risk if neg_risk is not None else (latest.neg_risk if latest is not None else None)
    resolved_fees_enabled = (
        fees_enabled if fees_enabled is not None else (latest.fees_enabled if latest is not None else None)
    )
    resolved_fee_schedule = (
        fee_schedule_json if fee_schedule_json is not None else (latest.fee_schedule_json if latest is not None else None)
    )
    resolved_maker_fee = (
        maker_base_fee if maker_base_fee is not None else (latest.maker_base_fee if latest is not None else None)
    )
    resolved_taker_fee = (
        taker_base_fee if taker_base_fee is not None else (latest.taker_base_fee if latest is not None else None)
    )
    resolved_resolution_state = (
        resolution_state if resolution_state is not None else (latest.resolution_state if latest is not None else None)
    )
    resolved_winning_asset = (
        winning_asset_id if winning_asset_id is not None else (latest.winning_asset_id if latest is not None else None)
    )

    fingerprint = _build_fingerprint(
        condition_id=condition_id,
        asset_id=asset_id,
        tick_size=resolved_tick_size,
        min_order_size=resolved_min_order_size,
        neg_risk=resolved_neg_risk,
        fees_enabled=resolved_fees_enabled,
        fee_schedule_json=resolved_fee_schedule,
        maker_base_fee=resolved_maker_fee,
        taker_base_fee=resolved_taker_fee,
        resolution_state=resolved_resolution_state,
        winning_asset_id=resolved_winning_asset,
    )
    if latest is not None and latest.fingerprint == fingerprint:
        return False

    row = PolymarketMarketParamHistory(
        market_dim_id=market_dim.id if market_dim is not None else None,
        asset_dim_id=asset_dim.id if asset_dim is not None else None,
        condition_id=condition_id,
        asset_id=asset_id,
        source_kind=source_kind,
        effective_at_exchange=effective_at_exchange,
        received_at_local=received_at_local,
        observed_at_local=observed_at_local,
        sync_run_id=sync_run_id,
        raw_event_id=raw_event_id,
        tick_size=resolved_tick_size,
        min_order_size=resolved_min_order_size,
        neg_risk=resolved_neg_risk,
        fees_enabled=resolved_fees_enabled,
        fee_schedule_json=resolved_fee_schedule,
        maker_base_fee=resolved_maker_fee,
        taker_base_fee=resolved_taker_fee,
        resolution_state=resolved_resolution_state,
        winning_asset_id=resolved_winning_asset,
        fingerprint=fingerprint,
        details_json={
            **(details_json or {}),
            "initial_observation": latest is None,
            "previous_fingerprint": latest.fingerprint if latest is not None else None,
        },
    )
    session.add(row)
    await session.flush()
    return True


async def _seed_params_from_market_payload(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim | None,
    asset_dims: list[PolymarketAssetDim],
    payload: dict[str, Any],
    source_kind: str,
    effective_at_exchange: datetime | None,
    received_at_local: datetime | None,
    observed_at_local: datetime,
    sync_run_id: uuid.UUID | None,
    raw_event_id: int | None,
) -> int:
    if market_dim is None:
        return 0

    asset_targets = asset_dims or [None]
    inserted = 0
    tick_size = _to_decimal(
        _coalesce(payload.get("orderPriceMinTickSize"), payload.get("order_price_min_tick_size"), payload.get("tick_size"))
    )
    min_order_size = _to_decimal(
        _coalesce(payload.get("orderMinSize"), payload.get("order_min_size"), payload.get("min_order_size"))
    )
    neg_risk = _to_bool(_coalesce(payload.get("negRisk"), payload.get("neg_risk")))
    fees_enabled = _to_bool(_coalesce(payload.get("feesEnabled"), payload.get("fees_enabled")))
    fee_schedule_json = _normalize_fee_schedule(_coalesce(payload.get("feeSchedule"), payload.get("fee_schedule")))
    maker_base_fee = _to_decimal(_coalesce(payload.get("makerBaseFee"), payload.get("maker_base_fee")))
    taker_base_fee = _to_decimal(_coalesce(payload.get("takerBaseFee"), payload.get("taker_base_fee")))
    resolution_state = _normalize_resolution_state(payload)
    winning_asset_id = _winner_from_payload(payload, _normalize_token_ids(payload))

    for asset_dim in asset_targets:
        if await _insert_param_history_if_changed(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id if asset_dim is not None else None,
            source_kind=source_kind,
            effective_at_exchange=effective_at_exchange,
            received_at_local=received_at_local,
            observed_at_local=observed_at_local,
            sync_run_id=sync_run_id,
            raw_event_id=raw_event_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            fees_enabled=fees_enabled,
            fee_schedule_json=fee_schedule_json,
            maker_base_fee=maker_base_fee,
            taker_base_fee=taker_base_fee,
            resolution_state=resolution_state,
            winning_asset_id=winning_asset_id,
            details_json={"source_payload_kind": source_kind},
        ):
            inserted += 1

    return inserted


async def _update_event_from_market_resolution(
    session: AsyncSession,
    *,
    event_dim: PolymarketEventDim | None,
) -> None:
    if event_dim is None:
        return
    unresolved_result = await session.execute(
        select(func.count(PolymarketMarketDim.id)).where(
            PolymarketMarketDim.event_dim_id == event_dim.id,
            or_(PolymarketMarketDim.resolved.is_(False), PolymarketMarketDim.resolved.is_(None)),
            or_(PolymarketMarketDim.closed.is_(False), PolymarketMarketDim.closed.is_(None)),
        )
    )
    unresolved_count = int(unresolved_result.scalar_one() or 0)
    if unresolved_count == 0:
        event_dim.closed = True
        event_dim.active = False
        await session.flush()


async def apply_stream_event_to_registry(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    raw_event_id: int,
) -> None:
    async with session_factory() as session:
        raw_event = await session.get(PolymarketMarketEvent, raw_event_id)
        if raw_event is None or not isinstance(raw_event.payload, dict):
            return

        payload = raw_event.payload
        observed_at_local = raw_event.received_at_local
        event_message = payload.get("event_message")
        event_dim = None
        if isinstance(event_message, dict):
            event_dim, _ = await _upsert_event_dim(
                session,
                payload=event_message,
                observed_at_local=observed_at_local,
                source_kind=raw_event.message_type,
            )

        if raw_event.message_type == "new_market":
            market_dim, _ = await _upsert_market_dim(
                session,
                payload=payload,
                event_dim=event_dim,
                observed_at_local=observed_at_local,
                source_kind=STREAM_NEW_MARKET_SOURCE,
            )
            asset_dims, _ = await _upsert_asset_dims(
                session,
                market_dim=market_dim,
                payload=payload,
                observed_at_local=observed_at_local,
                source_kind=STREAM_NEW_MARKET_SOURCE,
            )
            await _seed_params_from_market_payload(
                session,
                market_dim=market_dim,
                asset_dims=asset_dims,
                payload=payload,
                source_kind=STREAM_NEW_MARKET_SOURCE,
                effective_at_exchange=parse_polymarket_timestamp(payload.get("timestamp")),
                received_at_local=observed_at_local,
                observed_at_local=observed_at_local,
                sync_run_id=None,
                raw_event_id=raw_event.id,
            )
        elif raw_event.message_type == "tick_size_change":
            market_dim, _ = await _upsert_market_dim(
                session,
                payload=payload,
                event_dim=event_dim,
                observed_at_local=observed_at_local,
                source_kind=STREAM_TICK_SIZE_SOURCE,
            )
            asset_dims, _ = await _upsert_asset_dims(
                session,
                market_dim=market_dim,
                payload=payload,
                observed_at_local=observed_at_local,
                source_kind=STREAM_TICK_SIZE_SOURCE,
            )
            asset_dim = asset_dims[0] if asset_dims else None
            await _insert_param_history_if_changed(
                session,
                market_dim=market_dim,
                asset_dim=asset_dim,
                condition_id=market_dim.condition_id if market_dim is not None else str(payload.get("market")),
                asset_id=asset_dim.asset_id if asset_dim is not None else raw_event.asset_id,
                source_kind=STREAM_TICK_SIZE_SOURCE,
                effective_at_exchange=parse_polymarket_timestamp(payload.get("timestamp")),
                received_at_local=observed_at_local,
                observed_at_local=observed_at_local,
                sync_run_id=None,
                raw_event_id=raw_event.id,
                tick_size=_to_decimal(_coalesce(payload.get("new_tick_size"), payload.get("tick_size"))),
                details_json={
                    "old_tick_size": payload.get("old_tick_size"),
                    "new_tick_size": payload.get("new_tick_size"),
                },
            )
        elif raw_event.message_type == "market_resolved":
            market_dim, _ = await _upsert_market_dim(
                session,
                payload=payload,
                event_dim=event_dim,
                observed_at_local=observed_at_local,
                source_kind=STREAM_RESOLUTION_SOURCE,
            )
            asset_dims, _ = await _upsert_asset_dims(
                session,
                market_dim=market_dim,
                payload=payload,
                observed_at_local=observed_at_local,
                source_kind=STREAM_RESOLUTION_SOURCE,
            )
            await _seed_params_from_market_payload(
                session,
                market_dim=market_dim,
                asset_dims=asset_dims,
                payload=payload,
                source_kind=STREAM_RESOLUTION_SOURCE,
                effective_at_exchange=parse_polymarket_timestamp(payload.get("timestamp")),
                received_at_local=observed_at_local,
                observed_at_local=observed_at_local,
                sync_run_id=None,
                raw_event_id=raw_event.id,
            )
            await _update_event_from_market_resolution(session, event_dim=event_dim)

        await session.commit()


async def seed_registry_from_book_snapshot(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    observed_at_local: datetime,
    sync_run_id: uuid.UUID | None,
    raw_event_id: int | None,
    source_kind: str = BOOK_SEED_SOURCE,
) -> int:
    market_dim, _ = await _upsert_market_dim(
        session,
        payload=payload,
        event_dim=None,
        observed_at_local=observed_at_local,
        source_kind=source_kind,
    )
    if market_dim is None:
        return 0
    asset_dims, _ = await _upsert_asset_dims(
        session,
        market_dim=market_dim,
        payload=payload,
        observed_at_local=observed_at_local,
        source_kind=source_kind,
    )
    inserted = 0
    for asset_dim in asset_dims or [None]:
        if await _insert_param_history_if_changed(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id if asset_dim is not None else None,
            source_kind=source_kind,
            effective_at_exchange=parse_polymarket_timestamp(payload.get("timestamp")),
            received_at_local=observed_at_local,
            observed_at_local=observed_at_local,
            sync_run_id=sync_run_id,
            raw_event_id=raw_event_id,
            tick_size=_to_decimal(payload.get("tick_size")),
            min_order_size=_to_decimal(payload.get("min_order_size")),
            neg_risk=_to_bool(payload.get("neg_risk")),
            details_json={"source_payload_kind": source_kind},
        ):
            inserted += 1
    return inserted


class PolymarketMetaSyncService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
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

    async def _iter_keyset_pages(
        self,
        *,
        path: str,
        root_key: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        client = await self._get_client()
        items: list[dict[str, Any]] = []
        next_cursor: str | None = None
        while True:
            request_params = dict(params)
            if next_cursor:
                request_params["after_cursor"] = next_cursor
            response = await client.get(f"{settings.polymarket_gamma_base}{path}", params=request_params)
            response.raise_for_status()
            payload = response.json()
            page_items = payload.get(root_key) if isinstance(payload, dict) else None
            if not isinstance(page_items, list):
                break
            for item in page_items:
                if isinstance(item, dict):
                    items.append(item)
            next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
            if not next_cursor or not page_items:
                break
        return items

    def _event_limit(self) -> int:
        return max(1, min(settings.polymarket_meta_sync_page_size, 500))

    def _market_limit(self) -> int:
        return max(1, min(settings.polymarket_meta_sync_page_size, 1000))

    async def _sync_gamma_events(
        self,
        session: AsyncSession,
        *,
        counters: MetaSyncCounters,
        include_closed: bool,
    ) -> None:
        for closed_flag in ([False, True] if include_closed else [False]):
            items = await self._iter_keyset_pages(
                path="/events/keyset",
                root_key="events",
                params={
                    "limit": self._event_limit(),
                    "closed": str(closed_flag).lower(),
                },
            )
            for event_payload in items:
                counters.events_seen += 1
                _, changed = await _upsert_event_dim(
                    session,
                    payload=event_payload,
                    observed_at_local=utcnow(),
                    source_kind=GAMMA_EVENT_SOURCE,
                )
                if changed:
                    counters.events_upserted += 1

    async def _sync_gamma_markets(
        self,
        session: AsyncSession,
        *,
        counters: MetaSyncCounters,
        include_closed: bool,
        target_asset_ids: list[str] | None,
        sync_run_id: uuid.UUID,
        reward_configs_by_condition: dict[str, dict[str, Any]] | None,
    ) -> None:
        for closed_flag in ([False, True] if include_closed else [False]):
            params: dict[str, Any] = {
                "limit": self._market_limit(),
                "closed": str(closed_flag).lower(),
            }
            if target_asset_ids:
                params["clob_token_ids"] = target_asset_ids
            items = await self._iter_keyset_pages(
                path="/markets/keyset",
                root_key="markets",
                params=params,
            )
            for market_payload in items:
                counters.markets_seen += 1
                observed_at_local = utcnow()

                event_dim = None
                events = market_payload.get("events")
                if isinstance(events, list) and events:
                    primary_event = next((item for item in events if isinstance(item, dict)), None)
                    if primary_event is not None:
                        event_dim, event_changed = await _upsert_event_dim(
                            session,
                            payload=primary_event,
                            observed_at_local=observed_at_local,
                            source_kind=GAMMA_MARKET_SOURCE,
                        )
                        if event_changed:
                            counters.events_upserted += 1

                market_dim, market_changed = await _upsert_market_dim(
                    session,
                    payload=market_payload,
                    event_dim=event_dim,
                    observed_at_local=observed_at_local,
                    source_kind=GAMMA_MARKET_SOURCE,
                )
                if market_changed:
                    counters.markets_upserted += 1

                if market_dim is None:
                    counters.error_count += 1
                    continue

                asset_dims, asset_upserted = await _upsert_asset_dims(
                    session,
                    market_dim=market_dim,
                    payload=market_payload,
                    observed_at_local=observed_at_local,
                    source_kind=GAMMA_MARKET_SOURCE,
                )
                counters.assets_upserted += asset_upserted

                counters.param_rows_inserted += await _seed_params_from_market_payload(
                    session,
                    market_dim=market_dim,
                    asset_dims=asset_dims,
                    payload=market_payload,
                    source_kind=GAMMA_MARKET_SOURCE,
                    effective_at_exchange=_market_effective_at(market_payload),
                    received_at_local=observed_at_local,
                    observed_at_local=observed_at_local,
                    sync_run_id=sync_run_id,
                    raw_event_id=None,
                )
                counters.fee_rows_inserted += await self._sync_market_fee_history(
                    session,
                    market_dim=market_dim,
                    asset_dims=asset_dims,
                    observed_at_local=observed_at_local,
                    sync_run_id=sync_run_id,
                    market_payload=market_payload,
                )
                counters.reward_rows_inserted += await self._sync_market_reward_history(
                    session,
                    market_dim=market_dim,
                    observed_at_local=observed_at_local,
                    sync_run_id=sync_run_id,
                    reward_payload=(
                        reward_configs_by_condition.get(market_dim.condition_id)
                        if reward_configs_by_condition is not None
                        else None
                    ),
                )

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

    async def _fetch_token_fee_rate(self, asset_id: str) -> dict[str, Any] | None:
        client = await self._get_client()
        response = await client.get(f"{settings.polymarket_api_base}/fee-rate/{asset_id}")
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None

    async def _fetch_current_reward_configs(self) -> dict[str, dict[str, Any]]:
        client = await self._get_client()
        configs_by_condition: dict[str, dict[str, Any]] = {}
        next_cursor: str | None = None
        while True:
            params: dict[str, Any] = {}
            if next_cursor:
                params["next_cursor"] = next_cursor
            response = await client.get(f"{settings.polymarket_api_base}/rewards/markets/current", params=params)
            response.raise_for_status()
            payload = response.json()
            rows: list[dict[str, Any]] = []
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
            elif isinstance(payload, dict):
                if isinstance(payload.get("data"), list):
                    rows = [row for row in payload["data"] if isinstance(row, dict)]
                elif isinstance(payload.get("markets"), list):
                    rows = [row for row in payload["markets"] if isinstance(row, dict)]
            for row in rows:
                condition_id = _coalesce(
                    row.get("condition_id"),
                    row.get("market"),
                    row.get("market_id"),
                )
                if condition_id is None:
                    continue
                configs_by_condition[str(condition_id)] = row
            next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
            if not next_cursor or not rows:
                break
        return configs_by_condition

    async def _sync_market_fee_history(
        self,
        session: AsyncSession,
        *,
        market_dim: PolymarketMarketDim,
        asset_dims: list[PolymarketAssetDim],
        observed_at_local: datetime,
        sync_run_id: uuid.UUID,
        market_payload: dict[str, Any],
    ) -> int:
        if not settings.polymarket_fee_history_enabled:
            return 0
        inserted = 0
        fees_enabled = _to_bool(_coalesce(market_payload.get("feesEnabled"), market_payload.get("fees_enabled")))
        fee_schedule_json = _normalize_fee_schedule(_coalesce(market_payload.get("feeSchedule"), market_payload.get("fee_schedule")))
        maker_base_fee = _to_decimal(_coalesce(market_payload.get("makerBaseFee"), market_payload.get("maker_base_fee")))
        taker_base_fee = _to_decimal(_coalesce(market_payload.get("takerBaseFee"), market_payload.get("taker_base_fee")))
        effective_at_exchange = _market_effective_at(market_payload)
        for asset_dim in asset_dims:
            fee_payload = None
            try:
                fee_payload = await self._fetch_token_fee_rate(asset_dim.asset_id)
            except httpx.HTTPError:
                logger.warning("Failed to fetch fee rate for asset %s", asset_dim.asset_id, exc_info=True)
            token_base_fee_rate = _to_decimal(_coalesce(
                fee_payload.get("base_fee") if isinstance(fee_payload, dict) else None,
                fee_payload.get("baseFee") if isinstance(fee_payload, dict) else None,
            ))
            if await insert_token_fee_history_if_changed(
                session,
                market_dim=market_dim,
                asset_dim=asset_dim,
                condition_id=market_dim.condition_id,
                asset_id=asset_dim.asset_id,
                source_kind=FEE_SOURCE_KIND,
                effective_at_exchange=effective_at_exchange,
                observed_at_local=observed_at_local,
                sync_run_id=sync_run_id,
                fees_enabled=fees_enabled,
                maker_fee_rate=maker_base_fee,
                taker_fee_rate=taker_base_fee,
                token_base_fee_rate=token_base_fee_rate,
                fee_schedule_json=fee_schedule_json,
                details_json={
                    "market_fee_schedule": fee_schedule_json,
                    "fee_rate_payload": fee_payload,
                },
            ):
                inserted += 1
        return inserted

    async def _sync_market_reward_history(
        self,
        session: AsyncSession,
        *,
        market_dim: PolymarketMarketDim,
        observed_at_local: datetime,
        sync_run_id: uuid.UUID,
        reward_payload: dict[str, Any] | None,
    ) -> int:
        if not settings.polymarket_reward_history_enabled:
            return 0
        normalized = normalize_reward_history_payload(reward_payload, observed_at=observed_at_local)
        if await insert_reward_history_if_changed(
            session,
            market_dim=market_dim,
            condition_id=market_dim.condition_id,
            source_kind=REWARD_SOURCE_KIND,
            effective_at_exchange=normalized["start_at_exchange"] or observed_at_local,
            observed_at_local=observed_at_local,
            sync_run_id=sync_run_id,
            reward_status=normalized["reward_status"],
            reward_program_id=normalized["reward_program_id"],
            reward_daily_rate=normalized["reward_daily_rate"],
            min_incentive_size=normalized["min_incentive_size"],
            max_incentive_spread=normalized["max_incentive_spread"],
            start_at_exchange=normalized["start_at_exchange"],
            end_at_exchange=normalized["end_at_exchange"],
            rewards_config_json=normalized["rewards_config_json"],
            details_json={"reward_payload": reward_payload},
        ):
            return 1
        return 0

    async def _watched_assets_missing_metadata(
        self,
        session: AsyncSession,
        *,
        explicit_asset_ids: list[str] | None,
    ) -> list[str]:
        if explicit_asset_ids:
            return unique_preserving_order([str(asset_id) for asset_id in explicit_asset_ids if asset_id])

        result = await session.execute(
            select(PolymarketWatchAsset.asset_id).where(PolymarketWatchAsset.watch_enabled.is_(True))
        )
        watched_assets = unique_preserving_order([str(asset_id) for asset_id in result.scalars().all()])
        missing: list[str] = []
        for asset_id in watched_assets:
            condition_id = (
                (
                    await session.execute(
                        select(PolymarketAssetDim.condition_id).where(PolymarketAssetDim.asset_id == asset_id)
                    )
                ).scalar_one_or_none()
                or asset_id
            )
            latest = await _latest_param_history_for_asset_or_market(
                session,
                condition_id=condition_id,
                asset_id=asset_id,
            )
            if latest is None or latest.tick_size is None or latest.min_order_size is None or latest.neg_risk is None:
                missing.append(asset_id)
        return missing

    async def _seed_missing_book_metadata(
        self,
        session: AsyncSession,
        *,
        counters: MetaSyncCounters,
        sync_run_id: uuid.UUID,
        explicit_asset_ids: list[str] | None,
    ) -> None:
        target_asset_ids = await self._watched_assets_missing_metadata(session, explicit_asset_ids=explicit_asset_ids)
        if not target_asset_ids:
            return
        payloads = await self._fetch_books_batch(target_asset_ids)
        payloads_by_asset = {
            str(_coalesce(payload.get("asset_id"), payload.get("assetId"))): payload
            for payload in payloads
            if isinstance(payload, dict) and _coalesce(payload.get("asset_id"), payload.get("assetId")) is not None
        }
        for asset_id in target_asset_ids:
            payload = payloads_by_asset.get(asset_id)
            if payload is None:
                counters.error_count += 1
                continue
            observed_at_local = utcnow()
            counters.param_rows_inserted += await seed_registry_from_book_snapshot(
                session,
                payload=payload,
                observed_at_local=observed_at_local,
                sync_run_id=sync_run_id,
                raw_event_id=None,
                source_kind=BOOK_SEED_SOURCE,
            )

    async def sync_metadata(
        self,
        *,
        reason: str,
        include_closed: bool | None = None,
        asset_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        include_closed_value = settings.polymarket_meta_sync_include_closed if include_closed is None else include_closed
        sync_run_id = uuid.uuid4()
        target_asset_ids = unique_preserving_order([str(asset_id) for asset_id in (asset_ids or []) if asset_id])

        async with self._session_factory() as session:
            run = PolymarketMetaSyncRun(
                id=sync_run_id,
                status="running",
                reason=reason,
                include_closed=include_closed_value,
                details_json={"target_asset_ids": target_asset_ids or None},
            )
            session.add(run)
            await session.commit()

        counters = MetaSyncCounters()
        failure_exc: Exception | None = None
        try:
            async with self._session_factory() as session:
                reward_configs_by_condition = (
                    await self._fetch_current_reward_configs()
                    if settings.polymarket_reward_history_enabled
                    else None
                )
                if not target_asset_ids:
                    await self._sync_gamma_events(
                        session,
                        counters=counters,
                        include_closed=include_closed_value,
                    )
                await self._sync_gamma_markets(
                    session,
                    counters=counters,
                    include_closed=include_closed_value,
                    target_asset_ids=target_asset_ids or None,
                    sync_run_id=sync_run_id,
                    reward_configs_by_condition=reward_configs_by_condition,
                )
                await self._seed_missing_book_metadata(
                    session,
                    counters=counters,
                    sync_run_id=sync_run_id,
                    explicit_asset_ids=target_asset_ids or None,
                )

                run = await session.get(PolymarketMetaSyncRun, sync_run_id)
                assert run is not None
                run.completed_at = utcnow()
                run.status = "completed" if counters.error_count == 0 else "partial"
                run.events_seen = counters.events_seen
                run.markets_seen = counters.markets_seen
                run.assets_upserted = counters.assets_upserted
                run.events_upserted = counters.events_upserted
                run.markets_upserted = counters.markets_upserted
                run.param_rows_inserted = counters.param_rows_inserted
                run.error_count = counters.error_count
                run.details_json = {
                    "target_asset_ids": target_asset_ids or None,
                    "include_closed": include_closed_value,
                    "fee_rows_inserted": counters.fee_rows_inserted,
                    "reward_rows_inserted": counters.reward_rows_inserted,
                }
                await session.commit()
        except Exception as exc:
            failure_exc = exc
            async with self._session_factory() as session:
                run = await session.get(PolymarketMetaSyncRun, sync_run_id)
                if run is not None:
                    run.completed_at = utcnow()
                    run.status = "failed"
                    run.events_seen = counters.events_seen
                    run.markets_seen = counters.markets_seen
                    run.assets_upserted = counters.assets_upserted
                    run.events_upserted = counters.events_upserted
                    run.markets_upserted = counters.markets_upserted
                    run.param_rows_inserted = counters.param_rows_inserted
                    run.error_count = counters.error_count + 1
                    run.details_json = {
                        "target_asset_ids": target_asset_ids or None,
                        "include_closed": include_closed_value,
                        "fee_rows_inserted": counters.fee_rows_inserted,
                        "reward_rows_inserted": counters.reward_rows_inserted,
                        "error": str(exc),
                    }
                    await session.commit()

        async with self._session_factory() as session:
            run = await session.get(PolymarketMetaSyncRun, sync_run_id)
            assert run is not None
            serialized = _serialize_sync_run(run)
            status = run.status

        polymarket_meta_sync_runs.labels(reason=reason, status=status).inc()
        if status in {"failed", "partial"}:
            polymarket_meta_sync_failures.inc()
        polymarket_meta_events_upserted.inc(counters.events_upserted)
        polymarket_meta_markets_upserted.inc(counters.markets_upserted)
        polymarket_meta_assets_upserted.inc(counters.assets_upserted)
        polymarket_meta_param_rows_inserted.inc(counters.param_rows_inserted)

        if status in {"completed", "partial"} and serialized["completed_at"] is not None:
            completed_at = serialized["completed_at"]
            polymarket_meta_last_successful_sync_timestamp.set(completed_at.timestamp())
            polymarket_meta_last_successful_sync_age_seconds.set(0)

        if failure_exc is not None:
            raise failure_exc
        return serialized

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_meta_sync_enabled:
            logger.info("Polymarket metadata sync disabled; skipping worker startup")
            return

        if settings.polymarket_meta_sync_on_startup:
            try:
                await self.sync_metadata(reason="startup")
            except Exception:
                logger.warning("Polymarket metadata startup sync failed", exc_info=True)

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(1, settings.polymarket_meta_sync_interval_seconds),
                )
            except asyncio.TimeoutError:
                try:
                    await self.sync_metadata(reason="scheduled")
                except Exception:
                    logger.warning("Polymarket metadata scheduled sync failed", exc_info=True)


async def list_polymarket_meta_sync_runs(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    total_result = await session.execute(select(func.count(PolymarketMetaSyncRun.id)))
    total = int(total_result.scalar_one() or 0)
    result = await session.execute(
        select(PolymarketMetaSyncRun)
        .order_by(PolymarketMetaSyncRun.started_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return [_serialize_sync_run(row) for row in result.scalars().all()], total


async def fetch_polymarket_meta_sync_status(session: AsyncSession) -> dict[str, Any]:
    latest_run = (
        await session.execute(
            select(PolymarketMetaSyncRun)
            .order_by(PolymarketMetaSyncRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    last_successful_sync_at = (
        await session.execute(
            select(func.max(PolymarketMetaSyncRun.completed_at)).where(
                PolymarketMetaSyncRun.status.in_(("completed", "partial"))
            )
        )
    ).scalar_one_or_none()

    now = utcnow()
    recent_rows = (
        await session.execute(
            select(PolymarketMarketParamHistory)
            .where(PolymarketMarketParamHistory.observed_at_local >= now - timedelta(hours=24))
            .order_by(PolymarketMarketParamHistory.observed_at_local.desc())
        )
    ).scalars().all()
    recent_param_changes_24h = sum(
        1 for row in recent_rows if not (row.details_json or {}).get("initial_observation", False)
    )

    stale_after_seconds = max(settings.polymarket_meta_sync_interval_seconds * 2, 300)
    stale_before = now - timedelta(seconds=stale_after_seconds)
    stale_event_count = int(
        (
            await session.execute(
                select(func.count(PolymarketEventDim.id)).where(
                    or_(
                        func.coalesce(PolymarketEventDim.last_gamma_sync_at, PolymarketEventDim.last_stream_event_at)
                        < stale_before,
                        (
                            PolymarketEventDim.last_gamma_sync_at.is_(None)
                            & PolymarketEventDim.last_stream_event_at.is_(None)
                        ),
                    )
                )
            )
        ).scalar_one()
        or 0
    )
    stale_market_count = int(
        (
            await session.execute(
                select(func.count(PolymarketMarketDim.id)).where(
                    or_(
                        func.coalesce(PolymarketMarketDim.last_gamma_sync_at, PolymarketMarketDim.last_stream_event_at)
                        < stale_before,
                        (
                            PolymarketMarketDim.last_gamma_sync_at.is_(None)
                            & PolymarketMarketDim.last_stream_event_at.is_(None)
                        ),
                    )
                )
            )
        ).scalar_one()
        or 0
    )
    stale_asset_count = int(
        (
            await session.execute(
                select(func.count(PolymarketAssetDim.id)).where(
                    or_(
                        func.coalesce(PolymarketAssetDim.last_gamma_sync_at, PolymarketAssetDim.last_stream_event_at)
                        < stale_before,
                        (
                            PolymarketAssetDim.last_gamma_sync_at.is_(None)
                            & PolymarketAssetDim.last_stream_event_at.is_(None)
                        ),
                    )
                )
            )
        ).scalar_one()
        or 0
    )

    event_count = int((await session.execute(select(func.count(PolymarketEventDim.id)))).scalar_one() or 0)
    market_count = int((await session.execute(select(func.count(PolymarketMarketDim.id)))).scalar_one() or 0)
    asset_count = int((await session.execute(select(func.count(PolymarketAssetDim.id)))).scalar_one() or 0)

    recent_runs_result = await session.execute(
        select(PolymarketMetaSyncRun)
        .order_by(PolymarketMetaSyncRun.started_at.desc())
        .limit(5)
    )
    recent_runs = [_serialize_sync_run(row) for row in recent_runs_result.scalars().all()]

    freshness_seconds = None
    if last_successful_sync_at is not None:
        if last_successful_sync_at.tzinfo is None:
            last_successful_sync_at = last_successful_sync_at.replace(tzinfo=now.tzinfo)
        freshness_seconds = max(0, int((now - last_successful_sync_at).total_seconds()))
        polymarket_meta_last_successful_sync_timestamp.set(last_successful_sync_at.timestamp())
        polymarket_meta_last_successful_sync_age_seconds.set(freshness_seconds)
    polymarket_meta_registry_stale_rows.labels(kind="events").set(stale_event_count)
    polymarket_meta_registry_stale_rows.labels(kind="markets").set(stale_market_count)
    polymarket_meta_registry_stale_rows.labels(kind="assets").set(stale_asset_count)

    return {
        "enabled": settings.polymarket_meta_sync_enabled,
        "on_startup": settings.polymarket_meta_sync_on_startup,
        "interval_seconds": settings.polymarket_meta_sync_interval_seconds,
        "include_closed": settings.polymarket_meta_sync_include_closed,
        "page_size": settings.polymarket_meta_sync_page_size,
        "last_successful_sync_at": last_successful_sync_at,
        "last_run_status": latest_run.status if latest_run is not None else None,
        "last_run_started_at": latest_run.started_at if latest_run is not None else None,
        "last_run_completed_at": latest_run.completed_at if latest_run is not None else None,
        "last_run_id": latest_run.id if latest_run is not None else None,
        "recent_param_changes_24h": recent_param_changes_24h,
        "stale_registry_counts": {
            "events": stale_event_count,
            "markets": stale_market_count,
            "assets": stale_asset_count,
        },
        "registry_counts": {
            "events": event_count,
            "markets": market_count,
            "assets": asset_count,
        },
        "stale_after_seconds": stale_after_seconds,
        "freshness_seconds": freshness_seconds,
        "recent_sync_runs": recent_runs,
    }


async def lookup_polymarket_event_registry(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_slug: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketEventDim)
    if event_slug:
        query = query.where(PolymarketEventDim.event_slug == event_slug)
    elif condition_id or asset_id:
        query = query.join(PolymarketMarketDim, PolymarketMarketDim.event_dim_id == PolymarketEventDim.id)
        if asset_id:
            query = query.join(PolymarketAssetDim, PolymarketAssetDim.market_dim_id == PolymarketMarketDim.id).where(
                PolymarketAssetDim.asset_id == asset_id
            )
        if condition_id:
            query = query.where(PolymarketMarketDim.condition_id == condition_id)
    result = await session.execute(query.order_by(PolymarketEventDim.updated_at.desc()).limit(limit))
    return [_serialize_event_dim(row) for row in result.scalars().unique().all()]


async def lookup_polymarket_market_registry(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_slug: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketMarketDim)
    if asset_id:
        query = query.join(PolymarketAssetDim, PolymarketAssetDim.market_dim_id == PolymarketMarketDim.id).where(
            PolymarketAssetDim.asset_id == asset_id
        )
    if condition_id:
        query = query.where(PolymarketMarketDim.condition_id == condition_id)
    if event_slug:
        query = query.join(PolymarketEventDim, PolymarketMarketDim.event_dim_id == PolymarketEventDim.id).where(
            PolymarketEventDim.event_slug == event_slug
        )
    result = await session.execute(query.order_by(PolymarketMarketDim.updated_at.desc()).limit(limit))
    rows = result.scalars().unique().all()
    serialized: list[dict[str, Any]] = []
    for row in rows:
        serialized.append(
            _serialize_market_dim(
                row,
                latest_params=await _latest_param_payload(session, condition_id=row.condition_id, asset_id=None),
            )
        )
    return serialized


async def lookup_polymarket_asset_registry(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_slug: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketAssetDim)
    if asset_id:
        query = query.where(PolymarketAssetDim.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketAssetDim.condition_id == condition_id)
    if event_slug:
        query = (
            query.join(PolymarketMarketDim, PolymarketAssetDim.market_dim_id == PolymarketMarketDim.id)
            .join(PolymarketEventDim, PolymarketMarketDim.event_dim_id == PolymarketEventDim.id)
            .where(PolymarketEventDim.event_slug == event_slug)
        )
    result = await session.execute(query.order_by(PolymarketAssetDim.updated_at.desc()).limit(limit))
    rows = result.scalars().unique().all()
    serialized: list[dict[str, Any]] = []
    for row in rows:
        serialized.append(
            _serialize_asset_dim(
                row,
                latest_params=await _latest_param_payload(
                    session,
                    condition_id=row.condition_id,
                    asset_id=row.asset_id,
                ),
            )
        )
    return serialized


async def lookup_polymarket_market_param_history(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_slug: str | None,
    changed_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketMarketParamHistory)
    if asset_id:
        query = query.where(PolymarketMarketParamHistory.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketMarketParamHistory.condition_id == condition_id)
    if event_slug:
        query = (
            query.join(PolymarketMarketDim, PolymarketMarketParamHistory.market_dim_id == PolymarketMarketDim.id)
            .join(PolymarketEventDim, PolymarketMarketDim.event_dim_id == PolymarketEventDim.id)
            .where(PolymarketEventDim.event_slug == event_slug)
        )
    fetch_limit = min(max(limit * 5, limit), 500)
    result = await session.execute(
        query.order_by(
            PolymarketMarketParamHistory.observed_at_local.desc(),
            PolymarketMarketParamHistory.id.desc(),
        ).limit(fetch_limit)
    )
    rows = result.scalars().all()
    if changed_only:
        rows = [row for row in rows if not (row.details_json or {}).get("initial_observation", False)]
    return [_serialize_param_history(row) for row in rows[:limit]]


async def trigger_manual_polymarket_meta_sync(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    include_closed: bool | None = None,
    asset_ids: list[str] | None = None,
) -> dict[str, Any]:
    service = PolymarketMetaSyncService(session_factory)
    try:
        return await service.sync_metadata(
            reason=reason,
            include_closed=include_closed,
            asset_ids=asset_ids,
        )
    finally:
        await service.close()
