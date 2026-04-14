from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.ingestion.polymarket_execution_policy import (
    ONE,
    PRICE_Q,
    ZERO,
    PolymarketExecutionContext,
    _ensure_utc,
    _entry_price_for_direction,
    _estimate_taker_fee_total,
    _latest_param_history,
    _passive_label_summary,
    _per_share_from_bps,
    _rebuild_current_book,
    _resolve_maker_fee_rate,
    _resolve_taker_fee_rate,
)
from app.ingestion.polymarket_risk_graph import compute_quote_inventory_controls
from app.metrics import (
    polymarket_maker_economics_reason_codes,
    polymarket_maker_economics_snapshots,
    polymarket_maker_fee_history_rows,
    polymarket_maker_last_fee_sync_timestamp,
    polymarket_maker_last_reward_sync_timestamp,
    polymarket_maker_last_snapshot_timestamp,
    polymarket_maker_reward_history_rows,
    polymarket_maker_reward_states,
    polymarket_quote_recommendation_reason_codes,
    polymarket_quote_recommendations,
    polymarket_quote_optimizer_last_recommendation_timestamp,
)
from app.models.market_structure import (
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
    MarketStructureValidation,
)
from app.models.polymarket_maker import (
    PolymarketMakerEconomicsSnapshot,
    PolymarketMarketRewardConfigHistory,
    PolymarketQuoteRecommendation,
    PolymarketTokenFeeRateHistory,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_reconstruction import PolymarketBookReconState

FEE_SOURCE_KIND = "clob_fee_rate"
REWARD_SOURCE_KIND = "clob_reward_config"
ESTIMATOR_VERSION = "phase9_maker_economics_v1"
QUOTE_OPTIMIZER_VERSION = "phase9_quote_optimizer_v1"
ACTIVE_REWARD_STATES = {"active", "eligible"}
BLOCKING_REASON_CODES = {
    "fee_history_disabled",
    "maker_economics_disabled",
    "missing_fee_data",
    "missing_polymarket_leg",
    "no_structure_validation",
    "policy_blocked_recommendation",
    "quote_optimizer_disabled",
    "stale_economics_inputs",
    "unsupported_quoting_case",
}
DOWNGRADE_REASON_CODES = {
    "downgraded_confidence",
    "incomplete_economics",
    "missing_reward_config",
    "no_positive_taker_edge",
    "reward_not_active",
    "reward_size_ineligible",
    "reward_spread_ineligible",
    "unsupported_reward_structure",
}


@dataclass(slots=True)
class StructureEconomicsContext:
    opportunity: MarketStructureOpportunity
    validation: MarketStructureValidation | None
    legs: list[MarketStructureOpportunityLeg]
    maker_leg: MarketStructureOpportunityLeg | None
    market_dim: PolymarketMarketDim | None
    asset_dim: PolymarketAssetDim | None
    fee_state: PolymarketTokenFeeRateHistory | None
    reward_state: PolymarketMarketRewardConfigHistory | None
    as_of: datetime
    recon_state: PolymarketBookReconState | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    tick_size: Decimal | None
    reliable_book: bool
    book_reason: str | None
    snapshot_age_seconds: int | None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
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


def _stable_json(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _serialize_decimal(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _history_timestamp(row: Any) -> datetime | None:
    return _ensure_utc(getattr(row, "effective_at_exchange", None)) or _ensure_utc(
        getattr(row, "observed_at_local", None)
    )


def _as_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            timestamp = int(stripped)
            if len(stripped) > 10:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return None
        return _ensure_utc(parsed)
    return None


def _normalize_max_incentive_spread(value: Any) -> Decimal | None:
    parsed = _to_decimal(value)
    if parsed is None:
        return None
    if parsed > ONE:
        return (parsed / Decimal("100")).quantize(PRICE_Q)
    return parsed.quantize(PRICE_Q)


def _normalize_reward_daily_rate(payload: dict[str, Any]) -> Decimal | None:
    direct = _to_decimal(
        _coalesce(
            payload.get("reward_daily_rate"),
            payload.get("daily_rate"),
            payload.get("total_daily_rate"),
            payload.get("dailyReward"),
            payload.get("daily_reward"),
            payload.get("rewardsDailyRate"),
            payload.get("rewards_daily_rate"),
        )
    )
    if direct is not None:
        return direct
    rewards_config = payload.get("rewards_config")
    if isinstance(rewards_config, list):
        total = ZERO
        seen = False
        for row in rewards_config:
            if not isinstance(row, dict):
                continue
            candidate = _to_decimal(
                _coalesce(
                    row.get("rate"),
                    row.get("daily_rate"),
                    row.get("dailyReward"),
                    row.get("daily_reward"),
                    row.get("reward_daily_rate"),
                )
            )
            if candidate is None:
                continue
            total += candidate
            seen = True
        if seen:
            return total.quantize(PRICE_Q)
    return None


def _normalize_reward_configs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    configs = payload.get("rewards_config")
    if isinstance(configs, list):
        return [row for row in configs if isinstance(row, dict)]
    return []


def _normalize_reward_program_id(payload: dict[str, Any]) -> str | None:
    value = _coalesce(
        payload.get("reward_program_id"),
        payload.get("program_id"),
        payload.get("id"),
    )
    return str(value) if value is not None else None


def _normalize_reward_state(
    payload: dict[str, Any] | None,
    *,
    observed_at: datetime,
) -> str:
    if payload is None:
        return "missing"
    if not isinstance(payload, dict):
        return "unknown"
    raw_state = _coalesce(payload.get("status"), payload.get("reward_status"), payload.get("state"))
    if raw_state is not None:
        lowered = str(raw_state).strip().lower()
        if lowered in {"active", "eligible", "expired", "missing", "unknown"}:
            return lowered
    end_at = _as_datetime(_coalesce(payload.get("end_at_exchange"), payload.get("endDate"), payload.get("end_date")))
    if end_at is not None and end_at < observed_at:
        return "expired"
    configs = _normalize_reward_configs(payload)
    if configs or _normalize_reward_daily_rate(payload) is not None:
        return "active"
    return "unknown"


def normalize_reward_history_payload(
    payload: dict[str, Any] | None,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    reward_payload = payload or {}
    return {
        "reward_status": _normalize_reward_state(payload, observed_at=observed_at),
        "reward_program_id": _normalize_reward_program_id(reward_payload),
        "reward_daily_rate": _normalize_reward_daily_rate(reward_payload),
        "min_incentive_size": _to_decimal(
            _coalesce(
                reward_payload.get("min_incentive_size"),
                reward_payload.get("minIncentiveSize"),
            )
        ),
        "max_incentive_spread": _normalize_max_incentive_spread(
            _coalesce(
                reward_payload.get("max_incentive_spread"),
                reward_payload.get("maxIncentiveSpread"),
            )
        ),
        "start_at_exchange": _as_datetime(
            _coalesce(
                reward_payload.get("start_at_exchange"),
                reward_payload.get("startDate"),
                reward_payload.get("start_date"),
            )
        ),
        "end_at_exchange": _as_datetime(
            _coalesce(
                reward_payload.get("end_at_exchange"),
                reward_payload.get("endDate"),
                reward_payload.get("end_date"),
            )
        ),
        "rewards_config_json": _normalize_reward_configs(reward_payload),
    }


def _build_fee_history_fingerprint(
    *,
    condition_id: str,
    asset_id: str,
    fees_enabled: bool | None,
    maker_fee_rate: Decimal | None,
    taker_fee_rate: Decimal | None,
    token_base_fee_rate: Decimal | None,
    fee_schedule_json: dict[str, Any] | None,
) -> str:
    payload = {
        "condition_id": condition_id,
        "asset_id": asset_id,
        "fees_enabled": fees_enabled,
        "maker_fee_rate": _serialize_decimal(maker_fee_rate),
        "taker_fee_rate": _serialize_decimal(taker_fee_rate),
        "token_base_fee_rate": _serialize_decimal(token_base_fee_rate),
        "fee_schedule_json": fee_schedule_json,
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _build_reward_history_fingerprint(
    *,
    condition_id: str,
    reward_status: str,
    reward_program_id: str | None,
    reward_daily_rate: Decimal | None,
    min_incentive_size: Decimal | None,
    max_incentive_spread: Decimal | None,
    start_at_exchange: datetime | None,
    end_at_exchange: datetime | None,
    rewards_config_json: dict[str, Any] | list[Any] | str | None,
) -> str:
    payload = {
        "condition_id": condition_id,
        "reward_status": reward_status,
        "reward_program_id": reward_program_id,
        "reward_daily_rate": _serialize_decimal(reward_daily_rate),
        "min_incentive_size": _serialize_decimal(min_incentive_size),
        "max_incentive_spread": _serialize_decimal(max_incentive_spread),
        "start_at_exchange": _json_safe(start_at_exchange),
        "end_at_exchange": _json_safe(end_at_exchange),
        "rewards_config_json": rewards_config_json,
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


async def _latest_fee_history_row(
    session: AsyncSession,
    *,
    asset_id: str,
    condition_id: str | None = None,
    as_of: datetime | None = None,
) -> PolymarketTokenFeeRateHistory | None:
    query = select(PolymarketTokenFeeRateHistory).where(PolymarketTokenFeeRateHistory.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketTokenFeeRateHistory.condition_id == condition_id)
    if as_of is not None:
        query = query.where(
            func.coalesce(
                PolymarketTokenFeeRateHistory.effective_at_exchange,
                PolymarketTokenFeeRateHistory.observed_at_local,
            ) <= as_of
        )
    query = query.order_by(
        func.coalesce(
            PolymarketTokenFeeRateHistory.effective_at_exchange,
            PolymarketTokenFeeRateHistory.observed_at_local,
        ).desc(),
        PolymarketTokenFeeRateHistory.id.desc(),
    ).limit(1)
    return (await session.execute(query)).scalar_one_or_none()


async def _latest_reward_history_row(
    session: AsyncSession,
    *,
    condition_id: str,
    as_of: datetime | None = None,
) -> PolymarketMarketRewardConfigHistory | None:
    query = select(PolymarketMarketRewardConfigHistory).where(
        PolymarketMarketRewardConfigHistory.condition_id == condition_id
    )
    if as_of is not None:
        query = query.where(
            func.coalesce(
                PolymarketMarketRewardConfigHistory.effective_at_exchange,
                PolymarketMarketRewardConfigHistory.observed_at_local,
            ) <= as_of
        )
    query = query.order_by(
        func.coalesce(
            PolymarketMarketRewardConfigHistory.effective_at_exchange,
            PolymarketMarketRewardConfigHistory.observed_at_local,
        ).desc(),
        PolymarketMarketRewardConfigHistory.id.desc(),
    ).limit(1)
    return (await session.execute(query)).scalar_one_or_none()


async def insert_token_fee_history_if_changed(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim | None,
    asset_dim: PolymarketAssetDim | None,
    condition_id: str,
    asset_id: str,
    source_kind: str,
    effective_at_exchange: datetime | None,
    observed_at_local: datetime,
    sync_run_id: uuid.UUID | None,
    fees_enabled: bool | None,
    maker_fee_rate: Decimal | None,
    taker_fee_rate: Decimal | None,
    token_base_fee_rate: Decimal | None,
    fee_schedule_json: dict[str, Any] | None,
    details_json: dict[str, Any] | None = None,
) -> bool:
    latest = await _latest_fee_history_row(session, asset_id=asset_id, condition_id=condition_id)
    fingerprint = _build_fee_history_fingerprint(
        condition_id=condition_id,
        asset_id=asset_id,
        fees_enabled=fees_enabled,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        token_base_fee_rate=token_base_fee_rate,
        fee_schedule_json=fee_schedule_json,
    )
    if latest is not None and latest.fingerprint == fingerprint:
        return False

    row = PolymarketTokenFeeRateHistory(
        market_dim_id=market_dim.id if market_dim is not None else None,
        asset_dim_id=asset_dim.id if asset_dim is not None else None,
        condition_id=condition_id,
        asset_id=asset_id,
        source_kind=source_kind,
        effective_at_exchange=effective_at_exchange,
        observed_at_local=observed_at_local,
        sync_run_id=sync_run_id,
        fees_enabled=fees_enabled,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        token_base_fee_rate=token_base_fee_rate,
        fee_schedule_json=fee_schedule_json,
        fingerprint=fingerprint,
        details_json={
            **(details_json or {}),
            "initial_observation": latest is None,
            "previous_fingerprint": latest.fingerprint if latest is not None else None,
        },
    )
    session.add(row)
    await session.flush()
    polymarket_maker_fee_history_rows.labels(source_kind=source_kind).inc()
    polymarket_maker_last_fee_sync_timestamp.set(observed_at_local.timestamp())
    return True


async def insert_reward_history_if_changed(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim | None,
    condition_id: str,
    source_kind: str,
    effective_at_exchange: datetime | None,
    observed_at_local: datetime,
    sync_run_id: uuid.UUID | None,
    reward_status: str,
    reward_program_id: str | None,
    reward_daily_rate: Decimal | None,
    min_incentive_size: Decimal | None,
    max_incentive_spread: Decimal | None,
    start_at_exchange: datetime | None,
    end_at_exchange: datetime | None,
    rewards_config_json: dict[str, Any] | list[Any] | str | None,
    details_json: dict[str, Any] | None = None,
) -> bool:
    latest = await _latest_reward_history_row(session, condition_id=condition_id)
    fingerprint = _build_reward_history_fingerprint(
        condition_id=condition_id,
        reward_status=reward_status,
        reward_program_id=reward_program_id,
        reward_daily_rate=reward_daily_rate,
        min_incentive_size=min_incentive_size,
        max_incentive_spread=max_incentive_spread,
        start_at_exchange=start_at_exchange,
        end_at_exchange=end_at_exchange,
        rewards_config_json=rewards_config_json,
    )
    if latest is not None and latest.fingerprint == fingerprint:
        return False

    config_count = len(rewards_config_json) if isinstance(rewards_config_json, list) else 0
    row = PolymarketMarketRewardConfigHistory(
        market_dim_id=market_dim.id if market_dim is not None else None,
        condition_id=condition_id,
        source_kind=source_kind,
        effective_at_exchange=effective_at_exchange,
        observed_at_local=observed_at_local,
        sync_run_id=sync_run_id,
        reward_status=reward_status,
        reward_program_id=reward_program_id,
        reward_daily_rate=reward_daily_rate,
        min_incentive_size=min_incentive_size,
        max_incentive_spread=max_incentive_spread,
        start_at_exchange=start_at_exchange,
        end_at_exchange=end_at_exchange,
        config_count=config_count,
        rewards_config_json=rewards_config_json,
        fingerprint=fingerprint,
        details_json={
            **(details_json or {}),
            "initial_observation": latest is None,
            "previous_fingerprint": latest.fingerprint if latest is not None else None,
        },
    )
    session.add(row)
    await session.flush()
    polymarket_maker_reward_history_rows.labels(reward_status=reward_status).inc()
    polymarket_maker_last_reward_sync_timestamp.set(observed_at_local.timestamp())
    return True


def serialize_token_fee_history(row: PolymarketTokenFeeRateHistory) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "source_kind": row.source_kind,
        "effective_at_exchange": row.effective_at_exchange,
        "observed_at_local": row.observed_at_local,
        "sync_run_id": row.sync_run_id,
        "fees_enabled": row.fees_enabled,
        "maker_fee_rate": _serialize_decimal(row.maker_fee_rate),
        "taker_fee_rate": _serialize_decimal(row.taker_fee_rate),
        "token_base_fee_rate": _serialize_decimal(row.token_base_fee_rate),
        "fee_schedule_json": row.fee_schedule_json,
        "fingerprint": row.fingerprint,
        "details_json": row.details_json,
        "created_at": row.created_at,
    }


def serialize_reward_history(row: PolymarketMarketRewardConfigHistory) -> dict[str, Any]:
    return {
        "id": row.id,
        "market_dim_id": row.market_dim_id,
        "condition_id": row.condition_id,
        "source_kind": row.source_kind,
        "effective_at_exchange": row.effective_at_exchange,
        "observed_at_local": row.observed_at_local,
        "sync_run_id": row.sync_run_id,
        "reward_status": row.reward_status,
        "reward_program_id": row.reward_program_id,
        "reward_daily_rate": _serialize_decimal(row.reward_daily_rate),
        "min_incentive_size": _serialize_decimal(row.min_incentive_size),
        "max_incentive_spread": _serialize_decimal(row.max_incentive_spread),
        "start_at_exchange": row.start_at_exchange,
        "end_at_exchange": row.end_at_exchange,
        "config_count": row.config_count,
        "rewards_config_json": row.rewards_config_json,
        "fingerprint": row.fingerprint,
        "details_json": row.details_json,
        "created_at": row.created_at,
    }


def serialize_maker_economics_snapshot(row: PolymarketMakerEconomicsSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "opportunity_id": row.opportunity_id,
        "validation_id": row.validation_id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "fee_history_id": row.fee_history_id,
        "reward_history_id": row.reward_history_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "context_kind": row.context_kind,
        "estimator_version": row.estimator_version,
        "status": row.status,
        "preferred_action": row.preferred_action,
        "maker_action_type": row.maker_action_type,
        "side": row.side,
        "target_size": _serialize_decimal(row.target_size),
        "target_notional": _serialize_decimal(row.target_notional),
        "maker_fill_probability": _serialize_decimal(row.maker_fill_probability),
        "maker_gross_edge_total": _serialize_decimal(row.maker_gross_edge_total),
        "maker_fees_total": _serialize_decimal(row.maker_fees_total),
        "maker_rewards_total": _serialize_decimal(row.maker_rewards_total),
        "maker_realism_adjustment_total": _serialize_decimal(row.maker_realism_adjustment_total),
        "maker_net_total": _serialize_decimal(row.maker_net_total),
        "taker_gross_edge_total": _serialize_decimal(row.taker_gross_edge_total),
        "taker_fees_total": _serialize_decimal(row.taker_fees_total),
        "taker_rewards_total": _serialize_decimal(row.taker_rewards_total),
        "taker_realism_adjustment_total": _serialize_decimal(row.taker_realism_adjustment_total),
        "taker_net_total": _serialize_decimal(row.taker_net_total),
        "maker_advantage_total": _serialize_decimal(row.maker_advantage_total),
        "reason_codes_json": row.reason_codes_json,
        "details_json": row.details_json,
        "input_fingerprint": row.input_fingerprint,
        "evaluated_at": row.evaluated_at,
        "created_at": row.created_at,
    }


def serialize_quote_recommendation(row: PolymarketQuoteRecommendation) -> dict[str, Any]:
    return {
        "id": row.id,
        "snapshot_id": row.snapshot_id,
        "opportunity_id": row.opportunity_id,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "recommendation_kind": row.recommendation_kind,
        "status": row.status,
        "comparison_winner": row.comparison_winner,
        "recommendation_action": row.recommendation_action,
        "recommended_action_type": row.recommended_action_type,
        "recommended_side": row.recommended_side,
        "recommended_yes_price": _serialize_decimal(row.recommended_yes_price),
        "recommended_entry_price": _serialize_decimal(row.recommended_entry_price),
        "recommended_size": _serialize_decimal(row.recommended_size),
        "recommended_notional": _serialize_decimal(row.recommended_notional),
        "price_offset_ticks": row.price_offset_ticks,
        "reason_codes_json": row.reason_codes_json,
        "summary_json": row.summary_json,
        "details_json": row.details_json,
        "input_fingerprint": row.input_fingerprint,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def lookup_token_fee_history(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketTokenFeeRateHistory)
    if asset_id:
        query = query.where(PolymarketTokenFeeRateHistory.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketTokenFeeRateHistory.condition_id == condition_id)
    if start is not None:
        query = query.where(PolymarketTokenFeeRateHistory.observed_at_local >= start)
    if end is not None:
        query = query.where(PolymarketTokenFeeRateHistory.observed_at_local <= end)
    rows = (
        await session.execute(
            query.order_by(
                PolymarketTokenFeeRateHistory.observed_at_local.desc(),
                PolymarketTokenFeeRateHistory.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [serialize_token_fee_history(row) for row in rows]


async def lookup_current_token_fee_state(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    as_of: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketTokenFeeRateHistory)
    if asset_id:
        query = query.where(PolymarketTokenFeeRateHistory.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketTokenFeeRateHistory.condition_id == condition_id)
    if as_of is not None:
        query = query.where(
            func.coalesce(
                PolymarketTokenFeeRateHistory.effective_at_exchange,
                PolymarketTokenFeeRateHistory.observed_at_local,
            ) <= as_of
        )
    rows = (
        await session.execute(
            query.order_by(
                PolymarketTokenFeeRateHistory.asset_id.asc(),
                func.coalesce(
                    PolymarketTokenFeeRateHistory.effective_at_exchange,
                    PolymarketTokenFeeRateHistory.observed_at_local,
                ).desc(),
                PolymarketTokenFeeRateHistory.id.desc(),
            )
        )
    ).scalars().all()
    deduped: dict[str, PolymarketTokenFeeRateHistory] = {}
    for row in rows:
        deduped.setdefault(row.asset_id, row)
        if len(deduped) >= limit:
            break
    return [serialize_token_fee_history(row) for row in deduped.values()]


async def lookup_reward_history(
    session: AsyncSession,
    *,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketMarketRewardConfigHistory)
    if condition_id:
        query = query.where(PolymarketMarketRewardConfigHistory.condition_id == condition_id)
    if start is not None:
        query = query.where(PolymarketMarketRewardConfigHistory.observed_at_local >= start)
    if end is not None:
        query = query.where(PolymarketMarketRewardConfigHistory.observed_at_local <= end)
    rows = (
        await session.execute(
            query.order_by(
                PolymarketMarketRewardConfigHistory.observed_at_local.desc(),
                PolymarketMarketRewardConfigHistory.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [serialize_reward_history(row) for row in rows]


async def lookup_current_reward_state(
    session: AsyncSession,
    *,
    condition_id: str | None,
    as_of: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketMarketRewardConfigHistory)
    if condition_id:
        query = query.where(PolymarketMarketRewardConfigHistory.condition_id == condition_id)
    if as_of is not None:
        query = query.where(
            func.coalesce(
                PolymarketMarketRewardConfigHistory.effective_at_exchange,
                PolymarketMarketRewardConfigHistory.observed_at_local,
            ) <= as_of
        )
    rows = (
        await session.execute(
            query.order_by(
                PolymarketMarketRewardConfigHistory.condition_id.asc(),
                func.coalesce(
                    PolymarketMarketRewardConfigHistory.effective_at_exchange,
                    PolymarketMarketRewardConfigHistory.observed_at_local,
                ).desc(),
                PolymarketMarketRewardConfigHistory.id.desc(),
            )
        )
    ).scalars().all()
    deduped: dict[str, PolymarketMarketRewardConfigHistory] = {}
    for row in rows:
        deduped.setdefault(row.condition_id, row)
        if len(deduped) >= limit:
            break
    return [serialize_reward_history(row) for row in deduped.values()]


async def list_maker_economics_snapshots(
    session: AsyncSession,
    *,
    opportunity_id: int | None,
    condition_id: str | None,
    asset_id: str | None,
    status: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketMakerEconomicsSnapshot)
    if opportunity_id is not None:
        query = query.where(PolymarketMakerEconomicsSnapshot.opportunity_id == opportunity_id)
    if condition_id:
        query = query.where(PolymarketMakerEconomicsSnapshot.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketMakerEconomicsSnapshot.asset_id == asset_id)
    if status:
        query = query.where(PolymarketMakerEconomicsSnapshot.status == status)
    if start is not None:
        query = query.where(PolymarketMakerEconomicsSnapshot.evaluated_at >= start)
    if end is not None:
        query = query.where(PolymarketMakerEconomicsSnapshot.evaluated_at <= end)
    rows = (
        await session.execute(
            query.order_by(
                PolymarketMakerEconomicsSnapshot.evaluated_at.desc(),
                PolymarketMakerEconomicsSnapshot.created_at.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [serialize_maker_economics_snapshot(row) for row in rows]


async def list_quote_recommendations(
    session: AsyncSession,
    *,
    opportunity_id: int | None,
    condition_id: str | None,
    asset_id: str | None,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketQuoteRecommendation)
    if opportunity_id is not None:
        query = query.where(PolymarketQuoteRecommendation.opportunity_id == opportunity_id)
    if condition_id:
        query = query.where(PolymarketQuoteRecommendation.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketQuoteRecommendation.asset_id == asset_id)
    if status:
        query = query.where(PolymarketQuoteRecommendation.status == status)
    rows = (
        await session.execute(
            query.order_by(
                PolymarketQuoteRecommendation.created_at.desc(),
                PolymarketQuoteRecommendation.updated_at.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [serialize_quote_recommendation(row) for row in rows]


async def get_latest_maker_economics_snapshot(
    session: AsyncSession,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(PolymarketMakerEconomicsSnapshot)
            .where(PolymarketMakerEconomicsSnapshot.opportunity_id == opportunity_id)
            .order_by(
                PolymarketMakerEconomicsSnapshot.evaluated_at.desc(),
                PolymarketMakerEconomicsSnapshot.created_at.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return serialize_maker_economics_snapshot(row) if row is not None else None


async def get_latest_quote_recommendation(
    session: AsyncSession,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(PolymarketQuoteRecommendation)
            .where(PolymarketQuoteRecommendation.opportunity_id == opportunity_id)
            .order_by(
                PolymarketQuoteRecommendation.created_at.desc(),
                PolymarketQuoteRecommendation.updated_at.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return serialize_quote_recommendation(row) if row is not None else None


def _slippage_reference_total(legs: list[MarketStructureOpportunityLeg]) -> Decimal:
    total = ZERO
    for leg in legs:
        touch_entry = _to_decimal((leg.details_json or {}).get("touch_entry_price"))
        avg_entry = _to_decimal(leg.est_avg_entry_price)
        fillable_shares = _to_decimal(leg.est_fillable_size) or _to_decimal(leg.target_size) or ZERO
        if touch_entry is None or avg_entry is None or fillable_shares <= ZERO:
            continue
        diff = avg_entry - touch_entry
        if diff > ZERO:
            total += diff * fillable_shares
    return total.quantize(PRICE_Q)


async def _load_structure_context(
    session: AsyncSession,
    *,
    opportunity_id: int,
    as_of: datetime | None,
) -> StructureEconomicsContext:
    opportunity = await session.get(MarketStructureOpportunity, opportunity_id)
    if opportunity is None:
        raise LookupError("Structure opportunity not found")

    validation = (
        await session.execute(
            select(MarketStructureValidation)
            .where(MarketStructureValidation.opportunity_id == opportunity_id)
            .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    legs = (
        await session.execute(
            select(MarketStructureOpportunityLeg)
            .where(MarketStructureOpportunityLeg.opportunity_id == opportunity_id)
            .order_by(MarketStructureOpportunityLeg.leg_index.asc())
        )
    ).scalars().all()
    maker_leg = next(
        (
            leg for leg in legs
            if leg.venue == "polymarket"
            and leg.asset_id is not None
            and leg.condition_id is not None
        ),
        None,
    )
    effective_as_of = _ensure_utc(as_of) or _utcnow()
    if maker_leg is None:
        return StructureEconomicsContext(
            opportunity=opportunity,
            validation=validation,
            legs=legs,
            maker_leg=None,
            market_dim=None,
            asset_dim=None,
            fee_state=None,
            reward_state=None,
            as_of=effective_as_of,
            recon_state=None,
            best_bid=None,
            best_ask=None,
            tick_size=None,
            reliable_book=False,
            book_reason="missing_polymarket_leg",
            snapshot_age_seconds=None,
        )

    asset_dim = (
        await session.execute(
            select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == maker_leg.asset_id)
        )
    ).scalar_one_or_none()
    market_dim = (
        await session.execute(
            select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == maker_leg.condition_id)
        )
    ).scalar_one_or_none()
    if asset_dim is not None and market_dim is None and asset_dim.market_dim_id is not None:
        market_dim = await session.get(PolymarketMarketDim, asset_dim.market_dim_id)

    fee_state = await _latest_fee_history_row(
        session,
        asset_id=maker_leg.asset_id or "",
        condition_id=maker_leg.condition_id,
        as_of=effective_as_of,
    )
    reward_state = await _latest_reward_history_row(
        session,
        condition_id=maker_leg.condition_id or "",
        as_of=effective_as_of,
    )

    recon_state = (
        await session.execute(
            select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == maker_leg.asset_id)
        )
    ).scalar_one_or_none()
    best_bid = None
    best_ask = None
    tick_size = None
    reliable_book = False
    book_reason = "missing_recon_state"
    snapshot_age_seconds = None
    if recon_state is not None:
        snapshot, bids, asks, reliable_book, book_reason = await _rebuild_current_book(session, recon_state)
        best_bid = bids[0].yes_price if bids else None
        best_ask = asks[0].yes_price if asks else None
        param_history = await _latest_param_history(
            session,
            condition_id=maker_leg.condition_id or "",
            asset_id=maker_leg.asset_id or "",
        )
        tick_size = param_history.tick_size if param_history is not None else None
        if snapshot is not None and snapshot.created_at is not None:
            observed_at = _ensure_utc(snapshot.created_at)
            if observed_at is not None:
                snapshot_age_seconds = max(0, int((effective_as_of - observed_at).total_seconds()))
    return StructureEconomicsContext(
        opportunity=opportunity,
        validation=validation,
        legs=legs,
        maker_leg=maker_leg,
        market_dim=market_dim,
        asset_dim=asset_dim,
        fee_state=fee_state,
        reward_state=reward_state,
        as_of=effective_as_of,
        recon_state=recon_state,
        best_bid=best_bid,
        best_ask=best_ask,
        tick_size=tick_size,
        reliable_book=reliable_book,
        book_reason=book_reason,
        snapshot_age_seconds=snapshot_age_seconds,
    )


def _candidate_yes_price(
    *,
    side: str,
    action_type: str,
    best_bid: Decimal,
    best_ask: Decimal,
    tick_size: Decimal,
) -> tuple[Decimal | None, str | None]:
    if side == "buy_yes":
        candidate = best_bid
        if action_type == "step_ahead":
            candidate = (best_bid + tick_size).quantize(PRICE_Q)
            if candidate >= best_ask:
                return None, "unsupported_quoting_case"
        return candidate, None

    if side == "buy_no":
        candidate = best_ask
        if action_type == "step_ahead":
            candidate = (best_ask - tick_size).quantize(PRICE_Q)
            if candidate <= best_bid:
                return None, "unsupported_quoting_case"
        return candidate, None

    return None, "unsupported_quoting_case"


def _reward_spread_for_quote(*, side: str, yes_price: Decimal, best_bid: Decimal, best_ask: Decimal) -> Decimal:
    if side == "buy_yes":
        return max(best_ask - yes_price, ZERO).quantize(PRICE_Q)
    return max(yes_price - best_bid, ZERO).quantize(PRICE_Q)


def _estimate_reward_total(
    reward_state: PolymarketMarketRewardConfigHistory | None,
    *,
    target_size: Decimal,
    quote_spread: Decimal | None,
) -> tuple[Decimal, list[str], dict[str, Any]]:
    if reward_state is None:
        return ZERO, ["missing_reward_config"], {"supported": False}

    reason_codes: list[str] = []
    if reward_state.reward_status not in ACTIVE_REWARD_STATES:
        reason_codes.append("reward_not_active")

    min_incentive_size = _to_decimal(reward_state.min_incentive_size)
    if min_incentive_size is not None and target_size < min_incentive_size:
        reason_codes.append("reward_size_ineligible")

    max_incentive_spread = _to_decimal(reward_state.max_incentive_spread)
    if quote_spread is not None and max_incentive_spread is not None and quote_spread > max_incentive_spread:
        reason_codes.append("reward_spread_ineligible")

    reward_daily_rate = _to_decimal(reward_state.reward_daily_rate)
    if reward_daily_rate is None:
        reason_codes.append("unsupported_reward_structure")
        return ZERO, reason_codes, {
            "supported": False,
            "reward_status": reward_state.reward_status,
            "min_incentive_size": _serialize_decimal(min_incentive_size),
            "max_incentive_spread": _serialize_decimal(max_incentive_spread),
        }

    if any(code in reason_codes for code in ("reward_not_active", "reward_size_ineligible", "reward_spread_ineligible")):
        return ZERO, reason_codes, {
            "supported": True,
            "reward_status": reward_state.reward_status,
            "reward_daily_rate": _serialize_decimal(reward_daily_rate),
            "min_incentive_size": _serialize_decimal(min_incentive_size),
            "max_incentive_spread": _serialize_decimal(max_incentive_spread),
        }

    size_ratio = ONE
    if min_incentive_size is not None and min_incentive_size > ZERO:
        size_ratio = min(ONE, (target_size / min_incentive_size).quantize(PRICE_Q))
    reward_total = ((reward_daily_rate / Decimal("1440")) * size_ratio).quantize(PRICE_Q)
    return reward_total, reason_codes, {
        "supported": True,
        "reward_status": reward_state.reward_status,
        "reward_daily_rate": _serialize_decimal(reward_daily_rate),
        "estimated_dwell_minutes": 1,
        "size_ratio": _serialize_decimal(size_ratio),
    }


async def _build_passive_fill_summary(
    session: AsyncSession,
    *,
    context: StructureEconomicsContext,
    action_type: str,
) -> tuple[Decimal, Decimal, dict[str, Any], list[str]]:
    maker_leg = context.maker_leg
    if maker_leg is None or maker_leg.asset_id is None or maker_leg.condition_id is None:
        return ZERO, ZERO, {"supported": False}, ["missing_polymarket_leg"]
    passive_context = PolymarketExecutionContext(
        signal_id=None,
        market_id=uuid.uuid4(),
        outcome_id=maker_leg.outcome_id or uuid.uuid4(),
        direction=maker_leg.side,
        estimated_probability=ZERO,
        market_price=ZERO,
        baseline_target_size=_to_decimal(maker_leg.target_size) or ZERO,
        bankroll=ZERO,
        decision_at=context.as_of,
        asset_id=maker_leg.asset_id,
        condition_id=maker_leg.condition_id,
        market_dim_id=context.market_dim.id if context.market_dim is not None else None,
        asset_dim_id=context.asset_dim.id if context.asset_dim is not None else None,
        tick_size=context.tick_size,
        min_order_size=_to_decimal((maker_leg.details_json or {}).get("min_order_size")),
        fees_enabled=False,
        taker_fee_rate=ZERO,
        maker_fee_rate=ZERO,
        fee_schedule_json=None,
        recon_state_id=context.recon_state.id if context.recon_state is not None else None,
        recon_status=context.recon_state.status if context.recon_state is not None else None,
        reliable_book=context.reliable_book,
        book_reason=context.book_reason,
        best_bid=context.best_bid,
        best_ask=context.best_ask,
        spread=(context.best_ask - context.best_bid).quantize(PRICE_Q)
        if context.best_bid is not None and context.best_ask is not None
        else None,
        bids=[],
        asks=[],
        snapshot_id=context.recon_state.last_snapshot_id if context.recon_state is not None else None,
        snapshot_source_kind=context.recon_state.last_snapshot_source_kind if context.recon_state is not None else None,
        snapshot_observed_at=None,
        snapshot_age_seconds=context.snapshot_age_seconds,
        horizon_ms=settings.polymarket_execution_policy_default_horizon_ms,
        lookback_start=context.as_of - timedelta(hours=settings.polymarket_execution_policy_passive_lookback_hours),
    )
    passive_summary = await _passive_label_summary(session, context=passive_context, action_type=action_type)
    reasons: list[str] = []
    if passive_summary.row_count < settings.polymarket_execution_policy_passive_min_label_rows:
        reasons.append("downgraded_confidence")
    return (
        passive_summary.fill_probability or ZERO,
        passive_summary.adverse_selection_bps or ZERO,
        passive_summary.as_json(),
        reasons,
    )


def _build_snapshot_fingerprint(
    *,
    opportunity_id: int,
    as_of: datetime,
    action_type: str,
    fee_fingerprint: str | None,
    reward_fingerprint: str | None,
    target_yes_price: Decimal | None,
    target_size: Decimal | None,
) -> str:
    payload = {
        "opportunity_id": opportunity_id,
        "as_of": _json_safe(as_of),
        "action_type": action_type,
        "fee_fingerprint": fee_fingerprint,
        "reward_fingerprint": reward_fingerprint,
        "target_yes_price": _serialize_decimal(target_yes_price),
        "target_size": _serialize_decimal(target_size),
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


async def evaluate_structure_maker_economics(
    session: AsyncSession,
    *,
    opportunity_id: int,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    effective_as_of = _ensure_utc(as_of) or _utcnow()
    if not settings.polymarket_maker_economics_enabled:
        raise ValueError("Maker economics engine disabled")

    context = await _load_structure_context(session, opportunity_id=opportunity_id, as_of=effective_as_of)
    validation = context.validation
    reason_codes: list[str] = ["advisory_only_output"]
    taker_net_total = _to_decimal(validation.current_net_edge_total if validation is not None else context.opportunity.net_edge_total)
    taker_gross_edge_total = _to_decimal(
        validation.current_gross_edge_total if validation is not None else context.opportunity.gross_edge_total
    )
    taker_fees_total = sum((_to_decimal(leg.est_fee) or ZERO) for leg in context.legs).quantize(PRICE_Q)
    taker_slippage_reference_total = _slippage_reference_total(context.legs)

    if validation is None:
        reason_codes.append("no_structure_validation")
    if taker_net_total is None or taker_gross_edge_total is None:
        reason_codes.append("incomplete_economics")
    if taker_net_total is not None and taker_net_total <= ZERO:
        reason_codes.append("no_positive_taker_edge")
    if context.maker_leg is None:
        reason_codes.append("missing_polymarket_leg")
    if context.snapshot_age_seconds is not None and context.snapshot_age_seconds > settings.polymarket_quote_optimizer_max_age_seconds:
        reason_codes.append("stale_economics_inputs")
    if not context.reliable_book:
        reason_codes.append("stale_economics_inputs")
    if context.tick_size is None or context.tick_size <= ZERO:
        reason_codes.append("unsupported_quoting_case")
    if not settings.polymarket_fee_history_enabled:
        reason_codes.append("fee_history_disabled")

    if context.fee_state is None:
        param_fallback = None
        if context.maker_leg is not None and context.maker_leg.asset_id and context.maker_leg.condition_id:
            param_fallback = await _latest_param_history(
                session,
                condition_id=context.maker_leg.condition_id,
                asset_id=context.maker_leg.asset_id,
            )
        if param_fallback is not None:
            context.fee_state = PolymarketTokenFeeRateHistory(
                condition_id=param_fallback.condition_id,
                asset_id=param_fallback.asset_id or "",
                source_kind="param_fallback",
                observed_at_local=param_fallback.observed_at_local,
                fees_enabled=param_fallback.fees_enabled,
                maker_fee_rate=_resolve_maker_fee_rate(param_fallback),
                taker_fee_rate=_resolve_taker_fee_rate(param_fallback),
                token_base_fee_rate=_resolve_taker_fee_rate(param_fallback),
                fee_schedule_json=param_fallback.fee_schedule_json,
                fingerprint="param_fallback",
            )
            reason_codes.append("incomplete_economics")
        else:
            reason_codes.append("missing_fee_data")

    if context.reward_state is None:
        reason_codes.append("missing_reward_config")

    maker_action_rows: list[dict[str, Any]] = []
    preferred_action = "taker"
    selected_candidate: dict[str, Any] | None = None
    blocking = any(code in BLOCKING_REASON_CODES for code in reason_codes)
    maker_leg = context.maker_leg
    if not blocking and maker_leg is not None and context.best_bid is not None and context.best_ask is not None:
        target_shares = _to_decimal(maker_leg.target_size) or ZERO
        min_order_size = _to_decimal((maker_leg.details_json or {}).get("min_order_size"))
        max_notional = Decimal(str(settings.polymarket_quote_optimizer_max_notional))
        for action_type in ("post_best", "step_ahead"):
            candidate_reasons: list[str] = []
            yes_price, invalid_reason = _candidate_yes_price(
                side=maker_leg.side,
                action_type=action_type,
                best_bid=context.best_bid,
                best_ask=context.best_ask,
                tick_size=context.tick_size or ZERO,
            )
            if invalid_reason is not None:
                candidate_reasons.append(invalid_reason)
            if yes_price is None:
                maker_action_rows.append({"action_type": action_type, "status": "blocked", "reason_codes": candidate_reasons})
                continue

            entry_price = _entry_price_for_direction(maker_leg.side, yes_price=yes_price)
            candidate_shares = target_shares
            candidate_notional = (candidate_shares * entry_price).quantize(PRICE_Q)
            if candidate_notional > max_notional and entry_price > ZERO:
                candidate_shares = (max_notional / entry_price).quantize(Decimal("0.0001"))
                candidate_notional = (candidate_shares * entry_price).quantize(PRICE_Q)
            if min_order_size is not None and candidate_shares < min_order_size:
                candidate_reasons.append("policy_blocked_recommendation")

            fill_probability, adverse_selection_bps, passive_summary_json, passive_reasons = await _build_passive_fill_summary(
                session,
                context=context,
                action_type=action_type,
            )
            candidate_reasons.extend(passive_reasons)
            current_leg_entry_price = _to_decimal(maker_leg.est_avg_entry_price)
            if current_leg_entry_price is None:
                current_leg_entry_price = _to_decimal((maker_leg.details_json or {}).get("touch_entry_price"))
                candidate_reasons.append("incomplete_economics")
            if current_leg_entry_price is None:
                current_leg_entry_price = entry_price

            taker_leg_fee = _to_decimal(maker_leg.est_fee) or ZERO
            other_leg_fees = (taker_fees_total - taker_leg_fee).quantize(PRICE_Q)
            fees_enabled = bool(context.fee_state.fees_enabled) if context.fee_state is not None and context.fee_state.fees_enabled is not None else True
            maker_fee_rate = _to_decimal(context.fee_state.maker_fee_rate if context.fee_state is not None else None) or ZERO
            maker_fee_total = _estimate_taker_fee_total(
                fillable_size=candidate_notional,
                entry_price=entry_price,
                fee_rate=maker_fee_rate,
                fees_enabled=fees_enabled,
            )
            filled_fees_total = (other_leg_fees + maker_fee_total).quantize(PRICE_Q)
            price_improvement_total = (max(current_leg_entry_price - entry_price, ZERO) * candidate_shares).quantize(PRICE_Q)
            filled_gross_edge_total = ((taker_gross_edge_total or ZERO) + price_improvement_total).quantize(PRICE_Q)

            quote_spread = _reward_spread_for_quote(
                side=maker_leg.side,
                yes_price=yes_price,
                best_bid=context.best_bid,
                best_ask=context.best_ask,
            )
            reward_total, reward_reasons, reward_details = _estimate_reward_total(
                context.reward_state,
                target_size=candidate_shares,
                quote_spread=quote_spread,
            )
            candidate_reasons.extend(reward_reasons)

            adverse_selection_per_share = _per_share_from_bps(adverse_selection_bps, entry_price=entry_price) or ZERO
            adverse_selection_total = (adverse_selection_per_share * candidate_shares).quantize(PRICE_Q)
            filled_before_realism = (filled_gross_edge_total - filled_fees_total + reward_total).quantize(PRICE_Q)
            filled_after_adverse = (filled_before_realism - adverse_selection_total).quantize(PRICE_Q)
            missed_fill_penalty = (max(filled_after_adverse, ZERO) * (ONE - fill_probability)).quantize(PRICE_Q)
            maker_realism_adjustment_total = (adverse_selection_total + missed_fill_penalty).quantize(PRICE_Q)
            maker_net_total = (filled_before_realism - maker_realism_adjustment_total).quantize(PRICE_Q)
            maker_advantage_total = (
                (maker_net_total - (taker_net_total or ZERO)).quantize(PRICE_Q)
                if taker_net_total is not None
                else None
            )
            if maker_net_total <= ZERO:
                candidate_reasons.append("policy_blocked_recommendation")

            status = "ok"
            if any(code in BLOCKING_REASON_CODES for code in candidate_reasons):
                status = "blocked"
            elif any(code in DOWNGRADE_REASON_CODES for code in candidate_reasons):
                status = "degraded"

            maker_action_rows.append(
                _json_safe(
                    {
                        "action_type": action_type,
                        "status": status,
                        "reason_codes": sorted(set(candidate_reasons)),
                        "side": maker_leg.side,
                        "target_yes_price": yes_price,
                        "entry_price": entry_price,
                        "target_size": candidate_shares,
                        "target_notional": candidate_notional,
                        "fill_probability": fill_probability,
                        "maker_gross_edge_total": filled_gross_edge_total,
                        "maker_fees_total": filled_fees_total,
                        "maker_rewards_total": reward_total,
                        "maker_realism_adjustment_total": maker_realism_adjustment_total,
                        "maker_net_total": maker_net_total,
                        "maker_advantage_total": maker_advantage_total,
                        "reference_quote_spread": quote_spread,
                        "passive_summary": passive_summary_json,
                        "reward_details": reward_details,
                        "reference_price_improvement_total": price_improvement_total,
                        "reference_other_leg_fees_total": other_leg_fees,
                        "reference_taker_slippage_total": taker_slippage_reference_total,
                    }
                )
            )

        valid_candidates = [
            row for row in maker_action_rows
            if row["status"] != "blocked" and _to_decimal(row.get("maker_advantage_total")) is not None
        ]
        if valid_candidates:
            selected_candidate = max(
                valid_candidates,
                key=lambda row: _to_decimal(row.get("maker_advantage_total")) or Decimal("-999999999"),
            )
            if (_to_decimal(selected_candidate.get("maker_advantage_total")) or ZERO) > ZERO:
                preferred_action = "maker"
            reason_codes.extend(selected_candidate.get("reason_codes", []))
        elif maker_action_rows:
            selected_candidate = maker_action_rows[0]
            reason_codes.append("policy_blocked_recommendation")

    reason_codes = sorted(set(reason_codes))
    status = "blocked" if any(code in BLOCKING_REASON_CODES for code in reason_codes) else "degraded" if any(
        code in DOWNGRADE_REASON_CODES for code in reason_codes
    ) else "ok"

    maker_action_type = selected_candidate.get("action_type") if selected_candidate is not None else None
    side = selected_candidate.get("side") if selected_candidate is not None else (maker_leg.side if maker_leg is not None else None)
    target_size = _to_decimal(selected_candidate.get("target_size")) if selected_candidate is not None else (_to_decimal(maker_leg.target_size) if maker_leg is not None else None)
    target_notional = _to_decimal(selected_candidate.get("target_notional")) if selected_candidate is not None else None
    input_fingerprint = _build_snapshot_fingerprint(
        opportunity_id=opportunity_id,
        as_of=effective_as_of,
        action_type=maker_action_type or "none",
        fee_fingerprint=context.fee_state.fingerprint if context.fee_state is not None else None,
        reward_fingerprint=context.reward_state.fingerprint if context.reward_state is not None else None,
        target_yes_price=_to_decimal(selected_candidate.get("target_yes_price")) if selected_candidate is not None else None,
        target_size=target_size,
    )
    existing = (
        await session.execute(
            select(PolymarketMakerEconomicsSnapshot).where(
                PolymarketMakerEconomicsSnapshot.input_fingerprint == input_fingerprint
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return serialize_maker_economics_snapshot(existing)

    snapshot = PolymarketMakerEconomicsSnapshot(
        opportunity_id=context.opportunity.id,
        validation_id=validation.id if validation is not None else None,
        market_dim_id=context.market_dim.id if context.market_dim is not None else None,
        asset_dim_id=context.asset_dim.id if context.asset_dim is not None else None,
        fee_history_id=context.fee_state.id if context.fee_state is not None and context.fee_state.id is not None else None,
        reward_history_id=context.reward_state.id if context.reward_state is not None and context.reward_state.id is not None else None,
        condition_id=maker_leg.condition_id if maker_leg is not None and maker_leg.condition_id is not None else context.opportunity.anchor_condition_id or "",
        asset_id=maker_leg.asset_id if maker_leg is not None and maker_leg.asset_id is not None else context.opportunity.anchor_asset_id or "",
        context_kind="structure_opportunity",
        estimator_version=ESTIMATOR_VERSION,
        status=status,
        preferred_action=preferred_action,
        maker_action_type=maker_action_type,
        side=side,
        target_size=target_size,
        target_notional=target_notional,
        maker_fill_probability=_to_decimal(selected_candidate.get("fill_probability")) if selected_candidate is not None else None,
        maker_gross_edge_total=_to_decimal(selected_candidate.get("maker_gross_edge_total")) if selected_candidate is not None else None,
        maker_fees_total=_to_decimal(selected_candidate.get("maker_fees_total")) if selected_candidate is not None else None,
        maker_rewards_total=_to_decimal(selected_candidate.get("maker_rewards_total")) if selected_candidate is not None else None,
        maker_realism_adjustment_total=_to_decimal(selected_candidate.get("maker_realism_adjustment_total")) if selected_candidate is not None else None,
        maker_net_total=_to_decimal(selected_candidate.get("maker_net_total")) if selected_candidate is not None else None,
        taker_gross_edge_total=taker_gross_edge_total,
        taker_fees_total=taker_fees_total,
        taker_rewards_total=ZERO,
        taker_realism_adjustment_total=ZERO,
        taker_net_total=taker_net_total,
        maker_advantage_total=_to_decimal(selected_candidate.get("maker_advantage_total")) if selected_candidate is not None else None,
        reason_codes_json=reason_codes,
        details_json=_json_safe(
            {
                "as_of": effective_as_of,
                "maker_candidates": maker_action_rows,
                "selected_candidate": selected_candidate,
                "reference_taker_slippage_total": taker_slippage_reference_total,
                "validation_classification": validation.classification if validation is not None else None,
                "book": {
                    "reliable": context.reliable_book,
                    "reason": context.book_reason,
                    "best_bid": context.best_bid,
                    "best_ask": context.best_ask,
                    "tick_size": context.tick_size,
                    "snapshot_age_seconds": context.snapshot_age_seconds,
                },
                "fee_state": None if context.fee_state is None else serialize_token_fee_history(context.fee_state),
                "reward_state": None if context.reward_state is None else serialize_reward_history(context.reward_state),
            }
        ),
        input_fingerprint=input_fingerprint,
        evaluated_at=effective_as_of,
    )
    session.add(snapshot)
    await session.commit()
    polymarket_maker_economics_snapshots.labels(
        status=snapshot.status,
        preferred_action=snapshot.preferred_action or "none",
    ).inc()
    polymarket_maker_last_snapshot_timestamp.set(snapshot.evaluated_at.timestamp())
    for code in reason_codes:
        polymarket_maker_economics_reason_codes.labels(reason_code=code).inc()
    return serialize_maker_economics_snapshot(snapshot)


async def generate_quote_recommendation(
    session: AsyncSession,
    *,
    opportunity_id: int,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if not settings.polymarket_quote_optimizer_enabled:
        raise ValueError("Quote optimizer disabled")

    snapshot_payload = await evaluate_structure_maker_economics(session, opportunity_id=opportunity_id, as_of=as_of)
    snapshot = (
        await session.execute(
            select(PolymarketMakerEconomicsSnapshot).where(PolymarketMakerEconomicsSnapshot.id == snapshot_payload["id"])
        )
    ).scalar_one()
    selected_candidate = snapshot.details_json.get("selected_candidate") if isinstance(snapshot.details_json, dict) else None
    summary = {
        "maker_beats_taker": snapshot.preferred_action == "maker" and snapshot.status != "blocked",
        "maker_advantage_total": snapshot_payload["maker_advantage_total"],
        "maker_net_total": snapshot_payload["maker_net_total"],
        "taker_net_total": snapshot_payload["taker_net_total"],
        "advisory_only": True,
    }
    reason_codes = sorted(set(list(snapshot.reason_codes_json or [])))
    comparison_winner = "maker" if summary["maker_beats_taker"] else "taker"
    recommendation_action = "do_not_quote"
    recommended_action_type = None
    recommended_yes_price = None
    recommended_entry_price = None
    recommended_size = snapshot.target_size
    recommended_notional = snapshot.target_notional
    price_offset_ticks = None
    if summary["maker_beats_taker"] and isinstance(selected_candidate, dict):
        recommendation_action = "recommend_quote"
        recommended_action_type = selected_candidate.get("action_type")
        recommended_yes_price = _to_decimal(selected_candidate.get("target_yes_price"))
        recommended_entry_price = _to_decimal(selected_candidate.get("entry_price"))
        recommended_size = _to_decimal(selected_candidate.get("target_size"))
        recommended_notional = _to_decimal(selected_candidate.get("target_notional"))
        price_offset_ticks = 1 if recommended_action_type == "step_ahead" else 0
    if settings.polymarket_quote_optimizer_require_fee_data and snapshot.fee_history_id is None:
        reason_codes.append("missing_fee_data")
    if settings.polymarket_quote_optimizer_require_rewards_data and snapshot.reward_history_id is None:
        reason_codes.append("missing_reward_config")
    risk_controls = None
    if recommendation_action == "recommend_quote":
        risk_controls = await compute_quote_inventory_controls(
            session,
            condition_id=snapshot.condition_id,
            asset_id=snapshot.asset_id,
            recommended_side=snapshot.side,
            recommended_notional=recommended_notional,
            snapshot_at=as_of,
        )
        if risk_controls.get("applied"):
            for reason_code in risk_controls.get("reason_codes", []):
                reason_codes.append(reason_code)
            if risk_controls["recommendation_type"] in {"no_quote", "block"}:
                recommendation_action = "do_not_quote"
                recommended_action_type = None
                recommended_yes_price = None
                recommended_entry_price = None
                recommended_size = None
                recommended_notional = None
                price_offset_ticks = None
                reason_codes.append("phase10_no_quote")
            else:
                target_cap = _to_decimal(risk_controls.get("target_size_cap_usd")) or recommended_notional or ZERO
                if recommended_notional is not None and recommended_notional > ZERO and target_cap < recommended_notional:
                    scale = (target_cap / recommended_notional).quantize(Decimal("0.000001"))
                    recommended_notional = target_cap
                    if recommended_size is not None:
                        recommended_size = (recommended_size * scale).quantize(Decimal("0.0001"))
                    reason_codes.append("phase10_reduce_size")
                shift_bps = _to_decimal(risk_controls.get("reservation_price_adjustment_bps")) or ZERO
                if recommended_entry_price is not None and shift_bps != ZERO:
                    adjusted_entry = (recommended_entry_price * (ONE + (shift_bps / TEN_THOUSAND))).quantize(PRICE_Q)
                    if adjusted_entry < ZERO:
                        adjusted_entry = ZERO
                    if adjusted_entry > ONE:
                        adjusted_entry = ONE
                    recommended_entry_price = adjusted_entry
                    if recommended_yes_price is not None:
                        recommended_yes_price = adjusted_entry
                    reason_codes.append("phase10_quote_skew")
    if (
        recommendation_action == "recommend_quote"
        and any(code in BLOCKING_REASON_CODES for code in reason_codes)
    ):
        recommendation_action = "do_not_quote"
        recommended_action_type = None
        recommended_yes_price = None
        recommended_entry_price = None
        price_offset_ticks = None
    if recommendation_action != "recommend_quote":
        reason_codes.append("policy_blocked_recommendation")
    reason_codes = sorted(set(reason_codes))
    recommendation_status = (
        "blocked"
        if any(code in BLOCKING_REASON_CODES for code in reason_codes)
        else "degraded"
        if any(code in DOWNGRADE_REASON_CODES for code in reason_codes)
        else "ok"
    )

    input_fingerprint = hashlib.sha256(
        _stable_json(
            {
                "snapshot_id": str(snapshot.id),
                "comparison_winner": comparison_winner,
                "recommendation_action": recommendation_action,
                "recommended_action_type": recommended_action_type,
                "recommended_yes_price": _serialize_decimal(recommended_yes_price),
                "recommended_size": _serialize_decimal(recommended_size),
                "risk_controls": _json_safe(risk_controls),
            }
        ).encode("utf-8")
    ).hexdigest()
    existing = (
        await session.execute(
            select(PolymarketQuoteRecommendation).where(
                PolymarketQuoteRecommendation.input_fingerprint == input_fingerprint
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return serialize_quote_recommendation(existing)

    recommendation = PolymarketQuoteRecommendation(
        snapshot_id=snapshot.id,
        opportunity_id=snapshot.opportunity_id,
        market_dim_id=snapshot.market_dim_id,
        asset_dim_id=snapshot.asset_dim_id,
        condition_id=snapshot.condition_id,
        asset_id=snapshot.asset_id,
        recommendation_kind="advisory_quote",
        status=recommendation_status,
        comparison_winner=comparison_winner,
        recommendation_action=recommendation_action,
        recommended_action_type=recommended_action_type,
        recommended_side=snapshot.side,
        recommended_yes_price=recommended_yes_price,
        recommended_entry_price=recommended_entry_price,
        recommended_size=recommended_size,
        recommended_notional=recommended_notional,
        price_offset_ticks=price_offset_ticks,
        reason_codes_json=reason_codes,
        summary_json=_json_safe({**summary, "risk_controls": risk_controls}),
        details_json=_json_safe(
            {
                "optimizer_version": QUOTE_OPTIMIZER_VERSION,
                "selected_candidate": selected_candidate,
                "snapshot": snapshot_payload,
                "risk_controls": risk_controls,
                "max_notional": settings.polymarket_quote_optimizer_max_notional,
                "max_age_seconds": settings.polymarket_quote_optimizer_max_age_seconds,
            }
        ),
        input_fingerprint=input_fingerprint,
    )
    session.add(recommendation)
    await session.commit()
    polymarket_quote_recommendations.labels(
        status=recommendation.status,
        comparison_winner=recommendation.comparison_winner or "none",
    ).inc()
    polymarket_quote_optimizer_last_recommendation_timestamp.set(recommendation.created_at.timestamp())
    for code in reason_codes:
        polymarket_quote_recommendation_reason_codes.labels(reason_code=code).inc()
    return serialize_quote_recommendation(recommendation)


async def fetch_polymarket_maker_status(session: AsyncSession) -> dict[str, Any]:
    now = _utcnow()
    last_fee_sync_at = (await session.execute(select(func.max(PolymarketTokenFeeRateHistory.observed_at_local)))).scalar_one_or_none()
    last_reward_sync_at = (
        await session.execute(select(func.max(PolymarketMarketRewardConfigHistory.observed_at_local)))
    ).scalar_one_or_none()
    last_snapshot_at = (
        await session.execute(select(func.max(PolymarketMakerEconomicsSnapshot.evaluated_at)))
    ).scalar_one_or_none()
    last_recommendation_at = (
        await session.execute(select(func.max(PolymarketQuoteRecommendation.created_at)))
    ).scalar_one_or_none()

    reward_rows = (
        await session.execute(
            select(
                PolymarketMarketRewardConfigHistory.reward_status,
                func.count(PolymarketMarketRewardConfigHistory.id),
            ).group_by(PolymarketMarketRewardConfigHistory.reward_status)
        )
    ).all()
    reward_state_counts = {status: int(count) for status, count in reward_rows}
    for reward_status in ("active", "expired", "missing", "unknown"):
        polymarket_maker_reward_states.labels(reward_status=reward_status).set(reward_state_counts.get(reward_status, 0))

    recent_reason_rows = (
        await session.execute(
            select(PolymarketMakerEconomicsSnapshot)
            .where(PolymarketMakerEconomicsSnapshot.evaluated_at >= now - timedelta(hours=24))
            .order_by(PolymarketMakerEconomicsSnapshot.evaluated_at.desc())
        )
    ).scalars().all()
    reason_counts: dict[str, int] = {}
    for row in recent_reason_rows:
        for code in row.reason_codes_json or []:
            if not isinstance(code, str):
                continue
            reason_counts[code] = reason_counts.get(code, 0) + 1

    return {
        "enabled": settings.polymarket_maker_economics_enabled,
        "fee_history_enabled": settings.polymarket_fee_history_enabled,
        "reward_history_enabled": settings.polymarket_reward_history_enabled,
        "quote_optimizer_enabled": settings.polymarket_quote_optimizer_enabled,
        "quote_optimizer_max_notional": settings.polymarket_quote_optimizer_max_notional,
        "quote_optimizer_max_age_seconds": settings.polymarket_quote_optimizer_max_age_seconds,
        "quote_optimizer_require_rewards_data": settings.polymarket_quote_optimizer_require_rewards_data,
        "quote_optimizer_require_fee_data": settings.polymarket_quote_optimizer_require_fee_data,
        "last_fee_sync_at": last_fee_sync_at,
        "last_reward_sync_at": last_reward_sync_at,
        "last_snapshot_at": last_snapshot_at,
        "last_recommendation_at": last_recommendation_at,
        "fee_history_rows": int((await session.execute(select(func.count(PolymarketTokenFeeRateHistory.id)))).scalar_one() or 0),
        "reward_history_rows": int((await session.execute(select(func.count(PolymarketMarketRewardConfigHistory.id)))).scalar_one() or 0),
        "economics_snapshot_rows": int((await session.execute(select(func.count(PolymarketMakerEconomicsSnapshot.id)))).scalar_one() or 0),
        "quote_recommendation_rows": int((await session.execute(select(func.count(PolymarketQuoteRecommendation.id)))).scalar_one() or 0),
        "reward_state_counts": reward_state_counts,
        "recent_reason_counts_24h": reason_counts,
        "fee_freshness_seconds": max(0, int((now - _ensure_utc(last_fee_sync_at)).total_seconds())) if last_fee_sync_at is not None else None,
        "reward_freshness_seconds": max(0, int((now - _ensure_utc(last_reward_sync_at)).total_seconds())) if last_reward_sync_at is not None else None,
    }
