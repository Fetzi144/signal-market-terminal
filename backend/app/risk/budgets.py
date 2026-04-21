from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.polymarket_live_execution import CapitalReservation, LiveOrder
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotGuardrailEvent,
)
from app.models.strategy_registry import DemotionEvent, StrategyFamilyRegistry, StrategyVersion
from app.risk.regime import REGIME_HALTED, classify_regime
from app.risk.risk_of_ruin import calculate_risk_of_ruin

ZERO = Decimal("0")
ONE = Decimal("1")
MONEY_Q = Decimal("0.00000001")

PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET = "capital_budget_gate"
SERIOUS_GUARDRAIL_ACTIONS = {"pause_pilot", "disarm_pilot", "kill_switch"}
TERMINAL_ORDER_STATUSES = {
    "matched",
    "mined",
    "confirmed",
    "canceled",
    "expired",
    "rejected",
    "failed",
    "validation_failed",
    "submit_blocked",
}


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_string(value: Any) -> str | None:
    decimal_value = _to_decimal(value)
    return format(decimal_value, "f") if decimal_value is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _scaled_int_limit(value: int | None, multiplier: Decimal) -> int | None:
    if value is None:
        return None
    if multiplier <= ZERO:
        return 0
    scaled = (Decimal(value) * multiplier).quantize(Decimal("1"), rounding=ROUND_DOWN)
    return max(int(scaled), 0)


def normalize_risk_budget_policy(policy: dict[str, Any] | None) -> dict[str, Any]:
    source = policy or {}
    capital = source.get("capital") if isinstance(source.get("capital"), dict) else {}
    concentration = source.get("concentration") if isinstance(source.get("concentration"), dict) else {}
    capacity = source.get("capacity") if isinstance(source.get("capacity"), dict) else {}
    regime_multipliers = source.get("regime_multipliers") if isinstance(source.get("regime_multipliers"), dict) else {}
    ruin = source.get("risk_of_ruin") if isinstance(source.get("risk_of_ruin"), dict) else {}
    breach_actions = source.get("breach_actions") if isinstance(source.get("breach_actions"), dict) else {}

    return {
        "capital": {
            "outstanding_notional_usd": _to_decimal(capital.get("outstanding_notional_usd")) or ZERO,
            "daily_notional_usd": _to_decimal(capital.get("daily_notional_usd")) or ZERO,
            "daily_loss_usd": _to_decimal(capital.get("daily_loss_usd")) or ZERO,
        },
        "concentration": {
            "event_cap_usd": _to_decimal(concentration.get("event_cap_usd")) or ZERO,
            "entity_cap_usd": _to_decimal(concentration.get("entity_cap_usd")) or ZERO,
            "cluster_cap_usd": _to_decimal(concentration.get("cluster_cap_usd")) or ZERO,
            "conversion_group_cap_usd": _to_decimal(concentration.get("conversion_group_cap_usd")) or ZERO,
        },
        "capacity": {
            "max_order_notional_usd": _to_decimal(capacity.get("max_order_notional_usd")) or ZERO,
            "capacity_ceiling_usd": _to_decimal(capacity.get("capacity_ceiling_usd")) or ZERO,
            "max_open_orders": _int_value(capacity.get("max_open_orders"), 0),
        },
        "regime_multipliers": {
            "normal": _to_decimal(regime_multipliers.get("normal")) or ONE,
            "thin_liquidity": _to_decimal(regime_multipliers.get("thin_liquidity")) or Decimal("0.75"),
            "stressed": _to_decimal(regime_multipliers.get("stressed")) or Decimal("0.50"),
            "halted": _to_decimal(regime_multipliers.get("halted")) or ZERO,
        },
        "risk_of_ruin": {
            "warning_threshold": _to_decimal(ruin.get("warning_threshold")) or Decimal("0.50"),
            "critical_threshold": _to_decimal(ruin.get("critical_threshold")) or Decimal("0.90"),
            "warning_multiplier": _to_decimal(ruin.get("warning_multiplier")) or Decimal("0.75"),
            "critical_multiplier": _to_decimal(ruin.get("critical_multiplier")) or ZERO,
        },
        "breach_actions": {
            "live": str(breach_actions.get("live") or "pause_pilot"),
            "paper": str(breach_actions.get("paper") or "reduce_size"),
            "record_demotion": bool(breach_actions.get("record_demotion", True)),
        },
    }


def serialize_risk_budget_policy(policy: dict[str, Any] | None) -> dict[str, Any] | None:
    if policy is None:
        return None
    normalized = normalize_risk_budget_policy(policy)
    return _json_safe(normalized)


def serialize_risk_budget_status(status: dict[str, Any] | None) -> dict[str, Any] | None:
    if status is None:
        return None
    return _json_safe(status)


def seed_builtin_risk_budget_policy(family: str) -> dict[str, Any]:
    normalized = str(family or "").strip().lower()
    if normalized == "default_strategy":
        bankroll = Decimal(str(settings.default_bankroll))
        return {
            "capital": {
                "outstanding_notional_usd": bankroll * Decimal(str(settings.max_total_exposure_pct)),
                "daily_notional_usd": bankroll * Decimal("0.50"),
                "daily_loss_usd": bankroll * Decimal(str(settings.drawdown_circuit_breaker_pct)),
            },
            "concentration": {
                "event_cap_usd": Decimal(str(settings.polymarket_max_event_exposure_usd)),
                "entity_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
                "cluster_cap_usd": bankroll * Decimal(str(settings.max_cluster_exposure_pct)),
                "conversion_group_cap_usd": Decimal(str(settings.polymarket_max_conversion_group_exposure_usd)),
            },
            "capacity": {
                "max_order_notional_usd": bankroll * Decimal(str(settings.max_single_position_pct)),
                "capacity_ceiling_usd": bankroll * Decimal(str(settings.max_total_exposure_pct)),
                "max_open_orders": 12,
            },
            "regime_multipliers": {},
            "risk_of_ruin": {},
            "breach_actions": {"live": "block", "paper": "reduce_size", "record_demotion": False},
        }
    if normalized == "structure":
        return {
            "capital": {
                "outstanding_notional_usd": Decimal(str(settings.polymarket_structure_max_notional_per_plan)) * Decimal("4"),
                "daily_notional_usd": Decimal(str(settings.polymarket_structure_max_notional_per_plan)) * Decimal("8"),
                "daily_loss_usd": Decimal(str(settings.polymarket_structure_max_notional_per_plan)),
            },
            "concentration": {
                "event_cap_usd": Decimal(str(settings.polymarket_max_event_exposure_usd)),
                "entity_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
                "cluster_cap_usd": Decimal(str(settings.polymarket_max_event_exposure_usd)),
                "conversion_group_cap_usd": Decimal(str(settings.polymarket_max_conversion_group_exposure_usd)),
            },
            "capacity": {
                "max_order_notional_usd": Decimal(str(settings.polymarket_structure_max_notional_per_plan)),
                "capacity_ceiling_usd": Decimal(str(settings.polymarket_structure_max_notional_per_plan)) * Decimal("4"),
                "max_open_orders": 4,
            },
            "regime_multipliers": {},
            "risk_of_ruin": {},
            "breach_actions": {"live": "block", "paper": "reduce_size", "record_demotion": False},
        }
    if normalized == "maker":
        maker_budget = Decimal(str(settings.polymarket_maker_inventory_budget_usd))
        taker_budget = Decimal(str(settings.polymarket_taker_inventory_budget_usd))
        return {
            "capital": {
                "outstanding_notional_usd": maker_budget,
                "daily_notional_usd": maker_budget * Decimal("4"),
                "daily_loss_usd": taker_budget * Decimal("0.25"),
            },
            "concentration": {
                "event_cap_usd": Decimal(str(settings.polymarket_max_event_exposure_usd)),
                "entity_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
                "cluster_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
                "conversion_group_cap_usd": Decimal(str(settings.polymarket_max_conversion_group_exposure_usd)),
            },
            "capacity": {
                "max_order_notional_usd": Decimal(str(settings.polymarket_quote_optimizer_max_notional)),
                "capacity_ceiling_usd": maker_budget,
                "max_open_orders": 8,
            },
            "regime_multipliers": {},
            "risk_of_ruin": {},
            "breach_actions": {"live": "block", "paper": "reduce_size", "record_demotion": False},
        }
    if normalized == "cross_venue_basis":
        return {
            "capital": {
                "outstanding_notional_usd": ZERO,
                "daily_notional_usd": ZERO,
                "daily_loss_usd": ZERO,
            },
            "concentration": {
                "event_cap_usd": ZERO,
                "entity_cap_usd": ZERO,
                "cluster_cap_usd": ZERO,
                "conversion_group_cap_usd": ZERO,
            },
            "capacity": {
                "max_order_notional_usd": ZERO,
                "capacity_ceiling_usd": ZERO,
                "max_open_orders": 0,
            },
            "regime_multipliers": {"halted": ZERO},
            "risk_of_ruin": {"critical_multiplier": ZERO},
            "breach_actions": {"live": "block", "paper": "block", "record_demotion": False},
        }
    return {
        "capital": {
            "outstanding_notional_usd": ZERO,
            "daily_notional_usd": Decimal(str(settings.polymarket_pilot_max_daily_notional_usd)),
            "daily_loss_usd": Decimal(str(settings.polymarket_pilot_max_daily_loss_usd)),
        },
        "concentration": {
            "event_cap_usd": Decimal(str(settings.polymarket_max_event_exposure_usd)),
            "entity_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
            "cluster_cap_usd": Decimal(str(settings.polymarket_max_entity_exposure_usd)),
            "conversion_group_cap_usd": Decimal(str(settings.polymarket_max_conversion_group_exposure_usd)),
        },
        "capacity": {
            "max_order_notional_usd": ZERO,
            "capacity_ceiling_usd": ZERO,
            "max_open_orders": 0,
        },
        "regime_multipliers": {},
        "risk_of_ruin": {},
        "breach_actions": {"live": "pause_pilot", "paper": "reduce_size", "record_demotion": True},
    }


async def _resolve_version(
    session: AsyncSession,
    *,
    strategy_family: str | None,
    strategy_version_id: int | None,
) -> StrategyVersion | None:
    if strategy_version_id is not None:
        return await session.get(StrategyVersion, int(strategy_version_id))
    if not strategy_family:
        return None
    from app.strategies.registry import get_current_strategy_version

    return await get_current_strategy_version(session, strategy_family)


async def _family_row(session: AsyncSession, *, strategy_family: str) -> StrategyFamilyRegistry | None:
    return (
        await session.execute(
            select(StrategyFamilyRegistry).where(StrategyFamilyRegistry.family == strategy_family).limit(1)
        )
    ).scalar_one_or_none()


async def _latest_reservation_rows(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    strategy_version_id: int | None = None,
) -> list[CapitalReservation]:
    query = select(CapitalReservation).order_by(
        CapitalReservation.observed_at_local.desc(),
        CapitalReservation.id.desc(),
    )
    if strategy_family:
        query = query.where(CapitalReservation.strategy_family == strategy_family)
    if strategy_version_id is not None:
        query = query.where(CapitalReservation.strategy_version_id == strategy_version_id)
    rows = (await session.execute(query)).scalars().all()
    latest_by_key: dict[str, CapitalReservation] = {}
    for row in rows:
        key = str(row.live_order_id) if row.live_order_id is not None else f"orphan:{row.id}"
        if key not in latest_by_key:
            latest_by_key[key] = row
    return list(latest_by_key.values())


async def _count_open_orders(
    session: AsyncSession,
    *,
    strategy_family: str | None,
    strategy_version_id: int | None,
) -> int:
    query = select(func.count(LiveOrder.id)).where(LiveOrder.status.not_in(tuple(TERMINAL_ORDER_STATUSES)))
    if strategy_family:
        query = query.where(LiveOrder.strategy_family == strategy_family)
    if strategy_version_id is not None:
        query = query.where(LiveOrder.strategy_version_id == strategy_version_id)
    return int((await session.execute(query)).scalar_one() or 0)


async def build_strategy_budget_status(
    session: AsyncSession,
    *,
    strategy_family: str,
    strategy_version_id: int | None = None,
    now: datetime | None = None,
    book_fresh: bool | None = None,
    fillable_fraction: Decimal | float | int | None = None,
    concentration_utilization: Decimal | float | int | None = None,
) -> dict[str, Any]:
    normalized_family = str(strategy_family or "").strip().lower()
    version = await _resolve_version(
        session,
        strategy_family=normalized_family,
        strategy_version_id=strategy_version_id,
    )
    version_id = int(version.id) if version is not None and version.id is not None else strategy_version_id
    version_key = version.version_key if version is not None else None
    raw_policy = None
    if version is not None and isinstance(version.config_json, dict):
        raw_policy = version.config_json.get("risk_budget_policy")
    if raw_policy is None:
        raw_policy = seed_builtin_risk_budget_policy(normalized_family)
    policy = normalize_risk_budget_policy(raw_policy)

    effective_now = _ensure_utc(now) or datetime.now(timezone.utc)
    since = effective_now - timedelta(hours=24)
    latest_reservations = await _latest_reservation_rows(
        session,
        strategy_family=normalized_family,
        strategy_version_id=version_id,
    )
    current_outstanding = sum(
        (
            (_to_decimal(row.open_amount) or ZERO)
            for row in latest_reservations
            if row.status not in {"released", "failed"} and (_to_decimal(row.open_amount) or ZERO) > ZERO
        ),
        ZERO,
    )
    open_order_count = await _count_open_orders(
        session,
        strategy_family=normalized_family,
        strategy_version_id=version_id,
    )

    incident_query = select(func.count(PolymarketControlPlaneIncident.id)).where(
        PolymarketControlPlaneIncident.observed_at_local >= since
    )
    if version_id is not None:
        incident_query = incident_query.where(PolymarketControlPlaneIncident.strategy_version_id == version_id)
    incident_count = int((await session.execute(incident_query)).scalar_one() or 0)

    guardrail_query = select(PolymarketPilotGuardrailEvent).where(
        PolymarketPilotGuardrailEvent.observed_at_local >= since,
        PolymarketPilotGuardrailEvent.strategy_family == normalized_family,
    )
    if version_id is not None:
        guardrail_query = guardrail_query.where(PolymarketPilotGuardrailEvent.strategy_version_id == version_id)
    guardrails = (await session.execute(guardrail_query)).scalars().all()
    serious_guardrail_count = sum(1 for row in guardrails if row.action_taken in SERIOUS_GUARDRAIL_ACTIONS)
    shadow_gap_breach_count = sum(1 for row in guardrails if row.guardrail_type == "shadow_gap_breach")

    live_shadow_query = (
        select(PolymarketLiveShadowEvaluation, LiveOrder)
        .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id, isouter=True)
        .where(PolymarketLiveShadowEvaluation.updated_at >= since)
    )
    if normalized_family:
        live_shadow_query = live_shadow_query.where(LiveOrder.strategy_family == normalized_family)
    if version_id is not None:
        live_shadow_query = live_shadow_query.where(LiveOrder.strategy_version_id == version_id)
    live_shadow_rows = (await session.execute(live_shadow_query)).all()
    coverage_limited_count = sum(1 for row, _order in live_shadow_rows if row.coverage_limited)
    gap_values = [_to_decimal(row.gap_bps) or ZERO for row, _order in live_shadow_rows]
    avg_gap = (sum(gap_values) / Decimal(len(gap_values))).quantize(MONEY_Q) if gap_values else ZERO

    base_outstanding_cap = policy["capital"]["outstanding_notional_usd"]
    base_capacity_ceiling = policy["capacity"]["capacity_ceiling_usd"] or base_outstanding_cap
    max_open_orders = int(policy["capacity"]["max_open_orders"]) if int(policy["capacity"]["max_open_orders"]) > 0 else None
    regime = classify_regime(
        book_fresh=book_fresh,
        fillable_fraction=fillable_fraction,
        incident_count_24h=incident_count,
        serious_guardrail_count_24h=serious_guardrail_count,
        coverage_limited_count_24h=coverage_limited_count,
        shadow_gap_breach_count_24h=shadow_gap_breach_count,
        custom_multipliers=policy["regime_multipliers"],
    )

    open_util = (
        (current_outstanding / base_outstanding_cap).quantize(Decimal("0.0001"))
        if base_outstanding_cap > ZERO
        else ZERO
    )
    loss_util = (
        (avg_gap / Decimal("100")).quantize(Decimal("0.0001"))
        if avg_gap > ZERO
        else ZERO
    )
    ruin = calculate_risk_of_ruin(
        loss_utilization=loss_util,
        open_exposure_utilization=open_util,
        concentration_utilization=concentration_utilization,
        warning_threshold=policy["risk_of_ruin"]["warning_threshold"],
        critical_threshold=policy["risk_of_ruin"]["critical_threshold"],
        warning_multiplier=policy["risk_of_ruin"]["warning_multiplier"],
        critical_multiplier=policy["risk_of_ruin"]["critical_multiplier"],
    )
    effective_multiplier = min(regime["multiplier"], ruin["multiplier"])

    effective_outstanding_cap = (
        (base_outstanding_cap * effective_multiplier).quantize(MONEY_Q)
        if base_outstanding_cap > ZERO
        else ZERO
    )
    effective_max_order_notional = (
        (policy["capacity"]["max_order_notional_usd"] * effective_multiplier).quantize(MONEY_Q)
        if policy["capacity"]["max_order_notional_usd"] > ZERO
        else ZERO
    )
    effective_capacity_ceiling = (
        (base_capacity_ceiling * effective_multiplier).quantize(MONEY_Q)
        if base_capacity_ceiling > ZERO
        else ZERO
    )
    effective_max_open_orders = _scaled_int_limit(max_open_orders, effective_multiplier)

    reason_codes: list[str] = []
    breach = False
    if ruin["label"] == "critical":
        breach = True
        reason_codes.append("risk_of_ruin_exceeded")
    if regime["label"] == REGIME_HALTED:
        breach = True
        reason_codes.append("capacity_ceiling_exceeded")
    if effective_outstanding_cap > ZERO and current_outstanding >= effective_outstanding_cap:
        breach = True
        reason_codes.append("family_cap_exceeded")
    if effective_capacity_ceiling > ZERO and current_outstanding >= effective_capacity_ceiling:
        breach = True
        reason_codes.append("capacity_ceiling_exceeded")
    if effective_max_open_orders is not None and open_order_count >= effective_max_open_orders:
        breach = True
        reason_codes.append("capacity_ceiling_exceeded")

    capacity_status = "ok"
    if breach:
        capacity_status = "breached"
    elif effective_multiplier <= Decimal("0.50"):
        capacity_status = "constrained"
    elif regime["label"] != "normal" or ruin["label"] != "normal":
        capacity_status = "narrowed"

    return {
        "strategy_family": normalized_family,
        "strategy_version_id": version_id,
        "strategy_version_key": version_key,
        "risk_budget_policy": serialize_risk_budget_policy(policy),
        "current_outstanding_usd": current_outstanding.quantize(MONEY_Q),
        "base_outstanding_cap_usd": base_outstanding_cap.quantize(MONEY_Q),
        "effective_outstanding_cap_usd": effective_outstanding_cap,
        "capacity_ceiling_usd": base_capacity_ceiling.quantize(MONEY_Q),
        "effective_capacity_ceiling_usd": effective_capacity_ceiling,
        "max_order_notional_usd": policy["capacity"]["max_order_notional_usd"].quantize(MONEY_Q),
        "effective_max_order_notional_usd": effective_max_order_notional,
        "max_open_orders": max_open_orders,
        "effective_max_open_orders": effective_max_open_orders,
        "open_order_count": open_order_count,
        "incident_count_24h": incident_count,
        "serious_guardrail_count_24h": serious_guardrail_count,
        "coverage_limited_count_24h": coverage_limited_count,
        "shadow_gap_breach_count_24h": shadow_gap_breach_count,
        "regime_label": regime["label"],
        "regime_multiplier": regime["multiplier"],
        "regime_reasons": regime["reasons"],
        "risk_of_ruin_score": ruin["score"],
        "risk_of_ruin_label": ruin["label"],
        "risk_of_ruin_multiplier": ruin["multiplier"],
        "concentration_utilization": _to_decimal(concentration_utilization) or ZERO,
        "capacity_status": capacity_status,
        "action": "block" if breach else "allow",
        "reason_codes": sorted(set(reason_codes)),
        "breach": breach,
        "computed_at": effective_now,
    }


async def evaluate_budget_request(
    session: AsyncSession,
    *,
    strategy_family: str,
    strategy_version_id: int | None = None,
    requested_notional_usd: Decimal | float | int | None,
    requested_open_orders_delta: int = 1,
    allow_reduce: bool = False,
    concentration_utilization: Decimal | float | int | None = None,
    cluster_utilization: Decimal | float | int | None = None,
    book_fresh: bool | None = None,
    fillable_fraction: Decimal | float | int | None = None,
) -> dict[str, Any]:
    requested = (_to_decimal(requested_notional_usd) or ZERO).quantize(MONEY_Q)
    status = await build_strategy_budget_status(
        session,
        strategy_family=strategy_family,
        strategy_version_id=strategy_version_id,
        concentration_utilization=concentration_utilization,
        book_fresh=book_fresh,
        fillable_fraction=fillable_fraction,
    )
    approved_notional = requested
    reason_codes = list(status["reason_codes"])
    blocked_reason_code: str | None = None
    action = "allow"

    if status["action"] == "block":
        action = "block"
        blocked_reason_code = reason_codes[0] if reason_codes else "family_cap_exceeded"
        approved_notional = ZERO
    elif status["effective_max_order_notional_usd"] and requested > status["effective_max_order_notional_usd"]:
        reason_codes.append("capacity_ceiling_exceeded")
        if allow_reduce:
            action = "reduce"
            approved_notional = status["effective_max_order_notional_usd"]
        else:
            action = "block"
            blocked_reason_code = "capacity_ceiling_exceeded"
            approved_notional = ZERO
    elif status["effective_outstanding_cap_usd"] and (
        status["current_outstanding_usd"] + requested > status["effective_outstanding_cap_usd"]
    ):
        remaining = max(status["effective_outstanding_cap_usd"] - status["current_outstanding_usd"], ZERO)
        reason_codes.append("family_cap_exceeded")
        if allow_reduce and remaining > ZERO:
            action = "reduce"
            approved_notional = remaining
        else:
            action = "block"
            blocked_reason_code = "family_cap_exceeded"
            approved_notional = ZERO
    elif status["effective_capacity_ceiling_usd"] and (
        status["current_outstanding_usd"] + requested > status["effective_capacity_ceiling_usd"]
    ):
        remaining = max(status["effective_capacity_ceiling_usd"] - status["current_outstanding_usd"], ZERO)
        reason_codes.append("capacity_ceiling_exceeded")
        if allow_reduce and remaining > ZERO:
            action = "reduce"
            approved_notional = min(approved_notional, remaining)
        else:
            action = "block"
            blocked_reason_code = "capacity_ceiling_exceeded"
            approved_notional = ZERO

    effective_max_open_orders = status["effective_max_open_orders"]
    if action != "block" and effective_max_open_orders is not None and (
        status["open_order_count"] + requested_open_orders_delta > effective_max_open_orders
    ):
        reason_codes.append("capacity_ceiling_exceeded")
        action = "block"
        blocked_reason_code = "capacity_ceiling_exceeded"
        approved_notional = ZERO

    cluster_util = _to_decimal(cluster_utilization)
    if cluster_util is not None and cluster_util > ONE:
        reason_codes.append("cluster_cap_exceeded")
        if allow_reduce and action != "block":
            action = "reduce"
            approved_notional = min(approved_notional, requested / cluster_util)
        else:
            action = "block"
            blocked_reason_code = "cluster_cap_exceeded"
            approved_notional = ZERO

    if status["risk_of_ruin_label"] == "critical":
        reason_codes.append("risk_of_ruin_exceeded")
        action = "block"
        blocked_reason_code = blocked_reason_code or "risk_of_ruin_exceeded"
        approved_notional = ZERO

    budget_metadata = {
        "status": serialize_risk_budget_status(status),
        "requested_notional_usd": _decimal_string(requested),
        "approved_notional_usd": _decimal_string(approved_notional),
        "cluster_utilization": _decimal_string(cluster_util),
        "book_fresh": book_fresh,
        "fillable_fraction": _decimal_string(fillable_fraction),
    }
    return {
        "approved": action != "block",
        "action": action,
        "approved_notional_usd": approved_notional.quantize(MONEY_Q),
        "blocked_reason_code": blocked_reason_code,
        "reason_codes": sorted(set(reason_codes)),
        "status": status,
        "budget_metadata_json": budget_metadata,
    }


async def record_budget_gate_evaluation(
    session: AsyncSession,
    *,
    strategy_family: str,
    strategy_version_id: int | None,
    reason_code: str,
    summary_json: dict[str, Any],
    observed_at: datetime | None = None,
) -> None:
    if strategy_version_id is None:
        return
    from app.strategies.promotion import (
        PROMOTION_EVALUATION_STATUS_BLOCKED,
        PROMOTION_EVALUATION_STATUS_OBSERVE,
        rolling_promotion_window_bounds,
        upsert_promotion_evaluation,
    )
    from app.strategies.registry import PROMOTION_GATE_POLICY_V1, sync_strategy_registry

    registry_state = await sync_strategy_registry(session)
    family_row = registry_state["family_rows"].get(str(strategy_family).strip().lower())
    gate_policy = registry_state["gate_policy_rows"].get(PROMOTION_GATE_POLICY_V1)
    if family_row is None:
        return
    window_start, window_end = rolling_promotion_window_bounds(observed_at or datetime.now(timezone.utc))
    evaluation_status = PROMOTION_EVALUATION_STATUS_BLOCKED if summary_json.get("breach") else PROMOTION_EVALUATION_STATUS_OBSERVE
    await upsert_promotion_evaluation(
        session,
        family_id=family_row.id,
        strategy_version_id=int(strategy_version_id),
        gate_policy_id=gate_policy.id if gate_policy is not None else None,
        evaluation_kind=PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET,
        evaluation_status=evaluation_status,
        autonomy_tier="shadow_only",
        evaluation_window_start=window_start,
        evaluation_window_end=window_end,
        provenance_json={
            "source": "risk_budget_policy",
            "reason_code": reason_code,
            "strategy_family": strategy_family,
        },
        summary_json=_json_safe(summary_json),
    )


async def append_budget_demotion_event(
    session: AsyncSession,
    *,
    strategy_family: str,
    strategy_version_id: int | None,
    reason_code: str,
    details_json: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> DemotionEvent | None:
    if strategy_version_id is None:
        return None
    family_row = await _family_row(session, strategy_family=str(strategy_family).strip().lower())
    if family_row is None:
        return None
    effective_observed = _ensure_utc(observed_at) or datetime.now(timezone.utc)
    recent = (
        await session.execute(
            select(DemotionEvent)
            .where(
                DemotionEvent.family_id == family_row.id,
                DemotionEvent.strategy_version_id == int(strategy_version_id),
                DemotionEvent.reason_code == reason_code,
                DemotionEvent.observed_at_local >= effective_observed - timedelta(hours=1),
            )
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if recent is not None:
        return recent
    row = DemotionEvent(
        family_id=family_row.id,
        strategy_version_id=int(strategy_version_id),
        prior_autonomy_tier=None,
        fallback_autonomy_tier="shadow_only",
        reason_code=reason_code,
        cooling_off_ends_at=effective_observed + timedelta(hours=24),
        details_json=_json_safe(details_json or {}),
        observed_at_local=effective_observed,
    )
    session.add(row)
    await session.flush()
    return row


async def build_family_budget_summaries(session: AsyncSession) -> list[dict[str, Any]]:
    from app.strategies.registry import sync_strategy_registry

    await sync_strategy_registry(session)
    rows = (
        await session.execute(
            select(StrategyVersion, StrategyFamilyRegistry)
            .join(StrategyFamilyRegistry, StrategyFamilyRegistry.id == StrategyVersion.family_id)
            .where(StrategyVersion.is_current.is_(True))
            .order_by(StrategyFamilyRegistry.family.asc())
        )
    ).all()
    payload: list[dict[str, Any]] = []
    for version, family in rows:
        status = await build_strategy_budget_status(
            session,
            strategy_family=family.family,
            strategy_version_id=version.id,
        )
        payload.append(
            {
                "family": family.family,
                "label": family.label,
                "current_version": {
                    "id": version.id,
                    "version_key": version.version_key,
                    "version_label": version.version_label,
                },
                "risk_budget_policy": status["risk_budget_policy"],
                "risk_budget_status": serialize_risk_budget_status(status),
            }
        )
    return payload
