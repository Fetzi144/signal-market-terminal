from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.config import settings
from app.execution.polymarket_live_state import effective_kill_switch_enabled
from app.models.polymarket_live_execution import LiveOrder, PolymarketLiveState
from app.models.polymarket_pilot import PolymarketPilotConfig

SUPPORTED_PHASE12_FAMILY = "exec_policy"
SUPPORTED_PILOT_FAMILIES = {SUPPORTED_PHASE12_FAMILY}
ZERO = Decimal("0")
PRICE_Q = Decimal("0.00000001")
BPS_Q = Decimal("0.0001")


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        normalized = ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [json_safe(inner) for inner in value]
    return value


def stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(json_safe(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def normalize_strategy_family(value: str | None) -> str:
    normalized = str(value or settings.polymarket_pilot_default_strategy_family or SUPPORTED_PHASE12_FAMILY).strip().lower()
    if normalized not in SUPPORTED_PILOT_FAMILIES:
        raise ValueError(f"Unsupported strategy family: {value}")
    return normalized


def details_with(base: dict[str, Any] | list | str | None, **updates: Any) -> dict[str, Any]:
    details = dict(base) if isinstance(base, dict) else {}
    for key, value in updates.items():
        if value is not None:
            details[key] = value
    return details


def approval_required(config: PolymarketPilotConfig | None) -> bool:
    if config is None:
        return bool(settings.polymarket_live_manual_approval_required)
    return bool(config.manual_approval_required)


def pilot_limit_decimal(config: PolymarketPilotConfig | None, attr: str, fallback: float | Decimal | None) -> Decimal | None:
    value = getattr(config, attr, None) if config is not None else None
    if value is not None:
        return to_decimal(value)
    return to_decimal(fallback)


def pilot_limit_int(config: PolymarketPilotConfig | None, attr: str, fallback: int | None) -> int | None:
    value = getattr(config, attr, None) if config is not None else None
    if value is not None:
        return int(value)
    return fallback


def live_order_notional(order: LiveOrder) -> Decimal:
    price = to_decimal(order.limit_price) or to_decimal(order.target_price) or ZERO
    size = to_decimal(order.requested_size) or ZERO
    return (price * size).quantize(PRICE_Q) if price > ZERO and size > ZERO else ZERO


def heartbeat_status(state: PolymarketLiveState | None, *, needed: bool) -> str:
    if not needed:
        return "idle"
    if state is None or state.heartbeat_healthy is None:
        return "pending"
    return "healthy" if state.heartbeat_healthy else "degraded"


def guardrail_from_submission_reason(reason: str) -> tuple[str, str, str] | None:
    mapping = {
        "approval_expired": ("approval_ttl", "warning", "block"),
        "stale_execution_decision": ("decision_age", "warning", "block"),
        "heartbeat_degraded": ("heartbeat_degraded", "warning", "block"),
        "max_outstanding_notional_exceeded": ("max_outstanding_notional", "error", "block"),
    }
    return mapping.get(reason)


def price_gap_bps(*, expected: Decimal | None, actual: Decimal | None, side: str | None) -> Decimal | None:
    if expected is None or actual is None or expected <= ZERO:
        return None
    if str(side or "BUY").upper() == "SELL":
        gap = ((expected - actual) / expected) * Decimal("10000")
    else:
        gap = ((actual - expected) / expected) * Decimal("10000")
    return gap.quantize(BPS_Q)


__all__ = [
    "BPS_Q",
    "PRICE_Q",
    "SUPPORTED_PHASE12_FAMILY",
    "SUPPORTED_PILOT_FAMILIES",
    "ZERO",
    "approval_required",
    "details_with",
    "effective_kill_switch_enabled",
    "ensure_utc",
    "guardrail_from_submission_reason",
    "heartbeat_status",
    "json_safe",
    "live_order_notional",
    "normalize_strategy_family",
    "pilot_limit_decimal",
    "pilot_limit_int",
    "price_gap_bps",
    "serialize_decimal",
    "stable_hash",
    "to_decimal",
]
