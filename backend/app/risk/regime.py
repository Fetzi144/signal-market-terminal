from __future__ import annotations

from decimal import Decimal
from typing import Any

REGIME_NORMAL = "normal"
REGIME_THIN_LIQUIDITY = "thin_liquidity"
REGIME_STRESSED = "stressed"
REGIME_HALTED = "halted"

ZERO = Decimal("0")
ONE = Decimal("1")


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def classify_regime(
    *,
    book_fresh: bool | None = None,
    fillable_fraction: Decimal | float | int | None = None,
    incident_count_24h: int = 0,
    serious_guardrail_count_24h: int = 0,
    coverage_limited_count_24h: int = 0,
    shadow_gap_breach_count_24h: int = 0,
    custom_multipliers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fill_fraction = _to_decimal(fillable_fraction)
    multipliers = {
        REGIME_NORMAL: ONE,
        REGIME_THIN_LIQUIDITY: Decimal("0.75"),
        REGIME_STRESSED: Decimal("0.50"),
        REGIME_HALTED: ZERO,
    }
    for key, value in (custom_multipliers or {}).items():
        decimal_value = _to_decimal(value)
        if key in multipliers and decimal_value is not None:
            multipliers[key] = max(min(decimal_value, ONE), ZERO)

    reasons: list[str] = []
    label = REGIME_NORMAL

    if fill_fraction is not None and fill_fraction < ONE:
        reasons.append("limited_fillable_depth")
    if book_fresh is False:
        reasons.append("stale_or_missing_book")
    if incident_count_24h > 0:
        reasons.append("incident_pressure")
    if coverage_limited_count_24h > 0:
        reasons.append("coverage_limited")
    if serious_guardrail_count_24h > 0:
        reasons.append("serious_guardrail_pressure")
    if shadow_gap_breach_count_24h > 0:
        reasons.append("shadow_gap_pressure")

    if serious_guardrail_count_24h > 0 or shadow_gap_breach_count_24h > 0:
        label = REGIME_HALTED
    elif coverage_limited_count_24h > 0 or incident_count_24h >= 3 or book_fresh is False:
        label = REGIME_STRESSED
    elif incident_count_24h > 0 or fill_fraction is None or fill_fraction < ONE:
        label = REGIME_THIN_LIQUIDITY

    return {
        "label": label,
        "multiplier": multipliers[label],
        "reasons": reasons,
        "inputs": {
            "book_fresh": book_fresh,
            "fillable_fraction": str(fill_fraction) if fill_fraction is not None else None,
            "incident_count_24h": int(incident_count_24h),
            "serious_guardrail_count_24h": int(serious_guardrail_count_24h),
            "coverage_limited_count_24h": int(coverage_limited_count_24h),
            "shadow_gap_breach_count_24h": int(shadow_gap_breach_count_24h),
        },
    }
