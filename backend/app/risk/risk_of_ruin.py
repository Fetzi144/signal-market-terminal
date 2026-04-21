from __future__ import annotations

from decimal import Decimal
from typing import Any

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


def calculate_risk_of_ruin(
    *,
    loss_utilization: Decimal | float | int | None = None,
    open_exposure_utilization: Decimal | float | int | None = None,
    concentration_utilization: Decimal | float | int | None = None,
    warning_threshold: Decimal | float | int = Decimal("0.50"),
    critical_threshold: Decimal | float | int = Decimal("0.90"),
    warning_multiplier: Decimal | float | int = Decimal("0.75"),
    critical_multiplier: Decimal | float | int = ZERO,
) -> dict[str, Any]:
    inputs = [
        max(min(_to_decimal(loss_utilization) or ZERO, ONE), ZERO),
        max(min(_to_decimal(open_exposure_utilization) or ZERO, ONE), ZERO),
        max(min(_to_decimal(concentration_utilization) or ZERO, ONE), ZERO),
    ]
    score = ((inputs[0] * Decimal("0.40")) + (inputs[1] * Decimal("0.40")) + (inputs[2] * Decimal("0.20"))).quantize(
        Decimal("0.0001")
    )
    warn = max(min(_to_decimal(warning_threshold) or Decimal("0.50"), ONE), ZERO)
    critical = max(min(_to_decimal(critical_threshold) or Decimal("0.90"), ONE), ZERO)
    warning_mult = max(min(_to_decimal(warning_multiplier) or Decimal("0.75"), ONE), ZERO)
    critical_mult = max(min(_to_decimal(critical_multiplier) or ZERO, ONE), ZERO)

    if score >= critical:
        label = "critical"
        multiplier = critical_mult
    elif score >= warn:
        label = "warning"
        multiplier = warning_mult
    else:
        label = "normal"
        multiplier = ONE

    return {
        "score": score,
        "label": label,
        "multiplier": multiplier,
        "inputs": {
            "loss_utilization": str(inputs[0]),
            "open_exposure_utilization": str(inputs[1]),
            "concentration_utilization": str(inputs[2]),
        },
        "thresholds": {
            "warning": str(warn),
            "critical": str(critical),
        },
    }
