"""Probability engine utilities for the detector framework.

Core functions:
- prior_sensitivity: dampens signals at market price extremes, amplifies near 50%
- clamp_probability: enforce [0.01, 0.99] bounds
- compute_estimated_probability: apply adjustment to market price with sensitivity
- brier_score: calibration metric for probability estimates
"""
from decimal import Decimal

# Bounds for probability estimates — never output 0 or 1
PROB_MIN = Decimal("0.01")
PROB_MAX = Decimal("0.99")


def prior_sensitivity(market_price: Decimal) -> Decimal:
    """Sensitivity multiplier based on current market price.

    Peaks at 0.50 (returns 1.0), drops toward 0 at extremes.
    Formula: 4 * p * (1 - p)

    Examples:
        sensitivity(0.50) = 1.00  (max signal value — high uncertainty)
        sensitivity(0.30) = 0.84
        sensitivity(0.70) = 0.84
        sensitivity(0.10) = 0.36
        sensitivity(0.90) = 0.36
        sensitivity(0.95) = 0.19  (near-certain — signal dampened)
        sensitivity(0.05) = 0.19
    """
    p = market_price
    return (Decimal("4") * p * (Decimal("1") - p)).quantize(Decimal("0.0001"))


def clamp_probability(p: Decimal) -> Decimal:
    """Clamp a probability to [0.01, 0.99]."""
    return max(PROB_MIN, min(PROB_MAX, p))


def compute_estimated_probability(
    market_price: Decimal,
    raw_adjustment: Decimal,
) -> tuple[Decimal, Decimal]:
    """Apply sensitivity-weighted adjustment to market price.

    Returns (estimated_probability, actual_adjustment_applied).

    The market price IS the prior. The raw_adjustment is the detector's
    opinion of how far the true probability deviates from market price.
    Sensitivity dampens this near extremes.
    """
    sensitivity = prior_sensitivity(market_price)
    adjusted = raw_adjustment * sensitivity
    estimated = clamp_probability(market_price + adjusted)
    actual_adjustment = estimated - market_price
    return (
        estimated.quantize(Decimal("0.0001")),
        actual_adjustment.quantize(Decimal("0.0001")),
    )


def brier_score(predictions: list[tuple[Decimal, bool]]) -> Decimal | None:
    """Compute Brier score for a list of (predicted_probability, actual_outcome) pairs.

    Brier score = mean((predicted - actual)^2)
    Lower is better. Perfect = 0.0, random coin-flip = 0.25.

    Returns None if no predictions provided.
    """
    if not predictions:
        return None
    total = Decimal("0")
    for predicted, actual in predictions:
        outcome = Decimal("1") if actual else Decimal("0")
        total += (predicted - outcome) ** 2
    return (total / len(predictions)).quantize(Decimal("0.000001"))


def calibration_buckets(
    predictions: list[tuple[Decimal, bool]],
    n_bins: int = 10,
) -> list[dict]:
    """Bucket predictions into bins and compute actual vs predicted rates.

    Returns list of:
        {bin_center, predicted_rate, actual_rate, sample_size, correct}
    """
    if not predictions:
        return []

    bin_width = Decimal("1") / n_bins
    buckets: dict[int, list[tuple[Decimal, bool]]] = {}

    for predicted, actual in predictions:
        # Determine bin index (0-based)
        bin_idx = min(int(predicted / bin_width), n_bins - 1)
        buckets.setdefault(bin_idx, []).append((predicted, actual))

    result = []
    for bin_idx in range(n_bins):
        items = buckets.get(bin_idx, [])
        if not items:
            continue

        bin_center = (Decimal(str(bin_idx)) + Decimal("0.5")) * bin_width
        avg_predicted = sum(p for p, _ in items) / len(items)
        correct = sum(1 for _, a in items if a)
        actual_rate = Decimal(str(correct)) / len(items)

        result.append({
            "bin_center": float(bin_center.quantize(Decimal("0.01"))),
            "predicted_rate": float(avg_predicted.quantize(Decimal("0.0001"))),
            "actual_rate": float(actual_rate.quantize(Decimal("0.0001"))),
            "sample_size": len(items),
            "correct": correct,
        })

    return result
