"""Bayesian confluence engine: fuse multiple detector outputs on the same
outcome into a single posterior probability estimate.

When multiple detectors fire on the same outcome_id within a time window,
their probability adjustments are combined via Bayesian updating with
correlation discounts to avoid double-counting correlated evidence.

Core math:
    Prior = market_price
    Each detector's probability adjustment → likelihood ratio
    P_posterior ∝ P_prior × LR_1 × LR_2 × ... × LR_n
    Independence correction via pairwise correlation discounts
"""
import logging
from decimal import Decimal

from app.signals.base import SignalCandidate
from app.signals.probability import PROB_MAX, PROB_MIN, clamp_probability

logger = logging.getLogger(__name__)

# Pairwise correlation estimates between detector types.
# Higher correlation → more discount when combining (less independent information).
# These are domain-informed priors, to be tuned from data later.
DETECTOR_CORRELATIONS: dict[frozenset[str], Decimal] = {
    frozenset({"price_move", "volume_spike"}): Decimal("0.6"),
    frozenset({"price_move", "order_flow_imbalance"}): Decimal("0.3"),
    frozenset({"volume_spike", "order_flow_imbalance"}): Decimal("0.4"),
    frozenset({"price_move", "smart_money"}): Decimal("0.2"),
    frozenset({"volume_spike", "smart_money"}): Decimal("0.3"),
    frozenset({"order_flow_imbalance", "smart_money"}): Decimal("0.2"),
}

DEFAULT_CORRELATION = Decimal("0.1")


def _get_correlation(type_a: str, type_b: str) -> Decimal:
    """Look up pairwise correlation between two detector types."""
    return DETECTOR_CORRELATIONS.get(frozenset({type_a, type_b}), DEFAULT_CORRELATION)


def _probability_to_odds(p: Decimal) -> Decimal:
    """Convert probability to odds ratio: p / (1-p)."""
    p = max(PROB_MIN, min(PROB_MAX, p))
    return p / (Decimal("1") - p)


def _odds_to_probability(odds: Decimal) -> Decimal:
    """Convert odds ratio back to probability: odds / (1 + odds)."""
    return odds / (Decimal("1") + odds)


def _adjustment_to_likelihood_ratio(
    market_price: Decimal,
    adjustment: Decimal,
) -> Decimal:
    """Convert a probability adjustment to a Bayes likelihood ratio.

    If the detector says the true probability is market_price + adjustment,
    the likelihood ratio is:
        LR = P(signal | YES) / P(signal | NO)
           = (market_price + adjustment) / (1 - (market_price + adjustment))
             ÷ market_price / (1 - market_price)
           = odds(adjusted) / odds(prior)
    """
    adjusted = clamp_probability(market_price + adjustment)
    prior_odds = _probability_to_odds(market_price)
    adjusted_odds = _probability_to_odds(adjusted)

    if prior_odds == 0:
        return Decimal("1")

    return adjusted_odds / prior_odds


def _apply_correlation_discount(
    likelihood_ratio: Decimal,
    correlation: Decimal,
) -> Decimal:
    """Discount a likelihood ratio based on correlation with already-seen evidence.

    A correlation of 0 means fully independent (no discount).
    A correlation of 1 means fully redundant (full discount → LR=1).

    Discount formula: LR_effective = LR^(1 - correlation)
    This is a standard approach in dependent Bayesian evidence fusion.
    """
    if correlation >= Decimal("1"):
        return Decimal("1")
    if correlation <= Decimal("0"):
        return likelihood_ratio

    exponent = float(Decimal("1") - correlation)
    lr_float = float(likelihood_ratio)

    if lr_float <= 0:
        return Decimal("1")

    discounted = Decimal(str(lr_float ** exponent))
    return discounted


def fuse_signals(
    signals: list[SignalCandidate],
    market_price: Decimal,
) -> SignalCandidate | None:
    """Bayesian fusion of multiple detector signals on the same outcome.

    Args:
        signals: List of SignalCandidate objects for the same outcome_id,
                 all within the confluence time window.
        market_price: Current market price (the Bayesian prior).

    Returns:
        A new SignalCandidate with signal_type="confluence" and combined
        probability estimate, or None if fusion isn't possible.
    """
    # Filter to directional signals with probability adjustments
    directional = [
        s for s in signals
        if s.is_directional
        and s.probability_adjustment is not None
        and s.probability_adjustment != Decimal("0")
    ]

    if len(directional) < 2:
        return None

    # Ensure all signals are for the same outcome
    outcome_ids = {s.outcome_id for s in directional}
    if len(outcome_ids) != 1:
        return None

    # Start with prior odds
    prior_odds = _probability_to_odds(market_price)
    combined_odds = prior_odds

    # Track which detector types we've already incorporated (for correlation)
    seen_types: list[str] = []

    # Sort by absolute adjustment magnitude (strongest signal first)
    sorted_signals = sorted(directional, key=lambda s: abs(s.probability_adjustment), reverse=True)

    contributing = []
    for signal in sorted_signals:
        lr = _adjustment_to_likelihood_ratio(market_price, signal.probability_adjustment)

        # Apply correlation discount against ALL previously seen detectors.
        # Use the maximum correlation (most conservative).
        max_correlation = Decimal("0")
        for seen_type in seen_types:
            corr = _get_correlation(signal.signal_type, seen_type)
            max_correlation = max(max_correlation, corr)

        lr_discounted = _apply_correlation_discount(lr, max_correlation)
        combined_odds *= lr_discounted
        seen_types.append(signal.signal_type)
        contributing.append({
            "signal_type": signal.signal_type,
            "timeframe": signal.timeframe,
            "adjustment": str(signal.probability_adjustment),
            "likelihood_ratio": str(lr.quantize(Decimal("0.0001"))),
            "correlation_discount": str(max_correlation),
            "lr_discounted": str(lr_discounted.quantize(Decimal("0.0001"))),
        })

    # Convert back to probability
    posterior = _odds_to_probability(combined_odds)
    posterior = clamp_probability(posterior).quantize(Decimal("0.0001"))
    total_adjustment = (posterior - market_price).quantize(Decimal("0.0001"))

    # Non-directional modifiers: adjust confidence
    modifiers = [s for s in signals if not s.is_directional]
    confidence_boost = Decimal("0")
    modifier_details = []
    for mod in modifiers:
        if mod.signal_type == "deadline_near":
            urgency = Decimal(str(mod.details.get("urgency", "0")))
            confidence_boost += urgency * Decimal("0.1")
            modifier_details.append({"type": "deadline_near", "urgency": str(urgency)})
        elif mod.signal_type == "spread_change":
            if mod.details.get("direction") == "narrowing":
                confidence_boost += Decimal("0.05")
            else:
                confidence_boost -= Decimal("0.05")
            modifier_details.append({"type": "spread_change", "direction": mod.details.get("direction")})
        elif mod.signal_type == "liquidity_vacuum":
            confidence_boost -= Decimal("0.1")
            modifier_details.append({"type": "liquidity_vacuum"})

    # Compute confluence signal score and confidence
    avg_score = sum(s.signal_score for s in directional) / len(directional)
    avg_confidence = sum(s.confidence for s in directional) / len(directional)
    # Boost for multi-detector agreement
    agreement_boost = Decimal("0.1") * (len(directional) - 1)
    final_confidence = min(Decimal("1.0"), avg_confidence + confidence_boost + agreement_boost)

    # Use the first signal's market/outcome info
    ref = sorted_signals[0]

    return SignalCandidate(
        signal_type="confluence",
        market_id=ref.market_id,
        outcome_id=ref.outcome_id,
        signal_score=min(Decimal("1.0"), avg_score + agreement_boost).quantize(Decimal("0.001")),
        confidence=final_confidence.quantize(Decimal("0.001")),
        price_at_fire=ref.price_at_fire,
        estimated_probability=posterior,
        probability_adjustment=total_adjustment,
        is_directional=True,
        details={
            "market_question": ref.details.get("market_question", ""),
            "outcome_name": ref.details.get("outcome_name", ""),
            "market_price": str(market_price),
            "prior_odds": str(prior_odds.quantize(Decimal("0.0001"))),
            "posterior_probability": str(posterior),
            "total_adjustment": str(total_adjustment),
            "contributing_detectors": contributing,
            "modifiers": modifier_details,
            "detector_count": len(directional),
            "modifier_count": len(modifiers),
        },
    )
