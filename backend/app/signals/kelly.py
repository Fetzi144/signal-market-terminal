"""Kelly Criterion position sizing for prediction market trades.

Kelly fraction: f = (b*p - q) / b
    where b = net odds (payout per dollar risked), p = win probability, q = 1-p

For prediction markets buying YES at price $p:
    b = (1-p)/p   (risk $p to win $1-p)
    edge = estimated_prob - market_price
    f = edge / (1 - market_price)

We use fractional Kelly (default quarter-Kelly) for safety.
"""
from decimal import Decimal

ZERO = Decimal("0")


def kelly_size_for_trade(
    direction: str,
    estimated_probability: Decimal,
    entry_price: Decimal,
    bankroll: Decimal,
    kelly_fraction: Decimal = Decimal("0.25"),
    max_position_pct: Decimal = Decimal("0.05"),
) -> dict:
    """Compute Kelly-optimal size for a chosen trade direction and entry price."""
    if direction == "buy_yes":
        win_probability = estimated_probability
    elif direction == "buy_no":
        win_probability = Decimal("1") - estimated_probability
    else:
        return {
            "direction": "none",
            "kelly_full": ZERO,
            "kelly_used": ZERO,
            "recommended_size_usd": ZERO,
            "shares": ZERO,
            "edge_pct": ZERO,
            "entry_price": entry_price.quantize(Decimal("0.000001")),
        }

    edge = win_probability - entry_price
    if edge <= ZERO:
        return {
            "direction": direction,
            "kelly_full": ZERO,
            "kelly_used": ZERO,
            "recommended_size_usd": ZERO,
            "shares": ZERO,
            "edge_pct": (edge.copy_abs() * Decimal("100")).quantize(Decimal("0.01")),
            "entry_price": entry_price.quantize(Decimal("0.000001")),
        }

    if entry_price <= ZERO or entry_price >= Decimal("1"):
        return {
            "direction": direction,
            "kelly_full": ZERO,
            "kelly_used": ZERO,
            "recommended_size_usd": ZERO,
            "shares": ZERO,
            "edge_pct": (edge.copy_abs() * Decimal("100")).quantize(Decimal("0.01")),
            "entry_price": entry_price.quantize(Decimal("0.000001")),
        }

    kelly_f = edge / (Decimal("1") - entry_price)
    if kelly_f <= ZERO:
        return {
            "direction": direction,
            "kelly_full": ZERO,
            "kelly_used": ZERO,
            "recommended_size_usd": ZERO,
            "shares": ZERO,
            "edge_pct": (edge.copy_abs() * Decimal("100")).quantize(Decimal("0.01")),
            "entry_price": entry_price.quantize(Decimal("0.000001")),
        }

    kelly_used = kelly_f * kelly_fraction
    raw_size = bankroll * kelly_used
    max_size = bankroll * max_position_pct
    capped_size = min(raw_size, max_size)
    shares = (capped_size / entry_price).quantize(Decimal("0.0001")) if entry_price > ZERO else ZERO

    return {
        "direction": direction,
        "kelly_full": kelly_f.quantize(Decimal("0.0001")),
        "kelly_used": kelly_used.quantize(Decimal("0.0001")),
        "recommended_size_usd": capped_size.quantize(Decimal("0.01")),
        "shares": shares,
        "edge_pct": (edge.copy_abs() * Decimal("100")).quantize(Decimal("0.01")),
        "entry_price": entry_price.quantize(Decimal("0.000001")),
    }


def kelly_size(
    estimated_prob: Decimal,
    market_price: Decimal,
    bankroll: Decimal,
    kelly_fraction: Decimal = Decimal("0.25"),
    max_position_pct: Decimal = Decimal("0.05"),
) -> dict:
    """Compute Kelly-optimal position size for a prediction market trade.

    Args:
        estimated_prob: Our probability estimate for YES outcome.
        market_price: Current market price (what we'd pay for YES).
        bankroll: Total available bankroll in USD.
        kelly_fraction: Fraction of full Kelly to use (0.25 = quarter-Kelly).
        max_position_pct: Maximum single position as fraction of bankroll.

    Returns dict with:
        direction: "buy_yes" or "buy_no"
        kelly_full: Full Kelly fraction (before safety scaling)
        kelly_used: Fractional Kelly actually applied
        recommended_size_usd: Dollar amount to invest
        shares: Number of shares at entry price
        edge_pct: Edge as percentage
        entry_price: Price per share
    """
    edge = estimated_prob - market_price
    if edge > ZERO:
        direction = "buy_yes"
        entry_price = market_price
    elif edge < ZERO:
        direction = "buy_no"
        entry_price = Decimal("1") - market_price
    else:
        return {
            "direction": "none",
            "kelly_full": ZERO,
            "kelly_used": ZERO,
            "recommended_size_usd": ZERO,
            "shares": ZERO,
            "edge_pct": ZERO,
            "entry_price": market_price.quantize(Decimal("0.000001")),
        }
    return kelly_size_for_trade(
        direction=direction,
        estimated_probability=estimated_prob,
        entry_price=entry_price,
        bankroll=bankroll,
        kelly_fraction=kelly_fraction,
        max_position_pct=max_position_pct,
    )
