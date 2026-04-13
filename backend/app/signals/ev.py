"""Expected Value calculator for prediction market signals.

EV for a YES position: (probability * profit_if_win) - ((1 - probability) * loss_if_lose)
In prediction markets at price p with estimated probability q:
    If we buy YES at $p: win = $1-p, lose = $p
    EV = q * (1-p) - (1-q) * p = q - p

This simplifies to: EV = estimated_probability - market_price
"""
from decimal import Decimal


def compute_ev(
    estimated_probability: Decimal,
    market_price: Decimal,
) -> Decimal:
    """Compute expected value per share for a YES position.

    If EV is positive, buying YES is +EV.
    If EV is negative, buying NO (i.e., selling YES) may be +EV.

    Returns EV in dollars per $1-resolving share.
    """
    return (estimated_probability - market_price).quantize(Decimal("0.000001"))


def compute_directional_ev_full(
    direction: str,
    estimated_probability: Decimal,
    entry_price: Decimal,
) -> dict:
    """Compute EV breakdown for an already-chosen trade direction and entry price."""
    if direction == "buy_yes":
        win_probability = estimated_probability
    elif direction == "buy_no":
        win_probability = Decimal("1") - estimated_probability
    else:
        raise ValueError(f"Unsupported direction: {direction}")

    potential_profit = Decimal("1") - entry_price
    potential_loss = entry_price
    ev_per_share = (
        win_probability * potential_profit
        - (Decimal("1") - win_probability) * potential_loss
    ).quantize(Decimal("0.000001"))

    return {
        "direction": direction,
        "win_probability": win_probability.quantize(Decimal("0.000001")),
        "ev_per_share": ev_per_share,
        "edge_pct": (ev_per_share.copy_abs() * Decimal("100")).quantize(Decimal("0.01")),
        "entry_price": entry_price.quantize(Decimal("0.000001")),
        "potential_profit": potential_profit.quantize(Decimal("0.000001")),
        "potential_loss": potential_loss.quantize(Decimal("0.000001")),
    }


def compute_ev_full(
    estimated_probability: Decimal,
    market_price: Decimal,
) -> dict:
    """Compute full EV breakdown for display.

    Returns dict with:
        direction: "buy_yes" or "buy_no"
        ev_per_share: absolute EV
        edge_pct: edge as percentage
        potential_profit: profit per share if correct
        potential_loss: loss per share if wrong
    """
    raw_ev = estimated_probability - market_price

    if raw_ev >= Decimal("0"):
        direction = "buy_yes"
        entry_price = market_price
    else:
        direction = "buy_no"
        entry_price = Decimal("1") - market_price

    result = compute_directional_ev_full(
        direction=direction,
        estimated_probability=estimated_probability,
        entry_price=entry_price,
    )
    result["edge_pct"] = (abs(raw_ev) * Decimal("100")).quantize(Decimal("0.01"))
    return result
