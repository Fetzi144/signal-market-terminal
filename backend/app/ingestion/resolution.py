"""Market resolution service: backfill resolved_correctly on signals when markets settle.

Enhanced in Q2 Phase 1 to capture closing_price, resolution_price, CLV, and profit_loss.
"""
import logging
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market import Market, Outcome
from app.models.signal import Signal
from app.models.snapshot import PriceSnapshot

logger = logging.getLogger(__name__)


async def resolve_signals(session: AsyncSession, platform: str, resolved_markets: list[dict]) -> int:
    """Match resolved markets to signals and set resolved_correctly + CLV fields.

    For Polymarket: resolved_markets contains {platform_id, winning_outcome_id, winner}.
    For Kalshi: resolved_markets contains {platform_id, winning_outcome} where winning_outcome is 'yes'/'no'.

    A signal is resolved_correctly=True if:
      - direction == "up" and the signal's outcome IS the winning outcome
      - direction == "down" and the signal's outcome is NOT the winning outcome
    Non-directional signals (no direction in details) stay resolved_correctly=NULL.

    CLV fields set on every resolved signal (where data available):
      - closing_price: last snapshot price for the outcome before resolution
      - resolution_price: 1.0 if outcome won, 0.0 if lost
      - clv: closing_price - price_at_fire (positive = signal beat market)
      - profit_loss: resolution_price - price_at_fire (for "up"); price_at_fire - resolution_price (for "down")

    Returns count of signals resolved.
    """
    count = 0

    for rm in resolved_markets:
        platform_id = rm["platform_id"]

        # Find the market in our DB
        result = await session.execute(
            select(Market).where(Market.platform == platform, Market.platform_id == platform_id)
        )
        market = result.scalar_one_or_none()
        if market is None:
            continue

        # Determine the winning outcome ID(s) for this market
        winning_outcome_ids = await _get_winning_outcome_ids(session, market, platform, rm)

        if not winning_outcome_ids:
            continue

        # Find all unresolved signals for outcomes of this market
        outcome_result = await session.execute(
            select(Outcome.id).where(Outcome.market_id == market.id)
        )
        all_outcome_ids = [r for r in outcome_result.scalars().all()]

        if not all_outcome_ids:
            continue

        signal_result = await session.execute(
            select(Signal).where(
                Signal.outcome_id.in_(all_outcome_ids),
                Signal.resolved_correctly.is_(None),
            )
        )
        signals = signal_result.scalars().all()

        # Pre-fetch closing prices for all outcomes in this market (last snapshot per outcome)
        closing_prices = await _get_closing_prices(session, all_outcome_ids)

        for signal in signals:
            direction = (signal.details or {}).get("direction")

            if direction is None:
                # Non-directional signal — cannot resolve
                continue

            outcome_is_winner = signal.outcome_id in winning_outcome_ids

            if direction == "up":
                signal.resolved_correctly = outcome_is_winner
            elif direction == "down":
                signal.resolved_correctly = not outcome_is_winner
            else:
                continue

            # Set CLV fields
            _compute_clv_fields(signal, outcome_is_winner, closing_prices, direction)

            signal.resolved = True
            count += 1

    if count > 0:
        await session.commit()
        logger.info("Resolved %d signals for platform %s", count, platform)

    return count


def _compute_clv_fields(
    signal: Signal,
    outcome_is_winner: bool,
    closing_prices: dict,
    direction: str,
) -> None:
    """Set closing_price, resolution_price, clv, profit_loss on a signal."""
    # Resolution price: what the outcome settled at
    signal.resolution_price = Decimal("1.000000") if outcome_is_winner else Decimal("0.000000")

    # Closing price: last recorded snapshot price before resolution
    closing = closing_prices.get(signal.outcome_id)
    if closing is not None:
        signal.closing_price = closing

    # CLV: did we signal before the market priced it in?
    # closing_price - price_at_fire (positive = we caught the move early)
    if signal.price_at_fire is not None and signal.closing_price is not None:
        signal.clv = signal.closing_price - signal.price_at_fire

    # Profit/Loss: actual P&L per share if we had traded at signal time
    if signal.price_at_fire is not None:
        if direction == "up":
            # Bought YES at price_at_fire, settled at resolution_price
            signal.profit_loss = signal.resolution_price - signal.price_at_fire
        elif direction == "down":
            # Bought NO at (1 - price_at_fire), settled at (1 - resolution_price)
            # Equivalent: profit_loss = price_at_fire - resolution_price
            signal.profit_loss = signal.price_at_fire - signal.resolution_price


async def _get_closing_prices(session: AsyncSession, outcome_ids: list) -> dict:
    """Get the last recorded snapshot price for each outcome.

    Returns dict mapping outcome_id -> Decimal price.
    """
    closing_prices = {}
    for outcome_id in outcome_ids:
        # Exclude post-settlement snapshots (price exactly 0 or 1) to get the
        # last meaningful market price before the outcome settled.
        result = await session.execute(
            select(PriceSnapshot.price)
            .where(
                PriceSnapshot.outcome_id == outcome_id,
                PriceSnapshot.price > Decimal("0"),
                PriceSnapshot.price < Decimal("1"),
            )
            .order_by(PriceSnapshot.captured_at.desc())
            .limit(1)
        )
        price = result.scalar_one_or_none()
        if price is not None:
            closing_prices[outcome_id] = price
    return closing_prices


async def _get_winning_outcome_ids(
    session: AsyncSession, market: Market, platform: str, rm: dict
) -> set:
    """Determine which outcome IDs are the 'winners' for a resolved market."""
    winning_ids = set()

    if platform == "polymarket":
        # Polymarket winner field is the outcome index or name
        winner = rm.get("winner")
        if winner is not None:
            # Try matching by outcome name (Yes/No) or by index
            outcomes_result = await session.execute(
                select(Outcome).where(Outcome.market_id == market.id)
            )
            outcomes = outcomes_result.scalars().all()
            for outcome in outcomes:
                # Polymarket winner is typically "Yes" or "No"
                if outcome.name.lower() == str(winner).lower():
                    winning_ids.add(outcome.id)

    elif platform == "kalshi":
        # Kalshi result is "yes" or "no"
        winning_outcome = rm.get("winning_outcome", "").lower()
        if winning_outcome:
            outcomes_result = await session.execute(
                select(Outcome).where(Outcome.market_id == market.id)
            )
            outcomes = outcomes_result.scalars().all()
            for outcome in outcomes:
                if outcome.name.lower() == winning_outcome:
                    winning_ids.add(outcome.id)

    return winning_ids
