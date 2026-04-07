"""Market resolution service: backfill resolved_correctly on signals when markets settle."""
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market import Market, Outcome
from app.models.signal import Signal

logger = logging.getLogger(__name__)


async def resolve_signals(session: AsyncSession, platform: str, resolved_markets: list[dict]) -> int:
    """Match resolved markets to signals and set resolved_correctly.

    For Polymarket: resolved_markets contains {platform_id, winning_outcome_id, winner}.
    For Kalshi: resolved_markets contains {platform_id, winning_outcome} where winning_outcome is 'yes'/'no'.

    A signal is resolved_correctly=True if:
      - direction == "up" and the signal's outcome IS the winning outcome
      - direction == "down" and the signal's outcome is NOT the winning outcome
    Non-directional signals (no direction in details) stay resolved_correctly=NULL.

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

            count += 1

    if count > 0:
        await session.commit()
        logger.info("Resolved %d signals for platform %s", count, platform)

    return count


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
