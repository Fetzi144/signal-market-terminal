"""One-time backfill: compute CLV and P&L for already-resolved signals.

Run via:
    python -m app.ingestion.backfill_clv

Finds all signals where resolved_correctly is set but clv is NULL,
looks up the last PriceSnapshot for each signal's outcome, and computes
closing_price, resolution_price, clv, and profit_loss.
"""
import asyncio
import logging
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import async_session
from app.ingestion.resolution import get_closing_price
from app.models.market import Market
from app.models.signal import Signal

logger = logging.getLogger(__name__)


async def backfill_clv(session: AsyncSession) -> dict:
    """Backfill CLV fields for resolved signals missing them.

    Returns summary dict with counts.
    """
    # Find all resolved signals without CLV data
    result = await session.execute(
        select(Signal, Market.end_date)
        .join(Market, Market.id == Signal.market_id)
        .where(
            Signal.resolved_correctly.isnot(None),
            Signal.clv.is_(None),
        )
    )
    signal_rows = result.all()

    if not signal_rows:
        logger.info("Backfill: no signals need CLV computation")
        return {
            "total": 0,
            "updated": 0,
            "updated_from_price_snapshot": 0,
            "updated_from_orderbook_midpoint": 0,
            "skipped_no_snapshot": 0,
            "skipped_no_price": 0,
        }

    updated = 0
    updated_from_price_snapshot = 0
    updated_from_orderbook_midpoint = 0
    skipped_no_snapshot = 0
    skipped_no_price = 0

    for signal, market_end_date in signal_rows:
        direction = (signal.details or {}).get("direction")
        if direction is None:
            continue

        # Skip if no price_at_fire
        if signal.price_at_fire is None:
            skipped_no_price += 1
            continue

        if signal.outcome_id is None:
            skipped_no_snapshot += 1
            continue

        closing_price, source = await get_closing_price(
            session,
            signal.outcome_id,
            before=market_end_date,
        )

        if closing_price is None:
            skipped_no_snapshot += 1
            continue

        # Set resolution_price based on resolved_correctly + direction
        if direction == "up":
            outcome_won = signal.resolved_correctly
        elif direction == "down":
            outcome_won = not signal.resolved_correctly
        else:
            continue

        signal.resolution_price = Decimal("1.000000") if outcome_won else Decimal("0.000000")
        signal.closing_price = closing_price
        signal.clv = closing_price - signal.price_at_fire

        if direction == "up":
            signal.profit_loss = signal.resolution_price - signal.price_at_fire
        elif direction == "down":
            signal.profit_loss = signal.price_at_fire - signal.resolution_price

        signal.resolved = True
        updated += 1
        if source == "price_snapshot":
            updated_from_price_snapshot += 1
        elif source == "orderbook_midpoint":
            updated_from_orderbook_midpoint += 1

    if updated > 0:
        await session.commit()

    summary = {
        "total": len(signal_rows),
        "updated": updated,
        "updated_from_price_snapshot": updated_from_price_snapshot,
        "updated_from_orderbook_midpoint": updated_from_orderbook_midpoint,
        "skipped_no_snapshot": skipped_no_snapshot,
        "skipped_no_price": skipped_no_price,
    }
    logger.info("Backfill complete: %s", summary)
    return summary


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    async with async_session() as session:
        summary = await backfill_clv(session)
        print(f"Backfill results: {summary}")


if __name__ == "__main__":
    asyncio.run(main())
