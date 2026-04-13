"""Data retention: delete old snapshots, evaluations, and resolved signals."""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketOpenInterestHistory,
    PolymarketRawCaptureRun,
    PolymarketTradeTape,
)
from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot

logger = logging.getLogger(__name__)


async def cleanup_old_data(session: AsyncSession) -> dict[str, int]:
    """Delete expired data. Returns counts of deleted rows per table."""
    now = datetime.now(timezone.utc)
    counts: dict[str, int] = {}

    # Price snapshots
    cutoff = now - timedelta(days=settings.retention_price_snapshots_days)
    result = await session.execute(
        delete(PriceSnapshot).where(PriceSnapshot.captured_at < cutoff)
    )
    counts["price_snapshots"] = result.rowcount

    # Orderbook snapshots (shorter retention, larger rows)
    cutoff = now - timedelta(days=settings.retention_orderbook_snapshots_days)
    result = await session.execute(
        delete(OrderbookSnapshot).where(OrderbookSnapshot.captured_at < cutoff)
    )
    counts["orderbook_snapshots"] = result.rowcount

    # Resolved signals and their evaluations
    cutoff = now - timedelta(days=settings.retention_signals_days)
    old_signals = await session.execute(
        select(Signal.id).where(Signal.resolved.is_(True), Signal.fired_at < cutoff)
    )
    old_signal_ids = [r for r in old_signals.scalars().all()]

    if old_signal_ids:
        result = await session.execute(
            delete(SignalEvaluation).where(SignalEvaluation.signal_id.in_(old_signal_ids))
        )
        counts["signal_evaluations"] = result.rowcount

        result = await session.execute(
            delete(Signal).where(Signal.id.in_(old_signal_ids))
        )
        counts["signals"] = result.rowcount
    else:
        counts["signal_evaluations"] = 0
        counts["signals"] = 0

    raw_cutoff = now - timedelta(days=settings.polymarket_raw_retention_days)

    result = await session.execute(
        delete(PolymarketBookSnapshot).where(PolymarketBookSnapshot.observed_at_local < raw_cutoff)
    )
    counts["polymarket_book_snapshots"] = result.rowcount

    result = await session.execute(
        delete(PolymarketBookDelta).where(PolymarketBookDelta.ingest_ts_db < raw_cutoff)
    )
    counts["polymarket_book_deltas"] = result.rowcount

    result = await session.execute(
        delete(PolymarketBboEvent).where(PolymarketBboEvent.ingest_ts_db < raw_cutoff)
    )
    counts["polymarket_bbo_events"] = result.rowcount

    result = await session.execute(
        delete(PolymarketTradeTape).where(PolymarketTradeTape.observed_at_local < raw_cutoff)
    )
    counts["polymarket_trade_tape"] = result.rowcount

    result = await session.execute(
        delete(PolymarketOpenInterestHistory).where(PolymarketOpenInterestHistory.observed_at_local < raw_cutoff)
    )
    counts["polymarket_open_interest_history"] = result.rowcount

    result = await session.execute(
        delete(PolymarketRawCaptureRun).where(
            PolymarketRawCaptureRun.completed_at.is_not(None),
            PolymarketRawCaptureRun.completed_at < raw_cutoff,
        )
    )
    counts["polymarket_raw_capture_runs"] = result.rowcount

    await session.commit()

    total = sum(counts.values())
    if total > 0:
        logger.info("Cleanup: deleted %s", ", ".join(f"{k}={v}" for k, v in counts.items() if v > 0))
    else:
        logger.info("Cleanup: nothing to delete")

    return counts
