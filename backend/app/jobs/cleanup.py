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
from app.models.polymarket_stream import (
    PolymarketIngestIncident,
    PolymarketMarketEvent,
    PolymarketResyncRun,
)
from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot

logger = logging.getLogger(__name__)
_RAW_EVENT_DELETE_BATCH_SIZE = 5_000


async def _delete_in_batches(
    session: AsyncSession,
    model,
    pk_col,
    *filters,
    batch_size: int = _RAW_EVENT_DELETE_BATCH_SIZE,
) -> int:
    """Delete large retention sets in small transactions."""
    total = 0
    while True:
        ids = (
            await session.execute(
                select(pk_col)
                .where(*filters)
                .order_by(pk_col.asc())
                .limit(max(1, int(batch_size)))
            )
        ).scalars().all()
        if not ids:
            break
        result = await session.execute(delete(model).where(pk_col.in_(ids)))
        await session.commit()
        total += int(result.rowcount or 0)
    return total


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

    result = await session.execute(
        delete(PolymarketIngestIncident).where(PolymarketIngestIncident.created_at < raw_cutoff)
    )
    counts["polymarket_ingest_incidents"] = result.rowcount

    result = await session.execute(
        delete(PolymarketResyncRun).where(PolymarketResyncRun.started_at < raw_cutoff)
    )
    counts["polymarket_resync_runs"] = result.rowcount

    # Raw market events are one of the scanner's largest tables. Delete them
    # last so ON DELETE CASCADE/SET NULL can clean dependent evidence safely.
    counts["polymarket_market_events"] = await _delete_in_batches(
        session,
        PolymarketMarketEvent,
        PolymarketMarketEvent.id,
        PolymarketMarketEvent.received_at_local < raw_cutoff,
    )

    await session.commit()

    total = sum(counts.values())
    if total > 0:
        logger.info("Cleanup: deleted %s", ", ".join(f"{k}={v}" for k, v in counts.items() if v > 0))
    else:
        logger.info("Cleanup: nothing to delete")

    return counts
