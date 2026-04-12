"""Compute rank_score for signal candidates, persist them with dedupe,
and apply confluence scoring for multi-timeframe signals."""
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal import Signal
from app.signals.base import SignalCandidate

logger = logging.getLogger(__name__)

DEDUPE_WINDOW_MINUTES = 15
CONFLUENCE_BONUS_PER_TF = Decimal("0.15")


def compute_rank_score(signal_score: Decimal, confidence: Decimal, age_hours: float = 0) -> Decimal:
    """rank_score = signal_score * confidence * recency_weight"""
    # Recency: 1.0 at 0h, linear decay to 0.3 at 24h
    recency = max(Decimal("0.3"), Decimal("1.0") - Decimal(str(age_hours)) * Decimal("0.7") / Decimal("24"))
    return (signal_score * confidence * recency).quantize(Decimal("0.001"))


def _dedupe_bucket(dt: datetime) -> datetime:
    """Truncate to 15-minute bucket."""
    minute = (dt.minute // DEDUPE_WINDOW_MINUTES) * DEDUPE_WINDOW_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)


async def persist_signals(session: AsyncSession, candidates: list[SignalCandidate]) -> tuple[int, list[Signal]]:
    """Persist signal candidates with dedupe. Returns (count, list of new Signal objects)."""
    now = datetime.now(timezone.utc)
    bucket = _dedupe_bucket(now)
    new_signals: list[Signal] = []

    for c in candidates:
        # Check dedupe: same type + outcome + timeframe + bucket already exists?
        existing = await session.execute(
            select(Signal.id).where(
                Signal.signal_type == c.signal_type,
                Signal.outcome_id == uuid.UUID(c.outcome_id),
                Signal.timeframe == c.timeframe,
                Signal.dedupe_bucket == bucket,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue

        rank = compute_rank_score(c.signal_score, c.confidence)

        # Compute EV if probability data available
        ev = None
        if c.estimated_probability is not None and c.price_at_fire is not None:
            from app.signals.ev import compute_ev
            ev = compute_ev(c.estimated_probability, c.price_at_fire)

        signal = Signal(
            id=uuid.uuid4(),
            signal_type=c.signal_type,
            market_id=uuid.UUID(c.market_id),
            outcome_id=uuid.UUID(c.outcome_id),
            fired_at=now,
            dedupe_bucket=bucket,
            timeframe=c.timeframe,
            signal_score=c.signal_score,
            confidence=c.confidence,
            rank_score=rank,
            details=c.details,
            price_at_fire=c.price_at_fire,
            estimated_probability=c.estimated_probability,
            probability_adjustment=c.probability_adjustment,
            expected_value=ev,
        )
        session.add(signal)
        new_signals.append(signal)

    if new_signals:
        await session.commit()
        logger.info("Persisted %d new signals (dedupe filtered %d)", len(new_signals), len(candidates) - len(new_signals))

        # Apply confluence scoring for multi-timeframe signals
        confluence_count = await _apply_confluence_scoring(session, new_signals, bucket)
        if confluence_count > 0:
            logger.info("Applied confluence scoring to %d signals", confluence_count)

    return len(new_signals), new_signals


async def _apply_confluence_scoring(
    session: AsyncSession, new_signals: list[Signal], bucket: datetime
) -> int:
    """Check for confluence: same signal_type + outcome across multiple timeframes
    in the current dedupe bucket. Apply bonus to rank_score and store metadata."""
    # Group new signals by (signal_type, outcome_id) to find confluence candidates
    groups: dict[tuple[str, uuid.UUID], list[Signal]] = defaultdict(list)
    for sig in new_signals:
        if sig.outcome_id is not None:
            groups[(sig.signal_type, sig.outcome_id)].append(sig)

    updated = 0

    for (signal_type, outcome_id), signals in groups.items():
        # Also check for existing signals in same bucket with different timeframes
        existing_result = await session.execute(
            select(Signal).where(
                Signal.signal_type == signal_type,
                Signal.outcome_id == outcome_id,
                Signal.dedupe_bucket == bucket,
            )
        )
        all_tf_signals = existing_result.scalars().all()

        timeframes = list({s.timeframe for s in all_tf_signals})
        if len(timeframes) < 2:
            continue

        # Compute confluence bonus
        confluence_bonus = CONFLUENCE_BONUS_PER_TF * (len(timeframes) - 1)

        for sig in all_tf_signals:
            new_rank = min(
                Decimal("1.000"),
                sig.rank_score + confluence_bonus,
            ).quantize(Decimal("0.001"))
            sig.rank_score = new_rank
            # Store confluence metadata in details
            details = dict(sig.details) if sig.details else {}
            details["confluence_timeframes"] = sorted(timeframes)
            details["confluence_score"] = str(confluence_bonus.quantize(Decimal("0.001")))
            sig.details = details
            updated += 1

    if updated > 0:
        await session.commit()

    return updated
