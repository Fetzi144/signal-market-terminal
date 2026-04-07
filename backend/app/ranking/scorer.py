"""Compute rank_score for signal candidates and persist them with dedupe."""
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal import Signal
from app.signals.base import SignalCandidate

logger = logging.getLogger(__name__)

DEDUPE_WINDOW_MINUTES = 15


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
        # Check dedupe: same type + outcome + bucket already exists?
        existing = await session.execute(
            select(Signal.id).where(
                Signal.signal_type == c.signal_type,
                Signal.outcome_id == uuid.UUID(c.outcome_id),
                Signal.dedupe_bucket == bucket,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue

        rank = compute_rank_score(c.signal_score, c.confidence)

        signal = Signal(
            id=uuid.uuid4(),
            signal_type=c.signal_type,
            market_id=uuid.UUID(c.market_id),
            outcome_id=uuid.UUID(c.outcome_id),
            fired_at=now,
            dedupe_bucket=bucket,
            signal_score=c.signal_score,
            confidence=c.confidence,
            rank_score=rank,
            details=c.details,
            price_at_fire=c.price_at_fire,
        )
        session.add(signal)
        new_signals.append(signal)

    if new_signals:
        await session.commit()
        logger.info("Persisted %d new signals (dedupe filtered %d)", len(new_signals), len(candidates) - len(new_signals))

    return len(new_signals), new_signals
