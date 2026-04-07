"""Post-signal evaluation: check price at each horizon and record outcomes."""
import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import PriceSnapshot

logger = logging.getLogger(__name__)

HORIZONS = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "24h": timedelta(hours=24),
}

# How far from the target time we'll accept a snapshot
SNAPSHOT_TOLERANCE = timedelta(minutes=5)


async def evaluate_signals(session: AsyncSession) -> int:
    """Evaluate unresolved signals at each horizon. Returns evaluations created."""
    now = datetime.now(timezone.utc)
    created = 0

    # Get unresolved signals
    result = await session.execute(
        select(Signal).where(Signal.resolved.is_(False))
    )
    signals = result.scalars().all()

    for signal in signals:
        all_horizons_done = True

        for horizon_key, horizon_delta in HORIZONS.items():
            fired_at = signal.fired_at
            if fired_at.tzinfo is None:
                fired_at = fired_at.replace(tzinfo=timezone.utc)
            target_time = fired_at + horizon_delta

            # Not yet time for this horizon
            if target_time > now:
                all_horizons_done = False
                continue

            # Already evaluated?
            existing = await session.execute(
                select(SignalEvaluation.id).where(
                    SignalEvaluation.signal_id == signal.id,
                    SignalEvaluation.horizon == horizon_key,
                )
            )
            if existing.scalar_one_or_none() is not None:
                continue

            if signal.outcome_id is None or signal.price_at_fire is None:
                continue

            # Find closest snapshot to target_time
            snap = await _closest_snapshot(
                session, signal.outcome_id, target_time
            )
            if snap is None:
                # No data yet, might arrive later
                all_horizons_done = False
                continue

            price_change = snap.price - signal.price_at_fire
            if signal.price_at_fire > 0:
                price_change_pct = (price_change / signal.price_at_fire) * 100
            else:
                price_change_pct = Decimal("0")

            evaluation = SignalEvaluation(
                id=uuid.uuid4(),
                signal_id=signal.id,
                horizon=horizon_key,
                price_at_eval=snap.price,
                price_change=price_change,
                price_change_pct=price_change_pct.quantize(Decimal("0.01")),
                evaluated_at=now,
            )
            session.add(evaluation)
            created += 1

        if all_horizons_done:
            signal.resolved = True

    if created:
        await session.commit()
        logger.info("Created %d signal evaluations", created)

    return created


async def _closest_snapshot(
    session: AsyncSession, outcome_id: uuid.UUID, target_time: datetime
) -> PriceSnapshot | None:
    """Find the snapshot closest to target_time within tolerance."""
    lower = target_time - SNAPSHOT_TOLERANCE
    upper = target_time + SNAPSHOT_TOLERANCE

    # Fetch candidates within tolerance window, pick the one closest to target
    result = await session.execute(
        select(PriceSnapshot)
        .where(
            PriceSnapshot.outcome_id == outcome_id,
            PriceSnapshot.captured_at >= lower,
            PriceSnapshot.captured_at <= upper,
        )
    )
    candidates = result.scalars().all()
    if not candidates:
        return None

    # Pick closest by absolute time difference (works across all DB backends)
    return min(candidates, key=lambda s: abs((s.captured_at - target_time).total_seconds())
               if s.captured_at.tzinfo is not None or target_time.tzinfo is None
               else abs((s.captured_at.replace(tzinfo=timezone.utc) - target_time).total_seconds()))
