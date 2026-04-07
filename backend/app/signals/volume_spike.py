"""Volume Spike detector: fires when current volume far exceeds the rolling baseline."""
import logging
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate

logger = logging.getLogger(__name__)

# Minimum snapshots required before we consider baseline valid
MIN_BASELINE_SNAPSHOTS = 12


class VolumeSpikeDetector(BaseDetector):
    async def detect(self, session: AsyncSession) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        baseline_start = now - timedelta(hours=settings.volume_spike_baseline_hours)
        recent_window = now - timedelta(hours=1)
        multiplier_threshold = Decimal(str(settings.volume_spike_multiplier))

        # Average volume_24h over the baseline window per outcome
        baseline_sub = (
            select(
                PriceSnapshot.outcome_id,
                func.avg(PriceSnapshot.volume_24h).label("avg_vol"),
                func.count(PriceSnapshot.id).label("snap_count"),
            )
            .where(
                PriceSnapshot.captured_at >= baseline_start,
                PriceSnapshot.captured_at < recent_window,
                PriceSnapshot.volume_24h.isnot(None),
            )
            .group_by(PriceSnapshot.outcome_id)
            .subquery()
        )

        # Latest snapshot per outcome (for current volume)
        latest_sub = (
            select(
                PriceSnapshot.outcome_id,
                func.max(PriceSnapshot.captured_at).label("max_time"),
            )
            .where(PriceSnapshot.captured_at >= recent_window)
            .group_by(PriceSnapshot.outcome_id)
            .subquery()
        )

        result = await session.execute(
            select(PriceSnapshot, baseline_sub.c.avg_vol, baseline_sub.c.snap_count)
            .join(
                latest_sub,
                (PriceSnapshot.outcome_id == latest_sub.c.outcome_id)
                & (PriceSnapshot.captured_at == latest_sub.c.max_time),
            )
            .join(baseline_sub, PriceSnapshot.outcome_id == baseline_sub.c.outcome_id)
        )

        candidates: list[SignalCandidate] = []

        for snap, avg_vol, snap_count in result.all():
            if snap_count < MIN_BASELINE_SNAPSHOTS:
                continue
            if avg_vol is None or avg_vol <= 0:
                continue
            if snap.volume_24h is None:
                continue

            avg_vol_dec = Decimal(str(avg_vol))
            current_vol = snap.volume_24h
            multiplier = current_vol / avg_vol_dec

            if multiplier < multiplier_threshold:
                continue

            # Look up market context
            outcome_row = await session.execute(
                select(Outcome, Market)
                .join(Market, Outcome.market_id == Market.id)
                .where(Outcome.id == snap.outcome_id)
            )
            row = outcome_row.first()
            if row is None:
                continue
            outcome, market = row

            # Score: log-scaled multiplier, capped at 1.0
            raw_score = Decimal(str(math.log(float(multiplier), 10))) / Decimal("1.5")
            signal_score = min(Decimal("1.0"), max(Decimal("0.1"), raw_score))

            # Confidence: penalize low baseline volume
            confidence = Decimal("1.0")
            if avg_vol_dec < 1000:
                confidence *= Decimal("0.3")
            elif avg_vol_dec < 5000:
                confidence *= Decimal("0.6")

            candidates.append(SignalCandidate(
                signal_type="volume_spike",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=snap.price,
                details={
                    "current_volume_24h": str(current_vol),
                    "baseline_avg_volume": str(avg_vol_dec.quantize(Decimal("0.01"))),
                    "multiplier": str(multiplier.quantize(Decimal("0.1"))),
                    "baseline_snapshots": snap_count,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                },
            ))

        logger.info("VolumeSpikeDetector: %d candidates", len(candidates))
        return candidates
