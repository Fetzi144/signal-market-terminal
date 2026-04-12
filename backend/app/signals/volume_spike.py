"""Volume Spike detector: fires when current volume far exceeds the rolling baseline.

Supports multi-timeframe analysis: runs detection across each configured
timeframe (e.g. 1h, 4h, 24h baseline windows) and tags signals with their timeframe.
"""
import logging
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow
from app.signals.probability import compute_estimated_probability

logger = logging.getLogger(__name__)

# Minimum snapshots required before we consider baseline valid
MIN_BASELINE_SNAPSHOTS = 12


class VolumeSpikeDetector(BaseDetector):
    def __init__(
        self,
        *,
        multiplier: float | None = None,
        baseline_hours: int | None = None,
        timeframes: list[str] | None = None,
    ):
        tf = timeframes or settings.volume_spike_timeframes.split(",")
        super().__init__(timeframes=tf)
        self._multiplier = multiplier
        self._baseline_hours = baseline_hours

    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        all_candidates: list[SignalCandidate] = []
        for tf in self.timeframes:
            candidates = await self._detect_timeframe(session, tf, snapshot_window=snapshot_window)
            all_candidates.extend(candidates)
        return all_candidates

    async def _detect_timeframe(
        self,
        session: AsyncSession,
        timeframe: str,
        *,
        snapshot_window: SnapshotWindow | None = None,
    ) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        # Use explicit baseline_hours override, otherwise use settings default
        # The timeframe label identifies the analysis window for multi-TF grouping,
        # but the actual baseline period comes from config.
        if self._baseline_hours is not None:
            baseline_hours = self._baseline_hours
        else:
            baseline_hours = settings.volume_spike_baseline_hours
        baseline_start = now - timedelta(hours=baseline_hours)
        recent_window = now - timedelta(hours=1)
        raw_multiplier = self._multiplier if self._multiplier is not None else settings.volume_spike_multiplier
        multiplier_threshold = Decimal(str(raw_multiplier))

        if snapshot_window is not None:
            rows = _volume_from_window(snapshot_window.price_snapshots, baseline_start, recent_window)
        else:
            rows = await _volume_from_db(session, baseline_start, recent_window)

        candidates: list[SignalCandidate] = []

        for snap, avg_vol, snap_count in rows:
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

            # Probability engine: volume confirms the current price trend.
            # Determine trend from recent price movement relative to baseline.
            # Get a baseline price from the recent window for trend direction.
            baseline_price_result = await session.execute(
                select(PriceSnapshot.price)
                .where(
                    PriceSnapshot.outcome_id == snap.outcome_id,
                    PriceSnapshot.captured_at >= recent_window - timedelta(hours=1),
                    PriceSnapshot.captured_at < recent_window,
                )
                .order_by(PriceSnapshot.captured_at.asc())
                .limit(1)
            )
            baseline_price = baseline_price_result.scalar_one_or_none()

            est_prob = None
            adj_applied = None
            trend_direction = "neutral"
            if baseline_price is not None and snap.price is not None:
                price_delta = snap.price - baseline_price
                if abs(price_delta) > Decimal("0.005"):
                    # Volume spike in direction of price move = confirmation
                    direction_sign = Decimal("1") if price_delta > 0 else Decimal("-1")
                    trend_direction = "up" if price_delta > 0 else "down"
                    # Adjustment scales with log of volume ratio, capped reasonably
                    volume_factor = min(Decimal("0.15"), (multiplier - Decimal("1")) * Decimal("0.03"))
                    calibration_factor = Decimal("1.0")
                    raw_adjustment = direction_sign * volume_factor * calibration_factor
                    est_prob, adj_applied = compute_estimated_probability(snap.price, raw_adjustment)

            candidates.append(SignalCandidate(
                signal_type="volume_spike",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=snap.price,
                timeframe=timeframe,
                estimated_probability=est_prob,
                probability_adjustment=adj_applied,
                details={
                    "current_volume_24h": str(current_vol),
                    "baseline_avg_volume": str(avg_vol_dec.quantize(Decimal("0.01"))),
                    "multiplier": str(multiplier.quantize(Decimal("0.1"))),
                    "baseline_snapshots": snap_count,
                    "baseline_hours": baseline_hours,
                    "timeframe": timeframe,
                    "trend_direction": trend_direction,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                },
            ))

        logger.info("VolumeSpikeDetector[%s]: %d candidates", timeframe, len(candidates))
        return candidates


def _volume_from_window(
    price_snapshots: list, baseline_start: datetime, recent_window: datetime
) -> list[tuple]:
    """Compute baseline avg volume and latest snap from in-memory snapshots."""
    from collections import defaultdict
    from statistics import mean

    by_outcome: dict[object, list] = defaultdict(list)
    for snap in price_snapshots:
        by_outcome[snap.outcome_id].append(snap)

    rows = []
    for outcome_id, snaps in by_outcome.items():
        baseline = [
            s for s in snaps
            if s.captured_at >= baseline_start and s.captured_at < recent_window
            and s.volume_24h is not None
        ]
        recent = [s for s in snaps if s.captured_at >= recent_window]

        if not recent or len(baseline) < MIN_BASELINE_SNAPSHOTS:
            continue

        latest = max(recent, key=lambda s: s.captured_at)
        avg_vol = mean(float(s.volume_24h) for s in baseline)
        rows.append((latest, avg_vol, len(baseline)))

    return rows


async def _volume_from_db(session: AsyncSession, baseline_start: datetime, recent_window: datetime) -> list[tuple]:
    """Load volume baseline + latest snap from the database."""
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
    return result.all()
