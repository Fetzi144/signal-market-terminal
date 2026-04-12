"""Price Move detector: fires when an outcome's price moves significantly in a short window.

Supports multi-timeframe analysis: runs detection across each configured
timeframe (e.g. 30m, 1h, 4h) and tags signals with their timeframe.
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow, timeframe_to_minutes
from app.signals.probability import compute_estimated_probability

logger = logging.getLogger(__name__)


class PriceMoveDetector(BaseDetector):
    def __init__(
        self,
        *,
        threshold_pct: float | None = None,
        window_minutes: int | None = None,
        timeframes: list[str] | None = None,
    ):
        tf = timeframes or settings.price_move_timeframes.split(",")
        super().__init__(timeframes=tf)
        self._threshold_pct = threshold_pct
        self._window_minutes = window_minutes

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
        # Use explicit window_minutes override, otherwise derive from timeframe
        if self._window_minutes is not None:
            window_minutes = self._window_minutes
        else:
            window_minutes = timeframe_to_minutes(timeframe)
        window_start = now - timedelta(minutes=window_minutes)
        raw_threshold = self._threshold_pct if self._threshold_pct is not None else settings.price_move_threshold_pct
        threshold = Decimal(str(raw_threshold)) / 100

        if snapshot_window is not None:
            latest_snaps, earliest_snaps = _snapshots_from_window(
                snapshot_window.price_snapshots, window_start
            )
        else:
            latest_snaps, earliest_snaps = await _snapshots_from_db(session, window_start)

        candidates: list[SignalCandidate] = []

        for outcome_id, latest in latest_snaps.items():
            earliest = earliest_snaps.get(outcome_id)
            if earliest is None:
                continue

            old_price = earliest.price
            new_price = latest.price

            # Avoid division by near-zero
            if old_price < Decimal("0.01"):
                continue

            change_pct = abs(new_price - old_price) / old_price

            if change_pct < threshold:
                continue

            # Need at least 2 distinct snapshots
            if latest.captured_at == earliest.captured_at:
                continue

            # Look up outcome -> market for context
            outcome_row = await session.execute(
                select(Outcome, Market)
                .join(Market, Outcome.market_id == Market.id)
                .where(Outcome.id == outcome_id)
            )
            row = outcome_row.first()
            if row is None:
                continue
            outcome, market = row

            # Compute signal strength (0-1 scale, capped)
            signal_score = min(Decimal("1.0"), change_pct / Decimal("0.3"))

            # Confidence: penalize thin markets
            confidence = Decimal("1.0")
            if latest.volume_24h is not None and latest.volume_24h < 10000:
                confidence *= Decimal("0.5")
            if latest.liquidity is not None and latest.liquidity < 5000:
                confidence *= Decimal("0.5")

            direction = "up" if new_price > old_price else "down"

            # Probability engine: the observed price move suggests the market
            # is adjusting. Our signal says this move reflects real information.
            # raw_adjustment = signed change as fraction of price space.
            # Calibration factor starts at 1.0 (will be tuned from CLV data).
            direction_sign = Decimal("1") if direction == "up" else Decimal("-1")
            calibration_factor = Decimal("1.0")
            raw_adjustment = direction_sign * change_pct * calibration_factor
            est_prob, adj_applied = compute_estimated_probability(new_price, raw_adjustment)

            candidates.append(SignalCandidate(
                signal_type="price_move",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=new_price,
                timeframe=timeframe,
                estimated_probability=est_prob,
                probability_adjustment=adj_applied,
                details={
                    "direction": direction,
                    "old_price": str(old_price),
                    "new_price": str(new_price),
                    "change_pct": str((change_pct * 100).quantize(Decimal("0.01"))),
                    "window_minutes": window_minutes,
                    "timeframe": timeframe,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                    "raw_probability_adjustment": str(raw_adjustment.quantize(Decimal("0.0001"))),
                    "calibration_factor": str(calibration_factor),
                },
            ))

        logger.info("PriceMoveDetector[%s]: %d candidates from %d outcomes", timeframe, len(candidates), len(latest_snaps))
        return candidates


def _snapshots_from_window(
    price_snapshots: list, window_start: datetime
) -> tuple[dict, dict]:
    """Extract latest/earliest snapshots per outcome from an in-memory list."""
    from collections import defaultdict

    by_outcome: dict[object, list] = defaultdict(list)
    for snap in price_snapshots:
        if snap.captured_at >= window_start:
            by_outcome[snap.outcome_id].append(snap)

    latest_snaps = {}
    earliest_snaps = {}
    for outcome_id, snaps in by_outcome.items():
        sorted_snaps = sorted(snaps, key=lambda s: s.captured_at)
        earliest_snaps[outcome_id] = sorted_snaps[0]
        latest_snaps[outcome_id] = sorted_snaps[-1]

    return latest_snaps, earliest_snaps


async def _snapshots_from_db(session: AsyncSession, window_start: datetime) -> tuple[dict, dict]:
    """Load latest/earliest snapshots per outcome from the database."""
    latest_sub = (
        select(
            PriceSnapshot.outcome_id,
            func.max(PriceSnapshot.captured_at).label("max_time"),
        )
        .where(PriceSnapshot.captured_at >= window_start)
        .group_by(PriceSnapshot.outcome_id)
        .subquery()
    )

    latest_result = await session.execute(
        select(PriceSnapshot)
        .join(
            latest_sub,
            (PriceSnapshot.outcome_id == latest_sub.c.outcome_id)
            & (PriceSnapshot.captured_at == latest_sub.c.max_time),
        )
    )
    latest_snaps = {s.outcome_id: s for s in latest_result.scalars().all()}

    earliest_sub = (
        select(
            PriceSnapshot.outcome_id,
            func.min(PriceSnapshot.captured_at).label("min_time"),
        )
        .where(PriceSnapshot.captured_at >= window_start)
        .group_by(PriceSnapshot.outcome_id)
        .subquery()
    )

    earliest_result = await session.execute(
        select(PriceSnapshot)
        .join(
            earliest_sub,
            (PriceSnapshot.outcome_id == earliest_sub.c.outcome_id)
            & (PriceSnapshot.captured_at == earliest_sub.c.min_time),
        )
    )
    earliest_snaps = {s.outcome_id: s for s in earliest_result.scalars().all()}

    return latest_snaps, earliest_snaps
