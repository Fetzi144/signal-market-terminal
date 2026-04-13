"""Spread Change detector: fires when bid-ask spread widens or narrows significantly vs baseline."""
import logging
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow

logger = logging.getLogger(__name__)

MIN_BASELINE_SNAPSHOTS = 6


class SpreadChangeDetector(BaseDetector):
    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        baseline_start = now - timedelta(hours=settings.spread_change_baseline_hours)
        recent_window = now - timedelta(hours=1)
        threshold = Decimal(str(settings.spread_change_threshold_ratio))

        # Baseline: average spread per outcome over the baseline window (excluding recent)
        baseline_sub = (
            select(
                OrderbookSnapshot.outcome_id,
                func.avg(OrderbookSnapshot.spread).label("avg_spread"),
                func.count(OrderbookSnapshot.id).label("snap_count"),
            )
            .where(
                OrderbookSnapshot.captured_at >= baseline_start,
                OrderbookSnapshot.captured_at < recent_window,
                OrderbookSnapshot.spread.isnot(None),
            )
            .group_by(OrderbookSnapshot.outcome_id)
            .subquery()
        )

        # Latest orderbook snapshot per outcome
        latest_sub = (
            select(
                OrderbookSnapshot.outcome_id,
                func.max(OrderbookSnapshot.captured_at).label("max_time"),
            )
            .where(OrderbookSnapshot.captured_at >= recent_window)
            .group_by(OrderbookSnapshot.outcome_id)
            .subquery()
        )

        result = await session.execute(
            select(OrderbookSnapshot, baseline_sub.c.avg_spread, baseline_sub.c.snap_count)
            .join(
                latest_sub,
                (OrderbookSnapshot.outcome_id == latest_sub.c.outcome_id)
                & (OrderbookSnapshot.captured_at == latest_sub.c.max_time),
            )
            .join(baseline_sub, OrderbookSnapshot.outcome_id == baseline_sub.c.outcome_id)
        )

        candidates: list[SignalCandidate] = []

        for snap, avg_spread, snap_count in result.all():
            if snap_count < MIN_BASELINE_SNAPSHOTS:
                continue
            if avg_spread is None or avg_spread <= 0:
                continue
            if snap.spread is None or snap.spread <= 0:
                continue

            avg_spread_dec = Decimal(str(avg_spread))
            current_spread = snap.spread

            # Check for widening
            if current_spread / avg_spread_dec >= threshold:
                direction = "widening"
                ratio = current_spread / avg_spread_dec
            # Check for narrowing
            elif avg_spread_dec / current_spread >= threshold:
                direction = "narrowing"
                ratio = avg_spread_dec / current_spread
            else:
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

            # Get latest price for this outcome
            price_row = await session.execute(
                select(PriceSnapshot)
                .where(PriceSnapshot.outcome_id == outcome.id)
                .order_by(PriceSnapshot.captured_at.desc())
                .limit(1)
            )
            latest_price_snapshot = price_row.scalar_one_or_none()
            latest_price = latest_price_snapshot.price if latest_price_snapshot is not None else None
            received_at_local = snap.captured_at
            if latest_price_snapshot is not None and latest_price_snapshot.captured_at > received_at_local:
                received_at_local = latest_price_snapshot.captured_at

            # Score: log-scaled ratio, capped at 1.0
            raw_score = Decimal(str(math.log(float(ratio), 2))) / Decimal("2.0")
            signal_score = min(Decimal("1.0"), max(Decimal("0.1"), raw_score))

            # Widening spreads are more actionable than narrowing
            if direction == "widening":
                signal_score = min(Decimal("1.0"), signal_score * Decimal("1.2"))

            # Confidence: penalize thin baseline
            confidence = Decimal("1.0")
            if snap_count < 12:
                confidence *= Decimal("0.6")

            # Probability engine: spread_change is an uncertainty modifier,
            # not a directional signal. probability_adjustment = 0.
            # Widening spread = more uncertainty; narrowing = less uncertainty.
            candidates.append(SignalCandidate(
                signal_type="spread_change",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=latest_price,
                received_at_local=received_at_local,
                source_platform=market.platform,
                source_token_id=outcome.token_id,
                source_event_type="orderbook_snapshot",
                estimated_probability=None,
                probability_adjustment=Decimal("0"),
                is_directional=False,
                details={
                    "direction": direction,
                    "current_spread": str(current_spread),
                    "baseline_avg_spread": str(avg_spread_dec.quantize(Decimal("0.000001"))),
                    "ratio": str(ratio.quantize(Decimal("0.1"))),
                    "baseline_snapshots": snap_count,
                    "uncertainty_modifier": direction,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                },
            ))

        logger.info("SpreadChangeDetector: %d candidates", len(candidates))
        return candidates
