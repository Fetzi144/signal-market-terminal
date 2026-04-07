"""Liquidity Vacuum detector: fires when orderbook depth drops sharply vs baseline."""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate

logger = logging.getLogger(__name__)

MIN_BASELINE_SNAPSHOTS = 6


class LiquidityVacuumDetector(BaseDetector):
    async def detect(self, session: AsyncSession) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        baseline_start = now - timedelta(hours=settings.liquidity_vacuum_baseline_hours)
        recent_window = now - timedelta(hours=1)
        depth_threshold = Decimal(str(settings.liquidity_vacuum_depth_ratio_threshold))

        # Baseline average depth per outcome
        baseline_sub = (
            select(
                OrderbookSnapshot.outcome_id,
                func.avg(OrderbookSnapshot.depth_bid_10pct).label("avg_bid_depth"),
                func.avg(OrderbookSnapshot.depth_ask_10pct).label("avg_ask_depth"),
                func.count(OrderbookSnapshot.id).label("snap_count"),
            )
            .where(
                OrderbookSnapshot.captured_at >= baseline_start,
                OrderbookSnapshot.captured_at < recent_window,
                OrderbookSnapshot.depth_bid_10pct.isnot(None),
            )
            .group_by(OrderbookSnapshot.outcome_id)
            .subquery()
        )

        # Latest orderbook per outcome
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
            select(
                OrderbookSnapshot,
                baseline_sub.c.avg_bid_depth,
                baseline_sub.c.avg_ask_depth,
                baseline_sub.c.snap_count,
            )
            .join(
                latest_sub,
                (OrderbookSnapshot.outcome_id == latest_sub.c.outcome_id)
                & (OrderbookSnapshot.captured_at == latest_sub.c.max_time),
            )
            .join(baseline_sub, OrderbookSnapshot.outcome_id == baseline_sub.c.outcome_id)
        )

        candidates: list[SignalCandidate] = []

        for snap, avg_bid, avg_ask, snap_count in result.all():
            if snap_count < MIN_BASELINE_SNAPSHOTS:
                continue

            # Check if both bid and ask depths have data
            current_bid = snap.depth_bid_10pct
            current_ask = snap.depth_ask_10pct
            if current_bid is None or current_ask is None:
                continue
            if avg_bid is None or avg_ask is None or avg_bid <= 0 or avg_ask <= 0:
                continue

            avg_bid_dec = Decimal(str(avg_bid))
            avg_ask_dec = Decimal(str(avg_ask))

            bid_ratio = current_bid / avg_bid_dec
            ask_ratio = current_ask / avg_ask_dec

            # Fire if either side dropped below threshold
            bid_vacuum = bid_ratio < depth_threshold
            ask_vacuum = ask_ratio < depth_threshold

            if not bid_vacuum and not ask_vacuum:
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

            # Get latest price
            price_row = await session.execute(
                select(PriceSnapshot.price)
                .where(PriceSnapshot.outcome_id == outcome.id)
                .order_by(PriceSnapshot.captured_at.desc())
                .limit(1)
            )
            latest_price = price_row.scalar_one_or_none()

            # Score higher when both sides are thin
            both_sides = bid_vacuum and ask_vacuum
            worst_ratio = min(bid_ratio, ask_ratio)
            signal_score = min(Decimal("1.0"), (Decimal("1.0") - worst_ratio) * Decimal("1.5"))
            if both_sides:
                signal_score = min(Decimal("1.0"), signal_score * Decimal("1.3"))

            confidence = Decimal("1.0")
            if snap_count < 12:
                confidence *= Decimal("0.6")

            vacuum_side = "both" if both_sides else ("bid" if bid_vacuum else "ask")

            candidates.append(SignalCandidate(
                signal_type="liquidity_vacuum",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=latest_price,
                details={
                    "vacuum_side": vacuum_side,
                    "bid_depth_ratio": str(bid_ratio.quantize(Decimal("0.01"))),
                    "ask_depth_ratio": str(ask_ratio.quantize(Decimal("0.01"))),
                    "current_bid_depth": str(current_bid),
                    "current_ask_depth": str(current_ask),
                    "baseline_avg_bid": str(avg_bid_dec.quantize(Decimal("0.01"))),
                    "baseline_avg_ask": str(avg_ask_dec.quantize(Decimal("0.01"))),
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                },
            ))

        logger.info("LiquidityVacuumDetector: %d candidates", len(candidates))
        return candidates
