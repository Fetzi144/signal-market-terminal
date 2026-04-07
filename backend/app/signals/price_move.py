"""Price Move detector: fires when an outcome's price moves significantly in a short window."""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate

logger = logging.getLogger(__name__)


class PriceMoveDetector(BaseDetector):
    async def detect(self, session: AsyncSession) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(minutes=settings.price_move_window_minutes)
        threshold = Decimal(str(settings.price_move_threshold_pct)) / 100

        # Get the latest snapshot for each active outcome
        latest_sub = (
            select(
                PriceSnapshot.outcome_id,
                func.max(PriceSnapshot.captured_at).label("max_time"),
            )
            .where(PriceSnapshot.captured_at >= window_start)
            .group_by(PriceSnapshot.outcome_id)
            .subquery()
        )

        # Get latest price per outcome
        latest_result = await session.execute(
            select(PriceSnapshot)
            .join(
                latest_sub,
                (PriceSnapshot.outcome_id == latest_sub.c.outcome_id)
                & (PriceSnapshot.captured_at == latest_sub.c.max_time),
            )
        )
        latest_snaps = {s.outcome_id: s for s in latest_result.scalars().all()}

        # Get earliest snapshot in window for each outcome
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

            candidates.append(SignalCandidate(
                signal_type="price_move",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=new_price,
                details={
                    "direction": direction,
                    "old_price": str(old_price),
                    "new_price": str(new_price),
                    "change_pct": str((change_pct * 100).quantize(Decimal("0.01"))),
                    "window_minutes": settings.price_move_window_minutes,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                },
            ))

        logger.info("PriceMoveDetector: %d candidates from %d outcomes", len(candidates), len(latest_snaps))
        return candidates
