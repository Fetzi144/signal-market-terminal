"""Deadline Near detector: fires when markets approaching their end date show price movement."""
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


class DeadlineNearDetector(BaseDetector):
    async def detect(self, session: AsyncSession) -> list[SignalCandidate]:
        now = datetime.now(timezone.utc)
        deadline_cutoff = now + timedelta(hours=settings.deadline_near_hours)
        threshold = Decimal(str(settings.deadline_near_price_threshold_pct)) / 100
        # Use a 2-hour price window for deadline-near moves
        window_start = now - timedelta(hours=2)

        # Find active markets nearing their end date
        market_result = await session.execute(
            select(Market)
            .where(
                Market.active.is_(True),
                Market.end_date.isnot(None),
                Market.end_date > now,
                Market.end_date <= deadline_cutoff,
            )
        )
        near_deadline_markets = market_result.scalars().all()

        if not near_deadline_markets:
            logger.info("DeadlineNearDetector: no markets near deadline")
            return []

        candidates: list[SignalCandidate] = []

        for market in near_deadline_markets:
            # Get outcomes
            outcome_result = await session.execute(
                select(Outcome).where(Outcome.market_id == market.id)
            )
            outcomes = outcome_result.scalars().all()

            for outcome in outcomes:
                # Get latest and earliest price in window
                latest_result = await session.execute(
                    select(PriceSnapshot)
                    .where(
                        PriceSnapshot.outcome_id == outcome.id,
                        PriceSnapshot.captured_at >= window_start,
                    )
                    .order_by(PriceSnapshot.captured_at.desc())
                    .limit(1)
                )
                latest = latest_result.scalar_one_or_none()

                earliest_result = await session.execute(
                    select(PriceSnapshot)
                    .where(
                        PriceSnapshot.outcome_id == outcome.id,
                        PriceSnapshot.captured_at >= window_start,
                    )
                    .order_by(PriceSnapshot.captured_at.asc())
                    .limit(1)
                )
                earliest = earliest_result.scalar_one_or_none()

                if latest is None or earliest is None:
                    continue
                if latest.captured_at == earliest.captured_at:
                    continue

                old_price = earliest.price
                new_price = latest.price

                if old_price < Decimal("0.01"):
                    continue

                change_pct = abs(new_price - old_price) / old_price

                if change_pct < threshold:
                    continue

                # Hours until deadline — closer = higher urgency multiplier
                hours_remaining = (market.end_date - now).total_seconds() / 3600
                urgency = Decimal(str(max(0.5, 1.0 - (hours_remaining / settings.deadline_near_hours))))

                signal_score = min(Decimal("1.0"), (change_pct / Decimal("0.2")) * urgency)

                # Confidence: penalize thin markets
                confidence = Decimal("1.0")
                if latest.volume_24h is not None and latest.volume_24h < 10000:
                    confidence *= Decimal("0.5")
                if latest.liquidity is not None and latest.liquidity < 5000:
                    confidence *= Decimal("0.5")

                direction = "up" if new_price > old_price else "down"

                candidates.append(SignalCandidate(
                    signal_type="deadline_near",
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
                        "hours_until_deadline": round(hours_remaining, 1),
                        "end_date": market.end_date.isoformat(),
                        "urgency": str(urgency.quantize(Decimal("0.01"))),
                        "market_question": market.question,
                        "outcome_name": outcome.name,
                    },
                ))

        logger.info("DeadlineNearDetector: %d candidates from %d markets", len(candidates), len(near_deadline_markets))
        return candidates
