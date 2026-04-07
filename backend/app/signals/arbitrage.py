"""Arbitrage detector: fires when the same market trades at different prices across platforms."""
import logging
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow

logger = logging.getLogger(__name__)


class ArbitrageDetector(BaseDetector):
    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        if not settings.arb_enabled:
            return []

        threshold = Decimal(str(settings.arb_spread_threshold))

        # Find question_slugs that appear on more than one platform
        cross_platform = (
            select(Market.question_slug)
            .where(Market.active.is_(True), Market.question_slug.isnot(None))
            .group_by(Market.question_slug)
            .having(func.count(func.distinct(Market.platform)) > 1)
            .subquery()
        )

        # Get all active markets that are cross-platform
        markets_result = await session.execute(
            select(Market)
            .where(
                Market.active.is_(True),
                Market.question_slug.in_(select(cross_platform.c.question_slug)),
            )
            .order_by(Market.question_slug)
        )
        markets = markets_result.scalars().all()

        if not markets:
            logger.info("ArbitrageDetector: no cross-platform markets found")
            return []

        # Group markets by question_slug
        slug_groups: dict[str, list[Market]] = {}
        for m in markets:
            slug_groups.setdefault(m.question_slug, []).append(m)

        # For each group, get latest YES outcome price per platform
        candidates: list[SignalCandidate] = []

        for q_slug, group_markets in slug_groups.items():
            # Need at least 2 different platforms
            platforms = {m.platform for m in group_markets}
            if len(platforms) < 2:
                continue

            # Get latest price for the YES outcome of each market
            platform_prices: dict[str, tuple[Decimal, Market, Outcome]] = {}

            for market in group_markets:
                # Get YES outcome (or first outcome)
                outcome_result = await session.execute(
                    select(Outcome).where(Outcome.market_id == market.id)
                )
                outcomes = outcome_result.scalars().all()
                yes_outcome = None
                for o in outcomes:
                    if o.name.lower() in ("yes", "yes "):
                        yes_outcome = o
                        break
                if yes_outcome is None and outcomes:
                    yes_outcome = outcomes[0]
                if yes_outcome is None:
                    continue

                # Get latest price snapshot
                snap_result = await session.execute(
                    select(PriceSnapshot)
                    .where(PriceSnapshot.outcome_id == yes_outcome.id)
                    .order_by(PriceSnapshot.captured_at.desc())
                    .limit(1)
                )
                snap = snap_result.scalar_one_or_none()
                if snap is None:
                    continue

                platform_prices[market.platform] = (snap.price, market, yes_outcome)

            # Compare all platform pairs
            platform_list = list(platform_prices.keys())
            for i in range(len(platform_list)):
                for j in range(i + 1, len(platform_list)):
                    p1, p2 = platform_list[i], platform_list[j]
                    price1, market1, outcome1 = platform_prices[p1]
                    price2, market2, outcome2 = platform_prices[p2]

                    spread = abs(price1 - price2)
                    if spread < threshold:
                        continue

                    # Score: spread / 0.15 capped at 1.0
                    signal_score = min(Decimal("1.0"), spread / Decimal("0.15"))

                    # Direction: "up" on the cheaper platform's outcome (the one to buy)
                    if price1 < price2:
                        cheap_market, cheap_outcome, cheap_price = market1, outcome1, price1
                        expensive_market, expensive_price = market2, price2
                    else:
                        cheap_market, cheap_outcome, cheap_price = market2, outcome2, price2
                        expensive_market, expensive_price = market1, price1

                    candidates.append(SignalCandidate(
                        signal_type="arbitrage",
                        market_id=str(cheap_market.id),
                        outcome_id=str(cheap_outcome.id),
                        signal_score=signal_score.quantize(Decimal("0.001")),
                        confidence=Decimal("1.000"),
                        price_at_fire=cheap_price,
                        details={
                            "direction": "up",
                            "question_slug": q_slug,
                            "market_question": cheap_market.question,
                            "outcome_name": cheap_outcome.name,
                            f"{cheap_market.platform}_price": str(cheap_price),
                            f"{expensive_market.platform}_price": str(expensive_price),
                            "spread": str(spread.quantize(Decimal("0.0001"))),
                            "spread_pct": str((spread * 100).quantize(Decimal("0.01"))),
                            "buy_platform": cheap_market.platform,
                            "sell_platform": expensive_market.platform,
                        },
                    ))

        logger.info("ArbitrageDetector: %d candidates", len(candidates))
        return candidates
