"""Smart Money detector: fires when a tracked whale wallet makes a significant trade."""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.whale import WalletActivity, WalletProfile
from app.signals.base import BaseDetector, SignalCandidate, SnapshotWindow

logger = logging.getLogger(__name__)


class SmartMoneyDetector(BaseDetector):
    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        if not settings.whale_tracking_enabled:
            return []

        min_trade = Decimal(str(settings.whale_signal_min_trade_usd))
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=15)

        # Find recent activities from tracked wallets above minimum trade size
        result = await session.execute(
            select(WalletActivity, WalletProfile)
            .join(WalletProfile, WalletActivity.wallet_id == WalletProfile.id)
            .where(
                WalletProfile.tracked.is_(True),
                WalletActivity.notional_usd >= min_trade,
                WalletActivity.timestamp >= cutoff,
                WalletActivity.outcome_id.isnot(None),
            )
            .order_by(WalletActivity.timestamp.desc())
        )
        rows = result.all()

        if not rows:
            logger.debug("SmartMoneyDetector: no qualifying whale trades")
            return []

        candidates: list[SignalCandidate] = []

        for activity, wallet in rows:
            # Look up the outcome and market
            outcome_result = await session.execute(
                select(Outcome).where(Outcome.id == activity.outcome_id)
            )
            outcome = outcome_result.scalar_one_or_none()
            if outcome is None:
                continue

            market_result = await session.execute(
                select(Market).where(Market.id == outcome.market_id)
            )
            market = market_result.scalar_one_or_none()
            if market is None or not market.active:
                continue

            # Signal score based on wallet's historical win rate
            if wallet.win_rate is not None:
                signal_score = min(Decimal("1.0"), wallet.win_rate * Decimal("1.5"))
            else:
                signal_score = Decimal("0.500")

            # Confidence based on position size relative to trade threshold
            size_ratio = activity.notional_usd / min_trade
            confidence = min(Decimal("1.0"), Decimal("0.5") + size_ratio * Decimal("0.1"))

            direction = "up" if activity.action == "buy" else "down"

            candidates.append(SignalCandidate(
                signal_type="smart_money",
                market_id=str(market.id),
                outcome_id=str(outcome.id),
                signal_score=signal_score.quantize(Decimal("0.001")),
                confidence=confidence.quantize(Decimal("0.001")),
                price_at_fire=activity.price,
                details={
                    "direction": direction,
                    "market_question": market.question,
                    "outcome_name": outcome.name,
                    "wallet_address": wallet.address,
                    "wallet_label": wallet.label or "",
                    "action": activity.action,
                    "quantity": str(activity.quantity),
                    "notional_usd": str(activity.notional_usd),
                    "price": str(activity.price) if activity.price else None,
                    "wallet_win_rate": str(wallet.win_rate) if wallet.win_rate else None,
                    "wallet_total_volume": str(wallet.total_volume),
                    "wallet_trade_count": wallet.trade_count,
                    "tx_hash": activity.tx_hash,
                },
            ))

        logger.info("SmartMoneyDetector: %d candidates", len(candidates))
        return candidates
