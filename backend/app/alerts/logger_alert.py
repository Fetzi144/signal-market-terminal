"""Logger-based alerter: writes structured ALERT lines for high-ranking signals."""
import logging

from app.alerts.base import BaseAlerter
from app.models.signal import Signal

logger = logging.getLogger("smt.alerts")


class LoggerAlerter(BaseAlerter):
    async def send(self, signal: Signal, market_question: str) -> None:
        details = signal.details or {}
        direction = details.get("direction", "")
        outcome = details.get("outcome_name", "")

        logger.warning(
            "ALERT | type=%s | rank=%.2f | score=%.2f | conf=%.2f | market=%r | outcome=%s | direction=%s | price=%s",
            signal.signal_type,
            signal.rank_score,
            signal.signal_score,
            signal.confidence,
            market_question[:80],
            outcome,
            direction,
            signal.price_at_fire,
        )
