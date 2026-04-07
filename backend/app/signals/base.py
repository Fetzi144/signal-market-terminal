from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class SignalCandidate:
    """A detected signal before persistence."""
    signal_type: str
    market_id: str  # UUID as string
    outcome_id: str  # UUID as string
    signal_score: Decimal
    confidence: Decimal
    price_at_fire: Decimal | None
    details: dict


class BaseDetector(ABC):
    @abstractmethod
    async def detect(self, session: AsyncSession) -> list[SignalCandidate]:
        """Scan recent data and return any signals that should fire."""
        ...
