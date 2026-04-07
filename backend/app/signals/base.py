from abc import ABC, abstractmethod
from dataclasses import dataclass, field
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


@dataclass
class SnapshotWindow:
    """Pre-loaded snapshot data for backtesting replay mode.

    When provided to a detector, it should use these in-memory lists
    instead of querying the database for recent snapshots.
    """
    price_snapshots: list = field(default_factory=list)
    orderbook_snapshots: list = field(default_factory=list)
    window_start: "datetime | None" = None
    window_end: "datetime | None" = None


class BaseDetector(ABC):
    @abstractmethod
    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        """Scan recent data and return any signals that should fire.

        If snapshot_window is provided (backtesting mode), detectors should use
        the pre-loaded snapshots instead of querying the live database.
        """
        ...
