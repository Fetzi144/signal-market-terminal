from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession


# Maps timeframe strings to minutes for window calculations
TIMEFRAME_MINUTES = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "24h": 1440,
}


def timeframe_to_minutes(tf: str) -> int:
    """Convert a timeframe string like '30m' or '4h' to minutes."""
    if tf in TIMEFRAME_MINUTES:
        return TIMEFRAME_MINUTES[tf]
    raise ValueError(f"Unknown timeframe: {tf}")


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
    timeframe: str = "30m"


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
    """Base class for all signal detectors.

    Detectors that support multi-timeframe analysis should accept a
    `timeframes` parameter and run detection for each configured timeframe.
    """

    def __init__(self, *, timeframes: list[str] | None = None):
        self.timeframes = timeframes or ["30m"]

    @abstractmethod
    async def detect(
        self, session: AsyncSession, *, snapshot_window: SnapshotWindow | None = None
    ) -> list[SignalCandidate]:
        """Scan recent data and return any signals that should fire.

        If snapshot_window is provided (backtesting mode), detectors should use
        the pre-loaded snapshots instead of querying the live database.
        """
        ...
