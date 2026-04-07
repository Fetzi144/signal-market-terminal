from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class RawMarket:
    platform: str
    platform_id: str
    slug: str | None
    question: str
    category: str | None
    end_date: str | None  # ISO format or None
    active: bool
    outcomes: list["RawOutcome"]
    volume_24h: Decimal | None
    liquidity: Decimal | None
    metadata: dict


@dataclass
class RawOutcome:
    platform_outcome_id: str
    name: str
    token_id: str | None
    price: Decimal | None


@dataclass
class RawOrderbook:
    token_id: str
    bids: list[list[str]]  # [[price, size], ...]
    asks: list[list[str]]
    spread: Decimal | None


class BaseConnector(ABC):
    @abstractmethod
    async def fetch_markets(self, limit: int = 100, offset: int = 0) -> list[RawMarket]:
        ...

    @abstractmethod
    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        ...

    @abstractmethod
    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        ...
