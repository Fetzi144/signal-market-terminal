from abc import ABC, abstractmethod

from app.models.signal import Signal


class BaseAlerter(ABC):
    @abstractmethod
    async def send(self, signal: Signal, market_question: str) -> None:
        ...
