"""Connector registry — maps platform names to connector instances."""
from app.connectors.base import BaseConnector


def get_connector(platform: str) -> BaseConnector:
    """Return a connector instance for the given platform."""
    if platform == "polymarket":
        from app.connectors.polymarket import PolymarketConnector
        return PolymarketConnector()
    elif platform == "kalshi":
        from app.connectors.kalshi import KalshiConnector
        return KalshiConnector()
    else:
        raise ValueError(f"Unknown platform: {platform}")


def get_enabled_platforms() -> list[str]:
    """Return list of enabled platform names based on config."""
    from app.config import settings

    platforms: list[str] = []
    if settings.kalshi_enabled:
        platforms.append("kalshi")
    return platforms
