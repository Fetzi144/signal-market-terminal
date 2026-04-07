import logging
from decimal import Decimal, InvalidOperation

import httpx

from app.config import settings
from app.connectors.base import BaseConnector, RawMarket, RawOrderbook, RawOutcome

logger = logging.getLogger(__name__)

# Max token IDs per batch request
BATCH_SIZE = 200


class PolymarketConnector(BaseConnector):
    def __init__(self):
        self.gamma_base = settings.polymarket_gamma_base
        self.clob_base = settings.polymarket_api_base
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def fetch_markets(self, limit: int = 100, offset: int = 0) -> list[RawMarket]:
        """Fetch active events from Gamma API, flatten to markets."""
        client = await self._get_client()
        resp = await client.get(
            f"{self.gamma_base}/events",
            params={
                "active": "true",
                "closed": "false",
                "order": "volume_24hr",
                "ascending": "false",
                "limit": limit,
                "offset": offset,
            },
        )
        resp.raise_for_status()
        events = resp.json()

        raw_markets: list[RawMarket] = []
        for event in events:
            for mkt in event.get("markets", []):
                try:
                    raw_markets.append(self._parse_market(mkt))
                except Exception:
                    logger.warning("Failed to parse market %s", mkt.get("id"), exc_info=True)
        return raw_markets

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        """Batch fetch midpoints from CLOB API."""
        client = await self._get_client()
        results: dict[str, Decimal] = {}

        for i in range(0, len(token_ids), BATCH_SIZE):
            batch = token_ids[i : i + BATCH_SIZE]
            ids_param = ",".join(batch)
            resp = await client.get(f"{self.clob_base}/midpoints", params={"token_ids": ids_param})
            resp.raise_for_status()
            data = resp.json()
            for tid, mid in data.items():
                try:
                    results[tid] = Decimal(str(mid))
                except (InvalidOperation, TypeError):
                    logger.warning("Invalid midpoint for token %s: %s", tid, mid)
        return results

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        """Fetch L2 order book for a single token."""
        client = await self._get_client()
        resp = await client.get(f"{self.clob_base}/book", params={"token_id": token_id})
        resp.raise_for_status()
        data = resp.json()

        bids = [[b["price"], b["size"]] for b in data.get("bids", [])]
        asks = [[a["price"], a["size"]] for a in data.get("asks", [])]

        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        return RawOrderbook(token_id=token_id, bids=bids, asks=asks, spread=spread)

    def _parse_market(self, mkt: dict) -> RawMarket:
        outcomes_names = mkt.get("outcomes", [])
        outcome_prices = mkt.get("outcomePrices", [])
        clob_token_ids = mkt.get("clobTokenIds", [])

        raw_outcomes: list[RawOutcome] = []
        for idx, name in enumerate(outcomes_names):
            price = None
            if idx < len(outcome_prices):
                try:
                    price = Decimal(str(outcome_prices[idx]))
                except (InvalidOperation, TypeError):
                    pass

            token_id = clob_token_ids[idx] if idx < len(clob_token_ids) else None

            raw_outcomes.append(
                RawOutcome(
                    platform_outcome_id=f"{mkt['id']}_{idx}",
                    name=name if isinstance(name, str) else str(name),
                    token_id=token_id,
                    price=price,
                )
            )

        vol_24h = None
        for key in ("volume24hr", "volume24hrClob"):
            val = mkt.get(key)
            if val is not None:
                try:
                    vol_24h = Decimal(str(val))
                    break
                except (InvalidOperation, TypeError):
                    pass

        liquidity = None
        for key in ("liquidityNum", "liquidityClob"):
            val = mkt.get(key)
            if val is not None:
                try:
                    liquidity = Decimal(str(val))
                    break
                except (InvalidOperation, TypeError):
                    pass

        return RawMarket(
            platform="polymarket",
            platform_id=str(mkt["id"]),
            slug=mkt.get("slug"),
            question=mkt.get("question", ""),
            category=mkt.get("groupItemTitle") or mkt.get("category"),
            end_date=mkt.get("endDate") or mkt.get("end_date_iso"),
            active=mkt.get("active", True),
            outcomes=raw_outcomes,
            volume_24h=vol_24h,
            liquidity=liquidity,
            metadata={
                "condition_id": mkt.get("conditionId"),
                "event_slug": mkt.get("eventSlug"),
                "spread": mkt.get("spread"),
                "one_day_price_change": mkt.get("oneDayPriceChange"),
            },
        )
