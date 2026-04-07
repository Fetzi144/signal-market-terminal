import asyncio
import logging
from decimal import Decimal, InvalidOperation

import httpx

from app.config import settings
from app.connectors.base import BaseConnector, RawMarket, RawOrderbook, RawOutcome
from app.connectors.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

# Max token IDs per batch request (kept small — Polymarket token IDs are ~76 chars each)
BATCH_SIZE = 50

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = [1.0, 2.0, 4.0]  # seconds


class PolymarketConnector(BaseConnector):
    def __init__(self):
        self.gamma_base = settings.polymarket_gamma_base
        self.clob_base = settings.polymarket_api_base
        self._client: httpx.AsyncClient | None = None
        self.circuit_breaker = CircuitBreaker("polymarket")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=settings.connector_timeout_seconds,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with exponential backoff retry on transient failures."""
        self.circuit_breaker.check()
        client = await self._get_client()
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                resp = await getattr(client, method)(url, **kwargs)

                # Rate limited — back off and retry
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("retry-after", RETRY_BACKOFF[attempt]))
                    logger.warning(
                        "Rate limited (429) on %s, backing off %.1fs (attempt %d/%d)",
                        url, retry_after, attempt + 1, MAX_RETRIES,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                # Server errors — retry
                if resp.status_code >= 500:
                    logger.warning(
                        "Server error %d on %s (attempt %d/%d)",
                        resp.status_code, url, attempt + 1, MAX_RETRIES,
                    )
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_BACKOFF[attempt])
                        continue

                resp.raise_for_status()
                self.circuit_breaker.record_success()
                return resp

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as e:
                last_exc = e
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        "Connection error on %s: %s (attempt %d/%d, retrying in %.1fs)",
                        url, type(e).__name__, attempt + 1, MAX_RETRIES, RETRY_BACKOFF[attempt],
                    )
                    await asyncio.sleep(RETRY_BACKOFF[attempt])
                else:
                    logger.error("All %d retries exhausted for %s: %s", MAX_RETRIES, url, e)

        self.circuit_breaker.record_failure()
        if last_exc:
            raise last_exc
        raise httpx.HTTPError(f"Failed after {MAX_RETRIES} retries: {url}")

    async def fetch_markets(self, limit: int = 100, offset: int = 0) -> list[RawMarket]:
        """Fetch active markets from Gamma API."""
        resp = await self._request_with_retry(
            "get",
            f"{self.gamma_base}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            },
        )
        markets_data = resp.json()

        raw_markets: list[RawMarket] = []
        for mkt in markets_data:
            try:
                raw_markets.append(self._parse_market(mkt))
            except Exception:
                logger.warning("Failed to parse market %s", mkt.get("id"), exc_info=True)
        return raw_markets

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        """Batch fetch midpoints from CLOB API."""
        results: dict[str, Decimal] = {}

        for i in range(0, len(token_ids), BATCH_SIZE):
            batch = token_ids[i : i + BATCH_SIZE]
            ids_param = ",".join(batch)
            try:
                resp = await self._request_with_retry(
                    "get", f"{self.clob_base}/midpoints", params={"token_ids": ids_param},
                )
                data = resp.json()
                for tid, mid in data.items():
                    try:
                        results[tid] = Decimal(str(mid))
                    except (InvalidOperation, TypeError):
                        logger.warning("Invalid midpoint for token %s: %s", tid, mid)
            except (httpx.HTTPError, httpx.HTTPStatusError) as e:
                logger.warning("Failed to fetch midpoints batch %d: %s", i // BATCH_SIZE, e)
        return results

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        """Fetch L2 order book for a single token."""
        resp = await self._request_with_retry(
            "get", f"{self.clob_base}/book", params={"token_id": token_id},
        )
        data = resp.json()

        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        bids = [[b["price"], b["size"]] for b in raw_bids]
        asks = [[a["price"], a["size"]] for a in raw_asks]

        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        return RawOrderbook(token_id=token_id, bids=bids, asks=asks, spread=spread)

    def _parse_market(self, mkt: dict) -> RawMarket:
        import json as _json

        def _parse_json_list(val):
            """Gamma API returns some fields as JSON-encoded strings."""
            if isinstance(val, str):
                try:
                    return _json.loads(val)
                except (ValueError, TypeError):
                    return []
            return val if isinstance(val, list) else []

        outcomes_names = _parse_json_list(mkt.get("outcomes", []))
        outcome_prices = _parse_json_list(mkt.get("outcomePrices", []))
        clob_token_ids = _parse_json_list(mkt.get("clobTokenIds", []))

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
        for key in ("volume", "volumeClob", "volume24hr", "volume24hrClob"):
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
