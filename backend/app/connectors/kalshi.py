"""Kalshi prediction market connector — public REST API (no auth required for reads)."""
import asyncio
import logging
from decimal import Decimal, InvalidOperation

import httpx

from app.config import settings
from app.connectors.base import BaseConnector, RawMarket, RawOrderbook, RawOutcome
from app.connectors.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = [1.0, 2.0, 4.0]

# Kalshi returns prices as FixedPoint dollar strings (e.g. "0.5600")
# Binary markets: YES price + NO price ~= $1.00


class KalshiConnector(BaseConnector):
    def __init__(self):
        self.api_base = settings.kalshi_api_base
        self._client: httpx.AsyncClient | None = None
        self.circuit_breaker = CircuitBreaker("kalshi")

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
        """HTTP request with exponential backoff on transient failures."""
        self.circuit_breaker.check()
        client = await self._get_client()
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                resp = await getattr(client, method)(url, **kwargs)

                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("retry-after", RETRY_BACKOFF[attempt]))
                    logger.warning(
                        "Kalshi rate limited (429) on %s, backing off %.1fs (attempt %d/%d)",
                        url, retry_after, attempt + 1, MAX_RETRIES,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status_code >= 500:
                    logger.warning(
                        "Kalshi server error %d on %s (attempt %d/%d)",
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
                        "Kalshi connection error on %s: %s (attempt %d/%d, retrying in %.1fs)",
                        url, type(e).__name__, attempt + 1, MAX_RETRIES, RETRY_BACKOFF[attempt],
                    )
                    await asyncio.sleep(RETRY_BACKOFF[attempt])
                else:
                    logger.error("All %d retries exhausted for %s: %s", MAX_RETRIES, url, e)

        self.circuit_breaker.record_failure()
        if last_exc:
            raise last_exc
        raise httpx.HTTPError(f"Failed after {MAX_RETRIES} retries: {url}")

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    async def fetch_markets(self, limit: int = 100, offset: int = 0) -> list[RawMarket]:
        """Fetch open markets from Kalshi. Uses cursor pagination (offset ignored after first call)."""
        params: dict = {
            "limit": min(limit, 1000),
            "status": "open",
            "mve_filter": "exclude",  # exclude multivariate/parlay markets (mostly zero-volume)
        }
        # Kalshi uses cursor pagination. For the initial call we don't pass a cursor.
        # The caller (discover_markets) pages via offset; we translate offset > 0 into
        # "keep paginating until we've skipped enough" but practically the ingestion
        # layer will be refactored to call us in a loop with the cursor we return.
        # For simplicity, each call returns one page.

        resp = await self._request_with_retry(
            "get", f"{self.api_base}/markets", params=params,
        )
        data = resp.json()
        markets_data = data.get("markets") or []

        raw_markets: list[RawMarket] = []
        for mkt in markets_data:
            try:
                raw_markets.append(self._parse_market(mkt))
            except Exception:
                logger.warning("Failed to parse Kalshi market %s", mkt.get("ticker"), exc_info=True)

        return raw_markets

    async def fetch_markets_cursor(self, limit: int = 200, cursor: str | None = None) -> tuple[list[RawMarket], str | None]:
        """Cursor-based pagination for Kalshi markets. Returns (markets, next_cursor)."""
        params: dict = {
            "limit": min(limit, 1000),
            "status": "open",
            "mve_filter": "exclude",
        }
        if cursor:
            params["cursor"] = cursor

        resp = await self._request_with_retry(
            "get", f"{self.api_base}/markets", params=params,
        )
        data = resp.json()
        markets_data = data.get("markets") or []
        next_cursor = data.get("cursor") or None

        raw_markets: list[RawMarket] = []
        for mkt in markets_data:
            try:
                raw_markets.append(self._parse_market(mkt))
            except Exception:
                logger.warning("Failed to parse Kalshi market %s", mkt.get("ticker"), exc_info=True)

        return raw_markets, next_cursor

    async def fetch_resolved_markets(self, since_hours: int = 24) -> list[dict]:
        """Fetch recently settled Kalshi markets.

        Returns list of {platform_id, winning_outcome} dicts.
        Kalshi markets have a 'result' field ('yes' or 'no') when settled.
        """
        resolved: list[dict] = []
        cursor = None
        pages = 0
        while True:
            try:
                params: dict = {
                    "limit": 200,
                    "status": "settled",
                }
                if cursor:
                    params["cursor"] = cursor

                resp = await self._request_with_retry(
                    "get", f"{self.api_base}/markets", params=params,
                )
                data = resp.json()
                markets_data = data.get("markets") or []
                if not markets_data:
                    break

                for mkt in markets_data:
                    result = mkt.get("result")
                    if result is None:
                        continue
                    resolved.append({
                        "platform_id": mkt.get("ticker", ""),
                        "winning_outcome": result,  # "yes" or "no"
                    })

                next_cursor = data.get("cursor")
                pages += 1
                if not next_cursor or pages >= 5:
                    break
                cursor = next_cursor
            except Exception:
                logger.warning("Failed to fetch resolved Kalshi markets", exc_info=True)
                break

        logger.info("Kalshi: fetched %d resolved markets", len(resolved))
        return resolved

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        """Fetch current midpoints for Kalshi outcomes.

        Token IDs have the form "TICKER:yes" or "TICKER:no".
        We batch-fetch market data via GET /markets?tickers=... and compute midpoints
        from bid/ask prices.
        """
        results: dict[str, Decimal] = {}

        # Deduplicate tickers from token_ids
        ticker_set: set[str] = set()
        for tid in token_ids:
            ticker = tid.rsplit(":", 1)[0]
            ticker_set.add(ticker)

        tickers = list(ticker_set)

        # Kalshi accepts comma-separated tickers (batch up to 200)
        for i in range(0, len(tickers), 200):
            batch = tickers[i : i + 200]
            tickers_param = ",".join(batch)
            try:
                resp = await self._request_with_retry(
                    "get",
                    f"{self.api_base}/markets",
                    params={"tickers": tickers_param},
                )
                data = resp.json()
                for mkt in data.get("markets") or []:
                    ticker = mkt.get("ticker", "")
                    yes_mid = self._compute_midpoint(
                        mkt.get("yes_bid_dollars"), mkt.get("yes_ask_dollars"),
                        mkt.get("last_price_dollars"),
                    )
                    no_mid = self._compute_midpoint(
                        mkt.get("no_bid_dollars"), mkt.get("no_ask_dollars"),
                        fallback=str(1 - float(yes_mid)) if yes_mid else None,
                    )

                    if yes_mid is not None:
                        results[f"{ticker}:yes"] = yes_mid
                    if no_mid is not None:
                        results[f"{ticker}:no"] = no_mid

            except (httpx.HTTPError, httpx.HTTPStatusError) as e:
                logger.warning("Failed to fetch Kalshi midpoints batch %d: %s", i // 200, e)

        return results

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        """Fetch orderbook for a Kalshi outcome.

        token_id is "TICKER:yes" or "TICKER:no".
        The Kalshi orderbook returns yes_dollars and no_dollars arrays.
        For the YES side, yes_dollars are the bids and no_dollars (inverted) are the asks.
        For the NO side, it's reversed.
        """
        parts = token_id.rsplit(":", 1)
        ticker = parts[0]
        side = parts[1] if len(parts) > 1 else "yes"

        resp = await self._request_with_retry(
            "get",
            f"{self.api_base}/markets/{ticker}/orderbook",
            params={"depth": 20},
        )
        data = resp.json()
        ob = data.get("orderbook_fp") or data.get("orderbook") or {}

        yes_levels = ob.get("yes_dollars") or ob.get("yes") or []
        no_levels = ob.get("no_dollars") or ob.get("no") or []

        if side == "yes":
            # YES bids are in yes_dollars (sorted ascending, best bid = last)
            # YES asks: invert no_dollars prices (ask = 1 - no_bid_price)
            bids = [[p, s] for p, s in reversed(yes_levels)]  # best bid first
            asks = [[str(round(1.0 - float(p), 4)), s] for p, s in no_levels]
            asks.sort(key=lambda x: float(x[0]))  # lowest ask first
        else:
            # NO bids are in no_dollars (sorted ascending, best bid = last)
            # NO asks: invert yes_dollars prices
            bids = [[p, s] for p, s in reversed(no_levels)]
            asks = [[str(round(1.0 - float(p), 4)), s] for p, s in yes_levels]
            asks.sort(key=lambda x: float(x[0]))

        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        return RawOrderbook(token_id=token_id, bids=bids, asks=asks, spread=spread)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_midpoint(bid_str: str | None, ask_str: str | None, fallback: str | None = None) -> Decimal | None:
        """Compute midpoint from bid/ask strings, falling back to last_price or explicit fallback."""
        try:
            bid = Decimal(bid_str) if bid_str else None
            ask = Decimal(ask_str) if ask_str else None
            if bid and ask and bid > 0 and ask > 0:
                return (bid + ask) / 2
            if bid and bid > 0:
                return bid
            if ask and ask > 0:
                return ask
        except (InvalidOperation, TypeError):
            pass

        if fallback:
            try:
                val = Decimal(fallback)
                if val > 0:
                    return val
            except (InvalidOperation, TypeError):
                pass

        return None

    def _parse_market(self, mkt: dict) -> RawMarket:
        """Convert a Kalshi market JSON object to a RawMarket."""
        ticker = mkt.get("ticker", "")
        # Use event title + market subtitle for the question
        title = mkt.get("title") or mkt.get("yes_sub_title") or ticker

        # Volume
        vol_24h = None
        vol_str = mkt.get("volume_24h_fp") or mkt.get("volume_24h")
        if vol_str:
            try:
                vol_24h = Decimal(str(vol_str))
            except (InvalidOperation, TypeError):
                pass

        # Open interest as proxy for liquidity
        liquidity = None
        oi_str = mkt.get("open_interest_fp") or mkt.get("open_interest")
        if oi_str:
            try:
                liquidity = Decimal(str(oi_str))
            except (InvalidOperation, TypeError):
                pass

        # End date from close_time
        close_time = mkt.get("close_time") or mkt.get("expiration_time")

        # Current prices for YES and NO outcomes
        yes_price = self._compute_midpoint(
            mkt.get("yes_bid_dollars"), mkt.get("yes_ask_dollars"),
            mkt.get("last_price_dollars"),
        )
        no_price = self._compute_midpoint(
            mkt.get("no_bid_dollars"), mkt.get("no_ask_dollars"),
            fallback=str(1 - float(yes_price)) if yes_price else None,
        )

        outcomes = [
            RawOutcome(
                platform_outcome_id=f"{ticker}_yes",
                name="Yes",
                token_id=f"{ticker}:yes",
                price=yes_price,
            ),
            RawOutcome(
                platform_outcome_id=f"{ticker}_no",
                name="No",
                token_id=f"{ticker}:no",
                price=no_price,
            ),
        ]

        return RawMarket(
            platform="kalshi",
            platform_id=ticker,
            slug=ticker.lower(),
            question=title,
            category=mkt.get("category") or mkt.get("series_ticker"),
            end_date=close_time,
            active=mkt.get("status") == "open" or mkt.get("status") == "active",
            outcomes=outcomes,
            volume_24h=vol_24h,
            liquidity=liquidity,
            metadata={
                "event_ticker": mkt.get("event_ticker"),
                "series_ticker": mkt.get("series_ticker"),
                "market_type": mkt.get("market_type"),
                "strike_type": mkt.get("strike_type"),
                "floor_strike": mkt.get("floor_strike"),
                "cap_strike": mkt.get("cap_strike"),
                "open_interest": str(oi_str) if oi_str else None,
                "result": mkt.get("result"),
            },
        )
