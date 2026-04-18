import asyncio
import logging
import time
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
INVALID_TOKEN_TTL_SECONDS = 60 * 60


class PolymarketTokenNotFoundError(LookupError):
    """Raised when a Polymarket token no longer has a live orderbook."""

    def __init__(self, token_id: str):
        super().__init__(f"Polymarket token not found: {token_id}")
        self.token_id = token_id


class PolymarketConnector(BaseConnector):
    _invalid_token_cache: dict[str, float] = {}

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
        normalized_token_ids = self._normalize_token_ids(token_ids)

        for i in range(0, len(normalized_token_ids), BATCH_SIZE):
            batch = normalized_token_ids[i : i + BATCH_SIZE]
            batch_results = await self._fetch_midpoint_batch(batch, batch_index=i // BATCH_SIZE)
            results.update(batch_results)
        return results

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        """Fetch L2 order book for a single token."""
        normalized_token_id = str(token_id or "").strip()
        if not normalized_token_id:
            raise ValueError("token_id is required")
        if self._is_cached_invalid_token(normalized_token_id):
            raise PolymarketTokenNotFoundError(normalized_token_id)

        try:
            resp = await self._request_with_retry(
                "get", f"{self.clob_base}/book", params={"token_id": normalized_token_id},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                self._mark_invalid_token(normalized_token_id, reason="404 orderbook")
                raise PolymarketTokenNotFoundError(normalized_token_id) from exc
            raise
        data = resp.json()

        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        bids = [[b["price"], b["size"]] for b in raw_bids]
        asks = [[a["price"], a["size"]] for a in raw_asks]

        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        return RawOrderbook(token_id=normalized_token_id, bids=bids, asks=asks, spread=spread)

    async def fetch_resolved_markets(self, since_hours: int = 24) -> list[dict]:
        """Fetch recently resolved markets from Gamma API.

        Returns list of {platform_id, winning_outcome_id, winner} dicts.
        """
        resolved: list[dict] = []
        offset = 0
        limit = 100
        while True:
            try:
                resp = await self._request_with_retry(
                    "get",
                    f"{self.gamma_base}/markets",
                    params={
                        "closed": "true",
                        "limit": limit,
                        "offset": offset,
                    },
                )
                markets_data = resp.json()
                if not markets_data:
                    break

                for mkt in markets_data:
                    winner = mkt.get("winner")
                    if winner is None:
                        continue
                    resolved.append({
                        "platform_id": str(mkt["id"]),
                        "winning_outcome_id": winner,
                        "winner": winner,
                    })

                offset += limit
                if len(markets_data) < limit or offset >= 1000:
                    break
            except Exception:
                logger.warning("Failed to fetch resolved Polymarket markets at offset %d", offset, exc_info=True)
                break

        logger.info("Polymarket: fetched %d resolved markets", len(resolved))
        return resolved

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

    def _normalize_token_ids(self, token_ids: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        skipped_empty = 0
        skipped_cached_invalid = 0
        self._prune_invalid_token_cache()

        for token_id in token_ids:
            normalized_token_id = str(token_id or "").strip()
            if not normalized_token_id:
                skipped_empty += 1
                continue
            if self._is_cached_invalid_token(normalized_token_id):
                skipped_cached_invalid += 1
                continue
            if normalized_token_id in seen:
                continue
            seen.add(normalized_token_id)
            normalized.append(normalized_token_id)

        if skipped_empty:
            logger.warning("Polymarket midpoints skipped %d empty token ids", skipped_empty)
        if skipped_cached_invalid:
            logger.info(
                "Polymarket midpoints skipped %d cached invalid token ids",
                skipped_cached_invalid,
            )

        return normalized

    async def _fetch_midpoint_batch(self, batch: list[str], *, batch_index: int) -> dict[str, Decimal]:
        if not batch:
            return {}

        try:
            resp = await self._request_with_retry(
                "post",
                f"{self.clob_base}/midpoints",
                json=[{"token_id": token_id} for token_id in batch],
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 400 and len(batch) > 1:
                midpoint = max(1, len(batch) // 2)
                logger.warning(
                    "Polymarket midpoints batch %d rejected %d token ids with 400; splitting batch",
                    batch_index,
                    len(batch),
                )
                results: dict[str, Decimal] = {}
                results.update(await self._fetch_midpoint_batch(batch[:midpoint], batch_index=batch_index))
                results.update(await self._fetch_midpoint_batch(batch[midpoint:], batch_index=batch_index))
                return results

            if exc.response is not None and exc.response.status_code == 400:
                return await self._fetch_single_midpoint(batch[0], batch_index=batch_index)

            logger.warning(
                "Failed to fetch Polymarket midpoints batch %d (%d tokens): %s",
                batch_index,
                len(batch),
                exc,
            )
            return {}
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to fetch Polymarket midpoints batch %d (%d tokens): %s",
                batch_index,
                len(batch),
                exc,
            )
            return {}

        data = resp.json()
        results: dict[str, Decimal] = {}
        for tid, mid in data.items():
            try:
                results[tid] = Decimal(str(mid))
            except (InvalidOperation, TypeError):
                logger.warning("Invalid midpoint for token %s: %s", tid, mid)
        return results

    async def _fetch_single_midpoint(self, token_id: str, *, batch_index: int) -> dict[str, Decimal]:
        try:
            resp = await self._request_with_retry(
                "get",
                f"{self.clob_base}/midpoint",
                params={"token_id": token_id},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code in {400, 404}:
                self._mark_invalid_token(token_id, reason=f"{exc.response.status_code} midpoint")
                logger.warning(
                    "Polymarket midpoint rejected token %s with %d; skipping token",
                    token_id,
                    exc.response.status_code,
                )
                return {}

            logger.warning(
                "Failed to fetch Polymarket midpoint for token %s in batch %d: %s",
                token_id,
                batch_index,
                exc,
            )
            return {}
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to fetch Polymarket midpoint for token %s in batch %d: %s",
                token_id,
                batch_index,
                exc,
            )
            return {}

        data = resp.json()
        mid = data.get("mid")
        if mid is None:
            mid = data.get("mid_price")
        try:
            return {token_id: Decimal(str(mid))}
        except (InvalidOperation, TypeError):
            logger.warning("Invalid midpoint for token %s: %s", token_id, mid)
            return {}

    @classmethod
    def _prune_invalid_token_cache(cls) -> None:
        now = time.monotonic()
        expired = [
            token_id for token_id, expires_at in cls._invalid_token_cache.items()
            if expires_at <= now
        ]
        for token_id in expired:
            cls._invalid_token_cache.pop(token_id, None)

    @classmethod
    def _is_cached_invalid_token(cls, token_id: str) -> bool:
        expires_at = cls._invalid_token_cache.get(token_id)
        if expires_at is None:
            return False
        if expires_at <= time.monotonic():
            cls._invalid_token_cache.pop(token_id, None)
            return False
        return True

    @classmethod
    def _mark_invalid_token(cls, token_id: str, *, reason: str) -> None:
        if cls._is_cached_invalid_token(token_id):
            return
        cls._invalid_token_cache[token_id] = time.monotonic() + INVALID_TOKEN_TTL_SECONDS
        logger.warning(
            "Polymarket token %s marked invalid for %ds after %s",
            token_id,
            INVALID_TOKEN_TTL_SECONDS,
            reason,
        )
