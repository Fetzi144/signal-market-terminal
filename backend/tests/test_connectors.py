"""Tests for Polymarket and Kalshi connectors using respx HTTP mocking."""
import json
from decimal import Decimal

import httpx
import pytest
import respx

from app.connectors.kalshi import KalshiConnector
from app.connectors.polymarket import PolymarketConnector

# ── Polymarket Tests ──


class TestPolymarketParseMarket:
    """Test _parse_market with various Gamma API shapes."""

    def setup_method(self):
        self.connector = PolymarketConnector()

    def test_parse_basic_market(self):
        mkt = {
            "id": "abc123",
            "question": "Will it rain?",
            "slug": "will-it-rain",
            "active": True,
            "outcomes": json.dumps(["Yes", "No"]),
            "outcomePrices": json.dumps(["0.65", "0.35"]),
            "clobTokenIds": json.dumps(["token_yes", "token_no"]),
            "volume": "50000",
            "liquidityNum": "10000",
            "endDate": "2025-12-31T00:00:00Z",
            "groupItemTitle": "Weather",
        }
        raw = self.connector._parse_market(mkt)
        assert raw.platform == "polymarket"
        assert raw.platform_id == "abc123"
        assert raw.question == "Will it rain?"
        assert len(raw.outcomes) == 2
        assert raw.outcomes[0].name == "Yes"
        assert raw.outcomes[0].token_id == "token_yes"
        assert raw.outcomes[0].price == Decimal("0.65")
        assert raw.volume_24h == Decimal("50000")
        assert raw.liquidity == Decimal("10000")

    def test_parse_json_string_fields(self):
        """Gamma API returns some fields as JSON-encoded strings."""
        mkt = {
            "id": "xyz",
            "question": "Test?",
            "active": True,
            "outcomes": '["A", "B"]',
            "outcomePrices": '["0.50", "0.50"]',
            "clobTokenIds": '["t1", "t2"]',
        }
        raw = self.connector._parse_market(mkt)
        assert raw.outcomes[0].name == "A"
        assert raw.outcomes[0].token_id == "t1"

    def test_parse_market_missing_clob_tokens(self):
        """Markets without clobTokenIds should still parse (token_id=None)."""
        mkt = {
            "id": "no-tokens",
            "question": "No tokens?",
            "active": True,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.50", "0.50"]',
        }
        raw = self.connector._parse_market(mkt)
        assert raw.outcomes[0].token_id is None
        assert raw.outcomes[1].token_id is None


@pytest.mark.asyncio
class TestPolymarketMidpoints:
    async def test_fetch_midpoints_batch(self):
        connector = PolymarketConnector()
        try:
            with respx.mock:
                respx.get("https://clob.polymarket.com/midpoints").mock(
                    return_value=httpx.Response(200, json={"tok1": "0.55", "tok2": "0.45"})
                )
                result = await connector.fetch_midpoints(["tok1", "tok2"])
                assert result["tok1"] == Decimal("0.55")
                assert result["tok2"] == Decimal("0.45")
        finally:
            await connector.close()

    async def test_fetch_midpoints_invalid_value_skipped(self):
        connector = PolymarketConnector()
        try:
            with respx.mock:
                respx.get("https://clob.polymarket.com/midpoints").mock(
                    return_value=httpx.Response(200, json={"tok1": "0.55", "tok2": "invalid"})
                )
                result = await connector.fetch_midpoints(["tok1", "tok2"])
                assert "tok1" in result
                assert "tok2" not in result
        finally:
            await connector.close()


@pytest.mark.asyncio
class TestPolymarketOrderbook:
    async def test_fetch_orderbook(self):
        connector = PolymarketConnector()
        try:
            with respx.mock:
                respx.get("https://clob.polymarket.com/book").mock(
                    return_value=httpx.Response(200, json={
                        "bids": [{"price": "0.50", "size": "100"}, {"price": "0.49", "size": "200"}],
                        "asks": [{"price": "0.52", "size": "150"}],
                    })
                )
                ob = await connector.fetch_orderbook("tok1")
                assert ob.token_id == "tok1"
                assert len(ob.bids) == 2
                assert len(ob.asks) == 1
                assert ob.spread == Decimal("0.02")
        finally:
            await connector.close()


@pytest.mark.asyncio
class TestPolymarketRetry:
    async def test_retry_on_429(self):
        connector = PolymarketConnector()
        try:
            with respx.mock:
                route = respx.get("https://clob.polymarket.com/midpoints")
                route.side_effect = [
                    httpx.Response(429, headers={"retry-after": "0.01"}),
                    httpx.Response(200, json={"tok1": "0.60"}),
                ]
                result = await connector.fetch_midpoints(["tok1"])
                assert result["tok1"] == Decimal("0.60")
                assert route.call_count == 2
        finally:
            await connector.close()


# ── Kalshi Tests ──


class TestKalshiParseMarket:
    def setup_method(self):
        self.connector = KalshiConnector()

    def test_parse_basic_market(self):
        mkt = {
            "ticker": "KXBTC-25MAR",
            "title": "BTC above $100k?",
            "status": "open",
            "category": "Crypto",
            "close_time": "2025-03-31T00:00:00Z",
            "yes_bid_dollars": "0.6500",
            "yes_ask_dollars": "0.6700",
            "no_bid_dollars": "0.3100",
            "no_ask_dollars": "0.3700",
            "volume_24h_fp": "10000",
            "open_interest_fp": "5000",
        }
        raw = self.connector._parse_market(mkt)
        assert raw.platform == "kalshi"
        assert raw.platform_id == "KXBTC-25MAR"
        assert raw.question == "BTC above $100k?"
        assert len(raw.outcomes) == 2
        assert raw.outcomes[0].name == "Yes"
        assert raw.outcomes[0].token_id == "KXBTC-25MAR:yes"
        assert raw.outcomes[1].name == "No"
        assert raw.outcomes[1].token_id == "KXBTC-25MAR:no"
        assert raw.volume_24h == Decimal("10000")
        assert raw.active is True

    def test_parse_market_missing_prices(self):
        """Market with no bid/ask should still parse."""
        mkt = {
            "ticker": "KTEST",
            "title": "Test market",
            "status": "open",
        }
        raw = self.connector._parse_market(mkt)
        assert raw.outcomes[0].price is None
        assert raw.outcomes[1].price is None


class TestKalshiComputeMidpoint:
    def test_midpoint_from_bid_ask(self):
        mid = KalshiConnector._compute_midpoint("0.40", "0.60")
        assert mid == Decimal("0.50")

    def test_midpoint_bid_only(self):
        mid = KalshiConnector._compute_midpoint("0.40", None)
        assert mid == Decimal("0.40")

    def test_midpoint_fallback(self):
        mid = KalshiConnector._compute_midpoint(None, None, fallback="0.55")
        assert mid == Decimal("0.55")

    def test_midpoint_no_data(self):
        mid = KalshiConnector._compute_midpoint(None, None)
        assert mid is None


@pytest.mark.asyncio
class TestKalshiCursorPagination:
    async def test_cursor_pagination(self):
        connector = KalshiConnector()
        try:
            page1 = {
                "markets": [{
                    "ticker": "T1", "title": "Market 1", "status": "open",
                }],
                "cursor": "next_page",
            }
            page2 = {
                "markets": [{
                    "ticker": "T2", "title": "Market 2", "status": "open",
                }],
                "cursor": "",
            }
            with respx.mock:
                route = respx.get(f"{connector.api_base}/markets")
                route.side_effect = [
                    httpx.Response(200, json=page1),
                    httpx.Response(200, json=page2),
                ]
                markets1, cursor1 = await connector.fetch_markets_cursor(limit=1)
                assert len(markets1) == 1
                assert markets1[0].platform_id == "T1"
                assert cursor1 == "next_page"

                markets2, cursor2 = await connector.fetch_markets_cursor(limit=1, cursor=cursor1)
                assert len(markets2) == 1
                assert markets2[0].platform_id == "T2"
                assert cursor2 is None  # empty string -> None
        finally:
            await connector.close()


@pytest.mark.asyncio
class TestKalshiOrderbook:
    async def test_orderbook_yes_side(self):
        connector = KalshiConnector()
        try:
            with respx.mock:
                respx.get(f"{connector.api_base}/markets/KTEST/orderbook").mock(
                    return_value=httpx.Response(200, json={
                        "orderbook_fp": {
                            "yes_dollars": [["0.40", "100"], ["0.45", "200"]],
                            "no_dollars": [["0.50", "150"], ["0.55", "50"]],
                        }
                    })
                )
                ob = await connector.fetch_orderbook("KTEST:yes")
                assert ob.token_id == "KTEST:yes"
                # Best yes bid should be 0.45 (reversed from ascending list)
                assert ob.bids[0][0] == "0.45"
                # YES asks are inverted NO bids: 1 - 0.50 = 0.50, 1 - 0.55 = 0.45
                assert len(ob.asks) == 2
        finally:
            await connector.close()

    async def test_orderbook_no_side(self):
        connector = KalshiConnector()
        try:
            with respx.mock:
                respx.get(f"{connector.api_base}/markets/KTEST/orderbook").mock(
                    return_value=httpx.Response(200, json={
                        "orderbook_fp": {
                            "yes_dollars": [["0.40", "100"]],
                            "no_dollars": [["0.55", "200"]],
                        }
                    })
                )
                ob = await connector.fetch_orderbook("KTEST:no")
                assert ob.token_id == "KTEST:no"
                # Best NO bid = 0.55 (from no_dollars reversed)
                assert ob.bids[0][0] == "0.55"
        finally:
            await connector.close()
