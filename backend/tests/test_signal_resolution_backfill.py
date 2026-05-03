from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.reports.signal_resolution_backfill import (
    parse_polymarket_resolution,
    run_signal_resolution_backfill,
)
from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeKalshiConnector:
    api_base = "https://kalshi.test"

    def __init__(self):
        self.calls = []
        self.closed = False

    async def _request_with_retry(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return _FakeResponse(
            {
                "markets": [
                    {
                        "ticker": "KXTEST-SETTLED",
                        "status": "finalized",
                        "result": "yes",
                    }
                ]
            }
        )

    async def close(self):
        self.closed = True


class _FakePolymarketConnector:
    gamma_base = "https://gamma.test"

    def __init__(self):
        self.calls = []
        self.closed = False

    async def _request_with_retry(self, method, url, **kwargs):  # noqa: ARG002
        self.calls.append((method, url, kwargs))
        return _FakeResponse(
            {
                "id": "123",
                "closed": True,
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0", "1"]',
                "clobTokenIds": '["yes-token", "no-token"]',
            }
        )

    async def close(self):
        self.closed = True


def test_parse_polymarket_resolution_infers_unique_terminal_price():
    resolution, blocker = parse_polymarket_resolution(
        {
            "id": "123",
            "closed": True,
            "outcomes": '["Team A", "Team B"]',
            "outcomePrices": '["0", "1"]',
        }
    )

    assert blocker is None
    assert resolution == {"platform_id": "123", "winner": "Team B"}


def test_parse_polymarket_resolution_refuses_stale_open_end_date_market():
    resolution, blocker = parse_polymarket_resolution(
        {
            "id": "123",
            "closed": False,
            "active": True,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.95", "0.05"]',
        }
    )

    assert resolution is None
    assert blocker == "market_not_final"


@pytest.mark.asyncio
async def test_signal_resolution_backfill_applies_targeted_kalshi_resolution(session, monkeypatch):
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        platform="kalshi",
        platform_id="KXTEST-SETTLED",
        end_date=now - timedelta(days=1),
    )
    yes = make_outcome(session, market.id, name="Yes", platform_outcome_id="KXTEST-SETTLED_yes")
    make_price_snapshot(session, yes.id, "0.75")
    signal = make_signal(session, market.id, yes.id, details={"direction": "up"}, price_at_fire=Decimal("0.50"))
    await session.commit()

    connector = _FakeKalshiConnector()
    monkeypatch.setattr("app.reports.signal_resolution_backfill.get_connector", lambda platform: connector)

    result = await run_signal_resolution_backfill(session, platform="kalshi", apply=True)

    await session.refresh(signal)
    assert result["resolved_signal_count"] == 1
    assert result["alpha_ready_delta"] == 1
    assert signal.resolved_correctly is True
    assert str(signal.profit_loss) == "0.500000"
    assert str(signal.clv) == "0.250000"
    assert connector.closed is True


@pytest.mark.asyncio
async def test_signal_resolution_backfill_applies_closed_polymarket_price_resolution(session, monkeypatch):
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        platform="polymarket",
        platform_id="123",
        end_date=now - timedelta(hours=2),
    )
    make_outcome(session, market.id, name="Yes", token_id="yes-token")
    no = make_outcome(session, market.id, name="No", token_id="no-token")
    make_price_snapshot(session, no.id, "0.60")
    signal = make_signal(session, market.id, no.id, details={"direction": "up"}, price_at_fire=Decimal("0.40"))
    await session.commit()

    connector = _FakePolymarketConnector()
    monkeypatch.setattr("app.reports.signal_resolution_backfill.get_connector", lambda platform: connector)

    result = await run_signal_resolution_backfill(session, platform="polymarket", apply=True)

    await session.refresh(signal)
    assert result["resolved_signal_count"] == 1
    assert result["alpha_ready_delta"] == 1
    assert signal.resolved_correctly is True
    assert str(signal.profit_loss) == "0.600000"
    assert str(signal.clv) == "0.200000"
    assert connector.closed is True
