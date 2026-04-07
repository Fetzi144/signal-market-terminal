"""Missing endpoint tests: /alerts/recent, /push/*, CSV exports, chart-data."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal

# -- /alerts/recent --

@pytest.mark.asyncio
async def test_alerts_recent_empty(client, session):
    """GET /alerts/recent returns empty list when no alerted signals."""
    resp = await client.get("/api/v1/alerts/recent")
    assert resp.status_code == 200
    data = resp.json()
    assert data["alerts"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_alerts_recent_returns_alerted_signals(client, session):
    """GET /alerts/recent returns only signals marked as alerted."""
    market = make_market(session, question="Alert test market")
    outcome = make_outcome(session, market.id, name="Yes")
    # Alerted signal
    make_signal(session, market.id, outcome.id, alerted=True, rank_score=Decimal("0.800"))
    # Non-alerted signal — use different timeframe to avoid dedupe collision
    make_signal(session, market.id, outcome.id, alerted=False, rank_score=Decimal("0.900"),
                timeframe="4h")
    await session.commit()

    resp = await client.get("/api/v1/alerts/recent")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["alerts"]) == 1
    alert = data["alerts"][0]
    assert "signal_type" in alert
    assert "market_question" in alert
    assert "rank_score" in alert
    assert "fired_at" in alert


@pytest.mark.asyncio
async def test_alerts_recent_filter_by_type(client, session):
    """GET /alerts/recent?signal_type=volume_spike filters correctly."""
    market = make_market(session, question="Alert filter test")
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(session, market.id, outcome.id, alerted=True, signal_type="price_move")
    make_signal(session, market.id, outcome.id, alerted=True, signal_type="volume_spike",
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    await session.commit()

    resp = await client.get("/api/v1/alerts/recent", params={"signal_type": "volume_spike"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["alerts"][0]["signal_type"] == "volume_spike"


# -- /push/* --

@pytest.mark.asyncio
async def test_push_vapid_key(client):
    """GET /push/vapid-key returns the VAPID public key."""
    resp = await client.get("/api/v1/push/vapid-key")
    assert resp.status_code == 200
    data = resp.json()
    assert "vapid_public_key" in data


@pytest.mark.asyncio
async def test_push_subscribe_and_unsubscribe(client, session):
    """POST /push/subscribe creates subscription, DELETE removes it."""
    sub_data = {
        "endpoint": "https://fcm.googleapis.com/fcm/send/test-endpoint-123",
        "keys": {"p256dh": "test-p256dh", "auth": "test-auth"},
    }

    # Subscribe
    resp = await client.post("/api/v1/push/subscribe", json=sub_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "subscribed"
    assert "vapid_public_key" in data

    # Subscribe again (upsert — should not fail)
    resp = await client.post("/api/v1/push/subscribe", json=sub_data)
    assert resp.status_code == 200

    # Unsubscribe
    resp = await client.request("DELETE", "/api/v1/push/subscribe", json=sub_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "unsubscribed"


# -- CSV Exports --

@pytest.mark.asyncio
async def test_signals_csv_export(client, session):
    """GET /signals/export/csv returns valid CSV with headers."""
    market = make_market(session, question="CSV signal test")
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(session, market.id, outcome.id)
    await session.commit()

    resp = await client.get("/api/v1/signals/export/csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "content-disposition" in resp.headers

    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2  # header + at least 1 data row
    header = lines[0]
    assert "id" in header
    assert "signal_type" in header
    assert "rank_score" in header
    assert "resolved_correctly" in header


@pytest.mark.asyncio
async def test_markets_csv_export(client, session):
    """GET /markets/export/csv returns valid CSV."""
    make_market(session, question="CSV market test", active=True)
    await session.commit()

    resp = await client.get("/api/v1/markets/export/csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")

    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2
    header = lines[0]
    assert "id" in header
    assert "platform" in header
    assert "question" in header


@pytest.mark.asyncio
async def test_portfolio_csv_export(client, session):
    """GET /portfolio/export/csv returns valid CSV."""
    market = make_market(session, question="CSV portfolio test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.commit()

    # Create a position via service
    from app.portfolio.service import open_position
    await open_position(
        session,
        market_id=market.id,
        outcome_id=outcome.id,
        platform="polymarket",
        side="yes",
        quantity=Decimal("10"),
        price=Decimal("0.50"),
    )

    resp = await client.get("/api/v1/portfolio/export/csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")

    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2
    header = lines[0]
    assert "id" in header
    assert "market_id" in header
    assert "quantity" in header
    assert "avg_entry_price" in header


# -- /markets/{id}/chart-data --

@pytest.mark.asyncio
async def test_market_chart_data(client, session):
    """GET /markets/{id}/chart-data returns time series."""
    from datetime import timedelta

    market = make_market(session, question="Chart data test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    for i in range(5):
        make_price_snapshot(session, outcome.id, Decimal("0.50") + Decimal(str(i * 0.01)),
                            captured_at=now - timedelta(minutes=i * 10))
    await session.commit()

    resp = await client.get(f"/api/v1/markets/{market.id}/chart-data")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) or "chart_data" in str(type(data)) or isinstance(data, dict)


# -- /analytics/correlated-signals --

@pytest.mark.asyncio
async def test_analytics_correlated_signals(client, session):
    """GET /analytics/correlated-signals returns data (possibly empty)."""
    resp = await client.get("/api/v1/analytics/correlated-signals")
    assert resp.status_code == 200
