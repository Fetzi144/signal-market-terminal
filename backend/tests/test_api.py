"""Tests for API endpoints."""
import uuid

import pytest
import pytest_asyncio

from tests.conftest import make_market, make_outcome, make_signal


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "active_markets" in data
    assert "total_signals" in data
    assert "unresolved_signals" in data
    assert "recent_alerts_24h" in data
    assert "alert_threshold" in data
    assert "ingestion" in data


@pytest.mark.asyncio
async def test_signals_list_empty(client):
    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["signals"] == []
    assert data["total"] == 0
    assert data["page"] == 1


@pytest.mark.asyncio
async def test_signal_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/signals/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_markets_list_empty(client):
    resp = await client.get("/api/v1/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["markets"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_market_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/markets/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_root_endpoint(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Signal Market Terminal"
