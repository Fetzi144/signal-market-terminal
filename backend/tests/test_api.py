"""Integration tests for API endpoints."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


# ── Health ──────────────────────────────────────────────


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
    assert isinstance(data["ingestion"], list)


# ── Signals list ────────────────────────────────────────


@pytest.mark.asyncio
async def test_signals_list_empty(client):
    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["signals"] == []
    assert data["total"] == 0
    assert data["page"] == 1


@pytest.mark.asyncio
async def test_signals_list_returns_paginated(client, engine):
    """Signals are returned with correct schema and pagination."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session, platform="polymarket")
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        for i in range(3):
            make_signal(
                session, market.id, outcome.id,
                signal_type="price_move",
                rank_score=Decimal(f"0.{5 + i}00"),
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(minutes=15 * i),
            )
        await session.commit()

    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert len(data["signals"]) == 3
    # Check schema fields
    s = data["signals"][0]
    assert "id" in s
    assert "signal_type" in s
    assert "rank_score" in s
    assert "market_question" in s
    assert "platform" in s


@pytest.mark.asyncio
async def test_signals_filter_by_type(client, engine):
    """Filter signals by signal_type."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        now = datetime.now(timezone.utc)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        make_signal(session, market.id, outcome.id, signal_type="price_move", dedupe_bucket=bucket)
        make_signal(session, market.id, outcome.id, signal_type="volume_spike", dedupe_bucket=bucket - timedelta(minutes=15))
        await session.commit()

    resp = await client.get("/api/v1/signals?signal_type=price_move")
    data = resp.json()
    assert data["total"] == 1
    assert data["signals"][0]["signal_type"] == "price_move"


# ── Signal detail ───────────────────────────────────────


@pytest.mark.asyncio
async def test_signal_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/signals/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_signal_detail_with_evaluations(client, engine):
    """GET /signals/{id} returns signal with evaluations."""
    from app.models.signal import SignalEvaluation
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        signal = make_signal(session, market.id, outcome.id)
        await session.flush()
        # Add an evaluation
        ev = SignalEvaluation(
            id=uuid.uuid4(),
            signal_id=signal.id,
            horizon="15m",
            price_at_eval=Decimal("0.550"),
            price_change=Decimal("0.050"),
            price_change_pct=Decimal("10.00"),
        )
        session.add(ev)
        await session.commit()
        signal_id = str(signal.id)

    resp = await client.get(f"/api/v1/signals/{signal_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == signal_id
    assert len(data["evaluations"]) == 1
    assert data["evaluations"][0]["horizon"] == "15m"


# ── Markets list ────────────────────────────────────────


@pytest.mark.asyncio
async def test_markets_list_empty(client):
    resp = await client.get("/api/v1/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["markets"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_markets_list_returns_data(client, engine):
    """GET /markets returns active markets."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        make_market(session, platform="polymarket", question="Will BTC hit 100k?")
        make_market(session, platform="kalshi", question="Will the Fed cut rates?")
        await session.commit()

    resp = await client.get("/api/v1/markets")
    data = resp.json()
    assert data["total"] == 2
    assert len(data["markets"]) == 2


# ── Market detail ───────────────────────────────────────


@pytest.mark.asyncio
async def test_market_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/markets/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_market_detail_with_outcomes(client, engine):
    """GET /markets/{id} returns outcomes with latest prices."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session, question="Test market?")
        await session.flush()
        outcome = make_outcome(session, market.id, name="Yes")
        await session.flush()
        make_price_snapshot(session, outcome.id, "0.65")
        await session.commit()
        market_id = str(market.id)

    resp = await client.get(f"/api/v1/markets/{market_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["question"] == "Test market?"
    assert len(data["outcomes"]) == 1
    assert data["outcomes"][0]["name"] == "Yes"
    assert data["outcomes"][0]["latest_price"] is not None


# ── CSV exports ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_signals_export_csv(client, engine):
    """GET /signals/export/csv returns text/csv."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        make_signal(session, market.id, outcome.id)
        await session.commit()

    resp = await client.get("/api/v1/signals/export/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2  # header + at least 1 data row
    assert "signal_type" in lines[0]


@pytest.mark.asyncio
async def test_markets_export_csv(client, engine):
    """GET /markets/export/csv returns text/csv."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        make_market(session)
        await session.commit()

    resp = await client.get("/api/v1/markets/export/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2


# ── Root endpoint ───────────────────────────────────────


@pytest.mark.asyncio
async def test_root_endpoint(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Signal Market Terminal"
