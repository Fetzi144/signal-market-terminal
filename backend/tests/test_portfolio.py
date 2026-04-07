"""Tests for portfolio position tracking: service logic + API endpoints."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.conftest import make_market, make_outcome, make_price_snapshot


def _make_position_payload(market_id, outcome_id, **overrides):
    payload = {
        "market_id": str(market_id),
        "outcome_id": str(outcome_id),
        "platform": "polymarket",
        "side": "yes",
        "quantity": 100,
        "price": 0.60,
    }
    payload.update(overrides)
    return payload


# -- Service tests --

@pytest.mark.asyncio
async def test_open_position_correct_entry_price(session):
    """Open position stores correct avg entry price."""
    from app.portfolio.service import get_position, open_position

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.60)

    assert pos.avg_entry_price == 0.60
    assert pos.quantity == 100
    assert pos.status == "open"

    # Reload with eager trades
    pos = await get_position(session, pos.id)
    assert len(pos.trades) == 1
    assert pos.trades[0].action == "buy"


@pytest.mark.asyncio
async def test_add_to_position_weighted_average(session):
    """Adding to position recalculates weighted average entry price."""
    from app.portfolio.service import add_to_position, open_position

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.60)
    pos = await add_to_position(session, pos.id, 50, 0.90)

    # Weighted avg: (100 * 0.60 + 50 * 0.90) / 150 = (60 + 45) / 150 = 0.70
    assert pos.quantity == 150
    assert abs(pos.avg_entry_price - 0.70) < 0.001


@pytest.mark.asyncio
async def test_partial_close_remaining_correct(session):
    """Partial close reduces quantity and computes realized P&L."""
    from app.portfolio.service import close_position, open_position

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.60)
    pos = await close_position(session, pos.id, 40, 0.80)

    assert pos.quantity == 60
    assert pos.status == "open"
    # Realized P&L: (0.80 - 0.60) * 40 = 8.0
    assert abs(pos.realized_pnl - 8.0) < 0.001


@pytest.mark.asyncio
async def test_full_close_status_and_pnl(session):
    """Full close sets status to closed and computes realized P&L."""
    from app.portfolio.service import close_position, open_position

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.60)
    pos = await close_position(session, pos.id, 100, 0.85)

    assert pos.quantity == 0
    assert pos.status == "closed"
    assert pos.exit_price == 0.85
    # Realized P&L: (0.85 - 0.60) * 100 = 25.0
    assert abs(pos.realized_pnl - 25.0) < 0.001


@pytest.mark.asyncio
async def test_market_resolution_auto_closes(session):
    """When a market resolves, open positions auto-close at $1 or $0."""
    from app.portfolio.service import open_position, resolve_positions

    m = make_market(session, active=False)  # Resolved market
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    # Add a price snapshot near 1.0 (winning outcome)
    make_price_snapshot(session, o.id, 0.95)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.60)

    resolved = await resolve_positions(session)
    assert resolved == 1

    await session.refresh(pos)
    assert pos.status == "resolved"
    assert pos.exit_price == 1.0
    # P&L: (1.0 - 0.60) * 100 = 40.0
    assert abs(pos.realized_pnl - 40.0) < 0.001


@pytest.mark.asyncio
async def test_portfolio_summary_aggregation(session):
    """Portfolio summary correctly aggregates open/closed positions."""
    from app.portfolio.service import close_position, get_portfolio_summary, open_position

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    # Open 2 positions, close 1 with profit
    pos1 = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.50)
    pos2 = await open_position(session, m.id, o.id, "polymarket", "yes", 50, 0.40)
    await close_position(session, pos1.id, 100, 0.70)

    summary = await get_portfolio_summary(session)
    assert summary["open_positions"] == 1
    assert summary["closed_positions"] == 1
    # Realized: (0.70 - 0.50) * 100 = 20.0
    assert abs(summary["total_realized_pnl"] - 20.0) < 0.001
    assert summary["win_rate"] == 100.0


@pytest.mark.asyncio
async def test_update_current_prices(session):
    """Price refresh updates current_price and unrealized_pnl from snapshots."""
    from app.portfolio.service import open_position, update_current_prices

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    pos = await open_position(session, m.id, o.id, "polymarket", "yes", 100, 0.50)

    # Add a price snapshot at 0.75
    make_price_snapshot(session, o.id, 0.75)
    await session.commit()

    updated = await update_current_prices(session)
    assert updated == 1

    await session.refresh(pos)
    assert abs(pos.current_price - 0.75) < 0.001
    # Unrealized: (0.75 - 0.50) * 100 = 25.0
    assert abs(pos.unrealized_pnl - 25.0) < 0.001


# -- API tests --

@pytest.mark.asyncio
async def test_api_create_position(client, engine):
    """POST /api/v1/positions creates a position."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as s:
        m = make_market(s)
        await s.flush()
        o = make_outcome(s, m.id)
        await s.commit()
        market_id, outcome_id = m.id, o.id

    resp = await client.post(
        "/api/v1/positions",
        json=_make_position_payload(market_id, outcome_id),
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "open"
    assert data["quantity"] == 100
    assert data["avg_entry_price"] == 0.60


@pytest.mark.asyncio
async def test_api_list_positions(client, engine):
    """GET /api/v1/positions returns paginated list."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as s:
        m = make_market(s)
        await s.flush()
        o = make_outcome(s, m.id)
        await s.commit()
        market_id, outcome_id = m.id, o.id

    await client.post("/api/v1/positions", json=_make_position_payload(market_id, outcome_id))
    await client.post("/api/v1/positions", json=_make_position_payload(market_id, outcome_id, quantity=50))

    resp = await client.get("/api/v1/positions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert len(data["positions"]) == 2


@pytest.mark.asyncio
async def test_api_get_position_detail(client, engine):
    """GET /api/v1/positions/{id} returns position with trades."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as s:
        m = make_market(s)
        await s.flush()
        o = make_outcome(s, m.id)
        await s.commit()
        market_id, outcome_id = m.id, o.id

    create_resp = await client.post(
        "/api/v1/positions",
        json=_make_position_payload(market_id, outcome_id),
    )
    pos_id = create_resp.json()["id"]

    resp = await client.get(f"/api/v1/positions/{pos_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == pos_id
    assert len(data["trades"]) == 1


@pytest.mark.asyncio
async def test_api_position_not_found(client):
    """GET /api/v1/positions/{id} returns 404 for missing position."""
    resp = await client.get(f"/api/v1/positions/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_api_close_position(client, engine):
    """PUT /api/v1/positions/{id}/close closes a position."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as s:
        m = make_market(s)
        await s.flush()
        o = make_outcome(s, m.id)
        await s.commit()
        market_id, outcome_id = m.id, o.id

    create_resp = await client.post(
        "/api/v1/positions",
        json=_make_position_payload(market_id, outcome_id),
    )
    pos_id = create_resp.json()["id"]

    resp = await client.put(
        f"/api/v1/positions/{pos_id}/close",
        json={"quantity": 100, "price": 0.80},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "closed"
    assert abs(data["realized_pnl"] - 20.0) < 0.001


@pytest.mark.asyncio
async def test_api_portfolio_summary(client, engine):
    """GET /api/v1/portfolio/summary returns aggregated stats."""
    resp = await client.get("/api/v1/portfolio/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert "open_positions" in data
    assert "win_rate" in data


@pytest.mark.asyncio
async def test_api_csv_export(client, engine):
    """GET /api/v1/portfolio/export/csv returns CSV content."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as s:
        m = make_market(s)
        await s.flush()
        o = make_outcome(s, m.id)
        await s.commit()
        market_id, outcome_id = m.id, o.id

    await client.post("/api/v1/positions", json=_make_position_payload(market_id, outcome_id))

    resp = await client.get("/api/v1/portfolio/export/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().split("\n")
    assert len(lines) == 2  # header + 1 row
    assert "market_id" in lines[0]
