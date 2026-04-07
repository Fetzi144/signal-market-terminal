"""Integration test: Portfolio full cycle — open → trade → close → P&L check via API."""
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db
from app.main import app
from tests.conftest import make_market, make_outcome


@pytest.mark.asyncio
async def test_portfolio_full_api_flow(engine):
    """Open position → add trades → verify P&L → close → verify realized P&L."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Seed market + outcome
    async with async_sess() as session:
        market = make_market(session, question="Will ETH hit $5k?")
        outcome = make_outcome(session, market.id, name="Yes")
        await session.commit()
        market_id = str(market.id)
        outcome_id = str(outcome.id)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Open position: buy 100 @ 0.40
        resp = await client.post("/api/v1/positions", json={
            "market_id": market_id,
            "outcome_id": outcome_id,
            "platform": "polymarket",
            "side": "yes",
            "quantity": "100",
            "price": "0.40",
        })
        assert resp.status_code == 201
        pos = resp.json()
        position_id = pos["id"]
        assert pos["status"] == "open"
        assert pos["quantity"] == 100.0
        assert pos["avg_entry_price"] == 0.40

        # 2. Add trade: buy 50 more @ 0.50 → weighted avg = (100*0.40 + 50*0.50) / 150
        resp = await client.post(f"/api/v1/positions/{position_id}/trades", json={
            "action": "buy",
            "quantity": "50",
            "price": "0.50",
        })
        assert resp.status_code == 200
        pos = resp.json()
        expected_avg = (100 * 0.40 + 50 * 0.50) / 150  # ≈ 0.4333
        assert abs(pos["avg_entry_price"] - expected_avg) < 0.001
        assert pos["quantity"] == 150.0

        # 3. Partial sell: sell 50 @ 0.60
        resp = await client.post(f"/api/v1/positions/{position_id}/trades", json={
            "action": "sell",
            "quantity": "50",
            "price": "0.60",
        })
        assert resp.status_code == 200
        pos = resp.json()
        assert pos["quantity"] == 100.0
        assert pos["status"] == "open"
        # Realized P&L for this portion: (0.60 - avg_entry) * 50
        partial_pnl = (Decimal("0.60") - Decimal(str(expected_avg))) * 50
        assert pos["realized_pnl"] is not None
        assert abs(pos["realized_pnl"] - float(partial_pnl)) < 0.01

        # 4. Get position detail with trades
        resp = await client.get(f"/api/v1/positions/{position_id}")
        assert resp.status_code == 200
        detail = resp.json()
        assert len(detail["trades"]) == 3  # initial buy + buy + sell
        trade_actions = [t["action"] for t in detail["trades"]]
        assert trade_actions.count("buy") == 2
        assert trade_actions.count("sell") == 1

        # 5. Close remaining: sell 100 @ 0.70
        resp = await client.put(f"/api/v1/positions/{position_id}/close", json={
            "quantity": "100",
            "price": "0.70",
        })
        assert resp.status_code == 200
        pos = resp.json()
        assert pos["status"] == "closed"
        assert pos["quantity"] == 0.0
        assert pos["exit_price"] == 0.70

        # Total realized P&L = partial_pnl + (0.70 - avg_entry) * 100
        final_pnl = float(partial_pnl) + (0.70 - expected_avg) * 100
        assert abs(pos["realized_pnl"] - final_pnl) < 0.01
        assert pos["realized_pnl"] > 0  # We bought low and sold high

        # 6. Portfolio summary should reflect the closed position
        resp = await client.get("/api/v1/portfolio/summary")
        assert resp.status_code == 200
        summary = resp.json()
        assert summary["closed_positions"] == 1
        assert summary["open_positions"] == 0
        assert summary["total_realized_pnl"] > 0
        assert summary["win_rate"] == 100.0  # 1 winning position out of 1

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_portfolio_oversell_rejected(engine):
    """Cannot sell more shares than held."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_sess() as session:
        market = make_market(session, question="Oversell test")
        outcome = make_outcome(session, market.id, name="Yes")
        await session.commit()
        market_id = str(market.id)
        outcome_id = str(outcome.id)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Open with 10 shares
        resp = await client.post("/api/v1/positions", json={
            "market_id": market_id,
            "outcome_id": outcome_id,
            "platform": "polymarket",
            "side": "yes",
            "quantity": "10",
            "price": "0.50",
        })
        position_id = resp.json()["id"]

        # Try to sell 20 — should fail
        resp = await client.put(f"/api/v1/positions/{position_id}/close", json={
            "quantity": "20",
            "price": "0.60",
        })
        assert resp.status_code == 400

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_portfolio_no_side_pnl_inversion(engine):
    """'no' side position inverts P&L correctly."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_sess() as session:
        market = make_market(session, question="No-side test")
        outcome = make_outcome(session, market.id, name="No")
        await session.commit()
        market_id = str(market.id)
        outcome_id = str(outcome.id)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Buy 'no' side at 0.60, close at 0.40 → price dropped so 'no' wins
        resp = await client.post("/api/v1/positions", json={
            "market_id": market_id,
            "outcome_id": outcome_id,
            "platform": "polymarket",
            "side": "no",
            "quantity": "100",
            "price": "0.60",
        })
        position_id = resp.json()["id"]

        resp = await client.put(f"/api/v1/positions/{position_id}/close", json={
            "quantity": "100",
            "price": "0.40",
        })
        assert resp.status_code == 200
        pos = resp.json()
        # For 'no' side: pnl = -(price - avg_entry) * qty = -(0.40 - 0.60) * 100 = +20
        assert pos["realized_pnl"] > 0

    app.dependency_overrides.clear()
