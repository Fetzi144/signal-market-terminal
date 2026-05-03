from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from app.models.execution_decision import ExecutionDecision
from app.models.polymarket_stream import PolymarketWatchAsset
from app.reports.profit_operations import run_orderbook_context_repair, run_resolution_accelerator
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_signal
from tests.test_trading_intelligence_api import _make_paper_trade


@pytest.mark.asyncio
async def test_resolution_accelerator_resolves_open_paper_trade_from_signal_resolution(session):
    now = datetime.now(timezone.utc)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=now - timedelta(days=4))
    market = make_market(session, question="Resolvable paper market", end_date=now - timedelta(hours=1))
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=2),
        resolved=True,
        resolved_correctly=True,
        resolution_price=Decimal("1.000000"),
        price_at_fire=Decimal("0.400000"),
    )
    trade = _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="open",
        entry_price=Decimal("0.400000"),
        shares=Decimal("100.0000"),
        size_usd=Decimal("40.00"),
    )
    await session.commit()

    dry_run = await run_resolution_accelerator(session, apply=False)
    assert dry_run["mode"] == "dry_run"
    assert dry_run["resolvable_count"] == 1
    assert dry_run["resolved_trade_count"] == 0

    applied = await run_resolution_accelerator(session, apply=True)
    await session.refresh(trade)
    assert applied["mode"] == "apply"
    assert applied["resolved_trade_count"] == 1
    assert trade.status == "resolved"
    assert trade.pnl == Decimal("60.00")


@pytest.mark.asyncio
async def test_orderbook_context_repair_ensures_watch_assets_for_context_blocked_outcomes(session):
    now = datetime.now(timezone.utc)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=now - timedelta(days=2))
    market = make_market(
        session,
        platform="polymarket",
        question="Context repair market",
        end_date=now + timedelta(days=3),
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="repair-token-1")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(minutes=10),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=signal.fired_at,
            decision_status="skipped",
            action="skip",
            direction="buy_yes",
            net_ev_per_share=Decimal("0.05000000"),
            net_expected_pnl_usd=Decimal("15.00000000"),
            missing_orderbook_context=True,
            reason_code="execution_orderbook_context_unavailable",
            details={"source": "test"},
        )
    )
    await session.commit()

    dry_run = await run_orderbook_context_repair(session, apply=False)
    assert dry_run["mode"] == "dry_run"
    assert dry_run["candidate_outcomes"] == 1
    assert dry_run["watch_assets_ensured"] == 0
    assert dry_run["candidates"][0]["watch_enabled"] is False

    applied = await run_orderbook_context_repair(session, apply=True)
    assert applied["mode"] == "apply"
    assert applied["candidate_outcomes"] == 1
    assert applied["watch_assets_ensured"] == 1

    watch_asset = await session.get(PolymarketWatchAsset, UUID(applied["candidates"][0]["watch_asset_id"]))
    assert watch_asset is not None
    assert watch_asset.outcome_id == outcome.id
    assert watch_asset.watch_enabled is True
    assert watch_asset.watch_reason == "profit_orderbook_context_repair"
    assert watch_asset.priority == 100
