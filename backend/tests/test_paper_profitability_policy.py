from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.config import settings
from app.paper_trading.engine import attempt_open_trade
from app.strategy_runs.service import open_default_strategy_run
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal


def _enable_boring_profit_filter(monkeypatch):
    monkeypatch.setattr(settings, "paper_trading_profitability_filter_enabled", True)
    monkeypatch.setattr(settings, "paper_trading_max_resolution_horizon_days", 30)
    monkeypatch.setattr(settings, "paper_trading_min_market_liquidity_usd", 500.0)
    monkeypatch.setattr(settings, "paper_trading_require_market_end_date", True)


@pytest.mark.asyncio
async def test_profitability_filter_skips_long_dated_paper_trade(session, monkeypatch):
    _enable_boring_profit_filter(monkeypatch)
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        question="Long dated capital trap",
        end_date=now + timedelta(days=90),
        last_liquidity=Decimal("5000.00"),
        last_volume_24h=Decimal("2500.00"),
    )
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now,
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"market_question": "Long dated capital trap", "outcome_name": "Yes"},
    )
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(minutes=1))
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Long dated capital trap",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )

    assert result.trade is None
    assert result.decision == "skipped"
    assert result.reason_code == "profitability_market_long_dated"
    assert result.execution_decision is not None
    assert result.execution_decision.reason_code == "profitability_market_long_dated"


@pytest.mark.asyncio
async def test_profitability_filter_allows_short_horizon_liquid_paper_trade(session, monkeypatch):
    _enable_boring_profit_filter(monkeypatch)
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        question="Short liquid evidence market",
        end_date=now + timedelta(days=5),
        last_liquidity=Decimal("5000.00"),
        last_volume_24h=Decimal("2500.00"),
    )
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now,
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"market_question": "Short liquid evidence market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=now,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(minutes=1))
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Short liquid evidence market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )

    assert result.trade is not None
    assert result.decision == "opened"
    assert result.reason_code == "opened"
