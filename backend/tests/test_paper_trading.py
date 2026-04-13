"""Tests for the paper trading engine: open trades, resolve, metrics."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.paper_trading.engine import (
    attempt_open_trade,
    get_metrics,
    get_pnl_curve,
    get_portfolio_state,
    open_trade,
    resolve_trades,
)
from app.strategy_runs.service import ensure_active_default_strategy_run

from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal


def _make_paper_trade(session, signal_id, outcome_id, market_id, **kwargs):
    defaults = dict(
        id=uuid.uuid4(),
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        direction="buy_yes",
        entry_price=Decimal("0.400000"),
        size_usd=Decimal("500.00"),
        shares=Decimal("1250.0000"),
        status="open",
        opened_at=datetime.now(timezone.utc),
        details={},
    )
    defaults.update(kwargs)
    t = PaperTrade(**defaults)
    session.add(t)
    return t


@pytest.mark.asyncio
async def test_open_trade_positive_ev(session):
    """EV-positive signal opens a paper trade."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session, market.id, outcome.id,
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    await session.commit()

    trade = await open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Test question?",
        fired_at=fired_at,
    )
    await session.commit()

    assert trade is not None
    assert trade.direction == "buy_yes"
    assert trade.entry_price == Decimal("0.410000")
    assert trade.shadow_entry_price == Decimal("0.410000")
    assert trade.size_usd > Decimal("0")
    assert trade.shares > Decimal("0")
    assert trade.status == "open"
    assert trade.details["market_question"] == "Test question?"


@pytest.mark.asyncio
async def test_open_trade_low_ev_rejected(session):
    """EV below threshold is rejected."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(
        session, market.id, outcome.id,
        estimated_probability=Decimal("0.5100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.010000"),
    )
    await session.commit()

    trade = await open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.5100"),
        market_price=Decimal("0.500000"),
    )
    assert trade is None


@pytest.mark.asyncio
async def test_attempt_open_trade_returns_skip_reason(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        estimated_probability=Decimal("0.5100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.010000"),
    )
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.5100"),
        market_price=Decimal("0.500000"),
    )

    assert result.trade is None
    assert result.decision == "skipped"
    assert result.reason_code == "ev_below_threshold"
    assert result.reason_label == "EV below threshold"


@pytest.mark.asyncio
async def test_attempt_open_trade_persists_execution_decision_for_precheck_skip(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=None,
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=signal.fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
        precheck_reason_code="missing_expected_value",
        precheck_reason_label="Missing expected value",
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "missing_expected_value"
    assert result.reason_label == "Missing expected value"
    assert result.execution_decision.reason_code == "missing_expected_value"
    assert result.execution_decision.decision_status == "skipped"


@pytest.mark.asyncio
async def test_attempt_open_trade_persists_strategy_run_and_shadow_execution(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Shadow execution?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is not None
    assert result.execution_decision is not None
    assert result.trade.strategy_run_id == strategy_run.id
    assert result.trade.execution_decision_id == result.execution_decision.id
    assert result.trade.entry_price == Decimal("0.410000")
    assert result.trade.shadow_entry_price == Decimal("0.410000")
    assert result.trade.details["shadow_execution"]["missing_orderbook_context"] is False
    assert result.execution_decision.executable_entry_price == Decimal("0.410000")
    assert result.execution_decision.reason_code == "opened"


@pytest.mark.asyncio
async def test_attempt_open_trade_partial_shadow_fill_when_near_touch_depth_is_thin(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="500",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "200"], ["0.45", "800"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Partial shadow fill?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is not None
    assert result.execution_decision is not None
    shadow = result.trade.details["shadow_execution"]
    assert result.trade.shadow_entry_price == Decimal("0.410000")
    assert result.trade.entry_price == Decimal("0.410000")
    assert result.trade.size_usd == Decimal("207.00")
    assert shadow["fill_status"] == "partial_fill"
    assert shadow["fill_reason"] == "insufficient_near_touch_depth"
    assert shadow["liquidity_constrained"] is True
    assert Decimal(shadow["filled_size_usd"]) == Decimal("207.00")
    assert Decimal(shadow["fill_pct"]) == Decimal("0.4140")
    assert Decimal(shadow["shadow_shares"]) == result.trade.shares


@pytest.mark.asyncio
async def test_attempt_open_trade_shadow_no_fill_when_visible_depth_is_too_small(session, monkeypatch):
    monkeypatch.setattr(settings, "shadow_execution_min_fill_pct", 0.20)
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="50",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "50"], ["0.45", "400"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="No shadow fill?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "execution_partial_fill_below_minimum"
    shadow = result.execution_decision.details["shadow_execution"]
    assert result.execution_decision.executable_entry_price is None
    assert shadow["fill_status"] == "no_fill"
    assert shadow["fill_reason"] == "fill_below_minimum_threshold"
    assert shadow["liquidity_constrained"] is True
    assert Decimal(shadow["filled_size_usd"]) == Decimal("0.00")
    assert Decimal(shadow["shadow_shares"]) == Decimal("0.0000")


@pytest.mark.asyncio
async def test_attempt_open_trade_shadow_no_fill_without_orderbook_context(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Missing orderbook context?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "execution_missing_orderbook_context"
    shadow = result.execution_decision.details["shadow_execution"]
    assert result.execution_decision.executable_entry_price is None
    assert shadow["missing_orderbook_context"] is True
    assert shadow["stale_orderbook_context"] is False
    assert shadow["fill_status"] == "no_fill"
    assert shadow["fill_reason"] == "no_snapshot"


@pytest.mark.asyncio
async def test_attempt_open_trade_shadow_no_fill_with_stale_orderbook_context(session, monkeypatch):
    monkeypatch.setattr(settings, "shadow_execution_max_staleness_seconds", 60)
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at - timedelta(minutes=10),
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Stale orderbook context?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "execution_stale_orderbook_context"
    shadow = result.execution_decision.details["shadow_execution"]
    assert result.execution_decision.executable_entry_price is None
    assert shadow["missing_orderbook_context"] is True
    assert shadow["stale_orderbook_context"] is True
    assert shadow["fill_status"] == "no_fill"
    assert shadow["fill_reason"] == "stale_snapshot"
    assert shadow["snapshot_side"] == "before"
    assert shadow["snapshot_age_seconds"] >= 600


@pytest.mark.asyncio
async def test_attempt_open_trade_skips_when_orderbook_has_no_fillable_depth(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="0",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="No executable depth?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "execution_no_fill"
    shadow = result.execution_decision.details["shadow_execution"]
    assert shadow["fill_status"] == "no_fill"
    assert shadow["fill_reason"] == "no_near_touch_depth"
    assert result.execution_decision.fillable_size_usd == Decimal("0.0000")


@pytest.mark.asyncio
async def test_attempt_open_trade_skips_when_executable_ev_falls_below_threshold(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.5400"),
        price_at_fire=Decimal("0.450000"),
        expected_value=Decimal("0.090000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.1600",
        depth_bid="900",
        depth_ask="900",
        captured_at=fired_at,
        bids=[["0.37", "500"], ["0.36", "400"]],
        asks=[["0.53", "500"], ["0.54", "400"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.5400"),
        market_price=Decimal("0.450000"),
        market_question="Executable EV?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "execution_ev_below_threshold"
    assert result.execution_decision.executable_entry_price == Decimal("0.530000")
    assert result.execution_decision.net_ev_per_share == Decimal("0.010000")


@pytest.mark.asyncio
async def test_attempt_open_trade_skips_duplicate_signal_in_same_strategy_run(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    first_result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Duplicate guard?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    second_result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Duplicate guard?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )

    assert first_result.trade is not None
    assert second_result.trade is None
    assert second_result.reason_code == "already_recorded"
    assert second_result.reason_label == "Already recorded in run"
    count = await session.scalar(
        select(func.count()).select_from(ExecutionDecision).where(
            ExecutionDecision.signal_id == signal.id,
            ExecutionDecision.strategy_run_id == strategy_run.id,
        )
    )
    assert count == 1


@pytest.mark.asyncio
async def test_paper_trade_uniqueness_constraint_is_scoped_to_strategy_run(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=signal.fired_at)

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
    )
    await session.flush()

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        id=uuid.uuid4(),
        status="resolved",
        resolved_at=datetime.now(timezone.utc),
    )

    with pytest.raises(IntegrityError):
        await session.flush()


@pytest.mark.asyncio
async def test_resolve_trades_sets_shadow_pnl(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"]],
        asks=[["0.41", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    trade = await open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Shadow resolution?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    count = await resolve_trades(session, outcome.id, outcome_won=True)
    await session.commit()

    assert count == 1
    assert trade.shadow_pnl is not None
    assert trade.shadow_pnl <= trade.pnl


@pytest.mark.asyncio
async def test_resolve_trades_yes_wins(session):
    """Resolving outcome YES → profit for buy_yes trades."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    trade = _make_paper_trade(
        session, signal.id, outcome.id, market.id,
        direction="buy_yes",
        entry_price=Decimal("0.400000"),
        shares=Decimal("1000.0000"),
    )
    await session.commit()

    count = await resolve_trades(session, outcome.id, outcome_won=True)
    await session.commit()

    assert count == 1
    assert trade.status == "resolved"
    assert trade.exit_price == Decimal("1.000000")
    # P&L = 1000 * (1.0 - 0.4) = 600
    assert trade.pnl == Decimal("600.00")


@pytest.mark.asyncio
async def test_resolve_trades_yes_loses(session):
    """Resolving outcome NO → loss for buy_yes trades."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    trade = _make_paper_trade(
        session, signal.id, outcome.id, market.id,
        direction="buy_yes",
        entry_price=Decimal("0.400000"),
        shares=Decimal("1000.0000"),
    )
    await session.commit()

    count = await resolve_trades(session, outcome.id, outcome_won=False)
    await session.commit()

    assert count == 1
    assert trade.exit_price == Decimal("0.000000")
    # P&L = 1000 * (0.0 - 0.4) = -400
    assert trade.pnl == Decimal("-400.00")


@pytest.mark.asyncio
async def test_resolve_trades_buy_no(session):
    """Resolving outcome NO → profit for buy_no trades."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    trade = _make_paper_trade(
        session, signal.id, outcome.id, market.id,
        direction="buy_no",
        entry_price=Decimal("0.600000"),
        shares=Decimal("500.0000"),
    )
    await session.commit()

    count = await resolve_trades(session, outcome.id, outcome_won=False)
    await session.commit()

    assert count == 1
    assert trade.exit_price == Decimal("1.000000")
    # P&L = 500 * (1.0 - 0.6) = 200
    assert trade.pnl == Decimal("200.00")


@pytest.mark.asyncio
async def test_resolve_no_open_trades(session):
    """No open trades for outcome → zero count."""
    outcome_id = uuid.uuid4()
    count = await resolve_trades(session, outcome_id, outcome_won=True)
    assert count == 0


@pytest.mark.asyncio
async def test_portfolio_state_empty(session):
    """Empty portfolio returns zeros."""
    state = await get_portfolio_state(session)
    assert state["open_exposure"] == Decimal("0")
    assert state["total_resolved"] == 0
    assert state["cumulative_pnl"] == Decimal("0")


@pytest.mark.asyncio
async def test_portfolio_state_with_trades(session):
    """Portfolio state reflects open and resolved trades."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)

    # Open trade
    _make_paper_trade(session, signal.id, outcome.id, market.id, size_usd=Decimal("500.00"))

    # Resolved winning trade
    _make_paper_trade(
        session, signal.id, outcome.id, market.id,
        status="resolved",
        exit_price=Decimal("1.000000"),
        pnl=Decimal("300.00"),
        resolved_at=datetime.now(timezone.utc),
    )
    await session.commit()

    state = await get_portfolio_state(session)
    assert state["open_exposure"] == Decimal("500.00")
    assert state["total_resolved"] == 1
    assert state["cumulative_pnl"] == Decimal("300.00")
    assert state["wins"] == 1


@pytest.mark.asyncio
async def test_metrics_empty(session):
    """Metrics on empty portfolio."""
    metrics = await get_metrics(session)
    assert metrics["total_trades"] == 0
    assert metrics["sharpe_ratio"] == 0.0


@pytest.mark.asyncio
async def test_metrics_with_trades(session):
    """Metrics computed from resolved trades."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    # 3 winning, 1 losing
    for i, pnl_val in enumerate([100, 200, 150, -80]):
        _make_paper_trade(
            session, signal.id, outcome.id, market.id,
            status="resolved",
            pnl=Decimal(str(pnl_val)),
            resolved_at=now + timedelta(hours=i),
            exit_price=Decimal("1.000000") if pnl_val > 0 else Decimal("0.000000"),
        )
    await session.commit()

    metrics = await get_metrics(session)
    assert metrics["total_trades"] == 4
    assert metrics["wins"] == 3
    assert metrics["losses"] == 1
    assert metrics["win_rate"] == 0.75
    assert metrics["cumulative_pnl"] == 370.0
    assert metrics["profit_factor"] is not None
    assert metrics["profit_factor"] > 1.0


@pytest.mark.asyncio
async def test_metrics_include_shadow_and_coverage_counts(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("100.00"),
        shadow_pnl=Decimal("80.00"),
        resolved_at=now,
        details={"shadow_execution": {"liquidity_constrained": True, "missing_orderbook_context": False}},
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("-20.00"),
        shadow_pnl=Decimal("-30.00"),
        resolved_at=now + timedelta(hours=1),
        details={"shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": True}},
    )
    await session.commit()

    metrics = await get_metrics(session)
    assert metrics["shadow_cumulative_pnl"] == 50.0
    assert metrics["shadow_profit_factor"] is not None
    assert metrics["liquidity_constrained_trades"] == 1
    assert metrics["trades_missing_orderbook_context"] == 1


@pytest.mark.asyncio
async def test_attempt_open_trade_ignores_legacy_open_exposure_for_strategy_run(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    legacy_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    _make_paper_trade(
        session,
        legacy_signal.id,
        outcome.id,
        market.id,
        size_usd=Decimal("3000.00"),
        status="open",
        strategy_run_id=None,
    )
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6500"),
        market_price=Decimal("0.400000"),
        market_question="Scoped risk?",
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )

    assert result.trade is not None


@pytest.mark.asyncio
async def test_pnl_curve(session):
    """P&L curve returns cumulative data points."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    for i, pnl_val in enumerate([100, -50, 200]):
        _make_paper_trade(
            session, signal.id, outcome.id, market.id,
            status="resolved",
            pnl=Decimal(str(pnl_val)),
            resolved_at=now + timedelta(hours=i),
            exit_price=Decimal("1.000000"),
        )
    await session.commit()

    curve = await get_pnl_curve(session)
    assert len(curve) == 3
    assert curve[0]["pnl"] == 100.0
    assert curve[1]["pnl"] == 50.0  # 100 + (-50)
    assert curve[2]["pnl"] == 250.0  # 50 + 200
