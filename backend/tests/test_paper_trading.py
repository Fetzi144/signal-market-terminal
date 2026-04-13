"""Tests for the paper trading engine: open trades, resolve, metrics."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import pytest_asyncio

from app.models.paper_trade import PaperTrade
from app.paper_trading.engine import (
    attempt_open_trade,
    get_metrics,
    get_pnl_curve,
    get_portfolio_state,
    open_trade,
    resolve_trades,
)

from tests.conftest import make_market, make_outcome, make_signal


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
    signal = make_signal(
        session, market.id, outcome.id,
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
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
    )
    await session.commit()

    assert trade is not None
    assert trade.direction == "buy_yes"
    assert trade.entry_price == Decimal("0.400000")
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
