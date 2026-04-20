"""API and integration tests for trading intelligence surfaces."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

import app.paper_trading.analysis as analysis_module
import app.jobs.scheduler as scheduler_module
from app.config import settings
from app.jobs.scheduler import _fetch_overdue_open_trade_resolutions, _resolve_paper_trades, _run_paper_trading
from app.metrics import default_strategy_scheduler_no_active_run
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.paper_trading.engine import ensure_pending_execution_decision
from app.strategy_runs.service import ensure_active_default_strategy_run, open_default_strategy_run
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
        details={"market_question": "Test market?"},
    )
    defaults.update(kwargs)
    trade = PaperTrade(**defaults)
    session.add(trade)
    return trade


def _decimal_json(value) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _datetime_json(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_signal_list_includes_trading_fields_for_ev_capable_signal(client, session):
    market = make_market(session, question="Trading field market")
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(
        session,
        market.id,
        outcome.id,
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Trading field market", "outcome_name": "Yes"},
    )
    await session.commit()

    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    signal = resp.json()["signals"][0]

    assert _decimal_json(signal["expected_value"]) == Decimal("0.250000")
    assert signal["direction"] == "BUY YES"
    assert _decimal_json(signal["edge_pct"]) == Decimal("25.00")
    assert _decimal_json(signal["recommended_size_usd"]) == Decimal("500.00")
    assert float(_decimal_json(signal["kelly_fraction"])) == pytest.approx(0.1042, abs=1e-4)


@pytest.mark.asyncio
async def test_signal_api_exposes_phase0_timing_and_source_fields(client, session):
    market = make_market(session, question="Signal timing market")
    outcome = make_outcome(session, market.id, name="Yes")
    observed_at_exchange = datetime.now(timezone.utc) - timedelta(minutes=6)
    received_at_local = observed_at_exchange + timedelta(seconds=2)
    detected_at_local = received_at_local + timedelta(seconds=1)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        observed_at_exchange=observed_at_exchange,
        received_at_local=received_at_local,
        detected_at_local=detected_at_local,
        source_platform="polymarket",
        source_token_id=outcome.token_id,
        source_event_type="confluence_fusion",
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Signal timing market", "outcome_name": "Yes"},
    )
    await session.commit()

    resp = await client.get(f"/api/v1/signals/{signal.id}")
    assert resp.status_code == 200
    data = resp.json()

    assert _datetime_json(data["observed_at_exchange"]) == observed_at_exchange
    assert _datetime_json(data["received_at_local"]) == received_at_local
    assert _datetime_json(data["detected_at_local"]) == detected_at_local
    assert data["source_platform"] == "polymarket"
    assert data["source_token_id"] == outcome.token_id
    assert data["source_event_type"] == "confluence_fusion"
    assert data["display_signal_type"] == "confluence"
    assert data["review_family"] == "default_strategy"
    assert data["review_family_posture"] == "benchmark_only"


@pytest.mark.asyncio
async def test_signal_detail_returns_null_trading_fields_without_probability_inputs(client, session):
    market = make_market(session, question="No trading fields market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(session, market.id, outcome.id)
    await session.commit()

    resp = await client.get(f"/api/v1/signals/{signal.id}")
    assert resp.status_code == 200
    data = resp.json()

    assert data["expected_value"] is None
    assert data["direction"] is None
    assert data["edge_pct"] is None
    assert data["recommended_size_usd"] is None
    assert data["kelly_fraction"] is None


@pytest.mark.asyncio
async def test_paper_trading_portfolio_empty(client):
    resp = await client.get("/api/v1/paper-trading/portfolio")
    assert resp.status_code == 200
    data = resp.json()

    assert data["open_trades"] == []
    assert data["open_exposure"] == 0.0
    assert data["total_resolved"] == 0
    assert data["cumulative_pnl"] == 0.0
    assert data["win_rate"] == 0.0


@pytest.mark.asyncio
async def test_paper_trading_portfolio_populated(client, session):
    market = make_market(session, question="Portfolio market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(session, market.id, outcome.id)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=signal.fired_at)
    execution_decision = ExecutionDecision(
        id=uuid.uuid4(),
        signal_id=signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=datetime.now(timezone.utc),
        decision_status="opened",
        action="cross",
        direction="buy_yes",
        executable_entry_price=Decimal("0.41000000"),
        reason_code="opened",
        details={},
    )
    session.add(execution_decision)

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        direction="buy_yes",
        size_usd=Decimal("450.00"),
        status="open",
        execution_decision_id=execution_decision.id,
        submitted_at=datetime.now(timezone.utc) - timedelta(seconds=3),
        confirmed_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        direction="buy_no",
        status="resolved",
        exit_price=Decimal("1.000000"),
        pnl=Decimal("120.00"),
        resolved_at=datetime.now(timezone.utc),
    )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/portfolio")
    assert resp.status_code == 200
    data = resp.json()

    assert data["open_exposure"] == 450.0
    assert len(data["open_trades"]) == 1
    assert data["open_trades"][0]["direction"] == "buy_yes"
    assert data["open_trades"][0]["execution_decision_id"] == str(execution_decision.id)
    assert data["open_trades"][0]["submitted_at"] is not None
    assert data["open_trades"][0]["confirmed_at"] is not None
    assert data["total_resolved"] == 1
    assert data["cumulative_pnl"] == 120.0
    assert data["wins"] == 1


@pytest.mark.asyncio
async def test_paper_trading_history_supports_filters_and_pagination(client, session):
    market = make_market(session, question="History market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        id=uuid.uuid4(),
        direction="buy_yes",
        status="resolved",
        pnl=Decimal("100.00"),
        resolved_at=now,
        opened_at=now - timedelta(hours=3),
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        id=uuid.uuid4(),
        direction="buy_no",
        status="resolved",
        pnl=Decimal("80.00"),
        resolved_at=now + timedelta(hours=1),
        opened_at=now - timedelta(hours=2),
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        id=uuid.uuid4(),
        direction="buy_yes",
        status="open",
        opened_at=now - timedelta(hours=1),
    )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/history?status=resolved&page=1&page_size=1")
    assert resp.status_code == 200
    page_one = resp.json()
    assert page_one["total"] == 2
    assert len(page_one["trades"]) == 1

    resp = await client.get("/api/v1/paper-trading/history?status=resolved&page=2&page_size=1")
    assert resp.status_code == 200
    page_two = resp.json()
    assert len(page_two["trades"]) == 1
    assert page_one["trades"][0]["id"] != page_two["trades"][0]["id"]

    resp = await client.get("/api/v1/paper-trading/history?direction=buy_no")
    assert resp.status_code == 200
    filtered = resp.json()
    assert filtered["total"] == 1
    assert filtered["trades"][0]["direction"] == "buy_no"


@pytest.mark.asyncio
async def test_paper_trading_metrics_endpoint(client, session):
    market = make_market(session, question="Metrics market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    for i, pnl_value in enumerate((Decimal("110.00"), Decimal("-40.00"), Decimal("90.00"))):
        _make_paper_trade(
            session,
            signal.id,
            outcome.id,
            market.id,
            id=uuid.uuid4(),
            status="resolved",
            pnl=pnl_value,
            exit_price=Decimal("1.000000") if pnl_value > 0 else Decimal("0.000000"),
            resolved_at=now + timedelta(hours=i),
        )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/metrics")
    assert resp.status_code == 200
    data = resp.json()

    assert data["total_trades"] == 3
    assert data["wins"] == 2
    assert data["losses"] == 1
    assert data["cumulative_pnl"] == 160.0
    assert data["profit_factor"] is not None


@pytest.mark.asyncio
async def test_paper_trading_pnl_curve_endpoint(client, session):
    market = make_market(session, question="Curve market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(session, market.id, outcome.id)
    now = datetime.now(timezone.utc)

    for i, pnl_value in enumerate((Decimal("100.00"), Decimal("-25.00"))):
        _make_paper_trade(
            session,
            signal.id,
            outcome.id,
            market.id,
            id=uuid.uuid4(),
            status="resolved",
            pnl=pnl_value,
            exit_price=Decimal("1.000000") if pnl_value > 0 else Decimal("0.000000"),
            resolved_at=now + timedelta(hours=i),
        )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/pnl-curve")
    assert resp.status_code == 200
    data = resp.json()

    assert len(data) == 2
    assert data[0]["pnl"] == 100.0
    assert data[1]["pnl"] == 75.0


@pytest.mark.asyncio
async def test_scheduler_paper_trade_lifecycle_reflected_in_api(client, session):
    market = make_market(session, question="Lifecycle market")
    outcome = make_outcome(session, market.id, name="Yes")
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Lifecycle market", "outcome_name": "Yes"},
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
    await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    await session.commit()

    await _run_paper_trading(session, [signal])

    resp = await client.get("/api/v1/paper-trading/portfolio")
    assert resp.status_code == 200
    portfolio = resp.json()
    assert len(portfolio["open_trades"]) == 1
    assert portfolio["open_trades"][0]["status"] == "open"

    await _resolve_paper_trades(session, [{"outcomes": [{"id": str(outcome.id), "won": True}]}])

    resp = await client.get("/api/v1/paper-trading/metrics")
    assert resp.status_code == 200
    metrics = resp.json()
    assert metrics["total_trades"] == 1
    assert metrics["wins"] == 1
    assert metrics["cumulative_pnl"] > 0

    resp = await client.get("/api/v1/paper-trading/pnl-curve")
    assert resp.status_code == 200
    curve = resp.json()
    assert len(curve) == 1
    assert curve[0]["pnl"] > 0


@pytest.mark.asyncio
async def test_scheduler_resolves_paper_trade_from_polymarket_flat_payload(session):
    market = make_market(session, platform="polymarket", platform_id="pm-flat-market", question="Flat payload market")
    outcome_yes = make_outcome(session, market.id, name="Yes")
    make_outcome(session, market.id, name="No")
    signal = make_signal(
        session,
        market.id,
        outcome_yes.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Flat payload market", "outcome_name": "Yes"},
    )
    trade = _make_paper_trade(session, signal.id, outcome_yes.id, market.id)
    await session.commit()

    await _resolve_paper_trades(
        session,
        [{"platform_id": "pm-flat-market", "winner": "Yes", "winning_outcome_id": "Yes"}],
        platform="polymarket",
    )

    await session.refresh(trade)
    assert trade.status == "resolved"
    assert trade.exit_price == Decimal("1.000000")
    assert trade.pnl == Decimal("750.00")


@pytest.mark.asyncio
async def test_scheduler_resolves_paper_trade_from_kalshi_flat_payload(session):
    market = make_market(session, platform="kalshi", platform_id="KXTEST-SETTLED", question="Kalshi flat payload market")
    outcome_yes = make_outcome(session, market.id, name="Yes", platform_outcome_id="KXTEST-SETTLED_yes")
    make_outcome(session, market.id, name="No", platform_outcome_id="KXTEST-SETTLED_no")
    signal = make_signal(
        session,
        market.id,
        outcome_yes.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Kalshi flat payload market", "outcome_name": "Yes"},
    )
    trade = _make_paper_trade(session, signal.id, outcome_yes.id, market.id)
    await session.commit()

    await _resolve_paper_trades(
        session,
        [{"platform_id": "KXTEST-SETTLED", "winning_outcome": "yes"}],
        platform="kalshi",
    )

    await session.refresh(trade)
    assert trade.status == "resolved"
    assert trade.exit_price == Decimal("1.000000")
    assert trade.pnl == Decimal("750.00")


@pytest.mark.asyncio
async def test_scheduler_fetches_targeted_overdue_kalshi_resolutions(session):
    now = datetime.now(timezone.utc)
    overdue_market = make_market(
        session,
        platform="kalshi",
        platform_id="KXOVERDUE-SETTLED",
        question="Overdue Kalshi market",
        end_date=now - timedelta(days=2),
        active=True,
    )
    overdue_outcome = make_outcome(
        session,
        overdue_market.id,
        name="Yes",
        platform_outcome_id="KXOVERDUE-SETTLED_yes",
    )
    overdue_signal = make_signal(
        session,
        overdue_market.id,
        overdue_outcome.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Overdue Kalshi market", "outcome_name": "Yes"},
    )
    overdue_trade = _make_paper_trade(session, overdue_signal.id, overdue_outcome.id, overdue_market.id)

    fresh_market = make_market(
        session,
        platform="kalshi",
        platform_id="KXFUTURE-OPEN",
        question="Future Kalshi market",
        end_date=now + timedelta(days=2),
        active=True,
    )
    fresh_outcome = make_outcome(
        session,
        fresh_market.id,
        name="Yes",
        platform_outcome_id="KXFUTURE-OPEN_yes",
    )
    fresh_signal = make_signal(
        session,
        fresh_market.id,
        fresh_outcome.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Future Kalshi market", "outcome_name": "Yes"},
    )
    _make_paper_trade(session, fresh_signal.id, fresh_outcome.id, fresh_market.id)
    await session.commit()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    class _FakeKalshiConnector:
        api_base = "https://example.test"

        def __init__(self):
            self.calls = []

        async def _request_with_retry(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            return _FakeResponse(
                {
                    "markets": [
                        {
                            "ticker": "KXOVERDUE-SETTLED",
                            "status": "finalized",
                            "result": "yes",
                        }
                    ]
                }
            )

    connector = _FakeKalshiConnector()
    resolved_markets = await _fetch_overdue_open_trade_resolutions(
        session,
        connector,
        platform="kalshi",
    )

    assert resolved_markets == [{"platform_id": "KXOVERDUE-SETTLED", "winning_outcome": "yes"}]
    assert len(connector.calls) == 1
    assert "KXOVERDUE-SETTLED" in connector.calls[0][2]["params"]["tickers"]
    assert "KXFUTURE-OPEN" not in connector.calls[0][2]["params"]["tickers"]

    await _resolve_paper_trades(session, resolved_markets, platform="kalshi")
    await session.refresh(overdue_trade)
    assert overdue_trade.status == "resolved"
    assert overdue_trade.exit_price == Decimal("1.000000")


@pytest.mark.asyncio
async def test_strategy_health_overdue_backlog_clears_after_resolution_pass(client, session):
    now = datetime.now(timezone.utc)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=1))
    market = make_market(
        session,
        platform="kalshi",
        platform_id="KXBACKLOG-CLEAR",
        question="Backlog clear market",
        end_date=now - timedelta(hours=2),
        active=True,
    )
    outcome_yes = make_outcome(
        session,
        market.id,
        name="Yes",
        platform_outcome_id="KXBACKLOG-CLEAR_yes",
    )
    make_outcome(
        session,
        market.id,
        name="No",
        platform_outcome_id="KXBACKLOG-CLEAR_no",
    )
    signal = make_signal(
        session,
        market.id,
        outcome_yes.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Backlog clear market", "outcome_name": "Yes"},
    )
    trade = _make_paper_trade(
        session,
        signal.id,
        outcome_yes.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="open",
    )
    await session.commit()

    before = await client.get("/api/v1/paper-trading/strategy-health")
    assert before.status_code == 200
    assert before.json()["headline"]["overdue_open_trades"] == 1

    await _resolve_paper_trades(
        session,
        [{"platform_id": "KXBACKLOG-CLEAR", "winning_outcome": "yes"}],
        platform="kalshi",
    )

    after = await client.get("/api/v1/paper-trading/strategy-health")
    assert after.status_code == 200
    assert after.json()["headline"]["overdue_open_trades"] == 0

    await session.refresh(trade)
    assert trade.status == "resolved"


@pytest.mark.asyncio
async def test_scheduler_only_auto_trades_default_strategy_signals(client, session):
    market = make_market(session, question="Default strategy gate")
    outcome = make_outcome(session, market.id, name="Yes")
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)

    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Default strategy gate", "outcome_name": "Yes"},
    )
    confluence_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        timeframe="1h",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=(fired_at.minute // 15) * 15, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Default strategy gate", "outcome_name": "Yes"},
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
    await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    await session.commit()

    result = await session.execute(select(PaperTrade).order_by(PaperTrade.opened_at.desc()))
    assert result.scalars().all() == []

    signal_result = await session.execute(select(Signal).order_by(Signal.fired_at.asc()))
    signals = signal_result.scalars().all()
    await _run_paper_trading(session, signals)

    resp = await client.get("/api/v1/paper-trading/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["trades"][0]["signal_id"] == str(confluence_signal.id)


@pytest.mark.asyncio
async def test_scheduler_retries_pending_execution_when_orderbook_context_arrives(session):
    market = make_market(session, question="Execution skip market")
    outcome = make_outcome(session, market.id, name="Yes")
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Execution skip market", "outcome_name": "Yes"},
    )
    await open_default_strategy_run(session, launch_boundary_at=signal.fired_at - timedelta(minutes=1))
    await session.commit()

    await _run_paper_trading(session, [signal])
    await session.refresh(signal)

    metadata = signal.details["default_strategy"]
    assert metadata["decision"] == "pending_decision"
    assert metadata["reason_code"] == "execution_missing_orderbook_context"
    assert metadata["reason_label"] == "Missing orderbook context"
    assert metadata["trade_id"] is None

    execution_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == signal.id)
    )
    assert execution_decision is not None
    assert execution_decision.decision_status == "pending_decision"
    assert execution_decision.reason_code == "execution_missing_orderbook_context"

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

    await _run_paper_trading(session, [])
    await session.refresh(signal)
    await session.refresh(execution_decision)

    metadata = signal.details["default_strategy"]
    assert metadata["attempt_kind"] == "retry"
    assert metadata["decision"] == "opened"
    assert metadata["reason_code"] == "opened"
    assert metadata["trade_id"] is not None
    assert execution_decision.decision_status == "opened"


@pytest.mark.asyncio
async def test_scheduler_records_skip_reason_for_in_window_non_trade(session):
    market = make_market(session, question="Skip reason market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        estimated_probability=Decimal("0.5100"),
        probability_adjustment=Decimal("0.0100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.010000"),
        details={"direction": "up", "market_question": "Skip reason market", "outcome_name": "Yes"},
    )
    await open_default_strategy_run(session, launch_boundary_at=signal.fired_at - timedelta(minutes=1))
    await session.commit()

    await _run_paper_trading(session, [signal])
    await session.refresh(signal)

    metadata = signal.details["default_strategy"]
    assert metadata["decision"] == "skipped"
    assert metadata["reason_code"] == "ev_below_threshold"
    assert metadata["reason_label"] == "EV below threshold"
    assert metadata["trade_id"] is None
    execution_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == signal.id)
    )
    assert execution_decision is not None
    assert execution_decision.reason_code == "ev_below_threshold"


@pytest.mark.asyncio
async def test_scheduler_expires_stale_pending_execution_decisions_before_retry(session, monkeypatch):
    now = datetime.now(timezone.utc)
    market = make_market(session, question="Pending expiry market")
    outcome = make_outcome(session, market.id, name="Yes")
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(hours=1))

    stale_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        timeframe="stale_pending",
        fired_at=now - timedelta(minutes=25),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Pending expiry market", "outcome_name": "Yes"},
    )
    retry_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        timeframe="retry_pending",
        fired_at=now - timedelta(minutes=4),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Pending expiry market", "outcome_name": "Yes"},
    )
    await session.flush()

    session.add_all(
        [
            ExecutionDecision(
                id=uuid.uuid4(),
                signal_id=stale_signal.id,
                strategy_run_id=strategy_run.id,
                decision_at=now - timedelta(minutes=20),
                decision_status="pending_decision",
                action="pending",
                reason_code="execution_missing_orderbook_context",
                missing_orderbook_context=True,
                details={"reason_label": "Missing orderbook context", "detail": "Waiting for orderbook context"},
            ),
            ExecutionDecision(
                id=uuid.uuid4(),
                signal_id=retry_signal.id,
                strategy_run_id=strategy_run.id,
                decision_at=now - timedelta(minutes=2),
                decision_status="pending_decision",
                action="pending",
                reason_code="execution_missing_orderbook_context",
                missing_orderbook_context=True,
                details={"reason_label": "Missing orderbook context", "detail": "Waiting for orderbook context"},
            ),
        ]
    )
    await session.commit()

    monkeypatch.setattr(settings, "paper_trading_pending_decision_max_age_seconds", 300)

    await _run_paper_trading(session, [])

    stale_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == stale_signal.id)
    )
    retry_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == retry_signal.id)
    )
    await session.refresh(stale_signal)
    await session.refresh(retry_signal)

    assert stale_decision is not None
    assert stale_decision.decision_status == "skipped"
    assert stale_decision.reason_code == "pending_decision_expired"
    assert stale_decision.details["expired_pending_reason_code"] == "execution_missing_orderbook_context"
    assert stale_decision.details["diagnostics"]["retry_window_seconds"] == 300

    stale_metadata = stale_signal.details["default_strategy"]
    assert stale_metadata["attempt_kind"] == "pending_expiry"
    assert stale_metadata["eligible"] is True
    assert stale_metadata["decision"] == "skipped"
    assert stale_metadata["reason_code"] == "pending_decision_expired"
    assert stale_metadata["diagnostics"]["expired_pending_reason_code"] == "execution_missing_orderbook_context"

    assert retry_decision is not None
    assert retry_decision.decision_status == "pending_decision"
    assert retry_decision.reason_code == "execution_missing_orderbook_context"

    retry_metadata = retry_signal.details["default_strategy"]
    assert retry_metadata["attempt_kind"] == "retry"
    assert retry_metadata["decision"] == "pending_decision"
    assert retry_metadata["reason_code"] == "execution_missing_orderbook_context"


@pytest.mark.asyncio
async def test_scheduler_limits_paper_trading_retry_and_repair_backlogs(session, monkeypatch):
    market = make_market(session, question="Backlog throttle market")
    outcome = make_outcome(session, market.id, name="Yes")
    base_time = datetime.now(timezone.utc) - timedelta(minutes=10)

    strategy_run = await open_default_strategy_run(session, launch_boundary_at=base_time - timedelta(minutes=1))

    fresh_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        timeframe="fresh",
        fired_at=base_time + timedelta(minutes=3),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Backlog throttle market", "outcome_name": "Yes"},
    )
    pending_signals = [
        make_signal(
            session,
            market.id,
            outcome.id,
            signal_type="confluence",
            timeframe=f"retry_{index}",
            fired_at=base_time + timedelta(minutes=index),
            estimated_probability=Decimal("0.6500"),
            probability_adjustment=Decimal("0.2500"),
            price_at_fire=Decimal("0.400000"),
            expected_value=Decimal("0.250000"),
            details={"direction": "up", "market_question": "Backlog throttle market", "outcome_name": "Yes"},
        )
        for index in (1, 2)
    ]
    backlog_signals = [
        make_signal(
            session,
            market.id,
            outcome.id,
            signal_type="confluence",
            timeframe=f"repair_{index}",
            fired_at=base_time + timedelta(minutes=4 + index),
            estimated_probability=Decimal("0.6500"),
            probability_adjustment=Decimal("0.2500"),
            price_at_fire=Decimal("0.400000"),
            expected_value=Decimal("0.250000"),
            details={"direction": "up", "market_question": "Backlog throttle market", "outcome_name": "Yes"},
        )
        for index in (0, 1)
    ]
    await session.flush()

    for signal in pending_signals:
        await ensure_pending_execution_decision(
            session=session,
            signal_id=signal.id,
            outcome_id=signal.outcome_id,
            market_id=signal.market_id,
            estimated_probability=signal.estimated_probability,
            market_price=signal.price_at_fire,
            market_question=(signal.details or {}).get("market_question", ""),
            fired_at=signal.fired_at,
            strategy_run_id=strategy_run.id,
        )
    await session.commit()

    monkeypatch.setattr(scheduler_module, "PAPER_TRADING_PENDING_RETRY_BATCH_SIZE", 1)
    monkeypatch.setattr(scheduler_module, "PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE", 1)

    def fake_evaluate_default_strategy_signal(signal, *, started_at):
        return SimpleNamespace(
            signal_type_match=True,
            in_window=True,
            eligible=True,
            reason_code="eligible",
            reason_label="Eligible",
        )

    attempt_log: list[tuple[uuid.UUID, str | None]] = []

    async def fake_attempt_open_trade(
        session,
        *,
        signal_id,
        outcome_id,
        market_id,
        estimated_probability,
        market_price,
        market_question,
        fired_at,
        strategy_run_id,
        precheck_reason_code,
        precheck_reason_label,
    ):
        signal = await session.get(Signal, signal_id)
        attempt_kind = ((signal.details or {}).get("default_strategy") or {}).get("attempt_kind")
        attempt_log.append((signal_id, attempt_kind))
        return SimpleNamespace(
            trade=None,
            decision="skipped",
            reason_code="throttled_test_skip",
            reason_label="Throttled test skip",
            detail=None,
            diagnostics=None,
        )

    monkeypatch.setattr("app.default_strategy.evaluate_default_strategy_signal", fake_evaluate_default_strategy_signal)
    monkeypatch.setattr("app.paper_trading.engine.attempt_open_trade", fake_attempt_open_trade)

    await _run_paper_trading(session, [fresh_signal])

    await session.refresh(fresh_signal)
    for signal in pending_signals + backlog_signals:
        await session.refresh(signal)

    processed_pending = [
        signal for signal in pending_signals
        if ((signal.details or {}).get("default_strategy") or {}).get("attempt_kind") == "retry"
    ]
    processed_backlog = [
        signal for signal in backlog_signals
        if ((signal.details or {}).get("default_strategy") or {}).get("attempt_kind") == "backlog_repair"
    ]

    assert fresh_signal.details["default_strategy"]["attempt_kind"] == "fresh_signal"
    assert len(processed_pending) == 1
    assert len(processed_backlog) == 1
    assert len(attempt_log) == 3


@pytest.mark.asyncio
async def test_scheduler_backfills_missing_execution_decision_from_signal_metadata(session):
    market = make_market(session, question="Metadata backfill market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=datetime.now(timezone.utc) - timedelta(hours=1),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Metadata backfill market", "outcome_name": "Yes"},
    )
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=signal.fired_at - timedelta(minutes=1))
    signal.details = {
        **(signal.details or {}),
        "default_strategy": {
            "strategy_name": settings.default_strategy_name,
            "strategy_run_id": str(strategy_run.id),
            "baseline_start_at": strategy_run.started_at.isoformat(),
            "evaluated_at": signal.fired_at.isoformat(),
            "eligible": True,
            "decision": "skipped",
            "reason_code": "risk_total_exposure",
            "reason_label": "Total exposure limit reached",
            "detail": "Total exposure limit reached ($3000.00 / $3000.00)",
            "trade_id": None,
            "diagnostics": {
                "direction": "buy_yes",
                "approved_size_usd": "0",
                "recommended_size_usd": "500.00",
                "drawdown_active": False,
            },
        },
    }
    await session.commit()

    await _run_paper_trading(session, [])

    execution_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == signal.id)
    )
    assert execution_decision is not None
    assert execution_decision.decision_status == "skipped"
    assert execution_decision.reason_code == "risk_total_exposure"
    assert execution_decision.details["risk_result"]["risk_scope"] == "local_paper_book"


@pytest.mark.asyncio
async def test_scheduler_backfills_opened_execution_decision_and_links_trade_from_signal_metadata(session):
    market = make_market(session, question="Opened metadata backfill market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=datetime.now(timezone.utc) - timedelta(hours=1),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Opened metadata backfill market", "outcome_name": "Yes"},
    )
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=signal.fired_at - timedelta(minutes=1))
    trade = _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        execution_decision_id=None,
        status="open",
        entry_price=Decimal("0.410000"),
        size_usd=Decimal("250.00"),
        shares=Decimal("609.7561"),
        opened_at=signal.fired_at,
    )
    signal.details = {
        **(signal.details or {}),
        "default_strategy": {
            "strategy_name": settings.default_strategy_name,
            "strategy_run_id": str(strategy_run.id),
            "baseline_start_at": strategy_run.started_at.isoformat(),
            "evaluated_at": signal.fired_at.isoformat(),
            "eligible": True,
            "decision": "opened",
            "reason_code": "opened",
            "reason_label": "Trade opened",
            "detail": None,
            "trade_id": str(trade.id),
            "diagnostics": {
                "direction": "buy_yes",
                "approved_size_usd": "250.00",
                "recommended_size_usd": "250.00",
                "ev_per_share": "0.120000",
                "missing_orderbook_context": False,
            },
        },
    }
    await session.commit()

    await _run_paper_trading(session, [])
    await session.refresh(trade)

    execution_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == signal.id)
    )
    assert execution_decision is not None
    assert execution_decision.decision_status == "opened"
    assert execution_decision.reason_code == "opened"
    assert trade.execution_decision_id == execution_decision.id


@pytest.mark.asyncio
async def test_scheduler_repairs_missing_execution_decision_backlog_for_qualified_signal(session):
    market = make_market(session, question="Backlog repair market")
    outcome = make_outcome(session, market.id, name="Yes")
    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Backlog repair market", "outcome_name": "Yes"},
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
    await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    await session.commit()

    await _run_paper_trading(session, [])
    await session.refresh(signal)

    execution_decision = await session.scalar(
        select(ExecutionDecision).where(ExecutionDecision.signal_id == signal.id)
    )
    assert execution_decision is not None
    assert execution_decision.reason_code == "opened"
    assert signal.details["default_strategy"]["attempt_kind"] == "backlog_repair"


@pytest.mark.asyncio
async def test_scheduler_does_not_bootstrap_run_or_stamp_metadata_without_active_run(session):
    market = make_market(session, question="No bootstrap market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "No bootstrap market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=signal.fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    await session.commit()

    await _run_paper_trading(session, [signal])
    await session.refresh(signal)

    assert await session.scalar(select(PaperTrade.id).limit(1)) is None
    assert await session.scalar(select(ExecutionDecision.id).limit(1)) is None
    assert "default_strategy" not in (signal.details or {})


@pytest.mark.asyncio
async def test_scheduler_no_active_run_metric_increments_even_when_no_signals_are_available(session):
    before = default_strategy_scheduler_no_active_run._value.get()

    await _run_paper_trading(session, [])

    after = default_strategy_scheduler_no_active_run._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_legacy_phase0_null_rows_still_serialize_and_query_cleanly(client, session):
    market = make_market(session, question="Legacy compatibility market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        details={"direction": "up", "market_question": "Legacy compatibility market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        status="open",
        execution_decision_id=None,
        submitted_at=None,
        confirmed_at=None,
    )
    await session.commit()

    resp = await client.get(f"/api/v1/signals/{signal.id}")
    assert resp.status_code == 200
    signal_data = resp.json()
    assert signal_data["observed_at_exchange"] is None
    assert signal_data["received_at_local"] is None
    assert signal_data["detected_at_local"] is None
    assert signal_data["source_platform"] is None
    assert signal_data["source_token_id"] is None
    assert signal_data["source_event_type"] is None

    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    listed_signal = resp.json()["signals"][0]
    assert listed_signal["observed_at_exchange"] is None
    assert listed_signal["received_at_local"] is None

    resp = await client.get("/api/v1/paper-trading/portfolio")
    assert resp.status_code == 200
    portfolio = resp.json()
    assert len(portfolio["open_trades"]) == 1
    assert portfolio["open_trades"][0]["execution_decision_id"] is None
    assert portfolio["open_trades"][0]["submitted_at"] is None
    assert portfolio["open_trades"][0]["confirmed_at"] is None

    resp = await client.get("/api/v1/paper-trading/history")
    assert resp.status_code == 200
    history = resp.json()
    assert history["total"] == 1
    assert history["trades"][0]["execution_decision_id"] is None


@pytest.mark.asyncio
async def test_scoped_paper_trading_endpoints_only_measure_default_strategy(client, session):
    market = make_market(session, question="Scoped strategy market")
    outcome = make_outcome(session, market.id, name="Yes")
    now = datetime.now(timezone.utc)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=now - timedelta(days=1))

    confluence_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        estimated_probability=Decimal("0.6700"),
        probability_adjustment=Decimal("0.1700"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.170000"),
        details={"direction": "up", "market_question": "Scoped strategy market", "outcome_name": "Yes"},
    )
    legacy_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        details={"direction": "up", "market_question": "Scoped strategy market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        confluence_signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("100.00"),
        shadow_pnl=Decimal("90.00"),
        shadow_entry_price=Decimal("0.520000"),
        exit_price=Decimal("1.000000"),
        resolved_at=now,
        opened_at=now - timedelta(hours=3),
        details={
            "market_question": "Scoped strategy market",
            "ev_per_share": "0.170000",
            "shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": False},
        },
    )
    _make_paper_trade(
        session,
        legacy_signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("40.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now + timedelta(hours=1),
        opened_at=now - timedelta(hours=2),
        details={"market_question": "Scoped strategy market", "ev_per_share": "0.120000"},
    )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/portfolio?scope=default_strategy")
    assert resp.status_code == 200
    portfolio = resp.json()
    assert portfolio["total_resolved"] == 1
    assert portfolio["cumulative_pnl"] == 100.0
    assert portfolio["open_trades"] == []

    resp = await client.get("/api/v1/paper-trading/history?scope=default_strategy")
    assert resp.status_code == 200
    history = resp.json()
    assert history["total"] == 1
    assert history["trades"][0]["signal_id"] == str(confluence_signal.id)

    resp = await client.get("/api/v1/paper-trading/metrics?scope=default_strategy")
    assert resp.status_code == 200
    metrics = resp.json()
    assert metrics["total_trades"] == 1
    assert metrics["cumulative_pnl"] == 100.0
    assert metrics["shadow_cumulative_pnl"] == 90.0

    resp = await client.get("/api/v1/paper-trading/pnl-curve?scope=default_strategy")
    assert resp.status_code == 200
    curve = resp.json()
    assert len(curve) == 1
    assert curve[0]["pnl"] == 100.0


@pytest.mark.asyncio
async def test_default_strategy_dashboard_matches_scoped_read_endpoints(client, session):
    market = make_market(session, question="Dashboard strategy market")
    outcome = make_outcome(session, market.id, name="Yes")
    now = datetime.now(timezone.utc)
    opened_at = now - timedelta(days=2)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=opened_at)

    confluence_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=opened_at,
        dedupe_bucket=opened_at.replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.050000"),
        profit_loss=Decimal("0.100000"),
        details={"direction": "up", "market_question": "Dashboard strategy market", "outcome_name": "Yes"},
    )
    execution_decision = ExecutionDecision(
        id=uuid.uuid4(),
        signal_id=confluence_signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=opened_at,
        decision_status="opened",
        action="cross",
        direction="buy_yes",
        executable_entry_price=Decimal("0.50000000"),
        reason_code="opened",
        details={"source": "test"},
    )
    session.add(execution_decision)
    _make_paper_trade(
        session,
        confluence_signal.id,
        outcome.id,
        market.id,
        execution_decision_id=execution_decision.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("100.00"),
        shadow_pnl=Decimal("90.00"),
        shadow_entry_price=Decimal("0.520000"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(hours=4),
        opened_at=opened_at,
        details={
            "market_question": "Dashboard strategy market",
            "ev_per_share": "0.120000",
            "shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": False},
        },
    )
    await session.commit()

    dashboard = await client.get("/api/v1/paper-trading/default-strategy/dashboard")
    portfolio = await client.get("/api/v1/paper-trading/portfolio?scope=default_strategy")
    metrics = await client.get("/api/v1/paper-trading/metrics?scope=default_strategy")
    curve = await client.get("/api/v1/paper-trading/pnl-curve?scope=default_strategy")
    health = await client.get("/api/v1/paper-trading/strategy-health")

    assert dashboard.status_code == 200
    assert portfolio.status_code == 200
    assert metrics.status_code == 200
    assert curve.status_code == 200
    assert health.status_code == 200

    payload = dashboard.json()
    assert payload["portfolio"] == portfolio.json()
    assert payload["metrics"] == metrics.json()
    assert payload["pnl_curve"] == curve.json()
    assert payload["strategy_health"] == health.json()


@pytest.mark.asyncio
async def test_review_verdict_remains_default_strategy_scoped_when_benchmark_is_negative(client, session, monkeypatch):
    now = datetime.now(timezone.utc)
    started_at = now - timedelta(days=20)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=started_at)
    market = make_market(session, question="Scoped verdict market")
    outcome = make_outcome(session, market.id, name="Yes")

    default_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=1),
        dedupe_bucket=(started_at + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6400"),
        probability_adjustment=Decimal("0.1400"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.140000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.060000"),
        profit_loss=Decimal("0.110000"),
        details={"direction": "up", "market_question": "Scoped verdict market", "outcome_name": "Yes"},
    )
    execution_decision = ExecutionDecision(
        id=uuid.uuid4(),
        signal_id=default_signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=started_at + timedelta(hours=1),
        decision_status="opened",
        action="cross",
        direction="buy_yes",
        executable_entry_price=Decimal("0.50000000"),
        reason_code="opened",
        details={"source": "test"},
    )
    session.add(execution_decision)
    _make_paper_trade(
        session,
        default_signal.id,
        outcome.id,
        market.id,
        execution_decision_id=execution_decision.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("100.00"),
        shadow_pnl=Decimal("80.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(days=1),
        opened_at=started_at + timedelta(hours=1),
        details={
            "market_question": "Scoped verdict market",
            "shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": False},
        },
    )

    benchmark_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        timeframe="4h",
        fired_at=started_at + timedelta(hours=2),
        dedupe_bucket=(started_at + timedelta(hours=2)).replace(minute=0, second=0, microsecond=0),
        rank_score=Decimal("0.700"),
        estimated_probability=Decimal("0.5600"),
        probability_adjustment=Decimal("0.0600"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.060000"),
        resolved=True,
        resolved_correctly=False,
        clv=Decimal("-0.040000"),
        profit_loss=Decimal("-0.120000"),
        details={"direction": "up", "market_question": "Scoped verdict market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        benchmark_signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("-60.00"),
        exit_price=Decimal("0.000000"),
        resolved_at=now - timedelta(hours=12),
        opened_at=started_at + timedelta(hours=2),
        details={"market_question": "Scoped verdict market"},
    )
    await session.commit()

    async def _supported_replay_status(_session):  # noqa: ARG001
        return {
            "coverage_mode": "supported_detectors_only",
            "configured_supported_detectors": ["confluence"],
            "supported_detectors": ["confluence"],
            "unsupported_detectors": [],
            "recent_coverage_limited_run_count_24h": 0,
        }
    monkeypatch.setattr(
        analysis_module,
        "fetch_polymarket_replay_status",
        _supported_replay_status,
    )

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    verdict = response.json()["review_verdict"]

    assert verdict["verdict"] == "keep"
    assert verdict["reason_code"] == "positive_consensus"
    assert verdict["threshold_version"] == "default_strategy_review_v1"
    assert verdict["precedence"] == "blockers_first"
    assert verdict["blockers"] == []
    assert verdict["signals"] == {
        "execution_adjusted_pnl_sign": "positive",
        "signal_level_pnl_per_share_sign": "positive",
        "avg_clv_sign": "positive",
    }


@pytest.mark.asyncio
async def test_strategy_health_respects_launch_boundary_without_mutating_run_state(client, session, monkeypatch):
    now = datetime.now(timezone.utc)
    launch_at = now - timedelta(hours=12)
    monkeypatch.setattr(settings, "default_strategy_start_at", launch_at)

    market = make_market(session, question="Launch boundary market")
    outcome = make_outcome(session, market.id, name="Yes")

    pre_launch_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=2),
        dedupe_bucket=(now - timedelta(days=2)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.1500"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.150000"),
        details={"direction": "up", "market_question": "Launch boundary market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        pre_launch_signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("90.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(days=1),
        opened_at=now - timedelta(days=2),
        details={"market_question": "Launch boundary market", "ev_per_share": "0.150000"},
    )

    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=2),
        dedupe_bucket=(now - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.5100"),
        probability_adjustment=Decimal("0.0100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.010000"),
        details={
            "direction": "up",
            "market_question": "Launch boundary market",
            "outcome_name": "Yes",
            "default_strategy": {
                "decision": "skipped",
                "reason_code": "ev_below_threshold",
                "reason_label": "EV below threshold",
            },
        },
    )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/strategy-health")
    assert resp.status_code == 200
    data = resp.json()

    assert data["observation"]["started_at"] is None
    assert data["observation"]["baseline_start_at"] == launch_at.isoformat()
    assert data["observation"]["status"] == "no_active_run"
    assert data["bootstrap_required"] is True
    assert data["strategy_run"] is None
    assert data["trade_funnel"]["candidate_signals"] == 0
    assert data["trade_funnel"]["pre_launch_candidate_signals"] == 0
    assert data["trade_funnel"]["traded_signals"] == 0
    assert data["trade_funnel"]["excluded_pre_launch_trades"] == 0
    assert data["skip_reasons"] == []
    assert data["headline"]["resolved_trades"] == 0


@pytest.mark.asyncio
async def test_strategy_health_endpoint_returns_default_strategy_contract_and_benchmark(client, session):
    market = make_market(session, question="Strategy health market")
    outcome = make_outcome(session, market.id, name="Yes")
    now = datetime.now(timezone.utc)
    opened_at = now - timedelta(days=3)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=opened_at)

    confluence_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=3),
        dedupe_bucket=(now - timedelta(days=3)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.050000"),
        profit_loss=Decimal("0.100000"),
        details={"direction": "up", "market_question": "Strategy health market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=2),
        dedupe_bucket=(now - timedelta(days=2)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6400"),
        probability_adjustment=Decimal("0.1400"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.140000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.070000"),
        profit_loss=Decimal("0.120000"),
        details={"direction": "up", "market_question": "Strategy health market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=1),
        dedupe_bucket=(now - timedelta(days=1)).replace(minute=0, second=0, microsecond=0),
        details={"direction": "up", "market_question": "Strategy health market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        timeframe="4h",
        fired_at=now - timedelta(days=2),
        dedupe_bucket=(now - timedelta(days=2)).replace(minute=0, second=0, microsecond=0),
        rank_score=Decimal("0.700"),
        estimated_probability=Decimal("0.5600"),
        probability_adjustment=Decimal("0.0600"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.060000"),
        resolved=True,
        resolved_correctly=False,
        clv=Decimal("-0.020000"),
        profit_loss=Decimal("-0.100000"),
        details={"direction": "up", "market_question": "Strategy health market", "outcome_name": "Yes"},
    )
    execution_decision = ExecutionDecision(
        id=uuid.uuid4(),
        signal_id=confluence_signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=opened_at,
        decision_status="opened",
        action="cross",
        direction="buy_yes",
        executable_entry_price=Decimal("0.50000000"),
        reason_code="opened",
        details={"source": "test"},
    )
    session.add(execution_decision)
    _make_paper_trade(
        session,
        confluence_signal.id,
        outcome.id,
        market.id,
        execution_decision_id=execution_decision.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("125.00"),
        shadow_pnl=Decimal("100.00"),
        shadow_entry_price=Decimal("0.520000"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(days=1),
        opened_at=opened_at,
        details={
            "market_question": "Strategy health market",
            "ev_per_share": "0.120000",
            "shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": False},
        },
    )
    legacy_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        timeframe="1h",
        fired_at=now - timedelta(hours=20),
        dedupe_bucket=(now - timedelta(hours=20)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6100"),
        probability_adjustment=Decimal("0.1100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.110000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.040000"),
        profit_loss=Decimal("0.090000"),
        details={"direction": "up", "market_question": "Strategy health market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        legacy_signal.id,
        outcome.id,
        market.id,
        status="resolved",
        pnl=Decimal("75.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(hours=12),
        opened_at=now - timedelta(hours=20),
        details={"market_question": "Strategy health market", "ev_per_share": "0.110000"},
    )
    pending_signal = await ensure_pending_execution_decision(
        session=session,
        signal_id=(
            await session.scalar(
                select(Signal.id).where(
                    Signal.signal_type == "confluence",
                    Signal.fired_at == now - timedelta(days=2),
                )
            )
        ),
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.6400"),
        market_price=Decimal("0.500000"),
        market_question="Strategy health market",
        fired_at=now - timedelta(days=2),
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    resp = await client.get("/api/v1/paper-trading/strategy-health")
    assert resp.status_code == 200
    data = resp.json()

    assert data["strategy"]["signal_type"] == "confluence"
    assert data["strategy_run"]["id"] == str(strategy_run.id)
    assert data["strategy_run"]["contract_snapshot"]["bootstrap_source"] == "BOOTSTRAP_STARTED_AT"
    assert data["observation"]["started_at"] == opened_at.isoformat()
    assert data["trade_funnel"]["candidate_signals"] == 3
    assert data["trade_funnel"]["qualified_signals"] == 2
    assert data["trade_funnel"]["traded_signals"] == 1
    assert data["trade_funnel"]["qualified_not_traded"] == 1
    assert pending_signal is not None
    assert data["trade_funnel"]["resolved_trades"] == 1
    assert data["trade_funnel"]["resolved_signals"] == 1
    assert data["trade_funnel"]["excluded_legacy_trades"] == 1
    assert data["headline"]["cumulative_pnl"] == 125.0
    assert data["execution_realism"]["shadow_cumulative_pnl"] == 100.0
    assert data["headline"]["resolved_trades"] == 1
    assert data["headline"]["resolved_signals"] == 1
    assert data["headline"]["overdue_open_trades"] == 0
    assert data["headline"]["avg_clv"] == pytest.approx(0.05)
    assert data["headline"]["brier_score"] is not None
    assert data["benchmark"]["resolved_signals"] >= 1
    assert any(row["signal_type"] == "confluence" for row in data["detector_review"])
    assert isinstance(data["recent_mistakes"], list)
