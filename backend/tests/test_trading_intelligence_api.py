"""API and integration tests for trading intelligence surfaces."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config import settings
from app.jobs.scheduler import _resolve_paper_trades, _run_paper_trading
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.strategy_runs.service import ensure_active_default_strategy_run
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

    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        direction="buy_yes",
        size_usd=Decimal("450.00"),
        status="open",
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
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Lifecycle market", "outcome_name": "Yes"},
    )
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
async def test_scheduler_only_auto_trades_default_strategy_signals(client, session):
    market = make_market(session, question="Default strategy gate")
    outcome = make_outcome(session, market.id, name="Yes")

    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
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
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Default strategy gate", "outcome_name": "Yes"},
    )
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
    await session.commit()

    await _run_paper_trading(session, [signal])
    await session.refresh(signal)

    metadata = signal.details["default_strategy"]
    assert metadata["decision"] == "skipped"
    assert metadata["reason_code"] == "ev_below_threshold"
    assert metadata["reason_label"] == "EV below threshold"
    assert metadata["trade_id"] is None


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
async def test_strategy_health_respects_launch_boundary_and_skip_reasons(client, session, monkeypatch):
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

    in_window_signal = make_signal(
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

    assert data["observation"]["started_at"] == launch_at.isoformat()
    assert data["observation"]["baseline_start_at"] == launch_at.isoformat()
    assert data["observation"]["status"] == "live_waiting_for_trades"
    assert data["strategy_run"]["contract_snapshot"]["bootstrap_source"] == "DEFAULT_STRATEGY_START_AT"
    assert data["trade_funnel"]["candidate_signals"] == 1
    assert data["trade_funnel"]["pre_launch_candidate_signals"] == 1
    assert data["trade_funnel"]["traded_signals"] == 0
    assert data["trade_funnel"]["excluded_pre_launch_trades"] == 1
    assert data["skip_reasons"][0]["reason_code"] == "ev_below_threshold"
    assert data["skip_reasons"][0]["count"] == 1
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
    _make_paper_trade(
        session,
        confluence_signal.id,
        outcome.id,
        market.id,
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
    assert data["trade_funnel"]["resolved_trades"] == 1
    assert data["trade_funnel"]["resolved_signals"] == 1
    assert data["trade_funnel"]["excluded_legacy_trades"] == 1
    assert data["headline"]["cumulative_pnl"] == 125.0
    assert data["execution_realism"]["shadow_cumulative_pnl"] == 100.0
    assert data["headline"]["resolved_trades"] == 1
    assert data["headline"]["resolved_signals"] == 1
    assert data["headline"]["avg_clv"] == pytest.approx(0.05)
    assert data["headline"]["brier_score"] is not None
    assert data["benchmark"]["resolved_signals"] >= 1
    assert any(row["signal_type"] == "confluence" for row in data["detector_review"])
    assert isinstance(data["recent_mistakes"], list)
