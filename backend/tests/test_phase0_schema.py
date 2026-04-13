import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from tests.conftest import make_market, make_outcome, make_signal


@pytest.mark.asyncio
async def test_phase0_model_schema_matches_build_sheet(session):
    signal_columns = Signal.__table__.c
    assert signal_columns.observed_at_exchange.nullable is True
    assert signal_columns.received_at_local.nullable is True
    assert signal_columns.detected_at_local.nullable is True
    assert signal_columns.source_platform.nullable is True
    assert signal_columns.source_token_id.nullable is True
    assert signal_columns.source_stream_session_id.nullable is True
    assert signal_columns.source_event_hash.nullable is True
    assert signal_columns.source_event_type.nullable is True

    signal_indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in Signal.__table__.indexes
    }
    assert signal_indexes["ix_signal_observed_at_exchange"] == ("observed_at_exchange",)
    assert signal_indexes["ix_signal_received_at_local"] == ("received_at_local",)
    assert signal_indexes["ix_signal_source_platform_observed_at_exchange"] == (
        "source_platform",
        "observed_at_exchange",
    )
    assert signal_indexes["ix_signal_source_token_id_observed_at_exchange"] == (
        "source_token_id",
        "observed_at_exchange",
    )
    assert signal_indexes["ix_signal_source_stream_session_id"] == ("source_stream_session_id",)

    execution_columns = ExecutionDecision.__table__.c
    assert execution_columns.signal_id.nullable is False
    assert execution_columns.strategy_run_id.nullable is False
    assert execution_columns.decision_at.nullable is False
    assert execution_columns.decision_status.nullable is False
    assert execution_columns.action.nullable is False
    assert execution_columns.reason_code.nullable is False
    assert execution_columns.direction.nullable is True
    assert execution_columns.ideal_entry_price.nullable is True
    assert execution_columns.executable_entry_price.nullable is True
    assert execution_columns.requested_size_usd.nullable is True
    assert execution_columns.fillable_size_usd.nullable is True
    assert execution_columns.fill_probability.nullable is True
    assert execution_columns.net_ev_per_share.nullable is True
    assert execution_columns.net_expected_pnl_usd.nullable is True
    assert execution_columns.fill_status.nullable is True
    assert execution_columns.details.nullable is False

    execution_indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in ExecutionDecision.__table__.indexes
    }
    assert execution_indexes["ix_execution_decisions_strategy_run_decision_at"] == (
        "strategy_run_id",
        "decision_at",
    )
    assert execution_indexes["ix_execution_decisions_reason_code"] == ("reason_code",)
    assert execution_indexes["ix_execution_decisions_fill_status"] == ("fill_status",)

    execution_uniques = {
        tuple(column.name for column in constraint.columns)
        for constraint in ExecutionDecision.__table__.constraints
        if getattr(constraint, "name", None) == "uq_execution_decisions_signal_strategy_run"
    }
    assert ("signal_id", "strategy_run_id") in execution_uniques

    paper_columns = PaperTrade.__table__.c
    assert paper_columns.execution_decision_id.nullable is True
    assert paper_columns.submitted_at.nullable is True
    assert paper_columns.confirmed_at.nullable is True

    paper_indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in PaperTrade.__table__.indexes
    }
    assert paper_indexes["ix_paper_trades_submitted_at"] == ("submitted_at",)
    assert paper_indexes["ix_paper_trades_execution_decision_id"] == ("execution_decision_id",)

    paper_uniques = {
        tuple(column.name for column in constraint.columns)
        for constraint in PaperTrade.__table__.constraints
        if getattr(constraint, "name", None) == "uq_paper_trades_execution_decision_id"
    }
    assert ("execution_decision_id",) in paper_uniques

    assert session.bind is not None


@pytest.mark.asyncio
async def test_execution_decision_is_unique_per_signal_and_strategy_run(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal = make_signal(session, market.id, outcome.id)
    strategy_run = StrategyRun(
        id=uuid.uuid4(),
        strategy_name="prove_the_edge_default",
        status="active",
        started_at=datetime(2026, 4, 13, tzinfo=timezone.utc),
        contract_snapshot={"name": "prove_the_edge_default"},
    )
    session.add(strategy_run)
    await session.flush()

    first = ExecutionDecision(
        signal_id=signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=datetime.now(timezone.utc),
        decision_status="skipped",
        action="skip",
        reason_code="ev_below_threshold",
        details={},
    )
    session.add(first)
    await session.flush()

    duplicate = ExecutionDecision(
        signal_id=signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=datetime.now(timezone.utc),
        decision_status="opened",
        action="cross",
        reason_code="opened",
        details={},
    )
    session.add(duplicate)

    with pytest.raises(IntegrityError):
        await session.flush()


@pytest.mark.asyncio
async def test_paper_trade_execution_decision_link_is_unique(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    signal_one = make_signal(session, market.id, outcome.id)
    signal_two = make_signal(session, market.id, outcome.id, id=uuid.uuid4(), timeframe="4h")
    strategy_run = StrategyRun(
        id=uuid.uuid4(),
        strategy_name="prove_the_edge_default",
        status="active",
        started_at=datetime(2026, 4, 13, tzinfo=timezone.utc),
        contract_snapshot={"name": "prove_the_edge_default"},
    )
    session.add(strategy_run)
    await session.flush()

    decision = ExecutionDecision(
        signal_id=signal_one.id,
        strategy_run_id=strategy_run.id,
        decision_at=datetime.now(timezone.utc),
        decision_status="opened",
        action="cross",
        direction="buy_yes",
        executable_entry_price=Decimal("0.41000000"),
        reason_code="opened",
        details={},
    )
    session.add(decision)
    await session.flush()

    first_trade = PaperTrade(
        signal_id=signal_one.id,
        execution_decision_id=decision.id,
        outcome_id=outcome.id,
        market_id=market.id,
        direction="buy_yes",
        entry_price=Decimal("0.410000"),
        size_usd=Decimal("100.00"),
        shares=Decimal("243.9024"),
        details={},
    )
    second_trade = PaperTrade(
        signal_id=signal_two.id,
        execution_decision_id=decision.id,
        outcome_id=outcome.id,
        market_id=market.id,
        direction="buy_yes",
        entry_price=Decimal("0.410000"),
        size_usd=Decimal("120.00"),
        shares=Decimal("292.6829"),
        details={},
    )
    session.add(first_trade)
    await session.flush()
    session.add(second_trade)

    with pytest.raises(IntegrityError):
        await session.flush()
