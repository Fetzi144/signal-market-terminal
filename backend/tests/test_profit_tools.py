import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.models.execution_decision import ExecutionDecision
from app.reports.profit_tools import build_profit_tools_snapshot, generate_profit_tools_artifact
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_signal
from tests.test_trading_intelligence_api import _make_paper_trade


@pytest.mark.asyncio
async def test_profit_tools_snapshot_surfaces_next_ev_work_and_stays_paper_only(session, client):
    now = datetime.now(timezone.utc)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=now - timedelta(days=3))

    open_market = make_market(
        session,
        question="Long dated open exposure",
        end_date=now + timedelta(days=45),
        last_liquidity=Decimal("2500.00"),
    )
    open_outcome = make_outcome(session, open_market.id, name="Yes")
    open_signal = make_signal(
        session,
        open_market.id,
        open_outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=2),
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
    )
    session.add(
        ExecutionDecision(
            signal_id=open_signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=open_signal.fired_at,
            decision_status="opened",
            action="cross",
            direction="buy_yes",
            executable_entry_price=Decimal("0.40000000"),
            net_expected_pnl_usd=Decimal("35.00000000"),
            reason_code="opened",
            details={"source": "test"},
        )
    )
    _make_paper_trade(
        session,
        open_signal.id,
        open_outcome.id,
        open_market.id,
        strategy_run_id=strategy_run.id,
        status="open",
        size_usd=Decimal("700.00"),
        opened_at=now - timedelta(days=2),
        details={"market_question": "Long dated open exposure"},
    )

    missed_market = make_market(
        session,
        question="Missed short horizon edge",
        end_date=now + timedelta(days=2),
        last_liquidity=Decimal("5000.00"),
    )
    missed_outcome = make_outcome(session, missed_market.id, name="Yes")
    missed_signal = make_signal(
        session,
        missed_market.id,
        missed_outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(days=1),
        dedupe_bucket=(now - timedelta(days=1)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6200"),
        price_at_fire=Decimal("0.520000"),
        expected_value=Decimal("0.100000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.030000"),
        profit_loss=Decimal("0.080000"),
    )
    session.add(
        ExecutionDecision(
            signal_id=missed_signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=missed_signal.fired_at,
            decision_status="skipped",
            action="skip",
            direction="buy_yes",
            net_ev_per_share=Decimal("0.04000000"),
            net_expected_pnl_usd=Decimal("12.00000000"),
            missing_orderbook_context=True,
            reason_code="execution_orderbook_context_unavailable",
            details={"source": "test"},
        )
    )
    await session.commit()

    payload = await build_profit_tools_snapshot(session, use_cache=False)

    assert payload["schema_version"] == "profit_tools_v1"
    assert payload["paper_only"] is True
    assert payload["live_submission_permitted"] is False
    assert payload["operator_guardrails"]["live_orders_allowed"] is False
    assert payload["resolution_accelerator"]["open_trade_count"] == 1
    assert payload["resolution_accelerator"]["buckets"]["long_dated_capital_drag"]["trade_count"] == 1
    assert payload["profit_finder_workbench"]["missed_positive_cohorts"][0]["positive_evidence"] is True
    assert payload["lane_readiness"]["status"] == "research_ready"
    assert payload["lane_readiness"]["scope"] == "kalshi_only"
    assert set(payload["lane_readiness"]["retired_lanes"]) == {"structure", "maker", "exec_policy", "replay"}
    assert [step["step"] for step in payload["next_best_steps"]][:3] == [
        "resolution_accelerator",
        "repair_orderbook_context",
        "promote_missed_positive_cohort",
    ]

    response = await client.get("/api/v1/paper-trading/profit-tools")
    assert response.status_code == 200
    api_payload = response.json()
    assert api_payload["paper_only"] is True
    assert api_payload["resolution_accelerator"]["open_trade_count"] == 1
    assert api_payload["next_best_steps"][0]["step"] == "resolution_accelerator"


@pytest.mark.asyncio
async def test_profit_tools_artifact_generator_writes_json_and_markdown(session, monkeypatch, tmp_path: Path):
    import app.reports.profit_tools as profit_tools_module

    monkeypatch.setattr(profit_tools_module, "_repo_root", lambda: tmp_path)
    result = await generate_profit_tools_artifact(session)

    markdown_path = Path(result["profit_tools_markdown_path"])
    json_path = Path(result["profit_tools_json_path"])
    assert markdown_path.exists()
    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "profit_tools_v1"
    assert payload["paper_only"] is True
    assert payload["live_submission_permitted"] is False
    assert "Profit Tools Snapshot" in markdown_path.read_text(encoding="utf-8")
