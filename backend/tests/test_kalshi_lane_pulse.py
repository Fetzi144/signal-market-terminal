from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.strategy_run import StrategyRun
from app.reports.kalshi_lane_pulse import build_kalshi_lane_pulse, generate_kalshi_lane_pulse_artifact
from tests.conftest import make_market, make_outcome, make_signal


def _strategy_run(family: str, *, started_at: datetime) -> StrategyRun:
    return StrategyRun(
        id=uuid.uuid4(),
        strategy_name=f"{family}_v1",
        strategy_family=family,
        status="active",
        started_at=started_at,
        contract_snapshot={},
    )


def _trade(
    signal_id,
    outcome_id,
    market_id,
    strategy_run_id,
    *,
    opened_at: datetime,
    status: str = "open",
    pnl: Decimal | None = None,
) -> PaperTrade:
    return PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        strategy_run_id=strategy_run_id,
        direction="buy_yes",
        entry_price=Decimal("0.001000"),
        shadow_entry_price=Decimal("0.001000"),
        size_usd=Decimal("22.52"),
        shares=Decimal("22520.0000"),
        status=status,
        opened_at=opened_at,
        resolved_at=opened_at + timedelta(minutes=30) if status == "resolved" else None,
        exit_price=Decimal("0.000000") if status == "resolved" else None,
        pnl=pnl,
        details={"market_question": "Duplicate lane question?"},
    )


@pytest.mark.asyncio
async def test_kalshi_lane_pulse_surfaces_duplicate_warnings_and_quarantine(session):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    run = _strategy_run("kalshi_cheap_yes_follow", started_at=now - timedelta(hours=2))
    market = make_market(session, platform="kalshi", question="Duplicate lane question?", end_date=now + timedelta(days=1))
    outcome = make_outcome(session, market.id, name="Yes")
    first_signal = make_signal(session, market.id, outcome.id, fired_at=now - timedelta(hours=1))
    second_signal = make_signal(session, market.id, outcome.id, fired_at=now - timedelta(minutes=40))
    session.add(run)
    session.add_all(
        [
            _trade(
                first_signal.id,
                outcome.id,
                market.id,
                run.id,
                opened_at=now - timedelta(hours=1),
                status="resolved",
                pnl=Decimal("-22.52"),
            ),
            _trade(
                second_signal.id,
                outcome.id,
                market.id,
                run.id,
                opened_at=now - timedelta(minutes=40),
                status="resolved",
                pnl=Decimal("-22.52"),
            ),
            ExecutionDecision(
                id=uuid.uuid4(),
                signal_id=second_signal.id,
                strategy_run_id=run.id,
                decision_at=now - timedelta(minutes=40),
                decision_status="opened",
                action="cross",
                reason_code="opened",
                details={},
            ),
        ]
    )
    await session.commit()

    pulse = await build_kalshi_lane_pulse(session, as_of=now)
    cheap = next(lane for lane in pulse["lanes"] if lane["family"] == "kalshi_cheap_yes_follow")

    assert pulse["paper_only"] is True
    assert pulse["live_submission_permitted"] is False
    assert pulse["verdict"] == "needs_evidence_hygiene"
    assert cheap["quarantine"]["enabled"] is True
    assert cheap["resolved_trades_window"] == 2
    assert cheap["realized_pnl_window"] == -45.04
    assert cheap["duplicate_market_warnings"][0]["trade_count"] == 2
    assert cheap["decision_reasons"][0]["reason_code"] == "opened"
    assert any(action["step"] == "keep_cheap_yes_follow_quarantined" for action in pulse["next_best_actions"])


@pytest.mark.asyncio
async def test_kalshi_lane_pulse_artifact_generator_writes_json_and_markdown(session, monkeypatch, tmp_path: Path):
    import app.reports.kalshi_lane_pulse as pulse_module

    monkeypatch.setattr(pulse_module, "_repo_root", lambda: tmp_path)

    result = await generate_kalshi_lane_pulse_artifact(session)

    json_path = Path(result["pulse_json_path"])
    markdown_path = Path(result["pulse_markdown_path"])
    assert json_path.exists()
    assert markdown_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "kalshi_lane_pulse_v1"
    assert "Kalshi Lane Pulse" in markdown_path.read_text(encoding="utf-8")
