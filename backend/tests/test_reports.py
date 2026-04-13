from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.reports.strategy_review import generate_default_strategy_review
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_signal
from tests.test_trading_intelligence_api import _make_paper_trade


@pytest.mark.asyncio
async def test_review_generator_writes_versioned_artifacts(session, monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    start_at = now - timedelta(days=2)
    market = make_market(session, question="Review market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=start_at,
        dedupe_bucket=start_at.replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.1300"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.130000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.040000"),
        profit_loss=Decimal("0.090000"),
        details={"market_question": "Review market", "outcome_name": "Yes"},
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=start_at)
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("85.00"),
        shadow_pnl=Decimal("70.00"),
        exit_price=Decimal("1.000000"),
        shadow_entry_price=Decimal("0.420000"),
        resolved_at=now,
        opened_at=start_at,
        details={
            "market_question": "Review market",
            "shadow_execution": {"liquidity_constrained": False, "missing_orderbook_context": False},
        },
    )
    await session.commit()

    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)
    result = await generate_default_strategy_review(session)

    review_path = Path(result["review_path"])
    analysis_path = Path(result["analysis_path"])
    assert review_path.exists()
    assert analysis_path.exists()
    assert "Default Strategy Review" in review_path.read_text(encoding="utf-8")
    assert "Paper Trading Analysis v0.5" in analysis_path.read_text(encoding="utf-8")
