import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.models.execution_decision import ExecutionDecision
from app.reports.strategy_review import generate_default_strategy_review, get_latest_default_strategy_review_artifact_metadata
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_signal
from tests.test_trading_intelligence_api import _make_paper_trade


def test_latest_review_artifact_metadata_reports_missing_when_no_artifacts_exist(monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    artifact = get_latest_default_strategy_review_artifact_metadata()

    assert artifact == {
        "generation_status": "missing",
        "status_detail": "No default-strategy review artifacts have been generated yet.",
        "review_date": None,
        "generated_at": None,
        "verdict": None,
        "artifact_paths": {
            "markdown": None,
            "json": None,
        },
    }


@pytest.mark.asyncio
async def test_review_generator_writes_versioned_artifacts(session, monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    start_at = now - timedelta(days=20)
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
    strategy_run.contract_snapshot["contract_version"] = "default_strategy_v0.4.1"
    strategy_run.contract_snapshot["evidence_boundary"] = {
        "boundary_id": "v0.4.1",
        "release_tag": "v0.4.1",
        "commit_sha": "87a4315b81b81365d9ee974aff5b130813757897",
        "migration_revision": "038",
    }
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=start_at,
            decision_status="opened",
            action="cross",
            direction="buy_yes",
            executable_entry_price=Decimal("0.50000000"),
            reason_code="opened",
            details={"source": "test"},
        )
    )
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
    review_json_path = Path(result["review_json_path"])
    analysis_path = Path(result["analysis_path"])
    assert review_path.exists()
    assert review_json_path.exists()
    assert analysis_path.exists()
    review_text = review_path.read_text(encoding="utf-8")
    assert "Default Strategy Review" in review_text
    assert "Operator Verdict" in review_text
    assert "Contract Version" in review_text
    assert "v0.4.1" in review_text
    review_payload = json.loads(review_json_path.read_text(encoding="utf-8"))
    assert review_payload["review_verdict"]["verdict"] == "keep"
    assert review_payload["trade_funnel"]["resolved_trades"] == 1
    assert "Paper Trading Analysis v0.5" in analysis_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_review_generator_surfaces_shared_global_reasons_and_persisted_drawdown(session, monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    start_at = now - timedelta(days=2)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=start_at)
    strategy_run.current_equity = Decimal("10010.00")
    strategy_run.peak_equity = Decimal("10140.00")
    strategy_run.max_drawdown = Decimal("130.00")
    strategy_run.drawdown_pct = Decimal("0.012821")

    market = make_market(session, question="Shared review market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=start_at + timedelta(hours=1),
        dedupe_bucket=start_at.replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.1300"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.130000"),
        details={"market_question": "Shared review market", "outcome_name": "Yes"},
    )
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=signal.fired_at,
            decision_status="skipped",
            action="skip",
            reason_code="risk_shared_global_block",
            details={
                "reason_label": "Shared/global platform risk blocked the trade",
                "detail": "inventory cap hit",
                "risk_result": {
                    "risk_scope": "shared_global",
                    "risk_source": "risk_graph",
                    "reason_code": "risk_shared_global_block",
                    "original_reason_code": "inventory_cap",
                    "original_reason": "inventory_cap",
                },
            },
        )
    )
    await session.commit()

    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)
    result = await generate_default_strategy_review(session)

    review_path = Path(result["review_path"])
    contents = review_path.read_text(encoding="utf-8")
    assert "Max drawdown: $130.00" in contents
    assert "Operator Verdict" in contents
    assert "Shared/global upstream reasons" in contents
    assert "inventory_cap: 1" in contents
    assert "Execution/liquidity blocks" in contents


def test_latest_review_artifact_metadata_falls_back_to_markdown_when_json_is_missing(monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    markdown_path = review_dir / "2026-04-20-default-strategy-baseline.md"
    markdown_path.write_text(
        "# Default Strategy Review\n\n"
        "**Date:** 2026-04-20\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `cut`\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    artifact = get_latest_default_strategy_review_artifact_metadata()

    assert artifact["generation_status"] == "partial"
    assert artifact["verdict"] == "cut"
    assert artifact["generated_at"] is not None
    assert artifact["artifact_paths"] == {
        "markdown": "docs/strategy-reviews/2026-04-20-default-strategy-baseline.md",
        "json": None,
    }


def test_latest_review_artifact_metadata_falls_back_to_markdown_when_json_is_invalid(monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    markdown_path = review_dir / "2026-04-21-default-strategy-baseline.md"
    json_path = review_dir / "2026-04-21-default-strategy-baseline.json"
    markdown_path.write_text(
        "# Default Strategy Review\n\n"
        "**Date:** 2026-04-21\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `watch`\n",
        encoding="utf-8",
    )
    json_path.write_text("{not valid json", encoding="utf-8")
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    artifact = get_latest_default_strategy_review_artifact_metadata()

    assert artifact["generation_status"] == "invalid"
    assert artifact["status_detail"] == (
        "The latest review JSON artifact could not be parsed. "
        "Showing markdown fallback metadata when available."
    )
    assert artifact["review_date"] == "2026-04-21"
    assert artifact["verdict"] == "watch"
    assert artifact["generated_at"] is not None
    assert artifact["artifact_paths"] == {
        "markdown": "docs/strategy-reviews/2026-04-21-default-strategy-baseline.md",
        "json": "docs/strategy-reviews/2026-04-21-default-strategy-baseline.json",
    }


def test_latest_review_artifact_metadata_falls_back_to_markdown_when_json_is_unreadable(monkeypatch, tmp_path: Path):
    import app.reports.strategy_review as review_module

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    markdown_path = review_dir / "2026-04-22-default-strategy-baseline.md"
    json_path = review_dir / "2026-04-22-default-strategy-baseline.json"
    markdown_path.write_text(
        "# Default Strategy Review\n\n"
        "**Date:** 2026-04-22\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `keep`\n",
        encoding="utf-8",
    )
    json_path.write_text('{"generated_at": "2026-04-22T06:00:00+00:00"}', encoding="utf-8")
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    original_read_text = Path.read_text

    def _read_text(self, *args, **kwargs):
        if self == json_path:
            raise OSError("permission denied")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _read_text)

    artifact = get_latest_default_strategy_review_artifact_metadata()

    assert artifact["generation_status"] == "invalid"
    assert artifact["status_detail"] == (
        "The latest review JSON artifact could not be read. "
        "Showing markdown fallback metadata when available."
    )
    assert artifact["review_date"] == "2026-04-22"
    assert artifact["verdict"] == "keep"
    assert artifact["generated_at"] is not None
    assert artifact["artifact_paths"] == {
        "markdown": "docs/strategy-reviews/2026-04-22-default-strategy-baseline.md",
        "json": "docs/strategy-reviews/2026-04-22-default-strategy-baseline.json",
    }
