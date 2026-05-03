import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from app.models.execution_decision import ExecutionDecision
from app.reports.api_smoke import run_evidence_api_smoke
from app.reports.profitability_snapshot import generate_profitability_snapshot_artifact
from app.reports.strategy_review import (
    generate_default_strategy_review,
    get_latest_default_strategy_review_artifact_metadata,
    get_latest_default_strategy_review_artifact_payload,
)
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
        "strategy_run_ref": {
            "id": None,
            "started_at": None,
            "status": None,
        },
        "contract_ref": {
            "contract_version": None,
            "evidence_boundary_id": None,
            "release_tag": None,
            "migration_revision": None,
        },
        "generation_guidance": {
            "working_directory": "backend",
            "command": "python -m app.reports",
            "runbook_path": "docs/runbooks/default-strategy-controlled-evidence-relaunch.md",
            "artifacts_directory": "docs/strategy-reviews",
            "analysis_path": "docs/paper-trading-analysis-v0.5.md",
            "note": (
                "Read-only health and dashboard surfaces never generate review artifacts. "
                "Use the canonical backend command or the controlled relaunch runbook instead."
            ),
        },
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

    async def _unexpected_compare_locked_modes(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("review generation should reuse strategy-health comparison_modes")

    monkeypatch.setattr(review_module, "compare_locked_modes", _unexpected_compare_locked_modes)
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
    assert "Live Automation Safety" in review_text
    assert "Resolution Reconciliation" in review_text
    assert "Profitability Snapshot" in review_text
    assert "v0.4.1" in review_text
    review_payload = json.loads(review_json_path.read_text(encoding="utf-8"))
    assert review_payload["review_verdict"]["verdict"] == "keep"
    assert review_payload["live_safety"]["status"] == "fail_closed"
    assert review_payload["live_safety"]["counts"]["live_orders"] == 0
    assert review_payload["comparison_modes"] == result["comparison"]
    assert review_payload["artifact_schema_version"] == "default_strategy_review_v2"
    assert review_payload["artifact_fingerprint_status"] == "current"
    assert review_payload["activity_fingerprint"]["strategy_run_id"] == str(strategy_run.id)
    assert review_payload["strategy_health"]["activity_fingerprint"] == review_payload["activity_fingerprint"]
    assert review_payload["strategy_health"]["profitability_snapshot"]["family"] == "default_strategy"
    assert review_payload["profitability_snapshot"]["family"] == "default_strategy"
    assert review_payload["profitability_snapshot"]["realized_pnl"] == 85.0
    assert review_payload["trade_funnel"]["resolved_trades"] == 1
    assert review_payload["resolution_reconciliation"]["status"] == "reconciled"
    assert review_payload["resolution_reconciliation"]["resolved_trades"] == 1
    analysis_text = analysis_path.read_text(encoding="utf-8")
    assert "Paper Trading Analysis v0.5" in analysis_text
    assert "Profitability Gate" in analysis_text

    artifact = get_latest_default_strategy_review_artifact_metadata()
    assert artifact["strategy_run_ref"] == {
        "id": str(strategy_run.id),
        "started_at": start_at.isoformat(),
        "status": strategy_run.status,
    }
    assert artifact["contract_ref"] == {
        "contract_version": "default_strategy_v0.4.1",
        "evidence_boundary_id": "v0.4.1",
        "release_tag": "v0.4.1",
        "migration_revision": "038",
    }
    assert artifact["generation_guidance"] == {
        "working_directory": "backend",
        "command": "python -m app.reports",
        "runbook_path": "docs/runbooks/default-strategy-controlled-evidence-relaunch.md",
        "artifacts_directory": "docs/strategy-reviews",
        "analysis_path": "docs/paper-trading-analysis-v0.5.md",
        "note": (
            "Read-only health and dashboard surfaces never generate review artifacts. "
            "Use the canonical backend command or the controlled relaunch runbook instead."
        ),
    }

    artifact_payload = get_latest_default_strategy_review_artifact_payload()
    assert artifact_payload["generation_status"] == "complete"
    assert artifact_payload["payload"]["activity_fingerprint"] == review_payload["activity_fingerprint"]


@pytest.mark.asyncio
async def test_strategy_health_uses_current_materialized_review_artifact(session, monkeypatch, tmp_path: Path):
    import app.paper_trading.analysis as analysis_module
    import app.reports.strategy_review as review_module

    start_at = datetime.now(timezone.utc) - timedelta(days=3)
    await ensure_active_default_strategy_run(session, bootstrap_started_at=start_at)
    await session.commit()
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    result = await generate_default_strategy_review(session)
    review_payload = json.loads(Path(result["review_json_path"]).read_text(encoding="utf-8"))
    assert review_payload["artifact_fingerprint_status"] == "current"

    analysis_module.clear_default_strategy_evidence_cache()

    async def _unexpected_scope_load(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("strategy health should use the materialized review artifact")

    monkeypatch.setattr(analysis_module, "_get_default_strategy_scope", _unexpected_scope_load)
    health = await analysis_module.get_strategy_health(session)

    assert health["materialized_evidence"]["source"] == "default_strategy_review_artifact"
    assert health["activity_fingerprint"] == review_payload["activity_fingerprint"]
    assert health["profitability_snapshot"]["family"] == "default_strategy"
    assert health["evidence_freshness"]["status"] == "fresh"


@pytest.mark.asyncio
async def test_strategy_health_uses_stale_materialized_review_artifact_fail_closed(
    session,
    monkeypatch,
    tmp_path: Path,
):
    import app.paper_trading.analysis as analysis_module
    import app.reports.strategy_review as review_module

    start_at = datetime.now(timezone.utc) - timedelta(days=3)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=start_at)
    await session.commit()
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)
    result = await generate_default_strategy_review(session)
    review_payload = json.loads(Path(result["review_json_path"]).read_text(encoding="utf-8"))

    market = make_market(session, question="Post-review decision market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=datetime.now(timezone.utc),
        dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.1300"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.130000"),
    )
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=datetime.now(timezone.utc),
            decision_status="skipped",
            action="skip",
            reason_code="test_post_review_activity",
            details={"source": "test"},
        )
    )
    await session.commit()
    analysis_module.clear_default_strategy_evidence_cache()

    async def _unexpected_scope_load(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("stale same-run materialized health should stay fail-closed without a full scan")

    monkeypatch.setattr(analysis_module, "_get_default_strategy_scope", _unexpected_scope_load)
    health = await analysis_module.get_strategy_health(session)

    assert health["materialized_evidence"]["activity_fingerprint_status"] == "stale_activity"
    assert health["activity_fingerprint"] != review_payload["activity_fingerprint"]
    assert health["evidence_freshness"]["status"] == "stale"
    assert "evidence_stale" in health["profitability_snapshot"]["profitability_blockers"]


@pytest.mark.asyncio
async def test_profitability_snapshot_generator_writes_daily_artifacts(session, monkeypatch, tmp_path: Path):
    import app.reports.profitability_snapshot as snapshot_module

    monkeypatch.setattr(snapshot_module, "_repo_root", lambda: tmp_path)
    result = await generate_profitability_snapshot_artifact(session)

    markdown_path = Path(result["snapshot_markdown_path"])
    json_path = Path(result["snapshot_json_path"])
    assert markdown_path.exists()
    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["paper_only"] is True
    assert payload["live_submission_permitted"] is False
    assert payload["snapshot"]["family"] == "default_strategy"
    assert payload["snapshot"]["verdict"] == "insufficient_sample"
    assert "Paper Profitability Snapshot" in markdown_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_evidence_api_smoke_compacts_profitability_surfaces():
    def _handler(request: httpx.Request) -> httpx.Response:
        payloads = {
            "/": {"status": "ok"},
            "/api/v1/health": {"status": "healthy"},
            "/api/v1/paper-trading/strategy-health": {
                "review_verdict": {"verdict": "not_ready", "blockers": [{"code": "insufficient_observation_days"}]},
                "evidence_freshness": {"status": "fresh"},
            },
            "/api/v1/paper-trading/profitability-snapshot": {
                "verdict": "insufficient_sample",
                "realized_pnl": 0.0,
                "mark_to_market_pnl": 0.0,
                "profitability_blockers": ["insufficient_resolved_trades"],
            },
            "/api/v1/paper-trading/profit-tools": {
                "paper_only": True,
                "live_submission_permitted": False,
                "next_best_steps": [{"step": "resolution_accelerator"}],
                "lane_readiness": {"status": "research_blocked"},
            },
            "/api/v1/strategies/profitability": {
                "paper_only": True,
                "live_submission_permitted": False,
                "snapshots": [{"family": "default_strategy"}],
            },
        }
        return httpx.Response(200, json=payloads[str(request.url.path)])

    result = await run_evidence_api_smoke(
        base_url="http://testserver",
        transport=httpx.MockTransport(_handler),
    )

    assert result["status"] == "passing"
    checks = {row["name"]: row for row in result["checks"]}
    assert checks["profitability_snapshot"]["summary"]["verdict"] == "insufficient_sample"
    assert checks["profit_tools"]["summary"]["next_step_count"] == 1
    assert checks["strategy_health"]["summary"]["review_blockers"] == ["insufficient_observation_days"]
    assert checks["strategy_profitability"]["summary"]["snapshot_count"] == 1


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
