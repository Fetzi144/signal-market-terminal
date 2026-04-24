from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

import app.paper_trading.analysis as analysis_module
import app.paper_trading.engine as engine_module
from app.backtesting.comparison import compare_strategy_measurement_modes
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_status
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.strategy_run import StrategyRun
from app.paper_trading.engine import attempt_open_trade, ensure_pending_execution_decision, resolve_trades
from app.strategy_runs.service import open_default_strategy_run
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal
from tests.test_trading_intelligence_api import _make_paper_trade


async def _count_rows(session) -> dict[str, int]:
    return {
        "strategy_runs": int((await session.execute(select(func.count(StrategyRun.id)))).scalar_one() or 0),
        "execution_decisions": int((await session.execute(select(func.count(ExecutionDecision.id)))).scalar_one() or 0),
        "paper_trades": int((await session.execute(select(func.count(PaperTrade.id)))).scalar_one() or 0),
    }


@pytest.mark.asyncio
async def test_default_strategy_read_endpoints_do_not_create_rows_without_active_run(client, session):
    before = await _count_rows(session)

    responses = [
        await client.get("/api/v1/paper-trading/portfolio?scope=default_strategy"),
        await client.get("/api/v1/paper-trading/history?scope=default_strategy"),
        await client.get("/api/v1/paper-trading/metrics?scope=default_strategy"),
        await client.get("/api/v1/paper-trading/strategy-health"),
        await client.get("/api/v1/paper-trading/pnl-curve?scope=default_strategy"),
        await client.get("/api/v1/paper-trading/default-strategy/run"),
        await client.get("/api/v1/paper-trading/default-strategy/dashboard"),
    ]

    after = await _count_rows(session)

    assert all(response.status_code == 200 for response in responses)
    assert before == after == {
        "strategy_runs": 0,
        "execution_decisions": 0,
        "paper_trades": 0,
    }

    health = responses[3].json()
    lookup = responses[5].json()
    dashboard = responses[6].json()
    assert health["observation"]["status"] == "no_active_run"
    assert health["strategy_run"] is None
    assert health["headline"]["overdue_open_trades"] == 0
    assert lookup["state"] == "no_active_run"
    assert lookup["strategy_run"] is None
    assert lookup["bootstrap_required"] is True
    assert dashboard["portfolio"]["open_trades"] == []
    assert dashboard["metrics"]["total_trades"] == 0
    assert dashboard["pnl_curve"] == []
    assert dashboard["strategy_health"]["bootstrap_required"] is True
    assert dashboard["strategy_health"]["observation"]["status"] == "no_active_run"
    assert health["review_verdict"]["verdict"] == "not_ready"
    assert [row["code"] for row in health["review_verdict"]["blockers"]] == ["no_active_run"]
    assert dashboard["strategy_health"]["review_verdict"] == health["review_verdict"]


@pytest.mark.asyncio
async def test_strategy_health_surfaces_missing_latest_review_artifact_without_mutation(client, monkeypatch, tmp_path):
    import app.reports.strategy_review as review_module

    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    health = await client.get("/api/v1/paper-trading/strategy-health")
    dashboard = await client.get("/api/v1/paper-trading/default-strategy/dashboard")

    assert health.status_code == 200
    assert dashboard.status_code == 200

    latest_review = health.json()["latest_review_artifact"]
    assert latest_review["generation_status"] == "missing"
    assert latest_review["generated_at"] is None
    assert latest_review["verdict"] is None
    assert latest_review["strategy_run_ref"] == {"id": None, "started_at": None, "status": None}
    assert latest_review["contract_ref"] == {
        "contract_version": None,
        "evidence_boundary_id": None,
        "release_tag": None,
        "migration_revision": None,
    }
    assert latest_review["generation_guidance"] == {
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
    assert latest_review["artifact_paths"] == {"markdown": None, "json": None}
    assert health.json()["evidence_freshness"]["status"] == "no_active_run"
    assert dashboard.json()["strategy_health"]["latest_review_artifact"] == latest_review
    assert dashboard.json()["strategy_health"]["evidence_freshness"] == health.json()["evidence_freshness"]


@pytest.mark.asyncio
async def test_default_strategy_dashboard_surfaces_latest_review_artifact_metadata(client, monkeypatch, tmp_path):
    import json

    import app.reports.strategy_review as review_module

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    (review_dir / "2026-04-19-default-strategy-baseline.md").write_text(
        "# Default Strategy Review\n\n"
        "**Date:** 2026-04-19\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `watch`\n",
        encoding="utf-8",
    )
    (review_dir / "2026-04-19-default-strategy-baseline.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-19T12:30:00+00:00",
                "review_verdict": {"verdict": "watch"},
                "strategy_run": {
                    "id": "run-2026-04-19",
                    "started_at": "2026-04-17T00:00:00+00:00",
                    "status": "active",
                    "contract_snapshot": {
                        "contract_version": "default_strategy_v0.4.1",
                        "evidence_boundary": {
                            "boundary_id": "v0.4.1",
                            "release_tag": "v0.4.1",
                            "migration_revision": "038",
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    response = await client.get("/api/v1/paper-trading/default-strategy/dashboard")

    assert response.status_code == 200
    latest_review = response.json()["strategy_health"]["latest_review_artifact"]
    assert latest_review["generation_status"] == "complete"
    assert latest_review["generated_at"] == "2026-04-19T12:30:00+00:00"
    assert latest_review["verdict"] == "watch"
    assert latest_review["strategy_run_ref"] == {
        "id": "run-2026-04-19",
        "started_at": "2026-04-17T00:00:00+00:00",
        "status": "active",
    }
    assert latest_review["contract_ref"] == {
        "contract_version": "default_strategy_v0.4.1",
        "evidence_boundary_id": "v0.4.1",
        "release_tag": "v0.4.1",
        "migration_revision": "038",
    }
    assert latest_review["generation_guidance"] == {
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
    assert latest_review["artifact_paths"] == {
        "markdown": "docs/strategy-reviews/2026-04-19-default-strategy-baseline.md",
        "json": "docs/strategy-reviews/2026-04-19-default-strategy-baseline.json",
    }
    assert response.json()["strategy_health"]["evidence_freshness"]["status"] == "no_active_run"


@pytest.mark.asyncio
async def test_strategy_health_surfaces_fresh_evidence_for_current_review_artifact(client, session, monkeypatch, tmp_path):
    import json

    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=2))
    await session.commit()

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    generated_at = now.isoformat()
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.md").write_text(
        "# Default Strategy Review\n\n"
        f"**Date:** {now.date().isoformat()}\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `watch`\n",
        encoding="utf-8",
    )
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "review_verdict": {"verdict": "watch"},
                "strategy_run": {
                    "id": str(strategy_run.id),
                    "started_at": strategy_run.started_at.isoformat(),
                    "status": strategy_run.status,
                    "contract_snapshot": strategy_run.contract_snapshot,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    response = await client.get("/api/v1/paper-trading/strategy-health")

    assert response.status_code == 200
    freshness = response.json()["evidence_freshness"]
    assert freshness["status"] == "fresh"
    assert freshness["latest_review_generated_at"] == generated_at
    assert freshness["review_outdated"] is False
    assert freshness["artifact_identity_status"] == "match"
    assert freshness["artifact_run_matches_active_run"] is True
    assert freshness["last_activity_kind"] == "strategy_run_started"
    assert freshness["review_lag_seconds"] == 0


@pytest.mark.asyncio
async def test_strategy_health_surfaces_stale_evidence_when_review_lags_run_activity(client, session, monkeypatch, tmp_path):
    import json

    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    monkeypatch.setattr(analysis_module.settings, "paper_trading_pending_decision_max_age_seconds", 300)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=2))
    market = make_market(session, question="Evidence freshness market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=1),
        dedupe_bucket=(now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        details={"direction": "up", "market_question": "Evidence freshness market", "outcome_name": "Yes"},
    )
    await session.flush()
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=now - timedelta(minutes=30),
            decision_status="pending_decision",
            action="skip",
            reason_code="pending_decision",
            details={"reason_label": "Pending decision"},
        )
    )
    await session.commit()

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    review_generated_at = (now - timedelta(hours=3)).isoformat()
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.md").write_text(
        "# Default Strategy Review\n\n"
        f"**Date:** {now.date().isoformat()}\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `watch`\n",
        encoding="utf-8",
    )
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.json").write_text(
        json.dumps(
            {
                "generated_at": review_generated_at,
                "review_verdict": {"verdict": "watch"},
                "strategy_run": {
                    "id": str(strategy_run.id),
                    "started_at": strategy_run.started_at.isoformat(),
                    "status": strategy_run.status,
                    "contract_snapshot": strategy_run.contract_snapshot,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    response = await client.get("/api/v1/paper-trading/default-strategy/dashboard")

    assert response.status_code == 200
    freshness = response.json()["strategy_health"]["evidence_freshness"]
    assert freshness["status"] == "stale"
    assert freshness["review_outdated"] is True
    assert freshness["pending_decisions_stale"] is True
    assert freshness["last_activity_kind"] == "execution_decision"
    assert freshness["review_lag_seconds"] == 9000


@pytest.mark.asyncio
async def test_strategy_health_marks_review_identity_mismatch_as_stale(client, session, monkeypatch, tmp_path):
    import json

    import app.reports.strategy_review as review_module

    now = datetime.now(timezone.utc)
    await open_default_strategy_run(
        session,
        launch_boundary_at=now - timedelta(days=2),
        contract_metadata={
            "contract_version": "default_strategy_v0.4.1",
            "evidence_boundary": {
                "boundary_id": "v0.4.1",
                "release_tag": "v0.4.1",
                "migration_revision": "038",
            },
        },
    )
    await session.commit()

    review_dir = tmp_path / "docs" / "strategy-reviews"
    review_dir.mkdir(parents=True)
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.md").write_text(
        "# Default Strategy Review\n\n"
        f"**Date:** {now.date().isoformat()}\n\n"
        "## Operator Verdict\n\n"
        "- Verdict: `watch`\n",
        encoding="utf-8",
    )
    (review_dir / f"{now.date().isoformat()}-default-strategy-baseline.json").write_text(
        json.dumps(
            {
                "generated_at": now.isoformat(),
                "review_verdict": {"verdict": "watch"},
                "strategy_run": {
                    "id": "older-run-id",
                    "started_at": (now - timedelta(days=5)).isoformat(),
                    "status": "closed",
                    "contract_snapshot": {
                        "contract_version": "default_strategy_v0.4.0",
                        "evidence_boundary": {
                            "boundary_id": "v0.4.0",
                            "release_tag": "v0.4.0",
                            "migration_revision": "037",
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(review_module, "_repo_root", lambda: tmp_path)

    response = await client.get("/api/v1/paper-trading/strategy-health")

    assert response.status_code == 200
    payload = response.json()
    latest_review = payload["latest_review_artifact"]
    freshness = payload["evidence_freshness"]

    assert latest_review["strategy_run_ref"]["id"] == "older-run-id"
    assert latest_review["contract_ref"]["evidence_boundary_id"] == "v0.4.0"
    assert freshness["status"] == "stale"
    assert freshness["artifact_identity_status"] == "mismatch"
    assert freshness["artifact_run_matches_active_run"] is False
    assert freshness["artifact_contract_version_matches_active_run"] is False
    assert freshness["artifact_evidence_boundary_matches_active_run"] is False
    assert freshness["summary"] == (
        "The latest review artifact belongs to a different default-strategy run than the active baseline."
    )


@pytest.mark.asyncio
async def test_default_strategy_run_requires_explicit_bootstrap(client, session):
    launch_at = datetime.now(timezone.utc) - timedelta(hours=6)

    lookup_before = await client.get("/api/v1/paper-trading/default-strategy/run")
    assert lookup_before.status_code == 200
    assert lookup_before.json()["state"] == "no_active_run"

    bootstrap = await client.post(
        "/api/v1/paper-trading/default-strategy/bootstrap",
        json={"launch_boundary_at": launch_at.isoformat()},
    )
    assert bootstrap.status_code == 200
    payload = bootstrap.json()
    assert payload["state"] == "active_run"
    assert payload["strategy_run"]["started_at"] == launch_at.isoformat()

    lookup_after = await client.get("/api/v1/paper-trading/default-strategy/run")
    assert lookup_after.status_code == 200
    assert lookup_after.json()["state"] == "active_run"
    assert lookup_after.json()["strategy_run"]["id"] == payload["strategy_run"]["id"]

    run_count = int((await session.execute(select(func.count(StrategyRun.id)))).scalar_one() or 0)
    assert run_count == 1


@pytest.mark.asyncio
async def test_default_strategy_scoped_reads_bypass_heavy_health_scope(client, session, monkeypatch):
    started_at = datetime.now(timezone.utc) - timedelta(days=1)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=started_at)
    market = make_market(session, question="Scoped read market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=2),
        estimated_probability=Decimal("0.6400"),
        probability_adjustment=Decimal("0.1400"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.140000"),
        details={"direction": "up", "market_question": "Scoped read market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("75.00"),
        shadow_pnl=Decimal("55.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=started_at + timedelta(hours=3),
        opened_at=started_at + timedelta(hours=2),
        details={"market_question": "Scoped read market"},
    )
    await session.commit()

    async def _unexpected_scope(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("scoped portfolio reads should not use the heavyweight strategy-health scope")

    monkeypatch.setattr(analysis_module, "_get_default_strategy_scope", _unexpected_scope)

    portfolio = await client.get("/api/v1/paper-trading/portfolio?scope=default_strategy")
    history = await client.get("/api/v1/paper-trading/history?scope=default_strategy")
    metrics = await client.get("/api/v1/paper-trading/metrics?scope=default_strategy")
    pnl_curve = await client.get("/api/v1/paper-trading/pnl-curve?scope=default_strategy")

    assert portfolio.status_code == 200
    assert portfolio.json()["total_resolved"] == 1
    assert history.status_code == 200
    assert history.json()["total"] == 1
    assert metrics.status_code == 200
    assert metrics.json()["cumulative_pnl"] == 75.0
    assert pnl_curve.status_code == 200
    assert pnl_curve.json()[0]["trade_pnl"] == 75.0


@pytest.mark.asyncio
async def test_default_strategy_bootstrap_persists_evidence_boundary_metadata(client):
    launch_at = datetime.now(timezone.utc) - timedelta(hours=1)
    response = await client.post(
        "/api/v1/paper-trading/default-strategy/bootstrap",
        json={
            "launch_boundary_at": launch_at.isoformat(),
            "evidence_boundary_id": "v0.4.1",
            "release_tag": "v0.4.1",
            "commit_sha": "87a4315b81b81365d9ee974aff5b130813757897",
            "migration_revision": "038",
            "contract_version": "default_strategy_v0.4.1",
            "evidence_gate": {
                "min_resolved_trades": 20,
                "execution_adjusted_pnl_rule": "positive",
            },
        },
    )

    assert response.status_code == 200
    strategy_run = response.json()["strategy_run"]
    assert strategy_run["contract_snapshot"]["contract_version"] == "default_strategy_v0.4.1"
    assert strategy_run["contract_snapshot"]["evidence_boundary"]["boundary_id"] == "v0.4.1"
    assert strategy_run["contract_snapshot"]["evidence_boundary"]["migration_revision"] == "038"
    assert strategy_run["contract_snapshot"]["evidence_gate"]["min_resolved_trades"] == 20


@pytest.mark.asyncio
async def test_strategy_health_funnel_reconciles_qualified_opened_skipped_and_pending(client, session, monkeypatch):
    risk_calls = 0

    async def _mixed_risk(*args, **kwargs):  # noqa: ARG001
        nonlocal risk_calls
        risk_calls += 1
        if risk_calls == 2:
            return {
                "approved": False,
                "approved_size_usd": Decimal("0"),
                "reason": "inventory_cap",
                "drawdown_active": False,
                "risk_mode": "graph",
                "recommendation": {
                    "recommendation_type": "block",
                    "reason_code": "inventory_cap",
                    "details_json": {"source": "test"},
                },
            }
        return None

    monkeypatch.setattr(engine_module, "assess_paper_trade_risk", _mixed_risk)
    now = datetime.now(timezone.utc)
    started_at = now - timedelta(days=1)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=started_at)
    market = make_market(session, question="Funnel balance market")
    outcome = make_outcome(session, market.id, name="Yes")

    opened_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=3),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Funnel balance market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=opened_signal.fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )

    skipped_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=2),
        estimated_probability=Decimal("0.6400"),
        probability_adjustment=Decimal("0.2400"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.240000"),
        details={"direction": "up", "market_question": "Funnel balance market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=skipped_signal.fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )

    pending_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=1),
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.2300"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.230000"),
        details={
            "direction": "up",
            "market_question": "Funnel balance market",
            "outcome_name": "Yes",
            "default_strategy": {
                "decision": "skipped",
                "reason_code": "bogus_signal_blob_skip",
                "reason_label": "Bogus legacy blob skip",
            },
        },
    )
    await session.commit()

    opened_result = await attempt_open_trade(
        session=session,
        signal_id=opened_signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=opened_signal.estimated_probability,
        market_price=opened_signal.price_at_fire,
        market_question="Funnel balance market",
        fired_at=opened_signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    skipped_result = await attempt_open_trade(
        session=session,
        signal_id=skipped_signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=skipped_signal.estimated_probability,
        market_price=skipped_signal.price_at_fire,
        market_question="Funnel balance market",
        fired_at=skipped_signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    pending_decision = await ensure_pending_execution_decision(
        session=session,
        signal_id=pending_signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=pending_signal.estimated_probability,
        market_price=pending_signal.price_at_fire,
        market_question="Funnel balance market",
        fired_at=pending_signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert opened_result.trade is not None
    assert skipped_result.trade is None
    assert pending_decision is not None
    assert skipped_result.reason_code == "risk_shared_global_block"

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    data = response.json()
    funnel = data["trade_funnel"]

    assert funnel["qualified_signals"] == 3
    assert funnel["opened_trade_signals"] == 1
    assert funnel["skipped_signals"] == 1
    assert funnel["pending_decision_signals"] == 1
    assert funnel["qualified_signals"] == (
        funnel["opened_trade_signals"] + funnel["skipped_signals"] + funnel["pending_decision_signals"]
    )
    assert funnel["integrity_errors"] == []
    assert {row["reason_code"] for row in data["skip_reasons"]} == {"risk_shared_global_block"}


@pytest.mark.asyncio
async def test_strategy_health_flags_missing_execution_decision_as_integrity_error(client, session):
    now = datetime.now(timezone.utc)
    started_at = now - timedelta(days=1)
    await open_default_strategy_run(session, launch_boundary_at=started_at)
    market = make_market(session, question="Missing decision market")
    outcome = make_outcome(session, market.id, name="Yes")

    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=1),
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.2300"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.230000"),
        details={"direction": "up", "market_question": "Missing decision market", "outcome_name": "Yes"},
    )
    await session.commit()

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    data = response.json()
    funnel = data["trade_funnel"]

    assert funnel["qualified_signals"] == 1
    assert funnel["pending_decision_signals"] == 0
    assert funnel["conservation_holds"] is False
    assert funnel["integrity_errors"] == [
        {
            "signal_id": str(signal.id),
            "error": "missing_execution_decision",
        }
    ]
    assert data["run_integrity"]["integrity_errors"] == funnel["integrity_errors"]


@pytest.mark.asyncio
async def test_attempt_open_trade_rehydrates_incomplete_strategy_run_state_from_resolved_trades(session):
    market = make_market(session, question="Risk state hydrate market")
    outcome = make_outcome(session, market.id, name="Yes")
    now = datetime.now(timezone.utc)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=1))

    resolved_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=6),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Risk state hydrate market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        resolved_signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("125.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(hours=5),
        opened_at=now - timedelta(hours=6),
    )

    fresh_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(minutes=5),
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Risk state hydrate market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fresh_signal.fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )

    strategy_run.peak_equity = None
    strategy_run.current_equity = None
    strategy_run.max_drawdown = None
    strategy_run.drawdown_pct = None
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=fresh_signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=fresh_signal.estimated_probability,
        market_price=fresh_signal.price_at_fire,
        market_question="Risk state hydrate market",
        fired_at=fresh_signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.refresh(strategy_run)

    assert result.reason_code == "opened"
    assert result.execution_decision is not None
    assert strategy_run.current_equity == Decimal("10125.00")
    assert strategy_run.peak_equity == Decimal("10125.00")
    assert strategy_run.max_drawdown == Decimal("0.00")
    assert strategy_run.drawdown_pct == Decimal("0.000000")


@pytest.mark.asyncio
async def test_attempt_open_trade_labels_shared_global_risk_blocks_separately(session, monkeypatch):
    async def _block_globally(*args, **kwargs):  # noqa: ARG001
        return {
            "approved": False,
            "approved_size_usd": Decimal("0"),
            "reason": "inventory_cap",
            "drawdown_active": False,
            "risk_mode": "graph",
            "recommendation": {
                "recommendation_type": "block",
                "reason_code": "inventory_cap",
                "details_json": {"source": "test"},
            },
        }

    monkeypatch.setattr(engine_module, "assess_paper_trade_risk", _block_globally)

    market = make_market(session, question="Shared risk market")
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
        details={"direction": "up", "market_question": "Shared risk market", "outcome_name": "Yes"},
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
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Shared risk market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    assert result.execution_decision is not None
    assert result.reason_code == "risk_shared_global_block"
    assert result.execution_decision.details["risk_result"]["risk_scope"] == "shared_global"
    assert result.execution_decision.details["risk_result"]["original_reason_code"] == "inventory_cap"


@pytest.mark.asyncio
async def test_attempt_open_trade_uses_existing_pending_execution_decision(session):
    market = make_market(session, question="Pending reuse market")
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
        details={"direction": "up", "market_question": "Pending reuse market", "outcome_name": "Yes"},
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
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    await session.commit()

    pending_result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Pending reuse market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
        precheck_reason_code="pending_decision",
        precheck_reason_label="Pending decision",
    )
    await session.commit()

    assert pending_result.trade is None
    assert pending_result.execution_decision is not None
    pending_decision_id = pending_result.execution_decision.id
    assert pending_result.execution_decision.decision_status == "pending_decision"
    assert pending_result.execution_decision.reason_code == "pending_decision"

    resolved_result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Pending reuse market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert resolved_result.trade is not None
    assert resolved_result.execution_decision is not None
    assert resolved_result.execution_decision.id == pending_decision_id
    assert resolved_result.execution_decision.decision_status == "opened"
    decision_count = int(
        (
            await session.execute(
                select(func.count(ExecutionDecision.id)).where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.signal_id == signal.id,
                )
            )
        ).scalar_one()
        or 0
    )
    assert decision_count == 1


@pytest.mark.asyncio
async def test_strategy_health_surfaces_pending_decision_age_watch(client, session):
    now = datetime.now(timezone.utc)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=1))
    market = make_market(session, question="Pending age market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(hours=3),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        details={"direction": "up", "market_question": "Pending age market", "outcome_name": "Yes"},
    )
    await session.flush()
    session.add(
        ExecutionDecision(
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=now - timedelta(hours=2),
            decision_status="pending_decision",
            action="skip",
            reason_code="pending_decision",
            details={"reason_label": "Pending decision"},
        )
    )
    await session.commit()

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    pending_watch = response.json()["pending_decision_watch"]

    assert pending_watch["count"] == 1
    assert pending_watch["oldest_decision_at"] is not None
    assert pending_watch["max_age_seconds"] >= 7200
    assert pending_watch["stale_count"] == 1
    assert pending_watch["retry_window_seconds"] == float(analysis_module.settings.paper_trading_pending_decision_max_age_seconds)
    assert pending_watch["reason_counts"] == {"pending_decision": 1}
    assert pending_watch["examples"][0]["signal_id"] == str(signal.id)


@pytest.mark.asyncio
async def test_strategy_health_review_verdict_surfaces_evidence_gate_blockers(client, session, monkeypatch):
    now = datetime.now(timezone.utc)
    missing_dedupe_bucket = (now - timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)
    pending_dedupe_bucket = now.replace(minute=0, second=0, microsecond=0)
    monkeypatch.setattr(analysis_module.settings, "paper_trading_pending_decision_max_age_seconds", 300)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(hours=1))
    market = make_market(session, question="Verdict blocker market")
    outcome = make_outcome(session, market.id, name="Yes")

    missing_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(minutes=50),
        dedupe_bucket=missing_dedupe_bucket,
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        details={"direction": "up", "market_question": "Verdict blocker market", "outcome_name": "Yes"},
    )
    pending_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=now - timedelta(minutes=45),
        dedupe_bucket=pending_dedupe_bucket,
        estimated_probability=Decimal("0.6300"),
        probability_adjustment=Decimal("0.1300"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.130000"),
        details={"direction": "up", "market_question": "Verdict blocker market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        fired_at=now - timedelta(minutes=40),
        details={"direction": "up", "market_question": "Verdict blocker market", "outcome_name": "Yes"},
    )
    await session.flush()
    session.add(
        ExecutionDecision(
            signal_id=pending_signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=now - timedelta(minutes=44),
            decision_status="pending_decision",
            action="skip",
            reason_code="pending_decision",
            details={"reason_label": "Pending decision"},
        )
    )
    await session.commit()

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    data = response.json()

    assert data["replay"]["coverage_scope"] == "default_strategy_review"
    assert data["replay"]["coverage_mode"] == "supported_detectors_only"
    assert data["replay"]["global_coverage_mode"] == "partial_supported_detectors"
    assert data["replay"]["unsupported_detectors"] == []
    assert data["replay"]["global_unsupported_detectors"] == ["price_move"]
    assert data["trade_funnel"]["conservation_holds"] is False
    assert data["trade_funnel"]["integrity_errors"] == [
        {
            "signal_id": str(missing_signal.id),
            "error": "missing_execution_decision",
        }
    ]
    assert [row["code"] for row in data["review_verdict"]["blockers"]] == [
        "insufficient_observation_days",
        "stale_pending_decisions",
        "funnel_conservation_failure",
        "integrity_errors",
    ]
    assert data["review_verdict"]["verdict"] == "not_ready"
    assert data["review_verdict"]["reason_code"] == "blocked"
    assert data["review_verdict"]["threshold_version"] == "default_strategy_review_v1"
    assert data["review_verdict"]["precedence"] == "blockers_first"
    assert data["review_verdict"]["signals"] == {
        "execution_adjusted_pnl_sign": "flat",
        "signal_level_pnl_per_share_sign": "flat",
        "avg_clv_sign": "missing",
    }


@pytest.mark.asyncio
async def test_strategy_health_headline_counts_overdue_open_trades(client, session):
    now = datetime.now(timezone.utc)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=now - timedelta(days=1))

    overdue_market = make_market(
        session,
        platform="kalshi",
        platform_id="KXSTRAT-OVERDUE",
        question="Overdue strategy market",
        end_date=now - timedelta(hours=2),
        active=True,
    )
    overdue_outcome = make_outcome(
        session,
        overdue_market.id,
        name="Yes",
        platform_outcome_id="KXSTRAT-OVERDUE_yes",
    )
    overdue_signal = make_signal(
        session,
        overdue_market.id,
        overdue_outcome.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Overdue strategy market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        overdue_signal.id,
        overdue_outcome.id,
        overdue_market.id,
        strategy_run_id=strategy_run.id,
        status="open",
    )

    fresh_market = make_market(
        session,
        platform="kalshi",
        platform_id="KXSTRAT-FUTURE",
        question="Future strategy market",
        end_date=now + timedelta(hours=2),
        active=True,
    )
    fresh_outcome = make_outcome(
        session,
        fresh_market.id,
        name="Yes",
        platform_outcome_id="KXSTRAT-FUTURE_yes",
    )
    fresh_signal = make_signal(
        session,
        fresh_market.id,
        fresh_outcome.id,
        signal_type="confluence",
        details={"direction": "up", "market_question": "Future strategy market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        fresh_signal.id,
        fresh_outcome.id,
        fresh_market.id,
        strategy_run_id=strategy_run.id,
        status="open",
    )
    await session.commit()

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    headline = response.json()["headline"]

    assert headline["open_trades"] == 2
    assert headline["overdue_open_trades"] == 1


@pytest.mark.asyncio
async def test_drawdown_breaker_uses_persisted_run_peak_equity(session, monkeypatch):
    async def _disable_shared_risk(*args, **kwargs):  # noqa: ARG001
        return None

    monkeypatch.setattr(engine_module, "assess_paper_trade_risk", _disable_shared_risk)
    monkeypatch.setattr(engine_module.settings, "drawdown_circuit_breaker_pct", 0.005)

    started_at = datetime.now(timezone.utc) - timedelta(days=1)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=started_at)

    first_market = make_market(session, question="Winner market")
    first_outcome = make_outcome(session, first_market.id, name="Yes")
    second_market = make_market(session, question="Giveback market")
    second_outcome = make_outcome(session, second_market.id, name="Yes")
    third_market = make_market(session, question="Breaker market")
    third_outcome = make_outcome(session, third_market.id, name="Yes")
    first_signal = make_signal(
        session,
        first_market.id,
        first_outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=1),
        details={"direction": "up", "market_question": "Winner market", "outcome_name": "Yes"},
    )
    second_signal = make_signal(
        session,
        second_market.id,
        second_outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=2),
        details={"direction": "down", "market_question": "Giveback market", "outcome_name": "Yes"},
    )
    await session.flush()

    _make_paper_trade(
        session,
        signal_id=first_signal.id,
        outcome_id=first_outcome.id,
        market_id=first_market.id,
        strategy_run_id=strategy_run.id,
        direction="buy_yes",
        entry_price=Decimal("0.500000"),
        size_usd=Decimal("100.00"),
        shares=Decimal("200.0000"),
        status="open",
    )
    await session.commit()
    resolved_first = await resolve_trades(session, first_outcome.id, outcome_won=True, strategy_run_id=strategy_run.id)
    assert resolved_first == 1

    _make_paper_trade(
        session,
        signal_id=second_signal.id,
        outcome_id=second_outcome.id,
        market_id=second_market.id,
        strategy_run_id=strategy_run.id,
        direction="buy_no",
        entry_price=Decimal("0.400000"),
        size_usd=Decimal("80.00"),
        shares=Decimal("200.0000"),
        status="open",
    )
    await session.commit()
    resolved_second = await resolve_trades(session, second_outcome.id, outcome_won=True, strategy_run_id=strategy_run.id)
    assert resolved_second == 1

    await session.refresh(strategy_run)
    assert strategy_run.current_equity == Decimal("10020.00")
    assert strategy_run.peak_equity == Decimal("10100.00")
    assert strategy_run.max_drawdown == Decimal("80.00")
    assert strategy_run.drawdown_pct == Decimal("0.007921")

    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    signal = make_signal(
        session,
        third_market.id,
        third_outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        estimated_probability=Decimal("0.6500"),
        probability_adjustment=Decimal("0.2500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        details={"direction": "up", "market_question": "Breaker market", "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        third_outcome.id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=third_outcome.id,
        market_id=third_market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="Breaker market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is not None
    assert result.trade.details["drawdown_active"] is True
    assert result.execution_decision is not None
    assert result.execution_decision.details["risk_result"]["drawdown_active"] is True


@pytest.mark.asyncio
async def test_strategy_health_uses_persisted_drawdown_state_for_headline(client, session):
    started_at = datetime.now(timezone.utc) - timedelta(days=1)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=started_at)
    strategy_run.current_equity = Decimal("10020.00")
    strategy_run.peak_equity = Decimal("10150.00")
    strategy_run.max_drawdown = Decimal("130.00")
    strategy_run.drawdown_pct = Decimal("0.012808")

    market = make_market(session, question="Persisted drawdown market")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=1),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.050000"),
        profit_loss=Decimal("0.100000"),
        details={"direction": "up", "market_question": "Persisted drawdown market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("40.00"),
        shadow_pnl=Decimal("30.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=datetime.now(timezone.utc),
        opened_at=started_at + timedelta(hours=1),
        details={"market_question": "Persisted drawdown market"},
    )
    await session.commit()

    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    data = response.json()

    assert data["headline"]["current_equity"] == 10020.0
    assert data["headline"]["peak_equity"] == 10150.0
    assert data["headline"]["max_drawdown"] == 130.0
    assert data["headline"]["drawdown_pct"] == pytest.approx(0.012808)
    assert data["run_integrity"]["debug_drawdown"]["reconstructed_max_drawdown"] == 0.0


@pytest.mark.asyncio
async def test_comparison_modes_keep_signal_and_execution_units_separate(session):
    now = datetime.now(timezone.utc)
    started_at = now - timedelta(days=3)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=started_at)
    market = make_market(session, question="Comparison market")
    outcome = make_outcome(session, market.id, name="Yes")

    default_signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=started_at + timedelta(hours=1),
        estimated_probability=Decimal("0.6200"),
        probability_adjustment=Decimal("0.1200"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.120000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.050000"),
        profit_loss=Decimal("0.100000"),
        details={"direction": "up", "market_question": "Comparison market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        fired_at=started_at + timedelta(hours=2),
        rank_score=Decimal("0.700"),
        estimated_probability=Decimal("0.5600"),
        probability_adjustment=Decimal("0.0600"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.060000"),
        resolved=True,
        resolved_correctly=False,
        clv=Decimal("-0.020000"),
        profit_loss=Decimal("-0.100000"),
        details={"direction": "up", "market_question": "Comparison market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        rank_score=Decimal("0.900"),
        fired_at=started_at + timedelta(hours=3),
        estimated_probability=Decimal("0.6100"),
        probability_adjustment=Decimal("0.1100"),
        price_at_fire=Decimal("0.500000"),
        expected_value=Decimal("0.110000"),
        resolved=True,
        resolved_correctly=True,
        clv=Decimal("0.040000"),
        profit_loss=Decimal("0.090000"),
        details={"direction": "up", "market_question": "Comparison market", "outcome_name": "Yes"},
    )
    _make_paper_trade(
        session,
        default_signal.id,
        outcome.id,
        market.id,
        strategy_run_id=strategy_run.id,
        status="resolved",
        pnl=Decimal("125.00"),
        shadow_pnl=Decimal("100.00"),
        exit_price=Decimal("1.000000"),
        resolved_at=now - timedelta(days=1),
        opened_at=started_at + timedelta(hours=1),
        details={"market_question": "Comparison market"},
    )
    await session.commit()

    comparison = await compare_strategy_measurement_modes(
        session,
        start_date=started_at,
        end_date=now,
        strategy_run_id=strategy_run.id,
    )

    signal_level = comparison["signal_level"]
    execution_adjusted = comparison["execution_adjusted"]

    assert signal_level["unit"] == "per_share"
    assert "cumulative_pnl" not in signal_level["default_strategy"]
    assert "cumulative_pnl" not in signal_level["benchmark"]
    assert signal_level["default_strategy"]["resolved_signals"] == 2
    assert signal_level["benchmark"]["resolved_signals"] == 1
    assert execution_adjusted["unit"] == "usd"
    assert "total_profit_loss_per_share" not in execution_adjusted["default_strategy"]
    assert execution_adjusted["benchmark"]["available"] is False
    assert execution_adjusted["benchmark"]["reason"] == "legacy_execution_adjusted_unavailable"


@pytest.mark.asyncio
async def test_strategy_health_never_reports_local_total_exposure_for_shared_global_block(client, session, monkeypatch):
    async def _block_globally(*args, **kwargs):  # noqa: ARG001
        return {
            "approved": False,
            "approved_size_usd": Decimal("0"),
            "reason": "inventory_cap",
            "drawdown_active": False,
            "risk_mode": "graph",
            "recommendation": {
                "recommendation_type": "block",
                "reason_code": "inventory_cap",
                "details_json": {"source": "test"},
            },
        }

    monkeypatch.setattr(engine_module, "assess_paper_trade_risk", _block_globally)

    fired_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    strategy_run = await open_default_strategy_run(session, launch_boundary_at=fired_at - timedelta(minutes=1))
    market = make_market(session, question="No local exposure market")
    outcome = make_outcome(session, market.id, name="Yes")
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
        details={"direction": "up", "market_question": "No local exposure market", "outcome_name": "Yes"},
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

    result = await attempt_open_trade(
        session=session,
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        market_question="No local exposure market",
        fired_at=signal.fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is None
    response = await client.get("/api/v1/paper-trading/strategy-health")
    assert response.status_code == 200
    data = response.json()

    assert data["risk_blocks"]["local_paper_book_blocks"] == 0
    assert data["risk_blocks"]["shared_global_blocks"] == 1
    assert data["risk_blocks"]["shared_global_upstream_reason_counts"] == {"inventory_cap": 1}
    assert "risk_local_total_exposure" not in data["risk_blocks"]["local_reason_counts"]
    assert "risk_local_total_exposure" not in {row["reason_code"] for row in data["skip_reasons"]}


@pytest.mark.asyncio
async def test_replay_status_reports_partial_supported_detector_coverage(session):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    market = make_market(session, question="Coverage market")
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=recent,
        details={"direction": "up", "market_question": "Coverage market", "outcome_name": "Yes"},
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        fired_at=recent + timedelta(minutes=5),
        details={"direction": "up", "market_question": "Coverage market", "outcome_name": "Yes"},
    )
    await session.commit()

    status = await fetch_polymarket_replay_status(session)

    assert status["coverage_mode"] == "partial_supported_detectors"
    assert status["supported_detectors"] == ["confluence"]
    assert status["unsupported_detectors"] == ["price_move"]
