from datetime import datetime, timedelta, timezone

from app.paper_trading.evidence_freshness import build_evidence_freshness


def test_evidence_freshness_reports_no_active_run():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="no_active_run",
        latest_review_artifact={"generation_status": "missing", "generated_at": None},
        active_strategy_run=None,
        started_at=None,
        latest_trade_activity_at=None,
        latest_decision_at=None,
        pending_watch={"count": 0, "max_age_seconds": 0},
    )

    assert freshness["status"] == "no_active_run"
    assert freshness["review_outdated"] is False
    assert freshness["last_activity_at"] is None


def test_evidence_freshness_reports_missing_review_for_active_run():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact={"generation_status": "missing", "generated_at": None},
        active_strategy_run={"id": "run-1", "contract_snapshot": {}},
        started_at=now - timedelta(days=2),
        latest_trade_activity_at=None,
        latest_decision_at=None,
        pending_watch={"count": 0, "max_age_seconds": 0},
    )

    assert freshness["status"] == "missing_review"
    assert freshness["latest_review_generated_at"] is None


def test_evidence_freshness_reports_fresh_when_review_is_current():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact={
            "generation_status": "complete",
            "generated_at": (now - timedelta(minutes=10)).isoformat(),
            "strategy_run_ref": {"id": "run-1"},
            "contract_ref": {"contract_version": "v1", "evidence_boundary_id": "boundary-1"},
        },
        active_strategy_run={
            "id": "run-1",
            "contract_snapshot": {
                "contract_version": "v1",
                "evidence_boundary": {"boundary_id": "boundary-1"},
            },
        },
        started_at=now - timedelta(days=2),
        latest_trade_activity_at=now - timedelta(hours=1),
        latest_decision_at=now - timedelta(hours=2),
        pending_watch={"count": 1, "max_age_seconds": 60},
    )

    assert freshness["status"] == "fresh"
    assert freshness["review_outdated"] is False
    assert freshness["review_lag_seconds"] == 0
    assert freshness["last_activity_kind"] == "paper_trade"
    assert freshness["artifact_identity_status"] == "match"
    assert freshness["artifact_run_matches_active_run"] is True


def test_evidence_freshness_reports_stale_when_review_lags_run_activity_and_pending_decisions():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact={
            "generation_status": "complete",
            "generated_at": (now - timedelta(hours=3)).isoformat(),
            "strategy_run_ref": {"id": "run-1"},
            "contract_ref": {"contract_version": "v1", "evidence_boundary_id": "boundary-1"},
        },
        active_strategy_run={
            "id": "run-1",
            "contract_snapshot": {
                "contract_version": "v1",
                "evidence_boundary": {"boundary_id": "boundary-1"},
            },
        },
        started_at=now - timedelta(days=2),
        latest_trade_activity_at=now - timedelta(hours=2),
        latest_decision_at=now - timedelta(minutes=20),
        pending_watch={"count": 1, "max_age_seconds": 3600},
    )

    assert freshness["status"] == "stale"
    assert freshness["review_outdated"] is True
    assert freshness["pending_decisions_stale"] is True
    assert freshness["last_activity_kind"] == "execution_decision"
    assert freshness["review_lag_seconds"] == 9600


def test_evidence_freshness_reports_stale_when_review_artifact_targets_different_run():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact={
            "generation_status": "complete",
            "generated_at": (now - timedelta(minutes=5)).isoformat(),
            "strategy_run_ref": {"id": "run-older"},
            "contract_ref": {"contract_version": "v0", "evidence_boundary_id": "boundary-old"},
        },
        active_strategy_run={
            "id": "run-current",
            "contract_snapshot": {
                "contract_version": "v1",
                "evidence_boundary": {"boundary_id": "boundary-current"},
            },
        },
        started_at=now - timedelta(days=2),
        latest_trade_activity_at=now - timedelta(minutes=10),
        latest_decision_at=None,
        pending_watch={"count": 0, "max_age_seconds": 0},
    )

    assert freshness["status"] == "stale"
    assert freshness["artifact_identity_status"] == "mismatch"
    assert freshness["artifact_run_matches_active_run"] is False
    assert freshness["summary"] == (
        "The latest review artifact belongs to a different default-strategy run than the active baseline."
    )
