from datetime import datetime, timedelta, timezone

from app.paper_trading.evidence_freshness import build_evidence_freshness


def test_evidence_freshness_reports_no_active_run():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="no_active_run",
        latest_review_artifact={"generation_status": "missing", "generated_at": None},
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


def test_evidence_freshness_reports_stale_when_review_lags_run_activity_and_pending_decisions():
    now = datetime.now(timezone.utc)

    freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact={
            "generation_status": "complete",
            "generated_at": (now - timedelta(hours=3)).isoformat(),
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
