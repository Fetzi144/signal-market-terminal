"""Read-only freshness summary for default-strategy evidence surfaces."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.config import settings


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_seconds(*, newer: datetime | None, older: datetime | None) -> int | None:
    newer = _ensure_utc(newer)
    older = _ensure_utc(older)
    if newer is None or older is None:
        return None
    return max(0, int((newer - older).total_seconds()))


def _latest_activity(
    *,
    started_at: datetime | None,
    latest_trade_activity_at: datetime | None,
    latest_decision_at: datetime | None,
) -> tuple[str | None, datetime | None]:
    latest_kind = None
    latest_at = None
    for kind, value in (
        ("strategy_run_started", started_at),
        ("paper_trade", latest_trade_activity_at),
        ("execution_decision", latest_decision_at),
    ):
        value = _ensure_utc(value)
        if value is None:
            continue
        if latest_at is None or value > latest_at:
            latest_kind = kind
            latest_at = value
    return latest_kind, latest_at


def build_evidence_freshness(
    *,
    observed_at: datetime,
    run_state: str,
    latest_review_artifact: dict[str, Any],
    started_at: datetime | None,
    latest_trade_activity_at: datetime | None,
    latest_decision_at: datetime | None,
    pending_watch: dict[str, Any],
) -> dict[str, Any]:
    observed_at = _ensure_utc(observed_at) or datetime.now(timezone.utc)
    last_activity_kind, last_activity_at = _latest_activity(
        started_at=started_at,
        latest_trade_activity_at=latest_trade_activity_at,
        latest_decision_at=latest_decision_at,
    )
    latest_review_generated_at = _parse_iso_datetime(latest_review_artifact.get("generated_at"))
    review_age_seconds = _age_seconds(newer=observed_at, older=latest_review_generated_at)
    review_lag_seconds = _age_seconds(newer=last_activity_at, older=latest_review_generated_at)
    review_outdated = bool(
        last_activity_at is not None
        and latest_review_generated_at is not None
        and last_activity_at > latest_review_generated_at
    )
    pending_count = int(pending_watch.get("count") or 0)
    pending_max_age_seconds = float(pending_watch.get("max_age_seconds") or 0.0)
    pending_decisions_stale = (
        pending_count > 0
        and pending_max_age_seconds > float(settings.paper_trading_pending_decision_max_age_seconds)
    )

    if run_state != "active_run":
        status = "no_active_run"
        summary = "No active default-strategy run exists yet, so evidence freshness is not applicable."
    elif latest_review_artifact.get("generation_status") == "missing" or latest_review_generated_at is None:
        status = "missing_review"
        summary = "No default-strategy review artifact has been generated for the active run yet."
    elif review_outdated and pending_decisions_stale:
        status = "stale"
        summary = (
            f"The latest review artifact is {review_lag_seconds}s behind the most recent "
            f"{(last_activity_kind or 'run').replace('_', ' ')} activity, and pending decisions are past the retry window."
        )
    elif review_outdated:
        status = "stale"
        summary = (
            f"The latest review artifact is {review_lag_seconds}s behind the most recent "
            f"{(last_activity_kind or 'run').replace('_', ' ')} activity."
        )
    elif pending_decisions_stale:
        status = "stale"
        summary = "Pending decisions are past the retry window even though the latest review artifact is current."
    else:
        status = "fresh"
        summary = "The latest review artifact is current relative to active run activity and pending decisions remain within the retry window."

    return {
        "status": status,
        "summary": summary,
        "latest_review_generation_status": latest_review_artifact.get("generation_status"),
        "latest_review_generated_at": latest_review_generated_at.isoformat() if latest_review_generated_at else None,
        "review_age_seconds": review_age_seconds,
        "review_lag_seconds": review_lag_seconds,
        "review_outdated": review_outdated,
        "last_activity_at": last_activity_at.isoformat() if last_activity_at else None,
        "last_activity_kind": last_activity_kind,
        "pending_decision_count": pending_count,
        "pending_decision_max_age_seconds": pending_max_age_seconds,
        "pending_decisions_stale": pending_decisions_stale,
        "pending_decision_stale_after_seconds": float(settings.paper_trading_pending_decision_max_age_seconds),
    }
