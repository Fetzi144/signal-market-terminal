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


def _identity_value(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _match_identity(active_value: Any, artifact_value: Any) -> bool | None:
    active_identity = _identity_value(active_value)
    artifact_identity = _identity_value(artifact_value)
    if active_identity is None or artifact_identity is None:
        return None
    return active_identity == artifact_identity


def _artifact_contract_ref(latest_review_artifact: dict[str, Any]) -> dict[str, Any]:
    contract_ref = latest_review_artifact.get("contract_ref")
    return contract_ref if isinstance(contract_ref, dict) else {}


def _active_contract_ref(active_strategy_run: dict[str, Any] | None) -> dict[str, Any]:
    strategy_run = active_strategy_run if isinstance(active_strategy_run, dict) else {}
    contract_snapshot = strategy_run.get("contract_snapshot")
    return contract_snapshot if isinstance(contract_snapshot, dict) else {}


def _evidence_boundary_identity(boundary: dict[str, Any]) -> str | None:
    if not isinstance(boundary, dict):
        return None
    return _identity_value(boundary.get("boundary_id")) or _identity_value(boundary.get("release_tag"))


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


def _artifact_identity_alignment(
    *,
    run_state: str,
    latest_review_artifact: dict[str, Any],
    active_strategy_run: dict[str, Any] | None,
) -> tuple[str, str, bool | None, bool | None, bool | None]:
    if run_state != "active_run":
        return (
            "not_applicable",
            "No active default-strategy run exists, so review/artifact identity matching is not applicable.",
            None,
            None,
            None,
        )

    if latest_review_artifact.get("generation_status") == "missing":
        return (
            "missing_review",
            "No default-strategy review artifact exists yet, so there is nothing to match against the active run.",
            None,
            None,
            None,
        )

    strategy_run_ref = latest_review_artifact.get("strategy_run_ref")
    strategy_run_ref = strategy_run_ref if isinstance(strategy_run_ref, dict) else {}
    contract_ref = _artifact_contract_ref(latest_review_artifact)
    active_contract = _active_contract_ref(active_strategy_run)
    active_boundary = active_contract.get("evidence_boundary")
    active_boundary = active_boundary if isinstance(active_boundary, dict) else {}

    artifact_run_matches_active_run = _match_identity(
        (active_strategy_run or {}).get("id"),
        strategy_run_ref.get("id"),
    )
    artifact_contract_version_matches_active_run = _match_identity(
        active_contract.get("contract_version"),
        contract_ref.get("contract_version"),
    )
    artifact_evidence_boundary_matches_active_run = _match_identity(
        _evidence_boundary_identity(active_boundary),
        contract_ref.get("evidence_boundary_id") or contract_ref.get("release_tag"),
    )

    if artifact_run_matches_active_run is False:
        return (
            "mismatch",
            "The latest review artifact belongs to a different default-strategy run than the active baseline.",
            artifact_run_matches_active_run,
            artifact_contract_version_matches_active_run,
            artifact_evidence_boundary_matches_active_run,
        )

    if (
        artifact_contract_version_matches_active_run is False
        or artifact_evidence_boundary_matches_active_run is False
    ):
        return (
            "mismatch",
            "The latest review artifact does not match the active default-strategy contract boundary.",
            artifact_run_matches_active_run,
            artifact_contract_version_matches_active_run,
            artifact_evidence_boundary_matches_active_run,
        )

    if artifact_run_matches_active_run is True:
        return (
            "match",
            "The latest review artifact matches the active default-strategy run and benchmark boundary.",
            artifact_run_matches_active_run,
            artifact_contract_version_matches_active_run,
            artifact_evidence_boundary_matches_active_run,
        )

    return (
        "unknown",
        "The latest review artifact does not expose enough JSON identity metadata to verify the active-run match.",
        artifact_run_matches_active_run,
        artifact_contract_version_matches_active_run,
        artifact_evidence_boundary_matches_active_run,
    )


def build_evidence_freshness(
    *,
    observed_at: datetime,
    run_state: str,
    latest_review_artifact: dict[str, Any],
    active_strategy_run: dict[str, Any] | None,
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
    (
        artifact_identity_status,
        artifact_identity_summary,
        artifact_run_matches_active_run,
        artifact_contract_version_matches_active_run,
        artifact_evidence_boundary_matches_active_run,
    ) = _artifact_identity_alignment(
        run_state=run_state,
        latest_review_artifact=latest_review_artifact,
        active_strategy_run=active_strategy_run,
    )

    if run_state != "active_run":
        status = "no_active_run"
        summary = "No active default-strategy run exists yet, so evidence freshness is not applicable."
    elif latest_review_artifact.get("generation_status") == "missing" or latest_review_generated_at is None:
        status = "missing_review"
        summary = "No default-strategy review artifact has been generated for the active run yet."
    elif artifact_identity_status == "mismatch":
        status = "stale"
        summary = artifact_identity_summary
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
        "artifact_identity_status": artifact_identity_status,
        "artifact_identity_summary": artifact_identity_summary,
        "artifact_run_matches_active_run": artifact_run_matches_active_run,
        "artifact_contract_version_matches_active_run": artifact_contract_version_matches_active_run,
        "artifact_evidence_boundary_matches_active_run": artifact_evidence_boundary_matches_active_run,
        "last_activity_at": last_activity_at.isoformat() if last_activity_at else None,
        "last_activity_kind": last_activity_kind,
        "pending_decision_count": pending_count,
        "pending_decision_max_age_seconds": pending_max_age_seconds,
        "pending_decisions_stale": pending_decisions_stale,
        "pending_decision_stale_after_seconds": float(settings.paper_trading_pending_decision_max_age_seconds),
    }
