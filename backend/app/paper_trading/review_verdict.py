"""Default-strategy review verdict contract and threshold evaluation."""
from __future__ import annotations

from typing import Any

from app.config import settings

REVIEW_VERDICT_THRESHOLD_VERSION = "default_strategy_review_v1"
REVIEW_VERDICT_PRECEDENCE = "blockers_first"


def _review_blocker(code: str, label: str, detail: str) -> dict[str, str]:
    return {
        "code": code,
        "label": label,
        "detail": detail,
    }


def _metric_sign(value: Any) -> str:
    if value is None:
        return "missing"
    numeric = float(value)
    if numeric > 0:
        return "positive"
    if numeric < 0:
        return "negative"
    return "flat"


def _review_summary(verdict: str, blockers: list[dict[str, str]], *, reason_code: str) -> str:
    if verdict == "not_ready":
        if blockers:
            return f"Not ready: {blockers[0]['label']} is blocking a clean prove-the-edge verdict."
        return "Not ready: the evidence gate is still blocked."
    if verdict == "keep":
        return "Keep: the run is clean and both signal-level and execution-adjusted default-strategy evidence are positive."
    if verdict == "cut":
        return "Cut: the run is clean but both signal-level and execution-adjusted default-strategy evidence are negative."
    if reason_code == "no_resolved_trades":
        return "Watch: the evidence gate is clear, but there are no resolved default-strategy trades yet."
    if reason_code == "insufficient_consensus":
        return "Watch: the evidence gate is clear, but at least one review input is still flat or missing."
    return "Watch: the evidence gate is clear, but the execution-adjusted, signal-level, and CLV inputs are mixed."


def build_review_verdict(
    *,
    strategy_run: dict | None,
    run_state: str,
    observation: dict,
    trade_funnel: dict,
    pending_watch: dict,
    comparison_modes: dict,
    replay: dict,
    headline: dict,
) -> dict[str, Any]:
    blockers: list[dict[str, str]] = []
    if run_state != "active_run" or strategy_run is None:
        blockers.append(
            _review_blocker(
                "no_active_run",
                "No active run",
                "Bootstrap a default-strategy run explicitly before treating any read surface as evidence.",
            )
        )
    else:
        days_tracked = observation.get("days_tracked")
        minimum_days = int(observation.get("minimum_days") or 0)
        remaining_days = int(observation.get("days_until_minimum_window") or 0)
        if days_tracked is None or float(days_tracked) < minimum_days:
            blockers.append(
                _review_blocker(
                    "insufficient_observation_days",
                    "Insufficient observation days",
                    f"The active run has tracked {days_tracked if days_tracked is not None else 0:.1f} day(s); wait until the minimum {minimum_days} day window is met ({remaining_days} day(s) remaining).",
                )
            )

        pending_count = int(pending_watch.get("count") or 0)
        pending_max_age = float(pending_watch.get("max_age_seconds") or 0.0)
        if pending_count > 0 and pending_max_age > float(settings.paper_trading_pending_decision_max_age_seconds):
            blockers.append(
                _review_blocker(
                    "stale_pending_decisions",
                    "Stale pending decisions",
                    f"{pending_count} pending decision(s) remain, and the oldest is {pending_max_age:.1f}s old versus the {settings.paper_trading_pending_decision_max_age_seconds}s retry window.",
                )
            )

        if not bool(trade_funnel.get("conservation_holds", False)):
            blockers.append(
                _review_blocker(
                    "funnel_conservation_failure",
                    "Funnel conservation failure",
                    "Qualified signals do not reconcile exactly into opened, skipped, and pending decision states.",
                )
            )

        integrity_errors = list(trade_funnel.get("integrity_errors") or [])
        if integrity_errors:
            blockers.append(
                _review_blocker(
                    "integrity_errors",
                    "Integrity errors",
                    f"{len(integrity_errors)} explicit integrity error(s) remain in the qualified default-strategy ledger.",
                )
            )

        coverage_mode = str(replay.get("coverage_mode") or "no_detector_activity")
        if coverage_mode in {"partial_supported_detectors", "unsupported_detectors_only"}:
            unsupported = ", ".join(replay.get("unsupported_detectors") or []) or "unknown"
            blockers.append(
                _review_blocker(
                    "replay_coverage_limited",
                    "Replay coverage limited",
                    f"Replay coverage mode is `{coverage_mode}`; unsupported detector activity is still present ({unsupported}).",
                )
            )

    signal_level_default = ((comparison_modes.get("signal_level") or {}).get("default_strategy") or {})
    execution_adjusted_default = ((comparison_modes.get("execution_adjusted") or {}).get("default_strategy") or {})
    resolved_trades = int(headline.get("resolved_trades") or 0)
    execution_adjusted_pnl = execution_adjusted_default.get("cumulative_pnl", headline.get("cumulative_pnl"))
    signal_level_pnl = signal_level_default.get("total_profit_loss_per_share")
    avg_clv = headline.get("avg_clv")
    execution_adjusted_sign = _metric_sign(execution_adjusted_pnl)
    signal_level_sign = _metric_sign(signal_level_pnl)
    avg_clv_sign = _metric_sign(avg_clv)

    verdict = "not_ready"
    reason_code = "blocked"
    if not blockers:
        if (
            resolved_trades > 0
            and execution_adjusted_sign == "positive"
            and signal_level_sign == "positive"
            and avg_clv_sign == "positive"
        ):
            verdict = "keep"
            reason_code = "positive_consensus"
        elif (
            resolved_trades > 0
            and execution_adjusted_sign == "negative"
            and signal_level_sign == "negative"
            and avg_clv_sign == "negative"
        ):
            verdict = "cut"
            reason_code = "negative_consensus"
        else:
            verdict = "watch"
            if resolved_trades <= 0:
                reason_code = "no_resolved_trades"
            elif "missing" in {execution_adjusted_sign, signal_level_sign, avg_clv_sign} or "flat" in {
                execution_adjusted_sign,
                signal_level_sign,
                avg_clv_sign,
            }:
                reason_code = "insufficient_consensus"
            else:
                reason_code = "mixed_evidence"

    return {
        "verdict": verdict,
        "reason_code": reason_code,
        "summary": _review_summary(verdict, blockers, reason_code=reason_code),
        "threshold_version": REVIEW_VERDICT_THRESHOLD_VERSION,
        "precedence": REVIEW_VERDICT_PRECEDENCE,
        "blockers": blockers,
        "signals": {
            "execution_adjusted_pnl_sign": execution_adjusted_sign,
            "signal_level_pnl_per_share_sign": signal_level_sign,
            "avg_clv_sign": avg_clv_sign,
        },
        "inputs": {
            "run_state": run_state,
            "observation_days": observation.get("days_tracked"),
            "minimum_observation_days": observation.get("minimum_days"),
            "resolved_trades": resolved_trades,
            "execution_adjusted_pnl": execution_adjusted_pnl,
            "signal_level_pnl_per_share": signal_level_pnl,
            "avg_clv": avg_clv,
            "pending_decision_count": pending_watch.get("count"),
            "pending_decision_max_age_seconds": pending_watch.get("max_age_seconds"),
            "funnel_conservation_holds": trade_funnel.get("conservation_holds"),
            "integrity_error_count": len(trade_funnel.get("integrity_errors") or []),
            "replay_coverage_mode": replay.get("coverage_mode"),
        },
    }
