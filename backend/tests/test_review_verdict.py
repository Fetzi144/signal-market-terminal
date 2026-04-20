from app.paper_trading.review_verdict import (
    REVIEW_VERDICT_PRECEDENCE,
    REVIEW_VERDICT_THRESHOLD_VERSION,
    build_review_verdict,
)


def _build_payload(
    *,
    run_state: str = "active_run",
    strategy_run: dict | None = None,
    resolved_trades: int = 1,
    execution_adjusted_pnl: float | None = 10.0,
    signal_level_pnl_per_share: float | None = 0.1,
    avg_clv: float | None = 0.05,
    days_tracked: float = 14.0,
    minimum_days: int = 7,
    pending_count: int = 0,
    pending_max_age_seconds: float = 0.0,
    conservation_holds: bool = True,
    integrity_errors: list[dict] | None = None,
    replay_coverage_mode: str = "supported_detectors_only",
    unsupported_detectors: list[str] | None = None,
):
    return build_review_verdict(
        strategy_run=strategy_run or {"id": "run-1"},
        run_state=run_state,
        observation={
            "days_tracked": days_tracked,
            "minimum_days": minimum_days,
            "days_until_minimum_window": max(0, minimum_days - int(days_tracked)),
        },
        trade_funnel={
            "conservation_holds": conservation_holds,
            "integrity_errors": integrity_errors or [],
        },
        pending_watch={
            "count": pending_count,
            "max_age_seconds": pending_max_age_seconds,
        },
        comparison_modes={
            "signal_level": {
                "default_strategy": {
                    "total_profit_loss_per_share": signal_level_pnl_per_share,
                }
            },
            "execution_adjusted": {
                "default_strategy": {
                    "cumulative_pnl": execution_adjusted_pnl,
                }
            },
        },
        replay={
            "coverage_mode": replay_coverage_mode,
            "unsupported_detectors": unsupported_detectors or [],
        },
        headline={
            "resolved_trades": resolved_trades,
            "avg_clv": avg_clv,
            "cumulative_pnl": execution_adjusted_pnl,
        },
    )


def test_review_verdict_blockers_override_positive_consensus():
    verdict = _build_payload(
        run_state="no_active_run",
        strategy_run=None,
    )

    assert verdict["verdict"] == "not_ready"
    assert verdict["reason_code"] == "blocked"
    assert verdict["threshold_version"] == REVIEW_VERDICT_THRESHOLD_VERSION
    assert verdict["precedence"] == REVIEW_VERDICT_PRECEDENCE
    assert [row["code"] for row in verdict["blockers"]] == ["no_active_run"]
    assert verdict["signals"] == {
        "execution_adjusted_pnl_sign": "positive",
        "signal_level_pnl_per_share_sign": "positive",
        "avg_clv_sign": "positive",
    }


def test_review_verdict_returns_keep_for_positive_consensus():
    verdict = _build_payload()

    assert verdict["verdict"] == "keep"
    assert verdict["reason_code"] == "positive_consensus"
    assert verdict["blockers"] == []


def test_review_verdict_returns_cut_for_negative_consensus():
    verdict = _build_payload(
        execution_adjusted_pnl=-10.0,
        signal_level_pnl_per_share=-0.1,
        avg_clv=-0.05,
    )

    assert verdict["verdict"] == "cut"
    assert verdict["reason_code"] == "negative_consensus"
    assert verdict["blockers"] == []


def test_review_verdict_returns_watch_when_no_resolved_trades_exist():
    verdict = _build_payload(
        resolved_trades=0,
    )

    assert verdict["verdict"] == "watch"
    assert verdict["reason_code"] == "no_resolved_trades"


def test_review_verdict_returns_watch_when_any_input_is_flat_or_missing():
    verdict = _build_payload(
        execution_adjusted_pnl=0.0,
        signal_level_pnl_per_share=None,
    )

    assert verdict["verdict"] == "watch"
    assert verdict["reason_code"] == "insufficient_consensus"
    assert verdict["signals"] == {
        "execution_adjusted_pnl_sign": "flat",
        "signal_level_pnl_per_share_sign": "missing",
        "avg_clv_sign": "positive",
    }


def test_review_verdict_returns_watch_when_inputs_are_mixed():
    verdict = _build_payload(
        execution_adjusted_pnl=10.0,
        signal_level_pnl_per_share=-0.1,
        avg_clv=0.05,
    )

    assert verdict["verdict"] == "watch"
    assert verdict["reason_code"] == "mixed_evidence"
