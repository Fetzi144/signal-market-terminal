"""Canonical default strategy contract for the "prove the edge" phase."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from app.config import settings


@dataclass(frozen=True)
class DefaultStrategyEvaluation:
    signal_type_match: bool
    in_window: bool
    eligible: bool
    reason_code: str | None = None
    reason_label: str | None = None


SKIP_REASON_LABELS = {
    "pending_decision": "Pending decision",
    "pending_decision_expired": "Pending decision retry window expired",
    "before_baseline_start": "Before baseline start",
    "missing_outcome_id": "Missing outcome",
    "missing_probability": "Missing probability",
    "missing_market_price": "Missing market price",
    "missing_expected_value": "Missing expected value",
    "ev_below_threshold": "EV below threshold",
    "risk_state_uninitialized": "Run risk state not initialized",
    "risk_local_total_exposure": "Local paper-book total exposure limit reached",
    "risk_local_cluster_exposure": "Local paper-book cluster exposure limit reached",
    "risk_local_invalid_size": "Local paper-book invalid size",
    "risk_local_rejected": "Local paper-book risk rejected",
    "risk_shared_global_block": "Shared/global platform risk blocked the trade",
    "execution_missing_orderbook_context": "Missing orderbook context",
    "execution_stale_orderbook_context": "Stale orderbook context",
    "execution_no_fill": "No fill available",
    "execution_partial_fill_below_minimum": "Partial fill below minimum",
    "execution_ev_below_threshold": "Executable EV below threshold",
    "execution_size_zero_after_fill_cap": "Executable size is zero after fill cap",
}


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_default_strategy_start_at() -> datetime | None:
    """Return the bootstrap launch boundary for the default strategy window."""
    return _ensure_utc(settings.default_strategy_start_at)


def in_default_strategy_window(
    fired_at: datetime | None,
    *,
    started_at: datetime | None = None,
) -> bool:
    """Return True when the timestamp is inside the validation window."""
    started_at = _ensure_utc(started_at) if started_at is not None else get_default_strategy_start_at()
    if started_at is None:
        return True
    current = _ensure_utc(fired_at)
    return current is not None and current >= started_at


def default_strategy_skip_label(reason_code: str | None) -> str | None:
    if reason_code is None:
        return None
    return SKIP_REASON_LABELS.get(reason_code, reason_code.replace("_", " "))


def evaluate_default_strategy_signal(
    signal,
    *,
    started_at: datetime | None = None,
) -> DefaultStrategyEvaluation:
    """Evaluate a signal against the frozen default-strategy contract."""
    required_signal_type = settings.default_strategy_signal_type
    min_ev = Decimal(str(settings.min_ev_threshold))
    in_window = in_default_strategy_window(signal.fired_at, started_at=started_at)

    if required_signal_type and signal.signal_type != required_signal_type:
        return DefaultStrategyEvaluation(
            signal_type_match=False,
            in_window=in_window,
            eligible=False,
        )

    if not in_window:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=False,
            eligible=False,
            reason_code="before_baseline_start",
            reason_label=default_strategy_skip_label("before_baseline_start"),
        )

    if signal.outcome_id is None:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=True,
            eligible=False,
            reason_code="missing_outcome_id",
            reason_label=default_strategy_skip_label("missing_outcome_id"),
        )

    if signal.estimated_probability is None:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=True,
            eligible=False,
            reason_code="missing_probability",
            reason_label=default_strategy_skip_label("missing_probability"),
        )

    if signal.price_at_fire is None:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=True,
            eligible=False,
            reason_code="missing_market_price",
            reason_label=default_strategy_skip_label("missing_market_price"),
        )

    if signal.expected_value is None:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=True,
            eligible=False,
            reason_code="missing_expected_value",
            reason_label=default_strategy_skip_label("missing_expected_value"),
        )

    if abs(signal.expected_value) < min_ev:
        return DefaultStrategyEvaluation(
            signal_type_match=True,
            in_window=True,
            eligible=False,
            reason_code="ev_below_threshold",
            reason_label=default_strategy_skip_label("ev_below_threshold"),
        )

    return DefaultStrategyEvaluation(
        signal_type_match=True,
        in_window=True,
        eligible=True,
    )


def get_default_strategy_contract(*, started_at: datetime | None = None) -> dict:
    """Return the auditable contract for the default strategy."""
    started_at = _ensure_utc(started_at) if started_at is not None else get_default_strategy_start_at()
    return {
        "name": settings.default_strategy_name,
        "display_name": "Default Strategy",
        "objective": "Measure whether confluence-led, EV-ranked signals produce durable paper-trading edge.",
        "signal_type": settings.default_strategy_signal_type,
        "baseline_start_at": started_at.isoformat() if started_at else None,
        "ev_threshold": settings.min_ev_threshold,
        "kelly_multiplier": settings.kelly_multiplier,
        "paper_bankroll_usd": settings.default_bankroll,
        "max_single_position_pct": settings.max_single_position_pct,
        "max_total_exposure_pct": settings.max_total_exposure_pct,
        "max_cluster_exposure_pct": settings.max_cluster_exposure_pct,
        "drawdown_circuit_breaker_pct": settings.drawdown_circuit_breaker_pct,
        "minimum_observation_days": settings.default_strategy_min_observation_days,
        "preferred_observation_days": settings.default_strategy_preferred_observation_days,
        "legacy_benchmark_rank_threshold": settings.legacy_benchmark_rank_threshold,
        "paper_trading_enabled": settings.paper_trading_enabled,
        "paper_trading_pending_decision_max_age_seconds": settings.paper_trading_pending_decision_max_age_seconds,
    }


def matches_default_strategy(signal) -> bool:
    """Return True when a persisted signal belongs to the default strategy."""
    return evaluate_default_strategy_signal(signal).eligible
