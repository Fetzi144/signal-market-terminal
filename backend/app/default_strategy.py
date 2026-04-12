"""Canonical default strategy contract for the "prove the edge" phase."""
from decimal import Decimal

from app.config import settings


def get_default_strategy_contract() -> dict:
    """Return the auditable contract for the default strategy."""
    return {
        "name": settings.default_strategy_name,
        "display_name": "Default Strategy",
        "objective": "Measure whether confluence-led, EV-ranked signals produce durable paper-trading edge.",
        "signal_type": settings.default_strategy_signal_type,
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
    }


def matches_default_strategy(signal) -> bool:
    """Return True when a persisted signal belongs to the default strategy."""
    min_ev = Decimal(str(settings.min_ev_threshold))
    required_signal_type = settings.default_strategy_signal_type

    if required_signal_type and signal.signal_type != required_signal_type:
        return False

    if (
        signal.outcome_id is None
        or signal.estimated_probability is None
        or signal.price_at_fire is None
        or signal.expected_value is None
    ):
        return False

    return abs(signal.expected_value) >= min_ev
