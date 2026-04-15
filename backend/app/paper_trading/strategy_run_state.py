from __future__ import annotations

from decimal import Decimal

from app.config import settings
from app.models.strategy_run import StrategyRun

ZERO = Decimal("0.00")
PCT_ZERO = Decimal("0.000000")


def default_strategy_starting_equity() -> Decimal:
    return Decimal(str(settings.default_bankroll)).quantize(Decimal("0.01"))


def initialize_strategy_run_state(strategy_run: StrategyRun) -> StrategyRun:
    starting_equity = default_strategy_starting_equity()
    strategy_run.peak_equity = starting_equity
    strategy_run.current_equity = starting_equity
    strategy_run.max_drawdown = ZERO
    strategy_run.drawdown_pct = PCT_ZERO
    return strategy_run


def strategy_run_state_complete(strategy_run: StrategyRun | None) -> bool:
    if strategy_run is None:
        return False
    return all(
        value is not None
        for value in (
            strategy_run.peak_equity,
            strategy_run.current_equity,
            strategy_run.max_drawdown,
            strategy_run.drawdown_pct,
        )
    )


def serialize_strategy_run_state(strategy_run: StrategyRun | None) -> dict | None:
    if strategy_run is None:
        return None
    return {
        "peak_equity": float(strategy_run.peak_equity) if strategy_run.peak_equity is not None else None,
        "current_equity": float(strategy_run.current_equity) if strategy_run.current_equity is not None else None,
        "max_drawdown": float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else None,
        "drawdown_pct": float(strategy_run.drawdown_pct) if strategy_run.drawdown_pct is not None else None,
        "state_complete": strategy_run_state_complete(strategy_run),
    }


def apply_trade_resolution_to_run(
    strategy_run: StrategyRun,
    *,
    pnl: Decimal,
) -> StrategyRun:
    if not strategy_run_state_complete(strategy_run):
        raise ValueError("strategy_run risk state is not initialized")

    current_equity = Decimal(str(strategy_run.current_equity or ZERO)) + pnl
    peak_equity = max(Decimal(str(strategy_run.peak_equity or ZERO)), current_equity)
    current_drawdown = peak_equity - current_equity
    current_drawdown_pct = (
        (current_drawdown / peak_equity).quantize(Decimal("0.000001"))
        if peak_equity > ZERO
        else PCT_ZERO
    )

    strategy_run.current_equity = current_equity.quantize(Decimal("0.01"))
    strategy_run.peak_equity = peak_equity.quantize(Decimal("0.01"))
    strategy_run.max_drawdown = max(
        Decimal(str(strategy_run.max_drawdown or ZERO)),
        current_drawdown.quantize(Decimal("0.01")),
    )
    strategy_run.drawdown_pct = current_drawdown_pct
    return strategy_run
