"""Honest measurement comparisons for prove-the-edge reviews."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.signals.probability import brier_score

ZERO = Decimal("0")
MAX_DRAWDOWN_SIGNAL_ROWS = 50_000


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(str(len(values)))


def _compute_max_drawdown(values: list[Decimal]) -> Decimal:
    peak = ZERO
    running = ZERO
    max_drawdown = ZERO
    for value in values:
        running += value
        if running > peak:
            peak = running
        max_drawdown = max(max_drawdown, peak - running)
    return max_drawdown


def _serialize_decimal(value: Decimal | None, *, quantum: Decimal | None = None) -> float | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    normalized = value.quantize(quantum) if quantum is not None else value
    return float(normalized)


def _signal_level_summary(*, label: str, signals: list[Signal], cohort_label: str) -> dict:
    clvs = [signal.clv for signal in signals if signal.clv is not None]
    profit_losses = [signal.profit_loss or ZERO for signal in signals]
    predictions = [
        (signal.estimated_probability, signal.resolved_correctly)
        for signal in signals
        if signal.estimated_probability is not None and signal.resolved_correctly is not None
    ]
    wins = sum(1 for signal in signals if signal.resolved_correctly)
    return {
        "available": True,
        "mode": label,
        "unit": "per_share",
        "cohort": cohort_label,
        "resolved_signals": len(signals),
        "win_rate": round(wins / len(signals), 4) if signals else 0.0,
        "avg_clv": _serialize_decimal(_average(clvs), quantum=Decimal("0.000001")) if clvs else None,
        "total_profit_loss_per_share": _serialize_decimal(sum(profit_losses, ZERO), quantum=Decimal("0.000001")),
        "max_drawdown_per_share": _serialize_decimal(_compute_max_drawdown(profit_losses), quantum=Decimal("0.000001")),
        "brier_score": _serialize_decimal(brier_score(predictions), quantum=Decimal("0.000001")) if predictions else None,
    }


async def _signal_level_summary_for_filters(
    session: AsyncSession,
    *,
    label: str,
    filters: list,
    cohort_label: str,
) -> dict:
    resolved_value = case((Signal.resolved_correctly.is_(True), Decimal("1")), else_=Decimal("0"))
    brier_expr = (Signal.estimated_probability - resolved_value) * (Signal.estimated_probability - resolved_value)
    row = (
        await session.execute(
            select(
                func.count(Signal.id),
                func.sum(case((Signal.resolved_correctly.is_(True), 1), else_=0)),
                func.avg(Signal.clv),
                func.sum(func.coalesce(Signal.profit_loss, ZERO)),
                func.avg(brier_expr),
            ).where(*filters)
        )
    ).one()
    resolved_count = int(row[0] or 0)
    wins = int(row[1] or 0)
    total_profit_loss = row[3] or ZERO

    max_drawdown: Decimal | None = None
    max_drawdown_available = resolved_count <= MAX_DRAWDOWN_SIGNAL_ROWS
    if max_drawdown_available and resolved_count:
        pnl_rows = (
            await session.execute(
                select(func.coalesce(Signal.profit_loss, ZERO))
                .where(*filters)
                .order_by(Signal.fired_at.asc(), Signal.id.asc())
            )
        ).scalars().all()
        max_drawdown = _compute_max_drawdown([Decimal(str(value or ZERO)) for value in pnl_rows])

    return {
        "available": True,
        "mode": label,
        "unit": "per_share",
        "cohort": cohort_label,
        "resolved_signals": resolved_count,
        "win_rate": round(wins / resolved_count, 4) if resolved_count else 0.0,
        "avg_clv": _serialize_decimal(row[2], quantum=Decimal("0.000001")) if row[2] is not None else None,
        "total_profit_loss_per_share": _serialize_decimal(total_profit_loss, quantum=Decimal("0.000001")),
        "max_drawdown_per_share": _serialize_decimal(max_drawdown, quantum=Decimal("0.000001")),
        "max_drawdown_available": max_drawdown_available,
        "brier_score": _serialize_decimal(row[4], quantum=Decimal("0.000001")) if row[4] is not None else None,
    }


def _execution_adjusted_summary(*, trade_rows: list[tuple[PaperTrade, Signal]]) -> dict:
    pnls = [trade.pnl or ZERO for trade, _signal in trade_rows if trade.pnl is not None]
    shadow_pnls = [trade.shadow_pnl or ZERO for trade, _signal in trade_rows if trade.shadow_pnl is not None]
    wins = sum(1 for pnl in pnls if pnl > ZERO)
    losses = sum(1 for pnl in pnls if pnl <= ZERO)
    return {
        "available": True,
        "unit": "usd",
        "resolved_trades": len(pnls),
        "win_rate": round(wins / len(pnls), 4) if pnls else 0.0,
        "cumulative_pnl": _serialize_decimal(sum(pnls, ZERO), quantum=Decimal("0.01")) or 0.0,
        "shadow_cumulative_pnl": _serialize_decimal(sum(shadow_pnls, ZERO), quantum=Decimal("0.01")) or 0.0,
        "avg_trade_pnl": _serialize_decimal((sum(pnls, ZERO) / Decimal(str(len(pnls)))) if pnls else ZERO, quantum=Decimal("0.01")) or 0.0,
        "max_drawdown": _serialize_decimal(_compute_max_drawdown(pnls), quantum=Decimal("0.01")) or 0.0,
        "wins": wins,
        "losses": losses,
    }


def empty_strategy_measurement_modes() -> dict:
    return {
        "signal_level": {
            "unit": "per_share",
            "default_strategy": {
                "available": False,
                "reason": "no_active_run",
            },
            "benchmark": {
                "available": False,
                "reason": "no_active_run",
            },
        },
        "execution_adjusted": {
            "unit": "usd",
            "default_strategy": {
                "available": False,
                "reason": "no_active_run",
            },
            "benchmark": {
                "available": False,
                "reason": "legacy_execution_adjusted_unavailable",
            },
        },
    }


async def compare_strategy_measurement_modes(
    session: AsyncSession,
    *,
    start_date: datetime,
    end_date: datetime,
    strategy_run_id: uuid.UUID | None = None,
) -> dict:
    start_date = _ensure_utc(start_date) or datetime.now(timezone.utc)
    end_date = _ensure_utc(end_date) or datetime.now(timezone.utc)

    legacy_filters = [
        Signal.fired_at >= start_date,
        Signal.fired_at <= end_date,
        Signal.rank_score >= Decimal(str(settings.legacy_benchmark_rank_threshold)),
        Signal.resolved_correctly.is_not(None),
    ]
    if settings.default_strategy_signal_type:
        legacy_filters.append(Signal.signal_type != settings.default_strategy_signal_type)

    default_filters = [
        Signal.fired_at >= start_date,
        Signal.fired_at <= end_date,
        Signal.signal_type == settings.default_strategy_signal_type,
        Signal.resolved_correctly.is_not(None),
        Signal.outcome_id.is_not(None),
        Signal.estimated_probability.is_not(None),
        Signal.price_at_fire.is_not(None),
        Signal.expected_value.is_not(None),
        func.abs(Signal.expected_value) >= Decimal(str(settings.min_ev_threshold)),
    ]

    default_trade_rows: list[tuple[PaperTrade, Signal]] = []
    if strategy_run_id is not None:
        trade_result = await session.execute(
            select(PaperTrade, Signal)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.status == "resolved",
                Signal.fired_at >= start_date,
                Signal.fired_at <= end_date,
            )
            .order_by(PaperTrade.resolved_at.asc(), PaperTrade.id.asc())
        )
        default_trade_rows = trade_result.all()

    signal_level_default = await _signal_level_summary_for_filters(
        session,
        label="default_strategy",
        filters=default_filters,
        cohort_label="eligible_default_strategy_signals",
    )
    signal_level_benchmark = await _signal_level_summary_for_filters(
        session,
        label="legacy_benchmark",
        filters=legacy_filters,
        cohort_label="rank_threshold_signals",
    )
    signal_level_delta = {
        "unit": "per_share",
        "profit_loss_per_share_delta": round(
            (signal_level_default["total_profit_loss_per_share"] or 0.0)
            - (signal_level_benchmark["total_profit_loss_per_share"] or 0.0),
            6,
        ),
        "max_drawdown_per_share_delta": round(
            (signal_level_default["max_drawdown_per_share"] or 0.0)
            - (signal_level_benchmark["max_drawdown_per_share"] or 0.0),
            6,
        ),
    }

    execution_adjusted_default = _execution_adjusted_summary(trade_rows=default_trade_rows)
    execution_adjusted_benchmark = {
        "available": False,
        "unit": "usd",
        "reason": "legacy_execution_adjusted_unavailable",
    }

    return {
        "signal_level": {
            "unit": "per_share",
            "default_strategy": signal_level_default,
            "benchmark": signal_level_benchmark,
            "delta": signal_level_delta,
        },
        "execution_adjusted": {
            "unit": "usd",
            "default_strategy": execution_adjusted_default,
            "benchmark": execution_adjusted_benchmark,
        },
    }


async def compare_locked_modes(
    session: AsyncSession,
    *,
    start_date: datetime,
    end_date: datetime,
    strategy_run_id: uuid.UUID | None = None,
) -> dict:
    return await compare_strategy_measurement_modes(
        session,
        start_date=start_date,
        end_date=end_date,
        strategy_run_id=strategy_run_id,
    )
