"""Locked comparison helpers for prove-the-edge reviews."""
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.signals.probability import brier_score

ZERO = Decimal("0")


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


def _mode_summary(
    *,
    label: str,
    resolved_signals: int,
    win_rate: float,
    avg_clv: Decimal | None,
    total_profit_loss_per_share: Decimal,
    max_drawdown_per_share: Decimal,
    cumulative_pnl: Decimal,
    brier: Decimal | None,
) -> dict:
    return {
        "mode": label,
        "resolved_signals": resolved_signals,
        "win_rate": round(win_rate, 4),
        "avg_clv": float(avg_clv.quantize(Decimal("0.000001"))) if avg_clv is not None else None,
        "total_profit_loss_per_share": float(total_profit_loss_per_share.quantize(Decimal("0.000001"))),
        "max_drawdown_per_share": float(max_drawdown_per_share.quantize(Decimal("0.000001"))),
        "cumulative_pnl": float(cumulative_pnl.quantize(Decimal("0.01"))),
        "brier_score": float(brier.quantize(Decimal("0.000001"))) if brier is not None else None,
    }


async def compare_locked_modes(
    session: AsyncSession,
    *,
    start_date: datetime,
    end_date: datetime,
    strategy_run_id: uuid.UUID | None = None,
) -> dict:
    start_date = _ensure_utc(start_date) or datetime.now(timezone.utc)
    end_date = _ensure_utc(end_date) or datetime.now(timezone.utc)

    legacy_result = await session.execute(
        select(Signal).where(
            Signal.fired_at >= start_date,
            Signal.fired_at <= end_date,
            Signal.rank_score >= Decimal(str(settings.legacy_benchmark_rank_threshold)),
            Signal.resolved_correctly.isnot(None),
        )
    )
    legacy_signals = legacy_result.scalars().all()
    legacy_clvs = [signal.clv for signal in legacy_signals if signal.clv is not None]
    legacy_profit_losses = [signal.profit_loss or ZERO for signal in legacy_signals]
    legacy_predictions = [
        (signal.estimated_probability, signal.resolved_correctly)
        for signal in legacy_signals
        if signal.estimated_probability is not None and signal.resolved_correctly is not None
    ]
    legacy_wins = sum(1 for signal in legacy_signals if signal.resolved_correctly)

    default_trade_rows = []
    if strategy_run_id is not None:
        trade_result = await session.execute(
            select(PaperTrade, Signal)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                Signal.fired_at >= start_date,
                Signal.fired_at <= end_date,
                PaperTrade.status == "resolved",
            )
            .order_by(PaperTrade.resolved_at.asc())
        )
        default_trade_rows = trade_result.all()

    default_signals = [signal for _trade, signal in default_trade_rows]
    default_clvs = [signal.clv for signal in default_signals if signal.clv is not None]
    default_profit_losses = [signal.profit_loss or ZERO for signal in default_signals]
    default_predictions = [
        (signal.estimated_probability, signal.resolved_correctly)
        for signal in default_signals
        if signal.estimated_probability is not None and signal.resolved_correctly is not None
    ]
    default_wins = sum(1 for signal in default_signals if signal.resolved_correctly)
    default_cumulative_pnl = sum((trade.pnl or ZERO for trade, _signal in default_trade_rows), ZERO)

    return {
        "legacy": _mode_summary(
            label="legacy",
            resolved_signals=len(legacy_signals),
            win_rate=(legacy_wins / len(legacy_signals)) if legacy_signals else 0.0,
            avg_clv=_average(legacy_clvs),
            total_profit_loss_per_share=sum(legacy_profit_losses, ZERO),
            max_drawdown_per_share=_compute_max_drawdown(legacy_profit_losses),
            cumulative_pnl=ZERO,
            brier=brier_score(legacy_predictions) if legacy_predictions else None,
        ),
        "default_strategy": _mode_summary(
            label="default_strategy",
            resolved_signals=len(default_signals),
            win_rate=(default_wins / len(default_signals)) if default_signals else 0.0,
            avg_clv=_average(default_clvs),
            total_profit_loss_per_share=sum(default_profit_losses, ZERO),
            max_drawdown_per_share=_compute_max_drawdown(default_profit_losses),
            cumulative_pnl=default_cumulative_pnl,
            brier=brier_score(default_predictions) if default_predictions else None,
        ),
    }
