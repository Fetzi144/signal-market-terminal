from __future__ import annotations

import math
import uuid
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.paper_trade import PaperTrade

ZERO = Decimal("0")


async def get_portfolio_state(session: AsyncSession) -> dict:
    return await _get_portfolio_state(session)


async def _get_portfolio_state(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None = None,
) -> dict:
    open_query = select(PaperTrade).where(PaperTrade.status == "open")
    resolved_query = select(
        func.count(PaperTrade.id).label("total_trades"),
        func.sum(PaperTrade.pnl).label("cumulative_pnl"),
        func.count(PaperTrade.id).filter(PaperTrade.pnl > 0).label("wins"),
        func.count(PaperTrade.id).filter(PaperTrade.pnl <= 0).label("losses"),
    ).where(PaperTrade.status == "resolved")

    if strategy_run_id is not None:
        open_query = open_query.where(PaperTrade.strategy_run_id == strategy_run_id)
        resolved_query = resolved_query.where(PaperTrade.strategy_run_id == strategy_run_id)

    open_result = await session.execute(open_query.order_by(PaperTrade.opened_at.desc()))
    open_trades = open_result.scalars().all()

    stats = (await session.execute(resolved_query)).one()
    total_trades = stats.total_trades or 0
    cumulative_pnl = stats.cumulative_pnl or ZERO
    wins = stats.wins or 0
    losses = stats.losses or 0
    open_exposure = sum((trade.size_usd for trade in open_trades), ZERO)

    return {
        "open_trades": open_trades,
        "open_exposure": open_exposure,
        "total_resolved": total_trades,
        "cumulative_pnl": cumulative_pnl,
        "wins": wins,
        "losses": losses,
        "win_rate": Decimal(str(wins / total_trades)).quantize(Decimal("0.0001")) if total_trades > 0 else ZERO,
    }


async def get_metrics(session: AsyncSession) -> dict:
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "resolved")
        .order_by(PaperTrade.resolved_at.asc())
    )
    resolved = result.scalars().all()

    if not resolved:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "cumulative_pnl": 0.0,
            "shadow_cumulative_pnl": 0.0,
            "avg_pnl": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
            "profit_factor": 0.0,
            "shadow_profit_factor": 0.0,
            "best_trade": 0.0,
            "worst_trade": 0.0,
            "liquidity_constrained_trades": 0,
            "trades_missing_orderbook_context": 0,
        }

    pnls = [float(trade.pnl) for trade in resolved if trade.pnl is not None]
    shadow_pnls = [float(trade.shadow_pnl) for trade in resolved if trade.shadow_pnl is not None]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl <= 0]
    shadow_wins = [pnl for pnl in shadow_pnls if pnl > 0]
    shadow_losses = [pnl for pnl in shadow_pnls if pnl <= 0]

    cumulative = []
    running = 0.0
    for pnl in pnls:
        running += pnl
        cumulative.append(running)

    peak = 0.0
    max_drawdown = 0.0
    for value in cumulative:
        if value > peak:
            peak = value
        max_drawdown = max(max_drawdown, peak - value)

    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((pnl - mean_pnl) ** 2 for pnl in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance)
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

    total_wins = sum(wins) if wins else 0.0
    total_losses = abs(sum(losses)) if losses else 0.0
    profit_factor = (total_wins / total_losses) if total_losses > 0 else float("inf") if total_wins > 0 else 0.0
    shadow_total_wins = sum(shadow_wins) if shadow_wins else 0.0
    shadow_total_losses = abs(sum(shadow_losses)) if shadow_losses else 0.0
    shadow_profit_factor = (
        shadow_total_wins / shadow_total_losses
        if shadow_total_losses > 0
        else float("inf") if shadow_total_wins > 0 else 0.0
    )

    liquidity_constrained_trades = sum(
        1
        for trade in resolved
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("liquidity_constrained") is True
    )
    trades_missing_orderbook_context = sum(
        1
        for trade in resolved
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("missing_orderbook_context") is True
    )

    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        "cumulative_pnl": round(sum(pnls), 2),
        "shadow_cumulative_pnl": round(sum(shadow_pnls), 2) if shadow_pnls else 0.0,
        "avg_pnl": round(mean_pnl, 2),
        "max_drawdown": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "shadow_profit_factor": round(shadow_profit_factor, 4) if shadow_profit_factor != float("inf") else None,
        "best_trade": round(max(pnls), 2) if pnls else 0.0,
        "worst_trade": round(min(pnls), 2) if pnls else 0.0,
        "liquidity_constrained_trades": liquidity_constrained_trades,
        "trades_missing_orderbook_context": trades_missing_orderbook_context,
    }


async def get_pnl_curve(session: AsyncSession) -> list[dict]:
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "resolved")
        .order_by(PaperTrade.resolved_at.asc())
    )
    resolved = result.scalars().all()

    curve: list[dict] = []
    running = Decimal("0")
    for trade in resolved:
        if trade.pnl is not None and trade.resolved_at is not None:
            running += trade.pnl
            curve.append(
                {
                    "timestamp": trade.resolved_at.isoformat(),
                    "pnl": float(running),
                    "trade_pnl": float(trade.pnl),
                    "shadow_trade_pnl": float(trade.shadow_pnl) if trade.shadow_pnl is not None else None,
                    "direction": trade.direction,
                    "trade_id": str(trade.id),
                }
            )

    return curve


__all__ = [
    "_get_portfolio_state",
    "get_metrics",
    "get_pnl_curve",
    "get_portfolio_state",
]
