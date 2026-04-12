"""Paper trading engine: auto-open trades on EV-positive signals, resolve on market settlement.

This is the core simulation engine that tracks hypothetical P&L
without real money. Every EV-positive signal triggers a paper trade
using Kelly-recommended sizing, subject to risk management checks.
"""
import logging
import math
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.paper_trade import PaperTrade
from app.signals.ev import compute_ev_full
from app.signals.kelly import kelly_size
from app.signals.risk import check_exposure

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


async def get_portfolio_state(session: AsyncSession) -> dict:
    """Get current portfolio state: open positions, P&L, exposure."""
    # Open positions
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "open")
        .order_by(PaperTrade.opened_at.desc())
    )
    open_trades = result.scalars().all()

    # Resolved trades
    result = await session.execute(
        select(
            func.count(PaperTrade.id).label("total_trades"),
            func.sum(PaperTrade.pnl).label("cumulative_pnl"),
            func.count(PaperTrade.id).filter(PaperTrade.pnl > 0).label("wins"),
            func.count(PaperTrade.id).filter(PaperTrade.pnl <= 0).label("losses"),
        ).where(PaperTrade.status == "resolved")
    )
    stats = result.one()

    total_trades = stats.total_trades or 0
    cumulative_pnl = stats.cumulative_pnl or ZERO
    wins = stats.wins or 0
    losses = stats.losses or 0

    open_exposure = sum((t.size_usd for t in open_trades), ZERO)

    return {
        "open_trades": open_trades,
        "open_exposure": open_exposure,
        "total_resolved": total_trades,
        "cumulative_pnl": cumulative_pnl,
        "wins": wins,
        "losses": losses,
        "win_rate": Decimal(str(wins / total_trades)).quantize(Decimal("0.0001")) if total_trades > 0 else ZERO,
    }


async def open_trade(
    session: AsyncSession,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal,
    market_price: Decimal,
    market_question: str = "",
) -> PaperTrade | None:
    """Open a paper trade for an EV-positive signal.

    Runs Kelly sizing and risk checks. Returns the created PaperTrade or None.
    """
    if not settings.paper_trading_enabled or outcome_id is None:
        return None

    bankroll = Decimal(str(settings.default_bankroll))

    # Compute EV
    ev_data = compute_ev_full(estimated_probability, market_price)
    if ev_data["ev_per_share"] < Decimal(str(settings.min_ev_threshold)):
        return None

    # Kelly sizing
    sizing = kelly_size(
        estimated_prob=estimated_probability,
        market_price=market_price,
        bankroll=bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )

    if sizing["recommended_size_usd"] <= ZERO:
        return None

    # Risk check
    portfolio = await get_portfolio_state(session)
    open_positions = [
        {
            "size_usd": t.size_usd,
            "market_question": (t.details or {}).get("market_question", ""),
            "outcome_id": str(t.outcome_id),
        }
        for t in portfolio["open_trades"]
    ]

    # Compute peak bankroll for drawdown check
    peak_bankroll = bankroll  # start with default
    if portfolio["cumulative_pnl"] > ZERO:
        peak_bankroll = bankroll + portfolio["cumulative_pnl"]

    risk_result = check_exposure(
        open_positions=open_positions,
        new_trade={
            "size_usd": sizing["recommended_size_usd"],
            "market_question": market_question,
            "outcome_id": str(outcome_id),
        },
        bankroll=bankroll,
        max_total_pct=Decimal(str(settings.max_total_exposure_pct)),
        max_cluster_pct=Decimal(str(settings.max_cluster_exposure_pct)),
        drawdown_breaker_pct=Decimal(str(settings.drawdown_circuit_breaker_pct)),
        peak_bankroll=peak_bankroll,
        cumulative_pnl=portfolio["cumulative_pnl"],
    )

    if not risk_result["approved"]:
        logger.info(
            "Paper trade rejected by risk check: %s (signal %s)",
            risk_result["reason"], signal_id,
        )
        return None

    approved_size = risk_result["approved_size_usd"]
    entry_price = sizing["entry_price"]
    shares = (approved_size / entry_price).quantize(Decimal("0.0001")) if entry_price > ZERO else ZERO

    trade = PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        direction=sizing["direction"],
        entry_price=entry_price,
        size_usd=approved_size,
        shares=shares,
        status="open",
        opened_at=datetime.now(timezone.utc),
        details={
            "market_question": market_question,
            "estimated_probability": str(estimated_probability),
            "market_price": str(market_price),
            "ev_per_share": str(ev_data["ev_per_share"]),
            "edge_pct": str(ev_data["edge_pct"]),
            "kelly_full": str(sizing["kelly_full"]),
            "kelly_used": str(sizing["kelly_used"]),
            "risk_result": risk_result["reason"],
            "drawdown_active": risk_result["drawdown_active"],
        },
    )
    session.add(trade)
    await session.flush()
    logger.info(
        "Paper trade opened: %s %s @ $%s, size $%s (%s shares), signal=%s",
        trade.direction, outcome_id, entry_price, approved_size, shares, signal_id,
    )
    return trade


async def resolve_trades(
    session: AsyncSession,
    outcome_id: uuid.UUID,
    outcome_won: bool,
) -> int:
    """Resolve all open paper trades for a given outcome.

    Args:
        outcome_id: The resolved outcome
        outcome_won: True if the outcome resolved YES, False if NO

    Returns count of resolved trades.
    """
    result = await session.execute(
        select(PaperTrade).where(
            PaperTrade.outcome_id == outcome_id,
            PaperTrade.status == "open",
        )
    )
    trades = result.scalars().all()

    if not trades:
        return 0

    now = datetime.now(timezone.utc)
    count = 0

    for trade in trades:
        if trade.direction == "buy_yes":
            exit_price = Decimal("1.000000") if outcome_won else Decimal("0.000000")
        else:  # buy_no
            exit_price = Decimal("0.000000") if outcome_won else Decimal("1.000000")

        # P&L = shares * (exit_price - entry_price)
        pnl = (trade.shares * (exit_price - trade.entry_price)).quantize(Decimal("0.01"))

        trade.exit_price = exit_price
        trade.pnl = pnl
        trade.status = "resolved"
        trade.resolved_at = now
        count += 1

        logger.info(
            "Paper trade resolved: %s %s, entry=$%s exit=$%s, P&L=$%s",
            trade.direction, trade.outcome_id, trade.entry_price, exit_price, pnl,
        )

    if count > 0:
        await session.flush()

    return count


async def get_metrics(session: AsyncSession) -> dict:
    """Compute portfolio performance metrics: Sharpe, max drawdown, win rate, P&L."""
    # Get all resolved trades ordered by resolution time
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
            "avg_pnl": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
            "profit_factor": 0.0,
            "best_trade": 0.0,
            "worst_trade": 0.0,
        }

    pnls = [float(t.pnl) for t in resolved if t.pnl is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    # Cumulative P&L curve for max drawdown
    cumulative = []
    running = 0.0
    for p in pnls:
        running += p
        cumulative.append(running)

    # Max drawdown
    peak = 0.0
    max_dd = 0.0
    for value in cumulative:
        if value > peak:
            peak = value
        dd = peak - value
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio (simplified: mean/std of per-trade returns)
    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance)
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

    # Profit factor
    total_wins = sum(wins) if wins else 0.0
    total_losses = abs(sum(losses)) if losses else 0.0
    profit_factor = (total_wins / total_losses) if total_losses > 0 else float("inf") if total_wins > 0 else 0.0

    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        "cumulative_pnl": round(sum(pnls), 2),
        "avg_pnl": round(mean_pnl, 2),
        "max_drawdown": round(max_dd, 2),
        "sharpe_ratio": round(sharpe, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "best_trade": round(max(pnls), 2) if pnls else 0.0,
        "worst_trade": round(min(pnls), 2) if pnls else 0.0,
    }


async def get_pnl_curve(session: AsyncSession) -> list[dict]:
    """Return cumulative P&L curve data points for charting."""
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "resolved")
        .order_by(PaperTrade.resolved_at.asc())
    )
    resolved = result.scalars().all()

    curve = []
    running = Decimal("0")
    for trade in resolved:
        if trade.pnl is not None and trade.resolved_at is not None:
            running += trade.pnl
            curve.append({
                "timestamp": trade.resolved_at.isoformat(),
                "pnl": float(running),
                "trade_pnl": float(trade.pnl),
                "direction": trade.direction,
                "trade_id": str(trade.id),
            })

    return curve
