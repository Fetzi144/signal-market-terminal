"""Paper trading API endpoints: portfolio, trade history, and performance metrics."""
import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.paper_trade import PaperTrade
from app.paper_trading.analysis import (
    get_strategy_health,
    get_strategy_history,
    get_strategy_metrics,
    get_strategy_pnl_curve,
    get_strategy_portfolio_state,
)
from app.paper_trading.engine import get_metrics, get_pnl_curve, get_portfolio_state

router = APIRouter(prefix="/api/v1/paper-trading", tags=["paper-trading"])
paper_limiter = Limiter(key_func=get_remote_address)


class PaperTradeOut(BaseModel):
    id: uuid.UUID
    signal_id: uuid.UUID
    strategy_run_id: uuid.UUID | None = None
    execution_decision_id: uuid.UUID | None = None
    outcome_id: uuid.UUID
    market_id: uuid.UUID
    direction: str
    entry_price: Decimal
    shadow_entry_price: Decimal | None = None
    size_usd: Decimal
    shares: Decimal
    exit_price: Decimal | None = None
    pnl: Decimal | None = None
    shadow_pnl: Decimal | None = None
    status: str
    opened_at: datetime
    submitted_at: datetime | None = None
    confirmed_at: datetime | None = None
    resolved_at: datetime | None = None
    details: dict


def _paper_trade_out(trade: PaperTrade) -> PaperTradeOut:
    return PaperTradeOut(
        id=trade.id,
        signal_id=trade.signal_id,
        strategy_run_id=trade.strategy_run_id,
        execution_decision_id=trade.execution_decision_id,
        outcome_id=trade.outcome_id,
        market_id=trade.market_id,
        direction=trade.direction,
        entry_price=trade.entry_price,
        shadow_entry_price=trade.shadow_entry_price,
        size_usd=trade.size_usd,
        shares=trade.shares,
        exit_price=trade.exit_price,
        pnl=trade.pnl,
        shadow_pnl=trade.shadow_pnl,
        status=trade.status,
        opened_at=trade.opened_at,
        submitted_at=trade.submitted_at,
        confirmed_at=trade.confirmed_at,
        resolved_at=trade.resolved_at,
        details=trade.details or {},
    )


class PortfolioOut(BaseModel):
    bankroll: float
    open_exposure: float
    open_trades: list[PaperTradeOut]
    total_resolved: int
    cumulative_pnl: float
    wins: int
    losses: int
    win_rate: float


class MetricsOut(BaseModel):
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    cumulative_pnl: float
    shadow_cumulative_pnl: float = 0.0
    avg_pnl: float
    max_drawdown: float
    sharpe_ratio: float
    profit_factor: float | None
    shadow_profit_factor: float | None = None
    best_trade: float
    worst_trade: float
    liquidity_constrained_trades: int = 0
    trades_missing_orderbook_context: int = 0


class TradeHistoryOut(BaseModel):
    trades: list[PaperTradeOut]
    total: int
    page: int
    page_size: int


class PnlPointOut(BaseModel):
    timestamp: str
    pnl: float
    trade_pnl: float
    shadow_trade_pnl: float | None = None
    direction: str
    trade_id: str


@router.get("/portfolio", response_model=PortfolioOut)
@paper_limiter.limit("10/second")
async def get_portfolio(
    request: Request,
    scope: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Current paper trading portfolio: open positions + summary stats."""
    from app.config import settings
    state = await get_strategy_portfolio_state(db) if scope == "default_strategy" else await get_portfolio_state(db)
    return PortfolioOut(
        bankroll=float(settings.default_bankroll),
        open_exposure=float(state["open_exposure"]),
        open_trades=[_paper_trade_out(t) for t in state["open_trades"]],
        total_resolved=state["total_resolved"],
        cumulative_pnl=float(state["cumulative_pnl"]),
        wins=state["wins"],
        losses=state["losses"],
        win_rate=float(state["win_rate"]),
    )


@router.get("/history", response_model=TradeHistoryOut)
@paper_limiter.limit("10/second")
async def get_history(
    request: Request,
    status: str | None = None,
    direction: str | None = None,
    scope: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Trade history with optional filters."""
    if scope == "default_strategy":
        scoped = await get_strategy_history(
            db,
            status=status,
            direction=direction,
            page=page,
            page_size=page_size,
        )
        trades = scoped["trades"]
        total = scoped["total"]
        return TradeHistoryOut(
            trades=[_paper_trade_out(t) for t in trades],
            total=total,
            page=page,
            page_size=page_size,
        )

    query = select(PaperTrade)
    count_query = select(func.count(PaperTrade.id))

    if status:
        query = query.where(PaperTrade.status == status)
        count_query = count_query.where(PaperTrade.status == status)
    if direction:
        query = query.where(PaperTrade.direction == direction)
        count_query = count_query.where(PaperTrade.direction == direction)

    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(PaperTrade.opened_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    trades = result.scalars().all()

    return TradeHistoryOut(
        trades=[_paper_trade_out(t) for t in trades],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/metrics", response_model=MetricsOut)
@paper_limiter.limit("10/second")
async def get_trading_metrics(
    request: Request,
    scope: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Paper trading performance metrics: Sharpe, drawdown, win rate, cumulative P&L."""
    metrics = await get_strategy_metrics(db) if scope == "default_strategy" else await get_metrics(db)
    return MetricsOut(**metrics)


@router.get("/strategy-health")
@paper_limiter.limit("10/second")
async def get_strategy_health_endpoint(request: Request, db: AsyncSession = Depends(get_db)):
    """Consolidated health view for the default strategy."""
    return await get_strategy_health(db)


@router.get("/pnl-curve", response_model=list[PnlPointOut])
@paper_limiter.limit("10/second")
async def get_pnl_curve_endpoint(
    request: Request,
    scope: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Cumulative P&L curve for charting."""
    return await get_strategy_pnl_curve(db) if scope == "default_strategy" else await get_pnl_curve(db)
