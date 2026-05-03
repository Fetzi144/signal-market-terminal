"""Paper trading API endpoints: portfolio, trade history, and performance metrics."""
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.paper_trade import PaperTrade
from app.paper_trading.analysis import (
    get_default_strategy_dashboard,
    get_default_strategy_run_lookup,
    get_profitability_snapshot,
    get_strategy_health,
    get_strategy_history,
    get_strategy_metrics,
    get_strategy_pnl_curve,
    get_strategy_portfolio_state,
)
from app.paper_trading.engine import get_metrics, get_pnl_curve, get_portfolio_state
from app.reports.profit_tools import build_profit_tools_snapshot
from app.strategy_runs.service import ActiveStrategyRunExistsError, open_default_strategy_run, serialize_strategy_run

router = APIRouter(prefix="/api/v1/paper-trading", tags=["paper-trading"])
paper_limiter = Limiter(key_func=get_remote_address)


class PaperTradeOut(BaseModel):
    id: uuid.UUID
    signal_id: uuid.UUID
    strategy_run_id: uuid.UUID | None = None
    strategy_version_id: int | None = None
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
        strategy_version_id=trade.strategy_version_id,
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


class DefaultStrategyRunLookupOut(BaseModel):
    state: str
    strategy_run: dict | None = None
    bootstrap_required: bool
    suggested_launch_boundary_at: str | None = None


class DefaultStrategyBootstrapIn(BaseModel):
    launch_boundary_at: datetime | None = None
    bootstrap_started_at: datetime | None = None
    evidence_boundary_id: str | None = None
    release_tag: str | None = None
    commit_sha: str | None = None
    migration_revision: str | None = None
    contract_version: str | None = None
    evidence_gate: dict[str, Any] | None = None


class DefaultStrategyDashboardOut(BaseModel):
    portfolio: PortfolioOut
    metrics: MetricsOut
    pnl_curve: list[PnlPointOut]
    strategy_health: dict[str, Any]


def _build_contract_metadata(payload: DefaultStrategyBootstrapIn) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    evidence_boundary = {
        "boundary_id": payload.evidence_boundary_id,
        "release_tag": payload.release_tag,
        "commit_sha": payload.commit_sha,
        "migration_revision": payload.migration_revision,
    }
    evidence_boundary = {
        key: value for key, value in evidence_boundary.items() if value is not None and str(value).strip()
    }
    if evidence_boundary:
        metadata["evidence_boundary"] = evidence_boundary
    if payload.contract_version is not None and payload.contract_version.strip():
        metadata["contract_version"] = payload.contract_version.strip()
    if payload.evidence_gate:
        metadata["evidence_gate"] = payload.evidence_gate
    return metadata


def _portfolio_out_from_state(state: dict) -> PortfolioOut:
    from app.config import settings

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


def _metrics_out_from_payload(metrics: dict) -> MetricsOut:
    return MetricsOut(**metrics)


def _pnl_curve_out_from_rows(rows: list[dict]) -> list[PnlPointOut]:
    return [PnlPointOut(**row) for row in rows]


@router.get("/default-strategy/run", response_model=DefaultStrategyRunLookupOut)
@paper_limiter.limit("10/second")
async def get_default_strategy_run_endpoint(request: Request, db: AsyncSession = Depends(get_db)):
    return DefaultStrategyRunLookupOut(**await get_default_strategy_run_lookup(db))


@router.post("/default-strategy/bootstrap", response_model=DefaultStrategyRunLookupOut)
@paper_limiter.limit("5/minute")
async def bootstrap_default_strategy_run(
    request: Request,
    payload: DefaultStrategyBootstrapIn,
    db: AsyncSession = Depends(get_db),
):
    try:
        strategy_run = await open_default_strategy_run(
            db,
            launch_boundary_at=payload.launch_boundary_at,
            bootstrap_started_at=payload.bootstrap_started_at,
            contract_metadata=_build_contract_metadata(payload),
        )
        await db.commit()
    except ActiveStrategyRunExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return DefaultStrategyRunLookupOut(
        state="active_run",
        strategy_run=serialize_strategy_run(strategy_run),
        bootstrap_required=False,
        suggested_launch_boundary_at=(
            payload.launch_boundary_at.isoformat() if payload.launch_boundary_at is not None else None
        ),
    )


@router.get("/default-strategy/dashboard", response_model=DefaultStrategyDashboardOut)
@paper_limiter.limit("10/second")
async def get_default_strategy_dashboard_endpoint(request: Request, db: AsyncSession = Depends(get_db)):
    dashboard = await get_default_strategy_dashboard(db)
    return DefaultStrategyDashboardOut(
        portfolio=_portfolio_out_from_state(dashboard["portfolio"]),
        metrics=_metrics_out_from_payload(dashboard["metrics"]),
        pnl_curve=_pnl_curve_out_from_rows(dashboard["pnl_curve"]),
        strategy_health=dashboard["strategy_health"],
    )


@router.get("/portfolio", response_model=PortfolioOut)
@paper_limiter.limit("10/second")
async def get_portfolio(
    request: Request,
    scope: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Current paper trading portfolio: open positions + summary stats."""
    state = await get_strategy_portfolio_state(db) if scope == "default_strategy" else await get_portfolio_state(db)
    return _portfolio_out_from_state(state)


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
    return _metrics_out_from_payload(metrics)


@router.get("/strategy-health")
@paper_limiter.limit("10/second")
async def get_strategy_health_endpoint(request: Request, db: AsyncSession = Depends(get_db)):
    """Consolidated health view for the default strategy."""
    return await get_strategy_health(db)


@router.get("/profitability-snapshot")
@paper_limiter.limit("10/second")
async def get_profitability_snapshot_endpoint(
    request: Request,
    family: str = Query("default_strategy"),
    db: AsyncSession = Depends(get_db),
):
    """Read-only paper profitability gate snapshot for a strategy family."""
    return await get_profitability_snapshot(db, family=family)


@router.get("/profit-tools")
@paper_limiter.limit("10/second")
async def get_profit_tools_endpoint(
    request: Request,
    family: str = Query("default_strategy"),
    db: AsyncSession = Depends(get_db),
):
    """Read-only paper profit-finding workbench and lane readiness snapshot."""
    return await build_profit_tools_snapshot(db, family=family)


@router.get("/pnl-curve", response_model=list[PnlPointOut])
@paper_limiter.limit("10/second")
async def get_pnl_curve_endpoint(
    request: Request,
    scope: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Cumulative P&L curve for charting."""
    rows = await get_strategy_pnl_curve(db) if scope == "default_strategy" else await get_pnl_curve(db)
    return _pnl_curve_out_from_rows(rows)
