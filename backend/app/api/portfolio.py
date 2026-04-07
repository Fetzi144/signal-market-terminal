"""Portfolio position tracking endpoints."""
import csv
import io
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.portfolio import service

router = APIRouter(prefix="/api/v1", tags=["portfolio"])
portfolio_limiter = Limiter(key_func=get_remote_address)


# -- Request schemas --

class OpenPositionRequest(BaseModel):
    market_id: uuid.UUID
    outcome_id: uuid.UUID
    platform: str
    side: str = Field(..., pattern="^(yes|no)$")
    quantity: float = Field(..., gt=0)
    price: float = Field(..., ge=0, le=1)
    signal_id: uuid.UUID | None = None
    notes: str | None = None


class AddTradeRequest(BaseModel):
    action: str = Field(..., pattern="^(buy|sell)$")
    quantity: float = Field(..., gt=0)
    price: float = Field(..., ge=0, le=1)
    fees: float = Field(default=0.0, ge=0)


class ClosePositionRequest(BaseModel):
    quantity: float = Field(..., gt=0)
    price: float = Field(..., ge=0, le=1)
    fees: float = Field(default=0.0, ge=0)


# -- Response schemas --

class TradeOut(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    action: str
    quantity: float
    price: float
    fees: float
    created_at: datetime


class PositionOut(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    created_at: datetime
    updated_at: datetime
    market_id: uuid.UUID
    outcome_id: uuid.UUID
    platform: str
    side: str
    quantity: float
    avg_entry_price: float
    current_price: float | None = None
    unrealized_pnl: float | None = None
    status: str
    exit_price: float | None = None
    realized_pnl: float | None = None
    notes: str | None = None
    signal_id: uuid.UUID | None = None
    market_question: str | None = None
    outcome_name: str | None = None


class PositionDetailOut(PositionOut):
    trades: list[TradeOut] = []


class PositionListOut(BaseModel):
    positions: list[PositionOut]
    total: int
    page: int
    page_size: int


class PortfolioSummaryOut(BaseModel):
    open_positions: int
    closed_positions: int
    total_unrealized_pnl: float
    total_realized_pnl: float
    win_rate: float


# -- Endpoints --

@router.post("/positions", response_model=PositionOut, status_code=201)
async def create_position(
    body: OpenPositionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Open a new position."""
    position = await service.open_position(
        db,
        market_id=body.market_id,
        outcome_id=body.outcome_id,
        platform=body.platform,
        side=body.side,
        quantity=body.quantity,
        price=body.price,
        signal_id=body.signal_id,
        notes=body.notes,
    )
    return position


@router.get("/positions", response_model=PositionListOut)
async def list_positions(
    request: Request,
    db: AsyncSession = Depends(get_db),
    status: str | None = Query(None, pattern="^(open|closed|resolved)$"),
    platform: str | None = Query(None),
    market_id: uuid.UUID | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    """List positions with optional filters."""
    positions, total = await service.list_positions(
        db, status=status, platform=platform, market_id=market_id,
        page=page, page_size=page_size,
    )
    return PositionListOut(
        positions=positions, total=total, page=page, page_size=page_size,
    )


@router.get("/portfolio/summary", response_model=PortfolioSummaryOut)
async def portfolio_summary(
    db: AsyncSession = Depends(get_db),
):
    """Aggregate portfolio stats."""
    return await service.get_portfolio_summary(db)


@router.get("/portfolio/export/csv")
async def export_csv(
    db: AsyncSession = Depends(get_db),
):
    """Export all positions and trades as CSV."""
    positions, _ = await service.list_positions(db, page=1, page_size=5000)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "id", "market_id", "outcome_id", "platform", "side", "quantity",
        "avg_entry_price", "current_price", "unrealized_pnl", "status",
        "exit_price", "realized_pnl", "signal_id", "notes", "created_at",
    ])
    for p in positions:
        writer.writerow([
            str(p.id), str(p.market_id), str(p.outcome_id), p.platform,
            p.side, p.quantity, p.avg_entry_price, p.current_price,
            p.unrealized_pnl, p.status, p.exit_price, p.realized_pnl,
            str(p.signal_id) if p.signal_id else "", p.notes or "",
            p.created_at.isoformat(),
        ])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=portfolio.csv"},
    )


@router.get("/positions/{position_id}", response_model=PositionDetailOut)
async def get_position(
    position_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get position detail with trade history."""
    position = await service.get_position(db, position_id)
    if position is None:
        raise HTTPException(404, "Position not found")
    return position


@router.post("/positions/{position_id}/trades", response_model=PositionOut)
async def add_trade(
    position_id: uuid.UUID,
    body: AddTradeRequest,
    db: AsyncSession = Depends(get_db),
):
    """Add a trade to an existing position (buy more or partial sell)."""
    try:
        if body.action == "buy":
            return await service.add_to_position(
                db, position_id, body.quantity, body.price, body.fees,
            )
        else:
            return await service.close_position(
                db, position_id, body.quantity, body.price, body.fees,
            )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.put("/positions/{position_id}/close", response_model=PositionOut)
async def close_position(
    position_id: uuid.UUID,
    body: ClosePositionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Close a position (partially or fully) at the given price."""
    try:
        return await service.close_position(
            db, position_id, body.quantity, body.price, body.fees,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
