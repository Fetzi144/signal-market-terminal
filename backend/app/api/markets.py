"""Market listing and detail endpoints."""
import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot

router = APIRouter(prefix="/api/v1/markets", tags=["markets"])


class OutcomeOut(BaseModel):
    id: uuid.UUID
    name: str
    token_id: str | None
    latest_price: Decimal | None = None


class MarketOut(BaseModel):
    id: uuid.UUID
    platform: str
    platform_id: str
    slug: str | None
    question: str
    category: str | None
    end_date: datetime | None
    active: bool
    outcomes: list[OutcomeOut] = []


class MarketListOut(BaseModel):
    markets: list[MarketOut]
    total: int
    page: int
    page_size: int


class SnapshotOut(BaseModel):
    outcome_id: uuid.UUID
    price: Decimal
    volume_24h: Decimal | None
    captured_at: datetime


@router.get("", response_model=MarketListOut)
async def list_markets(
    active: bool | None = True,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    query = select(Market)
    count_query = select(func.count(Market.id))

    if active is not None:
        query = query.where(Market.active == active)
        count_query = count_query.where(Market.active == active)

    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(Market.updated_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    markets = result.scalars().all()

    return MarketListOut(
        markets=[
            MarketOut(
                id=m.id,
                platform=m.platform,
                platform_id=m.platform_id,
                slug=m.slug,
                question=m.question,
                category=m.category,
                end_date=m.end_date,
                active=m.active,
            )
            for m in markets
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{market_id}", response_model=MarketOut)
async def get_market(market_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Market).where(Market.id == market_id))
    market = result.scalar_one_or_none()
    if market is None:
        raise HTTPException(404, "Market not found")

    # Fetch outcomes with latest price
    outcome_result = await db.execute(
        select(Outcome).where(Outcome.market_id == market_id)
    )
    outcomes_out = []
    for outcome in outcome_result.scalars().all():
        # Get latest price
        price_result = await db.execute(
            select(PriceSnapshot.price)
            .where(PriceSnapshot.outcome_id == outcome.id)
            .order_by(PriceSnapshot.captured_at.desc())
            .limit(1)
        )
        latest_price = price_result.scalar_one_or_none()

        outcomes_out.append(OutcomeOut(
            id=outcome.id,
            name=outcome.name,
            token_id=outcome.token_id,
            latest_price=latest_price,
        ))

    return MarketOut(
        id=market.id,
        platform=market.platform,
        platform_id=market.platform_id,
        slug=market.slug,
        question=market.question,
        category=market.category,
        end_date=market.end_date,
        active=market.active,
        outcomes=outcomes_out,
    )


@router.get("/{market_id}/snapshots", response_model=list[SnapshotOut])
async def get_market_snapshots(
    market_id: uuid.UUID,
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    # Get outcome IDs for this market
    outcome_ids_result = await db.execute(
        select(Outcome.id).where(Outcome.market_id == market_id)
    )
    outcome_ids = [r for r in outcome_ids_result.scalars().all()]

    if not outcome_ids:
        return []

    result = await db.execute(
        select(PriceSnapshot)
        .where(PriceSnapshot.outcome_id.in_(outcome_ids))
        .order_by(PriceSnapshot.captured_at.desc())
        .limit(limit)
    )

    return [
        SnapshotOut(
            outcome_id=s.outcome_id,
            price=s.price,
            volume_24h=s.volume_24h,
            captured_at=s.captured_at,
        )
        for s in result.scalars().all()
    ]
