"""Market listing and detail endpoints."""
import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, select
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
    platform: str | None = None,
    search: str | None = None,
    category: str | None = None,
    sort_by: str = Query("updated", pattern="^(updated|volume|end_date|question)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    query = select(Market)
    count_query = select(func.count(Market.id))

    if active is not None:
        query = query.where(Market.active == active)
        count_query = count_query.where(Market.active == active)

    if platform:
        query = query.where(Market.platform == platform)
        count_query = count_query.where(Market.platform == platform)

    if search:
        query = query.where(Market.question.ilike(f"%{search}%"))
        count_query = count_query.where(Market.question.ilike(f"%{search}%"))

    if category:
        query = query.where(Market.category == category)
        count_query = count_query.where(Market.category == category)

    total = (await db.execute(count_query)).scalar() or 0

    sort_map = {
        "updated": Market.updated_at.desc(),
        "volume": Market.last_volume_24h.desc().nulls_last(),
        "end_date": Market.end_date.asc().nulls_last(),
        "question": Market.question.asc(),
    }
    query = query.order_by(sort_map.get(sort_by, Market.updated_at.desc()))
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


@router.get("/platforms")
async def list_platforms(db: AsyncSession = Depends(get_db)):
    """Return distinct platform values from the database."""
    result = await db.execute(
        select(Market.platform).distinct().order_by(Market.platform)
    )
    return {"platforms": [row for row in result.scalars().all()]}


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


RANGE_HOURS = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}


@router.get("/{market_id}/chart-data")
async def get_chart_data(
    market_id: uuid.UUID,
    range: str = Query("24h", pattern="^(1h|6h|24h|7d)$"),
    db: AsyncSession = Depends(get_db),
):
    """Return price time series for each outcome, suitable for charting."""
    hours = RANGE_HOURS.get(range, 24)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # Get outcomes for this market
    outcome_result = await db.execute(
        select(Outcome).where(Outcome.market_id == market_id)
    )
    outcomes = outcome_result.scalars().all()

    if not outcomes:
        raise HTTPException(404, "Market not found or has no outcomes")

    series = {}
    for outcome in outcomes:
        result = await db.execute(
            select(PriceSnapshot)
            .where(
                PriceSnapshot.outcome_id == outcome.id,
                PriceSnapshot.captured_at >= cutoff,
            )
            .order_by(PriceSnapshot.captured_at.asc())
        )
        snaps = result.scalars().all()
        series[outcome.name] = [
            {
                "time": s.captured_at.isoformat(),
                "price": float(s.price),
                "volume_24h": float(s.volume_24h) if s.volume_24h else None,
            }
            for s in snaps
        ]

    return {"market_id": str(market_id), "range": range, "series": series}


@router.get("/export/csv")
async def export_markets_csv(
    active: bool | None = True,
    db: AsyncSession = Depends(get_db),
):
    """Export markets as CSV."""
    query = select(Market)
    if active is not None:
        query = query.where(Market.active == active)
    query = query.order_by(Market.updated_at.desc())

    result = await db.execute(query)
    markets = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "platform", "question", "category", "end_date", "active", "volume_24h", "liquidity"])
    for m in markets:
        writer.writerow([
            str(m.id), m.platform, m.question, m.category,
            m.end_date.isoformat() if m.end_date else "",
            m.active, str(m.last_volume_24h or ""), str(m.last_liquidity or ""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=markets.csv"},
    )
