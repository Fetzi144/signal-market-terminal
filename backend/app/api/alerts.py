"""Alert API endpoints."""
import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.market import Market
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("/recent")
async def recent_alerts(
    signal_type: str | None = None,
    platform: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get the most recently alerted signals with market context."""
    query = (
        select(Signal, Market.question, Market.platform)
        .join(Market, Signal.market_id == Market.id)
        .where(Signal.alerted.is_(True))
    )
    count_query = select(func.count(Signal.id)).where(Signal.alerted.is_(True))

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
        count_query = count_query.where(Signal.signal_type == signal_type)

    if platform:
        query = query.join(Market, Signal.market_id == Market.id, isouter=True).where(Market.platform == platform)
        count_query = count_query.join(Market, Signal.market_id == Market.id).where(Market.platform == platform)

    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(Signal.fired_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    rows = result.all()

    return {
        "alerts": [
            {
                "id": str(s.id),
                "signal_type": s.signal_type,
                "market_id": str(s.market_id),
                "market_question": question,
                "platform": mkt_platform,
                "rank_score": float(s.rank_score),
                "signal_score": float(s.signal_score),
                "confidence": float(s.confidence),
                "fired_at": s.fired_at.isoformat() if s.fired_at else None,
                "details": s.details,
            }
            for s, question, mkt_platform in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
