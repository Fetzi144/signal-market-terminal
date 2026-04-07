"""Alert API endpoints."""
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("/recent")
async def recent_alerts(limit: int = 50, db: AsyncSession = Depends(get_db)):
    """Get the most recently alerted signals."""
    result = await db.execute(
        select(Signal)
        .where(Signal.alerted.is_(True))
        .order_by(Signal.fired_at.desc())
        .limit(min(limit, 100))
    )
    signals = result.scalars().all()

    return {
        "alerts": [
            {
                "id": str(s.id),
                "signal_type": s.signal_type,
                "market_id": str(s.market_id),
                "rank_score": float(s.rank_score),
                "signal_score": float(s.signal_score),
                "confidence": float(s.confidence),
                "fired_at": s.fired_at.isoformat() if s.fired_at else None,
                "details": s.details,
            }
            for s in signals
        ],
        "total": len(signals),
    }
