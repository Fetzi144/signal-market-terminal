"""Cross-platform analytics endpoints."""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.market import Market
from app.models.signal import Signal, SignalEvaluation

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])


@router.get("/platform-summary")
async def platform_summary(db: AsyncSession = Depends(get_db)):
    """Per-platform stats: market count, signal count, avg rank score."""
    # Market counts
    market_result = await db.execute(
        select(Market.platform, func.count(Market.id))
        .where(Market.active.is_(True))
        .group_by(Market.platform)
    )
    market_counts = dict(market_result.all())

    # Signal counts and avg rank per platform
    signal_result = await db.execute(
        select(
            Market.platform,
            func.count(Signal.id),
            func.avg(Signal.rank_score),
        )
        .join(Market, Signal.market_id == Market.id)
        .group_by(Market.platform)
    )
    signal_stats = {row[0]: {"count": row[1], "avg_rank": float(row[2]) if row[2] else 0} for row in signal_result.all()}

    platforms = sorted(set(list(market_counts.keys()) + list(signal_stats.keys())))
    summary = []
    for p in platforms:
        summary.append({
            "platform": p,
            "active_markets": market_counts.get(p, 0),
            "total_signals": signal_stats.get(p, {}).get("count", 0),
            "avg_rank_score": round(signal_stats.get(p, {}).get("avg_rank", 0), 3),
        })

    return {"platforms": summary}


@router.get("/signal-accuracy")
async def signal_accuracy(db: AsyncSession = Depends(get_db)):
    """Directional accuracy per signal_type per horizon.

    A signal is "accurate" if the price moved in the signaled direction:
    - direction=up and price_change_pct > 0
    - direction=down and price_change_pct < 0
    """
    result = await db.execute(
        select(
            Signal.signal_type,
            SignalEvaluation.horizon,
            func.count(SignalEvaluation.id),
            func.sum(
                case(
                    (SignalEvaluation.price_change_pct > 0, 1),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (SignalEvaluation.price_change_pct < 0, 1),
                    else_=0,
                )
            ),
            func.avg(func.abs(SignalEvaluation.price_change_pct)),
        )
        .join(Signal, SignalEvaluation.signal_id == Signal.id)
        .group_by(Signal.signal_type, SignalEvaluation.horizon)
    )

    rows = result.all()
    accuracy = []
    for signal_type, horizon, total, positive, negative, avg_change in rows:
        accuracy.append({
            "signal_type": signal_type,
            "horizon": horizon,
            "total_evaluations": total,
            "positive_moves": positive or 0,
            "negative_moves": negative or 0,
            "accuracy_pct": round((positive or 0) / total * 100, 1) if total > 0 else 0,
            "avg_abs_change_pct": round(float(avg_change), 2) if avg_change else 0,
        })

    return {"accuracy": accuracy}


@router.get("/correlated-signals")
async def correlated_signals(
    hours: int = 1,
    db: AsyncSession = Depends(get_db),
):
    """Find signals across different platforms that fired on the same category within a time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(hours, 1) * 24)

    # Get recent signals with market info
    result = await db.execute(
        select(Signal, Market.platform, Market.question, Market.category)
        .join(Market, Signal.market_id == Market.id)
        .where(Signal.fired_at >= cutoff, Market.category.isnot(None))
        .order_by(Signal.fired_at.desc())
        .limit(500)
    )
    rows = result.all()

    # Group by category
    by_category: dict[str, list] = {}
    for signal, platform, question, category in rows:
        if category not in by_category:
            by_category[category] = []
        by_category[category].append({
            "signal_id": str(signal.id),
            "signal_type": signal.signal_type,
            "platform": platform,
            "market_question": question,
            "rank_score": float(signal.rank_score),
            "fired_at": signal.fired_at.isoformat(),
        })

    # Only keep categories with signals from multiple platforms
    correlated = []
    for category, signals in by_category.items():
        platforms = {s["platform"] for s in signals}
        if len(platforms) >= 2:
            correlated.append({
                "category": category,
                "platforms": sorted(platforms),
                "signal_count": len(signals),
                "signals": signals[:10],  # limit per category
            })

    correlated.sort(key=lambda c: c["signal_count"], reverse=True)

    return {"correlated": correlated[:20]}
