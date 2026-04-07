"""Cross-platform analytics endpoints."""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
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
async def signal_accuracy(
    days: int = Query(None, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Signal accuracy per signal_type per horizon.

    Returns both ground-truth resolution accuracy and price-direction accuracy.
    """
    # Price-direction accuracy (original method)
    eval_query = (
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
    )
    if days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        eval_query = eval_query.where(Signal.fired_at >= cutoff)
    eval_query = eval_query.group_by(Signal.signal_type, SignalEvaluation.horizon)

    result = await db.execute(eval_query)
    rows = result.all()

    # Build price-direction data keyed by (signal_type, horizon)
    price_dir = {}
    for signal_type, horizon, total, positive, negative, avg_change in rows:
        price_dir[(signal_type, horizon)] = {
            "total_evaluations": total,
            "positive_moves": positive or 0,
            "negative_moves": negative or 0,
            "price_direction_accuracy_pct": round((positive or 0) / total * 100, 1) if total > 0 else 0,
            "avg_abs_change_pct": round(float(avg_change), 2) if avg_change else 0,
        }

    # Ground-truth resolution accuracy per signal_type
    resolution_query = (
        select(
            Signal.signal_type,
            func.count(Signal.id),
            func.sum(case((Signal.resolved_correctly.isnot(None), 1), else_=0)),
            func.sum(case((Signal.resolved_correctly.is_(True), 1), else_=0)),
        )
    )
    if days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        resolution_query = resolution_query.where(Signal.fired_at >= cutoff)
    resolution_query = resolution_query.group_by(Signal.signal_type)

    res_result = await db.execute(resolution_query)
    res_rows = res_result.all()

    resolution_data = {}
    for signal_type, total_signals, resolved_count, correct_count in res_rows:
        resolved_count = resolved_count or 0
        correct_count = correct_count or 0
        resolution_data[signal_type] = {
            "total_signals": total_signals,
            "resolved_count": resolved_count,
            "correct_count": correct_count,
            "resolution_rate_pct": round(resolved_count / total_signals * 100, 1) if total_signals > 0 else 0,
            "accuracy_pct": round(correct_count / resolved_count * 100, 1) if resolved_count > 0 else 0,
        }

    # Merge into final response
    accuracy = []
    for (signal_type, horizon), pd in price_dir.items():
        res = resolution_data.get(signal_type, {})
        accuracy.append({
            "signal_type": signal_type,
            "horizon": horizon,
            "total_evaluations": pd["total_evaluations"],
            "positive_moves": pd["positive_moves"],
            "negative_moves": pd["negative_moves"],
            "price_direction_accuracy_pct": pd["price_direction_accuracy_pct"],
            "avg_abs_change_pct": pd["avg_abs_change_pct"],
            "accuracy_pct": res.get("accuracy_pct", 0),
            "resolution_rate_pct": res.get("resolution_rate_pct", 0),
            "total_signals": res.get("total_signals", 0),
            "resolved_count": res.get("resolved_count", 0),
        })

    return {"accuracy": accuracy}


@router.get("/correlated-signals")
async def correlated_signals(
    hours: int = 1,
    db: AsyncSession = Depends(get_db),
):
    """Find signals across different platforms that fired on the same category within a time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(hours, 1))

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
