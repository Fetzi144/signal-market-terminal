"""Performance dashboard metrics endpoint."""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1/performance", tags=["performance"])

_LOOKBACK_DAYS = 30
_MIN_RESOLVED_FOR_RANKING = 10
_THRESHOLD_STEP = 0.05


@router.get("/summary")
async def performance_summary(db: AsyncSession = Depends(get_db)):
    """Return all performance metrics for the dashboard."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)

    # ── Overall win rate ────────────────────────────────────────────────────
    overall_result = await db.execute(
        select(
            func.count(Signal.id),
            func.sum(case((Signal.resolved_correctly.is_(True), 1), else_=0)),
        ).where(
            Signal.resolved_correctly.isnot(None),
            Signal.fired_at >= cutoff,
        )
    )
    total_resolved, total_correct = overall_result.one()
    total_resolved = total_resolved or 0
    total_correct = total_correct or 0
    overall_win_rate = round(total_correct / total_resolved, 4) if total_resolved > 0 else None

    # ── Win rate by signal type ─────────────────────────────────────────────
    type_result = await db.execute(
        select(
            Signal.signal_type,
            func.count(Signal.id),
            func.sum(case((Signal.resolved_correctly.is_(True), 1), else_=0)),
        )
        .where(Signal.resolved_correctly.isnot(None), Signal.fired_at >= cutoff)
        .group_by(Signal.signal_type)
    )
    win_rate_by_type = []
    type_rows = type_result.all()
    for signal_type, resolved, correct in type_rows:
        resolved = resolved or 0
        correct = correct or 0
        win_rate_by_type.append({
            "signal_type": signal_type,
            "resolved": resolved,
            "correct": correct,
            "win_rate": round(correct / resolved, 4) if resolved > 0 else None,
        })
    win_rate_by_type.sort(key=lambda r: r["win_rate"] or 0, reverse=True)

    # ── Best / worst detector (min 10 resolved signals) ────────────────────
    qualified = [r for r in win_rate_by_type if r["resolved"] >= _MIN_RESOLVED_FOR_RANKING and r["win_rate"] is not None]
    best_detector = qualified[0]["signal_type"] if qualified else None
    worst_detector = qualified[-1]["signal_type"] if qualified else None

    # ── Daily win rate trend (last 30 days) ─────────────────────────────────
    trend_result = await db.execute(
        select(
            func.date(Signal.fired_at).label("day"),
            func.count(Signal.id),
            func.sum(case((Signal.resolved_correctly.is_(True), 1), else_=0)),
        )
        .where(Signal.resolved_correctly.isnot(None), Signal.fired_at >= cutoff)
        .group_by(func.date(Signal.fired_at))
        .order_by(func.date(Signal.fired_at))
    )
    trend_rows = trend_result.all()
    win_rate_trend = []
    for day, resolved, correct in trend_rows:
        resolved = resolved or 0
        correct = correct or 0
        win_rate_trend.append({
            "date": str(day),
            "resolved": resolved,
            "correct": correct,
            "win_rate": round(correct / resolved, 4) if resolved > 0 else None,
        })

    # ── Avg rank of winners vs losers ──────────────────────────────────────
    rank_result = await db.execute(
        select(
            Signal.resolved_correctly,
            func.avg(Signal.rank_score),
        )
        .where(Signal.resolved_correctly.isnot(None), Signal.fired_at >= cutoff)
        .group_by(Signal.resolved_correctly)
    )
    avg_rank_winners = None
    avg_rank_losers = None
    for resolved_correctly, avg_rank in rank_result.all():
        val = round(float(avg_rank), 4) if avg_rank is not None else None
        if resolved_correctly:
            avg_rank_winners = val
        else:
            avg_rank_losers = val

    # ── Optimal rank threshold ──────────────────────────────────────────────
    # For each threshold bucket, compute win rate on signals >= that threshold
    threshold_result = await db.execute(
        select(Signal.rank_score, Signal.resolved_correctly)
        .where(Signal.resolved_correctly.isnot(None), Signal.fired_at >= cutoff)
    )
    threshold_rows = threshold_result.all()

    optimal_threshold = None
    threshold_curve = []
    if threshold_rows:
        rows_data = [(float(rs), rc) for rs, rc in threshold_rows]
        best_wr = -1.0
        thresholds = [round(i * _THRESHOLD_STEP, 2) for i in range(int(1.0 / _THRESHOLD_STEP) + 1)]
        for t in thresholds:
            subset = [(rs, rc) for rs, rc in rows_data if rs >= t]
            if not subset:
                continue
            wr = sum(1 for _, rc in subset if rc) / len(subset)
            threshold_curve.append({
                "threshold": t,
                "win_rate": round(wr, 4),
                "signal_count": len(subset),
            })
            if wr > best_wr and len(subset) >= 5:
                best_wr = wr
                optimal_threshold = t

    # ── Pending / resolved counts ──────────────────────────────────────────
    pending_result = await db.execute(
        select(func.count(Signal.id))
        .where(Signal.resolved.is_(False))
    )
    signals_pending = pending_result.scalar() or 0

    resolved_markets_result = await db.execute(
        select(func.count(Signal.market_id.distinct()))
        .where(Signal.resolved.is_(True))
    )
    total_markets_resolved = resolved_markets_result.scalar() or 0

    # ── Total signals fired (last 30 days) ────────────────────────────────
    fired_result = await db.execute(
        select(func.count(Signal.id)).where(Signal.fired_at >= cutoff)
    )
    total_signals_fired = fired_result.scalar() or 0

    # ── Recent resolved signals (last 20) ─────────────────────────────────
    recent_result = await db.execute(
        select(
            Signal.id,
            Signal.signal_type,
            Signal.fired_at,
            Signal.rank_score,
            Signal.resolved_correctly,
            Signal.market_id,
        )
        .where(Signal.resolved_correctly.isnot(None))
        .order_by(Signal.fired_at.desc())
        .limit(20)
    )
    recent_calls = [
        {
            "id": str(row.id),
            "signal_type": row.signal_type,
            "fired_at": row.fired_at.isoformat(),
            "rank_score": float(row.rank_score),
            "resolved_correctly": row.resolved_correctly,
            "market_id": str(row.market_id),
        }
        for row in recent_result.all()
    ]

    return {
        "overall_win_rate": overall_win_rate,
        "total_resolved": total_resolved,
        "total_signals_fired": total_signals_fired,
        "signals_pending_resolution": signals_pending,
        "total_markets_resolved": total_markets_resolved,
        "win_rate_by_type": win_rate_by_type,
        "win_rate_trend": win_rate_trend,
        "best_detector": best_detector,
        "worst_detector": worst_detector,
        "avg_rank_of_winners": avg_rank_winners,
        "avg_rank_of_losers": avg_rank_losers,
        "optimal_threshold": optimal_threshold,
        "threshold_curve": threshold_curve,
        "recent_calls": recent_calls,
        "lookback_days": _LOOKBACK_DAYS,
    }
