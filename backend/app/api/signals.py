"""Signal feed and detail endpoints."""
import csv
import io
import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.market import Market
from app.models.signal import Signal, SignalEvaluation
from app.strategy_families import (
    display_signal_label,
    display_signal_type,
    infer_signal_review_family,
)

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])
signals_limiter = Limiter(key_func=get_remote_address)


class EvaluationOut(BaseModel):
    horizon: str
    price_at_eval: Decimal | None
    price_change: Decimal | None
    price_change_pct: Decimal | None
    evaluated_at: datetime


class SignalOut(BaseModel):
    id: uuid.UUID
    signal_type: str
    display_signal_type: str | None = None
    display_signal_label: str | None = None
    timeframe: str
    market_id: uuid.UUID
    outcome_id: uuid.UUID | None
    fired_at: datetime
    observed_at_exchange: datetime | None = None
    received_at_local: datetime | None = None
    detected_at_local: datetime | None = None
    source_platform: str | None = None
    source_token_id: str | None = None
    source_event_type: str | None = None
    signal_score: Decimal
    confidence: Decimal
    rank_score: Decimal
    details: dict
    price_at_fire: Decimal | None
    resolved: bool
    resolved_correctly: bool | None = None
    market_question: str | None = None
    platform: str | None = None
    evaluations: list[EvaluationOut] = []
    # Probability engine fields
    estimated_probability: Decimal | None = None
    probability_adjustment: Decimal | None = None
    # Trading intelligence fields
    expected_value: Decimal | None = None
    direction: str | None = None
    edge_pct: Decimal | None = None
    recommended_size_usd: Decimal | None = None
    kelly_fraction: Decimal | None = None
    review_family: str | None = None
    review_family_label: str | None = None
    review_family_posture: str | None = None
    review_family_review_enabled: bool | None = None
    review_family_disabled_reason: str | None = None


class SignalListOut(BaseModel):
    signals: list[SignalOut]
    total: int
    page: int
    page_size: int


def _build_signal_out(signal: Signal, question: str | None, platform: str | None) -> SignalOut:
    """Build a SignalOut with trading intelligence fields computed on the fly."""
    direction = None
    edge_pct = None
    recommended_size_usd = None
    kelly_fraction = None
    review_family = infer_signal_review_family(signal.signal_type)

    if (
        signal.estimated_probability is not None
        and signal.price_at_fire is not None
        and signal.expected_value is not None
    ):
        from app.config import settings
        from app.signals.ev import compute_ev_full
        from app.signals.kelly import kelly_size

        ev_data = compute_ev_full(signal.estimated_probability, signal.price_at_fire)
        direction = ev_data["direction"].replace("_", " ").upper()
        edge_pct = ev_data["edge_pct"]

        sizing = kelly_size(
            estimated_prob=signal.estimated_probability,
            market_price=signal.price_at_fire,
            bankroll=Decimal(str(settings.default_bankroll)),
            kelly_fraction=Decimal(str(settings.kelly_multiplier)),
            max_position_pct=Decimal(str(settings.max_single_position_pct)),
        )
        recommended_size_usd = sizing["recommended_size_usd"]
        kelly_fraction = sizing["kelly_used"]

    return SignalOut(
        id=signal.id,
        signal_type=signal.signal_type,
        display_signal_type=display_signal_type(signal.signal_type),
        display_signal_label=display_signal_label(signal.signal_type),
        timeframe=signal.timeframe,
        market_id=signal.market_id,
        outcome_id=signal.outcome_id,
        fired_at=signal.fired_at,
        observed_at_exchange=signal.observed_at_exchange,
        received_at_local=signal.received_at_local,
        detected_at_local=signal.detected_at_local,
        source_platform=signal.source_platform,
        source_token_id=signal.source_token_id,
        source_event_type=signal.source_event_type,
        signal_score=signal.signal_score,
        confidence=signal.confidence,
        rank_score=signal.rank_score,
        details=signal.details,
        price_at_fire=signal.price_at_fire,
        resolved=signal.resolved,
        resolved_correctly=signal.resolved_correctly,
        market_question=question,
        platform=platform,
        estimated_probability=signal.estimated_probability,
        probability_adjustment=signal.probability_adjustment,
        expected_value=signal.expected_value,
        direction=direction,
        edge_pct=edge_pct,
        recommended_size_usd=recommended_size_usd,
        kelly_fraction=kelly_fraction,
        review_family=review_family["family"] if review_family else None,
        review_family_label=review_family["label"] if review_family else None,
        review_family_posture=review_family["posture"] if review_family else None,
        review_family_review_enabled=review_family["review_enabled"] if review_family else None,
        review_family_disabled_reason=review_family["disabled_reason"] if review_family else None,
    )


@router.get("", response_model=SignalListOut)
@signals_limiter.limit("10/second")
async def list_signals(
    request: Request,
    signal_type: str | None = None,
    market_id: uuid.UUID | None = None,
    platform: str | None = None,
    timeframe: str | None = None,
    resolved_correctly: bool | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    if limit is not None:
        page_size = limit

    query = select(Signal, Market.question, Market.platform).join(Market, Signal.market_id == Market.id)
    count_query = select(func.count(Signal.id))

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
        count_query = count_query.where(Signal.signal_type == signal_type)
    if market_id:
        query = query.where(Signal.market_id == market_id)
        count_query = count_query.where(Signal.market_id == market_id)
    if platform:
        count_query = count_query.join(Market, Signal.market_id == Market.id).where(Market.platform == platform)
        query = query.where(Market.platform == platform)
    if timeframe:
        query = query.where(Signal.timeframe == timeframe)
        count_query = count_query.where(Signal.timeframe == timeframe)
    if resolved_correctly is not None:
        query = query.where(Signal.resolved_correctly == resolved_correctly)
        count_query = count_query.where(Signal.resolved_correctly == resolved_correctly)

    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(Signal.rank_score.desc(), Signal.fired_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    rows = result.all()

    signals = []
    for signal, question, mkt_platform in rows:
        signals.append(_build_signal_out(signal, question, mkt_platform))

    return SignalListOut(signals=signals, total=total, page=page, page_size=page_size)


@router.get("/types")
async def list_signal_types(db: AsyncSession = Depends(get_db)):
    """Return distinct signal_type values from the database."""
    result = await db.execute(
        select(Signal.signal_type).distinct().order_by(Signal.signal_type)
    )
    return {"types": [row for row in result.scalars().all()]}


@router.get("/timeframes")
async def list_signal_timeframes(db: AsyncSession = Depends(get_db)):
    """Return distinct timeframe values from the database."""
    result = await db.execute(
        select(Signal.timeframe).distinct().order_by(Signal.timeframe)
    )
    return {"timeframes": [row for row in result.scalars().all()]}


@router.get("/{signal_id}", response_model=SignalOut)
@signals_limiter.limit("10/second")
async def get_signal(request: Request, signal_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Signal, Market.question, Market.platform)
        .join(Market, Signal.market_id == Market.id)
        .where(Signal.id == signal_id)
    )
    row = result.first()
    if row is None:
        raise HTTPException(404, "Signal not found")

    signal, question, mkt_platform = row

    # Fetch evaluations
    eval_result = await db.execute(
        select(SignalEvaluation).where(SignalEvaluation.signal_id == signal_id)
    )
    evals = [
        EvaluationOut(
            horizon=e.horizon,
            price_at_eval=e.price_at_eval,
            price_change=e.price_change,
            price_change_pct=e.price_change_pct,
            evaluated_at=e.evaluated_at,
        )
        for e in eval_result.scalars().all()
    ]

    out = _build_signal_out(signal, question, mkt_platform)
    out.evaluations = evals
    return out


@router.get("/export/csv")
async def export_signals_csv(
    signal_type: str | None = None,
    market_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export signals as CSV."""
    query = select(Signal, Market.question).join(Market, Signal.market_id == Market.id)

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
    if market_id:
        query = query.where(Signal.market_id == market_id)

    query = query.order_by(Signal.rank_score.desc(), Signal.fired_at.desc()).limit(5000)

    result = await db.execute(query)
    rows = result.all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "signal_type", "market_question", "rank_score",
        "signal_score", "confidence", "price_at_fire", "fired_at", "resolved",
        "resolved_correctly",
    ])
    for signal, question in rows:
        resolved_correctly_val = ""
        if signal.resolved_correctly is not None:
            resolved_correctly_val = signal.resolved_correctly
        writer.writerow([
            str(signal.id), signal.signal_type, question,
            float(signal.rank_score), float(signal.signal_score),
            float(signal.confidence),
            float(signal.price_at_fire) if signal.price_at_fire else "",
            signal.fired_at.isoformat() if signal.fired_at else "",
            signal.resolved,
            resolved_correctly_val,
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=signals.csv"},
    )
