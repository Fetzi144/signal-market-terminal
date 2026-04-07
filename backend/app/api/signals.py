"""Signal feed and detail endpoints."""
import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.signal import Signal, SignalEvaluation
from app.models.market import Market

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


class EvaluationOut(BaseModel):
    horizon: str
    price_at_eval: Decimal | None
    price_change: Decimal | None
    price_change_pct: Decimal | None
    evaluated_at: datetime


class SignalOut(BaseModel):
    id: uuid.UUID
    signal_type: str
    market_id: uuid.UUID
    outcome_id: uuid.UUID | None
    fired_at: datetime
    signal_score: Decimal
    confidence: Decimal
    rank_score: Decimal
    details: dict
    price_at_fire: Decimal | None
    resolved: bool
    market_question: str | None = None
    evaluations: list[EvaluationOut] = []


class SignalListOut(BaseModel):
    signals: list[SignalOut]
    total: int
    page: int
    page_size: int


@router.get("", response_model=SignalListOut)
async def list_signals(
    signal_type: str | None = None,
    market_id: uuid.UUID | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    query = select(Signal, Market.question).join(Market, Signal.market_id == Market.id)
    count_query = select(func.count(Signal.id))

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
        count_query = count_query.where(Signal.signal_type == signal_type)
    if market_id:
        query = query.where(Signal.market_id == market_id)
        count_query = count_query.where(Signal.market_id == market_id)

    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(Signal.rank_score.desc(), Signal.fired_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    rows = result.all()

    signals = []
    for signal, question in rows:
        signals.append(SignalOut(
            id=signal.id,
            signal_type=signal.signal_type,
            market_id=signal.market_id,
            outcome_id=signal.outcome_id,
            fired_at=signal.fired_at,
            signal_score=signal.signal_score,
            confidence=signal.confidence,
            rank_score=signal.rank_score,
            details=signal.details,
            price_at_fire=signal.price_at_fire,
            resolved=signal.resolved,
            market_question=question,
        ))

    return SignalListOut(signals=signals, total=total, page=page, page_size=page_size)


@router.get("/{signal_id}", response_model=SignalOut)
async def get_signal(signal_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Signal, Market.question)
        .join(Market, Signal.market_id == Market.id)
        .where(Signal.id == signal_id)
    )
    row = result.first()
    if row is None:
        raise HTTPException(404, "Signal not found")

    signal, question = row

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

    return SignalOut(
        id=signal.id,
        signal_type=signal.signal_type,
        market_id=signal.market_id,
        outcome_id=signal.outcome_id,
        fired_at=signal.fired_at,
        signal_score=signal.signal_score,
        confidence=signal.confidence,
        rank_score=signal.rank_score,
        details=signal.details,
        price_at_fire=signal.price_at_fire,
        resolved=signal.resolved,
        market_question=question,
        evaluations=evals,
    )
