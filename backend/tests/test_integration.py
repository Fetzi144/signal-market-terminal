"""Integration tests: full pipeline from snapshot data to signal detection + persistence."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models.signal import Signal
from app.ranking.scorer import persist_signals
from app.signals.price_move import PriceMoveDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_full_pipeline_snapshot_to_signal(session):
    """End-to-end: create snapshots -> detect signal -> persist with rank_score."""
    market = make_market(session, question="Will ETH hit $5000?")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)

    # Create price snapshots showing a significant move
    make_price_snapshot(session, outcome.id, "0.40", captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, outcome.id, "0.42", captured_at=now - timedelta(minutes=15))
    make_price_snapshot(session, outcome.id, "0.55", captured_at=now - timedelta(minutes=2))
    await session.commit()

    # Step 1: Detect
    detector = PriceMoveDetector()
    candidates = await detector.detect(session)
    assert len(candidates) >= 1

    candidate = candidates[0]
    assert candidate.signal_type == "price_move"
    assert float(candidate.signal_score) > 0
    assert float(candidate.confidence) > 0

    # Step 2: Persist
    created = await persist_signals(session, candidates)
    assert created >= 1

    # Step 3: Verify in DB
    result = await session.execute(
        select(Signal).where(Signal.market_id == market.id)
    )
    signals = result.scalars().all()
    assert len(signals) >= 1

    sig = signals[0]
    assert sig.signal_type == "price_move"
    assert sig.rank_score > 0
    assert sig.dedupe_bucket is not None
    assert sig.details["market_question"] == "Will ETH hit $5000?"


@pytest.mark.asyncio
async def test_dedupe_prevents_duplicate_signals(session):
    """persist_signals should not create duplicate signals in the same 15-min bucket."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    make_price_snapshot(session, outcome.id, "0.40", captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, outcome.id, "0.55", captured_at=now - timedelta(minutes=2))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)
    assert len(candidates) >= 1

    # Persist once
    created1 = await persist_signals(session, candidates)
    assert created1 >= 1

    # Persist same candidates again — should be deduped
    created2 = await persist_signals(session, candidates)
    assert created2 == 0

    # Only one signal in DB
    result = await session.execute(select(Signal).where(Signal.market_id == market.id))
    assert len(result.scalars().all()) == 1
