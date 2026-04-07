"""Tests for scheduler signal broadcast: ensure only newly-created signals are broadcast."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.models.signal import Signal
from app.ranking.scorer import persist_signals
from app.signals.base import SignalCandidate
from tests.conftest import make_market, make_outcome


@pytest.mark.asyncio
async def test_persist_signals_returns_new_signal_objects(session):
    """persist_signals returns (count, list[Signal]) where list contains only new signals."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    candidates = [
        SignalCandidate(
            signal_type="price_move",
            market_id=str(market.id),
            outcome_id=str(outcome.id),
            signal_score=Decimal("0.600"),
            confidence=Decimal("0.800"),
            details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
            price_at_fire=Decimal("0.500"),
        ),
    ]

    created, new_signals = await persist_signals(session, candidates)
    assert created == 1
    assert len(new_signals) == 1
    assert isinstance(new_signals[0], Signal)
    assert new_signals[0].signal_type == "price_move"
    assert new_signals[0].rank_score > 0


@pytest.mark.asyncio
async def test_persist_signals_deduped_not_in_new_list(session):
    """When some candidates are deduped, only genuinely new signals appear in the returned list."""
    market = make_market(session)
    await session.flush()
    outcome1 = make_outcome(session, market.id, name="Yes")
    outcome2 = make_outcome(session, market.id, name="No")
    await session.flush()

    candidates = [
        SignalCandidate(
            signal_type="price_move",
            market_id=str(market.id),
            outcome_id=str(outcome1.id),
            signal_score=Decimal("0.600"),
            confidence=Decimal("0.800"),
            details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
            price_at_fire=Decimal("0.500"),
        ),
        SignalCandidate(
            signal_type="price_move",
            market_id=str(market.id),
            outcome_id=str(outcome2.id),
            signal_score=Decimal("0.400"),
            confidence=Decimal("0.700"),
            details={"direction": "down", "market_question": "Test?", "outcome_name": "No"},
            price_at_fire=Decimal("0.600"),
        ),
    ]

    # First call: both should be created
    created1, new1 = await persist_signals(session, candidates)
    assert created1 == 2
    assert len(new1) == 2

    # Second call: both should be deduped
    created2, new2 = await persist_signals(session, candidates)
    assert created2 == 0
    assert new2 == []


@pytest.mark.asyncio
async def test_broadcast_receives_correct_signal_objects(session):
    """SSE broadcaster receives the actual new Signal objects, not arbitrary sliced candidates."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    new_signal = Signal(
        id=uuid.uuid4(),
        signal_type="volume_spike",
        market_id=market.id,
        outcome_id=outcome.id,
        fired_at=datetime.now(timezone.utc),
        dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0),
        signal_score=Decimal("0.700"),
        confidence=Decimal("0.900"),
        rank_score=Decimal("0.630"),
        details={"direction": "up", "market_question": "Will BTC hit 100k?", "outcome_name": "Yes"},
        price_at_fire=Decimal("0.450"),
    )

    mock_broadcaster = AsyncMock()
    mock_broadcaster.subscriber_count = 1

    with patch("app.api.sse.broadcaster", mock_broadcaster):
        from app.jobs.scheduler import _broadcast_new_signals
        await _broadcast_new_signals(session, [new_signal])

    mock_broadcaster.publish.assert_called_once_with("new_signal", {
        "signal_type": "volume_spike",
        "market_question": "Will BTC hit 100k?",
        "rank_score": 0.630,
        "outcome_name": "Yes",
        "direction": "up",
    })


@pytest.mark.asyncio
async def test_broadcast_skipped_when_no_subscribers(session):
    """No publish calls when subscriber_count is 0."""
    mock_broadcaster = AsyncMock()
    mock_broadcaster.subscriber_count = 0

    with patch("app.api.sse.broadcaster", mock_broadcaster):
        from app.jobs.scheduler import _broadcast_new_signals
        await _broadcast_new_signals(session, [])

    mock_broadcaster.publish.assert_not_called()
