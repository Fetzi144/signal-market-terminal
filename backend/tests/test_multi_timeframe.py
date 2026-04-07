"""Tests for multi-timeframe analysis and confluence scoring."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


# ── Detector multi-timeframe tests ──────────────────────────────────


@pytest.mark.asyncio
async def test_price_move_different_timeframes_produce_separate_signals(session):
    """PriceMoveDetector with 2 timeframes should produce separate signals per TF."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Create snapshots over 4 hours with a big move
    # Earliest snapshot for 4h window
    make_price_snapshot(session, o.id, price=0.30, captured_at=now - timedelta(hours=4))
    make_price_snapshot(session, o.id, price=0.30, captured_at=now - timedelta(hours=1))
    # Snapshot inside the 30m window showing old price
    make_price_snapshot(session, o.id, price=0.30, captured_at=now - timedelta(minutes=25))
    # Recent snapshot with big price move (visible to both 30m and 4h windows)
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.price_move import PriceMoveDetector

    detector = PriceMoveDetector(timeframes=["30m", "4h"], threshold_pct=5.0)
    candidates = await detector.detect(session)

    # Should get signal for each timeframe
    timeframes = {c.timeframe for c in candidates}
    assert "30m" in timeframes
    assert "4h" in timeframes
    # Each candidate should have distinct timeframe
    assert all(c.signal_type == "price_move" for c in candidates)
    for c in candidates:
        assert c.details["timeframe"] == c.timeframe


@pytest.mark.asyncio
async def test_single_timeframe_default_behavior(session):
    """With default config, detector should produce signals with '30m' timeframe."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o.id, price=0.30, captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.price_move import PriceMoveDetector

    with patch("app.signals.price_move.settings") as mock_settings:
        mock_settings.price_move_timeframes = "30m"
        mock_settings.price_move_threshold_pct = 5.0
        mock_settings.price_move_window_minutes = 30
        detector = PriceMoveDetector()

    candidates = await detector.detect(session)
    assert len(candidates) >= 1
    assert all(c.timeframe == "30m" for c in candidates)


# ── Dedupe with timeframe tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_dedupe_allows_same_type_different_timeframes(session):
    """Signals with same type+outcome but different timeframes should NOT be deduped."""
    from app.ranking.scorer import persist_signals
    from app.signals.base import SignalCandidate

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    candidates = [
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.700"),
            confidence=Decimal("0.800"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="30m",
        ),
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.600"),
            confidence=Decimal("0.800"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="4h",
        ),
    ]

    count, signals = await persist_signals(session, candidates)
    assert count == 2
    timeframes = {s.timeframe for s in signals}
    assert timeframes == {"30m", "4h"}


@pytest.mark.asyncio
async def test_dedupe_blocks_same_type_same_timeframe(session):
    """Signals with same type+outcome+timeframe in same bucket should be deduped."""
    from app.ranking.scorer import persist_signals
    from app.signals.base import SignalCandidate

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    candidate = SignalCandidate(
        signal_type="price_move",
        market_id=str(m.id),
        outcome_id=str(o.id),
        signal_score=Decimal("0.700"),
        confidence=Decimal("0.800"),
        price_at_fire=Decimal("0.50"),
        details={"direction": "up"},
        timeframe="30m",
    )

    count1, _ = await persist_signals(session, [candidate])
    count2, _ = await persist_signals(session, [candidate])
    assert count1 == 1
    assert count2 == 0  # deduped


# ── Confluence scoring tests ────────────────────────────────────────


@pytest.mark.asyncio
async def test_confluence_across_2_timeframes_applies_bonus(session):
    """When same signal fires on 2 TFs, confluence bonus of 0.15 should be applied."""
    from app.ranking.scorer import persist_signals
    from app.signals.base import SignalCandidate

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    candidates = [
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.500"),
            confidence=Decimal("0.500"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="30m",
        ),
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.500"),
            confidence=Decimal("0.500"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="4h",
        ),
    ]

    outcome_id = o.id  # capture before expire
    count, signals = await persist_signals(session, candidates)
    assert count == 2

    # Re-query to get updated values (expire_all + sync attr access breaks async SQLAlchemy)
    from sqlalchemy import select
    from app.models.signal import Signal
    result = await session.execute(
        select(Signal).where(
            Signal.signal_type == "price_move",
            Signal.outcome_id == outcome_id,
        )
    )
    db_signals = result.scalars().all()
    assert len(db_signals) == 2

    for sig in db_signals:
        assert sig.details.get("confluence_timeframes") is not None
        assert len(sig.details["confluence_timeframes"]) == 2
        assert "30m" in sig.details["confluence_timeframes"]
        assert "4h" in sig.details["confluence_timeframes"]
        # Base rank = 0.5 * 0.5 * 1.0 = 0.250, + confluence 0.15 = 0.400
        assert sig.rank_score == Decimal("0.400")


@pytest.mark.asyncio
async def test_confluence_across_3_timeframes_capped_at_1(session):
    """Confluence across 3 TFs gives 0.30 bonus, total capped at 1.0."""
    from app.ranking.scorer import persist_signals
    from app.signals.base import SignalCandidate

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    candidates = [
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.900"),
            confidence=Decimal("0.900"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="30m",
        ),
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.900"),
            confidence=Decimal("0.900"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="1h",
        ),
        SignalCandidate(
            signal_type="price_move",
            market_id=str(m.id),
            outcome_id=str(o.id),
            signal_score=Decimal("0.900"),
            confidence=Decimal("0.900"),
            price_at_fire=Decimal("0.50"),
            details={"direction": "up"},
            timeframe="4h",
        ),
    ]

    outcome_id = o.id  # capture before expire
    count, signals = await persist_signals(session, candidates)
    assert count == 3

    from sqlalchemy import select
    from app.models.signal import Signal
    result = await session.execute(
        select(Signal).where(
            Signal.signal_type == "price_move",
            Signal.outcome_id == outcome_id,
        )
    )
    db_signals = result.scalars().all()

    for sig in db_signals:
        assert len(sig.details["confluence_timeframes"]) == 3
        # Base rank = 0.9 * 0.9 * 1.0 = 0.810, + 0.30 = 1.110 -> capped at 1.0
        assert sig.rank_score == Decimal("1.000")


# ── API timeframe filter test ───────────────────────────────────────


@pytest.mark.asyncio
async def test_signals_api_timeframe_filter(client, session):
    """GET /api/v1/signals?timeframe=4h should only return 4h signals."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    make_signal(session, m.id, o.id, timeframe="30m")
    make_signal(session, m.id, o.id, timeframe="4h",
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    await session.commit()

    resp = await client.get("/api/v1/signals", params={"timeframe": "4h"})
    assert resp.status_code == 200
    data = resp.json()
    assert all(s["timeframe"] == "4h" for s in data["signals"])


# ── Timeframe utility tests ────────────────────────────────────────


def test_timeframe_to_minutes():
    """timeframe_to_minutes should correctly convert all supported values."""
    from app.signals.base import timeframe_to_minutes

    assert timeframe_to_minutes("5m") == 5
    assert timeframe_to_minutes("15m") == 15
    assert timeframe_to_minutes("30m") == 30
    assert timeframe_to_minutes("1h") == 60
    assert timeframe_to_minutes("4h") == 240
    assert timeframe_to_minutes("24h") == 1440

    with pytest.raises(ValueError):
        timeframe_to_minutes("invalid")
