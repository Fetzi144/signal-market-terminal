"""Tests for Day 4: Performance Dashboard API."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.models.signal import Signal
from tests.conftest import make_market, make_outcome

# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _make_signal(session, market_id, outcome_id=None, **kwargs):
    defaults = dict(
        id=uuid.uuid4(),
        signal_type="price_move",
        market_id=market_id,
        outcome_id=outcome_id,
        fired_at=datetime.now(timezone.utc),
        signal_score=Decimal("0.8"),
        confidence=Decimal("0.75"),
        rank_score=Decimal("0.7"),
        details={},
        resolved=False,
        resolved_correctly=None,
    )
    defaults.update(kwargs)
    s = Signal(**defaults)
    session.add(s)
    return s


# --------------------------------------------------------------------------- #
# Tests                                                                         #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_summary_empty(client):
    """No resolved signals → graceful empty response."""
    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["overall_win_rate"] is None
    assert data["total_resolved"] == 0
    assert data["win_rate_by_type"] == []
    assert data["win_rate_trend"] == []
    assert data["best_detector"] is None
    assert data["worst_detector"] is None


@pytest.mark.asyncio
async def test_win_rate_calculation(session, client):
    """Win rate = correct / resolved (mixed resolved/unresolved signals)."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    # 6 resolved: 4 correct, 2 wrong
    for i in range(4):
        _make_signal(session, market.id, outcome.id,
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.8"))
    for i in range(2):
        _make_signal(session, market.id, outcome.id,
                     resolved=True, resolved_correctly=False,
                     rank_score=Decimal("0.4"))
    # 3 unresolved (should not count)
    for i in range(3):
        _make_signal(session, market.id, outcome.id,
                     resolved=False, resolved_correctly=None,
                     rank_score=Decimal("0.6"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()

    assert data["total_resolved"] == 6
    assert abs(data["overall_win_rate"] - 4 / 6) < 0.001
    assert data["signals_pending_resolution"] == 3


@pytest.mark.asyncio
async def test_trend_returns_data(session, client):
    """Trend data has one entry per day with resolved signals."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Signals on 3 different days
    for day_offset in [1, 5, 10]:
        _make_signal(session, market.id, outcome.id,
                     fired_at=now - timedelta(days=day_offset),
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.7"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    trend = resp.json()["win_rate_trend"]
    assert len(trend) == 3
    for entry in trend:
        assert "date" in entry
        assert entry["win_rate"] == 1.0  # all correct


@pytest.mark.asyncio
async def test_best_worst_detector(session, client):
    """Best/worst detector derived from win rates (min 10 resolved)."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    # "volume_spike": 10 correct out of 10 → 100% win rate
    for _ in range(10):
        _make_signal(session, market.id, outcome.id,
                     signal_type="volume_spike",
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.9"))

    # "price_move": 3 correct out of 10 → 30% win rate
    for _ in range(3):
        _make_signal(session, market.id, outcome.id,
                     signal_type="price_move",
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.6"))
    for _ in range(7):
        _make_signal(session, market.id, outcome.id,
                     signal_type="price_move",
                     resolved=True, resolved_correctly=False,
                     rank_score=Decimal("0.4"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["best_detector"] == "volume_spike"
    assert data["worst_detector"] == "price_move"


@pytest.mark.asyncio
async def test_best_worst_requires_min_resolved(session, client):
    """Detectors with < 10 resolved signals are excluded from best/worst."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    # Only 5 resolved — below the threshold
    for _ in range(5):
        _make_signal(session, market.id, outcome.id,
                     signal_type="spread_change",
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.9"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()
    # Not enough resolved — should be excluded
    assert data["best_detector"] is None
    assert data["worst_detector"] is None


@pytest.mark.asyncio
async def test_avg_rank_winners_vs_losers(session, client):
    """Average rank of winners should be higher than losers."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    for _ in range(5):
        _make_signal(session, market.id, outcome.id,
                     resolved=True, resolved_correctly=True,
                     rank_score=Decimal("0.9"))
    for _ in range(5):
        _make_signal(session, market.id, outcome.id,
                     resolved=True, resolved_correctly=False,
                     rank_score=Decimal("0.3"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["avg_rank_of_winners"] is not None
    assert data["avg_rank_of_losers"] is not None
    assert data["avg_rank_of_winners"] > data["avg_rank_of_losers"]


@pytest.mark.asyncio
async def test_win_rate_by_type_breakdown(session, client):
    """win_rate_by_type contains correct per-type win rates."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    await session.flush()

    # volume_spike: 8/10
    for i in range(10):
        _make_signal(session, market.id, outcome.id,
                     signal_type="volume_spike",
                     resolved=True, resolved_correctly=(i < 8),
                     rank_score=Decimal("0.7"))

    # spread_change: 2/4
    for i in range(4):
        _make_signal(session, market.id, outcome.id,
                     signal_type="spread_change",
                     resolved=True, resolved_correctly=(i < 2),
                     rank_score=Decimal("0.6"))

    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    by_type = {r["signal_type"]: r for r in resp.json()["win_rate_by_type"]}

    assert "volume_spike" in by_type
    assert abs(by_type["volume_spike"]["win_rate"] - 0.8) < 0.001
    assert by_type["volume_spike"]["resolved"] == 10

    assert "spread_change" in by_type
    assert abs(by_type["spread_change"]["win_rate"] - 0.5) < 0.001
    assert by_type["spread_change"]["resolved"] == 4
