"""Tests for ranking/scorer module: rank score computation, recency decay, deduplication."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.ranking.scorer import _dedupe_bucket, compute_rank_score, persist_signals
from app.signals.base import SignalCandidate
from tests.conftest import make_market, make_outcome

# ── Rank score formula ──────────────────────────────────


def test_rank_score_basic():
    score = compute_rank_score(Decimal("0.800"), Decimal("1.000"), age_hours=0)
    assert score == Decimal("0.800")


def test_rank_score_with_confidence():
    score = compute_rank_score(Decimal("0.800"), Decimal("0.500"), age_hours=0)
    assert score == Decimal("0.400")


def test_rank_score_zero_confidence():
    score = compute_rank_score(Decimal("0.800"), Decimal("0.000"), age_hours=0)
    assert score == Decimal("0.000")


def test_recency_weight_1_at_zero_hours():
    """Recency weight = 1.0 for brand-new signal."""
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=0)
    assert score == Decimal("1.000")


def test_rank_score_24h_decay():
    """Recency weight decays to 0.3 at 24h."""
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=24)
    assert score == Decimal("0.300")


def test_rank_score_12h_decay():
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=12)
    assert score == Decimal("0.650")


def test_rank_score_beyond_24h_clamped():
    """Beyond 24h, recency weight floors at 0.3 (not negative)."""
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=48)
    assert score == Decimal("0.300")


def test_rank_score_formula():
    """rank_score = signal_score × confidence × recency_weight."""
    # At 6h: recency = 1.0 - 6 * 0.7/24 = 1.0 - 0.175 = 0.825
    score = compute_rank_score(Decimal("0.600"), Decimal("0.800"), age_hours=6)
    expected = (Decimal("0.600") * Decimal("0.800") * Decimal("0.825")).quantize(Decimal("0.001"))
    assert score == expected


# ── Dedupe bucket ───────────────────────────────────────


def test_dedupe_bucket_on_boundary():
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert _dedupe_bucket(dt) == dt


def test_dedupe_bucket_rounds_down():
    dt = datetime(2024, 1, 1, 12, 7, 33, tzinfo=timezone.utc)
    expected = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert _dedupe_bucket(dt) == expected


def test_dedupe_bucket_15min():
    dt = datetime(2024, 1, 1, 12, 22, 0, tzinfo=timezone.utc)
    expected = datetime(2024, 1, 1, 12, 15, 0, tzinfo=timezone.utc)
    assert _dedupe_bucket(dt) == expected


def test_dedupe_bucket_45min():
    dt = datetime(2024, 1, 1, 12, 59, 59, tzinfo=timezone.utc)
    expected = datetime(2024, 1, 1, 12, 45, 0, tzinfo=timezone.utc)
    assert _dedupe_bucket(dt) == expected


# ── Deduplication in persist_signals ────────────────────


def _make_candidate(market_id, outcome_id, signal_type="price_move"):
    return SignalCandidate(
        signal_type=signal_type,
        market_id=str(market_id),
        outcome_id=str(outcome_id),
        signal_score=Decimal("0.600"),
        confidence=Decimal("0.800"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        price_at_fire=Decimal("0.500"),
    )


@pytest.mark.asyncio
async def test_duplicate_same_bucket_returns_existing(session):
    """Same type, same outcome, same 15-min bucket → second call produces no new signal."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    candidate = _make_candidate(market.id, outcome.id)

    count1, signals1 = await persist_signals(session, [candidate])
    assert count1 == 1

    count2, signals2 = await persist_signals(session, [candidate])
    assert count2 == 0
    assert signals2 == []


@pytest.mark.asyncio
async def test_different_outcome_same_bucket_both_inserted(session):
    """Different outcome, same type, same bucket → both inserted."""
    market = make_market(session)
    await session.flush()
    outcome1 = make_outcome(session, market.id, name="Yes")
    outcome2 = make_outcome(session, market.id, name="No")
    await session.flush()

    c1 = _make_candidate(market.id, outcome1.id)
    c2 = _make_candidate(market.id, outcome2.id)

    count, signals = await persist_signals(session, [c1, c2])
    assert count == 2
    assert len(signals) == 2


@pytest.mark.asyncio
async def test_same_outcome_different_type_both_inserted(session):
    """Same outcome, different type, same bucket → both inserted."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    c1 = _make_candidate(market.id, outcome.id, signal_type="price_move")
    c2 = _make_candidate(market.id, outcome.id, signal_type="volume_spike")

    count, signals = await persist_signals(session, [c1, c2])
    assert count == 2
    types = {s.signal_type for s in signals}
    assert types == {"price_move", "volume_spike"}
