"""Tests for ranking/scorer module."""
from datetime import datetime, timezone
from decimal import Decimal

from app.ranking.scorer import compute_rank_score, _dedupe_bucket


def test_rank_score_basic():
    score = compute_rank_score(Decimal("0.800"), Decimal("1.000"), age_hours=0)
    assert score == Decimal("0.800")


def test_rank_score_with_confidence():
    score = compute_rank_score(Decimal("0.800"), Decimal("0.500"), age_hours=0)
    assert score == Decimal("0.400")


def test_rank_score_zero_confidence():
    score = compute_rank_score(Decimal("0.800"), Decimal("0.000"), age_hours=0)
    assert score == Decimal("0.000")


def test_rank_score_24h_decay():
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=24)
    assert score == Decimal("0.300")


def test_rank_score_12h_decay():
    score = compute_rank_score(Decimal("1.000"), Decimal("1.000"), age_hours=12)
    assert score == Decimal("0.650")


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
