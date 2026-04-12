"""Tests for the risk management module."""
from decimal import Decimal

from app.signals.risk import _extract_keywords, check_exposure, compute_keyword_overlap


class TestKeywordExtraction:
    def test_basic_extraction(self):
        keywords = _extract_keywords("Will the Fed raise interest rates in 2026?")
        assert "fed" in keywords
        assert "raise" in keywords
        assert "interest" in keywords
        assert "rates" in keywords
        # Stop words filtered
        assert "will" not in keywords
        assert "the" not in keywords
        assert "in" not in keywords

    def test_empty_string(self):
        assert _extract_keywords("") == set()


class TestKeywordOverlap:
    def test_identical_questions(self):
        q = "Will the Fed raise interest rates?"
        similarity = compute_keyword_overlap(q, q)
        assert similarity == 1.0

    def test_no_overlap(self):
        similarity = compute_keyword_overlap(
            "Will Bitcoin reach 100K?",
            "Who wins the Super Bowl?",
        )
        assert similarity < 0.2

    def test_partial_overlap(self):
        similarity = compute_keyword_overlap(
            "Will the Fed raise interest rates in June?",
            "Will the Fed cut interest rates in December?",
        )
        assert similarity > 0.3


class TestCheckExposure:
    def test_approved_within_limits(self):
        result = check_exposure(
            open_positions=[],
            new_trade={"size_usd": 500, "market_question": "Test?", "outcome_id": "1"},
            bankroll=Decimal("10000"),
        )
        assert result["approved"] is True
        assert result["approved_size_usd"] == Decimal("500")

    def test_total_exposure_cap(self):
        """Reject when total exposure would exceed 30%"""
        positions = [
            {"size_usd": 2500, "market_question": "Market A?", "outcome_id": "1"},
        ]
        result = check_exposure(
            open_positions=positions,
            new_trade={"size_usd": 1000, "market_question": "Market B?", "outcome_id": "2"},
            bankroll=Decimal("10000"),
            max_total_pct=Decimal("0.30"),
        )
        assert result["approved"] is True
        # Should be reduced to fit within 30% cap (3000 - 2500 = 500 remaining)
        assert result["approved_size_usd"] == Decimal("500.00")

    def test_total_exposure_full(self):
        """Reject when already at limit"""
        positions = [
            {"size_usd": 3000, "market_question": "Market A?", "outcome_id": "1"},
        ]
        result = check_exposure(
            open_positions=positions,
            new_trade={"size_usd": 100, "market_question": "Market B?", "outcome_id": "2"},
            bankroll=Decimal("10000"),
            max_total_pct=Decimal("0.30"),
        )
        assert result["approved"] is False

    def test_cluster_exposure_cap(self):
        """Reduce size when correlated markets near cluster limit"""
        positions = [
            {"size_usd": 1200, "market_question": "Will the Fed raise interest rates in June?", "outcome_id": "1"},
        ]
        result = check_exposure(
            open_positions=positions,
            new_trade={
                "size_usd": 500,
                "market_question": "Will the Fed raise interest rates in December?",
                "outcome_id": "2",
            },
            bankroll=Decimal("10000"),
            max_cluster_pct=Decimal("0.15"),
        )
        assert result["approved"] is True
        # Should be reduced to fit within cluster cap
        assert result["approved_size_usd"] <= Decimal("300.00")

    def test_drawdown_circuit_breaker(self):
        """Halve sizes when drawdown exceeds threshold"""
        result = check_exposure(
            open_positions=[],
            new_trade={"size_usd": 500, "market_question": "Test?", "outcome_id": "1"},
            bankroll=Decimal("10000"),
            peak_bankroll=Decimal("12000"),
            cumulative_pnl=Decimal("-2000"),
            drawdown_breaker_pct=Decimal("0.15"),
        )
        assert result["approved"] is True
        assert result["drawdown_active"] is True
        assert result["approved_size_usd"] == Decimal("250.00")  # halved

    def test_zero_size_rejected(self):
        result = check_exposure(
            open_positions=[],
            new_trade={"size_usd": 0, "market_question": "Test?", "outcome_id": "1"},
            bankroll=Decimal("10000"),
        )
        assert result["approved"] is False

    def test_no_drawdown_when_no_peak(self):
        """No peak bankroll → no drawdown check"""
        result = check_exposure(
            open_positions=[],
            new_trade={"size_usd": 500, "market_question": "Test?", "outcome_id": "1"},
            bankroll=Decimal("10000"),
            peak_bankroll=None,
        )
        assert result["drawdown_active"] is False
