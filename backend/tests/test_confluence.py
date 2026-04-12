"""Tests for the Bayesian confluence engine: signal fusion, correlation discounts, modifiers."""
from decimal import Decimal

import pytest

from app.signals.base import SignalCandidate
from app.signals.confluence import (
    DEFAULT_CORRELATION,
    _adjustment_to_likelihood_ratio,
    _apply_correlation_discount,
    _get_correlation,
    _odds_to_probability,
    _probability_to_odds,
    fuse_signals,
)


def _make_candidate(
    signal_type: str = "price_move",
    outcome_id: str = "00000000-0000-0000-0000-000000000001",
    price_at_fire: Decimal = Decimal("0.50"),
    probability_adjustment: Decimal | None = Decimal("0.05"),
    is_directional: bool = True,
    signal_score: Decimal = Decimal("0.500"),
    confidence: Decimal = Decimal("0.800"),
    details: dict | None = None,
    timeframe: str = "30m",
) -> SignalCandidate:
    return SignalCandidate(
        signal_type=signal_type,
        market_id="00000000-0000-0000-0000-000000000099",
        outcome_id=outcome_id,
        signal_score=signal_score,
        confidence=confidence,
        price_at_fire=price_at_fire,
        details=details or {"market_question": "Test?", "outcome_name": "Yes"},
        timeframe=timeframe,
        estimated_probability=None,
        probability_adjustment=probability_adjustment,
        is_directional=is_directional,
    )


# ── Odds conversion ───────────────────────────────────────────


class TestOddsConversion:
    def test_probability_to_odds_50(self):
        odds = _probability_to_odds(Decimal("0.50"))
        assert odds == Decimal("1")

    def test_probability_to_odds_75(self):
        odds = _probability_to_odds(Decimal("0.75"))
        assert odds == Decimal("3")

    def test_round_trip(self):
        """probability → odds → probability should be identity"""
        for p_str in ["0.10", "0.25", "0.50", "0.75", "0.90"]:
            p = Decimal(p_str)
            assert abs(_odds_to_probability(_probability_to_odds(p)) - p) < Decimal("0.0001")


# ── Likelihood ratio ──────────────────────────────────────────


class TestLikelihoodRatio:
    def test_positive_adjustment(self):
        lr = _adjustment_to_likelihood_ratio(Decimal("0.50"), Decimal("0.10"))
        # Prior odds = 1.0, adjusted p=0.60 → odds = 1.5
        # LR = 1.5 / 1.0 = 1.5
        assert abs(lr - Decimal("1.5")) < Decimal("0.01")

    def test_negative_adjustment(self):
        lr = _adjustment_to_likelihood_ratio(Decimal("0.50"), Decimal("-0.10"))
        # adjusted p=0.40 → odds = 0.667
        # LR = 0.667 / 1.0 ≈ 0.667
        assert lr < Decimal("1")

    def test_zero_adjustment(self):
        lr = _adjustment_to_likelihood_ratio(Decimal("0.50"), Decimal("0"))
        assert abs(lr - Decimal("1")) < Decimal("0.001")


# ── Correlation discount ──────────────────────────────────────


class TestCorrelationDiscount:
    def test_zero_correlation(self):
        """No correlation → no discount"""
        lr = Decimal("2.0")
        result = _apply_correlation_discount(lr, Decimal("0"))
        assert result == lr

    def test_full_correlation(self):
        """Full correlation → LR becomes 1 (no new information)"""
        result = _apply_correlation_discount(Decimal("2.0"), Decimal("1.0"))
        assert result == Decimal("1")

    def test_partial_correlation(self):
        """Partial correlation → LR reduced but still > 1 for LR > 1"""
        lr = Decimal("2.0")
        result = _apply_correlation_discount(lr, Decimal("0.5"))
        assert Decimal("1") < result < lr

    def test_known_correlation_price_volume(self):
        corr = _get_correlation("price_move", "volume_spike")
        assert corr == Decimal("0.6")

    def test_unknown_pair_uses_default(self):
        corr = _get_correlation("price_move", "deadline_near")
        assert corr == DEFAULT_CORRELATION


# ── Fusion engine ─────────────────────────────────────────────


class TestFuseSignals:
    def test_returns_none_with_single_signal(self):
        """Need at least 2 directional signals for confluence"""
        signals = [_make_candidate()]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is None

    def test_returns_none_with_no_directional(self):
        """Non-directional signals alone can't produce confluence"""
        signals = [
            _make_candidate(signal_type="spread_change", is_directional=False, probability_adjustment=Decimal("0")),
            _make_candidate(signal_type="liquidity_vacuum", is_directional=False, probability_adjustment=Decimal("0")),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is None

    def test_returns_none_with_different_outcomes(self):
        """Signals must be for the same outcome"""
        signals = [
            _make_candidate(outcome_id="00000000-0000-0000-0000-000000000001"),
            _make_candidate(outcome_id="00000000-0000-0000-0000-000000000002"),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is None

    def test_basic_two_signal_fusion(self):
        """Two agreeing signals should shift probability further than either alone"""
        signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.05")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("0.05")),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is not None
        assert result.signal_type == "confluence"
        assert result.estimated_probability > Decimal("0.55")  # more than either alone
        assert result.probability_adjustment > Decimal("0.05")  # combined > individual

    def test_opposing_signals_cancel(self):
        """Opposing signals should partially cancel each other"""
        signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.08")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("-0.06")),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is not None
        # Net effect should be small
        assert abs(result.probability_adjustment) < Decimal("0.08")

    def test_correlation_discount_applied(self):
        """Highly correlated detectors should produce weaker fusion than uncorrelated"""
        # High correlation pair: price_move + volume_spike (0.6)
        correlated = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.06")),
            _make_candidate(signal_type="volume_spike", probability_adjustment=Decimal("0.06")),
        ]

        # Low correlation pair: price_move + order_flow (0.3)
        uncorrelated = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.06")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("0.06")),
        ]

        r_corr = fuse_signals(correlated, Decimal("0.50"))
        r_uncorr = fuse_signals(uncorrelated, Decimal("0.50"))

        assert r_corr is not None
        assert r_uncorr is not None
        # Uncorrelated pair should produce stronger combined signal
        assert r_uncorr.probability_adjustment > r_corr.probability_adjustment

    def test_confluence_details_structure(self):
        signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.05")),
            _make_candidate(signal_type="volume_spike", probability_adjustment=Decimal("0.04")),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        assert result is not None

        details = result.details
        assert "contributing_detectors" in details
        assert len(details["contributing_detectors"]) == 2
        assert details["detector_count"] == 2
        assert "posterior_probability" in details
        assert "market_price" in details

    def test_three_signal_fusion(self):
        """Three signals should produce even stronger combined estimate"""
        two_signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.05")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("0.05")),
        ]
        three_signals = two_signals + [
            _make_candidate(signal_type="smart_money", probability_adjustment=Decimal("0.05")),
        ]

        r2 = fuse_signals(two_signals, Decimal("0.50"))
        r3 = fuse_signals(three_signals, Decimal("0.50"))

        assert r2 is not None
        assert r3 is not None
        assert r3.estimated_probability > r2.estimated_probability

    def test_non_directional_modifiers_affect_confidence(self):
        """Non-directional modifiers should affect confidence, not probability"""
        base = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.05")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("0.05")),
        ]

        with_deadline = base + [
            _make_candidate(
                signal_type="deadline_near",
                is_directional=False,
                probability_adjustment=Decimal("0"),
                details={"urgency": "0.8", "market_question": "Test?", "outcome_name": "Yes"},
            ),
        ]

        r_base = fuse_signals(base, Decimal("0.50"))
        r_deadline = fuse_signals(with_deadline, Decimal("0.50"))

        assert r_base is not None
        assert r_deadline is not None
        # Probability should be the same (modifier doesn't shift probability)
        assert r_base.estimated_probability == r_deadline.estimated_probability
        # But confidence should be higher with deadline urgency
        assert r_deadline.confidence >= r_base.confidence

    def test_probability_clamped(self):
        """Extreme fusion should still be clamped to [0.01, 0.99]"""
        signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.30")),
            _make_candidate(signal_type="order_flow_imbalance", probability_adjustment=Decimal("0.30")),
            _make_candidate(signal_type="smart_money", probability_adjustment=Decimal("0.30")),
        ]
        result = fuse_signals(signals, Decimal("0.70"))
        assert result is not None
        assert result.estimated_probability <= Decimal("0.99")
        assert result.estimated_probability >= Decimal("0.01")

    def test_filters_zero_adjustment_directional(self):
        """Directional signals with zero adjustment are filtered out"""
        signals = [
            _make_candidate(signal_type="price_move", probability_adjustment=Decimal("0.05")),
            _make_candidate(signal_type="volume_spike", probability_adjustment=Decimal("0")),
        ]
        result = fuse_signals(signals, Decimal("0.50"))
        # Only 1 directional with nonzero adjustment → returns None
        assert result is None
