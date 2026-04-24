"""Tests for the probability engine: sensitivity curve, Brier score, calibration, detector integration."""
from decimal import Decimal

from app.signals.probability import (
    PROB_MAX,
    PROB_MIN,
    brier_score,
    calibration_buckets,
    clamp_probability,
    compute_estimated_probability,
    prior_sensitivity,
)

# ── Prior sensitivity curve ────────────────────────────────────


class TestPriorSensitivity:
    def test_peak_at_50_percent(self):
        """Sensitivity peaks at p=0.50 → multiplier = 1.0"""
        s = prior_sensitivity(Decimal("0.50"))
        assert s == Decimal("1.0000")

    def test_symmetric_at_30_and_70(self):
        """Sensitivity is symmetric around 0.50"""
        s30 = prior_sensitivity(Decimal("0.30"))
        s70 = prior_sensitivity(Decimal("0.70"))
        assert s30 == s70

    def test_dampened_at_extremes(self):
        """Signals at price extremes are dampened"""
        s95 = prior_sensitivity(Decimal("0.95"))
        s05 = prior_sensitivity(Decimal("0.05"))
        assert s95 == s05
        assert s95 == Decimal("0.1900")

    def test_moderate_at_80(self):
        s80 = prior_sensitivity(Decimal("0.80"))
        assert s80 == Decimal("0.6400")

    def test_zero_at_boundaries(self):
        s0 = prior_sensitivity(Decimal("0"))
        s1 = prior_sensitivity(Decimal("1"))
        assert s0 == Decimal("0.0000")
        assert s1 == Decimal("0.0000")

    def test_sensitivity_ordering(self):
        """Closer to 0.5 = higher sensitivity"""
        s50 = prior_sensitivity(Decimal("0.50"))
        s30 = prior_sensitivity(Decimal("0.30"))
        s10 = prior_sensitivity(Decimal("0.10"))
        assert s50 > s30 > s10


# ── Clamp probability ──────────────────────────────────────────


class TestClampProbability:
    def test_within_bounds_unchanged(self):
        assert clamp_probability(Decimal("0.50")) == Decimal("0.50")
        assert clamp_probability(Decimal("0.01")) == Decimal("0.01")
        assert clamp_probability(Decimal("0.99")) == Decimal("0.99")

    def test_below_minimum(self):
        assert clamp_probability(Decimal("0.00")) == PROB_MIN
        assert clamp_probability(Decimal("-0.10")) == PROB_MIN

    def test_above_maximum(self):
        assert clamp_probability(Decimal("1.00")) == PROB_MAX
        assert clamp_probability(Decimal("1.50")) == PROB_MAX


# ── Compute estimated probability ──────────────────────────────


class TestComputeEstimatedProbability:
    def test_positive_adjustment_at_50(self):
        """At p=0.50, full sensitivity → adjustment applied at 1.0x"""
        est, adj = compute_estimated_probability(Decimal("0.50"), Decimal("0.10"))
        assert est == Decimal("0.6000")
        assert adj == Decimal("0.1000")

    def test_positive_adjustment_at_90(self):
        """At p=0.90, sensitivity = 0.36 → adjustment dampened"""
        est, adj = compute_estimated_probability(Decimal("0.90"), Decimal("0.10"))
        # raw_adj * sensitivity = 0.10 * 0.36 = 0.036
        assert est == Decimal("0.9360")
        assert adj == Decimal("0.0360")

    def test_negative_adjustment(self):
        """Negative adjustment shifts probability down"""
        est, adj = compute_estimated_probability(Decimal("0.50"), Decimal("-0.10"))
        assert est == Decimal("0.4000")
        assert adj == Decimal("-0.1000")

    def test_clamping_high(self):
        """Large positive adjustment clamped at 0.99"""
        est, adj = compute_estimated_probability(Decimal("0.95"), Decimal("0.50"))
        assert est == PROB_MAX

    def test_clamping_low(self):
        """Large negative adjustment clamped at 0.01"""
        est, adj = compute_estimated_probability(Decimal("0.05"), Decimal("-0.50"))
        assert est == PROB_MIN

    def test_zero_adjustment(self):
        """Zero adjustment returns market price as estimate"""
        est, adj = compute_estimated_probability(Decimal("0.60"), Decimal("0"))
        assert est == Decimal("0.6000")
        assert adj == Decimal("0.0000")


# ── Brier score ────────────────────────────────────────────────


class TestBrierScore:
    def test_perfect_predictions(self):
        """Perfect predictions → Brier score = 0.0"""
        predictions = [
            (Decimal("1.0"), True),
            (Decimal("0.0"), False),
        ]
        bs = brier_score(predictions)
        assert bs == Decimal("0.000000")

    def test_worst_predictions(self):
        """Maximally wrong → Brier score = 1.0"""
        predictions = [
            (Decimal("0.0"), True),
            (Decimal("1.0"), False),
        ]
        bs = brier_score(predictions)
        assert bs == Decimal("1.000000")

    def test_coin_flip(self):
        """50/50 predictions → Brier = 0.25"""
        predictions = [
            (Decimal("0.5"), True),
            (Decimal("0.5"), False),
        ]
        bs = brier_score(predictions)
        assert bs == Decimal("0.250000")

    def test_empty_returns_none(self):
        assert brier_score([]) is None

    def test_single_prediction(self):
        predictions = [(Decimal("0.7"), True)]
        bs = brier_score(predictions)
        # (0.7 - 1)^2 = 0.09
        assert bs == Decimal("0.090000")

    def test_mixed_predictions(self):
        """Partially calibrated predictions"""
        predictions = [
            (Decimal("0.8"), True),   # (0.8-1)^2 = 0.04
            (Decimal("0.3"), False),  # (0.3-0)^2 = 0.09
            (Decimal("0.6"), True),   # (0.6-1)^2 = 0.16
        ]
        bs = brier_score(predictions)
        # mean(0.04, 0.09, 0.16) = 0.29 / 3 ≈ 0.096667
        assert abs(bs - Decimal("0.096667")) < Decimal("0.001")


# ── Calibration buckets ───────────────────────────────────────


class TestCalibrationBuckets:
    def test_empty_returns_empty(self):
        assert calibration_buckets([]) == []

    def test_single_bucket(self):
        predictions = [
            (Decimal("0.55"), True),
            (Decimal("0.52"), False),
            (Decimal("0.58"), True),
        ]
        result = calibration_buckets(predictions, n_bins=10)
        # All fall in the 0.50-0.60 bin (index 5)
        assert len(result) == 1
        bucket = result[0]
        assert bucket["bin_center"] == 0.55
        assert bucket["sample_size"] == 3
        assert bucket["correct"] == 2
        assert abs(bucket["actual_rate"] - 0.6667) < 0.01

    def test_multiple_buckets(self):
        predictions = [
            (Decimal("0.20"), False),
            (Decimal("0.80"), True),
        ]
        result = calibration_buckets(predictions, n_bins=10)
        assert len(result) == 2

    def test_bin_count(self):
        """Test with 5 bins instead of default 10"""
        predictions = [
            (Decimal("0.10"), False),
            (Decimal("0.30"), True),
            (Decimal("0.50"), True),
            (Decimal("0.70"), True),
            (Decimal("0.90"), True),
        ]
        result = calibration_buckets(predictions, n_bins=5)
        assert len(result) == 5


# ── SignalCandidate probability fields ──────────────────────────


class TestSignalCandidateFields:
    def test_default_none(self):
        """New fields default to None (backward compatible)"""
        from app.signals.base import SignalCandidate
        c = SignalCandidate(
            signal_type="test",
            market_id="00000000-0000-0000-0000-000000000001",
            outcome_id="00000000-0000-0000-0000-000000000002",
            signal_score=Decimal("0.5"),
            confidence=Decimal("0.8"),
            price_at_fire=Decimal("0.5"),
            details={},
        )
        assert c.estimated_probability is None
        assert c.probability_adjustment is None
        assert c.is_directional is True

    def test_with_probability(self):
        from app.signals.base import SignalCandidate
        c = SignalCandidate(
            signal_type="test",
            market_id="00000000-0000-0000-0000-000000000001",
            outcome_id="00000000-0000-0000-0000-000000000002",
            signal_score=Decimal("0.5"),
            confidence=Decimal("0.8"),
            price_at_fire=Decimal("0.5"),
            details={},
            estimated_probability=Decimal("0.6000"),
            probability_adjustment=Decimal("0.1000"),
            is_directional=True,
        )
        assert c.estimated_probability == Decimal("0.6000")
        assert c.probability_adjustment == Decimal("0.1000")

    def test_non_directional_modifier(self):
        from app.signals.base import SignalCandidate
        c = SignalCandidate(
            signal_type="spread_change",
            market_id="00000000-0000-0000-0000-000000000001",
            outcome_id="00000000-0000-0000-0000-000000000002",
            signal_score=Decimal("0.5"),
            confidence=Decimal("0.8"),
            price_at_fire=Decimal("0.5"),
            details={},
            probability_adjustment=Decimal("0"),
            is_directional=False,
        )
        assert c.is_directional is False
        assert c.probability_adjustment == Decimal("0")
