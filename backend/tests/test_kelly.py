"""Tests for Kelly Criterion position sizing."""
from decimal import Decimal

from app.signals.kelly import kelly_size


class TestKellySize:
    def test_basic_positive_edge(self):
        """Positive edge → buy YES with nonzero size"""
        result = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("10000"),
        )
        assert result["direction"] == "buy_yes"
        assert result["recommended_size_usd"] > Decimal("0")
        assert result["kelly_full"] > Decimal("0")
        assert result["edge_pct"] == Decimal("25.00")
        assert result["entry_price"] == Decimal("0.400000")

    def test_quarter_kelly_default(self):
        """Quarter-Kelly should be 1/4 of full Kelly"""
        result = kelly_size(
            estimated_prob=Decimal("0.60"),
            market_price=Decimal("0.50"),
            bankroll=Decimal("10000"),
        )
        assert result["kelly_used"] == (result["kelly_full"] * Decimal("0.25")).quantize(Decimal("0.0001"))

    def test_negative_edge_buys_no(self):
        """Negative edge for YES → buy NO"""
        result = kelly_size(
            estimated_prob=Decimal("0.30"),
            market_price=Decimal("0.60"),
            bankroll=Decimal("10000"),
        )
        assert result["direction"] == "buy_no"
        assert result["recommended_size_usd"] > Decimal("0")
        assert result["entry_price"] == Decimal("0.400000")  # 1 - 0.60

    def test_zero_edge_no_trade(self):
        """Zero edge → no trade"""
        result = kelly_size(
            estimated_prob=Decimal("0.50"),
            market_price=Decimal("0.50"),
            bankroll=Decimal("10000"),
        )
        assert result["direction"] == "none"
        assert result["recommended_size_usd"] == Decimal("0")

    def test_max_position_cap(self):
        """Large edge should be capped at max_position_pct"""
        result = kelly_size(
            estimated_prob=Decimal("0.90"),
            market_price=Decimal("0.20"),
            bankroll=Decimal("10000"),
            max_position_pct=Decimal("0.05"),
        )
        assert result["recommended_size_usd"] <= Decimal("500.00")  # 5% of 10K

    def test_custom_kelly_fraction(self):
        """Custom Kelly fraction applied"""
        half = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("10000"),
            kelly_fraction=Decimal("0.50"),
        )
        quarter = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("10000"),
            kelly_fraction=Decimal("0.25"),
        )
        # Half-Kelly should recommend roughly double quarter-Kelly (before cap)
        assert half["kelly_used"] > quarter["kelly_used"]

    def test_extreme_price_zero(self):
        """Market price at 0 → no division error"""
        result = kelly_size(
            estimated_prob=Decimal("0.50"),
            market_price=Decimal("0.00"),
            bankroll=Decimal("10000"),
        )
        assert result["recommended_size_usd"] == Decimal("0")

    def test_extreme_price_one(self):
        """Market price at 1 → no division error"""
        result = kelly_size(
            estimated_prob=Decimal("0.50"),
            market_price=Decimal("1.00"),
            bankroll=Decimal("10000"),
        )
        assert result["recommended_size_usd"] == Decimal("0")

    def test_shares_calculation(self):
        """Shares = size_usd / entry_price"""
        result = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("10000"),
        )
        if result["recommended_size_usd"] > 0 and result["entry_price"] > 0:
            expected_shares = (result["recommended_size_usd"] / result["entry_price"]).quantize(Decimal("0.0001"))
            assert result["shares"] == expected_shares

    def test_small_bankroll(self):
        """Small bankroll still produces proportional sizing"""
        small = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("100"),
        )
        large = kelly_size(
            estimated_prob=Decimal("0.65"),
            market_price=Decimal("0.40"),
            bankroll=Decimal("10000"),
        )
        # Same Kelly fraction, different absolute sizes
        assert small["kelly_full"] == large["kelly_full"]
        assert small["recommended_size_usd"] < large["recommended_size_usd"]
