"""Tests for Expected Value calculator."""
from decimal import Decimal

from app.signals.ev import compute_ev, compute_ev_full


class TestComputeEV:
    def test_positive_ev_buy_yes(self):
        """Our estimate > market price → positive EV for YES"""
        ev = compute_ev(Decimal("0.65"), Decimal("0.40"))
        assert ev == Decimal("0.250000")

    def test_negative_ev(self):
        """Our estimate < market price → negative EV for YES"""
        ev = compute_ev(Decimal("0.40"), Decimal("0.65"))
        assert ev == Decimal("-0.250000")

    def test_zero_ev(self):
        """Our estimate = market price → zero EV"""
        ev = compute_ev(Decimal("0.50"), Decimal("0.50"))
        assert ev == Decimal("0.000000")

    def test_small_edge(self):
        """Small edge correctly computed"""
        ev = compute_ev(Decimal("0.53"), Decimal("0.50"))
        assert ev == Decimal("0.030000")


class TestComputeEVFull:
    def test_buy_yes_direction(self):
        """Positive edge → buy YES direction"""
        result = compute_ev_full(Decimal("0.65"), Decimal("0.40"))
        assert result["direction"] == "buy_yes"
        assert result["ev_per_share"] > Decimal("0")
        assert result["edge_pct"] == Decimal("25.00")
        assert result["entry_price"] == Decimal("0.400000")
        assert result["potential_profit"] == Decimal("0.600000")
        assert result["potential_loss"] == Decimal("0.400000")

    def test_buy_no_direction(self):
        """Negative edge → buy NO direction"""
        result = compute_ev_full(Decimal("0.35"), Decimal("0.60"))
        assert result["direction"] == "buy_no"
        assert result["ev_per_share"] > Decimal("0")
        assert result["entry_price"] == Decimal("0.400000")  # 1 - 0.60

    def test_ev_consistency(self):
        """Full EV should equal simple EV for YES direction"""
        prob = Decimal("0.65")
        price = Decimal("0.40")
        simple = compute_ev(prob, price)
        full = compute_ev_full(prob, price)
        # For YES: ev = p*(1-price) - (1-p)*price = p - price
        assert full["ev_per_share"] == simple

    def test_edge_pct_calculation(self):
        """Edge percentage is abs(prob - price) * 100"""
        result = compute_ev_full(Decimal("0.58"), Decimal("0.50"))
        assert result["edge_pct"] == Decimal("8.00")
