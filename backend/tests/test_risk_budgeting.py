from decimal import Decimal

from app.risk.budgets import normalize_risk_budget_policy
from app.risk.regime import REGIME_HALTED, REGIME_THIN_LIQUIDITY, classify_regime
from app.risk.risk_of_ruin import calculate_risk_of_ruin


def test_normalize_risk_budget_policy_coerces_values_and_fills_defaults():
    normalized = normalize_risk_budget_policy(
        {
            "capital": {"outstanding_notional_usd": "125.5"},
            "capacity": {"max_open_orders": "3", "max_order_notional_usd": "25"},
            "breach_actions": {"live": "block"},
        }
    )

    assert normalized["capital"]["outstanding_notional_usd"] == Decimal("125.5")
    assert normalized["capital"]["daily_notional_usd"] == Decimal("0")
    assert normalized["capacity"]["max_order_notional_usd"] == Decimal("25")
    assert normalized["capacity"]["capacity_ceiling_usd"] == Decimal("0")
    assert normalized["capacity"]["max_open_orders"] == 3
    assert normalized["regime_multipliers"]["thin_liquidity"] == Decimal("0.75")
    assert normalized["risk_of_ruin"]["critical_multiplier"] == Decimal("0")
    assert normalized["breach_actions"]["live"] == "block"
    assert normalized["breach_actions"]["paper"] == "reduce_size"


def test_classify_regime_is_fail_closed_and_halts_on_serious_pressure():
    unknown_depth = classify_regime(book_fresh=True, fillable_fraction=None)
    halted = classify_regime(
        book_fresh=True,
        fillable_fraction=Decimal("1.0"),
        serious_guardrail_count_24h=1,
    )

    assert unknown_depth["label"] == REGIME_THIN_LIQUIDITY
    assert unknown_depth["multiplier"] == Decimal("0.75")
    assert halted["label"] == REGIME_HALTED
    assert halted["multiplier"] == Decimal("0")
    assert "serious_guardrail_pressure" in halted["reasons"]


def test_calculate_risk_of_ruin_applies_warning_and_critical_multipliers():
    warning = calculate_risk_of_ruin(
        loss_utilization=Decimal("0.60"),
        open_exposure_utilization=Decimal("0.60"),
        concentration_utilization=Decimal("0.20"),
    )
    critical = calculate_risk_of_ruin(
        loss_utilization=Decimal("1.0"),
        open_exposure_utilization=Decimal("1.0"),
        concentration_utilization=Decimal("1.0"),
    )

    assert warning["label"] == "warning"
    assert warning["multiplier"] == Decimal("0.75")
    assert warning["score"] == Decimal("0.5200")
    assert critical["label"] == "critical"
    assert critical["multiplier"] == Decimal("0")
    assert critical["score"] == Decimal("1.0000")
