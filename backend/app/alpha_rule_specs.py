from __future__ import annotations

from copy import deepcopy
from typing import Any

ALPHA_KALSHI_4237F81367_FAMILY = "alpha_kalshi_price_move_up_liq_4237f81367"
ALPHA_KALSHI_4237F81367_VERSION = (
    "alpha_kalshi_type_price_move_platform_kalshi_direction_up_pri_4237f81367_v1"
)
ALPHA_KALSHI_4237F81367_REASON_PREFIX = "alpha_rule_4237f81367"

ALPHA_KALSHI_D80BDF77A9_FAMILY = "alpha_kalshi_price_move_up_vol_d80bdf77a9"
ALPHA_KALSHI_D80BDF77A9_VERSION = (
    "alpha_kalshi_type_price_move_platform_kalshi_direction_up_ev_d80bdf77a9_v1"
)
ALPHA_KALSHI_D80BDF77A9_REASON_PREFIX = "alpha_rule_d80bdf77a9"

ALPHA_KALSHI_4237F81367_V1: dict[str, Any] = {
    "implementation_status": "paper_active",
    "candidate_id": "kalshi_alpha_4237f81367",
    "strategy_family": ALPHA_KALSHI_4237F81367_FAMILY,
    "strategy_name": ALPHA_KALSHI_4237F81367_VERSION,
    "strategy_version": ALPHA_KALSHI_4237F81367_VERSION,
    "version_label": "Alpha Kalshi Price Move Up Liquidity v1",
    "lane_slug": ALPHA_KALSHI_4237F81367_FAMILY,
    "signal_details_key": ALPHA_KALSHI_4237F81367_FAMILY,
    "reason_prefix": ALPHA_KALSHI_4237F81367_REASON_PREFIX,
    "rule_digest": "4237f81367",
    "rule_label": (
        "type=price_move platform=kalshi direction=up price_bucket=p020_050 "
        "ev_bucket=ev_005_plus liquidity=liquidity_010k_100k family=liquidity_regime"
    ),
    "trade_direction": "buy_yes",
    "strategy_archetype": "follow_positive_yes_ev",
    "paper_only": True,
    "live_orders_enabled": False,
    "pilot_arming_enabled": False,
    "thresholds_frozen": True,
    "frozen_rule": {
        "signal_type": "price_move",
        "platform": "kalshi",
        "direction": "up",
        "timeframe": "all",
        "price_bucket": "p020_050",
        "expected_value_bucket": "ev_005_plus",
        "market_category": "all",
        "market_tenor_bucket": "all",
        "volume_bucket": "all",
        "liquidity_bucket": "liquidity_010k_100k",
        "feature_family": "liquidity_regime",
        "min_rank_score": None,
        "min_expected_value": None,
        "min_price_at_fire": None,
        "max_price_at_fire": None,
        "label": (
            "type=price_move platform=kalshi direction=up price_bucket=p020_050 "
            "ev_bucket=ev_005_plus liquidity=liquidity_010k_100k family=liquidity_regime"
        ),
    },
    "frozen_dimensions": {
        "signal_type": "price_move",
        "platform": "kalshi",
        "direction": "up",
        "liquidity_bucket": "liquidity_010k_100k",
        "feature_family": "liquidity_regime",
    },
    "frozen_thresholds": {
        "bucket_semantics": "lower_bound_inclusive_upper_bound_exclusive",
        "price_at_fire": {
            "bucket": "p020_050",
            "min_inclusive": 0.20,
            "max_exclusive": 0.50,
            "unit": "yes_price",
        },
        "expected_value": {
            "bucket": "ev_005_plus",
            "min_inclusive": 0.05,
            "max_exclusive": None,
            "unit": "yes_expected_value",
        },
        "last_liquidity": {
            "bucket": "liquidity_010k_100k",
            "min_inclusive": 10_000.0,
            "max_exclusive": 100_000.0,
            "unit": "usd",
        },
    },
    "current_market_precheck": {
        "required": True,
        "price_source": "fresh_kalshi_orderbook_midpoint",
        "reject_if_current_price_outside_frozen_price_bucket": True,
        "reject_if_trade_side_no_longer_has_positive_yes_edge": True,
    },
    "required_surfaces": [
        "frozen_rule_evaluator",
        "strategy_registry_seed",
        "paper_execution_run_loop",
        "profitability_snapshot",
        "research_lab_lane_payload",
        "scheduler_lane_wiring",
    ],
    "promotion_gates": [
        "observe at least 30 calendar days forward",
        "collect at least 20 resolved paper trades",
        "require positive execution-adjusted paper P&L",
        "require positive average CLV",
        "pause on 5% paper drawdown or evidence outage",
    ],
    "source_metrics": {
        "test": {
            "sample_count": 650,
            "total_profit_loss": 70.855,
            "avg_clv": 0.029769,
        }
    },
}

ALPHA_KALSHI_D80BDF77A9_V1: dict[str, Any] = {
    "implementation_status": "paper_active",
    "candidate_id": "kalshi_alpha_d80bdf77a9",
    "strategy_family": ALPHA_KALSHI_D80BDF77A9_FAMILY,
    "strategy_name": ALPHA_KALSHI_D80BDF77A9_VERSION,
    "strategy_version": ALPHA_KALSHI_D80BDF77A9_VERSION,
    "version_label": "Alpha Kalshi Price Move Up Volume v1",
    "lane_slug": ALPHA_KALSHI_D80BDF77A9_FAMILY,
    "signal_details_key": ALPHA_KALSHI_D80BDF77A9_FAMILY,
    "reason_prefix": ALPHA_KALSHI_D80BDF77A9_REASON_PREFIX,
    "rule_digest": "d80bdf77a9",
    "rule_label": (
        "type=price_move platform=kalshi direction=up ev_bucket=ev_001_002 "
        "volume=volume_001k_010k family=volume_regime"
    ),
    "trade_direction": "buy_yes",
    "strategy_archetype": "follow_positive_yes_ev",
    "paper_min_ev_threshold": "0.01",
    "paper_only": True,
    "live_orders_enabled": False,
    "pilot_arming_enabled": False,
    "thresholds_frozen": True,
    "frozen_rule": {
        "signal_type": "price_move",
        "platform": "kalshi",
        "direction": "up",
        "timeframe": "all",
        "price_bucket": "all",
        "expected_value_bucket": "ev_001_002",
        "market_category": "all",
        "market_tenor_bucket": "all",
        "volume_bucket": "volume_001k_010k",
        "liquidity_bucket": "all",
        "feature_family": "volume_regime",
        "min_rank_score": None,
        "min_expected_value": None,
        "min_price_at_fire": None,
        "max_price_at_fire": None,
        "label": (
            "type=price_move platform=kalshi direction=up ev_bucket=ev_001_002 "
            "volume=volume_001k_010k family=volume_regime"
        ),
    },
    "frozen_dimensions": {
        "signal_type": "price_move",
        "platform": "kalshi",
        "direction": "up",
        "volume_bucket": "volume_001k_010k",
        "feature_family": "volume_regime",
    },
    "frozen_thresholds": {
        "bucket_semantics": "lower_bound_inclusive_upper_bound_exclusive",
        "expected_value": {
            "bucket": "ev_001_002",
            "min_inclusive": 0.01,
            "max_exclusive": 0.02,
            "unit": "yes_expected_value",
        },
        "last_volume_24h": {
            "bucket": "volume_001k_010k",
            "min_inclusive": 1_000.0,
            "max_exclusive": 10_000.0,
            "unit": "usd",
        },
    },
    "current_market_precheck": {
        "required": True,
        "price_source": "fresh_kalshi_orderbook_midpoint",
        "reject_if_current_price_outside_frozen_price_bucket": False,
        "reject_if_trade_side_no_longer_has_positive_yes_edge": True,
    },
    "required_surfaces": [
        "frozen_rule_evaluator",
        "strategy_registry_seed",
        "paper_execution_run_loop",
        "profitability_snapshot",
        "research_lab_lane_payload",
        "scheduler_lane_wiring",
    ],
    "promotion_gates": [
        "observe at least 30 calendar days forward",
        "collect at least 20 resolved paper trades",
        "require positive execution-adjusted paper P&L",
        "require positive average CLV",
        "pause on 5% paper drawdown or evidence outage",
        "require operator review because the historical EV bucket is only 1-2 cents",
    ],
    "source_metrics": {
        "test": {
            "sample_count": 121,
            "total_profit_loss": 1.19,
            "avg_clv": 0.013058,
        }
    },
}


def enabled_alpha_rule_blueprints() -> list[dict[str, Any]]:
    return [
        deepcopy(ALPHA_KALSHI_4237F81367_V1),
        deepcopy(ALPHA_KALSHI_D80BDF77A9_V1),
    ]


def alpha_rule_blueprint_by_family(family: str | None) -> dict[str, Any] | None:
    normalized = str(family or "").strip().lower()
    for blueprint in enabled_alpha_rule_blueprints():
        if str(blueprint.get("strategy_family") or "").lower() == normalized:
            return blueprint
    return None


def alpha_rule_blueprint_by_version(version: str | None) -> dict[str, Any] | None:
    normalized = str(version or "").strip().lower()
    for blueprint in enabled_alpha_rule_blueprints():
        if str(blueprint.get("strategy_version") or "").lower() == normalized:
            return blueprint
    return None


def alpha_rule_family_seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "family": ALPHA_KALSHI_4237F81367_FAMILY,
            "label": "Alpha Kalshi Price Move Up",
            "posture": "research_active",
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": (
                "Paper-only frozen Alpha Factory Kalshi price-move-up candidate "
                "with mid YES price and 10k-100k liquidity."
            ),
            "disabled_reason": None,
        },
        {
            "family": ALPHA_KALSHI_D80BDF77A9_FAMILY,
            "label": "Alpha Kalshi Price Move Up Volume",
            "posture": "research_active",
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": (
                "Paper-only frozen Alpha Factory Kalshi price-move-up candidate "
                "with 1-2 cent YES EV and 1k-10k 24h volume."
            ),
            "disabled_reason": None,
        },
    ]


def alpha_rule_version_seed_rows() -> list[dict[str, Any]]:
    rows = []
    for blueprint in enabled_alpha_rule_blueprints():
        rows.append(
            {
                "family": blueprint["strategy_family"],
                "version_key": blueprint["strategy_version"],
                "version_label": blueprint["version_label"],
                "strategy_name": blueprint["strategy_name"],
                "version_status": "candidate",
                "autonomy_tier": "shadow_only",
                "is_current": True,
                "is_frozen": True,
                "config_json": {
                    "target_lane": "paper_alpha_candidate",
                    "paper_only": True,
                    "live_orders_enabled": False,
                    "pilot_arming_enabled": False,
                    "thresholds_frozen": True,
                    "blueprint_id": blueprint["strategy_version"],
                    "candidate_id": blueprint["candidate_id"],
                    "rule_digest": blueprint["rule_digest"],
                    "rule": deepcopy(blueprint["frozen_rule"]),
                    "frozen_thresholds": deepcopy(blueprint["frozen_thresholds"]),
                    "trade_direction": blueprint["trade_direction"],
                    "paper_min_ev_threshold": blueprint.get("paper_min_ev_threshold"),
                    "current_market_precheck": deepcopy(blueprint["current_market_precheck"]),
                    "source_metrics": deepcopy(blueprint.get("source_metrics") or {}),
                    "notes": (
                        "Frozen Alpha Factory candidate promoted only to forward paper "
                        "evidence collection; no live submission is permitted."
                    ),
                },
                "provenance_json": {
                    "seed_source": "alpha_factory_2026_05_06",
                    "family_kind": "strategy",
                    "candidate_id": blueprint["candidate_id"],
                    "rule_digest": blueprint["rule_digest"],
                    "historical_reference": {
                        "rule": blueprint["rule_label"],
                        "test": deepcopy((blueprint.get("source_metrics") or {}).get("test") or {}),
                        "caveat": (
                            "Historical signal-level alpha only; paper-only lane exists "
                            "to test whether the candidate survives current orderbook "
                            "execution and market resolution."
                        ),
                    },
                },
            }
        )
    return rows
