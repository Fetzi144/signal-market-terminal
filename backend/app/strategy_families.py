from __future__ import annotations

from typing import Any

from app.config import settings

POSTURE_BENCHMARK_ONLY = "benchmark_only"
POSTURE_RESEARCH_ACTIVE = "research_active"
POSTURE_ADVISORY_ONLY = "advisory_only"
POSTURE_DISABLED = "disabled"


def _family_index() -> dict[str, dict[str, Any]]:
    return {
        "default_strategy": {
            "family": "default_strategy",
            "label": "Default Strategy",
            "posture": POSTURE_BENCHMARK_ONLY,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": "Frozen confluence benchmark used to prove or falsify edge honestly.",
            "disabled_reason": None,
        },
        "structure": {
            "family": "structure",
            "label": "Polymarket Structure",
            "posture": POSTURE_RESEARCH_ACTIVE,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "structure",
            "description": "Polymarket-only neg-risk, complement, and parity structures remain the first live edge lane.",
            "disabled_reason": None,
        },
        "maker": {
            "family": "maker",
            "label": "Polymarket Maker",
            "posture": POSTURE_ADVISORY_ONLY,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "maker_economics",
            "description": "Fee-aware and reward-aware quoting stays advisory until continuity and replay are trustworthy.",
            "disabled_reason": None,
        },
        "kalshi_low_yes_fade": {
            "family": "kalshi_low_yes_fade",
            "label": "Kalshi Low-YES Fade",
            "posture": POSTURE_RESEARCH_ACTIVE,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": "Paper-only Kalshi candidate that buys NO on low-priced YES contracts already moving down.",
            "disabled_reason": None,
        },
        "kalshi_very_low_yes_fade": {
            "family": "kalshi_very_low_yes_fade",
            "label": "Kalshi Very-Low-YES Fade",
            "posture": POSTURE_RESEARCH_ACTIVE,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": "Paper-only Kalshi candidate that buys NO on 5-10 cent YES contracts moving down on the 30m signal.",
            "disabled_reason": None,
        },
        "kalshi_down_yes_fade": {
            "family": "kalshi_down_yes_fade",
            "label": "Kalshi Down-YES Fade",
            "posture": POSTURE_RESEARCH_ACTIVE,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": "Paper-only Kalshi candidate that buys NO on mid-priced YES contracts already moving down.",
            "disabled_reason": None,
        },
        "kalshi_cheap_yes_follow": {
            "family": "kalshi_cheap_yes_follow",
            "label": "Kalshi Cheap-YES Follow",
            "posture": POSTURE_RESEARCH_ACTIVE,
            "configured": True,
            "review_enabled": True,
            "primary_surface": "paper_trading",
            "description": "Paper-only Kalshi candidate that buys very cheap YES contracts after a down move when YES EV is mildly positive.",
            "disabled_reason": None,
        },
        "cross_venue_basis": {
            "family": "cross_venue_basis",
            "label": "Cross-Venue Basis",
            "posture": POSTURE_DISABLED,
            "configured": bool(settings.polymarket_structure_include_cross_venue),
            "review_enabled": False,
            "primary_surface": "structure",
            "description": "Cross-venue spread research stays informational until paired executable hedge routing exists.",
            "disabled_reason": "Paired executable hedge routing is not implemented yet.",
        },
    }


def build_strategy_family_reviews() -> list[dict[str, Any]]:
    return list(_family_index().values())


def get_strategy_family_review(family: str | None) -> dict[str, Any] | None:
    if family is None:
        return None
    return _family_index().get(str(family).strip().lower())


def infer_signal_review_family(signal_type: str | None) -> dict[str, Any] | None:
    normalized = str(signal_type or "").strip().lower()
    if normalized == "confluence":
        return get_strategy_family_review("default_strategy")
    if normalized == "arbitrage":
        return get_strategy_family_review("cross_venue_basis")
    return None


def display_signal_type(signal_type: str | None) -> str | None:
    normalized = str(signal_type or "").strip().lower()
    if not normalized:
        return None
    if normalized == "arbitrage":
        return "cross_venue_spread"
    return normalized


def display_signal_label(signal_type: str | None) -> str | None:
    display_type = display_signal_type(signal_type)
    if display_type is None:
        return None
    return display_type.replace("_", " ").title()


__all__ = [
    "POSTURE_ADVISORY_ONLY",
    "POSTURE_BENCHMARK_ONLY",
    "POSTURE_DISABLED",
    "POSTURE_RESEARCH_ACTIVE",
    "build_strategy_family_reviews",
    "display_signal_label",
    "display_signal_type",
    "get_strategy_family_review",
    "infer_signal_review_family",
]
