from __future__ import annotations

from app.jobs.scheduler import (
    _alpha_factory_existing_lane_candidate_count,
    _alpha_factory_new_candidate_count,
)


def test_alpha_factory_autopilot_counts_only_new_ready_candidates():
    snapshot = {
        "top_candidates": [
            {
                "ready_for_paper_lane": True,
                "trade_direction": "buy_yes",
                "existing_lane": None,
            },
            {
                "ready_for_paper_lane": True,
                "trade_direction": "buy_no",
                "existing_lane": {
                    "family": "kalshi_down_yes_fade",
                    "strategy_version": "kalshi_down_yes_fade_v2",
                },
            },
            {
                "ready_for_paper_lane": False,
                "trade_direction": "buy_yes",
                "existing_lane": None,
            },
            {
                "ready_for_paper_lane": True,
                "trade_direction": None,
                "existing_lane": None,
            },
        ]
    }

    assert _alpha_factory_new_candidate_count(snapshot) == 1
    assert _alpha_factory_existing_lane_candidate_count(snapshot) == 1


def test_alpha_factory_autopilot_does_not_count_existing_lanes_as_new():
    snapshot = {
        "top_candidates": [
            {
                "ready_for_paper_lane": True,
                "trade_direction": "buy_no",
                "existing_lane": {
                    "family": "kalshi_low_yes_fade",
                    "strategy_version": "kalshi_low_yes_fade_v1",
                },
            }
        ]
    }

    assert _alpha_factory_new_candidate_count(snapshot) == 0
    assert _alpha_factory_existing_lane_candidate_count(snapshot) == 1
