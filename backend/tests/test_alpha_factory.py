from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.reports.alpha_factory import alpha_factory_lane_payload, build_alpha_factory_snapshot_from_rows
from app.reports.alpha_gauntlet import AlphaSignalRow


def _row(
    index: int,
    *,
    signal_type: str = "price_move",
    platform: str = "kalshi",
    profit_loss: float = 0.08,
    clv: float = 0.02,
    direction: str = "down",
    timeframe: str = "30m",
    rank_score: float = 0.8,
    expected_value: float | None = -0.01,
    estimated_probability: float | None = 0.12,
    price_at_fire: float | None = 0.15,
) -> AlphaSignalRow:
    return AlphaSignalRow(
        signal_id=f"signal-{index}",
        fired_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
        signal_type=signal_type,
        platform=platform,
        profit_loss=profit_loss,
        clv=clv,
        resolved_correctly=profit_loss > 0,
        direction=direction,
        timeframe=timeframe,
        rank_score=rank_score,
        expected_value=expected_value,
        estimated_probability=estimated_probability,
        price_at_fire=price_at_fire,
    )


def test_alpha_factory_turns_surviving_kalshi_rule_into_paper_lane_candidate():
    rows = []
    for index in range(90):
        rows.append(_row(index * 2))
        rows.append(
            _row(
                index * 2 + 1,
                profit_loss=-0.06,
                clv=-0.01,
                direction="up",
                expected_value=0.04,
                estimated_probability=0.90,
                price_at_fire=0.85,
            )
        )

    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    assert snapshot["paper_only"] is True
    assert snapshot["live_submission_permitted"] is False
    assert snapshot["verdict"] == "candidate_queue_ready"
    assert snapshot["ready_candidate_count"] >= 1
    buy_no_candidates = [
        candidate
        for candidate in snapshot["top_candidates"]
        if candidate["trade_direction"] == "buy_no" and candidate["ready_for_paper_lane"]
    ]
    assert buy_no_candidates
    assert any(candidate["existing_lane"] for candidate in buy_no_candidates)


def test_alpha_factory_maps_mid_price_down_fade_to_v2_existing_lane():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.12,
                clv=0.03,
                expected_value=-0.08,
                estimated_probability=0.22,
                price_at_fire=0.35,
            )
        )
        rows.append(
            _row(
                index * 2 + 1,
                profit_loss=-0.06,
                clv=-0.01,
                direction="up",
                expected_value=0.04,
                estimated_probability=0.90,
                price_at_fire=0.85,
            )
        )

    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    existing_lanes = [
        candidate["existing_lane"]
        for candidate in snapshot["top_candidates"]
        if candidate.get("existing_lane")
    ]

    assert {
        "family": "kalshi_down_yes_fade",
        "strategy_version": "kalshi_down_yes_fade_v2",
        "lane": "paper_forward_gate",
    } in existing_lanes


def test_alpha_factory_maps_cheap_yes_follow_to_existing_lane():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.04,
                clv=0.01,
                expected_value=0.009,
                estimated_probability=0.049,
                price_at_fire=0.04,
            )
        )
        rows.append(
            _row(
                index * 2 + 1,
                profit_loss=-0.06,
                clv=-0.01,
                direction="up",
                expected_value=-0.03,
                estimated_probability=0.55,
                price_at_fire=0.58,
            )
        )

    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    existing_lanes = [
        candidate["existing_lane"]
        for candidate in snapshot["top_candidates"]
        if candidate.get("existing_lane")
    ]

    assert {
        "family": "kalshi_cheap_yes_follow",
        "strategy_version": "kalshi_cheap_yes_follow_v1",
        "lane": "paper_forward_gate",
    } in existing_lanes


def test_alpha_factory_filters_to_kalshi_and_reports_empty_history():
    rows = [_row(index, platform="polymarket") for index in range(30)]

    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        min_train_sample=5,
        min_validation_sample=5,
        min_test_sample=5,
    )

    assert snapshot["row_count"] == 0
    assert snapshot["candidate_count"] == 0
    assert snapshot["blockers"] == ["no_kalshi_resolved_signal_history"]


def test_alpha_factory_lane_payload_exposes_rankable_holdout_evidence():
    rows = [_row(index) for index in range(80)]
    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    payload = alpha_factory_lane_payload(snapshot)

    assert payload["family"] == "alpha_factory"
    assert payload["source_kind"] == "alpha_factory_snapshot"
    assert payload["verdict"] == "research_ready"
    assert payload["replay_net_pnl"] > 0
    assert payload["avg_clv"] > 0
    assert payload["details_json"]["top_candidates"]
