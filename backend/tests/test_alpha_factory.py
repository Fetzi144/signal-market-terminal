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
    market_category: str = "unknown",
    market_tenor_bucket: str = "tenor_unknown",
    volume_bucket: str = "volume_unknown",
    liquidity_bucket: str = "liquidity_unknown",
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
        market_category=market_category,
        market_tenor_bucket=market_tenor_bucket,
        volume_bucket=volume_bucket,
        liquidity_bucket=liquidity_bucket,
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


def test_alpha_factory_emits_new_candidate_queue_blueprint():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.12,
                clv=0.035,
                direction="up",
                expected_value=0.08,
                estimated_probability=0.44,
                price_at_fire=0.35,
                market_category="sports",
                market_tenor_bucket="tenor_3_7d",
                volume_bucket="volume_010k_100k",
                liquidity_bucket="liquidity_010k_100k",
            )
        )
        rows.append(
            _row(
                index * 2 + 1,
                profit_loss=-0.05,
                clv=-0.02,
                direction="down",
                expected_value=-0.04,
                estimated_probability=0.70,
                price_at_fire=0.68,
                market_category="politics",
                market_tenor_bucket="tenor_30d_plus",
                volume_bucket="volume_001k_010k",
                liquidity_bucket="liquidity_001k_010k",
            )
        )

    snapshot = build_alpha_factory_snapshot_from_rows(
        rows,
        platform="kalshi",
        max_candidates=20,
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    assert snapshot["candidate_queue_count"] >= 1
    assert snapshot["new_ready_candidate_count"] == snapshot["candidate_queue_count"]

    candidate = snapshot["candidate_queue"][0]
    blueprint = candidate["paper_lane_blueprint"]
    assert candidate["candidate_queue_status"] == "paper_lane_blueprint_ready"
    assert candidate["dedupe_status"] == "new_candidate"
    assert candidate["existing_lane"] is None
    assert candidate["live_orders_enabled"] is False
    assert blueprint["implementation_status"] == "ready_to_implement"
    assert blueprint["candidate_id"] == candidate["candidate_id"]
    assert blueprint["rule_digest"] == candidate["rule_digest"]
    assert blueprint["frozen_rule"] == candidate["rule"]
    assert blueprint["paper_only"] is True
    assert blueprint["live_orders_enabled"] is False
    assert "frozen_rule_evaluator" in blueprint["required_surfaces"]
    assert "price_at_fire" in blueprint["frozen_thresholds"]
    assert snapshot["next_best_actions"][0]["evidence"]["candidate_id"] == candidate["candidate_id"]

    payload = alpha_factory_lane_payload(snapshot)
    assert payload["strategy_version"] == candidate["strategy_version"]
    assert payload["details_json"]["candidate_queue_count"] == snapshot["candidate_queue_count"]
    assert payload["details_json"]["candidate_queue"][0]["candidate_id"] == candidate["candidate_id"]
    assert payload["details_json"]["paper_lane_blueprint"]["candidate_id"] == candidate["candidate_id"]


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

    assert any(
        lane["family"] == "kalshi_down_yes_fade"
        and lane["strategy_version"] == "kalshi_down_yes_fade_v2"
        and lane["match_type"] == "exact_existing_lane"
        for lane in (
            candidate["existing_lane"]
            for candidate in snapshot["top_candidates"]
            if candidate.get("existing_lane")
        )
    )


def test_alpha_factory_maps_very_low_price_down_fade_to_existing_lane():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.08,
                clv=0.012,
                timeframe="30m",
                expected_value=-0.045,
                estimated_probability=0.03,
                price_at_fire=0.075,
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

    assert any(
        lane["family"] == "kalshi_very_low_yes_fade"
        and lane["strategy_version"] == "kalshi_very_low_yes_fade_v1"
        and lane["match_type"] == "exact_existing_lane"
        for lane in (
            candidate["existing_lane"]
            for candidate in snapshot["top_candidates"]
            if candidate.get("existing_lane")
        )
    )


def test_alpha_factory_suppresses_broad_very_low_fade_variant_as_not_new():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.08,
                clv=0.012,
                timeframe="30m",
                expected_value=None,
                estimated_probability=0.03,
                price_at_fire=0.075,
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

    variant = next(
        candidate
        for candidate in snapshot["top_candidates"]
        if candidate["rule"]["price_bucket"] == "p005_010"
        and candidate["rule"]["timeframe"] == "30m"
        and candidate["rule"]["expected_value_bucket"] == "all"
    )

    assert variant["existing_lane"]["family"] == "kalshi_very_low_yes_fade"
    assert variant["existing_lane"]["match_type"] == "covered_existing_lane_variant"
    assert variant["ready_for_paper_lane"] is False
    assert variant["dedupe_status"] == "covered_existing_lane_variant"
    assert variant["next_step"] == "review_existing_lane_variant"
    assert "covered_by_existing_lane_variant" in variant["blockers"]
    assert snapshot["new_ready_candidate_count"] == 0
    assert snapshot["suppressed_candidate_count"] >= 1


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

    cheap_candidates = [
        candidate
        for candidate in snapshot["top_candidates"]
        if (candidate.get("existing_lane") or {}).get("family") == "kalshi_cheap_yes_follow"
    ]

    assert cheap_candidates
    assert all(candidate["ready_for_paper_lane"] is False for candidate in cheap_candidates)
    assert all(candidate["next_step"] == "keep_quarantined_lane_paused" for candidate in cheap_candidates)
    assert all("matched_quarantined_lane_family" in candidate["blockers"] for candidate in cheap_candidates)
    assert all(candidate["existing_lane"]["quarantine"]["enabled"] is True for candidate in cheap_candidates)


def test_alpha_factory_suppresses_tiny_positive_yes_variant_near_quarantined_cheap_lane():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                profit_loss=0.04,
                clv=0.01,
                timeframe="1h",
                expected_value=0.005,
                estimated_probability=0.40,
                price_at_fire=0.40,
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
        max_candidates=30,
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
    )

    variant = next(
        candidate
        for candidate in snapshot["top_candidates"]
        if (candidate.get("existing_lane") or {}).get("family") == "kalshi_cheap_yes_follow"
    )

    assert variant["existing_lane"]["family"] == "kalshi_cheap_yes_follow"
    assert variant["existing_lane"]["match_type"] == "quarantined_related_lane_variant"
    assert variant["ready_for_paper_lane"] is False
    assert variant["next_step"] == "keep_quarantined_lane_paused"
    assert "matched_quarantined_lane_family" in variant["blockers"]
    assert snapshot["new_ready_candidate_count"] == 0


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
