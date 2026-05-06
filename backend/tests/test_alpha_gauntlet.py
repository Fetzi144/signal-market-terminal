from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.reports.alpha_gauntlet import AlphaSignalRow, evaluate_alpha_gauntlet_rows, load_alpha_signal_rows
from tests.conftest import make_market, make_outcome, make_signal


def _row(
    index: int,
    *,
    signal_type: str = "edge",
    platform: str = "polymarket",
    profit_loss: float = 0.1,
    clv: float = 0.02,
    direction: str = "up",
    timeframe: str = "1h",
    rank_score: float = 0.8,
    expected_value: float = 0.03,
    price_at_fire: float = 0.5,
    market_category: str = "politics",
    market_tenor_bucket: str = "tenor_7_30d",
    volume_bucket: str = "volume_010k_100k",
    liquidity_bucket: str = "liquidity_010k_100k",
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
        estimated_probability=0.6 if profit_loss > 0 else 0.4,
        price_at_fire=price_at_fire,
        market_category=market_category,
        market_tenor_bucket=market_tenor_bucket,
        volume_bucket=volume_bucket,
        liquidity_bucket=liquidity_bucket,
    )


def test_alpha_gauntlet_reports_insufficient_data_before_searching_rules():
    result = evaluate_alpha_gauntlet_rows([_row(0), _row(1)], min_train_sample=2, min_validation_sample=1)

    assert result["verdict"] == "insufficient_data"
    assert result["rule_count"] == 0
    assert result["split_counts"] == {"train": 2, "validation": 0, "test": 0}


def test_alpha_gauntlet_finds_rule_that_survives_holdout():
    rows = [_row(index) for index in range(80)]

    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=5,
        min_validation_sample=5,
        min_test_sample=5,
    )

    assert result["verdict"] == "paper_alpha_candidate"
    assert result["surviving_candidates"]
    top_candidate = result["surviving_candidates"][0]
    assert top_candidate["train_pass"] is True
    assert top_candidate["validation_pass"] is True
    assert top_candidate["test_pass"] is True
    assert top_candidate["test"]["total_profit_loss"] > 0
    assert top_candidate["test"]["avg_clv"] > 0


def test_alpha_gauntlet_rejects_train_validation_edge_that_fails_test():
    rows = []
    rows.extend(_row(index, profit_loss=0.1, clv=0.02) for index in range(60))
    rows.extend(_row(index, profit_loss=-0.2, clv=-0.03) for index in range(60, 80))

    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=5,
        min_validation_sample=5,
        min_test_sample=5,
    )

    assert result["verdict"] == "failed_out_of_sample"
    assert result["selected_candidates"]
    assert not result["surviving_candidates"]
    assert result["selected_candidates"][0]["test_pass"] is False


def test_alpha_gauntlet_flags_train_only_overfit():
    rows = []
    rows.extend(_row(index, profit_loss=0.1, clv=0.02) for index in range(40))
    rows.extend(_row(index, profit_loss=-0.2, clv=-0.03) for index in range(40, 80))

    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=5,
        min_validation_sample=5,
        min_test_sample=5,
    )

    assert result["verdict"] == "overfit_warning"
    assert not result["selected_candidates"]
    assert result["rejected_candidates"][0]["verdict"] == "overfit_warning"


def test_alpha_gauntlet_generates_directional_price_ev_bucket_rules_from_train():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                signal_type="price_move",
                platform="kalshi",
                direction="down",
                timeframe="30m",
                price_at_fire=0.15,
                expected_value=-0.01,
                profit_loss=0.08,
                clv=0.02,
            )
        )
        rows.append(
            _row(
                index * 2 + 1,
                signal_type="price_move",
                platform="kalshi",
                direction="up",
                timeframe="30m",
                price_at_fire=0.85,
                expected_value=0.04,
                profit_loss=-0.06,
                clv=-0.01,
            )
        )

    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
        top_n=100,
    )

    assert result["verdict"] == "paper_alpha_candidate"
    survivor_labels = [candidate["rule"]["label"] for candidate in result["surviving_candidates"]]
    assert any(
        "direction=down" in label
        and "timeframe=30m" in label
        and "price_bucket=p010_020" in label
        and "ev_bucket=ev_neg" in label
        for label in survivor_labels
    )


def test_alpha_gauntlet_generates_market_feature_family_rules_from_train():
    rows = []
    for index in range(90):
        rows.append(
            _row(
                index * 2,
                signal_type="price_move",
                platform="kalshi",
                direction="down",
                timeframe="30m",
                price_at_fire=0.35,
                expected_value=-0.02,
                market_category="sports",
                market_tenor_bucket="tenor_0_1d",
                volume_bucket="volume_001k_010k",
                liquidity_bucket="liquidity_001k_010k",
                profit_loss=0.10,
                clv=0.025,
            )
        )
        rows.append(
            _row(
                index * 2 + 1,
                signal_type="price_move",
                platform="kalshi",
                direction="down",
                timeframe="30m",
                price_at_fire=0.35,
                expected_value=-0.02,
                market_category="crypto",
                market_tenor_bucket="tenor_0_1d",
                volume_bucket="volume_001k_010k",
                liquidity_bucket="liquidity_001k_010k",
                profit_loss=-0.12,
                clv=-0.03,
            )
        )

    result = evaluate_alpha_gauntlet_rows(
        rows,
        min_train_sample=10,
        min_validation_sample=10,
        min_test_sample=10,
        top_n=100,
    )

    assert result["verdict"] == "paper_alpha_candidate"
    survivor_rules = [candidate["rule"] for candidate in result["surviving_candidates"]]
    assert any(
        rule["feature_family"] in {"category_price_ev", "category_directional"}
        and rule["market_category"] == "sports"
        for rule in survivor_rules
    )
    assert not any(
        rule["feature_family"] in {"category_price_ev", "category_directional"}
        and rule["market_category"] == "crypto"
        for rule in survivor_rules
    )


@pytest.mark.asyncio
async def test_alpha_gauntlet_loader_caps_to_latest_rows_then_sorts_chronologically(session):
    now = datetime(2026, 4, 28, tzinfo=timezone.utc)
    market = make_market(
        session,
        platform="kalshi",
        question="Loader cap market",
        category="Sports",
        end_date=now + timedelta(days=2),
        last_volume_24h=Decimal("5000.00"),
        last_liquidity=Decimal("15000.00"),
    )
    outcome = make_outcome(session, market.id, name="Yes")
    for index in range(5):
        fired_at = now - timedelta(days=4 - index)
        make_signal(
            session,
            market.id,
            outcome.id,
            fired_at=fired_at,
            dedupe_bucket=fired_at,
            source_platform="kalshi",
            resolved=True,
            resolved_correctly=True,
            profit_loss=Decimal("0.100000"),
            clv=Decimal("0.010000"),
            expected_value=Decimal("0.020000"),
            estimated_probability=Decimal("0.6000"),
            price_at_fire=Decimal("0.500000"),
        )
    await session.commit()

    rows = await load_alpha_signal_rows(
        session,
        window_days=10,
        max_signals=3,
        as_of=now + timedelta(hours=1),
    )

    assert [row.fired_at for row in rows] == [
        now - timedelta(days=2),
        now - timedelta(days=1),
        now,
    ]
    assert rows[-1].market_category == "sports"
    assert rows[-1].market_tenor_bucket == "tenor_1_3d"
    assert rows[-1].volume_bucket == "volume_001k_010k"
    assert rows[-1].liquidity_bucket == "liquidity_010k_100k"
