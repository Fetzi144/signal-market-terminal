from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.ingestion.polymarket_maker_economics as maker_module
from app.config import settings
from app.ingestion.polymarket_execution_policy import PASSIVE_LABEL_BY_DIRECTION
from app.ingestion.polymarket_maker_economics import (
    FEE_SOURCE_KIND,
    REWARD_SOURCE_KIND,
    insert_reward_history_if_changed,
    insert_token_fee_history_if_changed,
)
from app.models.market_structure import MarketStructureOpportunityLeg
from app.models.polymarket_maker import PolymarketMakerEconomicsSnapshot
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_microstructure import PolymarketPassiveFillLabel
from tests.test_structure_engine import _seed_executable_neg_risk_setup, _set_structure_defaults


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _validated_opportunity(client, *, event_slug: str) -> dict:
    build_response = await client.post(
        "/api/v1/ingest/polymarket/structure/groups/build",
        json={"reason": "manual", "event_slug": event_slug},
    )
    scan_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/scan",
        json={"reason": "manual", "event_slug": event_slug},
    )
    scan_run = scan_response.json()
    validate_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/validate",
        json={"reason": "manual", "scan_run_id": scan_run["id"]},
    )

    assert build_response.status_code == 200
    assert scan_response.status_code == 200
    assert validate_response.status_code == 200

    opportunities_response = await client.get(
        "/api/v1/ingest/polymarket/structure/opportunities"
        f"?classification=executable_candidate&event_slug={event_slug}&executable_only=true"
    )
    assert opportunities_response.status_code == 200
    rows = opportunities_response.json()["rows"]
    assert rows
    return rows[0]


async def _maker_leg_context(session: AsyncSession, *, opportunity_id: int) -> tuple[MarketStructureOpportunityLeg, PolymarketMarketDim, PolymarketAssetDim]:
    maker_leg = (
        await session.execute(
            select(MarketStructureOpportunityLeg)
            .where(
                MarketStructureOpportunityLeg.opportunity_id == opportunity_id,
                MarketStructureOpportunityLeg.venue == "polymarket",
            )
            .order_by(MarketStructureOpportunityLeg.leg_index.asc())
            .limit(1)
        )
    ).scalar_one()
    market_dim = (
        await session.execute(
            select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == maker_leg.condition_id)
        )
    ).scalar_one()
    asset_dim = (
        await session.execute(
            select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == maker_leg.asset_id)
        )
    ).scalar_one()
    return maker_leg, market_dim, asset_dim


async def _seed_passive_fill_labels(
    session: AsyncSession,
    *,
    market_dim: PolymarketMarketDim,
    asset_dim: PolymarketAssetDim,
    side: str,
    anchor_end: datetime,
    count: int,
):
    label_side = PASSIVE_LABEL_BY_DIRECTION[side]
    posted_price = Decimal("0.80") if side == "buy_yes" else Decimal("0.20")
    for index in range(count):
        session.add(
            PolymarketPassiveFillLabel(
                market_dim_id=market_dim.id,
                asset_dim_id=asset_dim.id,
                condition_id=asset_dim.condition_id,
                asset_id=asset_dim.asset_id,
                anchor_bucket_start_exchange=anchor_end - timedelta(minutes=index + 1),
                horizon_ms=settings.polymarket_execution_policy_default_horizon_ms,
                side=label_side,
                posted_price=posted_price,
                touch_observed=True,
                trade_through_observed=True,
                best_price_improved_against_order=False,
                adverse_move_after_touch_bps=Decimal("0"),
                source_feature_table="test_passive_labels",
                source_feature_row_id=index + 1,
                completeness_flags_json={"source": "test"},
            )
        )


@pytest.mark.asyncio
async def test_structure_maker_economics_and_quote_optimizer_api_flow(client, engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase9-maker-api",
            title="Phase9 Maker API",
            anchor_condition_id="phase9-anchor",
            basket_condition_id="phase9-basket",
        )
        await session.commit()

    opportunity = await _validated_opportunity(client, event_slug="phase9-maker-api")
    observed_at = datetime.now(timezone.utc)

    async with session_factory() as session:
        maker_leg, market_dim, asset_dim = await _maker_leg_context(session, opportunity_id=opportunity["id"])
        maker_leg.details_json = {**(maker_leg.details_json or {}), "min_order_size": "0.01"}
        await session.flush()
        assert await insert_token_fee_history_if_changed(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id,
            source_kind=FEE_SOURCE_KIND,
            effective_at_exchange=observed_at - timedelta(minutes=5),
            observed_at_local=observed_at,
            sync_run_id=None,
            fees_enabled=True,
            maker_fee_rate=Decimal("0.0000"),
            taker_fee_rate=Decimal("0.0200"),
            token_base_fee_rate=Decimal("6.0000"),
            fee_schedule_json={"rate": "0.02"},
        ) is True
        assert await insert_reward_history_if_changed(
            session,
            market_dim=market_dim,
            condition_id=market_dim.condition_id,
            source_kind=REWARD_SOURCE_KIND,
            effective_at_exchange=observed_at - timedelta(minutes=5),
            observed_at_local=observed_at,
            sync_run_id=None,
            reward_status="active",
            reward_program_id="program-1",
            reward_daily_rate=Decimal("50.0000"),
            min_incentive_size=Decimal("0.0100"),
            max_incentive_spread=Decimal("1.0000"),
            start_at_exchange=observed_at - timedelta(hours=1),
            end_at_exchange=observed_at + timedelta(hours=1),
            rewards_config_json=[{"rate": "50.0"}],
        ) is True
        await _seed_passive_fill_labels(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            side=maker_leg.side,
            anchor_end=observed_at,
            count=settings.polymarket_execution_policy_passive_min_label_rows + 5,
        )
        await session.commit()

    snapshot_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/maker-economics",
        json={},
    )
    assert snapshot_response.status_code == 200
    snapshot = snapshot_response.json()
    assert snapshot["preferred_action"] in {"maker", "taker"}
    assert snapshot["taker_net_total"] is not None
    assert snapshot["status"] in {"ok", "degraded", "blocked"}
    assert "advisory_only_output" in snapshot["reason_codes_json"]
    assert snapshot["taker_gross_edge_total"] is not None
    assert snapshot["taker_fees_total"] is not None
    assert snapshot["details_json"]["fee_state"] is not None
    assert snapshot["details_json"]["reward_state"] is not None

    latest_snapshot = await client.get(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/maker-economics/latest"
    )
    assert latest_snapshot.status_code == 200
    assert latest_snapshot.json()["id"] == snapshot["id"]

    snapshot_rows = await client.get(
        f"/api/v1/ingest/polymarket/structure/maker-economics/snapshots?opportunity_id={opportunity['id']}"
    )
    assert snapshot_rows.status_code == 200
    assert snapshot_rows.json()["rows"]

    async with session_factory() as session:
        maker_leg, market_dim, asset_dim = await _maker_leg_context(session, opportunity_id=opportunity["id"])
        fee_history = (
            await session.execute(
                select(maker_module.PolymarketTokenFeeRateHistory)
                .where(maker_module.PolymarketTokenFeeRateHistory.asset_id == asset_dim.asset_id)
                .order_by(maker_module.PolymarketTokenFeeRateHistory.id.desc())
                .limit(1)
            )
        ).scalar_one()
        reward_history = (
            await session.execute(
                select(maker_module.PolymarketMarketRewardConfigHistory)
                .where(maker_module.PolymarketMarketRewardConfigHistory.condition_id == market_dim.condition_id)
                .order_by(maker_module.PolymarketMarketRewardConfigHistory.id.desc())
                .limit(1)
            )
        ).scalar_one()
        synthetic_snapshot = PolymarketMakerEconomicsSnapshot(
            opportunity_id=opportunity["id"],
            validation_id=snapshot["validation_id"],
            market_dim_id=market_dim.id,
            asset_dim_id=asset_dim.id,
            fee_history_id=fee_history.id,
            reward_history_id=reward_history.id,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id,
            context_kind="structure_opportunity",
            estimator_version="phase9_test",
            status="ok",
            preferred_action="maker",
            maker_action_type="step_ahead",
            side=maker_leg.side,
            target_size=Decimal("1.0000"),
            target_notional=Decimal("0.4000"),
            maker_fill_probability=Decimal("1.0000"),
            maker_gross_edge_total=Decimal("0.1500"),
            maker_fees_total=Decimal("0.0000"),
            maker_rewards_total=Decimal("0.0500"),
            maker_realism_adjustment_total=Decimal("0.0000"),
            maker_net_total=Decimal("0.2000"),
            taker_gross_edge_total=Decimal("0.1200"),
            taker_fees_total=Decimal("0.0100"),
            taker_rewards_total=Decimal("0.0000"),
            taker_realism_adjustment_total=Decimal("0.0000"),
            taker_net_total=Decimal("0.1100"),
            maker_advantage_total=Decimal("0.0900"),
            reason_codes_json=["advisory_only_output"],
            details_json={
                "selected_candidate": {
                    "action_type": "step_ahead",
                    "side": maker_leg.side,
                    "target_yes_price": "0.4050",
                    "entry_price": "0.4050",
                    "target_size": "1.0000",
                    "target_notional": "0.4000",
                }
            },
            input_fingerprint="phase9-test-snapshot",
            evaluated_at=observed_at,
        )
        session.add(synthetic_snapshot)
        await session.commit()

    async def _mocked_snapshot(session, *, opportunity_id, as_of=None):  # noqa: ARG001
        return {
            "id": synthetic_snapshot.id,
            "maker_advantage_total": "0.09000000",
            "maker_net_total": "0.20000000",
            "taker_net_total": "0.11000000",
        }

    monkeypatch.setattr(maker_module, "evaluate_structure_maker_economics", _mocked_snapshot)

    async with session_factory() as session:
        recommendation = await maker_module.generate_quote_recommendation(
            session,
            opportunity_id=opportunity["id"],
        )

    assert recommendation["recommendation_action"] == "recommend_quote"
    assert recommendation["comparison_winner"] == "maker"
    assert recommendation["recommended_action_type"] == "step_ahead"
    assert recommendation["recommended_side"] in {"buy_yes", "buy_no"}
    assert "policy_blocked_recommendation" not in recommendation["reason_codes_json"]

    latest_recommendation = await client.get(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/quote-recommendations/latest"
    )
    assert latest_recommendation.status_code == 200
    assert latest_recommendation.json()["id"] == str(recommendation["id"])

    recommendation_rows = await client.get(
        f"/api/v1/ingest/polymarket/structure/quote-recommendations?opportunity_id={opportunity['id']}"
    )
    assert recommendation_rows.status_code == 200
    assert recommendation_rows.json()["rows"]


@pytest.mark.asyncio
async def test_quote_optimizer_blocks_when_fee_history_is_required_but_missing(client, engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
    )
    monkeypatch.setattr(settings, "polymarket_quote_optimizer_require_fee_data", True)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase9-missing-fees",
            title="Phase9 Missing Fees",
            anchor_condition_id="phase9-missing-anchor",
            basket_condition_id="phase9-missing-basket",
        )
        await session.commit()

    opportunity = await _validated_opportunity(client, event_slug="phase9-missing-fees")
    observed_at = datetime.now(timezone.utc)

    async with session_factory() as session:
        maker_leg, market_dim, asset_dim = await _maker_leg_context(session, opportunity_id=opportunity["id"])
        maker_leg.details_json = {**(maker_leg.details_json or {}), "min_order_size": "0.01"}
        await session.flush()
        assert await insert_reward_history_if_changed(
            session,
            market_dim=market_dim,
            condition_id=market_dim.condition_id,
            source_kind=REWARD_SOURCE_KIND,
            effective_at_exchange=observed_at - timedelta(minutes=5),
            observed_at_local=observed_at,
            sync_run_id=None,
            reward_status="active",
            reward_program_id="program-2",
            reward_daily_rate=Decimal("1.0000"),
            min_incentive_size=Decimal("1.0000"),
            max_incentive_spread=Decimal("0.0500"),
            start_at_exchange=observed_at - timedelta(hours=1),
            end_at_exchange=observed_at + timedelta(hours=1),
            rewards_config_json=[{"rate": "1.0"}],
        ) is True
        await _seed_passive_fill_labels(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            side=maker_leg.side,
            anchor_end=observed_at,
            count=settings.polymarket_execution_policy_passive_min_label_rows + 2,
        )
        await session.commit()

    snapshot_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/maker-economics",
        json={},
    )
    assert snapshot_response.status_code == 200
    snapshot = snapshot_response.json()
    assert "incomplete_economics" in snapshot["reason_codes_json"]
    assert snapshot["fee_history_id"] is None

    recommendation_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/quote-recommendations",
        json={},
    )
    assert recommendation_response.status_code == 200
    recommendation = recommendation_response.json()
    assert recommendation["recommendation_action"] == "do_not_quote"
    assert recommendation["status"] == "blocked"
    assert "missing_fee_data" in recommendation["reason_codes_json"]
    assert "policy_blocked_recommendation" in recommendation["reason_codes_json"]
