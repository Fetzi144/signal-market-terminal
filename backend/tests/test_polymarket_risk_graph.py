from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.ingestion.polymarket_maker_economics as maker_module
from app.config import settings
from app.ingestion.polymarket_maker_economics import generate_quote_recommendation
from app.ingestion.polymarket_risk_graph import (
    build_risk_graph,
    compute_quote_inventory_controls,
    create_exposure_snapshot,
    list_portfolio_exposure_snapshots,
    lookup_risk_graph_edges,
    lookup_risk_graph_nodes,
)
from app.ingestion.structure_engine import PolymarketStructureEngineService
from app.models.market import Market
from app.models.market_structure import CrossVenueMarketLink
from app.models.paper_trade import PaperTrade
from app.models.polymarket_maker import PolymarketMakerEconomicsSnapshot, PolymarketQuoteRecommendation
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_risk import InventoryControlSnapshot, PortfolioOptimizerRecommendation, RiskGraphEdge, RiskGraphNode
from tests.conftest import make_signal
from tests.test_polymarket_maker_economics import _maker_leg_context, _validated_opportunity
from tests.test_structure_engine import _seed_executable_neg_risk_setup, _seed_generic_binary_market, _set_structure_defaults


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _set_phase10_defaults(monkeypatch, **overrides):
    values = {
        "polymarket_risk_graph_enabled": True,
        "polymarket_risk_graph_on_startup": False,
        "polymarket_risk_graph_interval_seconds": 300,
        "polymarket_portfolio_optimizer_enabled": True,
        "polymarket_portfolio_optimizer_interval_seconds": 300,
        "polymarket_max_event_exposure_usd": 100.0,
        "polymarket_max_entity_exposure_usd": 100.0,
        "polymarket_max_conversion_group_exposure_usd": 100.0,
        "polymarket_maker_inventory_budget_usd": 100.0,
        "polymarket_taker_inventory_budget_usd": 150.0,
        "polymarket_risk_graph_include_paper_positions": True,
        "polymarket_risk_graph_include_live_orders": True,
        "polymarket_risk_graph_include_reservations": True,
        "polymarket_no_quote_toxicity_threshold": 0.85,
        "polymarket_cross_venue_hedge_haircut_bps": 250.0,
        "polymarket_quote_optimizer_enabled": True,
        "polymarket_quote_optimizer_require_fee_data": False,
        "polymarket_quote_optimizer_require_rewards_data": False,
        "polymarket_quote_optimizer_max_age_seconds": 3600,
        "polymarket_live_trading_enabled": False,
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setattr(settings, key, value)


async def _build_groups(engine, *, reason: str = "manual", event_slug: str | None = None):
    service = PolymarketStructureEngineService(_session_factory(engine))
    try:
        return await service.build_groups(reason=reason, event_slug=event_slug)
    finally:
        await service.close()


async def _anchor_context(session: AsyncSession, *, condition_id: str) -> tuple[PolymarketMarketDim, PolymarketAssetDim, Market]:
    market_dim = (
        await session.execute(select(PolymarketMarketDim).where(PolymarketMarketDim.condition_id == condition_id))
    ).scalar_one()
    asset_dim = (
        await session.execute(select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == f"{condition_id}-yes"))
    ).scalar_one()
    market = (await session.execute(select(Market).where(Market.platform_id == f"pm-{condition_id}"))).scalar_one()
    return market_dim, asset_dim, market


async def _seed_paper_trade(
    session: AsyncSession,
    *,
    market: Market,
    outcome_id,
    direction: str,
    size_usd: str,
    entry_price: str,
) -> PaperTrade:
    signal = make_signal(
        session,
        market_id=market.id,
        outcome_id=outcome_id,
        details={"direction": direction, "phase": "phase10_test"},
    )
    await session.flush()
    size = Decimal(size_usd).quantize(Decimal("0.01"))
    price = Decimal(entry_price)
    shares = (size / price).quantize(Decimal("0.0001"))
    trade = PaperTrade(
        signal_id=signal.id,
        outcome_id=outcome_id,
        market_id=market.id,
        direction=direction,
        entry_price=price,
        size_usd=size,
        shares=shares,
        status="open",
        details={"phase": "phase10_test"},
    )
    session.add(trade)
    await session.flush()
    return trade


def _find_snapshot(rows, *, snapshot_at: datetime, node_type: str, exposure_kind: str = "aggregate", node_key_fragment: str | None = None):
    expected_snapshot_at = snapshot_at if snapshot_at.tzinfo is not None else snapshot_at.replace(tzinfo=timezone.utc)
    for row in rows:
        node = row.get("node") or {}
        observed_snapshot_at = row["snapshot_at"]
        if observed_snapshot_at.tzinfo is None:
            observed_snapshot_at = observed_snapshot_at.replace(tzinfo=timezone.utc)
        if observed_snapshot_at != expected_snapshot_at:
            continue
        if row["exposure_kind"] != exposure_kind:
            continue
        if node.get("node_type") != node_type:
            continue
        if node_key_fragment and node_key_fragment not in str(node.get("node_key")):
            continue
        return row
    raise AssertionError(f"snapshot row not found for {node_type} at {snapshot_at.isoformat()}")


@pytest.mark.asyncio
async def test_risk_graph_build_is_deterministic_and_uses_real_asset_ids(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_run_lock_enabled=False)
    _set_phase10_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase10-graph",
            title="Phase10 Graph",
            anchor_condition_id="phase10-anchor",
            basket_condition_id="phase10-basket",
        )
        market_dim, _asset_dim, _market = await _anchor_context(session, condition_id="phase10-anchor")
        market_dim.tags_json = [{"slug": "alice", "label": "Alice"}]
        kalshi_market = await _seed_generic_binary_market(
            session,
            platform="kalshi",
            platform_id="phase10-kalshi",
            question="Phase10 hedge outcome?",
            yes_asks=[["0.55", "5"]],
            no_asks=[["0.45", "5"]],
        )
        session.add(
            CrossVenueMarketLink(
                link_key="phase10-graph-link",
                left_venue="polymarket",
                left_condition_id="phase10-anchor",
                left_asset_id="phase10-anchor-yes",
                right_venue="kalshi",
                right_outcome_id=kalshi_market["yes_outcome"].id,
                mapping_kind="manual",
                provenance_source="test_fixture",
                review_status="approved",
                confidence=Decimal("0.95"),
                active=True,
            )
        )
        await session.commit()

    await _build_groups(engine, event_slug="phase10-graph")

    async with session_factory() as session:
        await build_risk_graph(session, reason="manual")
        node_count_before = (await session.execute(select(func.count(RiskGraphNode.id)))).scalar_one()
        edge_count_before = (await session.execute(select(func.count(RiskGraphEdge.id)))).scalar_one()
        await build_risk_graph(session, reason="manual")
        node_count_after = (await session.execute(select(func.count(RiskGraphNode.id)))).scalar_one()
        edge_count_after = (await session.execute(select(func.count(RiskGraphEdge.id)))).scalar_one()
        nodes = await lookup_risk_graph_nodes(session, limit=200)
        edges = await lookup_risk_graph_edges(session, limit=200)

    assert node_count_after == node_count_before
    assert edge_count_after == edge_count_before
    node_keys = {row["node_key"] for row in nodes}
    assert "event:polymarket:evt-phase10-graph" in node_keys
    assert "asset:polymarket:phase10-anchor-yes" in node_keys
    assert "entity:polymarket:alice" in node_keys
    assert any(row["node_type"] == "conversion_group" for row in nodes)
    assert any(
        row["edge_type"] == "same_entity"
        and any("alice" in str(node_key) for node_key in (row["left_node"]["node_key"], row["right_node"]["node_key"]))
        for row in edges
    )
    assert any(row["edge_type"] == "complement" for row in edges)
    assert any(row["edge_type"] == "conversion_equivalent" for row in edges)
    assert any(row["edge_type"] == "cross_venue_hedge" for row in edges)


@pytest.mark.asyncio
async def test_exposure_snapshots_roll_up_event_entity_and_conversion_group(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_run_lock_enabled=False)
    _set_phase10_defaults(monkeypatch, polymarket_max_conversion_group_exposure_usd=250.0)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase10-exposure",
            title="Phase10 Exposure",
            anchor_condition_id="phase10-exp-anchor",
            basket_condition_id="phase10-exp-basket",
        )
        market_dim, asset_dim, market = await _anchor_context(session, condition_id="phase10-exp-anchor")
        market_dim.tags_json = [{"slug": "alice", "label": "Alice"}]
        await _seed_paper_trade(
            session,
            market=market,
            outcome_id=asset_dim.outcome_id,
            direction="buy_yes",
            size_usd="60.00",
            entry_price="0.80",
        )
        await session.commit()

    await _build_groups(engine, event_slug="phase10-exposure")

    observed_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        await build_risk_graph(session, reason="manual")
        await create_exposure_snapshot(session, reason="manual", snapshot_at=observed_at)
        rows = await list_portfolio_exposure_snapshots(session, limit=200)

    event_row = _find_snapshot(rows, snapshot_at=observed_at, node_type="event", node_key_fragment="evt-phase10-exposure")
    entity_row = _find_snapshot(rows, snapshot_at=observed_at, node_type="entity", node_key_fragment="alice")
    conversion_row = _find_snapshot(rows, snapshot_at=observed_at, node_type="conversion_group")
    asset_row = _find_snapshot(rows, snapshot_at=observed_at, node_type="asset", node_key_fragment="phase10-exp-anchor-yes")

    assert event_row["gross_notional_usd"] == pytest.approx(60.0)
    assert entity_row["gross_notional_usd"] == pytest.approx(60.0)
    assert conversion_row["gross_notional_usd"] == pytest.approx(60.0)
    assert asset_row["details_json"]["source_kinds"] == ["paper_position"]


@pytest.mark.asyncio
async def test_cross_venue_hedge_relief_requires_explicit_mapping(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_run_lock_enabled=False)
    _set_phase10_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase10-hedge",
            title="Phase10 Hedge",
            anchor_condition_id="phase10-hedge-anchor",
            basket_condition_id="phase10-hedge-basket",
        )
        _market_dim, asset_dim, market = await _anchor_context(session, condition_id="phase10-hedge-anchor")
        kalshi_market = await _seed_generic_binary_market(
            session,
            platform="kalshi",
            platform_id="phase10-kalshi-hedge",
            question="Phase10 cross venue hedge?",
            yes_asks=[["0.52", "5"]],
            no_asks=[["0.48", "5"]],
        )
        await _seed_paper_trade(
            session,
            market=market,
            outcome_id=asset_dim.outcome_id,
            direction="buy_yes",
            size_usd="50.00",
            entry_price="0.80",
        )
        await _seed_paper_trade(
            session,
            market=kalshi_market["market"],
            outcome_id=kalshi_market["yes_outcome"].id,
            direction="buy_no",
            size_usd="30.00",
            entry_price="0.50",
        )
        await session.commit()

    no_link_at = datetime(2026, 4, 14, 12, 5, tzinfo=timezone.utc)
    with_link_at = no_link_at + timedelta(minutes=1)

    async with session_factory() as session:
        await build_risk_graph(session, reason="manual")
        await create_exposure_snapshot(session, reason="manual", snapshot_at=no_link_at)
        rows_without_link = await list_portfolio_exposure_snapshots(
            session,
            condition_id="phase10-hedge-anchor",
            asset_id="phase10-hedge-anchor-yes",
            limit=50,
        )
        without_link = _find_snapshot(
            rows_without_link,
            snapshot_at=no_link_at,
            node_type="asset",
            node_key_fragment="phase10-hedge-anchor-yes",
        )

        session.add(
            CrossVenueMarketLink(
                link_key="phase10-explicit-hedge",
                left_venue="polymarket",
                left_condition_id="phase10-hedge-anchor",
                left_asset_id="phase10-hedge-anchor-yes",
                right_venue="kalshi",
                right_outcome_id=kalshi_market["yes_outcome"].id,
                mapping_kind="manual",
                provenance_source="test_fixture",
                review_status="approved",
                confidence=Decimal("0.92"),
                active=True,
            )
        )
        await session.commit()
        await build_risk_graph(session, reason="manual")
        await create_exposure_snapshot(session, reason="manual", snapshot_at=with_link_at)
        rows_with_link = await list_portfolio_exposure_snapshots(
            session,
            condition_id="phase10-hedge-anchor",
            asset_id="phase10-hedge-anchor-yes",
            limit=50,
        )
        with_link = _find_snapshot(
            rows_with_link,
            snapshot_at=with_link_at,
            node_type="asset",
            node_key_fragment="phase10-hedge-anchor-yes",
        )

    assert without_link["hedged_fraction"] == pytest.approx(0.0)
    assert with_link["hedged_fraction"] > without_link["hedged_fraction"]
    assert with_link["hedged_fraction"] == pytest.approx(0.585, abs=0.02)


@pytest.mark.asyncio
async def test_quote_controls_reduce_size_and_phase9_optimizer_consumes_no_quote(engine, client, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_validation_enabled=True, polymarket_structure_run_lock_enabled=False)
    _set_phase10_defaults(
        monkeypatch,
        polymarket_max_event_exposure_usd=100.0,
        polymarket_max_conversion_group_exposure_usd=500.0,
        polymarket_max_entity_exposure_usd=500.0,
        polymarket_maker_inventory_budget_usd=100.0,
        polymarket_no_quote_toxicity_threshold=0.95,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase10-controls",
            title="Phase10 Controls",
            anchor_condition_id="phase10-ctrl-anchor",
            basket_condition_id="phase10-ctrl-basket",
        )
        _market_dim, asset_dim, market = await _anchor_context(session, condition_id="phase10-ctrl-anchor")
        await _seed_paper_trade(
            session,
            market=market,
            outcome_id=asset_dim.outcome_id,
            direction="buy_yes",
            size_usd="90.00",
            entry_price="0.80",
        )
        await session.commit()

    async with session_factory() as session:
        await build_risk_graph(session, reason="manual")
        controls = await compute_quote_inventory_controls(
            session,
            condition_id="phase10-ctrl-anchor",
            asset_id="phase10-ctrl-anchor-yes",
            recommended_side="buy_yes",
            recommended_notional=Decimal("20.00"),
        )

    assert controls["recommendation_type"] == "reduce_size"
    assert "event_cap_exceeded" in controls["reason_codes"]
    assert controls["target_size_cap_usd"] == Decimal("10.00000000")
    assert controls["quote_skew_direction"] == "bid_down"
    assert controls["reservation_price_adjustment_bps"] < Decimal("0")
    assert controls["no_quote"] is False

    opportunity = await _validated_opportunity(client, event_slug="phase10-controls")
    async with session_factory() as session:
        maker_leg, market_dim, asset_dim = await _maker_leg_context(session, opportunity_id=opportunity["id"])
        synthetic_snapshot = PolymarketMakerEconomicsSnapshot(
            opportunity_id=opportunity["id"],
            validation_id=None,
            market_dim_id=market_dim.id,
            asset_dim_id=asset_dim.id,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id,
            context_kind="structure_opportunity",
            estimator_version="phase10_test",
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
            input_fingerprint="phase10-phase9-snapshot",
            evaluated_at=datetime.now(timezone.utc),
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
    monkeypatch.setattr(settings, "polymarket_maker_inventory_budget_usd", 0.20)
    monkeypatch.setattr(settings, "polymarket_no_quote_toxicity_threshold", 0.85)

    async with session_factory() as session:
        recommendation = await generate_quote_recommendation(session, opportunity_id=opportunity["id"])

    assert recommendation["recommendation_action"] == "do_not_quote"
    assert "inventory_toxicity_exceeded" in recommendation["reason_codes_json"]
    assert "phase10_no_quote" in recommendation["reason_codes_json"]
    assert "policy_blocked_recommendation" in recommendation["reason_codes_json"]
    assert recommendation["details_json"]["risk_controls"]["no_quote"] is True


@pytest.mark.asyncio
async def test_manual_risk_api_and_health_include_phase10(engine, client, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_run_lock_enabled=False)
    _set_phase10_defaults(monkeypatch, polymarket_maker_inventory_budget_usd=50.0)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase10-api",
            title="Phase10 API",
            anchor_condition_id="phase10-api-anchor",
            basket_condition_id="phase10-api-basket",
        )
        session.add(
            PolymarketQuoteRecommendation(
                condition_id="phase10-api-anchor",
                asset_id="phase10-api-anchor-yes",
                recommendation_kind="advisory_quote",
                status="ok",
                comparison_winner="maker",
                recommendation_action="recommend_quote",
                recommended_action_type="step_ahead",
                recommended_side="buy_yes",
                recommended_yes_price=Decimal("0.4050"),
                recommended_entry_price=Decimal("0.4050"),
                recommended_size=Decimal("1.0000"),
                recommended_notional=Decimal("0.4000"),
                price_offset_ticks=1,
                reason_codes_json=["advisory_only_output"],
                summary_json={"advisory_only": True},
                details_json={"phase": "phase10_test"},
                input_fingerprint="phase10-api-quote",
            )
        )
        await session.commit()

    build_response = await client.post("/api/v1/ingest/polymarket/risk/graph/build", json={"reason": "manual"})
    snapshot_response = await client.post(
        "/api/v1/ingest/polymarket/risk/graph/snapshot",
        json={"reason": "manual", "snapshot_at": "2026-04-14T13:00:00Z"},
    )
    optimize_response = await client.post(
        "/api/v1/ingest/polymarket/risk/graph/optimize",
        json={"reason": "manual", "snapshot_at": "2026-04-14T13:00:00Z"},
    )

    assert build_response.status_code == 200
    assert snapshot_response.status_code == 200
    assert optimize_response.status_code == 200

    status_response = await client.get("/api/v1/ingest/polymarket/risk/status")
    nodes_response = await client.get("/api/v1/ingest/polymarket/risk/nodes?node_type=asset")
    edges_response = await client.get("/api/v1/ingest/polymarket/risk/edges")
    snapshots_response = await client.get("/api/v1/ingest/polymarket/risk/exposure-snapshots?condition_id=phase10-api-anchor")
    recommendations_response = await client.get("/api/v1/ingest/polymarket/risk/optimizer-recommendations?condition_id=phase10-api-anchor")
    controls_response = await client.get("/api/v1/ingest/polymarket/risk/inventory-controls?condition_id=phase10-api-anchor")
    health_response = await client.get("/api/v1/health")

    assert status_response.status_code == 200
    assert status_response.json()["last_optimizer_status"] == "completed"
    assert status_response.json()["advisory_only"] is True
    assert nodes_response.status_code == 200
    assert nodes_response.json()["rows"]
    assert edges_response.status_code == 200
    assert edges_response.json()["rows"]
    assert snapshots_response.status_code == 200
    assert snapshots_response.json()["rows"]
    assert recommendations_response.status_code == 200
    assert recommendations_response.json()["rows"]
    assert controls_response.status_code == 200
    assert controls_response.json()["rows"]
    assert health_response.status_code == 200
    phase10 = health_response.json()["polymarket_phase10"]
    assert phase10["enabled"] is True
    assert phase10["last_graph_build_status"] == "completed"
    assert phase10["last_optimizer_status"] == "completed"
