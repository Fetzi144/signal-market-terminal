from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.structure_engine import (
    STRUCTURE_ENGINE_LEASE_NAME,
    PolymarketStructureEngineService,
    fetch_market_structure_status,
    trigger_manual_structure_paper_plan_create,
    trigger_manual_structure_paper_plan_route,
)
from app.ingestion.structure_phase8b import (
    VALIDATION_BLOCKED,
    VALIDATION_EXECUTABLE,
    VALIDATION_INFORMATIONAL,
    approve_market_structure_paper_plan,
    create_market_structure_paper_plan,
    get_market_structure_opportunity_detail,
    get_market_structure_paper_plan_detail,
    route_market_structure_paper_plan,
)
from app.jobs.lease import acquire_named_lease, release_named_lease
from app.models.market_structure import (
    CrossVenueMarketLink,
    MarketStructureGroup,
    MarketStructureGroupMember,
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
    MarketStructurePaperOrder,
    MarketStructurePaperOrderEvent,
    MarketStructurePaperPlan,
    MarketStructureRun,
    MarketStructureValidation,
)
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketEventDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
)
from app.models.polymarket_raw import PolymarketBookSnapshot
from app.models.polymarket_reconstruction import PolymarketBookReconState
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome

FIXED_NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _decimal(value: str | int | float | Decimal) -> Decimal:
    return Decimal(str(value))


def _book_payload(levels: list[tuple[str, str]]) -> list[list[str]]:
    return [[str(price), str(size)] for price, size in levels]


def _fresh_now() -> datetime:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0)


def _set_structure_defaults(monkeypatch, **overrides):
    values = {
        "polymarket_structure_engine_enabled": True,
        "polymarket_structure_on_startup": False,
        "polymarket_structure_interval_seconds": 300,
        "polymarket_structure_min_net_edge_bps": 0.0,
        "polymarket_structure_require_executable_all_legs": True,
        "polymarket_structure_include_cross_venue": False,
        "polymarket_structure_allow_augmented_neg_risk": False,
        "polymarket_structure_max_groups_per_run": 250,
        "polymarket_structure_cross_venue_max_staleness_seconds": 180,
        "polymarket_structure_max_leg_slippage_bps": 150.0,
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setattr(settings, key, value)


def _adverse_book_for_side(side: str) -> tuple[Decimal, Decimal]:
    if side == "buy_no":
        return Decimal("0.04"), Decimal("0.05")
    return Decimal("0.94"), Decimal("0.95")


async def _seed_event(
    session: AsyncSession,
    *,
    slug: str,
    title: str,
    neg_risk: bool = False,
    active: bool = True,
    source_payload_json: dict | None = None,
) -> PolymarketEventDim:
    now = _fresh_now()
    event = PolymarketEventDim(
        gamma_event_id=f"evt-{slug}",
        event_slug=slug,
        event_ticker=slug.upper(),
        title=title,
        category="Politics",
        active=active,
        closed=False,
        archived=False,
        neg_risk=neg_risk,
        last_gamma_sync_at=now,
        source_payload_json=source_payload_json or {},
    )
    session.add(event)
    await session.flush()
    return event


async def _seed_polymarket_binary_market(
    session: AsyncSession,
    *,
    event: PolymarketEventDim,
    condition_id: str,
    question: str,
    yes_asset_id: str,
    no_asset_id: str,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    fee_rate: str = "0",
    min_order_size: str = "1",
    tick_size: str = "0.01",
    source_payload_json: dict | None = None,
    active: bool = True,
) -> dict[str, object]:
    now = _fresh_now()
    market = make_market(
        session,
        platform="polymarket",
        platform_id=f"pm-{condition_id}",
        question=question,
        question_slug=question.lower().replace(" ", "-"),
        active=active,
    )
    await session.flush()
    yes_outcome = make_outcome(session, market.id, name="Yes", token_id=yes_asset_id)
    no_outcome = make_outcome(session, market.id, name="No", token_id=no_asset_id)
    await session.flush()

    fee_rate_decimal = _decimal(fee_rate)
    fees_enabled = fee_rate_decimal > Decimal("0")

    market_dim = PolymarketMarketDim(
        gamma_market_id=f"gamma-{condition_id}",
        condition_id=condition_id,
        market_slug=condition_id,
        question=question,
        description=question,
        event_dim_id=event.id,
        enable_order_book=True,
        active=active,
        closed=False,
        archived=False,
        accepting_orders=active,
        resolved=False,
        resolution_state="open",
        fees_enabled=fees_enabled,
        fee_schedule_json={"rate": str(fee_rate_decimal)},
        maker_base_fee=Decimal("0"),
        taker_base_fee=fee_rate_decimal,
        last_gamma_sync_at=now,
        source_payload_json=source_payload_json or {},
    )
    session.add(market_dim)
    await session.flush()

    yes_asset = PolymarketAssetDim(
        asset_id=yes_asset_id,
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=yes_outcome.id,
        outcome_name="Yes",
        outcome_index=0,
        active=active,
        last_gamma_sync_at=now,
        source_payload_json={"asset_id": yes_asset_id},
    )
    no_asset = PolymarketAssetDim(
        asset_id=no_asset_id,
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=no_outcome.id,
        outcome_name="No",
        outcome_index=1,
        active=active,
        last_gamma_sync_at=now,
        source_payload_json={"asset_id": no_asset_id},
    )
    session.add_all([yes_asset, no_asset])
    await session.flush()

    for asset in (yes_asset, no_asset):
        session.add(
            PolymarketMarketParamHistory(
                market_dim_id=market_dim.id,
                asset_dim_id=asset.id,
                condition_id=condition_id,
                asset_id=asset.asset_id,
                source_kind="gamma_sync",
                effective_at_exchange=now,
                observed_at_local=now,
                tick_size=_decimal(tick_size),
                min_order_size=_decimal(min_order_size),
                neg_risk=event.neg_risk,
                fees_enabled=fees_enabled,
                fee_schedule_json={"rate": str(fee_rate_decimal)},
                maker_base_fee=Decimal("0"),
                taker_base_fee=fee_rate_decimal,
                resolution_state="open",
                fingerprint=f"{condition_id}-{asset.asset_id}-params",
                details_json={"source": "test"},
            )
        )

    best_bid = _decimal(bids[0][0])
    best_ask = _decimal(asks[0][0])
    snapshot = PolymarketBookSnapshot(
        market_dim_id=market_dim.id,
        asset_dim_id=yes_asset.id,
        condition_id=condition_id,
        asset_id=yes_asset.asset_id,
        source_kind="ws_book",
        event_ts_exchange=now,
        recv_ts_local=now,
        observed_at_local=now,
        bids_json=_book_payload(bids),
        asks_json=_book_payload(asks),
        min_order_size=_decimal(min_order_size),
        tick_size=_decimal(tick_size),
        best_bid=best_bid,
        best_ask=best_ask,
        spread=(best_ask - best_bid),
        source_payload_json={"source": "test"},
    )
    session.add(snapshot)
    await session.flush()

    session.add(
        PolymarketBookReconState(
            market_dim_id=market_dim.id,
            asset_dim_id=yes_asset.id,
            condition_id=condition_id,
            asset_id=yes_asset.asset_id,
            status="live",
            last_snapshot_id=snapshot.id,
            last_snapshot_source_kind="ws_book",
            last_snapshot_hash=f"{condition_id}-snapshot",
            last_snapshot_exchange_ts=now,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=(best_ask - best_bid),
            depth_levels_bid=len(bids),
            depth_levels_ask=len(asks),
            expected_tick_size=_decimal(tick_size),
            last_exchange_ts=now,
            last_received_at_local=now,
            last_reconciled_at=now,
            details_json={"source": "test"},
        )
    )
    await session.flush()

    return {
        "market": market,
        "market_dim": market_dim,
        "yes_outcome": yes_outcome,
        "no_outcome": no_outcome,
        "yes_asset": yes_asset,
        "no_asset": no_asset,
    }


async def _seed_generic_binary_market(
    session: AsyncSession,
    *,
    platform: str,
    platform_id: str,
    question: str,
    yes_asks: list[list[str]],
    no_asks: list[list[str]],
    yes_bids: list[list[str]] | None = None,
    no_bids: list[list[str]] | None = None,
) -> dict[str, object]:
    now = _fresh_now()
    market = make_market(
        session,
        platform=platform,
        platform_id=platform_id,
        question=question,
        question_slug=question.lower().replace(" ", "-"),
        active=True,
    )
    await session.flush()
    yes_outcome = make_outcome(session, market.id, name="Yes", token_id=f"{platform_id}-yes")
    no_outcome = make_outcome(session, market.id, name="No", token_id=f"{platform_id}-no")
    await session.flush()
    make_orderbook_snapshot(
        session,
        yes_outcome.id,
        spread=None,
        bids=yes_bids or [],
        asks=yes_asks,
        captured_at=now,
    )
    make_orderbook_snapshot(
        session,
        no_outcome.id,
        spread=None,
        bids=no_bids or [],
        asks=no_asks,
        captured_at=now,
    )
    await session.flush()
    return {
        "market": market,
        "yes_outcome": yes_outcome,
        "no_outcome": no_outcome,
    }


async def _seed_executable_neg_risk_setup(
    session: AsyncSession,
    *,
    slug: str,
    title: str,
    anchor_condition_id: str,
    basket_condition_id: str,
) -> None:
    event = await _seed_event(
        session,
        slug=slug,
        title=title,
        neg_risk=True,
    )
    await _seed_polymarket_binary_market(
        session,
        event=event,
        condition_id=anchor_condition_id,
        question=f"{title} Anchor?",
        yes_asset_id=f"{anchor_condition_id}-yes",
        no_asset_id=f"{anchor_condition_id}-no",
        bids=[("0.80", "5")],
        asks=[("0.81", "5")],
    )
    await _seed_polymarket_binary_market(
        session,
        event=event,
        condition_id=basket_condition_id,
        question=f"{title} Basket?",
        yes_asset_id=f"{basket_condition_id}-yes",
        no_asset_id=f"{basket_condition_id}-no",
        bids=[("0.15", "5")],
        asks=[("0.16", "5")],
    )


@pytest.mark.asyncio
async def test_structure_group_builds_standard_neg_risk_and_is_idempotent(engine, monkeypatch):
    _set_structure_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = await _seed_event(
            session,
            slug="neg-risk-primary",
            title="2028 Party Nominee",
            neg_risk=True,
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-a",
            question="Will Alice win the nomination?",
            yes_asset_id="alice-yes",
            no_asset_id="alice-no",
            bids=[("0.80", "5")],
            asks=[("0.82", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-b",
            question="Will Bob win the nomination?",
            yes_asset_id="bob-yes",
            no_asset_id="bob-no",
            bids=[("0.15", "5")],
            asks=[("0.17", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-c",
            question="Will Carol win the nomination?",
            yes_asset_id="carol-yes",
            no_asset_id="carol-no",
            bids=[("0.10", "5")],
            asks=[("0.12", "5")],
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    async with session_factory() as session:
        first_group_count = (await session.execute(select(func.count(MarketStructureGroup.id)))).scalar_one()
        first_member_count = (await session.execute(select(func.count(MarketStructureGroupMember.id)))).scalar_one()
        neg_risk_group = (
            await session.execute(
                select(MarketStructureGroup).where(MarketStructureGroup.group_type == "neg_risk_event")
            )
        ).scalar_one()
        assert neg_risk_group.actionable is True
        assert neg_risk_group.details_json["named_outcome_count"] == 3
        assert neg_risk_group.details_json["has_augmented_members"] is False

    await service.build_groups(reason="manual")

    async with session_factory() as session:
        second_group_count = (await session.execute(select(func.count(MarketStructureGroup.id)))).scalar_one()
        second_member_count = (await session.execute(select(func.count(MarketStructureGroupMember.id)))).scalar_one()
        complement_count = (
            await session.execute(
                select(func.count(MarketStructureGroup.id)).where(MarketStructureGroup.group_type == "binary_complement")
            )
        ).scalar_one()
        assert first_group_count == 5
        assert second_group_count == first_group_count
        assert first_member_count == 15
        assert second_member_count == first_member_count
        assert complement_count == 3

    await service.close()


@pytest.mark.asyncio
async def test_structure_scan_detects_neg_risk_direct_vs_basket_and_includes_fees(engine, monkeypatch):
    _set_structure_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = await _seed_event(
            session,
            slug="neg-risk-pricing",
            title="2028 Party Nominee Pricing",
            neg_risk=True,
        )
        anchor = await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-anchor",
            question="Will Alice win the nomination?",
            yes_asset_id="anchor-yes",
            no_asset_id="anchor-no",
            bids=[("0.90", "5")],
            asks=[("0.91", "5")],
            fee_rate="0.02",
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-rival-1",
            question="Will Bob win the nomination?",
            yes_asset_id="bob-yes",
            no_asset_id="bob-no",
            bids=[("0.29", "5")],
            asks=[("0.30", "5")],
            fee_rate="0.02",
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-rival-2",
            question="Will Carol win the nomination?",
            yes_asset_id="carol-yes",
            no_asset_id="carol-no",
            bids=[("0.31", "5")],
            asks=[("0.32", "5")],
            fee_rate="0.02",
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual", group_type="neg_risk_event")

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(
                    MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket",
                    MarketStructureOpportunity.anchor_condition_id == anchor["yes_asset"].condition_id,
                )
                .order_by(MarketStructureOpportunity.id.desc())
            )
        ).scalars().first()
        assert opportunity is not None
        assert opportunity.actionable is True
        assert opportunity.executable_all_legs is True
        assert opportunity.details_json["preferred_package"] == "direct"
        assert opportunity.gross_edge_total != opportunity.net_edge_total

        legs = (
            await session.execute(
                select(MarketStructureOpportunityLeg)
                .where(MarketStructureOpportunityLeg.opportunity_id == opportunity.id)
                .order_by(MarketStructureOpportunityLeg.leg_index.asc())
            )
        ).scalars().all()
        assert len(legs) == 3
        assert all(leg.valid for leg in legs)
        assert all((leg.est_fee or Decimal("0")) > Decimal("0") for leg in legs)
        assert any(leg.role == "direct_leg" and leg.side == "buy_no" for leg in legs)

    await service.close()


@pytest.mark.asyncio
async def test_augmented_neg_risk_members_visible_but_non_actionable(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_allow_augmented_neg_risk=False)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = await _seed_event(
            session,
            slug="neg-risk-augmented",
            title="Augmented Event",
            neg_risk=True,
            source_payload_json={"enableNegRisk": True},
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-aug-alice",
            question="Will Alice win?",
            yes_asset_id="aug-alice-yes",
            no_asset_id="aug-alice-no",
            bids=[("0.40", "5")],
            asks=[("0.41", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-aug-bob",
            question="Will Bob win?",
            yes_asset_id="aug-bob-yes",
            no_asset_id="aug-bob-no",
            bids=[("0.35", "5")],
            asks=[("0.36", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-aug-other",
            question="Other",
            yes_asset_id="aug-other-yes",
            no_asset_id="aug-other-no",
            bids=[("0.05", "5")],
            asks=[("0.06", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-aug-placeholder",
            question="Placeholder candidate",
            yes_asset_id="aug-placeholder-yes",
            no_asset_id="aug-placeholder-no",
            bids=[("0.03", "5")],
            asks=[("0.04", "5")],
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual", group_type="neg_risk_event")

    async with session_factory() as session:
        neg_risk_group = (
            await session.execute(
                select(MarketStructureGroup).where(MarketStructureGroup.group_type == "neg_risk_event")
            )
        ).scalar_one()
        members = (
            await session.execute(
                select(MarketStructureGroupMember)
                .where(MarketStructureGroupMember.group_id == neg_risk_group.id)
                .order_by(MarketStructureGroupMember.member_key.asc())
            )
        ).scalars().all()
        status = await fetch_market_structure_status(session)
        opportunities = (
            await session.execute(
                select(MarketStructureOpportunity).where(
                    MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket"
                )
            )
        ).scalars().all()

        assert neg_risk_group.actionable is False
        assert neg_risk_group.details_json["has_augmented_members"] is True
        assert status["informational_augmented_group_count"] == 1
        assert opportunities == []
        assert {member.member_role for member in members} >= {"named_outcome", "other", "placeholder", "binary_no"}
        assert all(member.actionable is False for member in members)

    await service.close()


@pytest.mark.asyncio
async def test_binary_complement_and_event_sum_parity_detection(engine, monkeypatch):
    _set_structure_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        complement_event = await _seed_event(
            session,
            slug="complement-event",
            title="Complement Event",
            neg_risk=False,
        )
        await _seed_polymarket_binary_market(
            session,
            event=complement_event,
            condition_id="cond-complement",
            question="Will the bill pass?",
            yes_asset_id="complement-yes",
            no_asset_id="complement-no",
            bids=[("0.52", "5")],
            asks=[("0.49", "5")],
        )

        parity_event = await _seed_event(
            session,
            slug="parity-event",
            title="Parity Event",
            neg_risk=False,
        )
        await _seed_polymarket_binary_market(
            session,
            event=parity_event,
            condition_id="cond-party-a",
            question="Will Party A win?",
            yes_asset_id="party-a-yes",
            no_asset_id="party-a-no",
            bids=[("0.24", "5")],
            asks=[("0.25", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=parity_event,
            condition_id="cond-party-b",
            question="Will Party B win?",
            yes_asset_id="party-b-yes",
            no_asset_id="party-b-no",
            bids=[("0.29", "5")],
            asks=[("0.30", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=parity_event,
            condition_id="cond-party-c",
            question="Will Party C win?",
            yes_asset_id="party-c-yes",
            no_asset_id="party-c-no",
            bids=[("0.19", "5")],
            asks=[("0.20", "5")],
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual")

    async with session_factory() as session:
        complement = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(
                    MarketStructureOpportunity.opportunity_type == "binary_complement",
                    MarketStructureOpportunity.anchor_condition_id == "cond-complement",
                )
                .order_by(MarketStructureOpportunity.id.desc())
            )
        ).scalars().first()
        parity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.opportunity_type == "event_sum_parity")
                .order_by(MarketStructureOpportunity.id.desc())
            )
        ).scalars().first()

        assert complement is not None
        assert complement.actionable is True
        assert complement.net_edge_total > Decimal("0")

        assert parity is not None
        assert parity.actionable is True
        assert parity.net_edge_total > Decimal("0")
        assert parity.details_json["named_leg_count"] == 3

    await service.close()


@pytest.mark.asyncio
async def test_structure_scan_rejects_non_executable_leg_on_slippage(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_max_leg_slippage_bps=50.0)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = await _seed_event(
            session,
            slug="slippage-event",
            title="Slippage Event",
            neg_risk=True,
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-slip-anchor",
            question="Will Alice win?",
            yes_asset_id="slip-anchor-yes",
            no_asset_id="slip-anchor-no",
            bids=[("0.75", "5")],
            asks=[("0.76", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-slip-basket",
            question="Will Bob win?",
            yes_asset_id="slip-basket-yes",
            no_asset_id="slip-basket-no",
            bids=[("0.19", "5")],
            asks=[("0.20", "0.40"), ("0.50", "0.60")],
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual", group_type="neg_risk_event")

    async with session_factory() as session:
        latest = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(
                    MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket",
                    MarketStructureOpportunity.anchor_condition_id == "cond-slip-anchor",
                )
                .order_by(MarketStructureOpportunity.id.desc())
            )
        ).scalars().first()
        legs = (
            await session.execute(
                select(MarketStructureOpportunityLeg)
                .where(MarketStructureOpportunityLeg.opportunity_id == latest.id)
                .order_by(MarketStructureOpportunityLeg.leg_index.asc())
            )
        ).scalars().all()

        assert latest.actionable is False
        assert latest.executable_all_legs is False
        assert latest.invalid_reason == "leg_slippage_too_high"
        assert any(leg.invalid_reason == "leg_slippage_too_high" for leg in legs)

    await service.close()


@pytest.mark.asyncio
async def test_cross_venue_basis_requires_explicit_link_and_executable_legs(engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_include_cross_venue=True,
        polymarket_structure_cross_venue_max_staleness_seconds=7200,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        left = await _seed_generic_binary_market(
            session,
            platform="manifold",
            platform_id="cross-left",
            question="Will Candidate A win the state?",
            yes_asks=[["0.40", "5"]],
            no_asks=[["0.62", "5"]],
        )
        right = await _seed_generic_binary_market(
            session,
            platform="kalshi",
            platform_id="cross-right",
            question="Will Candidate A win the state?",
            yes_asks=[["0.72", "5"]],
            no_asks=[["0.45", "5"]],
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    async with session_factory() as session:
        no_link_groups = (
            await session.execute(
                select(func.count(MarketStructureGroup.id)).where(
                    MarketStructureGroup.group_type == "cross_venue_basis"
                )
            )
        ).scalar_one()
        assert no_link_groups == 0

        session.add(
                CrossVenueMarketLink(
                    link_key="pm-kalshi-candidate-a",
                    left_venue="manifold",
                    left_market_id=left["market"].id,
                    left_outcome_id=left["yes_outcome"].id,
                    left_external_id="cross-left",
                right_venue="kalshi",
                right_market_id=right["market"].id,
                right_outcome_id=right["yes_outcome"].id,
                right_external_id="cross-right",
                mapping_kind="manual",
                active=True,
                details_json={
                    "left": {"taker_fee_rate": "0.00", "min_order_size": "1"},
                    "right": {"taker_fee_rate": "0.00", "min_order_size": "1"},
                },
            )
        )
        await session.commit()

    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual", group_type="cross_venue_basis")

    async with session_factory() as session:
        basis_group = (
            await session.execute(
                select(MarketStructureGroup).where(MarketStructureGroup.group_type == "cross_venue_basis")
            )
        ).scalar_one()
        basis = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(
                    MarketStructureOpportunity.group_id == basis_group.id,
                    MarketStructureOpportunity.opportunity_type == "cross_venue_basis",
                )
                .order_by(MarketStructureOpportunity.id.desc())
            )
        ).scalars().first()

        assert basis is not None
        assert basis.actionable is True
        assert basis.executable_all_legs is True
        assert basis.details_json["chosen_direction"] == "left_yes_vs_right_no"

    await service.close()


@pytest.mark.asyncio
async def test_structure_operator_apis_and_health_serialization(client, engine, monkeypatch):
    _set_structure_defaults(monkeypatch)
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = await _seed_event(
            session,
            slug="api-event",
            title="API Event",
            neg_risk=True,
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-api-a",
            question="Will Alice win?",
            yes_asset_id="api-a-yes",
            no_asset_id="api-a-no",
            bids=[("0.80", "5")],
            asks=[("0.81", "5")],
        )
        await _seed_polymarket_binary_market(
            session,
            event=event,
            condition_id="cond-api-b",
            question="Will Bob win?",
            yes_asset_id="api-b-yes",
            no_asset_id="api-b-no",
            bids=[("0.15", "5")],
            asks=[("0.16", "5")],
        )
        await session.commit()

    build_response = await client.post(
        "/api/v1/ingest/polymarket/structure/groups/build",
        json={"reason": "manual", "event_slug": "api-event"},
    )
    assert build_response.status_code == 200
    assert build_response.json()["status"] == "completed"

    scan_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/scan",
        json={"reason": "manual", "event_slug": "api-event"},
    )
    assert scan_response.status_code == 200
    assert scan_response.json()["status"] == "completed"

    status_response = await client.get("/api/v1/ingest/polymarket/structure/status")
    groups_response = await client.get("/api/v1/ingest/polymarket/structure/groups?event_slug=api-event")
    members_response = await client.get("/api/v1/ingest/polymarket/structure/group-members?event_slug=api-event")
    opportunities_response = await client.get(
        "/api/v1/ingest/polymarket/structure/opportunities?event_slug=api-event"
    )

    assert status_response.status_code == 200
    assert groups_response.status_code == 200
    assert members_response.status_code == 200
    assert opportunities_response.status_code == 200
    assert status_response.json()["active_group_counts"]["neg_risk_event"] == 1
    assert groups_response.json()["rows"]
    assert members_response.json()["rows"]
    assert opportunities_response.json()["rows"]

    opportunity_id = opportunities_response.json()["rows"][0]["id"]
    legs_response = await client.get(f"/api/v1/ingest/polymarket/structure/legs?opportunity_id={opportunity_id}")
    health_response = await client.get("/api/v1/health")

    assert legs_response.status_code == 200
    assert legs_response.json()["rows"]
    assert health_response.status_code == 200
    assert "polymarket_phase8a" in health_response.json()
    assert health_response.json()["polymarket_phase8a"]["last_scan_status"] == "completed"


@pytest.mark.asyncio
async def test_structure_run_lock_prevents_overlapping_manual_runs(engine, monkeypatch):
    _set_structure_defaults(monkeypatch)
    session_factory = _session_factory(engine)
    foreign_owner = "phase8b-test-lock"
    lease_acquired = await acquire_named_lease(
        session_factory,
        lease_name=STRUCTURE_ENGINE_LEASE_NAME,
        owner_token=foreign_owner,
        lease_seconds=120,
    )
    assert lease_acquired is True

    service = PolymarketStructureEngineService(session_factory)
    try:
        run = await service.build_groups(reason="manual")
        assert run["status"] == "blocked"
        assert run["details_json"]["reason"] == "lock_unavailable"

        async with session_factory() as session:
            latest_run = (
                await session.execute(
                    select(MarketStructureRun)
                    .where(MarketStructureRun.run_type == "group_build")
                    .order_by(MarketStructureRun.started_at.desc())
                    .limit(1)
                )
            ).scalar_one()
            assert latest_run.status == "blocked"
            assert latest_run.details_json["reason"] == "lock_unavailable"
    finally:
        await release_named_lease(
            session_factory,
            lease_name=STRUCTURE_ENGINE_LEASE_NAME,
            owner_token=foreign_owner,
        )
        await service.close()


@pytest.mark.asyncio
async def test_structure_validation_and_manual_paper_routing_audit_trail(engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
        polymarket_structure_paper_require_manual_approval=True,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase8b-audit",
            title="Phase8B Audit",
            anchor_condition_id="audit-anchor",
            basket_condition_id="audit-basket",
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual", event_slug="phase8b-audit")
    scan_run = await service.scan_opportunities(reason="manual", event_slug="phase8b-audit")
    await service.validate_opportunities(reason="manual", scan_run_id=scan_run["id"])

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket")
                .order_by(MarketStructureOpportunity.id.desc())
                .limit(1)
            )
        ).scalar_one()
        detail = await get_market_structure_opportunity_detail(session, opportunity_id=opportunity.id)
        assert detail is not None
        assert detail["latest_validation"]["classification"] == VALIDATION_EXECUTABLE

    created_plan = await trigger_manual_structure_paper_plan_create(
        session_factory,
        opportunity_id=opportunity.id,
        actor="operator",
    )
    assert created_plan["status"] == "approval_pending"
    assert created_plan["manual_approval_required"] is True

    gated_route = await trigger_manual_structure_paper_plan_route(
        session_factory,
        plan_id=created_plan["id"],
        actor="operator",
    )
    assert gated_route["status"] == "approval_pending"

    async with session_factory() as session:
        approved_plan = await approve_market_structure_paper_plan(
            session,
            plan_id=created_plan["id"],
            actor="operator",
        )
        await session.commit()
        await session.refresh(approved_plan)
        assert approved_plan.status == "routing_pending"

    routed_plan = await trigger_manual_structure_paper_plan_route(
        session_factory,
        plan_id=created_plan["id"],
        actor="operator",
    )
    assert routed_plan["status"] == "routed"

    async with session_factory() as session:
        plan_detail = await get_market_structure_paper_plan_detail(session, plan_id=created_plan["id"])
        assert plan_detail is not None
        assert all(order["status"] == "filled" for order in plan_detail["orders"])
        event_types = [event["event_type"] for event in plan_detail["events"]]
        assert "plan_created" in event_types
        assert "plan_approved" in event_types
        assert "plan_routed" in event_types

        status = await fetch_market_structure_status(session)
        assert status["last_validation_status"] == "completed"
        assert status["last_paper_plan_status"] == "completed"
        assert status["last_paper_route_status"] == "completed"

    await service.close()


@pytest.mark.asyncio
async def test_structure_validation_classifies_informational_when_edge_decays(engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase8b-informational",
            title="Phase8B Informational",
            anchor_condition_id="inform-anchor",
            basket_condition_id="inform-basket",
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual", event_slug="phase8b-informational")
    await service.scan_opportunities(reason="manual", event_slug="phase8b-informational")

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket")
                .order_by(MarketStructureOpportunity.id.desc())
                .limit(1)
            )
        ).scalar_one()
        legs = (
            await session.execute(
                select(MarketStructureOpportunityLeg)
                .where(MarketStructureOpportunityLeg.opportunity_id == opportunity.id)
                .order_by(MarketStructureOpportunityLeg.leg_index.asc())
            )
        ).scalars().all()
        current_now = _fresh_now()
        for leg in legs:
            adverse_bid, adverse_ask = _adverse_book_for_side(leg.side)
            snapshot = (
                await session.execute(
                    select(PolymarketBookSnapshot)
                    .where(
                        PolymarketBookSnapshot.asset_dim_id == leg.asset_dim_id
                        if leg.asset_dim_id is not None
                        else PolymarketBookSnapshot.asset_id == leg.asset_id
                    )
                    .order_by(PolymarketBookSnapshot.observed_at_local.desc(), PolymarketBookSnapshot.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if snapshot is None and leg.market_dim_id is not None:
                snapshot = (
                    await session.execute(
                        select(PolymarketBookSnapshot)
                        .where(PolymarketBookSnapshot.market_dim_id == leg.market_dim_id)
                        .order_by(PolymarketBookSnapshot.observed_at_local.desc(), PolymarketBookSnapshot.id.desc())
                        .limit(1)
                    )
                ).scalar_one()
            snapshot.bids_json = _book_payload([(str(adverse_bid), "10")])
            snapshot.asks_json = _book_payload([(str(adverse_ask), "10")])
            snapshot.best_bid = adverse_bid
            snapshot.best_ask = adverse_ask
            snapshot.spread = adverse_ask - adverse_bid
            snapshot.recv_ts_local = current_now
            snapshot.observed_at_local = current_now

            recon = (
                await session.execute(
                    select(PolymarketBookReconState)
                    .where(
                        PolymarketBookReconState.asset_dim_id == leg.asset_dim_id
                        if leg.asset_dim_id is not None
                        else PolymarketBookReconState.asset_id == leg.asset_id
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if recon is None and leg.market_dim_id is not None:
                recon = (
                    await session.execute(
                        select(PolymarketBookReconState)
                        .where(PolymarketBookReconState.market_dim_id == leg.market_dim_id)
                        .limit(1)
                    )
                ).scalar_one()
            recon.best_bid = adverse_bid
            recon.best_ask = adverse_ask
            recon.last_snapshot_exchange_ts = current_now
            recon.last_received_at_local = current_now
            recon.last_exchange_ts = current_now
            recon.last_reconciled_at = current_now
        await session.commit()

    monkeypatch.setattr(settings, "polymarket_structure_min_net_edge_bps", 5000.0)
    await service.validate_opportunities(reason="manual", opportunity_id=opportunity.id)

    async with session_factory() as session:
        validation = (
            await session.execute(
                select(MarketStructureValidation)
                .where(MarketStructureValidation.opportunity_id == opportunity.id)
                .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
                .limit(1)
            )
            ).scalar_one()
        assert validation.classification == VALIDATION_INFORMATIONAL
        assert any(
            code in {"no_positive_current_edge", "edge_decayed_below_threshold"}
            for code in validation.reason_codes_json
        )

    await service.close()


@pytest.mark.asyncio
async def test_cross_venue_validation_blocks_review_required_links(engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_include_cross_venue=True,
        polymarket_structure_link_review_required=True,
        polymarket_structure_cross_venue_max_staleness_seconds=7200,
        polymarket_structure_validation_enabled=True,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        left = await _seed_generic_binary_market(
            session,
            platform="manifold",
            platform_id="phase8b-left",
            question="Will Candidate B win the state?",
            yes_asks=[["0.41", "5"]],
            no_asks=[["0.63", "5"]],
        )
        right = await _seed_generic_binary_market(
            session,
            platform="kalshi",
            platform_id="phase8b-right",
            question="Will Candidate B win the state?",
            yes_asks=[["0.71", "5"]],
            no_asks=[["0.46", "5"]],
        )
        session.add(
            CrossVenueMarketLink(
                link_key="phase8b-cross-review",
                left_venue="manifold",
                left_market_id=left["market"].id,
                left_outcome_id=left["yes_outcome"].id,
                left_external_id="phase8b-left",
                right_venue="kalshi",
                right_market_id=right["market"].id,
                right_outcome_id=right["yes_outcome"].id,
                right_external_id="phase8b-right",
                mapping_kind="manual",
                provenance_source="operator_import",
                owner="ops",
                reviewed_by="reviewer",
                review_status="needs_review",
                confidence=Decimal("0.700000"),
                notes="Awaiting explicit review",
                active=True,
                details_json={
                    "left": {"taker_fee_rate": "0.00", "min_order_size": "1"},
                    "right": {"taker_fee_rate": "0.00", "min_order_size": "1"},
                },
            )
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual")
    await service.scan_opportunities(reason="manual", group_type="cross_venue_basis")

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.opportunity_type == "cross_venue_basis")
                .order_by(MarketStructureOpportunity.id.desc())
                .limit(1)
            )
        ).scalar_one()

    await service.validate_opportunities(reason="manual", opportunity_id=opportunity.id)

    async with session_factory() as session:
        validation = (
            await session.execute(
                select(MarketStructureValidation)
                .where(MarketStructureValidation.opportunity_id == opportunity.id)
                .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
                .limit(1)
            )
        ).scalar_one()
        assert validation.classification == VALIDATION_BLOCKED
        assert "cross_venue_review_required" in validation.reason_codes_json

        status = await fetch_market_structure_status(session)
        assert status["stale_cross_venue_link_count"] == 1
        assert status["blocked_opportunity_count"] >= 1

    await service.close()


@pytest.mark.asyncio
async def test_structure_paper_route_partial_failure_and_retention_prune(engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
        polymarket_structure_paper_require_manual_approval=False,
        polymarket_structure_retention_days=1,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase8b-retention",
            title="Phase8B Retention",
            anchor_condition_id="retain-anchor",
            basket_condition_id="retain-basket",
        )
        await session.commit()

    service = PolymarketStructureEngineService(session_factory)
    await service.build_groups(reason="manual", event_slug="phase8b-retention")
    scan_run = await service.scan_opportunities(reason="manual", event_slug="phase8b-retention")
    await service.validate_opportunities(reason="manual", scan_run_id=scan_run["id"])

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.opportunity_type == "neg_risk_direct_vs_basket")
                .order_by(MarketStructureOpportunity.id.desc())
                .limit(1)
            )
        ).scalar_one()
        plan = await create_market_structure_paper_plan(
            session,
            opportunity_id=opportunity.id,
            actor="operator",
        )
        await session.flush()
        plan = await route_market_structure_paper_plan(
            session,
            plan_id=plan.id,
            actor="operator",
            simulate_failure_leg_index=1,
        )

        assert plan.status == "partial_failed"
        await session.commit()

        orders = (
            await session.execute(
                select(MarketStructurePaperOrder)
                .where(MarketStructurePaperOrder.plan_id == plan.id)
                .order_by(MarketStructurePaperOrder.leg_index.asc())
            )
        ).scalars().all()
        assert orders[0].status == "filled"
        assert orders[1].status == "failed"

        events = (
            await session.execute(
                select(MarketStructurePaperOrderEvent)
                .where(MarketStructurePaperOrderEvent.plan_id == plan.id)
                .order_by(MarketStructurePaperOrderEvent.observed_at.asc(), MarketStructurePaperOrderEvent.id.asc())
            )
        ).scalars().all()
        event_types = [event.event_type for event in events]
        assert "order_filled" in event_types
        assert "order_failed" in event_types
        assert "plan_partial_failed" in event_types

        old_timestamp = FIXED_NOW - timedelta(days=3)
        runs = (
            await session.execute(
                select(MarketStructureRun).where(MarketStructureRun.run_type != "retention_prune")
            )
        ).scalars().all()
        for run in runs:
            run.started_at = old_timestamp
            run.completed_at = old_timestamp
        await session.commit()

    prune_run = await service.prune_retention(reason="manual")
    assert prune_run["status"] == "completed"
    assert prune_run["rows_inserted_json"]["runs_deleted"] >= 2

    async with session_factory() as session:
        remaining_runs = (
            await session.execute(
                select(func.count(MarketStructureRun.id)).where(MarketStructureRun.run_type != "retention_prune")
            )
        ).scalar_one()
        assert remaining_runs == 0
        assert (await session.execute(select(func.count(MarketStructureOpportunity.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(MarketStructureValidation.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(MarketStructurePaperPlan.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(MarketStructurePaperOrder.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(MarketStructurePaperOrderEvent.id)))).scalar_one() == 0
        assert (await session.execute(select(func.count(MarketStructureGroup.id)))).scalar_one() >= 1

    await service.close()
