from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.structure_engine import PolymarketStructureEngineService, fetch_market_structure_status
from app.models.market_structure import (
    CrossVenueMarketLink,
    MarketStructureGroup,
    MarketStructureGroupMember,
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
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


FIXED_NOW = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _decimal(value: str | int | float | Decimal) -> Decimal:
    return Decimal(str(value))


def _book_payload(levels: list[tuple[str, str]]) -> list[list[str]]:
    return [[str(price), str(size)] for price, size in levels]


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


async def _seed_event(
    session: AsyncSession,
    *,
    slug: str,
    title: str,
    neg_risk: bool = False,
    active: bool = True,
    source_payload_json: dict | None = None,
) -> PolymarketEventDim:
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
        last_gamma_sync_at=FIXED_NOW,
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
        last_gamma_sync_at=FIXED_NOW,
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
        last_gamma_sync_at=FIXED_NOW,
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
        last_gamma_sync_at=FIXED_NOW,
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
                effective_at_exchange=FIXED_NOW,
                observed_at_local=FIXED_NOW,
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
        event_ts_exchange=FIXED_NOW,
        recv_ts_local=FIXED_NOW,
        observed_at_local=FIXED_NOW,
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
            last_snapshot_exchange_ts=FIXED_NOW,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=(best_ask - best_bid),
            depth_levels_bid=len(bids),
            depth_levels_ask=len(asks),
            expected_tick_size=_decimal(tick_size),
            last_exchange_ts=FIXED_NOW,
            last_received_at_local=FIXED_NOW,
            last_reconciled_at=FIXED_NOW,
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
        captured_at=FIXED_NOW,
    )
    make_orderbook_snapshot(
        session,
        no_outcome.id,
        spread=None,
        bids=no_bids or [],
        asks=no_asks,
        captured_at=FIXED_NOW,
    )
    await session.flush()
    return {
        "market": market,
        "yes_outcome": yes_outcome,
        "no_outcome": no_outcome,
    }


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
