from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.test_structure_engine import (
    _seed_executable_neg_risk_setup,
    _seed_generic_binary_market,
    _set_structure_defaults,
)


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@pytest.mark.asyncio
async def test_structure_phase8b_api_opportunity_detail_validation_and_paper_plan_flow(client, engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_validation_enabled=True,
        polymarket_structure_paper_require_manual_approval=True,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase8b-api",
            title="Phase8B API",
            anchor_condition_id="api8b-anchor",
            basket_condition_id="api8b-basket",
        )
        await session.commit()

    build_response = await client.post(
        "/api/v1/ingest/polymarket/structure/groups/build",
        json={"reason": "manual", "event_slug": "phase8b-api"},
    )
    scan_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/scan",
        json={"reason": "manual", "event_slug": "phase8b-api"},
    )
    validate_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/validate",
        json={"reason": "manual"},
    )

    assert build_response.status_code == 200
    assert scan_response.status_code == 200
    assert validate_response.status_code == 200
    assert validate_response.json()["status"] == "completed"

    opportunities_response = await client.get(
        "/api/v1/ingest/polymarket/structure/opportunities"
        "?classification=executable_candidate&event_slug=phase8b-api&executable_only=true"
    )
    assert opportunities_response.status_code == 200
    opportunities = opportunities_response.json()["rows"]
    assert opportunities
    opportunity = opportunities[0]
    assert opportunity["validation_classification"] == "executable_candidate"

    detail_response = await client.get(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}"
    )
    validations_response = await client.get(
        f"/api/v1/ingest/polymarket/structure/validations?opportunity_id={opportunity['id']}"
    )
    assert detail_response.status_code == 200
    assert validations_response.status_code == 200
    assert detail_response.json()["latest_validation"]["classification"] == "executable_candidate"
    assert validations_response.json()["rows"]

    create_plan_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/opportunities/{opportunity['id']}/paper-plans",
        json={"actor": "operator"},
    )
    assert create_plan_response.status_code == 200
    created_plan = create_plan_response.json()
    assert created_plan["status"] == "approval_pending"
    assert created_plan["manual_approval_required"] is True

    approve_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/paper-plans/{created_plan['id']}/approve",
        json={"actor": "operator"},
    )
    assert approve_response.status_code == 200
    assert approve_response.json()["status"] == "routing_pending"

    route_response = await client.post(
        f"/api/v1/ingest/polymarket/structure/paper-plans/{created_plan['id']}/route",
        json={"actor": "operator"},
    )
    assert route_response.status_code == 200
    assert route_response.json()["status"] == "routed"

    plan_detail_response = await client.get(
        f"/api/v1/ingest/polymarket/structure/paper-plans/{created_plan['id']}"
    )
    plans_response = await client.get(
        "/api/v1/ingest/polymarket/structure/paper-plans?status=routed"
    )
    status_response = await client.get("/api/v1/ingest/polymarket/structure/status")
    health_response = await client.get("/api/v1/health")

    assert plan_detail_response.status_code == 200
    assert plans_response.status_code == 200
    assert status_response.status_code == 200
    assert health_response.status_code == 200
    plan_detail = plan_detail_response.json()
    assert plan_detail["orders"]
    assert any(event["event_type"] == "plan_routed" for event in plan_detail["events"])
    assert status_response.json()["last_paper_route_status"] == "completed"
    assert "pending_approval_count" in health_response.json()["polymarket_phase8a"]


@pytest.mark.asyncio
async def test_structure_phase8b_api_cross_venue_governance_filters(client, engine, monkeypatch):
    _set_structure_defaults(
        monkeypatch,
        polymarket_structure_include_cross_venue=True,
        polymarket_structure_validation_enabled=True,
        polymarket_structure_cross_venue_max_staleness_seconds=7200,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        left = await _seed_generic_binary_market(
            session,
            platform="manifold",
            platform_id="api-cross-left",
            question="Will Candidate C win the state?",
            yes_asks=[["0.40", "5"]],
            no_asks=[["0.63", "5"]],
        )
        right = await _seed_generic_binary_market(
            session,
            platform="kalshi",
            platform_id="api-cross-right",
            question="Will Candidate C win the state?",
            yes_asks=[["0.72", "5"]],
            no_asks=[["0.45", "5"]],
        )
        await session.commit()

    expires_at = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    link_response = await client.post(
        "/api/v1/ingest/polymarket/structure/cross-venue-links",
        json={
            "left_venue": "manifold",
            "left_market_id": str(left["market"].id),
            "left_outcome_id": str(left["yes_outcome"].id),
            "left_external_id": "api-cross-left",
            "right_venue": "kalshi",
            "right_market_id": str(right["market"].id),
            "right_outcome_id": str(right["yes_outcome"].id),
            "right_external_id": "api-cross-right",
            "mapping_kind": "manual",
            "provenance_source": "operator_console",
            "owner": "ops",
            "reviewed_by": "reviewer",
            "review_status": "approved",
            "confidence": 0.91,
            "notes": "Explicitly reviewed link",
            "expires_at": expires_at,
            "active": True,
            "details_json": {
                "left": {"taker_fee_rate": "0.00", "min_order_size": "1"},
                "right": {"taker_fee_rate": "0.00", "min_order_size": "1"},
            },
        },
    )
    assert link_response.status_code == 200
    link = link_response.json()
    assert link["provenance_source"] == "operator_console"
    assert link["effective_review_status"] == "expired"

    build_response = await client.post(
        "/api/v1/ingest/polymarket/structure/groups/build",
        json={"reason": "manual"},
    )
    scan_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/scan",
        json={"reason": "manual", "group_type": "cross_venue_basis"},
    )
    validate_response = await client.post(
        "/api/v1/ingest/polymarket/structure/opportunities/validate",
        json={"reason": "manual"},
    )
    assert build_response.status_code == 200
    assert scan_response.status_code == 200
    assert validate_response.status_code == 200

    links_response = await client.get(
        "/api/v1/ingest/polymarket/structure/cross-venue-links?review_status=expired&confidence_min=0.9"
    )
    opportunities_response = await client.get(
        "/api/v1/ingest/polymarket/structure/opportunities"
        "?opportunity_type=cross_venue_basis&review_status=expired"
    )
    status_response = await client.get("/api/v1/ingest/polymarket/structure/status")
    assert links_response.status_code == 200
    assert opportunities_response.status_code == 200
    assert status_response.status_code == 200

    links = links_response.json()["rows"]
    opportunities = opportunities_response.json()["rows"]
    assert links
    assert opportunities
    assert links[0]["effective_review_status"] == "expired"
    assert links[0]["owner"] == "ops"
    assert "cross_venue_link_expired" in opportunities[0]["validation_reason_codes"]
    assert status_response.json()["stale_cross_venue_link_count"] == 1
