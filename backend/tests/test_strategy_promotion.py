from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models.strategy_registry import AUTONOMY_TIER_SHADOW_ONLY, DemotionEvent, PromotionEvaluation
from app.strategies.promotion import (
    PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY,
    PROMOTION_EVALUATION_KIND_REPLAY,
    PROMOTION_EVALUATION_STATUS_OBSERVE,
    record_demotion_event_from_promotion_evaluation,
    record_promotion_eligibility_evaluation,
    upsert_promotion_evaluation,
)
from app.strategies.registry import PROMOTION_GATE_POLICY_V1, get_current_strategy_version, sync_strategy_registry


async def _seed_replay_gate(
    session,
    *,
    family_id: int,
    strategy_version_id: int,
    gate_policy_id: int | None,
    observed_at: datetime,
) -> PromotionEvaluation:
    return await upsert_promotion_evaluation(
        session,
        family_id=family_id,
        strategy_version_id=strategy_version_id,
        gate_policy_id=gate_policy_id,
        evaluation_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        evaluation_status=PROMOTION_EVALUATION_STATUS_OBSERVE,
        autonomy_tier=AUTONOMY_TIER_SHADOW_ONLY,
        evaluation_window_start=observed_at - timedelta(minutes=15),
        evaluation_window_end=observed_at,
        provenance_json={"source": "test"},
        summary_json={
            "replay_status": "completed",
            "coverage_limited_scenarios": 0,
            "variant_count": 1,
        },
    )


@pytest.mark.asyncio
async def test_promotion_eligibility_gate_dedupes_identical_reevaluations(session):
    registry_state = await sync_strategy_registry(session)
    version = await get_current_strategy_version(session, "exec_policy")

    assert version is not None

    family_row = registry_state["family_rows"]["exec_policy"]
    gate_policy = registry_state["gate_policy_rows"][PROMOTION_GATE_POLICY_V1]
    observed_at = datetime(2026, 4, 21, 8, 0, tzinfo=timezone.utc)

    await _seed_replay_gate(
        session,
        family_id=int(family_row.id),
        strategy_version_id=int(version.id),
        gate_policy_id=int(gate_policy.id) if gate_policy is not None else None,
        observed_at=observed_at,
    )

    first = await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(version.id),
        trigger_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        trigger_ref="replay-1",
    )
    second = await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(version.id),
        trigger_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        trigger_ref="replay-1",
    )

    rows = (
        await session.execute(
            select(PromotionEvaluation)
            .where(
                PromotionEvaluation.strategy_version_id == int(version.id),
                PromotionEvaluation.evaluation_kind == PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY,
            )
            .order_by(PromotionEvaluation.id.asc())
        )
    ).scalars().all()

    assert first is not None
    assert second is not None
    assert second.id == first.id
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_promotion_eligibility_gate_anchors_budget_snapshot_to_observed_at(session):
    registry_state = await sync_strategy_registry(session)
    version = await get_current_strategy_version(session, "exec_policy")

    assert version is not None

    family_row = registry_state["family_rows"]["exec_policy"]
    gate_policy = registry_state["gate_policy_rows"][PROMOTION_GATE_POLICY_V1]
    observed_at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    await _seed_replay_gate(
        session,
        family_id=int(family_row.id),
        strategy_version_id=int(version.id),
        gate_policy_id=int(gate_policy.id) if gate_policy is not None else None,
        observed_at=observed_at,
    )

    row = await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(version.id),
        trigger_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        trigger_ref="replay-historical",
        observed_at=observed_at,
    )

    assert row is not None
    assert row.summary_json["inputs"]["budget"]["status"]["computed_at"] == observed_at.isoformat()
    assert row.evaluation_window_end == observed_at


@pytest.mark.asyncio
async def test_blocked_promotion_eligibility_records_cooling_off_demotion(session):
    await sync_strategy_registry(session)
    version = await get_current_strategy_version(session, "exec_policy")

    assert version is not None

    observed_at = datetime(2026, 4, 21, 9, 30, tzinfo=timezone.utc)
    evaluation = await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(version.id),
        trigger_kind="pilot_supervisor_tick",
        trigger_ref="pilot-run-1",
        observed_at=observed_at,
    )
    demotion = await record_demotion_event_from_promotion_evaluation(
        session,
        evaluation=evaluation,
        trigger_kind="pilot_supervisor_tick",
        trigger_ref="pilot-run-1",
        observed_at=observed_at,
    )
    duplicate = await record_demotion_event_from_promotion_evaluation(
        session,
        evaluation=evaluation,
        trigger_kind="pilot_supervisor_tick",
        trigger_ref="pilot-run-1",
        observed_at=observed_at + timedelta(minutes=5),
    )

    assert evaluation is not None
    assert demotion is not None
    assert duplicate is not None
    assert duplicate.id == demotion.id
    assert demotion.prior_autonomy_tier == "assisted_live"
    assert demotion.fallback_autonomy_tier == AUTONOMY_TIER_SHADOW_ONLY
    assert demotion.reason_code == "replay_missing"
    assert demotion.cooling_off_ends_at == observed_at + timedelta(hours=24)
    assert demotion.details_json["evaluation_id"] == evaluation.id

    rows = (await session.execute(select(DemotionEvent))).scalars().all()
    assert len(rows) == 1
