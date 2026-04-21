from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import get_default_strategy_contract
from app.models.paper_trade import PaperTrade
from app.models.polymarket_live_execution import LiveOrder
from app.models.polymarket_pilot import PolymarketPilotReadinessReport, PolymarketPilotScorecard
from app.models.polymarket_replay import PolymarketReplayRun
from app.models.strategy_registry import (
    AUTONOMY_TIER_ASSISTED_LIVE,
    AUTONOMY_TIER_SHADOW_ONLY,
    VERSION_STATUS_BENCHMARK,
    VERSION_STATUS_CANDIDATE,
    VERSION_STATUS_PROMOTED,
    PromotionEvaluation,
    PromotionGatePolicy,
    StrategyFamilyRegistry,
    StrategyVersion,
    DemotionEvent,
)
from app.models.strategy_run import StrategyRun
from app.strategies.promotion import (
    serialize_demotion_event,
    serialize_promotion_evaluation,
    serialize_promotion_gate_policy,
)
from app.strategy_families import build_strategy_family_reviews

STRATEGY_FAMILY_DEFAULT = "default_strategy"
STRATEGY_FAMILY_EXEC_POLICY = "exec_policy"
PROMOTION_GATE_POLICY_V1 = "promotion_gate_policy_v1"


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _builtin_family_manifest() -> list[dict[str, Any]]:
    manifest = list(build_strategy_family_reviews())
    manifest.append(
        {
            "family": STRATEGY_FAMILY_EXEC_POLICY,
            "label": "Execution Policy",
            "posture": "advisory_only",
            "configured": True,
            "review_enabled": True,
            "primary_surface": "pilot_console",
            "description": "Shared execution infrastructure used by promoted families; not a standalone profitability claim.",
            "disabled_reason": None,
            "family_kind": "infrastructure",
        }
    )
    return manifest


def _version_seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "family": STRATEGY_FAMILY_DEFAULT,
            "version_key": "default_strategy_benchmark_v1",
            "version_label": "Frozen Benchmark v1",
            "strategy_name": settings.default_strategy_name,
            "version_status": VERSION_STATUS_BENCHMARK,
            "autonomy_tier": AUTONOMY_TIER_SHADOW_ONLY,
            "is_current": True,
            "is_frozen": True,
            "config_json": {
                "role": "benchmark_truth_anchor",
                "contract": get_default_strategy_contract(),
            },
            "provenance_json": {
                "seed_source": "phase13a_builtin_manifest",
                "family_kind": "strategy",
            },
        },
        {
            "family": "structure",
            "version_key": "structure_candidate_v1",
            "version_label": "Structure Candidate v1",
            "strategy_name": None,
            "version_status": VERSION_STATUS_CANDIDATE,
            "autonomy_tier": AUTONOMY_TIER_SHADOW_ONLY,
            "is_current": True,
            "is_frozen": False,
            "config_json": {
                "target_lane": "first_autonomy_candidate",
                "notes": "Research-active family seeded for Phase 13A lifecycle tracking.",
            },
            "provenance_json": {
                "seed_source": "phase13a_builtin_manifest",
                "family_kind": "strategy",
            },
        },
        {
            "family": "maker",
            "version_key": "maker_candidate_v1",
            "version_label": "Maker Candidate v1",
            "strategy_name": None,
            "version_status": VERSION_STATUS_CANDIDATE,
            "autonomy_tier": AUTONOMY_TIER_SHADOW_ONLY,
            "is_current": True,
            "is_frozen": False,
            "config_json": {
                "target_lane": "reward_aware_candidate",
                "notes": "Advisory-only family remains behind stronger replay and attribution proof.",
            },
            "provenance_json": {
                "seed_source": "phase13a_builtin_manifest",
                "family_kind": "strategy",
            },
        },
        {
            "family": "cross_venue_basis",
            "version_key": "cross_venue_basis_candidate_v1",
            "version_label": "Cross-Venue Basis Candidate v1",
            "strategy_name": None,
            "version_status": VERSION_STATUS_CANDIDATE,
            "autonomy_tier": AUTONOMY_TIER_SHADOW_ONLY,
            "is_current": True,
            "is_frozen": False,
            "config_json": {
                "target_lane": "deferred",
                "notes": "Disabled until executable paired hedge routing exists.",
            },
            "provenance_json": {
                "seed_source": "phase13a_builtin_manifest",
                "family_kind": "strategy",
            },
        },
        {
            "family": STRATEGY_FAMILY_EXEC_POLICY,
            "version_key": "exec_policy_infra_v1",
            "version_label": "Execution Policy Infra v1",
            "strategy_name": None,
            "version_status": VERSION_STATUS_PROMOTED,
            "autonomy_tier": AUTONOMY_TIER_ASSISTED_LIVE,
            "is_current": True,
            "is_frozen": False,
            "config_json": {
                "role": "shared_execution_infrastructure",
                "notes": "Execution policy remains shared infra rather than the alpha claim under review.",
            },
            "provenance_json": {
                "seed_source": "phase13a_builtin_manifest",
                "family_kind": "infrastructure",
            },
        },
    ]


def _gate_policy_seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "policy_key": PROMOTION_GATE_POLICY_V1,
            "label": "Promotion Gate Policy v1",
            "status": "active",
            "policy_json": {
                "required_inputs": [
                    "minimum_live_sample_size",
                    "minimum_calendar_observation_window",
                    "positive_net_realized_pnl_after_fees",
                    "explicit_reward_dependence_handling",
                    "acceptable_drawdown",
                    "acceptable_live_vs_shadow_gap",
                    "acceptable_incident_rate",
                    "acceptable_reconciliation_reliability",
                ],
                "notes": "Seeded in Phase 13A as the first inspectable gate-policy version.",
            },
        }
    ]


async def sync_strategy_registry(session: AsyncSession) -> dict[str, Any]:
    family_rows: dict[str, StrategyFamilyRegistry] = {}
    for manifest_row in _builtin_family_manifest():
        existing = (
            await session.execute(
                select(StrategyFamilyRegistry)
                .where(StrategyFamilyRegistry.family == manifest_row["family"])
                .limit(1)
            )
        ).scalar_one_or_none()
        row = existing or StrategyFamilyRegistry(family=manifest_row["family"])
        row.label = manifest_row["label"]
        row.posture = manifest_row["posture"]
        row.configured = bool(manifest_row["configured"])
        row.review_enabled = bool(manifest_row["review_enabled"])
        row.primary_surface = manifest_row["primary_surface"]
        row.description = manifest_row["description"]
        row.disabled_reason = manifest_row.get("disabled_reason")
        row.family_kind = manifest_row.get("family_kind") or "strategy"
        row.seeded_from = "builtin"
        if existing is None:
            session.add(row)
        family_rows[manifest_row["family"]] = row

    await session.flush()

    version_rows: dict[str, StrategyVersion] = {}
    for seed_row in _version_seed_rows():
        existing = (
            await session.execute(
                select(StrategyVersion)
                .where(StrategyVersion.version_key == seed_row["version_key"])
                .limit(1)
            )
        ).scalar_one_or_none()
        row = existing or StrategyVersion(version_key=seed_row["version_key"])
        row.family_id = family_rows[seed_row["family"]].id
        row.version_label = seed_row["version_label"]
        row.strategy_name = seed_row["strategy_name"]
        row.version_status = seed_row["version_status"]
        row.autonomy_tier = seed_row["autonomy_tier"]
        row.is_current = bool(seed_row["is_current"])
        row.is_frozen = bool(seed_row["is_frozen"])
        row.config_json = seed_row.get("config_json")
        row.provenance_json = seed_row.get("provenance_json")
        if existing is None:
            session.add(row)
        version_rows[seed_row["family"]] = row

    gate_policy_rows: dict[str, PromotionGatePolicy] = {}
    for seed_row in _gate_policy_seed_rows():
        existing = (
            await session.execute(
                select(PromotionGatePolicy)
                .where(PromotionGatePolicy.policy_key == seed_row["policy_key"])
                .limit(1)
            )
        ).scalar_one_or_none()
        row = existing or PromotionGatePolicy(policy_key=seed_row["policy_key"])
        row.label = seed_row["label"]
        row.status = seed_row["status"]
        row.policy_json = seed_row.get("policy_json")
        if existing is None:
            session.add(row)
        gate_policy_rows[seed_row["policy_key"]] = row

    await session.flush()
    await _backfill_phase13a_links(session, version_rows)

    return {
        "family_rows": family_rows,
        "version_rows": version_rows,
        "gate_policy_rows": gate_policy_rows,
    }


async def _backfill_phase13a_links(
    session: AsyncSession,
    version_rows: dict[str, StrategyVersion],
) -> None:
    version_by_family = {family: row for family, row in version_rows.items() if row.id is not None}
    default_version = version_by_family.get(STRATEGY_FAMILY_DEFAULT)

    if default_version is not None:
        default_runs = (
            await session.execute(
                select(StrategyRun)
                .where(
                    StrategyRun.strategy_name == settings.default_strategy_name,
                    StrategyRun.strategy_version_id.is_(None),
                )
            )
        ).scalars().all()
        for run in default_runs:
            run.strategy_family = STRATEGY_FAMILY_DEFAULT
            run.strategy_version_id = default_version.id
            if isinstance(run.contract_snapshot, dict):
                run.contract_snapshot.setdefault("strategy_family", STRATEGY_FAMILY_DEFAULT)
                run.contract_snapshot.setdefault("strategy_version_key", default_version.version_key)
                run.contract_snapshot.setdefault("strategy_version_label", default_version.version_label)
                run.contract_snapshot.setdefault("strategy_version_status", default_version.version_status)

    generic_runs = (
        await session.execute(
            select(StrategyRun)
            .where(
                StrategyRun.strategy_version_id.is_(None),
                StrategyRun.strategy_family.is_not(None),
            )
        )
    ).scalars().all()
    for run in generic_runs:
        version = version_by_family.get(str(run.strategy_family).strip().lower())
        if version is None:
            continue
        run.strategy_version_id = version.id

    paper_trade_rows = (
        await session.execute(
            select(PaperTrade, StrategyRun.strategy_version_id)
            .join(StrategyRun, StrategyRun.id == PaperTrade.strategy_run_id)
            .where(
                PaperTrade.strategy_version_id.is_(None),
                StrategyRun.strategy_version_id.is_not(None),
            )
        )
    ).all()
    for trade, strategy_version_id in paper_trade_rows:
        trade.strategy_version_id = strategy_version_id

    live_order_rows = (
        await session.execute(
            select(LiveOrder)
            .where(
                LiveOrder.strategy_version_id.is_(None),
                LiveOrder.strategy_family.is_not(None),
            )
        )
    ).scalars().all()
    for row in live_order_rows:
        version = version_by_family.get(str(row.strategy_family).strip().lower())
        if version is not None:
            row.strategy_version_id = version.id

    scorecard_rows = (
        await session.execute(
            select(PolymarketPilotScorecard)
            .where(
                PolymarketPilotScorecard.strategy_version_id.is_(None),
                PolymarketPilotScorecard.strategy_family.is_not(None),
            )
        )
    ).scalars().all()
    for row in scorecard_rows:
        version = version_by_family.get(str(row.strategy_family).strip().lower())
        if version is not None:
            row.strategy_version_id = version.id

    readiness_rows = (
        await session.execute(
            select(PolymarketPilotReadinessReport)
            .where(
                PolymarketPilotReadinessReport.strategy_version_id.is_(None),
                PolymarketPilotReadinessReport.strategy_family.is_not(None),
            )
        )
    ).scalars().all()
    for row in readiness_rows:
        version = version_by_family.get(str(row.strategy_family).strip().lower())
        if version is not None:
            row.strategy_version_id = version.id

    replay_rows = (
        await session.execute(
            select(PolymarketReplayRun)
            .where(
                PolymarketReplayRun.strategy_version_id.is_(None),
                PolymarketReplayRun.strategy_family.is_not(None),
            )
        )
    ).scalars().all()
    for row in replay_rows:
        version = version_by_family.get(str(row.strategy_family).strip().lower())
        if version is not None:
            row.strategy_version_id = version.id


async def get_current_strategy_version(
    session: AsyncSession,
    family: str,
    *,
    sync_registry: bool = True,
) -> StrategyVersion | None:
    normalized_family = str(family or "").strip().lower()
    if not normalized_family:
        return None
    if sync_registry:
        await sync_strategy_registry(session)
    result = await session.execute(
        select(StrategyVersion)
        .join(StrategyFamilyRegistry, StrategyFamilyRegistry.id == StrategyVersion.family_id)
        .where(
            StrategyFamilyRegistry.family == normalized_family,
            StrategyVersion.is_current.is_(True),
        )
        .order_by(StrategyVersion.updated_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _version_evidence_counts(session: AsyncSession) -> dict[int, dict[str, int]]:
    counts: dict[int, dict[str, int]] = defaultdict(
        lambda: {
            "strategy_runs": 0,
            "paper_trades": 0,
            "replay_runs": 0,
            "live_orders": 0,
            "pilot_scorecards": 0,
            "readiness_reports": 0,
        }
    )

    result = await session.execute(
        select(StrategyRun.strategy_version_id, func.count(StrategyRun.id))
        .where(StrategyRun.strategy_version_id.is_not(None))
        .group_by(StrategyRun.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["strategy_runs"] = int(row_count or 0)

    result = await session.execute(
        select(PaperTrade.strategy_version_id, func.count(PaperTrade.id))
        .where(PaperTrade.strategy_version_id.is_not(None))
        .group_by(PaperTrade.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["paper_trades"] = int(row_count or 0)

    result = await session.execute(
        select(PolymarketReplayRun.strategy_version_id, func.count(PolymarketReplayRun.id))
        .where(PolymarketReplayRun.strategy_version_id.is_not(None))
        .group_by(PolymarketReplayRun.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["replay_runs"] = int(row_count or 0)

    result = await session.execute(
        select(LiveOrder.strategy_version_id, func.count(LiveOrder.id))
        .where(LiveOrder.strategy_version_id.is_not(None))
        .group_by(LiveOrder.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["live_orders"] = int(row_count or 0)

    result = await session.execute(
        select(PolymarketPilotScorecard.strategy_version_id, func.count(PolymarketPilotScorecard.id))
        .where(PolymarketPilotScorecard.strategy_version_id.is_not(None))
        .group_by(PolymarketPilotScorecard.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["pilot_scorecards"] = int(row_count or 0)

    result = await session.execute(
        select(PolymarketPilotReadinessReport.strategy_version_id, func.count(PolymarketPilotReadinessReport.id))
        .where(PolymarketPilotReadinessReport.strategy_version_id.is_not(None))
        .group_by(PolymarketPilotReadinessReport.strategy_version_id)
    )
    for strategy_version_id, row_count in result.all():
        counts[int(strategy_version_id)]["readiness_reports"] = int(row_count or 0)

    return counts


def _serialize_strategy_version(
    row: StrategyVersion,
    *,
    evidence_counts: dict[int, dict[str, int]],
) -> dict[str, Any]:
    return {
        "id": row.id,
        "version_key": row.version_key,
        "version_label": row.version_label,
        "strategy_name": row.strategy_name,
        "version_status": row.version_status,
        "autonomy_tier": row.autonomy_tier,
        "is_current": row.is_current,
        "is_frozen": row.is_frozen,
        "config_json": row.config_json or {},
        "provenance_json": row.provenance_json or {},
        "evidence_counts": evidence_counts.get(
            row.id,
            {
                "strategy_runs": 0,
                "paper_trades": 0,
                "replay_runs": 0,
                "live_orders": 0,
                "pilot_scorecards": 0,
                "readiness_reports": 0,
            },
        ),
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


def serialize_strategy_version_snapshot(row: StrategyVersion | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family_id": row.family_id,
        "version_key": row.version_key,
        "version_label": row.version_label,
        "strategy_name": row.strategy_name,
        "version_status": row.version_status,
        "autonomy_tier": row.autonomy_tier,
        "is_current": row.is_current,
        "is_frozen": row.is_frozen,
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


async def get_strategy_version_snapshot_map(
    session: AsyncSession,
    *,
    version_ids: list[int] | set[int] | tuple[int, ...],
) -> dict[int, dict[str, Any]]:
    normalized_ids = sorted({int(value) for value in version_ids if value is not None})
    if not normalized_ids:
        return {}
    rows = (
        await session.execute(
            select(StrategyVersion).where(StrategyVersion.id.in_(normalized_ids))
        )
    ).scalars().all()
    return {
        int(row.id): serialize_strategy_version_snapshot(row)
        for row in rows
    }


async def get_latest_promotion_evaluation_by_version(
    session: AsyncSession,
    *,
    version_ids: list[int] | set[int] | tuple[int, ...],
) -> dict[int, dict[str, Any]]:
    normalized_ids = sorted({int(value) for value in version_ids if value is not None})
    if not normalized_ids:
        return {}
    rows = (
        await session.execute(
            select(PromotionEvaluation)
            .where(PromotionEvaluation.strategy_version_id.in_(normalized_ids))
            .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
        )
    ).scalars().all()
    latest_by_version: dict[int, dict[str, Any]] = {}
    for row in rows:
        latest_by_version.setdefault(int(row.strategy_version_id), serialize_promotion_evaluation(row))
    return latest_by_version


async def get_strategy_registry_payload(session: AsyncSession) -> dict[str, Any]:
    await sync_strategy_registry(session)
    evidence_counts = await _version_evidence_counts(session)

    families = (
        await session.execute(
            select(StrategyFamilyRegistry)
            .order_by(StrategyFamilyRegistry.family_kind.asc(), StrategyFamilyRegistry.family.asc())
        )
    ).scalars().all()

    versions = (
        await session.execute(
            select(StrategyVersion)
            .order_by(StrategyVersion.family_id.asc(), StrategyVersion.is_current.desc(), StrategyVersion.updated_at.desc())
        )
    ).scalars().all()
    versions_by_family: dict[int, list[StrategyVersion]] = defaultdict(list)
    for version in versions:
        versions_by_family[version.family_id].append(version)

    policies = (
        await session.execute(
            select(PromotionGatePolicy).order_by(PromotionGatePolicy.updated_at.desc(), PromotionGatePolicy.id.desc())
        )
    ).scalars().all()

    evaluations = (
        await session.execute(
            select(PromotionEvaluation)
            .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
        )
    ).scalars().all()
    latest_evaluation_by_family: dict[int, PromotionEvaluation] = {}
    for evaluation in evaluations:
        latest_evaluation_by_family.setdefault(evaluation.family_id, evaluation)

    demotion_events = (
        await session.execute(
            select(DemotionEvent)
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
        )
    ).scalars().all()
    latest_demotion_by_family: dict[int, DemotionEvent] = {}
    for event in demotion_events:
        latest_demotion_by_family.setdefault(event.family_id, event)

    serialized_families = []
    for family in families:
        serialized_versions = [
            _serialize_strategy_version(version, evidence_counts=evidence_counts)
            for version in versions_by_family.get(family.id, [])
        ]
        current_version = next((row for row in serialized_versions if row["is_current"]), None)
        serialized_families.append(
            {
                "id": family.id,
                "family": family.family,
                "label": family.label,
                "posture": family.posture,
                "configured": family.configured,
                "review_enabled": family.review_enabled,
                "primary_surface": family.primary_surface,
                "description": family.description,
                "disabled_reason": family.disabled_reason,
                "family_kind": family.family_kind,
                "seeded_from": family.seeded_from,
                "current_version": current_version,
                "versions": serialized_versions,
                "latest_promotion_evaluation": serialize_promotion_evaluation(latest_evaluation_by_family.get(family.id)),
                "latest_demotion_event": serialize_demotion_event(latest_demotion_by_family.get(family.id)),
                "created_at": _ensure_utc(family.created_at).isoformat() if family.created_at else None,
                "updated_at": _ensure_utc(family.updated_at).isoformat() if family.updated_at else None,
            }
        )

    return {
        "summary": {
            "phase": "13A",
            "family_count": len(serialized_families),
            "version_count": len(versions),
            "gate_policy_count": len(policies),
            "benchmark_family": STRATEGY_FAMILY_DEFAULT,
        },
        "families": serialized_families,
        "gate_policies": [serialize_promotion_gate_policy(row) for row in policies],
        "generated_at": _ensure_utc(datetime.now(timezone.utc)).isoformat(),
    }
