from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import get_default_strategy_contract
from app.models.paper_trade import PaperTrade
from app.models.polymarket_live_execution import LiveOrder
from app.models.polymarket_pilot import (
    PolymarketLiveShadowEvaluation,
    PolymarketPilotReadinessReport,
    PolymarketPilotScorecard,
)
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


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _serialize_decimal(value: Any) -> str | None:
    decimal_value = _to_decimal(value)
    return None if decimal_value is None else format(decimal_value, "f")


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


def _serialize_replay_alignment(row: PolymarketReplayRun | None) -> dict[str, Any] | None:
    if row is None:
        return None
    config = row.config_json if isinstance(row.config_json, dict) else {}
    details = row.details_json if isinstance(row.details_json, dict) else {}
    return {
        "id": str(row.id),
        "run_key": row.run_key,
        "run_type": row.run_type,
        "reason": row.reason,
        "status": row.status,
        "scenario_count": row.scenario_count,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version_key": config.get("strategy_version_key"),
        "strategy_version_label": config.get("strategy_version_label"),
        "time_window_start": _ensure_utc(row.time_window_start).isoformat() if row.time_window_start else None,
        "time_window_end": _ensure_utc(row.time_window_end).isoformat() if row.time_window_end else None,
        "started_at": _ensure_utc(row.started_at).isoformat() if row.started_at else None,
        "completed_at": _ensure_utc(row.completed_at).isoformat() if row.completed_at else None,
        "promotion_evaluation": details.get("promotion_evaluation"),
    }


def _serialize_live_shadow_alignment(
    latest_row: PolymarketLiveShadowEvaluation | None,
    recent_rows: list[PolymarketLiveShadowEvaluation],
    *,
    breach_threshold: Decimal,
) -> dict[str, Any] | None:
    if latest_row is None and not recent_rows:
        return None
    gap_values = [
        abs(_to_decimal(row.gap_bps) or Decimal("0"))
        for row in recent_rows
        if row.gap_bps is not None and not row.coverage_limited
    ]
    avg_gap = sum(gap_values, Decimal("0")) / Decimal(len(gap_values)) if gap_values else None
    worst_gap = max(gap_values) if gap_values else None
    breach_count = sum(1 for gap in gap_values if gap >= breach_threshold)
    coverage_limited_count = sum(1 for row in recent_rows if row.coverage_limited)
    return {
        "latest_updated_at": _ensure_utc(latest_row.updated_at).isoformat() if latest_row is not None and latest_row.updated_at else None,
        "latest_variant_name": latest_row.variant_name if latest_row is not None else None,
        "latest_reason_code": latest_row.reason_code if latest_row is not None else None,
        "latest_gap_bps": _serialize_decimal(latest_row.gap_bps if latest_row is not None else None),
        "latest_realized_net_bps": _serialize_decimal(latest_row.realized_net_bps if latest_row is not None else None),
        "latest_replay_run_id": str(latest_row.replay_run_id) if latest_row is not None and latest_row.replay_run_id is not None else None,
        "recent_count_24h": len(recent_rows),
        "coverage_limited_count_24h": coverage_limited_count,
        "average_gap_bps_24h": _serialize_decimal(avg_gap),
        "worst_gap_bps_24h": _serialize_decimal(worst_gap),
        "breach_count_24h": breach_count,
    }


def _serialize_scorecard_alignment(row: PolymarketPilotScorecard | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "status": row.status,
        "window_start": _ensure_utc(row.window_start).isoformat() if row.window_start else None,
        "window_end": _ensure_utc(row.window_end).isoformat() if row.window_end else None,
        "live_orders_count": row.live_orders_count,
        "fills_count": row.fills_count,
        "incident_count": row.incident_count,
        "net_pnl": _serialize_decimal(row.net_pnl),
        "avg_shadow_gap_bps": _serialize_decimal(row.avg_shadow_gap_bps),
        "coverage_limited_count": row.coverage_limited_count,
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
    }


def _serialize_readiness_alignment(row: PolymarketPilotReadinessReport | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "status": row.status,
        "window_start": _ensure_utc(row.window_start).isoformat() if row.window_start else None,
        "window_end": _ensure_utc(row.window_end).isoformat() if row.window_end else None,
        "generated_at": _ensure_utc(row.generated_at).isoformat() if row.generated_at else None,
        "approval_backlog_count": row.approval_backlog_count,
        "coverage_limited_count": row.coverage_limited_count,
        "shadow_gap_breach_count": row.shadow_gap_breach_count,
        "open_incidents": row.open_incidents,
    }


def _serialize_family_reference(row: StrategyFamilyRegistry | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family": row.family,
        "label": row.label,
        "posture": row.posture,
        "primary_surface": row.primary_surface,
        "family_kind": row.family_kind,
        "description": row.description,
        "disabled_reason": row.disabled_reason,
        "seeded_from": row.seeded_from,
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


def _serialize_live_shadow_detail(
    row: PolymarketLiveShadowEvaluation,
    *,
    live_order: LiveOrder | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "client_order_id": live_order.client_order_id if live_order is not None else None,
        "condition_id": live_order.condition_id if live_order is not None else None,
        "asset_id": live_order.asset_id if live_order is not None else None,
        "side": live_order.side if live_order is not None else None,
        "live_order_status": live_order.status if live_order is not None else None,
        "variant_name": row.variant_name,
        "gap_bps": _serialize_decimal(row.gap_bps),
        "realized_net_bps": _serialize_decimal(row.realized_net_bps),
        "expected_net_ev_bps": _serialize_decimal(row.expected_net_ev_bps),
        "coverage_limited": row.coverage_limited,
        "reason_code": row.reason_code,
        "replay_run_id": str(row.replay_run_id) if row.replay_run_id is not None else None,
        "details_json": row.details_json or {},
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


def _evidence_alignment_status(surface_count: int) -> str:
    if surface_count >= 4:
        return "complete"
    if surface_count > 0:
        return "partial"
    return "registry_only"


async def _version_evidence_alignment(
    session: AsyncSession,
    *,
    versions: list[StrategyVersion],
) -> dict[int, dict[str, Any]]:
    version_ids = sorted({int(row.id) for row in versions if row.id is not None})
    if not version_ids:
        return {}

    replay_rows = (
        await session.execute(
            select(PolymarketReplayRun)
            .where(PolymarketReplayRun.strategy_version_id.in_(version_ids))
            .order_by(PolymarketReplayRun.started_at.desc(), PolymarketReplayRun.id.desc())
        )
    ).scalars().all()
    latest_replay_by_version: dict[int, PolymarketReplayRun] = {}
    for row in replay_rows:
        if row.strategy_version_id is not None:
            latest_replay_by_version.setdefault(int(row.strategy_version_id), row)

    latest_shadow_by_version: dict[int, PolymarketLiveShadowEvaluation] = {}
    recent_shadow_rows_by_version: dict[int, list[PolymarketLiveShadowEvaluation]] = defaultdict(list)
    shadow_since = datetime.now(timezone.utc) - timedelta(hours=24)
    shadow_rows = (
        await session.execute(
            select(PolymarketLiveShadowEvaluation, LiveOrder.strategy_version_id)
            .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id)
            .where(LiveOrder.strategy_version_id.in_(version_ids))
            .order_by(PolymarketLiveShadowEvaluation.updated_at.desc(), PolymarketLiveShadowEvaluation.id.desc())
        )
    ).all()
    for shadow_row, strategy_version_id in shadow_rows:
        if strategy_version_id is None:
            continue
        version_id = int(strategy_version_id)
        latest_shadow_by_version.setdefault(version_id, shadow_row)
        if shadow_row.updated_at is not None and _ensure_utc(shadow_row.updated_at) >= shadow_since:
            recent_shadow_rows_by_version[version_id].append(shadow_row)

    scorecard_rows = (
        await session.execute(
            select(PolymarketPilotScorecard)
            .where(PolymarketPilotScorecard.strategy_version_id.in_(version_ids))
            .order_by(
                PolymarketPilotScorecard.window_end.desc(),
                PolymarketPilotScorecard.created_at.desc(),
                PolymarketPilotScorecard.id.desc(),
            )
        )
    ).scalars().all()
    latest_scorecard_by_version: dict[int, PolymarketPilotScorecard] = {}
    for row in scorecard_rows:
        if row.strategy_version_id is not None:
            latest_scorecard_by_version.setdefault(int(row.strategy_version_id), row)

    readiness_rows = (
        await session.execute(
            select(PolymarketPilotReadinessReport)
            .where(PolymarketPilotReadinessReport.strategy_version_id.in_(version_ids))
            .order_by(
                PolymarketPilotReadinessReport.generated_at.desc(),
                PolymarketPilotReadinessReport.id.desc(),
            )
        )
    ).scalars().all()
    latest_readiness_by_version: dict[int, PolymarketPilotReadinessReport] = {}
    for row in readiness_rows:
        if row.strategy_version_id is not None:
            latest_readiness_by_version.setdefault(int(row.strategy_version_id), row)

    latest_evaluations = await get_latest_promotion_evaluation_by_version(session, version_ids=version_ids)
    breach_threshold = Decimal(str(settings.polymarket_pilot_shadow_gap_breach_bps))

    payload: dict[int, dict[str, Any]] = {}
    for version in versions:
        if version.id is None:
            continue
        version_id = int(version.id)
        latest_replay = _serialize_replay_alignment(latest_replay_by_version.get(version_id))
        live_shadow = _serialize_live_shadow_alignment(
            latest_shadow_by_version.get(version_id),
            recent_shadow_rows_by_version.get(version_id, []),
            breach_threshold=breach_threshold,
        )
        latest_scorecard = _serialize_scorecard_alignment(latest_scorecard_by_version.get(version_id))
        latest_readiness = _serialize_readiness_alignment(latest_readiness_by_version.get(version_id))
        surfaces = {
            "replay": latest_replay,
            "live_shadow": live_shadow,
            "scorecard": latest_scorecard,
            "readiness": latest_readiness,
        }
        timestamps = [
            _ensure_utc(latest_replay_by_version[version_id].completed_at or latest_replay_by_version[version_id].started_at)
            if version_id in latest_replay_by_version
            else None,
            _ensure_utc(latest_shadow_by_version[version_id].updated_at) if version_id in latest_shadow_by_version else None,
            _ensure_utc(latest_scorecard_by_version[version_id].window_end) if version_id in latest_scorecard_by_version else None,
            _ensure_utc(latest_readiness_by_version[version_id].generated_at) if version_id in latest_readiness_by_version else None,
        ]
        present_surface_keys = [name for name, item in surfaces.items() if item is not None]
        latest_surface_at = max((timestamp for timestamp in timestamps if timestamp is not None), default=None)
        payload[version_id] = {
            "surface_status": _evidence_alignment_status(len(present_surface_keys)),
            "surfaces_present": len(present_surface_keys),
            "surface_keys_present": present_surface_keys,
            "latest_surface_at": latest_surface_at.isoformat() if latest_surface_at is not None else None,
            "latest_promotion_evaluation": latest_evaluations.get(version_id),
            "latest_replay_run": latest_replay,
            "live_shadow": live_shadow,
            "latest_scorecard": latest_scorecard,
            "latest_readiness_report": latest_readiness,
        }
    return payload


async def get_strategy_version_detail_payload(
    session: AsyncSession,
    *,
    version_id: int,
    replay_limit: int = 5,
    live_shadow_limit: int = 10,
    pilot_limit: int = 5,
    event_limit: int = 5,
) -> dict[str, Any] | None:
    await sync_strategy_registry(session)
    version = await session.get(StrategyVersion, version_id)
    if version is None:
        return None
    family = await session.get(StrategyFamilyRegistry, version.family_id)
    evidence_counts = await _version_evidence_counts(session)
    alignment_by_version = await _version_evidence_alignment(session, versions=[version])
    latest_evaluation = (
        await get_latest_promotion_evaluation_by_version(session, version_ids=[int(version.id)])
    ).get(int(version.id))
    latest_demotion = (
        await session.execute(
            select(DemotionEvent)
            .where(DemotionEvent.strategy_version_id == version.id)
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    replay_rows = (
        await session.execute(
            select(PolymarketReplayRun)
            .where(PolymarketReplayRun.strategy_version_id == version.id)
            .order_by(PolymarketReplayRun.started_at.desc(), PolymarketReplayRun.id.desc())
            .limit(replay_limit)
        )
    ).scalars().all()
    live_shadow_rows = (
        await session.execute(
            select(PolymarketLiveShadowEvaluation, LiveOrder)
            .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id, isouter=True)
            .where(LiveOrder.strategy_version_id == version.id)
            .order_by(PolymarketLiveShadowEvaluation.updated_at.desc(), PolymarketLiveShadowEvaluation.id.desc())
            .limit(live_shadow_limit)
        )
    ).all()
    scorecard_rows = (
        await session.execute(
            select(PolymarketPilotScorecard)
            .where(PolymarketPilotScorecard.strategy_version_id == version.id)
            .order_by(
                PolymarketPilotScorecard.window_end.desc(),
                PolymarketPilotScorecard.created_at.desc(),
                PolymarketPilotScorecard.id.desc(),
            )
            .limit(pilot_limit)
        )
    ).scalars().all()
    readiness_rows = (
        await session.execute(
            select(PolymarketPilotReadinessReport)
            .where(PolymarketPilotReadinessReport.strategy_version_id == version.id)
            .order_by(
                PolymarketPilotReadinessReport.generated_at.desc(),
                PolymarketPilotReadinessReport.id.desc(),
            )
            .limit(pilot_limit)
        )
    ).scalars().all()
    evaluation_rows = (
        await session.execute(
            select(PromotionEvaluation)
            .where(PromotionEvaluation.strategy_version_id == version.id)
            .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
            .limit(event_limit)
        )
    ).scalars().all()
    demotion_rows = (
        await session.execute(
            select(DemotionEvent)
            .where(DemotionEvent.strategy_version_id == version.id)
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
            .limit(event_limit)
        )
    ).scalars().all()
    return {
        "family": _serialize_family_reference(family),
        "version": _serialize_strategy_version(
            version,
            evidence_counts=evidence_counts,
            evidence_alignment=alignment_by_version.get(int(version.id)),
            latest_promotion_evaluation=latest_evaluation,
        ),
        "latest_demotion_event": serialize_demotion_event(latest_demotion),
        "replay_runs": [_serialize_replay_alignment(row) for row in replay_rows],
        "live_shadow_evaluations": [
            _serialize_live_shadow_detail(row, live_order=live_order)
            for row, live_order in live_shadow_rows
        ],
        "scorecards": [_serialize_scorecard_alignment(row) for row in scorecard_rows],
        "readiness_reports": [_serialize_readiness_alignment(row) for row in readiness_rows],
        "promotion_evaluations": [serialize_promotion_evaluation(row) for row in evaluation_rows],
        "demotion_events": [serialize_demotion_event(row) for row in demotion_rows],
        "generated_at": _ensure_utc(datetime.now(timezone.utc)).isoformat(),
    }


def _serialize_strategy_version(
    row: StrategyVersion,
    *,
    evidence_counts: dict[int, dict[str, int]],
    evidence_alignment: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
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
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "evidence_alignment": evidence_alignment,
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

    alignment_by_version = await _version_evidence_alignment(session, versions=versions)
    latest_evaluation_by_version = await get_latest_promotion_evaluation_by_version(
        session,
        version_ids=[int(row.id) for row in versions if row.id is not None],
    )

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
            _serialize_strategy_version(
                version,
                evidence_counts=evidence_counts,
                evidence_alignment=alignment_by_version.get(int(version.id)) if version.id is not None else None,
                latest_promotion_evaluation=latest_evaluation_by_version.get(int(version.id)) if version.id is not None else None,
            )
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
