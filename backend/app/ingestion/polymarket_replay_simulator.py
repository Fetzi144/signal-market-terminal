from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_execution_policy import (
    POLICY_VERSION,
    ActionCandidateEvaluation,
    BookLevel,
    PolymarketExecutionContext,
    _choose_candidate,
    _entry_price_for_direction,
    _estimate_taker_fee_total,
    _evaluate_cross_now,
    _evaluate_post_best,
    _evaluate_skip,
    _evaluate_step_ahead,
    _passive_label_summary,
)
from app.ingestion.polymarket_microstructure import (
    AssetContext,
    PolymarketMicrostructureService,
    ReplayData,
    ReplayMarker,
)
from app.ingestion.polymarket_settlement import get_polymarket_canonical_settlement
from app.metrics import (
    polymarket_replay_coverage_limited_runs,
    polymarket_replay_fills_total,
    polymarket_replay_last_successful_timestamp,
    polymarket_replay_policy_comparison_runs,
    polymarket_replay_runs_total,
    polymarket_replay_scenarios_total,
    polymarket_replay_variant_fill_rate,
    polymarket_replay_variant_net_pnl,
)
from app.models.execution_decision import ExecutionDecision
from app.models.market import Outcome
from app.models.market_structure import (
    MarketStructureOpportunity,
    MarketStructureOpportunityLeg,
    MarketStructureValidation,
)
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_maker import (
    PolymarketMakerEconomicsSnapshot,
    PolymarketQuoteRecommendation,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketParamHistory
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.polymarket_replay import (
    PolymarketReplayDecisionTrace,
    PolymarketReplayFill,
    PolymarketReplayMetric,
    PolymarketReplayOrder,
    PolymarketReplayRun,
    PolymarketReplayScenario,
)
from app.models.polymarket_risk import PortfolioOptimizerRecommendation
from app.models.signal import Signal
from app.strategies.promotion import (
    PROMOTION_EVALUATION_KIND_REPLAY,
    hash_json_payload,
    map_replay_summary_to_promotion_verdict,
    record_promotion_eligibility_evaluation,
    serialize_promotion_evaluation,
    upsert_promotion_evaluation,
)
from app.strategies.registry import (
    PROMOTION_GATE_POLICY_V1,
    get_current_strategy_version,
    sync_strategy_registry,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
TEN_THOUSAND = Decimal("10000")
PRICE_Q = Decimal("0.00000001")
SIZE_Q = Decimal("0.0001")
SHARE_Q = Decimal("0.0001")
DEFAULT_POLICY_ACTIONS = ("cross_now", "post_best", "step_ahead")
DEFAULT_VARIANTS = (
    "midpoint_baseline",
    "exec_policy",
    "maker_policy",
    "structure_policy",
    "risk_adjusted",
)
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_COMPLETED_WARNINGS = "completed_with_warnings"
RUN_STATUS_FAILED = "failed"
SCENARIO_STATUS_COMPLETED = "completed"
SCENARIO_STATUS_COVERAGE = "coverage_limited"
SCENARIO_STATUS_FAILED = "failed"
SCENARIO_STATUS_SKIPPED = "skipped"
ORDER_STATUS_FILLED = "filled"
ORDER_STATUS_PARTIAL = "partial_fill"
ORDER_STATUS_CANCELLED = "cancelled"
ORDER_STATUS_BLOCKED = "blocked"
ORDER_STATUS_SKIPPED = "skipped"
POLICY_SCENARIO = "policy_comparison"
MAKER_SCENARIO = "maker_quote"
STRUCTURE_SCENARIO = "structure_package"
RUN_TYPE_SCENARIOS = {
    "single_asset_replay": {POLICY_SCENARIO},
    "policy_compare": {POLICY_SCENARIO},
    "maker_replay": {MAKER_SCENARIO},
    "structure_replay": {STRUCTURE_SCENARIO},
    "portfolio_backtest": {POLICY_SCENARIO, MAKER_SCENARIO, STRUCTURE_SCENARIO},
}

STRATEGY_FAMILY_EXEC_POLICY = "exec_policy"
RUN_TYPE_STRATEGY_FAMILY = {
    "single_asset_replay": STRATEGY_FAMILY_EXEC_POLICY,
    "policy_compare": STRATEGY_FAMILY_EXEC_POLICY,
    "maker_replay": "maker",
    "structure_replay": "structure",
}
PRIMARY_REPLAY_VARIANT_BY_FAMILY = {
    STRATEGY_FAMILY_EXEC_POLICY: "exec_policy",
    "maker": "maker_policy",
    "structure": "structure_policy",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def _quantize(value: Decimal | None, quantum: Decimal) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(quantum)


def _serialize_decimal(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _build_run_key(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"phase11-{digest[:32]}"


def _bounded_window(
    *,
    start: datetime | None,
    end: datetime | None,
) -> tuple[datetime, datetime]:
    resolved_end = _ensure_utc(end) or _utcnow()
    resolved_start = _ensure_utc(start) or (
        resolved_end - timedelta(minutes=max(settings.polymarket_replay_default_window_minutes, 1))
    )
    if resolved_start > resolved_end:
        raise ValueError("Replay window start must be <= end")
    return resolved_start, resolved_end


def _variant_metric_template(*, variant_name: str) -> dict[str, Any]:
    return {
        "metric_scope": "scenario",
        "variant_name": variant_name,
        "gross_pnl": ZERO,
        "net_pnl": ZERO,
        "fees_paid": ZERO,
        "rewards_estimated": ZERO,
        "slippage_bps": ZERO,
        "fill_rate": ZERO,
        "cancel_rate": ZERO,
        "action_mix_json": {},
        "drawdown_proxy": ZERO,
        "details_json": {},
    }


def _trade_priority_multiplier(action_type: str | None) -> Decimal:
    if action_type == "step_ahead":
        return Decimal("0.75")
    if action_type == "post_best":
        return Decimal("0.50")
    return Decimal("0.40")


def _order_direction_to_yes_price(direction: str, limit_price: Decimal) -> Decimal:
    return limit_price if direction == "buy_yes" else (ONE - limit_price).quantize(PRICE_Q)


def _directional_exit_price(direction: str, marker: ReplayMarker | None) -> Decimal | None:
    if marker is None or marker.mid is None:
        return None
    return marker.mid if direction == "buy_yes" else (ONE - marker.mid).quantize(PRICE_Q)


def _directional_settlement_exit_price(direction: str, outcome_price: Decimal | None) -> Decimal | None:
    if outcome_price is None:
        return None
    return outcome_price if direction == "buy_yes" else (ONE - outcome_price).quantize(PRICE_Q)


def _action_mix_payload(orders: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for order in orders:
        action_type = str(order.get("action_type") or order.get("status") or "unknown")
        counts[action_type] = counts.get(action_type, 0) + 1
    return counts


def _supported_replay_detector_types() -> list[str]:
    detector = str(settings.default_strategy_signal_type).strip() if settings.default_strategy_signal_type else ""
    return [detector] if detector else []


def _variant_results_coverage_limited(results: list["VariantReplayResult"]) -> bool:
    for result in results:
        details = result.metric.get("details_json") if isinstance(result.metric, dict) else None
        if isinstance(details, dict) and details.get("coverage_limited"):
            return True
    return False


@dataclass(slots=True)
class ReplayScenarioBlueprint:
    scenario_type: str
    scenario_key: str
    window_start: datetime
    window_end: datetime
    decision_at: datetime | None = None
    condition_id: str | None = None
    asset_id: str | None = None
    group_id: int | None = None
    source_execution_decision_id: uuid.UUID | None = None
    source_execution_candidate_id: int | None = None
    source_signal_id: uuid.UUID | None = None
    source_structure_opportunity_id: int | None = None
    source_quote_recommendation_id: uuid.UUID | None = None
    source_maker_snapshot_id: uuid.UUID | None = None
    source_optimizer_recommendation_id: int | None = None
    policy_version: str | None = None
    direction: str | None = None
    estimated_probability: Decimal | None = None
    market_price: Decimal | None = None
    baseline_target_size: Decimal | None = None
    market_id: uuid.UUID | None = None
    outcome_id: uuid.UUID | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class VariantReplayResult:
    variant_name: str
    orders: list[dict[str, Any]]
    fills: list[dict[str, Any]]
    traces: list[dict[str, Any]]
    metric: dict[str, Any]


class PolymarketReplaySimulatorService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._microstructure = PolymarketMicrostructureService(session_factory)
        self._last_run_at: datetime | None = None

    async def close(self) -> None:
        await self._microstructure.close()

    async def run(
        self,
        stop_event: asyncio.Event,
    ) -> None:
        if not settings.polymarket_replay_enabled:
            return
        if settings.polymarket_replay_on_startup:
            try:
                await self.run_once(reason="scheduled", run_type="policy_compare")
            except Exception:
                logger.exception("Phase 11 replay startup run failed")

        interval = max(settings.polymarket_replay_interval_seconds, 1)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                continue
            except asyncio.TimeoutError:
                pass

            try:
                await self.run_once(reason="scheduled", run_type="policy_compare")
            except Exception:
                logger.exception("Scheduled Phase 11 replay run failed")

    async def run_once(
        self,
        *,
        reason: str,
        run_type: str,
        start: datetime | None = None,
        end: datetime | None = None,
        asset_ids: list[str] | None = None,
        condition_ids: list[str] | None = None,
        opportunity_ids: list[int] | None = None,
        quote_recommendation_ids: list[uuid.UUID] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        window_start, window_end = _bounded_window(start=start, end=end)
        normalized_assets = sorted({item for item in (asset_ids or []) if item})
        normalized_conditions = sorted({item for item in (condition_ids or []) if item})
        normalized_opportunities = sorted({int(item) for item in (opportunity_ids or [])})
        normalized_quotes = sorted({str(item) for item in (quote_recommendation_ids or [])})
        scenario_limit = min(
            max(limit or settings.polymarket_replay_max_scenarios_per_run, 1),
            max(settings.polymarket_replay_max_scenarios_per_run, 1),
        )
        run_payload = {
            "run_type": run_type,
            "reason": reason,
            "window_start": window_start,
            "window_end": window_end,
            "asset_ids": normalized_assets,
            "condition_ids": normalized_conditions,
            "opportunity_ids": normalized_opportunities,
            "quote_recommendation_ids": normalized_quotes,
            "scenario_limit": scenario_limit,
            "require_complete_book_coverage": settings.polymarket_replay_require_complete_book_coverage,
            "structure_enabled": settings.polymarket_replay_enable_structure,
            "maker_enabled": settings.polymarket_replay_enable_maker,
            "risk_enabled": settings.polymarket_replay_enable_risk_adjustments,
        }
        run_key = _build_run_key(run_payload)

        async with self._session_factory() as session:
            existing = (
                await session.execute(
                    select(PolymarketReplayRun)
                    .where(PolymarketReplayRun.run_key == run_key)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if existing is not None:
                return {
                    "run": _serialize_run(existing),
                    "rows_inserted": existing.rows_inserted_json or {},
                    "idempotent_hit": True,
                }

            strategy_family = RUN_TYPE_STRATEGY_FAMILY.get(run_type)
            strategy_version = (
                await get_current_strategy_version(session, strategy_family)
                if strategy_family is not None
                else None
            )
            run = PolymarketReplayRun(
                run_key=run_key,
                run_type=run_type,
                reason=reason,
                strategy_family=strategy_family,
                strategy_version_id=strategy_version.id if strategy_version is not None else None,
                status="running",
                time_window_start=window_start,
                time_window_end=window_end,
                config_json=_json_safe(
                    {
                        **run_payload,
                        "advisory_only": True,
                        "live_disabled_by_default": not settings.polymarket_live_trading_enabled,
                        "strategy_version_key": strategy_version.version_key if strategy_version is not None else None,
                        "strategy_version_label": strategy_version.version_label if strategy_version is not None else None,
                        "strategy_version_status": strategy_version.version_status if strategy_version is not None else None,
                    }
                ),
            )
            session.add(run)
            await session.flush()

            row_counts = {
                "runs": 1,
                "scenarios": 0,
                "orders": 0,
                "fills": 0,
                "metrics": 0,
                "decision_traces": 0,
            }
            try:
                blueprints = await self._collect_blueprints(
                    session,
                    run_type=run_type,
                    window_start=window_start,
                    window_end=window_end,
                    asset_ids=normalized_assets,
                    condition_ids=normalized_conditions,
                    opportunity_ids=normalized_opportunities,
                    quote_recommendation_ids=[uuid.UUID(item) for item in normalized_quotes],
                    limit=scenario_limit,
                )
                replay_cache: dict[tuple[str, datetime, datetime], ReplayData] = {}
                scenario_metrics: list[PolymarketReplayMetric] = []
                coverage_limited_count = 0
                for blueprint in blueprints:
                    scenario = PolymarketReplayScenario(
                        run_id=run.id,
                        scenario_key=f"{run_key}:{blueprint.scenario_key}",
                        scenario_type=blueprint.scenario_type,
                        condition_id=blueprint.condition_id,
                        asset_id=blueprint.asset_id,
                        group_id=blueprint.group_id,
                        window_start=blueprint.window_start,
                        window_end=blueprint.window_end,
                        policy_version=blueprint.policy_version,
                        status="running",
                        details_json=_json_safe(blueprint.details),
                    )
                    session.add(scenario)
                    await session.flush()
                    row_counts["scenarios"] += 1
                    results = await self._process_scenario(
                        session,
                        scenario=scenario,
                        blueprint=blueprint,
                        replay_cache=replay_cache,
                    )
                    scenario.status = results["status"]
                    scenario.details_json = _json_safe(results["details"])
                    if scenario.status == SCENARIO_STATUS_COVERAGE:
                        coverage_limited_count += 1
                    polymarket_replay_scenarios_total.labels(
                        scenario_type=scenario.scenario_type,
                        status=scenario.status,
                    ).inc()
                    row_counts["orders"] += results["row_counts"]["orders"]
                    row_counts["fills"] += results["row_counts"]["fills"]
                    row_counts["metrics"] += results["row_counts"]["metrics"]
                    row_counts["decision_traces"] += results["row_counts"]["decision_traces"]
                    scenario_metrics.extend(results["metric_rows"])

                run.scenario_count = len(blueprints)
                run.completed_at = _utcnow()
                run.rows_inserted_json = _json_safe(row_counts)
                run.details_json = _json_safe(
                    {
                        "coverage_limited_scenarios": coverage_limited_count,
                        "advisory_only": True,
                        "live_disabled_by_default": not settings.polymarket_live_trading_enabled,
                    }
                )
                run.status = (
                    RUN_STATUS_COMPLETED_WARNINGS if coverage_limited_count > 0 else RUN_STATUS_COMPLETED
                )
                await self._persist_run_metrics(
                    session,
                    run=run,
                    scenario_metrics=scenario_metrics,
                )
                try:
                    await _record_phase13a_replay_evaluation(
                        session,
                        run=run,
                        strategy_version=strategy_version,
                    )
                except Exception:
                    logger.exception("Failed to record Phase 13A replay promotion evaluation for run %s", run.id)
                await session.commit()
            except Exception as exc:
                run.completed_at = _utcnow()
                run.status = RUN_STATUS_FAILED
                run.error_count += 1
                run.rows_inserted_json = _json_safe(row_counts)
                run.details_json = _json_safe({"error": str(exc)})
                try:
                    await _record_phase13a_replay_evaluation(
                        session,
                        run=run,
                        strategy_version=strategy_version,
                    )
                except Exception:
                    logger.exception("Failed to record blocked replay promotion evaluation for run %s", run.id)
                await session.commit()
                polymarket_replay_runs_total.labels(run_type=run_type, status=RUN_STATUS_FAILED).inc()
                raise

            polymarket_replay_runs_total.labels(run_type=run_type, status=run.status).inc()
            if run.run_type == "policy_compare":
                polymarket_replay_policy_comparison_runs.labels(status=run.status).inc()
            polymarket_replay_coverage_limited_runs.set(float(coverage_limited_count))
            if run.completed_at is not None:
                polymarket_replay_last_successful_timestamp.set(run.completed_at.timestamp())
            self._last_run_at = _utcnow()
            await session.refresh(run)
            return {
                "run": _serialize_run(run),
                "rows_inserted": run.rows_inserted_json or {},
                "idempotent_hit": False,
            }

    async def _collect_blueprints(
        self,
        session: AsyncSession,
        *,
        run_type: str,
        window_start: datetime,
        window_end: datetime,
        asset_ids: list[str],
        condition_ids: list[str],
        opportunity_ids: list[int],
        quote_recommendation_ids: list[uuid.UUID],
        limit: int,
    ) -> list[ReplayScenarioBlueprint]:
        scenario_types = RUN_TYPE_SCENARIOS.get(run_type, RUN_TYPE_SCENARIOS["policy_compare"])
        blueprints: list[ReplayScenarioBlueprint] = []
        if POLICY_SCENARIO in scenario_types:
            blueprints.extend(
                await self._collect_policy_blueprints(
                    session,
                    window_start=window_start,
                    window_end=window_end,
                    asset_ids=asset_ids,
                    condition_ids=condition_ids,
                )
            )
        if STRUCTURE_SCENARIO in scenario_types and settings.polymarket_replay_enable_structure:
            blueprints.extend(
                await self._collect_structure_blueprints(
                    session,
                    window_start=window_start,
                    window_end=window_end,
                    asset_ids=asset_ids,
                    condition_ids=condition_ids,
                    opportunity_ids=opportunity_ids,
                )
            )
        if MAKER_SCENARIO in scenario_types and settings.polymarket_replay_enable_maker:
            blueprints.extend(
                await self._collect_maker_blueprints(
                    session,
                    window_start=window_start,
                    window_end=window_end,
                    asset_ids=asset_ids,
                    condition_ids=condition_ids,
                    quote_recommendation_ids=quote_recommendation_ids,
                )
            )
        blueprints.sort(
            key=lambda item: (
                item.window_start,
                item.decision_at or item.window_start,
                item.scenario_type,
                item.asset_id or "",
                item.scenario_key,
            )
        )
        return blueprints[:limit]

    async def _collect_policy_blueprints(
        self,
        session: AsyncSession,
        *,
        window_start: datetime,
        window_end: datetime,
        asset_ids: list[str],
        condition_ids: list[str],
    ) -> list[ReplayScenarioBlueprint]:
        rows = (
            await session.execute(
                select(
                    ExecutionDecision,
                    Signal,
                    PolymarketExecutionActionCandidate,
                    PolymarketAssetDim,
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .outerjoin(
                    PolymarketExecutionActionCandidate,
                    PolymarketExecutionActionCandidate.id == ExecutionDecision.chosen_action_candidate_id,
                )
                .outerjoin(PolymarketAssetDim, PolymarketAssetDim.outcome_id == Signal.outcome_id)
                .where(
                    ExecutionDecision.decision_at >= window_start,
                    ExecutionDecision.decision_at <= window_end,
                )
                .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
            )
        ).all()
        blueprints: list[ReplayScenarioBlueprint] = []
        for decision, signal, chosen_candidate, asset_dim in rows:
            asset_id = (
                chosen_candidate.asset_id
                if chosen_candidate is not None
                else (signal.source_token_id or (asset_dim.asset_id if asset_dim is not None else None))
            )
            condition_id = (
                chosen_candidate.condition_id
                if chosen_candidate is not None
                else (asset_dim.condition_id if asset_dim is not None else None)
            )
            if asset_ids and asset_id not in asset_ids:
                continue
            if condition_ids and condition_id not in condition_ids:
                continue
            direction = decision.direction or (
                "buy_yes"
                if signal.estimated_probability is not None
                and signal.price_at_fire is not None
                and signal.estimated_probability >= signal.price_at_fire
                else "buy_no"
            )
            decision_at = _ensure_utc(decision.decision_at) or window_start
            scenario_end = min(window_end, decision_at + timedelta(minutes=max(settings.polymarket_replay_default_window_minutes, 1)))
            blueprints.append(
                ReplayScenarioBlueprint(
                    scenario_type=POLICY_SCENARIO,
                    scenario_key=f"policy:{decision.id}",
                    decision_at=decision_at,
                    window_start=decision_at,
                    window_end=scenario_end,
                    condition_id=condition_id,
                    asset_id=asset_id,
                    source_execution_decision_id=decision.id,
                    source_execution_candidate_id=decision.chosen_action_candidate_id,
                    source_signal_id=signal.id,
                    policy_version=POLICY_VERSION,
                    direction=direction,
                    estimated_probability=_to_decimal(signal.estimated_probability),
                    market_price=_to_decimal(signal.price_at_fire),
                    baseline_target_size=(
                        _to_decimal(decision.requested_size_usd)
                        or _to_decimal(decision.fillable_size_usd)
                        or Decimal("100.00")
                    ),
                    market_id=signal.market_id,
                    outcome_id=signal.outcome_id,
                    details={
                        "signal_id": signal.id,
                        "source_chosen_action_type": decision.chosen_action_type,
                        "source_reason_code": decision.reason_code,
                    },
                )
            )

        candidate_rows = (
            await session.execute(
                select(
                    PolymarketExecutionActionCandidate,
                    Signal,
                )
                .join(Signal, Signal.id == PolymarketExecutionActionCandidate.signal_id)
                .where(
                    PolymarketExecutionActionCandidate.execution_decision_id.is_(None),
                    PolymarketExecutionActionCandidate.valid.is_(True),
                    PolymarketExecutionActionCandidate.action_type != "skip",
                    PolymarketExecutionActionCandidate.decided_at >= window_start,
                    PolymarketExecutionActionCandidate.decided_at <= window_end,
                )
                .order_by(
                    PolymarketExecutionActionCandidate.decided_at.asc(),
                    PolymarketExecutionActionCandidate.est_net_ev_total.desc().nullslast(),
                    PolymarketExecutionActionCandidate.id.asc(),
                )
            )
        ).all()
        for candidate, signal in candidate_rows:
            if asset_ids and candidate.asset_id not in asset_ids:
                continue
            if condition_ids and candidate.condition_id not in condition_ids:
                continue
            decision_at = _ensure_utc(candidate.decided_at) or window_start
            scenario_end = min(window_end, decision_at + timedelta(minutes=max(settings.polymarket_replay_default_window_minutes, 1)))
            blueprints.append(
                ReplayScenarioBlueprint(
                    scenario_type=POLICY_SCENARIO,
                    scenario_key=f"policy-candidate:{candidate.id}",
                    decision_at=decision_at,
                    window_start=decision_at,
                    window_end=scenario_end,
                    condition_id=candidate.condition_id,
                    asset_id=candidate.asset_id,
                    source_execution_candidate_id=candidate.id,
                    source_signal_id=signal.id,
                    policy_version=candidate.policy_version or POLICY_VERSION,
                    direction=candidate.side,
                    estimated_probability=_to_decimal(signal.estimated_probability),
                    market_price=_to_decimal(signal.price_at_fire),
                    baseline_target_size=_to_decimal(candidate.target_size) or Decimal("100.00"),
                    market_id=signal.market_id,
                    outcome_id=signal.outcome_id,
                    details={
                        "signal_id": signal.id,
                        "source_action_type": candidate.action_type,
                        "source": "standalone_execution_policy_candidate",
                    },
                )
            )
        return blueprints

    async def _collect_structure_blueprints(
        self,
        session: AsyncSession,
        *,
        window_start: datetime,
        window_end: datetime,
        asset_ids: list[str],
        condition_ids: list[str],
        opportunity_ids: list[int],
    ) -> list[ReplayScenarioBlueprint]:
        rows = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(
                    MarketStructureOpportunity.observed_at_local >= window_start,
                    MarketStructureOpportunity.observed_at_local <= window_end,
                )
                .order_by(MarketStructureOpportunity.observed_at_local.asc(), MarketStructureOpportunity.id.asc())
            )
        ).scalars().all()
        blueprints: list[ReplayScenarioBlueprint] = []
        for opportunity in rows:
            if opportunity_ids and opportunity.id not in opportunity_ids:
                continue
            if condition_ids and opportunity.anchor_condition_id not in condition_ids:
                continue
            if asset_ids and opportunity.anchor_asset_id not in asset_ids:
                continue
            observed_at = _ensure_utc(opportunity.observed_at_local) or window_start
            scenario_end = min(window_end, observed_at + timedelta(minutes=max(settings.polymarket_replay_default_window_minutes, 1)))
            blueprints.append(
                ReplayScenarioBlueprint(
                    scenario_type=STRUCTURE_SCENARIO,
                    scenario_key=f"struct:{opportunity.id}",
                    decision_at=observed_at,
                    window_start=observed_at,
                    window_end=scenario_end,
                    condition_id=opportunity.anchor_condition_id,
                    asset_id=opportunity.anchor_asset_id,
                    group_id=opportunity.group_id,
                    source_structure_opportunity_id=opportunity.id,
                    details={
                        "opportunity_type": opportunity.opportunity_type,
                        "pricing_method": opportunity.pricing_method,
                        "actionable": opportunity.actionable,
                    },
                )
            )
        return blueprints

    async def _collect_maker_blueprints(
        self,
        session: AsyncSession,
        *,
        window_start: datetime,
        window_end: datetime,
        asset_ids: list[str],
        condition_ids: list[str],
        quote_recommendation_ids: list[uuid.UUID],
    ) -> list[ReplayScenarioBlueprint]:
        rows = (
            await session.execute(
                select(PolymarketQuoteRecommendation)
                .where(
                    PolymarketQuoteRecommendation.created_at >= window_start,
                    PolymarketQuoteRecommendation.created_at <= window_end,
                )
                .order_by(PolymarketQuoteRecommendation.created_at.asc(), PolymarketQuoteRecommendation.id.asc())
            )
        ).scalars().all()
        blueprints: list[ReplayScenarioBlueprint] = []
        for quote in rows:
            if quote_recommendation_ids and quote.id not in quote_recommendation_ids:
                continue
            if asset_ids and quote.asset_id not in asset_ids:
                continue
            if condition_ids and quote.condition_id not in condition_ids:
                continue
            created_at = _ensure_utc(quote.created_at) or window_start
            scenario_end = min(window_end, created_at + timedelta(minutes=max(settings.polymarket_replay_default_window_minutes, 1)))
            blueprints.append(
                ReplayScenarioBlueprint(
                    scenario_type=MAKER_SCENARIO,
                    scenario_key=f"maker:{quote.id}",
                    decision_at=created_at,
                    window_start=created_at,
                    window_end=scenario_end,
                    condition_id=quote.condition_id,
                    asset_id=quote.asset_id,
                    source_quote_recommendation_id=quote.id,
                    details={
                        "opportunity_id": quote.opportunity_id,
                        "recommendation_action": quote.recommendation_action,
                    },
                )
            )
        return blueprints

    async def _process_scenario(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        blueprint: ReplayScenarioBlueprint,
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
    ) -> dict[str, Any]:
        if blueprint.scenario_type == POLICY_SCENARIO:
            return await self._process_policy_scenario(
                session,
                scenario=scenario,
                blueprint=blueprint,
                replay_cache=replay_cache,
            )
        if blueprint.scenario_type == STRUCTURE_SCENARIO:
            return await self._process_structure_scenario(
                session,
                scenario=scenario,
                blueprint=blueprint,
                replay_cache=replay_cache,
            )
        if blueprint.scenario_type == MAKER_SCENARIO:
            return await self._process_maker_scenario(
                session,
                scenario=scenario,
                blueprint=blueprint,
                replay_cache=replay_cache,
            )
        raise ValueError(f"Unsupported replay scenario type: {blueprint.scenario_type}")

    async def _process_policy_scenario(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        blueprint: ReplayScenarioBlueprint,
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
    ) -> dict[str, Any]:
        row_counts = {"orders": 0, "fills": 0, "metrics": 0, "decision_traces": 0}
        metric_rows: list[PolymarketReplayMetric] = []
        if not blueprint.asset_id or not blueprint.condition_id:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "missing_replay_identity"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        asset_context, asset_dim, recon_state = await self._load_asset_dependencies(
            session,
            asset_id=blueprint.asset_id,
            condition_id=blueprint.condition_id,
        )
        if asset_context is None:
            return {
                "status": SCENARIO_STATUS_COVERAGE,
                "details": {"reason": "asset_context_unavailable"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        replay = await self._get_replay(
            session,
            asset_context=asset_context,
            start=blueprint.window_start,
            end=blueprint.window_end,
            replay_cache=replay_cache,
        )
        coverage = self._coverage_payload(replay)
        marker = self._microstructure._marker_as_of(replay, blueprint.decision_at or blueprint.window_start)
        estimated_probability = (
            blueprint.estimated_probability
            or (marker.mid if marker is not None and marker.mid is not None else None)
            or Decimal("0.50")
        )
        market_price = (
            blueprint.market_price
            or (marker.mid if marker is not None and marker.mid is not None else None)
            or estimated_probability
        )
        baseline_target_size = blueprint.baseline_target_size or Decimal("100.00")
        context = await self._build_execution_context(
            session,
            replay=replay,
            asset_dim=asset_dim,
            recon_state=recon_state,
            decision_at=blueprint.decision_at or blueprint.window_start,
            direction=blueprint.direction or "buy_yes",
            estimated_probability=estimated_probability,
            market_price=market_price,
            baseline_target_size=baseline_target_size,
            market_id=blueprint.market_id,
            outcome_id=blueprint.outcome_id,
        )
        if context is None:
            return {
                "status": SCENARIO_STATUS_COVERAGE,
                "details": {
                    "reason": "execution_context_unavailable",
                    "coverage": coverage,
                },
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        results = [
            await self._replay_midpoint_variant(
                session,
                context=context,
                replay=replay,
                blueprint=blueprint,
            ),
            await self._replay_execution_policy_variant(
                session,
                context=context,
                replay=replay,
                blueprint=blueprint,
                variant_name="exec_policy",
                apply_risk=False,
            ),
        ]
        if settings.polymarket_replay_enable_risk_adjustments:
            results.append(
                await self._replay_execution_policy_variant(
                    session,
                    context=context,
                    replay=replay,
                    blueprint=blueprint,
                    variant_name="risk_adjusted",
                    apply_risk=True,
                )
            )

        for result in results:
            metric_row, counts = await self._persist_variant_result(session, scenario=scenario, result=result)
            metric_rows.append(metric_row)
            for key, value in counts.items():
                row_counts[key] += value

        scenario_coverage_limited = coverage["coverage_limited"] or _variant_results_coverage_limited(results)
        return {
            "status": SCENARIO_STATUS_COVERAGE if scenario_coverage_limited else SCENARIO_STATUS_COMPLETED,
            "details": {
                "coverage": coverage,
                "coverage_limited": scenario_coverage_limited,
                "recon_status": recon_state.status if recon_state is not None else None,
                "source_execution_decision_id": blueprint.source_execution_decision_id,
                "source_execution_candidate_id": blueprint.source_execution_candidate_id,
                "variant_names": [result.variant_name for result in results],
            },
            "row_counts": row_counts,
            "metric_rows": metric_rows,
        }

    async def _process_structure_scenario(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        blueprint: ReplayScenarioBlueprint,
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
    ) -> dict[str, Any]:
        row_counts = {"orders": 0, "fills": 0, "metrics": 0, "decision_traces": 0}
        metric_rows: list[PolymarketReplayMetric] = []
        if blueprint.source_structure_opportunity_id is None:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "missing_structure_opportunity"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        opportunity = await session.get(MarketStructureOpportunity, blueprint.source_structure_opportunity_id)
        if opportunity is None:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "structure_opportunity_not_found"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        legs = (
            await session.execute(
                select(MarketStructureOpportunityLeg)
                .where(MarketStructureOpportunityLeg.opportunity_id == opportunity.id)
                .order_by(MarketStructureOpportunityLeg.leg_index.asc())
            )
        ).scalars().all()
        validation = (
            await session.execute(
                select(MarketStructureValidation)
                .where(MarketStructureValidation.opportunity_id == opportunity.id)
                .order_by(MarketStructureValidation.created_at.desc(), MarketStructureValidation.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if not legs:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "structure_legs_missing"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }
        results = [
            await self._replay_structure_variant(
                session,
                scenario=scenario,
                opportunity=opportunity,
                validation=validation,
                legs=legs,
                replay_cache=replay_cache,
                decision_at=blueprint.decision_at or blueprint.window_start,
                variant_name="midpoint_baseline",
                mode="midpoint",
            ),
            await self._replay_structure_variant(
                session,
                scenario=scenario,
                opportunity=opportunity,
                validation=validation,
                legs=legs,
                replay_cache=replay_cache,
                decision_at=blueprint.decision_at or blueprint.window_start,
                variant_name="structure_policy",
                mode="stored",
            ),
        ]
        if settings.polymarket_replay_enable_risk_adjustments:
            results.append(
                await self._replay_structure_variant(
                    session,
                    scenario=scenario,
                    opportunity=opportunity,
                    validation=validation,
                    legs=legs,
                    replay_cache=replay_cache,
                    decision_at=blueprint.decision_at or blueprint.window_start,
                    variant_name="risk_adjusted",
                    mode="risk_adjusted",
                )
            )

        coverage_limited = False
        for result in results:
            metric_row, counts = await self._persist_variant_result(session, scenario=scenario, result=result)
            metric_rows.append(metric_row)
            for key, value in counts.items():
                row_counts[key] += value
            coverage_limited = coverage_limited or bool(metric_row.details_json and metric_row.details_json.get("coverage_limited"))

        return {
            "status": SCENARIO_STATUS_COVERAGE if coverage_limited else SCENARIO_STATUS_COMPLETED,
            "details": {
                "source_structure_opportunity_id": opportunity.id,
                "opportunity_type": opportunity.opportunity_type,
                "pricing_method": opportunity.pricing_method,
                "validation_classification": validation.classification if validation is not None else None,
                "variant_names": [result.variant_name for result in results],
            },
            "row_counts": row_counts,
            "metric_rows": metric_rows,
        }

    async def _process_maker_scenario(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        blueprint: ReplayScenarioBlueprint,
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
    ) -> dict[str, Any]:
        row_counts = {"orders": 0, "fills": 0, "metrics": 0, "decision_traces": 0}
        metric_rows: list[PolymarketReplayMetric] = []
        if blueprint.source_quote_recommendation_id is None:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "missing_quote_recommendation"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        quote = await session.get(PolymarketQuoteRecommendation, blueprint.source_quote_recommendation_id)
        if quote is None:
            return {
                "status": SCENARIO_STATUS_SKIPPED,
                "details": {"reason": "quote_recommendation_not_found"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }
        snapshot = await session.get(PolymarketMakerEconomicsSnapshot, quote.snapshot_id) if quote.snapshot_id is not None else None

        asset_context, asset_dim, recon_state = await self._load_asset_dependencies(
            session,
            asset_id=quote.asset_id,
            condition_id=quote.condition_id,
        )
        if asset_context is None:
            return {
                "status": SCENARIO_STATUS_COVERAGE,
                "details": {"reason": "maker_asset_context_unavailable"},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        replay = await self._get_replay(
            session,
            asset_context=asset_context,
            start=blueprint.window_start,
            end=blueprint.window_end,
            replay_cache=replay_cache,
        )
        coverage = self._coverage_payload(replay)
        marker = self._microstructure._marker_as_of(replay, blueprint.decision_at or blueprint.window_start)
        decision_at = blueprint.decision_at or blueprint.window_start
        direction = quote.recommended_side or (snapshot.side if snapshot is not None else None) or "buy_yes"
        baseline_target_size = (
            _to_decimal(quote.recommended_notional)
            or (_to_decimal(snapshot.target_notional) if snapshot is not None else None)
            or (_to_decimal(snapshot.target_size) if snapshot is not None else None)
            or Decimal("25.00")
        )
        estimated_probability = marker.mid if marker is not None and marker.mid is not None else Decimal("0.50")
        market_price = estimated_probability
        context = await self._build_execution_context(
            session,
            replay=replay,
            asset_dim=asset_dim,
            recon_state=recon_state,
            decision_at=decision_at,
            direction=direction,
            estimated_probability=estimated_probability,
            market_price=market_price,
            baseline_target_size=baseline_target_size,
            market_id=None,
            outcome_id=None,
        )
        if context is None:
            return {
                "status": SCENARIO_STATUS_COVERAGE,
                "details": {"reason": "maker_context_unavailable", "coverage": coverage},
                "row_counts": row_counts,
                "metric_rows": metric_rows,
            }

        results = [
            await self._replay_execution_policy_variant(
                session,
                context=context,
                replay=replay,
                blueprint=ReplayScenarioBlueprint(
                    scenario_type=POLICY_SCENARIO,
                    scenario_key=f"maker-exec:{quote.id}",
                    window_start=blueprint.window_start,
                    window_end=blueprint.window_end,
                    decision_at=decision_at,
                    condition_id=quote.condition_id,
                    asset_id=quote.asset_id,
                    source_quote_recommendation_id=quote.id,
                    direction=direction,
                    estimated_probability=estimated_probability,
                    market_price=market_price,
                    baseline_target_size=baseline_target_size,
                    details={"maker_baseline": True},
                ),
                variant_name="exec_policy",
                apply_risk=False,
            ),
            await self._replay_maker_variant(
                session,
                quote=quote,
                snapshot=snapshot,
                context=context,
                replay=replay,
                variant_name="maker_policy",
                use_quote_row=False,
            ),
        ]
        if settings.polymarket_replay_enable_risk_adjustments:
            results.append(
                await self._replay_maker_variant(
                    session,
                    quote=quote,
                    snapshot=snapshot,
                    context=context,
                    replay=replay,
                    variant_name="risk_adjusted",
                    use_quote_row=True,
                )
            )

        for result in results:
            metric_row, counts = await self._persist_variant_result(session, scenario=scenario, result=result)
            metric_rows.append(metric_row)
            for key, value in counts.items():
                row_counts[key] += value

        scenario_coverage_limited = coverage["coverage_limited"] or _variant_results_coverage_limited(results)
        return {
            "status": SCENARIO_STATUS_COVERAGE if scenario_coverage_limited else SCENARIO_STATUS_COMPLETED,
            "details": {
                "coverage": coverage,
                "coverage_limited": scenario_coverage_limited,
                "quote_status": quote.status,
                "quote_recommendation_action": quote.recommendation_action,
                "comparison_winner": quote.comparison_winner,
                "source_quote_recommendation_id": quote.id,
                "source_snapshot_id": quote.snapshot_id,
                "variant_names": [result.variant_name for result in results],
            },
            "row_counts": row_counts,
            "metric_rows": metric_rows,
        }

    async def _load_asset_dependencies(
        self,
        session: AsyncSession,
        *,
        asset_id: str,
        condition_id: str,
    ) -> tuple[AssetContext | None, PolymarketAssetDim | None, PolymarketBookReconState | None]:
        asset_dim = (
            await session.execute(
                select(PolymarketAssetDim)
                .where(PolymarketAssetDim.asset_id == asset_id)
                .order_by(PolymarketAssetDim.updated_at.desc(), PolymarketAssetDim.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        recon_state = (
            await session.execute(
                select(PolymarketBookReconState)
                .where(PolymarketBookReconState.asset_id == asset_id)
                .limit(1)
            )
        ).scalar_one_or_none()
        resolved_condition = condition_id or (asset_dim.condition_id if asset_dim is not None else None)
        if not resolved_condition:
            return None, asset_dim, recon_state
        return (
            AssetContext(
                asset_id=asset_id,
                condition_id=resolved_condition,
                market_dim_id=asset_dim.market_dim_id if asset_dim is not None else None,
                asset_dim_id=asset_dim.id if asset_dim is not None else None,
                recon_state_id=recon_state.id if recon_state is not None else None,
            ),
            asset_dim,
            recon_state,
        )

    async def _get_replay(
        self,
        session: AsyncSession,
        *,
        asset_context: AssetContext,
        start: datetime,
        end: datetime,
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
    ) -> ReplayData:
        key = (asset_context.asset_id, start, end)
        if key not in replay_cache:
            replay_cache[key] = await self._microstructure._build_replay(
                session,
                context=asset_context,
                start=start,
                end=end,
            )
        return replay_cache[key]

    async def _build_execution_context(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        asset_dim: PolymarketAssetDim | None,
        recon_state: PolymarketBookReconState | None,
        decision_at: datetime,
        direction: str,
        estimated_probability: Decimal,
        market_price: Decimal,
        baseline_target_size: Decimal,
        market_id: uuid.UUID | None,
        outcome_id: uuid.UUID | None,
    ) -> PolymarketExecutionContext | None:
        marker = self._microstructure._marker_as_of(replay, decision_at)
        if marker is None:
            return None
        resolved_outcome_id = outcome_id or (asset_dim.outcome_id if asset_dim is not None else None)
        resolved_market_id = market_id
        if resolved_market_id is None and resolved_outcome_id is not None:
            outcome = await session.get(Outcome, resolved_outcome_id)
            if outcome is not None:
                resolved_market_id = outcome.market_id
        if resolved_outcome_id is None or resolved_market_id is None:
            return None

        param_row = await self._latest_param_as_of(
            session,
            condition_id=replay.context.condition_id,
            asset_id=replay.context.asset_id,
            decision_at=decision_at,
        )
        tick_size = marker.tick_size or (_to_decimal(param_row.tick_size) if param_row is not None else None)
        min_order_size = (_to_decimal(param_row.min_order_size) if param_row is not None else None) or Decimal("1")
        fees_enabled = bool(param_row.fees_enabled) if param_row is not None and param_row.fees_enabled is not None else False
        reliable_book = (
            marker.best_bid is not None
            and marker.best_ask is not None
            and marker.best_bid < marker.best_ask
            and marker.trustworthy_seed
            and not marker.affected_by_drift
        )
        book_reason = "replay_book_ok" if reliable_book else "replay_book_unreliable"
        if settings.polymarket_replay_require_complete_book_coverage and (replay.drift_times or replay.partial_event_times):
            reliable_book = False
            book_reason = "replay_coverage_incomplete"

        return PolymarketExecutionContext(
            signal_id=None,
            market_id=resolved_market_id,
            outcome_id=resolved_outcome_id,
            direction=direction,
            estimated_probability=estimated_probability,
            market_price=market_price,
            baseline_target_size=baseline_target_size,
            bankroll=Decimal(str(settings.default_bankroll)),
            decision_at=decision_at,
            asset_id=replay.context.asset_id,
            condition_id=replay.context.condition_id,
            market_dim_id=replay.context.market_dim_id,
            asset_dim_id=replay.context.asset_dim_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            fees_enabled=fees_enabled,
            taker_fee_rate=self._resolve_taker_fee_rate(param_row),
            maker_fee_rate=self._resolve_maker_fee_rate(param_row),
            fee_schedule_json=param_row.fee_schedule_json if param_row is not None else None,
            recon_state_id=replay.context.recon_state_id,
            recon_status=recon_state.status if recon_state is not None else None,
            reliable_book=reliable_book,
            book_reason=book_reason,
            best_bid=marker.best_bid,
            best_ask=marker.best_ask,
            spread=marker.spread,
            bids=[BookLevel(yes_price=price, size_shares=size) for price, size in marker.bid_levels],
            asks=[BookLevel(yes_price=price, size_shares=size) for price, size in marker.ask_levels],
            snapshot_id=marker.last_snapshot_id,
            snapshot_source_kind=None,
            snapshot_observed_at=marker.observed_at_local,
            snapshot_age_seconds=max(0, int((decision_at - marker.exchange_time).total_seconds())),
            horizon_ms=max(
                settings.polymarket_execution_policy_default_horizon_ms,
                settings.polymarket_replay_passive_fill_timeout_seconds * 1000,
            ),
            lookback_start=decision_at - timedelta(hours=max(settings.polymarket_execution_policy_passive_lookback_hours, 1)),
        )

    async def _latest_param_as_of(
        self,
        session: AsyncSession,
        *,
        condition_id: str,
        asset_id: str,
        decision_at: datetime,
    ) -> PolymarketMarketParamHistory | None:
        effective = func.coalesce(
            PolymarketMarketParamHistory.effective_at_exchange,
            PolymarketMarketParamHistory.observed_at_local,
        )
        asset_row = (
            await session.execute(
                select(PolymarketMarketParamHistory)
                .where(
                    PolymarketMarketParamHistory.condition_id == condition_id,
                    PolymarketMarketParamHistory.asset_id == asset_id,
                    effective <= decision_at,
                )
                .order_by(effective.desc(), PolymarketMarketParamHistory.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if asset_row is not None:
            return asset_row
        return (
            await session.execute(
                select(PolymarketMarketParamHistory)
                .where(
                    PolymarketMarketParamHistory.condition_id == condition_id,
                    PolymarketMarketParamHistory.asset_id.is_(None),
                    effective <= decision_at,
                )
                .order_by(effective.desc(), PolymarketMarketParamHistory.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    def _coverage_payload(self, replay: ReplayData) -> dict[str, Any]:
        coverage_limited = not replay.markers or bool(replay.drift_times) or bool(replay.partial_event_times)
        return {
            "coverage_limited": coverage_limited,
            "marker_count": len(replay.markers),
            "trade_count": len(replay.trades),
            "bbo_count": len(replay.bbo_events),
            "snapshot_boundary_count": len(replay.snapshot_boundaries),
            "drift_count": len(replay.drift_times),
            "partial_event_count": len(replay.partial_event_times),
            "latest_observed_time": replay.latest_observed_time,
        }

    def _resolve_taker_fee_rate(self, row: PolymarketMarketParamHistory | None) -> Decimal:
        if row is None:
            return ZERO
        schedule = row.fee_schedule_json or {}
        schedule_rate = _to_decimal(schedule.get("rate")) if isinstance(schedule, dict) else None
        return schedule_rate or _to_decimal(row.taker_base_fee) or ZERO

    def _resolve_maker_fee_rate(self, row: PolymarketMarketParamHistory | None) -> Decimal:
        if row is None:
            return ZERO
        return _to_decimal(row.maker_base_fee) or ZERO

    async def _replay_midpoint_variant(
        self,
        session: AsyncSession,
        *,
        context: PolymarketExecutionContext,
        replay: ReplayData,
        blueprint: ReplayScenarioBlueprint,
    ) -> VariantReplayResult:
        decision_ts = blueprint.decision_at or blueprint.window_start
        expiry_ts = min(
            blueprint.window_end,
            decision_ts + timedelta(seconds=max(settings.polymarket_replay_passive_fill_timeout_seconds, 1)),
        )
        traces = [
            {
                "trace_type": "action_choice",
                "reason_code": "midpoint_baseline",
                "payload_json": {
                    "direction": context.direction,
                    "midpoint": context.midpoint,
                    "baseline_target_size": context.baseline_target_size,
                },
                "observed_at_local": decision_ts,
            }
        ]
        if context.midpoint is None:
            order = self._base_order_payload(
                variant_name="midpoint_baseline",
                sequence_no=1,
                side=context.direction,
                action_type="midpoint_baseline",
                order_type_hint="midpoint",
                limit_price=None,
                requested_size=context.baseline_target_size,
                submitted_size=ZERO,
                status=ORDER_STATUS_SKIPPED,
                decision_ts=decision_ts,
                expiry_ts=expiry_ts,
                source_execution_decision_id=blueprint.source_execution_decision_id,
                details_json={"reason": "missing_midpoint"},
            )
            metric = await self._metric_from_execution(
                session,
                variant_name="midpoint_baseline",
                direction=context.direction,
                orders=[order],
                fills=[],
                replay=replay,
                decision_ts=decision_ts,
                additional_details={"reason": "missing_midpoint"},
            )
            return VariantReplayResult(variant_name="midpoint_baseline", orders=[order], fills=[], traces=traces, metric=metric)

        limit_price = _entry_price_for_direction(context.direction, yes_price=context.midpoint)
        order = self._base_order_payload(
            variant_name="midpoint_baseline",
            sequence_no=1,
            side=context.direction,
            action_type="midpoint_baseline",
            order_type_hint="midpoint",
            limit_price=limit_price,
            requested_size=context.baseline_target_size,
            submitted_size=context.baseline_target_size,
            status="pending",
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            source_execution_decision_id=blueprint.source_execution_decision_id,
            details_json={
                "yes_price": context.midpoint,
                "touch_bid": context.best_bid,
                "touch_ask": context.best_ask,
            },
        )
        execution = await self._simulate_order_execution(
            session,
            replay=replay,
            context=context,
            action_type="midpoint_baseline",
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            limit_price=limit_price,
            requested_size=context.baseline_target_size,
            aggressive=False,
            reward_total=ZERO,
        )
        order["status"] = execution["status"]
        order["details_json"] = {**(order["details_json"] or {}), **execution["order_details"]}
        traces.extend(execution["traces"])
        fills = execution["fills"]
        metric = await self._metric_from_execution(
            session,
            variant_name="midpoint_baseline",
            direction=context.direction,
            orders=[order],
            fills=fills,
            replay=replay,
            decision_ts=decision_ts,
            additional_details={"model": "midpoint_baseline"},
        )
        return VariantReplayResult(variant_name="midpoint_baseline", orders=[order], fills=fills, traces=traces, metric=metric)

    async def _replay_execution_policy_variant(
        self,
        session: AsyncSession,
        *,
        context: PolymarketExecutionContext,
        replay: ReplayData,
        blueprint: ReplayScenarioBlueprint,
        variant_name: str,
        apply_risk: bool,
    ) -> VariantReplayResult:
        decision_ts = blueprint.decision_at or blueprint.window_start
        expiry_ts = min(
            blueprint.window_end,
            decision_ts + timedelta(seconds=max(settings.polymarket_replay_passive_fill_timeout_seconds, 1)),
        )
        candidates = [
            await _evaluate_cross_now(session, context),
            await _evaluate_post_best(session, context),
        ]
        if settings.polymarket_execution_policy_step_ahead_enabled:
            candidates.append(await _evaluate_step_ahead(session, context))
        else:
            candidates.append(
                ActionCandidateEvaluation(
                    side=context.direction,
                    action_type="step_ahead",
                    order_type_hint="post_only",
                    decision_horizon_ms=context.horizon_ms,
                    target_size=context.baseline_target_size,
                    est_tick_size=context.tick_size,
                    est_min_order_size=context.min_order_size,
                    valid=False,
                    invalid_reason="step_ahead_disabled",
                    details_json={"reason": "step_ahead_disabled"},
                )
            )
        candidates.append(_evaluate_skip(context, candidates))
        chosen_candidate, chosen_reason = _choose_candidate(candidates)
        traces = [
            {
                "trace_type": "action_choice",
                "reason_code": chosen_reason,
                "payload_json": {
                    "apply_risk": apply_risk,
                    "candidates": [self._candidate_payload(candidate) for candidate in candidates],
                    "chosen_action": self._candidate_payload(chosen_candidate) if chosen_candidate is not None else None,
                },
                "observed_at_local": decision_ts,
            }
        ]

        if chosen_candidate is None or chosen_candidate.action_type == "skip":
            order = self._base_order_payload(
                variant_name=variant_name,
                sequence_no=1,
                side=context.direction,
                action_type="skip",
                order_type_hint=None,
                limit_price=None,
                requested_size=context.baseline_target_size,
                submitted_size=ZERO,
                status=ORDER_STATUS_SKIPPED,
                decision_ts=decision_ts,
                expiry_ts=expiry_ts,
                source_execution_decision_id=blueprint.source_execution_decision_id,
                details_json={"reason_code": chosen_reason},
            )
            metric = await self._metric_from_execution(
                session,
                variant_name=variant_name,
                direction=context.direction,
                orders=[order],
                fills=[],
                replay=replay,
                decision_ts=decision_ts,
                additional_details={"reason_code": chosen_reason},
            )
            return VariantReplayResult(variant_name=variant_name, orders=[order], fills=[], traces=traces, metric=metric)

        requested_size = _to_decimal(chosen_candidate.target_size) or context.baseline_target_size
        limit_price = self._candidate_limit_price(context=context, candidate=chosen_candidate)
        source_optimizer_recommendation_id = None
        if apply_risk:
            recommendation = await self._latest_optimizer_recommendation(
                session,
                condition_id=context.condition_id,
                asset_id=context.asset_id,
                observed_at=decision_ts,
            )
            if recommendation is not None:
                source_optimizer_recommendation_id = recommendation.id
                traces.append(
                    {
                        "trace_type": "risk_adjustment",
                        "reason_code": recommendation.reason_code,
                        "payload_json": self._optimizer_payload(recommendation),
                        "observed_at_local": decision_ts,
                    }
                )
                recommendation_type = str(recommendation.recommendation_type or "")
                cap = _to_decimal(recommendation.target_size_cap_usd)
                if recommendation_type in {"block", "no_quote"}:
                    order = self._base_order_payload(
                        variant_name=variant_name,
                        sequence_no=1,
                        side=context.direction,
                        action_type=chosen_candidate.action_type,
                        order_type_hint=chosen_candidate.order_type_hint,
                        limit_price=limit_price,
                        requested_size=requested_size,
                        submitted_size=ZERO,
                        status=ORDER_STATUS_BLOCKED,
                        decision_ts=decision_ts,
                        expiry_ts=expiry_ts,
                        source_execution_decision_id=blueprint.source_execution_decision_id,
                        source_execution_candidate_id=blueprint.source_execution_candidate_id,
                        source_optimizer_recommendation_id=source_optimizer_recommendation_id,
                        details_json={"risk_recommendation_type": recommendation_type},
                    )
                    metric = await self._metric_from_execution(
                        session,
                        variant_name=variant_name,
                        direction=context.direction,
                        orders=[order],
                        fills=[],
                        replay=replay,
                        decision_ts=decision_ts,
                        additional_details={"risk_recommendation_type": recommendation_type},
                    )
                    return VariantReplayResult(variant_name=variant_name, orders=[order], fills=[], traces=traces, metric=metric)
                if cap is not None and cap > ZERO and cap < requested_size:
                    requested_size = cap.quantize(SIZE_Q)
                shift_bps = _to_decimal(recommendation.reservation_price_adjustment_bps) or ZERO
                if shift_bps != ZERO and limit_price is not None and chosen_candidate.action_type != "cross_now":
                    limit_price = max(
                        PRICE_Q,
                        min(
                            ONE - PRICE_Q,
                            (limit_price * (ONE + (shift_bps / TEN_THOUSAND))).quantize(PRICE_Q),
                        ),
                    )

        order = self._base_order_payload(
            variant_name=variant_name,
            sequence_no=1,
            side=context.direction,
            action_type=chosen_candidate.action_type,
            order_type_hint=chosen_candidate.order_type_hint,
            limit_price=limit_price,
            requested_size=_to_decimal(chosen_candidate.target_size) or context.baseline_target_size,
            submitted_size=requested_size,
            status="pending",
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            source_execution_decision_id=blueprint.source_execution_decision_id,
            source_execution_candidate_id=blueprint.source_execution_candidate_id,
            source_optimizer_recommendation_id=source_optimizer_recommendation_id,
            details_json={
                "chosen_reason": chosen_reason,
                "candidate": self._candidate_payload(chosen_candidate),
            },
        )
        execution = await self._simulate_order_execution(
            session,
            replay=replay,
            context=context,
            action_type=chosen_candidate.action_type,
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            limit_price=limit_price,
            requested_size=requested_size,
            aggressive=chosen_candidate.action_type == "cross_now",
            reward_total=ZERO,
        )
        order["status"] = execution["status"]
        order["details_json"] = {**(order["details_json"] or {}), **execution["order_details"]}
        traces.extend(execution["traces"])
        fills = execution["fills"]
        metric = await self._metric_from_execution(
            session,
            variant_name=variant_name,
            direction=context.direction,
            orders=[order],
            fills=fills,
            replay=replay,
            decision_ts=decision_ts,
            additional_details={"chosen_reason": chosen_reason},
        )
        return VariantReplayResult(variant_name=variant_name, orders=[order], fills=fills, traces=traces, metric=metric)

    async def _replay_maker_variant(
        self,
        session: AsyncSession,
        *,
        quote: PolymarketQuoteRecommendation,
        snapshot: PolymarketMakerEconomicsSnapshot | None,
        context: PolymarketExecutionContext,
        replay: ReplayData,
        variant_name: str,
        use_quote_row: bool,
    ) -> VariantReplayResult:
        decision_ts = _ensure_utc(quote.created_at) or context.decision_at
        expiry_ts = min(
            decision_ts + timedelta(seconds=max(settings.polymarket_replay_passive_fill_timeout_seconds, 1)),
            replay.latest_observed_time or decision_ts + timedelta(seconds=max(settings.polymarket_replay_passive_fill_timeout_seconds, 1)),
        )
        traces: list[dict[str, Any]] = []
        selected_candidate = (
            snapshot.details_json.get("selected_candidate")
            if snapshot is not None and isinstance(snapshot.details_json, dict)
            else None
        )
        if use_quote_row:
            recommendation_action = quote.recommendation_action
            action_type = quote.recommended_action_type
            limit_price = _to_decimal(quote.recommended_entry_price)
            requested_size = _to_decimal(quote.recommended_notional) or _to_decimal(quote.recommended_size) or ZERO
            traces.append(
                {
                    "trace_type": "maker_adjustment",
                    "reason_code": recommendation_action or "unknown",
                    "payload_json": {
                        "quote_id": str(quote.id),
                        "reason_codes": list(quote.reason_codes_json or []),
                        "status": quote.status,
                    },
                    "observed_at_local": decision_ts,
                }
            )
        else:
            recommendation_action = "recommend_quote" if snapshot is not None and snapshot.preferred_action == "maker" else "do_not_quote"
            action_type = selected_candidate.get("action_type") if isinstance(selected_candidate, dict) else None
            limit_price = _to_decimal(selected_candidate.get("entry_price")) if isinstance(selected_candidate, dict) else None
            requested_size = (
                _to_decimal(selected_candidate.get("target_notional"))
                if isinstance(selected_candidate, dict)
                else None
            ) or (_to_decimal(snapshot.target_notional) if snapshot is not None else None) or ZERO
            traces.append(
                {
                    "trace_type": "maker_adjustment",
                    "reason_code": "maker_snapshot",
                    "payload_json": {
                        "snapshot_id": str(snapshot.id) if snapshot is not None else None,
                        "preferred_action": snapshot.preferred_action if snapshot is not None else None,
                        "selected_candidate": selected_candidate,
                    },
                    "observed_at_local": decision_ts,
                }
            )

        if recommendation_action != "recommend_quote" or action_type is None or limit_price is None or requested_size <= ZERO:
            order = self._base_order_payload(
                variant_name=variant_name,
                sequence_no=1,
                side=context.direction,
                action_type=action_type or "maker_quote",
                order_type_hint="post_only",
                limit_price=limit_price,
                requested_size=requested_size,
                submitted_size=ZERO,
                status=ORDER_STATUS_BLOCKED,
                decision_ts=decision_ts,
                expiry_ts=expiry_ts,
                source_quote_recommendation_id=quote.id,
                details_json={"recommendation_action": recommendation_action},
            )
            metric = await self._metric_from_execution(
                session,
                variant_name=variant_name,
                direction=context.direction,
                orders=[order],
                fills=[],
                replay=replay,
                decision_ts=decision_ts,
                additional_details={"recommendation_action": recommendation_action},
            )
            return VariantReplayResult(variant_name=variant_name, orders=[order], fills=[], traces=traces, metric=metric)

        order = self._base_order_payload(
            variant_name=variant_name,
            sequence_no=1,
            side=context.direction,
            action_type=action_type,
            order_type_hint="post_only",
            limit_price=limit_price,
            requested_size=requested_size,
            submitted_size=requested_size,
            status="pending",
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            source_quote_recommendation_id=quote.id,
            details_json={
                "recommendation_action": recommendation_action,
                "comparison_winner": quote.comparison_winner,
            },
        )
        reward_total = _to_decimal(snapshot.maker_rewards_total) if snapshot is not None else ZERO
        execution = await self._simulate_order_execution(
            session,
            replay=replay,
            context=context,
            action_type=action_type,
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            limit_price=limit_price,
            requested_size=requested_size,
            aggressive=False,
            reward_total=reward_total or ZERO,
        )
        order["status"] = execution["status"]
        order["details_json"] = {
            **(order["details_json"] or {}),
            **execution["order_details"],
            "maker_advantage_total": _to_decimal(snapshot.maker_advantage_total) if snapshot is not None else None,
        }
        fills = execution["fills"]
        traces.extend(execution["traces"])
        realism_adjustment = _to_decimal(snapshot.maker_realism_adjustment_total) if snapshot is not None else ZERO
        metric = await self._metric_from_execution(
            session,
            variant_name=variant_name,
            direction=context.direction,
            orders=[order],
            fills=fills,
            replay=replay,
            decision_ts=decision_ts,
            additional_details={
                "recommendation_action": recommendation_action,
                "maker_advantage_total": _to_decimal(snapshot.maker_advantage_total) if snapshot is not None else None,
            },
            adverse_selection_cost=realism_adjustment or ZERO,
        )
        return VariantReplayResult(variant_name=variant_name, orders=[order], fills=fills, traces=traces, metric=metric)

    async def _replay_structure_variant(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        opportunity: MarketStructureOpportunity,
        validation: MarketStructureValidation | None,
        legs: list[MarketStructureOpportunityLeg],
        replay_cache: dict[tuple[str, datetime, datetime], ReplayData],
        decision_at: datetime,
        variant_name: str,
        mode: str,
    ) -> VariantReplayResult:
        orders: list[dict[str, Any]] = []
        fills: list[dict[str, Any]] = []
        traces: list[dict[str, Any]] = []
        leg_fill_rates: list[Decimal] = []
        coverage_limited = False

        for leg in legs:
            requested_size = _to_decimal(leg.target_size) or ZERO
            submitted_size = requested_size
            limit_price = _to_decimal(leg.est_avg_entry_price)
            status = ORDER_STATUS_SKIPPED
            order_details: dict[str, Any] = {
                "role": leg.role,
                "venue": leg.venue,
                "valid": leg.valid,
                "mode": mode,
            }
            source_optimizer_recommendation_id = None
            if mode == "midpoint" and leg.venue == "polymarket" and leg.asset_id and leg.condition_id:
                asset_context, asset_dim, recon_state = await self._load_asset_dependencies(
                    session,
                    asset_id=leg.asset_id,
                    condition_id=leg.condition_id,
                )
                if asset_context is not None:
                    replay = await self._get_replay(
                        session,
                        asset_context=asset_context,
                        start=scenario.window_start,
                        end=scenario.window_end,
                        replay_cache=replay_cache,
                    )
                    marker = self._microstructure._marker_as_of(replay, decision_at)
                    if marker is not None and marker.mid is not None:
                        limit_price = _entry_price_for_direction(leg.side, yes_price=marker.mid)
                        fill_shares = (requested_size / limit_price).quantize(SHARE_Q) if limit_price > ZERO else ZERO
                        if fill_shares > ZERO:
                            fills.append(
                                {
                                    "sequence_no": leg.leg_index + 1,
                                    "fill_index": 1,
                                    "fill_ts": decision_at,
                                    "price": limit_price,
                                    "size": fill_shares,
                                    "fee_paid": ZERO,
                                    "reward_estimate": ZERO,
                                    "maker_taker": "maker",
                                    "fill_source_kind": "bbo_touch",
                                    "details_json": {"baseline": "midpoint_assumption"},
                                }
                            )
                            status = ORDER_STATUS_FILLED
                            leg_fill_rates.append(ONE)
                        else:
                            leg_fill_rates.append(ZERO)
                    else:
                        coverage_limited = True
                        leg_fill_rates.append(ZERO)
                else:
                    coverage_limited = True
                    leg_fill_rates.append(ZERO)
            elif leg.venue == "polymarket" and leg.asset_id and leg.condition_id:
                asset_context, asset_dim, recon_state = await self._load_asset_dependencies(
                    session,
                    asset_id=leg.asset_id,
                    condition_id=leg.condition_id,
                )
                if asset_context is None:
                    coverage_limited = True
                    leg_fill_rates.append(ZERO)
                else:
                    replay = await self._get_replay(
                        session,
                        asset_context=asset_context,
                        start=scenario.window_start,
                        end=scenario.window_end,
                        replay_cache=replay_cache,
                    )
                    coverage_limited = coverage_limited or self._coverage_payload(replay)["coverage_limited"]
                    marker = self._microstructure._marker_as_of(replay, decision_at)
                    estimated_probability = marker.mid if marker is not None and marker.mid is not None else Decimal("0.50")
                    context = await self._build_execution_context(
                        session,
                        replay=replay,
                        asset_dim=asset_dim,
                        recon_state=recon_state,
                        decision_at=decision_at,
                        direction=leg.side,
                        estimated_probability=estimated_probability,
                        market_price=estimated_probability,
                        baseline_target_size=requested_size,
                        market_id=leg.market_id,
                        outcome_id=leg.outcome_id,
                    )
                    if context is None:
                        coverage_limited = True
                        leg_fill_rates.append(ZERO)
                    else:
                        if mode == "risk_adjusted":
                            recommendation = await self._latest_optimizer_recommendation(
                                session,
                                condition_id=leg.condition_id,
                                asset_id=leg.asset_id,
                                observed_at=decision_at,
                            )
                            if recommendation is not None:
                                source_optimizer_recommendation_id = recommendation.id
                                traces.append(
                                    {
                                        "sequence_no": leg.leg_index + 1,
                                        "trace_type": "risk_adjustment",
                                        "reason_code": recommendation.reason_code,
                                        "payload_json": self._optimizer_payload(recommendation),
                                        "observed_at_local": decision_at,
                                    }
                                )
                                recommendation_type = str(recommendation.recommendation_type or "")
                                cap = _to_decimal(recommendation.target_size_cap_usd)
                                if recommendation_type in {"block", "no_quote"}:
                                    submitted_size = ZERO
                                elif cap is not None and cap > ZERO and cap < submitted_size:
                                    submitted_size = cap.quantize(SIZE_Q)
                        action_type = leg.action_type or "cross_now"
                        if limit_price is None:
                            limit_price = self._candidate_limit_price(
                                context=context,
                                candidate=ActionCandidateEvaluation(
                                    side=leg.side,
                                    action_type=action_type,
                                    order_type_hint=leg.order_type_hint,
                                    decision_horizon_ms=context.horizon_ms,
                                    target_size=submitted_size,
                                ),
                            )
                        if submitted_size <= ZERO:
                            leg_fill_rates.append(ZERO)
                        else:
                            execution = await self._simulate_order_execution(
                                session,
                                replay=replay,
                                context=context,
                                action_type=action_type,
                                decision_ts=decision_at,
                                expiry_ts=scenario.window_end,
                                limit_price=limit_price,
                                requested_size=submitted_size,
                                aggressive=action_type == "cross_now",
                                reward_total=ZERO,
                            )
                            status = execution["status"]
                            order_details.update(execution["order_details"])
                            fills.extend(
                                {
                                    **fill,
                                    "sequence_no": leg.leg_index + 1,
                                    "fill_index": index + 1,
                                }
                                for index, fill in enumerate(execution["fills"])
                            )
                            filled_notional = sum(
                                (_to_decimal(fill["price"]) or ZERO) * (_to_decimal(fill["size"]) or ZERO)
                                for fill in execution["fills"]
                            )
                            leg_fill_rates.append(
                                min(ONE, (filled_notional / submitted_size).quantize(Decimal("0.000001")))
                                if submitted_size > ZERO
                                else ZERO
                            )
            else:
                stored_fillable = _to_decimal(leg.est_fillable_size) or requested_size
                fill_ratio = min(ONE, (stored_fillable / requested_size).quantize(Decimal("0.000001"))) if requested_size > ZERO else ZERO
                leg_fill_rates.append(fill_ratio)
                if fill_ratio > ZERO and limit_price is not None:
                    fill_notional = (requested_size * fill_ratio).quantize(SIZE_Q)
                    fill_shares = (fill_notional / limit_price).quantize(SHARE_Q) if limit_price > ZERO else ZERO
                    fills.append(
                        {
                            "sequence_no": leg.leg_index + 1,
                            "fill_index": 1,
                            "fill_ts": decision_at,
                            "price": limit_price,
                            "size": fill_shares,
                            "fee_paid": _to_decimal(leg.est_fee) or ZERO,
                            "reward_estimate": ZERO,
                            "maker_taker": "taker",
                            "fill_source_kind": "book_walk",
                            "details_json": {"source": "stored_leg_estimate", "fill_ratio": fill_ratio},
                        }
                    )
                    status = ORDER_STATUS_FILLED if fill_ratio >= ONE else ORDER_STATUS_PARTIAL
                else:
                    status = ORDER_STATUS_CANCELLED
                    coverage_limited = coverage_limited or leg.venue != "polymarket"

            if status == ORDER_STATUS_SKIPPED and submitted_size > ZERO:
                status = ORDER_STATUS_CANCELLED
            order = self._base_order_payload(
                variant_name=variant_name,
                sequence_no=leg.leg_index + 1,
                side=leg.side,
                action_type=leg.action_type,
                order_type_hint=leg.order_type_hint,
                limit_price=limit_price,
                requested_size=requested_size,
                submitted_size=submitted_size,
                status=status,
                decision_ts=decision_at,
                expiry_ts=scenario.window_end,
                source_structure_opportunity_id=opportunity.id,
                source_execution_candidate_id=leg.source_execution_candidate_id,
                source_optimizer_recommendation_id=source_optimizer_recommendation_id,
                details_json=order_details,
            )
            orders.append(order)

        package_fill_ratio = min(leg_fill_rates) if leg_fill_rates else ZERO
        gross_reference = (
            _to_decimal(validation.current_gross_edge_total if validation is not None else None)
            or _to_decimal(opportunity.gross_edge_total)
            or ZERO
        )
        net_reference = (
            _to_decimal(validation.current_net_edge_total if validation is not None else None)
            or _to_decimal(opportunity.net_edge_total)
            or ZERO
        )
        fees_paid = sum((_to_decimal(leg.est_fee) or ZERO) for leg in legs)
        metric = _variant_metric_template(variant_name=variant_name)
        metric["gross_pnl"] = (gross_reference * package_fill_ratio).quantize(PRICE_Q)
        metric["net_pnl"] = (net_reference * package_fill_ratio).quantize(PRICE_Q)
        metric["fees_paid"] = (fees_paid * package_fill_ratio).quantize(PRICE_Q)
        metric["fill_rate"] = package_fill_ratio.quantize(Decimal("0.000001"))
        metric["cancel_rate"] = (ONE - package_fill_ratio).quantize(Decimal("0.000001"))
        metric["slippage_bps"] = (
            _to_decimal(validation.current_net_edge_bps if validation is not None else None)
            or _to_decimal(opportunity.net_edge_bps)
            or ZERO
        )
        metric["action_mix_json"] = _action_mix_payload(orders)
        metric["details_json"] = {
            "opportunity_type": opportunity.opportunity_type,
            "pricing_method": opportunity.pricing_method,
            "package_fill_ratio": package_fill_ratio,
            "coverage_limited": coverage_limited,
            "validation_classification": validation.classification if validation is not None else None,
            "leg_count": len(legs),
        }
        traces.append(
            {
                "trace_type": "structure_package",
                "reason_code": "package_fill_ratio",
                "payload_json": metric["details_json"],
                "observed_at_local": decision_at,
            }
        )
        return VariantReplayResult(
            variant_name=variant_name,
            orders=orders,
            fills=fills,
            traces=traces,
            metric=metric,
        )

    async def _simulate_order_execution(
        self,
        session: AsyncSession,
        *,
        replay: ReplayData,
        context: PolymarketExecutionContext,
        action_type: str,
        decision_ts: datetime,
        expiry_ts: datetime,
        limit_price: Decimal | None,
        requested_size: Decimal,
        aggressive: bool,
        reward_total: Decimal,
    ) -> dict[str, Any]:
        if requested_size <= ZERO or limit_price is None:
            return {
                "status": ORDER_STATUS_SKIPPED,
                "fills": [],
                "order_details": {"reason": "missing_limit_or_size"},
                "traces": [
                    {
                        "trace_type": "fill_model",
                        "reason_code": "missing_limit_or_size",
                        "payload_json": {"limit_price": limit_price, "requested_size": requested_size},
                        "observed_at_local": decision_ts,
                    }
                ],
            }
        if aggressive:
            return self._simulate_aggressive_order(
                context=context,
                replay=replay,
                decision_ts=decision_ts,
                limit_price=limit_price,
                requested_size=requested_size,
            )
        return await self._simulate_passive_order(
            session,
            context=context,
            replay=replay,
            action_type=action_type,
            decision_ts=decision_ts,
            expiry_ts=expiry_ts,
            limit_price=limit_price,
            requested_size=requested_size,
            reward_total=reward_total,
        )

    def _simulate_aggressive_order(
        self,
        *,
        context: PolymarketExecutionContext,
        replay: ReplayData,
        decision_ts: datetime,
        limit_price: Decimal,
        requested_size: Decimal,
    ) -> dict[str, Any]:
        marker = self._microstructure._marker_as_of(replay, decision_ts)
        if marker is None:
            return {
                "status": ORDER_STATUS_CANCELLED,
                "fills": [],
                "order_details": {"reason": "missing_replay_marker"},
                "traces": [
                    {
                        "trace_type": "fill_model",
                        "reason_code": "missing_replay_marker",
                        "payload_json": None,
                        "observed_at_local": decision_ts,
                    }
                ],
            }
        levels = marker.ask_levels if context.direction == "buy_yes" else marker.bid_levels
        touch_yes_price = marker.best_ask if context.direction == "buy_yes" else marker.best_bid
        touch_entry_price = (
            _entry_price_for_direction(context.direction, yes_price=touch_yes_price)
            if touch_yes_price is not None
            else None
        )
        walk = self._walk_levels(
            direction=context.direction,
            levels=levels,
            target_size=requested_size,
            touch_entry_price=touch_entry_price,
        )
        if walk["fillable_notional"] <= ZERO:
            return {
                "status": ORDER_STATUS_CANCELLED,
                "fills": [],
                "order_details": {"reason": "no_visible_depth"},
                "traces": [
                    {
                        "trace_type": "fill_model",
                        "reason_code": "no_visible_depth",
                        "payload_json": {"touch_entry_price": touch_entry_price},
                        "observed_at_local": decision_ts,
                    }
                ],
            }
        fills: list[dict[str, Any]] = []
        for index, path in enumerate(walk["path"], start=1):
            fee_paid = _estimate_taker_fee_total(
                fillable_size=_to_decimal(path["taken_notional"]) or ZERO,
                entry_price=_to_decimal(path["entry_price"]) or ZERO,
                fee_rate=context.taker_fee_rate,
                fees_enabled=context.fees_enabled,
            )
            fills.append(
                {
                    "fill_index": index,
                    "fill_ts": decision_ts,
                    "price": _to_decimal(path["entry_price"]) or ZERO,
                    "size": _to_decimal(path["taken_shares"]) or ZERO,
                    "fee_paid": fee_paid,
                    "reward_estimate": ZERO,
                    "maker_taker": "taker",
                    "fill_source_kind": "book_walk",
                    "details_json": {"book_level": path},
                }
            )
        status = ORDER_STATUS_FILLED if walk["fillable_notional"] >= requested_size else ORDER_STATUS_PARTIAL
        return {
            "status": status,
            "fills": fills,
            "order_details": {
                "touch_entry_price": touch_entry_price,
                "avg_entry_price": walk["avg_entry_price"],
                "worst_price": walk["worst_price"],
                "fillable_notional": walk["fillable_notional"],
                "fillable_shares": walk["fillable_shares"],
                "slippage_cost": walk["slippage_cost"],
                "slippage_bps": walk["slippage_bps"],
                "book_walk": walk["path"],
            },
            "traces": [
                {
                    "trace_type": "fill_model",
                    "reason_code": "book_walk",
                    "payload_json": {
                        "avg_entry_price": walk["avg_entry_price"],
                        "fillable_notional": walk["fillable_notional"],
                        "fillable_shares": walk["fillable_shares"],
                        "slippage_bps": walk["slippage_bps"],
                    },
                    "observed_at_local": decision_ts,
                }
            ],
        }

    async def _simulate_passive_order(
        self,
        session: AsyncSession,
        *,
        context: PolymarketExecutionContext,
        replay: ReplayData,
        action_type: str,
        decision_ts: datetime,
        expiry_ts: datetime,
        limit_price: Decimal,
        requested_size: Decimal,
        reward_total: Decimal,
    ) -> dict[str, Any]:
        label_summary = await _passive_label_summary(session, context=context, action_type=action_type)
        limit_yes_price = _order_direction_to_yes_price(context.direction, limit_price)
        requested_shares = (requested_size / limit_price).quantize(SHARE_Q) if limit_price > ZERO else ZERO
        trade_capacity_total = ZERO
        relevant_trades: list[dict[str, Any]] = []
        for trade in replay.trades:
            trade_time = _ensure_utc(trade.exchange_time) or _ensure_utc(trade.observed_at_local)
            if trade_time is None or trade_time <= decision_ts or trade_time > expiry_ts:
                continue
            if self._trade_touches_order(
                direction=context.direction,
                trade_side=trade.side,
                trade_price=trade.price,
                order_yes_price=limit_yes_price,
            ):
                scaled_size = (trade.size * _trade_priority_multiplier(action_type)).quantize(SHARE_Q)
                trade_capacity_total += scaled_size
                relevant_trades.append(
                    {
                        "fill_ts": trade_time,
                        "trade_price": trade.price,
                        "scaled_size": scaled_size,
                        "raw_size": trade.size,
                        "trade_side": trade.side,
                    }
                )

        bbo_capacity = ZERO
        bbo_touch_time: datetime | None = None
        if trade_capacity_total <= ZERO:
            for marker in replay.markers:
                marker_time = _ensure_utc(marker.exchange_time) or _ensure_utc(marker.observed_at_local)
                if marker_time is None or marker_time <= decision_ts or marker_time > expiry_ts:
                    continue
                if self._marker_touches_passive_order(direction=context.direction, marker=marker, order_yes_price=limit_yes_price):
                    depth = marker.bid_depth_top1 if context.direction == "buy_yes" else marker.ask_depth_top1
                    if depth is not None and depth > ZERO:
                        bbo_capacity = (depth * Decimal("0.25")).quantize(SHARE_Q)
                        bbo_touch_time = marker_time
                        break

        if label_summary.row_count >= settings.polymarket_execution_policy_passive_min_label_rows:
            modeled_fraction = min(ONE, label_summary.fill_probability or ZERO)
            model_reason = "passive_label_calibrated"
        elif trade_capacity_total > ZERO:
            modeled_fraction = Decimal("0.25")
            model_reason = "trade_touch_without_sufficient_label_history"
        elif bbo_capacity > ZERO:
            modeled_fraction = Decimal("0.10")
            model_reason = "bbo_touch_without_sufficient_label_history"
        else:
            modeled_fraction = ZERO
            model_reason = "timeout_without_touch"

        target_fill_shares = (requested_shares * modeled_fraction).quantize(SHARE_Q)
        target_fill_shares = min(target_fill_shares, trade_capacity_total + bbo_capacity)
        if target_fill_shares <= ZERO:
            return {
                "status": ORDER_STATUS_CANCELLED,
                "fills": [],
                "order_details": {
                    "limit_yes_price": limit_yes_price,
                    "label_summary": label_summary.as_json(),
                    "model_reason": model_reason,
                },
                "traces": [
                    {
                        "trace_type": "fill_model",
                        "reason_code": model_reason,
                        "payload_json": {
                            "limit_yes_price": limit_yes_price,
                            "label_summary": label_summary.as_json(),
                            "trade_capacity_total": trade_capacity_total,
                            "bbo_capacity": bbo_capacity,
                        },
                        "observed_at_local": expiry_ts,
                    }
                ],
            }

        fills: list[dict[str, Any]] = []
        remaining = target_fill_shares
        reward_remaining = reward_total or ZERO
        for index, trade in enumerate(relevant_trades, start=1):
            if remaining <= ZERO:
                break
            fill_size = min(remaining, _to_decimal(trade["scaled_size"]) or ZERO)
            if fill_size <= ZERO:
                continue
            fee_paid = _estimate_taker_fee_total(
                fillable_size=(fill_size * limit_price).quantize(SIZE_Q),
                entry_price=limit_price,
                fee_rate=context.maker_fee_rate,
                fees_enabled=context.fees_enabled,
            )
            reward_piece = ZERO
            if reward_total > ZERO and target_fill_shares > ZERO:
                reward_piece = ((reward_total * fill_size) / target_fill_shares).quantize(PRICE_Q)
                reward_remaining -= reward_piece
            fills.append(
                {
                    "fill_index": index,
                    "fill_ts": trade["fill_ts"],
                    "price": limit_price,
                    "size": fill_size,
                    "fee_paid": fee_paid,
                    "reward_estimate": reward_piece,
                    "maker_taker": "maker",
                    "fill_source_kind": "trade_touch",
                    "details_json": {
                        "trade_price": trade["trade_price"],
                        "trade_side": trade["trade_side"],
                        "label_summary": label_summary.as_json(),
                    },
                }
            )
            remaining -= fill_size

        if remaining > ZERO and bbo_capacity > ZERO:
            fee_paid = _estimate_taker_fee_total(
                fillable_size=(remaining * limit_price).quantize(SIZE_Q),
                entry_price=limit_price,
                fee_rate=context.maker_fee_rate,
                fees_enabled=context.fees_enabled,
            )
            fills.append(
                {
                    "fill_index": len(fills) + 1,
                    "fill_ts": bbo_touch_time or expiry_ts,
                    "price": limit_price,
                    "size": remaining,
                    "fee_paid": fee_paid,
                    "reward_estimate": reward_remaining if reward_remaining > ZERO else ZERO,
                    "maker_taker": "maker",
                    "fill_source_kind": "bbo_touch",
                    "details_json": {"bbo_touch_time": bbo_touch_time, "label_summary": label_summary.as_json()},
                }
            )
            remaining = ZERO

        filled_shares = sum((_to_decimal(fill["size"]) or ZERO) for fill in fills)
        status = ORDER_STATUS_FILLED if filled_shares >= requested_shares else ORDER_STATUS_PARTIAL
        return {
            "status": status,
            "fills": fills,
            "order_details": {
                "limit_yes_price": limit_yes_price,
                "label_summary": label_summary.as_json(),
                "modeled_fraction": modeled_fraction,
                "filled_shares": filled_shares,
                "requested_shares": requested_shares,
                "model_reason": model_reason,
            },
            "traces": [
                {
                    "trace_type": "fill_model",
                    "reason_code": model_reason,
                    "payload_json": {
                        "limit_yes_price": limit_yes_price,
                        "trade_capacity_total": trade_capacity_total,
                        "bbo_capacity": bbo_capacity,
                        "modeled_fraction": modeled_fraction,
                        "label_summary": label_summary.as_json(),
                    },
                    "observed_at_local": fills[0]["fill_ts"] if fills else expiry_ts,
                }
            ],
        }

    async def _metric_from_execution(
        self,
        session: AsyncSession,
        *,
        variant_name: str,
        direction: str,
        orders: list[dict[str, Any]],
        fills: list[dict[str, Any]],
        replay: ReplayData,
        decision_ts: datetime,
        additional_details: dict[str, Any] | None = None,
        adverse_selection_cost: Decimal = ZERO,
    ) -> dict[str, Any]:
        metric = _variant_metric_template(variant_name=variant_name)
        requested_total = sum((_to_decimal(order.get("requested_size")) or ZERO) for order in orders)
        filled_notional = sum(
            (
                ((_to_decimal(fill.get("price")) or ZERO) * (_to_decimal(fill.get("size")) or ZERO))
                for fill in fills
            ),
            ZERO,
        ).quantize(PRICE_Q)
        fees_paid = sum(((_to_decimal(fill.get("fee_paid")) or ZERO) for fill in fills), ZERO).quantize(PRICE_Q)
        rewards_estimated = sum(((_to_decimal(fill.get("reward_estimate")) or ZERO) for fill in fills), ZERO).quantize(PRICE_Q)
        settlement = await get_polymarket_canonical_settlement(
            session,
            condition_id=replay.context.condition_id,
            asset_id=replay.context.asset_id,
        )
        exit_price = _directional_settlement_exit_price(direction, settlement.outcome_price)
        coverage_limited = self._coverage_payload(replay)["coverage_limited"] or settlement.coverage_limited
        gross_pnl = ZERO
        if exit_price is not None:
            for fill in fills:
                fill_price = _to_decimal(fill.get("price")) or ZERO
                fill_size = _to_decimal(fill.get("size")) or ZERO
                gross_pnl += (fill_size * (exit_price - fill_price)).quantize(PRICE_Q)
        gross_pnl = gross_pnl.quantize(PRICE_Q)
        net_pnl = (gross_pnl - fees_paid + rewards_estimated - adverse_selection_cost).quantize(PRICE_Q)
        slippage_values = [
            _to_decimal((order.get("details_json") or {}).get("slippage_bps"))
            for order in orders
            if isinstance(order.get("details_json"), dict)
        ]
        slippage_values = [value for value in slippage_values if value is not None]
        metric["gross_pnl"] = gross_pnl
        metric["net_pnl"] = net_pnl
        metric["fees_paid"] = fees_paid
        metric["rewards_estimated"] = rewards_estimated
        metric["slippage_bps"] = (
            (sum(slippage_values, ZERO) / Decimal(len(slippage_values))).quantize(PRICE_Q)
            if slippage_values
            else ZERO
        )
        metric["fill_rate"] = (
            min(ONE, (filled_notional / requested_total)).quantize(Decimal("0.000001"))
            if requested_total > ZERO
            else ZERO
        )
        cancelled_orders = sum(
            1
            for order in orders
            if order.get("status") in {ORDER_STATUS_CANCELLED, ORDER_STATUS_BLOCKED, ORDER_STATUS_SKIPPED}
        )
        metric["cancel_rate"] = (
            (Decimal(cancelled_orders) / Decimal(len(orders))).quantize(Decimal("0.000001"))
            if orders
            else ZERO
        )
        metric["action_mix_json"] = _action_mix_payload(orders)
        metric["drawdown_proxy"] = adverse_selection_cost.quantize(PRICE_Q)
        metric["details_json"] = {
            "requested_total": requested_total,
            "filled_notional": filled_notional,
            "exit_price": exit_price,
            "coverage_limited": coverage_limited,
            "coverage_mode": "canonical_settlement" if exit_price is not None else "canonical_settlement_unavailable",
            "settlement_source_kind": settlement.source_kind,
            "settlement_resolution_state": settlement.resolution_state,
            "settlement_winning_asset_id": settlement.winning_asset_id,
            **(additional_details or {}),
        }
        return metric

    def _walk_levels(
        self,
        *,
        direction: str,
        levels: list[tuple[Decimal, Decimal]],
        target_size: Decimal,
        touch_entry_price: Decimal | None,
    ) -> dict[str, Any]:
        remaining = target_size
        fillable_notional = ZERO
        fillable_shares = ZERO
        weighted_price = ZERO
        worst_price: Decimal | None = None
        path: list[dict[str, Any]] = []
        for yes_price, visible_shares in levels:
            entry_price = _entry_price_for_direction(direction, yes_price=yes_price)
            if entry_price <= ZERO:
                continue
            available_notional = (visible_shares * entry_price).quantize(SIZE_Q)
            take_notional = min(available_notional, remaining)
            if take_notional <= ZERO:
                continue
            take_shares = (take_notional / entry_price).quantize(SHARE_Q)
            realized_notional = (take_shares * entry_price).quantize(SIZE_Q)
            fillable_notional += realized_notional
            fillable_shares += take_shares
            weighted_price += take_shares * entry_price
            worst_price = entry_price
            remaining -= realized_notional
            path.append(
                {
                    "yes_price": yes_price,
                    "entry_price": entry_price,
                    "visible_shares": visible_shares,
                    "taken_shares": take_shares,
                    "taken_notional": realized_notional,
                }
            )
            if remaining <= ZERO:
                break
        avg_entry_price = (weighted_price / fillable_shares).quantize(PRICE_Q) if fillable_shares > ZERO else None
        slippage_cost = ZERO
        slippage_bps = ZERO
        if avg_entry_price is not None and touch_entry_price is not None and touch_entry_price > ZERO:
            slippage_per_share = max(avg_entry_price - touch_entry_price, ZERO)
            slippage_cost = (fillable_shares * slippage_per_share).quantize(PRICE_Q)
            slippage_bps = (((avg_entry_price - touch_entry_price) / touch_entry_price) * TEN_THOUSAND).quantize(PRICE_Q)
        return {
            "fillable_notional": fillable_notional.quantize(SIZE_Q),
            "fillable_shares": fillable_shares.quantize(SHARE_Q),
            "avg_entry_price": avg_entry_price,
            "worst_price": _quantize(worst_price, PRICE_Q),
            "slippage_cost": slippage_cost,
            "slippage_bps": slippage_bps,
            "path": _json_safe(path),
        }

    def _candidate_limit_price(
        self,
        *,
        context: PolymarketExecutionContext,
        candidate: ActionCandidateEvaluation,
    ) -> Decimal | None:
        if candidate.action_type == "cross_now":
            yes_price = context.best_ask if context.direction == "buy_yes" else context.best_bid
        elif candidate.action_type == "post_best":
            yes_price = context.best_bid if context.direction == "buy_yes" else context.best_ask
        elif candidate.action_type == "step_ahead":
            if context.tick_size is None:
                return candidate.est_avg_entry_price
            yes_price = (
                (context.best_bid + context.tick_size).quantize(PRICE_Q)
                if context.direction == "buy_yes"
                else (context.best_ask - context.tick_size).quantize(PRICE_Q)
            )
        elif candidate.est_avg_entry_price is not None:
            return candidate.est_avg_entry_price
        else:
            return None
        if yes_price is None:
            return candidate.est_avg_entry_price
        return _entry_price_for_direction(context.direction, yes_price=yes_price)

    def _candidate_payload(self, candidate: ActionCandidateEvaluation | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        return {
            "side": candidate.side,
            "action_type": candidate.action_type,
            "order_type_hint": candidate.order_type_hint,
            "decision_horizon_ms": candidate.decision_horizon_ms,
            "target_size": _serialize_decimal(_quantize(candidate.target_size, SIZE_Q)),
            "est_fillable_size": _serialize_decimal(_quantize(candidate.est_fillable_size, SIZE_Q)),
            "est_fill_probability": _serialize_decimal(_quantize(candidate.est_fill_probability, Decimal("0.000001"))),
            "est_avg_entry_price": _serialize_decimal(_quantize(candidate.est_avg_entry_price, PRICE_Q)),
            "est_worst_price": _serialize_decimal(_quantize(candidate.est_worst_price, PRICE_Q)),
            "est_taker_fee": _serialize_decimal(_quantize(candidate.est_taker_fee, PRICE_Q)),
            "est_maker_fee": _serialize_decimal(_quantize(candidate.est_maker_fee, PRICE_Q)),
            "est_slippage_cost": _serialize_decimal(_quantize(candidate.est_slippage_cost, PRICE_Q)),
            "est_alpha_capture_bps": _serialize_decimal(_quantize(candidate.est_alpha_capture_bps, PRICE_Q)),
            "est_adverse_selection_bps": _serialize_decimal(_quantize(candidate.est_adverse_selection_bps, PRICE_Q)),
            "est_net_ev_bps": _serialize_decimal(_quantize(candidate.est_net_ev_bps, PRICE_Q)),
            "est_net_ev_total": _serialize_decimal(_quantize(candidate.est_net_ev_total, PRICE_Q)),
            "valid": candidate.valid,
            "invalid_reason": candidate.invalid_reason,
            "source_feature_row_id": candidate.source_feature_row_id,
            "source_label_summary_json": _json_safe(candidate.source_label_summary_json),
            "details_json": _json_safe(candidate.details_json),
        }

    def _trade_touches_order(
        self,
        *,
        direction: str,
        trade_side: str | None,
        trade_price: Decimal,
        order_yes_price: Decimal,
    ) -> bool:
        normalized_side = (trade_side or "").lower()
        if direction == "buy_yes":
            return trade_price <= order_yes_price and normalized_side in {"", "sell", "ask", "offer"}
        return trade_price >= order_yes_price and normalized_side in {"", "buy", "bid"}

    def _marker_touches_passive_order(
        self,
        *,
        direction: str,
        marker: ReplayMarker,
        order_yes_price: Decimal,
    ) -> bool:
        if direction == "buy_yes":
            return marker.best_bid is not None and marker.best_bid >= order_yes_price
        return marker.best_ask is not None and marker.best_ask <= order_yes_price

    async def _latest_optimizer_recommendation(
        self,
        session: AsyncSession,
        *,
        condition_id: str,
        asset_id: str,
        observed_at: datetime,
    ) -> PortfolioOptimizerRecommendation | None:
        return (
            await session.execute(
                select(PortfolioOptimizerRecommendation)
                .where(
                    PortfolioOptimizerRecommendation.observed_at_local <= observed_at,
                    or_(
                        PortfolioOptimizerRecommendation.asset_id == asset_id,
                        (
                            PortfolioOptimizerRecommendation.asset_id.is_(None)
                            & (PortfolioOptimizerRecommendation.condition_id == condition_id)
                        ),
                    ),
                )
                .order_by(
                    PortfolioOptimizerRecommendation.observed_at_local.desc(),
                    PortfolioOptimizerRecommendation.id.desc(),
                )
                .limit(1)
            )
        ).scalar_one_or_none()

    def _optimizer_payload(self, row: PortfolioOptimizerRecommendation) -> dict[str, Any]:
        return {
            "id": row.id,
            "recommendation_type": row.recommendation_type,
            "scope_kind": row.scope_kind,
            "condition_id": row.condition_id,
            "asset_id": row.asset_id,
            "target_size_cap_usd": _serialize_decimal(_to_decimal(row.target_size_cap_usd)),
            "inventory_penalty_bps": _serialize_decimal(_to_decimal(row.inventory_penalty_bps)),
            "reservation_price_adjustment_bps": _serialize_decimal(_to_decimal(row.reservation_price_adjustment_bps)),
            "maker_budget_remaining_usd": _serialize_decimal(_to_decimal(row.maker_budget_remaining_usd)),
            "taker_budget_remaining_usd": _serialize_decimal(_to_decimal(row.taker_budget_remaining_usd)),
            "reason_code": row.reason_code,
            "details_json": _json_safe(row.details_json),
            "observed_at_local": row.observed_at_local,
        }

    def _base_order_payload(
        self,
        *,
        variant_name: str,
        sequence_no: int,
        side: str | None,
        action_type: str | None,
        order_type_hint: str | None,
        limit_price: Decimal | None,
        requested_size: Decimal | None,
        submitted_size: Decimal | None,
        status: str,
        decision_ts: datetime,
        expiry_ts: datetime | None,
        source_execution_decision_id: uuid.UUID | None = None,
        source_execution_candidate_id: int | None = None,
        source_structure_opportunity_id: int | None = None,
        source_quote_recommendation_id: uuid.UUID | None = None,
        source_optimizer_recommendation_id: int | None = None,
        details_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "variant_name": variant_name,
            "sequence_no": sequence_no,
            "side": side,
            "action_type": action_type,
            "order_type_hint": order_type_hint,
            "limit_price": _quantize(limit_price, PRICE_Q),
            "requested_size": _quantize(requested_size, SIZE_Q),
            "submitted_size": _quantize(submitted_size, SIZE_Q),
            "status": status,
            "decision_ts": decision_ts,
            "expiry_ts": expiry_ts,
            "source_execution_decision_id": source_execution_decision_id,
            "source_execution_candidate_id": source_execution_candidate_id,
            "source_structure_opportunity_id": source_structure_opportunity_id,
            "source_quote_recommendation_id": source_quote_recommendation_id,
            "source_optimizer_recommendation_id": source_optimizer_recommendation_id,
            "details_json": _json_safe(details_json or {}),
        }

    async def _persist_variant_result(
        self,
        session: AsyncSession,
        *,
        scenario: PolymarketReplayScenario,
        result: VariantReplayResult,
    ) -> tuple[PolymarketReplayMetric, dict[str, int]]:
        counts = {"orders": 0, "fills": 0, "metrics": 1, "decision_traces": 0}
        order_ids: dict[int, int] = {}
        for payload in sorted(result.orders, key=lambda item: int(item["sequence_no"])):
            order = PolymarketReplayOrder(
                scenario_id=scenario.id,
                variant_name=result.variant_name,
                sequence_no=int(payload["sequence_no"]),
                side=payload.get("side"),
                action_type=payload.get("action_type"),
                order_type_hint=payload.get("order_type_hint"),
                limit_price=_to_decimal(payload.get("limit_price")),
                requested_size=_to_decimal(payload.get("requested_size")),
                submitted_size=_to_decimal(payload.get("submitted_size")),
                status=str(payload.get("status") or ORDER_STATUS_SKIPPED),
                decision_ts=_ensure_utc(payload.get("decision_ts")) or _utcnow(),
                expiry_ts=_ensure_utc(payload.get("expiry_ts")),
                source_execution_decision_id=payload.get("source_execution_decision_id"),
                source_execution_candidate_id=payload.get("source_execution_candidate_id"),
                source_structure_opportunity_id=payload.get("source_structure_opportunity_id"),
                source_quote_recommendation_id=payload.get("source_quote_recommendation_id"),
                source_optimizer_recommendation_id=payload.get("source_optimizer_recommendation_id"),
                details_json=_json_safe(payload.get("details_json")),
            )
            session.add(order)
            await session.flush()
            order_ids[int(payload["sequence_no"])] = order.id
            counts["orders"] += 1
        for payload in sorted(
            result.fills,
            key=lambda item: (int(item.get("sequence_no") or 0), int(item.get("fill_index") or 0)),
        ):
            sequence_no = int(payload.get("sequence_no") or 0)
            order_id = order_ids.get(sequence_no)
            if order_id is None:
                continue
            fill = PolymarketReplayFill(
                scenario_id=scenario.id,
                replay_order_id=order_id,
                variant_name=result.variant_name,
                fill_index=int(payload.get("fill_index") or 1),
                fill_ts=_ensure_utc(payload.get("fill_ts")) or _utcnow(),
                price=_to_decimal(payload.get("price")) or ZERO,
                size=_to_decimal(payload.get("size")) or ZERO,
                fee_paid=_to_decimal(payload.get("fee_paid")),
                reward_estimate=_to_decimal(payload.get("reward_estimate")),
                maker_taker=payload.get("maker_taker"),
                fill_source_kind=str(payload.get("fill_source_kind") or "book_walk"),
                details_json=_json_safe(payload.get("details_json")),
            )
            session.add(fill)
            polymarket_replay_fills_total.labels(
                fill_source_kind=fill.fill_source_kind,
                variant_name=result.variant_name,
            ).inc()
            counts["fills"] += 1
        for payload in result.traces:
            sequence_no = payload.get("sequence_no")
            trace = PolymarketReplayDecisionTrace(
                scenario_id=scenario.id,
                replay_order_id=order_ids.get(int(sequence_no)) if sequence_no is not None else None,
                variant_name=result.variant_name,
                trace_type=str(payload.get("trace_type") or "action_choice"),
                reason_code=str(payload.get("reason_code") or "unknown"),
                payload_json=_json_safe(payload.get("payload_json")),
                observed_at_local=_ensure_utc(payload.get("observed_at_local")) or _utcnow(),
            )
            session.add(trace)
            counts["decision_traces"] += 1
        metric = PolymarketReplayMetric(
            run_id=scenario.run_id,
            scenario_id=scenario.id,
            metric_scope=str(result.metric.get("metric_scope") or "scenario"),
            variant_name=result.variant_name,
            gross_pnl=_to_decimal(result.metric.get("gross_pnl")),
            net_pnl=_to_decimal(result.metric.get("net_pnl")),
            fees_paid=_to_decimal(result.metric.get("fees_paid")),
            rewards_estimated=_to_decimal(result.metric.get("rewards_estimated")),
            slippage_bps=_to_decimal(result.metric.get("slippage_bps")),
            fill_rate=_to_decimal(result.metric.get("fill_rate")),
            cancel_rate=_to_decimal(result.metric.get("cancel_rate")),
            action_mix_json=_json_safe(result.metric.get("action_mix_json")),
            drawdown_proxy=_to_decimal(result.metric.get("drawdown_proxy")),
            details_json=_json_safe(result.metric.get("details_json")),
        )
        session.add(metric)
        await session.flush()
        return metric, counts

    async def _persist_run_metrics(
        self,
        session: AsyncSession,
        *,
        run: PolymarketReplayRun,
        scenario_metrics: list[PolymarketReplayMetric],
    ) -> None:
        aggregated: dict[str, dict[str, Any]] = {}
        counts: dict[str, dict[str, int]] = defaultdict(lambda: {"fill": 0, "cancel": 0, "slippage": 0})
        for row in scenario_metrics:
            if row.metric_scope != "scenario":
                continue
            item = aggregated.setdefault(row.variant_name, _variant_metric_template(variant_name=row.variant_name))
            item["gross_pnl"] += _to_decimal(row.gross_pnl) or ZERO
            item["net_pnl"] += _to_decimal(row.net_pnl) or ZERO
            item["fees_paid"] += _to_decimal(row.fees_paid) or ZERO
            item["rewards_estimated"] += _to_decimal(row.rewards_estimated) or ZERO
            item["drawdown_proxy"] += _to_decimal(row.drawdown_proxy) or ZERO
            mix = row.action_mix_json if isinstance(row.action_mix_json, dict) else {}
            merged_mix = item.get("action_mix_json") or {}
            for key, value in mix.items():
                merged_mix[str(key)] = int(merged_mix.get(str(key), 0)) + int(value)
            item["action_mix_json"] = merged_mix
            if row.fill_rate is not None:
                item["fill_rate"] += _to_decimal(row.fill_rate) or ZERO
                counts[row.variant_name]["fill"] += 1
            if row.cancel_rate is not None:
                item["cancel_rate"] += _to_decimal(row.cancel_rate) or ZERO
                counts[row.variant_name]["cancel"] += 1
            if row.slippage_bps is not None:
                item["slippage_bps"] += _to_decimal(row.slippage_bps) or ZERO
                counts[row.variant_name]["slippage"] += 1

        for variant_name, values in aggregated.items():
            fill_count = max(counts[variant_name]["fill"], 1)
            cancel_count = max(counts[variant_name]["cancel"], 1)
            slippage_count = max(counts[variant_name]["slippage"], 1)
            fill_rate = (values["fill_rate"] / Decimal(fill_count)).quantize(Decimal("0.000001"))
            cancel_rate = (values["cancel_rate"] / Decimal(cancel_count)).quantize(Decimal("0.000001"))
            slippage_bps = (values["slippage_bps"] / Decimal(slippage_count)).quantize(PRICE_Q)
            session.add(
                PolymarketReplayMetric(
                    run_id=run.id,
                    scenario_id=None,
                    metric_scope="run",
                    variant_name=variant_name,
                    gross_pnl=(values["gross_pnl"]).quantize(PRICE_Q),
                    net_pnl=(values["net_pnl"]).quantize(PRICE_Q),
                    fees_paid=(values["fees_paid"]).quantize(PRICE_Q),
                    rewards_estimated=(values["rewards_estimated"]).quantize(PRICE_Q),
                    slippage_bps=slippage_bps,
                    fill_rate=fill_rate,
                    cancel_rate=cancel_rate,
                    action_mix_json=_json_safe(values["action_mix_json"]),
                    drawdown_proxy=(values["drawdown_proxy"]).quantize(PRICE_Q),
                    details_json={"scenario_count": fill_count},
                )
            )
            polymarket_replay_variant_net_pnl.labels(variant_name=variant_name).set(float(values["net_pnl"]))
            polymarket_replay_variant_fill_rate.labels(variant_name=variant_name).set(float(fill_rate))


def _serialize_decimal_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _serialize_run_metric_snapshot(row: PolymarketReplayMetric) -> dict[str, Any]:
    details = row.details_json if isinstance(row.details_json, dict) else {}
    return {
        "variant_name": row.variant_name,
        "net_pnl": _serialize_decimal_float(_to_decimal(row.net_pnl)),
        "gross_pnl": _serialize_decimal_float(_to_decimal(row.gross_pnl)),
        "fees_paid": _serialize_decimal_float(_to_decimal(row.fees_paid)),
        "rewards_estimated": _serialize_decimal_float(_to_decimal(row.rewards_estimated)),
        "fill_rate": _serialize_decimal_float(_to_decimal(row.fill_rate)),
        "cancel_rate": _serialize_decimal_float(_to_decimal(row.cancel_rate)),
        "slippage_bps": _serialize_decimal_float(_to_decimal(row.slippage_bps)),
        "drawdown_proxy": _serialize_decimal_float(_to_decimal(row.drawdown_proxy)),
        "scenario_count": int(details.get("scenario_count") or 0),
    }


async def _record_phase13a_replay_evaluation(
    session: AsyncSession,
    *,
    run: PolymarketReplayRun,
    strategy_version,
) -> None:
    family = str(run.strategy_family or "").strip().lower()
    if not family or strategy_version is None:
        return

    registry_state = await sync_strategy_registry(session)
    family_row = registry_state["family_rows"].get(family)
    gate_policy = registry_state["gate_policy_rows"].get(PROMOTION_GATE_POLICY_V1)
    if family_row is None:
        return

    run_metric_rows = (
        await session.execute(
            select(PolymarketReplayMetric)
            .where(
                PolymarketReplayMetric.run_id == run.id,
                PolymarketReplayMetric.metric_scope == "run",
            )
            .order_by(PolymarketReplayMetric.variant_name.asc())
        )
    ).scalars().all()
    variant_summaries = {
        row.variant_name: _serialize_run_metric_snapshot(row)
        for row in run_metric_rows
    }

    scenario_scope = (
        await session.execute(
            select(PolymarketReplayScenario.condition_id, PolymarketReplayScenario.asset_id)
            .where(PolymarketReplayScenario.run_id == run.id)
        )
    ).all()
    market_universe = sorted(
        {
            str(value)
            for condition_id, asset_id in scenario_scope
            for value in (condition_id, asset_id)
            if value not in (None, "")
        }
    )
    coverage_limited_scenarios = int(
        ((run.details_json or {}) if isinstance(run.details_json, dict) else {}).get("coverage_limited_scenarios")
        or 0
    )
    primary_variant = PRIMARY_REPLAY_VARIANT_BY_FAMILY.get(family)
    if primary_variant not in variant_summaries:
        primary_variant = next(iter(variant_summaries), None)
    primary_summary = variant_summaries.get(primary_variant or "")
    evaluation_status, recommended_tier = map_replay_summary_to_promotion_verdict(
        run_status=run.status,
        coverage_limited_scenarios=coverage_limited_scenarios,
        variant_count=len(variant_summaries),
    )
    config_json = run.config_json if isinstance(run.config_json, dict) else {}
    config_payload = {
        "run_type": run.run_type,
        "reason": run.reason,
        "strategy_version_key": strategy_version.version_key,
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "require_complete_book_coverage": settings.polymarket_replay_require_complete_book_coverage,
        "default_window_minutes": settings.polymarket_replay_default_window_minutes,
        "passive_fill_timeout_seconds": settings.polymarket_replay_passive_fill_timeout_seconds,
        "structure_enabled": settings.polymarket_replay_enable_structure,
        "maker_enabled": settings.polymarket_replay_enable_maker,
        "risk_adjustments_enabled": settings.polymarket_replay_enable_risk_adjustments,
        "asset_ids": config_json.get("asset_ids") or [],
        "condition_ids": config_json.get("condition_ids") or [],
        "scenario_limit": config_json.get("scenario_limit"),
    }
    summary = {
        "replay_status": run.status,
        "scenario_count": int(run.scenario_count or 0),
        "coverage_limited_scenarios": coverage_limited_scenarios,
        "variant_count": len(variant_summaries),
        "primary_variant": primary_variant,
        "primary_variant_net_pnl": primary_summary.get("net_pnl") if primary_summary is not None else None,
        "primary_variant_fill_rate": primary_summary.get("fill_rate") if primary_summary is not None else None,
        "primary_variant_rewards_estimated": primary_summary.get("rewards_estimated") if primary_summary is not None else None,
        "variant_summaries": variant_summaries,
    }
    provenance = {
        "source": "polymarket_replay_run",
        "strategy_family": family,
        "strategy_version_key": strategy_version.version_key,
        "strategy_version_status": strategy_version.version_status,
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "promotion_gate_policy_label": gate_policy.label if gate_policy is not None else None,
        "replay_run_id": str(run.id),
        "replay_run_key": run.run_key,
        "run_type": run.run_type,
        "execution_policy_version": POLICY_VERSION if family == STRATEGY_FAMILY_EXEC_POLICY else None,
        "risk_policy_version": "risk_adjusted_variant_enabled" if settings.polymarket_replay_enable_risk_adjustments else None,
        "fee_schedule_version": "replay_fee_inputs_unversioned",
        "reward_schedule_version": "replay_reward_inputs_unversioned",
        "market_universe_hash": hash_json_payload(market_universe),
        "config_hash": hash_json_payload(config_payload),
    }
    evaluation = await upsert_promotion_evaluation(
        session,
        family_id=family_row.id,
        strategy_version_id=strategy_version.id,
        gate_policy_id=gate_policy.id if gate_policy is not None else None,
        evaluation_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        evaluation_status=evaluation_status,
        autonomy_tier=recommended_tier,
        evaluation_window_start=run.time_window_start,
        evaluation_window_end=run.time_window_end,
        provenance_json=provenance,
        summary_json=summary,
    )
    await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(strategy_version.id),
        trigger_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        trigger_ref=str(run.id),
        observed_at=run.completed_at or run.time_window_end,
    )
    run_details = dict(run.details_json or {}) if isinstance(run.details_json, dict) else {}
    run_details["promotion_evaluation"] = serialize_promotion_evaluation(evaluation)
    run.details_json = _json_safe(run_details)


def _serialize_run(row: PolymarketReplayRun) -> dict[str, Any]:
    config = row.config_json if isinstance(row.config_json, dict) else {}
    details = row.details_json if isinstance(row.details_json, dict) else {}
    return {
        "id": str(row.id),
        "run_key": row.run_key,
        "run_type": row.run_type,
        "reason": row.reason,
        "strategy_family": row.strategy_family,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version_key": config.get("strategy_version_key"),
        "strategy_version_label": config.get("strategy_version_label"),
        "strategy_version_status": config.get("strategy_version_status"),
        "status": row.status,
        "scenario_count": row.scenario_count,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
        "time_window_start": row.time_window_start,
        "time_window_end": row.time_window_end,
        "promotion_evaluation": details.get("promotion_evaluation"),
        "config_json": row.config_json,
        "rows_inserted_json": row.rows_inserted_json,
        "error_count": row.error_count,
        "details_json": row.details_json,
    }


def _serialize_scenario(row: PolymarketReplayScenario) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_id": str(row.run_id),
        "scenario_key": row.scenario_key,
        "scenario_type": row.scenario_type,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "group_id": row.group_id,
        "window_start": row.window_start,
        "window_end": row.window_end,
        "policy_version": row.policy_version,
        "status": row.status,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_order(row: PolymarketReplayOrder) -> dict[str, Any]:
    return {
        "id": row.id,
        "scenario_id": row.scenario_id,
        "variant_name": row.variant_name,
        "sequence_no": row.sequence_no,
        "side": row.side,
        "action_type": row.action_type,
        "order_type_hint": row.order_type_hint,
        "limit_price": _serialize_decimal_float(_to_decimal(row.limit_price)),
        "requested_size": _serialize_decimal_float(_to_decimal(row.requested_size)),
        "submitted_size": _serialize_decimal_float(_to_decimal(row.submitted_size)),
        "status": row.status,
        "decision_ts": row.decision_ts,
        "expiry_ts": row.expiry_ts,
        "source_execution_decision_id": str(row.source_execution_decision_id) if row.source_execution_decision_id is not None else None,
        "source_execution_candidate_id": row.source_execution_candidate_id,
        "source_structure_opportunity_id": row.source_structure_opportunity_id,
        "source_quote_recommendation_id": str(row.source_quote_recommendation_id) if row.source_quote_recommendation_id is not None else None,
        "source_optimizer_recommendation_id": row.source_optimizer_recommendation_id,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_fill(row: PolymarketReplayFill) -> dict[str, Any]:
    return {
        "id": row.id,
        "scenario_id": row.scenario_id,
        "replay_order_id": row.replay_order_id,
        "variant_name": row.variant_name,
        "fill_index": row.fill_index,
        "fill_ts": row.fill_ts,
        "price": _serialize_decimal_float(_to_decimal(row.price)),
        "size": _serialize_decimal_float(_to_decimal(row.size)),
        "fee_paid": _serialize_decimal_float(_to_decimal(row.fee_paid)),
        "reward_estimate": _serialize_decimal_float(_to_decimal(row.reward_estimate)),
        "maker_taker": row.maker_taker,
        "fill_source_kind": row.fill_source_kind,
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_metric(row: PolymarketReplayMetric) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_id": str(row.run_id),
        "scenario_id": row.scenario_id,
        "metric_scope": row.metric_scope,
        "variant_name": row.variant_name,
        "gross_pnl": _serialize_decimal_float(_to_decimal(row.gross_pnl)),
        "net_pnl": _serialize_decimal_float(_to_decimal(row.net_pnl)),
        "fees_paid": _serialize_decimal_float(_to_decimal(row.fees_paid)),
        "rewards_estimated": _serialize_decimal_float(_to_decimal(row.rewards_estimated)),
        "slippage_bps": _serialize_decimal_float(_to_decimal(row.slippage_bps)),
        "fill_rate": _serialize_decimal_float(_to_decimal(row.fill_rate)),
        "cancel_rate": _serialize_decimal_float(_to_decimal(row.cancel_rate)),
        "action_mix_json": row.action_mix_json,
        "drawdown_proxy": _serialize_decimal_float(_to_decimal(row.drawdown_proxy)),
        "details_json": row.details_json,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_trace(row: PolymarketReplayDecisionTrace) -> dict[str, Any]:
    return {
        "id": row.id,
        "scenario_id": row.scenario_id,
        "replay_order_id": row.replay_order_id,
        "variant_name": row.variant_name,
        "trace_type": row.trace_type,
        "reason_code": row.reason_code,
        "payload_json": row.payload_json,
        "observed_at_local": row.observed_at_local,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def list_polymarket_replay_runs(
    session: AsyncSession,
    *,
    run_type: str | None = None,
    reason: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = select(PolymarketReplayRun)
    if run_type:
        query = query.where(PolymarketReplayRun.run_type == run_type)
    if reason:
        query = query.where(PolymarketReplayRun.reason == reason)
    if start is not None:
        query = query.where(PolymarketReplayRun.started_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketReplayRun.started_at <= _ensure_utc(end))
    rows = (
        await session.execute(query.order_by(PolymarketReplayRun.started_at.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_run(row) for row in rows]


async def list_polymarket_replay_scenarios(
    session: AsyncSession,
    *,
    run_type: str | None = None,
    scenario_type: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(PolymarketReplayScenario).join(PolymarketReplayRun, PolymarketReplayRun.id == PolymarketReplayScenario.run_id)
    if run_type:
        query = query.where(PolymarketReplayRun.run_type == run_type)
    if scenario_type:
        query = query.where(PolymarketReplayScenario.scenario_type == scenario_type)
    if condition_id:
        query = query.where(PolymarketReplayScenario.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketReplayScenario.asset_id == asset_id)
    if start is not None:
        query = query.where(PolymarketReplayScenario.window_start >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketReplayScenario.window_end <= _ensure_utc(end))
    rows = (
        await session.execute(query.order_by(PolymarketReplayScenario.window_start.desc(), PolymarketReplayScenario.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_scenario(row) for row in rows]


async def get_polymarket_replay_scenario_detail(session: AsyncSession, *, scenario_id: int) -> dict[str, Any] | None:
    scenario = await session.get(PolymarketReplayScenario, scenario_id)
    if scenario is None:
        return None
    orders = (
        await session.execute(
            select(PolymarketReplayOrder)
            .where(PolymarketReplayOrder.scenario_id == scenario_id)
            .order_by(PolymarketReplayOrder.variant_name.asc(), PolymarketReplayOrder.sequence_no.asc())
        )
    ).scalars().all()
    fills = (
        await session.execute(
            select(PolymarketReplayFill)
            .where(PolymarketReplayFill.scenario_id == scenario_id)
            .order_by(PolymarketReplayFill.variant_name.asc(), PolymarketReplayFill.fill_ts.asc(), PolymarketReplayFill.fill_index.asc())
        )
    ).scalars().all()
    metrics = (
        await session.execute(
            select(PolymarketReplayMetric)
            .where(PolymarketReplayMetric.scenario_id == scenario_id)
            .order_by(PolymarketReplayMetric.variant_name.asc())
        )
    ).scalars().all()
    traces = (
        await session.execute(
            select(PolymarketReplayDecisionTrace)
            .where(PolymarketReplayDecisionTrace.scenario_id == scenario_id)
            .order_by(PolymarketReplayDecisionTrace.variant_name.asc(), PolymarketReplayDecisionTrace.observed_at_local.asc(), PolymarketReplayDecisionTrace.id.asc())
        )
    ).scalars().all()
    return {
        "scenario": _serialize_scenario(scenario),
        "orders": [_serialize_order(row) for row in orders],
        "fills": [_serialize_fill(row) for row in fills],
        "metrics": [_serialize_metric(row) for row in metrics],
        "decision_traces": [_serialize_trace(row) for row in traces],
    }


async def list_polymarket_replay_metrics(
    session: AsyncSession,
    *,
    run_type: str | None = None,
    scenario_type: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    variant_name: str | None = None,
    metric_scope: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    query = select(PolymarketReplayMetric).join(PolymarketReplayRun, PolymarketReplayRun.id == PolymarketReplayMetric.run_id)
    query = query.join(
        PolymarketReplayScenario,
        PolymarketReplayScenario.id == PolymarketReplayMetric.scenario_id,
        isouter=True,
    )
    if run_type:
        query = query.where(PolymarketReplayRun.run_type == run_type)
    if scenario_type:
        query = query.where(PolymarketReplayScenario.scenario_type == scenario_type)
    if condition_id:
        query = query.where(PolymarketReplayScenario.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketReplayScenario.asset_id == asset_id)
    if variant_name:
        query = query.where(PolymarketReplayMetric.variant_name == variant_name)
    if metric_scope:
        query = query.where(PolymarketReplayMetric.metric_scope == metric_scope)
    if start is not None:
        query = query.where(PolymarketReplayMetric.created_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketReplayMetric.created_at <= _ensure_utc(end))
    rows = (
        await session.execute(query.order_by(PolymarketReplayMetric.created_at.desc(), PolymarketReplayMetric.id.desc()).limit(limit))
    ).scalars().all()
    return [_serialize_metric(row) for row in rows]


async def list_polymarket_replay_decision_traces(
    session: AsyncSession,
    *,
    run_type: str | None = None,
    scenario_type: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    variant_name: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 250,
) -> list[dict[str, Any]]:
    query = (
        select(PolymarketReplayDecisionTrace)
        .join(PolymarketReplayScenario, PolymarketReplayScenario.id == PolymarketReplayDecisionTrace.scenario_id)
        .join(PolymarketReplayRun, PolymarketReplayRun.id == PolymarketReplayScenario.run_id)
    )
    if run_type:
        query = query.where(PolymarketReplayRun.run_type == run_type)
    if scenario_type:
        query = query.where(PolymarketReplayScenario.scenario_type == scenario_type)
    if condition_id:
        query = query.where(PolymarketReplayScenario.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketReplayScenario.asset_id == asset_id)
    if variant_name:
        query = query.where(PolymarketReplayDecisionTrace.variant_name == variant_name)
    if start is not None:
        query = query.where(PolymarketReplayDecisionTrace.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketReplayDecisionTrace.observed_at_local <= _ensure_utc(end))
    rows = (
        await session.execute(
            query.order_by(
                PolymarketReplayDecisionTrace.observed_at_local.desc(),
                PolymarketReplayDecisionTrace.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [_serialize_trace(row) for row in rows]


async def fetch_polymarket_replay_policy_summary(session: AsyncSession) -> dict[str, Any]:
    latest_policy_run = (
        await session.execute(
            select(PolymarketReplayRun)
            .where(
                PolymarketReplayRun.run_type == "policy_compare",
                PolymarketReplayRun.status.in_((RUN_STATUS_COMPLETED, RUN_STATUS_COMPLETED_WARNINGS)),
            )
            .order_by(PolymarketReplayRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if latest_policy_run is None:
        return {"run": None, "variants": {}}
    metrics = (
        await session.execute(
            select(PolymarketReplayMetric)
            .where(
                PolymarketReplayMetric.run_id == latest_policy_run.id,
                PolymarketReplayMetric.metric_scope == "run",
            )
            .order_by(PolymarketReplayMetric.variant_name.asc())
        )
    ).scalars().all()
    return {
        "run": _serialize_run(latest_policy_run),
        "variants": {row.variant_name: _serialize_metric(row) for row in metrics},
    }


async def fetch_polymarket_replay_status(session: AsyncSession) -> dict[str, Any]:
    recent_runs = (
        await session.execute(
            select(PolymarketReplayRun).order_by(PolymarketReplayRun.started_at.desc()).limit(10)
        )
    ).scalars().all()
    latest_run = recent_runs[0] if recent_runs else None
    latest_successful_policy_run = (
        await session.execute(
            select(PolymarketReplayRun)
            .where(
                PolymarketReplayRun.run_type == "policy_compare",
                PolymarketReplayRun.status.in_((RUN_STATUS_COMPLETED, RUN_STATUS_COMPLETED_WARNINGS)),
            )
            .order_by(PolymarketReplayRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    since = _utcnow() - timedelta(hours=24)
    recent_scenario_count = int(
        (
            await session.execute(
                select(func.count(PolymarketReplayScenario.id)).where(PolymarketReplayScenario.created_at >= since)
            )
        ).scalar_one()
        or 0
    )
    coverage_limited_run_count = int(
        (
            await session.execute(
                select(func.count(PolymarketReplayRun.id)).where(
                    PolymarketReplayRun.started_at >= since,
                    PolymarketReplayRun.status == RUN_STATUS_COMPLETED_WARNINGS,
                )
            )
        ).scalar_one()
        or 0
    )
    failed_run_count = int(
        (
            await session.execute(
                select(func.count(PolymarketReplayRun.id)).where(
                    PolymarketReplayRun.started_at >= since,
                    PolymarketReplayRun.status == RUN_STATUS_FAILED,
                )
            )
        ).scalar_one()
        or 0
    )
    observed_detector_rows = (
        await session.execute(
            select(Signal.signal_type)
            .where(Signal.fired_at >= since)
            .order_by(Signal.fired_at.desc())
            .limit(5000)
        )
    ).scalars().all()
    if not observed_detector_rows:
        observed_detector_rows = (
            await session.execute(
                select(Signal.signal_type)
                .order_by(Signal.fired_at.desc())
                .limit(5000)
            )
        ).scalars().all()
    observed_detectors = sorted({str(row) for row in observed_detector_rows if row})
    supported_detector_set = set(_supported_replay_detector_types())
    supported_detectors = [detector for detector in observed_detectors if detector in supported_detector_set]
    unsupported_detectors = [detector for detector in observed_detectors if detector not in supported_detector_set]
    if not observed_detectors:
        coverage_mode = "no_detector_activity"
    elif supported_detectors and unsupported_detectors:
        coverage_mode = "partial_supported_detectors"
    elif supported_detectors:
        coverage_mode = "supported_detectors_only"
    else:
        coverage_mode = "unsupported_detectors_only"
    summary = await fetch_polymarket_replay_policy_summary(session)
    return {
        "enabled": settings.polymarket_replay_enabled,
        "on_startup": settings.polymarket_replay_on_startup,
        "interval_seconds": settings.polymarket_replay_interval_seconds,
        "default_window_minutes": settings.polymarket_replay_default_window_minutes,
        "max_scenarios_per_run": settings.polymarket_replay_max_scenarios_per_run,
        "structure_enabled": settings.polymarket_replay_enable_structure,
        "maker_enabled": settings.polymarket_replay_enable_maker,
        "risk_adjustments_enabled": settings.polymarket_replay_enable_risk_adjustments,
        "require_complete_book_coverage": settings.polymarket_replay_require_complete_book_coverage,
        "passive_fill_timeout_seconds": settings.polymarket_replay_passive_fill_timeout_seconds,
        "advisory_only": True,
        "live_disabled_by_default": not settings.polymarket_live_trading_enabled,
        "last_replay_run": _serialize_run(latest_run) if latest_run is not None else None,
        "last_successful_policy_comparison": _serialize_run(latest_successful_policy_run) if latest_successful_policy_run is not None else None,
        "recent_scenario_count_24h": recent_scenario_count,
        "recent_coverage_limited_run_count_24h": coverage_limited_run_count,
        "recent_failed_run_count_24h": failed_run_count,
        "coverage_mode": coverage_mode,
        "configured_supported_detectors": _supported_replay_detector_types(),
        "supported_detectors": supported_detectors,
        "unsupported_detectors": unsupported_detectors,
        "recent_variant_summary": summary["variants"],
        "recent_runs": [_serialize_run(row) for row in recent_runs],
    }


async def trigger_manual_polymarket_replay(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    reason: str,
    run_type: str,
    start: datetime | None = None,
    end: datetime | None = None,
    asset_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
    opportunity_ids: list[int] | None = None,
    quote_recommendation_ids: list[uuid.UUID] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    service = PolymarketReplaySimulatorService(session_factory)
    try:
        return await service.run_once(
            reason=reason,
            run_type=run_type,
            start=start,
            end=end,
            asset_ids=asset_ids,
            condition_ids=condition_ids,
            opportunity_ids=opportunity_ids,
            quote_recommendation_ids=quote_recommendation_ids,
            limit=limit,
        )
    finally:
        await service.close()
