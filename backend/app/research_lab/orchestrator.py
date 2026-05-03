from __future__ import annotations

import hashlib
import inspect
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.backtesting.engine import BacktestEngine
from app.backtesting.modes import DETECTOR_REPLAY_MODE, STRATEGY_COMPARISON_REPLAY_MODE, with_replay_mode
from app.backtesting.sweep import parameter_sweep
from app.config import settings
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_policy_summary
from app.models.backtest import BacktestRun
from app.models.market_structure import MarketStructureOpportunity
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_maker import PolymarketQuoteRecommendation
from app.models.research_lab import ResearchBatch, ResearchLaneResult
from app.models.signal import Signal
from app.models.snapshot import PriceSnapshot
from app.paper_trading.analysis import get_profitability_snapshot
from app.reports.alpha_factory import alpha_factory_lane_payload, build_alpha_factory_snapshot
from app.reports.kalshi_down_yes_fade import (
    build_kalshi_down_yes_fade_snapshot,
    kalshi_down_yes_fade_lane_payload,
)
from app.reports.kalshi_low_yes_fade import (
    build_kalshi_low_yes_fade_snapshot,
    kalshi_low_yes_fade_lane_payload,
)
from app.reports.profit_tools import build_profit_tools_snapshot
from app.research_lab.artifacts import write_research_batch_artifacts
from app.research_lab.normalizer import (
    normalize_backtest_run,
    normalize_profit_tools,
    normalize_profitability_snapshot,
)
from app.research_lab.ranker import rank_lane_payloads
from app.research_lab.universe import select_research_universe

PRESET_PROFIT_HUNT_V1 = "profit_hunt_v1"
DEFAULT_FAMILIES = ("default_strategy", "kalshi_down_yes_fade", "kalshi_low_yes_fade", "alpha_factory")
RETIRED_POLYMARKET_FAMILIES = {
    "structure": "structure_replay",
    "maker": "maker_replay",
    "exec_policy": "execution_policy_replay",
}
SUPPORTED_FAMILIES = frozenset((*DEFAULT_FAMILIES, *RETIRED_POLYMARKET_FAMILIES.keys()))
MAX_INLINE_STRATEGY_CONTROL_SIGNALS = 25_000
MAX_INLINE_DETECTOR_SNAPSHOTS = 25_000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _default_window_end() -> datetime:
    now = _utcnow()
    return now.replace(minute=0, second=0, microsecond=0)


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_families(families: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized = [str(family).strip().lower() for family in (families or DEFAULT_FAMILIES) if str(family).strip()]
    unique = []
    for family in normalized:
        if family == "cross_venue_basis":
            continue
        if family not in SUPPORTED_FAMILIES:
            raise ValueError(f"Unsupported research family: {family}")
        if family not in unique:
            unique.append(family)
    return unique or list(DEFAULT_FAMILIES)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized else None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _batch_summary(batch: ResearchBatch) -> dict[str, Any]:
    return {
        "id": str(batch.id),
        "batch_key": batch.batch_key,
        "preset": batch.preset,
        "status": batch.status,
        "window_start": _json_safe(batch.window_start),
        "window_end": _json_safe(batch.window_end),
        "window_days": batch.window_days,
        "max_markets": batch.max_markets,
        "universe_fingerprint": batch.universe_fingerprint,
        "families": batch.families_json or [],
        "config": batch.config_json or {},
        "universe": batch.universe_json or {},
        "rows_inserted": batch.rows_inserted_json or {},
        "error_count": batch.error_count,
        "details": batch.details_json or {},
        "created_at": _json_safe(batch.created_at),
        "started_at": _json_safe(batch.started_at),
        "completed_at": _json_safe(batch.completed_at),
    }


def _lane_summary(row: ResearchLaneResult) -> dict[str, Any]:
    return {
        "id": row.id,
        "batch_id": str(row.batch_id),
        "family": row.family,
        "strategy_version": row.strategy_version,
        "lane": row.lane,
        "source_kind": row.source_kind,
        "source_ref": row.source_ref,
        "status": row.status,
        "verdict": row.verdict,
        "rank_position": row.rank_position,
        "rank_key": row.rank_key or {},
        "realized_pnl": _to_float(row.realized_pnl),
        "mark_to_market_pnl": _to_float(row.mark_to_market_pnl),
        "replay_net_pnl": _to_float(row.replay_net_pnl),
        "avg_clv": _to_float(row.avg_clv),
        "resolved_trades": row.resolved_trades,
        "fill_rate": _to_float(row.fill_rate),
        "drawdown": _to_float(row.drawdown),
        "open_exposure": _to_float(row.open_exposure),
        "coverage_mode": row.coverage_mode,
        "blockers": row.blockers_json or [],
        "details": row.details_json or {},
        "created_at": _json_safe(row.created_at),
        "updated_at": _json_safe(row.updated_at),
    }


def _payload_to_lane_result(batch_id: uuid.UUID, payload: dict[str, Any]) -> ResearchLaneResult:
    return ResearchLaneResult(
        batch_id=batch_id,
        family=str(payload.get("family") or "unknown"),
        strategy_version=payload.get("strategy_version"),
        lane=str(payload.get("lane") or "unknown"),
        source_kind=str(payload.get("source_kind") or "research_lab"),
        source_ref=payload.get("source_ref"),
        status=str(payload.get("status") or "completed"),
        verdict=str(payload.get("verdict") or "insufficient_evidence"),
        rank_position=payload.get("rank_position"),
        rank_key=_json_safe(payload.get("rank_key") or {}),
        realized_pnl=payload.get("realized_pnl"),
        mark_to_market_pnl=payload.get("mark_to_market_pnl"),
        replay_net_pnl=payload.get("replay_net_pnl"),
        avg_clv=payload.get("avg_clv"),
        resolved_trades=int(payload.get("resolved_trades") or 0),
        fill_rate=payload.get("fill_rate"),
        drawdown=payload.get("drawdown"),
        open_exposure=payload.get("open_exposure"),
        coverage_mode=payload.get("coverage_mode"),
        blockers_json=_json_safe(payload.get("blockers") or []),
        details_json=_json_safe(payload.get("details_json") or {}),
    )


def _skipped_payload(*, family: str, lane: str, blocker: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "family": family,
        "strategy_version": None,
        "lane": lane,
        "source_kind": "research_lab",
        "source_ref": None,
        "status": "skipped",
        "verdict": "insufficient_evidence",
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": None,
        "avg_clv": None,
        "resolved_trades": 0,
        "fill_rate": None,
        "drawdown": None,
        "open_exposure": None,
        "coverage_mode": "not_run",
        "blockers": [blocker],
        "details_json": details or {},
    }


def _failed_payload(*, family: str, lane: str, exc: Exception) -> dict[str, Any]:
    return {
        **_skipped_payload(family=family, lane=lane, blocker="lane_execution_failed"),
        "status": "failed",
        "details_json": {"error": str(exc), "error_type": type(exc).__name__},
    }


def _retired_polymarket_payload(*, family: str, lane: str) -> dict[str, Any]:
    return {
        **_skipped_payload(
            family=family,
            lane=lane,
            blocker="retired_polymarket_lane",
            details={
                "disabled_reason": (
                    "Polymarket research and execution lanes are retired in this deployment; "
                    "profit work is Kalshi-only."
                ),
                "next_step": "use_kalshi_only_research_lanes",
            },
        ),
        "status": "retired",
    }


async def create_research_batch(
    session: AsyncSession,
    *,
    preset: str = PRESET_PROFIT_HUNT_V1,
    window_days: int = 30,
    max_markets: int = 500,
    families: list[str] | None = None,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> tuple[ResearchBatch, bool]:
    if preset != PRESET_PROFIT_HUNT_V1:
        raise ValueError(f"Unsupported research preset: {preset}")
    normalized_families = _normalize_families(families)
    days = max(1, min(int(window_days), 180))
    capped_markets = max(1, min(int(max_markets), 5000))
    end = _ensure_utc(window_end) or _default_window_end()
    start = _ensure_utc(window_start) or (end - timedelta(days=days))
    if start >= end:
        raise ValueError("window_start must be before window_end")

    universe = await select_research_universe(
        session,
        window_start=start,
        window_end=end,
        max_markets=capped_markets,
    )
    batch_key = _hash_payload(
        {
            "preset": preset,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "window_days": days,
            "max_markets": capped_markets,
            "families": normalized_families,
            "universe_fingerprint": universe["fingerprint"],
        }
    )
    existing = (
        await session.execute(select(ResearchBatch).where(ResearchBatch.batch_key == batch_key))
    ).scalar_one_or_none()
    if existing is not None:
        return existing, True

    batch = ResearchBatch(
        id=uuid.uuid4(),
        batch_key=batch_key,
        preset=preset,
        status="pending",
        window_start=start,
        window_end=end,
        window_days=days,
        max_markets=capped_markets,
        universe_fingerprint=universe["fingerprint"],
        families_json=normalized_families,
        universe_json=universe,
        config_json={
            "paper_only": True,
            "live_orders_enabled": False,
            "pilot_arming_enabled": False,
            "manual_approval_relaxation": False,
            "live_trading_enabled_env": bool(settings.polymarket_live_trading_enabled),
            "live_dry_run_env": bool(settings.polymarket_live_dry_run),
            "live_manual_approval_required_env": bool(settings.polymarket_live_manual_approval_required),
            "pilot_enabled_env": bool(settings.polymarket_pilot_enabled),
        },
        rows_inserted_json={},
        details_json={},
    )
    session.add(batch)
    await session.commit()
    await session.refresh(batch)
    return batch, False


async def _run_default_control(session: AsyncSession, batch: ResearchBatch) -> dict[str, Any]:
    signal_count = int(
        (
            await session.execute(
                select(func.count(Signal.id)).where(
                    Signal.fired_at >= batch.window_start,
                    Signal.fired_at <= batch.window_end,
                    Signal.signal_type.in_([settings.default_strategy_signal_type, "price_move", "volume_spike"]),
                )
            )
        ).scalar_one()
        or 0
    )
    if signal_count == 0:
        return _skipped_payload(
            family="default_strategy",
            lane="frozen_default_control",
            blocker="no_historical_signals",
            details={"window_signal_count": signal_count},
        )
    if signal_count > MAX_INLINE_STRATEGY_CONTROL_SIGNALS:
        return _skipped_payload(
            family="default_strategy",
            lane="frozen_default_control",
            blocker="historical_signal_replay_too_large_for_inline_control",
            details={
                "window_signal_count": signal_count,
                "max_inline_strategy_control_signals": MAX_INLINE_STRATEGY_CONTROL_SIGNALS,
                "next_step": "run_universe_scoped_streaming_strategy_control",
            },
        )

    run = BacktestRun(
        id=uuid.uuid4(),
        name=f"Research Lab frozen default control {str(batch.id)[:8]}",
        start_date=batch.window_start,
        end_date=batch.window_end,
        detector_configs=with_replay_mode({}, STRATEGY_COMPARISON_REPLAY_MODE),
        rank_threshold=float(settings.legacy_benchmark_rank_threshold),
        status="pending",
    )
    session.add(run)
    await session.flush()
    await BacktestEngine().run(session, run)
    return normalize_backtest_run(
        run,
        family="default_strategy",
        lane="frozen_default_control",
        strategy_version="default_strategy_benchmark_v1",
    )


async def _run_detector_sweep(session: AsyncSession, batch: ResearchBatch) -> dict[str, Any]:
    snapshot_count = int(
        (
            await session.execute(
                select(func.count(PriceSnapshot.id)).where(
                    PriceSnapshot.captured_at >= batch.window_start,
                    PriceSnapshot.captured_at <= batch.window_end,
                )
            )
        ).scalar_one()
        or 0
    )
    if snapshot_count == 0:
        return _skipped_payload(
            family="default_strategy",
            lane="detector_sweep",
            blocker="no_price_snapshots",
            details={"window_price_snapshot_count": snapshot_count},
        )
    if snapshot_count > MAX_INLINE_DETECTOR_SNAPSHOTS:
        return _skipped_payload(
            family="default_strategy",
            lane="detector_sweep",
            blocker="snapshot_replay_too_large_for_inline_sweep",
            details={
                "window_price_snapshot_count": snapshot_count,
                "max_inline_detector_snapshots": MAX_INLINE_DETECTOR_SNAPSHOTS,
                "next_step": "run_universe_scoped_streaming_detector_sweep",
            },
        )

    runs = await parameter_sweep(
        session=session,
        name_prefix=f"Research Lab detector sweep {str(batch.id)[:8]}",
        start_date=batch.window_start,
        end_date=batch.window_end,
        base_detector_configs=with_replay_mode({}, DETECTOR_REPLAY_MODE) or {},
        base_rank_threshold=0.6,
        sweep_params={
            "price_move.threshold_pct": [0.04, 0.06],
            "rank_threshold": [0.55, 0.70],
        },
    )
    if not runs:
        return _skipped_payload(family="default_strategy", lane="detector_sweep", blocker="no_sweep_runs_created")
    best = sorted(
        runs,
        key=lambda run: (
            int((run.result_summary or {}).get("resolved_signals") or 0),
            float((run.result_summary or {}).get("win_rate") or 0.0),
        ),
        reverse=True,
    )[0]
    payload = normalize_backtest_run(
        best,
        family="default_strategy",
        lane="detector_sweep",
        strategy_version="detector_replay_sweep_v1",
    )
    payload["details_json"]["sweep_run_ids"] = [str(run.id) for run in runs]
    payload["details_json"]["sweep_count"] = len(runs)
    return payload


async def _run_structure_lane(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    batch: ResearchBatch,
) -> dict[str, Any]:
    _ = session_factory
    recent_opportunities = (
        await session.execute(
            select(MarketStructureOpportunity)
            .order_by(MarketStructureOpportunity.observed_at_local.desc(), MarketStructureOpportunity.id.desc())
            .limit(100)
        )
    ).scalars().all()
    opportunity_ids = (
        await session.execute(
            select(MarketStructureOpportunity.id)
            .where(MarketStructureOpportunity.actionable.is_(True))
            .order_by(MarketStructureOpportunity.observed_at_local.desc(), MarketStructureOpportunity.id.desc())
            .limit(50)
        )
    ).scalars().all()
    if not opportunity_ids:
        invalid_reason_counts: dict[str, int] = {}
        blocked_opportunities = []
        for row in recent_opportunities:
            if not row.actionable:
                reason = str(row.invalid_reason or "not_actionable")
                invalid_reason_counts[reason] = invalid_reason_counts.get(reason, 0) + 1
                blocked_opportunities.append(row)
        blocked_opportunities.sort(
            key=lambda row: (
                row.net_edge_bps is not None,
                float(row.net_edge_bps or 0),
                row.observed_at_local,
            ),
            reverse=True,
        )
        return _skipped_payload(
            family="structure",
            lane="structure_replay",
            blocker="no_actionable_structure_opportunities",
            details={
                "next_step": "run_bounded_structure_scan_worker",
                "max_markets": batch.max_markets,
                "opportunity_count": len(recent_opportunities),
                "blocked_count": len(blocked_opportunities),
                "invalid_reason_counts": invalid_reason_counts,
                "top_blocked_opportunity_ids": [str(row.id) for row in blocked_opportunities[:10]],
                "max_blocked_net_edge_bps": _json_safe(
                    max(
                        (row.net_edge_bps for row in blocked_opportunities if row.net_edge_bps is not None),
                        default=None,
                    )
                ),
            },
        )
    return _skipped_payload(
        family="structure",
        lane="structure_replay",
        blocker="structure_replay_deferred_to_bounded_worker",
        details={
            "actionable_opportunities": len(opportunity_ids),
            "top_opportunity_ids": [str(value) for value in opportunity_ids[:5]],
            "next_step": "run_structure_replay_as_bounded_background_job",
        },
    )


async def _run_maker_lane(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    batch: ResearchBatch,
) -> dict[str, Any]:
    _ = session_factory
    _ = batch
    quote_ids = (
        await session.execute(
            select(PolymarketQuoteRecommendation.id)
            .order_by(PolymarketQuoteRecommendation.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    if not quote_ids:
        return _skipped_payload(
            family="maker",
            lane="maker_replay",
            blocker="no_quote_recommendations",
            details={"next_step": "run_advisory_maker_economics_worker"},
        )
    return _skipped_payload(
        family="maker",
        lane="maker_replay",
        blocker="maker_replay_deferred_to_bounded_worker",
        details={
            "quote_recommendation_ids": [str(value) for value in quote_ids[:10]],
            "next_step": "run_maker_replay_as_bounded_background_job",
        },
    )


async def _run_exec_policy_lane(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    batch: ResearchBatch,
) -> dict[str, Any]:
    _ = session_factory
    _ = batch
    candidate_count = int(
        (
            await session.execute(select(func.count(PolymarketExecutionActionCandidate.id)))
        ).scalar_one()
        or 0
    )
    valid_candidate_count = int(
        (
            await session.execute(
                select(func.count(PolymarketExecutionActionCandidate.id)).where(
                    PolymarketExecutionActionCandidate.valid.is_(True)
                )
            )
        ).scalar_one()
        or 0
    )
    invalid_rows = (
        await session.execute(
            select(
                PolymarketExecutionActionCandidate.action_type,
                PolymarketExecutionActionCandidate.invalid_reason,
                func.count(PolymarketExecutionActionCandidate.id),
            )
            .where(PolymarketExecutionActionCandidate.valid.is_(False))
            .group_by(
                PolymarketExecutionActionCandidate.action_type,
                PolymarketExecutionActionCandidate.invalid_reason,
            )
            .order_by(func.count(PolymarketExecutionActionCandidate.id).desc())
            .limit(20)
        )
    ).all()
    invalid_reason_counts = {
        f"{action_type or 'unknown'}:{invalid_reason or 'invalid'}": int(count or 0)
        for action_type, invalid_reason, count in invalid_rows
    }
    top_candidates = (
        await session.execute(
            select(PolymarketExecutionActionCandidate)
            .where(PolymarketExecutionActionCandidate.valid.is_(True))
            .order_by(
                PolymarketExecutionActionCandidate.est_net_ev_total.desc().nullslast(),
                PolymarketExecutionActionCandidate.est_net_ev_bps.desc().nullslast(),
                PolymarketExecutionActionCandidate.decided_at.desc(),
                PolymarketExecutionActionCandidate.id.desc(),
            )
            .limit(20)
        )
    ).scalars().all()
    if not top_candidates:
        return _skipped_payload(
            family="exec_policy",
            lane="execution_policy_replay",
            blocker="no_execution_policy_candidates",
            details={
                "next_step": "generate_cross_post_step_ahead_candidates",
                "candidate_count": candidate_count,
                "valid_candidate_count": valid_candidate_count,
                "invalid_reason_counts": invalid_reason_counts,
            },
        )
    fill_probabilities = [
        float(row.est_fill_probability)
        for row in top_candidates
        if row.est_fill_probability is not None
    ]
    top_candidate_payloads = [
        {
            "id": row.id,
            "action_type": row.action_type,
            "side": row.side,
            "asset_id": row.asset_id,
            "condition_id": row.condition_id,
            "est_net_ev_bps": _json_safe(row.est_net_ev_bps),
            "est_net_ev_total": _json_safe(row.est_net_ev_total),
            "est_fill_probability": _json_safe(row.est_fill_probability),
            "est_avg_entry_price": _json_safe(row.est_avg_entry_price),
            "est_worst_price": _json_safe(row.est_worst_price),
            "target_size": _json_safe(row.target_size),
            "decided_at": _json_safe(row.decided_at),
        }
        for row in top_candidates[:10]
    ]
    replay_summary = await fetch_polymarket_replay_policy_summary(session)
    latest_replay_run = replay_summary.get("run")
    variant_summaries = replay_summary.get("variants") or {}
    exec_policy_metric = variant_summaries.get("exec_policy") or {}
    replay_net_pnl = exec_policy_metric.get("net_pnl")
    fill_rate = exec_policy_metric.get("fill_rate")
    replay_status = (latest_replay_run or {}).get("status")
    coverage_mode = "advisory_candidates_only"
    blockers = ["execution_policy_replay_deferred_to_bounded_worker"]
    verdict = "insufficient_evidence"
    if latest_replay_run is not None:
        blockers = []
        coverage_mode = "complete_replay" if replay_status == "completed" else "replay_coverage_limited"
        if replay_status == "completed_with_warnings":
            blockers.append("replay_coverage_limited")
        elif replay_status != "completed":
            blockers.append("execution_policy_replay_failed")
        if replay_net_pnl is None:
            blockers.append("missing_exec_policy_replay_metric")
        elif float(replay_net_pnl) <= 0:
            blockers.append("nonpositive_exec_policy_replay_pnl")
        verdict = "research_ready" if not blockers and float(replay_net_pnl or 0) > 0 else "needs_replay_improvement"

    return {
        "family": "exec_policy",
        "strategy_version": "execution_policy_advisory_v1",
        "lane": "execution_policy_replay",
        "source_kind": "execution_policy_candidate",
        "source_ref": str(top_candidates[0].id),
        "status": "completed",
        "verdict": verdict,
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": replay_net_pnl,
        "avg_clv": None,
        "resolved_trades": 0,
        "fill_rate": fill_rate if fill_rate is not None else (sum(fill_probabilities) / len(fill_probabilities) if fill_probabilities else None),
        "drawdown": None,
        "open_exposure": None,
        "coverage_mode": coverage_mode,
        "blockers": blockers,
        "details_json": {
            "candidate_count": candidate_count,
            "valid_candidate_count": valid_candidate_count,
            "candidate_ids": [str(row.id) for row in top_candidates[:10]],
            "top_candidates": top_candidate_payloads,
            "invalid_reason_counts": invalid_reason_counts,
            "latest_replay_run": latest_replay_run,
            "variant_summaries": variant_summaries,
            "next_step": "inspect_replay_metrics" if latest_replay_run is not None else "run_execution_policy_replay_as_bounded_background_job",
        },
    }


async def _run_profitability_lanes(session: AsyncSession) -> list[dict[str, Any]]:
    profitability = await get_profitability_snapshot(session, family="default_strategy", use_cache=False)
    profit_tools = await build_profit_tools_snapshot(session, family="default_strategy", use_cache=False)
    return [normalize_profitability_snapshot(profitability), normalize_profit_tools(profit_tools)]


async def _run_kalshi_low_yes_fade_lane(session: AsyncSession, batch: ResearchBatch) -> dict[str, Any]:
    snapshot = await build_kalshi_low_yes_fade_snapshot(
        session,
        window_days=int(batch.window_days or 30),
        max_signals=min(int(batch.max_markets or 500) * 10, 5000),
        as_of=batch.window_end,
    )
    return kalshi_low_yes_fade_lane_payload(snapshot)


async def _run_kalshi_down_yes_fade_lane(session: AsyncSession, batch: ResearchBatch) -> dict[str, Any]:
    snapshot = await build_kalshi_down_yes_fade_snapshot(
        session,
        window_days=int(batch.window_days or 30),
        max_signals=min(int(batch.max_markets or 500) * 10, 5000),
        as_of=batch.window_end,
    )
    return kalshi_down_yes_fade_lane_payload(snapshot)


async def _run_alpha_factory_lane(session: AsyncSession, batch: ResearchBatch) -> dict[str, Any]:
    snapshot = await build_alpha_factory_snapshot(
        session,
        window_days=max(int(batch.window_days or 30), 30),
        max_signals=min(max(int(batch.max_markets or 500) * 50, 5000), 50_000),
        platform="kalshi",
        max_candidates=10,
        as_of=batch.window_end,
    )
    return alpha_factory_lane_payload(snapshot)


def _top_blockers(lanes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in lanes:
        blockers = row.get("blockers") or []
        if not isinstance(blockers, list):
            blockers = [str(blockers)]
        for blocker in blockers:
            counts[str(blocker)] = counts.get(str(blocker), 0) + 1
    return [
        {"blocker": blocker, "count": count}
        for blocker, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]


BLOCKER_ACTIONS: dict[str, dict[str, str]] = {
    "empty_research_universe": {
        "label": "Connect the lab to a populated market universe",
        "why": "The batch selected zero markets, so every strategy lane is measuring an empty database instead of EV.",
    },
    "no_historical_signals": {
        "label": "Backfill or ingest historical signals for the selected window",
        "why": "The frozen default control needs historical decisions before it can prove or falsify paper EV.",
    },
    "historical_signal_replay_too_large_for_inline_control": {
        "label": "Move default-control replay into a universe-scoped worker",
        "why": "The selected lab batch is bounded, but the legacy control replay still sees production-wide signal history.",
    },
    "no_price_snapshots": {
        "label": "Run bounded price snapshot capture/backfill",
        "why": "Detector sweeps and replay comparisons need historical prices; without them the lab cannot rank signal variants.",
    },
    "snapshot_replay_too_large_for_inline_sweep": {
        "label": "Build a streaming detector sweep for the selected universe",
        "why": "Inline detector replay would load too many production snapshots; stream or shard it before trusting sweep EV.",
    },
    "no_actionable_structure_opportunities": {
        "label": "Populate fresh structure groups and executable orderbook context",
        "why": "Structure cannot produce profit candidates until negative-risk, complement, or parity opportunities become actionable.",
    },
    "no_actionable_opportunities_for_maker_quotes": {
        "label": "Generate maker economics after actionable structure exists",
        "why": "Maker replay depends on fee/reward-aware quote recommendations, which require actionable underlying opportunities.",
    },
    "no_quote_recommendations": {
        "label": "Sync fee/reward data and create advisory quote recommendations",
        "why": "The maker lane cannot estimate passive quote EV until recommendations exist.",
    },
    "no_execution_policy_candidates": {
        "label": "Generate cross/post/step-ahead execution candidates",
        "why": "Execution-policy replay needs candidate actions before it can compare fill quality and adverse selection.",
    },
    "no_active_candidate_run": {
        "label": "Start the Kalshi fade paper lane",
        "why": "The historical alpha candidate cannot become trustworthy until it has a separate forward paper run.",
    },
    "no_matching_kalshi_down_yes_fade_signals": {
        "label": "Wait for fresh Kalshi down-YES fade candidates",
        "why": "The v2 lane only learns when fresh mid-priced YES contracts move down with negative YES EV.",
    },
    "observation_window_below_30d": {
        "label": "Let the candidate lane age honestly",
        "why": "Forward paper alpha needs a 30-day observation window before promotion talk is meaningful.",
    },
    "insufficient_resolved_paper_trades": {
        "label": "Collect more resolved candidate trades",
        "why": "Execution-adjusted paper EV needs at least 20 resolved trades before the lane can pass.",
    },
    "nonpositive_execution_adjusted_pnl": {
        "label": "Keep the candidate paper-only until P&L turns positive",
        "why": "The historical edge is not enough if the forward execution-adjusted ledger is flat or negative.",
    },
    "nonpositive_avg_clv": {
        "label": "Watch CLV before trusting realized P&L",
        "why": "Positive CLV is the early warning that the lane is buying better than later market consensus.",
    },
    "lane_execution_failed": {
        "label": "Fix the failing lane before trusting the scoreboard",
        "why": "A failed lane may hide the best EV candidate; inspect the lane error and rerun the batch after the schema/code mismatch is fixed.",
    },
    "no_replay_scenarios": {
        "label": "Create replay scenarios for the selected lane",
        "why": "Replay-adjusted P&L is the strongest paper-only evidence gate for execution policy, structure, and maker behavior.",
    },
    "replay_coverage_limited": {
        "label": "Fill replay coverage gaps before promoting a lane",
        "why": "Partial book coverage can turn a tempting paper result into a false positive.",
    },
    "retired_polymarket_lane": {
        "label": "Keep research Kalshi-only",
        "why": "The retired Polymarket lane is no longer part of the profit plan for this deployment.",
    },
    "no_kalshi_resolved_signal_history": {
        "label": "Resolve more Kalshi signal history",
        "why": "The alpha factory needs resolved Kalshi signals with P&L and CLV before it can discover strategy candidates.",
    },
    "no_surviving_alpha_factory_candidates": {
        "label": "Broaden the Kalshi alpha search carefully",
        "why": "No train/validation-selected candidate survived the chronological holdout, so more data or new entry features are needed.",
    },
    "no_executable_alpha_factory_candidates": {
        "label": "Review ambiguous alpha candidates",
        "why": "Historical evidence exists, but the factory could not infer a safe paper trade expression yet.",
    },
}


def _readiness_actions(
    *,
    universe: dict[str, Any] | None,
    top_blockers: list[dict[str, Any]],
    lanes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered_blockers: list[str] = []
    counts = universe or {}
    if int(counts.get("market_count") or 0) == 0:
        ordered_blockers.append("empty_research_universe")
    ordered_blockers.extend(str(row.get("blocker")) for row in top_blockers if row.get("blocker"))

    actions: list[dict[str, Any]] = []
    for blocker in ordered_blockers:
        if blocker in seen:
            continue
        seen.add(blocker)
        template = BLOCKER_ACTIONS.get(blocker)
        if template is None:
            continue
        matching_lanes = [
            f"{row.get('family')}/{row.get('lane')}"
            for row in lanes
            if blocker in (row.get("blockers") or [])
        ]
        actions.append(
            {
                "family": "research_lab",
                "lane": "readiness",
                "label": template["label"],
                "why": template["why"],
                "source_kind": "research_readiness",
                "source_ref": blocker,
                "blocker": blocker,
                "affected_lanes": matching_lanes[:5],
            }
        )
    return actions[:5]


def _data_readiness(
    *,
    universe: dict[str, Any] | None,
    top_blockers: list[dict[str, Any]],
    lanes: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = universe or {}
    market_count = int(counts.get("market_count") or 0)
    signal_count = int(counts.get("signal_count") or 0)
    price_snapshot_count = int(counts.get("price_snapshot_count") or 0)
    orderbook_snapshot_count = int(counts.get("orderbook_snapshot_count") or 0)
    failed_lane_count = sum(1 for row in lanes if row.get("status") == "failed")
    blockers = [str(row.get("blocker")) for row in top_blockers if row.get("blocker")]

    if market_count == 0:
        status = "empty_universe"
        summary = "The lab ran against a database with zero selected markets."
    elif failed_lane_count:
        status = "lane_failures"
        summary = "The universe exists, but one or more research lanes failed before evidence could be ranked."
    elif signal_count == 0 and price_snapshot_count == 0 and orderbook_snapshot_count == 0:
        status = "missing_history"
        summary = "Markets were selected, but historical signal, price, and orderbook coverage is missing."
    elif signal_count == 0 or price_snapshot_count == 0:
        status = "partial_history"
        summary = "The lab has a market universe, but evidence coverage is still partial."
    else:
        status = "ready"
        summary = "The selected universe has enough stored evidence for ranked paper research."

    return {
        "status": status,
        "summary": summary,
        "counts": {
            "market_count": market_count,
            "outcome_count": int(counts.get("outcome_count") or 0),
            "signal_count": signal_count,
            "price_snapshot_count": price_snapshot_count,
            "orderbook_snapshot_count": orderbook_snapshot_count,
            "failed_lane_count": failed_lane_count,
        },
        "blockers": blockers,
        "actions": _readiness_actions(universe=counts, top_blockers=top_blockers, lanes=lanes),
    }


def _top_ev_candidates(lanes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in lanes:
        details = row.get("details_json") or {}
        for action in details.get("next_best_actions") or []:
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "step": action.get("step"),
                    "label": action.get("step") or "next_best_action",
                    "priority_score": action.get("priority_score"),
                    "why": action.get("why_ev") or action.get("operator_action") or "profit-tool action",
                    "operator_action": action.get("operator_action"),
                    "evidence": action.get("evidence"),
                    "source_kind": row.get("source_kind"),
                    "source_ref": row.get("source_ref"),
                }
            )
        for opportunity_id in details.get("top_opportunity_ids") or []:
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "label": f"structure_opportunity:{opportunity_id}",
                    "why": "actionable structure opportunity included in replay lane",
                    "source_kind": "structure_opportunity",
                    "source_ref": str(opportunity_id),
                }
            )
        for quote_id in details.get("quote_recommendation_ids") or []:
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "label": f"quote_recommendation:{quote_id}",
                    "why": "advisory quote recommendation included in maker replay lane",
                    "source_kind": "quote_recommendation",
                    "source_ref": str(quote_id),
                }
            )
        suppress_raw_exec_candidates = (
            row.get("family") == "exec_policy"
            and row.get("verdict") == "needs_replay_improvement"
            and bool(details.get("latest_replay_run"))
        )
        if suppress_raw_exec_candidates:
            latest_replay_run = details.get("latest_replay_run") or {}
            variant_summaries = details.get("variant_summaries") or {}
            exec_policy_summary = variant_summaries.get("exec_policy") or {}
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "step": "fix_execution_policy_replay",
                    "label": "fix_execution_policy_replay",
                    "priority_score": 70,
                    "why": (
                        "Latest replay did not confirm raw estimated EV; inspect coverage, "
                        "skip reasons, and execution filters before ranking candidates."
                    ),
                    "operator_action": "Fix replay/coverage or candidate validity before treating raw execution EV as actionable.",
                    "evidence": _json_safe({
                        "blockers": row.get("blockers") or [],
                        "latest_replay_run": {
                            "id": latest_replay_run.get("id"),
                            "status": latest_replay_run.get("status"),
                            "scenario_count": latest_replay_run.get("scenario_count"),
                            "time_window_start": latest_replay_run.get("time_window_start"),
                            "time_window_end": latest_replay_run.get("time_window_end"),
                        },
                        "exec_policy": {
                            "net_pnl": exec_policy_summary.get("net_pnl"),
                            "fill_rate": exec_policy_summary.get("fill_rate"),
                            "cancel_rate": exec_policy_summary.get("cancel_rate"),
                            "action_mix": exec_policy_summary.get("action_mix_json") or {},
                        },
                    }),
                    "source_kind": "execution_policy_replay",
                    "source_ref": row.get("source_ref"),
                }
            )
            continue
        for candidate in details.get("top_candidates") or []:
            candidate_id = candidate.get("id")
            if candidate_id is None:
                continue
            est_total = candidate.get("est_net_ev_total")
            est_bps = candidate.get("est_net_ev_bps")
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "label": f"exec_policy_candidate:{candidate_id}",
                    "priority_score": est_total if est_total is not None else est_bps,
                    "why": (
                        "Paper-only execution candidate with estimated net EV "
                        f"{est_total if est_total is not None else 'unknown'}; replay before promotion."
                    ),
                    "operator_action": "Run bounded execution-policy replay and compare cross/post/step-ahead outcomes.",
                    "evidence": candidate,
                    "source_kind": "execution_policy_candidate",
                    "source_ref": str(candidate_id),
                }
            )
        for candidate_id in details.get("candidate_ids") or []:
            if any(str(item.get("source_ref")) == str(candidate_id) for item in candidates):
                continue
            candidates.append(
                {
                    "family": row.get("family"),
                    "lane": row.get("lane"),
                    "label": f"exec_policy_candidate:{candidate_id}",
                    "why": "Paper-only execution candidate included in the replay lane.",
                    "source_kind": "execution_policy_candidate",
                    "source_ref": str(candidate_id),
                }
            )
    return candidates[:5]


async def run_research_batch(
    session_factory: async_sessionmaker[AsyncSession],
    batch_id: uuid.UUID | str,
) -> dict[str, Any]:
    batch_uuid = uuid.UUID(str(batch_id))
    async with session_factory() as session:
        batch = await session.get(ResearchBatch, batch_uuid)
        if batch is None:
            raise LookupError(f"Research batch not found: {batch_id}")
        if batch.status == "cancelled":
            return await get_research_batch_detail(session, batch_uuid)
        batch.status = "running"
        batch.started_at = batch.started_at or _utcnow()
        batch.error_count = 0
        await session.execute(delete(ResearchLaneResult).where(ResearchLaneResult.batch_id == batch.id))
        await session.commit()

        families = set(batch.families_json or DEFAULT_FAMILIES)
        lane_payloads: list[dict[str, Any]] = []
        lane_errors = 0

        async def run_lane(family: str, lane: str, func_) -> None:
            nonlocal lane_errors
            fresh_batch = await session.get(ResearchBatch, batch_uuid)
            if fresh_batch is not None and fresh_batch.status == "cancelled":
                lane_payloads.append(_skipped_payload(family=family, lane=lane, blocker="batch_cancelled"))
                return
            try:
                maybe_payload = func_()
                payload = await maybe_payload if inspect.isawaitable(maybe_payload) else maybe_payload
            except Exception as exc:
                lane_errors += 1
                payload = _failed_payload(family=family, lane=lane, exc=exc)
            lane_payloads.append(payload)

        if "default_strategy" in families:
            await run_lane("default_strategy", "profitability_gate", lambda: _run_profitability_lanes(session))
            flattened: list[dict[str, Any]] = []
            for item in lane_payloads:
                if isinstance(item, list):
                    flattened.extend(item)
                else:
                    flattened.append(item)
            lane_payloads = flattened
            await run_lane("default_strategy", "frozen_default_control", lambda: _run_default_control(session, batch))
            await run_lane("default_strategy", "detector_sweep", lambda: _run_detector_sweep(session, batch))
        if "kalshi_down_yes_fade" in families:
            await run_lane(
                "kalshi_down_yes_fade",
                "paper_forward_gate",
                lambda: _run_kalshi_down_yes_fade_lane(session, batch),
            )
        if "kalshi_low_yes_fade" in families:
            await run_lane(
                "kalshi_low_yes_fade",
                "paper_forward_gate",
                lambda: _run_kalshi_low_yes_fade_lane(session, batch),
            )
        if "alpha_factory" in families:
            await run_lane(
                "alpha_factory",
                "candidate_discovery",
                lambda: _run_alpha_factory_lane(session, batch),
            )
        if "structure" in families:
            await run_lane(
                "structure",
                "structure_replay",
                lambda: _retired_polymarket_payload(family="structure", lane="structure_replay"),
            )
        if "maker" in families:
            await run_lane(
                "maker",
                "maker_replay",
                lambda: _retired_polymarket_payload(family="maker", lane="maker_replay"),
            )
        if "exec_policy" in families:
            await run_lane(
                "exec_policy",
                "execution_policy_replay",
                lambda: _retired_polymarket_payload(family="exec_policy", lane="execution_policy_replay"),
            )

        lane_payloads = [payload for payload in lane_payloads if isinstance(payload, dict)]
        ranked_payloads = rank_lane_payloads(lane_payloads)
        result_rows = [_payload_to_lane_result(batch.id, payload) for payload in ranked_payloads]
        for row in result_rows:
            session.add(row)
        await session.flush()

        top_blockers = _top_blockers(ranked_payloads)
        top_ev_candidates = _top_ev_candidates(ranked_payloads)
        readiness = _data_readiness(
            universe=batch.universe_json if isinstance(batch.universe_json, dict) else {},
            top_blockers=top_blockers,
            lanes=ranked_payloads,
        )
        if not top_ev_candidates:
            top_ev_candidates = readiness["actions"]
        details = {
            "top_blockers": top_blockers,
            "top_ev_candidates": top_ev_candidates,
            "data_readiness": readiness,
            "paper_only": True,
            "live_orders_enabled": False,
            "pilot_arming_enabled": False,
        }
        batch.error_count = lane_errors
        batch.rows_inserted_json = {"lane_results": len(result_rows)}
        batch.details_json = details
        batch.status = "completed_with_warnings" if lane_errors else "completed"
        batch.completed_at = _utcnow()
        await session.commit()

        detail = await get_research_batch_detail(session, batch.id)
        artifact_paths = write_research_batch_artifacts(detail)
        batch = await session.get(ResearchBatch, batch.id)
        if batch is not None:
            details = dict(batch.details_json or {})
            details["artifacts"] = artifact_paths
            batch.details_json = details
            await session.commit()
        return await get_research_batch_detail(session, batch_uuid)


async def list_research_batches(
    session: AsyncSession,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(ResearchBatch).order_by(ResearchBatch.created_at.desc(), ResearchBatch.id.desc()).limit(limit)
        )
    ).scalars().all()
    return [_batch_summary(row) for row in rows]


def _research_batch_market_count(row: ResearchBatch) -> int:
    universe = row.universe_json if isinstance(row.universe_json, dict) else {}
    return int(universe.get("market_count") or 0)


async def get_latest_research_batch(session: AsyncSession, *, prefer_populated: bool = True) -> dict[str, Any] | None:
    rows = (
        await session.execute(
            select(ResearchBatch).order_by(ResearchBatch.created_at.desc(), ResearchBatch.id.desc()).limit(20)
        )
    ).scalars().all()
    if not rows:
        return None
    selected = rows[0]
    if prefer_populated:
        selected = next((row for row in rows if _research_batch_market_count(row) > 0), selected)
    detail = await get_research_batch_detail(session, selected.id)
    newest = rows[0]
    if selected.id != newest.id:
        batch = detail["batch"]
        details = dict(batch.get("details") or {})
        details["latest_selection_note"] = {
            "selected": "latest_populated_batch",
            "newest_batch_id": str(newest.id),
            "newest_batch_status": newest.status,
            "newest_batch_market_count": _research_batch_market_count(newest),
        }
        batch["details"] = details
    return detail


async def get_research_batch_detail(session: AsyncSession, batch_id: uuid.UUID | str) -> dict[str, Any]:
    batch_uuid = uuid.UUID(str(batch_id))
    batch = await session.get(ResearchBatch, batch_uuid)
    if batch is None:
        raise LookupError(f"Research batch not found: {batch_id}")
    lane_rows = (
        await session.execute(
            select(ResearchLaneResult)
            .where(ResearchLaneResult.batch_id == batch_uuid)
            .order_by(ResearchLaneResult.rank_position.asc(), ResearchLaneResult.id.asc())
        )
    ).scalars().all()
    details = batch.details_json if isinstance(batch.details_json, dict) else {}
    top_blockers = details.get("top_blockers") or []
    lane_summaries = [_lane_summary(row) for row in lane_rows]
    data_readiness = details.get("data_readiness") or _data_readiness(
        universe=batch.universe_json if isinstance(batch.universe_json, dict) else {},
        top_blockers=top_blockers,
        lanes=lane_summaries,
    )
    top_ev_candidates = details.get("top_ev_candidates") or []
    if not top_ev_candidates:
        top_ev_candidates = data_readiness.get("actions") or []
    return {
        "batch": _batch_summary(batch),
        "lane_results": lane_summaries,
        "top_blockers": top_blockers,
        "top_ev_candidates": top_ev_candidates,
        "data_readiness": data_readiness,
    }


async def cancel_research_batch(session: AsyncSession, batch_id: uuid.UUID | str) -> dict[str, Any]:
    batch = await session.get(ResearchBatch, uuid.UUID(str(batch_id)))
    if batch is None:
        raise LookupError(f"Research batch not found: {batch_id}")
    if batch.status not in {"completed", "completed_with_warnings", "failed"}:
        batch.status = "cancelled"
        batch.completed_at = batch.completed_at or _utcnow()
        await session.commit()
        await session.refresh(batch)
    return await get_research_batch_detail(session, batch.id)
