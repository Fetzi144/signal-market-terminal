from __future__ import annotations

import uuid
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.backtest import BacktestRun
from app.models.polymarket_replay import PolymarketReplayMetric, PolymarketReplayRun

PRIMARY_REPLAY_VARIANT_BY_FAMILY = {
    "exec_policy": "exec_policy",
    "maker": "maker_policy",
    "structure": "structure_policy",
}


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    converted = _decimal(value)
    return float(converted) if converted is not None else None


def _blockers(*items: Any) -> list[str]:
    blockers: list[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, list):
            blockers.extend(str(value) for value in item if value)
        elif isinstance(item, dict):
            blockers.extend(str(key) for key, value in item.items() if value)
        elif item:
            blockers.append(str(item))
    return sorted(set(blockers))


def _verdict_from_evidence(
    *,
    replay_net_pnl: Any = None,
    realized_pnl: Any = None,
    avg_clv: Any = None,
    resolved_trades: int = 0,
    blockers: list[str] | None = None,
) -> str:
    active_blockers = blockers or []
    replay_pnl = _decimal(replay_net_pnl) or Decimal("0")
    realized = _decimal(realized_pnl) or Decimal("0")
    clv = _decimal(avg_clv) or Decimal("0")
    if active_blockers:
        return "insufficient_evidence"
    if replay_pnl > 0 or (realized > 0 and clv > 0):
        return "healthy"
    if resolved_trades > 0 or replay_pnl != 0 or clv != 0:
        return "watch"
    return "insufficient_evidence"


def normalize_profitability_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    blockers = _blockers(snapshot.get("profitability_blockers"), snapshot.get("evidence_blockers"))
    return {
        "family": snapshot.get("family") or "default_strategy",
        "strategy_version": snapshot.get("strategy_version"),
        "lane": "profitability_gate",
        "source_kind": "profitability_snapshot",
        "source_ref": snapshot.get("window_end"),
        "status": "completed",
        "verdict": snapshot.get("verdict") or "insufficient_evidence",
        "realized_pnl": _decimal(snapshot.get("realized_pnl")),
        "mark_to_market_pnl": _decimal(snapshot.get("mark_to_market_pnl")),
        "replay_net_pnl": _decimal(snapshot.get("replay_net_pnl")),
        "avg_clv": _decimal(snapshot.get("avg_clv")),
        "resolved_trades": int(snapshot.get("resolved_trades") or 0),
        "fill_rate": None,
        "drawdown": None,
        "open_exposure": _decimal(snapshot.get("open_exposure")),
        "coverage_mode": snapshot.get("replay_coverage_mode"),
        "blockers": blockers,
        "details_json": {
            "snapshot": snapshot,
            "execution_adjusted_paper_pnl": snapshot.get("execution_adjusted_paper_pnl"),
        },
    }


def normalize_profit_tools(snapshot: dict[str, Any]) -> dict[str, Any]:
    actions = snapshot.get("next_best_steps") or []
    lane_readiness = snapshot.get("lane_readiness") or {}
    blocked_lanes = lane_readiness.get("blocked_lanes") or []
    blockers = _blockers(blocked_lanes)
    return {
        "family": snapshot.get("family") or "default_strategy",
        "strategy_version": None,
        "lane": "profit_tools",
        "source_kind": "profit_tools",
        "source_ref": snapshot.get("generated_at"),
        "status": "completed",
        "verdict": "research_ready" if not blockers else "insufficient_evidence",
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": None,
        "avg_clv": None,
        "resolved_trades": 0,
        "fill_rate": None,
        "drawdown": None,
        "open_exposure": None,
        "coverage_mode": "profit_tool_readiness",
        "blockers": blockers,
        "details_json": {
            "next_best_actions": actions[:5],
            "lane_readiness": lane_readiness,
            "profit_finder": snapshot.get("profit_finder_workbench") or {},
        },
    }


def normalize_backtest_run(
    run: BacktestRun,
    *,
    family: str,
    lane: str,
    strategy_version: str | None = None,
) -> dict[str, Any]:
    summary = run.result_summary or {}
    source_ref = str(run.id)
    if isinstance(summary.get("comparison"), dict):
        default_summary = summary["comparison"].get("default_strategy") or {}
        blockers = []
        if int(default_summary.get("trades_missing_orderbook_context") or 0) > 0:
            blockers.append("missing_orderbook_context")
        if int(default_summary.get("resolved_trades") or 0) == 0:
            blockers.append("no_resolved_backtest_trades")
        return {
            "family": family,
            "strategy_version": strategy_version,
            "lane": lane,
            "source_kind": "backtest",
            "source_ref": source_ref,
            "status": run.status,
            "verdict": _verdict_from_evidence(
                realized_pnl=default_summary.get("cumulative_pnl"),
                replay_net_pnl=default_summary.get("shadow_cumulative_pnl"),
                avg_clv=default_summary.get("avg_clv"),
                resolved_trades=int(default_summary.get("resolved_trades") or 0),
                blockers=blockers,
            ),
            "realized_pnl": _decimal(default_summary.get("cumulative_pnl")),
            "mark_to_market_pnl": None,
            "replay_net_pnl": _decimal(default_summary.get("shadow_cumulative_pnl")),
            "avg_clv": _decimal(default_summary.get("avg_clv")),
            "resolved_trades": int(default_summary.get("resolved_trades") or 0),
            "fill_rate": None,
            "drawdown": _decimal(default_summary.get("max_drawdown")),
            "open_exposure": None,
            "coverage_mode": "historical_signal_replay",
            "blockers": blockers,
            "details_json": {"summary": summary},
        }

    blockers = []
    resolved = int(summary.get("resolved_signals") or 0)
    if resolved == 0:
        blockers.append("no_resolved_detector_signals")
    return {
        "family": family,
        "strategy_version": strategy_version,
        "lane": lane,
        "source_kind": "backtest",
        "source_ref": source_ref,
        "status": run.status,
        "verdict": "watch" if resolved and float(summary.get("win_rate") or 0) > 0.5 else "insufficient_evidence",
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": None,
        "avg_clv": None,
        "resolved_trades": resolved,
        "fill_rate": None,
        "drawdown": None,
        "open_exposure": None,
        "coverage_mode": "snapshot_detector_replay",
        "blockers": blockers,
        "details_json": {"summary": summary},
    }


async def normalize_replay_run(
    session: AsyncSession,
    *,
    run_id: uuid.UUID | str,
    family: str,
    lane: str,
) -> dict[str, Any]:
    replay_uuid = uuid.UUID(str(run_id))
    run = await session.get(PolymarketReplayRun, replay_uuid)
    if run is None:
        raise LookupError(f"Replay run not found: {run_id}")

    metrics = (
        await session.execute(
            select(PolymarketReplayMetric).where(
                PolymarketReplayMetric.run_id == replay_uuid,
                PolymarketReplayMetric.metric_scope == "run",
            )
        )
    ).scalars().all()
    primary_variant = PRIMARY_REPLAY_VARIANT_BY_FAMILY.get(family)
    metric = next((row for row in metrics if row.variant_name == primary_variant), None)
    if metric is None and metrics:
        metric = sorted(metrics, key=lambda row: _float(row.net_pnl) or 0.0, reverse=True)[0]

    details = run.details_json if isinstance(run.details_json, dict) else {}
    coverage_limited = bool(details.get("coverage_limited")) or run.status == "completed_with_warnings"
    blockers = []
    if run.scenario_count <= 0:
        blockers.append("no_replay_scenarios")
    if coverage_limited:
        blockers.append("replay_coverage_limited")
    if metric is None:
        blockers.append("missing_run_metric")

    replay_net_pnl = metric.net_pnl if metric is not None else None
    config = run.config_json if isinstance(run.config_json, dict) else {}
    return {
        "family": family,
        "strategy_version": details.get("strategy_version_key") or config.get("strategy_version_key"),
        "lane": lane,
        "source_kind": "polymarket_replay",
        "source_ref": str(run.id),
        "status": run.status,
        "verdict": _verdict_from_evidence(
            replay_net_pnl=replay_net_pnl,
            resolved_trades=run.scenario_count,
            blockers=blockers,
        ),
        "realized_pnl": None,
        "mark_to_market_pnl": None,
        "replay_net_pnl": replay_net_pnl,
        "avg_clv": None,
        "resolved_trades": int(run.scenario_count or 0),
        "fill_rate": metric.fill_rate if metric is not None else None,
        "drawdown": metric.drawdown_proxy if metric is not None else None,
        "open_exposure": None,
        "coverage_mode": "coverage_limited" if coverage_limited else "complete_replay" if run.scenario_count else "no_replay",
        "blockers": blockers,
        "details_json": {
            "run": {
                "id": str(run.id),
                "run_type": run.run_type,
                "status": run.status,
                "scenario_count": run.scenario_count,
                "rows_inserted_json": run.rows_inserted_json,
                "details_json": run.details_json,
            },
            "primary_variant": metric.variant_name if metric is not None else primary_variant,
            "metric": {
                "net_pnl": _float(metric.net_pnl) if metric is not None else None,
                "gross_pnl": _float(metric.gross_pnl) if metric is not None else None,
                "fees_paid": _float(metric.fees_paid) if metric is not None else None,
                "rewards_estimated": _float(metric.rewards_estimated) if metric is not None else None,
                "fill_rate": _float(metric.fill_rate) if metric is not None else None,
                "drawdown_proxy": _float(metric.drawdown_proxy) if metric is not None else None,
                "details_json": metric.details_json if metric is not None else None,
            },
        },
    }
