"""Read-only profit-finding operator tools for paper trading."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from app.paper_trading.analysis import get_profitability_snapshot
from app.reports.strategy_review import _fmt_cents, _fmt_money, _repo_root
from app.strategy_runs.service import get_active_strategy_run, serialize_strategy_run

ZERO = Decimal("0")
PROFIT_TOOLS_SCHEMA_VERSION = "profit_tools_v1"
PROFIT_TOOLS_ACTION_LIMIT = 5
PROFIT_TOOLS_ROW_LIMIT = 20
PROFIT_TOOLS_OPEN_TRADE_SCAN_LIMIT = 100
PROFIT_TOOLS_ARTIFACT_DIR = "docs/profit-tools"
RETIRED_POLYMARKET_REASON = (
    "Polymarket research and execution lanes are retired in this deployment; "
    "profit work is Kalshi-only."
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    normalized = _ensure_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _float(value: Any, *, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _money(value: Any) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


def _int(value: Any) -> int:
    return int(value or 0)


def _days_between(start: datetime | None, end: datetime) -> float | None:
    normalized = _ensure_utc(start)
    if normalized is None:
        return None
    return round((end - normalized).total_seconds() / 86400, 2)


def _safe_text(value: Any, *, limit: int = 180) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _active_run_filter(strategy_run: StrategyRun | None) -> list[Any]:
    if strategy_run is None:
        return []
    return [ExecutionDecision.strategy_run_id == strategy_run.id]


async def _count_rows(session: AsyncSession, model, *filters) -> int:
    query = select(func.count()).select_from(model)
    if filters:
        query = query.where(*filters)
    return _int((await session.execute(query)).scalar_one())


async def _latest_value(session: AsyncSession, column, model, *filters) -> datetime | None:
    query = select(func.max(column)).select_from(model)
    if filters:
        query = query.where(*filters)
    return _ensure_utc((await session.execute(query)).scalar_one_or_none())


async def _decision_reason_rows(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    limit: int,
) -> list[dict[str, Any]]:
    if strategy_run is None:
        return []

    count_expr = func.count(ExecutionDecision.id)
    sum_expected_expr = func.sum(ExecutionDecision.net_expected_pnl_usd)
    rows = (
        await session.execute(
            select(
                ExecutionDecision.reason_code,
                ExecutionDecision.decision_status,
                count_expr,
                func.avg(ExecutionDecision.net_ev_per_share),
                sum_expected_expr,
            )
            .where(*_active_run_filter(strategy_run))
            .group_by(ExecutionDecision.reason_code, ExecutionDecision.decision_status)
            .order_by(count_expr.desc())
            .limit(limit)
        )
    ).all()
    return [
        {
            "reason_code": reason_code or "unknown",
            "decision_status": decision_status or "unknown",
            "decision_count": _int(decision_count),
            "avg_net_ev_per_share": _float(avg_net_ev),
            "sum_net_expected_pnl_usd": _money(sum_expected_pnl),
        }
        for reason_code, decision_status, decision_count, avg_net_ev, sum_expected_pnl in rows
    ]


async def _boring_filter_rows(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    limit: int,
) -> list[dict[str, Any]]:
    if strategy_run is None:
        return []

    count_expr = func.count(ExecutionDecision.id)
    rows = (
        await session.execute(
            select(ExecutionDecision.reason_code, count_expr, func.max(ExecutionDecision.decision_at))
            .where(
                *_active_run_filter(strategy_run),
                ExecutionDecision.reason_code.like("profitability_%"),
            )
            .group_by(ExecutionDecision.reason_code)
            .order_by(count_expr.desc())
            .limit(limit)
        )
    ).all()
    return [
        {
            "reason_code": reason_code or "unknown",
            "decision_count": _int(decision_count),
            "latest_decision_at": _iso(latest_decision_at),
        }
        for reason_code, decision_count, latest_decision_at in rows
    ]


async def _missed_positive_cohorts(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    limit: int,
) -> list[dict[str, Any]]:
    if strategy_run is None:
        return []

    count_expr = func.count(ExecutionDecision.id)
    signal_profit_expr = func.sum(func.coalesce(Signal.profit_loss, ZERO))
    expected_pnl_expr = func.sum(func.coalesce(ExecutionDecision.net_expected_pnl_usd, ZERO))
    rows = (
        await session.execute(
            select(
                ExecutionDecision.reason_code,
                Signal.signal_type,
                Market.platform,
                count_expr,
                signal_profit_expr,
                func.avg(Signal.clv),
                func.avg(Signal.expected_value),
                func.avg(ExecutionDecision.net_ev_per_share),
                expected_pnl_expr,
            )
            .join(Signal, Signal.id == ExecutionDecision.signal_id)
            .outerjoin(Market, Market.id == Signal.market_id)
            .where(
                *_active_run_filter(strategy_run),
                ExecutionDecision.decision_status == "skipped",
                Signal.resolved_correctly.is_not(None),
            )
            .group_by(ExecutionDecision.reason_code, Signal.signal_type, Market.platform)
            .order_by(expected_pnl_expr.desc(), signal_profit_expr.desc(), count_expr.desc())
            .limit(limit)
        )
    ).all()
    cohorts: list[dict[str, Any]] = []
    for (
        reason_code,
        signal_type,
        platform,
        decision_count,
        signal_profit_total,
        avg_clv,
        avg_expected_value,
        avg_net_ev_per_share,
        sum_net_expected_pnl,
    ) in rows:
        signal_profit_total = signal_profit_total or ZERO
        avg_clv = avg_clv or ZERO
        sum_net_expected_pnl = sum_net_expected_pnl or ZERO
        cohorts.append(
            {
                "reason_code": reason_code or "unknown",
                "signal_type": signal_type or "unknown",
                "platform": platform or "unknown",
                "decision_count": _int(decision_count),
                "signal_profit_loss_total": _float(signal_profit_total),
                "avg_clv": _float(avg_clv),
                "avg_expected_value": _float(avg_expected_value),
                "avg_decision_net_ev_per_share": _float(avg_net_ev_per_share),
                "sum_net_expected_pnl_usd": _money(sum_net_expected_pnl),
                "positive_evidence": (
                    signal_profit_total > ZERO
                    or avg_clv > ZERO
                    or sum_net_expected_pnl > ZERO
                ),
            }
        )
    return cohorts


async def _opened_trade_cohorts(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    limit: int,
) -> list[dict[str, Any]]:
    if strategy_run is None:
        return []

    count_expr = func.count(PaperTrade.id)
    rows = (
        await session.execute(
            select(
                Signal.signal_type,
                Market.platform,
                PaperTrade.status,
                count_expr,
                func.sum(PaperTrade.size_usd),
                func.sum(PaperTrade.pnl),
                func.avg(Signal.clv),
            )
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .outerjoin(Market, Market.id == PaperTrade.market_id)
            .where(PaperTrade.strategy_run_id == strategy_run.id)
            .group_by(Signal.signal_type, Market.platform, PaperTrade.status)
            .order_by(count_expr.desc())
            .limit(limit)
        )
    ).all()
    return [
        {
            "signal_type": signal_type or "unknown",
            "platform": platform or "unknown",
            "status": status or "unknown",
            "trade_count": _int(trade_count),
            "notional_usd": _money(notional),
            "realized_pnl": _money(realized_pnl),
            "avg_clv": _float(avg_clv),
        }
        for signal_type, platform, status, trade_count, notional, realized_pnl, avg_clv in rows
    ]


async def _build_profit_finder_workbench(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    limit: int,
) -> dict[str, Any]:
    top_decision_reasons = await _decision_reason_rows(session, strategy_run=strategy_run, limit=limit)
    missed_cohorts = await _missed_positive_cohorts(session, strategy_run=strategy_run, limit=limit)
    opened_cohorts = await _opened_trade_cohorts(session, strategy_run=strategy_run, limit=limit)
    boring_filter = await _boring_filter_rows(session, strategy_run=strategy_run, limit=limit)
    return {
        "status": "ready" if strategy_run is not None else "no_active_run",
        "strategy_run_id": str(strategy_run.id) if strategy_run is not None else None,
        "top_decision_reasons": top_decision_reasons,
        "missed_positive_cohorts": missed_cohorts,
        "opened_trade_cohorts": opened_cohorts,
        "boring_profit_filter": {
            "reason_counts": boring_filter,
            "enabled": bool(settings.paper_trading_profitability_filter_enabled),
            "max_resolution_horizon_days": settings.paper_trading_max_resolution_horizon_days,
            "min_market_liquidity_usd": settings.paper_trading_min_market_liquidity_usd,
        },
    }


async def _open_trade_bucket(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    filters: list[Any],
) -> dict[str, Any]:
    if strategy_run is None:
        return {"trade_count": 0, "open_exposure": 0.0}

    row = (
        await session.execute(
            select(func.count(PaperTrade.id), func.sum(PaperTrade.size_usd))
            .select_from(PaperTrade)
            .outerjoin(Market, Market.id == PaperTrade.market_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run.id,
                PaperTrade.status == "open",
                *filters,
            )
        )
    ).one()
    return {"trade_count": _int(row[0]), "open_exposure": _money(row[1])}


def _classify_open_trade(market: Market | None, *, now: datetime) -> tuple[str, int]:
    if market is None:
        return "missing_market", 10
    end_date = _ensure_utc(market.end_date)
    if end_date is None:
        return "missing_end_date", 20
    if end_date < now:
        return "overdue", 0
    days_to_end = (end_date - now).total_seconds() / 86400
    if days_to_end <= 7:
        return "short_horizon", 30
    if days_to_end <= 30:
        return "operating_window", 40
    return "long_dated_capital_drag", 15


def _resolution_action_for_bucket(bucket: str) -> str:
    return {
        "overdue": "settle_or_backfill_resolution",
        "missing_market": "repair_market_linkage",
        "missing_end_date": "backfill_market_end_date",
        "long_dated_capital_drag": "quarantine_from_profit_gate_and_review_exit",
        "short_horizon": "monitor_for_resolution",
        "operating_window": "keep_in_30_day_profit_window",
    }.get(bucket, "inspect_open_trade")


async def _build_resolution_accelerator(
    session: AsyncSession,
    *,
    strategy_run: StrategyRun | None,
    now: datetime,
    limit: int,
) -> dict[str, Any]:
    window_7d = now + timedelta(days=7)
    window_30d = now + timedelta(days=30)
    buckets = {
        "missing_market": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.id.is_(None)],
        ),
        "missing_end_date": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.id.is_not(None), Market.end_date.is_(None)],
        ),
        "overdue": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.end_date < now],
        ),
        "short_horizon": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.end_date >= now, Market.end_date <= window_7d],
        ),
        "operating_window": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.end_date > window_7d, Market.end_date <= window_30d],
        ),
        "long_dated_capital_drag": await _open_trade_bucket(
            session,
            strategy_run=strategy_run,
            filters=[Market.end_date > window_30d],
        ),
    }

    if strategy_run is None:
        open_trade_count = 0
        open_exposure = 0.0
        action_items: list[dict[str, Any]] = []
    else:
        open_trade_row = (
            await session.execute(
                select(func.count(PaperTrade.id), func.sum(PaperTrade.size_usd)).where(
                    PaperTrade.strategy_run_id == strategy_run.id,
                    PaperTrade.status == "open",
                )
            )
        ).one()
        open_trade_count = _int(open_trade_row[0])
        open_exposure = _money(open_trade_row[1])
        trade_rows = (
            await session.execute(
                select(PaperTrade, Market, Signal)
                .outerjoin(Market, Market.id == PaperTrade.market_id)
                .outerjoin(Signal, Signal.id == PaperTrade.signal_id)
                .where(
                    PaperTrade.strategy_run_id == strategy_run.id,
                    PaperTrade.status == "open",
                )
                .order_by(PaperTrade.size_usd.desc())
                .limit(PROFIT_TOOLS_OPEN_TRADE_SCAN_LIMIT)
            )
        ).all()
        sorted_rows = sorted(
            trade_rows,
            key=lambda row: (
                _classify_open_trade(row[1], now=now)[1],
                -float(row[0].size_usd or ZERO),
                _ensure_utc(row[0].opened_at) or now,
            ),
        )
        action_items = []
        for trade, market, signal in sorted_rows[:limit]:
            bucket, _priority = _classify_open_trade(market, now=now)
            end_date = _ensure_utc(market.end_date) if market is not None else None
            action_items.append(
                {
                    "trade_id": str(trade.id),
                    "signal_id": str(trade.signal_id),
                    "market_id": str(trade.market_id),
                    "market_platform_id": market.platform_id if market is not None else None,
                    "market_question": _safe_text(market.question if market is not None else (trade.details or {}).get("market_question")),
                    "signal_type": signal.signal_type if signal is not None else None,
                    "direction": trade.direction,
                    "size_usd": _money(trade.size_usd),
                    "opened_at": _iso(trade.opened_at),
                    "open_age_days": _days_between(trade.opened_at, now),
                    "market_end_date": _iso(end_date),
                    "days_to_end": _days_between(now, end_date) if end_date is not None else None,
                    "bucket": bucket,
                    "recommended_action": _resolution_action_for_bucket(bucket),
                }
            )

    return {
        "status": "ready" if strategy_run is not None else "no_active_run",
        "open_trade_count": open_trade_count,
        "open_exposure": open_exposure,
        "buckets": buckets,
        "action_items": action_items,
    }


async def _build_structure_lane(session: AsyncSession) -> dict[str, Any]:
    return {
        "lane": "structure",
        "status": "retired",
        "paper_only": True,
        "live_submission_permitted": False,
        "total_opportunities": 0,
        "actionable_opportunities": 0,
        "executable_opportunities": 0,
        "paper_plans": 0,
        "active_paper_plans": 0,
        "latest_opportunity_at": None,
        "next_step": "retired",
        "disabled_reason": RETIRED_POLYMARKET_REASON,
    }


async def _build_maker_lane(session: AsyncSession) -> dict[str, Any]:
    return {
        "lane": "maker",
        "status": "retired",
        "paper_only": True,
        "live_submission_permitted": False,
        "economics_snapshots": 0,
        "ok_economics_snapshots": 0,
        "quote_recommendations": 0,
        "advisory_quote_recommendations": 0,
        "latest_snapshot_at": None,
        "latest_recommendation_at": None,
        "next_step": "retired",
        "disabled_reason": RETIRED_POLYMARKET_REASON,
    }


async def _build_exec_policy_lane(session: AsyncSession, *, strategy_run: StrategyRun | None) -> dict[str, Any]:
    return {
        "lane": "exec_policy",
        "status": "retired",
        "paper_only": True,
        "live_submission_permitted": False,
        "action_candidates": 0,
        "valid_action_candidates": 0,
        "default_run_decisions_with_policy": 0,
        "latest_candidate_at": None,
        "action_type_counts": [],
        "next_step": "retired",
        "disabled_reason": RETIRED_POLYMARKET_REASON,
    }


async def _build_replay_lane(session: AsyncSession) -> dict[str, Any]:
    return {
        "lane": "replay",
        "status": "retired",
        "paper_only": True,
        "live_submission_permitted": False,
        "runs": 0,
        "completed_runs": 0,
        "scenarios": 0,
        "completed_scenarios": 0,
        "latest_run_at": None,
        "latest_completed_at": None,
        "next_step": "retired",
        "disabled_reason": RETIRED_POLYMARKET_REASON,
    }


async def _build_lane_readiness(session: AsyncSession, *, strategy_run: StrategyRun | None) -> dict[str, Any]:
    structure = await _build_structure_lane(session)
    maker = await _build_maker_lane(session)
    exec_policy = await _build_exec_policy_lane(session, strategy_run=strategy_run)
    replay = await _build_replay_lane(session)
    lanes = {
        "structure": structure,
        "maker": maker,
        "exec_policy": exec_policy,
        "replay": replay,
    }
    blockers = [
        lane_name
        for lane_name, lane in lanes.items()
        if lane.get("status") in {"not_populated", "invalid_candidate_only", "running_or_failed"}
    ]
    retired_lanes = [
        lane_name
        for lane_name, lane in lanes.items()
        if lane.get("status") == "retired"
    ]
    return {
        "paper_only": True,
        "live_submission_permitted": False,
        "lanes": lanes,
        "blocked_lanes": blockers,
        "retired_lanes": retired_lanes,
        "scope": "kalshi_only",
        "status": "research_blocked" if blockers else "research_ready",
    }


def _decision_reason_count(profit_finder: dict[str, Any], reason_code: str) -> int:
    return sum(
        row.get("decision_count", 0)
        for row in profit_finder.get("top_decision_reasons", [])
        if row.get("reason_code") == reason_code
    )


def _build_next_best_steps(
    *,
    profitability: dict[str, Any],
    profit_finder: dict[str, Any],
    resolution: dict[str, Any],
    lanes: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if resolution.get("open_trade_count", 0) > 0:
        high_pressure_buckets = [
            name
            for name in ["overdue", "missing_market", "missing_end_date", "long_dated_capital_drag"]
            if (resolution.get("buckets", {}).get(name) or {}).get("trade_count", 0) > 0
        ]
        actions.append(
            {
                "step": "resolution_accelerator",
                "priority_score": 100,
                "why_ev": "Realized profitability is the gate; unresolved and long-dated open exposure delays truth and ties up paper bankroll.",
                "evidence": {
                    "open_trade_count": resolution.get("open_trade_count", 0),
                    "open_exposure": resolution.get("open_exposure", 0.0),
                    "pressure_buckets": high_pressure_buckets,
                },
                "operator_action": "Settle/backfill overdue trades, backfill missing market dates, and quarantine long-dated capital drag from the 30-day profit gate.",
            }
        )

    orderbook_blocks = _decision_reason_count(profit_finder, "execution_orderbook_context_unavailable")
    if orderbook_blocks > 0:
        actions.append(
            {
                "step": "repair_orderbook_context",
                "priority_score": 95,
                "why_ev": "A skipped executable edge is worse than a bad trade because it prevents learning and may hide the best opportunities.",
                "evidence": {"blocked_decisions": orderbook_blocks},
                "operator_action": "Prioritize orderbook freshness, asset watch coverage, and replayable book snapshots for skipped EV-positive decisions.",
            }
        )

    positive_cohorts = [
        cohort for cohort in profit_finder.get("missed_positive_cohorts", []) if cohort.get("positive_evidence")
    ]
    if positive_cohorts:
        actions.append(
            {
                "step": "promote_missed_positive_cohort",
                "priority_score": 90,
                "why_ev": "Resolved skipped cohorts with positive CLV or positive hypothetical P&L are the fastest paper-only source of candidate edge.",
                "evidence": positive_cohorts[:3],
                "operator_action": "Create a narrow paper lane for the highest-ranked skipped cohort and compare it against the frozen default control.",
            }
        )

    blockers = profitability.get("profitability_blockers") or []
    if blockers and not actions:
        actions.append(
            {
                "step": "clear_profitability_gate_blockers",
                "priority_score": 70,
                "why_ev": "The paper-profitability verdict cannot turn positive until blockers are removed or explicitly measured as insufficient sample.",
                "evidence": {"profitability_blockers": blockers},
                "operator_action": "Work the listed blockers in order, starting with evidence outages and stale pending decisions.",
            }
        )

    actions.sort(key=lambda row: row.get("priority_score", 0), reverse=True)
    return actions[:PROFIT_TOOLS_ACTION_LIMIT]


async def build_profit_tools_snapshot(
    session: AsyncSession,
    *,
    family: str = "default_strategy",
    use_cache: bool = True,
    limit: int = PROFIT_TOOLS_ROW_LIMIT,
) -> dict[str, Any]:
    """Build a bounded, read-only view of the highest-EV paper-profit work."""
    now = _utcnow()
    normalized_family = str(family or "default_strategy").strip().lower()
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    profitability = await get_profitability_snapshot(session, family=normalized_family, use_cache=use_cache)
    profit_finder = await _build_profit_finder_workbench(session, strategy_run=strategy_run, limit=limit)
    resolution = await _build_resolution_accelerator(session, strategy_run=strategy_run, now=now, limit=limit)
    lanes = await _build_lane_readiness(session, strategy_run=strategy_run)
    next_steps = _build_next_best_steps(
        profitability=profitability,
        profit_finder=profit_finder,
        resolution=resolution,
        lanes=lanes,
    )
    return {
        "schema_version": PROFIT_TOOLS_SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "family": normalized_family,
        "paper_only": True,
        "live_submission_permitted": False,
        "strategy_run": serialize_strategy_run(strategy_run),
        "profitability": profitability,
        "profit_finder_workbench": profit_finder,
        "resolution_accelerator": resolution,
        "lane_readiness": lanes,
        "next_best_steps": next_steps,
        "operator_guardrails": {
            "default_strategy_control_preserved": True,
            "live_orders_allowed": False,
            "pilot_arming_allowed": False,
            "manual_approval_relaxation_allowed": False,
        },
    }


def _profit_tools_artifact_stem(*, family: str, as_of: datetime) -> str:
    normalized_family = str(family or "default_strategy").strip().lower().replace("/", "_")
    return f"{as_of.date().isoformat()}-{normalized_family}-profit-tools"


def _render_profit_tools_markdown(payload: dict[str, Any]) -> str:
    profitability = payload.get("profitability") or {}
    resolution = payload.get("resolution_accelerator") or {}
    lanes = (payload.get("lane_readiness") or {}).get("lanes") or {}
    steps = payload.get("next_best_steps") or []
    reasons = (payload.get("profit_finder_workbench") or {}).get("top_decision_reasons") or []

    step_lines = "\n".join(
        f"- `{step.get('step')}` ({step.get('priority_score')}): {step.get('operator_action')}"
        for step in steps
    ) or "- No urgent profit-tool actions."
    reason_lines = "\n".join(
        f"- `{row.get('reason_code')}` / `{row.get('decision_status')}`: {row.get('decision_count')} decision(s)"
        for row in reasons[:10]
    ) or "- No decision reasons."
    lane_lines = "\n".join(
        f"- `{name}`: `{lane.get('status')}`, next `{lane.get('next_step')}`"
        for name, lane in lanes.items()
    ) or "- No lane readiness data."
    return f"""# Profit Tools Snapshot

**Generated:** {payload.get('generated_at')}
**Family:** `{payload.get('family')}`
**Paper only:** `{payload.get('paper_only')}`
**Live submission permitted:** `{payload.get('live_submission_permitted')}`

## Profit Gate

- Verdict: `{profitability.get('verdict')}`
- Realized P&L: {_fmt_money(profitability.get('realized_pnl'))}
- Mark-to-market P&L: {_fmt_money(profitability.get('mark_to_market_pnl'))}
- Open exposure: {_fmt_money(profitability.get('open_exposure'))}
- Resolved trades: {profitability.get('resolved_trades', 0)}
- Average CLV: {_fmt_cents(profitability.get('avg_clv'))}

## Next Best Steps

{step_lines}

## Resolution Accelerator

- Open trades: {resolution.get('open_trade_count', 0)}
- Open exposure: {_fmt_money(resolution.get('open_exposure'))}

## Top Decision Reasons

{reason_lines}

## Lane Readiness

{lane_lines}
"""


async def generate_profit_tools_artifact(
    session: AsyncSession,
    *,
    family: str = "default_strategy",
    as_of: datetime | None = None,
    use_cache: bool = False,
) -> dict[str, Any]:
    as_of = as_of or _utcnow()
    payload = await build_profit_tools_snapshot(session, family=family, use_cache=use_cache)
    repo_root = _repo_root()
    output_dir = repo_root / PROFIT_TOOLS_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _profit_tools_artifact_stem(family=payload.get("family") or family, as_of=as_of)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_profit_tools_markdown(payload), encoding="utf-8")
    return {
        "profit_tools_json_path": str(json_path),
        "profit_tools_markdown_path": str(markdown_path),
        "snapshot": payload,
    }
