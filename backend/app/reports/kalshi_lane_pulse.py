from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.paper_trade import PaperTrade
from app.models.strategy_run import StrategyRun
from app.reports.strategy_review import _repo_root
from app.strategies.kalshi_cheap_yes_follow import (
    PAPER_QUARANTINE_DETAIL as CHEAP_YES_FOLLOW_QUARANTINE_DETAIL,
)
from app.strategies.kalshi_cheap_yes_follow import (
    PAPER_QUARANTINE_ENABLED as CHEAP_YES_FOLLOW_QUARANTINE_ENABLED,
)
from app.strategies.kalshi_cheap_yes_follow import (
    PAPER_QUARANTINE_REASON_CODE as CHEAP_YES_FOLLOW_QUARANTINE_REASON_CODE,
)

KALSHI_LANE_PULSE_SCHEMA_VERSION = "kalshi_lane_pulse_v1"
KALSHI_LANE_PULSE_ARTIFACT_DIR = "docs/research-lab/kalshi-lane-pulse"
KALSHI_LANE_FAMILIES = (
    "kalshi_down_yes_fade",
    "kalshi_low_yes_fade",
    "kalshi_very_low_yes_fade",
    "kalshi_cheap_yes_follow",
)

ZERO = Decimal("0")


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    normalized = _ensure_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _money(value: Decimal | int | float | None) -> float:
    if value is None:
        return 0.0
    return float(Decimal(str(value)).quantize(Decimal("0.01")))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _empty_lane(family: str) -> dict[str, Any]:
    return {
        "family": family,
        "strategy_run_id": None,
        "strategy_name": None,
        "run_status": None,
        "run_started_at": None,
        "opened_trades_window": 0,
        "open_trades": 0,
        "open_markets": 0,
        "open_exposure": 0.0,
        "resolved_trades_window": 0,
        "realized_pnl_window": 0.0,
        "avg_resolved_pnl_window": None,
        "decision_reasons": [],
        "duplicate_market_warnings": [],
        "quarantine": {"enabled": False},
    }


async def _active_runs_by_family(session: AsyncSession) -> dict[str, StrategyRun]:
    rows = (
        await session.execute(
            select(StrategyRun)
            .where(
                StrategyRun.strategy_family.in_(KALSHI_LANE_FAMILIES),
                StrategyRun.status == "active",
            )
            .order_by(StrategyRun.strategy_family.asc(), StrategyRun.started_at.desc())
        )
    ).scalars().all()
    runs: dict[str, StrategyRun] = {}
    for row in rows:
        if row.strategy_family and row.strategy_family not in runs:
            runs[row.strategy_family] = row
    return runs


async def _trade_rows(
    session: AsyncSession,
    *,
    window_start: datetime,
) -> list[tuple[PaperTrade, StrategyRun, Market]]:
    return (
        await session.execute(
            select(PaperTrade, StrategyRun, Market)
            .join(StrategyRun, StrategyRun.id == PaperTrade.strategy_run_id)
            .join(Market, Market.id == PaperTrade.market_id)
            .where(StrategyRun.strategy_family.in_(KALSHI_LANE_FAMILIES))
            .where(
                or_(
                    PaperTrade.status == "open",
                    PaperTrade.opened_at >= window_start,
                    PaperTrade.resolved_at >= window_start,
                )
            )
            .order_by(PaperTrade.opened_at.desc())
        )
    ).all()


async def _duplicate_trade_rows(
    session: AsyncSession,
    *,
    duplicate_start: datetime,
) -> list[tuple[PaperTrade, StrategyRun, Market]]:
    return (
        await session.execute(
            select(PaperTrade, StrategyRun, Market)
            .join(StrategyRun, StrategyRun.id == PaperTrade.strategy_run_id)
            .join(Market, Market.id == PaperTrade.market_id)
            .where(StrategyRun.strategy_family.in_(KALSHI_LANE_FAMILIES))
            .where(or_(PaperTrade.status == "open", PaperTrade.opened_at >= duplicate_start))
            .order_by(PaperTrade.opened_at.desc())
        )
    ).all()


async def _decision_reasons(
    session: AsyncSession,
    *,
    window_start: datetime,
) -> dict[str, list[dict[str, Any]]]:
    rows = (
        await session.execute(
            select(
                StrategyRun.strategy_family,
                ExecutionDecision.decision_status,
                ExecutionDecision.reason_code,
                func.count(ExecutionDecision.id),
            )
            .join(StrategyRun, StrategyRun.id == ExecutionDecision.strategy_run_id)
            .where(
                StrategyRun.strategy_family.in_(KALSHI_LANE_FAMILIES),
                ExecutionDecision.decision_at >= window_start,
            )
            .group_by(StrategyRun.strategy_family, ExecutionDecision.decision_status, ExecutionDecision.reason_code)
            .order_by(StrategyRun.strategy_family.asc(), func.count(ExecutionDecision.id).desc())
        )
    ).all()
    grouped: dict[str, list[dict[str, Any]]] = {family: [] for family in KALSHI_LANE_FAMILIES}
    for family, decision_status, reason_code, count in rows:
        if family not in grouped:
            continue
        grouped[family].append(
            {
                "decision_status": decision_status,
                "reason_code": reason_code,
                "count": int(count or 0),
            }
        )
    return {family: reasons[:10] for family, reasons in grouped.items()}


def _duplicate_warnings(
    rows: list[tuple[PaperTrade, StrategyRun, Market]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[tuple[PaperTrade, Market]]] = {}
    for trade, run, market in rows:
        family = run.strategy_family or "unknown"
        grouped.setdefault((family, str(trade.market_id)), []).append((trade, market))

    warnings: dict[str, list[dict[str, Any]]] = {family: [] for family in KALSHI_LANE_FAMILIES}
    for (family, market_id), trade_rows in grouped.items():
        if family not in warnings or len(trade_rows) < 2:
            continue
        trades = [trade for trade, _market in trade_rows]
        market = trade_rows[0][1]
        open_count = sum(1 for trade in trades if trade.status == "open")
        warning = {
            "market_id": market_id,
            "question": market.question,
            "trade_count": len(trades),
            "open_count": open_count,
            "resolved_count": sum(1 for trade in trades if trade.status == "resolved"),
            "first_opened_at": _iso(min((trade.opened_at for trade in trades), default=None)),
            "last_opened_at": _iso(max((trade.opened_at for trade in trades), default=None)),
            "total_size_usd": _money(sum((trade.size_usd or ZERO) for trade in trades)),
            "statuses": sorted({trade.status for trade in trades}),
            "severity": "active_duplicate" if open_count > 1 else "recent_duplicate",
        }
        warnings[family].append(warning)
    for family in warnings:
        warnings[family] = sorted(
            warnings[family],
            key=lambda item: (item["open_count"], item["trade_count"], item["last_opened_at"] or ""),
            reverse=True,
        )[:10]
    return warnings


def _verdict(lanes: list[dict[str, Any]]) -> str:
    if any(lane["duplicate_market_warnings"] for lane in lanes):
        return "needs_evidence_hygiene"
    if any(lane["open_trades"] for lane in lanes):
        return "collecting_forward_evidence"
    if any(lane["resolved_trades_window"] for lane in lanes):
        return "watch_resolved_evidence"
    return "waiting_for_signals"


def _next_actions(lanes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    duplicate_lanes = [lane["family"] for lane in lanes if lane["duplicate_market_warnings"]]
    if duplicate_lanes:
        actions.append(
            {
                "step": "inspect_duplicate_market_evidence",
                "why": "Repeated trades on the same market can make paper evidence look larger than it is.",
                "families": duplicate_lanes,
            }
        )
    cheap = next((lane for lane in lanes if lane["family"] == "kalshi_cheap_yes_follow"), None)
    if cheap and cheap["quarantine"].get("enabled"):
        actions.append(
            {
                "step": "keep_cheap_yes_follow_quarantined",
                "why": "Initial forward paper evidence was negative; the lane now reports skips until reviewed.",
                "families": ["kalshi_cheap_yes_follow"],
            }
        )
    open_lanes = [lane["family"] for lane in lanes if lane["open_trades"]]
    if open_lanes:
        actions.append(
            {
                "step": "wait_for_open_kalshi_resolutions",
                "why": "Promotion evidence needs resolved paper outcomes, not just opened trades.",
                "families": open_lanes,
            }
        )
    return actions


async def build_kalshi_lane_pulse(
    session: AsyncSession,
    *,
    window_hours: int = 24,
    duplicate_lookback_hours: int = 72,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(as_of) or datetime.now(timezone.utc)
    window_start = observed_at - timedelta(hours=window_hours)
    duplicate_start = observed_at - timedelta(hours=duplicate_lookback_hours)
    lanes = {family: _empty_lane(family) for family in KALSHI_LANE_FAMILIES}

    active_runs = await _active_runs_by_family(session)
    for family, run in active_runs.items():
        lanes[family].update(
            {
                "strategy_run_id": str(run.id),
                "strategy_name": run.strategy_name,
                "run_status": run.status,
                "run_started_at": _iso(run.started_at),
            }
        )

    resolved_pnls: dict[str, list[Decimal]] = {family: [] for family in KALSHI_LANE_FAMILIES}
    open_markets: dict[str, set[str]] = {family: set() for family in KALSHI_LANE_FAMILIES}
    for trade, run, _market in await _trade_rows(session, window_start=window_start):
        family = run.strategy_family
        if family not in lanes:
            continue
        lane = lanes[family]
        opened_at = _ensure_utc(trade.opened_at)
        resolved_at = _ensure_utc(trade.resolved_at)
        if opened_at is not None and opened_at >= window_start:
            lane["opened_trades_window"] += 1
        if trade.status == "open":
            lane["open_trades"] += 1
            lane["open_exposure"] = _money(Decimal(str(lane["open_exposure"])) + (trade.size_usd or ZERO))
            open_markets[family].add(str(trade.market_id))
        if trade.status == "resolved" and (
            (resolved_at is not None and resolved_at >= window_start)
            or (opened_at is not None and opened_at >= window_start)
        ):
            lane["resolved_trades_window"] += 1
            pnl = Decimal(str(trade.pnl or ZERO))
            resolved_pnls[family].append(pnl)
            lane["realized_pnl_window"] = _money(Decimal(str(lane["realized_pnl_window"])) + pnl)

    decision_reasons = await _decision_reasons(session, window_start=window_start)
    duplicates = _duplicate_warnings(await _duplicate_trade_rows(session, duplicate_start=duplicate_start))
    for family, lane in lanes.items():
        lane["open_markets"] = len(open_markets[family])
        lane["decision_reasons"] = decision_reasons.get(family, [])
        lane["duplicate_market_warnings"] = duplicates.get(family, [])
        if resolved_pnls[family]:
            lane["avg_resolved_pnl_window"] = _money(sum(resolved_pnls[family], ZERO) / len(resolved_pnls[family]))

    lanes["kalshi_cheap_yes_follow"]["quarantine"] = {
        "enabled": CHEAP_YES_FOLLOW_QUARANTINE_ENABLED,
        "reason_code": CHEAP_YES_FOLLOW_QUARANTINE_REASON_CODE,
        "detail": CHEAP_YES_FOLLOW_QUARANTINE_DETAIL,
    }

    lane_list = [lanes[family] for family in KALSHI_LANE_FAMILIES]
    return {
        "schema_version": KALSHI_LANE_PULSE_SCHEMA_VERSION,
        "generated_at": observed_at.isoformat(),
        "window_hours": window_hours,
        "window_start": window_start.isoformat(),
        "duplicate_lookback_hours": duplicate_lookback_hours,
        "paper_only": True,
        "live_submission_permitted": False,
        "verdict": _verdict(lane_list),
        "lanes": lane_list,
        "next_best_actions": _next_actions(lane_list),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    rows = []
    for lane in payload.get("lanes") or []:
        rows.append(
            "| {family} | {open_trades} | ${open_exposure:.2f} | {opened} | {resolved} | ${pnl:.2f} | {dupes} | {quarantine} |".format(
                family=lane["family"],
                open_trades=lane["open_trades"],
                open_exposure=lane["open_exposure"],
                opened=lane["opened_trades_window"],
                resolved=lane["resolved_trades_window"],
                pnl=lane["realized_pnl_window"],
                dupes=len(lane["duplicate_market_warnings"]),
                quarantine="yes" if lane["quarantine"].get("enabled") else "no",
            )
        )
    actions = "\n".join(
        f"- `{action.get('step')}`: {action.get('why')}"
        for action in payload.get("next_best_actions") or []
    ) or "- None"
    return f"""# Kalshi Lane Pulse

**Generated:** {payload.get('generated_at')}
**Verdict:** `{payload.get('verdict')}`
**Window hours:** {payload.get('window_hours')}
**Duplicate lookback hours:** {payload.get('duplicate_lookback_hours')}
**Paper only:** `{payload.get('paper_only')}`
**Live submission permitted:** `{payload.get('live_submission_permitted')}`

| Lane | Open | Exposure | Opened window | Resolved window | Realized P&L | Duplicate warnings | Quarantine |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
{chr(10).join(rows)}

## Next Actions

{actions}
"""


async def generate_kalshi_lane_pulse_artifact(
    session: AsyncSession,
    *,
    window_hours: int = 24,
    duplicate_lookback_hours: int = 72,
) -> dict[str, Any]:
    payload = await build_kalshi_lane_pulse(
        session,
        window_hours=window_hours,
        duplicate_lookback_hours=duplicate_lookback_hours,
    )
    root = _repo_root()
    artifact_dir = root / KALSHI_LANE_PULSE_ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _ensure_utc(datetime.fromisoformat(payload["generated_at"])) or datetime.now(timezone.utc)
    stem = f"{generated_at.date().isoformat()}-kalshi-lane-pulse"
    json_path = artifact_dir / f"{stem}.json"
    markdown_path = artifact_dir / f"{stem}.md"
    json_path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "snapshot": payload,
        "pulse_json_path": str(json_path),
        "pulse_markdown_path": str(markdown_path),
    }


__all__ = [
    "build_kalshi_lane_pulse",
    "generate_kalshi_lane_pulse_artifact",
]
