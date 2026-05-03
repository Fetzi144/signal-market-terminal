from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

PASSING_VERDICTS = {
    "profitable",
    "healthy",
    "passing",
    "pass",
    "paper_profitable",
    "research_ready",
}


def _decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _coverage_score(value: str | None) -> int:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return 0
    if "limited" in normalized or "partial" in normalized or "incomplete" in normalized:
        return 0
    if "complete" in normalized or "full" in normalized:
        return 1
    return 0


def build_rank_key(row: dict[str, Any]) -> dict[str, Any]:
    blockers = row.get("blockers") or row.get("blockers_json") or []
    if not isinstance(blockers, list):
        blockers = [str(blockers)]
    verdict = str(row.get("verdict") or "").strip().lower()
    realized_pnl = _decimal(row.get("realized_pnl"))
    mark_to_market_pnl = _decimal(row.get("mark_to_market_pnl"))
    execution_pnl = _decimal(
        (row.get("details_json") or {}).get("execution_adjusted_paper_pnl")
        if isinstance(row.get("details_json"), dict)
        else None
    )
    if execution_pnl == 0:
        execution_pnl = realized_pnl + mark_to_market_pnl
    replay_net_pnl = _decimal(row.get("replay_net_pnl"))
    avg_clv = _decimal(row.get("avg_clv"))
    resolved_trades = int(row.get("resolved_trades") or 0)
    drawdown = _decimal(row.get("drawdown"))
    open_exposure = _decimal(row.get("open_exposure"))
    return {
        "passing_verdict": 1 if verdict in PASSING_VERDICTS else 0,
        "complete_coverage": _coverage_score(row.get("coverage_mode")),
        "positive_execution_pnl": 1 if execution_pnl > 0 else 0,
        "positive_replay_net_pnl": 1 if replay_net_pnl > 0 else 0,
        "positive_avg_clv": 1 if avg_clv > 0 else 0,
        "resolved_trades": resolved_trades,
        "drawdown_penalty": float(drawdown),
        "open_exposure_penalty": float(open_exposure),
        "blocker_penalty": len(blockers),
        "execution_pnl": float(execution_pnl),
        "replay_net_pnl": float(replay_net_pnl),
        "avg_clv": float(avg_clv),
    }


def sort_key_for_rank_key(rank_key: dict[str, Any]) -> tuple:
    return (
        int(rank_key.get("passing_verdict") or 0),
        int(rank_key.get("complete_coverage") or 0),
        int(rank_key.get("positive_execution_pnl") or 0),
        int(rank_key.get("positive_replay_net_pnl") or 0),
        int(rank_key.get("positive_avg_clv") or 0),
        int(rank_key.get("resolved_trades") or 0),
        -float(rank_key.get("drawdown_penalty") or 0.0),
        -float(rank_key.get("open_exposure_penalty") or 0.0),
        -int(rank_key.get("blocker_penalty") or 0),
        float(rank_key.get("execution_pnl") or 0.0),
        float(rank_key.get("replay_net_pnl") or 0.0),
        float(rank_key.get("avg_clv") or 0.0),
    )


def rank_lane_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for payload in payloads:
        row = dict(payload)
        row["rank_key"] = build_rank_key(row)
        ranked.append(row)
    ranked.sort(key=lambda item: sort_key_for_rank_key(item["rank_key"]), reverse=True)
    for index, row in enumerate(ranked, start=1):
        row["rank_position"] = index
    return ranked
