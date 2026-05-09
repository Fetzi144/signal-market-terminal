"""Operator board for Alpha Factory candidates and Kalshi forward-paper lanes."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.reports.alpha_factory import (
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_PLATFORM,
    build_alpha_factory_snapshot,
)
from app.reports.kalshi_lane_pulse import build_kalshi_lane_pulse
from app.reports.strategy_review import _repo_root

ALPHA_EVIDENCE_BOARD_SCHEMA_VERSION = "alpha_evidence_board_v1"
ALPHA_EVIDENCE_BOARD_ARTIFACT_DIR = "docs/research-lab/alpha-evidence-board"


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


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(_decimal(value, Decimal(str(default))))
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _candidate_metric(candidate: dict[str, Any], split: str, metric: str, default: Any = None) -> Any:
    return (((candidate.get("metrics") or {}).get(split) or {}).get(metric, default))


def _candidate_card(candidate: dict[str, Any], *, source: str) -> dict[str, Any]:
    blockers = list(candidate.get("blockers") or [])
    existing_lane = candidate.get("existing_lane") or {}
    has_quarantine_blocker = "matched_quarantined_lane_family" in blockers
    ready = bool(candidate.get("ready_for_paper_lane"))

    if ready and not blockers and not existing_lane:
        action_bucket = "implement_next"
        status = "paper_lane_ready"
        operator_action = "Implement frozen paper-only evaluator; keep live orders disabled."
        priority = 95
    elif ready and existing_lane:
        action_bucket = "collecting_forward_evidence"
        status = "existing_lane_ready"
        operator_action = "Keep existing lane paper-only and wait for forward resolutions."
        priority = 85
    elif has_quarantine_blocker:
        action_bucket = "cut_or_quarantine"
        status = "quarantined_variant"
        operator_action = "Do not spawn a new lane; keep the related lane paused until reviewed."
        priority = 70
    elif blockers:
        action_bucket = "needs_review"
        status = "blocked"
        operator_action = "Review blockers before any implementation work."
        priority = 55
    else:
        action_bucket = "search_backlog"
        status = "holdout_failed_or_unready"
        operator_action = "Leave in the research backlog unless later evidence improves."
        priority = 35

    test_sample = _int(_candidate_metric(candidate, "test", "sample_count"))
    test_pnl = _float(_candidate_metric(candidate, "test", "total_profit_loss"))
    test_clv = _candidate_metric(candidate, "test", "avg_clv")

    return {
        "kind": "alpha_candidate",
        "source": source,
        "candidate_id": candidate.get("candidate_id"),
        "strategy_version": candidate.get("strategy_version"),
        "rank": candidate.get("rank"),
        "status": status,
        "action_bucket": action_bucket,
        "priority_score": priority,
        "operator_action": operator_action,
        "paper_only": True,
        "live_submission_permitted": False,
        "trade_direction": candidate.get("trade_direction"),
        "strategy_archetype": candidate.get("strategy_archetype"),
        "dedupe_status": candidate.get("dedupe_status"),
        "next_step": candidate.get("next_step"),
        "blockers": blockers,
        "existing_lane_family": existing_lane.get("family"),
        "existing_lane_version": existing_lane.get("strategy_version"),
        "test": {
            "sample_count": test_sample,
            "total_profit_loss": test_pnl,
            "avg_clv": _float(test_clv) if test_clv is not None else None,
            "max_drawdown": _float(_candidate_metric(candidate, "test", "max_drawdown")),
        },
        "rule_digest": candidate.get("rule_digest"),
        "rule_label": candidate.get("rule_label"),
    }


def _lane_card(lane: dict[str, Any]) -> dict[str, Any]:
    duplicate_count = len(lane.get("duplicate_market_warnings") or [])
    quarantine = lane.get("quarantine") or {}
    open_trades = _int(lane.get("open_trades"))
    resolved_trades = _int(lane.get("resolved_trades_window"))
    realized_pnl = _float(lane.get("realized_pnl_window"))

    if duplicate_count:
        action_bucket = "needs_hygiene"
        status = "evidence_hygiene_required"
        operator_action = "Run duplicate trade hygiene before interpreting this lane."
        priority = 100
    elif quarantine.get("enabled"):
        action_bucket = "cut_or_quarantine"
        status = "quarantined"
        operator_action = "Keep paused; require manual review before any new exposure."
        priority = 80
    elif open_trades:
        action_bucket = "collecting_forward_evidence"
        status = "open_forward_paper"
        operator_action = "Wait for market resolutions; do not judge by open exposure alone."
        priority = 85
    elif resolved_trades and realized_pnl > 0:
        action_bucket = "needs_review"
        status = "positive_forward_window"
        operator_action = "Review resolved P&L and CLV before considering promotion gates."
        priority = 75
    elif resolved_trades:
        action_bucket = "cut_or_quarantine"
        status = "negative_forward_window"
        operator_action = "Keep out of promotion; inspect whether the rule should be retired."
        priority = 70
    elif lane.get("run_status") == "active":
        action_bucket = "waiting_for_signals"
        status = "active_idle"
        operator_action = "Keep the lane running and wait for qualifying signals."
        priority = 45
    else:
        action_bucket = "search_backlog"
        status = "inactive_or_missing_run"
        operator_action = "No active forward evidence loop is visible for this family."
        priority = 30

    return {
        "kind": "paper_lane",
        "family": lane.get("family"),
        "strategy_run_id": lane.get("strategy_run_id"),
        "strategy_name": lane.get("strategy_name"),
        "run_status": lane.get("run_status"),
        "status": status,
        "action_bucket": action_bucket,
        "priority_score": priority,
        "operator_action": operator_action,
        "paper_only": True,
        "live_submission_permitted": False,
        "open_trades": open_trades,
        "open_markets": _int(lane.get("open_markets")),
        "open_exposure": _float(lane.get("open_exposure")),
        "opened_trades_window": _int(lane.get("opened_trades_window")),
        "resolved_trades_window": resolved_trades,
        "realized_pnl_window": realized_pnl,
        "avg_resolved_pnl_window": lane.get("avg_resolved_pnl_window"),
        "duplicate_warning_count": duplicate_count,
        "quarantine": quarantine,
        "top_decision_reasons": list(lane.get("decision_reasons") or [])[:5],
    }


def _candidate_cards(alpha_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    seen: set[str] = set()
    sources = [
        ("candidate_queue", alpha_snapshot.get("candidate_queue") or []),
        ("top_candidates", alpha_snapshot.get("top_candidates") or []),
    ]
    for source, candidates in sources:
        for candidate in candidates:
            key = str(
                candidate.get("candidate_id")
                or candidate.get("strategy_version")
                or candidate.get("rule_digest")
                or len(seen)
            )
            if key in seen:
                continue
            seen.add(key)
            cards.append(_candidate_card(candidate, source=source))
    return sorted(cards, key=lambda card: card["priority_score"], reverse=True)


def _summary(
    *,
    lane_cards: list[dict[str, Any]],
    candidate_cards: list[dict[str, Any]],
    alpha_snapshot: dict[str, Any],
    lane_pulse: dict[str, Any],
) -> dict[str, Any]:
    return {
        "alpha_factory_verdict": alpha_snapshot.get("verdict"),
        "lane_pulse_verdict": lane_pulse.get("verdict"),
        "candidate_pool_count": _int(alpha_snapshot.get("candidate_pool_count")),
        "new_paper_lane_candidates": sum(1 for card in candidate_cards if card["action_bucket"] == "implement_next"),
        "existing_ready_candidates": _int(alpha_snapshot.get("existing_ready_candidate_count")),
        "suppressed_candidate_count": _int(alpha_snapshot.get("suppressed_candidate_count")),
        "open_forward_trades": sum(_int(card.get("open_trades")) for card in lane_cards),
        "open_forward_exposure": round(sum(_float(card.get("open_exposure")) for card in lane_cards), 2),
        "resolved_forward_trades_window": sum(_int(card.get("resolved_trades_window")) for card in lane_cards),
        "realized_pnl_window": round(sum(_float(card.get("realized_pnl_window")) for card in lane_cards), 2),
        "duplicate_warning_count": sum(_int(card.get("duplicate_warning_count")) for card in lane_cards),
        "quarantined_lane_count": sum(1 for card in lane_cards if (card.get("quarantine") or {}).get("enabled")),
        "paper_only": True,
        "live_submission_permitted": False,
    }


def _verdict(summary: dict[str, Any], lane_cards: list[dict[str, Any]]) -> str:
    if summary["duplicate_warning_count"]:
        return "needs_evidence_hygiene"
    if summary["new_paper_lane_candidates"]:
        return "new_paper_lane_ready"
    if summary["open_forward_trades"] or summary["existing_ready_candidates"]:
        return "collecting_forward_evidence"
    if summary["resolved_forward_trades_window"]:
        return "review_forward_evidence"
    if any(card["action_bucket"] == "waiting_for_signals" for card in lane_cards):
        return "waiting_for_signals"
    return "search_for_alpha"


def _next_actions(
    *,
    verdict: str,
    summary: dict[str, Any],
    lane_cards: list[dict[str, Any]],
    candidate_cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    hygiene_lanes = [card["family"] for card in lane_cards if card["action_bucket"] == "needs_hygiene"]
    if hygiene_lanes:
        actions.append(
            {
                "step": "repair_evidence_hygiene",
                "priority_score": 100,
                "families": hygiene_lanes,
                "why": "Duplicate paper trades can inflate or distort forward evidence.",
                "operator_action": "Run duplicate-trade-hygiene and regenerate this board.",
            }
        )

    ready_candidates = [card for card in candidate_cards if card["action_bucket"] == "implement_next"]
    if ready_candidates:
        best = ready_candidates[0]
        actions.append(
            {
                "step": "implement_next_frozen_paper_lane",
                "priority_score": 95,
                "candidate_id": best.get("candidate_id"),
                "strategy_version": best.get("strategy_version"),
                "why": "Alpha Factory found a new holdout-surviving executable rule not covered by an existing lane.",
                "operator_action": "Implement paper-only evaluator and keep all live-order flags disabled.",
            }
        )

    quarantined = [card["family"] for card in lane_cards if (card.get("quarantine") or {}).get("enabled")]
    if quarantined:
        actions.append(
            {
                "step": "keep_quarantined_lanes_paused",
                "priority_score": 80,
                "families": quarantined,
                "why": "Quarantined lanes already showed poor or risky forward evidence.",
                "operator_action": "Do not create related variants until manual review clears the family.",
            }
        )

    open_lanes = [card["family"] for card in lane_cards if _int(card.get("open_trades")) > 0]
    if open_lanes:
        actions.append(
            {
                "step": "wait_for_forward_paper_resolutions",
                "priority_score": 75,
                "families": open_lanes,
                "why": "Open paper positions are exposure, not proof.",
                "operator_action": "Let the markets settle, then compare realized P&L, CLV, and drawdown.",
            }
        )

    if verdict == "search_for_alpha":
        actions.append(
            {
                "step": "expand_alpha_search",
                "priority_score": 60,
                "why": "No current lane or candidate is actionable.",
                "operator_action": "Run Alpha Factory with more resolved coverage or add a new feature template.",
            }
        )

    if not actions:
        actions.append(
            {
                "step": "keep_collecting_evidence",
                "priority_score": 50,
                "why": "No urgent hygiene or implementation work is visible.",
                "operator_action": "Regenerate after the next signal/resolution cycle.",
            }
        )

    return sorted(actions, key=lambda item: item["priority_score"], reverse=True)


def build_alpha_evidence_board_from_snapshots(
    *,
    alpha_snapshot: dict[str, Any],
    lane_pulse: dict[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(generated_at) or _utcnow()
    lane_cards = sorted(
        [_lane_card(lane) for lane in lane_pulse.get("lanes") or []],
        key=lambda card: card["priority_score"],
        reverse=True,
    )
    candidate_cards = _candidate_cards(alpha_snapshot)
    summary = _summary(
        lane_cards=lane_cards,
        candidate_cards=candidate_cards,
        alpha_snapshot=alpha_snapshot,
        lane_pulse=lane_pulse,
    )
    verdict = _verdict(summary, lane_cards)
    return {
        "schema_version": ALPHA_EVIDENCE_BOARD_SCHEMA_VERSION,
        "generated_at": observed_at.isoformat(),
        "paper_only": True,
        "live_submission_permitted": False,
        "verdict": verdict,
        "summary": summary,
        "sources": {
            "alpha_factory": {
                "schema_version": alpha_snapshot.get("schema_version"),
                "generated_at": alpha_snapshot.get("generated_at"),
                "platform": alpha_snapshot.get("platform"),
                "window_days": alpha_snapshot.get("window_days"),
                "max_signals": alpha_snapshot.get("max_signals"),
                "verdict": alpha_snapshot.get("verdict"),
            },
            "kalshi_lane_pulse": {
                "schema_version": lane_pulse.get("schema_version"),
                "generated_at": lane_pulse.get("generated_at"),
                "window_hours": lane_pulse.get("window_hours"),
                "duplicate_lookback_hours": lane_pulse.get("duplicate_lookback_hours"),
                "verdict": lane_pulse.get("verdict"),
            },
        },
        "lane_cards": lane_cards,
        "candidate_cards": candidate_cards,
        "next_best_actions": _next_actions(
            verdict=verdict,
            summary=summary,
            lane_cards=lane_cards,
            candidate_cards=candidate_cards,
        ),
        "warnings": sorted(
            {
                "paper_only_board_no_live_money_signal",
                "open_positions_are_not_promotion_evidence",
                *list(alpha_snapshot.get("warnings") or []),
            }
        ),
    }


async def build_alpha_evidence_board(
    session: AsyncSession,
    *,
    window_days: int = 365,
    max_signals: int = 50_000,
    platform: str = DEFAULT_PLATFORM,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    lane_window_hours: int = 48,
    duplicate_lookback_hours: int = 72,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(as_of) or _utcnow()
    alpha_snapshot = await build_alpha_factory_snapshot(
        session,
        window_days=window_days,
        max_signals=max_signals,
        platform=platform,
        max_candidates=max_candidates,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
        as_of=observed_at,
    )
    lane_pulse = await build_kalshi_lane_pulse(
        session,
        window_hours=lane_window_hours,
        duplicate_lookback_hours=duplicate_lookback_hours,
        as_of=observed_at,
    )
    return build_alpha_evidence_board_from_snapshots(
        alpha_snapshot=alpha_snapshot,
        lane_pulse=lane_pulse,
        generated_at=observed_at,
    )


def _card_table(cards: list[dict[str, Any]], *, kind: str) -> str:
    if kind == "lane":
        rows = [
            "| {family} | `{status}` | `{bucket}` | {open_trades} | ${exposure:.2f} | {resolved} | ${pnl:.2f} | {dupes} |".format(
                family=card.get("family"),
                status=card.get("status"),
                bucket=card.get("action_bucket"),
                open_trades=card.get("open_trades"),
                exposure=_float(card.get("open_exposure")),
                resolved=card.get("resolved_trades_window"),
                pnl=_float(card.get("realized_pnl_window")),
                dupes=card.get("duplicate_warning_count"),
            )
            for card in cards
        ]
        return "\n".join(rows) or "| - | - | - | - | - | - | - | - |"

    rows = [
        "| {source} | `{strategy}` | `{status}` | `{bucket}` | {sample} | {pnl:.4f} | {clv} | `{dedupe}` |".format(
            source=card.get("source"),
            strategy=card.get("strategy_version") or card.get("candidate_id"),
            status=card.get("status"),
            bucket=card.get("action_bucket"),
            sample=(card.get("test") or {}).get("sample_count"),
            pnl=_float((card.get("test") or {}).get("total_profit_loss")),
            clv=(card.get("test") or {}).get("avg_clv"),
            dedupe=card.get("dedupe_status"),
        )
        for card in cards
    ]
    return "\n".join(rows) or "| - | - | - | - | - | - | - | - |"


def _render_markdown(board: dict[str, Any]) -> str:
    summary = board.get("summary") or {}
    action_lines = "\n".join(
        f"- `{action.get('step')}`: {action.get('operator_action')} ({action.get('why')})"
        for action in board.get("next_best_actions") or []
    ) or "- None"
    warning_lines = "\n".join(f"- `{warning}`" for warning in board.get("warnings") or []) or "- None"
    return f"""# Alpha Evidence Board

**Generated:** {board.get('generated_at')}
**Verdict:** `{board.get('verdict')}`
**Paper only:** `{board.get('paper_only')}`
**Live submission permitted:** `{board.get('live_submission_permitted')}`

## Summary

| New candidates | Existing ready | Open trades | Open exposure | Resolved window | Realized P&L | Duplicate warnings | Quarantined lanes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| {summary.get('new_paper_lane_candidates')} | {summary.get('existing_ready_candidates')} | {summary.get('open_forward_trades')} | ${_float(summary.get('open_forward_exposure')):.2f} | {summary.get('resolved_forward_trades_window')} | ${_float(summary.get('realized_pnl_window')):.2f} | {summary.get('duplicate_warning_count')} | {summary.get('quarantined_lane_count')} |

## Lane Cards

| Family | Status | Bucket | Open | Exposure | Resolved window | Realized P&L | Dupes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
{_card_table(board.get('lane_cards') or [], kind='lane')}

## Candidate Cards

| Source | Strategy | Status | Bucket | Test N | Test P&L | Test CLV | Dedupe |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
{_card_table(board.get('candidate_cards') or [], kind='candidate')}

## Next Best Actions

{action_lines}

## Warnings

{warning_lines}
"""


def _artifact_stem(as_of: datetime) -> str:
    return f"{as_of.date().isoformat()}-alpha-evidence-board"


async def generate_alpha_evidence_board_artifact(
    session: AsyncSession,
    *,
    window_days: int = 365,
    max_signals: int = 50_000,
    platform: str = DEFAULT_PLATFORM,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_train_sample: int = 20,
    min_validation_sample: int = 10,
    min_test_sample: int = 10,
    lane_window_hours: int = 48,
    duplicate_lookback_hours: int = 72,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    observed_at = _ensure_utc(as_of) or _utcnow()
    board = await build_alpha_evidence_board(
        session,
        window_days=window_days,
        max_signals=max_signals,
        platform=platform,
        max_candidates=max_candidates,
        min_train_sample=min_train_sample,
        min_validation_sample=min_validation_sample,
        min_test_sample=min_test_sample,
        lane_window_hours=lane_window_hours,
        duplicate_lookback_hours=duplicate_lookback_hours,
        as_of=observed_at,
    )
    output_dir = _repo_root() / ALPHA_EVIDENCE_BOARD_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(observed_at)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(_json_safe(board), indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(board), encoding="utf-8")
    return {
        "snapshot": board,
        "evidence_board_json_path": str(json_path),
        "evidence_board_markdown_path": str(markdown_path),
    }


__all__ = [
    "build_alpha_evidence_board",
    "build_alpha_evidence_board_from_snapshots",
    "generate_alpha_evidence_board_artifact",
]
