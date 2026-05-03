from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.reports.strategy_review import _fmt_money, _repo_root
from app.strategies.kalshi_down_yes_fade import (
    MAX_YES_PRICE,
    MIN_YES_PRICE,
    STRATEGY_FAMILY,
    STRATEGY_NAME,
    STRATEGY_VERSION_KEY,
    ensure_active_kalshi_down_yes_fade_run,
    evaluate_kalshi_down_yes_fade_signal,
    run_kalshi_down_yes_fade_paper_lane,
)
from app.strategy_runs.service import get_active_strategy_run

ARTIFACT_DIR = "docs/research-lab/kalshi-down-yes-fade"
SCHEMA_VERSION = "kalshi_down_yes_fade_snapshot_v1"
MIN_OBSERVATION_DAYS = 30
MIN_RESOLVED_TRADES = 20
PAUSE_DRAWDOWN_PCT = Decimal("0.05")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


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
    return value


def _artifact_stem(as_of: datetime, window_days: int) -> str:
    return f"{as_of.date().isoformat()}-kalshi-down-yes-fade-{window_days}d"


async def _load_matching_signals(
    session: AsyncSession,
    *,
    window_start: datetime,
    window_end: datetime,
    max_signals: int,
) -> list[Signal]:
    rows = (
        await session.execute(
            select(Signal, Market.platform)
            .outerjoin(Market, Market.id == Signal.market_id)
            .where(
                Signal.fired_at >= window_start,
                Signal.fired_at <= window_end,
                Signal.signal_type == "price_move",
                Signal.price_at_fire >= MIN_YES_PRICE,
                Signal.price_at_fire < MAX_YES_PRICE,
                Signal.expected_value < Decimal("0"),
            )
            .order_by(Signal.fired_at.asc(), Signal.id.asc())
            .limit(max(1, min(int(max_signals), 50_000)))
        )
    ).all()
    return [
        signal
        for signal, platform in rows
        if evaluate_kalshi_down_yes_fade_signal(signal, market_platform=platform).eligible
    ]


def _historical_metrics(signals: Sequence[Signal]) -> dict[str, Any]:
    resolved = [
        signal
        for signal in signals
        if signal.resolved_correctly is not None
        and signal.profit_loss is not None
        and signal.clv is not None
    ]
    pnl_values = [_decimal(signal.profit_loss) or Decimal("0") for signal in resolved]
    clv_values = [_decimal(signal.clv) for signal in resolved if _decimal(signal.clv) is not None]
    total_pnl = sum(pnl_values, Decimal("0"))
    wins = sum(1 for value in pnl_values if value > 0)
    return {
        "matching_signals": len(signals),
        "resolved_signals": len(resolved),
        "signal_level_pnl": total_pnl,
        "avg_signal_clv": (sum(clv_values, Decimal("0")) / Decimal(len(clv_values))) if clv_values else None,
        "win_rate": Decimal(wins) / Decimal(len(resolved)) if resolved else None,
        "first_signal_at": _ensure_utc(signals[0].fired_at) if signals else None,
        "last_signal_at": _ensure_utc(signals[-1].fired_at) if signals else None,
    }


async def _paper_metrics(session: AsyncSession) -> dict[str, Any]:
    strategy_run = await get_active_strategy_run(session, STRATEGY_NAME)
    if strategy_run is None:
        return {
            "strategy_run": None,
            "opened_trades": 0,
            "resolved_trades": 0,
            "open_trades": 0,
            "realized_pnl": Decimal("0"),
            "mark_to_market_pnl": None,
            "open_exposure": Decimal("0"),
            "avg_clv": None,
            "fill_rate": None,
            "drawdown": None,
            "drawdown_pct": None,
            "skip_funnel": {},
            "pending_decisions": 0,
        }

    trade_rows = (
        await session.execute(
            select(PaperTrade, Signal)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .where(PaperTrade.strategy_run_id == strategy_run.id)
            .order_by(PaperTrade.opened_at.asc(), PaperTrade.id.asc())
        )
    ).all()
    decisions = (
        await session.execute(
            select(ExecutionDecision.reason_code, ExecutionDecision.decision_status, func.count(ExecutionDecision.id))
            .where(ExecutionDecision.strategy_run_id == strategy_run.id)
            .group_by(ExecutionDecision.reason_code, ExecutionDecision.decision_status)
        )
    ).all()
    skip_funnel: dict[str, int] = {}
    pending_decisions = 0
    opened_decisions = 0
    for reason_code, decision_status, count in decisions:
        key = str(reason_code or decision_status or "unknown")
        skip_funnel[key] = skip_funnel.get(key, 0) + int(count or 0)
        if decision_status == "pending_decision":
            pending_decisions += int(count or 0)
        if decision_status == "opened":
            opened_decisions += int(count or 0)

    resolved_rows = [
        (trade, signal)
        for trade, signal in trade_rows
        if trade.status == "resolved" and trade.pnl is not None
    ]
    open_rows = [(trade, signal) for trade, signal in trade_rows if trade.status == "open"]
    realized_pnl = sum((_decimal(trade.pnl) or Decimal("0")) for trade, _signal in resolved_rows)
    open_exposure = sum((_decimal(trade.size_usd) or Decimal("0")) for trade, _signal in open_rows)
    clv_values = [_decimal(signal.clv) for _trade, signal in resolved_rows if _decimal(signal.clv) is not None]
    decision_total = sum(skip_funnel.values())
    return {
        "strategy_run": {
            "id": str(strategy_run.id),
            "started_at": _ensure_utc(strategy_run.started_at),
            "strategy_version_id": strategy_run.strategy_version_id,
            "contract_snapshot": strategy_run.contract_snapshot or {},
        },
        "opened_trades": len(trade_rows),
        "resolved_trades": len(resolved_rows),
        "open_trades": len(open_rows),
        "realized_pnl": realized_pnl,
        "mark_to_market_pnl": None,
        "open_exposure": open_exposure,
        "avg_clv": (sum(clv_values, Decimal("0")) / Decimal(len(clv_values))) if clv_values else None,
        "fill_rate": Decimal(opened_decisions) / Decimal(decision_total) if decision_total else None,
        "drawdown": _decimal(strategy_run.max_drawdown),
        "drawdown_pct": _decimal(strategy_run.drawdown_pct),
        "skip_funnel": skip_funnel,
        "pending_decisions": pending_decisions,
    }


def _profitability_verdict(
    *,
    generated_at: datetime,
    historical: dict[str, Any],
    paper: dict[str, Any],
) -> tuple[str, list[str], list[dict[str, Any]]]:
    blockers: list[str] = []
    actions: list[dict[str, Any]] = []
    strategy_run = paper.get("strategy_run")
    resolved_trades = int(paper.get("resolved_trades") or 0)
    realized_pnl = _decimal(paper.get("realized_pnl")) or Decimal("0")
    avg_clv = _decimal(paper.get("avg_clv"))
    drawdown_pct = _decimal(paper.get("drawdown_pct"))
    pending_decisions = int(paper.get("pending_decisions") or 0)

    observed_days = 0
    if strategy_run and strategy_run.get("started_at"):
        started_at = _ensure_utc(strategy_run["started_at"])
        if started_at is not None:
            observed_days = max(0, int((generated_at - started_at).total_seconds() // 86400))
    else:
        blockers.append("no_active_candidate_run")
        actions.append(
            {
                "step": "start_kalshi_down_yes_fade_paper_run",
                "priority_score": 98,
                "why_ev": "This is the current best Alpha Factory bucket; it needs forward paper decisions with executable orderbook context.",
                "operator_action": "Run the scheduler or seed this paper lane, then monitor resolved trades, P&L, CLV, and orderbook-context skips.",
                "evidence": {"historical_matching_signals": historical.get("matching_signals")},
            }
        )

    if int(historical.get("matching_signals") or 0) == 0:
        blockers.append("no_matching_kalshi_down_yes_fade_signals")
    if observed_days < MIN_OBSERVATION_DAYS:
        blockers.append("observation_window_below_30d")
    if resolved_trades < MIN_RESOLVED_TRADES:
        blockers.append("insufficient_resolved_paper_trades")
    if realized_pnl <= Decimal("0"):
        blockers.append("nonpositive_execution_adjusted_pnl")
    if avg_clv is None or avg_clv <= Decimal("0"):
        blockers.append("nonpositive_avg_clv")
    if pending_decisions:
        blockers.append("pending_execution_decisions")
    if drawdown_pct is not None and drawdown_pct >= PAUSE_DRAWDOWN_PCT:
        blockers.append("paper_drawdown_pause")
    if resolved_trades >= MIN_RESOLVED_TRADES and realized_pnl < Decimal("0") and (avg_clv or Decimal("0")) < Decimal("0"):
        blockers.append("negative_pnl_and_clv_after_sample")

    if "paper_drawdown_pause" in blockers or "negative_pnl_and_clv_after_sample" in blockers:
        verdict = "paused"
    elif not blockers:
        verdict = "paper_profitable"
    elif strategy_run and (paper.get("opened_trades") or resolved_trades):
        verdict = "watch"
    elif historical.get("matching_signals"):
        verdict = "research_ready"
    else:
        verdict = "insufficient_evidence"

    if not actions:
        actions.append(
            {
                "step": "continue_kalshi_down_yes_fade_forward_validation",
                "priority_score": 90 if verdict in {"watch", "research_ready"} else 65,
                "why_ev": "The lane is already collecting the Alpha Factory's best current bucket; more resolved forward evidence is the highest-signal next proof.",
                "operator_action": "Keep the lane paper-only until 30 days and 20 resolved trades prove positive P&L and CLV.",
                "evidence": {
                    "resolved_trades": resolved_trades,
                    "realized_pnl": float(realized_pnl),
                    "avg_clv": float(avg_clv) if avg_clv is not None else None,
                },
            }
        )
    return verdict, sorted(set(blockers)), actions


async def build_kalshi_down_yes_fade_snapshot(
    session: AsyncSession,
    *,
    window_days: int = 30,
    max_signals: int = 5000,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _ensure_utc(as_of) or _utcnow()
    window_start = generated_at - timedelta(days=max(1, int(window_days)))
    signals = await _load_matching_signals(
        session,
        window_start=window_start,
        window_end=generated_at,
        max_signals=max_signals,
    )
    historical = _historical_metrics(signals)
    paper = await _paper_metrics(session)
    verdict, blockers, actions = _profitability_verdict(
        generated_at=generated_at,
        historical=historical,
        paper=paper,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "family": STRATEGY_FAMILY,
        "strategy_version": STRATEGY_VERSION_KEY,
        "strategy_name": STRATEGY_NAME,
        "window_start": window_start,
        "window_end": generated_at,
        "window_days": window_days,
        "max_signals": max_signals,
        "paper_only": True,
        "live_orders_enabled": False,
        "pilot_arming_enabled": False,
        "rule": {
            "platform": "kalshi",
            "signal_type": "price_move",
            "direction": "down",
            "min_yes_price": MIN_YES_PRICE,
            "max_yes_price_exclusive": MAX_YES_PRICE,
            "expected_value": "<0",
            "trade_direction": "buy_no",
            "targeted_orderbook_capture": True,
        },
        "historical": historical,
        "paper": paper,
        "verdict": verdict,
        "blockers": blockers,
        "next_best_actions": actions,
    }


def kalshi_down_yes_fade_lane_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    paper = snapshot.get("paper") or {}
    return {
        "family": STRATEGY_FAMILY,
        "strategy_version": STRATEGY_VERSION_KEY,
        "lane": "paper_forward_gate",
        "source_kind": "kalshi_down_yes_fade_snapshot",
        "source_ref": _json_safe(snapshot.get("generated_at")),
        "status": "completed",
        "verdict": snapshot.get("verdict") or "insufficient_evidence",
        "realized_pnl": _decimal(paper.get("realized_pnl")),
        "mark_to_market_pnl": _decimal(paper.get("mark_to_market_pnl")),
        "replay_net_pnl": None,
        "avg_clv": _decimal(paper.get("avg_clv")),
        "resolved_trades": int(paper.get("resolved_trades") or 0),
        "fill_rate": _decimal(paper.get("fill_rate")),
        "drawdown": _decimal(paper.get("drawdown")),
        "open_exposure": _decimal(paper.get("open_exposure")),
        "coverage_mode": "forward_paper" if paper.get("strategy_run") else "not_started",
        "blockers": snapshot.get("blockers") or [],
        "details_json": {
            "snapshot": _json_safe(snapshot),
            "next_best_actions": _json_safe(snapshot.get("next_best_actions") or []),
            "execution_adjusted_paper_pnl": _json_safe(paper.get("realized_pnl")),
        },
    }


def _render_markdown(snapshot: dict[str, Any]) -> str:
    historical = snapshot.get("historical") or {}
    paper = snapshot.get("paper") or {}
    blockers = "\n".join(f"- `{blocker}`" for blocker in snapshot.get("blockers") or []) or "- None"
    actions = "\n".join(
        f"- **{action.get('step')}**: {action.get('operator_action')}"
        for action in snapshot.get("next_best_actions") or []
    ) or "- None"
    return f"""# Kalshi Down-YES Fade v2

**Generated:** {snapshot.get('generated_at')}
**Verdict:** `{snapshot.get('verdict')}`
**Paper-only:** `true`

## Rule

Kalshi `price_move`, direction `down`, YES price `0.20 <= p < 0.50`, expected value `< 0`, expressed as `buy_no`.

## Historical Evidence

- Matching signals: {historical.get('matching_signals', 0)}
- Resolved signals: {historical.get('resolved_signals', 0)}
- Signal-level P&L: {_fmt_money(historical.get('signal_level_pnl'))}
- Average CLV: {historical.get('avg_signal_clv')}

## Forward Paper Evidence

- Opened trades: {paper.get('opened_trades', 0)}
- Resolved trades: {paper.get('resolved_trades', 0)}
- Realized P&L: {_fmt_money(paper.get('realized_pnl'))}
- Open exposure: {_fmt_money(paper.get('open_exposure'))}
- Average CLV: {paper.get('avg_clv')}

## Blockers

{blockers}

## Next Best Actions

{actions}
"""


async def generate_kalshi_down_yes_fade_artifact(
    session: AsyncSession,
    *,
    window_days: int = 30,
    max_signals: int = 5000,
    seed_paper: bool = False,
) -> dict[str, Any]:
    as_of = _utcnow()
    window_start = as_of - timedelta(days=max(1, int(window_days)))
    signals = await _load_matching_signals(
        session,
        window_start=window_start,
        window_end=as_of,
        max_signals=max_signals,
    )
    seed_result = None
    if seed_paper:
        started_at = min((_ensure_utc(signal.fired_at) for signal in signals if signal.fired_at), default=as_of)
        await ensure_active_kalshi_down_yes_fade_run(session, started_at=started_at)
        seed_result = await run_kalshi_down_yes_fade_paper_lane(
            session,
            signals,
            pending_retry_limit=0,
            backlog_limit=0,
            pending_expiry_limit=100,
        )

    snapshot = await build_kalshi_down_yes_fade_snapshot(
        session,
        window_days=window_days,
        max_signals=max_signals,
        as_of=as_of,
    )
    if seed_result is not None:
        snapshot["seed_result"] = seed_result
    root = _repo_root()
    artifact_dir = root / ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(as_of, window_days)
    json_path = artifact_dir / f"{stem}.json"
    markdown_path = artifact_dir / f"{stem}.md"
    json_path.write_text(json.dumps(_json_safe(snapshot), indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(_json_safe(snapshot)), encoding="utf-8")
    return {
        "snapshot": _json_safe(snapshot),
        "lane_payload": _json_safe(kalshi_down_yes_fade_lane_payload(snapshot)),
        "snapshot_json_path": str(json_path),
        "snapshot_markdown_path": str(markdown_path),
    }
