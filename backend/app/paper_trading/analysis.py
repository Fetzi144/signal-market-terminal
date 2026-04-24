"""Read-only analysis helpers for the default paper-trading strategy."""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.comparison import compare_strategy_measurement_modes, empty_strategy_measurement_modes
from app.config import settings
from app.default_strategy import (
    default_strategy_skip_label,
    get_default_strategy_contract,
)
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_status
from app.metrics import (
    default_strategy_latest_review_age_seconds,
    default_strategy_latest_review_generated_at_timestamp,
    default_strategy_pending_decision_count,
    default_strategy_pending_decision_max_age_seconds,
    default_strategy_review_outdated,
)
from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.paper_trading.evidence_freshness import build_evidence_freshness
from app.paper_trading.portfolio_views import (
    _get_metrics as _get_trade_metrics,
)
from app.paper_trading.portfolio_views import (
    _get_pnl_curve as _get_trade_pnl_curve,
)
from app.paper_trading.portfolio_views import (
    _get_portfolio_state as _get_trade_portfolio_state,
)
from app.paper_trading.review_verdict import build_review_verdict
from app.signals.probability import brier_score
from app.strategy_runs.service import (
    get_active_strategy_run,
    get_default_strategy_launch_boundary,
    serialize_strategy_run,
)

ZERO = Decimal("0")
PENDING_DECISION_EXAMPLE_LIMIT = 5
OVERDUE_OPEN_TRADE_EXAMPLE_LIMIT = 5


@dataclass(frozen=True)
class DecisionSummary:
    id: uuid.UUID
    signal_id: uuid.UUID
    decision_at: datetime | None
    decision_status: str
    reason_code: str | None
    details: dict | None = None


def _safe_float(value):
    return float(value) if value is not None else None


def _safe_seconds(value: timedelta | None) -> float | None:
    if value is None:
        return None
    return round(value.total_seconds(), 1)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(str(len(values)))


def _compute_max_drawdown(values: list[Decimal]) -> Decimal:
    peak = ZERO
    running = ZERO
    max_drawdown = ZERO
    for value in values:
        running += value
        if running > peak:
            peak = running
        drawdown = peak - running
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _trade_opened_sort_key(trade_row: tuple[PaperTrade, Signal]) -> datetime:
    trade, _signal = trade_row
    return _ensure_utc(trade.opened_at) or datetime.min.replace(tzinfo=timezone.utc)


def _trade_resolved_sort_key(trade_row: tuple[PaperTrade, Signal]) -> datetime:
    trade, signal = trade_row
    return (
        _ensure_utc(trade.resolved_at)
        or _ensure_utc(signal.fired_at)
        or _ensure_utc(trade.opened_at)
        or datetime.min.replace(tzinfo=timezone.utc)
    )


def _signal_sort_key(signal: Signal) -> datetime:
    return _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc)


def _observation_status(days_tracked: float | None, *, launched: bool, traded_signals: int) -> str:
    if not launched:
        return "no_active_run"
    if traded_signals == 0:
        return "live_waiting_for_trades"
    if days_tracked is None:
        return "no_active_run"
    if days_tracked < settings.default_strategy_min_observation_days:
        return "collecting_data"
    if days_tracked < settings.default_strategy_preferred_observation_days:
        return "minimum_window_reached"
    return "preferred_window_reached"


def _detector_verdict(
    resolved_signals: int,
    avg_clv: Decimal | None,
    total_profit_loss: Decimal,
    detector_brier: Decimal | None,
) -> tuple[str, str]:
    if resolved_signals < 5:
        return "watch", "Need more resolved samples before making a keep/cut call."
    if avg_clv is not None and avg_clv < ZERO and total_profit_loss < ZERO:
        return "cut", "Negative CLV and negative hypothetical P&L over the review window."
    if detector_brier is not None and detector_brier > Decimal("0.25"):
        return "watch", "Calibration is weak; monitor before trusting this detector more."
    if avg_clv is not None and avg_clv > ZERO and total_profit_loss >= ZERO:
        return "keep", "Positive edge with acceptable realized contribution so far."
    return "watch", "Mixed signals; keep collecting data before changing exposure."


def _empty_portfolio() -> dict:
    return {"open_trades": [], "open_exposure": ZERO, "total_resolved": 0, "cumulative_pnl": ZERO, "wins": 0, "losses": 0, "win_rate": ZERO}


def _empty_metrics() -> dict:
    return {
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "cumulative_pnl": 0.0,
        "shadow_cumulative_pnl": 0.0,
        "avg_pnl": 0.0,
        "max_drawdown": 0.0,
        "sharpe_ratio": 0.0,
        "profit_factor": 0.0,
        "shadow_profit_factor": 0.0,
        "best_trade": 0.0,
        "worst_trade": 0.0,
        "liquidity_constrained_trades": 0,
        "trades_missing_orderbook_context": 0,
    }


def _portfolio_from_trade_rows(trade_rows: list[tuple[PaperTrade, Signal]]) -> dict:
    open_trades = [trade for trade, _signal in trade_rows if trade.status == "open"]
    resolved_trades = [trade for trade, _signal in trade_rows if trade.status == "resolved" and trade.pnl is not None]
    total_resolved = len(resolved_trades)
    cumulative_pnl = sum((trade.pnl or ZERO for trade in resolved_trades), ZERO)
    wins = sum(1 for trade in resolved_trades if trade.pnl is not None and trade.pnl > ZERO)
    losses = sum(1 for trade in resolved_trades if trade.pnl is not None and trade.pnl <= ZERO)
    return {
        "open_trades": sorted(open_trades, key=lambda trade: _ensure_utc(trade.opened_at) or datetime.min.replace(tzinfo=timezone.utc), reverse=True),
        "open_exposure": sum((trade.size_usd for trade in open_trades), ZERO),
        "total_resolved": total_resolved,
        "cumulative_pnl": cumulative_pnl,
        "wins": wins,
        "losses": losses,
        "win_rate": Decimal(str(wins / total_resolved)).quantize(Decimal("0.0001")) if total_resolved > 0 else ZERO,
    }


def _metrics_from_trade_rows(trade_rows: list[tuple[PaperTrade, Signal]]) -> dict:
    resolved_rows = [(trade, signal) for trade, signal in sorted(trade_rows, key=_trade_resolved_sort_key) if trade.status == "resolved" and trade.pnl is not None]
    if not resolved_rows:
        return _empty_metrics()
    pnls = [float(trade.pnl) for trade, _signal in resolved_rows if trade.pnl is not None]
    shadow_pnls = [float(trade.shadow_pnl) for trade, _signal in resolved_rows if trade.shadow_pnl is not None]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl <= 0]
    shadow_wins = [pnl for pnl in shadow_pnls if pnl > 0]
    shadow_losses = [pnl for pnl in shadow_pnls if pnl <= 0]
    cumulative, running, peak, max_drawdown = [], 0.0, 0.0, 0.0
    for pnl in pnls:
        running += pnl
        cumulative.append(running)
    for value in cumulative:
        if value > peak:
            peak = value
        max_drawdown = max(max_drawdown, peak - value)
    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((pnl - mean_pnl) ** 2 for pnl in pnls) / (len(pnls) - 1)
        sharpe = (mean_pnl / math.sqrt(variance)) if variance > 0 else 0.0
    else:
        sharpe = 0.0
    total_wins = sum(wins) if wins else 0.0
    total_losses = abs(sum(losses)) if losses else 0.0
    shadow_total_wins = sum(shadow_wins) if shadow_wins else 0.0
    shadow_total_losses = abs(sum(shadow_losses)) if shadow_losses else 0.0
    profit_factor = (total_wins / total_losses) if total_losses > 0 else float("inf") if total_wins > 0 else 0.0
    shadow_profit_factor = (shadow_total_wins / shadow_total_losses) if shadow_total_losses > 0 else float("inf") if shadow_total_wins > 0 else 0.0
    liquidity_constrained_trades = sum(1 for trade, _signal in resolved_rows if isinstance(trade.details, dict) and isinstance(trade.details.get("shadow_execution"), dict) and trade.details["shadow_execution"].get("liquidity_constrained") is True)
    trades_missing_orderbook_context = sum(1 for trade, _signal in resolved_rows if isinstance(trade.details, dict) and isinstance(trade.details.get("shadow_execution"), dict) and trade.details["shadow_execution"].get("missing_orderbook_context") is True)
    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        "cumulative_pnl": round(sum(pnls), 2),
        "shadow_cumulative_pnl": round(sum(shadow_pnls), 2) if shadow_pnls else 0.0,
        "avg_pnl": round(mean_pnl, 2),
        "max_drawdown": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "shadow_profit_factor": round(shadow_profit_factor, 4) if shadow_profit_factor != float("inf") else None,
        "best_trade": round(max(pnls), 2) if pnls else 0.0,
        "worst_trade": round(min(pnls), 2) if pnls else 0.0,
        "liquidity_constrained_trades": liquidity_constrained_trades,
        "trades_missing_orderbook_context": trades_missing_orderbook_context,
    }


def _pnl_curve_from_trade_rows(trade_rows: list[tuple[PaperTrade, Signal]]) -> list[dict]:
    resolved_rows = [(trade, signal) for trade, signal in sorted(trade_rows, key=_trade_resolved_sort_key) if trade.status == "resolved" and trade.pnl is not None and trade.resolved_at is not None]
    curve, running = [], Decimal("0")
    for trade, _signal in resolved_rows:
        running += trade.pnl or ZERO
        curve.append({"timestamp": _ensure_utc(trade.resolved_at).isoformat(), "pnl": float(running), "trade_pnl": float(trade.pnl), "shadow_trade_pnl": float(trade.shadow_pnl) if trade.shadow_pnl is not None else None, "direction": trade.direction, "trade_id": str(trade.id)})
    return curve


def _dedupe_signals_by_trade_rows(trade_rows: list[tuple[PaperTrade, Signal]], *, resolved_only: bool = False) -> list[Signal]:
    ordered_rows = sorted(trade_rows, key=_trade_resolved_sort_key if resolved_only else _trade_opened_sort_key)
    deduped, seen = [], set()
    for trade, signal in ordered_rows:
        if resolved_only and trade.status != "resolved":
            continue
        if signal.id in seen:
            continue
        seen.add(signal.id)
        deduped.append(signal)
    return deduped


def _risk_scope_for_decision(decision: ExecutionDecision) -> str | None:
    details = decision.details or {}
    risk_result = details.get("risk_result")
    return risk_result.get("risk_scope") if isinstance(risk_result, dict) else None


def _risk_result_for_decision(decision: ExecutionDecision) -> dict:
    details = decision.details or {}
    risk_result = details.get("risk_result")
    return risk_result if isinstance(risk_result, dict) else {}


def _skip_reason_row(decision: ExecutionDecision) -> tuple[str, str]:
    details = decision.details or {}
    reason_code = decision.reason_code or "unclassified"
    return reason_code, details.get("reason_label") or default_strategy_skip_label(reason_code) or "Unclassified"


def _pending_decision_watch(
    decision_rows: list[ExecutionDecision],
    *,
    now: datetime,
    example_limit: int = PENDING_DECISION_EXAMPLE_LIMIT,
) -> dict:
    pending_rows = [row for row in decision_rows if row.decision_status == "pending_decision"]
    if not pending_rows:
        return {
            "count": 0,
            "oldest_decision_at": None,
            "max_age_seconds": 0.0,
            "avg_age_seconds": 0.0,
            "examples": [],
        }

    pending_rows.sort(key=lambda row: _ensure_utc(row.decision_at) or datetime.min.replace(tzinfo=timezone.utc))
    ages = [
        max(
            timedelta(0),
            now - ((_ensure_utc(row.decision_at) or now)),
        )
        for row in pending_rows
    ]
    oldest_pending = pending_rows[0]
    avg_age_seconds = sum(age.total_seconds() for age in ages) / len(ages)
    examples = []
    for row, age in zip(pending_rows[:example_limit], ages[:example_limit], strict=False):
        examples.append(
            {
                "decision_id": str(row.id),
                "signal_id": str(row.signal_id),
                "decision_at": _ensure_utc(row.decision_at).isoformat() if row.decision_at else None,
                "age_seconds": round(age.total_seconds(), 1),
                "reason_code": row.reason_code,
            }
        )

    return {
        "count": len(pending_rows),
        "oldest_decision_at": _ensure_utc(oldest_pending.decision_at).isoformat() if oldest_pending.decision_at else None,
        "max_age_seconds": round(max(age.total_seconds() for age in ages), 1),
        "avg_age_seconds": round(avg_age_seconds, 1),
        "examples": examples,
    }


def _publish_pending_decision_metrics(pending_watch: dict) -> None:
    default_strategy_pending_decision_count.set(float(pending_watch.get("count", 0)))
    default_strategy_pending_decision_max_age_seconds.set(float(pending_watch.get("max_age_seconds", 0.0) or 0.0))


def _publish_evidence_freshness_metrics(evidence_freshness: dict) -> None:
    generated_at = evidence_freshness.get("latest_review_generated_at")
    if generated_at:
        parsed = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
        default_strategy_latest_review_generated_at_timestamp.set(parsed.timestamp())
    else:
        default_strategy_latest_review_generated_at_timestamp.set(0.0)
    default_strategy_latest_review_age_seconds.set(float(evidence_freshness.get("review_age_seconds") or 0.0))
    default_strategy_review_outdated.set(1.0 if evidence_freshness.get("review_outdated") else 0.0)


def _empty_overdue_open_trade_watch() -> dict:
    return {
        "count": 0,
        "examples": [],
    }


def _build_resolution_reconciliation(
    *,
    headline: dict,
    trade_funnel: dict,
    pending_watch: dict,
    overdue_open_trade_watch: dict,
    run_integrity: dict,
    evidence_freshness: dict,
) -> dict:
    return {
        "open_trades": int(headline.get("open_trades") or 0),
        "missing_resolutions": int(headline.get("missing_resolutions") or 0),
        "overdue_open_trades": int(headline.get("overdue_open_trades") or 0),
        "resolved_trades": int(trade_funnel.get("resolved_trades") or 0),
        "resolved_signals": int(trade_funnel.get("resolved_signals") or 0),
        "unresolved_traded_signals": int(trade_funnel.get("unresolved_traded_signals") or 0),
        "pending_decisions": int(pending_watch.get("count") or 0),
        "pending_decision_max_age_seconds": float(pending_watch.get("max_age_seconds") or 0.0),
        "overdue_open_trade_watch": overdue_open_trade_watch,
        "integrity_errors": list(run_integrity.get("integrity_errors") or []),
        "evidence_freshness_status": evidence_freshness.get("status"),
        "evidence_freshness_reason": evidence_freshness.get("reason"),
        "status": _resolution_reconciliation_status(
            headline=headline,
            trade_funnel=trade_funnel,
            pending_watch=pending_watch,
            run_integrity=run_integrity,
        ),
    }


def _resolution_reconciliation_status(
    *,
    headline: dict,
    trade_funnel: dict,
    pending_watch: dict,
    run_integrity: dict,
) -> str:
    if run_integrity.get("integrity_errors"):
        return "integrity_blocked"
    if int(pending_watch.get("count") or 0) > 0:
        return "pending_decisions"
    if int(headline.get("overdue_open_trades") or 0) > 0:
        return "overdue_open_trades"
    if int(trade_funnel.get("unresolved_traded_signals") or 0) > 0:
        return "awaiting_resolution"
    if int(trade_funnel.get("resolved_trades") or 0) > 0:
        return "reconciled"
    return "collecting"


def _serialize_replay_review_status(replay_status: dict | None) -> dict:
    replay = replay_status or {}
    return {
        "coverage_mode": replay.get("coverage_mode", "no_detector_activity"),
        "configured_supported_detectors": list(replay.get("configured_supported_detectors") or []),
        "supported_detectors": list(replay.get("supported_detectors") or []),
        "unsupported_detectors": list(replay.get("unsupported_detectors") or []),
        "recent_coverage_limited_run_count_24h": int(replay.get("recent_coverage_limited_run_count_24h") or 0),
    }


async def get_overdue_open_trade_count(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    query = (
        select(func.count(PaperTrade.id))
        .join(Market, Market.id == PaperTrade.market_id)
        .where(
            PaperTrade.status == "open",
            or_(
                Market.active.is_(False),
                Market.end_date < now,
            ),
        )
    )
    if strategy_run_id is not None:
        query = query.where(PaperTrade.strategy_run_id == strategy_run_id)
    return int((await session.execute(query)).scalar_one() or 0)


async def get_overdue_open_trade_watch(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None = None,
    example_limit: int = OVERDUE_OPEN_TRADE_EXAMPLE_LIMIT,
) -> dict:
    now = datetime.now(timezone.utc)
    base_filters = [
        PaperTrade.status == "open",
        or_(
            Market.active.is_(False),
            Market.end_date < now,
        ),
    ]
    if strategy_run_id is not None:
        base_filters.append(PaperTrade.strategy_run_id == strategy_run_id)

    count_result = await session.execute(
        select(func.count(PaperTrade.id))
        .join(Market, Market.id == PaperTrade.market_id)
        .where(*base_filters)
    )
    total = int(count_result.scalar_one() or 0)
    if total == 0:
        return _empty_overdue_open_trade_watch()

    example_result = await session.execute(
        select(PaperTrade, Market)
        .join(Market, Market.id == PaperTrade.market_id)
        .where(*base_filters)
        .order_by(Market.end_date.asc().nulls_last(), PaperTrade.opened_at.asc())
        .limit(example_limit)
    )
    examples = []
    for trade, market in example_result.all():
        market_end = _ensure_utc(market.end_date)
        examples.append(
            {
                "trade_id": str(trade.id),
                "signal_id": str(trade.signal_id),
                "platform": market.platform,
                "platform_id": market.platform_id,
                "market_question": market.question,
                "market_active": bool(market.active),
                "market_end_date": market_end.isoformat() if market_end else None,
                "opened_at": _ensure_utc(trade.opened_at).isoformat() if trade.opened_at else None,
                "age_past_end_seconds": _safe_seconds(now - market_end)
                if market_end is not None and market_end < now
                else 0.0,
            }
        )

    return {
        "count": total,
        "examples": examples,
    }


async def get_default_strategy_run_lookup(session: AsyncSession) -> dict:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    launch_boundary = get_default_strategy_launch_boundary()
    if strategy_run is None:
        return {"state": "no_active_run", "strategy_run": None, "bootstrap_required": True, "suggested_launch_boundary_at": launch_boundary.isoformat() if launch_boundary is not None else None}
    return {"state": "active_run", "strategy_run": serialize_strategy_run(strategy_run), "bootstrap_required": False, "suggested_launch_boundary_at": launch_boundary.isoformat() if launch_boundary is not None else None}


def _default_strategy_signal_filters(
    *,
    launch_at: datetime,
    qualified_only: bool = False,
    before_launch: bool = False,
) -> list:
    filters = []
    if settings.default_strategy_signal_type:
        filters.append(Signal.signal_type == settings.default_strategy_signal_type)
    filters.append(Signal.fired_at < launch_at if before_launch else Signal.fired_at >= launch_at)
    if qualified_only:
        min_ev_threshold = Decimal(str(settings.min_ev_threshold))
        filters.extend(
            [
                Signal.outcome_id.is_not(None),
                Signal.estimated_probability.is_not(None),
                Signal.price_at_fire.is_not(None),
                Signal.expected_value.is_not(None),
                func.abs(Signal.expected_value) >= min_ev_threshold,
            ]
        )
    return filters


async def _count_default_strategy_signals(
    session: AsyncSession,
    *,
    launch_at: datetime,
    qualified_only: bool = False,
    before_launch: bool = False,
) -> int:
    result = await session.execute(
        select(func.count(Signal.id)).where(
            *_default_strategy_signal_filters(
                launch_at=launch_at,
                qualified_only=qualified_only,
                before_launch=before_launch,
            )
        )
    )
    return int(result.scalar_one() or 0)


async def _load_qualified_default_strategy_signal_ids(
    session: AsyncSession,
    *,
    launch_at: datetime,
) -> list[uuid.UUID]:
    result = await session.execute(
        select(Signal.id)
        .where(
            *_default_strategy_signal_filters(
                launch_at=launch_at,
                qualified_only=True,
            )
        )
        .order_by(Signal.fired_at.asc(), Signal.id.asc())
    )
    return list(result.scalars().all())


async def _load_strategy_trade_rows(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
) -> list[tuple[PaperTrade, Signal]]:
    result = await session.execute(
        select(PaperTrade, Signal)
        .join(Signal, Signal.id == PaperTrade.signal_id)
        .where(PaperTrade.strategy_run_id == strategy_run_id)
        .order_by(PaperTrade.opened_at.desc())
    )
    return result.all()


async def _load_qualified_decision_rows(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    launch_at: datetime,
) -> list[DecisionSummary]:
    result = await session.execute(
        select(
            ExecutionDecision.id,
            ExecutionDecision.signal_id,
            ExecutionDecision.decision_at,
            ExecutionDecision.decision_status,
            ExecutionDecision.reason_code,
            ExecutionDecision.details,
        )
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run_id,
            *_default_strategy_signal_filters(
                launch_at=launch_at,
                qualified_only=True,
            ),
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
    )
    return [
        DecisionSummary(
            id=decision_id,
            signal_id=signal_id,
            decision_at=decision_at,
            decision_status=decision_status,
            reason_code=reason_code,
            details=details if isinstance(details, dict) else {},
        )
        for decision_id, signal_id, decision_at, decision_status, reason_code, details in result.all()
    ]


async def _count_excluded_pre_launch_trades(
    session: AsyncSession,
    *,
    launch_at: datetime,
) -> int:
    if not settings.default_strategy_signal_type:
        return 0
    result = await session.execute(
        select(func.count(PaperTrade.id))
        .join(Signal, Signal.id == PaperTrade.signal_id)
        .where(
            Signal.signal_type == settings.default_strategy_signal_type,
            Signal.fired_at < launch_at,
        )
    )
    return int(result.scalar_one() or 0)


async def _count_excluded_legacy_trades(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
) -> int:
    result = await session.execute(
        select(func.count(PaperTrade.id)).where(
            or_(
                PaperTrade.strategy_run_id.is_(None),
                PaperTrade.strategy_run_id != strategy_run_id,
            )
        )
    )
    return int(result.scalar_one() or 0)


async def _get_default_strategy_scope(session: AsyncSession) -> dict:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    launch_boundary = get_default_strategy_launch_boundary()
    replay_status = await fetch_polymarket_replay_status(session)
    lookup = (
        {
            "state": "active_run",
            "strategy_run": serialize_strategy_run(strategy_run),
            "bootstrap_required": False,
            "suggested_launch_boundary_at": launch_boundary.isoformat() if launch_boundary is not None else None,
        }
        if strategy_run is not None
        else {
            "state": "no_active_run",
            "strategy_run": None,
            "bootstrap_required": True,
            "suggested_launch_boundary_at": launch_boundary.isoformat() if launch_boundary is not None else None,
        }
    )
    now = datetime.now(timezone.utc)
    if strategy_run is None:
        pending_watch = {
            "count": 0,
            "oldest_decision_at": None,
            "max_age_seconds": 0.0,
            "avg_age_seconds": 0.0,
            "examples": [],
        }
        _publish_pending_decision_metrics(pending_watch)
        return {
            "lookup": lookup,
            "strategy_run": None,
            "portfolio": _empty_portfolio(),
            "metrics": _empty_metrics(),
            "pnl_curve": [],
            "trade_funnel": {
                "candidate_signals": 0,
                "qualified_signals": 0,
                "opened_trade_signals": 0,
                "skipped_signals": 0,
                "pending_decision_signals": 0,
                "traded_signals": 0,
                "qualified_not_traded": 0,
                "open_trades": 0,
                "resolved_trades": 0,
                "resolved_signals": 0,
                "unresolved_traded_signals": 0,
                "pre_launch_candidate_signals": 0,
                "excluded_pre_launch_trades": 0,
                "excluded_legacy_trades": 0,
                "integrity_errors": [],
                "conservation_holds": True,
            },
            "skip_reasons": [],
            "risk_block_summary": {
                "local_paper_book_blocks": 0,
                "shared_global_blocks": 0,
                "execution_liquidity_blocks": 0,
                "local_reason_counts": {},
                "shared_global_reason_counts": {},
                "shared_global_upstream_reason_counts": {},
                "execution_liquidity_reason_counts": {},
                "shared_global_examples": [],
            },
            "candidate_signals": [],
            "qualified_signals": [],
            "strategy_trade_rows": [],
            "resolved_trade_rows": [],
            "resolved_trade_signals": [],
            "started_at": None,
            "launch_at": launch_boundary,
            "first_trade_at": None,
            "latest_trade_activity_at": None,
            "latest_decision_at": None,
            "observed_at": now,
            "pending_decision_watch": pending_watch,
            "comparison_modes": empty_strategy_measurement_modes(),
            "replay": replay_status,
            "run_state": lookup["state"],
        }

    launch_at = _ensure_utc(strategy_run.started_at)
    candidate_signal_count = await _count_default_strategy_signals(session, launch_at=launch_at)
    pre_launch_candidate_signals = await _count_default_strategy_signals(
        session,
        launch_at=launch_at,
        before_launch=True,
    )
    qualified_signal_ids = await _load_qualified_default_strategy_signal_ids(session, launch_at=launch_at)

    strategy_trade_rows = await _load_strategy_trade_rows(session, strategy_run_id=strategy_run.id)
    strategy_trade_rows.sort(key=_trade_opened_sort_key, reverse=True)
    trades_by_signal_id = {signal.id: trade for trade, signal in strategy_trade_rows}

    decision_rows = await _load_qualified_decision_rows(
        session,
        strategy_run_id=strategy_run.id,
        launch_at=launch_at,
    )
    decisions_by_signal_id = {row.signal_id: row for row in decision_rows}
    pending_watch = _pending_decision_watch(decision_rows, now=now)
    _publish_pending_decision_metrics(pending_watch)

    opened_trade_signals = 0
    skipped_signals = 0
    pending_decision_signals = 0
    integrity_errors: list[dict] = []
    skip_reason_counts: dict[str, dict] = {}
    local_reason_counts: dict[str, int] = {}
    shared_reason_counts: dict[str, int] = {}
    shared_upstream_reason_counts: dict[str, int] = {}
    execution_liquidity_reason_counts: dict[str, int] = {}
    shared_global_examples: list[dict] = []
    for signal_id in qualified_signal_ids:
        trade = trades_by_signal_id.get(signal_id)
        decision = decisions_by_signal_id.get(signal_id)
        if decision is None:
            integrity_errors.append({"signal_id": str(signal_id), "error": "missing_execution_decision"})
            continue
        if trade is not None:
            if decision.decision_status != "opened":
                integrity_errors.append({"signal_id": str(signal_id), "error": "trade_decision_status_mismatch", "decision_status": decision.decision_status})
                continue
            opened_trade_signals += 1
            continue
        if decision.decision_status == "pending_decision":
            pending_decision_signals += 1
            continue
        if decision.decision_status == "opened":
            integrity_errors.append({"signal_id": str(signal_id), "error": "opened_without_trade"})
            continue
        if decision.decision_status != "skipped":
            integrity_errors.append({"signal_id": str(signal_id), "error": "unrecognized_decision_status", "decision_status": decision.decision_status})
            continue
        skipped_signals += 1
        reason_code, reason_label = _skip_reason_row(decision)
        skip_reason_counts.setdefault(reason_code, {"reason_code": reason_code, "reason_label": reason_label, "count": 0})["count"] += 1
        risk_result = _risk_result_for_decision(decision)
        risk_scope = risk_result.get("risk_scope")
        if risk_scope == "shared_global":
            shared_reason_counts[reason_code] = shared_reason_counts.get(reason_code, 0) + 1
            upstream_reason = str(risk_result.get("original_reason_code") or reason_code)
            shared_upstream_reason_counts[upstream_reason] = shared_upstream_reason_counts.get(upstream_reason, 0) + 1
            if len(shared_global_examples) < 3:
                shared_global_examples.append(
                    {
                        "signal_id": str(signal_id),
                        "decision_id": str(decision.id),
                        "reason_code": reason_code,
                        "reason_label": reason_label,
                        "upstream_reason_code": upstream_reason,
                        "detail": (decision.details or {}).get("detail"),
                    }
                )
        elif risk_scope == "local_paper_book":
            local_reason_counts[reason_code] = local_reason_counts.get(reason_code, 0) + 1
        elif reason_code.startswith("execution_"):
            execution_liquidity_reason_counts[reason_code] = execution_liquidity_reason_counts.get(reason_code, 0) + 1

    open_trade_rows = [(trade, signal) for trade, signal in strategy_trade_rows if trade.status == "open"]
    resolved_trade_rows = [(trade, signal) for trade, signal in strategy_trade_rows if trade.status == "resolved" and trade.pnl is not None]
    resolved_trade_rows.sort(key=_trade_resolved_sort_key)
    resolved_trade_signals = _dedupe_signals_by_trade_rows(resolved_trade_rows, resolved_only=True)
    resolved_trade_signal_ids = {signal.id for signal in resolved_trade_signals}
    traded_signal_ids = {signal.id for _trade, signal in strategy_trade_rows}
    first_trade_at = min((_ensure_utc(trade.opened_at) for trade, _signal in strategy_trade_rows), default=None)
    latest_trade_activity_at = max(
        (
            value
            for trade, _signal in strategy_trade_rows
            for value in (_ensure_utc(trade.opened_at), _ensure_utc(trade.resolved_at))
            if value is not None
        ),
        default=None,
    )
    latest_decision_at = max((_ensure_utc(row.decision_at) for row in decision_rows if row.decision_at is not None), default=None)
    portfolio = await _get_trade_portfolio_state(session, strategy_run_id=strategy_run.id)
    metrics = await _get_trade_metrics(session, strategy_run_id=strategy_run.id)
    pnl_curve = await _get_trade_pnl_curve(session, strategy_run_id=strategy_run.id)
    comparison_modes = await compare_strategy_measurement_modes(
        session,
        start_date=strategy_run.started_at,
        end_date=now,
        strategy_run_id=strategy_run.id,
    )
    excluded_pre_launch_trades = await _count_excluded_pre_launch_trades(session, launch_at=launch_at)
    excluded_legacy_trades = await _count_excluded_legacy_trades(session, strategy_run_id=strategy_run.id)
    qualified_signal_count = len(qualified_signal_ids)
    trade_funnel = {
        "candidate_signals": candidate_signal_count,
        "qualified_signals": qualified_signal_count,
        "opened_trade_signals": opened_trade_signals,
        "skipped_signals": skipped_signals,
        "pending_decision_signals": pending_decision_signals,
        "traded_signals": opened_trade_signals,
        "qualified_not_traded": skipped_signals + pending_decision_signals,
        "open_trades": len(open_trade_rows),
        "resolved_trades": metrics["total_trades"],
        "resolved_signals": len(resolved_trade_signal_ids),
        "unresolved_traded_signals": max(0, len(traded_signal_ids) - len(resolved_trade_signal_ids)),
        "pre_launch_candidate_signals": pre_launch_candidate_signals,
        "excluded_pre_launch_trades": excluded_pre_launch_trades,
        "excluded_legacy_trades": excluded_legacy_trades,
        "integrity_errors": integrity_errors,
        "conservation_holds": qualified_signal_count == (opened_trade_signals + skipped_signals + pending_decision_signals),
    }
    return {
        "lookup": lookup,
        "strategy_run": strategy_run,
        "candidate_signals": [],
        "qualified_signals": [],
        "strategy_trade_rows": strategy_trade_rows,
        "resolved_trade_rows": resolved_trade_rows,
        "resolved_trade_signals": resolved_trade_signals,
        "portfolio": portfolio,
        "metrics": metrics,
        "pnl_curve": pnl_curve,
        "trade_funnel": trade_funnel,
        "started_at": launch_at,
        "launch_at": launch_at,
        "first_trade_at": first_trade_at,
        "latest_trade_activity_at": latest_trade_activity_at,
        "latest_decision_at": latest_decision_at,
        "observed_at": now,
        "pending_decision_watch": pending_watch,
        "skip_reasons": sorted(skip_reason_counts.values(), key=lambda row: (-row["count"], row["reason_label"])),
        "risk_block_summary": {
            "local_paper_book_blocks": sum(local_reason_counts.values()),
            "shared_global_blocks": sum(shared_reason_counts.values()),
            "execution_liquidity_blocks": sum(execution_liquidity_reason_counts.values()),
            "local_reason_counts": local_reason_counts,
            "shared_global_reason_counts": shared_reason_counts,
            "shared_global_upstream_reason_counts": shared_upstream_reason_counts,
            "execution_liquidity_reason_counts": execution_liquidity_reason_counts,
            "shared_global_examples": shared_global_examples,
        },
        "comparison_modes": comparison_modes,
        "replay": replay_status,
        "run_state": lookup["state"],
    }


async def get_strategy_portfolio_state(session: AsyncSession) -> dict:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return _empty_portfolio()
    return await _get_trade_portfolio_state(session, strategy_run_id=strategy_run.id)


async def get_strategy_metrics(session: AsyncSession) -> dict:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return _empty_metrics()
    return await _get_trade_metrics(session, strategy_run_id=strategy_run.id)


async def get_strategy_pnl_curve(session: AsyncSession) -> list[dict]:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return []
    return await _get_trade_pnl_curve(session, strategy_run_id=strategy_run.id)


async def get_strategy_history(
    session: AsyncSession,
    *,
    status: str | None = None,
    direction: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return {"trades": [], "total": 0, "page": page, "page_size": page_size}

    query = select(PaperTrade).where(PaperTrade.strategy_run_id == strategy_run.id)
    count_query = select(func.count(PaperTrade.id)).where(PaperTrade.strategy_run_id == strategy_run.id)
    if status:
        query = query.where(PaperTrade.status == status)
        count_query = count_query.where(PaperTrade.status == status)
    if direction:
        query = query.where(PaperTrade.direction == direction)
        count_query = count_query.where(PaperTrade.direction == direction)

    total = int((await session.execute(count_query)).scalar_one() or 0)
    result = await session.execute(
        query.order_by(PaperTrade.opened_at.desc()).offset((page - 1) * page_size).limit(page_size)
    )
    return {
        "trades": result.scalars().all(),
        "total": total,
        "page": page,
        "page_size": page_size,
    }


async def get_default_strategy_pending_decision_watch(session: AsyncSession) -> dict:
    return (await _get_default_strategy_scope(session))["pending_decision_watch"]


async def _serialize_strategy_health(session: AsyncSession, *, scope: dict) -> dict:
    from app.reports.strategy_review import get_latest_default_strategy_review_artifact_metadata

    strategy_run = scope["strategy_run"]
    serialized_strategy_run = serialize_strategy_run(strategy_run)
    now = scope["observed_at"]
    contract = strategy_run.contract_snapshot if strategy_run is not None else get_default_strategy_contract(started_at=scope["launch_at"])
    portfolio = scope["portfolio"]
    metrics = scope["metrics"]
    started_at = scope["started_at"]
    replay = _serialize_replay_review_status(scope.get("replay"))
    latest_review_artifact = get_latest_default_strategy_review_artifact_metadata()
    evidence_freshness = build_evidence_freshness(
        observed_at=now,
        run_state=scope["run_state"],
        latest_review_artifact=latest_review_artifact,
        active_strategy_run=serialized_strategy_run,
        started_at=scope.get("started_at"),
        latest_trade_activity_at=scope.get("latest_trade_activity_at"),
        latest_decision_at=scope.get("latest_decision_at"),
        pending_watch=scope["pending_decision_watch"],
    )
    _publish_evidence_freshness_metrics(evidence_freshness)
    days_tracked = round((now - started_at).total_seconds() / 86400, 1) if started_at is not None else None
    if strategy_run is None:
        observation = {"started_at": None, "baseline_start_at": contract.get("baseline_start_at"), "first_trade_at": None, "days_tracked": None, "status": "no_active_run", "minimum_days": settings.default_strategy_min_observation_days, "preferred_days": settings.default_strategy_preferred_observation_days, "days_until_minimum_window": settings.default_strategy_min_observation_days}
        headline = {"open_exposure": 0.0, "open_trades": 0, "resolved_trades": 0, "resolved_signals": 0, "missing_resolutions": 0, "overdue_open_trades": 0, "cumulative_pnl": 0.0, "avg_clv": None, "profit_factor": 0.0, "win_rate": 0.0, "max_drawdown": 0.0, "drawdown_pct": None, "current_equity": None, "peak_equity": None, "brier_score": None}
        run_integrity = {"pre_launch_candidate_signals": 0, "excluded_pre_launch_trades": 0, "excluded_legacy_trades": 0, "trades_missing_orderbook_context": 0, "integrity_errors": [], "debug_drawdown": {"reconstructed_max_drawdown": 0.0, "reconstructed_current_equity": None}}
        resolution_reconciliation = _build_resolution_reconciliation(
            headline=headline,
            trade_funnel=scope["trade_funnel"],
            pending_watch=scope["pending_decision_watch"],
            overdue_open_trade_watch=_empty_overdue_open_trade_watch(),
            run_integrity=run_integrity,
            evidence_freshness=evidence_freshness,
        )
        review_verdict = build_review_verdict(
            strategy_run=None,
            run_state=scope["run_state"],
            observation=observation,
            trade_funnel=scope["trade_funnel"],
            pending_watch=scope["pending_decision_watch"],
            comparison_modes=scope["comparison_modes"],
            replay=replay,
            headline=headline,
        )
        return {
            "strategy": contract,
            "strategy_run": serialized_strategy_run,
            "run_state": scope["run_state"],
            "bootstrap_required": True,
            "observation": observation,
            "trade_funnel": scope["trade_funnel"],
            "pending_decision_watch": scope["pending_decision_watch"],
            "skip_reasons": [],
            "headline": headline,
            "execution_realism": {"shadow_cumulative_pnl": 0.0, "shadow_profit_factor": 0.0, "liquidity_constrained_trades": 0, "trades_missing_orderbook_context": 0},
            "risk_blocks": scope["risk_block_summary"],
            "run_integrity": run_integrity,
            "resolution_reconciliation": resolution_reconciliation,
            "comparison_modes": scope["comparison_modes"],
            "benchmark": scope["comparison_modes"]["signal_level"]["benchmark"],
            "replay": replay,
            "review_verdict": review_verdict,
            "latest_review_artifact": latest_review_artifact,
            "evidence_freshness": evidence_freshness,
            "detector_review": [],
            "recent_mistakes": [],
            "review_questions": ["Has a fresh default-strategy run been explicitly bootstrapped?", "Are read-only verification surfaces still non-mutating?", "What evidence will exist once a run is active?"],
        }

    review_cutoff = max(now - timedelta(days=settings.strategy_review_lookback_days), scope["launch_at"])
    resolved_default_signals = scope["resolved_trade_signals"]
    avg_clv = _average([signal.clv for signal in resolved_default_signals if signal.clv is not None])
    default_predictions = [(signal.estimated_probability, signal.resolved_correctly) for signal in resolved_default_signals if signal.estimated_probability is not None and signal.resolved_correctly is not None]
    default_brier = brier_score(default_predictions) if default_predictions else None

    recent_mistakes, trade_counts_by_type, trade_pnl_by_type = [], {}, {}
    trade_impact_rows = await session.execute(
        select(
            Signal.signal_type,
            func.count(PaperTrade.id),
            func.sum(PaperTrade.pnl),
        )
        .join(Signal, Signal.id == PaperTrade.signal_id)
        .where(Signal.fired_at >= scope["launch_at"])
        .group_by(Signal.signal_type)
    )
    for signal_type, trade_count, total_pnl in trade_impact_rows.all():
        trade_counts_by_type[signal_type] = int(trade_count or 0)
        trade_pnl_by_type[signal_type] = total_pnl or ZERO
    for trade, signal in sorted(scope["strategy_trade_rows"], key=_trade_resolved_sort_key, reverse=True):
        if len(recent_mistakes) >= settings.strategy_review_recent_mistakes_limit:
            break
        if trade.status != "resolved" or trade.pnl is None or trade.pnl >= ZERO:
            continue
        recent_mistakes.append({"trade_id": str(trade.id), "signal_id": str(signal.id), "signal_type": signal.signal_type, "market_question": (trade.details or {}).get("market_question", ""), "direction": trade.direction, "pnl": float(trade.pnl), "clv": _safe_float(signal.clv), "resolved_at": _ensure_utc(trade.resolved_at).isoformat() if trade.resolved_at else None})

    review_signals = (await session.execute(select(Signal).where(Signal.resolved_correctly.is_not(None), Signal.fired_at >= review_cutoff))).scalars().all()
    detectors: dict[str, list[Signal]] = {}
    for signal in review_signals:
        detectors.setdefault(signal.signal_type, []).append(signal)
    detector_review = []
    for signal_type, signals in detectors.items():
        signals.sort(key=_signal_sort_key)
        clvs = [signal.clv for signal in signals if signal.clv is not None]
        profit_losses = [signal.profit_loss or ZERO for signal in signals]
        predictions = [(signal.estimated_probability, signal.resolved_correctly) for signal in signals if signal.estimated_probability is not None]
        detector_brier = brier_score(predictions) if predictions else None
        total_profit_loss = sum(profit_losses, ZERO)
        verdict, note = _detector_verdict(len(signals), _average(clvs), total_profit_loss, detector_brier)
        detector_review.append({"signal_type": signal_type, "resolved_signals": len(signals), "paper_trades": trade_counts_by_type.get(signal_type, 0), "avg_clv": _safe_float(_average(clvs).quantize(Decimal("0.000001"))) if clvs else None, "total_profit_loss": _safe_float(total_profit_loss.quantize(Decimal("0.000001"))), "paper_trade_pnl": _safe_float(trade_pnl_by_type.get(signal_type, ZERO).quantize(Decimal("0.01"))), "max_drawdown": _safe_float(_compute_max_drawdown(profit_losses).quantize(Decimal("0.000001"))), "brier_score": _safe_float(detector_brier.quantize(Decimal("0.000001"))) if detector_brier is not None else None, "verdict": verdict, "note": note})
    detector_review.sort(key=lambda row: (row["total_profit_loss"] or 0, row["avg_clv"] or 0), reverse=True)

    minimum_days = settings.default_strategy_min_observation_days
    remaining_days = max(0, math.ceil(minimum_days - days_tracked)) if days_tracked is not None and days_tracked < minimum_days else 0
    overdue_open_trades = await get_overdue_open_trade_count(
        session,
        strategy_run_id=strategy_run.id,
    )
    overdue_open_trade_watch = await get_overdue_open_trade_watch(
        session,
        strategy_run_id=strategy_run.id,
    )
    observation = {"started_at": started_at.isoformat() if started_at else None, "baseline_start_at": scope["launch_at"].isoformat() if scope["launch_at"] else None, "first_trade_at": scope["first_trade_at"].isoformat() if scope["first_trade_at"] else None, "days_tracked": days_tracked, "status": _observation_status(days_tracked, launched=started_at is not None, traded_signals=scope["trade_funnel"]["traded_signals"]), "minimum_days": settings.default_strategy_min_observation_days, "preferred_days": settings.default_strategy_preferred_observation_days, "days_until_minimum_window": remaining_days}
    headline = {"open_exposure": float(portfolio["open_exposure"]), "open_trades": len(portfolio["open_trades"]), "resolved_trades": metrics["total_trades"], "resolved_signals": scope["trade_funnel"]["resolved_signals"], "missing_resolutions": scope["trade_funnel"]["unresolved_traded_signals"], "overdue_open_trades": overdue_open_trades, "cumulative_pnl": metrics["cumulative_pnl"], "avg_clv": _safe_float(avg_clv.quantize(Decimal("0.000001"))) if avg_clv is not None else None, "profit_factor": metrics["profit_factor"], "win_rate": metrics["win_rate"], "max_drawdown": float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else None, "drawdown_pct": float(strategy_run.drawdown_pct) if strategy_run.drawdown_pct is not None else None, "current_equity": float(strategy_run.current_equity) if strategy_run.current_equity is not None else None, "peak_equity": float(strategy_run.peak_equity) if strategy_run.peak_equity is not None else None, "brier_score": _safe_float(default_brier.quantize(Decimal("0.000001"))) if default_brier is not None else None}
    run_integrity = {"pre_launch_candidate_signals": scope["trade_funnel"]["pre_launch_candidate_signals"], "excluded_pre_launch_trades": scope["trade_funnel"]["excluded_pre_launch_trades"], "excluded_legacy_trades": scope["trade_funnel"]["excluded_legacy_trades"], "trades_missing_orderbook_context": metrics["trades_missing_orderbook_context"], "integrity_errors": scope["trade_funnel"]["integrity_errors"], "debug_drawdown": {"reconstructed_max_drawdown": metrics["max_drawdown"], "reconstructed_current_equity": round(float(settings.default_bankroll) + metrics["cumulative_pnl"], 2)}}
    resolution_reconciliation = _build_resolution_reconciliation(
        headline=headline,
        trade_funnel=scope["trade_funnel"],
        pending_watch=scope["pending_decision_watch"],
        overdue_open_trade_watch=overdue_open_trade_watch,
        run_integrity=run_integrity,
        evidence_freshness=evidence_freshness,
    )
    review_verdict = build_review_verdict(
        strategy_run=serialized_strategy_run,
        run_state=scope["run_state"],
        observation=observation,
        trade_funnel=scope["trade_funnel"],
        pending_watch=scope["pending_decision_watch"],
        comparison_modes=scope["comparison_modes"],
        replay=replay,
        headline=headline,
    )
    return {
        "strategy": contract,
        "strategy_run": serialized_strategy_run,
        "run_state": scope["run_state"],
        "bootstrap_required": False,
        "observation": observation,
        "trade_funnel": scope["trade_funnel"],
        "pending_decision_watch": scope["pending_decision_watch"],
        "skip_reasons": scope["skip_reasons"],
        "headline": headline,
        "execution_realism": {"shadow_cumulative_pnl": metrics["shadow_cumulative_pnl"], "shadow_profit_factor": metrics["shadow_profit_factor"], "liquidity_constrained_trades": metrics["liquidity_constrained_trades"], "trades_missing_orderbook_context": metrics["trades_missing_orderbook_context"]},
        "risk_blocks": scope["risk_block_summary"],
        "run_integrity": run_integrity,
        "resolution_reconciliation": resolution_reconciliation,
        "comparison_modes": scope["comparison_modes"],
        "benchmark": scope["comparison_modes"]["signal_level"]["benchmark"],
        "replay": replay,
        "review_verdict": review_verdict,
        "latest_review_artifact": latest_review_artifact,
        "evidence_freshness": evidence_freshness,
        "detector_review": detector_review,
        "recent_mistakes": recent_mistakes,
        "review_questions": ["Did the default strategy make money after execution realism and risk controls?", "Does the qualified funnel reconcile exactly into opened, skipped, and pending decisions?", "Are shared/global risk controls contaminating what looks like local paper-book skips?", "How does the signal-level cohort compare with the legacy rank-threshold baseline?", "How much execution-adjusted evidence do we actually have?"],
    }


async def get_strategy_health(session: AsyncSession) -> dict:
    scope = await _get_default_strategy_scope(session)
    return await _serialize_strategy_health(session, scope=scope)


async def get_default_strategy_dashboard(session: AsyncSession) -> dict:
    scope = await _get_default_strategy_scope(session)
    return {
        "portfolio": scope["portfolio"],
        "metrics": scope["metrics"],
        "pnl_curve": scope["pnl_curve"],
        "strategy_health": await _serialize_strategy_health(session, scope=scope),
    }
