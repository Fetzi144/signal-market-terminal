"""Read-only analysis helpers for the default paper-trading strategy."""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from time import monotonic

from sqlalchemy import and_, case, func, or_, select
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
from app.models.snapshot import PriceSnapshot
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
INTEGRITY_ERROR_EXAMPLE_LIMIT = 50
PENDING_DECISION_LOAD_LIMIT = 10000
DECISION_ACTIVITY_LOOKBACK_DAYS = 7
DECISION_ACTIVITY_ROW_LIMIT = 5000
PROFITABILITY_OPERATING_WINDOW_DAYS = 30
PROFITABILITY_MIN_RESOLVED_TRADES = 20
MARK_TO_MARKET_STALE_AFTER = timedelta(hours=24)
EVIDENCE_READ_CACHE_TTL_SECONDS = 60.0
EXPOSURE_SHORT_HORIZON_DAYS = 7
EXPOSURE_OPERATING_WINDOW_DAYS = 30
OPEN_TRADE_DETAIL_LOAD_LIMIT = 10_000
RESOLVED_TRADE_DRAWDOWN_LOAD_LIMIT = 50_000

_EVIDENCE_READ_CACHE: dict[str, tuple[float, dict]] = {}


@dataclass(frozen=True)
class DecisionSummary:
    id: uuid.UUID
    signal_id: uuid.UUID
    decision_at: datetime | None
    decision_status: str
    reason_code: str | None
    details: dict | None = None


def clear_default_strategy_evidence_cache() -> None:
    _EVIDENCE_READ_CACHE.clear()


def _read_cached_payload(cache_key: str, *, use_cache: bool = True) -> dict | None:
    if not use_cache:
        return None
    cached = _EVIDENCE_READ_CACHE.get(cache_key)
    if cached is None:
        return None
    cached_at, payload = cached
    if monotonic() - cached_at > EVIDENCE_READ_CACHE_TTL_SECONDS:
        _EVIDENCE_READ_CACHE.pop(cache_key, None)
        return None
    return deepcopy(payload)


def _write_cached_payload(cache_key: str, payload: dict, *, use_cache: bool = True) -> None:
    if not use_cache:
        return
    _EVIDENCE_READ_CACHE[cache_key] = (monotonic(), deepcopy(payload))


def _session_cache_namespace(session: AsyncSession) -> str:
    try:
        bind = session.get_bind()
        return str(getattr(bind, "url", "unknown"))
    except Exception:
        return "unknown"


def _default_strategy_cache_key_from_fingerprint(
    session: AsyncSession,
    payload_name: str,
    fingerprint: dict,
) -> str:
    namespace = _session_cache_namespace(session)
    digest = hashlib.sha256(
        json.dumps(fingerprint, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"{namespace}:{payload_name}:{fingerprint.get('state', 'unknown')}:{digest}"


def _iso_utc(value: datetime | None) -> str | None:
    value = _ensure_utc(value)
    return value.isoformat() if value else None


def _parse_iso_utc(value) -> datetime | None:
    if value is None:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _max_datetime(*values: datetime | None) -> datetime | None:
    normalized = [_ensure_utc(value) for value in values if value is not None]
    return max(normalized) if normalized else None


def _decimal_fingerprint(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


async def get_default_strategy_activity_fingerprint(session: AsyncSession) -> dict:
    """Cheap identity for whether a materialized default-strategy artifact is current."""
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        return {
            "state": "no_active_run",
            "strategy_name": settings.default_strategy_name,
            "strategy_run_id": None,
        }

    trade_row = (
        await session.execute(
            select(
                func.count(PaperTrade.id),
                func.sum(case((PaperTrade.status == "open", 1), else_=0)),
                func.sum(case((PaperTrade.status == "resolved", 1), else_=0)),
                func.max(PaperTrade.opened_at),
                func.max(PaperTrade.resolved_at),
            ).where(PaperTrade.strategy_run_id == strategy_run.id)
        )
    ).one()
    decision_row = (
        await session.execute(
            select(
                func.count(ExecutionDecision.id),
                func.sum(case((ExecutionDecision.decision_status == "pending_decision", 1), else_=0)),
                func.max(ExecutionDecision.decision_at),
                func.min(
                    case(
                        (ExecutionDecision.decision_status == "pending_decision", ExecutionDecision.decision_at),
                        else_=None,
                    )
                ),
            ).where(ExecutionDecision.strategy_run_id == strategy_run.id)
        )
    ).one()
    trade_count, open_trade_count, resolved_trade_count, latest_opened_at, latest_resolved_at = trade_row
    decision_count, pending_decision_count, latest_decision_at, oldest_pending_decision_at = decision_row

    contract = strategy_run.contract_snapshot if isinstance(strategy_run.contract_snapshot, dict) else {}
    evidence_boundary = contract.get("evidence_boundary") if isinstance(contract.get("evidence_boundary"), dict) else {}
    latest_trade_activity_at = _max_datetime(latest_opened_at, latest_resolved_at)
    return {
        "state": "active_run",
        "strategy_name": strategy_run.strategy_name,
        "strategy_family": strategy_run.strategy_family,
        "strategy_run_id": str(strategy_run.id),
        "strategy_run_status": strategy_run.status,
        "strategy_run_started_at": _iso_utc(strategy_run.started_at),
        "strategy_version_key": contract.get("strategy_version_key"),
        "contract_version": contract.get("contract_version"),
        "evidence_boundary_id": evidence_boundary.get("boundary_id"),
        "evidence_boundary_release_tag": evidence_boundary.get("release_tag"),
        "trade_count": int(trade_count or 0),
        "open_trade_count": int(open_trade_count or 0),
        "resolved_trade_count": int(resolved_trade_count or 0),
        "latest_opened_at": _iso_utc(latest_opened_at),
        "latest_resolved_at": _iso_utc(latest_resolved_at),
        "latest_trade_activity_at": _iso_utc(latest_trade_activity_at),
        "decision_count": int(decision_count or 0),
        "pending_decision_count": int(pending_decision_count or 0),
        "latest_decision_at": _iso_utc(latest_decision_at),
        "oldest_pending_decision_at": _iso_utc(oldest_pending_decision_at),
        "current_equity": _decimal_fingerprint(strategy_run.current_equity),
        "peak_equity": _decimal_fingerprint(strategy_run.peak_equity),
        "max_drawdown": _decimal_fingerprint(strategy_run.max_drawdown),
        "drawdown_pct": _decimal_fingerprint(strategy_run.drawdown_pct),
    }


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


def _parse_activity_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _decision_activity_at(row: DecisionSummary) -> datetime | None:
    details = row.details if isinstance(row.details, dict) else {}
    diagnostics = details.get("diagnostics") if isinstance(details.get("diagnostics"), dict) else {}
    candidates = [
        _ensure_utc(row.decision_at),
        _parse_activity_datetime(details.get("expired_at")),
        _parse_activity_datetime(details.get("finalized_at")),
        _parse_activity_datetime(details.get("evaluated_at")),
        _parse_activity_datetime(diagnostics.get("expired_at")),
        _parse_activity_datetime(diagnostics.get("finalized_at")),
    ]
    return max((value for value in candidates if value is not None), default=None)


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


def _empty_mark_to_market() -> dict:
    return {
        "open_unrealized_pnl": 0.0,
        "mark_to_market_pnl": 0.0,
        "open_positions_marked": 0,
        "open_positions_missing_price": 0,
        "open_positions_stale_price": 0,
        "latest_price_at": None,
        "stale_after_seconds": int(MARK_TO_MARKET_STALE_AFTER.total_seconds()),
    }


def _empty_open_exposure_bucket() -> dict:
    return {
        "trade_count": 0,
        "open_exposure": 0.0,
        "open_mark_to_market_pnl": 0.0,
        "missing_price_count": 0,
        "stale_price_count": 0,
        "examples": [],
    }


def _empty_open_exposure_buckets(*, observed_at: datetime | None = None) -> dict:
    return {
        "observed_at": (_ensure_utc(observed_at) or datetime.now(timezone.utc)).isoformat(),
        "short_horizon_days": EXPOSURE_SHORT_HORIZON_DAYS,
        "operating_window_days": EXPOSURE_OPERATING_WINDOW_DAYS,
        "buckets": {
            "expired_or_due": _empty_open_exposure_bucket(),
            "short_horizon": _empty_open_exposure_bucket(),
            "operating_window": _empty_open_exposure_bucket(),
            "long_dated": _empty_open_exposure_bucket(),
            "unknown_end_date": _empty_open_exposure_bucket(),
        },
        "capital_drag": {
            "trade_count": 0,
            "open_exposure": 0.0,
            "pct_open_exposure": 0.0,
        },
    }


def _money_float(value: Decimal | None) -> float:
    return float((value or ZERO).quantize(Decimal("0.01")))


def _mark_price_for_trade(trade: PaperTrade, raw_price: Decimal) -> Decimal:
    return Decimal("1") - raw_price if trade.direction == "buy_no" else raw_price


def _trade_unrealized_pnl(trade: PaperTrade, raw_price: Decimal) -> Decimal:
    return (_mark_price_for_trade(trade, raw_price) - (trade.entry_price or ZERO)) * (trade.shares or ZERO)


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


def _decision_summary_from_row(row) -> DecisionSummary:
    decision_id, signal_id, decision_at, decision_status, reason_code, details = row
    return DecisionSummary(
        id=decision_id,
        signal_id=signal_id,
        decision_at=decision_at,
        decision_status=decision_status,
        reason_code=reason_code,
        details=details if isinstance(details, dict) else {},
    )


def _append_integrity_example(
    integrity_errors: list[dict],
    *,
    signal_id: uuid.UUID,
    error: str,
    decision_status: str | None = None,
) -> None:
    if len(integrity_errors) >= INTEGRITY_ERROR_EXAMPLE_LIMIT:
        return
    payload = {"signal_id": str(signal_id), "error": error}
    if decision_status is not None:
        payload["decision_status"] = decision_status
    integrity_errors.append(payload)


def _pending_decision_watch(
    decision_rows: list[ExecutionDecision],
    *,
    now: datetime,
    example_limit: int = PENDING_DECISION_EXAMPLE_LIMIT,
) -> dict:
    pending_rows = [row for row in decision_rows if row.decision_status == "pending_decision"]
    retry_window_seconds = float(settings.paper_trading_pending_decision_max_age_seconds)
    if not pending_rows:
        return {
            "count": 0,
            "oldest_decision_at": None,
            "max_age_seconds": 0.0,
            "avg_age_seconds": 0.0,
            "stale_count": 0,
            "retry_window_seconds": retry_window_seconds,
            "reason_counts": {},
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
    reason_counts: dict[str, int] = {}
    stale_count = 0
    for row, age in zip(pending_rows, ages, strict=False):
        reason_code = row.reason_code or "unknown"
        reason_counts[reason_code] = reason_counts.get(reason_code, 0) + 1
        if age.total_seconds() > retry_window_seconds:
            stale_count += 1
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
        "stale_count": stale_count,
        "retry_window_seconds": retry_window_seconds,
        "reason_counts": reason_counts,
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
        "coverage_scope": replay.get("coverage_scope", "global_observed_detectors"),
        "global_coverage_mode": replay.get("global_coverage_mode"),
        "global_supported_detectors": list(replay.get("global_supported_detectors") or []),
        "global_unsupported_detectors": list(replay.get("global_unsupported_detectors") or []),
        "review_observed_detectors": list(replay.get("review_observed_detectors") or []),
        "configured_supported_detectors": list(replay.get("configured_supported_detectors") or []),
        "supported_detectors": list(replay.get("supported_detectors") or []),
        "unsupported_detectors": list(replay.get("unsupported_detectors") or []),
        "recent_coverage_limited_run_count_24h": int(replay.get("recent_coverage_limited_run_count_24h") or 0),
    }


def _detector_coverage_mode(*, observed_detectors: list[str], supported_detector_set: set[str]) -> str:
    supported = [detector for detector in observed_detectors if detector in supported_detector_set]
    unsupported = [detector for detector in observed_detectors if detector not in supported_detector_set]
    if not observed_detectors:
        return "no_detector_activity"
    if supported and unsupported:
        return "partial_supported_detectors"
    if supported:
        return "supported_detectors_only"
    return "unsupported_detectors_only"


def _scope_replay_status_for_default_strategy(
    replay_status: dict,
    *,
    observed_detector_types: list[str],
) -> dict:
    supported_detector_set = set(replay_status.get("configured_supported_detectors") or [])
    observed_detectors = sorted({str(detector) for detector in observed_detector_types if detector})
    supported_detectors = [detector for detector in observed_detectors if detector in supported_detector_set]
    unsupported_detectors = [detector for detector in observed_detectors if detector not in supported_detector_set]
    scoped = dict(replay_status)
    scoped.update(
        {
            "coverage_scope": "default_strategy_review",
            "global_coverage_mode": replay_status.get("coverage_mode", "no_detector_activity"),
            "global_supported_detectors": list(replay_status.get("supported_detectors") or []),
            "global_unsupported_detectors": list(replay_status.get("unsupported_detectors") or []),
            "review_observed_detectors": observed_detectors,
            "coverage_mode": _detector_coverage_mode(
                observed_detectors=observed_detectors,
                supported_detector_set=supported_detector_set,
            ),
            "supported_detectors": supported_detectors,
            "unsupported_detectors": unsupported_detectors,
        }
    )
    return scoped


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


async def _load_pending_qualified_decision_rows(
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
            ExecutionDecision.decision_status == "pending_decision",
            *_default_strategy_signal_filters(
                launch_at=launch_at,
                qualified_only=True,
            ),
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(PENDING_DECISION_LOAD_LIMIT)
    )
    return [_decision_summary_from_row(row) for row in result.all()]


async def _latest_qualified_decision_activity_at(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    launch_at: datetime,
    now: datetime,
) -> datetime | None:
    latest_decision_at = (
        await session.execute(
            select(func.max(ExecutionDecision.decision_at))
            .join(Signal, Signal.id == ExecutionDecision.signal_id)
            .where(
                ExecutionDecision.strategy_run_id == strategy_run_id,
                *_default_strategy_signal_filters(
                    launch_at=launch_at,
                    qualified_only=True,
                ),
            )
        )
    ).scalar_one_or_none()
    latest_activity = _ensure_utc(latest_decision_at)

    recent_activity_result = await session.execute(
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
            ExecutionDecision.decision_at >= now - timedelta(days=DECISION_ACTIVITY_LOOKBACK_DAYS),
            *_default_strategy_signal_filters(
                launch_at=launch_at,
                qualified_only=True,
            ),
        )
        .order_by(ExecutionDecision.decision_at.desc(), ExecutionDecision.id.desc())
        .limit(DECISION_ACTIVITY_ROW_LIMIT)
    )
    for row in recent_activity_result.all():
        activity_at = _decision_activity_at(_decision_summary_from_row(row))
        if activity_at is not None and (latest_activity is None or activity_at > latest_activity):
            latest_activity = activity_at
    return latest_activity


async def _latest_price_rows_by_outcome(
    session: AsyncSession,
    outcome_ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[Decimal, datetime | None]]:
    unique_outcome_ids = list(dict.fromkeys(outcome_ids))
    if not unique_outcome_ids:
        return {}

    latest_price_subquery = (
        select(
            PriceSnapshot.outcome_id.label("outcome_id"),
            func.max(PriceSnapshot.captured_at).label("captured_at"),
        )
        .where(PriceSnapshot.outcome_id.in_(unique_outcome_ids))
        .group_by(PriceSnapshot.outcome_id)
        .subquery()
    )
    rows = await session.execute(
        select(
            PriceSnapshot.outcome_id,
            PriceSnapshot.price,
            PriceSnapshot.captured_at,
        ).join(
            latest_price_subquery,
            and_(
                latest_price_subquery.c.outcome_id == PriceSnapshot.outcome_id,
                latest_price_subquery.c.captured_at == PriceSnapshot.captured_at,
            ),
        )
    )
    latest: dict[uuid.UUID, tuple[Decimal, datetime | None]] = {}
    for outcome_id, price, captured_at in rows.all():
        latest[outcome_id] = (Decimal(str(price)), _ensure_utc(captured_at))
    return latest


async def _open_trade_mark_to_market(
    session: AsyncSession,
    *,
    open_trades: list[PaperTrade],
    realized_pnl: Decimal,
    now: datetime,
) -> dict:
    if not open_trades:
        return {
            **_empty_mark_to_market(),
            "mark_to_market_pnl": _money_float(realized_pnl),
        }

    open_unrealized = ZERO
    marked = 0
    missing_price = 0
    stale_price = 0
    latest_price_at: datetime | None = None
    latest_prices = await _latest_price_rows_by_outcome(
        session,
        [trade.outcome_id for trade in open_trades],
    )
    for trade in open_trades:
        row = latest_prices.get(trade.outcome_id)
        if row is None:
            missing_price += 1
            continue
        raw_price, captured_at = row
        if captured_at is not None and now - captured_at > MARK_TO_MARKET_STALE_AFTER:
            stale_price += 1
        if captured_at is not None and (latest_price_at is None or captured_at > latest_price_at):
            latest_price_at = captured_at
        open_unrealized += _trade_unrealized_pnl(trade, raw_price)
        marked += 1

    mark_to_market_pnl = realized_pnl + open_unrealized
    return {
        "open_unrealized_pnl": _money_float(open_unrealized),
        "mark_to_market_pnl": _money_float(mark_to_market_pnl),
        "open_positions_marked": marked,
        "open_positions_missing_price": missing_price,
        "open_positions_stale_price": stale_price,
        "latest_price_at": latest_price_at.isoformat() if latest_price_at else None,
        "stale_after_seconds": int(MARK_TO_MARKET_STALE_AFTER.total_seconds()),
    }


def _exposure_bucket_for_market(market: Market | None, *, now: datetime) -> tuple[str, float | None]:
    if market is None:
        return "unknown_end_date", None
    end_date = _ensure_utc(market.end_date)
    if end_date is None:
        return "unknown_end_date", None
    days_to_end = round((end_date - now).total_seconds() / 86400, 2)
    if not bool(market.active) or end_date <= now:
        return "expired_or_due", days_to_end
    if days_to_end <= EXPOSURE_SHORT_HORIZON_DAYS:
        return "short_horizon", days_to_end
    if days_to_end <= EXPOSURE_OPERATING_WINDOW_DAYS:
        return "operating_window", days_to_end
    return "long_dated", days_to_end


async def _open_exposure_resolution_buckets(
    session: AsyncSession,
    *,
    open_trades: list[PaperTrade],
    now: datetime,
) -> dict:
    summary = _empty_open_exposure_buckets(observed_at=now)
    if not open_trades:
        return summary

    market_ids = list({trade.market_id for trade in open_trades})
    market_rows = await session.execute(select(Market).where(Market.id.in_(market_ids)))
    markets_by_id = {market.id: market for market in market_rows.scalars().all()}
    latest_prices = await _latest_price_rows_by_outcome(
        session,
        [trade.outcome_id for trade in open_trades],
    )

    total_exposure = ZERO
    for trade in open_trades:
        bucket_key, days_to_end = _exposure_bucket_for_market(markets_by_id.get(trade.market_id), now=now)
        bucket = summary["buckets"][bucket_key]
        exposure = trade.size_usd or ZERO
        total_exposure += exposure
        bucket["trade_count"] += 1
        bucket["open_exposure"] += float(exposure)

        price_row = latest_prices.get(trade.outcome_id)
        unrealized_pnl: Decimal | None = None
        latest_price_at: datetime | None = None
        if price_row is None:
            bucket["missing_price_count"] += 1
        else:
            raw_price, latest_price_at = price_row
            unrealized_pnl = _trade_unrealized_pnl(trade, raw_price)
            bucket["open_mark_to_market_pnl"] += _money_float(unrealized_pnl)
            if latest_price_at is not None and now - latest_price_at > MARK_TO_MARKET_STALE_AFTER:
                bucket["stale_price_count"] += 1

        market = markets_by_id.get(trade.market_id)
        bucket["examples"].append(
            {
                "trade_id": str(trade.id),
                "signal_id": str(trade.signal_id),
                "platform": market.platform if market is not None else None,
                "platform_id": market.platform_id if market is not None else None,
                "market_question": market.question if market is not None else None,
                "market_end_date": _ensure_utc(market.end_date).isoformat() if market is not None and market.end_date else None,
                "days_to_end": days_to_end,
                "open_exposure": _money_float(exposure),
                "open_mark_to_market_pnl": _money_float(unrealized_pnl) if unrealized_pnl is not None else None,
                "latest_price_at": latest_price_at.isoformat() if latest_price_at else None,
            }
        )

    for bucket in summary["buckets"].values():
        bucket["open_exposure"] = _money_float(Decimal(str(bucket["open_exposure"])))
        bucket["open_mark_to_market_pnl"] = _money_float(Decimal(str(bucket["open_mark_to_market_pnl"])))
        bucket["examples"] = sorted(
            bucket["examples"],
            key=lambda row: (
                row.get("days_to_end") is None,
                row.get("days_to_end") if row.get("days_to_end") is not None else 10**9,
                row.get("trade_id") or "",
            ),
        )[:5]

    drag_trade_count = (
        summary["buckets"]["long_dated"]["trade_count"]
        + summary["buckets"]["unknown_end_date"]["trade_count"]
    )
    drag_exposure = Decimal(str(summary["buckets"]["long_dated"]["open_exposure"])) + Decimal(
        str(summary["buckets"]["unknown_end_date"]["open_exposure"])
    )
    summary["capital_drag"] = {
        "trade_count": drag_trade_count,
        "open_exposure": _money_float(drag_exposure),
        "pct_open_exposure": round(float(drag_exposure / total_exposure), 4) if total_exposure > ZERO else 0.0,
    }
    return summary


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
            "stale_count": 0,
            "retry_window_seconds": float(settings.paper_trading_pending_decision_max_age_seconds),
            "reason_counts": {},
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
                "integrity_error_count": 0,
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
            "mark_to_market": _empty_mark_to_market(),
            "open_exposure_buckets": _empty_open_exposure_buckets(observed_at=now),
        }

    launch_at = _ensure_utc(strategy_run.started_at)
    candidate_signal_count = await _count_default_strategy_signals(session, launch_at=launch_at)
    replay_status = _scope_replay_status_for_default_strategy(
        replay_status,
        observed_detector_types=([settings.default_strategy_signal_type] if settings.default_strategy_signal_type and candidate_signal_count > 0 else []),
    )
    pre_launch_candidate_signals = await _count_default_strategy_signals(
        session,
        launch_at=launch_at,
        before_launch=True,
    )
    qualified_signal_count = await _count_default_strategy_signals(
        session,
        launch_at=launch_at,
        qualified_only=True,
    )
    strategy_trade_rows = await _load_strategy_trade_rows(session, strategy_run_id=strategy_run.id)
    strategy_trade_rows.sort(key=_trade_opened_sort_key, reverse=True)
    pending_decision_rows = await _load_pending_qualified_decision_rows(
        session,
        strategy_run_id=strategy_run.id,
        launch_at=launch_at,
    )
    pending_watch = _pending_decision_watch(pending_decision_rows, now=now)
    _publish_pending_decision_metrics(pending_watch)

    qualified_filters = _default_strategy_signal_filters(
        launch_at=launch_at,
        qualified_only=True,
    )
    trade_exists = (
        select(PaperTrade.id)
        .where(
            PaperTrade.strategy_run_id == strategy_run.id,
            PaperTrade.signal_id == ExecutionDecision.signal_id,
        )
        .exists()
    )
    decision_exists = (
        select(ExecutionDecision.id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.signal_id == Signal.id,
        )
        .exists()
    )
    opened_trade_signals = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "opened",
                    trade_exists,
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    status_counts = {
        status: int(count or 0)
        for status, count in (
            await session.execute(
                select(
                    ExecutionDecision.decision_status,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists,
                    ExecutionDecision.decision_status.in_(["skipped", "pending_decision"]),
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.decision_status)
            )
        ).all()
    }
    skipped_signals = status_counts.get("skipped", 0)
    pending_decision_signals = status_counts.get("pending_decision", 0)

    integrity_errors: list[dict] = []
    reason_code_expr = func.coalesce(ExecutionDecision.reason_code, "unclassified")
    skip_reason_counts: dict[str, dict] = {
        str(reason_code): {
            "reason_code": str(reason_code),
            "reason_label": default_strategy_skip_label(str(reason_code)) or "Unclassified",
            "count": int(count or 0),
        }
        for reason_code, count in (
            await session.execute(
                select(
                    reason_code_expr,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists,
                    *qualified_filters,
                )
                .group_by(reason_code_expr)
            )
        ).all()
    }
    local_reason_counts: dict[str, int] = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists,
                    ExecutionDecision.reason_code.like("risk_%"),
                    ExecutionDecision.reason_code != "risk_shared_global_block",
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }
    shared_reason_counts: dict[str, int] = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists,
                    ExecutionDecision.reason_code == "risk_shared_global_block",
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }
    shared_upstream_reason_counts: dict[str, int] = {}
    execution_liquidity_reason_counts: dict[str, int] = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists,
                    ExecutionDecision.reason_code.like("execution_%"),
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }
    shared_global_examples: list[dict] = []
    shared_examples_result = await session.execute(
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
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.decision_status == "skipped",
            ~trade_exists,
            ExecutionDecision.reason_code == "risk_shared_global_block",
            *qualified_filters,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
    )
    for row in shared_examples_result.all():
        decision = _decision_summary_from_row(row)
        reason_code, reason_label = _skip_reason_row(decision)
        risk_result = _risk_result_for_decision(decision)
        risk_scope = risk_result.get("risk_scope")
        upstream_reason = str(risk_result.get("original_reason_code") or reason_code)
        shared_upstream_reason_counts[upstream_reason] = shared_upstream_reason_counts.get(upstream_reason, 0) + 1
        if risk_scope == "local_paper_book":
            local_reason_counts[reason_code] = local_reason_counts.get(reason_code, 0) + 1
        if len(shared_global_examples) < 3:
            shared_global_examples.append(
                {
                    "signal_id": str(decision.signal_id),
                    "decision_id": str(decision.id),
                    "reason_code": reason_code,
                    "reason_label": reason_label,
                    "upstream_reason_code": upstream_reason,
                    "detail": (decision.details or {}).get("detail"),
                }
            )

    decision_signal_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    missing_decision_count = max(0, qualified_signal_count - decision_signal_count)
    if missing_decision_count:
        missing_result = await session.execute(
            select(Signal.id)
            .where(
                *qualified_filters,
                ~decision_exists,
            )
            .order_by(Signal.fired_at.asc(), Signal.id.asc())
            .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
        )
        for signal_id in missing_result.scalars().all():
            _append_integrity_example(
                integrity_errors,
                signal_id=signal_id,
                error="missing_execution_decision",
            )

    status_mismatch_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    trade_exists,
                    ExecutionDecision.decision_status != "opened",
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    status_mismatch_result = await session.execute(
        select(ExecutionDecision.signal_id, ExecutionDecision.decision_status)
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            trade_exists,
            ExecutionDecision.decision_status != "opened",
            *qualified_filters,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
    )
    status_mismatch_rows = status_mismatch_result.all()
    for signal_id, decision_status in status_mismatch_rows:
        _append_integrity_example(
            integrity_errors,
            signal_id=signal_id,
            error="trade_decision_status_mismatch",
            decision_status=decision_status,
        )

    opened_without_trade_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists,
                    ExecutionDecision.decision_status == "opened",
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    opened_without_trade_result = await session.execute(
        select(ExecutionDecision.signal_id)
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ~trade_exists,
            ExecutionDecision.decision_status == "opened",
            *qualified_filters,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
    )
    opened_without_trade_rows = opened_without_trade_result.scalars().all()
    for signal_id in opened_without_trade_rows:
        _append_integrity_example(
            integrity_errors,
            signal_id=signal_id,
            error="opened_without_trade",
        )

    unrecognized_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists,
                    ExecutionDecision.decision_status.not_in(["opened", "skipped", "pending_decision"]),
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    unrecognized_result = await session.execute(
        select(ExecutionDecision.signal_id, ExecutionDecision.decision_status)
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ~trade_exists,
            ExecutionDecision.decision_status.not_in(["opened", "skipped", "pending_decision"]),
            *qualified_filters,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
    )
    unrecognized_rows = unrecognized_result.all()
    for signal_id, decision_status in unrecognized_rows:
        _append_integrity_example(
            integrity_errors,
            signal_id=signal_id,
            error="unrecognized_decision_status",
            decision_status=decision_status,
        )

    integrity_error_count = (
        missing_decision_count
        + status_mismatch_count
        + opened_without_trade_count
        + unrecognized_count
    )

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
    latest_decision_at = await _latest_qualified_decision_activity_at(
        session,
        strategy_run_id=strategy_run.id,
        launch_at=launch_at,
        now=now,
    )
    portfolio = await _get_trade_portfolio_state(session, strategy_run_id=strategy_run.id)
    metrics = await _get_trade_metrics(session, strategy_run_id=strategy_run.id)
    pnl_curve = await _get_trade_pnl_curve(session, strategy_run_id=strategy_run.id)
    mark_to_market = await _open_trade_mark_to_market(
        session,
        open_trades=list(portfolio["open_trades"]),
        realized_pnl=Decimal(str(metrics["cumulative_pnl"])),
        now=now,
    )
    open_exposure_buckets = await _open_exposure_resolution_buckets(
        session,
        open_trades=list(portfolio["open_trades"]),
        now=now,
    )
    comparison_modes = await compare_strategy_measurement_modes(
        session,
        start_date=strategy_run.started_at,
        end_date=now,
        strategy_run_id=strategy_run.id,
    )
    excluded_pre_launch_trades = await _count_excluded_pre_launch_trades(session, launch_at=launch_at)
    excluded_legacy_trades = await _count_excluded_legacy_trades(session, strategy_run_id=strategy_run.id)
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
        "integrity_error_count": integrity_error_count,
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
        "mark_to_market": mark_to_market,
        "open_exposure_buckets": open_exposure_buckets,
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


def _strategy_version_key(serialized_strategy_run: dict | None, contract: dict | None) -> str:
    contract = contract if isinstance(contract, dict) else {}
    serialized_strategy_run = serialized_strategy_run if isinstance(serialized_strategy_run, dict) else {}
    return (
        serialized_strategy_run.get("strategy_version_key")
        or contract.get("strategy_version_key")
        or contract.get("contract_version")
        or "default_strategy_benchmark_v1"
    )


def _execution_adjusted_pnl(comparison_modes: dict, headline: dict) -> float:
    execution_adjusted_default = ((comparison_modes.get("execution_adjusted") or {}).get("default_strategy") or {})
    return float(execution_adjusted_default.get("cumulative_pnl", headline.get("cumulative_pnl") or 0.0) or 0.0)


def _replay_net_pnl(comparison_modes: dict) -> float | None:
    replay_adjusted_default = ((comparison_modes.get("replay_adjusted") or {}).get("default_strategy") or {})
    value = replay_adjusted_default.get("net_pnl", replay_adjusted_default.get("cumulative_pnl"))
    return None if value is None else float(value)


def _profitability_verdict(
    *,
    observation: dict,
    headline: dict,
    comparison_modes: dict,
    replay: dict,
    review_verdict: dict,
    trade_funnel: dict,
    mark_to_market: dict,
    evidence_freshness: dict | None = None,
) -> tuple[str, list[str]]:
    blockers = [str(row.get("code")) for row in review_verdict.get("blockers", []) if row.get("code")]
    days_tracked = observation.get("days_tracked")
    resolved_trades = int(headline.get("resolved_trades") or 0)
    avg_clv = headline.get("avg_clv")
    execution_pnl = _execution_adjusted_pnl(comparison_modes, headline)
    replay_pnl = _replay_net_pnl(comparison_modes)
    coverage_mode = str(replay.get("coverage_mode") or "no_detector_activity")

    if days_tracked is None or float(days_tracked) < PROFITABILITY_OPERATING_WINDOW_DAYS:
        blockers.append("insufficient_operating_window")
    if resolved_trades < PROFITABILITY_MIN_RESOLVED_TRADES:
        blockers.append("insufficient_resolved_trades")
    if int(trade_funnel.get("integrity_error_count") or len(trade_funnel.get("integrity_errors") or [])) > 0:
        blockers.append("integrity_errors")
    if int(mark_to_market.get("open_positions_missing_price") or 0) > 0:
        blockers.append("missing_mark_to_market_price")
    if int(mark_to_market.get("open_positions_stale_price") or 0) > 0:
        blockers.append("stale_mark_to_market_price")
    if coverage_mode in {"partial_supported_detectors", "unsupported_detectors_only"}:
        blockers.append("replay_coverage_limited")
    if coverage_mode == "supported_detectors_only" and replay_pnl is not None and replay_pnl <= 0:
        blockers.append("non_positive_replay_evidence")
    evidence_status = (evidence_freshness or {}).get("status")
    if evidence_status in {"stale", "missing_review"}:
        blockers.append(f"evidence_{evidence_status}")

    drawdown_pct = headline.get("drawdown_pct")
    if drawdown_pct is not None and float(drawdown_pct) >= 0.05 and resolved_trades < PROFITABILITY_MIN_RESOLVED_TRADES:
        return "paused", sorted(set(blockers + ["paper_drawdown_pause"]))
    if resolved_trades >= PROFITABILITY_MIN_RESOLVED_TRADES and execution_pnl < 0 and avg_clv is not None and float(avg_clv) < 0:
        return "paused", sorted(set(blockers + ["negative_pnl_and_clv"]))
    if blockers:
        return "insufficient_sample", sorted(set(blockers))
    if execution_pnl > 0 and avg_clv is not None and float(avg_clv) > 0:
        return "profitable", []
    return "watch", []


def _build_profitability_snapshot(
    *,
    family: str,
    strategy_version: str,
    observation: dict,
    headline: dict,
    comparison_modes: dict,
    replay: dict,
    review_verdict: dict,
    trade_funnel: dict,
    risk_blocks: dict,
    mark_to_market: dict,
    open_exposure_buckets: dict | None = None,
    evidence_freshness: dict | None = None,
    observed_at: datetime,
) -> dict:
    verdict, blockers = _profitability_verdict(
        observation=observation,
        headline=headline,
        comparison_modes=comparison_modes,
        replay=replay,
        review_verdict=review_verdict,
        trade_funnel=trade_funnel,
        mark_to_market=mark_to_market,
        evidence_freshness=evidence_freshness,
    )
    return {
        "family": family,
        "strategy_version": strategy_version,
        "window_start": observation.get("started_at") or observation.get("baseline_start_at"),
        "window_end": observed_at.isoformat(),
        "operating_window_days": PROFITABILITY_OPERATING_WINDOW_DAYS,
        "minimum_resolved_trades": PROFITABILITY_MIN_RESOLVED_TRADES,
        "realized_pnl": float(headline.get("cumulative_pnl") or 0.0),
        "mark_to_market_pnl": float(mark_to_market.get("mark_to_market_pnl") or 0.0),
        "open_mark_to_market_pnl": float(mark_to_market.get("open_unrealized_pnl") or 0.0),
        "open_exposure": float(headline.get("open_exposure") or 0.0),
        "open_trades": int(headline.get("open_trades") or 0),
        "resolved_trades": int(headline.get("resolved_trades") or 0),
        "avg_clv": headline.get("avg_clv"),
        "execution_adjusted_paper_pnl": _execution_adjusted_pnl(comparison_modes, headline),
        "replay_net_pnl": _replay_net_pnl(comparison_modes),
        "replay_coverage_mode": replay.get("coverage_mode") or "no_detector_activity",
        "skip_funnel": {
            "candidate_signals": int(trade_funnel.get("candidate_signals") or 0),
            "qualified_signals": int(trade_funnel.get("qualified_signals") or 0),
            "opened_trade_signals": int(trade_funnel.get("opened_trade_signals") or 0),
            "skipped_signals": int(trade_funnel.get("skipped_signals") or 0),
            "pending_decision_signals": int(trade_funnel.get("pending_decision_signals") or 0),
            "integrity_error_count": int(trade_funnel.get("integrity_error_count") or len(trade_funnel.get("integrity_errors") or [])),
            "conservation_holds": bool(trade_funnel.get("conservation_holds", False)),
        },
        "risk_blocks": risk_blocks,
        "mark_to_market": mark_to_market,
        "open_exposure_buckets": open_exposure_buckets or _empty_open_exposure_buckets(observed_at=observed_at),
        "evidence_freshness": evidence_freshness or {},
        "risk_blocks_total": int(risk_blocks.get("local_paper_book_blocks") or 0)
        + int(risk_blocks.get("shared_global_blocks") or 0)
        + int(risk_blocks.get("execution_liquidity_blocks") or 0),
        "evidence_blockers": list(review_verdict.get("blockers") or []),
        "profitability_blockers": blockers,
        "verdict": verdict,
        "paper_only": True,
        "live_submission_permitted": False,
    }


def _activity_fingerprints_match(artifact_fingerprint: dict | None, current_fingerprint: dict) -> bool:
    if not isinstance(artifact_fingerprint, dict):
        return False
    return artifact_fingerprint == current_fingerprint


def _activity_fingerprint_identity_matches(artifact_fingerprint: dict | None, current_fingerprint: dict) -> bool:
    if not isinstance(artifact_fingerprint, dict):
        return False
    identity_keys = (
        "state",
        "strategy_name",
        "strategy_run_id",
        "strategy_run_status",
        "strategy_run_started_at",
        "contract_version",
        "evidence_boundary_id",
        "evidence_boundary_release_tag",
    )
    return all(artifact_fingerprint.get(key) == current_fingerprint.get(key) for key in identity_keys)


def _materialized_pending_watch(base_watch: dict | None, fingerprint: dict, *, now: datetime) -> dict:
    pending_watch = deepcopy(base_watch) if isinstance(base_watch, dict) else {}
    pending_count = int(fingerprint.get("pending_decision_count") or 0)
    oldest_pending_at = _parse_iso_utc(fingerprint.get("oldest_pending_decision_at"))
    if pending_count <= 0 or oldest_pending_at is None:
        pending_watch.update(
            {
                "count": 0,
                "oldest_decision_at": None,
                "max_age_seconds": 0.0,
                "avg_age_seconds": 0.0,
                "stale_count": 0,
                "retry_window_seconds": float(settings.paper_trading_pending_decision_max_age_seconds),
            }
        )
        return pending_watch

    max_age_seconds = round(max(0.0, (now - oldest_pending_at).total_seconds()), 1)
    retry_window_seconds = float(settings.paper_trading_pending_decision_max_age_seconds)
    pending_watch.update(
        {
            "count": pending_count,
            "oldest_decision_at": oldest_pending_at.isoformat(),
            "max_age_seconds": max_age_seconds,
            "avg_age_seconds": max(float(pending_watch.get("avg_age_seconds") or 0.0), max_age_seconds),
            "stale_count": pending_count if max_age_seconds > retry_window_seconds else 0,
            "retry_window_seconds": retry_window_seconds,
        }
    )
    return pending_watch


def _hydrate_materialized_strategy_health(
    *,
    artifact_payload: dict,
    latest_review_artifact: dict,
    current_fingerprint: dict,
    observed_at: datetime,
) -> dict | None:
    if artifact_payload.get("artifact_fingerprint_status") != "current":
        return None
    artifact_fingerprint = artifact_payload.get("activity_fingerprint")
    fingerprint_exact_match = _activity_fingerprints_match(artifact_fingerprint, current_fingerprint)
    if not fingerprint_exact_match and not _activity_fingerprint_identity_matches(
        artifact_fingerprint,
        current_fingerprint,
    ):
        return None

    artifact_health = artifact_payload.get("strategy_health")
    if not isinstance(artifact_health, dict):
        return None

    health = deepcopy(artifact_health)
    serialized_strategy_run = health.get("strategy_run") if isinstance(health.get("strategy_run"), dict) else None
    observation = health.get("observation") if isinstance(health.get("observation"), dict) else {}
    headline = health.get("headline") if isinstance(health.get("headline"), dict) else {}
    comparison_modes = health.get("comparison_modes") if isinstance(health.get("comparison_modes"), dict) else {}
    replay = health.get("replay") if isinstance(health.get("replay"), dict) else {}
    trade_funnel = health.get("trade_funnel") if isinstance(health.get("trade_funnel"), dict) else {}
    existing_snapshot = health.get("profitability_snapshot") if isinstance(health.get("profitability_snapshot"), dict) else {}
    risk_blocks = health.get("risk_blocks") if isinstance(health.get("risk_blocks"), dict) else {}
    if not risk_blocks and isinstance(existing_snapshot.get("risk_blocks"), dict):
        risk_blocks = existing_snapshot["risk_blocks"]
    mark_to_market = existing_snapshot.get("mark_to_market") if isinstance(existing_snapshot.get("mark_to_market"), dict) else _empty_mark_to_market()
    open_exposure_buckets = health.get("open_exposure_buckets")
    if not isinstance(open_exposure_buckets, dict):
        open_exposure_buckets = existing_snapshot.get("open_exposure_buckets")
    if not isinstance(open_exposure_buckets, dict):
        open_exposure_buckets = _empty_open_exposure_buckets(observed_at=observed_at)

    pending_watch = _materialized_pending_watch(
        health.get("pending_decision_watch"),
        current_fingerprint,
        now=observed_at,
    )
    trade_funnel["pending_decision_signals"] = pending_watch["count"]
    started_at = _parse_iso_utc(observation.get("started_at")) or _parse_iso_utc(
        (serialized_strategy_run or {}).get("started_at")
    )
    evidence_freshness = build_evidence_freshness(
        observed_at=observed_at,
        run_state=health.get("run_state") or current_fingerprint.get("state") or "active_run",
        latest_review_artifact=latest_review_artifact,
        active_strategy_run=serialized_strategy_run,
        started_at=started_at,
        latest_trade_activity_at=_parse_iso_utc(current_fingerprint.get("latest_trade_activity_at")),
        latest_decision_at=_parse_iso_utc(current_fingerprint.get("latest_decision_at")),
        pending_watch=pending_watch,
    )
    _publish_evidence_freshness_metrics(evidence_freshness)
    review_verdict = build_review_verdict(
        strategy_run=serialized_strategy_run,
        run_state=health.get("run_state") or current_fingerprint.get("state") or "active_run",
        observation=observation,
        trade_funnel=trade_funnel,
        pending_watch=pending_watch,
        comparison_modes=comparison_modes,
        replay=replay,
        headline=headline,
    )
    contract = (serialized_strategy_run or {}).get("contract_snapshot")
    if not isinstance(contract, dict):
        contract = health.get("strategy") if isinstance(health.get("strategy"), dict) else {}

    profitability_snapshot = _build_profitability_snapshot(
        family="default_strategy",
        strategy_version=_strategy_version_key(serialized_strategy_run, contract),
        observation=observation,
        headline=headline,
        comparison_modes=comparison_modes,
        replay=replay,
        review_verdict=review_verdict,
        trade_funnel=trade_funnel,
        risk_blocks=risk_blocks,
        mark_to_market=mark_to_market,
        open_exposure_buckets=open_exposure_buckets,
        evidence_freshness=evidence_freshness,
        observed_at=observed_at,
    )

    resolution_reconciliation = health.get("resolution_reconciliation")
    if isinstance(resolution_reconciliation, dict):
        resolution_reconciliation["pending_decisions"] = pending_watch["count"]
        resolution_reconciliation["pending_decision_max_age_seconds"] = pending_watch["max_age_seconds"]
        resolution_reconciliation["evidence_freshness_status"] = evidence_freshness["status"]

    health.update(
        {
            "trade_funnel": trade_funnel,
            "pending_decision_watch": pending_watch,
            "review_verdict": review_verdict,
            "profitability_snapshot": profitability_snapshot,
            "open_exposure_buckets": open_exposure_buckets,
            "latest_review_artifact": latest_review_artifact,
            "evidence_freshness": evidence_freshness,
            "activity_fingerprint": deepcopy(current_fingerprint),
            "materialized_evidence": {
                "source": "default_strategy_review_artifact",
                "generated_at": latest_review_artifact.get("generated_at"),
                "activity_fingerprint_status": "match" if fingerprint_exact_match else "stale_activity",
            },
        }
    )
    return health


def _load_materialized_default_strategy_health(current_fingerprint: dict) -> dict | None:
    from app.reports.strategy_review import (
        get_latest_default_strategy_review_artifact_metadata,
        get_latest_default_strategy_review_artifact_payload,
    )

    latest_review_artifact = get_latest_default_strategy_review_artifact_metadata()
    if latest_review_artifact.get("generation_status") != "complete":
        return None
    artifact = get_latest_default_strategy_review_artifact_payload()
    if artifact.get("generation_status") != "complete":
        return None
    payload = artifact.get("payload")
    if not isinstance(payload, dict):
        return None
    return _hydrate_materialized_strategy_health(
        artifact_payload=payload,
        latest_review_artifact=latest_review_artifact,
        current_fingerprint=current_fingerprint,
        observed_at=datetime.now(timezone.utc),
    )


def build_paper_lane_profitability_snapshot(
    *,
    family: str,
    strategy_version: str | None,
    observed_at: datetime | None = None,
    disabled_reason: str | None = None,
) -> dict:
    observed_at = observed_at or datetime.now(timezone.utc)
    blockers = ["paper_lane_not_populated"]
    verdict = "not_ready"
    if disabled_reason:
        blockers.append("lane_disabled")
        verdict = "disabled"
    return {
        "family": family,
        "strategy_version": strategy_version or f"{family}_candidate_v1",
        "window_start": None,
        "window_end": observed_at.isoformat(),
        "operating_window_days": PROFITABILITY_OPERATING_WINDOW_DAYS,
        "minimum_resolved_trades": PROFITABILITY_MIN_RESOLVED_TRADES,
        "realized_pnl": 0.0,
        "mark_to_market_pnl": 0.0,
        "open_mark_to_market_pnl": 0.0,
        "open_exposure": 0.0,
        "open_trades": 0,
        "resolved_trades": 0,
        "avg_clv": None,
        "execution_adjusted_paper_pnl": 0.0,
        "replay_net_pnl": None,
        "replay_coverage_mode": "not_populated",
        "skip_funnel": {
            "candidate_signals": 0,
            "qualified_signals": 0,
            "opened_trade_signals": 0,
            "skipped_signals": 0,
            "pending_decision_signals": 0,
            "integrity_error_count": 0,
            "conservation_holds": True,
        },
        "risk_blocks": {
            "local_paper_book_blocks": 0,
            "shared_global_blocks": 0,
            "execution_liquidity_blocks": 0,
            "local_reason_counts": {},
            "shared_global_reason_counts": {},
            "shared_global_upstream_reason_counts": {},
            "execution_liquidity_reason_counts": {},
            "shared_global_examples": [],
        },
        "mark_to_market": _empty_mark_to_market(),
        "open_exposure_buckets": _empty_open_exposure_buckets(observed_at=observed_at),
        "risk_blocks_total": 0,
        "evidence_blockers": [],
        "profitability_blockers": blockers,
        "verdict": verdict,
        "disabled_reason": disabled_reason,
        "paper_only": True,
        "live_submission_permitted": False,
    }


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
        run_integrity = {"pre_launch_candidate_signals": 0, "excluded_pre_launch_trades": 0, "excluded_legacy_trades": 0, "trades_missing_orderbook_context": 0, "integrity_errors": [], "integrity_error_count": 0, "debug_drawdown": {"reconstructed_max_drawdown": 0.0, "reconstructed_current_equity": None}}
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
        profitability_snapshot = _build_profitability_snapshot(
            family="default_strategy",
            strategy_version=_strategy_version_key(serialized_strategy_run, contract),
            observation=observation,
            headline=headline,
            comparison_modes=scope["comparison_modes"],
            replay=replay,
            review_verdict=review_verdict,
            trade_funnel=scope["trade_funnel"],
            risk_blocks=scope["risk_block_summary"],
            mark_to_market=scope["mark_to_market"],
            open_exposure_buckets=scope["open_exposure_buckets"],
            evidence_freshness=evidence_freshness,
            observed_at=now,
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
            "profitability_snapshot": profitability_snapshot,
            "open_exposure_buckets": scope["open_exposure_buckets"],
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
    run_integrity = {"pre_launch_candidate_signals": scope["trade_funnel"]["pre_launch_candidate_signals"], "excluded_pre_launch_trades": scope["trade_funnel"]["excluded_pre_launch_trades"], "excluded_legacy_trades": scope["trade_funnel"]["excluded_legacy_trades"], "trades_missing_orderbook_context": metrics["trades_missing_orderbook_context"], "integrity_errors": scope["trade_funnel"]["integrity_errors"], "integrity_error_count": scope["trade_funnel"].get("integrity_error_count", len(scope["trade_funnel"]["integrity_errors"])), "debug_drawdown": {"reconstructed_max_drawdown": metrics["max_drawdown"], "reconstructed_current_equity": round(float(settings.default_bankroll) + metrics["cumulative_pnl"], 2)}}
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
    profitability_snapshot = _build_profitability_snapshot(
        family="default_strategy",
        strategy_version=_strategy_version_key(serialized_strategy_run, contract),
        observation=observation,
        headline=headline,
        comparison_modes=scope["comparison_modes"],
        replay=replay,
        review_verdict=review_verdict,
        trade_funnel=scope["trade_funnel"],
        risk_blocks=scope["risk_block_summary"],
        mark_to_market=scope["mark_to_market"],
        open_exposure_buckets=scope["open_exposure_buckets"],
        evidence_freshness=evidence_freshness,
        observed_at=now,
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
        "profitability_snapshot": profitability_snapshot,
        "open_exposure_buckets": scope["open_exposure_buckets"],
        "latest_review_artifact": latest_review_artifact,
        "evidence_freshness": evidence_freshness,
        "detector_review": detector_review,
        "recent_mistakes": recent_mistakes,
        "review_questions": ["Did the default strategy make money after execution realism and risk controls?", "Does the qualified funnel reconcile exactly into opened, skipped, and pending decisions?", "Are shared/global risk controls contaminating what looks like local paper-book skips?", "How does the signal-level cohort compare with the legacy rank-threshold baseline?", "How much execution-adjusted evidence do we actually have?"],
    }


async def get_strategy_health(session: AsyncSession, *, use_cache: bool = True) -> dict:
    fingerprint = await get_default_strategy_activity_fingerprint(session)
    cache_key = _default_strategy_cache_key_from_fingerprint(
        session,
        "strategy_health:default_strategy",
        fingerprint,
    )
    cached = _read_cached_payload(cache_key, use_cache=use_cache)
    if cached is not None:
        return cached
    if use_cache:
        materialized = _load_materialized_default_strategy_health(fingerprint)
        if materialized is not None:
            _write_cached_payload(cache_key, materialized, use_cache=use_cache)
            _write_cached_payload(
                _default_strategy_cache_key_from_fingerprint(
                    session,
                    "profitability_snapshot:default_strategy",
                    fingerprint,
                ),
                materialized["profitability_snapshot"],
                use_cache=use_cache,
            )
            return materialized
    scope = await _get_default_strategy_scope(session)
    payload = await _serialize_strategy_health(session, scope=scope)
    final_fingerprint = await get_default_strategy_activity_fingerprint(session)
    payload["activity_fingerprint"] = deepcopy(final_fingerprint)
    final_cache_key = _default_strategy_cache_key_from_fingerprint(
        session,
        "strategy_health:default_strategy",
        final_fingerprint,
    )
    _write_cached_payload(final_cache_key, payload, use_cache=use_cache)
    if isinstance(payload.get("profitability_snapshot"), dict):
        _write_cached_payload(
            _default_strategy_cache_key_from_fingerprint(
                session,
                "profitability_snapshot:default_strategy",
                final_fingerprint,
            ),
            payload["profitability_snapshot"],
            use_cache=use_cache,
        )
    return payload


async def _serialize_profitability_snapshot_for_scope(session: AsyncSession, *, scope: dict) -> dict:
    from app.reports.strategy_review import get_latest_default_strategy_review_artifact_metadata

    strategy_run = scope["strategy_run"]
    serialized_strategy_run = serialize_strategy_run(strategy_run)
    now = scope["observed_at"]
    contract = strategy_run.contract_snapshot if strategy_run is not None else get_default_strategy_contract(started_at=scope["launch_at"])
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
    started_at = scope["started_at"]
    days_tracked = round((now - started_at).total_seconds() / 86400, 1) if started_at is not None else None
    if strategy_run is None:
        observation = {"started_at": None, "baseline_start_at": contract.get("baseline_start_at"), "first_trade_at": None, "days_tracked": None, "status": "no_active_run", "minimum_days": settings.default_strategy_min_observation_days, "preferred_days": settings.default_strategy_preferred_observation_days, "days_until_minimum_window": settings.default_strategy_min_observation_days}
        headline = {"open_exposure": 0.0, "open_trades": 0, "resolved_trades": 0, "resolved_signals": 0, "missing_resolutions": 0, "overdue_open_trades": 0, "cumulative_pnl": 0.0, "avg_clv": None, "profit_factor": 0.0, "win_rate": 0.0, "max_drawdown": 0.0, "drawdown_pct": None, "current_equity": None, "peak_equity": None, "brier_score": None}
    else:
        portfolio = scope["portfolio"]
        metrics = scope["metrics"]
        resolved_default_signals = scope["resolved_trade_signals"]
        avg_clv = _average([signal.clv for signal in resolved_default_signals if signal.clv is not None])
        minimum_days = settings.default_strategy_min_observation_days
        remaining_days = max(0, math.ceil(minimum_days - days_tracked)) if days_tracked is not None and days_tracked < minimum_days else 0
        overdue_open_trades = await get_overdue_open_trade_count(
            session,
            strategy_run_id=strategy_run.id,
        )
        observation = {"started_at": started_at.isoformat() if started_at else None, "baseline_start_at": scope["launch_at"].isoformat() if scope["launch_at"] else None, "first_trade_at": scope["first_trade_at"].isoformat() if scope["first_trade_at"] else None, "days_tracked": days_tracked, "status": _observation_status(days_tracked, launched=started_at is not None, traded_signals=scope["trade_funnel"]["traded_signals"]), "minimum_days": settings.default_strategy_min_observation_days, "preferred_days": settings.default_strategy_preferred_observation_days, "days_until_minimum_window": remaining_days}
        headline = {"open_exposure": float(portfolio["open_exposure"]), "open_trades": len(portfolio["open_trades"]), "resolved_trades": metrics["total_trades"], "resolved_signals": scope["trade_funnel"]["resolved_signals"], "missing_resolutions": scope["trade_funnel"]["unresolved_traded_signals"], "overdue_open_trades": overdue_open_trades, "cumulative_pnl": metrics["cumulative_pnl"], "avg_clv": _safe_float(avg_clv.quantize(Decimal("0.000001"))) if avg_clv is not None else None, "profit_factor": metrics["profit_factor"], "win_rate": metrics["win_rate"], "max_drawdown": float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else None, "drawdown_pct": float(strategy_run.drawdown_pct) if strategy_run.drawdown_pct is not None else None, "current_equity": float(strategy_run.current_equity) if strategy_run.current_equity is not None else None, "peak_equity": float(strategy_run.peak_equity) if strategy_run.peak_equity is not None else None, "brier_score": None}

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
    return _build_profitability_snapshot(
        family="default_strategy",
        strategy_version=_strategy_version_key(serialized_strategy_run, contract),
        observation=observation,
        headline=headline,
        comparison_modes=scope["comparison_modes"],
        replay=replay,
        review_verdict=review_verdict,
        trade_funnel=scope["trade_funnel"],
        risk_blocks=scope["risk_block_summary"],
        mark_to_market=scope["mark_to_market"],
        open_exposure_buckets=scope["open_exposure_buckets"],
        evidence_freshness=evidence_freshness,
        observed_at=now,
    )


def _decimal_or_zero(value) -> Decimal:
    return Decimal(str(value)) if value is not None else ZERO


async def _aggregate_trade_metrics(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    strategy_run,
) -> dict:
    resolved_filters = [
        PaperTrade.strategy_run_id == strategy_run_id,
        PaperTrade.status == "resolved",
        PaperTrade.pnl.is_not(None),
    ]
    row = (
        await session.execute(
            select(
                func.count(PaperTrade.id),
                func.count(PaperTrade.id).filter(PaperTrade.pnl > 0),
                func.count(PaperTrade.id).filter(PaperTrade.pnl <= 0),
                func.sum(PaperTrade.pnl),
                func.sum(PaperTrade.shadow_pnl),
                func.avg(PaperTrade.pnl),
                func.max(PaperTrade.pnl),
                func.min(PaperTrade.pnl),
                func.sum(case((PaperTrade.pnl > 0, PaperTrade.pnl), else_=ZERO)),
                func.sum(case((PaperTrade.pnl <= 0, PaperTrade.pnl), else_=ZERO)),
                func.sum(case((PaperTrade.shadow_pnl > 0, PaperTrade.shadow_pnl), else_=ZERO)),
                func.sum(case((PaperTrade.shadow_pnl <= 0, PaperTrade.shadow_pnl), else_=ZERO)),
            ).where(*resolved_filters)
        )
    ).one()
    (
        total_trades,
        wins,
        losses,
        cumulative_pnl,
        shadow_cumulative_pnl,
        avg_pnl,
        best_trade,
        worst_trade,
        total_wins,
        total_losses,
        shadow_total_wins,
        shadow_total_losses,
    ) = row
    total_trades = int(total_trades or 0)
    if total_trades <= 0:
        return _empty_metrics()

    cumulative_pnl = _decimal_or_zero(cumulative_pnl)
    shadow_cumulative_pnl = _decimal_or_zero(shadow_cumulative_pnl)
    total_wins = _decimal_or_zero(total_wins)
    total_losses_abs = abs(_decimal_or_zero(total_losses))
    shadow_total_wins = _decimal_or_zero(shadow_total_wins)
    shadow_total_losses_abs = abs(_decimal_or_zero(shadow_total_losses))
    profit_factor = (
        float(total_wins / total_losses_abs)
        if total_losses_abs > ZERO
        else None if total_wins > ZERO else 0.0
    )
    shadow_profit_factor = (
        float(shadow_total_wins / shadow_total_losses_abs)
        if shadow_total_losses_abs > ZERO
        else None if shadow_total_wins > ZERO else 0.0
    )

    if total_trades <= RESOLVED_TRADE_DRAWDOWN_LOAD_LIMIT:
        pnl_rows = (
            await session.execute(
                select(PaperTrade.pnl)
                .where(*resolved_filters)
                .order_by(PaperTrade.resolved_at.asc(), PaperTrade.id.asc())
            )
        ).scalars().all()
        max_drawdown = _safe_float(_compute_max_drawdown([_decimal_or_zero(value) for value in pnl_rows]))
    else:
        max_drawdown = _safe_float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else 0.0

    return {
        "total_trades": total_trades,
        "wins": int(wins or 0),
        "losses": int(losses or 0),
        "win_rate": round(int(wins or 0) / total_trades, 4),
        "cumulative_pnl": round(float(cumulative_pnl), 2),
        "shadow_cumulative_pnl": round(float(shadow_cumulative_pnl), 2),
        "avg_pnl": round(float(avg_pnl or 0.0), 2),
        "max_drawdown": round(float(max_drawdown or 0.0), 2),
        "sharpe_ratio": 0.0,
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "shadow_profit_factor": round(shadow_profit_factor, 4) if shadow_profit_factor is not None else None,
        "best_trade": round(float(best_trade or 0.0), 2),
        "worst_trade": round(float(worst_trade or 0.0), 2),
        "liquidity_constrained_trades": 0,
        "trades_missing_orderbook_context": 0,
    }


def _latest_open_trade_price_subquery(strategy_run_id: uuid.UUID):
    open_outcome_ids = (
        select(PaperTrade.outcome_id)
        .where(
            PaperTrade.strategy_run_id == strategy_run_id,
            PaperTrade.status == "open",
        )
        .distinct()
    )
    ranked_prices = (
        select(
            PriceSnapshot.outcome_id.label("outcome_id"),
            PriceSnapshot.price.label("price"),
            PriceSnapshot.captured_at.label("captured_at"),
            func.row_number()
            .over(
                partition_by=PriceSnapshot.outcome_id,
                order_by=(PriceSnapshot.captured_at.desc(), PriceSnapshot.id.desc()),
            )
            .label("price_rank"),
        )
        .where(PriceSnapshot.outcome_id.in_(open_outcome_ids))
        .subquery()
    )
    return (
        select(
            ranked_prices.c.outcome_id,
            ranked_prices.c.price,
            ranked_prices.c.captured_at,
        )
        .where(ranked_prices.c.price_rank == 1)
        .subquery()
    )


def _open_trade_unrealized_sql(latest_prices) -> object:
    return case(
        (
            PaperTrade.direction == "buy_no",
            (Decimal("1") - latest_prices.c.price - PaperTrade.entry_price) * PaperTrade.shares,
        ),
        else_=(latest_prices.c.price - PaperTrade.entry_price) * PaperTrade.shares,
    )


async def _aggregate_open_trade_mark_to_market(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    realized_pnl: Decimal,
    now: datetime,
) -> dict:
    latest_prices = _latest_open_trade_price_subquery(strategy_run_id)
    unrealized_expr = _open_trade_unrealized_sql(latest_prices)
    stale_cutoff = now - MARK_TO_MARKET_STALE_AFTER
    row = (
        await session.execute(
            select(
                func.coalesce(func.sum(case((latest_prices.c.price.is_not(None), unrealized_expr), else_=ZERO)), ZERO),
                func.sum(case((latest_prices.c.price.is_not(None), 1), else_=0)),
                func.sum(case((latest_prices.c.price.is_(None), 1), else_=0)),
                func.sum(
                    case(
                        (
                            and_(
                                latest_prices.c.captured_at.is_not(None),
                                latest_prices.c.captured_at < stale_cutoff,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ),
                func.max(latest_prices.c.captured_at),
            )
            .select_from(PaperTrade)
            .outerjoin(latest_prices, latest_prices.c.outcome_id == PaperTrade.outcome_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.status == "open",
            )
        )
    ).one()
    open_unrealized = _decimal_or_zero(row[0])
    return {
        "open_unrealized_pnl": _money_float(open_unrealized),
        "mark_to_market_pnl": _money_float(realized_pnl + open_unrealized),
        "open_positions_marked": int(row[1] or 0),
        "open_positions_missing_price": int(row[2] or 0),
        "open_positions_stale_price": int(row[3] or 0),
        "latest_price_at": _ensure_utc(row[4]).isoformat() if row[4] else None,
        "stale_after_seconds": int(MARK_TO_MARKET_STALE_AFTER.total_seconds()),
    }


def _open_exposure_bucket_sql(now: datetime):
    return case(
        (Market.id.is_(None), "unknown_end_date"),
        (Market.end_date.is_(None), "unknown_end_date"),
        (or_(Market.active.is_(False), Market.end_date <= now), "expired_or_due"),
        (Market.end_date <= now + timedelta(days=EXPOSURE_SHORT_HORIZON_DAYS), "short_horizon"),
        (Market.end_date <= now + timedelta(days=EXPOSURE_OPERATING_WINDOW_DAYS), "operating_window"),
        else_="long_dated",
    )


async def _aggregate_open_exposure_resolution_buckets(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    now: datetime,
) -> dict:
    latest_prices = _latest_open_trade_price_subquery(strategy_run_id)
    bucket_expr = _open_exposure_bucket_sql(now).label("bucket")
    unrealized_expr = _open_trade_unrealized_sql(latest_prices)
    stale_cutoff = now - MARK_TO_MARKET_STALE_AFTER
    summary = _empty_open_exposure_buckets(observed_at=now)
    rows = (
        await session.execute(
            select(
                bucket_expr,
                func.count(PaperTrade.id),
                func.sum(PaperTrade.size_usd),
                func.coalesce(func.sum(case((latest_prices.c.price.is_not(None), unrealized_expr), else_=ZERO)), ZERO),
                func.sum(case((latest_prices.c.price.is_(None), 1), else_=0)),
                func.sum(
                    case(
                        (
                            and_(
                                latest_prices.c.captured_at.is_not(None),
                                latest_prices.c.captured_at < stale_cutoff,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ),
            )
            .select_from(PaperTrade)
            .outerjoin(Market, Market.id == PaperTrade.market_id)
            .outerjoin(latest_prices, latest_prices.c.outcome_id == PaperTrade.outcome_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.status == "open",
            )
            .group_by(bucket_expr)
        )
    ).all()

    total_exposure = ZERO
    for bucket_key, trade_count, exposure, unrealized, missing_price_count, stale_price_count in rows:
        bucket = summary["buckets"].get(bucket_key)
        if bucket is None:
            continue
        exposure_decimal = _decimal_or_zero(exposure)
        total_exposure += exposure_decimal
        bucket["trade_count"] = int(trade_count or 0)
        bucket["open_exposure"] = _money_float(exposure_decimal)
        bucket["open_mark_to_market_pnl"] = _money_float(_decimal_or_zero(unrealized))
        bucket["missing_price_count"] = int(missing_price_count or 0)
        bucket["stale_price_count"] = int(stale_price_count or 0)

    example_rows = (
        await session.execute(
            select(PaperTrade, Market, latest_prices.c.price, latest_prices.c.captured_at)
            .select_from(PaperTrade)
            .outerjoin(Market, Market.id == PaperTrade.market_id)
            .outerjoin(latest_prices, latest_prices.c.outcome_id == PaperTrade.outcome_id)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.status == "open",
            )
            .order_by(PaperTrade.size_usd.desc(), PaperTrade.opened_at.asc())
            .limit(50)
        )
    ).all()
    for trade, market, latest_price, latest_price_at in example_rows:
        bucket_key, days_to_end = _exposure_bucket_for_market(market, now=now)
        bucket = summary["buckets"].get(bucket_key)
        if bucket is None or len(bucket["examples"]) >= 5:
            continue
        unrealized_pnl = (
            _trade_unrealized_pnl(trade, _decimal_or_zero(latest_price)) if latest_price is not None else None
        )
        bucket["examples"].append(
            {
                "trade_id": str(trade.id),
                "signal_id": str(trade.signal_id),
                "platform": market.platform if market is not None else None,
                "platform_id": market.platform_id if market is not None else None,
                "market_question": market.question if market is not None else None,
                "market_end_date": _ensure_utc(market.end_date).isoformat()
                if market is not None and market.end_date
                else None,
                "days_to_end": days_to_end,
                "open_exposure": _money_float(trade.size_usd),
                "open_mark_to_market_pnl": _money_float(unrealized_pnl) if unrealized_pnl is not None else None,
                "latest_price_at": _ensure_utc(latest_price_at).isoformat() if latest_price_at else None,
            }
        )

    drag_exposure = Decimal(str(summary["buckets"]["long_dated"]["open_exposure"])) + Decimal(
        str(summary["buckets"]["unknown_end_date"]["open_exposure"])
    )
    summary["capital_drag"] = {
        "trade_count": summary["buckets"]["long_dated"]["trade_count"]
        + summary["buckets"]["unknown_end_date"]["trade_count"],
        "open_exposure": _money_float(drag_exposure),
        "pct_open_exposure": round(float(drag_exposure / total_exposure), 4) if total_exposure > ZERO else 0.0,
    }
    return summary


async def _open_position_read_model(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    realized_pnl: Decimal,
    now: datetime,
) -> tuple[int, Decimal, dict, dict]:
    open_row = (
        await session.execute(
            select(func.count(PaperTrade.id), func.sum(PaperTrade.size_usd)).where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.status == "open",
            )
        )
    ).one()
    open_trade_count = int(open_row[0] or 0)
    open_exposure = _decimal_or_zero(open_row[1])
    if open_trade_count == 0:
        return (
            0,
            ZERO,
            {**_empty_mark_to_market(), "mark_to_market_pnl": _money_float(realized_pnl)},
            _empty_open_exposure_buckets(observed_at=now),
        )
    if open_trade_count <= OPEN_TRADE_DETAIL_LOAD_LIMIT:
        open_trades = (
            await session.execute(
                select(PaperTrade)
                .where(
                    PaperTrade.strategy_run_id == strategy_run_id,
                    PaperTrade.status == "open",
                )
                .order_by(PaperTrade.opened_at.desc(), PaperTrade.id.asc())
            )
        ).scalars().all()
        return (
            open_trade_count,
            open_exposure,
            await _open_trade_mark_to_market(
                session,
                open_trades=list(open_trades),
                realized_pnl=realized_pnl,
                now=now,
            ),
            await _open_exposure_resolution_buckets(session, open_trades=list(open_trades), now=now),
        )
    return (
        open_trade_count,
        open_exposure,
        await _aggregate_open_trade_mark_to_market(
            session,
            strategy_run_id=strategy_run_id,
            realized_pnl=realized_pnl,
            now=now,
        ),
        await _aggregate_open_exposure_resolution_buckets(session, strategy_run_id=strategy_run_id, now=now),
    )


async def _serialize_profitability_snapshot_lightweight(session: AsyncSession) -> dict:
    from app.reports.strategy_review import get_latest_default_strategy_review_artifact_metadata

    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    launch_boundary = get_default_strategy_launch_boundary()
    if strategy_run is None:
        scope = await _get_default_strategy_scope(session)
        return await _serialize_profitability_snapshot_for_scope(session, scope=scope)

    now = datetime.now(timezone.utc)
    launch_at = _ensure_utc(strategy_run.started_at)
    serialized_strategy_run = serialize_strategy_run(strategy_run)
    contract = strategy_run.contract_snapshot if isinstance(strategy_run.contract_snapshot, dict) else get_default_strategy_contract(started_at=launch_at)
    replay_status = await fetch_polymarket_replay_status(session)
    candidate_signal_count = await _count_default_strategy_signals(session, launch_at=launch_at)
    replay = _serialize_replay_review_status(
        _scope_replay_status_for_default_strategy(
            replay_status,
            observed_detector_types=(
                [settings.default_strategy_signal_type]
                if settings.default_strategy_signal_type and candidate_signal_count > 0
                else []
            ),
        )
    )
    pre_launch_candidate_signals = await _count_default_strategy_signals(
        session,
        launch_at=launch_at,
        before_launch=True,
    )
    qualified_signal_count = await _count_default_strategy_signals(
        session,
        launch_at=launch_at,
        qualified_only=True,
    )

    qualified_filters = _default_strategy_signal_filters(launch_at=launch_at, qualified_only=True)
    trade_exists_for_decision = (
        select(PaperTrade.id)
        .where(
            PaperTrade.strategy_run_id == strategy_run.id,
            PaperTrade.signal_id == ExecutionDecision.signal_id,
        )
        .exists()
    )
    decision_exists = (
        select(ExecutionDecision.id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.signal_id == Signal.id,
        )
        .exists()
    )

    opened_trade_signals = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "opened",
                    trade_exists_for_decision,
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    status_counts = {
        status: int(count or 0)
        for status, count in (
            await session.execute(
                select(
                    ExecutionDecision.decision_status,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists_for_decision,
                    ExecutionDecision.decision_status.in_(["skipped", "pending_decision"]),
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.decision_status)
            )
        ).all()
    }
    skipped_signals = status_counts.get("skipped", 0)
    pending_decision_signals = status_counts.get("pending_decision", 0)

    pending_decision_rows = await _load_pending_qualified_decision_rows(
        session,
        strategy_run_id=strategy_run.id,
        launch_at=launch_at,
    )
    pending_watch = _pending_decision_watch(pending_decision_rows, now=now)
    _publish_pending_decision_metrics(pending_watch)

    local_reason_counts = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists_for_decision,
                    ExecutionDecision.reason_code.like("risk_%"),
                    ExecutionDecision.reason_code != "risk_shared_global_block",
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }
    shared_reason_counts = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists_for_decision,
                    ExecutionDecision.reason_code == "risk_shared_global_block",
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }
    execution_liquidity_reason_counts = {
        str(reason_code): int(count or 0)
        for reason_code, count in (
            await session.execute(
                select(
                    ExecutionDecision.reason_code,
                    func.count(func.distinct(ExecutionDecision.signal_id)),
                )
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "skipped",
                    ~trade_exists_for_decision,
                    ExecutionDecision.reason_code.like("execution_%"),
                    *qualified_filters,
                )
                .group_by(ExecutionDecision.reason_code)
            )
        ).all()
        if reason_code
    }

    decision_signal_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    missing_decision_count = max(0, qualified_signal_count - decision_signal_count)
    integrity_errors: list[dict] = []
    if missing_decision_count:
        missing_result = await session.execute(
            select(Signal.id)
            .where(
                *qualified_filters,
                ~decision_exists,
            )
            .order_by(Signal.fired_at.asc(), Signal.id.asc())
            .limit(INTEGRITY_ERROR_EXAMPLE_LIMIT)
        )
        for signal_id in missing_result.scalars().all():
            _append_integrity_example(
                integrity_errors,
                signal_id=signal_id,
                error="missing_execution_decision",
            )

    status_mismatch_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    trade_exists_for_decision,
                    ExecutionDecision.decision_status != "opened",
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    opened_without_trade_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists_for_decision,
                    ExecutionDecision.decision_status == "opened",
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    unrecognized_count = int(
        (
            await session.execute(
                select(func.count(func.distinct(ExecutionDecision.signal_id)))
                .join(Signal, Signal.id == ExecutionDecision.signal_id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ~trade_exists_for_decision,
                    ExecutionDecision.decision_status.not_in(["opened", "skipped", "pending_decision"]),
                    *qualified_filters,
                )
            )
        ).scalar_one()
        or 0
    )
    integrity_error_count = (
        missing_decision_count
        + status_mismatch_count
        + opened_without_trade_count
        + unrecognized_count
    )

    metrics = await _aggregate_trade_metrics(
        session,
        strategy_run_id=strategy_run.id,
        strategy_run=strategy_run,
    )
    realized_pnl = Decimal(str(metrics["cumulative_pnl"]))
    open_trade_count, open_exposure, mark_to_market, open_exposure_buckets = await _open_position_read_model(
        session,
        strategy_run_id=strategy_run.id,
        realized_pnl=realized_pnl,
        now=now,
    )
    trade_counts = (
        await session.execute(
            select(
                func.count(func.distinct(PaperTrade.signal_id)),
                func.count(func.distinct(PaperTrade.signal_id)).filter(PaperTrade.status == "resolved"),
                func.min(PaperTrade.opened_at),
                func.max(PaperTrade.opened_at),
                func.max(PaperTrade.resolved_at),
            ).where(PaperTrade.strategy_run_id == strategy_run.id)
        )
    ).one()
    traded_signal_count = int(trade_counts[0] or 0)
    resolved_signal_count = int(trade_counts[1] or 0)
    first_trade_at = _ensure_utc(trade_counts[2])
    latest_trade_activity_at = _max_datetime(trade_counts[3], trade_counts[4])
    avg_clv = (
        await session.execute(
            select(func.avg(Signal.clv))
            .join(PaperTrade, PaperTrade.signal_id == Signal.id)
            .where(
                PaperTrade.strategy_run_id == strategy_run.id,
                PaperTrade.status == "resolved",
                PaperTrade.pnl.is_not(None),
                Signal.clv.is_not(None),
            )
        )
    ).scalar_one_or_none()
    latest_decision_at = await _latest_qualified_decision_activity_at(
        session,
        strategy_run_id=strategy_run.id,
        launch_at=launch_at,
        now=now,
    )
    comparison_modes = await compare_strategy_measurement_modes(
        session,
        start_date=strategy_run.started_at,
        end_date=now,
        strategy_run_id=strategy_run.id,
    )
    excluded_pre_launch_trades = await _count_excluded_pre_launch_trades(session, launch_at=launch_at)
    excluded_legacy_trades = await _count_excluded_legacy_trades(session, strategy_run_id=strategy_run.id)
    trade_funnel = {
        "candidate_signals": candidate_signal_count,
        "qualified_signals": qualified_signal_count,
        "opened_trade_signals": opened_trade_signals,
        "skipped_signals": skipped_signals,
        "pending_decision_signals": pending_decision_signals,
        "traded_signals": opened_trade_signals,
        "qualified_not_traded": skipped_signals + pending_decision_signals,
        "open_trades": open_trade_count,
        "resolved_trades": metrics["total_trades"],
        "resolved_signals": resolved_signal_count,
        "unresolved_traded_signals": max(0, traded_signal_count - resolved_signal_count),
        "pre_launch_candidate_signals": pre_launch_candidate_signals,
        "excluded_pre_launch_trades": excluded_pre_launch_trades,
        "excluded_legacy_trades": excluded_legacy_trades,
        "integrity_errors": integrity_errors,
        "integrity_error_count": integrity_error_count,
        "conservation_holds": qualified_signal_count
        == (opened_trade_signals + skipped_signals + pending_decision_signals),
    }
    days_tracked = round((now - launch_at).total_seconds() / 86400, 1) if launch_at is not None else None
    minimum_days = settings.default_strategy_min_observation_days
    remaining_days = (
        max(0, math.ceil(minimum_days - days_tracked))
        if days_tracked is not None and days_tracked < minimum_days
        else 0
    )
    observation = {
        "started_at": launch_at.isoformat() if launch_at else None,
        "baseline_start_at": launch_boundary.isoformat() if launch_boundary else None,
        "first_trade_at": first_trade_at.isoformat() if first_trade_at else None,
        "days_tracked": days_tracked,
        "status": _observation_status(
            days_tracked,
            launched=launch_at is not None,
            traded_signals=trade_funnel["traded_signals"],
        ),
        "minimum_days": settings.default_strategy_min_observation_days,
        "preferred_days": settings.default_strategy_preferred_observation_days,
        "days_until_minimum_window": remaining_days,
    }
    overdue_open_trades = await get_overdue_open_trade_count(
        session,
        strategy_run_id=strategy_run.id,
    )
    headline = {
        "open_exposure": float(open_exposure),
        "open_trades": open_trade_count,
        "resolved_trades": metrics["total_trades"],
        "resolved_signals": resolved_signal_count,
        "missing_resolutions": trade_funnel["unresolved_traded_signals"],
        "overdue_open_trades": overdue_open_trades,
        "cumulative_pnl": metrics["cumulative_pnl"],
        "avg_clv": _safe_float(_decimal_or_zero(avg_clv).quantize(Decimal("0.000001")))
        if avg_clv is not None
        else None,
        "profit_factor": metrics["profit_factor"],
        "win_rate": metrics["win_rate"],
        "max_drawdown": float(strategy_run.max_drawdown) if strategy_run.max_drawdown is not None else None,
        "drawdown_pct": float(strategy_run.drawdown_pct) if strategy_run.drawdown_pct is not None else None,
        "current_equity": float(strategy_run.current_equity) if strategy_run.current_equity is not None else None,
        "peak_equity": float(strategy_run.peak_equity) if strategy_run.peak_equity is not None else None,
        "brier_score": None,
    }
    latest_review_artifact = get_latest_default_strategy_review_artifact_metadata()
    evidence_freshness = build_evidence_freshness(
        observed_at=now,
        run_state="active_run",
        latest_review_artifact=latest_review_artifact,
        active_strategy_run=serialized_strategy_run,
        started_at=launch_at,
        latest_trade_activity_at=latest_trade_activity_at,
        latest_decision_at=latest_decision_at,
        pending_watch=pending_watch,
    )
    review_verdict = build_review_verdict(
        strategy_run=serialized_strategy_run,
        run_state="active_run",
        observation=observation,
        trade_funnel=trade_funnel,
        pending_watch=pending_watch,
        comparison_modes=comparison_modes,
        replay=replay,
        headline=headline,
    )
    return _build_profitability_snapshot(
        family="default_strategy",
        strategy_version=_strategy_version_key(serialized_strategy_run, contract),
        observation=observation,
        headline=headline,
        comparison_modes=comparison_modes,
        replay=replay,
        review_verdict=review_verdict,
        trade_funnel=trade_funnel,
        risk_blocks={
            "local_paper_book_blocks": sum(local_reason_counts.values()),
            "shared_global_blocks": sum(shared_reason_counts.values()),
            "execution_liquidity_blocks": sum(execution_liquidity_reason_counts.values()),
            "local_reason_counts": local_reason_counts,
            "shared_global_reason_counts": shared_reason_counts,
            "shared_global_upstream_reason_counts": {},
            "execution_liquidity_reason_counts": execution_liquidity_reason_counts,
            "shared_global_examples": [],
        },
        mark_to_market=mark_to_market,
        open_exposure_buckets=open_exposure_buckets,
        evidence_freshness=evidence_freshness,
        observed_at=now,
    )


async def get_profitability_snapshot(
    session: AsyncSession,
    *,
    family: str = "default_strategy",
    use_cache: bool = True,
) -> dict:
    normalized_family = str(family or "default_strategy").strip().lower()
    if normalized_family == "default_strategy":
        fingerprint = await get_default_strategy_activity_fingerprint(session)
        cache_key = _default_strategy_cache_key_from_fingerprint(
            session,
            "profitability_snapshot:default_strategy",
            fingerprint,
        )
    else:
        fingerprint = None
        cache_key = f"{_session_cache_namespace(session)}:profitability_snapshot:{normalized_family}"
    cached = _read_cached_payload(cache_key, use_cache=use_cache)
    if cached is not None:
        return cached
    if normalized_family != "default_strategy":
        payload = build_paper_lane_profitability_snapshot(
            family=normalized_family,
            strategy_version=None,
            disabled_reason="family_not_backed_by_default_paper_ledger",
        )
        _write_cached_payload(cache_key, payload, use_cache=use_cache)
        return payload
    if use_cache and fingerprint is not None:
        materialized = _load_materialized_default_strategy_health(fingerprint)
        if materialized is not None:
            payload = materialized["profitability_snapshot"]
            _write_cached_payload(cache_key, payload, use_cache=use_cache)
            _write_cached_payload(
                _default_strategy_cache_key_from_fingerprint(
                    session,
                    "strategy_health:default_strategy",
                    fingerprint,
                ),
                materialized,
                use_cache=use_cache,
            )
            return payload
    payload = await _serialize_profitability_snapshot_lightweight(session)
    final_fingerprint = await get_default_strategy_activity_fingerprint(session)
    _write_cached_payload(
        _default_strategy_cache_key_from_fingerprint(
            session,
            "profitability_snapshot:default_strategy",
            final_fingerprint,
        ),
        payload,
        use_cache=use_cache,
    )
    return payload


async def get_default_strategy_dashboard(session: AsyncSession) -> dict:
    scope = await _get_default_strategy_scope(session)
    strategy_health = await _serialize_strategy_health(session, scope=scope)
    fingerprint = await get_default_strategy_activity_fingerprint(session)
    strategy_health["activity_fingerprint"] = deepcopy(fingerprint)
    _write_cached_payload(
        _default_strategy_cache_key_from_fingerprint(
            session,
            "strategy_health:default_strategy",
            fingerprint,
        ),
        strategy_health,
    )
    if isinstance(strategy_health.get("profitability_snapshot"), dict):
        _write_cached_payload(
            _default_strategy_cache_key_from_fingerprint(
                session,
                "profitability_snapshot:default_strategy",
                fingerprint,
            ),
            strategy_health["profitability_snapshot"],
        )
    return {
        "portfolio": scope["portfolio"],
        "metrics": scope["metrics"],
        "pnl_curve": scope["pnl_curve"],
        "strategy_health": strategy_health,
    }
