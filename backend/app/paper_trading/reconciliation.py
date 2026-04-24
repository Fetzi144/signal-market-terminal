from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable, Sequence

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import default_strategy_skip_label
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from app.paper_trading.strategy_run_state import (
    apply_trade_resolution_to_run,
    initialize_strategy_run_state,
    strategy_run_state_complete,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
RETRYABLE_PENDING_DECISION_REASON_CODES = (
    "pending_decision",
    "execution_missing_orderbook_context",
    "execution_stale_orderbook_context",
    "execution_no_fill",
    "execution_partial_fill_below_minimum",
    "execution_ev_below_threshold",
)
ORDERBOOK_CONTEXT_PENDING_REASON_CODES = (
    "execution_missing_orderbook_context",
    "execution_stale_orderbook_context",
)
ORDERBOOK_CONTEXT_UNAVAILABLE_REASON_CODE = "execution_orderbook_context_unavailable"
ORDERBOOK_CONTEXT_FINALIZATION_MIN_ATTEMPTS = 2


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_safe(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _ensure_utc(value).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _parse_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed


def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _int_or_zero(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _reason_label(reason_code: str) -> str:
    if reason_code == "opened":
        return "Trade opened"
    if reason_code == "pending_decision":
        return "Pending decision"
    return default_strategy_skip_label(reason_code) or reason_code.replace("_", " ")


def _retry_attempt_count(decision: ExecutionDecision) -> int:
    details = decision.details if isinstance(decision.details, dict) else {}
    diagnostics = details.get("diagnostics") if isinstance(details.get("diagnostics"), dict) else {}
    return _int_or_zero(diagnostics.get("retry_attempt_count"))


def _decision_action(decision_status: str) -> str:
    if decision_status == "opened":
        return "cross"
    if decision_status == "pending_decision":
        return "pending"
    return "skip"


def _strategy_metadata(signal: Signal, strategy_run_id: uuid.UUID) -> dict | None:
    payload = signal.details or {}
    metadata = payload.get("default_strategy")
    if not isinstance(metadata, dict):
        return None
    stored_run_id = metadata.get("strategy_run_id")
    if stored_run_id and str(stored_run_id) != str(strategy_run_id):
        return None
    return metadata


def _resolution_sort_key(trade: PaperTrade) -> tuple[datetime, str]:
    timestamp = (
        _ensure_utc(trade.resolved_at)
        or _ensure_utc(trade.opened_at)
        or datetime.min.replace(tzinfo=timezone.utc)
    )
    return timestamp, str(trade.id)


async def hydrate_strategy_run_state(
    session: AsyncSession,
    strategy_run: StrategyRun | None,
) -> bool:
    if strategy_run is None or strategy_run_state_complete(strategy_run):
        return False

    result = await session.execute(
        select(PaperTrade).where(
            PaperTrade.strategy_run_id == strategy_run.id,
            PaperTrade.status == "resolved",
            PaperTrade.pnl.is_not(None),
        )
    )
    resolved_trades = result.scalars().all()
    resolved_trades.sort(key=_resolution_sort_key)

    initialize_strategy_run_state(strategy_run)
    for trade in resolved_trades:
        apply_trade_resolution_to_run(strategy_run, pnl=Decimal(str(trade.pnl)))

    await session.flush()
    logger.warning(
        "Hydrated missing strategy-run risk state for %s from %d resolved trade(s)",
        strategy_run.id,
        len(resolved_trades),
    )
    return True


async def load_missing_qualified_signals(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    exclude_signal_ids: Iterable[uuid.UUID] | None = None,
    limit: int | None = None,
) -> list[Signal]:
    min_ev_threshold = Decimal(str(settings.min_ev_threshold))
    query = (
        select(Signal)
        .outerjoin(
            ExecutionDecision,
            and_(
                ExecutionDecision.signal_id == Signal.id,
                ExecutionDecision.strategy_run_id == strategy_run.id,
            ),
        )
        .where(
            ExecutionDecision.id.is_(None),
            Signal.fired_at >= strategy_run.started_at,
            Signal.outcome_id.is_not(None),
            Signal.estimated_probability.is_not(None),
            Signal.price_at_fire.is_not(None),
            Signal.expected_value.is_not(None),
            func.abs(Signal.expected_value) >= min_ev_threshold,
        )
        .order_by(Signal.fired_at.asc(), Signal.id.asc())
    )
    if settings.default_strategy_signal_type:
        query = query.where(Signal.signal_type == settings.default_strategy_signal_type)

    exclude_ids = [signal_id for signal_id in (exclude_signal_ids or [])]
    if exclude_ids:
        query = query.where(Signal.id.not_in(exclude_ids))
    if limit is not None:
        query = query.limit(limit)

    result = await session.execute(query)
    return result.scalars().all()


async def backfill_execution_decisions_from_strategy_metadata(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    signals: Sequence[Signal],
) -> set[uuid.UUID]:
    if not signals:
        return set()

    signal_ids = [signal.id for signal in signals]
    trade_result = await session.execute(
        select(PaperTrade).where(
            PaperTrade.strategy_run_id == strategy_run.id,
            PaperTrade.signal_id.in_(signal_ids),
        )
    )
    trades_by_signal_id = {trade.signal_id: trade for trade in trade_result.scalars().all()}

    backfilled_signal_ids: set[uuid.UUID] = set()
    for signal in signals:
        metadata = _strategy_metadata(signal, strategy_run.id)
        if metadata is None:
            continue

        trade = trades_by_signal_id.get(signal.id)
        decision_hint = str(metadata.get("decision") or "").strip().lower()
        reason_code = str(metadata.get("reason_code") or decision_hint or "skipped").strip().lower()
        if not reason_code:
            continue

        if decision_hint == "pending_decision" or reason_code == "pending_decision":
            decision_status = "pending_decision"
        elif decision_hint == "opened" or reason_code == "opened":
            if trade is None:
                logger.warning(
                    "Skipping opened execution-decision backfill for signal %s in run %s because no paper trade exists",
                    signal.id,
                    strategy_run.id,
                )
                continue
            decision_status = "opened"
        else:
            decision_status = "skipped"

        diagnostics = metadata.get("diagnostics")
        if not isinstance(diagnostics, dict):
            diagnostics = {}

        detail = metadata.get("detail")
        reason_label = str(metadata.get("reason_label") or _reason_label(reason_code))
        approved_size_usd = _parse_decimal(diagnostics.get("approved_size_usd"))
        risk_result = None
        if reason_code.startswith("risk_"):
            risk_result = {
                "approved": False,
                "approved_size_usd": approved_size_usd or ZERO,
                "reason_code": reason_code,
                "reason": detail,
                "risk_scope": "local_paper_book",
                "risk_source": "paper_book",
            }

        decision = ExecutionDecision(
            id=uuid.uuid4(),
            signal_id=signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=(
                _parse_datetime(metadata.get("evaluated_at"))
                or _ensure_utc(signal.detected_at_local)
                or _ensure_utc(signal.received_at_local)
                or _ensure_utc(signal.fired_at)
                or datetime.now(timezone.utc)
            ),
            decision_status=decision_status,
            action=_decision_action(decision_status),
            direction=(
                trade.direction
                if trade is not None
                else diagnostics.get("direction")
            ),
            executable_entry_price=trade.entry_price if trade is not None else None,
            requested_size_usd=_parse_decimal(diagnostics.get("recommended_size_usd")),
            fillable_size_usd=approved_size_usd,
            net_ev_per_share=_parse_decimal(diagnostics.get("ev_per_share")),
            missing_orderbook_context=bool(diagnostics.get("missing_orderbook_context", False)),
            stale_orderbook_context=bool(diagnostics.get("stale_orderbook_context", False)),
            liquidity_constrained=bool(diagnostics.get("liquidity_constrained", False)),
            fill_status="filled" if trade is not None else None,
            reason_code=reason_code,
            details=_json_safe(
                {
                    "reason_label": reason_label,
                    "detail": detail,
                    "market_id": signal.market_id,
                    "market_question": (signal.details or {}).get("market_question"),
                    "estimated_probability": signal.estimated_probability,
                    "market_price": signal.price_at_fire,
                    "approved_size_usd": approved_size_usd,
                    "shares": trade.shares if trade is not None else None,
                    "diagnostics": diagnostics or None,
                    "risk_result": risk_result,
                }
            ),
        )
        session.add(decision)
        if trade is not None and trade.execution_decision_id is None:
            trade.execution_decision_id = decision.id

        backfilled_signal_ids.add(signal.id)
        if len(backfilled_signal_ids) % 500 == 0:
            await session.flush()

    if backfilled_signal_ids:
        await session.flush()
        logger.warning(
            "Backfilled %d missing execution decision(s) for strategy run %s from stored signal metadata",
            len(backfilled_signal_ids),
            strategy_run.id,
        )

    return backfilled_signal_ids


async def expire_stale_pending_execution_decisions(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> int:
    current = _ensure_utc(now) or datetime.now(timezone.utc)
    expiry_window_seconds = settings.paper_trading_pending_decision_max_age_seconds
    cutoff = current - timedelta(seconds=expiry_window_seconds)
    baseline_start_at = strategy_run.started_at.isoformat() if strategy_run.started_at else None
    expired_reason_code = "pending_decision_expired"
    expired_reason_label = _reason_label(expired_reason_code)

    query = (
        select(ExecutionDecision, Signal)
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.decision_status == "pending_decision",
            ExecutionDecision.reason_code.in_(RETRYABLE_PENDING_DECISION_REASON_CODES),
            ExecutionDecision.decision_at < cutoff,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
    )
    if limit is not None:
        query = query.limit(limit)

    rows = (await session.execute(query)).all()
    if not rows:
        return 0

    expired_at = current.isoformat()
    for decision, signal in rows:
        original_decision_at = _ensure_utc(decision.decision_at) or current
        pending_age_seconds = max(0, int((current - original_decision_at).total_seconds()))
        original_reason_code = decision.reason_code
        original_reason_label = _reason_label(original_reason_code)
        detail = (
            f"Pending execution decision expired after {pending_age_seconds} seconds "
            f"without resolving {original_reason_label.lower()}."
        )

        decision_diagnostics = dict((decision.details or {}).get("diagnostics") or {})
        decision_diagnostics.update(
            {
                "expired_pending_decision": True,
                "expired_pending_reason_code": original_reason_code,
                "expired_pending_reason_label": original_reason_label,
                "expired_pending_decision_at": original_decision_at.isoformat(),
                "expired_at": expired_at,
                "pending_age_seconds": pending_age_seconds,
                "retry_window_seconds": expiry_window_seconds,
            }
        )

        decision_details = dict(decision.details or {})
        decision_details.update(
            _json_safe(
                {
                    "reason_label": expired_reason_label,
                    "detail": detail,
                    "diagnostics": decision_diagnostics,
                    "expired_pending_reason_code": original_reason_code,
                    "expired_pending_reason_label": original_reason_label,
                    "expired_pending_decision_at": original_decision_at,
                    "expired_at": current,
                }
            )
        )

        decision.decision_status = "skipped"
        decision.action = "skip"
        decision.reason_code = expired_reason_code
        decision.details = decision_details

        signal_details = dict(signal.details or {})
        strategy_details = dict(signal_details.get("default_strategy") or {})
        signal_diagnostics = dict(strategy_details.get("diagnostics") or {})
        signal_diagnostics.update(
            {
                "expired_pending_decision": True,
                "expired_pending_reason_code": original_reason_code,
                "expired_pending_reason_label": original_reason_label,
                "expired_pending_decision_at": original_decision_at.isoformat(),
                "expired_at": expired_at,
                "pending_age_seconds": pending_age_seconds,
                "retry_window_seconds": expiry_window_seconds,
            }
        )
        strategy_details.update(
            {
                "strategy_name": settings.default_strategy_name,
                "strategy_run_id": str(strategy_run.id),
                "baseline_start_at": baseline_start_at,
                "evaluated_at": expired_at,
                "attempt_kind": "pending_expiry",
                "eligible": strategy_details.get("eligible", True),
                "decision": "skipped",
                "reason_code": expired_reason_code,
                "reason_label": expired_reason_label,
                "detail": detail,
                "trade_id": None,
                "diagnostics": signal_diagnostics,
            }
        )
        signal_details["default_strategy"] = strategy_details
        signal.details = signal_details

    await session.flush()
    return len(rows)


async def finalize_unrecoverable_orderbook_context_decisions(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> int:
    current = _ensure_utc(now) or datetime.now(timezone.utc)
    recovery_window_seconds = (
        int(settings.shadow_execution_max_forward_seconds)
        + int(settings.paper_trading_orderbook_context_finalization_grace_seconds)
    )
    cutoff = current - timedelta(seconds=recovery_window_seconds)
    baseline_start_at = strategy_run.started_at.isoformat() if strategy_run.started_at else None
    final_reason_label = _reason_label(ORDERBOOK_CONTEXT_UNAVAILABLE_REASON_CODE)

    query = (
        select(ExecutionDecision, Signal)
        .join(Signal, Signal.id == ExecutionDecision.signal_id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.decision_status == "pending_decision",
            ExecutionDecision.reason_code.in_(ORDERBOOK_CONTEXT_PENDING_REASON_CODES),
            ExecutionDecision.decision_at < cutoff,
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
    )
    if limit is not None:
        query = query.limit(limit)

    rows = (await session.execute(query)).all()
    finalized_count = 0
    finalized_at = current.isoformat()
    for decision, signal in rows:
        retry_attempt_count = _retry_attempt_count(decision)
        if retry_attempt_count < ORDERBOOK_CONTEXT_FINALIZATION_MIN_ATTEMPTS:
            continue

        original_decision_at = _ensure_utc(decision.decision_at) or current
        pending_age_seconds = max(0, int((current - original_decision_at).total_seconds()))
        original_reason_code = decision.reason_code
        original_reason_label = _reason_label(original_reason_code)
        detail = (
            f"Orderbook context remained unavailable after {pending_age_seconds} seconds "
            "and the event-time recovery window is closed."
        )

        decision_diagnostics = dict((decision.details or {}).get("diagnostics") or {})
        decision_diagnostics.update(
            {
                "orderbook_context_finalized": True,
                "finalized_orderbook_reason_code": original_reason_code,
                "finalized_orderbook_reason_label": original_reason_label,
                "finalized_orderbook_decision_at": original_decision_at.isoformat(),
                "finalized_at": finalized_at,
                "pending_age_seconds": pending_age_seconds,
                "retry_attempt_count": retry_attempt_count,
                "orderbook_context_finalization_min_attempts": ORDERBOOK_CONTEXT_FINALIZATION_MIN_ATTEMPTS,
                "orderbook_context_recovery_window_seconds": recovery_window_seconds,
                "shadow_execution_max_forward_seconds": settings.shadow_execution_max_forward_seconds,
                "orderbook_context_finalization_grace_seconds": (
                    settings.paper_trading_orderbook_context_finalization_grace_seconds
                ),
            }
        )

        decision_details = dict(decision.details or {})
        decision_details.update(
            _json_safe(
                {
                    "reason_label": final_reason_label,
                    "detail": detail,
                    "diagnostics": decision_diagnostics,
                    "finalized_orderbook_reason_code": original_reason_code,
                    "finalized_orderbook_reason_label": original_reason_label,
                    "finalized_orderbook_decision_at": original_decision_at,
                    "finalized_at": current,
                }
            )
        )

        decision.decision_status = "skipped"
        decision.action = "skip"
        decision.reason_code = ORDERBOOK_CONTEXT_UNAVAILABLE_REASON_CODE
        decision.details = decision_details

        signal_details = dict(signal.details or {})
        strategy_details = dict(signal_details.get("default_strategy") or {})
        signal_diagnostics = dict(strategy_details.get("diagnostics") or {})
        signal_diagnostics.update(decision_diagnostics)
        strategy_details.update(
            {
                "strategy_name": settings.default_strategy_name,
                "strategy_run_id": str(strategy_run.id),
                "baseline_start_at": baseline_start_at,
                "evaluated_at": finalized_at,
                "attempt_kind": "orderbook_context_finalization",
                "eligible": strategy_details.get("eligible", True),
                "decision": "skipped",
                "reason_code": ORDERBOOK_CONTEXT_UNAVAILABLE_REASON_CODE,
                "reason_label": final_reason_label,
                "detail": detail,
                "trade_id": None,
                "diagnostics": signal_diagnostics,
            }
        )
        signal_details["default_strategy"] = strategy_details
        signal.details = signal_details
        finalized_count += 1

    if finalized_count:
        await session.flush()
    return finalized_count
