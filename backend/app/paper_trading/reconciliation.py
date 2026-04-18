from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
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


def _reason_label(reason_code: str) -> str:
    if reason_code == "opened":
        return "Trade opened"
    if reason_code == "pending_decision":
        return "Pending decision"
    return default_strategy_skip_label(reason_code) or reason_code.replace("_", " ")


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
