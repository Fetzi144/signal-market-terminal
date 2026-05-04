from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from app.paper_trading.engine import attempt_open_trade, ensure_pending_execution_decision
from app.paper_trading.reconciliation import (
    expire_stale_pending_execution_decisions,
    finalize_unrecoverable_orderbook_context_decisions,
    hydrate_strategy_run_state,
)
from app.paper_trading.strategy_run_state import initialize_strategy_run_state
from app.strategies.registry import get_current_strategy_version, sync_strategy_registry
from app.strategy_runs.service import ACTIVE_RUN_STATUS, get_active_strategy_run

logger = logging.getLogger(__name__)

STRATEGY_FAMILY = "kalshi_cheap_yes_follow"
STRATEGY_NAME = "kalshi_cheap_yes_follow_v1"
STRATEGY_VERSION_KEY = "kalshi_cheap_yes_follow_v1"
SIGNAL_DETAILS_KEY = "kalshi_cheap_yes_follow"

MIN_YES_PRICE = Decimal("0")
MAX_YES_PRICE = Decimal("0.05")
MAX_EXPECTED_VALUE = Decimal("0.01")
ZERO = Decimal("0")


@dataclass(frozen=True)
class KalshiCheapYesFollowEvaluation:
    in_scope: bool
    eligible: bool
    reason_code: str
    reason_label: str
    detail: str | None = None
    diagnostics: dict | None = None


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _details(signal: Signal) -> dict:
    return signal.details if isinstance(signal.details, dict) else {}


def _reason_label(reason_code: str) -> str:
    labels = {
        "kalshi_cheap_yes_follow_candidate": "Kalshi cheap-YES follow candidate",
        "kalshi_cheap_yes_follow_missing_direction": "Missing price-move direction",
        "kalshi_cheap_yes_follow_missing_outcome": "Missing outcome",
        "kalshi_cheap_yes_follow_missing_price": "Missing YES price",
        "kalshi_cheap_yes_follow_missing_probability": "Missing estimated probability",
        "kalshi_cheap_yes_follow_missing_expected_value": "Missing expected value",
        "kalshi_cheap_yes_follow_expected_value_negative": "Expected value is negative",
        "kalshi_cheap_yes_follow_expected_value_too_large": "Expected value outside cheap-YES follow bucket",
        "kalshi_cheap_yes_follow_probability_not_above_price": "Probability is not above YES price",
    }
    return labels.get(reason_code, reason_code.replace("_", " "))


def evaluate_kalshi_cheap_yes_follow_signal(
    signal: Signal,
    *,
    market_platform: str | None = None,
) -> KalshiCheapYesFollowEvaluation:
    """Evaluate the fixed Kalshi cheap-YES follow rule without mutating state."""

    details = _details(signal)
    signal_type = _normalize_text(signal.signal_type)
    platform = _normalize_text(signal.source_platform or market_platform or details.get("platform"))
    direction = _normalize_text(details.get("direction"))
    yes_price = _decimal(signal.price_at_fire)
    expected_value = _decimal(signal.expected_value)
    estimated_probability = _decimal(signal.estimated_probability)

    diagnostics = {
        "strategy_family": STRATEGY_FAMILY,
        "strategy_version": STRATEGY_VERSION_KEY,
        "signal_type": signal_type,
        "platform": platform or None,
        "direction": direction or None,
        "yes_price": str(yes_price) if yes_price is not None else None,
        "expected_value": str(expected_value) if expected_value is not None else None,
        "estimated_probability": str(estimated_probability) if estimated_probability is not None else None,
        "min_yes_price": str(MIN_YES_PRICE),
        "max_yes_price": str(MAX_YES_PRICE),
        "max_expected_value": str(MAX_EXPECTED_VALUE),
        "intended_direction": "buy_yes",
    }

    if signal_type != "price_move" or platform != "kalshi":
        return KalshiCheapYesFollowEvaluation(
            in_scope=False,
            eligible=False,
            reason_code="not_kalshi_cheap_yes_follow_universe",
            reason_label="Not in Kalshi cheap-YES follow universe",
            diagnostics=diagnostics,
        )
    if not direction:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_missing_direction",
            reason_label=_reason_label("kalshi_cheap_yes_follow_missing_direction"),
            diagnostics=diagnostics,
        )
    if direction != "down":
        return KalshiCheapYesFollowEvaluation(
            in_scope=False,
            eligible=False,
            reason_code="not_kalshi_cheap_yes_follow_direction",
            reason_label="Not a downward price move",
            diagnostics=diagnostics,
        )
    if signal.outcome_id is None:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_missing_outcome",
            reason_label=_reason_label("kalshi_cheap_yes_follow_missing_outcome"),
            diagnostics=diagnostics,
        )
    if yes_price is None:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_missing_price",
            reason_label=_reason_label("kalshi_cheap_yes_follow_missing_price"),
            diagnostics=diagnostics,
        )
    if yes_price <= MIN_YES_PRICE or yes_price >= MAX_YES_PRICE:
        return KalshiCheapYesFollowEvaluation(
            in_scope=False,
            eligible=False,
            reason_code="not_kalshi_cheap_yes_follow_price_bucket",
            reason_label="YES price outside cheap follow bucket",
            diagnostics=diagnostics,
        )
    if expected_value is None:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_missing_expected_value",
            reason_label=_reason_label("kalshi_cheap_yes_follow_missing_expected_value"),
            diagnostics=diagnostics,
        )
    if expected_value < ZERO:
        return KalshiCheapYesFollowEvaluation(
            in_scope=False,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_expected_value_negative",
            reason_label=_reason_label("kalshi_cheap_yes_follow_expected_value_negative"),
            diagnostics=diagnostics,
        )
    if expected_value >= MAX_EXPECTED_VALUE:
        return KalshiCheapYesFollowEvaluation(
            in_scope=False,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_expected_value_too_large",
            reason_label=_reason_label("kalshi_cheap_yes_follow_expected_value_too_large"),
            diagnostics=diagnostics,
        )
    if estimated_probability is None:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_missing_probability",
            reason_label=_reason_label("kalshi_cheap_yes_follow_missing_probability"),
            diagnostics=diagnostics,
        )
    if estimated_probability <= yes_price:
        return KalshiCheapYesFollowEvaluation(
            in_scope=True,
            eligible=False,
            reason_code="kalshi_cheap_yes_follow_probability_not_above_price",
            reason_label=_reason_label("kalshi_cheap_yes_follow_probability_not_above_price"),
            diagnostics=diagnostics,
        )

    diagnostics["yes_entry_price"] = str(yes_price.quantize(Decimal("0.000001")))
    diagnostics["edge_per_share"] = str((estimated_probability - yes_price).quantize(Decimal("0.000001")))
    return KalshiCheapYesFollowEvaluation(
        in_scope=True,
        eligible=True,
        reason_code="kalshi_cheap_yes_follow_candidate",
        reason_label=_reason_label("kalshi_cheap_yes_follow_candidate"),
        diagnostics=diagnostics,
    )


async def ensure_active_kalshi_cheap_yes_follow_run(
    session: AsyncSession,
    *,
    started_at: datetime | None = None,
) -> tuple[StrategyRun, bool]:
    active = await get_active_strategy_run(session, STRATEGY_NAME)
    if active is not None:
        return active, False

    await sync_strategy_registry(session)
    version = await get_current_strategy_version(session, STRATEGY_FAMILY)
    resolved_started_at = _ensure_utc(started_at) or datetime.now(timezone.utc)
    strategy_run = StrategyRun(
        id=uuid.uuid4(),
        strategy_name=STRATEGY_NAME,
        strategy_family=STRATEGY_FAMILY,
        strategy_version_id=version.id if version is not None else None,
        status=ACTIVE_RUN_STATUS,
        started_at=resolved_started_at,
        contract_snapshot={
            "name": STRATEGY_NAME,
            "strategy_family": STRATEGY_FAMILY,
            "strategy_version_key": version.version_key if version is not None else STRATEGY_VERSION_KEY,
            "strategy_version_label": version.version_label if version is not None else "Kalshi Cheap-YES Follow v1",
            "strategy_version_status": version.version_status if version is not None else "candidate",
            "paper_only": True,
            "live_orders_enabled": False,
            "pilot_arming_enabled": False,
            "rule": {
                "platform": "kalshi",
                "signal_type": "price_move",
                "direction": "down",
                "min_yes_price": str(MIN_YES_PRICE),
                "max_yes_price_exclusive": str(MAX_YES_PRICE),
                "expected_value": ">=0 and <0.01",
                "trade_direction": "buy_yes",
                "paper_min_ev_threshold": str(ZERO),
            },
        },
    )
    initialize_strategy_run_state(strategy_run)
    session.add(strategy_run)
    await session.flush()
    return strategy_run, True


async def _market_platforms_for_signals(
    session: AsyncSession,
    signals: Sequence[Signal],
) -> dict[uuid.UUID, str]:
    market_ids = sorted({signal.market_id for signal in signals if signal.market_id is not None}, key=str)
    if not market_ids:
        return {}
    rows = (await session.execute(select(Market.id, Market.platform).where(Market.id.in_(market_ids)))).all()
    return {row.id: row.platform for row in rows}


async def load_unprocessed_kalshi_cheap_yes_follow_signals(
    session: AsyncSession,
    strategy_run: StrategyRun,
    *,
    exclude_signal_ids: Iterable[uuid.UUID] | None = None,
    limit: int = 100,
) -> list[Signal]:
    if limit <= 0:
        return []
    exclude_ids = [signal_id for signal_id in (exclude_signal_ids or [])]
    broad_limit = max(limit, min(limit * 10, 10_000))
    query = (
        select(Signal)
        .outerjoin(Market, Market.id == Signal.market_id)
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
            Signal.signal_type == "price_move",
            Signal.outcome_id.is_not(None),
            Signal.price_at_fire > MIN_YES_PRICE,
            Signal.price_at_fire < MAX_YES_PRICE,
            Signal.expected_value >= ZERO,
            Signal.expected_value < MAX_EXPECTED_VALUE,
            or_(Signal.source_platform == "kalshi", Market.platform == "kalshi"),
        )
        .order_by(Signal.fired_at.asc(), Signal.id.asc())
        .limit(broad_limit)
    )
    if exclude_ids:
        query = query.where(Signal.id.not_in(exclude_ids))
    signals = (await session.execute(query)).scalars().all()
    platforms = await _market_platforms_for_signals(session, signals)
    filtered = [
        signal
        for signal in signals
        if evaluate_kalshi_cheap_yes_follow_signal(
            signal,
            market_platform=platforms.get(signal.market_id),
        ).eligible
    ]
    return filtered[:limit]


async def run_kalshi_cheap_yes_follow_paper_lane(
    session: AsyncSession,
    signals: Sequence[Signal],
    *,
    pending_retry_limit: int = 100,
    backlog_limit: int = 100,
    pending_expiry_limit: int = 100,
) -> dict:
    strategy_run, run_created = await ensure_active_kalshi_cheap_yes_follow_run(session)
    state_rehydrated = await hydrate_strategy_run_state(session, strategy_run)
    expired_pending = await expire_stale_pending_execution_decisions(
        session,
        strategy_run,
        limit=pending_expiry_limit,
    )
    finalized_orderbook_context = await finalize_unrecoverable_orderbook_context_decisions(
        session,
        strategy_run,
        limit=pending_expiry_limit,
    )

    pending_retry_signals = []
    if pending_retry_limit > 0:
        pending_retry_signals = (
            await session.execute(
                select(Signal)
                .join(ExecutionDecision, ExecutionDecision.signal_id == Signal.id)
                .where(
                    ExecutionDecision.strategy_run_id == strategy_run.id,
                    ExecutionDecision.decision_status == "pending_decision",
                )
                .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
                .limit(pending_retry_limit)
            )
        ).scalars().all()

    fresh_signal_ids = {signal.id for signal in signals}
    backlog_signals = await load_unprocessed_kalshi_cheap_yes_follow_signals(
        session,
        strategy_run,
        exclude_signal_ids=fresh_signal_ids,
        limit=backlog_limit,
    )
    work_items = (
        [(signal, "fresh_signal") for signal in signals]
        + [(signal, "retry") for signal in pending_retry_signals if signal.id not in fresh_signal_ids]
        + [
            (signal, "backlog_repair")
            for signal in backlog_signals
            if signal.id not in fresh_signal_ids
        ]
    )
    platforms = await _market_platforms_for_signals(session, [signal for signal, _kind in work_items])

    candidate_count = 0
    opened_count = 0
    retry_candidates = 0
    backlog_candidates = 0
    skip_counts: dict[str, int] = {}
    changed = run_created or state_rehydrated or expired_pending or finalized_orderbook_context

    for signal, attempt_kind in work_items:
        evaluation = evaluate_kalshi_cheap_yes_follow_signal(
            signal,
            market_platform=platforms.get(signal.market_id),
        )
        if not evaluation.in_scope:
            continue

        candidate_count += 1
        if attempt_kind == "retry":
            retry_candidates += 1
        elif attempt_kind == "backlog_repair":
            backlog_candidates += 1

        details = dict(signal.details or {})
        market_question = str(details.get("market_question") or "")
        attempted_at = datetime.now(timezone.utc).isoformat()
        if evaluation.eligible and attempt_kind != "retry":
            await ensure_pending_execution_decision(
                session=session,
                signal_id=signal.id,
                outcome_id=signal.outcome_id,
                market_id=signal.market_id,
                estimated_probability=signal.estimated_probability,
                market_price=signal.price_at_fire,
                market_question=market_question,
                fired_at=signal.fired_at,
                strategy_run_id=strategy_run.id,
            )
        result = await attempt_open_trade(
            session=session,
            signal_id=signal.id,
            outcome_id=signal.outcome_id,
            market_id=signal.market_id,
            estimated_probability=signal.estimated_probability,
            market_price=signal.price_at_fire,
            market_question=market_question,
            fired_at=signal.fired_at,
            strategy_run_id=strategy_run.id,
            precheck_reason_code=None if evaluation.eligible else evaluation.reason_code,
            precheck_reason_label=evaluation.reason_label,
            min_ev_threshold=ZERO,
        )
        changed = True

        lane_details = dict(details.get(SIGNAL_DETAILS_KEY) or {})
        lane_details.update(
            {
                "strategy_name": STRATEGY_NAME,
                "strategy_family": STRATEGY_FAMILY,
                "strategy_version": STRATEGY_VERSION_KEY,
                "strategy_run_id": str(strategy_run.id),
                "evaluated_at": attempted_at,
                "attempt_kind": attempt_kind,
                "eligible": evaluation.eligible,
                "intended_direction": "buy_yes",
                "decision": result.decision,
                "reason_code": result.reason_code,
                "reason_label": result.reason_label,
                "detail": result.detail,
                "trade_id": str(result.trade.id) if result.trade is not None else None,
                "evaluation": evaluation.diagnostics or {},
            }
        )
        if result.diagnostics:
            lane_details["diagnostics"] = result.diagnostics
        details[SIGNAL_DETAILS_KEY] = lane_details
        signal.details = details

        if result.trade is not None:
            opened_count += 1
        else:
            skip_counts[result.reason_code or "unknown"] = skip_counts.get(result.reason_code or "unknown", 0) + 1

    if changed:
        await session.commit()

    if candidate_count or run_created or expired_pending or finalized_orderbook_context:
        logger.info(
            "Kalshi cheap-YES follow paper lane: opened %d trade(s) from %d candidate signal(s)",
            opened_count,
            candidate_count,
        )
        if skip_counts:
            logger.info("Kalshi cheap-YES follow skips by reason: %s", skip_counts)

    return {
        "strategy_run_id": str(strategy_run.id),
        "run_created": run_created,
        "state_rehydrated": state_rehydrated,
        "candidate_count": candidate_count,
        "opened_count": opened_count,
        "retry_candidates": retry_candidates,
        "backlog_candidates": backlog_candidates,
        "expired_pending_decisions": expired_pending,
        "finalized_orderbook_context_decisions": finalized_orderbook_context,
        "skip_counts": skip_counts,
    }
