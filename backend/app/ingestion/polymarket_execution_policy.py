from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.metrics import (
    polymarket_execution_action_candidates_evaluated,
    polymarket_execution_chosen_actions,
    polymarket_execution_chosen_decisions,
    polymarket_execution_decision_failures,
    polymarket_execution_estimated_slippage_bps,
    polymarket_execution_invalid_candidates,
    polymarket_execution_last_success_timestamp,
    polymarket_execution_skip_decisions,
)
from app.models.execution_decision import ExecutionDecision
from app.models.market import Market, Outcome
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketParamHistory
from app.models.polymarket_microstructure import PolymarketAlphaLabel, PolymarketPassiveFillLabel
from app.models.polymarket_raw import PolymarketBookDelta, PolymarketBookSnapshot
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.signal import Signal
from app.signals.ev import compute_directional_ev_full
from app.signals.kelly import kelly_size_for_trade

ZERO = Decimal("0")
ONE = Decimal("1")
TEN_THOUSAND = Decimal("10000")
PRICE_Q = Decimal("0.00000001")
SIZE_Q = Decimal("0.0001")
SHARE_Q = Decimal("0.0001")
POLICY_VERSION = "phase6_baseline_v1"
PASSIVE_LABEL_BY_DIRECTION = {
    "buy_yes": "buy_post_best_bid",
    "buy_no": "sell_post_best_ask",
}


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _quantize(value: Decimal | None, quantum: Decimal) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(quantum)


def _decimal_mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _entry_price_for_direction(direction: str, *, yes_price: Decimal) -> Decimal:
    return yes_price if direction == "buy_yes" else (ONE - yes_price).quantize(PRICE_Q)


def _directional_bps(mid_return_bps: Decimal | None, *, direction: str) -> Decimal | None:
    if mid_return_bps is None:
        return None
    return mid_return_bps if direction == "buy_yes" else (-mid_return_bps)


def _bps_from_per_share(ev_per_share: Decimal | None, *, entry_price: Decimal | None) -> Decimal | None:
    if ev_per_share is None or entry_price is None or entry_price <= ZERO:
        return None
    return ((ev_per_share / entry_price) * TEN_THOUSAND).quantize(PRICE_Q)


def _per_share_from_bps(bps: Decimal | None, *, entry_price: Decimal | None) -> Decimal | None:
    if bps is None or entry_price is None:
        return None
    return ((entry_price * bps) / TEN_THOUSAND).quantize(PRICE_Q)


def _is_tick_aligned(price: Decimal, tick_size: Decimal | None) -> bool:
    if tick_size is None or tick_size <= ZERO:
        return True
    try:
        return (price / tick_size).quantize(Decimal("1")) == (price / tick_size)
    except InvalidOperation:
        return False


@dataclass(slots=True)
class BookLevel:
    yes_price: Decimal
    size_shares: Decimal


@dataclass(slots=True)
class BookWalkResult:
    fillable_size: Decimal
    fillable_shares: Decimal
    avg_entry_price: Decimal | None
    worst_price: Decimal | None
    slippage_cost: Decimal
    slippage_bps: Decimal | None
    path: list[dict[str, Any]]


@dataclass(slots=True)
class PassiveLabelSummary:
    row_count: int
    fill_probability: Decimal | None
    touch_rate: Decimal | None
    trade_through_rate: Decimal | None
    improved_against_rate: Decimal | None
    adverse_selection_bps: Decimal | None
    source_feature_row_id: int | None

    def as_json(self) -> dict[str, Any]:
        return _json_safe(
            {
                "row_count": self.row_count,
                "fill_probability": self.fill_probability,
                "touch_rate": self.touch_rate,
                "trade_through_rate": self.trade_through_rate,
                "improved_against_rate": self.improved_against_rate,
                "adverse_selection_bps": self.adverse_selection_bps,
                "source_feature_row_id": self.source_feature_row_id,
            }
        )


@dataclass(slots=True)
class AlphaLabelSummary:
    row_count: int
    directional_mean_bps: Decimal | None
    positive_directional_mean_bps: Decimal | None
    source_feature_row_id: int | None

    def as_json(self) -> dict[str, Any]:
        return _json_safe(
            {
                "row_count": self.row_count,
                "directional_mean_bps": self.directional_mean_bps,
                "positive_directional_mean_bps": self.positive_directional_mean_bps,
                "source_feature_row_id": self.source_feature_row_id,
            }
        )


@dataclass(slots=True)
class PolymarketExecutionContext:
    signal_id: uuid.UUID | None
    market_id: uuid.UUID
    outcome_id: uuid.UUID
    direction: str
    estimated_probability: Decimal
    market_price: Decimal
    baseline_target_size: Decimal
    bankroll: Decimal
    decision_at: datetime
    asset_id: str
    condition_id: str
    market_dim_id: int | None
    asset_dim_id: int | None
    tick_size: Decimal | None
    min_order_size: Decimal | None
    fees_enabled: bool
    taker_fee_rate: Decimal
    maker_fee_rate: Decimal
    fee_schedule_json: dict[str, Any] | None
    recon_state_id: int | None
    recon_status: str | None
    reliable_book: bool
    book_reason: str | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    bids: list[BookLevel]
    asks: list[BookLevel]
    snapshot_id: int | None
    snapshot_source_kind: str | None
    snapshot_observed_at: datetime | None
    snapshot_age_seconds: int | None
    horizon_ms: int
    lookback_start: datetime

    @property
    def midpoint(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return ((self.best_bid + self.best_ask) / Decimal("2")).quantize(PRICE_Q)


@dataclass(slots=True)
class ActionCandidateEvaluation:
    side: str
    action_type: str
    order_type_hint: str | None
    decision_horizon_ms: int | None
    target_size: Decimal
    est_fillable_size: Decimal | None = None
    est_fill_probability: Decimal | None = None
    est_avg_entry_price: Decimal | None = None
    est_worst_price: Decimal | None = None
    est_tick_size: Decimal | None = None
    est_min_order_size: Decimal | None = None
    est_taker_fee: Decimal | None = None
    est_maker_fee: Decimal | None = None
    est_slippage_cost: Decimal | None = None
    est_alpha_capture_bps: Decimal | None = None
    est_adverse_selection_bps: Decimal | None = None
    est_net_ev_bps: Decimal | None = None
    est_net_ev_total: Decimal | None = None
    est_net_ev_per_share: Decimal | None = None
    valid: bool = False
    invalid_reason: str | None = None
    source_feature_row_id: int | None = None
    source_label_summary_json: dict[str, Any] | None = None
    details_json: dict[str, Any] = field(default_factory=dict)

    def as_model_kwargs(
        self,
        *,
        context: PolymarketExecutionContext,
        execution_decision_id: uuid.UUID | None,
    ) -> dict[str, Any]:
        return {
            "signal_id": context.signal_id,
            "execution_decision_id": execution_decision_id,
            "market_dim_id": context.market_dim_id,
            "asset_dim_id": context.asset_dim_id,
            "condition_id": context.condition_id,
            "asset_id": context.asset_id,
            "outcome_id": context.outcome_id,
            "side": self.side,
            "action_type": self.action_type,
            "order_type_hint": self.order_type_hint,
            "decision_horizon_ms": self.decision_horizon_ms,
            "target_size": _quantize(self.target_size, SIZE_Q) or ZERO,
            "est_fillable_size": _quantize(self.est_fillable_size, SIZE_Q),
            "est_fill_probability": _quantize(self.est_fill_probability, Decimal("0.000001")),
            "est_avg_entry_price": _quantize(self.est_avg_entry_price, PRICE_Q),
            "est_worst_price": _quantize(self.est_worst_price, PRICE_Q),
            "est_tick_size": _quantize(self.est_tick_size, PRICE_Q),
            "est_min_order_size": _quantize(self.est_min_order_size, Decimal("0.00000001")),
            "est_taker_fee": _quantize(self.est_taker_fee, PRICE_Q),
            "est_maker_fee": _quantize(self.est_maker_fee, PRICE_Q),
            "est_slippage_cost": _quantize(self.est_slippage_cost, PRICE_Q),
            "est_alpha_capture_bps": _quantize(self.est_alpha_capture_bps, PRICE_Q),
            "est_adverse_selection_bps": _quantize(self.est_adverse_selection_bps, PRICE_Q),
            "est_net_ev_bps": _quantize(self.est_net_ev_bps, PRICE_Q),
            "est_net_ev_total": _quantize(self.est_net_ev_total, PRICE_Q),
            "valid": self.valid,
            "invalid_reason": self.invalid_reason,
            "policy_version": POLICY_VERSION,
            "source_recon_state_id": context.recon_state_id,
            "source_feature_row_id": self.source_feature_row_id,
            "source_label_summary_json": _json_safe(self.source_label_summary_json),
            "details_json": _json_safe(self.details_json),
            "decided_at": context.decision_at,
        }


@dataclass(slots=True)
class PolymarketExecutionPolicyResult:
    applicable: bool
    policy_version: str
    context: PolymarketExecutionContext | None
    candidates: list[ActionCandidateEvaluation]
    chosen_candidate: ActionCandidateEvaluation | None
    chosen_reason: str | None

    def choice_payload(self) -> dict[str, Any] | None:
        if self.context is None or self.chosen_candidate is None:
            return None
        valid_actions = [candidate.action_type for candidate in self.candidates if candidate.valid]
        invalid_actions = {
            candidate.action_type: candidate.invalid_reason
            for candidate in self.candidates
            if not candidate.valid and candidate.invalid_reason is not None
        }
        return _json_safe(
            {
                "policy_version": self.policy_version,
                "chosen_action_type": self.chosen_candidate.action_type,
                "chosen_reason": self.chosen_reason,
                "valid_actions": valid_actions,
                "invalid_actions": invalid_actions,
                "asset_id": self.context.asset_id,
                "condition_id": self.context.condition_id,
                "reliable_book": self.context.reliable_book,
                "book_reason": self.context.book_reason,
            }
        )

    def shadow_execution(self) -> dict[str, Any]:
        if self.context is None:
            return {
                "shadow_entry_price": None,
                "details": {
                    "missing_orderbook_context": True,
                    "stale_orderbook_context": False,
                    "liquidity_constrained": False,
                    "fill_status": "no_fill",
                    "fill_reason": "policy_not_applicable",
                    "requested_size_usd": "0.0000",
                    "filled_size_usd": "0.0000",
                    "unfilled_size_usd": "0.0000",
                    "fill_pct": "0.000000",
                    "shadow_shares": "0.0000",
                },
            }

        chosen = self.chosen_candidate
        requested_size = chosen.target_size if chosen is not None else self.context.baseline_target_size
        filled_size = chosen.est_fillable_size if chosen is not None and chosen.est_fillable_size is not None else ZERO
        entry_price = chosen.est_avg_entry_price if chosen is not None else None
        fill_probability = chosen.est_fill_probability if chosen is not None else ZERO
        shadow_shares = (
            (filled_size / entry_price).quantize(SHARE_Q)
            if entry_price is not None and entry_price > ZERO and filled_size > ZERO
            else ZERO
        )
        action_type = chosen.action_type if chosen is not None else "skip"
        fill_status = "no_fill"
        if action_type == "cross_now":
            if fill_probability is not None and fill_probability >= ONE:
                fill_status = "full_fill"
            elif fill_probability is not None and fill_probability > ZERO:
                fill_status = "partial_fill"
        elif action_type in {"post_best", "step_ahead"} and filled_size > ZERO:
            fill_status = "expected_passive_fill"
        return {
            "shadow_entry_price": entry_price,
            "details": _json_safe(
                {
                    "missing_orderbook_context": not self.context.reliable_book,
                    "stale_orderbook_context": self.context.recon_status not in {None, "live"},
                    "liquidity_constrained": bool(
                        action_type == "cross_now" and fill_probability is not None and fill_probability < ONE
                    ),
                    "fill_status": fill_status,
                    "fill_reason": self.chosen_reason,
                    "snapshot_id": self.context.snapshot_id,
                    "snapshot_source_kind": self.context.snapshot_source_kind,
                    "captured_at": self.context.snapshot_observed_at,
                    "snapshot_age_seconds": self.context.snapshot_age_seconds,
                    "best_bid": self.context.best_bid,
                    "best_ask": self.context.best_ask,
                    "spread": self.context.spread,
                    "requested_size_usd": _quantize(requested_size, SIZE_Q) or ZERO,
                    "filled_size_usd": _quantize(filled_size, SIZE_Q) or ZERO,
                    "unfilled_size_usd": _quantize(max((requested_size - filled_size), ZERO), SIZE_Q) or ZERO,
                    "fill_pct": _quantize(fill_probability, Decimal("0.000001")) if fill_probability is not None else ZERO,
                    "shadow_shares": shadow_shares,
                    "policy_version": self.policy_version,
                    "selected_action_type": action_type,
                    "selected_order_type_hint": chosen.order_type_hint if chosen is not None else None,
                }
            ),
        }


async def evaluate_polymarket_execution_policy(
    session: AsyncSession,
    *,
    signal_id: uuid.UUID | None,
    outcome_id: uuid.UUID,
    market_id: uuid.UUID,
    direction: str,
    estimated_probability: Decimal,
    market_price: Decimal,
    decision_at: datetime,
    baseline_target_size: Decimal,
    bankroll: Decimal,
    force_enabled: bool = False,
) -> PolymarketExecutionPolicyResult:
    if not settings.polymarket_execution_policy_enabled and not force_enabled:
        return PolymarketExecutionPolicyResult(
            applicable=False,
            policy_version=POLICY_VERSION,
            context=None,
            candidates=[],
            chosen_candidate=None,
            chosen_reason="policy_disabled",
        )

    try:
        context = await _resolve_context(
            session,
            signal_id=signal_id,
            outcome_id=outcome_id,
            market_id=market_id,
            direction=direction,
            estimated_probability=estimated_probability,
            market_price=market_price,
            decision_at=decision_at,
            baseline_target_size=baseline_target_size,
            bankroll=bankroll,
        )
        if context is None:
            return PolymarketExecutionPolicyResult(
                applicable=False,
                policy_version=POLICY_VERSION,
                context=None,
                candidates=[],
                chosen_candidate=None,
                chosen_reason="polymarket_context_unavailable",
            )

        candidates = [
            await _evaluate_cross_now(session, context),
            await _evaluate_post_best(session, context),
        ]
        if settings.polymarket_execution_policy_step_ahead_enabled:
            candidates.append(await _evaluate_step_ahead(session, context))
        else:
            candidates.append(
                ActionCandidateEvaluation(
                    side=context.direction,
                    action_type="step_ahead",
                    order_type_hint="post_only",
                    decision_horizon_ms=context.horizon_ms,
                    target_size=context.baseline_target_size,
                    est_tick_size=context.tick_size,
                    est_min_order_size=context.min_order_size,
                    valid=False,
                    invalid_reason="step_ahead_disabled",
                    details_json={"reason": "step_ahead_disabled"},
                )
            )
        candidates.append(_evaluate_skip(context, candidates))

        chosen_candidate, chosen_reason = _choose_candidate(candidates)
        for candidate in candidates:
            polymarket_execution_action_candidates_evaluated.labels(action_type=candidate.action_type).inc()
            if not candidate.valid:
                polymarket_execution_invalid_candidates.labels(
                    action_type=candidate.action_type,
                    reason=candidate.invalid_reason or "invalid",
                ).inc()

        polymarket_execution_chosen_decisions.inc()
        if chosen_candidate is not None:
            polymarket_execution_chosen_actions.labels(action_type=chosen_candidate.action_type).inc()
            if chosen_candidate.action_type == "skip":
                polymarket_execution_skip_decisions.inc()
            slippage_bps = _to_decimal((chosen_candidate.details_json or {}).get("slippage_bps"))
            if slippage_bps is not None:
                polymarket_execution_estimated_slippage_bps.labels(
                    action_type=chosen_candidate.action_type
                ).observe(float(slippage_bps))
        polymarket_execution_last_success_timestamp.set((_ensure_utc(decision_at) or decision_at).timestamp())

        return PolymarketExecutionPolicyResult(
            applicable=True,
            policy_version=POLICY_VERSION,
            context=context,
            candidates=candidates,
            chosen_candidate=chosen_candidate,
            chosen_reason=chosen_reason,
        )
    except Exception:
        polymarket_execution_decision_failures.inc()
        raise


async def persist_polymarket_execution_policy_result(
    session: AsyncSession,
    *,
    result: PolymarketExecutionPolicyResult,
    execution_decision: ExecutionDecision | None,
) -> list[PolymarketExecutionActionCandidate]:
    if not result.applicable or result.context is None:
        return []

    execution_decision_id = execution_decision.id if execution_decision is not None else None
    rows: list[PolymarketExecutionActionCandidate] = []
    chosen_row: PolymarketExecutionActionCandidate | None = None
    for candidate in result.candidates:
        row = PolymarketExecutionActionCandidate(**candidate.as_model_kwargs(
            context=result.context,
            execution_decision_id=execution_decision_id,
        ))
        session.add(row)
        rows.append(row)
    await session.flush()

    if execution_decision is not None and result.chosen_candidate is not None:
        for row in rows:
            if row.action_type == result.chosen_candidate.action_type:
                chosen_row = row
                break
        if chosen_row is not None:
            execution_decision.chosen_action_candidate_id = chosen_row.id
            await session.flush()
    return rows


async def fetch_polymarket_execution_policy_status(session: AsyncSession) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    decision_stats = (
        await session.execute(
            select(
                func.max(ExecutionDecision.decision_at),
                func.count(ExecutionDecision.id),
                func.avg(ExecutionDecision.chosen_est_net_ev_bps),
            ).where(
                ExecutionDecision.chosen_policy_version == POLICY_VERSION,
                ExecutionDecision.decision_at >= start,
            )
        )
    ).one()

    action_rows = (
        await session.execute(
            select(
                ExecutionDecision.chosen_action_type,
                func.count(ExecutionDecision.id),
            )
            .where(
                ExecutionDecision.chosen_policy_version == POLICY_VERSION,
                ExecutionDecision.decision_at >= start,
            )
            .group_by(ExecutionDecision.chosen_action_type)
        )
    ).all()

    invalid_rows = (
        await session.execute(
            select(func.count(PolymarketExecutionActionCandidate.id)).where(
                PolymarketExecutionActionCandidate.policy_version == POLICY_VERSION,
                PolymarketExecutionActionCandidate.decided_at >= start,
                PolymarketExecutionActionCandidate.valid.is_(False),
            )
        )
    ).scalar() or 0

    skip_rows = (
        await session.execute(
            select(func.count(ExecutionDecision.id)).where(
                ExecutionDecision.chosen_policy_version == POLICY_VERSION,
                ExecutionDecision.decision_at >= start,
                ExecutionDecision.chosen_action_type == "skip",
            )
        )
    ).scalar() or 0

    return {
        "enabled": settings.polymarket_execution_policy_enabled,
        "require_live_book": settings.polymarket_execution_policy_require_live_book,
        "default_horizon_ms": settings.polymarket_execution_policy_default_horizon_ms,
        "passive_lookback_hours": settings.polymarket_execution_policy_passive_lookback_hours,
        "passive_min_label_rows": settings.polymarket_execution_policy_passive_min_label_rows,
        "step_ahead_enabled": settings.polymarket_execution_policy_step_ahead_enabled,
        "max_cross_slippage_bps": settings.polymarket_execution_policy_max_cross_slippage_bps,
        "min_net_ev_bps": settings.polymarket_execution_policy_min_net_ev_bps,
        "last_successful_decision_at": decision_stats[0],
        "recent_decisions_24h": int(decision_stats[1] or 0),
        "recent_action_mix": {str(action or "unknown"): int(count) for action, count in action_rows},
        "recent_invalid_candidates_24h": int(invalid_rows),
        "recent_skip_decisions_24h": int(skip_rows),
        "recent_avg_est_net_ev_bps": float(decision_stats[2]) if decision_stats[2] is not None else None,
    }


async def lookup_polymarket_execution_action_candidates(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    action_type: str | None,
    valid: bool | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(PolymarketExecutionActionCandidate).where(
        PolymarketExecutionActionCandidate.policy_version == POLICY_VERSION
    )
    if asset_id:
        query = query.where(PolymarketExecutionActionCandidate.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketExecutionActionCandidate.condition_id == condition_id)
    if action_type:
        query = query.where(PolymarketExecutionActionCandidate.action_type == action_type)
    if valid is not None:
        query = query.where(PolymarketExecutionActionCandidate.valid.is_(valid))
    if start is not None:
        query = query.where(PolymarketExecutionActionCandidate.decided_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketExecutionActionCandidate.decided_at <= _ensure_utc(end))

    rows = (
        await session.execute(
            query.order_by(
                PolymarketExecutionActionCandidate.decided_at.desc(),
                PolymarketExecutionActionCandidate.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [_serialize_candidate_row(row) for row in rows]


async def lookup_polymarket_execution_decisions(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    signal_id: uuid.UUID | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = (
        select(ExecutionDecision, PolymarketExecutionActionCandidate)
        .outerjoin(
            PolymarketExecutionActionCandidate,
            ExecutionDecision.chosen_action_candidate_id == PolymarketExecutionActionCandidate.id,
        )
        .where(ExecutionDecision.chosen_policy_version == POLICY_VERSION)
    )
    if signal_id is not None:
        query = query.where(ExecutionDecision.signal_id == signal_id)
    if asset_id:
        query = query.where(PolymarketExecutionActionCandidate.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketExecutionActionCandidate.condition_id == condition_id)
    if start is not None:
        query = query.where(ExecutionDecision.decision_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(ExecutionDecision.decision_at <= _ensure_utc(end))

    rows = (
        await session.execute(
            query.order_by(ExecutionDecision.decision_at.desc(), ExecutionDecision.id.desc()).limit(limit)
        )
    ).all()
    return [_serialize_execution_decision_row(decision, candidate) for decision, candidate in rows]


async def fetch_polymarket_execution_invalidation_reasons(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    query = select(
        PolymarketExecutionActionCandidate.invalid_reason,
        func.count(PolymarketExecutionActionCandidate.id),
    ).where(
        PolymarketExecutionActionCandidate.policy_version == POLICY_VERSION,
        PolymarketExecutionActionCandidate.valid.is_(False),
        PolymarketExecutionActionCandidate.invalid_reason.is_not(None),
    )
    if asset_id:
        query = query.where(PolymarketExecutionActionCandidate.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketExecutionActionCandidate.condition_id == condition_id)
    if start is not None:
        query = query.where(PolymarketExecutionActionCandidate.decided_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketExecutionActionCandidate.decided_at <= _ensure_utc(end))
    rows = (
        await session.execute(
            query.group_by(PolymarketExecutionActionCandidate.invalid_reason)
            .order_by(func.count(PolymarketExecutionActionCandidate.id).desc())
            .limit(limit)
        )
    ).all()
    return [{"invalid_reason": reason, "count": int(count)} for reason, count in rows]


async def fetch_polymarket_execution_action_mix(
    session: AsyncSession,
    *,
    asset_id: str | None,
    condition_id: str | None,
    start: datetime | None,
    end: datetime | None,
) -> list[dict[str, Any]]:
    query = (
        select(
            ExecutionDecision.chosen_action_type,
            func.count(ExecutionDecision.id),
        )
        .outerjoin(
            PolymarketExecutionActionCandidate,
            ExecutionDecision.chosen_action_candidate_id == PolymarketExecutionActionCandidate.id,
        )
        .where(ExecutionDecision.chosen_policy_version == POLICY_VERSION)
    )
    if asset_id:
        query = query.where(PolymarketExecutionActionCandidate.asset_id == asset_id)
    if condition_id:
        query = query.where(PolymarketExecutionActionCandidate.condition_id == condition_id)
    if start is not None:
        query = query.where(ExecutionDecision.decision_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(ExecutionDecision.decision_at <= _ensure_utc(end))
    rows = (
        await session.execute(
            query.group_by(ExecutionDecision.chosen_action_type)
            .order_by(func.count(ExecutionDecision.id).desc())
        )
    ).all()
    return [{"action_type": str(action or "unknown"), "count": int(count)} for action, count in rows]


async def evaluate_polymarket_execution_policy_dry_run(
    session: AsyncSession,
    *,
    signal_id: uuid.UUID,
) -> dict[str, Any]:
    signal = await session.get(Signal, signal_id)
    if signal is None:
        raise LookupError("Signal not found")
    if signal.outcome_id is None or signal.estimated_probability is None or signal.price_at_fire is None:
        return {
            "applicable": False,
            "reason": "signal_missing_required_fields",
            "signal_id": str(signal_id),
        }

    direction = "buy_yes" if signal.estimated_probability >= signal.price_at_fire else "buy_no"
    baseline = kelly_size_for_trade(
        direction=direction,
        estimated_probability=signal.estimated_probability,
        entry_price=signal.price_at_fire,
        bankroll=Decimal(str(settings.default_bankroll)),
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )
    result = await evaluate_polymarket_execution_policy(
        session,
        signal_id=signal.id,
        outcome_id=signal.outcome_id,
        market_id=signal.market_id,
        direction=direction,
        estimated_probability=signal.estimated_probability,
        market_price=signal.price_at_fire,
        decision_at=_ensure_utc(signal.fired_at) or datetime.now(timezone.utc),
        baseline_target_size=baseline["recommended_size_usd"],
        bankroll=Decimal(str(settings.default_bankroll)),
    )
    return serialize_polymarket_execution_policy_result(result)


def serialize_polymarket_execution_policy_result(result: PolymarketExecutionPolicyResult) -> dict[str, Any]:
    return {
        "applicable": result.applicable,
        "policy_version": result.policy_version,
        "context": (
            None
            if result.context is None
            else _json_safe(
                {
                    "signal_id": result.context.signal_id,
                    "market_id": result.context.market_id,
                    "outcome_id": result.context.outcome_id,
                    "direction": result.context.direction,
                    "asset_id": result.context.asset_id,
                    "condition_id": result.context.condition_id,
                    "tick_size": result.context.tick_size,
                    "min_order_size": result.context.min_order_size,
                    "fees_enabled": result.context.fees_enabled,
                    "taker_fee_rate": result.context.taker_fee_rate,
                    "reliable_book": result.context.reliable_book,
                    "book_reason": result.context.book_reason,
                    "best_bid": result.context.best_bid,
                    "best_ask": result.context.best_ask,
                    "spread": result.context.spread,
                    "snapshot_id": result.context.snapshot_id,
                    "snapshot_source_kind": result.context.snapshot_source_kind,
                    "snapshot_observed_at": result.context.snapshot_observed_at,
                    "snapshot_age_seconds": result.context.snapshot_age_seconds,
                    "horizon_ms": result.context.horizon_ms,
                }
            )
        ),
        "chosen_reason": result.chosen_reason,
        "chosen_candidate": (
            None
            if result.chosen_candidate is None
            else _serialize_candidate_payload(result.chosen_candidate)
        ),
        "candidates": [_serialize_candidate_payload(candidate) for candidate in result.candidates],
    }


async def _resolve_context(
    session: AsyncSession,
    *,
    signal_id: uuid.UUID | None,
    outcome_id: uuid.UUID,
    market_id: uuid.UUID,
    direction: str,
    estimated_probability: Decimal,
    market_price: Decimal,
    decision_at: datetime,
    baseline_target_size: Decimal,
    bankroll: Decimal,
) -> PolymarketExecutionContext | None:
    market = await session.get(Market, market_id)
    if market is None:
        return None

    outcome = await session.get(Outcome, outcome_id)
    if outcome is None:
        return None

    asset_dim = await _resolve_asset_dim(session, signal_id=signal_id, outcome=outcome)
    if asset_dim is None:
        return None

    recon_state = (
        await session.execute(
            select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == asset_dim.asset_id)
        )
    ).scalar_one_or_none()

    snapshot = None
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []
    reliable_book = False
    book_reason = "missing_recon_state"
    best_bid = recon_state.best_bid if recon_state is not None else None
    best_ask = recon_state.best_ask if recon_state is not None else None
    spread = recon_state.spread if recon_state is not None else None
    if recon_state is not None:
        snapshot, bids, asks, reliable_book, book_reason = await _rebuild_current_book(session, recon_state)
        if not reliable_book and not settings.polymarket_execution_policy_require_live_book and bids and asks:
            reliable_book = True
            book_reason = "reconstructed_book_without_live_requirement"

    param_history = await _latest_param_history(session, condition_id=asset_dim.condition_id, asset_id=asset_dim.asset_id)
    tick_size = None
    min_order_size = None
    fees_enabled = False
    taker_fee_rate = ZERO
    maker_fee_rate = ZERO
    fee_schedule_json = None
    if param_history is not None:
        tick_size = param_history.tick_size
        min_order_size = param_history.min_order_size
        fees_enabled = bool(param_history.fees_enabled)
        taker_fee_rate = _resolve_taker_fee_rate(param_history)
        maker_fee_rate = _resolve_maker_fee_rate(param_history)
        fee_schedule_json = param_history.fee_schedule_json or None
    if tick_size is None and recon_state is not None:
        tick_size = recon_state.expected_tick_size
    if tick_size is None and snapshot is not None:
        tick_size = snapshot.tick_size
    if min_order_size is None and snapshot is not None:
        min_order_size = snapshot.min_order_size

    snapshot_observed_at = _ensure_utc(snapshot.observed_at_local) if snapshot is not None else None
    snapshot_age_seconds = None
    if snapshot_observed_at is not None:
        snapshot_age_seconds = max(0, int((decision_at - snapshot_observed_at).total_seconds()))

    return PolymarketExecutionContext(
        signal_id=signal_id,
        market_id=market_id,
        outcome_id=outcome_id,
        direction=direction,
        estimated_probability=estimated_probability,
        market_price=market_price,
        baseline_target_size=_quantize(baseline_target_size, SIZE_Q) or ZERO,
        bankroll=bankroll,
        decision_at=_ensure_utc(decision_at) or datetime.now(timezone.utc),
        asset_id=asset_dim.asset_id,
        condition_id=asset_dim.condition_id,
        market_dim_id=asset_dim.market_dim_id,
        asset_dim_id=asset_dim.id,
        tick_size=tick_size,
        min_order_size=min_order_size,
        fees_enabled=fees_enabled,
        taker_fee_rate=taker_fee_rate,
        maker_fee_rate=maker_fee_rate,
        fee_schedule_json=fee_schedule_json,
        recon_state_id=recon_state.id if recon_state is not None else None,
        recon_status=recon_state.status if recon_state is not None else None,
        reliable_book=reliable_book,
        book_reason=book_reason,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        bids=bids,
        asks=asks,
        snapshot_id=snapshot.id if snapshot is not None else None,
        snapshot_source_kind=snapshot.source_kind if snapshot is not None else None,
        snapshot_observed_at=snapshot_observed_at,
        snapshot_age_seconds=snapshot_age_seconds,
        horizon_ms=settings.polymarket_execution_policy_default_horizon_ms,
        lookback_start=(_ensure_utc(decision_at) or datetime.now(timezone.utc))
        - timedelta(hours=settings.polymarket_execution_policy_passive_lookback_hours),
    )


async def _resolve_asset_dim(
    session: AsyncSession,
    *,
    signal_id: uuid.UUID | None,
    outcome: Outcome,
) -> PolymarketAssetDim | None:
    asset_dim = (
        await session.execute(
            select(PolymarketAssetDim).where(PolymarketAssetDim.outcome_id == outcome.id)
        )
    ).scalar_one_or_none()
    if asset_dim is not None:
        return asset_dim

    if outcome.token_id:
        asset_dim = (
            await session.execute(
                select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == outcome.token_id)
            )
        ).scalar_one_or_none()
        if asset_dim is not None:
            return asset_dim

    if signal_id is None:
        return None
    signal = await session.get(Signal, signal_id)
    if signal is None or signal.source_platform != "polymarket" or not signal.source_token_id:
        return None
    return (
        await session.execute(
            select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == signal.source_token_id)
        )
    ).scalar_one_or_none()


async def _latest_param_history(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str,
) -> PolymarketMarketParamHistory | None:
    asset_row = (
        await session.execute(
            select(PolymarketMarketParamHistory)
            .where(
                PolymarketMarketParamHistory.condition_id == condition_id,
                PolymarketMarketParamHistory.asset_id == asset_id,
            )
            .order_by(
                PolymarketMarketParamHistory.observed_at_local.desc(),
                PolymarketMarketParamHistory.id.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if asset_row is not None:
        return asset_row
    return (
        await session.execute(
            select(PolymarketMarketParamHistory)
            .where(
                PolymarketMarketParamHistory.condition_id == condition_id,
                PolymarketMarketParamHistory.asset_id.is_(None),
            )
            .order_by(
                PolymarketMarketParamHistory.observed_at_local.desc(),
                PolymarketMarketParamHistory.id.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()


def _resolve_taker_fee_rate(param_history: PolymarketMarketParamHistory) -> Decimal:
    schedule = param_history.fee_schedule_json or {}
    rate = _to_decimal(schedule.get("rate"))
    if rate is not None:
        return rate
    return _to_decimal(param_history.taker_base_fee) or ZERO


def _resolve_maker_fee_rate(param_history: PolymarketMarketParamHistory) -> Decimal:
    return _to_decimal(param_history.maker_base_fee) or ZERO


async def _rebuild_current_book(
    session: AsyncSession,
    recon_state: PolymarketBookReconState,
) -> tuple[PolymarketBookSnapshot | None, list[BookLevel], list[BookLevel], bool, str]:
    if recon_state.last_snapshot_id is None:
        return None, [], [], False, "missing_snapshot_boundary"
    snapshot = await session.get(PolymarketBookSnapshot, recon_state.last_snapshot_id)
    if snapshot is None:
        return None, [], [], False, "missing_snapshot_row"

    bid_map = _levels_to_map(snapshot.bids_json)
    ask_map = _levels_to_map(snapshot.asks_json)
    if snapshot.raw_event_id is None and recon_state.last_applied_delta_raw_event_id is not None:
        return snapshot, _map_to_levels(bid_map, side="bid"), _map_to_levels(ask_map, side="ask"), False, "snapshot_missing_raw_boundary"

    delta_query = select(PolymarketBookDelta).where(PolymarketBookDelta.asset_id == recon_state.asset_id)
    if snapshot.raw_event_id is not None:
        delta_query = delta_query.where(PolymarketBookDelta.raw_event_id > snapshot.raw_event_id)
    if recon_state.last_applied_delta_raw_event_id is not None:
        delta_query = delta_query.where(PolymarketBookDelta.raw_event_id <= recon_state.last_applied_delta_raw_event_id)
    rows = (
        await session.execute(
            delta_query.order_by(
                PolymarketBookDelta.raw_event_id.asc(),
                PolymarketBookDelta.delta_index.asc(),
            )
        )
    ).scalars().all()

    for row in rows:
        target = bid_map if str(row.side).upper() == "BUY" else ask_map
        if row.size <= ZERO:
            target.pop(row.price, None)
        else:
            target[row.price] = row.size

    bids = _map_to_levels(bid_map, side="bid")
    asks = _map_to_levels(ask_map, side="ask")
    rebuilt_best_bid = bids[0].yes_price if bids else None
    rebuilt_best_ask = asks[0].yes_price if asks else None
    reliable = (
        _prices_match(rebuilt_best_bid, recon_state.best_bid)
        and _prices_match(rebuilt_best_ask, recon_state.best_ask)
        and (recon_state.status == "live" or not settings.polymarket_execution_policy_require_live_book)
    )
    reason = "live_book_rebuilt"
    if recon_state.status != "live" and settings.polymarket_execution_policy_require_live_book:
        reason = f"recon_status_{recon_state.status}"
    elif not _prices_match(rebuilt_best_bid, recon_state.best_bid) or not _prices_match(rebuilt_best_ask, recon_state.best_ask):
        reason = "book_rebuild_mismatch"
    return snapshot, bids, asks, reliable, reason


def _levels_to_map(payload: Any) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    if not isinstance(payload, list):
        return result
    for level in payload:
        if isinstance(level, dict):
            price = _to_decimal(level.get("price"))
            size = _to_decimal(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _to_decimal(level[0])
            size = _to_decimal(level[1])
        else:
            continue
        if price is None or size is None or size <= ZERO:
            continue
        result[price] = size
    return result


def _map_to_levels(level_map: dict[Decimal, Decimal], *, side: str) -> list[BookLevel]:
    reverse = side == "bid"
    return [
        BookLevel(yes_price=price, size_shares=size)
        for price, size in sorted(level_map.items(), key=lambda item: item[0], reverse=reverse)
        if size > ZERO
    ]


def _prices_match(left: Decimal | None, right: Decimal | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    tolerance = Decimal(str(settings.polymarket_book_recon_bbo_tolerance))
    return abs(left - right) <= tolerance


def _walk_visible_book(
    *,
    direction: str,
    levels: list[BookLevel],
    target_size: Decimal,
    touch_entry_price: Decimal | None,
) -> BookWalkResult:
    remaining = target_size
    fillable_notional = ZERO
    fillable_shares = ZERO
    weighted_price = ZERO
    worst_price = None
    path: list[dict[str, Any]] = []
    for level in levels:
        entry_price = _entry_price_for_direction(direction, yes_price=level.yes_price)
        if entry_price <= ZERO:
            continue
        available_notional = (level.size_shares * entry_price).quantize(SIZE_Q)
        take_notional = min(available_notional, remaining)
        if take_notional <= ZERO:
            continue
        take_shares = (take_notional / entry_price).quantize(SHARE_Q)
        fillable_notional += take_notional
        fillable_shares += take_shares
        weighted_price += take_shares * entry_price
        worst_price = entry_price
        remaining -= take_notional
        path.append(
            _json_safe(
                {
                    "yes_price": level.yes_price,
                    "entry_price": entry_price,
                    "visible_shares": level.size_shares,
                    "taken_shares": take_shares,
                    "taken_notional": take_notional,
                }
            )
        )
        if remaining <= ZERO:
            break

    avg_entry_price = None
    if fillable_shares > ZERO:
        avg_entry_price = (weighted_price / fillable_shares).quantize(PRICE_Q)
    slippage_cost = ZERO
    slippage_bps = None
    if avg_entry_price is not None and touch_entry_price is not None and touch_entry_price > ZERO:
        slippage_per_share = max(avg_entry_price - touch_entry_price, ZERO)
        slippage_cost = (fillable_shares * slippage_per_share).quantize(PRICE_Q)
        slippage_bps = (((avg_entry_price - touch_entry_price) / touch_entry_price) * TEN_THOUSAND).quantize(PRICE_Q)
    return BookWalkResult(
        fillable_size=fillable_notional.quantize(SIZE_Q),
        fillable_shares=fillable_shares.quantize(SHARE_Q),
        avg_entry_price=avg_entry_price,
        worst_price=_quantize(worst_price, PRICE_Q),
        slippage_cost=slippage_cost,
        slippage_bps=slippage_bps,
        path=path,
    )


async def _evaluate_cross_now(
    session: AsyncSession,
    context: PolymarketExecutionContext,
) -> ActionCandidateEvaluation:
    candidate = ActionCandidateEvaluation(
        side=context.direction,
        action_type="cross_now",
        order_type_hint="FAK",
        decision_horizon_ms=context.horizon_ms,
        target_size=context.baseline_target_size,
        est_tick_size=context.tick_size,
        est_min_order_size=context.min_order_size,
    )
    if not context.reliable_book:
        candidate.invalid_reason = context.book_reason or "book_unreliable"
        candidate.details_json = {"reason": candidate.invalid_reason}
        return candidate

    touch_yes_price = context.best_ask if context.direction == "buy_yes" else context.best_bid
    touch_entry_price = (
        _entry_price_for_direction(context.direction, yes_price=touch_yes_price)
        if touch_yes_price is not None
        else None
    )
    levels = context.asks if context.direction == "buy_yes" else context.bids
    if not levels or touch_entry_price is None:
        candidate.invalid_reason = "no_visible_depth"
        candidate.details_json = {"reason": "no_visible_depth"}
        return candidate

    first_walk = _walk_visible_book(
        direction=context.direction,
        levels=levels,
        target_size=context.baseline_target_size,
        touch_entry_price=touch_entry_price,
    )
    entry_for_sizing = first_walk.avg_entry_price or touch_entry_price
    sizing = kelly_size_for_trade(
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        entry_price=entry_for_sizing,
        bankroll=context.bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )
    candidate.target_size = _quantize(sizing["recommended_size_usd"], SIZE_Q) or ZERO
    if candidate.target_size <= ZERO:
        candidate.invalid_reason = "size_zero"
        candidate.details_json = {"reason": "size_zero", "entry_price": entry_for_sizing}
        return candidate

    walk = _walk_visible_book(
        direction=context.direction,
        levels=levels,
        target_size=candidate.target_size,
        touch_entry_price=touch_entry_price,
    )
    candidate.est_fillable_size = walk.fillable_size
    candidate.est_avg_entry_price = walk.avg_entry_price
    candidate.est_worst_price = walk.worst_price
    candidate.est_slippage_cost = walk.slippage_cost
    if candidate.target_size > ZERO:
        candidate.est_fill_probability = (walk.fillable_size / candidate.target_size).quantize(Decimal("0.000001"))

    if walk.fillable_size <= ZERO or walk.avg_entry_price is None:
        candidate.invalid_reason = "no_visible_depth"
        candidate.details_json = {"reason": "no_visible_depth"}
        return candidate

    if walk.fillable_shares < (context.min_order_size or ZERO):
        candidate.invalid_reason = "below_min_order_size"
        candidate.details_json = {
            "reason": "below_min_order_size",
            "fillable_shares": walk.fillable_shares,
            "min_order_size": context.min_order_size,
        }
        return candidate

    min_fill_pct = Decimal(str(settings.shadow_execution_min_fill_pct))
    if candidate.est_fill_probability is not None and candidate.est_fill_probability < min_fill_pct:
        candidate.invalid_reason = "fillable_size_below_threshold"
        candidate.details_json = {
            "reason": "fillable_size_below_threshold",
            "fill_probability": candidate.est_fill_probability,
            "required_probability": min_fill_pct,
        }
        return candidate

    slippage_bps = walk.slippage_bps
    max_slippage_bps = Decimal(str(settings.polymarket_execution_policy_max_cross_slippage_bps))
    if slippage_bps is not None and slippage_bps > max_slippage_bps:
        candidate.invalid_reason = "cross_slippage_too_high"
        candidate.details_json = {
            "reason": "cross_slippage_too_high",
            "slippage_bps": slippage_bps,
            "max_slippage_bps": max_slippage_bps,
        }
        return candidate

    gross_ev = compute_directional_ev_full(
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        entry_price=walk.avg_entry_price,
    )
    taker_fee_total = _estimate_taker_fee_total(
        fillable_size=walk.fillable_size,
        entry_price=walk.avg_entry_price,
        fee_rate=context.taker_fee_rate,
        fees_enabled=context.fees_enabled,
    )
    taker_fee_per_share = (taker_fee_total / walk.fillable_shares).quantize(PRICE_Q) if walk.fillable_shares > ZERO else ZERO
    net_ev_per_share = (gross_ev["ev_per_share"] - taker_fee_per_share).quantize(PRICE_Q)
    net_ev_bps = _bps_from_per_share(net_ev_per_share, entry_price=walk.avg_entry_price)
    net_ev_total = (walk.fillable_shares * net_ev_per_share).quantize(PRICE_Q)

    candidate.est_taker_fee = taker_fee_total
    candidate.est_maker_fee = ZERO
    candidate.est_alpha_capture_bps = _bps_from_per_share(gross_ev["ev_per_share"], entry_price=walk.avg_entry_price)
    candidate.est_adverse_selection_bps = ZERO
    candidate.est_net_ev_bps = net_ev_bps
    candidate.est_net_ev_total = net_ev_total
    candidate.est_net_ev_per_share = net_ev_per_share
    candidate.valid = True
    candidate.details_json = _json_safe(
        {
            "slippage_bps": slippage_bps,
            "book_walk": walk.path,
            "touch_entry_price": touch_entry_price,
            "gross_ev_per_share": gross_ev["ev_per_share"],
            "gross_edge_pct": gross_ev["edge_pct"],
            "taker_fee_rate": context.taker_fee_rate,
            "taker_fee_total": taker_fee_total,
            "net_ev_per_share": net_ev_per_share,
        }
    )
    return candidate


async def _evaluate_post_best(
    session: AsyncSession,
    context: PolymarketExecutionContext,
) -> ActionCandidateEvaluation:
    return await _evaluate_passive_action(session, context, action_type="post_best")


async def _evaluate_step_ahead(
    session: AsyncSession,
    context: PolymarketExecutionContext,
) -> ActionCandidateEvaluation:
    return await _evaluate_passive_action(session, context, action_type="step_ahead")


async def _evaluate_passive_action(
    session: AsyncSession,
    context: PolymarketExecutionContext,
    *,
    action_type: str,
) -> ActionCandidateEvaluation:
    candidate = ActionCandidateEvaluation(
        side=context.direction,
        action_type=action_type,
        order_type_hint="post_only",
        decision_horizon_ms=context.horizon_ms,
        target_size=context.baseline_target_size,
        est_tick_size=context.tick_size,
        est_min_order_size=context.min_order_size,
    )
    if not context.reliable_book:
        candidate.invalid_reason = context.book_reason or "book_unreliable"
        candidate.details_json = {"reason": candidate.invalid_reason}
        return candidate
    if context.tick_size is None or context.tick_size <= ZERO:
        candidate.invalid_reason = "missing_tick_size"
        candidate.details_json = {"reason": "missing_tick_size"}
        return candidate
    if context.best_bid is None or context.best_ask is None or context.best_bid >= context.best_ask:
        candidate.invalid_reason = "invalid_top_of_book"
        candidate.details_json = {
            "reason": "invalid_top_of_book",
            "best_bid": context.best_bid,
            "best_ask": context.best_ask,
        }
        return candidate

    if context.direction == "buy_yes":
        target_yes_price = context.best_bid
        if action_type == "step_ahead":
            target_yes_price = (context.best_bid + context.tick_size).quantize(PRICE_Q)
            if target_yes_price >= context.best_ask:
                candidate.invalid_reason = "step_ahead_would_cross"
                candidate.details_json = {
                    "reason": "step_ahead_would_cross",
                    "candidate_price": target_yes_price,
                    "best_ask": context.best_ask,
                }
                return candidate
    else:
        target_yes_price = context.best_ask
        if action_type == "step_ahead":
            target_yes_price = (context.best_ask - context.tick_size).quantize(PRICE_Q)
            if target_yes_price <= context.best_bid:
                candidate.invalid_reason = "step_ahead_would_cross"
                candidate.details_json = {
                    "reason": "step_ahead_would_cross",
                    "candidate_price": target_yes_price,
                    "best_bid": context.best_bid,
                }
                return candidate

    if target_yes_price <= ZERO or target_yes_price >= ONE or not _is_tick_aligned(target_yes_price, context.tick_size):
        candidate.invalid_reason = "invalid_target_price"
        candidate.details_json = {
            "reason": "invalid_target_price",
            "target_yes_price": target_yes_price,
            "tick_size": context.tick_size,
        }
        return candidate

    entry_price = _entry_price_for_direction(context.direction, yes_price=target_yes_price)
    sizing = kelly_size_for_trade(
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        entry_price=entry_price,
        bankroll=context.bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )
    candidate.target_size = _quantize(sizing["recommended_size_usd"], SIZE_Q) or ZERO
    candidate.est_avg_entry_price = entry_price
    candidate.est_worst_price = entry_price
    if candidate.target_size <= ZERO:
        candidate.invalid_reason = "size_zero"
        candidate.details_json = {"reason": "size_zero", "entry_price": entry_price}
        return candidate

    target_shares = (candidate.target_size / entry_price).quantize(SHARE_Q) if entry_price > ZERO else ZERO
    if target_shares < (context.min_order_size or ZERO):
        candidate.invalid_reason = "below_min_order_size"
        candidate.details_json = {
            "reason": "below_min_order_size",
            "target_shares": target_shares,
            "min_order_size": context.min_order_size,
        }
        return candidate

    passive_summary = await _passive_label_summary(session, context=context, action_type=action_type)
    alpha_summary = await _alpha_label_summary(session, context=context)
    candidate.source_feature_row_id = passive_summary.source_feature_row_id or alpha_summary.source_feature_row_id
    candidate.source_label_summary_json = {
        "passive": passive_summary.as_json(),
        "alpha": alpha_summary.as_json(),
    }

    if passive_summary.row_count < settings.polymarket_execution_policy_passive_min_label_rows:
        candidate.invalid_reason = "passive_labels_insufficient"
        candidate.details_json = {
            "reason": "passive_labels_insufficient",
            "row_count": passive_summary.row_count,
            "required_rows": settings.polymarket_execution_policy_passive_min_label_rows,
        }
        return candidate

    fill_probability = passive_summary.fill_probability or ZERO
    expected_fillable_size = (candidate.target_size * fill_probability).quantize(SIZE_Q)
    expected_fillable_shares = (expected_fillable_size / entry_price).quantize(SHARE_Q) if entry_price > ZERO else ZERO
    if expected_fillable_shares < (context.min_order_size or ZERO):
        candidate.invalid_reason = "estimated_size_below_min_order_size"
        candidate.details_json = {
            "reason": "estimated_size_below_min_order_size",
            "expected_fillable_shares": expected_fillable_shares,
            "min_order_size": context.min_order_size,
        }
        return candidate

    if fill_probability < Decimal(str(settings.shadow_execution_min_fill_pct)):
        candidate.invalid_reason = "passive_fill_probability_too_low"
        candidate.details_json = {
            "reason": "passive_fill_probability_too_low",
            "fill_probability": fill_probability,
            "required_probability": Decimal(str(settings.shadow_execution_min_fill_pct)),
        }
        return candidate

    gross_ev = compute_directional_ev_full(
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        entry_price=entry_price,
    )
    delay_penalty_bps = (
        (alpha_summary.positive_directional_mean_bps or ZERO) * (ONE - fill_probability)
    ).quantize(PRICE_Q)
    delay_penalty_per_share = _per_share_from_bps(delay_penalty_bps, entry_price=entry_price) or ZERO
    adverse_selection_bps = passive_summary.adverse_selection_bps or ZERO
    adverse_selection_per_share = _per_share_from_bps(adverse_selection_bps, entry_price=entry_price) or ZERO
    net_ev_per_share = (
        gross_ev["ev_per_share"] - delay_penalty_per_share - adverse_selection_per_share
    ).quantize(PRICE_Q)
    net_ev_bps = _bps_from_per_share(net_ev_per_share, entry_price=entry_price)
    net_ev_total = (expected_fillable_shares * net_ev_per_share).quantize(PRICE_Q)

    candidate.est_fillable_size = expected_fillable_size
    candidate.est_fill_probability = fill_probability
    candidate.est_taker_fee = ZERO
    candidate.est_maker_fee = ZERO
    candidate.est_slippage_cost = ZERO
    candidate.est_alpha_capture_bps = (
        (_bps_from_per_share(gross_ev["ev_per_share"], entry_price=entry_price) or ZERO) - delay_penalty_bps
    ).quantize(PRICE_Q)
    candidate.est_adverse_selection_bps = adverse_selection_bps
    candidate.est_net_ev_bps = net_ev_bps
    candidate.est_net_ev_total = net_ev_total
    candidate.est_net_ev_per_share = net_ev_per_share
    candidate.valid = True
    candidate.details_json = _json_safe(
        {
            "target_yes_price": target_yes_price,
            "entry_price": entry_price,
            "gross_ev_per_share": gross_ev["ev_per_share"],
            "gross_edge_pct": gross_ev["edge_pct"],
            "delay_penalty_bps": delay_penalty_bps,
            "delay_penalty_per_share": delay_penalty_per_share,
            "net_ev_per_share": net_ev_per_share,
            "passive_summary": passive_summary.as_json(),
            "alpha_summary": alpha_summary.as_json(),
        }
    )
    return candidate


def _evaluate_skip(
    context: PolymarketExecutionContext,
    candidates: list[ActionCandidateEvaluation],
) -> ActionCandidateEvaluation:
    reason = "all_actions_negative_or_invalid"
    valid_non_skip = [candidate for candidate in candidates if candidate.valid and candidate.action_type != "skip"]
    if valid_non_skip:
        reason = "skip_beats_executable_actions"
    return ActionCandidateEvaluation(
        side=context.direction,
        action_type="skip",
        order_type_hint=None,
        decision_horizon_ms=context.horizon_ms,
        target_size=context.baseline_target_size,
        est_fillable_size=ZERO,
        est_fill_probability=ZERO,
        est_tick_size=context.tick_size,
        est_min_order_size=context.min_order_size,
        est_taker_fee=ZERO,
        est_maker_fee=ZERO,
        est_slippage_cost=ZERO,
        est_alpha_capture_bps=ZERO,
        est_adverse_selection_bps=ZERO,
        est_net_ev_bps=ZERO,
        est_net_ev_total=ZERO,
        est_net_ev_per_share=ZERO,
        valid=True,
        details_json={"reason": reason},
    )


def _choose_candidate(
    candidates: list[ActionCandidateEvaluation],
) -> tuple[ActionCandidateEvaluation | None, str]:
    if not candidates:
        return None, "no_candidates"
    skip_candidate = next((candidate for candidate in candidates if candidate.action_type == "skip"), None)
    valid_non_skip = [candidate for candidate in candidates if candidate.valid and candidate.action_type != "skip"]
    if not valid_non_skip:
        return skip_candidate, "all_non_skip_invalid"

    best = max(
        valid_non_skip,
        key=lambda candidate: (
            candidate.est_net_ev_total if candidate.est_net_ev_total is not None else Decimal("-999999999"),
            candidate.est_net_ev_bps if candidate.est_net_ev_bps is not None else Decimal("-999999999"),
        ),
    )
    min_net_ev_bps = Decimal(str(settings.polymarket_execution_policy_min_net_ev_bps))
    best_total = best.est_net_ev_total or ZERO
    best_bps = best.est_net_ev_bps or ZERO
    if best_total <= ZERO or best_bps < min_net_ev_bps:
        return skip_candidate, "skip_beats_negative_or_low_ev_actions"
    return best, "max_net_executable_ev"


async def _passive_label_summary(
    session: AsyncSession,
    *,
    context: PolymarketExecutionContext,
    action_type: str,
) -> PassiveLabelSummary:
    label_side = PASSIVE_LABEL_BY_DIRECTION[context.direction]
    rows = (
        await session.execute(
            select(PolymarketPassiveFillLabel)
            .where(
                PolymarketPassiveFillLabel.asset_id == context.asset_id,
                PolymarketPassiveFillLabel.condition_id == context.condition_id,
                PolymarketPassiveFillLabel.horizon_ms == context.horizon_ms,
                PolymarketPassiveFillLabel.side == label_side,
                PolymarketPassiveFillLabel.anchor_bucket_start_exchange >= context.lookback_start,
                PolymarketPassiveFillLabel.anchor_bucket_start_exchange <= context.decision_at,
            )
            .order_by(
                PolymarketPassiveFillLabel.anchor_bucket_start_exchange.desc(),
                PolymarketPassiveFillLabel.id.desc(),
            )
            .limit(1000)
        )
    ).scalars().all()
    row_count = len(rows)
    if row_count == 0:
        return PassiveLabelSummary(
            row_count=0,
            fill_probability=ZERO,
            touch_rate=ZERO,
            trade_through_rate=ZERO,
            improved_against_rate=ZERO,
            adverse_selection_bps=ZERO,
            source_feature_row_id=None,
        )

    touch_rate = Decimal(sum(1 for row in rows if row.touch_observed)) / Decimal(row_count)
    trade_through_rate = Decimal(sum(1 for row in rows if row.trade_through_observed)) / Decimal(row_count)
    improved_against_rate = Decimal(sum(1 for row in rows if row.best_price_improved_against_order)) / Decimal(row_count)
    base_fill_probability = ((trade_through_rate * Decimal("0.70")) + (touch_rate * Decimal("0.30"))).quantize(PRICE_Q)
    if action_type == "step_ahead":
        fill_probability = min(ONE, base_fill_probability + (improved_against_rate * Decimal("0.50")))
    else:
        fill_probability = max(ZERO, base_fill_probability - (improved_against_rate * Decimal("0.25")))
    adverse_values = [
        row.adverse_move_after_touch_bps
        for row in rows
        if row.adverse_move_after_touch_bps is not None and row.touch_observed
    ]
    adverse_selection_bps = _decimal_mean([value for value in adverse_values if value is not None]) or ZERO
    return PassiveLabelSummary(
        row_count=row_count,
        fill_probability=fill_probability.quantize(PRICE_Q),
        touch_rate=touch_rate.quantize(PRICE_Q),
        trade_through_rate=trade_through_rate.quantize(PRICE_Q),
        improved_against_rate=improved_against_rate.quantize(PRICE_Q),
        adverse_selection_bps=adverse_selection_bps.quantize(PRICE_Q),
        source_feature_row_id=rows[0].source_feature_row_id,
    )


async def _alpha_label_summary(
    session: AsyncSession,
    *,
    context: PolymarketExecutionContext,
) -> AlphaLabelSummary:
    rows = (
        await session.execute(
            select(PolymarketAlphaLabel)
            .where(
                PolymarketAlphaLabel.asset_id == context.asset_id,
                PolymarketAlphaLabel.condition_id == context.condition_id,
                PolymarketAlphaLabel.horizon_ms == context.horizon_ms,
                PolymarketAlphaLabel.anchor_bucket_start_exchange >= context.lookback_start,
                PolymarketAlphaLabel.anchor_bucket_start_exchange <= context.decision_at,
            )
            .order_by(
                PolymarketAlphaLabel.anchor_bucket_start_exchange.desc(),
                PolymarketAlphaLabel.id.desc(),
            )
            .limit(1000)
        )
    ).scalars().all()
    if not rows:
        return AlphaLabelSummary(
            row_count=0,
            directional_mean_bps=ZERO,
            positive_directional_mean_bps=ZERO,
            source_feature_row_id=None,
        )
    directional = [
        _directional_bps(row.mid_return_bps, direction=context.direction) or ZERO
        for row in rows
    ]
    positive_directional = [max(value, ZERO) for value in directional]
    return AlphaLabelSummary(
        row_count=len(rows),
        directional_mean_bps=(_decimal_mean(directional) or ZERO).quantize(PRICE_Q),
        positive_directional_mean_bps=(_decimal_mean(positive_directional) or ZERO).quantize(PRICE_Q),
        source_feature_row_id=rows[0].source_feature_row_id,
    )


def _estimate_taker_fee_total(
    *,
    fillable_size: Decimal,
    entry_price: Decimal,
    fee_rate: Decimal,
    fees_enabled: bool,
) -> Decimal:
    if not fees_enabled or fee_rate <= ZERO or fillable_size <= ZERO:
        return ZERO
    probability_term = (entry_price * (ONE - entry_price)).quantize(PRICE_Q)
    return (fillable_size * fee_rate * probability_term).quantize(PRICE_Q)


def _serialize_candidate_payload(candidate: ActionCandidateEvaluation) -> dict[str, Any]:
    return {
        "side": candidate.side,
        "action_type": candidate.action_type,
        "order_type_hint": candidate.order_type_hint,
        "decision_horizon_ms": candidate.decision_horizon_ms,
        "target_size": _serialize_decimal(_quantize(candidate.target_size, SIZE_Q)),
        "est_fillable_size": _serialize_decimal(_quantize(candidate.est_fillable_size, SIZE_Q)),
        "est_fill_probability": _serialize_decimal(_quantize(candidate.est_fill_probability, Decimal("0.000001"))),
        "est_avg_entry_price": _serialize_decimal(_quantize(candidate.est_avg_entry_price, PRICE_Q)),
        "est_worst_price": _serialize_decimal(_quantize(candidate.est_worst_price, PRICE_Q)),
        "est_tick_size": _serialize_decimal(_quantize(candidate.est_tick_size, PRICE_Q)),
        "est_min_order_size": _serialize_decimal(_quantize(candidate.est_min_order_size, Decimal("0.00000001"))),
        "est_taker_fee": _serialize_decimal(_quantize(candidate.est_taker_fee, PRICE_Q)),
        "est_maker_fee": _serialize_decimal(_quantize(candidate.est_maker_fee, PRICE_Q)),
        "est_slippage_cost": _serialize_decimal(_quantize(candidate.est_slippage_cost, PRICE_Q)),
        "est_alpha_capture_bps": _serialize_decimal(_quantize(candidate.est_alpha_capture_bps, PRICE_Q)),
        "est_adverse_selection_bps": _serialize_decimal(_quantize(candidate.est_adverse_selection_bps, PRICE_Q)),
        "est_net_ev_bps": _serialize_decimal(_quantize(candidate.est_net_ev_bps, PRICE_Q)),
        "est_net_ev_total": _serialize_decimal(_quantize(candidate.est_net_ev_total, PRICE_Q)),
        "valid": candidate.valid,
        "invalid_reason": candidate.invalid_reason,
        "source_feature_row_id": candidate.source_feature_row_id,
        "source_label_summary_json": _json_safe(candidate.source_label_summary_json),
        "details_json": _json_safe(candidate.details_json),
    }


def _serialize_candidate_row(row: PolymarketExecutionActionCandidate) -> dict[str, Any]:
    return {
        "id": row.id,
        "signal_id": str(row.signal_id) if row.signal_id is not None else None,
        "execution_decision_id": str(row.execution_decision_id) if row.execution_decision_id is not None else None,
        "market_dim_id": row.market_dim_id,
        "asset_dim_id": row.asset_dim_id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "outcome_id": str(row.outcome_id) if row.outcome_id is not None else None,
        "side": row.side,
        "action_type": row.action_type,
        "order_type_hint": row.order_type_hint,
        "decision_horizon_ms": row.decision_horizon_ms,
        "target_size": _serialize_decimal(row.target_size),
        "est_fillable_size": _serialize_decimal(row.est_fillable_size),
        "est_fill_probability": _serialize_decimal(row.est_fill_probability),
        "est_avg_entry_price": _serialize_decimal(row.est_avg_entry_price),
        "est_worst_price": _serialize_decimal(row.est_worst_price),
        "est_tick_size": _serialize_decimal(row.est_tick_size),
        "est_min_order_size": _serialize_decimal(row.est_min_order_size),
        "est_taker_fee": _serialize_decimal(row.est_taker_fee),
        "est_maker_fee": _serialize_decimal(row.est_maker_fee),
        "est_slippage_cost": _serialize_decimal(row.est_slippage_cost),
        "est_alpha_capture_bps": _serialize_decimal(row.est_alpha_capture_bps),
        "est_adverse_selection_bps": _serialize_decimal(row.est_adverse_selection_bps),
        "est_net_ev_bps": _serialize_decimal(row.est_net_ev_bps),
        "est_net_ev_total": _serialize_decimal(row.est_net_ev_total),
        "valid": row.valid,
        "invalid_reason": row.invalid_reason,
        "policy_version": row.policy_version,
        "source_recon_state_id": row.source_recon_state_id,
        "source_feature_row_id": row.source_feature_row_id,
        "source_label_summary_json": row.source_label_summary_json,
        "details_json": row.details_json,
        "decided_at": row.decided_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_execution_decision_row(
    decision: ExecutionDecision,
    candidate: PolymarketExecutionActionCandidate | None,
) -> dict[str, Any]:
    return {
        "id": str(decision.id),
        "signal_id": str(decision.signal_id),
        "strategy_run_id": str(decision.strategy_run_id),
        "decision_at": decision.decision_at,
        "decision_status": decision.decision_status,
        "reason_code": decision.reason_code,
        "action": decision.action,
        "chosen_action_type": decision.chosen_action_type,
        "chosen_order_type_hint": decision.chosen_order_type_hint,
        "chosen_target_price": _serialize_decimal(decision.chosen_target_price),
        "chosen_target_size": _serialize_decimal(decision.chosen_target_size),
        "chosen_est_fillable_size": _serialize_decimal(decision.chosen_est_fillable_size),
        "chosen_est_fill_probability": _serialize_decimal(decision.chosen_est_fill_probability),
        "chosen_est_net_ev_bps": _serialize_decimal(decision.chosen_est_net_ev_bps),
        "chosen_est_net_ev_total": _serialize_decimal(decision.chosen_est_net_ev_total),
        "chosen_est_fee": _serialize_decimal(decision.chosen_est_fee),
        "chosen_est_slippage": _serialize_decimal(decision.chosen_est_slippage),
        "chosen_policy_version": decision.chosen_policy_version,
        "chosen_action_candidate_id": decision.chosen_action_candidate_id,
        "decision_reason_json": decision.decision_reason_json,
        "candidate": _serialize_candidate_row(candidate) if candidate is not None else None,
    }
