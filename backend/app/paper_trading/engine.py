"""Paper trading engine: auto-open trades on EV-positive signals, resolve on market settlement.

This is the core simulation engine that tracks hypothetical P&L
without real money. Every EV-positive signal triggers a paper trade
using Kelly-recommended sizing, subject to risk management checks.
"""
from dataclasses import dataclass
import logging
import math
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import default_strategy_skip_label
from app.ingestion.polymarket_execution_policy import (
    evaluate_polymarket_execution_policy,
    persist_polymarket_execution_policy_result,
)
from app.ingestion.polymarket_risk_graph import assess_paper_trade_risk
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot
from app.signals.ev import compute_directional_ev_full, compute_ev_full
from app.signals.kelly import kelly_size_for_trade
from app.signals.risk import check_exposure

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")
REASON_LABELS = {
    "paper_trading_disabled": "Paper trading disabled",
    "already_recorded": "Already recorded in run",
    "already_open": "Already open",
    "size_zero": "Recommended size is zero",
    "execution_policy_skip": "Execution policy chose skip",
    "execution_policy_failure": "Execution policy evaluation failed",
    "execution_missing_orderbook_context": "Missing orderbook context",
    "execution_stale_orderbook_context": "Stale orderbook context",
    "execution_no_fill": "No fill available",
    "execution_partial_fill_below_minimum": "Partial fill below minimum",
    "execution_ev_below_threshold": "Executable EV below threshold",
    "execution_size_zero_after_fill_cap": "Executable size is zero after fill cap",
    "opened": "Trade opened",
}


@dataclass
class TradeOpenResult:
    trade: PaperTrade | None
    decision: str
    reason_code: str
    reason_label: str
    detail: str | None = None
    diagnostics: dict | None = None
    execution_decision: ExecutionDecision | None = None


@dataclass
class ExecutionDecisionBuildResult:
    execution_decision: ExecutionDecision | None
    decision: str
    reason_code: str
    reason_label: str
    detail: str | None = None
    diagnostics: dict | None = None
    direction: str | None = None
    ideal_entry_price: Decimal | None = None
    executable_entry_price: Decimal | None = None
    approved_size_usd: Decimal | None = None
    shares: Decimal | None = None
    shadow_execution: dict | None = None


@dataclass
class OrderbookContext:
    snapshot: OrderbookSnapshot | None
    snapshot_age_seconds: int | None
    snapshot_side: str | None
    usable: bool
    stale: bool = False
    missing_reason: str | None = None


def _risk_reason_code(reason: str) -> str:
    if reason == "event_cap_exceeded":
        return "risk_event_exposure"
    if reason == "entity_cap_exceeded":
        return "risk_entity_exposure"
    if reason == "conversion_group_cap_exceeded":
        return "risk_conversion_exposure"
    if reason == "inventory_toxicity_exceeded":
        return "risk_inventory_toxicity"
    if reason.startswith("Total exposure limit reached"):
        return "risk_total_exposure"
    if reason.startswith("Cluster exposure limit reached"):
        return "risk_cluster_exposure"
    return "risk_rejected"


def _risk_reason_label(reason_code: str) -> str:
    labels = {
        "risk_total_exposure": "Total exposure limit reached",
        "risk_cluster_exposure": "Cluster exposure limit reached",
        "risk_event_exposure": "Event exposure cap reached",
        "risk_entity_exposure": "Entity exposure cap reached",
        "risk_conversion_exposure": "Conversion group exposure cap reached",
        "risk_inventory_toxicity": "Inventory toxicity threshold reached",
        "risk_rejected": "Risk rejected",
    }
    return labels.get(reason_code, reason_code.replace("_", " "))


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _reason_label(reason_code: str) -> str:
    if reason_code in REASON_LABELS:
        return REASON_LABELS[reason_code]
    if reason_code.startswith("risk_"):
        return _risk_reason_label(reason_code)
    return default_strategy_skip_label(reason_code) or reason_code.replace("_", " ")


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


def _net_expected_pnl_usd(
    ev_per_share: Decimal | None,
    size_usd: Decimal | None,
    entry_price: Decimal | None,
) -> Decimal | None:
    if ev_per_share is None or size_usd is None or entry_price is None or entry_price <= ZERO:
        return None
    shares = (size_usd / entry_price).quantize(Decimal("0.0001"))
    return (shares * ev_per_share).quantize(Decimal("0.00000001"))


async def get_portfolio_state(session: AsyncSession) -> dict:
    """Get current portfolio state: open positions, P&L, exposure."""
    return await _get_portfolio_state(session)


async def _get_portfolio_state(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None = None,
) -> dict:
    """Get current portfolio state: open positions, P&L, exposure."""
    # Open positions
    open_query = select(PaperTrade).where(PaperTrade.status == "open")
    resolved_query = select(
        func.count(PaperTrade.id).label("total_trades"),
        func.sum(PaperTrade.pnl).label("cumulative_pnl"),
        func.count(PaperTrade.id).filter(PaperTrade.pnl > 0).label("wins"),
        func.count(PaperTrade.id).filter(PaperTrade.pnl <= 0).label("losses"),
    ).where(PaperTrade.status == "resolved")

    if strategy_run_id is not None:
        open_query = open_query.where(PaperTrade.strategy_run_id == strategy_run_id)
        resolved_query = resolved_query.where(PaperTrade.strategy_run_id == strategy_run_id)

    result = await session.execute(
        open_query.order_by(PaperTrade.opened_at.desc())
    )
    open_trades = result.scalars().all()

    # Resolved trades
    result = await session.execute(resolved_query)
    stats = result.one()

    total_trades = stats.total_trades or 0
    cumulative_pnl = stats.cumulative_pnl or ZERO
    wins = stats.wins or 0
    losses = stats.losses or 0

    open_exposure = sum((t.size_usd for t in open_trades), ZERO)

    return {
        "open_trades": open_trades,
        "open_exposure": open_exposure,
        "total_resolved": total_trades,
        "cumulative_pnl": cumulative_pnl,
        "wins": wins,
        "losses": losses,
        "win_rate": Decimal(str(wins / total_trades)).quantize(Decimal("0.0001")) if total_trades > 0 else ZERO,
    }


async def build_execution_decision(
    session: AsyncSession,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal | None,
    market_price: Decimal | None,
    market_question: str = "",
    fired_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
    precheck_reason_code: str | None = None,
    precheck_reason_label: str | None = None,
) -> ExecutionDecisionBuildResult:
    """Build and persist the Phase 0 execution decision before a trade opens."""

    decision_at = _ensure_utc(fired_at) or datetime.now(timezone.utc)

    async def finish(
        *,
        decision: str,
        reason_code: str,
        reason_label: str | None = None,
        detail: str | None = None,
        diagnostics: dict | None = None,
        action: str | None = None,
        direction: str | None = None,
        ideal_entry_price: Decimal | None = None,
        executable_entry_price: Decimal | None = None,
        requested_size_usd: Decimal | None = None,
        fillable_size_usd: Decimal | None = None,
        fill_probability: Decimal | None = None,
        net_ev_per_share: Decimal | None = None,
        net_expected_pnl_usd: Decimal | None = None,
        missing_orderbook_context: bool = False,
        stale_orderbook_context: bool = False,
        liquidity_constrained: bool = False,
        fill_status: str | None = None,
        ideal_ev: dict | None = None,
        provisional_sizing: dict | None = None,
        shadow_execution: dict | None = None,
        executable_ev: dict | None = None,
        executable_sizing: dict | None = None,
        fill_capped_size_usd: Decimal | None = None,
        risk_result: dict | None = None,
        approved_size_usd: Decimal | None = None,
        shares: Decimal | None = None,
        chosen_action_type: str | None = None,
        chosen_order_type_hint: str | None = None,
        chosen_target_price: Decimal | None = None,
        chosen_target_size: Decimal | None = None,
        chosen_est_fillable_size: Decimal | None = None,
        chosen_est_fill_probability: Decimal | None = None,
        chosen_est_net_ev_bps: Decimal | None = None,
        chosen_est_net_ev_total: Decimal | None = None,
        chosen_est_fee: Decimal | None = None,
        chosen_est_slippage: Decimal | None = None,
        chosen_policy_version: str | None = None,
        decision_reason_json: dict | None = None,
        polymarket_policy_result=None,
    ) -> ExecutionDecisionBuildResult:
        label = reason_label or _reason_label(reason_code)
        execution_decision = None
        if strategy_run_id is not None:
            details_payload = _json_safe(
                {
                    "reason_label": label,
                    "detail": detail,
                    "market_id": market_id,
                    "market_question": market_question,
                    "estimated_probability": estimated_probability,
                    "market_price": market_price,
                    "ideal_ev": ideal_ev,
                    "provisional_sizing": provisional_sizing,
                    "shadow_execution": shadow_execution["details"] if shadow_execution is not None else None,
                    "executable_ev": executable_ev,
                    "executable_sizing": executable_sizing,
                    "fill_capped_size_usd": fill_capped_size_usd,
                    "approved_size_usd": approved_size_usd,
                    "shares": shares,
                    "risk_result": risk_result,
                    "diagnostics": diagnostics,
                    "chosen_action_type": chosen_action_type,
                    "chosen_order_type_hint": chosen_order_type_hint,
                    "chosen_target_price": chosen_target_price,
                    "chosen_target_size": chosen_target_size,
                    "chosen_est_fillable_size": chosen_est_fillable_size,
                    "chosen_est_fill_probability": chosen_est_fill_probability,
                    "chosen_est_net_ev_bps": chosen_est_net_ev_bps,
                    "chosen_est_net_ev_total": chosen_est_net_ev_total,
                    "chosen_est_fee": chosen_est_fee,
                    "chosen_est_slippage": chosen_est_slippage,
                    "chosen_policy_version": chosen_policy_version,
                    "decision_reason_json": decision_reason_json,
                }
            )
            execution_decision = ExecutionDecision(
                id=uuid.uuid4(),
                signal_id=signal_id,
                strategy_run_id=strategy_run_id,
                decision_at=decision_at,
                decision_status=decision,
                action=action or ("cross" if decision == "opened" else "skip"),
                direction=direction,
                ideal_entry_price=ideal_entry_price,
                executable_entry_price=executable_entry_price,
                requested_size_usd=requested_size_usd,
                fillable_size_usd=fillable_size_usd,
                fill_probability=fill_probability,
                net_ev_per_share=net_ev_per_share,
                net_expected_pnl_usd=net_expected_pnl_usd,
                missing_orderbook_context=missing_orderbook_context,
                stale_orderbook_context=stale_orderbook_context,
                liquidity_constrained=liquidity_constrained,
                fill_status=fill_status,
                reason_code=reason_code,
                chosen_action_type=chosen_action_type,
                chosen_order_type_hint=chosen_order_type_hint,
                chosen_target_price=chosen_target_price,
                chosen_target_size=chosen_target_size,
                chosen_est_fillable_size=chosen_est_fillable_size,
                chosen_est_fill_probability=chosen_est_fill_probability,
                chosen_est_net_ev_bps=chosen_est_net_ev_bps,
                chosen_est_net_ev_total=chosen_est_net_ev_total,
                chosen_est_fee=chosen_est_fee,
                chosen_est_slippage=chosen_est_slippage,
                chosen_policy_version=chosen_policy_version,
                decision_reason_json=_json_safe(decision_reason_json),
                details=details_payload,
            )
            session.add(execution_decision)
            await session.flush()
            if polymarket_policy_result is not None:
                await persist_polymarket_execution_policy_result(
                    session,
                    result=polymarket_policy_result,
                    execution_decision=execution_decision,
                )

        return ExecutionDecisionBuildResult(
            execution_decision=execution_decision,
            decision=decision,
            reason_code=reason_code,
            reason_label=label,
            detail=detail,
            diagnostics=diagnostics,
            direction=direction,
            ideal_entry_price=ideal_entry_price,
            executable_entry_price=executable_entry_price,
            approved_size_usd=approved_size_usd,
            shares=shares,
            shadow_execution=shadow_execution,
        )

    if strategy_run_id is not None:
        existing_decision_result = await session.execute(
            select(ExecutionDecision).where(
                ExecutionDecision.signal_id == signal_id,
                ExecutionDecision.strategy_run_id == strategy_run_id,
            )
        )
        existing_decision = existing_decision_result.scalars().first()
        if existing_decision is not None:
            return ExecutionDecisionBuildResult(
                execution_decision=existing_decision,
                decision="skipped",
                reason_code="already_recorded",
                reason_label=_reason_label("already_recorded"),
                detail=f"Signal already has an execution decision in strategy run ({existing_decision.id})",
                diagnostics={
                    "existing_execution_decision_id": str(existing_decision.id),
                    "existing_decision_status": existing_decision.decision_status,
                    "existing_reason_code": existing_decision.reason_code,
                },
            )

    existing_query = select(PaperTrade.id, PaperTrade.status).where(PaperTrade.signal_id == signal_id)
    if strategy_run_id is not None:
        existing_query = existing_query.where(PaperTrade.strategy_run_id == strategy_run_id)
    else:
        existing_query = existing_query.where(PaperTrade.status == "open")
    existing_query = existing_query.order_by(PaperTrade.opened_at.desc()).limit(1)
    existing_trade = (await session.execute(existing_query)).first()
    if existing_trade is not None:
        existing_trade_id, existing_trade_status = existing_trade
        if strategy_run_id is not None:
            return ExecutionDecisionBuildResult(
                execution_decision=None,
                decision="skipped",
                reason_code="already_recorded",
                reason_label=_reason_label("already_recorded"),
                detail=(
                    f"Signal already has a {existing_trade_status} paper trade in "
                    f"strategy run ({existing_trade_id})"
                ),
            )
        return ExecutionDecisionBuildResult(
            execution_decision=None,
            decision="skipped",
            reason_code="already_open",
            reason_label=_reason_label("already_open"),
            detail=f"Signal already has an open paper trade ({existing_trade_id})",
        )

    if not settings.paper_trading_enabled:
        return await finish(decision="skipped", reason_code="paper_trading_disabled")

    derived_precheck_reason_code = precheck_reason_code
    derived_precheck_reason_label = precheck_reason_label
    if derived_precheck_reason_code is None:
        if outcome_id is None:
            derived_precheck_reason_code = "missing_outcome_id"
        elif estimated_probability is None:
            derived_precheck_reason_code = "missing_probability"
        elif market_price is None:
            derived_precheck_reason_code = "missing_market_price"

    if derived_precheck_reason_code is not None:
        return await finish(
            decision="skipped",
            reason_code=derived_precheck_reason_code,
            reason_label=derived_precheck_reason_label,
        )

    bankroll = Decimal(str(settings.default_bankroll))
    min_ev_threshold = Decimal(str(settings.min_ev_threshold))

    ideal_ev = compute_ev_full(estimated_probability, market_price)
    if ideal_ev["ev_per_share"] < min_ev_threshold:
        return await finish(
            decision="skipped",
            reason_code="ev_below_threshold",
            detail=f"Directional EV {ideal_ev['ev_per_share']} below threshold {settings.min_ev_threshold}",
            diagnostics={
                "direction": ideal_ev["direction"],
                "ev_per_share": str(ideal_ev["ev_per_share"]),
                "edge_pct": str(ideal_ev["edge_pct"]),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            net_ev_per_share=ideal_ev["ev_per_share"],
            ideal_ev=ideal_ev,
        )

    provisional_sizing = kelly_size_for_trade(
        direction=ideal_ev["direction"],
        estimated_probability=estimated_probability,
        entry_price=ideal_ev["entry_price"],
        bankroll=bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )
    if provisional_sizing["recommended_size_usd"] <= ZERO:
        return await finish(
            decision="skipped",
            reason_code="size_zero",
            diagnostics={
                "direction": provisional_sizing["direction"],
                "kelly_full": str(provisional_sizing["kelly_full"]),
                "kelly_used": str(provisional_sizing["kelly_used"]),
                "recommended_size_usd": str(provisional_sizing["recommended_size_usd"]),
                "entry_price": str(provisional_sizing["entry_price"]),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            requested_size_usd=provisional_sizing["recommended_size_usd"],
            net_ev_per_share=ideal_ev["ev_per_share"],
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
        )

    if settings.polymarket_execution_policy_enabled:
        try:
            policy_result = await evaluate_polymarket_execution_policy(
                session,
                signal_id=signal_id,
                outcome_id=outcome_id,
                market_id=market_id,
                direction=ideal_ev["direction"],
                estimated_probability=estimated_probability,
                market_price=market_price,
                decision_at=decision_at,
                baseline_target_size=provisional_sizing["recommended_size_usd"],
                bankroll=bankroll,
            )
        except Exception as exc:
            logger.exception("Polymarket execution policy evaluation failed for signal %s", signal_id)
            return await finish(
                decision="skipped",
                reason_code="execution_policy_failure",
                detail=str(exc),
                diagnostics={
                    "direction": ideal_ev["direction"],
                    "recommended_size_usd": str(provisional_sizing["recommended_size_usd"]),
                },
                direction=ideal_ev["direction"],
                ideal_entry_price=ideal_ev["entry_price"],
                requested_size_usd=provisional_sizing["recommended_size_usd"],
                net_ev_per_share=ideal_ev["ev_per_share"],
                ideal_ev=ideal_ev,
                provisional_sizing=provisional_sizing,
            )

        if policy_result.applicable and policy_result.chosen_candidate is not None:
            chosen = policy_result.chosen_candidate
            chosen_fee = (chosen.est_taker_fee or ZERO) + (chosen.est_maker_fee or ZERO)
            chosen_slippage = chosen.est_slippage_cost or ZERO
            decision_reason_json = policy_result.choice_payload()
            shadow_execution = policy_result.shadow_execution()
            shadow_details = shadow_execution["details"]
            executable_entry_price = chosen.est_avg_entry_price
            fillable_size_usd = chosen.est_fillable_size or ZERO
            fill_probability = chosen.est_fill_probability or ZERO
            executable_ev = {
                "direction": ideal_ev["direction"],
                "entry_price": executable_entry_price,
                "ev_per_share": chosen.est_net_ev_per_share,
                "edge_pct": (
                    str(((chosen.est_net_ev_bps or ZERO) / Decimal("10000")).quantize(Decimal("0.00000001")))
                    if chosen.est_net_ev_bps is not None
                    else None
                ),
            }
            executable_sizing = (
                kelly_size_for_trade(
                    direction=ideal_ev["direction"],
                    estimated_probability=estimated_probability,
                    entry_price=executable_entry_price,
                    bankroll=bankroll,
                    kelly_fraction=Decimal(str(settings.kelly_multiplier)),
                    max_position_pct=Decimal(str(settings.max_single_position_pct)),
                )
                if executable_entry_price is not None and executable_entry_price > ZERO
                else {
                    "direction": ideal_ev["direction"],
                    "entry_price": executable_entry_price,
                    "kelly_full": ZERO,
                    "kelly_used": ZERO,
                    "recommended_size_usd": ZERO,
                }
            )

            if chosen.action_type == "skip":
                return await finish(
                    decision="skipped",
                    reason_code="execution_policy_skip",
                    detail=f"Execution policy chose skip ({policy_result.chosen_reason})",
                    diagnostics={
                        "direction": ideal_ev["direction"],
                        "chosen_action_type": chosen.action_type,
                        "chosen_order_type_hint": chosen.order_type_hint,
                        "chosen_reason": policy_result.chosen_reason,
                        "decision_reason_json": decision_reason_json,
                    },
                    direction=ideal_ev["direction"],
                    ideal_entry_price=ideal_ev["entry_price"],
                    executable_entry_price=executable_entry_price,
                    requested_size_usd=chosen.target_size,
                    fillable_size_usd=fillable_size_usd,
                    fill_probability=fill_probability,
                    net_ev_per_share=chosen.est_net_ev_per_share,
                    net_expected_pnl_usd=chosen.est_net_ev_total,
                    missing_orderbook_context=bool(shadow_details.get("missing_orderbook_context")),
                    stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
                    liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
                    fill_status=shadow_details.get("fill_status"),
                    ideal_ev=ideal_ev,
                    provisional_sizing=provisional_sizing,
                    shadow_execution=shadow_execution,
                    executable_ev=executable_ev,
                    executable_sizing=executable_sizing,
                    fill_capped_size_usd=fillable_size_usd,
                    chosen_action_type=chosen.action_type,
                    chosen_order_type_hint=chosen.order_type_hint,
                    chosen_target_price=chosen.est_avg_entry_price,
                    chosen_target_size=chosen.target_size,
                    chosen_est_fillable_size=chosen.est_fillable_size,
                    chosen_est_fill_probability=chosen.est_fill_probability,
                    chosen_est_net_ev_bps=chosen.est_net_ev_bps,
                    chosen_est_net_ev_total=chosen.est_net_ev_total,
                    chosen_est_fee=chosen_fee,
                    chosen_est_slippage=chosen_slippage,
                    chosen_policy_version=policy_result.policy_version,
                    decision_reason_json=decision_reason_json,
                    polymarket_policy_result=policy_result,
                )

            fill_capped_size_usd = min(executable_sizing["recommended_size_usd"], fillable_size_usd).quantize(Decimal("0.01"))
            if executable_entry_price is None or executable_entry_price <= ZERO or fill_capped_size_usd <= ZERO:
                return await finish(
                    decision="skipped",
                    reason_code="execution_size_zero_after_fill_cap",
                    detail="Executable size became zero after applying the fill cap",
                    diagnostics={
                        "direction": ideal_ev["direction"],
                        "chosen_action_type": chosen.action_type,
                        "chosen_reason": policy_result.chosen_reason,
                        "executable_entry_price": str(executable_entry_price) if executable_entry_price is not None else None,
                        "fillable_size_usd": str(fillable_size_usd),
                    },
                    direction=ideal_ev["direction"],
                    ideal_entry_price=ideal_ev["entry_price"],
                    executable_entry_price=executable_entry_price,
                    requested_size_usd=chosen.target_size,
                    fillable_size_usd=fillable_size_usd,
                    fill_probability=fill_probability,
                    net_ev_per_share=chosen.est_net_ev_per_share,
                    net_expected_pnl_usd=chosen.est_net_ev_total,
                    missing_orderbook_context=bool(shadow_details.get("missing_orderbook_context")),
                    stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
                    liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
                    fill_status=shadow_details.get("fill_status"),
                    ideal_ev=ideal_ev,
                    provisional_sizing=provisional_sizing,
                    shadow_execution=shadow_execution,
                    executable_ev=executable_ev,
                    executable_sizing=executable_sizing,
                    fill_capped_size_usd=fill_capped_size_usd,
                    chosen_action_type=chosen.action_type,
                    chosen_order_type_hint=chosen.order_type_hint,
                    chosen_target_price=chosen.est_avg_entry_price,
                    chosen_target_size=chosen.target_size,
                    chosen_est_fillable_size=chosen.est_fillable_size,
                    chosen_est_fill_probability=chosen.est_fill_probability,
                    chosen_est_net_ev_bps=chosen.est_net_ev_bps,
                    chosen_est_net_ev_total=chosen.est_net_ev_total,
                    chosen_est_fee=chosen_fee,
                    chosen_est_slippage=chosen_slippage,
                    chosen_policy_version=policy_result.policy_version,
                    decision_reason_json=decision_reason_json,
                    polymarket_policy_result=policy_result,
                )

            portfolio = await _get_portfolio_state(session, strategy_run_id=strategy_run_id)
            open_positions = [
                {
                    "size_usd": t.size_usd,
                    "market_question": (t.details or {}).get("market_question", ""),
                    "outcome_id": str(t.outcome_id),
                }
                for t in portfolio["open_trades"]
            ]
            peak_bankroll = bankroll
            if portfolio["cumulative_pnl"] > ZERO:
                peak_bankroll = bankroll + portfolio["cumulative_pnl"]

            risk_result = await assess_paper_trade_risk(
                session,
                outcome_id=outcome_id,
                market_id=market_id,
                direction=ideal_ev["direction"],
                proposed_notional_usd=fill_capped_size_usd,
            )
            if risk_result is None:
                risk_result = check_exposure(
                    open_positions=open_positions,
                    new_trade={
                        "size_usd": fill_capped_size_usd,
                        "market_question": market_question,
                        "outcome_id": str(outcome_id),
                    },
                    bankroll=bankroll,
                    max_total_pct=Decimal(str(settings.max_total_exposure_pct)),
                    max_cluster_pct=Decimal(str(settings.max_cluster_exposure_pct)),
                    drawdown_breaker_pct=Decimal(str(settings.drawdown_circuit_breaker_pct)),
                    peak_bankroll=peak_bankroll,
                    cumulative_pnl=portfolio["cumulative_pnl"],
                )
            if not risk_result["approved"]:
                logger.info(
                    "Paper trade rejected by risk check: %s (signal %s)",
                    risk_result["reason"], signal_id,
                )
                reason_code = _risk_reason_code(risk_result["reason"])
                return await finish(
                    decision="skipped",
                    reason_code=reason_code,
                    detail=risk_result["reason"],
                    diagnostics={
                        "direction": ideal_ev["direction"],
                        "chosen_action_type": chosen.action_type,
                        "chosen_order_type_hint": chosen.order_type_hint,
                        "chosen_reason": policy_result.chosen_reason,
                        "recommended_size_usd": str(executable_sizing["recommended_size_usd"]),
                        "fillable_size_usd": str(fillable_size_usd),
                        "fill_capped_size_usd": str(fill_capped_size_usd),
                        "approved_size_usd": str(risk_result["approved_size_usd"]),
                        "risk_reason": risk_result["reason"],
                        "drawdown_active": risk_result["drawdown_active"],
                    },
                    direction=ideal_ev["direction"],
                    ideal_entry_price=ideal_ev["entry_price"],
                    executable_entry_price=executable_entry_price,
                    requested_size_usd=chosen.target_size,
                    fillable_size_usd=fillable_size_usd,
                    fill_probability=fill_probability,
                    net_ev_per_share=chosen.est_net_ev_per_share,
                    net_expected_pnl_usd=_net_expected_pnl_usd(
                        chosen.est_net_ev_per_share,
                        fill_capped_size_usd,
                        executable_entry_price,
                    ),
                    missing_orderbook_context=bool(shadow_details.get("missing_orderbook_context")),
                    stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
                    liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
                    fill_status=shadow_details.get("fill_status"),
                    ideal_ev=ideal_ev,
                    provisional_sizing=provisional_sizing,
                    shadow_execution=shadow_execution,
                    executable_ev=executable_ev,
                    executable_sizing=executable_sizing,
                    fill_capped_size_usd=fill_capped_size_usd,
                    risk_result=risk_result,
                    chosen_action_type=chosen.action_type,
                    chosen_order_type_hint=chosen.order_type_hint,
                    chosen_target_price=chosen.est_avg_entry_price,
                    chosen_target_size=chosen.target_size,
                    chosen_est_fillable_size=chosen.est_fillable_size,
                    chosen_est_fill_probability=chosen.est_fill_probability,
                    chosen_est_net_ev_bps=chosen.est_net_ev_bps,
                    chosen_est_net_ev_total=chosen.est_net_ev_total,
                    chosen_est_fee=chosen_fee,
                    chosen_est_slippage=chosen_slippage,
                    chosen_policy_version=policy_result.policy_version,
                    decision_reason_json=decision_reason_json,
                    polymarket_policy_result=policy_result,
                )

            approved_size_usd = risk_result["approved_size_usd"]
            shares = (
                (approved_size_usd / executable_entry_price).quantize(Decimal("0.0001"))
                if executable_entry_price > ZERO
                else ZERO
            )
            return await finish(
                decision="opened",
                reason_code="opened",
                diagnostics={
                    "direction": ideal_ev["direction"],
                    "ideal_entry_price": str(ideal_ev["entry_price"]),
                    "ideal_ev_per_share": str(ideal_ev["ev_per_share"]),
                    "executable_entry_price": str(executable_entry_price),
                    "ev_per_share": str(chosen.est_net_ev_per_share),
                    "edge_pct": (
                        str(((chosen.est_net_ev_bps or ZERO) / Decimal("10000")).quantize(Decimal("0.00000001")))
                        if chosen.est_net_ev_bps is not None
                        else None
                    ),
                    "kelly_full": str(executable_sizing["kelly_full"]),
                    "kelly_used": str(executable_sizing["kelly_used"]),
                    "recommended_size_usd": str(executable_sizing["recommended_size_usd"]),
                    "fillable_size_usd": str(fillable_size_usd),
                    "fill_capped_size_usd": str(fill_capped_size_usd),
                    "approved_size_usd": str(approved_size_usd),
                    "drawdown_active": risk_result["drawdown_active"],
                    "shadow_entry_price": str(executable_entry_price),
                    "liquidity_constrained": shadow_details.get("liquidity_constrained"),
                    "missing_orderbook_context": shadow_details.get("missing_orderbook_context"),
                    "stale_orderbook_context": shadow_details.get("stale_orderbook_context"),
                    "shadow_fill_status": shadow_details.get("fill_status"),
                    "shadow_fill_pct": shadow_details.get("fill_pct"),
                    "chosen_action_type": chosen.action_type,
                    "chosen_order_type_hint": chosen.order_type_hint,
                    "chosen_reason": policy_result.chosen_reason,
                    "decision_reason_json": decision_reason_json,
                },
                action=chosen.action_type,
                direction=ideal_ev["direction"],
                ideal_entry_price=ideal_ev["entry_price"],
                executable_entry_price=executable_entry_price,
                requested_size_usd=chosen.target_size,
                fillable_size_usd=fillable_size_usd,
                fill_probability=fill_probability,
                net_ev_per_share=chosen.est_net_ev_per_share,
                net_expected_pnl_usd=_net_expected_pnl_usd(
                    chosen.est_net_ev_per_share,
                    approved_size_usd,
                    executable_entry_price,
                ),
                missing_orderbook_context=bool(shadow_details.get("missing_orderbook_context")),
                stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
                liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
                fill_status=shadow_details.get("fill_status"),
                ideal_ev=ideal_ev,
                provisional_sizing=provisional_sizing,
                shadow_execution=shadow_execution,
                executable_ev=executable_ev,
                executable_sizing=executable_sizing,
                fill_capped_size_usd=fill_capped_size_usd,
                risk_result=risk_result,
                approved_size_usd=approved_size_usd,
                shares=shares,
                chosen_action_type=chosen.action_type,
                chosen_order_type_hint=chosen.order_type_hint,
                chosen_target_price=chosen.est_avg_entry_price,
                chosen_target_size=chosen.target_size,
                chosen_est_fillable_size=chosen.est_fillable_size,
                chosen_est_fill_probability=chosen.est_fill_probability,
                chosen_est_net_ev_bps=chosen.est_net_ev_bps,
                chosen_est_net_ev_total=chosen.est_net_ev_total,
                chosen_est_fee=chosen_fee,
                chosen_est_slippage=chosen_slippage,
                chosen_policy_version=policy_result.policy_version,
                decision_reason_json=decision_reason_json,
                polymarket_policy_result=policy_result,
            )

    shadow_execution = await _build_shadow_execution(
        session=session,
        outcome_id=outcome_id,
        direction=ideal_ev["direction"],
        approved_size=provisional_sizing["recommended_size_usd"],
        ideal_entry_price=ideal_ev["entry_price"],
        fired_at=fired_at,
    )
    shadow_details = shadow_execution["details"]
    fillable_size_usd = _parse_decimal(shadow_details.get("filled_size_usd")) or ZERO
    fill_probability = _parse_decimal(shadow_details.get("fill_pct"))

    if shadow_details.get("missing_orderbook_context") is True:
        reason_code = (
            "execution_stale_orderbook_context"
            if shadow_details.get("stale_orderbook_context") is True
            else "execution_missing_orderbook_context"
        )
        return await finish(
            decision="skipped",
            reason_code=reason_code,
            detail=f"Executable context unavailable ({shadow_details.get('fill_reason')})",
            diagnostics={
                "direction": ideal_ev["direction"],
                "ev_per_share": str(ideal_ev["ev_per_share"]),
                "recommended_size_usd": str(provisional_sizing["recommended_size_usd"]),
                "shadow_fill_status": shadow_details.get("fill_status"),
                "shadow_fill_reason": shadow_details.get("fill_reason"),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            requested_size_usd=provisional_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=ideal_ev["ev_per_share"],
            missing_orderbook_context=True,
            stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
        )

    if shadow_details.get("fill_reason") == "fill_below_minimum_threshold":
        return await finish(
            decision="skipped",
            reason_code="execution_partial_fill_below_minimum",
            detail="Executable fill is below the minimum threshold",
            diagnostics={
                "direction": ideal_ev["direction"],
                "recommended_size_usd": str(provisional_sizing["recommended_size_usd"]),
                "fillable_size_usd": str(fillable_size_usd),
                "shadow_fill_pct": shadow_details.get("fill_pct"),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            requested_size_usd=provisional_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=ideal_ev["ev_per_share"],
            liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
        )

    if shadow_details.get("fill_status") == "no_fill":
        return await finish(
            decision="skipped",
            reason_code="execution_no_fill",
            detail=f"No executable fill available ({shadow_details.get('fill_reason')})",
            diagnostics={
                "direction": ideal_ev["direction"],
                "recommended_size_usd": str(provisional_sizing["recommended_size_usd"]),
                "fillable_size_usd": str(fillable_size_usd),
                "shadow_fill_status": shadow_details.get("fill_status"),
                "shadow_fill_reason": shadow_details.get("fill_reason"),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            requested_size_usd=provisional_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=ideal_ev["ev_per_share"],
            liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
        )

    executable_entry_price = shadow_execution["shadow_entry_price"]
    executable_ev = compute_directional_ev_full(
        direction=ideal_ev["direction"],
        estimated_probability=estimated_probability,
        entry_price=executable_entry_price,
    )
    if executable_ev["ev_per_share"] < min_ev_threshold:
        return await finish(
            decision="skipped",
            reason_code="execution_ev_below_threshold",
            detail=(
                f"Executable EV {executable_ev['ev_per_share']} below threshold "
                f"{settings.min_ev_threshold}"
            ),
            diagnostics={
                "direction": ideal_ev["direction"],
                "ideal_ev_per_share": str(ideal_ev["ev_per_share"]),
                "executable_ev_per_share": str(executable_ev["ev_per_share"]),
                "executable_entry_price": str(executable_entry_price),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            executable_entry_price=executable_entry_price,
            requested_size_usd=provisional_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=executable_ev["ev_per_share"],
            liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
            executable_ev=executable_ev,
        )

    executable_sizing = kelly_size_for_trade(
        direction=ideal_ev["direction"],
        estimated_probability=estimated_probability,
        entry_price=executable_entry_price,
        bankroll=bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )
    fill_capped_size_usd = min(executable_sizing["recommended_size_usd"], fillable_size_usd).quantize(Decimal("0.01"))
    if fill_capped_size_usd <= ZERO:
        return await finish(
            decision="skipped",
            reason_code="execution_size_zero_after_fill_cap",
            detail="Executable size became zero after applying the fill cap",
            diagnostics={
                "direction": ideal_ev["direction"],
                "recommended_size_usd": str(executable_sizing["recommended_size_usd"]),
                "fillable_size_usd": str(fillable_size_usd),
                "executable_entry_price": str(executable_entry_price),
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            executable_entry_price=executable_entry_price,
            requested_size_usd=executable_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=executable_ev["ev_per_share"],
            net_expected_pnl_usd=_net_expected_pnl_usd(
                executable_ev["ev_per_share"],
                fill_capped_size_usd,
                executable_entry_price,
            ),
            liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
            executable_ev=executable_ev,
            executable_sizing=executable_sizing,
            fill_capped_size_usd=fill_capped_size_usd,
        )

    portfolio = await _get_portfolio_state(session, strategy_run_id=strategy_run_id)
    open_positions = [
        {
            "size_usd": t.size_usd,
            "market_question": (t.details or {}).get("market_question", ""),
            "outcome_id": str(t.outcome_id),
        }
        for t in portfolio["open_trades"]
    ]
    peak_bankroll = bankroll
    if portfolio["cumulative_pnl"] > ZERO:
        peak_bankroll = bankroll + portfolio["cumulative_pnl"]

    risk_result = await assess_paper_trade_risk(
        session,
        outcome_id=outcome_id,
        market_id=market_id,
        direction=ideal_ev["direction"],
        proposed_notional_usd=fill_capped_size_usd,
    )
    if risk_result is None:
        risk_result = check_exposure(
            open_positions=open_positions,
            new_trade={
                "size_usd": fill_capped_size_usd,
                "market_question": market_question,
                "outcome_id": str(outcome_id),
            },
            bankroll=bankroll,
            max_total_pct=Decimal(str(settings.max_total_exposure_pct)),
            max_cluster_pct=Decimal(str(settings.max_cluster_exposure_pct)),
            drawdown_breaker_pct=Decimal(str(settings.drawdown_circuit_breaker_pct)),
            peak_bankroll=peak_bankroll,
            cumulative_pnl=portfolio["cumulative_pnl"],
        )
    if not risk_result["approved"]:
        logger.info(
            "Paper trade rejected by risk check: %s (signal %s)",
            risk_result["reason"], signal_id,
        )
        reason_code = _risk_reason_code(risk_result["reason"])
        return await finish(
            decision="skipped",
            reason_code=reason_code,
            detail=risk_result["reason"],
            diagnostics={
                "direction": ideal_ev["direction"],
                "recommended_size_usd": str(executable_sizing["recommended_size_usd"]),
                "fillable_size_usd": str(fillable_size_usd),
                "fill_capped_size_usd": str(fill_capped_size_usd),
                "approved_size_usd": str(risk_result["approved_size_usd"]),
                "risk_reason": risk_result["reason"],
                "drawdown_active": risk_result["drawdown_active"],
            },
            direction=ideal_ev["direction"],
            ideal_entry_price=ideal_ev["entry_price"],
            executable_entry_price=executable_entry_price,
            requested_size_usd=executable_sizing["recommended_size_usd"],
            fillable_size_usd=fillable_size_usd,
            fill_probability=fill_probability,
            net_ev_per_share=executable_ev["ev_per_share"],
            net_expected_pnl_usd=_net_expected_pnl_usd(
                executable_ev["ev_per_share"],
                fill_capped_size_usd,
                executable_entry_price,
            ),
            liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
            fill_status=shadow_details.get("fill_status"),
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
            executable_ev=executable_ev,
            executable_sizing=executable_sizing,
            fill_capped_size_usd=fill_capped_size_usd,
            risk_result=risk_result,
        )

    approved_size_usd = risk_result["approved_size_usd"]
    shares = (
        (approved_size_usd / executable_entry_price).quantize(Decimal("0.0001"))
        if executable_entry_price > ZERO
        else ZERO
    )
    return await finish(
        decision="opened",
        reason_code="opened",
        diagnostics={
            "direction": ideal_ev["direction"],
            "ideal_entry_price": str(ideal_ev["entry_price"]),
            "ideal_ev_per_share": str(ideal_ev["ev_per_share"]),
            "executable_entry_price": str(executable_entry_price),
            "ev_per_share": str(executable_ev["ev_per_share"]),
            "edge_pct": str(executable_ev["edge_pct"]),
            "kelly_full": str(executable_sizing["kelly_full"]),
            "kelly_used": str(executable_sizing["kelly_used"]),
            "recommended_size_usd": str(executable_sizing["recommended_size_usd"]),
            "fillable_size_usd": str(fillable_size_usd),
            "fill_capped_size_usd": str(fill_capped_size_usd),
            "approved_size_usd": str(approved_size_usd),
            "drawdown_active": risk_result["drawdown_active"],
            "shadow_entry_price": str(executable_entry_price),
            "liquidity_constrained": shadow_details.get("liquidity_constrained"),
            "missing_orderbook_context": shadow_details.get("missing_orderbook_context"),
            "stale_orderbook_context": shadow_details.get("stale_orderbook_context"),
            "shadow_fill_status": shadow_details.get("fill_status"),
            "shadow_fill_pct": shadow_details.get("fill_pct"),
        },
        action="cross",
        direction=ideal_ev["direction"],
        ideal_entry_price=ideal_ev["entry_price"],
        executable_entry_price=executable_entry_price,
        requested_size_usd=executable_sizing["recommended_size_usd"],
        fillable_size_usd=fillable_size_usd,
        fill_probability=fill_probability,
        net_ev_per_share=executable_ev["ev_per_share"],
        net_expected_pnl_usd=_net_expected_pnl_usd(
            executable_ev["ev_per_share"],
            approved_size_usd,
            executable_entry_price,
        ),
        missing_orderbook_context=bool(shadow_details.get("missing_orderbook_context")),
        stale_orderbook_context=bool(shadow_details.get("stale_orderbook_context")),
        liquidity_constrained=bool(shadow_details.get("liquidity_constrained")),
        fill_status=shadow_details.get("fill_status"),
        ideal_ev=ideal_ev,
        provisional_sizing=provisional_sizing,
        shadow_execution=shadow_execution,
        executable_ev=executable_ev,
        executable_sizing=executable_sizing,
        fill_capped_size_usd=fill_capped_size_usd,
        risk_result=risk_result,
        approved_size_usd=approved_size_usd,
        shares=shares,
    )


async def attempt_open_trade(
    session: AsyncSession,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal | None,
    market_price: Decimal | None,
    market_question: str = "",
    fired_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
    precheck_reason_code: str | None = None,
    precheck_reason_label: str | None = None,
) -> TradeOpenResult:
    """Open a paper trade only after the Phase 0 execution gate approves it."""

    decision_result = await build_execution_decision(
        session=session,
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        estimated_probability=estimated_probability,
        market_price=market_price,
        market_question=market_question,
        fired_at=fired_at,
        strategy_run_id=strategy_run_id,
        precheck_reason_code=precheck_reason_code,
        precheck_reason_label=precheck_reason_label,
    )
    if decision_result.decision != "opened":
        return TradeOpenResult(
            trade=None,
            decision=decision_result.decision,
            reason_code=decision_result.reason_code,
            reason_label=decision_result.reason_label,
            detail=decision_result.detail,
            diagnostics=decision_result.diagnostics,
            execution_decision=decision_result.execution_decision,
        )

    opened_at = datetime.now(timezone.utc)
    shadow_details = (
        dict(decision_result.shadow_execution["details"])
        if decision_result.shadow_execution is not None
        else {}
    )
    trade = PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        strategy_run_id=strategy_run_id,
        execution_decision_id=(
            decision_result.execution_decision.id
            if decision_result.execution_decision is not None
            else None
        ),
        outcome_id=outcome_id,
        market_id=market_id,
        direction=decision_result.direction,
        entry_price=decision_result.executable_entry_price,
        shadow_entry_price=decision_result.executable_entry_price,
        size_usd=decision_result.approved_size_usd,
        shares=decision_result.shares,
        status="open",
        opened_at=opened_at,
        submitted_at=opened_at,
        confirmed_at=opened_at,
        details={
            "market_question": market_question,
            "estimated_probability": str(estimated_probability),
            "market_price": str(market_price),
            "ideal_entry_price": str(decision_result.ideal_entry_price),
            "executable_entry_price": str(decision_result.executable_entry_price),
            "ev_per_share": decision_result.diagnostics["ev_per_share"],
            "edge_pct": decision_result.diagnostics["edge_pct"],
            "kelly_full": decision_result.diagnostics["kelly_full"],
            "kelly_used": decision_result.diagnostics["kelly_used"],
            "recommended_size_usd": decision_result.diagnostics["recommended_size_usd"],
            "fillable_size_usd": decision_result.diagnostics["fillable_size_usd"],
            "fill_capped_size_usd": decision_result.diagnostics["fill_capped_size_usd"],
            "approved_size_usd": decision_result.diagnostics["approved_size_usd"],
            "ideal_ev_per_share": decision_result.diagnostics["ideal_ev_per_share"],
            "risk_result": "approved" if not decision_result.diagnostics["drawdown_active"] else "approved (drawdown-reduced)",
            "drawdown_active": decision_result.diagnostics["drawdown_active"],
            "chosen_action_type": decision_result.diagnostics.get("chosen_action_type"),
            "chosen_order_type_hint": decision_result.diagnostics.get("chosen_order_type_hint"),
            "chosen_reason": decision_result.diagnostics.get("chosen_reason"),
            "decision_reason_json": decision_result.diagnostics.get("decision_reason_json"),
            "strategy_run_id": str(strategy_run_id) if strategy_run_id else None,
            "execution_decision_id": (
                str(decision_result.execution_decision.id)
                if decision_result.execution_decision is not None
                else None
            ),
            "shadow_execution": shadow_details,
        },
    )
    session.add(trade)
    await session.flush()
    logger.info(
        "Paper trade opened: %s %s @ $%s, size $%s (%s shares), signal=%s",
        trade.direction, outcome_id, trade.entry_price, trade.size_usd, trade.shares, signal_id,
    )
    return TradeOpenResult(
        trade=trade,
        decision=decision_result.decision,
        reason_code=decision_result.reason_code,
        reason_label=decision_result.reason_label,
        detail=decision_result.detail,
        diagnostics=decision_result.diagnostics,
        execution_decision=decision_result.execution_decision,
    )

async def _legacy_attempt_open_trade(
    session: AsyncSession,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal | None,
    market_price: Decimal | None,
    market_question: str = "",
    fired_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
) -> TradeOpenResult:
    """Open a paper trade for an EV-positive signal.

    Runs Kelly sizing and risk checks.

    Returns a structured result so callers can persist skip reasons.
    """
    if not settings.paper_trading_enabled:
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="paper_trading_disabled",
            reason_label="Paper trading disabled",
        )

    if outcome_id is None:
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="missing_outcome_id",
            reason_label="Missing outcome",
        )

    if estimated_probability is None:
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="missing_probability",
            reason_label="Missing probability",
        )

    if market_price is None:
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="missing_market_price",
            reason_label="Missing market price",
        )

    existing_query = select(PaperTrade.id, PaperTrade.status).where(PaperTrade.signal_id == signal_id)
    if strategy_run_id is not None:
        existing_query = existing_query.where(PaperTrade.strategy_run_id == strategy_run_id)
    else:
        existing_query = existing_query.where(PaperTrade.status == "open")
    existing_query = existing_query.order_by(PaperTrade.opened_at.desc()).limit(1)
    existing = await session.execute(existing_query)
    existing_trade = existing.first()
    if existing_trade is not None:
        existing_trade_id, existing_trade_status = existing_trade
        if strategy_run_id is not None:
            return TradeOpenResult(
                trade=None,
                decision="skipped",
                reason_code="already_recorded",
                reason_label="Already recorded in run",
                detail=(
                    f"Signal already has a {existing_trade_status} paper trade in "
                    f"strategy run ({existing_trade_id})"
                ),
            )
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="already_open",
            reason_label="Already open",
            detail=f"Signal already has an open paper trade ({existing_trade_id})",
        )

    bankroll = Decimal(str(settings.default_bankroll))

    # Compute EV
    ev_data = compute_ev_full(estimated_probability, market_price)
    if ev_data["ev_per_share"] < Decimal(str(settings.min_ev_threshold)):
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="ev_below_threshold",
            reason_label="EV below threshold",
            detail=f"Directional EV {ev_data['ev_per_share']} below threshold {settings.min_ev_threshold}",
            diagnostics={
                "ev_per_share": str(ev_data["ev_per_share"]),
                "edge_pct": str(ev_data["edge_pct"]),
                "direction": ev_data["direction"],
            },
        )

    # Kelly sizing
    sizing = kelly_size(
        estimated_prob=estimated_probability,
        market_price=market_price,
        bankroll=bankroll,
        kelly_fraction=Decimal(str(settings.kelly_multiplier)),
        max_position_pct=Decimal(str(settings.max_single_position_pct)),
    )

    if sizing["recommended_size_usd"] <= ZERO:
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code="size_zero",
            reason_label="Recommended size is zero",
            diagnostics={
                "direction": sizing["direction"],
                "kelly_full": str(sizing["kelly_full"]),
                "kelly_used": str(sizing["kelly_used"]),
                "recommended_size_usd": str(sizing["recommended_size_usd"]),
                "entry_price": str(sizing["entry_price"]),
            },
        )

    # Risk check
    portfolio = await _get_portfolio_state(session, strategy_run_id=strategy_run_id)
    open_positions = [
        {
            "size_usd": t.size_usd,
            "market_question": (t.details or {}).get("market_question", ""),
            "outcome_id": str(t.outcome_id),
        }
        for t in portfolio["open_trades"]
    ]

    # Compute peak bankroll for drawdown check
    peak_bankroll = bankroll  # start with default
    if portfolio["cumulative_pnl"] > ZERO:
        peak_bankroll = bankroll + portfolio["cumulative_pnl"]

    risk_result = await assess_paper_trade_risk(
        session,
        outcome_id=outcome_id,
        market_id=market_id,
        direction=sizing["direction"],
        proposed_notional_usd=sizing["recommended_size_usd"],
    )
    if risk_result is None:
        risk_result = check_exposure(
            open_positions=open_positions,
            new_trade={
                "size_usd": sizing["recommended_size_usd"],
                "market_question": market_question,
                "outcome_id": str(outcome_id),
            },
            bankroll=bankroll,
            max_total_pct=Decimal(str(settings.max_total_exposure_pct)),
            max_cluster_pct=Decimal(str(settings.max_cluster_exposure_pct)),
            drawdown_breaker_pct=Decimal(str(settings.drawdown_circuit_breaker_pct)),
            peak_bankroll=peak_bankroll,
            cumulative_pnl=portfolio["cumulative_pnl"],
        )

    if not risk_result["approved"]:
        logger.info(
            "Paper trade rejected by risk check: %s (signal %s)",
            risk_result["reason"], signal_id,
        )
        reason_code = _risk_reason_code(risk_result["reason"])
        return TradeOpenResult(
            trade=None,
            decision="skipped",
            reason_code=reason_code,
            reason_label=_risk_reason_label(reason_code),
            detail=risk_result["reason"],
            diagnostics={
                "direction": sizing["direction"],
                "recommended_size_usd": str(sizing["recommended_size_usd"]),
                "approved_size_usd": str(risk_result["approved_size_usd"]),
                "risk_reason": risk_result["reason"],
                "drawdown_active": risk_result["drawdown_active"],
            },
        )

    approved_size = risk_result["approved_size_usd"]
    entry_price = sizing["entry_price"]
    shares = (approved_size / entry_price).quantize(Decimal("0.0001")) if entry_price > ZERO else ZERO
    shadow_execution = await _build_shadow_execution(
        session=session,
        outcome_id=outcome_id,
        direction=sizing["direction"],
        approved_size=approved_size,
        ideal_entry_price=entry_price,
        fired_at=fired_at,
    )

    trade = PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        strategy_run_id=strategy_run_id,
        outcome_id=outcome_id,
        market_id=market_id,
        direction=sizing["direction"],
        entry_price=entry_price,
        shadow_entry_price=shadow_execution["shadow_entry_price"],
        size_usd=approved_size,
        shares=shares,
        status="open",
        opened_at=datetime.now(timezone.utc),
        details={
            "market_question": market_question,
            "estimated_probability": str(estimated_probability),
            "market_price": str(market_price),
            "ev_per_share": str(ev_data["ev_per_share"]),
            "edge_pct": str(ev_data["edge_pct"]),
            "kelly_full": str(sizing["kelly_full"]),
            "kelly_used": str(sizing["kelly_used"]),
            "risk_result": risk_result["reason"],
            "drawdown_active": risk_result["drawdown_active"],
            "strategy_run_id": str(strategy_run_id) if strategy_run_id else None,
            "shadow_execution": shadow_execution["details"],
        },
    )
    session.add(trade)
    await session.flush()
    logger.info(
        "Paper trade opened: %s %s @ $%s, size $%s (%s shares), signal=%s",
        trade.direction, outcome_id, entry_price, approved_size, shares, signal_id,
    )
    return TradeOpenResult(
        trade=trade,
        decision="opened",
        reason_code="opened",
        reason_label="Trade opened",
        diagnostics={
            "direction": trade.direction,
            "ev_per_share": str(ev_data["ev_per_share"]),
            "edge_pct": str(ev_data["edge_pct"]),
            "kelly_full": str(sizing["kelly_full"]),
            "kelly_used": str(sizing["kelly_used"]),
            "recommended_size_usd": str(sizing["recommended_size_usd"]),
            "approved_size_usd": str(approved_size),
            "drawdown_active": risk_result["drawdown_active"],
            "shadow_entry_price": str(shadow_execution["shadow_entry_price"]) if shadow_execution["shadow_entry_price"] is not None else None,
            "liquidity_constrained": shadow_execution["details"]["liquidity_constrained"],
            "missing_orderbook_context": shadow_execution["details"]["missing_orderbook_context"],
            "stale_orderbook_context": shadow_execution["details"]["stale_orderbook_context"],
            "shadow_fill_status": shadow_execution["details"]["fill_status"],
            "shadow_fill_pct": shadow_execution["details"]["fill_pct"],
        },
    )


async def open_trade(
    session: AsyncSession,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal | None,
    market_price: Decimal | None,
    market_question: str = "",
    fired_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
) -> PaperTrade | None:
    """Backward-compatible wrapper that returns only the created trade."""
    result = await attempt_open_trade(
        session=session,
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        estimated_probability=estimated_probability,
        market_price=market_price,
        market_question=market_question,
        fired_at=fired_at,
        strategy_run_id=strategy_run_id,
    )
    return result.trade


async def resolve_trades(
    session: AsyncSession,
    outcome_id: uuid.UUID,
    outcome_won: bool,
    *,
    resolved_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
) -> int:
    """Resolve all open paper trades for a given outcome.

    Args:
        outcome_id: The resolved outcome
        outcome_won: True if the outcome resolved YES, False if NO
        resolved_at: Optional historical resolution timestamp
        strategy_run_id: Optional strategy run scope

    Returns count of resolved trades.
    """
    query = select(PaperTrade).where(
        PaperTrade.outcome_id == outcome_id,
        PaperTrade.status == "open",
    )
    if strategy_run_id is not None:
        query = query.where(PaperTrade.strategy_run_id == strategy_run_id)

    result = await session.execute(query)
    trades = result.scalars().all()

    if not trades:
        return 0

    now = resolved_at or datetime.now(timezone.utc)
    count = 0

    for trade in trades:
        if trade.direction == "buy_yes":
            exit_price = Decimal("1.000000") if outcome_won else Decimal("0.000000")
        else:  # buy_no
            exit_price = Decimal("0.000000") if outcome_won else Decimal("1.000000")

        # P&L = shares * (exit_price - entry_price)
        pnl = (trade.shares * (exit_price - trade.entry_price)).quantize(Decimal("0.01"))
        shadow_pnl = None
        shadow_shares = _shadow_shares_from_trade(trade)
        if shadow_shares == ZERO:
            shadow_pnl = ZERO.quantize(Decimal("0.01"))
        elif trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
            shadow_pnl = (shadow_shares * (exit_price - trade.shadow_entry_price)).quantize(Decimal("0.01"))

        trade.exit_price = exit_price
        trade.pnl = pnl
        trade.shadow_pnl = shadow_pnl
        trade.status = "resolved"
        trade.resolved_at = now
        count += 1

        logger.info(
            "Paper trade resolved: %s %s, entry=$%s exit=$%s, P&L=$%s",
            trade.direction, trade.outcome_id, trade.entry_price, exit_price, pnl,
        )

    if count > 0:
        await session.flush()

    return count


async def get_metrics(session: AsyncSession) -> dict:
    """Compute portfolio performance metrics: Sharpe, max drawdown, win rate, P&L."""
    # Get all resolved trades ordered by resolution time
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "resolved")
        .order_by(PaperTrade.resolved_at.asc())
    )
    resolved = result.scalars().all()

    if not resolved:
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

    pnls = [float(t.pnl) for t in resolved if t.pnl is not None]
    shadow_pnls = [float(t.shadow_pnl) for t in resolved if t.shadow_pnl is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    shadow_wins = [p for p in shadow_pnls if p > 0]
    shadow_losses = [p for p in shadow_pnls if p <= 0]

    # Cumulative P&L curve for max drawdown
    cumulative = []
    running = 0.0
    for p in pnls:
        running += p
        cumulative.append(running)

    # Max drawdown
    peak = 0.0
    max_dd = 0.0
    for value in cumulative:
        if value > peak:
            peak = value
        dd = peak - value
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio (simplified: mean/std of per-trade returns)
    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance)
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

    # Profit factor
    total_wins = sum(wins) if wins else 0.0
    total_losses = abs(sum(losses)) if losses else 0.0
    profit_factor = (total_wins / total_losses) if total_losses > 0 else float("inf") if total_wins > 0 else 0.0
    shadow_total_wins = sum(shadow_wins) if shadow_wins else 0.0
    shadow_total_losses = abs(sum(shadow_losses)) if shadow_losses else 0.0
    shadow_profit_factor = (
        shadow_total_wins / shadow_total_losses
        if shadow_total_losses > 0
        else float("inf") if shadow_total_wins > 0 else 0.0
    )
    liquidity_constrained_trades = sum(
        1
        for trade in resolved
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("liquidity_constrained") is True
    )
    trades_missing_orderbook_context = sum(
        1
        for trade in resolved
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("missing_orderbook_context") is True
    )

    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        "cumulative_pnl": round(sum(pnls), 2),
        "shadow_cumulative_pnl": round(sum(shadow_pnls), 2) if shadow_pnls else 0.0,
        "avg_pnl": round(mean_pnl, 2),
        "max_drawdown": round(max_dd, 2),
        "sharpe_ratio": round(sharpe, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "shadow_profit_factor": round(shadow_profit_factor, 4) if shadow_profit_factor != float("inf") else None,
        "best_trade": round(max(pnls), 2) if pnls else 0.0,
        "worst_trade": round(min(pnls), 2) if pnls else 0.0,
        "liquidity_constrained_trades": liquidity_constrained_trades,
        "trades_missing_orderbook_context": trades_missing_orderbook_context,
    }


async def get_pnl_curve(session: AsyncSession) -> list[dict]:
    """Return cumulative P&L curve data points for charting."""
    result = await session.execute(
        select(PaperTrade)
        .where(PaperTrade.status == "resolved")
        .order_by(PaperTrade.resolved_at.asc())
    )
    resolved = result.scalars().all()

    curve = []
    running = Decimal("0")
    for trade in resolved:
        if trade.pnl is not None and trade.resolved_at is not None:
            running += trade.pnl
            curve.append({
                "timestamp": trade.resolved_at.isoformat(),
                "pnl": float(running),
                "trade_pnl": float(trade.pnl),
                "shadow_trade_pnl": float(trade.shadow_pnl) if trade.shadow_pnl is not None else None,
                "direction": trade.direction,
                "trade_id": str(trade.id),
            })

    return curve


def _parse_decimal(value) -> Decimal | None:
    if value in (None, "", []):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _best_price(levels) -> Decimal | None:
    if not levels:
        return None
    try:
        return Decimal(str(levels[0][0]))
    except Exception:
        return None


def _near_touch_liquidity(
    levels,
    *,
    side: str,
    half_spread: Decimal,
) -> dict[str, Decimal | None]:
    if not levels:
        return {
            "available_depth_shares": None,
            "available_depth_usd": None,
        }

    best = _best_price(levels)
    if best is None:
        return {
            "available_depth_shares": None,
            "available_depth_usd": None,
        }

    threshold = half_spread if half_spread > ZERO else Decimal("0.01")
    depth_shares = ZERO
    depth_usd = ZERO
    saw_level = False

    for level in levels:
        if len(level) < 2:
            continue
        price = _parse_decimal(level[0])
        size = _parse_decimal(level[1])
        if price is None or size is None or size <= ZERO:
            continue
        if side == "ask":
            if price > best + threshold:
                break
            fill_price = price
        else:
            if price < best - threshold:
                break
            fill_price = (ONE - price).quantize(Decimal("0.000001"))
        saw_level = True
        if fill_price <= ZERO:
            continue
        depth_shares += size
        depth_usd += size * fill_price

    if not saw_level:
        return {
            "available_depth_shares": None,
            "available_depth_usd": None,
        }

    return {
        "available_depth_shares": depth_shares.quantize(Decimal("0.0001")),
        "available_depth_usd": depth_usd.quantize(Decimal("0.01")),
    }


async def _nearest_orderbook_snapshot(
    session: AsyncSession,
    outcome_id: uuid.UUID,
    fired_at: datetime | None,
) -> OrderbookContext:
    anchor = _ensure_utc(fired_at) or datetime.now(timezone.utc)
    before_result = await session.execute(
        select(OrderbookSnapshot)
        .where(
            OrderbookSnapshot.outcome_id == outcome_id,
            OrderbookSnapshot.captured_at <= anchor,
        )
        .order_by(desc(OrderbookSnapshot.captured_at))
        .limit(1)
    )
    before = before_result.scalars().first()

    after_result = await session.execute(
        select(OrderbookSnapshot)
        .where(
            OrderbookSnapshot.outcome_id == outcome_id,
            OrderbookSnapshot.captured_at >= anchor,
        )
        .order_by(OrderbookSnapshot.captured_at.asc())
        .limit(1)
    )
    after = after_result.scalars().first()

    usable_candidates: list[tuple[int, int, OrderbookSnapshot, str]] = []
    before_captured_at = _ensure_utc(before.captured_at) if before is not None else None
    after_captured_at = _ensure_utc(after.captured_at) if after is not None else None
    if before is not None and before_captured_at is not None:
        age_seconds = max(0, int((anchor - before_captured_at).total_seconds()))
        if age_seconds <= settings.shadow_execution_max_staleness_seconds:
            usable_candidates.append((age_seconds, 0, before, "before"))
    if after is not None and after_captured_at is not None:
        age_seconds = max(0, int((after_captured_at - anchor).total_seconds()))
        if age_seconds <= settings.shadow_execution_max_forward_seconds:
            usable_candidates.append((age_seconds, 1, after, "after"))

    if usable_candidates:
        age_seconds, _priority, snapshot, snapshot_side = min(usable_candidates, key=lambda row: (row[0], row[1]))
        return OrderbookContext(
            snapshot=snapshot,
            snapshot_age_seconds=age_seconds,
            snapshot_side=snapshot_side,
            usable=True,
        )

    stale_candidates: list[tuple[int, int, OrderbookSnapshot, str, str]] = []
    if before is not None and before_captured_at is not None:
        age_seconds = max(0, int((anchor - before_captured_at).total_seconds()))
        stale_candidates.append((age_seconds, 0, before, "before", "stale_snapshot"))
    if after is not None and after_captured_at is not None:
        age_seconds = max(0, int((after_captured_at - anchor).total_seconds()))
        stale_candidates.append((age_seconds, 1, after, "after", "future_snapshot_too_far"))

    if stale_candidates:
        age_seconds, _priority, snapshot, snapshot_side, missing_reason = min(
            stale_candidates,
            key=lambda row: (row[0], row[1]),
        )
        return OrderbookContext(
            snapshot=snapshot,
            snapshot_age_seconds=age_seconds,
            snapshot_side=snapshot_side,
            usable=False,
            stale=True,
            missing_reason=missing_reason,
        )

    return OrderbookContext(
        snapshot=None,
        snapshot_age_seconds=None,
        snapshot_side=None,
        usable=False,
        stale=False,
        missing_reason="no_snapshot",
    )


async def _build_shadow_execution(
    session: AsyncSession,
    *,
    outcome_id: uuid.UUID,
    direction: str,
    approved_size: Decimal,
    ideal_entry_price: Decimal,
    fired_at: datetime | None,
) -> dict:
    orderbook_context = await _nearest_orderbook_snapshot(session, outcome_id, fired_at)
    snapshot = orderbook_context.snapshot
    requested_size_usd = approved_size.quantize(Decimal("0.01"))
    if not orderbook_context.usable or snapshot is None:
        return {
            "shadow_entry_price": None,
            "details": {
                "missing_orderbook_context": True,
                "stale_orderbook_context": orderbook_context.stale,
                "liquidity_constrained": False,
                "fill_status": "no_fill",
                "fill_reason": orderbook_context.missing_reason or "missing_orderbook_context",
                "snapshot_id": snapshot.id if snapshot is not None else None,
                "captured_at": _ensure_utc(snapshot.captured_at).isoformat() if snapshot is not None and snapshot.captured_at else None,
                "snapshot_age_seconds": orderbook_context.snapshot_age_seconds,
                "snapshot_side": orderbook_context.snapshot_side,
                "spread": str(snapshot.spread) if snapshot is not None and snapshot.spread is not None else None,
                "best_bid": str(_best_price(snapshot.bids)) if snapshot is not None and _best_price(snapshot.bids) is not None else None,
                "best_ask": str(_best_price(snapshot.asks)) if snapshot is not None and _best_price(snapshot.asks) is not None else None,
                "available_depth_shares": None,
                "available_depth_usd": None,
                "size_to_depth_ratio": None,
                "requested_size_usd": str(requested_size_usd),
                "filled_size_usd": "0.00",
                "unfilled_size_usd": str(requested_size_usd),
                "fill_pct": "0.0000",
                "shadow_shares": "0.0000",
            },
        }

    spread = snapshot.spread or ZERO
    half_spread = (spread * HALF).quantize(Decimal("0.000001"))
    best_bid = _best_price(snapshot.bids)
    best_ask = _best_price(snapshot.asks)
    if direction == "buy_yes":
        liquidity = _near_touch_liquidity(
            snapshot.asks or [],
            side="ask",
            half_spread=half_spread,
        )
    else:
        liquidity = _near_touch_liquidity(
            snapshot.bids or [],
            side="bid",
            half_spread=half_spread,
        )
    available_depth_shares = liquidity["available_depth_shares"]
    available_depth_usd = liquidity["available_depth_usd"]

    shadow_entry_price = (ideal_entry_price + half_spread).quantize(Decimal("0.000001"))
    if best_ask is not None and direction == "buy_yes":
        shadow_entry_price = max(shadow_entry_price, best_ask)
    if best_bid is not None and direction == "buy_no":
        shadow_entry_price = max(shadow_entry_price, (ONE - best_bid).quantize(Decimal("0.000001")))
    shadow_entry_price = min(shadow_entry_price, ONE)

    size_to_depth_ratio = None
    if available_depth_usd is not None and available_depth_usd > ZERO:
        size_to_depth_ratio = (approved_size / available_depth_usd).quantize(Decimal("0.0001"))

    if available_depth_usd is None or available_depth_usd <= ZERO:
        fill_status = "no_fill"
        filled_size_usd = ZERO
        liquidity_constrained = True
        fill_reason = "no_near_touch_depth"
    elif approved_size > available_depth_usd:
        candidate_fill_pct = (available_depth_usd / approved_size).quantize(Decimal("0.0001")) if approved_size > ZERO else ZERO
        if candidate_fill_pct < Decimal(str(settings.shadow_execution_min_fill_pct)):
            fill_status = "no_fill"
            filled_size_usd = ZERO
            liquidity_constrained = True
            fill_reason = "fill_below_minimum_threshold"
        else:
            fill_status = "partial_fill"
            filled_size_usd = available_depth_usd
            liquidity_constrained = True
            fill_reason = "insufficient_near_touch_depth"
    else:
        fill_status = "full_fill"
        filled_size_usd = approved_size
        liquidity_constrained = False
        fill_reason = "filled_within_near_touch_depth"

    filled_size_usd = filled_size_usd.quantize(Decimal("0.01"))
    unfilled_size_usd = (approved_size - filled_size_usd).quantize(Decimal("0.01"))
    fill_pct = (filled_size_usd / approved_size).quantize(Decimal("0.0001")) if approved_size > ZERO else ZERO
    shadow_entry_price_to_store = shadow_entry_price if filled_size_usd > ZERO else None
    shadow_shares = (
        (filled_size_usd / shadow_entry_price).quantize(Decimal("0.0001"))
        if shadow_entry_price_to_store is not None and shadow_entry_price > ZERO
        else ZERO
    )

    return {
        "shadow_entry_price": shadow_entry_price_to_store,
        "details": {
            "missing_orderbook_context": False,
            "stale_orderbook_context": False,
            "liquidity_constrained": bool(liquidity_constrained),
            "fill_status": fill_status,
            "fill_reason": fill_reason,
            "snapshot_id": snapshot.id,
            "captured_at": _ensure_utc(snapshot.captured_at).isoformat() if snapshot.captured_at else None,
            "snapshot_age_seconds": orderbook_context.snapshot_age_seconds,
            "snapshot_side": orderbook_context.snapshot_side,
            "spread": str(snapshot.spread) if snapshot.spread is not None else None,
            "best_bid": str(best_bid) if best_bid is not None else None,
            "best_ask": str(best_ask) if best_ask is not None else None,
            "available_depth_shares": str(available_depth_shares) if available_depth_shares is not None else None,
            "available_depth_usd": str(available_depth_usd) if available_depth_usd is not None else None,
            "size_to_depth_ratio": str(size_to_depth_ratio) if size_to_depth_ratio is not None else None,
            "requested_size_usd": str(requested_size_usd),
            "filled_size_usd": str(filled_size_usd),
            "unfilled_size_usd": str(unfilled_size_usd),
            "fill_pct": str(fill_pct),
            "shadow_shares": str(shadow_shares),
        },
    }


def _shadow_shares_from_trade(trade: PaperTrade) -> Decimal:
    if isinstance(trade.details, dict):
        shadow_execution = trade.details.get("shadow_execution")
        if isinstance(shadow_execution, dict):
            fill_status = shadow_execution.get("fill_status")
            if fill_status == "no_fill":
                return ZERO
            shadow_shares = _parse_decimal(shadow_execution.get("shadow_shares"))
            if shadow_shares is not None:
                return shadow_shares
    if trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
        return (trade.size_usd / trade.shadow_entry_price).quantize(Decimal("0.0001"))
    return trade.shares
