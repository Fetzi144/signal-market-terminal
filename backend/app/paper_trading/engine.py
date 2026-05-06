"""Paper trading engine: auto-open trades on EV-positive signals, resolve on market settlement.

This is the core simulation engine that tracks hypothetical P&L
without real money. Every EV-positive signal triggers a paper trade
using Kelly-recommended sizing, subject to risk management checks.
"""
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import default_strategy_skip_label
from app.ingestion.polymarket_execution_policy import (
    evaluate_polymarket_execution_policy,
    persist_polymarket_execution_policy_result,
)
from app.ingestion.polymarket_risk_graph import assess_paper_trade_risk
from app.models.execution_decision import ExecutionDecision
from app.models.market import Market
from app.models.paper_trade import PaperTrade
from app.models.strategy_run import StrategyRun
from app.paper_trading import portfolio_views as portfolio_views_module
from app.paper_trading import shadow_execution as shadow_execution_module
from app.paper_trading.reconciliation import hydrate_strategy_run_state
from app.paper_trading.strategy_run_state import (
    apply_trade_resolution_to_run,
    initialize_strategy_run_state,
    strategy_run_state_complete,
)
from app.signals.ev import compute_directional_ev_full, compute_ev_full
from app.signals.kelly import kelly_size, kelly_size_for_trade
from app.signals.risk import check_exposure

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")
REASON_LABELS = {
    "pending_decision": "Pending decision",
    "pending_decision_expired": "Pending decision retry window expired",
    "paper_trading_disabled": "Paper trading disabled",
    "already_recorded": "Already recorded in run",
    "already_open": "Already open",
    "evidence_duplicate_open_market": "Market already has an open paper trade",
    "evidence_market_cooldown": "Market is in paper-trade cooldown",
    "size_zero": "Recommended size is zero",
    "execution_policy_skip": "Execution policy chose skip",
    "execution_policy_failure": "Execution policy evaluation failed",
    "execution_missing_orderbook_context": "Missing orderbook context",
    "execution_stale_orderbook_context": "Stale orderbook context",
    "execution_orderbook_context_unavailable": "Orderbook context unavailable after retry",
    "execution_no_fill": "No fill available",
    "execution_partial_fill_below_minimum": "Partial fill below minimum",
    "execution_ev_below_threshold": "Executable EV below threshold",
    "execution_size_zero_after_fill_cap": "Executable size is zero after fill cap",
    "profitability_market_inactive": "Market is inactive",
    "profitability_market_end_date_missing": "Market resolution date missing",
    "profitability_market_long_dated": "Market resolution is too far away",
    "profitability_market_liquidity_too_low": "Market liquidity below profitability filter",
    "profitability_market_metadata_missing": "Market metadata missing",
    "risk_state_uninitialized": "Run risk state not initialized",
    "risk_local_total_exposure": "Local paper-book total exposure limit reached",
    "risk_local_cluster_exposure": "Local paper-book cluster exposure limit reached",
    "risk_local_invalid_size": "Local paper-book invalid size",
    "risk_local_rejected": "Local paper-book risk rejected",
    "risk_shared_global_block": "Shared/global platform risk blocked the trade",
    "family_cap_exceeded": "Family capital envelope exceeded",
    "cluster_cap_exceeded": "Cluster capital envelope exceeded",
    "capacity_ceiling_exceeded": "Capacity ceiling exceeded",
    "risk_of_ruin_exceeded": "Risk-of-ruin ceiling exceeded",
    "opened": "Trade opened",
}


async def _resolve_strategy_version_id(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None,
) -> int | None:
    if strategy_run_id is None:
        return None
    result = await session.execute(
        select(StrategyRun.strategy_version_id).where(StrategyRun.id == strategy_run_id).limit(1)
    )
    return result.scalar_one_or_none()


def _iso(value: datetime | None) -> str | None:
    normalized = _ensure_utc(value)
    return normalized.isoformat() if normalized is not None else None


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
        "family_cap_exceeded": "Family capital envelope exceeded",
        "cluster_cap_exceeded": "Cluster capital envelope exceeded",
        "capacity_ceiling_exceeded": "Capacity ceiling exceeded",
        "risk_of_ruin_exceeded": "Risk-of-ruin ceiling exceeded",
        "risk_rejected": "Risk rejected",
    }
    return labels.get(reason_code, reason_code.replace("_", " "))


async def _load_strategy_run(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None,
) -> StrategyRun | None:
    if strategy_run_id is None:
        return None
    return await session.get(StrategyRun, strategy_run_id)


def _base_bankroll() -> Decimal:
    return Decimal(str(settings.default_bankroll)).quantize(Decimal("0.01"))


def _risk_context_from_run(strategy_run: StrategyRun | None, *, cumulative_pnl: Decimal) -> tuple[Decimal, Decimal]:
    bankroll = _base_bankroll()
    if strategy_run is None:
        peak_bankroll = bankroll
        if cumulative_pnl > ZERO:
            peak_bankroll = bankroll + cumulative_pnl
        return peak_bankroll, cumulative_pnl
    if not strategy_run_state_complete(strategy_run):
        raise ValueError("strategy_run risk state is not initialized")
    peak_bankroll = Decimal(str(strategy_run.peak_equity))
    run_cumulative_pnl = Decimal(str(strategy_run.current_equity)) - bankroll
    return peak_bankroll, run_cumulative_pnl


def _normalize_risk_result(
    risk_result: dict | None,
    *,
    upstream_source: str,
) -> dict | None:
    if risk_result is None:
        return None
    normalized = dict(risk_result)
    recommendation = normalized.get("recommendation")
    if not isinstance(recommendation, dict):
        recommendation = {}
    original_reason_code = (
        normalized.get("original_reason_code")
        or normalized.get("reason_code")
        or recommendation.get("reason_code")
    )
    original_reason = normalized.get("original_reason") or normalized.get("reason")
    is_shared_global = (
        upstream_source == "risk_graph"
        or normalized.get("risk_scope") == "shared_global"
        or normalized.get("risk_source") == "risk_graph"
        or normalized.get("risk_mode") == "graph"
        or recommendation.get("recommendation_type") in {"block", "reduce_size"}
    )
    if is_shared_global:
        normalized.setdefault("risk_scope", "shared_global")
        normalized.setdefault("risk_source", "risk_graph")
        if not normalized.get("approved"):
            normalized.setdefault("reason_code", "risk_shared_global_block")
    else:
        normalized.setdefault("risk_scope", "local_paper_book")
        normalized.setdefault("risk_source", "paper_book")
        if not normalized.get("approved"):
            normalized.setdefault("reason_code", "risk_local_rejected")
    normalized.setdefault("original_reason_code", original_reason_code)
    normalized.setdefault("original_reason", original_reason)
    return normalized


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


def _int_or_zero(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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
    return await portfolio_views_module.get_portfolio_state(session)


def _decision_action(decision: str, action: str | None = None) -> str:
    if action is not None:
        return action
    if decision == "opened":
        return "cross"
    if decision == "pending_decision":
        return "pending"
    return "skip"


def _market_liquidity_proxy(market: Market) -> Decimal:
    values = [
        Decimal(str(value))
        for value in (market.last_liquidity, market.last_volume_24h)
        if value is not None
    ]
    return max(values) if values else ZERO


async def _paper_profitability_market_precheck(
    session: AsyncSession,
    *,
    market_id: uuid.UUID,
    observed_at: datetime,
) -> dict | None:
    if not settings.paper_trading_profitability_filter_enabled:
        return None

    market = await session.get(Market, market_id)
    if market is None:
        return {
            "reason_code": "profitability_market_metadata_missing",
            "detail": f"Market metadata missing for {market_id}",
            "diagnostics": {
                "market_id": str(market_id),
                "profitability_filter_enabled": True,
            },
        }

    diagnostics = {
        "market_id": str(market.id),
        "platform": market.platform,
        "platform_id": market.platform_id,
        "active": bool(market.active),
        "market_end_date": _ensure_utc(market.end_date).isoformat() if market.end_date else None,
        "max_resolution_horizon_days": settings.paper_trading_max_resolution_horizon_days,
        "min_market_liquidity_usd": settings.paper_trading_min_market_liquidity_usd,
        "last_liquidity": str(market.last_liquidity) if market.last_liquidity is not None else None,
        "last_volume_24h": str(market.last_volume_24h) if market.last_volume_24h is not None else None,
    }
    if not bool(market.active):
        return {
            "reason_code": "profitability_market_inactive",
            "detail": "Market is inactive; paper evidence would not be a clean executable candidate.",
            "diagnostics": diagnostics,
        }

    end_date = _ensure_utc(market.end_date)
    if end_date is None:
        if settings.paper_trading_require_market_end_date:
            return {
                "reason_code": "profitability_market_end_date_missing",
                "detail": "Market has no end date, so it cannot contribute timely profitability evidence.",
                "diagnostics": diagnostics,
            }
        return None

    days_to_end = round((end_date - observed_at).total_seconds() / 86400, 4)
    diagnostics["days_to_end"] = days_to_end
    if days_to_end <= 0:
        return {
            "reason_code": "profitability_market_inactive",
            "detail": "Market has already reached or passed its end date.",
            "diagnostics": diagnostics,
        }
    if days_to_end > float(settings.paper_trading_max_resolution_horizon_days):
        return {
            "reason_code": "profitability_market_long_dated",
            "detail": (
                f"Market resolves in {days_to_end:.2f} days, beyond the "
                f"{settings.paper_trading_max_resolution_horizon_days}-day paper-profitability horizon."
            ),
            "diagnostics": diagnostics,
        }

    liquidity_proxy = _market_liquidity_proxy(market)
    diagnostics["liquidity_proxy_usd"] = str(liquidity_proxy)
    min_liquidity = Decimal(str(settings.paper_trading_min_market_liquidity_usd))
    if liquidity_proxy < min_liquidity:
        return {
            "reason_code": "profitability_market_liquidity_too_low",
            "detail": (
                f"Market liquidity/24h volume proxy ${liquidity_proxy} is below the "
                f"${min_liquidity} paper-profitability floor."
            ),
            "diagnostics": diagnostics,
        }
    return None


async def _paper_market_duplicate_precheck(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID | None,
    market_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    observed_at: datetime,
) -> dict | None:
    if strategy_run_id is None:
        return None

    open_row = (
        await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.market_id == market_id,
                PaperTrade.status == "open",
            )
            .order_by(PaperTrade.opened_at.desc(), PaperTrade.id.asc())
            .limit(1)
        )
    ).scalars().first()
    if open_row is not None:
        return {
            "reason_code": "evidence_duplicate_open_market",
            "detail": "This strategy run already has an open paper trade for the same market.",
            "diagnostics": {
                "strategy_run_id": str(strategy_run_id),
                "market_id": str(market_id),
                "outcome_id": str(outcome_id) if outcome_id is not None else None,
                "existing_trade_id": str(open_row.id),
                "existing_trade_status": open_row.status,
                "existing_trade_opened_at": _iso(open_row.opened_at),
                "existing_trade_outcome_id": str(open_row.outcome_id),
                "duplicate_scope": "strategy_run_market",
            },
        }

    cooldown_seconds = int(settings.paper_trading_market_cooldown_seconds)
    if cooldown_seconds <= 0:
        return None
    cooldown_start = observed_at - timedelta(seconds=cooldown_seconds)
    cooldown_row = (
        await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.strategy_run_id == strategy_run_id,
                PaperTrade.market_id == market_id,
                PaperTrade.opened_at >= cooldown_start,
            )
            .order_by(PaperTrade.opened_at.desc(), PaperTrade.id.asc())
            .limit(1)
        )
    ).scalars().first()
    if cooldown_row is None:
        return None

    return {
        "reason_code": "evidence_market_cooldown",
        "detail": (
            "This market was already paper-traded recently in the same strategy run; "
            "cooldown blocks duplicate evidence."
        ),
        "diagnostics": {
            "strategy_run_id": str(strategy_run_id),
            "market_id": str(market_id),
            "outcome_id": str(outcome_id) if outcome_id is not None else None,
            "existing_trade_id": str(cooldown_row.id),
            "existing_trade_status": cooldown_row.status,
            "existing_trade_opened_at": _iso(cooldown_row.opened_at),
            "existing_trade_outcome_id": str(cooldown_row.outcome_id),
            "cooldown_seconds": cooldown_seconds,
            "cooldown_started_at": _iso(cooldown_start),
            "duplicate_scope": "strategy_run_market",
        },
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
    min_ev_threshold: Decimal | None = None,
) -> ExecutionDecisionBuildResult:
    """Build and persist the Phase 0 execution decision before a trade opens."""

    decision_at = _ensure_utc(fired_at) or datetime.now(timezone.utc)
    execution_decision_row: ExecutionDecision | None = None

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
        nonlocal execution_decision_row
        label = reason_label or _reason_label(reason_code)
        execution_decision = execution_decision_row
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
            if execution_decision is None:
                execution_decision = ExecutionDecision(
                    id=uuid.uuid4(),
                    signal_id=signal_id,
                    strategy_run_id=strategy_run_id,
                    decision_at=decision_at,
                    decision_status=decision,
                    action=_decision_action(decision, action),
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
            else:
                execution_decision.decision_at = decision_at
                execution_decision.decision_status = decision
                execution_decision.action = _decision_action(decision, action)
                execution_decision.direction = direction
                execution_decision.ideal_entry_price = ideal_entry_price
                execution_decision.executable_entry_price = executable_entry_price
                execution_decision.requested_size_usd = requested_size_usd
                execution_decision.fillable_size_usd = fillable_size_usd
                execution_decision.fill_probability = fill_probability
                execution_decision.net_ev_per_share = net_ev_per_share
                execution_decision.net_expected_pnl_usd = net_expected_pnl_usd
                execution_decision.missing_orderbook_context = missing_orderbook_context
                execution_decision.stale_orderbook_context = stale_orderbook_context
                execution_decision.liquidity_constrained = liquidity_constrained
                execution_decision.fill_status = fill_status
                execution_decision.reason_code = reason_code
                execution_decision.chosen_action_type = chosen_action_type
                execution_decision.chosen_order_type_hint = chosen_order_type_hint
                execution_decision.chosen_target_price = chosen_target_price
                execution_decision.chosen_target_size = chosen_target_size
                execution_decision.chosen_est_fillable_size = chosen_est_fillable_size
                execution_decision.chosen_est_fill_probability = chosen_est_fill_probability
                execution_decision.chosen_est_net_ev_bps = chosen_est_net_ev_bps
                execution_decision.chosen_est_net_ev_total = chosen_est_net_ev_total
                execution_decision.chosen_est_fee = chosen_est_fee
                execution_decision.chosen_est_slippage = chosen_est_slippage
                execution_decision.chosen_policy_version = chosen_policy_version
                execution_decision.decision_reason_json = _json_safe(decision_reason_json)
                execution_decision.details = details_payload
            await session.flush()
            execution_decision_row = execution_decision
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

    async def finish_retryable_pending(
        *,
        reason_code: str,
        detail: str | None = None,
        diagnostics: dict | None = None,
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
        pending_diagnostics = dict(diagnostics or {})
        previous_details = dict(execution_decision_row.details or {}) if execution_decision_row is not None else {}
        previous_diagnostics = (
            previous_details.get("diagnostics")
            if isinstance(previous_details.get("diagnostics"), dict)
            else {}
        )
        previous_attempts = _int_or_zero(previous_diagnostics.get("retry_attempt_count"))
        existing_decision_at = (
            _ensure_utc(execution_decision_row.decision_at)
            if execution_decision_row is not None
            else None
        )
        first_pending_at = (
            previous_diagnostics.get("first_pending_decision_at")
            or (existing_decision_at.isoformat() if existing_decision_at is not None else None)
            or decision_at.isoformat()
        )
        pending_diagnostics.setdefault("retry_pending", True)
        pending_diagnostics.setdefault("retry_reason_code", reason_code)
        pending_diagnostics.setdefault("retry_reason_label", _reason_label(reason_code))
        pending_diagnostics["retry_attempt_count"] = previous_attempts + 1
        pending_diagnostics["first_pending_decision_at"] = first_pending_at
        pending_diagnostics["last_pending_attempt_at"] = datetime.now(timezone.utc).isoformat()
        return await finish(
            decision="pending_decision",
            reason_code=reason_code,
            reason_label=_reason_label(reason_code),
            detail=detail,
            diagnostics=pending_diagnostics,
            action="pending",
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
            ideal_ev=ideal_ev,
            provisional_sizing=provisional_sizing,
            shadow_execution=shadow_execution,
            executable_ev=executable_ev,
            executable_sizing=executable_sizing,
            fill_capped_size_usd=fill_capped_size_usd,
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
            decision_reason_json=decision_reason_json,
            polymarket_policy_result=polymarket_policy_result,
        )

    if strategy_run_id is not None:
        existing_decision_result = await session.execute(
            select(ExecutionDecision).where(
                ExecutionDecision.signal_id == signal_id,
                ExecutionDecision.strategy_run_id == strategy_run_id,
            )
        )
        execution_decision_row = existing_decision_result.scalars().first()
        if execution_decision_row is not None and execution_decision_row.decision_status != "pending_decision":
            return ExecutionDecisionBuildResult(
                execution_decision=execution_decision_row,
                decision="skipped",
                reason_code="already_recorded",
                reason_label=_reason_label("already_recorded"),
                detail=f"Signal already has an execution decision in strategy run ({execution_decision_row.id})",
                diagnostics={
                    "existing_execution_decision_id": str(execution_decision_row.id),
                    "existing_decision_status": execution_decision_row.decision_status,
                    "existing_reason_code": execution_decision_row.reason_code,
                },
            )

    strategy_run = await _load_strategy_run(session, strategy_run_id=strategy_run_id)
    if strategy_run_id is not None and strategy_run is None:
        return await finish(
            decision="skipped",
            reason_code="risk_state_uninitialized",
            detail=f"Strategy run {strategy_run_id} no longer exists",
        )
    if strategy_run is not None and not strategy_run_state_complete(strategy_run):
        await hydrate_strategy_run_state(session, strategy_run)
    if strategy_run is not None and not strategy_run_state_complete(strategy_run):
        return await finish(
            decision="skipped",
            reason_code="risk_state_uninitialized",
            detail="Strategy run drawdown state is not initialized. Start a fresh run before trading.",
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
                execution_decision=execution_decision_row,
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

    if strategy_run_id is not None and execution_decision_row is None:
        execution_decision_row = ExecutionDecision(
            id=uuid.uuid4(),
            signal_id=signal_id,
            strategy_run_id=strategy_run_id,
            decision_at=decision_at,
            decision_status="pending_decision",
            action="pending",
            direction=None,
            reason_code="pending_decision",
            decision_reason_json=None,
            details=_json_safe(
                {
                    "reason_label": _reason_label("pending_decision"),
                    "detail": None,
                    "market_id": market_id,
                    "market_question": market_question,
                    "estimated_probability": estimated_probability,
                    "market_price": market_price,
                }
            ),
        )
        session.add(execution_decision_row)
        await session.flush()

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
        if derived_precheck_reason_code == "pending_decision":
            return await finish(
                decision="pending_decision",
                reason_code="pending_decision",
                reason_label=derived_precheck_reason_label,
                action="pending",
            )
        return await finish(
            decision="skipped",
            reason_code=derived_precheck_reason_code,
            reason_label=derived_precheck_reason_label,
        )

    profitability_precheck = await _paper_profitability_market_precheck(
        session,
        market_id=market_id,
        observed_at=datetime.now(timezone.utc),
    )
    if profitability_precheck is not None:
        return await finish(
            decision="skipped",
            reason_code=profitability_precheck["reason_code"],
            detail=profitability_precheck["detail"],
            diagnostics=profitability_precheck["diagnostics"],
        )

    duplicate_precheck = await _paper_market_duplicate_precheck(
        session,
        strategy_run_id=strategy_run_id,
        market_id=market_id,
        outcome_id=outcome_id,
        observed_at=datetime.now(timezone.utc),
    )
    if duplicate_precheck is not None:
        return await finish(
            decision="skipped",
            reason_code=duplicate_precheck["reason_code"],
            detail=duplicate_precheck["detail"],
            diagnostics=duplicate_precheck["diagnostics"],
        )

    bankroll = Decimal(str(settings.default_bankroll))
    resolved_min_ev_threshold = (
        Decimal(str(settings.min_ev_threshold))
        if min_ev_threshold is None
        else Decimal(str(min_ev_threshold))
    )

    ideal_ev = compute_ev_full(estimated_probability, market_price)
    if ideal_ev["ev_per_share"] < resolved_min_ev_threshold:
        return await finish(
            decision="skipped",
            reason_code="ev_below_threshold",
            detail=f"Directional EV {ideal_ev['ev_per_share']} below threshold {resolved_min_ev_threshold}",
            diagnostics={
                "direction": ideal_ev["direction"],
                "ev_per_share": str(ideal_ev["ev_per_share"]),
                "edge_pct": str(ideal_ev["edge_pct"]),
                "min_ev_threshold": str(resolved_min_ev_threshold),
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

            portfolio = await portfolio_views_module._get_portfolio_state(session, strategy_run_id=strategy_run_id)
            open_positions = [
                {
                    "size_usd": t.size_usd,
                    "market_question": (t.details or {}).get("market_question", ""),
                    "outcome_id": str(t.outcome_id),
                }
                for t in portfolio["open_trades"]
            ]
            peak_bankroll, cumulative_pnl = _risk_context_from_run(
                strategy_run,
                cumulative_pnl=portfolio["cumulative_pnl"],
            )

            risk_result = await assess_paper_trade_risk(
                session,
                outcome_id=outcome_id,
                market_id=market_id,
                direction=ideal_ev["direction"],
                proposed_notional_usd=fill_capped_size_usd,
                strategy_family=(strategy_run.strategy_family if strategy_run is not None and strategy_run.strategy_family else "default_strategy"),
                strategy_version_id=(strategy_run.strategy_version_id if strategy_run is not None else None),
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
                    cumulative_pnl=cumulative_pnl,
                )
                risk_result = _normalize_risk_result(risk_result, upstream_source="local_paper_book")
            else:
                risk_result = _normalize_risk_result(risk_result, upstream_source="risk_graph")
            if not risk_result["approved"]:
                logger.info(
                    "Paper trade rejected by risk check: %s (signal %s)",
                    risk_result["reason"], signal_id,
                )
                reason_code = str(
                    risk_result.get("reason_code")
                    or ("risk_shared_global_block" if risk_result.get("risk_scope") == "shared_global" else "risk_local_rejected")
                )
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
                        "risk_result": _json_safe(risk_result),
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
                        "risk_result": _json_safe(risk_result),
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

    shadow_execution = await shadow_execution_module.build_shadow_execution(
        session=session,
        outcome_id=outcome_id,
        direction=ideal_ev["direction"],
        approved_size=provisional_sizing["recommended_size_usd"],
        ideal_entry_price=ideal_ev["entry_price"],
        fired_at=fired_at,
    )
    shadow_details = shadow_execution["details"]
    fillable_size_usd = shadow_execution_module.parse_decimal(shadow_details.get("filled_size_usd")) or ZERO
    fill_probability = shadow_execution_module.parse_decimal(shadow_details.get("fill_pct"))

    if shadow_details.get("missing_orderbook_context") is True:
        reason_code = (
            "execution_stale_orderbook_context"
            if shadow_details.get("stale_orderbook_context") is True
            else "execution_missing_orderbook_context"
        )
        return await finish_retryable_pending(
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
        return await finish_retryable_pending(
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
        return await finish_retryable_pending(
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
    if executable_ev["ev_per_share"] < resolved_min_ev_threshold:
        return await finish(
            decision="skipped",
            reason_code="execution_ev_below_threshold",
            detail=(
                f"Executable EV {executable_ev['ev_per_share']} below threshold "
                f"{resolved_min_ev_threshold}"
            ),
            diagnostics={
                "direction": ideal_ev["direction"],
                "ideal_ev_per_share": str(ideal_ev["ev_per_share"]),
                "executable_ev_per_share": str(executable_ev["ev_per_share"]),
                "executable_entry_price": str(executable_entry_price),
                "min_ev_threshold": str(resolved_min_ev_threshold),
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

    portfolio = await portfolio_views_module._get_portfolio_state(session, strategy_run_id=strategy_run_id)
    open_positions = [
        {
            "size_usd": t.size_usd,
            "market_question": (t.details or {}).get("market_question", ""),
            "outcome_id": str(t.outcome_id),
        }
        for t in portfolio["open_trades"]
    ]
    peak_bankroll, cumulative_pnl = _risk_context_from_run(
        strategy_run,
        cumulative_pnl=portfolio["cumulative_pnl"],
    )

    risk_result = await assess_paper_trade_risk(
        session,
        outcome_id=outcome_id,
        market_id=market_id,
        direction=ideal_ev["direction"],
        proposed_notional_usd=fill_capped_size_usd,
        strategy_family=(strategy_run.strategy_family if strategy_run is not None and strategy_run.strategy_family else "default_strategy"),
        strategy_version_id=(strategy_run.strategy_version_id if strategy_run is not None else None),
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
            cumulative_pnl=cumulative_pnl,
        )
        risk_result = _normalize_risk_result(risk_result, upstream_source="local_paper_book")
    else:
        risk_result = _normalize_risk_result(risk_result, upstream_source="risk_graph")
    if not risk_result["approved"]:
        logger.info(
            "Paper trade rejected by risk check: %s (signal %s)",
            risk_result["reason"], signal_id,
        )
        reason_code = str(
            risk_result.get("reason_code")
            or ("risk_shared_global_block" if risk_result.get("risk_scope") == "shared_global" else "risk_local_rejected")
        )
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
                "risk_result": _json_safe(risk_result),
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
    min_ev_threshold: Decimal | None = None,
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
        min_ev_threshold=min_ev_threshold,
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
    strategy_version_id = await _resolve_strategy_version_id(
        session,
        strategy_run_id=strategy_run_id,
    )
    trade = PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        strategy_run_id=strategy_run_id,
        strategy_version_id=strategy_version_id,
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
            "risk_control": decision_result.diagnostics.get("risk_result"),
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

    strategy_run = await _load_strategy_run(session, strategy_run_id=strategy_run_id)
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
    portfolio = await portfolio_views_module._get_portfolio_state(session, strategy_run_id=strategy_run_id)
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
        strategy_family=(strategy_run.strategy_family if strategy_run is not None and strategy_run.strategy_family else "default_strategy"),
        strategy_version_id=(strategy_run.strategy_version_id if strategy_run is not None else None),
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
    shadow_execution = await shadow_execution_module.build_shadow_execution(
        session=session,
        outcome_id=outcome_id,
        direction=sizing["direction"],
        approved_size=approved_size,
        ideal_entry_price=entry_price,
        fired_at=fired_at,
    )
    strategy_version_id = await _resolve_strategy_version_id(
        session,
        strategy_run_id=strategy_run_id,
    )

    trade = PaperTrade(
        id=uuid.uuid4(),
        signal_id=signal_id,
        strategy_run_id=strategy_run_id,
        strategy_version_id=strategy_version_id,
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


async def ensure_pending_execution_decision(
    session: AsyncSession,
    *,
    signal_id: uuid.UUID,
    outcome_id: uuid.UUID | None,
    market_id: uuid.UUID,
    estimated_probability: Decimal | None,
    market_price: Decimal | None,
    market_question: str = "",
    fired_at: datetime | None = None,
    strategy_run_id: uuid.UUID | None = None,
) -> ExecutionDecision | None:
    result = await build_execution_decision(
        session=session,
        signal_id=signal_id,
        outcome_id=outcome_id,
        market_id=market_id,
        estimated_probability=estimated_probability,
        market_price=market_price,
        market_question=market_question,
        fired_at=fired_at,
        strategy_run_id=strategy_run_id,
        precheck_reason_code="pending_decision",
        precheck_reason_label=_reason_label("pending_decision"),
    )
    return result.execution_decision


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
    strategy_runs: dict[uuid.UUID, StrategyRun] = {}

    for trade in sorted(trades, key=lambda row: str(row.id)):
        if trade.direction == "buy_yes":
            exit_price = Decimal("1.000000") if outcome_won else Decimal("0.000000")
        else:  # buy_no
            exit_price = Decimal("0.000000") if outcome_won else Decimal("1.000000")

        # P&L = shares * (exit_price - entry_price)
        pnl = (trade.shares * (exit_price - trade.entry_price)).quantize(Decimal("0.01"))
        shadow_pnl = None
        shadow_shares = shadow_execution_module.shadow_shares_from_trade(trade)
        if shadow_shares == ZERO:
            shadow_pnl = ZERO.quantize(Decimal("0.01"))
        elif trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
            shadow_pnl = (shadow_shares * (exit_price - trade.shadow_entry_price)).quantize(Decimal("0.01"))

        trade.exit_price = exit_price
        trade.pnl = pnl
        trade.shadow_pnl = shadow_pnl
        trade.status = "resolved"
        trade.resolved_at = now
        if trade.strategy_run_id is not None:
            strategy_run = strategy_runs.get(trade.strategy_run_id)
            if strategy_run is None:
                strategy_run = await session.get(StrategyRun, trade.strategy_run_id)
                if strategy_run is not None:
                    if not strategy_run_state_complete(strategy_run):
                        initialize_strategy_run_state(strategy_run)
                    strategy_runs[trade.strategy_run_id] = strategy_run
            if strategy_run is not None:
                apply_trade_resolution_to_run(strategy_run, pnl=pnl)
        count += 1

        logger.info(
            "Paper trade resolved: %s %s, entry=$%s exit=$%s, P&L=$%s",
            trade.direction, trade.outcome_id, trade.entry_price, exit_price, pnl,
        )

    if count > 0:
        await session.flush()

    return count


async def get_metrics(session: AsyncSession) -> dict:
    return await portfolio_views_module.get_metrics(session)


async def get_pnl_curve(session: AsyncSession) -> list[dict]:
    return await portfolio_views_module.get_pnl_curve(session)
