from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.alpha_rule_specs import (
    ALPHA_KALSHI_4237F81367_V1,
    enabled_alpha_rule_blueprints,
)
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
from app.strategies.kalshi_orderbook_capture import capture_targeted_kalshi_orderbook_snapshot
from app.strategies.registry import get_current_strategy_version, sync_strategy_registry
from app.strategy_runs.service import ACTIVE_RUN_STATUS, get_active_strategy_run

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")

PRICE_BUCKET_RANGES: dict[str, tuple[Decimal | None, Decimal | None]] = {
    "p00_005": (Decimal("0"), Decimal("0.05")),
    "p005_010": (Decimal("0.05"), Decimal("0.10")),
    "p010_020": (Decimal("0.10"), Decimal("0.20")),
    "p020_050": (Decimal("0.20"), Decimal("0.50")),
    "p050_080": (Decimal("0.50"), Decimal("0.80")),
    "p080_090": (Decimal("0.80"), Decimal("0.90")),
    "p090_100": (Decimal("0.90"), Decimal("1")),
}
EXPECTED_VALUE_BUCKET_RANGES: dict[str, tuple[Decimal | None, Decimal | None]] = {
    "ev_neg": (None, Decimal("0")),
    "ev_000_001": (Decimal("0"), Decimal("0.01")),
    "ev_001_002": (Decimal("0.01"), Decimal("0.02")),
    "ev_002_005": (Decimal("0.02"), Decimal("0.05")),
    "ev_005_plus": (Decimal("0.05"), None),
}
MONEY_BUCKET_RANGES: dict[str, tuple[Decimal | None, Decimal | None]] = {
    "000_001k": (Decimal("0"), Decimal("1000")),
    "001k_010k": (Decimal("1000"), Decimal("10000")),
    "010k_100k": (Decimal("10000"), Decimal("100000")),
    "100k_plus": (Decimal("100000"), None),
}


@dataclass(frozen=True)
class AlphaRuleEvaluation:
    in_scope: bool
    eligible: bool
    reason_code: str
    reason_label: str
    detail: str | None = None
    diagnostics: dict[str, Any] | None = None


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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _details(signal: Signal) -> dict[str, Any]:
    return signal.details if isinstance(signal.details, dict) else {}


def _rule(blueprint: dict[str, Any]) -> dict[str, Any]:
    return dict(blueprint.get("frozen_rule") or blueprint.get("rule") or {})


def _reason_prefix(blueprint: dict[str, Any]) -> str:
    return str(blueprint.get("reason_prefix") or "alpha_rule")


def _reason_code(blueprint: dict[str, Any], suffix: str) -> str:
    return f"{_reason_prefix(blueprint)}_{suffix}"


def _reason_label(reason_code: str) -> str:
    labels = {
        "candidate": "Frozen alpha-rule candidate",
        "missing_direction": "Missing price-move direction",
        "missing_outcome": "Missing outcome",
        "missing_price": "Missing YES price",
        "missing_probability": "Missing estimated probability",
        "missing_expected_value": "Missing expected value",
        "missing_liquidity": "Missing market liquidity",
        "price_bucket": "YES price outside frozen bucket",
        "expected_value_bucket": "Expected value outside frozen bucket",
        "liquidity_bucket": "Market liquidity outside frozen bucket",
        "probability_not_above_price": "Probability is not above YES price",
        "probability_not_below_price": "Probability is not below YES price",
        "current_price_unavailable": "Current YES price unavailable",
        "current_price_outside_bucket": "Current YES price outside frozen bucket",
        "current_probability_not_above_price": "Current probability is not above YES price",
        "current_probability_not_below_price": "Current probability is not below YES price",
    }
    normalized = reason_code.removeprefix("not_")
    for suffix, label in labels.items():
        if normalized == suffix or normalized.endswith(f"_{suffix}"):
            return label
    return reason_code.replace("_", " ")


def _bucket_bounds(bucket: str, ranges: dict[str, tuple[Decimal | None, Decimal | None]]) -> tuple[Decimal | None, Decimal | None] | None:
    if bucket in {"", "all", "unknown", "price_unknown", "ev_unknown", "liquidity_unknown"}:
        return None
    return ranges.get(bucket)


def _value_in_bounds(value: Decimal, bounds: tuple[Decimal | None, Decimal | None]) -> bool:
    lower, upper = bounds
    if lower is not None and value < lower:
        return False
    if upper is not None and value >= upper:
        return False
    return True


def _money_bucket_bounds(bucket: str) -> tuple[Decimal | None, Decimal | None] | None:
    if bucket in {"", "all", "unknown", "liquidity_unknown", "volume_unknown"}:
        return None
    suffix = bucket.removeprefix("liquidity_").removeprefix("volume_")
    return MONEY_BUCKET_RANGES.get(suffix)


def _platform_for_signal(signal: Signal, *, market_platform: str | None = None) -> str:
    details = _details(signal)
    return _normalize_text(signal.source_platform or market_platform or details.get("platform"))


def _direction_for_signal(signal: Signal) -> str:
    return _normalize_text(_details(signal).get("direction"))


def _timeframe_for_signal(signal: Signal) -> str:
    return _normalize_text(getattr(signal, "timeframe", None) or "unknown")


def _market_liquidity(market: Market | None) -> Decimal | None:
    if market is None:
        return None
    return _decimal(market.last_liquidity)


def _build_diagnostics(
    signal: Signal,
    *,
    blueprint: dict[str, Any],
    market: Market | None,
    market_platform: str | None,
) -> dict[str, Any]:
    rule = _rule(blueprint)
    yes_price = _decimal(signal.price_at_fire)
    expected_value = _decimal(signal.expected_value)
    estimated_probability = _decimal(signal.estimated_probability)
    liquidity = _market_liquidity(market)
    return {
        "strategy_family": blueprint.get("strategy_family"),
        "strategy_version": blueprint.get("strategy_version"),
        "candidate_id": blueprint.get("candidate_id"),
        "rule_digest": blueprint.get("rule_digest"),
        "rule_label": blueprint.get("rule_label"),
        "signal_type": _normalize_text(signal.signal_type),
        "platform": _platform_for_signal(signal, market_platform=market_platform) or None,
        "direction": _direction_for_signal(signal) or None,
        "timeframe": _timeframe_for_signal(signal),
        "yes_price": str(yes_price) if yes_price is not None else None,
        "expected_value": str(expected_value) if expected_value is not None else None,
        "estimated_probability": str(estimated_probability) if estimated_probability is not None else None,
        "market_liquidity": str(liquidity) if liquidity is not None else None,
        "intended_direction": blueprint.get("trade_direction"),
        "frozen_rule": rule,
    }


def evaluate_alpha_rule_signal(
    signal: Signal,
    *,
    blueprint: dict[str, Any] | None = None,
    market_platform: str | None = None,
    market: Market | None = None,
) -> AlphaRuleEvaluation:
    """Evaluate a frozen Alpha Factory blueprint without mutating state."""

    blueprint = blueprint or ALPHA_KALSHI_4237F81367_V1
    rule = _rule(blueprint)
    diagnostics = _build_diagnostics(
        signal,
        blueprint=blueprint,
        market=market,
        market_platform=market_platform,
    )
    signal_type = _normalize_text(signal.signal_type)
    expected_signal_type = _normalize_text(rule.get("signal_type") or "all")
    platform = _platform_for_signal(signal, market_platform=market_platform)
    expected_platform = _normalize_text(rule.get("platform") or "all")
    direction = _direction_for_signal(signal)
    expected_direction = _normalize_text(rule.get("direction") or "all")
    timeframe = _timeframe_for_signal(signal)
    expected_timeframe = _normalize_text(rule.get("timeframe") or "all")
    yes_price = _decimal(signal.price_at_fire)
    expected_value = _decimal(signal.expected_value)
    estimated_probability = _decimal(signal.estimated_probability)
    trade_direction = _normalize_text(blueprint.get("trade_direction"))

    if expected_signal_type != "all" and signal_type != expected_signal_type:
        return AlphaRuleEvaluation(
            in_scope=False,
            eligible=False,
            reason_code=f"not_{_reason_code(blueprint, 'universe')}",
            reason_label="Not in frozen alpha-rule universe",
            diagnostics=diagnostics,
        )
    if expected_platform != "all" and platform != expected_platform:
        return AlphaRuleEvaluation(
            in_scope=False,
            eligible=False,
            reason_code=f"not_{_reason_code(blueprint, 'platform')}",
            reason_label="Not on the frozen alpha-rule platform",
            diagnostics=diagnostics,
        )
    if expected_direction != "all":
        if not direction:
            return AlphaRuleEvaluation(
                in_scope=True,
                eligible=False,
                reason_code=_reason_code(blueprint, "missing_direction"),
                reason_label=_reason_label("missing_direction"),
                diagnostics=diagnostics,
            )
        if direction != expected_direction:
            return AlphaRuleEvaluation(
                in_scope=False,
                eligible=False,
                reason_code=f"not_{_reason_code(blueprint, 'direction')}",
                reason_label="Not the frozen alpha-rule direction",
                diagnostics=diagnostics,
            )
    if expected_timeframe != "all" and timeframe != expected_timeframe:
        return AlphaRuleEvaluation(
            in_scope=False,
            eligible=False,
            reason_code=f"not_{_reason_code(blueprint, 'timeframe')}",
            reason_label="Not the frozen alpha-rule timeframe",
            diagnostics=diagnostics,
        )
    if signal.outcome_id is None:
        return AlphaRuleEvaluation(
            in_scope=True,
            eligible=False,
            reason_code=_reason_code(blueprint, "missing_outcome"),
            reason_label=_reason_label("missing_outcome"),
            diagnostics=diagnostics,
        )

    price_bounds = _bucket_bounds(str(rule.get("price_bucket") or "all"), PRICE_BUCKET_RANGES)
    if price_bounds is not None:
        if yes_price is None:
            return AlphaRuleEvaluation(
                in_scope=True,
                eligible=False,
                reason_code=_reason_code(blueprint, "missing_price"),
                reason_label=_reason_label("missing_price"),
                diagnostics=diagnostics,
            )
        if not _value_in_bounds(yes_price, price_bounds):
            return AlphaRuleEvaluation(
                in_scope=False,
                eligible=False,
                reason_code=f"not_{_reason_code(blueprint, 'price_bucket')}",
                reason_label=_reason_label("price_bucket"),
                diagnostics=diagnostics,
            )

    ev_bounds = _bucket_bounds(str(rule.get("expected_value_bucket") or "all"), EXPECTED_VALUE_BUCKET_RANGES)
    if ev_bounds is not None:
        if expected_value is None:
            return AlphaRuleEvaluation(
                in_scope=True,
                eligible=False,
                reason_code=_reason_code(blueprint, "missing_expected_value"),
                reason_label=_reason_label("missing_expected_value"),
                diagnostics=diagnostics,
            )
        if not _value_in_bounds(expected_value, ev_bounds):
            return AlphaRuleEvaluation(
                in_scope=False,
                eligible=False,
                reason_code=f"not_{_reason_code(blueprint, 'expected_value_bucket')}",
                reason_label=_reason_label("expected_value_bucket"),
                diagnostics=diagnostics,
            )

    liquidity_bounds = _money_bucket_bounds(str(rule.get("liquidity_bucket") or "all"))
    if liquidity_bounds is not None:
        liquidity = _market_liquidity(market)
        if liquidity is None:
            return AlphaRuleEvaluation(
                in_scope=True,
                eligible=False,
                reason_code=_reason_code(blueprint, "missing_liquidity"),
                reason_label=_reason_label("missing_liquidity"),
                diagnostics=diagnostics,
            )
        if not _value_in_bounds(liquidity, liquidity_bounds):
            return AlphaRuleEvaluation(
                in_scope=False,
                eligible=False,
                reason_code=f"not_{_reason_code(blueprint, 'liquidity_bucket')}",
                reason_label=_reason_label("liquidity_bucket"),
                diagnostics=diagnostics,
            )

    if yes_price is None:
        return AlphaRuleEvaluation(
            in_scope=True,
            eligible=False,
            reason_code=_reason_code(blueprint, "missing_price"),
            reason_label=_reason_label("missing_price"),
            diagnostics=diagnostics,
        )
    if estimated_probability is None:
        return AlphaRuleEvaluation(
            in_scope=True,
            eligible=False,
            reason_code=_reason_code(blueprint, "missing_probability"),
            reason_label=_reason_label("missing_probability"),
            diagnostics=diagnostics,
        )
    if trade_direction == "buy_yes" and estimated_probability <= yes_price:
        return AlphaRuleEvaluation(
            in_scope=True,
            eligible=False,
            reason_code=_reason_code(blueprint, "probability_not_above_price"),
            reason_label=_reason_label("probability_not_above_price"),
            diagnostics=diagnostics,
        )
    if trade_direction == "buy_no" and estimated_probability >= yes_price:
        return AlphaRuleEvaluation(
            in_scope=True,
            eligible=False,
            reason_code=_reason_code(blueprint, "probability_not_below_price"),
            reason_label=_reason_label("probability_not_below_price"),
            diagnostics=diagnostics,
        )

    entry_edge = (
        estimated_probability - yes_price
        if trade_direction == "buy_yes"
        else yes_price - estimated_probability
    ).quantize(Decimal("0.000001"))
    diagnostics["edge_per_share"] = str(entry_edge)
    diagnostics["yes_entry_price"] = str(yes_price.quantize(Decimal("0.000001")))
    if trade_direction == "buy_no":
        diagnostics["no_entry_price"] = str((ONE - yes_price).quantize(Decimal("0.000001")))
    return AlphaRuleEvaluation(
        in_scope=True,
        eligible=True,
        reason_code=_reason_code(blueprint, "candidate"),
        reason_label=_reason_label("candidate"),
        diagnostics=diagnostics,
    )


async def ensure_active_alpha_rule_run(
    session: AsyncSession,
    blueprint: dict[str, Any] | None = None,
    *,
    started_at: datetime | None = None,
) -> tuple[StrategyRun, bool]:
    blueprint = blueprint or ALPHA_KALSHI_4237F81367_V1
    strategy_name = str(blueprint["strategy_name"])
    strategy_family = str(blueprint["strategy_family"])
    active = await get_active_strategy_run(session, strategy_name)
    if active is not None:
        return active, False

    await sync_strategy_registry(session)
    version = await get_current_strategy_version(session, strategy_family)
    resolved_started_at = _ensure_utc(started_at) or datetime.now(timezone.utc)
    strategy_run = StrategyRun(
        id=uuid.uuid4(),
        strategy_name=strategy_name,
        strategy_family=strategy_family,
        strategy_version_id=version.id if version is not None else None,
        status=ACTIVE_RUN_STATUS,
        started_at=resolved_started_at,
        contract_snapshot={
            "name": strategy_name,
            "strategy_family": strategy_family,
            "strategy_version_key": (
                version.version_key if version is not None else blueprint["strategy_version"]
            ),
            "strategy_version_label": (
                version.version_label if version is not None else blueprint.get("version_label")
            ),
            "strategy_version_status": (
                version.version_status if version is not None else "candidate"
            ),
            "paper_only": True,
            "live_orders_enabled": False,
            "pilot_arming_enabled": False,
            "thresholds_frozen": True,
            "alpha_factory_candidate_id": blueprint.get("candidate_id"),
            "rule_digest": blueprint.get("rule_digest"),
            "blueprint": blueprint,
            "rule": blueprint.get("frozen_rule") or {},
        },
    )
    initialize_strategy_run_state(strategy_run)
    session.add(strategy_run)
    await session.flush()
    return strategy_run, True


async def _markets_for_signals(
    session: AsyncSession,
    signals: Sequence[Signal],
) -> dict[uuid.UUID, Market]:
    market_ids = sorted({signal.market_id for signal in signals if signal.market_id is not None}, key=str)
    if not market_ids:
        return {}
    rows = (await session.execute(select(Market).where(Market.id.in_(market_ids)))).scalars().all()
    return {row.id: row for row in rows}


def _apply_sql_bounds(query, column, bounds: tuple[Decimal | None, Decimal | None] | None):
    if bounds is None:
        return query
    lower, upper = bounds
    if lower is not None:
        query = query.where(column >= lower)
    if upper is not None:
        query = query.where(column < upper)
    return query


async def load_unprocessed_alpha_rule_signals(
    session: AsyncSession,
    strategy_run: StrategyRun,
    blueprint: dict[str, Any] | None = None,
    *,
    exclude_signal_ids: Iterable[uuid.UUID] | None = None,
    limit: int = 100,
) -> list[Signal]:
    blueprint = blueprint or ALPHA_KALSHI_4237F81367_V1
    if limit <= 0:
        return []
    rule = _rule(blueprint)
    exclude_ids = [signal_id for signal_id in (exclude_signal_ids or [])]
    broad_limit = max(limit, min(limit * 10, 10_000))
    query = (
        select(Signal, Market)
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
            Signal.signal_type == str(rule.get("signal_type") or "price_move"),
            Signal.outcome_id.is_not(None),
            or_(Signal.source_platform == "kalshi", Market.platform == "kalshi"),
        )
        .order_by(Signal.fired_at.asc(), Signal.id.asc())
        .limit(broad_limit)
    )
    query = _apply_sql_bounds(
        query,
        Signal.price_at_fire,
        _bucket_bounds(str(rule.get("price_bucket") or "all"), PRICE_BUCKET_RANGES),
    )
    query = _apply_sql_bounds(
        query,
        Signal.expected_value,
        _bucket_bounds(str(rule.get("expected_value_bucket") or "all"), EXPECTED_VALUE_BUCKET_RANGES),
    )
    query = _apply_sql_bounds(
        query,
        Market.last_liquidity,
        _money_bucket_bounds(str(rule.get("liquidity_bucket") or "all")),
    )
    if exclude_ids:
        query = query.where(Signal.id.not_in(exclude_ids))
    rows = (await session.execute(query)).all()
    filtered = [
        signal
        for signal, market in rows
        if evaluate_alpha_rule_signal(
            signal,
            blueprint=blueprint,
            market_platform=market.platform if market is not None else None,
            market=market,
        ).eligible
    ]
    return filtered[:limit]


def _current_precheck(
    *,
    signal: Signal,
    blueprint: dict[str, Any],
    current_midpoint: Decimal | None,
) -> tuple[str | None, str | None]:
    rule = _rule(blueprint)
    trade_direction = _normalize_text(blueprint.get("trade_direction"))
    if current_midpoint is None:
        code = _reason_code(blueprint, "current_price_unavailable")
        return code, _reason_label("current_price_unavailable")
    price_bounds = _bucket_bounds(str(rule.get("price_bucket") or "all"), PRICE_BUCKET_RANGES)
    if price_bounds is not None and not _value_in_bounds(current_midpoint, price_bounds):
        code = _reason_code(blueprint, "current_price_outside_bucket")
        return code, _reason_label("current_price_outside_bucket")
    estimated_probability = _decimal(signal.estimated_probability)
    if estimated_probability is None:
        code = _reason_code(blueprint, "missing_probability")
        return code, _reason_label("missing_probability")
    if trade_direction == "buy_yes" and estimated_probability <= current_midpoint:
        code = _reason_code(blueprint, "current_probability_not_above_price")
        return code, _reason_label("current_probability_not_above_price")
    if trade_direction == "buy_no" and estimated_probability >= current_midpoint:
        code = _reason_code(blueprint, "current_probability_not_below_price")
        return code, _reason_label("current_probability_not_below_price")
    return None, None


async def run_alpha_rule_paper_lane(
    session: AsyncSession,
    signals: Sequence[Signal],
    *,
    blueprint: dict[str, Any] | None = None,
    pending_retry_limit: int = 100,
    backlog_limit: int = 100,
    pending_expiry_limit: int = 100,
    targeted_orderbook_limit: int = 25,
) -> dict[str, Any]:
    blueprint = blueprint or ALPHA_KALSHI_4237F81367_V1
    strategy_run, run_created = await ensure_active_alpha_rule_run(session, blueprint)
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

    pending_retry_signals: list[Signal] = []
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
    backlog_signals = await load_unprocessed_alpha_rule_signals(
        session,
        strategy_run,
        blueprint,
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
    markets = await _markets_for_signals(session, [signal for signal, _kind in work_items])

    candidate_count = 0
    opened_count = 0
    retry_candidates = 0
    backlog_candidates = 0
    targeted_orderbook_captures = 0
    targeted_orderbook_skips: dict[str, int] = {}
    skip_counts: dict[str, int] = {}
    changed = run_created or state_rehydrated or expired_pending or finalized_orderbook_context
    details_key = str(blueprint.get("signal_details_key") or blueprint["strategy_family"])
    trade_direction = _normalize_text(blueprint.get("trade_direction"))
    min_ev_threshold = _decimal(blueprint.get("paper_min_ev_threshold"))

    for signal, attempt_kind in work_items:
        market = markets.get(signal.market_id)
        evaluation = evaluate_alpha_rule_signal(
            signal,
            blueprint=blueprint,
            market_platform=market.platform if market is not None else None,
            market=market,
        )
        if not evaluation.in_scope:
            continue

        candidate_count += 1
        if attempt_kind == "retry":
            retry_candidates += 1
        elif attempt_kind == "backlog_repair":
            backlog_candidates += 1

        targeted_capture_result = None
        execution_market_price = signal.price_at_fire
        execution_observed_at = signal.fired_at
        current_precheck_reason_code = None
        current_precheck_reason_label = None
        if (
            evaluation.eligible
            and attempt_kind in {"fresh_signal", "retry", "backlog_repair"}
            and targeted_orderbook_captures < targeted_orderbook_limit
            and _normalize_text(blueprint.get("frozen_rule", {}).get("platform")) == "kalshi"
        ):
            captured_at = datetime.now(timezone.utc)
            targeted_capture_result = await capture_targeted_kalshi_orderbook_snapshot(
                session,
                signal,
                captured_at=captured_at,
                log_context=str(blueprint.get("version_label") or "Alpha rule paper lane"),
            )
            changed = True
            if targeted_capture_result.get("captured"):
                targeted_orderbook_captures += 1
                current_midpoint = _decimal(targeted_capture_result.get("midpoint"))
                execution_observed_at = captured_at
                current_precheck_reason_code, current_precheck_reason_label = _current_precheck(
                    signal=signal,
                    blueprint=blueprint,
                    current_midpoint=current_midpoint,
                )
                if current_precheck_reason_code is None:
                    execution_market_price = current_midpoint
            else:
                reason = str(targeted_capture_result.get("reason") or "targeted_orderbook_not_captured")
                targeted_orderbook_skips[reason] = targeted_orderbook_skips.get(reason, 0) + 1

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
                market_price=execution_market_price,
                market_question=market_question,
                fired_at=execution_observed_at,
                strategy_run_id=strategy_run.id,
            )
        result = await attempt_open_trade(
            session=session,
            signal_id=signal.id,
            outcome_id=signal.outcome_id,
            market_id=signal.market_id,
            estimated_probability=signal.estimated_probability,
            market_price=execution_market_price,
            market_question=market_question,
            fired_at=execution_observed_at,
            strategy_run_id=strategy_run.id,
            precheck_reason_code=(
                current_precheck_reason_code
                if current_precheck_reason_code is not None
                else (None if evaluation.eligible else evaluation.reason_code)
            ),
            precheck_reason_label=current_precheck_reason_label or evaluation.reason_label,
            min_ev_threshold=min_ev_threshold,
        )
        changed = True

        lane_details = dict(details.get(details_key) or {})
        lane_details.update(
            {
                "strategy_name": blueprint["strategy_name"],
                "strategy_family": blueprint["strategy_family"],
                "strategy_version": blueprint["strategy_version"],
                "strategy_run_id": str(strategy_run.id),
                "candidate_id": blueprint.get("candidate_id"),
                "rule_digest": blueprint.get("rule_digest"),
                "rule_label": blueprint.get("rule_label"),
                "evaluated_at": attempted_at,
                "attempt_kind": attempt_kind,
                "eligible": evaluation.eligible,
                "intended_direction": trade_direction,
                "targeted_orderbook_capture": targeted_capture_result,
                "execution_market_price": (
                    str(execution_market_price) if execution_market_price is not None else None
                ),
                "execution_observed_at": (
                    _ensure_utc(execution_observed_at).isoformat()
                    if execution_observed_at is not None
                    else None
                ),
                "current_execution_precheck_reason_code": current_precheck_reason_code,
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
        details[details_key] = lane_details
        signal.details = details

        if result.trade is not None:
            opened_count += 1
        else:
            skip_counts[result.reason_code or "unknown"] = skip_counts.get(result.reason_code or "unknown", 0) + 1

    if changed:
        await session.commit()

    if candidate_count or run_created or expired_pending or finalized_orderbook_context:
        logger.info(
            "%s paper lane: opened %d trade(s) from %d candidate signal(s)",
            blueprint.get("version_label") or blueprint.get("strategy_version"),
            opened_count,
            candidate_count,
        )
        if targeted_orderbook_captures or targeted_orderbook_skips:
            logger.info(
                "%s targeted orderbooks: captured=%d skips=%s",
                blueprint.get("version_label") or blueprint.get("strategy_version"),
                targeted_orderbook_captures,
                targeted_orderbook_skips,
            )
        if skip_counts:
            logger.info("%s skips by reason: %s", blueprint.get("strategy_version"), skip_counts)

    return {
        "strategy_run_id": str(strategy_run.id),
        "strategy_family": blueprint["strategy_family"],
        "strategy_version": blueprint["strategy_version"],
        "run_created": run_created,
        "state_rehydrated": state_rehydrated,
        "candidate_count": candidate_count,
        "opened_count": opened_count,
        "retry_candidates": retry_candidates,
        "backlog_candidates": backlog_candidates,
        "expired_pending_decisions": expired_pending,
        "finalized_orderbook_context_decisions": finalized_orderbook_context,
        "targeted_orderbook_captures": targeted_orderbook_captures,
        "targeted_orderbook_skips": targeted_orderbook_skips,
        "skip_counts": skip_counts,
    }


async def run_enabled_alpha_rule_paper_lanes(
    session: AsyncSession,
    signals: Sequence[Signal],
    *,
    pending_retry_limit: int = 100,
    backlog_limit: int = 100,
    pending_expiry_limit: int = 100,
    targeted_orderbook_limit: int = 25,
) -> list[dict[str, Any]]:
    results = []
    for blueprint in enabled_alpha_rule_blueprints():
        results.append(
            await run_alpha_rule_paper_lane(
                session,
                signals,
                blueprint=blueprint,
                pending_retry_limit=pending_retry_limit,
                backlog_limit=backlog_limit,
                pending_expiry_limit=pending_expiry_limit,
                targeted_orderbook_limit=targeted_orderbook_limit,
            )
        )
    return results
