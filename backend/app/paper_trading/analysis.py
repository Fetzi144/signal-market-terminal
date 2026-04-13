"""Analysis helpers for the default paper-trading strategy."""
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import (
    default_strategy_skip_label,
    evaluate_default_strategy_signal,
)
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.signals.probability import brier_score
from app.strategy_runs.service import ensure_active_default_strategy_run, serialize_strategy_run

ZERO = Decimal("0")


def _safe_float(value):
    return float(value) if value is not None else None


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
        return "not_started"
    if traded_signals == 0:
        return "live_waiting_for_trades"
    if days_tracked is None:
        return "not_started"
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


def _portfolio_from_trade_rows(trade_rows: list[tuple[PaperTrade, Signal]]) -> dict:
    open_trades = [trade for trade, _signal in trade_rows if trade.status == "open"]
    resolved_trades = [trade for trade, _signal in trade_rows if trade.status == "resolved" and trade.pnl is not None]
    cumulative_pnl = sum((trade.pnl or ZERO for trade in resolved_trades), ZERO)
    wins = sum(1 for trade in resolved_trades if trade.pnl is not None and trade.pnl > ZERO)
    losses = sum(1 for trade in resolved_trades if trade.pnl is not None and trade.pnl <= ZERO)
    total_resolved = len(resolved_trades)

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
    resolved_rows = [
        (trade, signal)
        for trade, signal in sorted(trade_rows, key=_trade_resolved_sort_key)
        if trade.status == "resolved" and trade.pnl is not None
    ]
    if not resolved_rows:
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

    pnls = [float(trade.pnl) for trade, _signal in resolved_rows if trade.pnl is not None]
    shadow_pnls = [float(trade.shadow_pnl) for trade, _signal in resolved_rows if trade.shadow_pnl is not None]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl <= 0]
    shadow_wins = [pnl for pnl in shadow_pnls if pnl > 0]
    shadow_losses = [pnl for pnl in shadow_pnls if pnl <= 0]

    cumulative = []
    running = 0.0
    for pnl in pnls:
        running += pnl
        cumulative.append(running)

    peak = 0.0
    max_drawdown = 0.0
    for value in cumulative:
        if value > peak:
            peak = value
        drawdown = peak - value
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((pnl - mean_pnl) ** 2 for pnl in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance)
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

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
        for trade, _signal in resolved_rows
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("liquidity_constrained") is True
    )
    trades_missing_orderbook_context = sum(
        1
        for trade, _signal in resolved_rows
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
    resolved_rows = [
        (trade, signal)
        for trade, signal in sorted(trade_rows, key=_trade_resolved_sort_key)
        if trade.status == "resolved" and trade.pnl is not None and trade.resolved_at is not None
    ]
    curve = []
    running = Decimal("0")
    for trade, _signal in resolved_rows:
        running += trade.pnl or ZERO
        curve.append({
            "timestamp": _ensure_utc(trade.resolved_at).isoformat(),
            "pnl": float(running),
            "trade_pnl": float(trade.pnl),
            "shadow_trade_pnl": float(trade.shadow_pnl) if trade.shadow_pnl is not None else None,
            "direction": trade.direction,
            "trade_id": str(trade.id),
        })
    return curve


def _dedupe_signals_by_trade_rows(
    trade_rows: list[tuple[PaperTrade, Signal]],
    *,
    resolved_only: bool = False,
) -> list[Signal]:
    ordered_rows = sorted(
        trade_rows,
        key=_trade_resolved_sort_key if resolved_only else _trade_opened_sort_key,
    )
    deduped: list[Signal] = []
    seen: set = set()
    for trade, signal in ordered_rows:
        if resolved_only and trade.status != "resolved":
            continue
        if signal.id in seen:
            continue
        seen.add(signal.id)
        deduped.append(signal)
    return deduped


def _default_strategy_metadata(signal: Signal) -> dict:
    details = signal.details or {}
    metadata = details.get("default_strategy")
    return metadata if isinstance(metadata, dict) else {}


async def _get_default_strategy_scope(session: AsyncSession) -> dict:
    required_signal_type = settings.default_strategy_signal_type
    strategy_run = await ensure_active_default_strategy_run(session)
    launch_at = _ensure_utc(strategy_run.started_at)

    signal_query = select(Signal)
    if required_signal_type:
        signal_query = signal_query.where(Signal.signal_type == required_signal_type)
    signal_result = await session.execute(signal_query)
    all_candidate_signals = signal_result.scalars().all()
    all_candidate_signals.sort(key=_signal_sort_key)

    if launch_at is not None:
        pre_launch_candidate_signals = [
            signal for signal in all_candidate_signals
            if (_ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc)) < launch_at
        ]
        candidate_signals = [
            signal for signal in all_candidate_signals
            if (_ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc)) >= launch_at
        ]
    else:
        pre_launch_candidate_signals = []
        candidate_signals = all_candidate_signals

    candidate_evaluations = [
        (signal, evaluate_default_strategy_signal(signal, started_at=launch_at))
        for signal in candidate_signals
    ]
    qualified_signals = [signal for signal, evaluation in candidate_evaluations if evaluation.eligible]
    qualified_signal_ids = {signal.id for signal in qualified_signals}

    trade_result = await session.execute(
        select(PaperTrade, Signal)
        .join(Signal, Signal.id == PaperTrade.signal_id)
        .order_by(PaperTrade.opened_at.desc())
    )
    all_trade_rows = trade_result.all()
    strategy_trade_rows = [
        (trade, signal)
        for trade, signal in all_trade_rows
        if trade.strategy_run_id == strategy_run.id
    ]
    strategy_trade_rows.sort(key=_trade_opened_sort_key, reverse=True)

    traded_signal_ids = {signal.id for _trade, signal in strategy_trade_rows}
    open_trade_rows = [(trade, signal) for trade, signal in strategy_trade_rows if trade.status == "open"]
    resolved_trade_rows = [
        (trade, signal)
        for trade, signal in strategy_trade_rows
        if trade.status == "resolved" and trade.pnl is not None
    ]
    resolved_trade_rows.sort(key=_trade_resolved_sort_key)

    resolved_trade_signals = _dedupe_signals_by_trade_rows(resolved_trade_rows, resolved_only=True)
    resolved_trade_signal_ids = {signal.id for signal in resolved_trade_signals}

    first_trade_at = None
    if strategy_trade_rows:
        first_trade_at = min(_ensure_utc(trade.opened_at) for trade, _signal in strategy_trade_rows)

    started_at = launch_at

    portfolio = _portfolio_from_trade_rows(strategy_trade_rows)
    metrics = _metrics_from_trade_rows(strategy_trade_rows)
    pnl_curve = _pnl_curve_from_trade_rows(strategy_trade_rows)

    skip_reason_counts: dict[str, dict] = {}
    for signal, evaluation in candidate_evaluations:
        if signal.id in traded_signal_ids:
            continue

        metadata = _default_strategy_metadata(signal)
        if metadata.get("strategy_run_id") not in (None, str(strategy_run.id)):
            continue
        reason_code = metadata.get("reason_code") or evaluation.reason_code or "unclassified"
        reason_label = metadata.get("reason_label") or evaluation.reason_label or default_strategy_skip_label(reason_code) or "Unclassified"
        bucket = skip_reason_counts.setdefault(
            reason_code,
            {
                "reason_code": reason_code,
                "reason_label": reason_label,
                "count": 0,
            },
        )
        bucket["count"] += 1

    excluded_pre_launch_trades = 0
    if launch_at is not None:
        excluded_pre_launch_trades = sum(
            1
            for trade, signal in all_trade_rows
            if required_signal_type
            and signal.signal_type == required_signal_type
            and ((_ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc)) < launch_at)
        )

    funnel = {
        "candidate_signals": len(candidate_signals),
        "qualified_signals": len(qualified_signals),
        "traded_signals": len(traded_signal_ids),
        "qualified_not_traded": max(0, len(qualified_signals) - len(traded_signal_ids)),
        "open_trades": len(open_trade_rows),
        "resolved_trades": metrics["total_trades"],
        "resolved_signals": len(resolved_trade_signal_ids),
        "unresolved_traded_signals": max(0, len(traded_signal_ids) - len(resolved_trade_signal_ids)),
        "pre_launch_candidate_signals": len(pre_launch_candidate_signals),
        "excluded_pre_launch_trades": excluded_pre_launch_trades,
        "excluded_legacy_trades": sum(1 for trade, _signal in all_trade_rows if trade.strategy_run_id != strategy_run.id),
    }

    return {
        "strategy_run": strategy_run,
        "candidate_signals": candidate_signals,
        "pre_launch_candidate_signals": pre_launch_candidate_signals,
        "qualified_signals": qualified_signals,
        "strategy_trade_rows": strategy_trade_rows,
        "resolved_trade_rows": resolved_trade_rows,
        "resolved_trade_signals": resolved_trade_signals,
        "portfolio": portfolio,
        "metrics": metrics,
        "pnl_curve": pnl_curve,
        "funnel": funnel,
        "started_at": started_at,
        "launch_at": launch_at,
        "first_trade_at": first_trade_at,
        "skip_reasons": sorted(skip_reason_counts.values(), key=lambda row: (-row["count"], row["reason_label"])),
    }


async def get_strategy_portfolio_state(session: AsyncSession) -> dict:
    scope = await _get_default_strategy_scope(session)
    return scope["portfolio"]


async def get_strategy_metrics(session: AsyncSession) -> dict:
    scope = await _get_default_strategy_scope(session)
    return scope["metrics"]


async def get_strategy_pnl_curve(session: AsyncSession) -> list[dict]:
    scope = await _get_default_strategy_scope(session)
    return scope["pnl_curve"]


async def get_strategy_history(
    session: AsyncSession,
    *,
    status: str | None = None,
    direction: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    scope = await _get_default_strategy_scope(session)
    trade_rows = scope["strategy_trade_rows"]

    filtered_trades = []
    for trade, _signal in trade_rows:
        if status and trade.status != status:
            continue
        if direction and trade.direction != direction:
            continue
        filtered_trades.append(trade)

    total = len(filtered_trades)
    start = (page - 1) * page_size
    end = start + page_size

    return {
        "trades": filtered_trades[start:end],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


async def get_strategy_health(session: AsyncSession) -> dict:
    """Return the consolidated health view for the default strategy."""
    scope = await _get_default_strategy_scope(session)
    strategy_run = scope["strategy_run"]
    contract = strategy_run.contract_snapshot or {}
    portfolio = scope["portfolio"]
    metrics = scope["metrics"]
    now = datetime.now(timezone.utc)
    review_cutoff = now - timedelta(days=settings.strategy_review_lookback_days)
    launch_at = scope["launch_at"]
    if launch_at is not None and launch_at > review_cutoff:
        review_cutoff = launch_at

    started_at = scope["started_at"]
    first_trade_at = scope["first_trade_at"]
    if started_at is not None:
        days_tracked = round((now - started_at).total_seconds() / 86400, 1)
    else:
        days_tracked = None

    resolved_default_signals = scope["resolved_trade_signals"]
    default_clvs = [signal.clv for signal in resolved_default_signals if signal.clv is not None]
    default_profit_losses = [signal.profit_loss or ZERO for signal in resolved_default_signals]
    default_predictions = [
        (signal.estimated_probability, signal.resolved_correctly)
        for signal in resolved_default_signals
        if signal.estimated_probability is not None and signal.resolved_correctly is not None
    ]

    avg_clv = _average(default_clvs)
    default_brier = brier_score(default_predictions) if default_predictions else None
    default_total_profit_loss = sum(default_profit_losses, ZERO)
    default_max_drawdown = _compute_max_drawdown(default_profit_losses)

    legacy_signals: list[Signal] = []
    if started_at is not None:
        benchmark_query = select(Signal).where(
            Signal.rank_score >= Decimal(str(settings.legacy_benchmark_rank_threshold)),
            Signal.resolved_correctly.isnot(None),
            Signal.fired_at >= started_at,
        )
        benchmark_result = await session.execute(benchmark_query)
        legacy_signals = benchmark_result.scalars().all()
        legacy_signals.sort(key=_signal_sort_key)

    legacy_clvs = [signal.clv for signal in legacy_signals if signal.clv is not None]
    legacy_profit_losses = [signal.profit_loss or ZERO for signal in legacy_signals]
    legacy_total_profit_loss = sum(legacy_profit_losses, ZERO)
    legacy_max_drawdown = _compute_max_drawdown(legacy_profit_losses)
    legacy_wins = sum(1 for signal in legacy_signals if signal.resolved_correctly)

    trade_result = await session.execute(
        select(PaperTrade, Signal)
        .join(Signal, Signal.id == PaperTrade.signal_id)
        .order_by(PaperTrade.opened_at.desc())
    )
    all_trade_rows = trade_result.all()

    recent_mistakes = []
    trade_counts_by_type: dict[str, int] = {}
    trade_pnl_by_type: dict[str, Decimal] = {}
    for trade, signal in all_trade_rows:
        if launch_at is not None:
            fired_at = _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc)
            if fired_at < launch_at:
                continue
        trade_counts_by_type[signal.signal_type] = trade_counts_by_type.get(signal.signal_type, 0) + 1
        if trade.pnl is not None:
            trade_pnl_by_type[signal.signal_type] = trade_pnl_by_type.get(signal.signal_type, ZERO) + trade.pnl

    for trade, signal in sorted(scope["strategy_trade_rows"], key=_trade_resolved_sort_key, reverse=True):
        if len(recent_mistakes) >= settings.strategy_review_recent_mistakes_limit:
            break
        if trade.status != "resolved" or trade.pnl is None or trade.pnl >= ZERO:
            continue
        recent_mistakes.append({
            "trade_id": str(trade.id),
            "signal_id": str(signal.id),
            "signal_type": signal.signal_type,
            "market_question": (trade.details or {}).get("market_question", ""),
            "direction": trade.direction,
            "pnl": float(trade.pnl),
            "clv": _safe_float(signal.clv),
            "resolved_at": _ensure_utc(trade.resolved_at).isoformat() if trade.resolved_at else None,
        })

    detector_result = await session.execute(
        select(Signal).where(
            Signal.resolved_correctly.isnot(None),
            Signal.fired_at >= review_cutoff,
        )
    )
    review_signals = detector_result.scalars().all()

    detectors: dict[str, list[Signal]] = {}
    for signal in review_signals:
        detectors.setdefault(signal.signal_type, []).append(signal)

    detector_review = []
    for signal_type, signals in detectors.items():
        signals.sort(key=_signal_sort_key)
        clvs = [signal.clv for signal in signals if signal.clv is not None]
        profit_losses = [signal.profit_loss or ZERO for signal in signals]
        predictions = [
            (signal.estimated_probability, signal.resolved_correctly)
            for signal in signals
            if signal.estimated_probability is not None
        ]
        avg_detector_clv = _average(clvs)
        detector_brier = brier_score(predictions) if predictions else None
        total_profit_loss = sum(profit_losses, ZERO)
        verdict, note = _detector_verdict(
            len(signals),
            avg_detector_clv,
            total_profit_loss,
            detector_brier,
        )
        detector_review.append({
            "signal_type": signal_type,
            "resolved_signals": len(signals),
            "paper_trades": trade_counts_by_type.get(signal_type, 0),
            "avg_clv": _safe_float(avg_detector_clv.quantize(Decimal("0.000001"))) if avg_detector_clv is not None else None,
            "total_profit_loss": _safe_float(total_profit_loss.quantize(Decimal("0.000001"))),
            "paper_trade_pnl": _safe_float(trade_pnl_by_type.get(signal_type, ZERO).quantize(Decimal("0.01"))),
            "max_drawdown": _safe_float(_compute_max_drawdown(profit_losses).quantize(Decimal("0.000001"))),
            "brier_score": _safe_float(detector_brier.quantize(Decimal("0.000001"))) if detector_brier is not None else None,
            "verdict": verdict,
            "note": note,
        })

    detector_review.sort(key=lambda row: (row["total_profit_loss"] or 0, row["avg_clv"] or 0), reverse=True)

    review_status = _observation_status(
        days_tracked,
        launched=started_at is not None,
        traded_signals=scope["funnel"]["traded_signals"],
    )
    minimum_days = settings.default_strategy_min_observation_days
    remaining_days = 0
    if days_tracked is not None and days_tracked < minimum_days:
        remaining_days = max(0, math.ceil(minimum_days - days_tracked))

    return {
        "strategy": contract,
        "strategy_run": serialize_strategy_run(strategy_run),
        "observation": {
            "started_at": started_at.isoformat() if started_at else None,
            "baseline_start_at": launch_at.isoformat() if launch_at else None,
            "first_trade_at": first_trade_at.isoformat() if first_trade_at else None,
            "days_tracked": days_tracked,
            "status": review_status,
            "minimum_days": settings.default_strategy_min_observation_days,
            "preferred_days": settings.default_strategy_preferred_observation_days,
            "days_until_minimum_window": remaining_days,
        },
        "trade_funnel": scope["funnel"],
        "skip_reasons": scope["skip_reasons"],
        "headline": {
            "open_exposure": float(portfolio["open_exposure"]),
            "open_trades": len(portfolio["open_trades"]),
            "resolved_trades": metrics["total_trades"],
            "resolved_signals": scope["funnel"]["resolved_signals"],
            "missing_resolutions": scope["funnel"]["unresolved_traded_signals"],
            "cumulative_pnl": metrics["cumulative_pnl"],
            "avg_clv": _safe_float(avg_clv.quantize(Decimal("0.000001"))) if avg_clv is not None else None,
            "profit_factor": metrics["profit_factor"],
            "win_rate": metrics["win_rate"],
            "max_drawdown": metrics["max_drawdown"],
            "brier_score": _safe_float(default_brier.quantize(Decimal("0.000001"))) if default_brier is not None else None,
            "total_profit_loss_per_share": _safe_float(default_total_profit_loss.quantize(Decimal("0.000001"))),
            "max_drawdown_per_share": _safe_float(default_max_drawdown.quantize(Decimal("0.000001"))),
        },
        "execution_realism": {
            "shadow_cumulative_pnl": metrics["shadow_cumulative_pnl"],
            "shadow_profit_factor": metrics["shadow_profit_factor"],
            "liquidity_constrained_trades": metrics["liquidity_constrained_trades"],
            "trades_missing_orderbook_context": metrics["trades_missing_orderbook_context"],
        },
        "run_integrity": {
            "pre_launch_candidate_signals": scope["funnel"]["pre_launch_candidate_signals"],
            "excluded_pre_launch_trades": scope["funnel"]["excluded_pre_launch_trades"],
            "excluded_legacy_trades": scope["funnel"]["excluded_legacy_trades"],
            "trades_missing_orderbook_context": metrics["trades_missing_orderbook_context"],
        },
        "benchmark": {
            "label": "legacy_rank_threshold",
            "rank_threshold": settings.legacy_benchmark_rank_threshold,
            "resolved_signals": len(legacy_signals),
            "win_rate": round(legacy_wins / len(legacy_signals), 4) if legacy_signals else 0.0,
            "avg_clv": _safe_float(_average(legacy_clvs).quantize(Decimal("0.000001"))) if legacy_clvs else None,
            "total_profit_loss_per_share": _safe_float(legacy_total_profit_loss.quantize(Decimal("0.000001"))),
            "max_drawdown_per_share": _safe_float(legacy_max_drawdown.quantize(Decimal("0.000001"))),
            "delta_profit_loss_per_share": _safe_float((default_total_profit_loss - legacy_total_profit_loss).quantize(Decimal("0.000001"))),
            "delta_max_drawdown_per_share": _safe_float((default_max_drawdown - legacy_max_drawdown).quantize(Decimal("0.000001"))),
        },
        "detector_review": detector_review,
        "recent_mistakes": recent_mistakes,
        "review_questions": [
            "Did the default strategy make money this week?",
            "Was average CLV positive for the traded path?",
            "How many qualified default-strategy signals actually turned into trades?",
            "Which skip reasons are dominating the funnel right now?",
            "Which detectors earned a keep/watch/cut verdict?",
            "Did confluence beat the legacy rank-threshold benchmark on P&L and drawdown?",
        ],
    }
