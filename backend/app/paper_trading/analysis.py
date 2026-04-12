"""Analysis helpers for the default paper-trading strategy."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import get_default_strategy_contract, matches_default_strategy
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.paper_trading.engine import get_metrics, get_portfolio_state
from app.signals.probability import brier_score

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


def _observation_status(days_tracked: float | None) -> str:
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


async def get_strategy_health(session: AsyncSession) -> dict:
    """Return the consolidated health view for the default strategy."""
    contract = get_default_strategy_contract()
    portfolio = await get_portfolio_state(session)
    metrics = await get_metrics(session)
    now = datetime.now(timezone.utc)
    review_cutoff = now - timedelta(days=settings.strategy_review_lookback_days)

    signal_result = await session.execute(
        select(Signal).where(
            Signal.expected_value.isnot(None),
            Signal.estimated_probability.isnot(None),
            Signal.price_at_fire.isnot(None),
        )
    )
    all_signals = signal_result.scalars().all()

    default_signals = [signal for signal in all_signals if matches_default_strategy(signal)]
    resolved_default_signals = [signal for signal in default_signals if signal.resolved_correctly is not None]
    resolved_default_signals.sort(key=lambda signal: _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc))

    started_at = None
    if default_signals:
        started_at = min(
            _ensure_utc(signal.fired_at)
            for signal in default_signals
            if signal.fired_at is not None
        )

    if started_at is not None:
        days_tracked = round((now - started_at).total_seconds() / 86400, 1)
    else:
        days_tracked = None

    default_clvs = [signal.clv for signal in resolved_default_signals if signal.clv is not None]
    default_profit_losses = [signal.profit_loss or ZERO for signal in resolved_default_signals]
    default_predictions = [
        (signal.estimated_probability, signal.resolved_correctly)
        for signal in resolved_default_signals
        if signal.estimated_probability is not None
    ]

    avg_clv = _average(default_clvs)
    default_brier = brier_score(default_predictions) if default_predictions else None
    default_total_profit_loss = sum(default_profit_losses, ZERO)
    default_max_drawdown = _compute_max_drawdown(default_profit_losses)

    benchmark_query = select(Signal).where(
        Signal.rank_score >= Decimal(str(settings.legacy_benchmark_rank_threshold)),
        Signal.resolved_correctly.isnot(None),
    )
    if started_at is not None:
        benchmark_query = benchmark_query.where(Signal.fired_at >= started_at)

    benchmark_result = await session.execute(benchmark_query)
    legacy_signals = benchmark_result.scalars().all()
    legacy_signals.sort(key=lambda signal: _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc))
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
    trade_rows = trade_result.all()

    recent_mistakes = []
    trade_counts_by_type: dict[str, int] = {}
    trade_pnl_by_type: dict[str, Decimal] = {}
    for trade, signal in trade_rows:
        trade_counts_by_type[signal.signal_type] = trade_counts_by_type.get(signal.signal_type, 0) + 1
        if trade.pnl is not None:
            trade_pnl_by_type[signal.signal_type] = trade_pnl_by_type.get(signal.signal_type, ZERO) + trade.pnl
        if (
            len(recent_mistakes) < settings.strategy_review_recent_mistakes_limit
            and trade.status == "resolved"
            and trade.pnl is not None
            and trade.pnl < ZERO
        ):
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
        signals.sort(key=lambda signal: _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc))
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

    review_status = _observation_status(days_tracked)
    minimum_days = settings.default_strategy_min_observation_days
    remaining_days = 0
    if days_tracked is not None and days_tracked < minimum_days:
        remaining_days = max(0, minimum_days - int(days_tracked))

    return {
        "strategy": contract,
        "observation": {
            "started_at": started_at.isoformat() if started_at else None,
            "days_tracked": days_tracked,
            "status": review_status,
            "minimum_days": settings.default_strategy_min_observation_days,
            "preferred_days": settings.default_strategy_preferred_observation_days,
            "days_until_minimum_window": remaining_days,
        },
        "headline": {
            "open_exposure": float(portfolio["open_exposure"]),
            "open_trades": len(portfolio["open_trades"]),
            "resolved_trades": metrics["total_trades"],
            "resolved_signals": len(resolved_default_signals),
            "missing_resolutions": len(default_signals) - len(resolved_default_signals),
            "cumulative_pnl": metrics["cumulative_pnl"],
            "avg_clv": _safe_float(avg_clv.quantize(Decimal("0.000001"))) if avg_clv is not None else None,
            "profit_factor": metrics["profit_factor"],
            "win_rate": metrics["win_rate"],
            "max_drawdown": metrics["max_drawdown"],
            "brier_score": _safe_float(default_brier.quantize(Decimal("0.000001"))) if default_brier is not None else None,
            "total_profit_loss_per_share": _safe_float(default_total_profit_loss.quantize(Decimal("0.000001"))),
            "max_drawdown_per_share": _safe_float(default_max_drawdown.quantize(Decimal("0.000001"))),
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
            "Which detectors earned a keep/watch/cut verdict?",
            "Did confluence beat the legacy rank-threshold benchmark on P&L and drawdown?",
        ],
    }
