"""Historical replay helpers for the frozen default strategy and legacy path."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.default_strategy import default_strategy_skip_label, evaluate_default_strategy_signal
from app.models.backtest import BacktestSignal, BacktestRun
from app.models.market import Market, Outcome
from app.models.paper_trade import PaperTrade
from app.models.signal import Signal
from app.models.strategy_run import StrategyRun
from app.paper_trading.engine import attempt_open_trade, resolve_trades
from app.signals.probability import brier_score

LEGACY_SIGNAL_TYPES = {"price_move", "volume_spike"}
ZERO = Decimal("0")


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _signal_sort_key(signal: Signal) -> tuple[datetime, str]:
    return (
        _ensure_utc(signal.fired_at) or datetime.min.replace(tzinfo=timezone.utc),
        str(signal.id),
    )


def _trade_sort_key(row: tuple[PaperTrade, Signal]) -> tuple[datetime, str]:
    trade, signal = row
    return (
        _ensure_utc(trade.resolved_at)
        or _ensure_utc(signal.fired_at)
        or _ensure_utc(trade.opened_at)
        or datetime.min.replace(tzinfo=timezone.utc),
        str(trade.id),
    )


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


def _safe_float(value: Decimal | None, digits: str = "0.0001") -> float | None:
    if value is None:
        return None
    return float(value.quantize(Decimal(digits)))


def _trade_directional_clv(trade: PaperTrade, signal: Signal) -> Decimal | None:
    if signal.price_at_fire is None or signal.closing_price is None:
        return None
    if trade.direction == "buy_yes":
        return signal.closing_price - signal.price_at_fire
    return signal.price_at_fire - signal.closing_price


def _trade_win_probability(trade: PaperTrade, signal: Signal) -> Decimal | None:
    if signal.estimated_probability is None:
        return None
    if trade.direction == "buy_yes":
        return signal.estimated_probability
    return (Decimal("1") - signal.estimated_probability).quantize(Decimal("0.0001"))


def _profit_factor(win_pnls: list[float], loss_pnls: list[float]) -> float | None:
    total_wins = sum(win_pnls) if win_pnls else 0.0
    total_losses = abs(sum(loss_pnls)) if loss_pnls else 0.0
    if total_losses == 0:
        return None if total_wins > 0 else 0.0
    return round(total_wins / total_losses, 4)


def _skip_reason_label(reason_code: str) -> str:
    if reason_code == "rank_below_threshold":
        return "Rank below threshold"
    return default_strategy_skip_label(reason_code) or reason_code.replace("_", " ")


def _empty_mode_summary(*, label: str, start_at: datetime, end_at: datetime, extra: dict | None = None) -> dict:
    span_days = max((end_at - start_at).total_seconds() / 86400, 1.0)
    summary = {
        "mode": label,
        "candidate_signals": 0,
        "qualified_signals": 0,
        "traded_signals": 0,
        "resolved_trades": 0,
        "win_rate": 0.0,
        "signals_per_day": round(0 / span_days, 2),
        "false_positive_rate": 0.0,
        "avg_clv": None,
        "brier_score": None,
        "cumulative_pnl": 0.0,
        "shadow_cumulative_pnl": 0.0,
        "max_drawdown": 0.0,
        "profit_factor": 0.0,
        "shadow_profit_factor": 0.0,
        "liquidity_constrained_trades": 0,
        "trades_missing_orderbook_context": 0,
        "accuracy_by_type": {},
        "skip_reasons": [],
    }
    if extra:
        summary.update(extra)
    return summary


def _build_mode_summary(
    *,
    label: str,
    start_at: datetime,
    end_at: datetime,
    candidate_count: int,
    qualified_count: int,
    trade_rows: list[tuple[PaperTrade, Signal]],
    skip_counts: dict[str, int],
) -> dict:
    if not trade_rows and candidate_count == 0:
        return _empty_mode_summary(label=label, start_at=start_at, end_at=end_at)

    span_days = max((end_at - start_at).total_seconds() / 86400, 1.0)
    ordered_rows = sorted(trade_rows, key=_trade_sort_key)
    resolved_rows = [
        (trade, signal)
        for trade, signal in ordered_rows
        if trade.status == "resolved" and trade.pnl is not None
    ]

    wins = [float(trade.pnl) for trade, _signal in resolved_rows if trade.pnl is not None and trade.pnl > ZERO]
    losses = [float(trade.pnl) for trade, _signal in resolved_rows if trade.pnl is not None and trade.pnl <= ZERO]
    shadow_wins = [float(trade.shadow_pnl) for trade, _signal in resolved_rows if trade.shadow_pnl is not None and trade.shadow_pnl > ZERO]
    shadow_losses = [float(trade.shadow_pnl) for trade, _signal in resolved_rows if trade.shadow_pnl is not None and trade.shadow_pnl <= ZERO]
    trade_clvs = [
        clv
        for trade, signal in resolved_rows
        if (clv := _trade_directional_clv(trade, signal)) is not None
    ]
    predictions = [
        (_trade_win_probability(trade, signal), trade.pnl > ZERO)
        for trade, signal in resolved_rows
        if _trade_win_probability(trade, signal) is not None
    ]
    cumulative_curve = [trade.pnl or ZERO for trade, _signal in resolved_rows]
    cumulative_pnl = sum((trade.pnl or ZERO for trade, _signal in resolved_rows), ZERO)
    shadow_cumulative_pnl = sum(
        (trade.shadow_pnl or ZERO for trade, _signal in resolved_rows if trade.shadow_pnl is not None),
        ZERO,
    )
    liquidity_constrained = sum(
        1
        for trade, _signal in resolved_rows
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("liquidity_constrained") is True
    )
    missing_orderbook = sum(
        1
        for trade, _signal in resolved_rows
        if isinstance(trade.details, dict)
        and isinstance(trade.details.get("shadow_execution"), dict)
        and trade.details["shadow_execution"].get("missing_orderbook_context") is True
    )

    by_type: dict[str, dict[str, int]] = {}
    for trade, signal in resolved_rows:
        bucket = by_type.setdefault(signal.signal_type, {"total": 0, "correct": 0})
        bucket["total"] += 1
        if trade.pnl is not None and trade.pnl > ZERO:
            bucket["correct"] += 1

    accuracy_by_type = {
        signal_type: {
            "total": stats["total"],
            "correct": stats["correct"],
            "win_rate": round(stats["correct"] / stats["total"], 4) if stats["total"] > 0 else 0.0,
        }
        for signal_type, stats in by_type.items()
    }

    skip_reasons = sorted(
        [
            {
                "reason_code": reason_code,
                "reason_label": _skip_reason_label(reason_code),
                "count": count,
            }
            for reason_code, count in skip_counts.items()
        ],
        key=lambda row: (-row["count"], row["reason_label"]),
    )

    return {
        "mode": label,
        "candidate_signals": candidate_count,
        "qualified_signals": qualified_count,
        "traded_signals": len(trade_rows),
        "resolved_trades": len(resolved_rows),
        "win_rate": round((len(wins) / len(resolved_rows)), 4) if resolved_rows else 0.0,
        "signals_per_day": round(len(trade_rows) / span_days, 2),
        "false_positive_rate": round((len(losses) / len(resolved_rows)), 4) if resolved_rows else 0.0,
        "avg_clv": _safe_float(_average(trade_clvs)),
        "brier_score": _safe_float(brier_score(predictions)) if predictions else None,
        "cumulative_pnl": float(cumulative_pnl.quantize(Decimal("0.01"))),
        "shadow_cumulative_pnl": float(shadow_cumulative_pnl.quantize(Decimal("0.01"))),
        "max_drawdown": float(_compute_max_drawdown(cumulative_curve).quantize(Decimal("0.01"))) if cumulative_curve else 0.0,
        "profit_factor": _profit_factor(wins, losses),
        "shadow_profit_factor": _profit_factor(shadow_wins, shadow_losses),
        "liquidity_constrained_trades": liquidity_constrained,
        "trades_missing_orderbook_context": missing_orderbook,
        "accuracy_by_type": accuracy_by_type,
        "skip_reasons": skip_reasons,
    }


async def _build_resolution_map(
    session: AsyncSession,
    outcome_ids: set[uuid.UUID],
) -> dict[uuid.UUID, dict]:
    if not outcome_ids:
        return {}

    outcome_rows = await session.execute(
        select(Outcome.id, Market.end_date)
        .join(Market, Outcome.market_id == Market.id)
        .where(Outcome.id.in_(outcome_ids))
    )
    end_dates = {
        outcome_id: _ensure_utc(end_date)
        for outcome_id, end_date in outcome_rows.all()
    }

    signal_rows = await session.execute(
        select(Signal.outcome_id, Signal.resolution_price, Signal.fired_at)
        .where(
            Signal.outcome_id.in_(outcome_ids),
            Signal.resolution_price.isnot(None),
        )
        .order_by(Signal.fired_at.asc())
    )

    resolution_map: dict[uuid.UUID, dict] = {}
    for outcome_id, resolution_price, fired_at in signal_rows.all():
        if outcome_id in resolution_map:
            continue
        resolution_map[outcome_id] = {
            "outcome_won": resolution_price >= Decimal("0.5"),
            "resolved_at": end_dates.get(outcome_id) or _ensure_utc(fired_at),
        }

    return resolution_map


async def _resolve_due_outcomes(
    session: AsyncSession,
    *,
    strategy_run_id: uuid.UUID,
    due_at: datetime,
    resolution_map: dict[uuid.UUID, dict],
    resolved_outcomes: set[uuid.UUID],
) -> None:
    for outcome_id, resolution in sorted(
        resolution_map.items(),
        key=lambda item: item[1]["resolved_at"] or datetime.max.replace(tzinfo=timezone.utc),
    ):
        resolved_at = resolution["resolved_at"]
        if resolved_at is None or resolved_at > due_at or outcome_id in resolved_outcomes:
            continue
        await resolve_trades(
            session,
            outcome_id=outcome_id,
            outcome_won=resolution["outcome_won"],
            resolved_at=resolved_at,
            strategy_run_id=strategy_run_id,
        )
        resolved_outcomes.add(outcome_id)


def _serialize_trade_row(
    *,
    replay_path: str,
    trade: PaperTrade,
    signal: Signal,
) -> dict:
    details = dict(signal.details or {})
    replay_details = dict(details.get("replay") or {})
    replay_details.update({
        "replay_path": replay_path,
        "trade_id": str(trade.id),
        "trade_direction": trade.direction,
        "trade_status": trade.status,
        "trade_entry_price": str(trade.entry_price),
        "trade_shadow_entry_price": str(trade.shadow_entry_price) if trade.shadow_entry_price is not None else None,
        "trade_size_usd": str(trade.size_usd),
        "trade_shares": str(trade.shares),
        "trade_pnl": str(trade.pnl) if trade.pnl is not None else None,
        "shadow_trade_pnl": str(trade.shadow_pnl) if trade.shadow_pnl is not None else None,
        "opened_at": _ensure_utc(trade.opened_at).isoformat() if trade.opened_at else None,
        "resolved_at": _ensure_utc(trade.resolved_at).isoformat() if trade.resolved_at else None,
    })
    details["replay"] = replay_details

    return {
        "signal_type": signal.signal_type,
        "timeframe": signal.timeframe,
        "outcome_id": signal.outcome_id,
        "fired_at": _ensure_utc(signal.fired_at) or _ensure_utc(trade.opened_at),
        "signal_score": signal.signal_score,
        "confidence": signal.confidence,
        "rank_score": signal.rank_score,
        "resolved_correctly": (trade.pnl > ZERO) if trade.status == "resolved" and trade.pnl is not None else None,
        "price_at_fire": signal.price_at_fire,
        "price_at_resolution": trade.exit_price,
        "details": details,
    }


async def _simulate_default_strategy(
    session: AsyncSession,
    *,
    run: BacktestRun,
    signals: list[Signal],
    resolution_map: dict[uuid.UUID, dict],
) -> dict:
    start_at = _ensure_utc(run.start_date) or datetime.now(timezone.utc)
    end_at = _ensure_utc(run.end_date) or datetime.now(timezone.utc)

    async with session.begin_nested():
        strategy_run = StrategyRun(
            id=uuid.uuid4(),
            strategy_name=f"backtest:{run.id}:default",
            status="active",
            started_at=start_at,
            contract_snapshot={
                "name": settings.default_strategy_name,
                "baseline_start_at": start_at.isoformat(),
                "replay_mode": "historical_backtest",
            },
        )
        session.add(strategy_run)
        await session.flush()

        candidate_count = 0
        qualified_count = 0
        skip_counts: dict[str, int] = {}
        resolved_outcomes: set[uuid.UUID] = set()

        for signal in sorted(signals, key=_signal_sort_key):
            signal_time = _ensure_utc(signal.fired_at) or start_at
            await _resolve_due_outcomes(
                session,
                strategy_run_id=strategy_run.id,
                due_at=signal_time,
                resolution_map=resolution_map,
                resolved_outcomes=resolved_outcomes,
            )

            candidate_count += 1
            evaluation = evaluate_default_strategy_signal(signal, started_at=start_at)
            if not evaluation.signal_type_match or not evaluation.in_window:
                reason_code = evaluation.reason_code or "filtered_out"
                skip_counts[reason_code] = skip_counts.get(reason_code, 0) + 1
                continue

            if evaluation.eligible:
                qualified_count += 1
            result = await attempt_open_trade(
                session=session,
                signal_id=signal.id,
                outcome_id=signal.outcome_id,
                market_id=signal.market_id,
                estimated_probability=signal.estimated_probability,
                market_price=signal.price_at_fire,
                market_question=(signal.details or {}).get("market_question", ""),
                fired_at=signal_time,
                strategy_run_id=strategy_run.id,
                precheck_reason_code=None if evaluation.eligible else evaluation.reason_code,
                precheck_reason_label=evaluation.reason_label,
            )
            if result.trade is not None:
                result.trade.opened_at = signal_time
                await session.flush()
            else:
                skip_counts[result.reason_code] = skip_counts.get(result.reason_code, 0) + 1

            await _resolve_due_outcomes(
                session,
                strategy_run_id=strategy_run.id,
                due_at=signal_time,
                resolution_map=resolution_map,
                resolved_outcomes=resolved_outcomes,
            )

        await _resolve_due_outcomes(
            session,
            strategy_run_id=strategy_run.id,
            due_at=end_at + timedelta(seconds=1),
            resolution_map=resolution_map,
            resolved_outcomes=resolved_outcomes,
        )

        trade_result = await session.execute(
            select(PaperTrade, Signal)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .where(PaperTrade.strategy_run_id == strategy_run.id)
            .order_by(PaperTrade.opened_at.asc())
        )
        trade_rows = trade_result.all()
        records = [
            _serialize_trade_row(replay_path="default_strategy", trade=trade, signal=signal)
            for trade, signal in trade_rows
        ]
        summary = _build_mode_summary(
            label="default_strategy",
            start_at=start_at,
            end_at=end_at,
            candidate_count=candidate_count,
            qualified_count=qualified_count,
            trade_rows=trade_rows,
            skip_counts=skip_counts,
        )

    return {"summary": summary, "records": records}


async def _simulate_legacy_benchmark(
    session: AsyncSession,
    *,
    run: BacktestRun,
    signals: list[Signal],
    resolution_map: dict[uuid.UUID, dict],
) -> dict:
    start_at = _ensure_utc(run.start_date) or datetime.now(timezone.utc)
    end_at = _ensure_utc(run.end_date) or datetime.now(timezone.utc)
    rank_threshold = Decimal(str(run.rank_threshold))

    async with session.begin_nested():
        strategy_run = StrategyRun(
            id=uuid.uuid4(),
            strategy_name=f"backtest:{run.id}:legacy",
            status="active",
            started_at=start_at,
            contract_snapshot={
                "name": "legacy_rank_threshold",
                "rank_threshold": str(rank_threshold),
                "signal_types": sorted(LEGACY_SIGNAL_TYPES),
                "replay_mode": "historical_backtest",
            },
        )
        session.add(strategy_run)
        await session.flush()

        candidate_count = 0
        qualified_count = 0
        skip_counts: dict[str, int] = {}
        resolved_outcomes: set[uuid.UUID] = set()

        for signal in sorted(signals, key=_signal_sort_key):
            signal_time = _ensure_utc(signal.fired_at) or start_at
            await _resolve_due_outcomes(
                session,
                strategy_run_id=strategy_run.id,
                due_at=signal_time,
                resolution_map=resolution_map,
                resolved_outcomes=resolved_outcomes,
            )

            candidate_count += 1
            if signal.rank_score < rank_threshold:
                skip_counts["rank_below_threshold"] = skip_counts.get("rank_below_threshold", 0) + 1
                continue

            qualified_count += 1
            result = await attempt_open_trade(
                session=session,
                signal_id=signal.id,
                outcome_id=signal.outcome_id,
                market_id=signal.market_id,
                estimated_probability=signal.estimated_probability,
                market_price=signal.price_at_fire,
                market_question=(signal.details or {}).get("market_question", ""),
                fired_at=signal_time,
                strategy_run_id=strategy_run.id,
            )
            if result.trade is not None:
                result.trade.opened_at = signal_time
                await session.flush()
            else:
                skip_counts[result.reason_code] = skip_counts.get(result.reason_code, 0) + 1

            await _resolve_due_outcomes(
                session,
                strategy_run_id=strategy_run.id,
                due_at=signal_time,
                resolution_map=resolution_map,
                resolved_outcomes=resolved_outcomes,
            )

        await _resolve_due_outcomes(
            session,
            strategy_run_id=strategy_run.id,
            due_at=end_at + timedelta(seconds=1),
            resolution_map=resolution_map,
            resolved_outcomes=resolved_outcomes,
        )

        trade_result = await session.execute(
            select(PaperTrade, Signal)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .where(PaperTrade.strategy_run_id == strategy_run.id)
            .order_by(PaperTrade.opened_at.asc())
        )
        trade_rows = trade_result.all()
        records = [
            _serialize_trade_row(replay_path="legacy", trade=trade, signal=signal)
            for trade, signal in trade_rows
        ]
        summary = _build_mode_summary(
            label="legacy",
            start_at=start_at,
            end_at=end_at,
            candidate_count=candidate_count,
            qualified_count=qualified_count,
            trade_rows=trade_rows,
            skip_counts=skip_counts,
        )
        summary["rank_threshold"] = float(rank_threshold)

    return {"summary": summary, "records": records}


def _build_comparison_summary(
    *,
    run: BacktestRun,
    default_strategy: dict,
    legacy: dict,
) -> dict:
    default_summary = default_strategy["summary"]
    legacy_summary = legacy["summary"]
    return {
        "replay_mode": "strategy_comparison",
        "win_rate": default_summary["win_rate"],
        "total_signals": default_summary["traded_signals"],
        "signals_per_day": default_summary["signals_per_day"],
        "false_positive_rate": default_summary["false_positive_rate"],
        "accuracy_by_type": default_summary["accuracy_by_type"],
        "accuracy_by_horizon": {},
        "comparison": {
            "default_strategy": default_summary,
            "legacy": legacy_summary,
            "delta": {
                "cumulative_pnl": round(default_summary["cumulative_pnl"] - legacy_summary["cumulative_pnl"], 2),
                "shadow_cumulative_pnl": round(default_summary["shadow_cumulative_pnl"] - legacy_summary["shadow_cumulative_pnl"], 2),
                "win_rate": round(default_summary["win_rate"] - legacy_summary["win_rate"], 4),
                "traded_signals": default_summary["traded_signals"] - legacy_summary["traded_signals"],
            },
            "start_date": _ensure_utc(run.start_date).isoformat() if run.start_date else None,
            "end_date": _ensure_utc(run.end_date).isoformat() if run.end_date else None,
        },
    }


async def run_strategy_comparison_replay(
    session: AsyncSession,
    run: BacktestRun,
) -> tuple[dict, list[BacktestSignal]]:
    start_at = _ensure_utc(run.start_date) or datetime.now(timezone.utc)
    end_at = _ensure_utc(run.end_date) or datetime.now(timezone.utc)

    signal_result = await session.execute(
        select(Signal)
        .where(
            Signal.fired_at >= start_at,
            Signal.fired_at <= end_at,
            Signal.signal_type.in_([settings.default_strategy_signal_type, *sorted(LEGACY_SIGNAL_TYPES)]),
        )
        .order_by(Signal.fired_at.asc())
    )
    signals = signal_result.scalars().all()

    default_signals = [signal for signal in signals if signal.signal_type == settings.default_strategy_signal_type]
    legacy_signals = [signal for signal in signals if signal.signal_type in LEGACY_SIGNAL_TYPES]
    outcome_ids = {
        signal.outcome_id
        for signal in signals
        if signal.outcome_id is not None
    }
    resolution_map = await _build_resolution_map(session, outcome_ids)

    default_strategy = await _simulate_default_strategy(
        session,
        run=run,
        signals=default_signals,
        resolution_map=resolution_map,
    )
    legacy = await _simulate_legacy_benchmark(
        session,
        run=run,
        signals=legacy_signals,
        resolution_map=resolution_map,
    )

    result_summary = _build_comparison_summary(
        run=run,
        default_strategy=default_strategy,
        legacy=legacy,
    )

    backtest_signals = []
    for record in [*default_strategy["records"], *legacy["records"]]:
        backtest_signals.append(
            BacktestSignal(
                id=uuid.uuid4(),
                backtest_run_id=run.id,
                signal_type=record["signal_type"],
                timeframe=record["timeframe"] or "30m",
                outcome_id=record["outcome_id"],
                fired_at=record["fired_at"] or start_at,
                signal_score=record["signal_score"],
                confidence=record["confidence"],
                rank_score=record["rank_score"],
                resolved_correctly=record["resolved_correctly"],
                price_at_fire=record["price_at_fire"],
                price_at_resolution=record["price_at_resolution"],
                details=record["details"],
            )
        )

    return result_summary, backtest_signals
