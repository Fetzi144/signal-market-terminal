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
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot
from app.signals.ev import compute_ev_full
from app.signals.kelly import kelly_size
from app.signals.risk import check_exposure

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")


@dataclass
class TradeOpenResult:
    trade: PaperTrade | None
    decision: str
    reason_code: str
    reason_label: str
    detail: str | None = None
    diagnostics: dict | None = None


def _risk_reason_code(reason: str) -> str:
    if reason.startswith("Total exposure limit reached"):
        return "risk_total_exposure"
    if reason.startswith("Cluster exposure limit reached"):
        return "risk_cluster_exposure"
    return "risk_rejected"


def _risk_reason_label(reason_code: str) -> str:
    labels = {
        "risk_total_exposure": "Total exposure limit reached",
        "risk_cluster_exposure": "Cluster exposure limit reached",
        "risk_rejected": "Risk rejected",
    }
    return labels.get(reason_code, reason_code.replace("_", " "))


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

    existing = await session.execute(
        select(PaperTrade.id).where(
            PaperTrade.signal_id == signal_id,
            PaperTrade.status == "open",
        )
    )
    existing_trade_id = existing.scalar_one_or_none()
    if existing_trade_id is not None:
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
) -> int:
    """Resolve all open paper trades for a given outcome.

    Args:
        outcome_id: The resolved outcome
        outcome_won: True if the outcome resolved YES, False if NO

    Returns count of resolved trades.
    """
    result = await session.execute(
        select(PaperTrade).where(
            PaperTrade.outcome_id == outcome_id,
            PaperTrade.status == "open",
        )
    )
    trades = result.scalars().all()

    if not trades:
        return 0

    now = datetime.now(timezone.utc)
    count = 0

    for trade in trades:
        if trade.direction == "buy_yes":
            exit_price = Decimal("1.000000") if outcome_won else Decimal("0.000000")
        else:  # buy_no
            exit_price = Decimal("0.000000") if outcome_won else Decimal("1.000000")

        # P&L = shares * (exit_price - entry_price)
        pnl = (trade.shares * (exit_price - trade.entry_price)).quantize(Decimal("0.01"))
        shadow_pnl = None
        if trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
            shadow_shares = _shadow_shares_from_trade(trade)
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


def _near_touch_depth(levels, *, side: str, half_spread: Decimal) -> Decimal | None:
    if not levels:
        return None
    best = _best_price(levels)
    if best is None:
        return None
    threshold = half_spread if half_spread > ZERO else Decimal("0.01")
    depth = ZERO
    for level in levels:
        if len(level) < 2:
            continue
        price = _parse_decimal(level[0])
        size = _parse_decimal(level[1])
        if price is None or size is None:
            continue
        if side == "ask" and price <= best + threshold:
            depth += size
            continue
        if side == "bid" and price >= best - threshold:
            depth += size
            continue
        break
    return depth if depth > ZERO else None


async def _nearest_orderbook_snapshot(
    session: AsyncSession,
    outcome_id: uuid.UUID,
    fired_at: datetime | None,
) -> OrderbookSnapshot | None:
    anchor = fired_at or datetime.now(timezone.utc)
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
    if before is not None:
        return before

    after_result = await session.execute(
        select(OrderbookSnapshot)
        .where(
            OrderbookSnapshot.outcome_id == outcome_id,
            OrderbookSnapshot.captured_at >= anchor,
        )
        .order_by(OrderbookSnapshot.captured_at.asc())
        .limit(1)
    )
    return after_result.scalars().first()


async def _build_shadow_execution(
    session: AsyncSession,
    *,
    outcome_id: uuid.UUID,
    direction: str,
    approved_size: Decimal,
    ideal_entry_price: Decimal,
    fired_at: datetime | None,
) -> dict:
    snapshot = await _nearest_orderbook_snapshot(session, outcome_id, fired_at)
    if snapshot is None:
        return {
            "shadow_entry_price": ideal_entry_price,
            "details": {
                "missing_orderbook_context": True,
                "liquidity_constrained": False,
                "snapshot_id": None,
                "captured_at": None,
                "spread": None,
                "available_depth_shares": None,
                "available_depth_usd": None,
                "size_to_depth_ratio": None,
                "shadow_shares": str((approved_size / ideal_entry_price).quantize(Decimal("0.0001"))) if ideal_entry_price > ZERO else None,
            },
        }

    spread = snapshot.spread or ZERO
    half_spread = (spread * HALF).quantize(Decimal("0.000001"))
    best_bid = _best_price(snapshot.bids)
    best_ask = _best_price(snapshot.asks)
    if direction == "buy_yes":
        available_depth_shares = _near_touch_depth(snapshot.asks or [], side="ask", half_spread=half_spread)
    else:
        available_depth_shares = _near_touch_depth(snapshot.bids or [], side="bid", half_spread=half_spread)

    shadow_entry_price = (ideal_entry_price + half_spread).quantize(Decimal("0.000001"))
    if best_ask is not None and direction == "buy_yes":
        shadow_entry_price = max(shadow_entry_price, best_ask)
    if best_bid is not None and direction == "buy_no":
        shadow_entry_price = max(shadow_entry_price, (ONE - best_bid).quantize(Decimal("0.000001")))
    shadow_entry_price = min(shadow_entry_price, ONE)

    available_depth_usd = None
    if available_depth_shares is not None:
        available_depth_usd = (available_depth_shares * shadow_entry_price).quantize(Decimal("0.01"))
    size_to_depth_ratio = None
    if available_depth_usd is not None and available_depth_usd > ZERO:
        size_to_depth_ratio = (approved_size / available_depth_usd).quantize(Decimal("0.0001"))

    liquidity_constrained = (
        available_depth_usd is not None and available_depth_usd > ZERO and approved_size > available_depth_usd
    )
    shadow_shares = (approved_size / shadow_entry_price).quantize(Decimal("0.0001")) if shadow_entry_price > ZERO else ZERO

    return {
        "shadow_entry_price": shadow_entry_price,
        "details": {
            "missing_orderbook_context": False,
            "liquidity_constrained": bool(liquidity_constrained),
            "snapshot_id": snapshot.id,
            "captured_at": snapshot.captured_at.isoformat() if snapshot.captured_at else None,
            "spread": str(snapshot.spread) if snapshot.spread is not None else None,
            "best_bid": str(best_bid) if best_bid is not None else None,
            "best_ask": str(best_ask) if best_ask is not None else None,
            "available_depth_shares": str(available_depth_shares) if available_depth_shares is not None else None,
            "available_depth_usd": str(available_depth_usd) if available_depth_usd is not None else None,
            "size_to_depth_ratio": str(size_to_depth_ratio) if size_to_depth_ratio is not None else None,
            "shadow_shares": str(shadow_shares),
        },
    }


def _shadow_shares_from_trade(trade: PaperTrade) -> Decimal:
    if isinstance(trade.details, dict):
        shadow_execution = trade.details.get("shadow_execution")
        if isinstance(shadow_execution, dict):
            shadow_shares = _parse_decimal(shadow_execution.get("shadow_shares"))
            if shadow_shares is not None and shadow_shares > ZERO:
                return shadow_shares
    if trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
        return (trade.size_usd / trade.shadow_entry_price).quantize(Decimal("0.0001"))
    return trade.shares
