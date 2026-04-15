from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")


@dataclass
class OrderbookContext:
    snapshot: OrderbookSnapshot | None
    snapshot_age_seconds: int | None
    snapshot_side: str | None
    usable: bool
    stale: bool = False
    missing_reason: str | None = None


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_decimal(value) -> Decimal | None:
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
        price = parse_decimal(level[0])
        size = parse_decimal(level[1])
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


async def nearest_orderbook_snapshot(
    session: AsyncSession,
    outcome_id: uuid.UUID,
    fired_at: datetime | None,
) -> OrderbookContext:
    anchor = _ensure_utc(fired_at) or datetime.now(timezone.utc)
    before = (
        await session.execute(
            select(OrderbookSnapshot)
            .where(
                OrderbookSnapshot.outcome_id == outcome_id,
                OrderbookSnapshot.captured_at <= anchor,
            )
            .order_by(desc(OrderbookSnapshot.captured_at))
            .limit(1)
        )
    ).scalars().first()
    after = (
        await session.execute(
            select(OrderbookSnapshot)
            .where(
                OrderbookSnapshot.outcome_id == outcome_id,
                OrderbookSnapshot.captured_at >= anchor,
            )
            .order_by(OrderbookSnapshot.captured_at.asc())
            .limit(1)
        )
    ).scalars().first()

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


async def build_shadow_execution(
    session: AsyncSession,
    *,
    outcome_id: uuid.UUID,
    direction: str,
    approved_size: Decimal,
    ideal_entry_price: Decimal,
    fired_at: datetime | None,
) -> dict:
    orderbook_context = await nearest_orderbook_snapshot(session, outcome_id, fired_at)
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
        liquidity = _near_touch_liquidity(snapshot.asks or [], side="ask", half_spread=half_spread)
    else:
        liquidity = _near_touch_liquidity(snapshot.bids or [], side="bid", half_spread=half_spread)
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


def shadow_shares_from_trade(trade: PaperTrade) -> Decimal:
    if isinstance(trade.details, dict):
        shadow_execution = trade.details.get("shadow_execution")
        if isinstance(shadow_execution, dict):
            if shadow_execution.get("fill_status") == "no_fill":
                return ZERO
            shadow_shares = parse_decimal(shadow_execution.get("shadow_shares"))
            if shadow_shares is not None:
                return shadow_shares
    if trade.shadow_entry_price is not None and trade.shadow_entry_price > ZERO:
        return (trade.size_usd / trade.shadow_entry_price).quantize(Decimal("0.0001"))
    return trade.shares


__all__ = [
    "OrderbookContext",
    "build_shadow_execution",
    "parse_decimal",
    "shadow_shares_from_trade",
]
