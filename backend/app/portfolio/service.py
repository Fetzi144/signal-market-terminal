"""Portfolio service: position CRUD, P&L calculations, price refresh."""
import logging
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.market import Market, Outcome
from app.models.portfolio import Position, Trade
from app.models.snapshot import PriceSnapshot

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


def _to_decimal(v: Decimal | float | int) -> Decimal:
    """Coerce numeric input to Decimal."""
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


async def open_position(
    session: AsyncSession,
    market_id: UUID,
    outcome_id: UUID,
    platform: str,
    side: str,
    quantity: Decimal | float,
    price: Decimal | float,
    signal_id: UUID | None = None,
    notes: str | None = None,
) -> Position:
    """Open a new position and record the initial trade."""
    quantity, price = _to_decimal(quantity), _to_decimal(price)
    position = Position(
        market_id=market_id,
        outcome_id=outcome_id,
        platform=platform,
        side=side,
        quantity=quantity,
        avg_entry_price=price,
        status="open",
        signal_id=signal_id,
        notes=notes,
    )
    session.add(position)
    await session.flush()

    trade = Trade(
        position_id=position.id,
        action="buy",
        quantity=quantity,
        price=price,
    )
    session.add(trade)
    await session.commit()
    return position


async def add_to_position(
    session: AsyncSession,
    position_id: UUID,
    quantity: Decimal | float,
    price: Decimal | float,
    fees: Decimal | float = _ZERO,
) -> Position:
    """Add to an existing position, updating the weighted average entry price."""
    position = await session.get(Position, position_id)
    if position is None:
        raise ValueError(f"Position {position_id} not found")
    if position.status != "open":
        raise ValueError(f"Position {position_id} is {position.status}, cannot add")

    quantity, price, fees = _to_decimal(quantity), _to_decimal(price), _to_decimal(fees)
    # Weighted average: (old_qty * old_price + new_qty * new_price) / total_qty
    total_qty = position.quantity + quantity
    position.avg_entry_price = (
        (position.quantity * position.avg_entry_price) + (quantity * price)
    ) / total_qty
    position.quantity = total_qty

    trade = Trade(
        position_id=position.id,
        action="buy",
        quantity=quantity,
        price=price,
        fees=fees,
    )
    session.add(trade)
    await session.commit()
    return position


async def close_position(
    session: AsyncSession,
    position_id: UUID,
    quantity: Decimal | float,
    price: Decimal | float,
    fees: Decimal | float = _ZERO,
) -> Position:
    """Close (partially or fully) a position at the given price."""
    position = await session.get(Position, position_id)
    if position is None:
        raise ValueError(f"Position {position_id} not found")
    if position.status != "open":
        raise ValueError(f"Position {position_id} is {position.status}, cannot close")
    quantity, price, fees = _to_decimal(quantity), _to_decimal(price), _to_decimal(fees)
    if quantity > position.quantity:
        raise ValueError(f"Cannot close {quantity} shares, only {position.quantity} held")

    # Calculate realized P&L for this portion
    pnl = (price - position.avg_entry_price) * quantity
    if position.side == "no":
        pnl = -pnl  # Inverted for "no" side

    position.realized_pnl = (position.realized_pnl or _ZERO) + pnl
    position.quantity -= quantity

    trade = Trade(
        position_id=position.id,
        action="sell",
        quantity=quantity,
        price=price,
        fees=fees,
    )
    session.add(trade)

    if position.quantity <= 0:
        position.quantity = _ZERO
        position.status = "closed"
        position.exit_price = price
        position.unrealized_pnl = _ZERO

    await session.commit()
    return position


async def update_current_prices(session: AsyncSession) -> int:
    """Refresh current_price and unrealized_pnl for all open positions from latest snapshots."""
    stmt = select(Position).where(Position.status == "open")
    result = await session.execute(stmt)
    positions = result.scalars().all()

    updated = 0
    for pos in positions:
        # Get latest price snapshot for this outcome
        price_stmt = (
            select(PriceSnapshot.price)
            .where(PriceSnapshot.outcome_id == pos.outcome_id)
            .order_by(PriceSnapshot.captured_at.desc())
            .limit(1)
        )
        price_result = await session.execute(price_stmt)
        latest_price = price_result.scalar_one_or_none()

        if latest_price is not None:
            pos.current_price = latest_price
            pnl = (pos.current_price - pos.avg_entry_price) * pos.quantity
            if pos.side == "no":
                pnl = -pnl
            pos.unrealized_pnl = pnl
            updated += 1

    await session.commit()
    return updated


async def resolve_positions(session: AsyncSession) -> int:
    """Close all open positions on resolved markets at $1 (winner) or $0 (loser)."""
    # Find open positions where the market is no longer active (resolved)
    stmt = (
        select(Position)
        .join(Market, Position.market_id == Market.id)
        .where(Position.status == "open", Market.active == False)  # noqa: E712
    )
    result = await session.execute(stmt)
    positions = result.scalars().all()

    resolved = 0
    for pos in positions:
        # Check if this outcome's final price is near 1 (winner) or 0 (loser)
        price_stmt = (
            select(PriceSnapshot.price)
            .where(PriceSnapshot.outcome_id == pos.outcome_id)
            .order_by(PriceSnapshot.captured_at.desc())
            .limit(1)
        )
        price_result = await session.execute(price_stmt)
        last_price = price_result.scalar_one_or_none()

        if last_price is not None:
            resolution_price = Decimal("1") if last_price >= Decimal("0.5") else _ZERO
        else:
            continue

        pnl = (resolution_price - pos.avg_entry_price) * pos.quantity
        if pos.side == "no":
            pnl = -pnl

        pos.realized_pnl = (pos.realized_pnl or _ZERO) + pnl
        pos.exit_price = resolution_price
        pos.current_price = resolution_price
        pos.unrealized_pnl = _ZERO
        pos.status = "resolved"
        pos.quantity = _ZERO
        resolved += 1

    await session.commit()
    return resolved


async def get_position(session: AsyncSession, position_id: UUID) -> Position | None:
    """Get a position with its trades."""
    stmt = (
        select(Position, Market.question, Outcome.name)
        .join(Market, Position.market_id == Market.id)
        .join(Outcome, Position.outcome_id == Outcome.id)
        .options(selectinload(Position.trades))
        .where(Position.id == position_id)
    )
    result = await session.execute(stmt)
    row = result.one_or_none()
    if row is None:
        return None
    pos, market_question, outcome_name = row
    pos.market_question = market_question
    pos.outcome_name = outcome_name
    return pos


async def list_positions(
    session: AsyncSession,
    status: str | None = None,
    platform: str | None = None,
    market_id: UUID | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[Position], int]:
    """List positions with optional filters, returns (positions, total_count)."""
    base = select(Position)
    count_base = select(func.count(Position.id))

    if status:
        base = base.where(Position.status == status)
        count_base = count_base.where(Position.status == status)
    if platform:
        base = base.where(Position.platform == platform)
        count_base = count_base.where(Position.platform == platform)
    if market_id:
        base = base.where(Position.market_id == market_id)
        count_base = count_base.where(Position.market_id == market_id)

    total = (await session.execute(count_base)).scalar_one()

    stmt = (
        base.join(Market, Position.market_id == Market.id)
        .join(Outcome, Position.outcome_id == Outcome.id)
        .add_columns(Market.question, Outcome.name)
        .order_by(Position.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await session.execute(stmt)
    rows = result.all()
    enriched = []
    for pos, market_question, outcome_name in rows:
        pos.market_question = market_question
        pos.outcome_name = outcome_name
        enriched.append(pos)
    return enriched, total


async def get_portfolio_summary(session: AsyncSession) -> dict:
    """Aggregate portfolio stats: total unrealized/realized P&L, open count, win rate."""
    # Open positions stats
    open_stmt = select(
        func.count(Position.id),
        func.coalesce(func.sum(Position.unrealized_pnl), _ZERO),
    ).where(Position.status == "open")
    open_result = await session.execute(open_stmt)
    open_count, total_unrealized = open_result.one()

    # Closed/resolved positions stats
    closed_stmt = select(
        func.count(Position.id),
        func.coalesce(func.sum(Position.realized_pnl), _ZERO),
    ).where(Position.status.in_(["closed", "resolved"]))
    closed_result = await session.execute(closed_stmt)
    closed_count, total_realized = closed_result.one()

    # Win rate (closed/resolved with positive realized_pnl)
    wins_stmt = select(func.count(Position.id)).where(
        Position.status.in_(["closed", "resolved"]),
        Position.realized_pnl > 0,
    )
    wins = (await session.execute(wins_stmt)).scalar_one()

    win_rate = (wins / closed_count * 100) if closed_count > 0 else 0.0

    return {
        "open_positions": open_count,
        "closed_positions": closed_count,
        "total_unrealized_pnl": total_unrealized,
        "total_realized_pnl": total_realized,
        "win_rate": round(win_rate, 1),
    }
