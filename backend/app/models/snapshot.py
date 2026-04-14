import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Numeric
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class PriceSnapshot(Base):
    """Append-only. One row per outcome per capture cycle."""

    __tablename__ = "price_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    outcome_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("outcomes.id"), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    volume_24h: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    liquidity: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    outcome: Mapped["Outcome | None"] = relationship(foreign_keys=[outcome_id])

    __table_args__ = (
        Index("ix_price_snap_outcome_time", "outcome_id", "captured_at"),
    )


class OrderbookSnapshot(Base):
    """Append-only. One row per outcome per capture cycle."""

    __tablename__ = "orderbook_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    outcome_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("outcomes.id"), nullable=False)
    bids: Mapped[dict | None] = mapped_column(JSONB)
    asks: Mapped[dict | None] = mapped_column(JSONB)
    spread: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    depth_bid_10pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    depth_ask_10pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    outcome: Mapped["Outcome | None"] = relationship(foreign_keys=[outcome_id])

    __table_args__ = (
        Index("ix_ob_snap_outcome_time", "outcome_id", "captured_at"),
    )
