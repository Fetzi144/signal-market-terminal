import uuid
from datetime import datetime, timezone

from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str | None] = mapped_column(String(512))
    question: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(String(128))
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_volume_24h: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    last_liquidity: Mapped[Decimal | None] = mapped_column(Numeric(20, 2))
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    outcomes: Mapped[list["Outcome"]] = relationship(back_populates="market", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("platform", "platform_id", name="uq_market_platform_id"),
        Index("ix_market_active_platform", "active", "platform"),
        Index("ix_market_end_date", "end_date"),
    )


class Outcome(Base):
    __tablename__ = "outcomes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    market_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("markets.id"), nullable=False)
    platform_outcome_id: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    token_id: Mapped[str | None] = mapped_column(String(255))

    market: Mapped["Market"] = relationship(back_populates="outcomes")

    __table_args__ = (
        UniqueConstraint("market_id", "platform_outcome_id", name="uq_outcome_market"),
        {"comment": "Individual outcomes (Yes/No) for a market"},
    )
