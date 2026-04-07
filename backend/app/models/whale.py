import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class WalletProfile(Base):
    __tablename__ = "wallet_profiles"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address: Mapped[str] = mapped_column(String(42), nullable=False, unique=True)
    label: Mapped[str | None] = mapped_column(String(128))
    total_volume: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False, default=Decimal("0"))
    win_rate: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    trade_count: Mapped[int] = mapped_column(nullable=False, default=0)
    last_active: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    tracked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    activities: Mapped[list["WalletActivity"]] = relationship(back_populates="wallet")

    __table_args__ = (
        Index("ix_wallet_tracked", "tracked"),
        Index("ix_wallet_volume", "total_volume"),
    )


class WalletActivity(Base):
    __tablename__ = "wallet_activities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wallet_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("wallet_profiles.id"), nullable=False
    )
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("outcomes.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(8), nullable=False)  # 'buy' or 'sell'
    quantity: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    notional_usd: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False, default=Decimal("0"))
    tx_hash: Mapped[str] = mapped_column(String(66), nullable=False, unique=True)
    block_number: Mapped[int | None] = mapped_column()
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    wallet: Mapped["WalletProfile"] = relationship(back_populates="activities")

    __table_args__ = (
        Index("ix_activity_wallet", "wallet_id", "timestamp"),
        Index("ix_activity_outcome", "outcome_id"),
        Index("ix_activity_timestamp", "timestamp"),
    )
