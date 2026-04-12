import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class PaperTrade(Base):
    __tablename__ = "paper_trades"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("signals.id"), nullable=False)
    outcome_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    market_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)  # buy_yes / buy_no
    entry_price: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    size_usd: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    shares: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_paper_trades_status", "status"),
        Index("ix_paper_trades_outcome", "outcome_id"),
        Index("ix_paper_trades_opened", "opened_at"),
    )
