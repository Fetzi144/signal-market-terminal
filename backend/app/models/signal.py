import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_type: Mapped[str] = mapped_column(String(64), nullable=False)
    market_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    fired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    # Truncated to 15-min bucket for dedupe. Set by application before insert.
    dedupe_bucket: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    signal_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    rank_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False)
    price_at_fire: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)

    evaluations: Mapped[list["SignalEvaluation"]] = relationship(back_populates="signal")

    __table_args__ = (
        # Dedupe: one signal per type per outcome per 15-min window
        Index(
            "uq_signal_dedupe", "signal_type", "outcome_id", "dedupe_bucket",
            unique=True,
        ),
        Index("ix_signal_fired", "fired_at"),
        Index("ix_signal_market", "market_id", "fired_at"),
        Index("ix_signal_type", "signal_type", "fired_at"),
        Index("ix_signal_rank", "rank_score"),
    )


class SignalEvaluation(Base):
    __tablename__ = "signal_evaluations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("signals.id"), nullable=False)
    horizon: Mapped[str] = mapped_column(String(8), nullable=False)  # '15m','1h','4h','24h'
    price_at_eval: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    price_change: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    price_change_pct: Mapped[Decimal | None] = mapped_column(Numeric(8, 4))
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    signal: Mapped["Signal"] = relationship(back_populates="evaluations")

    __table_args__ = (
        UniqueConstraint("signal_id", "horizon", name="uq_eval_signal_horizon"),
    )
