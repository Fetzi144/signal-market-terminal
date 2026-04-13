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
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False, default="30m")
    market_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("outcomes.id", ondelete="SET NULL")
    )
    fired_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    observed_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detected_at_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_platform: Mapped[str | None] = mapped_column(String(32))
    source_token_id: Mapped[str | None] = mapped_column(String(128))
    source_stream_session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    source_event_hash: Mapped[str | None] = mapped_column(String(128))
    source_event_type: Mapped[str | None] = mapped_column(String(64))
    # Truncated to 15-min bucket for dedupe. Set by application before insert.
    dedupe_bucket: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    signal_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    rank_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False)
    price_at_fire: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_correctly: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=None)
    alerted: Mapped[bool] = mapped_column(Boolean, default=False)

    # CLV tracking (Phase 1 Q2) — populated at resolution time
    closing_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    resolution_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    clv: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    profit_loss: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))

    # Probability engine (Phase 2 Q2) — populated at detection time
    estimated_probability: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    probability_adjustment: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))

    # Trading intelligence (Phase 3 Q2) — populated at detection time
    expected_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))

    evaluations: Mapped[list["SignalEvaluation"]] = relationship(back_populates="signal")
    execution_decisions: Mapped[list["ExecutionDecision"]] = relationship(back_populates="signal")

    __table_args__ = (
        # Dedupe: one signal per type per outcome per timeframe per 15-min window
        Index(
            "uq_signal_dedupe", "signal_type", "outcome_id", "timeframe", "dedupe_bucket",
            unique=True,
        ),
        Index("ix_signal_fired", "fired_at"),
        Index("ix_signal_observed_at_exchange", "observed_at_exchange"),
        Index("ix_signal_received_at_local", "received_at_local"),
        Index("ix_signal_source_platform_observed_at_exchange", "source_platform", "observed_at_exchange"),
        Index("ix_signal_source_token_id_observed_at_exchange", "source_token_id", "observed_at_exchange"),
        Index("ix_signal_source_stream_session_id", "source_stream_session_id"),
        Index("ix_signal_market", "market_id", "fired_at"),
        Index("ix_signal_type", "signal_type", "fired_at"),
        Index("ix_signal_rank", "rank_score"),
        Index("ix_signal_timeframe", "timeframe"),
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
