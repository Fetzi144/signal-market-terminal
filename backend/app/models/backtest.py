"""Backtest models: BacktestRun tracks a replay session, BacktestSignal stores hypothetical signals."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Numeric, String  # noqa: F401
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    start_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detector_configs: Mapped[dict | None] = mapped_column(JSONB)
    rank_threshold: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_summary: Mapped[dict | None] = mapped_column(JSONB)

    signals: Mapped[list["BacktestSignal"]] = relationship(
        back_populates="backtest_run", cascade="all, delete-orphan"
    )

    @property
    def replay_mode(self) -> str:
        configs = self.detector_configs or {}
        if isinstance(configs, dict):
            replay_mode = configs.get("_replay_mode")
            if isinstance(replay_mode, str) and replay_mode:
                return replay_mode
        return "detector_replay"

    __table_args__ = (
        Index("ix_backtest_run_status", "status"),
        Index("ix_backtest_run_created", "created_at"),
    )


class BacktestSignal(Base):
    __tablename__ = "backtest_signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    signal_type: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False, default="30m")
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("outcomes.id", ondelete="SET NULL")
    )
    fired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    signal_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    rank_score: Mapped[Decimal] = mapped_column(Numeric(5, 3), nullable=False)
    resolved_correctly: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=None)
    price_at_fire: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    price_at_resolution: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    details: Mapped[dict | None] = mapped_column(JSONB)

    backtest_run: Mapped["BacktestRun"] = relationship(back_populates="signals")

    __table_args__ = (
        Index("ix_bt_signal_run", "backtest_run_id", "fired_at"),
        Index("ix_bt_signal_type", "signal_type"),
        Index("ix_bt_signal_timeframe", "timeframe"),
    )
