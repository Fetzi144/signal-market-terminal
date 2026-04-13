import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class ExecutionDecision(Base):
    __tablename__ = "execution_decisions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("signals.id", ondelete="CASCADE"), nullable=False
    )
    strategy_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_runs.id", ondelete="CASCADE"), nullable=False
    )
    decision_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decision_status: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    direction: Mapped[str | None] = mapped_column(String(16))
    ideal_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    executable_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    requested_size_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    fillable_size_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    fill_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    net_ev_per_share: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    net_expected_pnl_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    missing_orderbook_context: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stale_orderbook_context: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    liquidity_constrained: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fill_status: Mapped[str | None] = mapped_column(String(32))
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    signal: Mapped["Signal"] = relationship(back_populates="execution_decisions")
    strategy_run: Mapped["StrategyRun"] = relationship()
    paper_trade: Mapped["PaperTrade"] = relationship(back_populates="execution_decision", uselist=False)

    __table_args__ = (
        UniqueConstraint("signal_id", "strategy_run_id", name="uq_execution_decisions_signal_strategy_run"),
        Index("ix_execution_decisions_strategy_run_decision_at", "strategy_run_id", "decision_at"),
        Index("ix_execution_decisions_reason_code", "reason_code"),
        Index("ix_execution_decisions_fill_status", "fill_status"),
    )
