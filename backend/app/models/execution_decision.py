import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
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
    chosen_action_type: Mapped[str | None] = mapped_column(String(32))
    chosen_order_type_hint: Mapped[str | None] = mapped_column(String(16))
    chosen_target_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    chosen_target_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    chosen_est_fillable_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    chosen_est_fill_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    chosen_est_net_ev_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    chosen_est_net_ev_total: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    chosen_est_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    chosen_est_slippage: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    chosen_policy_version: Mapped[str | None] = mapped_column(String(64))
    chosen_action_candidate_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_execution_action_candidates.id", ondelete="SET NULL"),
    )
    decision_reason_json: Mapped[dict | None] = mapped_column(JSONB)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    signal: Mapped["Signal"] = relationship(back_populates="execution_decisions")
    strategy_run: Mapped["StrategyRun"] = relationship()
    paper_trade: Mapped["PaperTrade"] = relationship(back_populates="execution_decision", uselist=False)
    polymarket_action_candidates: Mapped[list["PolymarketExecutionActionCandidate"]] = relationship(
        back_populates="execution_decision",
        foreign_keys="PolymarketExecutionActionCandidate.execution_decision_id",
    )
    chosen_action_candidate: Mapped["PolymarketExecutionActionCandidate | None"] = relationship(
        foreign_keys=[chosen_action_candidate_id],
        uselist=False,
        post_update=True,
    )

    __table_args__ = (
        UniqueConstraint("signal_id", "strategy_run_id", name="uq_execution_decisions_signal_strategy_run"),
        Index("ix_execution_decisions_strategy_run_decision_at", "strategy_run_id", "decision_at"),
        Index("ix_execution_decisions_reason_code", "reason_code"),
        Index("ix_execution_decisions_fill_status", "fill_status"),
        Index("ix_execution_decisions_chosen_action_type", "chosen_action_type"),
    )
