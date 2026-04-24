from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base

if TYPE_CHECKING:
    from app.models.execution_decision import ExecutionDecision


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketExecutionActionCandidate(Base):
    __tablename__ = "polymarket_execution_action_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("signals.id", ondelete="SET NULL"),
    )
    execution_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("execution_decisions.id", ondelete="SET NULL"),
    )
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    asset_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_asset_dim.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    order_type_hint: Mapped[str | None] = mapped_column(String(16))
    decision_horizon_ms: Mapped[int | None] = mapped_column(Integer)
    target_size: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    est_fillable_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    est_fill_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    est_avg_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_worst_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_min_order_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    est_taker_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_maker_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_slippage_cost: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_alpha_capture_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_adverse_selection_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_net_ev_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_net_ev_total: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    invalid_reason: Mapped[str | None] = mapped_column(String(128))
    policy_version: Mapped[str | None] = mapped_column(String(64))
    source_recon_state_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_recon_state.id", ondelete="SET NULL"),
    )
    source_feature_row_id: Mapped[int | None] = mapped_column(Integer)
    source_label_summary_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    execution_decision: Mapped["ExecutionDecision | None"] = relationship(
        back_populates="polymarket_action_candidates",
        foreign_keys=[execution_decision_id],
    )

    __table_args__ = (
        Index("ix_pm_execution_action_candidates_asset_decided", "asset_id", "decided_at"),
        Index("ix_pm_execution_action_candidates_condition_decided", "condition_id", "decided_at"),
        Index("ix_pm_execution_action_candidates_execution_decision_id", "execution_decision_id"),
        Index("ix_pm_execution_action_candidates_signal_id", "signal_id"),
        Index("ix_pm_execution_action_candidates_action_type", "action_type"),
        Index("ix_pm_execution_action_candidates_valid", "valid"),
        Index("ix_pm_execution_action_candidates_invalid_reason", "invalid_reason"),
    )
