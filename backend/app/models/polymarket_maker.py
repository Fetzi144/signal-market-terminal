from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketTokenFeeRateHistory(Base):
    __tablename__ = "polymarket_token_fee_rate_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
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
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    effective_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sync_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_meta_sync_runs.id", ondelete="SET NULL"),
    )
    fees_enabled: Mapped[bool | None] = mapped_column(Boolean)
    maker_fee_rate: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    taker_fee_rate: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    token_base_fee_rate: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fee_schedule_json: Mapped[dict | None] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_token_fee_history_asset_effective", "asset_id", "effective_at_exchange"),
        Index("ix_pm_token_fee_history_condition_effective", "condition_id", "effective_at_exchange"),
        Index("ix_pm_token_fee_history_market_effective", "market_dim_id", "effective_at_exchange"),
        Index("ix_pm_token_fee_history_observed_at", "observed_at_local"),
        Index("ix_pm_token_fee_history_sync_run_id", "sync_run_id"),
    )


class PolymarketMarketRewardConfigHistory(Base):
    __tablename__ = "polymarket_market_reward_config_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    effective_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sync_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_meta_sync_runs.id", ondelete="SET NULL"),
    )
    reward_status: Mapped[str] = mapped_column(String(32), nullable=False)
    reward_program_id: Mapped[str | None] = mapped_column(String(255))
    reward_daily_rate: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    min_incentive_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    max_incentive_spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    start_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rewards_config_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_reward_history_condition_effective", "condition_id", "effective_at_exchange"),
        Index("ix_pm_reward_history_market_effective", "market_dim_id", "effective_at_exchange"),
        Index("ix_pm_reward_history_reward_status", "reward_status"),
        Index("ix_pm_reward_history_observed_at", "observed_at_local"),
        Index("ix_pm_reward_history_sync_run_id", "sync_run_id"),
    )


class PolymarketMakerEconomicsSnapshot(Base):
    __tablename__ = "polymarket_maker_economics_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="SET NULL"),
    )
    validation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_validations.id", ondelete="SET NULL"),
    )
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    asset_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_asset_dim.id", ondelete="SET NULL"),
    )
    fee_history_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_token_fee_rate_history.id", ondelete="SET NULL"),
    )
    reward_history_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_reward_config_history.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    context_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    estimator_version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    preferred_action: Mapped[str | None] = mapped_column(String(32))
    maker_action_type: Mapped[str | None] = mapped_column(String(32))
    side: Mapped[str | None] = mapped_column(String(16))
    target_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    target_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_fill_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    maker_gross_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_fees_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_rewards_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_realism_adjustment_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_net_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_gross_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_fees_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_rewards_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_realism_adjustment_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_net_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_advantage_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reason_codes_json: Mapped[list | dict | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    input_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_maker_snapshots_opportunity_evaluated", "opportunity_id", "evaluated_at"),
        Index("ix_pm_maker_snapshots_asset_evaluated", "asset_id", "evaluated_at"),
        Index("ix_pm_maker_snapshots_condition_evaluated", "condition_id", "evaluated_at"),
        Index("ix_pm_maker_snapshots_status_evaluated", "status", "evaluated_at"),
    )


class PolymarketQuoteRecommendation(Base):
    __tablename__ = "polymarket_quote_recommendations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_maker_economics_snapshots.id", ondelete="SET NULL"),
    )
    opportunity_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="SET NULL"),
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
    recommendation_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="advisory_quote")
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    comparison_winner: Mapped[str | None] = mapped_column(String(16))
    recommendation_action: Mapped[str | None] = mapped_column(String(32))
    recommended_action_type: Mapped[str | None] = mapped_column(String(32))
    recommended_side: Mapped[str | None] = mapped_column(String(16))
    recommended_yes_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    recommended_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    recommended_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    recommended_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    price_offset_ticks: Mapped[int | None] = mapped_column(Integer)
    reason_codes_json: Mapped[list | dict | None] = mapped_column(JSONB)
    summary_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    input_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_quote_recommendations_opportunity_created", "opportunity_id", "created_at"),
        Index("ix_pm_quote_recommendations_snapshot_id", "snapshot_id"),
        Index("ix_pm_quote_recommendations_asset_created", "asset_id", "created_at"),
        Index("ix_pm_quote_recommendations_condition_created", "condition_id", "created_at"),
        Index("ix_pm_quote_recommendations_status_created", "status", "created_at"),
    )
