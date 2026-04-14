from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketFeatureRun(Base):
    __tablename__ = "polymarket_feature_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_type: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    scope_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    cursor_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    rows_inserted_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_pm_feature_runs_reason_started", "reason", "started_at"),
        Index("ix_pm_feature_runs_run_type_started", "run_type", "started_at"),
        Index("ix_pm_feature_runs_status_started", "status", "started_at"),
    )


class PolymarketBookStateTopN(Base):
    __tablename__ = "polymarket_book_state_topn"

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
    bucket_start_exchange: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_width_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recon_state_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_recon_state.id", ondelete="SET NULL"),
    )
    last_snapshot_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_snapshots.id", ondelete="SET NULL"),
    )
    last_snapshot_hash: Mapped[str | None] = mapped_column(String(255))
    last_applied_raw_event_id: Mapped[int | None] = mapped_column(Integer)
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    microprice: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    bid_levels_json: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    ask_levels_json: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    bid_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    imbalance_top1: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top3: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top5: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    completeness_flags_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "bucket_start_exchange",
            "bucket_width_ms",
            name="uq_pm_book_state_topn_asset_bucket_width",
        ),
        Index("ix_pm_book_state_topn_asset_bucket", "asset_id", "bucket_start_exchange"),
        Index("ix_pm_book_state_topn_condition_bucket", "condition_id", "bucket_start_exchange"),
        Index("ix_pm_book_state_topn_width_bucket", "bucket_width_ms", "bucket_start_exchange"),
        Index("ix_pm_book_state_topn_last_snapshot_id", "last_snapshot_id"),
        Index("ix_pm_book_state_topn_recon_state_id", "recon_state_id"),
    )


class PolymarketMicrostructureFeature100ms(Base):
    __tablename__ = "polymarket_microstructure_features_100ms"

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
    bucket_start_exchange: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_end_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_book_state_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_state_topn.id", ondelete="SET NULL"),
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_feature_runs.id", ondelete="SET NULL"),
    )
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    microprice: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    bid_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    imbalance_top1: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top3: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top5: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    bid_add_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_add_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_remove_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_remove_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    buy_trade_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    sell_trade_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    buy_trade_count: Mapped[int | None] = mapped_column(Integer)
    sell_trade_count: Mapped[int | None] = mapped_column(Integer)
    trade_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    last_trade_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    last_trade_side: Mapped[str | None] = mapped_column(String(16))
    book_update_count: Mapped[int | None] = mapped_column(Integer)
    bbo_update_count: Mapped[int | None] = mapped_column(Integer)
    completeness_flags_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("asset_id", "bucket_start_exchange", name="uq_pm_micro_features_100ms_asset_bucket"),
        Index("ix_pm_micro_features_100ms_asset_bucket", "asset_id", "bucket_start_exchange"),
        Index("ix_pm_micro_features_100ms_condition_bucket", "condition_id", "bucket_start_exchange"),
        Index("ix_pm_micro_features_100ms_run_id", "run_id"),
        Index("ix_pm_micro_features_100ms_source_book_state_id", "source_book_state_id"),
    )


class PolymarketMicrostructureFeature1s(Base):
    __tablename__ = "polymarket_microstructure_features_1s"

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
    bucket_start_exchange: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_end_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_book_state_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_state_topn.id", ondelete="SET NULL"),
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_feature_runs.id", ondelete="SET NULL"),
    )
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    microprice: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    bid_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top1: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top3: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_depth_top5: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    imbalance_top1: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top3: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    imbalance_top5: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    bid_add_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_add_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    bid_remove_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    ask_remove_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    buy_trade_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    sell_trade_volume: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    buy_trade_count: Mapped[int | None] = mapped_column(Integer)
    sell_trade_count: Mapped[int | None] = mapped_column(Integer)
    trade_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    last_trade_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    last_trade_side: Mapped[str | None] = mapped_column(String(16))
    book_update_count: Mapped[int | None] = mapped_column(Integer)
    bbo_update_count: Mapped[int | None] = mapped_column(Integer)
    completeness_flags_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("asset_id", "bucket_start_exchange", name="uq_pm_micro_features_1s_asset_bucket"),
        Index("ix_pm_micro_features_1s_asset_bucket", "asset_id", "bucket_start_exchange"),
        Index("ix_pm_micro_features_1s_condition_bucket", "condition_id", "bucket_start_exchange"),
        Index("ix_pm_micro_features_1s_run_id", "run_id"),
        Index("ix_pm_micro_features_1s_source_book_state_id", "source_book_state_id"),
    )


class PolymarketAlphaLabel(Base):
    __tablename__ = "polymarket_alpha_labels"

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
    anchor_bucket_start_exchange: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    horizon_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    source_feature_table: Mapped[str] = mapped_column(String(128), nullable=False)
    source_feature_row_id: Mapped[int] = mapped_column(Integer, nullable=False)
    start_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    end_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    mid_return_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    mid_move_ticks: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_bid_change: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask_change: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    up_move: Mapped[bool | None] = mapped_column(Boolean)
    down_move: Mapped[bool | None] = mapped_column(Boolean)
    flat_move: Mapped[bool | None] = mapped_column(Boolean)
    completeness_flags_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "anchor_bucket_start_exchange",
            "horizon_ms",
            "source_feature_table",
            name="uq_pm_alpha_labels_asset_anchor_horizon_source",
        ),
        Index("ix_pm_alpha_labels_asset_anchor", "asset_id", "anchor_bucket_start_exchange"),
        Index("ix_pm_alpha_labels_condition_anchor", "condition_id", "anchor_bucket_start_exchange"),
        Index("ix_pm_alpha_labels_horizon_source", "horizon_ms", "source_feature_table"),
    )


class PolymarketPassiveFillLabel(Base):
    __tablename__ = "polymarket_passive_fill_labels"

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
    anchor_bucket_start_exchange: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    horizon_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    side: Mapped[str] = mapped_column(String(32), nullable=False)
    posted_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    touch_observed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    trade_through_observed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    best_price_improved_against_order: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    adverse_move_after_touch_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    source_feature_table: Mapped[str] = mapped_column(String(128), nullable=False)
    source_feature_row_id: Mapped[int] = mapped_column(Integer, nullable=False)
    completeness_flags_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "anchor_bucket_start_exchange",
            "horizon_ms",
            "side",
            "source_feature_table",
            name="uq_pm_passive_fill_labels_asset_anchor_horizon_side_source",
        ),
        Index("ix_pm_passive_fill_labels_asset_anchor", "asset_id", "anchor_bucket_start_exchange"),
        Index("ix_pm_passive_fill_labels_condition_anchor", "condition_id", "anchor_bucket_start_exchange"),
        Index("ix_pm_passive_fill_labels_horizon_side_source", "horizon_ms", "side", "source_feature_table"),
    )
