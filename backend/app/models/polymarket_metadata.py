from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketEventDim(Base):
    __tablename__ = "polymarket_event_dim"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gamma_event_id: Mapped[str | None] = mapped_column(String(255), unique=True)
    event_slug: Mapped[str | None] = mapped_column(String(512))
    event_ticker: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str | None] = mapped_column(Text)
    subtitle: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(String(128))
    subcategory: Mapped[str | None] = mapped_column(String(128))
    active: Mapped[bool | None] = mapped_column(Boolean)
    closed: Mapped[bool | None] = mapped_column(Boolean)
    archived: Mapped[bool | None] = mapped_column(Boolean)
    neg_risk: Mapped[bool | None] = mapped_column(Boolean)
    neg_risk_market_id: Mapped[str | None] = mapped_column(String(255))
    neg_risk_fee_bips: Mapped[int | None] = mapped_column(Integer)
    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at_source: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at_source: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_gamma_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_stream_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_pm_event_dim_event_slug", "event_slug"),
        Index("ix_pm_event_dim_last_gamma_sync", "last_gamma_sync_at"),
        Index("ix_pm_event_dim_last_stream_event", "last_stream_event_at"),
    )


class PolymarketMarketDim(Base):
    __tablename__ = "polymarket_market_dim"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gamma_market_id: Mapped[str | None] = mapped_column(String(255), unique=True)
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    market_slug: Mapped[str | None] = mapped_column(String(512))
    question: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    event_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_event_dim.id", ondelete="SET NULL"),
    )
    enable_order_book: Mapped[bool | None] = mapped_column(Boolean)
    active: Mapped[bool | None] = mapped_column(Boolean)
    closed: Mapped[bool | None] = mapped_column(Boolean)
    archived: Mapped[bool | None] = mapped_column(Boolean)
    accepting_orders: Mapped[bool | None] = mapped_column(Boolean)
    resolved: Mapped[bool | None] = mapped_column(Boolean)
    resolution_state: Mapped[str | None] = mapped_column(String(64))
    winning_asset_id: Mapped[str | None] = mapped_column(String(255))
    clob_token_ids_json: Mapped[list[str] | None] = mapped_column(JSONB)
    outcomes_json: Mapped[list[str] | None] = mapped_column(JSONB)
    tags_json: Mapped[list | None] = mapped_column(JSONB)
    fees_enabled: Mapped[bool | None] = mapped_column(Boolean)
    fee_schedule_json: Mapped[dict | None] = mapped_column(JSONB)
    maker_base_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    taker_base_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    last_gamma_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_stream_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_pm_market_dim_event_dim_id", "event_dim_id"),
        Index("ix_pm_market_dim_market_slug", "market_slug"),
        Index("ix_pm_market_dim_last_gamma_sync", "last_gamma_sync_at"),
        Index("ix_pm_market_dim_last_stream_event", "last_stream_event_at"),
        Index("ix_pm_market_dim_resolution_state", "resolution_state"),
    )


class PolymarketAssetDim(Base):
    __tablename__ = "polymarket_asset_dim"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    outcome_name: Mapped[str | None] = mapped_column(String(255))
    outcome_index: Mapped[int | None] = mapped_column(Integer)
    active: Mapped[bool | None] = mapped_column(Boolean)
    winner: Mapped[bool | None] = mapped_column(Boolean)
    last_gamma_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_stream_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_pm_asset_dim_condition_id", "condition_id"),
        Index("ix_pm_asset_dim_market_dim_id", "market_dim_id"),
        Index("ix_pm_asset_dim_outcome_id", "outcome_id"),
        Index("ix_pm_asset_dim_last_gamma_sync", "last_gamma_sync_at"),
        Index("ix_pm_asset_dim_last_stream_event", "last_stream_event_at"),
    )


class PolymarketMetaSyncRun(Base):
    __tablename__ = "polymarket_meta_sync_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    include_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    events_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    markets_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    assets_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    events_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    markets_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    param_rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_pm_meta_sync_runs_started_at", "started_at"),
        Index("ix_pm_meta_sync_runs_reason_started", "reason", "started_at"),
        Index("ix_pm_meta_sync_runs_status_started", "status", "started_at"),
    )


class PolymarketMarketParamHistory(Base):
    __tablename__ = "polymarket_market_param_history"

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
    asset_id: Mapped[str | None] = mapped_column(String(255))
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    effective_at_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    sync_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_meta_sync_runs.id", ondelete="SET NULL"),
    )
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
    )
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    min_order_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    neg_risk: Mapped[bool | None] = mapped_column(Boolean)
    fees_enabled: Mapped[bool | None] = mapped_column(Boolean)
    fee_schedule_json: Mapped[dict | None] = mapped_column(JSONB)
    maker_base_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    taker_base_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    resolution_state: Mapped[str | None] = mapped_column(String(64))
    winning_asset_id: Mapped[str | None] = mapped_column(String(255))
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    details_json: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_pm_market_param_history_asset_effective", "asset_id", "effective_at_exchange"),
        Index("ix_pm_market_param_history_condition_effective", "condition_id", "effective_at_exchange"),
        Index("ix_pm_market_param_history_market_effective", "market_dim_id", "effective_at_exchange"),
        Index("ix_pm_market_param_history_observed_at", "observed_at_local"),
        Index("ix_pm_market_param_history_sync_run_id", "sync_run_id"),
        Index("ix_pm_market_param_history_raw_event_id", "raw_event_id"),
    )
