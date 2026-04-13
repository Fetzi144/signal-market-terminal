from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketRawCaptureRun(Base):
    __tablename__ = "polymarket_raw_capture_runs"

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
        Index("ix_pm_raw_capture_runs_reason_started", "reason", "started_at"),
        Index("ix_pm_raw_capture_runs_run_type_started", "run_type", "started_at"),
        Index("ix_pm_raw_capture_runs_status_started", "status", "started_at"),
    )


class PolymarketBookSnapshot(Base):
    __tablename__ = "polymarket_book_snapshots"

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
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recv_ts_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingest_ts_db: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    stream_session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
        unique=True,
    )
    capture_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_raw_capture_runs.id", ondelete="SET NULL"),
    )
    book_hash: Mapped[str | None] = mapped_column(String(255))
    bids_json: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    asks_json: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    min_order_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    neg_risk: Mapped[bool | None] = mapped_column(Boolean)
    last_trade_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fingerprint: Mapped[str | None] = mapped_column(String(128), unique=True)
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_book_snapshots_asset_event_ts", "asset_id", "event_ts_exchange"),
        Index("ix_pm_book_snapshots_condition_event_ts", "condition_id", "event_ts_exchange"),
        Index("ix_pm_book_snapshots_observed_at", "observed_at_local"),
        Index("ix_pm_book_snapshots_capture_run_id", "capture_run_id"),
    )


class PolymarketBookDelta(Base):
    __tablename__ = "polymarket_book_deltas"

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
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recv_ts_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingest_ts_db: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    stream_session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    raw_event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="CASCADE"),
        nullable=False,
    )
    delta_index: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    delta_hash: Mapped[str | None] = mapped_column(String(255))
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        UniqueConstraint("raw_event_id", "delta_index", name="uq_pm_book_deltas_raw_event_delta_index"),
        Index("ix_pm_book_deltas_asset_event_ts", "asset_id", "event_ts_exchange"),
        Index("ix_pm_book_deltas_condition_event_ts", "condition_id", "event_ts_exchange"),
        Index("ix_pm_book_deltas_raw_event_id", "raw_event_id"),
    )


class PolymarketBboEvent(Base):
    __tablename__ = "polymarket_bbo_events"

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
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recv_ts_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingest_ts_db: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    stream_session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
        unique=True,
    )
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_bbo_events_asset_event_ts", "asset_id", "event_ts_exchange"),
        Index("ix_pm_bbo_events_condition_event_ts", "condition_id", "event_ts_exchange"),
    )


class PolymarketTradeTape(Base):
    __tablename__ = "polymarket_trade_tape"

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
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recv_ts_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingest_ts_db: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    stream_session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
        unique=True,
    )
    capture_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_raw_capture_runs.id", ondelete="SET NULL"),
    )
    transaction_hash: Mapped[str | None] = mapped_column(String(255))
    side: Mapped[str | None] = mapped_column(String(16))
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    fee_rate_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    event_slug: Mapped[str | None] = mapped_column(String(512))
    outcome_name: Mapped[str | None] = mapped_column(String(255))
    outcome_index: Mapped[int | None] = mapped_column(Integer)
    proxy_wallet: Mapped[str | None] = mapped_column(String(255))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    fallback_fingerprint: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_pm_trade_tape_asset_event_ts", "asset_id", "event_ts_exchange"),
        Index("ix_pm_trade_tape_condition_event_ts", "condition_id", "event_ts_exchange"),
        Index("ix_pm_trade_tape_observed_at", "observed_at_local"),
        Index("ix_pm_trade_tape_capture_run_id", "capture_run_id"),
        Index("ix_pm_trade_tape_transaction_hash", "transaction_hash"),
        Index("ix_pm_trade_tape_fallback_fingerprint", "fallback_fingerprint"),
    )


class PolymarketOpenInterestHistory(Base):
    __tablename__ = "polymarket_open_interest_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    capture_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_raw_capture_runs.id", ondelete="SET NULL"),
    )
    value: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    source_payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        UniqueConstraint("capture_run_id", "condition_id", name="uq_pm_open_interest_history_capture_condition"),
        Index("ix_pm_open_interest_history_condition_observed", "condition_id", "observed_at_local"),
        Index("ix_pm_open_interest_history_market_observed", "market_dim_id", "observed_at_local"),
        Index("ix_pm_open_interest_history_capture_run_id", "capture_run_id"),
    )
