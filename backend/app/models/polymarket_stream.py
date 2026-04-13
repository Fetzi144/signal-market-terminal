import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketMarketEvent(Base):
    __tablename__ = "polymarket_market_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(32), nullable=False, default="polymarket")
    provenance: Mapped[str] = mapped_column(String(32), nullable=False)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    message_type: Mapped[str] = mapped_column(String(64), nullable=False)
    market_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    asset_ids: Mapped[list[str] | None] = mapped_column(JSONB)
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    connection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    ingest_batch_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    source_message_id: Mapped[str | None] = mapped_column(String(255))
    source_hash: Mapped[str | None] = mapped_column(String(255))
    source_sequence: Mapped[str | None] = mapped_column(String(255))
    source_cursor: Mapped[str | None] = mapped_column(String(255))
    resync_reason: Mapped[str | None] = mapped_column(String(64))
    resync_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_resync_runs.id", ondelete="SET NULL"),
    )
    payload: Mapped[dict | list | str] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_pm_market_events_asset_received", "asset_id", "received_at_local"),
        Index("ix_pm_market_events_event_time", "event_time"),
        Index("ix_pm_market_events_market_received", "market_id", "received_at_local"),
        Index("ix_pm_market_events_message_type", "message_type"),
        Index("ix_pm_market_events_resync_run_id", "resync_run_id"),
        Index("ix_pm_market_events_venue_provenance_received", "venue", "provenance", "received_at_local"),
    )


class PolymarketStreamStatus(Base):
    __tablename__ = "polymarket_stream_status"

    venue: Mapped[str] = mapped_column(String(32), primary_key=True, default="polymarket")
    connected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    connection_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    current_connection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    last_message_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    active_subscription_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reconnect_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    resync_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gap_suspected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    malformed_message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_resync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconciliation_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class PolymarketIngestIncident(Base):
    __tablename__ = "polymarket_ingest_incidents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    incident_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    asset_id: Mapped[str | None] = mapped_column(String(255))
    connection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
    )
    resync_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_resync_runs.id", ondelete="SET NULL"),
    )
    details_json: Mapped[dict | None] = mapped_column(JSONB)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_pm_ingest_incidents_asset_created", "asset_id", "created_at"),
        Index("ix_pm_ingest_incidents_connection_created", "connection_id", "created_at"),
        Index("ix_pm_ingest_incidents_created_at", "created_at"),
        Index("ix_pm_ingest_incidents_resync_run_id", "resync_run_id"),
        Index("ix_pm_ingest_incidents_type_created", "incident_type", "created_at"),
    )


class PolymarketResyncRun(Base):
    __tablename__ = "polymarket_resync_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    connection_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    requested_asset_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    succeeded_asset_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_asset_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_pm_resync_runs_reason_started", "reason", "started_at"),
        Index("ix_pm_resync_runs_started_at", "started_at"),
        Index("ix_pm_resync_runs_status_started", "status", "started_at"),
    )


class PolymarketWatchAsset(Base):
    __tablename__ = "polymarket_watch_assets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    outcome_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="CASCADE"),
        nullable=False,
    )
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    watch_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    watch_reason: Mapped[str | None] = mapped_column(String(255))
    priority: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_pm_watch_assets_asset_id", "asset_id"),
        Index("ix_pm_watch_assets_enabled_priority", "watch_enabled", "priority"),
        Index("ix_pm_watch_assets_outcome_id", "outcome_id", unique=True),
    )


class PolymarketNormalizedEvent(Base):
    __tablename__ = "polymarket_normalized_events"

    raw_event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="CASCADE"),
        primary_key=True,
    )
    venue: Mapped[str] = mapped_column(String(32), nullable=False, default="polymarket")
    provenance: Mapped[str] = mapped_column(String(32), nullable=False)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    message_type: Mapped[str] = mapped_column(String(64), nullable=False)
    market_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side: Mapped[str | None] = mapped_column(String(16))
    price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_bid_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    is_book_event: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_top_of_book: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parse_status: Mapped[str] = mapped_column(String(32), nullable=False, default="parsed")
    details_json: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_pm_normalized_events_asset_received", "asset_id", "received_at_local"),
        Index("ix_pm_normalized_events_message_type", "message_type"),
        Index("ix_pm_normalized_events_parse_status", "parse_status"),
    )
