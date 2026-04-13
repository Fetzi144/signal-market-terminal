from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketBookReconState(Base):
    __tablename__ = "polymarket_book_recon_state"

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
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="unseeded")
    last_snapshot_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_snapshots.id", ondelete="SET NULL"),
    )
    last_snapshot_source_kind: Mapped[str | None] = mapped_column(String(64))
    last_snapshot_hash: Mapped[str | None] = mapped_column(String(255))
    last_snapshot_exchange_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_applied_raw_event_id: Mapped[int | None] = mapped_column(Integer)
    last_applied_delta_raw_event_id: Mapped[int | None] = mapped_column(Integer)
    last_applied_delta_index: Mapped[int | None] = mapped_column(Integer)
    last_bbo_raw_event_id: Mapped[int | None] = mapped_column(Integer)
    last_trade_raw_event_id: Mapped[int | None] = mapped_column(Integer)
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    depth_levels_bid: Mapped[int | None] = mapped_column(Integer)
    depth_levels_ask: Mapped[int | None] = mapped_column(Integer)
    expected_tick_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    last_exchange_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_received_at_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_resynced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    drift_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    resync_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_book_recon_state_asset_id", "asset_id", unique=True),
        Index("ix_pm_book_recon_state_condition_id", "condition_id"),
        Index("ix_pm_book_recon_state_status_updated", "status", "updated_at"),
        Index("ix_pm_book_recon_state_last_reconciled", "last_reconciled_at"),
    )


class PolymarketBookReconIncident(Base):
    __tablename__ = "polymarket_book_recon_incidents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
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
    incident_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    raw_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_events.id", ondelete="SET NULL"),
    )
    snapshot_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_book_snapshots.id", ondelete="SET NULL"),
    )
    capture_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_raw_capture_runs.id", ondelete="SET NULL"),
    )
    exchange_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    expected_best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    observed_best_bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    expected_best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    observed_best_ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    expected_hash: Mapped[str | None] = mapped_column(String(255))
    observed_hash: Mapped[str | None] = mapped_column(String(255))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    __table_args__ = (
        Index("ix_pm_book_recon_incidents_asset_observed", "asset_id", "observed_at_local"),
        Index("ix_pm_book_recon_incidents_condition_observed", "condition_id", "observed_at_local"),
        Index("ix_pm_book_recon_incidents_type_observed", "incident_type", "observed_at_local"),
        Index("ix_pm_book_recon_incidents_observed", "observed_at_local"),
    )
