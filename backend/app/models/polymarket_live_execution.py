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
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketUserEventRaw(Base):
    __tablename__ = "polymarket_user_events_raw"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stream_session_id: Mapped[str | None] = mapped_column(String(64))
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recv_ts_local: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingest_ts_db: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    source_payload_json: Mapped[dict | list | str] = mapped_column(JSONB, nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_user_events_raw_condition_ingest", "condition_id", "ingest_ts_db"),
        Index("ix_pm_user_events_raw_asset_ingest", "asset_id", "ingest_ts_db"),
        Index("ix_pm_user_events_raw_event_type_ingest", "event_type", "ingest_ts_db"),
        Index("ix_pm_user_events_raw_stream_session_id", "stream_session_id"),
    )


class LiveOrder(Base):
    __tablename__ = "live_orders"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    execution_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("execution_decisions.id", ondelete="SET NULL"),
    )
    signal_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("signals.id", ondelete="SET NULL"),
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
    client_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    venue_order_id: Mapped[str | None] = mapped_column(String(255))
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    order_type: Mapped[str] = mapped_column(String(32), nullable=False)
    post_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    target_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    requested_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    submitted_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    filled_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False, default=Decimal("0"))
    avg_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    manual_approval_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    approved_by: Mapped[str | None] = mapped_column(String(128))
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    strategy_family: Mapped[str | None] = mapped_column(String(32))
    strategy_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="SET NULL"),
    )
    pilot_config_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_pilot_configs.id", ondelete="SET NULL"),
    )
    pilot_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_pilot_runs.id", ondelete="SET NULL"),
    )
    approval_state: Mapped[str] = mapped_column(String(32), nullable=False, default="not_required")
    approval_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    approval_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    blocked_reason_code: Mapped[str | None] = mapped_column(String(128))
    kill_switch_blocked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    allowlist_blocked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    validation_error: Mapped[str | None] = mapped_column(Text)
    submission_error: Mapped[str | None] = mapped_column(Text)
    policy_version: Mapped[str | None] = mapped_column(String(64))
    decision_reason_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    execution_decision: Mapped["ExecutionDecision | None"] = relationship()
    order_events: Mapped[list["LiveOrderEvent"]] = relationship(
        back_populates="live_order",
        cascade="all, delete-orphan",
    )
    fills: Mapped[list["LiveFill"]] = relationship(
        back_populates="live_order",
        cascade="all, delete-orphan",
    )
    reservations: Mapped[list["CapitalReservation"]] = relationship(
        back_populates="live_order",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("client_order_id", name="uq_live_orders_client_order_id"),
        Index("ix_live_orders_execution_decision_id", "execution_decision_id"),
        Index("ix_live_orders_signal_id", "signal_id"),
        Index("ix_live_orders_condition_status", "condition_id", "status"),
        Index("ix_live_orders_asset_status", "asset_id", "status"),
        Index("ix_live_orders_status_created_at", "status", "created_at"),
        Index("ix_live_orders_venue_order_id", "venue_order_id"),
        Index("ix_live_orders_strategy_status", "strategy_family", "status"),
        Index("ix_live_orders_strategy_version_created", "strategy_version_id", "created_at"),
        Index("ix_live_orders_approval_state_created", "approval_state", "created_at"),
    )


class LiveOrderEvent(Base):
    __tablename__ = "live_order_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    live_order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="CASCADE"),
        nullable=False,
    )
    raw_user_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_user_events_raw.id", ondelete="SET NULL"),
    )
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    venue_status: Mapped[str | None] = mapped_column(String(32))
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    live_order: Mapped["LiveOrder"] = relationship(back_populates="order_events")
    raw_user_event: Mapped["PolymarketUserEventRaw | None"] = relationship()

    __table_args__ = (
        UniqueConstraint("fingerprint", name="uq_live_order_events_fingerprint"),
        Index("ix_live_order_events_live_order_observed", "live_order_id", "observed_at_local"),
        Index("ix_live_order_events_raw_user_event_id", "raw_user_event_id"),
        Index("ix_live_order_events_source_kind_observed", "source_kind", "observed_at_local"),
    )


class LiveFill(Base):
    __tablename__ = "live_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    trade_id: Mapped[str | None] = mapped_column(String(255))
    transaction_hash: Mapped[str | None] = mapped_column(String(255))
    fill_status: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    fee_paid: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fee_currency: Mapped[str | None] = mapped_column(String(32))
    maker_taker: Mapped[str | None] = mapped_column(String(16))
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    raw_user_event_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_user_events_raw.id", ondelete="SET NULL"),
    )
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    live_order: Mapped["LiveOrder | None"] = relationship(back_populates="fills")
    raw_user_event: Mapped["PolymarketUserEventRaw | None"] = relationship()

    __table_args__ = (
        UniqueConstraint("fingerprint", name="uq_live_fills_fingerprint"),
        Index("ix_live_fills_live_order_observed", "live_order_id", "observed_at_local"),
        Index("ix_live_fills_condition_observed", "condition_id", "observed_at_local"),
        Index("ix_live_fills_asset_observed", "asset_id", "observed_at_local"),
        Index("ix_live_fills_trade_id", "trade_id"),
    )


class PositionLot(Base):
    __tablename__ = "position_lots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    condition_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy_family: Mapped[str] = mapped_column(String(32), nullable=False)
    pilot_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_pilot_runs.id", ondelete="SET NULL"),
    )
    source_live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    source_fill_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("live_fills.id", ondelete="SET NULL"),
    )
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    open_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    remaining_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    avg_open_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    avg_close_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    realized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    fee_paid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    source_live_order: Mapped["LiveOrder | None"] = relationship(foreign_keys=[source_live_order_id])
    source_fill: Mapped["LiveFill | None"] = relationship(foreign_keys=[source_fill_id])
    lot_events: Mapped[list["PositionLotEvent"]] = relationship(
        back_populates="lot",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_position_lots_strategy_status_opened", "strategy_family", "status", "opened_at"),
        Index("ix_position_lots_condition_asset_status", "condition_id", "asset_id", "status"),
        Index("ix_position_lots_pilot_run", "pilot_run_id"),
        Index("ix_position_lots_source_live_order", "source_live_order_id"),
        Index("ix_position_lots_source_fill", "source_fill_id"),
    )


class PositionLotEvent(Base):
    __tablename__ = "position_lot_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lot_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("position_lots.id", ondelete="CASCADE"),
        nullable=False,
    )
    live_fill_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("live_fills.id", ondelete="SET NULL"),
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    size_delta: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fee_delta: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    realized_pnl_delta: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    lot: Mapped["PositionLot"] = relationship(back_populates="lot_events")
    live_fill: Mapped["LiveFill | None"] = relationship(foreign_keys=[live_fill_id])

    __table_args__ = (
        Index("ix_position_lot_events_lot_observed", "lot_id", "observed_at_local"),
        Index("ix_position_lot_events_live_fill", "live_fill_id"),
        Index("ix_position_lot_events_type_observed", "event_type", "observed_at_local"),
    )


class CapitalReservation(Base):
    __tablename__ = "capital_reservations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    reservation_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_amount: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    reserved_amount: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    released_amount: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False, default=Decimal("0"))
    open_amount: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    live_order: Mapped["LiveOrder | None"] = relationship(back_populates="reservations")

    __table_args__ = (
        UniqueConstraint("fingerprint", name="uq_capital_reservations_fingerprint"),
        Index("ix_capital_reservations_live_order_observed", "live_order_id", "observed_at_local"),
        Index("ix_capital_reservations_condition_observed", "condition_id", "observed_at_local"),
        Index("ix_capital_reservations_status_observed", "status", "observed_at_local"),
    )


class PolymarketLiveState(Base):
    __tablename__ = "polymarket_live_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    kill_switch_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    allowlist_markets_json: Mapped[list[str] | None] = mapped_column(JSONB)
    allowlist_categories_json: Mapped[list[str] | None] = mapped_column(JSONB)
    gateway_reachable: Mapped[bool | None] = mapped_column(Boolean)
    gateway_last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    gateway_last_error: Mapped[str | None] = mapped_column(String(255))
    user_stream_connected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_stream_session_id: Mapped[str | None] = mapped_column(String(64))
    user_stream_connection_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_user_stream_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_user_stream_error: Mapped[str | None] = mapped_column(String(255))
    last_user_stream_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconciled_user_event_id: Mapped[int | None] = mapped_column(Integer)
    last_reconcile_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconcile_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_reconcile_error: Mapped[str | None] = mapped_column(String(255))
    last_reconcile_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_healthy: Mapped[bool | None] = mapped_column(Boolean)
    heartbeat_last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_last_error: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )
