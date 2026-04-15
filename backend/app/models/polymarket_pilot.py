from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketPilotConfig(Base):
    __tablename__ = "polymarket_pilot_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pilot_name: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_family: Mapped[str] = mapped_column(String(32), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    armed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    manual_approval_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    live_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    market_allowlist_json: Mapped[list[str] | None] = mapped_column(JSONB)
    category_allowlist_json: Mapped[list[str] | None] = mapped_column(JSONB)
    max_notional_per_order_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    max_notional_per_day_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    max_open_orders: Mapped[int | None] = mapped_column(Integer)
    max_plan_age_seconds: Mapped[int | None] = mapped_column(Integer)
    max_decision_age_seconds: Mapped[int | None] = mapped_column(Integer)
    max_slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    require_complete_replay_coverage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("pilot_name", name="uq_pm_pilot_configs_pilot_name"),
        Index("ix_pm_pilot_configs_family_active", "strategy_family", "active"),
        Index("ix_pm_pilot_configs_active_updated", "active", "updated_at"),
    )


class PolymarketPilotRun(Base):
    __tablename__ = "polymarket_pilot_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pilot_config_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_pilot_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_pilot_runs_config_started", "pilot_config_id", "started_at"),
        Index("ix_pm_pilot_runs_status_started", "status", "started_at"),
    )


class PolymarketPilotApprovalEvent(Base):
    __tablename__ = "polymarket_pilot_approval_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    execution_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("execution_decisions.id", ondelete="SET NULL"),
    )
    pilot_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_pilot_runs.id", ondelete="SET NULL"),
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    operator_identity: Mapped[str | None] = mapped_column(String(128))
    reason_code: Mapped[str | None] = mapped_column(String(128))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_pilot_approval_events_run_observed", "pilot_run_id", "observed_at_local"),
        Index("ix_pm_pilot_approval_events_live_order_observed", "live_order_id", "observed_at_local"),
        Index("ix_pm_pilot_approval_events_action_observed", "action", "observed_at_local"),
    )


class PolymarketControlPlaneIncident(Base):
    __tablename__ = "polymarket_control_plane_incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pilot_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_pilot_runs.id", ondelete="SET NULL"),
    )
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    incident_type: Mapped[str] = mapped_column(String(64), nullable=False)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_control_incidents_run_observed", "pilot_run_id", "observed_at_local"),
        Index("ix_pm_control_incidents_type_observed", "incident_type", "observed_at_local"),
        Index("ix_pm_control_incidents_severity_observed", "severity", "observed_at_local"),
    )


class PolymarketLiveShadowEvaluation(Base):
    __tablename__ = "polymarket_live_shadow_evaluations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    execution_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("execution_decisions.id", ondelete="SET NULL"),
    )
    replay_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_replay_runs.id", ondelete="SET NULL"),
    )
    variant_name: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    actual_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    expected_fill_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    actual_fill_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    expected_net_ev_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    realized_net_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    gap_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    reason_code: Mapped[str | None] = mapped_column(String(128))
    coverage_limited: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_live_shadow_eval_live_order", "live_order_id"),
        Index("ix_pm_live_shadow_eval_variant_created", "variant_name", "created_at"),
        Index("ix_pm_live_shadow_eval_replay_run", "replay_run_id"),
    )


class PolymarketPilotScorecard(Base):
    __tablename__ = "polymarket_pilot_scorecards"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_family: Mapped[str] = mapped_column(String(32), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    live_orders_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fills_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approval_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approval_expired_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejection_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    incident_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gross_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    fees_paid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    avg_shadow_gap_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    worst_shadow_gap_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    coverage_limited_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("strategy_family", "window_start", "window_end", name="uq_pm_pilot_scorecards_window"),
        Index("ix_pm_pilot_scorecards_strategy_window", "strategy_family", "window_start", "window_end"),
        Index("ix_pm_pilot_scorecards_status_created", "status", "created_at"),
    )


class PolymarketPilotGuardrailEvent(Base):
    __tablename__ = "polymarket_pilot_guardrail_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_family: Mapped[str] = mapped_column(String(32), nullable=False)
    guardrail_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("live_orders.id", ondelete="SET NULL"),
    )
    pilot_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_pilot_runs.id", ondelete="SET NULL"),
    )
    trigger_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    threshold_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    action_taken: Mapped[str] = mapped_column(String(32), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_guardrail_events_strategy_observed", "strategy_family", "observed_at_local"),
        Index("ix_pm_guardrail_events_type_observed", "guardrail_type", "observed_at_local"),
        Index("ix_pm_guardrail_events_run_observed", "pilot_run_id", "observed_at_local"),
    )


class PolymarketPilotReadinessReport(Base):
    __tablename__ = "polymarket_pilot_readiness_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_family: Mapped[str] = mapped_column(String(32), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    scorecard_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_pilot_scorecards.id", ondelete="SET NULL"),
    )
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approval_backlog_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    coverage_limited_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shadow_gap_breach_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("strategy_family", "window_start", "window_end", name="uq_pm_pilot_readiness_window"),
        Index("ix_pm_readiness_reports_strategy_generated", "strategy_family", "generated_at"),
        Index("ix_pm_readiness_reports_status_generated", "status", "generated_at"),
    )
