from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PolymarketReplayRun(Base):
    __tablename__ = "polymarket_replay_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    run_type: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_family: Mapped[str | None] = mapped_column(String(64))
    strategy_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="SET NULL"),
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    scenario_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    time_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    time_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    rows_inserted_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_pm_replay_runs_type_reason_started", "run_type", "reason", "started_at"),
        Index("ix_pm_replay_runs_strategy_version_started", "strategy_version_id", "started_at"),
        Index("ix_pm_replay_runs_status_started", "status", "started_at"),
        Index("ix_pm_replay_runs_window", "time_window_start", "time_window_end"),
    )


class PolymarketReplayScenario(Base):
    __tablename__ = "polymarket_replay_scenarios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_replay_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    scenario_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    scenario_type: Mapped[str] = mapped_column(String(64), nullable=False)
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    group_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_groups.id", ondelete="SET NULL"),
    )
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    policy_version: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_replay_scenarios_run_id", "run_id"),
        Index("ix_pm_replay_scenarios_type_window", "scenario_type", "window_start"),
        Index("ix_pm_replay_scenarios_condition_window", "condition_id", "window_start"),
        Index("ix_pm_replay_scenarios_asset_window", "asset_id", "window_start"),
        Index("ix_pm_replay_scenarios_status_updated", "status", "updated_at"),
    )


class PolymarketReplayOrder(Base):
    __tablename__ = "polymarket_replay_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scenario_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_scenarios.id", ondelete="CASCADE"),
        nullable=False,
    )
    variant_name: Mapped[str] = mapped_column(String(64), nullable=False)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    side: Mapped[str | None] = mapped_column(String(16))
    action_type: Mapped[str | None] = mapped_column(String(32))
    order_type_hint: Mapped[str | None] = mapped_column(String(32))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    requested_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    submitted_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expiry_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_execution_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("execution_decisions.id", ondelete="SET NULL"),
    )
    source_execution_candidate_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_execution_action_candidates.id", ondelete="SET NULL"),
    )
    source_structure_opportunity_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="SET NULL"),
    )
    source_quote_recommendation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_quote_recommendations.id", ondelete="SET NULL"),
    )
    source_optimizer_recommendation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("portfolio_optimizer_recommendations.id", ondelete="SET NULL"),
    )
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("scenario_id", "variant_name", "sequence_no", name="uq_pm_replay_orders_scenario_variant_sequence"),
        Index("ix_pm_replay_orders_scenario_variant", "scenario_id", "variant_name"),
        Index("ix_pm_replay_orders_status_decision_ts", "status", "decision_ts"),
        Index("ix_pm_replay_orders_source_execution_decision", "source_execution_decision_id"),
        Index("ix_pm_replay_orders_source_quote_recommendation", "source_quote_recommendation_id"),
    )


class PolymarketReplayFill(Base):
    __tablename__ = "polymarket_replay_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scenario_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_scenarios.id", ondelete="CASCADE"),
        nullable=False,
    )
    replay_order_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_orders.id", ondelete="CASCADE"),
        nullable=False,
    )
    variant_name: Mapped[str] = mapped_column(String(64), nullable=False)
    fill_index: Mapped[int] = mapped_column(Integer, nullable=False)
    fill_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    fee_paid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reward_estimate: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_taker: Mapped[str | None] = mapped_column(String(16))
    fill_source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("replay_order_id", "fill_index", name="uq_pm_replay_fills_order_fill_index"),
        Index("ix_pm_replay_fills_scenario_variant", "scenario_id", "variant_name"),
        Index("ix_pm_replay_fills_source_kind_fill_ts", "fill_source_kind", "fill_ts"),
    )


class PolymarketReplayMetric(Base):
    __tablename__ = "polymarket_replay_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("polymarket_replay_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    scenario_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_scenarios.id", ondelete="CASCADE"),
    )
    metric_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    variant_name: Mapped[str] = mapped_column(String(64), nullable=False)
    gross_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_pnl: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    fees_paid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    rewards_estimated: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fill_rate: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    cancel_rate: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    action_mix_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    drawdown_proxy: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_replay_metrics_run_variant", "run_id", "variant_name"),
        Index("ix_pm_replay_metrics_scenario_variant", "scenario_id", "variant_name"),
        Index("ix_pm_replay_metrics_scope_variant", "metric_scope", "variant_name"),
    )


class PolymarketReplayDecisionTrace(Base):
    __tablename__ = "polymarket_replay_decision_traces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scenario_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_scenarios.id", ondelete="CASCADE"),
        nullable=False,
    )
    replay_order_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_replay_orders.id", ondelete="SET NULL"),
    )
    variant_name: Mapped[str] = mapped_column(String(64), nullable=False)
    trace_type: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_pm_replay_decision_traces_scenario_variant", "scenario_id", "variant_name"),
        Index("ix_pm_replay_decision_traces_type_observed", "trace_type", "observed_at_local"),
        Index("ix_pm_replay_decision_traces_reason_observed", "reason_code", "observed_at_local"),
    )
