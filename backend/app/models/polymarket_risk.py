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


class RiskGraphNode(Base):
    __tablename__ = "risk_graph_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    node_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    node_type: Mapped[str] = mapped_column(String(64), nullable=False)
    venue: Mapped[str | None] = mapped_column(String(64))
    event_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_event_dim.id", ondelete="SET NULL"),
    )
    market_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_market_dim.id", ondelete="SET NULL"),
    )
    asset_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_asset_dim.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    label: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_risk_graph_nodes_type_active", "node_type", "active"),
        Index("ix_risk_graph_nodes_venue_type", "venue", "node_type"),
        Index("ix_risk_graph_nodes_condition_id", "condition_id"),
        Index("ix_risk_graph_nodes_asset_id", "asset_id"),
        Index("ix_risk_graph_nodes_event_dim_id", "event_dim_id"),
        Index("ix_risk_graph_nodes_market_dim_id", "market_dim_id"),
        Index("ix_risk_graph_nodes_asset_dim_id", "asset_dim_id"),
    )


class RiskGraphEdge(Base):
    __tablename__ = "risk_graph_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    left_node_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("risk_graph_nodes.id", ondelete="CASCADE"),
        nullable=False,
    )
    right_node_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("risk_graph_nodes.id", ondelete="CASCADE"),
        nullable=False,
    )
    edge_type: Mapped[str] = mapped_column(String(64), nullable=False)
    weight: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "left_node_id",
            "right_node_id",
            "edge_type",
            "source_kind",
            name="uq_risk_graph_edges_pair_type_source",
        ),
        Index("ix_risk_graph_edges_type_active", "edge_type", "active"),
        Index("ix_risk_graph_edges_left_node_id", "left_node_id"),
        Index("ix_risk_graph_edges_right_node_id", "right_node_id"),
        Index("ix_risk_graph_edges_source_kind", "source_kind"),
    )


class RiskGraphRun(Base):
    __tablename__ = "risk_graph_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_type: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    scope_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    rows_inserted_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_risk_graph_runs_started_at", "started_at"),
        Index("ix_risk_graph_runs_type_reason_started", "run_type", "reason", "started_at"),
        Index("ix_risk_graph_runs_status_started", "status", "started_at"),
    )


class PortfolioExposureSnapshot(Base):
    __tablename__ = "portfolio_exposure_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("risk_graph_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    strategy_family: Mapped[str | None] = mapped_column(String(32))
    strategy_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="SET NULL"),
    )
    node_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("risk_graph_nodes.id", ondelete="CASCADE"),
        nullable=False,
    )
    exposure_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    gross_notional_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_notional_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    buy_notional_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    sell_notional_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    share_exposure: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reservation_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    hedged_fraction: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    regime_label: Mapped[str | None] = mapped_column(String(32))
    budget_metadata_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_portfolio_exposure_snapshots_run_id", "run_id"),
        Index("ix_portfolio_exposure_snapshots_snapshot_at", "snapshot_at"),
        Index("ix_portfolio_exposure_snapshots_strategy_snapshot", "strategy_family", "snapshot_at"),
        Index("ix_portfolio_exposure_snapshots_strategy_version_snapshot", "strategy_version_id", "snapshot_at"),
        Index("ix_portfolio_exposure_snapshots_node_kind", "node_id", "exposure_kind"),
        Index("ix_portfolio_exposure_snapshots_kind_snapshot", "exposure_kind", "snapshot_at"),
    )


class PortfolioOptimizerRecommendation(Base):
    __tablename__ = "portfolio_optimizer_recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("risk_graph_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("risk_graph_nodes.id", ondelete="SET NULL"),
    )
    strategy_family: Mapped[str | None] = mapped_column(String(32))
    strategy_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="SET NULL"),
    )
    recommendation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    target_size_cap_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    inventory_penalty_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    reservation_price_adjustment_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    maker_budget_remaining_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_budget_remaining_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    regime_label: Mapped[str | None] = mapped_column(String(32))
    budget_metadata_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_portfolio_optimizer_recommendations_run_id", "run_id"),
        Index("ix_portfolio_optimizer_recommendations_strategy_observed", "strategy_family", "observed_at_local"),
        Index("ix_portfolio_optimizer_recs_strategy_version_observed", "strategy_version_id", "observed_at_local"),
        Index(
            "ix_portfolio_optimizer_recommendations_type_reason_observed",
            "recommendation_type",
            "reason_code",
            "observed_at_local",
        ),
        Index("ix_portfolio_optimizer_recommendations_condition_observed", "condition_id", "observed_at_local"),
        Index("ix_portfolio_optimizer_recommendations_asset_observed", "asset_id", "observed_at_local"),
        Index("ix_portfolio_optimizer_recommendations_scope_kind", "scope_kind"),
    )


class InventoryControlSnapshot(Base):
    __tablename__ = "inventory_control_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    strategy_family: Mapped[str | None] = mapped_column(String(32))
    strategy_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("strategy_versions.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    control_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    maker_budget_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_budget_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    maker_budget_used_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_budget_used_usd: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reservation_price_shift_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    quote_skew_direction: Mapped[str | None] = mapped_column(String(32))
    no_quote: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reason_code: Mapped[str | None] = mapped_column(String(64))
    regime_label: Mapped[str | None] = mapped_column(String(32))
    budget_metadata_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_inventory_control_snapshots_snapshot_at", "snapshot_at"),
        Index("ix_inventory_control_snapshots_strategy_snapshot", "strategy_family", "snapshot_at"),
        Index("ix_inventory_control_snapshots_strategy_version_snapshot", "strategy_version_id", "snapshot_at"),
        Index("ix_inventory_control_snapshots_scope_reason", "control_scope", "reason_code"),
        Index("ix_inventory_control_snapshots_condition_snapshot", "condition_id", "snapshot_at"),
        Index("ix_inventory_control_snapshots_asset_snapshot", "asset_id", "snapshot_at"),
    )
