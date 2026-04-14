from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MarketStructureGroup(Base):
    __tablename__ = "market_structure_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    group_type: Mapped[str] = mapped_column(String(64), nullable=False)
    primary_venue: Mapped[str | None] = mapped_column(String(64))
    event_dim_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_event_dim.id", ondelete="SET NULL"),
    )
    title: Mapped[str | None] = mapped_column(Text)
    event_slug: Mapped[str | None] = mapped_column(String(512))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    actionable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_market_structure_groups_type_active", "group_type", "active"),
        Index("ix_market_structure_groups_event_dim_id", "event_dim_id"),
        Index("ix_market_structure_groups_event_slug", "event_slug"),
        Index("ix_market_structure_groups_actionable", "actionable"),
    )


class MarketStructureGroupMember(Base):
    __tablename__ = "market_structure_group_members"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("market_structure_groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    member_key: Mapped[str] = mapped_column(String(255), nullable=False)
    venue: Mapped[str] = mapped_column(String(64), nullable=False)
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
    market_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("markets.id", ondelete="SET NULL"),
    )
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    outcome_name: Mapped[str | None] = mapped_column(String(255))
    outcome_index: Mapped[int | None] = mapped_column(Integer)
    member_role: Mapped[str] = mapped_column(String(32), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    actionable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("group_id", "member_key", name="uq_market_structure_group_members_group_key"),
        Index("ix_market_structure_group_members_group_id", "group_id"),
        Index("ix_market_structure_group_members_asset_id", "asset_id"),
        Index("ix_market_structure_group_members_condition_id", "condition_id"),
        Index("ix_market_structure_group_members_outcome_id", "outcome_id"),
        Index("ix_market_structure_group_members_venue_role", "venue", "member_role"),
    )


class MarketStructureRun(Base):
    __tablename__ = "market_structure_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_type: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    scope_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    cursor_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    rows_inserted_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_market_structure_runs_started_at", "started_at"),
        Index("ix_market_structure_runs_run_reason_started", "run_type", "reason", "started_at"),
        Index("ix_market_structure_runs_status_started", "status", "started_at"),
    )


class MarketStructureOpportunity(Base):
    __tablename__ = "market_structure_opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("market_structure_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("market_structure_groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    opportunity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    anchor_condition_id: Mapped[str | None] = mapped_column(String(255))
    anchor_asset_id: Mapped[str | None] = mapped_column(String(255))
    event_ts_exchange: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at_local: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    pricing_method: Mapped[str] = mapped_column(String(32), nullable=False)
    gross_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    net_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    gross_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    package_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    executable_all_legs: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    actionable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    invalid_reason: Mapped[str | None] = mapped_column(Text)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_market_structure_opportunities_run_id", "run_id"),
        Index("ix_market_structure_opportunities_group_id", "group_id"),
        Index("ix_market_structure_opportunities_type_observed", "opportunity_type", "observed_at_local"),
        Index("ix_market_structure_opportunities_actionable_observed", "actionable", "observed_at_local"),
        Index("ix_market_structure_opportunities_anchor_condition", "anchor_condition_id"),
        Index("ix_market_structure_opportunities_anchor_asset", "anchor_asset_id"),
    )


class MarketStructureOpportunityLeg(Base):
    __tablename__ = "market_structure_opportunity_legs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="CASCADE"),
        nullable=False,
    )
    leg_index: Mapped[int] = mapped_column(Integer, nullable=False)
    venue: Mapped[str] = mapped_column(String(64), nullable=False)
    market_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("markets.id", ondelete="SET NULL"),
    )
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
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
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    action_type: Mapped[str | None] = mapped_column(String(32))
    order_type_hint: Mapped[str | None] = mapped_column(String(32))
    target_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    est_fillable_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    est_avg_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_worst_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_fee: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    est_slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    invalid_reason: Mapped[str | None] = mapped_column(Text)
    source_execution_candidate_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("polymarket_execution_action_candidates.id", ondelete="SET NULL"),
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
        UniqueConstraint("opportunity_id", "leg_index", name="uq_market_structure_opportunity_legs_index"),
        Index("ix_market_structure_opportunity_legs_opportunity_id", "opportunity_id"),
        Index("ix_market_structure_opportunity_legs_asset_id", "asset_id"),
        Index("ix_market_structure_opportunity_legs_condition_id", "condition_id"),
        Index("ix_market_structure_opportunity_legs_venue_role", "venue", "role"),
        Index("ix_market_structure_opportunity_legs_valid", "valid"),
    )


class CrossVenueMarketLink(Base):
    __tablename__ = "cross_venue_market_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    link_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    left_venue: Mapped[str] = mapped_column(String(64), nullable=False)
    left_market_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("markets.id", ondelete="SET NULL"),
    )
    left_outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    left_condition_id: Mapped[str | None] = mapped_column(String(255))
    left_asset_id: Mapped[str | None] = mapped_column(String(255))
    left_external_id: Mapped[str | None] = mapped_column(String(255))
    left_symbol: Mapped[str | None] = mapped_column(String(255))
    right_venue: Mapped[str] = mapped_column(String(64), nullable=False)
    right_market_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("markets.id", ondelete="SET NULL"),
    )
    right_outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    right_condition_id: Mapped[str | None] = mapped_column(String(255))
    right_asset_id: Mapped[str | None] = mapped_column(String(255))
    right_external_id: Mapped[str | None] = mapped_column(String(255))
    right_symbol: Mapped[str | None] = mapped_column(String(255))
    mapping_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    provenance_source: Mapped[str | None] = mapped_column(String(128))
    owner: Mapped[str | None] = mapped_column(String(128))
    reviewed_by: Mapped[str | None] = mapped_column(String(128))
    review_status: Mapped[str] = mapped_column(String(32), nullable=False, default="approved")
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    notes: Mapped[str | None] = mapped_column(Text)
    last_reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
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
        Index("ix_cross_venue_market_links_active", "active"),
        Index("ix_cross_venue_market_links_venues", "left_venue", "right_venue"),
        Index("ix_cross_venue_market_links_left_condition", "left_condition_id"),
        Index("ix_cross_venue_market_links_right_condition", "right_condition_id"),
        Index("ix_cross_venue_market_links_left_outcome", "left_outcome_id"),
        Index("ix_cross_venue_market_links_right_outcome", "right_outcome_id"),
        Index("ix_cross_venue_market_links_review_status", "review_status"),
        Index("ix_cross_venue_market_links_expires_at", "expires_at"),
        Index("ix_cross_venue_market_links_owner", "owner"),
    )


class MarketStructureValidation(Base):
    __tablename__ = "market_structure_validations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="CASCADE"),
        nullable=False,
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("market_structure_runs.id", ondelete="SET NULL"),
    )
    evaluation_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    classification: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_codes_json: Mapped[list[str] | dict | None] = mapped_column(JSONB)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    detected_gross_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    detected_net_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    detected_gross_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    detected_net_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    current_gross_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    current_net_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    current_gross_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    current_net_edge_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    gross_edge_decay_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_edge_decay_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    detected_age_seconds: Mapped[int | None] = mapped_column(Integer)
    max_leg_age_seconds: Mapped[int | None] = mapped_column(Integer)
    stale_leg_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executable_leg_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_leg_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    summary_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_market_structure_validations_opportunity_created", "opportunity_id", "created_at"),
        Index("ix_market_structure_validations_classification_created", "classification", "created_at"),
        Index("ix_market_structure_validations_kind_created", "evaluation_kind", "created_at"),
        Index("ix_market_structure_validations_run_id", "run_id"),
    )


class MarketStructurePaperPlan(Base):
    __tablename__ = "market_structure_paper_plans"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunities.id", ondelete="CASCADE"),
        nullable=False,
    )
    validation_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_validations.id", ondelete="SET NULL"),
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("market_structure_runs.id", ondelete="SET NULL"),
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    auto_created: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    manual_approval_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    approved_by: Mapped[str | None] = mapped_column(String(128))
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejected_by: Mapped[str | None] = mapped_column(String(128))
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    routed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    package_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    plan_notional_total: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    reason_codes_json: Mapped[list[str] | dict | None] = mapped_column(JSONB)
    summary_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        Index("ix_market_structure_paper_plans_opportunity_id", "opportunity_id"),
        Index("ix_market_structure_paper_plans_status_created", "status", "created_at"),
        Index("ix_market_structure_paper_plans_validation_id", "validation_id"),
        Index(
            "uq_market_structure_paper_plans_active_opportunity",
            "opportunity_id",
            unique=True,
            sqlite_where=text("status IN ('approval_pending','routing_pending','routed','partial_failed')"),
            postgresql_where=text("status IN ('approval_pending','routing_pending','routed','partial_failed')"),
        ),
    )


class MarketStructurePaperOrder(Base):
    __tablename__ = "market_structure_paper_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("market_structure_paper_plans.id", ondelete="CASCADE"),
        nullable=False,
    )
    opportunity_leg_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_opportunity_legs.id", ondelete="SET NULL"),
    )
    leg_index: Mapped[int] = mapped_column(Integer, nullable=False)
    venue: Mapped[str] = mapped_column(String(64), nullable=False)
    market_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("markets.id", ondelete="SET NULL"),
    )
    outcome_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("outcomes.id", ondelete="SET NULL"),
    )
    condition_id: Mapped[str | None] = mapped_column(String(255))
    asset_id: Mapped[str | None] = mapped_column(String(255))
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    action_type: Mapped[str | None] = mapped_column(String(32))
    order_type_hint: Mapped[str | None] = mapped_column(String(32))
    target_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    planned_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    planned_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    filled_size: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False, default=Decimal("0"))
    avg_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8))
    fill_notional: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_reason: Mapped[str | None] = mapped_column(Text)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    __table_args__ = (
        UniqueConstraint("plan_id", "leg_index", name="uq_market_structure_paper_orders_plan_leg"),
        Index("ix_market_structure_paper_orders_plan_id", "plan_id"),
        Index("ix_market_structure_paper_orders_status_created", "status", "created_at"),
    )


class MarketStructurePaperOrderEvent(Base):
    __tablename__ = "market_structure_paper_order_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("market_structure_paper_plans.id", ondelete="CASCADE"),
        nullable=False,
    )
    paper_order_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("market_structure_paper_orders.id", ondelete="CASCADE"),
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str | None] = mapped_column(String(32))
    message: Mapped[str | None] = mapped_column(Text)
    details_json: Mapped[dict | list | str | None] = mapped_column(JSONB)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    __table_args__ = (
        Index("ix_market_structure_paper_order_events_plan_observed", "plan_id", "observed_at"),
        Index("ix_market_structure_paper_order_events_order_observed", "paper_order_id", "observed_at"),
        Index("ix_market_structure_paper_order_events_type_observed", "event_type", "observed_at"),
    )
