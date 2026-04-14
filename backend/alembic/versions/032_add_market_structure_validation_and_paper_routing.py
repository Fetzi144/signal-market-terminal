"""Add Phase 8B market-structure validation, paper routing, and link governance.

Revision ID: 032
Revises: 031
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "032"
down_revision = "031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cross_venue_market_links", sa.Column("provenance_source", sa.String(length=128), nullable=True))
    op.add_column("cross_venue_market_links", sa.Column("owner", sa.String(length=128), nullable=True))
    op.add_column("cross_venue_market_links", sa.Column("reviewed_by", sa.String(length=128), nullable=True))
    op.add_column(
        "cross_venue_market_links",
        sa.Column("review_status", sa.String(length=32), nullable=False, server_default="approved"),
    )
    op.add_column("cross_venue_market_links", sa.Column("confidence", sa.Numeric(10, 6), nullable=True))
    op.add_column("cross_venue_market_links", sa.Column("notes", sa.Text(), nullable=True))
    op.add_column("cross_venue_market_links", sa.Column("last_reviewed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("cross_venue_market_links", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_cross_venue_market_links_review_status", "cross_venue_market_links", ["review_status"])
    op.create_index("ix_cross_venue_market_links_expires_at", "cross_venue_market_links", ["expires_at"])
    op.create_index("ix_cross_venue_market_links_owner", "cross_venue_market_links", ["owner"])

    op.create_table(
        "market_structure_validations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("evaluation_kind", sa.String(length=32), nullable=False),
        sa.Column("classification", sa.String(length=32), nullable=False),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("confidence", sa.Numeric(10, 6), nullable=True),
        sa.Column("detected_gross_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("detected_net_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("detected_gross_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("detected_net_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("current_gross_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("current_net_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("current_gross_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("current_net_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("gross_edge_decay_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("net_edge_decay_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("detected_age_seconds", sa.Integer(), nullable=True),
        sa.Column("max_leg_age_seconds", sa.Integer(), nullable=True),
        sa.Column("stale_leg_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("executable_leg_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_leg_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["market_structure_opportunities.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["run_id"], ["market_structure_runs.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_market_structure_validations_opportunity_created",
        "market_structure_validations",
        ["opportunity_id", "created_at"],
    )
    op.create_index(
        "ix_market_structure_validations_classification_created",
        "market_structure_validations",
        ["classification", "created_at"],
    )
    op.create_index(
        "ix_market_structure_validations_kind_created",
        "market_structure_validations",
        ["evaluation_kind", "created_at"],
    )
    op.create_index("ix_market_structure_validations_run_id", "market_structure_validations", ["run_id"])

    op.create_table(
        "market_structure_paper_plans",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("validation_id", sa.Integer(), nullable=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("auto_created", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("manual_approval_required", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("approved_by", sa.String(length=128), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejected_by", sa.String(length=128), nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejection_reason", sa.Text(), nullable=True),
        sa.Column("routed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("package_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("plan_notional_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["market_structure_opportunities.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["validation_id"], ["market_structure_validations.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["run_id"], ["market_structure_runs.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_market_structure_paper_plans_opportunity_id", "market_structure_paper_plans", ["opportunity_id"])
    op.create_index(
        "ix_market_structure_paper_plans_status_created",
        "market_structure_paper_plans",
        ["status", "created_at"],
    )
    op.create_index("ix_market_structure_paper_plans_validation_id", "market_structure_paper_plans", ["validation_id"])
    op.create_index(
        "uq_market_structure_paper_plans_active_opportunity",
        "market_structure_paper_plans",
        ["opportunity_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('approval_pending','routing_pending','routed','partial_failed')"),
    )

    op.create_table(
        "market_structure_paper_orders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("plan_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("opportunity_leg_id", sa.Integer(), nullable=True),
        sa.Column("leg_index", sa.Integer(), nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("market_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=True),
        sa.Column("order_type_hint", sa.String(length=32), nullable=True),
        sa.Column("target_size", sa.Numeric(24, 8), nullable=False),
        sa.Column("planned_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("planned_notional", sa.Numeric(24, 8), nullable=True),
        sa.Column("filled_size", sa.Numeric(24, 8), nullable=False, server_default="0"),
        sa.Column("avg_fill_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("fill_notional", sa.Numeric(24, 8), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_reason", sa.Text(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["plan_id"], ["market_structure_paper_plans.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["opportunity_leg_id"], ["market_structure_opportunity_legs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("plan_id", "leg_index", name="uq_market_structure_paper_orders_plan_leg"),
    )
    op.create_index("ix_market_structure_paper_orders_plan_id", "market_structure_paper_orders", ["plan_id"])
    op.create_index(
        "ix_market_structure_paper_orders_status_created",
        "market_structure_paper_orders",
        ["status", "created_at"],
    )

    op.create_table(
        "market_structure_paper_order_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("plan_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("paper_order_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["plan_id"], ["market_structure_paper_plans.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["paper_order_id"], ["market_structure_paper_orders.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_market_structure_paper_order_events_plan_observed",
        "market_structure_paper_order_events",
        ["plan_id", "observed_at"],
    )
    op.create_index(
        "ix_market_structure_paper_order_events_order_observed",
        "market_structure_paper_order_events",
        ["paper_order_id", "observed_at"],
    )
    op.create_index(
        "ix_market_structure_paper_order_events_type_observed",
        "market_structure_paper_order_events",
        ["event_type", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_market_structure_paper_order_events_type_observed", table_name="market_structure_paper_order_events")
    op.drop_index("ix_market_structure_paper_order_events_order_observed", table_name="market_structure_paper_order_events")
    op.drop_index("ix_market_structure_paper_order_events_plan_observed", table_name="market_structure_paper_order_events")
    op.drop_table("market_structure_paper_order_events")

    op.drop_index("ix_market_structure_paper_orders_status_created", table_name="market_structure_paper_orders")
    op.drop_index("ix_market_structure_paper_orders_plan_id", table_name="market_structure_paper_orders")
    op.drop_table("market_structure_paper_orders")

    op.drop_index("uq_market_structure_paper_plans_active_opportunity", table_name="market_structure_paper_plans")
    op.drop_index("ix_market_structure_paper_plans_validation_id", table_name="market_structure_paper_plans")
    op.drop_index("ix_market_structure_paper_plans_status_created", table_name="market_structure_paper_plans")
    op.drop_index("ix_market_structure_paper_plans_opportunity_id", table_name="market_structure_paper_plans")
    op.drop_table("market_structure_paper_plans")

    op.drop_index("ix_market_structure_validations_run_id", table_name="market_structure_validations")
    op.drop_index("ix_market_structure_validations_kind_created", table_name="market_structure_validations")
    op.drop_index("ix_market_structure_validations_classification_created", table_name="market_structure_validations")
    op.drop_index("ix_market_structure_validations_opportunity_created", table_name="market_structure_validations")
    op.drop_table("market_structure_validations")

    op.drop_index("ix_cross_venue_market_links_owner", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_expires_at", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_review_status", table_name="cross_venue_market_links")
    op.drop_column("cross_venue_market_links", "expires_at")
    op.drop_column("cross_venue_market_links", "last_reviewed_at")
    op.drop_column("cross_venue_market_links", "notes")
    op.drop_column("cross_venue_market_links", "confidence")
    op.drop_column("cross_venue_market_links", "review_status")
    op.drop_column("cross_venue_market_links", "reviewed_by")
    op.drop_column("cross_venue_market_links", "owner")
    op.drop_column("cross_venue_market_links", "provenance_source")
