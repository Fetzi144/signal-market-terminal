"""Add Phase 8A market structure engine tables.

Revision ID: 031
Revises: 030
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "031"
down_revision = "030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_structure_groups",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("group_key", sa.String(length=255), nullable=False),
        sa.Column("group_type", sa.String(length=64), nullable=False),
        sa.Column("primary_venue", sa.String(length=64), nullable=True),
        sa.Column("event_dim_id", sa.Integer(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("event_slug", sa.String(length=512), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("actionable", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["event_dim_id"], ["polymarket_event_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("group_key", name="uq_market_structure_groups_group_key"),
    )
    op.create_index("ix_market_structure_groups_type_active", "market_structure_groups", ["group_type", "active"])
    op.create_index("ix_market_structure_groups_event_dim_id", "market_structure_groups", ["event_dim_id"])
    op.create_index("ix_market_structure_groups_event_slug", "market_structure_groups", ["event_slug"])
    op.create_index("ix_market_structure_groups_actionable", "market_structure_groups", ["actionable"])

    op.create_table(
        "market_structure_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("run_type", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="running"),
        sa.Column("scope_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("cursor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_market_structure_runs_started_at", "market_structure_runs", ["started_at"])
    op.create_index(
        "ix_market_structure_runs_run_reason_started",
        "market_structure_runs",
        ["run_type", "reason", "started_at"],
    )
    op.create_index("ix_market_structure_runs_status_started", "market_structure_runs", ["status", "started_at"])

    op.create_table(
        "cross_venue_market_links",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("link_key", sa.String(length=255), nullable=False),
        sa.Column("left_venue", sa.String(length=64), nullable=False),
        sa.Column("left_market_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("left_outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("left_condition_id", sa.String(length=255), nullable=True),
        sa.Column("left_asset_id", sa.String(length=255), nullable=True),
        sa.Column("left_external_id", sa.String(length=255), nullable=True),
        sa.Column("left_symbol", sa.String(length=255), nullable=True),
        sa.Column("right_venue", sa.String(length=64), nullable=False),
        sa.Column("right_market_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("right_outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("right_condition_id", sa.String(length=255), nullable=True),
        sa.Column("right_asset_id", sa.String(length=255), nullable=True),
        sa.Column("right_external_id", sa.String(length=255), nullable=True),
        sa.Column("right_symbol", sa.String(length=255), nullable=True),
        sa.Column("mapping_kind", sa.String(length=32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["left_market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["left_outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["right_market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["right_outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("link_key", name="uq_cross_venue_market_links_link_key"),
    )
    op.create_index("ix_cross_venue_market_links_active", "cross_venue_market_links", ["active"])
    op.create_index("ix_cross_venue_market_links_venues", "cross_venue_market_links", ["left_venue", "right_venue"])
    op.create_index("ix_cross_venue_market_links_left_condition", "cross_venue_market_links", ["left_condition_id"])
    op.create_index("ix_cross_venue_market_links_right_condition", "cross_venue_market_links", ["right_condition_id"])
    op.create_index("ix_cross_venue_market_links_left_outcome", "cross_venue_market_links", ["left_outcome_id"])
    op.create_index("ix_cross_venue_market_links_right_outcome", "cross_venue_market_links", ["right_outcome_id"])

    op.create_table(
        "market_structure_group_members",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("member_key", sa.String(length=255), nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("event_dim_id", sa.Integer(), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("market_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("outcome_name", sa.String(length=255), nullable=True),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("member_role", sa.String(length=32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("actionable", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["group_id"], ["market_structure_groups.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["event_dim_id"], ["polymarket_event_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("group_id", "member_key", name="uq_market_structure_group_members_group_key"),
    )
    op.create_index("ix_market_structure_group_members_group_id", "market_structure_group_members", ["group_id"])
    op.create_index("ix_market_structure_group_members_asset_id", "market_structure_group_members", ["asset_id"])
    op.create_index("ix_market_structure_group_members_condition_id", "market_structure_group_members", ["condition_id"])
    op.create_index("ix_market_structure_group_members_outcome_id", "market_structure_group_members", ["outcome_id"])
    op.create_index("ix_market_structure_group_members_venue_role", "market_structure_group_members", ["venue", "member_role"])

    op.create_table(
        "market_structure_opportunities",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("opportunity_type", sa.String(length=64), nullable=False),
        sa.Column("anchor_condition_id", sa.String(length=255), nullable=True),
        sa.Column("anchor_asset_id", sa.String(length=255), nullable=True),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("pricing_method", sa.String(length=32), nullable=False),
        sa.Column("gross_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("net_edge_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("gross_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("net_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("package_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("executable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("executable_all_legs", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("actionable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("confidence", sa.Numeric(10, 6), nullable=True),
        sa.Column("invalid_reason", sa.Text(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["market_structure_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], ["market_structure_groups.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_market_structure_opportunities_run_id", "market_structure_opportunities", ["run_id"])
    op.create_index("ix_market_structure_opportunities_group_id", "market_structure_opportunities", ["group_id"])
    op.create_index(
        "ix_market_structure_opportunities_type_observed",
        "market_structure_opportunities",
        ["opportunity_type", "observed_at_local"],
    )
    op.create_index(
        "ix_market_structure_opportunities_actionable_observed",
        "market_structure_opportunities",
        ["actionable", "observed_at_local"],
    )
    op.create_index(
        "ix_market_structure_opportunities_anchor_condition",
        "market_structure_opportunities",
        ["anchor_condition_id"],
    )
    op.create_index(
        "ix_market_structure_opportunities_anchor_asset",
        "market_structure_opportunities",
        ["anchor_asset_id"],
    )

    op.create_table(
        "market_structure_opportunity_legs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("leg_index", sa.Integer(), nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("market_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=True),
        sa.Column("order_type_hint", sa.String(length=32), nullable=True),
        sa.Column("target_size", sa.Numeric(24, 8), nullable=False),
        sa.Column("est_fillable_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("est_avg_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_worst_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_slippage_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("valid", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("invalid_reason", sa.Text(), nullable=True),
        sa.Column("source_execution_candidate_id", sa.Integer(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["market_structure_opportunities.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["source_execution_candidate_id"],
            ["polymarket_execution_action_candidates.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("opportunity_id", "leg_index", name="uq_market_structure_opportunity_legs_index"),
    )
    op.create_index(
        "ix_market_structure_opportunity_legs_opportunity_id",
        "market_structure_opportunity_legs",
        ["opportunity_id"],
    )
    op.create_index("ix_market_structure_opportunity_legs_asset_id", "market_structure_opportunity_legs", ["asset_id"])
    op.create_index(
        "ix_market_structure_opportunity_legs_condition_id",
        "market_structure_opportunity_legs",
        ["condition_id"],
    )
    op.create_index(
        "ix_market_structure_opportunity_legs_venue_role",
        "market_structure_opportunity_legs",
        ["venue", "role"],
    )
    op.create_index("ix_market_structure_opportunity_legs_valid", "market_structure_opportunity_legs", ["valid"])


def downgrade() -> None:
    op.drop_index("ix_market_structure_opportunity_legs_valid", table_name="market_structure_opportunity_legs")
    op.drop_index("ix_market_structure_opportunity_legs_venue_role", table_name="market_structure_opportunity_legs")
    op.drop_index("ix_market_structure_opportunity_legs_condition_id", table_name="market_structure_opportunity_legs")
    op.drop_index("ix_market_structure_opportunity_legs_asset_id", table_name="market_structure_opportunity_legs")
    op.drop_index("ix_market_structure_opportunity_legs_opportunity_id", table_name="market_structure_opportunity_legs")
    op.drop_table("market_structure_opportunity_legs")

    op.drop_index("ix_market_structure_opportunities_anchor_asset", table_name="market_structure_opportunities")
    op.drop_index("ix_market_structure_opportunities_anchor_condition", table_name="market_structure_opportunities")
    op.drop_index("ix_market_structure_opportunities_actionable_observed", table_name="market_structure_opportunities")
    op.drop_index("ix_market_structure_opportunities_type_observed", table_name="market_structure_opportunities")
    op.drop_index("ix_market_structure_opportunities_group_id", table_name="market_structure_opportunities")
    op.drop_index("ix_market_structure_opportunities_run_id", table_name="market_structure_opportunities")
    op.drop_table("market_structure_opportunities")

    op.drop_index("ix_market_structure_group_members_venue_role", table_name="market_structure_group_members")
    op.drop_index("ix_market_structure_group_members_outcome_id", table_name="market_structure_group_members")
    op.drop_index("ix_market_structure_group_members_condition_id", table_name="market_structure_group_members")
    op.drop_index("ix_market_structure_group_members_asset_id", table_name="market_structure_group_members")
    op.drop_index("ix_market_structure_group_members_group_id", table_name="market_structure_group_members")
    op.drop_table("market_structure_group_members")

    op.drop_index("ix_cross_venue_market_links_right_outcome", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_left_outcome", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_right_condition", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_left_condition", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_venues", table_name="cross_venue_market_links")
    op.drop_index("ix_cross_venue_market_links_active", table_name="cross_venue_market_links")
    op.drop_table("cross_venue_market_links")

    op.drop_index("ix_market_structure_runs_status_started", table_name="market_structure_runs")
    op.drop_index("ix_market_structure_runs_run_reason_started", table_name="market_structure_runs")
    op.drop_index("ix_market_structure_runs_started_at", table_name="market_structure_runs")
    op.drop_table("market_structure_runs")

    op.drop_index("ix_market_structure_groups_actionable", table_name="market_structure_groups")
    op.drop_index("ix_market_structure_groups_event_slug", table_name="market_structure_groups")
    op.drop_index("ix_market_structure_groups_event_dim_id", table_name="market_structure_groups")
    op.drop_index("ix_market_structure_groups_type_active", table_name="market_structure_groups")
    op.drop_table("market_structure_groups")
