"""Add Phase 10 risk graph, exposure snapshots, and optimizer artifacts.

Revision ID: 034
Revises: 033
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "risk_graph_nodes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("node_key", sa.String(length=255), nullable=False),
        sa.Column("node_type", sa.String(length=64), nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=True),
        sa.Column("event_dim_id", sa.Integer(), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["event_dim_id"], ["polymarket_event_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("node_key", name="uq_risk_graph_nodes_node_key"),
    )
    op.create_index("ix_risk_graph_nodes_type_active", "risk_graph_nodes", ["node_type", "active"])
    op.create_index("ix_risk_graph_nodes_venue_type", "risk_graph_nodes", ["venue", "node_type"])
    op.create_index("ix_risk_graph_nodes_condition_id", "risk_graph_nodes", ["condition_id"])
    op.create_index("ix_risk_graph_nodes_asset_id", "risk_graph_nodes", ["asset_id"])
    op.create_index("ix_risk_graph_nodes_event_dim_id", "risk_graph_nodes", ["event_dim_id"])
    op.create_index("ix_risk_graph_nodes_market_dim_id", "risk_graph_nodes", ["market_dim_id"])
    op.create_index("ix_risk_graph_nodes_asset_dim_id", "risk_graph_nodes", ["asset_dim_id"])

    op.create_table(
        "risk_graph_edges",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("left_node_id", sa.Integer(), nullable=False),
        sa.Column("right_node_id", sa.Integer(), nullable=False),
        sa.Column("edge_type", sa.String(length=64), nullable=False),
        sa.Column("weight", sa.Numeric(18, 8), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["left_node_id"], ["risk_graph_nodes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["right_node_id"], ["risk_graph_nodes.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "left_node_id",
            "right_node_id",
            "edge_type",
            "source_kind",
            name="uq_risk_graph_edges_pair_type_source",
        ),
    )
    op.create_index("ix_risk_graph_edges_type_active", "risk_graph_edges", ["edge_type", "active"])
    op.create_index("ix_risk_graph_edges_left_node_id", "risk_graph_edges", ["left_node_id"])
    op.create_index("ix_risk_graph_edges_right_node_id", "risk_graph_edges", ["right_node_id"])
    op.create_index("ix_risk_graph_edges_source_kind", "risk_graph_edges", ["source_kind"])

    op.create_table(
        "risk_graph_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("run_type", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="running"),
        sa.Column("scope_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_risk_graph_runs_started_at", "risk_graph_runs", ["started_at"])
    op.create_index("ix_risk_graph_runs_type_reason_started", "risk_graph_runs", ["run_type", "reason", "started_at"])
    op.create_index("ix_risk_graph_runs_status_started", "risk_graph_runs", ["status", "started_at"])

    op.create_table(
        "portfolio_exposure_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.Column("exposure_kind", sa.String(length=32), nullable=False),
        sa.Column("gross_notional_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("net_notional_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("buy_notional_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("sell_notional_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("share_exposure", sa.Numeric(24, 8), nullable=True),
        sa.Column("reservation_cost_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("hedged_fraction", sa.Numeric(10, 6), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["risk_graph_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["node_id"], ["risk_graph_nodes.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_portfolio_exposure_snapshots_run_id", "portfolio_exposure_snapshots", ["run_id"])
    op.create_index("ix_portfolio_exposure_snapshots_snapshot_at", "portfolio_exposure_snapshots", ["snapshot_at"])
    op.create_index("ix_portfolio_exposure_snapshots_node_kind", "portfolio_exposure_snapshots", ["node_id", "exposure_kind"])
    op.create_index(
        "ix_portfolio_exposure_snapshots_kind_snapshot",
        "portfolio_exposure_snapshots",
        ["exposure_kind", "snapshot_at"],
    )

    op.create_table(
        "portfolio_optimizer_recommendations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=True),
        sa.Column("recommendation_type", sa.String(length=32), nullable=False),
        sa.Column("scope_kind", sa.String(length=32), nullable=False),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("target_size_cap_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("inventory_penalty_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("reservation_price_adjustment_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("maker_budget_remaining_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_budget_remaining_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["risk_graph_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["node_id"], ["risk_graph_nodes.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_portfolio_optimizer_recommendations_run_id", "portfolio_optimizer_recommendations", ["run_id"])
    op.create_index(
        "ix_portfolio_optimizer_recommendations_type_reason_observed",
        "portfolio_optimizer_recommendations",
        ["recommendation_type", "reason_code", "observed_at_local"],
    )
    op.create_index(
        "ix_portfolio_optimizer_recommendations_condition_observed",
        "portfolio_optimizer_recommendations",
        ["condition_id", "observed_at_local"],
    )
    op.create_index(
        "ix_portfolio_optimizer_recommendations_asset_observed",
        "portfolio_optimizer_recommendations",
        ["asset_id", "observed_at_local"],
    )
    op.create_index(
        "ix_portfolio_optimizer_recommendations_scope_kind",
        "portfolio_optimizer_recommendations",
        ["scope_kind"],
    )

    op.create_table(
        "inventory_control_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("control_scope", sa.String(length=32), nullable=False),
        sa.Column("maker_budget_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_budget_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_budget_used_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_budget_used_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("reservation_price_shift_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("quote_skew_direction", sa.String(length=32), nullable=True),
        sa.Column("no_quote", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_inventory_control_snapshots_snapshot_at", "inventory_control_snapshots", ["snapshot_at"])
    op.create_index(
        "ix_inventory_control_snapshots_scope_reason",
        "inventory_control_snapshots",
        ["control_scope", "reason_code"],
    )
    op.create_index(
        "ix_inventory_control_snapshots_condition_snapshot",
        "inventory_control_snapshots",
        ["condition_id", "snapshot_at"],
    )
    op.create_index(
        "ix_inventory_control_snapshots_asset_snapshot",
        "inventory_control_snapshots",
        ["asset_id", "snapshot_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_inventory_control_snapshots_asset_snapshot", table_name="inventory_control_snapshots")
    op.drop_index("ix_inventory_control_snapshots_condition_snapshot", table_name="inventory_control_snapshots")
    op.drop_index("ix_inventory_control_snapshots_scope_reason", table_name="inventory_control_snapshots")
    op.drop_index("ix_inventory_control_snapshots_snapshot_at", table_name="inventory_control_snapshots")
    op.drop_table("inventory_control_snapshots")

    op.drop_index(
        "ix_portfolio_optimizer_recommendations_scope_kind",
        table_name="portfolio_optimizer_recommendations",
    )
    op.drop_index(
        "ix_portfolio_optimizer_recommendations_asset_observed",
        table_name="portfolio_optimizer_recommendations",
    )
    op.drop_index(
        "ix_portfolio_optimizer_recommendations_condition_observed",
        table_name="portfolio_optimizer_recommendations",
    )
    op.drop_index(
        "ix_portfolio_optimizer_recommendations_type_reason_observed",
        table_name="portfolio_optimizer_recommendations",
    )
    op.drop_index("ix_portfolio_optimizer_recommendations_run_id", table_name="portfolio_optimizer_recommendations")
    op.drop_table("portfolio_optimizer_recommendations")

    op.drop_index("ix_portfolio_exposure_snapshots_kind_snapshot", table_name="portfolio_exposure_snapshots")
    op.drop_index("ix_portfolio_exposure_snapshots_node_kind", table_name="portfolio_exposure_snapshots")
    op.drop_index("ix_portfolio_exposure_snapshots_snapshot_at", table_name="portfolio_exposure_snapshots")
    op.drop_index("ix_portfolio_exposure_snapshots_run_id", table_name="portfolio_exposure_snapshots")
    op.drop_table("portfolio_exposure_snapshots")

    op.drop_index("ix_risk_graph_runs_status_started", table_name="risk_graph_runs")
    op.drop_index("ix_risk_graph_runs_type_reason_started", table_name="risk_graph_runs")
    op.drop_index("ix_risk_graph_runs_started_at", table_name="risk_graph_runs")
    op.drop_table("risk_graph_runs")

    op.drop_index("ix_risk_graph_edges_source_kind", table_name="risk_graph_edges")
    op.drop_index("ix_risk_graph_edges_right_node_id", table_name="risk_graph_edges")
    op.drop_index("ix_risk_graph_edges_left_node_id", table_name="risk_graph_edges")
    op.drop_index("ix_risk_graph_edges_type_active", table_name="risk_graph_edges")
    op.drop_table("risk_graph_edges")

    op.drop_index("ix_risk_graph_nodes_asset_dim_id", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_market_dim_id", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_event_dim_id", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_asset_id", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_condition_id", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_venue_type", table_name="risk_graph_nodes")
    op.drop_index("ix_risk_graph_nodes_type_active", table_name="risk_graph_nodes")
    op.drop_table("risk_graph_nodes")
