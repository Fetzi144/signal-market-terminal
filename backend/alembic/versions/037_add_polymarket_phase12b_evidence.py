"""Add Phase 12B pilot evidence, guardrail, and lot-accounting tables.

Revision ID: 037
Revises: 036
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "position_lots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("strategy_family", sa.String(length=32), nullable=False),
        sa.Column("pilot_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("source_live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("source_fill_id", sa.Integer(), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open_size", sa.Numeric(24, 8), nullable=False),
        sa.Column("remaining_size", sa.Numeric(24, 8), nullable=False),
        sa.Column("avg_open_price", sa.Numeric(18, 8), nullable=False),
        sa.Column("avg_close_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("fee_paid", sa.Numeric(24, 8), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["pilot_run_id"], ["polymarket_pilot_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_fill_id"], ["live_fills.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_position_lots_strategy_status_opened",
        "position_lots",
        ["strategy_family", "status", "opened_at"],
    )
    op.create_index(
        "ix_position_lots_condition_asset_status",
        "position_lots",
        ["condition_id", "asset_id", "status"],
    )
    op.create_index("ix_position_lots_pilot_run", "position_lots", ["pilot_run_id"])
    op.create_index("ix_position_lots_source_live_order", "position_lots", ["source_live_order_id"])
    op.create_index("ix_position_lots_source_fill", "position_lots", ["source_fill_id"])

    op.create_table(
        "position_lot_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lot_id", sa.Integer(), nullable=False),
        sa.Column("live_fill_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("size_delta", sa.Numeric(24, 8), nullable=True),
        sa.Column("price", sa.Numeric(18, 8), nullable=True),
        sa.Column("fee_delta", sa.Numeric(24, 8), nullable=True),
        sa.Column("realized_pnl_delta", sa.Numeric(24, 8), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["lot_id"], ["position_lots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["live_fill_id"], ["live_fills.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_position_lot_events_lot_observed", "position_lot_events", ["lot_id", "observed_at_local"])
    op.create_index("ix_position_lot_events_live_fill", "position_lot_events", ["live_fill_id"])
    op.create_index("ix_position_lot_events_type_observed", "position_lot_events", ["event_type", "observed_at_local"])

    op.create_table(
        "polymarket_pilot_scorecards",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("strategy_family", sa.String(length=32), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("live_orders_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fills_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("approval_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("approval_expired_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejection_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("incident_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("gross_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("net_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("fees_paid", sa.Numeric(24, 8), nullable=True),
        sa.Column("avg_shadow_gap_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("worst_shadow_gap_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("coverage_limited_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("strategy_family", "window_start", "window_end", name="uq_pm_pilot_scorecards_window"),
    )
    op.create_index(
        "ix_pm_pilot_scorecards_strategy_window",
        "polymarket_pilot_scorecards",
        ["strategy_family", "window_start", "window_end"],
    )
    op.create_index(
        "ix_pm_pilot_scorecards_status_created",
        "polymarket_pilot_scorecards",
        ["status", "created_at"],
    )

    op.create_table(
        "polymarket_pilot_guardrail_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("strategy_family", sa.String(length=32), nullable=False),
        sa.Column("guardrail_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("pilot_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("trigger_value", sa.Numeric(24, 8), nullable=True),
        sa.Column("threshold_value", sa.Numeric(24, 8), nullable=True),
        sa.Column("action_taken", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pilot_run_id"], ["polymarket_pilot_runs.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_guardrail_events_strategy_observed",
        "polymarket_pilot_guardrail_events",
        ["strategy_family", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_guardrail_events_type_observed",
        "polymarket_pilot_guardrail_events",
        ["guardrail_type", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_guardrail_events_run_observed",
        "polymarket_pilot_guardrail_events",
        ["pilot_run_id", "observed_at_local"],
    )

    op.create_table(
        "polymarket_pilot_readiness_reports",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("strategy_family", sa.String(length=32), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("scorecard_id", sa.Integer(), nullable=True),
        sa.Column("open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("approval_backlog_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("coverage_limited_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("shadow_gap_breach_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["scorecard_id"], ["polymarket_pilot_scorecards.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("strategy_family", "window_start", "window_end", name="uq_pm_pilot_readiness_window"),
    )
    op.create_index(
        "ix_pm_readiness_reports_strategy_generated",
        "polymarket_pilot_readiness_reports",
        ["strategy_family", "generated_at"],
    )
    op.create_index(
        "ix_pm_readiness_reports_status_generated",
        "polymarket_pilot_readiness_reports",
        ["status", "generated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_readiness_reports_status_generated", table_name="polymarket_pilot_readiness_reports")
    op.drop_index("ix_pm_readiness_reports_strategy_generated", table_name="polymarket_pilot_readiness_reports")
    op.drop_table("polymarket_pilot_readiness_reports")

    op.drop_index("ix_pm_guardrail_events_run_observed", table_name="polymarket_pilot_guardrail_events")
    op.drop_index("ix_pm_guardrail_events_type_observed", table_name="polymarket_pilot_guardrail_events")
    op.drop_index("ix_pm_guardrail_events_strategy_observed", table_name="polymarket_pilot_guardrail_events")
    op.drop_table("polymarket_pilot_guardrail_events")

    op.drop_index("ix_pm_pilot_scorecards_status_created", table_name="polymarket_pilot_scorecards")
    op.drop_index("ix_pm_pilot_scorecards_strategy_window", table_name="polymarket_pilot_scorecards")
    op.drop_table("polymarket_pilot_scorecards")

    op.drop_index("ix_position_lot_events_type_observed", table_name="position_lot_events")
    op.drop_index("ix_position_lot_events_live_fill", table_name="position_lot_events")
    op.drop_index("ix_position_lot_events_lot_observed", table_name="position_lot_events")
    op.drop_table("position_lot_events")

    op.drop_index("ix_position_lots_pilot_run", table_name="position_lots")
    op.drop_index("ix_position_lots_source_fill", table_name="position_lots")
    op.drop_index("ix_position_lots_source_live_order", table_name="position_lots")
    op.drop_index("ix_position_lots_condition_asset_status", table_name="position_lots")
    op.drop_index("ix_position_lots_strategy_status_opened", table_name="position_lots")
    op.drop_table("position_lots")
