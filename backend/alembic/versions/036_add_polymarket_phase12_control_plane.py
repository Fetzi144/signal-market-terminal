"""Add Phase 12 Polymarket pilot control-plane tables and fields.

Revision ID: 036
Revises: 035
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_pilot_configs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("pilot_name", sa.String(length=128), nullable=False),
        sa.Column("strategy_family", sa.String(length=32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("armed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("manual_approval_required", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("live_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("market_allowlist_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("category_allowlist_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("max_notional_per_order_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("max_notional_per_day_usd", sa.Numeric(24, 8), nullable=True),
        sa.Column("max_open_orders", sa.Integer(), nullable=True),
        sa.Column("max_plan_age_seconds", sa.Integer(), nullable=True),
        sa.Column("max_decision_age_seconds", sa.Integer(), nullable=True),
        sa.Column("max_slippage_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("require_complete_replay_coverage", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("pilot_name", name="uq_pm_pilot_configs_pilot_name"),
    )
    op.create_index(
        "ix_pm_pilot_configs_family_active",
        "polymarket_pilot_configs",
        ["strategy_family", "active"],
    )
    op.create_index(
        "ix_pm_pilot_configs_active_updated",
        "polymarket_pilot_configs",
        ["active", "updated_at"],
    )

    op.create_table(
        "polymarket_pilot_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("pilot_config_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["pilot_config_id"], ["polymarket_pilot_configs.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_pm_pilot_runs_config_started", "polymarket_pilot_runs", ["pilot_config_id", "started_at"])
    op.create_index("ix_pm_pilot_runs_status_started", "polymarket_pilot_runs", ["status", "started_at"])

    op.create_table(
        "polymarket_pilot_approval_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("execution_decision_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("pilot_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("operator_identity", sa.String(length=128), nullable=True),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["execution_decision_id"], ["execution_decisions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pilot_run_id"], ["polymarket_pilot_runs.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_pilot_approval_events_run_observed",
        "polymarket_pilot_approval_events",
        ["pilot_run_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_pilot_approval_events_live_order_observed",
        "polymarket_pilot_approval_events",
        ["live_order_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_pilot_approval_events_action_observed",
        "polymarket_pilot_approval_events",
        ["action", "observed_at_local"],
    )

    op.create_table(
        "polymarket_control_plane_incidents",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("pilot_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("incident_type", sa.String(length=64), nullable=False),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["pilot_run_id"], ["polymarket_pilot_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_control_incidents_run_observed",
        "polymarket_control_plane_incidents",
        ["pilot_run_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_control_incidents_type_observed",
        "polymarket_control_plane_incidents",
        ["incident_type", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_control_incidents_severity_observed",
        "polymarket_control_plane_incidents",
        ["severity", "observed_at_local"],
    )

    op.create_table(
        "polymarket_live_shadow_evaluations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("execution_decision_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("replay_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("variant_name", sa.String(length=64), nullable=False),
        sa.Column("expected_fill_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("actual_fill_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("expected_fill_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("actual_fill_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("expected_net_ev_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("realized_net_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("gap_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("coverage_limited", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["execution_decision_id"], ["execution_decisions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["replay_run_id"], ["polymarket_replay_runs.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_pm_live_shadow_eval_live_order", "polymarket_live_shadow_evaluations", ["live_order_id"])
    op.create_index(
        "ix_pm_live_shadow_eval_variant_created",
        "polymarket_live_shadow_evaluations",
        ["variant_name", "created_at"],
    )
    op.create_index("ix_pm_live_shadow_eval_replay_run", "polymarket_live_shadow_evaluations", ["replay_run_id"])

    op.add_column("live_orders", sa.Column("strategy_family", sa.String(length=32), nullable=True))
    op.add_column("live_orders", sa.Column("pilot_config_id", sa.Integer(), nullable=True))
    op.add_column("live_orders", sa.Column("pilot_run_id", sa.Uuid(as_uuid=True), nullable=True))
    op.add_column(
        "live_orders",
        sa.Column("approval_state", sa.String(length=32), nullable=False, server_default="not_required"),
    )
    op.add_column("live_orders", sa.Column("approval_requested_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("live_orders", sa.Column("approval_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("live_orders", sa.Column("blocked_reason_code", sa.String(length=128), nullable=True))
    op.create_foreign_key(
        "fk_live_orders_pilot_config_id",
        "live_orders",
        "polymarket_pilot_configs",
        ["pilot_config_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_live_orders_pilot_run_id",
        "live_orders",
        "polymarket_pilot_runs",
        ["pilot_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_live_orders_strategy_status", "live_orders", ["strategy_family", "status"])
    op.create_index("ix_live_orders_approval_state_created", "live_orders", ["approval_state", "created_at"])

    op.add_column("polymarket_live_state", sa.Column("heartbeat_healthy", sa.Boolean(), nullable=True))
    op.add_column("polymarket_live_state", sa.Column("heartbeat_last_checked_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("polymarket_live_state", sa.Column("heartbeat_last_success_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("polymarket_live_state", sa.Column("heartbeat_last_error", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("polymarket_live_state", "heartbeat_last_error")
    op.drop_column("polymarket_live_state", "heartbeat_last_success_at")
    op.drop_column("polymarket_live_state", "heartbeat_last_checked_at")
    op.drop_column("polymarket_live_state", "heartbeat_healthy")

    op.drop_index("ix_live_orders_approval_state_created", table_name="live_orders")
    op.drop_index("ix_live_orders_strategy_status", table_name="live_orders")
    op.drop_constraint("fk_live_orders_pilot_run_id", "live_orders", type_="foreignkey")
    op.drop_constraint("fk_live_orders_pilot_config_id", "live_orders", type_="foreignkey")
    op.drop_column("live_orders", "blocked_reason_code")
    op.drop_column("live_orders", "approval_expires_at")
    op.drop_column("live_orders", "approval_requested_at")
    op.drop_column("live_orders", "approval_state")
    op.drop_column("live_orders", "pilot_run_id")
    op.drop_column("live_orders", "pilot_config_id")
    op.drop_column("live_orders", "strategy_family")

    op.drop_index("ix_pm_live_shadow_eval_replay_run", table_name="polymarket_live_shadow_evaluations")
    op.drop_index("ix_pm_live_shadow_eval_variant_created", table_name="polymarket_live_shadow_evaluations")
    op.drop_index("ix_pm_live_shadow_eval_live_order", table_name="polymarket_live_shadow_evaluations")
    op.drop_table("polymarket_live_shadow_evaluations")

    op.drop_index("ix_pm_control_incidents_severity_observed", table_name="polymarket_control_plane_incidents")
    op.drop_index("ix_pm_control_incidents_type_observed", table_name="polymarket_control_plane_incidents")
    op.drop_index("ix_pm_control_incidents_run_observed", table_name="polymarket_control_plane_incidents")
    op.drop_table("polymarket_control_plane_incidents")

    op.drop_index("ix_pm_pilot_approval_events_action_observed", table_name="polymarket_pilot_approval_events")
    op.drop_index("ix_pm_pilot_approval_events_live_order_observed", table_name="polymarket_pilot_approval_events")
    op.drop_index("ix_pm_pilot_approval_events_run_observed", table_name="polymarket_pilot_approval_events")
    op.drop_table("polymarket_pilot_approval_events")

    op.drop_index("ix_pm_pilot_runs_status_started", table_name="polymarket_pilot_runs")
    op.drop_index("ix_pm_pilot_runs_config_started", table_name="polymarket_pilot_runs")
    op.drop_table("polymarket_pilot_runs")

    op.drop_index("ix_pm_pilot_configs_active_updated", table_name="polymarket_pilot_configs")
    op.drop_index("ix_pm_pilot_configs_family_active", table_name="polymarket_pilot_configs")
    op.drop_table("polymarket_pilot_configs")
