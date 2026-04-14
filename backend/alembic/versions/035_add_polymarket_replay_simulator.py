"""Add Phase 11 Polymarket replay simulator and backtest artifacts.

Revision ID: 035
Revises: 034
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_replay_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("run_key", sa.String(length=128), nullable=False),
        sa.Column("run_type", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("scenario_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("time_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("time_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("config_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.UniqueConstraint("run_key", name="uq_pm_replay_runs_run_key"),
    )
    op.create_index(
        "ix_pm_replay_runs_type_reason_started",
        "polymarket_replay_runs",
        ["run_type", "reason", "started_at"],
    )
    op.create_index(
        "ix_pm_replay_runs_status_started",
        "polymarket_replay_runs",
        ["status", "started_at"],
    )
    op.create_index(
        "ix_pm_replay_runs_window",
        "polymarket_replay_runs",
        ["time_window_start", "time_window_end"],
    )

    op.create_table(
        "polymarket_replay_scenarios",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("scenario_key", sa.String(length=255), nullable=False),
        sa.Column("scenario_type", sa.String(length=64), nullable=False),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("policy_version", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["polymarket_replay_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], ["market_structure_groups.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("scenario_key", name="uq_pm_replay_scenarios_scenario_key"),
    )
    op.create_index("ix_pm_replay_scenarios_run_id", "polymarket_replay_scenarios", ["run_id"])
    op.create_index(
        "ix_pm_replay_scenarios_type_window",
        "polymarket_replay_scenarios",
        ["scenario_type", "window_start"],
    )
    op.create_index(
        "ix_pm_replay_scenarios_condition_window",
        "polymarket_replay_scenarios",
        ["condition_id", "window_start"],
    )
    op.create_index(
        "ix_pm_replay_scenarios_asset_window",
        "polymarket_replay_scenarios",
        ["asset_id", "window_start"],
    )
    op.create_index(
        "ix_pm_replay_scenarios_status_updated",
        "polymarket_replay_scenarios",
        ["status", "updated_at"],
    )

    op.create_table(
        "polymarket_replay_orders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("variant_name", sa.String(length=64), nullable=False),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("action_type", sa.String(length=32), nullable=True),
        sa.Column("order_type_hint", sa.String(length=32), nullable=True),
        sa.Column("limit_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("requested_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("submitted_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("decision_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expiry_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_execution_decision_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("source_execution_candidate_id", sa.Integer(), nullable=True),
        sa.Column("source_structure_opportunity_id", sa.Integer(), nullable=True),
        sa.Column("source_quote_recommendation_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("source_optimizer_recommendation_id", sa.Integer(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["scenario_id"], ["polymarket_replay_scenarios.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_execution_decision_id"], ["execution_decisions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["source_execution_candidate_id"],
            ["polymarket_execution_action_candidates.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_structure_opportunity_id"],
            ["market_structure_opportunities.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_quote_recommendation_id"],
            ["polymarket_quote_recommendations.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_optimizer_recommendation_id"],
            ["portfolio_optimizer_recommendations.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "scenario_id",
            "variant_name",
            "sequence_no",
            name="uq_pm_replay_orders_scenario_variant_sequence",
        ),
    )
    op.create_index(
        "ix_pm_replay_orders_scenario_variant",
        "polymarket_replay_orders",
        ["scenario_id", "variant_name"],
    )
    op.create_index(
        "ix_pm_replay_orders_status_decision_ts",
        "polymarket_replay_orders",
        ["status", "decision_ts"],
    )
    op.create_index(
        "ix_pm_replay_orders_source_execution_decision",
        "polymarket_replay_orders",
        ["source_execution_decision_id"],
    )
    op.create_index(
        "ix_pm_replay_orders_source_quote_recommendation",
        "polymarket_replay_orders",
        ["source_quote_recommendation_id"],
    )

    op.create_table(
        "polymarket_replay_fills",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("replay_order_id", sa.Integer(), nullable=False),
        sa.Column("variant_name", sa.String(length=64), nullable=False),
        sa.Column("fill_index", sa.Integer(), nullable=False),
        sa.Column("fill_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("size", sa.Numeric(24, 8), nullable=False),
        sa.Column("fee_paid", sa.Numeric(24, 8), nullable=True),
        sa.Column("reward_estimate", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_taker", sa.String(length=16), nullable=True),
        sa.Column("fill_source_kind", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["scenario_id"], ["polymarket_replay_scenarios.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["replay_order_id"], ["polymarket_replay_orders.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("replay_order_id", "fill_index", name="uq_pm_replay_fills_order_fill_index"),
    )
    op.create_index(
        "ix_pm_replay_fills_scenario_variant",
        "polymarket_replay_fills",
        ["scenario_id", "variant_name"],
    )
    op.create_index(
        "ix_pm_replay_fills_source_kind_fill_ts",
        "polymarket_replay_fills",
        ["fill_source_kind", "fill_ts"],
    )

    op.create_table(
        "polymarket_replay_metrics",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("scenario_id", sa.Integer(), nullable=True),
        sa.Column("metric_scope", sa.String(length=32), nullable=False),
        sa.Column("variant_name", sa.String(length=64), nullable=False),
        sa.Column("gross_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("net_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("fees_paid", sa.Numeric(24, 8), nullable=True),
        sa.Column("rewards_estimated", sa.Numeric(24, 8), nullable=True),
        sa.Column("slippage_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("fill_rate", sa.Numeric(10, 6), nullable=True),
        sa.Column("cancel_rate", sa.Numeric(10, 6), nullable=True),
        sa.Column("action_mix_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("drawdown_proxy", sa.Numeric(24, 8), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["run_id"], ["polymarket_replay_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["scenario_id"], ["polymarket_replay_scenarios.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_pm_replay_metrics_run_variant",
        "polymarket_replay_metrics",
        ["run_id", "variant_name"],
    )
    op.create_index(
        "ix_pm_replay_metrics_scenario_variant",
        "polymarket_replay_metrics",
        ["scenario_id", "variant_name"],
    )
    op.create_index(
        "ix_pm_replay_metrics_scope_variant",
        "polymarket_replay_metrics",
        ["metric_scope", "variant_name"],
    )

    op.create_table(
        "polymarket_replay_decision_traces",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("replay_order_id", sa.Integer(), nullable=True),
        sa.Column("variant_name", sa.String(length=64), nullable=False),
        sa.Column("trace_type", sa.String(length=64), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["scenario_id"], ["polymarket_replay_scenarios.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["replay_order_id"], ["polymarket_replay_orders.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_replay_decision_traces_scenario_variant",
        "polymarket_replay_decision_traces",
        ["scenario_id", "variant_name"],
    )
    op.create_index(
        "ix_pm_replay_decision_traces_type_observed",
        "polymarket_replay_decision_traces",
        ["trace_type", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_replay_decision_traces_reason_observed",
        "polymarket_replay_decision_traces",
        ["reason_code", "observed_at_local"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_replay_decision_traces_reason_observed", table_name="polymarket_replay_decision_traces")
    op.drop_index("ix_pm_replay_decision_traces_type_observed", table_name="polymarket_replay_decision_traces")
    op.drop_index("ix_pm_replay_decision_traces_scenario_variant", table_name="polymarket_replay_decision_traces")
    op.drop_table("polymarket_replay_decision_traces")

    op.drop_index("ix_pm_replay_metrics_scope_variant", table_name="polymarket_replay_metrics")
    op.drop_index("ix_pm_replay_metrics_scenario_variant", table_name="polymarket_replay_metrics")
    op.drop_index("ix_pm_replay_metrics_run_variant", table_name="polymarket_replay_metrics")
    op.drop_table("polymarket_replay_metrics")

    op.drop_index("ix_pm_replay_fills_source_kind_fill_ts", table_name="polymarket_replay_fills")
    op.drop_index("ix_pm_replay_fills_scenario_variant", table_name="polymarket_replay_fills")
    op.drop_table("polymarket_replay_fills")

    op.drop_index("ix_pm_replay_orders_source_quote_recommendation", table_name="polymarket_replay_orders")
    op.drop_index("ix_pm_replay_orders_source_execution_decision", table_name="polymarket_replay_orders")
    op.drop_index("ix_pm_replay_orders_status_decision_ts", table_name="polymarket_replay_orders")
    op.drop_index("ix_pm_replay_orders_scenario_variant", table_name="polymarket_replay_orders")
    op.drop_table("polymarket_replay_orders")

    op.drop_index("ix_pm_replay_scenarios_status_updated", table_name="polymarket_replay_scenarios")
    op.drop_index("ix_pm_replay_scenarios_asset_window", table_name="polymarket_replay_scenarios")
    op.drop_index("ix_pm_replay_scenarios_condition_window", table_name="polymarket_replay_scenarios")
    op.drop_index("ix_pm_replay_scenarios_type_window", table_name="polymarket_replay_scenarios")
    op.drop_index("ix_pm_replay_scenarios_run_id", table_name="polymarket_replay_scenarios")
    op.drop_table("polymarket_replay_scenarios")

    op.drop_index("ix_pm_replay_runs_window", table_name="polymarket_replay_runs")
    op.drop_index("ix_pm_replay_runs_status_started", table_name="polymarket_replay_runs")
    op.drop_index("ix_pm_replay_runs_type_reason_started", table_name="polymarket_replay_runs")
    op.drop_table("polymarket_replay_runs")
