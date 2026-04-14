"""Add Phase 9 Polymarket maker economics history and advisory artifacts.

Revision ID: 033
Revises: 032
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_token_fee_rate_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("effective_at_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sync_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("fees_enabled", sa.Boolean(), nullable=True),
        sa.Column("maker_fee_rate", sa.Numeric(18, 8), nullable=True),
        sa.Column("taker_fee_rate", sa.Numeric(18, 8), nullable=True),
        sa.Column("token_base_fee_rate", sa.Numeric(18, 8), nullable=True),
        sa.Column("fee_schedule_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["sync_run_id"], ["polymarket_meta_sync_runs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_pm_token_fee_rate_history_fingerprint"),
    )
    op.create_index(
        "ix_pm_token_fee_history_asset_effective",
        "polymarket_token_fee_rate_history",
        ["asset_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_token_fee_history_condition_effective",
        "polymarket_token_fee_rate_history",
        ["condition_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_token_fee_history_market_effective",
        "polymarket_token_fee_rate_history",
        ["market_dim_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_token_fee_history_observed_at",
        "polymarket_token_fee_rate_history",
        ["observed_at_local"],
    )
    op.create_index(
        "ix_pm_token_fee_history_sync_run_id",
        "polymarket_token_fee_rate_history",
        ["sync_run_id"],
    )

    op.create_table(
        "polymarket_market_reward_config_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("effective_at_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sync_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("reward_status", sa.String(length=32), nullable=False),
        sa.Column("reward_program_id", sa.String(length=255), nullable=True),
        sa.Column("reward_daily_rate", sa.Numeric(24, 8), nullable=True),
        sa.Column("min_incentive_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("max_incentive_spread", sa.Numeric(18, 8), nullable=True),
        sa.Column("start_at_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_at_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("config_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rewards_config_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["sync_run_id"], ["polymarket_meta_sync_runs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_pm_market_reward_history_fingerprint"),
    )
    op.create_index(
        "ix_pm_reward_history_condition_effective",
        "polymarket_market_reward_config_history",
        ["condition_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_reward_history_market_effective",
        "polymarket_market_reward_config_history",
        ["market_dim_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_reward_history_reward_status",
        "polymarket_market_reward_config_history",
        ["reward_status"],
    )
    op.create_index(
        "ix_pm_reward_history_observed_at",
        "polymarket_market_reward_config_history",
        ["observed_at_local"],
    )
    op.create_index(
        "ix_pm_reward_history_sync_run_id",
        "polymarket_market_reward_config_history",
        ["sync_run_id"],
    )

    op.create_table(
        "polymarket_maker_economics_snapshots",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=True),
        sa.Column("validation_id", sa.Integer(), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("fee_history_id", sa.Integer(), nullable=True),
        sa.Column("reward_history_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("context_kind", sa.String(length=64), nullable=False),
        sa.Column("estimator_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("preferred_action", sa.String(length=32), nullable=True),
        sa.Column("maker_action_type", sa.String(length=32), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("target_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("target_notional", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_fill_probability", sa.Numeric(10, 6), nullable=True),
        sa.Column("maker_gross_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_fees_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_rewards_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_realism_adjustment_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_net_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_gross_edge_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_fees_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_rewards_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_realism_adjustment_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_net_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("maker_advantage_total", sa.Numeric(24, 8), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("input_fingerprint", sa.String(length=128), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["market_structure_opportunities.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["validation_id"], ["market_structure_validations.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["fee_history_id"], ["polymarket_token_fee_rate_history.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["reward_history_id"],
            ["polymarket_market_reward_config_history.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("input_fingerprint", name="uq_pm_maker_snapshots_input_fingerprint"),
    )
    op.create_index(
        "ix_pm_maker_snapshots_opportunity_evaluated",
        "polymarket_maker_economics_snapshots",
        ["opportunity_id", "evaluated_at"],
    )
    op.create_index(
        "ix_pm_maker_snapshots_asset_evaluated",
        "polymarket_maker_economics_snapshots",
        ["asset_id", "evaluated_at"],
    )
    op.create_index(
        "ix_pm_maker_snapshots_condition_evaluated",
        "polymarket_maker_economics_snapshots",
        ["condition_id", "evaluated_at"],
    )
    op.create_index(
        "ix_pm_maker_snapshots_status_evaluated",
        "polymarket_maker_economics_snapshots",
        ["status", "evaluated_at"],
    )

    op.create_table(
        "polymarket_quote_recommendations",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("snapshot_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("recommendation_kind", sa.String(length=32), nullable=False, server_default="advisory_quote"),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("comparison_winner", sa.String(length=16), nullable=True),
        sa.Column("recommendation_action", sa.String(length=32), nullable=True),
        sa.Column("recommended_action_type", sa.String(length=32), nullable=True),
        sa.Column("recommended_side", sa.String(length=16), nullable=True),
        sa.Column("recommended_yes_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("recommended_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("recommended_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("recommended_notional", sa.Numeric(24, 8), nullable=True),
        sa.Column("price_offset_ticks", sa.Integer(), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("input_fingerprint", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["polymarket_maker_economics_snapshots.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["opportunity_id"], ["market_structure_opportunities.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("input_fingerprint", name="uq_pm_quote_recommendations_input_fingerprint"),
    )
    op.create_index(
        "ix_pm_quote_recommendations_opportunity_created",
        "polymarket_quote_recommendations",
        ["opportunity_id", "created_at"],
    )
    op.create_index(
        "ix_pm_quote_recommendations_snapshot_id",
        "polymarket_quote_recommendations",
        ["snapshot_id"],
    )
    op.create_index(
        "ix_pm_quote_recommendations_asset_created",
        "polymarket_quote_recommendations",
        ["asset_id", "created_at"],
    )
    op.create_index(
        "ix_pm_quote_recommendations_condition_created",
        "polymarket_quote_recommendations",
        ["condition_id", "created_at"],
    )
    op.create_index(
        "ix_pm_quote_recommendations_status_created",
        "polymarket_quote_recommendations",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_quote_recommendations_status_created", table_name="polymarket_quote_recommendations")
    op.drop_index("ix_pm_quote_recommendations_condition_created", table_name="polymarket_quote_recommendations")
    op.drop_index("ix_pm_quote_recommendations_asset_created", table_name="polymarket_quote_recommendations")
    op.drop_index("ix_pm_quote_recommendations_snapshot_id", table_name="polymarket_quote_recommendations")
    op.drop_index("ix_pm_quote_recommendations_opportunity_created", table_name="polymarket_quote_recommendations")
    op.drop_table("polymarket_quote_recommendations")

    op.drop_index("ix_pm_maker_snapshots_status_evaluated", table_name="polymarket_maker_economics_snapshots")
    op.drop_index("ix_pm_maker_snapshots_condition_evaluated", table_name="polymarket_maker_economics_snapshots")
    op.drop_index("ix_pm_maker_snapshots_asset_evaluated", table_name="polymarket_maker_economics_snapshots")
    op.drop_index("ix_pm_maker_snapshots_opportunity_evaluated", table_name="polymarket_maker_economics_snapshots")
    op.drop_table("polymarket_maker_economics_snapshots")

    op.drop_index("ix_pm_reward_history_sync_run_id", table_name="polymarket_market_reward_config_history")
    op.drop_index("ix_pm_reward_history_observed_at", table_name="polymarket_market_reward_config_history")
    op.drop_index("ix_pm_reward_history_reward_status", table_name="polymarket_market_reward_config_history")
    op.drop_index("ix_pm_reward_history_market_effective", table_name="polymarket_market_reward_config_history")
    op.drop_index("ix_pm_reward_history_condition_effective", table_name="polymarket_market_reward_config_history")
    op.drop_table("polymarket_market_reward_config_history")

    op.drop_index("ix_pm_token_fee_history_sync_run_id", table_name="polymarket_token_fee_rate_history")
    op.drop_index("ix_pm_token_fee_history_observed_at", table_name="polymarket_token_fee_rate_history")
    op.drop_index("ix_pm_token_fee_history_market_effective", table_name="polymarket_token_fee_rate_history")
    op.drop_index("ix_pm_token_fee_history_condition_effective", table_name="polymarket_token_fee_rate_history")
    op.drop_index("ix_pm_token_fee_history_asset_effective", table_name="polymarket_token_fee_rate_history")
    op.drop_table("polymarket_token_fee_rate_history")
