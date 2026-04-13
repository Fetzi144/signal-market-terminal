"""Add Polymarket metadata registry and sync audit layer.

Revision ID: 025
Revises: 024
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_event_dim",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("gamma_event_id", sa.String(length=255), nullable=True),
        sa.Column("event_slug", sa.String(length=512), nullable=True),
        sa.Column("event_ticker", sa.String(length=255), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("subtitle", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=128), nullable=True),
        sa.Column("subcategory", sa.String(length=128), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("closed", sa.Boolean(), nullable=True),
        sa.Column("archived", sa.Boolean(), nullable=True),
        sa.Column("neg_risk", sa.Boolean(), nullable=True),
        sa.Column("neg_risk_market_id", sa.String(length=255), nullable=True),
        sa.Column("neg_risk_fee_bips", sa.Integer(), nullable=True),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at_source", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at_source", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_gamma_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stream_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("gamma_event_id", name="uq_pm_event_dim_gamma_event_id"),
    )
    op.create_index("ix_pm_event_dim_event_slug", "polymarket_event_dim", ["event_slug"])
    op.create_index("ix_pm_event_dim_last_gamma_sync", "polymarket_event_dim", ["last_gamma_sync_at"])
    op.create_index("ix_pm_event_dim_last_stream_event", "polymarket_event_dim", ["last_stream_event_at"])

    op.create_table(
        "polymarket_market_dim",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("gamma_market_id", sa.String(length=255), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("market_slug", sa.String(length=512), nullable=True),
        sa.Column("question", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("event_dim_id", sa.Integer(), nullable=True),
        sa.Column("enable_order_book", sa.Boolean(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("closed", sa.Boolean(), nullable=True),
        sa.Column("archived", sa.Boolean(), nullable=True),
        sa.Column("accepting_orders", sa.Boolean(), nullable=True),
        sa.Column("resolved", sa.Boolean(), nullable=True),
        sa.Column("resolution_state", sa.String(length=64), nullable=True),
        sa.Column("winning_asset_id", sa.String(length=255), nullable=True),
        sa.Column("clob_token_ids_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("outcomes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("tags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fees_enabled", sa.Boolean(), nullable=True),
        sa.Column("fee_schedule_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("maker_base_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("taker_base_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("last_gamma_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stream_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["event_dim_id"], ["polymarket_event_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("gamma_market_id", name="uq_pm_market_dim_gamma_market_id"),
        sa.UniqueConstraint("condition_id", name="uq_pm_market_dim_condition_id"),
    )
    op.create_index("ix_pm_market_dim_event_dim_id", "polymarket_market_dim", ["event_dim_id"])
    op.create_index("ix_pm_market_dim_market_slug", "polymarket_market_dim", ["market_slug"])
    op.create_index("ix_pm_market_dim_last_gamma_sync", "polymarket_market_dim", ["last_gamma_sync_at"])
    op.create_index("ix_pm_market_dim_last_stream_event", "polymarket_market_dim", ["last_stream_event_at"])
    op.create_index("ix_pm_market_dim_resolution_state", "polymarket_market_dim", ["resolution_state"])

    op.create_table(
        "polymarket_asset_dim",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("outcome_name", sa.String(length=255), nullable=True),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("winner", sa.Boolean(), nullable=True),
        sa.Column("last_gamma_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stream_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("asset_id", name="uq_pm_asset_dim_asset_id"),
    )
    op.create_index("ix_pm_asset_dim_condition_id", "polymarket_asset_dim", ["condition_id"])
    op.create_index("ix_pm_asset_dim_market_dim_id", "polymarket_asset_dim", ["market_dim_id"])
    op.create_index("ix_pm_asset_dim_outcome_id", "polymarket_asset_dim", ["outcome_id"])
    op.create_index("ix_pm_asset_dim_last_gamma_sync", "polymarket_asset_dim", ["last_gamma_sync_at"])
    op.create_index("ix_pm_asset_dim_last_stream_event", "polymarket_asset_dim", ["last_stream_event_at"])

    op.create_table(
        "polymarket_meta_sync_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("include_closed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("events_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("markets_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("assets_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("events_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("markets_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("param_rows_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_pm_meta_sync_runs_started_at", "polymarket_meta_sync_runs", ["started_at"])
    op.create_index(
        "ix_pm_meta_sync_runs_reason_started",
        "polymarket_meta_sync_runs",
        ["reason", "started_at"],
    )
    op.create_index(
        "ix_pm_meta_sync_runs_status_started",
        "polymarket_meta_sync_runs",
        ["status", "started_at"],
    )

    op.create_table(
        "polymarket_market_param_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("effective_at_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("sync_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("tick_size", sa.Numeric(18, 8), nullable=True),
        sa.Column("min_order_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("neg_risk", sa.Boolean(), nullable=True),
        sa.Column("fees_enabled", sa.Boolean(), nullable=True),
        sa.Column("fee_schedule_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("maker_base_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("taker_base_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("resolution_state", sa.String(length=64), nullable=True),
        sa.Column("winning_asset_id", sa.String(length=255), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["sync_run_id"], ["polymarket_meta_sync_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_pm_market_param_history_fingerprint"),
    )
    op.create_index(
        "ix_pm_market_param_history_asset_effective",
        "polymarket_market_param_history",
        ["asset_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_market_param_history_condition_effective",
        "polymarket_market_param_history",
        ["condition_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_market_param_history_market_effective",
        "polymarket_market_param_history",
        ["market_dim_id", "effective_at_exchange"],
    )
    op.create_index(
        "ix_pm_market_param_history_observed_at",
        "polymarket_market_param_history",
        ["observed_at_local"],
    )
    op.create_index(
        "ix_pm_market_param_history_sync_run_id",
        "polymarket_market_param_history",
        ["sync_run_id"],
    )
    op.create_index(
        "ix_pm_market_param_history_raw_event_id",
        "polymarket_market_param_history",
        ["raw_event_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_market_param_history_raw_event_id", table_name="polymarket_market_param_history")
    op.drop_index("ix_pm_market_param_history_sync_run_id", table_name="polymarket_market_param_history")
    op.drop_index("ix_pm_market_param_history_observed_at", table_name="polymarket_market_param_history")
    op.drop_index("ix_pm_market_param_history_market_effective", table_name="polymarket_market_param_history")
    op.drop_index("ix_pm_market_param_history_condition_effective", table_name="polymarket_market_param_history")
    op.drop_index("ix_pm_market_param_history_asset_effective", table_name="polymarket_market_param_history")
    op.drop_table("polymarket_market_param_history")

    op.drop_index("ix_pm_meta_sync_runs_status_started", table_name="polymarket_meta_sync_runs")
    op.drop_index("ix_pm_meta_sync_runs_reason_started", table_name="polymarket_meta_sync_runs")
    op.drop_index("ix_pm_meta_sync_runs_started_at", table_name="polymarket_meta_sync_runs")
    op.drop_table("polymarket_meta_sync_runs")

    op.drop_index("ix_pm_asset_dim_last_stream_event", table_name="polymarket_asset_dim")
    op.drop_index("ix_pm_asset_dim_last_gamma_sync", table_name="polymarket_asset_dim")
    op.drop_index("ix_pm_asset_dim_outcome_id", table_name="polymarket_asset_dim")
    op.drop_index("ix_pm_asset_dim_market_dim_id", table_name="polymarket_asset_dim")
    op.drop_index("ix_pm_asset_dim_condition_id", table_name="polymarket_asset_dim")
    op.drop_table("polymarket_asset_dim")

    op.drop_index("ix_pm_market_dim_resolution_state", table_name="polymarket_market_dim")
    op.drop_index("ix_pm_market_dim_last_stream_event", table_name="polymarket_market_dim")
    op.drop_index("ix_pm_market_dim_last_gamma_sync", table_name="polymarket_market_dim")
    op.drop_index("ix_pm_market_dim_market_slug", table_name="polymarket_market_dim")
    op.drop_index("ix_pm_market_dim_event_dim_id", table_name="polymarket_market_dim")
    op.drop_table("polymarket_market_dim")

    op.drop_index("ix_pm_event_dim_last_stream_event", table_name="polymarket_event_dim")
    op.drop_index("ix_pm_event_dim_last_gamma_sync", table_name="polymarket_event_dim")
    op.drop_index("ix_pm_event_dim_event_slug", table_name="polymarket_event_dim")
    op.drop_table("polymarket_event_dim")
