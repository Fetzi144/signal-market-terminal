"""Add Phase 3 Polymarket raw storage layer.

Revision ID: 026
Revises: 025
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_raw_capture_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("run_type", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("scope_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("cursor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "ix_pm_raw_capture_runs_reason_started",
        "polymarket_raw_capture_runs",
        ["reason", "started_at"],
    )
    op.create_index(
        "ix_pm_raw_capture_runs_run_type_started",
        "polymarket_raw_capture_runs",
        ["run_type", "started_at"],
    )
    op.create_index(
        "ix_pm_raw_capture_runs_status_started",
        "polymarket_raw_capture_runs",
        ["status", "started_at"],
    )

    op.create_table(
        "polymarket_book_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recv_ts_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_ts_db", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("stream_session_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("capture_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("book_hash", sa.String(length=255), nullable=True),
        sa.Column("bids_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("asks_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("min_order_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("tick_size", sa.Numeric(18, 8), nullable=True),
        sa.Column("neg_risk", sa.Boolean(), nullable=True),
        sa.Column("last_trade_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("spread", sa.Numeric(18, 8), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["capture_run_id"], ["polymarket_raw_capture_runs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("raw_event_id", name="uq_pm_book_snapshots_raw_event_id"),
        sa.UniqueConstraint("fingerprint", name="uq_pm_book_snapshots_fingerprint"),
    )
    op.create_index(
        "ix_pm_book_snapshots_asset_event_ts",
        "polymarket_book_snapshots",
        ["asset_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_book_snapshots_condition_event_ts",
        "polymarket_book_snapshots",
        ["condition_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_book_snapshots_observed_at",
        "polymarket_book_snapshots",
        ["observed_at_local"],
    )
    op.create_index(
        "ix_pm_book_snapshots_capture_run_id",
        "polymarket_book_snapshots",
        ["capture_run_id"],
    )

    op.create_table(
        "polymarket_book_deltas",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recv_ts_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_ts_db", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("stream_session_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=False),
        sa.Column("delta_index", sa.Integer(), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("size", sa.Numeric(24, 8), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("delta_hash", sa.String(length=255), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("raw_event_id", "delta_index", name="uq_pm_book_deltas_raw_event_delta_index"),
    )
    op.create_index(
        "ix_pm_book_deltas_asset_event_ts",
        "polymarket_book_deltas",
        ["asset_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_book_deltas_condition_event_ts",
        "polymarket_book_deltas",
        ["condition_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_book_deltas_raw_event_id",
        "polymarket_book_deltas",
        ["raw_event_id"],
    )

    op.create_table(
        "polymarket_bbo_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recv_ts_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_ts_db", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("stream_session_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("spread", sa.Numeric(18, 8), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("raw_event_id", name="uq_pm_bbo_events_raw_event_id"),
    )
    op.create_index(
        "ix_pm_bbo_events_asset_event_ts",
        "polymarket_bbo_events",
        ["asset_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_bbo_events_condition_event_ts",
        "polymarket_bbo_events",
        ["condition_id", "event_ts_exchange"],
    )

    op.create_table(
        "polymarket_trade_tape",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recv_ts_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_ts_db", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("stream_session_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("capture_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("transaction_hash", sa.String(length=255), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("size", sa.Numeric(24, 8), nullable=False),
        sa.Column("fee_rate_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("event_slug", sa.String(length=512), nullable=True),
        sa.Column("outcome_name", sa.String(length=255), nullable=True),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("proxy_wallet", sa.String(length=255), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("fallback_fingerprint", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["capture_run_id"], ["polymarket_raw_capture_runs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("raw_event_id", name="uq_pm_trade_tape_raw_event_id"),
        sa.UniqueConstraint("fingerprint", name="uq_pm_trade_tape_fingerprint"),
    )
    op.create_index(
        "ix_pm_trade_tape_asset_event_ts",
        "polymarket_trade_tape",
        ["asset_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_trade_tape_condition_event_ts",
        "polymarket_trade_tape",
        ["condition_id", "event_ts_exchange"],
    )
    op.create_index(
        "ix_pm_trade_tape_observed_at",
        "polymarket_trade_tape",
        ["observed_at_local"],
    )
    op.create_index(
        "ix_pm_trade_tape_capture_run_id",
        "polymarket_trade_tape",
        ["capture_run_id"],
    )
    op.create_index(
        "ix_pm_trade_tape_transaction_hash",
        "polymarket_trade_tape",
        ["transaction_hash"],
    )
    op.create_index(
        "ix_pm_trade_tape_fallback_fingerprint",
        "polymarket_trade_tape",
        ["fallback_fingerprint"],
    )

    op.create_table(
        "polymarket_open_interest_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("capture_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("value", sa.Numeric(24, 8), nullable=False),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["capture_run_id"], ["polymarket_raw_capture_runs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "capture_run_id",
            "condition_id",
            name="uq_pm_open_interest_history_capture_condition",
        ),
    )
    op.create_index(
        "ix_pm_open_interest_history_condition_observed",
        "polymarket_open_interest_history",
        ["condition_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_open_interest_history_market_observed",
        "polymarket_open_interest_history",
        ["market_dim_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_open_interest_history_capture_run_id",
        "polymarket_open_interest_history",
        ["capture_run_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_open_interest_history_capture_run_id", table_name="polymarket_open_interest_history")
    op.drop_index("ix_pm_open_interest_history_market_observed", table_name="polymarket_open_interest_history")
    op.drop_index("ix_pm_open_interest_history_condition_observed", table_name="polymarket_open_interest_history")
    op.drop_table("polymarket_open_interest_history")

    op.drop_index("ix_pm_trade_tape_fallback_fingerprint", table_name="polymarket_trade_tape")
    op.drop_index("ix_pm_trade_tape_transaction_hash", table_name="polymarket_trade_tape")
    op.drop_index("ix_pm_trade_tape_capture_run_id", table_name="polymarket_trade_tape")
    op.drop_index("ix_pm_trade_tape_observed_at", table_name="polymarket_trade_tape")
    op.drop_index("ix_pm_trade_tape_condition_event_ts", table_name="polymarket_trade_tape")
    op.drop_index("ix_pm_trade_tape_asset_event_ts", table_name="polymarket_trade_tape")
    op.drop_table("polymarket_trade_tape")

    op.drop_index("ix_pm_bbo_events_condition_event_ts", table_name="polymarket_bbo_events")
    op.drop_index("ix_pm_bbo_events_asset_event_ts", table_name="polymarket_bbo_events")
    op.drop_table("polymarket_bbo_events")

    op.drop_index("ix_pm_book_deltas_raw_event_id", table_name="polymarket_book_deltas")
    op.drop_index("ix_pm_book_deltas_condition_event_ts", table_name="polymarket_book_deltas")
    op.drop_index("ix_pm_book_deltas_asset_event_ts", table_name="polymarket_book_deltas")
    op.drop_table("polymarket_book_deltas")

    op.drop_index("ix_pm_book_snapshots_capture_run_id", table_name="polymarket_book_snapshots")
    op.drop_index("ix_pm_book_snapshots_observed_at", table_name="polymarket_book_snapshots")
    op.drop_index("ix_pm_book_snapshots_condition_event_ts", table_name="polymarket_book_snapshots")
    op.drop_index("ix_pm_book_snapshots_asset_event_ts", table_name="polymarket_book_snapshots")
    op.drop_table("polymarket_book_snapshots")

    op.drop_index("ix_pm_raw_capture_runs_status_started", table_name="polymarket_raw_capture_runs")
    op.drop_index("ix_pm_raw_capture_runs_run_type_started", table_name="polymarket_raw_capture_runs")
    op.drop_index("ix_pm_raw_capture_runs_reason_started", table_name="polymarket_raw_capture_runs")
    op.drop_table("polymarket_raw_capture_runs")
