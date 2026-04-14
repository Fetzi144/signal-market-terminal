"""Add Phase 7A Polymarket OMS/EMS foundation tables.

Revision ID: 030
Revises: 029
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_user_events_raw",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("stream_session_id", sa.String(length=64), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recv_ts_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_ts_db", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("source_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_pm_user_events_raw_condition_ingest",
        "polymarket_user_events_raw",
        ["condition_id", "ingest_ts_db"],
    )
    op.create_index(
        "ix_pm_user_events_raw_asset_ingest",
        "polymarket_user_events_raw",
        ["asset_id", "ingest_ts_db"],
    )
    op.create_index(
        "ix_pm_user_events_raw_event_type_ingest",
        "polymarket_user_events_raw",
        ["event_type", "ingest_ts_db"],
    )
    op.create_index(
        "ix_pm_user_events_raw_stream_session_id",
        "polymarket_user_events_raw",
        ["stream_session_id"],
    )

    op.create_table(
        "live_orders",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("execution_decision_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("signal_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("client_order_id", sa.String(length=128), nullable=False),
        sa.Column("venue_order_id", sa.String(length=255), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("order_type", sa.String(length=32), nullable=False),
        sa.Column("post_only", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("limit_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("target_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("requested_size", sa.Numeric(24, 8), nullable=False),
        sa.Column("submitted_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("filled_size", sa.Numeric(24, 8), nullable=False, server_default="0"),
        sa.Column("avg_fill_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("manual_approval_required", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("approved_by", sa.String(length=128), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("kill_switch_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("allowlist_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("submission_error", sa.Text(), nullable=True),
        sa.Column("policy_version", sa.String(length=64), nullable=True),
        sa.Column("decision_reason_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["execution_decision_id"], ["execution_decisions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("client_order_id", name="uq_live_orders_client_order_id"),
    )
    op.create_index("ix_live_orders_execution_decision_id", "live_orders", ["execution_decision_id"])
    op.create_index("ix_live_orders_signal_id", "live_orders", ["signal_id"])
    op.create_index("ix_live_orders_condition_status", "live_orders", ["condition_id", "status"])
    op.create_index("ix_live_orders_asset_status", "live_orders", ["asset_id", "status"])
    op.create_index("ix_live_orders_status_created_at", "live_orders", ["status", "created_at"])
    op.create_index("ix_live_orders_venue_order_id", "live_orders", ["venue_order_id"])

    op.create_table(
        "live_order_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("raw_user_event_id", sa.Integer(), nullable=True),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("venue_status", sa.String(length=32), nullable=True),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["raw_user_event_id"], ["polymarket_user_events_raw.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_live_order_events_fingerprint"),
    )
    op.create_index("ix_live_order_events_live_order_observed", "live_order_events", ["live_order_id", "observed_at_local"])
    op.create_index("ix_live_order_events_raw_user_event_id", "live_order_events", ["raw_user_event_id"])
    op.create_index("ix_live_order_events_source_kind_observed", "live_order_events", ["source_kind", "observed_at_local"])

    op.create_table(
        "live_fills",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("trade_id", sa.String(length=255), nullable=True),
        sa.Column("transaction_hash", sa.String(length=255), nullable=True),
        sa.Column("fill_status", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("size", sa.Numeric(24, 8), nullable=False),
        sa.Column("fee_paid", sa.Numeric(18, 8), nullable=True),
        sa.Column("fee_currency", sa.String(length=32), nullable=True),
        sa.Column("maker_taker", sa.String(length=16), nullable=True),
        sa.Column("event_ts_exchange", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("raw_user_event_id", sa.Integer(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_user_event_id"], ["polymarket_user_events_raw.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_live_fills_fingerprint"),
    )
    op.create_index("ix_live_fills_live_order_observed", "live_fills", ["live_order_id", "observed_at_local"])
    op.create_index("ix_live_fills_condition_observed", "live_fills", ["condition_id", "observed_at_local"])
    op.create_index("ix_live_fills_asset_observed", "live_fills", ["asset_id", "observed_at_local"])
    op.create_index("ix_live_fills_trade_id", "live_fills", ["trade_id"])

    op.create_table(
        "capital_reservations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("live_order_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("reservation_kind", sa.String(length=32), nullable=False),
        sa.Column("requested_amount", sa.Numeric(24, 8), nullable=False),
        sa.Column("reserved_amount", sa.Numeric(24, 8), nullable=False),
        sa.Column("released_amount", sa.Numeric(24, 8), nullable=False, server_default="0"),
        sa.Column("open_amount", sa.Numeric(24, 8), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["live_order_id"], ["live_orders.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_capital_reservations_fingerprint"),
    )
    op.create_index(
        "ix_capital_reservations_live_order_observed",
        "capital_reservations",
        ["live_order_id", "observed_at_local"],
    )
    op.create_index(
        "ix_capital_reservations_condition_observed",
        "capital_reservations",
        ["condition_id", "observed_at_local"],
    )
    op.create_index(
        "ix_capital_reservations_status_observed",
        "capital_reservations",
        ["status", "observed_at_local"],
    )

    op.create_table(
        "polymarket_live_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("kill_switch_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("allowlist_markets_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("allowlist_categories_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("gateway_reachable", sa.Boolean(), nullable=True),
        sa.Column("gateway_last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("gateway_last_error", sa.String(length=255), nullable=True),
        sa.Column("user_stream_connected", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("user_stream_session_id", sa.String(length=64), nullable=True),
        sa.Column("user_stream_connection_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_user_stream_message_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_user_stream_error", sa.String(length=255), nullable=True),
        sa.Column("last_user_stream_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconciled_user_event_id", sa.Integer(), nullable=True),
        sa.Column("last_reconcile_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconcile_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconcile_error", sa.String(length=255), nullable=True),
        sa.Column("last_reconcile_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("polymarket_live_state")

    op.drop_index("ix_capital_reservations_status_observed", table_name="capital_reservations")
    op.drop_index("ix_capital_reservations_condition_observed", table_name="capital_reservations")
    op.drop_index("ix_capital_reservations_live_order_observed", table_name="capital_reservations")
    op.drop_table("capital_reservations")

    op.drop_index("ix_live_fills_trade_id", table_name="live_fills")
    op.drop_index("ix_live_fills_asset_observed", table_name="live_fills")
    op.drop_index("ix_live_fills_condition_observed", table_name="live_fills")
    op.drop_index("ix_live_fills_live_order_observed", table_name="live_fills")
    op.drop_table("live_fills")

    op.drop_index("ix_live_order_events_source_kind_observed", table_name="live_order_events")
    op.drop_index("ix_live_order_events_raw_user_event_id", table_name="live_order_events")
    op.drop_index("ix_live_order_events_live_order_observed", table_name="live_order_events")
    op.drop_table("live_order_events")

    op.drop_index("ix_live_orders_venue_order_id", table_name="live_orders")
    op.drop_index("ix_live_orders_status_created_at", table_name="live_orders")
    op.drop_index("ix_live_orders_asset_status", table_name="live_orders")
    op.drop_index("ix_live_orders_condition_status", table_name="live_orders")
    op.drop_index("ix_live_orders_signal_id", table_name="live_orders")
    op.drop_index("ix_live_orders_execution_decision_id", table_name="live_orders")
    op.drop_table("live_orders")

    op.drop_index("ix_pm_user_events_raw_stream_session_id", table_name="polymarket_user_events_raw")
    op.drop_index("ix_pm_user_events_raw_event_type_ingest", table_name="polymarket_user_events_raw")
    op.drop_index("ix_pm_user_events_raw_asset_ingest", table_name="polymarket_user_events_raw")
    op.drop_index("ix_pm_user_events_raw_condition_ingest", table_name="polymarket_user_events_raw")
    op.drop_table("polymarket_user_events_raw")
