"""Add Phase 1A Polymarket market event storage.

Revision ID: 023
Revises: 022
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_market_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("venue", sa.String(length=32), nullable=False, server_default="polymarket"),
        sa.Column("provenance", sa.String(length=32), nullable=False),
        sa.Column("channel", sa.String(length=64), nullable=False),
        sa.Column("message_type", sa.String(length=64), nullable=False),
        sa.Column("market_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("asset_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("connection_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("ingest_batch_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("source_message_id", sa.String(length=255), nullable=True),
        sa.Column("source_hash", sa.String(length=255), nullable=True),
        sa.Column("source_sequence", sa.String(length=255), nullable=True),
        sa.Column("source_cursor", sa.String(length=255), nullable=True),
        sa.Column("resync_reason", sa.String(length=64), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_pm_market_events_asset_received",
        "polymarket_market_events",
        ["asset_id", "received_at_local"],
    )
    op.create_index("ix_pm_market_events_event_time", "polymarket_market_events", ["event_time"])
    op.create_index(
        "ix_pm_market_events_market_received",
        "polymarket_market_events",
        ["market_id", "received_at_local"],
    )
    op.create_index("ix_pm_market_events_message_type", "polymarket_market_events", ["message_type"])
    op.create_index(
        "ix_pm_market_events_venue_provenance_received",
        "polymarket_market_events",
        ["venue", "provenance", "received_at_local"],
    )

    op.create_table(
        "polymarket_stream_status",
        sa.Column("venue", sa.String(length=32), primary_key=True, nullable=False, server_default="polymarket"),
        sa.Column("connected", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("connection_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_connection_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("active_subscription_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reconnect_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("resync_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_resync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("polymarket_stream_status")
    op.drop_index("ix_pm_market_events_venue_provenance_received", table_name="polymarket_market_events")
    op.drop_index("ix_pm_market_events_message_type", table_name="polymarket_market_events")
    op.drop_index("ix_pm_market_events_market_received", table_name="polymarket_market_events")
    op.drop_index("ix_pm_market_events_event_time", table_name="polymarket_market_events")
    op.drop_index("ix_pm_market_events_asset_received", table_name="polymarket_market_events")
    op.drop_table("polymarket_market_events")
