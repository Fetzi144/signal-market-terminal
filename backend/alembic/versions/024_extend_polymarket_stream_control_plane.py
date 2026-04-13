"""Extend Polymarket stream control plane for Phase 1B.

Revision ID: 024
Revises: 023
"""

import uuid
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("polymarket_stream_status") as batch_op:
        batch_op.add_column(sa.Column("last_message_received_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("gap_suspected_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("malformed_message_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("last_reconciliation_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "polymarket_resync_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("connection_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("requested_asset_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("succeeded_asset_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_asset_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_pm_resync_runs_started_at", "polymarket_resync_runs", ["started_at"])
    op.create_index("ix_pm_resync_runs_reason_started", "polymarket_resync_runs", ["reason", "started_at"])
    op.create_index("ix_pm_resync_runs_status_started", "polymarket_resync_runs", ["status", "started_at"])

    with op.batch_alter_table("polymarket_market_events") as batch_op:
        batch_op.add_column(sa.Column("resync_run_id", sa.Uuid(as_uuid=True), nullable=True))
        batch_op.create_foreign_key(
            "fk_polymarket_market_events_resync_run_id",
            "polymarket_resync_runs",
            ["resync_run_id"],
            ["id"],
            ondelete="SET NULL",
        )
    op.create_index("ix_pm_market_events_resync_run_id", "polymarket_market_events", ["resync_run_id"])

    op.create_table(
        "polymarket_ingest_incidents",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("incident_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("connection_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("resync_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["resync_run_id"], ["polymarket_resync_runs.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_pm_ingest_incidents_created_at", "polymarket_ingest_incidents", ["created_at"])
    op.create_index(
        "ix_pm_ingest_incidents_type_created",
        "polymarket_ingest_incidents",
        ["incident_type", "created_at"],
    )
    op.create_index(
        "ix_pm_ingest_incidents_asset_created",
        "polymarket_ingest_incidents",
        ["asset_id", "created_at"],
    )
    op.create_index(
        "ix_pm_ingest_incidents_connection_created",
        "polymarket_ingest_incidents",
        ["connection_id", "created_at"],
    )
    op.create_index(
        "ix_pm_ingest_incidents_resync_run_id",
        "polymarket_ingest_incidents",
        ["resync_run_id"],
    )

    op.create_table(
        "polymarket_watch_assets",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("watch_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("watch_reason", sa.String(length=255), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_pm_watch_assets_asset_id", "polymarket_watch_assets", ["asset_id"])
    op.create_index(
        "ix_pm_watch_assets_enabled_priority",
        "polymarket_watch_assets",
        ["watch_enabled", "priority"],
    )
    op.create_index("ix_pm_watch_assets_outcome_id", "polymarket_watch_assets", ["outcome_id"], unique=True)

    op.create_table(
        "polymarket_normalized_events",
        sa.Column("raw_event_id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("venue", sa.String(length=32), nullable=False, server_default="polymarket"),
        sa.Column("provenance", sa.String(length=32), nullable=False),
        sa.Column("channel", sa.String(length=64), nullable=False),
        sa.Column("message_type", sa.String(length=64), nullable=False),
        sa.Column("market_id", sa.String(length=255), nullable=True),
        sa.Column("asset_id", sa.String(length=255), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("price", sa.Numeric(18, 8), nullable=True),
        sa.Column("size", sa.Numeric(24, 8), nullable=True),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_bid_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("is_book_event", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_top_of_book", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("parse_status", sa.String(length=32), nullable=False, server_default="parsed"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_pm_normalized_events_asset_received",
        "polymarket_normalized_events",
        ["asset_id", "received_at_local"],
    )
    op.create_index(
        "ix_pm_normalized_events_message_type",
        "polymarket_normalized_events",
        ["message_type"],
    )
    op.create_index(
        "ix_pm_normalized_events_parse_status",
        "polymarket_normalized_events",
        ["parse_status"],
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT outcomes.id AS outcome_id, outcomes.token_id AS asset_id
            FROM outcomes
            JOIN markets ON outcomes.market_id = markets.id
            WHERE markets.platform = :platform
              AND markets.active = TRUE
              AND outcomes.token_id IS NOT NULL
            """
        ),
        {"platform": "polymarket"},
    ).mappings().all()

    if rows:
        watch_assets_table = sa.table(
            "polymarket_watch_assets",
            sa.column("id", sa.Uuid(as_uuid=True)),
            sa.column("outcome_id", sa.Uuid(as_uuid=True)),
            sa.column("asset_id", sa.String()),
            sa.column("watch_enabled", sa.Boolean()),
            sa.column("watch_reason", sa.String()),
            sa.column("created_at", sa.DateTime(timezone=True)),
            sa.column("updated_at", sa.DateTime(timezone=True)),
        )
        now = datetime.now(timezone.utc)
        op.bulk_insert(
            watch_assets_table,
            [
                {
                    "id": uuid.uuid4(),
                    "outcome_id": row["outcome_id"],
                    "asset_id": row["asset_id"],
                    "watch_enabled": True,
                    "watch_reason": "active_universe_bootstrap",
                    "created_at": now,
                    "updated_at": now,
                }
                for row in rows
            ],
        )


def downgrade() -> None:
    op.drop_index("ix_pm_normalized_events_parse_status", table_name="polymarket_normalized_events")
    op.drop_index("ix_pm_normalized_events_message_type", table_name="polymarket_normalized_events")
    op.drop_index("ix_pm_normalized_events_asset_received", table_name="polymarket_normalized_events")
    op.drop_table("polymarket_normalized_events")

    op.drop_index("ix_pm_watch_assets_outcome_id", table_name="polymarket_watch_assets")
    op.drop_index("ix_pm_watch_assets_enabled_priority", table_name="polymarket_watch_assets")
    op.drop_index("ix_pm_watch_assets_asset_id", table_name="polymarket_watch_assets")
    op.drop_table("polymarket_watch_assets")

    op.drop_index("ix_pm_ingest_incidents_resync_run_id", table_name="polymarket_ingest_incidents")
    op.drop_index("ix_pm_ingest_incidents_connection_created", table_name="polymarket_ingest_incidents")
    op.drop_index("ix_pm_ingest_incidents_asset_created", table_name="polymarket_ingest_incidents")
    op.drop_index("ix_pm_ingest_incidents_type_created", table_name="polymarket_ingest_incidents")
    op.drop_index("ix_pm_ingest_incidents_created_at", table_name="polymarket_ingest_incidents")
    op.drop_table("polymarket_ingest_incidents")

    op.drop_index("ix_pm_market_events_resync_run_id", table_name="polymarket_market_events")
    with op.batch_alter_table("polymarket_market_events") as batch_op:
        batch_op.drop_constraint("fk_polymarket_market_events_resync_run_id", type_="foreignkey")
        batch_op.drop_column("resync_run_id")

    op.drop_index("ix_pm_resync_runs_status_started", table_name="polymarket_resync_runs")
    op.drop_index("ix_pm_resync_runs_reason_started", table_name="polymarket_resync_runs")
    op.drop_index("ix_pm_resync_runs_started_at", table_name="polymarket_resync_runs")
    op.drop_table("polymarket_resync_runs")

    with op.batch_alter_table("polymarket_stream_status") as batch_op:
        batch_op.drop_column("last_reconciliation_at")
        batch_op.drop_column("malformed_message_count")
        batch_op.drop_column("gap_suspected_count")
        batch_op.drop_column("last_message_received_at")
