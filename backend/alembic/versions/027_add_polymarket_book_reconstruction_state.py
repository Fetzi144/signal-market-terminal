"""Add Polymarket Phase 4 reconstruction control-plane tables.

Revision ID: 027
Revises: 026
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_book_recon_state",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="unseeded"),
        sa.Column("last_snapshot_id", sa.Integer(), nullable=True),
        sa.Column("last_snapshot_source_kind", sa.String(length=64), nullable=True),
        sa.Column("last_snapshot_hash", sa.String(length=255), nullable=True),
        sa.Column("last_snapshot_exchange_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_applied_raw_event_id", sa.Integer(), nullable=True),
        sa.Column("last_applied_delta_raw_event_id", sa.Integer(), nullable=True),
        sa.Column("last_applied_delta_index", sa.Integer(), nullable=True),
        sa.Column("last_bbo_raw_event_id", sa.Integer(), nullable=True),
        sa.Column("last_trade_raw_event_id", sa.Integer(), nullable=True),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("spread", sa.Numeric(18, 8), nullable=True),
        sa.Column("depth_levels_bid", sa.Integer(), nullable=True),
        sa.Column("depth_levels_ask", sa.Integer(), nullable=True),
        sa.Column("expected_tick_size", sa.Numeric(18, 8), nullable=True),
        sa.Column("last_exchange_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_received_at_local", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_resynced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("drift_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("resync_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_snapshot_id"], ["polymarket_book_snapshots.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("asset_id", name="uq_pm_book_recon_state_asset_id"),
    )
    op.create_index("ix_pm_book_recon_state_asset_id", "polymarket_book_recon_state", ["asset_id"], unique=True)
    op.create_index("ix_pm_book_recon_state_condition_id", "polymarket_book_recon_state", ["condition_id"])
    op.create_index(
        "ix_pm_book_recon_state_status_updated",
        "polymarket_book_recon_state",
        ["status", "updated_at"],
    )
    op.create_index(
        "ix_pm_book_recon_state_last_reconciled",
        "polymarket_book_recon_state",
        ["last_reconciled_at"],
    )

    op.create_table(
        "polymarket_book_recon_incidents",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("incident_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False, server_default="info"),
        sa.Column("raw_event_id", sa.Integer(), nullable=True),
        sa.Column("snapshot_id", sa.Integer(), nullable=True),
        sa.Column("capture_run_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("exchange_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expected_best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("observed_best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("expected_best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("observed_best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("expected_hash", sa.String(length=255), nullable=True),
        sa.Column("observed_hash", sa.String(length=255), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["raw_event_id"], ["polymarket_market_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["snapshot_id"], ["polymarket_book_snapshots.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["capture_run_id"], ["polymarket_raw_capture_runs.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_book_recon_incidents_asset_observed",
        "polymarket_book_recon_incidents",
        ["asset_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_book_recon_incidents_condition_observed",
        "polymarket_book_recon_incidents",
        ["condition_id", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_book_recon_incidents_type_observed",
        "polymarket_book_recon_incidents",
        ["incident_type", "observed_at_local"],
    )
    op.create_index(
        "ix_pm_book_recon_incidents_observed",
        "polymarket_book_recon_incidents",
        ["observed_at_local"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_book_recon_incidents_observed", table_name="polymarket_book_recon_incidents")
    op.drop_index("ix_pm_book_recon_incidents_type_observed", table_name="polymarket_book_recon_incidents")
    op.drop_index("ix_pm_book_recon_incidents_condition_observed", table_name="polymarket_book_recon_incidents")
    op.drop_index("ix_pm_book_recon_incidents_asset_observed", table_name="polymarket_book_recon_incidents")
    op.drop_table("polymarket_book_recon_incidents")

    op.drop_index("ix_pm_book_recon_state_last_reconciled", table_name="polymarket_book_recon_state")
    op.drop_index("ix_pm_book_recon_state_status_updated", table_name="polymarket_book_recon_state")
    op.drop_index("ix_pm_book_recon_state_condition_id", table_name="polymarket_book_recon_state")
    op.drop_index("ix_pm_book_recon_state_asset_id", table_name="polymarket_book_recon_state")
    op.drop_table("polymarket_book_recon_state")
