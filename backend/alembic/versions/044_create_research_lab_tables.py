"""Create research lab batch and lane-result tables.

Revision ID: 044
Revises: 043
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "044"
down_revision = "043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "research_batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("batch_key", sa.String(length=128), nullable=False),
        sa.Column("preset", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("max_markets", sa.Integer(), nullable=False, server_default="500"),
        sa.Column("universe_fingerprint", sa.String(length=128), nullable=False),
        sa.Column("families_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("config_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("universe_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_key", name="uq_research_batches_batch_key"),
    )
    op.create_index(
        "ix_research_batches_status_created",
        "research_batches",
        ["status", "created_at"],
    )
    op.create_index(
        "ix_research_batches_preset_window",
        "research_batches",
        ["preset", "window_start", "window_end"],
    )
    op.create_index("ix_research_batches_universe", "research_batches", ["universe_fingerprint"])

    op.create_table(
        "research_lane_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("family", sa.String(length=64), nullable=False),
        sa.Column("strategy_version", sa.String(length=128), nullable=True),
        sa.Column("lane", sa.String(length=64), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_ref", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="completed"),
        sa.Column("verdict", sa.String(length=64), nullable=False, server_default="insufficient_evidence"),
        sa.Column("rank_position", sa.Integer(), nullable=True),
        sa.Column("rank_key", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("mark_to_market_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("replay_net_pnl", sa.Numeric(24, 8), nullable=True),
        sa.Column("avg_clv", sa.Numeric(18, 8), nullable=True),
        sa.Column("resolved_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fill_rate", sa.Numeric(10, 6), nullable=True),
        sa.Column("drawdown", sa.Numeric(24, 8), nullable=True),
        sa.Column("open_exposure", sa.Numeric(24, 8), nullable=True),
        sa.Column("coverage_mode", sa.String(length=64), nullable=True),
        sa.Column("blockers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["research_batches.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id",
            "family",
            "lane",
            "source_kind",
            name="uq_research_lane_results_batch_lane_source",
        ),
    )
    op.create_index(
        "ix_research_lane_results_batch_rank",
        "research_lane_results",
        ["batch_id", "rank_position"],
    )
    op.create_index(
        "ix_research_lane_results_family_created",
        "research_lane_results",
        ["family", "created_at"],
    )
    op.create_index("ix_research_lane_results_verdict", "research_lane_results", ["verdict"])


def downgrade() -> None:
    op.drop_index("ix_research_lane_results_verdict", table_name="research_lane_results")
    op.drop_index("ix_research_lane_results_family_created", table_name="research_lane_results")
    op.drop_index("ix_research_lane_results_batch_rank", table_name="research_lane_results")
    op.drop_table("research_lane_results")
    op.drop_index("ix_research_batches_universe", table_name="research_batches")
    op.drop_index("ix_research_batches_preset_window", table_name="research_batches")
    op.drop_index("ix_research_batches_status_created", table_name="research_batches")
    op.drop_table("research_batches")
