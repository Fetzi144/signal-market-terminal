"""Initial schema: markets, outcomes, snapshots, signals, evaluations, ingestion_runs.

Revision ID: 001
Revises:
Create Date: 2026-04-07
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Markets
    op.create_table(
        "markets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("platform_id", sa.String(255), nullable=False),
        sa.Column("slug", sa.String(512), nullable=True),
        sa.Column("question", sa.Text, nullable=False),
        sa.Column("category", sa.String(128), nullable=True),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("active", sa.Boolean, default=True),
        sa.Column("last_volume_24h", sa.Numeric(20, 2), nullable=True),
        sa.Column("last_liquidity", sa.Numeric(20, 2), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("platform", "platform_id", name="uq_market_platform_id"),
    )
    op.create_index("ix_market_active_platform", "markets", ["active", "platform"])
    op.create_index("ix_market_end_date", "markets", ["end_date"])

    # Outcomes
    op.create_table(
        "outcomes",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("market_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("markets.id"), nullable=False),
        sa.Column("platform_outcome_id", sa.String(255), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("token_id", sa.String(255), nullable=True),
        sa.UniqueConstraint("market_id", "platform_outcome_id", name="uq_outcome_market"),
        comment="Individual outcomes (Yes/No) for a market",
    )

    # Price Snapshots
    op.create_table(
        "price_snapshots",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("outcome_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("outcomes.id"), nullable=False),
        sa.Column("price", sa.Numeric(10, 6), nullable=False),
        sa.Column("volume_24h", sa.Numeric(20, 2), nullable=True),
        sa.Column("liquidity", sa.Numeric(20, 2), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_price_snap_outcome_time", "price_snapshots", ["outcome_id", "captured_at"])

    # Orderbook Snapshots
    op.create_table(
        "orderbook_snapshots",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("outcome_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("outcomes.id"), nullable=False),
        sa.Column("bids", postgresql.JSONB, nullable=True),
        sa.Column("asks", postgresql.JSONB, nullable=True),
        sa.Column("spread", sa.Numeric(10, 6), nullable=True),
        sa.Column("depth_bid_10pct", sa.Numeric(20, 2), nullable=True),
        sa.Column("depth_ask_10pct", sa.Numeric(20, 2), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ob_snap_outcome_time", "orderbook_snapshots", ["outcome_id", "captured_at"])

    # Signals
    op.create_table(
        "signals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("signal_type", sa.String(64), nullable=False),
        sa.Column("market_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("outcome_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dedupe_bucket", sa.DateTime(timezone=True), nullable=True),
        sa.Column("signal_score", sa.Numeric(5, 3), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 3), nullable=False),
        sa.Column("rank_score", sa.Numeric(5, 3), nullable=False),
        sa.Column("details", postgresql.JSONB, nullable=False),
        sa.Column("price_at_fire", sa.Numeric(10, 6), nullable=True),
        sa.Column("resolved", sa.Boolean, default=False),
    )
    op.create_index("uq_signal_dedupe", "signals", ["signal_type", "outcome_id", "dedupe_bucket"], unique=True)
    op.create_index("ix_signal_fired", "signals", ["fired_at"])
    op.create_index("ix_signal_market", "signals", ["market_id", "fired_at"])
    op.create_index("ix_signal_type", "signals", ["signal_type", "fired_at"])
    op.create_index("ix_signal_rank", "signals", ["rank_score"])

    # Signal Evaluations
    op.create_table(
        "signal_evaluations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("signal_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("signals.id"), nullable=False),
        sa.Column("horizon", sa.String(8), nullable=False),
        sa.Column("price_at_eval", sa.Numeric(10, 6), nullable=True),
        sa.Column("price_change", sa.Numeric(10, 6), nullable=True),
        sa.Column("price_change_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("signal_id", "horizon", name="uq_eval_signal_horizon"),
    )

    # Ingestion Runs
    op.create_table(
        "ingestion_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_type", sa.String(64), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(16), default="running"),
        sa.Column("markets_processed", sa.Integer, default=0),
        sa.Column("error", sa.Text, nullable=True),
    )


def downgrade() -> None:
    op.drop_table("ingestion_runs")
    op.drop_table("signal_evaluations")
    op.drop_table("signals")
    op.drop_table("orderbook_snapshots")
    op.drop_table("price_snapshots")
    op.drop_table("outcomes")
    op.drop_table("markets")
