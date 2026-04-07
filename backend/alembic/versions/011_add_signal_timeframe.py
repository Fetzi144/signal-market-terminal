"""Add timeframe column to signals table and update dedupe index.

Revision ID: 011
Revises: 010
"""
import sqlalchemy as sa

from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add timeframe column with default '30m' for existing signals
    op.add_column(
        "signals",
        sa.Column("timeframe", sa.String(8), nullable=False, server_default="30m"),
    )

    # Drop old dedupe index
    op.drop_index("uq_signal_dedupe", table_name="signals")

    # Create new dedupe index that includes timeframe
    op.create_index(
        "uq_signal_dedupe",
        "signals",
        ["signal_type", "outcome_id", "timeframe", "dedupe_bucket"],
        unique=True,
    )

    # Index for timeframe filtering
    op.create_index("ix_signal_timeframe", "signals", ["timeframe"])


def downgrade() -> None:
    op.drop_index("ix_signal_timeframe", table_name="signals")
    op.drop_index("uq_signal_dedupe", table_name="signals")
    op.create_index(
        "uq_signal_dedupe",
        "signals",
        ["signal_type", "outcome_id", "dedupe_bucket"],
        unique=True,
    )
    op.drop_column("signals", "timeframe")
