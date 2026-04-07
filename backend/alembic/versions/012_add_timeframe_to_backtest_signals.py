"""Add timeframe column to backtest_signals table.

Revision ID: 012
Revises: 011
"""
from alembic import op
import sqlalchemy as sa

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_signals",
        sa.Column("timeframe", sa.String(8), nullable=False, server_default="30m"),
    )
    op.create_index("ix_bt_signal_timeframe", "backtest_signals", ["timeframe"])


def downgrade() -> None:
    op.drop_index("ix_bt_signal_timeframe", table_name="backtest_signals")
    op.drop_column("backtest_signals", "timeframe")
