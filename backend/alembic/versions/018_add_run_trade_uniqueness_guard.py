"""Add per-run paper-trade uniqueness guard.

Revision ID: 018
Revises: 017
"""
import sqlalchemy as sa

from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_paper_trades_strategy_run_signal",
        "paper_trades",
        ["strategy_run_id", "signal_id"],
        unique=True,
        postgresql_where=sa.text("strategy_run_id IS NOT NULL"),
        sqlite_where=sa.text("strategy_run_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_paper_trades_strategy_run_signal", table_name="paper_trades")
