"""Add persisted equity and drawdown state to strategy runs.

Revision ID: 038
Revises: 037
"""

from alembic import op
import sqlalchemy as sa


revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("strategy_runs", sa.Column("peak_equity", sa.Numeric(12, 2), nullable=True))
    op.add_column("strategy_runs", sa.Column("current_equity", sa.Numeric(12, 2), nullable=True))
    op.add_column("strategy_runs", sa.Column("max_drawdown", sa.Numeric(12, 2), nullable=True))
    op.add_column("strategy_runs", sa.Column("drawdown_pct", sa.Numeric(10, 6), nullable=True))


def downgrade() -> None:
    op.drop_column("strategy_runs", "drawdown_pct")
    op.drop_column("strategy_runs", "max_drawdown")
    op.drop_column("strategy_runs", "current_equity")
    op.drop_column("strategy_runs", "peak_equity")
