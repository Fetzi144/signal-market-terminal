"""Add CLV and P&L columns to signals for profitability tracking.

Revision ID: 014
Revises: 013
"""
import sqlalchemy as sa

from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("signals", sa.Column("closing_price", sa.Numeric(10, 6), nullable=True))
    op.add_column("signals", sa.Column("resolution_price", sa.Numeric(10, 6), nullable=True))
    op.add_column("signals", sa.Column("clv", sa.Numeric(10, 6), nullable=True))
    op.add_column("signals", sa.Column("profit_loss", sa.Numeric(10, 6), nullable=True))


def downgrade() -> None:
    op.drop_column("signals", "profit_loss")
    op.drop_column("signals", "clv")
    op.drop_column("signals", "resolution_price")
    op.drop_column("signals", "closing_price")
