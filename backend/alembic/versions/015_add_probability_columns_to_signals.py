"""Add probability engine columns to signals.

Revision ID: 015
Revises: 014
"""
import sqlalchemy as sa

from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("signals", sa.Column("estimated_probability", sa.Numeric(5, 4), nullable=True))
    op.add_column("signals", sa.Column("probability_adjustment", sa.Numeric(5, 4), nullable=True))


def downgrade() -> None:
    op.drop_column("signals", "probability_adjustment")
    op.drop_column("signals", "estimated_probability")
