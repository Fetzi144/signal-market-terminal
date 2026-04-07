"""add resolved_correctly to signal

Revision ID: 004
Revises: 003
Create Date: 2026-04-07
"""
import sqlalchemy as sa

from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("signals", sa.Column("resolved_correctly", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("signals", "resolved_correctly")
