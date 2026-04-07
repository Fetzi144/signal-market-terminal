"""Add alerted column to signals table.

Revision ID: 002
Revises: 001
Create Date: 2026-04-07
"""
from alembic import op
import sqlalchemy as sa

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("signals", sa.Column("alerted", sa.Boolean, server_default="false"))


def downgrade() -> None:
    op.drop_column("signals", "alerted")
