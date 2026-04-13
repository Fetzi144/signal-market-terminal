"""Add scheduler leases for cross-process ownership.

Revision ID: 019
Revises: 018
"""
import sqlalchemy as sa

from alembic import op

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduler_leases",
        sa.Column("scheduler_name", sa.String(length=64), primary_key=True),
        sa.Column("owner_token", sa.String(length=128), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("scheduler_leases")
