"""add question_slug to market

Revision ID: 005
Revises: 004
Create Date: 2026-04-07
"""
import sqlalchemy as sa

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("markets", sa.Column("question_slug", sa.String(512), nullable=True))
    op.create_index("ix_market_question_slug", "markets", ["question_slug"])


def downgrade() -> None:
    op.drop_index("ix_market_question_slug", table_name="markets")
    op.drop_column("markets", "question_slug")
