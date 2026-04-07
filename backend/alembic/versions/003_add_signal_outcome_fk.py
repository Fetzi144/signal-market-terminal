"""Add foreign key constraint on signals.outcome_id -> outcomes.id.

Revision ID: 003
Revises: 002
Create Date: 2026-04-07
"""
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Clean up any orphan signals pointing to non-existent outcomes
    op.execute(
        "UPDATE signals SET outcome_id = NULL "
        "WHERE outcome_id IS NOT NULL "
        "AND outcome_id NOT IN (SELECT id FROM outcomes)"
    )
    op.create_foreign_key(
        "fk_signal_outcome_id",
        "signals",
        "outcomes",
        ["outcome_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_signal_outcome_id", "signals", type_="foreignkey")
