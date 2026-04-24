"""Add composite signal feed sort index.

Revision ID: 042
Revises: 041
"""

from alembic import op


revision = "042"
down_revision = "041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.create_index(
                "ix_signal_rank_fired",
                "signals",
                ["rank_score", "fired_at"],
                postgresql_concurrently=True,
            )
        return
    op.create_index("ix_signal_rank_fired", "signals", ["rank_score", "fired_at"])


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.drop_index(
                "ix_signal_rank_fired",
                table_name="signals",
                postgresql_concurrently=True,
            )
        return
    op.drop_index("ix_signal_rank_fired", table_name="signals")
