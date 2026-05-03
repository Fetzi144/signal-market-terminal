"""Add profitability read-path indexes.

Revision ID: 043
Revises: 042
"""

import sqlalchemy as sa
from alembic import op


revision = "043"
down_revision = "042"
branch_labels = None
depends_on = None


_INDEXES = (
    (
        "ix_signal_confluence_fired",
        ["fired_at"],
        "signal_type = 'confluence'",
    ),
    (
        "ix_signal_resolved_fired_type_rank",
        ["fired_at", "signal_type", "rank_score"],
        "resolved_correctly IS NOT NULL",
    ),
    (
        "ix_signal_qualified_type_fired_ev",
        ["signal_type", "fired_at", "expected_value"],
        (
            "outcome_id IS NOT NULL "
            "AND estimated_probability IS NOT NULL "
            "AND price_at_fire IS NOT NULL "
            "AND expected_value IS NOT NULL"
        ),
    ),
)


def upgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            for index_name, columns, predicate in _INDEXES:
                op.create_index(
                    index_name,
                    "signals",
                    columns,
                    postgresql_where=sa.text(predicate),
                    postgresql_concurrently=True,
                )
        return

    for index_name, columns, predicate in _INDEXES:
        op.create_index(
            index_name,
            "signals",
            columns,
            sqlite_where=sa.text(predicate),
        )


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            for index_name, _columns, _predicate in reversed(_INDEXES):
                op.drop_index(
                    index_name,
                    table_name="signals",
                    postgresql_concurrently=True,
                )
        return

    for index_name, _columns, _predicate in reversed(_INDEXES):
        op.drop_index(index_name, table_name="signals")
