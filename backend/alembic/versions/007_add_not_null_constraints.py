"""enforce NOT NULL on status/markets_processed/active/resolved/alerted

Revision ID: 007
Revises: 006
Create Date: 2026-04-07
"""

from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fill any existing NULLs with the column defaults before tightening constraints
    op.execute("UPDATE ingestion_runs SET status = 'running' WHERE status IS NULL")
    op.execute("UPDATE ingestion_runs SET markets_processed = 0 WHERE markets_processed IS NULL")
    op.execute("UPDATE markets SET active = TRUE WHERE active IS NULL")
    op.execute("UPDATE signals SET resolved = FALSE WHERE resolved IS NULL")
    op.execute("UPDATE signals SET alerted = FALSE WHERE alerted IS NULL")

    op.alter_column("ingestion_runs", "status", nullable=False)
    op.alter_column("ingestion_runs", "markets_processed", nullable=False)
    op.alter_column("markets", "active", nullable=False)
    op.alter_column("signals", "resolved", nullable=False)
    op.alter_column("signals", "alerted", nullable=False)


def downgrade() -> None:
    op.alter_column("ingestion_runs", "status", nullable=True)
    op.alter_column("ingestion_runs", "markets_processed", nullable=True)
    op.alter_column("markets", "active", nullable=True)
    op.alter_column("signals", "resolved", nullable=True)
    op.alter_column("signals", "alerted", nullable=True)
