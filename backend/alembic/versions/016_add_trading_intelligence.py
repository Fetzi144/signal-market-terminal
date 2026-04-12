"""Add trading intelligence: expected_value column on signals + paper_trades table.

Revision ID: 016
Revises: 015
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add expected_value to signals
    op.add_column("signals", sa.Column("expected_value", sa.Numeric(10, 6), nullable=True))

    # Create paper_trades table
    op.create_table(
        "paper_trades",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("signal_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("signals.id"), nullable=False),
        sa.Column("outcome_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("market_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("direction", sa.String(16), nullable=False),  # buy_yes / buy_no
        sa.Column("entry_price", sa.Numeric(10, 6), nullable=False),
        sa.Column("size_usd", sa.Numeric(12, 2), nullable=False),
        sa.Column("shares", sa.Numeric(12, 4), nullable=False),
        sa.Column("exit_price", sa.Numeric(10, 6), nullable=True),
        sa.Column("pnl", sa.Numeric(12, 2), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", postgresql.JSONB, nullable=False, server_default="{}"),
    )
    op.create_index("ix_paper_trades_status", "paper_trades", ["status"])
    op.create_index("ix_paper_trades_outcome", "paper_trades", ["outcome_id"])
    op.create_index("ix_paper_trades_opened", "paper_trades", ["opened_at"])


def downgrade() -> None:
    op.drop_table("paper_trades")
    op.drop_column("signals", "expected_value")
