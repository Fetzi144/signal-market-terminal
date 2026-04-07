"""add positions and trades tables for portfolio tracking

Revision ID: 008
Revises: 007
Create Date: 2026-04-07
"""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "positions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("market_id", UUID(as_uuid=True), sa.ForeignKey("markets.id"), nullable=False),
        sa.Column("outcome_id", UUID(as_uuid=True), sa.ForeignKey("outcomes.id"), nullable=False),
        sa.Column("platform", sa.String(64), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("avg_entry_price", sa.Float, nullable=False),
        sa.Column("current_price", sa.Float, nullable=True),
        sa.Column("unrealized_pnl", sa.Float, nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("exit_price", sa.Float, nullable=True),
        sa.Column("realized_pnl", sa.Float, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("signal_id", UUID(as_uuid=True), sa.ForeignKey("signals.id", ondelete="SET NULL"), nullable=True),
    )
    op.create_index("ix_position_status", "positions", ["status"])
    op.create_index("ix_position_market", "positions", ["market_id"])
    op.create_index("ix_position_platform", "positions", ["platform"])

    op.create_table(
        "trades",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("position_id", UUID(as_uuid=True), sa.ForeignKey("positions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("action", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=False),
        sa.Column("fees", sa.Float, nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_table("trades")
    op.drop_index("ix_position_platform", table_name="positions")
    op.drop_index("ix_position_market", table_name="positions")
    op.drop_index("ix_position_status", table_name="positions")
    op.drop_table("positions")
