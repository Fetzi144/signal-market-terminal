"""Convert portfolio Float columns to Numeric(20,8) for Decimal precision.

Revision ID: 013
Revises: 012
"""
import sqlalchemy as sa

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None

# Columns to convert: (table, column)
_POSITION_COLS = [
    "quantity",
    "avg_entry_price",
    "current_price",
    "unrealized_pnl",
    "exit_price",
    "realized_pnl",
]
_TRADE_COLS = ["quantity", "price", "fees"]


def upgrade() -> None:
    for col in _POSITION_COLS:
        op.alter_column(
            "positions",
            col,
            type_=sa.Numeric(20, 8),
            existing_type=sa.Float,
            existing_nullable=col in ("current_price", "unrealized_pnl", "exit_price", "realized_pnl"),
        )
    for col in _TRADE_COLS:
        op.alter_column(
            "trades",
            col,
            type_=sa.Numeric(20, 8),
            existing_type=sa.Float,
            existing_nullable=False,
        )


def downgrade() -> None:
    for col in _POSITION_COLS:
        op.alter_column(
            "positions",
            col,
            type_=sa.Float,
            existing_type=sa.Numeric(20, 8),
            existing_nullable=col in ("current_price", "unrealized_pnl", "exit_price", "realized_pnl"),
        )
    for col in _TRADE_COLS:
        op.alter_column(
            "trades",
            col,
            type_=sa.Float,
            existing_type=sa.Numeric(20, 8),
            existing_nullable=False,
        )
