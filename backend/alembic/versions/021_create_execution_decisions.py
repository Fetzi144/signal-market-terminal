"""Create execution decisions table for Phase 0.

Revision ID: 021
Revises: 020
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def _json_type() -> sa.JSON:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "execution_decisions",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("signal_id", sa.Uuid(as_uuid=True), sa.ForeignKey("signals.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "strategy_run_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("strategy_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("decision_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decision_status", sa.String(length=16), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("direction", sa.String(length=16), nullable=True),
        sa.Column("ideal_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("executable_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("requested_size_usd", sa.Numeric(18, 4), nullable=True),
        sa.Column("fillable_size_usd", sa.Numeric(18, 4), nullable=True),
        sa.Column("fill_probability", sa.Numeric(10, 6), nullable=True),
        sa.Column("net_ev_per_share", sa.Numeric(18, 8), nullable=True),
        sa.Column("net_expected_pnl_usd", sa.Numeric(18, 8), nullable=True),
        sa.Column("missing_orderbook_context", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("stale_orderbook_context", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("liquidity_constrained", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("fill_status", sa.String(length=32), nullable=True),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("details", _json_type(), nullable=False, server_default="{}"),
        sa.UniqueConstraint("signal_id", "strategy_run_id", name="uq_execution_decisions_signal_strategy_run"),
    )
    op.create_index(
        "ix_execution_decisions_strategy_run_decision_at",
        "execution_decisions",
        ["strategy_run_id", "decision_at"],
    )
    op.create_index("ix_execution_decisions_reason_code", "execution_decisions", ["reason_code"])
    op.create_index("ix_execution_decisions_fill_status", "execution_decisions", ["fill_status"])


def downgrade() -> None:
    op.drop_index("ix_execution_decisions_fill_status", table_name="execution_decisions")
    op.drop_index("ix_execution_decisions_reason_code", table_name="execution_decisions")
    op.drop_index("ix_execution_decisions_strategy_run_decision_at", table_name="execution_decisions")
    op.drop_table("execution_decisions")
