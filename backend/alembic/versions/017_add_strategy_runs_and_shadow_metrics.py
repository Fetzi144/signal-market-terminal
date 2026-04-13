"""Add strategy runs and paper-trade shadow execution fields.

Revision ID: 017
Revises: 016
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("contract_snapshot", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_strategy_runs_name_status", "strategy_runs", ["strategy_name", "status"])
    op.create_index("ix_strategy_runs_created", "strategy_runs", ["created_at"])
    op.create_index(
        "uq_strategy_runs_active_name",
        "strategy_runs",
        ["strategy_name"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.add_column(
        "paper_trades",
        sa.Column("strategy_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column("paper_trades", sa.Column("shadow_entry_price", sa.Numeric(10, 6), nullable=True))
    op.add_column("paper_trades", sa.Column("shadow_pnl", sa.Numeric(12, 2), nullable=True))
    op.create_foreign_key(
        "fk_paper_trades_strategy_run_id",
        "paper_trades",
        "strategy_runs",
        ["strategy_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_paper_trades_strategy_run", "paper_trades", ["strategy_run_id", "opened_at"])


def downgrade() -> None:
    op.drop_index("ix_paper_trades_strategy_run", table_name="paper_trades")
    op.drop_constraint("fk_paper_trades_strategy_run_id", "paper_trades", type_="foreignkey")
    op.drop_column("paper_trades", "shadow_pnl")
    op.drop_column("paper_trades", "shadow_entry_price")
    op.drop_column("paper_trades", "strategy_run_id")

    op.drop_index("uq_strategy_runs_active_name", table_name="strategy_runs")
    op.drop_index("ix_strategy_runs_created", table_name="strategy_runs")
    op.drop_index("ix_strategy_runs_name_status", table_name="strategy_runs")
    op.drop_table("strategy_runs")
