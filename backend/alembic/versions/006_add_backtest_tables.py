"""add backtest_runs and backtest_signals tables

Revision ID: 006
Revises: 005
Create Date: 2026-04-07
"""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detector_configs", JSONB, nullable=True),
        sa.Column("rank_threshold", sa.Float, nullable=False, server_default="0.5"),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_summary", JSONB, nullable=True),
    )
    op.create_index("ix_backtest_run_status", "backtest_runs", ["status"])
    op.create_index("ix_backtest_run_created", "backtest_runs", ["created_at"])

    op.create_table(
        "backtest_signals",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("backtest_run_id", UUID(as_uuid=True), sa.ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("signal_type", sa.String(64), nullable=False),
        sa.Column("outcome_id", UUID(as_uuid=True), sa.ForeignKey("outcomes.id", ondelete="SET NULL"), nullable=True),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_score", sa.Numeric(5, 3), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 3), nullable=False),
        sa.Column("rank_score", sa.Numeric(5, 3), nullable=False),
        sa.Column("resolved_correctly", sa.Boolean, nullable=True),
        sa.Column("price_at_fire", sa.Numeric(10, 6), nullable=True),
        sa.Column("price_at_resolution", sa.Numeric(10, 6), nullable=True),
        sa.Column("details", JSONB, nullable=True),
    )
    op.create_index("ix_bt_signal_run", "backtest_signals", ["backtest_run_id", "fired_at"])
    op.create_index("ix_bt_signal_type", "backtest_signals", ["signal_type"])


def downgrade() -> None:
    op.drop_table("backtest_signals")
    op.drop_table("backtest_runs")
