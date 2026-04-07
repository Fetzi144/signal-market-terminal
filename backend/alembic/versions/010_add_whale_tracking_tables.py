"""add wallet_profiles and wallet_activities tables for whale tracking

Revision ID: 010
Revises: 009
Create Date: 2026-04-07
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "wallet_profiles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("address", sa.String(42), nullable=False, unique=True),
        sa.Column("label", sa.String(128), nullable=True),
        sa.Column("total_volume", sa.Numeric(20, 2), nullable=False, server_default="0"),
        sa.Column("win_rate", sa.Numeric(5, 4), nullable=True),
        sa.Column("trade_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_active", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tracked", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_wallet_tracked", "wallet_profiles", ["tracked"])
    op.create_index("ix_wallet_volume", "wallet_profiles", ["total_volume"])

    op.create_table(
        "wallet_activities",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("wallet_id", UUID(as_uuid=True), sa.ForeignKey("wallet_profiles.id"), nullable=False),
        sa.Column("outcome_id", UUID(as_uuid=True), sa.ForeignKey("outcomes.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Numeric(20, 6), nullable=False),
        sa.Column("price", sa.Numeric(10, 6), nullable=True),
        sa.Column("notional_usd", sa.Numeric(20, 2), nullable=False, server_default="0"),
        sa.Column("tx_hash", sa.String(66), nullable=False, unique=True),
        sa.Column("block_number", sa.Integer, nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_activity_wallet", "wallet_activities", ["wallet_id", "timestamp"])
    op.create_index("ix_activity_outcome", "wallet_activities", ["outcome_id"])
    op.create_index("ix_activity_timestamp", "wallet_activities", ["timestamp"])


def downgrade() -> None:
    op.drop_index("ix_activity_timestamp", table_name="wallet_activities")
    op.drop_index("ix_activity_outcome", table_name="wallet_activities")
    op.drop_index("ix_activity_wallet", table_name="wallet_activities")
    op.drop_table("wallet_activities")
    op.drop_index("ix_wallet_volume", table_name="wallet_profiles")
    op.drop_index("ix_wallet_tracked", table_name="wallet_profiles")
    op.drop_table("wallet_profiles")
