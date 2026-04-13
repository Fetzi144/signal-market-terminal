"""Add Phase 0 signal timing and source fields.

Revision ID: 020
Revises: 019
"""
import sqlalchemy as sa

from alembic import op

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("signals") as batch_op:
        batch_op.add_column(sa.Column("observed_at_exchange", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("received_at_local", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("detected_at_local", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("source_platform", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("source_token_id", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("source_stream_session_id", sa.Uuid(as_uuid=True), nullable=True))
        batch_op.add_column(sa.Column("source_event_hash", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("source_event_type", sa.String(length=64), nullable=True))

    op.create_index("ix_signal_observed_at_exchange", "signals", ["observed_at_exchange"])
    op.create_index("ix_signal_received_at_local", "signals", ["received_at_local"])
    op.create_index(
        "ix_signal_source_platform_observed_at_exchange",
        "signals",
        ["source_platform", "observed_at_exchange"],
    )
    op.create_index(
        "ix_signal_source_token_id_observed_at_exchange",
        "signals",
        ["source_token_id", "observed_at_exchange"],
    )
    op.create_index("ix_signal_source_stream_session_id", "signals", ["source_stream_session_id"])


def downgrade() -> None:
    op.drop_index("ix_signal_source_stream_session_id", table_name="signals")
    op.drop_index("ix_signal_source_token_id_observed_at_exchange", table_name="signals")
    op.drop_index("ix_signal_source_platform_observed_at_exchange", table_name="signals")
    op.drop_index("ix_signal_received_at_local", table_name="signals")
    op.drop_index("ix_signal_observed_at_exchange", table_name="signals")

    with op.batch_alter_table("signals") as batch_op:
        batch_op.drop_column("source_event_type")
        batch_op.drop_column("source_event_hash")
        batch_op.drop_column("source_stream_session_id")
        batch_op.drop_column("source_token_id")
        batch_op.drop_column("source_platform")
        batch_op.drop_column("detected_at_local")
        batch_op.drop_column("received_at_local")
        batch_op.drop_column("observed_at_exchange")
