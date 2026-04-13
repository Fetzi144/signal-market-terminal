"""Link paper trades to execution decisions.

Revision ID: 022
Revises: 021
"""
import sqlalchemy as sa

from alembic import op

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("paper_trades") as batch_op:
        batch_op.add_column(sa.Column("execution_decision_id", sa.Uuid(as_uuid=True), nullable=True))
        batch_op.add_column(sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_foreign_key(
            "fk_paper_trades_execution_decision_id",
            "execution_decisions",
            ["execution_decision_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_unique_constraint(
            "uq_paper_trades_execution_decision_id",
            ["execution_decision_id"],
        )

    op.create_index("ix_paper_trades_submitted_at", "paper_trades", ["submitted_at"])
    op.create_index("ix_paper_trades_execution_decision_id", "paper_trades", ["execution_decision_id"])


def downgrade() -> None:
    op.drop_index("ix_paper_trades_execution_decision_id", table_name="paper_trades")
    op.drop_index("ix_paper_trades_submitted_at", table_name="paper_trades")

    with op.batch_alter_table("paper_trades") as batch_op:
        batch_op.drop_constraint("uq_paper_trades_execution_decision_id", type_="unique")
        batch_op.drop_constraint("fk_paper_trades_execution_decision_id", type_="foreignkey")
        batch_op.drop_column("confirmed_at")
        batch_op.drop_column("submitted_at")
        batch_op.drop_column("execution_decision_id")
