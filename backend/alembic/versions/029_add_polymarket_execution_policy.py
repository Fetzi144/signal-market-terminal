"""Add Phase 6 Polymarket execution policy layer.

Revision ID: 029
Revises: 028
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_execution_action_candidates",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("signal_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("execution_decision_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("outcome_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("order_type_hint", sa.String(length=16), nullable=True),
        sa.Column("decision_horizon_ms", sa.Integer(), nullable=True),
        sa.Column("target_size", sa.Numeric(18, 4), nullable=False),
        sa.Column("est_fillable_size", sa.Numeric(18, 4), nullable=True),
        sa.Column("est_fill_probability", sa.Numeric(10, 6), nullable=True),
        sa.Column("est_avg_entry_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_worst_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_tick_size", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_min_order_size", sa.Numeric(24, 8), nullable=True),
        sa.Column("est_taker_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_maker_fee", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_slippage_cost", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_alpha_capture_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_adverse_selection_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_net_ev_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("est_net_ev_total", sa.Numeric(18, 8), nullable=True),
        sa.Column("valid", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("invalid_reason", sa.String(length=128), nullable=True),
        sa.Column("policy_version", sa.String(length=64), nullable=True),
        sa.Column("source_recon_state_id", sa.Integer(), nullable=True),
        sa.Column("source_feature_row_id", sa.Integer(), nullable=True),
        sa.Column("source_label_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["execution_decision_id"], ["execution_decisions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["outcome_id"], ["outcomes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_recon_state_id"], ["polymarket_book_recon_state.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_pm_execution_action_candidates_asset_decided",
        "polymarket_execution_action_candidates",
        ["asset_id", "decided_at"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_condition_decided",
        "polymarket_execution_action_candidates",
        ["condition_id", "decided_at"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_execution_decision_id",
        "polymarket_execution_action_candidates",
        ["execution_decision_id"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_signal_id",
        "polymarket_execution_action_candidates",
        ["signal_id"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_action_type",
        "polymarket_execution_action_candidates",
        ["action_type"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_valid",
        "polymarket_execution_action_candidates",
        ["valid"],
    )
    op.create_index(
        "ix_pm_execution_action_candidates_invalid_reason",
        "polymarket_execution_action_candidates",
        ["invalid_reason"],
    )

    op.add_column("execution_decisions", sa.Column("chosen_action_type", sa.String(length=32), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_order_type_hint", sa.String(length=16), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_target_price", sa.Numeric(18, 8), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_target_size", sa.Numeric(18, 4), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_fillable_size", sa.Numeric(18, 4), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_fill_probability", sa.Numeric(10, 6), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_net_ev_bps", sa.Numeric(18, 8), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_net_ev_total", sa.Numeric(18, 8), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_fee", sa.Numeric(18, 8), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_est_slippage", sa.Numeric(18, 8), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_policy_version", sa.String(length=64), nullable=True))
    op.add_column("execution_decisions", sa.Column("chosen_action_candidate_id", sa.Integer(), nullable=True))
    op.add_column(
        "execution_decisions",
        sa.Column("decision_reason_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_foreign_key(
        "fk_execution_decisions_chosen_action_candidate_id",
        "execution_decisions",
        "polymarket_execution_action_candidates",
        ["chosen_action_candidate_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_execution_decisions_chosen_action_type", "execution_decisions", ["chosen_action_type"])


def downgrade() -> None:
    op.drop_index("ix_execution_decisions_chosen_action_type", table_name="execution_decisions")
    op.drop_constraint(
        "fk_execution_decisions_chosen_action_candidate_id",
        "execution_decisions",
        type_="foreignkey",
    )
    op.drop_column("execution_decisions", "decision_reason_json")
    op.drop_column("execution_decisions", "chosen_action_candidate_id")
    op.drop_column("execution_decisions", "chosen_policy_version")
    op.drop_column("execution_decisions", "chosen_est_slippage")
    op.drop_column("execution_decisions", "chosen_est_fee")
    op.drop_column("execution_decisions", "chosen_est_net_ev_total")
    op.drop_column("execution_decisions", "chosen_est_net_ev_bps")
    op.drop_column("execution_decisions", "chosen_est_fill_probability")
    op.drop_column("execution_decisions", "chosen_est_fillable_size")
    op.drop_column("execution_decisions", "chosen_target_size")
    op.drop_column("execution_decisions", "chosen_target_price")
    op.drop_column("execution_decisions", "chosen_order_type_hint")
    op.drop_column("execution_decisions", "chosen_action_type")

    op.drop_index(
        "ix_pm_execution_action_candidates_invalid_reason",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_valid",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_action_type",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_signal_id",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_execution_decision_id",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_condition_decided",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_index(
        "ix_pm_execution_action_candidates_asset_decided",
        table_name="polymarket_execution_action_candidates",
    )
    op.drop_table("polymarket_execution_action_candidates")
