"""Add Phase 13A strategy registry, gate policy, and evidence linkage.

Revision ID: 039
Revises: 038
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_families_registry",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("family", sa.String(length=64), nullable=False),
        sa.Column("label", sa.String(length=128), nullable=False),
        sa.Column("posture", sa.String(length=32), nullable=False),
        sa.Column("configured", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("review_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("primary_surface", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("disabled_reason", sa.Text(), nullable=True),
        sa.Column("family_kind", sa.String(length=32), nullable=False, server_default="strategy"),
        sa.Column("seeded_from", sa.String(length=32), nullable=False, server_default="builtin"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("family", name="uq_strategy_families_registry_family"),
    )
    op.create_index(
        "ix_strategy_families_registry_posture",
        "strategy_families_registry",
        ["posture", "updated_at"],
    )

    op.create_table(
        "strategy_versions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("family_id", sa.Integer(), nullable=False),
        sa.Column("version_key", sa.String(length=128), nullable=False),
        sa.Column("version_label", sa.String(length=128), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=True),
        sa.Column("version_status", sa.String(length=32), nullable=False),
        sa.Column("autonomy_tier", sa.String(length=32), nullable=False, server_default="shadow_only"),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_frozen", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("config_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("provenance_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["family_id"], ["strategy_families_registry.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("version_key", name="uq_strategy_versions_version_key"),
    )
    op.create_index(
        "ix_strategy_versions_family_current",
        "strategy_versions",
        ["family_id", "is_current", "updated_at"],
    )
    op.create_index("ix_strategy_versions_status", "strategy_versions", ["version_status", "updated_at"])

    op.create_table(
        "promotion_gate_policies",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("policy_key", sa.String(length=128), nullable=False),
        sa.Column("label", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("policy_key", name="uq_promotion_gate_policies_policy_key"),
    )
    op.create_index(
        "ix_promotion_gate_policies_status",
        "promotion_gate_policies",
        ["status", "updated_at"],
    )

    op.create_table(
        "promotion_evaluations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("family_id", sa.Integer(), nullable=False),
        sa.Column("strategy_version_id", sa.Integer(), nullable=False),
        sa.Column("gate_policy_id", sa.Integer(), nullable=True),
        sa.Column("evaluation_kind", sa.String(length=32), nullable=False),
        sa.Column("evaluation_status", sa.String(length=32), nullable=False),
        sa.Column("autonomy_tier", sa.String(length=32), nullable=False, server_default="shadow_only"),
        sa.Column("evaluation_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evaluation_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provenance_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["family_id"], ["strategy_families_registry.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["strategy_version_id"], ["strategy_versions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["gate_policy_id"], ["promotion_gate_policies.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_promotion_evaluations_version_created",
        "promotion_evaluations",
        ["strategy_version_id", "created_at"],
    )
    op.create_index(
        "ix_promotion_evaluations_family_created",
        "promotion_evaluations",
        ["family_id", "created_at"],
    )
    op.create_index(
        "ix_promotion_evaluations_status",
        "promotion_evaluations",
        ["evaluation_status", "created_at"],
    )

    op.create_table(
        "demotion_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("family_id", sa.Integer(), nullable=False),
        sa.Column("strategy_version_id", sa.Integer(), nullable=False),
        sa.Column("prior_autonomy_tier", sa.String(length=32), nullable=True),
        sa.Column("fallback_autonomy_tier", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=False),
        sa.Column("cooling_off_ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["family_id"], ["strategy_families_registry.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["strategy_version_id"], ["strategy_versions.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_demotion_events_version_observed",
        "demotion_events",
        ["strategy_version_id", "observed_at_local"],
    )
    op.create_index(
        "ix_demotion_events_family_observed",
        "demotion_events",
        ["family_id", "observed_at_local"],
    )
    op.create_index(
        "ix_demotion_events_reason_observed",
        "demotion_events",
        ["reason_code", "observed_at_local"],
    )

    op.add_column("strategy_runs", sa.Column("strategy_family", sa.String(length=64), nullable=True))
    op.add_column("strategy_runs", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_strategy_runs_strategy_version_id",
        "strategy_runs",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_strategy_runs_family_status", "strategy_runs", ["strategy_family", "status"])
    op.create_index(
        "ix_strategy_runs_strategy_version",
        "strategy_runs",
        ["strategy_version_id", "created_at"],
    )

    op.add_column("paper_trades", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_paper_trades_strategy_version_id",
        "paper_trades",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_paper_trades_strategy_version",
        "paper_trades",
        ["strategy_version_id", "opened_at"],
    )

    op.add_column("polymarket_replay_runs", sa.Column("strategy_family", sa.String(length=64), nullable=True))
    op.add_column("polymarket_replay_runs", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_pm_replay_runs_strategy_version_id",
        "polymarket_replay_runs",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pm_replay_runs_strategy_version_started",
        "polymarket_replay_runs",
        ["strategy_version_id", "started_at"],
    )

    op.add_column("live_orders", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_live_orders_strategy_version_id",
        "live_orders",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_live_orders_strategy_version_created",
        "live_orders",
        ["strategy_version_id", "created_at"],
    )

    op.add_column("polymarket_pilot_scorecards", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_pm_pilot_scorecards_strategy_version_id",
        "polymarket_pilot_scorecards",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pm_pilot_scorecards_strategy_version_created",
        "polymarket_pilot_scorecards",
        ["strategy_version_id", "created_at"],
    )

    op.add_column("polymarket_pilot_readiness_reports", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_pm_readiness_reports_strategy_version_id",
        "polymarket_pilot_readiness_reports",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pm_readiness_reports_strategy_version_generated",
        "polymarket_pilot_readiness_reports",
        ["strategy_version_id", "generated_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pm_readiness_reports_strategy_version_generated",
        table_name="polymarket_pilot_readiness_reports",
    )
    op.drop_constraint(
        "fk_pm_readiness_reports_strategy_version_id",
        "polymarket_pilot_readiness_reports",
        type_="foreignkey",
    )
    op.drop_column("polymarket_pilot_readiness_reports", "strategy_version_id")

    op.drop_index(
        "ix_pm_pilot_scorecards_strategy_version_created",
        table_name="polymarket_pilot_scorecards",
    )
    op.drop_constraint(
        "fk_pm_pilot_scorecards_strategy_version_id",
        "polymarket_pilot_scorecards",
        type_="foreignkey",
    )
    op.drop_column("polymarket_pilot_scorecards", "strategy_version_id")

    op.drop_index("ix_live_orders_strategy_version_created", table_name="live_orders")
    op.drop_constraint("fk_live_orders_strategy_version_id", "live_orders", type_="foreignkey")
    op.drop_column("live_orders", "strategy_version_id")

    op.drop_index("ix_pm_replay_runs_strategy_version_started", table_name="polymarket_replay_runs")
    op.drop_constraint(
        "fk_pm_replay_runs_strategy_version_id",
        "polymarket_replay_runs",
        type_="foreignkey",
    )
    op.drop_column("polymarket_replay_runs", "strategy_version_id")
    op.drop_column("polymarket_replay_runs", "strategy_family")

    op.drop_index("ix_paper_trades_strategy_version", table_name="paper_trades")
    op.drop_constraint("fk_paper_trades_strategy_version_id", "paper_trades", type_="foreignkey")
    op.drop_column("paper_trades", "strategy_version_id")

    op.drop_index("ix_strategy_runs_strategy_version", table_name="strategy_runs")
    op.drop_index("ix_strategy_runs_family_status", table_name="strategy_runs")
    op.drop_constraint("fk_strategy_runs_strategy_version_id", "strategy_runs", type_="foreignkey")
    op.drop_column("strategy_runs", "strategy_version_id")
    op.drop_column("strategy_runs", "strategy_family")

    op.drop_index("ix_demotion_events_reason_observed", table_name="demotion_events")
    op.drop_index("ix_demotion_events_family_observed", table_name="demotion_events")
    op.drop_index("ix_demotion_events_version_observed", table_name="demotion_events")
    op.drop_table("demotion_events")

    op.drop_index("ix_promotion_evaluations_status", table_name="promotion_evaluations")
    op.drop_index("ix_promotion_evaluations_family_created", table_name="promotion_evaluations")
    op.drop_index("ix_promotion_evaluations_version_created", table_name="promotion_evaluations")
    op.drop_table("promotion_evaluations")

    op.drop_index("ix_promotion_gate_policies_status", table_name="promotion_gate_policies")
    op.drop_table("promotion_gate_policies")

    op.drop_index("ix_strategy_versions_status", table_name="strategy_versions")
    op.drop_index("ix_strategy_versions_family_current", table_name="strategy_versions")
    op.drop_table("strategy_versions")

    op.drop_index("ix_strategy_families_registry_posture", table_name="strategy_families_registry")
    op.drop_table("strategy_families_registry")

