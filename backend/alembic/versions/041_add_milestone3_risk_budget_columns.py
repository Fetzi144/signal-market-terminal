"""Add Milestone 3 risk budget runtime columns.

Revision ID: 041
Revises: 040
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "041"
down_revision = "040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("capital_reservations", sa.Column("strategy_family", sa.String(length=32), nullable=True))
    op.add_column("capital_reservations", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.add_column("capital_reservations", sa.Column("regime_label", sa.String(length=32), nullable=True))
    op.add_column("capital_reservations", sa.Column("budget_metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_foreign_key(
        "fk_capital_reservations_strategy_version_id",
        "capital_reservations",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_capital_reservations_strategy_observed", "capital_reservations", ["strategy_family", "observed_at_local"])
    op.create_index("ix_capital_reservations_strategy_version_observed", "capital_reservations", ["strategy_version_id", "observed_at_local"])

    op.add_column("portfolio_exposure_snapshots", sa.Column("strategy_family", sa.String(length=32), nullable=True))
    op.add_column("portfolio_exposure_snapshots", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.add_column("portfolio_exposure_snapshots", sa.Column("regime_label", sa.String(length=32), nullable=True))
    op.add_column("portfolio_exposure_snapshots", sa.Column("budget_metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_foreign_key(
        "fk_portfolio_exposure_snapshots_strategy_version_id",
        "portfolio_exposure_snapshots",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_portfolio_exposure_snapshots_strategy_snapshot", "portfolio_exposure_snapshots", ["strategy_family", "snapshot_at"])
    op.create_index("ix_portfolio_exposure_snapshots_strategy_version_snapshot", "portfolio_exposure_snapshots", ["strategy_version_id", "snapshot_at"])

    op.add_column("portfolio_optimizer_recommendations", sa.Column("strategy_family", sa.String(length=32), nullable=True))
    op.add_column("portfolio_optimizer_recommendations", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.add_column("portfolio_optimizer_recommendations", sa.Column("regime_label", sa.String(length=32), nullable=True))
    op.add_column("portfolio_optimizer_recommendations", sa.Column("budget_metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_foreign_key(
        "fk_portfolio_optimizer_recommendations_strategy_version_id",
        "portfolio_optimizer_recommendations",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_portfolio_optimizer_recommendations_strategy_observed", "portfolio_optimizer_recommendations", ["strategy_family", "observed_at_local"])
    op.create_index("ix_portfolio_optimizer_recs_strategy_version_observed", "portfolio_optimizer_recommendations", ["strategy_version_id", "observed_at_local"])

    op.add_column("inventory_control_snapshots", sa.Column("strategy_family", sa.String(length=32), nullable=True))
    op.add_column("inventory_control_snapshots", sa.Column("strategy_version_id", sa.Integer(), nullable=True))
    op.add_column("inventory_control_snapshots", sa.Column("regime_label", sa.String(length=32), nullable=True))
    op.add_column("inventory_control_snapshots", sa.Column("budget_metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_foreign_key(
        "fk_inventory_control_snapshots_strategy_version_id",
        "inventory_control_snapshots",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_inventory_control_snapshots_strategy_snapshot", "inventory_control_snapshots", ["strategy_family", "snapshot_at"])
    op.create_index("ix_inventory_control_snapshots_strategy_version_snapshot", "inventory_control_snapshots", ["strategy_version_id", "snapshot_at"])

    op.execute(
        """
        UPDATE capital_reservations AS reservation
        SET strategy_family = live_orders.strategy_family,
            strategy_version_id = live_orders.strategy_version_id
        FROM live_orders
        WHERE reservation.live_order_id = live_orders.id
        """
    )


def downgrade() -> None:
    op.drop_index("ix_inventory_control_snapshots_strategy_version_snapshot", table_name="inventory_control_snapshots")
    op.drop_index("ix_inventory_control_snapshots_strategy_snapshot", table_name="inventory_control_snapshots")
    op.drop_constraint("fk_inventory_control_snapshots_strategy_version_id", "inventory_control_snapshots", type_="foreignkey")
    op.drop_column("inventory_control_snapshots", "budget_metadata_json")
    op.drop_column("inventory_control_snapshots", "regime_label")
    op.drop_column("inventory_control_snapshots", "strategy_version_id")
    op.drop_column("inventory_control_snapshots", "strategy_family")

    op.drop_index("ix_portfolio_optimizer_recs_strategy_version_observed", table_name="portfolio_optimizer_recommendations")
    op.drop_index("ix_portfolio_optimizer_recommendations_strategy_observed", table_name="portfolio_optimizer_recommendations")
    op.drop_constraint("fk_portfolio_optimizer_recommendations_strategy_version_id", "portfolio_optimizer_recommendations", type_="foreignkey")
    op.drop_column("portfolio_optimizer_recommendations", "budget_metadata_json")
    op.drop_column("portfolio_optimizer_recommendations", "regime_label")
    op.drop_column("portfolio_optimizer_recommendations", "strategy_version_id")
    op.drop_column("portfolio_optimizer_recommendations", "strategy_family")

    op.drop_index("ix_portfolio_exposure_snapshots_strategy_version_snapshot", table_name="portfolio_exposure_snapshots")
    op.drop_index("ix_portfolio_exposure_snapshots_strategy_snapshot", table_name="portfolio_exposure_snapshots")
    op.drop_constraint("fk_portfolio_exposure_snapshots_strategy_version_id", "portfolio_exposure_snapshots", type_="foreignkey")
    op.drop_column("portfolio_exposure_snapshots", "budget_metadata_json")
    op.drop_column("portfolio_exposure_snapshots", "regime_label")
    op.drop_column("portfolio_exposure_snapshots", "strategy_version_id")
    op.drop_column("portfolio_exposure_snapshots", "strategy_family")

    op.drop_index("ix_capital_reservations_strategy_version_observed", table_name="capital_reservations")
    op.drop_index("ix_capital_reservations_strategy_observed", table_name="capital_reservations")
    op.drop_constraint("fk_capital_reservations_strategy_version_id", "capital_reservations", type_="foreignkey")
    op.drop_column("capital_reservations", "budget_metadata_json")
    op.drop_column("capital_reservations", "regime_label")
    op.drop_column("capital_reservations", "strategy_version_id")
    op.drop_column("capital_reservations", "strategy_family")
