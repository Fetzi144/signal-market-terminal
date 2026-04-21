"""Add Phase 13A lifecycle attribution for pilot incidents and guardrails.

Revision ID: 040
Revises: 039
"""

from alembic import op
import sqlalchemy as sa


revision = "040"
down_revision = "039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "polymarket_control_plane_incidents",
        sa.Column("strategy_version_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_pm_control_incidents_strategy_version_id",
        "polymarket_control_plane_incidents",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pm_control_incidents_strategy_version_observed",
        "polymarket_control_plane_incidents",
        ["strategy_version_id", "observed_at_local"],
    )

    op.add_column(
        "polymarket_pilot_guardrail_events",
        sa.Column("strategy_version_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_pm_guardrail_events_strategy_version_id",
        "polymarket_pilot_guardrail_events",
        "strategy_versions",
        ["strategy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pm_guardrail_events_strategy_version_observed",
        "polymarket_pilot_guardrail_events",
        ["strategy_version_id", "observed_at_local"],
    )

    op.execute(
        """
        UPDATE polymarket_control_plane_incidents AS incident
        SET strategy_version_id = live_orders.strategy_version_id
        FROM live_orders
        WHERE incident.strategy_version_id IS NULL
          AND incident.live_order_id = live_orders.id
          AND live_orders.strategy_version_id IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE polymarket_pilot_guardrail_events AS guardrail
        SET strategy_version_id = live_orders.strategy_version_id
        FROM live_orders
        WHERE guardrail.strategy_version_id IS NULL
          AND guardrail.live_order_id = live_orders.id
          AND live_orders.strategy_version_id IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE polymarket_control_plane_incidents AS incident
        SET strategy_version_id = version_lookup.id
        FROM live_orders
        JOIN (
            SELECT family_registry.family, MIN(strategy_versions.id) AS id
            FROM strategy_versions
            JOIN strategy_families_registry AS family_registry
                ON family_registry.id = strategy_versions.family_id
            WHERE strategy_versions.is_current IS TRUE
            GROUP BY family_registry.family
        ) AS version_lookup
            ON version_lookup.family = live_orders.strategy_family
        WHERE incident.strategy_version_id IS NULL
          AND incident.live_order_id = live_orders.id
          AND live_orders.strategy_family IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE polymarket_pilot_guardrail_events AS guardrail
        SET strategy_version_id = version_lookup.id
        FROM live_orders
        JOIN (
            SELECT family_registry.family, MIN(strategy_versions.id) AS id
            FROM strategy_versions
            JOIN strategy_families_registry AS family_registry
                ON family_registry.id = strategy_versions.family_id
            WHERE strategy_versions.is_current IS TRUE
            GROUP BY family_registry.family
        ) AS version_lookup
            ON version_lookup.family = live_orders.strategy_family
        WHERE guardrail.strategy_version_id IS NULL
          AND guardrail.live_order_id = live_orders.id
          AND live_orders.strategy_family IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE polymarket_control_plane_incidents AS incident
        SET strategy_version_id = version_lookup.id
        FROM polymarket_pilot_runs AS pilot_run
        JOIN polymarket_pilot_configs AS pilot_config
            ON pilot_config.id = pilot_run.pilot_config_id
        JOIN (
            SELECT family_registry.family, MIN(strategy_versions.id) AS id
            FROM strategy_versions
            JOIN strategy_families_registry AS family_registry
                ON family_registry.id = strategy_versions.family_id
            WHERE strategy_versions.is_current IS TRUE
            GROUP BY family_registry.family
        ) AS version_lookup
            ON version_lookup.family = pilot_config.strategy_family
        WHERE incident.strategy_version_id IS NULL
          AND incident.pilot_run_id = pilot_run.id
        """
    )
    op.execute(
        """
        UPDATE polymarket_pilot_guardrail_events AS guardrail
        SET strategy_version_id = version_lookup.id
        FROM (
            SELECT family_registry.family, MIN(strategy_versions.id) AS id
            FROM strategy_versions
            JOIN strategy_families_registry AS family_registry
                ON family_registry.id = strategy_versions.family_id
            WHERE strategy_versions.is_current IS TRUE
            GROUP BY family_registry.family
        ) AS version_lookup
        WHERE guardrail.strategy_version_id IS NULL
          AND guardrail.strategy_family = version_lookup.family
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pm_guardrail_events_strategy_version_observed",
        table_name="polymarket_pilot_guardrail_events",
    )
    op.drop_constraint(
        "fk_pm_guardrail_events_strategy_version_id",
        "polymarket_pilot_guardrail_events",
        type_="foreignkey",
    )
    op.drop_column("polymarket_pilot_guardrail_events", "strategy_version_id")

    op.drop_index(
        "ix_pm_control_incidents_strategy_version_observed",
        table_name="polymarket_control_plane_incidents",
    )
    op.drop_constraint(
        "fk_pm_control_incidents_strategy_version_id",
        "polymarket_control_plane_incidents",
        type_="foreignkey",
    )
    op.drop_column("polymarket_control_plane_incidents", "strategy_version_id")
