"""Add indexes for cleanup-heavy foreign keys.

Revision ID: 045
Revises: 044
"""

from alembic import op


revision = "045"
down_revision = "044"
branch_labels = None
depends_on = None


INDEXES = [
    ("ix_backtest_signals_outcome_id", "backtest_signals", "outcome_id"),
    ("ix_live_orders_outcome_id", "live_orders", "outcome_id"),
    ("ix_market_structure_opportunity_legs_outcome_id", "market_structure_opportunity_legs", "outcome_id"),
    ("ix_market_structure_paper_orders_outcome_id", "market_structure_paper_orders", "outcome_id"),
    ("ix_polymarket_execution_action_candidates_outcome_id", "polymarket_execution_action_candidates", "outcome_id"),
    ("ix_positions_outcome_id", "positions", "outcome_id"),
    ("ix_cross_venue_market_links_left_market_id", "cross_venue_market_links", "left_market_id"),
    ("ix_cross_venue_market_links_right_market_id", "cross_venue_market_links", "right_market_id"),
    ("ix_market_structure_group_members_market_id", "market_structure_group_members", "market_id"),
    ("ix_market_structure_opportunity_legs_market_id", "market_structure_opportunity_legs", "market_id"),
    ("ix_market_structure_paper_orders_market_id", "market_structure_paper_orders", "market_id"),
]


def upgrade() -> None:
    for name, table, column in INDEXES:
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({column})")


def downgrade() -> None:
    for name, _table, _column in reversed(INDEXES):
        op.execute(f"DROP INDEX IF EXISTS {name}")
