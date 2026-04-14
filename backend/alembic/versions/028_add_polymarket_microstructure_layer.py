"""Add Phase 5 Polymarket derived research layer.

Revision ID: 028
Revises: 027
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "028"
down_revision = "027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "polymarket_feature_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("run_type", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("scope_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("cursor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rows_inserted_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "ix_pm_feature_runs_reason_started",
        "polymarket_feature_runs",
        ["reason", "started_at"],
    )
    op.create_index(
        "ix_pm_feature_runs_run_type_started",
        "polymarket_feature_runs",
        ["run_type", "started_at"],
    )
    op.create_index(
        "ix_pm_feature_runs_status_started",
        "polymarket_feature_runs",
        ["status", "started_at"],
    )

    op.create_table(
        "polymarket_book_state_topn",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("bucket_start_exchange", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bucket_width_ms", sa.Integer(), nullable=False),
        sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recon_state_id", sa.Integer(), nullable=True),
        sa.Column("last_snapshot_id", sa.Integer(), nullable=True),
        sa.Column("last_snapshot_hash", sa.String(length=255), nullable=True),
        sa.Column("last_applied_raw_event_id", sa.Integer(), nullable=True),
        sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("spread", sa.Numeric(18, 8), nullable=True),
        sa.Column("mid", sa.Numeric(18, 8), nullable=True),
        sa.Column("microprice", sa.Numeric(18, 8), nullable=True),
        sa.Column("bid_levels_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ask_levels_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("bid_depth_top1", sa.Numeric(24, 8), nullable=True),
        sa.Column("bid_depth_top3", sa.Numeric(24, 8), nullable=True),
        sa.Column("bid_depth_top5", sa.Numeric(24, 8), nullable=True),
        sa.Column("ask_depth_top1", sa.Numeric(24, 8), nullable=True),
        sa.Column("ask_depth_top3", sa.Numeric(24, 8), nullable=True),
        sa.Column("ask_depth_top5", sa.Numeric(24, 8), nullable=True),
        sa.Column("imbalance_top1", sa.Numeric(18, 8), nullable=True),
        sa.Column("imbalance_top3", sa.Numeric(18, 8), nullable=True),
        sa.Column("imbalance_top5", sa.Numeric(18, 8), nullable=True),
        sa.Column("completeness_flags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["recon_state_id"], ["polymarket_book_recon_state.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_snapshot_id"], ["polymarket_book_snapshots.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "asset_id",
            "bucket_start_exchange",
            "bucket_width_ms",
            name="uq_pm_book_state_topn_asset_bucket_width",
        ),
    )
    op.create_index("ix_pm_book_state_topn_asset_bucket", "polymarket_book_state_topn", ["asset_id", "bucket_start_exchange"])
    op.create_index(
        "ix_pm_book_state_topn_condition_bucket",
        "polymarket_book_state_topn",
        ["condition_id", "bucket_start_exchange"],
    )
    op.create_index(
        "ix_pm_book_state_topn_width_bucket",
        "polymarket_book_state_topn",
        ["bucket_width_ms", "bucket_start_exchange"],
    )
    op.create_index("ix_pm_book_state_topn_last_snapshot_id", "polymarket_book_state_topn", ["last_snapshot_id"])
    op.create_index("ix_pm_book_state_topn_recon_state_id", "polymarket_book_state_topn", ["recon_state_id"])

    for table_name in ("polymarket_microstructure_features_100ms", "polymarket_microstructure_features_1s"):
        op.create_table(
            table_name,
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("market_dim_id", sa.Integer(), nullable=True),
            sa.Column("asset_dim_id", sa.Integer(), nullable=True),
            sa.Column("condition_id", sa.String(length=255), nullable=False),
            sa.Column("asset_id", sa.String(length=255), nullable=False),
            sa.Column("bucket_start_exchange", sa.DateTime(timezone=True), nullable=False),
            sa.Column("bucket_end_exchange", sa.DateTime(timezone=True), nullable=True),
            sa.Column("observed_at_local", sa.DateTime(timezone=True), nullable=False),
            sa.Column("source_book_state_id", sa.Integer(), nullable=True),
            sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=True),
            sa.Column("best_bid", sa.Numeric(18, 8), nullable=True),
            sa.Column("best_ask", sa.Numeric(18, 8), nullable=True),
            sa.Column("spread", sa.Numeric(18, 8), nullable=True),
            sa.Column("mid", sa.Numeric(18, 8), nullable=True),
            sa.Column("microprice", sa.Numeric(18, 8), nullable=True),
            sa.Column("tick_size", sa.Numeric(18, 8), nullable=True),
            sa.Column("bid_depth_top1", sa.Numeric(24, 8), nullable=True),
            sa.Column("ask_depth_top1", sa.Numeric(24, 8), nullable=True),
            sa.Column("bid_depth_top3", sa.Numeric(24, 8), nullable=True),
            sa.Column("ask_depth_top3", sa.Numeric(24, 8), nullable=True),
            sa.Column("bid_depth_top5", sa.Numeric(24, 8), nullable=True),
            sa.Column("ask_depth_top5", sa.Numeric(24, 8), nullable=True),
            sa.Column("imbalance_top1", sa.Numeric(18, 8), nullable=True),
            sa.Column("imbalance_top3", sa.Numeric(18, 8), nullable=True),
            sa.Column("imbalance_top5", sa.Numeric(18, 8), nullable=True),
            sa.Column("bid_add_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("ask_add_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("bid_remove_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("ask_remove_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("buy_trade_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("sell_trade_volume", sa.Numeric(24, 8), nullable=True),
            sa.Column("buy_trade_count", sa.Integer(), nullable=True),
            sa.Column("sell_trade_count", sa.Integer(), nullable=True),
            sa.Column("trade_notional", sa.Numeric(24, 8), nullable=True),
            sa.Column("last_trade_price", sa.Numeric(18, 8), nullable=True),
            sa.Column("last_trade_side", sa.String(length=16), nullable=True),
            sa.Column("book_update_count", sa.Integer(), nullable=True),
            sa.Column("bbo_update_count", sa.Integer(), nullable=True),
            sa.Column("completeness_flags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["source_book_state_id"], ["polymarket_book_state_topn.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["run_id"], ["polymarket_feature_runs.id"], ondelete="SET NULL"),
            sa.UniqueConstraint("asset_id", "bucket_start_exchange", name=f"uq_{table_name}_asset_bucket"),
        )
        suffix = "100ms" if table_name.endswith("100ms") else "1s"
        op.create_index(f"ix_pm_micro_features_{suffix}_asset_bucket", table_name, ["asset_id", "bucket_start_exchange"])
        op.create_index(
            f"ix_pm_micro_features_{suffix}_condition_bucket",
            table_name,
            ["condition_id", "bucket_start_exchange"],
        )
        op.create_index(f"ix_pm_micro_features_{suffix}_run_id", table_name, ["run_id"])
        op.create_index(
            f"ix_pm_micro_features_{suffix}_source_book_state_id",
            table_name,
            ["source_book_state_id"],
        )

    op.create_table(
        "polymarket_alpha_labels",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("anchor_bucket_start_exchange", sa.DateTime(timezone=True), nullable=False),
        sa.Column("horizon_ms", sa.Integer(), nullable=False),
        sa.Column("source_feature_table", sa.String(length=128), nullable=False),
        sa.Column("source_feature_row_id", sa.Integer(), nullable=False),
        sa.Column("start_mid", sa.Numeric(18, 8), nullable=True),
        sa.Column("end_mid", sa.Numeric(18, 8), nullable=True),
        sa.Column("mid_return_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("mid_move_ticks", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_bid_change", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask_change", sa.Numeric(18, 8), nullable=True),
        sa.Column("up_move", sa.Boolean(), nullable=True),
        sa.Column("down_move", sa.Boolean(), nullable=True),
        sa.Column("flat_move", sa.Boolean(), nullable=True),
        sa.Column("completeness_flags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "asset_id",
            "anchor_bucket_start_exchange",
            "horizon_ms",
            "source_feature_table",
            name="uq_pm_alpha_labels_asset_anchor_horizon_source",
        ),
    )
    op.create_index(
        "ix_pm_alpha_labels_asset_anchor",
        "polymarket_alpha_labels",
        ["asset_id", "anchor_bucket_start_exchange"],
    )
    op.create_index(
        "ix_pm_alpha_labels_condition_anchor",
        "polymarket_alpha_labels",
        ["condition_id", "anchor_bucket_start_exchange"],
    )
    op.create_index(
        "ix_pm_alpha_labels_horizon_source",
        "polymarket_alpha_labels",
        ["horizon_ms", "source_feature_table"],
    )

    op.create_table(
        "polymarket_passive_fill_labels",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("market_dim_id", sa.Integer(), nullable=True),
        sa.Column("asset_dim_id", sa.Integer(), nullable=True),
        sa.Column("condition_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("anchor_bucket_start_exchange", sa.DateTime(timezone=True), nullable=False),
        sa.Column("horizon_ms", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(length=32), nullable=False),
        sa.Column("posted_price", sa.Numeric(18, 8), nullable=False),
        sa.Column("touch_observed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("trade_through_observed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("best_price_improved_against_order", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("adverse_move_after_touch_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("source_feature_table", sa.String(length=128), nullable=False),
        sa.Column("source_feature_row_id", sa.Integer(), nullable=False),
        sa.Column("completeness_flags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_dim_id"], ["polymarket_market_dim.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["asset_dim_id"], ["polymarket_asset_dim.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "asset_id",
            "anchor_bucket_start_exchange",
            "horizon_ms",
            "side",
            "source_feature_table",
            name="uq_pm_passive_fill_labels_asset_anchor_horizon_side_source",
        ),
    )
    op.create_index(
        "ix_pm_passive_fill_labels_asset_anchor",
        "polymarket_passive_fill_labels",
        ["asset_id", "anchor_bucket_start_exchange"],
    )
    op.create_index(
        "ix_pm_passive_fill_labels_condition_anchor",
        "polymarket_passive_fill_labels",
        ["condition_id", "anchor_bucket_start_exchange"],
    )
    op.create_index(
        "ix_pm_passive_fill_labels_horizon_side_source",
        "polymarket_passive_fill_labels",
        ["horizon_ms", "side", "source_feature_table"],
    )


def downgrade() -> None:
    op.drop_index("ix_pm_passive_fill_labels_horizon_side_source", table_name="polymarket_passive_fill_labels")
    op.drop_index("ix_pm_passive_fill_labels_condition_anchor", table_name="polymarket_passive_fill_labels")
    op.drop_index("ix_pm_passive_fill_labels_asset_anchor", table_name="polymarket_passive_fill_labels")
    op.drop_table("polymarket_passive_fill_labels")

    op.drop_index("ix_pm_alpha_labels_horizon_source", table_name="polymarket_alpha_labels")
    op.drop_index("ix_pm_alpha_labels_condition_anchor", table_name="polymarket_alpha_labels")
    op.drop_index("ix_pm_alpha_labels_asset_anchor", table_name="polymarket_alpha_labels")
    op.drop_table("polymarket_alpha_labels")

    op.drop_index(
        "ix_pm_micro_features_1s_source_book_state_id",
        table_name="polymarket_microstructure_features_1s",
    )
    op.drop_index("ix_pm_micro_features_1s_run_id", table_name="polymarket_microstructure_features_1s")
    op.drop_index(
        "ix_pm_micro_features_1s_condition_bucket",
        table_name="polymarket_microstructure_features_1s",
    )
    op.drop_index("ix_pm_micro_features_1s_asset_bucket", table_name="polymarket_microstructure_features_1s")
    op.drop_table("polymarket_microstructure_features_1s")

    op.drop_index(
        "ix_pm_micro_features_100ms_source_book_state_id",
        table_name="polymarket_microstructure_features_100ms",
    )
    op.drop_index("ix_pm_micro_features_100ms_run_id", table_name="polymarket_microstructure_features_100ms")
    op.drop_index(
        "ix_pm_micro_features_100ms_condition_bucket",
        table_name="polymarket_microstructure_features_100ms",
    )
    op.drop_index("ix_pm_micro_features_100ms_asset_bucket", table_name="polymarket_microstructure_features_100ms")
    op.drop_table("polymarket_microstructure_features_100ms")

    op.drop_index("ix_pm_book_state_topn_recon_state_id", table_name="polymarket_book_state_topn")
    op.drop_index("ix_pm_book_state_topn_last_snapshot_id", table_name="polymarket_book_state_topn")
    op.drop_index("ix_pm_book_state_topn_width_bucket", table_name="polymarket_book_state_topn")
    op.drop_index("ix_pm_book_state_topn_condition_bucket", table_name="polymarket_book_state_topn")
    op.drop_index("ix_pm_book_state_topn_asset_bucket", table_name="polymarket_book_state_topn")
    op.drop_table("polymarket_book_state_topn")

    op.drop_index("ix_pm_feature_runs_status_started", table_name="polymarket_feature_runs")
    op.drop_index("ix_pm_feature_runs_run_type_started", table_name="polymarket_feature_runs")
    op.drop_index("ix_pm_feature_runs_reason_started", table_name="polymarket_feature_runs")
    op.drop_table("polymarket_feature_runs")
