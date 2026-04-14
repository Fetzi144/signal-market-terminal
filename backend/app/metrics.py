"""Application metrics exposed via /metrics (Prometheus format)."""
from prometheus_client import Counter, Gauge, Histogram

# Ingestion
ingestion_duration = Histogram(
    "smt_ingestion_duration_seconds",
    "Duration of ingestion jobs",
    ["job_type", "platform"],
)

# Signals
signals_fired = Counter(
    "smt_signals_fired_total",
    "Total signals fired",
    ["signal_type"],
)

# Alerts
alerts_sent = Counter(
    "smt_alerts_sent_total",
    "Total alerts dispatched",
    ["channel"],
)

# Active markets gauge
active_markets = Gauge(
    "smt_active_markets",
    "Number of active markets",
    ["platform"],
)

# SSE connections
sse_connections = Gauge(
    "smt_sse_connections",
    "Current SSE subscriber count",
)

# Circuit breaker
circuit_breaker_state = Gauge(
    "smt_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=half-open, 2=open)",
    ["platform"],
)

# Polymarket stream
polymarket_stream_active_subscriptions = Gauge(
    "smt_polymarket_stream_active_subscriptions",
    "Current subscribed Polymarket asset count",
)

polymarket_stream_active_watches = Gauge(
    "smt_polymarket_stream_active_watches",
    "Current watched Polymarket asset count",
)

polymarket_stream_connected = Gauge(
    "smt_polymarket_stream_connected",
    "Current Polymarket stream connection state (1=connected, 0=disconnected)",
)

polymarket_stream_reconnects = Counter(
    "smt_polymarket_stream_reconnects_total",
    "Total Polymarket stream reconnects",
)

polymarket_stream_resyncs = Counter(
    "smt_polymarket_stream_resyncs_total",
    "Total Polymarket resync operations",
)

polymarket_raw_events_ingested = Counter(
    "smt_polymarket_raw_events_ingested_total",
    "Total raw Polymarket events ingested",
    ["provenance", "message_type"],
)

polymarket_resync_runs = Counter(
    "smt_polymarket_resync_runs_total",
    "Total Polymarket resync runs",
    ["reason", "status"],
)

polymarket_gap_suspicions = Counter(
    "smt_polymarket_gap_suspicions_total",
    "Total suspected Polymarket ingest gaps",
    ["reason"],
)

polymarket_malformed_messages = Counter(
    "smt_polymarket_malformed_messages_total",
    "Total malformed Polymarket stream messages",
)

# Polymarket metadata sync / registry
polymarket_meta_sync_runs = Counter(
    "smt_polymarket_meta_sync_runs_total",
    "Total Polymarket metadata sync runs",
    ["reason", "status"],
)

polymarket_meta_sync_failures = Counter(
    "smt_polymarket_meta_sync_failures_total",
    "Total failed or partial Polymarket metadata sync runs",
)

polymarket_meta_events_upserted = Counter(
    "smt_polymarket_meta_events_upserted_total",
    "Total Polymarket event registry rows inserted or updated",
)

polymarket_meta_markets_upserted = Counter(
    "smt_polymarket_meta_markets_upserted_total",
    "Total Polymarket market registry rows inserted or updated",
)

polymarket_meta_assets_upserted = Counter(
    "smt_polymarket_meta_assets_upserted_total",
    "Total Polymarket asset registry rows inserted or updated",
)

polymarket_meta_param_rows_inserted = Counter(
    "smt_polymarket_meta_param_rows_inserted_total",
    "Total Polymarket parameter history rows inserted",
)

polymarket_meta_last_successful_sync_timestamp = Gauge(
    "smt_polymarket_meta_last_successful_sync_timestamp",
    "Unix timestamp of the most recent successful Polymarket metadata sync",
)

polymarket_meta_last_successful_sync_age_seconds = Gauge(
    "smt_polymarket_meta_last_successful_sync_age_seconds",
    "Age in seconds since the most recent successful Polymarket metadata sync",
)

polymarket_meta_registry_stale_rows = Gauge(
    "smt_polymarket_meta_registry_stale_rows",
    "Current stale Polymarket registry row count",
    ["kind"],
)

# Polymarket raw storage / Phase 3
polymarket_raw_projector_runs = Counter(
    "smt_polymarket_raw_projector_runs_total",
    "Total Polymarket raw projector runs",
    ["reason", "status"],
)

polymarket_raw_projector_failures = Counter(
    "smt_polymarket_raw_projector_failures_total",
    "Total failed Polymarket raw projector runs",
)

polymarket_raw_projected_rows = Counter(
    "smt_polymarket_raw_projected_rows_total",
    "Total projected Polymarket raw rows inserted",
    ["table_name", "source_kind"],
)

polymarket_raw_projector_last_success_timestamp = Gauge(
    "smt_polymarket_raw_projector_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket raw projector run",
)

polymarket_raw_projector_lag = Gauge(
    "smt_polymarket_raw_projector_lag",
    "Lag in raw Polymarket event ids between latest relevant raw event and last successful projector watermark",
)

polymarket_book_snapshot_runs = Counter(
    "smt_polymarket_book_snapshot_runs_total",
    "Total Polymarket book snapshot runs",
    ["reason", "status"],
)

polymarket_book_snapshot_failures = Counter(
    "smt_polymarket_book_snapshot_failures_total",
    "Total failed Polymarket book snapshot runs",
)

polymarket_book_snapshot_last_success_timestamp = Gauge(
    "smt_polymarket_book_snapshot_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket book snapshot run",
)

polymarket_trade_backfill_runs = Counter(
    "smt_polymarket_trade_backfill_runs_total",
    "Total Polymarket trade backfill runs",
    ["reason", "status"],
)

polymarket_trade_backfill_failures = Counter(
    "smt_polymarket_trade_backfill_failures_total",
    "Total failed Polymarket trade backfill runs",
)

polymarket_trade_backfill_last_success_timestamp = Gauge(
    "smt_polymarket_trade_backfill_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket trade backfill run",
)

polymarket_oi_poll_runs = Counter(
    "smt_polymarket_oi_poll_runs_total",
    "Total Polymarket open-interest polling runs",
    ["reason", "status"],
)

polymarket_oi_poll_failures = Counter(
    "smt_polymarket_oi_poll_failures_total",
    "Total failed Polymarket open-interest polling runs",
)

polymarket_oi_poll_last_success_timestamp = Gauge(
    "smt_polymarket_oi_poll_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket open-interest polling run",
)

# Polymarket book reconstruction / Phase 4
polymarket_book_recon_live_books = Gauge(
    "smt_polymarket_book_recon_live_books",
    "Current number of watched Polymarket assets with live reconstructed books",
)

polymarket_book_recon_drift_incidents = Counter(
    "smt_polymarket_book_recon_drift_incidents_total",
    "Total Polymarket reconstruction drift or sanity incidents",
    ["incident_type"],
)

polymarket_book_recon_auto_resync_runs = Counter(
    "smt_polymarket_book_recon_auto_resync_runs_total",
    "Total automatic Polymarket reconstruction resync runs",
    ["status"],
)

polymarket_book_recon_manual_resync_runs = Counter(
    "smt_polymarket_book_recon_manual_resync_runs_total",
    "Total manual Polymarket reconstruction resync runs",
    ["status"],
)

polymarket_book_recon_rows_applied = Counter(
    "smt_polymarket_book_recon_rows_applied_total",
    "Total rows or seed boundaries applied by Polymarket reconstruction",
    ["kind"],
)

polymarket_book_recon_last_successful_resync_timestamp = Gauge(
    "smt_polymarket_book_recon_last_successful_resync_timestamp",
    "Unix timestamp of the most recent successful Polymarket reconstruction resync",
)

polymarket_book_recon_assets_degraded = Gauge(
    "smt_polymarket_book_recon_assets_degraded",
    "Current Polymarket reconstructed assets in degraded states",
    ["status"],
)

# Polymarket Phase 10 risk graph / portfolio optimizer
polymarket_risk_graph_build_runs = Counter(
    "smt_polymarket_risk_graph_build_runs_total",
    "Total Phase 10 risk graph build runs",
    ["status"],
)

polymarket_risk_graph_build_failures = Counter(
    "smt_polymarket_risk_graph_build_failures_total",
    "Total failed Phase 10 risk graph build runs",
)

polymarket_risk_exposure_snapshot_runs = Counter(
    "smt_polymarket_risk_exposure_snapshot_runs_total",
    "Total Phase 10 exposure snapshot runs",
    ["status"],
)

polymarket_risk_exposure_snapshot_failures = Counter(
    "smt_polymarket_risk_exposure_snapshot_failures_total",
    "Total failed Phase 10 exposure snapshot runs",
)

polymarket_risk_optimizer_runs = Counter(
    "smt_polymarket_risk_optimizer_runs_total",
    "Total Phase 10 portfolio optimizer runs",
    ["status"],
)

polymarket_risk_optimizer_failures = Counter(
    "smt_polymarket_risk_optimizer_failures_total",
    "Total failed Phase 10 portfolio optimizer runs",
)

polymarket_risk_concentration = Gauge(
    "smt_polymarket_risk_concentration",
    "Current Phase 10 unhedged concentration by bucket",
    ["bucket_type", "node_key"],
)

polymarket_risk_optimizer_recommendations = Counter(
    "smt_polymarket_risk_optimizer_recommendations_total",
    "Total Phase 10 optimizer recommendations",
    ["recommendation_type", "reason_code"],
)

polymarket_risk_no_quote_recommendations = Counter(
    "smt_polymarket_risk_no_quote_recommendations_total",
    "Total Phase 10 no-quote recommendations",
)

polymarket_risk_last_successful_timestamp = Gauge(
    "smt_polymarket_risk_last_successful_timestamp",
    "Unix timestamp of the most recent successful Phase 10 run by type",
    ["run_type"],
)

polymarket_risk_inventory_budget_utilization = Gauge(
    "smt_polymarket_risk_inventory_budget_utilization",
    "Current Phase 10 inventory budget utilization fraction",
    ["bucket"],
)

# Polymarket Phase 5 microstructure / derived research
polymarket_feature_runs = Counter(
    "smt_polymarket_feature_runs_total",
    "Total Polymarket feature-materialization runs",
    ["run_type", "reason", "status"],
)

polymarket_feature_run_failures = Counter(
    "smt_polymarket_feature_run_failures_total",
    "Total failed Polymarket feature-materialization runs",
    ["run_type"],
)

polymarket_feature_rows_inserted = Counter(
    "smt_polymarket_feature_rows_inserted_total",
    "Total Polymarket derived feature rows inserted",
    ["table_name"],
)

polymarket_label_rows_inserted = Counter(
    "smt_polymarket_label_rows_inserted_total",
    "Total Polymarket derived label rows inserted",
    ["label_type"],
)

polymarket_feature_last_success_timestamp = Gauge(
    "smt_polymarket_feature_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket feature-materialization run",
)

polymarket_label_last_success_timestamp = Gauge(
    "smt_polymarket_label_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket label-materialization run",
)

polymarket_incomplete_bucket_count = Gauge(
    "smt_polymarket_incomplete_bucket_count",
    "Current count of incomplete Polymarket derived feature buckets",
)

# Polymarket Phase 6 execution policy
polymarket_execution_action_candidates_evaluated = Counter(
    "smt_polymarket_execution_action_candidates_evaluated_total",
    "Total Polymarket execution action candidates evaluated",
    ["action_type"],
)

polymarket_execution_chosen_decisions = Counter(
    "smt_polymarket_execution_chosen_decisions_total",
    "Total Polymarket execution policy decisions chosen",
)

polymarket_execution_chosen_actions = Counter(
    "smt_polymarket_execution_chosen_actions_total",
    "Total Polymarket execution policy decisions by chosen action type",
    ["action_type"],
)

polymarket_execution_invalid_candidates = Counter(
    "smt_polymarket_execution_invalid_candidates_total",
    "Total invalid Polymarket execution action candidates",
    ["action_type", "reason"],
)

polymarket_execution_skip_decisions = Counter(
    "smt_polymarket_execution_skip_decisions_total",
    "Total Polymarket execution policy skip decisions",
)

polymarket_execution_decision_failures = Counter(
    "smt_polymarket_execution_decision_failures_total",
    "Total Polymarket execution policy evaluation failures",
)

polymarket_execution_last_success_timestamp = Gauge(
    "smt_polymarket_execution_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket execution policy evaluation",
)

polymarket_execution_estimated_slippage_bps = Histogram(
    "smt_polymarket_execution_estimated_slippage_bps",
    "Estimated slippage versus touch or midpoint for Polymarket execution actions in basis points",
    ["action_type"],
)

# Polymarket Phase 7A OMS / EMS foundation
polymarket_live_order_intents_created = Counter(
    "smt_polymarket_live_order_intents_created_total",
    "Total Polymarket live-order intents created",
)

polymarket_live_submissions_attempted = Counter(
    "smt_polymarket_live_submissions_attempted_total",
    "Total Polymarket live-order submission attempts",
)

polymarket_live_submissions_blocked = Counter(
    "smt_polymarket_live_submissions_blocked_total",
    "Total Polymarket live-order submissions blocked by validation or safety controls",
    ["reason"],
)

polymarket_live_submissions_failed = Counter(
    "smt_polymarket_live_submissions_failed_total",
    "Total failed Polymarket live-order submissions after an attempted gateway call",
)

polymarket_live_cancels = Counter(
    "smt_polymarket_live_cancels_total",
    "Total Polymarket live-order cancel attempts",
)

polymarket_live_cancel_failures = Counter(
    "smt_polymarket_live_cancel_failures_total",
    "Total failed Polymarket live-order cancel attempts",
)

polymarket_user_stream_reconnects = Counter(
    "smt_polymarket_user_stream_reconnects_total",
    "Total Polymarket authenticated user-stream reconnects",
)

polymarket_live_reconcile_runs = Counter(
    "smt_polymarket_live_reconcile_runs_total",
    "Total Polymarket live-order reconcile runs",
    ["reason"],
)

polymarket_live_reconcile_failures = Counter(
    "smt_polymarket_live_reconcile_failures_total",
    "Total failed Polymarket live-order reconcile runs",
)

polymarket_live_fills_observed = Counter(
    "smt_polymarket_live_fills_observed_total",
    "Total Polymarket live fills observed",
    ["fill_status"],
)

polymarket_live_outstanding_reservations = Gauge(
    "smt_polymarket_live_outstanding_reservations",
    "Current outstanding Polymarket reserved capacity",
)

polymarket_live_kill_switch = Gauge(
    "smt_polymarket_live_kill_switch",
    "Current Polymarket live kill-switch state (1=enabled, 0=disabled)",
)

polymarket_live_last_user_stream_message_timestamp = Gauge(
    "smt_polymarket_live_last_user_stream_message_timestamp",
    "Unix timestamp of the most recent Polymarket authenticated user-stream message",
)

polymarket_live_last_reconcile_success_timestamp = Gauge(
    "smt_polymarket_live_last_reconcile_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket live reconcile run",
)

# Polymarket Phase 12 live pilot / control plane
polymarket_pilot_runs_total = Counter(
    "smt_polymarket_pilot_runs_total",
    "Total Phase 12 pilot run state transitions",
    ["status", "reason"],
)

polymarket_pilot_failures_total = Counter(
    "smt_polymarket_pilot_failures_total",
    "Total Phase 12 pilot failures or abortive transitions",
    ["reason"],
)

polymarket_pilot_manual_approvals_total = Counter(
    "smt_polymarket_pilot_manual_approvals_total",
    "Total Phase 12 pilot approval queue actions",
    ["action"],
)

polymarket_control_plane_incidents_total = Counter(
    "smt_polymarket_control_plane_incidents_total",
    "Total Phase 12 control-plane incidents",
    ["incident_type", "severity"],
)

polymarket_heartbeat_healthy = Gauge(
    "smt_polymarket_heartbeat_healthy",
    "Current Phase 12 heartbeat health (1=healthy, 0=degraded or idle)",
)

polymarket_restart_pauses_total = Counter(
    "smt_polymarket_restart_pauses_total",
    "Total Phase 12 restart-window pauses triggered by venue signals",
)

polymarket_live_submissions_blocked_by_pilot_total = Counter(
    "smt_polymarket_live_submissions_blocked_by_pilot_total",
    "Total Phase 12 live submissions blocked by pilot control-plane rules",
    ["reason"],
)

polymarket_live_shadow_evaluations_total = Counter(
    "smt_polymarket_live_shadow_evaluations_total",
    "Total Phase 12 live-vs-shadow evaluations persisted",
    ["variant_name", "coverage_limited"],
)

polymarket_shadow_gap_breaches_total = Counter(
    "smt_polymarket_shadow_gap_breaches_total",
    "Total Phase 12 shadow-gap breach incidents",
    ["variant_name"],
)

polymarket_live_last_successful_fill_timestamp = Gauge(
    "smt_polymarket_live_last_successful_fill_timestamp",
    "Unix timestamp of the most recent successful non-dry-run live fill",
)

# Polymarket Phase 8A structural edge engine
polymarket_structure_runs = Counter(
    "smt_polymarket_structure_runs_total",
    "Total Polymarket structure engine runs",
    ["run_type", "status"],
)

polymarket_structure_run_failures = Counter(
    "smt_polymarket_structure_run_failures_total",
    "Total failed Polymarket structure engine runs",
    ["run_type"],
)

polymarket_structure_groups_built = Counter(
    "smt_polymarket_structure_groups_built_total",
    "Total market-structure groups built or refreshed",
    ["group_type"],
)

polymarket_structure_opportunities_detected = Counter(
    "smt_polymarket_structure_opportunities_detected_total",
    "Total structural opportunities detected",
    ["opportunity_type"],
)

polymarket_structure_actionable_opportunities = Counter(
    "smt_polymarket_structure_actionable_opportunities_total",
    "Total actionable structural opportunities detected",
    ["opportunity_type"],
)

polymarket_structure_non_executable_rejections = Counter(
    "smt_polymarket_structure_non_executable_rejections_total",
    "Total structural opportunities rejected for non-executable legs",
    ["opportunity_type"],
)

polymarket_structure_augmented_filters = Counter(
    "smt_polymarket_structure_augmented_filters_total",
    "Total augmented negative-risk placeholder or Other filters triggered",
)

polymarket_structure_last_successful_scan_timestamp = Gauge(
    "smt_polymarket_structure_last_successful_scan_timestamp",
    "Unix timestamp of the most recent successful structural opportunity scan",
)

polymarket_structure_last_successful_run_timestamp = Gauge(
    "smt_polymarket_structure_last_successful_run_timestamp",
    "Unix timestamp of the most recent successful structural engine run by run type",
    ["run_type"],
)

polymarket_structure_run_duration_seconds = Histogram(
    "smt_polymarket_structure_run_duration_seconds",
    "Duration of structural engine runs by run type",
    ["run_type", "status"],
)

polymarket_structure_lock_conflicts = Counter(
    "smt_polymarket_structure_lock_conflicts_total",
    "Total structural engine runs blocked by the single-run lease",
    ["run_type"],
)

polymarket_structure_validation_results = Counter(
    "smt_polymarket_structure_validation_results_total",
    "Total structural opportunity validations by classification",
    ["classification"],
)

polymarket_structure_validation_reason_codes = Counter(
    "smt_polymarket_structure_validation_reason_codes_total",
    "Total structural validation reason codes observed",
    ["classification", "reason_code"],
)

polymarket_structure_current_opportunities = Gauge(
    "smt_polymarket_structure_current_opportunities",
    "Current structural opportunities by type and classification in the recent window",
    ["opportunity_type", "classification"],
)

polymarket_structure_stale_cross_venue_links = Gauge(
    "smt_polymarket_structure_stale_cross_venue_links",
    "Current count of stale or expired cross-venue links",
)

polymarket_structure_skipped_groups = Gauge(
    "smt_polymarket_structure_skipped_groups",
    "Current count of structure groups skipped during the most recent cycle",
)

polymarket_structure_informational_only_opportunities = Gauge(
    "smt_polymarket_structure_informational_only_opportunities",
    "Current count of informational-only structure opportunities in the recent window",
)

polymarket_structure_pending_approvals = Gauge(
    "smt_polymarket_structure_pending_approvals",
    "Current count of structure paper plans waiting for operator approval",
)

polymarket_structure_paper_plans = Counter(
    "smt_polymarket_structure_paper_plans_total",
    "Total structure paper plans created",
    ["status"],
)

polymarket_structure_paper_route_attempts = Counter(
    "smt_polymarket_structure_paper_route_attempts_total",
    "Total structure paper-route attempts",
    ["status"],
)

# Polymarket Phase 9 maker economics
polymarket_maker_fee_history_rows = Counter(
    "smt_polymarket_maker_fee_history_rows_total",
    "Total Polymarket token fee-history rows inserted",
    ["source_kind"],
)

polymarket_maker_reward_history_rows = Counter(
    "smt_polymarket_maker_reward_history_rows_total",
    "Total Polymarket reward-history rows inserted",
    ["reward_status"],
)

polymarket_maker_last_fee_sync_timestamp = Gauge(
    "smt_polymarket_maker_last_fee_sync_timestamp",
    "Unix timestamp of the most recent Polymarket fee-history observation",
)

polymarket_maker_last_reward_sync_timestamp = Gauge(
    "smt_polymarket_maker_last_reward_sync_timestamp",
    "Unix timestamp of the most recent Polymarket reward-history observation",
)

polymarket_maker_reward_states = Gauge(
    "smt_polymarket_maker_reward_states",
    "Current Polymarket reward-state counts by status",
    ["reward_status"],
)

polymarket_maker_economics_snapshots = Counter(
    "smt_polymarket_maker_economics_snapshots_total",
    "Total Polymarket maker-economics snapshots persisted",
    ["status", "preferred_action"],
)

polymarket_maker_economics_reason_codes = Counter(
    "smt_polymarket_maker_economics_reason_codes_total",
    "Total Polymarket maker-economics reason codes observed",
    ["reason_code"],
)

polymarket_maker_last_snapshot_timestamp = Gauge(
    "smt_polymarket_maker_last_snapshot_timestamp",
    "Unix timestamp of the most recent Polymarket maker-economics snapshot",
)

polymarket_quote_recommendations = Counter(
    "smt_polymarket_quote_recommendations_total",
    "Total Polymarket quote recommendations persisted",
    ["status", "comparison_winner"],
)

polymarket_quote_recommendation_reason_codes = Counter(
    "smt_polymarket_quote_recommendation_reason_codes_total",
    "Total Polymarket quote-recommendation reason codes observed",
    ["reason_code"],
)

polymarket_quote_optimizer_last_recommendation_timestamp = Gauge(
    "smt_polymarket_quote_optimizer_last_recommendation_timestamp",
    "Unix timestamp of the most recent Polymarket quote recommendation",
)

# Polymarket Phase 11 replay simulator / backtest expansion
polymarket_replay_runs_total = Counter(
    "smt_polymarket_replay_runs_total",
    "Total Phase 11 replay or backtest runs",
    ["run_type", "status"],
)

polymarket_replay_scenarios_total = Counter(
    "smt_polymarket_replay_scenarios_total",
    "Total Phase 11 replay scenarios processed",
    ["scenario_type", "status"],
)

polymarket_replay_fills_total = Counter(
    "smt_polymarket_replay_fills_total",
    "Total Phase 11 replay fills produced",
    ["fill_source_kind", "variant_name"],
)

polymarket_replay_policy_comparison_runs = Counter(
    "smt_polymarket_replay_policy_comparison_runs_total",
    "Total Phase 11 policy-comparison replay runs",
    ["status"],
)

polymarket_replay_variant_net_pnl = Gauge(
    "smt_polymarket_replay_variant_net_pnl",
    "Latest Phase 11 run-level replay net PnL by variant",
    ["variant_name"],
)

polymarket_replay_variant_fill_rate = Gauge(
    "smt_polymarket_replay_variant_fill_rate",
    "Latest Phase 11 run-level replay fill rate by variant",
    ["variant_name"],
)

polymarket_replay_last_successful_timestamp = Gauge(
    "smt_polymarket_replay_last_successful_timestamp",
    "Unix timestamp of the most recent successful Phase 11 replay run",
)

polymarket_replay_coverage_limited_runs = Gauge(
    "smt_polymarket_replay_coverage_limited_runs",
    "Coverage-limited scenario count from the most recent Phase 11 replay run",
)
