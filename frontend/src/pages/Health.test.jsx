import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  getHealth,
  getPolymarketIngestStatus,
  getPolymarketWatchAssets,
  triggerPolymarketBookSnapshot,
  triggerPolymarketBookReconResync,
  triggerPolymarketFeatureMaterialization,
  triggerPolymarketMetadataSync,
  triggerPolymarketOiPoll,
  triggerPolymarketRawProjector,
  triggerPolymarketResync,
  triggerPolymarketStructureGroupBuild,
  triggerPolymarketStructureOpportunityScan,
  triggerPolymarketTradeBackfill,
  updatePolymarketWatchAsset,
} from "../api";
import Health from "./Health";

vi.mock("../api", () => ({
  getHealth: vi.fn(),
  getPolymarketIngestStatus: vi.fn(),
  getPolymarketWatchAssets: vi.fn(),
  triggerPolymarketBookSnapshot: vi.fn(),
  triggerPolymarketBookReconResync: vi.fn(),
  triggerPolymarketFeatureMaterialization: vi.fn(),
  triggerPolymarketMetadataSync: vi.fn(),
  triggerPolymarketOiPoll: vi.fn(),
  triggerPolymarketRawProjector: vi.fn(),
  triggerPolymarketResync: vi.fn(),
  triggerPolymarketStructureGroupBuild: vi.fn(),
  triggerPolymarketStructureOpportunityScan: vi.fn(),
  triggerPolymarketTradeBackfill: vi.fn(),
  updatePolymarketWatchAsset: vi.fn(),
}));

vi.mock("../components/PushNotificationToggle", () => ({
  default: () => <div data-testid="push-toggle">push-toggle</div>,
}));

const healthPayload = {
  status: "degraded",
  active_markets: 12,
  total_signals: 44,
  unresolved_signals: 7,
  recent_alerts_24h: 3,
  alert_threshold: 0.82,
  scheduler_lease: {
    owner_token: "default:worker-host:321:abcdef123456",
    heartbeat_freshness_seconds: 4,
    expires_in_seconds: 26,
  },
  default_strategy_runtime: {
    overdue_open_trades: 2,
    last_resolution_backfill_at: "2026-04-13T10:06:00Z",
    last_resolution_backfill_count: 3,
    evaluation_clamp_count_24h: 1,
    last_evaluation_failure_at: "2026-04-13T09:58:00Z",
  },
  runtime_invariants: [
    {
      key: "scheduler_lease_fresh",
      label: "Scheduler Lease Fresh",
      status: "passing",
      detail: "Owner default:worker-host:321:abcdef123456 heartbeat 4s ago, expires in 26s.",
    },
    {
      key: "overdue_open_trades_zero",
      label: "Overdue Open Trades",
      status: "failing",
      detail: "2 overdue open trade(s) remain past market end.",
    },
    {
      key: "evaluation_failures_24h_zero",
      label: "Evaluation Failures (24h)",
      status: "failing",
      detail: "Latest evaluation failure at 2026-04-13T09:58:00Z.",
    },
  ],
  polymarket_phase1: {
    enabled: true,
    connected: true,
    continuity_status: "healthy",
    connection_started_at: "2026-04-13T10:02:00Z",
    current_connection_id: "11111111-1111-1111-1111-111111111111",
    last_event_received_at: "2026-04-13T10:04:00Z",
    heartbeat_freshness_seconds: 6,
    watched_asset_count: 2,
    subscribed_asset_count: 2,
    reconnect_count: 4,
    resync_count: 3,
    gap_suspected_count: 2,
    malformed_message_count: 1,
    last_successful_resync_at: "2026-04-13T10:03:00Z",
  },
  polymarket_phase6: {
    enabled: true,
    require_live_book: true,
    default_horizon_ms: 1000,
    passive_lookback_hours: 24,
    passive_min_label_rows: 20,
    step_ahead_enabled: true,
    max_cross_slippage_bps: 150.0,
    min_net_ev_bps: 0.0,
    last_successful_decision_at: "2026-04-13T10:05:00Z",
    recent_decisions_24h: 5,
    recent_action_mix: { cross_now: 2, post_best: 1, step_ahead: 1, skip: 1 },
    recent_invalid_candidates_24h: 3,
    recent_skip_decisions_24h: 1,
    recent_avg_est_net_ev_bps: 17.25,
  },
  polymarket_phase7a: {
    enabled: false,
    dry_run: true,
    manual_approval_required: true,
    gateway_reachable: false,
    user_stream_connected: false,
    kill_switch_enabled: false,
    outstanding_live_orders: 2,
    outstanding_reservations: 75.25,
    recent_fills_24h: 1,
    last_reconcile_success_at: "2026-04-13T10:05:10Z",
    last_user_stream_message_at: "2026-04-13T10:04:59Z",
  },
  polymarket_phase8a: {
    enabled: true,
    on_startup: true,
    interval_seconds: 300,
    min_net_edge_bps: 0,
    require_executable_all_legs: true,
    include_cross_venue: false,
    allow_augmented_neg_risk: false,
    max_groups_per_run: 250,
    cross_venue_max_staleness_seconds: 180,
    max_leg_slippage_bps: 150,
    run_lock_enabled: true,
    retention_days: 30,
    validation_enabled: true,
    paper_routing_enabled: false,
    paper_require_manual_approval: true,
    max_notional_per_plan: 100,
    min_depth_per_leg: 1,
    plan_max_age_seconds: 180,
    link_review_required: false,
    last_successful_group_build_at: "2026-04-13T10:04:45Z",
    last_successful_scan_at: "2026-04-13T10:05:15Z",
    last_successful_validation_at: "2026-04-13T10:05:20Z",
    last_successful_paper_plan_at: "2026-04-13T10:05:25Z",
    last_successful_paper_route_at: "2026-04-13T10:05:30Z",
    last_successful_retention_prune_at: "2026-04-13T10:05:35Z",
    last_group_build_status: "completed",
    last_group_build_started_at: "2026-04-13T10:04:40Z",
    last_group_build_duration_seconds: 5,
    last_scan_status: "completed",
    last_scan_started_at: "2026-04-13T10:05:10Z",
    last_scan_duration_seconds: 5,
    last_validation_status: "completed",
    last_validation_started_at: "2026-04-13T10:05:16Z",
    last_validation_duration_seconds: 4,
    last_paper_plan_status: "completed",
    last_paper_plan_started_at: "2026-04-13T10:05:22Z",
    last_paper_plan_duration_seconds: 3,
    last_paper_route_status: "completed",
    last_paper_route_started_at: "2026-04-13T10:05:26Z",
    last_paper_route_duration_seconds: 4,
    last_retention_prune_status: "completed",
    last_retention_prune_started_at: "2026-04-13T10:05:32Z",
    last_retention_prune_duration_seconds: 2,
    recent_actionable_by_type: { neg_risk_direct_vs_basket: 2, binary_complement: 1 },
    recent_non_executable_count: 3,
    informational_augmented_group_count: 2,
    active_group_counts: { neg_risk_event: 4, binary_complement: 8, event_sum_parity: 2 },
    active_cross_venue_link_count: 0,
    informational_only_opportunity_count: 2,
    blocked_opportunity_count: 1,
    executable_candidate_count: 3,
    opportunity_counts_by_type: { neg_risk_direct_vs_basket: 2, cross_venue_basis: 1 },
    validation_reason_counts: { cross_venue_link_expired: 1, no_positive_current_edge: 2 },
    stale_cross_venue_link_count: 1,
    skipped_group_count: 2,
    pending_approval_count: 1,
  },
  polymarket_phase9: {
    enabled: true,
    fee_history_enabled: true,
    reward_history_enabled: true,
    quote_optimizer_enabled: true,
    quote_optimizer_max_notional: 25,
    quote_optimizer_max_age_seconds: 180,
    quote_optimizer_require_rewards_data: false,
    quote_optimizer_require_fee_data: true,
    last_fee_sync_at: "2026-04-13T10:05:40Z",
    last_reward_sync_at: "2026-04-13T10:05:41Z",
    last_snapshot_at: "2026-04-13T10:05:42Z",
    last_recommendation_at: "2026-04-13T10:05:43Z",
    fee_history_rows: 18,
    reward_history_rows: 9,
    economics_snapshot_rows: 5,
    quote_recommendation_rows: 3,
    reward_state_counts: { active: 4, missing: 3, expired: 2 },
    recent_reason_counts_24h: { advisory_only_output: 5, missing_reward_config: 2 },
    fee_freshness_seconds: 60,
    reward_freshness_seconds: 59,
  },
  polymarket_phase10: {
    enabled: true,
    on_startup: false,
    interval_seconds: 300,
    portfolio_optimizer_enabled: true,
    portfolio_optimizer_interval_seconds: 300,
    advisory_only: true,
    live_disabled_by_default: true,
    last_successful_graph_build_at: "2026-04-13T10:05:44Z",
    last_successful_exposure_snapshot_at: "2026-04-13T10:05:45Z",
    last_successful_optimizer_run_at: "2026-04-13T10:05:46Z",
    last_graph_build_status: "completed",
    last_exposure_snapshot_status: "completed",
    last_optimizer_status: "completed",
    top_concentrated_exposures: [
      {
        node_key: "event:polymarket:evt-phase10",
        node_type: "event",
        label: "Election Risk",
        gross_notional_usd: 92.5,
        net_notional_usd: 70.0,
        hedged_fraction: 0.24,
      },
    ],
    recent_block_reason_counts_24h: { inventory_toxicity_exceeded: 2, event_cap_exceeded: 1 },
    maker_budget_used_usd: 41.0,
    maker_budget_usd: 50.0,
    taker_budget_used_usd: 22.5,
    taker_budget_usd: 150.0,
    maker_budget_utilization: 0.82,
    taker_budget_utilization: 0.15,
  },
  polymarket_phase11: {
    enabled: true,
    live_disabled_by_default: true,
    default_window_minutes: 60,
    passive_fill_timeout_seconds: 15,
    last_replay_run: { started_at: "2026-04-13T10:05:50Z" },
    last_successful_policy_comparison: { started_at: "2026-04-13T10:05:55Z" },
    recent_scenario_count_24h: 3,
    recent_coverage_limited_run_count_24h: 1,
    recent_failed_run_count_24h: 0,
    coverage_mode: "supported_detectors_only",
    configured_supported_detectors: ["confluence", "arbitrage"],
    supported_detectors: ["confluence"],
    unsupported_detectors: [],
    recent_variant_summary: {
      exec_policy: { net_pnl: 12.25, fill_rate: 0.66, slippage_bps: 4.2 },
    },
  },
  polymarket_phase12: {
    pilot_enabled: false,
    live_trading_enabled: false,
    pilot_armed: false,
    pilot_paused: false,
    active_pilot_family: null,
    strategy_version: {
      id: 2,
      version_key: "exec_policy_infra_v1",
      version_label: "Execution Policy Infra v1",
    },
    latest_promotion_evaluation: {
      id: 22,
      evaluation_status: "blocked",
      evaluation_kind: "pilot_readiness_gate",
      autonomy_tier: "shadow_only",
    },
    autonomy_state: {
      effective_autonomy_tier: "shadow_only",
      submission_mode: "shadow_only",
      state_reason: "manual_approval_required",
      blocked_reasons: ["manual_approval_required"],
      gate_kind: "pilot_readiness_gate",
    },
    manual_approval_required: true,
    approval_queue_count: 2,
    heartbeat_status: "idle",
    user_stream_connected: false,
    recent_incident_count_24h: 1,
    recent_incidents: [
      {
        id: 7,
        observed_at_local: "2026-04-13T10:05:45Z",
        incident_type: "submission_blocked",
        strategy_version_id: 2,
        strategy_version: {
          version_key: "exec_policy_infra_v1",
          version_label: "Execution Policy Infra v1",
        },
        latest_promotion_evaluation: {
          evaluation_status: "blocked",
          evaluation_kind: "pilot_readiness_gate",
          autonomy_tier: "shadow_only",
        },
        severity: "warning",
        details_json: { reason: "manual_approval_required" },
      },
    ],
    live_shadow_summary: {
      recent_count_24h: 2,
      average_gap_bps_24h: 6.5,
      worst_gap_bps_24h: 11.4,
      breach_count_24h: 0,
    },
    daily_realized_pnl: {
      net_realized_pnl: -1.25,
    },
    approval_expired_count_24h: 1,
    recent_guardrail_triggers: [
      {
        id: 1,
        guardrail_type: "approval_ttl",
        observed_at_local: "2026-04-13T10:06:00Z",
        strategy_version_id: 2,
        strategy_version: {
          version_key: "exec_policy_infra_v1",
          version_label: "Execution Policy Infra v1",
        },
        latest_promotion_evaluation: {
          evaluation_status: "blocked",
          evaluation_kind: "pilot_readiness_gate",
          autonomy_tier: "shadow_only",
        },
        action_taken: "block",
      },
    ],
    latest_readiness_status: "manual_only",
    latest_readiness_generated_at: "2026-04-13T10:06:30Z",
    last_reconcile_success_at: "2026-04-13T10:05:10Z",
    kill_switch_enabled: false,
  },
  ingestion: [
    {
      run_type: "market_discovery",
      last_status: "success",
      last_run: "2026-04-13T10:00:00Z",
      markets_processed: 12,
    },
  ],
  strategy_families: [
    {
      family: "default_strategy",
      label: "Default Strategy",
      posture: "benchmark_only",
      configured: true,
      review_enabled: true,
      primary_surface: "paper_trading",
      description: "Frozen confluence benchmark used to prove or falsify edge honestly.",
      disabled_reason: null,
      current_version: {
        version_label: "Frozen Benchmark v1",
        autonomy_state: {
          effective_autonomy_tier: "shadow_only",
          state_reason: "benchmark_only",
          blocked_reasons: ["benchmark_only"],
        },
      },
      risk_budget_policy: {
        capital: { outstanding_notional_usd: 100.0 },
      },
      risk_budget_status: {
        current_outstanding_usd: 25.0,
        effective_outstanding_cap_usd: 100.0,
        regime_label: "thin_liquidity",
        capacity_status: "constrained",
        reason_codes: ["incident_pressure", "capacity_ceiling_exceeded"],
        open_order_count: 1,
        effective_max_open_orders: 12,
      },
      autonomy_state: {
        effective_autonomy_tier: "shadow_only",
        state_reason: "benchmark_only",
        blocked_reasons: ["benchmark_only"],
      },
    },
    {
      family: "cross_venue_basis",
      label: "Cross-Venue Basis",
      posture: "disabled",
      configured: false,
      review_enabled: false,
      primary_surface: "structure",
      description: "Cross-venue spread research stays informational until paired executable hedge routing exists.",
      disabled_reason: "Paired executable hedge routing is not implemented yet.",
      current_version: {
        version_label: "Cross-Venue Basis Disabled v1",
        autonomy_state: {
          effective_autonomy_tier: "shadow_only",
          state_reason: "family_disabled",
          blocked_reasons: ["family_disabled"],
        },
      },
      risk_budget_policy: {
        capital: { outstanding_notional_usd: 0.0 },
      },
      risk_budget_status: {
        current_outstanding_usd: 0.0,
        effective_outstanding_cap_usd: 0.0,
        regime_label: "halted",
        capacity_status: "breached",
        reason_codes: ["capacity_ceiling_exceeded"],
        open_order_count: 0,
        effective_max_open_orders: 0,
      },
      autonomy_state: {
        effective_autonomy_tier: "shadow_only",
        state_reason: "family_disabled",
        blocked_reasons: ["family_disabled"],
      },
    },
  ],
};

const ingestPayload = {
  enabled: true,
  connected: true,
  connection_started_at: "2026-04-13T10:02:00Z",
  current_connection_id: "11111111-1111-1111-1111-111111111111",
  last_event_received_at: "2026-04-13T10:04:00Z",
  heartbeat_freshness_seconds: 6,
  continuity_status: "healthy",
  watched_asset_count: 2,
  active_watch_count: 2,
  active_subscription_count: 2,
  subscribed_asset_count: 2,
  events_ingested_5m: 9,
  events_ingested: { "1m": 2, "5m": 9, "15m": 21 },
  reconnect_count: 4,
  resync_count: 3,
  gap_suspected_count: 2,
  malformed_message_count: 1,
  last_resync_at: "2026-04-13T10:03:00Z",
  last_successful_resync_at: "2026-04-13T10:03:00Z",
  last_reconciliation_at: "2026-04-13T10:04:00Z",
  last_error: null,
  last_error_at: null,
  updated_at: "2026-04-13T10:04:00Z",
  metadata_sync: {
    enabled: true,
    on_startup: true,
    interval_seconds: 900,
    include_closed: false,
    page_size: 200,
    last_successful_sync_at: "2026-04-13T10:01:00Z",
    last_run_status: "completed",
    last_run_started_at: "2026-04-13T10:00:40Z",
    last_run_completed_at: "2026-04-13T10:01:00Z",
    last_run_id: "22222222-2222-2222-2222-222222222222",
    recent_param_changes_24h: 4,
    stale_registry_counts: { events: 1, markets: 0, assets: 2 },
    registry_counts: { events: 3, markets: 8, assets: 16 },
    stale_after_seconds: 1800,
    freshness_seconds: 120,
    recent_sync_runs: [],
  },
  raw_storage: {
    enabled: true,
    book_snapshot_interval_seconds: 300,
    trade_backfill_enabled: true,
    trade_backfill_on_startup: true,
    trade_backfill_interval_seconds: 900,
    trade_backfill_lookback_hours: 24,
    trade_backfill_page_size: 200,
    oi_poll_enabled: true,
    oi_poll_interval_seconds: 900,
    retention_days: 14,
    projector_last_run_status: "completed",
    projector_last_run_started_at: "2026-04-13T10:03:50Z",
    projector_last_run_completed_at: "2026-04-13T10:03:55Z",
    last_projected_raw_event_id: 91,
    latest_relevant_raw_event_id: 91,
    projector_lag: 0,
    last_successful_book_snapshot_at: "2026-04-13T10:04:10Z",
    last_successful_trade_backfill_at: "2026-04-13T10:04:20Z",
    last_successful_oi_poll_at: "2026-04-13T10:04:30Z",
    book_snapshot_freshness_seconds: 50,
    trade_backfill_freshness_seconds: 40,
    oi_poll_freshness_seconds: 30,
    rows_inserted_24h: {
      book_snapshots: 3,
      book_deltas: 8,
      bbo_events: 4,
      trade_tape: 5,
      open_interest_history: 2,
    },
    recent_capture_runs: [],
  },
  book_reconstruction: {
    enabled: true,
    on_startup: true,
    auto_resync_enabled: true,
    stale_after_seconds: 900,
    resync_cooldown_seconds: 60,
    max_watched_assets: 500,
    bbo_tolerance: 0,
    watched_asset_count: 2,
    live_book_count: 2,
    drifted_asset_count: 1,
    stale_asset_count: 1,
    resyncing_asset_count: 0,
    degraded_asset_count: 1,
    last_successful_resync_at: "2026-04-13T10:04:40Z",
    recent_incident_count: 2,
    status_counts: { live: 2, drifted: 1 },
    recent_incidents: [],
  },
  features: {
    enabled: true,
    on_startup: true,
    interval_seconds: 300,
    lookback_hours: 1,
    bucket_widths_ms: [100, 1000],
    label_horizons_ms: [250, 1000, 5000],
    max_watched_assets: 50,
    last_successful_feature_run_at: "2026-04-13T10:04:50Z",
    last_successful_label_run_at: "2026-04-13T10:04:55Z",
    recent_feature_rows_24h: 12,
    recent_label_rows_24h: 18,
    incomplete_bucket_count_24h: 2,
    recent_runs: [],
  },
  execution_policy: {
    enabled: true,
    require_live_book: true,
    default_horizon_ms: 1000,
    passive_lookback_hours: 24,
    passive_min_label_rows: 20,
    step_ahead_enabled: true,
    max_cross_slippage_bps: 150.0,
    min_net_ev_bps: 0.0,
    last_successful_decision_at: "2026-04-13T10:05:00Z",
    recent_decisions_24h: 5,
    recent_action_mix: { cross_now: 2, post_best: 1, step_ahead: 1, skip: 1 },
    recent_invalid_candidates_24h: 3,
    recent_skip_decisions_24h: 1,
    recent_avg_est_net_ev_bps: 17.25,
  },
  maker_economics: {
    enabled: true,
    fee_history_enabled: true,
    reward_history_enabled: true,
    quote_optimizer_enabled: true,
    quote_optimizer_max_notional: 25,
    quote_optimizer_max_age_seconds: 180,
    quote_optimizer_require_rewards_data: false,
    quote_optimizer_require_fee_data: true,
    last_fee_sync_at: "2026-04-13T10:05:40Z",
    last_reward_sync_at: "2026-04-13T10:05:41Z",
    last_snapshot_at: "2026-04-13T10:05:42Z",
    last_recommendation_at: "2026-04-13T10:05:43Z",
    fee_history_rows: 18,
    reward_history_rows: 9,
    economics_snapshot_rows: 5,
    quote_recommendation_rows: 3,
    reward_state_counts: { active: 4, missing: 3, expired: 2 },
    recent_reason_counts_24h: { advisory_only_output: 5, missing_reward_config: 2 },
    fee_freshness_seconds: 60,
    reward_freshness_seconds: 59,
  },
  replay: {
    enabled: false,
    on_startup: false,
    interval_seconds: 1800,
    default_window_minutes: 60,
    max_scenarios_per_run: 100,
    structure_enabled: true,
    maker_enabled: true,
    risk_adjustments_enabled: true,
    require_complete_book_coverage: true,
    passive_fill_timeout_seconds: 15,
    advisory_only: true,
    live_disabled_by_default: true,
    last_replay_run: { started_at: "2026-04-13T10:05:50Z" },
    last_successful_policy_comparison: { started_at: "2026-04-13T10:05:55Z" },
    recent_scenario_count_24h: 3,
    recent_coverage_limited_run_count_24h: 1,
    recent_failed_run_count_24h: 0,
    coverage_mode: "supported_detectors_only",
    configured_supported_detectors: ["confluence", "arbitrage"],
    supported_detectors: ["confluence"],
    unsupported_detectors: [],
    recent_variant_summary: {
      exec_policy: { net_pnl: 12.25, fill_rate: 0.66, slippage_bps: 4.2 },
    },
    recent_runs: [],
  },
  strategy_families: healthPayload.strategy_families,
  structure_engine: {
    enabled: true,
    on_startup: true,
    interval_seconds: 300,
    min_net_edge_bps: 0,
    require_executable_all_legs: true,
    include_cross_venue: false,
    allow_augmented_neg_risk: false,
    max_groups_per_run: 250,
    cross_venue_max_staleness_seconds: 180,
    max_leg_slippage_bps: 150,
    run_lock_enabled: true,
    retention_days: 30,
    validation_enabled: true,
    paper_routing_enabled: false,
    paper_require_manual_approval: true,
    max_notional_per_plan: 100,
    min_depth_per_leg: 1,
    plan_max_age_seconds: 180,
    link_review_required: false,
    last_successful_group_build_at: "2026-04-13T10:04:45Z",
    last_successful_scan_at: "2026-04-13T10:05:15Z",
    last_successful_validation_at: "2026-04-13T10:05:20Z",
    last_successful_paper_plan_at: "2026-04-13T10:05:25Z",
    last_successful_paper_route_at: "2026-04-13T10:05:30Z",
    last_successful_retention_prune_at: "2026-04-13T10:05:35Z",
    last_group_build_status: "completed",
    last_group_build_started_at: "2026-04-13T10:04:40Z",
    last_group_build_duration_seconds: 5,
    last_scan_status: "completed",
    last_scan_started_at: "2026-04-13T10:05:10Z",
    last_scan_duration_seconds: 5,
    last_validation_status: "completed",
    last_validation_started_at: "2026-04-13T10:05:16Z",
    last_validation_duration_seconds: 4,
    last_paper_plan_status: "completed",
    last_paper_plan_started_at: "2026-04-13T10:05:22Z",
    last_paper_plan_duration_seconds: 3,
    last_paper_route_status: "completed",
    last_paper_route_started_at: "2026-04-13T10:05:26Z",
    last_paper_route_duration_seconds: 4,
    last_retention_prune_status: "completed",
    last_retention_prune_started_at: "2026-04-13T10:05:32Z",
    last_retention_prune_duration_seconds: 2,
    recent_actionable_by_type: { neg_risk_direct_vs_basket: 2, binary_complement: 1 },
    recent_non_executable_count: 3,
    informational_augmented_group_count: 2,
    active_group_counts: { neg_risk_event: 4, binary_complement: 8, event_sum_parity: 2 },
    active_cross_venue_link_count: 0,
    informational_only_opportunity_count: 2,
    blocked_opportunity_count: 1,
    executable_candidate_count: 3,
    opportunity_counts_by_type: { neg_risk_direct_vs_basket: 2, cross_venue_basis: 1 },
    validation_reason_counts: { cross_venue_link_expired: 1, no_positive_current_edge: 2 },
    stale_cross_venue_link_count: 1,
    skipped_group_count: 2,
    pending_approval_count: 1,
    recent_runs: [],
  },
  recent_incidents: [
    {
      id: "incident-1",
      created_at: "2026-04-13T10:03:30Z",
      incident_type: "gap_suspected",
      severity: "warning",
      asset_id: "token-1",
      connection_id: "11111111-1111-1111-1111-111111111111",
      raw_event_id: 91,
      resync_run_id: null,
      details_json: { reason: "reconnect" },
      resolved_at: null,
    },
  ],
  recent_resync_runs: [
    {
      id: "run-1",
      started_at: "2026-04-13T10:03:35Z",
      completed_at: "2026-04-13T10:03:50Z",
      status: "completed",
      reason: "reconnect",
      connection_id: "11111111-1111-1111-1111-111111111111",
      requested_asset_count: 2,
      succeeded_asset_count: 2,
      failed_asset_count: 0,
      details_json: {},
    },
  ],
};

const watchPayload = {
  total: 1,
  page: 1,
  page_size: 12,
  watch_assets: [
    {
      id: "watch-1",
      outcome_id: "00000000-0000-0000-0000-000000000001",
      asset_id: "token-1",
      watch_enabled: true,
      watch_reason: "active_universe_bootstrap",
      priority: 3,
      created_at: "2026-04-13T10:00:00Z",
      updated_at: "2026-04-13T10:00:00Z",
      market_id: "00000000-0000-0000-0000-000000000002",
      market_platform_id: "mkt-1",
      market_question: "Will the market stay healthy?",
      market_active: true,
      outcome_name: "Yes",
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  getHealth.mockResolvedValue(healthPayload);
  getPolymarketIngestStatus.mockResolvedValue(ingestPayload);
  getPolymarketWatchAssets.mockResolvedValue(watchPayload);
  triggerPolymarketBookSnapshot.mockResolvedValue({
    id: "run-book",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    reason: "manual",
    scope_json: {},
    cursor_json: null,
    rows_inserted_json: { book_snapshots: 1 },
    error_count: 0,
    details_json: {},
  });
  triggerPolymarketMetadataSync.mockResolvedValue({
    id: "run-3",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    reason: "manual",
    include_closed: false,
    events_seen: 1,
    markets_seen: 1,
    assets_upserted: 2,
    events_upserted: 1,
    markets_upserted: 1,
    param_rows_inserted: 2,
    error_count: 0,
    details_json: {},
  });
  triggerPolymarketOiPoll.mockResolvedValue({
    id: "run-oi",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    reason: "manual",
    scope_json: {},
    cursor_json: null,
    rows_inserted_json: { open_interest_history: 1 },
    error_count: 0,
    details_json: {},
  });
  triggerPolymarketRawProjector.mockResolvedValue({
    run_count: 1,
    last_run: {
      id: "run-projector",
      run_type: "raw_projector",
      reason: "manual",
      started_at: "2026-04-13T10:05:00Z",
      completed_at: "2026-04-13T10:05:05Z",
      status: "completed",
      scope_json: null,
      cursor_json: { last_projected_raw_event_id: 91 },
      rows_inserted_json: { book_snapshots: 1, book_deltas: 1, bbo_events: 1, trade_tape: 1 },
      error_count: 0,
      details_json: {},
    },
    runs: [],
  });
  triggerPolymarketResync.mockResolvedValue({
    run_id: "run-2",
    asset_ids: ["token-1"],
    requested_asset_count: 1,
    succeeded_asset_count: 1,
    failed_asset_count: 0,
    events_persisted: 1,
    reason: "manual",
    status: "completed",
  });
  triggerPolymarketTradeBackfill.mockResolvedValue({
    id: "run-trades",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    reason: "manual",
    scope_json: {},
    cursor_json: null,
    rows_inserted_json: { trade_tape: 1 },
    error_count: 0,
    details_json: {},
  });
  updatePolymarketWatchAsset.mockResolvedValue({
    ...watchPayload.watch_assets[0],
    watch_enabled: false,
    watch_reason: "manual_operator_disable",
  });
  triggerPolymarketBookReconResync.mockResolvedValue({
    asset_ids: ["token-1"],
    reason: "manual",
    status: "completed",
    run_id: "run-book-recon",
    requested_asset_count: 1,
    succeeded_asset_count: 1,
    failed_asset_count: 0,
    events_persisted: 1,
    reconstruction: { live_count: 1, degraded_count: 0, asset_count: 1, results: [] },
  });
  triggerPolymarketFeatureMaterialization.mockResolvedValue({
    status: "completed",
    scope_json: {},
    book_state_run: {
      id: "33333333-3333-3333-3333-333333333333",
      run_type: "book_state_materialize",
      reason: "manual",
      started_at: "2026-04-13T10:05:00Z",
      completed_at: "2026-04-13T10:05:05Z",
      status: "completed",
      scope_json: {},
      cursor_json: null,
      rows_inserted_json: { polymarket_book_state_topn: 2 },
      error_count: 0,
      details_json: {},
    },
    feature_run: {
      id: "44444444-4444-4444-4444-444444444444",
      run_type: "feature_materialize",
      reason: "manual",
      started_at: "2026-04-13T10:05:00Z",
      completed_at: "2026-04-13T10:05:05Z",
      status: "completed",
      scope_json: {},
      cursor_json: null,
      rows_inserted_json: { polymarket_microstructure_features_100ms: 2 },
      error_count: 0,
      details_json: {},
    },
    label_run: {
      id: "55555555-5555-5555-5555-555555555555",
      run_type: "label_materialize",
      reason: "manual",
      started_at: "2026-04-13T10:05:00Z",
      completed_at: "2026-04-13T10:05:05Z",
      status: "completed",
      scope_json: {},
      cursor_json: null,
      rows_inserted_json: { polymarket_alpha_labels: 2 },
      error_count: 0,
      details_json: {},
    },
  });
  triggerPolymarketStructureGroupBuild.mockResolvedValue({
    id: "66666666-6666-6666-6666-666666666666",
    run_type: "group_build",
    reason: "manual",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    scope_json: {},
    cursor_json: null,
    rows_inserted_json: { groups_upserted: 3, members_upserted: 8 },
    error_count: 0,
    details_json: {},
  });
  triggerPolymarketStructureOpportunityScan.mockResolvedValue({
    id: "77777777-7777-7777-7777-777777777777",
    run_type: "opportunity_scan",
    reason: "manual",
    started_at: "2026-04-13T10:05:00Z",
    completed_at: "2026-04-13T10:05:05Z",
    status: "completed",
    scope_json: {},
    cursor_json: null,
    rows_inserted_json: { opportunities_inserted: 2, legs_inserted: 5 },
    error_count: 0,
    details_json: {},
  });
});

describe("Health", () => {
  test("renders Polymarket stream details and operator controls", async () => {
    render(<Health />);

    expect(await screen.findByText("Polymarket Stream")).toBeInTheDocument();
    expect(screen.getByText("Connected")).toBeInTheDocument();
    expect(screen.getByText("Phase 2 Metadata Registry")).toBeInTheDocument();
    expect(screen.getByText("Phase 3 Raw Storage")).toBeInTheDocument();
    expect(screen.getByText("Phase 4 Book Reconstruction")).toBeInTheDocument();
    expect(screen.getByText("Phase 5 Derived Research")).toBeInTheDocument();
    expect(screen.getByText("Phase 6 Execution Policy")).toBeInTheDocument();
    expect(screen.getByText("Phase 7A OMS/EMS Foundation")).toBeInTheDocument();
    expect(screen.getByText("Phase 8B Validation, Controls, and Paper Routing")).toBeInTheDocument();
    expect(screen.getByText("Phase 9 Maker Economics")).toBeInTheDocument();
    expect(screen.getByText("Phase 10 Risk Graph and Portfolio Optimizer")).toBeInTheDocument();
    expect(screen.getByText("Phase 11 Replay Simulator and Backtest Expansion")).toBeInTheDocument();
    expect(screen.getByText("Phase 12 Live Pilot and Control Plane")).toBeInTheDocument();
    expect(screen.getByText("Benchmark Runtime")).toBeInTheDocument();
    expect(screen.getByText("Unattended Invariants")).toBeInTheDocument();
    expect(screen.getByText("Scheduler Lease Fresh")).toBeInTheDocument();
    expect(screen.getByText("Overdue Open Trades")).toBeInTheDocument();
    expect(screen.getByText("Evaluation Failures (24h)")).toBeInTheDocument();
    expect(screen.getByText("Owner token default:worker-host:321:abcdef123456")).toBeInTheDocument();
    expect(screen.getByText("2 overdue open trade(s) remain past market end.")).toBeInTheDocument();
    expect(screen.getByText("Strategy Families")).toBeInTheDocument();
    expect(screen.getByText("Cross-Venue Basis")).toBeInTheDocument();
    expect(screen.getByText("Frozen Benchmark v1")).toBeInTheDocument();
    expect(screen.getByText("25.00 / 100.00")).toBeInTheDocument();
    expect(screen.getAllByText("thin liquidity").length).toBeGreaterThan(0);
    expect(screen.getByText(/default_strategy:constrained:thin_liquidity/)).toBeInTheDocument();
    expect(screen.getByText("Paired executable hedge routing is not implemented yet.")).toBeInTheDocument();
    expect(screen.getAllByText(/Live disabled/).length).toBeGreaterThan(0);
    expect(screen.getByText(/cross_now:2/)).toBeInTheDocument();
    expect(screen.getAllByText(/neg_risk_direct_vs_basket:2/).length).toBeGreaterThan(0);
    expect(screen.getByText(/cross_venue_link_expired:1/)).toBeInTheDocument();
    expect(screen.getByText("Coverage Mode")).toBeInTheDocument();
    expect(screen.getAllByText("healthy").length).toBeGreaterThan(0);
    expect(screen.getByText("Fee Rows")).toBeInTheDocument();
    expect(screen.getByText("Reward Rows")).toBeInTheDocument();
    expect(screen.getByText(/advisory_only_output:5/)).toBeInTheDocument();
    expect(screen.getByText(/active:4/)).toBeInTheDocument();
    expect(screen.getByText(/inventory_toxicity_exceeded:2/)).toBeInTheDocument();
    expect(screen.getByText("Pending Approval")).toBeInTheDocument();
    expect(screen.getByText("Gap Breaches")).toBeInTheDocument();
    expect(screen.getByText("Shadow Evaluation")).toBeInTheDocument();
    expect(screen.getByText("Lifecycle Version")).toBeInTheDocument();
    expect(screen.getAllByText("Autonomy State").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Shadow Only").length).toBeGreaterThan(0);
    expect(screen.getByText("Gate Verdict")).toBeInTheDocument();
    expect(screen.getAllByText("Gate / Autonomy").length).toBeGreaterThan(0);
    expect(screen.getByText("Recent Pilot Incidents")).toBeInTheDocument();
    expect(screen.getByText("Recent Pilot Guardrails")).toBeInTheDocument();
    expect(screen.getByText("gap_suspected")).toBeInTheDocument();
    expect(screen.getAllByText("Execution Policy Infra v1").length).toBeGreaterThan(0);
    expect(screen.getByText("Will the market stay healthy?")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Run Resync" }));
    await waitFor(() => {
      expect(triggerPolymarketResync).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Run Metadata Sync" }));
    await waitFor(() => {
      expect(triggerPolymarketMetadataSync).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Catch Up Projector" }));
    await waitFor(() => {
      expect(triggerPolymarketRawProjector).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Capture Books" }));
    await waitFor(() => {
      expect(triggerPolymarketBookSnapshot).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Backfill Trades" }));
    await waitFor(() => {
      expect(triggerPolymarketTradeBackfill).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Poll OI" }));
    await waitFor(() => {
      expect(triggerPolymarketOiPoll).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Repair Books" }));
    await waitFor(() => {
      expect(triggerPolymarketBookReconResync).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Materialize Features" }));
    await waitFor(() => {
      expect(triggerPolymarketFeatureMaterialization).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Build Groups" }));
    await waitFor(() => {
      expect(triggerPolymarketStructureGroupBuild).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Scan Opportunities" }));
    await waitFor(() => {
      expect(triggerPolymarketStructureOpportunityScan).toHaveBeenCalledWith({ reason: "manual" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Disable" }));
    await waitFor(() => {
      expect(updatePolymarketWatchAsset).toHaveBeenCalledWith(
        "watch-1",
        expect.objectContaining({
          watch_enabled: false,
          watch_reason: "manual_operator_disable",
          priority: 3,
        }),
      );
    });
  });
});
