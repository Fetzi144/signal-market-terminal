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
  status: "ok",
  active_markets: 12,
  total_signals: 44,
  unresolved_signals: 7,
  recent_alerts_24h: 3,
  alert_threshold: 0.82,
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
    last_successful_group_build_at: "2026-04-13T10:04:45Z",
    last_successful_scan_at: "2026-04-13T10:05:15Z",
    last_group_build_status: "completed",
    last_scan_status: "completed",
    recent_actionable_by_type: { neg_risk_direct_vs_basket: 2, binary_complement: 1 },
    recent_non_executable_count: 3,
    informational_augmented_group_count: 2,
    active_group_counts: { neg_risk_event: 4, binary_complement: 8, event_sum_parity: 2 },
    active_cross_venue_link_count: 0,
  },
  ingestion: [
    {
      run_type: "market_discovery",
      last_status: "success",
      last_run: "2026-04-13T10:00:00Z",
      markets_processed: 12,
    },
  ],
};

const ingestPayload = {
  connected: true,
  connection_started_at: "2026-04-13T10:02:00Z",
  current_connection_id: "11111111-1111-1111-1111-111111111111",
  last_event_received_at: "2026-04-13T10:04:00Z",
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
    last_successful_group_build_at: "2026-04-13T10:04:45Z",
    last_successful_scan_at: "2026-04-13T10:05:15Z",
    last_group_build_status: "completed",
    last_group_build_started_at: "2026-04-13T10:04:40Z",
    last_scan_status: "completed",
    last_scan_started_at: "2026-04-13T10:05:10Z",
    recent_actionable_by_type: { neg_risk_direct_vs_basket: 2, binary_complement: 1 },
    recent_non_executable_count: 3,
    informational_augmented_group_count: 2,
    active_group_counts: { neg_risk_event: 4, binary_complement: 8, event_sum_parity: 2 },
    active_cross_venue_link_count: 0,
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
    expect(screen.getByText("Phase 8A Structural Edge Engine")).toBeInTheDocument();
    expect(screen.getByText(/Live disabled/)).toBeInTheDocument();
    expect(screen.getByText(/cross_now:2/)).toBeInTheDocument();
    expect(screen.getByText(/neg_risk_direct_vs_basket:2/)).toBeInTheDocument();
    expect(screen.getByText("gap_suspected")).toBeInTheDocument();
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
