import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  getHealth,
  getPolymarketIngestStatus,
  getPolymarketWatchAssets,
  triggerPolymarketBookSnapshot,
  triggerPolymarketMetadataSync,
  triggerPolymarketOiPoll,
  triggerPolymarketRawProjector,
  triggerPolymarketResync,
  triggerPolymarketTradeBackfill,
  updatePolymarketWatchAsset,
} from "../api";
import Health from "./Health";

vi.mock("../api", () => ({
  getHealth: vi.fn(),
  getPolymarketIngestStatus: vi.fn(),
  getPolymarketWatchAssets: vi.fn(),
  triggerPolymarketBookSnapshot: vi.fn(),
  triggerPolymarketMetadataSync: vi.fn(),
  triggerPolymarketOiPoll: vi.fn(),
  triggerPolymarketRawProjector: vi.fn(),
  triggerPolymarketResync: vi.fn(),
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
});

describe("Health", () => {
  test("renders Polymarket stream details and operator controls", async () => {
    render(<Health />);

    expect(await screen.findByText("Polymarket Stream")).toBeInTheDocument();
    expect(screen.getByText("Connected")).toBeInTheDocument();
    expect(screen.getByText("Phase 2 Metadata Registry")).toBeInTheDocument();
    expect(screen.getByText("Phase 3 Raw Storage")).toBeInTheDocument();
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
