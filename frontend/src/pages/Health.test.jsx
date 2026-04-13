import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  getHealth,
  getPolymarketIngestStatus,
  getPolymarketWatchAssets,
  triggerPolymarketResync,
  updatePolymarketWatchAsset,
} from "../api";
import Health from "./Health";

vi.mock("../api", () => ({
  getHealth: vi.fn(),
  getPolymarketIngestStatus: vi.fn(),
  getPolymarketWatchAssets: vi.fn(),
  triggerPolymarketResync: vi.fn(),
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
    expect(screen.getByText("gap_suspected")).toBeInTheDocument();
    expect(screen.getByText("Will the market stay healthy?")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Run Resync" }));
    await waitFor(() => {
      expect(triggerPolymarketResync).toHaveBeenCalledWith({ reason: "manual" });
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
