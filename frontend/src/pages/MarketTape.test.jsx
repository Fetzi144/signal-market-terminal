import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { getPolymarketMarketTape, getPolymarketPilotStatus } from "../api";
import MarketTape from "./MarketTape";

vi.mock("../api", () => ({
  getPolymarketMarketTape: vi.fn(),
  getPolymarketPilotStatus: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  getPolymarketPilotStatus.mockResolvedValue({
    heartbeat_status: "healthy",
    active_pilot: {
      armed: true,
      strategy_family: "exec_policy",
    },
  });
  getPolymarketMarketTape.mockResolvedValue({
    selected_condition_id: "cond-1",
    selected_asset_id: "asset-1",
    recon_state: {
      status: "live",
      best_bid: "0.51",
      best_ask: "0.53",
      spread: "0.02",
    },
    bbo: [
      {
        id: 1,
        event_ts_exchange: "2026-04-15T10:00:00Z",
        best_bid: "0.51",
        best_ask: "0.53",
        spread: "0.02",
      },
    ],
    trades: [
      {
        id: 1,
        event_ts_exchange: "2026-04-15T10:00:10Z",
        side: "BUY",
        price: "0.52",
        size: "14",
        outcome_name: "Yes",
      },
    ],
    structure_context: [
      {
        id: 1,
        created_at: "2026-04-15T10:00:20Z",
        opportunity_type: "neg_risk_direct_vs_basket",
        classification: "actionable_candidate",
        reason_code: "positive_edge",
      },
    ],
    quote_context: [
      {
        id: "quote-1",
        created_at: "2026-04-15T10:00:30Z",
        status: "advisory_only",
        recommendation_action: "hold",
        asset_id: "asset-1",
      },
    ],
    live_orders: [
      {
        id: "order-1",
        created_at: "2026-04-15T10:01:00Z",
        client_order_id: "client-1",
        status: "approval_pending",
        approval_state: "queued",
        blocked_reason_code: null,
        validation_error: null,
      },
    ],
    live_order_events: [
      {
        id: 1,
        observed_at_local: "2026-04-15T10:01:05Z",
        event_type: "approval_requested",
        venue_status: null,
        details_json: { reason: "manual_approval_required" },
        payload_json: {},
        source_kind: "control_plane",
      },
    ],
  });
});

describe("MarketTape", () => {
  test("renders pilot market tape and refreshes selected filters", async () => {
    render(<MarketTape />);

    expect(await screen.findByText("Market Tape")).toBeInTheDocument();
    expect(screen.getByText("Recent BBO")).toBeInTheDocument();
    expect(screen.getByText("Recent Trades")).toBeInTheDocument();
    expect(screen.getByText("Structure Context")).toBeInTheDocument();
    expect(screen.getByText("Live Orders Overlay")).toBeInTheDocument();
    expect(screen.getByText("neg_risk_direct_vs_basket")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Condition"), { target: { value: "cond-updated" } });
    await waitFor(() => {
      expect(getPolymarketMarketTape).toHaveBeenLastCalledWith(
        expect.objectContaining({ conditionId: "cond-updated" }),
      );
    });
  });
});
