import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  approvePolymarketLiveOrder,
  armPolymarketPilot,
  createPolymarketPilotConfig,
  disarmPolymarketPilot,
  getPolymarketPilotConfigs,
  getPolymarketPilotConsoleSummary,
  pausePolymarketPilot,
  rejectPolymarketLiveOrder,
  resumePolymarketPilot,
  setPolymarketLiveKillSwitch,
} from "../api";
import PilotConsole from "./PilotConsole";

vi.mock("../api", () => ({
  approvePolymarketLiveOrder: vi.fn(),
  armPolymarketPilot: vi.fn(),
  createPolymarketPilotConfig: vi.fn(),
  disarmPolymarketPilot: vi.fn(),
  getPolymarketPilotConfigs: vi.fn(),
  getPolymarketPilotConsoleSummary: vi.fn(),
  pausePolymarketPilot: vi.fn(),
  rejectPolymarketLiveOrder: vi.fn(),
  resumePolymarketPilot: vi.fn(),
  setPolymarketLiveKillSwitch: vi.fn(),
}));

const summaryPayload = {
  active_pilot_family: "exec_policy",
  pilot: {
    pilot_enabled: true,
    heartbeat_status: "healthy",
    manual_approval_required: true,
    approval_queue_count: 1,
    recent_incident_count_24h: 1,
    open_live_order_count: 1,
    kill_switch_enabled: false,
    active_pilot: {
      id: 11,
      pilot_name: "phase12-exec",
      strategy_family: "exec_policy",
      armed: true,
    },
    active_run: {
      id: "run-1",
      status: "armed",
    },
  },
  approvals: [
    {
      id: "order-1",
      client_order_id: "client-1",
      condition_id: "cond-1",
      approval_state: "queued",
      created_at: "2026-04-15T10:00:00Z",
      approval_expires_at: "2026-04-15T10:05:00Z",
    },
  ],
  incidents: [
    {
      id: 1,
      observed_at_local: "2026-04-15T10:01:00Z",
      incident_type: "submission_blocked",
      severity: "warning",
      details_json: { reason: "manual_approval_required" },
    },
  ],
  recent_orders: [
    {
      id: "order-1",
      client_order_id: "client-1",
      status: "approval_pending",
      approval_state: "queued",
      requested_size: "25",
      created_at: "2026-04-15T10:00:00Z",
    },
  ],
  recent_fills: [
    {
      id: "fill-1",
      asset_id: "asset-1",
      fill_status: "matched",
      price: "0.52",
      size: "10",
      observed_at_local: "2026-04-15T10:02:00Z",
    },
  ],
  recent_blocked_submissions: [
    {
      id: 1,
      observed_at_local: "2026-04-15T10:03:00Z",
      details_json: { reason: "pilot_not_armed" },
      live_order_id: "order-2",
    },
  ],
  live_shadow_summary: {
    recent_count_24h: 2,
    average_gap_bps_24h: 4.2,
    worst_gap_bps_24h: 9.8,
    breach_count_24h: 0,
  },
};

const configPayload = {
  rows: [
    {
      id: 11,
      pilot_name: "phase12-exec",
      strategy_family: "exec_policy",
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  getPolymarketPilotConsoleSummary.mockResolvedValue(summaryPayload);
  getPolymarketPilotConfigs.mockResolvedValue(configPayload);
  createPolymarketPilotConfig.mockResolvedValue({ id: 12 });
  armPolymarketPilot.mockResolvedValue({ ok: true });
  pausePolymarketPilot.mockResolvedValue({ ok: true });
  resumePolymarketPilot.mockResolvedValue({ ok: true });
  disarmPolymarketPilot.mockResolvedValue({ ok: true });
  approvePolymarketLiveOrder.mockResolvedValue({ ok: true });
  rejectPolymarketLiveOrder.mockResolvedValue({ ok: true });
  setPolymarketLiveKillSwitch.mockResolvedValue({ enabled: true });
});

describe("PilotConsole", () => {
  test("renders supervised pilot state and operator actions", async () => {
    render(<PilotConsole />);

    expect(await screen.findByText("Pilot Console")).toBeInTheDocument();
    expect(screen.getByText("Manual Approval Queue")).toBeInTheDocument();
    expect(screen.getByText("Live vs Shadow")).toBeInTheDocument();
    expect(screen.getByText("Recent Incidents")).toBeInTheDocument();
    expect(screen.getByText("phase12-exec (exec_policy)")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Arm" }));
    await waitFor(() => {
      expect(armPolymarketPilot).toHaveBeenCalledWith({
        pilot_config_id: 11,
        operator_identity: "operator",
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await waitFor(() => {
      expect(approvePolymarketLiveOrder).toHaveBeenCalledWith("order-1", { approved_by: "operator" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Kill Switch Off" }));
    await waitFor(() => {
      expect(setPolymarketLiveKillSwitch).toHaveBeenCalledWith(true);
    });
  });
});
