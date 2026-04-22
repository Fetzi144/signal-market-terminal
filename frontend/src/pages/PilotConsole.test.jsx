import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import {
  approvePolymarketLiveOrder,
  armPolymarketPilot,
  createPolymarketPilotConfig,
  disarmPolymarketPilot,
  generatePolymarketPilotReadinessReport,
  generatePolymarketPilotScorecard,
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
  generatePolymarketPilotReadinessReport: vi.fn(),
  generatePolymarketPilotScorecard: vi.fn(),
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
    active_strategy_version: {
      id: 2,
      version_key: "exec_policy_infra_v1",
      version_label: "Execution Policy Infra v1",
      version_status: "promoted",
      autonomy_tier: "assisted_live",
    },
    latest_promotion_evaluation: {
      id: 22,
      evaluation_status: "eligible",
      evaluation_kind: "promotion_eligibility_gate",
      autonomy_tier: "assisted_live",
      summary_json: {
        decision: {
          eligible: true,
        },
      },
    },
    active_autonomy_state: {
      recommended_autonomy_tier: "assisted_live",
      effective_autonomy_tier: "assisted_live",
      submission_mode: "manual_approval",
      state_reason: "manual_approval_required",
      blocked_reasons: ["manual_approval_required"],
      gate_kind: "promotion_eligibility_gate",
    },
  },
  active_autonomy_state: {
    recommended_autonomy_tier: "assisted_live",
    effective_autonomy_tier: "assisted_live",
    submission_mode: "manual_approval",
    state_reason: "manual_approval_required",
    blocked_reasons: ["manual_approval_required"],
    gate_kind: "promotion_eligibility_gate",
  },
  active_family_budget: {
    strategy_family: "exec_policy",
    current_outstanding_usd: 25,
    effective_outstanding_cap_usd: 75,
    regime_label: "thin_liquidity",
    capacity_status: "constrained",
    reason_codes: ["capacity_ceiling_exceeded"],
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
  recent_orders: [
    {
      id: "order-1",
      client_order_id: "client-1",
      strategy_version: {
        version_key: "exec_policy_infra_v1",
        version_label: "Execution Policy Infra v1",
      },
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
  evidence_summary: {
    approval_expired_count_24h: 1,
    daily_realized_pnl: { net_realized_pnl: -2.5 },
    strategy_version: {
      id: 2,
      version_key: "exec_policy_infra_v1",
      version_label: "Execution Policy Infra v1",
      version_status: "promoted",
    },
    latest_promotion_evaluation: {
      id: 22,
      evaluation_status: "eligible",
      evaluation_kind: "promotion_eligibility_gate",
      autonomy_tier: "assisted_live",
      summary_json: {
        decision: {
          eligible: true,
        },
      },
    },
    live_shadow_summary: {
      average_gap_bps_24h: 4.2,
      worst_gap_bps_24h: 9.8,
      breach_count_24h: 0,
    },
    latest_readiness_report: {
      status: "manual_only",
      generated_at: "2026-04-15T10:04:00Z",
      strategy_version: {
        version_key: "exec_policy_infra_v1",
        version_label: "Execution Policy Infra v1",
      },
      latest_promotion_evaluation: {
        evaluation_status: "blocked",
        autonomy_tier: "shadow_only",
      },
    },
  },
  guardrail_events: [
    {
      id: 10,
      observed_at_local: "2026-04-15T10:02:00Z",
      guardrail_type: "approval_ttl",
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
      details_json: { reason: "approval_expired" },
    },
  ],
  scorecards: [
    {
      id: 1,
      strategy_version: {
        version_key: "exec_policy_infra_v1",
        version_label: "Execution Policy Infra v1",
      },
      window_start: "2026-04-14T00:00:00Z",
      window_end: "2026-04-15T00:00:00Z",
      status: "watch",
      net_pnl: -2.5,
      avg_shadow_gap_bps: 4.2,
      coverage_limited_count: 0,
    },
  ],
  readiness_reports: [
    {
      id: 1,
      generated_at: "2026-04-15T10:04:00Z",
      strategy_version: {
        version_key: "exec_policy_infra_v1",
        version_label: "Execution Policy Infra v1",
      },
      latest_promotion_evaluation: {
        evaluation_status: "blocked",
        autonomy_tier: "shadow_only",
      },
      status: "manual_only",
      approval_backlog_count: 1,
      shadow_gap_breach_count: 0,
    },
  ],
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
  generatePolymarketPilotScorecard.mockResolvedValue({ ok: true });
  generatePolymarketPilotReadinessReport.mockResolvedValue({ ok: true });
  approvePolymarketLiveOrder.mockResolvedValue({ ok: true });
  rejectPolymarketLiveOrder.mockResolvedValue({ ok: true });
  setPolymarketLiveKillSwitch.mockResolvedValue({ enabled: true });
});

describe("PilotConsole", () => {
  test("renders supervised pilot state and operator actions", async () => {
    render(
      <MemoryRouter>
        <PilotConsole />
      </MemoryRouter>
    );

    expect(await screen.findByText("Pilot Console")).toBeInTheDocument();
    expect(screen.getByText("Manual Approval Queue")).toBeInTheDocument();
    expect(screen.getByText("Live vs Shadow")).toBeInTheDocument();
    expect(screen.getByText("Recent Incidents")).toBeInTheDocument();
    expect(screen.getByText("phase12-exec (exec_policy)")).toBeInTheDocument();
    expect(screen.getByText("Autonomy State")).toBeInTheDocument();
    expect(screen.getByText("Assisted Live")).toBeInTheDocument();
    expect(screen.getAllByText("Execution Policy Infra v1").length).toBeGreaterThan(0);
    expect(screen.getByText("Eligible")).toBeInTheDocument();
    expect(screen.queryByText(/^observe$/i)).not.toBeInTheDocument();
    expect(screen.getAllByText("Gate / Autonomy").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Blocked").length).toBeGreaterThan(0);
    expect(screen.getByText("$25.00 / $75.00")).toBeInTheDocument();
    expect(screen.getByText("thin_liquidity")).toBeInTheDocument();
    expect(screen.getByText("constrained")).toBeInTheDocument();
    expect(screen.getByText("capacity_ceiling_exceeded")).toBeInTheDocument();
    expect(screen.getByText(/Recommended tier: Assisted Live \| Autonomy reason: Manual Approval Required \| Blockers: Manual Approval Required/)).toBeInTheDocument();
    expect(screen.getByText(/Autonomy reason:/)).toBeInTheDocument();

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
