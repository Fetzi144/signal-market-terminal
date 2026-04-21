import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { getStrategiesRegistry } from "../api";
import Strategies from "./Strategies";

vi.mock("../api", () => ({
  getStrategiesRegistry: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  getStrategiesRegistry.mockResolvedValue({
    summary: {
      phase: "13A",
      family_count: 2,
      version_count: 2,
      gate_policy_count: 1,
      benchmark_family: "default_strategy",
    },
    gate_policies: [
      {
        id: 1,
        policy_key: "promotion_gate_policy_v1",
        label: "Promotion Gate Policy v1",
        status: "active",
        policy_json: {
          required_inputs: ["minimum_live_sample_size", "acceptable_drawdown"],
        },
        updated_at: "2026-04-21T08:00:00Z",
      },
    ],
    families: [
      {
        id: 1,
        family: "default_strategy",
        label: "Default Strategy",
        posture: "benchmark_only",
        configured: true,
        review_enabled: true,
        primary_surface: "paper_trading",
        description: "Frozen benchmark truth anchor.",
        disabled_reason: null,
        family_kind: "strategy",
        seeded_from: "builtin",
        updated_at: "2026-04-21T08:00:00Z",
        current_version: {
          id: 1,
          version_key: "default_strategy_benchmark_v1",
          version_label: "Frozen Benchmark v1",
          strategy_name: "prove_the_edge_default",
          version_status: "benchmark",
          autonomy_tier: "shadow_only",
          is_current: true,
          is_frozen: true,
          config_json: {},
          provenance_json: {},
          evidence_counts: {
            strategy_runs: 2,
            paper_trades: 4,
            replay_runs: 0,
            live_orders: 0,
            pilot_scorecards: 0,
            readiness_reports: 0,
          },
          updated_at: "2026-04-21T08:00:00Z",
        },
        versions: [
          {
            id: 1,
            version_key: "default_strategy_benchmark_v1",
            version_label: "Frozen Benchmark v1",
            strategy_name: "prove_the_edge_default",
            version_status: "benchmark",
            autonomy_tier: "shadow_only",
            is_current: true,
            is_frozen: true,
            config_json: {},
            provenance_json: {},
            evidence_counts: {
              strategy_runs: 2,
              paper_trades: 4,
              replay_runs: 0,
              live_orders: 0,
              pilot_scorecards: 0,
              readiness_reports: 0,
            },
            updated_at: "2026-04-21T08:00:00Z",
          },
        ],
        latest_promotion_evaluation: {
          id: 11,
          family_id: 1,
          strategy_version_id: 1,
          gate_policy_id: 1,
          evaluation_kind: "pilot_readiness_gate",
          evaluation_status: "observe",
          autonomy_tier: "assisted_live",
          evaluation_window_start: "2026-04-20T00:00:00Z",
          evaluation_window_end: "2026-04-21T00:00:00Z",
          provenance_json: {
            promotion_gate_policy_key: "promotion_gate_policy_v1",
            config_hash: "cfg-default-1234",
            market_universe_hash: "mkt-default-5678",
          },
          summary_json: {
            readiness_status: "manual_only",
            readiness_blockers: [],
            incident_count: 0,
            approval_backlog_count: 0,
          },
          created_at: "2026-04-21T08:00:00Z",
          updated_at: "2026-04-21T08:00:00Z",
        },
        latest_demotion_event: null,
      },
      {
        id: 2,
        family: "exec_policy",
        label: "Execution Policy",
        posture: "advisory_only",
        configured: true,
        review_enabled: true,
        primary_surface: "pilot_console",
        description: "Shared execution infrastructure.",
        disabled_reason: null,
        family_kind: "infrastructure",
        seeded_from: "builtin",
        updated_at: "2026-04-21T08:00:00Z",
        current_version: {
          id: 2,
          version_key: "exec_policy_infra_v1",
          version_label: "Execution Policy Infra v1",
          strategy_name: null,
          version_status: "promoted",
          autonomy_tier: "assisted_live",
          is_current: true,
          is_frozen: false,
          config_json: {},
          provenance_json: {},
          evidence_counts: {
            strategy_runs: 0,
            paper_trades: 0,
            replay_runs: 1,
            live_orders: 3,
            pilot_scorecards: 2,
            readiness_reports: 1,
          },
          updated_at: "2026-04-21T08:00:00Z",
        },
        versions: [
          {
            id: 2,
            version_key: "exec_policy_infra_v1",
            version_label: "Execution Policy Infra v1",
            strategy_name: null,
            version_status: "promoted",
            autonomy_tier: "assisted_live",
            is_current: true,
            is_frozen: false,
            config_json: {},
            provenance_json: {},
            evidence_counts: {
              strategy_runs: 0,
              paper_trades: 0,
              replay_runs: 1,
              live_orders: 3,
              pilot_scorecards: 2,
              readiness_reports: 1,
            },
            updated_at: "2026-04-21T08:00:00Z",
          },
        ],
        latest_promotion_evaluation: {
          id: 12,
          family_id: 2,
          strategy_version_id: 2,
          gate_policy_id: 1,
          evaluation_kind: "pilot_readiness_gate",
          evaluation_status: "candidate",
          autonomy_tier: "bounded_auto_submit",
          evaluation_window_start: "2026-04-20T00:00:00Z",
          evaluation_window_end: "2026-04-21T00:00:00Z",
          provenance_json: {
            promotion_gate_policy_key: "promotion_gate_policy_v1",
            config_hash: "cfg-exec-1234",
            market_universe_hash: "mkt-exec-5678",
          },
          summary_json: {
            readiness_status: "candidate_for_semi_auto",
            readiness_blockers: [],
            incident_count: 0,
            approval_backlog_count: 0,
          },
          created_at: "2026-04-21T08:00:00Z",
          updated_at: "2026-04-21T08:00:00Z",
        },
        latest_demotion_event: null,
      },
    ],
    generated_at: "2026-04-21T08:00:00Z",
  });
});

describe("Strategies", () => {
  test("renders the phase 13 lifecycle registry surface", async () => {
    render(
      <MemoryRouter>
        <Strategies />
      </MemoryRouter>
    );

    expect(await screen.findByText("Strategies")).toBeInTheDocument();
    expect(screen.getAllByText("Frozen Benchmark v1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Execution Policy Infra v1").length).toBeGreaterThan(0);
    expect(screen.getByText("Promotion Gate Policy v1")).toBeInTheDocument();
    expect(screen.getAllByText("Latest Gate Verdict").length).toBeGreaterThan(0);
    expect(screen.getByText("cfg-exec-1234")).toBeInTheDocument();
    expect(screen.getByText("Benchmark Health")).toBeInTheDocument();
    expect(screen.getByText("Pilot Console")).toBeInTheDocument();
  });
});
