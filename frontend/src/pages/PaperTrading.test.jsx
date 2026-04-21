import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import {
  getPaperTradingDefaultStrategyDashboard,
  getPaperTradingHistory,
} from "../api";
import PaperTrading from "./PaperTrading";

vi.mock("../api", () => ({
  getPaperTradingDefaultStrategyDashboard: vi.fn(),
  getPaperTradingHistory: vi.fn(),
}));

const historyPayload = {
  trades: [],
  total: 0,
};

const baseDashboard = {
  portfolio: {
    open_trades: [],
    open_exposure: 40,
    total_resolved: 3,
    cumulative_pnl: 12.5,
    wins: 2,
    losses: 1,
    win_rate: 0.667,
  },
  metrics: {
    total_trades: 0,
    wins: 0,
    losses: 0,
    win_rate: 0,
    cumulative_pnl: 0,
    shadow_cumulative_pnl: 0,
    avg_pnl: 0,
    max_drawdown: 0,
    sharpe_ratio: 0,
    profit_factor: null,
    shadow_profit_factor: null,
    best_trade: 0,
    worst_trade: 0,
    liquidity_constrained_trades: 0,
    trades_missing_orderbook_context: 0,
  },
  pnl_curve: [],
  strategy_health: {
    strategy: {
      display_name: "Frozen Default Strategy",
      objective: "Prove or falsify the frozen default-strategy edge before widening automation.",
      signal_type: "confluence",
      baseline_start_at: "2026-04-01T00:00:00Z",
      ev_threshold: 0.03,
      kelly_multiplier: 0.25,
      paper_bankroll_usd: 1000,
      max_single_position_pct: 0.05,
      max_total_exposure_pct: 0.25,
      minimum_observation_days: 14,
      preferred_observation_days: 30,
      legacy_benchmark_rank_threshold: 0.67,
    },
    strategy_run: {
      id: "run-active",
      strategy_name: "default_strategy",
      status: "active",
      started_at: "2026-04-10T10:00:00Z",
      created_at: "2026-04-10T10:00:00Z",
      ended_at: null,
    },
    observation: {
      status: "minimum_window_reached",
      days_tracked: 14.2,
      minimum_days: 14,
      preferred_days: 30,
      days_until_minimum_window: 0,
      baseline_start_at: "2026-04-01T00:00:00Z",
      first_trade_at: "2026-04-11T10:00:00Z",
    },
    headline: {
      open_exposure: 40,
      open_trades: 1,
      resolved_trades: 3,
      cumulative_pnl: 12.5,
      avg_clv: 0.04,
      missing_resolutions: 0,
      resolved_signals: 3,
      win_rate: 0.667,
      total_profit_loss_per_share: 0.08,
      max_drawdown_per_share: -0.03,
    },
    trade_funnel: {
      candidate_signals: 10,
      qualified_signals: 5,
      traded_signals: 3,
      resolved_signals: 3,
      qualified_not_traded: 2,
      excluded_legacy_trades: 0,
      pre_launch_candidate_signals: 1,
      excluded_pre_launch_trades: 0,
    },
    skip_reasons: [
      {
        reason_code: "no_liquidity",
        reason_label: "no_liquidity",
        count: 2,
      },
    ],
    benchmark: {
      resolved_signals: 4,
      win_rate: 0.5,
      avg_clv: 0.02,
      total_profit_loss_per_share: 0.01,
      max_drawdown_per_share: -0.05,
      delta_profit_loss_per_share: 0.07,
      delta_max_drawdown_per_share: 0.02,
    },
    replay: {
      coverage_mode: "supported_detectors_only",
    },
    review_verdict: {
      verdict: "watch",
      summary: "The baseline still needs more evidence before any keep or cut decision.",
      blockers: [
        {
          code: "insufficient_observation_days",
          label: "Observation window still maturing",
          detail: "Wait for more settled trades before locking a verdict.",
        },
      ],
    },
    execution_realism: {
      shadow_cumulative_pnl: 10,
      shadow_profit_factor: 1.4,
      liquidity_constrained_trades: 0,
      trades_missing_orderbook_context: 0,
    },
    run_integrity: {
      pre_launch_candidate_signals: 1,
      excluded_pre_launch_trades: 0,
      excluded_legacy_trades: 0,
      trades_missing_orderbook_context: 0,
    },
    detector_review: [
      {
        signal_type: "confluence",
        note: "Primary default-strategy detector.",
        resolved_signals: 3,
        paper_trades: 3,
        avg_clv: 0.04,
        total_profit_loss: 0.08,
        paper_trade_pnl: 12.5,
        brier_score: 0.16,
        verdict: "keep",
      },
    ],
    recent_mistakes: [],
    review_questions: [
      "Does the frozen baseline still outperform the legacy benchmark after fees and slippage?",
    ],
    latest_review_artifact: {
      generation_status: "missing",
      status_detail: "No review artifact has been generated for the active default-strategy baseline yet.",
      review_date: null,
      generated_at: null,
      verdict: null,
      strategy_run_ref: {},
      contract_ref: {},
      artifact_paths: {
        markdown: null,
        json: null,
      },
      generation_guidance: {
        working_directory: "backend",
        command: "python -m app.reports",
        runbook_path: "docs/runbooks/default-strategy-controlled-evidence-relaunch.md",
        artifacts_directory: "docs/strategy-reviews",
        analysis_path: "docs/paper-trading-analysis-v0.5.md",
        note: "Read-only health and dashboard surfaces never generate review artifacts.",
      },
    },
    evidence_freshness: {
      status: "missing_review",
      summary: "No review has been generated for the active baseline yet.",
      latest_review_generation_status: "missing",
      latest_review_generated_at: null,
      review_age_seconds: null,
      review_lag_seconds: null,
      review_outdated: true,
      artifact_identity_status: "missing_review",
      artifact_identity_summary: "No review artifact exists yet for the active run.",
      last_activity_at: "2026-04-21T08:00:00Z",
      last_activity_kind: "paper_trade_resolution",
      pending_decision_count: 0,
      pending_decision_max_age_seconds: 0,
      pending_decisions_stale: false,
      pending_decision_stale_after_seconds: 86400,
    },
  },
};

function makeDashboard(overrides = {}) {
  const dashboard = JSON.parse(JSON.stringify(baseDashboard));
  const { strategy_health: healthOverrides = {}, ...dashboardOverrides } = overrides;
  const {
    latest_review_artifact: artifactOverrides = {},
    evidence_freshness: freshnessOverrides = {},
    ...strategyHealthOverrides
  } = healthOverrides;

  Object.assign(dashboard, dashboardOverrides);
  Object.assign(dashboard.strategy_health, strategyHealthOverrides);
  Object.assign(
    dashboard.strategy_health.latest_review_artifact,
    artifactOverrides,
  );
  Object.assign(
    dashboard.strategy_health.evidence_freshness,
    freshnessOverrides,
  );

  return dashboard;
}

beforeEach(() => {
  vi.clearAllMocks();
  getPaperTradingDefaultStrategyDashboard.mockResolvedValue(makeDashboard());
  getPaperTradingHistory.mockResolvedValue(historyPayload);
});

describe("PaperTrading latest review surface", () => {
  test("shows manual regeneration guidance when the latest review artifact is missing", async () => {
    render(
      <MemoryRouter>
        <PaperTrading />
      </MemoryRouter>
    );

    expect(await screen.findByText("Latest Review Artifact")).toBeInTheDocument();
    expect(
      screen.getByText("No review artifact has been generated for the active default-strategy baseline yet."),
    ).toBeInTheDocument();
    expect(screen.getByText("Missing Review")).toBeInTheDocument();
    expect(screen.getByText("No Review Identity")).toBeInTheDocument();
    expect(
      screen.getByText(
        "This review needs manual regeneration from the canonical operator path below. The read-only dashboard will not generate artifacts for you.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("backend/: python -m app.reports")).toBeInTheDocument();

    await waitFor(() => {
      expect(getPaperTradingHistory).toHaveBeenCalledWith({
        status: undefined,
        direction: undefined,
        scope: "default_strategy",
        page: 1,
        pageSize: 20,
      });
    });
  });

  test("surfaces stale run-mismatch evidence details for the latest review artifact", async () => {
    getPaperTradingDefaultStrategyDashboard.mockResolvedValueOnce(
      makeDashboard({
        strategy_health: {
          latest_review_artifact: {
            generation_status: "complete",
            status_detail: "The newest review artifact belongs to an older frozen-baseline run.",
            review_date: "2026-04-05",
            generated_at: "2026-04-05T09:30:00Z",
            verdict: "watch",
            strategy_run_ref: {
              id: "run-old",
              started_at: "2026-04-05T09:00:00Z",
              status: "completed",
            },
            contract_ref: {
              contract_version: "default-strategy-v1",
              evidence_boundary_id: "baseline-2026-04-05",
              migration_revision: "phase12b",
            },
            artifact_paths: {
              markdown: "docs/strategy-reviews/2026-04-05-default-strategy-baseline.md",
              json: "docs/strategy-reviews/2026-04-05-default-strategy-baseline.json",
            },
          },
          evidence_freshness: {
            status: "stale",
            summary: "Active run activity has outpaced the latest review artifact.",
            latest_review_generation_status: "complete",
            latest_review_generated_at: "2026-04-05T09:30:00Z",
            review_age_seconds: 86400,
            review_lag_seconds: 7200,
            review_outdated: true,
            artifact_identity_status: "mismatch",
            artifact_identity_summary: "Latest review artifact belongs to run-old while the active run is run-active.",
            last_activity_at: "2026-04-21T09:00:00Z",
            last_activity_kind: "paper_trade_resolution",
            pending_decision_count: 1,
            pending_decision_max_age_seconds: 600,
            pending_decisions_stale: false,
            pending_decision_stale_after_seconds: 86400,
          },
        },
      }),
    );

    render(
      <MemoryRouter>
        <PaperTrading />
      </MemoryRouter>
    );

    expect(await screen.findByText("Run Mismatch")).toBeInTheDocument();
    expect(screen.getByText("Stale")).toBeInTheDocument();
    expect(
      screen.getByText("Active run activity has outpaced the latest review artifact."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Latest review artifact belongs to run-old while the active run is run-active."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("docs/strategy-reviews/2026-04-05-default-strategy-baseline.md"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("docs/strategy-reviews/2026-04-05-default-strategy-baseline.json"),
    ).toBeInTheDocument();
    expect(screen.getByText("Manual Review Generation")).toBeInTheDocument();
    expect(screen.getByText("backend/: python -m app.reports")).toBeInTheDocument();
  });
});
