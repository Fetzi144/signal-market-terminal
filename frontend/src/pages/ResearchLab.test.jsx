import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { createResearchBatch, getLatestResearchBatch } from "../api";
import ResearchLab from "./ResearchLab";

vi.mock("../api", () => ({
  createResearchBatch: vi.fn(),
  getApiBase: () => "/api/v1",
  getLatestResearchBatch: vi.fn(),
  getProductionUrl: () => "http://production.example",
  isLocalApiBase: () => false,
}));

beforeEach(() => {
  vi.clearAllMocks();
});

function latestPayload() {
  return {
    batch: {
      id: "batch-1",
      status: "completed",
      window_start: "2026-03-27T00:00:00Z",
      window_end: "2026-04-27T00:00:00Z",
      universe: { market_count: 42, signal_count: 100 },
    },
    lane_results: [
      {
        id: 1,
        rank_position: 1,
        family: "structure",
        lane: "structure_replay",
        source_kind: "polymarket_replay",
        source_ref: "run-1",
        verdict: "healthy",
        realized_pnl: null,
        replay_net_pnl: 12.5,
        avg_clv: 0.01,
        resolved_trades: 20,
        fill_rate: 0.7,
      },
      {
        id: 2,
        rank_position: 2,
        family: "default_strategy",
        lane: "profitability_gate",
        source_kind: "profitability_snapshot",
        source_ref: "snapshot",
        verdict: "insufficient_evidence",
        realized_pnl: -2,
        replay_net_pnl: 0,
        avg_clv: null,
        resolved_trades: 4,
        fill_rate: null,
      },
    ],
    top_blockers: [{ blocker: "insufficient_resolved_trades", count: 1 }],
    top_ev_candidates: [
      {
        family: "structure",
        lane: "structure_replay",
        label: "structure_opportunity:7",
        why: "actionable structure opportunity included in replay lane",
        source_kind: "structure_opportunity",
        source_ref: "7",
      },
    ],
    data_readiness: {
      status: "ready",
      summary: "The selected universe has enough stored evidence for ranked paper research.",
      counts: {
        outcome_count: 84,
        price_snapshot_count: 500,
        orderbook_snapshot_count: 100,
        failed_lane_count: 0,
      },
      actions: [],
    },
  };
}

test("renders latest research batch scoreboard and candidates", async () => {
  getLatestResearchBatch.mockResolvedValue(latestPayload());

  render(
    <MemoryRouter>
      <ResearchLab />
    </MemoryRouter>,
  );

  expect(await screen.findByText("Research Lab")).toBeInTheDocument();
  expect(screen.getByText("structure_replay")).toBeInTheDocument();
  expect(screen.getByText("batch-1")).toBeInTheDocument();
  expect(screen.getByText("/api/v1")).toBeInTheDocument();
  expect(screen.getByText("insufficient_resolved_trades")).toBeInTheDocument();
  expect(screen.getByText("structure_opportunity:7")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Replay" })).toHaveAttribute("href", "/strategies");
});

test("explains an empty local research run instead of naming a fake best lane", async () => {
  getLatestResearchBatch.mockResolvedValue({
    batch: {
      id: "batch-empty",
      status: "completed_with_warnings",
      window_start: "2026-03-27T00:00:00Z",
      window_end: "2026-04-27T00:00:00Z",
      universe: {
        market_count: 0,
        outcome_count: 0,
        signal_count: 0,
        price_snapshot_count: 0,
        orderbook_snapshot_count: 0,
      },
    },
    lane_results: [
      {
        id: 1,
        rank_position: 1,
        family: "default_strategy",
        lane: "profitability_gate",
        source_kind: "research_lab",
        verdict: "insufficient_evidence",
        status: "failed",
        blockers: ["lane_execution_failed"],
        resolved_trades: 0,
      },
    ],
    top_blockers: [{ blocker: "lane_execution_failed", count: 1 }],
    top_ev_candidates: [],
  });

  render(
    <MemoryRouter>
      <ResearchLab />
    </MemoryRouter>,
  );

  expect(await screen.findByText("No viable lane yet")).toBeInTheDocument();
  expect(screen.getByText("Data Readiness")).toBeInTheDocument();
  expect(screen.getByText("This batch ran against a backend with zero selected markets.")).toBeInTheDocument();
  expect(screen.getByText("Connect the lab to populated data")).toBeInTheDocument();
});

test("starts a profit hunt batch", async () => {
  getLatestResearchBatch.mockResolvedValue(null);
  createResearchBatch.mockResolvedValue({
    batch: { id: "batch-2", status: "pending", universe: { market_count: 0, signal_count: 0 } },
  });

  render(
    <MemoryRouter>
      <ResearchLab />
    </MemoryRouter>,
  );

  fireEvent.click(await screen.findByRole("button", { name: "Run Profit Hunt" }));

  await waitFor(() => {
    expect(createResearchBatch).toHaveBeenCalledWith();
  });
});
