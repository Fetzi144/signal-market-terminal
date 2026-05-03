import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { getHealth } from "../api";
import Health from "./Health";

vi.mock("../api", () => ({
  getHealth: vi.fn(),
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
      detail: "Owner heartbeat 4s ago, expires in 26s.",
    },
  ],
  strategy_families: [
    {
      family: "default_strategy",
      label: "Default Strategy",
      posture: "benchmark_only",
      primary_surface: "paper_trading",
      description: "Frozen confluence benchmark.",
    },
    {
      family: "structure",
      label: "Legacy Structure",
      posture: "retired",
      primary_surface: "structure",
      description: "Should not render.",
    },
    {
      family: "kalshi_low_yes_fade",
      label: "Kalshi Low-YES Fade",
      posture: "research_active",
      primary_surface: "paper_trading",
      description: "Paper-only Kalshi candidate.",
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  getHealth.mockResolvedValue(healthPayload);
});

test("renders Kalshi-only health without legacy scanner controls", async () => {
  render(<Health />);

  expect(await screen.findByText("Kalshi Only")).toBeInTheDocument();
  expect(screen.getByText("System Health")).toBeInTheDocument();
  expect(screen.getByText("Kalshi Only")).toBeInTheDocument();
  expect(screen.getByText("Default Strategy")).toBeInTheDocument();
  expect(screen.getByText("Kalshi Low-YES Fade")).toBeInTheDocument();
  expect(screen.queryByText("Legacy Structure")).not.toBeInTheDocument();
  expect(screen.queryByText("Polymarket Stream")).not.toBeInTheDocument();
  expect(screen.getByTestId("push-toggle")).toBeInTheDocument();
});

test("refreshes health on demand", async () => {
  render(<Health />);

  expect(await screen.findByText("Kalshi Only")).toBeInTheDocument();
  fireEvent.click(screen.getByText("Refresh"));

  await waitFor(() => expect(getHealth).toHaveBeenCalledTimes(2));
});
