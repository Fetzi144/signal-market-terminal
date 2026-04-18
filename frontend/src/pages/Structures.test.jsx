import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  approvePolymarketStructurePaperPlan,
  createPolymarketStructurePaperPlan,
  getPolymarketFeeHistory,
  getPolymarketRewardHistory,
  getPolymarketStructureOpportunity,
  getPolymarketStructureLatestMakerEconomics,
  getPolymarketStructureLatestQuoteRecommendation,
  getPolymarketStructureOpportunities,
  getPolymarketStructureStatus,
  runPolymarketStructureMakerEconomics,
  runPolymarketStructureQuoteRecommendation,
  routePolymarketStructurePaperPlan,
  validatePolymarketStructureOpportunities,
} from "../api";
import Structures from "./Structures";

vi.mock("../api", () => ({
  approvePolymarketStructurePaperPlan: vi.fn(),
  createPolymarketStructurePaperPlan: vi.fn(),
  getPolymarketFeeHistory: vi.fn(),
  getPolymarketRewardHistory: vi.fn(),
  getPolymarketStructureOpportunity: vi.fn(),
  getPolymarketStructureLatestMakerEconomics: vi.fn(),
  getPolymarketStructureLatestQuoteRecommendation: vi.fn(),
  getPolymarketStructureOpportunities: vi.fn(),
  getPolymarketStructureStatus: vi.fn(),
  rejectPolymarketStructurePaperPlan: vi.fn(),
  runPolymarketStructureMakerEconomics: vi.fn(),
  runPolymarketStructureQuoteRecommendation: vi.fn(),
  routePolymarketStructurePaperPlan: vi.fn(),
  validatePolymarketStructureOpportunities: vi.fn(),
}));

const statusPayload = {
  executable_candidate_count: 1,
  informational_only_opportunity_count: 2,
  blocked_opportunity_count: 1,
  pending_approval_count: 1,
  stale_cross_venue_link_count: 1,
  last_successful_validation_at: "2026-04-13T10:05:20Z",
  validation_reason_counts: {
    cross_venue_link_expired: 1,
    no_positive_current_edge: 2,
  },
};

const opportunitiesPayload = {
  rows: [
    {
      id: 101,
      group_title: "State race linkage",
      opportunity_type: "cross_venue_basis",
      group_type: "cross_venue_basis",
      validation_classification: "executable_candidate",
      validation_current_net_edge_bps: "142.5",
      net_edge_bps: "160.0",
      plan_status: null,
      cross_venue_review_status: "approved",
    },
  ],
  limit: 100,
};

const baseDetail = {
  opportunity: {
    id: 101,
    opportunity_type: "cross_venue_basis",
    observed_at_local: "2026-04-13T10:05:15Z",
    net_edge_bps: "160.0",
  },
  group: {
    title: "State race linkage",
  },
  latest_validation: {
    classification: "executable_candidate",
    current_net_edge_bps: "142.5",
    detected_age_seconds: 25,
    max_leg_age_seconds: 8,
    summary_json: {
      reason_labels: {},
    },
  },
  legs: [
    {
      id: 1,
      leg_index: 0,
      venue: "polymarket",
      side: "buy_yes",
      asset_id: "token-yes",
      condition_id: "cond-1",
      target_size: "1.00",
      est_avg_entry_price: "0.41",
      est_slippage_bps: "12.0",
      valid: true,
    },
    {
      id: 2,
      leg_index: 1,
      venue: "kalshi",
      side: "buy_no",
      target_size: "1.00",
      est_avg_entry_price: "0.28",
      est_slippage_bps: "8.0",
      valid: true,
    },
  ],
  cross_venue_link: {
    effective_review_status: "approved",
    confidence: "0.91",
    owner: "ops",
    provenance_source: "reviewed_sheet",
    reviewed_by: "analyst",
    expires_at: "2026-05-01T00:00:00Z",
    notes: "Manual link",
  },
  paper_plans: [],
};

const approvalPendingPlan = {
  id: "plan-1",
  status: "approval_pending",
  manual_approval_required: true,
  approved_by: null,
  rejected_by: null,
  plan_notional_total: "0.69",
  reason_codes_json: [],
  created_at: "2026-04-13T10:06:00Z",
  orders: [],
  events: [{ id: 1, event_type: "plan_created", status: "approval_pending", observed_at: "2026-04-13T10:06:00Z" }],
};

const routingPendingPlan = {
  ...approvalPendingPlan,
  status: "routing_pending",
  approved_by: "operator",
  events: [
    { id: 1, event_type: "plan_created", status: "approval_pending", observed_at: "2026-04-13T10:06:00Z" },
    { id: 2, event_type: "plan_approved", status: "routing_pending", observed_at: "2026-04-13T10:06:05Z" },
  ],
};

const routedPlan = {
  ...routingPendingPlan,
  status: "routed",
  orders: [
    { id: 1, leg_index: 0, venue: "manifold", side: "buy_yes", status: "filled", target_size: "1.00", avg_fill_price: "0.41", filled_size: "1.00" },
    { id: 2, leg_index: 1, venue: "kalshi", side: "buy_no", status: "filled", target_size: "1.00", avg_fill_price: "0.28", filled_size: "1.00" },
  ],
  events: [
    { id: 1, event_type: "plan_created", status: "approval_pending", observed_at: "2026-04-13T10:06:00Z" },
    { id: 2, event_type: "plan_approved", status: "routing_pending", observed_at: "2026-04-13T10:06:05Z" },
    { id: 3, event_type: "plan_routed", status: "routed", observed_at: "2026-04-13T10:06:10Z" },
  ],
};

const makerSnapshot = {
  id: "snapshot-1",
  preferred_action: "maker",
  maker_net_total: "0.1425",
  taker_net_total: "0.1025",
  maker_advantage_total: "0.0400",
  maker_fill_probability: "0.6200",
  maker_fees_total: "0.0080",
  maker_rewards_total: "0.0010",
  maker_realism_adjustment_total: "0.0120",
  maker_action_type: "step_ahead",
  status: "degraded",
  reason_codes_json: ["advisory_only_output", "downgraded_confidence"],
  evaluated_at: "2026-04-13T10:06:20Z",
};

const quoteRecommendation = {
  id: "quote-1",
  recommendation_action: "recommend_quote",
  comparison_winner: "maker",
  recommended_action_type: "step_ahead",
  recommended_side: "buy_yes",
  recommended_yes_price: "0.405",
  recommended_size: "1.00",
  recommended_notional: "0.4050",
  reason_codes_json: ["advisory_only_output", "downgraded_confidence"],
};

let currentDetail;

beforeEach(() => {
  vi.clearAllMocks();
  currentDetail = baseDetail;
  getPolymarketStructureStatus.mockResolvedValue(statusPayload);
  getPolymarketStructureOpportunities.mockResolvedValue(opportunitiesPayload);
  getPolymarketStructureOpportunity.mockImplementation(async () => currentDetail);
  getPolymarketStructureLatestMakerEconomics.mockResolvedValue(makerSnapshot);
  getPolymarketStructureLatestQuoteRecommendation.mockResolvedValue(quoteRecommendation);
  getPolymarketFeeHistory.mockResolvedValue({
    rows: [
      { id: 1, observed_at_local: "2026-04-13T10:01:00Z", taker_fee_rate: "0.0200", maker_fee_rate: "0.0000", token_base_fee_rate: "6.0000" },
    ],
  });
  getPolymarketRewardHistory.mockResolvedValue({
    rows: [
      { id: 1, observed_at_local: "2026-04-13T10:02:00Z", reward_status: "active", reward_daily_rate: "1.2500", min_incentive_size: "1.00" },
    ],
  });
  createPolymarketStructurePaperPlan.mockImplementation(async () => {
    currentDetail = { ...baseDetail, paper_plans: [approvalPendingPlan] };
    return approvalPendingPlan;
  });
  approvePolymarketStructurePaperPlan.mockImplementation(async () => {
    currentDetail = { ...baseDetail, paper_plans: [routingPendingPlan] };
    return routingPendingPlan;
  });
  routePolymarketStructurePaperPlan.mockImplementation(async () => {
    currentDetail = { ...baseDetail, paper_plans: [routedPlan] };
    return routedPlan;
  });
  runPolymarketStructureMakerEconomics.mockResolvedValue(makerSnapshot);
  runPolymarketStructureQuoteRecommendation.mockResolvedValue(quoteRecommendation);
  validatePolymarketStructureOpportunities.mockResolvedValue({ status: "completed" });
});

describe("Structures", () => {
  test("renders detail workflow, filters opportunities, and runs paper-plan controls", async () => {
    render(<Structures />);

    expect(await screen.findByText("Structure Opportunities")).toBeInTheDocument();
    expect(await screen.findByText("State race linkage")).toBeInTheDocument();
    expect(screen.getByText(/paired executable hedge routing exists/i)).toBeInTheDocument();
    expect(await screen.findByText("Cross-Venue Governance")).toBeInTheDocument();
    expect(screen.getByText("reviewed_sheet")).toBeInTheDocument();
    expect(await screen.findByText("Maker Economics")).toBeInTheDocument();
    expect(await screen.findByText("Quote Recommendation")).toBeInTheDocument();
    expect(screen.getByText("Fee History")).toBeInTheDocument();
    expect(screen.getByText("Reward History")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Validation"), {
      target: { value: "blocked" },
    });
    await waitFor(() => {
      expect(getPolymarketStructureOpportunities).toHaveBeenLastCalledWith(
        expect.objectContaining({ classification: "blocked" }),
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "Revalidate Selected" }));
    await waitFor(() => {
      expect(validatePolymarketStructureOpportunities).toHaveBeenCalledWith({
        reason: "manual",
        opportunity_id: 101,
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "Evaluate Economics" }));
    await waitFor(() => {
      expect(runPolymarketStructureMakerEconomics).toHaveBeenCalledWith(101, {});
    });

    fireEvent.click(screen.getByRole("button", { name: "Generate Quote Recommendation" }));
    await waitFor(() => {
      expect(runPolymarketStructureQuoteRecommendation).toHaveBeenCalledWith(101, {});
    });

    fireEvent.click(screen.getByRole("button", { name: "Create Paper Plan" }));
    await waitFor(() => {
      expect(createPolymarketStructurePaperPlan).toHaveBeenCalledWith(101, { actor: "operator" });
    });

    expect(await screen.findByRole("button", { name: "Approve Plan" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Approve Plan" }));
    await waitFor(() => {
      expect(approvePolymarketStructurePaperPlan).toHaveBeenCalledWith("plan-1", { actor: "operator" });
    });

    expect(await screen.findByRole("button", { name: "Route Plan" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Route Plan" }));
    await waitFor(() => {
      expect(routePolymarketStructurePaperPlan).toHaveBeenCalledWith("plan-1", { actor: "operator" });
    });

    expect((await screen.findAllByText("routed")).length).toBeGreaterThan(0);
  });
});
