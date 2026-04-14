import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  cancelPolymarketLiveOrder,
  getPolymarketLiveFills,
  getPolymarketLiveOrderEvents,
  getPolymarketLiveOrders,
  submitPolymarketLiveOrder,
} from "../api";
import LiveOrders from "./LiveOrders";

vi.mock("../api", () => ({
  cancelPolymarketLiveOrder: vi.fn(),
  getPolymarketLiveFills: vi.fn(),
  getPolymarketLiveOrderEvents: vi.fn(),
  getPolymarketLiveOrders: vi.fn(),
  submitPolymarketLiveOrder: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  getPolymarketLiveOrders.mockResolvedValue({
    rows: [
      {
        id: "order-1",
        client_order_id: "client-1",
        status: "approval_pending",
        approval_state: "queued",
        blocked_reason_code: null,
        validation_error: null,
        created_at: "2026-04-15T10:00:00Z",
      },
    ],
  });
  getPolymarketLiveOrderEvents.mockResolvedValue({
    rows: [
      {
        id: 1,
        observed_at_local: "2026-04-15T10:01:00Z",
        event_type: "approval_requested",
        venue_status: null,
        details_json: { reason: "manual_approval_required" },
        payload_json: {},
      },
    ],
  });
  getPolymarketLiveFills.mockResolvedValue({
    rows: [
      {
        id: "fill-1",
        observed_at_local: "2026-04-15T10:02:00Z",
        asset_id: "asset-1",
        fill_status: "matched",
        price: "0.52",
        size: "12",
      },
    ],
  });
  submitPolymarketLiveOrder.mockResolvedValue({ ok: true });
  cancelPolymarketLiveOrder.mockResolvedValue({ ok: true });
});

describe("LiveOrders", () => {
  test("renders filtered live order state and submit or cancel actions", async () => {
    render(<LiveOrders />);

    expect(await screen.findByText("Live Orders")).toBeInTheDocument();
    expect(getPolymarketLiveOrders).toHaveBeenCalledWith(
      expect.objectContaining({ strategyFamily: "exec_policy" }),
    );
    expect(screen.getByText("Recent Order Events")).toBeInTheDocument();
    expect(screen.getByText("Recent Fills")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Condition"), { target: { value: "cond-1" } });
    await waitFor(() => {
      expect(getPolymarketLiveOrders).toHaveBeenLastCalledWith(
        expect.objectContaining({ conditionId: "cond-1" }),
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    await waitFor(() => {
      expect(submitPolymarketLiveOrder).toHaveBeenCalledWith("order-1", { operator: "operator" });
    });

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => {
      expect(cancelPolymarketLiveOrder).toHaveBeenCalledWith("order-1", { operator: "operator" });
    });
  });
});
