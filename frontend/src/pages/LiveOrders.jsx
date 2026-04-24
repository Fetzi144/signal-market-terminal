import { useCallback, useEffect, useState } from "react";
import {
  cancelPolymarketLiveOrder,
  getPolymarketLiveFills,
  getPolymarketLiveOrderEvents,
  getPolymarketLiveOrders,
  submitPolymarketLiveOrder,
} from "../api";

export default function LiveOrders() {
  const [filters, setFilters] = useState({
    status: "",
    approvalState: "",
    strategyFamily: "exec_policy",
    conditionId: "",
  });
  const [orders, setOrders] = useState([]);
  const [events, setEvents] = useState([]);
  const [fills, setFills] = useState([]);
  const [error, setError] = useState(null);
  const [busyAction, setBusyAction] = useState(null);

  const load = useCallback(async () => {
    try {
      const params = {
        status: filters.status || undefined,
        approvalState: filters.approvalState || undefined,
        strategyFamily: filters.strategyFamily || undefined,
        conditionId: filters.conditionId || undefined,
      };
      const [orderRows, eventRows, fillRows] = await Promise.all([
        getPolymarketLiveOrders(params),
        getPolymarketLiveOrderEvents({ ...params, limit: 50 }),
        getPolymarketLiveFills({ ...params, limit: 50 }),
      ]);
      setOrders(orderRows.rows || []);
      setEvents(eventRows.rows || []);
      setFills(fillRows.rows || []);
      setError(null);
    } catch (requestError) {
      setError(requestError.message);
    }
  }, [filters]);

  useEffect(() => {
    load();
  }, [load]);

  const runAction = useCallback(async (key, fn) => {
    try {
      setBusyAction(key);
      setError(null);
      await fn();
      await load();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyAction(null);
    }
  }, [load]);

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div>
          <h2 style={titleStyle}>Live Orders</h2>
          <p style={subtitleStyle}>Operator view for live orders, events, fills, and approval state.</p>
        </div>
        <button onClick={load} style={secondaryButtonStyle}>Refresh</button>
      </div>

      {error && <div style={alertStyle}>{error}</div>}

      <section style={panelStyle}>
        <div style={filterGridStyle}>
          <label style={fieldStyle}>
            <span>Status</span>
            <select value={filters.status} onChange={(event) => setFilters((current) => ({ ...current, status: event.target.value }))} style={inputStyle}>
              <option value="">All</option>
              <option value="approval_pending">approval_pending</option>
              <option value="submission_pending">submission_pending</option>
              <option value="submit_blocked">submit_blocked</option>
              <option value="submitted">submitted</option>
              <option value="live">live</option>
              <option value="partially_filled">partially_filled</option>
              <option value="canceled">canceled</option>
            </select>
          </label>
          <label style={fieldStyle}>
            <span>Approval</span>
            <select value={filters.approvalState} onChange={(event) => setFilters((current) => ({ ...current, approvalState: event.target.value }))} style={inputStyle}>
              <option value="">All</option>
              <option value="queued">queued</option>
              <option value="approved">approved</option>
              <option value="rejected">rejected</option>
              <option value="expired">expired</option>
            </select>
          </label>
          <label style={fieldStyle}>
            <span>Family</span>
            <input value={filters.strategyFamily} onChange={(event) => setFilters((current) => ({ ...current, strategyFamily: event.target.value }))} style={inputStyle} />
          </label>
          <label style={fieldStyle}>
            <span>Condition</span>
            <input value={filters.conditionId} onChange={(event) => setFilters((current) => ({ ...current, conditionId: event.target.value }))} style={inputStyle} />
          </label>
        </div>
      </section>

      <section style={panelStyle}>
        <SectionTitle>Orders</SectionTitle>
        <SimpleTable
          columns={["Created", "Client ID", "Status", "Approval", "Reason", "Actions"]}
          rows={orders.map((order) => ([
            formatShortDateTime(order.created_at),
            order.client_order_id,
            order.status,
            order.approval_state,
            order.blocked_reason_code || order.validation_error || "-",
            <div key={order.id} style={actionRowStyle}>
              <button
                onClick={() => runAction(`submit-${order.id}`, () => submitPolymarketLiveOrder(order.id, { operator: "operator" }))}
                style={secondaryButtonStyle}
                disabled={busyAction === `submit-${order.id}` || !canSubmitOrder(order)}
              >
                {submitActionLabel(order)}
              </button>
              <button
                onClick={() => runAction(`cancel-${order.id}`, () => cancelPolymarketLiveOrder(order.id, { operator: "operator" }))}
                style={dangerButtonStyle}
                disabled={busyAction === `cancel-${order.id}`}
              >
                Cancel
              </button>
            </div>,
          ]))}
          emptyLabel="No live orders matched the current filters."
        />
      </section>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <SectionTitle>Recent Order Events</SectionTitle>
          <SimpleTable
            columns={["Observed", "Type", "Venue", "Summary"]}
            rows={events.map((event) => ([
              formatShortDateTime(event.observed_at_local),
              event.event_type,
              event.venue_status || "-",
              event.details_json?.reason || event.details_json?.operator || event.payload_json?.status || "-",
            ]))}
            emptyLabel="No order events yet."
          />
        </section>

        <section style={panelStyle}>
          <SectionTitle>Recent Fills</SectionTitle>
          <SimpleTable
            columns={["Observed", "Asset", "Status", "Price", "Size"]}
            rows={fills.map((fill) => ([
              formatShortDateTime(fill.observed_at_local),
              fill.asset_id,
              fill.fill_status,
              formatNumber(fill.price),
              formatNumber(fill.size),
            ]))}
            emptyLabel="No fills yet."
          />
        </section>
      </div>
    </div>
  );
}

function SectionTitle({ children }) {
  return <h3 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 12px" }}>{children}</h3>;
}

function SimpleTable({ columns, rows, emptyLabel }) {
  return (
    <div className="table-scroll">
      <table style={tableStyle}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} style={tableHeadStyle}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={emptyCellStyle}>{emptyLabel}</td>
            </tr>
          ) : rows.map((row, rowIndex) => (
            <tr key={`${columns[0]}-${rowIndex}`} style={tableRowStyle}>
              {row.map((cell, cellIndex) => (
                <td key={`${columns[0]}-${rowIndex}-${cellIndex}`} style={tableCellStyle}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatShortDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
}

function formatNumber(value) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(2);
}

function canSubmitOrder(order) {
  return order.status === "submission_pending" && order.approval_state === "approved";
}

function submitActionLabel(order) {
  if (canSubmitOrder(order)) return "Submit";
  if (order.approval_state === "queued" || order.status === "approval_pending") return "Approve First";
  if (order.status === "submit_blocked") return "Blocked";
  if (order.status === "submitted" || order.status === "live") return "Submitted";
  return "Unavailable";
}

const pageStyle = { display: "flex", flexDirection: "column", gap: 20 };
const headerStyle = { display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap" };
const titleStyle = { fontSize: 16, margin: 0 };
const subtitleStyle = { margin: "6px 0 0", fontSize: 12, color: "var(--text-dim)" };
const panelStyle = { background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 12, padding: 16 };
const filterGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 };
const fieldStyle = { display: "flex", flexDirection: "column", gap: 6, fontSize: 12, color: "var(--text-dim)" };
const inputStyle = { borderRadius: 8, border: "1px solid var(--border)", padding: "8px 10px", background: "var(--bg-card)", color: "var(--text)" };
const splitGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 };
const actionRowStyle = { display: "flex", gap: 8, flexWrap: "wrap" };
const secondaryButtonStyle = { background: "transparent", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const dangerButtonStyle = { background: "transparent", color: "var(--red)", border: "1px solid var(--red)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const alertStyle = { border: "1px solid var(--red)", color: "var(--red)", borderRadius: 10, padding: "10px 12px", fontSize: 12, background: "rgba(255, 255, 255, 0.03)" };
const tableStyle = { width: "100%", borderCollapse: "collapse" };
const tableHeadStyle = { padding: "0 0 10px", textAlign: "left", fontSize: 11, color: "var(--text-dim)", fontWeight: 500, whiteSpace: "nowrap" };
const tableRowStyle = { borderTop: "1px solid var(--border)" };
const tableCellStyle = { padding: "10px 10px 10px 0", fontSize: 12, verticalAlign: "top" };
const emptyCellStyle = { padding: "16px 0 4px", fontSize: 12, color: "var(--text-dim)" };
