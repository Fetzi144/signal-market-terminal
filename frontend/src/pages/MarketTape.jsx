import { useCallback, useEffect, useState } from "react";
import { getPolymarketMarketTape, getPolymarketPilotStatus } from "../api";

export default function MarketTape() {
  const [filters, setFilters] = useState({
    conditionId: "",
    assetId: "",
  });
  const [pilotStatus, setPilotStatus] = useState(null);
  const [tape, setTape] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    try {
      const params = {
        conditionId: filters.conditionId || undefined,
        assetId: filters.assetId || undefined,
        limit: 25,
      };
      const [statusResult, tapeResult] = await Promise.all([
        getPolymarketPilotStatus(),
        getPolymarketMarketTape(params),
      ]);
      setPilotStatus(statusResult);
      setTape(tapeResult);
      setError(null);
    } catch (requestError) {
      setError(requestError.message);
    }
  }, [filters.assetId, filters.conditionId]);

  useEffect(() => {
    load();
  }, [load]);

  const reconState = tape?.recon_state || null;
  const bboRows = tape?.bbo || [];
  const tradeRows = tape?.trades || [];
  const structureRows = tape?.structure_context || [];
  const quoteRows = tape?.quote_context || [];
  const liveOrders = tape?.live_orders || [];
  const liveEvents = tape?.live_order_events || [];

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div>
          <h2 style={titleStyle}>Market Tape</h2>
          <p style={subtitleStyle}>
            Pilot-focused tape, BBO, structure context, and live activity for one supervised market at a time.
          </p>
        </div>
        <button onClick={load} style={secondaryButtonStyle}>Refresh</button>
      </div>

      {error && <div style={alertStyle}>{error}</div>}

      <section style={panelStyle}>
        <div style={filterGridStyle}>
          <label style={fieldStyle}>
            <span>Condition</span>
            <input
              value={filters.conditionId}
              onChange={(event) => setFilters((current) => ({ ...current, conditionId: event.target.value }))}
              style={inputStyle}
              placeholder="cond-..."
            />
          </label>
          <label style={fieldStyle}>
            <span>Asset</span>
            <input
              value={filters.assetId}
              onChange={(event) => setFilters((current) => ({ ...current, assetId: event.target.value }))}
              style={inputStyle}
              placeholder="asset-..."
            />
          </label>
        </div>
      </section>

      <section style={panelStyle}>
        <div style={statsGridStyle}>
          <StatCard label="Pilot Armed" value={pilotStatus?.active_pilot?.armed ? "Yes" : "No"} />
          <StatCard label="Active Family" value={pilotStatus?.active_pilot?.strategy_family || "none"} />
          <StatCard label="Heartbeat" value={pilotStatus?.heartbeat_status || "idle"} />
          <StatCard label="Selected Condition" value={tape?.selected_condition_id || "-"} />
          <StatCard label="Selected Asset" value={tape?.selected_asset_id || "-"} />
          <StatCard label="Book Status" value={reconState?.status || "unknown"} />
          <StatCard label="Best Bid" value={formatNumber(reconState?.best_bid)} />
          <StatCard label="Best Ask" value={formatNumber(reconState?.best_ask)} />
          <StatCard label="Spread" value={formatNumber(reconState?.spread)} />
        </div>
      </section>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <SectionTitle>Recent BBO</SectionTitle>
          <SimpleTable
            columns={["Time", "Bid", "Ask", "Spread"]}
            rows={bboRows.map((row) => ([
              formatShortDateTime(row.event_ts_exchange),
              formatNumber(row.best_bid),
              formatNumber(row.best_ask),
              formatNumber(row.spread),
            ]))}
            emptyLabel="No BBO rows for the selected market."
          />
        </section>

        <section style={panelStyle}>
          <SectionTitle>Recent Trades</SectionTitle>
          <SimpleTable
            columns={["Time", "Side", "Price", "Size", "Outcome"]}
            rows={tradeRows.map((row) => ([
              formatShortDateTime(row.event_ts_exchange),
              row.side || "-",
              formatNumber(row.price),
              formatNumber(row.size),
              row.outcome_name || "-",
            ]))}
            emptyLabel="No trade tape rows for the selected market."
          />
        </section>
      </div>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <SectionTitle>Structure Context</SectionTitle>
          <SimpleTable
            columns={["Created", "Type", "Class", "Reason"]}
            rows={structureRows.map((row) => ([
              formatShortDateTime(row.created_at),
              row.opportunity_type,
              row.classification,
              row.reason_code || "-",
            ]))}
            emptyLabel="No recent structure context."
          />
        </section>

        <section style={panelStyle}>
          <SectionTitle>Quote Context</SectionTitle>
          <SimpleTable
            columns={["Created", "Status", "Action", "Asset"]}
            rows={quoteRows.map((row) => ([
              formatShortDateTime(row.created_at),
              row.status,
              row.recommendation_action || "-",
              row.asset_id || "-",
            ]))}
            emptyLabel="No recent quote context."
          />
        </section>
      </div>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <SectionTitle>Live Orders Overlay</SectionTitle>
          <SimpleTable
            columns={["Created", "Client ID", "Status", "Approval", "Reason"]}
            rows={liveOrders.map((row) => ([
              formatShortDateTime(row.created_at),
              row.client_order_id,
              row.status,
              row.approval_state || "-",
              row.blocked_reason_code || row.validation_error || "-",
            ]))}
            emptyLabel="No recent live orders on the selected tape."
          />
        </section>

        <section style={panelStyle}>
          <SectionTitle>Recent Live Events</SectionTitle>
          <SimpleTable
            columns={["Observed", "Type", "Venue", "Summary"]}
            rows={liveEvents.map((row) => ([
              formatShortDateTime(row.observed_at_local),
              row.event_type,
              row.venue_status || "-",
              row.details_json?.reason || row.payload_json?.status || row.source_kind,
            ]))}
            emptyLabel="No recent live events on the selected tape."
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

function StatCard({ label, value }) {
  return (
    <div style={statCardStyle}>
      <div style={statLabelStyle}>{label}</div>
      <div style={statValueStyle}>{value}</div>
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
  return Number(value).toFixed(3);
}

const pageStyle = { display: "flex", flexDirection: "column", gap: 20 };
const headerStyle = { display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap" };
const titleStyle = { fontSize: 16, margin: 0 };
const subtitleStyle = { margin: "6px 0 0", fontSize: 12, color: "var(--text-dim)" };
const panelStyle = { background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 12, padding: 16 };
const filterGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 };
const fieldStyle = { display: "flex", flexDirection: "column", gap: 6, fontSize: 12, color: "var(--text-dim)" };
const inputStyle = { borderRadius: 8, border: "1px solid var(--border)", padding: "8px 10px", background: "var(--bg-card)", color: "var(--text)" };
const statsGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12 };
const splitGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 };
const statCardStyle = { background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 };
const statLabelStyle = { fontSize: 11, color: "var(--text-dim)", marginBottom: 4 };
const statValueStyle = { fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600, overflowWrap: "anywhere" };
const secondaryButtonStyle = { background: "transparent", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const alertStyle = { border: "1px solid var(--red)", color: "var(--red)", borderRadius: 10, padding: "10px 12px", fontSize: 12, background: "rgba(255, 255, 255, 0.03)" };
const tableStyle = { width: "100%", borderCollapse: "collapse" };
const tableHeadStyle = { padding: "0 0 10px", textAlign: "left", fontSize: 11, color: "var(--text-dim)", fontWeight: 500, whiteSpace: "nowrap" };
const tableRowStyle = { borderTop: "1px solid var(--border)" };
const tableCellStyle = { padding: "10px 10px 10px 0", fontSize: 12, verticalAlign: "top" };
const emptyCellStyle = { padding: "16px 0 4px", fontSize: 12, color: "var(--text-dim)" };
