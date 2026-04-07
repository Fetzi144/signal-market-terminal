import { useEffect, useState, useCallback } from "react";
import { Link, useSearchParams } from "react-router-dom";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  getPortfolioSummary, getPositions, createPosition, closePosition,
  getMarkets, getMarket, exportPortfolioCsv,
} from "../api";

const PAGE_SIZE = 20;

function fmtPrice(v) {
  if (v == null) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function fmtPnl(v) {
  if (v == null) return "—";
  const n = Number(v);
  const sign = n >= 0 ? "+" : "";
  return `${sign}$${n.toFixed(2)}`;
}

function pnlColor(v) {
  if (v == null) return "var(--text-dim)";
  return Number(v) >= 0 ? "var(--green)" : "var(--red)";
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "2-digit" });
}

function fmtDuration(start, end) {
  if (!start || !end) return "—";
  const ms = new Date(end) - new Date(start);
  const hours = Math.floor(ms / 3600000);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  return `${days}d`;
}

// ── Summary Cards ──────────────────────────────────────────────────────────
function SummaryCards({ summary }) {
  if (!summary) return <div className="skeleton" style={{ height: 96, borderRadius: 8, marginBottom: 24 }} />;

  const cards = [
    { label: "Unrealized P&L", value: fmtPnl(summary.total_unrealized_pnl), color: pnlColor(summary.total_unrealized_pnl) },
    { label: "Realized P&L", value: fmtPnl(summary.total_realized_pnl), color: pnlColor(summary.total_realized_pnl) },
    { label: "Open Positions", value: summary.open_positions, color: "var(--accent)" },
    { label: "Win Rate", value: summary.closed_positions > 0 ? `${summary.win_rate.toFixed(1)}%` : "—", color: summary.win_rate >= 50 ? "var(--green)" : summary.win_rate > 0 ? "var(--red)" : "var(--text-dim)" },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 24 }}>
      {cards.map((c) => (
        <div key={c.label} style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "20px 24px" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>{c.label}</div>
          <div style={{ fontSize: 28, fontFamily: "var(--mono)", fontWeight: 700, color: c.color }}>{c.value}</div>
        </div>
      ))}
    </div>
  );
}

// ── Cumulative P&L Chart ───────────────────────────────────────────────────
function PnlChart({ positions }) {
  if (!positions || positions.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No closed positions for P&L chart yet.</div>;
  }

  // Build cumulative P&L from closed/resolved positions sorted by updated_at
  const closed = positions
    .filter((p) => p.status !== "open" && p.realized_pnl != null)
    .sort((a, b) => new Date(a.updated_at) - new Date(b.updated_at));

  if (closed.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No closed positions for P&L chart yet.</div>;
  }

  let cumulative = 0;
  const data = closed.map((p) => {
    cumulative += p.realized_pnl;
    return { date: p.updated_at, pnl: Number(cumulative.toFixed(2)) };
  });

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "16px 8px 8px", marginBottom: 24 }}>
      <div style={{ fontSize: 12, fontWeight: 600, paddingLeft: 8, marginBottom: 8 }}>Cumulative Realized P&L</div>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: "var(--text-dim)" }} tickFormatter={(v) => fmtDate(v)} />
          <YAxis tick={{ fontSize: 11, fill: "var(--text-dim)" }} width={50} tickFormatter={(v) => `$${v}`} />
          <Tooltip
            formatter={(v) => [`$${v.toFixed(2)}`, "Cumulative P&L"]}
            labelFormatter={(l) => fmtDate(l)}
            contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", fontSize: 12 }}
          />
          <ReferenceLine y={0} stroke="var(--text-dim)" strokeDasharray="4 4" />
          <Line
            type="monotone"
            dataKey="pnl"
            stroke={data[data.length - 1].pnl >= 0 ? "var(--green)" : "var(--red)"}
            strokeWidth={2}
            dot={{ r: 3 }}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Close Position Dialog ──────────────────────────────────────────────────
function CloseDialog({ position, onClose, onConfirm }) {
  const [quantity, setQuantity] = useState(position.quantity);
  const [price, setPrice] = useState(position.current_price || "");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      await onConfirm(position.id, { quantity: Number(quantity), price: Number(price), fees: 0 });
      onClose();
    } catch (err) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="dialog-overlay">
      <form onSubmit={handleSubmit} className="dialog-content">
        <h3 style={{ fontSize: 15, fontWeight: 600, marginBottom: 12 }}>Close Position</h3>
        <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 16 }}>
          {position.market_question || "Position"} — {position.side.toUpperCase()} x{position.quantity}
        </div>
        <label style={{ display: "block", marginBottom: 12 }}>
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Quantity</span>
          <input type="number" step="any" min="0.01" max={position.quantity} value={quantity} onChange={(e) => setQuantity(e.target.value)}
            style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontFamily: "var(--mono)", fontSize: 13, boxSizing: "border-box" }} />
        </label>
        <label style={{ display: "block", marginBottom: 16 }}>
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Exit Price ($0–$1)</span>
          <input type="number" step="0.01" min="0" max="1" value={price} onChange={(e) => setPrice(e.target.value)}
            style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontFamily: "var(--mono)", fontSize: 13, boxSizing: "border-box" }} />
        </label>
        {error && <div style={{ color: "var(--red)", fontSize: 12, marginBottom: 8 }}>{error}</div>}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button type="button" onClick={onClose} style={{ padding: "6px 14px", background: "transparent", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text-dim)", cursor: "pointer", fontSize: 13 }}>Cancel</button>
          <button type="submit" disabled={submitting} style={{ padding: "6px 14px", background: "var(--accent)", border: "none", borderRadius: 6, color: "#fff", cursor: "pointer", fontSize: 13, fontWeight: 600, opacity: submitting ? 0.6 : 1 }}>{submitting ? "Closing..." : "Close Position"}</button>
        </div>
      </form>
    </div>
  );
}

// ── Add Position Form ──────────────────────────────────────────────────────
function AddPositionForm({ onCreated, prefill }) {
  const [open, setOpen] = useState(!!prefill);
  const [marketSearch, setMarketSearch] = useState("");
  const [marketResults, setMarketResults] = useState([]);
  const [selectedMarket, setSelectedMarket] = useState(null);
  const [selectedOutcome, setSelectedOutcome] = useState(null);
  const [side, setSide] = useState(prefill?.side || "yes");
  const [quantity, setQuantity] = useState("");
  const [price, setPrice] = useState("");
  const [signalId, setSignalId] = useState(prefill?.signal_id || "");
  const [notes, setNotes] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  // Load prefill market
  useEffect(() => {
    if (prefill?.market_id) {
      getMarket(prefill.market_id).then((m) => {
        setSelectedMarket(m);
        if (prefill.outcome_id && m.outcomes) {
          const oc = m.outcomes.find((o) => o.id === prefill.outcome_id);
          if (oc) setSelectedOutcome(oc);
        }
      });
    }
  }, [prefill]);

  const searchMarkets = useCallback(async (q) => {
    if (q.length < 2) { setMarketResults([]); return; }
    try {
      const data = await getMarkets({ search: q, pageSize: 8 });
      setMarketResults(data.markets || []);
    } catch { setMarketResults([]); }
  }, []);

  useEffect(() => {
    const t = setTimeout(() => searchMarkets(marketSearch), 300);
    return () => clearTimeout(t);
  }, [marketSearch, searchMarkets]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!selectedMarket || !selectedOutcome) return;
    setSubmitting(true);
    setError(null);
    try {
      await createPosition({
        market_id: selectedMarket.id,
        outcome_id: selectedOutcome.id,
        platform: selectedMarket.platform,
        side,
        quantity: Number(quantity),
        price: Number(price),
        signal_id: signalId || null,
        notes: notes || null,
      });
      setSelectedMarket(null);
      setSelectedOutcome(null);
      setQuantity("");
      setPrice("");
      setSignalId("");
      setNotes("");
      setOpen(false);
      onCreated();
    } catch (err) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  if (!open) {
    return (
      <button onClick={() => setOpen(true)} style={{ padding: "8px 16px", background: "var(--accent)", border: "none", borderRadius: 6, color: "#fff", cursor: "pointer", fontSize: 13, fontWeight: 600, marginBottom: 20 }}>
        + New Position
      </button>
    );
  }

  return (
    <form onSubmit={handleSubmit} style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 20, marginBottom: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h3 style={{ fontSize: 14, fontWeight: 600 }}>Open New Position</h3>
        <button type="button" onClick={() => setOpen(false)} style={{ background: "transparent", border: "none", color: "var(--text-dim)", cursor: "pointer", fontSize: 16 }}>x</button>
      </div>

      {/* Market search */}
      {!selectedMarket ? (
        <div style={{ marginBottom: 12, position: "relative" }}>
          <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Search Market</label>
          <input value={marketSearch} onChange={(e) => setMarketSearch(e.target.value)} placeholder="Type to search markets..."
            style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontSize: 13, boxSizing: "border-box" }} />
          {marketResults.length > 0 && (
            <div style={{ position: "absolute", top: "100%", left: 0, right: 0, background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 6, zIndex: 10, maxHeight: 200, overflow: "auto" }}>
              {marketResults.map((m) => (
                <div key={m.id} onClick={() => { setSelectedMarket(m); setMarketResults([]); setMarketSearch(""); }}
                  style={{ padding: "8px 12px", cursor: "pointer", fontSize: 13, borderBottom: "1px solid var(--border)" }}
                  onMouseEnter={(e) => e.currentTarget.style.background = "var(--bg-hover)"}
                  onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}>
                  <div style={{ fontWeight: 600 }}>{m.question}</div>
                  <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{m.platform}</div>
                </div>
              ))}
            </div>
          )}
        </div>
      ) : (
        <div style={{ marginBottom: 12, padding: "8px 12px", background: "var(--bg)", borderRadius: 6, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 600 }}>{selectedMarket.question}</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{selectedMarket.platform}</div>
          </div>
          <button type="button" onClick={() => { setSelectedMarket(null); setSelectedOutcome(null); }}
            style={{ background: "transparent", border: "none", color: "var(--text-dim)", cursor: "pointer", fontSize: 13 }}>change</button>
        </div>
      )}

      {/* Outcome selector */}
      {selectedMarket && selectedMarket.outcomes && (
        <div style={{ marginBottom: 12 }}>
          <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Outcome</label>
          <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
            {selectedMarket.outcomes.map((oc) => (
              <button key={oc.id} type="button" onClick={() => setSelectedOutcome(oc)}
                style={{
                  padding: "6px 14px", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600,
                  background: selectedOutcome?.id === oc.id ? "var(--accent)" : "var(--bg)",
                  color: selectedOutcome?.id === oc.id ? "#fff" : "var(--text)",
                  border: `1px solid ${selectedOutcome?.id === oc.id ? "var(--accent)" : "var(--border)"}`,
                }}>
                {oc.name}
              </button>
            ))}
          </div>
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 12 }}>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Side</label>
          <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
            {["yes", "no"].map((s) => (
              <button key={s} type="button" onClick={() => setSide(s)}
                style={{
                  flex: 1, padding: "6px 0", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600,
                  background: side === s ? (s === "yes" ? "var(--green)" : "var(--red)") : "var(--bg)",
                  color: side === s ? "#fff" : "var(--text)",
                  border: `1px solid ${side === s ? "transparent" : "var(--border)"}`,
                }}>
                {s.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Quantity</label>
          <input type="number" step="any" min="0.01" value={quantity} onChange={(e) => setQuantity(e.target.value)} required
            style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontFamily: "var(--mono)", fontSize: 13, boxSizing: "border-box" }} />
        </div>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Price ($0–$1)</label>
          <input type="number" step="0.01" min="0" max="1" value={price} onChange={(e) => setPrice(e.target.value)} required
            style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontFamily: "var(--mono)", fontSize: 13, boxSizing: "border-box" }} />
        </div>
      </div>

      <div style={{ marginBottom: 16 }}>
        <label style={{ fontSize: 12, color: "var(--text-dim)" }}>Notes (optional)</label>
        <input value={notes} onChange={(e) => setNotes(e.target.value)} placeholder="Trade rationale..."
          style={{ display: "block", width: "100%", marginTop: 4, padding: "6px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontSize: 13, boxSizing: "border-box" }} />
      </div>

      {error && <div style={{ color: "var(--red)", fontSize: 12, marginBottom: 8 }}>{error}</div>}

      <div style={{ display: "flex", gap: 8 }}>
        <button type="submit" disabled={submitting || !selectedMarket || !selectedOutcome || !quantity || !price}
          style={{ padding: "8px 16px", background: "var(--accent)", border: "none", borderRadius: 6, color: "#fff", cursor: "pointer", fontSize: 13, fontWeight: 600, opacity: (submitting || !selectedMarket || !selectedOutcome) ? 0.5 : 1 }}>
          {submitting ? "Opening..." : "Open Position"}
        </button>
        <button type="button" onClick={() => setOpen(false)}
          style={{ padding: "8px 16px", background: "transparent", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text-dim)", cursor: "pointer", fontSize: 13 }}>
          Cancel
        </button>
      </div>
    </form>
  );
}

// ── Positions Table ────────────────────────────────────────────────────────
function PositionsTable({ positions, sortKey, sortDir, onSort, showClose, onCloseClick }) {
  if (!positions || positions.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No positions.</div>;
  }

  const thStyle = (key) => ({
    textAlign: key === "market_question" ? "left" : "right",
    padding: "8px 14px",
    color: "var(--text-dim)",
    fontWeight: 500,
    cursor: "pointer",
    userSelect: "none",
    whiteSpace: "nowrap",
  });

  const sortArrow = (key) => sortKey === key ? (sortDir === "asc" ? " ^" : " v") : "";

  const isOpen = showClose;
  const columns = isOpen
    ? [
        { key: "market_question", label: "Market" },
        { key: "side", label: "Side" },
        { key: "quantity", label: "Qty" },
        { key: "avg_entry_price", label: "Avg Entry" },
        { key: "current_price", label: "Current" },
        { key: "unrealized_pnl", label: "Unrealized P&L" },
        { key: "platform", label: "Source" },
      ]
    : [
        { key: "market_question", label: "Market" },
        { key: "side", label: "Side" },
        { key: "quantity", label: "Qty" },
        { key: "avg_entry_price", label: "Entry" },
        { key: "exit_price", label: "Exit" },
        { key: "realized_pnl", label: "Realized P&L" },
        { key: "updated_at", label: "Duration" },
      ];

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, minWidth: 600 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            {columns.map((col) => (
              <th key={col.key} onClick={() => onSort(col.key)} style={thStyle(col.key)}>
                {col.label}{sortArrow(col.key)}
              </th>
            ))}
            {isOpen && <th style={{ padding: "8px 14px" }}></th>}
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => (
            <tr key={p.id} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 14px", maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                <Link to={`/markets/${p.market_id}`} style={{ color: "var(--accent)", fontWeight: 500, fontSize: 12 }}>
                  {p.market_question || p.market_id.slice(0, 8)}
                </Link>
              </td>
              <td style={{ textAlign: "right", padding: "10px 14px" }}>
                <span style={{ fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 4, background: p.side === "yes" ? "var(--green)" : "var(--red)", color: "#fff" }}>
                  {p.side.toUpperCase()}
                </span>
              </td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{p.quantity}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtPrice(p.avg_entry_price)}</td>
              {isOpen ? (
                <>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtPrice(p.current_price)}</td>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", fontWeight: 700, color: pnlColor(p.unrealized_pnl) }}>{fmtPnl(p.unrealized_pnl)}</td>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase" }}>{p.platform}</td>
                  <td style={{ padding: "10px 14px", textAlign: "right" }}>
                    <button onClick={() => onCloseClick(p)}
                      style={{ padding: "4px 10px", background: "transparent", border: "1px solid var(--red)", borderRadius: 4, color: "var(--red)", cursor: "pointer", fontSize: 11, fontWeight: 600 }}>
                      Close
                    </button>
                  </td>
                </>
              ) : (
                <>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtPrice(p.exit_price)}</td>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", fontWeight: 700, color: pnlColor(p.realized_pnl) }}>{fmtPnl(p.realized_pnl)}</td>
                  <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", fontSize: 12, color: "var(--text-dim)" }}>{fmtDuration(p.created_at, p.updated_at)}</td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Pagination ─────────────────────────────────────────────────────────────
function Pagination({ page, totalPages, onPage }) {
  if (totalPages <= 1) return null;
  return (
    <div style={{ display: "flex", justifyContent: "center", gap: 8, marginTop: 12 }}>
      <button onClick={() => onPage(page - 1)} disabled={page <= 1}
        style={{ padding: "4px 12px", background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", cursor: page <= 1 ? "default" : "pointer", opacity: page <= 1 ? 0.4 : 1, fontSize: 13 }}>
        Prev
      </button>
      <span style={{ padding: "4px 8px", fontSize: 13, color: "var(--text-dim)" }}>{page} / {totalPages}</span>
      <button onClick={() => onPage(page + 1)} disabled={page >= totalPages}
        style={{ padding: "4px 12px", background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", cursor: page >= totalPages ? "default" : "pointer", opacity: page >= totalPages ? 0.4 : 1, fontSize: 13 }}>
        Next
      </button>
    </div>
  );
}

// ── Main Portfolio Page ────────────────────────────────────────────────────
export default function Portfolio() {
  const [searchParams] = useSearchParams();
  const prefillSignalId = searchParams.get("signal_id");
  const prefillMarketId = searchParams.get("market_id");
  const prefillOutcomeId = searchParams.get("outcome_id");
  const prefillSide = searchParams.get("side");

  const [summary, setSummary] = useState(null);
  const [openPositions, setOpenPositions] = useState([]);
  const [closedPositions, setClosedPositions] = useState([]);
  const [openTotal, setOpenTotal] = useState(0);
  const [closedTotal, setClosedTotal] = useState(0);
  const [openPage, setOpenPage] = useState(1);
  const [closedPage, setClosedPage] = useState(1);
  const [error, setError] = useState(null);
  const [closingPosition, setClosingPosition] = useState(null);
  const [tab, setTab] = useState("open");

  // Sort state
  const [openSort, setOpenSort] = useState({ key: "unrealized_pnl", dir: "desc" });
  const [closedSort, setClosedSort] = useState({ key: "realized_pnl", dir: "desc" });

  const prefill = prefillMarketId ? { market_id: prefillMarketId, outcome_id: prefillOutcomeId, side: prefillSide || "yes", signal_id: prefillSignalId } : null;

  const loadSummary = useCallback(() => {
    getPortfolioSummary().then(setSummary).catch((e) => setError(e.message));
  }, []);

  const loadOpen = useCallback(() => {
    getPositions({ page: openPage, pageSize: PAGE_SIZE, status: "open" })
      .then((d) => { setOpenPositions(d.positions); setOpenTotal(d.total); })
      .catch((e) => setError(e.message));
  }, [openPage]);

  const loadClosed = useCallback(() => {
    getPositions({ page: closedPage, pageSize: PAGE_SIZE, status: "closed" })
      .then((d) => { setClosedPositions(d.positions); setClosedTotal(d.total); })
      .catch((e) => setError(e.message));
  }, [closedPage]);

  // Also load all closed for P&L chart (up to 500)
  const [allClosed, setAllClosed] = useState([]);
  const loadAllClosed = useCallback(() => {
    getPositions({ page: 1, pageSize: 500, status: "closed" })
      .then((d) => setAllClosed(d.positions))
      .catch(() => {});
    // Also fetch resolved positions
    getPositions({ page: 1, pageSize: 500, status: "resolved" })
      .then((d) => setAllClosed((prev) => [...prev, ...d.positions]))
      .catch(() => {});
  }, []);

  useEffect(() => { loadSummary(); loadAllClosed(); }, [loadSummary, loadAllClosed]);
  useEffect(() => { loadOpen(); }, [loadOpen]);
  useEffect(() => { loadClosed(); }, [loadClosed]);

  const reload = () => { loadSummary(); loadOpen(); loadClosed(); loadAllClosed(); };

  const handleClose = async (positionId, body) => {
    await closePosition(positionId, body);
    reload();
  };

  // Client-side sorting
  const sortPositions = (positions, sortKey, sortDir) => {
    return [...positions].sort((a, b) => {
      let av = a[sortKey], bv = b[sortKey];
      if (av == null) av = sortDir === "asc" ? Infinity : -Infinity;
      if (bv == null) bv = sortDir === "asc" ? Infinity : -Infinity;
      if (typeof av === "string") return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      return sortDir === "asc" ? av - bv : bv - av;
    });
  };

  const handleOpenSort = (key) => {
    setOpenSort((prev) => ({ key, dir: prev.key === key && prev.dir === "desc" ? "asc" : "desc" }));
  };
  const handleClosedSort = (key) => {
    setClosedSort((prev) => ({ key, dir: prev.key === key && prev.dir === "desc" ? "asc" : "desc" }));
  };

  const sortedOpen = sortPositions(openPositions, openSort.key, openSort.dir);
  const sortedClosed = sortPositions(closedPositions, closedSort.key, closedSort.dir);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 20 }}>
        <h2 style={{ fontSize: 16, fontWeight: 600 }}>Portfolio Tracker</h2>
        <button onClick={exportPortfolioCsv} style={{ padding: "4px 12px", background: "transparent", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text-dim)", cursor: "pointer", fontSize: 12 }}>
          Export CSV
        </button>
      </div>

      <SummaryCards summary={summary} />

      <PnlChart positions={allClosed} />

      <AddPositionForm onCreated={reload} prefill={prefill} />

      {/* Tab switcher */}
      <div style={{ display: "flex", gap: 0, marginBottom: 16, borderBottom: "1px solid var(--border)" }}>
        {[
          { key: "open", label: `Open (${openTotal})` },
          { key: "closed", label: `Closed (${closedTotal})` },
        ].map((t) => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{
              padding: "8px 20px", background: "transparent", border: "none", cursor: "pointer",
              fontSize: 13, fontWeight: 600, color: tab === t.key ? "var(--accent)" : "var(--text-dim)",
              borderBottom: tab === t.key ? "2px solid var(--accent)" : "2px solid transparent",
            }}>
            {t.label}
          </button>
        ))}
      </div>

      {tab === "open" && (
        <>
          <PositionsTable positions={sortedOpen} sortKey={openSort.key} sortDir={openSort.dir} onSort={handleOpenSort} showClose onCloseClick={setClosingPosition} />
          <Pagination page={openPage} totalPages={Math.ceil(openTotal / PAGE_SIZE)} onPage={setOpenPage} />
        </>
      )}

      {tab === "closed" && (
        <>
          <PositionsTable positions={sortedClosed} sortKey={closedSort.key} sortDir={closedSort.dir} onSort={handleClosedSort} showClose={false} />
          <Pagination page={closedPage} totalPages={Math.ceil(closedTotal / PAGE_SIZE)} onPage={setClosedPage} />
        </>
      )}

      {closingPosition && (
        <CloseDialog position={closingPosition} onClose={() => setClosingPosition(null)} onConfirm={handleClose} />
      )}
    </div>
  );
}
