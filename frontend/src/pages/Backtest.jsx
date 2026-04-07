import { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { createBacktest, createSweep, getBacktests, deleteBacktest } from "../api";

const STATUS_COLORS = {
  pending: "var(--yellow)",
  running: "var(--accent)",
  completed: "var(--green)",
  failed: "var(--red)",
};

const SIGNAL_TYPES = [
  "price_move",
  "volume_spike",
  "liquidity_drop",
  "bid_ask_spread",
];

const DETECTOR_DEFAULTS = {
  price_move: { threshold_pct: 0.06 },
  volume_spike: { multiplier: 4.0 },
  liquidity_drop: { threshold_pct: 0.3 },
  bid_ask_spread: { threshold_pct: 0.1 },
};

function StatusBadge({ status }) {
  return (
    <span style={{
      fontSize: 11, fontWeight: 700, textTransform: "uppercase",
      color: STATUS_COLORS[status] || "var(--text-dim)",
      border: `1px solid ${STATUS_COLORS[status] || "var(--border)"}`,
      padding: "2px 7px", borderRadius: 4,
    }}>
      {status === "running" ? "⟳ running" : status}
    </span>
  );
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

function fmtPct(v) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function SliderRow({ label, name, value, min, max, step, onChange }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
      <label style={{ fontSize: 12, color: "var(--text-dim)", width: 170 }}>{label}</label>
      <input
        type="range" min={min} max={max} step={step} value={value}
        onChange={(e) => onChange(name, parseFloat(e.target.value))}
        style={{ flex: 1 }}
      />
      <span style={{ fontSize: 12, fontFamily: "var(--mono)", width: 48, textAlign: "right" }}>
        {value}
      </span>
    </div>
  );
}

function CreateForm({ onCreated }) {
  const today = new Date().toISOString().slice(0, 10);
  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);

  const [name, setName] = useState("");
  const [startDate, setStartDate] = useState(thirtyDaysAgo);
  const [endDate, setEndDate] = useState(today);
  const [rankThreshold, setRankThreshold] = useState(0.6);
  const [detectorParams, setDetectorParams] = useState({
    price_move_threshold_pct: 0.06,
    volume_spike_multiplier: 4.0,
    liquidity_drop_threshold_pct: 0.3,
    bid_ask_spread_threshold_pct: 0.1,
  });
  const [sweepMode, setSweepMode] = useState(false);
  const [sweepRankMin, setSweepRankMin] = useState(0.5);
  const [sweepRankMax, setSweepRankMax] = useState(0.8);
  const [sweepRankStep, setSweepRankStep] = useState(0.1);
  const [sweepPriceMoveMin, setSweepPriceMoveMin] = useState(0.03);
  const [sweepPriceMoveMax, setSweepPriceMoveMax] = useState(0.10);
  const [sweepPriceMoveStep, setSweepPriceMoveStep] = useState(0.02);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  function setParam(key, val) {
    setDetectorParams((p) => ({ ...p, [key]: val }));
  }

  function buildDetectorConfigs() {
    return {
      price_move: { threshold_pct: detectorParams.price_move_threshold_pct },
      volume_spike: { multiplier: detectorParams.volume_spike_multiplier },
      liquidity_drop: { threshold_pct: detectorParams.liquidity_drop_threshold_pct },
      bid_ask_spread: { threshold_pct: detectorParams.bid_ask_spread_threshold_pct },
    };
  }

  function range(min, max, step) {
    const vals = [];
    for (let v = min; v <= max + 1e-9; v += step) vals.push(parseFloat(v.toFixed(4)));
    return vals;
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      if (sweepMode) {
        await createSweep({
          name_prefix: name || "sweep",
          start_date: startDate,
          end_date: endDate,
          sweep_params: {
            "price_move.threshold_pct": range(sweepPriceMoveMin, sweepPriceMoveMax, sweepPriceMoveStep),
            rank_threshold: range(sweepRankMin, sweepRankMax, sweepRankStep),
          },
        });
      } else {
        await createBacktest({
          name: name || `Backtest ${new Date().toLocaleDateString()}`,
          start_date: startDate,
          end_date: endDate,
          detector_configs: buildDetectorConfigs(),
          rank_threshold: rankThreshold,
        });
      }
      onCreated();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  const inputStyle = {
    background: "var(--bg)", border: "1px solid var(--border)", color: "var(--text)",
    borderRadius: 6, padding: "6px 10px", fontSize: 13, width: "100%",
  };

  return (
    <form onSubmit={handleSubmit} style={{
      background: "var(--bg-card)", border: "1px solid var(--border)",
      borderRadius: 8, padding: 20, marginBottom: 24,
    }}>
      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 16 }}>New Backtest</h3>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)", display: "block", marginBottom: 4 }}>Name</label>
          <input style={inputStyle} value={name} onChange={(e) => setName(e.target.value)} placeholder="My backtest" />
        </div>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)", display: "block", marginBottom: 4 }}>Start Date</label>
          <input type="date" style={inputStyle} value={startDate} onChange={(e) => setStartDate(e.target.value)} required />
        </div>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-dim)", display: "block", marginBottom: 4 }}>End Date</label>
          <input type="date" style={inputStyle} value={endDate} onChange={(e) => setEndDate(e.target.value)} required />
        </div>
      </div>

      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 12, color: "var(--text-dim)", marginBottom: 8 }}>Detector Parameters</div>
        <SliderRow label="Price Move Threshold (%)" name="price_move_threshold_pct"
          value={detectorParams.price_move_threshold_pct} min={0.01} max={0.20} step={0.01} onChange={setParam} />
        <SliderRow label="Volume Spike Multiplier" name="volume_spike_multiplier"
          value={detectorParams.volume_spike_multiplier} min={1.0} max={10.0} step={0.5} onChange={setParam} />
        <SliderRow label="Liquidity Drop (%)" name="liquidity_drop_threshold_pct"
          value={detectorParams.liquidity_drop_threshold_pct} min={0.05} max={0.80} step={0.05} onChange={setParam} />
        <SliderRow label="Bid-Ask Spread (%)" name="bid_ask_spread_threshold_pct"
          value={detectorParams.bid_ask_spread_threshold_pct} min={0.01} max={0.50} step={0.01} onChange={setParam} />
        <SliderRow label="Rank Threshold" name="rank_threshold"
          value={rankThreshold} min={0.0} max={1.0} step={0.05}
          onChange={(_, v) => setRankThreshold(v)} />
      </div>

      <div style={{ marginBottom: 16 }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={sweepMode} onChange={(e) => setSweepMode(e.target.checked)} />
          Parameter Sweep — test multiple parameter combinations
        </label>
      </div>

      {sweepMode && (
        <div style={{
          background: "var(--bg)", border: "1px solid var(--border)",
          borderRadius: 6, padding: 14, marginBottom: 16,
        }}>
          <div style={{ fontSize: 12, color: "var(--text-dim)", marginBottom: 10 }}>
            Sweep generates all combinations (capped at 50 runs)
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 10 }}>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Rank Min</label>
              <input type="number" style={inputStyle} value={sweepRankMin} min={0} max={1} step={0.05}
                onChange={(e) => setSweepRankMin(parseFloat(e.target.value))} />
            </div>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Rank Max</label>
              <input type="number" style={inputStyle} value={sweepRankMax} min={0} max={1} step={0.05}
                onChange={(e) => setSweepRankMax(parseFloat(e.target.value))} />
            </div>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Rank Step</label>
              <input type="number" style={inputStyle} value={sweepRankStep} min={0.01} max={0.5} step={0.01}
                onChange={(e) => setSweepRankStep(parseFloat(e.target.value))} />
            </div>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Price Move Min</label>
              <input type="number" style={inputStyle} value={sweepPriceMoveMin} min={0.01} max={0.20} step={0.01}
                onChange={(e) => setSweepPriceMoveMin(parseFloat(e.target.value))} />
            </div>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Price Move Max</label>
              <input type="number" style={inputStyle} value={sweepPriceMoveMax} min={0.01} max={0.20} step={0.01}
                onChange={(e) => setSweepPriceMoveMax(parseFloat(e.target.value))} />
            </div>
            <div>
              <label style={{ fontSize: 11, color: "var(--text-dim)", display: "block", marginBottom: 3 }}>Price Move Step</label>
              <input type="number" style={inputStyle} value={sweepPriceMoveStep} min={0.01} max={0.10} step={0.01}
                onChange={(e) => setSweepPriceMoveStep(parseFloat(e.target.value))} />
            </div>
          </div>
        </div>
      )}

      {error && <div style={{ color: "var(--red)", fontSize: 13, marginBottom: 10 }}>{error}</div>}

      <button type="submit" disabled={loading} style={{
        background: "var(--accent)", color: "#fff", border: "none",
        borderRadius: 6, padding: "8px 20px", fontSize: 13, fontWeight: 600,
        cursor: loading ? "not-allowed" : "pointer", opacity: loading ? 0.6 : 1,
      }}>
        {loading ? "Starting…" : sweepMode ? "Run Parameter Sweep" : "Run Backtest"}
      </button>
    </form>
  );
}

function RunRow({ run, onDelete, onRefresh }) {
  const [expanded, setExpanded] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const summary = run.result_summary || {};

  async function handleDelete(e) {
    e.stopPropagation();
    if (!window.confirm(`Delete backtest "${run.name}"?`)) return;
    setDeleting(true);
    try {
      await deleteBacktest(run.id);
      onDelete(run.id);
    } catch {
      setDeleting(false);
    }
  }

  return (
    <>
      <tr
        onClick={() => setExpanded((x) => !x)}
        style={{ borderBottom: "1px solid var(--border)", cursor: "pointer" }}
        onMouseEnter={(e) => e.currentTarget.style.background = "var(--bg-hover)"}
        onMouseLeave={(e) => e.currentTarget.style.background = ""}
      >
        <td style={{ padding: "10px 8px" }}>
          <Link
            to={`/backtests/${run.id}`}
            onClick={(e) => e.stopPropagation()}
            style={{ fontWeight: 600, fontSize: 13 }}
          >
            {run.name}
          </Link>
        </td>
        <td style={{ padding: "10px 8px", fontSize: 12, color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
          {run.start_date} → {run.end_date}
        </td>
        <td style={{ padding: "10px 8px" }}><StatusBadge status={run.status} /></td>
        <td style={{ padding: "10px 8px", textAlign: "right", fontFamily: "var(--mono)", fontSize: 13,
          color: summary.win_rate >= 0.5 ? "var(--green)" : summary.win_rate != null ? "var(--red)" : "var(--text-dim)" }}>
          {fmtPct(summary.win_rate)}
        </td>
        <td style={{ padding: "10px 8px", textAlign: "right", fontFamily: "var(--mono)", fontSize: 13 }}>
          {summary.total_signals ?? "—"}
        </td>
        <td style={{ padding: "10px 8px", textAlign: "right", fontSize: 12, color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
          {fmtDate(run.created_at)}
        </td>
        <td style={{ padding: "10px 8px", textAlign: "right" }}>
          <button onClick={handleDelete} disabled={deleting} style={{
            background: "transparent", border: "1px solid var(--border)",
            color: "var(--red)", fontSize: 11, borderRadius: 4,
            padding: "2px 8px", cursor: "pointer",
          }}>
            {deleting ? "…" : "Delete"}
          </button>
        </td>
      </tr>
      {expanded && (
        <tr style={{ borderBottom: "1px solid var(--border)" }}>
          <td colSpan={7} style={{ padding: "12px 16px", background: "var(--bg)" }}>
            {run.status === "completed" && summary.total_signals != null ? (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                <StatMini label="Win Rate" value={fmtPct(summary.win_rate)} color={summary.win_rate >= 0.5 ? "var(--green)" : "var(--red)"} />
                <StatMini label="Total Signals" value={summary.total_signals} />
                <StatMini label="Signals/Day" value={summary.signals_per_day?.toFixed(1) ?? "—"} />
                <StatMini label="False Positive Rate" value={fmtPct(summary.false_positive_rate)} />
              </div>
            ) : run.status === "failed" ? (
              <div style={{ color: "var(--red)", fontSize: 13 }}>Run failed. Check backend logs.</div>
            ) : (
              <div style={{ color: "var(--text-dim)", fontSize: 13 }}>
                {run.status === "pending" || run.status === "running"
                  ? "Results will appear when the run completes."
                  : "No summary available."}
              </div>
            )}
            <div style={{ marginTop: 10 }}>
              <Link to={`/backtests/${run.id}`} style={{ fontSize: 13 }}>View full results →</Link>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function StatMini({ label, value, color }) {
  return (
    <div>
      <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{label}</div>
      <div style={{ fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600, color: color || "var(--text)" }}>{value}</div>
    </div>
  );
}

export default function Backtest() {
  const [runs, setRuns] = useState(null);
  const [error, setError] = useState(null);

  const loadRuns = useCallback(() => {
    getBacktests()
      .then((data) => setRuns(data.runs || data))
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    loadRuns();
    // Poll while any run is pending/running
    const interval = setInterval(() => {
      if (runs && runs.some((r) => r.status === "pending" || r.status === "running")) {
        loadRuns();
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [loadRuns, runs]);

  function handleDelete(id) {
    setRuns((rs) => rs.filter((r) => r.id !== id));
  }

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;

  return (
    <div>
      <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 16 }}>Backtesting</h2>

      <CreateForm onCreated={loadRuns} />

      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 10 }}>Backtest Runs</h3>
      {runs == null ? (
        <div className="skeleton" style={{ height: 120, borderRadius: 8 }} />
      ) : runs.length === 0 ? (
        <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 20 }}>No backtest runs yet. Create one above.</div>
      ) : (
        <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
          <table style={{ width: "100%", minWidth: 600, borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)" }}>
                <th style={{ textAlign: "left", padding: "8px 8px", color: "var(--text-dim)" }}>Name</th>
                <th style={{ textAlign: "left", padding: "8px 8px", color: "var(--text-dim)" }}>Date Range</th>
                <th style={{ textAlign: "left", padding: "8px 8px", color: "var(--text-dim)" }}>Status</th>
                <th style={{ textAlign: "right", padding: "8px 8px", color: "var(--text-dim)" }}>Win Rate</th>
                <th style={{ textAlign: "right", padding: "8px 8px", color: "var(--text-dim)" }}>Signals</th>
                <th style={{ textAlign: "right", padding: "8px 8px", color: "var(--text-dim)" }}>Created</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <RunRow key={run.id} run={run} onDelete={handleDelete} onRefresh={loadRuns} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
