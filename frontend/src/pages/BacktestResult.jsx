import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ScatterChart, Scatter, ResponsiveContainer, Cell,
} from "recharts";
import { getBacktest, getBacktestSignals } from "../api";

function fmtPct(v) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

function SummaryCard({ label, value, color, sub }) {
  return (
    <div style={{
      background: "var(--bg-card)", border: "1px solid var(--border)",
      borderRadius: 8, padding: 18,
    }}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 28, fontFamily: "var(--mono)", fontWeight: 700, color: color || "var(--text)" }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function winRateColor(rate) {
  if (rate == null) return "var(--text-dim)";
  return rate >= 0.5 ? "var(--green)" : "var(--red)";
}

const CHART_THEME = {
  cartesianGrid: "var(--border)",
  axis: "var(--text-dim)",
  tooltip: { bg: "var(--bg-card)", border: "var(--border)", text: "var(--text)" },
};

function AccuracyByTypeChart({ data }) {
  if (!data || Object.keys(data).length === 0) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 16 }}>No per-type data available.</div>;
  }

  const chartData = Object.entries(data).map(([type, stats]) => ({
    name: type.replace(/_/g, " "),
    win_rate: stats.win_rate != null ? parseFloat((stats.win_rate * 100).toFixed(1)) : 0,
    total: stats.total ?? 0,
  }));

  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={chartData} margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.cartesianGrid} />
        <XAxis dataKey="name" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} />
        <YAxis unit="%" domain={[0, 100]} tick={{ fill: CHART_THEME.axis, fontSize: 11 }} />
        <Tooltip
          contentStyle={{ background: CHART_THEME.tooltip.bg, border: `1px solid ${CHART_THEME.tooltip.border}`, borderRadius: 6 }}
          labelStyle={{ color: CHART_THEME.tooltip.text, fontWeight: 600 }}
          formatter={(v, name) => [name === "win_rate" ? `${v}%` : v, name === "win_rate" ? "Win Rate" : "Signals"]}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Bar dataKey="win_rate" name="Win Rate (%)" radius={[4, 4, 0, 0]}>
          {chartData.map((entry, i) => (
            <Cell key={i} fill={entry.win_rate >= 50 ? "#00d68f" : "#ff6b6b"} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

function AccuracyByHorizonChart({ data }) {
  if (!data || Object.keys(data).length === 0) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 16 }}>No horizon data available.</div>;
  }

  const horizons = ["15m", "1h", "4h", "24h"];
  const chartData = horizons
    .filter((h) => data[h])
    .map((h) => ({
      horizon: h,
      win_rate: data[h].win_rate != null ? parseFloat((data[h].win_rate * 100).toFixed(1)) : 0,
      total: data[h].total ?? 0,
    }));

  if (chartData.length === 0) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 16 }}>No horizon data available.</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={chartData} margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.cartesianGrid} />
        <XAxis dataKey="horizon" tick={{ fill: CHART_THEME.axis, fontSize: 12 }} />
        <YAxis unit="%" domain={[0, 100]} tick={{ fill: CHART_THEME.axis, fontSize: 11 }} />
        <Tooltip
          contentStyle={{ background: CHART_THEME.tooltip.bg, border: `1px solid ${CHART_THEME.tooltip.border}`, borderRadius: 6 }}
          labelStyle={{ color: CHART_THEME.tooltip.text, fontWeight: 600 }}
          formatter={(v) => [`${v}%`, "Win Rate"]}
        />
        <Bar dataKey="win_rate" name="Win Rate (%)" fill="var(--accent)" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

function SignalTimeline({ signals }) {
  if (!signals || signals.length === 0) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 16 }}>No signals to display.</div>;
  }

  const chartData = signals.map((s) => ({
    x: new Date(s.fired_at).getTime(),
    y: s.rank_score != null ? parseFloat((s.rank_score * 100).toFixed(1)) : 0,
    resolved: s.resolved_correctly,
  }));

  const colorFor = (r) => r === true ? "#00d68f" : r === false ? "#ff6b6b" : "#8888a0";

  return (
    <ResponsiveContainer width="100%" height={220}>
      <ScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.cartesianGrid} />
        <XAxis
          dataKey="x" type="number" domain={["auto", "auto"]} name="Time"
          tickFormatter={(t) => new Date(t).toLocaleDateString()}
          tick={{ fill: CHART_THEME.axis, fontSize: 10 }}
        />
        <YAxis dataKey="y" unit="%" name="Rank Score" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} />
        <Tooltip
          cursor={{ strokeDasharray: "3 3" }}
          contentStyle={{ background: CHART_THEME.tooltip.bg, border: `1px solid ${CHART_THEME.tooltip.border}`, borderRadius: 6 }}
          formatter={(v, name) => {
            if (name === "x") return [new Date(v).toLocaleString(), "Fired At"];
            if (name === "y") return [`${v}%`, "Rank Score"];
            return [v, name];
          }}
        />
        <Scatter data={chartData} shape={(props) => {
          const { cx, cy, payload } = props;
          return <circle cx={cx} cy={cy} r={4} fill={colorFor(payload.resolved)} fillOpacity={0.8} />;
        }} />
      </ScatterChart>
    </ResponsiveContainer>
  );
}

const SORT_KEYS = ["win_rate", "total_signals", "false_positive_rate"];

function SweepTable({ runs }) {
  const [sortKey, setSortKey] = useState("win_rate");
  const [sortDesc, setSortDesc] = useState(true);

  if (!runs || runs.length === 0) return null;

  const completed = runs.filter((r) => r.status === "completed" && r.result_summary);
  if (completed.length === 0) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13 }}>Sweep runs still in progress…</div>;
  }

  const sorted = [...completed].sort((a, b) => {
    const av = a.result_summary?.[sortKey] ?? -1;
    const bv = b.result_summary?.[sortKey] ?? -1;
    return sortDesc ? bv - av : av - bv;
  });

  const bestId = sorted[0]?.id;

  function toggleSort(key) {
    if (sortKey === key) setSortDesc((d) => !d);
    else { setSortKey(key); setSortDesc(true); }
  }

  function SortTh({ label, k, right }) {
    const active = sortKey === k;
    return (
      <th onClick={() => toggleSort(k)} style={{
        textAlign: right ? "right" : "left", padding: "8px 10px",
        color: active ? "var(--accent)" : "var(--text-dim)",
        cursor: "pointer", userSelect: "none", fontSize: 12,
      }}>
        {label} {active ? (sortDesc ? "↓" : "↑") : ""}
      </th>
    );
  }

  return (
    <div className="table-scroll" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", minWidth: 500, borderCollapse: "collapse", fontSize: 12 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)" }}>
            <th style={{ textAlign: "left", padding: "8px 10px", color: "var(--text-dim)" }}>Name</th>
            <th style={{ textAlign: "left", padding: "8px 10px", color: "var(--text-dim)" }}>Rank Threshold</th>
            <SortTh label="Win Rate" k="win_rate" right />
            <SortTh label="Signals" k="total_signals" right />
            <SortTh label="False Pos. Rate" k="false_positive_rate" right />
          </tr>
        </thead>
        <tbody>
          {sorted.map((run) => {
            const s = run.result_summary;
            const isBest = run.id === bestId;
            return (
              <tr key={run.id} style={{
                borderBottom: "1px solid var(--border)",
                background: isBest ? "rgba(108, 92, 231, 0.08)" : "",
              }}>
                <td style={{ padding: "8px 10px" }}>
                  <Link to={`/backtests/${run.id}`}>{run.name}</Link>
                  {isBest && (
                    <span style={{
                      marginLeft: 8, fontSize: 10, fontWeight: 700,
                      color: "var(--accent)", border: "1px solid var(--accent)",
                      padding: "1px 5px", borderRadius: 4,
                    }}>BEST</span>
                  )}
                </td>
                <td style={{ padding: "8px 10px", fontFamily: "var(--mono)" }}>{run.rank_threshold}</td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)",
                  fontWeight: 600, color: winRateColor(s.win_rate) }}>
                  {fmtPct(s.win_rate)}
                </td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)" }}>
                  {s.total_signals ?? "—"}
                </td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)" }}>
                  {fmtPct(s.false_positive_rate)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 28 }}>
      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>{title}</h3>
      <div style={{
        background: "var(--bg-card)", border: "1px solid var(--border)",
        borderRadius: 8, padding: 16,
      }}>
        {children}
      </div>
    </div>
  );
}

export default function BacktestResult() {
  const { id } = useParams();
  const [run, setRun] = useState(null);
  const [signals, setSignals] = useState(null);
  const [sweepRuns, setSweepRuns] = useState(null);
  const [signalFilter, setSignalFilter] = useState("");
  const [error, setError] = useState(null);

  useEffect(() => {
    getBacktest(id)
      .then((data) => {
        setRun(data);
        // If this is a sweep member, fetch sibling runs
        if (data.sweep_id) {
          // Backend should expose sweep siblings; if not, we just show this run
        }
      })
      .catch((e) => setError(e.message));

    getBacktestSignals(id, { pageSize: 500 })
      .then((data) => setSignals(data.signals || data))
      .catch(() => setSignals([]));
  }, [id]);

  // Poll while running
  useEffect(() => {
    if (!run || run.status === "completed" || run.status === "failed") return;
    const t = setTimeout(() => {
      getBacktest(id).then(setRun).catch(() => {});
    }, 4000);
    return () => clearTimeout(t);
  }, [run, id]);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  if (!run) return <div className="skeleton" style={{ height: 200, borderRadius: 8 }} />;

  const summary = run.result_summary || {};
  const isBusy = run.status === "pending" || run.status === "running";

  const filteredSignals = signals
    ? signals.filter((s) => !signalFilter || s.signal_type === signalFilter)
    : [];

  const signalTypes = signals ? [...new Set(signals.map((s) => s.signal_type))] : [];

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
        <Link to="/backtests" style={{ fontSize: 13, color: "var(--text-dim)" }}>← All Backtests</Link>
        <h2 style={{ fontSize: 16, fontWeight: 600 }}>{run.name}</h2>
        <span style={{
          fontSize: 11, fontWeight: 700, textTransform: "uppercase",
          color: { pending: "var(--yellow)", running: "var(--accent)", completed: "var(--green)", failed: "var(--red)" }[run.status] || "var(--text-dim)",
          border: `1px solid currentColor`, padding: "2px 7px", borderRadius: 4,
        }}>
          {run.status}
        </span>
      </div>

      <div style={{ fontSize: 12, color: "var(--text-dim)", marginBottom: 20 }}>
        {run.start_date} → {run.end_date} &nbsp;·&nbsp; Rank threshold: {run.rank_threshold}
        &nbsp;·&nbsp; Created {fmtDate(run.created_at)}
        {run.completed_at && ` · Completed ${fmtDate(run.completed_at)}`}
      </div>

      {isBusy && (
        <div style={{
          background: "var(--bg-card)", border: "1px solid var(--accent)",
          borderRadius: 8, padding: 20, textAlign: "center", marginBottom: 24,
          color: "var(--accent)", fontSize: 14,
        }}>
          {run.status === "pending" ? "Backtest queued, waiting to start…" : "Backtest running — results will appear when complete."}
        </div>
      )}

      {run.status === "completed" && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 28 }}>
            <SummaryCard label="Win Rate" value={fmtPct(summary.win_rate)}
              color={winRateColor(summary.win_rate)} />
            <SummaryCard label="Total Signals" value={summary.total_signals ?? "—"} />
            <SummaryCard label="Signals / Day" value={summary.signals_per_day?.toFixed(1) ?? "—"} />
            <SummaryCard label="False Positive Rate" value={fmtPct(summary.false_positive_rate)} />
          </div>

          <Section title="Win Rate by Detector Type">
            <AccuracyByTypeChart data={summary.accuracy_by_type} />
          </Section>

          <Section title="Win Rate by Horizon">
            <AccuracyByHorizonChart data={summary.accuracy_by_horizon} />
          </Section>

          {sweepRuns && sweepRuns.length > 1 && (
            <Section title="Parameter Sweep Comparison">
              <SweepTable runs={sweepRuns} />
            </Section>
          )}

          <Section title="Signal Timeline">
            <div style={{ marginBottom: 10, display: "flex", gap: 12, alignItems: "center" }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Filter:</span>
              <select
                value={signalFilter}
                onChange={(e) => setSignalFilter(e.target.value)}
                style={{
                  background: "var(--bg)", border: "1px solid var(--border)", color: "var(--text)",
                  borderRadius: 5, padding: "4px 24px 4px 8px", fontSize: 12,
                }}
              >
                <option value="">All types</option>
                {signalTypes.map((t) => <option key={t} value={t}>{t.replace(/_/g, " ")}</option>)}
              </select>
              <div style={{ display: "flex", gap: 12, marginLeft: "auto", fontSize: 11 }}>
                <span style={{ color: "var(--green)" }}>● Correct</span>
                <span style={{ color: "var(--red)" }}>● Incorrect</span>
                <span style={{ color: "var(--text-dim)" }}>● Unresolved</span>
              </div>
            </div>
            {signals == null ? (
              <div className="skeleton" style={{ height: 220, borderRadius: 6 }} />
            ) : (
              <SignalTimeline signals={filteredSignals} />
            )}
          </Section>

          <Section title="Signal List">
            {signals == null ? (
              <div className="skeleton" style={{ height: 100, borderRadius: 6 }} />
            ) : filteredSignals.length === 0 ? (
              <div style={{ color: "var(--text-dim)", fontSize: 13 }}>No signals match the current filter.</div>
            ) : (
              <div className="table-scroll" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", minWidth: 600, borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)" }}>
                      <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--text-dim)" }}>Type</th>
                      <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--text-dim)" }}>Fired At</th>
                      <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--text-dim)" }}>Rank</th>
                      <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--text-dim)" }}>Score</th>
                      <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--text-dim)" }}>Price @ Fire</th>
                      <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--text-dim)" }}>Price @ Res.</th>
                      <th style={{ textAlign: "center", padding: "6px 8px", color: "var(--text-dim)" }}>Outcome</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredSignals.slice(0, 200).map((s) => (
                      <tr key={s.id} style={{ borderBottom: "1px solid var(--border)" }}>
                        <td style={{ padding: "6px 8px", fontWeight: 600, fontSize: 11,
                          textTransform: "uppercase", color: "var(--accent)" }}>
                          {s.signal_type?.replace(/_/g, " ")}
                        </td>
                        <td style={{ padding: "6px 8px", color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
                          {fmtDate(s.fired_at)}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {s.rank_score != null ? (s.rank_score * 100).toFixed(0) + "%" : "—"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {s.signal_score != null ? s.signal_score.toFixed(3) : "—"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {s.price_at_fire != null ? `${(s.price_at_fire * 100).toFixed(1)}%` : "—"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {s.price_at_resolution != null ? `${(s.price_at_resolution * 100).toFixed(1)}%` : "—"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "center" }}>
                          {s.resolved_correctly === true
                            ? <span style={{ color: "var(--green)", fontWeight: 700 }}>✓</span>
                            : s.resolved_correctly === false
                              ? <span style={{ color: "var(--red)", fontWeight: 700 }}>✗</span>
                              : <span style={{ color: "var(--text-dim)" }}>—</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {filteredSignals.length > 200 && (
                  <div style={{ fontSize: 12, color: "var(--text-dim)", padding: "8px 8px 0" }}>
                    Showing first 200 of {filteredSignals.length} signals.
                  </div>
                )}
              </div>
            )}
          </Section>
        </>
      )}

      {run.status === "failed" && (
        <div style={{
          background: "var(--bg-card)", border: "1px solid var(--red)",
          borderRadius: 8, padding: 20, color: "var(--red)",
        }}>
          This backtest run failed. Check backend logs for details.
        </div>
      )}
    </div>
  );
}
