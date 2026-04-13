import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ScatterChart,
  Scatter,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { getBacktest, getBacktestSignals } from "../api";

const DETECTOR_REPLAY_MODE = "detector_replay";
const STRATEGY_COMPARISON_REPLAY_MODE = "strategy_comparison";

const REPLAY_MODE_LABELS = {
  [DETECTOR_REPLAY_MODE]: "Detector replay",
  [STRATEGY_COMPARISON_REPLAY_MODE]: "Frozen default vs legacy",
};

const REPLAY_PATH_LABELS = {
  default_strategy: "Frozen default",
  legacy: "Legacy",
};

function fmtPct(v) {
  if (v == null) return "--";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtDate(iso) {
  if (!iso) return "--";
  return new Date(iso).toLocaleString();
}

function fmtMoney(v) {
  if (v == null) return "--";
  const value = Number(v);
  const prefix = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${prefix}$${Math.abs(value).toFixed(2)}`;
}

function fmtNumber(v, digits = 2) {
  if (v == null) return "--";
  return Number(v).toFixed(digits);
}

function fmtIntegerDelta(v) {
  if (v == null) return "--";
  const prefix = v > 0 ? "+" : "";
  return `${prefix}${Number(v).toFixed(0)}`;
}

function fmtPctDelta(v) {
  if (v == null) return "--";
  const prefix = v > 0 ? "+" : "";
  return `${prefix}${(v * 100).toFixed(1)}%`;
}

function fmtNumberDelta(v, digits = 2) {
  if (v == null) return "--";
  const prefix = v > 0 ? "+" : "";
  return `${prefix}${Number(v).toFixed(digits)}`;
}

function getReplayMode(run) {
  return run?.replay_mode || run?.result_summary?.replay_mode || DETECTOR_REPLAY_MODE;
}

function getReplayPath(signal) {
  return signal?.details?.replay?.replay_path || "";
}

function replayPathLabel(path) {
  return REPLAY_PATH_LABELS[path] || path || "--";
}

function SummaryCard({ label, value, color, sub }) {
  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: 18,
      }}
    >
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
          formatter={(value, name) => [name === "win_rate" ? `${value}%` : value, name === "win_rate" ? "Win Rate" : "Signals"]}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Bar dataKey="win_rate" name="Win Rate (%)" radius={[4, 4, 0, 0]}>
          {chartData.map((entry, index) => (
            <Cell key={index} fill={entry.win_rate >= 50 ? "#00d68f" : "#ff6b6b"} />
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
    .filter((horizon) => data[horizon])
    .map((horizon) => ({
      horizon,
      win_rate: data[horizon].win_rate != null ? parseFloat((data[horizon].win_rate * 100).toFixed(1)) : 0,
      total: data[horizon].total ?? 0,
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
          formatter={(value) => [`${value}%`, "Win Rate"]}
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

  const chartData = signals.map((signal) => ({
    x: new Date(signal.fired_at).getTime(),
    y: signal.rank_score != null ? parseFloat((signal.rank_score * 100).toFixed(1)) : 0,
    resolved: signal.resolved_correctly,
  }));

  const colorFor = (resolved) => {
    if (resolved === true) return "#00d68f";
    if (resolved === false) return "#ff6b6b";
    return "#8888a0";
  };

  return (
    <ResponsiveContainer width="100%" height={220}>
      <ScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.cartesianGrid} />
        <XAxis
          dataKey="x"
          type="number"
          domain={["auto", "auto"]}
          name="Time"
          tickFormatter={(value) => new Date(value).toLocaleDateString()}
          tick={{ fill: CHART_THEME.axis, fontSize: 10 }}
        />
        <YAxis dataKey="y" unit="%" name="Rank Score" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} />
        <Tooltip
          cursor={{ strokeDasharray: "3 3" }}
          contentStyle={{ background: CHART_THEME.tooltip.bg, border: `1px solid ${CHART_THEME.tooltip.border}`, borderRadius: 6 }}
          formatter={(value, name) => {
            if (name === "x") return [new Date(value).toLocaleString(), "Fired At"];
            if (name === "y") return [`${value}%`, "Rank Score"];
            return [value, name];
          }}
        />
        <Scatter
          data={chartData}
          shape={(props) => {
            const { cx, cy, payload } = props;
            return <circle cx={cx} cy={cy} r={4} fill={colorFor(payload.resolved)} fillOpacity={0.8} />;
          }}
        />
      </ScatterChart>
    </ResponsiveContainer>
  );
}

function ComparisonTable({ comparison }) {
  const defaultSummary = comparison?.default_strategy;
  const legacySummary = comparison?.legacy;

  if (!defaultSummary || !legacySummary) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13 }}>Comparison summary is not available.</div>;
  }

  const metrics = [
    {
      label: "Candidate signals",
      key: "candidate_signals",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
    {
      label: "Qualified signals",
      key: "qualified_signals",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
    {
      label: "Trades opened",
      key: "traded_signals",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
    {
      label: "Resolved trades",
      key: "resolved_trades",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
    { label: "Win rate", key: "win_rate", format: fmtPct, deltaFormat: fmtPctDelta },
    { label: "Cumulative PnL", key: "cumulative_pnl", format: fmtMoney, deltaFormat: fmtMoney },
    { label: "Shadow PnL", key: "shadow_cumulative_pnl", format: fmtMoney, deltaFormat: fmtMoney },
    { label: "Max drawdown", key: "max_drawdown", format: fmtMoney, deltaFormat: fmtMoney },
    {
      label: "Profit factor",
      key: "profit_factor",
      format: (value) => fmtNumber(value, 2),
      deltaFormat: (value) => fmtNumberDelta(value, 2),
    },
    {
      label: "Liquidity constrained trades",
      key: "liquidity_constrained_trades",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
    {
      label: "Missing orderbook context",
      key: "trades_missing_orderbook_context",
      format: (value) => value ?? "--",
      deltaFormat: fmtIntegerDelta,
    },
  ];

  return (
    <div className="table-scroll" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", minWidth: 620, borderCollapse: "collapse", fontSize: 12 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)" }}>
            <th style={{ textAlign: "left", padding: "8px 10px", color: "var(--text-dim)" }}>Metric</th>
            <th style={{ textAlign: "right", padding: "8px 10px", color: "var(--text-dim)" }}>Frozen default</th>
            <th style={{ textAlign: "right", padding: "8px 10px", color: "var(--text-dim)" }}>Legacy</th>
            <th style={{ textAlign: "right", padding: "8px 10px", color: "var(--text-dim)" }}>Delta</th>
          </tr>
        </thead>
        <tbody>
          {metrics.map((metric) => {
            const defaultValue = defaultSummary[metric.key];
            const legacyValue = legacySummary[metric.key];
            const deltaValue = typeof defaultValue === "number" && typeof legacyValue === "number" ? defaultValue - legacyValue : null;

            return (
              <tr key={metric.key} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={{ padding: "8px 10px", color: "var(--text)" }}>{metric.label}</td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)" }}>{metric.format(defaultValue)}</td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)" }}>{metric.format(legacyValue)}</td>
                <td style={{ padding: "8px 10px", textAlign: "right", fontFamily: "var(--mono)" }}>
                  {metric.deltaFormat(deltaValue)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function SkipReasonList({ title, reasons }) {
  return (
    <div
      style={{
        background: "var(--bg)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: 14,
      }}
    >
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10 }}>{title}</div>
      {!reasons || reasons.length === 0 ? (
        <div style={{ color: "var(--text-dim)", fontSize: 12 }}>No skipped signals recorded.</div>
      ) : (
        <div style={{ display: "grid", gap: 8 }}>
          {reasons.map((reason) => (
            <div key={`${title}-${reason.reason_code}`} style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>{reason.reason_label}</span>
              <span style={{ fontFamily: "var(--mono)", fontSize: 12 }}>{reason.count}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 28 }}>
      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>{title}</h3>
      <div
        style={{
          background: "var(--bg-card)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: 16,
        }}
      >
        {children}
      </div>
    </div>
  );
}

export default function BacktestResult() {
  const { id } = useParams();
  const [run, setRun] = useState(null);
  const [signals, setSignals] = useState(null);
  const [signalFilter, setSignalFilter] = useState("");
  const [replayPathFilter, setReplayPathFilter] = useState("");
  const [error, setError] = useState(null);

  useEffect(() => {
    getBacktest(id)
      .then((data) => {
        setRun(data);
      })
      .catch((e) => setError(e.message));

    getBacktestSignals(id, { pageSize: 500 })
      .then((data) => setSignals(data.signals || data))
      .catch(() => setSignals([]));
  }, [id]);

  useEffect(() => {
    if (!run || run.status === "completed" || run.status === "failed") return;
    const timeout = setTimeout(() => {
      getBacktest(id).then(setRun).catch(() => {});
    }, 4000);
    return () => clearTimeout(timeout);
  }, [run, id]);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  if (!run) return <div className="skeleton" style={{ height: 200, borderRadius: 8 }} />;

  const summary = run.result_summary || {};
  const replayMode = getReplayMode(run);
  const isBusy = run.status === "pending" || run.status === "running";
  const isStrategyComparison = replayMode === STRATEGY_COMPARISON_REPLAY_MODE;
  const defaultSummary = summary.comparison?.default_strategy || {};
  const legacySummary = summary.comparison?.legacy || {};
  const deltaSummary = summary.comparison?.delta || {};

  const filteredSignals = signals
    ? signals.filter((signal) => {
        if (signalFilter && signal.signal_type !== signalFilter) return false;
        if (replayPathFilter && getReplayPath(signal) !== replayPathFilter) return false;
        return true;
      })
    : [];

  const signalTypes = signals ? [...new Set(signals.map((signal) => signal.signal_type))].sort() : [];
  const replayPaths = signals ? [...new Set(signals.map((signal) => getReplayPath(signal)).filter(Boolean))].sort() : [];

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        <Link to="/backtests" style={{ fontSize: 13, color: "var(--text-dim)" }}>{"<-"} All Backtests</Link>
        <h2 style={{ fontSize: 16, fontWeight: 600 }}>{run.name}</h2>
        <span
          style={{
            fontSize: 11,
            fontWeight: 700,
            textTransform: "uppercase",
            color: { pending: "var(--yellow)", running: "var(--accent)", completed: "var(--green)", failed: "var(--red)" }[run.status] || "var(--text-dim)",
            border: "1px solid currentColor",
            padding: "2px 7px",
            borderRadius: 4,
          }}
        >
          {run.status}
        </span>
        <span
          style={{
            fontSize: 10,
            fontWeight: 700,
            textTransform: "uppercase",
            color: isStrategyComparison ? "var(--accent)" : "var(--text-dim)",
            border: `1px solid ${isStrategyComparison ? "var(--accent)" : "var(--border)"}`,
            borderRadius: 999,
            padding: "2px 8px",
          }}
        >
          {REPLAY_MODE_LABELS[replayMode] || replayMode}
        </span>
      </div>

      <div style={{ fontSize: 12, color: "var(--text-dim)", marginBottom: 20 }}>
        {run.start_date} {"->"} {run.end_date} {" | "} Rank threshold: {run.rank_threshold} {" | "} Created {fmtDate(run.created_at)}
        {run.completed_at && ` | Completed ${fmtDate(run.completed_at)}`}
      </div>

      {isBusy && (
        <div
          style={{
            background: "var(--bg-card)",
            border: "1px solid var(--accent)",
            borderRadius: 8,
            padding: 20,
            textAlign: "center",
            marginBottom: 24,
            color: "var(--accent)",
            fontSize: 14,
          }}
        >
          {run.status === "pending" ? "Backtest queued, waiting to start..." : "Backtest running. Results will appear when complete."}
        </div>
      )}

      {run.status === "completed" && (
        <>
          {isStrategyComparison ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12, marginBottom: 28 }}>
                <SummaryCard label="Default Win Rate" value={fmtPct(defaultSummary.win_rate)} color={winRateColor(defaultSummary.win_rate)} />
                <SummaryCard label="Legacy Win Rate" value={fmtPct(legacySummary.win_rate)} color={winRateColor(legacySummary.win_rate)} />
                <SummaryCard label="Default PnL" value={fmtMoney(defaultSummary.cumulative_pnl)} color={(defaultSummary.cumulative_pnl || 0) >= 0 ? "var(--green)" : "var(--red)"} />
                <SummaryCard label="Legacy PnL" value={fmtMoney(legacySummary.cumulative_pnl)} color={(legacySummary.cumulative_pnl || 0) >= 0 ? "var(--green)" : "var(--red)"} />
                <SummaryCard label="PnL Delta" value={fmtMoney(deltaSummary.cumulative_pnl)} color={(deltaSummary.cumulative_pnl || 0) >= 0 ? "var(--green)" : "var(--red)"} />
                <SummaryCard label="Trade Delta" value={fmtIntegerDelta(deltaSummary.traded_signals)} />
              </div>

              <Section title="Replay Comparison">
                <ComparisonTable comparison={summary.comparison} />
              </Section>

              <Section title="Skip Reasons">
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
                  <SkipReasonList title="Frozen default" reasons={defaultSummary.skip_reasons} />
                  <SkipReasonList title="Legacy" reasons={legacySummary.skip_reasons} />
                </div>
              </Section>
            </>
          ) : (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12, marginBottom: 28 }}>
                <SummaryCard label="Win Rate" value={fmtPct(summary.win_rate)} color={winRateColor(summary.win_rate)} />
                <SummaryCard label="Total Signals" value={summary.total_signals ?? "--"} />
                <SummaryCard label="Signals / Day" value={summary.signals_per_day?.toFixed(1) ?? "--"} />
                <SummaryCard label="False Positive Rate" value={fmtPct(summary.false_positive_rate)} />
              </div>

              <Section title="Win Rate by Detector Type">
                <AccuracyByTypeChart data={summary.accuracy_by_type} />
              </Section>

              <Section title="Win Rate by Horizon">
                <AccuracyByHorizonChart data={summary.accuracy_by_horizon} />
              </Section>
            </>
          )}

          <Section title="Signal Timeline">
            <div style={{ marginBottom: 10, display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Filters:</span>
              <select
                value={signalFilter}
                onChange={(e) => setSignalFilter(e.target.value)}
                style={{
                  background: "var(--bg)",
                  border: "1px solid var(--border)",
                  color: "var(--text)",
                  borderRadius: 5,
                  padding: "4px 24px 4px 8px",
                  fontSize: 12,
                }}
              >
                <option value="">All types</option>
                {signalTypes.map((type) => (
                  <option key={type} value={type}>{type.replace(/_/g, " ")}</option>
                ))}
              </select>
              {isStrategyComparison && (
                <select
                  value={replayPathFilter}
                  onChange={(e) => setReplayPathFilter(e.target.value)}
                  style={{
                    background: "var(--bg)",
                    border: "1px solid var(--border)",
                    color: "var(--text)",
                    borderRadius: 5,
                    padding: "4px 24px 4px 8px",
                    fontSize: 12,
                  }}
                >
                  <option value="">All replay paths</option>
                  {replayPaths.map((path) => (
                    <option key={path} value={path}>{replayPathLabel(path)}</option>
                  ))}
                </select>
              )}
              <div style={{ display: "flex", gap: 12, marginLeft: "auto", fontSize: 11 }}>
                <span style={{ color: "var(--green)" }}>Correct</span>
                <span style={{ color: "var(--red)" }}>Incorrect</span>
                <span style={{ color: "var(--text-dim)" }}>Unresolved</span>
              </div>
            </div>
            {signals == null ? <div className="skeleton" style={{ height: 220, borderRadius: 6 }} /> : <SignalTimeline signals={filteredSignals} />}
          </Section>

          <Section title="Signal List">
            {signals == null ? (
              <div className="skeleton" style={{ height: 100, borderRadius: 6 }} />
            ) : filteredSignals.length === 0 ? (
              <div style={{ color: "var(--text-dim)", fontSize: 13 }}>No signals match the current filter.</div>
            ) : (
              <div className="table-scroll" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", minWidth: isStrategyComparison ? 760 : 640, borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)" }}>
                      {isStrategyComparison && (
                        <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--text-dim)" }}>Replay Path</th>
                      )}
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
                    {filteredSignals.slice(0, 200).map((signal) => (
                      <tr key={signal.id} style={{ borderBottom: "1px solid var(--border)" }}>
                        {isStrategyComparison && (
                          <td style={{ padding: "6px 8px", color: "var(--text-dim)" }}>
                            {replayPathLabel(getReplayPath(signal))}
                          </td>
                        )}
                        <td style={{ padding: "6px 8px", fontWeight: 600, fontSize: 11, textTransform: "uppercase", color: "var(--accent)" }}>
                          {signal.signal_type?.replace(/_/g, " ")}
                        </td>
                        <td style={{ padding: "6px 8px", color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
                          {fmtDate(signal.fired_at)}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {signal.rank_score != null ? `${(signal.rank_score * 100).toFixed(0)}%` : "--"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {signal.signal_score != null ? signal.signal_score.toFixed(3) : "--"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {signal.price_at_fire != null ? `${(signal.price_at_fire * 100).toFixed(1)}%` : "--"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: "var(--mono)" }}>
                          {signal.price_at_resolution != null ? `${(signal.price_at_resolution * 100).toFixed(1)}%` : "--"}
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "center" }}>
                          {signal.resolved_correctly === true ? (
                            <span style={{ color: "var(--green)", fontWeight: 700 }}>Yes</span>
                          ) : signal.resolved_correctly === false ? (
                            <span style={{ color: "var(--red)", fontWeight: 700 }}>No</span>
                          ) : (
                            <span style={{ color: "var(--text-dim)" }}>--</span>
                          )}
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
        <div
          style={{
            background: "var(--bg-card)",
            border: "1px solid var(--red)",
            borderRadius: 8,
            padding: 20,
            color: "var(--red)",
          }}
        >
          This backtest run failed. Check backend logs for details.
        </div>
      )}
    </div>
  );
}
