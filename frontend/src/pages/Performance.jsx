import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ReferenceLine,
  ResponsiveContainer, Cell,
} from "recharts";
import { getPerformanceSummary } from "../api";

function fmtPct(v) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function winColor(wr) {
  if (wr == null) return "var(--text-dim)";
  return wr >= 0.6 ? "var(--green)" : wr >= 0.4 ? "var(--yellow)" : "var(--red)";
}

// ── Hero metric card ────────────────────────────────────────────────────────
function HeroCard({ label, value, color, sub }) {
  return (
    <div style={{
      background: "var(--bg-card)", border: "1px solid var(--border)",
      borderRadius: 8, padding: "20px 24px",
    }}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ fontSize: 32, fontFamily: "var(--mono)", fontWeight: 700, color: color || "var(--text)" }}>
        {value}
      </div>
      {sub && <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

// ── Section wrapper ─────────────────────────────────────────────────────────
function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 32 }}>
      <h2 style={{ fontSize: 15, fontWeight: 600, marginBottom: 12, color: "var(--text)" }}>{title}</h2>
      {children}
    </div>
  );
}

// ── Win rate trend line chart ───────────────────────────────────────────────
function TrendChart({ data }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No trend data yet.</div>;
  }

  // Compute 7-day moving average
  const enriched = data.map((d, i) => {
    const slice = data.slice(Math.max(0, i - 6), i + 1).filter((x) => x.win_rate != null);
    const ma = slice.length > 0 ? slice.reduce((s, x) => s + x.win_rate, 0) / slice.length : null;
    return { ...d, ma };
  });

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "16px 8px 8px" }}>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={enriched} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: "var(--text-dim)" }} tickFormatter={(v) => fmtDate(v + "T00:00:00Z")} />
          <YAxis domain={[0, 1]} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 11, fill: "var(--text-dim)" }} width={40} />
          <Tooltip
            formatter={(v, name) => [fmtPct(v), name === "win_rate" ? "Win Rate" : "7d MA"]}
            labelFormatter={(l) => fmtDate(l + "T00:00:00Z")}
            contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", fontSize: 12 }}
          />
          <ReferenceLine y={0.5} stroke="var(--text-dim)" strokeDasharray="4 4" />
          <Line type="monotone" dataKey="win_rate" stroke="var(--accent)" dot={{ r: 3 }} strokeWidth={1.5} name="win_rate" connectNulls />
          <Line type="monotone" dataKey="ma" stroke="var(--green)" dot={false} strokeWidth={2} strokeDasharray="5 3" name="ma" connectNulls />
        </LineChart>
      </ResponsiveContainer>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textAlign: "center", marginTop: 4 }}>
        Daily win rate (blue) and 7-day moving average (green)
      </div>
    </div>
  );
}

// ── Detector leaderboard ────────────────────────────────────────────────────
function DetectorLeaderboard({ data, best, worst }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No detector data yet.</div>;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 600, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>#</th>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Detector</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Win Rate</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Resolved</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Correct</th>
            <th style={{ padding: "8px 14px", minWidth: 120 }}></th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => {
            const wr = row.win_rate;
            const color = winColor(wr);
            const badge = row.signal_type === best ? "BEST" : row.signal_type === worst ? "WORST" : null;
            const barWidth = wr != null ? `${(wr * 100).toFixed(1)}%` : "0%";
            return (
              <tr key={row.signal_type} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={{ padding: "10px 14px", color: "var(--text-dim)", fontFamily: "var(--mono)" }}>{i + 1}</td>
                <td style={{ padding: "10px 14px", fontWeight: 600, textTransform: "uppercase", fontSize: 11, color: "var(--accent)" }}>
                  {row.signal_type.replace(/_/g, " ")}
                  {badge && (
                    <span style={{
                      marginLeft: 8, fontSize: 9, padding: "1px 5px", borderRadius: 3,
                      background: badge === "BEST" ? "var(--green)" : "var(--red)", color: "#fff", verticalAlign: "middle",
                    }}>
                      {badge}
                    </span>
                  )}
                </td>
                <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", fontWeight: 700, color }}>
                  {fmtPct(wr)}
                </td>
                <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: "var(--text-dim)" }}>
                  {row.resolved}
                </td>
                <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: "var(--text-dim)" }}>
                  {row.correct}
                </td>
                <td style={{ padding: "10px 14px" }}>
                  <div style={{ background: "var(--border)", borderRadius: 3, height: 6, overflow: "hidden" }}>
                    <div style={{ width: barWidth, background: color, height: "100%", borderRadius: 3 }} />
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Rank score distribution (winners vs losers) ─────────────────────────────
function RankDistribution({ data }) {
  if (!data) return null;
  const { avg_rank_of_winners, avg_rank_of_losers, threshold_curve } = data;

  if (!threshold_curve || threshold_curve.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>Not enough data yet.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Avg Rank — Winners</div>
          <div style={{ fontSize: 24, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--green)" }}>
            {avg_rank_of_winners != null ? avg_rank_of_winners.toFixed(3) : "—"}
          </div>
        </div>
        <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Avg Rank — Losers</div>
          <div style={{ fontSize: 24, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--red)" }}>
            {avg_rank_of_losers != null ? avg_rank_of_losers.toFixed(3) : "—"}
          </div>
        </div>
      </div>
      <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "16px 8px 8px" }}>
        <div style={{ fontSize: 12, fontWeight: 600, paddingLeft: 8, marginBottom: 8 }}>Win Rate by Min Rank Threshold</div>
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={threshold_curve} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="threshold" tick={{ fontSize: 11, fill: "var(--text-dim)" }} tickFormatter={(v) => v.toFixed(2)} />
            <YAxis domain={[0, 1]} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 11, fill: "var(--text-dim)" }} width={40} />
            <Tooltip
              formatter={(v, name) => [name === "win_rate" ? fmtPct(v) : v, name === "win_rate" ? "Win Rate" : "Signals"]}
              labelFormatter={(l) => `Threshold ≥ ${l}`}
              contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", fontSize: 12 }}
            />
            {data.optimal_threshold != null && (
              <ReferenceLine x={data.optimal_threshold} stroke="var(--yellow)" strokeDasharray="4 4" label={{ value: "sweet spot", fontSize: 10, fill: "var(--yellow)" }} />
            )}
            <Bar dataKey="win_rate" name="win_rate" radius={[3, 3, 0, 0]}>
              {threshold_curve.map((entry, i) => (
                <Cell
                  key={i}
                  fill={entry.threshold === data.optimal_threshold ? "var(--yellow)" : "var(--accent)"}
                  fillOpacity={0.8}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        {data.optimal_threshold != null && (
          <div style={{ fontSize: 11, color: "var(--yellow)", textAlign: "center", marginTop: 4 }}>
            Sweet spot: rank ≥ {data.optimal_threshold} (highlighted)
          </div>
        )}
      </div>
    </div>
  );
}

// ── Recent calls list ───────────────────────────────────────────────────────
function RecentCalls({ calls }) {
  if (!calls || calls.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No resolved signals yet.</div>;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 500, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Type</th>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Fired</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Rank</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Result</th>
          </tr>
        </thead>
        <tbody>
          {calls.map((c) => (
            <tr key={c.id} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "8px 14px" }}>
                <Link to={`/signals/${c.id}`} style={{ fontWeight: 600, textTransform: "uppercase", fontSize: 11, color: "var(--accent)" }}>
                  {c.signal_type.replace(/_/g, " ")}
                </Link>
              </td>
              <td style={{ padding: "8px 14px", color: "var(--text-dim)", fontSize: 12 }}>
                {new Date(c.fired_at).toLocaleString()}
              </td>
              <td style={{ textAlign: "right", padding: "8px 14px", fontFamily: "var(--mono)", fontSize: 12 }}>
                {c.rank_score.toFixed(3)}
              </td>
              <td style={{ textAlign: "right", padding: "8px 14px" }}>
                <span style={{
                  fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 4,
                  background: c.resolved_correctly ? "var(--green)" : "var(--red)",
                  color: "#fff",
                }}>
                  {c.resolved_correctly ? "WIN" : "LOSS"}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Main page ───────────────────────────────────────────────────────────────
export default function Performance() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    getPerformanceSummary()
      .then(setData)
      .catch((e) => setError(e.message));
  }, []);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;

  const overallColor = data ? winColor(data.overall_win_rate) : "var(--text)";

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 20 }}>
        <h2 style={{ fontSize: 16, fontWeight: 600 }}>Performance Dashboard</h2>
        {data && (
          <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
            Last {data.lookback_days} days
          </div>
        )}
      </div>

      {/* Hero metrics */}
      {data ? (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 32 }}>
          <HeroCard
            label="Win Rate"
            value={fmtPct(data.overall_win_rate)}
            color={overallColor}
            sub={`${data.total_resolved} resolved signals`}
          />
          <HeroCard
            label="Signals Fired"
            value={data.total_signals_fired}
            sub="in lookback window"
          />
          <HeroCard
            label="Markets Resolved"
            value={data.total_markets_resolved}
            sub={`${data.signals_pending_resolution} pending`}
          />
          <HeroCard
            label="Best Detector"
            value={data.best_detector ? data.best_detector.replace(/_/g, " ").toUpperCase() : "—"}
            color="var(--green)"
            sub="by win rate (min 10 resolved)"
          />
          {data.optimal_threshold != null && (
            <HeroCard
              label="Optimal Threshold"
              value={`≥ ${data.optimal_threshold}`}
              color="var(--yellow)"
              sub="rank_score sweet spot"
            />
          )}
        </div>
      ) : (
        <div className="skeleton" style={{ height: 96, borderRadius: 8, marginBottom: 32 }} />
      )}

      <Section title="Win Rate Trend (30 days)">
        {data ? <TrendChart data={data.win_rate_trend} /> : <div className="skeleton" style={{ height: 220, borderRadius: 8 }} />}
      </Section>

      <Section title="Detector Leaderboard">
        {data ? (
          <DetectorLeaderboard
            data={data.win_rate_by_type}
            best={data.best_detector}
            worst={data.worst_detector}
          />
        ) : (
          <div className="skeleton" style={{ height: 160, borderRadius: 8 }} />
        )}
      </Section>

      <Section title="Rank Score Analysis">
        {data ? <RankDistribution data={data} /> : <div className="skeleton" style={{ height: 200, borderRadius: 8 }} />}
      </Section>

      <Section title="Recent Calls">
        {data ? <RecentCalls calls={data.recent_calls} /> : <div className="skeleton" style={{ height: 200, borderRadius: 8 }} />}
      </Section>
    </div>
  );
}
