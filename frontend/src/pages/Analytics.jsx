import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getPlatformSummary, getSignalAccuracy, getCorrelatedSignals } from "../api";

const PLATFORM_COLORS = { polymarket: "#6366f1", kalshi: "#f59e0b" };

function PlatformCard({ data }) {
  const color = PLATFORM_COLORS[data.platform] || "var(--text-dim)";
  return (
    <div style={{
      background: "var(--bg-card)", border: "1px solid var(--border)",
      borderRadius: 8, padding: 18, borderTop: `3px solid ${color}`,
    }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", color, marginBottom: 12 }}>
        {data.platform}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
        <Stat label="Markets" value={data.active_markets} />
        <Stat label="Signals" value={data.total_signals} />
        <Stat label="Avg Rank" value={`${Math.round(data.avg_rank_score * 100)}%`} />
      </div>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div>
      <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{label}</div>
      <div style={{ fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600 }}>{value}</div>
    </div>
  );
}

function AccuracyTable({ data }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No evaluation data yet.</div>;
  }

  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
      <thead>
        <tr style={{ borderBottom: "1px solid var(--border)" }}>
          <th style={{ textAlign: "left", padding: 8, color: "var(--text-dim)" }}>Signal Type</th>
          <th style={{ textAlign: "center", padding: 8, color: "var(--text-dim)" }}>Horizon</th>
          <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Evaluations</th>
          <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Accuracy</th>
          <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Avg |Change|</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => {
          const accColor = row.accuracy_pct >= 60 ? "var(--green)"
            : row.accuracy_pct >= 40 ? "var(--yellow)" : "var(--red)";
          return (
            <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: 8, fontWeight: 600, textTransform: "uppercase", fontSize: 11, color: "var(--accent)" }}>
                {row.signal_type.replace("_", " ")}
              </td>
              <td style={{ textAlign: "center", padding: 8, fontFamily: "var(--mono)" }}>{row.horizon}</td>
              <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)" }}>{row.total_evaluations}</td>
              <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)", color: accColor, fontWeight: 600 }}>
                {row.accuracy_pct}%
              </td>
              <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)" }}>{row.avg_abs_change_pct}%</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function CorrelatedList({ data }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No cross-platform correlations found.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {data.map((group, i) => (
        <div key={i} style={{
          background: "var(--bg-card)", border: "1px solid var(--border)",
          borderRadius: 8, padding: 14,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
            <span style={{ fontWeight: 600, fontSize: 14 }}>{group.category}</span>
            <div style={{ display: "flex", gap: 6 }}>
              {group.platforms.map((p) => (
                <span key={p} style={{
                  fontSize: 10, fontWeight: 700, textTransform: "uppercase",
                  color: "#fff", background: PLATFORM_COLORS[p] || "var(--text-dim)",
                  padding: "1px 6px", borderRadius: 4,
                }}>
                  {p === "polymarket" ? "PM" : "KA"}
                </span>
              ))}
            </div>
          </div>
          <div style={{ fontSize: 12, color: "var(--text-dim)", marginBottom: 6 }}>
            {group.signal_count} signals across {group.platforms.length} platforms
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {group.signals.slice(0, 5).map((s) => (
              <Link key={s.signal_id} to={`/signals/${s.signal_id}`} style={{ fontSize: 12 }}>
                [{s.platform === "polymarket" ? "PM" : "KA"}] {s.signal_type.replace("_", " ")} — {s.market_question?.slice(0, 60)}
              </Link>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function Analytics() {
  const [platformData, setPlatformData] = useState(null);
  const [accuracyData, setAccuracyData] = useState(null);
  const [correlatedData, setCorrelatedData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    Promise.all([
      getPlatformSummary(),
      getSignalAccuracy(),
      getCorrelatedSignals(),
    ])
      .then(([ps, acc, corr]) => {
        setPlatformData(ps.platforms);
        setAccuracyData(acc.accuracy);
        setCorrelatedData(corr.correlated);
      })
      .catch((e) => setError(e.message));
  }, []);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;

  return (
    <div>
      <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 16 }}>Platform Overview</h2>
      {platformData ? (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 12, marginBottom: 32 }}>
          {platformData.map((p) => <PlatformCard key={p.platform} data={p} />)}
        </div>
      ) : (
        <div className="skeleton" style={{ height: 100, borderRadius: 8, marginBottom: 32 }} />
      )}

      <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 12 }}>Signal Accuracy by Horizon</h2>
      <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 4, marginBottom: 32 }}>
        {accuracyData ? <AccuracyTable data={accuracyData} /> : (
          <div className="skeleton" style={{ height: 120, borderRadius: 8 }} />
        )}
      </div>

      <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 12 }}>Cross-Platform Correlations</h2>
      {correlatedData ? <CorrelatedList data={correlatedData} /> : (
        <div className="skeleton" style={{ height: 100, borderRadius: 8 }} />
      )}
    </div>
  );
}
