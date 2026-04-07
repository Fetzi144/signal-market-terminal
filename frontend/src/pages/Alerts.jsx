import { useEffect, useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { getRecentAlerts, getSignalTypes, getMarketPlatforms } from "../api";

function formatTypeLabel(t) {
  return t.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

const PLATFORM_COLORS = { polymarket: "#6366f1", kalshi: "#f59e0b" };
const PAGE_SIZE = 50;

function PlatformBadge({ platform }) {
  if (!platform) return null;
  const color = PLATFORM_COLORS[platform] || "var(--text-dim)";
  return (
    <span
      style={{
        fontSize: 10, fontWeight: 700, textTransform: "uppercase",
        letterSpacing: 0.5, color: "#fff", background: color,
        padding: "1px 6px", borderRadius: 4,
      }}
    >
      {platform === "polymarket" ? "PM" : "KA"}
    </span>
  );
}

export default function Alerts() {
  const [data, setData] = useState(null);
  const [typeFilter, setTypeFilter] = useState("");
  const [platformFilter, setPlatformFilter] = useState("");
  const [page, setPage] = useState(1);
  const [error, setError] = useState(null);
  const [typeOptions, setTypeOptions] = useState([{ value: "", label: "All Types" }]);
  const [platformOptions, setPlatformOptions] = useState([{ value: "", label: "All Platforms" }]);

  useEffect(() => {
    getSignalTypes()
      .then((d) => {
        setTypeOptions([
          { value: "", label: "All Types" },
          ...d.types.map((t) => ({ value: t, label: formatTypeLabel(t) })),
        ]);
      })
      .catch(() => {});
    getMarketPlatforms()
      .then((d) => {
        setPlatformOptions([
          { value: "", label: "All Platforms" },
          ...d.platforms.map((p) => ({ value: p, label: p.charAt(0).toUpperCase() + p.slice(1) })),
        ]);
      })
      .catch(() => {});
  }, []);

  const fetchData = useCallback(() => {
    const params = { page, pageSize: PAGE_SIZE };
    if (typeFilter) params.signalType = typeFilter;
    if (platformFilter) params.platform = platformFilter;
    getRecentAlerts(params)
      .then((d) => { setData(d); setError(null); })
      .catch((e) => setError(e.message));
  }, [typeFilter, platformFilter, page]);

  useEffect(() => { fetchData(); }, [fetchData]);
  useEffect(() => { setPage(1); }, [typeFilter, platformFilter]);

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;

  const selectStyle = {
    padding: "6px 12px", fontSize: 13, background: "var(--bg-card)",
    color: "var(--text)", border: "1px solid var(--border)",
    borderRadius: 6, cursor: "pointer",
  };

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)} style={selectStyle}>
          {typeOptions.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        <select value={platformFilter} onChange={(e) => setPlatformFilter(e.target.value)} style={selectStyle}>
          {platformOptions.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
      </div>

      {error && <div style={{ color: "var(--red)", marginBottom: 12 }}>Error: {error}</div>}

      {data && (
        <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 12 }}>
          {data.total} alert{data.total !== 1 ? "s" : ""}
        </div>
      )}

      {!data && !error && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {[1,2,3].map(i => <div key={i} className="skeleton" style={{ borderRadius: 8, height: 60 }} />)}
        </div>
      )}

      {data && data.alerts.length === 0 && (
        <div style={{ color: "var(--text-dim)", padding: 40, textAlign: "center" }}>
          No alerts yet. Alerts appear when high-ranking signals are detected.
        </div>
      )}

      {data && data.alerts.length > 0 && (
        <div className="table-scroll" style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", minWidth: 600, borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--border)" }}>
              <th style={{ textAlign: "left", padding: 8, color: "var(--text-dim)" }}>Type</th>
              <th style={{ textAlign: "left", padding: 8, color: "var(--text-dim)" }}>Market</th>
              <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Rank</th>
              <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Score</th>
              <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>Fired</th>
            </tr>
          </thead>
          <tbody>
            {data.alerts.map((a) => {
              const typeLabel = formatTypeLabel(a.signal_type);
              const rank = Math.round(a.rank_score * 100);
              const score = Math.round(a.signal_score * 100);
              const time = a.fired_at ? new Date(a.fired_at).toLocaleString() : "";
              const rankColor = rank >= 70 ? "var(--green)" : rank >= 40 ? "var(--yellow)" : "var(--text-dim)";
              return (
                <tr key={a.id} style={{ borderBottom: "1px solid var(--border)" }}>
                  <td style={{ padding: 8 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <PlatformBadge platform={a.platform} />
                      <span style={{ fontWeight: 600, color: "var(--accent)", textTransform: "uppercase", fontSize: 11 }}>
                        {typeLabel}
                      </span>
                    </div>
                  </td>
                  <td style={{ padding: 8, maxWidth: 350 }}>
                    <Link to={`/signals/${a.id}`} style={{ fontSize: 13 }}>
                      {(a.market_question || "Unknown market").slice(0, 80)}
                    </Link>
                  </td>
                  <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)", color: rankColor }}>
                    {rank}%
                  </td>
                  <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)" }}>
                    {score}%
                  </td>
                  <td style={{ textAlign: "right", padding: 8, color: "var(--text-dim)", fontSize: 12 }}>
                    {time}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        </div>
      )}

      {data && totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 16, marginTop: 20 }}>
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            style={{
              padding: "6px 14px", fontSize: 13, background: "transparent",
              color: page <= 1 ? "var(--border)" : "var(--text)",
              border: "1px solid var(--border)", borderRadius: 6,
              cursor: page <= 1 ? "default" : "pointer",
            }}
          >
            Previous
          </button>
          <span style={{ fontSize: 13, color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
            {page} / {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            style={{
              padding: "6px 14px", fontSize: 13, background: "transparent",
              color: page >= totalPages ? "var(--border)" : "var(--text)",
              border: "1px solid var(--border)", borderRadius: 6,
              cursor: page >= totalPages ? "default" : "pointer",
            }}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
