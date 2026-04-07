import { useEffect, useState, useRef } from "react";
import { getHealth } from "../api";

const REFRESH_INTERVAL = 15_000;
const STALE_THRESHOLD_MS = 10 * 60 * 1000; // 10 minutes

export default function Health() {
  const [health, setHealth] = useState(null);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const intervalRef = useRef(null);

  const fetchData = () => {
    getHealth()
      .then((h) => {
        setHealth(h);
        setLastUpdated(new Date());
        setError(null);
      })
      .catch((e) => setError(e.message));
  };

  useEffect(() => {
    fetchData();
    intervalRef.current = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(intervalRef.current);
  }, []);

  if (!health && !error) {
    return (
      <div>
        <h2 style={{ fontSize: 16, marginBottom: 16 }}>System Health</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
        </div>
      </div>
    );
  }

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h2 style={{ fontSize: 16 }}>System Health</h2>
        {lastUpdated && (
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
            Auto-refresh 15s &middot; Updated {lastUpdated.toLocaleTimeString()}
          </span>
        )}
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 12,
          marginBottom: 24,
        }}
      >
        <StatCard label="Status" value={health.status} />
        <StatCard label="Active Markets" value={health.active_markets} />
        <StatCard label="Total Signals" value={health.total_signals} />
        <StatCard label="Unresolved" value={health.unresolved_signals} />
        <StatCard label="Alerts (24h)" value={health.recent_alerts_24h} />
        <StatCard label="Alert Threshold" value={`${Math.round(health.alert_threshold * 100)}%`} />
      </div>

      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Ingestion</h3>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {health.ingestion.map((ing) => {
          const isStale =
            ing.last_run && Date.now() - new Date(ing.last_run).getTime() > STALE_THRESHOLD_MS;

          return (
            <div
              key={ing.run_type}
              style={{
                background: "var(--bg-card)",
                border: `1px solid ${isStale ? "var(--yellow)" : "var(--border)"}`,
                borderRadius: 8,
                padding: "12px 16px",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                fontSize: 13,
              }}
            >
              <span style={{ fontWeight: 500, minWidth: 140 }}>{ing.run_type}</span>
              <span
                style={{
                  color:
                    ing.last_status === "success"
                      ? "var(--green)"
                      : ing.last_status === "failed"
                      ? "var(--red)"
                      : "var(--text-dim)",
                }}
              >
                {ing.last_status || "never run"}
              </span>
              <span style={{ color: isStale ? "var(--yellow)" : "var(--text-dim)" }}>
                {ing.last_run ? new Date(ing.last_run).toLocaleString() : "\u2014"}
                {isStale && " (stale)"}
              </span>
              <span style={{ fontFamily: "var(--mono)" }}>
                {ing.markets_processed != null ? `${ing.markets_processed} mkts` : "\u2014"}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: "12px 16px",
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 20, fontFamily: "var(--mono)", fontWeight: 600 }}>{value}</div>
    </div>
  );
}
