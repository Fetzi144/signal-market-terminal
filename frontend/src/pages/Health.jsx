import { useCallback, useEffect, useRef, useState } from "react";
import { getHealth } from "../api";
import PushNotificationToggle from "../components/PushNotificationToggle";

const REFRESH_INTERVAL = 15_000;

export default function Health() {
  const [health, setHealth] = useState(null);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const intervalRef = useRef(null);

  const fetchData = useCallback(async () => {
    try {
      const payload = await getHealth();
      setHealth(payload);
      setLastUpdated(new Date());
      setError(null);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    fetchData();
    intervalRef.current = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(intervalRef.current);
  }, [fetchData]);

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

  if (error && !health) {
    return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  }

  const schedulerLease = health?.scheduler_lease || null;
  const defaultStrategyRuntime = health?.default_strategy_runtime || null;
  const runtimeInvariants = health?.runtime_invariants || [];
  const strategyFamilies = (health?.strategy_families || []).filter(
    (family) => !["structure", "maker"].includes(family.family)
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <h2 style={{ fontSize: 16 }}>System Health</h2>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <button onClick={fetchData} style={secondaryButtonStyle}>Refresh</button>
          {lastUpdated && (
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Auto-refresh 15s | Updated {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>

      {error && <InlineAlert tone="error">{error}</InlineAlert>}

      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
          <div style={{ fontSize: 14, fontWeight: 600 }}>Exchange Scope</div>
          <span style={statusPillStyle("passing")}>Kalshi Only</span>
        </div>
        <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
          Polymarket scanners, operator controls, and live/pilot routes are retired in this deployment.
        </div>
      </section>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12 }}>
        <StatCard label="Status" value={health?.status || "unknown"} />
        <StatCard label="Active Markets" value={health?.active_markets ?? "-"} />
        <StatCard label="Total Signals" value={health?.total_signals ?? "-"} />
        <StatCard label="Unresolved" value={health?.unresolved_signals ?? "-"} />
        <StatCard label="Alerts (24h)" value={health?.recent_alerts_24h ?? "-"} />
        <StatCard
          label="Alert Threshold"
          value={health?.alert_threshold != null ? `${Math.round(health.alert_threshold * 100)}%` : "-"}
        />
      </div>

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 }}>
        <div style={panelStyle}>
          <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Benchmark Runtime</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 12 }}>
            <StatCard label="Lease Owner" value={shortId(schedulerLease?.owner_token)} />
            <StatCard label="Lease Heartbeat" value={formatFreshness(schedulerLease?.heartbeat_freshness_seconds)} />
            <StatCard label="Lease Expiry" value={formatFreshness(schedulerLease?.expires_in_seconds)} />
            <StatCard label="Overdue Trades" value={defaultStrategyRuntime?.overdue_open_trades ?? 0} />
            <StatCard label="Last Backfill" value={formatShortDateTime(defaultStrategyRuntime?.last_resolution_backfill_at)} />
            <StatCard label="Backfill Count" value={defaultStrategyRuntime?.last_resolution_backfill_count ?? 0} />
            <StatCard label="Clamp Count (24h)" value={defaultStrategyRuntime?.evaluation_clamp_count_24h ?? 0} />
            <StatCard label="Last Eval Failure" value={formatShortDateTime(defaultStrategyRuntime?.last_evaluation_failure_at)} />
          </div>
        </div>

        <div style={panelStyle}>
          <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Unattended Invariants</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {runtimeInvariants.length === 0 ? (
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>No runtime invariants reported yet.</div>
            ) : (
              runtimeInvariants.map((invariant) => (
                <div key={invariant.key} style={smallPanelStyle}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 6 }}>
                    <div style={{ fontSize: 12, fontWeight: 600 }}>{invariant.label}</div>
                    <span style={statusPillStyle(invariant.status)}>{formatStatus(invariant.status)}</span>
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>{invariant.detail}</div>
                </div>
              ))
            )}
          </div>
        </div>
      </section>

      <section>
        <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Notifications</h3>
        <PushNotificationToggle />
      </section>

      <section style={panelStyle}>
        <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Strategy Families</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
          {strategyFamilies.length === 0 ? (
            <div style={{ fontSize: 12, color: "var(--text-dim)" }}>No strategy families reported yet.</div>
          ) : (
            strategyFamilies.map((family) => (
              <div key={family.family} style={smallPanelStyle}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8, marginBottom: 8 }}>
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 600 }}>{family.label}</div>
                    <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{family.primary_surface}</div>
                  </div>
                  <span style={statusPillStyle(family.posture)}>{formatStatus(family.posture)}</span>
                </div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>{family.description}</div>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div style={statCardStyle}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, marginTop: 6 }}>{value}</div>
    </div>
  );
}

function InlineAlert({ children, tone = "warning" }) {
  const color = tone === "error" ? "var(--red)" : "var(--yellow)";
  return (
    <div style={{ border: `1px solid ${color}`, color, borderRadius: 8, padding: "10px 12px", fontSize: 13 }}>
      {children}
    </div>
  );
}

function formatFreshness(value) {
  if (value == null) return "-";
  if (value < 60) return `${Math.round(value)}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  return `${Math.round(value / 3600)}h`;
}

function formatShortDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function shortId(value) {
  if (!value) return "-";
  const text = String(value);
  return text.length > 18 ? `${text.slice(0, 18)}...` : text;
}

function formatStatus(value) {
  return String(value || "unknown").replaceAll("_", " ").toUpperCase();
}

function statusPillStyle(status) {
  const normalized = String(status || "").toLowerCase();
  const color = normalized.includes("fail") || normalized.includes("error")
    ? "var(--red)"
    : normalized.includes("warn") || normalized.includes("degraded")
      ? "var(--yellow)"
      : "var(--green)";
  return {
    color,
    border: `1px solid ${color}`,
    borderRadius: 4,
    padding: "2px 6px",
    fontSize: 10,
    fontWeight: 700,
  };
}

const panelStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: 16,
};

const smallPanelStyle = {
  background: "rgba(255, 255, 255, 0.02)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: 12,
};

const statCardStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: 14,
};

const secondaryButtonStyle = {
  background: "transparent",
  border: "1px solid var(--border)",
  color: "var(--text)",
  borderRadius: 6,
  padding: "8px 12px",
  fontSize: 12,
  fontWeight: 600,
  cursor: "pointer",
};
