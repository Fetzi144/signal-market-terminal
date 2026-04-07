import { useEffect, useState, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { getSignals, exportSignalsCsv, getSignalTypes, getMarketPlatforms, getSignalTimeframes } from "../api";
import SignalEvaluationBar from "../components/SignalEvaluationBar";
import useSSE from "../hooks/useSSE";

const DEFAULT_TYPE_OPTIONS = [
  { value: "", label: "All Types" },
];

const DEFAULT_PLATFORM_OPTIONS = [
  { value: "", label: "All Platforms" },
];

function formatTypeLabel(t) {
  return t.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

const REFRESH_INTERVAL = 120_000;  // Extended since SSE provides real-time updates
const PAGE_SIZE = 50;

function ScoreBadge({ value, label }) {
  const pct = Math.round(value * 100);
  const color = pct >= 70 ? "var(--green)" : pct >= 40 ? "var(--yellow)" : "var(--text-dim)";
  return (
    <span style={{ color, fontFamily: "var(--mono)", fontSize: 13 }}>
      {label}: {pct}%
    </span>
  );
}

function DirectionBadge({ direction }) {
  if (!direction) return null;
  const isUp = direction === "up";
  return (
    <span style={{ color: isUp ? "var(--green)" : "var(--red)", fontWeight: 600, fontSize: 13 }}>
      {isUp ? "\u2191" : "\u2193"}
    </span>
  );
}

const TIMEFRAME_COLORS = {
  "5m": "#94a3b8", "15m": "#94a3b8", "30m": "#60a5fa",
  "1h": "#a78bfa", "4h": "#f59e0b", "24h": "#ef4444",
};

function TimeframeBadge({ timeframe }) {
  if (!timeframe) return null;
  const color = TIMEFRAME_COLORS[timeframe] || "var(--text-dim)";
  return (
    <span
      style={{
        fontSize: 10, fontWeight: 700, fontFamily: "var(--mono)",
        color: "#fff", background: color,
        padding: "1px 6px", borderRadius: 4,
      }}
    >
      {timeframe}
    </span>
  );
}

function ConfluenceBadge({ details }) {
  const tfs = details?.confluence_timeframes;
  if (!tfs || tfs.length < 2) return null;
  return (
    <span
      style={{
        fontSize: 11, fontWeight: 600, color: "var(--green)",
        background: "rgba(34,197,94,0.12)", padding: "2px 8px",
        borderRadius: 4, whiteSpace: "nowrap",
      }}
    >
      Confirmed: {tfs.join(" + ")}
    </span>
  );
}

const PLATFORM_COLORS = { polymarket: "#6366f1", kalshi: "#f59e0b" };

function PlatformBadge({ platform }) {
  if (!platform) return null;
  const color = PLATFORM_COLORS[platform] || "var(--text-dim)";
  return (
    <span
      style={{
        fontSize: 10,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: 0.5,
        color: "#fff",
        background: color,
        padding: "1px 6px",
        borderRadius: 4,
      }}
    >
      {platform === "polymarket" ? "PM" : "KA"}
    </span>
  );
}

function ResolutionBadge({ resolved_correctly }) {
  if (resolved_correctly === true) {
    return (
      <span style={{ fontSize: 12, fontWeight: 600, color: "var(--green)", display: "flex", alignItems: "center", gap: 3 }}>
        &#10003; Called it
      </span>
    );
  }
  if (resolved_correctly === false) {
    return (
      <span style={{ fontSize: 12, fontWeight: 600, color: "var(--red)", display: "flex", alignItems: "center", gap: 3 }}>
        &#10007; Wrong call
      </span>
    );
  }
  return (
    <span style={{ fontSize: 12, color: "var(--text-dim)", display: "flex", alignItems: "center", gap: 3 }}>
      &#8226; Pending
    </span>
  );
}

function SkeletonCard() {
  return (
    <div className="skeleton" style={{ borderRadius: 8, height: 100, marginBottom: 8 }} />
  );
}

function SignalCard({ signal }) {
  const s = signal;
  const d = s.details || {};
  const time = new Date(s.fired_at).toLocaleString();
  const typeLabel = formatTypeLabel(s.signal_type);

  // Build detail snippet based on type
  let snippet = null;
  if (d.change_pct) snippet = <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{d.change_pct}%</span>;
  if (d.multiplier) snippet = <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{d.multiplier}x vol</span>;
  if (d.ratio) snippet = <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{d.ratio}x spread</span>;
  if (d.vacuum_side) snippet = <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{d.vacuum_side} side</span>;
  if (d.hours_until_deadline != null) snippet = <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>{d.hours_until_deadline}h left</span>;

  return (
    <Link to={`/signals/${s.id}`} style={{ textDecoration: "none", color: "inherit" }}>
      <div
        style={{
          background: "var(--bg-card)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: "14px 18px",
          cursor: "pointer",
          transition: "background 0.15s",
        }}
        onMouseEnter={(e) => (e.currentTarget.style.background = "var(--bg-hover)")}
        onMouseLeave={(e) => (e.currentTarget.style.background = "var(--bg-card)")}
      >
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <PlatformBadge platform={s.platform} />
            <TimeframeBadge timeframe={s.timeframe} />
            <span style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--accent)" }}>
              {typeLabel}
            </span>
            <DirectionBadge direction={d.direction} />
            {snippet}
            <ConfluenceBadge details={d} />
          </div>
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>{time}</span>
        </div>
        <div style={{ fontSize: 14, marginBottom: 8, lineHeight: 1.4 }}>
          {s.market_question || "Unknown market"}
          {d.outcome_name && <span style={{ color: "var(--text-dim)" }}> &middot; {d.outcome_name}</span>}
        </div>
        <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
          <ScoreBadge value={parseFloat(s.signal_score)} label="Str" />
          <ScoreBadge value={parseFloat(s.confidence)} label="Conf" />
          <ScoreBadge value={parseFloat(s.rank_score)} label="Rank" />
          <ResolutionBadge resolved_correctly={s.resolved_correctly} />
          {s.evaluations && s.evaluations.length > 0 && (
            <div style={{ marginLeft: "auto" }}>
              <SignalEvaluationBar evaluations={s.evaluations} />
            </div>
          )}
        </div>
      </div>
    </Link>
  );
}

export default function SignalFeed() {
  const [data, setData] = useState(null);
  const [filter, setFilter] = useState("");
  const [platformFilter, setPlatformFilter] = useState("");
  const [timeframeFilter, setTimeframeFilter] = useState("");
  const [resolvedFilter, setResolvedFilter] = useState("");
  const [page, setPage] = useState(1);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [paused, setPaused] = useState(false);
  const [typeOptions, setTypeOptions] = useState(DEFAULT_TYPE_OPTIONS);
  const [platformOptions, setPlatformOptions] = useState(DEFAULT_PLATFORM_OPTIONS);
  const [timeframeOptions, setTimeframeOptions] = useState([{ value: "", label: "All Timeframes" }]);
  const intervalRef = useRef(null);
  const { connected, addEventListener } = useSSE();

  // Fetch dynamic filter options on mount
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
    getSignalTimeframes()
      .then((d) => {
        setTimeframeOptions([
          { value: "", label: "All Timeframes" },
          ...d.timeframes.map((t) => ({ value: t, label: t })),
        ]);
      })
      .catch(() => {});
  }, []);

  const fetchData = useCallback(() => {
    const params = { page, pageSize: PAGE_SIZE };
    if (filter) params.signalType = filter;
    if (platformFilter) params.platform = platformFilter;
    if (timeframeFilter) params.timeframe = timeframeFilter;
    if (resolvedFilter !== "") params.resolvedCorrectly = resolvedFilter;
    getSignals(params)
      .then((d) => {
        setData(d);
        setLastUpdated(new Date());
        setError(null);
      })
      .catch((e) => setError(e.message));
  }, [filter, platformFilter, timeframeFilter, resolvedFilter, page]);

  // Auto-fetch when SSE reports new signals
  useEffect(() => {
    const unsub = addEventListener("new_signal", () => {
      if (page === 1 && !paused) {
        setTimeout(() => fetchData(), 500);
      }
    });
    return unsub;
  }, [addEventListener, fetchData, page, paused]);

  // Fetch on filter/page change
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Auto-refresh
  useEffect(() => {
    if (paused) {
      clearInterval(intervalRef.current);
      return;
    }
    intervalRef.current = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(intervalRef.current);
  }, [fetchData, paused]);

  // Reset page on filter change
  useEffect(() => {
    setPage(1);
  }, [filter, platformFilter, timeframeFilter, resolvedFilter]);

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;

  return (
    <div>
      {/* Controls row */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <select
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            padding: "6px 12px",
            fontSize: 13,
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          {typeOptions.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>

        <select
          value={platformFilter}
          onChange={(e) => setPlatformFilter(e.target.value)}
          style={{
            padding: "6px 12px",
            fontSize: 13,
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          {platformOptions.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>

        <select
          value={timeframeFilter}
          onChange={(e) => setTimeframeFilter(e.target.value)}
          style={{
            padding: "6px 12px",
            fontSize: 13,
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          {timeframeOptions.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>

        <select
          value={resolvedFilter}
          onChange={(e) => setResolvedFilter(e.target.value)}
          style={{
            padding: "6px 12px",
            fontSize: 13,
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          <option value="">All Resolutions</option>
          <option value="true">Correct Calls Only</option>
          <option value="false">Wrong Calls Only</option>
        </select>

        <button
          onClick={() => exportSignalsCsv({ signalType: filter || undefined })}
          style={{
            padding: "6px 12px",
            fontSize: 12,
            background: "transparent",
            color: "var(--text-dim)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          Export CSV
        </button>

        <button
          onClick={() => setPaused((p) => !p)}
          style={{
            padding: "6px 12px",
            fontSize: 12,
            background: "transparent",
            color: paused ? "var(--yellow)" : "var(--text-dim)",
            border: `1px solid ${paused ? "var(--yellow)" : "var(--border)"}`,
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          {paused ? "Resume" : "Pause"} auto-refresh
        </button>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          {connected && (
            <span style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, color: "var(--green)" }}>
              <span style={{
                width: 6, height: 6, borderRadius: "50%", background: "var(--green)",
                display: "inline-block", animation: "pulse 2s infinite",
              }} />
              Live
            </span>
          )}
          {lastUpdated && (
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>

      {error && <div style={{ color: "var(--red)", marginBottom: 12 }}>Error: {error}</div>}

      {/* Signal count */}
      {data && (
        <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 12 }}>
          {data.total} signal{data.total !== 1 ? "s" : ""}
        </div>
      )}

      {/* Loading skeleton */}
      {!data && !error && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <SkeletonCard /><SkeletonCard /><SkeletonCard /><SkeletonCard />
        </div>
      )}

      {/* Empty state */}
      {data && data.signals.length === 0 && (
        <div style={{ color: "var(--text-dim)", padding: 40, textAlign: "center" }}>
          No signals yet. Waiting for data ingestion and detection...
        </div>
      )}

      {/* Signal cards */}
      {data && data.signals.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {data.signals.map((s) => (
            <SignalCard key={s.id} signal={s} />
          ))}
        </div>
      )}

      {/* Pagination */}
      {data && totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 16, marginTop: 20 }}>
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            style={{
              padding: "6px 14px",
              fontSize: 13,
              background: "transparent",
              color: page <= 1 ? "var(--border)" : "var(--text)",
              border: "1px solid var(--border)",
              borderRadius: 6,
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
              padding: "6px 14px",
              fontSize: 13,
              background: "transparent",
              color: page >= totalPages ? "var(--border)" : "var(--text)",
              border: "1px solid var(--border)",
              borderRadius: 6,
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
