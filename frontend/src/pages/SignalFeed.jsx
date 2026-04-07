import { useEffect, useState, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { getSignals } from "../api";

const TYPE_OPTIONS = [
  { value: "", label: "All Types" },
  { value: "price_move", label: "Price Move" },
  { value: "volume_spike", label: "Volume Spike" },
  { value: "spread_change", label: "Spread Change" },
  { value: "liquidity_vacuum", label: "Liquidity Vacuum" },
  { value: "deadline_near", label: "Deadline Near" },
];

const TYPE_LABELS = Object.fromEntries(
  TYPE_OPTIONS.filter((o) => o.value).map((o) => [o.value, o.label])
);

const REFRESH_INTERVAL = 30_000;
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

function SkeletonCard() {
  return (
    <div className="skeleton" style={{ borderRadius: 8, height: 100, marginBottom: 8 }} />
  );
}

function SignalCard({ signal }) {
  const s = signal;
  const d = s.details || {};
  const time = new Date(s.fired_at).toLocaleString();
  const typeLabel = TYPE_LABELS[s.signal_type] || s.signal_type;

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
            <span style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--accent)" }}>
              {typeLabel}
            </span>
            <DirectionBadge direction={d.direction} />
            {snippet}
          </div>
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>{time}</span>
        </div>
        <div style={{ fontSize: 14, marginBottom: 8, lineHeight: 1.4 }}>
          {s.market_question || "Unknown market"}
          {d.outcome_name && <span style={{ color: "var(--text-dim)" }}> &middot; {d.outcome_name}</span>}
        </div>
        <div style={{ display: "flex", gap: 16 }}>
          <ScoreBadge value={parseFloat(s.signal_score)} label="Str" />
          <ScoreBadge value={parseFloat(s.confidence)} label="Conf" />
          <ScoreBadge value={parseFloat(s.rank_score)} label="Rank" />
        </div>
      </div>
    </Link>
  );
}

export default function SignalFeed() {
  const [data, setData] = useState(null);
  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(1);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [paused, setPaused] = useState(false);
  const intervalRef = useRef(null);

  const fetchData = useCallback(() => {
    const params = { page, pageSize: PAGE_SIZE };
    if (filter) params.signalType = filter;
    getSignals(params)
      .then((d) => {
        setData(d);
        setLastUpdated(new Date());
        setError(null);
      })
      .catch((e) => setError(e.message));
  }, [filter, page]);

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
  }, [filter]);

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
          {TYPE_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>

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

        {lastUpdated && (
          <span style={{ fontSize: 12, color: "var(--text-dim)", marginLeft: "auto" }}>
            Updated {lastUpdated.toLocaleTimeString()}
          </span>
        )}
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
