import { useEffect, useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { getMarkets, exportMarketsCsv } from "../api";

const PLATFORM_OPTIONS = [
  { value: "", label: "All Platforms" },
  { value: "kalshi", label: "Kalshi" },
];

const SORT_OPTIONS = [
  { value: "updated", label: "Recently Updated" },
  { value: "volume", label: "Volume (High to Low)" },
  { value: "end_date", label: "Ending Soon" },
  { value: "question", label: "Alphabetical" },
];

const PLATFORM_COLORS = { kalshi: "#f59e0b" };
const PAGE_SIZE = 50;

function PlatformBadge({ platform }) {
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
      {platform === "kalshi" ? "KA" : platform?.slice(0, 2)?.toUpperCase() || "--"}
    </span>
  );
}

function MarketCard({ market }) {
  const m = market;
  const endDate = m.end_date ? new Date(m.end_date).toLocaleDateString() : null;

  return (
    <Link to={`/markets/${m.id}`} style={{ textDecoration: "none", color: "inherit" }}>
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
            <PlatformBadge platform={m.platform} />
            {m.category && (
              <span style={{ fontSize: 11, color: "var(--text-dim)" }}>{m.category}</span>
            )}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {!m.active && (
              <span style={{ fontSize: 10, color: "var(--red)", fontWeight: 600 }}>CLOSED</span>
            )}
            {endDate && (
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Ends {endDate}</span>
            )}
          </div>
        </div>
        <div style={{ fontSize: 14, lineHeight: 1.4 }}>{m.question}</div>
      </div>
    </Link>
  );
}

function SelectStyle() {
  return {
    padding: "6px 12px",
    fontSize: 13,
    background: "var(--bg-card)",
    color: "var(--text)",
    border: "1px solid var(--border)",
    borderRadius: 6,
    cursor: "pointer",
  };
}

export default function Markets() {
  const [data, setData] = useState(null);
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [platform, setPlatform] = useState("");
  const [sortBy, setSortBy] = useState("updated");
  const [page, setPage] = useState(1);
  const [error, setError] = useState(null);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(search), 300);
    return () => clearTimeout(timer);
  }, [search]);

  const fetchData = useCallback(() => {
    const params = { page, pageSize: PAGE_SIZE, sortBy };
    if (platform) params.platform = platform;
    if (debouncedSearch) params.search = debouncedSearch;
    getMarkets(params)
      .then((d) => { setData(d); setError(null); })
      .catch((e) => setError(e.message));
  }, [platform, debouncedSearch, sortBy, page]);

  useEffect(() => { fetchData(); }, [fetchData]);
  useEffect(() => { setPage(1); }, [platform, debouncedSearch, sortBy]);

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;

  return (
    <div>
      {/* Search + filters */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <input
          type="text"
          placeholder="Search markets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            padding: "6px 12px",
            fontSize: 13,
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            flex: "1 1 200px",
            minWidth: 150,
          }}
        />

        <select value={platform} onChange={(e) => setPlatform(e.target.value)} style={SelectStyle()}>
          {PLATFORM_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>

        <select value={sortBy} onChange={(e) => setSortBy(e.target.value)} style={SelectStyle()}>
          {SORT_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>

        <button
          onClick={exportMarketsCsv}
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
      </div>

      {error && <div style={{ color: "var(--red)", marginBottom: 12 }}>Error: {error}</div>}

      {data && (
        <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 12 }}>
          {data.total} market{data.total !== 1 ? "s" : ""}
        </div>
      )}

      {!data && !error && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {[1,2,3,4].map(i => <div key={i} className="skeleton" style={{ borderRadius: 8, height: 80, marginBottom: 8 }} />)}
        </div>
      )}

      {data && data.markets.length === 0 && (
        <div style={{ color: "var(--text-dim)", padding: 40, textAlign: "center" }}>
          No markets found. Try adjusting your search or filters.
        </div>
      )}

      {data && data.markets.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {data.markets.map((m) => <MarketCard key={m.id} market={m} />)}
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
