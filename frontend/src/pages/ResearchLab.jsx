import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  createResearchBatch,
  getApiBase,
  getLatestResearchBatch,
  getProductionUrl,
  isLocalApiBase,
} from "../api";

const STATUS_COLORS = {
  pending: "var(--yellow)",
  running: "var(--accent)",
  completed: "var(--green)",
  completed_with_warnings: "var(--yellow)",
  failed: "var(--red)",
  cancelled: "var(--text-dim)",
  empty_universe: "var(--red)",
  lane_failures: "var(--yellow)",
  missing_history: "var(--yellow)",
  partial_history: "var(--yellow)",
  ready: "var(--green)",
  healthy: "var(--green)",
  profitable: "var(--green)",
  research_ready: "var(--green)",
  watch: "var(--yellow)",
  insufficient_evidence: "var(--text-dim)",
};

const BLOCKER_ACTIONS = {
  empty_research_universe: {
    label: "Connect the lab to populated data",
    why: "This run selected zero markets, so the scoreboard is measuring an empty database.",
  },
  no_historical_signals: {
    label: "Backfill or ingest historical signals",
    why: "The frozen control needs stored decisions before it can prove paper EV.",
  },
  historical_signal_replay_too_large_for_inline_control: {
    label: "Move control replay into a worker",
    why: "The inline control still sees production-wide signal history instead of only the selected universe.",
  },
  no_price_snapshots: {
    label: "Run price snapshot capture",
    why: "Detector sweeps need historical prices before they can rank variants.",
  },
  snapshot_replay_too_large_for_inline_sweep: {
    label: "Build streaming detector sweep",
    why: "Inline replay would load too many snapshots; stream or shard it before trusting sweep EV.",
  },
  no_actionable_structure_opportunities: {
    label: "Populate actionable structure opportunities",
    why: "Structure needs fresh groups and executable orderbook context before it can surface candidates.",
  },
  no_actionable_opportunities_for_maker_quotes: {
    label: "Create maker inputs after structure exists",
    why: "Maker replay needs actionable opportunities before quote economics can be generated.",
  },
  no_quote_recommendations: {
    label: "Sync fee data and generate quote recommendations",
    why: "The maker lane cannot estimate passive quote EV without advisory quotes.",
  },
  no_execution_policy_candidates: {
    label: "Generate execution-policy candidates",
    why: "Execution replay needs cross/post/step-ahead candidates before it can compare fill quality.",
  },
  no_active_candidate_run: {
    label: "Start the Kalshi fade paper lane",
    why: "Historical alpha needs a separate forward paper run before it can become trustworthy.",
  },
  no_matching_kalshi_down_yes_fade_signals: {
    label: "Wait for fresh down-YES candidates",
    why: "The v2 lane only learns when fresh mid-priced Kalshi YES contracts move down with negative YES EV.",
  },
  observation_window_below_30d: {
    label: "Let the candidate lane age",
    why: "Forward paper alpha needs a 30-day observation window before promotion talk is meaningful.",
  },
  insufficient_resolved_paper_trades: {
    label: "Collect more resolved trades",
    why: "Execution-adjusted paper EV needs at least 20 resolved trades before the lane can pass.",
  },
  nonpositive_execution_adjusted_pnl: {
    label: "Keep the lane paper-only",
    why: "The historical edge is not enough if the forward execution-adjusted ledger is flat or negative.",
  },
  nonpositive_avg_clv: {
    label: "Watch CLV before trusting P&L",
    why: "Positive CLV is the early sign that the lane is buying better than later market consensus.",
  },
  lane_execution_failed: {
    label: "Fix the failing research lane",
    why: "A failed lane can hide the highest-EV path, usually because schema or replay inputs are not ready.",
  },
  no_kalshi_resolved_signal_history: {
    label: "Resolve more Kalshi history",
    why: "The alpha factory needs resolved Kalshi signals with P&L and CLV before it can discover candidates.",
  },
  no_surviving_alpha_factory_candidates: {
    label: "Broaden Kalshi alpha search",
    why: "No train/validation-selected candidate survived the chronological holdout.",
  },
  no_executable_alpha_factory_candidates: {
    label: "Review ambiguous candidates",
    why: "The factory found historical evidence, but not a safe paper trade expression yet.",
  },
};

function fmtMoney(value) {
  if (value == null) return "--";
  const numeric = Number(value);
  const sign = numeric > 0 ? "+" : numeric < 0 ? "-" : "";
  return `${sign}$${Math.abs(numeric).toFixed(2)}`;
}

function fmtPct(value) {
  if (value == null) return "--";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtInt(value) {
  return Number(value || 0).toLocaleString();
}

function fmtDate(value) {
  if (!value) return "--";
  return new Date(value).toLocaleString();
}

function shortId(value) {
  return value ? String(value).slice(0, 8) : "--";
}

function sourceLink(row) {
  if (row.source_kind === "backtest" && row.source_ref) {
    return { to: `/backtests/${row.source_ref}`, label: "Backtest" };
  }
  if (row.source_kind === "profitability_snapshot" || row.source_kind === "profit_tools") {
    return { to: "/paper-trading", label: "Profit Tools" };
  }
  if (row.source_kind === "kalshi_low_yes_fade_snapshot" || row.source_kind === "kalshi_down_yes_fade_snapshot") {
    return { to: "/paper-trading", label: "Paper Lane" };
  }
  if (row.source_kind === "alpha_factory_snapshot") {
    return { to: "/research-lab", label: "Alpha Factory" };
  }
  if (row.source_kind === "polymarket_replay" || row.source_kind === "execution_policy_candidate") {
    return { to: "/strategies", label: "Replay" };
  }
  if (row.source_kind === "structure_opportunity" || row.source_kind === "quote_recommendation") {
    return { to: "/structures", label: "Structure" };
  }
  if (row.source_kind === "research_readiness") {
    return { to: "/health", label: "Health" };
  }
  return null;
}

function hasBlockers(row) {
  return Array.isArray(row?.blockers) && row.blockers.length > 0;
}

function isViableLane(row) {
  if (!row || row.status === "failed" || hasBlockers(row)) return false;
  if (["healthy", "profitable", "research_ready"].includes(row.verdict)) return true;
  const executionPnl = Number(row.realized_pnl || 0) + Number(row.mark_to_market_pnl || 0);
  return executionPnl > 0 || Number(row.replay_net_pnl || 0) > 0 || Number(row.avg_clv || 0) > 0;
}

function buildFallbackReadiness(batch, topBlockers, laneResults) {
  const universe = batch?.universe || {};
  const counts = {
    market_count: Number(universe.market_count || 0),
    outcome_count: Number(universe.outcome_count || 0),
    signal_count: Number(universe.signal_count || 0),
    price_snapshot_count: Number(universe.price_snapshot_count || 0),
    orderbook_snapshot_count: Number(universe.orderbook_snapshot_count || 0),
    failed_lane_count: laneResults.filter((row) => row.status === "failed").length,
  };
  const blockers = topBlockers.map((row) => row.blocker).filter(Boolean);
  if (!counts.market_count) blockers.unshift("empty_research_universe");

  let status = "ready";
  let summary = "Stored evidence is ready for ranked paper research.";
  if (!counts.market_count) {
    status = "empty_universe";
    summary = "This batch ran against a backend with zero selected markets.";
  } else if (counts.failed_lane_count) {
    status = "lane_failures";
    summary = "The universe exists, but one or more lanes failed before evidence could be ranked.";
  } else if (!counts.signal_count && !counts.price_snapshot_count && !counts.orderbook_snapshot_count) {
    status = "missing_history";
    summary = "Markets were selected, but historical signal, price, and orderbook coverage is missing.";
  } else if (!counts.signal_count || !counts.price_snapshot_count) {
    status = "partial_history";
    summary = "The lab has a market universe, but evidence coverage is still partial.";
  }

  const seen = new Set();
  const actions = blockers.flatMap((blocker) => {
    if (seen.has(blocker)) return [];
    seen.add(blocker);
    const template = BLOCKER_ACTIONS[blocker];
    if (!template) return [];
    return [{
      family: "research_lab",
      lane: "readiness",
      label: template.label,
      why: template.why,
      source_kind: "research_readiness",
      source_ref: blocker,
    }];
  }).slice(0, 5);

  return { status, summary, counts, blockers, actions };
}

function StatusBadge({ status }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        border: `1px solid ${STATUS_COLORS[status] || "var(--border)"}`,
        color: STATUS_COLORS[status] || "var(--text-dim)",
        borderRadius: 4,
        padding: "2px 7px",
        fontSize: 11,
        fontWeight: 700,
        textTransform: "uppercase",
      }}
    >
      {status || "unknown"}
    </span>
  );
}

function SummaryStat({ label, value }) {
  return (
    <div
      style={{
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: 12,
        background: "var(--panel)",
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700 }}>{value}</div>
    </div>
  );
}

function EnvironmentWarning({ show }) {
  if (!show) return null;
  const productionUrl = getProductionUrl();
  const productionResearchUrl = productionUrl ? `${productionUrl}/research-lab` : null;
  return (
    <section
      style={{
        border: "1px solid var(--yellow)",
        borderRadius: 8,
        padding: 14,
        background: "rgba(245, 158, 11, 0.08)",
        marginBottom: 18,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "center" }}>
        <div>
          <h3 style={{ fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Local Empty Backend</h3>
          <div style={{ color: "var(--text-dim)", fontSize: 13 }}>
            This page is connected to <code>{getApiBase()}</code>, which is a local sandbox API. It is not the Hetzner production evidence stack.
          </div>
        </div>
        {productionResearchUrl && (
          <a
            href={productionResearchUrl}
            style={{
              border: "1px solid var(--yellow)",
              borderRadius: 6,
              color: "var(--yellow)",
              padding: "7px 10px",
              fontWeight: 700,
              whiteSpace: "nowrap",
            }}
          >
            Open Production
          </a>
        )}
      </div>
    </section>
  );
}

function DataReadiness({ readiness }) {
  if (!readiness) return null;
  const counts = readiness.counts || {};
  return (
    <section style={{ border: "1px solid var(--border)", borderRadius: 8, padding: 14, background: "var(--panel)", marginBottom: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 12 }}>
        <div>
          <h3 style={{ fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Data Readiness</h3>
          <div style={{ color: "var(--text-dim)", fontSize: 13 }}>{readiness.summary}</div>
        </div>
        <StatusBadge status={readiness.status} />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: 10, fontSize: 13 }}>
        {[
          ["Outcomes", counts.outcome_count],
          ["Prices", counts.price_snapshot_count],
          ["Orderbooks", counts.orderbook_snapshot_count],
          ["Failed Lanes", counts.failed_lane_count],
        ].map(([label, value]) => (
          <div key={label}>
            <div style={{ color: "var(--text-dim)", fontSize: 11, textTransform: "uppercase", marginBottom: 4 }}>{label}</div>
            <strong>{fmtInt(value)}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}

function LaneTable({ rows }) {
  if (!rows.length) {
    return <div style={{ color: "var(--text-dim)", fontSize: 13 }}>No lane results have been recorded yet.</div>;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ color: "var(--text-dim)", borderBottom: "1px solid var(--border)" }}>
            <th style={{ textAlign: "right", padding: 8 }}>Rank</th>
            <th style={{ textAlign: "left", padding: 8 }}>Family</th>
            <th style={{ textAlign: "left", padding: 8 }}>Lane</th>
            <th style={{ textAlign: "left", padding: 8 }}>Verdict</th>
            <th style={{ textAlign: "right", padding: 8 }}>Realized</th>
            <th style={{ textAlign: "right", padding: 8 }}>Replay</th>
            <th style={{ textAlign: "right", padding: 8 }}>CLV</th>
            <th style={{ textAlign: "right", padding: 8 }}>Sample</th>
            <th style={{ textAlign: "right", padding: 8 }}>Fill</th>
            <th style={{ textAlign: "left", padding: 8 }}>Source</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const link = sourceLink(row);
            return (
              <tr key={row.id || `${row.family}-${row.lane}`} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={{ textAlign: "right", padding: 8, fontFamily: "var(--mono)" }}>{row.rank_position ?? "--"}</td>
                <td style={{ padding: 8 }}><code>{row.family}</code></td>
                <td style={{ padding: 8 }}><code>{row.lane}</code></td>
                <td style={{ padding: 8 }}><StatusBadge status={row.verdict} /></td>
                <td style={{ textAlign: "right", padding: 8 }}>{fmtMoney(row.realized_pnl)}</td>
                <td style={{ textAlign: "right", padding: 8 }}>{fmtMoney(row.replay_net_pnl)}</td>
                <td style={{ textAlign: "right", padding: 8 }}>{row.avg_clv == null ? "--" : Number(row.avg_clv).toFixed(4)}</td>
                <td style={{ textAlign: "right", padding: 8 }}>{row.resolved_trades ?? 0}</td>
                <td style={{ textAlign: "right", padding: 8 }}>{fmtPct(row.fill_rate)}</td>
                <td style={{ padding: 8 }}>{link ? <Link to={link.to}>{link.label}</Link> : "--"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function ResearchLab() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState(null);
  const [lastLoadedAt, setLastLoadedAt] = useState(null);

  const load = async () => {
    setError(null);
    try {
      const latest = await getLatestResearchBatch();
      setData(latest);
      setLastLoadedAt(new Date());
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  useEffect(() => {
    const status = data?.batch?.status;
    if (status !== "pending" && status !== "running") return undefined;
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [data?.batch?.status]);

  useEffect(() => {
    if (!error || data) return undefined;
    const timer = setTimeout(load, 3000);
    return () => clearTimeout(timer);
  }, [error, data]);

  async function startBatch() {
    setStarting(true);
    setError(null);
    try {
      const created = await createResearchBatch();
      setData({
        batch: { ...created.batch, status: created.started ? "running" : created.batch?.status },
        lane_results: [],
        top_blockers: [],
        top_ev_candidates: [],
        data_readiness: null,
      });
      setTimeout(load, 1000);
    } catch (err) {
      setError(err.message);
    } finally {
      setStarting(false);
    }
  }

  const batch = data?.batch;
  const laneResults = data?.lane_results || [];
  const topBlockers = data?.top_blockers || [];
  const readiness = data?.data_readiness || buildFallbackReadiness(batch, topBlockers, laneResults);
  const topCandidates = (data?.top_ev_candidates?.length ? data.top_ev_candidates : readiness.actions) || [];
  const universe = batch?.universe || {};
  const bestLane = useMemo(() => laneResults.find(isViableLane), [laneResults]);
  const localEmptyBackend = isLocalApiBase() && readiness?.status === "empty_universe";

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 6 }}>Research Lab</h2>
          <div style={{ color: "var(--text-dim)", fontSize: 13 }}>Paper-only EV testing across default, Kalshi fade, structure, maker, and execution policy lanes.</div>
          <div style={{ color: "var(--text-dim)", fontSize: 12, marginTop: 5 }}>
            API <code>{getApiBase()}</code>{batch?.id ? ` · Batch ${shortId(batch.id)}` : ""}{lastLoadedAt ? ` · Loaded ${fmtDate(lastLoadedAt)}` : ""}
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <button
            type="button"
            onClick={load}
            disabled={loading}
            style={{
              background: "transparent",
              color: "var(--text)",
              border: "1px solid var(--border)",
              borderRadius: 6,
              padding: "8px 12px",
              fontWeight: 700,
              cursor: loading ? "default" : "pointer",
              opacity: loading ? 0.7 : 1,
            }}
          >
            Refresh
          </button>
          <button
            type="button"
            onClick={startBatch}
            disabled={starting || batch?.status === "running" || batch?.status === "pending"}
            style={{
              background: "var(--accent)",
              color: "white",
              border: 0,
              borderRadius: 6,
              padding: "8px 12px",
              fontWeight: 700,
              cursor: starting ? "default" : "pointer",
              opacity: starting || batch?.status === "running" || batch?.status === "pending" ? 0.7 : 1,
            }}
          >
            {starting ? "Starting..." : "Run Profit Hunt"}
          </button>
        </div>
      </div>

      {error && (
        <div style={{ border: "1px solid var(--red)", color: "var(--red)", borderRadius: 8, padding: 12, marginBottom: 16, display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
          <span>{error}</span>
          <button
            type="button"
            onClick={load}
            style={{
              border: "1px solid var(--red)",
              background: "transparent",
              color: "var(--red)",
              borderRadius: 6,
              padding: "5px 9px",
              cursor: "pointer",
              fontWeight: 700,
            }}
          >
            Retry
          </button>
        </div>
      )}

      {loading ? (
        <div style={{ color: "var(--text-dim)" }}>Loading research batch...</div>
      ) : !batch ? (
        <div style={{ border: "1px solid var(--border)", borderRadius: 8, padding: 16, background: "var(--panel)" }}>
          No research batch has been run yet.
        </div>
      ) : (
        <>
          <EnvironmentWarning show={localEmptyBackend} />

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12, marginBottom: 18 }}>
            <SummaryStat label="Status" value={<StatusBadge status={batch.status} />} />
            <SummaryStat label="Batch" value={shortId(batch.id)} />
            <SummaryStat label="Markets" value={universe.market_count ?? 0} />
            <SummaryStat label="Signals" value={universe.signal_count ?? 0} />
            <SummaryStat label="Best Lane" value={bestLane ? `${bestLane.family}/${bestLane.lane}` : "No viable lane yet"} />
          </div>

          <DataReadiness readiness={readiness} />

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 18, alignItems: "start" }}>
            <section>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 10 }}>
                <h3 style={{ fontSize: 15, fontWeight: 700 }}>Ranked Lane Scoreboard</h3>
                <div style={{ color: "var(--text-dim)", fontSize: 12 }}>{fmtDate(batch.window_start)} - {fmtDate(batch.window_end)}</div>
              </div>
              <LaneTable rows={laneResults} />
            </section>

            <aside style={{ display: "grid", gap: 14 }}>
              <section style={{ border: "1px solid var(--border)", borderRadius: 8, padding: 14, background: "var(--panel)" }}>
                <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>Top Blockers</h3>
                {!topBlockers.length ? (
                  <div style={{ color: "var(--text-dim)", fontSize: 13 }}>None</div>
                ) : (
                  <div style={{ display: "grid", gap: 8 }}>
                    {topBlockers.map((row) => (
                      <div key={row.blocker} style={{ display: "flex", justifyContent: "space-between", gap: 10, fontSize: 13 }}>
                        <code>{row.blocker}</code>
                        <strong>{row.count}</strong>
                      </div>
                    ))}
                  </div>
                )}
              </section>

              <section style={{ border: "1px solid var(--border)", borderRadius: 8, padding: 14, background: "var(--panel)" }}>
                <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>Top 5 Moves</h3>
                {!topCandidates.length ? (
                  <div style={{ color: "var(--text-dim)", fontSize: 13 }}>None</div>
                ) : (
                  <div style={{ display: "grid", gap: 10 }}>
                    {topCandidates.map((candidate, index) => {
                      const link = sourceLink(candidate);
                      return (
                        <div key={`${candidate.label}-${index}`} style={{ fontSize: 13 }}>
                          <div style={{ fontWeight: 700 }}>{candidate.label}</div>
                          <div style={{ color: "var(--text-dim)", margin: "3px 0" }}>{candidate.why}</div>
                          {link && <Link to={link.to}>{link.label}</Link>}
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>
            </aside>
          </div>
        </>
      )}
    </div>
  );
}
