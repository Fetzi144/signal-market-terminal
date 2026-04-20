import { useCallback, useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import {
  getPaperTradingDefaultStrategyDashboard,
  getPaperTradingHistory,
} from "../api";

const PAGE_SIZE = 20;
const STRATEGY_SCOPE = "default_strategy";

function fmtCurrency(value) {
  if (value == null) return "-";
  return Number(value).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function fmtPercent(value) {
  if (value == null) return "-";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtCents(value) {
  if (value == null) return "-";
  return `${(Number(value) * 100).toFixed(1)}c`;
}

function fmtDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function pnlColor(value) {
  if (value == null) return "var(--text-dim)";
  if (Number(value) > 0) return "var(--green)";
  if (Number(value) < 0) return "var(--red)";
  return "var(--text-dim)";
}

function directionLabel(direction) {
  if (!direction) return "-";
  return direction === "buy_no" ? "BUY NO" : "BUY YES";
}

function directionColor(direction) {
  if (direction === "buy_no") return "var(--yellow)";
  if (direction === "buy_yes") return "var(--green)";
  return "var(--text-dim)";
}

function verdictColor(verdict) {
  if (verdict === "keep") return "var(--green)";
  if (verdict === "cut") return "var(--red)";
  return "var(--yellow)";
}

function reviewVerdictColor(verdict) {
  if (verdict === "keep") return "var(--green)";
  if (verdict === "cut" || verdict === "not_ready") return "var(--red)";
  return "var(--yellow)";
}

function artifactGenerationColor(status) {
  if (status === "complete") return "var(--green)";
  if (status === "invalid") return "var(--red)";
  if (status === "partial") return "var(--yellow)";
  return "var(--text-dim)";
}

function artifactGenerationLabel(status) {
  if (status === "complete") return "Complete";
  if (status === "invalid") return "Invalid";
  if (status === "partial") return "Partial";
  return "Missing";
}

function evidenceFreshnessColor(status) {
  if (status === "fresh") return "var(--green)";
  if (status === "stale" || status === "missing_review") return "var(--yellow)";
  return "var(--text-dim)";
}

function evidenceFreshnessLabel(status) {
  if (status === "fresh") return "Fresh";
  if (status === "stale") return "Stale";
  if (status === "missing_review") return "Missing Review";
  return "No Active Run";
}

function fmtSeconds(value) {
  if (value == null) return "-";
  return `${fmtWhole(value)}s`;
}

function observationMeta(status) {
  switch (status) {
    case "live_waiting_for_trades":
      return {
        label: "Live, waiting for trades",
        color: "var(--yellow)",
        text: "The frozen baseline has launched, but no in-window paper trades have opened yet. Use the skip-reason panel to find the bottleneck.",
      };
    case "preferred_window_reached":
      return {
        label: "Preferred window reached",
        color: "var(--green)",
        text: "The default strategy has enough tracked time to judge it on more than short-term noise.",
      };
    case "minimum_window_reached":
      return {
        label: "Minimum window reached",
        color: "var(--yellow)",
        text: "You have the minimum observation window. Keep the strategy frozen and keep collecting evidence.",
      };
    case "collecting_data":
      return {
        label: "Collecting data",
        color: "var(--accent)",
        text: "The baseline strategy is live, but the observation window is still too short for hard calls.",
      };
    default:
      return {
        label: "Not started",
        color: "var(--text-dim)",
        text: "The baseline launch boundary is not active yet, so the prove-the-edge window has not opened.",
      };
  }
}

function fmtWhole(value) {
  if (value == null) return "-";
  return Number(value).toLocaleString();
}

function observationProgress(observation) {
  if (!observation) return 0;
  const minimumDays = Number(observation.minimum_days || 0);
  const trackedDays = Number(observation.days_tracked || 0);
  if (minimumDays <= 0) return 0;
  return Math.max(0, Math.min(100, (trackedDays / minimumDays) * 100));
}

function briefToneColor(tone) {
  if (tone === "positive") return "var(--green)";
  if (tone === "negative") return "var(--red)";
  if (tone === "watch") return "var(--yellow)";
  return "var(--accent)";
}

function buildStrategyBrief(health) {
  const strategy = health?.strategy || {};
  const observation = health?.observation || {};
  const headline = health?.headline || {};
  const benchmark = health?.benchmark || {};
  const funnel = health?.trade_funnel || {};
  const topSkipReason = health?.skip_reasons?.[0];
  const detectorRows = health?.detector_review || [];
  const cutCount = detectorRows.filter((row) => row.verdict === "cut").length;
  const watchCount = detectorRows.filter((row) => row.verdict === "watch").length;
  const bankroll = Number(strategy.paper_bankroll_usd || 0);
  const openExposure = Number(headline.open_exposure || 0);
  const exposurePct = bankroll > 0 ? openExposure / bankroll : 0;

  let tone = "info";
  let badge = "Collecting evidence";
  let title = "Keep the baseline frozen";
  let action = "Let the default strategy run unchanged until the observation window fills in.";

  if (observation.status === "not_started") {
    badge = "Not started";
    title = "The baseline window has not opened yet";
    action = "Keep the contract unchanged and wait for the launch boundary to begin the prove-the-edge run.";
  } else if (observation.status === "live_waiting_for_trades") {
    tone = "watch";
    badge = "Waiting for first trade";
    title = "The baseline is live, but nothing is entering the book";
    action = "Treat skip reasons as the top priority until the first in-window baseline trade opens.";
  } else if (
    Number(headline.resolved_trades || 0) > 0
    && Number(headline.cumulative_pnl || 0) > 0
    && Number(headline.avg_clv || 0) > 0
    && observation.status === "preferred_window_reached"
  ) {
    tone = "positive";
    badge = "Edge improving";
    title = "The baseline is earning the right to stay on";
    action = "Keep the contract unchanged and focus on whether the edge persists through more settled trades.";
  } else if (
    Number(headline.resolved_trades || 0) > 0
    && (Number(headline.cumulative_pnl || 0) < 0 || Number(headline.avg_clv || 0) < 0)
  ) {
    tone = "negative";
    badge = "Review risk";
    title = "The baseline needs a harder review";
    action = "Do not add new alpha paths. Prune weak detectors or raise thresholds only after the current window closes.";
  } else if (observation.status === "minimum_window_reached") {
    tone = "watch";
    badge = "Minimum reached";
    title = "You can review, but not celebrate yet";
    action = "Treat the current numbers as directional only and wait for the preferred window before changing the baseline.";
  }

  const evidence = [
    Number(funnel.traded_signals || 0) > 0
      ? `${fmtWhole(funnel.traded_signals)} traded default-strategy signal(s) came from ${fmtWhole(funnel.qualified_signals)} qualified opportunities.`
      : Number(funnel.qualified_signals || 0) > 0
        ? `${fmtWhole(funnel.qualified_signals)} qualified default-strategy signals exist, but none have turned into measured baseline trades yet.`
        : "No default-strategy trades exist yet, so the edge verdict is still provisional.",
    observation?.baseline_start_at
      ? `The frozen baseline window started at ${fmtDate(observation.baseline_start_at)}.`
      : "No explicit baseline start is configured yet.",
    `${fmtWhole(headline.open_trades || 0)} live baseline trade(s) currently use ${fmtCurrency(openExposure)} of ${fmtCurrency(bankroll)} bankroll.`,
    topSkipReason
      ? `${fmtWhole(topSkipReason.count)} in-window signal(s) are currently blocked by "${topSkipReason.reason_label}".`
      : Number(funnel.excluded_legacy_trades || 0) > 0
        ? `${fmtWhole(funnel.excluded_legacy_trades)} legacy paper trade(s) are excluded from the frozen baseline read.`
        : cutCount > 0
          ? `${cutCount} detector${cutCount === 1 ? "" : "s"} ${cutCount === 1 ? "is" : "are"} already marked cut in the detector review loop.`
          : `${watchCount} detector${watchCount === 1 ? "" : "s"} ${watchCount === 1 ? "is" : "are"} still on watch and none are cut yet.`,
    Number(funnel.excluded_pre_launch_trades || 0) > 0
      ? `${fmtWhole(funnel.excluded_pre_launch_trades)} pre-launch trade(s) sit outside the prove-the-edge window.`
      : cutCount > 0
        ? `${cutCount} detector${cutCount === 1 ? "" : "s"} ${cutCount === 1 ? "is" : "are"} already marked cut in the detector review loop.`
        : `${watchCount} detector${watchCount === 1 ? "" : "s"} ${watchCount === 1 ? "is" : "are"} still on watch and none are cut yet.`,
  ];

  const priorities = [];
  if (observation.status === "collecting_data") {
    priorities.push(`Do not re-tune the baseline for another ${observation.days_until_minimum_window} day(s).`);
  }
  if (Number(funnel.qualified_not_traded || 0) > 0) {
    priorities.push(`Review why ${fmtWhole(funnel.qualified_not_traded)} qualified signal(s) did not turn into baseline trades.`);
  }
  if (topSkipReason) {
    priorities.push(`The biggest trade blocker right now is "${topSkipReason.reason_label}" across ${fmtWhole(topSkipReason.count)} in-window signal(s).`);
  }
  if (Number(headline.missing_resolutions || 0) > 0) {
    priorities.push(`Track the ${fmtWhole(headline.missing_resolutions)} traded signal(s) that are still waiting to resolve.`);
  }
  if (exposurePct >= 0.25) {
    priorities.push(`Open exposure is already ${(exposurePct * 100).toFixed(0)}% of bankroll, so avoid layering in discretionary trades that muddy the read.`);
  }
  if (Number(funnel.excluded_legacy_trades || 0) > 0) {
    priorities.push(`Keep legacy paper trades out of the default-strategy read until they are archived or analyzed separately.`);
  }
  if (Number(benchmark.resolved_signals || 0) === 0) {
    priorities.push("The legacy benchmark has no resolved sample yet, so benchmark deltas are not decision-grade.");
  }
  if (priorities.length === 0) {
    priorities.push("Keep the contract unchanged and review again after the next block of settled trades.");
  }

  return {
    tone,
    badge,
    title,
    action,
    evidence,
    priorities,
    cutCount,
    watchCount,
    exposurePct,
  };
}

function SummaryCard({ label, value, sub, color }) {
  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: "18px 20px",
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ fontSize: 28, fontFamily: "var(--mono)", fontWeight: 700, color: color || "var(--text)" }}>
        {value}
      </div>
      {sub && <div style={{ marginTop: 4, fontSize: 11, color: "var(--text-dim)" }}>{sub}</div>}
    </div>
  );
}

function Section({ title, actions, children }) {
  return (
    <section style={{ marginBottom: 28 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, marginBottom: 12 }}>
        <h2 style={{ fontSize: 15, fontWeight: 600 }}>{title}</h2>
        {actions}
      </div>
      {children}
    </section>
  );
}

function PnlChart({ curve }) {
  if (!curve || curve.length === 0) {
    return (
      <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 20, color: "var(--text-dim)" }}>
        No resolved paper trades yet. The P&L curve will appear once trades settle.
      </div>
    );
  }

  const chartData = curve.map((point) => ({
    ...point,
    label: new Date(point.timestamp).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
  }));

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "16px 8px 8px" }}>
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={chartData} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis dataKey="label" tick={{ fontSize: 11, fill: "var(--text-dim)" }} />
          <YAxis tick={{ fontSize: 11, fill: "var(--text-dim)" }} tickFormatter={(value) => `$${value}`} width={55} />
          <Tooltip
            formatter={(value, name) => [fmtCurrency(value), name === "pnl" ? "Cumulative P&L" : "Trade P&L"]}
            labelFormatter={(label, payload) => payload?.[0]?.payload?.timestamp ? fmtDate(payload[0].payload.timestamp) : label}
            contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", fontSize: 12 }}
          />
          <ReferenceLine y={0} stroke="var(--text-dim)" strokeDasharray="4 4" />
          <Line
            type="monotone"
            dataKey="pnl"
            stroke={chartData[chartData.length - 1].pnl >= 0 ? "var(--green)" : "var(--red)"}
            strokeWidth={2}
            dot={{ r: 3 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function MetricStrip({ metrics }) {
  if (!metrics || metrics.total_trades === 0) {
    return null;
  }

  const cards = [
    { label: "Sharpe", value: metrics.sharpe_ratio.toFixed(4) },
    { label: "Max Drawdown", value: fmtCurrency(metrics.max_drawdown), color: "var(--red)" },
    { label: "Profit Factor", value: metrics.profit_factor == null ? "-" : metrics.profit_factor.toFixed(2) },
    { label: "Avg Trade", value: fmtCurrency(metrics.avg_pnl), color: pnlColor(metrics.avg_pnl) },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12 }}>
      {cards.map((card) => (
        <div key={card.label} style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "14px 16px" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{card.label}</div>
          <div style={{ fontSize: 22, fontFamily: "var(--mono)", fontWeight: 700, color: card.color || "var(--text)" }}>
            {card.value}
          </div>
        </div>
      ))}
    </div>
  );
}

function EmptyState({ text }) {
  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 20, color: "var(--text-dim)" }}>
      {text}
    </div>
  );
}

function ObservationBanner({ observation }) {
  const meta = observationMeta(observation?.status);

  return (
    <div
      style={{
        marginBottom: 20,
        padding: "14px 16px",
        borderRadius: 8,
        border: `1px solid ${meta.color}`,
        background: "var(--bg-card)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginBottom: 6 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: meta.color }}>{meta.label}</div>
        <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
          {observation?.days_tracked == null ? "0 days tracked" : `${observation.days_tracked} days tracked`}
        </div>
      </div>
      <div style={{ fontSize: 13, color: "var(--text)" }}>{meta.text}</div>
      {observation?.baseline_start_at && (
        <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 6 }}>
          Baseline window started {fmtDate(observation.baseline_start_at)}.
        </div>
      )}
      {observation?.status === "live_waiting_for_trades" && (
        <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 6 }}>
          No baseline trade has opened yet. The observation clock is running, but evidence is still blocked at the funnel.
        </div>
      )}
      {observation?.status === "collecting_data" && (
        <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 6 }}>
          {observation.days_until_minimum_window} day(s) until the minimum review window.
        </div>
      )}
    </div>
  );
}

function ReviewVerdictPanel({ health }) {
  const review = health?.review_verdict;
  const replay = health?.replay;
  if (!review) return <EmptyState text="Review verdict unavailable." />;
  const color = reviewVerdictColor(review.verdict);

  return (
    <div
      style={{
        marginBottom: 20,
        padding: 18,
        borderRadius: 12,
        background: "var(--bg-card)",
        border: `1px solid ${color}`,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
        <div>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
            Operator Verdict
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginBottom: 8 }}>
            <span
              style={{
                fontSize: 11,
                fontWeight: 800,
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                padding: "4px 10px",
                borderRadius: 999,
                background: "var(--bg)",
                border: "1px solid var(--border)",
                color,
              }}
            >
              {review.verdict}
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              {fmtWhole(review.blockers?.length || 0)} blocker(s)
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Replay {replay?.coverage_mode || "-"}
            </span>
          </div>
          <div style={{ fontSize: 14, color: "var(--text)", lineHeight: 1.6, maxWidth: 820 }}>
            {review.summary}
          </div>
        </div>
      </div>

      {review.blockers && review.blockers.length > 0 ? (
        <div style={{ display: "grid", gap: 10 }}>
          {review.blockers.map((blocker) => (
            <div
              key={blocker.code}
              style={{
                borderRadius: 10,
                border: "1px solid var(--border)",
                background: "var(--bg)",
                padding: 14,
              }}
            >
              <div style={{ fontSize: 12, fontWeight: 700, color: color, marginBottom: 6 }}>
                {blocker.label}
              </div>
              <div style={{ fontSize: 13, color: "var(--text-dim)", lineHeight: 1.55 }}>
                {blocker.detail}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div style={{ fontSize: 13, color: "var(--text-dim)", lineHeight: 1.6 }}>
          No active evidence-gate blockers are present. Keep/cut still depends on the default-strategy evidence itself, not on pilot or live-trading state.
        </div>
      )}
    </div>
  );
}

function ArtifactPathRow({ label, value }) {
  return (
    <div
      style={{
        borderRadius: 10,
        border: "1px solid var(--border)",
        background: "var(--bg)",
        padding: 14,
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ fontSize: 12, color: value ? "var(--text)" : "var(--text-dim)", fontFamily: "var(--mono)", wordBreak: "break-all", lineHeight: 1.55 }}>
        {value || "Missing"}
      </div>
    </div>
  );
}

function LatestReviewArtifactPanel({ artifact, freshness }) {
  if (!artifact) return <EmptyState text="Latest review artifact metadata unavailable." />;
  const color = artifactGenerationColor(artifact.generation_status);
  const paths = artifact.artifact_paths || {};
  const freshnessColor = evidenceFreshnessColor(freshness?.status);

  return (
    <div
      style={{
        marginBottom: 20,
        padding: 18,
        borderRadius: 12,
        background: "var(--bg-card)",
        border: `1px solid ${color}`,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
        <div>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
            Latest Review Artifact
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginBottom: 8 }}>
            <span
              style={{
                fontSize: 11,
                fontWeight: 800,
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                padding: "4px 10px",
                borderRadius: 999,
                background: "var(--bg)",
                border: "1px solid var(--border)",
                color,
              }}
            >
              {artifactGenerationLabel(artifact.generation_status)}
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Generated {artifact.generated_at ? fmtDate(artifact.generated_at) : "-"}
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Verdict {artifact.verdict || "-"}
            </span>
          </div>
          <div style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.6, maxWidth: 820 }}>
            {artifact.status_detail}
          </div>
        </div>
      </div>

      {freshness && (
        <div
          style={{
            borderRadius: 10,
            border: "1px solid var(--border)",
            background: "var(--bg)",
            padding: 14,
            marginBottom: 10,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginBottom: 8 }}>
            <span
              style={{
                fontSize: 11,
                fontWeight: 800,
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                padding: "4px 10px",
                borderRadius: 999,
                background: "var(--bg-card)",
                border: "1px solid var(--border)",
                color: freshnessColor,
              }}
            >
              {evidenceFreshnessLabel(freshness.status)}
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Last activity {freshness.last_activity_at ? fmtDate(freshness.last_activity_at) : "-"}
            </span>
          </div>
          <div style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.6, marginBottom: 10 }}>
            {freshness.summary}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10 }}>
            <ArtifactPathRow label="Review Age" value={fmtSeconds(freshness.review_age_seconds)} />
            <ArtifactPathRow label="Review Lag" value={fmtSeconds(freshness.review_lag_seconds)} />
            <ArtifactPathRow
              label="Pending Decision Max Age"
              value={`${fmtSeconds(freshness.pending_decision_max_age_seconds)} / stale after ${fmtSeconds(freshness.pending_decision_stale_after_seconds)}`}
            />
          </div>
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 10 }}>
        <ArtifactPathRow label="Markdown Artifact" value={paths.markdown} />
        <ArtifactPathRow label="JSON Artifact" value={paths.json} />
      </div>
    </div>
  );
}

function RefreshButton({ refreshing, onRefresh, lastUpdated }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", justifyContent: "flex-end" }}>
      <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
        {lastUpdated ? `Last sync ${fmtDate(lastUpdated)}` : "Waiting for first sync"}
      </div>
      <button
        onClick={onRefresh}
        disabled={refreshing}
        style={{
          padding: "7px 12px",
          fontSize: 12,
          fontWeight: 700,
          borderRadius: 999,
          border: "1px solid var(--border)",
          background: refreshing ? "var(--bg)" : "var(--bg-card)",
          color: refreshing ? "var(--text-dim)" : "var(--text)",
          cursor: refreshing ? "default" : "pointer",
        }}
      >
        {refreshing ? "Refreshing..." : "Refresh now"}
      </button>
    </div>
  );
}

function OperatorBrief({ health }) {
  const brief = buildStrategyBrief(health);
  const observation = health?.observation;
  const headline = health?.headline;
  const funnel = health?.trade_funnel;
  const toneColor = briefToneColor(brief.tone);
  const progress = observationProgress(observation);

  return (
    <div
      style={{
        marginBottom: 20,
        padding: 20,
        borderRadius: 16,
        border: `1px solid ${toneColor}`,
        background: "linear-gradient(135deg, rgba(59,130,246,0.10), rgba(34,197,94,0.08)), var(--bg-card)",
        boxShadow: "0 10px 30px rgba(15, 23, 42, 0.08)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", marginBottom: 18 }}>
        <div style={{ maxWidth: 720 }}>
          <div
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "5px 10px",
              borderRadius: 999,
              background: "var(--bg)",
              border: "1px solid var(--border)",
              color: toneColor,
              fontSize: 11,
              fontWeight: 800,
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              marginBottom: 12,
            }}
          >
            {brief.badge}
          </div>
          <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1.15, marginBottom: 10 }}>
            {brief.title}
          </div>
          <div style={{ fontSize: 14, color: "var(--text)", lineHeight: 1.6, maxWidth: 760 }}>
            {brief.action}
          </div>
        </div>
        <div
          style={{
            minWidth: 220,
            padding: "14px 16px",
            borderRadius: 12,
            background: "rgba(15, 23, 42, 0.10)",
            border: "1px solid var(--border)",
          }}
        >
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
            Validation Window
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8 }}>
            <div style={{ fontSize: 24, fontWeight: 700, fontFamily: "var(--mono)" }}>
              {observation?.days_tracked == null ? "0.0d" : `${Number(observation.days_tracked).toFixed(1)}d`}
            </div>
            <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
              min {observation?.minimum_days || 0}d
            </div>
          </div>
          <div style={{ marginTop: 12, height: 8, borderRadius: 999, background: "rgba(148, 163, 184, 0.18)", overflow: "hidden" }}>
            <div style={{ width: `${progress}%`, height: "100%", borderRadius: 999, background: toneColor }} />
          </div>
          <div style={{ marginTop: 10, fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
            {observation?.status === "not_started"
              ? "The baseline start boundary is still pending."
              : observation?.status === "live_waiting_for_trades"
              ? "The baseline window is open, but the first trade still has not landed."
              : observation?.status === "collecting_data"
              ? `${observation.days_until_minimum_window} day(s) remain before the first hard review.`
              : "The minimum review gate has been cleared."}
          </div>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
        <div style={{ padding: 16, borderRadius: 12, background: "var(--bg-card)", border: "1px solid var(--border)" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
            Why This Call
          </div>
          <div style={{ display: "grid", gap: 8 }}>
            {brief.evidence.map((item) => (
              <div key={item} style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.55 }}>
                {item}
              </div>
            ))}
          </div>
        </div>

        <div style={{ padding: 16, borderRadius: 12, background: "var(--bg-card)", border: "1px solid var(--border)" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
            Priority Queue
          </div>
          <div style={{ display: "grid", gap: 8 }}>
            {brief.priorities.map((item) => (
              <div key={item} style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.55 }}>
                {item}
              </div>
            ))}
          </div>
        </div>

        <div style={{ padding: 16, borderRadius: 12, background: "var(--bg-card)", border: "1px solid var(--border)" }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
            Fast Read
          </div>
          <div style={{ display: "grid", gap: 10 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Open exposure</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: pnlColor(headline?.open_exposure) }}>
                {(brief.exposurePct * 100).toFixed(0)}%
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Resolved trades</span>
              <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "var(--mono)" }}>
                {fmtWhole(headline?.resolved_trades)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Detector cuts</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: brief.cutCount > 0 ? "var(--red)" : "var(--green)" }}>
                {brief.cutCount}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Missing resolutions</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: Number(headline?.missing_resolutions || 0) > 0 ? "var(--yellow)" : "var(--green)" }}>
                {fmtWhole(headline?.missing_resolutions)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Legacy trades excluded</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: Number(funnel?.excluded_legacy_trades || 0) > 0 ? "var(--yellow)" : "var(--green)" }}>
                {fmtWhole(funnel?.excluded_legacy_trades)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ fontSize: 12, color: "var(--text-dim)" }}>First trade</span>
              <span style={{ fontSize: 13, fontWeight: 700, color: observation?.first_trade_at ? "var(--text)" : "var(--yellow)" }}>
                {observation?.first_trade_at ? fmtDate(observation.first_trade_at) : "Waiting"}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function StrategyContract({ health }) {
  const strategy = health?.strategy;
  if (!strategy) return <EmptyState text="Strategy contract unavailable." />;

  const rules = [
    `Signal path: ${strategy.signal_type}`,
    `Baseline start: ${strategy.baseline_start_at ? fmtDate(strategy.baseline_start_at) : "Unset"}`,
    `EV filter: ${fmtCents(strategy.ev_threshold)}`,
    `Kelly: ${(strategy.kelly_multiplier * 100).toFixed(0)}%`,
    `Bankroll: ${fmtCurrency(strategy.paper_bankroll_usd)}`,
    `Max single: ${(strategy.max_single_position_pct * 100).toFixed(0)}%`,
    `Max total: ${(strategy.max_total_exposure_pct * 100).toFixed(0)}%`,
  ];

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 18 }}>
      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>{strategy.display_name}</div>
      <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 12 }}>{strategy.objective}</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 12 }}>
        {rules.map((rule) => (
          <span
            key={rule}
            style={{
              padding: "4px 8px",
              borderRadius: 999,
              fontSize: 11,
              fontWeight: 700,
              background: "var(--bg)",
              color: "var(--text-dim)",
              border: "1px solid var(--border)",
            }}
          >
            {rule}
          </span>
        ))}
      </div>
      <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
        Observation window: minimum {strategy.minimum_observation_days} days, preferred {strategy.preferred_observation_days} days. Legacy benchmark rank threshold at least {strategy.legacy_benchmark_rank_threshold.toFixed(2)}.
      </div>
    </div>
  );
}

function StrategyRunPanel({ health }) {
  const strategyRun = health?.strategy_run;
  if (!strategyRun) return <EmptyState text="No active strategy run has been seeded yet." />;

  const rows = [
    ["Run ID", strategyRun.id],
    ["Strategy", strategyRun.strategy_name],
    ["Status", strategyRun.status?.toUpperCase?.() || "-"],
    ["Started", fmtDate(strategyRun.started_at)],
    ["Created", fmtDate(strategyRun.created_at)],
    ["Ended", fmtDate(strategyRun.ended_at)],
  ];

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <tbody>
          {rows.map(([label, value]) => (
            <tr key={label} style={{ borderTop: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 16px", color: "var(--text-dim)", width: 160 }}>{label}</td>
              <td style={{ padding: "10px 16px", fontFamily: label === "Run ID" ? "var(--mono)" : "inherit" }}>{value || "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ padding: "12px 16px", borderTop: "1px solid var(--border)", fontSize: 12, color: "var(--text-dim)" }}>
        This run ID is the immutable anchor for the prove-the-edge window. Scoped metrics, paper trades, and review artifacts should all point back to it.
      </div>
    </div>
  );
}

function ExecutionRealismPanel({ health, metrics }) {
  const realism = health?.execution_realism;
  if (!realism && !metrics) {
    return <EmptyState text="Execution-realism metrics are unavailable." />;
  }

  const source = realism || metrics;
  const cards = [
    {
      label: "Shadow P&L",
      value: fmtCurrency(source?.shadow_cumulative_pnl),
      color: pnlColor(source?.shadow_cumulative_pnl),
      sub: "Conservative P&L using half-spread penalties on entry.",
    },
    {
      label: "Shadow Profit Factor",
      value: source?.shadow_profit_factor == null ? "-" : Number(source.shadow_profit_factor).toFixed(2),
      sub: "Shadow wins divided by shadow losses.",
    },
    {
      label: "Liquidity Constrained",
      value: fmtWhole(source?.liquidity_constrained_trades),
      color: Number(source?.liquidity_constrained_trades || 0) > 0 ? "var(--yellow)" : "var(--green)",
      sub: "Trades where recommended size exceeded near-touch depth.",
    },
    {
      label: "Orderbook Gaps",
      value: fmtWhole(source?.trades_missing_orderbook_context),
      color: Number(source?.trades_missing_orderbook_context || 0) > 0 ? "var(--yellow)" : "var(--green)",
      sub: "Trades missing usable orderbook context for shadow fills.",
    },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
      {cards.map((card) => (
        <SummaryCard
          key={card.label}
          label={card.label}
          value={card.value}
          color={card.color}
          sub={card.sub}
        />
      ))}
    </div>
  );
}

function RunIntegrityPanel({ health }) {
  const integrity = health?.run_integrity;
  if (!integrity) {
    return <EmptyState text="Run integrity data is unavailable." />;
  }

  const items = [
    {
      label: "Pre-run signals",
      value: fmtWhole(integrity.pre_launch_candidate_signals),
      note: "Historical candidate signals excluded because they predate the active run start.",
    },
    {
      label: "Pre-run trades",
      value: fmtWhole(integrity.excluded_pre_launch_trades),
      note: "Historical paper trades excluded because they opened before the run boundary.",
    },
    {
      label: "Legacy trades",
      value: fmtWhole(integrity.excluded_legacy_trades),
      note: "Paper trades that do not carry the active strategy_run_id and therefore stay outside this sample.",
    },
    {
      label: "Orderbook coverage gaps",
      value: fmtWhole(integrity.trades_missing_orderbook_context),
      note: "Resolved trades without enough orderbook data to compute a shadow execution path.",
    },
  ];

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 18 }}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 14 }}>
        {items.map((item) => (
          <div key={item.label} style={{ border: "1px solid var(--border)", borderRadius: 10, padding: 14, background: "var(--bg)" }}>
            <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
              {item.label}
            </div>
            <div style={{ fontSize: 24, fontFamily: "var(--mono)", fontWeight: 700, marginBottom: 6 }}>{item.value}</div>
            <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>{item.note}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function FunnelCard({ label, value, sub, color }) {
  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 10, padding: 16 }}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ fontSize: 26, fontWeight: 700, fontFamily: "var(--mono)", color: color || "var(--text)" }}>
        {fmtWhole(value)}
      </div>
      <div style={{ marginTop: 6, fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
        {sub}
      </div>
    </div>
  );
}

function StrategyFunnel({ health }) {
  const funnel = health?.trade_funnel;
  if (!funnel) return <EmptyState text="Strategy funnel unavailable." />;

  const cards = [
    {
      label: "Candidate Path",
      value: funnel.candidate_signals,
      sub: "Signals on the frozen confluence path since the baseline start boundary.",
    },
    {
      label: "Qualified",
      value: funnel.qualified_signals,
      sub: "Signals that met the default strategy contract and were eligible to trade.",
      color: funnel.qualified_signals > 0 ? "var(--accent)" : "var(--text)",
    },
    {
      label: "Traded",
      value: funnel.traded_signals,
      sub: "Qualified signals that actually became default-strategy paper trades.",
      color: funnel.traded_signals > 0 ? "var(--green)" : "var(--text)",
    },
    {
      label: "Resolved",
      value: funnel.resolved_signals,
      sub: "Traded signals with a settled outcome and usable edge measurements.",
      color: funnel.resolved_signals > 0 ? "var(--green)" : "var(--text)",
    },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 12 }}>
      {cards.map((card) => (
        <FunnelCard
          key={card.label}
          label={card.label}
          value={card.value}
          sub={card.sub}
          color={card.color}
        />
      ))}
      <FunnelCard
        label="Not Traded"
        value={funnel.qualified_not_traded}
        sub="Qualified signals that never entered the measured baseline."
        color={funnel.qualified_not_traded > 0 ? "var(--yellow)" : "var(--text)"}
      />
      <FunnelCard
        label="Legacy Excluded"
        value={funnel.excluded_legacy_trades}
        sub="Older paper trades that stay outside the frozen default-strategy sample."
        color={funnel.excluded_legacy_trades > 0 ? "var(--yellow)" : "var(--text)"}
      />
      <FunnelCard
        label="Pre-Launch Signals"
        value={funnel.pre_launch_candidate_signals}
        sub="Historical confluence signals that were intentionally left out of the prove-the-edge window."
        color={funnel.pre_launch_candidate_signals > 0 ? "var(--text-dim)" : "var(--green)"}
      />
    </div>
  );
}

function SkipReasonsPanel({ rows }) {
  if (!rows || rows.length === 0) {
    return <EmptyState text="No in-window skip reasons yet. Once the scheduler evaluates baseline signals, reasons will appear here." />;
  }

  const total = rows.reduce((sum, row) => sum + Number(row.count || 0), 0);

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Reason</th>
            <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Count</th>
            <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Share</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const share = total > 0 ? Number(row.count || 0) / total : 0;
            return (
              <tr key={row.reason_code} style={{ borderTop: "1px solid var(--border)" }}>
                <td style={{ padding: "10px 16px" }}>{row.reason_label}</td>
                <td style={{ textAlign: "right", padding: "10px 16px", fontFamily: "var(--mono)", fontWeight: 700 }}>
                  {fmtWhole(row.count)}
                </td>
                <td style={{ textAlign: "right", padding: "10px 16px", fontFamily: "var(--mono)", color: "var(--text-dim)" }}>
                  {fmtPercent(share)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <div style={{ padding: "12px 16px", borderTop: "1px solid var(--border)", fontSize: 12, color: "var(--text-dim)" }}>
        These counts cover in-window default-strategy path signals that did not become a paper trade.
      </div>
    </div>
  );
}

function BenchmarkComparison({ health }) {
  const headline = health?.headline;
  const benchmark = health?.benchmark;
  if (!headline || !benchmark) return <EmptyState text="Benchmark comparison unavailable." />;
  const hasResolvedEvidence = Number(headline.resolved_signals || 0) > 0 || Number(benchmark.resolved_signals || 0) > 0;

  const rows = [
    ["Resolved Signals", headline.resolved_signals, benchmark.resolved_signals],
    ["Win Rate", fmtPercent(headline.win_rate), fmtPercent(benchmark.win_rate)],
    ["Avg CLV", fmtCents(headline.avg_clv), fmtCents(benchmark.avg_clv)],
    ["1-Share P&L", fmtCents(headline.total_profit_loss_per_share), fmtCents(benchmark.total_profit_loss_per_share)],
    ["1-Share Max Drawdown", fmtCents(headline.max_drawdown_per_share), fmtCents(benchmark.max_drawdown_per_share)],
  ];

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Metric</th>
            <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Default Strategy</th>
            <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-dim)", fontWeight: 500 }}>Legacy Benchmark</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([label, current, legacy]) => (
            <tr key={label} style={{ borderTop: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 16px" }}>{label}</td>
              <td style={{ textAlign: "right", padding: "10px 16px", fontFamily: "var(--mono)" }}>{current}</td>
              <td style={{ textAlign: "right", padding: "10px 16px", fontFamily: "var(--mono)", color: "var(--text-dim)" }}>{legacy}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ padding: "12px 16px", borderTop: "1px solid var(--border)", fontSize: 12, color: pnlColor(benchmark.delta_profit_loss_per_share) }}>
        {hasResolvedEvidence
          ? `P&L delta vs legacy: ${fmtCents(benchmark.delta_profit_loss_per_share)} | Drawdown delta: ${fmtCents(benchmark.delta_max_drawdown_per_share)}`
          : "No resolved baseline or benchmark signals yet. Start using this comparison after the first settlements land."}
      </div>
    </div>
  );
}

function DetectorReviewTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <EmptyState text="No detector verdicts yet. Resolved signals will populate this review table." />;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 900, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Detector</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Resolved</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Paper Trades</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Avg CLV</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>1-Share P&L</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Trade P&L</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Brier</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Verdict</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.signal_type} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 14px" }}>
                <div style={{ fontWeight: 700, color: "var(--accent)", textTransform: "uppercase", fontSize: 11 }}>
                  {row.signal_type.replace(/_/g, " ")}
                </div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 2 }}>{row.note}</div>
              </td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{row.resolved_signals}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{row.paper_trades}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: pnlColor(row.avg_clv) }}>{fmtCents(row.avg_clv)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: pnlColor(row.total_profit_loss) }}>{fmtCents(row.total_profit_loss)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: pnlColor(row.paper_trade_pnl) }}>{fmtCurrency(row.paper_trade_pnl)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{row.brier_score == null ? "-" : row.brier_score.toFixed(4)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px" }}>
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    padding: "2px 8px",
                    borderRadius: 999,
                    color: "#fff",
                    background: verdictColor(row.verdict),
                    textTransform: "uppercase",
                  }}
                >
                  {row.verdict}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RecentMistakesTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <EmptyState text="No resolved losing trades yet. Recent mistakes will show up here once the baseline takes losses." />;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 720, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Market</th>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Signal</th>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Direction</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>P&L</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>CLV</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Resolved</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.trade_id} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 14px", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{row.market_question || row.signal_id}</td>
              <td style={{ padding: "10px 14px", fontWeight: 700, color: "var(--accent)", textTransform: "uppercase", fontSize: 11 }}>{row.signal_type.replace(/_/g, " ")}</td>
              <td style={{ padding: "10px 14px", color: directionColor(row.direction), fontWeight: 700 }}>{directionLabel(row.direction)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: pnlColor(row.pnl) }}>{fmtCurrency(row.pnl)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", color: pnlColor(row.clv) }}>{fmtCents(row.clv)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontSize: 12, color: "var(--text-dim)" }}>{fmtDate(row.resolved_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TradeTable({ trades, showStatus }) {
  if (!trades || trades.length === 0) {
    return <EmptyState text={showStatus ? "No paper trades match the current filters." : "No open paper trades yet."} />;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 880, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Market</th>
            <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Direction</th>
            {showStatus && <th style={{ textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Status</th>}
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Size</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Entry</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Exit</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>EV / Share</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>P&L</th>
            <th style={{ textAlign: "right", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 }}>Opened</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade) => (
            <tr key={trade.id} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 14px", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {trade.details?.market_question || trade.market_id.slice(0, 8)}
              </td>
              <td style={{ padding: "10px 14px", fontWeight: 700, color: directionColor(trade.direction) }}>
                {directionLabel(trade.direction)}
              </td>
              {showStatus && (
                <td style={{ padding: "10px 14px", textTransform: "uppercase", fontSize: 11, color: trade.status === "resolved" ? "var(--green)" : "var(--yellow)" }}>
                  {trade.status}
                </td>
              )}
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtCurrency(trade.size_usd)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtCurrency(trade.entry_price)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>{fmtCurrency(trade.exit_price)}</td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" }}>
                {trade.details?.ev_per_share == null ? "-" : `${(Math.abs(Number(trade.details.ev_per_share)) * 100).toFixed(1)}c`}
              </td>
              <td style={{ textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)", fontWeight: 700, color: pnlColor(trade.pnl) }}>
                {fmtCurrency(trade.pnl)}
              </td>
              <td style={{ textAlign: "right", padding: "10px 14px", color: "var(--text-dim)", fontSize: 12 }}>
                {fmtDate(trade.opened_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Pagination({ page, totalPages, onPageChange }) {
  if (totalPages <= 1) return null;

  return (
    <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 12, marginTop: 14 }}>
      <button
        onClick={() => onPageChange(page - 1)}
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
        onClick={() => onPageChange(page + 1)}
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
  );
}

export default function PaperTrading() {
  const [portfolio, setPortfolio] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [curve, setCurve] = useState(null);
  const [strategyHealth, setStrategyHealth] = useState(null);
  const [history, setHistory] = useState(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [directionFilter, setDirectionFilter] = useState("");
  const [page, setPage] = useState(1);
  const [dashboardError, setDashboardError] = useState(null);
  const [historyError, setHistoryError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  const loadDashboard = useCallback(async ({ silent = false } = {}) => {
    if (!silent) setRefreshing(true);
    try {
      const dashboard = await getPaperTradingDefaultStrategyDashboard();
      setPortfolio(dashboard.portfolio);
      setMetrics(dashboard.metrics);
      setCurve(dashboard.pnl_curve);
      setStrategyHealth(dashboard.strategy_health);
      setDashboardError(null);
      setLastUpdated(new Date().toISOString());
    } catch (err) {
      setDashboardError(err.message);
    } finally {
      if (!silent) setRefreshing(false);
    }
  }, []);

  const loadHistory = useCallback(async () => {
    try {
      const data = await getPaperTradingHistory({
      status: statusFilter || undefined,
      direction: directionFilter || undefined,
      scope: STRATEGY_SCOPE,
      page,
      pageSize: PAGE_SIZE,
      });
      setHistory(data);
      setHistoryError(null);
    } catch (err) {
      setHistoryError(err.message);
    }
  }, [directionFilter, page, statusFilter]);

  const refreshAll = useCallback(async () => {
    setRefreshing(true);
    try {
      await Promise.all([
        loadDashboard({ silent: true }),
        loadHistory(),
      ]);
      setLastUpdated(new Date().toISOString());
    } finally {
      setRefreshing(false);
    }
  }, [loadDashboard, loadHistory]);

  useEffect(() => {
    loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    loadHistory();
  }, [loadHistory]);

  useEffect(() => {
    const interval = setInterval(() => {
      loadDashboard({ silent: true });
      loadHistory();
    }, 30_000);
    return () => clearInterval(interval);
  }, [loadDashboard, loadHistory]);

  const totalPages = useMemo(() => {
    if (!history) return 1;
    return Math.max(1, Math.ceil(history.total / PAGE_SIZE));
  }, [history]);

  const headline = strategyHealth?.headline;
  const funnel = strategyHealth?.trade_funnel;
  const strategyRun = strategyHealth?.strategy_run;
  const executionRealism = strategyHealth?.execution_realism;
  const brief = useMemo(
    () => (strategyHealth ? buildStrategyBrief(strategyHealth) : null),
    [strategyHealth]
  );
  const summaryCards = useMemo(() => {
    if (!strategyHealth) return [];
    return [
      {
        label: "Cumulative P&L",
        value: fmtCurrency(headline?.cumulative_pnl),
        color: pnlColor(headline?.cumulative_pnl),
        sub: Number(headline?.resolved_trades || 0) > 0 ? "Resolved paper-trade outcome so far" : "No resolved paper trades yet",
      },
      {
        label: "Observation Window",
        value: strategyHealth.observation?.days_tracked == null ? "0.0d" : `${Number(strategyHealth.observation.days_tracked).toFixed(1)}d`,
        sub: `Minimum ${strategyHealth.observation?.minimum_days || 0}d, preferred ${strategyHealth.observation?.preferred_days || 0}d`,
      },
      {
        label: "Active Run",
        value: strategyRun?.id ? `${String(strategyRun.id).slice(0, 8)}...` : "-",
        sub: strategyRun?.started_at ? `Immutable start ${fmtDate(strategyRun.started_at)}` : "No active run seeded yet",
      },
      {
        label: "Open Exposure",
        value: fmtCurrency(headline?.open_exposure),
        sub: `${((brief?.exposurePct || 0) * 100).toFixed(0)}% of bankroll across ${fmtWhole(headline?.open_trades)} live trades`,
      },
      {
        label: "Qualified Signals",
        value: fmtWhole(funnel?.qualified_signals),
        sub: `${fmtWhole(funnel?.traded_signals)} traded from the frozen baseline since launch`,
      },
      {
        label: "Avg CLV",
        value: fmtCents(headline?.avg_clv),
        color: pnlColor(headline?.avg_clv),
        sub: headline?.avg_clv == null ? "Wait for settled signals" : "Average closing-line edge on the baseline",
      },
      {
        label: "Resolved Evidence",
        value: fmtWhole(headline?.resolved_trades),
        sub: `${fmtWhole(headline?.resolved_signals)} resolved traded signals`,
      },
      {
        label: "Shadow P&L",
        value: fmtCurrency(executionRealism?.shadow_cumulative_pnl),
        color: pnlColor(executionRealism?.shadow_cumulative_pnl),
        sub: "Conservative execution realism line from shadow fills",
      },
      {
        label: "Qualified Not Traded",
        value: fmtWhole(funnel?.qualified_not_traded),
        color: Number(funnel?.qualified_not_traded || 0) > 0 ? "var(--yellow)" : "var(--green)",
        sub: "Qualified signals that never entered the measured baseline",
      },
      {
        label: "Legacy Trades Excluded",
        value: fmtWhole(funnel?.excluded_legacy_trades),
        color: Number(funnel?.excluded_legacy_trades || 0) > 0 ? "var(--yellow)" : "var(--green)",
        sub: "Historical paper trades kept outside the frozen strategy read",
      },
      {
        label: "Orderbook Gaps",
        value: fmtWhole(executionRealism?.trades_missing_orderbook_context),
        color: Number(executionRealism?.trades_missing_orderbook_context || 0) > 0 ? "var(--yellow)" : "var(--green)",
        sub: "Resolved trades missing shadow execution context",
      },
    ];
  }, [brief, executionRealism, funnel, headline, strategyHealth, strategyRun]);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16, marginBottom: 18, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>
            Strategy Console
          </div>
          <h2 style={{ fontSize: 26, fontWeight: 700, marginBottom: 6 }}>Default Strategy Health</h2>
          <div style={{ fontSize: 13, color: "var(--text-dim)", maxWidth: 720 }}>
            Paper trading is the source of truth for the prove-the-edge phase. This page should tell you whether the frozen baseline is making money, whether the evidence is trustworthy, and what to review next.
          </div>
        </div>
        <RefreshButton refreshing={refreshing} onRefresh={refreshAll} lastUpdated={lastUpdated} />
      </div>

      {(dashboardError || historyError) && (
        <div
          style={{
            marginBottom: 20,
            padding: "12px 14px",
            borderRadius: 8,
            background: "rgba(239, 68, 68, 0.08)",
            border: "1px solid rgba(239, 68, 68, 0.25)",
            color: "var(--red)",
            fontSize: 13,
          }}
        >
          {dashboardError || historyError}
        </div>
      )}

      {strategyHealth ? (
        <>
          <OperatorBrief health={strategyHealth} />
          <ObservationBanner observation={strategyHealth.observation} />
          <ReviewVerdictPanel health={strategyHealth} />
          <LatestReviewArtifactPanel artifact={strategyHealth.latest_review_artifact} freshness={strategyHealth.evidence_freshness} />
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12, marginBottom: 28 }}>
            {summaryCards.map((card) => (
              <SummaryCard
                key={card.label}
                label={card.label}
                value={card.value}
                color={card.color}
                sub={card.sub}
              />
            ))}
          </div>
        </>
      ) : dashboardError ? (
        <div style={{ marginBottom: 28 }}>
          <EmptyState text="Unable to load the default strategy summary right now." />
        </div>
      ) : (
        <div className="skeleton" style={{ height: 160, borderRadius: 8, marginBottom: 28 }} />
      )}

      <Section title="Default vs Legacy Benchmark">
        {strategyHealth ? <BenchmarkComparison health={strategyHealth} /> : <div className="skeleton" style={{ height: 220, borderRadius: 8 }} />}
      </Section>

      <Section title="Active Strategy Run">
        {strategyHealth ? <StrategyRunPanel health={strategyHealth} /> : <div className="skeleton" style={{ height: 180, borderRadius: 8 }} />}
      </Section>

      <Section title="Strategy Funnel">
        {strategyHealth ? <StrategyFunnel health={strategyHealth} /> : <div className="skeleton" style={{ height: 180, borderRadius: 8 }} />}
      </Section>

      <Section title="Run Integrity">
        {strategyHealth ? <RunIntegrityPanel health={strategyHealth} /> : <div className="skeleton" style={{ height: 180, borderRadius: 8 }} />}
      </Section>

      <Section title="Why Trades Were Skipped">
        {strategyHealth ? <SkipReasonsPanel rows={strategyHealth.skip_reasons} /> : <div className="skeleton" style={{ height: 180, borderRadius: 8 }} />}
      </Section>

      <Section title="Detector Verdicts">
        {strategyHealth ? <DetectorReviewTable rows={strategyHealth.detector_review} /> : <div className="skeleton" style={{ height: 220, borderRadius: 8 }} />}
      </Section>

      <Section title="Recent Mistakes">
        {strategyHealth ? <RecentMistakesTable rows={strategyHealth.recent_mistakes} /> : <div className="skeleton" style={{ height: 160, borderRadius: 8 }} />}
      </Section>

      <Section title="Weekly Review Questions">
        {strategyHealth ? (
          <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: 18 }}>
            <ul style={{ margin: 0, paddingLeft: 18, color: "var(--text)", fontSize: 13, lineHeight: 1.7 }}>
              {strategyHealth.review_questions.map((question) => (
                <li key={question}>{question}</li>
              ))}
            </ul>
          </div>
        ) : (
          <div className="skeleton" style={{ height: 110, borderRadius: 8 }} />
        )}
      </Section>

      <Section title="Default Strategy Contract">
        {strategyHealth ? <StrategyContract health={strategyHealth} /> : <div className="skeleton" style={{ height: 150, borderRadius: 8 }} />}
      </Section>

      <Section title="Cumulative P&L">
        {curve ? (
          <PnlChart curve={curve} />
        ) : dashboardError ? (
          <EmptyState text="Unable to load the P&L curve right now." />
        ) : (
          <div className="skeleton" style={{ height: 260, borderRadius: 8 }} />
        )}
      </Section>

      <Section title="Execution Metrics">
        {metrics ? (
          metrics.total_trades > 0 ? (
            <MetricStrip metrics={metrics} />
          ) : (
            <EmptyState text="No resolved paper trades yet. Execution metrics will appear once trades settle." />
          )
        ) : dashboardError ? (
          <EmptyState text="Unable to load execution metrics right now." />
        ) : (
          <div className="skeleton" style={{ height: 100, borderRadius: 8 }} />
        )}
      </Section>

      <Section title="Execution Realism">
        {strategyHealth || metrics ? (
          <ExecutionRealismPanel health={strategyHealth} metrics={metrics} />
        ) : dashboardError ? (
          <EmptyState text="Unable to load execution-realism metrics right now." />
        ) : (
          <div className="skeleton" style={{ height: 120, borderRadius: 8 }} />
        )}
      </Section>

      <Section title="Open Trades">
        {portfolio ? (
          <TradeTable trades={portfolio.open_trades} showStatus={false} />
        ) : dashboardError ? (
          <EmptyState text="Unable to load open paper trades right now." />
        ) : (
          <div className="skeleton" style={{ height: 180, borderRadius: 8 }} />
        )}
      </Section>

      <Section
        title="Trade History"
        actions={(
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <select
              value={statusFilter}
              onChange={(event) => {
                setStatusFilter(event.target.value);
                setPage(1);
              }}
              style={{
                padding: "6px 12px",
                fontSize: 13,
                background: "var(--bg-card)",
                color: "var(--text)",
                border: "1px solid var(--border)",
                borderRadius: 6,
              }}
            >
              <option value="">All statuses</option>
              <option value="open">Open</option>
              <option value="resolved">Resolved</option>
            </select>
            <select
              value={directionFilter}
              onChange={(event) => {
                setDirectionFilter(event.target.value);
                setPage(1);
              }}
              style={{
                padding: "6px 12px",
                fontSize: 13,
                background: "var(--bg-card)",
                color: "var(--text)",
                border: "1px solid var(--border)",
                borderRadius: 6,
              }}
            >
              <option value="">All directions</option>
              <option value="buy_yes">BUY YES</option>
              <option value="buy_no">BUY NO</option>
            </select>
          </div>
        )}
      >
        {history ? (
          <>
            <TradeTable trades={history.trades} showStatus />
            <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
          </>
        ) : historyError ? (
          <EmptyState text="Unable to load paper trade history right now." />
        ) : (
          <div className="skeleton" style={{ height: 220, borderRadius: 8 }} />
        )}
      </Section>
    </div>
  );
}
