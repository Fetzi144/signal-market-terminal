import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getStrategiesRegistry } from "../api";

function fmtDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function badgeColor(value) {
  if (value === "benchmark" || value === "fresh") return "var(--green)";
  if (value === "promoted" || value === "assisted_live") return "var(--yellow)";
  if (value === "demoted" || value === "disabled") return "var(--red)";
  return "var(--text-dim)";
}

function titleCase(value) {
  if (!value) return "-";
  return String(value).replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function MetricCard({ label, value, hint }) {
  return (
    <div style={metricCardStyle}>
      <div style={metricLabelStyle}>{label}</div>
      <div style={metricValueStyle}>{value}</div>
      {hint ? <div style={metricHintStyle}>{hint}</div> : null}
    </div>
  );
}

function Badge({ value }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "4px 8px",
        borderRadius: 999,
        border: `1px solid ${badgeColor(value)}`,
        color: badgeColor(value),
        fontSize: 11,
        fontWeight: 600,
      }}
    >
      {titleCase(value)}
    </span>
  );
}

function EvidencePill({ label, value }) {
  return (
    <div style={evidencePillStyle}>
      <span style={evidencePillLabelStyle}>{label}</span>
      <span style={evidencePillValueStyle}>{value}</span>
    </div>
  );
}

function SimpleTable({ columns, rows, emptyLabel }) {
  return (
    <div className="table-scroll">
      <table style={tableStyle}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} style={tableHeadStyle}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={emptyCellStyle}>{emptyLabel}</td>
            </tr>
          ) : rows.map((row, rowIndex) => (
            <tr key={`${columns[0]}-${rowIndex}`} style={tableRowStyle}>
              {row.map((cell, cellIndex) => (
                <td key={`${columns[0]}-${rowIndex}-${cellIndex}`} style={tableCellStyle}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Strategies() {
  const [registry, setRegistry] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const payload = await getStrategiesRegistry();
      setRegistry(payload);
      setError(null);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const summary = registry?.summary || {};
  const families = registry?.families || [];
  const gatePolicies = registry?.gate_policies || [];

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div>
          <h2 style={titleStyle}>Strategies</h2>
          <p style={subtitleStyle}>
            Phase 13A lifecycle registry for benchmark, candidate, promoted, and demoted versions.
          </p>
        </div>
        <button onClick={load} style={secondaryButtonStyle}>
          {loading ? "Refreshing..." : "Refresh"}
        </button>
      </div>

      {error ? <div style={alertStyle}>{error}</div> : null}

      <section style={panelStyle}>
        <div style={statsGridStyle}>
          <MetricCard label="Phase" value={summary.phase || "13A"} hint="Registry-only slice" />
          <MetricCard label="Families" value={summary.family_count ?? 0} hint="Strategy + infra rows" />
          <MetricCard label="Versions" value={summary.version_count ?? 0} hint="Current seeded lifecycle versions" />
          <MetricCard label="Gate Policies" value={summary.gate_policy_count ?? 0} hint="Inspectable policy versions" />
        </div>
      </section>

      <section style={panelStyle}>
        <div style={sectionHeaderStyle}>
          <div>
            <h3 style={sectionTitleStyle}>Registry Scope</h3>
            <div style={metaStyle}>
              The frozen benchmark stays visible here while autonomy work advances elsewhere.
            </div>
          </div>
          <div style={linkRowStyle}>
            <Link to="/paper-trading" style={inlineLinkStyle}>Benchmark Health</Link>
            <Link to="/pilot-console" style={inlineLinkStyle}>Pilot Console</Link>
          </div>
        </div>
        <SimpleTable
          columns={["Policy", "Status", "Notes", "Updated"]}
          rows={gatePolicies.map((policy) => ([
            policy.label,
            <Badge key={`${policy.policy_key}-status`} value={policy.status} />,
            Array.isArray(policy.policy_json?.required_inputs)
              ? `${policy.policy_json.required_inputs.length} tracked gate inputs`
              : "Seeded policy",
            fmtDate(policy.updated_at),
          ]))}
          emptyLabel="No gate policies registered yet."
        />
      </section>

      {families.map((family) => {
        const currentVersion = family.current_version;
        const evidence = currentVersion?.evidence_counts || {};
        const latestEvaluation = family.latest_promotion_evaluation;
        const evaluationSummary = latestEvaluation?.summary_json || {};
        const evaluationProvenance = latestEvaluation?.provenance_json || {};
        return (
          <section key={family.family} style={panelStyle}>
            <div style={sectionHeaderStyle}>
              <div>
                <div style={familyHeaderRowStyle}>
                  <h3 style={sectionTitleStyle}>{family.label}</h3>
                  <Badge value={family.posture} />
                  <Badge value={family.family_kind} />
                </div>
                <div style={metaStyle}>
                  {family.description}
                </div>
                {family.disabled_reason ? (
                  <div style={{ ...metaStyle, color: "var(--red)" }}>
                    {family.disabled_reason}
                  </div>
                ) : null}
              </div>
              <div style={linkRowStyle}>
                {family.primary_surface === "paper_trading" ? (
                  <Link to="/paper-trading" style={inlineLinkStyle}>Open Surface</Link>
                ) : null}
                {family.primary_surface === "pilot_console" ? (
                  <Link to="/pilot-console" style={inlineLinkStyle}>Open Surface</Link>
                ) : null}
                {family.primary_surface === "structure" ? (
                  <Link to="/structures" style={inlineLinkStyle}>Open Surface</Link>
                ) : null}
              </div>
            </div>

            <div style={statsGridStyle}>
              <MetricCard
                label="Current Version"
                value={currentVersion?.version_label || "Missing"}
                hint={currentVersion?.version_key || "No seeded version"}
              />
              <MetricCard
                label="Lifecycle State"
                value={titleCase(currentVersion?.version_status)}
                hint={titleCase(currentVersion?.autonomy_tier)}
              />
              <MetricCard
                label="Evidence Links"
                value={
                  (evidence.strategy_runs || 0)
                  + (evidence.paper_trades || 0)
                  + (evidence.replay_runs || 0)
                  + (evidence.live_orders || 0)
                }
                hint="Run, paper, replay, and live links"
              />
              <MetricCard
                label="Last Registry Sync"
                value={fmtDate(family.updated_at)}
                hint={family.seeded_from}
              />
            </div>

            <div style={evidenceWrapStyle}>
              <EvidencePill label="Runs" value={evidence.strategy_runs ?? 0} />
              <EvidencePill label="Paper Trades" value={evidence.paper_trades ?? 0} />
              <EvidencePill label="Replay Runs" value={evidence.replay_runs ?? 0} />
              <EvidencePill label="Live Orders" value={evidence.live_orders ?? 0} />
              <EvidencePill label="Scorecards" value={evidence.pilot_scorecards ?? 0} />
              <EvidencePill label="Readiness" value={evidence.readiness_reports ?? 0} />
            </div>

            <div style={promotionPanelStyle}>
              <div style={promotionHeaderStyle}>
                <div style={sectionTitleStyle}>Latest Gate Verdict</div>
                {latestEvaluation ? <Badge value={latestEvaluation.evaluation_status} /> : null}
              </div>
              {latestEvaluation ? (
                <div style={statsGridStyle}>
                  <MetricCard
                    label="Recommended Tier"
                    value={titleCase(latestEvaluation.autonomy_tier)}
                    hint={titleCase(evaluationSummary.readiness_status)}
                  />
                  <MetricCard
                    label="Policy"
                    value={evaluationProvenance.promotion_gate_policy_key || "Unknown"}
                    hint={`Window ends ${fmtDate(latestEvaluation.evaluation_window_end)}`}
                  />
                  <MetricCard
                    label="Gate Summary"
                    value={evaluationSummary.readiness_blockers?.length ? `${evaluationSummary.readiness_blockers.length} blockers` : "No blockers"}
                    hint={`Incidents ${evaluationSummary.incident_count ?? 0} | Backlog ${evaluationSummary.approval_backlog_count ?? 0}`}
                  />
                  <MetricCard
                    label="Provenance"
                    value={evaluationProvenance.config_hash || "Unhashed"}
                    hint={evaluationProvenance.market_universe_hash || "No market-universe hash"}
                  />
                </div>
              ) : (
                <div style={metaStyle}>
                  No promotion evaluation has been recorded yet for this family.
                </div>
              )}
            </div>

            <SimpleTable
              columns={["Version", "State", "Tier", "Frozen", "Evidence", "Updated"]}
              rows={family.versions.map((version) => ([
                <div key={version.version_key}>
                  <div>{version.version_label}</div>
                  <div style={subtleCellStyle}>{version.version_key}</div>
                </div>,
                <Badge key={`${version.version_key}-state`} value={version.version_status} />,
                <Badge key={`${version.version_key}-tier`} value={version.autonomy_tier} />,
                version.is_frozen ? "Yes" : "No",
                `${(version.evidence_counts.strategy_runs || 0)} run / ${(version.evidence_counts.paper_trades || 0)} paper / ${(version.evidence_counts.live_orders || 0)} live`,
                fmtDate(version.updated_at),
              ]))}
              emptyLabel="No versions registered for this family."
            />
          </section>
        );
      })}
    </div>
  );
}

const pageStyle = {
  display: "flex",
  flexDirection: "column",
  gap: 20,
};

const headerStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "flex-start",
  gap: 12,
  flexWrap: "wrap",
};

const titleStyle = { fontSize: 16, margin: 0 };
const subtitleStyle = { margin: "6px 0 0", fontSize: 12, color: "var(--text-dim)" };
const panelStyle = { background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 12, padding: 16 };
const sectionHeaderStyle = { display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 12 };
const familyHeaderRowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginBottom: 6 };
const sectionTitleStyle = { fontSize: 14, fontWeight: 600, margin: 0 };
const metaStyle = { fontSize: 12, color: "var(--text-dim)" };
const linkRowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" };
const inlineLinkStyle = { color: "var(--green)", fontSize: 12, textDecoration: "none" };
const statsGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12 };
const metricCardStyle = { background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 };
const metricLabelStyle = { fontSize: 11, color: "var(--text-dim)", marginBottom: 4 };
const metricValueStyle = { fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600 };
const metricHintStyle = { fontSize: 11, color: "var(--text-dim)", marginTop: 6 };
const evidenceWrapStyle = { display: "flex", gap: 8, flexWrap: "wrap", margin: "14px 0" };
const promotionPanelStyle = { display: "flex", flexDirection: "column", gap: 12, margin: "0 0 16px" };
const promotionHeaderStyle = { display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" };
const evidencePillStyle = { display: "inline-flex", alignItems: "center", gap: 8, borderRadius: 999, border: "1px solid var(--border)", padding: "6px 10px", fontSize: 11 };
const evidencePillLabelStyle = { color: "var(--text-dim)" };
const evidencePillValueStyle = { fontFamily: "var(--mono)", fontWeight: 600 };
const secondaryButtonStyle = { background: "transparent", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const alertStyle = { border: "1px solid var(--red)", color: "var(--red)", borderRadius: 10, padding: "10px 12px", fontSize: 12, background: "rgba(255, 255, 255, 0.03)" };
const tableStyle = { width: "100%", borderCollapse: "collapse" };
const tableHeadStyle = { padding: "0 0 10px", textAlign: "left", fontSize: 11, color: "var(--text-dim)", fontWeight: 500, whiteSpace: "nowrap" };
const tableRowStyle = { borderTop: "1px solid var(--border)" };
const tableCellStyle = { padding: "10px 10px 10px 0", fontSize: 12, verticalAlign: "top" };
const subtleCellStyle = { fontSize: 11, color: "var(--text-dim)", marginTop: 4 };
const emptyCellStyle = { padding: "16px 0 4px", fontSize: 12, color: "var(--text-dim)" };
