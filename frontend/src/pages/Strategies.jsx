import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getStrategiesRegistry, getStrategyVersionDetail } from "../api";

function fmtDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function fmtMetric(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toFixed(digits) : String(value);
}

function badgeColor(value) {
  if (value === "benchmark" || value === "fresh" || value === "candidate" || value === "complete") return "var(--green)";
  if (value === "promoted" || value === "assisted_live" || value === "observe" || value === "partial") return "var(--yellow)";
  if (value === "demoted" || value === "disabled" || value === "blocked") return "var(--red)";
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

function evaluationStateHint(evaluation) {
  const summary = evaluation?.summary_json || {};
  if (evaluation?.evaluation_kind === "replay_gate") return titleCase(summary.replay_status);
  return titleCase(summary.readiness_status);
}

function evaluationSummaryValue(evaluation) {
  const summary = evaluation?.summary_json || {};
  if (evaluation?.evaluation_kind === "replay_gate") {
    const variantCount = Number(summary.variant_count || 0);
    return variantCount > 0 ? `${variantCount} variants` : "No variants";
  }
  const blockers = Array.isArray(summary.readiness_blockers) ? summary.readiness_blockers : [];
  return blockers.length > 0 ? `${blockers.length} blockers` : "No blockers";
}

function evaluationSummaryHint(evaluation) {
  const summary = evaluation?.summary_json || {};
  if (evaluation?.evaluation_kind === "replay_gate") {
    return `${titleCase(summary.primary_variant)} | Net ${fmtMetric(summary.primary_variant_net_pnl)} | Coverage ${summary.coverage_limited_scenarios ?? 0}`;
  }
  return `Incidents ${summary.incident_count ?? 0} | Backlog ${summary.approval_backlog_count ?? 0}`;
}

function evaluationSourceHint(evaluation) {
  const provenance = evaluation?.provenance_json || {};
  if (evaluation?.evaluation_kind === "replay_gate") {
    return provenance.replay_run_key || provenance.source || "-";
  }
  return provenance.readiness_report_id ? `Report ${provenance.readiness_report_id}` : (provenance.source || "-");
}

function gateHistorySummary(evaluation) {
  const summary = evaluation?.summary_json || {};
  if (evaluation?.evaluation_kind === "replay_gate") {
    return `${titleCase(summary.replay_status)} | ${summary.variant_count ?? 0} variants`;
  }
  if (evaluation?.evaluation_kind === "pilot_readiness_gate") {
    const blockers = Array.isArray(summary.readiness_blockers) ? summary.readiness_blockers.length : 0;
    return `${titleCase(summary.readiness_status)} | ${blockers} blockers`;
  }
  if (evaluation?.evaluation_kind === "scorecard_gate") {
    return `${titleCase(summary.scorecard_status)} | Net ${fmtMetric(summary.net_pnl)} | Incidents ${summary.incident_count ?? 0}`;
  }
  if (evaluation?.evaluation_kind === "incident_gate") {
    return `${summary.incident_count_24h ?? 0} incidents | Latest ${titleCase(summary.latest_incident_type)}`;
  }
  if (evaluation?.evaluation_kind === "guardrail_gate") {
    return `${summary.guardrail_count_24h ?? 0} guardrails | Latest ${titleCase(summary.latest_guardrail_type)}`;
  }
  if (evaluation?.evaluation_kind === "capital_budget_gate") {
    return `${titleCase(summary.capacity_status)} | Regime ${titleCase(summary.regime_label)} | ${summary.reason_codes?.join(", ") || "No breach"}`;
  }
  return "-";
}

function gateHistoryObservedAt(evaluation) {
  return evaluation?.evaluation_window_end || evaluation?.updated_at || evaluation?.created_at || null;
}

function budgetUsedHint(status) {
  if (!status) return "-";
  return `${fmtMetric(status.current_outstanding_usd)} / ${fmtMetric(status.effective_outstanding_cap_usd)} used`;
}

function budgetCapacityHint(status) {
  if (!status) return "-";
  return `Regime ${titleCase(status.regime_label)} | ${titleCase(status.capacity_status)}`;
}

function renderEmptySurface(label) {
  return <span style={metaStyle}>{label}</span>;
}

function renderGateSurface(evaluation) {
  if (!evaluation) return renderEmptySurface("No gate");
  return (
    <div>
      <Badge value={evaluation.evaluation_status} />
      <div style={subtleCellStyle}>{titleCase(evaluation.evaluation_kind)}</div>
      <div style={subtleCellStyle}>{titleCase(evaluation.autonomy_tier)}</div>
    </div>
  );
}

function renderReplaySurface(alignment) {
  const replay = alignment?.latest_replay_run;
  if (!replay) return renderEmptySurface("No replay");
  return (
    <div>
      <div>{titleCase(replay.status)}</div>
      <div style={subtleCellStyle}>{replay.run_key}</div>
      <div style={subtleCellStyle}>
        {replay.scenario_count} scenarios | {fmtDate(replay.completed_at || replay.started_at)}
      </div>
    </div>
  );
}

function renderLiveShadowSurface(alignment) {
  const liveShadow = alignment?.live_shadow;
  if (!liveShadow) return renderEmptySurface("No live shadow");
  return (
    <div>
      <div>{liveShadow.recent_count_24h ?? 0} recent evals</div>
      <div style={subtleCellStyle}>
        Avg gap {fmtMetric(liveShadow.average_gap_bps_24h)} bps | Breaches {liveShadow.breach_count_24h ?? 0}
      </div>
      <div style={subtleCellStyle}>
        Coverage-limited {liveShadow.coverage_limited_count_24h ?? 0} | Latest {fmtDate(liveShadow.latest_updated_at)}
      </div>
    </div>
  );
}

function renderScorecardSurface(alignment) {
  const scorecard = alignment?.latest_scorecard;
  if (!scorecard) return renderEmptySurface("No scorecard");
  return (
    <div>
      <div>{titleCase(scorecard.status)}</div>
      <div style={subtleCellStyle}>
        Net {fmtMetric(scorecard.net_pnl)} | Gap {fmtMetric(scorecard.avg_shadow_gap_bps)} bps
      </div>
      <div style={subtleCellStyle}>
        Coverage {scorecard.coverage_limited_count ?? 0} | {fmtDate(scorecard.window_end || scorecard.created_at)}
      </div>
    </div>
  );
}

function renderReadinessSurface(alignment) {
  const readiness = alignment?.latest_readiness_report;
  if (!readiness) return renderEmptySurface("No readiness");
  return (
    <div>
      <div>{titleCase(readiness.status)}</div>
      <div style={subtleCellStyle}>
        Backlog {readiness.approval_backlog_count ?? 0} | Shadow breaches {readiness.shadow_gap_breach_count ?? 0}
      </div>
      <div style={subtleCellStyle}>
        Incidents {readiness.open_incidents ?? 0} | {fmtDate(readiness.generated_at)}
      </div>
    </div>
  );
}

function renderAlignmentFreshness(alignment) {
  if (!alignment) return renderEmptySurface("No surfaces");
  return (
    <div>
      <Badge value={alignment.surface_status} />
      <div style={subtleCellStyle}>{alignment.surfaces_present ?? 0} linked surfaces</div>
      <div style={subtleCellStyle}>{fmtDate(alignment.latest_surface_at)}</div>
    </div>
  );
}

function buttonLabelForVersion(version, isOpen, isLoading) {
  if (isLoading) return "Loading...";
  return isOpen ? "Hide" : "Inspect";
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
  const [expandedVersionId, setExpandedVersionId] = useState(null);
  const [versionDetails, setVersionDetails] = useState({});
  const [detailLoadingId, setDetailLoadingId] = useState(null);
  const [detailError, setDetailError] = useState(null);

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

  const inspectVersion = useCallback(async (version) => {
    if (!version?.id) return;
    const versionId = version.id;
    if (expandedVersionId === versionId) {
      setExpandedVersionId(null);
      setDetailError(null);
      return;
    }
    setExpandedVersionId(versionId);
    setDetailError(null);
    if (versionDetails[versionId]) return;
    try {
      setDetailLoadingId(versionId);
      const payload = await getStrategyVersionDetail(versionId);
      setVersionDetails((current) => ({
        ...current,
        [versionId]: payload,
      }));
    } catch (requestError) {
      setDetailError(requestError.message);
    } finally {
      setDetailLoadingId(null);
    }
  }, [expandedVersionId, versionDetails]);

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
        const evaluationProvenance = latestEvaluation?.provenance_json || {};
        const expandedVersion = family.versions.find((version) => version.id === expandedVersionId) || null;
        const versionDetail = expandedVersion ? versionDetails[expandedVersion.id] : null;
        const versionDetailLoading = expandedVersion ? detailLoadingId === expandedVersion.id : false;
        const versionDetailError = expandedVersion ? detailError : null;
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
                    label="Evidence Source"
                    value={titleCase(latestEvaluation.evaluation_kind)}
                    hint={evaluationSourceHint(latestEvaluation)}
                  />
                  <MetricCard
                    label="Recommended Tier"
                    value={titleCase(latestEvaluation.autonomy_tier)}
                    hint={evaluationStateHint(latestEvaluation)}
                  />
                  <MetricCard
                    label="Policy"
                    value={evaluationProvenance.promotion_gate_policy_key || "Unknown"}
                    hint={`Window ends ${fmtDate(latestEvaluation.evaluation_window_end)}`}
                  />
                  <MetricCard
                    label="Gate Summary"
                    value={evaluationSummaryValue(latestEvaluation)}
                    hint={evaluationSummaryHint(latestEvaluation)}
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

            <div style={promotionPanelStyle}>
              <div style={promotionHeaderStyle}>
                <div style={sectionTitleStyle}>Evidence Alignment</div>
              </div>
              <div style={metaStyle}>
                Latest replay, live shadow, scorecards, and readiness artifacts lined up by strategy version.
              </div>
              <SimpleTable
                columns={["Version", "Gate", "Replay", "Live Shadow", "Scorecard", "Readiness", "Freshness", "Detail"]}
                rows={family.versions.map((version) => ([
                  <div key={`${version.version_key}-alignment`}>
                    <div>{version.version_label}</div>
                    <div style={subtleCellStyle}>{version.version_key}</div>
                  </div>,
                  renderGateSurface(version.latest_promotion_evaluation || version.evidence_alignment?.latest_promotion_evaluation),
                  renderReplaySurface(version.evidence_alignment),
                  renderLiveShadowSurface(version.evidence_alignment),
                  renderScorecardSurface(version.evidence_alignment),
                  renderReadinessSurface(version.evidence_alignment),
                  renderAlignmentFreshness(version.evidence_alignment),
                  <button
                    key={`${version.version_key}-inspect`}
                    type="button"
                    onClick={() => inspectVersion(version)}
                    aria-label={`${expandedVersionId === version.id ? "Hide" : "Inspect"} ${version.version_label}`}
                    style={secondaryButtonStyle}
                    disabled={detailLoadingId === version.id}
                  >
                    {buttonLabelForVersion(version, expandedVersionId === version.id, detailLoadingId === version.id)}
                  </button>,
                ]))}
                emptyLabel="No version-linked evidence has been aligned yet."
              />
            </div>

            {expandedVersion ? (
              <div style={detailPanelStyle}>
                <div style={sectionHeaderStyle}>
                  <div>
                    <h3 style={sectionTitleStyle}>Version Detail</h3>
                    <div style={metaStyle}>
                      {expandedVersion.version_label} | {expandedVersion.version_key}
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setExpandedVersionId(null)}
                    style={secondaryButtonStyle}
                  >
                    Close
                  </button>
                </div>

                {versionDetailError ? <div style={alertStyle}>{versionDetailError}</div> : null}
                {versionDetailLoading && !versionDetail ? (
                  <div style={metaStyle}>Loading version detail...</div>
                ) : null}

                {versionDetail ? (
                  <>
                    <div style={statsGridStyle}>
                      <MetricCard
                        label="Replay Runs"
                        value={versionDetail.replay_runs?.length ?? 0}
                        hint="Version-scoped replay artifacts"
                      />
                      <MetricCard
                        label="Live Shadow"
                        value={versionDetail.live_shadow_evaluations?.length ?? 0}
                        hint="Recent persisted live-shadow rows"
                      />
                      <MetricCard
                        label="Scorecards"
                        value={versionDetail.scorecards?.length ?? 0}
                        hint="Latest version-linked scorecards"
                      />
                      <MetricCard
                        label="Readiness"
                        value={versionDetail.readiness_reports?.length ?? 0}
                        hint="Latest version-linked readiness reports"
                      />
                      <MetricCard
                        label="Promotion Events"
                        value={versionDetail.promotion_evaluations?.length ?? 0}
                        hint="Recent version-scoped gate evaluations"
                      />
                      <MetricCard
                        label="Gate History"
                        value={versionDetail.gate_history?.length ?? 0}
                        hint="Primary and supporting gate snapshots"
                      />
                      <MetricCard
                        label="Budget Status"
                        value={titleCase(versionDetail.version?.risk_budget_status?.capacity_status || "unknown")}
                        hint={budgetUsedHint(versionDetail.version?.risk_budget_status)}
                      />
                      <MetricCard
                        label="Budget Regime"
                        value={titleCase(versionDetail.version?.risk_budget_status?.regime_label || "unknown")}
                        hint={`Ruin ${titleCase(versionDetail.version?.risk_budget_status?.risk_of_ruin_label || "unknown")}`}
                      />
                    </div>

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Policy", "Value", "Status", "Hint"]}
                      rows={[
                        [
                          "Outstanding Cap",
                          fmtMetric(versionDetail.version?.risk_budget_status?.effective_outstanding_cap_usd),
                          titleCase(versionDetail.version?.risk_budget_status?.capacity_status || "unknown"),
                          `Current ${fmtMetric(versionDetail.version?.risk_budget_status?.current_outstanding_usd)}`,
                        ],
                        [
                          "Capacity Ceiling",
                          fmtMetric(versionDetail.version?.risk_budget_status?.effective_capacity_ceiling_usd),
                          titleCase(versionDetail.version?.risk_budget_status?.regime_label || "unknown"),
                          `Open orders ${versionDetail.version?.risk_budget_status?.open_order_count ?? 0} / ${versionDetail.version?.risk_budget_status?.effective_max_open_orders ?? "-"}`,
                        ],
                        [
                          "Max Order",
                          fmtMetric(versionDetail.version?.risk_budget_status?.effective_max_order_notional_usd),
                          titleCase(versionDetail.version?.risk_budget_status?.risk_of_ruin_label || "unknown"),
                          `Ruin score ${fmtMetric(versionDetail.version?.risk_budget_status?.risk_of_ruin_score, 3)}`,
                        ],
                      ]}
                      emptyLabel="No risk budget policy linked to this version yet."
                    />

                    <SimpleTable
                      columns={["Replay Run", "Status", "Window", "Scenarios", "Gate"]}
                      rows={(versionDetail.replay_runs || []).map((run) => ([
                        <div key={run.id}>
                          <div>{run.run_key}</div>
                          <div style={subtleCellStyle}>{titleCase(run.run_type)}</div>
                        </div>,
                        titleCase(run.status),
                        `${fmtDate(run.time_window_start)} -> ${fmtDate(run.time_window_end)}`,
                        run.scenario_count ?? 0,
                        run.promotion_evaluation?.evaluation_status
                          ? `${titleCase(run.promotion_evaluation.evaluation_status)} / ${titleCase(run.promotion_evaluation.autonomy_tier)}`
                          : "-",
                      ]))}
                      emptyLabel="No replay runs linked to this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Updated", "Client Order", "Market", "Gap", "Realized", "Coverage"]}
                      rows={(versionDetail.live_shadow_evaluations || []).map((row) => ([
                        fmtDate(row.updated_at),
                        row.client_order_id || row.live_order_id || "-",
                        `${row.condition_id || "-"} / ${row.asset_id || "-"}`,
                        row.gap_bps ? `${fmtMetric(row.gap_bps)} bps` : "-",
                        row.realized_net_bps ? `${fmtMetric(row.realized_net_bps)} bps` : "-",
                        row.coverage_limited ? "Limited" : "Full",
                      ]))}
                      emptyLabel="No live-shadow evaluations linked to this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Scorecard Window", "Status", "Net P&L", "Avg Gap", "Coverage"]}
                      rows={(versionDetail.scorecards || []).map((row) => ([
                        `${fmtDate(row.window_start)} -> ${fmtDate(row.window_end)}`,
                        titleCase(row.status),
                        fmtMetric(row.net_pnl),
                        row.avg_shadow_gap_bps ? `${fmtMetric(row.avg_shadow_gap_bps)} bps` : "-",
                        row.coverage_limited_count ?? 0,
                      ]))}
                      emptyLabel="No scorecards linked to this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Generated", "Status", "Backlog", "Shadow Breaches", "Incidents"]}
                      rows={(versionDetail.readiness_reports || []).map((row) => ([
                        fmtDate(row.generated_at),
                        titleCase(row.status),
                        row.approval_backlog_count ?? 0,
                        row.shadow_gap_breach_count ?? 0,
                        row.open_incidents ?? 0,
                      ]))}
                      emptyLabel="No readiness reports linked to this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Gate Event", "Status", "Window", "Summary", "Observed"]}
                      rows={(versionDetail.gate_history || []).map((row) => ([
                        titleCase(row.evaluation_kind),
                        <div key={`${row.id}-status`}>
                          <Badge value={row.evaluation_status} />
                          <div style={subtleCellStyle}>{titleCase(row.autonomy_tier)}</div>
                        </div>,
                        `${fmtDate(row.evaluation_window_start)} -> ${fmtDate(row.evaluation_window_end)}`,
                        gateHistorySummary(row),
                        fmtDate(gateHistoryObservedAt(row)),
                      ]))}
                      emptyLabel="No gate history recorded for this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Promotion Evaluation", "Status", "Tier", "Observed"]}
                      rows={(versionDetail.promotion_evaluations || []).map((row) => ([
                        titleCase(row.evaluation_kind),
                        titleCase(row.evaluation_status),
                        titleCase(row.autonomy_tier),
                        fmtDate(row.updated_at || row.created_at),
                      ]))}
                      emptyLabel="No promotion evaluations recorded for this version yet."
                    />

                    <div style={detailSectionGapStyle} />
                    <SimpleTable
                      columns={["Demotion Reason", "Fallback", "Cooling Off Ends", "Observed"]}
                      rows={(versionDetail.demotion_events || []).map((row) => ([
                        titleCase(row.reason_code),
                        titleCase(row.fallback_autonomy_tier),
                        fmtDate(row.cooling_off_ends_at),
                        fmtDate(row.observed_at_local || row.created_at),
                      ]))}
                      emptyLabel="No demotion events recorded for this version yet."
                    />
                  </>
                ) : null}
              </div>
            ) : null}

            <SimpleTable
              columns={["Version", "State", "Tier", "Frozen", "Budget", "Evidence", "Updated"]}
              rows={family.versions.map((version) => ([
                <div key={version.version_key}>
                  <div>{version.version_label}</div>
                  <div style={subtleCellStyle}>{version.version_key}</div>
                </div>,
                <Badge key={`${version.version_key}-state`} value={version.version_status} />,
                <Badge key={`${version.version_key}-tier`} value={version.autonomy_tier} />,
                version.is_frozen ? "Yes" : "No",
                <div key={`${version.version_key}-budget`}>
                  <div>{titleCase(version.risk_budget_status?.capacity_status || "unknown")}</div>
                  <div style={subtleCellStyle}>{budgetCapacityHint(version.risk_budget_status)}</div>
                </div>,
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
const detailPanelStyle = { display: "flex", flexDirection: "column", gap: 12, borderTop: "1px solid var(--border)", paddingTop: 16, marginBottom: 16 };
const detailSectionGapStyle = { height: 4 };
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
