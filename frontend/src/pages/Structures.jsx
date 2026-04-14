import { useEffect, useState } from "react";
import {
  approvePolymarketStructurePaperPlan,
  createPolymarketStructurePaperPlan,
  getPolymarketStructureOpportunity,
  getPolymarketStructureOpportunities,
  getPolymarketStructureStatus,
  rejectPolymarketStructurePaperPlan,
  routePolymarketStructurePaperPlan,
  validatePolymarketStructureOpportunities,
} from "../api";

const INITIAL_FILTERS = {
  opportunityType: "",
  classification: "",
  edgeBucket: "",
  reasonCode: "",
  reviewStatus: "",
  confidenceMin: "",
  executableOnly: "",
};

const opportunityTypeOptions = [
  { value: "", label: "All Structures" },
  { value: "neg_risk_direct_vs_basket", label: "Neg-Risk" },
  { value: "binary_complement", label: "Complement / Parity" },
  { value: "event_sum_parity", label: "Augmented Neg-Risk" },
  { value: "cross_venue_basis", label: "Cross-Venue Basis" },
];

const classificationOptions = [
  { value: "", label: "All Validations" },
  { value: "executable_candidate", label: "Executable" },
  { value: "informational_only", label: "Informational" },
  { value: "blocked", label: "Blocked" },
];

const edgeBucketOptions = [
  { value: "", label: "All Edge Buckets" },
  { value: "100bps_plus", label: "100bps+" },
  { value: "25-100bps", label: "25-100bps" },
  { value: "0-25bps", label: "0-25bps" },
  { value: "negative", label: "Negative" },
  { value: "unknown", label: "Unknown" },
];

const reviewStatusOptions = [
  { value: "", label: "All Link States" },
  { value: "approved", label: "Approved" },
  { value: "needs_review", label: "Needs Review" },
  { value: "expired", label: "Expired" },
  { value: "disabled", label: "Disabled" },
];

const executableOptions = [
  { value: "", label: "All Executability" },
  { value: "true", label: "Executable Only" },
  { value: "false", label: "Non-Executable Only" },
];

function fmtDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function fmtNumber(value, digits = 2) {
  if (value == null || value === "") return "-";
  return Number(value).toFixed(digits);
}

function fmtReasonLabels(validation) {
  const labels = validation?.summary_json?.reason_labels || {};
  const entries = Object.entries(labels);
  if (!entries.length) return [];
  return entries.map(([code, label]) => ({ code, label }));
}

function latestPlanFromDetail(detail) {
  return detail?.paper_plans?.[0] || null;
}

function statusTone(value) {
  if (value === "executable_candidate" || value === "routed") return "var(--green)";
  if (value === "blocked" || value === "rejected") return "var(--red)";
  if (value === "informational_only" || value === "approval_pending" || value === "partial_failed") return "var(--yellow)";
  return "var(--text-dim)";
}

export default function Structures() {
  const [filters, setFilters] = useState(INITIAL_FILTERS);
  const [status, setStatus] = useState(null);
  const [opportunities, setOpportunities] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState(null);
  const [actionError, setActionError] = useState(null);
  const [busyAction, setBusyAction] = useState("");
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    let active = true;
    async function loadOverview() {
      setLoading(true);
      try {
        const [statusData, opportunityData] = await Promise.all([
          getPolymarketStructureStatus(),
          getPolymarketStructureOpportunities({
            opportunityType: filters.opportunityType,
            classification: filters.classification,
            edgeBucket: filters.edgeBucket,
            reasonCode: filters.reasonCode,
            reviewStatus: filters.reviewStatus,
            confidenceMin: filters.confidenceMin,
            executableOnly: filters.executableOnly,
            limit: 100,
          }),
        ]);
        if (!active) return;
        const rows = opportunityData.rows || [];
        setStatus(statusData);
        setOpportunities(rows);
        setSelectedId((current) => (
          current && rows.some((row) => row.id === current)
            ? current
            : rows[0]?.id ?? null
        ));
        setError(null);
      } catch (e) {
        if (active) {
          setError(e.message);
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }
    loadOverview();
    return () => {
      active = false;
    };
  }, [
    filters.classification,
    filters.confidenceMin,
    filters.edgeBucket,
    filters.executableOnly,
    filters.opportunityType,
    filters.reasonCode,
    filters.reviewStatus,
    refreshToken,
  ]);

  useEffect(() => {
    let active = true;
    async function loadDetail() {
      if (!selectedId) {
        setDetail(null);
        return;
      }
      setDetailLoading(true);
      try {
        const detailData = await getPolymarketStructureOpportunity(selectedId);
        if (!active) return;
        setDetail(detailData);
        setActionError(null);
      } catch (e) {
        if (active) {
          setActionError(e.message);
        }
      } finally {
        if (active) {
          setDetailLoading(false);
        }
      }
    }
    loadDetail();
    return () => {
      active = false;
    };
  }, [selectedId, refreshToken]);

  async function runAction(actionName, action) {
    setBusyAction(actionName);
    setActionError(null);
    try {
      await action();
      setRefreshToken((value) => value + 1);
    } catch (e) {
      setActionError(e.message);
    } finally {
      setBusyAction("");
    }
  }

  const latestPlan = latestPlanFromDetail(detail);
  const reasonOptions = Object.keys(status?.validation_reason_counts || {});
  const latestValidation = detail?.latest_validation || null;
  const reasonLabels = fmtReasonLabels(latestValidation);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
        <div>
          <h2 style={{ fontSize: 18, marginBottom: 6 }}>Structure Opportunities</h2>
          <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
            Phase 8B validation, controls, and paper-routing workflow.
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button style={secondaryButtonStyle} onClick={() => setRefreshToken((value) => value + 1)}>
            Refresh
          </button>
          <button
            style={primaryButtonStyle}
            disabled={!selectedId || busyAction === "validate"}
            onClick={() => runAction("validate", () => validatePolymarketStructureOpportunities({
              reason: "manual",
              opportunity_id: selectedId,
            }))}
          >
            {busyAction === "validate" ? "Validating..." : "Revalidate Selected"}
          </button>
        </div>
      </div>

      {error && <InlineAlert tone="error">{error}</InlineAlert>}
      {actionError && <InlineAlert tone="warning">{actionError}</InlineAlert>}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 12,
        }}
      >
        <SummaryCard label="Executable" value={status?.executable_candidate_count ?? "-"} />
        <SummaryCard label="Informational" value={status?.informational_only_opportunity_count ?? "-"} />
        <SummaryCard label="Blocked" value={status?.blocked_opportunity_count ?? "-"} />
        <SummaryCard label="Pending Approval" value={status?.pending_approval_count ?? "-"} />
        <SummaryCard label="Stale Links" value={status?.stale_cross_venue_link_count ?? "-"} />
        <SummaryCard label="Last Validation" value={fmtDateTime(status?.last_successful_validation_at)} />
      </div>

      <section style={panelStyle}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
          <FilterField
            label="Structure"
            value={filters.opportunityType}
            onChange={(value) => setFilters((current) => ({ ...current, opportunityType: value }))}
            options={opportunityTypeOptions}
          />
          <FilterField
            label="Validation"
            value={filters.classification}
            onChange={(value) => setFilters((current) => ({ ...current, classification: value }))}
            options={classificationOptions}
          />
          <FilterField
            label="Edge Bucket"
            value={filters.edgeBucket}
            onChange={(value) => setFilters((current) => ({ ...current, edgeBucket: value }))}
            options={edgeBucketOptions}
          />
          <FilterField
            label="Reason Code"
            value={filters.reasonCode}
            onChange={(value) => setFilters((current) => ({ ...current, reasonCode: value }))}
            options={[{ value: "", label: "All Reasons" }, ...reasonOptions.map((value) => ({ value, label: value }))]}
          />
          <FilterField
            label="Link Review"
            value={filters.reviewStatus}
            onChange={(value) => setFilters((current) => ({ ...current, reviewStatus: value }))}
            options={reviewStatusOptions}
          />
          <FilterField
            label="Executability"
            value={filters.executableOnly}
            onChange={(value) => setFilters((current) => ({ ...current, executableOnly: value }))}
            options={executableOptions}
          />
          <label style={{ display: "flex", flexDirection: "column", gap: 6, fontSize: 12 }}>
            <span style={{ color: "var(--text-dim)" }}>Min Confidence</span>
            <input
              type="number"
              step="0.01"
              min="0"
              max="1"
              value={filters.confidenceMin}
              onChange={(event) => setFilters((current) => ({ ...current, confidenceMin: event.target.value }))}
              style={inputStyle}
              placeholder="0.50"
            />
          </label>
        </div>
      </section>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 1.1fr) minmax(380px, 1fr)", gap: 16, alignItems: "start" }}>
        <section style={panelStyle}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12, gap: 8 }}>
            <div>
              <h3 style={{ fontSize: 14, fontWeight: 600 }}>Opportunity Queue</h3>
              <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{opportunities.length} visible opportunities</div>
            </div>
            {loading && <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Loading...</span>}
          </div>

          {!opportunities.length && !loading ? (
            <div style={emptyStateStyle}>No structure opportunities match the current filters.</div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {opportunities.map((row) => (
                <button
                  key={row.id}
                  type="button"
                  onClick={() => setSelectedId(row.id)}
                  style={{
                    ...rowButtonStyle,
                    borderColor: row.id === selectedId ? "var(--accent)" : "var(--border)",
                    background: row.id === selectedId ? "rgba(255, 255, 255, 0.04)" : "var(--bg-card)",
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 600 }}>{row.group_title || row.opportunity_type}</div>
                      <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>
                        {row.opportunity_type} | {row.group_type || "ungrouped"}
                      </div>
                    </div>
                    <span style={{ color: statusTone(row.validation_classification), fontSize: 12, fontWeight: 600 }}>
                      {row.validation_classification || "unvalidated"}
                    </span>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 8, marginTop: 10, fontSize: 12 }}>
                    <DetailStat label="Net Edge" value={row.validation_current_net_edge_bps ? `${fmtNumber(row.validation_current_net_edge_bps, 1)} bps` : `${fmtNumber(row.net_edge_bps, 1)} bps`} />
                    <DetailStat label="Plan" value={row.plan_status || "-"} />
                    <DetailStat label="Link" value={row.cross_venue_review_status || "-"} />
                  </div>
                </button>
              ))}
            </div>
          )}
        </section>

        <section style={panelStyle}>
          {!selectedId ? (
            <div style={emptyStateStyle}>Select an opportunity to inspect its legs, validation state, and paper plan audit trail.</div>
          ) : detailLoading ? (
            <div style={emptyStateStyle}>Loading detail...</div>
          ) : !detail ? (
            <div style={emptyStateStyle}>No detail available for the selected opportunity.</div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", flexWrap: "wrap" }}>
                <div>
                  <h3 style={{ fontSize: 15, fontWeight: 600 }}>{detail.group?.title || detail.opportunity.opportunity_type}</h3>
                  <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 4 }}>
                    {detail.opportunity.opportunity_type} | Observed {fmtDateTime(detail.opportunity.observed_at_local)}
                  </div>
                </div>
                <span style={{ color: statusTone(latestValidation?.classification), fontSize: 12, fontWeight: 700 }}>
                  {latestValidation?.classification || "unvalidated"}
                </span>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 10 }}>
                <SummaryCard label="Detected Net" value={detail.opportunity.net_edge_bps ? `${fmtNumber(detail.opportunity.net_edge_bps, 1)} bps` : "-"} />
                <SummaryCard label="Current Net" value={latestValidation?.current_net_edge_bps ? `${fmtNumber(latestValidation.current_net_edge_bps, 1)} bps` : "-"} />
                <SummaryCard label="Detected Age" value={latestValidation?.detected_age_seconds != null ? `${latestValidation.detected_age_seconds}s` : "-"} />
                <SummaryCard label="Leg Staleness" value={latestValidation?.max_leg_age_seconds != null ? `${latestValidation.max_leg_age_seconds}s` : "-"} />
              </div>

              <div>
                <div style={sectionTitleStyle}>Reason Codes</div>
                {reasonLabels.length ? (
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    {reasonLabels.map((reason) => (
                      <span key={reason.code} style={pillStyle}>
                        {reason.label}
                      </span>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: 12, color: "var(--text-dim)" }}>No active reason codes for the latest validation.</div>
                )}
              </div>

              <div>
                <div style={sectionTitleStyle}>Leg Estimates</div>
                <div className="table-scroll">
                  <table style={tableStyle}>
                    <thead>
                      <tr>
                        <TableHead>Leg</TableHead>
                        <TableHead>Venue</TableHead>
                        <TableHead>Side</TableHead>
                        <TableHead>Target</TableHead>
                        <TableHead>Avg Entry</TableHead>
                        <TableHead>Slippage</TableHead>
                        <TableHead>Status</TableHead>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.legs.map((leg) => (
                        <tr key={leg.id}>
                          <TableCell>{leg.leg_index}</TableCell>
                          <TableCell>{leg.venue}</TableCell>
                          <TableCell>{leg.side}</TableCell>
                          <TableCell>{fmtNumber(leg.target_size, 2)}</TableCell>
                          <TableCell>{fmtNumber(leg.est_avg_entry_price, 3)}</TableCell>
                          <TableCell>{leg.est_slippage_bps ? `${fmtNumber(leg.est_slippage_bps, 1)} bps` : "-"}</TableCell>
                          <TableCell>{leg.valid ? "valid" : (leg.invalid_reason || "blocked")}</TableCell>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {detail.cross_venue_link && (
                <div>
                  <div style={sectionTitleStyle}>Cross-Venue Governance</div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 10 }}>
                    <DetailStat label="Effective Review" value={detail.cross_venue_link.effective_review_status || "-"} />
                    <DetailStat label="Confidence" value={detail.cross_venue_link.confidence || "-"} />
                    <DetailStat label="Owner" value={detail.cross_venue_link.owner || "-"} />
                    <DetailStat label="Provenance" value={detail.cross_venue_link.provenance_source || "-"} />
                    <DetailStat label="Reviewed By" value={detail.cross_venue_link.reviewed_by || "-"} />
                    <DetailStat label="Expires" value={fmtDateTime(detail.cross_venue_link.expires_at)} />
                  </div>
                  {detail.cross_venue_link.notes && (
                    <div style={{ marginTop: 10, fontSize: 12, color: "var(--text-dim)" }}>{detail.cross_venue_link.notes}</div>
                  )}
                </div>
              )}

              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button
                  style={secondaryButtonStyle}
                  disabled={busyAction === "create-plan"}
                  onClick={() => runAction("create-plan", () => createPolymarketStructurePaperPlan(detail.opportunity.id, { actor: "operator" }))}
                >
                  {busyAction === "create-plan" ? "Creating..." : "Create Paper Plan"}
                </button>
                {latestPlan?.status === "approval_pending" && (
                  <>
                    <button
                      style={primaryButtonStyle}
                      disabled={busyAction === "approve-plan"}
                      onClick={() => runAction("approve-plan", () => approvePolymarketStructurePaperPlan(latestPlan.id, { actor: "operator" }))}
                    >
                      {busyAction === "approve-plan" ? "Approving..." : "Approve Plan"}
                    </button>
                    <button
                      style={dangerButtonStyle}
                      disabled={busyAction === "reject-plan"}
                      onClick={() => runAction("reject-plan", () => rejectPolymarketStructurePaperPlan(latestPlan.id, {
                        actor: "operator",
                        reason: "operator_rejected",
                      }))}
                    >
                      {busyAction === "reject-plan" ? "Rejecting..." : "Reject Plan"}
                    </button>
                  </>
                )}
                {latestPlan && ["routing_pending", "partial_failed"].includes(latestPlan.status) && (
                  <button
                    style={primaryButtonStyle}
                    disabled={busyAction === "route-plan"}
                    onClick={() => runAction("route-plan", () => routePolymarketStructurePaperPlan(latestPlan.id, { actor: "operator" }))}
                  >
                    {busyAction === "route-plan" ? "Routing..." : "Route Plan"}
                  </button>
                )}
              </div>

              <div>
                <div style={sectionTitleStyle}>Paper Plan Audit</div>
                {!detail.paper_plans?.length ? (
                  <div style={{ fontSize: 12, color: "var(--text-dim)" }}>No paper plans have been created for this opportunity yet.</div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                    {detail.paper_plans.map((plan) => (
                      <div key={plan.id} style={auditCardStyle}>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
                          <div>
                            <div style={{ fontSize: 13, fontWeight: 600 }}>Plan {String(plan.id).slice(0, 8)}</div>
                            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                              Created {fmtDateTime(plan.created_at)} | Notional {plan.plan_notional_total || "-"}
                            </div>
                          </div>
                          <span style={{ color: statusTone(plan.status), fontSize: 12, fontWeight: 700 }}>{plan.status}</span>
                        </div>
                        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 8, marginBottom: 10 }}>
                          <DetailStat label="Approval" value={plan.manual_approval_required ? "required" : "not required"} />
                          <DetailStat label="Approved By" value={plan.approved_by || "-"} />
                          <DetailStat label="Rejected By" value={plan.rejected_by || "-"} />
                          <DetailStat label="Reason Codes" value={(plan.reason_codes_json || []).join(", ") || "-"} />
                        </div>
                        <div className="table-scroll">
                          <table style={tableStyle}>
                            <thead>
                              <tr>
                                <TableHead>Leg</TableHead>
                                <TableHead>Venue</TableHead>
                                <TableHead>Side</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Target</TableHead>
                                <TableHead>Fill</TableHead>
                              </tr>
                            </thead>
                            <tbody>
                              {(plan.orders || []).map((order) => (
                                <tr key={order.id}>
                                  <TableCell>{order.leg_index}</TableCell>
                                  <TableCell>{order.venue}</TableCell>
                                  <TableCell>{order.side}</TableCell>
                                  <TableCell>{order.status}</TableCell>
                                  <TableCell>{fmtNumber(order.target_size, 2)}</TableCell>
                                  <TableCell>{order.avg_fill_price ? `${fmtNumber(order.avg_fill_price, 3)} @ ${fmtNumber(order.filled_size, 2)}` : "-"}</TableCell>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 6 }}>
                          {(plan.events || []).slice(0, 6).map((event) => (
                            <div key={event.id} style={{ fontSize: 12, color: "var(--text-dim)" }}>
                              {fmtDateTime(event.observed_at)} | {event.event_type} | {event.status || "-"}
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

function FilterField({ label, value, onChange, options }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 6, fontSize: 12 }}>
      <span style={{ color: "var(--text-dim)" }}>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} style={inputStyle}>
        {options.map((option) => (
          <option key={option.value || option.label} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function SummaryCard({ label, value }) {
  return (
    <div style={summaryCardStyle}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 22, fontFamily: "var(--mono)", fontWeight: 700 }}>{value}</div>
    </div>
  );
}

function DetailStat({ label, value }) {
  return (
    <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 8, padding: "10px 12px" }}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 13 }}>{value}</div>
    </div>
  );
}

function TableHead({ children }) {
  return (
    <th style={{ textAlign: "left", fontSize: 11, color: "var(--text-dim)", padding: "0 0 8px" }}>
      {children}
    </th>
  );
}

function TableCell({ children }) {
  return (
    <td style={{ padding: "8px 0", borderTop: "1px solid var(--border)", fontSize: 12, verticalAlign: "top" }}>
      {children}
    </td>
  );
}

function InlineAlert({ tone, children }) {
  const color = tone === "error" ? "var(--red)" : "var(--yellow)";
  return (
    <div style={{ border: `1px solid ${color}`, borderRadius: 10, padding: "12px 14px", color }}>
      {children}
    </div>
  );
}

const panelStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  borderRadius: 12,
  padding: 16,
};

const inputStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  color: "var(--text)",
  borderRadius: 8,
  padding: "10px 12px",
};

const summaryCardStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  borderRadius: 10,
  padding: "14px 16px",
};

const primaryButtonStyle = {
  background: "var(--accent)",
  color: "var(--bg)",
  border: "none",
  borderRadius: 8,
  padding: "10px 14px",
  cursor: "pointer",
  fontWeight: 600,
};

const secondaryButtonStyle = {
  background: "transparent",
  color: "var(--text)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: "10px 14px",
  cursor: "pointer",
  fontWeight: 600,
};

const dangerButtonStyle = {
  background: "transparent",
  color: "var(--red)",
  border: "1px solid var(--red)",
  borderRadius: 8,
  padding: "10px 14px",
  cursor: "pointer",
  fontWeight: 600,
};

const rowButtonStyle = {
  display: "block",
  width: "100%",
  textAlign: "left",
  border: "1px solid var(--border)",
  borderRadius: 10,
  padding: "12px 14px",
  cursor: "pointer",
  color: "var(--text)",
};

const emptyStateStyle = {
  border: "1px dashed var(--border)",
  borderRadius: 10,
  padding: "20px 16px",
  color: "var(--text-dim)",
  fontSize: 12,
};

const sectionTitleStyle = {
  fontSize: 13,
  fontWeight: 600,
  marginBottom: 8,
};

const pillStyle = {
  background: "rgba(255, 255, 255, 0.04)",
  border: "1px solid var(--border)",
  borderRadius: 999,
  padding: "6px 10px",
  fontSize: 12,
};

const auditCardStyle = {
  border: "1px solid var(--border)",
  borderRadius: 10,
  padding: 12,
  background: "rgba(255, 255, 255, 0.02)",
};

const tableStyle = {
  width: "100%",
  borderCollapse: "collapse",
};
