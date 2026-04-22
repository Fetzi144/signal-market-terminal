import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  approvePolymarketLiveOrder,
  armPolymarketPilot,
  createPolymarketPilotConfig,
  disarmPolymarketPilot,
  generatePolymarketPilotReadinessReport,
  generatePolymarketPilotScorecard,
  getPolymarketPilotConfigs,
  getPolymarketPilotConsoleSummary,
  pausePolymarketPilot,
  rejectPolymarketLiveOrder,
  resumePolymarketPilot,
  setPolymarketLiveKillSwitch,
} from "../api";

export default function PilotConsole() {
  const [summary, setSummary] = useState(null);
  const [configs, setConfigs] = useState([]);
  const [selectedConfigId, setSelectedConfigId] = useState("");
  const [error, setError] = useState(null);
  const [busyAction, setBusyAction] = useState(null);

  const load = useCallback(async () => {
    try {
      const [consoleSummary, configResult] = await Promise.all([
        getPolymarketPilotConsoleSummary(),
        getPolymarketPilotConfigs({ limit: 20 }),
      ]);
      setSummary(consoleSummary);
      setConfigs(configResult.rows || []);
      const activeId = consoleSummary?.pilot?.active_pilot?.id;
      setSelectedConfigId(String(activeId || configResult.rows?.[0]?.id || ""));
      setError(null);
    } catch (requestError) {
      setError(requestError.message);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const runAction = useCallback(async (key, fn) => {
    try {
      setBusyAction(key);
      setError(null);
      await fn();
      await load();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusyAction(null);
    }
  }, [load]);

  const createDefaultConfig = () => runAction("create-config", async () => {
    await createPolymarketPilotConfig({
      pilot_name: `phase12-exec-${Date.now()}`,
      strategy_family: "exec_policy",
      active: false,
      live_enabled: false,
      manual_approval_required: true,
      max_notional_per_day_usd: 100,
      max_notional_per_order_usd: 100,
      max_open_orders: 1,
      max_decision_age_seconds: 300,
    });
  });

  const armSelected = () => {
    if (!selectedConfigId) return;
    return runAction("arm", () => armPolymarketPilot({
      pilot_config_id: Number(selectedConfigId),
      operator_identity: "operator",
    }));
  };

  const activePilot = summary?.pilot?.active_pilot || null;
  const activeRun = summary?.pilot?.active_run || null;
  const approvals = summary?.approvals || [];
  const incidents = summary?.incidents || [];
  const guardrails = summary?.guardrail_events || [];
  const recentOrders = summary?.recent_orders || [];
  const recentFills = summary?.recent_fills || [];
  const blocked = summary?.recent_blocked_submissions || [];
  const shadow = summary?.live_shadow_summary || {};
  const evidence = summary?.evidence_summary || {};
  const evidenceShadow = evidence?.live_shadow_summary || {};
  const dailyRealized = evidence?.daily_realized_pnl || {};
  const scorecards = summary?.scorecards || [];
  const readinessReports = summary?.readiness_reports || [];
  const latestReadiness = evidence?.latest_readiness_report || readinessReports[0] || null;
  const activeStrategyVersion = summary?.pilot?.active_strategy_version || evidence?.strategy_version || null;
  const latestGate = evidence?.latest_promotion_evaluation || summary?.pilot?.latest_promotion_evaluation || latestReadiness?.latest_promotion_evaluation || null;
  const activeFamilyBudget = summary?.active_family_budget || summary?.pilot?.active_family_budget || null;
  const activeAutonomyState = summary?.active_autonomy_state || summary?.pilot?.active_autonomy_state || null;

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div>
          <h2 style={titleStyle}>Pilot Console</h2>
          <p style={subtitleStyle}>
            Narrow supervised Polymarket pilot for the currently supported family only. Lifecycle registry now lives in{" "}
            <Link to="/strategies" style={inlineLinkStyle}>Strategies</Link>.
          </p>
        </div>
        <button onClick={load} style={secondaryButtonStyle}>Refresh</button>
      </div>

      {error && <InlineAlert>{error}</InlineAlert>}

      <section style={panelStyle}>
        <div style={sectionHeaderStyle}>
          <div>
            <h3 style={sectionTitleStyle}>Pilot State</h3>
            <div style={metaStyle}>
              Active family: {summary?.active_pilot_family || "none"} | Heartbeat: {summary?.pilot?.heartbeat_status || "idle"}
            </div>
          </div>
          <div style={actionRowStyle}>
            <select
              value={selectedConfigId}
              onChange={(event) => setSelectedConfigId(event.target.value)}
              style={selectStyle}
            >
              <option value="">Select config</option>
              {configs.map((config) => (
                <option key={config.id} value={config.id}>
                  {config.pilot_name} ({config.strategy_family})
                </option>
              ))}
            </select>
            <button onClick={createDefaultConfig} disabled={busyAction === "create-config"} style={secondaryButtonStyle}>
              {busyAction === "create-config" ? "Creating..." : "Create Default Config"}
            </button>
            <button onClick={armSelected} disabled={!selectedConfigId || busyAction === "arm"} style={primaryButtonStyle}>
              {busyAction === "arm" ? "Arming..." : "Arm"}
            </button>
            <button onClick={() => runAction("pause", () => pausePolymarketPilot({ operator: "operator" }))} disabled={busyAction === "pause"} style={secondaryButtonStyle}>
              Pause
            </button>
            <button onClick={() => runAction("resume", () => resumePolymarketPilot({ operator: "operator" }))} disabled={busyAction === "resume"} style={secondaryButtonStyle}>
              Resume
            </button>
            <button onClick={() => runAction("disarm", () => disarmPolymarketPilot({ operator: "operator" }))} disabled={busyAction === "disarm"} style={dangerButtonStyle}>
              Disarm
            </button>
            <button
              onClick={() => runAction("kill-switch", () => setPolymarketLiveKillSwitch(!summary?.pilot?.kill_switch_enabled))}
              disabled={busyAction === "kill-switch"}
              style={summary?.pilot?.kill_switch_enabled ? dangerButtonStyle : secondaryButtonStyle}
            >
              {summary?.pilot?.kill_switch_enabled ? "Kill Switch On" : "Kill Switch Off"}
            </button>
            <button
              onClick={() => runAction("generate-scorecard", () => generatePolymarketPilotScorecard({ strategy_family: "exec_policy", window: "daily" }))}
              disabled={busyAction === "generate-scorecard"}
              style={secondaryButtonStyle}
            >
              Scorecard
            </button>
            <button
              onClick={() => runAction("generate-readiness", () => generatePolymarketPilotReadinessReport({ strategy_family: "exec_policy", window: "daily" }))}
              disabled={busyAction === "generate-readiness"}
              style={secondaryButtonStyle}
            >
              Readiness
            </button>
          </div>
        </div>

        <div style={statsGridStyle}>
          <StatCard label="Pilot Enabled" value={summary?.pilot?.pilot_enabled ? "Yes" : "No"} />
          <StatCard label="Armed" value={activePilot?.armed ? "Yes" : "No"} />
          <StatCard label="Run State" value={activeRun?.status || "idle"} />
          <StatCard label="Lifecycle Version" value={activeStrategyVersion?.version_label || "-"} />
          <StatCard label="Autonomy State" value={formatAutonomyState(activeAutonomyState)} />
          <StatCard label="Gate Verdict" value={latestGate?.evaluation_status || "-"} />
          <StatCard label="Submission Mode" value={titleCase(activeAutonomyState?.submission_mode)} />
          <StatCard label="Manual Approval" value={summary?.pilot?.manual_approval_required ? "On" : "Off"} />
          <StatCard label="Approval Queue" value={summary?.pilot?.approval_queue_count ?? 0} />
          <StatCard label="Expired (24h)" value={evidence?.approval_expired_count_24h ?? 0} />
          <StatCard label="Incidents (24h)" value={summary?.pilot?.recent_incident_count_24h ?? 0} />
          <StatCard label="Open Live Orders" value={summary?.pilot?.open_live_order_count ?? 0} />
          <StatCard label="Shadow Breaches" value={shadow.breach_count_24h ?? 0} />
          <StatCard label="Daily Net P&L" value={formatCurrency(dailyRealized?.net_realized_pnl)} />
          <StatCard label="Readiness" value={latestReadiness?.status || "manual_only"} />
          <StatCard
            label="Budget Used"
            value={
              activeFamilyBudget
                ? `${formatCurrency(activeFamilyBudget.current_outstanding_usd)} / ${formatCurrency(activeFamilyBudget.effective_outstanding_cap_usd)}`
                : "-"
            }
          />
          <StatCard label="Budget Regime" value={activeFamilyBudget?.regime_label || "-"} />
          <StatCard label="Capacity Status" value={activeFamilyBudget?.capacity_status || "-"} />
          <StatCard
            label="Budget Gate"
            value={latestGate?.evaluation_kind === "capital_budget_gate" ? latestGate?.evaluation_status || "-" : (activeFamilyBudget?.reason_codes || []).join(", ") || "-"}
          />
        </div>
        {activeAutonomyState ? (
          <div style={metaStyle}>
            Autonomy reason: {formatAutonomyReason(activeAutonomyState)} | Blockers: {(activeAutonomyState.blocked_reasons || []).map(titleCase).join(", ") || "None"}
          </div>
        ) : null}
      </section>

      <section style={panelStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={sectionTitleStyle}>Manual Approval Queue</h3>
          <div style={metaStyle}>Durable approvals expire automatically when stale.</div>
        </div>
        <SimpleTable
          columns={["Created", "Order", "Market", "State", "Expires", "Actions"]}
          rows={approvals.map((order) => ([
            formatShortDateTime(order.created_at),
            order.client_order_id,
            order.condition_id,
            order.approval_state,
            formatShortDateTime(order.approval_expires_at),
            <div key={order.id} style={actionRowStyle}>
              <button
                onClick={() => runAction(`approve-${order.id}`, () => approvePolymarketLiveOrder(order.id, { approved_by: "operator" }))}
                style={primaryButtonStyle}
              >
                Approve
              </button>
              <button
                onClick={() => runAction(`reject-${order.id}`, () => rejectPolymarketLiveOrder(order.id, { rejected_by: "operator", reason: "operator_rejected" }))}
                style={dangerButtonStyle}
              >
                Reject
              </button>
            </div>,
          ]))}
          emptyLabel="No pending approval items."
        />
      </section>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={sectionTitleStyle}>Guardrail Events</h3>
          </div>
          <SimpleTable
            columns={["When", "Guardrail", "Version", "Gate / Autonomy", "Action", "Details"]}
            rows={guardrails.map((event) => ([
              formatShortDateTime(event.observed_at_local),
              event.guardrail_type,
              formatLifecycleVersion(event),
              renderGateAutonomy(event),
              event.action_taken,
              event.details_json?.reason || event.details_json?.error || event.details_json?.gap_bps || "-",
            ]))}
            emptyLabel="No recent guardrail events."
          />
        </section>

        <section style={panelStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={sectionTitleStyle}>Live vs Shadow</h3>
          </div>
          <div style={statsGridStyle}>
            <StatCard label="Evaluations (24h)" value={shadow.recent_count_24h ?? 0} />
            <StatCard label="Avg Gap" value={formatBps(evidenceShadow.average_gap_bps_24h ?? shadow.average_gap_bps_24h)} />
            <StatCard label="Worst Gap" value={formatBps(evidenceShadow.worst_gap_bps_24h ?? shadow.worst_gap_bps_24h)} />
            <StatCard label="Breaches" value={evidenceShadow.breach_count_24h ?? shadow.breach_count_24h ?? 0} />
          </div>
          <div style={{ marginTop: 12 }}>
            <SimpleTable
              columns={["When", "Reason", "Order"]}
              rows={blocked.map((event) => ([
                formatShortDateTime(event.observed_at_local),
                event.details_json?.budget_metadata?.status?.reason_codes?.join(", ")
                  || event.details_json?.reason
                  || "blocked",
                event.live_order_id || "-",
              ]))}
              emptyLabel="No recent blocked submissions."
            />
          </div>
        </section>
      </div>

      <div style={splitGridStyle}>
        <section style={panelStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={sectionTitleStyle}>Evidence Summary</h3>
          </div>
          <SimpleTable
            columns={["Window", "Version", "Status", "Net P&L", "Avg Gap", "Coverage"]}
            rows={scorecards.map((card) => ([
              `${formatShortDateTime(card.window_start)} -> ${formatShortDateTime(card.window_end)}`,
              card.strategy_version?.version_label || card.strategy_version?.version_key || "-",
              card.status,
              formatCurrency(card.net_pnl),
              formatBps(card.avg_shadow_gap_bps),
              card.coverage_limited_count ?? 0,
            ]))}
            emptyLabel="No scorecards generated yet."
          />
          <div style={{ marginTop: 12 }}>
            <SimpleTable
              columns={["Generated", "Version", "Status", "Gate / Autonomy", "Backlog", "Breaches"]}
              rows={readinessReports.map((report) => ([
                formatShortDateTime(report.generated_at),
                report.strategy_version?.version_label || report.strategy_version?.version_key || "-",
                report.status,
                renderGateAutonomy({ latest_promotion_evaluation: report.latest_promotion_evaluation }),
                report.approval_backlog_count ?? 0,
                report.shadow_gap_breach_count ?? 0,
              ]))}
              emptyLabel="No readiness reports generated yet."
            />
          </div>
        </section>

        <section style={panelStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={sectionTitleStyle}>Recent Orders</h3>
          </div>
          <SimpleTable
            columns={["Created", "Client ID", "Version", "Status", "Approval", "Size"]}
            rows={recentOrders.map((order) => ([
              formatShortDateTime(order.created_at),
              order.client_order_id,
              order.strategy_version?.version_key || "-",
              order.status,
              order.approval_state,
              formatNumber(order.requested_size),
            ]))}
            emptyLabel="No live orders yet."
          />
        </section>
      </div>

      <section style={panelStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={sectionTitleStyle}>Recent Incidents</h3>
        </div>
        <SimpleTable
          columns={["When", "Type", "Version", "Gate / Autonomy", "Severity", "Details"]}
          rows={incidents.map((incident) => ([
            formatShortDateTime(incident.observed_at_local),
            incident.incident_type,
            formatLifecycleVersion(incident),
            renderGateAutonomy(incident),
            incident.severity,
            incident.details_json?.reason || incident.details_json?.error || incident.asset_id || "operator event",
          ]))}
          emptyLabel="No recent pilot incidents."
        />
      </section>

      <section style={panelStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={sectionTitleStyle}>Recent Fills</h3>
        </div>
        <SimpleTable
          columns={["Observed", "Asset", "Status", "Price", "Size"]}
          rows={recentFills.map((fill) => ([
            formatShortDateTime(fill.observed_at_local),
            fill.asset_id,
            fill.fill_status,
            formatNumber(fill.price),
            formatNumber(fill.size),
          ]))}
          emptyLabel="No fills yet."
        />
      </section>
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

function StatCard({ label, value }) {
  return (
    <div style={statCardStyle}>
      <div style={statLabelStyle}>{label}</div>
      <div style={statValueStyle}>{value}</div>
    </div>
  );
}

function InlineAlert({ children }) {
  return <div style={alertStyle}>{children}</div>;
}

function formatShortDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
}

function formatNumber(value) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(2);
}

function formatCurrency(value) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return `$${Number(value).toFixed(2)}`;
}

function formatBps(value) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return `${Number(value).toFixed(1)} bps`;
}

function formatLifecycleVersion(row) {
  return row?.strategy_version?.version_label || row?.strategy_version?.version_key || "-";
}

function formatLifecycleGate(row) {
  return row?.latest_promotion_evaluation?.evaluation_status || "-";
}

function titleCase(value) {
  if (!value) return "-";
  return String(value).replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatAutonomyState(state) {
  if (!state) return "-";
  return titleCase(state.effective_autonomy_tier);
}

function formatAutonomyReason(state) {
  if (!state) return "-";
  return titleCase(state.state_reason || state.blocked_reasons?.[0] || state.submission_mode);
}

function renderGateAutonomy(row) {
  const gate = row?.latest_promotion_evaluation;
  if (!gate) return "-";
  return (
    <div>
      <div>{titleCase(gate.evaluation_status)}</div>
      <div style={metaStyle}>{titleCase(gate.autonomy_tier)}</div>
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
const inlineLinkStyle = { color: "var(--green)", textDecoration: "none" };
const panelStyle = { background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 12, padding: 16 };
const sectionHeaderStyle = { display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 12 };
const sectionTitleStyle = { fontSize: 14, fontWeight: 600, margin: 0 };
const metaStyle = { fontSize: 12, color: "var(--text-dim)", marginTop: 4 };
const actionRowStyle = { display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" };
const statsGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12 };
const splitGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 };
const statCardStyle = { background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 };
const statLabelStyle = { fontSize: 11, color: "var(--text-dim)", marginBottom: 4 };
const statValueStyle = { fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600 };
const selectStyle = { minWidth: 220, borderRadius: 8, border: "1px solid var(--border)", padding: "8px 10px", background: "var(--bg-card)", color: "var(--text)" };
const primaryButtonStyle = { background: "var(--green)", color: "#fff", border: "none", borderRadius: 8, padding: "8px 12px", fontSize: 13, fontWeight: 600 };
const secondaryButtonStyle = { background: "transparent", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const dangerButtonStyle = { background: "transparent", color: "var(--red)", border: "1px solid var(--red)", borderRadius: 8, padding: "8px 12px", fontSize: 13 };
const alertStyle = { border: "1px solid var(--red)", color: "var(--red)", borderRadius: 10, padding: "10px 12px", fontSize: 12, background: "rgba(255, 255, 255, 0.03)" };
const tableStyle = { width: "100%", borderCollapse: "collapse" };
const tableHeadStyle = { padding: "0 0 10px", textAlign: "left", fontSize: 11, color: "var(--text-dim)", fontWeight: 500, whiteSpace: "nowrap" };
const tableRowStyle = { borderTop: "1px solid var(--border)" };
const tableCellStyle = { padding: "10px 10px 10px 0", fontSize: 12, verticalAlign: "top" };
const emptyCellStyle = { padding: "16px 0 4px", fontSize: 12, color: "var(--text-dim)" };
