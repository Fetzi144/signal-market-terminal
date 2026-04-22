import { useCallback, useEffect, useRef, useState } from "react";
import {
  getHealth,
  getPolymarketIngestStatus,
  getPolymarketWatchAssets,
  triggerPolymarketBookSnapshot,
  triggerPolymarketMetadataSync,
  triggerPolymarketBookReconResync,
  triggerPolymarketFeatureMaterialization,
  triggerPolymarketOiPoll,
  triggerPolymarketRawProjector,
  triggerPolymarketReplay,
  triggerPolymarketResync,
  triggerPolymarketStructureGroupBuild,
  triggerPolymarketStructureOpportunityScan,
  triggerPolymarketTradeBackfill,
  updatePolymarketWatchAsset,
} from "../api";
import PushNotificationToggle from "../components/PushNotificationToggle";

const REFRESH_INTERVAL = 15_000;
const STALE_THRESHOLD_MS = 10 * 60 * 1000;
const WATCH_PAGE_SIZE = 12;

export default function Health() {
  const [health, setHealth] = useState(null);
  const [streamStatus, setStreamStatus] = useState(null);
  const [watchAssets, setWatchAssets] = useState([]);
  const [watchAssetTotal, setWatchAssetTotal] = useState(0);
  const [error, setError] = useState(null);
  const [actionError, setActionError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isResyncing, setIsResyncing] = useState(false);
  const [isMetadataSyncing, setIsMetadataSyncing] = useState(false);
  const [isProjecting, setIsProjecting] = useState(false);
  const [isSnapshotting, setIsSnapshotting] = useState(false);
  const [isTradeBackfilling, setIsTradeBackfilling] = useState(false);
  const [isOiPolling, setIsOiPolling] = useState(false);
  const [isReconResyncing, setIsReconResyncing] = useState(false);
  const [isFeatureMaterializing, setIsFeatureMaterializing] = useState(false);
  const [isStructureBuilding, setIsStructureBuilding] = useState(false);
  const [isStructureScanning, setIsStructureScanning] = useState(false);
  const [isReplaying, setIsReplaying] = useState(false);
  const [updatingWatchAssetId, setUpdatingWatchAssetId] = useState(null);
  const intervalRef = useRef(null);

  const fetchData = useCallback(async () => {
    try {
      const [healthData, ingestData, watchAssetData] = await Promise.all([
        getHealth(),
        getPolymarketIngestStatus(),
        getPolymarketWatchAssets({ page: 1, pageSize: WATCH_PAGE_SIZE }),
      ]);
      setHealth(healthData);
      setStreamStatus(ingestData);
      setWatchAssets(watchAssetData.watch_assets || []);
      setWatchAssetTotal(watchAssetData.total || 0);
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

  const handleManualResync = async () => {
    try {
      setIsResyncing(true);
      setActionError(null);
      await triggerPolymarketResync({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsResyncing(false);
    }
  };

  const handleManualMetadataSync = async () => {
    try {
      setIsMetadataSyncing(true);
      setActionError(null);
      await triggerPolymarketMetadataSync({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsMetadataSyncing(false);
    }
  };

  const handleToggleWatch = async (watchAsset) => {
    try {
      setUpdatingWatchAssetId(watchAsset.id);
      setActionError(null);
      await updatePolymarketWatchAsset(watchAsset.id, {
        watch_enabled: !watchAsset.watch_enabled,
        watch_reason: !watchAsset.watch_enabled
          ? "manual_operator_enable"
          : "manual_operator_disable",
        priority: watchAsset.priority,
      });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setUpdatingWatchAssetId(null);
    }
  };

  const handleProjectorCatchup = async () => {
    try {
      setIsProjecting(true);
      setActionError(null);
      await triggerPolymarketRawProjector({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsProjecting(false);
    }
  };

  const handleManualBookSnapshot = async () => {
    try {
      setIsSnapshotting(true);
      setActionError(null);
      await triggerPolymarketBookSnapshot({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsSnapshotting(false);
    }
  };

  const handleTradeBackfill = async () => {
    try {
      setIsTradeBackfilling(true);
      setActionError(null);
      await triggerPolymarketTradeBackfill({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsTradeBackfilling(false);
    }
  };

  const handleOiPoll = async () => {
    try {
      setIsOiPolling(true);
      setActionError(null);
      await triggerPolymarketOiPoll({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsOiPolling(false);
    }
  };

  const handleBookReconResync = async () => {
    try {
      setIsReconResyncing(true);
      setActionError(null);
      await triggerPolymarketBookReconResync({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsReconResyncing(false);
    }
  };

  const handleFeatureMaterialization = async () => {
    try {
      setIsFeatureMaterializing(true);
      setActionError(null);
      await triggerPolymarketFeatureMaterialization({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsFeatureMaterializing(false);
    }
  };

  const handleStructureGroupBuild = async () => {
    try {
      setIsStructureBuilding(true);
      setActionError(null);
      await triggerPolymarketStructureGroupBuild({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsStructureBuilding(false);
    }
  };

  const handleStructureOpportunityScan = async () => {
    try {
      setIsStructureScanning(true);
      setActionError(null);
      await triggerPolymarketStructureOpportunityScan({ reason: "manual" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsStructureScanning(false);
    }
  };

  const handleReplayRun = async () => {
    try {
      setIsReplaying(true);
      setActionError(null);
      await triggerPolymarketReplay({ reason: "manual", run_type: "policy_compare" });
      await fetchData();
    } catch (e) {
      setActionError(e.message);
    } finally {
      setIsReplaying(false);
    }
  };

  if ((!health || !streamStatus) && !error) {
    return (
      <div>
        <h2 style={{ fontSize: 16, marginBottom: 16 }}>System Health</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
          <div className="skeleton" style={{ height: 70, borderRadius: 8 }} />
        </div>
        <div className="skeleton" style={{ height: 220, borderRadius: 8, marginTop: 18 }} />
        <div className="skeleton" style={{ height: 220, borderRadius: 8, marginTop: 18 }} />
      </div>
    );
  }

  if (error && !health && !streamStatus) {
    return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  }

  const ingestionRows = health?.ingestion || [];
  const recentIncidents = streamStatus?.recent_incidents || [];
  const recentRuns = streamStatus?.recent_resync_runs || [];
  const eventsIngested = streamStatus?.events_ingested || {};
  const metadataSync = streamStatus?.metadata_sync || null;
  const rawStorage = streamStatus?.raw_storage || null;
  const bookReconstruction = streamStatus?.book_reconstruction || health?.polymarket_phase4 || null;
  const featureStatus = streamStatus?.features || health?.polymarket_phase5 || null;
  const executionPolicy = streamStatus?.execution_policy || health?.polymarket_phase6 || null;
  const liveExecution = health?.polymarket_phase7a || null;
  const makerEconomics = streamStatus?.maker_economics || health?.polymarket_phase9 || null;
  const riskGraph = health?.polymarket_phase10 || null;
  const replayStatus = health?.polymarket_phase11 || null;
  const phase12Status = health?.polymarket_phase12 || null;
  const phase12AutonomyState = phase12Status?.autonomy_state || null;
  const phase12RecentIncidents = phase12Status?.recent_incidents || [];
  const phase12RecentGuardrails = phase12Status?.recent_guardrail_triggers || [];
  const structureStatus = health?.polymarket_phase8a || streamStatus?.structure_engine || null;
  const schedulerLease = health?.scheduler_lease || null;
  const defaultStrategyRuntime = health?.default_strategy_runtime || null;
  const runtimeInvariants = health?.runtime_invariants || [];
  const strategyFamilies = health?.strategy_families || streamStatus?.strategy_families || [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <h2 style={{ fontSize: 16 }}>System Health</h2>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <button
            onClick={fetchData}
            style={secondaryButtonStyle}
          >
            Refresh
          </button>
          {lastUpdated && (
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              Auto-refresh 15s | Updated {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>

      {error && (
        <InlineAlert tone="error">
          {error}
        </InlineAlert>
      )}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 12,
        }}
      >
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
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 14, fontWeight: 600 }}>Benchmark Runtime</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              Lease freshness, backlog, and evaluator guardrails for the frozen default strategy.
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
            }}
          >
            <StatCard label="Lease Owner" value={shortId(schedulerLease?.owner_token)} />
            <StatCard label="Lease Heartbeat" value={formatFreshness(schedulerLease?.heartbeat_freshness_seconds)} />
            <StatCard label="Lease Expiry" value={formatFreshness(schedulerLease?.expires_in_seconds)} />
            <StatCard label="Overdue Trades" value={defaultStrategyRuntime?.overdue_open_trades ?? 0} />
            <StatCard label="Last Backfill" value={formatShortDateTime(defaultStrategyRuntime?.last_resolution_backfill_at)} />
            <StatCard label="Backfill Count" value={defaultStrategyRuntime?.last_resolution_backfill_count ?? 0} />
            <StatCard label="Clamp Count (24h)" value={defaultStrategyRuntime?.evaluation_clamp_count_24h ?? 0} />
            <StatCard label="Last Eval Failure" value={formatShortDateTime(defaultStrategyRuntime?.last_evaluation_failure_at)} />
          </div>
          {schedulerLease?.owner_token && (
            <div style={{ marginTop: 12, fontSize: 11, color: "var(--text-dim)", fontFamily: "var(--mono)" }}>
              Owner token {schedulerLease.owner_token}
            </div>
          )}
        </div>

        <div style={panelStyle}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 14, fontWeight: 600 }}>Unattended Invariants</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              These checks should stay green if the worker is safe to leave unattended.
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {runtimeInvariants.length === 0 ? (
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>No runtime invariants reported yet.</div>
            ) : (
              runtimeInvariants.map((invariant) => (
                <div
                  key={invariant.key}
                  style={{
                    background: "rgba(255, 255, 255, 0.02)",
                    border: "1px solid var(--border)",
                    borderRadius: 10,
                    padding: 12,
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8, marginBottom: 6 }}>
                    <div style={{ fontSize: 12, fontWeight: 600 }}>{invariant.label}</div>
                    <span style={strategyPostureStyle(invariant.status)}>
                      {formatStrategyPosture(invariant.status)}
                    </span>
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
                    {invariant.detail}
                  </div>
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
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
          <div style={{ fontSize: 14, fontWeight: 600 }}>Strategy Families</div>
          <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
            Default benchmark stays frozen while structure and maker research advance.
          </div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
          {strategyFamilies.map((family) => (
            <div key={family.family} style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8, marginBottom: 8 }}>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 600 }}>{family.label}</div>
                  <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{family.primary_surface}</div>
                </div>
                <span style={strategyPostureStyle(family.posture)}>
                  {formatStrategyPosture(family.posture)}
                </span>
              </div>
              <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>{family.description}</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 8, marginTop: 10 }}>
                <StatCard label="Current Version" value={family.current_version?.version_label || "-"} />
                <StatCard label="Current Autonomy" value={formatAutonomyState(family.autonomy_state || family.current_version?.autonomy_state)} />
                <StatCard label="Budget" value={formatBudgetEnvelope(family.risk_budget_status)} />
                <StatCard label="Regime" value={formatContinuityStatus(family.risk_budget_status?.regime_label)} />
                <StatCard label="Capacity" value={formatContinuityStatus(family.risk_budget_status?.capacity_status)} />
                <StatCard label="Recent Breaches" value={(family.risk_budget_status?.reason_codes || []).length} />
                <StatCard label="Open Orders" value={`${family.risk_budget_status?.open_order_count ?? 0} / ${family.risk_budget_status?.effective_max_open_orders ?? "-"}`} />
              </div>
              {(family.autonomy_state || family.current_version?.autonomy_state) ? (
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 8 }}>
                  Eligibility {formatEligibilityVerdict(family.current_version?.latest_promotion_evaluation || family.latest_promotion_evaluation)} | Reason {formatAutonomyReason(family.autonomy_state || family.current_version?.autonomy_state)} | Recommended {titleCase((family.autonomy_state || family.current_version?.autonomy_state)?.recommended_autonomy_tier)} | Blockers {((family.autonomy_state || family.current_version?.autonomy_state)?.blocked_reasons || []).map(titleCase).join(", ") || "None"}
                </div>
              ) : null}
              {family.disabled_reason && (
                <div style={{ fontSize: 11, color: "var(--yellow)", marginTop: 8 }}>{family.disabled_reason}</div>
              )}
            </div>
          ))}
        </div>
      </section>

      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <h3 style={{ fontSize: 14, fontWeight: 600 }}>Polymarket Stream</h3>
              <StatusPill connected={streamStatus?.connected} />
              <span style={strategyPostureStyle(streamStatus?.continuity_status)}>
                {formatContinuityStatus(streamStatus?.continuity_status)}
              </span>
            </div>
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 6 }}>
              Connection {shortId(streamStatus?.current_connection_id)} | Last event {formatDateTime(streamStatus?.last_event_received_at)} | Heartbeat {formatFreshness(streamStatus?.heartbeat_freshness_seconds)}
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleManualResync}
              disabled={isResyncing}
              style={{
                ...primaryButtonStyle,
                opacity: isResyncing ? 0.65 : 1,
                cursor: isResyncing ? "wait" : "pointer",
              }}
            >
              {isResyncing ? "Resyncing..." : "Run Resync"}
            </button>
            <button
              onClick={handleManualMetadataSync}
              disabled={isMetadataSyncing || !metadataSync?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isMetadataSyncing || !metadataSync?.enabled ? 0.65 : 1,
                cursor: isMetadataSyncing ? "wait" : (!metadataSync?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isMetadataSyncing ? "Syncing..." : "Run Metadata Sync"}
            </button>
          </div>
        </div>

        {(streamStatus?.last_error || actionError) && (
          <div style={{ marginBottom: 16 }}>
            <InlineAlert tone="warning">
              {actionError || streamStatus.last_error}
            </InlineAlert>
          </div>
        )}

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
            gap: 12,
            marginBottom: 16,
          }}
        >
          <StatCard label="Reconnects" value={streamStatus?.reconnect_count ?? 0} />
          <StatCard label="Gap Suspicions" value={streamStatus?.gap_suspected_count ?? 0} />
          <StatCard label="Malformed" value={streamStatus?.malformed_message_count ?? 0} />
          <StatCard label="Continuity" value={formatContinuityStatus(streamStatus?.continuity_status)} />
          <StatCard label="Last Resync" value={formatShortDateTime(streamStatus?.last_successful_resync_at)} />
          <StatCard
            label="Watched / Subscribed"
            value={`${streamStatus?.watched_asset_count ?? 0} / ${streamStatus?.subscribed_asset_count ?? 0}`}
          />
          <StatCard
            label="Events 1 / 5 / 15m"
            value={`${eventsIngested["1m"] ?? 0} / ${eventsIngested["5m"] ?? 0} / ${eventsIngested["15m"] ?? 0}`}
          />
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 2 Metadata Registry</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {metadataSync?.enabled ? "Enabled" : "Disabled"} | Last success {formatShortDateTime(metadataSync?.last_successful_sync_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
            }}
          >
            <StatCard label="Last Run" value={metadataSync?.last_run_status || "-"} />
            <StatCard label="Param Changes (24h)" value={metadataSync?.recent_param_changes_24h ?? 0} />
            <StatCard
              label="Registry Rows"
              value={`${metadataSync?.registry_counts?.events ?? 0} / ${metadataSync?.registry_counts?.markets ?? 0} / ${metadataSync?.registry_counts?.assets ?? 0}`}
            />
            <StatCard
              label="Stale E / M / A"
              value={`${metadataSync?.stale_registry_counts?.events ?? 0} / ${metadataSync?.stale_registry_counts?.markets ?? 0} / ${metadataSync?.stale_registry_counts?.assets ?? 0}`}
            />
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 3 Raw Storage</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {rawStorage?.enabled ? "Enabled" : "Disabled"} | Projector lag {rawStorage?.projector_lag ?? 0}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Projector" value={rawStorage?.projector_last_run_status || "-"} />
            <StatCard
              label="Raw Watermark"
              value={`${rawStorage?.last_projected_raw_event_id ?? 0} / ${rawStorage?.latest_relevant_raw_event_id ?? 0}`}
            />
            <StatCard label="Book Snapshots" value={`${formatShortDateTime(rawStorage?.last_successful_book_snapshot_at)} | ${formatFreshness(rawStorage?.book_snapshot_freshness_seconds)}`} />
            <StatCard label="Trade Backfill" value={`${formatShortDateTime(rawStorage?.last_successful_trade_backfill_at)} | ${formatFreshness(rawStorage?.trade_backfill_freshness_seconds)}`} />
            <StatCard label="OI Poll" value={`${formatShortDateTime(rawStorage?.last_successful_oi_poll_at)} | ${formatFreshness(rawStorage?.oi_poll_freshness_seconds)}`} />
            <StatCard
              label="Rows 24h"
              value={`${rawStorage?.rows_inserted_24h?.book_snapshots ?? 0}/${rawStorage?.rows_inserted_24h?.book_deltas ?? 0}/${rawStorage?.rows_inserted_24h?.trade_tape ?? 0}`}
            />
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleProjectorCatchup}
              disabled={isProjecting || !rawStorage?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isProjecting || !rawStorage?.enabled ? 0.65 : 1,
                cursor: isProjecting ? "wait" : (!rawStorage?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isProjecting ? "Projecting..." : "Catch Up Projector"}
            </button>
            <button
              onClick={handleManualBookSnapshot}
              disabled={isSnapshotting || !rawStorage?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isSnapshotting || !rawStorage?.enabled ? 0.65 : 1,
                cursor: isSnapshotting ? "wait" : (!rawStorage?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isSnapshotting ? "Capturing..." : "Capture Books"}
            </button>
            <button
              onClick={handleTradeBackfill}
              disabled={isTradeBackfilling || !rawStorage?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isTradeBackfilling || !rawStorage?.enabled ? 0.65 : 1,
                cursor: isTradeBackfilling ? "wait" : (!rawStorage?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isTradeBackfilling ? "Backfilling..." : "Backfill Trades"}
            </button>
            <button
              onClick={handleOiPoll}
              disabled={isOiPolling || !rawStorage?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isOiPolling || !rawStorage?.enabled ? 0.65 : 1,
                cursor: isOiPolling ? "wait" : (!rawStorage?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isOiPolling ? "Polling..." : "Poll OI"}
            </button>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 4 Book Reconstruction</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {bookReconstruction?.enabled ? "Enabled" : "Disabled"} | Last repair {formatShortDateTime(bookReconstruction?.last_successful_resync_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard
              label="Live / Watched"
              value={`${bookReconstruction?.live_book_count ?? 0} / ${bookReconstruction?.watched_asset_count ?? 0}`}
            />
            <StatCard label="Drifted" value={bookReconstruction?.drifted_asset_count ?? 0} />
            <StatCard label="Stale" value={bookReconstruction?.stale_asset_count ?? 0} />
            <StatCard label="Resyncing" value={bookReconstruction?.resyncing_asset_count ?? 0} />
            <StatCard label="Degraded" value={bookReconstruction?.degraded_asset_count ?? 0} />
            <StatCard label="Incidents (24h)" value={bookReconstruction?.recent_incident_count ?? 0} />
            <StatCard label="Stale After" value={bookReconstruction?.stale_after_seconds != null ? `${bookReconstruction.stale_after_seconds}s` : "-"} />
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleBookReconResync}
              disabled={isReconResyncing || !bookReconstruction?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isReconResyncing || !bookReconstruction?.enabled ? 0.65 : 1,
                cursor: isReconResyncing ? "wait" : (!bookReconstruction?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isReconResyncing ? "Repairing..." : "Repair Books"}
            </button>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 5 Derived Research</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {featureStatus?.enabled ? "Enabled" : "Disabled"} | Last feature run {formatShortDateTime(featureStatus?.last_successful_feature_run_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard
              label="Buckets"
              value={(featureStatus?.bucket_widths_ms || []).length ? (featureStatus.bucket_widths_ms || []).join(" / ") : "-"}
            />
            <StatCard
              label="Horizons"
              value={(featureStatus?.label_horizons_ms || []).length ? (featureStatus.label_horizons_ms || []).join(" / ") : "-"}
            />
            <StatCard label="Feature Rows (24h)" value={featureStatus?.recent_feature_rows_24h ?? 0} />
            <StatCard label="Label Rows (24h)" value={featureStatus?.recent_label_rows_24h ?? 0} />
            <StatCard label="Incomplete Buckets" value={featureStatus?.incomplete_bucket_count_24h ?? 0} />
            <StatCard label="Last Label Run" value={formatShortDateTime(featureStatus?.last_successful_label_run_at)} />
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleFeatureMaterialization}
              disabled={isFeatureMaterializing || !featureStatus?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isFeatureMaterializing || !featureStatus?.enabled ? 0.65 : 1,
                cursor: isFeatureMaterializing ? "wait" : (!featureStatus?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isFeatureMaterializing ? "Materializing..." : "Materialize Features"}
            </button>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 6 Execution Policy</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {executionPolicy?.enabled ? "Enabled" : "Disabled"} | Last decision {formatShortDateTime(executionPolicy?.last_successful_decision_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
            }}
          >
            <StatCard label="Recent Decisions (24h)" value={executionPolicy?.recent_decisions_24h ?? 0} />
            <StatCard
              label="Action Mix"
              value={formatActionMix(executionPolicy?.recent_action_mix)}
            />
            <StatCard label="Invalid Candidates" value={executionPolicy?.recent_invalid_candidates_24h ?? 0} />
            <StatCard label="Skip Decisions" value={executionPolicy?.recent_skip_decisions_24h ?? 0} />
            <StatCard
              label="Avg Net EV"
              value={executionPolicy?.recent_avg_est_net_ev_bps != null ? `${Number(executionPolicy.recent_avg_est_net_ev_bps).toFixed(1)} bps` : "-"}
            />
            <StatCard
              label="Horizon / Labels"
              value={executionPolicy ? `${executionPolicy.default_horizon_ms}ms / ${executionPolicy.passive_min_label_rows}` : "-"}
            />
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 7A OMS/EMS Foundation</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {liveExecution?.enabled ? "Live enabled" : "Live disabled"} | {liveExecution?.dry_run ? "Dry-run" : "Venue submit"} | {liveExecution?.manual_approval_required ? "Manual approval" : "Auto approval"}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
            }}
          >
            <StatCard label="Gateway" value={liveExecution?.gateway_reachable ? "Reachable" : "Unreachable"} />
            <StatCard label="User Stream" value={liveExecution?.user_stream_connected ? "Connected" : "Disconnected"} />
            <StatCard label="Kill Switch" value={liveExecution?.kill_switch_enabled ? "On" : "Off"} />
            <StatCard label="Open Live Orders" value={liveExecution?.outstanding_live_orders ?? 0} />
            <StatCard
              label="Reservations"
              value={liveExecution?.outstanding_reservations != null ? Number(liveExecution.outstanding_reservations).toFixed(2) : "-"}
            />
            <StatCard label="Fills (24h)" value={liveExecution?.recent_fills_24h ?? 0} />
            <StatCard label="Last Reconcile" value={formatShortDateTime(liveExecution?.last_reconcile_success_at)} />
            <StatCard label="Last User Msg" value={formatShortDateTime(liveExecution?.last_user_stream_message_at)} />
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 8B Validation, Controls, and Paper Routing</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {structureStatus?.enabled ? "Enabled" : "Disabled"} | Last scan {formatShortDateTime(structureStatus?.last_successful_scan_at)} | Last validation {formatShortDateTime(structureStatus?.last_successful_validation_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Last Group Build" value={formatShortDateTime(structureStatus?.last_successful_group_build_at)} />
            <StatCard label="Last Scan" value={formatShortDateTime(structureStatus?.last_successful_scan_at)} />
            <StatCard label="Last Validation" value={formatShortDateTime(structureStatus?.last_successful_validation_at)} />
            <StatCard label="Last Paper Plan" value={formatShortDateTime(structureStatus?.last_successful_paper_plan_at)} />
            <StatCard label="Last Paper Route" value={formatShortDateTime(structureStatus?.last_successful_paper_route_at)} />
            <StatCard label="Last Prune" value={formatShortDateTime(structureStatus?.last_successful_retention_prune_at)} />
            <StatCard label="Actionable" value={formatActionMix(structureStatus?.recent_actionable_by_type)} />
            <StatCard label="Non-Executable" value={structureStatus?.recent_non_executable_count ?? 0} />
            <StatCard label="Augmented / Info" value={structureStatus?.informational_augmented_group_count ?? 0} />
            <StatCard label="Groups" value={formatActionMix(structureStatus?.active_group_counts)} />
            <StatCard label="Executable" value={structureStatus?.executable_candidate_count ?? 0} />
            <StatCard label="Informational" value={structureStatus?.informational_only_opportunity_count ?? 0} />
            <StatCard label="Blocked" value={structureStatus?.blocked_opportunity_count ?? 0} />
            <StatCard label="Pending Approval" value={structureStatus?.pending_approval_count ?? 0} />
            <StatCard label="Stale Links" value={structureStatus?.stale_cross_venue_link_count ?? 0} />
            <StatCard label="Skipped Groups" value={structureStatus?.skipped_group_count ?? 0} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 12, marginBottom: 12 }}>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Opportunity Types</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {formatActionMix(structureStatus?.opportunity_counts_by_type)}
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Validation Reasons</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {formatActionMix(structureStatus?.validation_reason_counts)}
              </div>
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleStructureGroupBuild}
              disabled={isStructureBuilding || !structureStatus?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isStructureBuilding || !structureStatus?.enabled ? 0.65 : 1,
                cursor: isStructureBuilding ? "wait" : (!structureStatus?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isStructureBuilding ? "Building..." : "Build Groups"}
            </button>
            <button
              onClick={handleStructureOpportunityScan}
              disabled={isStructureScanning || !structureStatus?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isStructureScanning || !structureStatus?.enabled ? 0.65 : 1,
                cursor: isStructureScanning ? "wait" : (!structureStatus?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isStructureScanning ? "Scanning..." : "Scan Opportunities"}
            </button>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 9 Maker Economics</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {makerEconomics?.enabled ? "Enabled" : "Disabled"} | Fee sync {formatShortDateTime(makerEconomics?.last_fee_sync_at)} | Reward sync {formatShortDateTime(makerEconomics?.last_reward_sync_at)}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Fee Rows" value={makerEconomics?.fee_history_rows ?? 0} />
            <StatCard label="Reward Rows" value={makerEconomics?.reward_history_rows ?? 0} />
            <StatCard label="Snapshots" value={makerEconomics?.economics_snapshot_rows ?? 0} />
            <StatCard label="Recommendations" value={makerEconomics?.quote_recommendation_rows ?? 0} />
            <StatCard label="Fee Freshness" value={makerEconomics?.fee_freshness_seconds != null ? `${makerEconomics.fee_freshness_seconds}s` : "-"} />
            <StatCard label="Reward Freshness" value={makerEconomics?.reward_freshness_seconds != null ? `${makerEconomics.reward_freshness_seconds}s` : "-"} />
            <StatCard label="Max Quote Notional" value={makerEconomics?.quote_optimizer_max_notional != null ? Number(makerEconomics.quote_optimizer_max_notional).toFixed(2) : "-"} />
            <StatCard label="Max Input Age" value={makerEconomics?.quote_optimizer_max_age_seconds != null ? `${makerEconomics.quote_optimizer_max_age_seconds}s` : "-"} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 12 }}>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Reward States</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {formatActionMix(makerEconomics?.reward_state_counts)}
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Recent Reason Codes</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {formatActionMix(makerEconomics?.recent_reason_counts_24h)}
              </div>
            </div>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 10 Risk Graph and Portfolio Optimizer</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {riskGraph?.enabled ? "Enabled" : "Disabled"} | Family budgets now fail-closed where lifecycle policy exists | Live disabled {riskGraph?.live_disabled_by_default ? "yes" : "no"}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Last Graph Build" value={formatShortDateTime(riskGraph?.last_successful_graph_build_at)} />
            <StatCard label="Last Snapshot" value={formatShortDateTime(riskGraph?.last_successful_exposure_snapshot_at)} />
            <StatCard label="Last Optimizer" value={formatShortDateTime(riskGraph?.last_successful_optimizer_run_at)} />
            <StatCard label="Graph Status" value={riskGraph?.last_graph_build_status || "-"} />
            <StatCard label="Snapshot Status" value={riskGraph?.last_exposure_snapshot_status || "-"} />
            <StatCard label="Optimizer Status" value={riskGraph?.last_optimizer_status || "-"} />
            <StatCard
              label="Maker Budget"
              value={riskGraph?.maker_budget_usd != null ? `${Number(riskGraph.maker_budget_used_usd || 0).toFixed(2)} / ${Number(riskGraph.maker_budget_usd).toFixed(2)}` : "-"}
            />
            <StatCard
              label="Taker Budget"
              value={riskGraph?.taker_budget_usd != null ? `${Number(riskGraph.taker_budget_used_usd || 0).toFixed(2)} / ${Number(riskGraph.taker_budget_usd).toFixed(2)}` : "-"}
            />
            <StatCard
              label="Maker Utilization"
              value={riskGraph?.maker_budget_utilization != null ? `${Math.round(riskGraph.maker_budget_utilization * 100)}%` : "-"}
            />
            <StatCard
              label="Taker Utilization"
              value={riskGraph?.taker_budget_utilization != null ? `${Math.round(riskGraph.taker_budget_utilization * 100)}%` : "-"}
            />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 12 }}>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Top Concentrated Buckets</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {riskGraph?.top_concentrated_exposures?.length
                  ? riskGraph.top_concentrated_exposures.map((row) => `${row.node_type}:${row.label || row.node_key}`).join(" | ")
                  : "-"}
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Recent Blocks / No-Quote</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {formatActionMix(riskGraph?.recent_block_reason_counts_24h)}
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Family Budget Pressure</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {strategyFamilies.length
                  ? strategyFamilies.map((row) => `${row.family}:${row.risk_budget_status?.capacity_status || "unknown"}:${row.risk_budget_status?.regime_label || "unknown"}`).join(" | ")
                  : "-"}
              </div>
            </div>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 11 Replay Simulator and Backtest Expansion</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {replayStatus?.enabled ? "Enabled" : "Disabled"} | Advisory only | Live disabled {replayStatus?.live_disabled_by_default ? "yes" : "no"}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Last Replay" value={formatShortDateTime(replayStatus?.last_replay_run?.started_at)} />
            <StatCard label="Last Policy Compare" value={formatShortDateTime(replayStatus?.last_successful_policy_comparison?.started_at)} />
            <StatCard label="Scenarios (24h)" value={replayStatus?.recent_scenario_count_24h ?? 0} />
            <StatCard label="Coverage-Limited Runs" value={replayStatus?.recent_coverage_limited_run_count_24h ?? 0} />
            <StatCard label="Failed Runs" value={replayStatus?.recent_failed_run_count_24h ?? 0} />
            <StatCard
              label="Window / Timeout"
              value={replayStatus ? `${replayStatus.default_window_minutes}m / ${replayStatus.passive_fill_timeout_seconds}s` : "-"}
            />
            <StatCard label="Coverage Mode" value={replayStatus?.coverage_mode || "-"} />
            <StatCard label="Supported Detectors" value={(replayStatus?.supported_detectors || []).join(" | ") || "-"} />
            <StatCard label="Unsupported Detectors" value={(replayStatus?.unsupported_detectors || []).join(" | ") || "-"} />
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            {(Object.entries(replayStatus?.recent_variant_summary || {})).length === 0 ? (
              <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12, fontSize: 12, color: "var(--text-dim)" }}>
                No replay policy comparison metrics yet.
              </div>
            ) : (
              Object.entries(replayStatus?.recent_variant_summary || {}).map(([variantName, metric]) => (
                <div key={variantName} style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>{variantName}</div>
                  <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                    {formatReplayVariantSummary(metric)}
                  </div>
                </div>
              ))
            )}
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button
              onClick={handleReplayRun}
              disabled={isReplaying || !replayStatus?.enabled}
              style={{
                ...secondaryButtonStyle,
                opacity: isReplaying || !replayStatus?.enabled ? 0.65 : 1,
                cursor: isReplaying ? "wait" : (!replayStatus?.enabled ? "not-allowed" : "pointer"),
              }}
            >
              {isReplaying ? "Replaying..." : "Run Replay"}
            </button>
          </div>
        </div>

        <div
          style={{
            background: "rgba(255, 255, 255, 0.02)",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 14,
            marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Phase 12 Live Pilot and Control Plane</div>
            <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
              {phase12Status?.live_trading_enabled ? "Live enabled" : "Live disabled"} | {phase12Status?.pilot_armed ? "Armed" : "Disarmed"} | {phase12Status?.manual_approval_required ? "Manual approval" : "Approval bypassed"}
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <StatCard label="Pilot Enabled" value={phase12Status?.pilot_enabled ? "Yes" : "No"} />
            <StatCard label="Pilot Paused" value={phase12Status?.pilot_paused ? "Yes" : "No"} />
            <StatCard label="Active Family" value={phase12Status?.active_pilot_family || "none"} />
            <StatCard label="Lifecycle Version" value={formatLifecycleVersion(phase12Status)} />
            <StatCard label="Autonomy State" value={formatAutonomyState(phase12AutonomyState)} />
            <StatCard label="Gate Verdict" value={formatLifecycleGate(phase12Status)} />
            <StatCard label="Submission Mode" value={titleCase(phase12AutonomyState?.submission_mode)} />
            <StatCard label="Approval Queue" value={phase12Status?.approval_queue_count ?? 0} />
            <StatCard label="Heartbeat" value={phase12Status?.heartbeat_status || "-"} />
            <StatCard label="User Stream" value={phase12Status?.user_stream_connected ? "Connected" : "Disconnected"} />
            <StatCard label="Incidents (24h)" value={phase12Status?.recent_incident_count_24h ?? 0} />
            <StatCard label="Expired (24h)" value={phase12Status?.approval_expired_count_24h ?? 0} />
            <StatCard label="Kill Switch" value={phase12Status?.kill_switch_enabled ? "On" : "Off"} />
            <StatCard label="Last Reconcile" value={formatShortDateTime(phase12Status?.last_reconcile_success_at)} />
            <StatCard label="Avg Gap" value={formatBps(phase12Status?.live_shadow_summary?.average_gap_bps_24h)} />
            <StatCard label="Worst Gap" value={formatBps(phase12Status?.live_shadow_summary?.worst_gap_bps_24h)} />
            <StatCard label="Gap Breaches" value={phase12Status?.live_shadow_summary?.breach_count_24h ?? 0} />
            <StatCard label="Daily Net P&L" value={formatUsd(phase12Status?.daily_realized_pnl?.net_realized_pnl)} />
            <StatCard label="Readiness" value={phase12Status?.latest_readiness_status || "manual_only"} />
          </div>
          {phase12AutonomyState ? (
            <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 12 }}>
              Recommended tier {titleCase(phase12AutonomyState.recommended_autonomy_tier)} | Autonomy reason {formatAutonomyReason(phase12AutonomyState)} | Blockers {(phase12AutonomyState.blocked_reasons || []).map(titleCase).join(", ") || "None"}
            </div>
          ) : null}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 12 }}>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Pilot Semantics</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                One narrow pilot family only. Live submission stays fail-closed until the pilot is explicitly armed and each eligible live intent is approved.
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Shadow Evaluation</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {phase12Status?.live_shadow_summary?.recent_count_24h ?? 0} comparisons in the last 24h with conservative coverage limits carried forward from replay provenance.
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Evidence Loop</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                Realized P&amp;L, approval expirations, guardrail triggers, and readiness remain operator-visible before any broader automation discussion.
              </div>
            </div>
            <div style={{ background: "rgba(255, 255, 255, 0.02)", border: "1px solid var(--border)", borderRadius: 10, padding: 12 }}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Recent Guardrails</div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                {phase12RecentGuardrails
                  .slice(0, 3)
                  .map((event) => `${event.guardrail_type} @ ${formatLifecycleVersion(event)}`)
                  .join(", ") || "No recent triggers."}
              </div>
            </div>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 16, marginTop: 16 }}>
            <TablePanel
              title="Recent Pilot Incidents"
              subtitle={`${phase12RecentIncidents.length} most recent`}
              emptyLabel="No recent pilot incidents recorded."
              columns={["When", "Type", "Version", "Gate / Autonomy", "Summary"]}
              rows={phase12RecentIncidents.map((incident) => [
                formatShortDateTime(incident.observed_at_local || incident.created_at),
                incident.incident_type,
                formatLifecycleVersion(incident),
                renderGateAutonomy(incident),
                summarizeIncident(incident),
              ])}
            />
            <TablePanel
              title="Recent Pilot Guardrails"
              subtitle={`${phase12RecentGuardrails.length} most recent`}
              emptyLabel="No recent pilot guardrails recorded."
              columns={["When", "Guardrail", "Version", "Gate / Autonomy", "Action"]}
              rows={phase12RecentGuardrails.map((event) => [
                formatShortDateTime(event.observed_at_local),
                event.guardrail_type,
                formatLifecycleVersion(event),
                renderGateAutonomy(event),
                event.action_taken || "-",
              ])}
            />
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 16 }}>
          <TablePanel
            title="Recent Incidents"
            subtitle={`${recentIncidents.length} most recent`}
            emptyLabel="No stream incidents recorded yet."
            columns={["When", "Type", "Severity", "Summary"]}
            rows={recentIncidents.map((incident) => [
              formatShortDateTime(incident.created_at),
              incident.incident_type,
              incident.severity,
              summarizeIncident(incident),
            ])}
          />

          <TablePanel
            title="Recent Resync Runs"
            subtitle={`${recentRuns.length} most recent`}
            emptyLabel="No resync runs recorded yet."
            columns={["Started", "Reason", "Status", "Assets"]}
            rows={recentRuns.map((run) => [
              formatShortDateTime(run.started_at),
              run.reason,
              run.status,
              `${run.succeeded_asset_count}/${run.requested_asset_count}`,
            ])}
          />
        </div>
      </section>

      <section>
        <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Scheduled Ingestion</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {ingestionRows.map((ing) => {
            const isStale =
              ing.last_run && Date.now() - new Date(ing.last_run).getTime() > STALE_THRESHOLD_MS;

            return (
              <div
                key={ing.run_type}
                style={{
                  background: "var(--bg-card)",
                  border: `1px solid ${isStale ? "var(--yellow)" : "var(--border)"}`,
                  borderRadius: 8,
                  padding: "12px 16px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  gap: 12,
                  flexWrap: "wrap",
                  fontSize: 13,
                }}
              >
                <span style={{ fontWeight: 500, minWidth: 140 }}>{ing.run_type}</span>
                <span
                  style={{
                    color:
                      ing.last_status === "success"
                        ? "var(--green)"
                        : ing.last_status === "failed"
                        ? "var(--red)"
                        : "var(--text-dim)",
                  }}
                >
                  {ing.last_status || "never run"}
                </span>
                <span style={{ color: isStale ? "var(--yellow)" : "var(--text-dim)" }}>
                  {formatDateTime(ing.last_run)}
                  {isStale && " (stale)"}
                </span>
                <span style={{ fontFamily: "var(--mono)" }}>
                  {ing.markets_processed != null ? `${ing.markets_processed} mkts` : "-"}
                </span>
              </div>
            );
          })}
        </div>
      </section>

      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
          <div>
            <h3 style={{ fontSize: 14, fontWeight: 600 }}>Watch Registry</h3>
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 4 }}>
              Showing {watchAssets.length} of {watchAssetTotal} watch assets
            </div>
          </div>
        </div>

        <div className="table-scroll">
          <table style={tableStyle}>
            <thead>
              <tr>
                <TableHead>Asset</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Question</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Priority</TableHead>
                <TableHead>Action</TableHead>
              </tr>
            </thead>
            <tbody>
              {watchAssets.length === 0 ? (
                <tr>
                  <td colSpan={6} style={emptyCellStyle}>No watch assets configured.</td>
                </tr>
              ) : (
                watchAssets.map((watchAsset) => {
                  const isUpdating = updatingWatchAssetId === watchAsset.id;
                  return (
                    <tr key={watchAsset.id} style={{ borderTop: "1px solid var(--border)" }}>
                      <TableCell mono>{watchAsset.asset_id}</TableCell>
                      <TableCell>{watchAsset.outcome_name}</TableCell>
                      <TableCell>{watchAsset.market_question}</TableCell>
                      <TableCell>
                        <span style={{ color: watchAsset.watch_enabled ? "var(--green)" : "var(--text-dim)" }}>
                          {watchAsset.watch_enabled ? "Watching" : "Paused"}
                        </span>
                      </TableCell>
                      <TableCell>{watchAsset.priority ?? "-"}</TableCell>
                      <TableCell>
                        <button
                          onClick={() => handleToggleWatch(watchAsset)}
                          disabled={isUpdating}
                          style={{
                            ...secondaryButtonStyle,
                            minHeight: 32,
                            padding: "6px 10px",
                            opacity: isUpdating ? 0.65 : 1,
                            cursor: isUpdating ? "wait" : "pointer",
                          }}
                        >
                          {isUpdating
                            ? "Saving..."
                            : watchAsset.watch_enabled
                            ? "Disable"
                            : "Enable"}
                        </button>
                      </TableCell>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: "12px 16px",
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 20, fontFamily: "var(--mono)", fontWeight: 600 }}>{value}</div>
    </div>
  );
}

function StatusPill({ connected }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        borderRadius: 999,
        padding: "4px 10px",
        fontSize: 12,
        border: `1px solid ${connected ? "rgba(0, 214, 143, 0.35)" : "var(--border)"}`,
        color: connected ? "var(--green)" : "var(--text-dim)",
        background: connected ? "rgba(0, 214, 143, 0.08)" : "transparent",
      }}
    >
      {connected ? "Connected" : "Disconnected"}
    </span>
  );
}

function TablePanel({ title, subtitle, emptyLabel, columns, rows }) {
  return (
    <div
      style={{
        background: "rgba(255, 255, 255, 0.02)",
        border: "1px solid var(--border)",
        borderRadius: 10,
        padding: 14,
        minWidth: 0,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
        <div style={{ fontSize: 13, fontWeight: 600 }}>{title}</div>
        <div style={{ fontSize: 11, color: "var(--text-dim)" }}>{subtitle}</div>
      </div>
      <div className="table-scroll">
        <table style={tableStyle}>
          <thead>
            <tr>
              {columns.map((column) => (
                <TableHead key={column}>{column}</TableHead>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} style={emptyCellStyle}>{emptyLabel}</td>
              </tr>
            ) : (
              rows.map((row, index) => (
                <tr key={`${title}-${index}`} style={{ borderTop: "1px solid var(--border)" }}>
                  {row.map((cell, cellIndex) => (
                    <TableCell key={`${title}-${index}-${cellIndex}`}>{cell}</TableCell>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TableHead({ children }) {
  return (
    <th
      style={{
        padding: "0 0 10px",
        textAlign: "left",
        fontSize: 11,
        color: "var(--text-dim)",
        fontWeight: 500,
        whiteSpace: "nowrap",
      }}
    >
      {children}
    </th>
  );
}

function TableCell({ children, mono = false }) {
  return (
    <td
      style={{
        padding: "10px 10px 10px 0",
        fontSize: 12,
        verticalAlign: "top",
        fontFamily: mono ? "var(--mono)" : "inherit",
        color: "var(--text)",
      }}
    >
      {children}
    </td>
  );
}

function InlineAlert({ tone, children }) {
  const color = tone === "error" ? "var(--red)" : "var(--yellow)";
  return (
    <div
      style={{
        background: "rgba(255, 255, 255, 0.03)",
        border: `1px solid ${color}`,
        color,
        borderRadius: 8,
        padding: "10px 12px",
        fontSize: 12,
      }}
    >
      {children}
    </div>
  );
}

function summarizeIncident(incident) {
  const details = incident.details_json || {};
  if (details.reason) return details.reason;
  if (details.error) return details.error;
  if (details.current_sequence != null) {
    return `sequence ${details.previous_sequence ?? "?"} -> ${details.current_sequence}`;
  }
  if (details.to_subscribe || details.to_unsubscribe) {
    return `+${(details.to_subscribe || []).length} / -${(details.to_unsubscribe || []).length}`;
  }
  if (incident.asset_id) return incident.asset_id;
  return "operator event";
}

function formatDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function formatShortDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
}

function shortId(value) {
  if (!value) return "-";
  const text = String(value);
  if (text.length <= 12) return text;
  return `${text.slice(0, 8)}...${text.slice(-4)}`;
}

function formatActionMix(value) {
  if (!value || Object.keys(value).length === 0) return "-";
  return Object.entries(value)
    .map(([action, count]) => `${action}:${count}`)
    .join(" | ");
}

function formatBudgetEnvelope(status) {
  if (!status) return "-";
  return `${Number(status.current_outstanding_usd || 0).toFixed(2)} / ${Number(status.effective_outstanding_cap_usd || 0).toFixed(2)}`;
}

function formatFreshness(value) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return `${Number(value)}s`;
}

function formatContinuityStatus(value) {
  if (!value) return "Unknown";
  return String(value).replace(/_/g, " ");
}

function formatStrategyPosture(value) {
  if (!value) return "unknown";
  return String(value).replace(/_/g, " ");
}

function strategyPostureStyle(value) {
  const normalized = String(value || "").toLowerCase();
  const isGood = normalized === "healthy" || normalized === "research_active" || normalized === "passing";
  const isWarning = normalized === "stale" || normalized === "advisory_only" || normalized === "benchmark_only" || normalized === "awaiting_events" || normalized === "not_applicable";
  const isBad = normalized === "disabled" || normalized === "disconnected" || normalized === "failing" || normalized === "failed";
  const borderColor = isGood
    ? "rgba(0, 214, 143, 0.35)"
    : isBad
      ? "rgba(255, 112, 112, 0.35)"
      : "rgba(255, 214, 61, 0.35)";
  const color = isGood ? "var(--green)" : isBad ? "var(--red)" : "var(--yellow)";
  const background = isGood
    ? "rgba(0, 214, 143, 0.08)"
    : isBad
      ? "rgba(255, 112, 112, 0.08)"
      : "rgba(255, 214, 61, 0.08)";
  return {
    display: "inline-flex",
    alignItems: "center",
    borderRadius: 999,
    padding: "4px 10px",
    fontSize: 11,
    fontWeight: 600,
    textTransform: "uppercase",
    border: `1px solid ${borderColor}`,
    color,
    background,
  };
}

function formatUsd(value) {
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
  return formatEligibilityVerdict(row?.latest_promotion_evaluation);
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

function formatEligibilityVerdict(gate) {
  if (!gate) return "-";
  if (gate.evaluation_kind === "promotion_eligibility_gate") {
    return gate.summary_json?.decision?.eligible ? "Eligible" : "Not Eligible";
  }
  return titleCase(gate.evaluation_status);
}

function renderGateAutonomy(row) {
  const gate = row?.latest_promotion_evaluation;
  if (!gate) return "-";
  return (
    <div>
      <div>{formatEligibilityVerdict(gate)}</div>
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{titleCase(gate.autonomy_tier)}</div>
    </div>
  );
}

function formatReplayVariantSummary(metric) {
  if (!metric) return "-";
  const netPnl = metric.net_pnl != null ? Number(metric.net_pnl).toFixed(4) : "-";
  const fillRate = metric.fill_rate != null ? `${Math.round(Number(metric.fill_rate) * 100)}%` : "-";
  const slippage = metric.slippage_bps != null ? `${Number(metric.slippage_bps).toFixed(1)} bps` : "-";
  return `Net ${netPnl} | Fill ${fillRate} | Slip ${slippage}`;
}

const panelStyle = {
  background: "var(--bg-card)",
  border: "1px solid var(--border)",
  borderRadius: 12,
  padding: 16,
};

const tableStyle = {
  width: "100%",
  borderCollapse: "collapse",
};

const emptyCellStyle = {
  padding: "18px 0 6px",
  fontSize: 12,
  color: "var(--text-dim)",
};

const primaryButtonStyle = {
  background: "var(--green)",
  color: "#fff",
  border: "none",
  borderRadius: 8,
  padding: "8px 14px",
  fontSize: 13,
  fontWeight: 600,
};

const secondaryButtonStyle = {
  background: "transparent",
  color: "var(--text)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: "8px 14px",
  fontSize: 13,
};
