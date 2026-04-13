import { useCallback, useEffect, useRef, useState } from "react";
import {
  getHealth,
  getPolymarketIngestStatus,
  getPolymarketWatchAssets,
  triggerPolymarketBookSnapshot,
  triggerPolymarketMetadataSync,
  triggerPolymarketBookReconResync,
  triggerPolymarketOiPoll,
  triggerPolymarketRawProjector,
  triggerPolymarketResync,
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

      <section>
        <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Notifications</h3>
        <PushNotificationToggle />
      </section>

      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <h3 style={{ fontSize: 14, fontWeight: 600 }}>Polymarket Stream</h3>
              <StatusPill connected={streamStatus?.connected} />
            </div>
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 6 }}>
              Connection {shortId(streamStatus?.current_connection_id)} | Last event {formatDateTime(streamStatus?.last_event_received_at)}
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
            <StatCard label="Book Snapshots" value={formatShortDateTime(rawStorage?.last_successful_book_snapshot_at)} />
            <StatCard label="Trade Backfill" value={formatShortDateTime(rawStorage?.last_successful_trade_backfill_at)} />
            <StatCard label="OI Poll" value={formatShortDateTime(rawStorage?.last_successful_oi_poll_at)} />
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
