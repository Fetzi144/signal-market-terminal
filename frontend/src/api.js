function resolveApiBase() {
  const configuredBase = import.meta.env.VITE_API_BASE;
  if (configuredBase) {
    return `${configuredBase.replace(/\/$/, "")}/api/v1`;
  }

  if (
    typeof window !== "undefined"
    && ["4173", "5173"].includes(window.location.port)
  ) {
    return `http://${window.location.hostname}:8001/api/v1`;
  }

  return "/api/v1";
}

const API_BASE = resolveApiBase();

async function requestJson(url, options = {}) {
  const init = {
    ...options,
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  };
  const res = await fetch(url, init);
  if (!res.ok) {
    let message = `API ${res.status}: ${res.statusText}`;
    try {
      const payload = await res.json();
      message = payload.detail || payload.message || message;
    } catch {
      // Ignore non-JSON error bodies and fall back to the HTTP status text.
    }
    throw new Error(message);
  }
  if (res.status === 204) return null;
  return res.json();
}

async function fetchJson(url) {
  return requestJson(url);
}

export function getSignals({ page = 1, pageSize = 50, signalType, marketId, platform, timeframe, resolvedCorrectly } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (signalType) params.set("signal_type", signalType);
  if (marketId) params.set("market_id", marketId);
  if (platform) params.set("platform", platform);
  if (timeframe) params.set("timeframe", timeframe);
  if (resolvedCorrectly !== undefined && resolvedCorrectly !== "") params.set("resolved_correctly", resolvedCorrectly);
  return fetchJson(`${API_BASE}/signals?${params}`);
}

export function getSignal(id) {
  return fetchJson(`${API_BASE}/signals/${id}`);
}

export function getMarkets({ page = 1, pageSize = 50, platform, search, category, sortBy } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (platform) params.set("platform", platform);
  if (search) params.set("search", search);
  if (category) params.set("category", category);
  if (sortBy) params.set("sort_by", sortBy);
  return fetchJson(`${API_BASE}/markets?${params}`);
}

export function getMarket(id) {
  return fetchJson(`${API_BASE}/markets/${id}`);
}

export function getMarketSnapshots(id, limit = 100) {
  return fetchJson(`${API_BASE}/markets/${id}/snapshots?limit=${limit}`);
}

export function getHealth() {
  return fetchJson(`${API_BASE}/health`);
}

export function getPolymarketIngestStatus() {
  return fetchJson(`${API_BASE}/ingest/polymarket/status`);
}

export function getPolymarketStructureStatus() {
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/status`);
}

export function getPolymarketIncidents({ page = 1, pageSize = 20 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  return fetchJson(`${API_BASE}/ingest/polymarket/incidents?${params}`);
}

export function getPolymarketResyncRuns({ page = 1, pageSize = 20 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  return fetchJson(`${API_BASE}/ingest/polymarket/resync-runs?${params}`);
}

export function getPolymarketMetaSyncStatus() {
  return fetchJson(`${API_BASE}/ingest/polymarket/meta-sync/status`);
}

export function getPolymarketMetaSyncRuns({ page = 1, pageSize = 20 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  return fetchJson(`${API_BASE}/ingest/polymarket/meta-sync/runs?${params}`);
}

export function getPolymarketMakerEconomicsStatus() {
  return fetchJson(`${API_BASE}/ingest/polymarket/maker-economics/status`);
}

export function getPolymarketCurrentFeeState({ assetId, conditionId, asOf, limit = 50 } = {}) {
  const params = new URLSearchParams({ limit });
  if (assetId) params.set("asset_id", assetId);
  if (conditionId) params.set("condition_id", conditionId);
  if (asOf) params.set("as_of", asOf);
  return fetchJson(`${API_BASE}/ingest/polymarket/maker-economics/fees/current?${params}`);
}

export function getPolymarketFeeHistory({ assetId, conditionId, start, end, limit = 100 } = {}) {
  const params = new URLSearchParams({ limit });
  if (assetId) params.set("asset_id", assetId);
  if (conditionId) params.set("condition_id", conditionId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/maker-economics/fees/history?${params}`);
}

export function getPolymarketCurrentRewardState({ conditionId, asOf, limit = 50 } = {}) {
  const params = new URLSearchParams({ limit });
  if (conditionId) params.set("condition_id", conditionId);
  if (asOf) params.set("as_of", asOf);
  return fetchJson(`${API_BASE}/ingest/polymarket/maker-economics/rewards/current?${params}`);
}

export function getPolymarketRewardHistory({ conditionId, start, end, limit = 100 } = {}) {
  const params = new URLSearchParams({ limit });
  if (conditionId) params.set("condition_id", conditionId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/maker-economics/rewards/history?${params}`);
}

export function getPolymarketWatchAssets({ page = 1, pageSize = 20 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  return fetchJson(`${API_BASE}/ingest/polymarket/watch-assets?${params}`);
}

export function createPolymarketWatchAsset(body) {
  return requestJson(`${API_BASE}/ingest/polymarket/watch-assets`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updatePolymarketWatchAsset(id, body) {
  return requestJson(`${API_BASE}/ingest/polymarket/watch-assets/${id}`, {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketResync(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/resync`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketMetadataSync(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/meta-sync`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketRawProjector(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/raw/project`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketBookSnapshot(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/raw/book-snapshots/trigger`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketTradeBackfill(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/raw/trade-backfill/trigger`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketOiPoll(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/raw/oi-poll/trigger`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketBookReconResync(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/reconstruction/resync`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketFeatureMaterialization(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/features/materialize`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketStructureGroupBuild(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/groups/build`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketStructureOpportunityScan(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/opportunities/scan`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerPolymarketReplay(body = { reason: "manual", run_type: "policy_compare" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/replay/trigger`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function validatePolymarketStructureOpportunities(body = { reason: "manual" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/opportunities/validate`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getPolymarketStructureOpportunities({
  groupType,
  opportunityType,
  eventSlug,
  classification,
  reasonCode,
  edgeBucket,
  planStatus,
  reviewStatus,
  confidenceMin,
  executableOnly,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (groupType) params.set("group_type", groupType);
  if (opportunityType) params.set("opportunity_type", opportunityType);
  if (eventSlug) params.set("event_slug", eventSlug);
  if (classification) params.set("classification", classification);
  if (reasonCode) params.set("reason_code", reasonCode);
  if (edgeBucket) params.set("edge_bucket", edgeBucket);
  if (planStatus) params.set("plan_status", planStatus);
  if (reviewStatus) params.set("review_status", reviewStatus);
  if (confidenceMin !== undefined && confidenceMin !== "") params.set("confidence_min", confidenceMin);
  if (executableOnly !== undefined && executableOnly !== "") params.set("executable_only", executableOnly);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/opportunities?${params}`);
}

export function getPolymarketStructureOpportunity(id) {
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${id}`);
}

export function getPolymarketStructureLatestMakerEconomics(id) {
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${id}/maker-economics/latest`);
}

export function runPolymarketStructureMakerEconomics(id, body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${id}/maker-economics`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getPolymarketStructureMakerEconomicsSnapshots({
  opportunityId,
  conditionId,
  assetId,
  status,
  start,
  end,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (opportunityId) params.set("opportunity_id", opportunityId);
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  if (status) params.set("status", status);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/maker-economics/snapshots?${params}`);
}

export function getPolymarketStructureLatestQuoteRecommendation(id) {
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${id}/quote-recommendations/latest`);
}

export function runPolymarketStructureQuoteRecommendation(id, body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${id}/quote-recommendations`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getPolymarketStructureQuoteRecommendations({
  opportunityId,
  conditionId,
  assetId,
  status,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (opportunityId) params.set("opportunity_id", opportunityId);
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  if (status) params.set("status", status);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/quote-recommendations?${params}`);
}

export function getPolymarketStructureValidations({
  opportunityId,
  classification,
  evaluationKind,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (opportunityId) params.set("opportunity_id", opportunityId);
  if (classification) params.set("classification", classification);
  if (evaluationKind) params.set("evaluation_kind", evaluationKind);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/validations?${params}`);
}

export function getPolymarketStructurePaperPlans({ opportunityId, status, limit = 100 } = {}) {
  const params = new URLSearchParams({ limit });
  if (opportunityId) params.set("opportunity_id", opportunityId);
  if (status) params.set("status", status);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/paper-plans?${params}`);
}

export function getPolymarketStructurePaperPlan(id) {
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/paper-plans/${id}`);
}

export function createPolymarketStructurePaperPlan(opportunityId, body = { actor: "operator" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/opportunities/${opportunityId}/paper-plans`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function approvePolymarketStructurePaperPlan(planId, body = { actor: "operator" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/paper-plans/${planId}/approve`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function rejectPolymarketStructurePaperPlan(
  planId,
  body = { actor: "operator", reason: "operator_rejected" },
) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/paper-plans/${planId}/reject`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function routePolymarketStructurePaperPlan(planId, body = { actor: "operator" }) {
  return requestJson(`${API_BASE}/ingest/polymarket/structure/paper-plans/${planId}/route`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getPolymarketStructureCrossVenueLinks({
  venue,
  actionable,
  reviewStatus,
  confidenceMin,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (venue) params.set("venue", venue);
  if (actionable !== undefined && actionable !== "") params.set("actionable", actionable);
  if (reviewStatus) params.set("review_status", reviewStatus);
  if (confidenceMin !== undefined && confidenceMin !== "") params.set("confidence_min", confidenceMin);
  return fetchJson(`${API_BASE}/ingest/polymarket/structure/cross-venue-links?${params}`);
}

export function getPolymarketLiveStatus({ probeGateway = false } = {}) {
  const params = new URLSearchParams();
  if (probeGateway) params.set("probe_gateway", "true");
  const query = params.toString();
  return fetchJson(`${API_BASE}/ingest/polymarket/live/status${query ? `?${query}` : ""}`);
}

export function getPolymarketPilotConsoleSummary() {
  return fetchJson(`${API_BASE}/ingest/polymarket/live/console-summary`);
}

export function getPolymarketPilotConfigs({ strategyFamily, active, limit = 20 } = {}) {
  const params = new URLSearchParams({ limit });
  if (strategyFamily) params.set("strategy_family", strategyFamily);
  if (active !== undefined && active !== "") params.set("active", active);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/pilot/configs?${params}`);
}

export function createPolymarketPilotConfig(body) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/configs`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updatePolymarketPilotConfig(id, body) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/configs/${id}`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export function armPolymarketPilot(body) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/arm`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function disarmPolymarketPilot(body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/disarm`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function pausePolymarketPilot(body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/pause`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function resumePolymarketPilot(body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/pilot/resume`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getPolymarketPilotStatus() {
  return fetchJson(`${API_BASE}/ingest/polymarket/live/pilot/status`);
}

export function getPolymarketPilotRuns({ status, limit = 20 } = {}) {
  const params = new URLSearchParams({ limit });
  if (status) params.set("status", status);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/pilot/runs?${params}`);
}

export function getPolymarketApprovalQueue({
  strategyFamily,
  approvalState,
  status,
  conditionId,
  assetId,
  limit = 50,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (strategyFamily) params.set("strategy_family", strategyFamily);
  if (approvalState) params.set("approval_state", approvalState);
  if (status) params.set("status", status);
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/approvals?${params}`);
}

export function getPolymarketControlPlaneIncidents({
  incidentType,
  conditionId,
  assetId,
  start,
  end,
  limit = 50,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (incidentType) params.set("incident_type", incidentType);
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/incidents?${params}`);
}

export function getPolymarketShadowEvaluations({
  variantName,
  conditionId,
  assetId,
  start,
  end,
  limit = 50,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (variantName) params.set("variant_name", variantName);
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/shadow-evaluations?${params}`);
}

export function getPolymarketLiveOrders({
  assetId,
  conditionId,
  status,
  strategyFamily,
  approvalState,
  clientOrderId,
  venueOrderId,
  start,
  end,
  limit = 50,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (assetId) params.set("asset_id", assetId);
  if (conditionId) params.set("condition_id", conditionId);
  if (status) params.set("status", status);
  if (strategyFamily) params.set("strategy_family", strategyFamily);
  if (approvalState) params.set("approval_state", approvalState);
  if (clientOrderId) params.set("client_order_id", clientOrderId);
  if (venueOrderId) params.set("venue_order_id", venueOrderId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/orders?${params}`);
}

export function getPolymarketLiveOrderEvents({
  assetId,
  conditionId,
  status,
  strategyFamily,
  approvalState,
  clientOrderId,
  venueOrderId,
  start,
  end,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (assetId) params.set("asset_id", assetId);
  if (conditionId) params.set("condition_id", conditionId);
  if (status) params.set("status", status);
  if (strategyFamily) params.set("strategy_family", strategyFamily);
  if (approvalState) params.set("approval_state", approvalState);
  if (clientOrderId) params.set("client_order_id", clientOrderId);
  if (venueOrderId) params.set("venue_order_id", venueOrderId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/orders/events?${params}`);
}

export function getPolymarketLiveFills({
  assetId,
  conditionId,
  status,
  strategyFamily,
  approvalState,
  clientOrderId,
  venueOrderId,
  start,
  end,
  limit = 100,
} = {}) {
  const params = new URLSearchParams({ limit });
  if (assetId) params.set("asset_id", assetId);
  if (conditionId) params.set("condition_id", conditionId);
  if (status) params.set("status", status);
  if (strategyFamily) params.set("strategy_family", strategyFamily);
  if (approvalState) params.set("approval_state", approvalState);
  if (clientOrderId) params.set("client_order_id", clientOrderId);
  if (venueOrderId) params.set("venue_order_id", venueOrderId);
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/fills?${params}`);
}

export function approvePolymarketLiveOrder(liveOrderId, body) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/orders/${liveOrderId}/approve`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function rejectPolymarketLiveOrder(liveOrderId, body) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/orders/${liveOrderId}/reject`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function submitPolymarketLiveOrder(liveOrderId, body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/orders/${liveOrderId}/submit`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function cancelPolymarketLiveOrder(liveOrderId, body = {}) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/orders/${liveOrderId}/cancel`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function setPolymarketLiveKillSwitch(enabled) {
  return requestJson(`${API_BASE}/ingest/polymarket/live/kill-switch`, {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}

export function getPolymarketMarketTape({ conditionId, assetId, limit = 25 } = {}) {
  const params = new URLSearchParams({ limit });
  if (conditionId) params.set("condition_id", conditionId);
  if (assetId) params.set("asset_id", assetId);
  return fetchJson(`${API_BASE}/ingest/polymarket/live/tape?${params}`);
}

export function getChartData(marketId, range = "24h") {
  return fetchJson(`${API_BASE}/markets/${marketId}/chart-data?range=${range}`);
}

export function getRecentAlerts({ page = 1, pageSize = 50, signalType, platform } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (signalType) params.set("signal_type", signalType);
  if (platform) params.set("platform", platform);
  return fetchJson(`${API_BASE}/alerts/recent?${params}`);
}

export function exportSignalsCsv({ signalType } = {}) {
  const params = new URLSearchParams();
  if (signalType) params.set("signal_type", signalType);
  window.open(`${API_BASE}/signals/export/csv?${params}`, "_blank");
}

export function exportMarketsCsv() {
  window.open(`${API_BASE}/markets/export/csv`, "_blank");
}

export function getPlatformSummary() {
  return fetchJson(`${API_BASE}/analytics/platform-summary`);
}

export function getSignalAccuracy() {
  return fetchJson(`${API_BASE}/analytics/signal-accuracy`);
}

export function getCorrelatedSignals(hours = 1) {
  return fetchJson(`${API_BASE}/analytics/correlated-signals?hours=${hours}`);
}

export function getTimeframeAccuracy(days) {
  const params = new URLSearchParams();
  if (days) params.set("days", days);
  return fetchJson(`${API_BASE}/analytics/timeframe-accuracy?${params}`);
}

export function getSignalTypes() {
  return fetchJson(`${API_BASE}/signals/types`);
}

export function getSignalTimeframes() {
  return fetchJson(`${API_BASE}/signals/timeframes`);
}

export function getMarketPlatforms() {
  return fetchJson(`${API_BASE}/markets/platforms`);
}

export function getSignalAccuracyWithDays(days) {
  const params = new URLSearchParams();
  if (days) params.set("days", days);
  return fetchJson(`${API_BASE}/analytics/signal-accuracy?${params}`);
}

// Backtest API
export function createBacktest(body) {
  return fetch(`${API_BASE}/backtests`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => {
    if (!res.ok) return res.json().then((e) => { throw new Error(e.detail || `API ${res.status}`); });
    return res.json();
  });
}

export function createSweep(body) {
  return fetch(`${API_BASE}/backtests/sweep`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => {
    if (!res.ok) return res.json().then((e) => { throw new Error(e.detail || `API ${res.status}`); });
    return res.json();
  });
}

export function getBacktests() {
  return fetchJson(`${API_BASE}/backtests`);
}

export function getBacktest(id) {
  return fetchJson(`${API_BASE}/backtests/${id}`);
}

export function getBacktestSignals(id, { signalType, resolvedCorrectly, page = 1, pageSize = 100 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (signalType) params.set("signal_type", signalType);
  if (resolvedCorrectly !== undefined && resolvedCorrectly !== "") params.set("resolved_correctly", resolvedCorrectly);
  return fetchJson(`${API_BASE}/backtests/${id}/signals?${params}`);
}

export function deleteBacktest(id) {
  return fetch(`${API_BASE}/backtests/${id}`, { method: "DELETE" }).then((res) => {
    if (!res.ok) throw new Error(`API ${res.status}`);
  });
}

// Performance API
export function getPerformanceSummary() {
  return fetchJson(`${API_BASE}/performance/summary`);
}

// Paper Trading API
export function getPaperTradingPortfolio({ scope } = {}) {
  const params = new URLSearchParams();
  if (scope) params.set("scope", scope);
  const query = params.toString();
  return fetchJson(`${API_BASE}/paper-trading/portfolio${query ? `?${query}` : ""}`);
}

export function getPaperTradingHistory({ status, direction, scope, page = 1, pageSize = 50 } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (status) params.set("status", status);
  if (direction) params.set("direction", direction);
  if (scope) params.set("scope", scope);
  return fetchJson(`${API_BASE}/paper-trading/history?${params}`);
}

export function getPaperTradingMetrics({ scope } = {}) {
  const params = new URLSearchParams();
  if (scope) params.set("scope", scope);
  const query = params.toString();
  return fetchJson(`${API_BASE}/paper-trading/metrics${query ? `?${query}` : ""}`);
}

export function getPaperTradingStrategyHealth() {
  return fetchJson(`${API_BASE}/paper-trading/strategy-health`);
}

export function getPaperTradingPnlCurve({ scope } = {}) {
  const params = new URLSearchParams();
  if (scope) params.set("scope", scope);
  const query = params.toString();
  return fetchJson(`${API_BASE}/paper-trading/pnl-curve${query ? `?${query}` : ""}`);
}

// Portfolio API
export function getPortfolioSummary() {
  return fetchJson(`${API_BASE}/portfolio/summary`);
}

export function getPositions({ page = 1, pageSize = 50, status, platform, marketId } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (status) params.set("status", status);
  if (platform) params.set("platform", platform);
  if (marketId) params.set("market_id", marketId);
  return fetchJson(`${API_BASE}/positions?${params}`);
}

export function getPosition(id) {
  return fetchJson(`${API_BASE}/positions/${id}`);
}

export function createPosition(body) {
  return fetch(`${API_BASE}/positions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => {
    if (!res.ok) return res.json().then((e) => { throw new Error(e.detail || `API ${res.status}`); });
    return res.json();
  });
}

export function closePosition(positionId, body) {
  return fetch(`${API_BASE}/positions/${positionId}/close`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => {
    if (!res.ok) return res.json().then((e) => { throw new Error(e.detail || `API ${res.status}`); });
    return res.json();
  });
}

export function exportPortfolioCsv() {
  window.open(`${API_BASE}/portfolio/export/csv`, "_blank");
}

// Push Notifications API
export function getVapidKey() {
  return fetchJson(`${API_BASE}/push/vapid-key`);
}

export function subscribePush(subscription) {
  return fetch(`${API_BASE}/push/subscribe`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      endpoint: subscription.endpoint,
      keys: {
        p256dh: subscription.toJSON().keys.p256dh,
        auth: subscription.toJSON().keys.auth,
      },
    }),
  }).then((res) => {
    if (!res.ok) return res.json().then((e) => { throw new Error(e.detail || `API ${res.status}`); });
    return res.json();
  });
}

export function unsubscribePush(endpoint) {
  return fetch(`${API_BASE}/push/subscribe`, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ endpoint, keys: {} }),
  }).then((res) => {
    if (!res.ok) throw new Error(`API ${res.status}`);
    return res.json();
  });
}
