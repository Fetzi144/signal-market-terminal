const API_BASE = (import.meta.env.VITE_API_BASE || "") + "/api/v1";

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
  return res.json();
}

export function getSignals({ page = 1, pageSize = 50, signalType, marketId, platform, resolvedCorrectly } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (signalType) params.set("signal_type", signalType);
  if (marketId) params.set("market_id", marketId);
  if (platform) params.set("platform", platform);
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

export function getSignalTypes() {
  return fetchJson(`${API_BASE}/signals/types`);
}

export function getMarketPlatforms() {
  return fetchJson(`${API_BASE}/markets/platforms`);
}

export function getSignalAccuracyWithDays(days) {
  const params = new URLSearchParams();
  if (days) params.set("days", days);
  return fetchJson(`${API_BASE}/analytics/signal-accuracy?${params}`);
}
