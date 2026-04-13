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
