const API_BASE = "/api/v1";

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
  return res.json();
}

export function getSignals({ page = 1, pageSize = 50, signalType, marketId } = {}) {
  const params = new URLSearchParams({ page, page_size: pageSize });
  if (signalType) params.set("signal_type", signalType);
  if (marketId) params.set("market_id", marketId);
  return fetchJson(`${API_BASE}/signals?${params}`);
}

export function getSignal(id) {
  return fetchJson(`${API_BASE}/signals/${id}`);
}

export function getMarkets({ page = 1, pageSize = 50 } = {}) {
  return fetchJson(`${API_BASE}/markets?page=${page}&page_size=${pageSize}`);
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
