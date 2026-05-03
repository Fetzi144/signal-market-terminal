"""Read-only smoke checks for paper-profitability evidence APIs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

_SMOKE_ENDPOINTS = (
    ("root", "/"),
    ("health", "/api/v1/health"),
    ("strategy_health", "/api/v1/paper-trading/strategy-health"),
    ("profitability_snapshot", "/api/v1/paper-trading/profitability-snapshot"),
    ("profit_tools", "/api/v1/paper-trading/profit-tools"),
    ("strategy_profitability", "/api/v1/strategies/profitability"),
)


def _compact_payload(name: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if name == "profitability_snapshot":
        return {
            "verdict": payload.get("verdict"),
            "realized_pnl": payload.get("realized_pnl"),
            "mark_to_market_pnl": payload.get("mark_to_market_pnl"),
            "profitability_blockers": payload.get("profitability_blockers"),
        }
    if name == "strategy_health":
        verdict = payload.get("review_verdict") if isinstance(payload.get("review_verdict"), dict) else {}
        freshness = payload.get("evidence_freshness") if isinstance(payload.get("evidence_freshness"), dict) else {}
        return {
            "review_verdict": verdict.get("verdict"),
            "review_blockers": [row.get("code") for row in verdict.get("blockers", []) if isinstance(row, dict)],
            "evidence_freshness": freshness.get("status"),
        }
    if name == "strategy_profitability":
        snapshots = payload.get("snapshots") if isinstance(payload.get("snapshots"), list) else []
        return {
            "snapshot_count": len(snapshots),
            "paper_only": payload.get("paper_only"),
            "live_submission_permitted": payload.get("live_submission_permitted"),
        }
    if name == "profit_tools":
        steps = payload.get("next_best_steps") if isinstance(payload.get("next_best_steps"), list) else []
        lanes = payload.get("lane_readiness") if isinstance(payload.get("lane_readiness"), dict) else {}
        return {
            "next_step_count": len(steps),
            "lane_status": lanes.get("status"),
            "paper_only": payload.get("paper_only"),
            "live_submission_permitted": payload.get("live_submission_permitted"),
        }
    if name == "health":
        return {"status": payload.get("status")}
    return {}


async def run_evidence_api_smoke(
    *,
    base_url: str,
    timeout_seconds: float = 15.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    normalized_base_url = str(base_url or "").rstrip("/")
    checks: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        base_url=normalized_base_url,
        timeout=timeout_seconds,
        transport=transport,
    ) as client:
        for name, path in _SMOKE_ENDPOINTS:
            started_at = datetime.now(timezone.utc)
            try:
                response = await client.get(path)
                latency_ms = round((datetime.now(timezone.utc) - started_at).total_seconds() * 1000, 1)
                payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else None
                checks.append(
                    {
                        "name": name,
                        "path": path,
                        "status": "passing" if 200 <= response.status_code < 300 else "failing",
                        "status_code": response.status_code,
                        "latency_ms": latency_ms,
                        "summary": _compact_payload(name, payload),
                    }
                )
            except Exception as exc:
                latency_ms = round((datetime.now(timezone.utc) - started_at).total_seconds() * 1000, 1)
                checks.append(
                    {
                        "name": name,
                        "path": path,
                        "status": "failing",
                        "status_code": None,
                        "latency_ms": latency_ms,
                        "error": str(exc),
                        "summary": {},
                    }
                )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": normalized_base_url,
        "status": "passing" if all(check["status"] == "passing" for check in checks) else "failing",
        "checks": checks,
    }
