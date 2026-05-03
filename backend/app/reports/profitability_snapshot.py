"""Generate daily paper-profitability snapshot artifacts."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.paper_trading.analysis import (
    PROFITABILITY_MIN_RESOLVED_TRADES,
    PROFITABILITY_OPERATING_WINDOW_DAYS,
    get_profitability_snapshot,
)
from app.reports.strategy_review import (
    _fmt_cents,
    _fmt_money,
    _repo_root,
    ensure_default_strategy_review_current,
)

_PROFITABILITY_SNAPSHOT_DIR = "docs/profitability-snapshots"


def _snapshot_artifact_stem(*, family: str, as_of: datetime) -> str:
    normalized_family = str(family or "default_strategy").strip().lower().replace("/", "_")
    return f"{as_of.date().isoformat()}-{normalized_family}-paper-profitability"


def _render_profitability_snapshot_markdown(payload: dict) -> str:
    snapshot = payload.get("snapshot") or {}
    skip_funnel = snapshot.get("skip_funnel") or {}
    mark = snapshot.get("mark_to_market") or {}
    exposure = snapshot.get("open_exposure_buckets") or {}
    buckets = exposure.get("buckets") or {}
    capital_drag = exposure.get("capital_drag") or {}
    blockers = snapshot.get("profitability_blockers") or []
    evidence_blockers = snapshot.get("evidence_blockers") or []
    evidence_blocker_lines = "- None" if not evidence_blockers else "\n".join(
        f"- `{row.get('code', 'unknown')}`: {row.get('detail', '-')}" for row in evidence_blockers
    )
    exposure_lines = "\n".join(
        f"- `{bucket_key}`: {row.get('trade_count', 0)} trade(s), {_fmt_money(row.get('open_exposure'))} exposure, "
        f"{_fmt_money(row.get('open_mark_to_market_pnl'))} open MTM"
        for bucket_key, row in buckets.items()
    ) or "- No open exposure."
    return f"""# Paper Profitability Snapshot

**Generated:** {payload.get('generated_at')}
**Family:** `{snapshot.get('family', '-')}`
**Strategy version:** `{snapshot.get('strategy_version', '-')}`
**Verdict:** `{snapshot.get('verdict', '-')}`

## Gate

- Window: {snapshot.get('window_start') or '-'} to {snapshot.get('window_end') or '-'}
- Required days: {payload.get('operating_window_days', PROFITABILITY_OPERATING_WINDOW_DAYS)}
- Required resolved trades: {payload.get('minimum_resolved_trades', PROFITABILITY_MIN_RESOLVED_TRADES)}
- Profitability blockers: {", ".join(blockers) if blockers else "-"}

## P&L

- Realized P&L: {_fmt_money(snapshot.get('realized_pnl'))}
- Mark-to-market P&L: {_fmt_money(snapshot.get('mark_to_market_pnl'))}
- Open mark-to-market P&L: {_fmt_money(snapshot.get('open_mark_to_market_pnl'))}
- Open exposure: {_fmt_money(snapshot.get('open_exposure'))}
- Open trades: {snapshot.get('open_trades', 0)}
- Resolved trades: {snapshot.get('resolved_trades', 0)}
- Average CLV: {_fmt_cents(snapshot.get('avg_clv'))}
- Replay coverage: `{snapshot.get('replay_coverage_mode', '-')}`

## Mark To Market

- Open positions marked: {mark.get('open_positions_marked', 0)}
- Missing latest price: {mark.get('open_positions_missing_price', 0)}
- Stale latest price: {mark.get('open_positions_stale_price', 0)}
- Latest price at: {mark.get('latest_price_at') or '-'}

## Open Exposure Timing

- Short horizon: {exposure.get('short_horizon_days', 7)} day(s)
- Operating window: {exposure.get('operating_window_days', 30)} day(s)
- Capital drag exposure: {_fmt_money(capital_drag.get('open_exposure'))}
- Capital drag share: {capital_drag.get('pct_open_exposure', 0.0)}

{exposure_lines}

## Funnel

- Candidate signals: {skip_funnel.get('candidate_signals', 0)}
- Qualified signals: {skip_funnel.get('qualified_signals', 0)}
- Opened trade signals: {skip_funnel.get('opened_trade_signals', 0)}
- Skipped signals: {skip_funnel.get('skipped_signals', 0)}
- Pending decisions: {skip_funnel.get('pending_decision_signals', 0)}
- Integrity errors: {skip_funnel.get('integrity_error_count', 0)}
- Conservation holds: `{skip_funnel.get('conservation_holds', False)}`

## Evidence Blockers

{evidence_blocker_lines}
"""


async def generate_profitability_snapshot_artifact(
    session: AsyncSession,
    *,
    family: str = "default_strategy",
    as_of: datetime | None = None,
    ensure_review_current: bool = True,
) -> dict:
    as_of = as_of or datetime.now(timezone.utc)
    review_refresh = None
    if ensure_review_current and str(family or "default_strategy").strip().lower() == "default_strategy":
        review_refresh = await ensure_default_strategy_review_current(session)
    snapshot = await get_profitability_snapshot(session, family=family, use_cache=False)
    payload = {
        "generated_at": as_of.isoformat(),
        "operating_window_days": PROFITABILITY_OPERATING_WINDOW_DAYS,
        "minimum_resolved_trades": PROFITABILITY_MIN_RESOLVED_TRADES,
        "paper_only": True,
        "live_submission_permitted": False,
        "review_refresh": review_refresh,
        "snapshot": snapshot,
    }

    repo_root = _repo_root()
    output_dir = repo_root / _PROFITABILITY_SNAPSHOT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _snapshot_artifact_stem(family=snapshot.get("family") or family, as_of=as_of)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_profitability_snapshot_markdown(payload), encoding="utf-8")
    return {
        "snapshot_json_path": str(json_path),
        "snapshot_markdown_path": str(markdown_path),
        "snapshot": snapshot,
    }
