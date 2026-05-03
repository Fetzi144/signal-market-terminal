from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.reports.strategy_review import _fmt_money, _repo_root

ARTIFACT_DIR = "docs/research-lab"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _render_markdown(payload: dict[str, Any]) -> str:
    batch = payload.get("batch") or {}
    lanes = payload.get("lane_results") or []
    blockers = payload.get("top_blockers") or []
    candidates = payload.get("top_ev_candidates") or []
    lane_lines = "\n".join(
        (
            f"| {row.get('rank_position', '-')} | `{row.get('family')}` | `{row.get('lane')}` | "
            f"`{row.get('verdict')}` | {_fmt_money(row.get('realized_pnl'))} | "
            f"{_fmt_money(row.get('replay_net_pnl'))} | {row.get('resolved_trades', 0)} | "
            f"`{row.get('coverage_mode') or '-'}` |"
        )
        for row in lanes
    )
    if not lane_lines:
        lane_lines = "| - | - | - | - | - | - | - | - |"
    blocker_lines = "\n".join(f"- `{row.get('blocker')}`: {row.get('count')}" for row in blockers) or "- None"
    candidate_lines = "\n".join(
        f"- `{row.get('family')}` / `{row.get('lane')}`: {row.get('label')} ({row.get('why')})"
        for row in candidates
    ) or "- None"
    return f"""# Research Lab Batch

**Preset:** `{batch.get('preset', '-')}`
**Status:** `{batch.get('status', '-')}`
**Window:** {batch.get('window_start') or '-'} to {batch.get('window_end') or '-'}
**Universe:** {batch.get('universe', {}).get('market_count', 0)} markets, {batch.get('universe', {}).get('signal_count', 0)} signals
**Paper-only:** `true`

## Ranked Lanes

| Rank | Family | Lane | Verdict | Realized P&L | Replay P&L | Sample | Coverage |
| ---: | --- | --- | --- | ---: | ---: | ---: | --- |
{lane_lines}

## Top Blockers

{blocker_lines}

## Top EV Candidates

{candidate_lines}
"""


def write_research_batch_artifacts(payload: dict[str, Any]) -> dict[str, str]:
    root = _repo_root()
    artifact_dir = root / ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    batch = payload.get("batch") or {}
    stem = f"{batch.get('preset', 'research')}-{str(batch.get('id', 'unknown'))[:8]}"
    json_path = artifact_dir / f"{stem}.json"
    markdown_path = artifact_dir / f"{stem}.md"
    _write_json(json_path, payload)
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "research_lab_json_path": str(json_path),
        "research_lab_markdown_path": str(markdown_path),
    }
