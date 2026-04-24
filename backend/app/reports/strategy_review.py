"""Generate prove-the-edge review artifacts from the active strategy run."""
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.comparison import compare_locked_modes
from app.config import settings
from app.models.polymarket_live_execution import CapitalReservation, LiveFill, LiveOrder, PositionLot
from app.models.polymarket_pilot import PolymarketPilotConfig, PolymarketPilotRun
from app.paper_trading.analysis import get_strategy_health

_REVIEW_ARTIFACT_NAME_RE = re.compile(
    r"(?P<review_date>\d{4}-\d{2}-\d{2})-default-strategy-baseline\.(?P<extension>json|md)$"
)
_REVIEW_DATE_LINE_RE = re.compile(r"^\*\*Date:\*\*\s*(?P<review_date>\d{4}-\d{2}-\d{2})\s*$", re.MULTILINE)
_REVIEW_VERDICT_LINE_RE = re.compile(r"^- Verdict:\s*`(?P<verdict>[^`]+)`\s*$", re.MULTILINE)
_DEFAULT_REVIEW_GENERATION_WORKDIR = "backend"
_DEFAULT_REVIEW_GENERATION_COMMAND = "python -m app.reports"
_DEFAULT_REVIEW_RUNBOOK_PATH = "docs/runbooks/default-strategy-controlled-evidence-relaunch.md"
_DEFAULT_REVIEW_ARTIFACTS_DIRECTORY = "docs/strategy-reviews"
_DEFAULT_REVIEW_ANALYSIS_PATH = "docs/paper-trading-analysis-v0.5.md"
_DEFAULT_REVIEW_GENERATION_NOTE = (
    "Read-only health and dashboard surfaces never generate review artifacts. "
    "Use the canonical backend command or the controlled relaunch runbook instead."
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _repo_relative_path(path: Path | None, *, repo_root: Path) -> str | None:
    if path is None:
        return None
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return str(path)


def _latest_artifact_mtime_iso(*paths: Path | None) -> str | None:
    mtimes: list[float] = []
    for path in paths:
        if path is None:
            continue
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    if not mtimes:
        return None
    return datetime.fromtimestamp(max(mtimes), tz=timezone.utc).isoformat()


def _parse_review_markdown_metadata(path: Path | None) -> dict[str, str | None]:
    if path is None:
        return {"review_date": None, "verdict": None}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {"review_date": None, "verdict": None}
    review_date_match = _REVIEW_DATE_LINE_RE.search(text)
    verdict_match = _REVIEW_VERDICT_LINE_RE.search(text)
    return {
        "review_date": review_date_match.group("review_date") if review_date_match else None,
        "verdict": verdict_match.group("verdict") if verdict_match else None,
    }


def _review_generation_guidance() -> dict[str, str]:
    return {
        "working_directory": _DEFAULT_REVIEW_GENERATION_WORKDIR,
        "command": _DEFAULT_REVIEW_GENERATION_COMMAND,
        "runbook_path": _DEFAULT_REVIEW_RUNBOOK_PATH,
        "artifacts_directory": _DEFAULT_REVIEW_ARTIFACTS_DIRECTORY,
        "analysis_path": _DEFAULT_REVIEW_ANALYSIS_PATH,
        "note": _DEFAULT_REVIEW_GENERATION_NOTE,
    }


def _strategy_run_ref_from_review_payload(review_payload: dict | None) -> dict[str, str | None]:
    strategy_run = (review_payload or {}).get("strategy_run")
    if not isinstance(strategy_run, dict):
        strategy_run = {}
    return {
        "id": strategy_run.get("id"),
        "started_at": strategy_run.get("started_at"),
        "status": strategy_run.get("status"),
    }


def _contract_ref_from_review_payload(review_payload: dict | None) -> dict[str, str | None]:
    strategy_run = (review_payload or {}).get("strategy_run")
    if not isinstance(strategy_run, dict):
        strategy_run = {}
    contract_snapshot = strategy_run.get("contract_snapshot")
    if not isinstance(contract_snapshot, dict):
        contract_snapshot = {}
    evidence_boundary = contract_snapshot.get("evidence_boundary")
    if not isinstance(evidence_boundary, dict):
        evidence_boundary = {}
    return {
        "contract_version": contract_snapshot.get("contract_version"),
        "evidence_boundary_id": evidence_boundary.get("boundary_id"),
        "release_tag": evidence_boundary.get("release_tag"),
        "migration_revision": evidence_boundary.get("migration_revision"),
    }


def get_latest_default_strategy_review_artifact_metadata() -> dict:
    repo_root = _repo_root()
    review_dir = repo_root / "docs" / "strategy-reviews"
    grouped_artifacts: dict[str, dict[str, Path]] = {}

    if review_dir.exists():
        for path in review_dir.iterdir():
            if not path.is_file():
                continue
            match = _REVIEW_ARTIFACT_NAME_RE.fullmatch(path.name)
            if match is None:
                continue
            review_date = match.group("review_date")
            extension = match.group("extension")
            grouped_artifacts.setdefault(review_date, {})[extension] = path

    if not grouped_artifacts:
        return {
            "generation_status": "missing",
            "status_detail": "No default-strategy review artifacts have been generated yet.",
            "review_date": None,
            "generated_at": None,
            "verdict": None,
            "strategy_run_ref": {
                "id": None,
                "started_at": None,
                "status": None,
            },
            "contract_ref": {
                "contract_version": None,
                "evidence_boundary_id": None,
                "release_tag": None,
                "migration_revision": None,
            },
            "generation_guidance": _review_generation_guidance(),
            "artifact_paths": {
                "markdown": None,
                "json": None,
            },
        }

    latest_review_date = max(grouped_artifacts)
    artifacts = grouped_artifacts[latest_review_date]
    markdown_path = artifacts.get("md")
    json_path = artifacts.get("json")

    review_payload = None
    json_error = None
    if json_path is not None:
        try:
            review_payload = json.loads(json_path.read_text(encoding="utf-8"))
        except OSError:
            json_error = "unreadable"
        except json.JSONDecodeError:
            json_error = "invalid"
    if not isinstance(review_payload, dict):
        review_payload = {}

    markdown_metadata = _parse_review_markdown_metadata(markdown_path)
    review_verdict = (review_payload or {}).get("review_verdict") or {}
    generated_at = (review_payload or {}).get("generated_at") or _latest_artifact_mtime_iso(markdown_path, json_path)
    verdict = review_verdict.get("verdict") or markdown_metadata.get("verdict")
    review_date = markdown_metadata.get("review_date") or latest_review_date

    if json_error == "unreadable":
        generation_status = "invalid"
        status_detail = (
            "The latest review JSON artifact could not be read. "
            "Showing markdown fallback metadata when available."
        )
    elif json_error == "invalid":
        generation_status = "invalid"
        status_detail = (
            "The latest review JSON artifact could not be parsed. "
            "Showing markdown fallback metadata when available."
        )
    elif markdown_path is not None and json_path is not None:
        generation_status = "complete"
        status_detail = "Markdown and JSON review artifacts are present."
    elif markdown_path is not None:
        generation_status = "partial"
        status_detail = "Markdown review artifact exists, but the JSON artifact is missing."
    else:
        generation_status = "partial"
        status_detail = "JSON review artifact exists, but the markdown artifact is missing."

    return {
        "generation_status": generation_status,
        "status_detail": status_detail,
        "review_date": review_date,
        "generated_at": generated_at,
        "verdict": verdict,
        "strategy_run_ref": _strategy_run_ref_from_review_payload(review_payload),
        "contract_ref": _contract_ref_from_review_payload(review_payload),
        "generation_guidance": _review_generation_guidance(),
        "artifact_paths": {
            "markdown": _repo_relative_path(markdown_path, repo_root=repo_root),
            "json": _repo_relative_path(json_path, repo_root=repo_root),
        },
    }


def _fmt_money(value) -> str:
    return "-" if value is None else f"${value:,.2f}"


def _fmt_pct(value) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def _fmt_cents(value) -> str:
    return "-" if value is None else f"{value * 100:.1f}c"


async def _count_rows(session: AsyncSession, model, *filters) -> int:
    query = select(func.count()).select_from(model)
    if filters:
        query = query.where(*filters)
    return int((await session.execute(query)).scalar_one() or 0)


async def _build_live_safety_snapshot(session: AsyncSession) -> dict:
    live_order_count = await _count_rows(session, LiveOrder)
    live_fill_count = await _count_rows(session, LiveFill)
    outstanding_reservations = await _count_rows(session, CapitalReservation, CapitalReservation.status == "open")
    open_position_lots = await _count_rows(session, PositionLot, PositionLot.status == "open")
    active_pilot_configs = await _count_rows(
        session,
        PolymarketPilotConfig,
        or_(
            PolymarketPilotConfig.active.is_(True),
            PolymarketPilotConfig.armed.is_(True),
            PolymarketPilotConfig.live_enabled.is_(True),
        ),
    )
    active_pilot_runs = await _count_rows(
        session,
        PolymarketPilotRun,
        PolymarketPilotRun.ended_at.is_(None),
        PolymarketPilotRun.status.in_(["active", "running", "armed"]),
    )
    live_submission_permitted = bool(settings.polymarket_live_trading_enabled and not settings.polymarket_live_dry_run)
    status = "fail_closed"
    if live_order_count or live_fill_count or outstanding_reservations or open_position_lots:
        status = "live_activity_present"
    elif live_submission_permitted or settings.polymarket_pilot_enabled or active_pilot_configs or active_pilot_runs:
        status = "armed_or_configured"

    return {
        "status": status,
        "settings": {
            "live_trading_enabled": settings.polymarket_live_trading_enabled,
            "live_dry_run": settings.polymarket_live_dry_run,
            "manual_approval_required": settings.polymarket_live_manual_approval_required,
            "pilot_enabled": settings.polymarket_pilot_enabled,
        },
        "live_submission_permitted": live_submission_permitted,
        "counts": {
            "live_orders": live_order_count,
            "live_fills": live_fill_count,
            "outstanding_reservations": outstanding_reservations,
            "open_position_lots": open_position_lots,
            "active_pilot_configs": active_pilot_configs,
            "active_pilot_runs": active_pilot_runs,
        },
    }


def _render_review_markdown(health: dict, comparison: dict, *, as_of: datetime, live_safety: dict | None = None) -> str:
    strategy_run = health.get("strategy_run") or {}
    contract_snapshot = strategy_run.get("contract_snapshot") or {}
    evidence_boundary = contract_snapshot.get("evidence_boundary") or {}
    observation = health.get("observation") or {}
    headline = health.get("headline") or {}
    review_verdict = health.get("review_verdict") or {}
    execution_realism = health.get("execution_realism") or {}
    funnel = health.get("trade_funnel") or {}
    pending_watch = health.get("pending_decision_watch") or {}
    risk_blocks = health.get("risk_blocks") or {}
    replay = health.get("replay") or {}
    live_safety = live_safety or {}
    live_counts = live_safety.get("counts") or {}
    live_settings = live_safety.get("settings") or {}
    resolution_reconciliation = health.get("resolution_reconciliation") or {}
    overdue_watch = resolution_reconciliation.get("overdue_open_trade_watch") or {}
    signal_level = comparison.get("signal_level") or {}
    signal_level_default = signal_level.get("default_strategy") or {}
    signal_level_benchmark = signal_level.get("benchmark") or {}
    execution_adjusted = comparison.get("execution_adjusted") or {}
    execution_adjusted_default = execution_adjusted.get("default_strategy") or {}
    skip_reasons = health.get("skip_reasons") or []
    detector_review = health.get("detector_review") or []
    local_reason_counts = risk_blocks.get("local_reason_counts") or {}
    shared_reason_counts = risk_blocks.get("shared_global_reason_counts") or {}
    shared_upstream_reason_counts = risk_blocks.get("shared_global_upstream_reason_counts") or {}
    execution_reason_counts = risk_blocks.get("execution_liquidity_reason_counts") or {}
    shared_examples = risk_blocks.get("shared_global_examples") or []
    local_reason_lines = "- No local paper-book blocks recorded." if not local_reason_counts else "\n".join(
        f"- {reason_code}: {count}" for reason_code, count in local_reason_counts.items()
    )
    shared_reason_lines = "- No shared/global strategy-facing blocks recorded." if not shared_reason_counts else "\n".join(
        f"- {reason_code}: {count}" for reason_code, count in shared_reason_counts.items()
    )
    shared_upstream_lines = "- No shared/global upstream reasons recorded." if not shared_upstream_reason_counts else "\n".join(
        f"- {reason_code}: {count}" for reason_code, count in shared_upstream_reason_counts.items()
    )
    execution_reason_lines = "- No execution/liquidity blocks recorded." if not execution_reason_counts else "\n".join(
        f"- {reason_code}: {count}" for reason_code, count in execution_reason_counts.items()
    )
    shared_example_lines = "- No representative shared/global examples yet." if not shared_examples else "\n".join(
        f"- signal `{row['signal_id']}` / decision `{row['decision_id']}`: {row['reason_code']} <- {row['upstream_reason_code']}"
        + (f" ({row['detail']})" if row.get("detail") else "")
        for row in shared_examples
    )

    skip_lines = "- No in-window skip reasons recorded yet." if not skip_reasons else "\n".join(
        f"- {row['reason_label']}: {row['count']}" for row in skip_reasons[:5]
    )
    detector_lines = "- No detector verdicts yet." if not detector_review else "\n".join(
        f"- `{row['signal_type']}`: {row['verdict']} - {row['note']}" for row in detector_review[:6]
    )
    review_blocker_lines = "- No active verdict blockers." if not review_verdict.get("blockers") else "\n".join(
        f"- `{row['code']}`: {row['detail']}" for row in review_verdict.get("blockers", [])
    )
    overdue_trade_lines = "- No overdue open paper trades." if not overdue_watch.get("examples") else "\n".join(
        f"- trade `{row['trade_id']}` on `{row['platform']}:{row['platform_id']}` ended {row.get('market_end_date') or '-'}"
        for row in overdue_watch.get("examples", [])
    )
    empty_state = "" if funnel.get("opened_trade_signals", 0) else (
        "\n## Empty State\n\n"
        "No active-run paper trades have resolved yet. Keep the baseline frozen, "
        "watch the skip-reason funnel, and do not change the contract until the run produces measured trades.\n"
    )

    return f"""# Default Strategy Review

**Date:** {as_of.date().isoformat()}
**Strategy:** `{strategy_run.get('strategy_name', '-')}`
**Run ID:** `{strategy_run.get('id', '-')}`
**Run Status:** `{strategy_run.get('status', '-')}`
**Contract Version:** `{contract_snapshot.get('contract_version', '-')}`
**Evidence Boundary:** `{evidence_boundary.get('boundary_id', evidence_boundary.get('release_tag', '-'))}`
**Boundary Commit:** `{evidence_boundary.get('commit_sha', '-')}`
**Alembic Revision:** `{evidence_boundary.get('migration_revision', '-')}`

## Run Metadata

- Run start: {observation.get('started_at') or '-'}
- Immutable launch boundary: {observation.get('baseline_start_at') or '-'}
- Days tracked: {observation.get('days_tracked') if observation.get('days_tracked') is not None else '-'}
- Observation status: `{observation.get('status', '-')}`

## Operator Verdict

- Verdict: `{review_verdict.get('verdict', '-')}`
- Summary: {review_verdict.get('summary', '-')}

### Active blockers

{review_blocker_lines}

## Current Health Snapshot

- Open exposure: {_fmt_money(headline.get('open_exposure'))}
- Resolved trades: {headline.get('resolved_trades', 0)}
- Cumulative paper P&L: {_fmt_money(headline.get('cumulative_pnl'))}
- Current equity: {_fmt_money(headline.get('current_equity'))}
- Peak equity: {_fmt_money(headline.get('peak_equity'))}
- Shadow cumulative P&L: {_fmt_money(execution_realism.get('shadow_cumulative_pnl'))}
- Average CLV: {_fmt_cents(headline.get('avg_clv'))}
- Profit factor: {headline.get('profit_factor') if headline.get('profit_factor') is not None else '-'}
- Shadow profit factor: {execution_realism.get('shadow_profit_factor') if execution_realism.get('shadow_profit_factor') is not None else '-'}
- Win rate: {_fmt_pct(headline.get('win_rate'))}
- Brier score: {headline.get('brier_score') if headline.get('brier_score') is not None else '-'}
- Max drawdown: {_fmt_money(headline.get('max_drawdown'))}
- Current drawdown pct: {_fmt_pct(headline.get('drawdown_pct'))}

## Live Automation Safety

- Status: `{live_safety.get('status', '-')}`
- Live submission permitted: `{live_safety.get('live_submission_permitted', False)}`
- Live trading enabled: `{live_settings.get('live_trading_enabled', False)}`
- Dry run: `{live_settings.get('live_dry_run', True)}`
- Manual approval required: `{live_settings.get('manual_approval_required', True)}`
- Pilot enabled: `{live_settings.get('pilot_enabled', False)}`
- Live orders: {live_counts.get('live_orders', 0)}
- Live fills: {live_counts.get('live_fills', 0)}
- Outstanding reservations: {live_counts.get('outstanding_reservations', 0)}
- Open live position lots: {live_counts.get('open_position_lots', 0)}
- Active pilot configs: {live_counts.get('active_pilot_configs', 0)}
- Active pilot runs: {live_counts.get('active_pilot_runs', 0)}

## Trade Funnel

- Candidate signals: {funnel.get('candidate_signals', 0)}
- Qualified signals: {funnel.get('qualified_signals', 0)}
- Opened trade signals: {funnel.get('opened_trade_signals', 0)}
- Skipped signals: {funnel.get('skipped_signals', 0)}
- Pending decisions: {funnel.get('pending_decision_signals', 0)}
- Oldest pending decision at: {pending_watch.get('oldest_decision_at') or '-'}
- Pending max age (seconds): {pending_watch.get('max_age_seconds', 0)}
- Resolved signals: {funnel.get('resolved_signals', 0)}
- Qualified not traded: {funnel.get('qualified_not_traded', 0)}
- Legacy trades excluded: {funnel.get('excluded_legacy_trades', 0)}
- Funnel conservation holds: `{funnel.get('conservation_holds', False)}`

## Resolution Reconciliation

- Status: `{resolution_reconciliation.get('status', '-')}`
- Open trades: {resolution_reconciliation.get('open_trades', 0)}
- Missing resolutions: {resolution_reconciliation.get('missing_resolutions', 0)}
- Overdue open trades: {resolution_reconciliation.get('overdue_open_trades', 0)}
- Resolved trades: {resolution_reconciliation.get('resolved_trades', 0)}
- Resolved signals: {resolution_reconciliation.get('resolved_signals', 0)}
- Pending decisions: {resolution_reconciliation.get('pending_decisions', 0)}
- Pending max age (seconds): {resolution_reconciliation.get('pending_decision_max_age_seconds', 0)}
- Evidence freshness: `{resolution_reconciliation.get('evidence_freshness_status', '-')}`

### Overdue open trade examples

{overdue_trade_lines}

## Risk Block Attribution

- Local paper-book blocks: {risk_blocks.get('local_paper_book_blocks', 0)}
- Shared/global blocks: {risk_blocks.get('shared_global_blocks', 0)}
- Execution/liquidity blocks: {risk_blocks.get('execution_liquidity_blocks', 0)}

### Local paper-book reasons

{local_reason_lines}

### Shared/global strategy reasons

{shared_reason_lines}

### Shared/global upstream reasons

{shared_upstream_lines}

### Execution/liquidity blocks

{execution_reason_lines}

### Representative shared/global examples

{shared_example_lines}

## Skip Reasons

{skip_lines}

## Detector Verdicts

{detector_lines}

## Comparison Modes

### Signal-Level (`per_share`)

| Cohort | Resolved Signals | Win Rate | Avg CLV | 1-Share P&L | Max Drawdown | Brier |
|--------|------------------|----------|---------|-------------|--------------|-------|
| Default Strategy | {signal_level_default.get('resolved_signals', 0)} | {_fmt_pct(signal_level_default.get('win_rate'))} | {_fmt_cents(signal_level_default.get('avg_clv'))} | {_fmt_cents(signal_level_default.get('total_profit_loss_per_share'))} | {_fmt_cents(signal_level_default.get('max_drawdown_per_share'))} | {signal_level_default.get('brier_score') if signal_level_default.get('brier_score') is not None else '-'} |
| Legacy Benchmark | {signal_level_benchmark.get('resolved_signals', 0)} | {_fmt_pct(signal_level_benchmark.get('win_rate'))} | {_fmt_cents(signal_level_benchmark.get('avg_clv'))} | {_fmt_cents(signal_level_benchmark.get('total_profit_loss_per_share'))} | {_fmt_cents(signal_level_benchmark.get('max_drawdown_per_share'))} | {signal_level_benchmark.get('brier_score') if signal_level_benchmark.get('brier_score') is not None else '-'} |

### Execution-Adjusted (`usd`)

- Default strategy resolved trades: {execution_adjusted_default.get('resolved_trades', 0)}
- Default strategy paper P&L: {_fmt_money(execution_adjusted_default.get('cumulative_pnl'))}
- Default strategy shadow P&L: {_fmt_money(execution_adjusted_default.get('shadow_cumulative_pnl'))}
- Legacy execution-adjusted benchmark: unavailable in this remediation slice

## Replay Coverage

- Coverage mode: `{replay.get('coverage_mode', '-')}`
- Supported detectors: {", ".join(replay.get('supported_detectors') or []) or '-'}
- Unsupported detectors: {", ".join(replay.get('unsupported_detectors') or []) or '-'}

## Execution Realism Caveat

- Liquidity-constrained trades: {execution_realism.get('liquidity_constrained_trades', 0)}
- Trades missing orderbook context: {execution_realism.get('trades_missing_orderbook_context', 0)}
- Shadow execution uses a conservative half-spread penalty and near-touch depth checks. It is a realism overlay, not a full market-impact model.
{empty_state}
"""


def _review_artifact_payload(
    health: dict,
    comparison: dict,
    *,
    as_of: datetime,
    live_safety: dict | None = None,
) -> dict:
    return {
        "generated_at": as_of.isoformat(),
        "strategy_run": health.get("strategy_run"),
        "live_safety": live_safety,
        "review_verdict": health.get("review_verdict"),
        "observation": health.get("observation"),
        "headline": health.get("headline"),
        "trade_funnel": health.get("trade_funnel"),
        "resolution_reconciliation": health.get("resolution_reconciliation"),
        "pending_decision_watch": health.get("pending_decision_watch"),
        "run_integrity": health.get("run_integrity"),
        "replay": health.get("replay"),
        "comparison_modes": comparison,
    }


def _render_analysis_markdown(health: dict, comparison: dict, *, as_of: datetime) -> str:
    headline = health.get("headline") or {}
    execution_realism = health.get("execution_realism") or {}
    signal_level = comparison.get("signal_level") or {}
    benchmark = signal_level.get("benchmark") or {}
    default_strategy = signal_level.get("default_strategy") or {}
    execution_adjusted = comparison.get("execution_adjusted") or {}
    execution_adjusted_default = execution_adjusted.get("default_strategy") or {}
    detector_review = health.get("detector_review") or []
    detector_lines = "\n".join(
        f"- `{row['signal_type']}`: {row['verdict']} | trade P&L {_fmt_money(row.get('paper_trade_pnl'))} | avg CLV {_fmt_cents(row.get('avg_clv'))}"
        for row in detector_review[:8]
    ) or "- No detector verdicts yet."

    return f"""# Paper Trading Analysis v0.5

**Generated:** {as_of.isoformat()}

## Baseline Summary

- Paper P&L: {_fmt_money(headline.get('cumulative_pnl'))}
- Shadow P&L: {_fmt_money(execution_realism.get('shadow_cumulative_pnl'))}
- Avg CLV: {_fmt_cents(headline.get('avg_clv'))}
- Brier score: {headline.get('brier_score') if headline.get('brier_score') is not None else '-'}
- Max drawdown: {_fmt_money(headline.get('max_drawdown'))}
- Shadow profit factor: {execution_realism.get('shadow_profit_factor') if execution_realism.get('shadow_profit_factor') is not None else '-'}

## Heuristic vs Probability

- Default strategy resolved signals: {default_strategy.get('resolved_signals', 0)}
- Default strategy signal-level P&L: {_fmt_cents(default_strategy.get('total_profit_loss_per_share'))}
- Default strategy execution-adjusted P&L: {_fmt_money(execution_adjusted_default.get('cumulative_pnl'))}
- Legacy resolved signals: {benchmark.get('resolved_signals', 0)}
- Legacy 1-share P&L: {_fmt_cents(benchmark.get('total_profit_loss_per_share'))}

## Detector Keep / Watch / Cut

{detector_lines}

## Notes

- The default strategy run is immutable once seeded.
- Skip reasons should be reviewed weekly before changing thresholds or detectors.
- Shadow execution is conservative and should be treated as the realism floor, not the final trading simulator.
"""


async def generate_default_strategy_review(session: AsyncSession) -> dict:
    as_of = datetime.now(timezone.utc)
    health = await get_strategy_health(session)
    live_safety = await _build_live_safety_snapshot(session)
    strategy_run = health.get("strategy_run") or {}
    comparison = health.get("comparison_modes")
    if comparison is None:
        started_at = strategy_run.get("started_at") or health.get("observation", {}).get("started_at") or as_of.isoformat()
        started_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        run_id = strategy_run.get("id")
        comparison = await compare_locked_modes(
            session,
            start_date=started_dt,
            end_date=as_of,
            strategy_run_id=None if run_id is None else uuid.UUID(run_id),
        )

    repo_root = _repo_root()
    review_path = repo_root / "docs" / "strategy-reviews" / f"{as_of.date().isoformat()}-default-strategy-baseline.md"
    review_json_path = repo_root / "docs" / "strategy-reviews" / f"{as_of.date().isoformat()}-default-strategy-baseline.json"
    analysis_path = repo_root / "docs" / "paper-trading-analysis-v0.5.md"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    analysis_path.parent.mkdir(parents=True, exist_ok=True)

    review_path.write_text(
        _render_review_markdown(health, comparison, as_of=as_of, live_safety=live_safety),
        encoding="utf-8",
    )
    review_json_path.write_text(
        json.dumps(
            _review_artifact_payload(health, comparison, as_of=as_of, live_safety=live_safety),
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    analysis_path.write_text(_render_analysis_markdown(health, comparison, as_of=as_of), encoding="utf-8")
    return {
        "review_path": str(review_path),
        "review_json_path": str(review_json_path),
        "analysis_path": str(analysis_path),
        "strategy_run": strategy_run,
        "review_verdict": health.get("review_verdict"),
        "live_safety": live_safety,
        "comparison": comparison,
    }
