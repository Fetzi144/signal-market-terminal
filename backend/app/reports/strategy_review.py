"""Generate prove-the-edge review artifacts from the active strategy run."""
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.comparison import compare_locked_modes
from app.paper_trading.analysis import get_strategy_health


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _fmt_money(value) -> str:
    return "-" if value is None else f"${value:,.2f}"


def _fmt_pct(value) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def _fmt_cents(value) -> str:
    return "-" if value is None else f"{value * 100:.1f}c"


def _render_review_markdown(health: dict, comparison: dict, *, as_of: datetime) -> str:
    strategy_run = health.get("strategy_run") or {}
    contract_snapshot = strategy_run.get("contract_snapshot") or {}
    evidence_boundary = contract_snapshot.get("evidence_boundary") or {}
    observation = health.get("observation") or {}
    headline = health.get("headline") or {}
    execution_realism = health.get("execution_realism") or {}
    funnel = health.get("trade_funnel") or {}
    pending_watch = health.get("pending_decision_watch") or {}
    risk_blocks = health.get("risk_blocks") or {}
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

## Execution Realism Caveat

- Liquidity-constrained trades: {execution_realism.get('liquidity_constrained_trades', 0)}
- Trades missing orderbook context: {execution_realism.get('trades_missing_orderbook_context', 0)}
- Shadow execution uses a conservative half-spread penalty and near-touch depth checks. It is a realism overlay, not a full market-impact model.
{empty_state}
"""


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
    strategy_run = health.get("strategy_run") or {}
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
    analysis_path = repo_root / "docs" / "paper-trading-analysis-v0.5.md"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    analysis_path.parent.mkdir(parents=True, exist_ok=True)

    review_path.write_text(_render_review_markdown(health, comparison, as_of=as_of), encoding="utf-8")
    analysis_path.write_text(_render_analysis_markdown(health, comparison, as_of=as_of), encoding="utf-8")
    return {
        "review_path": str(review_path),
        "analysis_path": str(analysis_path),
        "strategy_run": strategy_run,
        "comparison": comparison,
    }
