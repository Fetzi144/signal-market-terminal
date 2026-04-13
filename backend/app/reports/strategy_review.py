"""Generate prove-the-edge review artifacts from the active strategy run."""
from datetime import datetime, timezone
from pathlib import Path
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.comparison import compare_locked_modes
from app.paper_trading.analysis import get_strategy_health


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _fmt_money(value) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _fmt_pct(value) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def _fmt_cents(value) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}c"


def _render_review_markdown(health: dict, comparison: dict, *, as_of: datetime) -> str:
    strategy_run = health.get("strategy_run") or {}
    observation = health.get("observation") or {}
    headline = health.get("headline") or {}
    execution_realism = health.get("execution_realism") or {}
    funnel = health.get("trade_funnel") or {}
    benchmark = comparison.get("legacy") or {}
    default_strategy = comparison.get("default_strategy") or {}
    skip_reasons = health.get("skip_reasons") or []
    detector_review = health.get("detector_review") or []

    if not skip_reasons:
        skip_lines = "- No in-window skip reasons recorded yet."
    else:
        skip_lines = "\n".join(
            f"- {row['reason_label']}: {row['count']}"
            for row in skip_reasons[:5]
        )

    if not detector_review:
        detector_lines = "- No detector verdicts yet."
    else:
        detector_lines = "\n".join(
            f"- `{row['signal_type']}`: {row['verdict']} — {row['note']}"
            for row in detector_review[:6]
        )

    if funnel.get("traded_signals", 0):
        empty_state = ""
    else:
        empty_state = (
            "\n## Empty State\n\n"
            "No active-run paper trades have resolved yet. Keep the baseline frozen, "
            "watch the skip-reason funnel, and do not change the contract until the run produces measured trades.\n"
        )

    return f"""# Default Strategy Review

**Date:** {as_of.date().isoformat()}  
**Strategy:** `{strategy_run.get('strategy_name', '-')}`  
**Run ID:** `{strategy_run.get('id', '-')}`  
**Run Status:** `{strategy_run.get('status', '-')}`  

## Run Metadata

- Run start: {observation.get('started_at') or '-'}
- Immutable launch boundary: {observation.get('baseline_start_at') or '-'}
- Days tracked: {observation.get('days_tracked') if observation.get('days_tracked') is not None else '-'}
- Observation status: `{observation.get('status', '-')}`

## Current Health Snapshot

- Open exposure: {_fmt_money(headline.get('open_exposure'))}
- Resolved trades: {headline.get('resolved_trades', 0)}
- Cumulative paper P&L: {_fmt_money(headline.get('cumulative_pnl'))}
- Shadow cumulative P&L: {_fmt_money(execution_realism.get('shadow_cumulative_pnl'))}
- Average CLV: {_fmt_cents(headline.get('avg_clv'))}
- Profit factor: {headline.get('profit_factor') if headline.get('profit_factor') is not None else '-'}
- Shadow profit factor: {execution_realism.get('shadow_profit_factor') if execution_realism.get('shadow_profit_factor') is not None else '-'}
- Win rate: {_fmt_pct(headline.get('win_rate'))}
- Brier score: {headline.get('brier_score') if headline.get('brier_score') is not None else '-'}
- Max drawdown: {_fmt_money(headline.get('max_drawdown'))}

## Trade Funnel

- Candidate signals: {funnel.get('candidate_signals', 0)}
- Qualified signals: {funnel.get('qualified_signals', 0)}
- Traded signals: {funnel.get('traded_signals', 0)}
- Resolved signals: {funnel.get('resolved_signals', 0)}
- Qualified not traded: {funnel.get('qualified_not_traded', 0)}
- Legacy trades excluded: {funnel.get('excluded_legacy_trades', 0)}

## Skip Reasons

{skip_lines}

## Detector Verdicts

{detector_lines}

## Locked Comparison

| Mode | Resolved Signals | Win Rate | Avg CLV | 1-Share P&L | Max Drawdown | Paper P&L | Brier |
|------|------------------|----------|---------|-------------|--------------|-----------|-------|
| Default Strategy | {default_strategy.get('resolved_signals', 0)} | {_fmt_pct(default_strategy.get('win_rate'))} | {_fmt_cents(default_strategy.get('avg_clv'))} | {_fmt_cents(default_strategy.get('total_profit_loss_per_share'))} | {_fmt_cents(default_strategy.get('max_drawdown_per_share'))} | {_fmt_money(default_strategy.get('cumulative_pnl'))} | {default_strategy.get('brier_score') if default_strategy.get('brier_score') is not None else '-'} |
| Legacy | {benchmark.get('resolved_signals', 0)} | {_fmt_pct(benchmark.get('win_rate'))} | {_fmt_cents(benchmark.get('avg_clv'))} | {_fmt_cents(benchmark.get('total_profit_loss_per_share'))} | {_fmt_cents(benchmark.get('max_drawdown_per_share'))} | {_fmt_money(benchmark.get('cumulative_pnl'))} | {benchmark.get('brier_score') if benchmark.get('brier_score') is not None else '-'} |

## Execution Realism Caveat

- Liquidity-constrained trades: {execution_realism.get('liquidity_constrained_trades', 0)}
- Trades missing orderbook context: {execution_realism.get('trades_missing_orderbook_context', 0)}
- Shadow execution uses a conservative half-spread penalty and near-touch depth checks. It is a realism overlay, not a full market-impact model.
{empty_state}
"""


def _render_analysis_markdown(health: dict, comparison: dict, *, as_of: datetime) -> str:
    headline = health.get("headline") or {}
    execution_realism = health.get("execution_realism") or {}
    benchmark = comparison.get("legacy") or {}
    default_strategy = comparison.get("default_strategy") or {}
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
- Default strategy paper P&L: {_fmt_money(default_strategy.get('cumulative_pnl'))}
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

    review_markdown = _render_review_markdown(health, comparison, as_of=as_of)
    analysis_markdown = _render_analysis_markdown(health, comparison, as_of=as_of)
    review_path.write_text(review_markdown, encoding="utf-8")
    analysis_path.write_text(analysis_markdown, encoding="utf-8")

    return {
        "review_path": str(review_path),
        "analysis_path": str(analysis_path),
        "strategy_run": strategy_run,
        "comparison": comparison,
    }
