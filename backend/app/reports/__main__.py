import argparse
import asyncio
import json

from app.alpha_rule_specs import enabled_alpha_rule_blueprints
from app.db import async_session
from app.reports.alpha_factory import generate_alpha_factory_artifact
from app.reports.alpha_gauntlet import generate_alpha_gauntlet_artifact
from app.reports.api_smoke import run_evidence_api_smoke
from app.reports.execution_policy_replay import generate_execution_policy_replay_artifact
from app.reports.kalshi_cheap_yes_follow import generate_kalshi_cheap_yes_follow_artifact
from app.reports.kalshi_down_yes_fade import generate_kalshi_down_yes_fade_artifact
from app.reports.kalshi_lane_pulse import generate_kalshi_lane_pulse_artifact
from app.reports.kalshi_low_yes_fade import generate_kalshi_low_yes_fade_artifact
from app.reports.kalshi_very_low_yes_fade import generate_kalshi_very_low_yes_fade_artifact
from app.reports.profit_operations import (
    operation_result_to_json,
    run_orderbook_context_repair,
    run_resolution_accelerator,
)
from app.reports.profit_tools import generate_profit_tools_artifact
from app.reports.profitability_snapshot import generate_profitability_snapshot_artifact
from app.reports.scanner_storage import run_scanner_storage_retention
from app.reports.signal_resolution_backfill import run_signal_resolution_backfill
from app.reports.strategy_review import generate_default_strategy_review
from app.research_lab.orchestrator import create_research_batch, run_research_batch


def _default_research_families() -> str:
    families = [
        "default_strategy",
        "kalshi_down_yes_fade",
        "kalshi_low_yes_fade",
        "kalshi_very_low_yes_fade",
        "kalshi_cheap_yes_follow",
        *(str(blueprint["strategy_family"]) for blueprint in enabled_alpha_rule_blueprints()),
        "alpha_factory",
    ]
    return ",".join(families)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Signal Market Terminal evidence artifacts.")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("review", help="Generate the full default-strategy review artifacts.")
    snapshot = subparsers.add_parser("profitability-snapshot", help="Generate a cheap daily paper-profitability snapshot.")
    snapshot.add_argument("--family", default="default_strategy")

    profit_tools = subparsers.add_parser("profit-tools", help="Generate read-only profit-finding operator artifacts.")
    profit_tools.add_argument("--family", default="default_strategy")

    resolution = subparsers.add_parser(
        "resolution-accelerator",
        help="Reconcile open paper trades with local settlement evidence.",
    )
    resolution.add_argument("--apply", action="store_true", help="Resolve eligible paper trades.")
    resolution.add_argument("--limit", type=int, default=200)

    orderbook = subparsers.add_parser(
        "orderbook-context-repair",
        help="Ensure context-blocked Polymarket outcomes are watched and optionally captured.",
    )
    orderbook.add_argument("--apply", action="store_true", help="Create or update watch assets.")
    orderbook.add_argument("--capture-orderbooks", action="store_true", help="Fetch targeted current orderbooks.")
    orderbook.add_argument("--limit", type=int, default=100)

    smoke = subparsers.add_parser("smoke", help="Run read-only API smoke checks against evidence surfaces.")
    smoke.add_argument("--base-url", default="http://localhost:8000")
    smoke.add_argument("--timeout-seconds", type=float, default=15.0)

    scanner_storage = subparsers.add_parser(
        "scanner-storage",
        help="Report scanner database usage and optionally prune expired scanner rows.",
    )
    scanner_storage.add_argument("--apply", action="store_true", help="Delete expired rows; default is dry-run.")
    scanner_storage.add_argument(
        "--include-raw-events",
        action="store_true",
        help="Also prune expired polymarket_market_events rows; this can cascade normalized events/book deltas.",
    )
    scanner_storage.add_argument("--batch-size", type=int, default=5000)
    scanner_storage.add_argument(
        "--vacuum-analyze",
        action="store_true",
        help="Run VACUUM ANALYZE on tables that had rows deleted.",
    )

    research = subparsers.add_parser("research-lab", help="Run the paper-only broad EV research lab.")
    research.add_argument("--preset", default="profit_hunt_v1")
    research.add_argument("--window-days", type=int, default=30)
    research.add_argument("--max-markets", type=int, default=500)
    research.add_argument(
        "--families",
        default=_default_research_families(),
        help="Comma-separated strategy families to test.",
    )

    alpha = subparsers.add_parser("alpha-gauntlet", help="Run a read-only walk-forward alpha search.")
    alpha.add_argument("--window-days", type=int, default=365)
    alpha.add_argument("--max-signals", type=int, default=50_000)
    alpha.add_argument("--min-train-sample", type=int, default=20)
    alpha.add_argument("--min-validation-sample", type=int, default=10)
    alpha.add_argument("--min-test-sample", type=int, default=10)

    alpha_factory = subparsers.add_parser(
        "alpha-factory",
        help="Run the Kalshi-only strategy factory and emit a frozen paper-lane queue.",
    )
    alpha_factory.add_argument("--window-days", type=int, default=365)
    alpha_factory.add_argument("--max-signals", type=int, default=50_000)
    alpha_factory.add_argument("--platform", default="kalshi")
    alpha_factory.add_argument("--max-candidates", type=int, default=10)
    alpha_factory.add_argument("--min-train-sample", type=int, default=20)
    alpha_factory.add_argument("--min-validation-sample", type=int, default=10)
    alpha_factory.add_argument("--min-test-sample", type=int, default=10)

    kalshi_fade = subparsers.add_parser(
        "kalshi-low-yes-fade",
        help="Generate the paper-only Kalshi low-YES fade candidate report.",
    )
    kalshi_fade.add_argument("--window-days", type=int, default=30)
    kalshi_fade.add_argument("--max-signals", type=int, default=5000)
    kalshi_fade.add_argument("--seed-paper", action="store_true", help="Idempotently seed matching paper decisions/trades.")

    kalshi_down_fade = subparsers.add_parser(
        "kalshi-down-yes-fade",
        help="Generate the paper-only Kalshi down-YES fade v2 candidate report.",
    )
    kalshi_down_fade.add_argument("--window-days", type=int, default=30)
    kalshi_down_fade.add_argument("--max-signals", type=int, default=5000)
    kalshi_down_fade.add_argument("--seed-paper", action="store_true", help="Idempotently seed matching paper decisions/trades.")

    kalshi_very_low_fade = subparsers.add_parser(
        "kalshi-very-low-yes-fade",
        help="Generate the paper-only Kalshi very-low-YES fade candidate report.",
    )
    kalshi_very_low_fade.add_argument("--window-days", type=int, default=30)
    kalshi_very_low_fade.add_argument("--max-signals", type=int, default=5000)
    kalshi_very_low_fade.add_argument("--seed-paper", action="store_true", help="Idempotently seed matching paper decisions/trades.")

    kalshi_cheap_yes_follow = subparsers.add_parser(
        "kalshi-cheap-yes-follow",
        help="Generate the paper-only Kalshi cheap-YES follow candidate report.",
    )
    kalshi_cheap_yes_follow.add_argument("--window-days", type=int, default=30)
    kalshi_cheap_yes_follow.add_argument("--max-signals", type=int, default=5000)
    kalshi_cheap_yes_follow.add_argument("--seed-paper", action="store_true", help="Idempotently seed matching paper decisions/trades.")

    kalshi_lane_pulse = subparsers.add_parser(
        "kalshi-lane-pulse",
        help="Generate a concise forward-paper pulse report for active Kalshi lanes.",
    )
    kalshi_lane_pulse.add_argument("--window-hours", type=int, default=24)
    kalshi_lane_pulse.add_argument("--duplicate-lookback-hours", type=int, default=72)

    exec_policy_replay = subparsers.add_parser(
        "execution-policy-replay",
        help="Run or report a bounded paper-only execution-policy replay.",
    )
    exec_policy_replay.add_argument("--window-days", type=int, default=30)
    exec_policy_replay.add_argument("--max-candidates", type=int, default=20)
    exec_policy_replay.add_argument(
        "--candidate-lookback-minutes",
        type=int,
        default=None,
        help="Restrict replay selection to recently decided advisory candidates.",
    )
    exec_policy_replay.add_argument(
        "--mine-candidates",
        action="store_true",
        help="Persist fresh advisory-only execution-policy candidates from recent Polymarket signals.",
    )
    exec_policy_replay.add_argument("--max-mine-signals", type=int, default=200)
    exec_policy_replay.add_argument(
        "--mine-signal-lookback-minutes",
        type=int,
        default=None,
        help="Restrict advisory candidate mining to recent Polymarket signals.",
    )
    exec_policy_replay.add_argument(
        "--candidate-maturity-minutes",
        type=int,
        default=None,
        help=(
            "Only replay candidates at least this old. Defaults to the replay window "
            "plus one minute when --run is used."
        ),
    )
    exec_policy_replay.add_argument("--run", action="store_true", help="Trigger the bounded paper-only replay.")
    exec_policy_replay.add_argument("--repair-watch", action="store_true", help="Ensure top candidate assets are watched.")
    exec_policy_replay.add_argument("--capture-orderbooks", action="store_true", help="Capture current books for repaired watch assets.")

    signal_resolution = subparsers.add_parser(
        "signal-resolution-backfill",
        help="Fetch targeted settlement for ended markets with unresolved signals.",
    )
    signal_resolution.add_argument("--platform", default="all", choices=["all", "kalshi", "polymarket"])
    signal_resolution.add_argument("--limit", type=int, default=2000, help="Maximum markets per platform.")
    signal_resolution.add_argument("--apply", action="store_true", help="Persist resolved signal labels.")
    return parser


async def _main() -> None:
    args = _parser().parse_args()
    command = args.command or "review"
    if command == "smoke":
        result = await run_evidence_api_smoke(
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    async with async_session() as session:
        if command == "research-lab":
            families = [item.strip() for item in args.families.split(",") if item.strip()]
            batch, _idempotent = await create_research_batch(
                session,
                preset=args.preset,
                window_days=args.window_days,
                max_markets=args.max_markets,
                families=families,
            )
            result = await run_research_batch(async_session, batch.id)
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return

        if command == "scanner-storage":
            result = await run_scanner_storage_retention(
                session,
                apply=args.apply,
                include_raw_events=args.include_raw_events,
                batch_size=args.batch_size,
                vacuum_analyze=args.vacuum_analyze,
            )
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return

        if command == "alpha-gauntlet":
            result = await generate_alpha_gauntlet_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                min_train_sample=args.min_train_sample,
                min_validation_sample=args.min_validation_sample,
                min_test_sample=args.min_test_sample,
            )
            print(json.dumps(result["snapshot"], indent=2, sort_keys=True, default=str))
            return

        if command == "alpha-factory":
            result = await generate_alpha_factory_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                platform=args.platform,
                max_candidates=args.max_candidates,
                min_train_sample=args.min_train_sample,
                min_validation_sample=args.min_validation_sample,
                min_test_sample=args.min_test_sample,
            )
            print(result["alpha_factory_markdown_path"])
            print(result["alpha_factory_json_path"])
            return

        if command == "kalshi-low-yes-fade":
            result = await generate_kalshi_low_yes_fade_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                seed_paper=args.seed_paper,
            )
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        if command == "kalshi-down-yes-fade":
            result = await generate_kalshi_down_yes_fade_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                seed_paper=args.seed_paper,
            )
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        if command == "kalshi-very-low-yes-fade":
            result = await generate_kalshi_very_low_yes_fade_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                seed_paper=args.seed_paper,
            )
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        if command == "kalshi-cheap-yes-follow":
            result = await generate_kalshi_cheap_yes_follow_artifact(
                session,
                window_days=args.window_days,
                max_signals=args.max_signals,
                seed_paper=args.seed_paper,
            )
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        if command == "kalshi-lane-pulse":
            result = await generate_kalshi_lane_pulse_artifact(
                session,
                window_hours=args.window_hours,
                duplicate_lookback_hours=args.duplicate_lookback_hours,
            )
            print(result["pulse_markdown_path"])
            print(result["pulse_json_path"])
            return

        if command == "execution-policy-replay":
            result = await generate_execution_policy_replay_artifact(
                session,
                session_factory=async_session,
                window_days=args.window_days,
                max_candidates=args.max_candidates,
                candidate_lookback_minutes=args.candidate_lookback_minutes,
                mine_candidates=args.mine_candidates,
                max_mine_signals=args.max_mine_signals,
                mine_signal_lookback_minutes=args.mine_signal_lookback_minutes,
                run_replay=args.run,
                candidate_maturity_minutes=args.candidate_maturity_minutes,
                repair_watch=args.repair_watch,
                capture_orderbooks=args.capture_orderbooks,
            )
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        if command == "signal-resolution-backfill":
            result = await run_signal_resolution_backfill(
                session,
                platform=args.platform,
                limit=args.limit,
                apply=args.apply,
            )
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return

        if command == "profit-tools":
            result = await generate_profit_tools_artifact(session, family=args.family)
            print(result["profit_tools_markdown_path"])
            print(result["profit_tools_json_path"])
            return

        if command == "resolution-accelerator":
            result = await run_resolution_accelerator(session, apply=args.apply, limit=args.limit)
            print(operation_result_to_json(result))
            return

        if command == "orderbook-context-repair":
            result = await run_orderbook_context_repair(
                session,
                apply=args.apply,
                capture_orderbooks=args.capture_orderbooks,
                limit=args.limit,
            )
            print(operation_result_to_json(result))
            return

        if command == "profitability-snapshot":
            result = await generate_profitability_snapshot_artifact(session, family=args.family)
            print(result["snapshot_markdown_path"])
            print(result["snapshot_json_path"])
            return

        result = await generate_default_strategy_review(session)
        print(result["review_path"])
        print(result["review_json_path"])
        print(result["analysis_path"])


if __name__ == "__main__":
    asyncio.run(_main())
