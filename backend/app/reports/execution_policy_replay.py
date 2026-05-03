"""Bounded paper-only execution-policy replay report."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.connectors import get_connector
from app.ingestion.polymarket_execution_policy import (
    POLICY_VERSION,
    evaluate_polymarket_execution_policy,
    persist_polymarket_execution_policy_result,
)
from app.ingestion.polymarket_replay_simulator import trigger_manual_polymarket_replay
from app.ingestion.polymarket_stream import upsert_watch_asset
from app.models.market import Market
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_replay import PolymarketReplayMetric, PolymarketReplayRun
from app.models.signal import Signal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.reports.strategy_review import _repo_root
from app.signals.kelly import kelly_size_for_trade

ARTIFACT_DIR = "docs/research-lab/execution-policy-replay"
SCHEMA_VERSION = "execution_policy_replay_snapshot_v1"
WATCH_REASON = "exec_policy_replay_repair"
WATCH_PRIORITY = 110
ZERO = Decimal("0")
ONE = Decimal("1")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    return value


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _depth_within_pct(levels, *, side: str, pct: Decimal = Decimal("0.10")) -> Decimal | None:
    if not levels:
        return None
    best = _parse_decimal(levels[0][0]) if levels and levels[0] else None
    if best is None or best <= ZERO:
        return None
    total = ZERO
    for level in levels:
        if len(level) < 2:
            continue
        price = _parse_decimal(level[0])
        size = _parse_decimal(level[1])
        if price is None or size is None:
            continue
        if side == "bid" and price >= best * (ONE - pct):
            total += size
        elif side == "ask" and price <= best * (ONE + pct):
            total += size
        else:
            break
    return total if total > ZERO else None


def _artifact_stem(as_of: datetime) -> str:
    return f"{as_of.date().isoformat()}-execution-policy-replay"


def _candidate_payload(row: PolymarketExecutionActionCandidate) -> dict[str, Any]:
    return {
        "id": row.id,
        "execution_decision_id": str(row.execution_decision_id) if row.execution_decision_id is not None else None,
        "signal_id": str(row.signal_id) if row.signal_id is not None else None,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "side": row.side,
        "action_type": row.action_type,
        "target_size": row.target_size,
        "est_fill_probability": row.est_fill_probability,
        "est_avg_entry_price": row.est_avg_entry_price,
        "est_worst_price": row.est_worst_price,
        "est_net_ev_bps": row.est_net_ev_bps,
        "est_net_ev_total": row.est_net_ev_total,
        "policy_version": row.policy_version,
        "decided_at": row.decided_at,
    }


def _metric_payload(row: PolymarketReplayMetric) -> dict[str, Any]:
    return {
        "variant_name": row.variant_name,
        "metric_scope": row.metric_scope,
        "gross_pnl": row.gross_pnl,
        "net_pnl": row.net_pnl,
        "fees_paid": row.fees_paid,
        "rewards_estimated": row.rewards_estimated,
        "slippage_bps": row.slippage_bps,
        "fill_rate": row.fill_rate,
        "cancel_rate": row.cancel_rate,
        "drawdown_proxy": row.drawdown_proxy,
        "action_mix": row.action_mix_json or {},
    }


async def _load_top_candidates(
    session: AsyncSession,
    *,
    max_candidates: int,
    window_days: int,
    candidate_lookback_minutes: int | None = None,
    candidate_maturity_minutes: int | None = None,
) -> list[PolymarketExecutionActionCandidate]:
    cutoff = _utcnow() - timedelta(days=max(1, int(window_days)))
    if candidate_lookback_minutes is not None:
        cutoff = max(cutoff, _utcnow() - timedelta(minutes=max(1, int(candidate_lookback_minutes))))
    maturity_cutoff = None
    if candidate_maturity_minutes is not None:
        maturity_cutoff = _utcnow() - timedelta(minutes=max(0, int(candidate_maturity_minutes)))
    filters = [
        PolymarketExecutionActionCandidate.valid.is_(True),
        PolymarketExecutionActionCandidate.decided_at >= cutoff,
    ]
    if maturity_cutoff is not None:
        filters.append(PolymarketExecutionActionCandidate.decided_at <= maturity_cutoff)
    rows = (
        await session.execute(
            select(PolymarketExecutionActionCandidate)
            .where(*filters)
            .order_by(
                PolymarketExecutionActionCandidate.est_net_ev_total.desc().nullslast(),
                PolymarketExecutionActionCandidate.est_net_ev_bps.desc().nullslast(),
                PolymarketExecutionActionCandidate.decided_at.desc(),
                PolymarketExecutionActionCandidate.id.desc(),
            )
            .limit(max(1, int(max_candidates)))
        )
    ).scalars().all()
    return list(rows)


async def _mine_fresh_advisory_candidates(
    session: AsyncSession,
    *,
    window_days: int,
    max_signals: int,
    signal_lookback_minutes: int | None,
) -> dict[str, Any]:
    cutoff = _utcnow() - timedelta(days=max(1, int(window_days)))
    if signal_lookback_minutes is not None:
        cutoff = max(cutoff, _utcnow() - timedelta(minutes=max(1, int(signal_lookback_minutes))))

    rows = (
        await session.execute(
            select(Signal)
            .join(Market, Market.id == Signal.market_id)
            .where(
                Market.platform == "polymarket",
                Signal.fired_at >= cutoff,
                Signal.outcome_id.is_not(None),
                Signal.estimated_probability.is_not(None),
                Signal.price_at_fire.is_not(None),
            )
            .order_by(
                Signal.fired_at.desc(),
                Signal.rank_score.desc(),
                Signal.id.desc(),
            )
            .limit(max(1, int(max_signals)))
        )
    ).scalars().all()

    evaluated = 0
    applicable = 0
    skipped_existing = 0
    inserted_rows = 0
    valid_rows = 0
    chosen_actions: dict[str, int] = {}
    reasons: dict[str, int] = {}

    for signal in rows:
        existing = await session.scalar(
            select(PolymarketExecutionActionCandidate.id)
            .where(
                PolymarketExecutionActionCandidate.signal_id == signal.id,
                PolymarketExecutionActionCandidate.policy_version == POLICY_VERSION,
            )
            .limit(1)
        )
        if existing is not None:
            skipped_existing += 1
            continue

        evaluated += 1
        direction = "buy_yes" if signal.estimated_probability >= signal.price_at_fire else "buy_no"
        baseline = kelly_size_for_trade(
            direction=direction,
            estimated_probability=signal.estimated_probability,
            entry_price=signal.price_at_fire,
            bankroll=Decimal(str(settings.default_bankroll)),
            kelly_fraction=Decimal(str(settings.kelly_multiplier)),
            max_position_pct=Decimal(str(settings.max_single_position_pct)),
        )
        result = await evaluate_polymarket_execution_policy(
            session,
            signal_id=signal.id,
            outcome_id=signal.outcome_id,
            market_id=signal.market_id,
            direction=direction,
            estimated_probability=signal.estimated_probability,
            market_price=signal.price_at_fire,
            decision_at=_ensure_utc(signal.fired_at) or _utcnow(),
            baseline_target_size=baseline["recommended_size_usd"],
            bankroll=Decimal(str(settings.default_bankroll)),
            force_enabled=True,
        )
        if not result.applicable or result.context is None:
            reasons[result.chosen_reason or "not_applicable"] = reasons.get(result.chosen_reason or "not_applicable", 0) + 1
            continue

        applicable += 1
        persisted = await persist_polymarket_execution_policy_result(
            session,
            result=result,
            execution_decision=None,
        )
        inserted_rows += len(persisted)
        valid_rows += sum(1 for row in persisted if row.valid)
        chosen = result.chosen_candidate.action_type if result.chosen_candidate is not None else "none"
        chosen_actions[chosen] = chosen_actions.get(chosen, 0) + 1
        if result.chosen_reason:
            reasons[result.chosen_reason] = reasons.get(result.chosen_reason, 0) + 1

    await session.commit()
    return {
        "paper_only": True,
        "live_orders_enabled": False,
        "policy_globally_enabled": bool(settings.polymarket_execution_policy_enabled),
        "force_advisory_evaluation": True,
        "signal_cutoff": cutoff,
        "signals_loaded": len(rows),
        "signals_evaluated": evaluated,
        "signals_applicable": applicable,
        "skipped_existing_signals": skipped_existing,
        "candidate_rows_inserted": inserted_rows,
        "valid_candidate_rows_inserted": valid_rows,
        "chosen_actions": chosen_actions,
        "reasons": reasons,
    }


async def _latest_policy_run(session: AsyncSession) -> PolymarketReplayRun | None:
    return (
        await session.execute(
            select(PolymarketReplayRun)
            .where(PolymarketReplayRun.run_type == "policy_compare")
            .order_by(PolymarketReplayRun.started_at.desc(), PolymarketReplayRun.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def _run_metrics(session: AsyncSession, run_id) -> dict[str, Any]:
    rows = (
        await session.execute(
            select(PolymarketReplayMetric)
            .where(
                PolymarketReplayMetric.run_id == run_id,
                PolymarketReplayMetric.metric_scope == "run",
            )
            .order_by(PolymarketReplayMetric.variant_name.asc())
        )
    ).scalars().all()
    return {row.variant_name: _json_safe(_metric_payload(row)) for row in rows}


async def _repair_candidate_watch_assets(
    session: AsyncSession,
    candidates: list[PolymarketExecutionActionCandidate],
    *,
    capture_orderbooks: bool,
) -> dict[str, Any]:
    watch_assets_ensured = 0
    token_rows: list[tuple[Any, str]] = []
    seen_outcomes = set()
    for candidate in candidates:
        if candidate.outcome_id is None and not candidate.asset_id:
            continue
        watch_asset = await upsert_watch_asset(
            session,
            outcome_id=candidate.outcome_id,
            asset_id=candidate.asset_id,
            watch_enabled=True,
            watch_reason=WATCH_REASON,
            priority=WATCH_PRIORITY,
        )
        watch_assets_ensured += 1
        if candidate.outcome_id is not None and candidate.outcome_id not in seen_outcomes:
            token_rows.append((candidate.outcome_id, watch_asset.asset_id))
            seen_outcomes.add(candidate.outcome_id)
    await session.commit()

    capture_result = None
    if capture_orderbooks:
        connector = get_connector("polymarket")
        now = _utcnow()
        price_count = 0
        orderbook_count = 0
        errors: list[dict[str, str]] = []
        try:
            token_ids = [token_id for _outcome_id, token_id in token_rows if token_id]
            midpoints = await connector.fetch_midpoints(token_ids) if token_ids else {}
            for outcome_id, token_id in token_rows:
                midpoint = midpoints.get(token_id)
                if midpoint is not None:
                    session.add(PriceSnapshot(outcome_id=outcome_id, price=midpoint, captured_at=now))
                    price_count += 1
                try:
                    orderbook = await connector.fetch_orderbook(token_id)
                except Exception as exc:
                    errors.append({"token_id": token_id, "error": str(exc)[:300]})
                    continue
                session.add(
                    OrderbookSnapshot(
                        outcome_id=outcome_id,
                        bids=orderbook.bids,
                        asks=orderbook.asks,
                        spread=orderbook.spread,
                        depth_bid_10pct=_depth_within_pct(orderbook.bids, side="bid"),
                        depth_ask_10pct=_depth_within_pct(orderbook.asks, side="ask"),
                        captured_at=now,
                    )
                )
                orderbook_count += 1
            await session.commit()
        finally:
            await connector.close()
        capture_result = {
            "requested_tokens": len(token_rows),
            "price_snapshots": price_count,
            "orderbook_snapshots": orderbook_count,
            "errors": errors[:20],
        }

    return {
        "watch_assets_ensured": watch_assets_ensured,
        "unique_outcomes": len(token_rows),
        "watch_reason": WATCH_REASON,
        "watch_priority": WATCH_PRIORITY,
        "capture_orderbooks": bool(capture_orderbooks),
        "capture_result": capture_result,
    }


async def build_execution_policy_replay_snapshot(
    session: AsyncSession,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    max_candidates: int = 20,
    window_days: int = 30,
    candidate_lookback_minutes: int | None = None,
    mine_candidates: bool = False,
    max_mine_signals: int = 200,
    mine_signal_lookback_minutes: int | None = None,
    run_replay: bool = False,
    candidate_maturity_minutes: int | None = None,
    repair_watch: bool = False,
    capture_orderbooks: bool = False,
) -> dict[str, Any]:
    as_of = _utcnow()
    effective_candidate_maturity_minutes = candidate_maturity_minutes
    if run_replay and effective_candidate_maturity_minutes is None:
        effective_candidate_maturity_minutes = max(1, int(settings.polymarket_replay_default_window_minutes) + 1)
    mine_result = None
    if mine_candidates:
        mine_result = await _mine_fresh_advisory_candidates(
            session,
            window_days=window_days,
            max_signals=max_mine_signals,
            signal_lookback_minutes=mine_signal_lookback_minutes,
        )
    candidates = await _load_top_candidates(
        session,
        max_candidates=max_candidates,
        window_days=window_days,
        candidate_lookback_minutes=candidate_lookback_minutes,
        candidate_maturity_minutes=effective_candidate_maturity_minutes,
    )
    selected = [_candidate_payload(row) for row in candidates]
    blockers: list[str] = []
    replay_result = None
    latest_run = await _latest_policy_run(session)

    repair_result = None
    if repair_watch and candidates:
        repair_result = await _repair_candidate_watch_assets(
            session,
            candidates,
            capture_orderbooks=capture_orderbooks,
        )

    if run_replay:
        if session_factory is None:
            raise ValueError("session_factory is required when run_replay=True")
        if not candidates:
            blockers.append("no_execution_policy_candidates")
        else:
            decision_times = [_ensure_utc(row.decided_at) for row in candidates if row.decided_at]
            window_start = min(decision_times) - timedelta(minutes=1) if decision_times else as_of - timedelta(days=1)
            window_end = max(decision_times) + timedelta(minutes=31) if decision_times else as_of
            window_end = min(window_end, as_of)
            replay_result = await trigger_manual_polymarket_replay(
                session_factory,
                reason="research_lab_exec_policy",
                run_type="policy_compare",
                start=window_start,
                end=window_end,
                asset_ids=sorted({row.asset_id for row in candidates if row.asset_id}),
                condition_ids=sorted({row.condition_id for row in candidates if row.condition_id}),
                limit=max_candidates,
            )
            run_payload = replay_result.get("run") or {}
            run_id = run_payload.get("id")
            if run_id:
                latest_run = await session.get(PolymarketReplayRun, run_id)

    metrics = await _run_metrics(session, latest_run.id) if latest_run is not None else {}
    exec_metric = metrics.get("exec_policy") or {}
    run_status = latest_run.status if latest_run is not None else None
    replay_net_pnl = exec_metric.get("net_pnl")
    selected_decision_times = [_ensure_utc(row.decided_at) for row in candidates if row.decided_at]
    next_complete_replay_at = None
    if selected_decision_times:
        next_complete_replay_at = max(selected_decision_times) + timedelta(
            minutes=max(1, int(settings.polymarket_replay_default_window_minutes) + 1)
        )
    coverage_mode = "not_run"
    if latest_run is not None:
        coverage_mode = "complete_replay" if run_status == "completed" else "replay_coverage_limited"
    if latest_run is None:
        blockers.append("no_execution_policy_replay_run")
    elif run_status not in {"completed", "completed_with_warnings"}:
        blockers.append("execution_policy_replay_failed")
    elif run_status == "completed_with_warnings":
        blockers.append("replay_coverage_limited")
    if replay_net_pnl is None:
        blockers.append("missing_exec_policy_replay_metric")
    elif float(replay_net_pnl) <= 0:
        blockers.append("nonpositive_exec_policy_replay_pnl")

    verdict = "watch"
    if latest_run is None:
        verdict = "not_run"
    elif not blockers and replay_net_pnl is not None and float(replay_net_pnl) > 0:
        verdict = "research_ready"
    elif latest_run is not None:
        verdict = "needs_replay_improvement"

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": as_of,
        "paper_only": True,
        "live_orders_enabled": False,
        "pilot_arming_enabled": False,
        "window_days": window_days,
        "max_candidates": max_candidates,
        "candidate_lookback_minutes": candidate_lookback_minutes,
        "candidate_maturity_minutes": effective_candidate_maturity_minutes,
        "next_complete_replay_at": next_complete_replay_at,
        "candidate_count": len(candidates),
        "selected_candidates": selected,
        "mine_requested": bool(mine_candidates),
        "mine_result": mine_result,
        "replay_requested": bool(run_replay),
        "replay_result": replay_result,
        "repair_requested": bool(repair_watch),
        "repair_result": repair_result,
        "latest_run": _json_safe(
            {
                "id": str(latest_run.id),
                "run_key": latest_run.run_key,
                "run_type": latest_run.run_type,
                "reason": latest_run.reason,
                "status": latest_run.status,
                "scenario_count": latest_run.scenario_count,
                "started_at": latest_run.started_at,
                "completed_at": latest_run.completed_at,
                "time_window_start": latest_run.time_window_start,
                "time_window_end": latest_run.time_window_end,
                "rows_inserted": latest_run.rows_inserted_json,
                "details": latest_run.details_json,
            }
            if latest_run is not None
            else None
        ),
        "variant_metrics": metrics,
        "coverage_mode": coverage_mode,
        "exec_policy_replay_net_pnl": replay_net_pnl,
        "exec_policy_fill_rate": exec_metric.get("fill_rate"),
        "blockers": sorted(set(blockers)),
        "verdict": verdict,
    }


def _render_markdown(snapshot: dict[str, Any]) -> str:
    latest = snapshot.get("latest_run") or {}
    metrics = snapshot.get("variant_metrics") or {}
    mine = snapshot.get("mine_result") or {}
    blockers = "\n".join(f"- {item}" for item in snapshot.get("blockers") or []) or "- None"
    metric_lines = []
    for variant, row in metrics.items():
        metric_lines.append(
            f"- {variant}: net_pnl={row.get('net_pnl')}, fill_rate={row.get('fill_rate')}, "
            f"slippage_bps={row.get('slippage_bps')}"
        )
    variants = "\n".join(metric_lines) or "- None"
    return f"""# Execution-Policy Replay

- Verdict: {snapshot.get('verdict')}
- Coverage: {snapshot.get('coverage_mode')}
- Candidates selected: {snapshot.get('candidate_count')}
- Candidate lookback minutes: {snapshot.get('candidate_lookback_minutes')}
- Candidate maturity minutes: {snapshot.get('candidate_maturity_minutes')}
- Next complete replay at: {snapshot.get('next_complete_replay_at')}
- Mine requested: {snapshot.get('mine_requested')}
- Mine inserted rows: {mine.get('candidate_rows_inserted')}
- Mine valid rows: {mine.get('valid_candidate_rows_inserted')}
- Replay requested: {snapshot.get('replay_requested')}
- Watch repair requested: {snapshot.get('repair_requested')}
- Latest run: {latest.get('id')} ({latest.get('status')})
- Scenario count: {latest.get('scenario_count')}
- Repair result: {snapshot.get('repair_result')}

## Variant Metrics

{variants}

## Blockers

{blockers}
"""


async def generate_execution_policy_replay_artifact(
    session: AsyncSession,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    max_candidates: int = 20,
    window_days: int = 30,
    candidate_lookback_minutes: int | None = None,
    mine_candidates: bool = False,
    max_mine_signals: int = 200,
    mine_signal_lookback_minutes: int | None = None,
    run_replay: bool = False,
    candidate_maturity_minutes: int | None = None,
    repair_watch: bool = False,
    capture_orderbooks: bool = False,
) -> dict[str, Any]:
    snapshot = await build_execution_policy_replay_snapshot(
        session,
        session_factory=session_factory,
        max_candidates=max_candidates,
        window_days=window_days,
        candidate_lookback_minutes=candidate_lookback_minutes,
        mine_candidates=mine_candidates,
        max_mine_signals=max_mine_signals,
        mine_signal_lookback_minutes=mine_signal_lookback_minutes,
        run_replay=run_replay,
        candidate_maturity_minutes=candidate_maturity_minutes,
        repair_watch=repair_watch,
        capture_orderbooks=capture_orderbooks,
    )
    root = _repo_root()
    artifact_dir = root / ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(_utcnow())
    json_path = artifact_dir / f"{stem}.json"
    markdown_path = artifact_dir / f"{stem}.md"
    safe_snapshot = _json_safe(snapshot)
    json_path.write_text(json.dumps(safe_snapshot, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(safe_snapshot), encoding="utf-8")
    return {
        "snapshot": safe_snapshot,
        "snapshot_json_path": str(json_path),
        "snapshot_markdown_path": str(markdown_path),
    }
