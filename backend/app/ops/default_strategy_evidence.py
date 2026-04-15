from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.config import settings
from app.db import async_session
from app.models.execution_decision import ExecutionDecision
from app.strategy_runs.service import (
    close_active_default_strategy_run,
    get_active_strategy_run,
    open_default_strategy_run,
    serialize_strategy_run,
)

BALANCED_EVIDENCE_GATE = {
    "min_resolved_trades": 20,
    "execution_adjusted_pnl_rule": "positive",
    "max_drawdown_pct": 0.12,
    "clv_rule": "non_negative_or_improving",
    "max_brier_score": 0.25,
    "zero_funnel_integrity_failures": True,
    "require_no_hidden_shared_global_contamination": True,
}


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_default(value: Any):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))


def _clean_string(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _load_gate_payload(args: argparse.Namespace) -> dict[str, Any] | None:
    gate_path = _clean_string(args.evidence_gate_json)
    if gate_path is None:
        return BALANCED_EVIDENCE_GATE if args.use_balanced_gate else None
    payload = json.loads(Path(gate_path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("evidence gate JSON must decode to an object")
    return payload


def _append_operator_log(log_path: Path, title: str, lines: list[str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    block = "\n".join([f"## {title}", *lines]).strip() + "\n"
    if log_path.exists() and log_path.read_text(encoding="utf-8").strip():
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write("\n---\n\n")
            handle.write(block)
        return
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("# Default Strategy Operator Evidence Log\n\n")
        handle.write(block)


def _contract_metadata_from_args(args: argparse.Namespace) -> dict[str, Any]:
    evidence_boundary = {
        "boundary_id": _clean_string(args.evidence_boundary_id),
        "release_tag": _clean_string(args.release_tag),
        "commit_sha": _clean_string(args.commit_sha),
        "migration_revision": _clean_string(args.migration_revision),
    }
    evidence_boundary = {key: value for key, value in evidence_boundary.items() if value is not None}

    metadata: dict[str, Any] = {}
    if evidence_boundary:
        metadata["evidence_boundary"] = evidence_boundary
    contract_version = _clean_string(args.contract_version)
    if contract_version is not None:
        metadata["contract_version"] = contract_version
    gate_payload = _load_gate_payload(args)
    if gate_payload is not None:
        metadata["evidence_gate"] = gate_payload
    return metadata


async def _command_record_boundary(args: argparse.Namespace) -> None:
    payload = {
        "action": "record_boundary",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "release_tag": _clean_string(args.release_tag),
        "commit_sha": _clean_string(args.commit_sha),
        "migration_revision": _clean_string(args.migration_revision),
        "boundary_id": _clean_string(args.evidence_boundary_id),
        "contract_version": _clean_string(args.contract_version),
        "note": _clean_string(args.note),
        "evidence_gate": _load_gate_payload(args),
    }
    log_path = _clean_string(args.log_path)
    if log_path is not None:
        lines = [
            f"- Recorded at: {payload['recorded_at']}",
            f"- Boundary id: `{payload['boundary_id'] or payload['release_tag'] or '-'}`",
            f"- Release tag: `{payload['release_tag'] or '-'}`",
            f"- Commit SHA: `{payload['commit_sha'] or '-'}`",
            f"- Alembic revision: `{payload['migration_revision'] or '-'}`",
            f"- Contract version: `{payload['contract_version'] or '-'}`",
            f"- Note: {payload['note'] or 'Only post-fix runs count as evidence.'}",
            f"- Evidence gate: `{json.dumps(payload['evidence_gate'] or {}, sort_keys=True)}`",
        ]
        _append_operator_log(Path(log_path), "Evidence Boundary", lines)
        payload["log_path"] = str(Path(log_path))
    _print_json(payload)


async def _command_retire_active_run(args: argparse.Namespace) -> None:
    ended_at = _parse_datetime(args.ended_at)
    async with async_session() as session:
        retired_run = await close_active_default_strategy_run(session, ended_at=ended_at)
        if retired_run is not None:
            await session.commit()
            await session.refresh(retired_run)
            serialized = serialize_strategy_run(retired_run)
        else:
            serialized = None
    payload = {
        "action": "retire_active_run",
        "retired_at": datetime.now(timezone.utc).isoformat(),
        "retired_run": serialized,
        "external_labels": [
            "pre_fix_invalid_for_evidence",
            "retired_after_truth_boundary_remediation",
        ],
    }
    log_path = _clean_string(args.log_path)
    if log_path is not None:
        retired_id = serialized["id"] if serialized is not None else "-"
        lines = [
            f"- Recorded at: {payload['retired_at']}",
            f"- Retired run id: `{retired_id}`",
            "- External labels: `pre_fix_invalid_for_evidence`, `retired_after_truth_boundary_remediation`",
            f"- Note: {_clean_string(args.note) or 'Pre-fix run retired after truth-boundary remediation.'}",
        ]
        _append_operator_log(Path(log_path), "Retired Run", lines)
        payload["log_path"] = str(Path(log_path))
    _print_json(payload)


async def _command_bootstrap_run(args: argparse.Namespace) -> None:
    launch_boundary_at = _parse_datetime(args.launch_boundary_at)
    if launch_boundary_at is None:
        raise ValueError("launch_boundary_at is required for evidence bootstrap")
    bootstrap_started_at = _parse_datetime(args.bootstrap_started_at)
    contract_metadata = _contract_metadata_from_args(args)

    async with async_session() as session:
        strategy_run = await open_default_strategy_run(
            session,
            launch_boundary_at=launch_boundary_at,
            bootstrap_started_at=bootstrap_started_at,
            contract_metadata=contract_metadata,
        )
        await session.commit()
        await session.refresh(strategy_run)
        serialized = serialize_strategy_run(strategy_run)

    payload = {
        "action": "bootstrap_run",
        "bootstrapped_at": datetime.now(timezone.utc).isoformat(),
        "launch_boundary_at": launch_boundary_at.isoformat(),
        "bootstrap_started_at": bootstrap_started_at.isoformat() if bootstrap_started_at else None,
        "strategy_run": serialized,
    }
    log_path = _clean_string(args.log_path)
    if log_path is not None:
        evidence_boundary = (serialized or {}).get("contract_snapshot", {}).get("evidence_boundary", {})
        lines = [
            f"- Recorded at: {payload['bootstrapped_at']}",
            f"- Run id: `{serialized['id']}`",
            f"- Launch boundary: `{payload['launch_boundary_at']}`",
            f"- Release tag: `{evidence_boundary.get('release_tag', '-')}`",
            f"- Commit SHA: `{evidence_boundary.get('commit_sha', '-')}`",
            f"- Alembic revision: `{evidence_boundary.get('migration_revision', '-')}`",
            f"- Contract version: `{serialized['contract_snapshot'].get('contract_version', '-')}`",
        ]
        _append_operator_log(Path(log_path), "Bootstrapped Evidence Run", lines)
        payload["log_path"] = str(Path(log_path))
    _print_json(payload)


async def _command_pending_watch(args: argparse.Namespace) -> None:
    stale_after_seconds = float(args.stale_after_seconds) if args.stale_after_seconds is not None else None
    now = datetime.now(timezone.utc)
    async with async_session() as session:
        strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
        if strategy_run is None:
            payload = {
                "action": "pending_watch",
                "active_run": None,
                "count": 0,
                "max_age_seconds": 0.0,
                "oldest_decision_at": None,
                "stale_after_seconds": stale_after_seconds,
                "stale_count": 0,
                "examples": [],
            }
            _print_json(payload)
            return

        pending_rows = (
            (
                await session.execute(
                    select(ExecutionDecision)
                    .where(
                        ExecutionDecision.strategy_run_id == strategy_run.id,
                        ExecutionDecision.decision_status == "pending_decision",
                    )
                    .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
                )
            )
            .scalars()
            .all()
        )

    ages: list[float] = []
    examples = []
    stale_count = 0
    for row in pending_rows:
        decision_at = row.decision_at.astimezone(timezone.utc) if row.decision_at.tzinfo else row.decision_at.replace(tzinfo=timezone.utc)
        age_seconds = round(max(0.0, (now - decision_at).total_seconds()), 1)
        ages.append(age_seconds)
        if stale_after_seconds is not None and age_seconds >= stale_after_seconds:
            stale_count += 1
        if len(examples) < max(1, args.example_limit):
            examples.append(
                {
                    "decision_id": str(row.id),
                    "signal_id": str(row.signal_id),
                    "decision_at": decision_at.isoformat(),
                    "age_seconds": age_seconds,
                    "reason_code": row.reason_code,
                }
            )

    payload = {
        "action": "pending_watch",
        "active_run": serialize_strategy_run(strategy_run),
        "count": len(pending_rows),
        "max_age_seconds": max(ages) if ages else 0.0,
        "oldest_decision_at": examples[0]["decision_at"] if examples else None,
        "stale_after_seconds": stale_after_seconds,
        "stale_count": stale_count,
        "examples": examples,
    }
    _print_json(payload)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Operator tooling for default-strategy controlled evidence relaunches."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    boundary = subparsers.add_parser("record-boundary", help="Append an evidence-boundary entry to the operator log.")
    boundary.add_argument("--log-path", required=True)
    boundary.add_argument("--evidence-boundary-id")
    boundary.add_argument("--release-tag")
    boundary.add_argument("--commit-sha")
    boundary.add_argument("--migration-revision", default="038")
    boundary.add_argument("--contract-version")
    boundary.add_argument("--note")
    boundary.add_argument("--evidence-gate-json")
    boundary.add_argument("--use-balanced-gate", action="store_true")

    retire = subparsers.add_parser("retire-active-run", help="Close the current active default-strategy run.")
    retire.add_argument("--ended-at")
    retire.add_argument("--log-path")
    retire.add_argument("--note")

    bootstrap = subparsers.add_parser("bootstrap-run", help="Bootstrap a new active run with explicit boundary metadata.")
    bootstrap.add_argument("--launch-boundary-at", required=True)
    bootstrap.add_argument("--bootstrap-started-at")
    bootstrap.add_argument("--log-path")
    bootstrap.add_argument("--evidence-boundary-id")
    bootstrap.add_argument("--release-tag")
    bootstrap.add_argument("--commit-sha")
    bootstrap.add_argument("--migration-revision", default="038")
    bootstrap.add_argument("--contract-version")
    bootstrap.add_argument("--evidence-gate-json")
    bootstrap.add_argument("--use-balanced-gate", action="store_true")

    pending = subparsers.add_parser("pending-watch", help="Summarize age and count of pending execution decisions.")
    pending.add_argument("--stale-after-seconds", type=float)
    pending.add_argument("--example-limit", type=int, default=5)

    return parser


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "record-boundary":
        await _command_record_boundary(args)
        return
    if args.command == "retire-active-run":
        await _command_retire_active_run(args)
        return
    if args.command == "bootstrap-run":
        await _command_bootstrap_run(args)
        return
    if args.command == "pending-watch":
        await _command_pending_watch(args)
        return
    raise ValueError(f"Unsupported command: {args.command}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_dispatch(args))


if __name__ == "__main__":
    main()
