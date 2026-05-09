from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.reports.alpha_evidence_board import (
    build_alpha_evidence_board_from_snapshots,
    generate_alpha_evidence_board_artifact,
)


def _candidate(**overrides):
    payload = {
        "candidate_id": "kalshi_alpha_test",
        "strategy_version": "alpha_kalshi_test_v1",
        "rank": 1,
        "ready_for_paper_lane": True,
        "trade_direction": "buy_no",
        "strategy_archetype": "fade_negative_yes_ev",
        "dedupe_status": "new_candidate",
        "next_step": "implement_frozen_paper_lane",
        "blockers": [],
        "metrics": {
            "test": {
                "sample_count": 42,
                "total_profit_loss": 12.5,
                "avg_clv": 0.031,
                "max_drawdown": 1.2,
            }
        },
        "rule_digest": "abc123",
        "rule_label": "price_move kalshi down",
    }
    payload.update(overrides)
    return payload


def _alpha_snapshot(*, queue=None, top=None, verdict="candidate_queue_ready"):
    return {
        "schema_version": "alpha_factory_v2",
        "generated_at": "2026-05-09T10:00:00+00:00",
        "platform": "kalshi",
        "window_days": 365,
        "max_signals": 50_000,
        "verdict": verdict,
        "candidate_pool_count": len(top or queue or []),
        "existing_ready_candidate_count": sum(1 for item in top or [] if item.get("existing_lane")),
        "suppressed_candidate_count": 0,
        "candidate_queue": list(queue or []),
        "top_candidates": list(top or queue or []),
        "warnings": [],
    }


def _lane(family="kalshi_down_yes_fade", **overrides):
    payload = {
        "family": family,
        "strategy_run_id": "run-1",
        "strategy_name": f"{family}_v1",
        "run_status": "active",
        "open_trades": 0,
        "open_markets": 0,
        "open_exposure": 0.0,
        "opened_trades_window": 0,
        "resolved_trades_window": 0,
        "realized_pnl_window": 0.0,
        "avg_resolved_pnl_window": None,
        "decision_reasons": [],
        "duplicate_market_warnings": [],
        "quarantine": {"enabled": False},
    }
    payload.update(overrides)
    return payload


def _lane_pulse(*, lanes=None, verdict="waiting_for_signals"):
    return {
        "schema_version": "kalshi_lane_pulse_v1",
        "generated_at": "2026-05-09T10:00:00+00:00",
        "window_hours": 48,
        "duplicate_lookback_hours": 72,
        "verdict": verdict,
        "lanes": list(lanes or []),
    }


def test_alpha_evidence_board_prioritizes_hygiene_over_new_candidates():
    now = datetime(2026, 5, 9, tzinfo=timezone.utc)
    board = build_alpha_evidence_board_from_snapshots(
        alpha_snapshot=_alpha_snapshot(queue=[_candidate()]),
        lane_pulse=_lane_pulse(
            verdict="needs_evidence_hygiene",
            lanes=[
                _lane(
                    duplicate_market_warnings=[
                        {"market_id": "m1", "trade_count": 2, "severity": "active_duplicate"}
                    ],
                )
            ],
        ),
        generated_at=now,
    )

    assert board["paper_only"] is True
    assert board["live_submission_permitted"] is False
    assert board["verdict"] == "needs_evidence_hygiene"
    assert board["summary"]["duplicate_warning_count"] == 1
    assert board["summary"]["new_paper_lane_candidates"] == 1
    assert board["next_best_actions"][0]["step"] == "repair_evidence_hygiene"


def test_alpha_evidence_board_surfaces_new_paper_lane_candidate():
    candidate = _candidate()

    board = build_alpha_evidence_board_from_snapshots(
        alpha_snapshot=_alpha_snapshot(queue=[candidate]),
        lane_pulse=_lane_pulse(lanes=[_lane(open_trades=0)]),
        generated_at=datetime(2026, 5, 9, tzinfo=timezone.utc),
    )

    assert board["verdict"] == "new_paper_lane_ready"
    assert board["candidate_cards"][0]["action_bucket"] == "implement_next"
    assert board["candidate_cards"][0]["test"]["sample_count"] == 42
    assert board["next_best_actions"][0]["step"] == "implement_next_frozen_paper_lane"


def test_alpha_evidence_board_keeps_existing_lanes_collecting_forward_evidence():
    existing = _candidate(
        candidate_id="kalshi_alpha_existing",
        strategy_version="kalshi_down_yes_fade_v2",
        dedupe_status="exact_existing_lane",
        existing_lane={"family": "kalshi_down_yes_fade", "strategy_version": "kalshi_down_yes_fade_v2"},
    )

    board = build_alpha_evidence_board_from_snapshots(
        alpha_snapshot=_alpha_snapshot(queue=[], top=[existing], verdict="existing_lanes_collecting_forward_evidence"),
        lane_pulse=_lane_pulse(lanes=[_lane(open_trades=3, open_markets=3, open_exposure=900.0)]),
        generated_at=datetime(2026, 5, 9, tzinfo=timezone.utc),
    )

    assert board["verdict"] == "collecting_forward_evidence"
    assert board["summary"]["open_forward_trades"] == 3
    assert board["candidate_cards"][0]["action_bucket"] == "collecting_forward_evidence"
    assert any(action["step"] == "wait_for_forward_paper_resolutions" for action in board["next_best_actions"])


@pytest.mark.asyncio
async def test_alpha_evidence_board_artifact_generator_writes_json_and_markdown(session, monkeypatch, tmp_path: Path):
    import app.reports.alpha_evidence_board as board_module

    monkeypatch.setattr(board_module, "_repo_root", lambda: tmp_path)

    result = await generate_alpha_evidence_board_artifact(
        session,
        window_days=30,
        max_signals=25,
        max_candidates=3,
        lane_window_hours=24,
    )

    json_path = Path(result["evidence_board_json_path"])
    markdown_path = Path(result["evidence_board_markdown_path"])
    assert json_path.exists()
    assert markdown_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "alpha_evidence_board_v1"
    assert "Alpha Evidence Board" in markdown_path.read_text(encoding="utf-8")
