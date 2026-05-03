from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.market_structure import MarketStructureGroup, MarketStructureOpportunity, MarketStructureRun
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_replay import PolymarketReplayMetric, PolymarketReplayRun
from app.models.research_lab import ResearchBatch, ResearchLaneResult
from app.research_lab.orchestrator import create_research_batch, get_latest_research_batch, run_research_batch
from app.research_lab.ranker import rank_lane_payloads
from app.research_lab.universe import select_research_universe
from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


@pytest.mark.asyncio
async def test_research_universe_selector_caps_and_fingerprints(session):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for index in range(5):
        market = make_market(
            session,
            question=f"Research market {index}",
            platform="kalshi",
            end_date=now + timedelta(days=index + 1),
            last_liquidity=Decimal(str(1000 + index)),
        )
        outcome = make_outcome(session, market.id, name="Yes")
        make_price_snapshot(session, outcome.id, Decimal("0.500000"), captured_at=now - timedelta(hours=1))
        make_signal(
            session,
            market.id,
            outcome.id,
            signal_type="confluence",
            fired_at=now - timedelta(hours=1),
        )
    await session.commit()

    universe = await select_research_universe(
        session,
        window_start=now - timedelta(days=1),
        window_end=now,
        max_markets=2,
    )

    assert universe["platform"] == "kalshi"
    assert universe["market_count"] == 2
    assert universe["max_markets"] == 2
    assert universe["price_snapshot_count"] == 2
    assert universe["signal_count"] == 2
    assert len(universe["fingerprint"]) == 64


def test_research_ranker_prefers_complete_positive_ev_with_lower_risk():
    ranked = rank_lane_payloads(
        [
            {
                "family": "maker",
                "lane": "maker_replay",
                "verdict": "watch",
                "coverage_mode": "coverage_limited",
                "replay_net_pnl": Decimal("10.00"),
                "avg_clv": Decimal("0.0100"),
                "resolved_trades": 20,
                "drawdown": Decimal("3.00"),
                "open_exposure": Decimal("50.00"),
                "blockers": ["replay_coverage_limited"],
            },
            {
                "family": "structure",
                "lane": "structure_replay",
                "verdict": "healthy",
                "coverage_mode": "complete_replay",
                "replay_net_pnl": Decimal("2.00"),
                "avg_clv": Decimal("0.0020"),
                "resolved_trades": 8,
                "drawdown": Decimal("1.00"),
                "open_exposure": Decimal("0.00"),
                "blockers": [],
            },
        ]
    )

    assert ranked[0]["family"] == "structure"
    assert ranked[0]["rank_position"] == 1
    assert ranked[1]["rank_position"] == 2


@pytest.mark.asyncio
async def test_research_batch_is_idempotent_by_preset_window_and_universe(session):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    market = make_market(
        session,
        question="Idempotent market",
        platform="polymarket",
        end_date=now + timedelta(days=2),
        last_liquidity=Decimal("1500.00"),
    )
    make_outcome(session, market.id, name="Yes")
    await session.commit()

    first, first_hit = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["default_strategy"],
    )
    second, second_hit = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["default_strategy"],
    )

    assert first_hit is False
    assert second_hit is True
    assert first.id == second.id
    assert first.config_json["paper_only"] is True
    assert first.config_json["live_orders_enabled"] is False


@pytest.mark.asyncio
async def test_latest_research_batch_prefers_populated_batch_over_newer_empty(session):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    populated = ResearchBatch(
        batch_key="populated",
        preset="profit_hunt_v1",
        status="completed",
        window_start=now - timedelta(days=30),
        window_end=now,
        window_days=30,
        max_markets=500,
        universe_fingerprint="populated-fingerprint",
        families_json=["default_strategy"],
        config_json={"paper_only": True},
        universe_json={"market_count": 5, "signal_count": 10},
        rows_inserted_json={"lane_results": 1},
        details_json={},
        created_at=now,
        completed_at=now,
    )
    empty = ResearchBatch(
        batch_key="empty",
        preset="profit_hunt_v1",
        status="completed_with_warnings",
        window_start=now - timedelta(days=30),
        window_end=now + timedelta(minutes=1),
        window_days=30,
        max_markets=500,
        universe_fingerprint="empty-fingerprint",
        families_json=["default_strategy"],
        config_json={"paper_only": True},
        universe_json={"market_count": 0, "signal_count": 0},
        rows_inserted_json={"lane_results": 1},
        details_json={},
        created_at=now + timedelta(minutes=1),
        completed_at=now + timedelta(minutes=1),
    )
    session.add_all([populated, empty])
    await session.commit()

    latest = await get_latest_research_batch(session)

    assert latest is not None
    assert latest["batch"]["id"] == str(populated.id)
    note = latest["batch"]["details"]["latest_selection_note"]
    assert note["newest_batch_id"] == str(empty.id)
    assert note["newest_batch_market_count"] == 0


@pytest.mark.asyncio
async def test_research_exec_policy_lane_surfaces_candidate_ev(session, engine, monkeypatch):
    import app.research_lab.orchestrator as orchestrator_module

    monkeypatch.setattr(orchestrator_module, "write_research_batch_artifacts", lambda _payload: {})

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    candidate = PolymarketExecutionActionCandidate(
        condition_id="0xcondition",
        asset_id="token-yes",
        side="buy_yes",
        action_type="cross_now",
        target_size=Decimal("100.00"),
        est_fillable_size=Decimal("100.00"),
        est_fill_probability=Decimal("1.0"),
        est_avg_entry_price=Decimal("0.42"),
        est_worst_price=Decimal("0.43"),
        est_net_ev_bps=Decimal("125.00"),
        est_net_ev_total=Decimal("12.50"),
        valid=True,
        policy_version="test",
        decided_at=now,
    )
    invalid_candidate = PolymarketExecutionActionCandidate(
        condition_id="0xcondition",
        asset_id="token-no",
        side="buy_no",
        action_type="post_best",
        target_size=Decimal("100.00"),
        valid=False,
        invalid_reason="passive_labels_insufficient",
        policy_version="test",
        decided_at=now,
    )
    session.add_all([candidate, invalid_candidate])
    await session.flush()

    batch, _ = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["exec_policy"],
    )
    await session.commit()

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    detail = await run_research_batch(session_factory, batch.id)

    lane = detail["lane_results"][0]
    assert lane["family"] == "exec_policy"
    assert lane["status"] == "retired"
    assert lane["blockers"] == ["retired_polymarket_lane"]
    assert lane["details"]["next_step"] == "use_kalshi_only_research_lanes"
    assert not any(
        item.get("label") == f"exec_policy_candidate:{candidate.id}"
        for item in detail["top_ev_candidates"]
    )


@pytest.mark.asyncio
async def test_research_exec_policy_lane_uses_latest_replay_metrics(session, engine, monkeypatch):
    import app.research_lab.orchestrator as orchestrator_module

    monkeypatch.setattr(orchestrator_module, "write_research_batch_artifacts", lambda _payload: {})

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    candidate = PolymarketExecutionActionCandidate(
        condition_id="0xcondition",
        asset_id="token-yes",
        side="buy_yes",
        action_type="cross_now",
        target_size=Decimal("100.00"),
        est_fill_probability=Decimal("1.0"),
        est_net_ev_bps=Decimal("125.00"),
        est_net_ev_total=Decimal("12.50"),
        valid=True,
        policy_version="test",
        decided_at=now,
    )
    replay_run = PolymarketReplayRun(
        run_key="test-policy-replay",
        run_type="policy_compare",
        reason="research_lab_exec_policy",
        strategy_family="exec_policy",
        status="completed",
        scenario_count=2,
        started_at=now,
        completed_at=now + timedelta(minutes=1),
        time_window_start=now - timedelta(minutes=5),
        time_window_end=now + timedelta(minutes=30),
        config_json={},
        rows_inserted_json={"scenarios": 2},
        details_json={},
    )
    session.add_all([candidate, replay_run])
    await session.flush()
    session.add(
        PolymarketReplayMetric(
            run_id=replay_run.id,
            scenario_id=None,
            metric_scope="run",
            variant_name="exec_policy",
            gross_pnl=Decimal("10.00"),
            net_pnl=Decimal("9.50"),
            fill_rate=Decimal("1.000000"),
            cancel_rate=Decimal("0.000000"),
            action_mix_json={"cross_now": 2},
        )
    )
    batch, _ = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["exec_policy"],
    )
    await session.commit()

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    detail = await run_research_batch(session_factory, batch.id)

    lane = detail["lane_results"][0]
    assert lane["family"] == "exec_policy"
    assert lane["status"] == "retired"
    assert lane["coverage_mode"] == "not_run"
    assert lane["replay_net_pnl"] is None
    assert lane["details"]["next_step"] == "use_kalshi_only_research_lanes"


@pytest.mark.asyncio
async def test_research_exec_policy_top_ev_suppresses_failed_raw_candidates(session, engine, monkeypatch):
    import app.research_lab.orchestrator as orchestrator_module

    monkeypatch.setattr(orchestrator_module, "write_research_batch_artifacts", lambda _payload: {})

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    candidate = PolymarketExecutionActionCandidate(
        condition_id="0xcondition",
        asset_id="token-yes",
        side="buy_yes",
        action_type="cross_now",
        target_size=Decimal("100.00"),
        est_fill_probability=Decimal("1.0"),
        est_net_ev_bps=Decimal("125.00"),
        est_net_ev_total=Decimal("12.50"),
        valid=True,
        policy_version="test",
        decided_at=now,
    )
    replay_run = PolymarketReplayRun(
        run_key="test-policy-replay-warnings",
        run_type="policy_compare",
        reason="research_lab_exec_policy",
        strategy_family="exec_policy",
        status="completed_with_warnings",
        scenario_count=1,
        started_at=now,
        completed_at=now + timedelta(minutes=1),
        time_window_start=now - timedelta(minutes=5),
        time_window_end=now + timedelta(minutes=30),
        config_json={},
        rows_inserted_json={"scenarios": 1},
        details_json={"coverage_limited_scenarios": 1},
    )
    session.add_all([candidate, replay_run])
    await session.flush()
    session.add(
        PolymarketReplayMetric(
            run_id=replay_run.id,
            scenario_id=None,
            metric_scope="run",
            variant_name="exec_policy",
            gross_pnl=Decimal("0.00"),
            net_pnl=Decimal("0.00"),
            fill_rate=Decimal("0.000000"),
            cancel_rate=Decimal("1.000000"),
            action_mix_json={"skip": 1},
        )
    )
    batch, _ = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["exec_policy"],
    )
    await session.commit()

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    detail = await run_research_batch(session_factory, batch.id)

    assert detail["lane_results"][0]["status"] == "retired"
    assert detail["lane_results"][0]["blockers"] == ["retired_polymarket_lane"]
    assert not any(
        item.get("label") == f"exec_policy_candidate:{candidate.id}"
        for item in detail["top_ev_candidates"]
    )


@pytest.mark.asyncio
async def test_research_structure_lane_reports_block_reasons(session, engine, monkeypatch):
    import app.research_lab.orchestrator as orchestrator_module

    monkeypatch.setattr(orchestrator_module, "write_research_batch_artifacts", lambda _payload: {})

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    run = MarketStructureRun(
        run_type="opportunity_scan",
        reason="test",
        completed_at=now,
        status="completed",
        scope_json={},
        rows_inserted_json={},
        error_count=0,
    )
    group = MarketStructureGroup(
        group_key="test-group",
        group_type="neg_risk_event",
        primary_venue="polymarket",
        title="Test group",
        active=True,
        actionable=True,
        source_kind="test",
    )
    session.add_all([run, group])
    await session.flush()
    session.add(
        MarketStructureOpportunity(
            run_id=run.id,
            group_id=group.id,
            opportunity_type="event_sum_parity",
            observed_at_local=now,
            pricing_method="reconstructed_book",
            net_edge_bps=Decimal("250.00"),
            executable=False,
            executable_all_legs=False,
            actionable=False,
            invalid_reason="below_min_order_size",
        )
    )
    batch, _ = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["structure"],
    )
    await session.commit()

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    detail = await run_research_batch(session_factory, batch.id)

    lane = detail["lane_results"][0]
    assert lane["family"] == "structure"
    assert lane["status"] == "retired"
    assert lane["blockers"] == ["retired_polymarket_lane"]
    assert lane["details"]["next_step"] == "use_kalshi_only_research_lanes"


@pytest.mark.asyncio
async def test_research_api_create_latest_and_cancel(client):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    response = await client.post(
        "/api/v1/research/batches",
        json={
            "preset": "profit_hunt_v1",
            "window_days": 30,
            "max_markets": 10,
            "families": ["default_strategy"],
            "start_immediately": False,
            "window_start": (now - timedelta(days=30)).isoformat(),
            "window_end": now.isoformat(),
        },
    )

    assert response.status_code == 201
    created = response.json()
    assert created["started"] is False
    assert created["batch"]["status"] == "pending"

    latest = await client.get("/api/v1/research/batches/latest")
    assert latest.status_code == 200
    assert latest.json()["batch"]["id"] == created["batch"]["id"]

    cancelled = await client.post(f"/api/v1/research/batches/{created['batch']['id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["batch"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_research_batch_partial_lane_failure_becomes_warning(session, engine, monkeypatch, tmp_path):
    import app.research_lab.artifacts as artifacts_module
    import app.research_lab.orchestrator as orchestrator_module

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    batch, _ = await create_research_batch(
        session,
        window_start=now - timedelta(days=30),
        window_end=now,
        families=["default_strategy", "structure"],
    )

    async def fake_profitability(_session):
        return [
            {
                "family": "default_strategy",
                "strategy_version": "default_strategy_benchmark_v1",
                "lane": "profitability_gate",
                "source_kind": "test",
                "source_ref": "profit",
                "status": "completed",
                "verdict": "healthy",
                "realized_pnl": Decimal("3.00"),
                "mark_to_market_pnl": Decimal("0.00"),
                "replay_net_pnl": Decimal("0.00"),
                "avg_clv": Decimal("0.0100"),
                "resolved_trades": 20,
                "fill_rate": None,
                "drawdown": Decimal("0.00"),
                "open_exposure": Decimal("0.00"),
                "coverage_mode": "complete_replay",
                "blockers": [],
                "details_json": {},
            }
        ]

    async def fake_default(_session, _batch):
        return {
            "family": "default_strategy",
            "strategy_version": "default_strategy_benchmark_v1",
            "lane": "frozen_default_control",
            "source_kind": "test",
            "source_ref": "default",
            "status": "completed",
            "verdict": "watch",
            "realized_pnl": Decimal("1.00"),
            "mark_to_market_pnl": Decimal("0.00"),
            "replay_net_pnl": Decimal("0.00"),
            "avg_clv": Decimal("0.0010"),
            "resolved_trades": 5,
            "fill_rate": None,
            "drawdown": Decimal("0.10"),
            "open_exposure": Decimal("0.00"),
            "coverage_mode": "historical_signal_replay",
            "blockers": [],
            "details_json": {},
        }

    async def fake_detector(_session, _batch):
        return {
            "family": "default_strategy",
            "strategy_version": "detector_replay_sweep_v1",
            "lane": "detector_sweep",
            "source_kind": "test",
            "source_ref": "detector",
            "status": "skipped",
            "verdict": "insufficient_evidence",
            "realized_pnl": None,
            "mark_to_market_pnl": None,
            "replay_net_pnl": None,
            "avg_clv": None,
            "resolved_trades": 0,
            "fill_rate": None,
            "drawdown": None,
            "open_exposure": None,
            "coverage_mode": "not_run",
            "blockers": ["no_price_snapshots"],
            "details_json": {},
        }

    async def broken_structure(*_args):
        raise RuntimeError("structure lane boom")

    monkeypatch.setattr(orchestrator_module, "_run_profitability_lanes", fake_profitability)
    monkeypatch.setattr(orchestrator_module, "_run_default_control", fake_default)
    monkeypatch.setattr(orchestrator_module, "_run_detector_sweep", fake_detector)
    monkeypatch.setattr(orchestrator_module, "_run_structure_lane", broken_structure)
    monkeypatch.setattr(artifacts_module, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(orchestrator_module, "write_research_batch_artifacts", lambda payload: {})

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    detail = await run_research_batch(session_factory, batch.id)

    assert detail["batch"]["status"] == "completed"
    assert detail["batch"]["error_count"] == 0
    assert not any(row["status"] == "failed" for row in detail["lane_results"])
    assert any(row["status"] == "retired" for row in detail["lane_results"])
    rows = (await session.execute(select(ResearchLaneResult))).scalars().all()
    assert len(rows) == 4
