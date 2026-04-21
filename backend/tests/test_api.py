"""Integration tests for API endpoints."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.models.ingestion import IngestionRun
from app.models.paper_trade import PaperTrade
from app.models.scheduler_lease import SchedulerLease
from app.models.signal import SignalEvaluation
from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal

# ── Health ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "active_markets" in data
    assert "total_signals" in data
    assert "unresolved_signals" in data
    assert "recent_alerts_24h" in data
    assert "alert_threshold" in data
    assert "ingestion" in data
    assert isinstance(data["ingestion"], list)
    assert "polymarket_phase1" in data
    assert "polymarket_phase8a" in data
    assert "scheduler_lease" in data
    assert "default_strategy_runtime" in data
    assert "runtime_invariants" in data
    assert "strategy_families" in data


@pytest.mark.asyncio
async def test_health_endpoint_surfaces_scheduler_and_runtime_status(client, session, monkeypatch):
    from app.api import health as health_api

    monkeypatch.setattr(health_api.settings, "scheduler_enabled", True)
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        platform="kalshi",
        platform_id="KXHEALTH-OVERDUE",
        question="Health overdue market",
        end_date=now - timedelta(hours=2),
        active=True,
    )
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes", platform_outcome_id="KXHEALTH-OVERDUE_yes")
    await session.flush()
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.006000"),
    )
    await session.flush()

    session.add(
        PaperTrade(
            id=uuid.uuid4(),
            signal_id=signal.id,
            outcome_id=outcome.id,
            market_id=market.id,
            direction="buy_yes",
            entry_price=Decimal("0.400000"),
            size_usd=Decimal("500.00"),
            shares=Decimal("1250.0000"),
            status="open",
            opened_at=now - timedelta(hours=1),
            details={"market_question": "Health overdue market"},
        )
    )
    session.add(
        SignalEvaluation(
            id=uuid.uuid4(),
            signal_id=signal.id,
            horizon="15m",
            price_at_eval=Decimal("1.000000"),
            price_change=Decimal("0.994000"),
            price_change_pct=Decimal("9999.9999"),
            evaluated_at=now - timedelta(hours=1),
        )
    )
    session.add(
        SchedulerLease(
            scheduler_name="default",
            owner_token="default:health-host:123:abc",
            acquired_at=now - timedelta(seconds=10),
            heartbeat_at=now - timedelta(seconds=5),
            expires_at=now + timedelta(seconds=25),
        )
    )
    session.add(
        IngestionRun(
            run_type="resolution_backfill",
            platform="kalshi",
            status="success",
            started_at=now - timedelta(minutes=30),
            finished_at=now - timedelta(minutes=29),
            markets_processed=2,
        )
    )
    session.add(
        IngestionRun(
            run_type="evaluation",
            platform="system",
            status="error",
            started_at=now - timedelta(minutes=20),
            finished_at=now - timedelta(minutes=19),
            markets_processed=1,
            error="1 signal evaluation horizon(s) failed",
        )
    )
    await session.commit()

    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()

    assert data["status"] == "degraded"
    assert data["scheduler_lease"]["owner_token"] == "default:health-host:123:abc"
    assert data["scheduler_lease"]["heartbeat_freshness_seconds"] >= 5
    assert data["scheduler_lease"]["expires_in_seconds"] >= 0
    assert data["default_strategy_runtime"]["overdue_open_trades"] == 1
    assert data["default_strategy_runtime"]["last_resolution_backfill_count"] == 2
    assert data["default_strategy_runtime"]["last_resolution_backfill_at"] is not None
    assert data["default_strategy_runtime"]["evaluation_clamp_count_24h"] == 1
    assert data["default_strategy_runtime"]["last_evaluation_failure_at"] is not None
    invariant_by_key = {row["key"]: row for row in data["runtime_invariants"]}
    assert invariant_by_key["scheduler_lease_fresh"]["status"] == "passing"
    assert invariant_by_key["overdue_open_trades_zero"]["status"] == "failing"
    assert invariant_by_key["evaluation_failures_24h_zero"]["status"] == "failing"


@pytest.mark.asyncio
async def test_strategies_registry_endpoint_seeds_phase13a_registry_and_exposes_benchmark_linkage(client, session):
    from app.strategy_runs.service import open_default_strategy_run

    await open_default_strategy_run(session)
    await session.commit()

    resp = await client.get("/api/v1/strategies")
    assert resp.status_code == 200
    data = resp.json()

    assert data["summary"]["phase"] == "13A"
    assert data["summary"]["benchmark_family"] == "default_strategy"
    assert data["summary"]["family_count"] >= 5

    families = {row["family"]: row for row in data["families"]}
    assert families["default_strategy"]["current_version"]["version_key"] == "default_strategy_benchmark_v1"
    assert families["default_strategy"]["current_version"]["version_status"] == "benchmark"
    assert families["default_strategy"]["current_version"]["evidence_counts"]["strategy_runs"] >= 1
    assert families["default_strategy"]["current_version"]["evidence_alignment"]["surface_status"] == "registry_only"
    assert families["exec_policy"]["family_kind"] == "infrastructure"
    assert families["exec_policy"]["current_version"]["autonomy_tier"] == "assisted_live"
    assert data["gate_policies"][0]["policy_key"] == "promotion_gate_policy_v1"

    detail_response = await client.get(f"/api/v1/strategies/versions/{families['default_strategy']['current_version']['id']}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["version"]["version_key"] == "default_strategy_benchmark_v1"
    assert detail_payload["family"]["family"] == "default_strategy"
    assert detail_payload["replay_runs"] == []
    assert detail_payload["live_shadow_evaluations"] == []
    assert detail_payload["gate_history"] == []


# ── Signals list ────────────────────────────────────────


@pytest.mark.asyncio
async def test_signals_list_empty(client):
    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["signals"] == []
    assert data["total"] == 0
    assert data["page"] == 1


@pytest.mark.asyncio
async def test_signals_list_returns_paginated(client, engine):
    """Signals are returned with correct schema and pagination."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session, platform="polymarket")
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        for i in range(3):
            make_signal(
                session, market.id, outcome.id,
                signal_type="price_move",
                rank_score=Decimal(f"0.{5 + i}00"),
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(minutes=15 * i),
            )
        await session.commit()

    resp = await client.get("/api/v1/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert len(data["signals"]) == 3
    # Check schema fields
    s = data["signals"][0]
    assert "id" in s
    assert "signal_type" in s
    assert "rank_score" in s
    assert "market_question" in s
    assert "platform" in s


@pytest.mark.asyncio
async def test_signals_filter_by_type(client, engine):
    """Filter signals by signal_type."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        now = datetime.now(timezone.utc)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        make_signal(session, market.id, outcome.id, signal_type="price_move", dedupe_bucket=bucket)
        make_signal(session, market.id, outcome.id, signal_type="volume_spike", dedupe_bucket=bucket - timedelta(minutes=15))
        await session.commit()

    resp = await client.get("/api/v1/signals?signal_type=price_move")
    data = resp.json()
    assert data["total"] == 1
    assert data["signals"][0]["signal_type"] == "price_move"


# ── Signal detail ───────────────────────────────────────


@pytest.mark.asyncio
async def test_signal_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/signals/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_signal_detail_with_evaluations(client, engine):
    """GET /signals/{id} returns signal with evaluations."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.models.signal import SignalEvaluation
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        signal = make_signal(session, market.id, outcome.id)
        await session.flush()
        # Add an evaluation
        ev = SignalEvaluation(
            id=uuid.uuid4(),
            signal_id=signal.id,
            horizon="15m",
            price_at_eval=Decimal("0.550"),
            price_change=Decimal("0.050"),
            price_change_pct=Decimal("10.00"),
        )
        session.add(ev)
        await session.commit()
        signal_id = str(signal.id)

    resp = await client.get(f"/api/v1/signals/{signal_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == signal_id
    assert len(data["evaluations"]) == 1
    assert data["evaluations"][0]["horizon"] == "15m"


@pytest.mark.asyncio
async def test_signal_detail_exposes_display_and_strategy_family_metadata(client, engine):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session, platform="polymarket", question="Spread research market")
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        signal = make_signal(session, market.id, outcome.id, signal_type="arbitrage")
        await session.commit()
        signal_id = str(signal.id)

    resp = await client.get(f"/api/v1/signals/{signal_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["display_signal_type"] == "cross_venue_spread"
    assert data["display_signal_label"] == "Cross Venue Spread"
    assert data["review_family"] == "cross_venue_basis"
    assert data["review_family_posture"] == "disabled"
    assert data["review_family_review_enabled"] is False


# ── Markets list ────────────────────────────────────────


@pytest.mark.asyncio
async def test_markets_list_empty(client):
    resp = await client.get("/api/v1/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["markets"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_markets_list_returns_data(client, engine):
    """GET /markets returns active markets."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        make_market(session, platform="polymarket", question="Will BTC hit 100k?")
        make_market(session, platform="kalshi", question="Will the Fed cut rates?")
        await session.commit()

    resp = await client.get("/api/v1/markets")
    data = resp.json()
    assert data["total"] == 2
    assert len(data["markets"]) == 2


# ── Market detail ───────────────────────────────────────


@pytest.mark.asyncio
async def test_market_not_found(client):
    fake_id = str(uuid.uuid4())
    resp = await client.get(f"/api/v1/markets/{fake_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_market_detail_with_outcomes(client, engine):
    """GET /markets/{id} returns outcomes with latest prices."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session, question="Test market?")
        await session.flush()
        outcome = make_outcome(session, market.id, name="Yes")
        await session.flush()
        make_price_snapshot(session, outcome.id, "0.65")
        await session.commit()
        market_id = str(market.id)

    resp = await client.get(f"/api/v1/markets/{market_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["question"] == "Test market?"
    assert len(data["outcomes"]) == 1
    assert data["outcomes"][0]["name"] == "Yes"
    assert data["outcomes"][0]["latest_price"] is not None


# ── CSV exports ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_signals_export_csv(client, engine):
    """GET /signals/export/csv returns text/csv."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        make_signal(session, market.id, outcome.id)
        await session.commit()

    resp = await client.get("/api/v1/signals/export/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2  # header + at least 1 data row
    assert "signal_type" in lines[0]


@pytest.mark.asyncio
async def test_markets_export_csv(client, engine):
    """GET /markets/export/csv returns text/csv."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        make_market(session)
        await session.commit()

    resp = await client.get("/api/v1/markets/export/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    lines = resp.text.strip().split("\n")
    assert len(lines) >= 2


# ── resolved_correctly filter ──────────────────────────


@pytest.mark.asyncio
async def test_signals_filter_resolved_correctly(client, engine):
    """Filter signals by resolved_correctly."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        now = datetime.now(timezone.utc)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        make_signal(session, market.id, outcome.id, signal_type="price_move",
                    dedupe_bucket=bucket, resolved_correctly=True)
        make_signal(session, market.id, outcome.id, signal_type="volume_spike",
                    dedupe_bucket=bucket - timedelta(minutes=15), resolved_correctly=False)
        make_signal(session, market.id, outcome.id, signal_type="spread_change",
                    dedupe_bucket=bucket - timedelta(minutes=30), resolved_correctly=None)
        await session.commit()

    # Filter for correct calls
    resp = await client.get("/api/v1/signals?resolved_correctly=true")
    data = resp.json()
    assert data["total"] == 1
    assert data["signals"][0]["signal_type"] == "price_move"
    assert data["signals"][0]["resolved_correctly"] is True

    # Filter for wrong calls
    resp = await client.get("/api/v1/signals?resolved_correctly=false")
    data = resp.json()
    assert data["total"] == 1
    assert data["signals"][0]["signal_type"] == "volume_spike"
    assert data["signals"][0]["resolved_correctly"] is False


@pytest.mark.asyncio
async def test_signal_detail_includes_resolved_correctly(client, engine):
    """GET /signals/{id} includes resolved_correctly."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        signal = make_signal(session, market.id, outcome.id, resolved_correctly=True)
        await session.commit()
        signal_id = str(signal.id)

    resp = await client.get(f"/api/v1/signals/{signal_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["resolved_correctly"] is True


@pytest.mark.asyncio
async def test_signals_csv_includes_resolved_correctly(client, engine):
    """CSV export includes resolved_correctly column."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        make_signal(session, market.id, outcome.id, resolved_correctly=True)
        await session.commit()

    resp = await client.get("/api/v1/signals/export/csv")
    assert resp.status_code == 200
    lines = resp.text.strip().split("\n")
    assert "resolved_correctly" in lines[0]


# ── Signal types endpoint ─────────────────────────────


@pytest.mark.asyncio
async def test_signal_types_empty(client):
    resp = await client.get("/api/v1/signals/types")
    assert resp.status_code == 200
    data = resp.json()
    assert data["types"] == []


@pytest.mark.asyncio
async def test_signal_types_returns_distinct(client, engine):
    """GET /signals/types returns distinct signal types."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        now = datetime.now(timezone.utc)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        make_signal(session, market.id, outcome.id, signal_type="price_move", dedupe_bucket=bucket)
        make_signal(session, market.id, outcome.id, signal_type="volume_spike", dedupe_bucket=bucket - timedelta(minutes=15))
        make_signal(session, market.id, outcome.id, signal_type="price_move", dedupe_bucket=bucket - timedelta(minutes=30))
        await session.commit()

    resp = await client.get("/api/v1/signals/types")
    data = resp.json()
    assert sorted(data["types"]) == ["price_move", "volume_spike"]


# ── Market platforms endpoint ──────────────────────────


@pytest.mark.asyncio
async def test_market_platforms_empty(client):
    resp = await client.get("/api/v1/markets/platforms")
    assert resp.status_code == 200
    data = resp.json()
    assert data["platforms"] == []


@pytest.mark.asyncio
async def test_market_platforms_returns_distinct(client, engine):
    """GET /markets/platforms returns distinct platforms."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        make_market(session, platform="polymarket")
        make_market(session, platform="kalshi")
        make_market(session, platform="polymarket", question="Another PM market?")
        await session.commit()

    resp = await client.get("/api/v1/markets/platforms")
    data = resp.json()
    assert sorted(data["platforms"]) == ["kalshi", "polymarket"]


# ── Analytics accuracy updated schema ──────────────────


@pytest.mark.asyncio
async def test_analytics_accuracy_schema(client, engine):
    """Signal accuracy endpoint returns both resolution and price-direction fields."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from app.models.signal import SignalEvaluation
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as session:
        market = make_market(session)
        await session.flush()
        outcome = make_outcome(session, market.id)
        await session.flush()
        signal = make_signal(session, market.id, outcome.id, resolved_correctly=True)
        await session.flush()
        ev = SignalEvaluation(
            id=uuid.uuid4(),
            signal_id=signal.id,
            horizon="15m",
            price_at_eval=Decimal("0.550"),
            price_change=Decimal("0.050"),
            price_change_pct=Decimal("10.00"),
        )
        session.add(ev)
        await session.commit()

    resp = await client.get("/api/v1/analytics/signal-accuracy")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["accuracy"]) >= 1
    row = data["accuracy"][0]
    # Ground-truth fields
    assert "accuracy_pct" in row
    assert "resolution_rate_pct" in row
    assert "resolved_count" in row
    assert "total_signals" in row
    # Price-direction field
    assert "price_direction_accuracy_pct" in row
    assert "avg_abs_change_pct" in row


@pytest.mark.asyncio
async def test_analytics_accuracy_days_filter(client, engine):
    """Signal accuracy endpoint supports days filter."""
    resp = await client.get("/api/v1/analytics/signal-accuracy?days=30")
    assert resp.status_code == 200
    data = resp.json()
    assert "accuracy" in data


# ── Root endpoint ───────────────────────────────────────


@pytest.mark.asyncio
async def test_root_endpoint(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Signal Market Terminal"
