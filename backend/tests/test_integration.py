"""Integration tests: full pipeline from snapshot data to signal detection, evaluation, resolution, and API."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db
from app.ingestion.resolution import resolve_signals
from app.models.signal import Signal, SignalEvaluation
from app.ranking.scorer import persist_signals
from app.signals.arbitrage import ArbitrageDetector
from app.signals.price_move import PriceMoveDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_full_pipeline_snapshot_to_signal(session):
    """End-to-end: create snapshots -> detect signal -> persist with rank_score."""
    market = make_market(session, question="Will ETH hit $5000?")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)

    # Create price snapshots showing a significant move
    make_price_snapshot(session, outcome.id, "0.40", captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, outcome.id, "0.42", captured_at=now - timedelta(minutes=15))
    make_price_snapshot(session, outcome.id, "0.55", captured_at=now - timedelta(minutes=2))
    await session.commit()

    # Step 1: Detect
    detector = PriceMoveDetector()
    candidates = await detector.detect(session)
    assert len(candidates) >= 1

    candidate = candidates[0]
    assert candidate.signal_type == "price_move"
    assert float(candidate.signal_score) > 0
    assert float(candidate.confidence) > 0

    # Step 2: Persist
    created, new_signals = await persist_signals(session, candidates)
    assert created >= 1
    assert len(new_signals) == created

    # Step 3: Verify in DB
    result = await session.execute(
        select(Signal).where(Signal.market_id == market.id)
    )
    signals = result.scalars().all()
    assert len(signals) >= 1

    sig = signals[0]
    assert sig.signal_type == "price_move"
    assert sig.rank_score > 0
    assert sig.dedupe_bucket is not None
    assert sig.details["market_question"] == "Will ETH hit $5000?"


@pytest.mark.asyncio
async def test_dedupe_prevents_duplicate_signals(session):
    """persist_signals should not create duplicate signals in the same 15-min bucket."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    make_price_snapshot(session, outcome.id, "0.40", captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, outcome.id, "0.55", captured_at=now - timedelta(minutes=2))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)
    assert len(candidates) >= 1

    # Persist once
    created1, new_signals1 = await persist_signals(session, candidates)
    assert created1 >= 1
    assert len(new_signals1) == created1

    # Persist same candidates again — should be deduped
    created2, new_signals2 = await persist_signals(session, candidates)
    assert created2 == 0
    assert new_signals2 == []

    # Same number of signals as first persist (multi-timeframe may produce >1, but no duplicates)
    result = await session.execute(select(Signal).where(Signal.market_id == market.id))
    assert len(result.scalars().all()) == created1


@pytest.mark.asyncio
async def test_full_pipeline_with_api_evaluation_and_resolution(session, engine):
    """Full end-to-end: detect -> persist -> API list -> API detail -> evaluate -> resolve."""
    now = datetime.now(timezone.utc)

    # 1. Create market + outcomes
    market = make_market(session, question="Will BTC reach $100k?", platform="polymarket")
    await session.flush()
    yes_outcome = make_outcome(session, market.id, name="Yes")
    make_outcome(session, market.id, name="No")
    await session.flush()

    # 2. Insert price snapshots that trigger a price move signal
    make_price_snapshot(session, yes_outcome.id, "0.30", captured_at=now - timedelta(minutes=28))
    make_price_snapshot(session, yes_outcome.id, "0.32", captured_at=now - timedelta(minutes=15))
    make_price_snapshot(session, yes_outcome.id, "0.50", captured_at=now - timedelta(minutes=1))
    await session.commit()

    # 3. Run detect_and_persist_signals (detect + persist)
    detector = PriceMoveDetector()
    candidates = await detector.detect(session)
    assert len(candidates) >= 1

    created, new_signals = await persist_signals(session, candidates)
    assert created >= 1

    signal = new_signals[0]
    assert signal.rank_score > 0
    signal_id = signal.id

    # 4. Verify signal_score computation: price moved from ~0.30 to ~0.50 (~66% change)
    assert float(signal.signal_score) > 0.3

    # 5. Call GET /api/v1/signals and assert the signal appears
    from app.main import app
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/signals")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        signal_ids = [s["id"] for s in data["signals"]]
        assert str(signal_id) in signal_ids

        # 6. Call GET /api/v1/signals/{id} — evaluations empty before evaluator runs
        resp = await client.get(f"/api/v1/signals/{signal_id}")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["signal_type"] == "price_move"
        assert detail["resolved_correctly"] is None
        assert detail["market_question"] == "Will BTC reach $100k?"

        # 7. Run evaluator: create snapshots at evaluation horizons
        # We need a snapshot at fired_at + 15m for the 15m horizon
        fired_at = signal.fired_at
        if fired_at.tzinfo is None:
            fired_at = fired_at.replace(tzinfo=timezone.utc)

        async with async_sess() as eval_sess:
            make_price_snapshot(
                eval_sess, yes_outcome.id, "0.55",
                captured_at=fired_at + timedelta(minutes=15),
            )
            await eval_sess.commit()

            # Evaluate with time shifted forward so 15m horizon is ready
            # We manually create the evaluation since evaluate_signals checks current time
            eval_obj = SignalEvaluation(
                id=uuid.uuid4(),
                signal_id=signal_id,
                horizon="15m",
                price_at_eval=Decimal("0.550000"),
                price_change=Decimal("0.550000") - signal.price_at_fire,
                price_change_pct=((Decimal("0.550000") - signal.price_at_fire) / signal.price_at_fire * 100).quantize(Decimal("0.01")),
                evaluated_at=now,
            )
            eval_sess.add(eval_obj)
            await eval_sess.commit()

        # Verify evaluation via API
        resp = await client.get(f"/api/v1/signals/{signal_id}")
        assert resp.status_code == 200
        detail = resp.json()
        assert len(detail["evaluations"]) >= 1
        eval_data = detail["evaluations"][0]
        assert eval_data["horizon"] == "15m"
        assert float(eval_data["price_at_eval"]) == 0.55

        # 8. Insert a resolved market outcome and run the resolution service
        async with async_sess() as res_sess:
            resolved_markets = [{"platform_id": market.platform_id, "winner": "Yes"}]
            resolved_count = await resolve_signals(res_sess, "polymarket", resolved_markets)
            assert resolved_count >= 1

        # 9. Verify resolved_correctly is set on the signal
        resp = await client.get(f"/api/v1/signals/{signal_id}")
        assert resp.status_code == 200
        detail = resp.json()
        # Signal predicted "up" on "Yes" outcome, and "Yes" won → resolved_correctly = True
        assert detail["resolved_correctly"] is True

        # 10. Call GET /api/v1/health and assert all ingestion fields present
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        health = resp.json()
        assert health["status"] == "ok"
        assert "active_markets" in health
        assert "total_signals" in health
        assert "unresolved_signals" in health
        assert "recent_alerts_24h" in health
        assert "ingestion" in health
        assert health["active_markets"] >= 1
        assert health["total_signals"] >= 1

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_arbitrage_integration(session):
    """Create same question on two platforms with different prices, detect arbitrage signal."""
    now = datetime.now(timezone.utc)
    question = "Will the Fed cut rates in June?"
    slug = "will the fed cut rates in june"

    # Platform 1: Polymarket — YES at 0.60
    pm_market = make_market(
        session,
        question=question,
        question_slug=slug,
        platform="polymarket",
        platform_id="pm-fed-rates",
    )
    await session.flush()
    pm_yes = make_outcome(session, pm_market.id, name="Yes")
    await session.flush()
    make_price_snapshot(session, pm_yes.id, "0.60", captured_at=now - timedelta(minutes=1))

    # Platform 2: Kalshi — YES at 0.50 (10-point spread, above 4% threshold)
    ka_market = make_market(
        session,
        question=question,
        question_slug=slug,
        platform="kalshi",
        platform_id="ka-fed-rates",
    )
    await session.flush()
    ka_yes = make_outcome(session, ka_market.id, name="Yes")
    await session.flush()
    make_price_snapshot(session, ka_yes.id, "0.50", captured_at=now - timedelta(minutes=1))
    await session.commit()

    # Run ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    arb = candidates[0]
    assert arb.signal_type == "arbitrage"

    # Spread is 0.10, score = 0.10 / 0.15 ≈ 0.667
    assert float(arb.signal_score) > 0.5
    assert float(arb.confidence) == 1.0
    assert arb.details["spread_pct"] == "10.00"
    assert arb.details["question_slug"] == slug

    # The cheaper platform (kalshi at 0.50) should be the buy side
    assert arb.details["buy_platform"] == "kalshi"
    assert arb.details["sell_platform"] == "polymarket"

    # Persist and verify
    created, new_signals = await persist_signals(session, candidates)
    assert created >= 1

    sig = new_signals[0]
    assert sig.signal_type == "arbitrage"
    assert sig.rank_score > 0
    assert "0.50" in sig.details["kalshi_price"]
    assert "0.60" in sig.details["polymarket_price"]
