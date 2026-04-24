import asyncio

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class _FakeScheduler:
    def __init__(self):
        self.running = False
        self.jobs = []

    def remove_all_jobs(self):
        self.jobs = []

    def add_job(self, _func, _trigger, id, replace_existing, args, **_kwargs):
        self.jobs = [job for job in self.jobs if job["id"] != id]
        self.jobs.append({"id": id, "args": args, "replace_existing": replace_existing})

    def start(self):
        self.running = True

    def shutdown(self, wait=False):
        self.running = False

    def get_jobs(self):
        return list(self.jobs)


@pytest.mark.asyncio
async def test_lifespan_skips_scheduler_when_disabled(monkeypatch):
    from app import main as main_module

    calls = {"start": 0, "stop": 0}

    monkeypatch.setattr(main_module.settings, "scheduler_enabled", False)
    monkeypatch.setattr(main_module, "start_scheduler", lambda: calls.__setitem__("start", calls["start"] + 1))
    monkeypatch.setattr(main_module, "stop_scheduler", lambda: calls.__setitem__("stop", calls["stop"] + 1))

    async with main_module.lifespan(main_module.app):
        pass

    assert calls["start"] == 0
    assert calls["stop"] == 0


@pytest.mark.asyncio
async def test_lifespan_starts_scheduler_when_enabled(monkeypatch):
    from app import main as main_module

    calls = {"start": 0, "stop": 0}

    monkeypatch.setattr(main_module.settings, "scheduler_enabled", True)
    monkeypatch.setattr(main_module, "start_scheduler", lambda: calls.__setitem__("start", calls["start"] + 1))
    monkeypatch.setattr(main_module, "stop_scheduler", lambda: calls.__setitem__("stop", calls["stop"] + 1))

    async with main_module.lifespan(main_module.app):
        pass

    assert calls["start"] == 1
    assert calls["stop"] == 1


@pytest.mark.asyncio
async def test_scheduler_lease_allows_single_owner(engine, monkeypatch):
    from app.jobs import scheduler as scheduler_module

    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(scheduler_module, "async_session", async_sess)
    monkeypatch.setattr(scheduler_module.settings, "scheduler_lease_seconds", 60)
    monkeypatch.setattr(scheduler_module, "_scheduler_owner_token", None)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_task", None)

    assert await scheduler_module._acquire_scheduler_ownership("owner-a") is True
    assert await scheduler_module._acquire_scheduler_ownership("owner-b") is False
    assert await scheduler_module._renew_scheduler_ownership("owner-a") is True
    assert await scheduler_module._release_scheduler_ownership("owner-a") is True
    assert await scheduler_module._acquire_scheduler_ownership("owner-b") is True
    assert await scheduler_module._release_scheduler_ownership("owner-b") is True


@pytest.mark.asyncio
async def test_start_scheduler_skips_when_ownership_is_held(monkeypatch):
    from app.jobs import scheduler as scheduler_module

    fake_scheduler = _FakeScheduler()

    async def deny(_owner_token):
        return False

    monkeypatch.setattr(scheduler_module, "scheduler", fake_scheduler)
    monkeypatch.setattr(scheduler_module, "_acquire_scheduler_ownership", deny)
    monkeypatch.setattr(scheduler_module, "_scheduler_owner_token", None)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_task", None)

    started = await scheduler_module.start_scheduler()

    assert started is False
    assert fake_scheduler.running is False
    assert fake_scheduler.get_jobs() == []


@pytest.mark.asyncio
async def test_start_scheduler_releases_ownership_on_stop(monkeypatch):
    from app.jobs import scheduler as scheduler_module

    fake_scheduler = _FakeScheduler()
    calls = {"released": 0}

    async def acquire(_owner_token):
        return True

    async def release(owner_token):
        calls["released"] += 1
        calls["owner_token"] = owner_token
        return True

    async def heartbeat(_owner_token):
        await asyncio.sleep(3600)

    monkeypatch.setattr(scheduler_module, "scheduler", fake_scheduler)
    monkeypatch.setattr(scheduler_module, "_build_scheduler_owner_token", lambda: "owner-fixed")
    monkeypatch.setattr(scheduler_module, "_acquire_scheduler_ownership", acquire)
    monkeypatch.setattr(scheduler_module, "_release_scheduler_ownership", release)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_heartbeat", heartbeat)
    monkeypatch.setattr(scheduler_module, "_scheduler_owner_token", None)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_task", None)

    started = await scheduler_module.start_scheduler()
    assert started is True
    assert fake_scheduler.running is True
    assert scheduler_module._scheduler_owner_token == "owner-fixed"

    await scheduler_module.stop_scheduler()
    await asyncio.sleep(0)

    assert fake_scheduler.running is False
    assert scheduler_module._scheduler_owner_token is None
    assert calls["released"] == 1
    assert calls["owner_token"] == "owner-fixed"


@pytest.mark.asyncio
async def test_start_scheduler_registers_review_generation_only_when_enabled(monkeypatch):
    from app.jobs import scheduler as scheduler_module

    fake_scheduler = _FakeScheduler()

    async def acquire(_owner_token):
        return True

    async def heartbeat(_owner_token):
        await asyncio.sleep(3600)

    async def release(_owner_token):
        return True

    monkeypatch.setattr(scheduler_module, "scheduler", fake_scheduler)
    monkeypatch.setattr(scheduler_module, "_build_scheduler_owner_token", lambda: "owner-fixed")
    monkeypatch.setattr(scheduler_module, "_acquire_scheduler_ownership", acquire)
    monkeypatch.setattr(scheduler_module, "_release_scheduler_ownership", release)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_heartbeat", heartbeat)
    monkeypatch.setattr(scheduler_module, "_scheduler_owner_token", None)
    monkeypatch.setattr(scheduler_module, "_scheduler_lease_task", None)
    monkeypatch.setattr(scheduler_module.settings, "default_strategy_review_auto_generate_enabled", True)

    assert await scheduler_module.start_scheduler() is True

    job_ids = {job["id"] for job in fake_scheduler.get_jobs()}
    assert "default_strategy_review_generation" in job_ids

    await scheduler_module.stop_scheduler()


@pytest.mark.asyncio
async def test_scheduler_supervisor_retries_until_lease_is_available(monkeypatch):
    from app import worker as worker_module

    fake_scheduler = _FakeScheduler()
    attempts = {"count": 0}

    async def fake_start_scheduler():
        attempts["count"] += 1
        if attempts["count"] >= 3:
            fake_scheduler.running = True
            return True
        return False

    monkeypatch.setattr(worker_module, "scheduler_runtime", fake_scheduler)
    monkeypatch.setattr(worker_module, "start_scheduler", fake_start_scheduler)
    monkeypatch.setattr(worker_module, "_scheduler_supervisor_retry_seconds", lambda: 0.01)

    stop_event = asyncio.Event()
    task = asyncio.create_task(worker_module._run_scheduler_supervisor(stop_event))

    try:
        for _ in range(100):
            if fake_scheduler.running:
                break
            await asyncio.sleep(0.01)
        assert fake_scheduler.running is True
        assert attempts["count"] == 3
    finally:
        stop_event.set()
        await task


@pytest.mark.asyncio
async def test_scheduler_supervisor_retries_after_local_scheduler_stops(monkeypatch):
    from app import worker as worker_module

    fake_scheduler = _FakeScheduler()
    attempts = {"count": 0}

    async def fake_start_scheduler():
        attempts["count"] += 1
        fake_scheduler.running = True
        return True

    monkeypatch.setattr(worker_module, "scheduler_runtime", fake_scheduler)
    monkeypatch.setattr(worker_module, "start_scheduler", fake_start_scheduler)
    monkeypatch.setattr(worker_module, "_scheduler_supervisor_retry_seconds", lambda: 0.01)

    stop_event = asyncio.Event()
    task = asyncio.create_task(worker_module._run_scheduler_supervisor(stop_event))

    try:
        for _ in range(100):
            if attempts["count"] >= 1:
                break
            await asyncio.sleep(0.01)
        fake_scheduler.running = False
        for _ in range(100):
            if attempts["count"] >= 2:
                break
            await asyncio.sleep(0.01)
        assert attempts["count"] >= 2
        assert fake_scheduler.running is True
    finally:
        stop_event.set()
        await task


@pytest.mark.asyncio
async def test_run_evaluation_records_error_run_when_horizons_fail(engine, monkeypatch):
    from app.evaluation import evaluator as evaluator_module
    from app.jobs import scheduler as scheduler_module
    from app.models.ingestion import IngestionRun

    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(scheduler_module, "async_session", async_sess)

    async def fake_evaluate(session):
        session.sync_session.info["signal_evaluation_stats"] = {
            "created": 1,
            "failed": 2,
        }
        return 1

    monkeypatch.setattr(evaluator_module, "evaluate_signals", fake_evaluate)

    await scheduler_module._run_evaluation()

    async with async_sess() as session:
        result = await session.execute(
            select(IngestionRun)
            .where(IngestionRun.run_type == "evaluation")
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        run = result.scalar_one()

    assert run.platform == "system"
    assert run.status == "error"
    assert run.markets_processed == 1
    assert "2 signal evaluation horizon(s) failed" in (run.error or "")
    assert run.finished_at is not None
