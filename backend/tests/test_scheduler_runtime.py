import asyncio

import pytest
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
