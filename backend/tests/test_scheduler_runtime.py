import pytest


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
