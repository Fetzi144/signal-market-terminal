from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_text(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_oracle_micro_profile_keeps_heavy_polymarket_phases_off():
    content = _read_text("backend/.env.oracle-micro.example")
    assert "KALSHI_ENABLED=false" in content
    assert "POLYMARKET_STREAM_ENABLED=false" in content
    assert "POLYMARKET_RAW_STORAGE_ENABLED=false" in content
    assert "POLYMARKET_BOOK_RECON_ENABLED=false" in content
    assert "POLYMARKET_FEATURES_ENABLED=false" in content
    assert "POLYMARKET_STRUCTURE_ENGINE_ENABLED=false" in content
    assert "POLYMARKET_REPLAY_ENABLED=false" in content
    assert "POLYMARKET_USER_STREAM_ENABLED=false" in content
    assert "POLYMARKET_LIVE_TRADING_ENABLED=false" in content
    assert "POLYMARKET_PILOT_ENABLED=false" in content


def test_polymarket_capture_profile_enables_continuous_capture_but_keeps_research_and_live_off():
    content = _read_text("backend/.env.polymarket-capture.example")
    assert "KALSHI_ENABLED=false" in content
    assert "POLYMARKET_STREAM_ENABLED=true" in content
    assert "POLYMARKET_META_SYNC_ENABLED=true" in content
    assert "POLYMARKET_RAW_STORAGE_ENABLED=true" in content
    assert "POLYMARKET_TRADE_BACKFILL_ENABLED=true" in content
    assert "POLYMARKET_OI_POLL_ENABLED=true" in content
    assert "POLYMARKET_BOOK_RECON_ENABLED=true" in content
    assert "POLYMARKET_FEATURES_ENABLED=false" in content
    assert "POLYMARKET_REPLAY_ENABLED=false" in content
    assert "POLYMARKET_USER_STREAM_ENABLED=false" in content
    assert "POLYMARKET_PILOT_ENABLED=false" in content
    assert "POLYMARKET_LIVE_TRADING_ENABLED=false" in content


def test_capture_compose_uses_capture_env_file_and_stays_headless_first():
    content = _read_text("docker-compose.polymarket-capture.yml")
    assert "./backend/.env.polymarket-capture" in content
    assert 'profiles: ["api"]' in content
    assert "command: [\"python\", \"-m\", \"app.worker\"]" in content
    assert "frontend:" not in content


def test_prod_compose_runs_migrations_once():
    content = _read_text("docker-compose.prod.yml")
    assert content.count("RUN_MIGRATIONS=true") == 1
    assert content.count("RUN_MIGRATIONS=false") == 1


def test_dev_compose_runs_migrations_once():
    content = _read_text("docker-compose.yml")
    assert content.count("RUN_MIGRATIONS=true") == 1
    assert content.count("RUN_MIGRATIONS=false") == 1
    assert "alembic upgrade head" not in content


def test_oracle_micro_compose_runs_migrations_once():
    content = _read_text("docker-compose.oracle-micro.yml")
    assert content.count('RUN_MIGRATIONS: "true"') == 1
    assert content.count('RUN_MIGRATIONS: "false"') == 1


def test_polymarket_capture_compose_runs_migrations_once():
    content = _read_text("docker-compose.polymarket-capture.yml")
    assert content.count('RUN_MIGRATIONS: "true"') == 1
    assert content.count('RUN_MIGRATIONS: "false"') == 1


def test_backend_entrypoint_honors_run_migrations_flag():
    content = _read_text("backend/entrypoint.sh")
    assert 'RUN_MIGRATIONS:-true' in content
    assert "Skipping database migrations" in content
