# Hetzner Production Ops Runbook

This is the day-to-day operator runbook for the canonical production host:

- host: `smt-prod-1`
- checkout: `/opt/signal-market-terminal`
- production compose file: `docker-compose.prod.yml`
- public entrypoint: frontend/nginx on host port `80` unless `FRONTEND_PORT` overrides it

Run commands from the host unless a command explicitly says it runs inside a
container.

## Access And Posture

```bash
ssh smt-prod-1
cd /opt/signal-market-terminal
```

On the local Windows workstation, the repo helper uses the existing private key
without storing it in the repository:

```powershell
.\scripts\ssh-prod.ps1
.\scripts\ssh-prod.ps1 "cd /opt/signal-market-terminal && docker compose -f docker-compose.prod.yml ps"
```

By default it connects to the SSH host alias `smt-prod-1` using
`$HOME\.ssh\hetzner_smt_ed25519_clean`. Configure that alias locally, or
override with `SMT_PROD_SSH_HOST`, `SMT_PROD_SSH_USER`, or `SMT_PROD_SSH_KEY`
if needed.

Production is the full stack:

- `db`: PostgreSQL 16, persistent `pgdata` volume
- `backend`: FastAPI/Gunicorn, `RUN_MIGRATIONS=true`, `SCHEDULER_ENABLED=false`
- `worker`: scheduler and background services, `RUN_MIGRATIONS=false`, `SCHEDULER_ENABLED=true`
- `frontend`: nginx serving the React app and proxying `/api/` plus `/metrics`

`docker-compose.prod.yml` does not define Compose profiles. For production,
the active profile set is therefore empty, and `docker compose -f
docker-compose.prod.yml up -d` starts all four services.

Confirm this with:

```bash
docker compose -f docker-compose.prod.yml config --profiles
```

Blank output is expected for the production compose file.

The smaller headless files are separate profiles, not the canonical production
stack:

- `docker-compose.polymarket-capture.yml` uses optional profile `api`
- `docker-compose.oracle-micro.yml` uses optional profile `api`

## Deploy

Use a fast-forward pull or an explicitly reviewed revision.

```bash
git fetch --all --prune
git status --short --branch
git pull --ff-only
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d --remove-orphans
docker compose -f docker-compose.prod.yml ps
```

The backend container runs Alembic migrations on startup. If the backend fails
to become healthy, stop and inspect backend and db logs before restarting the
worker repeatedly.

Check the applied migration:

```bash
docker compose -f docker-compose.prod.yml exec -T backend alembic current
docker compose -f docker-compose.prod.yml exec -T db psql -U smt -d smt -tAc 'select version_num from alembic_version;'
```

## Restart

Restart one service when possible:

```bash
docker compose -f docker-compose.prod.yml restart backend
docker compose -f docker-compose.prod.yml restart worker
docker compose -f docker-compose.prod.yml restart frontend
```

For a full app restart without dropping the database volume:

```bash
docker compose -f docker-compose.prod.yml up -d --force-recreate backend worker frontend
docker compose -f docker-compose.prod.yml ps
```

Avoid `down -v` on production. That deletes the Postgres volume.

## Health Checks

Basic service shape:

```bash
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml exec -T db pg_isready -U smt -d smt
curl -fsS http://127.0.0.1/api/v1/health | jq .
curl -fsS http://127.0.0.1/api/v1/health/summary | jq .
```

If `API_KEY` is enabled, add `-H "x-api-key: $API_KEY"` to read-only API calls
other than `/api/v1/health`.

Key health fields:

- `status` should be `ok`; `degraded` means one or more runtime invariants are failing.
- `scheduler_lease.heartbeat_freshness_seconds` should stay near the worker lease cadence.
- `runtime_invariants` should have no `failing` rows.
- `default_strategy_runtime.overdue_open_trades` should be `0`.
- `polymarket_phase12.live_trading_enabled`, pilot state, approval count, and kill switch should match the intended fail-closed posture.

## Metrics Checks

Backend metrics are proxied by nginx:

```bash
curl -fsS http://127.0.0.1/metrics | grep -E 'smt_|http_'
```

Worker metrics listen inside the worker container on `WORKER_METRICS_PORT`
(`9101` by default):

```bash
docker compose -f docker-compose.prod.yml exec -T worker python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:9101/metrics', timeout=10).read().decode(), end='')"
```

Important metrics to spot-check:

- `smt_default_strategy_scheduler_no_active_run_total`
- `smt_default_strategy_pending_decision_count`
- `smt_default_strategy_pending_decision_max_age_seconds`
- `smt_default_strategy_latest_review_age_seconds`
- `smt_default_strategy_review_outdated`
- `smt_polymarket_stream_connected`
- `smt_polymarket_raw_projector_lag`
- `smt_polymarket_book_recon_assets_degraded`
- `smt_polymarket_replay_coverage_limited_runs`

Host resource snapshots are collected by `scripts/log_server_metrics.py` when
the cron entry from `scripts/smt-monitor.cron` is installed.

```bash
sudo tail -n 50 /var/log/smt-monitor/cron.log
sudo tail -n 5 /var/log/smt-monitor/resource-$(date -u +%F).jsonl
```

## Logs

Application logs are container stdout/stderr:

```bash
docker compose -f docker-compose.prod.yml logs --tail=200 backend
docker compose -f docker-compose.prod.yml logs --tail=200 worker
docker compose -f docker-compose.prod.yml logs --tail=200 frontend
docker compose -f docker-compose.prod.yml logs --tail=100 db
```

Follow only the service you are investigating:

```bash
docker compose -f docker-compose.prod.yml logs -f --tail=100 worker
```

Find Docker's JSON log path for a container:

```bash
docker inspect --format '{{.LogPath}}' "$(docker compose -f docker-compose.prod.yml ps -q worker)"
```

Host-side monitor logs:

- `/var/log/smt-monitor/cron.log`
- `/var/log/smt-monitor/resource-YYYY-MM-DD.jsonl`

Default-strategy evidence artifacts should be checked under the host checkout:

- `/opt/signal-market-terminal/docs/evidence/default-strategy-operator-log.md`
- `/opt/signal-market-terminal/docs/strategy-reviews/`
- `/opt/signal-market-terminal/docs/profitability-snapshots/`

## Backup Checks

This repo does not currently ship a committed backup service or restore
automation. Do not mark the backup check as passing unless the host has either
a current provider snapshot or a current database dump artifact.

Daily minimum check:

```bash
sudo find /var/backups/smt -type f -mtime -1 -size +1M -print 2>/dev/null || true
docker compose -f docker-compose.prod.yml exec -T db pg_dump -U smt -d smt --schema-only >/tmp/smt-schema-backup-check.sql
test -s /tmp/smt-schema-backup-check.sql && rm /tmp/smt-schema-backup-check.sql
```

Also identify the live database mount before any maintenance:

```bash
docker inspect "$(docker compose -f docker-compose.prod.yml ps -q db)" --format '{{range .Mounts}}{{println .Name .Destination}}{{end}}'
```

If `/var/backups/smt` is absent or stale, record the backup check as failing and
do not perform destructive maintenance. `docker compose down -v`, manual volume
removal, or host rebuilds require a verified restore path first.

## Capture Continuity Checks

Use the combined Polymarket ingest status for stream, raw storage, and book
reconstruction continuity:

```bash
curl -fsS http://127.0.0.1/api/v1/ingest/polymarket/status | jq '{
  stream: {
    enabled,
    connected,
    continuity_status,
    last_event_received_at,
    heartbeat_freshness_seconds,
    watched_asset_count,
    subscribed_asset_count,
    reconnect_count,
    resync_count,
    gap_suspected_count
  },
  raw_storage,
  book_reconstruction
}'
```

Pass conditions for the production truth path:

- `continuity_status` is `healthy` or intentionally `disabled`.
- `connected` is `true` when `polymarket_phase1.enabled` is true.
- `heartbeat_freshness_seconds` is recent relative to the configured stream cadence.
- `gap_suspected_count` and `malformed_message_count` are not climbing unexpectedly.
- `raw_storage.projector_lag` is near `0`.
- `book_snapshot_freshness_seconds`, `trade_backfill_freshness_seconds`, and `oi_poll_freshness_seconds` are within their configured intervals plus a small grace window.
- `book_reconstruction.stale_asset_count`, `degraded_asset_count`, and unresolved incident counts are not growing.

Use the Health frontend page for a visual read, but use the JSON payloads above
as the source of truth for incident notes.

## Default-Strategy Bootstrap And Evidence

Inspect current run state without mutation:

```bash
curl -fsS http://127.0.0.1/api/v1/paper-trading/default-strategy/run | jq .
curl -fsS http://127.0.0.1/api/v1/paper-trading/strategy-health | jq .
```

Record an evidence boundary:

```bash
COMMIT_SHA="$(git rev-parse HEAD)"
MIGRATION_REVISION="$(docker compose -f docker-compose.prod.yml exec -T backend alembic current | awk '{print $1}')"
docker compose -f docker-compose.prod.yml exec -T backend python -m app.ops.default_strategy_evidence record-boundary \
  --log-path /docs/evidence/default-strategy-operator-log.md \
  --evidence-boundary-id v0.4.1 \
  --release-tag v0.4.1 \
  --commit-sha "$COMMIT_SHA" \
  --migration-revision "$MIGRATION_REVISION" \
  --contract-version default_strategy_v0.4.1 \
  --use-balanced-gate
```

Retire a pre-boundary active run:

```bash
docker compose -f docker-compose.prod.yml exec -T backend python -m app.ops.default_strategy_evidence retire-active-run \
  --log-path /docs/evidence/default-strategy-operator-log.md \
  --note "Pre-boundary run retired before valid evidence bootstrap."
```

Bootstrap the first valid run only after health and migration checks pass:

```bash
CUTOVER_TIMESTAMP="2026-04-15T12:00:00Z"
COMMIT_SHA="$(git rev-parse HEAD)"
MIGRATION_REVISION="$(docker compose -f docker-compose.prod.yml exec -T backend alembic current | awk '{print $1}')"
docker compose -f docker-compose.prod.yml exec -T backend python -m app.ops.default_strategy_evidence bootstrap-run \
  --launch-boundary-at "$CUTOVER_TIMESTAMP" \
  --log-path /docs/evidence/default-strategy-operator-log.md \
  --evidence-boundary-id v0.4.1 \
  --release-tag v0.4.1 \
  --commit-sha "$COMMIT_SHA" \
  --migration-revision "$MIGRATION_REVISION" \
  --contract-version default_strategy_v0.4.1 \
  --use-balanced-gate
```

Daily pending-decision watch:

```bash
docker compose -f docker-compose.prod.yml exec -T backend python -m app.ops.default_strategy_evidence pending-watch --stale-after-seconds 900
```

Read-only evidence smoke and artifact generation:

```bash
docker compose -f docker-compose.prod.yml exec -T backend python -m app.reports smoke --base-url http://localhost:8000
docker compose -f docker-compose.prod.yml exec -T backend python -m app.reports profitability-snapshot
docker compose -f docker-compose.prod.yml exec -T backend python -m app.reports review
```

The report commands print their artifact paths. Before rebuilding or replacing
a container, verify the generated files are present under the host checkout's
`docs/` tree. The operator log commands above explicitly write through the
production `/docs` mount.

For the full controlled relaunch procedure, use
`docs/runbooks/default-strategy-controlled-evidence-relaunch.md`.

## Daily Operator Checklist

1. Confirm host and checkout:
   `hostname` should identify `smt-prod-1`, and `pwd` should be `/opt/signal-market-terminal`.
2. Confirm service shape:
   `docker compose -f docker-compose.prod.yml ps` shows `db`, `backend`, `worker`, and `frontend` running or healthy.
3. Check `/api/v1/health` and `/api/v1/health/summary`; no runtime invariant should be `failing`.
4. Check `/metrics` and worker `:9101/metrics`; default-strategy and capture metrics should be present.
5. Review `/var/log/smt-monitor/cron.log` and today's resource JSONL sample.
6. Inspect worker logs for scheduler ownership, stream reconnect loops, migration errors, or repeated exceptions.
7. Run capture-continuity JSON checks; record any stale, degraded, or gap-suspected state.
8. Run `pending-watch --stale-after-seconds 900`; stale pending decisions block clean evidence.
9. Run the read-only evidence smoke command.
10. Generate the daily profitability snapshot and verify it exists in the host `docs/profitability-snapshots/` directory.
11. Check the latest review artifact freshness through `strategy-health.evidence_freshness`.
12. Verify a current backup artifact or provider snapshot exists. If not, mark backup as failing.
13. Confirm live and pilot posture remains intended: live trading disabled by default, pilot disabled unless explicitly supervised, manual approval required, kill switch state known.
14. Record incidents, degraded checks, and evidence notes in the operator log or the current handoff.
