# Oracle Free Micro Runbook

This runbook is for `VM.Standard.E2.1.Micro` or any similarly tiny box with about `1 GB RAM`.

It is intentionally not the full production stack. The goal is a small always-on scanner that keeps discovery, snapshots, signal detection, evaluation, and alert delivery alive without the React frontend.

If you want continuity-grade Polymarket capture, use [docs/runbooks/polymarket-capture.md](<C:/Code/Signal Market Terminal/docs/runbooks/polymarket-capture.md>) instead. The micro profile is for cheap scanning, not research truth.

## What Fits

- PostgreSQL
- Scheduler worker
- Optional lightweight FastAPI process for health checks and read-only API access

## What Stays Off

- Frontend / nginx
- Polymarket live stream
- Raw storage and book reconstruction
- Feature generation, structure engine, risk graph, replay, and live trading
- Whale tracking
- Kalshi by default

## Why This Shape

The regular production deployment targets more headroom than a `1 GB` instance provides. The micro profile keeps the scanner useful by prioritizing:

- periodic market discovery
- periodic price and orderbook snapshots
- detector runs
- evaluation and alerting

This is the best fit when the VM is acting like a headless overnight watcher rather than a full-time dashboard host.

## Setup

1. Copy [backend/.env.oracle-micro.example](</C:/Code/Signal Market Terminal/backend/.env.oracle-micro.example>) to `backend/.env.oracle-micro`.
2. Fill in any alert secrets you want to use.
3. Start the headless scanner:

```bash
docker compose -f docker-compose.oracle-micro.yml up -d db worker
```

4. If you also want the API for health and inspection, start the optional profile:

```bash
docker compose -f docker-compose.oracle-micro.yml --profile api up -d backend
```

## What To Expect

- `worker` runs migrations on startup, then owns the APScheduler jobs.
- `backend` is optional and should keep `SCHEDULER_ENABLED=false`.
- Alerts can still fire through logger, webhook, Telegram, or Discord.
- The frontend is intentionally omitted to save memory.

## Good Defaults To Keep

- `SNAPSHOT_INTERVAL_SECONDS=300`
- `MARKET_DISCOVERY_INTERVAL_SECONDS=900`
- `EVALUATION_INTERVAL_SECONDS=900`
- `ORDERBOOK_SAMPLE_SIZE=8`
- `MIN_VOLUME_24H=10000`

If the box still feels tight, the first things to reduce further are `ORDERBOOK_SAMPLE_SIZE`, retention windows, and discovery frequency.
