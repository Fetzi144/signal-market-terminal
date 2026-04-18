# Polymarket Capture Runbook

This runbook is for the always-on `polymarket-capture` profile: a small but real headless box that prioritizes continuity and raw truth over UI hosting.

Target shape:

- at least `2 vCPU`
- at least `4 GB RAM`
- PostgreSQL local to the box
- scheduler worker always on
- optional FastAPI process for health and read-only inspection

## What This Profile Is For

- real-time Polymarket websocket continuity
- metadata sync and raw event projection
- trade backfill and open-interest polling
- book reconstruction and resync visibility
- continuity-first health checks for stream freshness, lag, and replay readiness

## What Stays Off Initially

- frontend / nginx
- Kalshi ingestion
- feature generation
- structure engine
- replay simulator
- user stream
- pilot and live order submission

This profile is meant to prove capture continuity first. Research and execution layers turn on only after the box can keep up without blind spots.

## Setup

1. Copy [backend/.env.polymarket-capture.example](</C:/Code/Signal Market Terminal/backend/.env.polymarket-capture.example>) to `backend/.env.polymarket-capture`.
2. Add alert credentials if you want notifications.
3. Start the headless capture stack:

```bash
docker compose -f docker-compose.polymarket-capture.yml up -d db worker
```

4. If you want the API for inspection and health dashboards, start the optional profile:

```bash
docker compose -f docker-compose.polymarket-capture.yml --profile api up -d backend
```

## Operator Expectations

- `worker` owns migrations and APScheduler jobs.
- `backend` stays optional and must keep `SCHEDULER_ENABLED=false`.
- Health surfaces should show websocket continuity, heartbeat freshness, raw projector lag, recon stale counts, and replay coverage posture.
- Features, replay, and live routing remain off until you have a clean capture streak.

## Promotion Gates

1. Seven consecutive days of continuous capture with no unresolved stream or reconstruction blind spots.
2. Only then enable feature materialization and replay on this profile.
3. Only after replay-adjusted evidence is positive should structure or maker move toward paper-routing and later pilot work.
