# Phase 1 Closeout

## What Changed

- added a dedicated public Polymarket market-data worker that runs beside the existing polling scheduler instead of replacing it
- persisted raw market-data events append-only with non-null `received_at_local`, preserved exchange `event_time` when present, and kept raw payloads structurally lossless for replay
- made provenance explicit so public stream events and REST resync snapshots remain distinguishable
- reconciled watched Polymarket assets dynamically and resubscribed cleanly after reconnects with bounded backoff and resync support
- exposed operator-facing health and manual resync surfaces so stream state, ingest cadence, and recovery behavior are visible

## Core Guarantees

- exchange time stays the source of truth when Polymarket provides it
- raw payloads remain append-only and are not collapsed into only derived snapshots
- every persisted raw event includes `received_at_local`
- the legacy polling and snapshot path remains available during this phase

## Intentionally Untouched

- deterministic L2 reconstruction and gap-perfect book replay
- full trade tape, open-interest history, and microstructure feature generation
- OMS/EMS, live order placement, and executable-EV policy beyond data ingestion
- structural edge, maker economics, and graph-based portfolio risk

## What Phase 2 Starts Next

- store Polymarket execution metadata such as tick size, min order size, fee-enabled flags, and negative-risk flags
- enrich the market and event registry from Gamma plus stream lifecycle messages
- prepare the structural registry foundation that later execution and strategy phases depend on
