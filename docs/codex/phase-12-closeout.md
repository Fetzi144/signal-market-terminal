# Phase 12 Closeout

## Summary

Phase 12 adds a narrow, supervised Polymarket live-pilot control plane without broadening the repo into autonomous live trading. The backend now has durable pilot configuration, arming/run audit trails, approval-event audit trails, control-plane incident logging, heartbeat and restart-aware pilot supervision, and conservative live-vs-shadow evaluation tied back to Phase 11 replay provenance where possible.

The implementation stays fail-closed by default. Live trading remains off unless explicitly enabled, the pilot layer is disabled by default, manual approval remains on by default, and only one narrow strategy family can be armed in this slice.

## What Changed In Phase 12

- Added Phase 12 persistence for:
  - `polymarket_pilot_configs`
  - `polymarket_pilot_runs`
  - `polymarket_pilot_approval_events`
  - `polymarket_control_plane_incidents`
  - `polymarket_live_shadow_evaluations`
- Extended existing live OMS state with:
  - pilot config/run linkage on live orders
  - approval state, approval timestamps, and blocked-reason tracking
  - heartbeat state on `polymarket_live_state`
- Added backend control-plane services:
  - `backend/app/execution/polymarket_control_plane.py`
  - `backend/app/execution/polymarket_heartbeat.py`
  - `backend/app/execution/polymarket_pilot_supervisor.py`
- Integrated the control plane into:
  - order-intent creation
  - submit-time revalidation
  - restart-window handling
  - user-stream incident reporting
  - reconciler restart-aware pauses
  - worker supervision
  - health/status serialization
  - Prometheus metrics
- Added operator-facing API/UI surfaces for:
  - pilot config create/update/list
  - pilot arm, pause, resume, disarm, and status
  - approval queue visibility and actions
  - control-plane incidents
  - live-vs-shadow evaluations and summary
  - operator console summary
  - live orders, events, and fills
  - pilot market-tape view

## Pilot Arming And Approval Semantics

- Phase 12 currently supports one narrow pilot family first:
  - `exec_policy`
- No pilot family is active by default.
- A live submit is fail-closed unless all of the following are true:
  - global live trading is enabled
  - dry-run is off
  - pilot mode is enabled
  - an active pilot config exists
  - that pilot is armed
  - the order strategy family matches the armed family
  - the pilot config itself has `live_enabled=true`
  - kill switch is off
  - allowlist and pilot scope checks pass
  - freshness checks pass
  - reservation/capital checks pass
  - manual approval has been granted when required
  - heartbeat is not degraded when open live orders exist
  - no restart-window pause is active
- Manual approval queue behavior:
  - candidate live intents are durably recorded with `approval_state=queued`
  - approval actions are append-only in `polymarket_pilot_approval_events`
  - approvals expire after `POLYMARKET_PILOT_APPROVAL_TTL_SECONDS`
  - expired approvals become explicit `approval_timeout` incidents and are no longer approvable
- Only one pilot config can be active/armed at a time. Arming a new config disarms any prior active run.

## Heartbeat And Restart Semantics

- Heartbeat only runs when:
  - heartbeat is enabled
  - a pilot is armed
  - the run is not paused
  - there is at least one open non-dry-run live order
- Heartbeat state is operator-visible through:
  - live status
  - pilot status
  - console summary
  - health serialization
- On heartbeat failure:
  - heartbeat state is marked degraded
  - a `heartbeat_missed` incident is recorded
  - the active pilot run is paused
- Restart-window handling:
  - HTTP 425 or restart-like errors are treated as explicit restart-window events
  - the active pilot is paused with a `restart_425` incident
  - submission is blocked with `restart_pause_active`
  - the system does not continue submitting through restart windows

## Live-Vs-Shadow Evaluation Semantics And Limits

- Live-vs-shadow evaluation is stored per live order in `polymarket_live_shadow_evaluations`.
- The implementation compares live fills to replay/shadow expectations only when enough provenance exists:
  - replay linkage is taken from `source_execution_decision_id`
  - expected fill price/size are derived from stored replay orders and replay fills
  - actual fill price/size come from the live order state
  - expected net EV comes from the stored execution decision
- Coverage limits are explicit:
  - if replay linkage is missing
  - if replay scenario coverage is limited
  - if expected or actual fill fields are incomplete
  - then the record is marked `coverage_limited` instead of inventing comparability
- Gap behavior:
  - conservative price-gap bps are stored per evaluation
  - 24h average gap, worst gap, and breach count are exposed in operator status
  - shadow-gap breaches create incidents
  - the pilot can auto-pause on a breach when configured

## Operator Surface Summary

- New compact operator surfaces were added without a broad frontend redesign:
  - `Pilot Console`
  - `Live Orders`
  - `Market Tape`
  - Phase 12 health panel additions
- Operators can now:
  - arm, pause, resume, and disarm the active pilot
  - inspect approval queue state and approve or reject queued orders
  - review incidents, restart pauses, heartbeat state, and blocked submissions
  - inspect live orders, fills, and recent order events
  - compare recent live outcomes to shadow expectations
  - inspect pilot-market BBO/trades/book state plus nearby structure and quote context

## What Remains For Later Rollout Expansion

- Broader autonomous live trading remains out of scope.
- Additional pilot families remain out of scope for this slice.
- Broad multi-family concurrency remains out of scope.
- Replay redesign remains out of scope.
- SaaS/admin expansion remains out of scope.
- Any later rollout beyond this roadmap step should build on this audited control plane rather than bypass it.

## Default Safety Posture

Yes. The repo remains fail-closed and conservative by default.

- `POLYMARKET_LIVE_TRADING_ENABLED=False`
- `POLYMARKET_LIVE_DRY_RUN=True`
- `POLYMARKET_LIVE_MANUAL_APPROVAL_REQUIRED=True`
- `POLYMARKET_PILOT_ENABLED=False`
- `POLYMARKET_PILOT_REQUIRE_MANUAL_APPROVAL=True`
- no pilot is armed by default
- Phase 12 only exposes `exec_policy` as the first supported pilot family
