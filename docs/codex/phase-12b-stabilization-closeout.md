# Phase 12B Stabilization Closeout

## Summary

Phase 12B is post-roadmap hardening for the existing narrow Polymarket live pilot. It does not create an official Phase 13, does not broaden the strategy surface, and does not relax the repo's fail-closed defaults. The work adds durable lot accounting, realized P&L evidence, pilot guardrail audit trails, scorecards, readiness reporting, and stronger operator visibility around incidents and approval latency.

The implementation keeps the pilot narrow and supervised. Live trading stays disabled by default, the pilot layer stays disabled by default, manual approval stays required by default, and `exec_policy` remains the only armable family in this slice.

## What Changed In Phase 12B

- Added Phase 12B persistence for:
  - `position_lots`
  - `position_lot_events`
  - `polymarket_pilot_scorecards`
  - `polymarket_pilot_guardrail_events`
  - `polymarket_pilot_readiness_reports`
- Added `backend/app/execution/polymarket_pilot_evidence.py` to:
  - maintain durable lot state from canonical live fills
  - compute realized gross/net P&L and fee totals
  - generate scorecards and readiness reports
  - emit append-only guardrail audit events
  - expose compact evidence summaries for API and health surfaces
- Hardened live-fill provenance so Phase 12B lot accounting keys off canonical economic fills rather than duplicate status-lifecycle rows.
- Tightened the family boundary so the pilot support set and arming path both remain `exec_policy` only.
- Extended the control plane, heartbeat path, OMS, reconciler, API, health endpoint, metrics, and pilot frontend surfaces to carry the new evidence and guardrail state.

## Position-Lot Semantics

- Lots are built from actual persisted live fills only.
- Same-side fills do not get merged into synthetic blended positions; each unmatched residual fill opens its own lot with durable provenance back to:
  - `source_live_order_id`
  - `source_fill_id`
  - `pilot_run_id` when available
- Opposite-side fills close older open lots conservatively in FIFO order.
- Partial closes update:
  - `remaining_size`
  - `avg_close_price`
  - `realized_pnl`
  - `fee_paid`
  - lot `status`
- Fill-fee changes after first observation are handled through `position_lot_events.event_type = fee_update` so fee corrections remain auditable instead of silently rewriting history.

## Realized P&L Semantics

- Gross realized P&L is recorded on close/partial-close lot events.
- Fees are tracked separately on lot and lot-event rows.
- Net realized P&L is therefore:
  - `gross realized pnl - fees paid`
- Current-day realized P&L is surfaced in:
  - pilot evidence summaries
  - health serialization
  - Prometheus gauge state
- The implementation does not invent hedges, conversions, or synthetic close paths that were not actually executed.

## Guardrail Semantics

- Phase 12B records append-only guardrail audit rows for:
  - approval TTL breaches
  - stale decision-age blocks
  - max daily loss breaches
  - max outstanding notional cross-check breaches
  - shadow-gap breaches
  - heartbeat degradation
  - restart pauses
- Guardrails are fail-closed:
  - block when the safe action is to refuse a submission
  - pause when the safe action is to stop the pilot
- Approval expirations remain durable in both:
  - approval-event audit rows
  - guardrail-event audit rows
- Heartbeat failure and restart-window handling now land in both incident/operator surfaces and guardrail audit surfaces.

## Scorecard Semantics

- Scorecards aggregate the pilot evidence window for:
  - live orders
  - fills
  - approvals
  - approval expirations
  - rejections
  - incidents
  - realized gross/net P&L
  - fees
  - average and worst live-vs-shadow gap
  - coverage-limited counts
- Scorecards stay conservative:
  - coverage-limited replay linkage is carried forward explicitly
  - the system does not overstate comparability where provenance is incomplete
- Scheduled generation uses stable completed windows rather than constantly creating new rolling snapshots.

## Readiness-Report Semantics

- Readiness reports are operator-facing evidence summaries only.
- They do not change pilot mode automatically.
- `manual_only` remains the default recommendation.
- `candidate_for_semi_auto` is only emitted when explicit thresholds are met across the configured minimum evidence window.
- `not_ready` is used when blockers remain present, such as:
  - open incidents
  - approval backlog
  - coverage-limited evidence gaps
  - shadow-gap breaches
  - serious guardrail events

## Operator Surface Changes

- Added API endpoints for:
  - position lots
  - lot events
  - pilot scorecards
  - guardrail events
  - readiness reports
  - manual scorecard generation
  - manual readiness-report generation
- Extended console summary and health serialization with:
  - daily realized P&L
  - approval expirations
  - recent guardrail triggers
  - latest readiness state
- Added compact pilot UI visibility for:
  - guardrail events
  - evidence summaries
  - readiness outputs
  - manual scorecard/readiness generation

## Preconditions Before Any Semi-Automatic Expansion Discussion

All of the following must still be true before any later semi-automatic expansion is even discussed:

- live trading remains off by default
- pilot mode remains off by default
- manual approval remains on by default
- `exec_policy` remains the only armable family unless a separate scoped change explicitly broadens that boundary
- lot accounting and realized P&L remain driven by actual live fills, not synthetic inference
- scorecards and readiness reports remain advisory/operator-facing, not auto-enablement logic
- shadow-gap, approval-latency, heartbeat, restart, and loss guardrails remain fail-closed and auditable
- no secret material is exposed through API payloads, health output, or frontend responses

This slice stops at stabilization, evidence, and lot accounting.
