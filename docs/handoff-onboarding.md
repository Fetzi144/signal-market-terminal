# Handoff: Onboarding & Developer Experience Improvements

**Date:** 2026-04-09
**Scope:** Makefile, auto-migrations, Getting Started Wizard

---

## What was done

### 1. Makefile (`make dev` one-liner)

**File:** `Makefile` (new, project root)

Targets:
- `make dev` — checks for `backend/.env` (copies from `.env.example` if missing), then runs `docker compose up`
- `make stop` — `docker compose down`
- `make logs` — tails backend logs
- `make migrate` — runs `alembic upgrade head` inside the backend container
- `make reset` — destroys volumes and restarts fresh

New developer flow is now: `git clone … && cd signal-market-terminal && make dev`.

### 2. Auto-Migrations in Docker Compose

**File:** `docker-compose.yml` (modified backend service `command`)

The backend command now runs `alembic upgrade head` before starting uvicorn:
```yaml
command: >
  sh -c "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"
```

This ensures the database schema is always up-to-date when the container starts. Note: `entrypoint.sh` also runs migrations — the duplication is intentional for visibility and safety (alembic upgrade is idempotent).

### 3. Getting Started Wizard (OnboardingWizard)

**New file:** `frontend/src/components/OnboardingWizard.jsx`
**Modified:** `frontend/src/App.jsx`, `frontend/src/pages/SignalFeed.jsx`, `frontend/src/index.css`

#### Behavior
- Shows a 3-step modal wizard on first visit (no `smt-onboarded` key in localStorage)
- Skippable at any step (Skip button, X button, or clicking overlay)
- After completion or skip, sets `localStorage("smt-onboarded", "true")` and never shows again

#### Steps

| Step | Title | Content |
|------|-------|---------|
| 1 | What is this tool? | Explains SMT purpose, rank formula, platform badges (PM/KA), timeframe color legend, signal type glossary |
| 2 | How to read a signal card | Annotated mock signal card, explains Str/Conf/Rank, resolution badges, confluence |
| 3 | Recommended filters | Suggests Timeframe 1h/4h + Rank >= 60%. Two buttons: "Apply recommended" (sets timeframe filter to 1h) or "Show everything" |

#### Integration points
- `App.jsx`: manages `showWizard` state via localStorage check, passes `initialFilters` to SignalFeed
- `SignalFeed.jsx`: accepts optional `initialFilters` prop, uses `initialFilters.timeframe` to set initial timeframe filter state
- `index.css`: `.wizard-overlay`, `.wizard-content`, `.wizard-close`, `.wizard-body` classes with mobile responsive breakpoint

---

## Testing

- All 275 backend tests pass (`pytest` — no changes to backend code)
- Frontend changes are UI-only; no API contract changes

## How to reset the wizard

Clear the localStorage key to see the wizard again:
```js
localStorage.removeItem("smt-onboarded");
```
Then reload the page.

## Future improvements

- Dynamic signal type list from API in Step 1 (currently static)
- Persist recommended filters to localStorage so they survive page reload
- Add a "Show wizard again" button in a settings/help menu
- Improve empty state on SignalFeed with contextual help when no signals exist
