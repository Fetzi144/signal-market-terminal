# QA Report v0.4 — Signal Market Terminal

**Date:** 2026-04-07  
**Tester:** Claude (automated)  
**Stack:** Docker Compose — backend :8001, db :5433, frontend :5173

---

## 1. Infra / Health

| Check | Status | Detail |
|-------|--------|--------|
| Docker Stack | RUNNING | backend, db (healthy), frontend alle Up |
| `GET /api/v1/health` | PASS (200) | active_markets=3997, total_signals=3571, ingestion läuft (snapshot + market_discovery zuletzt ~20:34 UTC) |
| Frontend :5173 | PASS (200) | HTML-Response erhalten |

---

## 2. v0.4 Endpoint-Tests

### 2.1 Backtesting — `GET /api/v1/backtests`

| Status | HTTP | Response |
|--------|------|----------|
| PASS (mit Vorbehalt) | 200 | `[]` — leeres Array |

**Befund:** Endpoint existiert und antwortet korrekt. Tabelle `backtest_runs` ist in der DB vorhanden, aber leer — es wurden noch keine Backtest-Runs ausgeführt. Der Route `/api/v1/backtests/sweep` existiert in der OpenAPI-Spec, antwortet aber mit 422 (wird fälschlich als `/{run_id}` gematcht, Routing-Kollision mit UUID-Param).

---

### 2.2 Performance Dashboard — `GET /api/v1/performance/summary`

| Status | HTTP | Response |
|--------|------|----------|
| PASS | 200 | Vollständige Daten |

**Befund:** Funktioniert korrekt. Relevante Metriken:
- `overall_win_rate`: 0.3247 (32.5 %)
- `total_resolved`: 77 von 3571 Signals
- `signals_pending_resolution`: 3571 (alle unresolved — `total_markets_resolved=0`)
- `best_detector`: `deadline_near` (34.0 %)
- `worst_detector`: `price_move` (27.6 %)
- `optimal_threshold`: 0.1
- `win_rate_trend`: 1 Datenpunkt (nur heute)
- `lookback_days`: 30
- `recent_calls`: 20 Einträge zurückgegeben

**Auffälligkeit:** `signals_pending_resolution` = 3571 entspricht `total_signals_fired` = 3571 — d.h. kein einziges Signal ist bisher auf `resolved=true` gesetzt worden, obwohl 77 über `signal_evaluations` ausgewertet sind. `total_markets_resolved=0` deutet auf eine fehlende Resolution-Pipeline hin.

---

### 2.3 Portfolio Tracker — `GET /api/v1/portfolio/positions`

| Status | HTTP | Response |
|--------|------|----------|
| FAIL | 404 | `{"detail":"Not Found"}` |

**Befund:** Route `/api/v1/portfolio/positions` existiert **nicht** in der OpenAPI-Spec. Tatsächlich vorhandene Portfolio-/Positions-Routen:
- `/api/v1/positions` → **500** (`relation "positions" does not exist` — Tabelle fehlt in der DB)
- `/api/v1/portfolio/summary` → **500** (gleicher Fehler)
- `/api/v1/portfolio/export/csv` → nicht getestet, vermutlich gleicher Fehler

**Root Cause:** Die DB-Migration für die `positions`-Tabelle wurde nie ausgeführt. Tabelle fehlt komplett in `\dt`.

---

### 2.4 OFI Detection — `GET /api/v1/signals?signal_type=ofi`

| Status | HTTP | Response |
|--------|------|----------|
| FAIL | 500 | `Internal Server Error` |

**Root Cause:** `column signals.timeframe does not exist`. Das ORM-Model referenziert eine Spalte `timeframe`, die in der DB-Tabelle nicht existiert (Migration fehlt). Betrifft **alle** Aufrufe von `/api/v1/signals` unabhängig vom `signal_type`-Parameter.

**Zusatz:** Signal-Typ `ofi` ist ohnehin nicht in der DB vorhanden. Tatsächliche Typen: `deadline_near`, `order_flow_imbalance`, `price_move`, `spread_change`, `volume_spike`, `liquidity_vacuum`. OFI wird intern als `order_flow_imbalance` gespeichert, nicht als `ofi`.

---

### 2.5 Arbitrage — `GET /api/v1/signals?signal_type=arbitrage`

| Status | HTTP | Response |
|--------|------|----------|
| FAIL | 500 | `Internal Server Error` |

**Root Cause:** Gleicher Fehler wie 2.4 (`signals.timeframe` fehlt). Zusätzlich: Signal-Typ `arbitrage` existiert nicht in der DB.

---

### 2.6 Whale Tracking — `GET /api/v1/signals?signal_type=whale`

| Status | HTTP | Response |
|--------|------|----------|
| FAIL | 500 | `Internal Server Error` |

**Root Cause:** Gleicher Fehler wie 2.4 (`signals.timeframe` fehlt). Zusätzlich: Signal-Typ `whale` existiert nicht in der DB.

---

## 3. Pytest

```
249 passed, 74 warnings in 26.42s
```

**Alle 249 Tests grün.** Die 74 Warnings sind `DeprecationWarning` aus `asyncio.iscoroutinefunction` (FastAPI/slowapi, Python 3.14-Kompatibilität) — kein Handlungsbedarf.

---

## 4. Zusammenfassung

| Feature | Endpoint | HTTP | Ergebnis |
|---------|----------|------|----------|
| Health | `GET /health` | 200 | PASS |
| Backtesting | `GET /backtests` | 200 | PASS (leer, kein Run) |
| Performance Dashboard | `GET /performance/summary` | 200 | PASS |
| Portfolio Tracker | `GET /portfolio/positions` | 404 | FAIL — Route falsch |
| Portfolio (korrekte Route) | `GET /positions` | 500 | FAIL — Tabelle fehlt |
| OFI Detection | `GET /signals?signal_type=ofi` | 500 | FAIL — `timeframe`-Spalte fehlt |
| Arbitrage | `GET /signals?signal_type=arbitrage` | 500 | FAIL — `timeframe`-Spalte fehlt |
| Whale Tracking | `GET /signals?signal_type=whale` | 500 | FAIL — `timeframe`-Spalte fehlt |
| Frontend | `:5173` | 200 | PASS |
| Tests | pytest | — | 249/249 PASS |

---

## 5. Gefundene Bugs (nicht gefixt)

### BUG-1: Fehlende DB-Migration — `signals.timeframe`
- **Symptom:** `GET /api/v1/signals` → 500 auf allen Calls
- **Ursache:** ORM-Model hat Spalte `timeframe`, DB-Tabelle nicht
- **Betrifft:** Alle 3 neuen Signal-Typen (OFI, Arbitrage, Whale)

### BUG-2: Fehlende DB-Migration — Tabelle `positions`
- **Symptom:** `/api/v1/positions` und `/api/v1/portfolio/summary` → 500
- **Ursache:** Tabelle `positions` existiert nicht in DB (`relation "positions" does not exist`)

### BUG-3: Falsche Route in Dokumentation/Tests — `/portfolio/positions`
- **Symptom:** 404 Not Found
- **Ursache:** Route heißt `/api/v1/positions`, nicht `/api/v1/portfolio/positions`

### BUG-4: Signal-Typen `ofi`, `arbitrage`, `whale` nicht in DB
- **Symptom:** Auch nach Behebung von BUG-1 würden diese Filter 0 Ergebnisse zurückgeben
- **Ursache:** Detektoren für diese Typen sind offenbar noch nicht aktiv / haben noch keine Signale gefeuert. Tatsächliche Typen: `order_flow_imbalance`, `spread_change`, `volume_spike`, `price_move`, `deadline_near`, `liquidity_vacuum`

### BUG-5: Routing-Kollision `/api/v1/backtests/sweep`
- **Symptom:** 422 statt erwarteter Antwort
- **Ursache:** `/backtests/sweep` wird als `/backtests/{run_id}` gematcht (FastAPI-Routing-Reihenfolge)
