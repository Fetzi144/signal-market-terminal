# Onboarding UX + Developer Experience Analysis

**Date:** 2026-04-09  
**Scope:** Signal Market Terminal v0.4.0  
**Goal:** Identify gaps for a Getting Started Wizard and a `make dev` one-liner

---

## 1. Aktueller First-Visit Flow

### React Routes (App.jsx)

| Route | Seite | Erreichbar über Nav? |
|---|---|---|
| `/` | SignalFeed | Feed |
| `/signals/:id` | SignalDetail | — (nur via Card-Klick) |
| `/markets` | Markets | Markets |
| `/markets/:id` | MarketDetail | — (nur via Zeile) |
| `/performance` | Performance | Performance |
| `/portfolio` | Portfolio | Portfolio |
| `/analytics` | Analytics | Analytics |
| `/backtests` | Backtest | Backtest |
| `/backtests/:id` | BacktestResult | — (nur via Ergebnis-Klick) |
| `/alerts` | Alerts | Alerts |
| `/health` | Health | Health |

**Startseite:** `/` → `SignalFeed`

Die App lädt direkt in den vollen Feed. Keine Begrüßung, kein Onboarding-State, kein leerer-Zustand-Handling außer dem generischen Text „No signals yet. Waiting for data ingestion and detection...".

### Was passiert beim ersten Laden (SignalFeed.jsx)

1. Vier simultane API-Calls werden ausgelöst:
   - `getSignalTypes()` → befüllt Type-Dropdown
   - `getMarketPlatforms()` → befüllt Platform-Dropdown
   - `getSignalTimeframes()` → befüllt Timeframe-Dropdown
   - `getSignals({ page: 1, pageSize: 50 })` → lädt bis zu 50 Signale
2. Während des Ladens zeigt `SkeletonCard` × 4 (vier Platzhalter).
3. Nach dem Laden: entweder Signals-Liste oder der leere-Zustand-Text.

### Filter-Defaults

Alle Filter starten auf „Alles anzeigen" (kein Vorauswahl):

```js
const [filter, setFilter] = useState("");            // All Types
const [platformFilter, setPlatformFilter] = useState(""); // All Platforms
const [timeframeFilter, setTimeframeFilter] = useState(""); // All Timeframes
const [resolvedFilter, setResolvedFilter] = useState(""); // All Resolutions
```

Es gibt keine kuratierten Defaults (z. B. „zeige nur High-Rank-Signale der letzten 24h").

### Signale auf der Startseite

`PAGE_SIZE = 50` — maximal 50 Signale, keine Score-Schwelle, keine Zeitbegrenzung, absteigende Reihenfolge nach `rank_score` (serverseitig).

---

## 2. Was ein neuer Nutzer sieht — und warum es überwältigend ist

### Szenario A: Frische Installation, keine Daten

Der Nutzer startet `docker compose up`, öffnet `localhost:5173` und sieht:

- Vier leere Skeleton-Karten für ~1 s
- Danach: **„No signals yet. Waiting for data ingestion and detection..."**
- Vier Filter-Dropdowns (leer, weil die API nichts zurückgibt)
- Acht Nav-Links ohne Erklärung
- Kein Hinweis: *Warum gibt es keine Signale? Was muss ich tun?*

Der Nutzer weiß nicht, ob die App kaputt ist, ob API-Keys fehlen, oder ob er einfach warten muss.

### Szenario B: Daten vorhanden (Production oder nach einigen Minuten)

Der Nutzer sieht sofort bis zu 50 Signal-Karten mit:

- Abkürzungen (`PM`, `KA`, `Str`, `Conf`, `Rank`)
- Farb-codierten Timeframe-Badges (5m/15m/30m/1h/4h/24h) ohne Legende
- Score-Werte (`Str: 73%`, `Conf: 45%`, `Rank: 62%`) ohne Erklärung der Formel
- Signaltyp-Namen in UPPERCASE (`PRICE MOVE`, `LIQUIDITY VACUUM`, `CONFLUENCE`) ohne Definition
- Confluence-Badges (`Confirmed: 1h + 4h`) ohne Kontext

Kein Tooltip, kein Glossar, keine „Was ist das?"-Erklärung irgendwo in der UI.

**Kernproblem:** Die App setzt implizit voraus, dass der Nutzer die `CLAUDE.md` gelesen hat. Für jeden anderen ist der erste Blick ein Wall of jargon.

---

## 3. Konkreter Plan: Getting Started Wizard

### Konzept

Ein einmaliger, überspringbarer Modal-Wizard (3 Schritte), der beim allerersten Besuch erscheint. State wird in `localStorage` gespeichert (`smt-onboarded: true`).

### Schritt 1: „Was ist dieses Tool?"

- Kurze Erklärung: Polymarket + Kalshi Signals
- Erklärung der Rank-Formel: `signal_score × confidence × recency_weight`
- Legende der Badges (PM = Polymarket, KA = Kalshi, Timeframe-Farben)
- Glossar der Signaltypen (Price Move, Volume Spike, Spread Change, Liquidity Vacuum, Deadline Near, Confluence)

**API-Calls benötigt:**
- `GET /api/v1/signals/types` → Liste aller Typen für dynamische Legende

### Schritt 2: „So liest du eine Signal-Karte"

- Annotierte Signal-Card (Mock-Daten, kein echter API-Call)
- Erklärung: Str = signal_score, Conf = confidence, Rank = rank_score
- Erklärung: „Called it" / „Wrong call" / „Pending" Resolution-Badges
- Erklärung: Was Confluence bedeutet (gleiche Richtung auf mehreren Timeframes)

**API-Calls benötigt:** Keine (statische Mock-Karte)

### Schritt 3: „Empfohlene Filter-Einstellungen"

- Vorschlag: Starte mit `Rank > 60%`, `Timeframe: 1h oder 4h`
- Button: „Diese Filter anwenden" → setzt State in SignalFeed
- Button: „Alles zeigen" → überspringt

**API-Calls benötigt:** Keine (setzt nur lokalen State)

### UI-Komponenten die erstellt werden müssen

| Komponente | Datei |
|---|---|
| `OnboardingWizard` | `frontend/src/components/OnboardingWizard.jsx` |
| `MockSignalCard` | `frontend/src/components/MockSignalCard.jsx` (oder inline) |
| Wizard-Trigger in App | `frontend/src/App.jsx` (localStorage-Check) |
| CSS für Modal/Overlay | `frontend/src/index.css` (bereits vorhanden, erweitern) |

### State-Management

```js
// App.jsx
const [showWizard, setShowWizard] = useState(
  () => !localStorage.getItem("smt-onboarded")
);
// Nach Abschluss:
localStorage.setItem("smt-onboarded", "true");
setShowWizard(false);
```

Kein neuer globaler State-Manager nötig — `useState` + Props reichen.

### Initiale Filter-Empfehlungen via Props

```jsx
// App.jsx
<OnboardingWizard
  onComplete={(filters) => {
    // filters: { minRank: 0.6, timeframe: "1h" } oder null
    setInitialFilters(filters);
    setShowWizard(false);
  }}
/>
// Dann: initialFilters als prop an SignalFeed
```

SignalFeed muss `initialFilters` als optionale Props akzeptieren, um die Defaults zu überschreiben.

---

## 4. Konkreter Plan: `make dev` One-Liner

### Aktueller Stand (kein Makefile vorhanden)

Ein neuer Entwickler muss heute folgende Schritte ausführen:

1. Repository klonen
2. `cp backend/.env.example backend/.env` ausführen
3. `.env` editieren (DATABASE_URL, API-Keys prüfen/anpassen)
4. `docker compose up` ausführen
5. Warten bis alle drei Services healthy sind
6. Browser auf `localhost:5173` öffnen
7. Bei Datenbankproblemen: manuell `alembic upgrade head` ausführen

**Das sind 7 Schritte**, von denen Schritt 3 und 7 undokumentiert sind, wenn man nur die README liest.

### Probleme mit Alembic

- `alembic.ini` hat hardcoded `postgresql+asyncpg://smt:smt@localhost:5432/smt`
- Bei Docker läuft Postgres auf Port `5433` (Host) / `5432` (intern)
- Migrationen **müssen aus dem Backend-Container oder mit korrekter DATABASE_URL** ausgeführt werden, sonst Verbindungsfehler
- Docker Compose führt Migrationen **nicht automatisch** aus — das Backend startet direkt mit `uvicorn`

### Makefile-Ziele (zu erstellen)

```makefile
# Makefile (im Root)
.PHONY: dev setup stop logs

dev: setup
	docker compose up

setup:
	@test -f backend/.env || cp backend/.env.example backend/.env
	@echo "✓ .env ready (edit backend/.env to add API keys)"

stop:
	docker compose down

logs:
	docker compose logs -f backend

migrate:
	docker compose exec backend alembic upgrade head

reset:
	docker compose down -v
	docker compose up
```

**Ergebnis:** `make dev` als echter One-Liner (mit automatischem `.env`-Setup).

### Migrations-Problem lösen

Option A (empfohlen): Migrations als `command` im `docker-compose.yml` verketten:
```yaml
command: >
  sh -c "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"
```

Option B: Separater `migrate`-Service in docker-compose (läuft einmal, dann `exit 0`).

Aktuell ist Option A einfacher und erfordert nur eine Zeile in `docker-compose.yml`.

---

## 5. Welche Dateien müssen geändert werden

### Für den Getting Started Wizard

| Datei | Änderung |
|---|---|
| `frontend/src/App.jsx` | localStorage-Check, `showWizard`-State, `<OnboardingWizard>`-Einbindung, `initialFilters`-State |
| `frontend/src/pages/SignalFeed.jsx` | `initialFilters`-Props akzeptieren, Filter-State-Init daraus ableiten |
| `frontend/src/components/OnboardingWizard.jsx` | **Neue Datei** — 3-Schritt-Wizard-Komponente |
| `frontend/src/index.css` | Modal/Overlay-CSS ergänzen (Klassen bereits teilweise da) |

### Für `make dev`

| Datei | Änderung |
|---|---|
| `Makefile` | **Neue Datei** im Projekt-Root |
| `docker-compose.yml` | `command` im `backend`-Service: `alembic upgrade head &&` voranstellen |

### Optional: Empty State verbessern

| Datei | Änderung |
|---|---|
| `frontend/src/pages/SignalFeed.jsx` | Leerer-Zustand-Text durch kontextreichen `EmptyState`-Block ersetzen (zeigt Health-Link, erklärt Warteprozess) |

---

## Zusammenfassung

| Problem | Schwere | Aufwand |
|---|---|---|
| Kein Onboarding-Kontext für neuen Nutzer | Hoch | ~1 Tag (Wizard) |
| Leerer Zustand ohne Erklärung | Mittel | ~2h |
| Kein Makefile, 7 manuelle Schritte | Mittel | ~1h |
| Migrations laufen nicht automatisch | Mittel | ~30min |
| Keine Filter-Defaults / Empfehlungen | Niedrig | ~1h |
