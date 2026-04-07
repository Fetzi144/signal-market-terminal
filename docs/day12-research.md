# Day 12 Research — Mobile-Responsive Frontend + PWA

**Date:** 2026-04-07  
**Scope:** Frontend structure, CSS, responsive design, PWA readiness

---

## 1. React Components Inventory

### Pages (11 total)
- `SignalFeed.jsx` — Signal list with real-time SSE updates, filtering, pagination (50 per page)
- `SignalDetail.jsx` — Detail view for single signal
- `Markets.jsx` — Market list with search, filters, sort, pagination
- `MarketDetail.jsx` — Market detail page with chart
- `Analytics.jsx` — Analytics dashboard
- `Backtest.jsx` — Backtest list/sweep
- `BacktestResult.jsx` — Backtest result detail
- `Alerts.jsx` — Alerts configuration
- `Performance.jsx` — Performance metrics
- `Portfolio.jsx` — Portfolio tracker with positions, P&L chart
- `Health.jsx` — System health dashboard

### Reusable Components (3 total)
- `PriceChart.jsx` — Recharts LineChart/ComposedChart for OHLCV data, responsive container
- `SignalEvaluationBar.jsx` — Signal evaluation visualization
- `PushNotificationToggle.jsx` — Web push notification toggle

### Hooks (1 total)
- `useSSE.js` — Server-Sent Events hook with auto-reconnect

---

## 2. CSS & Styling Framework

**Framework:** **Plain CSS** (no Tailwind, no Material-UI, no styled-components)

### Color System
- Uses **CSS Custom Properties** (`--bg`, `--text`, `--accent`, etc.) defined in `:root`
- Dark mode (default): `#0a0a0f` background, `#e0e0e8` text
- Light mode: `#f5f5f7` background, `#1a1a2e` text
- Theme toggle: `[data-theme="light"]` selector, stored in `localStorage` as `smt-theme`
- Platform colors: Polymarket `#6366f1` (indigo), Kalshi `#f59e0b` (amber)
- Semantic colors: `--green` `#00d68f`, `--red` `#ff6b6b`, `--yellow` `#ffd93d`

### Responsive Approach
- **Flexbox & Grid** used throughout
- `flex-wrap: wrap` on filter rows for collapse on small screens
- `gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))"` for responsive card grids (Portfolio summary cards)
- **No explicit media queries** found in `index.css`
- Fixed layout: `maxWidth: 960px` center container in `App.jsx` with padding

### Current Gaps
- No mobile-first breakpoints (mobile, tablet, desktop)
- No media queries for navigation collapse/hamburger
- No touch-friendly button sizing (current: 6px padding, 13px font — tight on mobile)
- Flex controls row uses `gap: 12px` but no wrapping strategy defined

---

## 3. Existing Responsive/Adaptive Features

✅ **Working:**
- Chart component uses Recharts `<ResponsiveContainer>` (width="100%" height={N})
- Portfolio grid cards use `auto-fit` to stack on small screens
- Flex controls naturally wrap with `flexWrap: "wrap"`
- Input width set with `flex: "1 1 200px"` for responsive search

❌ **Missing:**
- Navigation header (12 links) does not collapse on mobile
- Modal/dialog uses `fixed, inset: 0` — needs viewport handling
- Table (`PositionsTable`) has no horizontal scroll for mobile
- Inline styles hardcoded for desktop widths (e.g., `width: 360` for dialogs, `gridTemplateColumns: "1fr 1fr 1fr"`)

---

## 4. Routing & Pages Structure

**Router:** React Router v6

**Routes:**
```
/ → SignalFeed
/signals/:id → SignalDetail
/markets → Markets
/markets/:id → MarketDetail
/performance → Performance
/portfolio → Portfolio
/analytics → Analytics
/backtests → Backtest
/backtests/:id → BacktestResult
/alerts → Alerts
/health → Health
```

**Entry point:** `App.jsx` with top navigation bar (header with flexbox layout)

---

## 5. PWA Status

### Current PWA Implementation
✅ **Service Worker:** `public/sw.js` exists
- **Scope:** Push notifications only (`self.addEventListener("push")`)
- **Registration:** In `main.jsx` — `navigator.serviceWorker.register("/sw.js").catch(() => {})`
- **Features:**
  - Shows desktop notifications on `push` events
  - Handles `notificationclick` to navigate to URL in notification data

❌ **Missing PWA Files:**
- **manifest.json** — NOT FOUND; needed for app icon, name, display mode, theme color
- **favicon.ico** — Referenced in sw.js but not verified in `public/`
- **Offline caching** — Service worker has no cache-first or network-first strategies
- **Installation prompt** — No `beforeinstallprompt` handling

### What's Needed for PWA
1. `public/manifest.json` with:
   - `"name"`, `"short_name"`, `"display": "standalone"`
   - `"start_url": "/"`, `"theme_color"`, `"background_color"`
   - Icons (192x192, 512x512)
2. Offline caching strategy in `sw.js` (Cache API, Workbox, etc.)
3. App shell caching for instant load on slow networks
4. Install button/UX to trigger PWA install

---

## 6. Browser Support & Environment

**Dependencies:**
```json
"react": "^18.3.1",
"react-dom": "^18.3.1",
"react-router-dom": "^6.28.0",
"recharts": "^2.12.0"
```

**Build:** Vite 6 with React plugin, no TypeScript (JSX only)

**Dev server:** Port 5173, proxies `/api` to backend

---

## 7. Gaps Summary (Day 12 Scope)

| Category | Status | Gap |
|----------|--------|-----|
| **Mobile Layout** | ⚠️ Partial | Navigation bar not responsive, dialogs not mobile-optimized |
| **Media Queries** | ❌ None | Need breakpoints for `480px`, `768px`, `1024px` |
| **Touch UX** | ❌ Missing | Button/input padding too small for touch (6px), no touch-friendly spacing |
| **PWA Manifest** | ❌ Missing | `manifest.json` not created |
| **Offline Mode** | ❌ Missing | Service worker has no caching strategy |
| **Viewport Meta** | ✅ Present | `<meta name="viewport">` in `index.html` |
| **Responsive Images** | N/A | No images yet (charts are SVG/canvas) |
| **Form UX** | ⚠️ Partial | Select fields work, but mobile keyboard handling untested |

---

## 8. Next Steps (for Day 12 Implementation)

1. **Breakpoints:** Define `480px`, `768px`, `1024px` and add media queries to `index.css`
2. **Navigation:** Make header responsive (hamburger on mobile, collapse menu)
3. **Touch Sizing:** Increase button padding to 8–10px, input height to 36–40px
4. **manifest.json:** Create with app metadata and icons
5. **Service Worker:** Add cache strategies (app shell + API caching)
6. **Dialog/Modal:** Make responsive with max-width, max-height, overflow handling
7. **Table Scroll:** Add horizontal scroll or card view for mobile
8. **Testing:** Test at 375px (mobile), 768px (tablet), 1440px (desktop)

---

**Research Date:** 2026-04-07  
**Researcher:** Claude Code  
**Status:** Ready for implementation planning
