# Day 12 Plan — Mobile-Responsive Frontend + PWA

**Date:** 2026-04-07
**Goal:** Make Signal Market Terminal fully usable on mobile devices and installable as a PWA.

---

## 1. Media Queries (index.css)

**Breakpoints:**
- `1024px` — tablet landscape / small desktop: reduce max-width, adjust grid columns
- `768px` — tablet portrait: stack navigation, reduce padding
- `480px` — mobile: single-column layout, full-width elements

**What changes per breakpoint:**
- Container max-width & padding
- Navigation layout (horizontal → hamburger)
- Grid columns (auto-fit adjustments)
- Font sizes for headings
- Dialog/modal width (fixed 360px → responsive max-width)

## 2. Responsive Navigation (App.jsx)

- Add hamburger button (hidden on desktop, visible at ≤768px)
- Navigation links collapse into a slide-down/overlay menu on mobile
- Menu closes on link click and outside click
- Theme toggle & push notification toggle stay accessible in mobile menu

## 3. Touch-Friendly Sizing (index.css)

- Buttons: min-height 36px, padding 8px 16px minimum
- Inputs/selects: min-height 40px, padding 8px 12px
- Navigation links: min 44px tap target
- Table action buttons: increase padding for touch

## 4. Table Horizontal Scroll

- Wrap tables in a container with `overflow-x: auto`
- Already partially done in Portfolio (`overflow: auto` on wrapper div)
- Add CSS class `.table-scroll` for consistent behavior across all pages
- Apply to PositionsTable and any other data tables

## 5. manifest.json

```json
{
  "name": "Signal Market Terminal",
  "short_name": "SMT",
  "display": "standalone",
  "start_url": "/",
  "theme_color": "#6c5ce7",
  "background_color": "#0a0a0f",
  "icons": [
    { "src": "/icons/icon-192.png", "sizes": "192x192", "type": "image/png" },
    { "src": "/icons/icon-512.png", "sizes": "512x512", "type": "image/png" }
  ]
}
```

- Link in `index.html`: `<link rel="manifest" href="/manifest.json">`
- Generate minimal SVG-based icons (or use placeholder PNGs)

## 6. Service Worker — Offline App-Shell Caching

**Strategy:** Cache-first for app shell, network-first for API calls.

**App shell assets to cache:**
- `/` (index.html)
- CSS/JS bundles (from Vite build output)
- Manifest & icons

**Implementation:**
- `install` event: pre-cache app shell with versioned cache name
- `activate` event: clean old caches
- `fetch` event:
  - App shell requests → cache-first (fall back to network)
  - API requests (`/api/`) → network-first (fall back to cache for offline)
  - Other assets → stale-while-revalidate

## 7. Testing Plan

- Run `pytest` for backend (should still pass — no backend changes)
- Run `npm run build` for frontend to verify no CSS/JSX errors
- Manual breakpoint checks: 375px, 768px, 1024px, 1440px

## 8. Deliverables

| File | Change |
|------|--------|
| `frontend/src/index.css` | Media queries, touch sizing, table scroll classes |
| `frontend/src/App.jsx` | Hamburger menu, responsive nav |
| `frontend/index.html` | Manifest link, theme-color meta |
| `frontend/public/manifest.json` | NEW — PWA manifest |
| `frontend/public/sw.js` | Offline caching strategies |
| `docs/day12-plan.md` | This file |
| `docs/handoff-day12.md` | Handoff document |
