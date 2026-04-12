import { useState, useEffect, useCallback } from "react";
import { Routes, Route, Link, useLocation } from "react-router-dom";
import SignalFeed from "./pages/SignalFeed";
import OnboardingWizard from "./components/OnboardingWizard";
import SignalDetail from "./pages/SignalDetail";
import MarketDetail from "./pages/MarketDetail";
import Markets from "./pages/Markets";
import Analytics from "./pages/Analytics";
import Backtest from "./pages/Backtest";
import BacktestResult from "./pages/BacktestResult";
import Alerts from "./pages/Alerts";
import Performance from "./pages/Performance";
import PaperTrading from "./pages/PaperTrading";
import Health from "./pages/Health";
import Portfolio from "./pages/Portfolio";
import PushNotificationToggle from "./components/PushNotificationToggle";

function ThemeToggle() {
  const [theme, setTheme] = useState(() => localStorage.getItem("smt-theme") || "dark");

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("smt-theme", theme);
  }, [theme]);

  return (
    <button
      onClick={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
      style={{
        background: "transparent",
        border: "1px solid var(--border)",
        color: "var(--text-dim)",
        borderRadius: 6,
        padding: "4px 8px",
        fontSize: 13,
        cursor: "pointer",
      }}
      title="Toggle theme"
    >
      {theme === "dark" ? "\u2600" : "\u263E"}
    </button>
  );
}

const NAV_LINKS = [
  { to: "/", label: "Feed" },
  { to: "/performance", label: "Performance" },
  { to: "/paper-trading", label: "Strategy Health" },
  { to: "/portfolio", label: "Portfolio" },
  { to: "/markets", label: "Markets" },
  { to: "/analytics", label: "Analytics" },
  { to: "/backtests", label: "Backtest" },
  { to: "/alerts", label: "Alerts" },
  { to: "/health", label: "Health" },
];

export default function App() {
  const [menuOpen, setMenuOpen] = useState(false);
  const [showWizard, setShowWizard] = useState(
    () => !localStorage.getItem("smt-onboarded")
  );
  const [initialFilters, setInitialFilters] = useState(null);
  const location = useLocation();

  // Close mobile menu on navigation
  useEffect(() => {
    setMenuOpen(false);
  }, [location.pathname]);

  // Close menu on Escape key
  useEffect(() => {
    if (!menuOpen) return;
    const handleKey = (e) => { if (e.key === "Escape") setMenuOpen(false); };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [menuOpen]);

  // Prevent body scroll when menu is open
  useEffect(() => {
    document.body.style.overflow = menuOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [menuOpen]);

  return (
    <div className="app-container">
      <header className="app-header">
        <Link to="/" style={{ textDecoration: "none" }}>
          <h1 style={{ fontSize: 18, fontWeight: 600, color: "var(--text)" }}>
            Signal Market Terminal
          </h1>
        </Link>

        {/* Desktop navigation */}
        <nav className="app-nav">
          {NAV_LINKS.map((link) => (
            <Link key={link.to} to={link.to}>{link.label}</Link>
          ))}
          <PushNotificationToggle compact />
          <ThemeToggle />
        </nav>

        {/* Hamburger button (visible on mobile via CSS) */}
        <button className="hamburger" onClick={() => setMenuOpen(true)} aria-label="Open menu">
          &#9776;
        </button>
      </header>

      {/* Mobile menu overlay */}
      <div
        className={`mobile-menu-overlay${menuOpen ? " open" : ""}`}
        onClick={() => setMenuOpen(false)}
      />

      {/* Mobile slide-out menu */}
      <nav className={`mobile-menu${menuOpen ? " open" : ""}`}>
        <button className="mobile-menu-close" onClick={() => setMenuOpen(false)} aria-label="Close menu">
          &times;
        </button>
        {NAV_LINKS.map((link) => (
          <Link key={link.to} to={link.to}>{link.label}</Link>
        ))}
        <div className="mobile-menu-actions">
          <PushNotificationToggle compact />
          <ThemeToggle />
        </div>
      </nav>

      {showWizard && (
        <OnboardingWizard
          onComplete={(filters) => {
            setInitialFilters(filters);
            setShowWizard(false);
          }}
        />
      )}

      <Routes>
        <Route path="/" element={<SignalFeed initialFilters={initialFilters} />} />
        <Route path="/signals/:id" element={<SignalDetail />} />
        <Route path="/markets" element={<Markets />} />
        <Route path="/markets/:id" element={<MarketDetail />} />
        <Route path="/performance" element={<Performance />} />
        <Route path="/paper-trading" element={<PaperTrading />} />
        <Route path="/portfolio" element={<Portfolio />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/backtests" element={<Backtest />} />
        <Route path="/backtests/:id" element={<BacktestResult />} />
        <Route path="/alerts" element={<Alerts />} />
        <Route path="/health" element={<Health />} />
      </Routes>
    </div>
  );
}
