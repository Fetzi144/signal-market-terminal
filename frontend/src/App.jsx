import { useState, useEffect } from "react";
import { Routes, Route, Link } from "react-router-dom";
import SignalFeed from "./pages/SignalFeed";
import SignalDetail from "./pages/SignalDetail";
import MarketDetail from "./pages/MarketDetail";
import Markets from "./pages/Markets";
import Analytics from "./pages/Analytics";
import Backtest from "./pages/Backtest";
import BacktestResult from "./pages/BacktestResult";
import Alerts from "./pages/Alerts";
import Performance from "./pages/Performance";
import Health from "./pages/Health";

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

export default function App() {
  return (
    <div style={{ maxWidth: 960, margin: "0 auto", padding: "16px 20px" }}>
      <header
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: "1px solid var(--border)",
          paddingBottom: 12,
          marginBottom: 20,
        }}
      >
        <Link to="/" style={{ textDecoration: "none" }}>
          <h1 style={{ fontSize: 18, fontWeight: 600, color: "var(--text)" }}>
            Signal Market Terminal
          </h1>
        </Link>
        <nav style={{ display: "flex", gap: 16, fontSize: 14, alignItems: "center" }}>
          <Link to="/">Feed</Link>
          <Link to="/performance">Performance</Link>
          <Link to="/markets">Markets</Link>
          <Link to="/analytics">Analytics</Link>
          <Link to="/backtests">Backtest</Link>
          <Link to="/alerts">Alerts</Link>
          <Link to="/health">Health</Link>
          <ThemeToggle />
        </nav>
      </header>
      <Routes>
        <Route path="/" element={<SignalFeed />} />
        <Route path="/signals/:id" element={<SignalDetail />} />
        <Route path="/markets" element={<Markets />} />
        <Route path="/markets/:id" element={<MarketDetail />} />
        <Route path="/performance" element={<Performance />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/backtests" element={<Backtest />} />
        <Route path="/backtests/:id" element={<BacktestResult />} />
        <Route path="/alerts" element={<Alerts />} />
        <Route path="/health" element={<Health />} />
      </Routes>
    </div>
  );
}
