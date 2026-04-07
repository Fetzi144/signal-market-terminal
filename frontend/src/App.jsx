import { Routes, Route, Link } from "react-router-dom";
import SignalFeed from "./pages/SignalFeed";
import SignalDetail from "./pages/SignalDetail";
import MarketDetail from "./pages/MarketDetail";
import Markets from "./pages/Markets";
import Alerts from "./pages/Alerts";
import Health from "./pages/Health";

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
        <nav style={{ display: "flex", gap: 16, fontSize: 14 }}>
          <Link to="/">Feed</Link>
          <Link to="/markets">Markets</Link>
          <Link to="/alerts">Alerts</Link>
          <Link to="/health">Health</Link>
        </nav>
      </header>
      <Routes>
        <Route path="/" element={<SignalFeed />} />
        <Route path="/signals/:id" element={<SignalDetail />} />
        <Route path="/markets" element={<Markets />} />
        <Route path="/markets/:id" element={<MarketDetail />} />
        <Route path="/alerts" element={<Alerts />} />
        <Route path="/health" element={<Health />} />
      </Routes>
    </div>
  );
}
