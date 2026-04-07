import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getMarket, getMarketSnapshots, getSignals } from "../api";
import PriceChart from "../components/PriceChart";

export default function MarketDetail() {
  const { id } = useParams();
  const [market, setMarket] = useState(null);
  const [snapshots, setSnapshots] = useState([]);
  const [signals, setSignals] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    Promise.all([
      getMarket(id),
      getMarketSnapshots(id, 50),
      getSignals({ marketId: id, pageSize: 20 }),
    ])
      .then(([m, s, sig]) => {
        setMarket(m);
        setSnapshots(s);
        setSignals(sig);
      })
      .catch((e) => setError(e.message));
  }, [id]);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  if (!market) return <div style={{ color: "var(--text-dim)" }}>Loading...</div>;

  return (
    <div>
      <Link to="/" style={{ fontSize: 13, color: "var(--text-dim)" }}>
        &larr; Back to feed
      </Link>

      <div
        style={{
          background: "var(--bg-card)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: 20,
          marginTop: 12,
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
          <span
            style={{
              fontSize: 10,
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: 0.5,
              color: "#fff",
              background: market.platform === "kalshi" ? "#f59e0b" : "#6366f1",
              padding: "2px 8px",
              borderRadius: 4,
            }}
          >
            {market.platform}
          </span>
          <span
            style={{
              fontSize: 12,
              color: market.active ? "var(--green)" : "var(--red)",
            }}
          >
            {market.active ? "Active" : "Closed"}
          </span>
        </div>
        <h2 style={{ fontSize: 18, marginBottom: 12 }}>{market.question}</h2>

        {market.category && (
          <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 8 }}>
            {market.category}
          </div>
        )}
        {market.end_date && (
          <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 16 }}>
            Ends: {new Date(market.end_date).toLocaleDateString()}
          </div>
        )}

        {market.outcomes && market.outcomes.length > 0 && (
          <div style={{ marginBottom: 20 }}>
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Outcomes</h3>
            <div style={{ display: "flex", gap: 12 }}>
              {market.outcomes.map((o) => (
                <div
                  key={o.id}
                  style={{
                    background: "var(--bg)",
                    borderRadius: 6,
                    padding: "10px 16px",
                    flex: 1,
                  }}
                >
                  <div style={{ fontSize: 13, color: "var(--text-dim)", marginBottom: 4 }}>
                    {o.name}
                  </div>
                  <div style={{ fontSize: 18, fontFamily: "var(--mono)", fontWeight: 600 }}>
                    {o.latest_price != null
                      ? `${(parseFloat(o.latest_price) * 100).toFixed(1)}%`
                      : "—"}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <PriceChart marketId={id} />

        {snapshots.length > 0 && (
          <div style={{ marginBottom: 20 }}>
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>
              Recent Snapshots ({snapshots.length})
            </h3>
            <div className="table-scroll" style={{ maxHeight: 200, overflow: "auto" }}>
              <table
                style={{
                  width: "100%",
                  minWidth: 500,
                  borderCollapse: "collapse",
                  fontSize: 12,
                  fontFamily: "var(--mono)",
                }}
              >
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--border)" }}>
                    <th style={{ textAlign: "left", padding: 6, color: "var(--text-dim)" }}>
                      Time
                    </th>
                    <th style={{ textAlign: "right", padding: 6, color: "var(--text-dim)" }}>
                      Price
                    </th>
                    <th style={{ textAlign: "right", padding: 6, color: "var(--text-dim)" }}>
                      Vol 24h
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {snapshots.map((s, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
                      <td style={{ padding: 6 }}>{new Date(s.captured_at).toLocaleString()}</td>
                      <td style={{ textAlign: "right", padding: 6 }}>
                        {(parseFloat(s.price) * 100).toFixed(1)}%
                      </td>
                      <td style={{ textAlign: "right", padding: 6 }}>
                        {s.volume_24h ? `$${Number(s.volume_24h).toLocaleString()}` : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {signals && signals.signals.length > 0 && (
          <div>
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>
              Signals ({signals.total})
            </h3>
            {signals.signals.map((s) => (
              <Link
                key={s.id}
                to={`/signals/${s.id}`}
                style={{
                  display: "block",
                  textDecoration: "none",
                  color: "inherit",
                  padding: "8px 12px",
                  borderBottom: "1px solid var(--border)",
                  fontSize: 13,
                }}
              >
                <span style={{ color: "var(--accent)", marginRight: 8 }}>
                  {s.signal_type.replace("_", " ")}
                </span>
                <span style={{ fontFamily: "var(--mono)" }}>
                  rank: {Math.round(s.rank_score * 100)}%
                </span>
                <span style={{ float: "right", color: "var(--text-dim)" }}>
                  {new Date(s.fired_at).toLocaleString()}
                </span>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
