import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getSignal } from "../api";

export default function SignalDetail() {
  const { id } = useParams();
  const [signal, setSignal] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    getSignal(id).then(setSignal).catch((e) => setError(e.message));
  }, [id]);

  if (error) return <div style={{ color: "var(--red)" }}>Error: {error}</div>;
  if (!signal) return <div style={{ color: "var(--text-dim)" }}>Loading...</div>;

  const s = signal;
  const d = s.details || {};

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
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span
              style={{
                fontSize: 12,
                fontWeight: 600,
                textTransform: "uppercase",
                color: "var(--accent)",
              }}
            >
              {s.signal_type.replace("_", " ")}
            </span>
            <span
              style={{
                fontSize: 10, fontWeight: 700, fontFamily: "var(--mono)",
                color: "#fff", background: "#60a5fa",
                padding: "1px 6px", borderRadius: 4,
              }}
            >
              {s.timeframe || "30m"}
            </span>
            {d.confluence_timeframes && d.confluence_timeframes.length >= 2 && (
              <span
                style={{
                  fontSize: 11, fontWeight: 600, color: "var(--green)",
                  background: "rgba(34,197,94,0.12)", padding: "2px 8px",
                  borderRadius: 4,
                }}
              >
                Confirmed: {d.confluence_timeframes.join(" + ")}
              </span>
            )}
          </div>
          <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
            {new Date(s.fired_at).toLocaleString()}
          </span>
        </div>

        <h2 style={{ fontSize: 18, marginBottom: 8 }}>{s.market_question}</h2>

        <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
          <Link
            to={`/markets/${s.market_id}`}
            style={{ fontSize: 13 }}
          >
            View market &rarr;
          </Link>
          <Link
            to={`/portfolio?signal_id=${s.id}&market_id=${s.market_id}${s.outcome_id ? `&outcome_id=${s.outcome_id}` : ""}${d.direction === "down" || d.direction === "sell" ? "&side=no" : "&side=yes"}`}
            style={{
              fontSize: 12,
              fontWeight: 600,
              padding: "4px 12px",
              background: "var(--accent)",
              color: "#fff",
              borderRadius: 6,
              textDecoration: "none",
            }}
          >
            Track Position
          </Link>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
            gap: 12,
            marginBottom: 20,
          }}
        >
          <Stat label="Signal Score" value={`${Math.round(s.signal_score * 100)}%`} />
          <Stat label="Confidence" value={`${Math.round(s.confidence * 100)}%`} />
          <Stat label="Rank Score" value={`${Math.round(s.rank_score * 100)}%`} />
          {s.price_at_fire && <Stat label="Price at Fire" value={`$${s.price_at_fire}`} />}
          <Stat label="Resolved" value={s.resolved ? "Yes" : "No"} />
          <div
            style={{
              background: "var(--bg)",
              borderRadius: 6,
              padding: "10px 14px",
            }}
          >
            <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Resolution</div>
            <div style={{ fontSize: 15, fontFamily: "var(--mono)", fontWeight: 600 }}>
              {s.resolved_correctly === true && (
                <span style={{ color: "var(--green)" }}>&#10003; Called it</span>
              )}
              {s.resolved_correctly === false && (
                <span style={{ color: "var(--red)" }}>&#10007; Wrong call</span>
              )}
              {s.resolved_correctly == null && (
                <span style={{ color: "var(--text-dim)" }}>&#8226; Pending</span>
              )}
            </div>
          </div>
        </div>

        {s.signal_type === "order_flow_imbalance" && (
          <div
            style={{
              background: "var(--bg)",
              border: "1px solid var(--accent)",
              borderRadius: 8,
              padding: 16,
              marginBottom: 20,
            }}
          >
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12, color: "var(--accent)" }}>
              Order Flow Imbalance
            </h3>
            <OfiBar ofi={parseFloat(d.ofi_value || 0)} />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 12,
                marginTop: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Bid Depth</div>
                <div style={{ fontSize: 16, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--green)" }}>
                  {d.bid_depth_current || "—"}
                </div>
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                  prev: {d.bid_depth_previous || "—"}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Ask Depth</div>
                <div style={{ fontSize: 16, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--red)" }}>
                  {d.ask_depth_current || "—"}
                </div>
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                  prev: {d.ask_depth_previous || "—"}
                </div>
              </div>
            </div>
            <div
              style={{
                marginTop: 12,
                padding: "8px 12px",
                background: "var(--bg-card)",
                borderRadius: 6,
                fontSize: 13,
                color: "var(--accent)",
              }}
            >
              Price flat — OFI suggests move {d.direction === "up" ? "upward" : "downward"} incoming
            </div>
          </div>
        )}

        {s.signal_type === "smart_money" && (
          <div
            style={{
              background: "var(--bg)",
              border: "1px solid var(--accent)",
              borderRadius: 8,
              padding: 16,
              marginBottom: 20,
            }}
          >
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12, color: "var(--accent)" }}>
              Smart Money / Whale Trade
            </h3>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 12,
                marginBottom: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Wallet</div>
                <a
                  href={`https://polygonscan.com/address/${d.wallet_address}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ fontSize: 13, fontFamily: "var(--mono)", color: "var(--accent)" }}
                >
                  {d.wallet_label || `${(d.wallet_address || "").slice(0, 6)}...${(d.wallet_address || "").slice(-4)}`}
                </a>
              </div>
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Action</div>
                <div
                  style={{
                    fontSize: 15,
                    fontFamily: "var(--mono)",
                    fontWeight: 700,
                    color: d.action === "buy" ? "var(--green)" : "var(--red)",
                    textTransform: "uppercase",
                  }}
                >
                  {d.action || "—"}
                </div>
              </div>
            </div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr 1fr",
                gap: 12,
                marginBottom: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Notional</div>
                <div style={{ fontSize: 16, fontFamily: "var(--mono)", fontWeight: 700 }}>
                  ${d.notional_usd ? Number(d.notional_usd).toLocaleString() : "—"}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Win Rate</div>
                <div style={{ fontSize: 16, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--green)" }}>
                  {d.wallet_win_rate ? `${(Number(d.wallet_win_rate) * 100).toFixed(1)}%` : "N/A"}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>Total Volume</div>
                <div style={{ fontSize: 16, fontFamily: "var(--mono)", fontWeight: 700 }}>
                  ${d.wallet_total_volume ? Number(d.wallet_total_volume).toLocaleString() : "—"}
                </div>
              </div>
            </div>
            {d.tx_hash && (
              <a
                href={`https://polygonscan.com/tx/${d.tx_hash}`}
                target="_blank"
                rel="noopener noreferrer"
                style={{ fontSize: 12, color: "var(--accent)" }}
              >
                View transaction on Polygonscan &rarr;
              </a>
            )}
          </div>
        )}

        {s.signal_type === "arbitrage" && (
          <div
            style={{
              background: "var(--bg)",
              border: "1px solid var(--accent)",
              borderRadius: 8,
              padding: 16,
              marginBottom: 20,
            }}
          >
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12, color: "var(--accent)" }}>
              Cross-Platform Arbitrage
            </h3>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr auto 1fr",
                gap: 12,
                alignItems: "center",
                textAlign: "center",
              }}
            >
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>
                  {d.buy_platform?.toUpperCase() || "BUY"}
                </div>
                <div style={{ fontSize: 20, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--green)" }}>
                  ${d[`${d.buy_platform}_price`] || d.polymarket_price || "—"}
                </div>
              </div>
              <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
                <div>spread</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: "var(--accent)" }}>
                  {d.spread_pct || "—"}pp
                </div>
              </div>
              <div>
                <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>
                  {d.sell_platform?.toUpperCase() || "SELL"}
                </div>
                <div style={{ fontSize: 20, fontFamily: "var(--mono)", fontWeight: 700, color: "var(--red)" }}>
                  ${d[`${d.sell_platform}_price`] || d.kalshi_price || "—"}
                </div>
              </div>
            </div>
          </div>
        )}

        <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Details</h3>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: 8,
            marginBottom: 20,
          }}
        >
          {Object.entries(d).map(([key, value]) => (
            <DetailItem key={key} label={key} value={value} />
          ))}
        </div>

        {s.evaluations && s.evaluations.length > 0 && (
          <>
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Evaluations</h3>
            <div className="table-scroll" style={{ overflowX: "auto" }}>
            <table
              style={{
                width: "100%",
                minWidth: 500,
                borderCollapse: "collapse",
                fontSize: 13,
                fontFamily: "var(--mono)",
              }}
            >
              <thead>
                <tr style={{ borderBottom: "1px solid var(--border)" }}>
                  <th style={{ textAlign: "left", padding: 8, color: "var(--text-dim)" }}>
                    Horizon
                  </th>
                  <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>
                    Price
                  </th>
                  <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>
                    Change
                  </th>
                  <th style={{ textAlign: "right", padding: 8, color: "var(--text-dim)" }}>
                    Change %
                  </th>
                </tr>
              </thead>
              <tbody>
                {s.evaluations.map((ev) => (
                  <tr key={ev.horizon} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td style={{ padding: 8 }}>{ev.horizon}</td>
                    <td style={{ textAlign: "right", padding: 8 }}>{ev.price_at_eval}</td>
                    <td style={{ textAlign: "right", padding: 8 }}>{ev.price_change}</td>
                    <td
                      style={{
                        textAlign: "right",
                        padding: 8,
                        color:
                          ev.price_change_pct > 0
                            ? "var(--green)"
                            : ev.price_change_pct < 0
                            ? "var(--red)"
                            : "var(--text)",
                      }}
                    >
                      {ev.price_change_pct}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function OfiBar({ ofi }) {
  // ofi ranges from -1 to 1; negative = sell pressure (red/left), positive = buy pressure (green/right)
  const pct = Math.min(Math.max(ofi, -1), 1) * 50; // -50 to +50
  const barColor = ofi > 0 ? "var(--green)" : "var(--red)";
  const label = ofi > 0 ? "BUY PRESSURE" : "SELL PRESSURE";
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>
        <span>Sell</span>
        <span style={{ fontFamily: "var(--mono)", fontWeight: 600, color: barColor }}>{label} ({(ofi * 100).toFixed(1)}%)</span>
        <span>Buy</span>
      </div>
      <div style={{ position: "relative", height: 16, background: "var(--bg-card)", borderRadius: 8, overflow: "hidden" }}>
        <div style={{ position: "absolute", left: "50%", top: 0, bottom: 0, width: 1, background: "var(--border)" }} />
        <div
          style={{
            position: "absolute",
            top: 2,
            bottom: 2,
            borderRadius: 6,
            background: barColor,
            ...(pct > 0
              ? { left: "50%", width: `${pct}%` }
              : { right: "50%", width: `${-pct}%` }),
          }}
        />
      </div>
    </div>
  );
}

const DETAIL_LABELS = {
  direction: "Direction",
  outcome_name: "Outcome",
  market_question: "Market",
  change_pct: "Change %",
  multiplier: "Volume Multiplier",
  ratio: "Spread Ratio",
  vacuum_side: "Vacuum Side",
  hours_until_deadline: "Hours to Deadline",
  baseline_avg: "Baseline Avg",
  current_value: "Current Value",
  window_minutes: "Window (min)",
  question_slug: "Question Slug",
  spread: "Spread",
  spread_pct: "Spread (pp)",
  buy_platform: "Buy On",
  sell_platform: "Sell On",
  polymarket_price: "Polymarket Price",
  kalshi_price: "Kalshi Price",
  wallet_address: "Wallet Address",
  wallet_label: "Wallet Label",
  wallet_win_rate: "Win Rate",
  wallet_total_volume: "Total Volume",
  wallet_trade_count: "Trade Count",
  notional_usd: "Notional (USD)",
  tx_hash: "Transaction Hash",
  ofi_value: "OFI Value",
  bid_depth_current: "Bid Depth (Current)",
  ask_depth_current: "Ask Depth (Current)",
  bid_depth_previous: "Bid Depth (Previous)",
  ask_depth_previous: "Ask Depth (Previous)",
  price_current: "Price (Current)",
};

function DetailItem({ label, value }) {
  const displayLabel = DETAIL_LABELS[label] || label.replace(/_/g, " ");
  const displayValue = typeof value === "object" ? JSON.stringify(value) : String(value ?? "N/A");
  return (
    <div style={{ background: "var(--bg)", borderRadius: 6, padding: "10px 14px" }}>
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4, textTransform: "capitalize" }}>
        {displayLabel}
      </div>
      <div style={{ fontSize: 14, fontFamily: "var(--mono)", fontWeight: 500 }}>
        {displayValue}
      </div>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div
      style={{
        background: "var(--bg)",
        borderRadius: 6,
        padding: "10px 14px",
      }}
    >
      <div style={{ fontSize: 11, color: "var(--text-dim)", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 15, fontFamily: "var(--mono)", fontWeight: 600 }}>{value}</div>
    </div>
  );
}
