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
          <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
            {new Date(s.fired_at).toLocaleString()}
          </span>
        </div>

        <h2 style={{ fontSize: 18, marginBottom: 8 }}>{s.market_question}</h2>

        <Link
          to={`/markets/${s.market_id}`}
          style={{ fontSize: 13, display: "inline-block", marginBottom: 16 }}
        >
          View market &rarr;
        </Link>

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
        </div>

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
            <table
              style={{
                width: "100%",
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
          </>
        )}
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
