import { useState } from "react";

const SIGNAL_TYPES = [
  { name: "Price Move", desc: "Significant price change on an outcome within a timeframe" },
  { name: "Volume Spike", desc: "Trading volume surges above normal levels" },
  { name: "Spread Change", desc: "Bid-ask spread widens or narrows sharply" },
  { name: "Liquidity Vacuum", desc: "One side of the order book thins out" },
  { name: "Deadline Near", desc: "Market resolution date is approaching with active trading" },
  { name: "Confluence", desc: "Multiple timeframes confirm the same directional move" },
];

const BADGE_LEGEND = [
  { label: "PM", color: "#6366f1", meaning: "Polymarket" },
  { label: "KA", color: "#f59e0b", meaning: "Kalshi" },
];

const TIMEFRAME_LEGEND = [
  { label: "5m", color: "#94a3b8" },
  { label: "15m", color: "#94a3b8" },
  { label: "30m", color: "#60a5fa" },
  { label: "1h", color: "#a78bfa" },
  { label: "4h", color: "#f59e0b" },
  { label: "24h", color: "#ef4444" },
];

function StepIndicator({ current, total }) {
  return (
    <div style={{ display: "flex", gap: 8, justifyContent: "center", marginBottom: 24 }}>
      {Array.from({ length: total }, (_, i) => (
        <div
          key={i}
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: i === current ? "var(--accent)" : "var(--border)",
            transition: "background 0.2s",
          }}
        />
      ))}
    </div>
  );
}

function Step1() {
  return (
    <div>
      <h3 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>
        What is Signal Market Terminal?
      </h3>
      <p style={{ color: "var(--text-dim)", fontSize: 14, lineHeight: 1.6, marginBottom: 16 }}>
        SMT scans <strong style={{ color: "var(--text)" }}>Polymarket</strong> and{" "}
        <strong style={{ color: "var(--text)" }}>Kalshi</strong> prediction markets in real time,
        detecting trading signals like price moves, volume spikes, and liquidity shifts.
      </p>

      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Rank Formula</div>
        <div
          style={{
            background: "var(--bg)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            padding: "10px 14px",
            fontFamily: "var(--mono)",
            fontSize: 13,
          }}
        >
          rank = signal_score x confidence x recency_weight
        </div>
        <p style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 4 }}>
          Recency decays linearly from 1.0 (just fired) to 0.3 (24h old).
        </p>
      </div>

      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Platform Badges</div>
        <div style={{ display: "flex", gap: 12 }}>
          {BADGE_LEGEND.map((b) => (
            <div key={b.label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span
                style={{
                  fontSize: 10,
                  fontWeight: 700,
                  color: "#fff",
                  background: b.color,
                  padding: "1px 6px",
                  borderRadius: 4,
                }}
              >
                {b.label}
              </span>
              <span style={{ fontSize: 13, color: "var(--text-dim)" }}>{b.meaning}</span>
            </div>
          ))}
        </div>
      </div>

      <div>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Timeframe Colors</div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {TIMEFRAME_LEGEND.map((t) => (
            <span
              key={t.label}
              style={{
                fontSize: 10,
                fontWeight: 700,
                fontFamily: "var(--mono)",
                color: "#fff",
                background: t.color,
                padding: "1px 6px",
                borderRadius: 4,
              }}
            >
              {t.label}
            </span>
          ))}
        </div>
      </div>

      <div style={{ marginTop: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Signal Types</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {SIGNAL_TYPES.map((s) => (
            <div key={s.name} style={{ fontSize: 13 }}>
              <strong style={{ color: "var(--accent)" }}>{s.name}</strong>
              <span style={{ color: "var(--text-dim)" }}> — {s.desc}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Step2() {
  return (
    <div>
      <h3 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>
        How to Read a Signal Card
      </h3>
      <p style={{ color: "var(--text-dim)", fontSize: 14, lineHeight: 1.6, marginBottom: 16 }}>
        Each card represents one detected signal. Here is what the values mean:
      </p>

      {/* Mock signal card */}
      <div
        style={{
          background: "var(--bg)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: "14px 18px",
          marginBottom: 16,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
          <span
            style={{
              fontSize: 10, fontWeight: 700, color: "#fff",
              background: "#6366f1", padding: "1px 6px", borderRadius: 4,
            }}
          >
            PM
          </span>
          <span
            style={{
              fontSize: 10, fontWeight: 700, fontFamily: "var(--mono)",
              color: "#fff", background: "#a78bfa", padding: "1px 6px", borderRadius: 4,
            }}
          >
            1h
          </span>
          <span style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--accent)" }}>
            Price Move
          </span>
          <span style={{ color: "var(--green)", fontWeight: 600, fontSize: 13 }}>
            &uarr;
          </span>
          <span style={{ fontFamily: "var(--mono)", fontSize: 13 }}>4.2%</span>
        </div>
        <div style={{ fontSize: 14, marginBottom: 8, lineHeight: 1.4 }}>
          Will Bitcoin hit $100k by July?
          <span style={{ color: "var(--text-dim)" }}> &middot; Yes</span>
        </div>
        <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
          <span style={{ color: "var(--green)", fontFamily: "var(--mono)", fontSize: 13 }}>Str: 73%</span>
          <span style={{ color: "var(--yellow)", fontFamily: "var(--mono)", fontSize: 13 }}>Conf: 45%</span>
          <span style={{ color: "var(--green)", fontFamily: "var(--mono)", fontSize: 13 }}>Rank: 62%</span>
          <span style={{ fontSize: 12, color: "var(--text-dim)" }}>&bull; Pending</span>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {[
          { label: "Str (Strength)", desc: "Raw signal score — how strong the detected pattern is (0-100%)" },
          { label: "Conf (Confidence)", desc: "Statistical confidence that the signal is real, not noise" },
          { label: "Rank", desc: "Combined score used for sorting: strength x confidence x recency" },
        ].map((item) => (
          <div key={item.label} style={{ fontSize: 13 }}>
            <strong style={{ color: "var(--text)" }}>{item.label}</strong>
            <span style={{ color: "var(--text-dim)" }}> — {item.desc}</span>
          </div>
        ))}
      </div>

      <div style={{ marginTop: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Resolution Badges</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ fontSize: 13 }}>
            <span style={{ color: "var(--green)", fontWeight: 600 }}>&#10003; Called it</span>
            <span style={{ color: "var(--text-dim)" }}> — signal prediction was correct</span>
          </div>
          <div style={{ fontSize: 13 }}>
            <span style={{ color: "var(--red)", fontWeight: 600 }}>&#10007; Wrong call</span>
            <span style={{ color: "var(--text-dim)" }}> — signal prediction was incorrect</span>
          </div>
          <div style={{ fontSize: 13 }}>
            <span style={{ color: "var(--text-dim)" }}>&bull; Pending</span>
            <span style={{ color: "var(--text-dim)" }}> — not yet resolved</span>
          </div>
        </div>
      </div>

      <div style={{ marginTop: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Confluence</div>
        <p style={{ fontSize: 13, color: "var(--text-dim)" }}>
          When a signal fires in the same direction across multiple timeframes (e.g. 1h + 4h),
          it shows a green <strong style={{ color: "var(--green)" }}>Confirmed</strong> badge.
          This increases conviction.
        </p>
      </div>
    </div>
  );
}

function Step3({ onApplyFilters }) {
  return (
    <div>
      <h3 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>
        Recommended Filters
      </h3>
      <p style={{ color: "var(--text-dim)", fontSize: 14, lineHeight: 1.6, marginBottom: 16 }}>
        The default feed shows all signals unfiltered. For a less noisy start, we recommend
        focusing on higher-ranked signals on medium timeframes.
      </p>

      <div
        style={{
          background: "var(--bg)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          padding: 16,
          marginBottom: 20,
        }}
      >
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Suggested starting filters:</div>
        <ul style={{ fontSize: 13, color: "var(--text-dim)", paddingLeft: 20, lineHeight: 1.8 }}>
          <li>Timeframe: <strong style={{ color: "var(--text)" }}>1h</strong> or <strong style={{ color: "var(--text)" }}>4h</strong></li>
          <li>Focus on signals with <strong style={{ color: "var(--text)" }}>Rank &ge; 60%</strong></li>
          <li>Watch for <strong style={{ color: "var(--green)" }}>Confluence</strong> badges — they indicate multi-timeframe confirmation</li>
        </ul>
      </div>

      <div style={{ display: "flex", gap: 12 }}>
        <button
          onClick={() => onApplyFilters({ timeframe: "1h" })}
          style={{
            flex: 1,
            padding: "10px 16px",
            fontSize: 14,
            fontWeight: 600,
            background: "var(--accent)",
            color: "#fff",
            border: "none",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          Apply recommended filters
        </button>
        <button
          onClick={() => onApplyFilters(null)}
          style={{
            flex: 1,
            padding: "10px 16px",
            fontSize: 14,
            background: "transparent",
            color: "var(--text-dim)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          Show everything
        </button>
      </div>
    </div>
  );
}

export default function OnboardingWizard({ onComplete }) {
  const [step, setStep] = useState(0);
  const totalSteps = 3;

  function handleFinish(filters) {
    localStorage.setItem("smt-onboarded", "true");
    onComplete(filters);
  }

  return (
    <div className="wizard-overlay" onClick={() => handleFinish(null)}>
      <div
        className="wizard-content"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          className="wizard-close"
          onClick={() => handleFinish(null)}
          aria-label="Close wizard"
        >
          &times;
        </button>

        <StepIndicator current={step} total={totalSteps} />

        <div className="wizard-body">
          {step === 0 && <Step1 />}
          {step === 1 && <Step2 />}
          {step === 2 && <Step3 onApplyFilters={handleFinish} />}
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 24 }}>
          {step > 0 ? (
            <button
              onClick={() => setStep((s) => s - 1)}
              style={{
                padding: "8px 20px",
                fontSize: 13,
                background: "transparent",
                color: "var(--text-dim)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                cursor: "pointer",
              }}
            >
              Back
            </button>
          ) : (
            <button
              onClick={() => handleFinish(null)}
              style={{
                padding: "8px 20px",
                fontSize: 13,
                background: "transparent",
                color: "var(--text-dim)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                cursor: "pointer",
              }}
            >
              Skip
            </button>
          )}

          {step < totalSteps - 1 && (
            <button
              onClick={() => setStep((s) => s + 1)}
              style={{
                padding: "8px 20px",
                fontSize: 13,
                fontWeight: 600,
                background: "var(--accent)",
                color: "#fff",
                border: "none",
                borderRadius: 6,
                cursor: "pointer",
              }}
            >
              Next
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
