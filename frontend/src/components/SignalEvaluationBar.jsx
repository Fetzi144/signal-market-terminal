const HORIZONS = ["15m", "1h", "4h", "24h"];

function getBadge(evaluations, horizon) {
  const ev = evaluations?.find((e) => e.horizon === horizon);
  if (!ev) return { color: "var(--fg-muted)", label: "?", title: `${horizon}: pending` };

  const pct = parseFloat(ev.price_change_pct);
  if (pct > 0) return { color: "var(--green)", label: "\u2191", title: `${horizon}: +${pct.toFixed(1)}%` };
  if (pct < 0) return { color: "var(--red)", label: "\u2193", title: `${horizon}: ${pct.toFixed(1)}%` };
  return { color: "var(--yellow)", label: "\u2192", title: `${horizon}: 0%` };
}

export default function SignalEvaluationBar({ evaluations }) {
  return (
    <div style={{ display: "flex", gap: "0.25rem", alignItems: "center" }}>
      {HORIZONS.map((h) => {
        const { color, label, title } = getBadge(evaluations, h);
        return (
          <span
            key={h}
            title={title}
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              width: 24,
              height: 24,
              borderRadius: 4,
              background: "var(--bg-hover)",
              color,
              fontSize: "0.75rem",
              fontWeight: 600,
              cursor: "default",
            }}
          >
            {label}
          </span>
        );
      })}
    </div>
  );
}
