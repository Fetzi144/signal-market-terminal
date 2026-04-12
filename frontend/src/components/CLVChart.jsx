import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";

function fmtCents(v) {
  if (v == null) return "\u2014";
  const cents = (v * 100).toFixed(1);
  return `${cents > 0 ? "+" : ""}${cents}\u00a2`;
}

function fmtDollar(v) {
  if (v == null) return "\u2014";
  return `$${v.toFixed(2)}`;
}

// ── CLV bar chart per detector ─────────────────────────────────────────────
export function CLVBarChart({ data }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No CLV data yet.</div>;
  }

  // Filter to detectors with CLV data and sort by avg_clv descending
  const chartData = data
    .filter((d) => d.avg_clv != null)
    .map((d) => ({
      name: d.signal_type.replace(/_/g, " "),
      signal_type: d.signal_type,
      avg_clv: d.avg_clv,
      avg_clv_cents: +(d.avg_clv * 100).toFixed(1),
      avg_pnl_cents: d.avg_profit_loss != null ? +(d.avg_profit_loss * 100).toFixed(1) : null,
      resolved: d.resolved,
    }))
    .sort((a, b) => b.avg_clv - a.avg_clv);

  if (chartData.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No CLV data yet. Run the backfill script after markets resolve.</div>;
  }

  return (
    <div style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, padding: "16px 8px 8px" }}>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData} margin={{ top: 4, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis
            dataKey="name"
            tick={{ fontSize: 10, fill: "var(--text-dim)" }}
            interval={0}
            angle={-20}
            textAnchor="end"
            height={50}
          />
          <YAxis
            tick={{ fontSize: 11, fill: "var(--text-dim)" }}
            tickFormatter={(v) => `${v > 0 ? "+" : ""}${v}\u00a2`}
            width={50}
          />
          <Tooltip
            formatter={(v, name) => {
              if (name === "avg_clv_cents") return [`${v > 0 ? "+" : ""}${v}\u00a2`, "Avg CLV"];
              return [v, name];
            }}
            labelFormatter={(l) => l.toUpperCase()}
            contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", fontSize: 12 }}
          />
          <ReferenceLine y={0} stroke="var(--text-dim)" strokeDasharray="4 4" />
          <Bar dataKey="avg_clv_cents" name="avg_clv_cents" radius={[3, 3, 0, 0]}>
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.avg_clv >= 0 ? "var(--green)" : "var(--red)"}
                fillOpacity={0.85}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textAlign: "center", marginTop: 4 }}>
        Average CLV per detector (cents). Positive = signal beat the closing line.
      </div>
    </div>
  );
}

// ── Signal quality table ───────────────────────────────────────────────────
export function SignalQualityTable({ data }) {
  if (!data || data.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No quality data yet.</div>;
  }

  const rows = data
    .filter((d) => d.avg_clv != null || d.avg_profit_loss != null)
    .sort((a, b) => (b.signal_quality_score || -999) - (a.signal_quality_score || -999));

  if (rows.length === 0) {
    return <div style={{ color: "var(--text-dim)", padding: 20 }}>No CLV data yet.</div>;
  }

  return (
    <div className="table-scroll" style={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
      <table style={{ width: "100%", minWidth: 700, borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
            <th style={thStyle}>Detector</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Win Rate</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Avg CLV</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Avg P&L</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Total P&L</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Profit Factor</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Quality Score</th>
            <th style={{ ...thStyle, textAlign: "right" }}>n</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.signal_type} style={{ borderBottom: "1px solid var(--border)" }}>
              <td style={{ padding: "10px 14px", fontWeight: 600, textTransform: "uppercase", fontSize: 11, color: "var(--accent)" }}>
                {row.signal_type.replace(/_/g, " ")}
              </td>
              <td style={{ ...tdRight, color: pnlColor(row.win_rate, 0.5) }}>
                {row.win_rate != null ? `${(row.win_rate * 100).toFixed(1)}%` : "\u2014"}
              </td>
              <td style={{ ...tdRight, color: pnlColor(row.avg_clv, 0) }}>
                {fmtCents(row.avg_clv)}
              </td>
              <td style={{ ...tdRight, color: pnlColor(row.avg_profit_loss, 0) }}>
                {fmtCents(row.avg_profit_loss)}
              </td>
              <td style={{ ...tdRight, color: pnlColor(row.total_profit_loss, 0) }}>
                {row.total_profit_loss != null ? fmtDollar(row.total_profit_loss) : "\u2014"}
              </td>
              <td style={{ ...tdRight, color: pnlColor(row.profit_factor, 1) }}>
                {row.profit_factor != null ? row.profit_factor.toFixed(2) : "\u2014"}
              </td>
              <td style={{ ...tdRight, fontWeight: 700, color: pnlColor(row.signal_quality_score, 0) }}>
                {row.signal_quality_score != null ? row.signal_quality_score.toFixed(3) : "\u2014"}
              </td>
              <td style={{ ...tdRight, color: "var(--text-dim)" }}>
                {row.resolved}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Hypothetical P&L hero card ────────────────────────────────────────────
export function PnLSummaryCards({ data }) {
  if (!data) return null;

  const cards = [
    {
      label: "Total P&L ($1/share)",
      value: data.hypothetical_pnl_per_share != null ? fmtDollar(data.hypothetical_pnl_per_share) : "\u2014",
      color: pnlColor(data.hypothetical_pnl_per_share, 0),
      sub: `${data.signals_with_clv || 0} signals with CLV data`,
    },
    {
      label: "Avg CLV",
      value: fmtCents(data.overall_avg_clv),
      color: pnlColor(data.overall_avg_clv, 0),
      sub: "signal vs closing line",
    },
    {
      label: "Avg P&L / Signal",
      value: fmtCents(data.overall_avg_profit_loss),
      color: pnlColor(data.overall_avg_profit_loss, 0),
      sub: "per resolved signal",
    },
    {
      label: "Profit Factor",
      value: data.overall_profit_factor != null ? data.overall_profit_factor.toFixed(2) : "\u2014",
      color: pnlColor(data.overall_profit_factor, 1),
      sub: "wins / losses ratio",
    },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
      {cards.map((c) => (
        <div key={c.label} style={{
          background: "var(--bg-card)", border: "1px solid var(--border)",
          borderRadius: 8, padding: "20px 24px",
        }}>
          <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
            {c.label}
          </div>
          <div style={{ fontSize: 28, fontFamily: "var(--mono)", fontWeight: 700, color: c.color || "var(--text)" }}>
            {c.value}
          </div>
          {c.sub && <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>{c.sub}</div>}
        </div>
      ))}
    </div>
  );
}

// ── Helpers ────────────────────────────────────────────────────────────────
const thStyle = { textAlign: "left", padding: "8px 14px", color: "var(--text-dim)", fontWeight: 500 };
const tdRight = { textAlign: "right", padding: "10px 14px", fontFamily: "var(--mono)" };

function pnlColor(val, neutral) {
  if (val == null) return "var(--text-dim)";
  if (val > neutral) return "var(--green)";
  if (val < neutral) return "var(--red)";
  return "var(--text-dim)";
}
