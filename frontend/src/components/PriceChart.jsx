import { useState, useEffect } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Bar,
  ComposedChart,
} from "recharts";
import { getChartData } from "../api";

const RANGES = ["1h", "6h", "24h", "7d"];
const COLORS = ["#00d4aa", "#ff6b6b", "#ffd93d", "#6c5ce7", "#00b894"];

export default function PriceChart({ marketId }) {
  const [range, setRange] = useState("24h");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    getChartData(marketId, range)
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [marketId, range]);

  if (loading) return <div className="skeleton" style={{ height: 300 }} />;
  if (error) return <div className="card" style={{ color: "var(--red)" }}>Chart error: {error}</div>;
  if (!data || !data.series) return null;

  // Merge all outcome series into unified time-indexed rows
  const outcomesNames = Object.keys(data.series);
  const timeMap = {};

  for (const name of outcomesNames) {
    for (const point of data.series[name]) {
      if (!timeMap[point.time]) {
        timeMap[point.time] = { time: point.time };
      }
      timeMap[point.time][name] = point.price;
      if (point.volume_24h != null) {
        timeMap[point.time]["volume"] = point.volume_24h;
      }
    }
  }

  const chartData = Object.values(timeMap).sort(
    (a, b) => new Date(a.time) - new Date(b.time)
  );

  if (chartData.length === 0) {
    return <div className="card">No chart data available for this time range.</div>;
  }

  const hasVolume = chartData.some((d) => d.volume != null);

  const formatTime = (iso) => {
    const d = new Date(iso);
    if (range === "1h" || range === "6h") {
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }
    return d.toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  };

  return (
    <div className="card" style={{ padding: "1rem" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.75rem" }}>
        <h3 style={{ margin: 0 }}>Price History</h3>
        <div style={{ display: "flex", gap: "0.25rem" }}>
          {RANGES.map((r) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              style={{
                padding: "0.25rem 0.5rem",
                background: range === r ? "var(--accent)" : "var(--bg-hover)",
                color: range === r ? "var(--bg)" : "var(--fg)",
                border: "none",
                borderRadius: 4,
                cursor: "pointer",
                fontSize: "0.75rem",
              }}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={chartData}>
          <XAxis dataKey="time" tickFormatter={formatTime} tick={{ fill: "var(--fg-muted)", fontSize: 11 }} />
          <YAxis
            yAxisId="price"
            domain={["auto", "auto"]}
            tick={{ fill: "var(--fg-muted)", fontSize: 11 }}
            tickFormatter={(v) => `${(v * 100).toFixed(0)}%`}
          />
          {hasVolume && (
            <YAxis yAxisId="volume" orientation="right" tick={{ fill: "var(--fg-muted)", fontSize: 11 }} />
          )}
          <Tooltip
            contentStyle={{ background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: 6 }}
            labelFormatter={formatTime}
            formatter={(value, name) =>
              name === "volume" ? [`$${value?.toLocaleString()}`, "Volume 24h"] : [`${(value * 100).toFixed(2)}%`, name]
            }
          />
          <Legend />
          {hasVolume && (
            <Bar yAxisId="volume" dataKey="volume" fill="var(--fg-muted)" opacity={0.2} name="Volume 24h" />
          )}
          {outcomesNames.map((name, i) => (
            <Line
              key={name}
              yAxisId="price"
              type="monotone"
              dataKey={name}
              stroke={COLORS[i % COLORS.length]}
              dot={false}
              strokeWidth={2}
              name={name}
            />
          ))}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
