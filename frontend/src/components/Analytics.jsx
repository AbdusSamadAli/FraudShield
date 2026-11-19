import { useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api, formatInr, formatTime, titleCase } from "../api.js";
import { usePolling } from "../hooks/usePolling.js";
import { Card, Empty, Stat } from "./ui.jsx";

const tooltipStyle = { background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 8 };
const pct = (v) => (v === null || v === undefined ? "n/a" : `${(v * 100).toFixed(1)}%`);

function ModelCard({ model }) {
  if (!model) return <Empty>Model metadata unavailable</Empty>;
  const rows = [
    ["Rules + XGBoost + iForest", model.metrics.combined],
    ["XGBoost only", model.metrics.xgboost_only],
    ["Isolation Forest only", model.metrics.isolation_forest_only],
    ["Rules only", model.metrics.rules_only],
  ];
  const importance = Object.entries(model.feature_importance)
    .slice(0, 8)
    .map(([name, value]) => ({ name: titleCase(name), value: Math.round(value * 1000) / 10 }));

  return (
    <div className="stack">
      <div className="muted small">
        {model.version} · trained on {model.dataset.train_rows.toLocaleString()} events · tested on{" "}
        {model.dataset.test_rows.toLocaleString()} ({model.dataset.split})
      </div>
      <table className="table compact">
        <thead>
          <tr>
            <th>Approach</th>
            <th>PR-AUC</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>F1</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([name, m]) => (
            <tr key={name}>
              <td>{name}</td>
              <td className="mono">{m.pr_auc.toFixed(3)}</td>
              <td className="mono">{pct(m.precision)}</td>
              <td className="mono">{pct(m.recall)}</td>
              <td className="mono">{m.f1.toFixed(3)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="grid-2">
        <div>
          <div className="chart-title">Precision-recall curve</div>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={model.pr_curve}>
              <CartesianGrid stroke="var(--grid)" />
              <XAxis dataKey="recall" type="number" domain={[0, 1]} stroke="var(--muted)" fontSize={11} />
              <YAxis domain={[0, 1]} stroke="var(--muted)" fontSize={11} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line dataKey="precision" stroke="var(--accent)" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div>
          <div className="chart-title">Feature importance (gain %)</div>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={importance} layout="vertical" margin={{ left: 40 }}>
              <XAxis type="number" stroke="var(--muted)" fontSize={11} />
              <YAxis type="category" dataKey="name" stroke="var(--muted)" fontSize={11} width={120} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="value" fill="var(--accent)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

export default function Analytics() {
  const [minutes, setMinutes] = useState(15);
  const { data: stats } = usePolling(() => api.stats(minutes), 5000, [minutes]);
  const { data: model } = usePolling(() => api.model(), 60000);

  if (!stats) return <Empty>Loading analytics...</Empty>;
  const s = stats.summary;
  const series = stats.timeseries.map((p) => ({ ...p, time: formatTime(p.minute).slice(0, 5) }));
  const patterns = stats.patterns.map((p) => ({ name: titleCase(p.pattern), caught: p.caught, missed: p.total - p.caught }));

  return (
    <div className="stack">
      <div className="toolbar">
        <span className="muted">Window</span>
        {[5, 15, 60, 240].map((m) => (
          <button key={m} className={`chip ${m === minutes ? "active" : ""}`} onClick={() => setMinutes(m)}>
            {m < 60 ? `${m}m` : `${m / 60}h`}
          </button>
        ))}
      </div>
      <div className="stats-row">
        <Stat label="Transactions" value={s.total.toLocaleString()} hint={`${s.throughput_per_sec}/s now`} />
        <Stat label="Blocked value" value={formatInr(s.blocked_amount)} tone="block" hint={`${s.blocked} payments`} />
        <Stat label="Sent to review" value={s.review} tone="review" />
        <Stat label="Live precision" value={pct(s.precision)} hint="vs simulator ground truth" />
        <Stat label="Live recall" value={pct(s.recall)} hint={`${s.false_negative} missed`} />
        <Stat label="p95 latency" value={`${Number(s.p95_latency_ms).toFixed(0)} ms`} hint={`avg ${Number(s.avg_latency_ms).toFixed(0)} ms`} />
      </div>

      <div className="grid-2">
        <Card title="Volume per minute">
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={series}>
              <CartesianGrid stroke="var(--grid)" />
              <XAxis dataKey="time" stroke="var(--muted)" fontSize={11} />
              <YAxis stroke="var(--muted)" fontSize={11} />
              <Tooltip contentStyle={tooltipStyle} />
              <Area dataKey="approved" stroke="var(--approve)" fill="var(--approve)" fillOpacity={0.15} />
            </AreaChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Flagged per minute">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={series}>
              <CartesianGrid stroke="var(--grid)" />
              <XAxis dataKey="time" stroke="var(--muted)" fontSize={11} />
              <YAxis stroke="var(--muted)" fontSize={11} allowDecimals={false} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend />
              <Bar dataKey="review" stackId="a" fill="var(--review)" />
              <Bar dataKey="blocked" stackId="a" fill="var(--block)" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Fraud patterns caught">
          {patterns.length === 0 ? (
            <Empty>No fraud in this window yet</Empty>
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={patterns} layout="vertical" margin={{ left: 50 }}>
                <XAxis type="number" stroke="var(--muted)" fontSize={11} allowDecimals={false} />
                <YAxis type="category" dataKey="name" stroke="var(--muted)" fontSize={11} width={140} />
                <Tooltip contentStyle={tooltipStyle} />
                <Legend />
                <Bar dataKey="caught" stackId="p" fill="var(--approve)" />
                <Bar dataKey="missed" stackId="p" fill="var(--block)" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </Card>
        <Card title="Rule hits">
          {stats.rule_hits.length === 0 ? (
            <Empty>No rules fired in this window</Empty>
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={stats.rule_hits} layout="vertical" margin={{ left: 70 }}>
                <XAxis type="number" stroke="var(--muted)" fontSize={11} allowDecimals={false} />
                <YAxis type="category" dataKey="rule" stroke="var(--muted)" fontSize={10} width={200} />
                <Tooltip contentStyle={tooltipStyle} />
                <Bar dataKey="hits" fill="var(--accent)" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </Card>
      </div>

      <Card title="Model card (offline evaluation)" action={<span className="muted small">Analyst labels: {stats.labels.FRAUD || 0} fraud · {stats.labels.LEGIT || 0} legit</span>}>
        <ModelCard model={model} />
      </Card>
    </div>
  );
}
