import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../api.js";
import { usePolling } from "../hooks/usePolling.js";
import { Card, Empty, Stat } from "./ui.jsx";

const PIPELINE = [
  { name: "payment-api", note: "Postgres + outbox" },
  { name: "outbox-relay", note: "transactions.raw" },
  { name: "feature-engine", note: "Redis state → transactions.enriched" },
  { name: "ml-scorer", note: "rules + XGBoost + SHAP → transactions.scored" },
  { name: "decision-service", note: "decisions + fraud.alerts" },
  { name: "dashboard-api", note: "WebSocket + REST" },
];

const LINKS = [
  ["Grafana", "http://localhost:3001"],
  ["Kafka UI", "http://localhost:8080"],
  ["Prometheus", "http://localhost:9090"],
  ["Payment API docs", "http://localhost:8000/docs"],
  ["Dashboard API docs", "http://localhost:8001/docs"],
];

export default function System() {
  const { data, error } = usePolling(() => api.system(), 3000);

  if (error && !data) return <Empty>Could not reach Kafka: {error}</Empty>;
  if (!data) return <Empty>Loading cluster state...</Empty>;

  const totalLag = data.consumer_groups.reduce((sum, g) => sum + (g.lag || 0), 0);
  const deadLetters = Object.values(data.dead_letters).reduce((a, b) => a + b, 0);

  return (
    <div className="stack">
      <div className="stats-row">
        <Stat label="Brokers" value={data.brokers} />
        <Stat label="Topics" value={data.topics.length} />
        <Stat label="Consumer groups" value={data.consumer_groups.length} />
        <Stat label="Total lag" value={totalLag.toLocaleString()} tone={totalLag > 500 ? "review" : "approve"} />
        <Stat label="Dead letters" value={deadLetters} tone={deadLetters ? "block" : "approve"} />
      </div>

      <Card title="Event pipeline">
        <div className="pipeline">
          {PIPELINE.map((step, i) => (
            <div key={step.name} className="pipe-step">
              <div className="pipe-box">
                <span className="mono">{step.name}</span>
                <span className="muted small">{step.note}</span>
              </div>
              {i < PIPELINE.length - 1 && <span className="pipe-arrow">→</span>}
            </div>
          ))}
        </div>
      </Card>

      <div className="grid-2">
        {data.consumer_groups.map((group) => (
          <Card
            key={group.group}
            title={group.group}
            action={
              <span className={`badge ${group.state === "STABLE" ? "badge-approve" : "badge-review"}`}>
                {group.state} · {group.members} member{group.members === 1 ? "" : "s"}
              </span>
            }
          >
            <div className="muted small">
              topic <span className="mono">{group.topic}</span> · lag{" "}
              <span className="mono">{group.lag ?? "n/a"}</span>
            </div>
            {group.partitions && group.partitions.length > 0 ? (
              <ResponsiveContainer width="100%" height={140}>
                <BarChart data={group.partitions}>
                  <XAxis dataKey="partition" stroke="var(--muted)" fontSize={11} />
                  <YAxis stroke="var(--muted)" fontSize={11} allowDecimals={false} />
                  <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--border)" }} />
                  <Bar dataKey="lag" fill="var(--accent)" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <Empty>{group.error || "No committed offsets yet"}</Empty>
            )}
          </Card>
        ))}
      </div>

      <div className="grid-2">
        <Card title="Dead letter topics">
          <table className="table compact">
            <tbody>
              {Object.entries(data.dead_letters).map(([topic, count]) => (
                <tr key={topic}>
                  <td className="mono">{topic}</td>
                  <td className="num mono">{count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
        <Card title="Tooling">
          <div className="links">
            {LINKS.map(([name, url]) => (
              <a key={name} href={url} target="_blank" rel="noreferrer">
                {name}
              </a>
            ))}
          </div>
          <div className="muted small topics">{data.topics.join(" · ")}</div>
        </Card>
      </div>
    </div>
  );
}
