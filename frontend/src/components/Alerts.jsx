import { useEffect, useState } from "react";
import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, formatInr, formatTime, titleCase } from "../api.js";
import { usePolling } from "../hooks/usePolling.js";
import { Card, DecisionBadge, Empty, RiskBar } from "./ui.jsx";

const FEATURE_LABELS = {
  amount_to_avg_ratio: "Amount vs average",
  txn_count_1m: "Txns in 1 min",
  txn_count_10m: "Txns in 10 min",
  amount_sum_10m: "Spent in 10 min",
  seconds_since_last_txn: "Seconds since last",
  distance_from_last_km: "Distance from last (km)",
  speed_kmh: "Implied speed (km/h)",
  is_new_device: "New device",
  is_new_payee: "New payee",
  account_age_days: "Account age (days)",
  user_txn_count: "Prior transactions",
  is_night: "Night time",
};

function AlertDetail({ id, onLabelled }) {
  const [detail, setDetail] = useState(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  useEffect(() => {
    setDetail(null);
    setMessage("");
    api.alert(id).then(setDetail).catch((err) => setMessage(err.message));
  }, [id]);

  const label = async (value) => {
    setBusy(true);
    try {
      await api.feedback(id, value, "analyst-console");
      setMessage(`Marked as ${value}. Published to fraud.feedback`);
      setDetail((d) => ({ ...d, status: "RESOLVED", label: value }));
      onLabelled();
    } catch (err) {
      setMessage(err.message);
    } finally {
      setBusy(false);
    }
  };

  if (!detail) return <Empty>{message || "Loading case..."}</Empty>;

  const reasons = detail.reasons.map((r) => ({ name: r.text, value: r.contribution }));
  const scoreBars = [
    { name: "Rules", value: detail.rule_score },
    { name: "XGBoost", value: detail.ml_score },
    { name: "Anomaly", value: detail.anomaly_score },
    { name: "Final risk", value: detail.risk_score },
  ];

  return (
    <div className="detail">
      <div className="detail-head">
        <div>
          <div className="muted small mono">{detail.txn_id}</div>
          <h2>
            {formatInr(detail.amount)} <span className="muted">to</span> {detail.payee_id}
          </h2>
          <div className="muted">
            {detail.user_id} · {detail.city} · {titleCase(detail.category)} · {detail.device_id}
          </div>
        </div>
        <div className="detail-badges">
          <DecisionBadge decision={detail.decision} />
          {detail.ground_truth && <span className="badge badge-truth">sim: {titleCase(detail.ground_truth)}</span>}
          {detail.label && <span className="badge badge-label">analyst: {detail.label}</span>}
        </div>
      </div>

      <div className="grid-2">
        <Card title="Score breakdown">
          <ResponsiveContainer width="100%" height={170}>
            <BarChart data={scoreBars} layout="vertical" margin={{ left: 20, right: 20 }}>
              <XAxis type="number" domain={[0, 1]} stroke="var(--muted)" fontSize={11} />
              <YAxis type="category" dataKey="name" stroke="var(--muted)" fontSize={12} width={80} />
              <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--border)" }} />
              <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                {scoreBars.map((b) => (
                  <Cell key={b.name} fill={b.name === "Final risk" ? "var(--accent)" : "var(--accent-soft)"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Why was this flagged? (SHAP)">
          {reasons.length === 0 ? (
            <Empty>No positive model contributions</Empty>
          ) : (
            <ul className="reasons">
              {reasons.map((r) => (
                <li key={r.name}>
                  <span>{r.name}</span>
                  <span className="mono muted">+{r.value.toFixed(2)}</span>
                </li>
              ))}
            </ul>
          )}
          {detail.rules.length > 0 && (
            <div className="rules">
              {detail.rules.map((rule) => (
                <div key={rule.code} className="rule">
                  <span className="mono">{rule.code}</span>
                  <span className="muted small">{rule.description}</span>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      <Card title="Stream features at decision time">
        <div className="features">
          {Object.entries(FEATURE_LABELS).map(([key, name]) => (
            <div key={key} className="feature">
              <span className="muted small">{name}</span>
              <span className="mono">{Number(detail.features[key] ?? 0).toLocaleString("en-IN", { maximumFractionDigits: 1 })}</span>
            </div>
          ))}
        </div>
      </Card>

      <Card title="Recent activity for this user">
        <table className="table compact">
          <tbody>
            {detail.user_history.map((h) => (
              <tr key={h.txn_id} className={h.txn_id === detail.txn_id ? "current" : ""}>
                <td className="mono">{formatTime(h.decided_at)}</td>
                <td className="num">{formatInr(h.amount)}</td>
                <td>{h.city}</td>
                <td className="mono small">{h.device_id}</td>
                <td>
                  <DecisionBadge decision={h.decision} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>

      <div className="actions">
        <button className="btn btn-block" disabled={busy || detail.status === "RESOLVED"} onClick={() => label("FRAUD")}>
          Confirm fraud
        </button>
        <button className="btn btn-approve" disabled={busy || detail.status === "RESOLVED"} onClick={() => label("LEGIT")}>
          Mark legitimate
        </button>
        <span className="muted small">{message}</span>
      </div>
    </div>
  );
}

export default function Alerts({ selected, onSelect }) {
  const [status, setStatus] = useState("OPEN");
  const [decision, setDecision] = useState("ALL");
  const [refreshKey, setRefreshKey] = useState(0);
  const { data: alerts } = usePolling(() => api.alerts(status, decision), 3000, [status, decision, refreshKey]);

  return (
    <div className="split">
      <Card
        title={`Alert queue ${alerts ? `(${alerts.length})` : ""}`}
        className="queue"
        action={
          <div className="filters">
            <select value={status} onChange={(e) => setStatus(e.target.value)}>
              <option value="OPEN">Open</option>
              <option value="RESOLVED">Resolved</option>
              <option value="ALL">All</option>
            </select>
            <select value={decision} onChange={(e) => setDecision(e.target.value)}>
              <option value="ALL">Review + Block</option>
              <option value="REVIEW">Review</option>
              <option value="BLOCK">Block</option>
            </select>
          </div>
        }
      >
        {!alerts || alerts.length === 0 ? (
          <Empty>No alerts. Launch an attack from the header.</Empty>
        ) : (
          <ul className="alert-list">
            {alerts.map((a) => (
              <li key={a.txn_id} className={a.txn_id === selected ? "active" : ""} onClick={() => onSelect(a.txn_id)}>
                <div className="alert-top">
                  <DecisionBadge decision={a.decision} />
                  <span className="num">{formatInr(a.amount)}</span>
                </div>
                <div className="small">{a.reasons[0]?.text || a.rule_hits[0] || "Model risk"}</div>
                <div className="alert-meta muted small">
                  <span className="mono">{a.user_id}</span>
                  <span>{a.city}</span>
                  <span>{formatTime(a.decided_at)}</span>
                </div>
                <RiskBar value={a.risk_score} />
              </li>
            ))}
          </ul>
        )}
      </Card>
      <Card className="case">
        {selected ? (
          <AlertDetail id={selected} onLabelled={() => setRefreshKey((k) => k + 1)} />
        ) : (
          <Empty>Select an alert to review the case</Empty>
        )}
      </Card>
    </div>
  );
}
