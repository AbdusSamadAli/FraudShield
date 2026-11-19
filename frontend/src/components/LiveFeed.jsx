import { formatInr, formatTime, titleCase } from "../api.js";
import { Card, DecisionBadge, Empty, RiskBar, Stat } from "./ui.jsx";

export default function LiveFeed({ live, onSelect }) {
  const { rows, rate } = live;
  const flagged = rate.REVIEW + rate.BLOCK;

  return (
    <div className="stack">
      <div className="stats-row">
        <Stat label="Throughput" value={`${rate.tps.toFixed(1)}/s`} hint="last 10 seconds" />
        <Stat label="Decisions (60s)" value={rate.total} />
        <Stat label="Approved" value={rate.APPROVE} tone="approve" />
        <Stat label="In review" value={rate.REVIEW} tone="review" />
        <Stat label="Blocked" value={rate.BLOCK} tone="block" />
        <Stat
          label="Avg latency"
          value={`${rate.avgLatency.toFixed(0)} ms`}
          hint={rate.total ? `${((flagged / rate.total) * 100).toFixed(1)}% flagged` : "waiting"}
        />
      </div>

      <Card title="Live transaction stream" action={<span className="muted">newest first, streamed over WebSocket</span>}>
        {rows.length === 0 ? (
          <Empty>Waiting for transactions. Is the simulator running?</Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>User</th>
                  <th>Amount</th>
                  <th>Payee</th>
                  <th>City</th>
                  <th>Risk</th>
                  <th>Decision</th>
                  <th>Top reason</th>
                  <th>Latency</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr
                    key={row.txn_id}
                    className={`row-${row.decision.toLowerCase()} ${row.decision !== "APPROVE" ? "clickable" : ""}`}
                    onClick={() => row.decision !== "APPROVE" && onSelect(row.txn_id)}
                  >
                    <td className="mono">{formatTime(row.decided_at)}</td>
                    <td className="mono">{row.user_id}</td>
                    <td className="num">{formatInr(row.amount)}</td>
                    <td>
                      <span className="muted small">{row.payee_type === "P2P" ? "P2P" : titleCase(row.category)}</span>
                    </td>
                    <td>{row.city}</td>
                    <td>
                      <RiskBar value={row.risk_score} />
                    </td>
                    <td>
                      <DecisionBadge decision={row.decision} />
                    </td>
                    <td className="reason">{row.decision !== "APPROVE" ? row.reasons[0] || row.rule_hits[0] : ""}</td>
                    <td className="mono num">{row.latency_ms.toFixed(0)} ms</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
