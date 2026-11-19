export const DECISION_COLORS = {
  APPROVE: "var(--approve)",
  REVIEW: "var(--review)",
  BLOCK: "var(--block)",
};

export function Card({ title, action, children, className = "" }) {
  return (
    <section className={`card ${className}`}>
      {(title || action) && (
        <header className="card-header">
          <h3>{title}</h3>
          {action}
        </header>
      )}
      <div className="card-body">{children}</div>
    </section>
  );
}

export function Stat({ label, value, hint, tone }) {
  return (
    <div className={`stat ${tone ? `stat-${tone}` : ""}`}>
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value}</span>
      {hint && <span className="stat-hint">{hint}</span>}
    </div>
  );
}

export function DecisionBadge({ decision }) {
  return <span className={`badge badge-${(decision || "").toLowerCase()}`}>{decision}</span>;
}

export function RiskBar({ value }) {
  const pct = Math.round((value || 0) * 100);
  const color = value >= 0.8 ? "var(--block)" : value >= 0.5 ? "var(--review)" : "var(--approve)";
  return (
    <div className="risk">
      <div className="risk-track">
        <div className="risk-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="mono">{pct}</span>
    </div>
  );
}

export function Empty({ children }) {
  return <div className="empty">{children}</div>;
}
