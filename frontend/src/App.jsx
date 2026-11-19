import { useEffect, useState } from "react";
import { api, titleCase } from "./api.js";
import Alerts from "./components/Alerts.jsx";
import Analytics from "./components/Analytics.jsx";
import LiveFeed from "./components/LiveFeed.jsx";
import System from "./components/System.jsx";
import { useLiveFeed } from "./hooks/useLiveFeed.js";

const TABS = [
  ["live", "Live feed"],
  ["alerts", "Alerts & cases"],
  ["analytics", "Analytics"],
  ["system", "System health"],
];

function AttackControl() {
  const [patterns, setPatterns] = useState([]);
  const [pattern, setPattern] = useState("card_testing");
  const [count, setCount] = useState(3);
  const [status, setStatus] = useState("");

  useEffect(() => {
    api.patterns().then(setPatterns).catch(() => setPatterns([]));
  }, []);

  const launch = async () => {
    setStatus("launching...");
    try {
      await api.attack(pattern, count);
      setStatus(`${count} × ${titleCase(pattern)} sent to sim.control`);
    } catch (err) {
      setStatus(err.message);
    }
    setTimeout(() => setStatus(""), 4000);
  };

  return (
    <div className="attack">
      <select value={pattern} onChange={(e) => setPattern(e.target.value)}>
        {patterns.map((p) => (
          <option key={p} value={p}>
            {titleCase(p)}
          </option>
        ))}
      </select>
      <select value={count} onChange={(e) => setCount(Number(e.target.value))}>
        {[1, 3, 5, 10].map((n) => (
          <option key={n} value={n}>
            × {n}
          </option>
        ))}
      </select>
      <button className="btn btn-block" onClick={launch}>
        Launch attack
      </button>
      {status && <span className="muted small">{status}</span>}
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("live");
  const [selected, setSelected] = useState(null);
  const live = useLiveFeed();

  const openCase = (id) => {
    setSelected(id);
    setTab("alerts");
  };

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">◆</span>
          <div>
            <div className="brand-name">FraudShield</div>
            <div className="muted small">Real-time UPI fraud detection on Kafka</div>
          </div>
        </div>
        <nav className="tabs">
          {TABS.map(([id, label]) => (
            <button key={id} className={`tab ${tab === id ? "active" : ""}`} onClick={() => setTab(id)}>
              {label}
            </button>
          ))}
        </nav>
        <div className="topbar-right">
          <AttackControl />
          <span className={`dot ${live.connected ? "on" : "off"}`} title={live.connected ? "stream connected" : "reconnecting"} />
        </div>
      </header>
      <main className="content">
        {tab === "live" && <LiveFeed live={live} onSelect={openCase} />}
        {tab === "alerts" && <Alerts selected={selected} onSelect={setSelected} />}
        {tab === "analytics" && <Analytics />}
        {tab === "system" && <System />}
      </main>
    </div>
  );
}
