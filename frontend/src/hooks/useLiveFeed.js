import { useEffect, useRef, useState } from "react";

const MAX_ROWS = 150;
const WINDOW_MS = 60000;

export function useLiveFeed() {
  const [rows, setRows] = useState([]);
  const [connected, setConnected] = useState(false);
  const [rate, setRate] = useState({ total: 0, APPROVE: 0, REVIEW: 0, BLOCK: 0, avgLatency: 0, tps: 0 });
  const windowRef = useRef([]);

  useEffect(() => {
    let socket;
    let retry;
    let closed = false;

    const connect = () => {
      const protocol = window.location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${protocol}://${window.location.host}/ws/live`);
      socket.onopen = () => setConnected(true);
      socket.onclose = () => {
        setConnected(false);
        if (!closed) retry = setTimeout(connect, 2000);
      };
      socket.onmessage = (event) => {
        const batch = JSON.parse(event.data);
        const now = Date.now();
        windowRef.current = windowRef.current
          .concat(batch.map((d) => ({ t: now, decision: d.decision, latency: d.latency_ms })))
          .filter((d) => now - d.t < WINDOW_MS);
        setRows((prev) => batch.slice().reverse().concat(prev).slice(0, MAX_ROWS));
      };
    };

    connect();
    const ticker = setInterval(() => {
      const now = Date.now();
      const win = windowRef.current.filter((d) => now - d.t < WINDOW_MS);
      const lastTen = win.filter((d) => now - d.t < 10000).length;
      const counts = { APPROVE: 0, REVIEW: 0, BLOCK: 0 };
      let latency = 0;
      win.forEach((d) => {
        counts[d.decision] += 1;
        latency += d.latency;
      });
      setRate({ total: win.length, ...counts, avgLatency: win.length ? latency / win.length : 0, tps: lastTen / 10 });
    }, 1000);

    return () => {
      closed = true;
      clearTimeout(retry);
      clearInterval(ticker);
      if (socket) socket.close();
    };
  }, []);

  return { rows, connected, rate };
}
