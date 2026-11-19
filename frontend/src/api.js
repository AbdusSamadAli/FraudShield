async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text}`);
  }
  return response.json();
}

export const api = {
  stats: (minutes = 15) => request(`/api/stats?minutes=${minutes}`),
  alerts: (status = "OPEN", decision = "ALL") => request(`/api/alerts?status=${status}&decision=${decision}&limit=100`),
  alert: (id) => request(`/api/alerts/${id}`),
  feedback: (id, label, analyst) =>
    request(`/api/alerts/${id}/feedback`, { method: "POST", body: JSON.stringify({ label, analyst }) }),
  model: () => request("/api/model"),
  system: () => request("/api/system"),
  patterns: () => request("/api/simulator/patterns"),
  attack: (pattern, count) =>
    request("/api/simulator/attack", { method: "POST", body: JSON.stringify({ pattern, count }) }),
};

export const formatInr = (value) =>
  new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR", maximumFractionDigits: 0 }).format(value || 0);

export const formatTime = (ms) => new Date(ms).toLocaleTimeString("en-IN", { hour12: false });

export const titleCase = (text) => (text || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
