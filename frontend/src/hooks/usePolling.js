import { useEffect, useRef, useState } from "react";

export function usePolling(fetcher, intervalMs, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const saved = useRef(fetcher);
  saved.current = fetcher;

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const result = await saved.current();
        if (active) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        if (active) setError(err.message);
      }
    };
    load();
    const timer = setInterval(load, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [intervalMs, ...deps]);

  return { data, error };
}
