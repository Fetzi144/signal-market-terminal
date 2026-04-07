import { useEffect, useRef, useState, useCallback } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE || "") + "/api/v1";
const RECONNECT_DELAY = 5000;

/**
 * Hook for consuming SSE events from the signal stream.
 * Returns { connected, lastEvent, addEventListener }.
 */
export default function useSSE() {
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState(null);
  const esRef = useRef(null);
  const listenersRef = useRef({});

  const addEventListener = useCallback((eventType, handler) => {
    if (!listenersRef.current[eventType]) {
      listenersRef.current[eventType] = [];
    }
    listenersRef.current[eventType].push(handler);

    // Return cleanup function
    return () => {
      const arr = listenersRef.current[eventType];
      if (arr) {
        listenersRef.current[eventType] = arr.filter((h) => h !== handler);
      }
    };
  }, []);

  useEffect(() => {
    let reconnectTimer = null;

    function connect() {
      const es = new EventSource(`${API_BASE}/events/signals`);
      esRef.current = es;

      es.addEventListener("connected", () => {
        setConnected(true);
      });

      es.addEventListener("new_signal", (e) => {
        try {
          const data = JSON.parse(e.data);
          setLastEvent({ type: "new_signal", data });
          (listenersRef.current["new_signal"] || []).forEach((h) => h(data));
        } catch {}
      });

      es.addEventListener("new_alert", (e) => {
        try {
          const data = JSON.parse(e.data);
          setLastEvent({ type: "new_alert", data });
          (listenersRef.current["new_alert"] || []).forEach((h) => h(data));
        } catch {}
      });

      es.onerror = () => {
        setConnected(false);
        es.close();
        reconnectTimer = setTimeout(connect, RECONNECT_DELAY);
      };
    }

    connect();

    return () => {
      if (esRef.current) esRef.current.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
      setConnected(false);
    };
  }, []);

  return { connected, lastEvent, addEventListener };
}
