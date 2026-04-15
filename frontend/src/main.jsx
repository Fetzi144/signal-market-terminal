import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import App from "./App";
import "./index.css";

const LOCAL_DEV_HOSTS = new Set(["localhost", "127.0.0.1", "0.0.0.0"]);
const isLocalDev = typeof window !== "undefined" && LOCAL_DEV_HOSTS.has(window.location.hostname);

// Keep the service worker out of local dev so stale cached modules cannot white-screen Vite routes.
if ("serviceWorker" in navigator) {
  if (import.meta.env.PROD && !isLocalDev) {
    navigator.serviceWorker.register("/sw.js").catch(() => {});
  } else {
    navigator.serviceWorker.getRegistrations()
      .then((registrations) => Promise.all(registrations.map((registration) => registration.unregister())))
      .catch(() => {});

    if ("caches" in window) {
      caches.keys()
        .then((keys) => Promise.all(keys
          .filter((key) => key.startsWith("smt-"))
          .map((key) => caches.delete(key))))
        .catch(() => {});
    }
  }
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/*" element={<App />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);
