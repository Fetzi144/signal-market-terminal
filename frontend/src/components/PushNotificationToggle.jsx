import { useEffect, useState } from "react";
import { getVapidKey, subscribePush, unsubscribePush } from "../api";

function urlBase64ToUint8Array(base64String) {
  const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const rawData = atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; i++) {
    outputArray[i] = rawData.charCodeAt(i);
  }
  return outputArray;
}

export default function PushNotificationToggle({ compact = false }) {
  const [status, setStatus] = useState("loading"); // loading | unsupported | denied | subscribed | unsubscribed | error
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
      setStatus("unsupported");
      return;
    }
    if (Notification.permission === "denied") {
      setStatus("denied");
      return;
    }

    navigator.serviceWorker.ready.then((reg) => {
      reg.pushManager.getSubscription().then((sub) => {
        setStatus(sub ? "subscribed" : "unsubscribed");
      });
    }).catch(() => setStatus("unsubscribed"));
  }, []);

  const handleSubscribe = async () => {
    try {
      setError(null);
      const permission = await Notification.requestPermission();
      if (permission !== "granted") {
        setStatus("denied");
        return;
      }

      const { vapid_public_key } = await getVapidKey();
      if (!vapid_public_key) {
        setError("Push not configured on server");
        return;
      }

      const reg = await navigator.serviceWorker.ready;
      const subscription = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapid_public_key),
      });

      await subscribePush(subscription);
      setStatus("subscribed");
    } catch (e) {
      setError(e.message);
      setStatus("error");
    }
  };

  const handleUnsubscribe = async () => {
    try {
      setError(null);
      const reg = await navigator.serviceWorker.ready;
      const subscription = await reg.pushManager.getSubscription();
      if (subscription) {
        await unsubscribePush(subscription.endpoint);
        await subscription.unsubscribe();
      }
      setStatus("unsubscribed");
    } catch (e) {
      setError(e.message);
      setStatus("error");
    }
  };

  if (status === "loading") return null;
  if (status === "unsupported") {
    return compact ? null : (
      <span style={{ fontSize: 12, color: "var(--text-dim)" }}>Push not supported</span>
    );
  }

  const isSubscribed = status === "subscribed";

  if (compact) {
    return (
      <button
        onClick={isSubscribed ? handleUnsubscribe : handleSubscribe}
        title={isSubscribed ? "Push notifications enabled" : "Enable push notifications"}
        style={{
          background: "transparent",
          border: "1px solid var(--border)",
          color: isSubscribed ? "var(--green)" : "var(--text-dim)",
          borderRadius: 6,
          padding: "4px 8px",
          fontSize: 13,
          cursor: "pointer",
        }}
      >
        {isSubscribed ? "\uD83D\uDD14" : "\uD83D\uDD15"}
      </button>
    );
  }

  return (
    <div
      style={{
        background: "var(--bg-card)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        padding: "12px 16px",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
      }}
    >
      <div>
        <div style={{ fontWeight: 500, fontSize: 14 }}>Push Notifications</div>
        <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2 }}>
          {status === "denied"
            ? "Notifications blocked in browser settings"
            : isSubscribed
            ? "You will receive alerts as push notifications"
            : "Get notified when high-rank signals fire"}
        </div>
        {error && <div style={{ fontSize: 12, color: "var(--red)", marginTop: 4 }}>{error}</div>}
      </div>
      <button
        onClick={isSubscribed ? handleUnsubscribe : handleSubscribe}
        disabled={status === "denied"}
        style={{
          background: isSubscribed ? "var(--red)" : "var(--green)",
          color: "#fff",
          border: "none",
          borderRadius: 6,
          padding: "6px 14px",
          fontSize: 13,
          cursor: status === "denied" ? "not-allowed" : "pointer",
          opacity: status === "denied" ? 0.5 : 1,
        }}
      >
        {isSubscribed ? "Disable" : "Enable"}
      </button>
    </div>
  );
}
