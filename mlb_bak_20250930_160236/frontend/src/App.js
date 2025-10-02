import { useEffect, useState } from "react";

export default function App() {
  const [resp, setResp] = useState("…");
  useEffect(() => {
    const base = import.meta.env.VITE_API_BASE || "";
    fetch(`${base}/admin/ping`)
      .then((r) => r.json())
      .then((j) => setResp(JSON.stringify(j)))
      .catch((e) => setResp(String(e)));
  }, []);
  return (
    <div style={{ padding: 24, fontFamily: "system-ui, sans-serif" }}>
      <h1>Proppadia MLB</h1>
      <p>API ping: {resp}</p>
    </div>
  );
}
