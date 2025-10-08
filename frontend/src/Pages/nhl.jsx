// frontend/src/pages/NHL.jsx
import { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

function useFetch(url) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setErr(null);
    fetch(url)
      .then((r) => (r.ok ? r.json() : Promise.reject(r.statusText)))
      .then((j) => {
        if (active) setData(j);
      })
      .catch((e) => {
        if (active) setErr(String(e));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [url]);

  return { data, loading, err };
}

export default function NHLPage() {
  const [date, setDate] = useState(() => {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  });

  // Point the page at your backend API (uses VITE_API_BASE from .env)
  const gamesUrl = useMemo(
    () => `${API_BASE}/api/nhl/games/today?date=${encodeURIComponent(date)}`,
    [date]
  );

  const sogUrl = useMemo(
    () => `${API_BASE}/api/nhl/sog_stage?date=${encodeURIComponent(date)}`,
    [date]
  );

  const savesUrl = useMemo(
    () => `${API_BASE}/api/nhl/saves_stage?date=${encodeURIComponent(date)}`,
    [date]
  );

  const games = useFetch(gamesUrl);
  const props = useFetch(propsUrl);

  return (
    <div style={{ padding: 24, fontFamily: "ui-sans-serif, system-ui" }}>
      <h1 style={{ marginBottom: 8 }}>NHL Predictions</h1>

      <div
        style={{
          display: "flex",
          gap: 12,
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <label>
          Date:{" "}
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
          />
        </label>
        <small>API: {API_BASE}</small>
      </div>

      <section style={{ marginBottom: 24 }}>
        <h2>Games</h2>
        {games.loading ? (
          <p>Loading…</p>
        ) : games.err ? (
          <p>Error: {games.err}</p>
        ) : (
          <table
            cellPadding={6}
            style={{ borderCollapse: "collapse", width: "100%" }}
          >
            <thead>
              <tr>
                <th>Game ID</th>
                <th>Date</th>
                <th>Status</th>
                <th>Home</th>
                <th>Away</th>
              </tr>
            </thead>
            <tbody>
              {(games.data || []).map((g) => (
                <tr key={g.game_id}>
                  <td>{g.game_id}</td>
                  <td>{g.game_date}</td>
                  <td>{g.status}</td>
                  <td>{g.home}</td>
                  <td>{g.away}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>Props (today)</h2>
        {props.loading ? (
          <p>Loading…</p>
        ) : props.err ? (
          <p>Error: {props.err}</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table
              cellPadding={6}
              style={{ borderCollapse: "collapse", width: "100%" }}
            >
              <thead>
                <tr>
                  {Object.keys(props.data?.[0] || {}).map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(props.data || []).map((r, i) => (
                  <tr key={i}>
                    {Object.keys(props.data?.[0] || {}).map((k) => (
                      <td key={k}>{String(r[k])}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
