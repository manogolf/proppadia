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

  return { data: data || [], loading, err };
}

function ProbCell({ value }) {
  if (value == null) return <td></td>;
  const v = Number(value);
  const pct = (v * 100).toFixed(1) + "%";
  const hot = v >= 0.6; // simple visual cue; tweak as you like
  const warm = !hot && v >= 0.56;
  const bg = hot ? "#ffe7e7" : warm ? "#fff5d6" : "transparent";
  return <td style={{ background: bg }}>{pct}</td>;
}

export default function NHLPage() {
  const [date, setDate] = useState(() => {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  });

  const gamesUrl = useMemo(
    () => `${API_BASE}/api/nhl/games/today?date=${encodeURIComponent(date)}`,
    [date]
  );
  const sogUrl = useMemo(
    () => `${API_BASE}/api/nhl/sog?date=${encodeURIComponent(date)}`,
    [date]
  );
  const savesUrl = useMemo(
    () => `${API_BASE}/api/nhl/saves?date=${encodeURIComponent(date)}`,
    [date]
  );

  const games = useFetch(gamesUrl);
  const sog = useFetch(sogUrl);
  const saves = useFetch(savesUrl);

  // Helper to detect available p_over_* columns (underscored style)
  const sogProbCols = useMemo(() => {
    const row = sog.data[0] || {};
    return Object.keys(row)
      .filter((k) => k.startsWith("p_over_"))
      .sort();
  }, [sog.data]);
  const savesProbCols = useMemo(() => {
    const row = saves.data[0] || {};
    return Object.keys(row)
      .filter((k) => k.startsWith("p_over_"))
      .sort();
  }, [saves.data]);

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

      {/* Games */}
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
              {games.data.map((g) => (
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

      {/* SOG */}
      <section style={{ marginBottom: 24 }}>
        <h2>Skater SOG</h2>
        {sog.loading ? (
          <p>Loading…</p>
        ) : sog.err ? (
          <p>Error: {sog.err}</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table
              cellPadding={6}
              style={{ borderCollapse: "collapse", width: "100%" }}
            >
              <thead>
                <tr>
                  <th>Player</th>
                  <th>Team</th>
                  <th>Game</th>
                  {sogProbCols.map((c) => (
                    <th key={c}>{c.replaceAll("_", ".")}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sog.data.map((r, i) => (
                  <tr key={i}>
                    <td>{r.full_name ?? r.player_id}</td>
                    <td>{r.team ?? r.team_id}</td>
                    <td>{r.game_id}</td>
                    {sogProbCols.map((c) => (
                      <ProbCell key={c} value={r[c]} />
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Saves */}
      <section>
        <h2>Goalie Saves</h2>
        {saves.loading ? (
          <p>Loading…</p>
        ) : saves.err ? (
          <p>Error: {saves.err}</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table
              cellPadding={6}
              style={{ borderCollapse: "collapse", width: "100%" }}
            >
              <thead>
                <tr>
                  <th>Goalie</th>
                  <th>Team</th>
                  <th>Game</th>
                  {savesProbCols.map((c) => (
                    <th key={c}>{c.replaceAll("_", ".")}</th>
                  ))}
                  <th>start_prob</th>
                </tr>
              </thead>
              <tbody>
                {saves.data.map((r, i) => (
                  <tr key={i}>
                    <td>{r.full_name ?? r.player_id}</td>
                    <td>{r.team ?? r.team_id}</td>
                    <td>{r.game_id}</td>
                    {savesProbCols.map((c) => (
                      <ProbCell key={c} value={r[c]} />
                    ))}
                    <td>
                      {r.start_prob != null
                        ? (Number(r.start_prob) * 100).toFixed(0) + "%"
                        : ""}
                    </td>
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
