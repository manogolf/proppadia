import React, { useEffect, useState } from "react";
import { api } from "../lib/api.js";
import { nowET, todayET } from "../shared/timeUtils.js";
import { getPropDisplayLabel } from "../shared/propUtils.js";

const STREAK_WINDOW_GAMES = 7;
// Fetch more than 7 calendar days so each player/market can still have 7 recent graded entries.
const HISTORY_LOOKBACK_DAYS = 45;

function rowTime(row) {
  const raw = row?.game_date || row?.created_at || row?.updated_at || "";
  const t = new Date(raw).getTime();
  return Number.isFinite(t) ? t : 0;
}

function buildStreakRows(rows) {
  const grouped = new Map();

  for (const row of rows || []) {
    const outcome = String(row?.outcome || row?.status || "").toLowerCase();
    if (!["win", "loss"].includes(outcome)) continue;
    const key = `${row?.player_id ?? row?.player_name}-${row?.prop_type}`;
    if (!key) continue;
    const bucket = grouped.get(key) || [];
    bucket.push(row);
    grouped.set(key, bucket);
  }

  const hot = [];
  const cold = [];

  for (const bucket of grouped.values()) {
    bucket.sort((a, b) => rowTime(b) - rowTime(a));
    const recentWindow = bucket.slice(0, STREAK_WINDOW_GAMES);
    if (!recentWindow.length) continue;
    const firstOutcome = String(
      recentWindow[0]?.outcome || recentWindow[0]?.status || ""
    ).toLowerCase();
    if (!["win", "loss"].includes(firstOutcome)) continue;
    let streak = 0;
    for (const row of recentWindow) {
      const outcome = String(row?.outcome || row?.status || "").toLowerCase();
      if (outcome !== firstOutcome) break;
      streak += 1;
    }
    if (streak < 2) continue;

    const item = {
      player_name:
        recentWindow[0]?.player_name || String(recentWindow[0]?.player_id || "Unknown"),
      team: recentWindow[0]?.team || null,
      prop_type: recentWindow[0]?.prop_type || "",
      streak,
      lastOutcome: firstOutcome,
      windowSize: recentWindow.length,
    };
    if (firstOutcome === "win") hot.push(item);
    if (firstOutcome === "loss") cold.push(item);
  }

  hot.sort((a, b) => b.streak - a.streak || String(a.player_name).localeCompare(String(b.player_name)));
  cold.sort((a, b) => b.streak - a.streak || String(a.player_name).localeCompare(String(b.player_name)));
  return { hot: hot.slice(0, 5), cold: cold.slice(0, 5) };
}

export default function NHLStreaksDashboardCard() {
  const [hotStreaks, setHotStreaks] = useState([]);
  const [coldStreaks, setColdStreaks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError("");
      try {
        const toDate = todayET();
        const fromDate = nowET().minus({ days: HISTORY_LOOKBACK_DAYS }).toISODate();
        const rows = [];
        let offset = 0;
        const limit = 200;
        let total = Infinity;

        while (offset < total && offset < 5000) {
          const payload = await api(
            `/api/nhl/props/history?from_date=${encodeURIComponent(fromDate)}&to_date=${encodeURIComponent(toDate)}&limit=${limit}&offset=${offset}`
          );
          const pageRows = Array.isArray(payload?.rows) ? payload.rows : [];
          total = Number(payload?.total ?? pageRows.length);
          rows.push(...pageRows);
          if (pageRows.length < limit) break;
          offset += limit;
        }

        const { hot, cold } = buildStreakRows(rows);
        if (!cancelled) {
          setHotStreaks(hot);
          setColdStreaks(cold);
        }
      } catch (e) {
        if (!cancelled) {
          setError(e?.message || "Failed to load NHL streaks.");
          setHotStreaks([]);
          setColdStreaks([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="pp-card p-6 space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-medium text-slate-900">
            NHL Streaks Dashboard
          </h2>
          <span className="text-xl" aria-hidden="true">
            🔥❄️
          </span>
        </div>
        <div className="pp-chip px-3 py-1 text-xs font-semibold text-slate-600">
          7-Game Window
        </div>
      </div>
      <p className="text-sm text-slate-600">
        Recent graded NHL props (win/loss only), using each player market&apos;s last 7 graded entries.
      </p>

      {loading ? (
        <div className="text-center text-slate-400">Loading NHL streaks…</div>
      ) : error ? (
        <div className="pp-chip p-3 text-sm text-rose-700 bg-rose-50 border border-rose-200">
          {error}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-lg font-semibold text-emerald-700 mb-2">
              Hot Streaks 🔥
            </h3>
            {hotStreaks.length === 0 ? (
              <div className="pp-chip p-3 text-sm text-slate-500">No hot streaks.</div>
            ) : (
              <ul className="space-y-2">
                {hotStreaks.map((row) => (
                  <li
                    key={`${row.player_name}-${row.prop_type}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {row.player_name}
                        {row.team ? ` (${row.team})` : ""}
                      </div>
                      <div className="text-sm text-slate-600">
                        {getPropDisplayLabel(row.prop_type)}
                      </div>
                    </div>
                    <div className="text-emerald-600 font-bold pl-4">W{row.streak}</div>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div>
            <h3 className="text-lg font-semibold text-sky-700 mb-2">
              Cold Streaks ❄️
            </h3>
            {coldStreaks.length === 0 ? (
              <div className="pp-chip p-3 text-sm text-slate-500">No cold streaks.</div>
            ) : (
              <ul className="space-y-2">
                {coldStreaks.map((row) => (
                  <li
                    key={`${row.player_name}-${row.prop_type}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {row.player_name}
                        {row.team ? ` (${row.team})` : ""}
                      </div>
                      <div className="text-sm text-slate-600">
                        {getPropDisplayLabel(row.prop_type)}
                      </div>
                    </div>
                    <div className="text-sky-600 font-bold pl-4">L{row.streak}</div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
