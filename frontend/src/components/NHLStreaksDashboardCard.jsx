import React, { useEffect, useState } from "react";
import { api } from "../lib/api.js";
import { todayET } from "../shared/timeUtils.js";

const STREAK_WINDOW_GAMES = 7;

function fmtLine(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(1) : "—";
}

function fmtActual(v) {
  const n = Number(v);
  return Number.isFinite(n) ? String(Math.round(n * 10) / 10) : "—";
}

export default function NHLStreaksDashboardCard() {
  const [hotStreaks, setHotStreaks] = useState([]);
  const [coldStreaks, setColdStreaks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [windowGames, setWindowGames] = useState(STREAK_WINDOW_GAMES);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError("");
      try {
        const anchorDate = todayET();
        const payload = await api(
          `/api/nhl/streaks/sog?date=${encodeURIComponent(anchorDate)}&window_games=${STREAK_WINDOW_GAMES}&min_streak=2&top_n=5`
        );
        if (!cancelled) {
          setHotStreaks(Array.isArray(payload?.hot) ? payload.hot : []);
          setColdStreaks(Array.isArray(payload?.cold) ? payload.cold : []);
          setWindowGames(
            Number.isFinite(Number(payload?.window_games))
              ? Number(payload.window_games)
              : STREAK_WINDOW_GAMES
          );
        }
      } catch (e) {
        if (!cancelled) {
          setError(e?.message || "Failed to load NHL streaks.");
          setHotStreaks([]);
          setColdStreaks([]);
          setWindowGames(STREAK_WINDOW_GAMES);
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
          {windowGames}-Game Window
        </div>
      </div>
      <p className="text-sm text-slate-600">
        Real NHL SOG streaks from prediction rows + skater game logs, ranked by consecutive hits/misses vs each player&apos;s model-selected line.
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
                        {row.team_abbr ? ` (${row.team_abbr})` : ""}
                      </div>
                      <div className="text-sm text-slate-600">
                        SOG vs {fmtLine(row.line_value)} • {row.window_wins ?? 0}-{row.window_losses ?? 0} in {row.window_games ?? windowGames}
                      </div>
                      <div className="text-xs text-slate-500">
                        Last: {fmtActual(row.last_actual_value)} on {row.last_game_date || "—"}
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
                        {row.team_abbr ? ` (${row.team_abbr})` : ""}
                      </div>
                      <div className="text-sm text-slate-600">
                        SOG vs {fmtLine(row.line_value)} • {row.window_wins ?? 0}-{row.window_losses ?? 0} in {row.window_games ?? windowGames}
                      </div>
                      <div className="text-xs text-slate-500">
                        Last: {fmtActual(row.last_actual_value)} on {row.last_game_date || "—"}
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
