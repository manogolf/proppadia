import React, { useCallback, useEffect, useMemo, useState } from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import {
  WATCHLIST_SCOPE_MLB,
  WATCHLIST_SCOPE_NHL,
  watchlistStorageKey,
} from "../shared/watchlistStorage.js";

function readScope(userId, scopePath) {
  try {
    const raw = window.localStorage.getItem(watchlistStorageKey(userId, scopePath));
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeScope(userId, scopePath, rows) {
  try {
    window.localStorage.setItem(
      watchlistStorageKey(userId, scopePath),
      JSON.stringify(Array.isArray(rows) ? rows.slice(0, 100) : [])
    );
  } catch {
    // ignore local storage errors
  }
}

function playerQuery(row) {
  return encodeURIComponent(String(row?.player_name || row?.player_id || "").trim());
}

export default function WatchlistPage() {
  const { user } = useAuth();
  const [mlbRows, setMlbRows] = useState([]);
  const [nhlRows, setNhlRows] = useState([]);

  useEffect(() => {
    if (!user?.id) return;
    setMlbRows(readScope(user.id, WATCHLIST_SCOPE_MLB));
    setNhlRows(readScope(user.id, WATCHLIST_SCOPE_NHL));
  }, [user?.id]);

  const removeRow = useCallback(
    (scopePath, id) => {
      if (!user?.id) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        const next = mlbRows.filter((r) => String(r.id) !== String(id));
        setMlbRows(next);
        writeScope(user.id, scopePath, next);
        return;
      }
      const next = nhlRows.filter((r) => String(r.id) !== String(id));
      setNhlRows(next);
      writeScope(user.id, scopePath, next);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const clearScope = useCallback(
    (scopePath) => {
      if (!user?.id) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        setMlbRows([]);
        writeScope(user.id, scopePath, []);
        return;
      }
      setNhlRows([]);
      writeScope(user.id, scopePath, []);
    },
    [user?.id]
  );

  const total = useMemo(() => mlbRows.length + nhlRows.length, [mlbRows.length, nhlRows.length]);

  return (
    <div className="min-h-screen pp-page px-4 py-8">
      <div className="max-w-5xl mx-auto">
        <div className="pp-card p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h1 className="text-2xl font-semibold text-slate-900">Watchlist</h1>
              <p className="text-sm text-slate-600 mt-1">
                Saved players for quick research access. Total saved: {total}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <PrefetchLink to="/props" className="pp-btn pp-btn-secondary pp-btn-sm">
                Open MLB
              </PrefetchLink>
              <PrefetchLink to="/nhl/predictions" className="pp-btn pp-btn-secondary pp-btn-sm">
                Open NHL
              </PrefetchLink>
            </div>
          </div>

          <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-slate-900">MLB Watchlist ({mlbRows.length})</h2>
                <button
                  type="button"
                  className="pp-btn pp-btn-ghost pp-btn-sm"
                  disabled={mlbRows.length === 0}
                  onClick={() => clearScope(WATCHLIST_SCOPE_MLB)}
                >
                  Clear
                </button>
              </div>
              {mlbRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">No MLB players saved yet.</div>
              ) : (
                <div className="mt-3 space-y-2">
                  {mlbRows.map((row) => (
                    <div
                      key={String(row.id)}
                      className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm flex items-center justify-between gap-2"
                    >
                      <div>
                        <PrefetchLink
                          to={`/props?player=${playerQuery(row)}`}
                          className="font-medium text-slate-900 underline"
                        >
                          {row.player_name || row.player_id || "Unknown"}
                        </PrefetchLink>
                        <div className="text-xs text-slate-500">{row.team || "-"}</div>
                      </div>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_MLB, row.id)}
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>

            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-slate-900">NHL Watchlist ({nhlRows.length})</h2>
                <button
                  type="button"
                  className="pp-btn pp-btn-ghost pp-btn-sm"
                  disabled={nhlRows.length === 0}
                  onClick={() => clearScope(WATCHLIST_SCOPE_NHL)}
                >
                  Clear
                </button>
              </div>
              {nhlRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">No NHL players saved yet.</div>
              ) : (
                <div className="mt-3 space-y-2">
                  {nhlRows.map((row) => (
                    <div
                      key={String(row.id)}
                      className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm flex items-center justify-between gap-2"
                    >
                      <div>
                        <PrefetchLink
                          to={`/nhl/predictions?player=${playerQuery(row)}`}
                          className="font-medium text-slate-900 underline"
                        >
                          {row.player_name || row.player_id || "Unknown"}
                        </PrefetchLink>
                        <div className="text-xs text-slate-500">{row.team || "-"}</div>
                      </div>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_NHL, row.id)}
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>
          </div>
        </div>
      </div>
    </div>
  );
}
