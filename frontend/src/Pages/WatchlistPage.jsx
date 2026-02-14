import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import {
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_MLB,
  WATCHLIST_SCOPE_NHL,
  readWatchlistScope,
  writeWatchlistScope,
} from "../shared/watchlistStorage.js";

const WATCHLIST_PAGE_PREFS_KEY = "proppadia_watchlist_page_prefs_v1";

function playerQuery(row) {
  return encodeURIComponent(String(row?.player_name || row?.player_id || "").trim());
}

function formatAddedAt(value) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

function toRowId(row) {
  const pid = row?.player_id;
  if (pid !== undefined && pid !== null && String(pid).trim() !== "") return String(pid);
  const name = String(row?.player_name || "").trim().toLowerCase();
  const team = String(row?.team || row?.team_abbr || "").trim().toLowerCase();
  return `${name}:${team}`;
}

function normalizeRows(rows) {
  if (!Array.isArray(rows)) return [];
  const dedup = new Map();
  for (const row of rows) {
    const id = toRowId(row);
    if (!id) continue;
    const candidate = {
      id,
      player_id:
        row?.player_id !== undefined && row?.player_id !== null && String(row.player_id).trim() !== ""
          ? row.player_id
          : null,
      player_name: row?.player_name ? String(row.player_name) : null,
      team: row?.team ? String(row.team) : row?.team_abbr ? String(row.team_abbr) : null,
      added_at: row?.added_at && !Number.isNaN(new Date(row.added_at).getTime())
        ? String(row.added_at)
        : new Date().toISOString(),
    };
    const existing = dedup.get(id);
    if (!existing) {
      dedup.set(id, candidate);
      continue;
    }
    const existingTs = new Date(existing.added_at || 0).getTime();
    const candidateTs = new Date(candidate.added_at || 0).getTime();
    if (candidateTs > existingTs) dedup.set(id, candidate);
  }
  return Array.from(dedup.values())
    .sort((a, b) => new Date(b.added_at || 0).getTime() - new Date(a.added_at || 0).getTime())
    .slice(0, 100);
}

export default function WatchlistPage() {
  const { user } = useAuth();
  const [mlbRows, setMlbRows] = useState([]);
  const [nhlRows, setNhlRows] = useState([]);
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState("newest");
  const [viewScope, setViewScope] = useState("all");
  const [importMode, setImportMode] = useState("replace");
  const [copyNotice, setCopyNotice] = useState("");
  const importInputRef = useRef(null);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(WATCHLIST_PAGE_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed?.sortBy === "newest" || parsed?.sortBy === "name") {
        setSortBy(parsed.sortBy);
      }
      if (parsed?.viewScope === "all" || parsed?.viewScope === "mlb" || parsed?.viewScope === "nhl") {
        setViewScope(parsed.viewScope);
      }
      if (parsed?.importMode === "replace" || parsed?.importMode === "merge") {
        setImportMode(parsed.importMode);
      }
    } catch {
      // ignore malformed local preferences
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        WATCHLIST_PAGE_PREFS_KEY,
        JSON.stringify({ sortBy, viewScope, importMode })
      );
    } catch {
      // ignore local preference write errors
    }
  }, [importMode, sortBy, viewScope]);

  const refreshRows = useCallback(() => {
    if (!user?.id) {
      setMlbRows([]);
      setNhlRows([]);
      return;
    }
    setMlbRows(readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB));
    setNhlRows(readWatchlistScope(user.id, WATCHLIST_SCOPE_NHL));
  }, [user?.id]);

  useEffect(() => {
    refreshRows();
  }, [refreshRows]);

  useEffect(() => {
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshRows();
      }
    }
    function onWatchlistUpdated() {
      refreshRows();
    }
    window.addEventListener("storage", onStorage);
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    };
  }, [refreshRows]);

  const removeRow = useCallback(
    (scopePath, id) => {
      if (!user?.id) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        const next = mlbRows.filter((r) => String(r.id) !== String(id));
        setMlbRows(next);
        writeWatchlistScope(user.id, scopePath, next);
        return;
      }
      const next = nhlRows.filter((r) => String(r.id) !== String(id));
      setNhlRows(next);
      writeWatchlistScope(user.id, scopePath, next);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const clearScope = useCallback(
    (scopePath) => {
      if (!user?.id) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        setMlbRows([]);
        writeWatchlistScope(user.id, scopePath, []);
        return;
      }
      setNhlRows([]);
      writeWatchlistScope(user.id, scopePath, []);
    },
    [user?.id]
  );

  const removeVisible = useCallback(
    (scopePath, visibleRows) => {
      if (!user?.id) return;
      const visibleIds = new Set((visibleRows || []).map((r) => String(r.id)));
      if (visibleIds.size === 0) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        const next = mlbRows.filter((r) => !visibleIds.has(String(r.id)));
        setMlbRows(next);
        writeWatchlistScope(user.id, scopePath, next);
        setCopyNotice(`Removed ${visibleIds.size} visible MLB watchlist row(s).`);
        window.setTimeout(() => setCopyNotice(""), 1500);
        return;
      }
      const next = nhlRows.filter((r) => !visibleIds.has(String(r.id)));
      setNhlRows(next);
      writeWatchlistScope(user.id, scopePath, next);
      setCopyNotice(`Removed ${visibleIds.size} visible NHL watchlist row(s).`);
      window.setTimeout(() => setCopyNotice(""), 1500);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const total = useMemo(() => mlbRows.length + nhlRows.length, [mlbRows.length, nhlRows.length]);
  const q = useMemo(() => query.trim().toLowerCase(), [query]);

  function applyFilters(rows) {
    const filtered = !q
      ? rows
      : rows.filter((row) => {
          const haystack = [row?.player_name, row?.player_id, row?.team]
            .map((v) => String(v || "").toLowerCase())
            .join(" ");
          return haystack.includes(q);
        });
    const out = [...filtered];
    if (sortBy === "name") {
      out.sort((a, b) =>
        String(a?.player_name || a?.player_id || "").localeCompare(
          String(b?.player_name || b?.player_id || "")
        )
      );
      return out;
    }
    out.sort(
      (a, b) =>
        new Date(b?.added_at || 0).getTime() - new Date(a?.added_at || 0).getTime()
    );
    return out;
  }

  const visibleMlbRows = useMemo(() => applyFilters(mlbRows), [mlbRows, q, sortBy]);
  const visibleNhlRows = useMemo(() => applyFilters(nhlRows), [nhlRows, q, sortBy]);
  const visibleTotal = useMemo(
    () => visibleMlbRows.length + visibleNhlRows.length,
    [visibleMlbRows.length, visibleNhlRows.length]
  );

  const handleCopyLink = useCallback(async (sport, row) => {
    const player = String(row?.player_name || row?.player_id || "").trim();
    if (!player) return;
    const path =
      sport === "mlb"
        ? `/props?player=${playerQuery(row)}`
        : `/nhl/predictions?player=${playerQuery(row)}`;
    try {
      const url = `${window.location.origin}${path}`;
      await navigator.clipboard.writeText(url);
      setCopyNotice(`Copied ${sport.toUpperCase()} link for ${player}`);
      window.setTimeout(() => setCopyNotice(""), 1500);
    } catch {
      setCopyNotice("Failed to copy link.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, []);

  const handleExportAll = useCallback(() => {
    try {
      const payload = {
        version: 1,
        exported_at: new Date().toISOString(),
        mlb: mlbRows.slice(0, 100),
        nhl: nhlRows.slice(0, 100),
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json;charset=utf-8;",
      });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = `watchlist_all_${String(user?.id || "member")}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(href);
      setCopyNotice("Exported watchlist bundle.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    } catch {
      setCopyNotice("Failed to export watchlist.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, [mlbRows, nhlRows, user?.id]);

  const handleImportClick = useCallback(() => {
    if (importInputRef.current) importInputRef.current.click();
  }, [importInputRef]);

  const handleImportAll = useCallback(
    async (e) => {
      const file = e?.target?.files?.[0];
      if (!file) return;
      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        const incomingMlb = normalizeRows(parsed?.mlb);
        const incomingNhl = normalizeRows(parsed?.nhl);
        if (!user?.id) return;
        const nextMlb =
          importMode === "merge"
            ? normalizeRows([...mlbRows, ...incomingMlb])
            : incomingMlb;
        const nextNhl =
          importMode === "merge"
            ? normalizeRows([...nhlRows, ...incomingNhl])
            : incomingNhl;
        setMlbRows(nextMlb);
        setNhlRows(nextNhl);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, nextMlb);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, nextNhl);
        setCopyNotice(
          `Imported watchlist bundle (${importMode === "merge" ? "merge" : "replace"}; MLB ${nextMlb.length}, NHL ${nextNhl.length}).`
        );
        window.setTimeout(() => setCopyNotice(""), 1800);
      } catch {
        setCopyNotice("Failed to import watchlist bundle.");
        window.setTimeout(() => setCopyNotice(""), 1800);
      } finally {
        if (e?.target) e.target.value = "";
      }
    },
    [importMode, mlbRows, nhlRows, user?.id]
  );

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
            {copyNotice ? (
              <div className="lg:col-span-2 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
                {copyNotice}
              </div>
            ) : null}
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap gap-2">
                <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs">
                  Total <strong>{total}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
                  Visible <strong>{visibleTotal}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 text-blue-700 px-2 py-1 text-xs">
                  MLB <strong>{visibleMlbRows.length}/{mlbRows.length}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-indigo-100 text-indigo-700 px-2 py-1 text-xs">
                  NHL <strong>{visibleNhlRows.length}/{nhlRows.length}</strong>
                </span>
              </div>
            </section>
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs text-slate-500">View</span>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "all" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("all")}
                >
                  All
                </button>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "mlb" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("mlb")}
                >
                  MLB
                </button>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "nhl" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("nhl")}
                >
                  NHL
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={() => {
                    setQuery("");
                    setSortBy("newest");
                    setViewScope("all");
                    setImportMode("replace");
                  }}
                >
                  Reset View
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleExportAll}
                >
                  Export All
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleImportClick}
                >
                  Import All
                </button>
                <label className="inline-flex items-center gap-2 text-xs text-slate-600">
                  Import mode
                  <select
                    value={importMode}
                    onChange={(e) => setImportMode(e.target.value)}
                    className="pp-chip px-2 py-1 text-xs text-slate-800"
                  >
                    <option value="replace">Replace</option>
                    <option value="merge">Merge</option>
                  </select>
                </label>
                <input
                  ref={(el) => {
                    importInputRef.current = el;
                  }}
                  type="file"
                  accept="application/json,.json"
                  className="hidden"
                  onChange={handleImportAll}
                />
              </div>
            </section>
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
                <div className="md:col-span-2">
                  <div className="text-xs text-slate-500 mb-1">Search watchlist</div>
                  <input
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Player, team, or player id..."
                    className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                  />
                </div>
                <div>
                  <div className="text-xs text-slate-500 mb-1">Sort by</div>
                  <select
                    value={sortBy}
                    onChange={(e) => setSortBy(e.target.value)}
                    className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                  >
                    <option value="newest">Newest added</option>
                    <option value="name">Player name</option>
                  </select>
                </div>
                <div className="md:col-span-3">
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={() => setQuery("")}
                    disabled={!query.trim()}
                  >
                    Clear Search
                  </button>
                </div>
              </div>
            </section>
            {(viewScope === "all" || viewScope === "mlb") ? (
            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-slate-900">
                  MLB Watchlist ({visibleMlbRows.length}/{mlbRows.length})
                </h2>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                    disabled={visibleMlbRows.length === 0}
                    onClick={() => removeVisible(WATCHLIST_SCOPE_MLB, visibleMlbRows)}
                  >
                    Remove Visible
                  </button>
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm"
                    disabled={mlbRows.length === 0}
                    onClick={() => clearScope(WATCHLIST_SCOPE_MLB)}
                  >
                    Clear
                  </button>
                </div>
              </div>
              {visibleMlbRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">
                  {mlbRows.length === 0 ? "No MLB players saved yet." : "No MLB matches for current search."}
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {visibleMlbRows.map((row) => (
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
                        <div className="text-xs text-slate-400">Added {formatAddedAt(row.added_at)}</div>
                      </div>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_MLB, row.id)}
                      >
                        Remove
                      </button>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm"
                        onClick={() => handleCopyLink("mlb", row)}
                      >
                        Copy Link
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>
            ) : null}

            {(viewScope === "all" || viewScope === "nhl") ? (
            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-slate-900">
                  NHL Watchlist ({visibleNhlRows.length}/{nhlRows.length})
                </h2>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                    disabled={visibleNhlRows.length === 0}
                    onClick={() => removeVisible(WATCHLIST_SCOPE_NHL, visibleNhlRows)}
                  >
                    Remove Visible
                  </button>
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm"
                    disabled={nhlRows.length === 0}
                    onClick={() => clearScope(WATCHLIST_SCOPE_NHL)}
                  >
                    Clear
                  </button>
                </div>
              </div>
              {visibleNhlRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">
                  {nhlRows.length === 0 ? "No NHL players saved yet." : "No NHL matches for current search."}
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {visibleNhlRows.map((row) => (
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
                        <div className="text-xs text-slate-400">Added {formatAddedAt(row.added_at)}</div>
                      </div>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_NHL, row.id)}
                      >
                        Remove
                      </button>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm"
                        onClick={() => handleCopyLink("nhl", row)}
                      >
                        Copy Link
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
