import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";
import {
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_MLB,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../shared/watchlistStorage.js";

const PLAYER_BROWSER_PREFS_KEY = "proppadia_player_browser_prefs_v1";

function playerQuery(value) {
  return encodeURIComponent(String(value || "").trim());
}

function toUtcDay(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const d = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export default function PlayerTeamBrowser() {
  const location = useLocation();
  const navigate = useNavigate();
  const { user } = useAuth();
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState("");
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [watchedTeamsOnly, setWatchedTeamsOnly] = useState(false);
  const [recentOnly, setRecentOnly] = useState(false);
  const [recentDays, setRecentDays] = useState("any");
  const [watchlist, setWatchlist] = useState([]);
  const [openTeams, setOpenTeams] = useState({});
  const [teamSort, setTeamSort] = useState("alpha");
  const [rowSort, setRowSort] = useState("recent");
  const [showUnknownTeam, setShowUnknownTeam] = useState(false);
  const [notice, setNotice] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [loadedAt, setLoadedAt] = useState(null);
  const teamRefs = useRef(new Map());

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(PLAYER_BROWSER_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (typeof parsed?.query === "string") setQuery(parsed.query);
      if (typeof parsed?.watchlistOnly === "boolean") setWatchlistOnly(parsed.watchlistOnly);
      if (typeof parsed?.watchedTeamsOnly === "boolean") setWatchedTeamsOnly(parsed.watchedTeamsOnly);
      if (typeof parsed?.recentOnly === "boolean") setRecentOnly(parsed.recentOnly);
      if (parsed?.recentDays === "any" || parsed?.recentDays === "7" || parsed?.recentDays === "30" || parsed?.recentDays === "90") {
        setRecentDays(parsed.recentDays);
      }
      if (parsed?.openTeams && typeof parsed.openTeams === "object") setOpenTeams(parsed.openTeams);
      if (parsed?.teamSort === "alpha" || parsed?.teamSort === "players" || parsed?.teamSort === "watched") {
        setTeamSort(parsed.teamSort);
      }
      if (parsed?.rowSort === "recent" || parsed?.rowSort === "name" || parsed?.rowSort === "watched") {
        setRowSort(parsed.rowSort);
      }
    } catch {
      // ignore malformed local prefs
    }
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const q = String(params.get("q") || "");
    const wl = String(params.get("watchlist") || "").toLowerCase() === "1";
    const wt = String(params.get("watched_teams") || "").toLowerCase() === "1";
    const recent = String(params.get("recent") || "").toLowerCase() === "1";
    const rdays = String(params.get("recent_days") || "");
    const sort = String(params.get("sort") || "");
    const rowSortParam = String(params.get("row_sort") || "");
    if (q) setQuery(q);
    if (wl) setWatchlistOnly(true);
    if (wt) setWatchedTeamsOnly(true);
    if (recent) setRecentOnly(true);
    if (rdays === "any" || rdays === "7" || rdays === "30" || rdays === "90") {
      setRecentDays(rdays);
    }
    if (sort === "alpha" || sort === "players" || sort === "watched") {
      setTeamSort(sort);
    }
    if (rowSortParam === "recent" || rowSortParam === "name" || rowSortParam === "watched") {
      setRowSort(rowSortParam);
    }
  }, [location.search]);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        PLAYER_BROWSER_PREFS_KEY,
        JSON.stringify({
          query,
          watchlistOnly,
          watchedTeamsOnly,
          recentOnly,
          recentDays,
          openTeams,
          teamSort,
          rowSort,
        })
      );
    } catch {
      // ignore local pref write errors
    }
  }, [openTeams, query, recentDays, recentOnly, rowSort, teamSort, watchedTeamsOnly, watchlistOnly]);

  useEffect(() => {
    const params = new URLSearchParams();
    if (query.trim()) params.set("q", query.trim());
    if (watchlistOnly) params.set("watchlist", "1");
    if (watchedTeamsOnly) params.set("watched_teams", "1");
    if (recentOnly) params.set("recent", "1");
    if (recentDays !== "any") params.set("recent_days", recentDays);
    if (teamSort !== "alpha") params.set("sort", teamSort);
    if (rowSort !== "recent") params.set("row_sort", rowSort);
    const next = params.toString();
    const current = location.search.startsWith("?") ? location.search.slice(1) : location.search;
    if (next === current) return;
    navigate({ pathname: location.pathname, search: next ? `?${next}` : "" }, { replace: true });
  }, [
    location.pathname,
    location.search,
    navigate,
    query,
    recentDays,
    recentOnly,
    rowSort,
    teamSort,
    watchedTeamsOnly,
    watchlistOnly,
  ]);

  const fetchPlayers = useCallback(async ({ silent = false } = {}) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);
      const res = await fetch(`${getBaseURL()}/api/players`);
      if (!res.ok) throw new Error("Failed to fetch player list");
      const data = await res.json();
      setPlayers(data);
      setLoadedAt(new Date().toISOString());
      if (silent) {
        setNotice("Player list refreshed.");
        window.setTimeout(() => setNotice(""), 1400);
      }
    } catch (err) {
      console.error("❌ Error fetching players:", err);
      setError("Unable to load players.");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchPlayers();
  }, [fetchPlayers]);

  useEffect(() => {
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    setWatchlist(readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB));
  }, [user?.id]);

  useEffect(() => {
    function refreshWatchlistFromStorage() {
      if (!user?.id) {
        setWatchlist([]);
        return;
      }
      const next = readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB);
      setWatchlist((prev) => {
        if (JSON.stringify(prev) === JSON.stringify(next)) return prev;
        return next;
      });
    }
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshWatchlistFromStorage();
      }
    }
    function onWatchlistUpdated() {
      refreshWatchlistFromStorage();
    }
    window.addEventListener("storage", onStorage);
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    };
  }, [user?.id]);

  useEffect(() => {
    if (!user?.id) return;
    writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, watchlist);
  }, [user?.id, watchlist]);

  const watchIdSet = useMemo(
    () => new Set(watchlist.map((w) => String(w.id))),
    [watchlist]
  );

  const normalizedPlayers = useMemo(
    () =>
      (players || []).map((p) => ({
        ...p,
        teamLabel: p.team || p.team_abbr || "Unknown",
      })),
    [players]
  );

  const filteredPlayers = useMemo(() => {
    const q = query.trim().toLowerCase();
    const baseRows = watchlistOnly
      ? normalizedPlayers.filter((p) => {
          const id = toWatchlistId({
            player_id: p.player_id,
            player_name: p.player_name,
            team: p.teamLabel,
          });
          return Boolean(id && watchIdSet.has(String(id)));
        })
      : normalizedPlayers;
    const recencyRows = recentOnly
      ? baseRows.filter((p) => {
          const d = toUtcDay(p.last_prop_date);
          if (!d) return false;
          if (recentDays === "any") return true;
          const days = Number(recentDays);
          if (!Number.isFinite(days) || days <= 0) return true;
          const now = new Date();
          const cutoff = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
          cutoff.setUTCDate(cutoff.getUTCDate() - days);
          return d >= cutoff;
        })
      : baseRows;
    if (!q) return recencyRows;
    return recencyRows.filter((p) => {
      const haystack = [
        p.player_name,
        p.player_id,
        p.teamLabel,
      ]
        .map((v) => String(v || "").toLowerCase())
        .join(" ");
      return haystack.includes(q);
    });
  }, [normalizedPlayers, query, recentDays, recentOnly, watchIdSet, watchlistOnly]);

  const groupedByTeam = useMemo(() => {
    const grouped = filteredPlayers.reduce((acc, player) => {
      const team = player.teamLabel || "Unknown";
      if (!acc[team]) acc[team] = [];
      acc[team].push(player);
      return acc;
    }, {});
    if (!watchedTeamsOnly) return grouped;
    const filtered = {};
    for (const [team, rows] of Object.entries(grouped)) {
      const watchedCount = rows.reduce((count, p) => {
        const id = toWatchlistId({
          player_id: p.player_id,
          player_name: p.player_name,
          team: p.teamLabel,
        });
        return count + (id && watchIdSet.has(String(id)) ? 1 : 0);
      }, 0);
      if (watchedCount > 0) filtered[team] = rows;
    }
    return filtered;
  }, [filteredPlayers, watchedTeamsOnly, watchIdSet]);

  const UNKNOWN_TEAM = "Unknown";
  const unknownTeamRows = groupedByTeam[UNKNOWN_TEAM] || [];
  const unknownTeamCount = unknownTeamRows.length;

  const teamNames = useMemo(() => {
    const names = Object.keys(groupedByTeam).filter((name) =>
      showUnknownTeam ? true : name !== UNKNOWN_TEAM
    );
    if (teamSort === "players") {
      return names.sort(
        (a, b) =>
          groupedByTeam[b].length - groupedByTeam[a].length || a.localeCompare(b)
      );
    }
    if (teamSort === "watched") {
      const watchedCountForTeam = (team) =>
        groupedByTeam[team].reduce((count, p) => {
          const id = toWatchlistId({
            player_id: p.player_id,
            player_name: p.player_name,
            team: p.teamLabel,
          });
          return count + (id && watchIdSet.has(String(id)) ? 1 : 0);
        }, 0);
      return names.sort((a, b) => {
        const diff = watchedCountForTeam(b) - watchedCountForTeam(a);
        if (diff !== 0) return diff;
        return groupedByTeam[b].length - groupedByTeam[a].length || a.localeCompare(b);
      });
    }
    return names.sort();
  }, [UNKNOWN_TEAM, groupedByTeam, showUnknownTeam, teamSort, watchIdSet]);
  const visiblePlayerCount = filteredPlayers.length;
  const totalPlayerCount = normalizedPlayers.length;
  const visibleTeamCount = teamNames.length;
  const totalTeamCount = useMemo(
    () => new Set(normalizedPlayers.map((p) => p.teamLabel || "Unknown")).size,
    [normalizedPlayers]
  );

  useEffect(() => {
    if (!query.trim()) return;
    const next = {};
    for (const team of teamNames) next[team] = true;
    setOpenTeams(next);
  }, [query, teamNames]);

  useEffect(() => {
    if (query.trim()) return;
    if (teamNames.length === 0) return;
    if (Object.keys(openTeams).length > 0) return;
    const topTeams = Object.entries(groupedByTeam)
      .sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]))
      .slice(0, 3)
      .map(([team]) => team);
    const next = {};
    for (const team of topTeams) next[team] = true;
    setOpenTeams(next);
  }, [groupedByTeam, openTeams, query, teamNames.length]);

  const freshnessLabel = (d) => {
    if (!d) return "no recent props";
    return `last prop ${d}`;
  };

  function toggleTeam(team) {
    setOpenTeams((prev) => ({ ...prev, [team]: !prev[team] }));
  }

  function setAllTeams(open) {
    const next = {};
    for (const team of teamNames) next[team] = open;
    setOpenTeams(next);
  }

  function jumpToTeam(team) {
    if (!team) return;
    setOpenTeams((prev) => ({ ...prev, [team]: true }));
    const el = teamRefs.current.get(team);
    if (el && typeof el.scrollIntoView === "function") {
      el.scrollIntoView({ block: "start", behavior: "smooth" });
    }
  }

  async function handleCopyViewLink() {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setNotice("Copied current view link.");
      window.setTimeout(() => setNotice(""), 1400);
    } catch {
      setNotice("Failed to copy view link.");
      window.setTimeout(() => setNotice(""), 1400);
    }
  }

  function toggleWatch(player) {
    if (!user?.id) {
      setNotice("Sign in to use watchlist.");
      window.setTimeout(() => setNotice(""), 1600);
      return;
    }
    const row = {
      player_id: player.player_id,
      player_name: player.player_name,
      team: player.teamLabel,
    };
    const id = toWatchlistId(row);
    if (!id) return;
    const exists = watchIdSet.has(String(id));
    setWatchlist((prev) => {
      if (exists) return prev.filter((w) => String(w.id) !== String(id));
      const next = [
        {
          id: String(id),
          player_id: row.player_id ?? null,
          player_name: row.player_name || null,
          team: row.team || null,
          added_at: new Date().toISOString(),
        },
        ...prev,
      ];
      return next.slice(0, 100);
    });
    setNotice(exists ? "Removed from watchlist." : "Added to watchlist.");
    window.setTimeout(() => setNotice(""), 1400);
  }

  function setTeamWatch(rows, targetWatched, bypassConfirm = false) {
    if (!user?.id) {
      setNotice("Sign in to use watchlist.");
      window.setTimeout(() => setNotice(""), 1600);
      return;
    }
    const ids = new Set();
    const mapped = [];
    for (const p of rows || []) {
      const id = toWatchlistId({
        player_id: p.player_id,
        player_name: p.player_name,
        team: p.teamLabel,
      });
      if (!id) continue;
      ids.add(String(id));
      mapped.push({
        id: String(id),
        player_id: p.player_id ?? null,
        player_name: p.player_name || null,
        team: p.teamLabel || null,
      });
    }
    if (ids.size === 0) return;
    if (!bypassConfirm) {
      const action = targetWatched ? "add" : "remove";
      const ok = window.confirm(`Confirm ${action} ${ids.size} player(s) ${targetWatched ? "to" : "from"} watchlist?`);
      if (!ok) return;
    }
    setWatchlist((prev) => {
      if (!targetWatched) {
        return prev.filter((w) => !ids.has(String(w.id)));
      }
      const existing = new Set(prev.map((w) => String(w.id)));
      const toAdd = mapped
        .filter((m) => !existing.has(m.id))
        .map((m) => ({ ...m, added_at: new Date().toISOString() }));
      return [...toAdd, ...prev].slice(0, 100);
    });
    setNotice(
      targetWatched
        ? `Added ${ids.size} team player(s) to watchlist.`
        : `Removed ${ids.size} team player(s) from watchlist.`
    );
    window.setTimeout(() => setNotice(""), 1600);
  }

  if (loading)
    return (
      <div className="min-h-screen pp-page p-6">
        <div className="max-w-5xl mx-auto pp-card p-4 text-slate-600">
          Loading player list...
        </div>
      </div>
    );
  if (error)
    return (
      <div className="min-h-screen pp-page p-6">
        <div className="max-w-5xl mx-auto pp-card p-4 text-rose-600">{error}</div>
      </div>
    );

  return (
    <div className="min-h-screen pp-page p-6">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-2xl font-bold text-slate-900 mb-4">Players by Team</h1>
        <div className="pp-card p-4 mb-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <div className="md:col-span-2">
              <div className="text-xs text-slate-500 mb-1">Search</div>
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Player name, team, or player id..."
                className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
              />
              <label className="mt-2 inline-flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={watchlistOnly}
                  onChange={(e) => setWatchlistOnly(e.target.checked)}
                />
                Watchlist only
              </label>
              <label className="mt-2 ml-4 inline-flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={watchedTeamsOnly}
                  onChange={(e) => setWatchedTeamsOnly(e.target.checked)}
                />
                Watched teams only
              </label>
              <label className="mt-2 ml-4 inline-flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={recentOnly}
                  onChange={(e) => setRecentOnly(e.target.checked)}
                />
                Recent only
              </label>
              <label className="mt-2 ml-4 inline-flex items-center gap-2 text-sm text-slate-700">
                Recent window
                <select
                  value={recentDays}
                  onChange={(e) => setRecentDays(e.target.value)}
                  className="pp-chip px-2 py-1 text-xs text-slate-800"
                >
                  <option value="any">Any recent</option>
                  <option value="7">7d</option>
                  <option value="30">30d</option>
                  <option value="90">90d</option>
                </select>
              </label>
            </div>
            <div>
              <div className="text-xs text-slate-500 mb-1">Team order</div>
              <select
                value={teamSort}
                onChange={(e) => setTeamSort(e.target.value)}
                className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
              >
                <option value="alpha">A–Z</option>
                <option value="players">Most players</option>
                <option value="watched">Most watched</option>
              </select>
            </div>
            <div>
              <div className="text-xs text-slate-500 mb-1">Player order</div>
              <select
                value={rowSort}
                onChange={(e) => setRowSort(e.target.value)}
                className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
              >
                <option value="recent">Recent first</option>
                <option value="name">Name</option>
                <option value="watched">Watched first</option>
              </select>
            </div>
            <div>
              <div className="text-xs text-slate-500 mb-1">Jump to team</div>
              <select
                defaultValue=""
                onChange={(e) => {
                  jumpToTeam(e.target.value);
                  e.target.value = "";
                }}
                className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
              >
                <option value="" disabled>
                  Select team...
                </option>
                {teamNames.map((team) => (
                  <option key={`jump-${team}`} value={team}>
                    {team}
                  </option>
                ))}
              </select>
            </div>
            <div className="md:col-span-3 flex flex-wrap items-center gap-2 md:justify-end">
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setQuery("")}
                disabled={!query.trim()}
              >
                Clear Search
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => {
                  setQuery("");
                  setWatchlistOnly(false);
                  setWatchedTeamsOnly(false);
                  setRecentOnly(false);
                  setRecentDays("any");
                  setOpenTeams({});
                  setTeamSort("alpha");
                  setRowSort("recent");
                }}
              >
                Reset View
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setAllTeams(true)}
              >
                Expand all
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setAllTeams(false)}
              >
                Collapse all
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={handleCopyViewLink}
              >
                Copy View Link
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => fetchPlayers({ silent: true })}
                disabled={refreshing}
              >
                {refreshing ? "Refreshing..." : "Refresh Players"}
              </button>
            </div>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Loaded: {loadedAt ? new Date(loadedAt).toLocaleString() : "—"}
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs">
              Players <strong>{visiblePlayerCount}/{totalPlayerCount}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 text-blue-700 px-2 py-1 text-xs">
              Teams <strong>{visibleTeamCount}/{totalTeamCount}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
              Watching <strong>{watchlist.length}</strong>
            </span>
            {unknownTeamCount > 0 ? (
              <button
                type="button"
                className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs hover:bg-slate-200"
                onClick={() => setShowUnknownTeam((prev) => !prev)}
                title={showUnknownTeam ? "Hide Unknown team bucket" : "Show Unknown team bucket"}
              >
                Unknown <strong>{unknownTeamCount}</strong> {showUnknownTeam ? "hide" : "show"}
              </button>
            ) : null}
            {watchlistOnly ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 text-amber-700 px-2 py-1 text-xs">
                Filter <strong>Watchlist only</strong>
              </span>
            ) : null}
            {watchedTeamsOnly ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-purple-100 text-purple-700 px-2 py-1 text-xs">
                Filter <strong>Watched teams only</strong>
              </span>
            ) : null}
            {recentOnly ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-cyan-100 text-cyan-700 px-2 py-1 text-xs">
                Filter <strong>{recentDays === "any" ? "Recent only" : `Recent ${recentDays}d`}</strong>
              </span>
            ) : null}
          </div>
          {notice ? (
            <div className="mt-3 text-sm text-emerald-700 bg-emerald-50 rounded-md px-3 py-2">
              {notice}
            </div>
          ) : null}
        </div>

        {teamNames.length === 0 ? (
          <div className="pp-card p-4 text-slate-600">
            <div className="font-medium text-slate-800">No players match this view.</div>
            <div className="mt-1 text-sm">
              Try clearing one or more filters ({[
                query.trim() ? "search" : null,
                watchlistOnly ? "watchlist only" : null,
                watchedTeamsOnly ? "watched teams only" : null,
                recentOnly ? "recent only" : null,
              ].filter(Boolean).join(", ") || "none active"}).
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setQuery("")}
                disabled={!query.trim()}
              >
                Clear Search
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => {
                  setQuery("");
                  setWatchlistOnly(false);
                  setWatchedTeamsOnly(false);
                  setRecentOnly(false);
                  setOpenTeams({});
                  setTeamSort("alpha");
                  setRowSort("recent");
                }}
              >
                Reset All Filters
              </button>
            </div>
          </div>
        ) : (
          teamNames.map((team) => {
            const rows = groupedByTeam[team]
              .slice()
              .sort((a, b) => {
                if (rowSort === "watched") {
                  const aw = watchIdSet.has(
                    String(
                      toWatchlistId({
                        player_id: a.player_id,
                        player_name: a.player_name,
                        team: a.teamLabel,
                      })
                    )
                  );
                  const bw = watchIdSet.has(
                    String(
                      toWatchlistId({
                        player_id: b.player_id,
                        player_name: b.player_name,
                        team: b.teamLabel,
                      })
                    )
                  );
                  if (aw !== bw) return bw ? 1 : -1;
                }
                if (rowSort === "name") {
                  return String(a.player_name || "").localeCompare(String(b.player_name || ""));
                }
                const ad = a.last_prop_date || "";
                const bd = b.last_prop_date || "";
                if (ad !== bd) return bd.localeCompare(ad);
                return String(a.player_name || "").localeCompare(String(b.player_name || ""));
              });
            const watchedCount = rows.reduce((count, p) => {
              const id = toWatchlistId({
                player_id: p.player_id,
                player_name: p.player_name,
                team: p.teamLabel,
              });
              return count + (id && watchIdSet.has(String(id)) ? 1 : 0);
            }, 0);
            const isOpen = Boolean(openTeams[team]);
            return (
              <div
                key={team}
                ref={(el) => {
                  if (el) teamRefs.current.set(team, el);
                  else teamRefs.current.delete(team);
                }}
                className="mb-2 pp-card p-4"
              >
                <button
                  type="button"
                  className="w-full flex items-center justify-between text-left"
                  onClick={() => toggleTeam(team)}
                >
                  <h2 className="text-xl font-semibold text-slate-900">
                    {team}{" "}
                    <span className="text-sm text-slate-500 font-normal">({rows.length})</span>
                    <span className="ml-2 text-xs text-emerald-700 font-medium">
                      watched {watchedCount}/{rows.length}
                    </span>
                  </h2>
                  <span className="text-slate-500 text-sm">{isOpen ? "Hide" : "Show"}</span>
                </button>
                <div className="mt-1 flex items-center gap-2">
                  <Link
                    to={`/props?player=${playerQuery(team)}`}
                    className="text-xs text-slate-500 hover:underline"
                  >
                    Open Team Props
                  </Link>
                </div>
                {isOpen ? (
                  <>
                    <div className="mt-2 mb-2 flex items-center gap-2">
                      <button
                        type="button"
                        className="pp-btn pp-btn-secondary pp-btn-sm"
                        onClick={(e) => setTeamWatch(rows, true, e.shiftKey)}
                        title="Adds all players in this team section to watchlist (Shift+Click skips confirm)"
                      >
                        Watch all
                      </button>
                      <button
                        type="button"
                        className="pp-btn pp-btn-secondary pp-btn-sm"
                        onClick={(e) => setTeamWatch(rows, false, e.shiftKey)}
                        title="Removes all players in this team section from watchlist (Shift+Click skips confirm)"
                      >
                        Unwatch all
                      </button>
                    </div>
                    <ul className="space-y-1 mt-2">
                    {rows.map((p) => {
                      const id = toWatchlistId({
                        player_id: p.player_id,
                        player_name: p.player_name,
                        team: p.teamLabel,
                      });
                      const isWatched = Boolean(id && watchIdSet.has(String(id)));
                      return (
                        <li
                          key={p.player_id}
                          className={`flex items-center justify-between gap-3 pp-chip px-2 py-1 ${
                            isWatched ? "bg-emerald-50 border border-emerald-200" : ""
                          }`}
                        >
                          <div className="min-w-0">
                            <Link to={`/player/${p.player_id}`} className="text-slate-700 hover:underline">
                              {p.player_name || p.player_id}
                            </Link>
                            <div className="mt-0.5">
                              <Link
                                to={`/props?player=${playerQuery(p.player_name || p.player_id)}`}
                                className="text-xs text-slate-500 hover:underline"
                              >
                                Open Props
                              </Link>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            {isWatched ? (
                              <span className="inline-flex items-center gap-1 text-[11px] text-emerald-700">
                                <span className="h-1.5 w-1.5 rounded-full bg-emerald-600" />
                                Watched
                              </span>
                            ) : null}
                            <span className="text-xs text-slate-500">{freshnessLabel(p.last_prop_date)}</span>
                            <button
                              type="button"
                              className="pp-btn pp-btn-secondary pp-btn-sm"
                              onClick={() => toggleWatch(p)}
                            >
                              {isWatched ? "Watching" : "+ Watch"}
                            </button>
                          </div>
                        </li>
                      );
                    })}
                    </ul>
                  </>
                ) : null}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
