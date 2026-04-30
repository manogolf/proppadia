import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";
import { normalizeHttpErrorMessage } from "../shared/httpErrorMessage.js";
import {
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_MLB,
  WATCHLIST_SCOPE_NHL,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../shared/watchlistStorage.js";

const PLAYER_BROWSER_PREFS_KEY = "proppadia_player_browser_prefs_v2";
const PLAYER_BROWSER_SESSION_CACHE_PREFIX = "proppadia_player_browser_session_v1";
const UNKNOWN_TEAM = "Unknown";
const MLB_UNASSIGNED_TEAM = "Unassigned / Minors / Unknown";
const PLAYER_SUGGESTION_MIN_CHARS = 1;
const playerBrowserSessionCache = new Map();

function playerBrowserCacheDate() {
  return new Date().toISOString().slice(0, 10);
}

function playerBrowserCacheKey(sport) {
  return `${PLAYER_BROWSER_SESSION_CACHE_PREFIX}:${sport}:${playerBrowserCacheDate()}`;
}

function readPlayerBrowserCache(sport) {
  const key = playerBrowserCacheKey(sport);
  if (playerBrowserSessionCache.has(key)) return playerBrowserSessionCache.get(key);
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    playerBrowserSessionCache.set(key, parsed);
    return parsed;
  } catch {
    return null;
  }
}

function writePlayerBrowserCache(sport, payload) {
  const key = playerBrowserCacheKey(sport);
  const safePayload = {
    ...payload,
    cachedAt: new Date().toISOString(),
    cacheDate: playerBrowserCacheDate(),
  };
  playerBrowserSessionCache.set(key, safePayload);
  try {
    window.sessionStorage.setItem(key, JSON.stringify(safePayload));
  } catch {
    // ignore session cache write errors
  }
}

const NHL_ACTIVE_TEAM_ABBRS = new Set([
  "ANA",
  "BOS",
  "BUF",
  "CAR",
  "CBJ",
  "CGY",
  "CHI",
  "COL",
  "DAL",
  "DET",
  "EDM",
  "FLA",
  "LAK",
  "MIN",
  "MTL",
  "NJD",
  "NSH",
  "NYI",
  "NYR",
  "OTT",
  "PHI",
  "PIT",
  "SEA",
  "SJS",
  "STL",
  "TBL",
  "TOR",
  "UTA",
  "VAN",
  "VGK",
  "WPG",
  "WSH",
]);

const NHL_TEAM_ALIASES = {
  ARI: "UTA",
  PHX: "UTA",
  UTAH: "UTA",
};

const MLB_ACTIVE_TEAM_ABBRS = new Set([
  "ARI",
  "ATL",
  "BAL",
  "BOS",
  "CHC",
  "CIN",
  "CLE",
  "COL",
  "CWS",
  "DET",
  "HOU",
  "KC",
  "LAA",
  "LAD",
  "MIA",
  "MIL",
  "MIN",
  "NYM",
  "NYY",
  "OAK",
  "PHI",
  "PIT",
  "SD",
  "SEA",
  "SF",
  "STL",
  "TB",
  "TEX",
  "TOR",
  "WSH",
]);

const MLB_TEAM_ALIASES = {
  ATH: "OAK",
  AZ: "ARI",
  LV: "OAK",
  VIL: "OAK",
};

function normalizeTeamLabelBySport(rawValue, sport) {
  const raw = String(rawValue || "").trim();
  if (!raw) return sport === "mlb" ? MLB_UNASSIGNED_TEAM : UNKNOWN_TEAM;
  if (sport === "mlb") {
    const upper = raw.toUpperCase();
    const mapped = MLB_TEAM_ALIASES[upper] || upper;
    return MLB_ACTIVE_TEAM_ABBRS.has(mapped) ? mapped : MLB_UNASSIGNED_TEAM;
  }
  if (sport !== "nhl") return raw;
  const upper = raw.toUpperCase();
  const mapped = NHL_TEAM_ALIASES[upper] || upper;
  return NHL_ACTIVE_TEAM_ABBRS.has(mapped) ? mapped : UNKNOWN_TEAM;
}

function playerQuery(value) {
  return encodeURIComponent(String(value || "").trim());
}

function normalizeSearchValue(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function compactSearchValue(value) {
  return normalizeSearchValue(value).replace(/\s+/g, "");
}

function getPlayerName(player) {
  const direct = String(player?.player_name || player?.name || player?.full_name || "").trim();
  if (direct) return direct;
  const firstLast = [player?.first_name, player?.last_name]
    .map((v) => String(v || "").trim())
    .filter(Boolean)
    .join(" ");
  return firstLast || String(player?.player_id || "").trim();
}

function playerSuggestionKey(player) {
  const id = player?.player_id != null ? String(player.player_id) : "";
  if (id) return id;
  return `${normalizeSearchValue(getPlayerName(player))}:${normalizeSearchValue(player?.teamLabel)}`;
}

function playerSearchText(player) {
  const values = [
    getPlayerName(player),
    player?.player_id,
    player?.teamLabel,
    player?.team,
    player?.team_abbr,
    player?.team_abbreviation,
  ];
  const normalized = values.map(normalizeSearchValue).filter(Boolean).join(" ");
  const compactName = compactSearchValue(getPlayerName(player));
  return `${normalized} ${compactName}`.trim();
}

function playerPositionLabel(player) {
  return String(player?.position || player?.primary_position || player?.pos || "").trim();
}

function toUtcDay(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const d = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export default function PlayerTeamBrowser({ forcedSport = null }) {
  const location = useLocation();
  const navigate = useNavigate();
  const { user } = useAuth();
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState("");
  const [sport, setSport] = useState(forcedSport === "nhl" ? "nhl" : "mlb");
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [watchedTeamsOnly, setWatchedTeamsOnly] = useState(false);
  const [nhlSlateOnly, setNhlSlateOnly] = useState(false);
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
  const [nhlSlatePlayerIds, setNhlSlatePlayerIds] = useState(new Set());
  const [suggestionsOpen, setSuggestionsOpen] = useState(false);
  const [highlightedSuggestionIndex, setHighlightedSuggestionIndex] = useState(0);
  const teamRefs = useRef(new Map());
  const playerRefs = useRef(new Map());
  const searchWrapRef = useRef(null);
  const restoredCacheKeyRef = useRef("");
  const inactiveTeamLabel = sport === "mlb" ? MLB_UNASSIGNED_TEAM : UNKNOWN_TEAM;

  useEffect(() => {
    if (forcedSport) return;
    try {
      const raw = window.localStorage.getItem(PLAYER_BROWSER_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed?.sport === "mlb" || parsed?.sport === "nhl") setSport(parsed.sport);
      if (typeof parsed?.query === "string") setQuery(parsed.query);
      if (typeof parsed?.watchlistOnly === "boolean") setWatchlistOnly(parsed.watchlistOnly);
      if (typeof parsed?.watchedTeamsOnly === "boolean") setWatchedTeamsOnly(parsed.watchedTeamsOnly);
      if (typeof parsed?.nhlSlateOnly === "boolean") setNhlSlateOnly(parsed.nhlSlateOnly);
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
  }, [forcedSport]);

  useEffect(() => {
    if (forcedSport) return;
    const params = new URLSearchParams(location.search || "");
    const q = String(params.get("q") || "");
    const sportParam = String(params.get("sport") || "").toLowerCase();
    const wl = String(params.get("watchlist") || "").toLowerCase() === "1";
    const wt = String(params.get("watched_teams") || "").toLowerCase() === "1";
    const slateOnly = String(params.get("slate_only") || "").toLowerCase() === "1";
    const recent = String(params.get("recent") || "").toLowerCase() === "1";
    const rdays = String(params.get("recent_days") || "");
    const sort = String(params.get("sort") || "");
    const rowSortParam = String(params.get("row_sort") || "");
    if (sportParam === "mlb" || sportParam === "nhl") setSport(sportParam);
    if (q) setQuery(q);
    if (wl) setWatchlistOnly(true);
    if (wt) setWatchedTeamsOnly(true);
    if (slateOnly) setNhlSlateOnly(true);
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
  }, [forcedSport, location.search]);

  useEffect(() => {
    if (forcedSport) return;
    try {
      window.localStorage.setItem(
        PLAYER_BROWSER_PREFS_KEY,
        JSON.stringify({
          sport,
          query,
          watchlistOnly,
          watchedTeamsOnly,
          nhlSlateOnly,
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
  }, [forcedSport, nhlSlateOnly, openTeams, query, recentDays, recentOnly, rowSort, sport, teamSort, watchedTeamsOnly, watchlistOnly]);

  useEffect(() => {
    if (forcedSport) return;
    const params = new URLSearchParams();
    if (sport !== "mlb") params.set("sport", sport);
    if (query.trim()) params.set("q", query.trim());
    if (watchlistOnly) params.set("watchlist", "1");
    if (watchedTeamsOnly) params.set("watched_teams", "1");
    if (sport === "nhl" && nhlSlateOnly) params.set("slate_only", "1");
    if (recentOnly) params.set("recent", "1");
    if (recentDays !== "any") params.set("recent_days", recentDays);
    if (teamSort !== "alpha") params.set("sort", teamSort);
    if (rowSort !== "recent") params.set("row_sort", rowSort);
    const next = params.toString();
    const current = location.search.startsWith("?") ? location.search.slice(1) : location.search;
    if (next === current) return;
    navigate({ pathname: location.pathname, search: next ? `?${next}` : "" }, { replace: true });
  }, [
    forcedSport,
    location.pathname,
    location.search,
    navigate,
    query,
    recentDays,
    recentOnly,
    rowSort,
    sport,
    teamSort,
    watchedTeamsOnly,
    nhlSlateOnly,
    watchlistOnly,
  ]);

  useEffect(() => {
    if (forcedSport === "mlb" || forcedSport === "nhl") setSport(forcedSport);
  }, [forcedSport]);

  const watchlistScope = sport === "nhl" ? WATCHLIST_SCOPE_NHL : WATCHLIST_SCOPE_MLB;

  const fetchPlayers = useCallback(async ({ silent = false } = {}) => {
    try {
      if (!silent) setLoading(true);
      else setRefreshing(true);
      setError(null);
      let nextPlayers = [];
      let nextNhlSlatePlayerIds = new Set();
      if (sport === "mlb") {
        const res = await fetch(`${getBaseURL()}/api/mlb/players?limit=5000`);
        if (!res.ok) throw new Error("Failed to fetch MLB player list");
        const payload = await res.json().catch(() => []);
        const data = Array.isArray(payload)
          ? payload
          : Array.isArray(payload?.rows)
            ? payload.rows
            : [];
        nextPlayers = data;
        setPlayers(nextPlayers);
        setNhlSlatePlayerIds(nextNhlSlatePlayerIds);
      } else {
        const res = await fetch(`${getBaseURL()}/api/nhl/players?limit=5000&offset=0`);
        const payload = await res.json().catch(() => []);
        if (!res.ok) throw new Error("Failed to fetch NHL player list");
        const rows = Array.isArray(payload)
          ? payload
          : Array.isArray(payload?.rows)
            ? payload.rows
            : [];
        const dedup = new Map();
        for (const row of rows) {
          const pid = row?.player_id != null ? String(row.player_id) : "";
          const name = String(row?.player_name || "").trim();
          const team = String(row?.team_abbr || row?.team || "").trim();
          const key = pid || `${name.toLowerCase()}:${team.toLowerCase()}`;
          if (!key) continue;
          if (!dedup.has(key)) {
            dedup.set(key, {
              player_id: row?.player_id ?? null,
              player_name: name || row?.player_id,
              team: team || null,
              team_abbr: team || null,
              last_prop_date: row?.last_prop_date || null,
            });
          }
        }
        nextPlayers = Array.from(dedup.values());
        setPlayers(nextPlayers);
        const ids = new Set();
        let offset = 0;
        const pageLimit = 200;
        let total = Infinity;
        while (offset < total && offset < 5000) {
          const slateRes = await fetch(
            `${getBaseURL()}/api/nhl/props/today?limit=${pageLimit}&offset=${offset}`
          );
          const slatePayload = await slateRes.json().catch(() => ({}));
          if (!slateRes.ok || !Array.isArray(slatePayload?.rows)) {
            break;
          }
          for (const row of slatePayload.rows) {
            const pid = row?.player_id != null ? String(row.player_id) : "";
            if (pid) ids.add(pid);
          }
          total = Number(slatePayload?.total ?? slatePayload.rows.length);
          if (slatePayload.rows.length < pageLimit) break;
          offset += pageLimit;
        }
        nextNhlSlatePlayerIds = ids;
        setNhlSlatePlayerIds(nextNhlSlatePlayerIds);
      }
      const nextLoadedAt = new Date().toISOString();
      setLoadedAt(nextLoadedAt);
      writePlayerBrowserCache(sport, {
        players: nextPlayers,
        nhlSlatePlayerIds: Array.from(nextNhlSlatePlayerIds),
        loadedAt: nextLoadedAt,
        viewState: {
          query,
          watchlistOnly,
          watchedTeamsOnly,
          nhlSlateOnly,
          recentOnly,
          recentDays,
          openTeams,
          teamSort,
          rowSort,
          showUnknownTeam,
          scrollY: window.scrollY || 0,
        },
      });
      if (silent) {
        setNotice(`${sport.toUpperCase()} player list refreshed.`);
        window.setTimeout(() => setNotice(""), 1400);
      }
    } catch (err) {
      console.error("❌ Error fetching players:", err);
      const msg = normalizeHttpErrorMessage(err, `Unable to load ${sport.toUpperCase()} players.`);
      setError(msg);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sport]);

  useEffect(() => {
    const key = playerBrowserCacheKey(sport);
    const cached = readPlayerBrowserCache(sport);
    if (cached && Array.isArray(cached.players) && cached.players.length > 0) {
      setPlayers(cached.players);
      setNhlSlatePlayerIds(new Set(cached.nhlSlatePlayerIds || []));
      setLoadedAt(cached.loadedAt || cached.cachedAt || null);
      setError(null);
      setLoading(false);
      const view = cached.viewState || {};
      if (typeof view.query === "string") setQuery(view.query);
      if (typeof view.watchlistOnly === "boolean") setWatchlistOnly(view.watchlistOnly);
      if (typeof view.watchedTeamsOnly === "boolean") setWatchedTeamsOnly(view.watchedTeamsOnly);
      if (typeof view.nhlSlateOnly === "boolean") setNhlSlateOnly(view.nhlSlateOnly);
      if (typeof view.recentOnly === "boolean") setRecentOnly(view.recentOnly);
      if (view.recentDays === "any" || view.recentDays === "7" || view.recentDays === "30" || view.recentDays === "90") {
        setRecentDays(view.recentDays);
      }
      if (view.openTeams && typeof view.openTeams === "object") setOpenTeams(view.openTeams);
      if (view.teamSort === "alpha" || view.teamSort === "players" || view.teamSort === "watched") {
        setTeamSort(view.teamSort);
      }
      if (view.rowSort === "recent" || view.rowSort === "name" || view.rowSort === "watched") {
        setRowSort(view.rowSort);
      }
      if (typeof view.showUnknownTeam === "boolean") setShowUnknownTeam(view.showUnknownTeam);
      if (restoredCacheKeyRef.current !== key) {
        restoredCacheKeyRef.current = key;
        window.requestAnimationFrame(() => {
          const scrollY = Number(view.scrollY);
          if (Number.isFinite(scrollY) && scrollY > 0) {
            window.scrollTo({ top: scrollY, behavior: "auto" });
          }
        });
      }
      return;
    }
    fetchPlayers();
  }, [fetchPlayers, sport]);

  const savePlayerBrowserCache = useCallback(() => {
    if (!Array.isArray(players) || players.length === 0) return;
    writePlayerBrowserCache(sport, {
      players,
      nhlSlatePlayerIds: Array.from(nhlSlatePlayerIds || []),
      loadedAt,
      viewState: {
        query,
        watchlistOnly,
        watchedTeamsOnly,
        nhlSlateOnly,
        recentOnly,
        recentDays,
        openTeams,
        teamSort,
        rowSort,
        showUnknownTeam,
        scrollY: window.scrollY || 0,
      },
    });
  }, [
    loadedAt,
    nhlSlateOnly,
    nhlSlatePlayerIds,
    openTeams,
    players,
    query,
    recentDays,
    recentOnly,
    rowSort,
    showUnknownTeam,
    sport,
    teamSort,
    watchedTeamsOnly,
    watchlistOnly,
  ]);

  useEffect(() => {
    function onPageHide() {
      savePlayerBrowserCache();
    }
    window.addEventListener("pagehide", onPageHide);
    return () => {
      savePlayerBrowserCache();
      window.removeEventListener("pagehide", onPageHide);
    };
  }, [savePlayerBrowserCache]);

  useEffect(() => {
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    setWatchlist(readWatchlistScope(user.id, watchlistScope));
  }, [user?.id, watchlistScope]);

  useEffect(() => {
    function refreshWatchlistFromStorage() {
      if (!user?.id) {
        setWatchlist([]);
        return;
      }
      const next = readWatchlistScope(user.id, watchlistScope);
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
  }, [user?.id, watchlistScope]);

  useEffect(() => {
    if (!user?.id) return;
    writeWatchlistScope(user.id, watchlistScope, watchlist);
  }, [user?.id, watchlist, watchlistScope]);

  const watchIdSet = useMemo(
    () => new Set(watchlist.map((w) => String(w.id))),
    [watchlist]
  );

  const normalizedPlayers = useMemo(
    () =>
      (players || []).map((p) => {
        const playerName = getPlayerName(p);
        return {
          ...p,
          player_name: playerName,
          teamLabel: normalizeTeamLabelBySport(p.team || p.team_abbr || p.team_abbreviation, sport),
        };
      }),
    [players, sport]
  );

  const candidatePlayers = useMemo(() => {
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
    const slateRows =
      sport === "nhl" && nhlSlateOnly
        ? baseRows.filter((p) => p?.player_id != null && nhlSlatePlayerIds.has(String(p.player_id)))
        : baseRows;
    const recencyRows = recentOnly
      ? slateRows.filter((p) => {
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
      : slateRows;
    return recencyRows;
  }, [nhlSlateOnly, nhlSlatePlayerIds, normalizedPlayers, recentDays, recentOnly, sport, watchIdSet, watchlistOnly]);

  const filteredPlayers = useMemo(() => {
    const q = normalizeSearchValue(query);
    const compactQ = compactSearchValue(query);
    if (!q) return candidatePlayers;
    return candidatePlayers.filter((p) => {
      const searchText = playerSearchText(p);
      return searchText.includes(q) || Boolean(compactQ && searchText.includes(compactQ));
    });
  }, [candidatePlayers, query]);

  const playerSuggestions = useMemo(() => {
    const q = normalizeSearchValue(query);
    const compactQ = compactSearchValue(query);
    if (q.length < PLAYER_SUGGESTION_MIN_CHARS) return [];
    return normalizedPlayers
      .filter((p) => {
        const searchText = playerSearchText(p);
        return searchText.includes(q) || Boolean(compactQ && searchText.includes(compactQ));
      })
      .slice(0, 10);
  }, [normalizedPlayers, query]);

  useEffect(() => {
    setHighlightedSuggestionIndex(0);
  }, [playerSuggestions.length, query]);

  useEffect(() => {
    function handleDocumentPointerDown(e) {
      if (!searchWrapRef.current) return;
      if (!searchWrapRef.current.contains(e.target)) setSuggestionsOpen(false);
    }
    document.addEventListener("mousedown", handleDocumentPointerDown);
    return () => document.removeEventListener("mousedown", handleDocumentPointerDown);
  }, []);

  function selectPlayerSuggestion(player) {
    if (!player) return;
    const name = getPlayerName(player);
    const team = player.teamLabel || inactiveTeamLabel;
    const key = playerSuggestionKey(player);
    const isVisibleInCurrentView = candidatePlayers.some((p) => playerSuggestionKey(p) === key);
    setQuery(name);
    setSuggestionsOpen(false);
    setHighlightedSuggestionIndex(0);
    if (!isVisibleInCurrentView) {
      setWatchlistOnly(false);
      setWatchedTeamsOnly(false);
      setNhlSlateOnly(false);
      setRecentOnly(false);
      setRecentDays("any");
    }
    setOpenTeams((prev) => ({ ...prev, [team]: true }));
    window.setTimeout(() => {
      const row = playerRefs.current.get(key);
      if (row && typeof row.scrollIntoView === "function") {
        row.scrollIntoView({ block: "center", behavior: "smooth" });
        if (typeof row.focus === "function") row.focus({ preventScroll: true });
        return;
      }
      const teamEl = teamRefs.current.get(team);
      if (teamEl && typeof teamEl.scrollIntoView === "function") {
        teamEl.scrollIntoView({ block: "start", behavior: "smooth" });
      }
    }, 0);
  }

  function handleSearchKeyDown(e) {
    if (e.key === "Escape") {
      setSuggestionsOpen(false);
      return;
    }
    if (normalizeSearchValue(query).length < PLAYER_SUGGESTION_MIN_CHARS || playerSuggestions.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSuggestionsOpen(true);
      setHighlightedSuggestionIndex((prev) =>
        Math.min(prev + 1, playerSuggestions.length - 1)
      );
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      setSuggestionsOpen(true);
      setHighlightedSuggestionIndex((prev) => Math.max(prev - 1, 0));
      return;
    }
    if (e.key === "Enter" && suggestionsOpen) {
      const selected = playerSuggestions[highlightedSuggestionIndex] || playerSuggestions[0];
      if (selected) {
        e.preventDefault();
        selectPlayerSuggestion(selected);
      }
    }
  }

  const groupedByTeam = useMemo(() => {
    const grouped = filteredPlayers.reduce((acc, player) => {
      const team = player.teamLabel || inactiveTeamLabel;
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
  }, [filteredPlayers, inactiveTeamLabel, watchedTeamsOnly, watchIdSet]);

  const unknownTeamRows = groupedByTeam[inactiveTeamLabel] || [];
  const unknownTeamCount = unknownTeamRows.length;

  const teamNames = useMemo(() => {
    const names = Object.keys(groupedByTeam).filter((name) =>
      showUnknownTeam ? true : name !== inactiveTeamLabel
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
  }, [groupedByTeam, inactiveTeamLabel, showUnknownTeam, teamSort, watchIdSet]);
  const visiblePlayerCount = teamNames.reduce(
    (count, team) => count + (groupedByTeam[team]?.length || 0),
    0
  );
  const totalPlayerCount = normalizedPlayers.length;
  const hasSearchText = Boolean(query.trim());
  const emptyStateTitle = hasSearchText
    ? "No matching players."
    : totalPlayerCount === 0
      ? "No player data loaded."
      : "No players match this view.";
  const emptyStateDetail = hasSearchText
    ? "Try a different player name, team abbreviation, or player id."
    : totalPlayerCount === 0
      ? "Refresh the player list or try again shortly."
      : `Try clearing one or more filters (${[
          watchlistOnly ? "watchlist only" : null,
          watchedTeamsOnly ? "watched teams only" : null,
          sport === "nhl" && nhlSlateOnly ? "in today's slate" : null,
          recentOnly ? "recent only" : null,
        ].filter(Boolean).join(", ") || "none active"}).`;
  const visibleTeamCount = useMemo(
    () => teamNames.filter((name) => name !== inactiveTeamLabel).length,
    [inactiveTeamLabel, teamNames]
  );
  const totalTeamCount = useMemo(
    () =>
      new Set(
        normalizedPlayers
          .map((p) => p.teamLabel || inactiveTeamLabel)
          .filter((name) => name !== inactiveTeamLabel)
      ).size,
    [inactiveTeamLabel, normalizedPlayers]
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
    if (!d) return "";
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
        <h1 className="text-2xl font-bold text-slate-900 mb-4">
          {sport === "nhl" ? "NHL Players by Team" : "MLB Players by Team"}
        </h1>
        <div className="pp-card p-4 mb-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <div className="md:col-span-2">
              <div className="text-xs text-slate-500 mb-1">Search</div>
              <div className="relative" ref={searchWrapRef}>
                <input
                  value={query}
                  onChange={(e) => {
                    const next = e.target.value;
                    setQuery(next);
                    setSuggestionsOpen(normalizeSearchValue(next).length >= PLAYER_SUGGESTION_MIN_CHARS);
                    setHighlightedSuggestionIndex(0);
                  }}
                  onFocus={() => {
                    if (normalizeSearchValue(query).length >= PLAYER_SUGGESTION_MIN_CHARS) setSuggestionsOpen(true);
                  }}
                  onKeyDown={handleSearchKeyDown}
                  placeholder="Player name, team, or player id..."
                  className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                  aria-autocomplete="list"
                  aria-expanded={suggestionsOpen && normalizeSearchValue(query).length >= PLAYER_SUGGESTION_MIN_CHARS}
                />
                {suggestionsOpen && normalizeSearchValue(query).length >= PLAYER_SUGGESTION_MIN_CHARS ? (
                  <div className="absolute left-0 right-0 top-full z-30 mt-1 overflow-hidden rounded-md border border-slate-200 bg-white shadow-lg">
                    {playerSuggestions.length > 0 ? (
                      <div className="max-h-80 overflow-auto py-1">
                        {playerSuggestions.map((player, index) => {
                          const team = player.teamLabel || inactiveTeamLabel;
                          const position = playerPositionLabel(player);
                          const isHighlighted = index === highlightedSuggestionIndex;
                          return (
                            <button
                              key={`suggestion-${playerSuggestionKey(player)}`}
                              type="button"
                              className={`w-full px-3 py-2 text-left text-sm ${
                                isHighlighted ? "bg-slate-100" : "bg-white hover:bg-slate-50"
                              }`}
                              onMouseEnter={() => setHighlightedSuggestionIndex(index)}
                              onMouseDown={(e) => {
                                e.preventDefault();
                                selectPlayerSuggestion(player);
                              }}
                            >
                              <div className="flex items-center justify-between gap-3">
                                <span className="min-w-0 truncate font-medium text-slate-800">
                                  {getPlayerName(player)}
                                </span>
                                <span className="shrink-0 text-xs text-slate-500">
                                  {team}{position ? ` · ${position}` : ""}
                                </span>
                              </div>
                              {player.player_id != null ? (
                                <div className="mt-0.5 text-xs text-slate-400">
                                  ID {player.player_id}
                                </div>
                              ) : null}
                            </button>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="px-3 py-2 text-sm text-slate-500">
                        No matching players.
                      </div>
                    )}
                  </div>
                ) : null}
              </div>
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
              {sport === "nhl" ? (
                <label className="mt-2 ml-4 inline-flex items-center gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={nhlSlateOnly}
                    onChange={(e) => setNhlSlateOnly(e.target.checked)}
                  />
                  In today&apos;s slate
                </label>
              ) : null}
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
                onClick={() => {
                  setQuery("");
                  setSuggestionsOpen(false);
                }}
                disabled={!query.trim()}
              >
                Clear Search
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => {
                  setQuery("");
                  setSuggestionsOpen(false);
                  setWatchlistOnly(false);
                  setWatchedTeamsOnly(false);
                  setNhlSlateOnly(false);
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
                title={showUnknownTeam ? `Hide ${inactiveTeamLabel} bucket` : `Show ${inactiveTeamLabel} bucket`}
              >
                {inactiveTeamLabel} <strong>{unknownTeamCount}</strong> {showUnknownTeam ? "hide" : "show"}
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
            {sport === "nhl" && nhlSlateOnly ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
                Filter <strong>In today&apos;s slate</strong>
              </span>
            ) : null}
            {recentOnly ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-cyan-100 text-cyan-700 px-2 py-1 text-xs">
                Filter <strong>{recentDays === "any" ? "Recent only" : `Recent ${recentDays}d`}</strong>
              </span>
            ) : null}
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Team header counts reflect player rows currently in this view (after filters), not official game-day roster size.
            {unknownTeamCount > 0 ? ` ${inactiveTeamLabel} rows are excluded unless shown.` : ""}
          </div>
          {notice ? (
            <div className="mt-3 text-sm text-emerald-700 bg-emerald-50 rounded-md px-3 py-2">
              {notice}
            </div>
          ) : null}
        </div>

        {teamNames.length === 0 ? (
          <div className="pp-card p-4 text-slate-600">
            <div className="font-medium text-slate-800">{emptyStateTitle}</div>
            <div className="mt-1 text-sm">{emptyStateDetail}</div>
            <div className="mt-3 flex items-center gap-2">
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => {
                  setQuery("");
                  setSuggestionsOpen(false);
                }}
                disabled={!query.trim()}
              >
                Clear Search
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => {
                  setQuery("");
                  setSuggestionsOpen(false);
                  setWatchlistOnly(false);
                  setWatchedTeamsOnly(false);
                  setNhlSlateOnly(false);
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
                  const aId = toWatchlistId({
                    player_id: a.player_id,
                    player_name: a.player_name,
                    team: a.teamLabel,
                  });
                  const bId = toWatchlistId({
                    player_id: b.player_id,
                    player_name: b.player_name,
                    team: b.teamLabel,
                  });
                  const aw = Boolean(aId && watchIdSet.has(String(aId)));
                  const bw = Boolean(bId && watchIdSet.has(String(bId)));
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
                    to={
                      sport === "mlb"
                        ? `/props?mode=board&team=${playerQuery(team)}`
                        : `/nhl/predictions?mode=board&team=${playerQuery(team)}`
                    }
                    className="text-xs text-slate-500 hover:underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    title="Open in a new tab"
                  >
                    {sport === "mlb" ? "Open Team Props" : "Open Team Predictions"}
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
                      const freshness = freshnessLabel(p.last_prop_date);
                      const rowKey = playerSuggestionKey(p);
                      return (
                        <li
                          key={rowKey}
                          ref={(el) => {
                            if (el) playerRefs.current.set(rowKey, el);
                            else playerRefs.current.delete(rowKey);
                          }}
                          tabIndex={-1}
                          className={`flex items-center justify-between gap-3 pp-chip px-2 py-1 ${
                            isWatched ? "bg-emerald-50 border border-emerald-200" : ""
                          } focus:outline-none focus:ring-2 focus:ring-slate-300`}
                        >
                          <div className="min-w-0">
                            <Link
                              to={sport === "nhl" ? `/nhl/players/${p.player_id}` : `/mlb/players/${p.player_id}`}
                              state={{
                                sport,
                                player_name: getPlayerName(p) || null,
                                team: p.teamLabel || null,
                              }}
                              className="text-slate-700 hover:underline"
                            >
                              {getPlayerName(p)}
                            </Link>
                            <div className="mt-0.5">
                              <Link
                                to={
                                  sport === "mlb"
                                    ? `/props?mode=research&player=${playerQuery(
                                        getPlayerName(p)
                                      )}&team=${playerQuery(p.teamLabel || "")}`
                                    : `/nhl/predictions?mode=board&player=${playerQuery(
                                        getPlayerName(p)
                                      )}`
                                }
                                className="text-xs text-slate-500 hover:underline"
                                target="_blank"
                                rel="noopener noreferrer"
                                title="Open in a new tab"
                              >
                                {sport === "mlb" ? "Open Props" : "Open Predictions"}
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
                            {freshness ? (
                              <span className="text-xs text-slate-500">{freshness}</span>
                            ) : null}
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
