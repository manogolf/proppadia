import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "react-router-dom";

import SogEvalCard from "../../components/SogEvalCard.jsx";
import { PrefetchLink } from "../../components/navigation/PrefetchLink.jsx";
import TodayGamesNHL from "../../components/TodayGamesNHL.jsx";
import ModelVsMarketCard from "../../components/predictions/ModelVsMarketCard.jsx";
import MyPropsPanel from "../../components/predictions/MyPropsPanel.jsx";
import PredictionWorkspace from "../../components/predictions/PredictionWorkspace.jsx";
import WorkspaceStatePanel from "../../components/predictions/WorkspaceStatePanel.jsx";
import {
  NHL_WORKSPACE_MODES,
  WORKSPACE_MODE_BOARD,
  WORKSPACE_MODE_RESEARCH,
  isWorkspaceMode,
} from "../../components/predictions/workspaceModes.js";
import { useAuth } from "../../context/AuthContext.jsx";
import { getBaseURL } from "../../shared/getBaseURL.js";
import { normalizeHttpErrorMessage } from "../../shared/httpErrorMessage.js";
import { buildMarketContext } from "../../shared/marketContext.js";
import {
  adaptNhlBoardPrediction,
  formatNhlPredictionLine,
} from "../../shared/predictionAdapters/nhlAdapter.js";
import { todayET } from "../../shared/timeUtils.js";
import {
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_NHL,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../../shared/watchlistStorage.js";

function num(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function fmtProb(x) {
  const v = num(x);
  if (v == null) return "";
  return `${Math.round(v * 1000) / 10}%`;
}

function extractOverLines(row) {
  const out = [];
  for (const [key, value] of Object.entries(row || {})) {
    if (!key.startsWith("p_over_")) continue;
    const p = num(value);
    if (p == null) continue;
    const line = Number(key.replace("p_over_", "").replace(/_/g, "."));
    if (!Number.isFinite(line)) continue;
    out.push({ line, p });
  }
  out.sort((a, b) => a.line - b.line);
  return out;
}

function bestLineFromRow(row) {
  const lines = extractOverLines(row);
  if (lines.length === 0) return null;
  return [...lines].sort((a, b) => b.p - a.p)[0];
}

function probForLine(row, line) {
  const found = extractOverLines(row).find((x) => x.line === line);
  return found?.p ?? null;
}

function parseCsvRows(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const row = {};
    headers.forEach((h, i) => {
      row[h] = values[i] ?? "";
    });
    return row;
  });
}

function marketKey(playerId, gameId, line) {
  return `${String(playerId ?? "")}|${String(gameId ?? "")}|${String(line ?? "")}`;
}

export default function NHLPredictions() {
  const location = useLocation();
  const { user } = useAuth();
  const slateDate = useMemo(() => todayET(), []);
  const [mode, setMode] = useState(WORKSPACE_MODE_RESEARCH);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [loadedAt, setLoadedAt] = useState(null);
  const [marketLoadedAt, setMarketLoadedAt] = useState(null);
  const [marketMaps, setMarketMaps] = useState({ sog: new Map(), saves: new Map() });
  const [games, setGames] = useState([]);
  const [gamesLoading, setGamesLoading] = useState(true);
  const [gamesError, setGamesError] = useState("");

  const [sogRows, setSogRows] = useState([]);
  const [savesRows, setSavesRows] = useState([]);

  const [q, setQ] = useState("");
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [sogSort, setSogSort] = useState("best");
  const [savesSort, setSavesSort] = useState("best");
  const [saveError, setSaveError] = useState("");
  const [saveNotice, setSaveNotice] = useState("");
  const [savingKeys, setSavingKeys] = useState({});
  const [watchlist, setWatchlist] = useState([]);
  const queryAutoSelectRef = useRef("");
  const rowRefs = useRef(new Map());

  useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const modeFromUrl = String(params.get("mode") || "").trim().toLowerCase();
    const playerFromUrl = String(params.get("player") || "").trim();
    const teamFromUrl = String(params.get("team") || "").trim();
    const seed = playerFromUrl || teamFromUrl;
    if (isWorkspaceMode(modeFromUrl)) {
      setMode(modeFromUrl);
    } else if (seed) {
      setMode(WORKSPACE_MODE_BOARD);
    }
    if (!seed) return;
    setQ(seed);
    queryAutoSelectRef.current = seed.toLowerCase();
  }, [location.search]);

  function refreshWatchlistRows() {
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    setWatchlist(readWatchlistScope(user.id, WATCHLIST_SCOPE_NHL));
  }

  useEffect(() => {
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    refreshWatchlistRows();
  }, [user?.id]);

  useEffect(() => {
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshWatchlistRows();
      }
    }
    function onWatchlistUpdated() {
      refreshWatchlistRows();
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
    writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, watchlist);
  }, [user?.id, watchlist]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        setError("");
        setLoading(true);

        const [sogRes, savesRes] = await Promise.all([
          fetch(`${getBaseURL()}/api/nhl/sog?date=${encodeURIComponent(slateDate)}&limit=200&offset=0`),
          fetch(`${getBaseURL()}/api/nhl/saves?date=${encodeURIComponent(slateDate)}&limit=200&offset=0`),
        ]);

        const sogJson = await sogRes.json();
        const savesJson = await savesRes.json();

        if (!sogRes.ok || sogJson?.ok === false) {
          throw new Error(sogJson?.error || `SOG endpoint failed (${sogRes.status})`);
        }
        if (!savesRes.ok || savesJson?.ok === false) {
          throw new Error(savesJson?.error || `Saves endpoint failed (${savesRes.status})`);
        }

        if (cancelled) return;

        setSogRows(Array.isArray(sogJson) ? sogJson : sogJson?.rows || []);
        setSavesRows(Array.isArray(savesJson) ? savesJson : savesJson?.rows || []);
        setLoadedAt(new Date().toISOString());
        setLoading(false);
      } catch (e) {
        if (cancelled) return;
        setError(normalizeHttpErrorMessage(e, "Failed to load NHL predictions."));
        setLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [slateDate]);

  useEffect(() => {
    let cancelled = false;
    async function loadGames() {
      try {
        setGamesLoading(true);
        setGamesError("");
        const url = `${getBaseURL()}/api/nhl/games/today?date=${encodeURIComponent(slateDate)}`;
        const res = await fetch(url);
        const j = await res.json();
        if (!res.ok || j?.ok === false) {
          throw new Error(j?.error || `NHL games endpoint failed (${res.status})`);
        }
        if (!cancelled) {
          setGames(Array.isArray(j?.rows) ? j.rows : []);
        }
      } catch (e) {
        if (!cancelled) setGamesError(normalizeHttpErrorMessage(e, "Failed to load NHL games."));
      } finally {
        if (!cancelled) setGamesLoading(false);
      }
    }
    loadGames();
    return () => {
      cancelled = true;
    };
  }, [slateDate]);

  useEffect(() => {
    let cancelled = false;

    async function loadMarketContext() {
      try {
        const [sogRes, savesRes] = await Promise.all([
          fetch(`${getBaseURL()}/nhl/site/data/sog_with_market.csv`, { cache: "no-store" }),
          fetch(`${getBaseURL()}/nhl/site/data/saves_with_market.csv`, { cache: "no-store" }),
        ]);

        const [sogText, savesText] = await Promise.all([
          sogRes.ok ? sogRes.text() : Promise.resolve(""),
          savesRes.ok ? savesRes.text() : Promise.resolve(""),
        ]);

        if (cancelled) return;

        const sogRowsCsv = parseCsvRows(sogText);
        const savesRowsCsv = parseCsvRows(savesText);

        const sogMap = new Map();
        for (const row of sogRowsCsv) {
          const key = marketKey(row.player_id, row.game_id, row.line);
          sogMap.set(key, {
            marketProbability: num(row.p_over_mkt),
            priceOver: row.price_over,
          });
        }

        const savesMap = new Map();
        for (const row of savesRowsCsv) {
          const key = marketKey(row.player_id, row.game_id, row.line);
          savesMap.set(key, {
            marketProbability: num(row.p_over_mkt),
            priceOver: row.price_over,
          });
        }

        setMarketMaps({ sog: sogMap, saves: savesMap });
        setMarketLoadedAt(new Date().toISOString());
      } catch {
        if (cancelled) return;
        setMarketMaps({ sog: new Map(), saves: new Map() });
      }
    }

    loadMarketContext();
    return () => {
      cancelled = true;
    };
  }, []);

  const query = useMemo(() => q.trim().toLowerCase(), [q]);
  const watchIdSet = useMemo(
    () => new Set(watchlist.map((w) => String(w.id))),
    [watchlist]
  );

  const filteredSog = useMemo(() => {
    const baseRows = watchlistOnly
      ? (sogRows || []).filter((r) => {
          const id = toWatchlistId({
            player_id: r.player_id,
            player_name: r.player_name,
            team: r.team_abbr || r.team || "",
          });
          return Boolean(id && watchIdSet.has(id));
        })
      : sogRows || [];
    if (!query) return baseRows;
    return baseRows.filter((r) => {
      const haystack = [
        r.player_id,
        r.game_id,
        r.player_name,
        r.team_abbr,
      ]
        .map((v) => String(v ?? "").toLowerCase())
        .join(" ");
      return haystack.includes(query);
    });
  }, [sogRows, query, watchIdSet, watchlistOnly]);

  const filteredSaves = useMemo(() => {
    const baseRows = watchlistOnly
      ? (savesRows || []).filter((r) => {
          const id = toWatchlistId({
            player_id: r.player_id,
            player_name: r.player_name,
            team: r.team_abbr || r.team || "",
          });
          return Boolean(id && watchIdSet.has(id));
        })
      : savesRows || [];
    if (!query) return baseRows;
    return baseRows.filter((r) => {
      const haystack = [
        r.player_id,
        r.game_id,
        r.player_name,
        r.team_abbr,
      ]
        .map((v) => String(v ?? "").toLowerCase())
        .join(" ");
      return haystack.includes(query);
    });
  }, [savesRows, query, watchIdSet, watchlistOnly]);

  const sogLines = useMemo(() => {
    const set = new Set();
    for (const row of filteredSog) {
      for (const x of extractOverLines(row)) set.add(x.line);
    }
    return [...set].sort((a, b) => a - b);
  }, [filteredSog]);

  const savesLines = useMemo(() => {
    const set = new Set();
    for (const row of filteredSaves) {
      for (const x of extractOverLines(row)) set.add(x.line);
    }
    return [...set].sort((a, b) => a - b);
  }, [filteredSaves]);

  const sortedSog = useMemo(() => {
    const arr = [...(filteredSog || [])];
    const getKey = (r) => {
      if (sogSort !== "best") return probForLine(r, Number(sogSort)) ?? -1;
      const best = bestLineFromRow(r);
      return best?.p ?? -1;
    };
    arr.sort((a, b) => getKey(b) - getKey(a));
    return arr;
  }, [filteredSog, sogSort]);

  const sortedSaves = useMemo(() => {
    const arr = [...(filteredSaves || [])];
    const getKey = (r) => {
      if (savesSort !== "best") return probForLine(r, Number(savesSort)) ?? -1;
      const best = bestLineFromRow(r);
      return best?.p ?? -1;
    };
    arr.sort((a, b) => getKey(b) - getKey(a));
    return arr;
  }, [filteredSaves, savesSort]);

  useEffect(() => {
    const qLower = queryAutoSelectRef.current;
    if (!qLower) return;
    const findMatch = (rows, propType) => {
      for (const row of rows) {
        const name = String(row?.player_name || "").toLowerCase();
        const pid = String(row?.player_id || "").toLowerCase();
        if (name.includes(qLower) || pid === qLower) {
          return `${propType}:${String(row.game_id)}:${String(row.player_id)}`;
        }
      }
      return "";
    };
    const key = findMatch(sortedSog, "sog") || findMatch(sortedSaves, "saves");
    if (!key) return;
    const rowEl = rowRefs.current.get(key);
    if (rowEl && typeof rowEl.scrollIntoView === "function") {
      rowEl.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
    queryAutoSelectRef.current = "";
  }, [sortedSaves, sortedSog]);

  const subtitle = useMemo(() => {
    return mode === WORKSPACE_MODE_RESEARCH
      ? "Review strongest model probabilities before scanning the full board."
      : "Search and rank shots-on-goal and saves lines for the active slate.";
  }, [mode]);

  const slateSection = useMemo(() => {
    if (gamesLoading) {
      return (
        <WorkspaceStatePanel
          kind="loading"
          title="Loading NHL slate"
          detail={`Checking schedule context for ${slateDate}.`}
          centered
        />
      );
    }
    if (gamesError) {
      return (
        <WorkspaceStatePanel
          kind="error"
          title="Could not load NHL slate"
          detail={gamesError}
          centered
        />
      );
    }
    return <TodayGamesNHL games={games} />;
  }, [games, gamesError, gamesLoading, slateDate]);

  const topSogRows = useMemo(() => sortedSog.slice(0, 8), [sortedSog]);
  const topSavesRows = useMemo(() => sortedSaves.slice(0, 8), [sortedSaves]);

  const topSog = topSogRows[0] || null;
  const topSogBest = topSog ? bestLineFromRow(topSog) : null;
  const topSaves = topSavesRows[0] || null;
  const topSavesBest = topSaves ? bestLineFromRow(topSaves) : null;

  const topSogMarket = useMemo(() => {
    if (!topSog || !topSogBest) return null;
    return marketMaps.sog.get(marketKey(topSog.player_id, topSog.game_id, topSogBest.line)) || null;
  }, [marketMaps.sog, topSog, topSogBest]);

  const topSavesMarket = useMemo(() => {
    if (!topSaves || !topSavesBest) return null;
    return marketMaps.saves.get(marketKey(topSaves.player_id, topSaves.game_id, topSavesBest.line)) || null;
  }, [marketMaps.saves, topSaves, topSavesBest]);

  const topSogPrediction = useMemo(
    () =>
      adaptNhlBoardPrediction({
        propType: "sog",
        row: topSog,
        bestLine: topSogBest,
        market: topSogMarket,
        modelUpdatedAt: loadedAt || null,
        marketUpdatedAt: marketLoadedAt || null,
        modelSource: "NHL SOG model",
        marketSource: "OddsAPI market median",
      }),
    [loadedAt, marketLoadedAt, topSog, topSogBest, topSogMarket]
  );

  const topSavesPrediction = useMemo(
    () =>
      adaptNhlBoardPrediction({
        propType: "saves",
        row: topSaves,
        bestLine: topSavesBest,
        market: topSavesMarket,
        modelUpdatedAt: loadedAt || null,
        marketUpdatedAt: marketLoadedAt || null,
        modelSource: "NHL saves model",
        marketSource: "OddsAPI market median",
      }),
    [loadedAt, marketLoadedAt, topSaves, topSavesBest, topSavesMarket]
  );

  const boardPrediction = topSogPrediction;

  const dataConfidence = useMemo(() => {
    const total = sortedSog.length + sortedSaves.length;
    if (total >= 120) return "High";
    if (total >= 40) return "Medium";
    return "Low";
  }, [sortedSog.length, sortedSaves.length]);

  const sparseData = useMemo(() => {
    return sortedSog.length + sortedSaves.length < 25;
  }, [sortedSog.length, sortedSaves.length]);

  const boardSummary = useMemo(() => {
    const sogTop = sortedSog.slice(0, 25);
    const savesTop = sortedSaves.slice(0, 25);
    const topPlayerCounts = new Map();
    const topPropCounts = new Map();
    for (const row of sogTop) {
      const name = String(row.player_name || row.player_id || "Unknown");
      topPlayerCounts.set(name, (topPlayerCounts.get(name) || 0) + 1);
      topPropCounts.set("SOG", (topPropCounts.get("SOG") || 0) + 1);
    }
    for (const row of savesTop) {
      const name = String(row.player_name || row.player_id || "Unknown");
      topPlayerCounts.set(name, (topPlayerCounts.get(name) || 0) + 1);
      topPropCounts.set("Saves", (topPropCounts.get("Saves") || 0) + 1);
    }
    const topPlayers = Array.from(topPlayerCounts.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 5);
    return {
      totalRows: sortedSog.length + sortedSaves.length,
      sogRows: sortedSog.length,
      savesRows: sortedSaves.length,
      topPlayers,
      topTags: Array.from(topPropCounts.entries()).sort((a, b) => b[1] - a[1]),
    };
  }, [sortedSaves, sortedSog]);
  const watchlistCoverage = useMemo(() => {
    const inViewIds = new Set();
    for (const row of [...sortedSog, ...sortedSaves]) {
      const id = toWatchlistId({
        player_id: row.player_id,
        player_name: row.player_name,
        team: row.team_abbr || row.team || "",
      });
      if (id && watchIdSet.has(String(id))) inViewIds.add(String(id));
    }
    return { inView: inViewIds.size, total: watchlist.length };
  }, [sortedSog, sortedSaves, watchIdSet, watchlist.length]);

  const sortedWatchlist = useMemo(() => {
    return [...watchlist].sort(
      (a, b) =>
        new Date(b?.added_at || 0).getTime() - new Date(a?.added_at || 0).getTime()
    );
  }, [watchlist]);

  function toggleTopPlayerWatch(playerName) {
    if (!user?.id) {
      setSaveError("Sign in required to manage NHL watchlist.");
      return;
    }
    const targetName = String(playerName || "").trim();
    if (!targetName) return;
    const match =
      sortedSog.find((row) => String(row?.player_name || row?.player_id || "").trim() === targetName) ||
      sortedSaves.find((row) => String(row?.player_name || row?.player_id || "").trim() === targetName);
    if (!match) {
      setSaveError(`Could not find ${targetName} in current board rows.`);
      return;
    }
    const id = toWatchlistId({
      player_id: match.player_id,
      player_name: match.player_name,
      team: match.team_abbr || match.team || "",
    });
    if (!id) return;
    const exists = watchIdSet.has(String(id));
    setWatchlist((prev) => {
      if (exists) return prev.filter((w) => String(w.id) !== String(id));
      const next = [
        {
          id: String(id),
          player_id: match.player_id ?? null,
          player_name: match.player_name || null,
          team: match.team_abbr || match.team || null,
          added_at: new Date().toISOString(),
        },
        ...prev,
      ];
      return next.slice(0, 100);
    });
    setSaveError("");
    setSaveNotice(exists ? "Player removed from NHL watchlist." : "Player added to NHL watchlist.");
  }

  function toggleWatchByRow(row) {
    if (!user?.id) {
      setSaveError("Sign in required to manage NHL watchlist.");
      return;
    }
    if (!row) return;
    const id = toWatchlistId({
      player_id: row.player_id,
      player_name: row.player_name,
      team: row.team_abbr || row.team || "",
    });
    if (!id) return;
    const exists = watchIdSet.has(String(id));
    setWatchlist((prev) => {
      if (exists) return prev.filter((w) => String(w.id) !== String(id));
      const next = [
        {
          id: String(id),
          player_id: row.player_id ?? null,
          player_name: row.player_name || null,
          team: row.team_abbr || row.team || null,
          added_at: new Date().toISOString(),
        },
        ...prev,
      ];
      return next.slice(0, 100);
    });
    setSaveError("");
    setSaveNotice(exists ? "Player removed from NHL watchlist." : "Player added to NHL watchlist.");
  }

  function removeWatchById(id) {
    setWatchlist((prev) => prev.filter((w) => String(w.id) !== String(id)));
    setSaveError("");
    setSaveNotice("Player removed from NHL watchlist.");
  }

  const activeFilterLabel = useMemo(() => {
    const parts = [];
    if (query) parts.push(`Search: "${query}"`);
    if (watchlistOnly) parts.push("Watchlist only");
    if (sogSort !== "best") parts.push(`SOG sort: over ${sogSort}`);
    if (savesSort !== "best") parts.push(`Saves sort: over ${savesSort}`);
    return parts.length ? parts.join(" • ") : "No active board filters";
  }, [query, savesSort, sogSort, watchlistOnly]);

  const sogMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: topSogPrediction?.marketProbability ?? null,
        marketSource: topSogPrediction?.marketSource || null,
        marketUpdatedAt: topSogPrediction?.marketUpdatedAt || null,
        modelUpdatedAt: topSogPrediction?.modelUpdatedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: topSogPrediction?.modelSource || "NHL SOG model",
      }),
    [topSogPrediction]
  );

  const savesMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: topSavesPrediction?.marketProbability ?? null,
        marketSource: topSavesPrediction?.marketSource || null,
        marketUpdatedAt: topSavesPrediction?.marketUpdatedAt || null,
        modelUpdatedAt: topSavesPrediction?.modelUpdatedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: topSavesPrediction?.modelSource || "NHL saves model",
      }),
    [topSavesPrediction]
  );

  const boardMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: boardPrediction?.marketProbability ?? null,
        marketSource: boardPrediction?.marketSource || null,
        marketUpdatedAt: boardPrediction?.marketUpdatedAt || null,
        modelUpdatedAt: boardPrediction?.modelUpdatedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: boardPrediction?.modelSource || "NHL board",
      }),
    [boardPrediction]
  );

  const topSogWatchId = topSog
    ? toWatchlistId({
        player_id: topSog.player_id,
        player_name: topSog.player_name,
        team: topSog.team_abbr || topSog.team || "",
      })
    : "";
  const topSavesWatchId = topSaves
    ? toWatchlistId({
        player_id: topSaves.player_id,
        player_name: topSaves.player_name,
        team: topSaves.team_abbr || topSaves.team || "",
      })
    : "";
  const isTopSogWatched = Boolean(topSogWatchId && watchIdSet.has(String(topSogWatchId)));
  const isTopSavesWatched = Boolean(topSavesWatchId && watchIdSet.has(String(topSavesWatchId)));
  const topSogLastPropDate = String(topSog?.last_prop_date || "").trim();
  const topSavesLastPropDate = String(topSaves?.last_prop_date || "").trim();

  const boardControls = (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
      <div>
        <div className="text-xs text-slate-500 mb-1">Search</div>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Filter by player, team, player_id, or game_id..."
          className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
        />
      </div>
      <div>
        <label className="inline-flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={watchlistOnly}
            onChange={(e) => setWatchlistOnly(e.target.checked)}
          />
          Watchlist only
        </label>
      </div>

      <div>
        <div className="text-xs text-slate-500 mb-1">Sort SOG by</div>
        <select
          value={sogSort}
          onChange={(e) => setSogSort(e.target.value)}
          className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
        >
          <option value="best">Best line</option>
          {sogLines.map((line) => (
            <option key={`sog-sort-${line}`} value={String(line)}>
              {`P(over ${line})`}
            </option>
          ))}
        </select>
      </div>

      <div>
        <div className="text-xs text-slate-500 mb-1">Sort Saves by</div>
        <select
          value={savesSort}
          onChange={(e) => setSavesSort(e.target.value)}
          className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
        >
          <option value="best">Best line</option>
          {savesLines.map((line) => (
            <option key={`saves-sort-${line}`} value={String(line)}>
              {`P(over ${line})`}
            </option>
          ))}
        </select>
      </div>
      <div className="md:col-span-4">
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={() => {
            setQ("");
            setWatchlistOnly(false);
            setSogSort("best");
            setSavesSort("best");
          }}
        >
          Reset Board Filters
        </button>
      </div>
    </div>
  );

  async function saveBestLine(row, propTypeKey) {
    setSaveError("");
    setSaveNotice("");
    if (!user?.id) {
      setSaveError("Sign in required to save NHL props.");
      return;
    }
    const best = bestLineFromRow(row);
    if (!best) {
      setSaveError("No available line to save for this row.");
      return;
    }
    const saveKey = `${propTypeKey}:${row.game_id}:${row.player_id}:${best.line}`;
    setSavingKeys((prev) => ({ ...prev, [saveKey]: true }));
    try {
      const payload = {
        player_id: Number(row.player_id),
        player_name: row.player_name || null,
        team: row.team_abbr || null,
        team_id: row.team_id != null ? Number(row.team_id) : null,
        game_id: Number(row.game_id),
        game_date: row.game_date || slateDate,
        prop_type: propTypeKey,
        prop_value: Number(best.line),
        over_under: "over",
        probability: Number(best.p),
        prop_source: "nhl_user_added",
        user_id: String(user.id),
      };
      const res = await fetch(`${getBaseURL()}/api/nhl/props/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok || body?.ok === false) {
        throw new Error(body?.detail || body?.error || `Save failed (${res.status})`);
      }
      if (body?.duplicate) {
        setSaveNotice(`Already saved: ${row.player_name || row.player_id} over ${best.line}`);
      } else {
        setSaveNotice(`Saved: ${row.player_name || row.player_id} over ${best.line}`);
      }
    } catch (e) {
      setSaveError(e?.message || "Failed to save NHL prop.");
    } finally {
      setSavingKeys((prev) => {
        const next = { ...prev };
        delete next[saveKey];
        return next;
      });
    }
  }

  return (
    <PredictionWorkspace
      sportLabel="NHL"
      title="Prediction Workspace"
      subtitle={subtitle}
      dateLabel={`Slate (ET): ${slateDate}`}
      modes={NHL_WORKSPACE_MODES}
      activeMode={mode}
      onModeChange={setMode}
      controls={mode === WORKSPACE_MODE_BOARD ? boardControls : null}
    >
      {loading ? (
        <WorkspaceStatePanel
          kind="loading"
          title="Loading NHL predictions"
          detail="Fetching shots-on-goal and saves models for the current slate."
        />
      ) : error ? (
        <WorkspaceStatePanel kind="error" title="Could not load NHL predictions" detail={error} />
      ) : sortedSog.length === 0 && sortedSaves.length === 0 ? (
        <WorkspaceStatePanel
          kind="empty"
          title="No predictions available"
          detail="No rows returned for this slate date."
        />
      ) : mode === WORKSPACE_MODE_RESEARCH ? (
        <div className="space-y-6">
          {slateSection}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <ModelVsMarketCard
              title="Top SOG Model Edge"
              lineLabel={formatNhlPredictionLine(topSogPrediction, "No SOG edge available")}
              modelProbability={topSogPrediction?.modelProbability ?? null}
              marketProbability={topSogPrediction?.marketProbability ?? null}
              sourceLabel={sogMarketContext.sourceLabel}
              sourceKind={sogMarketContext.sourceKind}
              updatedLabel={sogMarketContext.updatedLabel}
              confidenceLabel={dataConfidence}
              badges={[{ label: isTopSogWatched ? "Watched" : "Not watched", tone: isTopSogWatched ? "success" : "muted" }]}
              actions={
                topSog ? (
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      className="pp-btn pp-btn-secondary pp-btn-sm"
                      onClick={() => toggleWatchByRow(topSog)}
                      title={isTopSogWatched ? "Remove player from watchlist" : "Add player to watchlist"}
                    >
                      {isTopSogWatched ? "Watching" : "+ Watch"}
                    </button>
                    <PrefetchLink
                      to={`/player/${encodeURIComponent(String(topSog.player_id))}`}
                      className="text-xs text-slate-500 underline"
                    >
                      Open Player
                    </PrefetchLink>
                    <span className="text-xs text-slate-500">
                      {topSogLastPropDate ? `last prop ${topSogLastPropDate}` : "last prop unavailable"}
                    </span>
                  </div>
                ) : null
              }
            />
            <ModelVsMarketCard
              title="Top Saves Model Edge"
              lineLabel={formatNhlPredictionLine(topSavesPrediction, "No saves edge available")}
              modelProbability={topSavesPrediction?.modelProbability ?? null}
              marketProbability={topSavesPrediction?.marketProbability ?? null}
              sourceLabel={savesMarketContext.sourceLabel}
              sourceKind={savesMarketContext.sourceKind}
              updatedLabel={savesMarketContext.updatedLabel}
              confidenceLabel={dataConfidence}
              badges={[{ label: isTopSavesWatched ? "Watched" : "Not watched", tone: isTopSavesWatched ? "success" : "muted" }]}
              actions={
                topSaves ? (
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      className="pp-btn pp-btn-secondary pp-btn-sm"
                      onClick={() => toggleWatchByRow(topSaves)}
                      title={isTopSavesWatched ? "Remove player from watchlist" : "Add player to watchlist"}
                    >
                      {isTopSavesWatched ? "Watching" : "+ Watch"}
                    </button>
                    <PrefetchLink
                      to={`/player/${encodeURIComponent(String(topSaves.player_id))}`}
                      className="text-xs text-slate-500 underline"
                    >
                      Open Player
                    </PrefetchLink>
                    <span className="text-xs text-slate-500">
                      {topSavesLastPropDate ? `last prop ${topSavesLastPropDate}` : "last prop unavailable"}
                    </span>
                  </div>
                ) : null
              }
            />
          </div>

          {sparseData ? (
            <WorkspaceStatePanel
              kind="sparse"
              title="Sparse data on this slate"
              detail="Model output is available, but row volume is low. Interpret rankings with caution."
            />
          ) : null}

          <SogEvalCard />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <section className="pp-card p-4">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-slate-900">Top SOG Edges</h3>
                <div className="text-sm text-slate-500">Top {topSogRows.length}</div>
              </div>
              <div className="space-y-2">
                {topSogRows.map((r) => {
                  const best = bestLineFromRow(r);
                  return (
                    <div
                      key={`top-sog-${r.game_id}-${r.player_id}`}
                      className="pp-chip px-3 py-2 flex items-center justify-between"
                    >
                      <div>
                        <div className="font-medium text-slate-900">{r.player_name || r.player_id}</div>
                        <div className="text-xs text-slate-500">{r.team_abbr || ""} - game {r.game_id}</div>
                      </div>
                      <div className="text-sm font-semibold text-slate-700">
                        {best ? `Over ${best.line}: ${fmtProb(best.p)}` : "-"}
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>

            <section className="pp-card p-4">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-slate-900">Top Saves Edges</h3>
                <div className="text-sm text-slate-500">Top {topSavesRows.length}</div>
              </div>
              <div className="space-y-2">
                {topSavesRows.map((r) => {
                  const best = bestLineFromRow(r);
                  return (
                    <div
                      key={`top-saves-${r.game_id}-${r.player_id}`}
                      className="pp-chip px-3 py-2 flex items-center justify-between"
                    >
                      <div>
                        <div className="font-medium text-slate-900">{r.player_name || r.player_id}</div>
                        <div className="text-xs text-slate-500">{r.team_abbr || ""} - game {r.game_id}</div>
                      </div>
                      <div className="text-sm font-semibold text-slate-700">
                        {best ? `Over ${best.line}: ${fmtProb(best.p)}` : "-"}
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>
          </div>
        </div>
      ) : (
        <div className="space-y-6">
          {slateSection}

          {saveError ? (
            <div className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {saveError}
            </div>
          ) : null}
          {!saveError && saveNotice ? (
            <div className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
              {saveNotice}
            </div>
          ) : null}
          <div className="pp-chip px-3 py-2 text-xs text-slate-600">{activeFilterLabel}</div>

          <div className="flex flex-wrap gap-2">
            <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs">
              Total <strong>{boardSummary.totalRows}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 text-blue-700 px-2 py-1 text-xs">
              SOG <strong>{boardSummary.sogRows}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-indigo-100 text-indigo-700 px-2 py-1 text-xs">
              Saves <strong>{boardSummary.savesRows}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
              Watchlist in view <strong>{watchlistCoverage.inView}/{watchlistCoverage.total}</strong>
            </span>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <section className="pp-card p-4">
              <h3 className="text-sm font-semibold text-slate-900 mb-2">Top Players in View</h3>
              {boardSummary.topPlayers.length === 0 ? (
                <div className="text-xs text-slate-500">No rows in current filter.</div>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {boardSummary.topPlayers.map(([name, count]) => (
                    (() => {
                      const match =
                        sortedSog.find((row) => String(row?.player_name || row?.player_id || "").trim() === String(name)) ||
                        sortedSaves.find((row) => String(row?.player_name || row?.player_id || "").trim() === String(name));
                      const watchId = match
                        ? toWatchlistId({
                            player_id: match.player_id,
                            player_name: match.player_name,
                            team: match.team_abbr || match.team || "",
                          })
                        : "";
                      const isWatched = Boolean(watchId && watchIdSet.has(String(watchId)));
                      return (
                        <span key={name} className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-700">
                          <button
                            type="button"
                            className="underline"
                            onClick={() => setQ(String(name || ""))}
                            title="Filter board by this player"
                          >
                            {name}
                          </button>
                          <strong>{count}</strong>
                          <button
                            type="button"
                            className="text-slate-500 underline"
                            onClick={() => toggleTopPlayerWatch(name)}
                            title={isWatched ? "Remove from watchlist" : "Add to watchlist"}
                          >
                            {isWatched ? "Unwatch" : "Watch"}
                          </button>
                          <PrefetchLink to="/watchlist" className="text-slate-500 underline" title="Open watchlist page">
                            WL
                          </PrefetchLink>
                        </span>
                      );
                    })()
                  ))}
                </div>
              )}
            </section>
            <section className="pp-card p-4">
              <h3 className="text-sm font-semibold text-slate-900 mb-2">Top Prop Groups</h3>
              <div className="flex flex-wrap gap-2">
                {boardSummary.topTags.map(([name, count]) => (
                  <span key={name} className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-700">
                    {name}
                    <strong>{count}</strong>
                  </span>
                ))}
              </div>
            </section>
          </div>

          <section className="pp-card p-4">
            <div className="flex items-center justify-between gap-2 mb-2">
              <h3 className="text-sm font-semibold text-slate-900">
                NHL Watchlist ({watchlist.length})
              </h3>
              <PrefetchLink to="/watchlist" className="text-xs text-slate-500 underline">
                Open Watchlist
              </PrefetchLink>
            </div>
            {sortedWatchlist.length === 0 ? (
              <div className="text-xs text-slate-500">No NHL players saved yet.</div>
            ) : (
              <div className="flex flex-wrap gap-2">
                {sortedWatchlist.map((w) => (
                  <span
                    key={String(w.id)}
                    className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-700"
                  >
                    <button
                      type="button"
                      className="underline"
                      onClick={() => {
                        setQ(String(w.player_name || w.player_id || ""));
                        setWatchlistOnly(true);
                      }}
                      title="Filter board by this player"
                    >
                      {w.player_name || w.player_id || "Unknown"}
                    </button>
                    {w.team ? <span className="text-slate-500">({w.team})</span> : null}
                    <button
                      type="button"
                      className="text-rose-700"
                      onClick={() => removeWatchById(w.id)}
                      title="Remove from watchlist"
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
            )}
          </section>

          <ModelVsMarketCard
            title="Board Snapshot"
            lineLabel={formatNhlPredictionLine(boardPrediction, "Top line snapshot")}
            modelProbability={boardPrediction?.modelProbability ?? null}
            marketProbability={boardPrediction?.marketProbability ?? null}
            sourceLabel={boardMarketContext.sourceLabel}
            sourceKind={boardMarketContext.sourceKind}
            updatedLabel={boardMarketContext.updatedLabel}
            confidenceLabel={dataConfidence}
          />

          {sparseData ? (
            <WorkspaceStatePanel
              kind="sparse"
              title="Sparse board for this date"
              detail="Some sort/filter views may look thin because the slate is small or off-season."
            />
          ) : null}

          <section className="pp-card p-4">
            <div className="flex items-baseline justify-between mb-3">
              <h3 className="text-lg font-semibold text-slate-900">Shots on Goal (SOG)</h3>
              <div className="text-sm text-slate-500">Rows: {sortedSog.length}</div>
            </div>

            {sortedSog.length === 0 ? (
              <div className="text-slate-500 text-sm">No SOG predictions found.</div>
            ) : (
              <div className="overflow-auto max-h-[30rem] rounded-md border border-slate-200">
                <table className="min-w-full text-sm">
                  <thead className="bg-white">
                    <tr className="text-left text-slate-600 border-b border-slate-200">
                      <th className="py-2 pr-3">game_id</th>
                      <th className="py-2 pr-3">team</th>
                      <th className="py-2 pr-3">player</th>
                      <th className="py-2 pr-3">player_id</th>
                      {sogLines.map((line) => (
                        <th key={`sog-col-${line}`} className="py-2 pr-3">{`P(over ${line})`}</th>
                      ))}
                      <th className="py-2 pr-3">action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSog.map((r) => (
                      <tr
                        key={`${r.game_id}-${r.player_id}`}
                        ref={(el) => {
                          const k = `sog:${String(r.game_id)}:${String(r.player_id)}`;
                          if (el) rowRefs.current.set(k, el);
                          else rowRefs.current.delete(k);
                        }}
                        className="border-b border-slate-100"
                      >
                        <td className="py-2 pr-3 font-mono">{r.game_id}</td>
                        <td className="py-2 pr-3">
                          <span className="font-semibold">{r.team_abbr || ""}</span>
                        </td>
                        <td className="py-2 pr-3">{r.player_name || ""}</td>
                        <td className="py-2 pr-3 font-mono">{r.player_id}</td>
                        {sogLines.map((line) => (
                          <td key={`sog-cell-${r.game_id}-${r.player_id}-${line}`} className="py-2 pr-3">
                            {fmtProb(probForLine(r, line))}
                          </td>
                        ))}
                        <td className="py-2 pr-3">
                          <button
                            type="button"
                            className="pp-btn pp-btn-secondary pp-btn-sm"
                            onClick={() => saveBestLine(r, "sog")}
                            disabled={Boolean(savingKeys[`sog:${r.game_id}:${r.player_id}:${bestLineFromRow(r)?.line}`])}
                          >
                            Save best
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className="pp-card p-4">
            <div className="flex items-baseline justify-between mb-3">
              <h3 className="text-lg font-semibold text-slate-900">Goalie Saves</h3>
              <div className="text-sm text-slate-500">Rows: {sortedSaves.length}</div>
            </div>

            {sortedSaves.length === 0 ? (
              <div className="text-slate-500 text-sm">No saves predictions found.</div>
            ) : (
              <div className="overflow-auto max-h-[30rem] rounded-md border border-slate-200">
                <table className="min-w-full text-sm">
                  <thead className="bg-white">
                    <tr className="text-left text-slate-600 border-b border-slate-200">
                      <th className="py-2 pr-3">game_id</th>
                      <th className="py-2 pr-3">team</th>
                      <th className="py-2 pr-3">player</th>
                      <th className="py-2 pr-3">player_id</th>
                      {savesLines.map((line) => (
                        <th key={`saves-col-${line}`} className="py-2 pr-3">{`P(over ${line})`}</th>
                      ))}
                      <th className="py-2 pr-3">action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSaves.map((r) => (
                      <tr
                        key={`${r.game_id}-${r.player_id}`}
                        ref={(el) => {
                          const k = `saves:${String(r.game_id)}:${String(r.player_id)}`;
                          if (el) rowRefs.current.set(k, el);
                          else rowRefs.current.delete(k);
                        }}
                        className="border-b border-slate-100"
                      >
                        <td className="py-2 pr-3 font-mono">{r.game_id}</td>
                        <td className="py-2 pr-3">
                          <span className="font-semibold">{r.team_abbr || ""}</span>
                        </td>
                        <td className="py-2 pr-3">{r.player_name || ""}</td>
                        <td className="py-2 pr-3 font-mono">{r.player_id}</td>
                        {savesLines.map((line) => (
                          <td key={`saves-cell-${r.game_id}-${r.player_id}-${line}`} className="py-2 pr-3">
                            {fmtProb(probForLine(r, line))}
                          </td>
                        ))}
                        <td className="py-2 pr-3">
                          <button
                            type="button"
                            className="pp-btn pp-btn-secondary pp-btn-sm"
                            onClick={() => saveBestLine(r, "saves")}
                            disabled={Boolean(savingKeys[`saves:${r.game_id}:${r.player_id}:${bestLineFromRow(r)?.line}`])}
                          >
                            Save best
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <MyPropsPanel
            apiPath="/api/nhl/props/history"
            propSource="nhl_user_added"
            title="My Saved NHL Props"
            exportPrefix="my_nhl_props"
          />
        </div>
      )}
    </PredictionWorkspace>
  );
}
