import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "react-router-dom";

import SogEvalCard from "../../components/SogEvalCard.jsx";
import { PrefetchLink } from "../../components/navigation/PrefetchLink.jsx";
import PredictionCalendar from "../../components/predictions/calendar/PredictionCalendar.jsx";
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
import { isISODateString, todayET } from "../../shared/timeUtils.js";
import {
  normalizeWatchlistRows,
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_NHL,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../../shared/watchlistStorage.js";

function num(x) {
  if (x == null) return null;
  if (typeof x === "string" && x.trim() === "") return null;
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function fmtProb(x) {
  const v = num(x);
  if (v == null) return "";
  return `${Math.round(v * 1000) / 10}%`;
}

function fmtEdgePoints(x) {
  const v = num(x);
  if (v == null) return "-";
  const points = v * 100;
  return `${points >= 0 ? "+" : ""}${points.toFixed(1)} pts`;
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

function buildWideRowsFromMarketCsv(rowsCsv, { probabilityColumn = "p_over", nameColumn = "full_name" } = {}) {
  const grouped = new Map();
  for (const row of rowsCsv || []) {
    const playerId = row.player_id;
    const gameId = row.game_id;
    const line = num(row.line);
    const probability = num(row[probabilityColumn]);
    if (playerId == null || gameId == null || line == null || probability == null) continue;

    const key = `${String(playerId)}|${String(gameId)}`;
    const lineKey = `p_over_${String(line).replace(/\./g, "_")}`;
    let wide = grouped.get(key);
    if (!wide) {
      wide = {
        player_id: num(playerId) ?? playerId,
        player_name: row[nameColumn] || row.player_name || String(playerId),
        game_id: num(gameId) ?? gameId,
        game_date: row.game_date || "",
        team_id: num(row.team_id),
        team_abbr: row.team_abbr || row.team || "",
      };
      grouped.set(key, wide);
    }
    wide[lineKey] = probability;
  }
  return Array.from(grouped.values());
}

function marketKey(playerId, gameId, line) {
  return `${String(playerId ?? "")}|${String(gameId ?? "")}|${String(line ?? "")}`;
}

const MAX_EDGE_FAVORITE_PRICE = -350;
const MAX_EDGE_DOG_PRICE = 500;

function marketPrice(value) {
  const parsed = num(value);
  return parsed == null ? null : Math.trunc(parsed);
}

function isPlayableMarket(market) {
  const marketProbability = num(market?.marketProbability);
  const priceOver = marketPrice(market?.priceOver);
  if (marketProbability == null || priceOver == null) return false;
  if (priceOver < 0 && priceOver < MAX_EDGE_FAVORITE_PRICE) return false;
  if (priceOver > 0 && priceOver > MAX_EDGE_DOG_PRICE) return false;
  return true;
}

function bestEdgeCandidate(rows, marketMap) {
  let best = null;
  for (const row of rows || []) {
    for (const line of extractOverLines(row)) {
      const market = marketMap.get(marketKey(row.player_id, row.game_id, line.line)) || null;
      if (!isPlayableMarket(market)) continue;
      const marketProbability = num(market?.marketProbability);
      const edge = line.p - marketProbability;
      if (!best || edge > best.edge) {
        best = { row, bestLine: line, market, edge };
      }
    }
  }
  return best;
}

function topPlayableEdgeRows(rows, marketMap, limit = 8) {
  const out = [];
  for (const row of rows || []) {
    let best = null;
    for (const line of extractOverLines(row)) {
      const market = marketMap.get(marketKey(row.player_id, row.game_id, line.line)) || null;
      if (!isPlayableMarket(market)) continue;
      const marketProbability = num(market?.marketProbability);
      if (marketProbability == null) continue;
      const edge = line.p - marketProbability;
      if (!best || edge > best.edge) {
        best = { row, bestLine: line, market, edge };
      }
    }
    if (best) out.push(best);
  }
  out.sort((a, b) => b.edge - a.edge || (b.bestLine?.p ?? 0) - (a.bestLine?.p ?? 0));
  return out.slice(0, limit);
}

export default function NHLPredictions() {
  const location = useLocation();
  const { user } = useAuth();
  const [slateDate, setSlateDate] = useState(todayET());
  const [mode, setMode] = useState(WORKSPACE_MODE_RESEARCH);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [loadedAt, setLoadedAt] = useState(null);
  const [marketLoadedAt, setMarketLoadedAt] = useState(null);
  const [marketMaps, setMarketMaps] = useState({
    sog: new Map(),
    saves: new Map(),
    points: new Map(),
  });

  const [sogRows, setSogRows] = useState([]);
  const [savesRows, setSavesRows] = useState([]);
  const [pointsRows, setPointsRows] = useState([]);

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
    const dateFromUrl = String(params.get("date") || "").trim();
    const seed = playerFromUrl || teamFromUrl;
    if (isWorkspaceMode(modeFromUrl)) {
      setMode(modeFromUrl);
    } else if (seed) {
      setMode(WORKSPACE_MODE_BOARD);
    }
    if (isISODateString(dateFromUrl)) {
      setSlateDate(dateFromUrl);
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

  function commitWatchlist(updater) {
    if (!user?.id) return;
    setWatchlist((prev) => {
      const nextRaw = typeof updater === "function" ? updater(prev) : updater;
      const next = normalizeWatchlistRows(nextRaw);
      writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, next);
      return next;
    });
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

    async function loadMarketContext() {
      try {
        const selectedDate = String(slateDate || "").trim();
        const isHistorical = Boolean(selectedDate && selectedDate !== todayET());
        const encodedDate = encodeURIComponent(selectedDate);

        const primaryPrefix = isHistorical
          ? `${getBaseURL()}/nhl/exports/odds_history/${encodedDate}`
          : `${getBaseURL()}/nhl/site/data`;
        const fallbackPrefix = `${getBaseURL()}/nhl/site/data`;

        async function fetchText(primaryPath, fallbackPath) {
          const primary = await fetch(primaryPath, { cache: "no-store" });
          if (primary.ok) return primary.text();
          if (fallbackPath) {
            const fallback = await fetch(fallbackPath, { cache: "no-store" });
            if (fallback.ok) return fallback.text();
          }
          return "";
        }

        const [sogText, savesText, pointsText] = await Promise.all([
          fetchText(
            `${primaryPrefix}/sog_with_market.csv`,
            isHistorical ? null : `${fallbackPrefix}/sog_with_market.csv`
          ),
          fetchText(
            `${primaryPrefix}/saves_with_market.csv`,
            isHistorical ? null : `${fallbackPrefix}/saves_with_market.csv`
          ),
          fetchText(
            `${primaryPrefix}/points_with_market.csv`,
            isHistorical ? null : `${fallbackPrefix}/points_with_market.csv`
          ),
        ]);

        if (cancelled) return;

        const effectiveDate = isHistorical ? selectedDate : null;

        const sogRowsCsv = parseCsvRows(sogText);
        const savesRowsCsv = parseCsvRows(savesText);
        const pointsRowsCsv = parseCsvRows(pointsText);
        const filteredSogRowsCsv = effectiveDate
          ? sogRowsCsv.filter((row) => String(row.game_date || "").trim() === effectiveDate)
          : sogRowsCsv;
        const filteredSavesRowsCsv = effectiveDate
          ? savesRowsCsv.filter((row) => String(row.game_date || "").trim() === effectiveDate)
          : savesRowsCsv;
        const filteredPointsRowsCsv = effectiveDate
          ? pointsRowsCsv.filter((row) => String(row.game_date || "").trim() === effectiveDate)
          : pointsRowsCsv;
        const matchedPointsRowsCsv = filteredPointsRowsCsv.filter((row) => {
          const marketProbability = num(row.p_over_mkt);
          const priceOver = marketPrice(row.price_over);
          return marketProbability != null && priceOver != null;
        });

        const pointsWideRows = buildWideRowsFromMarketCsv(matchedPointsRowsCsv, {
          probabilityColumn: "p_over",
          nameColumn: "full_name",
        });

        const sogMap = new Map();
        for (const row of filteredSogRowsCsv) {
          const key = marketKey(row.player_id, row.game_id, row.line);
          sogMap.set(key, {
            marketProbability: num(row.p_over_mkt),
            priceOver: row.price_over,
          });
        }

        const savesMap = new Map();
        for (const row of filteredSavesRowsCsv) {
          const key = marketKey(row.player_id, row.game_id, row.line);
          savesMap.set(key, {
            marketProbability: num(row.p_over_mkt),
            priceOver: row.price_over,
          });
        }

        const pointsMap = new Map();
        for (const row of matchedPointsRowsCsv) {
          const key = marketKey(row.player_id, row.game_id, row.line);
          pointsMap.set(key, {
            marketProbability: num(row.p_over_mkt),
            priceOver: row.price_over,
          });
        }

        setPointsRows(pointsWideRows);
        setMarketMaps({ sog: sogMap, saves: savesMap, points: pointsMap });
        setMarketLoadedAt(new Date().toISOString());
      } catch {
        if (cancelled) return;
        setPointsRows([]);
        setMarketMaps({ sog: new Map(), saves: new Map(), points: new Map() });
      }
    }

    loadMarketContext();
    return () => {
      cancelled = true;
    };
  }, [slateDate]);

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

  const teamAbbrByTeamId = useMemo(() => {
    const out = new Map();
    for (const row of [...(sogRows || []), ...(savesRows || [])]) {
      const key = String(row?.team_id ?? "").trim();
      const value = String(row?.team_abbr || row?.team || "").trim();
      if (!key || !value || out.has(key)) continue;
      out.set(key, value);
    }
    return out;
  }, [savesRows, sogRows]);

  const normalizedPointsRows = useMemo(() => {
    return (pointsRows || []).map((row) => {
      const key = String(row?.team_id ?? "").trim();
      return {
        ...row,
        team_abbr: row?.team_abbr || (key ? teamAbbrByTeamId.get(key) || "" : ""),
      };
    });
  }, [pointsRows, teamAbbrByTeamId]);

  const filteredPoints = useMemo(() => {
    const baseRows = watchlistOnly
      ? normalizedPointsRows.filter((r) => {
          const id = toWatchlistId({
            player_id: r.player_id,
            player_name: r.player_name,
            team: r.team_abbr || r.team || "",
          });
          return Boolean(id && watchIdSet.has(id));
        })
      : normalizedPointsRows;
    if (!query) return baseRows;
    return baseRows.filter((r) => {
      const haystack = [r.player_id, r.game_id, r.player_name, r.team_abbr]
        .map((v) => String(v ?? "").toLowerCase())
        .join(" ");
      return haystack.includes(query);
    });
  }, [normalizedPointsRows, query, watchIdSet, watchlistOnly]);

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

  const sortedPoints = useMemo(() => {
    const arr = [...(filteredPoints || [])];
    arr.sort((a, b) => {
      const aBest = bestLineFromRow(a)?.p ?? -1;
      const bBest = bestLineFromRow(b)?.p ?? -1;
      return bBest - aBest;
    });
    return arr;
  }, [filteredPoints]);

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
      ? "Review strongest SOG and points edges before scanning the full board."
      : "Search and rank shots-on-goal and saves lines for the active slate.";
  }, [mode]);

  const calendarSection = (
    <PredictionCalendar
      selectedDate={slateDate}
      setSelectedDate={setSlateDate}
      title="Slate Calendar"
      subtitle="Choose the NHL slate date before scanning the board to find past top SOG model edges."
    />
  );

  const topSogRows = useMemo(() => sortedSog.slice(0, 10), [sortedSog]);
  const topSavesRows = useMemo(() => sortedSaves.slice(0, 10), [sortedSaves]);
  const hasResearchRows = sortedSog.length > 0 || sortedPoints.length > 0;
  const hasBoardRows = sortedSog.length > 0 || sortedSaves.length > 0;

  const topPlayableSogRows = useMemo(
    () => topPlayableEdgeRows(sortedSog, marketMaps.sog, 10),
    [marketMaps.sog, sortedSog]
  );

  const topPlayablePointsRows = useMemo(
    () => topPlayableEdgeRows(sortedPoints, marketMaps.points, 10),
    [marketMaps.points, sortedPoints]
  );

  const topSogCandidate = useMemo(
    () => bestEdgeCandidate(sortedSog, marketMaps.sog),
    [marketMaps.sog, sortedSog]
  );
  const topSavesCandidate = useMemo(
    () => bestEdgeCandidate(sortedSaves, marketMaps.saves),
    [marketMaps.saves, sortedSaves]
  );

  const topSog = topSogCandidate?.row || topSogRows[0] || null;
  const topSogBest = topSogCandidate?.bestLine || (topSog ? bestLineFromRow(topSog) : null);
  const topSaves = topSavesCandidate?.row || topSavesRows[0] || null;
  const topSavesBest =
    topSavesCandidate?.bestLine || (topSaves ? bestLineFromRow(topSaves) : null);

  const topSogMarket = topSogCandidate?.market || null;
  const topSavesMarket = topSavesCandidate?.market || null;

  const topSogPrediction = useMemo(
    () =>
      adaptNhlBoardPrediction({
        propType: "sog",
        row: topSog,
        bestLine: topSogBest,
        market: topSogMarket,
        modelUpdatedAt: loadedAt || null,
        marketUpdatedAt: marketLoadedAt || null,
        modelSource: "NHL SOG Poisson baseline",
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

  const boardPrediction =
    (topSogPrediction?.marketProbability != null &&
    topSavesPrediction?.marketProbability != null
      ? (topSogCandidate?.edge ?? -Infinity) >= (topSavesCandidate?.edge ?? -Infinity)
        ? topSogPrediction
        : topSavesPrediction
      : topSogPrediction?.marketProbability != null
        ? topSogPrediction
        : topSavesPrediction?.marketProbability != null
          ? topSavesPrediction
          : topSogPrediction);

  const dataConfidence = useMemo(() => {
    const total = sortedSog.length + sortedPoints.length;
    if (total >= 120) return "High";
    if (total >= 40) return "Medium";
    return "Low";
  }, [sortedPoints.length, sortedSog.length]);

  const sparseData = useMemo(() => {
    return sortedSog.length + sortedPoints.length < 25;
  }, [sortedPoints.length, sortedSog.length]);

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
    commitWatchlist((prev) => {
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
      return next;
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
    commitWatchlist((prev) => {
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
      return next;
    });
    setSaveError("");
    setSaveNotice(exists ? "Player removed from NHL watchlist." : "Player added to NHL watchlist.");
  }

  function removeWatchById(id) {
    commitWatchlist((prev) => prev.filter((w) => String(w.id) !== String(id)));
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

  const topSogEdgeCards = useMemo(() => {
    return topPlayableSogRows.map((candidate, index) => {
      const prediction = adaptNhlBoardPrediction({
        propType: "sog",
        row: candidate.row,
        bestLine: candidate.bestLine,
        market: candidate.market,
        modelUpdatedAt: loadedAt || null,
        marketUpdatedAt: marketLoadedAt || null,
        modelSource: "NHL SOG Poisson baseline",
        marketSource: "OddsAPI market median",
      });
      const watchId = toWatchlistId({
        player_id: candidate.row?.player_id,
        player_name: candidate.row?.player_name,
        team: candidate.row?.team_abbr || candidate.row?.team || "",
      });
      const isWatched = Boolean(watchId && watchIdSet.has(String(watchId)));
      return {
        key: `sog-edge-${String(candidate.row?.game_id)}-${String(candidate.row?.player_id)}-${index}`,
        row: candidate.row,
        prediction,
        isWatched,
        lastPropDate: String(candidate.row?.last_prop_date || "").trim(),
        edge: candidate.edge,
        rank: index + 1,
      };
    });
  }, [loadedAt, marketLoadedAt, topPlayableSogRows, watchIdSet]);

  const topPointsEdgeCards = useMemo(() => {
    return topPlayablePointsRows.map((candidate, index) => {
      const prediction = adaptNhlBoardPrediction({
        propType: "points",
        row: candidate.row,
        bestLine: candidate.bestLine,
        market: candidate.market,
        modelUpdatedAt: loadedAt || null,
        marketUpdatedAt: marketLoadedAt || null,
        modelSource: "NHL points model",
        marketSource: "OddsAPI market median",
      });
      const watchId = toWatchlistId({
        player_id: candidate.row?.player_id,
        player_name: candidate.row?.player_name,
        team: candidate.row?.team_abbr || candidate.row?.team || "",
      });
      const isWatched = Boolean(watchId && watchIdSet.has(String(watchId)));
      return {
        key: `points-edge-${String(candidate.row?.game_id)}-${String(candidate.row?.player_id)}-${index}`,
        row: candidate.row,
        prediction,
        isWatched,
        lastPropDate: String(candidate.row?.last_prop_date || "").trim(),
        edge: candidate.edge,
        rank: index + 1,
      };
    });
  }, [loadedAt, marketLoadedAt, topPlayablePointsRows, watchIdSet]);

  const matchedSogLineCount = useMemo(() => marketMaps.sog.size, [marketMaps.sog]);
  const matchedPointsLineCount = useMemo(() => marketMaps.points.size, [marketMaps.points]);

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
          detail="Fetching shots-on-goal, saves, and points context for the current slate."
        />
      ) : error ? (
        <WorkspaceStatePanel kind="error" title="Could not load NHL predictions" detail={error} />
      ) : (mode === WORKSPACE_MODE_RESEARCH ? !hasResearchRows : !hasBoardRows) ? (
        <WorkspaceStatePanel
          kind="empty"
          title="No predictions available"
          detail="No rows returned for this slate date."
        />
      ) : mode === WORKSPACE_MODE_RESEARCH ? (
        <div className="space-y-6">
          {calendarSection}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <section className="pp-card p-4">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-slate-900">Top SOG Model Edges</h3>
                <div className="text-sm text-slate-500">Top {topSogEdgeCards.length}</div>
              </div>
              <div className="text-xs text-slate-500 mb-3">
                Ranked by model-minus-market edge (playable prices only).
              </div>
              <div className="text-xs text-slate-500 mb-3">
                Matched lines: {matchedSogLineCount}
              </div>
              <div className="space-y-2">
                {topSogEdgeCards.map((card) => (
                  <div key={card.key} className="pp-chip px-3 py-2">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="font-medium text-slate-900">
                          #{card.rank} {card.row?.player_name || card.row?.player_id}
                        </div>
                        <div className="text-xs text-slate-500">
                          {card.row?.team_abbr || ""} - game {card.row?.game_id}
                        </div>
                        <div className="text-sm text-slate-700 mt-1">
                          {formatNhlPredictionLine(card.prediction, "No SOG edge available")}
                        </div>
                      </div>
                      <div className="text-right text-xs min-w-[8rem]">
                        <div className="text-slate-600">Model: {fmtProb(card.prediction?.modelProbability) || "-"}</div>
                        <div className="text-slate-600">Market: {fmtProb(card.prediction?.marketProbability) || "-"}</div>
                        <div className={card.edge >= 0 ? "text-emerald-700 font-semibold" : "text-rose-700 font-semibold"}>
                          Edge: {fmtEdgePoints(card.edge)}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 mt-2">
                      <button
                        type="button"
                        className="pp-btn pp-btn-secondary pp-btn-sm"
                        onClick={() => toggleWatchByRow(card.row)}
                        title={card.isWatched ? "Remove player from watchlist" : "Add player to watchlist"}
                      >
                        {card.isWatched ? "Watching" : "+ Watch"}
                      </button>
                      <PrefetchLink
                        to={`/nhl/players/${encodeURIComponent(String(card.row?.player_id))}`}
                        state={{
                          sport: "nhl",
                          player_name: card.row?.player_name || null,
                          team: card.row?.team_abbr || card.row?.team || null,
                        }}
                        className="text-xs text-slate-500 underline"
                      >
                        Open Player
                      </PrefetchLink>
                      {card.lastPropDate ? (
                        <span className="text-xs text-slate-500">
                          {`last prop ${card.lastPropDate}`}
                        </span>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </section>

            <section className="pp-card p-4">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-slate-900">Top Points Model Edges</h3>
                <div className="text-sm text-slate-500">Top {topPointsEdgeCards.length}</div>
              </div>
              <div className="text-xs text-slate-500 mb-3">
                Ranked by model-minus-market edge (playable prices only).
              </div>
              <div className="text-xs text-slate-500 mb-3">
                Matched lines: {matchedPointsLineCount}
              </div>
              <div className="space-y-2">
                {topPointsEdgeCards.map((card) => (
                  <div key={card.key} className="pp-chip px-3 py-2">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="font-medium text-slate-900">
                          #{card.rank} {card.row?.player_name || card.row?.player_id}
                        </div>
                        <div className="text-xs text-slate-500">
                          {card.row?.team_abbr || ""} - game {card.row?.game_id}
                        </div>
                        <div className="text-sm text-slate-700 mt-1">
                          {formatNhlPredictionLine(card.prediction, "No points edge available")}
                        </div>
                      </div>
                      <div className="text-right text-xs min-w-[8rem]">
                        <div className="text-slate-600">Model: {fmtProb(card.prediction?.modelProbability) || "-"}</div>
                        <div className="text-slate-600">Market: {fmtProb(card.prediction?.marketProbability) || "-"}</div>
                        <div className={card.edge >= 0 ? "text-emerald-700 font-semibold" : "text-rose-700 font-semibold"}>
                          Edge: {fmtEdgePoints(card.edge)}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 mt-2">
                      <button
                        type="button"
                        className="pp-btn pp-btn-secondary pp-btn-sm"
                        onClick={() => toggleWatchByRow(card.row)}
                        title={card.isWatched ? "Remove player from watchlist" : "Add player to watchlist"}
                      >
                        {card.isWatched ? "Watching" : "+ Watch"}
                      </button>
                      <PrefetchLink
                        to={`/nhl/players/${encodeURIComponent(String(card.row?.player_id))}`}
                        state={{
                          sport: "nhl",
                          player_name: card.row?.player_name || null,
                          team: card.row?.team_abbr || card.row?.team || null,
                        }}
                        className="text-xs text-slate-500 underline"
                      >
                        Open Player
                      </PrefetchLink>
                      {card.lastPropDate ? (
                        <span className="text-xs text-slate-500">
                          {`last prop ${card.lastPropDate}`}
                        </span>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </section>
          </div>

          {sparseData ? (
            <WorkspaceStatePanel
              kind="sparse"
              title="Sparse data on this slate"
              detail="Model output is available, but row volume is low. Interpret rankings with caution."
            />
          ) : null}

          <section className="pp-card p-4">
            <h3 className="text-base font-semibold text-slate-900 mb-2">Model Quality Explained</h3>
            <p className="text-sm text-slate-700">
              This is the model&apos;s report card. If these numbers are stable, it means our projected
              shot lines are matching real game outcomes at a usable level over time.
            </p>
            <p className="text-sm text-slate-700 mt-2">
              Lower error means fewer bad misses. Higher ranking quality means the players we rate as
              stronger shot candidates are more often the ones who actually produce.
            </p>
          </section>

          <SogEvalCard />
        </div>
      ) : (
        <div className="space-y-6">
          {calendarSection}

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
            deletePath="/api/nhl/props/delete"
            propSource="nhl_user_added"
            title="My Saved NHL Props"
            exportPrefix="my_nhl_props"
          />
        </div>
      )}
    </PredictionWorkspace>
  );
}
