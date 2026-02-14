import { useEffect, useMemo, useState } from "react";

import SogEvalCard from "../../components/SogEvalCard.jsx";
import TodayGamesNHL from "../../components/TodayGamesNHL.jsx";
import ModelVsMarketCard from "../../components/predictions/ModelVsMarketCard.jsx";
import MyPropsPanel from "../../components/predictions/MyPropsPanel.jsx";
import PredictionWorkspace from "../../components/predictions/PredictionWorkspace.jsx";
import WorkspaceStatePanel from "../../components/predictions/WorkspaceStatePanel.jsx";
import { useAuth } from "../../context/AuthContext.jsx";
import { getBaseURL } from "../../shared/getBaseURL.js";
import { buildMarketContext } from "../../shared/marketContext.js";
import { todayET } from "../../shared/timeUtils.js";

const MODES = [
  {
    id: "research",
    label: "Player Research",
    hint: "Evaluate leaders and model confidence",
  },
  {
    id: "board",
    label: "Market Board",
    hint: "Search and sort the full slate",
  },
];

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
  const { user } = useAuth();
  const slateDate = useMemo(() => todayET(), []);
  const [mode, setMode] = useState("research");

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
  const [sogSort, setSogSort] = useState("best");
  const [savesSort, setSavesSort] = useState("best");
  const [saveError, setSaveError] = useState("");
  const [saveNotice, setSaveNotice] = useState("");
  const [savingKeys, setSavingKeys] = useState({});

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
        setError(e?.message || "Failed to load NHL predictions.");
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
        if (!cancelled) setGamesError(e?.message || "Failed to load NHL games.");
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

  const filteredSog = useMemo(() => {
    if (!query) return sogRows;
    return (sogRows || []).filter((r) => {
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
  }, [sogRows, query]);

  const filteredSaves = useMemo(() => {
    if (!query) return savesRows;
    return (savesRows || []).filter((r) => {
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
  }, [savesRows, query]);

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

  const subtitle = useMemo(() => {
    return mode === "research"
      ? "Review strongest model probabilities before scanning the full board."
      : "Search and rank shots-on-goal and saves lines for the active slate.";
  }, [mode]);

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

  const activeFilterLabel = useMemo(() => {
    const parts = [];
    if (query) parts.push(`Search: "${query}"`);
    if (sogSort !== "best") parts.push(`SOG sort: over ${sogSort}`);
    if (savesSort !== "best") parts.push(`Saves sort: over ${savesSort}`);
    return parts.length ? parts.join(" • ") : "No active board filters";
  }, [query, savesSort, sogSort]);

  const sogMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: topSogMarket?.marketProbability ?? null,
        marketSource: "OddsAPI market median",
        marketUpdatedAt: marketLoadedAt || null,
        modelUpdatedAt: loadedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: "NHL SOG model",
      }),
    [loadedAt, marketLoadedAt, topSogMarket?.marketProbability]
  );

  const savesMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: topSavesMarket?.marketProbability ?? null,
        marketSource: "OddsAPI market median",
        marketUpdatedAt: marketLoadedAt || null,
        modelUpdatedAt: loadedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: "NHL saves model",
      }),
    [loadedAt, marketLoadedAt, topSavesMarket?.marketProbability]
  );

  const boardMarketContext = useMemo(
    () =>
      buildMarketContext({
        marketProbability: topSogMarket?.marketProbability ?? null,
        marketSource: "OddsAPI market median",
        marketUpdatedAt: marketLoadedAt || null,
        modelUpdatedAt: loadedAt || null,
        marketSourceFallback: "OddsAPI market median",
        modelSourceFallback: "NHL board",
      }),
    [loadedAt, marketLoadedAt, topSogMarket?.marketProbability]
  );

  const boardControls = (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
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
      modes={MODES}
      activeMode={mode}
      onModeChange={setMode}
      controls={mode === "board" ? boardControls : null}
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
      ) : mode === "research" ? (
        <div className="space-y-6">
          {gamesLoading ? (
            <div className="pp-chip p-3 text-sm text-slate-500 text-center">Loading NHL slate...</div>
          ) : gamesError ? (
            <div className="pp-chip p-3 text-sm text-rose-700 text-center">{gamesError}</div>
          ) : (
            <TodayGamesNHL games={games} />
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <ModelVsMarketCard
              title="Top SOG Model Edge"
              lineLabel={
                topSog && topSogBest
                  ? `${topSog.player_name || topSog.player_id} • Over ${topSogBest.line}`
                  : "No SOG edge available"
              }
              modelProbability={topSogBest?.p ?? null}
              marketProbability={topSogMarket?.marketProbability ?? null}
              sourceLabel={sogMarketContext.sourceLabel}
              updatedLabel={sogMarketContext.updatedLabel}
              confidenceLabel={dataConfidence}
            />
            <ModelVsMarketCard
              title="Top Saves Model Edge"
              lineLabel={
                topSaves && topSavesBest
                  ? `${topSaves.player_name || topSaves.player_id} • Over ${topSavesBest.line}`
                  : "No saves edge available"
              }
              modelProbability={topSavesBest?.p ?? null}
              marketProbability={topSavesMarket?.marketProbability ?? null}
              sourceLabel={savesMarketContext.sourceLabel}
              updatedLabel={savesMarketContext.updatedLabel}
              confidenceLabel={dataConfidence}
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
          {gamesLoading ? (
            <div className="pp-chip p-3 text-sm text-slate-500 text-center">Loading NHL slate...</div>
          ) : gamesError ? (
            <div className="pp-chip p-3 text-sm text-rose-700 text-center">{gamesError}</div>
          ) : (
            <TodayGamesNHL games={games} />
          )}

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
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <section className="pp-card p-4">
              <h3 className="text-sm font-semibold text-slate-900 mb-2">Top Players in View</h3>
              {boardSummary.topPlayers.length === 0 ? (
                <div className="text-xs text-slate-500">No rows in current filter.</div>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {boardSummary.topPlayers.map(([name, count]) => (
                    <span key={name} className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-700">
                      {name}
                      <strong>{count}</strong>
                    </span>
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

          <ModelVsMarketCard
            title="Board Snapshot"
            lineLabel={
              topSog && topSogBest
                ? `${topSog.player_name || topSog.player_id} • Over ${topSogBest.line}`
                : "Top line snapshot"
            }
            modelProbability={topSogBest?.p ?? null}
            marketProbability={topSogMarket?.marketProbability ?? null}
            sourceLabel={boardMarketContext.sourceLabel}
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
                  <thead className="sticky top-0 z-10 bg-white">
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
                      <tr key={`${r.game_id}-${r.player_id}`} className="border-b border-slate-100">
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
                  <thead className="sticky top-0 z-10 bg-white">
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
                      <tr key={`${r.game_id}-${r.player_id}`} className="border-b border-slate-100">
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
