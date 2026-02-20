import { useEffect, useMemo, useState } from "react";

import PredictionWorkspace from "../../components/predictions/PredictionWorkspace.jsx";
import WorkspaceStatePanel from "../../components/predictions/WorkspaceStatePanel.jsx";
import { useAuth } from "../../context/AuthContext.jsx";
import { nhlPlayerPropsAPI } from "../../lib/api.js";
import { normalizeHttpErrorMessage } from "../../shared/httpErrorMessage.js";
import { todayET } from "../../shared/timeUtils.js";

function num(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractOverLines(row) {
  const lines = [];
  for (const [key, value] of Object.entries(row || {})) {
    if (!key.startsWith("p_over_")) continue;
    const probability = num(value);
    if (probability == null) continue;
    const line = Number(key.replace("p_over_", "").replace(/_/g, "."));
    if (!Number.isFinite(line)) continue;
    lines.push({ line, probability });
  }
  lines.sort((a, b) => a.line - b.line);
  return lines;
}

function bestLine(row) {
  const lines = extractOverLines(row);
  if (lines.length === 0) return null;
  return [...lines].sort((a, b) => b.probability - a.probability)[0];
}

function formatProbability(value) {
  const parsed = num(value);
  if (parsed == null) return "--";
  return `${Math.round(parsed * 1000) / 10}%`;
}

export default function NHLPlayerPropsPage() {
  const { user } = useAuth();
  const [selectedDate, setSelectedDate] = useState(() => todayET());
  const [selectedMarket, setSelectedMarket] = useState("sog");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [rows, setRows] = useState([]);
  const [savingId, setSavingId] = useState("");
  const [saveError, setSaveError] = useState("");
  const [saveNotice, setSaveNotice] = useState("");
  const [loadedAt, setLoadedAt] = useState("");

  const marketAvailability = useMemo(
    () => nhlPlayerPropsAPI.getMarketAvailability(),
    []
  );

  const activeMarkets = useMemo(
    () => marketAvailability.filter((entry) => entry.status === "active"),
    [marketAvailability]
  );

  useEffect(() => {
    if (activeMarkets.some((entry) => entry.id === selectedMarket)) return;
    setSelectedMarket(activeMarkets[0]?.id || "sog");
  }, [activeMarkets, selectedMarket]);

  useEffect(() => {
    let cancelled = false;

    async function loadRows() {
      if (selectedMarket !== "sog") {
        setRows([]);
        setLoading(false);
        setError("");
        return;
      }

      try {
        setLoading(true);
        setError("");
        const payload = await nhlPlayerPropsAPI.listSogRows({
          date: selectedDate,
          limit: 200,
          offset: 0,
        });
        if (cancelled) return;
        setRows(Array.isArray(payload) ? payload : []);
        setLoadedAt(new Date().toISOString());
      } catch (err) {
        if (cancelled) return;
        setRows([]);
        setError(
          normalizeHttpErrorMessage(err, "Failed to load NHL shots-on-goal rows.")
        );
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadRows();
    return () => {
      cancelled = true;
    };
  }, [selectedDate, selectedMarket]);

  const filteredRows = useMemo(() => {
    const q = String(query || "").trim().toLowerCase();
    const withBest = (rows || [])
      .map((row) => ({ row, best: bestLine(row) }))
      .filter((entry) => entry.best);

    if (!q) {
      return withBest.sort((a, b) => b.best.probability - a.best.probability);
    }

    return withBest
      .filter(({ row }) => {
        const haystack = [
          row.player_name,
          row.player_id,
          row.team_abbr,
          row.game_id,
        ]
          .map((value) => String(value || "").toLowerCase())
          .join(" ");
        return haystack.includes(q);
      })
      .sort((a, b) => b.best.probability - a.best.probability);
  }, [query, rows]);

  async function saveSog(entry) {
    const selected = entry?.row;
    const best = entry?.best;
    if (!selected || !best) return;

    const playerId = num(selected.player_id);
    const gameId = num(selected.game_id);
    if (playerId == null || gameId == null) {
      setSaveError("Missing player or game identifier for this row.");
      return;
    }

    const uniqueId = `${selected.game_id}:${selected.player_id}:${best.line}`;

    try {
      setSavingId(uniqueId);
      setSaveError("");
      setSaveNotice("");

      const response = await nhlPlayerPropsAPI.addProp({
        player_id: playerId,
        player_name: selected.player_name || "",
        team: selected.team_abbr || selected.team || "",
        game_id: gameId,
        game_date: selectedDate,
        prop_type: "sog",
        prop_value: best.line,
        over_under: "over",
        probability: best.probability,
        prop_source: "nhl_user_added",
        user_id: user?.id || null,
      });

      if (!response?.ok) {
        throw new Error(response?.error || "NHL prop save failed.");
      }

      if (response?.duplicate) {
        setSaveNotice("Prop already exists for this player/line.");
      } else {
        setSaveNotice("Saved SOG prop to NHL history.");
      }
    } catch (err) {
      setSaveError(normalizeHttpErrorMessage(err, "Failed to save NHL SOG prop."));
    } finally {
      setSavingId("");
    }
  }

  const controls = (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
      <label className="text-sm text-slate-700">
        <div className="mb-1 font-medium">Market</div>
        <select
          className="w-full border border-slate-300 rounded-lg px-3 py-2 bg-white"
          value={selectedMarket}
          onChange={(event) => setSelectedMarket(event.target.value)}
        >
          {marketAvailability.map((entry) => (
            <option
              key={entry.id}
              value={entry.id}
              disabled={entry.status !== "active"}
            >
              {entry.label} {entry.status === "active" ? "(Active)" : "(Staged)"}
            </option>
          ))}
        </select>
      </label>

      <label className="text-sm text-slate-700">
        <div className="mb-1 font-medium">Date (ET)</div>
        <input
          type="date"
          className="w-full border border-slate-300 rounded-lg px-3 py-2"
          value={selectedDate}
          onChange={(event) => setSelectedDate(event.target.value)}
        />
      </label>

      <label className="text-sm text-slate-700">
        <div className="mb-1 font-medium">Player Search</div>
        <input
          type="text"
          className="w-full border border-slate-300 rounded-lg px-3 py-2"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Player, team, or id"
        />
      </label>
    </div>
  );

  return (
    <PredictionWorkspace
      sportLabel="NHL"
      title="Player Props Form"
      subtitle="Create NHL player props with staged market support."
      dateLabel={`Selected Date (ET): ${selectedDate}`}
      controls={controls}
    >
      <div className="space-y-4">
        <section className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {marketAvailability.map((entry) => {
            const badgeClass =
              entry.status === "active"
                ? "bg-emerald-100 text-emerald-700"
                : "bg-amber-100 text-amber-800";
            return (
              <div key={entry.id} className="pp-chip p-3">
                <div className="flex items-center justify-between gap-2">
                  <h2 className="text-sm font-semibold text-slate-900">{entry.label}</h2>
                  <span className={`text-xs px-2 py-0.5 rounded-full ${badgeClass}`}>
                    {entry.status}
                  </span>
                </div>
                <p className="text-xs text-slate-600 mt-2">{entry.description}</p>
              </div>
            );
          })}
        </section>

        {saveNotice ? (
          <WorkspaceStatePanel
            kind="loading"
            title={saveNotice}
            className="bg-emerald-50 text-emerald-700 border-emerald-200"
          />
        ) : null}
        {saveError ? <WorkspaceStatePanel kind="error" title="Save failed" detail={saveError} /> : null}

        {selectedMarket !== "sog" ? (
          <WorkspaceStatePanel
            kind="empty"
            title="This market is staged"
            detail="SOG is active now. Saves and points remain staged until enabled."
            centered
          />
        ) : loading ? (
          <WorkspaceStatePanel
            kind="loading"
            title="Loading NHL SOG rows"
            detail={`Fetching shots-on-goal model rows for ${selectedDate}.`}
            centered
          />
        ) : error ? (
          <WorkspaceStatePanel
            kind="error"
            title="Could not load NHL SOG rows"
            detail={error}
            centered
          />
        ) : filteredRows.length === 0 ? (
          <WorkspaceStatePanel
            kind="empty"
            title="No SOG rows found"
            detail="Try another date or clear the search filter."
            centered
          />
        ) : (
          <section className="pp-card p-4">
            <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
              <h3 className="text-lg font-semibold text-slate-900">SOG Candidate Rows</h3>
              <div className="text-xs text-slate-500">
                {filteredRows.length} rows
                {loadedAt ? ` • updated ${new Date(loadedAt).toLocaleTimeString()}` : ""}
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-left text-slate-600 border-b border-slate-200">
                    <th className="py-2 pr-3">Player</th>
                    <th className="py-2 pr-3">Team</th>
                    <th className="py-2 pr-3">Best Line</th>
                    <th className="py-2 pr-3">P(over)</th>
                    <th className="py-2 pr-0">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.slice(0, 100).map((entry) => {
                    const row = entry.row;
                    const best = entry.best;
                    const id = `${row.game_id}:${row.player_id}:${best.line}`;
                    return (
                      <tr key={id} className="border-b border-slate-100 align-top">
                        <td className="py-2 pr-3 text-slate-900">{row.player_name || row.player_id}</td>
                        <td className="py-2 pr-3 text-slate-700">{row.team_abbr || "--"}</td>
                        <td className="py-2 pr-3 text-slate-700">{best.line}</td>
                        <td className="py-2 pr-3 text-slate-700">{formatProbability(best.probability)}</td>
                        <td className="py-2 pr-0">
                          <button
                            type="button"
                            className="pp-btn pp-btn-secondary pp-btn-sm"
                            onClick={() => saveSog(entry)}
                            disabled={savingId === id}
                          >
                            {savingId === id ? "Saving..." : "Save"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </div>
    </PredictionWorkspace>
  );
}
