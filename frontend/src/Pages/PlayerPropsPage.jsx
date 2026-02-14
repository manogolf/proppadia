import React, { useEffect, useMemo, useState } from "react";

import TodayGames from "../components/TodayGames.jsx";
import PlayerPropFormV2 from "../components/PlayerPropFormv2.jsx";
import PlayerPropsTable from "../components/PlayerPropsTable.jsx";
import PropTracker from "../components/PropTracker.jsx";
import ModelVsMarketCard from "../components/predictions/ModelVsMarketCard.jsx";
import MyPropsPanel from "../components/predictions/MyPropsPanel.jsx";
import PredictionWorkspace from "../components/predictions/PredictionWorkspace.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";
import { buildMarketContext } from "../shared/marketContext.js";
import { todayET } from "../shared/timeUtils.js";

const MODES = [
  {
    id: "research",
    label: "Player Research",
    hint: "Single-player guided analysis",
  },
  {
    id: "board",
    label: "Market Board",
    hint: "Saved props and calendar view",
  },
];

export default function PlayerPropsPage() {
  const [mode, setMode] = useState("research");
  const [selectedDate, setSelectedDate] = useState(todayET());
  const [tableRefreshNonce, setTableRefreshNonce] = useState(0);
  const [lastSaveEvent, setLastSaveEvent] = useState(null);
  const [latestPrediction, setLatestPrediction] = useState(null);
  const [games, setGames] = useState([]);
  const [gamesLoading, setGamesLoading] = useState(true);
  const [gamesError, setGamesError] = useState("");

  const subtitle = useMemo(() => {
    return mode === "research"
      ? "Resolve player and context, then generate model output."
      : "Review saved props by date and inspect tracking history.";
  }, [mode]);
  const marketCtx = useMemo(
    () =>
      buildMarketContext({
        marketProbability: latestPrediction?.marketProbability ?? null,
        marketSource: latestPrediction?.marketSource || null,
        marketUpdatedAt: latestPrediction?.marketUpdatedAt || null,
        modelUpdatedAt: latestPrediction?.updatedAt || null,
        marketSourceFallback: "OddsAPI market",
        modelSourceFallback: latestPrediction ? "Model output" : "Awaiting prediction",
      }),
    [latestPrediction]
  );

  useEffect(() => {
    let cancelled = false;
    async function loadGames() {
      try {
        setGamesLoading(true);
        setGamesError("");
        const base = getBaseURL();
        const res = await fetch(
          `${base}/api/mlb/schedule?date=${encodeURIComponent(selectedDate)}`
        );
        const data = await res.json();
        const gameList = Array.isArray(data?.dates) ? data.dates[0]?.games || [] : [];
        if (!cancelled) setGames(Array.isArray(gameList) ? gameList : []);
      } catch (e) {
        if (!cancelled) {
          setGamesError(e?.message || "Failed to load MLB games.");
          setGames([]);
        }
      } finally {
        if (!cancelled) setGamesLoading(false);
      }
    }
    loadGames();
    return () => {
      cancelled = true;
    };
  }, [selectedDate]);

  return (
    <PredictionWorkspace
      sportLabel="MLB"
      title="Prediction Workspace"
      subtitle={subtitle}
      dateLabel={`Selected Date (ET): ${selectedDate}`}
      modes={MODES}
      activeMode={mode}
      onModeChange={setMode}
    >
      {mode === "research" ? (
        <div className="space-y-4">
          {gamesLoading ? (
            <div className="pp-chip p-3 text-sm text-slate-500 text-center">Loading MLB slate...</div>
          ) : gamesError ? (
            <div className="pp-chip p-3 text-sm text-rose-700 text-center">{gamesError}</div>
          ) : (
            <TodayGames games={games} />
          )}

          <ModelVsMarketCard
            title="Model vs Market (MLB)"
            lineLabel={
              latestPrediction?.features?.prop_type
                ? `${latestPrediction.features.prop_type} • ${latestPrediction.features.over_under || ""} ${latestPrediction.features.prop_value ?? ""}`
                : "Run a prediction to populate this card"
            }
            modelProbability={latestPrediction?.probability ?? null}
            marketProbability={latestPrediction?.marketProbability ?? null}
            sourceLabel={marketCtx.sourceLabel}
            updatedLabel={marketCtx.updatedLabel}
            confidenceLabel={latestPrediction ? "Model" : "Pending"}
          />

          <div className="pp-chip p-4">
            <PlayerPropFormV2
              onPredicted={(evt) => setLatestPrediction(evt || null)}
              onSaved={(evt) => {
                if (evt?.gameDate) setSelectedDate(evt.gameDate);
                setLastSaveEvent(evt || null);
                setTableRefreshNonce((n) => n + 1);
              }}
            />
          </div>
        </div>
      ) : (
        <div className="space-y-5">
          {gamesLoading ? (
            <div className="pp-chip p-3 text-sm text-slate-500 text-center">Loading MLB slate...</div>
          ) : gamesError ? (
            <div className="pp-chip p-3 text-sm text-rose-700 text-center">{gamesError}</div>
          ) : (
            <TodayGames games={games} />
          )}

          <MyPropsPanel
            refreshNonce={tableRefreshNonce}
            selectedDate={selectedDate}
          />
          <section className="pp-card p-4">
            <PlayerPropsTable
              selectedDate={selectedDate}
              refreshNonce={tableRefreshNonce}
              lastSaveEvent={lastSaveEvent}
            />
          </section>
          <section className="pp-card p-4">
            <PropTracker
              selectedDate={selectedDate}
              setSelectedDate={setSelectedDate}
            />
          </section>
        </div>
      )}
    </PredictionWorkspace>
  );
}
