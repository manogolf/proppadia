import React, { useMemo, useState } from "react";

import PlayerPropFormV2 from "../components/PlayerPropFormv2.jsx";
import PlayerPropsTable from "../components/PlayerPropsTable.jsx";
import PropTracker from "../components/PropTracker.jsx";
import ModelVsMarketCard from "../components/predictions/ModelVsMarketCard.jsx";
import MyPropsPanel from "../components/predictions/MyPropsPanel.jsx";
import PredictionWorkspace from "../components/predictions/PredictionWorkspace.jsx";
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

  const subtitle = useMemo(() => {
    return mode === "research"
      ? "Resolve player and context, then generate model output."
      : "Review saved props by date and inspect tracking history.";
  }, [mode]);

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
          <ModelVsMarketCard
            title="Model vs Market (MLB)"
            lineLabel={
              latestPrediction?.features?.prop_type
                ? `${latestPrediction.features.prop_type} • ${latestPrediction.features.over_under || ""} ${latestPrediction.features.prop_value ?? ""}`
                : "Run a prediction to populate this card"
            }
            modelProbability={latestPrediction?.probability ?? null}
            marketProbability={latestPrediction?.marketProbability ?? null}
            sourceLabel={
              latestPrediction?.marketProbability != null
                ? (latestPrediction?.marketSource || "Market odds")
                : latestPrediction
                  ? "Model output"
                  : "Awaiting prediction"
            }
            updatedLabel={latestPrediction?.updatedAt ? new Date(latestPrediction.updatedAt).toLocaleString() : "-"}
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
