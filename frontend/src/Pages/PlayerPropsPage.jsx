import React, { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";

import TodayGames from "../components/TodayGames.jsx";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import PlayerPropFormV2 from "../components/PlayerPropFormv2.jsx";
import PlayerPropsTable from "../components/PlayerPropsTable.jsx";
import PropTracker from "../components/PropTracker.jsx";
import ModelVsMarketCard from "../components/predictions/ModelVsMarketCard.jsx";
import MyPropsPanel from "../components/predictions/MyPropsPanel.jsx";
import PredictionWorkspace from "../components/predictions/PredictionWorkspace.jsx";
import WorkspaceStatePanel from "../components/predictions/WorkspaceStatePanel.jsx";
import {
  MLB_WORKSPACE_MODES,
  WORKSPACE_MODE_BOARD,
  WORKSPACE_MODE_RESEARCH,
  isWorkspaceMode,
} from "../components/predictions/workspaceModes.js";
import { useAuth } from "../context/AuthContext.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";
import { normalizeHttpErrorMessage } from "../shared/httpErrorMessage.js";
import { buildMarketContext } from "../shared/marketContext.js";
import {
  WATCHLIST_SCOPE_MLB,
  WATCHLIST_UPDATED_EVENT,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../shared/watchlistStorage.js";
import { todayET } from "../shared/timeUtils.js";

export default function PlayerPropsPage() {
  const location = useLocation();
  const { user } = useAuth();
  const [mode, setMode] = useState(WORKSPACE_MODE_RESEARCH);
  const [selectedDate, setSelectedDate] = useState(todayET());
  const [tableRefreshNonce, setTableRefreshNonce] = useState(0);
  const [lastSaveEvent, setLastSaveEvent] = useState(null);
  const [latestPrediction, setLatestPrediction] = useState(null);
  const [watchlist, setWatchlist] = useState([]);
  const [games, setGames] = useState([]);
  const [gamesLoading, setGamesLoading] = useState(true);
  const [gamesError, setGamesError] = useState("");
  const [seedPlayerName, setSeedPlayerName] = useState("");
  const [seedTeamAbbr, setSeedTeamAbbr] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const modeFromUrl = String(params.get("mode") || "").trim().toLowerCase();
    const playerFromUrl = String(params.get("player") || "").trim();
    const teamFromUrl = String(params.get("team") || "").trim();
    const dateFromUrl = String(params.get("date") || "").trim();

    if (isWorkspaceMode(modeFromUrl)) {
      setMode(modeFromUrl);
    } else if (playerFromUrl || teamFromUrl) {
      // Deep links from Players-by-Team should open directly into board view.
      setMode(WORKSPACE_MODE_BOARD);
    }

    setSeedPlayerName(playerFromUrl);
    setSeedTeamAbbr(teamFromUrl);

    if (/^\d{4}-\d{2}-\d{2}$/.test(dateFromUrl)) {
      setSelectedDate(dateFromUrl);
    }
  }, [location.search]);

  const subtitle = useMemo(() => {
    return mode === WORKSPACE_MODE_RESEARCH
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
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    setWatchlist(readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB));
  }, [user?.id]);

  useEffect(() => {
    if (!user?.id) return;
    writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, watchlist);
  }, [user?.id, watchlist]);

  useEffect(() => {
    function refreshWatchlistFromStorage() {
      if (!user?.id) {
        setWatchlist([]);
        return;
      }
      setWatchlist(readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB));
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

  const currentPlayerWatchId = useMemo(() => {
    const features = latestPrediction?.features || {};
    return toWatchlistId({
      player_id: features.player_id || null,
      player_name: features.player_name || null,
      team: features.team || null,
    });
  }, [latestPrediction]);

  const currentPlayerWatched = useMemo(() => {
    if (!currentPlayerWatchId) return false;
    return watchlist.some((w) => String(w.id) === String(currentPlayerWatchId));
  }, [currentPlayerWatchId, watchlist]);
  const currentPlayerLastPropDate = useMemo(
    () => String(latestPrediction?.features?.last_prop_date || "").trim(),
    [latestPrediction]
  );

  function toggleCurrentPredictionWatch() {
    if (!user?.id || !latestPrediction?.features) return;
    const features = latestPrediction.features;
    const id = toWatchlistId({
      player_id: features.player_id || null,
      player_name: features.player_name || null,
      team: features.team || null,
    });
    if (!id) return;
    setWatchlist((prev) => {
      const exists = prev.some((w) => String(w.id) === String(id));
      if (exists) return prev.filter((w) => String(w.id) !== String(id));
      return [
        {
          id: String(id),
          player_id: features.player_id || null,
          player_name: features.player_name || null,
          team: features.team || null,
          added_at: new Date().toISOString(),
        },
        ...prev,
      ].slice(0, 100);
    });
  }

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
          setGamesError(normalizeHttpErrorMessage(e, "Failed to load MLB games."));
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

  const slateSection = useMemo(() => {
    if (gamesLoading) {
      return (
        <WorkspaceStatePanel
          kind="loading"
          title="Loading MLB slate"
          detail={`Checking schedule context for ${selectedDate}.`}
        />
      );
    }
    if (gamesError) {
      return <WorkspaceStatePanel kind="error" title="Could not load MLB slate" detail={gamesError} />;
    }
    return <TodayGames games={games} />;
  }, [games, gamesError, gamesLoading, selectedDate]);

  return (
    <PredictionWorkspace
      sportLabel="MLB"
      title="Prediction Workspace"
      subtitle={subtitle}
      dateLabel={`Selected Date (ET): ${selectedDate}`}
      modes={MLB_WORKSPACE_MODES}
      activeMode={mode}
      onModeChange={setMode}
    >
      {mode === WORKSPACE_MODE_RESEARCH ? (
        <div className="space-y-4">
          {slateSection}

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
            sourceKind={marketCtx.sourceKind}
            updatedLabel={marketCtx.updatedLabel}
            confidenceLabel={latestPrediction ? "Model" : "Pending"}
            badges={
              latestPrediction?.features?.player_id
                ? [{ label: currentPlayerWatched ? "Watched" : "Not watched", tone: currentPlayerWatched ? "success" : "muted" }]
                : []
            }
            actions={
              latestPrediction?.features?.player_id ? (
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={toggleCurrentPredictionWatch}
                  >
                    {currentPlayerWatched ? "Watching" : "+ Watch"}
                  </button>
                  <PrefetchLink
                    to={`/player/${encodeURIComponent(String(latestPrediction.features.player_id))}`}
                    className="text-xs text-slate-500 underline"
                  >
                    Open Player
                  </PrefetchLink>
                  <PrefetchLink to="/watchlist" className="text-xs text-slate-500 underline">
                    Open Watchlist
                  </PrefetchLink>
                  <span className="text-xs text-slate-500">
                    {currentPlayerLastPropDate ? `last prop ${currentPlayerLastPropDate}` : "last prop unavailable"}
                  </span>
                </div>
              ) : null
            }
          />

          <div className="pp-chip p-4">
            <PlayerPropFormV2
              initialPlayerName={seedPlayerName}
              initialTeamAbbr={seedTeamAbbr}
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
          {slateSection}

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
