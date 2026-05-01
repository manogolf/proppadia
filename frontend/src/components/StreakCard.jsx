import React, { useState, useEffect } from "react";
import { fetchMlbStreakDashboard } from "../lib/mlbPropsApi.js";
import { todayET, nowET } from "../shared/timeUtils.js";

let streakDashboardCache = null;

function formatPropType(propType) {
  const prop = String(propType || "").trim();
  const labels = {
    hits: "Hits",
    total_bases: "Total Bases",
    hits_runs_rbis: "Hits + Runs + RBIs",
    rbis: "RBIs",
    runs_scored: "Runs",
    walks: "Plate discipline",
    strikeouts_batting: "Batter Ks",
  };
  return labels[prop] || "Player prop";
}

function streakCacheKey({ fromDate, toDate, propSource, limitPerSide }) {
  return [fromDate || "", toDate || "", propSource || "", limitPerSide || ""].join("|");
}

function streakLabel(player, fallbackSide) {
  const side = String(player?.streak_side || fallbackSide || "").toUpperCase();
  const prefix = side === "COLD" ? "L" : "W";
  return `${prefix}${Number(player?.primary_streak_count || 0)}`;
}

function streakReason(player, fallbackSide) {
  const side = String(player?.streak_side || fallbackSide || "").toUpperCase();
  const propKey = String(player?.primary_prop || "").trim();
  const prop = formatPropType(player?.primary_prop);
  const count = Number(player?.primary_streak_count || 0);
  const games = count === 1 ? "game" : "games";
  if (propKey === "strikeouts_batting") {
    return side === "COLD"
      ? `${prop} over in ${count} straight ${games}`
      : `${prop} under in ${count} straight ${games}`;
  }
  if (["total_bases", "hits_runs_rbis", "walks"].includes(propKey)) {
    return side === "COLD"
      ? `${prop} cold for ${count} straight ${games}`
      : `${prop} over in ${count} straight ${games}`;
  }
  if (side === "COLD") {
    return `${prop} cold for ${count} straight ${games}`;
  }
  return `${prop} in ${count} straight ${games}`;
}

const StreaksCard = () => {
  const [hotStreaks, setHotStreaks] = useState([]);
  const [coldStreaks, setColdStreaks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStreaks = async () => {
      try {
        const today = todayET();
        const fourteenDaysAgo = nowET().minus({ days: 14 }).toISODate();
        const request = {
          fromDate: fourteenDaysAgo,
          toDate: today,
          propSource: "mlb_api",
          limitPerSide: 5,
        };
        const cacheKey = streakCacheKey(request);
        const payload =
          streakDashboardCache?.key === cacheKey
            ? streakDashboardCache.payload
            : await fetchMlbStreakDashboard(request);
        streakDashboardCache = { key: cacheKey, payload };
        setHotStreaks(Array.isArray(payload?.hot) ? payload.hot : []);
        setColdStreaks(Array.isArray(payload?.cold) ? payload.cold : []);
      } catch (error) {
        console.error("Error in fetchStreaks:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchStreaks();
  }, []);

  return (
    <section className="pp-card p-6 space-y-6">
      <div className="flex items-center justify-center gap-4 text-4xl font-semibold text-slate-800 mb-4">
        <span className="flex items-center gap-2">🔥 Streaks Dashboard</span>
        <span className="text-4xl">❄️</span>
      </div>

      {loading ? (
        <div className="text-center text-slate-400">Loading streaks...</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Hot Streaks */}
          <div>
            <h3 className="text-lg font-semibold text-emerald-700 mb-2">
              Hot Streaks 🔥
            </h3>
            {hotStreaks.length === 0 ? (
              <div className="pp-chip p-3 text-sm text-slate-500">No hot streaks.</div>
            ) : (
              <ul className="space-y-2">
                {hotStreaks.map((player) => (
                  <li
                    key={`hot-${player.player_id || player.player_name}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {player.player_name} ({player.team})
                      </div>
                      <div className="text-sm text-slate-600">{formatPropType(player.primary_prop)}</div>
                      <div className="text-xs text-slate-500 mt-1">{streakReason(player, "HOT")}</div>
                    </div>
                    <div className="text-right pl-4">
                      <div className="text-emerald-600 font-bold">
                        {streakLabel(player, "HOT")}
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Cold Streaks */}
          <div>
            <h3 className="text-lg font-semibold text-sky-700 mb-2">
              Cold Streaks ❄️
            </h3>
            {coldStreaks.length === 0 ? (
              <div className="pp-chip p-3 text-sm text-slate-500">No cold streaks.</div>
            ) : (
              <ul className="space-y-2">
                {coldStreaks.map((player) => (
                  <li
                    key={`cold-${player.player_id || player.player_name}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {player.player_name} ({player.team})
                      </div>
                      <div className="text-sm text-slate-600">{formatPropType(player.primary_prop)}</div>
                      <div className="text-xs text-slate-500 mt-1">{streakReason(player, "COLD")}</div>
                    </div>
                    <div className="text-right pl-4">
                      <div className="text-sky-600 font-bold">
                        {streakLabel(player, "COLD")}
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}
    </section>
  );
};

export default StreaksCard;
