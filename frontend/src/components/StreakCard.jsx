import React, { useState, useEffect } from "react";
import { fetchMlbCurrentPropHistoryAll } from "../lib/mlbPropsApi.js";
import { todayET, nowET } from "../shared/timeUtils.js";

const StreaksCard = () => {
  const [hotStreaks, setHotStreaks] = useState([]);
  const [coldStreaks, setColdStreaks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStreaks = async () => {
      try {
        const today = todayET();
        const sevenDaysAgo = nowET().minus({ days: 7 }).toISODate();
        const data = await fetchMlbCurrentPropHistoryAll({
          fromDate: sevenDaysAgo,
          toDate: today,
          propSource: "mlb_api",
        });

        const playerStreaks = {};

        data.forEach((prop) => {
          const key = `${prop.player_name}-${prop.prop_type}`;

          if (!playerStreaks[key]) {
            playerStreaks[key] = {
              player_name: prop.player_name,
              team: prop.team,
              prop_type: prop.prop_type,
              streak: 0,
              lastOutcome: null,
            };
          }

          const currentOutcome = prop.outcome;

          if (playerStreaks[key].lastOutcome === currentOutcome) {
            playerStreaks[key].streak += 1;
          } else {
            playerStreaks[key].streak = 1;
          }

          playerStreaks[key].lastOutcome = currentOutcome;
        });

        const hot = [];
        const cold = [];

        Object.values(playerStreaks).forEach((streak) => {
          if (streak.lastOutcome === "win" && streak.streak >= 2) {
            hot.push(streak);
          } else if (streak.lastOutcome === "loss" && streak.streak >= 2) {
            cold.push(streak);
          }
        });

        setHotStreaks(hot.sort((a, b) => b.streak - a.streak).slice(0, 5));
        setColdStreaks(cold.sort((a, b) => b.streak - a.streak).slice(0, 5));
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
                    key={`${player.player_name}-${player.prop_type}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {player.player_name} ({player.team})
                      </div>
                      <div className="text-sm text-slate-600">{player.prop_type}</div>
                    </div>
                    <div className="text-emerald-600 font-bold pl-4">
                      W{player.streak}
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
                    key={`${player.player_name}-${player.prop_type}`}
                    className="pp-chip p-3 grid grid-cols-[1fr_auto] items-center"
                  >
                    <div>
                      <div className="font-medium truncate">
                        {player.player_name} ({player.team})
                      </div>
                      <div className="text-sm text-slate-600">{player.prop_type}</div>
                    </div>
                    <div className="text-sky-600 font-bold pl-4">
                      L{player.streak}
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
