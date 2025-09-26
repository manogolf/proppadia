import React, { useState, useEffect } from "react";
import { supabase } from "../utils/supabaseFrontend.js";
import { nowET, todayET, currentTimeET } from "../shared/timeUtils.js";

const StreaksCard = () => {
  const [hotStreaks, setHotStreaks] = useState([]);
  const [coldStreaks, setColdStreaks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStreaks = async () => {
      try {
        const today = todayET();
        const sevenDaysAgo = nowET().minus({ days: 7 }).toISODate();

        const { data, error } = await supabase
          .from("player_props")
          .select("*")
          .gte("game_date", sevenDaysAgo)
          .lte("game_date", today);

        if (error) {
          console.error("Error fetching player props:", error.message);
          return;
        }

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
    <div className="bg-gray-200 p-6 rounded-xl shadow space-y-6">
      <div className="flex items-center justify-center gap-4 text-4xl font-semibold text-gray-800 mb-4">
        <span className="flex items-center gap-2">🔥 Streaks Dashboard</span>
        <span className="text-4xl">❄️</span>
      </div>

      {loading ? (
        <div className="text-center text-gray-400">Loading streaks...</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Hot Streaks */}
          <div>
            <h3 className="text-lg font-semibold text-green-700 mb-2">
              Hot Streaks 🔥
            </h3>
            <ul className="space-y-2">
              {hotStreaks.map((player) => (
                <li
                  key={`${player.player_name}-${player.prop_type}`}
                  className="p-3 rounded bg-green-50 border border-green-200 grid grid-cols-[1fr_auto] items-center"
                >
                  <div>
                    <div className="font-medium truncate">
                      {player.player_name} ({player.team})
                    </div>
                    <div className="text-sm text-gray-600">
                      {player.prop_type}
                    </div>
                  </div>
                  <div className="text-green-600 font-bold pl-4">
                    W{player.streak}
                  </div>
                </li>
              ))}
            </ul>
          </div>

          {/* Cold Streaks */}
          <div>
            <h3 className="text-lg font-semibold text-blue-700 mb-2">
              Cold Streaks ❄️
            </h3>
            <ul className="space-y-2">
              {coldStreaks.map((player) => (
                <li
                  key={`${player.player_name}-${player.prop_type}`}
                  className="p-3 rounded bg-blue-50 border border-blue-200 grid grid-cols-[1fr_auto] items-center"
                >
                  <div>
                    <div className="font-medium truncate">
                      {player.player_name} ({player.team})
                    </div>
                    <div className="text-sm text-gray-600">
                      {player.prop_type}
                    </div>
                  </div>
                  <div className="text-blue-600 font-bold pl-4">
                    L{player.streak}
                  </div>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}
    </div>
  );
};

export default StreaksCard;
