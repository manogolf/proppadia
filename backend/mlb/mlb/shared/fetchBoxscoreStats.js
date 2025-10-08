// backend/scripts/shared/fetchBoxscoreStats.js

import fetch from "node-fetch";
import { flattenPlayerBoxscore } from "./playerUtilsBackend.js";

// 🔍 Fetch entire boxscore and flatten all players
export async function fetchBoxscoreStatsForGame(gamePk) {
  const url = `https://statsapi.mlb.com/api/v1/game/${gamePk}/boxscore`;
  console.log(`📡 Fetching boxscore for game ${gamePk}`);

  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const json = await res.json();

    const players = [];

    for (const side of ["home", "away"]) {
      const team = json.teams?.[side];
      const playerMap = team?.players || {};

      for (const key in playerMap) {
        const player = playerMap[key];
        const flattened = flattenPlayerBoxscore(player);
        const id = player.person?.id;
        const name = player.person?.fullName;

        if (id) {
          players.push({
            id,
            fullName: name,
            teamAbbr: team?.team?.abbreviation,
            isHome: side === "home",
            stats: flattened,
          });
        }
      }
    }

    console.log(`📦 Parsed ${players.length} players from game ${gamePk}`);
    return players;
  } catch (err) {
    console.error(
      `❌ Failed to fetch boxscore for game ${gamePk}:`,
      err.message
    );
    return null;
  }
}
