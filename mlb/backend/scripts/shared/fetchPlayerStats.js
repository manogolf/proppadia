// backend/scripts/shared/fetchPlayerStats.js
import fetch from "node-fetch";

// 🔄 Unified function for both seasonal/career stats and per-game boxscores
export async function fetchPlayerStats({
  playerId = null,
  gameId = null,
  year = null,
}) {
  // 🧪 If gameId is passed, fetch all player stats for a single game
  if (gameId) {
    const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;

    try {
      const res = await fetch(url);
      if (!res.ok) {
        console.warn(
          `⚠️ Boxscore fetch failed for game ${gameId}:`,
          res.status
        );
        return null;
      }

      const data = await res.json();
      if (!data?.teams?.home || !data?.teams?.away) return null;

      return {
        home: data.teams.home.players ?? {},
        away: data.teams.away.players ?? {},
      };
    } catch (err) {
      console.error("❌ Error fetching boxscore:", err.message);
      return null;
    }
  }

  // ⚾ Otherwise, fetch seasonal + career stats for a given playerId
  const groupTypes = ["hitting", "pitching"];
  const results = {};

  for (const group of groupTypes) {
    const seasonStatsUrl = `https://statsapi.mlb.com/api/v1/people/${playerId}/stats?stats=season&season=${year}&group=${group}`;
    const careerStatsUrl = `https://statsapi.mlb.com/api/v1/people/${playerId}/stats?stats=career&group=${group}`;

    try {
      const [seasonRes, careerRes] = await Promise.all([
        fetch(seasonStatsUrl).then((res) => res.json()),
        fetch(careerStatsUrl).then((res) => res.json()),
      ]);

      if (seasonRes?.stats?.[0]?.splits?.[0]?.stat) {
        results[`${group}_season`] = seasonRes.stats[0].splits[0].stat;
      }

      if (careerRes?.stats?.[0]?.splits?.[0]?.stat) {
        results[`${group}_career`] = careerRes.stats[0].splits[0].stat;
      }
    } catch (err) {
      console.warn(
        `⚠️ Failed to fetch ${group} stats for ${playerId}:`,
        err.message
      );
    }
  }

  return results;
}
