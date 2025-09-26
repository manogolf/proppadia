// 📄 File: backend/scripts/shared/getDerivedStats.js

import fetch from "node-fetch";
import { toISODate } from "./timeUtilsBackend";
import { extractStatForPropType } from "./propUtils.js"; // Assumes this exists for stat resolution

/**
 * Aggregates player prop stats (d7/d15/d30) using cached boxscore data.
 *
 * @param {string|number} playerId - MLB player ID
 * @param {string} gameDate - ISO date of the current game
 * @returns {Promise<Object>} - { d7_hits, d15_total_bases, ... }
 */
export async function getDerivedStats(playerId, gameDate) {
  const rollingDays = [7, 15, 30];
  const propTypes = [
    "at_bats",
    "base_on_balls",
    "doubles",
    "hits",
    "hits_runs_rbis",
    "home_runs",
    "plate_appearances",
    "rbis",
    "runs",
    "runs_rbis",
    "singles",
    "strikeouts_batting",
    "strikeouts_pitching",
    "total_bases",
    "triples",
    "outs_recorded",
    "earned_runs",
    "pitches",
  ];

  const results = {};
  const now = new Date(gameDate);
  const boxscoreCache = new Map();

  for (const days of rollingDays) {
    const fromDate = new Date(now);
    fromDate.setDate(now.getDate() - days);

    for (let d = new Date(fromDate); d <= now; d.setDate(d.getDate() + 1)) {
      const dISO = toISODate(d);
      const schedUrl = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dISO}`;
      const schedRes = await fetch(schedUrl)
        .then((r) => r.json())
        .catch(() => null);
      const gameIds = (schedRes?.dates?.[0]?.games || []).map((g) => g.gamePk);

      for (const gamePk of gameIds) {
        if (!boxscoreCache.has(gamePk)) {
          const boxUrl = `https://statsapi.mlb.com/api/v1/game/${gamePk}/boxscore`;
          const boxRes = await fetch(boxUrl)
            .then((r) => r.json())
            .catch(() => null);
          if (!boxRes) continue;
          boxscoreCache.set(gamePk, boxRes);
        }

        const box = boxscoreCache.get(gamePk);
        const allPlayers = {
          ...box?.teams?.home?.players,
          ...box?.teams?.away?.players,
        };
        const match = Object.values(allPlayers).find(
          (p) => String(p?.person?.id) === String(playerId)
        );
        if (!match) continue;

        for (const propType of propTypes) {
          const key = `d${days}_${propType}`;
          const val = extractStatForPropType(
            propType,
            match.stats?.batting || match.stats?.pitching || {}
          );
          if (typeof val === "number") {
            results[key] = (results[key] || 0) + val;
          }
        }
      }
    }
  }

  return results;
}
