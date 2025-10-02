/**
 * ✅ FRONTEND-SAFE MLB API UTILS
 * - Can be used in PlayerPropForm or anywhere in the browser
 * - Includes safe schedule fetching (no node-fetch, no backend-only logic)
 */

import { normalizeTeamAbbreviation } from "./teamNameMap.js";
import { getTimeOfDayBucketET } from "./timeUtils.js";

/**
 * Fetches the full schedule of MLB games for a given ISO date (YYYY-MM-DD).
 * @param {string} dateISO
 * @returns {Promise<Array>} List of games on that date
 */
export async function getGameSchedule(dateISO) {
  const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dateISO}`;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    return json.dates?.[0]?.games || [];
  } catch (err) {
    console.error(`❌ Failed to fetch schedule for ${dateISO}:`, err);
    return [];
  }
}

/**
 * Resolves a single game's context for a team on a given date.
 * @param {string} teamAbbr - e.g., 'MIL'
 * @param {string} dateISO - e.g., '2025-07-29'
 * @returns {Promise<Object|null>}
 */
export async function getGameContextForTeam(teamAbbr, dateISO) {
  const normalizedTeam = normalizeTeamAbbreviation(teamAbbr);
  const games = await getGameSchedule(dateISO);

  const game = games.find((g) => {
    const homeAbbr = normalizeTeamAbbreviation(g.teams.home.team.abbreviation);
    const awayAbbr = normalizeTeamAbbreviation(g.teams.away.team.abbreviation);
    return homeAbbr === normalizedTeam || awayAbbr === normalizedTeam;
  });

  if (!game) {
    console.warn(`❌ No game found for ${teamAbbr} on ${dateISO}`);
    return null;
  }

  const isHome =
    normalizeTeamAbbreviation(game.teams.home.team.abbreviation) ===
    normalizedTeam;
  const opponentAbbr = isHome
    ? game.teams.away.team.abbreviation
    : game.teams.home.team.abbreviation;

  const gameTime = game.gameDate; // ISO string
  const localTime = new Date(gameTime);
  const hour = localTime.getHours();

  const time_of_day_bucket = getTimeOfDayBucketET(game_time);

  return {
    game_id: game.gamePk,
    is_home: isHome,
    opponent: normalizeTeamAbbreviation(opponentAbbr),
    game_time: gameTime,
    game_day_of_week: localTime.getDay(),
    time_of_day_bucket,
    starting_pitcher_id: isHome
      ? game.teams.away.probablePitcher?.id ?? null
      : game.teams.home.probablePitcher?.id ?? null,
  };
}
