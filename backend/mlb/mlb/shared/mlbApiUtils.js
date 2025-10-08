// backend/scripts/shared/mlbApiUtils.js
import fetch from "node-fetch";
import {
  getTeamInfoByAbbr,
  normalizeTeamAbbreviation,
} from "../../../shared/teamNameMap.js";
import {
  getGameStartTimeET,
  todayET,
  getTimeOfDayBucketET,
} from "./timeUtilsBackend.js";

/**
 * Returns full boxscore JSON for a given MLB game_id.
 * @param {number|string} gameId
 * @returns {Promise<Object|null>}
 */
export async function getBoxscoreFromGameID(gameId) {
  const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.error(`❌ Failed to fetch boxscore for game ${gameId}:`, err);
    return null;
  }
}

/**
 * Fetches full game context for a given game ID and team.
 * Requires team abbreviation to determine home/away.
 */
export async function getGameContextFields(gameId, teamAbbr) {
  const boxscore = await getBoxscoreFromGameID(gameId);

  const homeTeam = boxscore?.teams?.home?.team?.abbreviation;
  const awayTeam = boxscore?.teams?.away?.team?.abbreviation;

  if (!homeTeam || !awayTeam || !teamAbbr) {
    console.warn(`⚠️ Could not determine teams for game ${gameId}`);
    return null;
  }

  const normalizedTeamAbbr = teamAbbr.toUpperCase();
  const is_home = normalizedTeamAbbr === homeTeam.toUpperCase();
  const home_away = is_home ? "home" : "away";
  const opponentAbbr = is_home ? awayTeam : homeTeam;

  if (!opponentAbbr) {
    console.warn(`⚠️ Missing opponentAbbr for game ${gameId} and team ${team}`);
    return {};
  }

  const normalizedOpponentAbbr = normalizeTeamAbbreviation(opponentAbbr);
  // console.log("🏷️ Original opponentAbbr:", opponentAbbr);
  // console.log("🎯 Normalized opponentAbbr:", normalizedOpponentAbbr);

  const teamInfo = getTeamInfoByAbbr(normalizedOpponentAbbr);
  console.log("🔬 teamInfo returned:", teamInfo);

  const opponent_encoded = teamInfo?.id != null ? String(teamInfo.id) : null;

  const game_time = await getGameStartTimeET(gameId);
  const game_day_of_week = game_time ? todayET(game_time) : null;
  const time_of_day_bucket = game_time ? getTimeOfDayBucketET(game_time) : null;

  return {
    is_home,
    home_away,
    opponent: opponentAbbr,
    opponent_encoded,
    game_time,
    game_day_of_week,
    time_of_day_bucket,
  };
}

/**
 * Fetches the full live game feed for a given game ID.
 * Includes liveData.plays.allPlays, needed for BvP and PvB analysis.
 */
export async function getLiveFeedFromGameID(gameId) {
  const url = `https://statsapi.mlb.com/api/v1.1/game/${gameId}/feed/live`;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.error(`❌ Failed to fetch live feed for game ${gameId}:`, err);
    return null;
  }
}

export async function preloadBoxscoresForGame(gameId) {
  const box = await getBoxscoreFromGameID(gameId);
  return new Map([[gameId, box]]);
}
