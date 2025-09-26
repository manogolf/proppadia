// backend/scripts/shared/fetchGameID.js

import { getGameSchedule } from "../../../shared/mlbApiUtilsFrontend.js";

/**
 * Finds the gamePk for the given team_id on the given date.
 * @param {number} teamId - The MLB team ID (e.g. 144 for ATL)
 * @param {string} dateISO - The date string (e.g. '2025-08-01')
 * @returns {Promise<number|null>}
 */

export async function getGamePkForTeamOnDate(teamId, dateISO) {
  const games = await getGameSchedule(dateISO);

  for (const game of games) {
    if (
      game.teams?.home?.team?.id === teamId ||
      game.teams?.away?.team?.id === teamId
    ) {
      return game.gamePk;
    }
  }

  console.warn(`⚠️ No game found for team ID ${teamId} on ${dateISO}`);
  return null;
}
