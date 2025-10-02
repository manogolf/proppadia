// shared/playerUtilsFrontend.js

import { getFullTeamAbbreviationFromID } from "./teamNameMap.jsx";

// ✅ Safely usable in frontend (no node-fetch or backend imports)

/**
 * Flatten a player's boxscore object to simpler batting/pitching stats.
 */
// flatten player export was here but moved to backend

/**
 * Returns true if player has any non-zero batting or pitching stats.
 */
export function didPlayerParticipate(stats) {
  if (!stats || typeof stats !== "object") return false;

  const hasBattingStats =
    stats.batting &&
    Object.values(stats.batting).some((v) => typeof v === "number" && v > 0);

  const hasPitchingStats =
    stats.pitching &&
    Object.values(stats.pitching).some((v) => typeof v === "number" && v > 0);

  return hasBattingStats || hasPitchingStats;
}

/**
 * Determine which team (home/away) the player is on from boxscore.
 */
export function getPlayerTeamFromBoxscoreData(boxscore, playerId) {
  for (const team of ["home", "away"]) {
    const players = boxscore?.teams?.[team]?.players || {};
    for (const player of Object.values(players)) {
      if (player?.person?.id === Number(playerId)) return team;
    }
  }
  return null;
}
