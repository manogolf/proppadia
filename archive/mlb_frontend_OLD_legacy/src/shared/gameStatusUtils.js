// backend/scripts/shared/gameStatusUtils.js

import { formatGameTime } from "./timeUtils.js";

/**
 * Returns a human-readable game status string.
 * @param {Object} game - MLB game object from API.
 */
/**
 * Returns true if the game status is final.
 * Accepts detailedState from MLB API or internal status.
 */
export function isGameFinal(status) {
  return typeof status === "string" && status.toLowerCase() === "final";
}

export function getStatusDisplay(game) {
  const status = game.status?.detailedState;

  if (status === "In Progress" && game.linescore) {
    const inningHalf = game.linescore.isTopInning ? "Top" : "Bot";
    const inning = game.linescore.currentInning;
    const outs = game.linescore.outs ?? "?";
    const homeScore = game.teams.home.score ?? 0;
    const awayScore = game.teams.away.score ?? 0;
    const balls = game.linescore.balls ?? "-";
    const strikes = game.linescore.strikes ?? "-";

    return `${inningHalf} ${inning} • ${awayScore}-${homeScore} • ${outs} out${
      outs === 1 ? "" : "s"
    } • ${balls}B ${strikes}S`;
  }

  if (status === "Scheduled") {
    const { etTime, localTime } = formatGameTime(game.gameDate);
    return `ET: ${etTime} / Local: ${localTime}`;
  }

  if (status === "Final") return "Final";

  return status || "Unknown";
}

/**
 * Returns true if the game is actively in progress.
 */
export function isGameLive(status) {
  return typeof status === "string" && status.toLowerCase() === "in progress";
}

/**
 * Returns a Tailwind CSS class for a given game status.
 * @param {string} status - Detailed game status string.
 */
export function getStatusColor(status) {
  if (status === "Final") return "text-red-500";
  if (status === "In Progress") return "text-green-600";
  if (status === "Postponed" || status?.includes("Delayed"))
    return "text-yellow-500";
  return "text-gray-500";
}

export async function fetchGameStatusById(gameId) {
  const url = `https://statsapi.mlb.com/api/v1.1/game/${gameId}/feed/live`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    return json?.gameData?.status?.detailedState ?? null;
  } catch (err) {
    console.error(`❌ Failed to fetch game status for ID ${gameId}:`, err);
    return null;
  }
}
