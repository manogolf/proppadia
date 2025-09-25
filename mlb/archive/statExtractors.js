// ⚠️ DEPRECATED: use propExtractors in propUtils.js instead.

// src/scripts/resolution/statExtractors.js

/**
 * Extracts the relevant stat value from a player's game data
 * based on the prop type tracked in our system.
 *
 * @param {string} propType - Normalized prop type (e.g., 'runs_scored')
 * @param {object} playerData - Boxscore or live player data from the MLB API
 * @returns {number|null} - Stat value for that prop type or null if not found
 */
export function extractStatForPropType(propType, playerData) {
  const statMap = {
    hits: playerData.hits ?? null,
    runs_scored: playerData.runs ?? null,
    rbis: playerData.rbis ?? null,
    home_runs: playerData.home_runs ?? null,
    singles:
      (playerData.hits ?? 0) -
      (playerData.doubles ?? 0) -
      (playerData.triples ?? 0) -
      (playerData.home_runs ?? 0),
    doubles: playerData.doubles ?? null,
    triples: playerData.triples ?? null,
    walks: playerData.walks ?? null,
    strikeouts_batting: playerData.strikeouts_batting ?? null,
    stolen_bases: playerData.stolen_bases ?? null,
    total_bases: playerData.total_bases ?? null,
    hits_runs_rbis:
      (playerData.hits ?? 0) + (playerData.runs ?? 0) + (playerData.rbis ?? 0),
    runs_rbis: (playerData.runs ?? 0) + (playerData.rbis ?? 0),

    // Pitching props
    outs_recorded: playerData.outs_recorded ?? null,
    strikeouts_pitching: playerData.strikeouts_pitching ?? null,
    walks_allowed: playerData.walks_allowed ?? null,
    earned_runs: playerData.earned_runs ?? null,
    hits_allowed: playerData.hits_allowed ?? null,
  };

  return statMap[propType] ?? null;
}
