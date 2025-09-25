// backend/scripts/resolution/statResolvers.js

import { getStatFromLiveFeed } from "./getStatFromLiveFeed.js";
import {
  flattenPlayerBoxscore,
  getPlayerStatsFromBoxscore,
} from "../shared/playerUtilsBackend.js";
import { derivePropValue } from "./derivePropValue.js";

/** Verbose toggle (quiet by default; set VERBOSE=1 to enable chatty logs) */
const VERBOSE = process.env.VERBOSE === "1";
const dbg = (...a) => {
  if (VERBOSE) console.log(...a);
};

/**
 * Resolves a stat value using boxscore first, then allPlays as fallback.
 */

export function hasMeaningfulStats(stats) {
  if (!stats || typeof stats !== "object") return false;

  const { batting = {}, pitching = {} } = stats;

  const hasBatting =
    typeof batting.hits === "number" ||
    typeof batting.runs === "number" ||
    typeof batting.rbi === "number" ||
    typeof batting.totalBases === "number" ||
    typeof batting.baseOnBalls === "number" ||
    typeof batting.strikeOuts === "number" ||
    typeof batting.homeRuns === "number" ||
    typeof batting.doubles === "number" ||
    typeof batting.triples === "number" ||
    typeof batting.stolenBases === "number";

  const hasPitching =
    typeof pitching.strikeOuts === "number" ||
    typeof pitching.baseOnBalls === "number" ||
    typeof pitching.hits === "number" ||
    typeof pitching.earnedRuns === "number" ||
    typeof pitching.outs === "number";

  return hasBatting || hasPitching;
}

export function hasMeaningfulBvPStats(stats) {
  if (!stats || typeof stats !== "object") return false;

  return (
    typeof stats.hits === "number" ||
    typeof stats.rbi === "number" ||
    typeof stats.total_bases === "number" ||
    typeof stats.pa === "number" ||
    typeof stats.ab === "number"
  );
}

export async function resolveStatForPlayer(options) {
  const {
    player_id,
    player_name,
    game_id,
    team,
    prop_type,
    mode,
    batter_id,
    pitcher_id,
  } = options;

  // Info banner → gated
  dbg(
    `📡 Resolving stat for ${player_name} (${prop_type}) — Game ID: ${game_id}`
  );

  const boxscoreData = await getPlayerStatsFromBoxscore({
    game_id,
    player_id,
    player_name,
    team,
    prop_type,
  });

  if (!boxscoreData) {
    console.warn(`📭 No boxscore data found for ${player_name} (${prop_type})`);
    return { result: undefined, source: "no_boxscore", rawStats: undefined };
  }

  const rawStats = flattenPlayerBoxscore(boxscoreData);

  // Big dumps → gated
  if (VERBOSE) {
    if (rawStats === null) {
      console.log(`📊 Raw stats for ${player_name} (${prop_type}): null`);
    } else if (typeof rawStats === "object") {
      const keys = Object.keys(rawStats);
      console.log(
        `📊 Raw stats for ${player_name} (${prop_type}):`,
        JSON.stringify(rawStats, null, 2)
      );
      console.log(
        `🔬 Keys present in rawStats:`,
        keys.length ? keys : "(empty object)"
      );
    } else {
      console.log(
        `❓ Unexpected rawStats type for ${player_name}:`,
        typeof rawStats
      );
    }
  }

  if (rawStats && hasMeaningfulStats(rawStats)) {
    try {
      const extracted = derivePropValue(prop_type, rawStats);

      if (extracted == null) {
        console.warn(
          `⚠️ Could not extract value for ${player_name} (${prop_type})`
        );
      }

      dbg(`🎯 Extracted value from boxscore: ${extracted}`);

      if (extracted != null) {
        return {
          result: extracted,
          source: "boxscore",
          rawStats,
        };
      }
    } catch (err) {
      console.error(
        `❌ Extraction failed for ${player_name} (${prop_type})`,
        err
      );
    }
  } else {
    console.warn(
      `⚠️ Boxscore stats were invalid for ${player_name} (${prop_type})`
    );
  }

  // Fallback to live → info logs gated
  const liveResult = await getStatFromLiveFeed(game_id, player_id, prop_type);

  dbg(`📺 Live fallback result for ${player_name}:`, liveResult);
  dbg(
    `🧪 Final result for ${player_name} (${prop_type}) → ${
      liveResult ?? "null"
    }`
  );

  return {
    result: typeof liveResult === "number" ? liveResult : undefined,
    source: typeof liveResult === "number" ? "live" : "missing",
    rawStats,
  };
}
