// 📁 getStatFromLiveFeed.js

import fetch from "node-fetch";
import { normalizePropType } from "../shared/propUtilsBackend.js";
import { extractStatFromPlays } from "../shared/extractStatFromPlays.js";
import { flattenPlayerBoxscore } from "../shared/playerUtilsBackend.js";
import { derivePropValue } from "../resolution/derivePropValue.js";

// 🧠 Prop types that rely on pitching stats
const PITCHING_PROPS = new Set([
  "strikeouts_pitching",
  "walks_allowed",
  "earned_runs",
  "outs_recorded",
  "hits_allowed",
]);

export async function getStatFromLiveFeed(gameId, playerId, propType) {
  const url = `https://statsapi.mlb.com/api/v1.1/game/${gameId}/feed/live`;
  const normalized = normalizePropType(propType);

  try {
    const response = await fetch(url);
    const data = await response.json();
    const plays = data?.liveData?.plays?.allPlays;

    if (!Array.isArray(plays)) {
      console.warn(
        `⚠️ Live feed format unexpected or incomplete for game ${gameId}`
      );
      return null;
    }

    // 🧪 Extract and flatten from plays
    const statMap = extractStatFromPlays(plays, playerId);
    const flat = flattenPlayerBoxscore(statMap);

    const scopedStats = PITCHING_PROPS.has(normalized)
      ? { pitching: flat.pitching ?? {} }
      : { batting: flat.batting ?? {} };

    const result = derivePropValue(normalized, scopedStats);
    console.log(
      `🎯 Final derived value for ${propType} (${playerId}) in game ${gameId}:`,
      result
    );
    return result;
  } catch (err) {
    console.error(`❌ Live feed fetch failed for game ${gameId}:`, err.message);
    return null;
  }
}
