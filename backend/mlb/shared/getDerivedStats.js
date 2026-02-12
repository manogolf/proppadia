// backend/scripts/shared/getDerivedStats.js
import {
  VALID_PROP_TYPES,
  extractStatForPropType,
  getRollingAverage,
} from "./propUtilsBackend.js";

export async function getDerivedStats(
  playerId,
  gameDate,
  gameId,
  cache, // Map of player_id -> rows (history mode) or a boxscore cache (boxscore mode)
  supabase,
  sourceMode = "history" // default to history, since generateDerivedStats passes "history"
) {
  const rollingDays = [7, 15, 30];
  const result = {};
  const now = new Date(gameDate);

  if (!Array.isArray(VALID_PROP_TYPES) || VALID_PROP_TYPES.length === 0) {
    throw new Error("VALID_PROP_TYPES is missing or empty");
  }

  for (const days of rollingDays) {
    for (const propType of VALID_PROP_TYPES) {
      const key = `d${days}_${propType}`;
      try {
        // 1) Try DB-based rolling average first
        const avg = await getRollingAverage(
          supabase,
          playerId,
          propType,
          gameDate,
          null,
          days
        );
        if (Number.isFinite(avg)) {
          result[key] = avg;
          continue;
        }

        // 2) Fallback compute
        const fromDate = new Date(now);
        fromDate.setDate(now.getDate() - days);

        let total = 0;
        let count = 0;

        if (sourceMode === "history") {
          const history = cache.get(String(playerId)) || [];
          for (const row of history) {
            const rowDate = new Date(row.game_date);
            if (rowDate >= now) continue; // strictly before gameDate
            if (row.prop_type !== propType) continue;
            if (Number.isFinite(row.prop_value)) {
              total += row.prop_value;
              count += 1;
            }
          }
        } else if (sourceMode === "boxscore") {
          if (!cache || typeof cache.entries !== "function") continue;

          for (const [, box] of cache.entries()) {
            const gameDateStr = box?.gameDate?.split("T")[0];
            if (!gameDateStr) continue;

            const boxDate = new Date(gameDateStr);
            if (boxDate < fromDate || boxDate >= now) continue;

            const allPlayers = {
              ...box?.teams?.home?.players,
              ...box?.teams?.away?.players,
            };

            const player = Object.values(allPlayers).find(
              (p) => String(p?.person?.id) === String(playerId)
            );
            if (!player) continue;

            const hasPitch =
              !!player.stats?.pitching &&
              Object.keys(player.stats.pitching).length > 0;
            const statBlock =
              hasPitch && propType.includes("pitching")
                ? player.stats.pitching
                : player.stats.batting;

            if (!statBlock) continue;

            // ✅ correct order for your backend util: (statsObject, propType)
            const val = extractStatForPropType(statBlock, propType);
            if (Number.isFinite(val)) {
              total += val;
              count += 1;
            }
          }
        }

        if (count > 0) {
          result[key] = total / count;
        }
      } catch (err) {
        // swallow per-key errors to keep the rest going
      }
    }
  }

  return result; // ✅ the missing piece
}
