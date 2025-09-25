/**
 * 🛑 DEPRECATION NOTICE — July 2025
 *
 * This script is officially deprecated and should no longer be used.
 *
 * 🧹 Reason for deprecation:
 * All training fields previously handled here — including:
 *   - rolling_result_avg_7
 *   - hit_streak / win_streak
 *   - game_time
 *   - is_home / home_away
 *   - opponent / opponent_team_id / opponent_encoded
 *   - prop_value / over_under
 * — are now handled by specialized, reliable, and faster scripts:
 *   ✅ insertStatDerivedProps.js
 *   ✅ generateDerivedStats.js
 *   ✅ generatePlayerStreakProfiles.js
 *   ✅ backfillGameContextFields.js
 *
 * 🛑 Running this script may override accurate values with incomplete or outdated logic.
 * It is retained for reference only and should not be invoked by cron or CLI.
 *
 * 🔒 To enforce deprecation, disable or remove any use of:
 *   node scripts/backfillTrainingFieldsExtended.js
 */

/**
 * 📄 File: scripts/backfillTrainingFieldsExtended.js
 *
 * 🔍 Description:
 * This script performs a comprehensive backfill of missing training features in the
 * `model_training_props` table. It is designed to fill in any null or incomplete fields
 * critical for model training, such as:
 *   - Rolling average (7-day result)
 *   - Hit/win streaks
 *   - Game time
 *   - Home/away status
 *   - Opponent
 *   - Derived prop value (if missing but result exists)
 *   - Inferred over/under (if missing but result + value exist)
 *
 * 🧱 It fetches all incomplete rows and processes them grouped by player, supporting
 * optional bucketing to parallelize large backfills.
 *
 * ✅ Triggered manually or via cron using:
 *   `node scripts/backfillTrainingFieldsExtended.js`
 *   or with buckets:
 *   `node scripts/backfillTrainingFieldsExtended.js --bucket=1/4`
 *
 * 🧩 Dependencies:
 * - Supabase DB connection
 * - Shared utilities: propUtils.js, playerUtils.js, timeUtils.js, fetchSchedule.js
 *
 * 🎯 Purpose:
 * Ensures all training rows are complete and feature-rich before being used for model
 * retraining or evaluation. Critical to maintain feature consistency across rows.
 */

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import {
  getRollingAverage,
  determineHomeAway,
  determineOpponent,
} from "../../../shared/propUtils.js";
import { getStreaksForPlayer } from "../../shared/playerUtilsFrontend.js";
import { getGameTimeFromID } from "../backend/scripts/shared/fetchSchedule.js";

console.log("🚀 Starting extended backfill for training fields...");

const BATCH_SIZE = 500;

// Bucket setup
const bucketArg = process.argv.find((arg) => arg.startsWith("--bucket="));
const bucketInfo = bucketArg
  ? bucketArg.replace("--bucket=", "").split("/")
  : null;

let currentBucket = 0;
let totalBuckets = 1;

if (bucketInfo && bucketInfo.length === 2) {
  currentBucket = parseInt(bucketInfo[0]) - 1;
  totalBuckets = parseInt(bucketInfo[1]);
}

function isTrainingRowComplete(row) {
  return (
    row.rolling_result_avg_7 != null &&
    row.hit_streak != null &&
    row.win_streak != null &&
    row.game_time != null &&
    row.is_home != null &&
    row.opponent != null &&
    row.prop_value != null &&
    row.over_under != null
  );
}

export async function runTrainingBackfillIfNeeded() {
  const { data } = await supabase
    .from("model_training_props")
    .select("id")
    .is("game_time", null)
    .limit(1);

  if (data.length === 0) {
    console.log("✅ Training data already complete. Skipping backfill.");
    return;
  }

  console.log("⚠️ Incomplete training rows found. Running backfill...");
  await runExtendedBackfill();
}

async function fetchAllIncompleteRowsForPlayers(playerIds) {
  if (!playerIds || playerIds.length === 0) return [];

  const { data, error } = await supabase
    .from("model_training_props")
    .select("*")
    .in("player_id", playerIds)
    .or(
      "rolling_result_avg_7.is.null,hit_streak.is.null,win_streak.is.null,game_time.is.null,is_home.is.null,opponent.is.null,prop_value.is.null,over_under.is.null"
    )
    .order("game_date", { ascending: true });

  if (error) {
    console.error("❌ Error fetching incomplete rows:", error.message);
    return [];
  }

  return data;
}

export async function runExtendedBackfill() {
  const { data: allIncomplete } = await supabase
    .from("model_training_props")
    .select("player_id")
    .or(
      "rolling_result_avg_7.is.null,hit_streak.is.null,win_streak.is.null,game_time.is.null,is_home.is.null,opponent.is.null,prop_value.is.null,over_under.is.null"
    );

  if (!allIncomplete || allIncomplete.length === 0) {
    console.log("🎉 No incomplete training rows found.");
    return;
  }

  const allPlayerIds = [...new Set(allIncomplete.map((row) => row.player_id))];
  const filteredPlayerIds = allPlayerIds.filter(
    (_, index) => index % totalBuckets === currentBucket
  );

  console.log(
    `🧩 Bucket ${currentBucket + 1}/${totalBuckets} → ${
      filteredPlayerIds.length
    } players`
  );

  const rows = await fetchAllIncompleteRowsForPlayers(filteredPlayerIds);

  if (!rows || rows.length === 0) {
    console.log("🎉 No incomplete rows found in this bucket.");
    return;
  }

  const grouped = {};
  for (const row of rows) {
    if (!grouped[row.player_id]) grouped[row.player_id] = [];
    grouped[row.player_id].push(row);
  }

  let updated = 0,
    failed = 0;
  const rowUpdates = [];
  const skippedByProp = {};

  const playerIds = Object.keys(grouped);
  for (let i = 0; i < playerIds.length; i++) {
    const player_id = playerIds[i];
    const playerProps = grouped[player_id];

    console.log(
      `🔍 (${i + 1}/${filteredPlayerIds.length}) Player ${player_id} → ${
        playerProps.length
      } rows`
    );

    let streaks = null;
    const rollingAvgPromises = [];
    const rowUpdates = [];

    for (const row of playerProps) {
      if (isTrainingRowComplete(row)) continue;

      const updates = {};

      try {
        if (row.rolling_result_avg_7 == null) {
          const avg = await getRollingAverage(
            supabase,
            row.player_id,
            row.prop_type,
            row.game_date,
            row.game_id,
            7
          );
          if (avg != null && !isNaN(avg)) {
            updates.rolling_result_avg_7 = avg;
          }
        }

        if (row.hit_streak == null || row.win_streak == null) {
          const streaks = await getStreaksForPlayer(
            row.player_id,
            row.prop_type,
            row.prop_source || "mlb_api"
          );

          if (!streaks || streaks.streak_count == null) {
            skippedByProp[row.prop_type] =
              (skippedByProp[row.prop_type] || 0) + 1;
            continue;
          }

          if (row.hit_streak == null) updates.hit_streak = streaks.streak_count;
          if (row.win_streak == null) updates.win_streak = streaks.streak_count;
        }

        if (row.game_time == null && row.game_id) {
          const gameTime = await getGameTimeFromID(row.game_id);
          if (gameTime != null) updates.game_time = gameTime;
        }

        if (row.is_home == null && row.team && row.game_id) {
          const isHome = await determineHomeAway(
            supabase,
            row.team,
            row.game_id
          );
          if (typeof isHome === "boolean") updates.is_home = isHome;
        }

        if (row.opponent == null && row.team && row.game_id) {
          const opponent = await determineOpponent(
            supabase,
            row.team,
            row.game_id
          );
          if (opponent != null) updates.opponent = opponent;
        }

        const parsedValue = parseFloat(row.result);
        if (
          row.prop_value == null &&
          row.result != null &&
          !isNaN(parsedValue) &&
          row.source === "mlb_api"
        ) {
          updates.prop_value = parsedValue;
        }

        const actual = parseFloat(row.result);
        const line = parseFloat(row.prop_value);
        if (
          row.over_under == null &&
          row.predicted_outcome &&
          !isNaN(actual) &&
          !isNaN(line)
        ) {
          updates.over_under =
            actual > line ? "over" : actual < line ? "under" : "push";
        }

        // ✅ Collect update if changes exist
        if (Object.keys(updates).length > 0) {
          // Only include non-null updates
          const cleanUpdates = {};
          for (const [key, val] of Object.entries(updates)) {
            if (val !== null && val !== undefined) {
              cleanUpdates[key] = val;
            }
          }

          if (Object.keys(cleanUpdates).length > 0) {
            rowUpdates.push({ id: row.id, updates: cleanUpdates });
          }
        }
      } catch (err) {
        console.error(`❌ Failed to prepare row ${row.id}:`, err.message);
        failed++;
      }
    }

    // Wait for all rolling averages to resolve
    await Promise.all(rollingAvgPromises);

    // Apply updates
    if (rowUpdates.length > 0) {
      const { error: rpcError } = await supabase.rpc(
        "batch_update_training_props",
        {
          rows: rowUpdates.map(({ id, updates }) => ({
            id,
            ...updates,
          })),
        }
      );

      if (rpcError) {
        console.error("❌ Batch update failed:", rpcError.message);
        failed += rowUpdates.length;
      } else {
        updated += rowUpdates.length;
      }

      // Clear for next player
      rowUpdates.length = 0;
    }

    if ((i + 1) % 100 === 0) {
      console.log(`⏳ Player progress: ${i + 1}/${filteredPlayerIds.length}`);
      console.log(`   → ✅ ${updated} | ❌ ${failed}`);
    }
  }

  console.log(`\n🏁 Final Backfill Summary → ✅ ${updated} | ❌ ${failed}`);

  if (Object.keys(skippedByProp).length) {
    console.log(`\n⚠️ Skipped due to missing streak profiles:`);
    Object.entries(skippedByProp)
      .sort((a, b) => b[1] - a[1])
      .forEach(([prop_type, count]) => {
        console.log(`  ${prop_type.padEnd(20)} — ${count}`);
      });
  }
}

// Allow CLI usage
if (
  process.argv[1].includes("backfillTrainingFieldsExtended.js") &&
  !process.argv.some((arg) => arg.startsWith("--bucket="))
) {
  const totalBuckets = 16;
  for (let i = 1; i <= totalBuckets; i++) {
    console.log(`\n⏳ Starting bucket ${i}/${totalBuckets}...\n`);
    process.argv.push(`--bucket=${i}/${totalBuckets}`);
    await runTrainingBackfillIfNeeded();
    process.argv.pop();
  }
} else {
  await runTrainingBackfillIfNeeded();
}
