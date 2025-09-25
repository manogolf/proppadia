/**
 * 📄 File: scripts/backfillGameContextFields.js
 *
 * Populates missing contextual game metadata in the `model_training_props` table.
 *
 * Fills in:
 * - opponent, opponent_encoded
 * - home_away, is_home
 * - game_time, game_day_of_week, time_of_day_bucket
 *
 * ⚙️ Safe and cron-ready:
 * - Skips rows with missing team or unresolved game context
 * - Limits noisy logging
 * - Summarizes results at batch and job level
 */

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { getGameContextFields } from "../backend/scripts/shared/mlbApiUtils.js";

const LOOKBACK_DAYS = 2;

const BATCH_SIZE = 1000;
const CONCURRENCY = 1;

let totalUpdated = 0;
let totalSkipped = 0;
let totalErrors = 0;

async function fetchNextBatch() {
  const cutoffDate = new Date(Date.now() - LOOKBACK_DAYS * 86400000)
    .toISOString()
    .split("T")[0]; // YYYY-MM-DD

  const { data, error } = await supabase
    .from("model_training_props")
    .select("id, game_id, game_date, team, is_home")
    .gte("game_date", cutoffDate)
    .or(
      [
        "game_time.is.null",
        "time_of_day_bucket.is.null",
        "game_day_of_week.is.null",
        "is_home.is.null",
        "home_away.is.null",
        "opponent_encoded.is.null",
      ].join(",")
    )
    .order("id", { ascending: true })
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ Failed to fetch batch:", error.message);
    return null; // distinguish error from empty result
  }

  return data;
}

async function processRow(row) {
  const { id, game_id, team } = row;
  let { is_home } = row;

  try {
    console.log(`🔍 Processing row ${id} (game_id=${game_id}, team=${team})`);

    if (!team || !game_id) {
      return { skipped: true };
    }

    const context = await getGameContextFields(game_id, team, is_home);

    if (!context || !context.opponent_encoded) {
      console.warn(`⚠️ Skipping row ${id}: missing opponent_encoded`);
      return { skipped: true };
    }

    const {
      home_away,
      opponent,
      opponent_encoded,
      game_time,
      game_day_of_week,
      time_of_day_bucket,
      is_home: resolvedIsHome,
    } = context;

    is_home = resolvedIsHome;

    const updates = {
      is_home,
      home_away,
      opponent,
      opponent_encoded,
      game_time,
      game_day_of_week,
      time_of_day_bucket,
    };

    const cleaned = Object.fromEntries(
      Object.entries(updates).filter(([_, v]) => v !== undefined && v !== null)
    );

    if (Object.keys(cleaned).length === 0) {
      return { skipped: true };
    }

    const { error } = await supabase
      .from("model_training_props")
      .update(cleaned)
      .eq("id", id);

    if (error) {
      console.error(`❌ Supabase update error for row ${id}:`, error);
      return { error: true };
    }

    return { updated: true };
  } catch (err) {
    console.error(`❌ Unexpected error in row ${id}:`, err);
    return { error: true };
  }
}

async function runConcurrent() {
  console.log("🚀 Starting game context backfill...");

  let didWork = false; // track if *any* row was processed

  const workers = Array(CONCURRENCY)
    .fill(null)
    .map(async () => {
      while (true) {
        const batch = await fetchNextBatch();
        if (batch === null) {
          totalErrors++;
          break; // stop on fetch failure
        }
        if (!batch.length) {
          if (!didWork) {
            console.log(
              "📭 No rows found matching criteria — nothing to backfill."
            );
          }
          break;
        }

        didWork = true;

        let updated = 0;
        let skipped = 0;
        let errors = 0;

        for (const row of batch) {
          try {
            const result = await processRow(row);
            if (result?.updated) updated++;
            else if (result?.skipped) skipped++;
            else if (result?.error) errors++;
          } catch (err) {
            errors++;
          }
        }

        totalUpdated += updated;
        totalSkipped += skipped;
        totalErrors += errors;

        console.log(
          `📦 Batch complete: ✅ ${updated} updated | ⏭️ ${skipped} skipped | ❌ ${errors} errors`
        );
      }
    });

  await Promise.all(workers);

  console.log("\n🎉 Game context backfill complete.");
  console.log(`   ✅ Total updated: ${totalUpdated}`);
  console.log(`   ⏭️ Total skipped: ${totalSkipped}`);
  console.log(`   ❌ Total errors: ${totalErrors}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runConcurrent();
}

export { runConcurrent };
