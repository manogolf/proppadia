// scripts/backfillPositionsFromBoxscore.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { getPlayerStatsFromBoxscore } from "../backend/scripts/shared/playerUtils.js";

const BATCH_SIZE = 500;
const CONCURRENCY = 4;

async function fetchNextBatch() {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("id, player_id, game_id")
    .is("position", null)
    .is("position_backfill_status", null)
    .not("game_id", "is", null)
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ Failed to fetch batch:", error.message);
    return [];
  }

  const ids = data.map((row) => row.id);
  if (ids.length > 0) {
    const { error: claimError } = await supabase
      .from("model_training_props")
      .update({ position_backfill_status: "in_progress" })
      .in("id", ids);

    if (claimError) {
      console.error(
        "❌ Failed to mark rows as in_progress:",
        claimError.message
      );
      return [];
    }
  }

  return data;
}

async function processRow(row) {
  const { id, player_id, game_id } = row;
  if (!player_id || !game_id) return null;

  try {
    const stats = await getPlayerStatsFromBoxscore({ game_id, player_id });
    const position = stats?.position?.abbreviation || stats?.position || null;

    if (!position) {
      console.warn(`⚠️ No position for player ${player_id}, game ${game_id}`);
      return null;
    }

    const { error } = await supabase
      .from("model_training_props")
      .update({ position, position_backfill_status: "done" })
      .eq("id", id);

    if (error) {
      console.error(`❌ Failed to update row ${id}:`, error.message);
      return null;
    }

    console.log(`✅ Updated ${id} → ${position}`);
    return true;
  } catch (err) {
    console.error(`💥 Error in processRow for ${id}:`, err.message);
    return null;
  }
}

async function workerLoop(workerId) {
  let updatedCount = 0;

  while (true) {
    const batch = await fetchNextBatch();
    if (!batch.length) {
      console.log(`🛑 Worker ${workerId} complete (no more rows)`);
      break;
    }

    console.log(`🔄 Worker ${workerId} processing ${batch.length} rows`);

    for (const row of batch) {
      const updated = await processRow(row);
      if (updated) updatedCount++;
    }
  }

  return updatedCount;
}

async function runConcurrent() {
  console.log("🚀 Starting concurrent position backfill from boxscore");

  const results = await Promise.all(
    Array.from({ length: CONCURRENCY }, (_, i) => workerLoop(i + 1))
  );

  const totalUpdated = results.reduce((sum, n) => sum + n, 0);
  console.log(`🎉 Done! Total positions updated: ${totalUpdated}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runConcurrent();
}
