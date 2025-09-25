// scripts/backfillPlayerPositions.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";

const BATCH_SIZE = 2000;
const TWO_DAYS_AGO = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000)
  .toISOString()
  .slice(0, 10); // YYYY-MM-DD

async function loadPositionMap() {
  console.log("📥 Loading known positions from player_stats...");
  const { data, error } = await supabase
    .from("player_stats")
    .select("player_id, position")
    .not("position", "is", null);

  if (error) {
    console.error("❌ Failed to load player_stats:", error.message);
    return {};
  }

  const map = {};
  for (const row of data) {
    map[row.player_id] = row.position; // FIXED: position is already a string
  }

  console.log(`✅ Loaded ${Object.keys(map).length} unique player positions`);
  return map;
}

async function run() {
  const positionMap = await loadPositionMap();
  if (!Object.keys(positionMap).length) return;

  console.log("📤 Fetching model_training_props with missing positions...");
  const { data: rows, error } = await supabase
    .from("model_training_props")
    .select("id, player_id")
    .is("position", null)
    .gte("game_date", TWO_DAYS_AGO); // LIMIT TO LAST 2 DAYS

  if (error) {
    console.error("❌ Failed to fetch model_training_props:", error.message);
    return;
  }

  const updates = [];

  for (const row of rows) {
    const position = positionMap[row.player_id];
    if (position) {
      updates.push({ id: row.id, position });
    }
  }

  console.log(`📦 Prepared ${updates.length} rows for update`);

  let totalUpdated = 0;

  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from("model_training_props")
      .upsert(batch, { onConflict: "id" });

    if (error) {
      console.error(`❌ Failed to upsert batch at index ${i}:`, error.message);
    } else {
      totalUpdated += batch.length;
      console.log(
        `✅ Batch ${i / BATCH_SIZE + 1}: Updated ${batch.length} rows`
      );
    }
  }

  console.log(`🎉 Done! Total positions updated: ${totalUpdated}`);
}

export default run;

if (import.meta.url === `file://${process.argv[1]}`) {
  run(); // Run only if executed directly
}
