// scripts/populatePositionsFromPlayerStats.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";

const BATCH_SIZE = 2000;

async function loadPlayerPositions() {
  console.log("📥 Loading player_id → position from player_stats...");
  const { data, error } = await supabase
    .from("player_stats")
    .select("player_id, position")
    .not("position", "is", null);

  if (error) {
    console.error("❌ Failed to load player_stats:", error.message);
    return {};
  }

  const positionMap = {};
  for (const row of data) {
    if (row.player_id && row.position) {
      positionMap[row.player_id] = row.position;
    }
  }

  console.log(
    `✅ Loaded ${Object.keys(positionMap).length} unique player positions`
  );
  return positionMap;
}

async function run() {
  const positionMap = await loadPlayerPositions();
  if (!Object.keys(positionMap).length) return;

  console.log("📤 Fetching model_training_props with missing positions...");
  const { data: rows, error } = await supabase
    .from("model_training_props")
    .select("id, player_id")
    .is("position", null);

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

run();
