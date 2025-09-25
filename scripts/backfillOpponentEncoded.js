// scripts/backfillOpponentEncoded.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { getTeamIdFromAbbr } from "../backend/scripts/shared/teamNameMap.js";

const BATCH_SIZE = 1000;

async function fetchRowsToUpdate() {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("id, opponent")
    .not("opponent", "is", null)
    .is("opponent_encoded", null)
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ Failed to fetch rows:", error.message);
    return [];
  }

  return data;
}

async function updateOpponentEncoded(batch) {
  const updates = batch
    .map(({ id, opponent }) => {
      const encoded = getTeamIdFromAbbr(opponent);
      if (encoded == null) {
        console.warn(`⚠️ Unknown team abbreviation: ${opponent}`);
        return null;
      }
      return { id, opponent_encoded: encoded };
    })
    .filter(Boolean);

  if (updates.length === 0) return 0;

  const { error } = await supabase
    .from("model_training_props")
    .upsert(updates, { onConflict: "id" });

  if (error) {
    console.error("❌ Failed to upsert updates:", error.message);
    return 0;
  }

  return updates.length;
}

async function run() {
  let totalUpdated = 0;

  while (true) {
    const batch = await fetchRowsToUpdate();
    if (!batch.length) break;

    const updated = await updateOpponentEncoded(batch);
    totalUpdated += updated;

    console.log(`✅ Updated ${updated} rows (total: ${totalUpdated})`);

    if (batch.length < BATCH_SIZE) break;
  }

  console.log(`🎉 Done. Total rows updated: ${totalUpdated}`);
}

run();
