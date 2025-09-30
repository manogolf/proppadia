// scripts/resolveHitsOutcomes.js
import { supabase } from "../../backend/scripts/shared/supabaseBackend.js";

const BATCH_SIZE = 500;

function determineOutcome(result, line, overUnder) {
  if (result === null || line === null || !overUnder) return null;

  if (result === line) return "push";
  if (overUnder.toLowerCase() === "over") {
    return result > line ? "win" : "loss";
  } else {
    return result < line ? "win" : "loss";
  }
}

async function resolveHitsOutcomes() {
  let offset = 0;
  let totalUpdated = 0;

  while (true) {
    const { data: rows, error } = await supabase
      .from("model_training_props")
      .select("id, prop_value, result, over_under")
      .eq("prop_type", "hits")
      .eq("prop_source", "mlb_api")
      .is("outcome", null)
      .range(offset, offset + BATCH_SIZE - 1);

    if (error) {
      console.error("❌ Fetch error:", error.message);
      break;
    }

    if (!rows.length) {
      console.log("✅ No more rows to process.");
      break;
    }

    for (const row of rows) {
      const { id, prop_value, result, over_under } = row;

      const resolved = determineOutcome(result, prop_value, over_under);
      if (!resolved) {
        console.warn(`⚠️ Skipping row ${id}: cannot determine outcome.`);
        continue;
      }

      const { error: updateError } = await supabase
        .from("model_training_props")
        .update({ outcome: resolved })
        .eq("id", id);

      if (updateError) {
        console.error(`❌ Failed to update row ${id}:`, updateError.message);
      } else {
        totalUpdated++;
        console.log(`✅ Resolved row ${id}: ${resolved}`);
      }
    }

    offset += BATCH_SIZE;
  }

  console.log(`🎯 Completed. Total outcomes resolved: ${totalUpdated}`);
}

resolveHitsOutcomes();
