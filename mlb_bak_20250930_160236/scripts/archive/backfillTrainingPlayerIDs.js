import { supabase } from "../utils/supabaseBackend.js";

async function run() {
  console.log("🔍 Finding training rows missing player_id...");

  const { data, error } = await supabase
    .from("model_training_props")
    .select("id")
    .is("player_id", null);

  if (error) {
    console.error("❌ Fetch failed:", error.message);
    return;
  }

  console.log(`📦 Found ${data.length} training rows to backfill`);

  let updated = 0;

  for (const row of data) {
    const { data: match, error: matchError } = await supabase
      .from("player_props")
      .select("player_id")
      .eq("id", row.id)
      .maybeSingle();

    if (matchError || !match?.player_id) {
      console.warn(`⚠️ No matching player_id for prop ID ${row.id}`);
      continue;
    }

    const { error: updateError } = await supabase
      .from("model_training_props")
      .update({ player_id: match.player_id })
      .eq("id", row.id);

    if (updateError) {
      console.error(`❌ Failed to update ${row.id}:`, updateError.message);
    } else {
      updated++;
      console.log(`✅ Updated ${row.id} → ${match.player_id}`);
    }
  }

  console.log(`🏁 Done. Updated ${updated} of ${data.length}`);
}

run();
