import "dotenv/config";
import { supabase } from "../utils/supabaseBackend.js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function calculateWasCorrect(row) {
  if (row.outcome === "push") return null; // Pushes aren't wins or losses

  const result = parseFloat(row.result);
  const line = parseFloat(row.prop_value);
  const predicted = row.predicted_outcome;
  const overUnder = row.over_under;

  if (isNaN(result) || isNaN(line) || !predicted || !overUnder) return null; // Incomplete data

  const actualOutcome = result > line ? "win" : result < line ? "loss" : "push";

  return predicted === actualOutcome;
}

async function backfillWasCorrect() {
  console.log("🔍 Fetching resolved props missing `was_correct`...");

  const { data, error } = await supabase
    .from("player_props")
    .select(
      "id, predicted_outcome, outcome, over_under, result, prop_value, was_correct, status"
    )
    .eq("status", "resolved")
    .is("was_correct", null)
    .not("predicted_outcome", "is", null);

  if (error) {
    console.error("❌ Failed to fetch props:", error.message);
    return;
  }

  console.log(`📦 Found ${data.length} props to backfill...`);

  for (const row of data) {
    const calculated = calculateWasCorrect(row);

    const { error: updateError } = await supabase
      .from("player_props")
      .update({ was_correct: calculated })
      .eq("id", row.id);

    if (updateError) {
      console.error(`❌ Failed to update prop ${row.id}:`, updateError.message);
    } else {
      console.log(`✅ Backfilled was_correct for prop ${row.id}`);
    }
  }

  console.log("🏁 Backfill complete.");
}

backfillWasCorrect();
