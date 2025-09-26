import "dotenv/config";
import { supabase } from "../shared/index.js";
import { trainFromData } from "./trainer.js"; // ⬅️ adjust path as needed

async function fetchTrainingData() {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("*")
    .gte("game_date", "2025-05-01")
    .in("status", ["win", "loss", "push"])
    .not("player_id", "is", null)
    .not("predicted_outcome", "is", null);

  if (error) {
    console.error("❌ Error fetching training data:", error.message);
    process.exit(1);
  }

  console.log(`📦 Fetched ${data.length} rows. Passing to trainer...`);
  await trainFromData(data, { label: "May2025", saveModel: true });
}

fetchTrainingData()
  .then(() => console.log("✅ Model training complete."))
  .catch((err) => {
    console.error("🔥 Training failed:", err.message);
    process.exit(1);
  });
