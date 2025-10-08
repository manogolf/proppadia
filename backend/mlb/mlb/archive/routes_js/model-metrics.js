// backend/app/routes/api/model-metrics.js

import { supabase } from "../../../backend/scripts/shared/supabaseBackend.js";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { data, error } = await supabase.rpc("get_model_accuracy_metrics");

  if (error) {
    console.error("❌ Failed to fetch metrics:", error.message);
    return res.status(500).json({ error: "Failed to load metrics" });
  }

  res.status(200).json(data);
}
