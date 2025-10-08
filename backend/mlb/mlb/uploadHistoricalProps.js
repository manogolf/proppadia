import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { supabase } from "../utils/supabaseUtils.js";
import "dotenv/config";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("❌ Supabase environment variables are not loaded.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

async function uploadHistoricalProps(csvFilePath) {
  const rows = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on("data", (data) => {
      rows.push(data);
    })
    .on("end", async () => {
      console.log(`📊 Loaded ${rows.length} rows from ${csvFilePath}.`);

      for (const row of rows) {
        const insertPayload = {
          id: row.id,
          player_name: row.player_name,
          team: row.team,
          position: row.position,
          prop_type: row.prop_type,
          prop_value: parseFloat(row.prop_value),
          result: parseFloat(row.result),
          outcome: row.outcome,
          is_pitcher: row.is_pitcher === "true",
          game_date: row.game_date,
          game_id: parseInt(row.game_id, 10),
          over_under: row.over_under,
          source: row.source || "stat-derived",
          player_id: row.player_id || null,
          rolling_result_avg_7: row.rolling_result_avg_7
            ? parseFloat(row.rolling_result_avg_7)
            : null,
          hit_streak: row.hit_streak ? parseInt(row.hit_streak, 10) : null,
          win_streak: row.win_streak ? parseInt(row.win_streak, 10) : null,
        };

        const { error } = await supabase
          .from("model_training_props")
          .upsert(insertPayload, {
            onConflict: ["player_id", "game_id", "prop_type"],
          });

        if (error) {
          console.warn(
            `⚠️ Failed to upsert row ID ${row.id}: ${error.message}`
          );
        } else {
          console.log(`✅ Inserted row ID ${row.id}`);
        }
      }

      console.log("🎉 Historical props upload complete!");
    });
}

// Usage: node uploadHistoricalProps.js path/to/file.csv
const args = process.argv.slice(2);
if (!args[0]) {
  console.error("❌ Please provide a CSV file path.");
  process.exit(1);
}

const csvFilePath = path.resolve(process.cwd(), args[0]);
uploadHistoricalProps(csvFilePath);
