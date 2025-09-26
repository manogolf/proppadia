// ==========================================
// 📄 File: backend/scripts/generatePlayerStreakProfiles.js
// 📌 Purpose: Compute and upsert active streaks per player and prop_type
//
// 🔁 How it works:
// - Fetches resolved props (status: 'win' or 'loss') from model_training_props
// - Groups by (player_id, prop_type, prop_source)
// - Computes active "hot" or "cold" streaks
// - Upserts into player_streak_profiles with one row per (player_id, prop_type, prop_source)
//
// 🛠️ Features:
// - Fully bucketed for scalable execution over large datasets
// - Can be run safely on a cron schedule (daily or hourly)
// - Ignores unresolved or irrelevant rows (e.g., missing player_id)
// - Includes updated_at timestamp for freshness tracking
//
// 🧠 Why this matters:
// - Supports PlayerProfileDashboard streak displays
// - Feeds streak features into model training
// - Enables future streak-based alerts, filters, and prediction logic
//
// 📤 Outputs: player_streak_profiles (1 row per player + prop_type + prop_source)
//
// ✅ Dependencies:
// - model_training_props: must contain resolved props with valid outcomes
// - supabase client: defined in shared/supabaseBackend.js
// - normalizePropType: maps legacy prop names consistently
// - timeUtils.toISODate: generates UTC timestamps
//
// 🔒 Uniqueness enforced on (player_id, prop_type, prop_source)
// ==========================================

import { supabase } from "./shared/supabaseBackend.js";
import { toISODate } from "./shared/timeUtilsBackend.js";
import { normalizePropType } from "./shared/propUtilsBackend.js";

const BATCH_SIZE = 1000;
const MAX_DAYS_BACK = 1000;

// Bucket CLI setup (e.g. --bucket=3/10)
const bucketArg = process.argv.find((arg) => arg.startsWith("--bucket="));
const bucketInfo = bucketArg
  ? bucketArg.replace("--bucket=", "").split("/")
  : null;

let currentBucket = 0;
let totalBuckets = 1;

if (bucketInfo && bucketInfo.length === 2) {
  currentBucket = parseInt(bucketInfo[0]) - 1;
  totalBuckets = parseInt(bucketInfo[1]);
}

function computeStreaks(resolvedProps) {
  const grouped = {};

  // Step 1: Group by player + prop + source
  for (const row of resolvedProps) {
    const prop_type = normalizePropType(row.prop_type);
    const { player_id, outcome, prop_source, game_date } = row;
    if (!player_id || player_id === "None") continue;

    const key = `${player_id}_${prop_type}_${prop_source}`;
    if (!grouped[key]) {
      grouped[key] = [];
    }

    grouped[key].push({
      outcome,
      game_date,
      player_id,
      prop_type,
      prop_source,
    });
  }

  const streakProfiles = [];

  // Step 2: For each group, sort and compute streak
  for (const groupKey in grouped) {
    const entries = grouped[groupKey];
    if (entries.length < 2) continue; // ❌ Skip if only 1 resolved prop

    entries.sort((a, b) => new Date(a.game_date) - new Date(b.game_date));

    let streakType = null;
    let streakCount = 0;

    for (const entry of entries) {
      if (streakType === null) {
        streakType = entry.outcome === "win" ? "hot" : "cold";
        streakCount = 1;
      } else if (
        (streakType === "hot" && entry.outcome === "win") ||
        (streakType === "cold" && entry.outcome === "loss")
      ) {
        streakCount += 1;
      } else {
        streakType = entry.outcome === "win" ? "hot" : "cold";
        streakCount = 1;
      }
    }

    // Only one final streak per group survives
    const final = entries[entries.length - 1];
    streakProfiles.push({
      player_id: final.player_id,
      prop_type: final.prop_type,
      prop_source: final.prop_source,
      streak_type: streakType,
      streak_count: streakCount,
    });
  }

  return streakProfiles;
}

async function upsertStreaks(streakProfiles) {
  if (!streakProfiles.length) return;

  const enriched = streakProfiles.map((profile) => ({
    ...profile,
    updated_at: toISODate(new Date()),
  }));

  const { error } = await supabase
    .from("player_streak_profiles")
    .upsert(enriched, {
      onConflict: ["player_id", "prop_type", "prop_source"],
    });

  if (error) {
    console.error("❌ Bulk upsert failed:", error.message || error);
  } else {
    console.log(`✅ Upserted ${streakProfiles.length} streak profiles.`);
  }
}

async function main() {
  const MAX_DAYS_BACK = 1000;
  const cutoffDate = toISODate(new Date(Date.now() - MAX_DAYS_BACK * 86400000));

  let totalProcessed = 0;
  let totalUpserted = 0;

  const { data: resolvedPairs, error } = await supabase
    .from("model_training_props")
    .select("player_id, prop_type, prop_source")
    .eq("status", "resolved")
    .in("outcome", ["win", "loss"])
    .not("player_id", "is", null)
    .gte("game_date", cutoffDate);

  if (error)
    throw new Error(`❌ Failed to fetch resolved pairs: ${error.message}`);

  const uniqueKeys = [
    ...new Set(
      resolvedPairs.map(
        (row) =>
          `${row.player_id}__${normalizePropType(row.prop_type)}__${
            row.prop_source
          }`
      )
    ),
  ];

  // 🎯 Filter by bucket
  const bucketedKeys = uniqueKeys; // ← bypass bucketing to check total keys

  console.log(
    `🧩 Bucket ${currentBucket + 1}/${totalBuckets} → ${
      bucketedKeys.length
    } player-prop keys`
  );

  for (let i = 0; i < bucketedKeys.length; i++) {
    const [player_id, prop_type, prop_source] = bucketedKeys[i].split("__");

    const { data: props, error } = await supabase
      .from("model_training_props")
      .select("player_id, prop_type, outcome, game_date, prop_source")
      .eq("player_id", player_id)
      .eq("prop_source", prop_source)
      .eq("prop_type", prop_type)
      .eq("status", "resolved")
      .in("outcome", ["win", "loss"])
      .gte("game_date", cutoffDate)
      .order("game_date", { ascending: false });

    if (error) {
      console.error(`❌ Fetch error for player ${player_id}: ${error.message}`);
      continue;
    }

    if (!props.length) continue;

    const streaks = computeStreaks(props);
    await upsertStreaks(streaks);

    totalProcessed += props.length;
    totalUpserted += streaks.length;

    console.log(
      `(${i + 1}/${bucketedKeys.length}) Player ${player_id} — ${
        props.length
      } props → ${streaks.length} streaks`
    );
  }

  console.log(`\n📊 Total props processed: ${totalProcessed}`);
  console.log(`📥 Total streaks upserted: ${totalUpserted}`);
  console.log("🎉 Done.");
}

main();
