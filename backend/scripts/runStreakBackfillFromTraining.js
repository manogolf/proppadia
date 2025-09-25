// backend/scripts/runStreakBackfillFromTraining.js
import { supabase } from "./shared/supabaseBackend.js";
import { normalizePropType } from "./shared/propUtilsBackend.js";
import { toISODate } from "./shared/timeUtilsBackend.js";

// 🔁 Recompute streaks from existing resolved training data (no need for raw MLB stats)

function mapOutcomeToStreakType(outcome) {
  if (outcome === "win") return "hot";
  if (outcome === "loss") return "cold";
  return "neutral"; // fallback if needed
}

function computeStreaks(props) {
  const streaks = [];
  let currentStreak = 0;
  let currentType = null;

  for (let i = 0; i < props.length; i++) {
    const outcome = props[i].outcome;

    if (!["win", "loss"].includes(outcome)) continue;

    if (i === 0 || outcome === currentType) {
      currentStreak += 1;
      currentType = outcome;
    } else {
      currentStreak = 1;
      currentType = outcome;
    }
  }

  const last = props[0];
  if (last && ["win", "loss"].includes(currentType) && currentStreak > 0) {
    streaks.push({
      player_id: last.player_id,
      prop_type: normalizePropType(last.prop_type),
      prop_source: last.prop_source,
      streak_type: mapOutcomeToStreakType(currentType),
      streak_count: currentStreak,
    });
  }

  return streaks;
}

async function upsertStreaks(streaks) {
  if (!streaks.length) return;
  const { error } = await supabase
    .from("player_streak_profiles")
    .upsert(streaks, {
      onConflict: "player_id,prop_type,prop_source",
    });
  if (error) console.error("❌ Error upserting streaks:", error.message);
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

  console.log(
    `📦 Processing ${uniqueKeys.length} unique (player, prop_type, source) combos...`
  );

  for (let i = 0; i < uniqueKeys.length; i++) {
    const [player_id, prop_type, prop_source] = uniqueKeys[i].split("__");

    const { data: props, error } = await supabase
      .from("model_training_props")
      .select("player_id, prop_type, outcome, game_date, prop_source")
      .eq("player_id", player_id)
      .eq("prop_type", prop_type)
      .eq("prop_source", prop_source)
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
      `(${i + 1}/${uniqueKeys.length}) Player ${player_id} — ${
        props.length
      } props → ${streaks.length} streaks`
    );
  }

  console.log(`\n📊 Total props processed: ${totalProcessed}`);
  console.log(`📥 Total streaks upserted: ${totalUpserted}`);
  console.log("🎉 Done.");
}

main();
