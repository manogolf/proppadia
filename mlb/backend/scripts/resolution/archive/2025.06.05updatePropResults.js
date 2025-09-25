//

import "dotenv/config";
import { supabase } from "../../shared/index.js";
import {
  todayET,
  yesterdayET,
} from "../../../../src/shared/timeUtilsBackend.js";
import { expireOldPendingProps } from "../../../../src/shared/archive/propUtils.js";
import { getPendingProps } from "../../shared/supabaseBackend.js";
import { getStatFromLiveFeed } from "../getStatFromLiveFeed.js";
import { propExtractors } from "../../../../src/shared/archive/propUtils.js";
import {
  determineStatus,
  normalizePropType,
} from "../../../../src/shared/archive/propUtils.js";
import fs from "fs";

const affectedPlayerIds = new Set();

export async function updatePropStatus(prop) {
  console.log(`📡 Checking prop: ${prop.player_name} - ${prop.prop_type}`);

  if (prop.prop_value < 0) {
    console.warn(`🚫 Invalid prop line value: ${prop.prop_value} — skipping`);
    return { status: "skipped", reason: "invalid line" };
  }

  let statBlock = null;
  let statsSource = "boxscore";

  // 🔍 Try player_stats first
  const { data: playerStats, error: statsError } = await supabase
    .from("player_stats")
    .select("*")
    .eq("game_id", prop.game_id)
    .eq("player_id", prop.player_id)
    .maybeSingle();

  if (!statsError && playerStats) {
    statBlock = playerStats;
  } else {
    console.warn(`⚠️ No stats in player_stats, trying live feed...`);
    statsSource = "live";
    statBlock = await getStatFromLiveFeed(
      prop.game_id,
      prop.player_id,
      prop.prop_type
    );
  }

  console.log("📊 Stat block keys:", Object.keys(statBlock || {}));

  // 🧪 Try to extract relevant stat
  let relevantStat = null;

  // 🧪 Check statBlock validity before proceeding
  if (!statBlock || typeof statBlock !== "object") {
    console.warn(
      `🚷 DNP (no statBlock): ${prop.player_name} (${prop.prop_type})`
    );
    await supabase
      .from("player_props")
      .update({ status: "dnp" })
      .eq("id", prop.id);
    return { status: "dnp" };
  }

  // 🧼 Filter out non-stat fields
  const statKeys = Object.keys(statBlock).filter(
    (key) =>
      ![
        "player_id",
        "game_id",
        "game_date",
        "team",
        "opponent",
        "is_home",
        "position",
      ].includes(key)
  );

  // 🚫 If no stat fields remain
  if (statKeys.length === 0) {
    console.warn(
      `🚷 DNP (only meta fields): ${prop.player_name} (${prop.prop_type})`
    );
    await supabase
      .from("player_props")
      .update({ status: "dnp" })
      .eq("id", prop.id);
    return { status: "dnp" };
  }

  // ⚠️ If all stat fields are null, undefined, or 0
  const isTrulyEmpty = statKeys.every((key) => {
    const val = statBlock[key];
    return val === null || val === undefined || val === 0;
  });

  if (isTrulyEmpty) {
    console.warn(
      `🚷 DNP (all stat fields empty): ${prop.player_name} (${prop.prop_type})`
    );
    await supabase
      .from("player_props")
      .update({ status: "dnp" })
      .eq("id", prop.id);
    return { status: "dnp" };
  }

  // ✅ Extract stat if not a DNP
  const normalizedType = normalizePropType(prop.prop_type);
  const extractor = propExtractors[normalizedType];

  if (!extractor) {
    console.warn(`⚠️ Unknown propType: ${normalizedType}`);
  }

  relevantStat = extractor ? extractor(statBlock) : null;

  // 🧼 Original fallback if no stat or result
  if (
    statBlock == null ||
    relevantStat === null ||
    relevantStat === undefined
  ) {
    console.warn(
      `🚷 DNP (no stat found): ${prop.player_name} (${prop.prop_type})`
    );
    await supabase
      .from("player_props")
      .update({ status: "dnp" })
      .eq("id", prop.id);
    return { status: "dnp" };
  }

  // ✅ Stat found — extract and evaluate
  prop.result = relevantStat;
  console.log(
    `🧪 Extracted result for ${prop.player_name} (${prop.prop_type}): ${prop.result}`
  );

  const outcome = determineStatus(
    prop.result,
    prop.prop_value,
    prop.over_under
  );

  console.log(
    `🎯 Outcome (${statsSource}): ${prop.result} vs ${prop.prop_value} (${prop.over_under}) → ${outcome}`
  );

  const { error: updateError } = await supabase
    .from("player_props")
    .update({
      result: prop.result,
      outcome,
      status: outcome,
      was_correct: prop.predicted_outcome
        ? outcome === prop.predicted_outcome
        : null,
    })
    .eq("id", prop.id);

  if (updateError) {
    console.error(
      `❌ Supabase update failed for ${prop.player_name} (ID: ${prop.id}): ${updateError.message}`
    );
    return { status: "error" };
  } else {
    affectedPlayerIds.add(prop.player_id); // ✅ track affected
    console.log(
      `✅ Updated prop ${prop.id} (${prop.player_name}) → ${outcome}`
    );
    return { status: "updated" };
  }
}

export async function updatePropStatuses() {
  const props = await getPendingProps();
  console.log(`🔎 Found ${props.length} pending props.`);

  let updated = 0,
    skipped = 0,
    dnps = 0,
    errors = 0;

  const skippedProps = [];

  for (const prop of props) {
    try {
      const result = await updatePropStatus(prop);
      switch (result.status) {
        case "updated":
          updated++;
          break;
        case "dnp":
          dnps++;
          break;
        case "skipped":
          skipped++;
          skippedProps.push({ ...prop, reason: result.reason });
          break;
        case "error":
          errors++;
          break;
      }
    } catch (err) {
      console.error(`🔥 Error processing ${prop.player_name}:`, err.message);
      errors++;
    }
  }

  if (skippedProps.length > 0) {
    fs.writeFileSync(
      "./skipped_props.json",
      JSON.stringify(skippedProps, null, 2)
    );
  }

  await expireOldPendingProps();

  console.log(
    `🏁 Update Summary → ✅ Updated: ${updated} | ⏭️ Skipped: ${skipped} | 🚷 DNP: ${dnps} | ❌ Errors: ${errors}`
  );

  // ✅ Invalidate cache for affected players
  if (affectedPlayerIds.size > 0) {
    const { error: cacheError } = await supabase
      .from("player_profiles_cache")
      .delete()
      .in("player_id", Array.from(affectedPlayerIds));

    if (cacheError) {
      console.warn("⚠️ Failed to clear player cache:", cacheError.message);
    } else {
      console.log(`🧹 Cleared cache for ${affectedPlayerIds.size} players`);
    }
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    try {
      await updatePropStatuses();
      console.log("✅ Finished running updatePropStatuses");
    } catch (err) {
      console.error("🔥 Fatal error in updatePropStatuses:", err);
      process.exit(1);
    }
  })();
}
