/**
 * 📄 File: backend/scripts/resolution/updatePropResults.js
 * 🧠 Purpose: Grade and resolve player props from stats.
 *
 * ⚠️ Assumptions / Pre-conditions:
 * - Game is final (status === 'F') — must be verified *before* calling this.
 * - Player ID and prop_type are already normalized.
 * - Stats will be pulled via resolveStatForPlayer().
 *
 * ❌ This file does NOT:
 * - Check game status from MLB API or local cache.
 * - Generate or validate player streaks.
 * - Re-fetch updated lines or predictions.
 *
 * ✅ Responsibilities:
 * - Validate player participation (DNP logic).
 * - Compute outcome: win/loss/push via determineStatus().
 * - Update Supabase with final result, outcome, and was_correct.
 */
// ==========================================
// 📄 File: updatePropResults.js
// 📌 Purpose: Resolve outcomes for all finalized props (user-added and stat-derived).
//
// 🔁 Used by: cronRunner.js (daily run)
// 📥 Sources:
//   - player_props → user-added props
//   - model_training_props → stat-derived props
//   - MLB Stats API → live boxscores and allPlays data
//   - player_stats (fallback)
//
// 🧠 Why it exists:
// - Grades props as win/loss/push based on actual player performance.
// - Computes result values for props using boxscore data or cached player_stats.
// - Handles DNPs (did not play), participation checks, and edge cases.
//
// 🔧 Notes:
// - Writes status and result updates directly to Supabase.
// - Skipped/incomplete rows are logged for future inspection.
// - Fully integrates with shared utilities (e.g., playerUtils, derivePropValue).
// ==========================================

// 📄 File: backend/scripts/resolution/updatePropResults.js
// 🧠 Purpose: Grade and resolve player props from stats (user-added and stat-derived).

import fs from "fs";
import { supabase } from "../shared/supabaseBackend.js";
import {
  expireOldPendingProps,
  determineStatus,
} from "../shared/propUtilsBackend.js";
import { didPlayerParticipate } from "../shared/playerUtilsBackend.js";
import { getPendingProps } from "../shared/supabaseBackend.js";
import { resolveStatForPlayer, hasMeaningfulStats } from "./statResolvers.js";
import {
  isGameFinal,
  fetchGameStatusById,
} from "../../../src/shared/gameStatusUtils.js";

// ───── Core Resolver ─────
export async function updatePropStatus(prop) {
  if (process.env.VERBOSE === "1") {
    console.log(`📡 Checking prop: ${prop.player_name} - ${prop.prop_type}`);
  }

  const gameStatus = await fetchGameStatusById(prop.game_id);
  if (!isGameFinal(gameStatus)) {
    console.log(`⏳ Skipping unresolved game (status: ${gameStatus})`);
    return { status: "skipped", reason: "game not final" };
  }

  if (prop.prop_value < 0) {
    console.warn(`🚫 Invalid line value: ${prop.prop_value}`);
    return { status: "skipped", reason: "invalid line" };
  }

  const { result, rawStats } = await resolveStatForPlayer({
    player_id: prop.player_id,
    player_name: prop.player_name,
    team: prop.team,
    game_id: prop.game_id,
    prop_type: prop.prop_type,
  });

  if (!rawStats || typeof rawStats !== "object") {
    await markDNP(prop.id, "missing rawStats");
    return { status: "dnp", reason: "missing rawStats" };
  }

  if (typeof result !== "number" || isNaN(result)) {
    await markDNP(prop.id, "invalid result type");
    return { status: "dnp", reason: "invalid result type" };
  }

  const isValid = hasMeaningfulStats(rawStats);
  const didPlay = didPlayerParticipate(rawStats);

  if (!isValid || !didPlay || result == null) {
    const reason = !didPlay
      ? "no participation"
      : !isValid
      ? "invalid stats"
      : "no result";
    await markDNP(prop.id, reason);
    return { status: "dnp", reason };
  }

  const outcome = determineStatus(result, prop.prop_value, prop.over_under);
  const was_correct =
    prop.predicted_outcome != null ? outcome === prop.predicted_outcome : null;

  const { error: updateError } = await supabase
    .from("player_props")
    .update({
      result,
      outcome,
      status: outcome,
      was_correct,
    })
    .eq("id", prop.id);

  if (updateError) {
    console.error(`❌ Supabase update failed:`, updateError.message);
    return { status: "error" };
  }

  return { status: "updated", player_id: prop.player_id };
}

async function markDNP(id, reason) {
  await supabase.from("player_props").update({ status: "dnp" }).eq("id", id);
  console.warn(`🚷 Marked DNP: ${reason}`);
}

// ───── Shared Batch Processor ─────
export async function updatePropsBatch(props, options = {}) {
  const {
    writeLogs = false,
    clearCache = true,
    summaryLabel = "Batch",
  } = options;

  const resultsLog = [];
  const skippedProps = [];
  const affectedPlayerIds = new Set();

  let updated = 0,
    dnp = 0,
    skipped = 0,
    error = 0;

  for (const prop of props) {
    try {
      const result = await updatePropStatus(prop);
      resultsLog.push({
        ...prop,
        ...result,
        timestamp: new Date().toISOString(),
      });

      if (result.status === "updated") {
        updated++;
        affectedPlayerIds.add(result.player_id);
      } else if (result.status === "dnp") dnp++;
      else if (result.status === "skipped") {
        skipped++;
        skippedProps.push({ ...prop, reason: result.reason });
      } else error++;
    } catch (err) {
      console.error(`🔥 Error processing ${prop.player_name}:`, err.message);
      error++;
    }
  }

  console.log(
    `🏁 ${summaryLabel} Summary → ✅ ${updated} | 🚷 ${dnp} | ⏭️ ${skipped} | ❌ ${error}`
  );

  if (writeLogs) {
    const timestamp = Date.now();
    fs.writeFileSync(
      `./prop_results_log_${timestamp}.json`,
      JSON.stringify(resultsLog, null, 2)
    );
    if (skippedProps.length > 0) {
      fs.writeFileSync(
        "./skipped_props.json",
        JSON.stringify(skippedProps, null, 2)
      );
    }
  }

  if (clearCache && affectedPlayerIds.size > 0) {
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

// ───── Backward-Compatible Entrypoints ─────
export async function updatePropStatuses() {
  const props = await getPendingProps();
  await updatePropsBatch(props, {
    writeLogs: true,
    clearCache: true,
    summaryLabel: "Main Update",
  });
}

export async function updatePropStatusesForRows(props) {
  await updatePropsBatch(props, {
    writeLogs: false,
    clearCache: false,
    summaryLabel: "Custom Batch",
  });
}

// ───── CLI Entrypoint ─────
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
