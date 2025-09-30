// src/utils/validateAndFixOutcomes.js

import { supabase } from "../utils/supabaseFrontend.js";
import { getPlayerStatsFromBoxscore } from "./shared/playerUtils.js";
import { extractStatForPropType } from "../scripts/shared/propUtils.js";

const BATCH_SIZE = 500;

function determineOutcome(result, line, overUnder) {
  if (result === null || result === undefined || line === null) return null;

  if (result === line) return "push";
  if (overUnder === "over") return result > line ? "win" : "loss";
  if (overUnder === "under") return result < line ? "win" : "loss";
  return null;
}

async function validateAndFixOutcomes() {
  let offset = 0;
  let totalCorrected = 0;

  while (true) {
    const { data: rows, error } = await supabase
      .from("model_training_props")
      .select(
        "id, player_id, game_id, prop_type, prop_value, over_under, outcome"
      )
      .not("outcome", "in.('win','loss','push')") // ✅ exclude known resolved outcomes
      .not("player_id", "is", null)
      .not("game_id", "is", null)
      .order("game_date", { ascending: true })
      .range(offset, offset + BATCH_SIZE - 1);

    if (error) {
      console.error("❌ Supabase fetch error:", error.message);
      break;
    }

    if (!rows.length) {
      console.log("✅ No more rows to process.");
      break;
    }

    for (const row of rows) {
      const {
        id,
        player_id,
        game_id,
        prop_type,
        prop_value,
        over_under,
        outcome,
      } = row;

      // ⛔ Skip synthetic player IDs
      if (typeof player_id === "string" && player_id.startsWith("synthetic")) {
        console.log(`⏩ Skipping synthetic player: ${player_id}`);
        continue;
      }

      const playerStats = await getPlayerStatsFromBoxscore(game_id, player_id);
      if (!playerStats) {
        console.warn(
          `⚠️ No stats found for player ${player_id} in game ${game_id}`
        );
        continue;
      }

      const actualValue = extractStatForPropType(prop_type, playerStats);
      if (actualValue === null || actualValue === undefined) {
        console.warn(`⚠️ Could not extract stat for prop ${id}`);
        continue;
      }

      let corrected;
      if (actualValue === prop_value) {
        corrected = "push";
      } else if (
        (over_under === "over" && actualValue > prop_value) ||
        (over_under === "under" && actualValue < prop_value)
      ) {
        corrected = "win";
      } else {
        corrected = "loss";
      }

      if (corrected !== outcome) {
        const { error: updateError } = await supabase
          .from("model_training_props")
          .update({
            outcome: corrected,
            outcome_corrected_at: new Date().toISOString(),
          })
          .eq("id", id);

        if (updateError) {
          console.error(
            `❌ Failed to update outcome for row ${id}:`,
            updateError.message
          );
        } else {
          totalCorrected++;
          console.log(
            `🔄 Outcome corrected for ${id}: ${outcome} → ${corrected}`
          );
        }
      }
    }

    offset += BATCH_SIZE;
  }

  console.log(`🎯 Completed. Total outcomes corrected: ${totalCorrected}`);
}

validateAndFixOutcomes();
