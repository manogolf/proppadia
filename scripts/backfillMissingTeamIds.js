// File: scripts/backfillMissingTeamIds.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { getTeamIdFromAbbr } from "../shared/teamNameMap.js";

const BATCH_SIZE = 5000;

async function backfillMissingTeamIds() {
  console.log("🚀 Starting backfill of missing team_id values...");

  let offset = 0;
  let totalUpdated = 0;
  let batchNum = 1;

  while (true) {
    console.log(`📦 Processing batch ${batchNum}, offset ${offset}...`);

    const { data: rows, error } = await supabase
      .from("model_training_props")
      .select("id, team, player_name, game_date")
      .is("team_id", null)
      .not("team", "is", null)
      .range(offset, offset + BATCH_SIZE - 1);

    if (error) {
      console.error("❌ Error fetching rows:", error);
      break;
    }

    if (!rows || rows.length === 0) {
      console.log("✅ No more rows to update. Backfill complete.");
      break;
    }

    for (const row of rows) {
      const { id, team, player_name, game_date } = row;
      const teamId = getTeamIdFromAbbr(team);

      if (!teamId) {
        console.warn(`⚠️ ${id}: Unknown team abbr '${team}' — skipping`);
        continue;
      }

      const { error: updateError } = await supabase
        .from("model_training_props")
        .update({ team_id: teamId })
        .eq("id", id);

      if (updateError) {
        console.error(`❌ Failed to update ${id}:`, updateError.message);
      } else {
        console.log(
          `✅ Updated ${id} (${player_name} on ${game_date}) → team_id = ${teamId}`
        );
        totalUpdated++;
      }
    }

    offset += BATCH_SIZE;
    batchNum++;
  }

  console.log(`🎯 Backfill complete. Total rows updated: ${totalUpdated}`);
}

backfillMissingTeamIds();
