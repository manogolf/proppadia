// scripts/backfillPlayerIdsTeamId.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { getTeamIdFromAbbr } from "../shared/teamNameMap.js";

const BATCH_SIZE = 5000;

async function backfillTeamIdsInPlayerIds() {
  console.log("🔍 Looking for player_ids missing team_id...");

  const { data: rows, error } = await supabase
    .from("player_ids")
    .select("id, team, player_name, player_id")
    .is("team_id", null)
    .not("team", "is", null)
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ Failed to fetch rows:", error);
    return;
  }

  console.log(`🛠 Found ${rows.length} rows to process...`);

  for (const row of rows) {
    const teamId = getTeamIdFromAbbr(row.team);
    if (!teamId) {
      console.warn(`⚠️ No team_id found for ${row.player_name} (${row.team})`);
      continue;
    }

    const { error: updateError } = await supabase
      .from("player_ids")
      .update({ team_id: teamId })
      .eq("id", row.id);

    if (updateError) {
      console.error(`❌ Failed to update ${row.player_name}:`, updateError);
    } else {
      console.log(
        `✅ Updated ${row.player_name} (${row.team}) → team_id = ${teamId}`
      );
    }
  }

  console.log("🎯 Done updating player_ids.team_id");
}

backfillTeamIdsInPlayerIds();
