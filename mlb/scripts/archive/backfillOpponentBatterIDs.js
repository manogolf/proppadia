// File: scripts/backfillOpponentBatterIDs.js
import { supabase } from "../../backend/scripts/shared/supabaseBackend.js";
import { getBoxscoreFromGameID } from "../../backend/scripts/shared/mlbApiUtils.js";
import { toISODate } from "../../backend/scripts/shared/timeUtils.js";

async function fetchRowsToBackfill(limit = 1000) {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("id, player_id, game_id, opponent")
    .is("opposing_batter_id", null)
    .not("player_id", "is", null)
    .not("game_id", "is", null)
    .not("opponent", "is", null)
    .neq("player_id", "") // safety
    .limit(limit);

  if (error) throw new Error("❌ Failed to fetch rows: " + error.message);
  return data;
}

function findFirstBatterID(boxscore, opponentAbbr) {
  const teamSide =
    boxscore.teams.home.team.abbreviation === opponentAbbr ? "home" : "away";
  const players = boxscore.teams[teamSide]?.players || {};

  // Attempt to find first batter in batting order (slot 1)
  const sorted = Object.values(players)
    .filter((p) => p.battingOrder)
    .sort((a, b) => Number(a.battingOrder) - Number(b.battingOrder));

  return sorted[0]?.person?.id || null;
}

async function backfill() {
  const rows = await fetchRowsToBackfill();
  console.log(`🔍 Found ${rows.length} pitcher rows to backfill`);

  for (const row of rows) {
    const { id, game_id, opponent } = row;

    const boxscore = await getBoxscoreFromGameID(game_id);
    if (!boxscore) {
      console.warn(`⚠️ Skipping game ${game_id}, no boxscore`);
      continue;
    }

    const batterId = findFirstBatterID(boxscore, opponent);
    if (!batterId) {
      console.warn(`⚠️ No opposing batter found for game ${game_id}`);
      continue;
    }

    const { error } = await supabase
      .from("model_training_props")
      .update({ opposing_batter_id: batterId })
      .eq("id", id);

    if (error) {
      console.error(`❌ Failed to update ${id}:`, error.message);
    } else {
      console.log(`✅ Updated ${id} with batter ${batterId}`);
    }
  }

  console.log("🎉 Done.");
}

backfill();
