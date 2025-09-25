// File: scripts/backfillOpponentPlayerIDs.js

import { supabase } from "../../backend/scripts/shared/supabaseBackend.js";
import { getBoxscoreFromGameID } from "../../backend/scripts/shared/mlbApiUtils.js";

async function fetchRecentProps(days = 30) {
  const cutoff = new Date(Date.now() - days * 86400000)
    .toISOString()
    .split("T")[0];

  const { data, error } = await supabase
    .from("model_training_props")
    .select("id, player_id, team, is_home, game_id, game_date")
    .gte("game_date", cutoff)
    .is("opponent_player_id", null); // Only where not yet filled

  if (error) throw new Error("❌ Failed to fetch props: " + error.message);
  return data;
}

function getStartingPitcher(boxscore, isHomeTeam) {
  const side = isHomeTeam ? "away" : "home"; // opponent's side
  const pitchers = boxscore?.teams?.[side]?.players || {};

  for (const pid in pitchers) {
    const player = pitchers[pid];
    if (player?.stats?.pitching?.gamesStarted === 1) {
      return player?.person?.id;
    }
  }
  return null;
}

async function run() {
  const props = await fetchRecentProps();
  console.log(`🔍 Found ${props.length} rows to backfill`);

  for (const prop of props) {
    const { id, game_id, is_home } = prop;
    const boxscore = await getBoxscoreFromGameID(game_id);

    if (!boxscore) {
      console.warn(`⚠️ No boxscore for game ${game_id}`);
      continue;
    }

    const opponentPitcherId = getStartingPitcher(boxscore, is_home);

    if (!opponentPitcherId) {
      console.warn(`⚠️ No starter found for game ${game_id}`);
      continue;
    }

    const { error } = await supabase
      .from("model_training_props")
      .update({ opponent_player_id: opponentPitcherId })
      .eq("id", id);

    if (error) {
      console.error(`❌ Failed to update row ${id}: ${error.message}`);
    } else {
      console.log(`✅ Updated ${id} with pitcher ${opponentPitcherId}`);
    }
  }

  console.log("🎉 Done.");
}

run();
