import { supabase } from "../scripts/shared/supabaseBackend.js";
import { upsertPlayerID } from "../scripts/shared/upsertPlayerID.js";
import { enrichGameContext } from "../../shared/enrichGameContext.js";

export default async function preparePropSubmission({
  playerName,
  teamAbbr,
  propType,
  line,
  overUnder,
  gameDate,
  game_id,
}) {
  console.log("🛠️ preparePropSubmission called with:", {
    playerName,
    teamAbbr,
    propType,
    line,
    overUnder,
    gameDate,
    game_id,
  });

  // ✅ 1. Try to fetch player_id from mlb.model_training_props
  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id")
    .eq("player_name", playerName)
    .eq("team", teamAbbr)
    .limit(1);

  if (!data?.length) {
    console.warn(`❌ Could not find player_id for ${playerName} (${teamAbbr})`);
    return {
      error: "Could not resolve player ID. Please check player name and team.",
    };
  }

  const player_id = data[0].player_id;

  // ✅ Upsert to player_ids
  await upsertPlayerID(supabase, {
    player_name: playerName,
    player_id,
    team: teamAbbr,
  });

  // 🌐 Game context
  const context = await enrichGameContext({ team: teamAbbr, gameDate });

  // 📦 Final prepared object
  const prepared = {
    player_name: playerName,
    team: teamAbbr,
    prop_type: propType,
    prop_value: parseFloat(line),
    over_under: overUnder.toLowerCase(),
    game_date: gameDate,
    game_id,
    player_id,
    ...context,
  };

  console.log("📦 Prepared prop submission:", prepared);
  return prepared;
}
