import { supabase } from "../utils/supabaseBackend.js";
import fetch from "node-fetch";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/[\.\,]/g, "")
    .trim();
}

async function fetchPlayerID(playerName, gameId) {
  const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;
  const res = await fetch(url);
  const data = await res.json();
  const allPlayers = {
    ...data.teams.home.players,
    ...data.teams.away.players,
  };
  const normalizedTarget = normalizeName(playerName);

  for (const playerKey in allPlayers) {
    const player = allPlayers[playerKey];
    const fullName = player?.person?.fullName || "";
    if (normalizeName(fullName) === normalizedTarget) {
      return player.person.id;
    }
  }

  return null;
}

async function run() {
  console.log("🔍 Fetching props missing player_id...");
  const { data, error } = await supabase
    .from("player_props")
    .select("id, player_name, game_id")
    .is("player_id", null)
    .not("game_id", "is", null);

  if (error) {
    console.error("❌ Failed to fetch props:", error.message);
    return;
  }

  console.log(`📦 Found ${data.length} to backfill`);
  let filled = 0;

  for (const prop of data) {
    const playerId = await fetchPlayerID(prop.player_name, prop.game_id);
    if (!playerId) {
      console.warn(`⚠️ No ID for ${prop.player_name} (game ${prop.game_id})`);
      continue;
    }

    const { error: updateError } = await supabase
      .from("player_props")
      .update({ player_id: playerId })
      .eq("id", prop.id);

    if (updateError) {
      console.error(`❌ Failed to update ${prop.id}:`, updateError.message);
    } else {
      console.log(`✅ Updated ${prop.player_name} → ${playerId}`);
      filled++;
    }
  }

  console.log(`🏁 Done. Updated ${filled} of ${data.length}`);
}

run();
