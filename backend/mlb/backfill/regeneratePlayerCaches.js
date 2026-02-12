// scripts/backfill/regeneratePlayerCaches.js
import { supabase } from "../mlb/shared/supabaseBackend.js";
import fetch from "node-fetch";
import "dotenv/config";
import { getBaseURL } from "../../../src/shared/archive/getBaseURL.js";

async function getAllPlayerIDs() {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id")
    .not("player_id", "is", null);

  if (error) {
    console.error("❌ Failed to fetch player IDs:", error.message);
    return [];
  }

  const uniqueIds = [...new Set(data.map((d) => d.player_id))];
  console.log(`📦 Found ${uniqueIds.length} unique player IDs`);
  return uniqueIds;
}

async function fetchAndCacheProfile(playerId) {
  const url = `${getBaseURL()}/player-profile/${playerId}`;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Status ${res.status}`);
    const profile = await res.json();

    const { error } = await supabase.from("player_profiles_cache").upsert({
      player_id: playerId,
      cached_json: profile,
      updated_at: new Date().toISOString(),
    });

    if (error) {
      console.warn(
        `⚠️ Failed to cache profile for ${playerId}: ${error.message}`
      );
    } else {
      console.log(`✅ Cached profile for ${playerId}`);
    }
  } catch (err) {
    console.error(`❌ Error for ${playerId}: ${err.message}`);
  }
}

async function main() {
  const playerIds = await getAllPlayerIDs();
  for (const playerId of playerIds) {
    await fetchAndCacheProfile(playerId);
  }

  console.log("🎉 Finished caching all player profiles.");
}

main();
