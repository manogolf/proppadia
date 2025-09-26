// backend/scripts/cache/generateCachedPlayerProfiles.js

import fetch from "node-fetch";
import { supabase } from "../shared/supabaseBackend.js";
import { getBaseURL } from "../../../shared/getBaseURL.js";
import "dotenv/config";

const BATCH_SIZE = 10;
const DELAY_MS = 1000;

async function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getAllPlayerIds() {
  const { data, error } = await supabase
    .from("player_props")
    .select("player_id")
    .not("player_id", "is", null);

  if (error) throw new Error(`❌ Failed to fetch player_ids: ${error.message}`);

  const uniqueIds = [...new Set(data.map((d) => d.player_id))];
  console.log(`📦 Found ${uniqueIds.length} unique player_ids`);
  return uniqueIds;
}

async function warmCacheForPlayer(playerId) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000); // 8s timeout

    const url = `${getBaseURL()}/player-profile/${playerId}`;
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    if (!res.ok) throw new Error(`Status ${res.status}`);
    const json = await res.json();
    console.log(`✅ Cached profile for ${playerId}`);
    return json;
  } catch (err) {
    console.warn(`⚠️ Failed to cache profile for ${playerId}: ${err.message}`);
    return null;
  }
}

async function main() {
  try {
    console.log("🚀 Starting player profile cache generation...");
    const playerIds = await getAllPlayerIds();
    let successCount = 0;
    let failureCount = 0;

    for (let i = 0; i < playerIds.length; i += BATCH_SIZE) {
      const chunk = playerIds.slice(i, i + BATCH_SIZE);

      const results = await Promise.allSettled(
        chunk.map((id) => warmCacheForPlayer(id))
      );

      for (const result of results) {
        if (result.status === "fulfilled" && result.value) {
          successCount++;
        } else {
          failureCount++;
        }
      }

      console.log(
        `📊 Progress: ${successCount} success / ${failureCount} failed`
      );
      await pause(DELAY_MS); // give server memory a break
    }

    console.log(`🎯 Finished: ${successCount} cached / ${failureCount} failed`);
    process.exit(0);
  } catch (err) {
    console.error("🔥 Fatal error during cache generation:", err);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await main();
}
