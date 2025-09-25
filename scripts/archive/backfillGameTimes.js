// File: scripts/backfillGameTimes.js

import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { DateTime } from "luxon";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// 🕒 Try datetime.dateTime, fall back to firstPitch
async function fetchGameTimeFromMLB(gameId, gameDate) {
  const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/feed/live`;
  try {
    const res = await fetch(url);
    if (res.status === 404) {
      console.warn(`⚠️ Game ${gameId} not found (404)`);
      return null;
    }
    if (!res.ok) throw new Error(`HTTP error ${res.status}`);

    const data = await res.json();
    let dateTime = data?.gameData?.datetime?.dateTime;
    if (!dateTime && data?.gameData?.officialDate) {
      dateTime = `${data.gameData.officialDate}T13:00:00Z`;
    }
    return dateTime ? DateTime.fromISO(dateTime).toISO() : null;
  } catch (error) {
    console.error(`❌ Error fetching game ${gameId}:`, error.message);
    return null;
  }
}

async function updateGameTime(gameId, gameTime) {
  const { error } = await supabase
    .from("model_training_props")
    .update({ game_time: gameTime })
    .eq("game_id", gameId);

  if (error) {
    console.error(`❌ Failed to update game ${gameId}:`, error.message);
  } else {
    console.log(`✅ Updated game ${gameId} with time ${gameTime}`);
  }
}

async function main() {
  console.log("🚀 Starting game time backfill...");

  const { data: games, error } = await supabase
    .from("model_training_props")
    .select("game_id, game_date")
    .is("game_time", null);

  if (error) {
    console.error("❌ Failed to fetch game_ids:", error.message);
    process.exit(1);
  }

  const cleanedGames = games.filter(
    (g) => typeof g.game_id === "number" && g.game_date && !isNaN(g.game_id)
  );

  if (cleanedGames.length === 0) {
    console.warn("⚠️ No valid game_ids found to process.");
    process.exit(0);
  }

  console.log(
    `🧹 Cleaned game_ids: ${cleanedGames.length} (original: ${games.length})`
  );

  for (const { game_id: gameId, game_date: gameDate } of cleanedGames) {
    const gameTime = await fetchGameTimeFromMLB(gameId, gameDate);
    if (gameTime) {
      await updateGameTime(gameId, gameTime);
    } else {
      console.warn(`⚠️ No game time found for game ${gameId}`);
    }
  }

  console.log("🎯 Game time backfill complete.");
}

main();
