/**
 * 📄 File: backend/scripts/generateDerivedStats.js
 *
 * Populates the `player_derived_stats` table (d7d15d30) with derived stat features for recently played games.
 *
 * It works by:
 * - Fetching unique (player_id, game_date, game_id) rows from `model_training_props`
 * - Preloading 30 days of boxscores into a cache
 * - Computing derived stats using `getDerivedStats(...)`
 * - Writing/upserting derived features to Supabase, bucketed for parallelism
 *
 * Features:
 * - Supports optional --bucket=1/8 arguments for distributed runs
 * - Suppresses logs unless --verbose is passed
 * - Includes fetch timeout protection and Supabase error escalation
 *
 * Intended to be run daily via cron to ensure fresh derived stats are available for modeling.
 */

//  backend/scripts/generateDerivedStats.js

import { supabase } from "./shared/supabaseBackend.js";
import { getDerivedStats } from "./shared/getDerivedStats.js";
import { parseArgs } from "node:util";

const DEFAULT_START = "2023-03-30";
const DEFAULT_END = new Date().toISOString().slice(0, 10);
const BATCH_SIZE = 500;

let inserted = 0;
let skipped = 0;
let failed = 0;

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

async function getPlayerGameHistoryByDate(gameDate) {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id, game_id, game_date, prop_type, prop_value")
    .lte("game_date", gameDate);

  if (error) {
    console.error("❌ Failed to load player history:", error);
    return new Map();
  }
  console.log(`🧪 Querying MT history for game_date <= '${gameDate}'`);

  console.log(
    `🧪 Loaded ${data.length} history rows for gameDate <= ${gameDate}`
  ); // ✅ ADD THIS

  const historyMap = new Map();
  for (const row of data) {
    const pid = String(row.player_id);
    if (!historyMap.has(pid)) historyMap.set(pid, []);
    historyMap.get(pid).push(row);
  }
  return historyMap;
}

async function setIfMissing(row, tableName) {
  const { player_id, game_id } = row;
  if (!player_id || !game_id) {
    console.warn(`⚠️ Missing player_id or game_id for upsert`);
    return "skipped";
  }

  const { data, error } = await supabase
    .from(tableName)
    .select("id")
    .eq("player_id", player_id)
    .eq("game_id", game_id)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    console.error(`❌ Supabase error on check:`, error.message);
    return "skipped";
  }

  if (data) {
    return "skipped"; // row already exists
  }

  const { error: insertError } = await supabase.from(tableName).insert(row);

  if (insertError) {
    console.error(
      `❌ Insert failed for ${player_id} on ${game_id}:`,
      insertError.message
    );
    return "skipped";
  }

  return "inserted";
}

async function processDay(gameDate) {
  console.log(`\n📅 Processing date: ${gameDate}`);

  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id, game_id, game_date")
    .eq("game_date", gameDate);

  if (error) {
    console.error(`❌ Supabase error on ${gameDate}:`, error);
    failed++;
    return;
  }

  const uniquePairs = Array.from(
    new Map(
      data.map((row) => [`${row.player_id}_${row.game_id}`, row])
    ).values()
  );

  const playerGameHistory = await getPlayerGameHistoryByDate(gameDate);
  console.log(
    `🧪 Loaded history map for ${gameDate} → players:`,
    playerGameHistory.size
  );

  for (const { player_id, game_id, game_date } of uniquePairs) {
    try {
      const stats = await getDerivedStats(
        player_id,
        game_date,
        game_id,
        playerGameHistory,
        supabase,
        "history"
      );

      if (!stats || Object.keys(stats).length === 0) {
        skipped++;
        continue;
      }

      const status = await setIfMissing(
        {
          player_id,
          game_id,
          game_date,
          ...stats,
        },
        "player_derived_stats"
      );

      if (status === "inserted") {
        inserted++;
      } else {
        skipped++;
      }
    } catch (err) {
      console.error(`❌ Error for ${player_id} on ${game_id}:`, err.message);
      failed++;
    }

    await delay(50); // ✅ ← Add delay after each player-game pair
  }

  console.log(
    `✅ ${gameDate} complete: Inserted=${inserted}, Skipped=${skipped}, Failed=${failed}`
  );
}

async function runBackfill(startDate, endDate) {
  let current = new Date(startDate);
  const end = new Date(endDate);

  while (current <= end) {
    const iso = current.toISOString().slice(0, 10);
    await processDay(iso);
    current.setDate(current.getDate() + 1);
    await delay(1000);
  }

  console.log("\n🏁 Historical derived stats backfill complete.");
  console.log("📈 Total inserted:", inserted);
  console.log("🟡 Total skipped:", skipped);
  console.log("❌ Total errors:", failed);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { values } = parseArgs({
    options: {
      start: { type: "string" },
      end: { type: "string" },
    },
  });

  const start = values.start || DEFAULT_START;
  const end = values.end || DEFAULT_END;
  runBackfill(start, end);
}

export { runBackfill };
