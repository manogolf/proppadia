/**
 * backend/scripts/generateDerivedStats.js
 *
 * Populates `player_derived_stats` with derived features for recently played games.
 * - Pulls (player_id, game_id, game_date) from model_training_props
 * - Builds per-player history (<= game_date)
 * - Computes features via getDerivedStats(...)
 * - Upserts into player_derived_stats on (player_id, game_date)
 *
 * CLI:
 *   node backend/scripts/generateDerivedStats.js --start=YYYY-MM-DD --end=YYYY-MM-DD --lookback=21 --verbose
 */

import { supabase } from "./shared/supabaseBackend.js";
import { getDerivedStats } from "./shared/getDerivedStats.js";
import { parseArgs } from "node:util";

const DEFAULT_LOOKBACK_DAYS = 7;
const BATCH_SLEEP_MS = 50; // small delay between rows
const DAY_SLEEP_MS = 300; // small delay between days

let totalInserted = 0;
let totalUpdated = 0;
let totalSkipped = 0;
let totalFailed = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getDateRangeFromLookback(days) {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - days);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
  };
}

/**
 * Upsert a single row and report inserted vs updated vs skipped.
 * Assumes UNIQUE(player_id, game_date) exists on player_derived_stats.
 */
async function upsertWithReport(tableName, row, verbose = false) {
  const { data: exist, error: existErr } = await supabase
    .from(tableName)
    .select("id")
    .eq("player_id", row.player_id)
    .eq("game_date", row.game_date)
    .limit(1);

  const existed = (exist?.length ?? 0) > 0;
  if (existErr && verbose) {
    console.warn(
      "⚠️ Precheck failed; proceeding with upsert:",
      existErr.message || existErr
    );
  }

  const { error: upsertError } = await supabase
    .from(tableName)
    .upsert(row, { onConflict: "player_id,game_date", returning: "minimal" });

  if (upsertError) {
    console.error(
      `❌ Upsert failed for player_id=${row.player_id}, game_date=${row.game_date} (game_id=${row.game_id}):`,
      upsertError.message || upsertError
    );
    return "failed";
  }
  return existed ? "updated" : "inserted";
}

/** Build a history map: player_id (string) -> array of MT rows up to gameDate */
async function getPlayerGameHistoryByDate(gameDate, verbose = false) {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id, game_id, game_date, prop_type, prop_value")
    .lte("game_date", gameDate);

  if (error) {
    console.error("❌ Failed to load player history:", error);
    return new Map();
  }
  if (verbose) {
    console.log(
      `🧪 Loaded ${data.length} history rows for game_date <= ${gameDate}`
    );
  }

  const map = new Map();
  for (const r of data) {
    const pid = String(r.player_id);
    if (!map.has(pid)) map.set(pid, []);
    map.get(pid).push(r);
  }
  return map;
}

async function processDay(gameDate, verbose = false) {
  if (verbose) console.log(`\n📅 Processing: ${gameDate}`);

  // Pull distinct (player_id, game_id, game_date) for that day
  const { data, error } = await supabase
    .from("model_training_props")
    .select("player_id, game_id, game_date")
    .eq("game_date", gameDate);

  if (error) {
    console.error(`❌ Supabase error on ${gameDate}:`, error);
    totalFailed++;
    return;
  }

  const uniquePairs = Array.from(
    new Map(
      (data || []).map((r) => [`${r.player_id}_${r.game_id}`, r])
    ).values()
  );

  const historyMap = await getPlayerGameHistoryByDate(gameDate, verbose);
  if (verbose) {
    console.log(
      `🧪 History map for ${gameDate} → players=${historyMap.size}, pairs=${uniquePairs.length}`
    );
  }

  let dayInserted = 0;
  let dayUpdated = 0;
  let daySkipped = 0;
  let dayFailed = 0;

  for (const { player_id, game_id, game_date } of uniquePairs) {
    try {
      const stats = await getDerivedStats(
        player_id,
        game_date,
        game_id,
        historyMap,
        supabase,
        "history" // sourceMode; OK to keep or drop if your impl ignores it
      );

      if (!stats || Object.keys(stats).length === 0) {
        daySkipped++;
        continue;
      }

      const row = {
        player_id,
        game_id,
        game_date,
        ...stats,
      };

      const status = await upsertWithReport(
        "player_derived_stats",
        row,
        verbose
      );

      if (status === "inserted") {
        dayInserted++;
      } else if (status === "updated") {
        dayUpdated++;
      } else if (status === "failed") {
        dayFailed++;
      } else {
        daySkipped++;
      }
    } catch (err) {
      console.error(
        `❌ Error computing stats for ${player_id} on ${game_id}:`,
        err.message || err
      );
      dayFailed++;
    }

    await sleep(BATCH_SLEEP_MS);
  }

  totalInserted += dayInserted;
  totalUpdated += dayUpdated;
  totalSkipped += daySkipped;
  totalFailed += dayFailed;

  console.log(
    `✅ ${gameDate} → inserted=${dayInserted}, updated=${dayUpdated}, skipped=${daySkipped}, failed=${dayFailed}`
  );
}

async function runBackfill({ start, end, lookback, verbose }) {
  let startDate = start;
  let endDate = end;

  if (!startDate || !endDate) {
    const rng = getDateRangeFromLookback(lookback ?? DEFAULT_LOOKBACK_DAYS);
    startDate = startDate || rng.start;
    endDate = endDate || rng.end;
  }

  console.log(
    `🚀 Derived-stats backfill: ${startDate} → ${endDate} (lookback=${
      lookback ?? DEFAULT_LOOKBACK_DAYS
    })`
  );

  let d = new Date(startDate);
  const endD = new Date(endDate);

  while (d <= endD) {
    const iso = d.toISOString().slice(0, 10);
    await processDay(iso, verbose);
    d.setDate(d.getDate() + 1);
    await sleep(DAY_SLEEP_MS);
  }

  console.log("\n🏁 Done.");
  console.log(
    `📈 Totals — inserted=${totalInserted}, updated=${totalUpdated}, skipped=${totalSkipped}, failed=${totalFailed}`
  );
}

/* ─────────────────────────── CLI ─────────────────────────── */

if (import.meta.url === `file://${process.argv[1]}`) {
  const {
    values: { start, end, lookback, verbose },
  } = parseArgs({
    options: {
      start: { type: "string" },
      end: { type: "string" },
      lookback: { type: "string" },
      verbose: { type: "boolean", default: false },
    },
  });

  const lb = lookback != null ? Number(lookback) : undefined;
  runBackfill({ start, end, lookback: lb, verbose: !!verbose }).catch((e) => {
    console.error("❌ Top-level failure:", e);
    process.exitCode = 1;
  });
}
