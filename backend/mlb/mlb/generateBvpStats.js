// backend/scripts/generateBvpStats.js

import { supabase } from "./shared/supabaseBackend.js";
import { getLiveFeedFromGameID } from "./shared/mlbApiUtils.js";
import { setIfMissing } from "../../shared/objectUtils.js";
import dayjs from "dayjs";

const BATCH_DELAY_MS = 500;
const BUCKET_SIZE = 10000;

let totalInserted = 0;
let totalUpdated = 0;
let totalSkipped = 0;
let totalFailed = 0;

async function runFullBackfill() {
  const argStart = process.argv.find((arg) => arg.startsWith("--startDate="));
  const argEnd = process.argv.find((arg) => arg.startsWith("--endDate="));

  const startDate = argStart
    ? argStart.split("=")[1]
    : dayjs().subtract(1, "day").format("YYYY-MM-DD");
  const endDate = argEnd ? argEnd.split("=")[1] : null;

  console.log(
    `🚀 Starting full BvP backfill from ${startDate}${
      endDate ? ` to ${endDate}` : ""
    }`
  );

  let offset = 0;

  while (true) {
    console.log(`📦 Fetching batch at offset ${offset}`);

    // Build Supabase query with dynamic date filters
    let query = supabase
      .from("model_training_props")
      .select("game_id")
      .eq("prop_source", "mlb_api")
      .gte("game_date", startDate);

    if (endDate) query = query.lte("game_date", endDate);

    query = query
      .order("game_date", { ascending: false })
      .range(offset, offset + BUCKET_SIZE - 1);

    const { data: games, error: fetchError } = await query;

    if (fetchError) {
      if (fetchError.message?.includes("timeout")) {
        console.warn(`⚠️ Timeout at offset ${offset}, skipping to next batch`);
        offset += BUCKET_SIZE;
        await new Promise((r) => setTimeout(r, BATCH_DELAY_MS));
        continue;
      }
      console.error("❌ Failed to fetch recent games:", fetchError);
      break;
    }

    if (!games || games.length === 0) {
      console.log("✅ No more games to process. Done.");
      break;
    }

    const uniqueGameIds = [...new Set(games.map((g) => g.game_id))];

    for (const gameId of uniqueGameIds) {
      try {
        await processGame(gameId);
      } catch (e) {
        console.error(`❌ Error processing game ${gameId}:`, e);
      }
      await new Promise((r) => setTimeout(r, BATCH_DELAY_MS));
    }

    offset += BUCKET_SIZE;
  }

  console.log("🏁 Full BvP backfill complete");
  console.log(
    `📊 Totals — Inserted=${totalInserted}, Updated=${totalUpdated}, Skipped=${totalSkipped}, Failed=${totalFailed}`
  );
}

async function processGame(gameId) {
  console.log(`🎯 Target game: ${gameId}`);

  const liveFeed = await getLiveFeedFromGameID(gameId);
  const allPlays = liveFeed?.liveData?.plays?.allPlays || [];
  const gameType = liveFeed?.gameData?.game?.type;

  if (gameType !== "R") {
    console.warn(
      `⏩ Skipping non-regular season game ${gameId} (type=${gameType})`
    );
    return;
  }

  // Robust starter detection
  const probableHome = liveFeed?.gameData?.probablePitchers?.home?.id ?? null;
  const probableAway = liveFeed?.gameData?.probablePitchers?.away?.id ?? null;

  const boxHomeFirst =
    liveFeed?.liveData?.boxscore?.teams?.home?.pitchers?.[0] ?? null;
  const boxAwayFirst =
    liveFeed?.liveData?.boxscore?.teams?.away?.pitchers?.[0] ?? null;

  const firstTop = allPlays.find(
    (p) => p.about?.inning === 1 && p.about?.halfInning === "top"
  );
  const firstBot = allPlays.find(
    (p) => p.about?.inning === 1 && p.about?.halfInning === "bottom"
  );
  const topPitcher = firstTop?.matchup?.pitcher?.id ?? null; // home SP
  const bottomPitcher = firstBot?.matchup?.pitcher?.id ?? null; // away SP

  const homeStarterId = probableHome ?? boxHomeFirst ?? topPitcher ?? null;
  const awayStarterId = probableAway ?? boxAwayFirst ?? bottomPitcher ?? null;

  if (!homeStarterId || !awayStarterId) {
    console.warn(
      `⚠️ Missing starters for game ${gameId} (home=${homeStarterId}, away=${awayStarterId})`
    );
    // We can still proceed; default mapping below handles many cases
  }

  // Team membership sets (for default mapping)
  const homeBatters = new Set(
    liveFeed?.liveData?.boxscore?.teams?.home?.batters || []
  );
  const awayBatters = new Set(
    liveFeed?.liveData?.boxscore?.teams?.away?.batters || []
  );

  const starters = new Set([homeStarterId, awayStarterId].filter(Boolean));

  console.log(`🎮 Loaded ${allPlays.length} plays`);

  const { data: rows, error } = await supabase
    .from("model_training_props")
    .select("id, player_id")
    .eq("game_id", gameId)
    .eq("prop_source", "mlb_api");

  if (error) {
    console.error("❌ Error fetching props:", error);
    return;
  }

  const bvpStatsCache = new Map();

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  for (const row of rows) {
    const batterId = Number(row.player_id);
    if (!Number.isFinite(batterId)) {
      skipped++;
      continue;
    }

    if (!bvpStatsCache.has(batterId)) {
      // Default opponent pitcher by team membership
      const defaultPitcherId = homeBatters.has(batterId)
        ? awayStarterId ?? null
        : awayBatters.has(batterId)
        ? homeStarterId ?? null
        : null;

      // Try to find an actual PA vs a starter
      const playVsStarter = allPlays.find(
        (p) =>
          p.matchup?.batter?.id === batterId &&
          starters.has(p.matchup?.pitcher?.id)
      );

      const pitcherId = playVsStarter?.matchup?.pitcher?.id ?? defaultPitcherId;

      if (!pitcherId) {
        bvpStatsCache.set(batterId, null);
      } else {
        const stats = computeBvpStats(batterId, pitcherId, allPlays);
        bvpStatsCache.set(batterId, { pitcherId, stats });
      }
    }

    const cached = bvpStatsCache.get(batterId);
    if (!cached) {
      skipped++;
      continue;
    }

    const { pitcherId, stats } = cached;

    // Allow zero-rows via env toggle
    const INSERT_ZERO_ROWS = process.env.BVP_INSERT_ZEROS === "true";
    if (!stats && !INSERT_ZERO_ROWS) {
      skipped++;
      continue;
    }

    const effective = stats ?? {
      pa: 0,
      ab: 0,
      hits: 0,
      home_runs: 0,
      strikeouts: 0,
      walks: 0,
      rbi: 0,
      total_bases: 0,
    };

    // Preselect existing
    const { data: existingRows, error: selErr } = await supabase
      .from("bvp_stats")
      .select(
        "bvp_plate_appearances,bvp_at_bats,bvp_hits,bvp_home_runs,bvp_strikeouts,bvp_walks,bvp_rbi,bvp_total_bases"
      )
      .eq("game_id", gameId)
      .eq("batter_id", String(batterId))
      .eq("pitcher_id", String(pitcherId))
      .limit(1);

    if (selErr) {
      console.error(`❌ Select error for batter ${batterId}:`, selErr);
      failed++;
      continue;
    }

    const existed = (existingRows?.length ?? 0) > 0;
    const existing = existingRows?.[0] || {};

    // Build upsert payload with setIfMissing
    const upsertPayload = {
      game_id: gameId,
      batter_id: String(batterId),
      pitcher_id: String(pitcherId),
    };

    setIfMissing(
      upsertPayload,
      "bvp_plate_appearances",
      effective.pa,
      existing.bvp_plate_appearances
    );
    setIfMissing(
      upsertPayload,
      "bvp_at_bats",
      effective.ab,
      existing.bvp_at_bats
    );
    setIfMissing(upsertPayload, "bvp_hits", effective.hits, existing.bvp_hits);
    setIfMissing(
      upsertPayload,
      "bvp_home_runs",
      effective.home_runs,
      existing.bvp_home_runs
    );
    setIfMissing(
      upsertPayload,
      "bvp_strikeouts",
      effective.strikeouts,
      existing.bvp_strikeouts
    );
    setIfMissing(
      upsertPayload,
      "bvp_walks",
      effective.walks,
      existing.bvp_walks
    );
    setIfMissing(upsertPayload, "bvp_rbi", effective.rbi, existing.bvp_rbi);
    setIfMissing(
      upsertPayload,
      "bvp_total_bases",
      effective.total_bases,
      existing.bvp_total_bases
    );

    // Did we add any new fields (beyond the PK triplet)?
    const changed = Object.keys(upsertPayload).length > 3;

    if (!changed) {
      skipped++;
      continue;
    }

    const { error: upsertError } = await supabase
      .from("bvp_stats")
      .upsert(upsertPayload, { onConflict: "game_id,batter_id,pitcher_id" });

    if (upsertError) {
      console.error(
        `❌ Failed to upsert BvP for batter ${batterId}:`,
        upsertError
      );
      failed++;
    } else if (!existed) {
      inserted++;
    } else {
      updated++;
    }
  }

  console.log(
    `✅ Game ${gameId} complete: Inserted=${inserted}, Updated=${updated}, Skipped=${skipped}, Failed=${failed}`
  );
  totalInserted += inserted;
  totalUpdated += updated;
  totalSkipped += skipped;
  totalFailed += failed;
}

function computeBvpStats(batterId, pitcherId, allPlays) {
  const relevantPlays = allPlays.filter(
    (p) =>
      p.matchup?.batter?.id === batterId && p.matchup?.pitcher?.id === pitcherId
  );

  if (relevantPlays.length === 0) return null;

  let pa = 0,
    ab = 0,
    hits = 0,
    home_runs = 0,
    strikeouts = 0,
    walks = 0,
    rbi = 0,
    total_bases = 0;

  for (const play of relevantPlays) {
    const result = play.result?.eventType;
    if (!result) continue;

    pa++;
    const isAB = ![
      "walk",
      "hit_by_pitch",
      "sac_bunt",
      "sac_fly",
      "catcher_interf",
    ].includes(result);
    if (isAB) ab++;

    switch (result) {
      case "single":
        hits++;
        total_bases += 1;
        break;
      case "double":
        hits++;
        total_bases += 2;
        break;
      case "triple":
        hits++;
        total_bases += 3;
        break;
      case "home_run":
        hits++;
        home_runs++;
        total_bases += 4;
        break;
      case "walk":
        walks++;
        break;
      case "strikeout":
        strikeouts++;
        break;
    }

    rbi += play.result?.rbi || 0;
  }

  return { pa, ab, hits, home_runs, strikeouts, walks, rbi, total_bases };
}

// 🔁 Run
runFullBackfill().catch((err) => {
  console.error("❌ Uncaught error in full BvP backfill:", err);
});
