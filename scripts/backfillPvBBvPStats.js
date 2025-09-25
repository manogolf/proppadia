// File: scripts/backfillPvBBvPStats.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { resolveStatForPlayer } from "../backend/scripts/resolution/statResolvers.js";
import {
  getBoxscoreFromGameID,
  getLiveFeedFromGameID,
} from "../backend/scripts/shared/mlbApiUtils.js";
import { getPlayerTeamFromBoxscoreData } from "../backend/scripts/shared/playerUtils.js";
import fetch from "node-fetch";

const DELAY_MS = 200;

const MISSING_CONDITIONS = [
  "pvb_at_bats=is.null",
  "pvb_hits=is.null",
  "pvb_home_runs=is.null",
  "pvb_strikeouts=is.null",
  "pvb_walks=is.null",
  "pvb_plate_appearances=is.null",
  "pvb_rbi=is.null",
  "pvb_total_bases=is.null",
  "pvb_sac_flies=is.null",
  "bvp_at_bats=is.null",
  "bvp_hits=is.null",
  "bvp_home_runs=is.null",
  "bvp_strikeouts=is.null",
  "bvp_walks=is.null",
  "bvp_plate_appearances=is.null",
  "bvp_rbi=is.null",
];

const columnsToCheck = MISSING_CONDITIONS.map((c) => c.split("=")[0]);

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function fetchRowsNeedingStats(offset, limit) {
  const { data, error } = await supabase
    .from("model_training_props")
    .select(`id, player_id, prop_type, game_id, ${columnsToCheck.join(", ")}`)
    .not("player_id", "is", null)
    .not("game_id", "is", null)
    .range(offset, offset + limit - 1);

  if (error) {
    console.error("❌ Failed to fetch base rows:", error.message);
    return [];
  }

  return data.filter((row) =>
    columnsToCheck.some((col) => row[col] === null || row[col] === undefined)
  );
}

function hasMeaningfulBvPStats(stats) {
  return !!(
    stats &&
    typeof stats === "object" &&
    (typeof stats.hits === "number" ||
      typeof stats.rbi === "number" ||
      typeof stats.total_bases === "number" ||
      typeof stats.home_runs === "number" ||
      typeof stats.strikeouts === "number" ||
      typeof stats.walks === "number" ||
      typeof stats.pa === "number" ||
      typeof stats.ab === "number")
  );
}

async function processRow(row) {
  const { id, player_id, prop_type, game_id } = row;
  const mode = prop_type.includes("pitching") ? "pvb" : "bvp";

  console.log(
    `🧪 ${mode.toUpperCase()} | Row: ${id} | Prop: ${prop_type} | Game: ${game_id}`
  );

  let box;
  try {
    box = await getBoxscoreFromGameID(game_id);
    if (!box?.teams?.home?.players || !box.teams.away?.players) {
      console.warn(
        `❌ Skipping ${id} — incomplete boxscore for game ${game_id}`
      );
      return;
    }
  } catch (err) {
    console.error(`❌ Error fetching boxscore for game ${game_id}:`, err);
    return;
  }

  const playerTeam = getPlayerTeamFromBoxscoreData(box, player_id);
  if (!playerTeam) {
    console.warn(
      `❌ Skipping ${id} — could not determine player's team in game ${game_id}`
    );
    return;
  }

  const opponentTeam = playerTeam === "home" ? "away" : "home";
  const options = { mode, game_id };

  // Load live feed and allPlays early
  const liveFeed = await getLiveFeedFromGameID(game_id);
  const allPlays = liveFeed?.liveData?.plays?.allPlays || [];
  options.allPlays = allPlays;

  if (mode === "bvp") {
    options.batter_id = player_id;

    const bvpPlays = allPlays.filter(
      (play) =>
        play?.matchup?.batter?.id === player_id &&
        play?.matchup?.pitcher?.id != null
    );

    if (bvpPlays.length === 0) {
      console.warn(
        `⚠️ Skipping ${id} — no plays found where batter ${player_id} faced any pitcher`
      );

      // 🔍 Add debug block here
      const batterPlays = allPlays.filter(
        (p) => p?.matchup?.batter?.id === player_id
      );

      const pitcherPlays = allPlays.filter(
        (p) => p?.matchup?.pitcher?.id != null
      );

      console.log(`📊 Summary for ${id} — batter ${player_id}:`);
      console.log(`  batterMatch=${batterPlays.length}`);
      console.log(`  pitcherMatch=${pitcherPlays.length}`);
      console.log(
        `  bothMatch=${
          allPlays.filter(
            (p) =>
              p?.matchup?.batter?.id === player_id &&
              p?.matchup?.pitcher?.id != null
          ).length
        }`
      );

      for (const [i, play] of allPlays.entries()) {
        const batterMatch = play?.matchup?.batter?.id === player_id;
        const pitcherMatch = !!play?.matchup?.pitcher?.id;
        if (batterMatch || pitcherMatch) {
          console.log(
            `🧪 Play ${i} → batter=${play?.matchup?.batter?.id} (${batterMatch}), pitcher=${play?.matchup?.pitcher?.id} (${pitcherMatch}), desc=${play?.result?.description}`
          );
        }
      }

      return;
    }

    // Collect all unique pitchers this batter faced
    const pitcherIds = [...new Set(bvpPlays.map((p) => p.matchup.pitcher.id))];
    if (pitcherIds.length > 1) {
      console.log(
        `🔍 Batter ${player_id} faced multiple pitchers: [${pitcherIds.join(
          ", "
        )}]`
      );
    }

    options.pitcher_id = pitcherIds[0]; // pick the first for consistency (or enhance later)

    if (!bvpPlay) {
      console.warn(
        `⚠️ Skipping ${id} — no plays found where batter ${player_id} faced any pitcher`
      );
      return;
    }

    options.pitcher_id = bvpPlay.matchup.pitcher.id;
    console.log(
      `🔍 Using opponent pitcher from play data: ${options.pitcher_id}`
    );
  } else {
    options.pitcher_id = player_id;

    const pvbPlay = allPlays.find(
      (play) =>
        play?.matchup?.pitcher?.id === player_id &&
        play?.matchup?.batter?.id != null
    );

    if (!pvbPlay) {
      console.warn(
        `⚠️ Skipping ${id} — no plays found where pitcher ${player_id} faced any batter`
      );
      return;
    }

    options.batter_id = pvbPlay.matchup.batter.id;
    console.log(
      `🔍 Using opponent batter from play data: ${options.batter_id}`
    );
  }

  const result = await resolveStatForPlayer(options);
  const stats = result?.rawStats;

  if (!hasMeaningfulBvPStats(stats)) {
    console.warn(
      `⚠️ Skipping ${id} — no meaningful stats from resolveStatForPlayer`
    );
    return;
  }

  const updates = {};
  const setIfDefined = (obj, key, value, existing) => {
    if (
      value !== undefined &&
      value !== null &&
      (existing === null || existing === undefined)
    ) {
      obj[key] = value;
    }
  };

  const map =
    mode === "bvp"
      ? {
          bvp_plate_appearances: stats.pa ?? stats.plate_appearances,
          bvp_at_bats: stats.ab ?? stats.at_bats,
          bvp_hits: stats.hits,
          bvp_home_runs: stats.home_runs,
          bvp_strikeouts: stats.strikeouts,
          bvp_walks: stats.walks,
          bvp_rbi: stats.rbi,
          bvp_total_bases: stats.total_bases,
        }
      : {
          pvb_plate_appearances: stats.pa ?? stats.plate_appearances,
          pvb_at_bats: stats.ab ?? stats.at_bats,
          pvb_hits: stats.hits,
          pvb_home_runs: stats.home_runs,
          pvb_strikeouts: stats.strikeouts,
          pvb_walks: stats.walks,
          pvb_rbi: stats.rbi,
          pvb_total_bases: stats.total_bases,
        };

  for (const [k, v] of Object.entries(map)) {
    setIfDefined(updates, k, v, row[k]);
  }

  if (!Object.keys(updates).length) {
    console.warn(`⚠️ Skipping ${id} — no new values to update`);
    return;
  }

  const { error } = await supabase
    .from("model_training_props")
    .update(updates)
    .eq("id", id);
  if (error) {
    console.error(`❌ Failed to update row ${id}:`, error);
  } else {
    console.log(
      `✅ Updated row ${id} with fields: ${Object.keys(updates).join(", ")}`
    );
  }
}

async function run() {
  let offset = parseInt(process.env.START_OFFSET || "0", 10);
  const batchSize = 1000;
  let batchCount = 0;

  while (true) {
    const rows = await fetchRowsNeedingStats(offset, batchSize);
    if (!rows.length) break;

    console.log(
      `🚀 Batch ${++batchCount} | Offset ${offset} | Rows: ${rows.length}`
    );
    for (const row of rows) await processRow(row);
    offset += batchSize;
  }

  console.log("🏁 All done");
}

run().catch((err) => {
  console.error("💥 Fatal crash:", err);
});
