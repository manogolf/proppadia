//  scripts/backfillBvpPvbByGame.js

import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import {
  getBoxscoreFromGameID,
  getLiveFeedFromGameID,
} from "../backend/scripts/shared/mlbApiUtils.js";
import { resolveStatForPlayer } from "../backend/scripts/resolution/statResolvers.js";
import { setIfMissing } from "../backend/scripts/shared/objectUtils.js";

const TARGET_GAME_IDS = [777131]; // Replace with actual game_id(s)

function getPitchersFromBoxscore(boxscore) {
  const pitchers = new Set();

  for (const team of ["home", "away"]) {
    const pitcherIds = boxscore.teams?.[team]?.pitchers ?? [];
    for (const pid of pitcherIds) {
      pitchers.add(pid);
    }
  }

  return pitchers;
}

function hasMeaningfulBvPStats(stats) {
  if (!stats) return false;
  return (
    stats.ab > 0 ||
    stats.pa > 0 ||
    stats.hits > 0 ||
    stats.home_runs > 0 ||
    stats.strikeouts > 0 ||
    stats.walks > 0
  );
}

async function processPlayer(
  game_id,
  player_id,
  mode,
  cachedAllPlays,
  cachedPitcherIds,
  row
) {
  const options = { game_id, mode };

  if (!Array.isArray(cachedAllPlays)) {
    return { success: false, reason: "cachedAllPlays_missing", row_id: row.id };
  }

  const allPlayCount = cachedAllPlays.length;
  console.log(
    `🧪 ${mode.toUpperCase()} | player=${player_id} | total plays=${allPlayCount}`
  );

  if (mode === "bvp") {
    options.batter_id = player_id;
    const playerIdNum = Number(player_id);

    const bvpPlays = cachedAllPlays.filter((play) => {
      const playBatterId = Number(play?.matchup?.batter?.id);
      const playPitcherId = Number(play?.matchup?.pitcher?.id);
      const batterMatch = playBatterId === playerIdNum;
      const pitcherMatch = cachedPitcherIds.has(playPitcherId);
      return batterMatch && pitcherMatch;
    });

    console.log(
      `🔍 BvP: Found ${bvpPlays.length} plays for batter ${player_id}`
    );

    if (!bvpPlays.length) {
      console.warn(`⚠️ No matching BvP plays for batter ${player_id}`);
      return { success: false, reason: "no_bvp_plays", row_id: row.id };
    }

    const resolvedPitcherId = bvpPlays.at(-1)?.matchup?.pitcher?.id;
    options.pitcher_id = resolvedPitcherId;
    options.allPlays = bvpPlays;

    if (!resolvedPitcherId || typeof resolvedPitcherId !== "number") {
      console.warn(
        `⚠️ BvP: Could not resolve valid pitcher for batter ${player_id} in game ${game_id}`
      );
    } else {
      console.log(
        `✅ BvP: Resolved pitcher ${resolvedPitcherId} for batter ${player_id}`
      );
    }
  } else {
    options.pitcher_id = player_id;

    const pvbPlays = cachedAllPlays.filter((play) => {
      const pitcherMatch =
        Number(play?.matchup?.pitcher?.id) === Number(player_id);
      const batterExists = play?.matchup?.batter?.id != null;
      return pitcherMatch && batterExists;
    });

    console.log(
      `🔍 PvB: Found ${pvbPlays.length} plays for pitcher ${player_id}`
    );

    if (!pvbPlays.length) {
      console.warn(`⚠️ No matching PvB plays for pitcher ${player_id}`);
      return { success: false, reason: "no_pvb_plays", row_id: row.id };
    }

    const resolvedBatterId = pvbPlays.at(-1).matchup.batter.id;
    options.batter_id = resolvedBatterId;
    options.allPlays = pvbPlays;

    if (!resolvedBatterId || typeof resolvedBatterId !== "number") {
      console.warn(
        `⚠️ PvB: Could not resolve valid batter for pitcher ${player_id} in game ${game_id}`
      );
    } else {
      console.log(
        `✅ PvB: Resolved batter ${resolvedBatterId} for pitcher ${player_id}`
      );
    }
  }

  if (!options.allPlays) {
    console.error("❌ Missing allPlays before calling resolveStatForPlayer", {
      mode,
      player_id,
      game_id,
    });
    return { success: false, reason: "missing_allPlays", row_id: row.id };
  }

  const result = await resolveStatForPlayer(options);
  const stats = result?.rawStats;

  if (!hasMeaningfulBvPStats(stats)) {
    console.warn(
      `⚠️ ${mode.toUpperCase()} | ${player_id} → no meaningful stats returned`
    );
    return { success: false, reason: "no_meaningful_stats", row_id: row.id };
  }

  const updates = {};
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
    setIfMissing(updates, k, v, row[k]);
  }

  if (!Object.keys(updates).length) {
    return { success: false, reason: "no_new_updates", row_id: row.id };
  }

  return {
    success: true,
    player_id,
    row_id: row.id,
    mode,
    updates,
  };
}

async function run() {
  console.log(`🎯 Target games:`, TARGET_GAME_IDS);
  for (const game_id of TARGET_GAME_IDS) {
    console.log(`📦 Processing game ${game_id}`);

    const boxscore = await getBoxscoreFromGameID(game_id);
    const cachedPitcherIds = getPitchersFromBoxscore(boxscore);
    console.log(`🧾 Pitchers:`, Array.from(cachedPitcherIds));

    const liveFeed = await getLiveFeedFromGameID(game_id);
    const allPlays = liveFeed?.liveData?.plays?.allPlays || [];
    console.log(`🎮 Loaded ${allPlays.length} plays`);

    const { data: rows, error } = await supabase
      .from("model_training_props")
      .select("*")
      .eq("game_id", game_id)
      .eq("prop_source", "mlb_api"); // ✅ limit to stat-derived rows only

    if (error) {
      console.error(`❌ Failed to fetch rows for game ${game_id}:`, error);
      continue;
    }

    if (!rows?.length) {
      console.warn(
        `⚠️ No rows found for game ${game_id} with prop_source='mlb_api'`
      );
      continue;
    }

    console.log(`📥 Fetched ${rows.length} row(s) for game ${game_id}`);

    const seenPairs = new Set(); // batterId-pitcherId keys

    for (const row of rows) {
      const isPitcherProp = row.prop_type.includes("pitching");
      const mode = isPitcherProp ? "pvb" : "bvp";

      let pitcherId;

      if (mode === "pvb") {
        pitcherId = row.player_id; // pitcher row
      } else {
        // bvp: lookup one of the pitchers this batter faced
        const play = allPlays.find(
          (p) =>
            p.matchup?.batter?.id === row.player_id &&
            cachedPitcherIds.has(p.matchup?.pitcher?.id)
        );
        pitcherId = play?.matchup?.pitcher?.id;
      }

      if (!pitcherId) {
        console.warn(
          `⚠️ Could not resolve valid pitcher for player ${row.player_id} in game ${game_id}`
        );
        continue;
      }

      const pairKey =
        mode === "pvb"
          ? `${row.player_id}-ALL`
          : `${row.player_id}-${pitcherId}`;

      if (seenPairs.has(pairKey)) {
        console.log(
          `⏩ Skipped duplicate ${mode.toUpperCase()} pair: ${pairKey}`
        );
        continue;
      }

      seenPairs.add(pairKey);

      const result = await processPlayer(
        game_id,
        row.player_id,
        mode,
        allPlays,
        cachedPitcherIds,
        row
      );

      if (result?.success) {
        console.log("🔬 Debug: result returned from processPlayer = ", result);

        const { error: updateError } = await supabase
          .from("model_training_props")
          .update(result.updates)
          .eq("id", result.row_id);

        if (updateError) {
          console.error(
            `❌ Failed to update row ${result.row_id} for player ${result.player_id}:`,
            updateError
          );
        } else {
          console.log(`📦 Supabase updated for row ${result.row_id}`);
        }
      }
    }
  }
  console.log("🏁 Script complete");
}

run();
