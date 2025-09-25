// scripts/backfillRunsRBIs.js
import { DateTime } from "luxon";
import { fetchSchedule } from "../../src/utils/fetchSchedule.js";
import { fetchBoxscoreStatsForGame } from "../../src/utils/fetchBoxscoreStats.js";
import { derivePropValue } from "../../backend/scripts/resolution/derivePropValue.js";
import { supabase } from "../../backend/scripts/shared/supabaseBackend.js";

const DAYS_BACK = 5;
const propType = "runs_rbis"; // ✅ correct normalized name

async function run() {
  for (let i = 1; i <= DAYS_BACK; i++) {
    const targetDate = DateTime.now().minus({ days: i }).toISODate();
    console.log(`📅 Backfilling '${propType}' for ${targetDate}...`);

    const { data: schedule, error } = await fetchSchedule(targetDate);
    if (error || !schedule) {
      console.warn(`⚠️ No schedule for ${targetDate}`);
      continue;
    }

    for (const game of schedule) {
      const gameId = game.gamePk;
      const boxscore = await fetchBoxscoreStatsForGame(gameId);
      if (!boxscore) continue;

      for (const player of boxscore) {
        const total = derivePropValue(player.stats, propType);
        if (total === null || total === 0) continue;

        await supabase.from("model_training_props").upsert(
          {
            player_name: player.fullName,
            player_id: player.id,
            game_id: gameId,
            game_date: targetDate,
            team: player.teamAbbr,
            is_home: player.isHome ? 1 : 0,
            prop_type: propType,
            prop_value: 0.5,
            result: total,
            outcome: total > 0.5 ? "win" : "loss",
            status: "resolved",
            prop_source: "mlb_api",
          },
          { onConflict: ["player_id", "game_id", "prop_type"] }
        );
      }
    }

    console.log(`✅ Finished ${targetDate}`);
  }

  console.log("🎯 All backfills complete.");
}

run();
