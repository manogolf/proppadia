// scripts/syncStreakProfiles.js
import { supabase } from "../backend/scripts/shared/supabaseBackend.js";

export async function syncStreakProfiles() {
  console.log("🔄 Starting sync to player_streak_profiles...");

  const { data: streaks, error: fetchError } = await supabase
    .from("mlb_live_streaks")
    .select(
      "player_name, team, position, prop_type, streak_count, direction, last_game_date, player_id"
    );

  if (fetchError) {
    console.error("❌ Error fetching streaks:", fetchError.message);
    return;
  }

  const updates = streaks.map((s) => ({
    player_id: s.player_id || null,
    player_name: s.player_name,
    team: s.team,
    position: s.position,
    prop_type: s.prop_type,
    streak_count: s.streak_count,
    direction: s.direction,
    last_game_date: s.last_game_date,
    updated_at: new Date().toISOString(),
  }));

  const { error: upsertError } = await supabase
    .from("player_streak_profiles")
    .upsert(updates, {
      onConflict: ["player_id", "prop_type"],
    });

  if (upsertError) {
    console.error("❌ Upsert failed:", upsertError.message);
  } else {
    console.log(`✅ Synced ${updates.length} player streak profiles.`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  syncStreakProfiles();
}
