// backend/routes/playerProfile.js

import { Router } from "express";
import { supabase } from "@shared/supabaseBackend.js";

const router = Router();

// GET /player-profile/:player_id
router.get("/:player_id", async (req, res) => {
  const { player_id } = req.params;

  try {
    // ✅ Basic Info: Name, Team, Position
    const { data: playerRows, error: playerError } = await supabase
      .from("player_props")
      .select("player_name, team, position")
      .eq("player_id", player_id)
      .order("game_date", { ascending: false })
      .limit(1);

    if (playerError || !playerRows?.length) {
      return res.status(404).json({ error: "Player not found." });
    }

    const { player_name, team, position } = playerRows[0];

    // ✅ Recent Props (last 10 games)
    const { data: recentProps, error: recentError } = await supabase
      .from("player_props")
      .select("game_date, prop_type, result, outcome, over_under, prop_value")
      .eq("player_id", player_id)
      .order("game_date", { ascending: false })
      .limit(10);

    if (recentError) {
      throw recentError;
    }

    // ✅ Profile Summary: Streak count and type
    const { data: profile, error: profileError } = await supabase
      .from("player_streak_profiles")
      .select("streak_count, streak_type")
      .eq("player_id", player_id)
      .maybeSingle();

    // ✅ Final response payload
    return res.json({
      player_id,
      player_name,
      team,
      position,
      streak: profile || { streak_count: 0, streak_type: "neutral" },
      recent_props: recentProps || [],
    });
  } catch (err) {
    console.error("❌ Error in /player-profile/:player_id:", err);
    return res.status(500).json({ error: "Internal server error." });
  }
});

export default router;
