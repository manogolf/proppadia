// File: src/utils/buildFeatureVector.js

import { supabase } from "./supabaseFrontend.js";
//import { checkIfHome, getPlayerID } from "../shared/playerUtilsFrontend.js";
import { toISODate } from "../shared/timeUtils.js";
import { getOpponentAbbreviation } from "../shared/teamNameMap.js";
import yaml from "js-yaml";

const apiUrl = process.env.REACT_APP_API_URL || "http://localhost:3001";

// Load YAML spec
function loadFeatureSpec() {
  const file = fs.readFileSync("model_features.yaml", "utf8");
  const doc = yaml.load(file);
  return doc.features || {};
}

export async function buildFeatureVector({
  player_name,
  team,
  prop_type,
  prop_value,
  over_under,
  game_date,
}) {
  const dateISO = toISODate(game_date);
  const player_id = await getPlayerID(player_name, team);
  if (!player_id) return null;

  const spec = loadFeatureSpec();
  const vector = {
    prop_type,
    prop_value,
    over_under,
    player_id,
  };

  // 1. Resolve game ID, home/away, opponent
  let game_id = null;
  try {
    const res = await fetch(`${apiUrl}/api/getGamePk`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ team, game_date }),
    });
    const { gamePk } = await res.json();
    game_id = gamePk;
    vector.is_home = (await checkIfHome(team, game_id)) ? 1 : 0;
  } catch {
    vector.is_home = null;
  }

  const opponent = await getOpponentAbbreviation(team, game_id);
  vector.opponent = opponent;

  // 2. opponent_win_rate
  try {
    const { data = [] } = await supabase
      .from("model_training_props")
      .select("outcome")
      .eq("player_id", player_id)
      .eq("prop_type", prop_type)
      .eq("opponent", opponent)
      .lt("game_date", dateISO)
      .limit(5);
    const wins = data.filter((d) => d.outcome === "win").length;
    vector.opponent_win_rate = data.length ? wins / data.length : 0.5;
  } catch {
    vector.opponent_win_rate = null;
  }

  // 3. opponent_avg_win_rate
  try {
    const { data = [] } = await supabase
      .from("model_training_props")
      .select("outcome")
      .eq("prop_type", prop_type)
      .eq("opponent", opponent)
      .lt("game_date", dateISO);
    const wins = data.filter((d) => d.outcome === "win").length;
    vector.opponent_avg_win_rate = data.length ? wins / data.length : 0.5;
  } catch {
    vector.opponent_avg_win_rate = null;
  }

  // 4. rolling_result_avg_7 + streaks
  try {
    const { data = [] } = await supabase
      .from("model_training_props")
      .select("outcome")
      .eq("player_id", player_id)
      .eq("prop_type", prop_type)
      .lt("game_date", dateISO)
      .order("game_date", { ascending: false })
      .limit(7);
    const wins = data.filter((p) => p.outcome === "win").length;
    vector.rolling_result_avg_7 = data.length ? wins / data.length : 0.5;
    vector.hit_streak = 0;
    vector.win_streak = 0;
    for (const prop of data) {
      if (prop.outcome === "win") {
        vector.hit_streak++;
        vector.win_streak++;
      } else break;
    }
  } catch {
    vector.rolling_result_avg_7 = null;
    vector.hit_streak = 0;
    vector.win_streak = 0;
  }

  // 5. BvP stats from bvp_stats
  try {
    const { data = [] } = await supabase
      .from("bvp_stats")
      .select("*")
      .eq("batter_id", player_id)
      .eq("game_id", game_id);
    const bvp = data[0] || {};
    for (const key of Object.keys(spec)) {
      if (key.startsWith("bvp_")) {
        vector[key] = bvp[key] ?? null;
      }
    }
  } catch {}

  // 6. Derived stats
  try {
    const { data = [] } = await supabase
      .from("player_derived_stats")
      .select("*")
      .eq("player_id", player_id)
      .eq("game_id", game_id);
    const derived = data[0] || {};
    for (const key of Object.keys(spec)) {
      if (
        key.startsWith("d7_") ||
        key.startsWith("d15_") ||
        key.startsWith("d30_")
      ) {
        vector[key] = derived[key] ?? null;
      }
    }
  } catch {}

  // 7. Ensure all spec fields are present
  for (const field of Object.keys(spec)) {
    if (!(field in vector)) {
      vector[field] = null;
    }
  }

  return vector;
}
