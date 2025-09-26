// 📦 Only load dotenv in Node.js (backend) environments
// backend/scripts/shared/supabaseUtils.js

import { createClient } from "@supabase/supabase-js";
import {
  nowET,
  todayET,
  currentTimeET,
} from "../../../shared/timeUtilsBackend.js";

let supabaseUrl, supabaseKey;

// Only load dotenv and backend env vars in Node.js
if (typeof window === "undefined") {
  const dotenv = await import("dotenv");
  dotenv.config();

  supabaseUrl = process.env.SUPABASE_URL;
  supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
} else {
  // Frontend env vars (injected by Vite/Webpack)
  supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
  supabaseKey = process.env.REACT_APP_SUPABASE_ANON_KEY;
}

if (!supabaseUrl || !supabaseKey) {
  console.warn(
    "⚠️ Missing Supabase env vars. URL:",
    supabaseUrl,
    "KEY:",
    supabaseKey
  );
}

export const supabase =
  supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

// 📌 Fetch Resolved Props
export async function fetchResolvedProps() {
  const { data, error } = await supabase
    .from("player_props")
    .select("*")
    .eq("status", "resolved");

  if (error) {
    console.error("❌ Failed to fetch resolved props:", error.message);
    return [];
  }
  return data;
}

// 📌 Fetch Pending Props
export async function getPendingProps() {
  const { data, error } = await supabase
    .from("player_props")
    .select("*")
    .eq("status", "pending")
    .or(
      `game_date.lt.${todayET()},and(game_date.eq.${todayET()},game_time.lte.${currentTimeET()}),and(game_date.eq.${todayET()},game_time.is.null)`
    )
    .order("game_date", { ascending: false })
    .order("game_time", { ascending: false });

  if (error) {
    console.error("❌ Failed to fetch pending props:", error.message);
    return [];
  }

  return data.filter(
    (prop) =>
      prop.game_date < todayET() ||
      (prop.game_date === todayET() && prop.game_time <= currentTimeET())
  );
}

// 📌 Delete Old Pending Props
export async function expireOldPendingProps() {
  const twoDaysAgo = nowET().minus({ days: 2 }).toISODate();
  const { data, error } = await supabase
    .from("player_props")
    .delete()
    .eq("status", "pending")
    .lt("game_date", twoDaysAgo);

  if (error) {
    console.error("⚠️ Failed to delete old pending props:", error.message);
  } else {
    const deletedCount = data?.length || 0;
    console.log(`🧹 Deleted ${deletedCount} stale pending props.`);
  }
}

// 📌 Update Prop Statuses Using a Callback
export async function updatePropStatuses(updatePropStatusFn) {
  const props = await getPendingProps();
  console.log(`🔎 Found ${props.length} pending props to update.`);

  let updated = 0,
    skipped = 0,
    errors = 0;

  for (const prop of props) {
    try {
      const success = await updatePropStatusFn(prop);
      if (success) updated++;
      else skipped++;
    } catch (err) {
      console.error(`🔥 Error processing ${prop.player_name}:`, err.message);
      errors++;
    }
  }

  await expireOldPendingProps();

  console.log(
    `🏁 Status Update Complete — Updated: ${updated}, Skipped: ${skipped}, Errors: ${errors}`
  );
}

// 📌 Sync Resolved Props to Training Table
export async function syncTrainingData() {
  const resolvedProps = await fetchResolvedProps();

  for (const prop of resolvedProps) {
    const upsertData = {
      id: prop.id,
      game_date: prop.game_date,
      player_name: prop.player_name,
      team: prop.team,
      position: prop.position,
      prop_type: prop.prop_type,
      prop_value: prop.prop_value,
      result: prop.result,
      outcome: prop.outcome,
      is_pitcher: prop.is_pitcher,
      streak_count: prop.streak_count,
      over_under: prop.over_under,
      status: prop.status,
      game_id: prop.game_id,
      opponent: prop.opponent,
      home_away: prop.home_away,
      game_time: prop.game_time,
      player_id: prop.player_id,
      predicted_outcome: prop.predicted_outcome || null,
      confidence_score: prop.confidence_score || null,
      prediction_timestamp: prop.prediction_timestamp || null,
      was_correct: prop.was_correct || null,
      prop_source: prop.prop_source || "user-added",
    };

    const { error: upsertError } = await supabase
      .from("model_training_props")
      .upsert(upsertData, { onConflict: ["id"] });

    if (upsertError) {
      console.error(
        `❌ Failed to upsert prop ${prop.id}:`,
        upsertError.message
      );
    } else {
      console.log(`✅ Synced prop ${prop.id} to model_training_props`);
    }
  }
}

// 📌 NEW: Fetch Recent Props for Feature Engineering
export async function fetchRecentProps(
  player_name,
  prop_type,
  dateISO,
  limit = 7
) {
  const { data, error } = await supabase
    .from("player_props")
    .select("outcome")
    .eq("player_name", player_name)
    .eq("prop_type", prop_type)
    .lt("game_date", dateISO)
    .order("game_date", { ascending: false })
    .limit(limit);

  if (error) {
    console.error(
      `❌ Failed to fetch recent props for ${player_name}:`,
      error.message
    );
    return [];
  }
  return data;
}

// 📌 NEW: Fetch Opponent-Level Outcomes for Feature Engineering
export async function fetchOpponentGames(
  player_name,
  prop_type,
  opponent,
  dateISO,
  limit = 5
) {
  const { data, error } = await supabase
    .from("player_props")
    .select("outcome")
    .eq("player_name", player_name)
    .eq("prop_type", prop_type)
    .eq("opponent", opponent)
    .lt("game_date", dateISO)
    .order("game_date", { ascending: false })
    .limit(limit);

  if (error) {
    console.error(
      `❌ Failed to fetch opponent games for ${player_name} vs ${opponent}:`,
      error.message
    );
    return [];
  }
  return data;
}
