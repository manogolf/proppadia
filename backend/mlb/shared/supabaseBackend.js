// File: backend/scripts/shared/supabaseBackend.js

import { nowET, todayET } from "./timeUtilsBackend.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Optional dotenv load for local runs; render/github envs can rely on process env only.
try {
  const dotenv = await import("dotenv");
  if (dotenv?.default?.config) dotenv.default.config();
} catch {
  // no-op: dotenv is optional in this monorepo layout
}

function _stripQuotes(v) {
  const s = String(v ?? "").trim();
  if (
    (s.startsWith('"') && s.endsWith('"')) ||
    (s.startsWith("'") && s.endsWith("'"))
  ) {
    return s.slice(1, -1);
  }
  return s;
}

function _loadEnvFileIfPresent(envPath) {
  try {
    if (!fs.existsSync(envPath)) return;
    const raw = fs.readFileSync(envPath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const t = line.trim();
      if (!t || t.startsWith("#")) continue;
      const i = t.indexOf("=");
      if (i <= 0) continue;
      const k = t.slice(0, i).trim();
      if (!k || process.env[k] != null) continue;
      const v = _stripQuotes(t.slice(i + 1));
      process.env[k] = v;
    }
  } catch {
    // no-op
  }
}

// Fallback loader when dotenv package is unavailable.
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  const thisFile = fileURLToPath(import.meta.url);
  const repoRoot = path.resolve(path.dirname(thisFile), "../../..");
  const candidates = [
    path.join(repoRoot, ".env.local"),
    path.join(repoRoot, ".env"),
    path.join(repoRoot, "backend", ".env"),
    path.join(repoRoot, "mlb", ".env"),
  ];
  for (const p of candidates) _loadEnvFileIfPresent(p);
}

let createClient;
try {
  ({ createClient } = await import("@supabase/supabase-js"));
} catch {
  // Fallback for this repo where node deps are installed under frontend/.
  ({ createClient } = await import("../../../frontend/node_modules/@supabase/supabase-js/dist/module/index.js"));
}

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

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

// ─────────────────────────────────────────────
// Retain all backend helper functions below
// ─────────────────────────────────────────────

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

export async function getPendingProps() {
  // keep ET for the DATE gate (same as before)
  const today = todayET(); // e.g., "2025-08-12"

  // 🔑 Use a full ISO timestamp for the TIME gate. Works with timestamptz.
  const nowIso = new Date().toISOString(); // e.g., "2025-08-12T21:03:27.123Z"

  const { data, error } = await supabase
    .from("player_props")
    .select("*")
    .eq("status", "pending")
    .or(
      // game_date < today
      // OR (game_date = today AND game_time <= now)
      // OR (game_date = today AND game_time IS NULL)
      `game_date.lt.${today},and(game_date.eq.${today},game_time.lte.${nowIso}),and(game_date.eq.${today},game_time.is.null)`
    )
    .order("game_date", { ascending: false })
    .order("game_time", { ascending: false });

  if (error) {
    console.error("❌ Failed to fetch pending props:", {
      message: error.message,
      details: error.details,
      hint: error.hint,
      today,
      nowIso,
    });
    return [];
  }
  return data ?? [];
}

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
