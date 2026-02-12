// backend/scripts/shared/modelTrainingUtils.js
import crypto from "node:crypto";
import { supabase } from "./supabaseBackend.js";
import {
  normalizePropType,
  getRollingAverage,
  determineOpponent,
} from "../../../shared/propUtils.js";
import { getStreaksForPlayer } from "./playerUtilsBackend.js";
import { getTeamIdFromAbbr } from "../../../shared/teamNameMap.js";

/** Sample MT columns so we only send valid keys (prevents PostgREST errors). */
async function getModelTrainingPropsColumns() {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("*")
    .limit(1);

  if (error) {
    console.warn(
      "⚠️ Could not sample model_training_props schema:",
      error.message
    );
    // Reasonable fallback; adjust if your table has more fields
    return new Set([
      "id",
      "game_id",
      "player_id",
      "player_name",
      "team",
      "team_id",
      "opponent",
      "opponent_team_id",
      "opponent_encoded",
      "position",
      "is_home",
      "is_pitcher",
      "prop_type",
      "prop_value",
      "line",
      "over_under",
      "status",
      "outcome",
      "was_correct",
      "predicted_outcome",
      "confidence_score",
      "prediction_timestamp",
      "prop_source",
      "game_date",
      "rolling_result_avg_7",
      "line_diff",
      "hit_streak",
      "win_streak",
      "created_at",
      "updated_at",
    ]);
  }
  if (!data || !data[0]) return new Set(); // empty table → allow all keys
  return new Set(Object.keys(data[0]));
}

function pick(obj, allowed) {
  if (!allowed) return { ...obj };
  const out = {};
  for (const k of Object.keys(obj)) {
    if (allowed.has(k)) out[k] = obj[k];
  }
  return out;
}

/**
 * Upsert all user-added props from `player_props` into `model_training_props`.
 *
 * @param {object} opts
 * @param {number} opts.batchSize – rows per page (default 1000)
 * @param {number} opts.daysBack  – look-back window on player_props.created_at (0 = all)
 */
export async function upsertUserPropsToTraining(opts = {}) {
  const { batchSize = 1_000, daysBack = 0 } = opts;

  console.log(
    `🔁 Re-syncing user-added props into model_training_props (batch ${batchSize}, daysBack ${daysBack})`
  );

  const allowedCols = await getModelTrainingPropsColumns();
  const nowIso = new Date().toISOString();

  // Optional created_at cutoff (YYYY-MM-DD is fine for timestamptz comparisons)
  let createdCutoff = null;
  if (daysBack > 0) {
    createdCutoff = new Date(Date.now() - daysBack * 86_400_000)
      .toISOString()
      .slice(0, 10);
  }

  let offset = 0;
  let totalProcessed = 0;
  let totalInserted = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  const t0 = Date.now();

  while (true) {
    // Fetch a page from player_props — NOTE: no `line` in the select
    let query = supabase
      .from("player_props")
      .select(
        `
          id,
          game_id,
          game_date,
          player_id,
          player_name,
          team,
          position,
          is_home,
          is_pitcher,
          prop_type,
          prop_value,
          over_under,
          status,
          outcome,
          was_correct,
          predicted_outcome,
          confidence_score,
          prediction_timestamp,
          prop_source,
          opponent,
          opponent_encoded,
          team_id,
          opponent_team_id,
          created_at
        `
      )
      .eq("prop_source", "user_added")
      .in("status", ["win", "loss"])
      .neq("outcome", "dnp")
      .order("created_at", { ascending: true })
      .range(offset, offset + batchSize - 1);

    if (createdCutoff) query = query.gte("created_at", createdCutoff);

    const { data: props, error } = await query;

    if (error) {
      console.error("❌ Fetch error:", error.message);
      break;
    }
    if (!props?.length) {
      console.log("✅ Sync complete - no more rows.");
      break;
    }

    const rowsPrepared = [];

    for (const p of props) {
      if (!p?.player_id || !p?.game_date) {
        totalSkipped++;
        continue;
      }
      if (p.status !== "win" && p.status !== "loss") {
        totalSkipped++;
        continue;
      }
      if (p.outcome === "dnp") {
        totalSkipped++;
        continue;
      }

      const propTypeNorm = normalizePropType(p.prop_type);

      // Rolling average (last 7 games up to this game)
      let rollingAvg = null;
      try {
        rollingAvg = await getRollingAverage(
          supabase,
          p.player_id,
          propTypeNorm,
          p.game_date,
          p.game_id,
          7
        );
      } catch {
        /* ignore rolling avg errors */
      }

      const lineDiff =
        typeof rollingAvg === "number" && typeof p.prop_value === "number"
          ? rollingAvg - p.prop_value
          : null;

      // Streaks (may be missing for some props)
      let streaks = {};
      try {
        streaks =
          (await getStreaksForPlayer(supabase, p.player_id, propTypeNorm)) ||
          {};
      } catch {
        /* ignore streak errors */
      }

      // Opponent
      let opponent = p.opponent ?? null;
      try {
        if (!opponent) {
          opponent = await determineOpponent(supabase, p.player_id, p.game_id);
        }
      } catch {
        /* ignore opponent errors */
      }
      const opponent_encoded =
        p.opponent_encoded ?? (opponent ? getTeamIdFromAbbr(opponent) : null);

      // Team IDs (best-effort from abbrs if missing)
      const team_id =
        p.team_id ??
        (typeof p.team === "string" && p.team.length <= 3
          ? String(getTeamIdFromAbbr(p.team) ?? "")
          : null);

      const opponent_team_id =
        p.opponent_team_id ??
        (opponent ? String(getTeamIdFromAbbr(opponent) ?? "") : null);

      // Build MT row
      const base = {
        // identity
        game_id: p.game_id,
        player_id: String(p.player_id),
        player_name: p.player_name ?? null,

        // team info (keep abbrs too)
        team: p.team ?? null,
        team_id: team_id || null,
        opponent: opponent ?? null,
        opponent_team_id: opponent_team_id || null,
        opponent_encoded: opponent_encoded ?? null,

        position: p.position ?? null,
        is_home: p.is_home ?? null,
        is_pitcher: p.is_pitcher ?? null,

        // prop
        prop_type: propTypeNorm,
        prop_value: p.prop_value ?? null,
        // 🔧 user-added rows don’t have a separate `line`; use prop_value
        line: p.line ?? p.prop_value ?? null,
        over_under: p.over_under ?? null,

        // outcomes
        status: p.status ?? null,
        outcome: p.outcome ?? null,
        was_correct: p.was_correct ?? null,
        predicted_outcome: p.predicted_outcome ?? null,
        confidence_score: p.confidence_score ?? null,
        prediction_timestamp: p.prediction_timestamp ?? null,

        // meta
        prop_source: "user_added",
        game_date: p.game_date,

        // enrichments
        rolling_result_avg_7: rollingAvg ?? null,
        line_diff: lineDiff,
        hit_streak: streaks?.hit_streak ?? null,
        win_streak: streaks?.win_streak ?? null,

        // timestamps
        created_at: p.created_at ?? nowIso,
        updated_at: nowIso, // filtered out if MT doesn't have this column
      };

      rowsPrepared.push(base);
    }

    // Upsert one-by-one to avoid changing IDs on existing rows
    for (const row of rowsPrepared) {
      // does a row already exist?
      const { data: existingRows, error: existErr } = await supabase
        .from("model_training_props")
        .select("id")
        .eq("player_id", row.player_id)
        .eq("game_id", row.game_id)
        .eq("prop_type", row.prop_type)
        .eq("prop_source", "user_added")
        .limit(1);

      if (existErr) {
        console.error("❌ Precheck error:", existErr.message);
        totalSkipped++;
        continue;
      }

      const exists = Array.isArray(existingRows) && existingRows.length > 0;
      const payload = exists ? { ...row } : { id: crypto.randomUUID(), ...row }; // MT id is uuid

      const allowedPayload = pick(
        payload,
        allowedCols.size ? allowedCols : null
      );

      const { error: upsertErr } = await supabase
        .from("model_training_props")
        .upsert(allowedPayload, {
          onConflict: "player_id,game_id,prop_type,prop_source",
        });

      if (upsertErr) {
        console.error("❌ Upsert error:", upsertErr.message, "row:", {
          player_id: row.player_id,
          game_id: row.game_id,
          prop_type: row.prop_type,
        });
        totalSkipped++;
      } else {
        if (exists) totalUpdated++;
        else totalInserted++;
        totalProcessed++;
      }
    }

    console.log(
      `📦 Page done (offset ${offset}) — fetched=${props.length}, prepared=${rowsPrepared.length}, totalProcessed=${totalProcessed}`
    );

    offset += batchSize;
  }

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(
    `🎉 Sync finished. Processed=${totalProcessed} (Inserted=${totalInserted}, Updated=${totalUpdated}, Skipped=${totalSkipped}) in ${secs}s`
  );
}
