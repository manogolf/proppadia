// src/shared/resolvePlayerAndTeam.js

import { supabase } from "../utils/supabaseFrontend.js";
import {
  normalizeTeamAbbreviation,
  getTeamIdFromAbbr,
} from "../../shared/teamNameMap.js";

/**
 * Normalize player names (remove accents and lowercase).
 */
function normalizeName(name) {
  return name
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

/**
 * Resolve player_id from player_ids or fallback to MT by name.
 */
export async function resolvePlayerId({ player_id, player_name }) {
  if (!player_id && !player_name) {
    console.warn("❌ Missing player_id and player_name.");
    return null;
  }

  if (player_id) {
    const { data, error } = await supabase
      .from("player_ids")
      .select("player_id")
      .eq("player_id", player_id)
      .maybeSingle();

    if (data?.player_id) return data.player_id;
  }

  if (player_name) {
    const { data, error } = await supabase
      .from("model_training_props")
      .select("player_id, player_name")
      .order("game_date", { ascending: false })
      .limit(50); // short recent window

    const match = data?.find(
      (row) => normalizeName(row.player_name) === normalizeName(player_name)
    );

    if (match?.player_id) {
      console.warn("⚠️ Resolved player_id via MT fallback");
      return match.player_id;
    }
  }

  console.warn("❌ Could not resolve player_id");
  return null;
}

/**
 * Resolve team_id from player_ids (preferred), fallback to MT.
 */
export async function resolveTeamId(player_id) {
  if (!player_id) {
    console.warn("❌ Missing player_id when resolving team_id.");
    return null;
  }

  // Preferred: player_ids
  const { data: ids, error: idsError } = await supabase
    .from("player_ids")
    .select("team_id")
    .eq("player_id", player_id)
    .maybeSingle();

  if (ids?.team_id) return ids.team_id;

  // Fallback: model_training_props
  const { data: mt, error: mtError } = await supabase
    .from("model_training_props")
    .select("team_id")
    .eq("player_id", player_id)
    .filter("team_id", "not.is", null)
    .order("game_date", { ascending: false })
    .limit(1);

  if (mt?.[0]?.team_id) return mt[0].team_id;

  console.warn(`⚠️ Could not resolve team_id for player ${player_id}`);
  return null;
}

/**
 * Unified resolver for both player_id and team_id.
 */
export async function resolvePlayerAndTeam({
  player_id,
  player_name,
  team_abbr,
}) {
  const resolvedPlayerId = await resolvePlayerId({ player_id, player_name });
  if (!resolvedPlayerId) return { player_id: null, team_id: null };

  const resolvedTeamId = await resolveTeamId(resolvedPlayerId);

  // Optionally override with user-provided team_abbr if team_id is still missing
  if (!resolvedTeamId && team_abbr) {
    const normalizedAbbr = normalizeTeamAbbreviation(team_abbr);
    const fallbackId = getTeamIdFromAbbr(normalizedAbbr);
    if (fallbackId) {
      console.warn(
        "⚠️ Used team_abbr fallback to resolve team_id:",
        fallbackId
      );
      return { player_id: resolvedPlayerId, team_id: fallbackId };
    }
  }

  return { player_id: resolvedPlayerId, team_id: resolvedTeamId };
}
