// src/shared/resolvePlayerAndTeam.js

import { playersAPI } from "../lib/api.js";
import { normalizeTeamAbbreviation, getTeamIdFromAbbr } from "./teamNameMap.jsx";

/**
 * Resolve player identity via backend-owned MLB endpoints.
 */
export async function resolvePlayerId({ player_id, player_name }) {
  if (!player_id && !player_name) {
    console.warn("❌ Missing player_id and player_name.");
    return null;
  }

  if (player_id) {
    const lookup = await playersAPI.lookup(player_id).catch(() => null);
    if (lookup?.ok && lookup?.found && lookup?.player_id) return lookup.player_id;
  }

  if (player_name) {
    const resolved = await playersAPI
      .resolve({ player_name })
      .catch(() => null);
    if (resolved?.ok && resolved?.found && resolved?.player_id) {
      return resolved.player_id;
    }
  }

  console.warn("❌ Could not resolve player_id");
  return null;
}

/**
 * Resolve team_id via backend-owned MLB player lookup.
 */
export async function resolveTeamId(player_id) {
  if (!player_id) {
    console.warn("❌ Missing player_id when resolving team_id.");
    return null;
  }

  const lookup = await playersAPI.lookup(player_id).catch(() => null);
  if (lookup?.ok && lookup?.found && lookup?.team_id) return lookup.team_id;

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
