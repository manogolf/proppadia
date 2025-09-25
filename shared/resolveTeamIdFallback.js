// shared/resolveTeamIdFallback.js
import { getTeamIdFromAbbr } from "./teamNameMap.js";

/**
 * Safely resolve team_id using all known fallbacks.
 * Logs each attempt.
 */
export function resolveTeamId({ context = {}, formData = {}, prepared = {} }) {
  const candidates = [
    { label: "context.team_id", value: context.team_id },
    { label: "formData.team", value: getTeamIdFromAbbr(formData.team) },
    { label: "prepared.team", value: getTeamIdFromAbbr(prepared.team) },
    { label: "context.teamAbbr", value: getTeamIdFromAbbr(context.teamAbbr) },
    { label: "context.team", value: getTeamIdFromAbbr(context.team) },
  ];

  for (const { label, value } of candidates) {
    if (value) {
      console.log(`✅ Resolved team_id from ${label} → ${value}`);
      return value;
    } else {
      console.warn(`⚠️ Could not resolve from ${label}`);
    }
  }

  console.error("❌ Failed to resolve team_id from all sources.");
  return null;
}
