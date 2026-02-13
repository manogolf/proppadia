// shared/resolveTeamIdFallback.js
import { getTeamIdFromAbbr } from "./teamNameMap.jsx";

/** Safely resolve team_id using all known fallbacks. */
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
      return value;
    }
  }
  return null;
}
