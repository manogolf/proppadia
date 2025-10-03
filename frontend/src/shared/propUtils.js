// shared/propUtils.js

import { toISODate, todayET } from "./timeUtils.js";

// ✅ Canonical list of supported prop types
export const VALID_PROP_TYPES = [
  "hits",
  "runs_scored",
  "rbis",
  "home_runs",
  "singles",
  "doubles",
  "triples",
  "walks",
  "strikeouts_batting",
  "stolen_bases",
  "total_bases",
  "hits_runs_rbis",
  "runs_rbis",
  "outs_recorded",
  "strikeouts_pitching",
  "walks_allowed",
  "earned_runs",
  "hits_allowed",
];

// ✅ Human-readable labels for prop types
const DISPLAY_LABELS = {
  hits: "Hits",
  runs_scored: "Runs Scored",
  rbis: "RBIs",
  home_runs: "Home Runs",
  singles: "Singles",
  doubles: "Doubles",
  triples: "Triples",
  walks: "Walks",
  strikeouts_batting: "Strikeouts (Batting)",
  stolen_bases: "Stolen Bases",
  total_bases: "Total Bases",
  hits_runs_rbis: "Hits + Runs + RBIs",
  runs_rbis: "Runs + RBIs",
  outs_recorded: "Outs Recorded",
  strikeouts_pitching: "Strikeouts (Pitching)",
  walks_allowed: "Walks Allowed",
  earned_runs: "Earned Runs",
  hits_allowed: "Hits Allowed",
};

export function normalizePropType(label) {
  return label.toLowerCase().replace(/[()]/g, "").replace(/\s+/g, "_");
}

export function getPropDisplayLabel(propType) {
  return DISPLAY_LABELS[propType] || propType;
}

export function getPropTypeOptions() {
  return VALID_PROP_TYPES.map((propType) => ({
    value: propType,
    label: getPropDisplayLabel(propType),
  })).sort((a, b) => a.label.localeCompare(b.label));
}

export function expireOldPendingProps(props = []) {
  const todayISO = toISODate(todayET());
  return props.map((prop) => {
    const propDate = toISODate(prop.game_date);
    if (prop.status === "pending" && propDate < todayISO) {
      return { ...prop, status: "expired" };
    }
    return prop;
  });
}

export function determineStatus(actual, line, overUnder) {
  const direction = overUnder?.toLowerCase?.();

  if (typeof actual !== "number" || typeof line !== "number" || !direction) {
    return "invalid";
  }

  if (actual === line) return "push";

  const isWin =
    (direction === "over" && actual > line) ||
    (direction === "under" && actual < line);

  return isWin ? "win" : "loss";
}

export async function determineHomeAway(supabase, team, gameId) {
  const { data, error } = await supabase
    .from("player_props")
    .select("team, is_home, game_id")
    .eq("team", team)
    .eq("game_id", gameId)
    .limit(1)
    .maybeSingle();
  return error || !data ? null : data.is_home;
}

export async function determineOpponent(supabase, team, gameId) {
  const { data, error } = await supabase
    .from("player_props")
    .select("team")
    .eq("game_id", gameId)
    .neq("team", team)
    .limit(1)
    .maybeSingle();
  return error || !data ? null : data.team;
}

export async function getRollingAverage(
  supabase,
  playerId,
  propType,
  gameDate,
  gameId,
  days = 7
) {
  const { data, error } = await supabase
    .from("model_training_props")
    .select("result, game_date, game_id")
    .eq("player_id", playerId)
    .eq("prop_type", propType)
    .lt("game_date", gameDate)
    .order("game_date", { ascending: false });

  if (error) {
    console.warn(`❌ Supabase error in getRollingAverage: ${error.message}`);
    return null;
  }

  if (!data || data.length === 0) {
    console.log(
      `🟡 No prior games for ${playerId} | ${propType} before ${gameDate}`
    );
    return null;
  }

  const filtered = data
    .filter((row) => row.game_id !== gameId)
    .slice(0, days)
    .map((row) => parseFloat(row.result))
    .filter((v) => !isNaN(v));

  if (filtered.length === 0) {
    console.log(`🟡 No valid numeric results for ${playerId} | ${propType}`);
    return null;
  }

  const sum = filtered.reduce((acc, v) => acc + v, 0);
  const avg = parseFloat((sum / filtered.length).toFixed(2));

  return avg;
}

export const BATTER_PROP_TYPES = [
  "hits",
  "doubles",
  "triples",
  "home_runs",
  "rbis",
  "runs_scored",
  "strikeouts_batting",
  "walks",
  "stolen_bases",
  "total_bases",
  "hits_runs_rbis",
  "runs_rbis",
  "singles",
];

export const PITCHER_PROP_TYPES = [
  "strikeouts_pitching",
  "outs_recorded",
  "walks_allowed",
  "hits_allowed",
  "earned_runs",
];

export function isBatterProp(propType) {
  return BATTER_PROP_TYPES.includes(propType);
}

export function isPitcherProp(propType) {
  return PITCHER_PROP_TYPES.includes(propType);
}

export function determineOutcome(propValue, line, overUnder) {
  if (propValue === null || line === null || overUnder === null) return null;
  if (overUnder === "over") return propValue > line ? "win" : "loss";
  if (overUnder === "under") return propValue < line ? "win" : "loss";
  return null;
}
