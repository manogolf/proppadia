/**
 * syntheticLineUtils.js
 * ------------------------------------------------------------
 * Central hub for *all* synthetic-line logic:
 *   1.  getSyntheticLineFromUserProps → median of recent user-added lines
 *   2.  getStaticFallbackLine        → hard-coded defaults (last resort)
 *   3.  buildSyntheticLine           → smart season-average fallback
 *
 *  >  Import only what you need.  Nothing else in the project
 *     should call getSyntheticLine() from propUtils any more.
 * ------------------------------------------------------------
 */

import { supabase } from "./supabaseBackend.js";
import { toISODate } from "../shared/timeUtilsBackend.js";

/* -----------------------------------------------------------
   1)  Median of user-added lines from the last N days
----------------------------------------------------------- */
export async function getSyntheticLine(propType, daysBack = 60, limit = 1000) {
  const cutoffDate = toISODate(new Date(Date.now() - daysBack * 864e5));

  const { data, error } = await supabase
    .from("player_props")
    .select("prop_value")
    .eq("prop_type", propType)
    .eq("prop_source", "user_added")
    .gte("game_date", cutoffDate)
    .order("game_date", { ascending: false })
    .limit(limit);

  if (error || !data?.length) {
    if (process.env.VERBOSE_SYNTHETIC !== "false") {
      //console.warn(`⚠️  No recent user lines for ${propType}; fallback.`);
    }
    return getStaticFallbackLine(propType);
  }

  const vals = data
    .map((d) => parseFloat(d.prop_value))
    .filter((v) => Number.isFinite(v));

  if (!vals.length) return getStaticFallbackLine(propType);

  vals.sort((a, b) => a - b);
  const mid = Math.floor(vals.length / 2);
  const median =
    vals.length % 2 === 0 ? (vals[mid - 1] + vals[mid]) / 2 : vals[mid];

  return median;
}

/* -----------------------------------------------------------
   2)  Hard-coded default lines (last resort)
----------------------------------------------------------- */
export function getStaticFallbackLine(propType) {
  const d = {
    hits: 1.5,
    home_runs: 0.5,
    rbis: 0.5,
    runs_scored: 0.5,
    strikeouts_batting: 1.5,
    walks: 0.5,
    total_bases: 1.5,
    hits_runs_rbis: 1.5,
    runs_rbis: 0.5,
    doubles: 0.5,
    triples: 0.5,
    stolen_bases: 0.5,
    walks_allowed: 1.5,
    hits_allowed: 4.5,
    earned_runs: 2.5,
    outs_recorded: 15.5,
    strikeouts_pitching: 4.5,
    singles: 0.5,
  };
  return d[propType] ?? 1.0;
}

/* -----------------------------------------------------------
   3)  Season-average-based synthetic line
       • Call this *after* trying real-odds APIs.
       • Requires playerId so it can query MLB StatsAPI.
----------------------------------------------------------- */
const seasonCache = new Map(); // playerId -> season stat block

export async function buildSyntheticLine(
  propType,
  playerId,
  legacyDefault = getStaticFallbackLine
) {
  // 3a. Try cache → StatsAPI
  let stats = seasonCache.get(playerId);
  if (!stats) {
    const url = `https://statsapi.mlb.com/api/v1/people/${playerId}/stats?stats=season&group=hitting,pitching`;
    const res = await fetch(url)
      .then((r) => r.json())
      .catch(() => null);
    stats = res?.stats?.[0]?.splits?.[0]?.stat ?? null;
    if (stats) seasonCache.set(playerId, stats);
  }

  // 3b. Map propType → season stat field
  const FIELD = {
    hits: "hits",
    runs_scored: "runs",
    rbis: "rbi",
    home_runs: "homeRuns",
    singles: null, // estimate below
    doubles: "doubles",
    triples: "triples",
    walks: "baseOnBalls",
    strikeouts_batting: "strikeOuts",
    stolen_bases: "stolenBases",
    total_bases: "totalBases",
    hits_runs_rbis: null,
    runs_rbis: null,
    // pitcher props
    outs_recorded: "outs",
    strikeouts_pitching: "strikeOuts",
    walks_allowed: "baseOnBalls",
    earned_runs: "earnedRuns",
    hits_allowed: "hits",
  };

  let avg = null;
  if (stats) {
    const gp = stats.gamesPlayed || 1; // avoid /0
    switch (propType) {
      case "hits_runs_rbis":
        avg = (stats.hits + stats.runs + stats.rbi) / gp;
        break;
      case "runs_rbis":
        avg = (stats.runs + stats.rbi) / gp;
        break;
      case "singles":
        avg =
          (stats.hits - stats.homeRuns - stats.doubles - stats.triples) / gp;
        break;
      default:
        avg = FIELD[propType] ? stats[FIELD[propType]] / gp : null;
    }
  }

  if (avg == null || !Number.isFinite(avg)) {
    console.warn(
      `⚠️  SyntheticLine: no season stats for ${propType}; using legacy default`
    );
    return legacyDefault(propType);
  }

  // round to nearest 0.5 and bump +0.5 so line > avg
  const rounded = Math.round(avg * 2) / 2;
  const synthetic = Math.max(0.5, rounded + 0.5);

  console.log(
    `🛠️  SyntheticLine ${propType} p${playerId} → ${synthetic.toFixed(
      1
    )} (avg ${avg.toFixed(2)})`
  );
  return synthetic;
}
