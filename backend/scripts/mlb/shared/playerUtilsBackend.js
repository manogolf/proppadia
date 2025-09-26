// ✅ File: backend/scripts/shared/playerUtilsBackend.js (backend-only functions)

import fetch from "node-fetch";
import { getGamePkForTeamOnDate } from "./fetchGameID.js";
import { getLiveFeedFromGameID } from "./mlbApiUtils.js";
import {
  getFullTeamAbbreviationFromID,
  getTeamInfoByAbbr,
} from "../../../shared/teamNameMap.js";
import { normalizePropType } from "./propUtilsBackend.js";
import { toISODate } from "./timeUtilsBackend.js";
import { supabase } from "./supabaseBackend.js";
import { upsertPlayerID } from "./upsertPlayerID.js";

const missingStreakCache = new Set();

export async function preparePropSubmission({
  supabase,
  player_name,
  team,
  prop_type,
  prop_value,
  over_under,
  game_date,
  game_time = null,
}) {
  const normalizedPropType = normalizePropType(prop_type);
  const dateISO = toISODate(game_date);
  const game_id = await getGamePkForTeamOnDate(team, dateISO);
  const player_id = await getPlayerID(supabase, player_name, team, game_id);

  return {
    player_name,
    team,
    prop_type: normalizedPropType,
    prop_value: parseFloat(prop_value),
    over_under: over_under.toLowerCase(),
    game_date: dateISO,
    game_time,
    game_id,
    player_id: String(player_id),
    prop_source: "user_added",
  };
}

export async function getPlayerStatsFromBoxscore({ game_id, player_id }) {
  const url = `https://statsapi.mlb.com/api/v1/game/${game_id}/boxscore`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    const allPlayers = {
      ...data.teams?.home?.players,
      ...data.teams?.away?.players,
    };
    return Object.values(allPlayers).find(
      (p) => String(p.person?.id) === String(player_id)
    );
  } catch (err) {
    console.error("❌ Error fetching boxscore:", err.message);
    return null;
  }
}

export async function getPlayerID(supabase, playerName, teamAbbr) {
  console.log("🔁 getPlayerID full logic:", { playerName, teamAbbr });

  // 1. Try `player_ids`
  const { data: direct, error: directErr } = await supabase
    .from("player_ids")
    .select("player_id")
    .eq("player_name", playerName)
    .eq("team_abbr", teamAbbr)
    .limit(1);

  if (directErr) {
    console.error("❌ Error querying player_ids:", directErr);
  } else if (direct?.length) {
    console.log("✅ Found in player_ids:", direct[0].player_id);
    return direct[0].player_id;
  } else {
    console.warn("⚠️ Not found in player_ids");
  }

  // 2. Fallback: `model_training_props`
  const { data: fromProps, error: propsErr } = await supabase
    .from("model_training_props")
    .select("player_id")
    .eq("player_name", playerName)
    .eq("team", teamAbbr)
    .limit(1);

  if (propsErr) {
    console.error("❌ Error querying model_training_props:", propsErr);
  } else if (fromProps?.length) {
    console.log("✅ Found in model_training_props:", fromProps[0].player_id);
    return fromProps[0].player_id;
  } else {
    console.warn("⚠️ Not found in model_training_props either.");
  }

  // 3. Final fallback: Query MLB API team roster
  try {
    const teamInfo = getTeamInfoByAbbr(teamAbbr);
    if (!teamInfo?.id) {
      console.warn(`⚠️ No team ID found for abbreviation ${teamAbbr}`);
      return null;
    }

    const url = `https://statsapi.mlb.com/api/v1/teams/${teamInfo.id}/roster`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const json = await res.json();
    const match = json.roster?.find((p) => p.person?.fullName === playerName);
    if (match) {
      const player_id = match.person.id;
      await upsertPlayerID(supabase, {
        player_name: playerName,
        player_id,
        team_abbr: teamAbbr,
      });
      console.log(`🆕 Inserted player ID from MLB API for ${playerName}`);
      return player_id;
    }
  } catch (err) {
    console.error("❌ MLB API fallback failed:", err.message);
  }

  console.warn(
    `❌ getPlayerID: No match found for ${playerName} (${teamAbbr})`
  );
  return null;
}

export async function getOpponentAbbreviation(teamAbbr, gameId) {
  const feed = await getLiveFeedFromGameID(gameId);
  const homeId = feed?.teams?.home?.team?.id;
  const awayId = feed?.teams?.away?.team?.id;
  const homeAbbr = getFullTeamAbbreviationFromID(homeId);
  const awayAbbr = getFullTeamAbbreviationFromID(awayId);
  if (homeAbbr === teamAbbr) return awayAbbr;
  if (awayAbbr === teamAbbr) return homeAbbr;
  return null;
}

export async function getStreaksForPlayer(
  supabase,
  player_id,
  prop_type,
  prop_source = "mlb_api"
) {
  const key = `${player_id}:${prop_type}:${prop_source}`;
  if (missingStreakCache.has(key)) return null;

  const { data, error } = await supabase
    .from("player_streak_profiles")
    .select("streak_count, streak_type")
    .eq("player_id", player_id)
    .eq("prop_type", prop_type)
    .eq("prop_source", prop_source) // 🔥 CRITICAL!
    .single();

  if (error || !data) {
    missingStreakCache.add(key);
    return null;
  }
}
export function flattenPlayerBoxscore(boxscore, gameData) {
  const stats = boxscore?.stats || {};
  const flattened = { ...stats };

  if (stats.pitching) Object.assign(flattened, stats.pitching);
  if (stats.batting) Object.assign(flattened, stats.batting);
  if (stats.fielding) Object.assign(flattened, stats.fielding);
  if (stats.running) Object.assign(flattened, stats.running);

  flattened.player_id = boxscore?.person?.id;
  flattened.team_abbr = getTeamAbbreviationFromBoxscore(boxscore, gameData);
  return flattened;
}

// 🔁 Utility: Get position map from recent player_stats
// Maps player_id => position string (e.g., "P", "C", "1B", etc.)
export async function getPlayerPositionMap(dateStr = null) {
  const query = supabase.from("player_stats").select("player_id, position");
  if (dateStr) query.eq("game_date", dateStr);

  const { data, error } = await query;

  if (error) {
    console.error("❌ Failed to fetch player positions:", error.message);
    return {};
  }

  const map = new Map();
  for (const row of data || []) {
    if (!map.has(row.player_id) && row.position) {
      map.set(row.player_id, row.position);
    }
  }
  return map;
}

// ✅ Utility: Determine if a position string is a pitcher
export function isPitcher(position) {
  if (!position) return false;
  const pos = position.toUpperCase();
  return pos === "P" || pos === "SP" || pos === "RP";
}

// ✅ New: true only for the starting pitcher
export function isStarterPitcher(position, stats) {
  const pos = (position || "").toUpperCase();
  const gamesStarted = Number(stats?.pitching?.gamesStarted ?? 0);
  // If StatsAPI flags a start, trust it.
  if (gamesStarted > 0) return true;
  // Fallback: some feeds tag starters as "SP" even if gamesStarted is missing.
  if (pos === "SP") return true;
  return false;
}

export function getPlayerTeamFromBoxscoreData(player, gameData) {
  const homePlayers = gameData?.teams?.home?.players || {};
  const awayPlayers = gameData?.teams?.away?.players || {};
  const pid = player?.person?.id;

  for (const [_, p] of Object.entries(homePlayers)) {
    if (p?.person?.id === pid) return "home";
  }
  for (const [_, p] of Object.entries(awayPlayers)) {
    if (p?.person?.id === pid) return "away";
  }
  return null;
}

export function getTeamAbbreviationFromBoxscore(player, gameData) {
  const side = getPlayerTeamFromBoxscoreData(player, gameData);
  if (!side) return null;
  return (
    gameData?.teams?.[side]?.team?.abbreviation ||
    gameData?.teams?.[side]?.team?.triCode ||
    null
  );
}

export function didPlayerParticipate(stats) {
  if (!stats || typeof stats !== "object") return false;

  const hasBattingStats =
    stats.batting &&
    Object.values(stats.batting).some((v) => typeof v === "number" && v > 0);

  const hasPitchingStats =
    stats.pitching &&
    Object.values(stats.pitching).some((v) => typeof v === "number" && v > 0);

  return hasBattingStats || hasPitchingStats;
}
