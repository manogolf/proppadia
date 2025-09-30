import "dotenv/config";
import { supabase } from "../utils/supabaseUtils.js";
import fetch from "node-fetch";
import { getStatFromLiveFeed } from "./getStatFromLiveFeed.js";

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const MLB_API_BASE = "https://statsapi.mlb.com/api/v1";

function normalizeName(name) {
  return name.toLowerCase().replace(",", "").replace(/\./g, "").trim();
}

function findPlayerId(boxscore, targetName) {
  const players = boxscore?.teams?.home?.players || {};
  const awayPlayers = boxscore?.teams?.away?.players || {};
  const allPlayers = { ...players, ...awayPlayers };
  const normalizedTarget = normalizeName(targetName);

  for (const [id, info] of Object.entries(allPlayers)) {
    const name = normalizeName(info?.person?.fullName || "");
    if (name.includes(normalizedTarget)) {
      return id.replace("ID", "");
    }
  }
  return null;
}

function determineStatus(actual, line, overUnder) {
  if (actual === line) return "push";
  return (actual > line && overUnder === "over") ||
    (actual < line && overUnder === "under")
    ? "win"
    : "loss";
}

function getStatFromBoxscore(boxscore, playerId, propType) {
  const stats = boxscore?.players?.[`ID${playerId}`]?.stats;
  if (!stats) {
    console.warn(`⚠️ No stats found for player ID: ${playerId}`);
    return null;
  }

  const batting = stats.batting || {};
  const pitching = stats.pitching || {};
  const singles =
    (batting.hits ?? 0) -
    (batting.doubles ?? 0) -
    (batting.triples ?? 0) -
    (batting.homeRuns ?? 0);
  const outsRecorded = pitching?.inningsPitched
    ? Math.floor(parseFloat(pitching.inningsPitched) * 3)
    : 0;

  switch (propType) {
    case "hits":
      return batting.hits ?? 0;
    case "strikeouts":
    case "Strikeouts (Pitching)":
      return pitching.strikeOuts ?? 0;
    case "homeRuns":
      return batting.homeRuns ?? 0;
    case "walks":
      return batting.baseOnBalls ?? 0;
    case "Total Bases":
      return (
        singles * 1 +
        (batting.doubles ?? 0) * 2 +
        (batting.triples ?? 0) * 3 +
        (batting.homeRuns ?? 0) * 4
      );
    case "Hits + Runs + RBIs":
      return (batting.hits ?? 0) + (batting.runs ?? 0) + (batting.rbi ?? 0);
    case "Runs Scored":
      return batting.runs ?? 0;
    case "Doubles":
      return batting.doubles ?? 0;
    case "Singles":
      return singles;
    case "RBIs":
      return batting.rbi ?? 0;
    case "Stolen Bases":
      return batting.stolenBases ?? 0;
    case "Hits Allowed":
      return pitching.hits ?? 0;
    case "Walks Allowed":
      return pitching.baseOnBalls ?? 0;
    case "Strikeouts (Batting)":
      return batting.strikeOuts ?? 0;
    case "Outs Recorded":
      return outsRecorded;
    case "Runs + RBIs":
      return (batting.runs ?? 0) + (batting.rbi ?? 0);
    default:
      console.warn(`⚠️ Unknown propType: ${propType}`);
      return null;
  }
}

async function updatePropStatus(prop) {
  const url = `${MLB_API_BASE}/game/${prop.game_id}/boxscore`;
  const res = await fetch(url);
  const json = await res.json();

  const playerId = findPlayerId(json, prop.player_name);
  if (!playerId) {
    console.warn(
      `⚠️ Player not found: ${prop.player_name} (likely didn't play)`
    );
    return false;
  }

  let actualValue = getStatFromBoxscore(json, playerId, prop.prop_type);

  if (actualValue === null) {
    console.warn(
      `⚠️ No stat found for ${prop.prop_type} on ${prop.player_name}, trying live feed...`
    );
    actualValue = await getStatFromLiveFeed(
      prop.game_id,
      playerId,
      prop.prop_type
    );
  }

  if (actualValue === null) {
    console.warn(
      `⚠️ No stat found (boxscore + live) for ${prop.prop_type} on ${prop.player_name}`
    );
    return false;
  }

  const outcome = determineStatus(
    actualValue,
    prop.prop_value,
    prop.over_under
  );
  const { error } = await supabase
    .from("player_props")
    .update({ result: actualValue, outcome, status: "resolved" })
    .eq("id", prop.id);

  if (error) {
    console.error(`❌ Failed to update prop ${prop.id}:`, error.message);
    return false;
  } else {
    console.log(
      `✅ ${prop.player_name} (${prop.prop_type}): ${actualValue} → ${outcome}`
    );
    return true;
  }
}

async function getPendingProps() {
  const { data, error } = await supabase
    .from("player_props")
    .select("*")
    .eq("status", "pending")
    .lte("game_date", new Date().toISOString().split("T")[0]);

  if (error) throw error;
  return data;
}

export async function updatePropStatuses() {
  const props = await getPendingProps();
  console.log(`🔎 Found ${props.length} pending props to update.`);

  let updated = 0,
    skipped = 0,
    errors = 0;
  for (const prop of props) {
    try {
      const ok = await updatePropStatus(prop);
      if (ok) updated++;
      else skipped++;
    } catch (e) {
      console.error(`🔥 Error on ${prop.player_name}:`, e.message);
      errors++;
    }
  }

  console.log("🏁 Finished processing pending props:");
  console.log(`✅ Updated: ${updated}`);
  console.log(`⏭️ Skipped: ${skipped}`);
  console.log(`❌ Errors: ${errors}`);
}
