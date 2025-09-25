// src/utils/fetchSchedule.js
import fetch from "node-fetch";
import { DateTime } from "luxon";

export async function fetchSchedule(targetDate) {
  const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${targetDate}`;
  const res = await fetch(url);

  if (!res.ok) {
    console.error(`❌ Failed to fetch schedule for ${targetDate}`);
    return { data: [], error: `HTTP ${res.status}` };
  }

  const json = await res.json();
  const games = json.dates?.[0]?.games || [];

  return { data: games, error: null };
}

// 📌 Extract game time from game ID via boxscore
export async function getGameTimeFromID(gameId) {
  try {
    const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Failed request: ${res.status}`);

    const data = await res.json();
    const dateStr = data?.gameData?.datetime?.dateTime;
    if (!dateStr) return null;

    return DateTime.fromISO(dateStr).setZone("America/New_York").toISO();
  } catch (err) {
    console.warn(`⚠️ Failed to get game time for ${gameId}:`, err.message);
    return null;
  }
}
