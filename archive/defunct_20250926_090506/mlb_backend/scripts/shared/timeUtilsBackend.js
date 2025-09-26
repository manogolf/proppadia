// backend/scripts/shared/timeUtilsBackend.js

import { DateTime } from "luxon";

// ✅ Add previously ported functions here...
export function nowET() {
  return DateTime.now().setZone("America/New_York");
}

export function todayET() {
  return nowET().toISODate();
}

export function yesterdayET() {
  return nowET().minus({ days: 1 }).toISODate();
}

export function currentTimeET() {
  return nowET().toFormat("HH:mm");
}

export function toISODate(dateInput) {
  if (!dateInput) return null;

  if (typeof dateInput === "string") {
    return DateTime.fromISO(dateInput).toISODate();
  } else if (dateInput instanceof Date) {
    return DateTime.fromJSDate(dateInput).toISODate();
  } else if (DateTime.isDateTime(dateInput)) {
    return dateInput.toISODate();
  }

  console.warn("⚠️ Invalid date input provided to toISODate:", dateInput);
  return null;
}

// ✅ Add these two exports for MLB API support
export async function getGameStartTimeET(gameId) {
  try {
    let res = await fetch(
      `https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk=${gameId}`
    );
    if (res.ok) {
      const schedJson = await res.json();
      const schedISO = schedJson?.dates?.[0]?.games?.[0]?.gameDate;
      if (schedISO) {
        return DateTime.fromISO(schedISO).setZone("America/New_York").toISO();
      }
    }

    res = await fetch(
      `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`
    );
    if (res.ok) {
      const boxJson = await res.json();
      const boxISO = boxJson?.gameData?.datetime?.dateTime;
      if (boxISO) {
        return DateTime.fromISO(boxISO).setZone("America/New_York").toISO();
      }
    }

    return null;
  } catch (err) {
    console.warn(`⚠️ Could not get game time for ${gameId}:`, err.message);
    return null;
  }
}

export function getTimeOfDayBucketET(isoDateTime) {
  const hour = DateTime.fromISO(isoDateTime, { zone: "America/New_York" }).hour;
  if (hour < 12) return "morning";
  if (hour < 17) return "afternoon";
  if (hour < 21) return "evening";
  return "night";
}

export function getDayOfWeekET(isoDate) {
  return DateTime.fromISO(isoDate, { zone: "America/New_York" }).toFormat(
    "cccc"
  );
}

// ✅ Combine gameDate and gameTime into an Eastern-zone DateTime object
export function toEasternDateTime(gameDate, gameTime) {
  if (!gameDate || !gameTime) return null; // guard clause
  const iso = `${gameDate}T${gameTime}`; // e.g. 2023-04-30T19:05:00
  return DateTime.fromISO(iso, { zone: "America/New_York" });
}
