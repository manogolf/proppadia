// src/shared/timeUtils.js

import { DateTime } from "luxon";

// 📌 Get Current Time in Eastern Time (ISO String)
export function nowET() {
  return DateTime.now().setZone("America/New_York");
}

// 📌 Get Today’s Date in Eastern Time (YYYY-MM-DD)
export function todayET() {
  return nowET().toISODate();
}

// 📌 Get Yesterday’s Date in Eastern Time (YYYY-MM-DD)
export function yesterdayET() {
  return nowET().minus({ days: 1 }).toISODate();
}

// 📌 Get Current Time of Day in Eastern Time (HH:mm)
export function currentTimeET() {
  return nowET().toFormat("HH:mm");
}

// 📌 Convert Any Date to ISO Date (YYYY-MM-DD)
// Accepts a Date object, ISO string, or Luxon DateTime
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
// ✅ Add this function to support TodayGames.js
export function formatGameTime(isoDateTime) {
  if (!isoDateTime) return { etTime: "", localTime: "" };

  const dt = DateTime.fromISO(isoDateTime);
  return {
    etTime: dt.setZone("America/New_York").toFormat("HH:mm"),
    localTime: dt.toFormat("HH:mm"),
  };
}

export function formatDateET(dateString) {
  return DateTime.fromISO(dateString, { zone: "utc" })
    .setZone("America/New_York")
    .toFormat("LLL dd, yyyy");
}

export function getDayOfWeekET(isoDate) {
  return DateTime.fromISO(isoDate, { zone: "America/New_York" }).toFormat(
    "cccc"
  );
}

export function getTimeOfDayBucketET(isoDateTime) {
  const hour = DateTime.fromISO(isoDateTime, { zone: "America/New_York" }).hour;
  if (hour < 12) return "morning";
  if (hour < 17) return "afternoon";
  if (hour < 21) return "evening";
  return "night";
}

// 📌 Get Eastern Time (HH:mm) game time from game ID using boxscore endpoint
/**
 * Get the scheduled start time in Eastern Time ("HH:mm") for a gamePk.
 * 1) Try the Schedule endpoint (more complete for older seasons)
 * 2) Fall back to Boxscore if Schedule has no gameDate
 */
export async function getGameStartTimeET(gameId) {
  try {
    let res = await fetch(
      `https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk=${gameId}`
    );
    if (res.ok) {
      const schedJson = await res.json();
      const schedISO = schedJson?.dates?.[0]?.games?.[0]?.gameDate;
      if (schedISO) {
        return DateTime.fromISO(schedISO).setZone("America/New_York").toISO(); // ✅ ISO 8601 full timestamp
      }
    }

    res = await fetch(
      `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`
    );
    if (res.ok) {
      const boxJson = await res.json();
      const boxISO = boxJson?.gameData?.datetime?.dateTime;
      if (boxISO) {
        return DateTime.fromISO(boxISO).setZone("America/New_York").toISO(); // ✅ ISO 8601 full timestamp
      }
    }

    return null;
  } catch (err) {
    console.warn(`⚠️ Could not get game time for ${gameId}:`, err.message);
    return null;
  }
}

/* Keep your other helpers here … */

// ✅ Combine gameDate and gameTime into an Eastern-zone DateTime object
export function toEasternDateTime(gameDate, gameTime) {
  if (!gameDate || !gameTime) return null; // guard clause
  const iso = `${gameDate}T${gameTime}`; // e.g. 2023-04-30T19:05:00
  return DateTime.fromISO(iso, { zone: "America/New_York" });
}
