// backend/scripts/shared/fetchGameID.js

/**
 * Finds the gamePk for the given team ID on the given date.
 * @param {number|string} teamId - MLB team ID (e.g. 144 for ATL)
 * @param {string} dateISO - YYYY-MM-DD
 * @returns {Promise<number|null>}
 */
export async function getGamePkForTeamOnDate(teamId, dateISO) {
  const teamNum = Number(teamId);
  const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dateISO}`;

  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    const games = json?.dates?.[0]?.games || [];

    for (const game of games) {
      const homeId = Number(game?.teams?.home?.team?.id);
      const awayId = Number(game?.teams?.away?.team?.id);
      if (homeId === teamNum || awayId === teamNum) {
        return game.gamePk ?? null;
      }
    }
  } catch (err) {
    console.warn(
      `⚠️ Failed to fetch schedule for team ${teamId} on ${dateISO}:`,
      err?.message || err
    );
  }

  console.warn(`⚠️ No game found for team ID ${teamId} on ${dateISO}`);
  return null;
}

