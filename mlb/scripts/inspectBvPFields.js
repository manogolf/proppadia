// 📄 File: scripts/inspectBvPFields.js
import fetch from "node-fetch";

/**
 * Inspect MLB API to determine what PvB/BvP fields are actually returned.
 *
 * @param {string|number} batterId - The MLB player ID for the batter
 * @param {string|number} pitcherId - The MLB player ID for the pitcher
 */
async function inspectBvPFields(batterId, pitcherId) {
  const bvPUrl = `https://statsapi.mlb.com/api/v1/people/${batterId}/stats?stats=vsPlayer&opposingPlayerId=${pitcherId}&group=hitting`;
  const pvBUrl = `https://statsapi.mlb.com/api/v1/people/${pitcherId}/stats?stats=vsPlayer&opposingPlayerId=${batterId}&group=pitching`;

  const bvPResponse = await fetch(bvPUrl);
  const pvBResponse = await fetch(pvBUrl);

  const bvPData = await bvPResponse.json();
  const pvBData = await pvBResponse.json();

  const bvPStats = bvPData?.stats?.[0]?.splits?.[0]?.stat || {};
  const pvBStats = pvBData?.stats?.[0]?.splits?.[0]?.stat || {};

  console.log("🟢 BvP (batter vs pitcher) field keys:");
  console.log(Object.keys(bvPStats).sort());

  console.log("\n🔵 PvB (pitcher vs batter) field keys:");
  console.log(Object.keys(pvBStats).sort());

  console.log("\n🧪 BvP example values:");
  console.log(bvPStats);

  console.log("\n🧪 PvB example values:");
  console.log(pvBStats);
}

// Replace with real batter/pitcher combos to explore
const BATTER_ID = 592450; // Mike Trout
const PITCHER_ID = 660271; // Justin Verlander

inspectBvPFields(BATTER_ID, PITCHER_ID);
