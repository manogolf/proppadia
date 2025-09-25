// scripts/testFetch2023.js

import { fetchBoxscoreStatsForGame } from "../../backend/scripts/shared/fetchBoxscoreStats.js";

const gamePk = 718715; // Yankees vs Twins, April 15, 2023

const main = async () => {
  const players = await fetchBoxscoreStatsForGame(gamePk);

  if (!players) {
    console.error("❌ No data returned");
    return;
  }

  for (const p of players) {
    const batStats = p.stats?.batting || {};
    const pitStats = p.stats?.pitching || {};

    const hasBatStats = Object.values(batStats).some(
      (v) => typeof v === "number" && v > 0
    );
    const isStarterPitcher = pitStats.gamesStarted === 1;

    if (hasBatStats || isStarterPitcher) {
      console.log(`🧑 ${p.fullName} — ${p.teamAbbr}`);
      console.dir(p.stats, { depth: null });
    }
  }
};

main();
