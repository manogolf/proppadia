//  scripts/findBvPMatchups.js

import fs from "fs";

// Replace with your own values
const jsonPath = "./718223.json";
const batterId = 643446;
const pitcherId = 643361;

const data = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
const plays = data?.liveData?.plays?.allPlays || [];

const matchedPlays = plays.filter((play) => {
  const isBatter = play?.matchup?.batter?.id === batterId;
  const isPitcher =
    play?.matchup?.pitcher?.id === pitcherId ||
    (play?.playEvents || []).map((e) => e?.pitcher?.id).includes(pitcherId);

  return isBatter && isPitcher;
});

console.log(`🎯 Found ${matchedPlays.length} matching plays`);
matchedPlays.forEach((play, idx) => {
  console.log(`\nPlay #${idx + 1} — ${play.result?.description}`);
});
