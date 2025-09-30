// scripts/dumpPlaysForGame.js
import { getLiveFeedFromGameID } from "../backend/scripts/shared/mlbApiUtils.js";

const gameId = 777145;

(async () => {
  const liveFeed = await getLiveFeedFromGameID(gameId);
  const allPlays = liveFeed?.liveData?.plays?.allPlays || [];

  console.log(`📦 Retrieved ${allPlays.length} plays for game ${gameId}\n`);

  allPlays.forEach((play, i) => {
    const batterId = play.matchup?.batter?.id;
    const pitcherId = play.matchup?.pitcher?.id;
    const description = play.result?.description;

    console.log(
      `Play ${
        i + 1
      }: batter = ${batterId}, pitcher = ${pitcherId}, desc = ${description}`
    );
  });
})();
