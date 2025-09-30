//  scripts/testPvBBvP.js

import { getLiveFeedFromGameID } from "../backend/scripts/shared/mlbApiUtils.js";
import { getBatterVsPitcherStats } from "../backend/scripts/shared/playerUtils.js";
import { getPvBStats } from "../backend/scripts/shared/playerUtils.js";
import { format } from "date-fns";

const gameId = 718265; // ✅ replace with verified game ID
const batterId = 676694; // ✅ known batter from that game
const pitcherId = 656232; // ✅ pitcher he faced

(async () => {
  const liveFeed = await getLiveFeedFromGameID(gameId);
  const allPlays = liveFeed?.liveData?.plays?.allPlays || [];

  console.log(`📦 Retrieved ${allPlays.length} plays for game ${gameId}`);

  const bvpStats = await getBatterVsPitcherStats(
    gameId,
    batterId,
    pitcherId,
    allPlays
  );
  console.log(
    `🎯 BvP stats for batter ${batterId} vs pitcher ${pitcherId}:`,
    bvpStats
  );

  if (!bvpStats) {
    console.log("🚫 No BvP stats returned — possibly 0 plays matched.");
  }

  const pvbStats = getPvBStats(gameId, pitcherId, allPlays);
  console.log(`🎯 PvB stats for pitcher ${pitcherId}:`, pvbStats);
})();
