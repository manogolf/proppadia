import { getStatFromLiveFeed } from "./getStatFromLiveFeed.js";

const LIVE_GAME_ID = 777916; // Replace with a current live game ID
const KNOWN_PLAYER_ID = 683776; // Replace with a real player in that game
const PROP_TYPE = "hits"; // Any prop type you support

const result = await getStatFromLiveFeed(
  LIVE_GAME_ID,
  KNOWN_PLAYER_ID,
  PROP_TYPE
);
console.log("📊 Returned stat result:", result);
