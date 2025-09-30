import { getStatFromLiveFeed } from "../../backend/scripts/resolution/getStatFromLiveFeed.js";

const stat = await getStatFromLiveFeed(777675, "669160", "strikeouts_pitching");
console.log("📊 Live stat:", stat);
