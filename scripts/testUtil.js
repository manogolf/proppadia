// scripts/testUtil.js

import {
  normalizeTeamAbbreviation,
  getTeamInfoByAbbr,
} from "../backend/scripts/shared/teamNameMap.js";

console.log("Normalized:", normalizeTeamAbbreviation("ATH")); // Should print "OAK"
console.log("Team Info:", getTeamInfoByAbbr("ATH")); // Should return info for OAK
