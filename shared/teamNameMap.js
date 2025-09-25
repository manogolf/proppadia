//  /shared/teamNameMap.js

// Map of team abbreviations to full names
export const teamNameMap = {
  ATH: "Athletics",
  ATL: "Atlanta Braves",
  AZ: "Arizona Diamondbacks",
  BAL: "Baltimore Orioles",
  BOS: "Boston Red Sox",
  CHC: "Chicago Cubs",
  CWS: "Chicago White Sox",
  CIN: "Cincinnati Reds",
  CLE: "Cleveland Guardians",
  COL: "Colorado Rockies",
  DET: "Detroit Tigers",
  HOU: "Houston Astros",
  KC: "Kansas City Royals",
  LAA: "Los Angeles Angels",
  LAD: "Los Angeles Dodgers",
  MIA: "Miami Marlins",
  MIL: "Milwaukee Brewers",
  MIN: "Minnesota Twins",
  NYM: "New York Mets",
  NYY: "New York Yankees",
  PHI: "Philadelphia Phillies",
  PIT: "Pittsburgh Pirates",
  SD: "San Diego Padres",
  SEA: "Seattle Mariners",
  SF: "San Francisco Giants",
  STL: "St. Louis Cardinals",
  TB: "Tampa Bay Rays",
  TEX: "Texas Rangers",
  TOR: "Toronto Blue Jays",
  WSH: "Washington Nationals",
};

// Map of MLB team IDs to { abbr, fullName }
export const teamIdMap = {
  108: { abbr: "LAA", fullName: "Los Angeles Angels" },
  109: { abbr: "ARI", fullName: "Arizona Diamondbacks" },
  110: { abbr: "BAL", fullName: "Baltimore Orioles" },
  111: { abbr: "BOS", fullName: "Boston Red Sox" },
  112: { abbr: "CHC", fullName: "Chicago Cubs" },
  113: { abbr: "CIN", fullName: "Cincinnati Reds" },
  114: { abbr: "CLE", fullName: "Cleveland Guardians" },
  115: { abbr: "COL", fullName: "Colorado Rockies" },
  116: { abbr: "DET", fullName: "Detroit Tigers" },
  117: { abbr: "HOU", fullName: "Houston Astros" },
  118: { abbr: "KC", fullName: "Kansas City Royals" },
  119: { abbr: "LAD", fullName: "Los Angeles Dodgers" },
  120: { abbr: "WSH", fullName: "Washington Nationals" },
  121: { abbr: "NYM", fullName: "New York Mets" },
  133: { abbr: "OAK", fullName: "Athletics" }, // OAK/LV
  134: { abbr: "PIT", fullName: "Pittsburgh Pirates" },
  135: { abbr: "SD", fullName: "San Diego Padres" },
  136: { abbr: "SEA", fullName: "Seattle Mariners" },
  137: { abbr: "SF", fullName: "San Francisco Giants" },
  138: { abbr: "STL", fullName: "St. Louis Cardinals" },
  139: { abbr: "TB", fullName: "Tampa Bay Rays" },
  140: { abbr: "TEX", fullName: "Texas Rangers" },
  141: { abbr: "TOR", fullName: "Toronto Blue Jays" },
  142: { abbr: "MIN", fullName: "Minnesota Twins" },
  143: { abbr: "PHI", fullName: "Philadelphia Phillies" },
  144: { abbr: "ATL", fullName: "Atlanta Braves" },
  145: { abbr: "CWS", fullName: "Chicago White Sox" },
  146: { abbr: "MIA", fullName: "Miami Marlins" },
  147: { abbr: "NYY", fullName: "New York Yankees" },
  158: { abbr: "MIL", fullName: "Milwaukee Brewers" },
};

// Normalize special cases and get full name from abbreviation
export const getFullTeamName = (abbr) => {
  const normalized = normalizeTeamAbbreviation(abbr);
  if (["OAK", "LV", "VIL"].includes(normalized)) return "Athletics";
  return teamNameMap[normalized] || normalized;
};

// Get full name and abbreviation from team ID
export const getTeamInfoByID = (abbrOrId) => {
  if (typeof abbrOrId === "number" || /^\d+$/.test(abbrOrId)) {
    return teamIdMap[Number(abbrOrId)] || null;
  }

  const normalized = normalizeTeamAbbreviation(abbrOrId);
  for (const [id, info] of Object.entries(teamIdMap)) {
    if (info.abbr === normalized) return info;
  }

  return null;
};

export function getFullTeamAbbreviationFromID(teamId) {
  if (!teamId) return null;
  const info = teamIdMap[parseInt(teamId)];
  return info ? info.abbr : null;
}

export async function getOpponentAbbreviation(teamAbbr, gameId) {
  const url = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;
  const res = await fetch(url);
  const json = await res.json();

  const home = json.teams.home.team;
  const away = json.teams.away.team;

  if (home.abbreviation === teamAbbr) {
    return away.abbreviation;
  } else if (away.abbreviation === teamAbbr) {
    return home.abbreviation;
  } else {
    throw new Error(
      `Team ${teamAbbr} not found in boxscore for game ${gameId}`
    );
  }
}

export function getTeamIdFromAbbr(abbr) {
  const normalized = normalizeTeamAbbreviation(abbr);
  for (const [id, info] of Object.entries(teamIdMap)) {
    if (info.abbr === normalized) return parseInt(id);
  }
  return null;
}

export function normalizeTeamAbbreviation(abbr) {
  if (!abbr) return abbr;
  const upper = abbr.toUpperCase();
  if (["AZ"].includes(upper)) return "ARI";
  if (["ATH", "LV", "VIL"].includes(upper)) return "OAK"; // ✅ this handles it
  return upper;
}

const specialValidTeams = ["OAK", "LV", "VIL", "ATH"];

export const isValidMLBTeam = (abbr) => {
  const normalized = abbr?.toUpperCase();
  return (
    teamNameMap.hasOwnProperty(normalized) ||
    ["OAK", "LV", "VIL"].includes(normalized)
  );
};

export function getTeamInfoByAbbr(abbr) {
  const normalized = normalizeTeamAbbreviation(abbr);
  for (const [id, info] of Object.entries(teamIdMap)) {
    if (info.abbr === normalized) {
      return { ...info, id: Number(id) };
    }
  }
  return null;
}

// Build it once
const abbrToIdMap = Object.entries(teamIdMap).reduce((acc, [id, { abbr }]) => {
  acc[abbr] = parseInt(id);
  return acc;
}, {});

export function getTeamInfoById(teamId) {
  const idStr = String(teamId);
  const info = teamIdMap[idStr];
  if (!info) return null;
  return {
    id: parseInt(idStr),
    abbr: info.abbr,
    fullName: info.fullName,
  };
}
