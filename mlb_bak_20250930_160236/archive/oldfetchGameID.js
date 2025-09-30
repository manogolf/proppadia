// ❗ Top of fetchGameId.js
import { getFullTeamName } from "./teamNameMap.js";

const TEAM_NAME_MAP = {
  ATL: "Atlanta Braves",
  ARI: "Arizona Diamondbacks",
  BAL: "Baltimore Orioles",
  BOS: "Boston Red Sox",
  CHC: "Chicago Cubs",
  CHW: "Chicago White Sox",
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
  OAK: "Oakland Athletics",
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

async function getGamePkForTeamOnDate(teamAbbr, gameDate) {
  const fullTeamName = getFullTeamName(teamAbbr);

  if (!fullTeamName) {
    console.warn(`❌ Team abbreviation not recognized: ${teamAbbr}`);
    return null;
  }

  const response = await fetch(
    `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${gameDate}`
  );
  const data = await response.json();

  const games = data.dates?.[0]?.games ?? [];

  const match = games.find((game) => {
    return (
      game.teams.away.team.name === fullTeamName ||
      game.teams.home.team.name === fullTeamName
    );
  });

  const game_id = match?.gamePk;
  console.log("Fetched game ID:", game_id);

  return game_id || null;
}

export { getGamePkForTeamOnDate };
