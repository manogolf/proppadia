// utils/mlbStats.js

export async function fetchLiveStatsByPlayer(playerName, gameDate) {
  const formattedDate = new Date(gameDate).toISOString().split('T')[0];
  const scheduleUrl = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${formattedDate}`;

  const scheduleRes = await fetch(scheduleUrl);
  const scheduleData = await scheduleRes.json();
  const games = scheduleData.dates?.[0]?.games || [];

  for (const game of games) {
    const gamePk = game.gamePk;
    const boxscoreUrl = `https://statsapi.mlb.com/api/v1/game/${gamePk}/boxscore`;
    const boxscoreRes = await fetch(boxscoreUrl);
    const boxscore = await boxscoreRes.json();

    const allPlayers = {
      ...boxscore.teams.home.players,
      ...boxscore.teams.away.players,
    };

    for (const playerId in allPlayers) {
      const player = allPlayers[playerId];
      if (
        player.person.fullName.toLowerCase() === playerName.toLowerCase()
      ) {
        const stats = player.stats?.batting;
        return {
          gamePk,
          name: player.person.fullName,
          hits: stats?.hits ?? 0,
          atBats: stats?.atBats ?? 0,
        };
      }
    }
  }

  return null;
}
