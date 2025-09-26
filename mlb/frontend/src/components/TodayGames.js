// src/components/TodayGames.js
import React, { useState, useEffect } from "react";
import { todayET, toISODate } from "../shared/timeUtils.js";
import { getStatusDisplay, getStatusColor } from "../shared/gameStatusUtils.js";

const TodayGames = ({ games }) => {
  const [standings, setStandings] = useState([]);

  useEffect(() => {
    const fetchStandings = async () => {
      try {
        const response = await fetch(
          "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason"
        );
        const data = await response.json();
        const teams = data.records.flatMap((record) =>
          record.teamRecords.map((team) => ({
            id: team.team.id,
            name: team.team.name,
            wins: team.wins,
            losses: team.losses,
          }))
        );
        setStandings(teams);
      } catch (error) {
        console.error("Error fetching standings:", error);
      }
    };

    fetchStandings();
  }, []);

  const getTeamRecordFromStandings = (teamName) => {
    const team = standings.find((t) => t.name === teamName);
    if (team) {
      return `🗒 Record: ${team.wins}-${team.losses}`;
    }
    return "🗒 Record: N/A";
  };

  const getStartingPitcher = (teamKey, game) => {
    const actualPitcherId = game?.boxscore?.teams?.[teamKey]?.pitchers?.[0];
    const scheduledPitcher = game?.teams?.[teamKey]?.probablePitcher;

    if (actualPitcherId) {
      const player =
        game.boxscore.teams[teamKey].players?.[`ID${actualPitcherId}`];
      const fullName =
        player?.person?.fullName || scheduledPitcher?.fullName || "TBD";
      const wins = player?.stats?.pitching?.wins;
      const losses = player?.stats?.pitching?.losses;
      if (wins !== undefined && losses !== undefined) {
        return `${fullName} (${wins}-${losses})`;
      }
      return fullName;
    }

    if (scheduledPitcher) {
      const fullName = scheduledPitcher.fullName;
      const wins = scheduledPitcher.stats?.pitching?.wins;
      const losses = scheduledPitcher.stats?.pitching?.losses;
      if (wins !== undefined && losses !== undefined) {
        return `${fullName} (${wins}-${losses})`;
      }
      return fullName;
    }

    return "TBD";
  };

  // ✅ Filter games to only those scheduled for today (Eastern Time)
  const today = todayET();
  const todaysGames = (games || []).filter((game) => {
    const gameDateET = toISODate(game.gameDate);
    return gameDateET === today;
  });

  const sortedGames = [...todaysGames].sort(
    (a, b) => new Date(a.gameDate) - new Date(b.gameDate)
  );

  const groupedMatchups = {};
  sortedGames.forEach((game) => {
    const key = `${game.teams.away.team.name}@${game.teams.home.team.name}`;
    if (!groupedMatchups[key]) groupedMatchups[key] = [];
    groupedMatchups[key].push(game);
  });
  Object.values(groupedMatchups).forEach((group) => {
    group.sort((a, b) => new Date(a.gameDate) - new Date(b.gameDate));
  });

  return (
    <div className="w-full bg-gray-200 shadow rounded-xl p-4">
      <h2 className="text-xl font-bold text-indigo-900 text-center mb-1">
        🗓 Today’s Games
      </h2>
      <p className="text-sm text-gray-500 text-center mb-4">
        Live from MLB • ET and Local Time Displayed
      </p>

      {sortedGames.length === 0 ? (
        <p className="text-center text-gray-500">No games scheduled.</p>
      ) : (
        <ul className="space-y-4">
          {sortedGames.map((game) => {
            const awayTeam = game.teams.away.team;
            const homeTeam = game.teams.home.team;
            const matchupKey = `${awayTeam.name}@${homeTeam.name}`;
            const matchupGroup = groupedMatchups[matchupKey] || [];
            const multiGame = matchupGroup.length > 1;
            const gameIndex = matchupGroup.findIndex(
              (g) => g.gamePk === game.gamePk
            );
            const gameLabel = multiGame ? `Game ${gameIndex + 1}` : null;

            const status = game.status.detailedState;
            const showScore = status === "Final" || status === "In Progress";
            const score = showScore
              ? `${game.teams.away.score} - ${game.teams.home.score}`
              : "-";
            const statusText = getStatusDisplay(game);
            const statusColor = getStatusColor(status);

            return (
              <li
                key={game.gamePk}
                className="grid grid-cols-[1fr_auto_1fr] items-center gap-4 p-4 rounded-lg border border border-blue-200 bg-blue-50 max-w-5xl mx-auto"
              >
                {/* Away Team */}
                <div className="flex flex-col items-start gap-2 max-w-[140px]">
                  <div className="flex items-center gap-2">
                    <img
                      src={`https://www.mlbstatic.com/team-logos/${awayTeam.id}.svg`}
                      alt={awayTeam.name}
                      className="w-10 h-10 object-contain shrink-0"
                    />
                    <span className="text-sm font-medium text-gray-800 break-words">
                      {awayTeam.name}
                    </span>
                  </div>
                  <div className="text-xs text-gray-500">
                    {getTeamRecordFromStandings(awayTeam.name)}
                  </div>
                  <div className="text-xs text-gray-500">
                    SP: {getStartingPitcher("away", game)}
                  </div>
                </div>

                {/* Game Info */}
                <div className="flex flex-col items-center text-center gap-1">
                  {gameLabel && (
                    <span className="text-xs text-gray-500">{gameLabel}</span>
                  )}
                  <span className="text-lg font-semibold">{score}</span>
                  <span className={`text-sm ${statusColor}`}>{statusText}</span>
                </div>

                {/* Home Team */}
                <div className="flex flex-col items-end gap-2 text-right ml-auto">
                  <div className="flex items-center gap-2 justify-end">
                    <span className="text-sm font-medium text-gray-800 break-words">
                      {homeTeam.name}
                    </span>
                    <img
                      src={`https://www.mlbstatic.com/team-logos/${homeTeam.id}.svg`}
                      alt={homeTeam.name}
                      className="w-10 h-10 object-contain shrink-0"
                    />
                  </div>
                  <div className="text-xs text-gray-500">
                    {getTeamRecordFromStandings(homeTeam.name)}
                  </div>
                  <div className="text-xs text-gray-500">
                    SP: {getStartingPitcher("home", game)}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
};

export default TodayGames;
