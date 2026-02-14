// src/components/TodayGames.js
import React, { useState, useEffect } from "react";
import { todayET, toISODate } from "../shared/timeUtils.js";
import { getStatusDisplay, getStatusColor } from "../shared/gameStatusUtils.js";
import { getBaseURL } from "../shared/getBaseURL.js";

const TodayGames = ({ sport = "mlb", games }) => {
  const [standings, setStandings] = useState([]);

  useEffect(() => {
    const fetchStandings = async () => {
      try {
        const season = new Date().toLocaleDateString("en-CA", {
          timeZone: "America/New_York",
          year: "numeric",
        });
        const response = await fetch(
          `${getBaseURL()}/api/mlb/standings?season=${encodeURIComponent(season)}`
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

    if (String(sport).toLowerCase() === "mlb") fetchStandings();
  }, [sport]);

  const getTeamRecordFromStandings = (teamId) => {
    const team = standings.find((t) => Number(t.id) === Number(teamId));
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
    <section className="w-full pp-card p-4">
      <h2 className="text-xl font-bold text-slate-900 text-center mb-1">
        🗓 Today’s Games
      </h2>
      <p className="text-sm text-slate-500 text-center mb-4">
        Live from {sport.toUpperCase()} • ET and Local Time Displayed
      </p>

      {sortedGames.length === 0 ? (
        <p className="text-center text-slate-500">No games scheduled.</p>
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
                className="pp-chip grid grid-cols-[1fr_auto_1fr] items-center gap-4 p-4 rounded-lg max-w-5xl mx-auto"
              >
                {/* Away Team */}
                <div className="flex flex-col items-start gap-2 max-w-[140px]">
                  <div className="flex items-center gap-2">
                    <img
                      src={`https://www.mlbstatic.com/team-logos/${awayTeam.id}.svg`}
                      alt={awayTeam.name}
                      className="w-10 h-10 object-contain shrink-0"
                    />
                    <span className="text-sm font-medium text-slate-800 break-words">
                      {awayTeam.name}
                    </span>
                  </div>
                  <div className="text-xs text-slate-500">
                    {getTeamRecordFromStandings(awayTeam.id)}
                  </div>
                  <div className="text-xs text-slate-500">
                    SP: {getStartingPitcher("away", game)}
                  </div>
                </div>

                {/* Game Info */}
                <div className="flex flex-col items-center text-center gap-1">
                  {gameLabel && (
                    <span className="text-xs text-slate-500">{gameLabel}</span>
                  )}
                  <span className="text-lg font-semibold">{score}</span>
                  <span className={`text-sm ${statusColor}`}>{statusText}</span>
                </div>

                {/* Home Team */}
                <div className="flex flex-col items-end gap-2 text-right ml-auto">
                  <div className="flex items-center gap-2 justify-end">
                    <span className="text-sm font-medium text-slate-800 break-words">
                      {homeTeam.name}
                    </span>
                    <img
                      src={`https://www.mlbstatic.com/team-logos/${homeTeam.id}.svg`}
                      alt={homeTeam.name}
                      className="w-10 h-10 object-contain shrink-0"
                    />
                  </div>
                  <div className="text-xs text-slate-500">
                    {getTeamRecordFromStandings(homeTeam.id)}
                  </div>
                  <div className="text-xs text-slate-500">
                    SP: {getStartingPitcher("home", game)}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
};

export default TodayGames;
