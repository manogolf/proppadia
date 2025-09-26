import React, { useState, useEffect } from "react";
import TodayGames from "../components/TodayGames.js";
import StreakCard from "../components/StreakCard.js";
import { todayET } from "../shared/timeUtils.js";

export default function Home() {
  console.log("✅ API URL:", process.env.REACT_APP_API_URL);
  const [games, setGames] = useState([]);

  useEffect(() => {
    const fetchGames = async () => {
      try {
        const today = todayET();

        const response = await fetch(
          `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${today}&hydrate=team,linescore,probablePitcher,decisions,game(content(summary),live),boxscore`
        );
        const data = await response.json();
        const gameList = data.dates?.[0]?.games || [];
        setGames(gameList);
      } catch (error) {
        console.error("Error fetching games:", error);
      }
    };

    fetchGames();
  }, []);

  return (
    <div className="min-h-screen bg-gray-100 px-4 py-6">
      <div className="max-w-4xl mx-auto space-y-6">
        <TodayGames games={games} />
        <StreakCard />
      </div>
    </div>
  );
}
