// frontend/src/Pages/Home.js
import React, { useState, useEffect } from "react";
import TodayGames from "../components/TodayGames.js";
import StreakCard from "../components/StreakCard.js";
import { todayET } from "../shared/timeUtils.js";

export default function Home() {
  const [games, setGames] = useState([]);

  useEffect(() => {
    let isMounted = true;
    (async () => {
      try {
        const today =
          typeof todayET === "function"
            ? todayET()
            : new Date().toISOString().slice(0, 10);

        const res = await fetch(
          `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${today}&hydrate=team,linescore,probablePitcher,decisions,game(content(summary),live),boxscore`
        );
        const data = await res.json();
        const gameList = Array.isArray(data?.dates)
          ? data.dates[0]?.games || []
          : [];
        if (isMounted) setGames(gameList);
      } catch (err) {
        console.error("Error fetching games:", err);
        if (isMounted) setGames([]);
      }
    })();
    return () => {
      isMounted = false;
    };
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
