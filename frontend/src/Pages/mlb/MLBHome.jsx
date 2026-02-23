import React, { useEffect, useState } from "react";
import TodayGames from "../../components/TodayGames.jsx";
import StreakCard from "../../components/StreakCard.jsx";
import { todayET } from "../../shared/timeUtils.js";
import { getBaseURL } from "../../shared/getBaseURL.js";
import MemberAccessCard from "../../components/predictions/MemberAccessCard.jsx";

export default function MLBHome() {
  const [games, setGames] = useState([]);

  useEffect(() => {
    let isMounted = true;
    (async () => {
      try {
        const today =
          typeof todayET === "function"
            ? todayET()
            : new Date().toISOString().slice(0, 10);
        const base = getBaseURL();

        const res = await fetch(
          `${base}/api/mlb/schedule?date=${encodeURIComponent(today)}`
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
    <div className="min-h-screen pp-page px-4 py-6">
      <div className="max-w-4xl mx-auto space-y-6">
        <TodayGames games={games} />
        <StreakCard />
        <MemberAccessCard
          openTo="/mlb/predictions"
          loginFrom="/mlb/predictions"
        />
      </div>
    </div>
  );
}
