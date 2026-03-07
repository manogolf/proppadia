import React, { useEffect, useMemo, useState } from "react";
import TodayGames from "../../components/TodayGames.jsx";
import StreakCard from "../../components/StreakCard.jsx";
import { todayET } from "../../shared/timeUtils.js";
import { getBaseURL } from "../../shared/getBaseURL.js";
import MemberAccessCard from "../../components/predictions/MemberAccessCard.jsx";

export default function MLBHome() {
  const [games, setGames] = useState([]);
  const slateDate = useMemo(() => todayET(), []);

  useEffect(() => {
    let isMounted = true;
    (async () => {
      try {
        const base = getBaseURL();

        const res = await fetch(
          `${base}/api/mlb/schedule?date=${encodeURIComponent(slateDate)}`
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
  }, [slateDate]);

  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-5xl mx-auto px-4 pb-10">
        <div className="flex items-baseline justify-between mb-4">
          <h2 className="text-2xl font-bold text-slate-900">MLB</h2>
          <div className="text-sm text-slate-500">Slate (ET): {slateDate}</div>
        </div>

        <div className="mt-2">
          <StreakCard />
        </div>

        <div className="mt-6">
          <TodayGames games={games} />
        </div>

        <div className="mt-6">
          <MemberAccessCard
            openTo="/mlb/predictions"
            loginFrom="/mlb/predictions"
          />
        </div>
      </div>
    </div>
  );
}
