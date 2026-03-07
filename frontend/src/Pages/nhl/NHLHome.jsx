// frontend/src/Pages/nhl/NhlHome.jsx
import { useEffect, useMemo, useState } from "react";
import TodayGamesNHL from "../../components/TodayGamesNHL.jsx";
import { todayET } from "../../shared/timeUtils.js";
import MemberAccessCard from "../../components/predictions/MemberAccessCard.jsx";
import { getBaseURL } from "../../shared/getBaseURL.js";
import NHLStreaksDashboardCard from "../../components/NHLStreaksDashboardCard.jsx";

export default function NhlHome() {
  const [games, setGames] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const slateDate = useMemo(() => todayET(), []);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        setError("");

        const url = `${getBaseURL()}/api/nhl/games/today?date=${encodeURIComponent(
          slateDate
        )}`;
        const res = await fetch(url);
        const j = await res.json();

        if (!res.ok || j?.ok === false) {
          throw new Error(
            j?.error || `Backend NHL endpoint failed (${res.status})`
          );
        }

        if (!cancelled) setGames(Array.isArray(j?.rows) ? j.rows : []);
      } catch (e) {
        if (!cancelled) setError(e?.message || "Failed to load NHL games.");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [slateDate]);

  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-5xl mx-auto px-4 pb-10">
        <div className="flex items-baseline justify-between mb-4">
          <h2 className="text-2xl font-bold text-slate-900">NHL</h2>
          <div className="text-sm text-slate-500">Slate (ET): {slateDate}</div>
        </div>

        <div className="mt-2">
          <NHLStreaksDashboardCard />
        </div>

        <div className="mt-6">
          {loading ? (
            <div className="w-full pp-card p-6 text-center text-slate-500">
              Loading NHL games…
            </div>
          ) : error ? (
            <div className="w-full pp-card p-6 text-center text-rose-600">
              {error}
            </div>
          ) : (
            <TodayGamesNHL games={games} />
          )}
        </div>

        <div className="mt-6">
          <MemberAccessCard
            openTo="/nhl/predictions"
            loginFrom="/nhl/predictions"
          />
        </div>
      </div>
    </div>
  );
}
