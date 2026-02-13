//  src/Pages/PlayerTeamBrowser.js

import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getBaseURL } from "../shared/getBaseURL.js";

export default function PlayerTeamBrowser() {
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchPlayers() {
      try {
        const res = await fetch(`${getBaseURL()}/api/players`);
        if (!res.ok) throw new Error("Failed to fetch player list");
        const data = await res.json();
        setPlayers(data);
      } catch (err) {
        console.error("❌ Error fetching players:", err);
        setError("Unable to load players.");
      } finally {
        setLoading(false);
      }
    }

    fetchPlayers();
  }, []);

  const groupedByTeam = players.reduce((acc, player) => {
    const team = player.team || "Unknown";
    if (!acc[team]) acc[team] = [];
    acc[team].push(player);
    return acc;
  }, {});

  const freshnessLabel = (d) => {
    if (!d) return "no recent props";
    return `last prop ${d}`;
  };

  if (loading)
    return (
      <div className="min-h-screen pp-page p-6">
        <div className="max-w-5xl mx-auto pp-card p-4 text-slate-600">
          Loading player list...
        </div>
      </div>
    );
  if (error)
    return (
      <div className="min-h-screen pp-page p-6">
        <div className="max-w-5xl mx-auto pp-card p-4 text-rose-600">{error}</div>
      </div>
    );

  return (
    <div className="min-h-screen pp-page p-6">
      <div className="max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold text-slate-900 mb-6">Players by Team</h1>
      {Object.keys(groupedByTeam)
        .sort()
        .map((team) => (
          <div key={team} className="mb-6 pp-card p-4">
            <h2 className="text-xl font-semibold mb-2 text-slate-900">
              {team}{" "}
              <span className="text-sm text-slate-500 font-normal">
                ({groupedByTeam[team].length})
              </span>
            </h2>
            <ul className="space-y-1">
              {groupedByTeam[team]
                .slice()
                .sort((a, b) => {
                  const ad = a.last_prop_date || "";
                  const bd = b.last_prop_date || "";
                  if (ad !== bd) return bd.localeCompare(ad);
                  return String(a.player_name || "").localeCompare(String(b.player_name || ""));
                })
                .map((p) => (
                <li key={p.player_id} className="flex items-center justify-between gap-3 pp-chip px-2 py-1">
                  <Link
                    to={`/player/${p.player_id}`}
                    className="text-slate-700 hover:underline"
                  >
                    {p.player_name || p.player_id}
                  </Link>
                  <span className="text-xs text-slate-500">{freshnessLabel(p.last_prop_date)}</span>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}
