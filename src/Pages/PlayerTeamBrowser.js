//  src/Pages/PlayerTeamBrowser.js

import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getBaseURL } from "../../shared/getBaseURL.js";

export default function PlayerTeamBrowser() {
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchPlayers() {
      try {
        const res = await fetch(`${getBaseURL()}/players`);
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

  if (loading) return <div className="p-4">Loading player list...</div>;
  if (error) return <div className="p-4 text-red-600">{error}</div>;

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">Players by Team</h1>
      {Object.keys(groupedByTeam)
        .sort()
        .map((team) => (
          <div key={team} className="mb-6">
            <h2 className="text-xl font-semibold mb-2">{team}</h2>
            <ul className="space-y-1">
              {groupedByTeam[team].map((p) => (
                <li key={p.player_id}>
                  <Link
                    to={`/player/${p.player_id}`}
                    className="text-blue-600 hover:underline"
                  >
                    {p.player_name || p.player_id}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        ))}
    </div>
  );
}
