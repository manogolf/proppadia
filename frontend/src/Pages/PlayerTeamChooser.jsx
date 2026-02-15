import React from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";

export default function PlayerTeamChooser() {
  return (
    <div className="min-h-screen pp-page px-4 py-8">
      <div className="max-w-4xl mx-auto">
        <div className="pp-card p-6">
          <h1 className="text-2xl font-semibold text-slate-900">Players by Team</h1>
          <p className="text-sm text-slate-600 mt-2">
            Choose a sport workspace.
          </p>
          <div className="mt-5 grid grid-cols-1 md:grid-cols-2 gap-4">
            <PrefetchLink
              to="/players/mlb"
              className="pp-card p-4 border border-slate-200 hover:border-slate-300 transition"
            >
              <div className="text-xs uppercase tracking-wide text-slate-500">MLB</div>
              <div className="mt-1 text-lg font-semibold text-slate-900">MLB Players by Team</div>
              <div className="mt-1 text-sm text-slate-600">Roster browser and watchlist workflow</div>
            </PrefetchLink>
            <PrefetchLink
              to="/players/nhl"
              className="pp-card p-4 border border-slate-200 hover:border-slate-300 transition"
            >
              <div className="text-xs uppercase tracking-wide text-slate-500">NHL</div>
              <div className="mt-1 text-lg font-semibold text-slate-900">NHL Players by Team</div>
              <div className="mt-1 text-sm text-slate-600">Slate-driven player browser and watchlist workflow</div>
            </PrefetchLink>
          </div>
        </div>
      </div>
    </div>
  );
}
