import React from "react";
import MemberAccessCard from "../components/predictions/MemberAccessCard.jsx";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";

export default function HomeGateway() {
  return (
    <div className="min-h-screen pp-page px-4 py-10">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-2xl font-semibold text-slate-900 mb-6">
          Welcome to Proppadia
        </h1>
        <p className="text-slate-600 mb-8">
          Choose a league to view today&rsquo;s games, streaks, and dashboards.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
          {/* MLB tile */}
          <PrefetchLink
            to="/mlb"
            className="pp-card transition p-6 flex items-center justify-between hover:shadow-md"
          >
            <div>
              <h2 className="text-lg font-medium text-slate-900">MLB</h2>
              <p className="text-sm text-slate-500">
                Today&rsquo;s games & streaks
              </p>
            </div>
            <span className="text-slate-400" aria-hidden>
              →
            </span>
          </PrefetchLink>

          {/* NHL tile (placeholder for now) */}
          <PrefetchLink
            to="/nhl"
            className="pp-card transition p-6 flex items-center justify-between hover:shadow-md"
          >
            <div>
              <h2 className="text-lg font-medium text-slate-900">NHL</h2>
              <p className="text-sm text-slate-500">
                Shots on goal & dashboards
              </p>
            </div>
            <span className="text-slate-400" aria-hidden>
              →
            </span>
          </PrefetchLink>
        </div>

        <div className="mt-6">
          <MemberAccessCard
            ctas={[
              { label: "MLB Predictions", openTo: "/props", loginFrom: "/props" },
              {
                label: "NHL Predictions",
                openTo: "/nhl/predictions",
                loginFrom: "/nhl/predictions",
              },
            ]}
          />
        </div>
      </div>
    </div>
  );
}
