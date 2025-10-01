import React from "react";
import { Link } from "react-router-dom";

export default function HomeGateway() {
  return (
    <div className="min-h-screen bg-gray-100 px-4 py-10">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-2xl font-semibold text-gray-900 mb-6">
          Welcome to Proppadia
        </h1>
        <p className="text-gray-600 mb-8">
          Choose a league to view today&rsquo;s games, streaks, and dashboards.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
          {/* MLB tile */}
          <Link
            to="/mlb"
            className="rounded-2xl bg-white shadow hover:shadow-md transition p-6 flex items-center justify-between"
          >
            <div>
              <h2 className="text-lg font-medium text-gray-900">MLB</h2>
              <p className="text-sm text-gray-500">
                Today&rsquo;s games & streaks
              </p>
            </div>
            <span className="text-gray-400" aria-hidden>
              →
            </span>
          </Link>

          {/* NHL tile (placeholder for now) */}
          <Link
            to="/nhl"
            className="rounded-2xl bg-white shadow hover:shadow-md transition p-6 flex items-center justify-between"
          >
            <div>
              <h2 className="text-lg font-medium text-gray-900">NHL</h2>
              <p className="text-sm text-gray-500">
                Shots on goal & dashboards
              </p>
            </div>
            <span className="text-gray-400" aria-hidden>
              →
            </span>
          </Link>
        </div>
      </div>
    </div>
  );
}
