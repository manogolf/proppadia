import React from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";

export default function AccessRequiredPage({ requiredPath, requiredLabel }) {
  return (
    <div className="min-h-screen pp-page px-4 py-10">
      <div className="max-w-2xl mx-auto pp-card p-6">
        <div className="text-xs tracking-wide uppercase text-slate-500 mb-1">
          Access Required
        </div>
        <h1 className="text-2xl font-semibold text-slate-900">
          Sign in to continue
        </h1>
        <p className="text-sm text-slate-600 mt-2">
          This page is available to signed-in members. You were trying to open{" "}
          <span className="font-medium">{requiredLabel}</span>.
        </p>

        <div className="mt-5 flex flex-wrap gap-2">
          <PrefetchLink
            to="/login"
            state={{ from: { pathname: requiredPath || "/props" } }}
            prefetchTo="/login"
            className="pp-btn pp-btn-primary pp-btn-md"
          >
            Login and Continue
          </PrefetchLink>
          <PrefetchLink
            to="/login"
            state={{ from: { pathname: "/props" } }}
            prefetchTo="/login"
            className="pp-btn pp-btn-secondary pp-btn-md"
          >
            Login for MLB Predictions
          </PrefetchLink>
          <PrefetchLink
            to="/login"
            state={{ from: { pathname: "/nhl/predictions" } }}
            prefetchTo="/login"
            className="pp-btn pp-btn-secondary pp-btn-md"
          >
            Login for NHL Predictions
          </PrefetchLink>
        </div>

        <div className="mt-6 border-t border-slate-200 pt-4">
          <div className="text-sm text-slate-700 mb-2">Public pages</div>
          <div className="flex flex-wrap gap-2">
            <PrefetchLink
              to="/"
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              Home
            </PrefetchLink>
            <PrefetchLink
              to="/mlb"
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              MLB Home
            </PrefetchLink>
            <PrefetchLink
              to="/nhl"
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              NHL Home
            </PrefetchLink>
            <PrefetchLink
              to="/players"
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              Players By Team
            </PrefetchLink>
          </div>
        </div>
      </div>
    </div>
  );
}
