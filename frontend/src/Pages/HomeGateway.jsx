import React, { useEffect, useState } from "react";
import MemberAccessCard from "../components/predictions/MemberAccessCard.jsx";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";

async function fetchJson(path) {
  const base = getBaseURL();
  const res = await fetch(`${base}${path}`, { credentials: "include" });
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  return { ok: res.ok, status: res.status, body };
}

export default function HomeGateway() {
  const [snapshotLoading, setSnapshotLoading] = useState(true);
  const [mlbSnapshot, setMlbSnapshot] = useState(null);
  const [nhlSnapshot, setNhlSnapshot] = useState(null);

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      setSnapshotLoading(true);
      const [mlb, nhl] = await Promise.all([
        fetchJson("/api/mlb/standings"),
        fetchJson("/api/nhl/slate/meta"),
      ]);
      if (!mounted) return;
      setMlbSnapshot(mlb.ok && mlb.body?.ok ? mlb.body : null);
      setNhlSnapshot(nhl.ok && nhl.body?.ok ? nhl.body : null);
      setSnapshotLoading(false);
    };
    load();
    return () => {
      mounted = false;
    };
  }, []);

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

        <div className="mt-6 pp-card p-5">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-base font-semibold text-slate-900">Today Snapshot</h2>
            <span className="text-xs text-slate-500">
              {snapshotLoading ? "Loading..." : "Backend-owned status"}
            </span>
          </div>
          <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
              <div className="font-semibold text-slate-900">MLB</div>
              <div className="text-slate-600 mt-1">
                Teams tracked: {Array.isArray(mlbSnapshot?.records) ? mlbSnapshot.records.length : "-"}
              </div>
              <div className="text-slate-600">
                Source: {mlbSnapshot?.source || "-"}{mlbSnapshot?.stale ? " (stale)" : ""}
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
              <div className="font-semibold text-slate-900">NHL</div>
              <div className="text-slate-600 mt-1">
                Components healthy: {nhlSnapshot?.components ? Object.values(nhlSnapshot.components).filter((c) => c?.ok === true).length : "-"}
              </div>
              <div className="text-slate-600">
                Source: {nhlSnapshot?.source || "-"}{nhlSnapshot?.stale ? " (stale)" : ""}
              </div>
            </div>
          </div>
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
