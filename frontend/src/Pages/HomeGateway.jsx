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

function snapshotChip(snapshot) {
  if (!snapshot) return { label: "Unavailable", tone: "bg-rose-50 text-rose-700 border-rose-200" };
  if (snapshot.stale) return { label: "Stale", tone: "bg-amber-50 text-amber-700 border-amber-200" };
  return { label: "Live", tone: "bg-emerald-50 text-emerald-700 border-emerald-200" };
}

function agoLabel(isoTs) {
  if (!isoTs) return "-";
  const ts = new Date(isoTs).getTime();
  if (!Number.isFinite(ts)) return "-";
  const delta = Date.now() - ts;
  if (delta < 60_000) return "just now";
  const mins = Math.floor(delta / 60_000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 48) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function mlbSlateState(snapshot) {
  const teamsTracked = Array.isArray(snapshot?.records) ? snapshot.records.length : 0;
  if (!snapshot) return { label: "Unavailable", tone: "text-rose-700" };
  if (teamsTracked > 0) return { label: "Active today", tone: "text-emerald-700" };
  return { label: "No slate today", tone: "text-slate-600" };
}

function nhlSlateState(snapshot) {
  const gamesToday = Number(snapshot?.components?.games_today?.count || 0);
  if (!snapshot) return { label: "Unavailable", tone: "text-rose-700" };
  if (gamesToday > 0) return { label: "Active today", tone: "text-emerald-700" };
  return { label: "No slate today", tone: "text-slate-600" };
}

export default function HomeGateway() {
  const [snapshotLoading, setSnapshotLoading] = useState(true);
  const [mlbSnapshot, setMlbSnapshot] = useState(null);
  const [nhlSnapshot, setNhlSnapshot] = useState(null);
  const [snapshotError, setSnapshotError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);
  const mlbChip = snapshotChip(mlbSnapshot);
  const nhlChip = snapshotChip(nhlSnapshot);
  const mlbState = mlbSlateState(mlbSnapshot);
  const nhlState = nhlSlateState(nhlSnapshot);

  const loadSnapshot = async () => {
    setSnapshotLoading(true);
    setSnapshotError("");
    const [mlb, nhl] = await Promise.all([
      fetchJson("/api/mlb/standings"),
      fetchJson("/api/nhl/slate/meta"),
    ]);
    setMlbSnapshot(mlb.ok && mlb.body?.ok ? mlb.body : null);
    setNhlSnapshot(nhl.ok && nhl.body?.ok ? nhl.body : null);
    if ((!mlb.ok || !mlb.body?.ok) && (!nhl.ok || !nhl.body?.ok)) {
      setSnapshotError("Snapshot data unavailable right now.");
    }
    setLastUpdated(new Date().toISOString());
    setSnapshotLoading(false);
  };

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      if (!mounted) return;
      await loadSnapshot();
    };
    run();
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
            <div className="flex items-center gap-2">
              <span className="text-xs text-slate-500">
                {snapshotLoading
                  ? "Loading..."
                  : lastUpdated
                    ? `Updated ${new Date(lastUpdated).toLocaleTimeString()}`
                    : "Backend-owned status"}
              </span>
              <button
                type="button"
                onClick={loadSnapshot}
                disabled={snapshotLoading}
                className="pp-btn pp-btn-secondary pp-btn-sm text-xs"
              >
                {snapshotLoading ? "Refreshing..." : "Refresh snapshot"}
              </button>
            </div>
          </div>
          {snapshotError ? (
            <div className="mt-2 text-xs text-amber-700">{snapshotError}</div>
          ) : null}
          <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
              <div className="flex items-center justify-between gap-2">
                <div className="font-semibold text-slate-900">MLB</div>
                <span
                  className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium ${mlbChip.tone}`}
                >
                  {mlbChip.label}
                </span>
              </div>
              <div className="text-slate-600 mt-1">
                Teams tracked: {Array.isArray(mlbSnapshot?.records) ? mlbSnapshot.records.length : "-"}
              </div>
              <div className={`text-xs font-medium mt-1 ${mlbState.tone}`}>{mlbState.label}</div>
              <div className="text-slate-600">
                Source: {mlbSnapshot?.source || "-"}{mlbSnapshot?.stale ? " (stale)" : ""}
              </div>
              <div className="text-slate-600">
                As of: {mlbSnapshot?.cached_at ? new Date(mlbSnapshot.cached_at).toLocaleString() : "-"}
              </div>
              <div className="text-slate-500 text-xs">
                Cache age: {agoLabel(mlbSnapshot?.cached_at)}
              </div>
              <div className="mt-2">
                <PrefetchLink
                  to="/mlb"
                  className="pp-btn pp-btn-ghost pp-btn-sm text-xs"
                >
                  Open MLB
                </PrefetchLink>
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
              <div className="flex items-center justify-between gap-2">
                <div className="font-semibold text-slate-900">NHL</div>
                <span
                  className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium ${nhlChip.tone}`}
                >
                  {nhlChip.label}
                </span>
              </div>
              <div className="text-slate-600 mt-1">
                Components healthy: {nhlSnapshot?.components ? Object.values(nhlSnapshot.components).filter((c) => c?.ok === true).length : "-"}
              </div>
              <div className={`text-xs font-medium mt-1 ${nhlState.tone}`}>{nhlState.label}</div>
              <div className="text-slate-600">
                Source: {nhlSnapshot?.source || "-"}{nhlSnapshot?.stale ? " (stale)" : ""}
              </div>
              <div className="text-slate-600">
                As of: {nhlSnapshot?.cached_at ? new Date(nhlSnapshot.cached_at).toLocaleString() : "-"}
              </div>
              <div className="text-slate-500 text-xs">
                Cache age: {agoLabel(nhlSnapshot?.cached_at)}
              </div>
              <div className="mt-2">
                <PrefetchLink
                  to="/nhl"
                  className="pp-btn pp-btn-ghost pp-btn-sm text-xs"
                >
                  Open NHL
                </PrefetchLink>
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
