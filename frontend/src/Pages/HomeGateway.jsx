import React, { useEffect, useRef, useState } from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";
import { isOpsUser } from "../shared/opsAccess.js";

const HOME_SNAPSHOT_CACHE_KEY = "proppadia_home_snapshot_v1";

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

async function fetchJsonTimed(path) {
  const started = performance.now();
  const res = await fetchJson(path);
  return { ...res, durationMs: Math.round(performance.now() - started) };
}

function snapshotChip(snapshot) {
  if (!snapshot)
    return {
      label: "Unavailable",
      tone: "bg-rose-50 text-rose-700 border-rose-200",
    };
  if (snapshot.stale)
    return {
      label: "Stale",
      tone: "bg-amber-50 text-amber-700 border-amber-200",
    };
  return {
    label: "Healthy",
    tone: "bg-emerald-50 text-emerald-700 border-emerald-200",
  };
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
  const gamesToday = Number(snapshot?.totalGames || 0);
  if (!snapshot)
    return { label: "Schedule unavailable", tone: "text-amber-700" };
  if (gamesToday > 0)
    return { label: "Active today", tone: "text-emerald-700" };
  return { label: "No games today", tone: "text-slate-600" };
}

function nhlSlateState(snapshot) {
  const gamesToday = Number(snapshot?.components?.games_today?.count || 0);
  if (!snapshot) return { label: "Unavailable", tone: "text-rose-700" };
  if (gamesToday > 0)
    return { label: "Active today", tone: "text-emerald-700" };
  return { label: "No slate today", tone: "text-slate-600" };
}

export default function HomeGateway() {
  const { user } = useAuth();
  const [snapshotLoading, setSnapshotLoading] = useState(true);
  const [mlbSnapshot, setMlbSnapshot] = useState(null);
  const [mlbScheduleSnapshot, setMlbScheduleSnapshot] = useState(null);
  const [nhlSnapshot, setNhlSnapshot] = useState(null);
  const [snapshotError, setSnapshotError] = useState("");
  const [snapshotErrorDetail, setSnapshotErrorDetail] = useState(null);
  const [showSnapshotErrorDetail, setShowSnapshotErrorDetail] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [lastFetchMs, setLastFetchMs] = useState(null);
  const snapshotReqRef = useRef(0);
  const mlbChip = snapshotChip(mlbSnapshot);
  const nhlChip = snapshotChip(nhlSnapshot);
  const mlbState = mlbSlateState(mlbScheduleSnapshot);
  const nhlState = nhlSlateState(nhlSnapshot);
  const mlbGamesToday = Number.isFinite(Number(mlbScheduleSnapshot?.totalGames))
    ? Number(mlbScheduleSnapshot.totalGames)
    : 0;
  const nhlGamesToday = Number(
    nhlSnapshot?.components?.games_today?.count || 0
  );
  const nhlPropsToday = Number(
    nhlSnapshot?.components?.props_today?.count || 0
  );
  const nhlSogRowsToday = Number(nhlSnapshot?.components?.sog?.count || 0);
  const nhlSavesRowsToday = Number(nhlSnapshot?.components?.saves?.count || 0);
  const hasOpsAccess = isOpsUser(user);

  const loadSnapshot = async () => {
    const reqId = snapshotReqRef.current + 1;
    snapshotReqRef.current = reqId;
    setSnapshotLoading(true);
    setSnapshotError("");
    setShowSnapshotErrorDetail(false);
    const [mlb, mlbSchedule, nhl] = await Promise.all([
      fetchJsonTimed("/api/mlb/standings"),
      fetchJsonTimed("/api/mlb/schedule"),
      fetchJsonTimed("/api/nhl/slate/meta"),
    ]);
    if (snapshotReqRef.current !== reqId) return;
    setLastFetchMs(
      (mlb.durationMs || 0) +
        (mlbSchedule.durationMs || 0) +
        (nhl.durationMs || 0)
    );
    setMlbSnapshot(mlb.ok && mlb.body?.ok ? mlb.body : null);
    setMlbScheduleSnapshot(
      mlbSchedule.ok && mlbSchedule.body ? mlbSchedule.body : null
    );
    setNhlSnapshot(nhl.ok && nhl.body?.ok ? nhl.body : null);
    if ((!mlb.ok || !mlb.body?.ok) && (!nhl.ok || !nhl.body?.ok)) {
      setSnapshotError("Snapshot data unavailable right now.");
      setSnapshotErrorDetail({
        mlb_status: mlb.status,
        mlb_ok: Boolean(mlb.body?.ok),
        mlb_schedule_status: mlbSchedule.status,
        mlb_schedule_ok: Boolean(mlbSchedule.body),
        nhl_status: nhl.status,
        nhl_ok: Boolean(nhl.body?.ok),
      });
    } else {
      setSnapshotErrorDetail(null);
    }
    setLastUpdated(new Date().toISOString());
    setSnapshotLoading(false);
  };

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(HOME_SNAPSHOT_CACHE_KEY);
      if (!raw) return;
      const cached = JSON.parse(raw);
      if (cached && typeof cached === "object") {
        setMlbSnapshot(cached.mlbSnapshot || null);
        setMlbScheduleSnapshot(cached.mlbScheduleSnapshot || null);
        setNhlSnapshot(cached.nhlSnapshot || null);
        setLastUpdated(cached.lastUpdated || null);
        setLastFetchMs(
          Number.isFinite(Number(cached.lastFetchMs))
            ? Number(cached.lastFetchMs)
            : null
        );
      }
    } catch {
      // ignore malformed cache
    }
  }, []);

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

  useEffect(() => {
    const timer = window.setInterval(() => {
      loadSnapshot();
    }, 300000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        HOME_SNAPSHOT_CACHE_KEY,
        JSON.stringify({
          mlbSnapshot,
          mlbScheduleSnapshot,
          nhlSnapshot,
          lastUpdated,
          lastFetchMs,
        })
      );
    } catch {
      // ignore local storage write errors
    }
  }, [lastFetchMs, lastUpdated, mlbScheduleSnapshot, mlbSnapshot, nhlSnapshot]);

  return (
    <div className="min-h-screen pp-page px-4 py-10">
      <div className="max-w-5xl mx-auto space-y-6">
        <section className="pp-card p-6 overflow-hidden">
          <div className="flex flex-col gap-6 md:flex-row md:items-stretch md:justify-between">
            <div className="max-w-3xl">
              <span className="inline-flex items-center rounded-full border border-sky-200 bg-sky-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.08em] text-sky-800">
                Decision Workspace
              </span>
              <h1 className="mt-3 text-4xl md:text-5xl font-bold text-slate-900 leading-tight">
                The line tells a story.
                <br />
                We help you read it.
              </h1>
              <p className="mt-4 text-[1.05rem] text-slate-700 leading-8">
                See where prices sit, how they move, and what it means.
              </p>
              <div className="mt-4 rounded-xl border border-emerald-100 bg-gradient-to-r from-emerald-50 to-sky-50 px-4 py-3 text-slate-700">
                <ul className="space-y-1.5 text-[0.95rem] leading-7">
                  <li>Track how prices move across the slate.</li>
                  <li>
                    Understand which numbers are stable and which are shifting.
                  </li>
                  <li>Evaluate players with context, not guesses.</li>
                </ul>
              </div>
            </div>
            <div className="w-full max-w-xs md:w-64 flex flex-col gap-3 md:items-stretch md:self-stretch md:justify-between">
              <div className="hidden md:block rounded-xl border border-slate-200 bg-gradient-to-b from-slate-50 to-white px-4 py-4">
                <p className="mt-2 text-sm text-slate-600 leading-6">
                  Better decisions are made after understanding the market
                  behind the number.
                </p>
              </div>
              <div className="flex flex-col gap-3">
                <PrefetchLink
                  to="/mlb/today"
                  className="pp-btn pp-btn-primary pp-btn-md w-full text-center"
                >
                  Open MLB Today
                </PrefetchLink>
                <PrefetchLink
                  to="/players/mlb"
                  className="pp-btn pp-btn-secondary pp-btn-md w-full text-center"
                >
                  Explore Player Research
                </PrefetchLink>
                {hasOpsAccess ? (
                  <PrefetchLink
                    to="/archive"
                    className="pp-btn pp-btn-ghost pp-btn-md w-full text-center text-slate-600"
                  >
                    Browse Archive
                  </PrefetchLink>
                ) : null}
              </div>
            </div>
          </div>
        </section>

        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="pp-card p-5 h-full flex flex-col">
            <div className="flex items-center justify-between gap-3">
              <h2 className="text-base font-semibold text-slate-900">
                MLB Preview
              </h2>
              <span
                className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium ${mlbChip.tone}`}
              >
                {mlbChip.label}
              </span>
            </div>
            <div className="mt-2 text-sm text-slate-600">
              <div>
                Today&apos;s schedule: <strong>{mlbGamesToday}</strong> games
              </div>
              <div className={`mt-1 text-xs font-medium ${mlbState.tone}`}>
                {mlbState.label}
              </div>
              <div className="mt-2 text-xs text-slate-500">
                {mlbGamesToday > 0
                  ? "Use MLB Today to see current market position, timing, and player context."
                  : "No active slate right now. Research tools remain available."}
              </div>
            </div>
            <div className="mt-auto pt-8">
              <PrefetchLink
                to="/mlb/today"
                className="pp-btn pp-btn-ghost pp-btn-sm text-xs"
              >
                Open MLB Today
              </PrefetchLink>
            </div>
          </div>

          <div className="pp-card p-5 h-full flex flex-col">
            <div className="flex items-center justify-between gap-3">
              <h2 className="text-base font-semibold text-slate-900">
                NHL Preview
              </h2>
              <span
                className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium ${nhlChip.tone}`}
              >
                {nhlChip.label}
              </span>
            </div>
            <div className="mt-2 text-sm text-slate-600">
              <div>
                Games: <strong>{nhlGamesToday}</strong>
              </div>
              <div>
                Prop rows: <strong>{nhlPropsToday}</strong>
              </div>
              <div>
                SOG rows: <strong>{nhlSogRowsToday}</strong> • Saves rows:{" "}
                <strong>{nhlSavesRowsToday}</strong>
              </div>
              <div className={`mt-1 text-xs font-medium ${nhlState.tone}`}>
                {nhlState.label}
              </div>
              <div className="mt-2 text-xs text-slate-500">
                Home preview is intentionally limited. Open the NHL workspace
                for detailed market context.
              </div>
            </div>
            <div className="mt-auto pt-8">
              <PrefetchLink
                to="/nhl"
                className="pp-btn pp-btn-ghost pp-btn-sm text-xs"
              >
                Open NHL
              </PrefetchLink>
            </div>
          </div>
        </section>

        <section className="pp-card p-5">
          <h2 className="text-base font-semibold text-slate-900">
            Workspace Tools
          </h2>
          <p className="mt-1 text-sm text-slate-600">
            We show what&apos;s happening and why it matters across market,
            timing, and player context.
          </p>
          <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
            <PrefetchLink
              to="/mlb/today"
              className="block rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 cursor-pointer transition hover:border-slate-300 hover:shadow-sm"
            >
              <div className="font-semibold text-slate-900">
                Market Workspace
              </div>
              <div className="mt-1 text-xs text-slate-600">
                Read price position, movement, and stability for the live slate.
              </div>
              <div className="mt-2 text-[11px] text-slate-500">
                MLB Today
              </div>
            </PrefetchLink>
            <PrefetchLink
              to="/players/mlb"
              className="block rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 cursor-pointer transition hover:border-slate-300 hover:shadow-sm"
            >
              <div className="font-semibold text-slate-900">
                Player Research
              </div>
              <div className="mt-1 text-xs text-slate-600">
                Team browsers, player pages, and profile-level context workflows.
              </div>
              <div className="mt-2 text-[11px] text-slate-500">
                Research tools
              </div>
            </PrefetchLink>
            <PrefetchLink
              to="/watchlist"
              className="block rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 cursor-pointer transition hover:border-slate-300 hover:shadow-sm"
            >
              <div className="font-semibold text-slate-900">
                Watchlist
              </div>
              <div className="mt-1 text-xs text-slate-600">
                Track players you follow and jump back into their research context.
              </div>
              <div className="mt-2 text-[11px] text-slate-500">
                Ongoing review
              </div>
            </PrefetchLink>
          </div>
        </section>

        <details className="pp-card p-5">
          <summary className="cursor-pointer text-sm font-semibold text-slate-800">
            System Snapshot (technical)
          </summary>
          <div className="mt-3">
            <div className="flex items-center justify-between gap-3">
              <span className="text-xs text-slate-500">
                {snapshotLoading
                  ? "Loading..."
                  : lastUpdated
                  ? `Updated ${new Date(lastUpdated).toLocaleTimeString()}`
                  : "Backend-owned status"}
              </span>
              <div className="flex items-center gap-2">
                {lastFetchMs !== null ? (
                  <span className="text-[11px] text-slate-400">
                    Fetch {lastFetchMs}ms
                  </span>
                ) : null}
                <span className="text-[11px] text-slate-400">
                  Auto refresh 5m
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
              <div className="mt-2 text-xs text-amber-700">
                {snapshotError}
                {snapshotErrorDetail ? (
                  <>
                    <button
                      type="button"
                      onClick={() => setShowSnapshotErrorDetail((v) => !v)}
                      className="ml-2 underline"
                    >
                      {showSnapshotErrorDetail ? "Hide detail" : "Show detail"}
                    </button>
                    {showSnapshotErrorDetail ? (
                      <pre className="mt-2 whitespace-pre-wrap break-words text-[11px] text-amber-800">
                        {JSON.stringify(snapshotErrorDetail, null, 2)}
                      </pre>
                    ) : null}
                  </>
                ) : null}
              </div>
            ) : null}
            <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
                <div className="font-semibold text-slate-900">MLB source</div>
                <div className="text-slate-600 mt-1">
                  Source: {mlbSnapshot?.source || "-"}
                  {mlbSnapshot?.stale ? " (stale)" : ""}
                </div>
                <div className="text-slate-600">
                  As of:{" "}
                  {mlbSnapshot?.cached_at
                    ? new Date(mlbSnapshot.cached_at).toLocaleString()
                    : "-"}
                </div>
                <div className="text-slate-500 text-xs">
                  Cache age: {agoLabel(mlbSnapshot?.cached_at)}
                </div>
              </div>
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3">
                <div className="font-semibold text-slate-900">NHL source</div>
                <div className="text-slate-600 mt-1">
                  Source: {nhlSnapshot?.source || "-"}
                  {nhlSnapshot?.stale ? " (stale)" : ""}
                </div>
                <div className="text-slate-600">
                  As of:{" "}
                  {nhlSnapshot?.cached_at
                    ? new Date(nhlSnapshot.cached_at).toLocaleString()
                    : "-"}
                </div>
                <div className="text-slate-500 text-xs">
                  Cache age: {agoLabel(nhlSnapshot?.cached_at)}
                </div>
              </div>
            </div>
          </div>
        </details>
      </div>
    </div>
  );
}
