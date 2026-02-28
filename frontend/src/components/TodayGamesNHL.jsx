// frontend/src/components/TodayGamesNHL.jsx  (or wherever you keep it)
import { useEffect, useMemo, useState } from "react";
import { getBaseURL } from "../shared/getBaseURL.js";
import { todayET } from "../shared/timeUtils.js";

function nhlLogoUrl(teamId) {
  if (!teamId) return null;
  return `https://a.espncdn.com/i/teamlogos/nhl/500/${teamId}.png`;
}

function fmtEtTimeFromUtcIso(isoUtc) {
  if (!isoUtc) return "";
  const d = new Date(isoUtc);
  if (Number.isNaN(d.getTime())) return "";
  return (
    new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour: "numeric",
      minute: "2-digit",
    }).format(d) + " ET"
  );
}

function fmtCountdownFromUtcIso(isoUtc) {
  if (!isoUtc) return "";
  const t = new Date(isoUtc);
  if (Number.isNaN(t.getTime())) return "";

  const ms = t.getTime() - Date.now();
  if (ms <= 0) return "Live / Final";

  const totalMin = Math.floor(ms / 60000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;

  if (h >= 24) {
    const d = Math.floor(h / 24);
    const hh = h % 24;
    return `${d}d ${hh}h ${m}m`;
  }
  return `${h}h ${m}m`;
}

function normalizeNhlStatus(s) {
  const v = String(s || "").toLowerCase();
  if (!v) return "";
  if (v.includes("live")) return "LIVE";
  if (v.includes("final")) return "FINAL";
  if (v.includes("sched")) return "SCHEDULED";
  return v.toUpperCase();
}

function getLanding(liveDetails, gameId) {
  const payload = liveDetails?.[gameId];
  if (!payload || payload.ok === false) return null;
  return payload.data || null;
}

function getScorelineFromLanding(landing) {
  const a = landing?.awayTeam;
  const h = landing?.homeTeam;
  if (!a || !h) return null;
  if (a.score == null || h.score == null) return null;
  return `${a.score} - ${h.score}`;
}

function getClockLabelFromLanding(landing) {
  const state = String(landing?.gameState || "").toUpperCase();
  const clock = landing?.clock;
  const pd = landing?.periodDescriptor;

  const time = clock?.timeRemaining; // "12:34"
  const periodNum = pd?.number; // 1,2,3...
  const periodType = pd?.periodType; // "REG" | "OT" | "SO"

  if (state === "LIVE") {
    if (periodType === "SO") return "SO";
    if (periodType === "OT") return time ? `OT ${time}` : "OT";
    if (periodNum != null)
      return time ? `P${periodNum} ${time}` : `P${periodNum}`;
    return time || "LIVE";
  }

  if (state === "FINAL") {
    if (periodType === "SO") return "FINAL/SO";
    if (periodType === "OT") return "FINAL/OT";
    return "FINAL";
  }

  return null;
}

function formatRecord(team) {
  if (!team) return "Record: N/A";
  const wins = team.wins;
  const losses = team.losses;
  const otLosses = team.otLosses ?? team.otl ?? team.otlLosses;
  if (wins == null || losses == null) return "Record: N/A";
  return otLosses != null
    ? `Record: ${wins}-${losses}-${otLosses}`
    : `Record: ${wins}-${losses}`;
}

export default function TodayGamesNHL({ games = [], selectedDate = null }) {
  const targetDate = useMemo(
    () => String(selectedDate || "").trim() || todayET(),
    [selectedDate]
  );
  const todays = useMemo(
    () => (games || []).filter((g) => String(g.game_date) === String(targetDate)),
    [games, targetDate]
  );

  const sorted = useMemo(() => {
    return [...todays].sort(
      (a, b) => new Date(a.start_time_utc) - new Date(b.start_time_utc)
    );
  }, [todays]);

  // game_id -> { ok, data }
  const [liveDetails, setLiveDetails] = useState({});
  const [standings, setStandings] = useState([]);
  const [projectedGoalies, setProjectedGoalies] = useState([]);

  // Fetch landing for ALL games on slate (small list, avoids DB status mismatch)
  useEffect(() => {
    const ids = (sorted || []).map((g) => g.game_id);
    if (ids.length === 0) return;

    let cancelled = false;

    async function fetchLandingBatch() {
      try {
        const pairs = await Promise.all(
          ids.map(async (id) => {
            const res = await fetch(
              `${getBaseURL()}/api/nhl/gamecenter/${id}/landing`
            );
            const j = await res.json().catch(() => ({}));
            return [id, j];
          })
        );

        if (cancelled) return;

        setLiveDetails((prev) => {
          const next = { ...prev };
          for (const [id, payload] of pairs) next[id] = payload;
          return next;
        });
      } catch (e) {
        console.error("NHL landing fetch failed:", e);
      }
    }

    fetchLandingBatch();
    const interval = setInterval(fetchLandingBatch, 15000);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [sorted]);

  useEffect(() => {
    let cancelled = false;

    async function loadSupportData() {
      try {
        const [standingsRes, goaliesRes] = await Promise.all([
          fetch(
            `${getBaseURL()}/api/nhl/standings?date=${encodeURIComponent(targetDate)}`
          ),
          fetch(
            `${getBaseURL()}/api/nhl/goalies/projected?date=${encodeURIComponent(targetDate)}&limit=100`
          ),
        ]);

        const standingsJson = await standingsRes.json().catch(() => ({}));
        const goaliesJson = await goaliesRes.json().catch(() => ({}));

        if (cancelled) return;

        setStandings(Array.isArray(standingsJson?.standings) ? standingsJson.standings : []);
        setProjectedGoalies(Array.isArray(goaliesJson?.rows) ? goaliesJson.rows : []);
      } catch (e) {
        console.error("NHL support data fetch failed:", e);
      }
    }

    loadSupportData();
    return () => {
      cancelled = true;
    };
  }, [targetDate]);

  // Force re-render so countdown ticks even if no state changes
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 60000);
    return () => clearInterval(id);
  }, []);

  const standingsByAbbr = useMemo(() => {
    const map = new Map();
    for (const row of standings) {
      const abbr =
        row?.teamAbbrev?.default ||
        row?.teamAbbrev ||
        row?.teamCommonName?.default ||
        row?.teamName?.default ||
        null;
      if (abbr) map.set(String(abbr).toUpperCase(), row);
    }
    return map;
  }, [standings]);

  const projectedGoalieByTeamGame = useMemo(() => {
    const map = new Map();
    for (const row of projectedGoalies) {
      const key = `${row.game_id}::${row.team_id}`;
      map.set(key, row);
    }
    return map;
  }, [projectedGoalies]);

  return (
    <section className="w-full pp-card p-4">
      <h2 className="text-xl font-bold text-slate-900 text-center mb-1">
        🗓 Today’s Games
      </h2>
      <p className="text-sm text-slate-500 text-center mb-4">
        Live from NHL • ET Displayed
      </p>

      {sorted.length === 0 ? (
        <p className="text-center text-slate-500">No games scheduled.</p>
      ) : (
        <ul className="space-y-4">
          {sorted.map((g) => {
            const away = g.away_abbr;
            const home = g.home_abbr;

            const fallbackAwayLogo = nhlLogoUrl(g.away_team_id);
            const fallbackHomeLogo = nhlLogoUrl(g.home_team_id);

            const timeEt = fmtEtTimeFromUtcIso(g.start_time_utc);
            const cd = fmtCountdownFromUtcIso(g.start_time_utc);
            const status = normalizeNhlStatus(g.status);

            const landing = getLanding(liveDetails, g.game_id);

            const liveState = landing
              ? String(landing.gameState || "").toUpperCase()
              : null;

            const isLive = liveState === "LIVE";
            const isFinal = liveState === "FINAL";

            const scoreline = landing ? getScorelineFromLanding(landing) : null;
            const clockLabel = landing
              ? getClockLabelFromLanding(landing)
              : null;

            // Prefer NHL-provided SVG logos, fallback to ESPN PNG
            const awayLogoSrc = landing?.awayTeam?.logo || fallbackAwayLogo;
            const homeLogoSrc = landing?.homeTeam?.logo || fallbackHomeLogo;
            const awayRecord = formatRecord(standingsByAbbr.get(String(away || "").toUpperCase()));
            const homeRecord = formatRecord(standingsByAbbr.get(String(home || "").toUpperCase()));
            const awayGoalie = projectedGoalieByTeamGame.get(`${g.game_id}::${g.away_team_id}`);
            const homeGoalie = projectedGoalieByTeamGame.get(`${g.game_id}::${g.home_team_id}`);

            return (
              <li
                key={g.game_id}
                className="pp-chip grid grid-cols-[1fr_auto_1fr] items-center gap-4 p-4 rounded-lg max-w-5xl mx-auto"
              >
                {/* Away */}
                <div className="flex flex-col items-start gap-2 max-w-[240px]">
                  <div className="flex items-center gap-3">
                    {awayLogoSrc ? (
                      <img
                        src={awayLogoSrc}
                        alt={away}
                        className="w-20 h-20 object-contain shrink-0"
                      />
                    ) : null}
                    <span className="text-3xl font-bold tracking-tight text-slate-900 break-words">
                      {away}
                    </span>
                  </div>
                  <div className="text-sm text-slate-500">{awayRecord}</div>
                  <div className="text-sm text-slate-500">
                    Projected G: {awayGoalie?.goalie_name || "TBD"}
                  </div>
                </div>

                {/* Game Info */}
                <div className="flex flex-col items-center text-center gap-1">
                  {isLive || isFinal ? (
                    <>
                      <span className="text-lg font-semibold">
                        {scoreline ?? "-"}
                      </span>
                      <span className="text-sm text-slate-600">
                        {clockLabel ?? (isLive ? "LIVE" : "FINAL")}
                      </span>
                    </>
                  ) : (
                    <>
                      <span className="text-lg font-semibold">{timeEt}</span>
                      <span className="text-sm text-slate-600">{cd}</span>
                    </>
                  )}

                  <span className="text-sm text-slate-700">
                    {isLive ? "LIVE" : isFinal ? "FINAL" : status}
                  </span>
                </div>

                {/* Home */}
                <div className="flex flex-col items-end gap-2 text-right ml-auto max-w-[240px]">
                  <div className="flex items-center gap-3 justify-end">
                    <span className="text-3xl font-bold tracking-tight text-slate-900 break-words">
                      {home}
                    </span>
                    {homeLogoSrc ? (
                      <img
                        src={homeLogoSrc}
                        alt={home}
                        className="w-20 h-20 object-contain shrink-0"
                      />
                    ) : null}
                  </div>
                  <div className="text-sm text-slate-500">{homeRecord}</div>
                  <div className="text-sm text-slate-500">
                    Projected G: {homeGoalie?.goalie_name || "TBD"}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
