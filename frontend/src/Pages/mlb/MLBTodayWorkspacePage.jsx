import React, { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { getBaseURL } from "../../shared/getBaseURL.js";
import { todayET } from "../../shared/timeUtils.js";

const DASH = "—";

function asNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function fmtNumber(v, digits = 2) {
  const n = asNumber(v);
  if (n === null) return DASH;
  return n.toFixed(digits);
}

function fmtPct(v) {
  const n = asNumber(v);
  if (n === null) return DASH;
  return `${(n * 100).toFixed(1)}%`;
}

function fmtPrice(v) {
  const n = asNumber(v);
  if (n === null) return DASH;
  const rounded = Math.round(n);
  const absDiff = Math.abs(n - rounded);
  const body = absDiff < 0.05 ? String(rounded) : n.toFixed(1);
  const sign = n > 0 ? "+" : "";
  return `${sign}${body}`;
}

function propLabel(propType) {
  const p = String(propType || "").trim().toLowerCase();
  const map = {
    hits: "Hits",
    total_bases: "Total Bases",
    hits_runs_rbis: "Hits + Runs + RBIs",
    strikeouts_pitching: "Pitcher Strikeouts",
    outs_recorded: "Pitcher Outs",
    earned_runs: "Earned Runs",
    walks_allowed: "Walks Allowed",
    hits_allowed: "Hits Allowed",
    runs_scored: "Runs",
    rbis: "RBIs",
    walks: "Walks",
    strikeouts_batting: "Batter Strikeouts",
  };
  return map[p] || p.replaceAll("_", " ");
}

function timingLabel(signal) {
  const s = String(signal || "").trim().toUpperCase();
  const map = {
    EARLY: "Early better",
    WAIT: "Improving now",
    VOLATILE: "Volatile market",
    STABLE: "Stable market",
  };
  return map[s] || DASH;
}

function streakLabel(label) {
  const s = String(label || "").trim().toUpperCase();
  const map = {
    HOT: "Hot",
    COLD: "Cold",
    NEUTRAL: "Neutral",
    ABOVE_BASELINE: "Above baseline",
    BELOW_BASELINE: "Below baseline",
  };
  return map[s] || DASH;
}

function consistencySubLabel(score) {
  const n = asNumber(score);
  if (n === null) return DASH;
  if (n >= 70) return "More stable";
  if (n <= 35) return "More volatile";
  return "Mixed";
}

function bucketCounts(rows, field) {
  const counts = {};
  for (const r of rows) {
    const key = String(r?.[field] || "").trim() || "UNKNOWN";
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1]);
}

export default function MLBTodayWorkspacePage() {
  const location = useLocation();
  const debugMode = useMemo(() => {
    const params = new URLSearchParams(location.search || "");
    return params.get("debug") === "1";
  }, [location.search]);

  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState({});
  const [filters, setFilters] = useState({
    prop_type: "",
    team: "",
    timing_signal: "",
    player_query: "",
  });

  const slateDate = useMemo(() => todayET(), []);

  const propOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => String(r.prop_type || "").trim()).filter(Boolean))).sort(),
    [rows]
  );
  const teamOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => String(r.team || "").trim()).filter(Boolean))).sort(),
    [rows]
  );
  const timingOptions = useMemo(
    () => Array.from(new Set(rows.map((r) => String(r.timing_signal || "").trim()).filter(Boolean))).sort(),
    [rows]
  );

  const timingCounts = useMemo(() => bucketCounts(rows, "timing_signal"), [rows]);
  const streakCounts = useMemo(() => bucketCounts(rows, "streak_context_label"), [rows]);

  const debugSlices = useMemo(() => {
    const withDelta = rows.filter((r) => asNumber(r.value_vs_market) !== null);
    const withConsistency = rows.filter((r) => asNumber(r.consistency_score) !== null);
    const byDeltaDesc = [...withDelta].sort((a, b) => Number(b.value_vs_market) - Number(a.value_vs_market));
    const byDeltaAsc = [...withDelta].sort((a, b) => Number(a.value_vs_market) - Number(b.value_vs_market));
    const byConsistencyDesc = [...withConsistency].sort(
      (a, b) => Number(b.consistency_score) - Number(a.consistency_score)
    );
    const byConsistencyAsc = [...withConsistency].sort(
      (a, b) => Number(a.consistency_score) - Number(b.consistency_score)
    );
    return {
      topPositive: byDeltaDesc.slice(0, 5),
      mostNegative: byDeltaAsc.slice(0, 5),
      highestConsistency: byConsistencyDesc.slice(0, 5),
      lowestConsistency: byConsistencyAsc.slice(0, 5),
      byTiming: timingCounts,
      byStreak: streakCounts,
    };
  }, [rows, timingCounts, streakCounts]);

  useEffect(() => {
    let isMounted = true;
    async function run() {
      setLoading(true);
      setError("");
      try {
        const qs = new URLSearchParams();
        qs.set("limit", "1000");
        qs.set("offset", "0");
        if (filters.prop_type) qs.set("prop_type", filters.prop_type);
        if (filters.team) qs.set("team", filters.team);
        if (filters.timing_signal) qs.set("timing_signal", filters.timing_signal);
        if (filters.player_query.trim()) qs.set("player_query", filters.player_query.trim());

        const url = `${getBaseURL()}/api/mlb/today/workspace?${qs.toString()}`;
        const res = await fetch(url, { credentials: "include" });
        const data = await res.json();
        if (!res.ok) throw new Error(data?.detail || `${res.status} ${res.statusText}`);
        if (!isMounted) return;
        const nextRows = Array.isArray(data?.rows) ? data.rows : [];
        setRows(nextRows);
        setTotal(Number(data?.total) || nextRows.length);
      } catch (e) {
        if (!isMounted) return;
        setRows([]);
        setTotal(0);
        setError(e?.message || "Failed to load MLB today workspace.");
      } finally {
        if (isMounted) setLoading(false);
      }
    }
    run();
    return () => {
      isMounted = false;
    };
  }, [filters.prop_type, filters.team, filters.timing_signal, filters.player_query]);

  function toggleRow(key) {
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-7xl mx-auto px-4 pb-10">
        <div className="flex items-baseline justify-between mb-4">
          <h2 className="text-2xl font-bold text-slate-900">MLB Today Workspace</h2>
          <div className="text-sm text-slate-500">Slate (ET): {slateDate}</div>
        </div>

        <div className="pp-card p-3 mb-4 flex flex-wrap gap-3 items-end">
          <div>
            <div className="text-xs text-slate-500 mb-1">Player</div>
            <input
              className="border rounded px-2 py-1 text-sm min-w-[180px]"
              value={filters.player_query}
              onChange={(e) => setFilters((f) => ({ ...f, player_query: e.target.value }))}
              placeholder="Search player"
            />
          </div>
          <div>
            <div className="text-xs text-slate-500 mb-1">Prop</div>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={filters.prop_type}
              onChange={(e) => setFilters((f) => ({ ...f, prop_type: e.target.value }))}
            >
              <option value="">All</option>
              {propOptions.map((p) => (
                <option key={p} value={p}>
                  {propLabel(p)}
                </option>
              ))}
            </select>
          </div>
          <div>
            <div className="text-xs text-slate-500 mb-1">Team</div>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={filters.team}
              onChange={(e) => setFilters((f) => ({ ...f, team: e.target.value }))}
            >
              <option value="">All</option>
              {teamOptions.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div>
            <div className="text-xs text-slate-500 mb-1">Timing</div>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={filters.timing_signal}
              onChange={(e) => setFilters((f) => ({ ...f, timing_signal: e.target.value }))}
            >
              <option value="">All</option>
              {timingOptions.map((t) => (
                <option key={t} value={t}>
                  {timingLabel(t)}
                </option>
              ))}
            </select>
          </div>
          <div className="text-xs text-slate-500 ml-auto">Rows: {rows.length} / {total}</div>
        </div>

        <div className="pp-card p-3 mb-4 flex flex-wrap gap-2">
          <div className="text-xs text-slate-500 mr-1">Timing mix:</div>
          {timingCounts.map(([label, count]) => (
            <span key={`timing-${label}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
              {timingLabel(label)}: {count}
            </span>
          ))}
          <div className="w-full" />
          <div className="text-xs text-slate-500 mr-1">Streak mix:</div>
          {streakCounts.map(([label, count]) => (
            <span key={`streak-${label}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
              {streakLabel(label)}: {count}
            </span>
          ))}
        </div>

        {debugMode ? (
          <div className="pp-card p-3 mb-4 text-xs text-slate-700">
            <div className="font-semibold text-slate-900 mb-2">Debug QA Slices</div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Highest +Δ vs market</div>
                {debugSlices.topPositive.map((r) => (
                  <div key={`pos-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}`}>
                    {r.player_name} | {propLabel(r.prop_type)} {fmtNumber(r.line, 1)} | {fmtPrice(r.value_vs_market)}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Most negative Δ vs market</div>
                {debugSlices.mostNegative.map((r) => (
                  <div key={`neg-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}`}>
                    {r.player_name} | {propLabel(r.prop_type)} {fmtNumber(r.line, 1)} | {fmtPrice(r.value_vs_market)}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Highest consistency</div>
                {debugSlices.highestConsistency.map((r) => (
                  <div key={`hi-c-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}`}>
                    {r.player_name} | {propLabel(r.prop_type)} | {fmtNumber(r.consistency_score, 1)}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Lowest consistency</div>
                {debugSlices.lowestConsistency.map((r) => (
                  <div key={`lo-c-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}`}>
                    {r.player_name} | {propLabel(r.prop_type)} | {fmtNumber(r.consistency_score, 1)}
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-2 text-slate-500">Use <code>?debug=1</code> in URL to show/hide this panel.</div>
          </div>
        ) : null}

        <div className="pp-card p-0 overflow-x-auto">
          {loading ? (
            <div className="p-4 text-slate-600 text-sm">Loading workspace…</div>
          ) : error ? (
            <div className="p-4 text-rose-700 text-sm">{error}</div>
          ) : rows.length === 0 ? (
            <div className="p-4 text-slate-600 text-sm">No rows for current filters.</div>
          ) : (
            <table className="min-w-full text-sm text-slate-800">
              <thead className="bg-slate-50">
                <tr className="text-left border-b border-slate-200">
                  <th className="py-2.5 px-3">Player</th>
                  <th className="py-2.5 px-3">Prop</th>
                  <th className="py-2.5 px-3 text-right">Line</th>
                  <th className="py-2.5 px-3 text-right">Best Price</th>
                  <th className="py-2.5 px-3 text-right">Market Median</th>
                  <th className="py-2.5 px-3 text-right">Δ vs Market</th>
                  <th className="py-2.5 px-3">Timing</th>
                  <th className="py-2.5 px-3">Streak</th>
                  <th className="py-2.5 px-3 text-right">Consistency</th>
                  <th className="py-2.5 px-3"></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const key = `${r.player_id}:${r.game_id}:${r.prop_type}:${r.line}`;
                  const isOpen = Boolean(expanded[key]);
                  return (
                    <React.Fragment key={key}>
                      <tr className="border-b border-slate-100 align-top">
                        <td className="py-2.5 px-3">
                          <div className="font-medium">{r.player_name || DASH}</div>
                          <div className="text-xs text-slate-500">
                            {(r.team || DASH)} vs {(r.opponent || DASH)}
                          </div>
                        </td>
                        <td className="py-2.5 px-3">{propLabel(r.prop_type)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums">{fmtNumber(r.line, 1)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums">{fmtPrice(r.best_price)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums">{fmtPrice(r.market_median)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums">{fmtPrice(r.value_vs_market)}</td>
                        <td className="py-2.5 px-3">
                          <div>{timingLabel(r.timing_signal)}</div>
                          <div className="text-xs text-slate-500">{r.timing_reason || DASH}</div>
                        </td>
                        <td className="py-2.5 px-3">
                          <div>{streakLabel(r.streak_context_label)}</div>
                          <div className="text-xs text-slate-500">count: {asNumber(r.streak_count) ?? 0}</div>
                        </td>
                        <td className="py-2.5 px-3 text-right tabular-nums">
                          <div>{fmtNumber(r.consistency_score, 1)}</div>
                          <div className="text-xs text-slate-500">{consistencySubLabel(r.consistency_score)}</div>
                        </td>
                        <td className="py-2.5 px-3">
                          <button
                            type="button"
                            className="text-xs border rounded px-2 py-1 hover:bg-slate-50"
                            onClick={() => toggleRow(key)}
                          >
                            {isOpen ? "Hide" : "Details"}
                          </button>
                        </td>
                      </tr>
                      {isOpen ? (
                        <tr className="border-b border-slate-100 bg-slate-50/60">
                          <td colSpan={10} className="py-2 px-3">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-x-4 gap-y-2 text-xs">
                              <div>Open over price: <strong>{fmtPrice(r.open_over_price)}</strong></div>
                              <div>Latest over price: <strong>{fmtPrice(r.latest_over_price)}</strong></div>
                              <div>Best over now: <strong>{fmtPrice(r.best_price)}</strong></div>
                              <div>Snapshots: <strong>{asNumber(r.num_snapshots) ?? 0}</strong></div>
                              <div>Over move from open: <strong>{fmtPrice(r.over_price_change_from_open)}</strong></div>
                              <div>Hit rate last 5: <strong>{fmtPct(r.hit_rate_last_5)}</strong></div>
                              <div>Hit rate last 10: <strong>{fmtPct(r.hit_rate_last_10)}</strong></div>
                              <div>Hit rate season: <strong>{fmtPct(r.hit_rate_season)}</strong></div>
                              <div>Last 5 avg: <strong>{fmtNumber(r.last_5_avg)}</strong></div>
                              <div>Last 10 avg: <strong>{fmtNumber(r.last_10_avg)}</strong></div>
                              <div>Season avg: <strong>{fmtNumber(r.season_avg)}</strong></div>
                              <div>Baseline delta: <strong>{fmtPct(r.baseline_delta)}</strong></div>
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
