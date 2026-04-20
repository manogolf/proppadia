import React, { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { getBaseURL } from "../../shared/getBaseURL.js";
import { todayET } from "../../shared/timeUtils.js";

const DASH = "—";

function asNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function isMissing(v) {
  if (v === null || v === undefined) return true;
  if (typeof v === "number") return !Number.isFinite(v);
  const s = String(v).trim().toLowerCase();
  return s === "" || s === "nan" || s === "null" || s === "undefined";
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

function fmtPctSigned(v) {
  const n = asNumber(v);
  if (n === null) return DASH;
  const sign = n > 0 || n === 0 ? "+" : "";
  return `${sign}${(n * 100).toFixed(1)}%`;
}

function fmtPrice(v, { forceSign = false } = {}) {
  const n = asNumber(v);
  if (n === null) return DASH;
  const rounded = Math.round(n);
  const absDiff = Math.abs(n - rounded);
  const body = absDiff < 0.05 ? String(rounded) : n.toFixed(1);
  const sign = n > 0 || (forceSign && n === 0) ? "+" : "";
  return `${sign}${body}`;
}

function withSelected(options, selected) {
  const base = Array.isArray(options) ? options : [];
  const val = String(selected || "").trim();
  if (!val) return base;
  return base.includes(val) ? base : [val, ...base];
}

function propLabel(propType) {
  const p = String(propType || "").trim().toLowerCase();
  const map = {
    hits: "Hits",
    total_bases: "Total bases",
    hits_runs_rbis: "Hits + Runs + RBIs",
    strikeouts_pitching: "Pitcher strikeouts",
    strikeouts_batting: "Batter strikeouts",
    outs_recorded: "Outs recorded",
    earned_runs: "Earned runs",
    walks_allowed: "Walks allowed",
    hits_allowed: "Hits allowed",
    runs_scored: "Runs",
    runs_batted_in: "RBIs",
    rbi: "RBIs",
    rbis: "RBIs",
    home_runs: "Home runs",
    walks: "Walks",
  };
  return map[p] || p.replaceAll("_", " ");
}

function timingLabel(signal) {
  const s = String(signal || "").trim().toUpperCase();
  const map = {
    EARLY: "Earlier price was better",
    WAIT: "Price has improved",
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

function coverageLabel(label) {
  const s = String(label || "").trim().toUpperCase();
  const map = {
    STRONG: "Strong coverage",
    GOOD: "Good coverage",
    LIMITED: "Limited coverage",
    THIN: "Thin coverage",
    UNRELIABLE: "Unreliable coverage",
  };
  return map[s] || DASH;
}

function bucketCounts(rows, field) {
  const counts = {};
  for (const r of rows) {
    const key = String(r?.[field] || "").trim() || "UNKNOWN";
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1]);
}

function groupedSamples(rows, field, sampleSize = 4) {
  const groups = {};
  const counts = {};
  for (const r of rows) {
    const key = String(r?.[field] || "").trim() || "UNKNOWN";
    counts[key] = (counts[key] || 0) + 1;
    if (!groups[key]) groups[key] = [];
    if (groups[key].length < sampleSize) groups[key].push(r);
  }
  return Object.keys(counts)
    .sort((a, b) => (counts[b] || 0) - (counts[a] || 0))
    .map((key) => ({ key, count: counts[key], rows: groups[key] || [] }));
}

function rowSnippet(r) {
  return `${r.player_name || DASH} | ${propLabel(r.prop_type)} ${fmtNumber(r.line, 1)}`;
}

function marketRangeLabel(rangeVal) {
  const n = asNumber(rangeVal);
  if (n === null) return DASH;
  if (n >= 80) return "Wide market";
  if (n <= 25) return "Tight market";
  return "Mixed market";
}

function marketRangeBucket(rangeVal) {
  const n = asNumber(rangeVal);
  if (n === null) return "missing";
  if (n < 25) return "<25";
  if (n < 50) return "25-49";
  if (n < 80) return "50-79";
  return "80+";
}

function compareDefaultRows(a, b) {
  const av = asNumber(a?.value_vs_market);
  const bv = asNumber(b?.value_vs_market);
  if (av === null && bv !== null) return 1;
  if (av !== null && bv === null) return -1;
  if (av !== null && bv !== null && av !== bv) return bv - av;

  const an = String(a?.player_name || "");
  const bn = String(b?.player_name || "");
  if (an !== bn) return an.localeCompare(bn);

  const ap = String(a?.prop_type || "");
  const bp = String(b?.prop_type || "");
  if (ap !== bp) return ap.localeCompare(bp);

  const al = asNumber(a?.line) ?? 0;
  const bl = asNumber(b?.line) ?? 0;
  return al - bl;
}

const VIEW_PRESETS = [
  { key: "all", label: "All rows" },
  { key: "best_covered", label: "Best covered" },
  { key: "wide_markets", label: "Wide markets" },
  { key: "sparse_markets", label: "Sparse markets" },
];

export default function MLBTodayWorkspacePage() {
  const location = useLocation();
  const debugMode = useMemo(() => {
    const params = new URLSearchParams(location.search || "");
    return params.get("debug") === "1";
  }, [location.search]);

  const [rows, setRows] = useState([]);
  const [optionRows, setOptionRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState({});
  const [viewPreset, setViewPreset] = useState("all");
  const [filters, setFilters] = useState({
    prop_type: "",
    team: "",
    timing_signal: "",
    player_query: "",
  });

  const slateDate = useMemo(() => todayET(), []);

  const propOptions = useMemo(() => {
    const base = optionRows.length ? optionRows : rows;
    const options = Array.from(new Set(base.map((r) => String(r.prop_type || "").trim()).filter(Boolean))).sort();
    return withSelected(options, filters.prop_type);
  }, [optionRows, rows, filters.prop_type]);
  const teamOptions = useMemo(() => {
    const base = optionRows.length ? optionRows : rows;
    const options = Array.from(new Set(base.map((r) => String(r.team || "").trim()).filter(Boolean))).sort();
    return withSelected(options, filters.team);
  }, [optionRows, rows, filters.team]);
  const timingOptions = useMemo(() => {
    const base = optionRows.length ? optionRows : rows;
    const options = Array.from(new Set(base.map((r) => String(r.timing_signal || "").trim()).filter(Boolean))).sort();
    return withSelected(options, filters.timing_signal);
  }, [optionRows, rows, filters.timing_signal]);

  const sortedRows = useMemo(() => [...rows].sort(compareDefaultRows), [rows]);

  const displayedRows = useMemo(() => {
    if (viewPreset === "all") return sortedRows;
    if (viewPreset === "best_covered") {
      return sortedRows.filter((r) => {
        const c = String(r.coverage_quality_label || "").trim().toUpperCase();
        return c === "STRONG" || c === "GOOD";
      });
    }
    if (viewPreset === "wide_markets") {
      return sortedRows.filter((r) => {
        const range = asNumber(r.market_range);
        return range !== null && range >= 80;
      });
    }
    if (viewPreset === "sparse_markets") {
      return sortedRows.filter((r) => {
        const c = String(r.coverage_quality_label || "").trim().toUpperCase();
        return c === "LIMITED" || c === "THIN" || c === "UNRELIABLE";
      });
    }
    return sortedRows;
  }, [sortedRows, viewPreset]);

  const timingCounts = useMemo(() => bucketCounts(displayedRows, "timing_signal"), [displayedRows]);
  const streakCounts = useMemo(() => bucketCounts(displayedRows, "streak_context_label"), [displayedRows]);
  const coverageCounts = useMemo(() => bucketCounts(displayedRows, "coverage_quality_label"), [displayedRows]);
  const propDisplayCounts = useMemo(() => {
    return bucketCounts(
      displayedRows.map((r) => ({ prop_label: propLabel(r.prop_type) })),
      "prop_label"
    );
  }, [displayedRows]);

  const debugSlices = useMemo(() => {
    const withDelta = displayedRows.filter((r) => asNumber(r.value_vs_market) !== null);
    const withConsistency = displayedRows.filter((r) => asNumber(r.consistency_score) !== null);
    const withRangeNullValue = displayedRows
      .filter((r) => asNumber(r.market_range) !== null && asNumber(r.value_vs_market) === null)
      .sort((a, b) => Number(b.market_range) - Number(a.market_range));
    const medianNullRows = displayedRows.filter((r) => asNumber(r.market_median) === null);
    const wideRangeRows = displayedRows
      .filter((r) => asNumber(r.market_range) !== null && Number(r.market_range) >= 80)
      .sort((a, b) => Number(b.market_range) - Number(a.market_range));
    const lowBookRows = displayedRows
      .filter((r) => {
        const n = asNumber(r.book_count_over);
        return n !== null && n < 2;
      })
      .sort((a, b) => Number(a.book_count_over) - Number(b.book_count_over));
    const sparseSnapshotRows = displayedRows
      .filter((r) => {
        const n = asNumber(r.num_snapshots);
        return n !== null && n <= 1;
      })
      .sort((a, b) => Number(a.num_snapshots) - Number(b.num_snapshots));
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
      byCoverage: coverageCounts,
      byPropLabel: propDisplayCounts,
      timingSamples: groupedSamples(displayedRows, "timing_signal", 4),
      streakSamples: groupedSamples(displayedRows, "streak_context_label", 4),
      coverageSamples: groupedSamples(displayedRows, "coverage_quality_label", 4),
      missingCounts: {
        timing_reason: displayedRows.filter((r) => isMissing(r.timing_reason)).length,
        streak_count: displayedRows.filter((r) => isMissing(r.streak_count)).length,
        baseline_delta: displayedRows.filter((r) => isMissing(r.baseline_delta)).length,
        consistency_score: displayedRows.filter((r) => isMissing(r.consistency_score)).length,
        market_median: displayedRows.filter((r) => isMissing(r.market_median)).length,
        value_vs_market: displayedRows.filter((r) => isMissing(r.value_vs_market)).length,
        coverage_quality_label: displayedRows.filter((r) => isMissing(r.coverage_quality_label)).length,
        coverage_quality_reason: displayedRows.filter((r) => isMissing(r.coverage_quality_reason)).length,
      },
      highRangeNullValueSamples: withRangeNullValue.slice(0, 8),
      medianNullSamples: medianNullRows.slice(0, 8),
      wideRangeSamples: wideRangeRows.slice(0, 8),
      lowBookSamples: lowBookRows.slice(0, 8),
      sparseSnapshotSamples: sparseSnapshotRows.slice(0, 8),
      marketRangeBuckets: bucketCounts(
        displayedRows.map((r) => ({ market_range_bucket: marketRangeBucket(r.market_range) })),
        "market_range_bucket"
      ),
      nullFieldSamples: displayedRows
        .filter(
          (r) =>
            isMissing(r.timing_reason) ||
            isMissing(r.streak_count) ||
            isMissing(r.baseline_delta) ||
            isMissing(r.consistency_score) ||
            isMissing(r.market_median) ||
            isMissing(r.value_vs_market) ||
            isMissing(r.coverage_quality_label) ||
            isMissing(r.coverage_quality_reason)
        )
        .slice(0, 8),
      topVisibleRows: displayedRows.slice(0, 10).map((r) => ({
        player_name: r.player_name,
        prop_label: propLabel(r.prop_type),
        line: r.line,
        value_vs_market: r.value_vs_market,
        coverage_quality_label: r.coverage_quality_label,
      })),
    };
  }, [displayedRows, timingCounts, streakCounts, coverageCounts, propDisplayCounts]);

  useEffect(() => {
    let isMounted = true;
    async function loadOptions() {
      try {
        const qs = new URLSearchParams();
        qs.set("limit", "5000");
        qs.set("offset", "0");
        const url = `${getBaseURL()}/api/mlb/today/workspace?${qs.toString()}`;
        const res = await fetch(url, { credentials: "include" });
        const data = await res.json();
        if (!res.ok || !isMounted) return;
        const nextRows = Array.isArray(data?.rows) ? data.rows : [];
        setOptionRows(nextRows);
      } catch (_e) {
        if (!isMounted) return;
        setOptionRows([]);
      }
    }
    loadOptions();
    return () => {
      isMounted = false;
    };
  }, []);

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
          <div className="text-xs text-slate-500 ml-auto">
            Rows: {displayedRows.length} shown / {rows.length} filtered / {total} slate
          </div>
        </div>

        <div className="pp-card p-3 mb-4 flex flex-wrap gap-2 items-center">
          <div className="text-xs text-slate-500 mr-1">View:</div>
          {VIEW_PRESETS.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => setViewPreset(preset.key)}
              className={`text-xs px-2 py-1 rounded border ${
                viewPreset === preset.key
                  ? "bg-slate-800 text-white border-slate-800"
                  : "bg-white text-slate-700 border-slate-300 hover:bg-slate-50"
              }`}
            >
              {preset.label}
            </button>
          ))}
          <div className="text-xs text-slate-500 ml-auto">
            Default sort: non-null Δ vs Median, then strongest Δ
          </div>
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
          <div className="w-full" />
          <div className="text-xs text-slate-500 mr-1">Coverage mix:</div>
          {coverageCounts.map(([label, count]) => (
            <span key={`coverage-${label}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
              {coverageLabel(label)}: {count}
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
                    {r.player_name} | {propLabel(r.prop_type)} {fmtNumber(r.line, 1)} |{" "}
                    {fmtPrice(r.value_vs_market, { forceSign: true })}
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
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Sample rows by timing bucket (up to 4 each)</div>
                {debugSlices.timingSamples.map((bucket) => (
                  <div key={`dbg-timing-${bucket.key}`} className="mb-2">
                    <div className="text-slate-600">{timingLabel(bucket.key)} ({bucket.count})</div>
                    {bucket.rows.map((r, idx) => (
                      <div key={`dbg-timing-row-${bucket.key}-${idx}`}>{rowSnippet(r)}</div>
                    ))}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Sample rows by coverage bucket (up to 4 each)</div>
                {debugSlices.coverageSamples.map((bucket) => (
                  <div key={`dbg-coverage-${bucket.key}`} className="mb-2">
                    <div className="text-slate-600">{coverageLabel(bucket.key)} ({bucket.count})</div>
                    {bucket.rows.map((r, idx) => (
                      <div key={`dbg-coverage-row-${bucket.key}-${idx}`}>
                        {rowSnippet(r)} | reason: {isMissing(r.coverage_quality_reason) ? DASH : String(r.coverage_quality_reason)}
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Sample rows by streak bucket (up to 4 each)</div>
                {debugSlices.streakSamples.map((bucket) => (
                  <div key={`dbg-streak-${bucket.key}`} className="mb-2">
                    <div className="text-slate-600">{streakLabel(bucket.key)} ({bucket.count})</div>
                    {bucket.rows.map((r, idx) => (
                      <div key={`dbg-streak-row-${bucket.key}-${idx}`}>{rowSnippet(r)}</div>
                    ))}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Coverage label counts</div>
                {debugSlices.byCoverage.map(([label, count]) => (
                  <div key={`dbg-coverage-count-${label}`}>{coverageLabel(label)}: {count}</div>
                ))}
                <div className="font-medium mt-2 mb-1">Displayed prop label counts</div>
                {debugSlices.byPropLabel.map(([label, count]) => (
                  <div key={`dbg-prop-count-${label}`}>{label}: {count}</div>
                ))}
              </div>
            </div>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Missing key-field counts</div>
                <div>timing_reason: {debugSlices.missingCounts.timing_reason}</div>
                <div>streak_count: {debugSlices.missingCounts.streak_count}</div>
                <div>baseline_delta: {debugSlices.missingCounts.baseline_delta}</div>
                <div>consistency_score: {debugSlices.missingCounts.consistency_score}</div>
                <div>market_median: {debugSlices.missingCounts.market_median}</div>
                <div>value_vs_market: {debugSlices.missingCounts.value_vs_market}</div>
                <div>coverage_quality_label: {debugSlices.missingCounts.coverage_quality_label}</div>
                <div>coverage_quality_reason: {debugSlices.missingCounts.coverage_quality_reason}</div>
                <div className="mt-1 text-slate-600">Market range buckets:</div>
                {debugSlices.marketRangeBuckets.map(([bucket, count]) => (
                  <div key={`dbg-range-${bucket}`}>{bucket}: {count}</div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Sample rows with key nulls</div>
                {debugSlices.nullFieldSamples.length === 0 ? (
                  <div className="text-slate-500">None</div>
                ) : (
                  debugSlices.nullFieldSamples.map((r, idx) => {
                    const missing = [];
                    if (isMissing(r.timing_reason)) missing.push("timing_reason");
                    if (isMissing(r.streak_count)) missing.push("streak_count");
                    if (isMissing(r.baseline_delta)) missing.push("baseline_delta");
                    if (isMissing(r.consistency_score)) missing.push("consistency_score");
                    if (isMissing(r.market_median)) missing.push("market_median");
                    if (isMissing(r.value_vs_market)) missing.push("value_vs_market");
                    if (isMissing(r.coverage_quality_label)) missing.push("coverage_quality_label");
                    if (isMissing(r.coverage_quality_reason)) missing.push("coverage_quality_reason");
                    return (
                      <div key={`dbg-null-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                        {rowSnippet(r)} | missing: {missing.join(", ")}
                      </div>
                    );
                  })
                )}
              </div>
            </div>
            <div className="mt-3">
              <div className="font-medium mb-1">High market range + null value sample</div>
              {debugSlices.highRangeNullValueSamples.length === 0 ? (
                <div className="text-slate-500">None</div>
              ) : (
                debugSlices.highRangeNullValueSamples.map((r, idx) => (
                  <div key={`dbg-range-null-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                    {rowSnippet(r)} | range: {fmtPrice(r.market_range)} | median: {fmtPrice(r.market_median)} | value: {fmtPrice(r.value_vs_market)}
                  </div>
                ))
              )}
            </div>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Median-null sample</div>
                {debugSlices.medianNullSamples.length === 0 ? (
                  <div className="text-slate-500">None</div>
                ) : (
                  debugSlices.medianNullSamples.map((r, idx) => (
                    <div key={`dbg-median-null-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                      {rowSnippet(r)} | books: {fmtNumber(r.book_count_over, 0)} | snapshots: {fmtNumber(r.num_snapshots, 0)}
                    </div>
                  ))
                )}
              </div>
              <div>
                <div className="font-medium mb-1">Wide market range sample</div>
                {debugSlices.wideRangeSamples.length === 0 ? (
                  <div className="text-slate-500">None</div>
                ) : (
                  debugSlices.wideRangeSamples.map((r, idx) => (
                    <div key={`dbg-wide-range-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                      {rowSnippet(r)} | range: {fmtPrice(r.market_range)} | books: {fmtNumber(r.book_count_over, 0)}
                    </div>
                  ))
                )}
              </div>
            </div>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Low book-count sample</div>
                {debugSlices.lowBookSamples.length === 0 ? (
                  <div className="text-slate-500">None</div>
                ) : (
                  debugSlices.lowBookSamples.map((r, idx) => (
                    <div key={`dbg-low-books-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                      {rowSnippet(r)} | books: {fmtNumber(r.book_count_over, 0)} | reason: {isMissing(r.coverage_quality_reason) ? DASH : String(r.coverage_quality_reason)}
                    </div>
                  ))
                )}
              </div>
              <div>
                <div className="font-medium mb-1">Sparse snapshot sample</div>
                {debugSlices.sparseSnapshotSamples.length === 0 ? (
                  <div className="text-slate-500">None</div>
                ) : (
                  debugSlices.sparseSnapshotSamples.map((r, idx) => (
                    <div key={`dbg-sparse-snap-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                      {rowSnippet(r)} | snapshots: {fmtNumber(r.num_snapshots, 0)} | reason: {isMissing(r.coverage_quality_reason) ? DASH : String(r.coverage_quality_reason)}
                    </div>
                  ))
                )}
              </div>
            </div>
            <div className="mt-3">
              <div className="font-medium mb-1">Top 10 visible rows under current default sort</div>
              {debugSlices.topVisibleRows.length === 0 ? (
                <div className="text-slate-500">None</div>
              ) : (
                debugSlices.topVisibleRows.map((r, idx) => (
                  <div key={`dbg-top-visible-${idx}`}>
                    {idx + 1}. {r.player_name || DASH} | {r.prop_label} {fmtNumber(r.line, 1)} | Δ {fmtPrice(r.value_vs_market, { forceSign: true })} | {coverageLabel(r.coverage_quality_label)}
                  </div>
                ))
              )}
            </div>
            <div className="mt-2 text-slate-500">Use <code>?debug=1</code> in URL to show/hide this panel.</div>
          </div>
        ) : null}

        <div className="text-xs text-slate-500 mb-2 px-1">
          Δ vs Median is the best available over price minus the market median over price. It is descriptive market context, not a recommendation signal.
        </div>
        <div className="pp-card p-0 overflow-x-auto">
          {loading ? (
            <div className="p-4 text-slate-600 text-sm">Loading workspace…</div>
          ) : error ? (
            <div className="p-4 text-rose-700 text-sm">{error}</div>
          ) : displayedRows.length === 0 ? (
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
                  <th className="py-2.5 px-3 text-right">Δ vs Median</th>
                  <th className="py-2.5 px-3">Timing</th>
                  <th className="py-2.5 px-3">Streak</th>
                  <th className="py-2.5 px-3 text-right">Consistency</th>
                  <th className="py-2.5 px-3"></th>
                </tr>
              </thead>
              <tbody>
                {displayedRows.map((r) => {
                  const key = `${r.player_id}:${r.game_id}:${r.prop_type}:${r.line}`;
                  const isOpen = Boolean(expanded[key]);
                  return (
                    <React.Fragment key={key}>
                      <tr className="border-b border-slate-100 align-top">
                        <td className="py-2.5 px-3">
                          <div className="font-semibold text-slate-900">{r.player_name || DASH}</div>
                          <div className="text-xs text-slate-500">
                            {(r.team || DASH)} vs {(r.opponent || DASH)}
                          </div>
                        </td>
                        <td className="py-2.5 px-3 font-medium">{propLabel(r.prop_type)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums text-slate-700">{fmtNumber(r.line, 1)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums font-medium">{fmtPrice(r.best_price)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums font-medium">{fmtPrice(r.market_median)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums">
                          {asNumber(r.value_vs_market) === null ? (
                            <>
                              <div>{DASH}</div>
                              <div className="text-[11px] text-slate-400">
                                {isMissing(r.coverage_quality_reason)
                                  ? (asNumber(r.market_range) !== null && asNumber(r.market_range) >= 80
                                      ? "Wide market spread"
                                      : "No reliable median")
                                  : String(r.coverage_quality_reason)}
                              </div>
                            </>
                          ) : (
                            <div
                              className={`font-semibold ${
                                Math.abs(asNumber(r.value_vs_market) || 0) < 5 ? "text-slate-500" : "text-slate-800"
                              }`}
                            >
                              {fmtPrice(r.value_vs_market, { forceSign: true })}
                            </div>
                          )}
                        </td>
                        <td className="py-2.5 px-3">
                          <div className="text-slate-700">{timingLabel(r.timing_signal)}</div>
                          <div className="text-xs text-slate-500">{isMissing(r.timing_reason) ? DASH : String(r.timing_reason)}</div>
                        </td>
                        <td className="py-2.5 px-3">
                          <div className="text-slate-700">{streakLabel(r.streak_context_label)}</div>
                          {!isMissing(r.streak_count) ? (
                            <div className="text-xs text-slate-500">count: {fmtNumber(r.streak_count, 0)}</div>
                          ) : null}
                        </td>
                        <td className="py-2.5 px-3 text-right tabular-nums">
                          <div className="text-slate-700">{fmtNumber(r.consistency_score, 1)}</div>
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
                              <div><span className="text-slate-500">Open over median:</span> <strong>{fmtPrice(r.open_over_price)}</strong></div>
                              <div><span className="text-slate-500">Latest over median:</span> <strong>{fmtPrice(r.latest_over_price)}</strong></div>
                              <div><span className="text-slate-500">Best over now:</span> <strong>{fmtPrice(r.best_price)}</strong></div>
                              <div>
                                <span className="text-slate-500">Over market range:</span>{" "}
                                <strong>{fmtPrice(r.market_range)}</strong>
                                <span className="text-slate-500"> ({marketRangeLabel(r.market_range)})</span>
                              </div>
                              <div><span className="text-slate-500">Market coverage:</span> <strong>{coverageLabel(r.coverage_quality_label)}</strong></div>
                              <div>
                                <span className="text-slate-500">Coverage detail:</span>{" "}
                                <strong>{isMissing(r.coverage_quality_reason) ? DASH : String(r.coverage_quality_reason)}</strong>
                              </div>
                              <div><span className="text-slate-500">Snapshots:</span> <strong>{fmtNumber(r.num_snapshots, 0)}</strong></div>
                              <div><span className="text-slate-500">Books (over/under):</span> <strong>{fmtNumber(r.book_count_over, 0)} / {fmtNumber(r.book_count_under, 0)}</strong></div>
                              <div><span className="text-slate-500">Over move from open:</span> <strong>{fmtPrice(r.over_price_change_from_open, { forceSign: true })}</strong></div>
                              <div><span className="text-slate-500">Hit rate last 5:</span> <strong>{fmtPct(r.hit_rate_last_5)}</strong></div>
                              <div><span className="text-slate-500">Hit rate last 10:</span> <strong>{fmtPct(r.hit_rate_last_10)}</strong></div>
                              <div><span className="text-slate-500">Hit rate season:</span> <strong>{fmtPct(r.hit_rate_season)}</strong></div>
                              <div><span className="text-slate-500">Last 5 avg:</span> <strong>{fmtNumber(r.last_5_avg)}</strong></div>
                              <div><span className="text-slate-500">Last 10 avg:</span> <strong>{fmtNumber(r.last_10_avg)}</strong></div>
                              <div><span className="text-slate-500">Season avg:</span> <strong>{fmtNumber(r.season_avg)}</strong></div>
                              <div><span className="text-slate-500">Baseline delta:</span> <strong>{fmtPctSigned(r.baseline_delta)}</strong></div>
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
