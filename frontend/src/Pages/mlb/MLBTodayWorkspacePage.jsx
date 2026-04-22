import React, { useEffect, useMemo, useRef, useState } from "react";
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

function fmtLastUpdatedET(v) {
  if (isMissing(v)) return DASH;
  const raw = String(v).trim();
  const normalized = raw
    // Postgres often returns `YYYY-MM-DD HH:MM:SS+00`; normalize for browser-safe parsing.
    .replace(/^(\d{4}-\d{2}-\d{2})\s+/, "$1T")
    .replace(/([+-]\d{2})$/, "$1:00");
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) return DASH;
  const time = d.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: "America/New_York",
  });
  return `${time} ET`;
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

function sideLabel(side) {
  const s = String(side || "").trim().toUpperCase();
  if (s === "OVER") return "Over";
  if (s === "UNDER") return "Under";
  return DASH;
}

function sideUpper(side) {
  const s = String(side || "").trim().toUpperCase();
  if (s === "OVER" || s === "UNDER") return s;
  return "";
}

function timingLabelForRow(signal, side) {
  const s = sideUpper(side);
  if (!s) return timingLabel(signal);
  const sig = String(signal || "").trim().toUpperCase();
  const map = {
    EARLY: `Earlier ${s} price was better`,
    WAIT: `${s} price has improved`,
    VOLATILE: `${s} market is volatile`,
    STABLE: `${s} market is stable`,
  };
  return map[sig] || DASH;
}

function timingReasonForRow(reason, side) {
  if (isMissing(reason)) return DASH;
  const s = sideUpper(side);
  if (!s) return String(reason);
  return `${s}: ${String(reason)}`;
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

function coverageReasonForRow(reason, side) {
  if (isMissing(reason)) return DASH;
  const s = sideUpper(side);
  if (!s) return String(reason);
  return `${s}: ${String(reason)}`;
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
  return `${r.player_name || DASH} | ${propLabel(r.prop_type)} ${fmtNumber(r.line, 1)} ${String(r.side || "").toUpperCase() || ""}`.trim();
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

// Frontend-only translation layer for novice readability.
const MARKET_POSITION_THRESHOLDS = {
  alignedAbsDeltaMax: 5,
  alignedRangeMax: 40,
  slightlyOffAbsDeltaMin: 15,
  outlierAbsDeltaMin: 40,
  wideRangeMin: 120,
};

function marketPositionInfo(row) {
  const median = asNumber(row?.market_median);
  const delta = asNumber(row?.value_vs_market);
  const range = asNumber(row?.market_range);
  const coverage = String(row?.coverage_quality_label || "").trim().toUpperCase();
  const absDelta = delta === null ? null : Math.abs(delta);

  if (median === null || delta === null || coverage === "UNRELIABLE") {
    return {
      key: "UNCLEAR_MARKET",
      label: "Unclear market",
      detail: "No reliable market center",
    };
  }

  if (range !== null && range >= MARKET_POSITION_THRESHOLDS.wideRangeMin) {
    return {
      key: "WIDE_MARKET",
      label: "Wide market",
      detail: "Books are spread out right now",
    };
  }

  if (
    absDelta !== null &&
    absDelta <= MARKET_POSITION_THRESHOLDS.alignedAbsDeltaMax &&
    (range === null || range <= MARKET_POSITION_THRESHOLDS.alignedRangeMax)
  ) {
    return {
      key: "ALIGNED_MARKET",
      label: "Aligned market",
      detail: "Best price is near market center",
    };
  }

  if (absDelta !== null && absDelta >= MARKET_POSITION_THRESHOLDS.outlierAbsDeltaMin) {
    return {
      key: "OUTLIER_PRICE",
      label: "Outlier price",
      detail: "Best price is far from market center",
    };
  }

  if (absDelta !== null && absDelta >= MARKET_POSITION_THRESHOLDS.slightlyOffAbsDeltaMin) {
    return {
      key: "SLIGHTLY_OFF_MARKET",
      label: "Slightly off market",
      detail: "Best price is somewhat away from center",
    };
  }

  return {
    key: "ALIGNED_MARKET",
    label: "Aligned market",
    detail: "Best price is close to market center",
  };
}

function marketPositionLabelForRow(info, side) {
  const s = sideUpper(side);
  if (!s || !info?.key) return info?.label || DASH;
  const map = {
    UNCLEAR_MARKET: `Unclear ${s} market`,
    ALIGNED_MARKET: `Aligned ${s} market`,
    OUTLIER_PRICE: `Outlier ${s} price`,
    SLIGHTLY_OFF_MARKET: `Slightly off ${s} market`,
    WIDE_MARKET: `Wide ${s} market`,
  };
  return map[info.key] || info.label || DASH;
}

function marketPositionDetailForRow(info, side) {
  const s = sideUpper(side);
  if (!s || !info?.key) return info?.detail || DASH;
  const map = {
    UNCLEAR_MARKET: `No reliable ${s} market center`,
    ALIGNED_MARKET: `${s} best price is near market center`,
    OUTLIER_PRICE: `${s} best price sits far from market center`,
    SLIGHTLY_OFF_MARKET: `${s} best price is somewhat away from center`,
    WIDE_MARKET: `${s} books are spread out right now`,
  };
  return map[info.key] || info.detail || DASH;
}

function fallbackMedianTextForRow(row) {
  const s = sideUpper(row?.side);
  if (asNumber(row?.market_range) !== null && asNumber(row?.market_range) >= 80) {
    return s ? `Wide ${s} market spread` : "Wide market spread";
  }
  return s ? `No reliable ${s} median` : "No reliable median";
}

function deltaExplanationForRow(row) {
  const s = sideUpper(row?.side) || "SIDE";
  if (asNumber(row?.value_vs_market) === null || asNumber(row?.market_median) === null) {
    return `No reliable ${s} median available for comparison.`;
  }
  return `Δ compares best available ${s} price to the current ${s} market median.`;
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
  if (al !== bl) return al - bl;

  const as = String(a?.side || "");
  const bs = String(b?.side || "");
  return as.localeCompare(bs);
}

function rowKeyForRow(row) {
  return `${row?.player_id}:${row?.game_id}:${row?.prop_type}:${row?.line}:${row?.side}`;
}

const VIEW_PRESETS = [
  { key: "all", label: "All rows", tooltip: "Show the full workspace without preset filtering." },
  {
    key: "best_covered",
    label: "Best covered",
    tooltip: "Strong market coverage across multiple books with tight pricing.",
  },
  {
    key: "wide_markets",
    label: "Wide markets",
    tooltip: "Books disagree significantly. Prices are spread out.",
  },
  {
    key: "sparse_markets",
    label: "Sparse markets",
    tooltip: "Limited or incomplete market data (few books or weak coverage).",
  },
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
  const [lastUpdated, setLastUpdated] = useState(null);
  const [requestedSlateDate, setRequestedSlateDate] = useState(null);
  const [activeSlateDate, setActiveSlateDate] = useState(null);
  const [isReady, setIsReady] = useState(false);
  const [workspaceReady, setWorkspaceReady] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expandedRowKeys, setExpandedRowKeys] = useState(() => new Set());
  const [focusedRowIndex, setFocusedRowIndex] = useState(-1);
  const [lastInteraction, setLastInteraction] = useState("none");
  const [viewPreset, setViewPreset] = useState("all");
  const [filters, setFilters] = useState({
    prop_type: "",
    team: "",
    side: "",
    timing_signal: "",
    player_query: "",
  });
  const rowRefs = useRef([]);
  const topScrollRef = useRef(null);
  const headerScrollRef = useRef(null);
  const tableScrollRef = useRef(null);
  const [tableScrollWidth, setTableScrollWidth] = useState(1320);

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
  const sideOptions = useMemo(() => {
    const base = optionRows.length ? optionRows : rows;
    const options = Array.from(
      new Set(
        base
          .map((r) => String(r.side || "").trim().toUpperCase())
          .filter((s) => s === "OVER" || s === "UNDER")
      )
    ).sort();
    return withSelected(options, filters.side);
  }, [optionRows, rows, filters.side]);
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

  const compareMode = useMemo(() => {
    const hasPlayerQuery = Boolean(filters.player_query && filters.player_query.trim());
    return hasPlayerQuery || displayedRows.length <= 12;
  }, [filters.player_query, displayedRows.length]);

  const expandedRowCount = expandedRowKeys.size;
  const expandedRowKeyList = useMemo(() => Array.from(expandedRowKeys), [expandedRowKeys]);
  const focusedRowKey =
    focusedRowIndex >= 0 && focusedRowIndex < displayedRows.length ? rowKeyForRow(displayedRows[focusedRowIndex]) : null;

  const hasActiveFilters = useMemo(() => {
    return Boolean(
      filters.prop_type ||
      filters.team ||
      filters.side ||
      filters.timing_signal ||
      (filters.player_query && filters.player_query.trim())
    );
  }, [filters]);

  const activeStateTokens = useMemo(() => {
    const out = [];
    const selectedPreset = VIEW_PRESETS.find((p) => p.key === viewPreset);
    if (selectedPreset && selectedPreset.key !== "all") out.push(selectedPreset.label);
    if (filters.prop_type) out.push(propLabel(filters.prop_type));
    if (filters.team) out.push(filters.team);
    if (filters.side) out.push(sideLabel(filters.side));
    if (filters.timing_signal) out.push(timingLabel(filters.timing_signal));
    if (filters.player_query && filters.player_query.trim()) out.push(`Player: ${filters.player_query.trim()}`);
    return out;
  }, [viewPreset, filters]);

  const activeFilterTokens = useMemo(() => {
    const out = [];
    if (filters.prop_type) out.push(propLabel(filters.prop_type));
    if (filters.team) out.push(filters.team);
    if (filters.side) out.push(sideLabel(filters.side));
    if (filters.timing_signal) out.push(timingLabel(filters.timing_signal));
    if (filters.player_query && filters.player_query.trim()) out.push(`Player: ${filters.player_query.trim()}`);
    return out;
  }, [filters]);

  const timingCounts = useMemo(() => bucketCounts(displayedRows, "timing_signal"), [displayedRows]);
  const streakCounts = useMemo(() => bucketCounts(displayedRows, "streak_context_label"), [displayedRows]);
  const coverageCounts = useMemo(() => bucketCounts(displayedRows, "coverage_quality_label"), [displayedRows]);
  const sideCounts = useMemo(() => bucketCounts(displayedRows, "side"), [displayedRows]);
  const propSideCounts = useMemo(() => {
    const counts = {};
    for (const r of displayedRows) {
      const key = `${propLabel(r.prop_type)} | ${sideLabel(r.side)}`;
      counts[key] = (counts[key] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => b[1] - a[1]);
  }, [displayedRows]);
  const marketPositionCounts = useMemo(() => {
    return bucketCounts(
      displayedRows.map((r) => ({ market_position_label: marketPositionInfo(r).label })),
      "market_position_label"
    );
  }, [displayedRows]);
  const isWorkspaceReady = workspaceReady === null ? isReady : workspaceReady;
  const propDisplayCounts = useMemo(() => {
    return bucketCounts(
      displayedRows.map((r) => ({ prop_label: propLabel(r.prop_type) })),
      "prop_label"
    );
  }, [displayedRows]);

  const debugSlices = useMemo(() => {
    const rowsWithMarketPosition = displayedRows.map((r) => {
      const info = marketPositionInfo(r);
      return {
        ...r,
        market_position_key: info.key,
        market_position_label: info.label,
        market_position_detail: info.detail,
      };
    });

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
        const n = asNumber(r.book_count);
        return n !== null && n < 2;
      })
      .sort((a, b) => Number(a.book_count) - Number(b.book_count));
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
    const marketCoverageMismatchRows = rowsWithMarketPosition
      .filter((r) => {
        const cov = String(r.coverage_quality_label || "").trim().toUpperCase();
        const pos = String(r.market_position_key || "").trim().toUpperCase();
        return (
          (["STRONG", "GOOD"].includes(cov) && ["WIDE_MARKET", "OUTLIER_PRICE"].includes(pos)) ||
          (["LIMITED", "THIN"].includes(cov) && pos === "ALIGNED_MARKET")
        );
      })
      .slice(0, 8);

    return {
      topPositive: byDeltaDesc.slice(0, 5),
      mostNegative: byDeltaAsc.slice(0, 5),
      highestConsistency: byConsistencyDesc.slice(0, 5),
      lowestConsistency: byConsistencyAsc.slice(0, 5),
      byTiming: timingCounts,
      byStreak: streakCounts,
      byCoverage: coverageCounts,
      bySide: sideCounts,
      byPropSide: propSideCounts,
      byMarketPosition: marketPositionCounts,
      byPropLabel: propDisplayCounts,
      timingSamples: groupedSamples(displayedRows, "timing_signal", 4),
      streakSamples: groupedSamples(displayedRows, "streak_context_label", 4),
      coverageSamples: groupedSamples(displayedRows, "coverage_quality_label", 4),
      marketPositionSamples: groupedSamples(rowsWithMarketPosition, "market_position_label", 4),
      marketCoverageMismatchRows,
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
        side: r.side,
        line: r.line,
        value_vs_market: r.value_vs_market,
        market_range: r.market_range,
        coverage_quality_label: r.coverage_quality_label,
        market_position_label: marketPositionLabelForRow(marketPositionInfo(r), r.side),
        timing_signal: r.timing_signal,
      })),
    };
  }, [displayedRows, timingCounts, streakCounts, coverageCounts, sideCounts, propSideCounts, marketPositionCounts, propDisplayCounts]);

  useEffect(() => {
    let isMounted = true;
    async function loadOptions() {
      try {
        const qs = new URLSearchParams();
        qs.set("slate_date", slateDate);
        qs.set("limit", "5000");
        qs.set("offset", "0");
        const url = `${getBaseURL()}/api/mlb/today/workspace?${qs.toString()}`;
        const res = await fetch(url, { credentials: "include" });
        const data = await res.json();
        if (!res.ok || !isMounted) return;
        const nextRows = Array.isArray(data?.rows) ? data.rows : [];
        setOptionRows(nextRows);
        setWorkspaceReady(Boolean(data?.is_ready));
        setRequestedSlateDate(data?.requested_slate_date ?? slateDate);
        setActiveSlateDate(data?.active_slate_date ?? null);
        setLastUpdated(data?.last_updated ?? null);
        setTotal(Number(data?.total) || nextRows.length);
      } catch (_e) {
        if (!isMounted) return;
        setOptionRows([]);
        setWorkspaceReady(null);
      }
    }
    loadOptions();
    return () => {
      isMounted = false;
    };
  }, [slateDate]);

  useEffect(() => {
    let isMounted = true;
    async function run() {
      setLoading(true);
      setError("");
      try {
        const qs = new URLSearchParams();
        qs.set("slate_date", slateDate);
        qs.set("limit", "1000");
        qs.set("offset", "0");
        if (filters.prop_type) qs.set("prop_type", filters.prop_type);
        if (filters.team) qs.set("team", filters.team);
        if (filters.side) qs.set("side", filters.side);
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
        setLastUpdated(data?.last_updated ?? null);
        setRequestedSlateDate(data?.requested_slate_date ?? slateDate);
        setActiveSlateDate(data?.active_slate_date ?? null);
        setIsReady(Boolean(data?.is_ready));
      } catch (e) {
        if (!isMounted) return;
        setRows([]);
        setTotal(0);
        setLastUpdated(null);
        setRequestedSlateDate(slateDate);
        setActiveSlateDate(null);
        setIsReady(false);
        setError(e?.message || "Failed to load MLB today workspace.");
      } finally {
        if (isMounted) setLoading(false);
      }
    }
    run();
    return () => {
      isMounted = false;
    };
  }, [filters.prop_type, filters.team, filters.side, filters.timing_signal, filters.player_query]);

  useEffect(() => {
    rowRefs.current = rowRefs.current.slice(0, displayedRows.length);
    if (focusedRowIndex >= displayedRows.length) {
      setFocusedRowIndex(-1);
    }
  }, [displayedRows.length, focusedRowIndex]);

  useEffect(() => {
    const tableWrap = tableScrollRef.current;
    if (!tableWrap) return;

    const syncWidth = () => {
      const measured = Number(tableWrap.scrollWidth) || 1320;
      setTableScrollWidth(Math.max(1, measured));
    };

    syncWidth();
    window.addEventListener("resize", syncWidth);

    let observer = null;
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(syncWidth);
      observer.observe(tableWrap);
      const tableEl = tableWrap.querySelector("table");
      if (tableEl) observer.observe(tableEl);
    }

    return () => {
      window.removeEventListener("resize", syncWidth);
      if (observer) observer.disconnect();
    };
  }, [displayedRows.length, loading, error]);

  useEffect(() => {
    const topEl = topScrollRef.current;
    const headerEl = headerScrollRef.current;
    const tableEl = tableScrollRef.current;
    if (!topEl || !headerEl || !tableEl) return;

    let syncing = false;
    const syncAll = (source, left) => {
      if (syncing) return;
      syncing = true;
      if (source !== "top") topEl.scrollLeft = left;
      if (source !== "header") headerEl.scrollLeft = left;
      if (source !== "table") tableEl.scrollLeft = left;
      requestAnimationFrame(() => {
        syncing = false;
      });
    };
    const onTopScroll = () => syncAll("top", topEl.scrollLeft);
    const onHeaderScroll = () => syncAll("header", headerEl.scrollLeft);
    const onTableScroll = () => syncAll("table", tableEl.scrollLeft);

    topEl.addEventListener("scroll", onTopScroll, { passive: true });
    headerEl.addEventListener("scroll", onHeaderScroll, { passive: true });
    tableEl.addEventListener("scroll", onTableScroll, { passive: true });
    syncAll("table", tableEl.scrollLeft);

    return () => {
      topEl.removeEventListener("scroll", onTopScroll);
      headerEl.removeEventListener("scroll", onHeaderScroll);
      tableEl.removeEventListener("scroll", onTableScroll);
    };
  }, [displayedRows.length, loading, error]);

  useEffect(() => {
    if (!expandedRowKeys.size) return;
    const visibleKeys = new Set(displayedRows.map((r) => rowKeyForRow(r)));
    let changed = false;
    const next = new Set();
    expandedRowKeys.forEach((key) => {
      if (visibleKeys.has(key)) {
        next.add(key);
      } else {
        changed = true;
      }
    });
    if (changed) {
      setExpandedRowKeys(next);
      setLastInteraction("auto-collapse:view-change");
    }
  }, [displayedRows, expandedRowKeys]);

  useEffect(() => {
    setExpandedRowKeys(new Set());
    setFocusedRowIndex(-1);
    setLastInteraction("collapse:controls-change");
  }, [viewPreset, filters.prop_type, filters.team, filters.side, filters.timing_signal, filters.player_query]);

  function toggleRow(key, source = "row") {
    setExpandedRowKeys((prev) => {
      const next = new Set(prev);
      let action = "collapse";
      if (compareMode) {
        if (next.has(key)) {
          next.delete(key);
        } else {
          next.add(key);
          action = "expand";
        }
      } else {
        if (next.has(key)) {
          next.clear();
        } else {
          next.clear();
          next.add(key);
          action = "expand";
        }
      }
      setLastInteraction(`${action}:${source}:${key}`);
      return next;
    });
  }

  function focusRowAtIndex(nextIndex) {
    if (nextIndex < 0 || nextIndex >= rowRefs.current.length) return;
    const target = rowRefs.current[nextIndex];
    if (target && typeof target.focus === "function") {
      target.focus();
      setFocusedRowIndex(nextIndex);
    }
  }

  function handleRowClick(event, key) {
    const interactive = event.target.closest("button, a, input, select, textarea, label");
    if (interactive) return;
    toggleRow(key, "row-click");
  }

  function handleRowKeyDown(event, rowIndex, key) {
    const interactive = event.target.closest("button, a, input, select, textarea, label");
    if (interactive && interactive !== event.currentTarget) {
      if (event.key === "Escape" && expandedRowCount > 0) {
        event.preventDefault();
        setExpandedRowKeys(new Set());
        setLastInteraction("collapse:escape:all");
      }
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      focusRowAtIndex(rowIndex + 1);
      setLastInteraction(`focus:arrow-down:${rowIndex + 1}`);
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      focusRowAtIndex(rowIndex - 1);
      setLastInteraction(`focus:arrow-up:${rowIndex - 1}`);
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      toggleRow(key, "keyboard-toggle");
      return;
    }
    if (event.key === "Escape" && expandedRowCount > 0) {
      event.preventDefault();
      setExpandedRowKeys(new Set());
      setLastInteraction("collapse:escape:all");
    }
  }

  function clearAllReviewControls() {
    setViewPreset("all");
    setExpandedRowKeys(new Set());
    setFocusedRowIndex(-1);
    setLastInteraction("reset-view");
    setFilters({
      prop_type: "",
      team: "",
      side: "",
      timing_signal: "",
      player_query: "",
    });
  }

  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-7xl mx-auto px-4 pb-10">
        <div className="flex items-baseline justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-slate-900">MLB Today Workspace</h2>
            <p className="mt-1 text-sm text-slate-600">
              Your decision workspace for today&apos;s slate. See where prices sit, how they move, and how stable each player profile looks right now.
            </p>
          </div>
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
            <div className="text-xs text-slate-500 mb-1">Side</div>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={filters.side}
              onChange={(e) => setFilters((f) => ({ ...f, side: e.target.value }))}
            >
              <option value="">All</option>
              {sideOptions.map((s) => (
                <option key={s} value={s}>
                  {sideLabel(s)}
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
          <div>
            <button
              type="button"
              className="text-xs border rounded px-2 py-1 text-slate-700 hover:bg-slate-50"
              onClick={clearAllReviewControls}
              disabled={!hasActiveFilters && viewPreset === "all"}
              title="Reset preset and all filters"
            >
              Reset view
            </button>
          </div>
          <div className="text-xs text-slate-500 ml-auto text-right">
            <div>Rows: {displayedRows.length} shown / {rows.length} filtered / {total} slate</div>
            <div>Slate (ET): {requestedSlateDate || slateDate}</div>
            {activeSlateDate ? <div className="text-slate-400">Active staged slate: {activeSlateDate}</div> : null}
            <div className="text-slate-400">Last updated: {fmtLastUpdatedET(lastUpdated)}</div>
          </div>
        </div>

        <div className="pp-card p-3 mb-4 flex flex-wrap gap-2 items-center">
          <div className="text-xs text-slate-500 mr-1">View:</div>
          {VIEW_PRESETS.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => setViewPreset(preset.key)}
              title={preset.tooltip}
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
            Default sort: non-null Δ vs Side Median, then strongest Δ
          </div>
        </div>

        <div className="pp-card p-3 mb-4 flex flex-wrap gap-2 items-center">
          <div className="text-xs text-slate-500 mr-1">Active view:</div>
          {activeStateTokens.length === 0 ? (
            <span className="text-xs text-slate-600">All rows</span>
          ) : (
            activeStateTokens.map((t) => (
              <span key={`active-token-${t}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
                {t}
              </span>
            ))
          )}
        </div>

        {compareMode ? (
          <div className="text-xs text-slate-500 mb-2 px-1">Multiple rows can be opened for comparison.</div>
        ) : null}

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
          <div className="w-full" />
          <div className="text-xs text-slate-500 mr-1">Side mix:</div>
          {sideCounts.map(([label, count]) => (
            <span key={`side-${label}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
              {sideLabel(label)}: {count}
            </span>
          ))}
          <div className="w-full" />
          <div className="text-xs text-slate-500 mr-1">Market Position mix:</div>
          {marketPositionCounts.map(([label, count]) => (
            <span key={`market-position-${label}`} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-700">
              {label}: {count}
            </span>
          ))}
        </div>

        {debugMode ? (
          <div className="pp-card p-3 mb-4 text-xs text-slate-700">
            <div className="font-semibold text-slate-900 mb-2">Debug QA Slices</div>
            <div className="mb-3 border border-slate-200 rounded p-2 bg-slate-50">
              <div className="font-medium mb-1">Current view summary</div>
              <div>Preset: {VIEW_PRESETS.find((p) => p.key === viewPreset)?.label || "All rows"}</div>
              <div>Filters: {activeFilterTokens.length ? activeFilterTokens.join(" · ") : "None"}</div>
              <div>Visible rows: {displayedRows.length}</div>
              <div>Compare mode: {compareMode ? "on" : "off"}</div>
              <div>Expanded rows: {expandedRowCount}</div>
              <div>Focused row key: {focusedRowKey || DASH}</div>
              <div>
                Expanded row keys:{" "}
                {expandedRowKeyList.length
                  ? expandedRowKeyList.slice(0, 4).join(", ") + (expandedRowKeyList.length > 4 ? " …" : "")
                  : DASH}
              </div>
              <div>Last interaction: {lastInteraction}</div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <div className="font-medium mb-1">Highest +Δ vs market</div>
                {debugSlices.topPositive.map((r) => (
                  <div key={`pos-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}-${r.side}`}>
                    {r.player_name} | {propLabel(r.prop_type)} {fmtNumber(r.line, 1)} |{" "}
                    {fmtPrice(r.value_vs_market, { forceSign: true })}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Most negative Δ vs market</div>
                {debugSlices.mostNegative.map((r) => (
                  <div key={`neg-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}-${r.side}`}>
                    {r.player_name} | {propLabel(r.prop_type)} {fmtNumber(r.line, 1)} | {fmtPrice(r.value_vs_market)}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Highest consistency</div>
                {debugSlices.highestConsistency.map((r) => (
                  <div key={`hi-c-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}-${r.side}`}>
                    {r.player_name} | {propLabel(r.prop_type)} | {fmtNumber(r.consistency_score, 1)}
                  </div>
                ))}
              </div>
              <div>
                <div className="font-medium mb-1">Lowest consistency</div>
                {debugSlices.lowestConsistency.map((r) => (
                  <div key={`lo-c-${r.player_id}-${r.game_id}-${r.prop_type}-${r.line}-${r.side}`}>
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
                <div className="font-medium mt-2 mb-1">Side counts</div>
                {debugSlices.bySide.map(([label, count]) => (
                  <div key={`dbg-side-count-${label}`}>{sideLabel(label)}: {count}</div>
                ))}
                <div className="font-medium mt-2 mb-1">Prop + side counts</div>
                {debugSlices.byPropSide.slice(0, 12).map(([label, count]) => (
                  <div key={`dbg-prop-side-count-${label}`}>{label}: {count}</div>
                ))}
                <div className="font-medium mt-2 mb-1">Market Position counts</div>
                {debugSlices.byMarketPosition.map(([label, count]) => (
                  <div key={`dbg-market-position-count-${label}`}>{label}: {count}</div>
                ))}
                <div className="font-medium mt-2 mb-1">Displayed prop label counts</div>
                {debugSlices.byPropLabel.map(([label, count]) => (
                  <div key={`dbg-prop-count-${label}`}>{label}: {count}</div>
                ))}
              </div>
            </div>
            <div className="mt-3">
              <div className="font-medium mb-1">Sample rows by Market Position (up to 4 each)</div>
              {debugSlices.marketPositionSamples.map((bucket) => (
                <div key={`dbg-market-position-${bucket.key}`} className="mb-2">
                  <div className="text-slate-600">{bucket.key} ({bucket.count})</div>
                  {bucket.rows.map((r, idx) => (
                    <div key={`dbg-market-position-row-${bucket.key}-${idx}`}>
                      {rowSnippet(r)} | Δ: {fmtPrice(r.value_vs_market, { forceSign: true })} | range: {fmtPrice(r.market_range)} | coverage: {coverageLabel(r.coverage_quality_label)}
                    </div>
                  ))}
                </div>
              ))}
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
                      {rowSnippet(r)} | books: {fmtNumber(r.book_count, 0)} | snapshots: {fmtNumber(r.num_snapshots, 0)}
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
                      {rowSnippet(r)} | range: {fmtPrice(r.market_range)} | books: {fmtNumber(r.book_count, 0)}
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
                      {rowSnippet(r)} | books: {fmtNumber(r.book_count, 0)} | reason: {isMissing(r.coverage_quality_reason) ? DASH : String(r.coverage_quality_reason)}
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
              <div className="font-medium mb-1">Market Position vs coverage (interesting differences)</div>
              {debugSlices.marketCoverageMismatchRows.length === 0 ? (
                <div className="text-slate-500">None</div>
              ) : (
                debugSlices.marketCoverageMismatchRows.map((r, idx) => (
                  <div key={`dbg-market-cov-mismatch-${r.player_id}-${r.game_id}-${r.prop_type}-${idx}`}>
                    {rowSnippet(r)} | position: {r.market_position_label} | coverage: {coverageLabel(r.coverage_quality_label)} | Δ: {fmtPrice(r.value_vs_market, { forceSign: true })} | range: {fmtPrice(r.market_range)}
                  </div>
                ))
              )}
            </div>
            <div className="mt-3">
              <div className="font-medium mb-1">Top 10 visible rows under current default sort</div>
              {debugSlices.topVisibleRows.length === 0 ? (
                <div className="text-slate-500">None</div>
              ) : (
                debugSlices.topVisibleRows.map((r, idx) => (
                  <div key={`dbg-top-visible-${idx}`}>
                    {idx + 1}. {r.player_name || DASH} | {r.prop_label} {fmtNumber(r.line, 1)} {sideLabel(r.side)} | Δ {fmtPrice(r.value_vs_market, { forceSign: true })} | range {fmtPrice(r.market_range)} | {timingLabelForRow(r.timing_signal, r.side)} | {coverageLabel(r.coverage_quality_label)} | {r.market_position_label}
                  </div>
                ))
              )}
            </div>
            <div className="mt-2 text-slate-500">Use <code>?debug=1</code> in URL to show/hide this panel.</div>
          </div>
        ) : null}

        <div className="text-xs text-slate-500 mb-2 px-1">
          <div>Each row is side-specific (Over or Under). Δ vs Side Median compares that row’s best side price to that same side’s current market median.</div>
          <div>Larger Δ values indicate a bigger gap between the best available price and the market median for that side. This reflects pricing differences, not outcomes.</div>
        </div>
        <div className="pp-card p-0">
          <div
            ref={topScrollRef}
            className="sticky top-0 z-[55] overflow-x-auto overflow-y-hidden h-4 border-b border-slate-200 bg-slate-50/95"
            aria-label="Horizontal table scroll"
          >
            <div style={{ width: `${tableScrollWidth}px`, height: "1px" }} />
          </div>
          <div
            ref={headerScrollRef}
            className="sticky top-4 z-[54] overflow-x-auto overflow-y-hidden border-b border-slate-200 bg-slate-50/95"
            aria-label="Sticky table header scroll"
          >
            <table className="min-w-[1320px] w-full text-sm text-slate-800 table-fixed">
              <colgroup>
                <col style={{ width: "220px" }} />
                <col style={{ width: "170px" }} />
                <col style={{ width: "82px" }} />
                <col style={{ width: "76px" }} />
                <col style={{ width: "118px" }} />
                <col style={{ width: "134px" }} />
                <col style={{ width: "132px" }} />
                <col style={{ width: "190px" }} />
                <col style={{ width: "130px" }} />
                <col style={{ width: "110px" }} />
              </colgroup>
              <thead>
                <tr className="text-left border-b border-slate-200">
                  <th className="sticky left-0 z-[56] bg-slate-50 py-2.5 px-3 border-r border-slate-200 shadow-[inset_-1px_0_0_0_rgba(148,163,184,0.45)]">Player</th>
                  <th className="bg-slate-50 py-2.5 px-3">Prop</th>
                  <th className="bg-slate-50 py-2.5 px-3">Side</th>
                  <th className="bg-slate-50 py-2.5 px-3 text-right whitespace-nowrap">Line</th>
                  <th className="bg-slate-50 py-2.5 px-3 text-right whitespace-nowrap">Best Side Price</th>
                  <th className="bg-slate-50 py-2.5 px-3 text-right whitespace-nowrap">Side Median</th>
                  <th className="bg-slate-50 py-2.5 px-3 text-right whitespace-nowrap">Δ vs Side Median</th>
                  <th className="bg-slate-50 py-2.5 px-3">Timing</th>
                  <th className="bg-slate-50 py-2.5 px-3">Streak</th>
                  <th className="bg-slate-50 py-2.5 px-3 text-right whitespace-nowrap">Consistency</th>
                </tr>
              </thead>
            </table>
          </div>
          <div ref={tableScrollRef} className="overflow-x-auto mlb-today-table-scroll-hidden">
          {loading ? (
            <div className="p-4 text-slate-600 text-sm">Loading workspace…</div>
          ) : error ? (
            <div className="p-4 text-rose-700 text-sm">{error}</div>
          ) : !isWorkspaceReady ? (
            <div className="p-4 text-slate-600 text-sm">
              Today&apos;s slate is not loaded yet for {requestedSlateDate || slateDate}. Check back after the scheduled refresh.
            </div>
          ) : displayedRows.length === 0 ? (
            <div className="p-4 text-slate-600 text-sm">
              <div>No rows match the current filters.</div>
              <div className="mt-1">Try clearing one or more filters.</div>
            </div>
          ) : (
            <table className="min-w-[1320px] w-full text-sm text-slate-800 table-fixed">
              <colgroup>
                <col style={{ width: "220px" }} />
                <col style={{ width: "170px" }} />
                <col style={{ width: "82px" }} />
                <col style={{ width: "76px" }} />
                <col style={{ width: "118px" }} />
                <col style={{ width: "134px" }} />
                <col style={{ width: "132px" }} />
                <col style={{ width: "190px" }} />
                <col style={{ width: "130px" }} />
                <col style={{ width: "110px" }} />
              </colgroup>
              <tbody>
                {displayedRows.map((r, rowIndex) => {
                  const key = rowKeyForRow(r);
                  const isOpen = expandedRowKeys.has(key);
                  const marketPosition = marketPositionInfo(r);
                  return (
                    <React.Fragment key={key}>
                      <tr
                        ref={(el) => {
                          rowRefs.current[rowIndex] = el;
                        }}
                        tabIndex={0}
                        aria-expanded={isOpen}
                        onFocus={() => setFocusedRowIndex(rowIndex)}
                        onClick={(event) => handleRowClick(event, key)}
                        onKeyDown={(event) => handleRowKeyDown(event, rowIndex, key)}
                        className={`group border-b border-slate-100 align-top focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 ${
                          isOpen ? "bg-slate-100 border-slate-200" : "hover:bg-slate-50"
                        }`}
                      >
                        <td
                          className={`sticky left-0 z-30 py-2.5 px-3 border-r border-slate-100 shadow-[inset_-1px_0_0_0_rgba(148,163,184,0.35)] ${
                            isOpen ? "bg-slate-100" : "bg-white group-hover:bg-slate-50"
                          }`}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0">
                              <div className="font-semibold text-slate-900 truncate" title={r.player_name || ""}>
                                {r.player_name || DASH}
                              </div>
                              <div className="text-xs text-slate-500 truncate" title={`${r.team || DASH} vs ${r.opponent || DASH}`}>
                                {(r.team || DASH)} vs {(r.opponent || DASH)}
                              </div>
                            </div>
                            <button
                              type="button"
                              className="shrink-0 text-[11px] border rounded px-1.5 py-0.5 hover:bg-slate-50"
                              onClick={(event) => {
                                event.stopPropagation();
                                toggleRow(key, "button");
                              }}
                              aria-label={isOpen ? "Collapse row details" : "Expand row details"}
                              title={isOpen ? "Hide details" : "Show details"}
                            >
                              <span className="mr-1" aria-hidden="true">{isOpen ? "▾" : "▸"}</span>
                              {isOpen ? "Hide" : "Details"}
                            </button>
                          </div>
                        </td>
                        <td className="py-2.5 px-3 font-medium">{propLabel(r.prop_type)}</td>
                        <td className="py-2.5 px-3 text-slate-700">
                          <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-semibold tracking-wide">
                            {sideUpper(r.side) || DASH}
                          </span>
                        </td>
                        <td className="py-2.5 px-3 text-right tabular-nums text-slate-700 whitespace-nowrap">{fmtNumber(r.line, 1)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums font-medium whitespace-nowrap">{fmtPrice(r.best_price)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums font-medium whitespace-nowrap">{fmtPrice(r.market_median)}</td>
                        <td className="py-2.5 px-3 text-right tabular-nums whitespace-nowrap">
                          <>
                            {asNumber(r.value_vs_market) === null ? (
                              <>
                                <div>{DASH}</div>
                                <div className="text-[11px] text-slate-500">{marketPositionLabelForRow(marketPosition, r.side)}</div>
                              </>
                            ) : (
                              <>
                                <div
                                  className={`font-semibold ${
                                    Math.abs(asNumber(r.value_vs_market) || 0) < 5 ? "text-slate-500" : "text-slate-800"
                                  }`}
                                >
                                  {fmtPrice(r.value_vs_market, { forceSign: true })}
                                </div>
                                <div className="text-[11px] text-slate-500">{marketPositionLabelForRow(marketPosition, r.side)}</div>
                              </>
                            )}
                          </>
                        </td>
                        <td className="py-2.5 px-3">
                          <div className="text-slate-700">{timingLabelForRow(r.timing_signal, r.side)}</div>
                        </td>
                        <td className="py-2.5 px-3">
                          <div className="text-slate-700">{streakLabel(r.streak_context_label)}</div>
                          {!isMissing(r.streak_count) ? (
                            <div className="text-xs text-slate-500">count: {fmtNumber(r.streak_count, 0)}</div>
                          ) : null}
                        </td>
                        <td className="py-2.5 px-3 text-right tabular-nums whitespace-nowrap">
                          <div className="text-slate-700">{fmtNumber(r.consistency_score, 1)}</div>
                          <div className="text-xs text-slate-500">{consistencySubLabel(r.consistency_score)}</div>
                        </td>
                      </tr>
                      {isOpen ? (
                        <tr className="border-b border-slate-200 bg-slate-100/70">
                          <td colSpan={10} className="py-2 px-3 border-l-2 border-slate-300">
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
                              <div className="space-y-1.5">
                                <div className="text-[11px] uppercase tracking-wide font-semibold text-slate-600">Market</div>
                                <div><span className="text-slate-500">Side:</span> <strong>{sideLabel(r.side)}</strong></div>
                                <div><span className="text-slate-500">Open {sideUpper(r.side) || "SIDE"} median:</span> <strong>{fmtPrice(r.open_price)}</strong></div>
                                <div><span className="text-slate-500">Latest {sideUpper(r.side) || "SIDE"} median:</span> <strong>{fmtPrice(r.latest_price)}</strong></div>
                                <div><span className="text-slate-500">Best {sideUpper(r.side) || "SIDE"} price now:</span> <strong>{fmtPrice(r.best_price)}</strong></div>
                                <div>
                                  <span className="text-slate-500">Δ vs Side Median:</span>{" "}
                                  <strong>{asNumber(r.value_vs_market) === null ? DASH : fmtPrice(r.value_vs_market, { forceSign: true })}</strong>
                                </div>
                                <div><span className="text-slate-500">Δ explanation:</span> <strong>{deltaExplanationForRow(r)}</strong></div>
                                <div><span className="text-slate-500">Market Position:</span> <strong>{marketPositionLabelForRow(marketPosition, r.side)}</strong></div>
                                <div><span className="text-slate-500">Position detail:</span> <strong>{marketPositionDetailForRow(marketPosition, r.side)}</strong></div>
                                <div>
                                  <span className="text-slate-500">{sideUpper(r.side) || "SIDE"} market range:</span>{" "}
                                  <strong>{fmtPrice(r.market_range)}</strong>
                                  <span className="text-slate-500"> ({marketRangeLabel(r.market_range)})</span>
                                </div>
                                <div><span className="text-slate-500">{sideUpper(r.side) || "SIDE"} move from open:</span> <strong>{fmtPrice(r.price_change_from_open, { forceSign: true })}</strong></div>
                                <div><span className="text-slate-500">Snapshots:</span> <strong>{fmtNumber(r.num_snapshots, 0)}</strong></div>
                                <div><span className="text-slate-500">Books quoting this side:</span> <strong>{fmtNumber(r.book_count, 0)}</strong></div>
                              </div>
                              <div className="space-y-1.5">
                                <div className="text-[11px] uppercase tracking-wide font-semibold text-slate-600">Performance</div>
                                <div><span className="text-slate-500">Hit rate last 5:</span> <strong>{fmtPct(r.hit_rate_last_5)}</strong></div>
                                <div><span className="text-slate-500">Hit rate last 10:</span> <strong>{fmtPct(r.hit_rate_last_10)}</strong></div>
                                <div><span className="text-slate-500">Hit rate season:</span> <strong>{fmtPct(r.hit_rate_season)}</strong></div>
                                <div><span className="text-slate-500">Last 5 avg:</span> <strong>{fmtNumber(r.last_5_avg)}</strong></div>
                                <div><span className="text-slate-500">Last 10 avg:</span> <strong>{fmtNumber(r.last_10_avg)}</strong></div>
                                <div><span className="text-slate-500">Season avg:</span> <strong>{fmtNumber(r.season_avg)}</strong></div>
                              </div>
                              <div className="space-y-1.5">
                                <div className="text-[11px] uppercase tracking-wide font-semibold text-slate-600">Context</div>
                                <div><span className="text-slate-500">Streak:</span> <strong>{streakLabel(r.streak_context_label)}</strong></div>
                                <div><span className="text-slate-500">Streak count:</span> <strong>{fmtNumber(r.streak_count, 0)}</strong></div>
                                <div><span className="text-slate-500">Baseline delta:</span> <strong>{fmtPctSigned(r.baseline_delta)}</strong></div>
                                <div><span className="text-slate-500">Consistency:</span> <strong>{fmtNumber(r.consistency_score, 1)}</strong> <span className="text-slate-500">({consistencySubLabel(r.consistency_score)})</span></div>
                                <div className="pt-1 text-[11px] uppercase tracking-wide font-semibold text-slate-600">Timing</div>
                                <div><span className="text-slate-500">Timing label:</span> <strong>{timingLabelForRow(r.timing_signal, r.side)}</strong></div>
                                <div><span className="text-slate-500">Timing detail:</span> <strong>{timingReasonForRow(r.timing_reason, r.side)}</strong></div>
                                <div className="pt-1 text-[11px] uppercase tracking-wide font-semibold text-slate-600">Coverage</div>
                                <div><span className="text-slate-500">Market coverage:</span> <strong>{coverageLabel(r.coverage_quality_label)}</strong></div>
                                <div>
                                  <span className="text-slate-500">Coverage detail:</span>{" "}
                                  <strong>
                                    {isMissing(r.coverage_quality_reason)
                                      ? fallbackMedianTextForRow(r)
                                      : coverageReasonForRow(r.coverage_quality_reason, r.side)}
                                  </strong>
                                </div>
                              </div>
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
    </div>
  );
}
