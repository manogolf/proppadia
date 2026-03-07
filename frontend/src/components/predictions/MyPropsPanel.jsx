import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "react-router-dom";
import { useAuth } from "../../context/AuthContext.jsx";
import { PrefetchLink } from "../navigation/PrefetchLink.jsx";
import { getBaseURL } from "../../shared/getBaseURL.js";
import { normalizeHttpErrorMessage } from "../../shared/httpErrorMessage.js";
import { getPropDisplayLabel } from "../../shared/propUtils.js";
import { nowET, todayET } from "../../shared/timeUtils.js";
import {
  normalizeWatchlistRows,
  WATCHLIST_UPDATED_EVENT,
  readWatchlistScope,
  toWatchlistId,
  writeWatchlistScope,
} from "../../shared/watchlistStorage.js";

const BASE_API = getBaseURL();

function formatStatus(row) {
  const outcome = String(row?.outcome || "").toLowerCase();
  if (["win", "loss", "push"].includes(outcome)) return outcome;
  const status = String(row?.status || "").toLowerCase();
  if (status) return status;
  return "pending";
}

function formatWhen(raw) {
  if (!raw) return "—";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

function toneForStatus(status) {
  if (status === "win") return "bg-emerald-100 text-emerald-800";
  if (status === "loss") return "bg-rose-100 text-rose-700";
  if (status === "push") return "bg-sky-100 text-sky-700";
  if (status === "pending") return "bg-slate-100 text-slate-700";
  return "bg-zinc-100 text-zinc-700";
}

const STATUS_OPTIONS = ["all", "pending", "win", "loss", "push", "resolved", "dnp"];

const CSV_COLUMNS = [
  "id",
  "player_id",
  "player_name",
  "team",
  "team_id",
  "game_id",
  "game_date",
  "prop_type",
  "over_under",
  "prop_value",
  "status_effective",
  "status",
  "outcome",
  "prop_source",
  "predicted_outcome",
  "confidence_score",
  "created_at",
  "updated_at",
];

function escapeCsv(value) {
  const s = value == null ? "" : String(value);
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
    return `"${s.replace(/"/g, "\"\"")}"`;
  }
  return s;
}

export default function MyPropsPanel({
  refreshNonce = 0,
  limit = 20,
  selectedDate = null,
  apiPath = "/api/props/history",
  deletePath = "",
  propSource = "user_added",
  title = "My Saved Props",
  exportPrefix = "my_mlb_props",
}) {
  const location = useLocation();
  const { user } = useAuth();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [rows, setRows] = useState([]);
  const [fromDate, setFromDate] = useState(selectedDate || "");
  const [toDate, setToDate] = useState(selectedDate || "");
  const [statusFilter, setStatusFilter] = useState("all");
  const [page, setPage] = useState(0);
  const [totalRows, setTotalRows] = useState(0);
  const [exporting, setExporting] = useState(false);
  const [notice, setNotice] = useState("");
  const [copiedUrl, setCopiedUrl] = useState(false);
  const [copiedCurl, setCopiedCurl] = useState(false);
  const [copyToast, setCopyToast] = useState("");
  const [sortKey, setSortKey] = useState("saved");
  const [sortDir, setSortDir] = useState("desc");
  const [selectedRow, setSelectedRow] = useState(null);
  const [copiedRowJson, setCopiedRowJson] = useState(false);
  const [copiedRowId, setCopiedRowId] = useState(false);
  const [removingRow, setRemovingRow] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [watchlist, setWatchlist] = useState([]);
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const importWatchInputRef = useRef(null);
  const rowRefs = useRef(new Map());
  const queryAutoSelectRef = useRef("");

  useEffect(() => {
    if (!selectedDate) return;
    setFromDate((prev) => prev || selectedDate);
    setToDate((prev) => prev || selectedDate);
  }, [selectedDate]);

  useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const playerFromUrl = String(params.get("player") || "").trim();
    const teamFromUrl = String(params.get("team") || "").trim();
    const seed = playerFromUrl || teamFromUrl;
    if (seed) {
      setSearchTerm(seed);
      setPage(0);
      queryAutoSelectRef.current = seed.toLowerCase();
    }
  }, [location.search]);

  const refreshWatchlistRows = useCallback(() => {
    if (!user?.id) {
      setWatchlist([]);
      return;
    }
    setWatchlist(readWatchlistScope(user.id, apiPath));
  }, [apiPath, user?.id]);

  const commitWatchlist = useCallback(
    (updater) => {
      if (!user?.id) return;
      setWatchlist((prev) => {
        const nextRaw = typeof updater === "function" ? updater(prev) : updater;
        const next = normalizeWatchlistRows(nextRaw);
        writeWatchlistScope(user.id, apiPath, next);
        return next;
      });
    },
    [apiPath, user?.id]
  );

  useEffect(() => {
    refreshWatchlistRows();
  }, [refreshWatchlistRows]);

  useEffect(() => {
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshWatchlistRows();
      }
    }
    function onWatchlistUpdated() {
      refreshWatchlistRows();
    }
    window.addEventListener("storage", onStorage);
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    };
  }, [refreshWatchlistRows]);

  const watchIdSet = useMemo(
    () => new Set(watchlist.map((w) => String(w.id))),
    [watchlist]
  );

  const fetchRows = useCallback(async () => {
    if (!user?.id) {
      setRows([]);
      setTotalRows(0);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const url = new URL(`${BASE_API}${apiPath}`);
      url.searchParams.set("limit", String(limit));
      url.searchParams.set("offset", String(page * limit));
      url.searchParams.set("user_id", String(user.id));
      if (propSource) url.searchParams.set("prop_source", propSource);
      if (fromDate) url.searchParams.set("from_date", fromDate);
      if (toDate) url.searchParams.set("to_date", toDate);
      if (statusFilter !== "all") url.searchParams.set("status", statusFilter);
      const res = await fetch(url.toString(), {
        mode: "cors",
        credentials: "omit",
      });
      const payload = await res.json();
      if (!res.ok || !payload?.ok) {
        const detail = payload?.detail || payload?.error || "Failed to load member prop history.";
        throw new Error(String(detail));
      }
      const nextRows = Array.isArray(payload.rows) ? payload.rows : [];
      setRows(nextRows);
      setTotalRows(Number(payload.total || 0));
    } catch (e) {
      setRows([]);
      setTotalRows(0);
      setError(normalizeHttpErrorMessage(e, "Failed to load member prop history."));
    } finally {
      setLoading(false);
    }
  }, [apiPath, fromDate, limit, page, propSource, statusFilter, toDate, user?.id]);

  useEffect(() => {
    fetchRows();
  }, [fetchRows, refreshNonce]);

  useEffect(() => {
    setPage(0);
  }, [fromDate, statusFilter, toDate, user?.id]);

  useEffect(() => {
    if (page === 0) return;
    if (page * limit < totalRows) return;
    setPage(Math.max(0, Math.ceil(totalRows / limit) - 1));
  }, [limit, page, totalRows]);

  const emptyMessage = useMemo(() => {
    if (!user?.id) return "Sign in to view your saved prop history.";
    if (loading) return "Loading your saved props…";
    return "No saved props yet.";
  }, [loading, user?.id]);

  const visibleRows = useMemo(() => {
    const q = searchTerm.trim().toLowerCase();
    const baseRows = watchlistOnly
      ? rows.filter((row) => {
          const id = toWatchlistId(row);
          return Boolean(id && watchIdSet.has(id));
        })
      : rows;
    if (!q) return baseRows;
    return baseRows.filter((row) => {
      const haystack = [
        row?.player_name,
        row?.player_id,
        row?.team,
        row?.prop_type,
        getPropDisplayLabel(row?.prop_type || ""),
        row?.over_under,
      ]
        .map((v) => String(v || "").toLowerCase())
        .join(" ");
      return haystack.includes(q);
    });
  }, [rows, searchTerm, watchIdSet, watchlistOnly]);
  const sortedRows = useMemo(() => {
    const out = [...visibleRows];
    const keyFn = (row) => {
      if (sortKey === "player") return String(row?.player_name || row?.player_id || "").toLowerCase();
      if (sortKey === "prop") return String(getPropDisplayLabel(row?.prop_type || "")).toLowerCase();
      if (sortKey === "line") return Number(row?.prop_value ?? 0);
      if (sortKey === "status") return String(formatStatus(row)).toLowerCase();
      if (sortKey === "saved") return new Date(row?.created_at || row?.prediction_timestamp || 0).getTime();
      return String(row?.[sortKey] || "").toLowerCase();
    };
    out.sort((a, b) => {
      const av = keyFn(a);
      const bv = keyFn(b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return out;
  }, [sortDir, sortKey, visibleRows]);

  useEffect(() => {
    const q = queryAutoSelectRef.current;
    if (!q || sortedRows.length === 0) return;
    const match = sortedRows.find((row) => {
      const name = String(row?.player_name || "").toLowerCase();
      const pid = String(row?.player_id || "").toLowerCase();
      return name.includes(q) || pid === q;
    });
    if (!match) return;
    setSelectedRow(match);
    const rowEl = rowRefs.current.get(String(match.id));
    if (rowEl && typeof rowEl.scrollIntoView === "function") {
      rowEl.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
    queryAutoSelectRef.current = "";
  }, [sortedRows]);
  const statusSummary = useMemo(() => {
    const out = { pending: 0, win: 0, loss: 0, push: 0 };
    for (const row of sortedRows) {
      const key = formatStatus(row);
      if (Object.prototype.hasOwnProperty.call(out, key)) out[key] += 1;
    }
    return out;
  }, [sortedRows]);
  const activeFiltersLabel = useMemo(() => {
    const parts = [];
    if (fromDate || toDate) parts.push(`Date: ${fromDate || "start"} to ${toDate || "today"}`);
    if (statusFilter !== "all") parts.push(`Status: ${statusFilter}`);
    if (searchTerm.trim()) parts.push(`Search: "${searchTerm.trim()}"`);
    if (watchlistOnly) parts.push("Watchlist only");
    if (!parts.length) return "No active filters";
    return parts.join(" • ");
  }, [fromDate, searchTerm, statusFilter, toDate, watchlistOnly]);
  const topPlayers = useMemo(() => {
    const counts = new Map();
    for (const row of sortedRows) {
      const name = String(row?.player_name || row?.player_id || "Unknown").trim();
      if (!name) continue;
      counts.set(name, (counts.get(name) || 0) + 1);
    }
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 5);
  }, [sortedRows]);
  const topProps = useMemo(() => {
    const counts = new Map();
    for (const row of sortedRows) {
      const label = getPropDisplayLabel(row?.prop_type || "Unknown");
      counts.set(label, (counts.get(label) || 0) + 1);
    }
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 5);
  }, [sortedRows]);
  const playerActivityMap = useMemo(() => {
    const out = new Map();
    const todayIso = todayET();
    for (const row of sortedRows) {
      const key = toWatchlistId(row);
      if (!key) continue;
      const status = String(formatStatus(row)).toLowerCase();
      const gameDate = String(row?.game_date || "");
      const isLive = status === "live";
      const isTodayPending = status === "pending" && gameDate === todayIso;
      const prev = out.get(key) || { activeNow: false, liveNow: false };
      out.set(key, {
        activeNow: prev.activeNow || isLive || isTodayPending,
        liveNow: prev.liveNow || isLive,
      });
    }
    return out;
  }, [sortedRows]);
  const totalPages = Math.max(1, Math.ceil(totalRows / limit));
  const hasMore = (page + 1) * limit < totalRows;
  const canPrev = page > 0;

  useEffect(() => {
    if (!selectedRow?.id) return;
    const stillVisible = sortedRows.some((r) => String(r.id) === String(selectedRow.id));
    if (!stillVisible) setSelectedRow(null);
  }, [selectedRow?.id, sortedRows]);

  const buildHistoryUrl = useCallback(
    ({ reqLimit, reqOffset }) => {
      const currentUserId = user?.id ? String(user.id) : "";
      const url = new URL(`${BASE_API}/api/props/history`);
      url.searchParams.set("limit", String(reqLimit));
      url.searchParams.set("offset", String(reqOffset));
      url.searchParams.set("user_id", currentUserId);
      if (propSource) url.searchParams.set("prop_source", propSource);
      if (fromDate) url.searchParams.set("from_date", fromDate);
      if (toDate) url.searchParams.set("to_date", toDate);
      if (statusFilter !== "all") url.searchParams.set("status", statusFilter);
      return url;
    },
    [apiPath, fromDate, propSource, statusFilter, toDate, user?.id]
  );

  const handleExportCsv = useCallback(async () => {
    if (!user?.id || exporting) return;
    setExporting(true);
    setError("");
    setNotice("");
    try {
      const batchSize = 200;
      const allRows = [];
      let offset = 0;
      let total = 0;
      let guard = 0;

      do {
        const url = buildHistoryUrl({ reqLimit: batchSize, reqOffset: offset });
        const res = await fetch(url.toString(), { mode: "cors", credentials: "omit" });
        const payload = await res.json();
        if (!res.ok || !payload?.ok) {
          const detail = payload?.detail || payload?.error || "Failed to export member prop history.";
          throw new Error(String(detail));
        }
        const batch = Array.isArray(payload.rows) ? payload.rows : [];
        total = Number(payload.total || 0);
        allRows.push(...batch);
        offset += batch.length;
        guard += 1;
        if (guard > 50) break;
        if (batch.length === 0) break;
      } while (offset < total);

      const lines = [];
      lines.push(CSV_COLUMNS.join(","));
      for (const row of allRows) {
        const statusEffective = formatStatus(row);
        const cells = CSV_COLUMNS.map((key) => {
          if (key === "status_effective") return escapeCsv(statusEffective);
          return escapeCsv(row?.[key]);
        });
        lines.push(cells.join(","));
      }

      const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const rangeLabel =
        fromDate || toDate ? `${fromDate || "start"}_${toDate || "today"}` : "all_dates";
      const statusLabel = statusFilter || "all";
      a.href = href;
      a.download = `${exportPrefix}_${rangeLabel}_${statusLabel}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(href);
      setNotice(
        `Exported ${allRows.length} rows at ${new Date().toLocaleTimeString()}.`
      );
    } catch (e) {
      setError(normalizeHttpErrorMessage(e, "Failed to export member prop history."));
    } finally {
      setExporting(false);
    }
  }, [buildHistoryUrl, exportPrefix, exporting, fromDate, statusFilter, toDate, user?.id]);

  const handleCopyApiUrl = useCallback(async () => {
    if (!user?.id) return;
    const url = buildHistoryUrl({ reqLimit: limit, reqOffset: page * limit }).toString();
    try {
      await navigator.clipboard.writeText(url);
      setCopiedUrl(true);
      setCopyToast("API URL copied");
      setTimeout(() => setCopiedUrl(false), 1200);
      setTimeout(() => setCopyToast(""), 1200);
    } catch {
      setError("Failed to copy API URL.");
    }
  }, [buildHistoryUrl, limit, page, user?.id]);

  const handleCopyCurl = useCallback(async () => {
    if (!user?.id) return;
    const url = buildHistoryUrl({ reqLimit: limit, reqOffset: page * limit }).toString();
    const curl = `curl "${url}"`;
    try {
      await navigator.clipboard.writeText(curl);
      setCopiedCurl(true);
      setCopyToast("cURL copied");
      setTimeout(() => setCopiedCurl(false), 1200);
      setTimeout(() => setCopyToast(""), 1200);
    } catch {
      setError("Failed to copy cURL command.");
    }
  }, [buildHistoryUrl, limit, page, user?.id]);

  const setSort = useCallback((key) => {
    setSortKey((prev) => {
      if (prev !== key) {
        setSortDir("asc");
        return key;
      }
      setSortDir((dir) => (dir === "asc" ? "desc" : "asc"));
      return prev;
    });
  }, []);

  const sortArrow = useCallback(
    (key) => {
      if (sortKey !== key) return "";
      return sortDir === "asc" ? " ↑" : " ↓";
    },
    [sortDir, sortKey]
  );

  const handleResetFilters = useCallback(() => {
    setFromDate(selectedDate || "");
    setToDate(selectedDate || "");
    setStatusFilter("all");
    setSearchTerm("");
    setWatchlistOnly(false);
    setPage(0);
    setNotice("");
    setError("");
  }, [selectedDate]);

  const handleCopyRowJson = useCallback(async () => {
    if (!selectedRow) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(selectedRow, null, 2));
      setCopiedRowJson(true);
      setTimeout(() => setCopiedRowJson(false), 1200);
    } catch {
      setError("Failed to copy row JSON.");
    }
  }, [selectedRow]);

  const handleCopyRowId = useCallback(async () => {
    const id = selectedRow?.id;
    if (!id) return;
    try {
      await navigator.clipboard.writeText(String(id));
      setCopiedRowId(true);
      setTimeout(() => setCopiedRowId(false), 1200);
    } catch {
      setError("Failed to copy row id.");
    }
  }, [selectedRow?.id]);

  const handleRemoveSelectedRow = useCallback(async () => {
    if (!deletePath || !selectedRow?.id || !user?.id) return;
    setRemovingRow(true);
    setError("");
    setNotice("");
    try {
      const res = await fetch(`${BASE_API}${deletePath}`, {
        method: "POST",
        mode: "cors",
        credentials: "omit",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: String(selectedRow.id),
          user_id: String(user.id),
        }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok || !payload?.ok) {
        const detail = payload?.detail || payload?.error || "Failed to remove saved prop row.";
        throw new Error(String(detail));
      }
      if (!payload?.deleted) {
        throw new Error("Row not found or already removed.");
      }
      setSelectedRow(null);
      await fetchRows();
      setNotice("Saved prop removed from view.");
    } catch (e) {
      setError(normalizeHttpErrorMessage(e, "Failed to remove saved prop row."));
    } finally {
      setRemovingRow(false);
    }
  }, [deletePath, fetchRows, selectedRow, user?.id]);

  const selectedWatchId = useMemo(() => (selectedRow ? toWatchlistId(selectedRow) : ""), [selectedRow]);
  const isSelectedInWatchlist = useMemo(
    () => Boolean(selectedWatchId && watchIdSet.has(selectedWatchId)),
    [selectedWatchId, watchIdSet]
  );

  const addSelectedToWatchlist = useCallback(() => {
    if (!selectedRow) return;
    const id = toWatchlistId(selectedRow);
    if (!id) return;
    commitWatchlist((prev) => {
      if (prev.some((w) => String(w.id) === id)) return prev;
      const item = {
        id,
        player_id: selectedRow.player_id ?? null,
        player_name: selectedRow.player_name || null,
        team: selectedRow.team || null,
        added_at: new Date().toISOString(),
      };
      return [item, ...prev];
    });
    setNotice("Player added to watchlist.");
    setError("");
  }, [commitWatchlist, selectedRow]);

  const removeSelectedFromWatchlist = useCallback(() => {
    if (!selectedRow) return;
    const id = toWatchlistId(selectedRow);
    commitWatchlist((prev) => prev.filter((w) => String(w.id) !== id));
    setNotice("Player removed from watchlist.");
    setError("");
  }, [commitWatchlist, selectedRow]);

  const removeWatchItem = useCallback((id) => {
    commitWatchlist((prev) => prev.filter((w) => String(w.id) !== String(id)));
  }, [commitWatchlist]);

  const toggleTopPlayerWatch = useCallback(
    (playerName) => {
      const targetName = String(playerName || "").trim();
      if (!targetName) return;
      const match = sortedRows.find(
        (row) => String(row?.player_name || row?.player_id || "Unknown").trim() === targetName
      );
      if (!match) {
        setError(`Could not find ${targetName} in current rows.`);
        return;
      }
      const id = toWatchlistId(match);
      if (!id) return;
      const exists = watchIdSet.has(id);
      commitWatchlist((prev) => {
        if (exists) {
          return prev.filter((w) => String(w.id) !== id);
        }
        const item = {
          id,
          player_id: match.player_id ?? null,
          player_name: match.player_name || null,
          team: match.team || null,
          added_at: new Date().toISOString(),
        };
        return [item, ...prev];
      });
      setNotice(exists ? "Player removed from watchlist." : "Player added to watchlist.");
      setError("");
    },
    [commitWatchlist, sortedRows, watchIdSet]
  );

  const topPlayerWatchState = useMemo(() => {
    const state = new Map();
    for (const [name] of topPlayers) {
      const match = sortedRows.find(
        (row) => String(row?.player_name || row?.player_id || "Unknown").trim() === name
      );
      const id = match ? toWatchlistId(match) : "";
      state.set(name, Boolean(id && watchIdSet.has(id)));
    }
    return state;
  }, [sortedRows, topPlayers, watchIdSet]);

  const handleExportWatchlist = useCallback(() => {
    if (!watchlist.length) return;
    try {
      const blob = new Blob([JSON.stringify(watchlist, null, 2)], {
        type: "application/json;charset=utf-8;",
      });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = `watchlist_${String(user?.id || "member")}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(href);
      setNotice(`Exported ${watchlist.length} watchlist rows.`);
      setError("");
    } catch {
      setError("Failed to export watchlist.");
    }
  }, [user?.id, watchlist]);

  const handleImportWatchlistClick = useCallback(() => {
    importWatchInputRef.current?.click();
  }, []);

  const handleImportWatchlist = useCallback(async (e) => {
    const file = e?.target?.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      if (!Array.isArray(parsed)) throw new Error("Watchlist file must be an array.");
      const normalized = parsed
        .map((w) => ({
          id: String(w?.id || "").trim(),
          player_id: w?.player_id ?? null,
          player_name: w?.player_name || null,
          team: w?.team || null,
          added_at: w?.added_at || new Date().toISOString(),
        }))
        .filter((w) => w.id);
      commitWatchlist((prev) => {
        const byId = new Map(normalizeWatchlistRows(prev).map((w) => [String(w.id), w]));
        for (const row of normalized) byId.set(String(row.id), row);
        return Array.from(byId.values());
      });
      setNotice(`Imported ${normalized.length} watchlist rows.`);
      setError("");
    } catch (err) {
      setError(err?.message || "Failed to import watchlist.");
    } finally {
      if (e?.target) e.target.value = "";
    }
  }, [commitWatchlist]);

  const applyDatePreset = useCallback(
    (preset) => {
      const today = todayET();
      if (preset === "today") {
        setFromDate(today);
        setToDate(today);
        setPage(0);
        return;
      }
      if (preset === "7d") {
        setFromDate(nowET().minus({ days: 6 }).toISODate());
        setToDate(today);
        setPage(0);
        return;
      }
      if (preset === "30d") {
        setFromDate(nowET().minus({ days: 29 }).toISODate());
        setToDate(today);
        setPage(0);
        return;
      }
      setFromDate("");
      setToDate("");
      setPage(0);
    },
    []
  );

  const handleCopyApiKeyDown = useCallback(
    (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "c") {
        e.preventDefault();
        handleCopyApiUrl();
      }
    },
    [handleCopyApiUrl]
  );

  const handleCopyCurlKeyDown = useCallback(
    (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "c") {
        e.preventDefault();
        handleCopyCurl();
      }
    },
    [handleCopyCurl]
  );

  return (
    <section className="pp-card p-4">
      <div className="flex items-center justify-between mb-3 gap-2 relative">
        <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={fetchRows}
          disabled={loading || !user?.id}
        >
          {loading ? "Refreshing…" : "Refresh"}
        </button>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={handleExportCsv}
          disabled={loading || exporting || !user?.id || totalRows === 0}
        >
          {exporting ? "Exporting…" : "Export CSV"}
        </button>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={handleCopyApiUrl}
          onKeyDown={handleCopyApiKeyDown}
          disabled={loading || !user?.id}
          title="Focus button then press Cmd/Ctrl+C"
        >
          {copiedUrl ? "Copied" : "Copy API URL"}
        </button>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={handleCopyCurl}
          onKeyDown={handleCopyCurlKeyDown}
          disabled={loading || !user?.id}
          title="Focus button then press Cmd/Ctrl+C"
        >
          {copiedCurl ? "Copied" : "Copy cURL"}
        </button>
        {copyToast ? (
          <div className="absolute right-0 -bottom-7 text-xs px-2 py-1 rounded-md bg-slate-900 text-white shadow">
            {copyToast}
          </div>
        ) : null}
      </div>
      <div className="mb-3 text-xs text-slate-500">
        Shortcut: focus a copy button, then press Cmd/Ctrl+C.
      </div>
      <div className="mb-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs text-slate-700">
        {activeFiltersLabel}
      </div>
      <div className="mb-3">
        <label className="text-sm text-slate-700 block">
          <span className="block mb-1">Quick Search</span>
          <input
            type="text"
            className="w-full rounded-md border border-slate-300 px-3 py-2"
            placeholder="Search player, team, prop..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </label>
        <label className="mt-2 inline-flex items-center gap-2 text-xs text-slate-600">
          <input
            type="checkbox"
            checked={watchlistOnly}
            onChange={(e) => setWatchlistOnly(e.target.checked)}
          />
          Watchlist only
        </label>
      </div>
      <div className="mb-3 grid grid-cols-1 md:grid-cols-4 gap-2">
        <label className="text-sm text-slate-700">
          <span className="block mb-1">From</span>
          <input
            type="date"
            className="w-full rounded-md border border-slate-300 px-2 py-1"
            value={fromDate}
            onChange={(e) => setFromDate(e.target.value)}
            max={toDate || todayET()}
          />
        </label>
        <label className="text-sm text-slate-700">
          <span className="block mb-1">To</span>
          <input
            type="date"
            className="w-full rounded-md border border-slate-300 px-2 py-1"
            value={toDate}
            onChange={(e) => setToDate(e.target.value)}
            min={fromDate || undefined}
            max={todayET()}
          />
        </label>
        <label className="text-sm text-slate-700">
          <span className="block mb-1">Status</span>
          <select
            className="w-full rounded-md border border-slate-300 px-2 py-1 bg-white"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            {STATUS_OPTIONS.map((opt) => (
              <option key={opt} value={opt}>
                {opt === "all" ? "All statuses" : opt}
              </option>
            ))}
          </select>
        </label>
        <div className="text-sm text-slate-700">
          <span className="block mb-1">Quick Range</span>
          <div className="flex flex-wrap gap-1">
            <button type="button" className="pp-btn pp-btn-secondary pp-btn-sm" onClick={() => applyDatePreset("today")}>
              Today
            </button>
            <button type="button" className="pp-btn pp-btn-secondary pp-btn-sm" onClick={() => applyDatePreset("7d")}>
              Last 7d
            </button>
            <button type="button" className="pp-btn pp-btn-secondary pp-btn-sm" onClick={() => applyDatePreset("30d")}>
              Last 30d
            </button>
            <button type="button" className="pp-btn pp-btn-secondary pp-btn-sm" onClick={() => applyDatePreset("all")}>
              All
            </button>
          </div>
        </div>
        <div className="text-sm text-slate-600 flex items-end pb-1">
          <div className="w-full flex items-center justify-between gap-2">
            <span>Showing {visibleRows.length} of {totalRows}</span>
            <button
              type="button"
              className="pp-btn pp-btn-secondary pp-btn-sm"
              onClick={handleResetFilters}
              disabled={loading}
            >
              Reset Filters
            </button>
          </div>
        </div>
      </div>
      <div className="mb-3 flex flex-wrap gap-2">
        <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs">
          Pending <strong>{statusSummary.pending}</strong>
        </span>
        <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-800 px-2 py-1 text-xs">
          Win <strong>{statusSummary.win}</strong>
        </span>
        <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-1 text-xs">
          Loss <strong>{statusSummary.loss}</strong>
        </span>
        <span className="inline-flex items-center gap-1 rounded-full bg-sky-100 text-sky-700 px-2 py-1 text-xs">
          Push <strong>{statusSummary.push}</strong>
        </span>
      </div>
      <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
        <div className="text-xs font-semibold text-slate-700 mb-1">
          Top Players in View (Top {Math.min(5, sortedRows.length)} from {sortedRows.length} props)
        </div>
        {topPlayers.length === 0 ? (
          <div className="text-xs text-slate-500">No player rows in current filter.</div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {topPlayers.map(([name, count]) => (
              <span
                key={name}
                className="inline-flex items-center gap-1 rounded-full bg-white border border-slate-300 px-2 py-1 text-xs text-slate-700"
              >
                <button
                  type="button"
                  className="underline"
                  onClick={() => setSearchTerm(String(name || ""))}
                  title="Filter table by this player"
                >
                  {name}
                </button>
                <strong>{count}</strong>
                {(() => {
                  const row = sortedRows.find(
                    (r) => String(r?.player_name || r?.player_id || "Unknown").trim() === String(name)
                  );
                  const activity = row ? playerActivityMap.get(toWatchlistId(row)) : null;
                  if (!activity?.activeNow) return null;
                  return (
                    <span
                      className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${
                        activity.liveNow
                          ? "bg-rose-100 text-rose-700"
                          : "bg-emerald-100 text-emerald-700"
                      }`}
                      title={activity.liveNow ? "Live now" : "Active today"}
                    >
                      {activity.liveNow ? "LIVE" : "ACTIVE"}
                    </span>
                  );
                })()}
                <PrefetchLink
                  to="/watchlist"
                  className="text-slate-500 underline"
                  title="Open watchlist page"
                >
                  WL
                </PrefetchLink>
                <button
                  type="button"
                  className="text-slate-500 underline"
                  onClick={() => toggleTopPlayerWatch(name)}
                  title={
                    topPlayerWatchState.get(name)
                      ? "Remove player from watchlist"
                      : "Add player to watchlist"
                  }
                >
                  {topPlayerWatchState.get(name) ? "Unwatch" : "Watch"}
                </button>
              </span>
            ))}
          </div>
        )}
      </div>
      <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
        <div className="text-xs font-semibold text-slate-700 mb-1">Props in View (by type)</div>
        {topProps.length === 0 ? (
          <div className="text-xs text-slate-500">No prop rows in current filter.</div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {topProps.map(([label, count]) => (
              <span
                key={label}
                className="inline-flex items-center gap-1 rounded-full bg-white border border-slate-300 px-2 py-1 text-xs text-slate-700"
              >
                {label}
                <strong>{count}</strong>
              </span>
            ))}
          </div>
        )}
      </div>
      <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
        <div className="text-xs font-semibold text-slate-700 mb-1">
          Watchlist ({watchlist.length})
        </div>
        <div className="mb-2 flex items-center gap-2">
          <button
            type="button"
            className="pp-btn pp-btn-secondary pp-btn-sm"
            onClick={handleExportWatchlist}
            disabled={watchlist.length === 0}
          >
            Export Watchlist
          </button>
          <button
            type="button"
            className="pp-btn pp-btn-secondary pp-btn-sm"
            onClick={handleImportWatchlistClick}
          >
            Import Watchlist
          </button>
          <input
            ref={importWatchInputRef}
            type="file"
            accept="application/json,.json"
            className="hidden"
            onChange={handleImportWatchlist}
          />
        </div>
        {watchlist.length === 0 ? (
          <div className="text-xs text-slate-500">Save a player from Row Details to build your watchlist.</div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {watchlist.map((w) => (
              <span
                key={w.id}
                className="inline-flex items-center gap-1 rounded-full bg-white border border-slate-300 px-2 py-1 text-xs text-slate-700"
              >
                <button
                  type="button"
                  className="underline"
                  onClick={() => setSearchTerm(String(w.player_name || w.player_id || ""))}
                  title="Filter table by this player"
                >
                  {w.player_name || w.player_id || "Unknown"}
                </button>
                {w.team ? <span className="text-slate-500">({w.team})</span> : null}
                {(() => {
                  const activity = playerActivityMap.get(String(w.id));
                  if (!activity?.activeNow) return null;
                  return (
                    <span
                      className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${
                        activity.liveNow
                          ? "bg-rose-100 text-rose-700"
                          : "bg-emerald-100 text-emerald-700"
                      }`}
                      title={activity.liveNow ? "Live now" : "Active today"}
                    >
                      {activity.liveNow ? "LIVE" : "ACTIVE"}
                    </span>
                  );
                })()}
                <button
                  type="button"
                  className="text-rose-700"
                  onClick={() => removeWatchItem(w.id)}
                  title="Remove from watchlist"
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        )}
      </div>
      {error ? (
        <div className="text-sm text-rose-700 bg-rose-50 rounded-md px-3 py-2 mb-3">{error}</div>
      ) : null}
      {!error && notice ? (
        <div className="text-sm text-emerald-700 bg-emerald-50 rounded-md px-3 py-2 mb-3">
          {notice}
        </div>
      ) : null}
      {sortedRows.length === 0 ? (
        <div className="text-sm text-slate-600">{emptyMessage}</div>
      ) : (
        <div>
          <div className="rounded-md border border-slate-200 overflow-hidden">
            <div className="overflow-auto max-h-[34rem]">
              <table className="min-w-full text-sm">
            <thead className="bg-slate-100 text-slate-700 shadow-sm">
              <tr>
                <th colSpan={5} className="px-3 pt-2 pb-1 text-left text-xs font-semibold">
                  Players in View ({sortedRows.length})
                </th>
              </tr>
              <tr>
                <th className="px-3 pt-1 pb-2 text-left">
                  <button type="button" className="font-medium" onClick={() => setSort("player")}>
                    Player{sortArrow("player")}
                  </button>
                </th>
                <th className="px-3 pt-1 pb-2 text-left">
                  <button type="button" className="font-medium" onClick={() => setSort("prop")}>
                    Prop{sortArrow("prop")}
                  </button>
                </th>
                <th className="px-3 pt-1 pb-2 text-left">
                  <button type="button" className="font-medium" onClick={() => setSort("line")}>
                    Line{sortArrow("line")}
                  </button>
                </th>
                <th className="px-3 pt-1 pb-2 text-left">
                  <button type="button" className="font-medium" onClick={() => setSort("status")}>
                    Status{sortArrow("status")}
                  </button>
                </th>
                <th className="px-3 pt-1 pb-2 text-left">
                  <button type="button" className="font-medium" onClick={() => setSort("saved")}>
                    Saved{sortArrow("saved")}
                  </button>
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => {
                const status = formatStatus(row);
                const isActive = selectedRow && String(selectedRow.id) === String(row.id);
                return (
                  <tr
                    key={String(row.id)}
                    ref={(el) => {
                      const key = String(row.id);
                      if (el) rowRefs.current.set(key, el);
                      else rowRefs.current.delete(key);
                    }}
                    className={`border-t border-slate-200 cursor-pointer ${isActive ? "bg-slate-50" : "hover:bg-slate-50"}`}
                    onClick={() => setSelectedRow(row)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        setSelectedRow(row);
                      }
                    }}
                    tabIndex={0}
                    aria-label={`Open details for ${row.player_name || row.player_id || "row"}`}
                  >
                    <td className="px-3 py-2">
                      {row.player_name || row.player_id || "Unknown"}{" "}
                      <span className="text-slate-500">{row.team ? `(${row.team})` : ""}</span>
                    </td>
                    <td className="px-3 py-2">{getPropDisplayLabel(row.prop_type)}</td>
                    <td className="px-3 py-2">
                      {row.over_under} {row.prop_value}
                    </td>
                    <td className="px-3 py-2">
                      <span className={`inline-flex rounded-full px-2 py-1 text-xs font-medium ${toneForStatus(status)}`}>
                        {status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-slate-600">
                      {formatWhen(row.created_at || row.prediction_timestamp)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
              </table>
            </div>
          </div>
          {selectedRow ? (
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3">
              <div className="flex items-center justify-between gap-2">
                <div>
                  <div className="text-sm font-semibold text-slate-900">
                    Row Details: {selectedRow.player_name || selectedRow.player_id || "Unknown"}
                  </div>
                  <div className="text-xs text-slate-600">
                    id={selectedRow.id} • status={formatStatus(selectedRow)} • saved={formatWhen(selectedRow.created_at || selectedRow.prediction_timestamp)}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {isSelectedInWatchlist ? (
                    <button
                      type="button"
                      className="pp-btn pp-btn-secondary pp-btn-sm"
                      onClick={removeSelectedFromWatchlist}
                    >
                      Remove Watch
                    </button>
                  ) : (
                    <button
                      type="button"
                      className="pp-btn pp-btn-secondary pp-btn-sm"
                      onClick={addSelectedToWatchlist}
                    >
                      Add Watch
                    </button>
                  )}
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={handleCopyRowId}
                  >
                    {copiedRowId ? "Copied ID" : "Copy ID"}
                  </button>
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={handleCopyRowJson}
                  >
                    {copiedRowJson ? "Copied" : "Copy JSON"}
                  </button>
                  {deletePath ? (
                    <button
                      type="button"
                      className="pp-btn pp-btn-secondary pp-btn-sm"
                      onClick={handleRemoveSelectedRow}
                      disabled={removingRow}
                      title="Remove this saved row from your NHL props history"
                    >
                      {removingRow ? "Removing…" : "Remove from View"}
                    </button>
                  ) : null}
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={() => setSelectedRow(null)}
                  >
                    Close
                  </button>
                  <PrefetchLink
                    to="/watchlist"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                  >
                    Open Watchlist
                  </PrefetchLink>
                </div>
              </div>
              <pre className="mt-2 text-xs whitespace-pre-wrap break-words">
                {JSON.stringify(selectedRow, null, 2)}
              </pre>
            </div>
          ) : null}
          <div className="mt-3 flex items-center justify-between">
            <div className="text-xs text-slate-500">
              Page {Math.min(page + 1, totalPages)} of {totalPages}
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={loading || !canPrev}
              >
                Prev
              </button>
              <button
                type="button"
                className="pp-btn pp-btn-secondary pp-btn-sm"
                onClick={() => setPage((p) => p + 1)}
                disabled={loading || !hasMore}
              >
                Next
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
