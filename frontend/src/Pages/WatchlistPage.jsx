import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { PrefetchLink } from "../components/navigation/PrefetchLink.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import {
  WATCHLIST_UPDATED_EVENT,
  WATCHLIST_SCOPE_MLB,
  WATCHLIST_SCOPE_NHL,
  readWatchlistScope,
  writeWatchlistScope,
} from "../shared/watchlistStorage.js";

const WATCHLIST_PAGE_PREFS_KEY = "proppadia_watchlist_page_prefs_v1";

function playerQuery(row) {
  return encodeURIComponent(String(row?.player_name || row?.player_id || "").trim());
}

function formatAddedAt(value) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

function addedRecency(value) {
  if (!value) return { label: "Added: unknown", tone: "muted" };
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return { label: "Added: unknown", tone: "muted" };
  const now = new Date();
  const days = Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60 * 24));
  if (days <= 0) return { label: "Added today", tone: "fresh" };
  if (days <= 7) return { label: `Added ${days}d ago`, tone: "fresh" };
  if (days <= 30) return { label: `Added ${days}d ago`, tone: "warn" };
  return { label: `Added ${days}d ago`, tone: "stale" };
}

function recencyBucket(value) {
  if (!value) return "unknown";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "unknown";
  const now = new Date();
  const days = Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60 * 24));
  if (days <= 7) return "fresh";
  if (days <= 30) return "aging";
  return "stale";
}

function toRowId(row) {
  const pid = row?.player_id;
  if (pid !== undefined && pid !== null && String(pid).trim() !== "") return String(pid);
  const name = String(row?.player_name || "").trim().toLowerCase();
  const team = String(row?.team || row?.team_abbr || "").trim().toLowerCase();
  return `${name}:${team}`;
}

function normalizeRows(rows) {
  if (!Array.isArray(rows)) return [];
  const dedup = new Map();
  for (const row of rows) {
    const id = toRowId(row);
    if (!id) continue;
    const candidate = {
      id,
      player_id:
        row?.player_id !== undefined && row?.player_id !== null && String(row.player_id).trim() !== ""
          ? row.player_id
          : null,
      player_name: row?.player_name ? String(row.player_name) : null,
      team: row?.team ? String(row.team) : row?.team_abbr ? String(row.team_abbr) : null,
      added_at: row?.added_at && !Number.isNaN(new Date(row.added_at).getTime())
        ? String(row.added_at)
        : new Date().toISOString(),
    };
    const existing = dedup.get(id);
    if (!existing) {
      dedup.set(id, candidate);
      continue;
    }
    const existingTs = new Date(existing.added_at || 0).getTime();
    const candidateTs = new Date(candidate.added_at || 0).getTime();
    if (candidateTs > existingTs) dedup.set(id, candidate);
  }
  return Array.from(dedup.values())
    .sort((a, b) => new Date(b.added_at || 0).getTime() - new Date(a.added_at || 0).getTime())
    .slice(0, 100);
}

export default function WatchlistPage() {
  const { user } = useAuth();
  const [mlbRows, setMlbRows] = useState([]);
  const [nhlRows, setNhlRows] = useState([]);
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState("newest");
  const [viewScope, setViewScope] = useState("all");
  const [recencyFilter, setRecencyFilter] = useState("all");
  const [importMode, setImportMode] = useState("replace");
  const [copyNotice, setCopyNotice] = useState("");
  const [undoState, setUndoState] = useState(null);
  const importInputRef = useRef(null);
  const searchInputRef = useRef(null);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(WATCHLIST_PAGE_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed?.sortBy === "newest" || parsed?.sortBy === "oldest" || parsed?.sortBy === "name") {
        setSortBy(parsed.sortBy);
      }
      if (parsed?.viewScope === "all" || parsed?.viewScope === "mlb" || parsed?.viewScope === "nhl") {
        setViewScope(parsed.viewScope);
      }
      if (parsed?.recencyFilter === "all" || parsed?.recencyFilter === "fresh" || parsed?.recencyFilter === "aging" || parsed?.recencyFilter === "stale") {
        setRecencyFilter(parsed.recencyFilter);
      }
      if (parsed?.importMode === "replace" || parsed?.importMode === "merge") {
        setImportMode(parsed.importMode);
      }
    } catch {
      // ignore malformed local preferences
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        WATCHLIST_PAGE_PREFS_KEY,
        JSON.stringify({ sortBy, viewScope, recencyFilter, importMode })
      );
    } catch {
      // ignore local preference write errors
    }
  }, [importMode, recencyFilter, sortBy, viewScope]);

  const refreshRows = useCallback(() => {
    if (!user?.id) {
      setMlbRows([]);
      setNhlRows([]);
      return;
    }
    setMlbRows(readWatchlistScope(user.id, WATCHLIST_SCOPE_MLB));
    setNhlRows(readWatchlistScope(user.id, WATCHLIST_SCOPE_NHL));
  }, [user?.id]);

  useEffect(() => {
    refreshRows();
  }, [refreshRows]);

  useEffect(() => {
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshRows();
      }
    }
    function onWatchlistUpdated() {
      refreshRows();
    }
    window.addEventListener("storage", onStorage);
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    };
  }, [refreshRows]);

  useEffect(() => {
    function onKeyDown(e) {
      if (e.defaultPrevented) return;
      const tag = String(document.activeElement?.tagName || "").toLowerCase();
      const typing =
        tag === "input" || tag === "textarea" || tag === "select" || document.activeElement?.isContentEditable;
      if (!typing) {
        if (e.key === "1") {
          e.preventDefault();
          setRecencyFilter("all");
          return;
        }
        if (e.key === "2") {
          e.preventDefault();
          setRecencyFilter("fresh");
          return;
        }
        if (e.key === "3") {
          e.preventDefault();
          setRecencyFilter("aging");
          return;
        }
        if (e.key === "4") {
          e.preventDefault();
          setRecencyFilter("stale");
          return;
        }
      }
      if (e.key === "Escape") {
        if (!query.trim()) return;
        const active = document.activeElement;
        const inSearch = active === searchInputRef.current;
        if (!inSearch) return;
        e.preventDefault();
        setQuery("");
        return;
      }
      if (e.key !== "/") return;
      if (typing) return;
      e.preventDefault();
      searchInputRef.current?.focus();
      searchInputRef.current?.select?.();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [query]);

  const removeRow = useCallback(
    (scopePath, id) => {
      if (!user?.id) return;
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        const next = mlbRows.filter((r) => String(r.id) !== String(id));
        setMlbRows(next);
        writeWatchlistScope(user.id, scopePath, next);
        return;
      }
      const next = nhlRows.filter((r) => String(r.id) !== String(id));
      setNhlRows(next);
      writeWatchlistScope(user.id, scopePath, next);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const clearScope = useCallback(
    (scopePath, bypassConfirm = false) => {
      if (!user?.id) return;
      const totalToClear =
        scopePath === WATCHLIST_SCOPE_MLB ? mlbRows.length : nhlRows.length;
      if (totalToClear <= 0) return;
      if (!bypassConfirm) {
        const ok = window.confirm(
          `Clear ${totalToClear} ${
            scopePath === WATCHLIST_SCOPE_MLB ? "MLB" : "NHL"
          } watchlist row(s)?`
        );
        if (!ok) return;
      }
      setUndoState({
        mlbRows: [...mlbRows],
        nhlRows: [...nhlRows],
        label:
          scopePath === WATCHLIST_SCOPE_MLB
            ? "Undo clear MLB watchlist"
            : "Undo clear NHL watchlist",
      });
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        setMlbRows([]);
        writeWatchlistScope(user.id, scopePath, []);
        return;
      }
      setNhlRows([]);
      writeWatchlistScope(user.id, scopePath, []);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const removeVisible = useCallback(
    (scopePath, visibleRows, bypassConfirm = false) => {
      if (!user?.id) return;
      const visibleIds = new Set((visibleRows || []).map((r) => String(r.id)));
      if (visibleIds.size === 0) return;
      if (!bypassConfirm) {
        const ok = window.confirm(
          `Remove ${visibleIds.size} visible ${
            scopePath === WATCHLIST_SCOPE_MLB ? "MLB" : "NHL"
          } row(s)?`
        );
        if (!ok) return;
      }
      setUndoState({
        mlbRows: [...mlbRows],
        nhlRows: [...nhlRows],
        label:
          scopePath === WATCHLIST_SCOPE_MLB
            ? "Undo remove visible MLB rows"
            : "Undo remove visible NHL rows",
      });
      if (scopePath === WATCHLIST_SCOPE_MLB) {
        const next = mlbRows.filter((r) => !visibleIds.has(String(r.id)));
        setMlbRows(next);
        writeWatchlistScope(user.id, scopePath, next);
        setCopyNotice(`Removed ${visibleIds.size} visible MLB watchlist row(s).`);
        window.setTimeout(() => setCopyNotice(""), 1500);
        return;
      }
      const next = nhlRows.filter((r) => !visibleIds.has(String(r.id)));
      setNhlRows(next);
      writeWatchlistScope(user.id, scopePath, next);
      setCopyNotice(`Removed ${visibleIds.size} visible NHL watchlist row(s).`);
      window.setTimeout(() => setCopyNotice(""), 1500);
    },
    [mlbRows, nhlRows, user?.id]
  );

  const total = useMemo(() => mlbRows.length + nhlRows.length, [mlbRows.length, nhlRows.length]);
  const q = useMemo(() => query.trim().toLowerCase(), [query]);

  function applyFilters(rows) {
    const filtered = !q
      ? rows
      : rows.filter((row) => {
          const haystack = [row?.player_name, row?.player_id, row?.team]
            .map((v) => String(v || "").toLowerCase())
            .join(" ");
          return haystack.includes(q);
        });
    const recencyRows =
      recencyFilter === "all"
        ? filtered
        : filtered.filter((row) => recencyBucket(row?.added_at) === recencyFilter);
    const out = [...recencyRows];
    if (sortBy === "name") {
      out.sort((a, b) =>
        String(a?.player_name || a?.player_id || "").localeCompare(
          String(b?.player_name || b?.player_id || "")
        )
      );
      return out;
    }
    if (sortBy === "oldest") {
      out.sort(
        (a, b) =>
          new Date(a?.added_at || 0).getTime() - new Date(b?.added_at || 0).getTime()
      );
      return out;
    }
    out.sort(
      (a, b) =>
        new Date(b?.added_at || 0).getTime() - new Date(a?.added_at || 0).getTime()
    );
    return out;
  }

  const visibleMlbRows = useMemo(() => applyFilters(mlbRows), [mlbRows, q, recencyFilter, sortBy]);
  const visibleNhlRows = useMemo(() => applyFilters(nhlRows), [nhlRows, q, recencyFilter, sortBy]);
  const visibleTotal = useMemo(
    () => visibleMlbRows.length + visibleNhlRows.length,
    [visibleMlbRows.length, visibleNhlRows.length]
  );
  const recencyCounts = useMemo(() => {
    const combined = [...mlbRows, ...nhlRows];
    let fresh = 0;
    let aging = 0;
    let stale = 0;
    for (const row of combined) {
      const bucket = recencyBucket(row?.added_at);
      if (bucket === "fresh") fresh += 1;
      else if (bucket === "aging") aging += 1;
      else if (bucket === "stale") stale += 1;
    }
    return { fresh, aging, stale };
  }, [mlbRows, nhlRows]);
  const mlbVisibleRecency = useMemo(() => {
    let fresh = 0;
    let aging = 0;
    let stale = 0;
    for (const row of visibleMlbRows) {
      const bucket = recencyBucket(row?.added_at);
      if (bucket === "fresh") fresh += 1;
      else if (bucket === "aging") aging += 1;
      else if (bucket === "stale") stale += 1;
    }
    return { fresh, aging, stale };
  }, [visibleMlbRows]);
  const nhlVisibleRecency = useMemo(() => {
    let fresh = 0;
    let aging = 0;
    let stale = 0;
    for (const row of visibleNhlRows) {
      const bucket = recencyBucket(row?.added_at);
      if (bucket === "fresh") fresh += 1;
      else if (bucket === "aging") aging += 1;
      else if (bucket === "stale") stale += 1;
    }
    return { fresh, aging, stale };
  }, [visibleNhlRows]);
  const staleVisibleMlbRows = useMemo(
    () => visibleMlbRows.filter((row) => recencyBucket(row?.added_at) === "stale"),
    [visibleMlbRows]
  );
  const staleVisibleNhlRows = useMemo(
    () => visibleNhlRows.filter((row) => recencyBucket(row?.added_at) === "stale"),
    [visibleNhlRows]
  );

  const pruneVisibleStale = useCallback((bypassConfirm = false) => {
    if (!user?.id) return;
    const pruneMlb = viewScope === "all" || viewScope === "mlb";
    const pruneNhl = viewScope === "all" || viewScope === "nhl";
    const mlbIds = pruneMlb ? new Set(staleVisibleMlbRows.map((r) => String(r.id))) : new Set();
    const nhlIds = pruneNhl ? new Set(staleVisibleNhlRows.map((r) => String(r.id))) : new Set();
    const total = mlbIds.size + nhlIds.size;
    if (total === 0) {
      setCopyNotice("No stale visible rows to prune.");
      window.setTimeout(() => setCopyNotice(""), 1500);
      return;
    }
    if (!bypassConfirm) {
      const ok = window.confirm(`Remove ${total} stale visible watchlist row(s)?`);
      if (!ok) return;
    }
    setUndoState({
      mlbRows: [...mlbRows],
      nhlRows: [...nhlRows],
      label: "Undo prune stale visible rows",
    });
    if (mlbIds.size > 0) {
      const nextMlb = mlbRows.filter((r) => !mlbIds.has(String(r.id)));
      setMlbRows(nextMlb);
      writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, nextMlb);
    }
    if (nhlIds.size > 0) {
      const nextNhl = nhlRows.filter((r) => !nhlIds.has(String(r.id)));
      setNhlRows(nextNhl);
      writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, nextNhl);
    }
    setCopyNotice(`Pruned ${total} stale visible row(s).`);
    window.setTimeout(() => setCopyNotice(""), 1600);
  }, [
    mlbRows,
    nhlRows,
    staleVisibleMlbRows,
    staleVisibleNhlRows,
    user?.id,
    viewScope,
  ]);
  const removeVisibleByRecency = useCallback(
    (bucket, bypassConfirm = false) => {
      if (!user?.id) return;
      const valid =
        bucket === "fresh" || bucket === "aging" || bucket === "stale";
      if (!valid) return;
      const pickRows = (rows) =>
        rows.filter((row) => recencyBucket(row?.added_at) === bucket);
      const pruneMlb = viewScope === "all" || viewScope === "mlb";
      const pruneNhl = viewScope === "all" || viewScope === "nhl";
      const mlbIds = pruneMlb
        ? new Set(pickRows(visibleMlbRows).map((r) => String(r.id)))
        : new Set();
      const nhlIds = pruneNhl
        ? new Set(pickRows(visibleNhlRows).map((r) => String(r.id)))
        : new Set();
      const total = mlbIds.size + nhlIds.size;
      if (total === 0) {
        setCopyNotice(`No ${bucket} visible rows to remove.`);
        window.setTimeout(() => setCopyNotice(""), 1500);
        return;
      }
      if (!bypassConfirm) {
        const ok = window.confirm(`Remove ${total} ${bucket} visible watchlist row(s)?`);
        if (!ok) return;
      }
      setUndoState({
        mlbRows: [...mlbRows],
        nhlRows: [...nhlRows],
        label: `Undo remove visible ${bucket} rows`,
      });
      if (mlbIds.size > 0) {
        const nextMlb = mlbRows.filter((r) => !mlbIds.has(String(r.id)));
        setMlbRows(nextMlb);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, nextMlb);
      }
      if (nhlIds.size > 0) {
        const nextNhl = nhlRows.filter((r) => !nhlIds.has(String(r.id)));
        setNhlRows(nextNhl);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, nextNhl);
      }
      setCopyNotice(`Removed ${total} ${bucket} visible row(s).`);
      window.setTimeout(() => setCopyNotice(""), 1600);
    },
    [mlbRows, nhlRows, user?.id, viewScope, visibleMlbRows, visibleNhlRows]
  );
  const staleVisibleCount = useMemo(() => {
    if (viewScope === "mlb") return staleVisibleMlbRows.length;
    if (viewScope === "nhl") return staleVisibleNhlRows.length;
    return staleVisibleMlbRows.length + staleVisibleNhlRows.length;
  }, [staleVisibleMlbRows.length, staleVisibleNhlRows.length, viewScope]);
  const visibleRecencyCounts = useMemo(() => {
    const rows =
      viewScope === "mlb"
        ? visibleMlbRows
        : viewScope === "nhl"
        ? visibleNhlRows
        : [...visibleMlbRows, ...visibleNhlRows];
    let fresh = 0;
    let aging = 0;
    let stale = 0;
    for (const row of rows) {
      const bucket = recencyBucket(row?.added_at);
      if (bucket === "fresh") fresh += 1;
      else if (bucket === "aging") aging += 1;
      else if (bucket === "stale") stale += 1;
    }
    return { fresh, aging, stale };
  }, [viewScope, visibleMlbRows, visibleNhlRows]);

  const handleCopyLink = useCallback(async (sport, row) => {
    const player = String(row?.player_name || row?.player_id || "").trim();
    if (!player) return;
    const path =
      sport === "mlb"
        ? `/props?player=${playerQuery(row)}`
        : `/nhl/predictions?player=${playerQuery(row)}`;
    try {
      const url = `${window.location.origin}${path}`;
      await navigator.clipboard.writeText(url);
      setCopyNotice(`Copied ${sport.toUpperCase()} link for ${player}`);
      window.setTimeout(() => setCopyNotice(""), 1500);
    } catch {
      setCopyNotice("Failed to copy link.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, []);

  const handleExportAll = useCallback(() => {
    try {
      const payload = {
        version: 1,
        exported_at: new Date().toISOString(),
        mlb: mlbRows.slice(0, 100),
        nhl: nhlRows.slice(0, 100),
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json;charset=utf-8;",
      });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = `watchlist_all_${String(user?.id || "member")}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(href);
      setCopyNotice("Exported watchlist bundle.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    } catch {
      setCopyNotice("Failed to export watchlist.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, [mlbRows, nhlRows, user?.id]);

  const handleExportVisible = useCallback(() => {
    try {
      const payload = {
        version: 1,
        exported_at: new Date().toISOString(),
        filter: {
          view_scope: viewScope,
          query: query.trim(),
          sort_by: sortBy,
          recency: recencyFilter,
        },
        mlb: (viewScope === "all" || viewScope === "mlb") ? visibleMlbRows.slice(0, 100) : [],
        nhl: (viewScope === "all" || viewScope === "nhl") ? visibleNhlRows.slice(0, 100) : [],
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json;charset=utf-8;",
      });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = `watchlist_visible_${String(user?.id || "member")}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(href);
      setCopyNotice("Exported visible watchlist rows.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    } catch {
      setCopyNotice("Failed to export visible watchlist rows.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, [query, recencyFilter, sortBy, user?.id, viewScope, visibleMlbRows, visibleNhlRows]);

  const handleCopyVisibleLinks = useCallback(async () => {
    try {
      const rows = [];
      if (viewScope === "all" || viewScope === "mlb") {
        for (const row of visibleMlbRows) {
          rows.push({
            sport: "MLB",
            name: String(row?.player_name || row?.player_id || "Unknown"),
            url: `${window.location.origin}/props?player=${playerQuery(row)}`,
          });
        }
      }
      if (viewScope === "all" || viewScope === "nhl") {
        for (const row of visibleNhlRows) {
          rows.push({
            sport: "NHL",
            name: String(row?.player_name || row?.player_id || "Unknown"),
            url: `${window.location.origin}/nhl/predictions?player=${playerQuery(row)}`,
          });
        }
      }
      const capped = rows.slice(0, 200);
      if (capped.length === 0) {
        setCopyNotice("No visible rows to copy.");
        window.setTimeout(() => setCopyNotice(""), 1500);
        return;
      }
      const payload = capped
        .map((r) => `${r.sport}\t${r.name}\t${r.url}`)
        .join("\n");
      await navigator.clipboard.writeText(payload);
      setCopyNotice(`Copied ${capped.length} visible link(s) to clipboard${rows.length > capped.length ? " (capped at 200)." : "."}`);
      window.setTimeout(() => setCopyNotice(""), 1800);
    } catch {
      setCopyNotice("Failed to copy visible links.");
      window.setTimeout(() => setCopyNotice(""), 1500);
    }
  }, [viewScope, visibleMlbRows, visibleNhlRows]);

  const handleImportClick = useCallback(() => {
    if (importInputRef.current) importInputRef.current.click();
  }, [importInputRef]);

  const handleImportAll = useCallback(
    async (e) => {
      const file = e?.target?.files?.[0];
      if (!file) return;
      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        const incomingMlb = normalizeRows(parsed?.mlb);
        const incomingNhl = normalizeRows(parsed?.nhl);
        if (!user?.id) return;
        const nextMlb =
          importMode === "merge"
            ? normalizeRows([...mlbRows, ...incomingMlb])
            : incomingMlb;
        const nextNhl =
          importMode === "merge"
            ? normalizeRows([...nhlRows, ...incomingNhl])
            : incomingNhl;
        setMlbRows(nextMlb);
        setNhlRows(nextNhl);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, nextMlb);
        writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, nextNhl);
        setCopyNotice(
          `Imported watchlist bundle (${importMode === "merge" ? "merge" : "replace"}; MLB ${nextMlb.length}, NHL ${nextNhl.length}).`
        );
        window.setTimeout(() => setCopyNotice(""), 1800);
      } catch {
        setCopyNotice("Failed to import watchlist bundle.");
        window.setTimeout(() => setCopyNotice(""), 1800);
      } finally {
        if (e?.target) e.target.value = "";
      }
    },
    [importMode, mlbRows, nhlRows, user?.id]
  );

  return (
    <div className="min-h-screen pp-page px-4 py-8">
      <div className="max-w-5xl mx-auto">
        <div className="pp-card p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h1 className="text-2xl font-semibold text-slate-900">Watchlist</h1>
              <p className="text-sm text-slate-600 mt-1">
                Saved players for quick research access. Total saved: {total}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <PrefetchLink to="/props" className="pp-btn pp-btn-secondary pp-btn-sm">
                Open MLB
              </PrefetchLink>
              <PrefetchLink to="/nhl/predictions" className="pp-btn pp-btn-secondary pp-btn-sm">
                Open NHL
              </PrefetchLink>
            </div>
          </div>

          <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
            {copyNotice ? (
              <div className="lg:col-span-2 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
                {copyNotice}
              </div>
            ) : null}
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap gap-2">
                <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-700 px-2 py-1 text-xs">
                  Total <strong>{total}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
                  Visible <strong>{visibleTotal}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-1 text-xs">
                  Fresh in view <strong>{visibleRecencyCounts.fresh}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 text-amber-700 px-2 py-1 text-xs">
                  Aging in view <strong>{visibleRecencyCounts.aging}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-1 text-xs">
                  Stale in view <strong>{visibleRecencyCounts.stale}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 text-blue-700 px-2 py-1 text-xs">
                  MLB <strong>{visibleMlbRows.length}/{mlbRows.length}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-indigo-100 text-indigo-700 px-2 py-1 text-xs">
                  NHL <strong>{visibleNhlRows.length}/{nhlRows.length}</strong>
                </span>
              </div>
            </section>
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs text-slate-500">View</span>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "all" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("all")}
                >
                  All
                </button>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "mlb" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("mlb")}
                >
                  MLB
                </button>
                <button
                  type="button"
                  className={`pp-btn pp-btn-sm ${viewScope === "nhl" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                  onClick={() => setViewScope("nhl")}
                >
                  NHL
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={() => {
                    setQuery("");
                    setSortBy("newest");
                    setViewScope("all");
                    setRecencyFilter("all");
                    setImportMode("replace");
                  }}
                >
                  Reset View
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleExportAll}
                >
                  Export All
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleExportVisible}
                  disabled={visibleTotal === 0}
                >
                  Export Visible
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleImportClick}
                >
                  Import All
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={handleCopyVisibleLinks}
                  disabled={visibleTotal === 0}
                >
                  Copy Visible Links
                </button>
                {undoState ? (
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={() => {
                      if (!user?.id) return;
                      const nextMlb = Array.isArray(undoState.mlbRows) ? undoState.mlbRows : [];
                      const nextNhl = Array.isArray(undoState.nhlRows) ? undoState.nhlRows : [];
                      setMlbRows(nextMlb);
                      setNhlRows(nextNhl);
                      writeWatchlistScope(user.id, WATCHLIST_SCOPE_MLB, nextMlb);
                      writeWatchlistScope(user.id, WATCHLIST_SCOPE_NHL, nextNhl);
                      setUndoState(null);
                      setCopyNotice("Undo applied.");
                      window.setTimeout(() => setCopyNotice(""), 1500);
                    }}
                  >
                    {undoState.label || "Undo last bulk change"}
                  </button>
                ) : null}
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={(e) => pruneVisibleStale(e.shiftKey)}
                  disabled={staleVisibleCount === 0}
                  title="Shift+Click skips confirm"
                >
                  {`Prune stale visible (${staleVisibleCount})`}
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={(e) => removeVisibleByRecency("aging", e.shiftKey)}
                  disabled={visibleRecencyCounts.aging === 0}
                  title="Shift+Click skips confirm"
                >
                  {`Remove visible aging (${visibleRecencyCounts.aging})`}
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={(e) => removeVisibleByRecency("fresh", e.shiftKey)}
                  disabled={visibleRecencyCounts.fresh === 0}
                  title="Shift+Click skips confirm"
                >
                  {`Remove visible fresh (${visibleRecencyCounts.fresh})`}
                </button>
                <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-1 text-xs">
                  Stale in view <strong>{staleVisibleCount}</strong>
                </span>
                <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 text-slate-600 px-2 py-1 text-xs">
                  Tip: <strong>Shift+Click</strong> skips confirm
                </span>
                <label className="inline-flex items-center gap-2 text-xs text-slate-600">
                  Import mode
                  <select
                    value={importMode}
                    onChange={(e) => setImportMode(e.target.value)}
                    className="pp-chip px-2 py-1 text-xs text-slate-800"
                  >
                    <option value="replace">Replace</option>
                    <option value="merge">Merge</option>
                  </select>
                </label>
                <input
                  ref={(el) => {
                    importInputRef.current = el;
                  }}
                  type="file"
                  accept="application/json,.json"
                  className="hidden"
                  onChange={handleImportAll}
                />
              </div>
            </section>
            <section className="lg:col-span-2 rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
                <div className="md:col-span-2">
                  <div className="text-xs text-slate-500 mb-1">Search watchlist</div>
                  <input
                    ref={searchInputRef}
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Player, team, or player id... (/ focus, Esc clear, 1-4 recency)"
                    className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                  />
                </div>
                <div>
                  <div className="text-xs text-slate-500 mb-1">Sort by</div>
                  <select
                    value={sortBy}
                    onChange={(e) => setSortBy(e.target.value)}
                    className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                  >
                    <option value="newest">Newest added</option>
                    <option value="oldest">Oldest added</option>
                    <option value="name">Player name</option>
                  </select>
                </div>
                <div className="md:col-span-3">
                  <button
                    type="button"
                    className="pp-btn pp-btn-secondary pp-btn-sm"
                    onClick={() => setQuery("")}
                    disabled={!query.trim()}
                  >
                    Clear Search
                  </button>
                </div>
                <div className="md:col-span-3 flex flex-wrap items-center gap-2">
                  <span className="text-xs text-slate-500">Recency</span>
                  <button
                    type="button"
                    className={`pp-btn pp-btn-sm ${recencyFilter === "all" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                    onClick={() => setRecencyFilter("all")}
                  >
                    All
                  </button>
                  <button
                    type="button"
                    className={`pp-btn pp-btn-sm ${recencyFilter === "fresh" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                    onClick={() => setRecencyFilter("fresh")}
                  >
                    Fresh ({recencyCounts.fresh})
                  </button>
                  <button
                    type="button"
                    className={`pp-btn pp-btn-sm ${recencyFilter === "aging" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                    onClick={() => setRecencyFilter("aging")}
                  >
                    Aging ({recencyCounts.aging})
                  </button>
                  <button
                    type="button"
                    className={`pp-btn pp-btn-sm ${recencyFilter === "stale" ? "pp-btn-primary" : "pp-btn-secondary"}`}
                    onClick={() => setRecencyFilter("stale")}
                  >
                    Stale ({recencyCounts.stale})
                  </button>
                </div>
              </div>
            </section>
            {(viewScope === "all" || viewScope === "mlb") ? (
            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <div>
                  <h2 className="text-sm font-semibold text-slate-900">
                    MLB Watchlist ({visibleMlbRows.length}/{mlbRows.length})
                  </h2>
                  <div className="mt-1 flex flex-wrap gap-2">
                    <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-0.5 text-[11px]">
                      Fresh <strong>{mlbVisibleRecency.fresh}</strong>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 text-amber-700 px-2 py-0.5 text-[11px]">
                      Aging <strong>{mlbVisibleRecency.aging}</strong>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-0.5 text-[11px]">
                      Stale <strong>{mlbVisibleRecency.stale}</strong>
                    </span>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                    disabled={visibleMlbRows.length === 0}
                    onClick={(e) => removeVisible(WATCHLIST_SCOPE_MLB, visibleMlbRows, e.shiftKey)}
                    title="Shift+Click skips confirm"
                  >
                    Remove Visible
                  </button>
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm"
                    disabled={mlbRows.length === 0}
                    onClick={(e) => clearScope(WATCHLIST_SCOPE_MLB, e.shiftKey)}
                    title="Shift+Click skips confirm"
                  >
                    Clear
                  </button>
                </div>
              </div>
              {visibleMlbRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">
                  {mlbRows.length === 0 ? "No MLB players saved yet." : "No MLB matches for current search."}
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {visibleMlbRows.map((row) => (
                    <div
                      key={String(row.id)}
                      className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm flex items-center justify-between gap-2"
                    >
                      {(() => {
                        const recency = addedRecency(row.added_at);
                        const recencyClass =
                          recency.tone === "fresh"
                            ? "bg-emerald-100 text-emerald-700"
                            : recency.tone === "warn"
                            ? "bg-amber-100 text-amber-700"
                            : recency.tone === "stale"
                            ? "bg-rose-100 text-rose-700"
                            : "bg-slate-100 text-slate-600";
                        return (
                      <div>
                        <PrefetchLink
                          to={`/props?player=${playerQuery(row)}`}
                          className="font-medium text-slate-900 underline"
                        >
                          {row.player_name || row.player_id || "Unknown"}
                        </PrefetchLink>
                        <div className="text-xs text-slate-500">{row.team || "-"}</div>
                        <div className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium mt-1 ${recencyClass}`}>
                          {recency.label}
                        </div>
                        <div className="text-xs text-slate-400">Added {formatAddedAt(row.added_at)}</div>
                      </div>
                        );
                      })()}
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_MLB, row.id)}
                      >
                        Remove
                      </button>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm"
                        onClick={() => handleCopyLink("mlb", row)}
                      >
                        Copy Link
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>
            ) : null}

            {(viewScope === "all" || viewScope === "nhl") ? (
            <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-2">
                <div>
                  <h2 className="text-sm font-semibold text-slate-900">
                    NHL Watchlist ({visibleNhlRows.length}/{nhlRows.length})
                  </h2>
                  <div className="mt-1 flex flex-wrap gap-2">
                    <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-0.5 text-[11px]">
                      Fresh <strong>{nhlVisibleRecency.fresh}</strong>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 text-amber-700 px-2 py-0.5 text-[11px]">
                      Aging <strong>{nhlVisibleRecency.aging}</strong>
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-0.5 text-[11px]">
                      Stale <strong>{nhlVisibleRecency.stale}</strong>
                    </span>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                    disabled={visibleNhlRows.length === 0}
                    onClick={(e) => removeVisible(WATCHLIST_SCOPE_NHL, visibleNhlRows, e.shiftKey)}
                    title="Shift+Click skips confirm"
                  >
                    Remove Visible
                  </button>
                  <button
                    type="button"
                    className="pp-btn pp-btn-ghost pp-btn-sm"
                    disabled={nhlRows.length === 0}
                    onClick={(e) => clearScope(WATCHLIST_SCOPE_NHL, e.shiftKey)}
                    title="Shift+Click skips confirm"
                  >
                    Clear
                  </button>
                </div>
              </div>
              {visibleNhlRows.length === 0 ? (
                <div className="text-xs text-slate-500 mt-2">
                  {nhlRows.length === 0 ? "No NHL players saved yet." : "No NHL matches for current search."}
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {visibleNhlRows.map((row) => (
                    <div
                      key={String(row.id)}
                      className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm flex items-center justify-between gap-2"
                    >
                      {(() => {
                        const recency = addedRecency(row.added_at);
                        const recencyClass =
                          recency.tone === "fresh"
                            ? "bg-emerald-100 text-emerald-700"
                            : recency.tone === "warn"
                            ? "bg-amber-100 text-amber-700"
                            : recency.tone === "stale"
                            ? "bg-rose-100 text-rose-700"
                            : "bg-slate-100 text-slate-600";
                        return (
                      <div>
                        <PrefetchLink
                          to={`/nhl/predictions?player=${playerQuery(row)}`}
                          className="font-medium text-slate-900 underline"
                        >
                          {row.player_name || row.player_id || "Unknown"}
                        </PrefetchLink>
                        <div className="text-xs text-slate-500">{row.team || "-"}</div>
                        <div className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium mt-1 ${recencyClass}`}>
                          {recency.label}
                        </div>
                        <div className="text-xs text-slate-400">Added {formatAddedAt(row.added_at)}</div>
                      </div>
                        );
                      })()}
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                        onClick={() => removeRow(WATCHLIST_SCOPE_NHL, row.id)}
                      >
                        Remove
                      </button>
                      <button
                        type="button"
                        className="pp-btn pp-btn-ghost pp-btn-sm"
                        onClick={() => handleCopyLink("nhl", row)}
                      >
                        Copy Link
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </section>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
