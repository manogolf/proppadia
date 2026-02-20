import React from "react";

import { PrefetchLink } from "../../navigation/PrefetchLink.jsx";

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

function recencyClassName(tone) {
  if (tone === "fresh") return "bg-emerald-100 text-emerald-700";
  if (tone === "warn") return "bg-amber-100 text-amber-700";
  if (tone === "stale") return "bg-rose-100 text-rose-700";
  return "bg-slate-100 text-slate-600";
}

function propsPathForSport(sport, row) {
  const query = encodeURIComponent(String(row?.player_name || row?.player_id || "").trim());
  if (sport === "nhl") return `/nhl/predictions?player=${query}`;
  return `/props?player=${query}`;
}

export default function SavedPropsTable({
  sport,
  rows,
  totalRows,
  recencyCounts,
  onRemoveVisible,
  onClear,
  onRemoveRow,
  onCopyLink,
}) {
  const sportLabel = sport === "nhl" ? "NHL" : "MLB";

  return (
    <section className="rounded-xl border border-slate-200 bg-slate-50 p-4">
      <div className="flex items-center justify-between gap-2">
        <div>
          <h2 className="text-sm font-semibold text-slate-900">
            {sportLabel} Watchlist ({rows.length}/{totalRows})
          </h2>
          <div className="mt-1 flex flex-wrap gap-2">
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-700 px-2 py-0.5 text-[11px]">
              Fresh <strong>{recencyCounts.fresh}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 text-amber-700 px-2 py-0.5 text-[11px]">
              Aging <strong>{recencyCounts.aging}</strong>
            </span>
            <span className="inline-flex items-center gap-1 rounded-full bg-rose-100 text-rose-700 px-2 py-0.5 text-[11px]">
              Stale <strong>{recencyCounts.stale}</strong>
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
            disabled={rows.length === 0}
            onClick={(e) => onRemoveVisible?.(Boolean(e.shiftKey))}
            title="Shift+Click skips confirm"
          >
            Remove Visible
          </button>
          <button
            type="button"
            className="pp-btn pp-btn-ghost pp-btn-sm"
            disabled={totalRows === 0}
            onClick={(e) => onClear?.(Boolean(e.shiftKey))}
            title="Shift+Click skips confirm"
          >
            Clear
          </button>
        </div>
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-slate-500 mt-2">
          {totalRows === 0
            ? `No ${sportLabel} players saved yet.`
            : `No ${sportLabel} matches for current search.`}
        </div>
      ) : (
        <div className="mt-3 space-y-2">
          {rows.map((row) => {
            const recency = addedRecency(row.added_at);
            return (
              <div
                key={String(row.id)}
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm flex items-center justify-between gap-2"
              >
                <div>
                  <PrefetchLink
                    to={propsPathForSport(sport, row)}
                    className="font-medium text-slate-900 underline"
                  >
                    {row.player_name || row.player_id || "Unknown"}
                  </PrefetchLink>
                  <div className="text-xs text-slate-500">{row.team || "-"}</div>
                  <div
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium mt-1 ${recencyClassName(recency.tone)}`}
                  >
                    {recency.label}
                  </div>
                  <div className="text-xs text-slate-400">Added {formatAddedAt(row.added_at)}</div>
                </div>
                <button
                  type="button"
                  className="pp-btn pp-btn-ghost pp-btn-sm text-rose-700"
                  onClick={() => onRemoveRow?.(row.id)}
                >
                  Remove
                </button>
                <button
                  type="button"
                  className="pp-btn pp-btn-ghost pp-btn-sm"
                  onClick={() => onCopyLink?.(row)}
                >
                  Copy Link
                </button>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}
