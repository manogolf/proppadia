import React, { useEffect, useState } from "react";
import { getBaseURL } from "../shared/getBaseURL.js";

function fmt(x, digits = 3) {
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (Number.isNaN(n)) return "—";
  return n.toFixed(digits);
}

function InfoTip({ text }) {
  return (
    <span className="relative inline-block align-middle ml-1 group">
      <span
        tabIndex={0}
        className="inline-flex items-center justify-center w-5 h-5 rounded-full
                     text-slate-500 font-bold cursor-help select-none
                     focus:outline-none focus:ring-2 focus:ring-slate-300"
        aria-label={text}
      >
        ⓘ
      </span>

      <div
        className="pointer-events-none absolute z-50 left-1/2 -translate-x-1/2 mt-2
                     min-w-[220px] max-w-[360px] whitespace-normal break-words
                     rounded-lg bg-white border border-slate-200 shadow-lg p-3 text-xs text-slate-700
                     opacity-0 translate-y-1
                     transition-opacity transition-transform duration-150
                     group-hover:opacity-100 group-hover:translate-y-0
                     group-focus-within:opacity-100 group-focus-within:translate-y-0"
      >
        {text}
      </div>
    </span>
  );
}

export default function SogEvalCard() {
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let cancelled = false;

    fetch(`${getBaseURL()}/nhl/site/data/sog_eval.json`, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((j) => {
        if (!cancelled) setData(j);
      })
      .catch((e) => {
        console.error("[sog_eval] fetch error", e);
        if (!cancelled) setErr(String(e));
      });

    return () => {
      cancelled = true;
    };
  }, []);

  // non-blocking: if it errors, show a small warning card (doesn't break page)
  if (err) {
    return (
      <div className="pp-card p-4 text-rose-700">
        <div className="font-semibold mb-1 text-slate-900">Model Quality (SOG)</div>
        <div className="text-sm">SOG eval fetch error: {String(err)}</div>
      </div>
    );
  }

  // non-blocking: show a small placeholder so you *know* it mounted
  if (!data?.rows?.length) {
    return (
      <div className="pp-card p-4 text-slate-700">
        <div className="font-semibold mb-1 text-slate-900">Model Quality (SOG)</div>
        <div className="text-sm">Loading evaluation…</div>
      </div>
    );
  }

  const rowsRaw = Array.isArray(data) ? data : data?.rows || [];
  if (!rowsRaw.length) return null;

  const rows = [...rowsRaw].sort((a, b) => Number(a.line) - Number(b.line));
  const latest = rows[0]?.latest_eval_date;
  const publishedAt = data?.published_at_et || null;

  const headers = [
    { key: "line", label: "Line" },
    {
      key: "brier",
      label: "Brier",
      tip: "Mean squared error for probabilities. Lower is better (0 is perfect).",
    },
    {
      key: "logloss",
      label: "Logloss",
      tip: "Penalty for wrong confidence. Lower is better; confident mistakes hurt most.",
    },
    {
      key: "auc",
      label: "AUC",
      tip: "Ranking quality: 0.5=random, 1.0=perfect. Higher is better.",
    },
    {
      key: "truth_cov",
      label: "% Used",
      tip: "Percent of predictions that had completed-game actuals available and were included in evaluation.",
    },
    { key: "games", label: "Games" },
    {
      key: "sample",
      label: "Sample",
      tip: "Low = fewer than 5 games on the evaluated date.",
    },
  ];

  return (
    <section className="pp-card p-4 overflow-x-auto">
      <div className="flex items-baseline justify-between gap-3 mb-3">
        <h2 className="text-lg font-semibold text-slate-900">
          Model Quality (SOG)
        </h2>

        <div className="text-xs text-slate-600 text-right">
          <div>
            Updated: <span className="font-mono">{publishedAt ?? "—"}</span>
          </div>
          <div>
            Last completed games evaluated:{" "}
            <span className="font-mono">{latest ?? "—"}</span>
          </div>
        </div>
      </div>

      <table className="min-w-full text-sm text-slate-800">
        <thead className="bg-slate-100">
          <tr>
            {headers.map((h) => (
              <th key={h.key} className="px-3 py-2 text-left whitespace-nowrap">
                {h.label}
                {h.tip ? <InfoTip text={h.tip} /> : null}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {rows.map((r) => (
            <tr
              key={`${r.latest_eval_date}-${r.line}`}
              className="border-t border-slate-200 hover:bg-slate-50"
            >
              <td className="px-3 py-2 font-mono">{fmt(r.line, 1)}</td>
              <td className="px-3 py-2">{fmt(r.brier, 4)}</td>
              <td className="px-3 py-2">{fmt(r.logloss, 4)}</td>
              <td className="px-3 py-2">{fmt(r.auc, 3)}</td>
              <td className="px-3 py-2">{fmt(r.truth_coverage, 3)}</td>
              <td className="px-3 py-2">{r.games_on_date ?? "—"}</td>
              <td className="px-3 py-2">
                <span
                  className={`px-2 py-1 rounded-full text-xs font-semibold ${
                    r.is_low_sample
                      ? "bg-amber-100 text-amber-800"
                      : "bg-green-100 text-green-700"
                  }`}
                  title={r.is_low_sample ? "Low sample day (<5 games)" : "OK"}
                >
                  {r.is_low_sample ? "Low" : "OK"}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* optional: 7-day rollup */}
      <div className="mt-3 text-xs text-slate-700">
        <span className="font-semibold">7d (weighted):</span>{" "}
        {rows.map((r) => (
          <span key={`7d-${r.line}`} className="mr-3 whitespace-nowrap">
            {fmt(r.line, 1)}: brier {fmt(r.brier_w, 4)}, logloss{" "}
            {fmt(r.logloss_w, 4)} ({r.days_included ?? 0}d)
          </span>
        ))}
      </div>
    </section>
  );
}
