import React from "react";

function pct(v) {
  if (v == null || Number.isNaN(Number(v))) return "-";
  return `${(Number(v) * 100).toFixed(1)}%`;
}

function delta(modelProbability, marketProbability) {
  if (modelProbability == null || marketProbability == null) return "-";
  const d = Number(modelProbability) - Number(marketProbability);
  const sign = d > 0 ? "+" : "";
  return `${sign}${(d * 100).toFixed(1)} pts`;
}

export default function ModelVsMarketCard({
  title,
  modelProbability,
  marketProbability,
  lineLabel,
  sourceLabel,
  updatedLabel,
  confidenceLabel,
}) {
  return (
    <section className="pp-card p-4">
      <div className="flex items-start justify-between gap-3 mb-3">
        <div>
          <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
          {lineLabel ? <p className="text-xs text-slate-500 mt-1">{lineLabel}</p> : null}
        </div>
        {confidenceLabel ? (
          <span className="pp-chip text-xs text-slate-700 px-2 py-1 rounded-full">
            Confidence: {confidenceLabel}
          </span>
        ) : null}
      </div>

      <div className="grid grid-cols-3 gap-3 text-sm mb-3">
        <div className="pp-chip p-2">
          <div className="text-xs text-slate-500">Model</div>
          <div className="font-semibold text-slate-900">{pct(modelProbability)}</div>
        </div>
        <div className="pp-chip p-2">
          <div className="text-xs text-slate-500">Market</div>
          <div className="font-semibold text-slate-900">{pct(marketProbability)}</div>
        </div>
        <div className="pp-chip p-2">
          <div className="text-xs text-slate-500">Delta</div>
          <div className="font-semibold text-slate-900">{delta(modelProbability, marketProbability)}</div>
        </div>
      </div>

      <div className="text-xs text-slate-500 flex flex-wrap gap-3">
        <span>Source: {sourceLabel || "Model only"}</span>
        <span>Last updated: {updatedLabel || "-"}</span>
      </div>
    </section>
  );
}
