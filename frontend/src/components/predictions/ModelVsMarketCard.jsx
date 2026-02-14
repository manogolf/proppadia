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
  actions = null,
  badges = [],
}) {
  const badgeToneClass = (tone) => {
    if (tone === "success") return "bg-emerald-100 text-emerald-700";
    if (tone === "muted") return "bg-slate-100 text-slate-600";
    if (tone === "warn") return "bg-amber-100 text-amber-700";
    return "bg-blue-100 text-blue-700";
  };

  return (
    <section className="pp-card p-4">
      <div className="flex items-start justify-between gap-3 mb-3">
        <div>
          <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
          {lineLabel ? <p className="text-xs text-slate-500 mt-1">{lineLabel}</p> : null}
          {badges.length ? (
            <div className="mt-2 flex flex-wrap gap-2">
              {badges.map((badge) => (
                <span
                  key={`${badge.label}-${badge.tone || "default"}`}
                  className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${badgeToneClass(
                    badge.tone
                  )}`}
                >
                  {badge.label}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        {confidenceLabel ? (
          <span className="pp-chip text-xs text-slate-700 px-2 py-1 rounded-full">
            Confidence: {confidenceLabel}
          </span>
        ) : null}
      </div>
      {actions ? <div className="mb-3">{actions}</div> : null}

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
