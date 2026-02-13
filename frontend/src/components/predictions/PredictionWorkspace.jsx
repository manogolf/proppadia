import React from "react";

export default function PredictionWorkspace({
  sportLabel,
  title,
  subtitle,
  dateLabel,
  modes = [],
  activeMode,
  onModeChange,
  controls = null,
  children,
}) {
  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-6xl mx-auto px-4 py-4">
        <div className="pp-card pp-reveal">
          <div className="px-5 py-4 border-b border-slate-200">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <div className="text-xs tracking-wide uppercase text-slate-500 mb-1">
                  {sportLabel}
                </div>
                <h1 className="text-2xl font-semibold text-slate-900">{title}</h1>
                {subtitle ? <p className="text-sm text-slate-600 mt-1">{subtitle}</p> : null}
              </div>
              {dateLabel ? <div className="text-sm text-slate-500">{dateLabel}</div> : null}
            </div>

            {modes.length > 0 ? (
              <div className="mt-4 flex flex-wrap gap-2">
                {modes.map((m) => {
                  const active = m.id === activeMode;
                  return (
                    <button
                      key={m.id}
                      type="button"
                      onClick={() => onModeChange?.(m.id)}
                      aria-pressed={active}
                      className={[
                        "pp-btn pp-btn-md transition text-left",
                        active
                          ? "pp-btn-primary"
                          : "pp-btn-secondary",
                      ].join(" ")}
                    >
                      <div className="font-medium">{m.label}</div>
                      {m.hint ? <div className="text-xs opacity-80">{m.hint}</div> : null}
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {controls ? <div className="px-5 py-4 border-b border-slate-200">{controls}</div> : null}

          <div className="px-5 py-5">{children}</div>
        </div>
      </div>
    </div>
  );
}
