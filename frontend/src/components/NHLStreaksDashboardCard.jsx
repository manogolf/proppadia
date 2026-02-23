import React from "react";

export default function NHLStreaksDashboardCard() {
  return (
    <section className="pp-card pp-reveal-soft p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-medium text-slate-900">
            NHL Streaks Dashboard
          </h2>
          <p className="text-sm text-slate-600 mt-1">
            Coming soon. This section will return when NHL streaks are wired back
            into the daily pipeline.
          </p>
        </div>
        <div className="pp-chip px-3 py-1 text-xs font-semibold text-slate-600">
          Pending
        </div>
      </div>
    </section>
  );
}
