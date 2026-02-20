import React, { useMemo } from "react";

import PropTracker from "../../PropTracker.jsx";
import {
  coerceISODate,
  formatISODateLongET,
  shiftISODateByDaysET,
  todayET,
} from "../../../shared/timeUtils.js";

export default function PredictionCalendar({
  selectedDate,
  setSelectedDate,
  title = "Calendar",
  subtitle = "",
  showTracker = false,
  className = "",
}) {
  const activeDate = useMemo(
    () => coerceISODate(selectedDate, todayET()),
    [selectedDate]
  );

  function updateDate(nextDate) {
    setSelectedDate?.(coerceISODate(nextDate, activeDate));
  }

  return (
    <section className={`pp-card p-4 space-y-4 ${className}`.trim()}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
          {subtitle ? <p className="text-xs text-slate-500 mt-1">{subtitle}</p> : null}
        </div>
        <div className="text-xs text-slate-500">{formatISODateLongET(activeDate)}</div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={() => updateDate(shiftISODateByDaysET(activeDate, -1))}
        >
          Prev
        </button>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={() => updateDate(todayET())}
        >
          Today
        </button>
        <button
          type="button"
          className="pp-btn pp-btn-secondary pp-btn-sm"
          onClick={() => updateDate(shiftISODateByDaysET(activeDate, 1))}
        >
          Next
        </button>
        <input
          type="date"
          className="pp-chip px-3 py-2 text-sm text-slate-800"
          value={activeDate}
          onChange={(e) => updateDate(e.target.value)}
        />
      </div>

      {showTracker ? (
        <PropTracker selectedDate={activeDate} setSelectedDate={setSelectedDate} />
      ) : null}
    </section>
  );
}
