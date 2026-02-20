import React from "react";

import PropTracker from "../../PropTracker.jsx";

export default function CalendarCard({ selectedDate, setSelectedDate }) {
  return (
    <section className="pp-card p-4">
      <h3 className="text-sm font-semibold text-slate-900 mb-3">Calendar View</h3>
      <PropTracker selectedDate={selectedDate} setSelectedDate={setSelectedDate} />
    </section>
  );
}
