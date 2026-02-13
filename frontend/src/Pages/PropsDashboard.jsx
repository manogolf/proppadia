// frontend/src/Pages/PropsDashboard.jsx
import React, { useState } from "react";
import { Link } from "react-router-dom";
import PropTracker from "../components/PropTracker.jsx"; // 📅 Calendar
import PlayerPropsTable from "../components/PlayerPropsTable.jsx"; // 📊 Table
import PlayerPropFormV2 from "../components/PlayerPropFormv2.jsx"; // 📝 V2 Form (default export)
import { useAuth } from "../context/AuthContext.jsx";
import { todayET } from "../shared/timeUtils.js";

export default function PropsDashboard() {
  const { user } = useAuth();
  const [selectedDate, setSelectedDate] = useState(todayET()); // single source of truth
  const [tableRefreshNonce, setTableRefreshNonce] = useState(0);
  const [lastSaveEvent, setLastSaveEvent] = useState(null);

  const LoginGate = ({ children }) =>
    user ? (
      children
    ) : (
      <div className="text-center text-slate-600">
        🔒 You must{" "}
        <Link to="/login" className="underline text-slate-700">
          log in
        </Link>{" "}
        to add props.
      </div>
    );

  return (
    <div className="min-h-screen pp-page px-4 py-6 space-y-6">
      {/* V2: Add Props */}
      <section className="pp-card p-4 overflow-x-auto">
        <h2 className="text-xl font-semibold mb-3 text-slate-900">Add Player Props</h2>
        <LoginGate>
          <PlayerPropFormV2
            onSaved={(evt) => {
              if (evt?.gameDate) setSelectedDate(evt.gameDate);
              setLastSaveEvent(evt || null);
              setTableRefreshNonce((n) => n + 1);
            }}
          />
        </LoginGate>
      </section>

      {/* Today’s (or selected) table */}
      <section className="pp-card p-4">
        <PlayerPropsTable
          selectedDate={selectedDate}
          refreshNonce={tableRefreshNonce}
          lastSaveEvent={lastSaveEvent}
        />
      </section>

      {/* Calendar + “props for selected date” */}
      <section className="pp-card p-4">
        <PropTracker
          selectedDate={selectedDate}
          setSelectedDate={setSelectedDate}
        />
      </section>
    </div>
  );
}
