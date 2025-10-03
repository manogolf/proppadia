import React, { useState } from "react";
import PropTracker from "../components/PropTracker.jsx"; // 📅 Calendar
import PlayerPropsTable from "../components/PlayerPropsTable.jsx"; // 📊 Table
// import PlayerPropForm from "../components/PlayerPropForm.jsx"; // 📝 Form
import PlayerPropFormV2 from "../components/PlayerPropFormv2.jsx"; // V2
import { useAuth } from "../context/AuthContext.jsx";
import { todayET } from "../shared/timeUtils.js";

export default function PropsDashboard() {
  const { user } = useAuth();
  const [selectedDate, setSelectedDate] = useState(todayET()); // keep one source of truth
  const LoginGate = ({ children }) =>
    user ? (
      children
    ) : (
      <div className="text-center text-gray-500">
        🔒 You must{" "}
        <a href="/login" className="underline text-blue-600">
          log in
        </a>{" "}
        to add props.
      </div>
    );

  // return (
  // <div className="min-h-screen bg-gray-100 px-4 py-6 space-y-6">

  /* V1 card */

  /* <section className="bg-blue-100 p-4 rounded-xl shadow-md overflow-x-auto">
        <h2 className="text-xl font-semibold mb-2">Player Props (Classic)</h2>
        <LoginGate>
          <PlayerPropForm />
        </LoginGate>
      </section> */

  /* V2 card */

  return (
    <div className="min-h-screen bg-gray-100 px-4 py-6">
      <div className="bg-blue-100 p-4 rounded-xl shadow-md overflow-x-auto">
        {user ? (
          <PlayerPropFormV2 />
        ) : (
          <div className="text-center text-gray-500">
            🔒 You must{" "}
            <a href="/login" className="underline text-blue-600">
              log in
            </a>{" "}
            to add props.
          </div>
        )}
      </div>

      {/* Today’s (or selected) table */}
      <div className="bg-gray-100 p-4 rounded-xl shadow">
        <PlayerPropsTable selectedDate={selectedDate} />
      </div>

      {/* Calendar + “props for selected date” */}
      <div className="bg-gray-100 p-4 rounded-xl shadow">
        <PropTracker
          selectedDate={selectedDate}
          setSelectedDate={setSelectedDate}
        />
      </div>
    </div>
  );
}
