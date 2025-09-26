// /src/components/PlayerPropsTable.js
import React, { useEffect, useMemo, useState } from "react";
import { format, isValid } from "date-fns";
import { supabase } from "../utils/supabaseFrontend.js";
import { todayET } from "../shared/timeUtils.js";
import { getPropDisplayLabel } from "../../shared/propUtils.js";
// (Optional) if you want "only my props":
// import { useAuth } from "../context/AuthContext.js";

const statusColor = {
  win: "bg-green-100 text-green-700",
  loss: "bg-red-100 text-red-700",
  push: "bg-blue-100 text-blue-700",
  resolved: "bg-gray-200 text-gray-600",
  live: "bg-yellow-100 text-yellow-800",
  pending: "bg-gray-100 text-gray-500 italic",
  dnp: "bg-zinc-200 text-zinc-700 italic",
  expired: "bg-gray-300 text-gray-500 italic",
};

export default function PlayerPropsTable({ selectedDate, onlyMine = false }) {
  const [rows, setRows] = useState([]);
  const [sort, setSort] = useState({ key: "game_date", dir: "asc" });
  // const { user } = useAuth();

  const day = useMemo(() => {
    if (!selectedDate) return todayET();
    if (typeof selectedDate === "string") return selectedDate;
    return isValid(selectedDate)
      ? format(selectedDate, "yyyy-MM-dd")
      : todayET();
  }, [selectedDate]);

  useEffect(() => {
    let q = supabase
      .from("player_props")
      .select("*")
      .eq("game_date", day)
      .neq("status", "expired")
      .order("created_at", { ascending: false });

    // If you want to show only the current user’s props, uncomment this:
    // if (onlyMine && user?.id) q = q.eq("user_id", user.id);

    q.then(({ data, error }) => {
      if (error) {
        console.error("❌ fetch player_props:", error);
        setRows([]);
      } else {
        setRows(data || []);
      }
    });

    // Realtime (optional; v2 syntax). Requires Realtime enabled on the table.
    const channel = supabase
      .channel("props-table")
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "player_props",
          filter: `game_date=eq.${day}`,
        },
        (payload) => setRows((prev) => [payload.new, ...prev])
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [day /*, onlyMine, user?.id */]);

  const sorted = useMemo(() => {
    const arr = [...rows];
    arr.sort((a, b) => {
      const av = a[sort.key];
      const bv = b[sort.key];
      if (av < bv) return sort.dir === "asc" ? -1 : 1;
      if (av > bv) return sort.dir === "asc" ? 1 : -1;
      return 0;
    });
    return arr;
  }, [rows, sort]);

  const setSortKey = (key) =>
    setSort((prev) => ({
      key,
      dir: prev.key === key && prev.dir === "asc" ? "desc" : "asc",
    }));

  const arrow = (key) =>
    sort.key === key ? (sort.dir === "asc" ? " ↑" : " ↓") : "";

  return (
    <div className="bg-blue-100 p-4 rounded-xl shadow-md overflow-x-auto">
      <h2 className="text-lg font-semibold mb-4">Player Props for {day}</h2>

      <table className="min-w-full text-sm text-gray-800">
        <thead className="bg-gray-100">
          <tr>
            <th
              onClick={() => setSortKey("player_name")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Player{arrow("player_name")}
            </th>
            <th
              onClick={() => setSortKey("team")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Team{arrow("team")}
            </th>
            <th
              onClick={() => setSortKey("prop_type")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Prop{arrow("prop_type")}
            </th>
            <th
              onClick={() => setSortKey("over_under")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              O/U{arrow("over_under")}
            </th>
            <th
              onClick={() => setSortKey("prop_value")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Value{arrow("prop_value")}
            </th>
            <th className="px-3 py-2 text-left">Status</th>
            <th
              onClick={() => setSortKey("game_date")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Game Date{arrow("game_date")}
            </th>
          </tr>
        </thead>

        <tbody>
          {sorted.map((p) => {
            const key = (p.outcome || p.status || "pending").toLowerCase();
            const label = key[0]?.toUpperCase() + key.slice(1);
            return (
              <tr key={p.id} className="border-t hover:bg-gray-50">
                <td className="px-3 py-2">
                  {p.player_name}
                  {p.position && (
                    <span className="ml-1 text-xs text-gray-500">
                      ({p.position})
                    </span>
                  )}
                </td>
                <td className="px-3 py-2">{p.team}</td>
                <td className="px-3 py-2">
                  {getPropDisplayLabel(p.prop_type)}
                </td>
                <td className="px-3 py-2">{p.over_under}</td>
                <td className="px-3 py-2">{p.prop_value}</td>
                <td className="px-3 py-2">
                  <span
                    className={`px-2 py-1 rounded-full text-xs font-semibold ${
                      statusColor[key] || statusColor.pending
                    }`}
                  >
                    {label}
                  </span>
                </td>
                <td className="px-3 py-2">{p.game_date}</td>
              </tr>
            );
          })}
          {sorted.length === 0 && (
            <tr>
              <td colSpan="7" className="px-3 py-6 text-center text-gray-500">
                No props for {day}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
