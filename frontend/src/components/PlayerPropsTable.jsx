// /src/components/PlayerPropsTable.js
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { format, isValid } from "date-fns";
import { supabase } from "../utils/supabaseFrontend.js";
import { todayET } from "../shared/timeUtils.js";
import { getPropDisplayLabel } from "../shared/propUtils.js";
// (Optional) if you want "only my props":
// import { useAuth } from "../context/AuthContext.jsx";

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

function normalizeState(row) {
  const status = String(row?.status || "").toLowerCase();
  const outcome = String(row?.outcome || "").toLowerCase();

  if (["win", "loss", "push"].includes(outcome)) return outcome;
  if (["resolved", "live", "pending", "expired", "dnp"].includes(status)) return status;
  if (["win", "loss", "push"].includes(status)) return status;
  return "pending";
}

function formatSource(row) {
  const source = String(row?.prop_source || "").trim();
  if (!source) return "unknown";
  return source.replace(/_/g, " ");
}

function formatUpdated(row) {
  const raw = row?.updated_at || row?.prediction_timestamp || row?.created_at;
  if (!raw) return "—";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("en-US", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export default function PlayerPropsTable({
  selectedDate,
  onlyMine = false,
  refreshNonce = 0,
  lastSaveEvent = null,
}) {
  const [rows, setRows] = useState([]);
  const [sort, setSort] = useState({ key: "game_date", dir: "asc" });
  const [loading, setLoading] = useState(false);
  const [lastError, setLastError] = useState("");
  const [highlightedId, setHighlightedId] = useState(null);
  // const { user } = useAuth();

  const day = useMemo(() => {
    if (!selectedDate) return todayET();
    if (typeof selectedDate === "string") return selectedDate;
    return isValid(selectedDate)
      ? format(selectedDate, "yyyy-MM-dd")
      : todayET();
  }, [selectedDate]);

  const fetchRows = useCallback(async () => {
    setLoading(true);
    setLastError("");
    let q = supabase
      .schema("mlb")
      .from("player_props")
      .select("*")
      .eq("game_date", day)
      .neq("status", "expired")
      .order("created_at", { ascending: false });

    // If you want to show only the current user’s props, uncomment this:
    // if (onlyMine && user?.id) q = q.eq("user_id", user.id);

    const { data, error } = await q;
    if (error) {
      console.error("❌ fetch player_props:", error);
      setRows([]);
      setLastError("Failed to refresh props table.");
    } else {
      setRows(data || []);
    }
    setLoading(false);
  }, [day]);

  useEffect(() => {
    fetchRows();

    // Realtime (optional; v2 syntax). Requires Realtime enabled on the table.
    const channel = supabase
      .channel("props-table")
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "mlb",
          table: "player_props",
          filter: `game_date=eq.${day}`,
        },
        () => fetchRows()
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [day, fetchRows /*, onlyMine, user?.id */]);

  useEffect(() => {
    if (!refreshNonce) return;
    fetchRows();
  }, [refreshNonce, fetchRows]);

  useEffect(() => {
    const id = lastSaveEvent?.id;
    if (!id) return;
    const marker = String(id);
    setHighlightedId(marker);
    const t = setTimeout(() => setHighlightedId(null), 6000);
    return () => clearTimeout(t);
  }, [lastSaveEvent]);

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
    <div className="pp-card p-4 overflow-x-auto">
      <div className="flex items-center justify-between mb-4 gap-2">
        <h2 className="text-lg font-semibold text-slate-900">Player Props for {day}</h2>
        <button
          type="button"
          onClick={fetchRows}
          disabled={loading}
          className="pp-btn pp-btn-secondary pp-btn-sm"
        >
          {loading ? "Refreshing…" : "Refresh"}
        </button>
      </div>
      {lastError && (
        <div className="mb-3 text-sm text-rose-700 bg-rose-100 rounded-md p-2">
          {lastError}
        </div>
      )}

      <table className="min-w-full text-sm text-slate-800">
        <thead className="bg-slate-100">
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
              onClick={() => setSortKey("prop_source")}
              className="px-3 py-2 text-left cursor-pointer"
            >
              Source{arrow("prop_source")}
            </th>
            <th className="px-3 py-2 text-left">Updated</th>
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
            const key = normalizeState(p);
            const label = key[0]?.toUpperCase() + key.slice(1);
            const isHighlighted = highlightedId != null && String(p.id) === highlightedId;
            const highlightStyle = isHighlighted
              ? lastSaveEvent?.duplicate
                ? "bg-amber-50 ring-1 ring-amber-300"
                : "bg-green-50 ring-1 ring-green-300"
              : "";
            return (
              <tr key={p.id} className={`border-t border-slate-200 hover:bg-slate-50 ${highlightStyle}`}>
                <td className="px-3 py-2">
                  {p.player_name}
                  {p.position && (
                    <span className="ml-1 text-xs text-slate-500">
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
                <td className="px-3 py-2 capitalize">{formatSource(p)}</td>
                <td className="px-3 py-2 text-slate-600">{formatUpdated(p)}</td>
                <td className="px-3 py-2">{p.game_date}</td>
              </tr>
            );
          })}
          {sorted.length === 0 && (
            <tr>
              <td colSpan="9" className="px-3 py-6 text-center text-slate-500">
                No props for {day}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
