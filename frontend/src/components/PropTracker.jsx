// /src/components/PropTracker.js
import React, { useEffect, useMemo, useState } from "react";
import { format, isValid } from "date-fns";
import { todayET } from "../shared/timeUtils.js";
import { fetchMlbPropsForDate } from "../lib/mlbPropsApi.js";
import Calendar from "./ui/calendar.jsx";
import AccuracyByPropType from "./AccuracyByPropType.jsx";
import { getPropDisplayLabel } from "../shared/propUtils.js";

export default function PropTracker({ selectedDate, setSelectedDate }) {
  // default to ET today if nothing chosen
  useEffect(() => {
    if (!selectedDate) setSelectedDate?.(todayET());
  }, [selectedDate, setSelectedDate]);

  const day = useMemo(() => {
    if (!selectedDate) return todayET();
    if (typeof selectedDate === "string") return selectedDate;
    return isValid(selectedDate)
      ? format(selectedDate, "yyyy-MM-dd")
      : todayET();
  }, [selectedDate]);

  const [props, setProps] = useState([]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const out = await fetchMlbPropsForDate(day);
        if (!cancelled) setProps(out);
      } catch (error) {
        console.error("❌ fetch props via backend:", error);
        if (!cancelled) setProps([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [day]);

  const selectedDateObj = useMemo(() => new Date(`${day}T00:00:00`), [day]);

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-row gap-8">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">Select a Date</h2>
          <Calendar
            mode="single"
            selected={selectedDateObj}
            onSelect={(d) =>
              setSelectedDate?.(format(d ?? new Date(), "yyyy-MM-dd"))
            }
            className="rounded-md border border-slate-200"
          />
          <AccuracyByPropType selectedDate={selectedDateObj} />
        </div>

        <div className="flex-1">
          <h2 className="text-lg font-semibold mb-2 text-slate-900">
            Player Props for {format(selectedDateObj, "PPP")}
          </h2>

          <div className="pp-card overflow-hidden">
            <table className="w-full text-sm text-left text-slate-800">
              <thead className="bg-slate-100">
                <tr>
                  <th className="px-4 py-2">Player</th>
                  <th className="px-4 py-2">Team</th>
                  <th className="px-4 py-2">Prop</th>
                  <th className="px-4 py-2">O/U</th>
                  <th className="px-4 py-2">Value</th>
                  <th className="px-4 py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {props.length > 0 ? (
                  props.map((p) => (
                    <tr key={p.id} className="border-t border-slate-200">
                      <td className="px-4 py-2">
                        {p.player_name}
                        {p.position && (
                          <span className="ml-1 text-xs text-slate-500">
                            ({p.position})
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-2">{p.team}</td>
                      <td className="px-4 py-2">
                        {getPropDisplayLabel(p.prop_type)}
                      </td>
                      <td className="px-4 py-2">{p.over_under}</td>
                      <td className="px-4 py-2">{p.prop_value}</td>
                      <td className="px-4 py-2">
                        <span className="px-2 py-1 rounded-full text-xs font-semibold bg-slate-100 text-slate-600">
                          {(p.outcome || p.status || "pending")
                            .toString()
                            .replace(/^\w/, (c) => c.toUpperCase())}
                        </span>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan="6"
                      className="px-4 py-4 text-center text-slate-500"
                    >
                      No props for {day}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
