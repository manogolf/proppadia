import React, { useState, useEffect } from "react";
import { toISODate } from "../shared/timeUtils.js";
import { fetchMlbPropsForDate } from "../lib/mlbPropsApi.js";
import { getPropDisplayLabel } from "../shared/propUtils.js";

export default function AccuracyByPropType({ selectedDate }) {
  const [accuracyData, setAccuracyData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!selectedDate) return;

    const fetchAccuracy = async () => {
      setLoading(true);
      try {
        const rows = await fetchMlbPropsForDate(toISODate(selectedDate));
        const byProp = new Map();

        for (const row of rows) {
          const propType = String(row?.prop_type || "").trim();
          if (!propType) continue;
          const outcome = String(row?.outcome || row?.status || "").toLowerCase();
          // Treat win/loss as accuracy rows; pushes are neutral and excluded.
          if (!["win", "loss"].includes(outcome)) continue;

          const bucket = byProp.get(propType) || { prop_type: propType, total: 0, correct: 0 };
          bucket.total += 1;
          if (outcome === "win") bucket.correct += 1;
          byProp.set(propType, bucket);
        }

        const data = Array.from(byProp.values())
          .map((row) => ({
            ...row,
            accuracy_pct:
              row.total > 0 ? ((row.correct / row.total) * 100).toFixed(1) : "0.0",
          }))
          .sort((a, b) => a.prop_type.localeCompare(b.prop_type));

        setAccuracyData(data);
      } catch (error) {
        console.error("❌ Failed to fetch accuracy data:", error?.message || error);
        setAccuracyData([]);
      }
      setLoading(false);
    };

    fetchAccuracy();
  }, [selectedDate]);

  const renderTable = () => (
    <table className="w-full text-sm border-collapse text-slate-800">
      <thead>
        <tr className="border-b border-slate-200">
          <th className="text-left py-1">Prop Type</th>
          <th className="text-right py-1">Total</th>
          <th className="text-right py-1">Correct</th>
          <th className="text-right py-1">Accuracy (%)</th>
        </tr>
      </thead>
      <tbody>
        {accuracyData.map((row) => (
          <tr key={row.prop_type} className="border-b border-slate-200">
            <td className="py-1">{getPropDisplayLabel(row.prop_type)}</td>
            <td className="text-right py-1">{row.total}</td>
            <td className="text-right py-1">{row.correct}</td>
            <td className="text-right py-1">{row.accuracy_pct}</td>
          </tr>
        ))}
        {accuracyData.length > 1 && (
          <tr className="border-t border-slate-300 font-semibold">
            <td className="py-1">Total</td>
            <td className="text-right py-1">
              {accuracyData.reduce((sum, row) => sum + row.total, 0)}
            </td>
            <td className="text-right py-1">
              {accuracyData.reduce((sum, row) => sum + row.correct, 0)}
            </td>
            <td className="text-right py-1">
              {(
                (accuracyData.reduce((sum, row) => sum + row.correct, 0) /
                  accuracyData.reduce((sum, row) => sum + row.total, 0)) *
                100
              ).toFixed(1)}
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );

  return (
    <div className="mt-12 pp-card p-3 w-full max-w-sm">
      <h3 className="text-lg font-semibold mb-2 text-slate-900">Prediction Accuracy</h3>
      {loading ? (
        <p className="text-sm text-slate-500">Loading...</p>
      ) : accuracyData.length === 0 ? (
        <p className="text-sm text-slate-500">No predictions for this day.</p>
      ) : (
        renderTable()
      )}
    </div>
  );
}
