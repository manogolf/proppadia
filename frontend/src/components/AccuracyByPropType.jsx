import React, { useState, useEffect } from "react";
import { supabase } from "../utils/supabaseFrontend.js";
import { toISODate } from "../shared/timeUtils.js";
import { getPropDisplayLabel } from "../shared/propUtils.js";

export default function AccuracyByPropType({ selectedDate }) {
  const [accuracyData, setAccuracyData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!selectedDate) return;

    const fetchAccuracy = async () => {
      setLoading(true);
      const { data, error } = await supabase.rpc("get_daily_prop_accuracy", {
        target_date: toISODate(selectedDate),
      });

      if (error) {
        console.error("❌ Failed to fetch accuracy data:", error.message);
        setAccuracyData([]);
      } else {
        setAccuracyData(data);
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
