// WeeklyUserVsAccuracy.js
import { useEffect, useState } from "react";
import { Card, CardContent } from "../components/ui/card.js";
import WeeklyMetricsTable from "./archive/WeeklyMetricsTable.js";
import WeeklyMetricsBarChart from "./archive/WeeklyMetricsBarChart.js";
import { getBaseURL } from "@shared/getBaseURL.js";
import { DateTime } from "luxon";

export default function WeeklyUserVsAccuracy() {
  const [weeklyData, setWeeklyData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchWeeklyData() {
      try {
        const res = await fetch(
          `${getBaseURL()}/api/user-vs-model-accuracy-weekly`
        );
        const raw = await res.json();

        const normalized = raw.map((row) => ({
          ...row,
          week_start: DateTime.fromISO(row.week).toFormat("LLL dd"),
          user_accuracy_pct:
            row.user_total > 0
              ? (100 * row.user_correct) / row.user_total
              : null,
          model_accuracy_pct:
            row.model_total > 0
              ? (100 * row.model_correct) / row.model_total
              : null,
        }));

        setWeeklyData(normalized);
      } catch (error) {
        console.error("❌ Failed to load weekly metrics:", error);
      } finally {
        setLoading(false);
      }
    }

    fetchWeeklyData();
  }, []);

  if (loading) return <div className="p-4">Loading weekly accuracy...</div>;
  if (!weeklyData.length)
    return <div className="p-4">No weekly accuracy data available.</div>;

  return (
    <Card>
      <CardContent>
        <h2 className="text-xl font-semibold mb-2">
          Rolling 6-Week User vs Model Accuracy
        </h2>
        <WeeklyMetricsTable data={weeklyData} />
        <div className="mt-6 h-[320px]">
          <WeeklyMetricsBarChart data={weeklyData} />
        </div>
      </CardContent>
    </Card>
  );
}
