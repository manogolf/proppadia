import { useEffect, useState } from "react";
import { Card, CardContent } from "../components/ui/card.js";
import { getBaseURL } from "../../shared/getBaseURL.js";
import {
  normalizePropType,
  getPropDisplayLabel,
} from "../../shared/propUtils.js";
import { formatDateET } from "../shared/timeUtils.js";
import MetricsTable from "../components/MetricsTable.js";
import MetricsBarChart from "../components/MetricsBarChart.js";
import ModelAccuracyTable from "../components/ModelAccuracyTable.js";
import ModelAccuracyBarChart from "../components/ModelAccuracyBarChart.js";
import WeeklyModelAccuracyBarChart from "../components/WeeklyModelAccuracyBarChart.js";
import WeeklyModelAccuracyTable from "../components/WeeklyModelAccuracyTable.js";
import WeeklyUserVsModelBarChart from "../components/WeeklyUserVsModelBarChart.js";

export default function ModelMetricsDashboard() {
  const [metrics, setMetrics] = useState([]);
  const [modelAccuracy, setModelAccuracy] = useState([]);
  const [weeklyUserMetrics, setWeeklyUserMetrics] = useState([]);
  const [weeklyModelMetrics, setWeeklyModelMetrics] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchMetrics() {
      try {
        const userRes = await fetch(
          `${getBaseURL()}/api/user-vs-model-accuracy`
        );
        const userData = await userRes.json();

        const aggregated = {};
        for (const row of userData) {
          const key = normalizePropType(row.prop_type);
          if (!aggregated[key]) {
            aggregated[key] = {
              prop_type: key,
              total: 0,
              user_correct: 0,
              model_correct: 0,
              user_total: 0,
              model_total: 0,
            };
          }
          aggregated[key].total += row.total ?? 0;
          aggregated[key].user_correct += row.user_correct ?? 0;
          aggregated[key].model_correct += row.model_correct ?? 0;
          aggregated[key].user_total += row.user_total ?? 0;
          aggregated[key].model_total += row.model_total ?? 0;
        }

        const cleaned = Object.values(aggregated).map((row) => ({
          ...row,
          user_accuracy_pct:
            row.user_total > 0
              ? (100 * row.user_correct) / row.user_total
              : null,
          model_accuracy_pct:
            row.model_total > 0
              ? (100 * row.model_correct) / row.model_total
              : null,
        }));

        setMetrics(cleaned);

        const modelRes = await fetch(`${getBaseURL()}/api/model-metrics`);
        const modelData = await modelRes.json();

        const overallAggregated = {};
        for (const row of modelData) {
          const key = normalizePropType(row.prop_type);
          if (!overallAggregated[key]) {
            overallAggregated[key] = { prop_type: key, total: 0, correct: 0 };
          }
          overallAggregated[key].total += row.total ?? 0;
          overallAggregated[key].correct += row.correct ?? 0;
        }

        const overallCleaned = Object.values(overallAggregated).map((row) => ({
          ...row,
          accuracy: row.total > 0 ? (100 * row.correct) / row.total : null,
        }));

        setModelAccuracy(overallCleaned);

        const weeklyUserRes = await fetch(
          `${getBaseURL()}/api/user-vs-model-accuracy-weekly`
        );
        const weeklyUserData = await weeklyUserRes.json();
        setWeeklyUserMetrics(weeklyUserData);

        const weeklyModelRes = await fetch(
          `${getBaseURL()}/api/model-accuracy-weekly`
        );
        const weeklyModelData = await weeklyModelRes.json();
        // ✅ Move console.log here, after the data is loaded
        console.log("📦 Weekly Model Metrics:", weeklyModelData);
        setWeeklyModelMetrics(weeklyModelData);
      } catch (error) {
        console.error("❌ Failed to load accuracy metrics:", error);
      } finally {
        setLoading(false);
      }
    }

    fetchMetrics();
  }, []);

  if (loading) return <div className="p-4">Loading metrics...</div>;

  const weeklyUserMetricsGrouped = weeklyUserMetrics.reduce((acc, row) => {
    const week = row.week_start;
    if (!acc[week]) acc[week] = [];
    acc[week].push({
      ...row,
      user_accuracy_pct:
        row.user_total > 0 ? (100 * row.user_correct) / row.user_total : null,
      model_accuracy_pct:
        row.model_total > 0
          ? (100 * row.model_correct) / row.model_total
          : null,
    });
    return acc;
  }, {});

  // ✅ Format week_start to readable ET format
  const formattedWeeklyModelMetrics = weeklyModelMetrics.map((row) => ({
    ...row,
    week: formatDateET(row.week_start), // ✅ use actual field from API
  }));

  return (
    <div className="space-y-8">
      {/* ✅ Cumulative User vs Model */}
      <Card>
        <CardContent>
          <h2 className="text-xl font-semibold mb-2">
            User vs Model Accuracy by Prop Type
          </h2>
          <MetricsTable metrics={metrics} />
          <MetricsBarChart data={metrics} />
        </CardContent>
      </Card>

      {/* ✅ Cumulative Model Only */}
      <Card>
        <CardContent>
          <h2 className="text-xl font-semibold mb-2">Overall Model Accuracy</h2>
          <ModelAccuracyTable data={modelAccuracy} />
          <div className="mt-6 h-[320px]">
            <ModelAccuracyBarChart data={modelAccuracy} />
          </div>
        </CardContent>
      </Card>

      {/* ✅ Weekly Cards */}
      {Object.entries(weeklyUserMetricsGrouped)
        .sort(([a], [b]) => new Date(b) - new Date(a))
        .slice(0, 6)
        .filter(([_, rows]) =>
          rows.some((r) => r.user_total > 0 || r.model_total > 0)
        )
        .map(([week, rows]) => (
          <Card key={week}>
            <CardContent>
              <h2 className="text-lg font-semibold mb-2">
                Week of {formatDateET(week)} — User vs Model Accuracy
              </h2>
              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left p-2">Prop Type</th>
                      <th className="text-left p-2">User Accuracy %</th>
                      <th className="text-left p-2">Model Accuracy %</th>
                      <th className="text-left p-2">Total Props</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, i) => (
                      <tr key={i} className="border-b">
                        <td className="p-2">
                          {getPropDisplayLabel(row.prop_type)}
                        </td>
                        <td className="p-2">
                          {typeof row.user_accuracy_pct === "number"
                            ? `${row.user_accuracy_pct.toFixed(1)}%`
                            : "N/A"}
                        </td>
                        <td className="p-2">
                          {typeof row.model_accuracy_pct === "number"
                            ? `${row.model_accuracy_pct.toFixed(1)}%`
                            : "N/A"}
                        </td>
                        <td className="p-2">{row.total ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="mt-6 h-[320px]">
                <WeeklyUserVsModelBarChart data={rows} />
              </div>
            </CardContent>
          </Card>
        ))}

      {/* ✅ Weekly Model Accuracy */}
      <Card>
        <CardContent>
          <h2 className="text-xl font-semibold mb-2">
            Rolling 6-Week Model Accuracy
          </h2>
          <WeeklyModelAccuracyTable data={formattedWeeklyModelMetrics} />

          <div className="mt-6 h-[320px]">
            <WeeklyModelAccuracyBarChart data={formattedWeeklyModelMetrics} />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
