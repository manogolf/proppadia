import React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

export default function WeeklyModelAccuracyBarChart({ data }) {
  if (!data?.length) return null;

  return (
    <div className="h-[400px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          margin={{ top: 10, right: 20, left: 10, bottom: 40 }}
        >
          <XAxis
            dataKey="week_start"
            angle={-45}
            textAnchor="end"
            height={60}
          />
          <YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} />
          <Tooltip
            formatter={(value) => `${value.toFixed(1)}%`}
            labelFormatter={(label) => `Week of ${label}`}
          />
          <Legend />
          <Bar
            dataKey="accuracy"
            name="Model Accuracy %"
            fill="#00AA88"
            barSize={24}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
