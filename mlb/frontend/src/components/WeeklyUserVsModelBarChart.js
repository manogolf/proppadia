import React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { getPropDisplayLabel } from "../../shared/propUtils.js";

export default function WeeklyUserVsModelBarChart({ data }) {
  return (
    <div className="h-[400px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 10, right: 20, left: 0, bottom: 0 }}
        >
          <XAxis
            type="number"
            domain={[0, 100]}
            tickFormatter={(val) => `${val}%`}
          />
          <YAxis
            type="category"
            dataKey="prop_type"
            width={160}
            tickFormatter={getPropDisplayLabel}
          />
          <Tooltip
            formatter={(value) => `${value.toFixed(1)}%`}
            labelFormatter={(label) => getPropDisplayLabel(label)}
          />
          <Legend />
          <Bar
            dataKey="user_accuracy_pct"
            name="User Accuracy"
            fill="#FFA500"
            barSize={14}
          />
          <Bar
            dataKey="model_accuracy_pct"
            name="Model Accuracy"
            fill="#CC6600"
            barSize={14}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
