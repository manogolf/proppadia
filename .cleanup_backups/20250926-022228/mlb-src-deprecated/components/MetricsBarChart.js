import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { getPropDisplayLabel } from "../../shared/propUtils.js";

export default function MetricsBarChart({ data }) {
  if (!data?.length) return null;

  return (
    <div className="mt-6 h-[320px]">
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
          <Bar
            dataKey="user_accuracy_pct"
            name="User"
            fill="#FFA500"
            barSize={14}
          />
          <Bar
            dataKey="model_accuracy_pct"
            name="Model"
            fill="#CC6600"
            barSize={14}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
