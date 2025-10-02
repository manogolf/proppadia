// ✅ WeeklyMetricsTable.js
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "../ui/table.jsx";
import { getPropDisplayLabel } from "../shared/archive/propUtils.js";
import { DateTime } from "luxon";

export default function WeeklyMetricsTable({ data }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableCell>Week</TableCell>
          <TableCell>Prop Type</TableCell>
          <TableCell>User Accuracy %</TableCell>
          <TableCell>Model Accuracy %</TableCell>
          <TableCell>Total Props</TableCell>
        </TableRow>
      </TableHeader>
      <TableBody>
        {data.map((row, index) => (
          <TableRow key={index}>
            <TableCell>
              {DateTime.fromISO(row.week_start).toFormat("LLL dd")}
            </TableCell>
            <TableCell>{getPropDisplayLabel(row.prop_type)}</TableCell>
            <TableCell>
              {typeof row.user_accuracy_pct === "number"
                ? `${row.user_accuracy_pct.toFixed(1)}%`
                : "N/A"}
            </TableCell>
            <TableCell>
              {typeof row.model_accuracy_pct === "number"
                ? `${row.model_accuracy_pct.toFixed(1)}%`
                : "N/A"}
            </TableCell>
            <TableCell>{row.total}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
