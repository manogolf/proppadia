// ✅ WeeklyModelAccuracyTable.js
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "../components/ui/table.js";
import { getPropDisplayLabel } from "../shared/propUtils.js";
import { DateTime } from "luxon";

export default function WeeklyModelAccuracyTable({ data }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableCell>Week</TableCell>
          <TableCell>Prop Type</TableCell>
          <TableCell>Accuracy %</TableCell>
          <TableCell>Total</TableCell>
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
              {typeof row.accuracy === "number"
                ? `${row.accuracy.toFixed(1)}%`
                : "N/A"}
            </TableCell>
            <TableCell>{row.total}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
