import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "../components/ui/table.jsx";
import { getPropDisplayLabel } from "../shared/propUtils.js";

export default function ModelAccuracyTable({ data }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableCell>Prop Type</TableCell>
          <TableCell>Accuracy %</TableCell>
          <TableCell>Total</TableCell>
        </TableRow>
      </TableHeader>
      <TableBody>
        {data.map((row) => (
          <TableRow key={row.prop_type}>
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
