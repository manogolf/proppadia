// components/MetricsTable.js
import React from "react";
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "./ui/table.js";
import { getPropDisplayLabel } from "../../shared/propUtils.js";

export default function MetricsTable({ metrics }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Prop Type</TableHead>
          <TableHead>User Accuracy %</TableHead>
          <TableHead>Model Accuracy %</TableHead>
          <TableHead>Total Props</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {metrics.map((row) => (
          <TableRow key={row.prop_type}>
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
