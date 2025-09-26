import React from "react";

export function Table({ children }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border border-gray-300">{children}</table>
    </div>
  );
}

export function TableHeader({ children }) {
  return <thead className="bg-gray-100">{children}</thead>;
}

export function TableRow({ children }) {
  return <tr className="border-t border-gray-200">{children}</tr>;
}

export function TableHead({ children }) {
  return (
    <th className="px-4 py-2 text-left text-sm font-semibold text-gray-700">
      {children}
    </th>
  );
}

export function TableBody({ children }) {
  return <tbody>{children}</tbody>;
}

export function TableCell({ children }) {
  return <td className="px-4 py-2 text-sm text-gray-800">{children}</td>;
}
