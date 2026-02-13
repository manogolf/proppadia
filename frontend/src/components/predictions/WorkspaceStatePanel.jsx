import React from "react";

const TONE = {
  loading: "bg-slate-100 text-slate-700 border-slate-200",
  error: "bg-rose-50 text-rose-700 border-rose-200",
  empty: "bg-amber-50 text-amber-800 border-amber-200",
  sparse: "bg-slate-100 text-slate-700 border-slate-300",
};

export default function WorkspaceStatePanel({ kind = "loading", title, detail }) {
  const tone = TONE[kind] || TONE.loading;
  return (
    <div className={`w-full border rounded-xl p-4 ${tone}`}>
      <div className="font-semibold">{title}</div>
      {detail ? <div className="text-sm mt-1">{detail}</div> : null}
    </div>
  );
}
