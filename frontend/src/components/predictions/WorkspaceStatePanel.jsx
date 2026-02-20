import React from "react";

const TONE = {
  loading: "bg-slate-100 text-slate-700 border-slate-200",
  error: "bg-rose-50 text-rose-700 border-rose-200",
  empty: "bg-amber-50 text-amber-800 border-amber-200",
  sparse: "bg-slate-100 text-slate-700 border-slate-300",
};

export default function WorkspaceStatePanel({
  kind = "loading",
  title,
  detail,
  centered = false,
  className = "",
}) {
  const tone = TONE[kind] || TONE.loading;
  const titleClassName = centered ? "font-semibold text-center" : "font-semibold";
  const detailClassName = centered ? "text-sm mt-1 text-center" : "text-sm mt-1";
  return (
    <div className={`w-full border rounded-xl p-4 ${tone} ${className}`.trim()}>
      <div className={titleClassName}>{title}</div>
      {detail ? <div className={detailClassName}>{detail}</div> : null}
    </div>
  );
}
