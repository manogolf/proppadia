#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.mlb.scripts.build_o15_decision_flow_prototype import _fmt_pct, _read_csv, _rel_link, _surface_rows


PERF_DIR = Path("artifacts/analysis/mlb/review_aids/performance")
MLB_ROOT = Path("artifacts/analysis/mlb")

WORKBENCH_DISPLAY = {
    "Alternate O1.5 Discovery Universe": "Alternate Discovery",
    "Expanded O1.5 Universe": "Expanded Universe",
    "Main O1.5 Expanded Review Population": "Main Expanded Review",
    "Main O1.5 Watch Population": "Watch",
}

WORKBENCH_PURPOSE = {
    "Alternate O1.5 Discovery Universe": "Largest operational population with stable recent historical support.",
    "Expanded O1.5 Universe": "Canonical current-slate research view across main and alternate sources.",
    "Main O1.5 Expanded Review Population": "Main-market review list with tier, context, and QC visibility.",
    "Main O1.5 Watch Population": "Narrow main-market check after broader candidate lists.",
}

WORKBENCH_PIVOTS = {
    "Alternate O1.5 Discovery Universe": [
        "Start Here: Tier",
        "Start Here: Price Bucket",
        "Start Here: Starter Expected Hits",
        "Then: Team Expected Hits",
        "Research: Opportunity Type",
        "Research: Provenance",
    ],
    "Expanded O1.5 Universe": [
        "Start Here: Opportunity Type",
        "Start Here: Tier",
        "Start Here: Team Expected Hits",
        "Then: Price Bucket",
        "Research: Source Bucket",
        "Research: Provenance",
    ],
    "Main O1.5 Expanded Review Population": [
        "Start Here: Tier",
        "Start Here: QC Score",
        "Start Here: Starter Expected Hits",
        "Then: Team Expected Hits",
        "Research: Provenance",
    ],
    "Main O1.5 Watch Population": [
        "Start Here: Tier",
        "Start Here: QC Score",
        "Start Here: Starter Expected Hits",
    ],
}

WORKBENCH_QUESTIONS = {
    "Alternate O1.5 Discovery Universe": [
        "Which C/A opportunities also have positive context?",
        "Are the +200s supported by starter or team expected hits?",
    ],
    "Expanded O1.5 Universe": [
        "Does today's slate contain context-supported plus-money candidates?",
        "Are alternate-only candidates carrying the best context?",
    ],
    "Main O1.5 Expanded Review Population": [
        "Any QC candidates matching successful archetypes?",
        "Do the main-market candidates overlap with alternate support?",
    ],
    "Main O1.5 Watch Population": [
        "Are any watch names still compelling after price/context review?",
        "Is this a true candidate or just a legacy watch flag today?",
    ],
}

PRIORITY_ORDER = {
    "Alternate O1.5 Discovery Universe": 5,
    "Expanded O1.5 Universe": 4,
    "Main O1.5 Expanded Review Population": 2,
    "Main O1.5 Watch Population": 1,
}

DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _system_status(date_text: str) -> tuple[str, list[str]]:
    gate = MLB_ROOT / "morning_gate_warnings.csv"
    summary_text = (MLB_ROOT / "morning_gate_summary.md").read_text(encoding="utf-8") if (MLB_ROOT / "morning_gate_summary.md").exists() else ""
    if "Operational Gate: `FAIL`" in summary_text:
        status = "FAIL"
    elif "Operational Gate: `WARN`" in summary_text:
        status = "WARN"
    else:
        status = "PASS"
    warnings: list[str] = []
    if gate.exists():
        with gate.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get("severity") in {"BLOCKER", "MAJOR", "MINOR"}:
                    warnings.append(f"{row.get('severity')} {row.get('issue')}: {row.get('detail')}")
    return status, warnings


def _rows_number(row: dict[str, Any]) -> int:
    text = str(row.get("current_slate_row_count") or "")
    return int(text) if text.isdigit() else 0


def _ranked_workbenches(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    workbenches = [
        row
        for row in rows
        if row.get("item_type") == "decision_surface"
        and str(row.get("launch_csv") or "").lower() == "true"
        and _rows_number(row) > 0
    ]
    for row in workbenches:
        score = PRIORITY_ORDER.get(str(row.get("item_name") or ""), 1)
        if str(row.get("status") or "") == "weak":
            score = min(score, 2)
        if str(row.get("status") or "") == "too thin":
            score = min(score, 1)
        row["priority_order"] = score
        row["priority_label"] = f"Priority {6 - score}" if score >= 1 else "Priority"
        row["workbench_name"] = WORKBENCH_DISPLAY.get(str(row.get("item_name") or ""), str(row.get("item_name") or ""))
        row["purpose"] = WORKBENCH_PURPOSE.get(str(row.get("item_name") or ""), "")
        row["suggested_pivots_friendly"] = "; ".join(WORKBENCH_PIVOTS.get(str(row.get("item_name") or ""), []))
        row["today_questions"] = "; ".join(WORKBENCH_QUESTIONS.get(str(row.get("item_name") or ""), []))
    sorted_rows = sorted(workbenches, key=lambda row: (-int(row.get("priority_order") or 0), str(row.get("item_name") or "")))
    for index, row in enumerate(sorted_rows, start=1):
        row["priority_label"] = f"Priority {index}"
    return sorted_rows


def _links_rows(workbenches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in workbenches:
        launch_path = str(row.get("launch_csv_path") or "")
        path_dates = DATE_RE.findall(launch_path)
        out.append(
            {
                "current_slate_date": row.get("current_slate_date"),
                "today_workbench": row.get("workbench_name"),
                "priority": row.get("priority_label"),
                "current_rows": row.get("current_slate_row_count"),
                "status": row.get("status"),
                "historical_resolved_rows": row.get("historical_resolved_rows"),
                "last_30_roi": row.get("last_30_roi"),
                "last_14_roi": row.get("last_14_roi"),
                "last_7_roi": row.get("last_7_roi"),
                "launch_csv_path": launch_path,
                "launch_csv_date": path_dates[-1] if path_dates else "",
                "current_slate_filter_required": "true" if row.get("population") == "expanded_universe" else "false",
                "purpose": row.get("purpose"),
                "suggested_pivots": row.get("suggested_pivots_friendly"),
                "today_questions": row.get("today_questions"),
            }
        )
    return out


def _evidence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence = [row for row in rows if row.get("item_type") == "evidence_signal"]
    evidence.append(
        {
            "item_name": "Review Aid Decision Performance",
            "status": "active",
            "historical_resolved_rows": "",
            "full_history_roi": "",
            "last_30_roi": "",
            "last_14_roi": "",
            "last_7_roi": "",
            "what_it_suggests_inspecting_today": "Canonical historical O1.5 performance tells which workbenches deserve attention.",
        }
    )
    evidence.append(
        {
            "item_name": "Expanded O1.5 Research Signals",
            "status": "monitoring",
            "historical_resolved_rows": "",
            "full_history_roi": "",
            "last_30_roi": "",
            "last_14_roi": "",
            "last_7_roi": "",
            "what_it_suggests_inspecting_today": "Use current research themes as pivot ideas, not as automatic decisions.",
        }
    )
    return evidence


def _ops_brief_path(date_text: str) -> Path:
    dated = MLB_ROOT / f"mlb_daily_ops_brief_{date_text}.md"
    return dated if dated.exists() else MLB_ROOT / "mlb_daily_ops_brief_latest.md"


def _write_md(path: Path, date_text: str, rows: list[dict[str, Any]], workbenches: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# O1.5 Morning Workbench",
        "",
        f"- Current slate date: `{date_text}`",
        f"- Generated at: `{datetime.utcnow().replace(microsecond=0).isoformat()}Z`",
        "- Prototype only; no existing report replaced.",
        "",
        "This workbench assumes the Ops Brief has already been read: system readiness checked, baseball context reviewed, and candidate review cleared.",
        "",
    ]
    lines.extend(["", "## Today's Priorities", ""])
    for row in workbenches:
        link = _rel_link(path, Path(str(row.get("launch_csv_path") or "")), "Open Today's Candidate CSV →")
        lines.extend(
            [
                f"### {row.get('priority_label')}: {row.get('workbench_name')}",
                "",
                f"- Reason: {row.get('purpose')}",
                f"- Current rows: `{row.get('current_slate_row_count')}`",
                f"- Historical sample: `{row.get('historical_resolved_rows')}` resolved",
                f"- Recent trend: last 30 ROI `{_fmt_pct(row.get('last_30_roi'))}`, last 14 ROI `{_fmt_pct(row.get('last_14_roi'))}`, last 7 ROI `{_fmt_pct(row.get('last_7_roi'))}`",
                f"- Status: `{row.get('status')}`",
                "- Estimated review time: `10-15 minutes`" if row.get("priority_label") == "Priority 1" else "- Estimated review time: `3-8 minutes`",
                "",
                "Questions:",
            ]
        )
        for question in WORKBENCH_QUESTIONS.get(str(row.get("item_name") or ""), []):
            lines.append(f"- {question}")
        lines.extend(["", link, "", "Suggested Pivot:"])
        for pivot in WORKBENCH_PIVOTS.get(str(row.get("item_name") or ""), []):
            lines.append(f"- {pivot}")
        lines.append("")
    lines.extend(
        [
            "## Why These Are Ranked",
            "",
            "Historical evidence guides attention. Today's CSV determines today's decision. History never makes today's decision.",
            "",
            "## Today's Candidate CSVs",
            "",
            "| Today's Candidate List | purpose | current rows | Open Today's CSV |",
            "|---|---|---:|---|",
        ]
    )
    for row in workbenches:
        link = _rel_link(path, Path(str(row.get("launch_csv_path") or "")), "Open Today's CSV")
        lines.append(f"| {row.get('workbench_name')} | {row.get('purpose')} | `{row.get('current_slate_row_count')}` | {link} |")
    lines.extend(
        [
            "",
            "## Evidence Summary",
            "",
            "This is why the priorities are ranked. This is not the workbench.",
            "",
            "| Evidence | status | historical confidence | recent trend | sample |",
            "|---|---|---|---|---:|",
        ]
    )
    for row in _evidence_rows(rows):
        confidence = (
            f"full ROI `{_fmt_pct(row.get('full_history_roi'))}`"
            if row.get("historical_resolved_rows")
            else "qualitative"
        )
        trend = f"last 30 `{_fmt_pct(row.get('last_30_roi'))}`, last 14 `{_fmt_pct(row.get('last_14_roi'))}`" if row.get("historical_resolved_rows") else row.get("what_it_suggests_inspecting_today")
        sample = row.get("historical_resolved_rows") or "n/a"
        lines.append(
            f"| {row.get('item_name')} | `{row.get('status')}` | {confidence} | {trend} | `{sample}` |"
        )
    lines.extend(
        [
            "",
            "## Research Parking Lot",
            "",
            "- Context-supported plus-money",
            "- Late-game composition",
            "- Price buckets",
            "- Feature centrality",
            "- Positive BvP as confirmation",
            "",
            "## Today's Conclusions",
            "",
            "- ",
            "",
            _rel_link(path, MLB_ROOT / "morning_timing_template.md", "Record Today's Conclusions / Timing Notes"),
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build O1.5 morning workbench prototype.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-dir", default=str(PERF_DIR))
    args = ap.parse_args()
    date_text = str(args.date)[:10]
    out_dir = Path(args.out_dir)
    rows = _surface_rows(date_text)
    for row in rows:
        row["current_slate_date"] = date_text
    workbenches = _ranked_workbenches(rows)
    _write_csv(out_dir / "o15_morning_workbench_links.csv", _links_rows(workbenches))
    _write_md(out_dir / "o15_morning_workbench.md", date_text, rows, workbenches)
    print(f"morning_workbench_count={len(workbenches)}")
    print(f"report={(out_dir / 'o15_morning_workbench.md').as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
