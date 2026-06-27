#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_REVIEW_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_SUMMARY = Path("artifacts/analysis/mlb/review_aids/alternate_history/o15_alternate_history_build_summary.csv")
DEFAULT_RECHECK = Path("artifacts/analysis/mlb/review_aids/alternate_history/o15_alternate_history_7day_recheck.md")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_dates(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


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


def _line15_count(rows: list[dict[str, Any]]) -> int:
    out = 0
    for row in rows:
        try:
            if abs(float(row.get("line")) - 1.5) < 1e-9:
                out += 1
        except Exception:
            pass
    return out


def _count(rows: list[dict[str, Any]], column: str, value: str) -> int:
    return sum(1 for row in rows if str(row.get(column) or "") == value)


def _nonempty_count(rows: list[dict[str, Any]], column: str) -> int:
    return sum(1 for row in rows if str(row.get(column) or "").strip())


def _build_one(
    *,
    day: date,
    source_csv: Path,
    review_dir: Path,
    slate_output_csv: Path,
    dry_run: bool,
) -> tuple[str, str]:
    cmd = [
        sys.executable,
        "backend/mlb/scripts/run_mlb_hits_o15_review_board.py",
        "--board",
        "alternate_o15",
        "--date",
        day.isoformat(),
        "--slate-output-csv",
        str(slate_output_csv),
        "--hits-environment-json",
        "artifacts/analysis/mlb/mlb_hits_environment_latest.json",
        "--hits-environment-history-jsonl",
        "artifacts/analysis/mlb/mlb_hits_environment_history.jsonl",
        "--hits-environment-snapshot-dir",
        "artifacts/analysis/mlb/hits_environment_snapshots",
        "--starter-required-min-starts",
        "5",
        "--alternate-book-level-csv",
        str(source_csv),
        "--out-dir",
        str(review_dir),
    ]
    if dry_run:
        return "dry_run", " ".join(cmd)
    proc = subprocess.run(cmd, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    return ("built" if proc.returncode == 0 else "build_failed"), proc.stdout


def _summarize_date(day: date, source_csv: Path, board_csv: Path, status: str, note: str) -> dict[str, Any]:
    source_rows = _read_csv(source_csv)
    board_rows = _read_csv(board_csv)
    starter_context_rows = _nonempty_count(board_rows, "starter_expected_hits_allowed")
    return {
        "date": day.isoformat(),
        "status": status,
        "source_csv": str(source_csv),
        "source_exists": source_csv.exists(),
        "source_rows": len(source_rows),
        "source_line_1_5_rows": _line15_count(source_rows),
        "board_csv": str(board_csv),
        "board_exists": board_csv.exists(),
        "board_rows": len(board_rows),
        "layer_a": _count(board_rows, "alternate_layer", "alternate_layer_a_d7_d15_starter"),
        "layer_b": _count(board_rows, "alternate_layer", "alternate_layer_b_d7_d15"),
        "layer_c": _count(board_rows, "alternate_layer", "alternate_layer_c_d7_hot"),
        "starter_context_rows": starter_context_rows,
        "missing_context_rows": max(0, len(board_rows) - starter_context_rows),
        "note": note[:500],
    }


def _write_report(path: Path, rows: list[dict[str, Any]], *, date_from: str, date_to: str) -> None:
    built = [row for row in rows if row.get("status") == "built"]
    missing = [row for row in rows if row.get("status") == "missing_source"]
    total_board = sum(int(row.get("board_rows") or 0) for row in rows)
    total_layer_a = sum(int(row.get("layer_a") or 0) for row in rows)
    total_layer_b = sum(int(row.get("layer_b") or 0) for row in rows)
    total_layer_c = sum(int(row.get("layer_c") or 0) for row in rows)
    lines = [
        "# O1.5 Alternate Historical 7-Day Recheck",
        "",
        f"- Date range: `{date_from}` through `{date_to}`",
        f"- Dates built: `{len(built)}`",
        f"- Missing source dates: `{len(missing)}`",
        f"- Board rows: `{total_board}`",
        f"- Layer A: `{total_layer_a}`",
        f"- Layer B: `{total_layer_b}`",
        f"- Layer C: `{total_layer_c}`",
        "",
        "## By Date",
        "",
        "| date | status | source rows | line 1.5 source rows | board rows | Layer A | Layer B | Layer C | starter context | missing context |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('date')} | {row.get('status')} | {row.get('source_rows')} | {row.get('source_line_1_5_rows')} | "
            f"{row.get('board_rows')} | {row.get('layer_a')} | {row.get('layer_b')} | {row.get('layer_c')} | "
            f"{row.get('starter_context_rows')} | {row.get('missing_context_rows')} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Alternate history remains manual/research-only and over-only.",
            "- These boards are not production scoring, upload, selection, or grading inputs.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build O1.5 alternate discovery boards from historical alternate source CSVs over a date range.")
    ap.add_argument("--date-from", required=True, type=_parse_date)
    ap.add_argument("--date-to", required=True, type=_parse_date)
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--review-dir", default=str(DEFAULT_REVIEW_DIR))
    ap.add_argument("--summary-csv", default=str(DEFAULT_SUMMARY))
    ap.add_argument("--report-md", default=str(DEFAULT_RECHECK))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.date_to < args.date_from:
        raise SystemExit("--date-to must be on or after --date-from")

    rows: list[dict[str, Any]] = []
    backfill_root = Path(args.backfill_root)
    review_dir = Path(args.review_dir)
    for day in _iter_dates(args.date_from, args.date_to):
        date_text = day.isoformat()
        source_csv = backfill_root / date_text / "live_alternate_book_level_rows.csv"
        board_csv = review_dir / f"hits_o15_alternate_discovery_{date_text}.csv"
        slate_output_csv = Path(f"backend/mlb/exports/odds_history/{date_text}/mlb_slate_output.csv")
        if not source_csv.exists():
            rows.append(_summarize_date(day, source_csv, board_csv, "missing_source", "historical source CSV missing"))
            continue
        if not slate_output_csv.exists():
            rows.append(_summarize_date(day, source_csv, board_csv, "missing_slate_output", "archived slate output missing"))
            continue
        status, note = _build_one(
            day=day,
            source_csv=source_csv,
            review_dir=review_dir,
            slate_output_csv=slate_output_csv,
            dry_run=args.dry_run,
        )
        rows.append(_summarize_date(day, source_csv, board_csv, status, note))

    _write_csv(Path(args.summary_csv), rows)
    _write_report(Path(args.report_md), rows, date_from=args.date_from.isoformat(), date_to=args.date_to.isoformat())
    print(json.dumps({"dates": len(rows), "built": sum(1 for row in rows if row.get("status") == "built"), "summary_csv": args.summary_csv}, indent=2))


if __name__ == "__main__":
    main()
