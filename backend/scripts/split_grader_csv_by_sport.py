#!/usr/bin/env python3
"""Split a combined 8rainstation grader CSV into per-sport/per-league files.

Example:
  .venv/bin/python backend/scripts/split_grader_csv_by_sport.py \
    --in-csv ~/Downloads/8rainstation_daily_2026_03_26.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip().replace(",", "")
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _norm_slug(value: Any) -> str:
    text = str(value if value is not None else "").strip().lower()
    if not text:
        return "unknown"
    chars = []
    for ch in text:
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "unknown"


def _norm_grade(value: Any) -> str:
    text = str(value if value is not None else "").strip().lower()
    if text in {"win", "w"}:
        return "win"
    if text in {"loss", "l"}:
        return "loss"
    if text in {"push", "p"}:
        return "push"
    return text or "unknown"


def _infer_date_label(in_csv: Path, rows: list[dict[str, Any]]) -> str:
    stem = in_csv.stem
    if stem.startswith("8rainstation_daily_"):
        raw = stem.replace("8rainstation_daily_", "").replace("_", "-")
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        except Exception:
            pass

    event_dates: list[str] = []
    for row in rows:
        raw = str(row.get("Event Date") or "").strip()
        if not raw:
            continue
        try:
            d = datetime.strptime(raw, "%m/%d/%Y %I:%M:%S %p")
            event_dates.append(d.date().isoformat())
        except Exception:
            continue
    if event_dates:
        return sorted(event_dates)[-1]
    return datetime.now().date().isoformat()


def _build_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    graded = [r for r in rows if _norm_grade(r.get("Grade")) in {"win", "loss", "push"}]
    wl = [r for r in graded if _norm_grade(r.get("Grade")) in {"win", "loss"}]
    wins = sum(1 for r in wl if _norm_grade(r.get("Grade")) == "win")
    losses = sum(1 for r in wl if _norm_grade(r.get("Grade")) == "loss")
    pushes = sum(1 for r in graded if _norm_grade(r.get("Grade")) == "push")
    stake = sum(_to_float(r.get("Amount")) for r in graded)
    pnl = sum(_to_float(r.get("$ W/L")) for r in graded)
    roi = (pnl / stake) if stake > 0 else 0.0
    return {
        "rows": len(rows),
        "graded_rows": len(graded),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "stake": stake,
        "pnl": pnl,
        "roi": roi,
    }


def split_grader_csv(*, in_csv: Path, out_dir: Path) -> dict[str, Any]:
    with in_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if not fieldnames:
        raise RuntimeError(f"CSV has no header: {in_csv}")

    date_label = _infer_date_label(in_csv, rows)
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        sport = _norm_slug(row.get("Sport"))
        league = _norm_slug(row.get("League"))
        by_key.setdefault((sport, league), []).append(row)

    out_dir.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, Any] = {}
    total_rows_written = 0
    for (sport, league), group in sorted(by_key.items()):
        out_csv = out_dir / f"8rainstation_daily_{date_label}_{league}.csv"
        with out_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(group)
        stats = _build_stats(group)
        outputs[f"{sport}:{league}"] = {
            "out_csv": str(out_csv),
            **stats,
        }
        total_rows_written += len(group)

    summary = {
        "ok": True,
        "in_csv": str(in_csv),
        "date_label": date_label,
        "rows_in_file": len(rows),
        "rows_written": total_rows_written,
        "outputs": outputs,
    }

    out_json = out_dir / f"8rainstation_daily_{date_label}_split_summary.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["out_json"] = str(out_json)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Split combined 8rainstation grader CSV into per-sport files.")
    ap.add_argument("--in-csv", required=True, help="Path to 8rainstation_daily_YYYY_MM_DD.csv")
    ap.add_argument("--out-dir", default="tmp/graded", help="Directory for split outputs.")
    args = ap.parse_args()

    in_csv = Path(args.in_csv).expanduser().resolve()
    if not in_csv.exists():
        raise SystemExit(f"Missing input CSV: {in_csv}")

    out_dir = Path(args.out_dir).expanduser()
    summary = split_grader_csv(in_csv=in_csv, out_dir=out_dir)

    print(f"[split-grader] in={summary['in_csv']}")
    print(f"[split-grader] rows_in_file={summary['rows_in_file']} rows_written={summary['rows_written']}")
    for key, payload in summary["outputs"].items():
        print(
            f"[split-grader] {key} rows={payload['rows']} "
            f"W-L-P={payload['wins']}-{payload['losses']}-{payload['pushes']} "
            f"roi={payload['roi']*100:.2f}% -> {payload['out_csv']}"
        )
    print(f"[split-grader] summary_json={summary['out_json']}")


if __name__ == "__main__":
    main()

