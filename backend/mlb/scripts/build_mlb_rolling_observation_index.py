#!/usr/bin/env python3
"""Build a read-only multi-day index for rolling market-late observations."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from typing import Any


FIELDNAMES = [
    "date",
    "ledger_rows",
    "current_projection_rows",
    "current_eligible_rows",
    "late_discovered_current_rows",
    "current_eligible_late_discovered_rows",
    "disappeared_rows",
    "reappeared_rows",
    "hits_15_morning",
    "hits_15_late_discovered",
    "hits_15_current_eligible",
    "confirmed_lineup_overlay_count",
    "delta_summary_csv",
    "pivot_source_csv",
    "observation_md",
    "ops_brief_input_json",
]


def _date_key(value: Any) -> str:
    try:
        raw = str(value or "").strip()
        if not raw:
            return ""
        return date.fromisoformat(raw[:10]).isoformat()
    except Exception:
        return ""


def _int_value(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except Exception:
        return 0


def _parse_csv(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def _load_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"{path} did not contain a JSON object")
    return obj


def _candidate_dirs(root: Path, index_date: str) -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for path in sorted(root.glob("rolling_observation_*")):
        if not path.is_dir():
            continue
        date_value = _date_key(path.name.replace("rolling_observation_", "", 1))
        if not date_value:
            continue
        if index_date and date_value > index_date:
            continue
        out.append((date_value, path))
    return out


def _row_for_day(date_value: str, day_dir: Path) -> dict[str, Any] | None:
    ops_path = day_dir / f"rolling_candidate_ops_brief_input_{date_value}.json"
    if not ops_path.exists():
        return None
    payload = _load_json(ops_path)

    delta_path = Path(
        str(payload.get("delta_summary_csv") or day_dir / f"rolling_candidate_delta_summary_{date_value}.csv")
    )
    pivot_path = Path(
        str(payload.get("pivot_source_csv") or day_dir / f"rolling_candidate_pivot_source_{date_value}.csv")
    )
    current_path = Path(
        str(payload.get("current_projection_csv") or day_dir / f"rolling_candidate_current_projection_{date_value}.csv")
    )
    ledger_path = Path(
        str(payload.get("ledger_csv") or day_dir / f"rolling_candidate_ledger_{date_value}.csv")
    )

    for parse_path in (delta_path, pivot_path, current_path, ledger_path):
        if parse_path.exists():
            _parse_csv(parse_path)

    return {
        "date": date_value,
        "ledger_rows": _int_value(payload.get("ledger_rows")),
        "current_projection_rows": _int_value(payload.get("current_projection_rows")),
        "current_eligible_rows": _int_value(payload.get("current_eligible_rows")),
        "late_discovered_current_rows": _int_value(payload.get("current_late_discovered_rows")),
        "current_eligible_late_discovered_rows": _int_value(
            payload.get("current_eligible_late_discovered_candidates")
        ),
        "disappeared_rows": _int_value(payload.get("historical_disappeared_rows") or payload.get("disappeared_candidates")),
        "reappeared_rows": _int_value(payload.get("reappeared_rows")),
        "hits_15_morning": _int_value(payload.get("hits_15_morning_count")),
        "hits_15_late_discovered": _int_value(payload.get("hits_15_late_discovered_count")),
        "hits_15_current_eligible": _int_value(payload.get("hits_15_current_eligible_count")),
        "confirmed_lineup_overlay_count": _int_value(payload.get("confirmed_lineup_overlay_count")),
        "delta_summary_csv": str(delta_path),
        "pivot_source_csv": str(pivot_path),
        "observation_md": str(payload.get("rolling_observation_md") or day_dir / f"rolling_market_late_candidate_observation_{date_value}.md"),
        "ops_brief_input_json": str(ops_path),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, *, index_date: str, root: Path, rows: list[dict[str, Any]], csv_path: Path) -> None:
    total_late_current = sum(_int_value(row.get("late_discovered_current_rows")) for row in rows)
    total_current_eligible_late = sum(_int_value(row.get("current_eligible_late_discovered_rows")) for row in rows)
    total_hits15_late = sum(_int_value(row.get("hits_15_late_discovered")) for row in rows)
    lines = [
        f"# Rolling Observation Multi-Day Index - {index_date}",
        "",
        "## Scope",
        "",
        "Read-only index over local rolling market-late observation folders. No OddsAPI calls, DB writes, model changes, upload changes, or immutable run-tagged artifact rewrites are performed.",
        "",
        f"- Observation root: `{root}`",
        f"- Index CSV: `{csv_path}`",
        f"- Days indexed: `{len(rows)}`",
        f"- Total late-discovered current rows: `{total_late_current}`",
        f"- Total current eligible late-discovered rows: `{total_current_eligible_late}`",
        f"- Total Hits 1.5 late-discovered rows: `{total_hits15_late}`",
        "",
        "## Daily Scoreboard",
        "",
        "| date | current | eligible | late current | eligible late | disappeared | reappeared | hits 1.5 morning | hits 1.5 late | hits 1.5 eligible |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['date']}` | `{row['current_projection_rows']}` | `{row['current_eligible_rows']}` | "
            f"`{row['late_discovered_current_rows']}` | `{row['current_eligible_late_discovered_rows']}` | "
            f"`{row['disappeared_rows']}` | `{row['reappeared_rows']}` | `{row['hits_15_morning']}` | "
            f"`{row['hits_15_late_discovered']}` | `{row['hits_15_current_eligible']}` |"
        )
    lines.extend(
        [
            "",
            "## Use",
            "",
            "Use this file as the daily scoreboard for the pre-All-Star-break rolling observation window. The per-day delta CSV remains the row-level pivot source for morning/midday/late/post-start candidate movement.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def build_index(index_date: str, root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for date_value, day_dir in _candidate_dirs(root, index_date):
        row = _row_for_day(date_value, day_dir)
        if row is not None:
            rows.append(row)
    csv_path = root / f"rolling_observation_multi_day_index_{index_date}.csv"
    md_path = root / f"rolling_observation_multi_day_index_{index_date}.md"
    _write_csv(csv_path, rows)
    _write_markdown(md_path, index_date=index_date, root=root, rows=rows, csv_path=csv_path)
    return {
        "index_date": index_date,
        "days_indexed": len(rows),
        "out_csv": str(csv_path),
        "out_md": str(md_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only multi-day rolling observation index.")
    parser.add_argument("--index-date", required=True)
    parser.add_argument(
        "--root",
        default="artifacts/analysis/mlb/market_late_candidate_discovery",
    )
    args = parser.parse_args()
    index_date = _date_key(args.index_date)
    if not index_date:
        raise SystemExit(f"invalid --index-date: {args.index_date}")
    result = build_index(index_date=index_date, root=Path(args.root))
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
