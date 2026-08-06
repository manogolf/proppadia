#!/usr/bin/env python3
"""Passively retain PA foundation fields in MLB research/diagnostic artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())

DIRECT_PA_FIELDS = [
    "plate_appearances",
    "hit_by_pitch",
    "sacrifice_flies",
    "sacrifice_hits",
    "catcher_interference",
    "pa_source",
    "pa_backfilled_at",
]
ROLLING_PA_FIELDS = [
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
]
PA_OUTPUT_FIELDS = [
    *DIRECT_PA_FIELDS,
    *ROLLING_PA_FIELDS,
    "pa_context_date",
    "pa_retention_source",
    "pa_retention_status",
]

DEFAULT_TARGETS = [
    "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{date}.csv",
    "artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv",
    "artifacts/analysis/mlb/environment_v2/daily/{date}/environment_v2_beta_daily_profiles_{date}.csv",
    "artifacts/analysis/mlb/execution_vs_model/{completed_date}/reconcile_rows.csv",
]

DIAGNOSTIC_COPY_SOURCES = [
    (
        "backend/mlb/exports/model_v2/lanes/today/{date}/hits_lane_selector_{date}_pa_context.csv",
        "backend/mlb/exports/model_v2/lanes/today/{date}/hits_lane_selector_{date}.csv",
    ),
    (
        "backend/mlb/exports/model_v2/lanes/today/{date}/quick_card_hits_{date}_pa_context.csv",
        "backend/mlb/exports/model_v2/lanes/today/{date}/quick_card_hits_{date}.csv",
    ),
    (
        "backend/mlb/data/processed/mlb_slate_output_pa_context_{date}.csv",
        "backend/mlb/data/processed/mlb_slate_output.csv",
    ),
    (
        "backend/mlb/data/processed/mlb_predictions_wide_calibrated_pa_context_{date}.csv",
        "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv",
    ),
]

EXAMPLE_SOURCES = [
    (
        "lane_selector_pa_context_{date}.csv",
        "backend/mlb/exports/model_v2/lanes/today/{date}/hits_lane_selector_{date}_pa_context.csv",
    ),
    (
        "quick_card_pa_context_{date}.csv",
        "backend/mlb/exports/model_v2/lanes/today/{date}/quick_card_hits_{date}_pa_context.csv",
    ),
    (
        "current_slate_pa_context_{date}.csv",
        "backend/mlb/data/processed/mlb_slate_output_pa_context_{date}.csv",
    ),
    (
        "predictions_wide_pa_context_{date}.csv",
        "backend/mlb/data/processed/mlb_predictions_wide_calibrated_pa_context_{date}.csv",
    ),
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null", "<na>"}


def _i(value: Any) -> int | None:
    if _is_empty(value):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _row_date(row: dict[str, Any], fallback: str) -> str:
    return str(row.get("game_date") or row.get("date") or row.get("slate_date") or row.get("board_date") or fallback)[:10]


def _player_id(row: dict[str, Any]) -> int | None:
    return _i(row.get("player_id") or row.get("canonical_player_id") or row.get("mlb_player_id"))


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        return [dict(row) for row in reader], fields


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    _write_csv(path, rows, fields)


def _load_pa_context(start_date: str, end_date: str, player_ids: list[int]) -> dict[tuple[str, int], dict[str, Any]]:
    if not player_ids:
        return {}
    rows = pg_fetchall(
        """
WITH wanted AS (
  SELECT unnest(%s::bigint[]) AS player_id
),
dates AS (
  SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS artifact_date
),
rolling AS (
  SELECT
    d.artifact_date,
    pds.player_id,
    pds.game_date::date AS pa_context_date,
    pds.d7_plate_appearances::float8 AS d7_plate_appearances,
    pds.d15_plate_appearances::float8 AS d15_plate_appearances,
    pds.d30_plate_appearances::float8 AS d30_plate_appearances,
    ROW_NUMBER() OVER (
      PARTITION BY d.artifact_date, pds.player_id
      ORDER BY pds.game_date DESC
    ) AS rn
  FROM dates d
  JOIN mlb.player_derived_stats pds
    ON pds.game_date < d.artifact_date
   AND pds.player_id IN (SELECT player_id FROM wanted)
  WHERE pds.d7_plate_appearances IS NOT NULL
     OR pds.d15_plate_appearances IS NOT NULL
     OR pds.d30_plate_appearances IS NOT NULL
)
SELECT
  r.artifact_date,
  r.player_id,
  NULL::integer AS plate_appearances,
  NULL::integer AS hit_by_pitch,
  NULL::integer AS sacrifice_flies,
  NULL::integer AS sacrifice_hits,
  NULL::integer AS catcher_interference,
  NULL::text AS pa_source,
  NULL::timestamptz AS pa_backfilled_at,
  r.d7_plate_appearances,
  r.d15_plate_appearances,
  r.d30_plate_appearances,
  r.pa_context_date
FROM rolling r
WHERE r.rn = 1
""",
        (player_ids, start_date, end_date),
    )
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows or []:
        player_id = _i(row.get("player_id"))
        artifact_date = str(row.get("artifact_date") or "")[:10]
        if player_id is None or not artifact_date:
            continue
        out[(artifact_date, player_id)] = dict(row)
    return out


def _date_bounds(rows: list[dict[str, Any]], fallback: str) -> tuple[str, str]:
    dates = sorted({_row_date(row, fallback) for row in rows if _row_date(row, fallback)})
    return (dates[0], dates[-1]) if dates else (fallback, fallback)


def _hydrate_file(path: Path, fallback_date: str, pa_context: dict[tuple[str, int], dict[str, Any]], *, write: bool) -> dict[str, Any]:
    rows, fields = _read_csv(path)
    if not path.exists():
        return {
            "path": _rel(path),
            "exists": False,
            "rows": 0,
            "rows_with_player_id": 0,
            "rows_with_rolling_pa": 0,
            "rows_with_direct_pa": 0,
            "cells_written": 0,
            "columns_added": 0,
            "status": "missing_file",
        }
    original_count = len(rows)
    columns_added = 0
    for field in PA_OUTPUT_FIELDS:
        if field not in fields:
            fields.append(field)
            columns_added += 1
            for row in rows:
                row[field] = ""

    cells_written = 0
    rows_with_player_id = 0
    rows_with_rolling = 0
    rows_with_direct = 0
    missing_source = 0
    for row in rows:
        player_id = _player_id(row)
        if player_id is None:
            row["pa_retention_status"] = row.get("pa_retention_status") or "missing_player_id"
            continue
        rows_with_player_id += 1
        row_date = _row_date(row, fallback_date)
        ctx = pa_context.get((row_date, player_id))
        if not ctx:
            missing_source += 1
            if _is_empty(row.get("pa_retention_status")):
                row["pa_retention_status"] = "missing_pa_source"
                cells_written += 1
            continue
        direct_present = not _is_empty(ctx.get("plate_appearances"))
        rolling_present = any(not _is_empty(ctx.get(field)) for field in ROLLING_PA_FIELDS)
        if direct_present:
            rows_with_direct += 1
        if rolling_present:
            rows_with_rolling += 1
        for field in PA_OUTPUT_FIELDS:
            if field == "pa_retention_source":
                value = "mlb.player_stats+mlb.player_derived_stats"
            elif field == "pa_retention_status":
                value = "retained"
            else:
                value = ctx.get(field)
            if not _is_empty(value) and _is_empty(row.get(field)):
                row[field] = value
                cells_written += 1

    if len(rows) != original_count:
        raise RuntimeError(f"Row count changed for {_rel(path)}")
    if write:
        _write_csv(path, rows, fields)
    return {
        "path": _rel(path),
        "exists": True,
        "rows": len(rows),
        "rows_with_player_id": rows_with_player_id,
        "rows_with_rolling_pa": rows_with_rolling,
        "rolling_pa_coverage_pct": round(100.0 * rows_with_rolling / len(rows), 2) if rows else None,
        "rows_with_direct_pa": rows_with_direct,
        "direct_pa_coverage_pct": round(100.0 * rows_with_direct / len(rows), 2) if rows else None,
        "missing_source_rows": missing_source,
        "cells_written": cells_written if write else 0,
        "cells_to_write": cells_written,
        "columns_added": columns_added,
        "status": "hydrated" if write else "dry_run",
    }


def _hydrate_rows_copy(
    *,
    date_text: str,
    source: Path,
    out_path: Path,
    pa_context: dict[tuple[str, int], dict[str, Any]],
) -> dict[str, Any]:
    rows, _ = _read_csv(source)
    if not source.exists():
        return {"path": _rel(source), "copy_path": _rel(out_path), "exists": False, "rows": 0, "status": "missing_file"}
    out_rows: list[dict[str, Any]] = []
    rows_with_rolling = 0
    rows_with_direct = 0
    for row in rows:
        item = dict(row)
        player_id = _player_id(item)
        ctx = pa_context.get((date_text, player_id or -1)) if player_id is not None else None
        for field in PA_OUTPUT_FIELDS:
            item[field] = ""
        if ctx:
            for field in PA_OUTPUT_FIELDS:
                if field == "pa_retention_source":
                    item[field] = "mlb.player_stats+mlb.player_derived_stats"
                elif field == "pa_retention_status":
                    item[field] = "retained"
                else:
                    item[field] = ctx.get(field, "")
            if any(not _is_empty(item.get(field)) for field in ROLLING_PA_FIELDS):
                rows_with_rolling += 1
            if not _is_empty(item.get("plate_appearances")):
                rows_with_direct += 1
        else:
            item["pa_retention_status"] = "missing_pa_source" if player_id is not None else "missing_player_id"
        out_rows.append(item)
    _write_rows(out_path, out_rows)
    return {
        "path": _rel(source),
        "copy_path": _rel(out_path),
        "exists": True,
        "rows": len(out_rows),
        "rows_with_rolling_pa": rows_with_rolling,
        "rows_with_direct_pa": rows_with_direct,
        "status": "copy_written",
    }


def _write_diagnostic_copies(
    *,
    date_text: str,
    pa_context: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for out_template, source_template in DIAGNOSTIC_COPY_SOURCES:
        reports.append(
            _hydrate_rows_copy(
                date_text=date_text,
                source=ROOT / source_template.format(date=date_text),
                out_path=ROOT / out_template.format(date=date_text),
                pa_context=pa_context,
            )
        )
    return reports


def _write_examples(
    *,
    date_text: str,
    examples_dir: Path,
    pa_context: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for filename_template, source_template in EXAMPLE_SOURCES:
        report = _hydrate_rows_copy(
            date_text=date_text,
            source=ROOT / source_template.format(date=date_text),
            out_path=examples_dir / filename_template.format(date=date_text),
            pa_context=pa_context,
        )
        report["example_path"] = report.pop("copy_path", "")
        report["status"] = "example_written" if report.get("exists") else report.get("status")
        reports.append(report)
    return reports


def run(args: argparse.Namespace) -> int:
    date_text = args.date
    completed_date = args.completed_date or date_text
    target_paths = [ROOT / item.format(date=date_text, completed_date=completed_date) for item in DEFAULT_TARGETS]
    all_rows: list[dict[str, Any]] = []
    for path in target_paths:
        rows, _ = _read_csv(path)
        all_rows.extend(rows)
    for _, source_template in [*DIAGNOSTIC_COPY_SOURCES, *EXAMPLE_SOURCES]:
        rows, _ = _read_csv(ROOT / source_template.format(date=date_text))
        all_rows.extend(rows)
    start_date, end_date = _date_bounds(all_rows, date_text)
    player_ids = sorted({player_id for row in all_rows if (player_id := _player_id(row)) is not None})
    pa_context = _load_pa_context(start_date, end_date, player_ids)

    reports = [
        _hydrate_file(path, date_text, pa_context, write=args.write)
        for path in target_paths
    ]
    diagnostic_copies = _write_diagnostic_copies(
        date_text=date_text,
        pa_context=pa_context,
    )
    examples = _write_examples(
        date_text=date_text,
        examples_dir=ROOT / args.examples_dir,
        pa_context=pa_context,
    )
    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_rows(out_dir / f"pa_foundation_propagation_report_{date_text}.csv", reports)
    _write_rows(out_dir / f"pa_foundation_propagation_diagnostics_{date_text}.csv", diagnostic_copies)
    _write_rows(out_dir / f"pa_foundation_propagation_examples_{date_text}.csv", examples)
    summary = {
        "date": date_text,
        "completed_date": completed_date,
        "generated_at": _utc_now(),
        "mode": "write" if args.write else "dry_run",
        "targets": len(reports),
        "targets_existing": sum(1 for row in reports if row.get("exists")),
        "rows_checked": sum(int(row.get("rows") or 0) for row in reports),
        "rows_with_rolling_pa": sum(int(row.get("rows_with_rolling_pa") or 0) for row in reports),
        "rows_with_direct_pa": sum(int(row.get("rows_with_direct_pa") or 0) for row in reports),
        "cells_written": sum(int(row.get("cells_written") or 0) for row in reports),
        "diagnostic_copies_written": sum(1 for row in diagnostic_copies if row.get("status") == "copy_written"),
        "examples_written": sum(1 for row in examples if row.get("status") == "example_written"),
        "pa_context_rows": len(pa_context),
        "player_ids": len(player_ids),
        "date_bounds": {"start": start_date, "end": end_date},
    }
    (out_dir / f"pa_foundation_propagation_summary_{date_text}.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--completed-date", default="")
    parser.add_argument("--out-dir", default="artifacts/analysis/mlb/pa_foundation")
    parser.add_argument("--examples-dir", default="artifacts/analysis/mlb/pa_foundation/examples")
    parser.add_argument("--write", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
