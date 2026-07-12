#!/usr/bin/env python3
"""Rehydrate passive rolling context into current execution reconcile artifacts.

This is an artifact-lineage repair utility. It reads existing
artifacts/analysis/mlb/execution_vs_model/<date>/reconcile_rows.csv files and
fills only empty rolling-context cells from mlb.player_derived_stats. It does
not rebuild reconcile, change row counts, touch final upload files, or modify
prices/outcomes/grading fields.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/mlb/feature_lineage"
DEFAULT_RECONCILE_ROOT = ROOT / "artifacts/analysis/mlb/execution_vs_model"

ROLLING_CONTEXT_FIELDS = [
    "rolling_result_avg_7",
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_strikeouts_batting",
    "d15_strikeouts_batting",
    "d30_strikeouts_batting",
    "d7_hits_allowed",
    "d15_hits_allowed",
    "d30_hits_allowed",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
]

PDS_SOURCE_FIELDS = [
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_strikeouts_batting",
    "d15_strikeouts_batting",
    "d30_strikeouts_batting",
    "d7_hits_allowed",
    "d15_hits_allowed",
    "d30_hits_allowed",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "d7_doubles",
    "d7_triples",
    "d7_singles",
    "d7_stolen_bases",
    "d7_home_runs",
    "d7_walks",
    "d7_runs_scored",
    "d7_rbis",
    "d7_runs_rbis",
    "d7_strikeouts_pitching",
    "d7_outs_recorded",
    "d7_walks_allowed",
    "d7_earned_runs",
]

ROLLING_RESULT_AVG_7_BY_PROP = {
    "hits": "d7_hits",
    "total_bases": "d7_total_bases",
    "strikeouts_batting": "d7_strikeouts_batting",
    "earned_runs": "d7_earned_runs",
    "doubles": "d7_doubles",
    "triples": "d7_triples",
    "singles": "d7_singles",
    "stolen_bases": "d7_stolen_bases",
    "home_runs": "d7_home_runs",
    "hits_allowed": "d7_hits_allowed",
    "strikeouts_pitching": "d7_strikeouts_pitching",
    "outs_recorded": "d7_outs_recorded",
    "walks": "d7_walks",
    "hits_runs_rbis": "d7_hits_runs_rbis",
    "runs_scored": "d7_runs_scored",
    "walks_allowed": "d7_walks_allowed",
    "runs_rbis": "d7_runs_rbis",
    "rbis": "d7_rbis",
}

PROTECTED_SUBSTRINGS = (
    "actual_",
    "outcome",
    "result",
    "pnl",
    "price",
    "odds",
    "implied",
    "market_",
    "book",
    "grade",
    "match",
    "upload",
    "model_prob",
    "fair_",
    "pick",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _dates(start: str, end: str) -> list[str]:
    cur = _parse_date(start)
    stop = _parse_date(end)
    out: list[str] = []
    while cur <= stop:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _f(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if text == "" or text.lower() in {"nan", "none", "null"}:
            return None
        value_f = float(text)
        if math.isnan(value_f):
            return None
        return value_f
    except Exception:
        return None


def _i(value: Any) -> int | None:
    value_f = _f(value)
    if value_f is None:
        return None
    return int(value_f)


def _is_empty(value: Any) -> bool:
    return _f(value) is None


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        return [dict(row) for row in reader], fields


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _stable_hash(rows: list[dict[str, Any]], fields: list[str]) -> str:
    data = "\n".join("\t".join(str(row.get(col, "")) for col in fields) for row in rows)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _protected_fields(fields: list[str]) -> list[str]:
    out = []
    for col in fields:
        if col in ROLLING_CONTEXT_FIELDS:
            continue
        low = col.lower()
        if any(token in low for token in PROTECTED_SUBSTRINGS):
            out.append(col)
    return out


def _table_columns() -> set[str]:
    rows = pg_fetchall(
        """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'mlb'
  AND table_name = 'player_derived_stats'
"""
    )
    return {str(row.get("column_name")) for row in rows or []}


def _source_select_fields(existing_cols: set[str]) -> list[str]:
    return [col for col in PDS_SOURCE_FIELDS if col in existing_cols]


def _load_pds_context(
    *,
    start_date: str,
    end_date: str,
    player_ids: list[int],
    source_fields: list[str],
) -> dict[tuple[str, int], dict[str, Any]]:
    if not player_ids or not source_fields:
        return {}
    select_cols = ",\n  ".join(f"pds.{col}::float8 AS {col}" for col in source_fields)
    rows = pg_fetchall(
        f"""
WITH dates AS (
  SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS artifact_date
),
ranked AS (
SELECT
  d.artifact_date,
  pds.game_date::date AS game_date,
  pds.player_id,
  {select_cols},
  ROW_NUMBER() OVER (
    PARTITION BY d.artifact_date, pds.player_id
    ORDER BY pds.game_date DESC
  ) AS rn
FROM dates d
JOIN mlb.player_derived_stats pds
  ON pds.game_date < d.artifact_date
 AND pds.player_id = ANY(%s)
)
SELECT *
FROM ranked
WHERE rn = 1
""",
        (start_date, end_date, player_ids),
    )
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows or []:
        player_id = _i(row.get("player_id"))
        date_text = str(row.get("artifact_date") or "")[:10]
        if player_id is None or not date_text:
            continue
        out[(date_text, player_id)] = dict(row)
    return out


def _date_from_row(row: dict[str, Any], fallback: str) -> str:
    return str(row.get("game_date") or row.get("slate_date") or row.get("date") or fallback)[:10]


def _coverage_row(
    *,
    date_value: str,
    path: Path,
    rows: list[dict[str, Any]],
    pds_context: dict[tuple[str, int], dict[str, Any]],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "date": date_value,
        "path": _rel(path),
        "rows": len(rows),
        "unresolved_rows": sum(
            1
            for item in rows
            if _clean(item.get("actual_over_outcome")) not in {"win", "loss", "push"}
            and _clean(item.get("actual_under_outcome")) not in {"win", "loss", "push"}
            and _clean(item.get("actual_model_pick_outcome")) not in {"win", "loss", "push"}
        ),
    }
    for col in ROLLING_CONTEXT_FIELDS:
        nonnull = sum(1 for item in rows if not _is_empty(item.get(col)))
        row[f"{col}_nonnull"] = nonnull
        row[f"{col}_nonnull_rate"] = nonnull / len(rows) if rows else None
    source_missing = 0
    for item in rows:
        player_id = _i(item.get("player_id"))
        key = (_date_from_row(item, date_value), player_id or -1)
        if player_id is None or key not in pds_context:
            source_missing += 1
    row["source_missing_rows"] = source_missing
    return row


def _source_value(row: dict[str, Any], src: dict[str, Any], col: str) -> Any:
    if col == "rolling_result_avg_7":
        source_col = ROLLING_RESULT_AVG_7_BY_PROP.get(_clean(row.get("prop_type")))
        return src.get(source_col) if source_col else None
    return src.get(col)


def _process_file(
    *,
    date_value: str,
    path: Path,
    pds_context: dict[tuple[str, int], dict[str, Any]],
    write: bool,
    out_dir: Path,
    backup_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    rows, fields = _read_csv(path)
    original_row_count = len(rows)
    original_fields = list(fields)
    protected = _protected_fields(fields)
    protected_before = _stable_hash(rows, protected)
    non_context = [col for col in fields if col not in ROLLING_CONTEXT_FIELDS]
    non_context_before = _stable_hash(rows, non_context)

    added_columns = []
    for col in ROLLING_CONTEXT_FIELDS:
        if col not in fields:
            fields.append(col)
            added_columns.append(col)
            for item in rows:
                item[col] = ""

    rows_with_source = 0
    rows_affected = 0
    cells_to_write = 0
    cells_written = 0
    skipped_existing_nonnull = 0
    missing_source_rows = 0
    missing_source_cells = 0
    per_field_cells = {col: 0 for col in ROLLING_CONTEXT_FIELDS}

    for item in rows:
        player_id = _i(item.get("player_id"))
        source = pds_context.get((_date_from_row(item, date_value), player_id or -1)) if player_id is not None else None
        if not source:
            missing_source_rows += 1
            continue
        rows_with_source += 1
        changed_row = False
        for col in ROLLING_CONTEXT_FIELDS:
            source_value = _source_value(item, source, col)
            if _f(source_value) is None:
                if _is_empty(item.get(col)):
                    missing_source_cells += 1
                continue
            if _is_empty(item.get(col)):
                cells_to_write += 1
                per_field_cells[col] += 1
                changed_row = True
                if write:
                    item[col] = source_value
                    cells_written += 1
            else:
                skipped_existing_nonnull += 1
        if changed_row:
            rows_affected += 1

    protected_after = _stable_hash(rows, protected)
    non_context_after = _stable_hash(rows, non_context)
    unsafe = []
    if len(rows) != original_row_count:
        unsafe.append("row_count_changed")
    if protected_before != protected_after:
        unsafe.append("protected_fields_changed")
    if non_context_before != non_context_after:
        unsafe.append("non_context_fields_changed")

    if unsafe:
        raise RuntimeError(f"Unsafe changes detected for {_rel(path)}: {', '.join(unsafe)}")

    if write and cells_written:
        backup_path = backup_dir / _rel(path).replace("/", "__")
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup_path)
        _write_csv(path, rows, fields)

    report = {
        "date": date_value,
        "path": _rel(path),
        "file_exists": True,
        "row_count": original_row_count,
        "added_columns": ",".join(added_columns),
        "rows_with_source": rows_with_source,
        "missing_source_rows": missing_source_rows,
        "rows_affected": rows_affected,
        "cells_to_write": cells_to_write,
        "cells_written": cells_written,
        "skipped_existing_nonnull_cells": skipped_existing_nonnull,
        "missing_source_cells": missing_source_cells,
        "unsafe_non_context_changes": 0,
        "source": "mlb.player_derived_stats",
        **{f"{col}_cells_to_write": per_field_cells[col] for col in ROLLING_CONTEXT_FIELDS},
    }
    return report, _coverage_row(date_value=date_value, path=path, rows=rows, pds_context=pds_context)


def _write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_plan(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Reconcile Rolling Context Rehydrate Plan",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Date range: `{payload['start_date']}` through `{payload['end_date']}`",
        "- Target artifacts: `artifacts/analysis/mlb/execution_vs_model/<date>/reconcile_rows.csv`",
        "- Source: `mlb.player_derived_stats`, joined by latest `pds.game_date < artifact_date` plus `player_id`.",
        "- Mode: fill missing/null rolling-context cells only; existing non-null context is preserved.",
        "- Guardrails: row counts, protected fields, and non-context fields are hash-checked before write.",
        "- Final 8rain upload files are not touched.",
        "",
        "## Fields",
        "",
    ]
    lines.extend(f"- `{col}`" for col in ROLLING_CONTEXT_FIELDS)
    lines.extend(
        [
            "",
            "## Local Commands",
            "",
            "```bash",
            "make mlb-rehydrate-reconcile-rolling-context DATE_FROM=2026-04-01 DATE_TO=2026-06-14 DRY_RUN=1",
            "make mlb-rehydrate-reconcile-rolling-context DATE_FROM=2026-04-01 DATE_TO=2026-06-14 DRY_RUN=0",
            "make mlb-rehydrate-reconcile-rolling-context DATE_FROM=2026-04-01 DATE_TO=2026-06-14 DRY_RUN=1",
            "make mlb-refresh-hits-15-tier-backtest",
            "make mlb-review-aid-performance",
            "```",
            "",
            "## Success Shape",
            "",
            "- Dry run reports `unsafe_non_context_changes = 0`.",
            "- Write reports `unsafe_non_context_changes = 0` and non-zero `cells_written` when gaps exist.",
            "- Post-write dry run reports `cells_to_write = 0` for rows whose source exists.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    reconcile_root = Path(args.reconcile_root)
    generated_at = _utc_now()
    dates = _dates(args.start_date, args.end_date)
    files = [(date_value, reconcile_root / date_value / "reconcile_rows.csv") for date_value in dates]
    existing_files = [(date_value, path) for date_value, path in files if path.exists()]

    all_rows_for_ids: list[dict[str, Any]] = []
    missing_files = []
    for date_value, path in files:
        if not path.exists():
            missing_files.append({"date": date_value, "path": _rel(path), "reason": "file_missing"})
            continue
        rows, _ = _read_csv(path)
        all_rows_for_ids.extend(rows)

    player_ids = sorted({int(pid) for row in all_rows_for_ids if (pid := _i(row.get("player_id"))) is not None})
    table_cols = _table_columns()
    source_fields = _source_select_fields(table_cols)
    pds_context = _load_pds_context(
        start_date=args.start_date,
        end_date=args.end_date,
        player_ids=player_ids,
        source_fields=source_fields,
    )

    backup_dir = out_dir / "reconcile_rolling_context_rehydrate_backups" / generated_at.replace(":", "").replace("-", "")
    file_reports: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    for date_value, path in existing_files:
        report, coverage = _process_file(
            date_value=date_value,
            path=path,
            pds_context=pds_context,
            write=args.write,
            out_dir=out_dir,
            backup_dir=backup_dir,
        )
        file_reports.append(report)
        coverage_rows.append(coverage)

    summary = {
        "generated_at": generated_at,
        "mode": "write" if args.write else "dry_run",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "files_checked": len(files),
        "files_existing": len(existing_files),
        "files_missing": len(missing_files),
        "player_ids_queried": len(player_ids),
        "pds_context_rows": len(pds_context),
        "pds_source_fields": source_fields,
        "rolling_context_fields": ROLLING_CONTEXT_FIELDS,
        "files_with_changes": sum(1 for row in file_reports if int(row.get("cells_to_write") or 0) > 0),
        "rows_affected": sum(int(row.get("rows_affected") or 0) for row in file_reports),
        "cells_to_write": sum(int(row.get("cells_to_write") or 0) for row in file_reports),
        "cells_written": sum(int(row.get("cells_written") or 0) for row in file_reports),
        "unsafe_non_context_changes": sum(int(row.get("unsafe_non_context_changes") or 0) for row in file_reports),
        "missing_files": missing_files,
    }

    _write_plan(
        out_dir / "reconcile_rolling_context_rehydrate_plan.md",
        {
            "generated_at": generated_at,
            "start_date": args.start_date,
            "end_date": args.end_date,
        },
    )
    _write_csv_rows(out_dir / "reconcile_rolling_context_gap_audit.csv", file_reports + missing_files)
    _write_json(
        out_dir
        / (
            "reconcile_rolling_context_rehydrate_write_summary.json"
            if args.write
            else "reconcile_rolling_context_rehydrate_dry_run_summary.json"
        ),
        summary,
    )
    _write_csv_rows(out_dir / "reconcile_rolling_context_post_verify.csv", coverage_rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
