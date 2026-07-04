#!/usr/bin/env python3
"""Report MLB PA foundation source health and passive downstream coverage."""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import date, datetime, timedelta, timezone
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

PA_FIELDS = DIRECT_PA_FIELDS + ROLLING_PA_FIELDS

REVIEW_AID_FILES = [
    "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{date}.csv",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null"}


def _as_int(value: Any) -> int | None:
    if _is_empty(value):
        return None
    try:
        value_f = float(str(value).strip())
        if math.isnan(value_f):
            return None
        return int(value_f)
    except Exception:
        return None


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        return [dict(row) for row in reader], fields


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _artifact_paths(date_value: str) -> list[dict[str, Any]]:
    previous_date = (_parse_date(date_value) - timedelta(days=1)).isoformat()
    paths = [
        (
            "current_slate_output",
            "backend/mlb/data/processed/mlb_slate_output.csv",
            False,
            "intentionally_excluded_source_schema",
        ),
        (
            "current_slate_output_pa_context",
            f"backend/mlb/data/processed/mlb_slate_output_pa_context_{date_value}.csv",
            True,
            "generator_owned_non_upload_diagnostic",
        ),
        (
            "predictions_wide",
            "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv",
            False,
            "intentionally_excluded_model_source_schema",
        ),
        (
            "predictions_wide_pa_context",
            f"backend/mlb/data/processed/mlb_predictions_wide_calibrated_pa_context_{date_value}.csv",
            True,
            "generator_owned_non_upload_diagnostic",
        ),
        (
            "lane_selector",
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}.csv",
            False,
            "intentionally_excluded_selector_schema",
        ),
        (
            "lane_selector_pa_context",
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}_pa_context.csv",
            True,
            "generator_owned_non_upload_diagnostic",
        ),
        (
            "quick_card",
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/quick_card_hits_{date_value}.csv",
            False,
            "intentionally_excluded_quick_card_schema",
        ),
        (
            "quick_card_pa_context",
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/quick_card_hits_{date_value}_pa_context.csv",
            True,
            "generator_owned_non_upload_diagnostic",
        ),
        ("expanded_o15_universe", "artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv", True, "research_output"),
        ("execution_reconcile_current_date", f"artifacts/analysis/mlb/execution_vs_model/{date_value}/reconcile_rows.csv", False, "pending_completed_slate"),
        ("execution_reconcile_previous_date", f"artifacts/analysis/mlb/execution_vs_model/{previous_date}/reconcile_rows.csv", True, "reconcile_linked_research"),
        ("opportunity_stability", "artifacts/analysis/mlb/research_gap_analysis/player_opportunity_stability_audit.csv", False, "legacy_research_output_not_yet_generator_owned"),
        ("ops_brief_latest", "artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md", False, "text_surface_not_pa_target"),
        ("morning_workflow_audit", "artifacts/analysis/mlb/morning_workflow/morning_workflow_audit_latest.json", False, "workflow_health_not_pa_target"),
        ("environment_v2_daily_profiles", f"artifacts/analysis/mlb/environment_v2/daily/{date_value}/environment_v2_beta_daily_profiles_{date_value}.csv", True, "research_output"),
        (
            "ranking_tool_upload_final",
            f"backend/mlb/exports/model_v2/upload/{date_value}/ranking_tool_upload_{date_value}.csv",
            False,
            "final_upload_schema_intentionally_excluded",
        ),
        (
            "quick_card_tool_upload_final",
            f"backend/mlb/exports/model_v2/upload/{date_value}/quick_card_tool_upload_{date_value}.csv",
            False,
            "final_upload_schema_intentionally_excluded",
        ),
    ]
    for template in REVIEW_AID_FILES:
        path = template.format(date=date_value)
        name = Path(path).stem.replace(f"_{date_value}", "")
        paths.append((f"review_aid_{name}", path, True, "review_aid_research_output"))
    return [
        {
            "artifact_family": family,
            "path": path,
            "pa_expected": expected,
            "pa_target_role": role,
        }
        for family, path, expected, role in paths
    ]


def _artifact_coverage(date_value: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _artifact_paths(date_value):
        path = ROOT / item["path"]
        data, fields = _read_csv(path) if path.suffix.lower() == ".csv" else ([], [])
        row: dict[str, Any] = {
            "date": date_value,
            "scope": "downstream_artifact",
            "artifact_family": item["artifact_family"],
            "path": item["path"],
            "exists": path.exists(),
            "rows": len(data) if path.suffix.lower() == ".csv" else "",
            "pa_fields_present": 0,
            "pa_fields_expected": len(PA_FIELDS),
            "pa_fields_missing": len(PA_FIELDS),
            "rolling_pa_fields_present": 0,
            "direct_pa_fields_present": 0,
            "pa_expected": bool(item.get("pa_expected")),
            "pa_target_role": item.get("pa_target_role", ""),
            "notes": "",
        }
        if path.suffix.lower() != ".csv":
            text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
            present = [field for field in PA_FIELDS if field in text]
            row["pa_fields_present"] = len(present)
            row["pa_fields_missing"] = len(PA_FIELDS) - len(present)
            row["rolling_pa_fields_present"] = sum(1 for field in ROLLING_PA_FIELDS if field in text)
            row["direct_pa_fields_present"] = sum(1 for field in DIRECT_PA_FIELDS if field in text)
            row["notes"] = "text_scan_only"
        else:
            present = [field for field in PA_FIELDS if field in fields]
            row["pa_fields_present"] = len(present)
            row["pa_fields_missing"] = len(PA_FIELDS) - len(present)
            row["rolling_pa_fields_present"] = sum(1 for field in ROLLING_PA_FIELDS if field in fields)
            row["direct_pa_fields_present"] = sum(1 for field in DIRECT_PA_FIELDS if field in fields)
            for field in PA_FIELDS:
                field_present = field in fields
                nonnull = sum(1 for r in data if not _is_empty(r.get(field))) if field_present else 0
                row[f"{field}_present"] = field_present
                row[f"{field}_nonnull"] = nonnull
                row[f"{field}_nonnull_rate"] = (nonnull / len(data)) if field_present and data else None
        rows.append(row)
    return rows


def _table_columns(schema: str, table: str) -> set[str]:
    rows = pg_fetchall(
        """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = %s
  AND table_name = %s
""",
        (schema, table),
    )
    return {str(row.get("column_name")) for row in rows or []}


def _source_health(date_value: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"db_available": True}
    ps_cols = _table_columns("mlb", "player_stats")
    pds_cols = _table_columns("mlb", "player_derived_stats")
    summary["player_stats_pa_columns_present"] = sorted(col for col in DIRECT_PA_FIELDS if col in ps_cols)
    summary["player_derived_pa_columns_present"] = sorted(col for col in ROLLING_PA_FIELDS if col in pds_cols)

    ps_daily = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  COUNT(*) AS rows,
  COUNT(plate_appearances) AS rows_with_pa,
  COUNT(*) FILTER (WHERE plate_appearances IS NULL) AS rows_missing_pa,
  COUNT(pa_source) AS rows_with_pa_source,
  MAX(pa_backfilled_at) AS max_pa_backfilled_at
FROM mlb.player_stats
WHERE game_date = %s::date
GROUP BY game_date
""",
        (date_value,),
    )
    ps_latest = pg_fetchall(
        """
SELECT
  MAX(game_date)::date AS latest_player_stats_date_with_pa,
  MAX(pa_backfilled_at) AS latest_pa_backfilled_at
FROM mlb.player_stats
WHERE plate_appearances IS NOT NULL
""",
    )
    ps_latest_available = pg_fetchall(
        """
SELECT MAX(game_date)::date AS latest_player_stats_game_date
FROM mlb.player_stats
WHERE game_date <= %s::date
  AND COALESCE(position, '') <> 'P'
""",
        (date_value,),
    )
    ps_missing_by_date = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  COUNT(*) AS rows,
  COUNT(plate_appearances) AS rows_with_pa,
  COUNT(*) FILTER (WHERE plate_appearances IS NULL) AS rows_missing_pa,
  COUNT(pa_source) AS rows_with_pa_source
FROM mlb.player_stats
WHERE game_date >= (%s::date - INTERVAL '14 days')
  AND game_date <= %s::date
GROUP BY game_date
ORDER BY game_date
""",
        (date_value, date_value),
    )
    pds_daily = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  COUNT(*) AS rows,
  COUNT(d7_plate_appearances) AS rows_with_d7_pa,
  COUNT(d15_plate_appearances) AS rows_with_d15_pa,
  COUNT(d30_plate_appearances) AS rows_with_d30_pa
FROM mlb.player_derived_stats
WHERE game_date = %s::date
GROUP BY game_date
""",
        (date_value,),
    )
    pds_latest = pg_fetchall(
        """
SELECT MAX(game_date)::date AS latest_rolling_pa_date
FROM mlb.player_derived_stats
WHERE d7_plate_appearances IS NOT NULL
   OR d15_plate_appearances IS NOT NULL
   OR d30_plate_appearances IS NOT NULL
""",
    )

    latest_ps = dict(ps_latest[0]) if ps_latest else {}
    latest_pds = dict(pds_latest[0]) if pds_latest else {}
    latest_available = dict(ps_latest_available[0]) if ps_latest_available else {}
    summary.update(latest_ps)
    summary.update(latest_available)
    summary.update(latest_pds)
    summary["player_stats_daily_rows"] = int((ps_daily[0] or {}).get("rows") or 0) if ps_daily else 0
    summary["player_stats_daily_rows_with_pa"] = int((ps_daily[0] or {}).get("rows_with_pa") or 0) if ps_daily else 0
    summary["player_stats_daily_rows_missing_pa"] = int((ps_daily[0] or {}).get("rows_missing_pa") or 0) if ps_daily else 0
    summary["player_stats_daily_pa_coverage_pct"] = (
        round(100.0 * summary["player_stats_daily_rows_with_pa"] / summary["player_stats_daily_rows"], 2)
        if summary["player_stats_daily_rows"]
        else None
    )
    summary["rolling_daily_rows"] = int((pds_daily[0] or {}).get("rows") or 0) if pds_daily else 0

    for row in ps_missing_by_date:
        total = int(row.get("rows") or 0)
        with_pa = int(row.get("rows_with_pa") or 0)
        output_rows.append(
            {
                "date": str(row.get("game_date"))[:10],
                "scope": "source_table",
                "artifact_family": "mlb.player_stats",
                "path": "db:mlb.player_stats",
                "exists": True,
                "rows": total,
                "rows_with_pa": with_pa,
                "rows_missing_pa": int(row.get("rows_missing_pa") or 0),
                "pa_coverage_pct": round(100.0 * with_pa / total, 2) if total else None,
                "rows_with_pa_source": int(row.get("rows_with_pa_source") or 0),
                "notes": "last_14_day_source_coverage",
            }
        )

    if pds_daily:
        row = pds_daily[0]
        total = int(row.get("rows") or 0)
        for field, source_col in [
            ("d7_plate_appearances", "rows_with_d7_pa"),
            ("d15_plate_appearances", "rows_with_d15_pa"),
            ("d30_plate_appearances", "rows_with_d30_pa"),
        ]:
            with_value = int(row.get(source_col) or 0)
            output_rows.append(
                {
                    "date": date_value,
                    "scope": "source_table",
                    "artifact_family": "mlb.player_derived_stats",
                    "path": "db:mlb.player_derived_stats",
                    "exists": True,
                    "rows": total,
                    "field": field,
                    "rows_with_pa": with_value,
                    "rows_missing_pa": max(total - with_value, 0),
                    "pa_coverage_pct": round(100.0 * with_value / total, 2) if total else None,
                    "notes": "rolling_pa_daily_source_coverage",
                }
            )
    return output_rows, summary


def _load_pds_for_rows(date_value: str, rows: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    player_ids = sorted(
        {
            player_id
            for row in rows
            if (player_id := _as_int(row.get("player_id") or row.get("canonical_player_id"))) is not None
        }
    )
    if not player_ids:
        return {}
    db_rows = pg_fetchall(
        """
WITH wanted AS (
  SELECT unnest(%s::bigint[]) AS player_id
),
ranked AS (
SELECT
  game_date::date AS game_date,
  player_id,
  d7_plate_appearances::float8 AS d7_plate_appearances,
  d15_plate_appearances::float8 AS d15_plate_appearances,
  d30_plate_appearances::float8 AS d30_plate_appearances,
  ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
FROM mlb.player_derived_stats
WHERE game_date <= %s::date
  AND player_id IN (SELECT player_id FROM wanted)
  AND (
    d7_plate_appearances IS NOT NULL
    OR d15_plate_appearances IS NOT NULL
    OR d30_plate_appearances IS NOT NULL
  )
)
SELECT *
FROM ranked
WHERE rn = 1
""",
        (player_ids, date_value),
    )
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for row in db_rows or []:
        player_id = _as_int(row.get("player_id"))
        row_date = str(row.get("game_date") or "")[:10]
        if player_id is not None and row_date:
            out[(date_value, player_id)] = dict(row)
    return out


def _write_review_aid_pa_pilot(date_value: str, out_path: Path) -> dict[str, Any]:
    source_rows: list[dict[str, Any]] = []
    for template in REVIEW_AID_FILES:
        source_path = ROOT / template.format(date=date_value)
        rows, _ = _read_csv(source_path)
        for row in rows:
            row = dict(row)
            row["pa_pilot_source_artifact"] = _rel(source_path)
            source_rows.append(row)

    pds = _load_pds_for_rows(date_value, source_rows)
    pilot_fields = [
        "date",
        "player_id",
        "canonical_player_id",
        "player_name",
        "team",
        "opponent",
        "board_name",
        "population",
        "combined_tier",
        "pa_pilot_source_artifact",
        "d7_plate_appearances",
        "d15_plate_appearances",
        "d30_plate_appearances",
        "pa_retention_source",
        "pa_retention_status",
    ]
    pilot_rows: list[dict[str, Any]] = []
    for row in source_rows:
        player_id = _as_int(row.get("player_id") or row.get("canonical_player_id"))
        src = pds.get((date_value, player_id or -1)) if player_id is not None else None
        out = {field: row.get(field, "") for field in pilot_fields}
        if src:
            for field in ROLLING_PA_FIELDS:
                out[field] = src.get(field, "")
            out["pa_retention_source"] = "mlb.player_derived_stats"
            out["pa_retention_status"] = "retained"
        else:
            out["pa_retention_source"] = "mlb.player_derived_stats"
            out["pa_retention_status"] = "missing_source"
        pilot_rows.append(out)
    _write_csv(out_path, pilot_rows, pilot_fields)
    retained = sum(1 for row in pilot_rows if row.get("pa_retention_status") == "retained")
    return {
        "path": _rel(out_path),
        "rows": len(pilot_rows),
        "rows_with_pa_source": retained,
        "coverage_pct": round(100.0 * retained / len(pilot_rows), 2) if pilot_rows else None,
    }


def _write_md(path: Path, payload: dict[str, Any], downstream_rows: list[dict[str, Any]]) -> None:
    summary = payload["summary"]
    lines = [
        "# MLB PA Foundation Health",
        "",
        f"- Date: `{payload['date']}`",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Status: `{payload['status']}`",
        f"- Mode: informational/WARN-only; not a production gate.",
        f"- Warnings: `{', '.join(payload.get('warnings') or []) or 'none'}`",
        "",
        "## Source Health",
        "",
        f"- DB available: `{summary.get('db_available')}`",
        f"- Latest player_stats game date available: `{summary.get('latest_player_stats_game_date')}`",
        f"- Latest player_stats date with PA: `{summary.get('latest_player_stats_date_with_pa')}`",
        f"- Latest rolling PA date: `{summary.get('latest_rolling_pa_date')}`",
        f"- Daily player_stats rows: `{summary.get('player_stats_daily_rows')}`",
        f"- Daily rows with PA: `{summary.get('player_stats_daily_rows_with_pa')}`",
        f"- Daily rows missing PA: `{summary.get('player_stats_daily_rows_missing_pa')}`",
        f"- Daily PA coverage: `{summary.get('player_stats_daily_pa_coverage_pct')}`%",
        "",
        "## Downstream Propagation",
        "",
        "| artifact family | role | PA expected | exists | rows | PA fields present | rolling PA fields present |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in downstream_rows:
        lines.append(
            "| {artifact_family} | {pa_target_role} | `{pa_expected}` | `{exists}` | `{rows}` | `{pa_fields_present}/{pa_fields_expected}` | `{rolling_pa_fields_present}/3` |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Passive Retention Pilot",
            "",
            f"- Pilot artifact: `{summary.get('pilot_retention', {}).get('path')}`",
            f"- Pilot rows: `{summary.get('pilot_retention', {}).get('rows')}`",
            f"- Rows with rolling PA source: `{summary.get('pilot_retention', {}).get('rows_with_pa_source')}`",
            f"- Coverage: `{summary.get('pilot_retention', {}).get('coverage_pct')}`%",
            "",
            "## Notes",
            "",
            "- PA is being evaluated as platform lineage/context, not a decision rule.",
            "- Final production upload schemas are unchanged.",
            "- Final upload schemas and selector decision inputs intentionally remain PA-free.",
            "- Phase 4 evaluates PA-context diagnostic/research targets instead of warning on intentionally excluded final schemas.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    out_dir = ROOT / args.out_dir
    date_value = args.date
    generated_at = _utc_now()
    downstream_rows = _artifact_coverage(date_value)
    source_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    warnings: list[str] = []

    try:
        source_rows, source_summary = _source_health(date_value)
        summary.update(source_summary)
    except Exception as exc:
        summary["db_available"] = False
        summary["db_error"] = str(exc)
        warnings.append("pa_source_db_unavailable")

    missing_downstream = [
        row["artifact_family"]
        for row in downstream_rows
        if row.get("pa_expected")
        and row.get("exists")
        and int(row.get("rolling_pa_fields_present") or 0) < len(ROLLING_PA_FIELDS)
    ]
    if missing_downstream:
        warnings.append("rolling_pa_missing_from_expected_downstream_artifacts")
    missing_expected_artifacts = [
        row["artifact_family"]
        for row in downstream_rows
        if row.get("pa_expected") and not row.get("exists")
    ]
    if missing_expected_artifacts:
        warnings.append("expected_pa_downstream_artifacts_missing")

    pilot_summary: dict[str, Any] = {}
    if args.write_pilot:
        try:
            pilot_summary = _write_review_aid_pa_pilot(
                date_value,
                out_dir / f"review_aid_pa_retention_pilot_{date_value}.csv",
            )
        except Exception as exc:
            pilot_summary = {"error": str(exc)}
            warnings.append("pa_retention_pilot_failed")
    summary["pilot_retention"] = pilot_summary

    coverage_rows = source_rows + downstream_rows
    coverage_csv = out_dir / f"pa_foundation_coverage_{date_value}.csv"
    _write_csv(coverage_csv, coverage_rows)
    downstream_csv = out_dir / f"mlb_pa_downstream_coverage_{date_value}.csv"
    _write_csv(downstream_csv, downstream_rows)

    latest_expected = str(summary.get("latest_player_stats_game_date") or "")[:10]
    if summary.get("latest_player_stats_date_with_pa") and latest_expected:
        latest_pa_date = str(summary.get("latest_player_stats_date_with_pa"))[:10]
        if latest_pa_date < latest_expected:
            warnings.append("pa_source_not_current")
    if summary.get("latest_rolling_pa_date") and latest_expected:
        latest_rolling_date = str(summary.get("latest_rolling_pa_date"))[:10]
        if latest_rolling_date < latest_expected:
            warnings.append("rolling_pa_source_not_current")
    if (
        summary.get("db_available")
        and summary.get("player_stats_daily_rows")
        and not summary.get("player_stats_daily_rows_with_pa")
    ):
        warnings.append("no_daily_player_stats_pa_rows")

    status = "pass" if not warnings else "warn"
    payload = {
        "date": date_value,
        "generated_at": generated_at,
        "status": status,
        "warnings": warnings,
        "summary": summary,
        "fields": {
            "direct_pa_fields": DIRECT_PA_FIELDS,
            "rolling_pa_fields": ROLLING_PA_FIELDS,
        },
        "outputs": {
            "coverage_csv": _rel(coverage_csv),
            "downstream_coverage_csv": _rel(downstream_csv),
            "pilot_retention_csv": pilot_summary.get("path") if pilot_summary else "",
        },
    }
    out_json = out_dir / f"pa_foundation_health_{date_value}.json"
    out_md = out_dir / f"pa_foundation_health_{date_value}.md"
    _write_json(out_json, payload)
    _write_md(out_md, payload, downstream_rows)
    print(
        "[mlb-pa-foundation-health] "
        f"date={date_value} status={status} warnings={len(warnings)} out_json={_rel(out_json)}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out-dir", default="artifacts/analysis/mlb/pa_foundation")
    parser.add_argument("--write-pilot", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
