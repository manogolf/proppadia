#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_REVIEW_AIDS_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_PERFORMANCE_DIR = Path("artifacts/analysis/mlb/review_aids/performance")
DEFAULT_EXPANDED_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
DEFAULT_SNAPSHOT_ROOT = Path("artifacts/analysis/mlb/hits_environment_snapshots")
DEFAULT_BACKFILL_JSON_ROOT = Path("artifacts/analysis/mlb/hits_environment_backfill_2026/daily")
DEFAULT_LINEAGE_DIAGNOSTICS_ROOT = Path(
    "artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_dry_run/backups/"
    "backend/mlb/exports/model_v2/lanes/today"
)

COMPONENT_FIELDS = [
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "offense_hits_pg_last7",
    "offense_hits_pg_last15",
    "offense_hits_pg_last30",
    "offense_hits_form_blended",
    "league_offense_hits_form_blended",
    "offense_factor_vs_league",
    "offense_factor_vs_league_clamped",
    "bullpen_hits_allowed_pg_last7",
    "bullpen_hits_allowed_pg_last15",
    "bullpen_hits_allowed_pg_last30",
    "bullpen_hits_allowed_form_blended",
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
]

SOURCE_FIELD_MAP = {
    "pitcher_expected_hits_allowed_weighted": "pitcher_expected_hits_allowed_weighted",
    "pitcher_base": "pitcher_expected_hits_allowed_weighted",
    "offense_hits_pg_last7": "offense_hits_pg_last7",
    "offense_hits_pg_last15": "offense_hits_pg_last15",
    "offense_hits_pg_last30": "offense_hits_pg_last30",
    "offense_hits_form_blended": "offense_hits_form_blended",
    "league_offense_hits_form_blended": "league_offense_hits_form_blended",
    "offense_factor_vs_league": "offense_factor_vs_league",
    "offense_factor_vs_league_clamped": "offense_factor_vs_league_clamped",
    "bullpen_hits_allowed_pg_last7": "bullpen_hits_allowed_pg_last7",
    "bullpen_hits_allowed_pg_last15": "bullpen_hits_allowed_pg_last15",
    "bullpen_hits_allowed_pg_last30": "bullpen_hits_allowed_pg_last30",
    "bullpen_hits_allowed_form_blended": "bullpen_hits_allowed_form_blended",
    "starter_expected_hits_allowed": "expected_hits_allowed_matchup",
    "team_expected_hits_allowed": "expected_team_hits_allowed_matchup",
}

BOARD_PATTERNS = [
    "hits_o15_simple_filter_*.csv",
    "hits_o15_watch_candidates_*.csv",
    "hits_o15_layered_candidates_*.csv",
    "hits_u15_favorite_audit_*.csv",
    "hits_o15_alternate_discovery_*.csv",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    merged_fields = list(fields)
    for field in COMPONENT_FIELDS:
        if field not in merged_fields:
            merged_fields.append(field)
    for row in rows:
        for key in row:
            if key not in merged_fields:
                merged_fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=merged_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in merged_fields})


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _non_null(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    aliases = {
        "AZ": "ARI",
        "CHW": "CWS",
        "KCR": "KC",
        "OAK": "ATH",
        "SDP": "SD",
        "SFG": "SF",
        "TBR": "TB",
        "WSN": "WSH",
        "NYA": "NYY",
        "NYN": "NYM",
        "LA": "LAD",
    }
    return aliases.get(text, text)


def _date_from_filename(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if match:
        return match.group(1)
    for part in path.parts:
        if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", part):
            return part
    return ""


def _snapshot_timestamp(path: Path) -> str:
    match = re.search(r"__(\d{8}T\d{6}Z)", path.name)
    return match.group(1) if match else path.name


def _snapshot_files(snapshot_root: Path) -> list[Path]:
    return sorted(snapshot_root.glob("20??-??-??/mlb_hits_environment_hits_allowed_rows_*.csv"))


def _backfill_json_files(backfill_json_root: Path) -> list[Path]:
    return sorted(backfill_json_root.glob("20??-??-??/mlb_hits_environment_*.json"))


def _lineage_diagnostic_files(lineage_diagnostics_root: Path) -> list[Path]:
    return sorted(lineage_diagnostics_root.glob("20??-??-??/*_environment_diagnostics.csv"))


def _source_components(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field, source_field in SOURCE_FIELD_MAP.items():
        out[field] = row.get(source_field)
    return out


def _source_sort_key(payload: dict[str, Any]) -> tuple[int, str]:
    return (int(payload.get("_source_priority") or 0), str(payload.get("_source_snapshot_timestamp") or ""))


def _iter_dicts(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        rows.append(value)
        for child in value.values():
            rows.extend(_iter_dicts(child))
    elif isinstance(value, list):
        for child in value:
            rows.extend(_iter_dicts(child))
    return rows


def _backfill_json_rows(path: Path) -> tuple[list[dict[str, Any]], list[str], str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], [], ""
    generated_at = str(data.get("generated_at_utc") or data.get("generated_at") or path.name)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in _iter_dicts(data):
        if not isinstance(row, dict):
            continue
        if not row.get("offense_team") or not row.get("pitcher_team"):
            continue
        components = _source_components(row)
        if not any(_non_null(v) for v in components.values()):
            continue
        key = (
            row.get("game_id"),
            row.get("player_id"),
            _team(row.get("offense_team")),
            _team(row.get("pitcher_team")),
            tuple((field, str(components.get(field) or "")) for field in COMPONENT_FIELDS),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(dict(row))
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    return rows, fields, generated_at


def _store_source_payload(
    *,
    row: dict[str, Any],
    source_date: str,
    source_path: Path,
    source_timestamp: str,
    source_kind: str,
    source_priority: int,
    by_starter: dict[tuple[str, str, str, str], dict[str, Any]],
    pair_candidates: dict[tuple[str, str, str], dict[str, dict[str, Any]]],
) -> bool:
    offense_team = _team(row.get("offense_team"))
    pitcher_team = _team(row.get("pitcher_team"))
    starter_id = str(row.get("player_id") or "").strip().replace(".0", "")
    if not source_date or not offense_team or not pitcher_team:
        return False
    components = _source_components(row)
    if not any(_non_null(v) for v in components.values()):
        return False
    payload = {
        **components,
        "_source_path": _rel(source_path),
        "_source_kind": source_kind,
        "_source_priority": source_priority,
        "_source_snapshot_timestamp": source_timestamp,
        "_source_player_id": starter_id,
        "_source_offense_team": offense_team,
        "_source_pitcher_team": pitcher_team,
    }
    if starter_id:
        key4 = (source_date, offense_team, pitcher_team, starter_id)
        if key4 not in by_starter or _source_sort_key(payload) > _source_sort_key(by_starter[key4]):
            by_starter[key4] = payload
    pair_key = (source_date, offense_team, pitcher_team)
    existing = pair_candidates[pair_key].get(starter_id)
    if existing is None or _source_sort_key(payload) > _source_sort_key(existing):
        pair_candidates[pair_key][starter_id] = payload
    return True


def _build_source_index(
    snapshot_root: Path,
    backfill_json_root: Path,
) -> tuple[dict[tuple[str, str, str, str], dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]], list[dict[str, Any]]]:
    by_starter: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    pair_candidates: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    inventory: list[dict[str, Any]] = []
    for path in _snapshot_files(snapshot_root):
        rows, fields = _read_csv(path)
        snapshot_ts = _snapshot_timestamp(path)
        date_text = _date_from_filename(path)
        non_null_by_field = {
            field: sum(1 for row in rows if _non_null(row.get(SOURCE_FIELD_MAP[field])))
            for field in COMPONENT_FIELDS
            if SOURCE_FIELD_MAP[field] in fields
        }
        inventory.append(
            {
                "source_path": _rel(path),
                "source_kind": "snapshot_csv",
                "date": date_text,
                "snapshot_timestamp": snapshot_ts,
                "rows": len(rows),
                "component_columns_present": sum(1 for field in COMPONENT_FIELDS if SOURCE_FIELD_MAP[field] in fields),
                "component_values_present_total": sum(non_null_by_field.values()),
            }
        )
        for row in rows:
            source_date = str(row.get("slate_date") or row.get("game_date") or date_text)[:10]
            _store_source_payload(
                row=row,
                source_date=source_date,
                source_path=path,
                source_timestamp=snapshot_ts,
                source_kind="snapshot_csv",
                source_priority=2,
                by_starter=by_starter,
                pair_candidates=pair_candidates,
            )
    for path in _backfill_json_files(backfill_json_root):
        rows, fields, generated_at = _backfill_json_rows(path)
        date_text = _date_from_filename(path)
        non_null_by_field = {
            field: sum(1 for row in rows if _non_null(row.get(SOURCE_FIELD_MAP[field])))
            for field in COMPONENT_FIELDS
            if SOURCE_FIELD_MAP[field] in fields
        }
        inventory.append(
            {
                "source_path": _rel(path),
                "source_kind": "backfill_json",
                "date": date_text,
                "snapshot_timestamp": generated_at or path.name,
                "rows": len(rows),
                "component_columns_present": sum(1 for field in COMPONENT_FIELDS if SOURCE_FIELD_MAP[field] in fields),
                "component_values_present_total": sum(non_null_by_field.values()),
            }
        )
        for row in rows:
            source_date = str(row.get("slate_date") or row.get("game_date") or date_text)[:10]
            _store_source_payload(
                row=row,
                source_date=source_date,
                source_path=path,
                source_timestamp=generated_at or path.name,
                source_kind="backfill_json",
                source_priority=1,
                by_starter=by_starter,
                pair_candidates=pair_candidates,
            )
    by_pair: dict[tuple[str, str, str], dict[str, Any]] = {}
    for key, by_id in pair_candidates.items():
        non_empty_ids = [starter_id for starter_id in by_id if starter_id]
        if len(non_empty_ids) == 1:
            by_pair[key] = by_id[non_empty_ids[0]]
        elif not non_empty_ids and len(by_id) == 1:
            by_pair[key] = next(iter(by_id.values()))
    return by_starter, by_pair, inventory


def _recoverability_rows(
    *,
    source_inventory: list[dict[str, Any]],
    lineage_diagnostics_root: Path,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    from datetime import date, timedelta

    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_inventory:
        if row.get("date"):
            by_date[str(row["date"])].append(row)
    lineage_dates = {_date_from_filename(path) for path in _lineage_diagnostic_files(lineage_diagnostics_root)}
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    rows: list[dict[str, Any]] = []
    current = start
    while current <= end:
        date_text = current.isoformat()
        sources = by_date.get(date_text, [])
        compatible_sources = [
            src
            for src in sources
            if int(src.get("component_columns_present") or 0) > 0 and int(src.get("component_values_present_total") or 0) > 0
        ]
        if compatible_sources:
            source_kinds = sorted({str(src.get("source_kind") or "") for src in compatible_sources})
            reason = "compatible_environment_source_available"
            schema_compatible = "yes"
            hydratable = "yes"
        elif date_text in lineage_dates:
            source_kinds = ["lineage_environment_diagnostics"]
            reason = "incompatible_schema_environment_diagnostics_no_v1_1_component_fields"
            schema_compatible = "no"
            hydratable = "no"
        else:
            source_kinds = []
            reason = "no_existing_environment_source_artifact_found"
            schema_compatible = "no"
            hydratable = "no"
        rows.append(
            {
                "date": date_text,
                "snapshot_exists": "yes" if sources or date_text in lineage_dates else "no",
                "source_kinds": ";".join(source_kinds),
                "source_files": ";".join(str(src.get("source_path") or "") for src in compatible_sources),
                "schema_compatible": schema_compatible,
                "hydratable": hydratable,
                "source_rows": sum(int(src.get("rows") or 0) for src in compatible_sources),
                "component_columns_present_max": max((int(src.get("component_columns_present") or 0) for src in compatible_sources), default=0),
                "component_values_present_total": sum(int(src.get("component_values_present_total") or 0) for src in compatible_sources),
                "reason_if_not": "" if hydratable == "yes" else reason,
            }
        )
        current += timedelta(days=1)
    return rows


def _target_files(review_aids_dir: Path, performance_dir: Path, expanded_dir: Path) -> list[tuple[str, Path, str]]:
    targets: list[tuple[str, Path, str]] = []
    for pattern in BOARD_PATTERNS:
        for path in sorted(review_aids_dir.glob(pattern)):
            targets.append(("review_board_rows", path, _date_from_filename(path)))
    for name in ("hits_o15_tier_backtest_rows.csv", "hits_u15_tier_backtest_rows.csv"):
        targets.append(("tier_audit_rows", review_aids_dir / name, "all"))
    targets.extend(
        [
            ("manual_unified_rows", performance_dir / "o15_manual_unified_board_universe_rows.csv", "all"),
            ("reconcile_linked_research_rows", performance_dir / "review_aid_performance_latest_slate.csv", "latest"),
            ("expanded_universe_rows", expanded_dir / "expanded_o15_universe_rows.csv", "all"),
        ]
    )
    seen: set[Path] = set()
    unique: list[tuple[str, Path, str]] = []
    for artifact_type, path, date_text in targets:
        if path in seen:
            continue
        seen.add(path)
        unique.append((artifact_type, path, date_text))
    return unique


def _target_lookup(row: dict[str, Any], fallback_date: str) -> tuple[tuple[str, str, str, str] | None, tuple[str, str, str] | None]:
    date_text = str(row.get("date") or row.get("board_date") or row.get("game_date") or row.get("slate_date") or fallback_date)[:10]
    offense_team = _team(row.get("team") or row.get("offense_team"))
    pitcher_team = _team(row.get("opponent") or row.get("pitcher_team"))
    starter_id = str(row.get("opposing_starter_id") or row.get("starter_player_id") or "").strip().replace(".0", "")
    key4 = (date_text, offense_team, pitcher_team, starter_id) if date_text and offense_team and pitcher_team and starter_id else None
    key3 = (date_text, offense_team, pitcher_team) if date_text and offense_team and pitcher_team else None
    return key4, key3


def _coverage(path: Path, artifact_type: str, date_text: str, rows: list[dict[str, Any]], fields: list[str], phase: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in COMPONENT_FIELDS:
        present = field in fields or any(field in row for row in rows)
        non_null = sum(1 for row in rows if _non_null(row.get(field))) if present else 0
        out.append(
            {
                "phase": phase,
                "artifact_type": artifact_type,
                "path": _rel(path),
                "date": date_text,
                "rows": len(rows),
                "field": field,
                "field_present": present,
                "non_null_rows": non_null,
                "blank_rows": len(rows) - non_null if present else len(rows),
                "coverage_pct": (non_null / len(rows)) if rows else "",
            }
        )
    return out


def _hydrate_file(
    *,
    path: Path,
    artifact_type: str,
    date_text: str,
    by_starter: dict[tuple[str, str, str, str], dict[str, Any]],
    by_pair: dict[tuple[str, str, str], dict[str, Any]],
    write: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows, fields = _read_csv(path)
    before = _coverage(path, artifact_type, date_text, rows, fields, "before")
    projected_cells = 0
    matched_rows = 0
    unsafe_rows = 0
    source_paths: set[str] = set()
    for row in rows:
        key4, key3 = _target_lookup(row, "" if date_text in {"all", "latest"} else date_text)
        source = by_starter.get(key4) if key4 else None
        match_method = "starter_id"
        if source is None and key3:
            source = by_pair.get(key3)
            match_method = "team_pair_unambiguous"
        if source is None:
            continue
        row_changed = False
        for field in COMPONENT_FIELDS:
            current = row.get(field)
            candidate = source.get(field)
            if _non_null(current):
                continue
            if not _non_null(candidate):
                continue
            row[field] = candidate
            projected_cells += 1
            row_changed = True
        if row_changed:
            matched_rows += 1
            source_paths.add(str(source.get("_source_path") or ""))
            row["environment_lineage_hydration_source"] = source.get("_source_path") or ""
            row["environment_lineage_hydration_method"] = match_method
            row["environment_lineage_hydration_snapshot"] = source.get("_source_snapshot_timestamp") or ""
            row["environment_lineage_hydration_source_kind"] = source.get("_source_kind") or ""
    after_fields = list(fields)
    for field in COMPONENT_FIELDS:
        if field not in after_fields:
            after_fields.append(field)
    for field in (
        "environment_lineage_hydration_source",
        "environment_lineage_hydration_method",
        "environment_lineage_hydration_snapshot",
        "environment_lineage_hydration_source_kind",
    ):
        if any(_non_null(row.get(field)) for row in rows) and field not in after_fields:
            after_fields.append(field)
    after = _coverage(path, artifact_type, date_text, rows, after_fields, "after_projected")
    if write and projected_cells:
        _write_csv(path, rows, after_fields)
    summary = {
        "artifact_type": artifact_type,
        "path": _rel(path),
        "date": date_text,
        "rows": len(rows),
        "matched_rows_to_source": matched_rows,
        "projected_cells_to_fill": projected_cells,
        "unsafe_rows": unsafe_rows,
        "source_paths_used": ";".join(sorted(p for p in source_paths if p)),
        "write_applied": bool(write and projected_cells),
    }
    return before, after, summary


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


def _write_assessment(
    path: Path,
    *,
    source_inventory: list[dict[str, Any]],
    recoverability_rows: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    write: bool,
) -> None:
    total_cells = sum(int(row.get("projected_cells_to_fill") or 0) for row in summaries)
    hydratable_dates = [row["date"] for row in recoverability_rows if row.get("hydratable") == "yes"]
    not_hydratable_dates = [row["date"] for row in recoverability_rows if row.get("hydratable") != "yes"]
    source_counts_by_kind: dict[str, int] = defaultdict(int)
    source_counts_by_date: dict[str, int] = defaultdict(int)
    for row in source_inventory:
        source_counts_by_kind[str(row.get("source_kind") or "unknown")] += 1
        if row.get("date"):
            source_counts_by_date[str(row["date"])] += 1
    duplicate_dates = sorted(date for date, count in source_counts_by_date.items() if count > 1)
    incompatible_dates = [
        row["date"]
        for row in recoverability_rows
        if row.get("reason_if_not") == "incompatible_schema_environment_diagnostics_no_v1_1_component_fields"
    ]
    missing_source_dates = [
        row["date"]
        for row in recoverability_rows
        if row.get("reason_if_not") == "no_existing_environment_source_artifact_found"
    ]
    status_line = (
        "C. 60-day hydration was partially completed, with exact dates and reasons for remaining gaps."
        if not_hydratable_dates
        else "A. 60-day hydration successfully completed."
    )
    lines = [
        "# Offensive Environment v1.1 60-Day Historical Hydration Assessment",
        "",
        f"- Generated at: `{_now()}`",
        f"- Mode: `{'write' if write else 'dry-run'}`",
        "- Scope: fill blank Environment v1.1 component fields from existing Proppadia hits-environment source artifacts only.",
        "- No formulas, tiers, selectors, uploads, grading, model predictions, Ops Brief behavior, or Morning Workbench behavior are changed.",
        f"- Final status: **{status_line}**",
        "",
        "## Source Inventory",
        "",
        f"- Compatible source artifacts found: `{len(source_inventory)}`",
        f"- Earliest compatible source date: `{min((r.get('date') for r in source_inventory if r.get('date')), default='n/a')}`",
        f"- Latest compatible source date: `{max((r.get('date') for r in source_inventory if r.get('date')), default='n/a')}`",
        f"- Source artifacts by kind: `{dict(sorted(source_counts_by_kind.items()))}`",
        f"- Dates with duplicate compatible snapshots: `{len(duplicate_dates)}`",
        f"- Incompatible-schema dates in 60-day window: `{len(incompatible_dates)}`",
        f"- Missing-source dates in 60-day window: `{len(missing_source_dates)}`",
        f"- Hydratable dates in 60-day window: `{len(hydratable_dates)}`",
        f"- Non-hydratable dates in 60-day window: `{len(not_hydratable_dates)}`",
        "",
        "## Why Hydration Stopped Before 60 Days",
        "",
        "The first hydration utility scanned only the newer `hits_environment_snapshots` CSV directory, whose earliest compatible file is `2026-06-16`.",
        "This pass also searched the older daily JSON backfill family and feature-lineage backup diagnostics.",
        "The older JSON backfill is compatible through `2026-05-12`, but current Expanded/manual research rows begin on `2026-05-28`, so it does not create additional fill opportunities.",
        "The `2026-05-13` through `2026-06-11` feature-lineage diagnostics exist, but they do not carry the Environment v1.1 component fields and are intentionally not used for hydration.",
        "`2026-06-12` through `2026-06-15` have no compatible historical environment source artifact found.",
        "",
        "## Recoverability",
        "",
        "| date | hydratable | source kinds | reason if not |",
        "|---|---|---|---|",
    ]
    for row in recoverability_rows:
        lines.append(
            f"| `{row.get('date')}` | `{row.get('hydratable')}` | `{row.get('source_kinds')}` | `{row.get('reason_if_not')}` |"
        )
    lines.extend(
        [
            "",
            "## Dry-Run / Write Summary",
            "",
        f"- Target artifacts checked: `{len(summaries)}`",
        f"- Candidate component cells to fill: `{total_cells}`",
        f"- Write pass recommended: `{'yes' if total_cells else 'no'}`",
        "",
            "| artifact type | path | rows | matched rows | cells to fill | write applied |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in summaries:
        lines.append(
            f"| `{row.get('artifact_type')}` | `{row.get('path')}` | `{row.get('rows')}` | "
            f"`{row.get('matched_rows_to_source')}` | `{row.get('projected_cells_to_fill')}` | `{row.get('write_applied')}` |"
        )
    lines.extend(
        [
            "",
            "## Safety Decision",
            "",
            "Hydration is considered safe only when a row maps to a snapshot by exact starter id or by an unambiguous date/team/opponent team-pair source row.",
            "Rows without a reliable source match are left blank.",
            "Existing nonblank component values are never overwritten.",
            "",
            "## Final Status",
            "",
            status_line,
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Assess or hydrate historical Hits 1.5 environment component lineage.")
    ap.add_argument("--write", action="store_true", help="Apply safe blank-cell fills. Default is dry-run only.")
    ap.add_argument("--review-aids-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--performance-dir", type=Path, default=DEFAULT_PERFORMANCE_DIR)
    ap.add_argument("--expanded-dir", type=Path, default=DEFAULT_EXPANDED_DIR)
    ap.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    ap.add_argument("--backfill-json-root", type=Path, default=DEFAULT_BACKFILL_JSON_ROOT)
    ap.add_argument("--lineage-diagnostics-root", type=Path, default=DEFAULT_LINEAGE_DIAGNOSTICS_ROOT)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--recoverability-start-date", default="2026-05-01")
    ap.add_argument("--recoverability-end-date", default="2026-06-29")
    ap.add_argument("--output-prefix", default="offensive_environment_v1_1_historical_hydration")
    args = ap.parse_args()

    by_starter, by_pair, source_inventory = _build_source_index(args.snapshot_root, args.backfill_json_root)
    recoverability = _recoverability_rows(
        source_inventory=source_inventory,
        lineage_diagnostics_root=args.lineage_diagnostics_root,
        start_date=args.recoverability_start_date,
        end_date=args.recoverability_end_date,
    )
    coverage_rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for artifact_type, path, date_text in _target_files(args.review_aids_dir, args.performance_dir, args.expanded_dir):
        if not path.exists():
            continue
        before, after, summary = _hydrate_file(
            path=path,
            artifact_type=artifact_type,
            date_text=date_text,
            by_starter=by_starter,
            by_pair=by_pair,
            write=args.write,
        )
        coverage_rows.extend(before)
        coverage_rows.extend(after)
        summaries.append(summary)

    date_label = "2026-06-29"
    prefix = args.output_prefix
    coverage_csv = args.out_dir / f"{prefix}_coverage_{date_label}.csv"
    field_coverage_csv = args.out_dir / f"{prefix}_field_coverage_{date_label}.csv"
    assessment_md = args.out_dir / f"{prefix}_assessment_{date_label}.md"
    summary_md = args.out_dir / f"{prefix}_summary_{date_label}.md"
    source_inventory_csv = args.out_dir / f"{prefix}_sources_{date_label}.csv"
    artifact_summary_csv = args.out_dir / f"{prefix}_artifacts_{date_label}.csv"
    _write_csv_rows(coverage_csv, recoverability)
    _write_csv_rows(field_coverage_csv, coverage_rows)
    _write_csv_rows(source_inventory_csv, source_inventory)
    _write_csv_rows(artifact_summary_csv, summaries)
    _write_assessment(
        assessment_md,
        source_inventory=source_inventory,
        recoverability_rows=recoverability,
        summaries=summaries,
        write=args.write,
    )
    if args.write:
        _write_assessment(
            summary_md,
            source_inventory=source_inventory,
            recoverability_rows=recoverability,
            summaries=summaries,
            write=True,
        )

    payload = {
        "status": "ok",
        "mode": "write" if args.write else "dry_run",
        "generated_at": _now(),
        "source_snapshots": len(source_inventory),
        "target_artifacts": len(summaries),
        "projected_cells_to_fill": sum(int(row.get("projected_cells_to_fill") or 0) for row in summaries),
        "outputs": {
            "assessment_md": _rel(assessment_md),
            "coverage_csv": _rel(coverage_csv),
            "field_coverage_csv": _rel(field_coverage_csv),
            "source_inventory_csv": _rel(source_inventory_csv),
            "artifact_summary_csv": _rel(artifact_summary_csv),
        },
    }
    if args.write:
        payload["outputs"]["summary_md"] = _rel(summary_md)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
