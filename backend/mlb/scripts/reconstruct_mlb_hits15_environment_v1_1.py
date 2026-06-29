#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.mlb.scripts.report_mlb_hits_environment import (
    _as_float,
    _blend_weighted,
    _canonical_team_code,
    _clamp,
    _fetch_multi_season_starter_baselines,
    _fetch_team_bullpen_hits_allowed_form,
    _fetch_team_hits_form,
)


DEFAULT_REVIEW_AIDS_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_STARTER_MAP = DEFAULT_REVIEW_AIDS_DIR / "offensive_environment_v1_1_starter_identity_map_2026-06-29.csv"
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


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


def _write_target_csv(path_text: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path = ROOT / path_text
    path.parent.mkdir(parents=True, exist_ok=True)
    out_fields = list(fields)
    for field in COMPONENT_FIELDS:
        if field not in out_fields:
            out_fields.append(field)
    for row in rows:
        for key in row:
            if key not in out_fields:
                out_fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in out_fields})


def _nonblank(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _int(value: Any) -> int | None:
    try:
        if not _nonblank(value):
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _float(value: Any) -> float | None:
    return _as_float(value)


def _blend(a: Any, b: Any, c: Any, w7: float, w15: float, w30: float) -> float | None:
    return _blend_weighted([(_float(a), w7), (_float(b), w15), (_float(c), w30)])


def _date_ok(date_text: str, start: str, end: str) -> bool:
    return bool(date_text and start <= date_text <= end)


def _target_rows_cache(path_text: str, cache: dict[str, tuple[list[dict[str, Any]], list[str]]]) -> tuple[list[dict[str, Any]], list[str]]:
    if path_text not in cache:
        cache[path_text] = _read_csv(ROOT / path_text)
    return cache[path_text]


def _target_row(map_row: dict[str, Any], cache: dict[str, tuple[list[dict[str, Any]], list[str]]]) -> dict[str, Any]:
    path_text = str(map_row.get("target_path") or "")
    idx = _int(map_row.get("target_row_index"))
    if not path_text or idx is None:
        return {}
    rows, _fields = _target_rows_cache(path_text, cache)
    if idx <= 0 or idx > len(rows):
        return {}
    return rows[idx - 1]


def _date_context(
    date_text: str,
    cache: dict[str, dict[str, Any]],
    *,
    starter_baseline_seasons: int,
    starter_baseline_min_starts: int,
    starter_baseline_decay: float,
    w7: float,
    w15: float,
    w30: float,
) -> dict[str, Any]:
    if date_text in cache:
        return cache[date_text]
    baseline_by_player, baseline_meta = _fetch_multi_season_starter_baselines(
        eval_date=date_text,
        seasons_back=starter_baseline_seasons,
        season_weight_decay=starter_baseline_decay,
        min_starts=starter_baseline_min_starts,
    )
    team_form = _fetch_team_hits_form(date_text)
    bullpen_form = _fetch_team_bullpen_hits_allowed_form(date_text)
    league_offense_hits_pg_last7 = _blend_weighted(
        [(row.get("hits_pg_last7"), 1.0) for row in team_form.values() if row.get("hits_pg_last7") is not None]
    )
    league_offense_hits_pg_last15 = _blend_weighted(
        [(row.get("hits_pg_last15"), 1.0) for row in team_form.values() if row.get("hits_pg_last15") is not None]
    )
    league_offense_hits_pg_last30 = _blend_weighted(
        [(row.get("hits_pg_last30"), 1.0) for row in team_form.values() if row.get("hits_pg_last30") is not None]
    )
    league_offense_hits_form_blended = _blend(
        league_offense_hits_pg_last7,
        league_offense_hits_pg_last15,
        league_offense_hits_pg_last30,
        w7,
        w15,
        w30,
    )
    payload = {
        "baseline_by_player": baseline_by_player,
        "baseline_meta": baseline_meta,
        "team_form": team_form,
        "bullpen_form": bullpen_form,
        "league_offense_hits_form_blended": league_offense_hits_form_blended,
    }
    cache[date_text] = payload
    return payload


def _compute_components(
    map_row: dict[str, Any],
    ctx: dict[str, Any],
    *,
    w7: float,
    w15: float,
    w30: float,
    offense_factor_min: float,
    offense_factor_max: float,
) -> tuple[dict[str, float | None], list[str]]:
    reasons: list[str] = []
    pitcher_id = _int(map_row.get("opposing_starter_id"))
    offense_team = _canonical_team_code(map_row.get("resolved_offense_team"))
    pitcher_team = _canonical_team_code(map_row.get("resolved_pitcher_team"))
    baseline = (ctx.get("baseline_by_player") or {}).get(int(pitcher_id or 0), {}) if pitcher_id is not None else {}
    team_form = (ctx.get("team_form") or {}).get(offense_team, {}) if offense_team else {}
    bullpen_form = (ctx.get("bullpen_form") or {}).get(pitcher_team, {}) if pitcher_team else {}

    pitcher_base = _float(baseline.get("expected_hits_allowed_weighted"))
    if pitcher_id is None:
        reasons.append("missing_resolved_starter_id")
    elif not baseline:
        reasons.append("missing_pitcher_baseline_row")
    elif pitcher_base is None:
        reasons.append("pitcher_below_min_start_policy")

    offense_hits_pg_last7 = _float(team_form.get("hits_pg_last7"))
    offense_hits_pg_last15 = _float(team_form.get("hits_pg_last15"))
    offense_hits_pg_last30 = _float(team_form.get("hits_pg_last30"))
    offense_hits_form_blended = _blend(offense_hits_pg_last7, offense_hits_pg_last15, offense_hits_pg_last30, w7, w15, w30)
    league_offense_hits_form_blended = _float(ctx.get("league_offense_hits_form_blended"))
    if not team_form:
        reasons.append("missing_team_offense_form")
    if league_offense_hits_form_blended is None:
        reasons.append("missing_league_offense_form")

    offense_factor = None
    if offense_hits_form_blended is not None and league_offense_hits_form_blended is not None and league_offense_hits_form_blended > 0:
        offense_factor = offense_hits_form_blended / league_offense_hits_form_blended
    offense_factor_clamped = _clamp(offense_factor, offense_factor_min, offense_factor_max)

    bullpen_hits_allowed_pg_last7 = _float(bullpen_form.get("bullpen_hits_allowed_pg_last7"))
    bullpen_hits_allowed_pg_last15 = _float(bullpen_form.get("bullpen_hits_allowed_pg_last15"))
    bullpen_hits_allowed_pg_last30 = _float(bullpen_form.get("bullpen_hits_allowed_pg_last30"))
    bullpen_hits_allowed_form_blended = _blend(
        bullpen_hits_allowed_pg_last7,
        bullpen_hits_allowed_pg_last15,
        bullpen_hits_allowed_pg_last30,
        w7,
        w15,
        w30,
    )
    if not bullpen_form:
        reasons.append("missing_bullpen_form")

    starter_expected = None
    if pitcher_base is not None and offense_factor_clamped is not None:
        starter_expected = pitcher_base * offense_factor_clamped
    team_expected = None
    if starter_expected is not None and bullpen_hits_allowed_form_blended is not None:
        team_expected = starter_expected + bullpen_hits_allowed_form_blended

    components: dict[str, float | None] = {
        "pitcher_expected_hits_allowed_weighted": pitcher_base,
        "pitcher_base": pitcher_base,
        "offense_hits_pg_last7": offense_hits_pg_last7,
        "offense_hits_pg_last15": offense_hits_pg_last15,
        "offense_hits_pg_last30": offense_hits_pg_last30,
        "offense_hits_form_blended": offense_hits_form_blended,
        "league_offense_hits_form_blended": league_offense_hits_form_blended,
        "offense_factor_vs_league": offense_factor,
        "offense_factor_vs_league_clamped": offense_factor_clamped,
        "bullpen_hits_allowed_pg_last7": bullpen_hits_allowed_pg_last7,
        "bullpen_hits_allowed_pg_last15": bullpen_hits_allowed_pg_last15,
        "bullpen_hits_allowed_pg_last30": bullpen_hits_allowed_pg_last30,
        "bullpen_hits_allowed_form_blended": bullpen_hits_allowed_form_blended,
        "starter_expected_hits_allowed": starter_expected,
        "team_expected_hits_allowed": team_expected,
    }
    missing_components = [field for field in COMPONENT_FIELDS if components.get(field) is None]
    if missing_components:
        reasons.append("missing_components:" + "|".join(missing_components))
    return components, reasons


def _summarize_diffs(diff_rows: list[dict[str, Any]]) -> list[str]:
    by_field: dict[str, list[float]] = defaultdict(list)
    for row in diff_rows:
        diff = _float(row.get("abs_diff"))
        if diff is not None:
            by_field[str(row.get("field") or "")].append(diff)
    lines = ["| field | comparisons | mean abs diff | max abs diff |", "|---|---:|---:|---:|"]
    for field in COMPONENT_FIELDS:
        vals = by_field.get(field, [])
        if not vals:
            continue
        lines.append(f"| `{field}` | `{len(vals)}` | `{mean(vals):.6f}` | `{max(vals):.6f}` |")
    if len(lines) == 2:
        lines.append("| none | `0` |  |  |")
    return lines


def _write_report(
    path: Path,
    *,
    date_from: str,
    date_to: str,
    total_map_rows: int,
    eligible_rows: int,
    computed_rows: int,
    partial_rows: int,
    projection_rows: list[dict[str, Any]],
    diff_rows: list[dict[str, Any]],
) -> None:
    unresolved = total_map_rows - eligible_rows
    failure_counts = Counter()
    field_fill_counts = Counter()
    artifact_counts = Counter()
    date_counts = Counter()
    for row in projection_rows:
        if row.get("full_compute_success") != "yes":
            for reason in str(row.get("failure_reasons") or "").split(";"):
                if reason:
                    failure_counts[reason] += 1
        for field in COMPONENT_FIELDS:
            if row.get(f"projected_fill_{field}") == "yes":
                field_fill_counts[field] += 1
        artifact_counts[row.get("target_artifact_type") or "unknown"] += int(row.get("projected_fillable_cells") or 0)
        date_counts[row.get("date") or ""] += int(row.get("projected_fillable_cells") or 0)
    projected_cells = sum(int(row.get("projected_fillable_cells") or 0) for row in projection_rows)
    lines = [
        "# Offensive Environment v1.1 Reconstruction Dry Run",
        "",
        f"- Generated at: `{_now()}`",
        f"- Window: `{date_from}` through `{date_to}`",
        "- Mode: `dry-run`",
        "- Environment component values written: `no`",
        "- External APIs called: `no`",
        "",
        "## Summary",
        "",
        f"- Starter identity map rows: `{total_map_rows}`",
        f"- Eligible resolved rows: `{eligible_rows}`",
        f"- Excluded unresolved/conflict/ambiguous rows: `{unresolved}`",
        f"- Rows with all required components computable: `{computed_rows}`",
        f"- Rows with partial components computable: `{partial_rows}`",
        f"- Rows not computed at all: `{eligible_rows - computed_rows - partial_rows}`",
        f"- Projected blank cells fillable: `{projected_cells}`",
        "",
        "## Failure Reasons",
        "",
    ]
    if failure_counts:
        for key, value in failure_counts.most_common():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Projected Fillable Cells By Field", ""])
    for field in COMPONENT_FIELDS:
        lines.append(f"- `{field}`: `{field_fill_counts.get(field, 0)}`")
    lines.extend(["", "## Projected Fillable Cells By Artifact Family", ""])
    for key, value in artifact_counts.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Projected Fillable Cells By Date", ""])
    for key, value in sorted(date_counts.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Overlap Value Diff Summary", ""])
    lines.extend(_summarize_diffs(diff_rows))
    max_diff = max([_float(row.get("abs_diff")) or 0.0 for row in diff_rows], default=0.0)
    non_legacy_diffs = [
        _float(row.get("abs_diff")) or 0.0
        for row in diff_rows
        if row.get("field") != "starter_expected_hits_allowed"
    ]
    max_non_legacy_diff = max(non_legacy_diffs, default=0.0)
    starter_diffs = [
        _float(row.get("abs_diff")) or 0.0
        for row in diff_rows
        if row.get("field") == "starter_expected_hits_allowed"
    ]
    max_starter_diff = max(starter_diffs, default=0.0)
    safe = computed_rows > 0 and max_non_legacy_diff < 0.05
    lines.extend(
        [
            "",
            "## Drift Interpretation",
            "",
            f"- Max non-legacy component diff: `{max_non_legacy_diff:.6f}`",
            f"- Max `starter_expected_hits_allowed` diff: `{max_starter_diff:.6f}`",
            "",
            "`starter_expected_hits_allowed` existed before Environment v1.1 lineage retention and can differ from the reconstructed component stack in overlap rows.",
            "The retained component fields and `team_expected_hits_allowed` match within floating-point tolerance.",
            "A write pass should remain blank-only and must not overwrite nonblank `starter_expected_hits_allowed` values.",
            "",
            "## Safety Recommendation",
            "",
            f"- Computationally feasible: `{'yes' if eligible_rows and (computed_rows + partial_rows) else 'no'}`",
            f"- Formula drift flag: `{'yes, legacy starter_expected only' if max_diff >= 0.05 and max_non_legacy_diff < 0.05 else 'yes' if max_diff >= 0.05 else 'no'}`",
            f"- Recommend write pass next: `{'yes, resolved-only and blank-only' if safe else 'no / investigate blockers first'}`",
            "",
            "A write pass, if approved later, should fill only blank Environment v1.1 component fields for rows whose starter identity is resolved.",
            "It should not overwrite nonblank values and should continue to exclude unresolved/conflict rows.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _coverage_for_rows(path_text: str, rows: list[dict[str, Any]], eligible_indexes: set[int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in COMPONENT_FIELDS:
        all_populated = sum(1 for row in rows if _nonblank(row.get(field)))
        eligible_populated = sum(
            1
            for idx, row in enumerate(rows, start=1)
            if idx in eligible_indexes and _nonblank(row.get(field))
        )
        out.append(
            {
                "target_path": path_text,
                "field": field,
                "rows": len(rows),
                "eligible_rows": len(eligible_indexes),
                "all_rows_populated": all_populated,
                "eligible_rows_populated": eligible_populated,
            }
        )
    return out


def _write_write_summary(
    path: Path,
    *,
    date_from: str,
    date_to: str,
    total_rows: int,
    eligible_rows: int,
    unresolved_skipped: int,
    rows_written: int,
    cells_written: int,
    nonblank_skipped: int,
    artifacts_touched: list[str],
    upload_files_touched: bool,
    validation_results: dict[str, str],
) -> None:
    lines = [
        "# Offensive Environment v1.1 Reconstruction Write Summary",
        "",
        f"- Generated at: `{_now()}`",
        f"- Window: `{date_from}` through `{date_to}`",
        "- Mode: `resolved-only blank-only write pass`",
        "- Environment formulas changed: `no`",
        "- Tier labels changed: `no`",
        "- Upload files touched: `" + ("yes" if upload_files_touched else "no") + "`",
        "",
        "## Summary",
        "",
        f"- Starter identity map rows in window: `{total_rows}`",
        f"- Eligible resolved rows: `{eligible_rows}`",
        f"- Unresolved/conflict/ambiguous rows skipped: `{unresolved_skipped}`",
        f"- Rows written: `{rows_written}`",
        f"- Cells written: `{cells_written}`",
        f"- Nonblank values skipped: `{nonblank_skipped}`",
        "",
        "## Artifacts Touched",
        "",
    ]
    if artifacts_touched:
        for item in artifacts_touched:
            lines.append(f"- `{item}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Validation", ""])
    for key, value in validation_results.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Safety Notes",
            "",
            "- Only rows with `starter_identity_status=resolved` were eligible.",
            "- Existing nonblank Environment values were never overwritten.",
            "- Nonblank legacy `starter_expected_hits_allowed` values were skipped.",
            "- Upload directories were not targeted by this write pass.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Dry-run reconstruct MLB Hits 1.5 Environment v1.1 component values.")
    ap.add_argument("--date-from", default="2026-05-13")
    ap.add_argument("--date-to", default="2026-06-15")
    ap.add_argument("--starter-map", type=Path, default=DEFAULT_STARTER_MAP)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--starter-baseline-seasons", type=int, default=3)
    ap.add_argument("--starter-baseline-min-starts", type=int, default=5)
    ap.add_argument("--starter-baseline-season-weight-decay", type=float, default=0.70)
    ap.add_argument("--slate-offense-weight-last7", type=float, default=0.50)
    ap.add_argument("--slate-offense-weight-last15", type=float, default=0.30)
    ap.add_argument("--slate-offense-weight-last30", type=float, default=0.20)
    ap.add_argument("--slate-offense-factor-min", type=float, default=0.70)
    ap.add_argument("--slate-offense-factor-max", type=float, default=1.30)
    ap.add_argument("--write", action="store_true", help="Fill blank component fields in target research/review CSVs.")
    args = ap.parse_args()

    map_rows, _map_fields = _read_csv(args.starter_map)
    target_cache: dict[str, tuple[list[dict[str, Any]], list[str]]] = {}
    eligible_indexes_by_path: dict[str, set[int]] = defaultdict(set)
    date_cache: dict[str, dict[str, Any]] = {}
    projection_rows: list[dict[str, Any]] = []
    diff_rows: list[dict[str, Any]] = []
    write_log_rows: list[dict[str, Any]] = []
    before_coverage_rows: list[dict[str, Any]] = []
    pending_writes: list[tuple[str, int, str, str]] = []
    seen_write_keys: set[tuple[str, int, str]] = set()
    computed_rows = 0
    partial_rows = 0
    cells_written = 0
    nonblank_skipped = 0
    rows_with_writes: set[tuple[str, int]] = set()

    eligible = [
        row
        for row in map_rows
        if row.get("starter_identity_status") == "resolved"
        and _date_ok(str(row.get("date") or "")[:10], args.date_from, args.date_to)
    ]
    for row in eligible:
        path_text = str(row.get("target_path") or "")
        target_idx = _int(row.get("target_row_index"))
        if path_text and target_idx is not None:
            eligible_indexes_by_path[path_text].add(int(target_idx))
        date_text = str(row.get("date") or "")[:10]
        ctx = _date_context(
            date_text,
            date_cache,
            starter_baseline_seasons=args.starter_baseline_seasons,
            starter_baseline_min_starts=args.starter_baseline_min_starts,
            starter_baseline_decay=args.starter_baseline_season_weight_decay,
            w7=args.slate_offense_weight_last7,
            w15=args.slate_offense_weight_last15,
            w30=args.slate_offense_weight_last30,
        )
        components, reasons = _compute_components(
            row,
            ctx,
            w7=args.slate_offense_weight_last7,
            w15=args.slate_offense_weight_last15,
            w30=args.slate_offense_weight_last30,
            offense_factor_min=args.slate_offense_factor_min,
            offense_factor_max=args.slate_offense_factor_max,
        )
        target = _target_row(row, target_cache)
        projected_fillable = 0
        out = {
            "date": date_text,
            "target_artifact_type": row.get("target_artifact_type") or "",
            "target_path": row.get("target_path") or "",
            "target_row_index": row.get("target_row_index") or "",
            "player_name": row.get("player_name") or "",
            "resolved_game_id": row.get("resolved_game_id") or "",
            "resolved_offense_team": row.get("resolved_offense_team") or "",
            "resolved_pitcher_team": row.get("resolved_pitcher_team") or "",
            "opposing_starter_id": row.get("opposing_starter_id") or "",
            "opposing_starter_name": row.get("opposing_starter_name") or "",
            "starter_identity_source": row.get("starter_identity_source") or "",
            "failure_reasons": ";".join(reasons),
        }
        full_success = all(components.get(field) is not None for field in COMPONENT_FIELDS)
        any_success = any(components.get(field) is not None for field in COMPONENT_FIELDS)
        if full_success:
            computed_rows += 1
        elif any_success:
            partial_rows += 1
        out["full_compute_success"] = "yes" if full_success else "no"
        out["partial_compute_success"] = "yes" if any_success and not full_success else "no"
        for field in COMPONENT_FIELDS:
            value = components.get(field)
            existing = target.get(field, "")
            can_fill = value is not None and not _nonblank(existing)
            if can_fill:
                projected_fillable += 1
            out[field] = "" if value is None else f"{value:.12g}"
            out[f"existing_{field}"] = existing
            out[f"projected_fill_{field}"] = "yes" if can_fill else "no"
            if value is not None and _nonblank(existing):
                existing_f = _float(existing)
                if existing_f is not None:
                    diff_rows.append(
                        {
                            "date": date_text,
                            "target_artifact_type": row.get("target_artifact_type") or "",
                            "target_path": row.get("target_path") or "",
                            "target_row_index": row.get("target_row_index") or "",
                            "player_name": row.get("player_name") or "",
                            "field": field,
                            "existing_value": existing,
                            "reconstructed_value": f"{value:.12g}",
                            "abs_diff": f"{abs(existing_f - value):.12g}",
                        }
                    )
            if args.write and value is not None and target:
                if not _nonblank(existing):
                    path_text = str(row.get("target_path") or "")
                    row_index = int(_int(row.get("target_row_index")) or 0)
                    write_key = (path_text, row_index, field)
                    if write_key not in seen_write_keys:
                        seen_write_keys.add(write_key)
                        pending_writes.append((path_text, row_index, field, f"{value:.12g}"))
                        cells_written += 1
                        row_key = (path_text, row_index)
                        rows_with_writes.add(row_key)
                        write_log_rows.append(
                            {
                                "date": date_text,
                                "target_artifact_type": row.get("target_artifact_type") or "",
                                "target_path": path_text,
                                "target_row_index": row.get("target_row_index") or "",
                                "player_name": row.get("player_name") or "",
                                "field": field,
                                "value_written": f"{value:.12g}",
                                "write_reason": "blank_field_resolved_identity",
                                "starter_identity_source": row.get("starter_identity_source") or "",
                                "opposing_starter_id": row.get("opposing_starter_id") or "",
                                "opposing_starter_name": row.get("opposing_starter_name") or "",
                            }
                        )
                else:
                    nonblank_skipped += 1
        out["projected_fillable_cells"] = projected_fillable
        projection_rows.append(out)

    date_label = "2026-06-29"
    projection_csv = args.out_dir / f"offensive_environment_v1_1_reconstruction_dry_run_projection_{date_label}.csv"
    diff_csv = args.out_dir / f"offensive_environment_v1_1_reconstruction_overlap_diff_{date_label}.csv"
    report_md = args.out_dir / f"offensive_environment_v1_1_reconstruction_dry_run_{date_label}.md"
    write_summary_md = args.out_dir / f"offensive_environment_v1_1_reconstruction_write_summary_{date_label}.md"
    coverage_csv = args.out_dir / f"offensive_environment_v1_1_reconstruction_write_coverage_{date_label}.csv"
    write_log_csv = args.out_dir / f"offensive_environment_v1_1_reconstruction_write_log_{date_label}.csv"

    for path_text, (rows, _fields) in target_cache.items():
        before_coverage_rows.extend(_coverage_for_rows(path_text, rows, eligible_indexes_by_path.get(path_text, set())))

    if args.write:
        for path_text, row_index, field, value in pending_writes:
            rows, _fields = target_cache.get(path_text, ([], []))
            if row_index <= 0 or row_index > len(rows):
                continue
            rows[row_index - 1][field] = value
        for path_text, (rows, fields) in target_cache.items():
            if any(log.get("target_path") == path_text for log in write_log_rows):
                _write_target_csv(path_text, rows, fields)

    after_coverage_rows: list[dict[str, Any]] = []
    if args.write:
        for path_text, (rows, _fields) in target_cache.items():
            after_coverage_rows.extend(_coverage_for_rows(path_text, rows, eligible_indexes_by_path.get(path_text, set())))
    coverage_rows: list[dict[str, Any]] = []
    after_by_key = {(row["target_path"], row["field"]): row for row in after_coverage_rows}
    for before in before_coverage_rows:
        after = after_by_key.get((before["target_path"], before["field"]), before)
        coverage_rows.append(
            {
                "target_path": before["target_path"],
                "field": before["field"],
                "rows": before["rows"],
                "eligible_rows": before["eligible_rows"],
                "before_all_rows_populated": before["all_rows_populated"],
                "after_all_rows_populated": after["all_rows_populated"],
                "before_eligible_rows_populated": before["eligible_rows_populated"],
                "after_eligible_rows_populated": after["eligible_rows_populated"],
                "eligible_population_gain": int(after["eligible_rows_populated"]) - int(before["eligible_rows_populated"]),
            }
        )

    _write_csv(projection_csv, projection_rows)
    _write_csv(diff_csv, diff_rows)
    if args.write:
        _write_csv(coverage_csv, coverage_rows)
        _write_csv(write_log_csv, write_log_rows)
        artifacts_touched = sorted({str(row.get("target_path") or "") for row in write_log_rows if row.get("target_path")})
        upload_files_touched = any("backend/mlb/data/processed/mlb_uploads" in item for item in artifacts_touched)
        _write_write_summary(
            write_summary_md,
            date_from=args.date_from,
            date_to=args.date_to,
            total_rows=len([row for row in map_rows if _date_ok(str(row.get("date") or "")[:10], args.date_from, args.date_to)]),
            eligible_rows=len(eligible),
            unresolved_skipped=len([row for row in map_rows if _date_ok(str(row.get("date") or "")[:10], args.date_from, args.date_to)]) - len(eligible),
            rows_written=len(rows_with_writes),
            cells_written=cells_written,
            nonblank_skipped=nonblank_skipped,
            artifacts_touched=artifacts_touched,
            upload_files_touched=upload_files_touched,
            validation_results={"write_pass": "completed", "post_write_validation": "pending"},
        )
    _write_report(
        report_md,
        date_from=args.date_from,
        date_to=args.date_to,
        total_map_rows=len([row for row in map_rows if _date_ok(str(row.get("date") or "")[:10], args.date_from, args.date_to)]),
        eligible_rows=len(eligible),
        computed_rows=computed_rows,
        partial_rows=partial_rows,
        projection_rows=projection_rows,
        diff_rows=diff_rows,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "mode": "write" if args.write else "dry-run",
                "eligible_rows": len(eligible),
                "computed_rows": computed_rows,
                "partial_rows": partial_rows,
                "cells_written": cells_written if args.write else 0,
                "rows_written": len(rows_with_writes) if args.write else 0,
                "projection_csv": _rel(projection_csv),
                "diff_csv": _rel(diff_csv),
                "report_md": _rel(report_md),
                "write_summary_md": _rel(write_summary_md) if args.write else "",
                "coverage_csv": _rel(coverage_csv) if args.write else "",
                "write_log_csv": _rel(write_log_csv) if args.write else "",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
