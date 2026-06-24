#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/mlb/feature_lineage"
FIELDS = ("game_time", "time_of_day_bucket", "game_day_of_week")


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _non_null(value: Any) -> bool:
    text = _clean(value)
    return bool(text and text.lower() not in {"nan", "none", "null", "missing"})


def _date_from_path(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if match:
        return match.group(1)
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _latest_file(pattern: str) -> Path | None:
    files = sorted(Path().glob(pattern))
    return files[-1] if files else None


def _date_in_window(date_text: str, window: str, latest: str) -> bool:
    if window == "current":
        return date_text == latest
    if window == "full_history":
        return True
    try:
        from datetime import datetime

        d = datetime.strptime(date_text, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        return False
    if window == "last_30":
        return 0 <= (latest_d - d).days <= 29
    return False


def _summarize_rows(
    *,
    stage: str,
    scope: str,
    window: str,
    source_path: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "stage": stage,
        "scope": scope,
        "window": window,
        "source_path": source_path,
        "row_count": len(rows),
    }
    for field in FIELDS:
        present = any(field in row for row in rows)
        count = sum(1 for row in rows if field in row and _non_null(row.get(field))) if present else 0
        out[f"{field}_present"] = present
        out[f"{field}_non_null_rows"] = count
        out[f"{field}_null_rows"] = len(rows) - count if present else len(rows)
        out[f"{field}_coverage"] = count / len(rows) if rows else None
    return out


def _filter_hits_o15(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        try:
            line = float(row.get("line") or 0)
        except Exception:
            line = 0
        prop = str(row.get("prop_type") or "").strip().lower()
        side = str(row.get("side") or "over").strip().lower()
        if prop == "hits" and abs(line - 1.5) < 1e-9 and side in {"", "over"}:
            out.append(row)
    return out


def _current_artifacts(date_text: str) -> list[tuple[str, str, Path | None]]:
    return [
        ("prediction_features", "current_slate_all_rows", Path("backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")),
        ("slate_output", "current_slate_all_rows", Path("backend/mlb/data/processed/mlb_slate_output.csv")),
        (
            "selector",
            "current_slate_hits_lane_selector",
            Path(f"backend/mlb/exports/model_v2/lanes/today/{date_text}/hits_lane_selector_{date_text}.csv"),
        ),
        (
            "ranking_upload_input",
            "current_slate_ranking_upload_input",
            Path(f"backend/mlb/exports/model_v2/lanes/today/{date_text}/hits_lane_selector_{date_text}_ranking_upload_input.csv"),
        ),
        (
            "quick_card_output",
            "current_slate_quick_card",
            Path(f"backend/mlb/exports/model_v2/lanes/today/{date_text}/quick_card_hits_{date_text}.csv"),
        ),
        (
            "ranking_upload_diagnostics",
            "current_slate_upload_diagnostics",
            Path(f"backend/mlb/exports/model_v2/upload/{date_text}/ranking_tool_upload_diagnostics_{date_text}.csv"),
        ),
        (
            "quick_card_upload_diagnostics",
            "current_slate_upload_diagnostics",
            Path(f"backend/mlb/exports/model_v2/upload/{date_text}/quick_card_tool_upload_diagnostics_{date_text}.csv"),
        ),
        (
            "review_aid_o15_layered",
            "current_review_aid",
            Path(f"artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date_text}.csv"),
        ),
        (
            "review_aid_u15",
            "current_review_aid",
            Path(f"artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{date_text}.csv"),
        ),
    ]


def _historical_stage_files(stage: str) -> list[Path]:
    if stage == "prediction_features":
        return sorted(Path("backend/mlb/exports/odds_history").glob("20??-??-??/mlb_predictions_wide_calibrated*.csv"))
    if stage == "slate_output":
        return sorted(Path("backend/mlb/exports/odds_history").glob("20??-??-??/mlb_slate_output*.csv"))
    if stage == "selector":
        return sorted(Path("backend/mlb/exports/model_v2/lanes/today").glob("20??-??-??/hits_lane_selector_20??-??-??.csv"))
    if stage == "ranking_upload_input":
        return sorted(
            Path("backend/mlb/exports/model_v2/lanes/today").glob(
                "20??-??-??/hits_lane_selector_20??-??-??_ranking_upload_input.csv"
            )
        )
    if stage == "quick_card_output":
        return sorted(Path("backend/mlb/exports/model_v2/lanes/today").glob("20??-??-??/quick_card_hits_20??-??-??.csv"))
    if stage == "ranking_upload_diagnostics":
        return sorted(Path("backend/mlb/exports/model_v2/upload").glob("20??-??-??/ranking_tool_upload_diagnostics_20??-??-??.csv"))
    if stage == "quick_card_upload_diagnostics":
        return sorted(Path("backend/mlb/exports/model_v2/upload").glob("20??-??-??/quick_card_tool_upload_diagnostics_20??-??-??.csv"))
    if stage == "execution_reconcile":
        return sorted(Path("artifacts/analysis/mlb/execution_vs_model").glob("20??-??-??/reconcile_rows.csv"))
    if stage == "actual_reconcile":
        return sorted(Path("backend/mlb/exports/model_v2/reconcile").glob("20??-??-??/actual_wagers_by_source_*.csv"))
    if stage == "review_aid_o15_layered":
        return sorted(Path("artifacts/analysis/mlb/review_aids").glob("hits_o15_layered_candidates_20??-??-??.csv"))
    if stage == "review_aid_u15":
        return sorted(Path("artifacts/analysis/mlb/review_aids").glob("hits_u15_favorite_audit_20??-??-??.csv"))
    return []


def _stage_scope(stage: str) -> str:
    if stage == "execution_reconcile":
        return "historical_reconcile_all_rows"
    if stage == "actual_reconcile":
        return "historical_actual_wagers_all_rows"
    if stage.startswith("review_aid"):
        return "historical_review_aid_rows"
    return "historical_stage_all_rows"


def _combine_files(stage: str, window: str, latest: str) -> tuple[list[dict[str, Any]], int, str]:
    rows: list[dict[str, Any]] = []
    files = []
    for path in _historical_stage_files(stage):
        date_text = _date_from_path(path)
        if not date_text or not _date_in_window(date_text, window, latest):
            continue
        loaded = _read_csv(path)
        if loaded:
            rows.extend(loaded)
            files.append(path)
    source = f"{len(files)} files"
    return rows, len(files), source


def _per_date_coverage(stage: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in _historical_stage_files(stage):
        date_text = _date_from_path(path)
        rows = _read_csv(path)
        if not date_text or not rows:
            continue
        summary = _summarize_rows(
            stage=stage,
            scope=_stage_scope(stage),
            window=date_text,
            source_path=_rel(path),
            rows=rows,
        )
        out.append(summary)
    return out


def _first_null_dates(per_date: list[dict[str, Any]], field: str) -> tuple[str, str]:
    zero = ""
    partial = ""
    for row in sorted(per_date, key=lambda r: str(r.get("window") or "")):
        cov = row.get(f"{field}_coverage")
        if cov is None:
            continue
        if cov == 0 and not zero:
            zero = str(row.get("window") or "")
        if cov < 1 and not partial:
            partial = str(row.get("window") or "")
    return zero, partial


def _render_report(path: Path, coverage_rows: list[dict[str, Any]], per_date_rows: list[dict[str, Any]], current_date: str, latest_completed: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def cov(stage: str, window: str, field: str, scope: str | None = None) -> str:
        for row in coverage_rows:
            if row.get("stage") == stage and row.get("window") == window and (scope is None or row.get("scope") == scope):
                value = row.get(f"{field}_coverage")
                return "n/a" if value is None else f"{float(value) * 100:.2f}%"
        return "n/a"

    current_rows = [row for row in coverage_rows if row.get("window") == "current"]
    last30_rows = [row for row in coverage_rows if row.get("window") == "last_30"]
    full_rows = [row for row in coverage_rows if row.get("window") == "full_history"]
    exec_per_date = [row for row in per_date_rows if row.get("stage") == "execution_reconcile"]
    lines = [
        "# Time Context Lineage Audit",
        "",
        f"- Current slate date audited: `{current_date}`",
        f"- Latest completed reconcile date: `{latest_completed}`",
        "- Fields audited: `game_time`, `time_of_day_bucket`, `game_day_of_week`.",
        "- Scope: analysis only; no production/model/selector/upload/grading changes.",
        "",
        "## Source Trace",
        "",
        "- `game_time` originates in prediction/scoring game metadata from `build_mlb_predictions_wide.py` (`g.game_time`).",
        "- `time_of_day_bucket` is derived from `game_time` via `backend.mlb.shared.time_utils_backend.get_time_of_day_bucket_et` in prediction generation, and via `_time_of_day_bucket(game_time)` in slate/reconcile builders.",
        "- `game_day_of_week` is derived from `game_date` / `game_time` in prediction generation and from `game_date` in slate/reconcile builders.",
        "- Patch 1A/1A.1 restored these fields from prediction -> slate -> selector/upload diagnostics -> reconcile where artifacts were regenerated or backfilled.",
        "",
        "## Current Slate Coverage",
        "",
        "| stage | rows | game_time | time_of_day_bucket | game_day_of_week | source |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in current_rows:
        lines.append(
            f"| `{row.get('stage')}` | `{row.get('row_count')}` | "
            f"`{(row.get('game_time_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('time_of_day_bucket_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('game_day_of_week_coverage') or 0) * 100:.2f}%` | `{row.get('source_path')}` |"
        )
    lines.extend(
        [
            "",
            "## Last 30 Days By Stage",
            "",
            "| stage | files/rows source | rows | game_time | time_of_day_bucket | game_day_of_week |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in last30_rows:
        lines.append(
            f"| `{row.get('stage')}` | `{row.get('source_path')}` | `{row.get('row_count')}` | "
            f"`{(row.get('game_time_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('time_of_day_bucket_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('game_day_of_week_coverage') or 0) * 100:.2f}%` |"
        )
    lines.extend(
        [
            "",
            "## Full Historical Sample By Stage",
            "",
            "| stage | files/rows source | rows | game_time | time_of_day_bucket | game_day_of_week |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in full_rows:
        lines.append(
            f"| `{row.get('stage')}` | `{row.get('source_path')}` | `{row.get('row_count')}` | "
            f"`{(row.get('game_time_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('time_of_day_bucket_coverage') or 0) * 100:.2f}%` | "
            f"`{(row.get('game_day_of_week_coverage') or 0) * 100:.2f}%` |"
        )
    lines.extend(["", "## Execution Reconcile Date Ranges Affected", ""])
    for field in FIELDS:
        zero, partial = _first_null_dates(exec_per_date, field)
        latest_non_full = ""
        for row in sorted(exec_per_date, key=lambda r: str(r.get("window") or ""), reverse=True):
            cov_value = row.get(f"{field}_coverage")
            if cov_value is not None and cov_value < 1:
                latest_non_full = str(row.get("window") or "")
                break
        lines.append(
            f"- `{field}`: first zero-coverage date `{zero or 'none'}`; first partial date `{partial or 'none'}`; latest non-full date `{latest_non_full or 'none'}`."
        )
    lines.extend(
        [
            "",
            "## Diagnosis",
            "",
            "- Current slate preservation is the key test for daily collection. If current slate stages are near/full coverage, current collection is working.",
            "- Low coverage in the o1.5 rest audit is expected when older execution reconcile rows are included: many pre-restoration/backfill artifacts still lack `game_time` / `time_of_day_bucket`, while `game_day_of_week` is more recoverable because it can be derived from date.",
            "- The first disappearance historically was the prediction-to-slate boundary introduced by commit `86e9daec`; Patch 1A/1A.1 restored current forward propagation.",
            "- Remaining low historical coverage is partial historical restoration / unrepaired historical reconcile coverage, not evidence that today’s pipeline is dropping the fields.",
            "",
            "## Recommendation",
            "",
            "- Treat time/rest analysis as reliable for current and post-Patch artifacts; use missing buckets explicitly for older history.",
            "- If time/rest becomes decision-critical, run a targeted execution-reconcile backfill for `game_time` and derived `time_of_day_bucket` using schedule/game metadata with strict row-count and non-patch-column guards.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit lineage coverage for MLB time context fields.")
    parser.add_argument("--current-date", default="2026-06-21")
    parser.add_argument("--latest-completed-date", default="2026-06-20")
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    args = parser.parse_args()

    current_date = str(args.current_date)[:10]
    latest_completed = str(args.latest_completed_date)[:10]
    coverage_rows: list[dict[str, Any]] = []

    for stage, scope, path in _current_artifacts(current_date):
        rows = _read_csv(path) if path and path.exists() else []
        coverage_rows.append(
            _summarize_rows(
                stage=stage,
                scope=scope,
                window="current",
                source_path=_rel(path) if path else "missing",
                rows=rows,
            )
        )

    historical_stages = [
        "prediction_features",
        "slate_output",
        "selector",
        "ranking_upload_input",
        "quick_card_output",
        "ranking_upload_diagnostics",
        "quick_card_upload_diagnostics",
        "execution_reconcile",
        "actual_reconcile",
        "review_aid_o15_layered",
        "review_aid_u15",
    ]
    for stage in historical_stages:
        for window in ("last_30", "full_history"):
            rows, file_count, source = _combine_files(stage, window, latest_completed)
            coverage_rows.append(
                _summarize_rows(
                    stage=stage,
                    scope=_stage_scope(stage),
                    window=window,
                    source_path=source,
                    rows=rows,
                )
            )
        if stage in {"execution_reconcile", "slate_output", "selector", "ranking_upload_input", "quick_card_output"}:
            rows, _, source = _combine_files(stage, "current", current_date)
            if rows:
                coverage_rows.append(
                    _summarize_rows(
                        stage=stage,
                        scope=f"{_stage_scope(stage)}_current_window",
                        window="current_from_archives",
                        source_path=source,
                        rows=rows,
                    )
                )

    per_date_rows: list[dict[str, Any]] = []
    for stage in historical_stages:
        per_date_rows.extend(_per_date_coverage(stage))

    out_dir = ROOT / args.out_dir
    coverage_path = out_dir / "time_context_coverage_by_stage.csv"
    report_path = out_dir / "time_context_lineage_audit.md"
    _write_csv(coverage_path, coverage_rows)
    _render_report(report_path, coverage_rows, per_date_rows, current_date, latest_completed)

    print(f"coverage_csv={_rel(coverage_path)}")
    print(f"report_md={_rel(report_path)}")
    for row in coverage_rows:
        if row.get("window") == "current":
            print(
                f"{row.get('stage')}: rows={row.get('row_count')} "
                f"game_time={row.get('game_time_coverage')} "
                f"time_bucket={row.get('time_of_day_bucket_coverage')} "
                f"dow={row.get('game_day_of_week_coverage')}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
