#!/usr/bin/env python3
"""Non-production MLB hits-environment backfill for the 2026 exact-parity window."""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import report_mlb_hits_environment


DEFAULT_START = "2026-03-25"
DEFAULT_END = "2026-05-12"
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/hits_environment_backfill_2026")
DEFAULT_PERSISTENCE_OUT_DIR = Path("artifacts/analysis/mlb/hits_environment_persistence_backfill_2026")
ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: str, end: str) -> list[str]:
    cur = _parse_date(start)
    last = _parse_date(end)
    out: list[str] = []
    while cur <= last:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        return max(0, sum(1 for _ in csv.reader(fh)) - 1)


def _reset_file(path: Path) -> None:
    if path.exists():
        path.unlink()


def _run_report_for_date(
    run_date: str,
    *,
    out_dir: Path,
    history_jsonl: Path,
    tracker_csv: Path,
) -> dict[str, Any]:
    day_root = ODDS_HISTORY_ROOT / run_date
    slate_csv = day_root / "mlb_slate_output.csv"
    wide_csv = day_root / "mlb_predictions_wide_calibrated.csv"
    date_dir = out_dir / "daily" / run_date
    date_dir.mkdir(parents=True, exist_ok=True)
    out_json = date_dir / f"mlb_hits_environment_{run_date}.json"
    out_csv = date_dir / f"mlb_hits_environment_hits_allowed_rows_{run_date}.csv"
    log_path = date_dir / f"mlb_hits_environment_{run_date}.log"

    missing = []
    if not slate_csv.exists():
        missing.append("missing_mlb_slate_output_csv")
    if not wide_csv.exists():
        missing.append("missing_mlb_predictions_wide_calibrated_csv")
    if missing:
        return {
            "date": run_date,
            "status": "skipped",
            "ok": False,
            "history_rows_appended": 0,
            "team_eval_rows_in_snapshot": 0,
            "team_eval_rows_with_expected": 0,
            "team_eval_rows_with_actual": 0,
            "hits_allowed_rows": 0,
            "warnings_count": 0,
            "warnings": [],
            "missing_artifact_reasons": missing,
            "out_json": str(out_json),
            "out_csv": str(out_csv),
            "log_path": str(log_path),
        }

    before_history_count = _jsonl_count(history_jsonl)
    argv = [
        "--as-of-date",
        run_date,
        "--slate-date",
        run_date,
        "--slate-csv",
        str(slate_csv),
        "--wide-csv",
        str(wide_csv),
        "--out-json",
        str(out_json),
        "--out-csv",
        str(out_csv),
        "--history-jsonl",
        str(history_jsonl),
        "--eval-tracker-csv",
        str(tracker_csv),
    ]
    exit_code = 0
    with log_path.open("w", encoding="utf-8") as log_fh, contextlib.redirect_stdout(log_fh):
        try:
            exit_code = int(report_mlb_hits_environment.main(argv))
        except SystemExit as exc:
            exit_code = int(exc.code or 0)
        except Exception as exc:
            exit_code = 99
            print(f"backfill_report_exception:{type(exc).__name__}:{exc}")
    after_history_count = _jsonl_count(history_jsonl)

    payload = _read_json(out_json)
    team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
    slate_summary = payload.get("slate_hits_allowed_context") or {}
    warnings = payload.get("warnings") or []
    return {
        "date": run_date,
        "status": "success" if exit_code == 0 and bool(payload.get("ok")) else "failed",
        "ok": bool(payload.get("ok")),
        "exit_code": exit_code,
        "evaluation_date": payload.get("evaluation_date"),
        "history_rows_appended": max(0, after_history_count - before_history_count),
        "team_eval_rows_in_snapshot": int(team_eval.get("rows_in_eval_snapshot") or 0),
        "team_eval_rows_with_expected": int(team_eval.get("rows_with_expected") or 0),
        "team_eval_rows_with_actual": int(team_eval.get("rows_with_actual") or 0),
        "hits_allowed_rows": int(slate_summary.get("rows") or _csv_row_count(out_csv)),
        "warnings_count": len(warnings),
        "warnings": warnings,
        "missing_artifact_reasons": [],
        "out_json": str(out_json),
        "out_csv": str(out_csv),
        "log_path": str(log_path),
    }


def _aggregate_hits_allowed_rows(run_rows: list[dict[str, Any]], aggregate_csv: Path) -> int:
    frames = []
    for row in run_rows:
        if row.get("status") != "success":
            continue
        csv_path = Path(str(row.get("out_csv") or ""))
        if not csv_path.exists() or _csv_row_count(csv_path) <= 0:
            continue
        df = pd.read_csv(csv_path)
        df.insert(0, "backfill_date", row["date"])
        frames.append(df)
    if not frames:
        aggregate_csv.write_text("", encoding="utf-8")
        return 0
    out = pd.concat(frames, ignore_index=True, sort=False)
    out.to_csv(aggregate_csv, index=False)
    return int(len(out))


def _run_subprocess(args: list[str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as fh:
        proc = subprocess.run(args, stdout=fh, stderr=subprocess.STDOUT, text=True, check=False)
    return int(proc.returncode)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _focus(summary: dict[str, Any], key: str) -> dict[str, Any]:
    focus = summary.get("focus_population") or {}
    value = focus.get(key) or {}
    return {
        "bets": int(value.get("bets") or 0),
        "roi": value.get("roi"),
        "win_rate": value.get("win_rate"),
        "units": value.get("units"),
    }


def _write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    comparison = summary.get("comparison_vs_production_history_only") or {}
    backfill_findings = comparison.get("backfill_favorites") or {}
    production_findings = comparison.get("production_favorites") or {}
    lines = [
        "# MLB Hits Environment Backfill 2026",
        "",
        "Non-production historical reconstruction and descriptive validation only.",
        "",
        "## Run Summary",
        f"- Date range: `{summary['date_range']['start']}` through `{summary['date_range']['end']}`",
        f"- Dates processed: `{summary['counts']['dates_processed']}`",
        f"- Successful dates: `{summary['counts']['successful_dates']}`",
        f"- Skipped dates: `{summary['counts']['skipped_dates']}`",
        f"- Failed dates: `{summary['counts']['failed_dates']}`",
        f"- Warning dates: `{summary['counts']['warning_dates']}`",
        f"- First successful date: `{summary['counts']['first_successful_date']}`",
        f"- Last successful date: `{summary['counts']['last_successful_date']}`",
        f"- History rows appended: `{summary['counts']['history_rows_appended']}`",
        f"- Aggregated hits-allowed rows: `{summary['counts']['aggregated_hits_allowed_rows']}`",
        "",
        "## Outputs",
        f"- History JSONL: `{summary['outputs']['history_jsonl']}`",
        f"- Team eval tracker: `{summary['outputs']['team_eval_tracker_csv']}`",
        f"- Hits-allowed rows: `{summary['outputs']['hits_allowed_rows_csv']}`",
        f"- Persistence out dir: `{summary['outputs']['persistence_out_dir']}`",
        "",
        "## Recurring Hostile / Non-Hostile Check",
        f"- Backfill hostile favorites: `{backfill_findings.get('all_hostile', {}).get('bets', 0)}` bets, ROI `{backfill_findings.get('all_hostile', {}).get('roi')}`, WR `{backfill_findings.get('all_hostile', {}).get('win_rate')}`",
        f"- Backfill non-hostile favorites: `{backfill_findings.get('all_non_hostile', {}).get('bets', 0)}` bets, ROI `{backfill_findings.get('all_non_hostile', {}).get('roi')}`, WR `{backfill_findings.get('all_non_hostile', {}).get('win_rate')}`",
        f"- Production-history hostile favorites: `{production_findings.get('all_hostile', {}).get('bets', 0)}` bets, ROI `{production_findings.get('all_hostile', {}).get('roi')}`, WR `{production_findings.get('all_hostile', {}).get('win_rate')}`",
        f"- Production-history non-hostile favorites: `{production_findings.get('all_non_hostile', {}).get('bets', 0)}` bets, ROI `{production_findings.get('all_non_hostile', {}).get('roi')}`, WR `{production_findings.get('all_non_hostile', {}).get('win_rate')}`",
        f"- Holds directionally: `{comparison.get('hostile_non_hostile_direction_holds')}`",
        "",
        "## Notes",
        "- Production/default hits-environment files were not used as outputs.",
        "- Existing persistence and interaction scripts were rerun against the isolated backfill outputs.",
        "- This does not add model logic, upload changes, lane changes, daily ops changes, or production filters.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start-date", default=DEFAULT_START)
    ap.add_argument("--end-date", default=DEFAULT_END)
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--persistence-out-dir", default=str(DEFAULT_PERSISTENCE_OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    persistence_out_dir = Path(args.persistence_out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    persistence_out_dir.mkdir(parents=True, exist_ok=True)

    history_jsonl = out_dir / "mlb_hits_environment_history_backfill_2026.jsonl"
    tracker_csv = out_dir / "mlb_hits_environment_team_eval_daily_tracker_backfill_2026.csv"
    hits_allowed_csv = out_dir / "mlb_hits_environment_hits_allowed_rows_backfill_2026.csv"
    summary_json = out_dir / "backfill_run_summary.json"
    summary_md = out_dir / "backfill_run_summary.md"

    for path in [history_jsonl, tracker_csv, hits_allowed_csv, summary_json, summary_md]:
        _reset_file(path)

    run_rows = []
    for run_date in _date_range(args.start_date, args.end_date):
        print(f"[backfill] {run_date}")
        run_rows.append(
            _run_report_for_date(
                run_date,
                out_dir=out_dir,
                history_jsonl=history_jsonl,
                tracker_csv=tracker_csv,
            )
        )

    aggregate_count = _aggregate_hits_allowed_rows(run_rows, hits_allowed_csv)

    logs_dir = out_dir / "logs"
    persistence_rc = _run_subprocess(
        [
            sys.executable,
            "backend/mlb/scripts/analyze_mlb_hits_environment_persistence.py",
            "--history-jsonl",
            str(history_jsonl),
            "--team-eval-tracker-csv",
            str(tracker_csv),
            "--hits-allowed-rows-csv",
            str(hits_allowed_csv),
            "--out-dir",
            str(persistence_out_dir),
        ],
        logs_dir / "analyze_mlb_hits_environment_persistence.log",
    )
    interaction_out_dir = persistence_out_dir / "v2_environment_interactions"
    interaction_rc = _run_subprocess(
        [
            sys.executable,
            "backend/mlb/scripts/analyze_mlb_v2_environment_interactions.py",
            "--regimes-csv",
            str(persistence_out_dir / "recurring_team_environment_regimes.csv"),
            "--out-dir",
            str(interaction_out_dir),
        ],
        logs_dir / "analyze_mlb_v2_environment_interactions_backfill.log",
    )
    favorites_rc = _run_subprocess(
        [
            sys.executable,
            "backend/mlb/scripts/analyze_mlb_v2_favorites_environment_breakdown.py",
            "--interaction-rows-csv",
            str(interaction_out_dir / "v2_environment_interaction_rows.csv"),
            "--regimes-csv",
            str(persistence_out_dir / "recurring_team_environment_regimes.csv"),
            "--out-dir",
            str(persistence_out_dir),
        ],
        logs_dir / "analyze_mlb_v2_favorites_environment_breakdown_backfill.log",
    )

    success_dates = [r["date"] for r in run_rows if r.get("status") == "success"]
    skipped_dates = [r["date"] for r in run_rows if r.get("status") == "skipped"]
    failed_dates = [r["date"] for r in run_rows if r.get("status") == "failed"]
    warning_dates = [r["date"] for r in run_rows if int(r.get("warnings_count") or 0) > 0]

    backfill_favorites_summary = _load_json(persistence_out_dir / "v2_favorites_environment_breakdown_summary.json")
    production_favorites_summary = _load_json(
        Path("artifacts/analysis/mlb/hits_environment_persistence/v2_favorites_environment_breakdown_summary.json")
    )
    backfill_hostile = _focus(backfill_favorites_summary, "all_hostile")
    backfill_non_hostile = _focus(backfill_favorites_summary, "all_non_hostile")
    prod_hostile = _focus(production_favorites_summary, "all_hostile")
    prod_non_hostile = _focus(production_favorites_summary, "all_non_hostile")
    backfill_delta = (
        None
        if backfill_hostile["roi"] is None or backfill_non_hostile["roi"] is None
        else float(backfill_non_hostile["roi"]) - float(backfill_hostile["roi"])
    )
    prod_delta = (
        None
        if prod_hostile["roi"] is None or prod_non_hostile["roi"] is None
        else float(prod_non_hostile["roi"]) - float(prod_hostile["roi"])
    )

    summary = {
        "date_range": {"start": args.start_date, "end": args.end_date},
        "outputs": {
            "history_jsonl": str(history_jsonl),
            "team_eval_tracker_csv": str(tracker_csv),
            "hits_allowed_rows_csv": str(hits_allowed_csv),
            "backfill_run_summary_json": str(summary_json),
            "backfill_run_summary_md": str(summary_md),
            "persistence_out_dir": str(persistence_out_dir),
        },
        "counts": {
            "dates_processed": len(run_rows),
            "successful_dates": len(success_dates),
            "skipped_dates": len(skipped_dates),
            "failed_dates": len(failed_dates),
            "warning_dates": len(warning_dates),
            "first_successful_date": min(success_dates) if success_dates else None,
            "last_successful_date": max(success_dates) if success_dates else None,
            "history_rows_appended": _jsonl_count(history_jsonl),
            "team_eval_tracker_rows": _csv_row_count(tracker_csv),
            "aggregated_hits_allowed_rows": aggregate_count,
        },
        "analysis_return_codes": {
            "persistence": persistence_rc,
            "v2_environment_interactions": interaction_rc,
            "v2_favorites_environment_breakdown": favorites_rc,
        },
        "date_results": run_rows,
        "comparison_vs_production_history_only": {
            "backfill_favorites": {
                "all_hostile": backfill_hostile,
                "all_non_hostile": backfill_non_hostile,
                "actual_hostile": _focus(backfill_favorites_summary, "actual_hostile"),
                "actual_non_hostile": _focus(backfill_favorites_summary, "actual_non_hostile"),
            },
            "production_favorites": {
                "all_hostile": prod_hostile,
                "all_non_hostile": prod_non_hostile,
                "actual_hostile": _focus(production_favorites_summary, "actual_hostile"),
                "actual_non_hostile": _focus(production_favorites_summary, "actual_non_hostile"),
            },
            "backfill_non_hostile_minus_hostile_roi": backfill_delta,
            "production_non_hostile_minus_hostile_roi": prod_delta,
            "hostile_non_hostile_direction_holds": bool(backfill_delta is not None and backfill_delta > 0),
        },
    }
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    _write_summary_md(summary_md, summary)
    print(json.dumps(summary["counts"], indent=2))


if __name__ == "__main__":
    main()
