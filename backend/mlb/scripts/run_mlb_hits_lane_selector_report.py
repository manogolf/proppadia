#!/usr/bin/env python3
"""One-command daily runner/report for the MLB hits lane selector."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
UPLOAD_ROOT = Path("backend/mlb/exports/model_v2/upload")
RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DAILY_SCRIPT = Path("backend/mlb/scripts/run_mlb_hits_lane_selector_daily.py")
RESULTS_SCRIPT = Path("backend/mlb/scripts/compare_hits_lane_selector_to_results.py")
QUICK_CARD_UPLOAD_SCRIPT = Path("backend/mlb/scripts/export_quick_card_hits_tool_upload.py")


def _lane_date_dir(date_value: str) -> Path:
    return LANE_ROOT / date_value


def _upload_date_dir(date_value: str) -> Path:
    return UPLOAD_ROOT / date_value


def _dated_or_legacy(date_value: str, filename: str) -> Path:
    dated = _lane_date_dir(date_value) / filename
    if dated.exists():
        return dated
    return LANE_ROOT / filename


def _upload_dated_or_legacy(date_value: str, filename: str) -> Path:
    dated = _upload_date_dir(date_value) / filename
    if dated.exists():
        return dated
    return UPLOAD_ROOT / filename


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        raise SystemExit(f"Invalid --date: {value}")
    return dt.date().isoformat()


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_csv(path, low_memory=False)))
    except Exception:
        return 0


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "n/a"


def _fmt_units(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):+.2f}"
    except Exception:
        return "n/a"


def _metric_by_group(results: dict[str, Any], group: str) -> list[dict[str, Any]]:
    return [m for m in results.get("metrics", []) if m.get("group") == group]


def _write_md(
    *,
    path: Path,
    date_value: str,
    selector_summary: dict[str, Any],
    upload_diag: dict[str, Any],
    upload_rows: int,
    quick_upload_rows: int,
    quick_rows: int,
    results_summary: dict[str, Any],
    selector_proc: subprocess.CompletedProcess[str] | None,
    results_proc: subprocess.CompletedProcess[str] | None,
) -> None:
    counts = selector_summary.get("counts_by_lane", {})
    identity = selector_summary.get("upload_identity_validation", {})
    overall_results = next(iter(_metric_by_group(results_summary, "overall")), {})
    quick_warning = selector_summary.get("quick_card_warning", "")
    lines = [
        f"# Hits Lane Selector Daily Report - {date_value}",
        "",
        "## Selector",
        f"- Mode: `{selector_summary.get('mode', 'unknown')}`",
        f"- Note: `{selector_summary.get('note', '')}`",
        f"- Selector rows: `{selector_summary.get('total_selected', 0)}`",
        f"- Ranking upload input rows: `{upload_diag.get('ranking_upload_input_rows', 0)}`",
        f"- Ranking upload rows: `{upload_rows}`",
        f"- Quick Card upload rows: `{quick_upload_rows}`",
        f"- Combined tool-upload rows: `{upload_rows + quick_upload_rows}`",
        f"- Quick Card rows: `{quick_rows}`",
        f"- Quick Card sent to ranking upload: `{upload_diag.get('quick_card_lane', {}).get('sent_to_ranking_upload')}`",
        f"- Quick Card source existed before: `{selector_summary.get('quick_card_source_exists_before')}`",
        f"- Quick Card builder ran: `{selector_summary.get('quick_card_builder_ran')}`",
        f"- Quick Card source exists after: `{selector_summary.get('quick_card_source_exists_after')}`",
        f"- Quick Card hits rows: `{selector_summary.get('quick_card_hits_rows', quick_rows)}`",
        f"- Quick Card warning: `{quick_warning}`",
        "",
        "## Rows By Lane",
    ]
    for lane, row in counts.items():
        lines.append(f"- `{lane}`: `{row.get('count', 0)}` rows | avg odds `{row.get('avg_odds')}`")

    lines.extend(
        [
            "",
            "## Upload Diagnostics",
            f"- Excluded low-sample rows: `{upload_diag.get('excluded_low_sample', 0)}`",
            f"- Excluded unmapped rows: `{upload_diag.get('excluded_unmapped_bucket', 0)}`",
            f"- Excluded missing required fields: `{upload_diag.get('excluded_missing_required_fields', 0)}`",
            f"- Would pass with allow-low-sample: `{upload_diag.get('would_pass_with_allow_low_sample_upload', 0)}`",
            "",
            "## Upload Identity",
            f"- Raw HOME/AWAY teams: `{identity.get('raw_teams', [])}`",
            f"- Normalized HOME/AWAY teams: `{identity.get('upload_teams', [])}`",
            f"- Team match ok true: `{identity.get('team_match_ok_true', 0)}`",
            f"- Team match ok false: `{identity.get('team_match_ok_false', 0)}`",
            f"- Team normalizer: `{identity.get('team_normalizer', '')}`",
            f"- Team alias map: `{identity.get('team_alias_map', {})}`",
        ]
    )

    if results_summary:
        lines.extend(
            [
                "",
                "## Results",
                f"- Resolved rows: `{results_summary.get('rows_with_resolved_pnl', 0)}`",
                f"- Missing outcome rows: `{results_summary.get('missing_outcome_rows', 0)}`",
                f"- Win rate: `{_fmt_pct(overall_results.get('win_rate'))}`",
                f"- ROI: `{_fmt_pct(overall_results.get('roi'))}`",
                f"- Units: `{_fmt_units(overall_results.get('units'))}`",
                "",
                "## Results By Lane",
            ]
        )
        for metric in _metric_by_group(results_summary, "by_lane"):
            lines.append(
                f"- `{metric.get('value')}`: `{metric.get('bets')}` bets | "
                f"WR `{_fmt_pct(metric.get('win_rate'))}` | ROI `{_fmt_pct(metric.get('roi'))}` | "
                f"units `{_fmt_units(metric.get('units'))}`"
            )
    else:
        note = selector_summary.get("note") or "Outcomes unavailable or results summary not produced."
        lines.extend(["", "## Results", f"- {note}"])

    lines.extend(
        [
            "",
            "## Commands",
            f"- Selector command status: `{selector_proc.returncode if selector_proc else 'skipped'}`",
            f"- Results command status: `{results_proc.returncode if results_proc else 'skipped'}`",
            "",
            "Lane rules unchanged: UNDER 0.5 top decile, OVER bucket 9, Quick Card separated from ranking upload.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    date_value = _date_key(args.date)
    date_dir = _lane_date_dir(date_value)
    selector_csv = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}.csv")
    selector_summary_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_summary.json")
    upload_diag_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_upload_diagnostics.json")
    quick_card_csv = _dated_or_legacy(date_value, f"quick_card_hits_{date_value}.csv")
    upload_csv = _upload_dated_or_legacy(date_value, f"ranking_tool_upload_{date_value}.csv")
    quick_upload_csv = _upload_date_dir(date_value) / f"quick_card_tool_upload_{date_value}.csv"
    quick_upload_diag_csv = _upload_date_dir(date_value) / f"quick_card_tool_upload_diagnostics_{date_value}.csv"
    results_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_results_summary.json")
    md_path = date_dir / f"hits_lane_selector_{date_value}_daily_report.md"

    selector_proc: subprocess.CompletedProcess[str] | None = None
    if not args.skip_run_selector:
        cmd = [sys.executable, str(DAILY_SCRIPT), "--date", date_value]
        if args.allow_low_sample_upload:
            cmd.append("--allow-low-sample-upload")
        if args.drop_team_mismatch_upload:
            cmd.append("--drop-team-mismatch-upload")
        selector_proc = _run(cmd)
        if selector_proc.returncode != 0:
            print(selector_proc.stdout)
            print(selector_proc.stderr, file=sys.stderr)
            raise SystemExit(selector_proc.returncode)
        selector_csv = date_dir / f"hits_lane_selector_{date_value}.csv"
        selector_summary_json = date_dir / f"hits_lane_selector_{date_value}_summary.json"
        upload_diag_json = date_dir / f"hits_lane_selector_{date_value}_upload_diagnostics.json"
        quick_card_csv = date_dir / f"quick_card_hits_{date_value}.csv"
        results_json = date_dir / f"hits_lane_selector_{date_value}_results_summary.json"
        upload_csv = _upload_date_dir(date_value) / f"ranking_tool_upload_{date_value}.csv"

    selector_summary = _load_json(selector_summary_json)
    upload_diag = _load_json(upload_diag_json)
    upload_rows = _csv_rows(upload_csv)
    quick_rows = _csv_rows(quick_card_csv)
    quick_upload_proc = _run(
        [
            sys.executable,
            str(QUICK_CARD_UPLOAD_SCRIPT),
            "--date",
            date_value,
            "--in-csv",
            str(quick_card_csv),
            "--out-csv",
            str(quick_upload_csv),
            "--diagnostics-csv",
            str(quick_upload_diag_csv),
        ]
    )
    if quick_upload_proc.returncode != 0:
        print(quick_upload_proc.stdout)
        print(quick_upload_proc.stderr, file=sys.stderr)
        raise SystemExit(quick_upload_proc.returncode)
    quick_upload_rows = _csv_rows(quick_upload_csv)
    mode = selector_summary.get("mode") or ("postgame" if (RECONCILE_ROOT / date_value / "reconcile_rows.csv").exists() else "pregame")

    results_proc: subprocess.CompletedProcess[str] | None = None
    if mode != "pregame" and (RECONCILE_ROOT / date_value / "reconcile_rows.csv").exists():
        results_proc = _run([sys.executable, str(RESULTS_SCRIPT), "--date", date_value])
        if results_proc.returncode != 0:
            print(results_proc.stdout)
            print(results_proc.stderr, file=sys.stderr)
        results_summary = _load_json(results_json)
    else:
        results_summary = {}

    _write_md(
        path=md_path,
        date_value=date_value,
        selector_summary=selector_summary,
        upload_diag=upload_diag,
        upload_rows=upload_rows,
        quick_upload_rows=quick_upload_rows,
        quick_rows=quick_rows,
        results_summary=results_summary,
        selector_proc=selector_proc,
        results_proc=results_proc,
    )

    overall = next(iter(_metric_by_group(results_summary, "overall")), {})
    print(f"Hits lane selector report: {date_value}")
    print(f"mode={mode}")
    if mode == "pregame":
        print("note=no outcomes available")
    print(f"selector_rows={selector_summary.get('total_selected', 0)}")
    print(f"rows_by_lane={json.dumps(selector_summary.get('counts_by_lane', {}), sort_keys=True)}")
    print(f"ranking_upload_input_rows={upload_diag.get('ranking_upload_input_rows', 0)}")
    print(f"ranking_upload_rows={upload_rows}")
    print(f"quick_card_upload_rows={quick_upload_rows}")
    print(f"combined_tool_upload_rows={upload_rows + quick_upload_rows}")
    print(f"excluded_low_sample={upload_diag.get('excluded_low_sample', 0)}")
    print(f"excluded_unmapped={upload_diag.get('excluded_unmapped_bucket', 0)}")
    print(f"quick_card_rows={quick_rows}")
    print(f"quick_card_source_exists_before={selector_summary.get('quick_card_source_exists_before')}")
    print(f"quick_card_builder_ran={selector_summary.get('quick_card_builder_ran')}")
    print(f"quick_card_source_exists_after={selector_summary.get('quick_card_source_exists_after')}")
    print(f"quick_card_hits_rows={selector_summary.get('quick_card_hits_rows', quick_rows)}")
    if selector_summary.get("quick_card_warning"):
        print(f"quick_card_warning={selector_summary.get('quick_card_warning')}")
    print(f"quick_card_sent_to_ranking_upload={upload_diag.get('quick_card_lane', {}).get('sent_to_ranking_upload')}")
    identity = selector_summary.get("upload_identity_validation", {})
    print(f"raw_home_away_teams={json.dumps(identity.get('raw_teams', []))}")
    print(f"normalized_home_away_teams={json.dumps(identity.get('upload_teams', []))}")
    print(
        "team_match_ok "
        f"true={identity.get('team_match_ok_true', 0)} false={identity.get('team_match_ok_false', 0)}"
    )
    print(f"team_match_false_rows={json.dumps(identity.get('false_rows', []), sort_keys=True)}")
    print(f"team_normalizer={identity.get('team_normalizer', '')}")
    print(f"team_alias_map={json.dumps(identity.get('team_alias_map', {}), sort_keys=True)}")
    if results_summary:
        print(
            "results "
            f"resolved={results_summary.get('rows_with_resolved_pnl', 0)} "
            f"win_rate={_fmt_pct(overall.get('win_rate'))} "
            f"roi={_fmt_pct(overall.get('roi'))} "
            f"units={_fmt_units(overall.get('units'))}"
        )
        by_lane = {
            m.get("value"): {
                "bets": m.get("bets"),
                "win_rate": _fmt_pct(m.get("win_rate")),
                "roi": _fmt_pct(m.get("roi")),
                "units": _fmt_units(m.get("units")),
            }
            for m in _metric_by_group(results_summary, "by_lane")
        }
        print(f"results_by_lane={json.dumps(by_lane, sort_keys=True)}")
    else:
        print("results unavailable")
    print(f"markdown_report={md_path}")
    print("confirmed_no_lane_rule_changes=true")

    return {
        "date": date_value,
        "markdown_report": str(md_path),
        "selector_rows": selector_summary.get("total_selected", 0),
        "ranking_upload_rows": upload_rows,
        "quick_card_upload_rows": quick_upload_rows,
        "combined_tool_upload_rows": upload_rows + quick_upload_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run/report MLB hits lane selector in one command.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--skip-run-selector", action="store_true")
    parser.add_argument("--allow-low-sample-upload", action="store_true")
    parser.add_argument("--drop-team-mismatch-upload", action="store_true")
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
