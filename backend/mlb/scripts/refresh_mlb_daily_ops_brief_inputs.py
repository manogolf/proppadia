#!/usr/bin/env python3
"""Refresh and assert date-owned inputs for the MLB daily ops brief."""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional


@dataclass
class RefreshResult:
    name: str
    artifact: str
    expected_date: str
    actual_date: str
    status: str
    command: str
    detail: str = ""


def _run(name: str, cmd: list[str], *, allow_fail: bool = False, timeout_sec: int = 0) -> tuple[bool, str]:
    print(f"[mlb-daily-ops-inputs] refresh {name}: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(cmd, text=True, start_new_session=True)
        proc.wait(timeout=timeout_sec if timeout_sec > 0 else None)
    except subprocess.TimeoutExpired:
        detail = f"timeout_sec={timeout_sec}"
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                pass
        if not allow_fail:
            return False, detail
        print(f"[mlb-daily-ops-inputs] WARN {name} refresh timed out {detail}; continuing")
        return False, detail
    if proc.returncode == 0:
        return True, ""
    detail = f"rc={proc.returncode}"
    if not allow_fail:
        return False, detail
    print(f"[mlb-daily-ops-inputs] WARN {name} refresh failed {detail}; continuing")
    return False, detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _json_date(path: Path, keys: Iterable[str]) -> str:
    obj = _load_json(path)
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key)
    return str(cur or "")[:10]


def _max_csv_date(path: Path, columns: Iterable[str]) -> str:
    if not path.exists():
        return ""
    vals: list[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                for col in columns:
                    raw = str(row.get(col) or "").strip()
                    if len(raw) >= 10:
                        vals.append(raw[:10])
        return max(vals) if vals else ""
    except Exception:
        return ""


def _record(
    results: list[RefreshResult],
    *,
    name: str,
    artifact: Path,
    expected_date: str,
    actual_date: str,
    command: list[str],
    refresh_ok: bool,
    required: bool = True,
    detail: str = "",
) -> None:
    if not artifact.exists() and detail.startswith("dependency_missing:"):
        status = "dependency_missing"
    elif refresh_ok and actual_date == expected_date:
        status = "refreshed"
    elif not refresh_ok:
        status = "refresh_failed"
    elif not artifact.exists():
        status = "dependency_missing"
    elif required:
        status = "stale_after_refresh"
    else:
        status = "not_fresh"
    results.append(
        RefreshResult(
            name=name,
            artifact=str(artifact),
            expected_date=expected_date,
            actual_date=actual_date or "",
            status=status,
            command=" ".join(command),
            detail=detail,
        )
    )


def _print_assertions(results: list[RefreshResult]) -> None:
    for row in results:
        if row.status in {"dependency_missing", "stale_after_refresh", "refresh_failed"}:
            print(
                "[mlb-daily-ops-inputs] "
                f"{row.status}: artifact={row.artifact} expected={row.expected_date} "
                f"actual={row.actual_date or 'n/a'} refresh_command='{row.command}' detail={row.detail or 'n/a'}"
            )


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Refresh MLB daily ops brief input artifacts.")
    ap.add_argument("--completed-slate-date", required=True)
    ap.add_argument("--current-slate-date", required=True)
    ap.add_argument("--reconcile-rows-csv", required=True)
    ap.add_argument("--model-vs-fade-json", required=True)
    ap.add_argument("--model-vs-fade-csv", required=True)
    ap.add_argument("--all-available-json", required=True)
    ap.add_argument("--all-available-csv", required=True)
    ap.add_argument("--postgrade-alerts-json", required=True)
    ap.add_argument("--postgrade-alerts-history-jsonl", required=True)
    ap.add_argument("--postgrade-tracker-csv", required=True)
    ap.add_argument("--postgrade-by-prop-tracker-csv", required=True)
    ap.add_argument("--graded-summary-json", required=True)
    ap.add_argument("--graded-by-prop-csv", required=True)
    ap.add_argument("--book-upload-csv", required=True)
    ap.add_argument("--model-performance-summary-csv", required=True)
    ap.add_argument("--model-performance-daily-csv", required=True)
    ap.add_argument("--reporting-alignment-csv", required=True)
    ap.add_argument("--reporting-alignment-md", required=True)
    ap.add_argument("--prop-regime-csv", required=True)
    ap.add_argument("--hits-environment-json", required=True)
    ap.add_argument("--bvp-impact-json", required=True)
    ap.add_argument("--overlap-watch-json", required=True)
    ap.add_argument(
        "--hits-15-tier-backtest-json",
        default="artifacts/analysis/mlb/review_aids/hits_15_tier_backtest_summary.json",
    )
    ap.add_argument(
        "--review-aid-performance-json",
        default="artifacts/analysis/mlb/review_aids/performance/review_aid_performance_summary.json",
    )
    ap.add_argument(
        "--total-bases-shadow-summary-json",
        default="artifacts/analysis/mlb/model_quality/total_bases_shadow/{current_slate_date}/total_bases_shadow_summary_{current_slate_date}.json",
    )
    ap.add_argument(
        "--total-bases-shadow-evaluation-json",
        default="artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json",
    )
    ap.add_argument("--brief-output-md", required=True)
    ap.add_argument(
        "--status-json",
        default="artifacts/analysis/mlb/mlb_daily_ops_brief_input_refresh_latest.json",
    )
    ap.add_argument("--refresh-bvp-impact", type=int, default=1)
    ap.add_argument(
        "--bvp-impact-timeout-sec",
        type=int,
        default=180,
        help="Max seconds to allow optional BvP impact refresh before marking refresh_failed.",
    )
    ap.add_argument("--allow-graded-date-mismatch", type=int, default=1)
    args = ap.parse_args(argv)

    completed = str(args.completed_slate_date).strip()
    current = str(args.current_slate_date).strip()
    py = sys.executable
    results: list[RefreshResult] = []

    reconcile_rows = Path(args.reconcile_rows_csv)
    if not reconcile_rows.exists():
        cmd = ["dependency", "check", str(reconcile_rows)]
        for name, artifact in (
            ("model_vs_fade", Path(args.model_vs_fade_json)),
            ("all_available", Path(args.all_available_json)),
            ("postgrade_alerts", Path(args.postgrade_alerts_json)),
        ):
            _record(
                results,
                name=name,
                artifact=artifact,
                expected_date=completed,
                actual_date="",
                command=cmd,
                refresh_ok=False,
                detail=f"dependency_missing:{reconcile_rows}",
            )
    else:
        mvf_cmd = [
            py,
            "backend/mlb/scripts/report_mlb_model_vs_fade.py",
            "--rows-csv",
            str(reconcile_rows),
            "--out-json",
            args.model_vs_fade_json,
            "--out-csv",
            args.model_vs_fade_csv,
        ]
        ok, detail = _run("model_vs_fade", mvf_cmd)
        _record(
            results,
            name="model_vs_fade",
            artifact=Path(args.model_vs_fade_json),
            expected_date=completed,
            actual_date=_json_date(Path(args.model_vs_fade_json), ("window", "game_date_max")),
            command=mvf_cmd,
            refresh_ok=ok,
            detail=detail,
        )

        all_cmd = [
            py,
            "backend/mlb/scripts/report_mlb_all_available.py",
            "--rows-csv",
            str(reconcile_rows),
            "--out-json",
            args.all_available_json,
            "--out-csv",
            args.all_available_csv,
        ]
        ok, detail = _run("all_available", all_cmd)
        _record(
            results,
            name="all_available",
            artifact=Path(args.all_available_json),
            expected_date=completed,
            actual_date=_json_date(Path(args.all_available_json), ("window", "game_date_max")),
            command=all_cmd,
            refresh_ok=ok,
            detail=detail,
        )

        post_cmd = [
            py,
            "backend/mlb/scripts/mlb_postgrade_tracker.py",
            "--date",
            completed,
            "--model-vs-fade-summary-json",
            args.model_vs_fade_json,
            "--all-available-summary-json",
            args.all_available_json,
            "--all-available-by-prop-csv",
            args.all_available_csv,
            "--graded-summary-json",
            args.graded_summary_json,
            "--graded-by-prop-csv",
            args.graded_by_prop_csv,
            "--book-upload-csv",
            args.book_upload_csv,
            "--out-csv",
            args.postgrade_tracker_csv,
            "--out-by-prop-csv",
            args.postgrade_by_prop_tracker_csv,
            "--alerts-out-json",
            args.postgrade_alerts_json,
            "--alerts-history-jsonl",
            args.postgrade_alerts_history_jsonl,
            "--skip-charts",
        ]
        if int(args.allow_graded_date_mismatch) == 1:
            post_cmd.append("--allow-graded-date-mismatch")
        ok, detail = _run("postgrade_alerts", post_cmd)
        _record(
            results,
            name="postgrade_alerts",
            artifact=Path(args.postgrade_alerts_json),
            expected_date=completed,
            actual_date=_json_date(Path(args.postgrade_alerts_json), ("report_date",)),
            command=post_cmd,
            refresh_ok=ok,
            detail=detail,
        )

    perf_cmd = [
        "make",
        "mlb-model-performance-by-prop",
        f"MLB_MODEL_PERFORMANCE_TO_DATE={completed}",
        "MLB_MODEL_PERFORMANCE_SOURCE_TYPE=full_slate_model_pick",
        f"MLB_MODEL_PERFORMANCE_SUMMARY_CSV={args.model_performance_summary_csv}",
        f"MLB_MODEL_PERFORMANCE_DAILY_CSV={args.model_performance_daily_csv}",
    ]
    ok, detail = _run("model_performance", perf_cmd)
    perf_actual = _max_csv_date(Path(args.model_performance_daily_csv), ("game_date", "date", "report_date"))
    _record(
        results,
        name="model_performance",
        artifact=Path(args.model_performance_daily_csv),
        expected_date=completed,
        actual_date=perf_actual,
        command=perf_cmd,
        refresh_ok=ok,
        detail=detail,
    )

    align_cmd = [
        "make",
        "mlb-reporting-alignment-audit",
        f"MLB_REPORTING_ALIGNMENT_DATE={completed}",
        f"MLB_REPORTING_ALIGNMENT_OUT_CSV={args.reporting_alignment_csv}",
        f"MLB_REPORTING_ALIGNMENT_OUT_MD={args.reporting_alignment_md}",
    ]
    ok, detail = _run("reporting_alignment", align_cmd)
    _record(
        results,
        name="reporting_alignment",
        artifact=Path(args.reporting_alignment_csv),
        expected_date=completed,
        actual_date=completed if Path(args.reporting_alignment_csv).exists() else "",
        command=align_cmd,
        refresh_ok=ok,
        detail=detail,
    )

    regime_cmd = ["make", "mlb-prop-regime-validation", f"MLB_PROP_REGIME_DEPLOY_CSV={args.prop_regime_csv}"]
    ok, detail = _run("prop_regime", regime_cmd)
    _record(
        results,
        name="prop_regime",
        artifact=Path(args.prop_regime_csv),
        expected_date=completed,
        actual_date=_max_csv_date(Path(args.prop_regime_csv), ("latest_usable_date",)),
        command=regime_cmd,
        refresh_ok=ok,
        detail=detail,
    )

    hits_cmd = [
        "make",
        "mlb-hits-environment-report",
        f"MLB_HITS_ENV_AS_OF_DATE={current}",
        f"MLB_HITS_ENV_SLATE_DATE={current}",
        f"MLB_HITS_ENV_OUT_JSON={args.hits_environment_json}",
    ]
    ok, detail = _run("hits_environment", hits_cmd, allow_fail=True)
    _record(
        results,
        name="hits_environment",
        artifact=Path(args.hits_environment_json),
        expected_date=current,
        actual_date=_json_date(Path(args.hits_environment_json), ("requested_as_of_date",)),
        command=hits_cmd,
        refresh_ok=ok,
        detail=detail,
    )

    if int(args.refresh_bvp_impact) == 1:
        bvp_cmd = [
            "make",
            "mlb-bvp-impact-report",
            f"MLB_BVP_IMPACT_LABEL_DATE={current}",
            f"MLB_BVP_IMPACT_OUT_JSON={args.bvp_impact_json}",
        ]
        ok, detail = _run("bvp_impact", bvp_cmd, allow_fail=True, timeout_sec=max(1, int(args.bvp_impact_timeout_sec)))
        _record(
            results,
            name="bvp_impact",
            artifact=Path(args.bvp_impact_json),
            expected_date=current,
            actual_date=_json_date(Path(args.bvp_impact_json), ("label_date",)),
            command=bvp_cmd,
            refresh_ok=ok,
            required=False,
            detail=detail,
        )

    overlap_watch_cmd = [
        py,
        "backend/mlb/scripts/build_mlb_ranking_qc_overlap_watch.py",
    ]
    ok, detail = _run("ranking_qc_overlap_watch", overlap_watch_cmd, allow_fail=True)
    _record(
        results,
        name="ranking_qc_overlap_watch",
        artifact=Path(args.overlap_watch_json),
        expected_date=completed,
        actual_date=_json_date(Path(args.overlap_watch_json), ("composition_diagnostics", "latest_completed_slate")),
        command=overlap_watch_cmd,
        refresh_ok=ok,
        detail=detail,
    )

    hits_15_tier_cmd = ["make", "mlb-refresh-hits-15-tier-backtest"]
    ok, detail = _run("hits_15_tier_backtest", hits_15_tier_cmd, allow_fail=True)
    hits_15_tier_path = Path(args.hits_15_tier_backtest_json)
    _record(
        results,
        name="hits_15_tier_backtest",
        artifact=hits_15_tier_path,
        expected_date=completed,
        actual_date=_json_date(hits_15_tier_path, ("latest_completed_slate",)),
        command=hits_15_tier_cmd,
        refresh_ok=ok,
        required=False,
        detail=detail,
    )

    review_aid_performance_cmd = [
        "make",
        "mlb-review-aid-performance",
        f"MLB_DAILY_RECONCILE_DATE={completed}",
    ]
    ok, detail = _run("review_aid_performance", review_aid_performance_cmd, allow_fail=True)
    review_aid_performance_path = Path(args.review_aid_performance_json)
    _record(
        results,
        name="review_aid_performance",
        artifact=review_aid_performance_path,
        expected_date=completed,
        actual_date=_json_date(review_aid_performance_path, ("latest_completed_slate",)),
        command=review_aid_performance_cmd,
        refresh_ok=ok,
        required=False,
        detail=detail,
    )

    # The balanced and unweighted Total Bases shadows were retired when the
    # certified UBO-5 TB1.5 established-hitter production route was activated.

    _print_assertions(results)
    refreshed_count = sum(1 for row in results if row.status == "refreshed")
    refresh_failed_count = sum(1 for row in results if row.status == "refresh_failed")
    stale_after_refresh_count = sum(1 for row in results if row.status == "stale_after_refresh")
    dependency_missing_count = sum(1 for row in results if row.status == "dependency_missing")

    print(
        "[mlb-daily-ops-inputs] summary "
        f"completed_slate_date={completed} current_slate_date={current} "
        f"refreshed_artifact_count={refreshed_count} "
        f"refresh_failed_count={refresh_failed_count} "
        f"stale_after_refresh_count={stale_after_refresh_count} "
        f"dependency_missing_count={dependency_missing_count} "
        f"brief_output_path={args.brief_output_md}"
    )
    status_payload = {
        "completed_slate_date": completed,
        "current_slate_date": current,
        "reconcile_rows_csv": str(reconcile_rows),
        "reconcile_rows_exists": reconcile_rows.exists(),
        "refreshed_artifact_count": refreshed_count,
        "refresh_failed_count": refresh_failed_count,
        "stale_after_refresh_count": stale_after_refresh_count,
        "dependency_missing_count": dependency_missing_count,
        "brief_output_path": args.brief_output_md,
        "results": [row.__dict__ for row in results],
    }
    status_path = Path(args.status_json)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[mlb-daily-ops-inputs] status_json={status_path}")
    if stale_after_refresh_count or dependency_missing_count:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
