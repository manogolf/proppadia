#!/usr/bin/env python3
"""Lightweight daily MLB feature-lineage health check.

This validates passive context survival in current-slate artifacts. It is
strict for slate/selector artifacts and advisory for upload diagnostics; final
8rain upload CSVs are intentionally out of scope.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.shared.market_audit_context import MARKET_AUDIT_CONTEXT_COLUMNS


CRITICAL_FIELDS = [
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "is_home",
    "team",
    "opponent",
    "team_id",
    "opponent_id",
]

BVP_COMPACT_FIELDS = [
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_avg",
    "bvp_slg",
    "bvp_payload_present",
    "bvp_source",
]
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
]
MARKET_AUDIT_FIELDS = MARKET_AUDIT_CONTEXT_COLUMNS

STRICT_ARTIFACTS = {"slate_output", "lane_selector_output", "ranking_upload_input", "quick_card_output"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_parent(path: Path) -> None:
    if path.parent and str(path.parent) not in {"", "."}:
        path.parent.mkdir(parents=True, exist_ok=True)


def _null_rate(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns or len(df) == 0:
        return None
    return float(df[col].isna().mean())


def _artifact_status(
    *,
    artifact_name: str,
    path: Path,
    warn_null_threshold: float,
) -> dict[str, Any]:
    required = artifact_name in STRICT_ARTIFACTS
    row: dict[str, Any] = {
        "artifact": artifact_name,
        "path": str(path),
        "required": required,
        "exists": path.exists(),
        "row_count": 0,
        "status": "pass",
        "issues": [],
        "fields": {},
    }
    if not path.exists():
        row["status"] = "fail" if required else "warn"
        row["issues"].append("file_missing")
        return row

    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        row["status"] = "fail" if required else "warn"
        row["issues"].append(f"read_failed:{type(exc).__name__}:{exc}")
        return row

    row["row_count"] = int(len(df))
    if len(df) == 0:
        row["status"] = "warn"
        row["issues"].append("empty_file")

    all_fields = CRITICAL_FIELDS + BVP_COMPACT_FIELDS + ROLLING_CONTEXT_FIELDS + MARKET_AUDIT_FIELDS
    direct_bvp_cols = ["bvp_plate_appearances", "bvp_at_bats", "bvp_hits", "bvp_total_bases"]
    direct_bvp_payload_rows = 0
    if all(col in df.columns for col in direct_bvp_cols) and len(df) > 0:
        direct_bvp_payload_rows = int(df[direct_bvp_cols].notna().any(axis=1).sum())
    row["bvp_direct_payload_rows"] = direct_bvp_payload_rows
    row["bvp_direct_payload_rate"] = float(direct_bvp_payload_rows / len(df)) if len(df) > 0 else None
    for field in all_fields:
        present = field in df.columns
        rate = _null_rate(df, field)
        info = {
            "present": bool(present),
            "null_rate": rate,
            "nonnull_count": int(df[field].notna().sum()) if present else 0,
        }
        if field == "bvp_payload_present" and present and len(df) > 0:
            truthy = df[field].astype(str).str.lower().isin(["true", "1", "1.0", "yes"])
            info["true_count"] = int(truthy.sum())
            info["true_rate"] = float(truthy.sum() / len(df))
        row["fields"][field] = info
        if not present:
            issue = f"{field}:missing"
            row["issues"].append(issue)
            if required:
                row["status"] = "fail"
            elif row["status"] == "pass":
                row["status"] = "warn"
        elif field in CRITICAL_FIELDS and rate is not None and rate > warn_null_threshold:
            row["issues"].append(f"{field}:null_rate>{warn_null_threshold:g}")
            if row["status"] == "pass":
                row["status"] = "warn"

    payload_info = row["fields"].get("bvp_payload_present") or {}
    source_info = row["fields"].get("bvp_source") or {}
    if direct_bvp_payload_rows > 0:
        payload_true_count = int(payload_info.get("true_count") or 0)
        source_nonnull_count = int(source_info.get("nonnull_count") or 0)
        if payload_true_count == 0:
            row["issues"].append("bvp_payload_present:no_true_values_with_direct_bvp")
            row["status"] = "fail" if required else "warn"
        if source_nonnull_count == 0:
            row["issues"].append("bvp_source:no_values_with_direct_bvp")
            row["status"] = "fail" if required else "warn"

    return row


def _derive_overall(artifacts: list[dict[str, Any]]) -> str:
    if any(a.get("status") == "fail" for a in artifacts):
        return "fail"
    if any(a.get("status") == "warn" for a in artifacts):
        return "warn"
    return "pass"


def _write_md(payload: dict[str, Any], out_md: Path) -> None:
    _ensure_parent(out_md)
    lines = [
        "# MLB Daily Feature Lineage Health",
        "",
        f"- generated_at_utc: `{payload.get('generated_at_utc')}`",
        f"- slate_date: `{payload.get('slate_date')}`",
        f"- status: `{payload.get('status')}`",
        f"- critical_fields: `{', '.join(payload.get('critical_fields') or [])}`",
        f"- bvp_compact_fields: `{', '.join(payload.get('bvp_compact_fields') or [])}`",
        f"- market_audit_fields: `{', '.join(payload.get('market_audit_fields') or [])}`",
        "",
        "| artifact | status | rows | missing_fields | high_null_fields | bvp_payload_rate | path |",
        "|---|---|---:|---|---|---:|---|",
    ]
    for artifact in payload.get("artifacts") or []:
        fields = artifact.get("fields") or {}
        missing = [name for name, info in fields.items() if not info.get("present")]
        high_null = [
            name
            for name, info in fields.items()
            if name in payload.get("critical_fields", [])
            and info.get("present")
            and info.get("null_rate") is not None
            and info.get("null_rate") > payload.get("warn_null_threshold", 0.0)
        ]
        bvp_payload = fields.get("bvp_payload_present") or {}
        bvp_payload_rate = bvp_payload.get("true_rate")
        bvp_payload_text = "n/a" if bvp_payload_rate is None else f"{float(bvp_payload_rate):.2%}"
        lines.append(
            f"| {artifact.get('artifact')} | `{artifact.get('status')}` | `{artifact.get('row_count')}` | "
            f"{', '.join(missing) or 'none'} | {', '.join(high_null) or 'none'} | "
            f"{bvp_payload_text} | `{artifact.get('path')}` |"
        )
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    artifacts = [
        _artifact_status(artifact_name="slate_output", path=Path(args.slate_output_csv), warn_null_threshold=args.warn_null_threshold),
        _artifact_status(artifact_name="lane_selector_output", path=Path(args.lane_selector_csv), warn_null_threshold=args.warn_null_threshold),
        _artifact_status(artifact_name="ranking_upload_input", path=Path(args.ranking_upload_input_csv), warn_null_threshold=args.warn_null_threshold),
        _artifact_status(artifact_name="quick_card_output", path=Path(args.quick_card_csv), warn_null_threshold=args.warn_null_threshold),
        _artifact_status(artifact_name="ranking_upload_diagnostics", path=Path(args.ranking_upload_diagnostics_csv), warn_null_threshold=args.warn_null_threshold),
        _artifact_status(artifact_name="quick_card_upload_diagnostics", path=Path(args.quick_card_upload_diagnostics_csv), warn_null_threshold=args.warn_null_threshold),
    ]
    status = _derive_overall(artifacts)
    payload = {
        "generated_at_utc": _utc_now_iso(),
        "slate_date": args.date,
        "status": status,
        "ok": status == "pass",
        "warn_null_threshold": args.warn_null_threshold,
        "critical_fields": CRITICAL_FIELDS,
            "bvp_compact_fields": BVP_COMPACT_FIELDS,
            "rolling_context_fields": ROLLING_CONTEXT_FIELDS,
            "market_audit_fields": MARKET_AUDIT_FIELDS,
            "artifacts": artifacts,
            "summary": {
            "artifact_count": len(artifacts),
            "pass_count": sum(1 for a in artifacts if a.get("status") == "pass"),
            "warn_count": sum(1 for a in artifacts if a.get("status") == "warn"),
            "fail_count": sum(1 for a in artifacts if a.get("status") == "fail"),
            "missing_required_columns": sorted(
                {
                    f"{a.get('artifact')}:{field}"
                    for a in artifacts
                    if a.get("artifact") in STRICT_ARTIFACTS
                    for field, info in (a.get("fields") or {}).items()
                    if not info.get("present")
                }
            ),
            "bvp_missing_required_columns": sorted(
                {
                    f"{a.get('artifact')}:{field}"
                    for a in artifacts
                    if a.get("artifact") in STRICT_ARTIFACTS
                    for field, info in (a.get("fields") or {}).items()
                    if field in BVP_COMPACT_FIELDS and not info.get("present")
                }
            ),
            "bvp_artifacts_with_payload": sum(
                1
                for a in artifacts
                if (((a.get("fields") or {}).get("bvp_payload_present") or {}).get("true_count") or 0) > 0
            ),
            "bvp_payload_rates": {
                str(a.get("artifact")): (((a.get("fields") or {}).get("bvp_payload_present") or {}).get("true_rate"))
                for a in artifacts
                if "bvp_payload_present" in (a.get("fields") or {})
            },
            "rolling_missing_required_columns": sorted(
                {
                    f"{a.get('artifact')}:{field}"
                    for a in artifacts
                    if a.get("artifact") in STRICT_ARTIFACTS
                    for field, info in (a.get("fields") or {}).items()
                    if field in ROLLING_CONTEXT_FIELDS and not info.get("present")
                }
            ),
            "market_audit_missing_required_columns": sorted(
                {
                    f"{a.get('artifact')}:{field}"
                    for a in artifacts
                    if a.get("artifact") in STRICT_ARTIFACTS
                    for field, info in (a.get("fields") or {}).items()
                    if field in MARKET_AUDIT_FIELDS and not info.get("present")
                }
            ),
            "market_audit_nonnull_rates": {
                str(a.get("artifact")): {
                    field: (
                        None
                        if not ((a.get("fields") or {}).get(field) or {}).get("present")
                        else 1.0 - float(((a.get("fields") or {}).get(field) or {}).get("null_rate") or 0.0)
                    )
                    for field in MARKET_AUDIT_FIELDS
                }
                for a in artifacts
            },
        },
    }

    dated_json = Path(args.out_json)
    latest_json = Path(args.latest_json)
    _ensure_parent(dated_json)
    dated_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if latest_json != dated_json:
        _ensure_parent(latest_json)
        latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.out_md:
        _write_md(payload, Path(args.out_md))

    print(
        "[mlb-feature-lineage-health] "
        f"slate_date={args.date} status={status} "
        f"pass={payload['summary']['pass_count']} warn={payload['summary']['warn_count']} "
        f"fail={payload['summary']['fail_count']} out_json={dated_json}"
    )
    return 2 if status == "fail" else 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Check daily MLB feature lineage context fields in current-slate artifacts.")
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--slate-output-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--lane-selector-csv", default="")
    ap.add_argument("--ranking-upload-input-csv", default="")
    ap.add_argument("--quick-card-csv", default="")
    ap.add_argument("--ranking-upload-diagnostics-csv", default="")
    ap.add_argument("--quick-card-upload-diagnostics-csv", default="")
    ap.add_argument("--warn-null-threshold", type=float, default=0.05)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--latest-json", default="artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_latest.json")
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    date_value = args.date
    if not args.lane_selector_csv:
        args.lane_selector_csv = f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}.csv"
    if not args.ranking_upload_input_csv:
        args.ranking_upload_input_csv = (
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}_ranking_upload_input.csv"
        )
    if not args.quick_card_csv:
        args.quick_card_csv = f"backend/mlb/exports/model_v2/lanes/today/{date_value}/quick_card_hits_{date_value}.csv"
    if not args.ranking_upload_diagnostics_csv:
        args.ranking_upload_diagnostics_csv = (
            f"backend/mlb/exports/model_v2/upload/{date_value}/ranking_tool_upload_diagnostics_{date_value}.csv"
        )
    if not args.quick_card_upload_diagnostics_csv:
        args.quick_card_upload_diagnostics_csv = (
            f"backend/mlb/exports/model_v2/upload/{date_value}/quick_card_tool_upload_diagnostics_{date_value}.csv"
        )
    if not args.out_json:
        args.out_json = f"artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_{date_value}.json"
    return args


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
