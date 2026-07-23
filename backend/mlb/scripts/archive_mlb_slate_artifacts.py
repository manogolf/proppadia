#!/usr/bin/env python3
"""
Archive MLB per-slate artifacts for replay/reconciliation.

Phase-1 intent:
- keep one durable folder per slate under backend/mlb/exports/odds_history/YYYY-MM-DD/
- preserve model artifacts and the exact odds snapshot used for prediction build
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
DEFAULT_ODDS_ROOT = BASE_DIR / "mlb" / "exports" / "odds_history"
DEFAULT_PRED_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"
DEFAULT_SLATE_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_slate_output.csv"
DEFAULT_BOOK_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_book_upload.csv"


@dataclass
class CopyResult:
    source: str
    destination: str
    copied: bool
    reason: str
    bytes: int


def _date_et_today() -> str:
    return datetime.now(ET).date().isoformat()


def _sanitize_run_tag(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text[:96]


def _copy_artifact(src: Path, dst: Path) -> CopyResult:
    if not src.exists():
        return CopyResult(str(src), str(dst), False, "missing", 0)
    if src.is_dir():
        return CopyResult(str(src), str(dst), False, "source_is_directory", 0)
    if src.stat().st_size <= 0:
        return CopyResult(str(src), str(dst), False, "empty_file", 0)

    dst.parent.mkdir(parents=True, exist_ok=True)
    src_resolved = src.resolve()
    dst_resolved = dst.resolve() if dst.exists() else dst
    if src_resolved == dst_resolved:
        return CopyResult(str(src), str(dst), True, "already_in_archive", int(src.stat().st_size))

    shutil.copy2(src, dst)
    return CopyResult(str(src), str(dst), True, "copied", int(dst.stat().st_size))


def _write_manifest(
    *,
    manifest_path: Path,
    slate_date: str,
    archive_dir: Path,
    copy_results: List[CopyResult],
) -> None:
    copied = [r for r in copy_results if r.copied]
    missing = [r for r in copy_results if not r.copied]
    payload = {
        "slate_date": str(slate_date),
        "captured_at_utc": datetime.now(UTC).isoformat(),
        "archive_dir": str(archive_dir),
        "copied_count": len(copied),
        "missing_count": len(missing),
        "copied_bytes": sum(int(r.bytes) for r in copied),
        "artifacts": [
            {
                "source": r.source,
                "destination": r.destination,
                "copied": bool(r.copied),
                "reason": r.reason,
                "bytes": int(r.bytes),
            }
            for r in copy_results
        ],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive MLB slate artifacts under backend/mlb/exports/odds_history/YYYY-MM-DD/")
    ap.add_argument("--slate-date", default=os.environ.get("MLB_DATE", ""), help="YYYY-MM-DD (ET). Default: today ET")
    ap.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT), help="Archive root directory")
    ap.add_argument("--pred-csv", default=str(DEFAULT_PRED_CSV), help="Predictions wide CSV path")
    ap.add_argument("--slate-csv", default=str(DEFAULT_SLATE_CSV), help="Canonical slate output CSV path")
    ap.add_argument("--book-upload-csv", default=str(DEFAULT_BOOK_CSV), help="Book upload CSV path")
    ap.add_argument(
        "--odds-snapshot-json",
        default=os.environ.get("MLB_ODDS_SNAPSHOT_JSON", ""),
        help="Optional odds snapshot JSON source to archive as odds_mlb_playerprops.json",
    )
    ap.add_argument(
        "--run-tag",
        default=os.environ.get("MLB_ARCHIVE_RUN_TAG", ""),
        help="Optional tag to keep a per-run odds snapshot copy in the date archive folder.",
    )
    ap.add_argument(
        "--manifest-json",
        default="",
        help="Optional explicit manifest path. Default: <archive_dir>/manifest.json",
    )
    ap.add_argument("--strict", action="store_true", help="Fail when any expected artifact is missing")
    ap.add_argument("--ubo5-feature-ledger", default="")
    ap.add_argument("--ubo5-route-ledger", default="")
    ap.add_argument("--ubo5-route-health", default="")
    ap.add_argument("--ubo5-board-md", default="")
    ap.add_argument("--ubo5-board-csv", default="")
    args = ap.parse_args()

    slate_date = str(args.slate_date or "").strip() or _date_et_today()
    archive_dir = Path(str(args.odds_root)).expanduser() / slate_date
    archive_dir.mkdir(parents=True, exist_ok=True)

    pred_csv = Path(str(args.pred_csv)).expanduser()
    slate_csv = Path(str(args.slate_csv)).expanduser()
    book_csv = Path(str(args.book_upload_csv)).expanduser()
    odds_snapshot_src: Optional[Path] = None
    if str(args.odds_snapshot_json or "").strip():
        odds_snapshot_src = Path(str(args.odds_snapshot_json)).expanduser()
    run_tag = _sanitize_run_tag(str(args.run_tag or ""))

    copy_plan: List[tuple[Path, Path]] = [
        (pred_csv, archive_dir / pred_csv.name),
        (slate_csv, archive_dir / slate_csv.name),
        (book_csv, archive_dir / book_csv.name),
    ]
    for raw in (
        args.ubo5_feature_ledger,
        args.ubo5_route_ledger,
        args.ubo5_route_health,
        args.ubo5_board_md,
        args.ubo5_board_csv,
    ):
        if str(raw).strip():
            source = Path(str(raw)).expanduser()
            copy_plan.append((source, archive_dir / source.name))
    if run_tag:
        copy_plan.extend(
            [
                (pred_csv, archive_dir / f"{pred_csv.stem}__{run_tag}{pred_csv.suffix}"),
                (slate_csv, archive_dir / f"{slate_csv.stem}__{run_tag}{slate_csv.suffix}"),
                (book_csv, archive_dir / f"{book_csv.stem}__{run_tag}{book_csv.suffix}"),
            ]
        )
    if odds_snapshot_src is not None:
        copy_plan.append((odds_snapshot_src, archive_dir / "odds_mlb_playerprops.json"))
        # Keep legacy-compatible filename in sync for reconcile/report defaults.
        copy_plan.append((odds_snapshot_src, archive_dir / "odds_latest_compatible.json"))
        if run_tag:
            copy_plan.append((odds_snapshot_src, archive_dir / f"odds_mlb_playerprops__{run_tag}.json"))

    results: List[CopyResult] = []
    for src, dst in copy_plan:
        results.append(_copy_artifact(src, dst))

    manifest_path = (
        Path(str(args.manifest_json)).expanduser()
        if str(args.manifest_json or "").strip()
        else archive_dir / "manifest.json"
    )
    _write_manifest(
        manifest_path=manifest_path,
        slate_date=slate_date,
        archive_dir=archive_dir,
        copy_results=results,
    )

    copied = [r for r in results if r.copied]
    missing = [r for r in results if not r.copied]
    copied_names = ", ".join(Path(r.destination).name for r in copied) if copied else "none"
    missing_names = ", ".join(Path(r.source).name for r in missing) if missing else "none"
    print(f"[mlb-archive] slate={slate_date} archive_dir={archive_dir}")
    print(f"[mlb-archive] copied={len(copied)} files ({copied_names})")
    print(f"[mlb-archive] missing={len(missing)} files ({missing_names})")
    print(f"[mlb-archive] manifest={manifest_path}")

    if args.strict and missing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
