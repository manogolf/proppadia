#!/usr/bin/env python3
"""Write a prod12 release manifest with artifact checksums and contract metadata."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.scripts.check_season_baseline_artifacts import _latest_file


def _split_csv(raw: str) -> list[str]:
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _collect_artifacts(artifact_dirs: list[str], patterns: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    seen: set[Path] = set()
    records: list[dict[str, Any]] = []
    warnings: list[str] = []

    for dir_value in artifact_dirs:
        root = Path(dir_value)
        if not root.exists():
            warnings.append(f"artifact_dir_missing:{root}")
            continue
        if not root.is_dir():
            warnings.append(f"artifact_dir_not_directory:{root}")
            continue
        for pattern in patterns:
            for file_path in root.rglob(pattern):
                if not file_path.is_file() or file_path in seen:
                    continue
                seen.add(file_path)
                st = file_path.stat()
                records.append(
                    {
                        "path": str(file_path),
                        "size_bytes": int(st.st_size),
                        "modified_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                        "sha256": _sha256(file_path),
                    }
                )

    records.sort(key=lambda item: item["path"])
    return records, warnings


def _load_last_jsonl_row(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    last_line = ""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                last_line = line
    if not last_line:
        return None
    try:
        payload = json.loads(last_line)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_payload(args: argparse.Namespace) -> dict[str, Any]:
    artifact_dirs = _split_csv(args.artifact_dirs)
    patterns = _split_csv(args.artifact_patterns)
    if not artifact_dirs:
        artifact_dirs = ["models_out"]
    if not patterns:
        patterns = ["*.joblib", "*.pkl", "*.onnx", "*.bin"]

    baseline_path = Path(args.baseline_path) if str(args.baseline_path).strip() else _latest_file(Path(args.baseline_dir), "mlb_quality_*.json")
    pipeline_last = _load_last_jsonl_row(Path(args.pipeline_history))
    artifacts, warnings = _collect_artifacts(artifact_dirs, patterns)

    if baseline_path is None:
        warnings.append("baseline_missing:mlb_quality_*.json")

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass",
        "ok": True,
        "lane_group": "prod12_release",
        "release_contract": {
            "prop_types": _split_csv(args.prop_types),
            "quality_window_mode": "games",
            "quality_games_back": int(args.quality_games_back),
            "quality_min_total": int(args.quality_min_total),
            "quality_min_accuracy_pct": float(args.quality_min_accuracy),
            "candidate_max_prop_drop_pct": float(args.max_prop_drop_pct),
        },
        "inputs": {
            "baseline_path": str(baseline_path) if baseline_path else None,
            "pipeline_history": str(args.pipeline_history),
            "artifact_dirs": artifact_dirs,
            "artifact_patterns": patterns,
        },
        "artifacts": {
            "count": len(artifacts),
            "total_size_bytes": sum(int(item["size_bytes"]) for item in artifacts),
            "files": artifacts,
        },
        "latest_pipeline_snapshot": pipeline_last,
        "warnings": warnings,
    }

    if baseline_path is None:
        payload["ok"] = False
        payload["status"] = "fail"
        payload["failures"] = ["baseline_missing"]
    elif args.require_artifacts and not artifacts:
        payload["ok"] = False
        payload["status"] = "fail"
        payload["failures"] = ["artifacts_missing"]
    else:
        payload["failures"] = []

    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Write MLB prod12 release manifest and checksums.")
    ap.add_argument("--output", default="artifacts/releases/mlb_prod12_release_manifest.json")
    ap.add_argument("--artifact-dirs", default="models_out", help="Comma-separated directories to scan recursively.")
    ap.add_argument("--artifact-patterns", default="*.joblib,*.pkl,*.onnx,*.bin", help="Comma-separated glob patterns.")
    ap.add_argument("--baseline-path", default="", help="Optional explicit baseline JSON path.")
    ap.add_argument("--baseline-dir", default="artifacts/season_baselines")
    ap.add_argument("--pipeline-history", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--prop-types", default="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis")
    ap.add_argument("--quality-games-back", type=int, default=30)
    ap.add_argument("--quality-min-total", type=int, default=1000)
    ap.add_argument("--quality-min-accuracy", type=float, default=48.0)
    ap.add_argument("--max-prop-drop-pct", type=float, default=3.5)
    ap.add_argument("--require-artifacts", action="store_true")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    payload = _build_payload(args)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
