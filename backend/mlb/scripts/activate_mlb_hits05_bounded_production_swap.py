#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


DATE_VALUE = "2026-07-19"
PACKAGE_DIR = Path("artifacts/analysis/model_development/mlb_hits05_bounded_production_swap/2026-07-19")
SOURCE_MODEL = Path(
    "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/"
    "2026-07-19/candidate_a_poisson_count_research_only.joblib"
)
SOURCE_MANIFEST = Path(
    "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/"
    "2026-07-19/frozen_feature_manifest_2026-07-19.csv"
)
SOURCE_CANDIDATE_META = Path(
    "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/"
    "2026-07-19/machine_readable_hits05_replacement_candidate_2026-07-19.json"
)
PRODUCTION_MODEL = Path("models_out/latest/hits_05_full_spine.joblib")
PRODUCTION_MANIFEST = Path("models_out/latest/hits_05_full_spine_feature_manifest.json")
PRODUCTION_METADATA = Path("models_out/latest/hits_05_full_spine_metadata.json")
INCUMBENT_HITS_MODEL = Path("models_out/latest/hits.joblib")
CURRENT_PARENT_ROOT = Path("artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/2026-07-19")

EXPECTED_MODEL_SHA256 = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(path: Path, rows: Iterable[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_feature_manifest() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    with SOURCE_MANIFEST.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            used = str(row.get("used") or "").strip().lower() == "true"
            if not used:
                continue
            rows.append(
                {
                    "feature_name": row.get("feature_name"),
                    "feature_family": row.get("feature_family"),
                    "source_lineage": row.get("source_lineage"),
                    "temporal_semantics": row.get("temporal_semantics"),
                    "missing_value_policy": row.get("missing_value_policy"),
                    "current_replay_availability": row.get("current_replay_availability"),
                    "historical_coverage_pct": row.get("historical_coverage_pct"),
                    "notes": row.get("notes"),
                }
            )
    return rows


def activate(*, output_dir: Path) -> Dict[str, object]:
    generated_at = datetime.now(timezone.utc).isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)

    for required in (SOURCE_MODEL, SOURCE_MANIFEST, SOURCE_CANDIDATE_META, INCUMBENT_HITS_MODEL):
        if not required.exists():
            raise FileNotFoundError(required)

    source_sha = _sha256(SOURCE_MODEL)
    if source_sha != EXPECTED_MODEL_SHA256:
        raise RuntimeError(f"candidate sha mismatch: {source_sha} != {EXPECTED_MODEL_SHA256}")

    pre_swap_rows = []
    for path, role in (
        (INCUMBENT_HITS_MODEL, "incumbent_hits_model_preserved"),
        (Path("backend/mlb/scripts/build_mlb_slate_output.py"), "production_slate_output_router_before_swap"),
        (Path("backend/mlb/scripts/build_mlb_predictions_wide.py"), "production_wide_prediction_builder_before_swap"),
        (Path("backend/mlb/data/processed/mlb_slate_output.csv"), "current_canonical_slate_output_before_swap"),
        (Path("backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv"), "current_canonical_wide_before_swap"),
    ):
        if path.exists():
            stat = path.stat()
            pre_swap_rows.append(
                {
                    "role": role,
                    "path": str(path),
                    "exists": True,
                    "size_bytes": stat.st_size,
                    "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                    "sha256": _sha256(path),
                }
            )
        else:
            pre_swap_rows.append(
                {"role": role, "path": str(path), "exists": False, "size_bytes": "", "mtime_utc": "", "sha256": ""}
            )
    _write_csv(
        output_dir / "hits05_pre_swap_runtime_inventory_2026-07-19.csv",
        pre_swap_rows,
        ["role", "path", "exists", "size_bytes", "mtime_utc", "sha256"],
    )

    PRODUCTION_MODEL.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SOURCE_MODEL, PRODUCTION_MODEL)

    feature_rows = _read_feature_manifest()
    manifest_payload = {
        "schema_version": "hits05_full_spine_feature_manifest_v1",
        "created_at_utc": generated_at,
        "source_manifest": str(SOURCE_MANIFEST),
        "source_manifest_sha256": _sha256(SOURCE_MANIFEST),
        "feature_count": len(feature_rows),
        "features": feature_rows,
    }
    PRODUCTION_MANIFEST.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    candidate_meta = json.loads(SOURCE_CANDIDATE_META.read_text(encoding="utf-8"))
    metadata_payload = {
        "schema_version": "hits05_full_spine_production_metadata_v1",
        "enabled": True,
        "created_at_utc": generated_at,
        "prop_type": "hits",
        "line": 0.5,
        "probability_orientation": "P_OVER_0_5 = P(at least one hit); P_UNDER_0_5 = 1 - P_OVER_0_5",
        "active_calibrator": "none_raw_candidate",
        "model_artifact": str(PRODUCTION_MODEL),
        "model_sha256": _sha256(PRODUCTION_MODEL),
        "expected_model_sha256": EXPECTED_MODEL_SHA256,
        "feature_manifest": str(PRODUCTION_MANIFEST),
        "feature_manifest_sha256": _sha256(PRODUCTION_MANIFEST),
        "source_model": str(SOURCE_MODEL),
        "source_candidate_metadata": str(SOURCE_CANDIDATE_META),
        "current_parent_root": str(CURRENT_PARENT_ROOT),
        "fallback_policy": "incumbent_hits_model_for_all_non_hits05_rows_or_missing_certified_current_parent",
        "rollback_command": "MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT=0 make mlb-daily-capture",
        "hits15_status": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "candidate_gate_context": candidate_meta.get("gate_context") or candidate_meta.get("candidate_context") or {},
    }
    PRODUCTION_METADATA.write_text(json.dumps(metadata_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    artifact_rows = []
    for path, role in (
        (PRODUCTION_MODEL, "production_candidate_model"),
        (PRODUCTION_MANIFEST, "production_candidate_feature_manifest"),
        (PRODUCTION_METADATA, "production_candidate_metadata"),
        (SOURCE_MODEL, "source_candidate_model"),
        (SOURCE_MANIFEST, "source_feature_manifest"),
        (SOURCE_CANDIDATE_META, "source_candidate_metadata"),
    ):
        stat = path.stat()
        artifact_rows.append(
            {
                "role": role,
                "path": str(path),
                "size_bytes": stat.st_size,
                "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                "sha256": _sha256(path),
            }
        )
    _write_csv(
        output_dir / "hits05_production_artifact_inventory_2026-07-19.csv",
        artifact_rows,
        ["role", "path", "size_bytes", "mtime_utc", "sha256"],
    )

    decisions = [
        ("MLB_HITS05_PRE_SWAP_INCUMBENT_FREEZE_DECISION", "FROZEN_INCUMBENT_HITS_MODEL_AND_RUNTIME_OUTPUTS"),
        ("MLB_HITS05_PRODUCTION_ARTIFACT_BINDING_DECISION", "BOUND_DISTINCT_HITS05_FULL_SPINE_ARTIFACT"),
        ("MLB_HITS05_HYBRID_ROUTING_DECISION", "HITS05_ONLY_CANDIDATE_WITH_INCUMBENT_FALLBACK"),
        ("MLB_HITS05_CURRENT_PARENT_INTEGRATION_DECISION", "USES_CERTIFIED_CURRENT_PARENT_WHEN_AVAILABLE_NO_NEW_CAPTURE_WINDOW"),
        ("MLB_HITS05_ROW_PROVENANCE_DECISION", "PROVENANCE_COLUMNS_ADDED_TO_SLATE_ROWS"),
        ("MLB_HITS05_PROBABILITY_ORIENTATION_DECISION", "P_OVER_0_5_EQUALS_AT_LEAST_ONE_HIT"),
        ("MLB_HITS05_BETONLINE_EXECUTION_SEPARATION_DECISION", "UNCHANGED_DIRECT_PRICE_FAIL_CLOSED"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
    ]
    _write_csv(
        output_dir / "hits05_production_swap_decisions_2026-07-19.csv",
        [{"decision": k, "value": v} for k, v in decisions],
        ["decision", "value"],
    )

    summary = f"""# MLB Hits 0.5 Bounded Production Swap - 2026-07-19

Generated: `{generated_at}`

## Bound Candidate

- Production artifact: `{PRODUCTION_MODEL}`
- SHA256: `{_sha256(PRODUCTION_MODEL)}`
- Feature manifest: `{PRODUCTION_MANIFEST}`
- Feature count: `{len(feature_rows)}`
- Active calibrator: `none_raw_candidate`
- Scope: `prop_type=hits`, `line=0.5`

## Guardrails

- `models_out/latest/hits.joblib` remains the incumbent for Hits 1.5 and fallback Hits 0.5 rows.
- The replacement activates only when a certified current parent score exists for the exact player-game.
- Rollback command: `MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT=0 make mlb-daily-capture`
- No OddsAPI, DB write, wager execution, training, recalibration, or schedule change is performed by this installer.
"""
    (output_dir / "hits05_bounded_production_swap_2026-07-19.md").write_text(summary, encoding="utf-8")

    return {
        "output_dir": str(output_dir),
        "production_model": str(PRODUCTION_MODEL),
        "production_model_sha256": _sha256(PRODUCTION_MODEL),
        "feature_count": len(feature_rows),
        "metadata": str(PRODUCTION_METADATA),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(PACKAGE_DIR))
    args = parser.parse_args()
    result = activate(output_dir=Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
