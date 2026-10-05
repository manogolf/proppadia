"""Version an Oct 4 SOG evidence-gap cause annotation without changing evidence."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.nhl.daily_capture import sha256_file, verify_package
from backend.nhl.performance_summary import _write_immutable_package
from backend.nhl.sog_coverage_annotations import (
    NORMAL_MARKET_OBSERVED, NORMAL_MARKET_UNMATCHED, PIPELINE_REPAIR_DELAY,
    POSTSTART_INELIGIBLE, PRESTART_EVIDENCE_UNAVAILABLE, SCHEMA_VERSION,
    validate_annotation,
)

ROOT = Path(__file__).resolve().parents[3]
SLATE = "2026-10-04"
GAME_ID = 2026020035
RECON_DIR = (ROOT / "artifacts/operational/nhl/sog_market_coverage_reconstructions"
             / "season=2026" / f"slate_date={SLATE}"
             / "reconstruction=f0d43f3f34bdf6a752cc")
SUMMARY_DIR = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/learning_restatements"
               / SLATE / "performance_summary=40df4e6557c45d8d69e0")


def annotate() -> tuple[Path, Path]:
    recon_manifest_sha = verify_package(RECON_DIR)
    recon_path = RECON_DIR / "coverage.json"
    recon = json.loads(recon_path.read_text())
    if recon.get("slate_date") != SLATE or recon.get("status") != "PASS":
        raise ValueError("SOG_RECONSTRUCTION_SOURCE_INVALID")
    game = recon["game_coverage"].get(str(GAME_ID))
    if not game or game.get("selected_source_run_id") is not None:
        raise ValueError("ANNOTATED_GAME_HAS_PRESTART_SOURCE")
    affected = int(game["prediction_keys"])
    if affected != int(game["poststart_excluded"]) or affected <= 0:
        raise ValueError("PIPELINE_GAP_KEY_COUNT_MISMATCH")
    annotation_row = {
        "slate_date": SLATE,
        "game_id": GAME_ID,
        "coverage_status": "NO_VALID_PRESTART_EVIDENCE",
        "cause_classification": PIPELINE_REPAIR_DELAY,
        "cause_scope": "OPERATIONAL_PIPELINE",
        "affected_proposition_keys": affected,
        "market_absence_inferred": False,
        "bookmaker_behavior_inferred": False,
        "prediction_absence_inferred": False,
        "recoverable_after_start": False,
        "notes": "pregame evidence lost because SOG market-retention/binding repair was not completed before puck drop",
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "classification": "OCT4_SOG_OPERATIONAL_COVERAGE_GAP_ANNOTATION",
        "slate_date": SLATE,
        "source_reconstruction_path": str(RECON_DIR.resolve()),
        "source_reconstruction_identity": recon["reconstruction_identity"],
        "source_reconstruction_manifest_sha256": recon_manifest_sha,
        "source_reconstruction_sha256": sha256_file(recon_path),
        "game_annotations": [annotation_row],
        "coverage_state_counts": {
            NORMAL_MARKET_OBSERVED: int(recon["counts"]["prestart_matched"]),
            NORMAL_MARKET_UNMATCHED: int(recon["counts"]["prestart_unmatched"]),
            POSTSTART_INELIGIBLE: 0,
            PRESTART_EVIDENCE_UNAVAILABLE: 0,
            PIPELINE_REPAIR_DELAY: affected,
        },
        "matched_unmatched_poststart_counts_unchanged": True,
    }
    validate_annotation(payload, slate_date=SLATE)
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "source_reconstruction_manifest_sha256": recon_manifest_sha,
        "source_reconstruction_identity": recon["reconstruction_identity"],
        "annotation": annotation_row,
        "coverage_state_counts": payload["coverage_state_counts"],
    }
    identity = hashlib.sha256(json.dumps(
        identity_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    sidecar = (RECON_DIR.parent / f"coverage_gap_annotation={identity[:20]}")
    if sidecar.exists():
        verify_package(sidecar)
    else:
        sidecar.mkdir(parents=True)
        (sidecar / "annotation.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n")
        (sidecar / "RUN_COMPLETE.json").write_text(json.dumps({
            "status": "COMPLETE", "schema_version": SCHEMA_VERSION,
            "annotation_identity": identity,
        }, indent=2, sort_keys=True) + "\n")
        files = sorted(p for p in sidecar.iterdir() if p.is_file())
        (sidecar / "SHA256SUMS").write_text("".join(
            f"{sha256_file(path)}  {path.name}\n" for path in files))
    sidecar_manifest_sha = verify_package(sidecar)

    source_summary_path = SUMMARY_DIR / "performance_summary.json"
    old = json.loads(source_summary_path.read_text())
    coverage = old["models"]["sog"]["market_coverage"]
    if (coverage.get("matched"), coverage.get("unmatched"), coverage.get("poststart_excluded")) != (
            recon["counts"]["prestart_matched"], recon["counts"]["prestart_unmatched"],
            recon["counts"]["poststart_excluded_prediction_keys"]):
        raise ValueError("SUMMARY_RECONSTRUCTION_COUNTS_DIVERGED")
    summary = json.loads(json.dumps(old))
    summary_coverage = summary["models"]["sog"]["market_coverage"]
    summary_coverage["cause_annotations"] = [annotation_row]
    summary_coverage["coverage_state_counts"] = payload["coverage_state_counts"]
    summary_coverage["cause_annotation_path"] = str((sidecar / "annotation.json").resolve())
    summary_coverage["cause_annotation_sha256"] = sha256_file(sidecar / "annotation.json")
    summary_coverage["cause_annotation_package_manifest_sha256"] = sidecar_manifest_sha
    summary_payload = {
        "prior_summary_identity": old["summary_identity"],
        "cause_annotation_package_manifest_sha256": sidecar_manifest_sha,
        "source_reconstruction_manifest_sha256": recon_manifest_sha,
    }
    summary_identity = hashlib.sha256(json.dumps(
        summary_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    summary["summary_identity"] = summary_identity
    summary_dir = SUMMARY_DIR.parent / f"performance_summary={summary_identity[:20]}"
    summary["summary_package"] = str(summary_dir.resolve())
    summary["source_artifacts"]["sog_coverage_cause_annotation_sha256"] = sha256_file(
        sidecar / "annotation.json")
    summary["source_artifacts"]["sog_coverage_cause_annotation_package_manifest_sha256"] = sidecar_manifest_sha
    _write_immutable_package(summary_dir, summary)
    return sidecar, summary_dir


if __name__ == "__main__":
    annotation_dir, summary_dir = annotate()
    print(json.dumps({"annotation_package": str(annotation_dir),
                      "performance_summary": str(summary_dir)}, sort_keys=True))
