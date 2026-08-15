from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14"

REQUIRED = {
    "hits05_frozen_modeling_procedure.md",
    "hits05_frozen_modeling_procedure.json",
    "hits05_external_source_manifest.csv",
    "hits05_2026_official_game_spine.csv",
    "hits05_2026_player_eligibility_spine.csv",
    "hits05_feature_reconstruction_registry.csv",
    "hits05_initial_training_manifest.csv",
    "hits05_walk_forward_fit_manifest.csv",
    "hits05_slate_snapshot_policy.md",
    "hits05_external_acquisition_log.csv",
    "hits05_reconstruction_coverage.csv",
    "hits05_walk_forward_prediction_ledger.csv",
    "hits05_prediction_hash_manifest.json",
    "hits05_outcome_ledger.csv",
    "hits05_slate_scorecard.csv",
    "hits05_cumulative_scorecard.csv",
    "hits05_monthly_scorecard.csv",
    "hits05_baseline_comparison.csv",
    "hits05_confidence_ordering.csv",
    "hits05_calibration_bands.csv",
    "hits05_clustered_uncertainty.csv",
    "hits05_august_live_reference_comparison.csv",
    "hits05_betonline_reference_comparison.csv",
    "hits05_stitched_vs_first_principles_comparison.md",
    "hits05_first_principles_evidence_assessment.md",
    "concise_mlb_hits05_2026_first_principles_season_rebuild_v1.md",
    "reproducibility_hashes.csv",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    top_files = {path.name for path in OUT.iterdir() if path.is_file()}
    missing = sorted(REQUIRED - top_files)
    unexpected = sorted(top_files - REQUIRED)

    spine = pd.read_csv(OUT / "hits05_2026_official_game_spine.csv")
    eligibility = pd.read_csv(OUT / "hits05_2026_player_eligibility_spine.csv")
    registry = pd.read_csv(OUT / "hits05_feature_reconstruction_registry.csv")
    fits = pd.read_csv(OUT / "hits05_walk_forward_fit_manifest.csv")
    predictions = pd.read_csv(OUT / "hits05_walk_forward_prediction_ledger.csv")
    outcomes = pd.read_csv(OUT / "hits05_outcome_ledger.csv")
    scorecard = pd.read_csv(OUT / "hits05_slate_scorecard.csv")
    acquisition = pd.read_csv(OUT / "hits05_external_acquisition_log.csv")

    prediction_manifest = json.loads((OUT / "hits05_prediction_hash_manifest.json").read_text())
    prediction_hash_ok = prediction_manifest["sha256"] == sha(OUT / prediction_manifest["ledger"])
    prediction_rows_ok = prediction_manifest["rows"] == len(predictions)

    fit_hash_mismatches = []
    for row in fits.itertuples(index=False):
        artifact = ROOT / row.artifact_path
        if not artifact.is_file() or sha(artifact) != row.fitted_artifact_sha256:
            fit_hash_mismatches.append(row.fit_id)

    repro = pd.read_csv(OUT / "reproducibility_hashes.csv")
    repro_mismatches = []
    for row in repro.itertuples(index=False):
        path = ROOT / row.path
        if not path.is_file() or sha(path) != row.sha256 or path.stat().st_size != row.bytes:
            repro_mismatches.append(row.path)

    forbidden_outcome_tokens = ("hit_1plus", "actual_hits", "plate_appearances", "appearance_status")
    outcome_columns_in_predictions = sorted(set(predictions.columns).intersection(forbidden_outcome_tokens))

    checks = {
        "required_deliverables_present": not missing,
        "no_unexpected_top_level_files": not unexpected,
        "official_game_pk_unique": not spine.game_pk.duplicated().any(),
        "official_game_count": int(len(spine)),
        "official_slates": int(spine.date.nunique()),
        "eligibility_identity_unique": not eligibility.identity.duplicated().any(),
        "prediction_identity_unique": not predictions.identity.duplicated().any(),
        "outcome_identity_unique": not outcomes.identity.duplicated().any(),
        "duplicate_prediction_identities": int(predictions.identity.duplicated().sum()),
        "prediction_rows": int(len(predictions)),
        "outcome_rows": int(len(outcomes)),
        "resolved_rows": int(outcomes.hit_1plus.notna().sum()),
        "resolved_not_above_predictions": int(outcomes.hit_1plus.notna().sum()) <= len(predictions),
        "prediction_outcome_columns_absent": not outcome_columns_in_predictions,
        "prediction_hash_ok": prediction_hash_ok,
        "prediction_manifest_rows_ok": prediction_rows_ok,
        "feature_registry_rows": int(len(registry)),
        "feature_registry_exactly_73": len(registry) == 73,
        "fit_count": int(len(fits)),
        "fit_artifact_hashes_ok": not fit_hash_mismatches,
        "training_population_hashes_present": bool(fits.training_population_hash.notna().all()),
        "scorecard_slates": int(len(scorecard)),
        "acquisition_failures": int(acquisition.failures.fillna(0).sum()),
        "reproducibility_hashes_ok": not repro_mismatches,
    }
    failures = sorted(key for key, value in checks.items() if isinstance(value, bool) and not value)
    result = {
        "status": "PASS" if not failures else "FAIL",
        "checks": checks,
        "missing_deliverables": missing,
        "unexpected_top_level_files": unexpected,
        "prediction_outcome_columns": outcome_columns_in_predictions,
        "fit_hash_mismatches": fit_hash_mismatches,
        "reproducibility_hash_mismatches": repro_mismatches,
        "failures": failures,
    }
    print(json.dumps(result, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
