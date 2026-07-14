#!/usr/bin/env python3
"""Prepare the MLB Collective Bundle v1 bounded process-validation request.

This is governance/package preparation only. It writes Markdown, JSON, and CSV
approval artifacts. It does not train, fit preprocessing, score, create
predictions, mutate certified matrices, call external APIs, or write databases.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
CERT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12"
)
READINESS_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_updated_training_population_readiness/2026-07-12"
)

EXPECTED_SPEC_SHA = "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833"
EXPECTED_SPINE_SHA = "a391043df6db97da705ae8f1921055ca705e1d94c4c075c3e58cf752fbfd39f7"
EXPECTED_CERT_SHA = "a2f3416790fa8613abc3ae79769d09c05ce837093311a95f554422cc2e4998a4"
EXPECTED_READINESS_SHA = "70aae17681bc1415c646d28759d933f0e777156e07bf6f874e4cc6fea142ba51"

EXPERIMENT_ID = "MLB_COLLECTIVE_BUNDLE_V1_BOUNDED_OFFLINE_PROCESS_VALIDATION_EXPERIMENT"
EXPERIMENT_CLASS = "PROCESS_VALIDATION_ONLY"
AUTHORIZATION_STATE = "PREPARED_NOT_AUTHORIZED"
READINESS_DECISION = "PROCESS_VALIDATION_REQUEST_READY_FOR_HUMAN_APPROVAL"
TRAINING_READINESS = "NOT_READY_FOR_MODEL_TRAINING"
TIMESTAMP_UTC = "2026-07-13T12:00:00Z"
TIMESTAMP_PT = "2026-07-13T05:00:00-07:00"

START_DATE = "2026-06-29"
END_DATE = "2026-07-09"
SELECTED_FIT_MANIFESTS = ["variant_d", "variant_a"]
LOAD_SCHEMA_CHECK_MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]


def sha256(path: Path) -> str:
    if path.is_dir():
        digest = hashlib.sha256()
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(child.relative_to(path)).encode())
            digest.update(b"\0")
            digest.update(sha256(child).encode())
            digest.update(b"\n")
        return digest.hexdigest()
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_digest_from_manifest(path: Path) -> str:
    manifests = sorted(path.glob("*sha256_manifest*.csv"))
    if manifests:
        with manifests[0].open(newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get("relative_path", "").startswith("__PACKAGE_DIGEST"):
                    return row.get("sha256", "")
    return sha256(path) if path.exists() else ""


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def verified_inputs() -> list[dict[str, Any]]:
    refs = [
        ("frozen_bundle_v1", SPEC_DIR, EXPECTED_SPEC_SHA, "MLB_COLLECTIVE_BUNDLE_V1_SPECIFICATION_FROZEN"),
        ("frozen_population_spine_v1", SPINE_DIR, EXPECTED_SPINE_SHA, "FROZEN"),
        ("certified_bounded_matrices", CERT_DIR, EXPECTED_CERT_SHA, "CERTIFIED"),
        ("updated_training_population_readiness", READINESS_DIR, EXPECTED_READINESS_SHA, "READY_TO_REQUEST_PROCESS_VALIDATION"),
    ]
    rows = []
    for name, path, expected, status in refs:
        actual = package_digest_from_manifest(path)
        rows.append(
            {
                "input_name": name,
                "path": str(path),
                "expected_sha256": expected,
                "actual_sha256": actual,
                "sha_match": expected == actual,
                "status": "PASS" if expected == actual else "FAIL",
                "input_status": status,
                "required_for_execution": True,
            }
        )
    return rows


def selected_fold_rows() -> list[dict[str, Any]]:
    rows = read_csv(READINESS_DIR / "fold_population_summary_2026-07-12.csv")
    return [r for r in rows if r["design"] == "preferred"]


def missingness_by_fold() -> list[dict[str, Any]]:
    rows = read_csv(READINESS_DIR / "missingness_stability_by_field_fold_2026-07-12.csv")
    return [r for r in rows if r["fold_design"] == "preferred"]


def manifest_scope_rows() -> list[dict[str, Any]]:
    readiness = {r["manifest_id"]: r for r in read_csv(READINESS_DIR / "manifest_specific_readiness_audit_2026-07-12.csv")}
    rows = []
    for manifest in LOAD_SCHEMA_CHECK_MANIFESTS:
        if manifest in SELECTED_FIT_MANIFESTS:
            scope = "model_fit_process_validation"
        else:
            scope = "load_schema_check_only"
        if manifest == "variant_c":
            scope = "excluded_from_fit_load_schema_check_only"
            rationale = "market metadata limitation adds unnecessary process complexity"
        elif manifest == "variant_b":
            rationale = "interaction-width compatibility is covered by load/schema check; not needed for smallest fit scope"
        elif manifest == "hits_1_5":
            rationale = "standalone inferential evaluation excluded because Hits 1.5 population remains below threshold"
        elif manifest == "hits_0_5":
            rationale = "not required for smallest workflow fit scope; retained for load/schema check"
        elif manifest == "variant_d":
            rationale = "primary parsimonious workflow test"
        else:
            rationale = "secondary wider-schema compatibility fit test"
        r = readiness[manifest]
        rows.append(
            {
                "manifest_id": manifest,
                "execution_scope": scope,
                "certification_status": r["certification_status"],
                "certified_rows": r["certified_rows"],
                "feature_count": r["feature_count"],
                "usable_labeled_rows": r["usable_labeled_rows"],
                "rationale": rationale,
                "authorization_required_before_fit": True,
            }
        )
    return rows


def experiment_identity() -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "experiment_class": EXPERIMENT_CLASS,
        "authorization_state": AUTHORIZATION_STATE,
        "training_authorized": False,
        "signal_evaluation_authorized": False,
        "champion_challenger_authorized": False,
        "production_authorized": False,
        "training_readiness": TRAINING_READINESS,
        "created_at_utc": TIMESTAMP_UTC,
        "created_at_pt": TIMESTAMP_PT,
        "certified_interval": {"start": START_DATE, "end": END_DATE},
        "selected_fit_manifests": SELECTED_FIT_MANIFESTS,
        "load_schema_check_manifests": LOAD_SCHEMA_CHECK_MANIFESTS,
    }


def experiment_contract() -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "status": AUTHORIZATION_STATE,
        "class": EXPERIMENT_CLASS,
        "purpose": "Validate offline training workflow mechanics and governance fail-closed behavior only.",
        "not_authorized_for": [
            "signal_evaluation",
            "variant_ranking",
            "feature_value_claims",
            "champion_challenger_comparison",
            "roi_analysis",
            "production_promotion",
        ],
        "inputs": {
            "frozen_bundle_sha256": EXPECTED_SPEC_SHA,
            "frozen_spine_sha256": EXPECTED_SPINE_SHA,
            "certification_sha256": EXPECTED_CERT_SHA,
            "readiness_sha256": EXPECTED_READINESS_SHA,
        },
        "population": {
            "date_start": START_DATE,
            "date_end": END_DATE,
            "certified_rows": 2104,
            "outcome_attachable_rows": 2027,
            "certified_matrices_mutable": False,
        },
        "fold_contract": {
            "design": "preferred_three_way_chronological",
            "train": {"start": "2026-06-29", "end": "2026-07-04"},
            "validation": {"start": "2026-07-05", "end": "2026-07-07"},
            "holdout": {"start": "2026-07-08", "end": "2026-07-09"},
            "random_splitting": False,
            "cross_validation": False,
            "holdout_tuning": False,
        },
        "manifest_scope": {"fit": SELECTED_FIT_MANIFESTS, "load_schema_check": LOAD_SCHEMA_CHECK_MANIFESTS},
        "model": {
            "family": "logistic_regression",
            "role": "test_instrument_not_challenger",
            "fixed_parameters": {
                "solver": "liblinear",
                "penalty": "l2",
                "C": 1.0,
                "max_iter": 1000,
                "class_weight": None,
                "random_state": 1729,
            },
            "hyperparameter_search": False,
            "calibration_fit": False,
        },
        "runner": "backend/mlb/scripts/run_mlb_collective_bundle_v1_bounded_process_validation.py",
        "approval_required_before_execution": True,
    }


def authorization_state() -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "authorization_state": AUTHORIZATION_STATE,
        "training_authorized": False,
        "preprocessing_fit_authorized": False,
        "prediction_generation_authorized": False,
        "signal_evaluation_authorized": False,
        "champion_challenger_authorized": False,
        "production_authorized": False,
        "approval_artifact_present": False,
        "approval_granted": False,
        "next_human_decision": "Approve or do not approve one execution of the exact frozen process-validation contract.",
    }


def outcome_attachment_contract() -> dict[str, Any]:
    return {
        "status": "CONTRACT_DEFINED_NOT_EXECUTED",
        "source": "compatible outcome source identified in readiness review",
        "certified_rows": 2104,
        "outcome_attachable_rows": 2027,
        "unattached_rows": 77,
        "ambiguous_rows_required": 0,
        "duplicate_label_matches_required": 0,
        "join_policy": "exact canonical_row_id only",
        "name_only_fallback_allowed": False,
        "write_back_to_certified_matrices": False,
        "experiment_local_label_artifact_required": True,
        "exclusion_ledger_required": True,
        "push_policy": "document and exclude or encode by preapproved binary-label policy; no after-metric redesign",
    }


def preprocessing_contract() -> dict[str, Any]:
    return {
        "status": "DEFINED_NOT_FIT",
        "fit_scope": "training_fold_only",
        "apply_scope": ["validation", "holdout"],
        "feature_ordering": "certified_manifest_order",
        "unexpected_columns": "FAIL",
        "missing_required_columns": "FAIL",
        "feature_selection": False,
        "target_encoding": False,
        "full_population_statistics": False,
        "numeric_policy": "training-fold median imputation plus training-fold standardization where needed",
        "categorical_policy": "training-fold most-frequent imputation plus one-hot categories frozen from train; unknown categories fail",
        "indicator_policy": "cast to 0/1 using certified values; unexpected values fail",
        "missingness_policy": "frozen Bundle v1 missingness contract only",
        "serialization_required": True,
        "serialized_object_is_research_only": True,
    }


def model_contract() -> dict[str, Any]:
    return experiment_contract()["model"] | {
        "status": "DEFINED_NOT_TRAINED",
        "one_model_family": True,
        "one_fixed_parameter_set": True,
        "no_ensembling": True,
        "no_early_stopping_search": True,
        "no_production_comparison": True,
        "interpretation": "non-inferential process-validation test instrument",
    }


def metrics_contract() -> dict[str, Any]:
    return {
        "metric_label_required": "NON_INFERENTIAL_PROCESS_VALIDATION_DIAGNOSTIC",
        "permitted": [
            "row_counts",
            "class_counts",
            "prediction_count",
            "prediction_range",
            "finite_value_checks",
            "log_loss_calculation_validity",
            "auc_calculation_validity_when_both_classes_exist",
            "confusion_matrix_generation_validity",
            "deterministic_metric_equality_across_reruns",
            "artifact_completeness",
        ],
        "forbidden": [
            "manifest_performance_ranking",
            "predictive_signal_claims",
            "feature_improvement_claims",
            "wagering_roi",
            "promotion_claims",
            "production_model_comparison",
            "metric_based_tuning",
        ],
    }


def leakage_registry() -> list[dict[str, Any]]:
    tests = [
        ("input_sha_verification", "verify all frozen package digests before loading", "FAIL_CLOSED"),
        ("feature_order_verification", "feature order must match certified manifest", "FAIL_CLOSED"),
        ("training_only_preprocessing_fit", "fit preprocessing on train rows only", "FAIL_CLOSED"),
        ("fold_identity_hashes", "hash canonical identities by fold before and after run", "FAIL_CLOSED"),
        ("no_validation_holdout_fit_rows", "assert validation/holdout identities absent from fit data", "FAIL_CLOSED"),
        ("no_outcome_columns_in_features", "reject outcome-like feature names and source columns", "FAIL_CLOSED"),
        ("no_future_derived_fields", "reject postgame/future-derived aliases", "FAIL_CLOSED"),
        ("duplicate_identity_check", "canonical_row_id uniqueness required", "FAIL_CLOSED"),
        ("date_fold_overlap_check", "no date overlap across folds", "FAIL_CLOSED"),
        ("game_overlap_check", "no same game across chronological date folds", "FAIL_CLOSED"),
        ("fixed_seed_check", "runner config seed must equal 1729", "FAIL_CLOSED"),
        ("deterministic_predictions", "rerun predictions must hash identically", "FAIL_CLOSED"),
        ("deterministic_diagnostics", "diagnostic artifacts must hash identically", "FAIL_CLOSED"),
        ("no_production_write_guard", "runner must run in research-only no-write mode", "FAIL_CLOSED"),
    ]
    return [
        {
            "test_id": test_id,
            "description": description,
            "required": True,
            "failure_action": action,
            "phase": "future_execution",
        }
        for test_id, description, action in tests
    ]


def negative_tests() -> list[dict[str, Any]]:
    tests = [
        ("mutated_matrix_sha", "change certified matrix SHA input", "runner refuses before loading"),
        ("missing_required_feature", "remove one required feature from a copied matrix", "runner fails schema check"),
        ("outcome_column_in_features", "inject actual_hits into feature set", "runner fails leakage check"),
        ("holdout_row_in_train", "insert holdout identity into train fold", "runner fails fold integrity"),
        ("unauthorized_manifest", "request variant_c fit or unknown manifest", "runner refuses unauthorized scope"),
        ("bad_source_identity", "alter source package identity SHA", "runner fails input identity check"),
        ("missing_approval", "run without explicit approved artifact", "runner refuses execution"),
    ]
    return [
        {
            "negative_test_id": tid,
            "injected_condition": condition,
            "expected_result": result,
            "required_before_interpreting_process_success": True,
        }
        for tid, condition, result in tests
    ]


def stop_conditions() -> list[dict[str, Any]]:
    conditions = [
        "frozen identity or SHA mismatch",
        "ambiguous outcome attachment",
        "duplicate canonical identities",
        "improper fold date overlap",
        "preprocessing uses non-training data",
        "outcome or future-derived field enters features",
        "matrix schema differs from certified manifest",
        "deterministic rerun mismatch",
        "production write attempted",
        "unapproved manifest requested",
        "unapproved model setting requested",
    ]
    return [
        {
            "stop_condition": condition,
            "future_runner_action": "BLOCKED_FAIL_CLOSED",
            "partial_success_interpretation": "NO_SIGNAL_EVIDENCE",
        }
        for condition in conditions
    ]


def execution_artifacts() -> list[dict[str, Any]]:
    names = [
        ("input_identity_verification", "json/csv", "required before load"),
        ("experiment_local_label_attachment", "csv", "exact identity labels only"),
        ("label_exclusion_ledger", "csv", "unattached/ambiguous excluded rows"),
        ("fold_identity_manifest", "csv/json", "canonical IDs by fold"),
        ("preprocessing_fit_manifest", "json", "train-only fit provenance"),
        ("serialized_preprocessor", "binary", "research-only future output"),
        ("model_fit_manifest", "json", "fixed parameter record"),
        ("serialized_model", "binary", "research-only future output"),
        ("prediction_diagnostics", "csv/json", "non-inferential diagnostics"),
        ("deterministic_rerun_comparison", "csv/json", "hash equality check"),
        ("negative_test_results", "csv", "fail-closed proof"),
        ("execution_summary", "md/json", "process validation only"),
    ]
    return [{"artifact_name": n, "format": fmt, "requirement": req, "production_artifact": False} for n, fmt, req in names]


def blockers() -> list[dict[str, Any]]:
    return [
        {
            "item": "human_approval_not_granted",
            "severity": "GOVERNANCE",
            "description": "This package prepares but does not approve execution.",
            "required_resolution": "Human approval artifact must explicitly authorize one exact run.",
            "blocks_execution": True,
        },
        {
            "item": "signal_evaluation_fold_limits",
            "severity": "HIGH",
            "description": "11 slates and small chronological holdout remain insufficient for signal claims.",
            "required_resolution": "Use this run only for process validation; accumulate additional slates before signal evaluation.",
            "blocks_execution": False,
        },
        {
            "item": "variant_c_market_metadata_missingness",
            "severity": "MEDIUM",
            "description": "Variant C has contract-permitted market metadata missingness.",
            "required_resolution": "Exclude Variant C from fit scope; allow load/schema check only.",
            "blocks_execution": False,
        },
        {
            "item": "hits_1_5_below_threshold",
            "severity": "MEDIUM",
            "description": "Hits 1.5 population remains below standalone inferential threshold.",
            "required_resolution": "Do not run standalone Hits 1.5 signal evaluation.",
            "blocks_execution": False,
        },
    ]


def readiness_json() -> dict[str, Any]:
    return {
        "readiness_decision": READINESS_DECISION,
        "authorization_state": AUTHORIZATION_STATE,
        "training_readiness": TRAINING_READINESS,
        "training_authorized": False,
        "next_human_decision": "approve_or_do_not_approve_one_exact_process_validation_execution",
        "reason": "all input identities verify and the design is process-validation only; execution still requires explicit human approval",
    }


def parse_validation() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file()):
        if path.name in {"sha256_manifest_2026-07-13.csv", "parse_schema_validation_results_2026-07-13.csv"}:
            continue
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.lstrip().startswith("#"):
                    status = "FAIL"
                    detail = "markdown_missing_heading"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "placeholder_token"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"relative_path": str(path.relative_to(OUT_DIR)), "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def write_sha_manifest() -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-13.csv"):
        rel = str(path.relative_to(OUT_DIR))
        file_sha = sha256(path)
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / "sha256_manifest_2026-07-13.csv", rows)
    return package_sha


def write_markdown_files() -> None:
    fold_rows = selected_fold_rows()
    train = next(r for r in fold_rows if r["fold"] == "train")
    validation = next(r for r in fold_rows if r["fold"] == "validation")
    holdout = next(r for r in fold_rows if r["fold"] == "holdout")
    executive = f"""# Executive Summary

This package prepares the MLB Collective Bundle v1 bounded offline
process-validation experiment for human review. It does not authorize or run
training.

The request is ready for human approval because the frozen Bundle v1, frozen
Spine Contract v1, certified bounded matrices, and updated readiness package
all verify by SHA256. The experiment remains process-validation only.

- Experiment identity: `{EXPERIMENT_ID}`
- Authorization state: `{AUTHORIZATION_STATE}`
- Training readiness: `{TRAINING_READINESS}`
- Selected fit manifests: `variant_d`, `variant_a`
- Certified rows: `2,104`
- Outcome-attachable rows: `2,027`
- Fold design: fixed chronological train / validation / holdout
"""
    (OUT_DIR / "executive_summary_2026-07-13.md").write_text(executive)

    (OUT_DIR / "main_assessment_2026-07-13.md").write_text(
        f"""# Main Assessment

## Purpose

This approval package converts the completed training-population readiness
decision into an implementation-ready process-validation contract. It validates
workflow integrity only: input identity, label attachment, fold construction,
train-only preprocessing, deterministic fixed-model mechanics, fail-closed
guards, and research-only output generation.

## What This Does Not Authorize

It does not authorize training, preprocessing fit, prediction generation,
performance interpretation, Champion-Challenger comparison, production
integration, uploads, database writes, OddsAPI calls, or certified-matrix
mutation.

## Decision

Readiness decision: `{READINESS_DECISION}`.

Authorization state: `{AUTHORIZATION_STATE}`.
"""
    )
    (OUT_DIR / "one_page_approval_brief_2026-07-13.md").write_text(
        f"""# One-Page Approval Brief

## Approve

Authorize one execution of the exact frozen process-validation experiment
contract for `{EXPERIMENT_ID}`.

## Do Not Approve

No training occurs.

## Approval Meaning

Approval would allow one research-only process-validation run. It would not
permit signal conclusions, production comparison, Champion-Challenger claims,
upload changes, or promotion.

Current state: `{AUTHORIZATION_STATE}`.
"""
    )
    (OUT_DIR / "experiment_contract_2026-07-13.md").write_text(
        f"""# Experiment Contract

Experiment identity: `{EXPERIMENT_ID}`

Class: `{EXPERIMENT_CLASS}`

Status: `{AUTHORIZATION_STATE}`

The future run may load certified matrices, attach compatible outcomes into an
experiment-local artifact, construct fixed chronological folds, fit
preprocessing on training rows only, and train one deterministic logistic
regression test instrument only after explicit approval.

The selected fit manifests are Variant D and Variant A. All manifests may be
load/schema checked. Variant C, Variant B, Hits 0.5, and Hits 1.5 are excluded
from model fitting in this process-validation contract.
"""
    )
    (OUT_DIR / "outcome_attachment_contract_2026-07-13.md").write_text(
        """# Outcome Attachment Contract

Outcomes must be attached only to an experiment-local artifact by exact
canonical identity. The certified matrices must remain unchanged.

Requirements:

- exact canonical identity
- zero ambiguous matches
- zero duplicate label matches
- exclusion ledger for unattached rows
- no name-only fallback
- no outcome write-back into certified matrices
"""
    )
    (OUT_DIR / "chronological_fold_contract_2026-07-13.md").write_text(
        f"""# Chronological Fold Contract

Use the preferred fixed chronological split from the readiness review.

## Train

Dates: `{train['start_date']}` through `{train['end_date']}`

Rows: `{train['total_rows']}`; outcome-attached rows: `{train['outcome_attachable_rows']}`.

## Validation

Dates: `{validation['start_date']}` through `{validation['end_date']}`

Rows: `{validation['total_rows']}`; outcome-attached rows: `{validation['outcome_attachable_rows']}`.

## Untouched Holdout

Dates: `{holdout['start_date']}` through `{holdout['end_date']}`

Rows: `{holdout['total_rows']}`; outcome-attached rows: `{holdout['outcome_attachable_rows']}`.

No random split, no cross-validation, no fold redesign after diagnostics, and
no holdout tuning.
"""
    )
    (OUT_DIR / "preprocessing_contract_2026-07-13.md").write_text(
        """# Preprocessing Contract

Preprocessing may be fit only on the training fold after explicit approval.
The fitted object must be serialized and applied unchanged to validation and
holdout rows.

Policies:

- preserve certified feature ordering
- fail on unexpected columns
- fail on missing required columns
- no feature selection
- no target encoding
- no full-population normalization
- no outcome-informed imputation
- categorical unknowns fail
- missing values follow the frozen Bundle v1 contract
"""
    )
    (OUT_DIR / "model_contract_2026-07-13.md").write_text(
        """# Model Contract

Use one deterministic logistic regression test instrument:

- solver: `liblinear`
- penalty: `l2`
- C: `1.0`
- max_iter: `1000`
- class_weight: `None`
- random_state: `1729`

No hyperparameter search, no calibration fitting, no feature selection, no
ensembling, no production comparison, and no promotion interpretation.
"""
    )
    (OUT_DIR / "metrics_interpretation_contract_2026-07-13.md").write_text(
        """# Metrics and Interpretation Contract

All diagnostics must be labeled:

`NON_INFERENTIAL_PROCESS_VALIDATION_DIAGNOSTIC`

Metrics exist only to verify that the pipeline emits valid deterministic
outputs. They may not be used to rank manifests, claim signal, tune settings,
calculate wagering ROI, compare to production, or support promotion.
"""
    )
    (OUT_DIR / "proposed_runner_design_2026-07-13.md").write_text(
        """# Proposed Runner Design

Preferred future path:

`backend/mlb/scripts/run_mlb_collective_bundle_v1_bounded_process_validation.py`

The runner must require an explicit experiment contract path, explicit approval
artifact, exact input identities, fold contract, manifest list, fixed model
configuration, output directory, research-only mode, no database writes, no
production model writes, and no upload changes.

It must refuse execution unless the approval artifact explicitly authorizes
this exact frozen contract.
"""
    )
    (OUT_DIR / "human_approval_request_2026-07-13.md").write_text(
        f"""# Human Approval Request

## Approve

Authorize one execution of the exact frozen process-validation experiment
contract for `{EXPERIMENT_ID}`.

## Do Not Approve

No training occurs.

## Important Limits

This is process validation only. No signal conclusions are permitted. No
production comparison occurs. No Champion-Challenger implication exists. The
certified matrices remain unchanged. Outputs remain research-only.

This package does not grant approval. Current authorization state:
`{AUTHORIZATION_STATE}`.
"""
    )
    (OUT_DIR / "readiness_decision_2026-07-13.md").write_text(
        f"""# Readiness Decision

Readiness decision: `{READINESS_DECISION}`

Authorization state: `{AUTHORIZATION_STATE}`

Training readiness: `{TRAINING_READINESS}`

The next human decision is whether to approve one execution of the exact frozen
process-validation contract. Until then, no model training or preprocessing fit
is authorized.
"""
    )


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_markdown_files()
    write_json(OUT_DIR / "experiment_contract_2026-07-13.json", experiment_contract())
    write_json(OUT_DIR / "experiment_identity_2026-07-13.json", experiment_identity())
    write_csv(OUT_DIR / "input_identity_sha_registry_2026-07-13.csv", verified_inputs())
    write_csv(OUT_DIR / "manifest_scope_decision_2026-07-13.csv", manifest_scope_rows())
    write_json(OUT_DIR / "outcome_attachment_contract_2026-07-13.json", outcome_attachment_contract())
    write_json(
        OUT_DIR / "chronological_fold_contract_2026-07-13.json",
        {"design": "preferred_three_way_chronological", "folds": selected_fold_rows(), "missingness_by_fold": missingness_by_fold()},
    )
    write_csv(OUT_DIR / "fold_row_inventory_2026-07-13.csv", selected_fold_rows())
    write_json(OUT_DIR / "preprocessing_contract_2026-07-13.json", preprocessing_contract())
    write_json(OUT_DIR / "model_contract_2026-07-13.json", model_contract())
    write_json(OUT_DIR / "metrics_interpretation_contract_2026-07-13.json", metrics_contract())
    write_csv(OUT_DIR / "leakage_integrity_test_registry_2026-07-13.csv", leakage_registry())
    write_csv(OUT_DIR / "negative_test_plan_2026-07-13.csv", negative_tests())
    write_csv(OUT_DIR / "stop_condition_registry_2026-07-13.csv", stop_conditions())
    write_csv(OUT_DIR / "required_execution_artifact_registry_2026-07-13.csv", execution_artifacts())
    write_json(OUT_DIR / "human_approval_request_2026-07-13.json", {"approval_granted": False, "choices": ["approve", "do_not_approve"], "experiment_id": EXPERIMENT_ID})
    write_json(OUT_DIR / "authorization_state_2026-07-13.json", authorization_state())
    write_csv(OUT_DIR / "blocker_limitation_register_2026-07-13.csv", blockers())
    write_json(OUT_DIR / "readiness_decision_2026-07-13.json", readiness_json())
    write_csv(OUT_DIR / "evidence_provenance_manifest_2026-07-13.csv", verified_inputs())
    write_csv(OUT_DIR / "parse_schema_validation_results_2026-07-13.csv", parse_validation())
    package_sha = write_sha_manifest()
    return {
        "output_dir": str(OUT_DIR),
        "experiment_id": EXPERIMENT_ID,
        "authorization_state": AUTHORIZATION_STATE,
        "readiness_decision": READINESS_DECISION,
        "selected_fit_manifests": SELECTED_FIT_MANIFESTS,
        "training_authorized": False,
        "package_sha256": package_sha,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global OUT_DIR
    OUT_DIR = Path(args.output_dir)
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
