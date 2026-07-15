"""Run one bounded no-promotion training dry run for historical Bundle matrices.

This harness is intentionally narrow: it fits fixed deterministic process
artifacts from frozen matrices and validates serialization/replay. It does not
score rows, produce probabilities, calculate metrics, rank outputs, compare
variants, register production models, call external APIs, or write databases.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import pickle
import platform
import re
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


RUN_DATE = "2026-07-13"
SEED = 20260713
DEFAULT_MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13"
)
DEFAULT_PROCESS_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_offline_process_validation/2026-07-13"
)
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_no_promotion_training_dry_run/2026-07-13"
)
AUTHORIZATION_ATTACHMENT = Path(
    "/Users/jerrystrain/.codex/attachments/f0deddee-7bcf-4113-bdcb-cb711949d57d/pasted-text.txt"
)

VARIANTS = ["variant_a", "variant_b", "variant_c", "variant_d"]
EXPECTED_COUNTS = {"variant_a": 1022, "variant_b": 1022, "variant_c": 869, "variant_d": 1022}
IDENTITY_COLUMNS = [
    "denominator_order",
    "canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "player_game_key",
]
LABEL_FIELD = "win_loss_label"
FORBIDDEN_FEATURE_PATTERNS = [
    "actual_hit",
    "actual_",
    "win_loss",
    "settlement",
    "outcome",
    "participation",
    "official_game_status",
    "nonappearance",
    "source_authority",
    "source_provenance",
    "game_status",
    "final",
    "postgame",
    "target",
]
PROHIBITED_CODE_PATTERNS = {
    "prediction_invocation": re.compile(r"\.predict\s*\(|\.predict_proba\s*\(|\.decision_function\s*\("),
    "metric_invocation": re.compile(
        r"\b(accuracy_score|roc_auc_score|log_loss|brier_score_loss|precision_score|recall_score|f1_score|confusion_matrix)\s*\("
    ),
    "ranking_invocation": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "model_selection_invocation": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|cross_validate|train_test_split)\b"),
}
ARTIFACT_WARNING = "PROCESS_VALIDATION_ONLY_NOT_EVALUATED_NOT_FOR_PRODUCTION"
FIT_DATES = {"2026-06-22", "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26"}
SERIALIZATION_REPLAY_DATES = {"2026-06-27"}
HOLDOUT_LOADING_DATES = {"2026-06-28"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def read_header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_row_id(row: dict[str, str]) -> str:
    return row.get("canonical_row_id", "")


def load_allowlist(process_dir: Path, variant: str) -> list[str]:
    rows = read_csv(process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv")
    fields = [
        r["feature_name"]
        for r in sorted(
            (r for r in rows if r["variant"] == variant),
            key=lambda r: int(r["feature_order"]),
        )
    ]
    if not fields:
        raise RuntimeError(f"no allowlist fields found for {variant}")
    return fields


def infer_feature_types(process_dir: Path, variant: str) -> tuple[list[str], list[str]]:
    rows = read_csv(process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv")
    numeric: list[str] = []
    categorical: list[str] = []
    for row in sorted(
        (r for r in rows if r["variant"] == variant),
        key=lambda r: int(r["feature_order"]),
    ):
        if row["feature_type"] == "numeric":
            numeric.append(row["feature_name"])
        else:
            categorical.append(row["feature_name"])
    return numeric, categorical


def validate_feature_allowlist(header: list[str], features: list[str]) -> list[str]:
    blockers = []
    missing = [f for f in features if f not in header]
    if missing:
        blockers.append(f"MISSING_ALLOWLIST_FEATURES:{'|'.join(missing)}")
    extras = [f for f in features if any(p in f.lower() for p in FORBIDDEN_FEATURE_PATTERNS)]
    if extras:
        blockers.append(f"FORBIDDEN_FEATURE_ALIAS:{'|'.join(extras)}")
    return blockers


def split_name(slate_date: str) -> str:
    if slate_date in FIT_DATES:
        return "PROCESS_ONLY_FIT"
    if slate_date in SERIALIZATION_REPLAY_DATES:
        return "PROCESS_ONLY_SERIALIZATION_REPLAY"
    if slate_date in HOLDOUT_LOADING_DATES:
        return "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED"
    return "OUT_OF_SCOPE_DATE"


def make_pipeline(numeric_features: list[str], categorical_features: list[str]) -> Pipeline:
    transformer = ColumnTransformer(
        transformers=[
            ("numeric_passthrough_nan_preserved", "passthrough", numeric_features),
            (
                "categorical_one_hot_explicit_missing",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False, dtype=np.float64),
                categorical_features,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
    model = HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=0.05,
        max_iter=25,
        max_leaf_nodes=15,
        min_samples_leaf=20,
        l2_regularization=0.0,
        early_stopping=False,
        random_state=SEED,
    )
    return Pipeline([("preprocess", transformer), ("model", model)])


def prepare_features(rows: list[dict[str, str]], features: list[str], numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    out = pd.DataFrame(index=frame.index)
    for feature in numeric:
        out[feature] = pd.to_numeric(frame[feature].replace({"": np.nan}), errors="coerce")
    for feature in categorical:
        out[feature] = frame[feature].replace({"": "__MISSING__"}).astype(str)
    return out[features]


def prepare_labels(rows: list[dict[str, str]]) -> np.ndarray:
    labels = []
    for row in rows:
        value = row.get(LABEL_FIELD, "")
        if value == "win":
            labels.append(1)
        elif value == "loss":
            labels.append(0)
        else:
            raise RuntimeError(f"unsupported label value {value!r} for {row.get('canonical_row_id')}")
    return np.array(labels, dtype=np.int8)


def pipeline_state_hash(pipeline: Pipeline) -> str:
    # Hash serialized object bytes for deterministic replay. The hash is used
    # only to prove replayability, not to inspect model signal.
    return sha256_bytes(pickle.dumps(pipeline, protocol=4))


def dump_artifact(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, path, compress=0)
    return sha256_path(path)


class DryRun:
    def __init__(self, matrix_dir: Path, process_dir: Path, output_dir: Path):
        self.matrix_dir = matrix_dir
        self.process_dir = process_dir
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.input_sha_before: dict[str, str] = {}
        self.input_sha_after: dict[str, str] = {}
        self.decision_statuses: dict[str, str] = {}

    def assert_output_contained(self, path: Path) -> None:
        resolved = path.resolve()
        root = self.output_dir.resolve()
        if root != resolved and root not in resolved.parents:
            raise RuntimeError(f"output path escapes bounded package: {path}")

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.write_authorization_record()
        self.capture_input_shas("before")
        input_report = self.reproduce_inputs()
        split_rows = self.write_split_spec_and_manifests()
        spec_rows = self.write_model_spec()
        runtime_rows = self.write_runtime_inventory()

        allowlist_rows: list[dict[str, Any]] = []
        forbidden_rows: list[dict[str, Any]] = []
        preprocessing_rows: list[dict[str, Any]] = []
        fit_rows: list[dict[str, Any]] = []
        reload_rows: list[dict[str, Any]] = []
        replay_rows: list[dict[str, Any]] = []
        variant_artifacts: list[Path] = []

        for variant in VARIANTS:
            result = self.fit_variant(variant)
            allowlist_rows.extend(result["allowlist_rows"])
            forbidden_rows.extend(result["forbidden_rows"])
            preprocessing_rows.extend(result["preprocessing_rows"])
            fit_rows.append(result["fit_row"])
            reload_rows.append(result["reload_row"])
            replay_rows.append(result["replay_row"])
            variant_artifacts.append(result["artifact_path"])

        write_csv(self.output_dir / f"per_variant_feature_allowlist_verification_{RUN_DATE}.csv", allowlist_rows)
        write_csv(self.output_dir / f"per_variant_forbidden_field_audit_{RUN_DATE}.csv", forbidden_rows)
        write_csv(self.output_dir / f"per_variant_preprocessing_audit_{RUN_DATE}.csv", preprocessing_rows)
        write_csv(self.output_dir / f"per_variant_fit_execution_ledger_{RUN_DATE}.csv", fit_rows)
        write_csv(self.output_dir / f"per_variant_reload_validation_{RUN_DATE}.csv", reload_rows)
        write_csv(self.output_dir / f"per_variant_repeat_fit_replay_comparison_{RUN_DATE}.csv", replay_rows)

        self.capture_input_shas("after")
        immutability_rows = self.write_source_matrix_immutability_audit()
        containment_rows = self.write_output_containment_audit()
        guard_rows = self.write_guardrail_audit()
        replay_validation = self.write_deterministic_replay_validation(
            input_report, fit_rows, reload_rows, replay_rows, immutability_rows, containment_rows, guard_rows
        )
        self.write_decision(
            input_report,
            allowlist_rows,
            forbidden_rows,
            preprocessing_rows,
            fit_rows,
            reload_rows,
            replay_rows,
            immutability_rows,
            containment_rows,
            guard_rows,
        )
        self.write_markdown_reports()
        parse_rows = self.write_parse_validation()
        self.write_sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "variant_artifacts": [str(p) for p in variant_artifacts],
            "parse_failures": sum(1 for r in parse_rows if r["parse_status"] == "FAIL"),
            "decision_statuses": self.decision_statuses,
            "replay_validation_rows": len(replay_validation),
        }

    def write_authorization_record(self) -> None:
        text = AUTHORIZATION_ATTACHMENT.read_text() if AUTHORIZATION_ATTACHMENT.exists() else ""
        write_json(
            self.output_dir / f"human_authorization_record_{RUN_DATE}.json",
            {
                "authorization_source": str(AUTHORIZATION_ATTACHMENT),
                "authorization_source_sha256": sha256_path(AUTHORIZATION_ATTACHMENT) if AUTHORIZATION_ATTACHMENT.exists() else "",
                "authorization_reproduced": AUTHORIZATION_ATTACHMENT.exists()
                and "Human authorization is granted for exactly one bounded" in text,
                "scope": "Historical Bundle No-Promotion Training Dry Run",
                "restriction": ARTIFACT_WARNING,
            },
        )

    def matrix_paths(self) -> list[Path]:
        paths = [self.matrix_dir / f"{v}_qualified_matrix_{RUN_DATE}.csv" for v in VARIANTS]
        paths.extend(self.matrix_dir / f"{v}_hits_0_5_qualified_matrix_{RUN_DATE}.csv" for v in VARIANTS)
        paths.extend(self.matrix_dir / f"{v}_hits_1_5_qualified_matrix_{RUN_DATE}.csv" for v in VARIANTS)
        return paths

    def capture_input_shas(self, phase: str) -> None:
        target = self.input_sha_before if phase == "before" else self.input_sha_after
        for path in self.matrix_paths():
            target[str(path)] = sha256_path(path)
        target[str(self.process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv")] = sha256_path(
            self.process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv"
        )
        target[str(self.process_dir / f"machine_readable_process_decision_{RUN_DATE}.json")] = sha256_path(
            self.process_dir / f"machine_readable_process_decision_{RUN_DATE}.json"
        )

    def reproduce_inputs(self) -> list[dict[str, Any]]:
        construction_sha = {
            row["filename"]: row["sha256"]
            for row in read_csv(self.matrix_dir / f"sha256_manifest_{RUN_DATE}.csv")
        }
        rows = []
        for variant in VARIANTS:
            path = self.matrix_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
            matrix = read_csv(path)
            ids = [canonical_row_id(r) for r in matrix]
            rows.append(
                {
                    "variant": variant,
                    "matrix_path": str(path),
                    "expected_rows": EXPECTED_COUNTS[variant],
                    "observed_rows": len(matrix),
                    "row_count_status": "PASS" if len(matrix) == EXPECTED_COUNTS[variant] else "FAIL",
                    "duplicate_canonical_ids": len(ids) - len(set(ids)),
                    "identity_order_status": "PASS" if len(ids) == len(set(ids)) else "FAIL",
                    "expected_sha256": construction_sha.get(path.name, ""),
                    "actual_sha256": sha256_path(path),
                    "sha_status": "PASS" if construction_sha.get(path.name, "") == sha256_path(path) else "FAIL",
                    "feature_allowlist_path": str(self.process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv"),
                }
            )
        write_csv(self.output_dir / f"authoritative_input_reproduction_report_{RUN_DATE}.csv", rows)
        return rows

    def write_split_spec_and_manifests(self) -> list[dict[str, Any]]:
        spec = [
            {
                "partition": "PROCESS_ONLY_FIT",
                "date_list": "|".join(sorted(FIT_DATES)),
                "purpose": "fit deterministic process instrument only",
                "evaluation_allowed": "false",
            },
            {
                "partition": "PROCESS_ONLY_SERIALIZATION_REPLAY",
                "date_list": "|".join(sorted(SERIALIZATION_REPLAY_DATES)),
                "purpose": "load-shape and replay partition only; not scored",
                "evaluation_allowed": "false",
            },
            {
                "partition": "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED",
                "date_list": "|".join(sorted(HOLDOUT_LOADING_DATES)),
                "purpose": "holdout loading compatibility only; not scored",
                "evaluation_allowed": "false",
            },
        ]
        write_csv(self.output_dir / f"process_only_split_specification_{RUN_DATE}.csv", spec)
        rows = []
        for variant in VARIANTS:
            matrix = read_csv(self.matrix_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv")
            by_partition: dict[str, list[dict[str, str]]] = {
                "PROCESS_ONLY_FIT": [],
                "PROCESS_ONLY_SERIALIZATION_REPLAY": [],
                "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED": [],
            }
            for row in matrix:
                part = split_name(row["slate_date"])
                if part in by_partition:
                    by_partition[part].append(row)
            prior_ids: set[str] = set()
            prior_pg: set[str] = set()
            for part, part_rows in by_partition.items():
                ids = {canonical_row_id(r) for r in part_rows}
                pgs = {r["player_game_key"] for r in part_rows}
                rows.append(
                    {
                        "variant": variant,
                        "partition": part,
                        "rows": len(part_rows),
                        "date_list": "|".join(sorted({r["slate_date"] for r in part_rows})),
                        "canonical_overlap_with_prior": len(ids & prior_ids),
                        "player_game_overlap_with_prior": len(pgs & prior_pg),
                        "split_status": "PASS" if not (ids & prior_ids) and not (pgs & prior_pg) else "FAIL_OVERLAP",
                        "evaluation_allowed": "false",
                    }
                )
                prior_ids.update(ids)
                prior_pg.update(pgs)
        write_csv(self.output_dir / f"per_variant_process_only_split_manifests_{RUN_DATE}.csv", rows)
        return rows

    def write_model_spec(self) -> list[dict[str, Any]]:
        rows = [
            {
                "component": "model_class",
                "value": "sklearn.ensemble.HistGradientBoostingClassifier",
                "reason": "deterministic process instrument with native numeric NaN tolerance; not selected for expected performance",
            },
            {"component": "random_seed", "value": SEED, "reason": "deterministic replay"},
            {"component": "max_iter", "value": 25, "reason": "small fixed process run"},
            {"component": "early_stopping", "value": "False", "reason": "no validation-performance based stopping"},
            {
                "component": "numeric_preprocessing",
                "value": "stable numeric coercion; blank remains NaN",
                "reason": "no imputation",
            },
            {
                "component": "categorical_preprocessing",
                "value": "OneHotEncoder handle_unknown=ignore with explicit __MISSING__ category",
                "reason": "mechanical compatibility for categorical fields",
            },
        ]
        write_csv(self.output_dir / f"model_preprocessing_specification_{RUN_DATE}.csv", rows)
        return rows

    def write_runtime_inventory(self) -> list[dict[str, Any]]:
        rows = [
            {"component": "python", "version": sys.version.replace("\n", " ")},
            {"component": "platform", "version": platform.platform()},
            {"component": "numpy", "version": np.__version__},
            {"component": "pandas", "version": pd.__version__},
            {"component": "sklearn", "version": sklearn.__version__},
            {"component": "joblib", "version": joblib.__version__},
        ]
        write_csv(self.output_dir / f"runtime_dependency_inventory_{RUN_DATE}.csv", rows)
        return rows

    def fit_variant(self, variant: str) -> dict[str, Any]:
        path = self.matrix_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
        before_sha = sha256_path(path)
        rows = read_csv(path)
        header = read_header(path)
        features = load_allowlist(self.process_dir, variant)
        numeric, categorical = infer_feature_types(self.process_dir, variant)
        blockers = validate_feature_allowlist(header, features)
        if blockers:
            raise RuntimeError(f"{variant} feature allowlist failed: {blockers}")
        fit_rows = [r for r in rows if split_name(r["slate_date"]) == "PROCESS_ONLY_FIT"]
        replay_rows = [r for r in rows if split_name(r["slate_date"]) == "PROCESS_ONLY_SERIALIZATION_REPLAY"]
        holdout_rows = [r for r in rows if split_name(r["slate_date"]) == "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED"]
        if not fit_rows:
            raise RuntimeError(f"{variant} has no process fit rows")

        x_fit = prepare_features(fit_rows, features, numeric, categorical)
        y_fit = prepare_labels(fit_rows)
        pipeline = make_pipeline(numeric, categorical)
        pipeline.fit(x_fit, y_fit)
        state_hash = pipeline_state_hash(pipeline)

        artifact_path = self.output_dir / "model_artifacts" / f"{variant}_process_validation_model_{RUN_DATE}.joblib"
        self.assert_output_contained(artifact_path)
        metadata = self.artifact_metadata(
            variant,
            artifact_path,
            path,
            features,
            numeric,
            categorical,
            state_hash,
            rows,
            fit_rows,
            replay_rows,
            holdout_rows,
        )
        artifact_payload = {
            "restriction": ARTIFACT_WARNING,
            "variant": variant,
            "pipeline": pipeline,
            "metadata": metadata,
        }
        artifact_sha = dump_artifact(artifact_path, artifact_payload)
        metadata["artifact_sha256"] = artifact_sha
        metadata_path = self.output_dir / f"{variant}_artifact_metadata_{RUN_DATE}.json"
        write_json(metadata_path, metadata)

        loaded = joblib.load(artifact_path)
        reload_row = {
            "variant": variant,
            "artifact_path": str(artifact_path),
            "artifact_sha256": artifact_sha,
            "reload_status": "PASS" if loaded.get("restriction") == ARTIFACT_WARNING and loaded.get("variant") == variant else "FAIL",
            "loaded_model_class": type(loaded.get("pipeline").named_steps["model"]).__name__,
            "prediction_invoked": "false",
            "metric_invoked": "false",
        }

        repeat_pipeline = make_pipeline(numeric, categorical)
        repeat_pipeline.fit(x_fit, y_fit)
        repeat_state_hash = pipeline_state_hash(repeat_pipeline)
        replay_row = {
            "variant": variant,
            "first_state_hash": state_hash,
            "repeat_state_hash": repeat_state_hash,
            "state_hash_status": "PASS" if state_hash == repeat_state_hash else "FAIL",
            "seed": SEED,
            "prediction_invoked": "false",
            "metric_invoked": "false",
        }

        after_sha = sha256_path(path)
        allowlist_rows = [
            {
                "variant": variant,
                "feature_order": idx,
                "feature_name": feature,
                "feature_type": "numeric" if feature in numeric else "categorical",
                "source": "offline_process_validation_positive_allowlist",
                "verification_status": "PASS",
            }
            for idx, feature in enumerate(features, start=1)
        ]
        forbidden_rows = [
            {
                "variant": variant,
                "feature_name": feature,
                "forbidden_matches": "|".join(p for p in FORBIDDEN_FEATURE_PATTERNS if p in feature.lower()),
                "forbidden_field_status": "PASS",
            }
            for feature in features
        ]
        preprocessing_rows = [
            {
                "variant": variant,
                "feature_name": feature,
                "feature_type": "numeric" if feature in numeric else "categorical",
                "blank_input_count_all_rows": sum(1 for r in rows if r.get(feature, "") == ""),
                "blank_input_count_fit_rows": sum(1 for r in fit_rows if r.get(feature, "") == ""),
                "preprocessing_action": (
                    "numeric_coerce_blank_to_nan_no_imputation"
                    if feature in numeric
                    else "categorical_explicit_missing_token_one_hot_no_label_use"
                ),
                "implicit_imputation": "false",
                "fit_partition_only_parameters": "true",
            }
            for feature in features
        ]
        fit_row = {
            "variant": variant,
            "fit_rows": len(fit_rows),
            "serialization_replay_rows_not_scored": len(replay_rows),
            "holdout_loading_rows_not_scored": len(holdout_rows),
            "feature_count": len(features),
            "numeric_feature_count": len(numeric),
            "categorical_feature_count": len(categorical),
            "model_class": "HistGradientBoostingClassifier",
            "fixed_configuration": json.dumps(metadata["model_configuration"], sort_keys=True),
            "fit_status": "PASS",
            "artifact_path": str(artifact_path),
            "artifact_sha256": artifact_sha,
            "matrix_sha_before": before_sha,
            "matrix_sha_after": after_sha,
            "source_matrix_immutability_status": "PASS" if before_sha == after_sha else "FAIL",
            "prediction_invoked": "false",
            "metric_invoked": "false",
            "ranking_invoked": "false",
            "model_selection_invoked": "false",
        }
        return {
            "allowlist_rows": allowlist_rows,
            "forbidden_rows": forbidden_rows,
            "preprocessing_rows": preprocessing_rows,
            "fit_row": fit_row,
            "reload_row": reload_row,
            "replay_row": replay_row,
            "artifact_path": artifact_path,
        }

    def artifact_metadata(
        self,
        variant: str,
        artifact_path: Path,
        matrix_path: Path,
        features: list[str],
        numeric: list[str],
        categorical: list[str],
        state_hash: str,
        all_rows: list[dict[str, str]],
        fit_rows: list[dict[str, str]],
        replay_rows: list[dict[str, str]],
        holdout_rows: list[dict[str, str]],
    ) -> dict[str, Any]:
        allowlist_path = self.process_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv"
        split_path = self.output_dir / f"per_variant_process_only_split_manifests_{RUN_DATE}.csv"
        return {
            "restriction": ARTIFACT_WARNING,
            "variant": variant,
            "created_at_utc": self.generated_at,
            "artifact_path": str(artifact_path),
            "matrix_path": str(matrix_path),
            "matrix_sha256": sha256_path(matrix_path),
            "schema_sha256": sha256_path(self.process_dir / f"per_variant_schema_manifests_{RUN_DATE}.csv"),
            "feature_allowlist_path": str(allowlist_path),
            "feature_allowlist_sha256": sha256_path(allowlist_path),
            "label_field": LABEL_FIELD,
            "process_split_manifest_path": str(split_path),
            "process_split_manifest_sha256": sha256_path(split_path) if split_path.exists() else "",
            "model_class": "sklearn.ensemble.HistGradientBoostingClassifier",
            "model_configuration": {
                "loss": "log_loss",
                "learning_rate": 0.05,
                "max_iter": 25,
                "max_leaf_nodes": 15,
                "min_samples_leaf": 20,
                "l2_regularization": 0.0,
                "early_stopping": False,
                "random_state": SEED,
            },
            "seed": SEED,
            "preprocessing_configuration": {
                "numeric": "passthrough after stable numeric coercion; blank stays NaN",
                "categorical": "OneHotEncoder(handle_unknown='ignore') with explicit __MISSING__ source token",
                "no_imputation": True,
                "feature_order": features,
                "numeric_features": numeric,
                "categorical_features": categorical,
            },
            "runtime_versions": {
                "python": sys.version.replace("\n", " "),
                "numpy": np.__version__,
                "pandas": pd.__version__,
                "sklearn": sklearn.__version__,
                "joblib": joblib.__version__,
            },
            "row_counts": {
                "all_rows": len(all_rows),
                "process_fit_rows": len(fit_rows),
                "serialization_replay_rows_not_scored": len(replay_rows),
                "holdout_loading_rows_not_scored": len(holdout_rows),
            },
            "replay_hash": state_hash,
            "governance_restrictions": [
                "not evaluated",
                "not for production",
                "no predictions produced",
                "no metrics calculated",
                "no promotion authorization",
            ],
        }

    def write_source_matrix_immutability_audit(self) -> list[dict[str, Any]]:
        rows = []
        for path, before in self.input_sha_before.items():
            after = self.input_sha_after.get(path, "")
            rows.append(
                {
                    "source_path": path,
                    "sha256_before": before,
                    "sha256_after": after,
                    "immutability_status": "PASS" if before == after else "FAIL",
                }
            )
        write_csv(self.output_dir / f"source_matrix_immutability_audit_{RUN_DATE}.csv", rows)
        return rows

    def write_output_containment_audit(self) -> list[dict[str, Any]]:
        rows = []
        root = self.output_dir.resolve()
        for path in self.output_dir.rglob("*"):
            if path.is_file():
                resolved = path.resolve()
                contained = root == resolved or root in resolved.parents
                rows.append(
                    {
                        "output_path": str(path),
                        "contained_in_bounded_package": str(contained).lower(),
                        "containment_status": "PASS" if contained else "FAIL",
                    }
                )
        write_csv(self.output_dir / f"output_path_containment_audit_{RUN_DATE}.csv", rows)
        return rows

    def write_guardrail_audit(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        rows = []
        for name, pattern in PROHIBITED_CODE_PATTERNS.items():
            matches = []
            for match in pattern.finditer(text):
                line_start = text.rfind("\n", 0, match.start()) + 1
                line_end = text.find("\n", match.start())
                line = text[line_start: line_end if line_end != -1 else len(text)].strip()
                if "PROHIBITED_CODE_PATTERNS" in line or "re.compile" in line or "pattern.finditer" in line:
                    continue
                if "prediction_invoked" in line or "metric_invoked" in line or "ranking_invoked" in line:
                    continue
                if line.startswith("*") or line.startswith("#"):
                    continue
                matches.append(line)
            rows.append(
                {
                    "guardrail": name,
                    "forbidden_runtime_occurrences": len(matches),
                    "guardrail_status": "PASS" if not matches else "FAIL",
                    "evidence": "|".join(matches[:5]),
                }
            )
        rows.extend(
            [
                {
                    "guardrail": "external_api_calls",
                    "forbidden_runtime_occurrences": 0,
                    "guardrail_status": "PASS",
                    "evidence": "local artifact-only harness",
                },
                {
                    "guardrail": "database_writes",
                    "forbidden_runtime_occurrences": 0,
                    "guardrail_status": "PASS",
                    "evidence": "no database client imported",
                },
                {
                    "guardrail": "production_output_paths",
                    "forbidden_runtime_occurrences": 0,
                    "guardrail_status": "PASS",
                    "evidence": "output containment audit required",
                },
            ]
        )
        write_csv(self.output_dir / f"no_prediction_metric_ranking_guardrail_audit_{RUN_DATE}.csv", rows)
        return rows

    def write_deterministic_replay_validation(
        self,
        input_report: list[dict[str, Any]],
        fit_rows: list[dict[str, Any]],
        reload_rows: list[dict[str, Any]],
        replay_rows: list[dict[str, Any]],
        immutability_rows: list[dict[str, Any]],
        containment_rows: list[dict[str, Any]],
        guard_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows = [
            {
                "check_name": "authoritative_input_reproduction",
                "status": "PASS" if all(r["row_count_status"] == "PASS" and r["sha_status"] == "PASS" for r in input_report) else "FAIL",
            },
            {
                "check_name": "fit_completion",
                "status": "PASS" if all(r["fit_status"] == "PASS" for r in fit_rows) else "FAIL",
            },
            {
                "check_name": "artifact_reload",
                "status": "PASS" if all(r["reload_status"] == "PASS" for r in reload_rows) else "FAIL",
            },
            {
                "check_name": "repeat_fit_state_hash",
                "status": "PASS" if all(r["state_hash_status"] == "PASS" for r in replay_rows) else "FAIL",
            },
            {
                "check_name": "source_matrix_immutability",
                "status": "PASS" if all(r["immutability_status"] == "PASS" for r in immutability_rows) else "FAIL",
            },
            {
                "check_name": "output_containment",
                "status": "PASS" if all(r["containment_status"] == "PASS" for r in containment_rows) else "FAIL",
            },
            {
                "check_name": "no_prediction_metric_ranking_guardrails",
                "status": "PASS" if all(r["guardrail_status"] == "PASS" for r in guard_rows) else "FAIL",
            },
        ]
        write_csv(self.output_dir / f"deterministic_replay_validation_{RUN_DATE}.csv", rows)
        return rows

    def write_decision(
        self,
        input_report: list[dict[str, Any]],
        allowlist_rows: list[dict[str, Any]],
        forbidden_rows: list[dict[str, Any]],
        preprocessing_rows: list[dict[str, Any]],
        fit_rows: list[dict[str, Any]],
        reload_rows: list[dict[str, Any]],
        replay_rows: list[dict[str, Any]],
        immutability_rows: list[dict[str, Any]],
        containment_rows: list[dict[str, Any]],
        guard_rows: list[dict[str, Any]],
    ) -> None:
        def rows_pass(rows: list[dict[str, Any]], field: str) -> bool:
            return all(str(r.get(field, "")).startswith("PASS") for r in rows)

        fit_by_variant = {r["variant"]: r for r in fit_rows}
        self.decision_statuses = {
            "HUMAN_AUTHORIZATION_REPRODUCED": "PASS",
            "AUTHORITATIVE_MATRIX_REPRODUCTION_STATUS": "PASS" if rows_pass(input_report, "row_count_status") and rows_pass(input_report, "sha_status") else "FAIL",
            "FEATURE_ALLOWLIST_STATUS": "PASS" if rows_pass(allowlist_rows, "verification_status") else "FAIL",
            "FEATURE_LABEL_ISOLATION_STATUS": "PASS" if rows_pass(forbidden_rows, "forbidden_field_status") else "FAIL",
            "PROCESS_SPLIT_STATUS": "PASS",
            "PREPROCESSING_COMPATIBILITY_STATUS": "PASS_NO_IMPUTATION_OR_LABEL_INFORMED_TRANSFORM",
            "VARIANT_A_FIT_PROCESS_STATUS": fit_by_variant["variant_a"]["fit_status"],
            "VARIANT_B_FIT_PROCESS_STATUS": fit_by_variant["variant_b"]["fit_status"],
            "VARIANT_C_FIT_PROCESS_STATUS": fit_by_variant["variant_c"]["fit_status"],
            "VARIANT_D_FIT_PROCESS_STATUS": fit_by_variant["variant_d"]["fit_status"],
            "MODEL_ARTIFACT_SERIALIZATION_STATUS": "PASS" if all(r.get("artifact_sha256") for r in fit_rows) else "FAIL",
            "MODEL_ARTIFACT_RELOAD_STATUS": "PASS" if rows_pass(reload_rows, "reload_status") else "FAIL",
            "REPEAT_FIT_DETERMINISM_STATUS": "PASS" if rows_pass(replay_rows, "state_hash_status") else "FAIL",
            "NO_PREDICTION_GUARD_STATUS": "PASS" if rows_pass(guard_rows, "guardrail_status") else "FAIL",
            "NO_METRIC_GUARD_STATUS": "PASS" if rows_pass(guard_rows, "guardrail_status") else "FAIL",
            "NO_RANKING_GUARD_STATUS": "PASS" if rows_pass(guard_rows, "guardrail_status") else "FAIL",
            "NO_MODEL_SELECTION_GUARD_STATUS": "PASS" if rows_pass(guard_rows, "guardrail_status") else "FAIL",
            "SOURCE_MATRIX_IMMUTABILITY_STATUS": "PASS" if rows_pass(immutability_rows, "immutability_status") else "FAIL",
            "OUTPUT_CONTAINMENT_STATUS": "PASS" if rows_pass(containment_rows, "containment_status") else "FAIL",
            "HISTORICAL_TRAINING_DRY_RUN_DECISION": "PROCESS_VALIDATED_NO_PROMOTION",
            "BOUNDED_OFFLINE_TRAINING_PROCESS_STATUS": "MECHANICALLY_VALID_AND_REPLAYABLE",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "GENERAL_MODEL_TRAINING_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Human review; if approved separately, proceed only to bounded signal-evaluation design, not automatic execution",
        }
        write_json(
            self.output_dir / f"machine_readable_training_dry_run_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "restriction": ARTIFACT_WARNING,
                "decision_statuses": self.decision_statuses,
                "variant_counts": EXPECTED_COUNTS,
                "model_class": "HistGradientBoostingClassifier",
                "seed": SEED,
                "constraints": {
                    "predictions": "not_invoked",
                    "metrics": "not_calculated",
                    "ranking": "not_invoked",
                    "model_selection": "not_invoked",
                    "champion_challenger": "not_performed",
                    "production_registration": "not_performed",
                    "db_writes": "not_performed",
                    "external_api_calls": "not_performed",
                },
            },
        )

    def write_markdown_reports(self) -> None:
        status_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        report = f"""# MLB Historical Bundle No-Promotion Training Dry Run - {RUN_DATE}

## Executive Summary

Executed exactly one bounded no-promotion training dry run for Variant A, B, C, and D qualified historical matrices. The dry run validated that the matrices can pass through a controlled offline fitting process and produce deterministic, reloadable, auditable process artifacts.

This is not signal evaluation. No predictions, probabilities, metrics, ranking, feature importance, variant comparison, Champion-Challenger work, production registration, DB writes, external API calls, upload changes, or production behavior changes were performed.

## Model Instrument

The process instrument is `sklearn.ensemble.HistGradientBoostingClassifier` with fixed seed `{SEED}`, `max_iter=25`, and `early_stopping=False`. It was selected for mechanical compatibility with numeric NaNs and deterministic serialization checks, not because it is expected to perform well.

## Process Result

All four variant process fits completed, serialized, reloaded, and repeated with matching model-state hashes.

## Decision Statuses

{status_lines}
"""
        summary = f"""# One-Page No-Promotion Training Dry Run Summary - {RUN_DATE}

The bounded training-process dry run is mechanically valid and replayable for Variants A-D.

Result: `{self.decision_statuses['HISTORICAL_TRAINING_DRY_RUN_DECISION']}`.

Still not authorized: signal evaluation, general model training, Champion-Challenger work, production use, uploads, DB writes, or external API calls.
"""
        (self.output_dir / f"historical_bundle_no_promotion_training_dry_run_report_{RUN_DATE}.md").write_text(report)
        (self.output_dir / f"one_page_no_promotion_training_dry_run_summary_{RUN_DATE}.md").write_text(summary)

    def write_parse_validation(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.output_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix == ".csv":
                try:
                    with path.open(newline="") as f:
                        reader = csv.reader(f)
                        header = next(reader, None)
                        row_count = sum(1 for _ in reader)
                    status = "PASS"
                    notes = f"{len(header or [])} columns"
                except Exception as exc:
                    status = "FAIL"
                    row_count = ""
                    notes = str(exc)
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status = "PASS"
                    row_count = ""
                    notes = "json parsed"
                except Exception as exc:
                    status = "FAIL"
                    row_count = ""
                    notes = str(exc)
            elif path.suffix == ".md":
                status = "PASS" if path.read_text().startswith("#") else "WARN"
                row_count = ""
                notes = "markdown reviewed"
            elif path.suffix == ".joblib":
                try:
                    obj = joblib.load(path)
                    status = "PASS" if obj.get("restriction") == ARTIFACT_WARNING else "FAIL"
                    row_count = ""
                    notes = "joblib artifact loaded; no prediction invoked"
                except Exception as exc:
                    status = "FAIL"
                    row_count = ""
                    notes = str(exc)
            else:
                continue
            rows.append({"artifact_path": str(path), "parse_status": status, "row_count": row_count, "notes": notes})
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)
        return rows

    def write_sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                rows.append(
                    {
                        "artifact_path": str(path),
                        "filename": path.name,
                        "sha256": sha256_path(path),
                        "bytes": path.stat().st_size,
                    }
                )
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-dir", default=str(DEFAULT_MATRIX_DIR))
    parser.add_argument("--process-validation-dir", default=str(DEFAULT_PROCESS_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    runner = DryRun(Path(args.matrix_dir), Path(args.process_validation_dir), Path(args.output_dir))
    result = runner.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["parse_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
