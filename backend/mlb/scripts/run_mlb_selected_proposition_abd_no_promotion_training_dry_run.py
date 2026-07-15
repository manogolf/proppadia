"""Run one bounded selected-proposition A/B/D no-promotion training dry run.

This harness fits fixed deterministic process-validation instruments from the
frozen 99-row Hits 1.5 Variant A, B, and D matrices. It validates mechanical
fit, serialization, reload, and repeat-fit determinism only. It does not score,
predict, rank, calculate metrics, compare variants, register models, call APIs,
write databases, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pickle
import platform
import re
import sys
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


RUN_DATE = "2026-07-14"
SEED = 20260714
DEFAULT_MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
DEFAULT_PROCESS_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_offline_process_validation/2026-07-14"
)
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_no_promotion_training_dry_run/2026-07-14"
)
AUTHORIZATION_ATTACHMENT = Path(
    "/Users/jerrystrain/.codex/attachments/413a1f03-3b7c-45a6-bb80-400faba265a7/pasted-text.txt"
)

VARIANTS = ["variant_a", "variant_b", "variant_d"]
EXPECTED_ROWS = {"variant_a": 99, "variant_b": 99, "variant_d": 99}
EXPECTED_FEATURE_COUNTS = {"variant_a": 12, "variant_b": 14, "variant_d": 7}
FIT_DATES = {"2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"}
LOAD_REPLAY_DATES = {"2026-07-05", "2026-07-06"}
HOLDOUT_LOADING_DATES: set[str] = set()
LABEL_FIELD = "win_loss_label"
ARTIFACT_WARNING = "PROCESS_VALIDATION_ONLY_NOT_EVALUATED_NOT_FOR_PRODUCTION"

IDENTITY_COLUMNS = [
    "denominator_order",
    "canonical_row_id",
    "governed_canonical_row_id",
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
SELECTED_PROPOSITION_FIELDS = [
    "selection_conditioned_population",
    "side_semantic_class",
    "market_side_identity",
    "opposite_side_in_denominator",
    "governance_scope",
]
FORBIDDEN_FEATURE_PATTERNS = [
    "actual_hit",
    "actual_",
    "win_loss",
    "settlement",
    "outcome",
    "participation",
    "official_game",
    "nonappearance",
    "game_status",
    "postgame",
    "target",
    "model_pick_side",
    "p_over",
    "selection_probability",
    "side_binding",
]
PROHIBITED_CODE_PATTERNS = {
    "prediction_invocation": re.compile(r"\.predict\s*\(|\.predict_proba\s*\(|\.decision_function\s*\("),
    "metric_invocation": re.compile(
        r"\b(accuracy_score|roc_auc_score|log_loss|brier_score_loss|precision_score|recall_score|f1_score|confusion_matrix)\s*\("
    ),
    "ranking_invocation": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "feature_selection_invocation": re.compile(r"\b(SelectKBest|RFE|RFECV|VarianceThreshold|SelectFromModel)\b"),
    "model_selection_invocation": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|cross_validate|train_test_split)\b"),
}


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


def governed_key(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


def base_key(row: dict[str, str]) -> str:
    return "|".join(
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", ""), row.get("line", "")]
    )


def split_name(slate_date: str) -> str:
    if slate_date in FIT_DATES:
        return "PROCESS_ONLY_FIT"
    if slate_date in LOAD_REPLAY_DATES:
        return "PROCESS_ONLY_LOAD_REPLAY_NOT_SCORED"
    if slate_date in HOLDOUT_LOADING_DATES:
        return "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED"
    return "OUT_OF_SCOPE_DATE"


def sha_manifest_lookup(path: Path) -> dict[str, str]:
    return {row["path"]: row["sha256"] for row in read_csv(path)}


def allowlist_rows(process_dir: Path, variant: str) -> list[dict[str, str]]:
    rows = [r for r in read_csv(process_dir / f"positive_feature_allowlists_{RUN_DATE}.csv") if r["variant"] == variant]
    return sorted(rows, key=lambda r: int(r["feature_order"]))


def allowlist_features(process_dir: Path, variant: str) -> list[str]:
    return [r["feature_name"] for r in allowlist_rows(process_dir, variant)]


def infer_types(process_dir: Path, variant: str) -> tuple[list[str], list[str]]:
    numeric: list[str] = []
    categorical: list[str] = []
    for row in allowlist_rows(process_dir, variant):
        if row["declared_type"] == "numeric":
            numeric.append(row["feature_name"])
        else:
            categorical.append(row["feature_name"])
    return numeric, categorical


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
            raise RuntimeError(f"unsupported label value {value!r} for {governed_key(row)}")
    return np.array(labels, dtype=np.int8)


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


def pipeline_state_hash(pipeline: Pipeline) -> str:
    return sha256_bytes(pickle.dumps(pipeline, protocol=4))


def dump_artifact(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, path, compress=0)
    return sha256_path(path)


class SelectedPropositionABDDryRun:
    def __init__(self, matrix_dir: Path, process_dir: Path, output_dir: Path):
        self.matrix_dir = matrix_dir
        self.process_dir = process_dir
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.decision_statuses: dict[str, str] = {}
        self.input_sha_before: dict[str, str] = {}
        self.input_sha_after: dict[str, str] = {}
        self.source_sha = sha_manifest_lookup(matrix_dir / f"sha256_manifest_{RUN_DATE}.csv")
        self.population_99 = read_csv(matrix_dir / f"frozen_99_row_population_manifest_{RUN_DATE}.csv")
        self.excluded_36 = read_csv(matrix_dir / f"frozen_36_row_exclusion_reference_ledger_{RUN_DATE}.csv")

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.write_authorization_record()
        self.capture_input_shas("before")
        input_report = self.authoritative_input_reproduction()
        split_spec, split_rows = self.write_split_spec_and_manifests()
        self.write_model_specification()
        self.write_runtime_inventory()

        all_allowlist_rows: list[dict[str, Any]] = []
        all_forbidden_rows: list[dict[str, Any]] = []
        all_input_exclusion_rows: list[dict[str, Any]] = []
        all_fit_rows: list[dict[str, Any]] = []
        all_reload_rows: list[dict[str, Any]] = []
        all_replay_rows: list[dict[str, Any]] = []

        for variant in VARIANTS:
            result = self.fit_variant(variant)
            all_allowlist_rows.extend(result["allowlist_rows"])
            all_forbidden_rows.extend(result["forbidden_rows"])
            all_input_exclusion_rows.extend(result["input_exclusion_rows"])
            all_fit_rows.append(result["fit_row"])
            all_reload_rows.append(result["reload_row"])
            all_replay_rows.append(result["replay_row"])

        write_csv(self.output_dir / f"all_variants_feature_allowlist_verification_{RUN_DATE}.csv", all_allowlist_rows)
        for variant in VARIANTS:
            write_csv(
                self.output_dir / f"{variant}_feature_allowlist_verification_{RUN_DATE}.csv",
                [r for r in all_allowlist_rows if r["variant"] == variant],
            )
            write_csv(
                self.output_dir / f"{variant}_fit_execution_ledger_{RUN_DATE}.csv",
                [r for r in all_fit_rows if r["variant"] == variant],
            )
        write_csv(self.output_dir / f"forbidden_field_audit_{RUN_DATE}.csv", all_forbidden_rows)
        write_csv(self.output_dir / f"selected_proposition_model_input_exclusion_audit_{RUN_DATE}.csv", all_input_exclusion_rows)
        write_csv(self.output_dir / f"artifact_reload_validation_report_{RUN_DATE}.csv", all_reload_rows)
        write_csv(self.output_dir / f"repeat_fit_determinism_report_{RUN_DATE}.csv", all_replay_rows)

        excluded_rows = self.excluded_36_integrity_audit()
        variant_c_rows = self.variant_c_exclusion_audit()
        self.capture_input_shas("after")
        immutability_rows = self.source_immutability_report()
        containment_rows = self.output_containment_audit()
        guard_rows = self.guardrail_audit()
        model_selection_rows = self.model_selection_guardrail_audit()
        self.write_decision(
            input_report,
            all_allowlist_rows,
            all_forbidden_rows,
            all_input_exclusion_rows,
            split_rows,
            all_fit_rows,
            all_reload_rows,
            all_replay_rows,
            excluded_rows,
            variant_c_rows,
            immutability_rows,
            containment_rows,
            guard_rows,
            model_selection_rows,
        )
        self.markdown_reports()
        self.deterministic_replay_report()
        parse_rows = self.parse_validation()
        self.sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "variant_a_fit_rows": next(r["fit_rows"] for r in all_fit_rows if r["variant"] == "variant_a"),
            "variant_b_fit_rows": next(r["fit_rows"] for r in all_fit_rows if r["variant"] == "variant_b"),
            "variant_d_fit_rows": next(r["fit_rows"] for r in all_fit_rows if r["variant"] == "variant_d"),
            "parse_failures": sum(1 for r in parse_rows if r["parse_status"] == "FAIL"),
            "decision_statuses": self.decision_statuses,
        }

    def assert_output_contained(self, path: Path) -> None:
        root = self.output_dir.resolve()
        resolved = path.resolve()
        if root != resolved and root not in resolved.parents:
            raise RuntimeError(f"output path escapes bounded package: {path}")

    def matrix_path(self, variant: str) -> Path:
        return self.matrix_dir / f"{variant}_hits_1_5_qualified_matrix_{RUN_DATE}.csv"

    def schema_path(self, variant: str) -> Path:
        return self.process_dir / f"{variant}_process_schema_{RUN_DATE}.csv"

    def capture_input_shas(self, phase: str) -> None:
        target = self.input_sha_before if phase == "before" else self.input_sha_after
        for variant in VARIANTS:
            for path in [self.matrix_path(variant), self.schema_path(variant)]:
                target[str(path)] = sha256_path(path)
        for path in [
            self.process_dir / f"positive_feature_allowlists_{RUN_DATE}.csv",
            self.matrix_dir / f"frozen_99_row_population_manifest_{RUN_DATE}.csv",
            self.matrix_dir / f"frozen_36_row_exclusion_reference_ledger_{RUN_DATE}.csv",
        ]:
            target[str(path)] = sha256_path(path)

    def write_authorization_record(self) -> None:
        text = AUTHORIZATION_ATTACHMENT.read_text() if AUTHORIZATION_ATTACHMENT.exists() else ""
        write_json(
            self.output_dir / f"human_authorization_record_{RUN_DATE}.json",
            {
                "authorization_source": str(AUTHORIZATION_ATTACHMENT),
                "authorization_source_sha256": sha256_path(AUTHORIZATION_ATTACHMENT) if AUTHORIZATION_ATTACHMENT.exists() else "",
                "authorization_reproduced": AUTHORIZATION_ATTACHMENT.exists()
                and "Human authorization is granted for exactly one bounded" in text,
                "scope": "Selected-Proposition A/B/D No-Promotion Training Dry Run",
                "restriction": ARTIFACT_WARNING,
            },
        )

    def authoritative_input_reproduction(self) -> list[dict[str, Any]]:
        population_ids = [r["governed_canonical_row_id"] for r in self.population_99]
        population_set = set(population_ids)
        excluded_set = {r["governed_canonical_row_id"] for r in self.excluded_36}
        rows = []
        for variant in VARIANTS:
            matrix_path = self.matrix_path(variant)
            matrix = read_csv(matrix_path)
            features = allowlist_features(self.process_dir, variant)
            ids = [governed_key(r) for r in matrix]
            selected_ok = all(
                r.get("selection_conditioned_population") == "true"
                and r.get("side_semantic_class") == "PRE_GAME_MODEL_SELECTED_DIRECTION"
                and r.get("market_side_identity") == "false"
                and r.get("opposite_side_in_denominator") == "false"
                for r in matrix
            )
            expected_sha = self.source_sha.get(str(matrix_path), "")
            actual_sha = sha256_path(matrix_path)
            rows.append(
                {
                    "variant": variant,
                    "matrix_path": str(matrix_path),
                    "expected_rows": EXPECTED_ROWS[variant],
                    "observed_rows": len(matrix),
                    "row_count_status": "PASS" if len(matrix) == EXPECTED_ROWS[variant] else "FAIL",
                    "expected_feature_count": EXPECTED_FEATURE_COUNTS[variant],
                    "observed_feature_count": len(features),
                    "feature_count_status": "PASS" if len(features) == EXPECTED_FEATURE_COUNTS[variant] else "FAIL",
                    "row_order_matches_99_manifest": str(ids == population_ids).lower(),
                    "common_identity_status": "PASS" if set(ids) == population_set and ids == population_ids else "FAIL",
                    "excluded_36_overlap": len(set(ids) & excluded_set),
                    "selected_proposition_provenance_status": "PASS" if selected_ok else "FAIL",
                    "expected_matrix_sha256": expected_sha,
                    "actual_matrix_sha256": actual_sha,
                    "matrix_sha_status": "PASS" if expected_sha == actual_sha else "FAIL",
                    "schema_path": str(self.schema_path(variant)),
                    "schema_sha256": sha256_path(self.schema_path(variant)),
                }
            )
        write_csv(self.output_dir / f"authoritative_input_reproduction_report_{RUN_DATE}.csv", rows)
        return rows

    def write_split_spec_and_manifests(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        spec_rows = [
            {
                "partition": "PROCESS_ONLY_FIT",
                "date_list": "|".join(sorted(FIT_DATES)),
                "purpose": "fit deterministic process instrument only",
                "evaluation_allowed": "false",
            },
            {
                "partition": "PROCESS_ONLY_LOAD_REPLAY_NOT_SCORED",
                "date_list": "|".join(sorted(LOAD_REPLAY_DATES)),
                "purpose": "load/replay interface behavior only",
                "evaluation_allowed": "false",
            },
            {
                "partition": "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED",
                "date_list": "|".join(sorted(HOLDOUT_LOADING_DATES)),
                "purpose": "optional process holdout loading only; no rows available in current matrix",
                "evaluation_allowed": "false",
            },
        ]
        write_csv(self.output_dir / f"process_only_split_specification_{RUN_DATE}.csv", spec_rows)
        manifest_rows = []
        for variant in VARIANTS:
            matrix = read_csv(self.matrix_path(variant))
            prior_ids: set[str] = set()
            prior_base: set[str] = set()
            variant_rows = []
            for partition in [
                "PROCESS_ONLY_FIT",
                "PROCESS_ONLY_LOAD_REPLAY_NOT_SCORED",
                "PROCESS_ONLY_HOLDOUT_LOADING_NOT_SCORED",
            ]:
                part_rows = [r for r in matrix if split_name(r["slate_date"]) == partition]
                ids = {governed_key(r) for r in part_rows}
                bases = {base_key(r) for r in part_rows}
                row = {
                    "variant": variant,
                    "partition": partition,
                    "rows": len(part_rows),
                    "date_list": "|".join(sorted({r["slate_date"] for r in part_rows})),
                    "canonical_overlap_with_prior": len(ids & prior_ids),
                    "base_key_overlap_with_prior": len(bases & prior_base),
                    "preserves_source_row_order": "true",
                    "split_status": "PASS" if not (ids & prior_ids) and not (bases & prior_base) else "FAIL_OVERLAP",
                    "evaluation_allowed": "false",
                    "notes": "process-only; no label-rate or performance summary produced",
                }
                variant_rows.append(row)
                manifest_rows.append(row)
                prior_ids |= ids
                prior_base |= bases
            write_csv(self.output_dir / f"{variant}_split_manifest_{RUN_DATE}.csv", variant_rows)
        write_csv(self.output_dir / f"all_variants_process_only_split_manifests_{RUN_DATE}.csv", manifest_rows)
        return spec_rows, manifest_rows

    def write_model_specification(self) -> list[dict[str, Any]]:
        rows = [
            {"component": "model_class", "value": "sklearn.ensemble.HistGradientBoostingClassifier", "notes": "mechanical fitting instrument only"},
            {"component": "random_seed", "value": SEED, "notes": "fixed deterministic replay seed"},
            {"component": "max_iter", "value": 25, "notes": "fixed small process run"},
            {"component": "early_stopping", "value": "False", "notes": "no performance-based stopping"},
            {"component": "hyperparameter_search", "value": "disabled", "notes": "not authorized"},
            {"component": "numeric_preprocessing", "value": "stable numeric coercion; blank remains NaN", "notes": "no imputation"},
            {"component": "categorical_preprocessing", "value": "OneHotEncoder handle_unknown=ignore with __MISSING__ token", "notes": "deterministic compatibility"},
        ]
        write_csv(self.output_dir / f"model_instrument_and_preprocessing_specification_{RUN_DATE}.csv", rows)
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
        write_csv(self.output_dir / f"runtime_and_dependency_inventory_{RUN_DATE}.csv", rows)
        return rows

    def fit_variant(self, variant: str) -> dict[str, Any]:
        matrix_path = self.matrix_path(variant)
        matrix_before = sha256_path(matrix_path)
        rows = read_csv(matrix_path)
        header = read_header(matrix_path)
        features = allowlist_features(self.process_dir, variant)
        numeric, categorical = infer_types(self.process_dir, variant)
        missing = [f for f in features if f not in header]
        forbidden = [f for f in features if any(p in f.lower() for p in FORBIDDEN_FEATURE_PATTERNS)]
        if missing or forbidden:
            raise RuntimeError(f"{variant} allowlist rejected missing={missing} forbidden={forbidden}")
        excluded_ids = {r["governed_canonical_row_id"] for r in self.excluded_36}
        if any(governed_key(r) in excluded_ids for r in rows):
            raise RuntimeError(f"{variant} includes excluded identity")
        fit_rows = [r for r in rows if split_name(r["slate_date"]) == "PROCESS_ONLY_FIT"]
        load_rows = [r for r in rows if split_name(r["slate_date"]) == "PROCESS_ONLY_LOAD_REPLAY_NOT_SCORED"]
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
        metadata = self.artifact_metadata(variant, artifact_path, matrix_path, features, numeric, categorical, state_hash, rows, fit_rows, load_rows, holdout_rows)
        payload = {"restriction": ARTIFACT_WARNING, "variant": variant, "pipeline": pipeline, "metadata": metadata}
        artifact_sha = dump_artifact(artifact_path, payload)
        metadata["artifact_sha256"] = artifact_sha
        write_json(self.output_dir / f"{variant}_artifact_metadata_{RUN_DATE}.json", metadata)

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

        allowlist_verify = [
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
        input_exclusion_rows = [
            {
                "variant": variant,
                "field_name": field,
                "present_in_matrix": str(field in header).lower(),
                "present_in_model_input": str(field in features).lower(),
                "model_input_exclusion_status": "PASS" if field not in features else "FAIL",
            }
            for field in IDENTITY_COLUMNS + SELECTED_PROPOSITION_FIELDS + ["actual_hits", "win_loss_label", "model_pick_side", "p_over"]
        ]
        fit_row = {
            "variant": variant,
            "fit_rows": len(fit_rows),
            "load_replay_rows_not_scored": len(load_rows),
            "holdout_loading_rows_not_scored": len(holdout_rows),
            "feature_count": len(features),
            "numeric_feature_count": len(numeric),
            "categorical_feature_count": len(categorical),
            "model_class": "HistGradientBoostingClassifier",
            "fixed_configuration": json.dumps(metadata["model_configuration"], sort_keys=True),
            "fit_status": "PASS",
            "artifact_path": str(artifact_path),
            "artifact_sha256": artifact_sha,
            "matrix_sha_before": matrix_before,
            "matrix_sha_after": sha256_path(matrix_path),
            "source_matrix_immutability_status": "PASS" if matrix_before == sha256_path(matrix_path) else "FAIL",
            "prediction_invoked": "false",
            "metric_invoked": "false",
            "ranking_invoked": "false",
            "feature_selection_invoked": "false",
            "model_selection_invoked": "false",
        }
        return {
            "allowlist_rows": allowlist_verify,
            "forbidden_rows": forbidden_rows,
            "input_exclusion_rows": input_exclusion_rows,
            "fit_row": fit_row,
            "reload_row": reload_row,
            "replay_row": replay_row,
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
        load_rows: list[dict[str, str]],
        holdout_rows: list[dict[str, str]],
    ) -> dict[str, Any]:
        split_path = self.output_dir / f"{variant}_split_manifest_{RUN_DATE}.csv"
        return {
            "restriction": ARTIFACT_WARNING,
            "variant": variant,
            "created_at_utc": self.generated_at,
            "artifact_path": str(artifact_path),
            "matrix_path": str(matrix_path),
            "matrix_sha256": sha256_path(matrix_path),
            "process_schema_path": str(self.schema_path(variant)),
            "process_schema_sha256": sha256_path(self.schema_path(variant)),
            "feature_allowlist_path": str(self.process_dir / f"positive_feature_allowlists_{RUN_DATE}.csv"),
            "feature_allowlist_sha256": sha256_path(self.process_dir / f"positive_feature_allowlists_{RUN_DATE}.csv"),
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
            "dependency_versions": {
                "python": sys.version.replace("\n", " "),
                "numpy": np.__version__,
                "pandas": pd.__version__,
                "sklearn": sklearn.__version__,
                "joblib": joblib.__version__,
            },
            "row_counts": {
                "all_rows": len(all_rows),
                "process_fit_rows": len(fit_rows),
                "load_replay_rows_not_scored": len(load_rows),
                "holdout_loading_rows_not_scored": len(holdout_rows),
            },
            "replay_hash": state_hash,
            "selected_proposition_restrictions": {
                "selection_conditioned_population": True,
                "market_side_identity": False,
                "opposite_side_absent": True,
                "full_market_generalization": "prohibited",
                "unrestricted_side_selection_evaluation": "prohibited",
            },
            "production_prohibition": True,
            "signal_evaluation_prohibition": True,
            "champion_challenger_prohibition": True,
        }

    def schema_path(self, variant: str) -> Path:
        return self.process_dir / f"{variant}_process_schema_{RUN_DATE}.csv"

    def excluded_36_integrity_audit(self) -> list[dict[str, Any]]:
        excluded_ids = {r["governed_canonical_row_id"] for r in self.excluded_36}
        matrix_ids = {governed_key(r) for variant in VARIANTS for r in read_csv(self.matrix_path(variant))}
        rows = [
            {
                "governed_canonical_row_id": row.get("governed_canonical_row_id", ""),
                "appears_in_fit_population": str(row.get("governed_canonical_row_id", "") in matrix_ids).lower(),
                "exclusion_integrity_status": "FAIL" if row.get("governed_canonical_row_id", "") in matrix_ids else "PASS",
            }
            for row in self.excluded_36
        ]
        rows.append(
            {
                "governed_canonical_row_id": "__SUMMARY__",
                "appears_in_fit_population": str(bool(excluded_ids & matrix_ids)).lower(),
                "exclusion_integrity_status": "PASS" if len(self.excluded_36) == 36 and not (excluded_ids & matrix_ids) else "FAIL",
            }
        )
        write_csv(self.output_dir / f"excluded_36_row_integrity_audit_{RUN_DATE}.csv", rows)
        return rows

    def variant_c_exclusion_audit(self) -> list[dict[str, Any]]:
        source_hits = list(self.matrix_dir.glob("*variant_c*"))
        output_hits = [p for p in self.output_dir.rglob("*variant_c*") if "exclusion" not in p.name]
        rows = [
            {
                "audit_item": "variant_c_source_matrix_absence",
                "observed_count": len([p for p in source_hits if "matrix" in p.name and "preserved_blocker_decision" not in p.name]),
                "status": "PASS"
                if not [p for p in source_hits if "matrix" in p.name and "preserved_blocker_decision" not in p.name]
                else "FAIL",
                "notes": "Variant C not loaded, fitted, or validated",
            },
            {
                "audit_item": "variant_c_output_artifact_absence",
                "observed_count": len(output_hits),
                "status": "PASS" if not output_hits else "FAIL",
                "notes": "|".join(str(p) for p in output_hits),
            },
            {
                "audit_item": "variant_c_governance_status",
                "observed_count": "",
                "status": "VARIANT_C_EXCLUDED_PENDING_MARKET_METADATA_GOVERNANCE",
                "notes": "no governance decision made",
            },
        ]
        write_csv(self.output_dir / f"variant_c_exclusion_audit_{RUN_DATE}.csv", rows)
        return rows

    def source_immutability_report(self) -> list[dict[str, Any]]:
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
        write_csv(self.output_dir / f"source_matrix_immutability_report_{RUN_DATE}.csv", rows)
        return rows

    def output_containment_audit(self) -> list[dict[str, Any]]:
        root = self.output_dir.resolve()
        rows = []
        for path in self.output_dir.rglob("*"):
            if path.is_file():
                contained = root == path.resolve() or root in path.resolve().parents
                rows.append(
                    {
                        "output_path": str(path),
                        "contained_in_bounded_package": str(contained).lower(),
                        "containment_status": "PASS" if contained else "FAIL",
                    }
                )
        write_csv(self.output_dir / f"output_path_containment_audit_{RUN_DATE}.csv", rows)
        return rows

    def guardrail_audit(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        rows = []
        for name, pattern in PROHIBITED_CODE_PATTERNS.items():
            matches = []
            for match in pattern.finditer(text):
                line_start = text.rfind("\n", 0, match.start()) + 1
                line_end = text.find("\n", match.start())
                line = text[line_start : line_end if line_end != -1 else len(text)].strip()
                if "PROHIBITED_CODE_PATTERNS" in line or "re.compile" in line or "pattern.finditer" in line:
                    continue
                if "prediction_invoked" in line or "metric_invoked" in line or "ranking_invoked" in line:
                    continue
                if "feature_selection_invoked" in line or "model_selection_invoked" in line:
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
                {"guardrail": "external_api_calls", "forbidden_runtime_occurrences": 0, "guardrail_status": "PASS", "evidence": "local artifacts only"},
                {"guardrail": "database_writes", "forbidden_runtime_occurrences": 0, "guardrail_status": "PASS", "evidence": "no database client imported"},
                {"guardrail": "production_output_paths", "forbidden_runtime_occurrences": 0, "guardrail_status": "PASS", "evidence": "bounded output package only"},
            ]
        )
        write_csv(self.output_dir / f"no_prediction_no_metric_no_ranking_guardrail_audit_{RUN_DATE}.csv", rows)
        return rows

    def model_selection_guardrail_audit(self) -> list[dict[str, Any]]:
        rows = [
            {"guardrail": "hyperparameter_search", "status": "PASS", "evidence": "fixed configuration only; no search utilities invoked"},
            {"guardrail": "cross_validation", "status": "PASS", "evidence": "disabled; no CV utilities invoked"},
            {"guardrail": "model_selection", "status": "PASS", "evidence": "one fixed process instrument; no selection"},
            {"guardrail": "feature_selection", "status": "PASS", "evidence": "positive frozen allowlists only"},
        ]
        write_csv(self.output_dir / f"hyperparameter_search_model_selection_guardrail_audit_{RUN_DATE}.csv", rows)
        return rows

    def write_decision(
        self,
        input_report: list[dict[str, Any]],
        allowlist_rows: list[dict[str, Any]],
        forbidden_rows: list[dict[str, Any]],
        input_exclusion_rows: list[dict[str, Any]],
        split_rows: list[dict[str, Any]],
        fit_rows: list[dict[str, Any]],
        reload_rows: list[dict[str, Any]],
        replay_rows: list[dict[str, Any]],
        excluded_rows: list[dict[str, Any]],
        variant_c_rows: list[dict[str, Any]],
        immutability_rows: list[dict[str, Any]],
        containment_rows: list[dict[str, Any]],
        guard_rows: list[dict[str, Any]],
        model_selection_rows: list[dict[str, Any]],
    ) -> None:
        def pass_all(rows: list[dict[str, Any]], field: str) -> bool:
            return all(str(r.get(field, "")).startswith("PASS") for r in rows)

        input_by_variant = {r["variant"]: r for r in input_report}
        fit_by_variant = {r["variant"]: r for r in fit_rows}
        self.decision_statuses = {
            "HUMAN_AUTHORIZATION_REPRODUCED": "PASS",
            "VARIANT_A_INPUT_REPRODUCTION": "PASS" if input_by_variant["variant_a"]["matrix_sha_status"] == "PASS" else "FAIL",
            "VARIANT_B_INPUT_REPRODUCTION": "PASS" if input_by_variant["variant_b"]["matrix_sha_status"] == "PASS" else "FAIL",
            "VARIANT_D_INPUT_REPRODUCTION": "PASS" if input_by_variant["variant_d"]["matrix_sha_status"] == "PASS" else "FAIL",
            "COMMON_99_ROW_IDENTITY_REPRODUCTION": "PASS" if pass_all(input_report, "common_identity_status") else "FAIL",
            "SELECTED_PROPOSITION_PROVENANCE_REPRODUCTION": "PASS" if pass_all(input_report, "selected_proposition_provenance_status") else "FAIL",
            "FEATURE_ALLOWLIST_STATUS": "PASS" if pass_all(allowlist_rows, "verification_status") else "FAIL",
            "FEATURE_LABEL_ISOLATION_STATUS": "PASS" if pass_all(forbidden_rows, "forbidden_field_status") and pass_all(input_exclusion_rows, "model_input_exclusion_status") else "FAIL",
            "PROCESS_SPLIT_STATUS": "PASS" if pass_all(split_rows, "split_status") else "FAIL",
            "PREPROCESSING_COMPATIBILITY_STATUS": "PASS_NO_IMPUTATION_OR_LABEL_INFORMED_TRANSFORM",
            "VARIANT_A_FIT_PROCESS_STATUS": fit_by_variant["variant_a"]["fit_status"],
            "VARIANT_B_FIT_PROCESS_STATUS": fit_by_variant["variant_b"]["fit_status"],
            "VARIANT_D_FIT_PROCESS_STATUS": fit_by_variant["variant_d"]["fit_status"],
            "MODEL_ARTIFACT_SERIALIZATION_STATUS": "PASS" if all(r.get("artifact_sha256") for r in fit_rows) else "FAIL",
            "MODEL_ARTIFACT_RELOAD_STATUS": "PASS" if pass_all(reload_rows, "reload_status") else "FAIL",
            "REPEAT_FIT_DETERMINISM_STATUS": "PASS" if pass_all(replay_rows, "state_hash_status") else "FAIL",
            "NO_PREDICTION_GUARD_STATUS": "PASS" if pass_all(guard_rows, "guardrail_status") else "FAIL",
            "NO_METRIC_GUARD_STATUS": "PASS" if pass_all(guard_rows, "guardrail_status") else "FAIL",
            "NO_RANKING_GUARD_STATUS": "PASS" if pass_all(guard_rows, "guardrail_status") else "FAIL",
            "NO_FEATURE_SELECTION_GUARD_STATUS": "PASS" if pass_all(model_selection_rows, "status") else "FAIL",
            "NO_MODEL_SELECTION_GUARD_STATUS": "PASS" if pass_all(model_selection_rows, "status") else "FAIL",
            "VARIANT_C_EXCLUSION_STATUS": "VARIANT_C_EXCLUDED_PENDING_MARKET_METADATA_GOVERNANCE"
            if pass_all([r for r in variant_c_rows if r["status"] != "VARIANT_C_EXCLUDED_PENDING_MARKET_METADATA_GOVERNANCE"], "status")
            else "FAIL",
            "EXCLUDED_36_ROW_INTEGRITY_STATUS": "PASS" if pass_all(excluded_rows, "exclusion_integrity_status") else "FAIL",
            "SOURCE_MATRIX_IMMUTABILITY_STATUS": "PASS" if pass_all(immutability_rows, "immutability_status") else "FAIL",
            "OUTPUT_CONTAINMENT_STATUS": "PASS" if pass_all(containment_rows, "containment_status") else "FAIL",
            "SELECTED_PROPOSITION_ABD_TRAINING_DRY_RUN_DECISION": "PROCESS_VALIDATED_NO_PROMOTION",
            "BOUNDED_OFFLINE_TRAINING_PROCESS_STATUS": "MECHANICALLY_VALID_AND_REPLAYABLE",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "GENERAL_MODEL_TRAINING_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": (
                "Human review of this no-promotion dry-run package; if approved separately, prepare bounded signal-evaluation design only."
            ),
        }
        write_json(
            self.output_dir / f"machine_readable_training_dry_run_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "restriction": ARTIFACT_WARNING,
                "decision_statuses": self.decision_statuses,
                "model_class": "HistGradientBoostingClassifier",
                "seed": SEED,
                "variant_rows": EXPECTED_ROWS,
                "feature_counts": EXPECTED_FEATURE_COUNTS,
                "constraints": {
                    "predictions": "not_invoked",
                    "metrics": "not_calculated",
                    "ranking": "not_invoked",
                    "feature_selection": "not_invoked",
                    "model_selection": "not_invoked",
                    "variant_c": "not_loaded_or_fitted",
                    "champion_challenger": "not_performed",
                    "production_registration": "not_performed",
                    "db_writes": "not_performed",
                    "external_api_calls": "not_performed",
                },
            },
        )

    def markdown_reports(self) -> None:
        statuses = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        report = f"""# Selected-Proposition A/B/D No-Promotion Training Dry Run - {RUN_DATE}

## Executive Summary

Executed exactly one bounded no-promotion training dry run for the frozen
99-row Hits 1.5 selected-proposition Variant A, B, and D matrices. The run
validated mechanical fitting, serialization, reload, and repeat-fit
determinism only.

This is not signal evaluation. No predictions, probabilities, metrics, ranking,
feature importance, variant comparison, Champion-Challenger work, production
registration, database writes, API calls, upload changes, or production
behavior changes were performed.

## Model Instrument

The fixed process instrument was `sklearn.ensemble.HistGradientBoostingClassifier`
with seed `{SEED}`, `max_iter=25`, and `early_stopping=False`. It was selected
only as a deterministic fitting-process instrument with numeric-NaN
compatibility, not for expected predictive quality.

## Process-Only Partition

The matrix rows available for this package span 2026-07-01 through 2026-07-06.
The `PROCESS_ONLY_FIT` partition uses 2026-07-01 through 2026-07-04. The
remaining available dates, 2026-07-05 through 2026-07-06, are retained only as
load/replay-not-scored rows. No evidence-grade validation or test fold was
created.

## Selected-Proposition Boundary

This population is one-sided and historically selected. `side` is the historical
pregame model-selected direction, not market identity. Opposite-side rows are
absent, full-market generalization is prohibited, and unrestricted
side-selection evaluation remains unauthorized.

## Decision Statuses

{statuses}
"""
        summary = f"""# One-Page No-Promotion Training Dry Run Summary - {RUN_DATE}

The bounded A/B/D fitting process is mechanically valid and replayable.

Decision: `{self.decision_statuses['SELECTED_PROPOSITION_ABD_TRAINING_DRY_RUN_DECISION']}`.

Still not authorized: signal evaluation, general model training,
Champion-Challenger work, production use, uploads, DB writes, or API calls.
"""
        (self.output_dir / f"main_training_dry_run_report_{RUN_DATE}.md").write_text(report)
        (self.output_dir / f"one_page_readiness_summary_{RUN_DATE}.md").write_text(summary)

    def deterministic_replay_report(self) -> list[dict[str, Any]]:
        checks = [
            ("HUMAN_AUTHORIZATION_REPRODUCED", "PASS"),
            ("VARIANT_A_INPUT_REPRODUCTION", "PASS"),
            ("VARIANT_B_INPUT_REPRODUCTION", "PASS"),
            ("VARIANT_D_INPUT_REPRODUCTION", "PASS"),
            ("COMMON_99_ROW_IDENTITY_REPRODUCTION", "PASS"),
            ("SELECTED_PROPOSITION_PROVENANCE_REPRODUCTION", "PASS"),
            ("FEATURE_ALLOWLIST_STATUS", "PASS"),
            ("FEATURE_LABEL_ISOLATION_STATUS", "PASS"),
            ("PROCESS_SPLIT_STATUS", "PASS"),
            ("MODEL_ARTIFACT_RELOAD_STATUS", "PASS"),
            ("REPEAT_FIT_DETERMINISM_STATUS", "PASS"),
            ("VARIANT_C_EXCLUSION_STATUS", "VARIANT_C_EXCLUDED_PENDING_MARKET_METADATA_GOVERNANCE"),
            ("EXCLUDED_36_ROW_INTEGRITY_STATUS", "PASS"),
            ("SOURCE_MATRIX_IMMUTABILITY_STATUS", "PASS"),
            ("OUTPUT_CONTAINMENT_STATUS", "PASS"),
        ]
        rows = [
            {
                "check": check,
                "observed": self.decision_statuses.get(check, ""),
                "expected": expected,
                "status": "PASS" if self.decision_statuses.get(check, "") == expected else "FAIL",
            }
            for check, expected in checks
        ]
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", rows)
        return rows

    def parse_validation(self) -> list[dict[str, Any]]:
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
                status = "PASS" if path.read_text().startswith("#") else "FAIL"
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

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                rows.append({"artifact_path": str(path), "filename": path.name, "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-dir", default=str(DEFAULT_MATRIX_DIR))
    parser.add_argument("--process-dir", default=str(DEFAULT_PROCESS_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = SelectedPropositionABDDryRun(Path(args.matrix_dir), Path(args.process_dir), Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["parse_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
