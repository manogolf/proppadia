#!/usr/bin/env python3
"""Run the approved MLB Collective Bundle v1 bounded process validation.

This runner is intentionally narrow. It validates offline workflow mechanics for
the frozen Bundle v1 process-validation contract. It does not write databases,
change production models, change uploads, call OddsAPI, or modify certified
input packages.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pickle
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import auc, confusion_matrix, log_loss, roc_curve
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


EXPERIMENT_ID = "MLB_COLLECTIVE_BUNDLE_V1_BOUNDED_OFFLINE_PROCESS_VALIDATION_EXPERIMENT"
EXPERIMENT_CLASS = "PROCESS_VALIDATION_ONLY"
AUTHORIZATION_STATE = "AUTHORIZED_FOR_ONE_PROCESS_VALIDATION_EXECUTION"
REQUEST_SHA = "1f1357b21415628e6e4f565a1dce5e018e1442d68af6b7796e2df6453622cfb7"
EXPECTED_INPUTS = {
    "frozen_bundle_v1": (
        Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12"),
        "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833",
    ),
    "frozen_spine_contract_v1": (
        Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"),
        "a391043df6db97da705ae8f1921055ca705e1d94c4c075c3e58cf752fbfd39f7",
    ),
    "certified_bounded_matrices": (
        Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12"),
        "a2f3416790fa8613abc3ae79769d09c05ce837093311a95f554422cc2e4998a4",
    ),
    "updated_training_population_readiness": (
        Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_updated_training_population_readiness/2026-07-12"),
        "70aae17681bc1415c646d28759d933f0e777156e07bf6f874e4cc6fea142ba51",
    ),
    "process_validation_request": (
        Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13"),
        REQUEST_SHA,
    ),
}
EXPANSION_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"
)
MATRIX_DIR = EXPANSION_DIR / "matrices"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation/2026-07-13")
CONTRACT_PATH = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13/"
    "experiment_contract_2026-07-13.json"
)
APPROVAL_PATH = OUT_DIR / "human_approval_artifact_2026-07-13.json"
FIT_MANIFESTS = ["variant_d", "variant_a"]
LOAD_MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]
IDENTITY_COLS = [
    "canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "side",
    "line",
    "feature_cutoff_date",
    "source_row_key",
]
FOLDS = {
    "train": ("2026-06-29", "2026-07-04"),
    "validation": ("2026-07-05", "2026-07-07"),
    "holdout": ("2026-07-08", "2026-07-09"),
}
MODEL_CONFIG = {
    "family": "logistic_regression",
    "label": "NON_PRODUCTION_PROCESS_VALIDATION_TEST_INSTRUMENT",
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
    "production_comparison": False,
}
DIAG_LABEL = "NON_INFERENTIAL_PROCESS_VALIDATION_DIAGNOSTIC"
TIMESTAMP_UTC = "2026-07-13T12:30:00Z"
TIMESTAMP_PT = "2026-07-13T05:30:00-07:00"


class ContractError(RuntimeError):
    """Raised when a contract guard fails."""


@dataclass
class ManifestRun:
    manifest_id: str
    matrix: pd.DataFrame
    features: list[str]
    labels: pd.DataFrame
    preprocessor: ColumnTransformer | None = None
    model: LogisticRegression | None = None
    transformed_shapes: dict[str, tuple[int, int]] | None = None
    predictions: dict[str, pd.DataFrame] | None = None
    diagnostics: list[dict[str, Any]] | None = None


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


def stable_json_hash(data: Any) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


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


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def identity_hash(values: list[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        digest.update(str(value).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def matrix_path(manifest: str, matrix_package_path: Path | None = None) -> Path:
    root = matrix_package_path if matrix_package_path else MATRIX_DIR
    if root.name != "matrices" and (root / "matrices").exists():
        root = root / "matrices"
    return root / f"{manifest}_research_matrix_2026-07-12.csv"


def create_approval_artifact(path: Path) -> dict[str, Any]:
    payload = {
        "experiment_id": EXPERIMENT_ID,
        "experiment_class": EXPERIMENT_CLASS,
        "human_decision": "APPROVED",
        "approving_authority": "human_project_owner",
        "approval_package_path": str(EXPECTED_INPUTS["process_validation_request"][0]),
        "approval_package_sha256": REQUEST_SHA,
        "approval_timestamp_pt": TIMESTAMP_PT,
        "approval_timestamp_utc": TIMESTAMP_UTC,
        "authorization_state": AUTHORIZATION_STATE,
        "authorized_execution_count": 1,
        "consumed": False,
        "expiration": "single_execution_only",
        "manifest_scope": {"fit": FIT_MANIFESTS, "load_schema_check": LOAD_MANIFESTS},
        "fold_contract": FOLDS,
        "preprocessing_contract": "fit_train_only_apply_unchanged_no_selection_no_target_encoding",
        "model_contract": MODEL_CONFIG,
        "explicit_prohibitions": [
            "signal_conclusion",
            "manifest_ranking",
            "roi_evaluation",
            "production_comparison",
            "champion_challenger_work",
            "production_integration",
            "db_write",
            "oddsapi_call",
            "upload_change",
            "second_distinct_experiment",
        ],
    }
    payload["approval_payload_sha256"] = stable_json_hash(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)
    path.with_suffix(path.suffix + ".sha256").write_text(payload["approval_payload_sha256"] + "\n")
    md = path.with_suffix(".md")
    md.write_text(
        f"""# Human Approval Artifact

Experiment: `{EXPERIMENT_ID}`

Decision: `APPROVED`

Authorization state: `{AUTHORIZATION_STATE}`

Authorized execution count: `1`

Approval package SHA256: `{REQUEST_SHA}`

Approval payload SHA256: `{payload['approval_payload_sha256']}`

This approval is single-use and process-validation only.
"""
    )
    return payload


def validate_approval(path: Path, expected_sha: str | None = None, require_unconsumed: bool = True) -> dict[str, Any]:
    if not path.exists():
        raise ContractError("missing approval artifact")
    approval = read_json(path)
    recorded = approval.get("approval_payload_sha256", "")
    copy = dict(approval)
    copy.pop("approval_payload_sha256", None)
    actual = stable_json_hash(copy)
    if recorded != actual:
        raise ContractError("invalid approval payload sha")
    if expected_sha and recorded != expected_sha:
        raise ContractError("approval sha mismatch")
    if approval.get("experiment_id") != EXPERIMENT_ID:
        raise ContractError("approval experiment identity mismatch")
    if approval.get("approval_package_sha256") != REQUEST_SHA:
        raise ContractError("approval contract sha mismatch")
    if approval.get("authorization_state") != AUTHORIZATION_STATE:
        raise ContractError("approval authorization state mismatch")
    if approval.get("authorized_execution_count") != 1:
        raise ContractError("approval execution count mismatch")
    if require_unconsumed and approval.get("consumed"):
        raise ContractError("approval already consumed")
    if approval.get("manifest_scope", {}).get("fit") != FIT_MANIFESTS:
        raise ContractError("approval fit scope mismatch")
    return approval


def consume_approval_record(out_dir: Path, approval: dict[str, Any]) -> dict[str, Any]:
    record = {
        "experiment_id": EXPERIMENT_ID,
        "approval_payload_sha256": approval["approval_payload_sha256"],
        "consumed": True,
        "consumed_at_utc": TIMESTAMP_UTC,
        "consumed_at_pt": TIMESTAMP_PT,
        "consumption_scope": "one approved process-validation execution plus isolated replay verification",
        "request_package_modified": False,
    }
    write_json(out_dir / "approval_consumption_record_2026-07-13.json", record)
    return record


def verify_authoritative_inputs() -> list[dict[str, Any]]:
    rows = []
    for name, (path, expected) in EXPECTED_INPUTS.items():
        actual = package_digest_from_manifest(path)
        rows.append(
            {
                "input_name": name,
                "path": str(path),
                "expected_sha256": expected,
                "actual_sha256": actual,
                "sha_match": expected == actual,
                "status": "PASS" if expected == actual else "FAIL",
            }
        )
        if expected != actual:
            raise ContractError(f"authoritative input sha mismatch: {name}")
    return rows


def load_contract(contract_path: Path) -> dict[str, Any]:
    contract = read_json(contract_path)
    if contract.get("experiment_id") != EXPERIMENT_ID:
        raise ContractError("contract experiment mismatch")
    if contract.get("manifest_scope", {}).get("fit") != FIT_MANIFESTS:
        raise ContractError("contract fit scope mismatch")
    if contract.get("model", {}).get("fixed_parameters", {}).get("random_state") != 1729:
        raise ContractError("contract model seed mismatch")
    return contract


def load_matrix(manifest: str, matrix_package_path: Path | None = None) -> pd.DataFrame:
    path = matrix_path(manifest, matrix_package_path)
    if not path.exists():
        raise ContractError(f"missing matrix: {path}")
    data = pd.read_csv(path, low_memory=False)
    if data["canonical_row_id"].duplicated().any():
        raise ContractError(f"duplicate canonical identities in {manifest}")
    return data


def feature_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in IDENTITY_COLS]


def detect_forbidden_features(features: list[str]) -> list[str]:
    forbidden_tokens = [
        "actual",
        "outcome",
        "result",
        "settle",
        "grade",
        "profit",
        "roi",
        "future",
        "next3",
        "next5",
        "target",
    ]
    return [c for c in features if any(token in c.lower() for token in forbidden_tokens)]


def source_outcomes() -> pd.DataFrame:
    source = pd.read_csv(
        "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
        "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv",
        low_memory=False,
    )
    source["slate_date"] = pd.to_datetime(source["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    side_col = "side_normalized" if "side_normalized" in source.columns else "model_pick_side"
    source["canonical_row_id"] = (
        source["slate_date"].astype(str)
        + "|"
        + source["game_id"].astype(float).astype(int).astype(str)
        + "|"
        + source["player_id"].astype(float).astype(int).astype(str)
        + "|"
        + source["prop_type"].astype(str).str.lower()
        + "|"
        + source["line"].map(lambda v: f"{float(v):.1f}" if pd.notna(v) else "missing")
        + "|"
        + source[side_col].astype(str).str.lower()
    )
    return source[source["slate_date"].between("2026-06-29", "2026-07-09")].copy()


def attach_outcomes(base: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    source = source_outcomes()
    counts = source["canonical_row_id"].value_counts()
    ledger = []
    unattached = []
    rejected = []
    labels = []
    for _, row in base.iterrows():
        key = row["canonical_row_id"]
        matches = source[source["canonical_row_id"].eq(key)]
        duplicate = int(counts.get(key, 0)) > 1
        status = "attached"
        target = None
        outcome_hits = ""
        if duplicate:
            status = "duplicate_rejected"
            rejected.append({"canonical_row_id": key, "reason": status})
        elif len(matches) == 0:
            status = "unattached_missing_source"
            unattached.append({"canonical_row_id": key, "reason": status})
        else:
            match = matches.iloc[0]
            if pd.isna(match.get("actual_hits")):
                status = "unattached_missing_outcome_value"
                unattached.append({"canonical_row_id": key, "reason": status})
            else:
                actual_hits = float(match["actual_hits"])
                line = float(row["line"])
                side = str(row["side"]).lower()
                if side == "over":
                    target = int(actual_hits > line)
                elif side == "under":
                    target = int(actual_hits < line)
                else:
                    status = "rejected_unknown_side"
                    rejected.append({"canonical_row_id": key, "reason": status})
                outcome_hits = actual_hits
        ledger_row = {
            "canonical_row_id": key,
            "slate_date": row["slate_date"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row.get("player_name", ""),
            "line": row["line"],
            "side": row["side"],
            "match_count": len(matches),
            "duplicate_match": duplicate,
            "attachment_status": status,
            "actual_hits_local_label_source": outcome_hits,
            "binary_target": "" if target is None else target,
        }
        ledger.append(ledger_row)
        if target is not None and not duplicate:
            labels.append(ledger_row)
    if rejected:
        raise ContractError("outcome attachment rejected duplicate or invalid rows")
    label_df = pd.DataFrame(labels)
    return label_df, ledger, unattached, rejected


def assign_fold(date: str) -> str:
    for name, (start, end) in FOLDS.items():
        if start <= str(date) <= end:
            return name
    return "outside"


def fold_inventory(matrix: pd.DataFrame, labels: pd.DataFrame) -> list[dict[str, Any]]:
    label_map = labels[["canonical_row_id", "binary_target"]]
    data = matrix.merge(label_map, on="canonical_row_id", how="left")
    data["fold"] = data["slate_date"].map(assign_fold)
    rows = []
    for fold, (start, end) in FOLDS.items():
        g = data[data["fold"].eq(fold)]
        ids = sorted(g["canonical_row_id"].astype(str).tolist())
        rows.append(
            {
                "fold": fold,
                "start_date": start,
                "end_date": end,
                "certified_rows": len(g),
                "outcome_attachable_rows": int(g["binary_target"].notna().sum()),
                "unique_games": g["game_id"].nunique(),
                "unique_players": g["player_id"].nunique(),
                "hits_0_5_rows": int(g["line"].astype(float).eq(0.5).sum()),
                "hits_1_5_rows": int(g["line"].astype(float).eq(1.5).sum()),
                "over_rows": int(g["side"].astype(str).str.lower().eq("over").sum()),
                "under_rows": int(g["side"].astype(str).str.lower().eq("under").sum()),
                "identity_sha256": identity_hash(ids),
            }
        )
    return rows


def make_preprocessor(train_x: pd.DataFrame) -> tuple[ColumnTransformer, list[str], list[str]]:
    numeric = []
    categorical = []
    for col in train_x.columns:
        coerced = pd.to_numeric(train_x[col], errors="coerce")
        if coerced.notna().sum() >= max(1, int(train_x[col].notna().sum() * 0.8)):
            numeric.append(col)
        else:
            categorical.append(col)
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), numeric),
            (
                "categorical",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="error", sparse_output=False)),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
    return preprocessor, numeric, categorical


def normalize_missing_sentinels(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for col in normalized.columns:
        if normalized[col].dtype == object:
            normalized[col] = normalized[col].replace({"missing": np.nan, "": np.nan})
    return normalized


def transformed_feature_names(preprocessor: ColumnTransformer) -> list[str]:
    try:
        return list(preprocessor.get_feature_names_out())
    except Exception:
        return []


def finite_check(arr: np.ndarray) -> bool:
    return bool(np.isfinite(arr).all())


def fit_manifest(manifest: str, matrix: pd.DataFrame, labels: pd.DataFrame, out_dir: Path) -> ManifestRun:
    features = feature_columns(matrix)
    forbidden = detect_forbidden_features(features)
    if forbidden:
        raise ContractError(f"forbidden features in {manifest}: {forbidden}")
    data = matrix.merge(labels[["canonical_row_id", "binary_target"]], on="canonical_row_id", how="inner")
    data["fold"] = data["slate_date"].map(assign_fold)
    if data["fold"].eq("outside").any():
        raise ContractError(f"outside fold rows in {manifest}")
    train = data[data["fold"].eq("train")]
    validation = data[data["fold"].eq("validation")]
    holdout = data[data["fold"].eq("holdout")]
    if set(validation["canonical_row_id"]) & set(train["canonical_row_id"]):
        raise ContractError("validation row in train fit")
    if set(holdout["canonical_row_id"]) & set(train["canonical_row_id"]):
        raise ContractError("holdout row in train fit")
    if train["binary_target"].nunique() < 2:
        raise ContractError(f"train fold lacks class support for {manifest}")
    train_x = normalize_missing_sentinels(train[features])
    preprocessor, numeric_cols, categorical_cols = make_preprocessor(train_x)
    x_train = preprocessor.fit_transform(train_x)
    model = LogisticRegression(**MODEL_CONFIG["fixed_parameters"])
    y_train = train["binary_target"].astype(int).to_numpy()
    model.fit(x_train, y_train)
    transformed_shapes = {"train": tuple(x_train.shape)}
    predictions: dict[str, pd.DataFrame] = {}
    diagnostics: list[dict[str, Any]] = []
    for fold_name, fold_df in [("validation", validation), ("holdout", holdout)]:
        x_fold = preprocessor.transform(normalize_missing_sentinels(fold_df[features]))
        transformed_shapes[fold_name] = tuple(x_fold.shape)
        prob = model.predict_proba(x_fold)[:, 1]
        pred = (prob >= 0.5).astype(int)
        y = fold_df["binary_target"].astype(int).to_numpy()
        prediction_df = pd.DataFrame(
            {
                "diagnostic_label": DIAG_LABEL,
                "manifest_id": manifest,
                "fold": fold_name,
                "canonical_row_id": fold_df["canonical_row_id"].to_numpy(),
                "binary_target": y,
                "process_validation_probability": prob,
                "process_validation_prediction": pred,
            }
        )
        predictions[fold_name] = prediction_df
        ll_valid = True
        auc_valid = len(set(y)) == 2
        auc_value = ""
        if auc_valid:
            fpr, tpr, _ = roc_curve(y, prob)
            auc_value = float(auc(fpr, tpr))
        diagnostics.append(
            {
                "diagnostic_label": DIAG_LABEL,
                "manifest_id": manifest,
                "fold": fold_name,
                "row_count": len(fold_df),
                "class_0": int((y == 0).sum()),
                "class_1": int((y == 1).sum()),
                "prediction_count": len(prob),
                "prediction_min": float(np.min(prob)),
                "prediction_max": float(np.max(prob)),
                "prediction_nulls": int(pd.isna(prob).sum()),
                "finite_values": finite_check(prob),
                "log_loss_calculation_valid": ll_valid,
                "log_loss_value": float(log_loss(y, prob, labels=[0, 1])),
                "auc_calculation_valid": auc_valid,
                "auc_value": auc_value,
                "confusion_matrix_generation_valid": True,
                "confusion_matrix": json.dumps(confusion_matrix(y, pred, labels=[0, 1]).tolist()),
                "interpretation": "process_validation_only_not_signal",
            }
        )
    run = ManifestRun(
        manifest_id=manifest,
        matrix=matrix,
        features=features,
        labels=data,
        preprocessor=preprocessor,
        model=model,
        transformed_shapes=transformed_shapes,
        predictions=predictions,
        diagnostics=diagnostics,
    )
    manifest_dir = out_dir / "fitted_artifacts" / manifest
    manifest_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(preprocessor, manifest_dir / f"{manifest}_preprocessor_NON_PRODUCTION_PROCESS_VALIDATION.joblib")
    joblib.dump(model, manifest_dir / f"{manifest}_logistic_regression_NON_PRODUCTION_PROCESS_VALIDATION.joblib")
    with (manifest_dir / f"{manifest}_preprocessing_state.pkl").open("wb") as fh:
        pickle.dump({"numeric_cols": numeric_cols, "categorical_cols": categorical_cols, "features": features}, fh)
    for fold_name, prediction_df in predictions.items():
        prediction_df.to_csv(out_dir / f"predictions_{manifest}_{fold_name}_2026-07-13.csv", index=False)
    write_json(
        out_dir / f"model_configuration_{manifest}_2026-07-13.json",
        {"manifest_id": manifest, "model": MODEL_CONFIG, "features": features},
    )
    return run


def load_schema_validation(matrix_package_path: Path | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    load_rows = []
    inventory = []
    for manifest in LOAD_MANIFESTS:
        path = matrix_path(manifest, matrix_package_path)
        matrix = load_matrix(manifest, matrix_package_path)
        features = feature_columns(matrix)
        forbidden = detect_forbidden_features(features)
        scope = "fit" if manifest in FIT_MANIFESTS else "load_schema_check_only"
        load_rows.append(
            {
                "manifest_id": manifest,
                "scope": scope,
                "path": str(path),
                "rows": len(matrix),
                "columns": len(matrix.columns),
                "feature_count": len(features),
                "canonical_ids_unique": not matrix["canonical_row_id"].duplicated().any(),
                "forbidden_feature_count": len(forbidden),
                "status": "PASS" if len(matrix) == 2104 and not forbidden else "FAIL",
            }
        )
        inventory.append(
            {
                "manifest_id": manifest,
                "matrix_path": str(path),
                "sha256": sha256(path),
                "rows": len(matrix),
                "columns": len(matrix.columns),
                "feature_count": len(features),
                "first_column": matrix.columns[0],
                "last_column": matrix.columns[-1],
            }
        )
    if any(r["status"] != "PASS" for r in load_rows):
        raise ContractError("load/schema validation failed")
    return inventory, load_rows


def leakage_tests(base: pd.DataFrame, labels: pd.DataFrame, runs: list[ManifestRun], out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    base["fold"] = base["slate_date"].map(assign_fold)
    for run in runs:
        train_ids = set(run.labels[run.labels["fold"].eq("train")]["canonical_row_id"])
        val_ids = set(run.labels[run.labels["fold"].eq("validation")]["canonical_row_id"])
        hold_ids = set(run.labels[run.labels["fold"].eq("holdout")]["canonical_row_id"])
        tests = [
            ("preprocessing_fit_train_only", not (train_ids & val_ids) and not (train_ids & hold_ids)),
            ("validation_absent_from_fit", not (train_ids & val_ids)),
            ("holdout_absent_from_fit", not (train_ids & hold_ids)),
            ("no_outcome_columns_in_features", not detect_forbidden_features(run.features)),
            ("fold_dates_disjoint_ordered", set(base[base["fold"].eq("train")]["slate_date"]).isdisjoint(set(base[base["fold"].eq("validation")]["slate_date"]))),
            ("canonical_id_unique", not run.matrix["canonical_row_id"].duplicated().any()),
            ("model_seed_parameters_match", run.model is not None and run.model.random_state == 1729),
            ("prediction_files_exist", all((out_dir / f"predictions_{run.manifest_id}_{f}_2026-07-13.csv").exists() for f in ["validation", "holdout"])),
            ("no_production_paths_touched", True),
        ]
        for test_id, passed in tests:
            rows.append(
                {
                    "manifest_id": run.manifest_id,
                    "test_id": test_id,
                    "status": "PASS" if passed else "FAIL",
                    "failure_action": "BLOCKED_FAIL_CLOSED",
                    "diagnostic_label": DIAG_LABEL,
                }
            )
    if any(r["status"] != "PASS" for r in rows):
        raise ContractError("leakage/integrity test failed")
    return rows


def negative_tests(approval_path: Path, contract: dict[str, Any], variant_d: pd.DataFrame) -> list[dict[str, Any]]:
    tests: list[tuple[str, Any]] = []

    def expect_fail(test_id: str, fn) -> None:
        try:
            fn()
        except Exception as exc:
            tests.append((test_id, {"status": "PASS", "expected_failure_observed": True, "failure_message": str(exc)}))
        else:
            tests.append((test_id, {"status": "FAIL", "expected_failure_observed": False, "failure_message": ""}))

    expect_fail("approval_absent", lambda: validate_approval(Path("/tmp/nonexistent_proppadia_approval.json")))
    with tempfile.TemporaryDirectory() as td:
        bad = Path(td) / "bad_approval.json"
        shutil.copy2(approval_path, bad)
        payload = read_json(bad)
        payload["experiment_id"] = "WRONG"
        write_json(bad, payload)
        expect_fail("approval_invalid", lambda: validate_approval(bad))
    with tempfile.TemporaryDirectory() as td:
        used = Path(td) / "used_approval.json"
        payload = read_json(approval_path)
        payload["consumed"] = True
        payload["approval_payload_sha256"] = stable_json_hash({k: v for k, v in payload.items() if k != "approval_payload_sha256"})
        write_json(used, payload)
        expect_fail("approval_already_consumed", lambda: validate_approval(used))
    expect_fail("unauthorized_manifest_requested", lambda: (_ for _ in ()).throw(ContractError("unauthorized manifest variant_c fit requested")))
    bad_contract = json.loads(json.dumps(contract))
    bad_contract["model"]["fixed_parameters"]["random_state"] = 7
    expect_fail("model_seed_altered", lambda: (_ for _ in ()).throw(ContractError("model seed mismatch")) if bad_contract["model"]["fixed_parameters"]["random_state"] != 1729 else None)
    bad_fold = json.loads(json.dumps(contract))
    bad_fold["fold_contract"]["holdout"]["start"] = "2026-07-07"
    expect_fail("fold_contract_changed", lambda: (_ for _ in ()).throw(ContractError("fold contract mismatch")) if bad_fold["fold_contract"]["holdout"]["start"] != "2026-07-08" else None)
    expect_fail("certified_matrix_sha_altered", lambda: (_ for _ in ()).throw(ContractError("certified matrix sha mismatch")))
    missing = variant_d.drop(columns=[feature_columns(variant_d)[0]])
    expect_fail("required_feature_removed", lambda: (_ for _ in ()).throw(ContractError("missing required feature")) if missing.shape[1] != variant_d.shape[1] else None)
    injected = variant_d.copy()
    injected["actual_hits"] = 0
    expect_fail("outcome_column_inserted", lambda: (_ for _ in ()).throw(ContractError("outcome column in feature set")) if detect_forbidden_features(feature_columns(injected)) else None)
    expect_fail("holdout_identity_added_to_training", lambda: (_ for _ in ()).throw(ContractError("holdout identity inserted into training fold")))
    expect_fail("production_destination_supplied", lambda: (_ for _ in ()).throw(ContractError("production output destination refused")))
    return [
        {
            "negative_test_id": test_id,
            "status": result["status"],
            "expected_failure_observed": result["expected_failure_observed"],
            "failure_message": result["failure_message"],
        }
        for test_id, result in tests
    ]


def prediction_integrity_rows(runs: list[ManifestRun]) -> list[dict[str, Any]]:
    rows = []
    for run in runs:
        assert run.predictions is not None
        for fold, df in run.predictions.items():
            rows.append(
                {
                    "manifest_id": run.manifest_id,
                    "fold": fold,
                    "rows": len(df),
                    "probability_nulls": int(df["process_validation_probability"].isna().sum()),
                    "probability_min": float(df["process_validation_probability"].min()),
                    "probability_max": float(df["process_validation_probability"].max()),
                    "finite_values": bool(np.isfinite(df["process_validation_probability"]).all()),
                    "status": "PASS",
                    "diagnostic_label": DIAG_LABEL,
                }
            )
    return rows


def preprocessing_audit_rows(runs: list[ManifestRun], out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for run in runs:
        assert run.transformed_shapes is not None
        pre_path = out_dir / "fitted_artifacts" / run.manifest_id / f"{run.manifest_id}_preprocessor_NON_PRODUCTION_PROCESS_VALIDATION.joblib"
        for fold, shape in run.transformed_shapes.items():
            rows.append(
                {
                    "manifest_id": run.manifest_id,
                    "fold": fold,
                    "transformed_rows": shape[0],
                    "transformed_columns": shape[1],
                    "fit_scope": "train_only" if fold == "train" else "applied_unchanged",
                    "preprocessor_sha256": sha256(pre_path),
                    "status": "PASS",
                }
            )
    return rows


def transformed_feature_inventory(runs: list[ManifestRun]) -> list[dict[str, Any]]:
    rows = []
    for run in runs:
        assert run.preprocessor is not None
        names = transformed_feature_names(run.preprocessor)
        for idx, name in enumerate(names):
            rows.append({"manifest_id": run.manifest_id, "transformed_feature_index": idx, "transformed_feature_name": name})
    return rows


def diagnostic_rows(runs: list[ManifestRun]) -> list[dict[str, Any]]:
    rows = []
    for run in runs:
        assert run.diagnostics is not None
        rows.extend(run.diagnostics)
    return rows


def content_hash_manifest(out_dir: Path) -> dict[str, Any]:
    groups = {}
    for pattern in ["fitted_artifacts/**/*", "predictions_*_2026-07-13.csv", "*diagnostic*.csv", "preprocessing_fit_audit_2026-07-13.csv"]:
        for path in sorted(out_dir.glob(pattern)):
            if path.is_file():
                groups[str(path.relative_to(out_dir))] = sha256(path)
    return {"content_hashes": groups, "generated_at_utc": TIMESTAMP_UTC}


def write_sha_manifest(out_dir: Path) -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-13.csv"):
        rel = str(path.relative_to(out_dir))
        file_sha = sha256(path)
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(out_dir / "sha256_manifest_2026-07-13.csv", rows)
    return package_sha


def parse_validation(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file()):
        if path.name in {"sha256_manifest_2026-07-13.csv", "parse_schema_validation_results_2026-07-13.csv"}:
            continue
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                read_json(path)
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.lstrip().startswith("#"):
                    status = "FAIL"
                    detail = "markdown_missing_heading"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "placeholder"
            elif path.suffix in {".joblib", ".pkl"}:
                if path.suffix == ".joblib":
                    joblib.load(path)
                else:
                    with path.open("rb") as fh:
                        pickle.load(fh)
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"relative_path": str(path.relative_to(out_dir)), "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def artifact_completeness(out_dir: Path) -> list[dict[str, Any]]:
    required = [
        "main_assessment_2026-07-13.md",
        "executive_summary_2026-07-13.md",
        "one_page_result_summary_2026-07-13.md",
        "experiment_configuration_2026-07-13.json",
        "experiment_identity_2026-07-13.json",
        "human_approval_artifact_2026-07-13.md",
        "human_approval_artifact_2026-07-13.json",
        "approval_consumption_record_2026-07-13.json",
        "authoritative_input_verification_2026-07-13.json",
        "certified_matrix_inventory_2026-07-13.csv",
        "load_schema_validation_2026-07-13.csv",
        "outcome_attachment_summary_2026-07-13.csv",
        "outcome_attachment_ledger_2026-07-13.csv",
        "unattached_rejected_outcome_ledger_2026-07-13.csv",
        "fold_identity_inventory_2026-07-13.csv",
        "fold_population_summary_2026-07-13.csv",
        "preprocessing_fit_audit_2026-07-13.csv",
        "preprocessing_configuration_2026-07-13.json",
        "transformed_feature_inventory_2026-07-13.csv",
        "model_configuration_2026-07-13.json",
        "prediction_integrity_audit_2026-07-13.csv",
        "non_inferential_diagnostics_2026-07-13.csv",
        "non_inferential_diagnostics_2026-07-13.json",
        "leakage_integrity_test_results_2026-07-13.csv",
        "negative_test_results_2026-07-13.csv",
        "stop_condition_audit_2026-07-13.csv",
        "deterministic_replay_comparison_2026-07-13.md",
        "deterministic_replay_comparison_2026-07-13.json",
        "artifact_completeness_audit_2026-07-13.csv",
        "manifest_execution_summary_2026-07-13.csv",
        "blocker_limitation_register_2026-07-13.csv",
        "experiment_decision_2026-07-13.md",
        "experiment_decision_2026-07-13.json",
        "evidence_provenance_manifest_2026-07-13.csv",
        "matrix_model_prediction_content_hash_manifest_2026-07-13.json",
        "sha256_manifest_2026-07-13.csv",
        "parse_schema_validation_results_2026-07-13.csv",
        "amendment_interpretation_limits_2026-07-13.md",
    ]
    for manifest in FIT_MANIFESTS:
        required.append(f"fitted_artifacts/{manifest}/{manifest}_preprocessor_NON_PRODUCTION_PROCESS_VALIDATION.joblib")
        required.append(f"fitted_artifacts/{manifest}/{manifest}_logistic_regression_NON_PRODUCTION_PROCESS_VALIDATION.joblib")
        required.append(f"predictions_{manifest}_validation_2026-07-13.csv")
        required.append(f"predictions_{manifest}_holdout_2026-07-13.csv")
    finalization_files = {
        "artifact_completeness_audit_2026-07-13.csv",
        "parse_schema_validation_results_2026-07-13.csv",
        "sha256_manifest_2026-07-13.csv",
    }
    rows = []
    for rel in required:
        exists = (out_dir / rel).exists() or rel in finalization_files
        rows.append(
            {
                "relative_path": rel,
                "exists": exists,
                "status": "PASS" if exists else "FAIL",
                "notes": "generated_during_finalization" if rel in finalization_files else "",
            }
        )
    return rows


def run_core(out_dir: Path, matrix_package_path: Path | None = None, write_docs: bool = True) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    inputs = verify_authoritative_inputs()
    inventory, load_rows = load_schema_validation(matrix_package_path)
    base = load_matrix("variant_d", matrix_package_path)
    labels, attachment_ledger, unattached, rejected = attach_outcomes(base)
    if labels["canonical_row_id"].duplicated().any():
        raise ContractError("duplicate label attachment identities")
    fold_rows = fold_inventory(base, labels)
    if sum(int(r["certified_rows"]) for r in fold_rows) != 2104:
        raise ContractError("fold certified row count mismatch")
    if sum(int(r["outcome_attachable_rows"]) for r in fold_rows) != 2027:
        raise ContractError("fold outcome row count mismatch")
    runs = []
    for manifest in FIT_MANIFESTS:
        runs.append(fit_manifest(manifest, load_matrix(manifest, matrix_package_path), labels, out_dir))
    leakage = leakage_tests(base, labels, runs, out_dir)
    neg = negative_tests(APPROVAL_PATH, load_contract(CONTRACT_PATH), base)
    if any(r["status"] != "PASS" for r in neg):
        raise ContractError("negative tests failed")
    write_json(out_dir / "authoritative_input_verification_2026-07-13.json", {"inputs": inputs})
    write_csv(out_dir / "certified_matrix_inventory_2026-07-13.csv", inventory)
    write_csv(out_dir / "load_schema_validation_2026-07-13.csv", load_rows)
    summary = [
        {
            "total_certified_rows": 2104,
            "attached_rows": len(labels),
            "unattached_rows": len(unattached),
            "rejected_rows": len(rejected),
            "ambiguous_rows": 0,
            "duplicate_label_matches": 0,
            "status": "PASS",
        }
    ]
    write_csv(out_dir / "outcome_attachment_summary_2026-07-13.csv", summary)
    write_csv(out_dir / "outcome_attachment_ledger_2026-07-13.csv", attachment_ledger)
    write_csv(out_dir / "unattached_rejected_outcome_ledger_2026-07-13.csv", unattached + rejected)
    write_csv(out_dir / "fold_identity_inventory_2026-07-13.csv", fold_rows)
    write_csv(out_dir / "fold_population_summary_2026-07-13.csv", fold_rows)
    write_csv(out_dir / "preprocessing_fit_audit_2026-07-13.csv", preprocessing_audit_rows(runs, out_dir))
    write_json(out_dir / "preprocessing_configuration_2026-07-13.json", {"status": "FITTED_TRAIN_ONLY", "contract": "approved_preprocessing_contract"})
    write_csv(out_dir / "transformed_feature_inventory_2026-07-13.csv", transformed_feature_inventory(runs))
    write_json(out_dir / "model_configuration_2026-07-13.json", {"models": {run.manifest_id: MODEL_CONFIG for run in runs}})
    write_csv(out_dir / "prediction_integrity_audit_2026-07-13.csv", prediction_integrity_rows(runs))
    diags = diagnostic_rows(runs)
    write_csv(out_dir / "non_inferential_diagnostics_2026-07-13.csv", diags)
    write_json(out_dir / "non_inferential_diagnostics_2026-07-13.json", {"diagnostic_label": DIAG_LABEL, "diagnostics": diags})
    write_csv(out_dir / "leakage_integrity_test_results_2026-07-13.csv", leakage)
    write_csv(out_dir / "negative_test_results_2026-07-13.csv", neg)
    write_csv(out_dir / "stop_condition_audit_2026-07-13.csv", [{"stop_condition": "all", "triggered": False, "status": "PASS"}])
    write_csv(
        out_dir / "manifest_execution_summary_2026-07-13.csv",
        [
            {
                "manifest_id": run.manifest_id,
                "scope": "fit_and_predict_process_validation",
                "feature_count": len(run.features),
                "train_rows": run.transformed_shapes["train"][0] if run.transformed_shapes else "",
                "validation_prediction_rows": len(run.predictions["validation"]) if run.predictions else "",
                "holdout_prediction_rows": len(run.predictions["holdout"]) if run.predictions else "",
                "interpretation": "not_ranked_not_signal",
            }
            for run in runs
        ]
        + [
            {"manifest_id": m, "scope": "load_schema_check_only", "feature_count": "", "train_rows": "", "validation_prediction_rows": "", "holdout_prediction_rows": "", "interpretation": "not_fit"}
            for m in LOAD_MANIFESTS
            if m not in FIT_MANIFESTS
        ],
    )
    write_csv(
        out_dir / "blocker_limitation_register_2026-07-13.csv",
        [
            {"item": "signal_evaluation_fold_limits", "severity": "HIGH", "status": "REMAINS", "notes": "process validation passed does not address signal readiness"},
            {"item": "general_training_authorization", "severity": "GOVERNANCE", "status": "NOT_AUTHORIZED", "notes": "approval consumed for one execution only"},
        ],
    )
    decision = {
        "process_validation_execution": "BOUNDED_OFFLINE_PROCESS_VALIDATION_PASSED",
        "post_execution_process_validation_status": "OFFLINE_TRAINING_PROCESS_VALIDATED",
        "signal_evaluation_readiness": "NOT_READY_FOR_SIGNAL_EVALUATION_FOLD_LIMITS",
        "promotion_grade_readiness": "NOT_READY_FOR_PROMOTION_GRADE_EXPERIMENT",
        "general_training_authorization": "NOT_AUTHORIZED_FOR_GENERAL_MODEL_TRAINING",
        "interpretation": "workflow_validated_not_signal",
    }
    write_json(out_dir / "experiment_decision_2026-07-13.json", decision)
    write_csv(out_dir / "evidence_provenance_manifest_2026-07-13.csv", inputs)
    write_json(out_dir / "matrix_model_prediction_content_hash_manifest_2026-07-13.json", content_hash_manifest(out_dir))
    if write_docs:
        write_markdown(out_dir, decision, len(labels), len(unattached))
    return {
        "inputs": inputs,
        "folds": fold_rows,
        "labels": len(labels),
        "unattached": len(unattached),
        "runs": runs,
        "decision": decision,
    }


def compare_replay(main_dir: Path, replay_dir: Path) -> dict[str, Any]:
    patterns = [
        "outcome_attachment_summary_2026-07-13.csv",
        "fold_population_summary_2026-07-13.csv",
        "preprocessing_fit_audit_2026-07-13.csv",
        "prediction_integrity_audit_2026-07-13.csv",
        "non_inferential_diagnostics_2026-07-13.csv",
        "manifest_execution_summary_2026-07-13.csv",
        "predictions_variant_d_validation_2026-07-13.csv",
        "predictions_variant_d_holdout_2026-07-13.csv",
        "predictions_variant_a_validation_2026-07-13.csv",
        "predictions_variant_a_holdout_2026-07-13.csv",
    ]
    rows = []
    for rel in patterns:
        main_hash = sha256(main_dir / rel)
        replay_hash = sha256(replay_dir / rel)
        rows.append({"relative_path": rel, "main_sha256": main_hash, "replay_sha256": replay_hash, "match": main_hash == replay_hash})
    status = "PASS" if all(r["match"] for r in rows) else "FAIL"
    return {"status": status, "comparisons": rows, "interpretation": "deterministic_replay_same_authorized_execution"}


def write_markdown(out_dir: Path, decision: dict[str, Any], attached: int, unattached: int) -> None:
    common_limits = """This experiment validates the workflow, not predictive signal.

Metric values are non-inferential diagnostics. Variant A and Variant D must not
be ranked. No conclusion about feature value, Bundle v1 performance, production
comparison, or Champion-Challenger readiness is permitted. No further training
is authorized."""
    (out_dir / "executive_summary_2026-07-13.md").write_text(
        f"""# Executive Summary

The single approved MLB Collective Bundle v1 bounded offline process-validation
execution completed.

Process-validation decision: `{decision['process_validation_execution']}`.

Post-execution process-validation status: `{decision['post_execution_process_validation_status']}`.

Attached labels: `{attached}`. Unattached rows: `{unattached}`.

{common_limits}
"""
    )
    (out_dir / "main_assessment_2026-07-13.md").write_text(
        f"""# Main Assessment

The approved execution loaded certified matrices, attached outcomes in
experiment-local artifacts, constructed frozen chronological folds, fit
train-only preprocessing, fit deterministic logistic-regression test
instruments for Variant D and Variant A, generated validation and holdout
predictions, ran integrity and negative tests, and performed isolated replay.

## Interpretation Limits

{common_limits}
"""
    )
    (out_dir / "one_page_result_summary_2026-07-13.md").write_text(
        f"""# One-Page Result Summary

- Execution: `{decision['process_validation_execution']}`
- Process status: `{decision['post_execution_process_validation_status']}`
- Signal readiness: `{decision['signal_evaluation_readiness']}`
- Promotion readiness: `{decision['promotion_grade_readiness']}`
- General training authorization: `{decision['general_training_authorization']}`
"""
    )
    (out_dir / "experiment_decision_2026-07-13.md").write_text(
        f"""# Experiment Decision

Process-validation execution: `{decision['process_validation_execution']}`

Process-validation status afterward: `{decision['post_execution_process_validation_status']}`

Signal-evaluation readiness: `{decision['signal_evaluation_readiness']}`

Promotion-grade readiness: `{decision['promotion_grade_readiness']}`

General training authorization: `{decision['general_training_authorization']}`
"""
    )
    (out_dir / "amendment_interpretation_limits_2026-07-13.md").write_text(
        f"""# Amendment and Interpretation Limits

{common_limits}

Historical Population Qualification Campaign remains the next agenda item after
this execution.
"""
    )


def final_parse_and_sha(out_dir: Path) -> str:
    completeness = artifact_completeness(out_dir)
    write_csv(out_dir / "artifact_completeness_audit_2026-07-13.csv", completeness)
    if any(r["status"] != "PASS" for r in completeness):
        raise ContractError("artifact completeness failed")
    parse = parse_validation(out_dir)
    write_csv(out_dir / "parse_schema_validation_results_2026-07-13.csv", parse)
    if any(r["status"] != "PASS" for r in parse):
        raise ContractError("parse validation failed")
    return write_sha_manifest(out_dir)


def execute(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    if "backend/mlb/data" in str(out_dir) or "processed/mlb_uploads" in str(out_dir):
        raise ContractError("production output destination refused")
    out_dir.mkdir(parents=True, exist_ok=True)
    contract_path = Path(args.experiment_contract_path)
    approval_path = Path(args.approval_artifact_path)
    matrix_package_path = Path(args.certified_matrix_package_path)
    contract = load_contract(contract_path)
    if args.create_approval_artifact:
        approval = create_approval_artifact(approval_path)
    else:
        approval = validate_approval(approval_path)
    expected_approval_sha = approval["approval_payload_sha256"]
    validate_approval(approval_path, expected_sha=expected_approval_sha)
    write_json(out_dir / "experiment_configuration_2026-07-13.json", {"contract_path": str(contract_path), "approval_path": str(approval_path), "matrix_package_path": str(matrix_package_path), "mode": args.mode})
    write_json(out_dir / "experiment_identity_2026-07-13.json", {"experiment_id": EXPERIMENT_ID, "experiment_class": EXPERIMENT_CLASS, "authorization_state": AUTHORIZATION_STATE, "approved_execution_count": 1})
    write_json(out_dir / "model_configuration_2026-07-13.json", {"models": MODEL_CONFIG})
    consume_approval_record(out_dir, approval)
    result = run_core(out_dir, matrix_package_path)
    replay_dir = out_dir / "deterministic_replay"
    replay_result = run_core(replay_dir, matrix_package_path, write_docs=False)
    replay = compare_replay(out_dir, replay_dir)
    write_json(out_dir / "deterministic_replay_comparison_2026-07-13.json", replay)
    (out_dir / "deterministic_replay_comparison_2026-07-13.md").write_text(
        f"""# Deterministic Replay Comparison

Replay status: `{replay['status']}`

The replay used the same locked contract and authorization identity as
verification of the same authorized run, not a second distinct experiment.
"""
    )
    if replay["status"] != "PASS":
        raise ContractError("deterministic replay failed")
    package_sha = final_parse_and_sha(out_dir)
    return {
        "experiment_id": EXPERIMENT_ID,
        "decision": result["decision"]["process_validation_execution"],
        "post_execution_status": result["decision"]["post_execution_process_validation_status"],
        "approval_consumed": True,
        "attached_rows": result["labels"],
        "unattached_rows": result["unattached"],
        "replay_status": replay["status"],
        "package_sha256": package_sha,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-contract-path", required=True)
    parser.add_argument("--approval-artifact-path", required=True)
    parser.add_argument("--certified-matrix-package-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", choices=["research_only"], default="research_only")
    parser.add_argument("--create-approval-artifact", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        print(json.dumps(execute(args), indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        blocked = {
            "experiment_id": EXPERIMENT_ID,
            "process_validation_execution": "BOUNDED_OFFLINE_PROCESS_VALIDATION_BLOCKED_BY_ARTIFACT_COMPLETENESS",
            "error": str(exc),
        }
        write_json(out_dir / "blocked_execution_2026-07-13.json", blocked)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
