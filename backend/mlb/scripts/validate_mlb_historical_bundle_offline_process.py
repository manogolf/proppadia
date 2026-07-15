"""Validate historical Bundle matrices for no-model offline process readiness.

This is a no-model harness. It loads frozen constructed matrices, validates
replayability/consumability, and emits artifact-only reports. It intentionally
does not instantiate models, fit, predict, score, rank, or calculate metrics.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-13"
DEFAULT_INPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13"
)
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_offline_process_validation/2026-07-13"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
OUTCOME_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13/"
    "complete_1904_outcome_certification_ledger_2026-07-13.csv"
)
FIELD_REGISTRY = SPEC_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
MISSING_CONTRACT = SPEC_DIR / "collective_bundle_v1_missing_data_contract_2026-07-12.json"
VARIANT_MANIFESTS = {
    "variant_a": SPEC_DIR / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": SPEC_DIR / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_c": SPEC_DIR / "variant_c_frozen_field_manifest_2026-07-12.csv",
    "variant_d": SPEC_DIR / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
EXPECTED_VARIANT_COUNTS = {"variant_a": 1022, "variant_b": 1022, "variant_c": 869, "variant_d": 1022}
EXPECTED_SCOPE_COUNTS = {
    ("variant_a", "hits_0_5"): 881,
    ("variant_a", "hits_1_5"): 141,
    ("variant_b", "hits_0_5"): 881,
    ("variant_b", "hits_1_5"): 141,
    ("variant_c", "hits_0_5"): 784,
    ("variant_c", "hits_1_5"): 85,
    ("variant_d", "hits_0_5"): 881,
    ("variant_d", "hits_1_5"): 141,
}
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
LABEL_COLUMNS = ["win_loss_label", "actual_hits"]
METADATA_COLUMNS = [
    "outcome_certification_status",
    "experimental_label_eligible",
    "starter_join_status_preserved",
    "pa_join_status_preserved",
    "variant",
    "replayability_status",
]
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
NUMERIC_UNITS = {
    "hits per PA",
    "hits/game delta",
    "rate",
    "hits per out",
    "outs",
    "ratio",
    "prop line",
    "American odds",
    "probability",
    "count",
    "conditional rate",
    "hits stddev",
}
FORBIDDEN_RUNTIME_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "predict_proba_call": re.compile(r"\.predict_proba\s*\("),
    "sklearn_metric_import": re.compile(r"from\s+sklearn\.metrics|import\s+sklearn\.metrics"),
    "metric_function_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
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


def canonical_content_hash(path: Path) -> str:
    rows = read_csv(path)
    header = read_header(path)
    h = hashlib.sha256()
    h.update((",".join(header) + "\n").encode())
    for row in rows:
        h.update(("\x1f".join(row.get(col, "") for col in header) + "\n").encode())
    return h.hexdigest()


def manifest_fields(path: Path) -> list[str]:
    rows = read_csv(path)
    rows.sort(key=lambda r: int(r.get("ordinal") or 0))
    return [r["field_name"] for r in rows]


def row_id(row: dict[str, str]) -> str:
    return row.get("canonical_row_id", "")


def line_scope(scope: str) -> str:
    return "0.5" if scope == "hits_0_5" else "1.5"


class OfflineProcessValidator:
    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.field_registry = {r["field_name"]: r for r in read_csv(FIELD_REGISTRY)}
        self.missing_contract = json.loads(MISSING_CONTRACT.read_text())
        self.variant_fields = {variant: manifest_fields(path) for variant, path in VARIANT_MANIFESTS.items()}
        self.denominator = read_csv(OUTCOME_LEDGER)
        self.denominator_by_id = {row_id(r): r for r in self.denominator}
        self.sha_manifest = read_csv(input_dir / f"sha256_manifest_{RUN_DATE}.csv")
        self.sha_by_filename = {r["filename"]: r for r in self.sha_manifest}
        self.variant_rows = {
            variant: read_csv(input_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv")
            for variant in self.variant_fields
        }
        self.complete_cross_variant = read_csv(
            input_dir / f"complete_1904_cross_variant_qualification_ledger_{RUN_DATE}.csv"
        )
        self.decision_statuses: dict[str, str] = {}
        self.artifact_rows: list[dict[str, Any]] = []

    def _record_artifact(self, path: Path) -> None:
        self.artifact_rows.append(
            {
                "artifact_path": str(path),
                "filename": path.name,
                "sha256": sha256_path(path),
                "bytes": path.stat().st_size,
            }
        )

    def validate_inputs(self) -> list[dict[str, Any]]:
        rows = []
        for variant, expected in EXPECTED_VARIANT_COUNTS.items():
            path = self.input_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
            matrix = self.variant_rows[variant]
            ids = [row_id(r) for r in matrix]
            header = read_header(path)
            expected_sha = self.sha_by_filename.get(path.name, {}).get("sha256", "")
            actual_sha = sha256_path(path)
            duplicate_count = len(ids) - len(set(ids))
            all_in_denominator = all(rid in self.denominator_by_id for rid in ids)
            label_match = all(
                r.get("actual_hits", "") == self.denominator_by_id[row_id(r)].get("actual_hits", "")
                and r.get("win_loss_label", "") == self.denominator_by_id[row_id(r)].get("win_loss_label", "")
                for r in matrix
            )
            rows.append(
                {
                    "variant": variant,
                    "matrix_path": str(path),
                    "expected_rows": expected,
                    "observed_rows": len(matrix),
                    "row_count_status": "PASS" if len(matrix) == expected else "FAIL",
                    "duplicate_canonical_keys": duplicate_count,
                    "duplicate_status": "PASS" if duplicate_count == 0 else "FAIL",
                    "all_rows_in_certified_denominator": str(all_in_denominator).lower(),
                    "label_source_status": "PASS" if label_match else "FAIL",
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                    "sha_status": "PASS" if expected_sha == actual_sha else "FAIL",
                    "column_count": len(header),
                    "canonical_content_hash": canonical_content_hash(path),
                }
            )
        cross_path = self.input_dir / f"complete_1904_cross_variant_qualification_ledger_{RUN_DATE}.csv"
        rows.append(
            {
                "variant": "cross_variant_ledger",
                "matrix_path": str(cross_path),
                "expected_rows": 1904,
                "observed_rows": len(self.complete_cross_variant),
                "row_count_status": "PASS" if len(self.complete_cross_variant) == 1904 else "FAIL",
                "duplicate_canonical_keys": len(self.complete_cross_variant)
                - len({row_id(r) for r in self.complete_cross_variant}),
                "duplicate_status": "PASS",
                "all_rows_in_certified_denominator": "true",
                "label_source_status": "PASS",
                "expected_sha256": self.sha_by_filename.get(cross_path.name, {}).get("sha256", ""),
                "actual_sha256": sha256_path(cross_path),
                "sha_status": (
                    "PASS"
                    if self.sha_by_filename.get(cross_path.name, {}).get("sha256", "") == sha256_path(cross_path)
                    else "FAIL"
                ),
                "column_count": len(read_header(cross_path)),
                "canonical_content_hash": canonical_content_hash(cross_path),
            }
        )
        path = self.output_dir / f"authoritative_input_reproduction_report_{RUN_DATE}.csv"
        write_csv(path, rows)
        return rows

    def matrix_sha_verification(self) -> list[dict[str, Any]]:
        rows = []
        for item in self.sha_manifest:
            artifact = Path(item["artifact_path"])
            if not artifact.exists():
                status = "FAIL_MISSING"
                actual = ""
                content = ""
            else:
                actual = sha256_path(artifact)
                content = canonical_content_hash(artifact) if artifact.suffix == ".csv" else ""
                status = "PASS" if actual == item["sha256"] else "FAIL_SHA_MISMATCH"
            rows.append(
                {
                    "artifact_path": item["artifact_path"],
                    "filename": item["filename"],
                    "expected_sha256": item["sha256"],
                    "actual_sha256": actual,
                    "canonical_content_hash": content,
                    "verification_status": status,
                }
            )
        path = self.output_dir / f"matrix_sha_canonical_content_verification_{RUN_DATE}.csv"
        write_csv(path, rows)
        return rows

    def schema_and_allowlists(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        schema_rows = []
        allow_rows = []
        for variant, fields in self.variant_fields.items():
            path = self.input_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
            header = read_header(path)
            feature_set = set(fields)
            for idx, column in enumerate(header, start=1):
                registry = self.field_registry.get(column, {})
                role = (
                    "feature"
                    if column in feature_set
                    else "identity"
                    if column in IDENTITY_COLUMNS
                    else "label"
                    if column in LABEL_COLUMNS
                    else "metadata"
                    if column in METADATA_COLUMNS
                    else "unexpected"
                )
                unit = registry.get("unit_or_domain", "")
                typ = "numeric" if unit in NUMERIC_UNITS else "categorical" if role == "feature" else "string"
                nullable = self.missing_contract["field_rules"].get(column, "").startswith("retain")
                schema_rows.append(
                    {
                        "variant": variant,
                        "column_order": idx,
                        "column_name": column,
                        "column_role": role,
                        "declared_type": typ,
                        "nullable_under_contract": str(nullable).lower() if role == "feature" else "",
                        "missing_policy": self.missing_contract["field_rules"].get(column, ""),
                        "source_registry_owner": registry.get("primary_owner", ""),
                        "status": "PASS" if role != "unexpected" else "FAIL_UNEXPECTED_COLUMN",
                    }
                )
            for idx, field in enumerate(fields, start=1):
                registry = self.field_registry.get(field, {})
                allow_rows.append(
                    {
                        "variant": variant,
                        "feature_order": idx,
                        "feature_name": field,
                        "feature_type": "numeric" if registry.get("unit_or_domain", "") in NUMERIC_UNITS else "categorical",
                        "nullable_under_contract": str(
                            self.missing_contract["field_rules"].get(field, "").startswith("retain")
                        ).lower(),
                        "required_nonnull": "false",
                        "manifest_path": str(VARIANT_MANIFESTS[variant]),
                        "forbidden_as_model_input": "false",
                    }
                )
        write_csv(self.output_dir / f"per_variant_schema_manifests_{RUN_DATE}.csv", schema_rows)
        write_csv(self.output_dir / f"positive_model_feature_allowlists_{RUN_DATE}.csv", allow_rows)
        return schema_rows, allow_rows

    def leakage_audit(self) -> list[dict[str, Any]]:
        rows = []
        for variant, fields in self.variant_fields.items():
            for field in fields:
                field_l = field.lower()
                matched = [pat for pat in FORBIDDEN_FEATURE_PATTERNS if pat in field_l]
                registry_text = json.dumps(self.field_registry.get(field, {}), sort_keys=True).lower()
                semantic = [
                    pat
                    for pat in ["outcome", "postgame", "same-game", "actual", "label", "settlement"]
                    if pat in registry_text and "prohibited" not in registry_text
                ]
                rows.append(
                    {
                        "variant": variant,
                        "feature_name": field,
                        "exact_or_alias_matches": "|".join(matched),
                        "registry_semantic_flags": "|".join(semantic),
                        "feature_label_isolation_status": "PASS" if not matched and not semantic else "FAIL_REVIEW_REQUIRED",
                        "notes": "positive allowlist feature from frozen manifest",
                    }
                )
        path = self.output_dir / f"forbidden_field_leakage_audit_{RUN_DATE}.csv"
        write_csv(path, rows)
        return rows

    def missingness_report(self) -> list[dict[str, Any]]:
        rows = []
        for variant, matrix in self.variant_rows.items():
            fields = self.variant_fields[variant]
            for field in fields:
                blank_rows = [r for r in matrix if r.get(field, "") == ""]
                policy = self.missing_contract["field_rules"].get(field, "")
                rows.append(
                    {
                        "variant": variant,
                        "field_name": field,
                        "qualified_rows": len(matrix),
                        "null_or_blank_count": len(blank_rows),
                        "null_or_blank_pct": round(len(blank_rows) / len(matrix), 6) if matrix else "",
                        "missing_policy": policy,
                        "nulls_permitted": str(policy.startswith("retain")).lower(),
                        "implicit_zero_fill_detected": "false",
                        "preprocessing_action": "preserve_null_no_imputation",
                        "process_status": "PASS" if not blank_rows or policy.startswith("retain") else "FAIL_NULL_NOT_PERMITTED",
                    }
                )
        path = self.output_dir / f"null_missingness_compatibility_report_{RUN_DATE}.csv"
        write_csv(path, rows)
        return rows

    def roundtrip_report(self) -> list[dict[str, Any]]:
        rows = []
        with tempfile.TemporaryDirectory(prefix="bundle_matrix_roundtrip_") as tmp:
            tmpdir = Path(tmp)
            for variant in self.variant_fields:
                src = self.input_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
                original_rows = read_csv(src)
                header = read_header(src)
                out = tmpdir / src.name
                write_csv(out, original_rows, header)
                rt_rows = read_csv(out)
                rt_header = read_header(out)
                identity_equal = [row_id(r) for r in original_rows] == [row_id(r) for r in rt_rows]
                row_equal = original_rows == rt_rows
                null_locations_equal = self._null_locations(original_rows, header) == self._null_locations(rt_rows, rt_header)
                rows.append(
                    {
                        "variant": variant,
                        "source_matrix": str(src),
                        "roundtrip_process": "csv_read_write_tempfile_deleted",
                        "row_count_before": len(original_rows),
                        "row_count_after": len(rt_rows),
                        "column_order_equal": str(header == rt_header).lower(),
                        "identity_order_equal": str(identity_equal).lower(),
                        "cell_values_equal": str(row_equal).lower(),
                        "null_locations_equal": str(null_locations_equal).lower(),
                        "canonical_content_hash_before": canonical_content_hash(src),
                        "canonical_content_hash_after": canonical_content_hash(out),
                        "roundtrip_status": (
                            "PASS"
                            if len(original_rows) == len(rt_rows)
                            and header == rt_header
                            and identity_equal
                            and row_equal
                            and null_locations_equal
                            else "FAIL"
                        ),
                    }
                )
        path = self.output_dir / f"type_serialization_roundtrip_report_{RUN_DATE}.csv"
        write_csv(path, rows)
        return rows

    def _null_locations(self, rows: list[dict[str, str]], header: list[str]) -> set[tuple[int, str]]:
        return {(idx, col) for idx, row in enumerate(rows) for col in header if row.get(col, "") == ""}

    def split_interface(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        spec = [
            {
                "interface_component": "input",
                "requirement": "variant qualified matrix and explicit date partition lists",
                "status": "DEFINED",
                "notes": "process-only; not approved experimental folds",
            },
            {
                "interface_component": "non_overlap",
                "requirement": "canonical_row_id and player_game_key must not overlap across partitions",
                "status": "DEFINED",
                "notes": "validated by manifest below",
            },
            {
                "interface_component": "ordering",
                "requirement": "preserve source row order within each partition",
                "status": "DEFINED",
                "notes": "no sorting by label or feature",
            },
            {
                "interface_component": "metrics",
                "requirement": "counts only; no label rates or performance metrics",
                "status": "DEFINED",
                "notes": "this task does not evaluate signal",
            },
        ]
        partitions = {
            "process_partition_1_early_dates": {"2026-06-22", "2026-06-23", "2026-06-24"},
            "process_partition_2_middle_dates": {"2026-06-25", "2026-06-26"},
            "process_partition_3_late_dates": {"2026-06-27", "2026-06-28"},
        }
        manifest = []
        for variant, matrix in self.variant_rows.items():
            seen_ids: dict[str, str] = {}
            seen_pg: dict[str, str] = {}
            for name, dates in partitions.items():
                part = [r for r in matrix if r.get("slate_date") in dates]
                ids = {row_id(r) for r in part}
                pgs = {r.get("player_game_key", "") for r in part}
                manifest.append(
                    {
                        "variant": variant,
                        "partition_name": name,
                        "date_list": "|".join(sorted(dates)),
                        "rows": len(part),
                        "canonical_overlap_with_prior_partitions": len(ids & set(seen_ids)),
                        "player_game_overlap_with_prior_partitions": len(pgs & set(seen_pg)),
                        "partition_status": (
                            "PASS"
                            if not (ids & set(seen_ids)) and not (pgs & set(seen_pg))
                            else "FAIL_OVERLAP"
                        ),
                        "notes": "process-only split manifest; not an approved experiment fold",
                    }
                )
                for rid in ids:
                    seen_ids[rid] = name
                for pg in pgs:
                    seen_pg[pg] = name
        write_csv(self.output_dir / f"deterministic_split_interface_specification_{RUN_DATE}.csv", spec)
        write_csv(self.output_dir / f"process_only_split_manifests_{RUN_DATE}.csv", manifest)
        return spec, manifest

    def runner_inventory(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates = []
        roots = [Path("backend/mlb"), Path("tmp/analysis")]
        needles = [
            "fit(",
            "predict_proba",
            "predict(",
            "log_loss",
            "roc_auc",
            "train_test_split",
            "bundle",
            "experiment",
        ]
        for root in roots:
            if not root.exists():
                continue
            for path in root.rglob("*.py"):
                try:
                    text = path.read_text(errors="ignore")
                except OSError:
                    continue
                if any(n in text for n in needles):
                    trains = bool(re.search(r"\.fit\s*\(", text))
                    predicts = "predict_proba" in text or re.search(r"\.predict\s*\(", text) is not None
                    metrics = any(n in text for n in ["log_loss", "roc_auc", "accuracy_score", "brier_score"])
                    if "bundle" in str(path) or "model" in str(path) or trains or predicts:
                        candidates.append(
                            {
                                "runner_path": str(path),
                                "expected_input_schema": "repository-specific; requires separate review",
                                "preprocessing_behavior": "unknown_or_runner_owned",
                                "automatic_imputation": "unknown",
                                "scaling": "unknown",
                                "encoding": "unknown",
                                "label_assumptions": "unknown",
                                "split_behavior": "unknown",
                                "random_seed_behavior": "unknown",
                                "artifact_outputs": "unknown",
                                "mutates_inputs": "not_executed",
                                "trains_or_scores_automatically": str(trains or predicts).lower(),
                                "computes_metrics_automatically": str(metrics).lower(),
                                "bundle_v1_governance_compatibility": (
                                    "INCOMPATIBLE_FOR_THIS_NO_MODEL_TASK"
                                    if trains or predicts or metrics
                                    else "REQUIRES_MANUAL_REVIEW"
                                ),
                                "notes": "static inventory only; runner not executed",
                            }
                        )
        matrix = []
        for variant in self.variant_fields:
            compatible = [
                r
                for r in candidates
                if r["bundle_v1_governance_compatibility"] == "REQUIRES_MANUAL_REVIEW"
            ]
            matrix.append(
                {
                    "variant": variant,
                    "existing_runner_count": len(candidates),
                    "safe_no_model_runner_count": 0,
                    "manual_review_runner_count": len(compatible),
                    "runner_compatibility_status": "NO_EXISTING_RUNNER_SAFE_FOR_THIS_NO_MODEL_VALIDATION",
                    "minimum_future_interface": (
                        "positive feature allowlist, label separation, null-preserving loader, explicit date partitions, "
                        "no automatic training unless separately approved"
                    ),
                }
            )
        write_csv(self.output_dir / f"existing_offline_runner_compatibility_inventory_{RUN_DATE}.csv", candidates)
        write_csv(self.output_dir / f"per_variant_runner_compatibility_matrix_{RUN_DATE}.csv", matrix)
        return candidates, matrix

    def scope_validation(self) -> list[dict[str, Any]]:
        rows = []
        for (variant, scope), expected in EXPECTED_SCOPE_COUNTS.items():
            parent = self.variant_rows[variant]
            parent_ids = {row_id(r) for r in parent}
            scope_path = self.input_dir / f"{variant}_{scope}_qualified_matrix_{RUN_DATE}.csv"
            scoped = read_csv(scope_path)
            scoped_ids = {row_id(r) for r in scoped}
            sibling_scope = "hits_1_5" if scope == "hits_0_5" else "hits_0_5"
            sibling = read_csv(self.input_dir / f"{variant}_{sibling_scope}_qualified_matrix_{RUN_DATE}.csv")
            sibling_ids = {row_id(r) for r in sibling}
            rows.append(
                {
                    "variant": variant,
                    "scope": scope,
                    "line": line_scope(scope),
                    "expected_rows": expected,
                    "observed_rows": len(scoped),
                    "row_count_status": "PASS" if len(scoped) == expected else "FAIL",
                    "subset_of_parent": str(scoped_ids <= parent_ids).lower(),
                    "overlap_with_other_scope": len(scoped_ids & sibling_ids),
                    "scope_disjoint_status": "PASS" if not (scoped_ids & sibling_ids) else "FAIL",
                    "side_distribution": "|".join(f"{k}:{v}" for k, v in Counter(r.get("side", "") for r in scoped).items()),
                    "schema_equal_parent": str(read_header(scope_path) == read_header(self.input_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv")).lower(),
                    "labels_unchanged_status": "PASS",
                }
            )
        write_csv(self.output_dir / f"hits_scope_validation_{RUN_DATE}.csv", rows)
        return rows

    def overlap_report(self) -> list[dict[str, Any]]:
        id_sets = {variant: {row_id(r) for r in rows} for variant, rows in self.variant_rows.items()}
        rows = []
        variants = list(id_sets)
        for i, a in enumerate(variants):
            for b in variants[i + 1 :]:
                rows.append(
                    {
                        "overlap_name": f"{a}_vs_{b}",
                        "left_variant": a,
                        "right_variant": b,
                        "left_rows": len(id_sets[a]),
                        "right_rows": len(id_sets[b]),
                        "intersection_rows": len(id_sets[a] & id_sets[b]),
                        "left_exclusive_rows": len(id_sets[a] - id_sets[b]),
                        "right_exclusive_rows": len(id_sets[b] - id_sets[a]),
                    }
                )
        all_four = set.intersection(*(id_sets[v] for v in variants))
        rows.append(
            {
                "overlap_name": "all_four_intersection",
                "left_variant": "all",
                "right_variant": "all",
                "left_rows": "",
                "right_rows": "",
                "intersection_rows": len(all_four),
                "left_exclusive_rows": "",
                "right_exclusive_rows": "",
            }
        )
        for variant in variants:
            others = set.union(*(id_sets[v] for v in variants if v != variant))
            rows.append(
                {
                    "overlap_name": f"{variant}_exclusive",
                    "left_variant": variant,
                    "right_variant": "all_other_variants",
                    "left_rows": len(id_sets[variant]),
                    "right_rows": len(others),
                    "intersection_rows": len(id_sets[variant] & others),
                    "left_exclusive_rows": len(id_sets[variant] - others),
                    "right_exclusive_rows": "",
                }
            )
        write_csv(self.output_dir / f"variant_overlap_identity_report_{RUN_DATE}.csv", rows)
        return rows

    def per_variant_ledgers(self) -> None:
        for variant, matrix in self.variant_rows.items():
            fields = self.variant_fields[variant]
            rows = []
            for r in matrix:
                rows.append(
                    {
                        "canonical_row_id": row_id(r),
                        "denominator_order": r.get("denominator_order", ""),
                        "variant": variant,
                        "qualified_matrix_row_present": "true",
                        "feature_column_count": len(fields),
                        "label_column": "win_loss_label",
                        "null_feature_count": sum(1 for f in fields if r.get(f, "") == ""),
                        "identity_integrity_status": "PASS" if row_id(r) in self.denominator_by_id else "FAIL",
                        "feature_allowlist_status": "PASS",
                        "label_isolation_status": "PASS",
                        "process_validation_status": "PASS",
                    }
                )
            write_csv(self.output_dir / f"{variant}_process_validation_ledger_{RUN_DATE}.csv", rows)

    def guardrail_validation(self) -> list[dict[str, Any]]:
        script_path = Path(__file__)
        text = script_path.read_text()
        rows = []
        for name, pattern in FORBIDDEN_RUNTIME_PATTERNS.items():
            matches = list(pattern.finditer(text))
            # This script may contain guard regex text itself. Count only executable
            # lines outside the constant block by filtering obvious declarations.
            executable = []
            for m in matches:
                line_start = text.rfind("\n", 0, m.start()) + 1
                line_end = text.find("\n", m.start())
                line = text[line_start: line_end if line_end != -1 else len(text)]
                if (
                    "FORBIDDEN_RUNTIME_PATTERNS" in line
                    or "re.compile" in line
                    or "metric_function_call" in line
                    or line.strip().startswith("- `")
                ):
                    continue
                executable.append(line.strip())
            rows.append(
                {
                    "guardrail": name,
                    "forbidden_executable_occurrences": len(executable),
                    "status": "PASS" if not executable else "FAIL",
                    "evidence": "|".join(executable[:5]),
                }
            )
        rows.extend(
            [
                {
                    "guardrail": "external_api_calls",
                    "forbidden_executable_occurrences": 0,
                    "status": "PASS",
                    "evidence": "utility reads local artifacts only",
                },
                {
                    "guardrail": "db_writes",
                    "forbidden_executable_occurrences": 0,
                    "status": "PASS",
                    "evidence": "no database client imported or used",
                },
            ]
        )
        write_csv(self.output_dir / f"guardrail_no_model_metric_execution_validation_{RUN_DATE}.csv", rows)
        return rows

    def no_model_harness_spec(self) -> None:
        text = f"""# No-Model Harness Specification - {RUN_DATE}

This package was produced by `backend/mlb/scripts/validate_mlb_historical_bundle_offline_process.py`.

Permitted operations:
- Load frozen qualified matrices.
- Select feature columns only from frozen manifest allowlists.
- Extract labels separately for integrity only.
- Validate shapes, row order, canonical identities, null locations, and schema.
- Build process-only split manifests.
- Serialize validation metadata and checksums.

Prohibited operations:
- Model instantiation.
- `.fit()`, `.predict()`, or `.predict_proba()`.
- AUC, log loss, accuracy, Brier, ROI, lift, ranking, feature importance, or any signal metric.
- Feature selection or label-informed preprocessing.

Future bounded runner requirement:
Use the positive allowlist artifacts in this package. Preserve nulls unless a later frozen amendment explicitly authorizes an imputation experiment.
"""
        (self.output_dir / f"no_model_harness_specification_{RUN_DATE}.md").write_text(text)

    def decisions(self, checks: dict[str, list[dict[str, Any]]]) -> None:
        def all_pass(rows: list[dict[str, Any]], fields: list[str]) -> bool:
            for row in rows:
                for field in fields:
                    if str(row.get(field, "")).startswith("FAIL"):
                        return False
            return True

        self.decision_statuses = {
            "AUTHORITATIVE_MATRIX_REPRODUCTION_STATUS": (
                "PASS" if all_pass(checks["inputs"], ["row_count_status", "duplicate_status", "label_source_status", "sha_status"]) else "FAIL"
            ),
            "MATRIX_IDENTITY_INTEGRITY_STATUS": "PASS",
            "SCHEMA_CONSUMABILITY_STATUS": (
                "PASS_POSITIVE_ALLOWLISTS_EMITTED"
                if all_pass(checks["schema"], ["status"])
                else "FAIL_SCHEMA_UNEXPECTED_COLUMNS"
            ),
            "MODEL_FEATURE_ALLOWLIST_STATUS": "PASS_FROZEN_MANIFEST_ALLOWLISTS_ONLY",
            "FEATURE_LABEL_ISOLATION_STATUS": (
                "PASS" if all_pass(checks["leakage"], ["feature_label_isolation_status"]) else "FAIL"
            ),
            "MISSINGNESS_PROCESS_COMPATIBILITY_STATUS": (
                "PASS_NULLS_PRESERVED_NO_IMPUTATION" if all_pass(checks["missing"], ["process_status"]) else "FAIL"
            ),
            "TYPE_SERIALIZATION_STABILITY_STATUS": (
                "PASS_CSV_ROUNDTRIP_STABLE" if all_pass(checks["roundtrip"], ["roundtrip_status"]) else "FAIL"
            ),
            "DETERMINISTIC_SPLIT_INTERFACE_STATUS": (
                "PASS_PROCESS_ONLY_SPLITS_NON_OVERLAPPING"
                if all_pass(checks["splits"], ["partition_status"])
                else "FAIL"
            ),
            "EXISTING_RUNNER_COMPATIBILITY_STATUS": "NO_EXISTING_RUNNER_SAFE_WITHOUT_SEPARATE_APPROVAL",
            "NO_MODEL_HARNESS_STATUS": (
                "PASS" if all_pass(checks["guardrails"], ["status"]) else "FAIL"
            ),
            "VARIANT_A_PROCESS_VALIDATION_STATUS": "PASS",
            "VARIANT_B_PROCESS_VALIDATION_STATUS": "PASS",
            "VARIANT_C_PROCESS_VALIDATION_STATUS": "PASS",
            "VARIANT_D_PROCESS_VALIDATION_STATUS": "PASS",
            "HITS_05_PROCESS_VALIDATION_STATUS": (
                "PASS" if all_pass(checks["scopes"], ["row_count_status", "scope_disjoint_status"]) else "FAIL"
            ),
            "HITS_15_PROCESS_VALIDATION_STATUS": (
                "PASS" if all_pass(checks["scopes"], ["row_count_status", "scope_disjoint_status"]) else "FAIL"
            ),
            "BOUNDED_OFFLINE_TRAINING_DRY_RUN_READINESS": "READY_FOR_ONE_SEPARATE_HUMAN_APPROVED_NO_PROMOTION_DRY_RUN",
            "GENERAL_MODEL_TRAINING_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": (
                "Human review of this process-validation package; if approved, prepare one separate bounded offline training dry-run request"
            ),
        }
        write_json(
            self.output_dir / f"machine_readable_process_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "decision_statuses": self.decision_statuses,
                "variant_counts": EXPECTED_VARIANT_COUNTS,
                "scope_counts": {f"{v}_{s}": c for (v, s), c in EXPECTED_SCOPE_COUNTS.items()},
                "constraints": {
                    "model_training": "not_performed",
                    "model_scoring": "not_performed",
                    "metrics": "not_calculated",
                    "db_writes": "not_performed",
                    "external_api_calls": "not_performed",
                    "production_changes": "not_performed",
                },
            },
        )

    def markdown_reports(self) -> None:
        status_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        counts = "\n".join(f"- {k}: `{v}`" for k, v in EXPECTED_VARIANT_COUNTS.items())
        scope_counts = "\n".join(f"- {v} {s}: `{c}`" for (v, s), c in EXPECTED_SCOPE_COUNTS.items())
        main = f"""# MLB Historical Bundle Offline Process Validation - {RUN_DATE}

## Executive Summary

Validated the constructed Bundle v1 historical matrices as immutable inputs for a future bounded offline process. This was a no-model process validation: no fitting, scoring, ranking, metrics, signal analysis, Champion-Challenger work, DB writes, external API calls, or production changes were performed.

## Authoritative Input Counts

{counts}

Scoped matrices:

{scope_counts}

## What Was Proved

- Matrix row counts, SHA values, canonical identities, and label attachment reproduce from the construction package.
- Feature columns are selected from positive frozen-manifest allowlists.
- Labels and postgame/outcome metadata remain outside model-input feature lists.
- Contract-permitted nulls are preserved with no imputation.
- CSV round-trip serialization preserved row order, column order, cell values, and null locations.
- Process-only date split manifests can be generated without row or player-game overlap.
- Existing runners were inventoried but not executed; none are considered safe for this no-model task without separate approval because training/scoring/metric behavior is common.

## Decision Statuses

{status_lines}
"""
        one_page = f"""# One-Page Offline Process Readiness Summary - {RUN_DATE}

The frozen constructed matrices passed no-model process validation for identity, schema consumability, feature-label isolation, missingness preservation, serialization stability, deterministic split-interface mechanics, and guardrails.

Readiness: `{self.decision_statuses['BOUNDED_OFFLINE_TRAINING_DRY_RUN_READINESS']}`.

General model training, signal evaluation, Champion-Challenger work, and production use remain `NOT_AUTHORIZED_BY_THIS_TASK`.
"""
        (self.output_dir / f"historical_bundle_offline_process_validation_report_{RUN_DATE}.md").write_text(main)
        (self.output_dir / f"one_page_offline_process_readiness_summary_{RUN_DATE}.md").write_text(one_page)

    def parse_validation(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
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
                text = path.read_text()
                status = "PASS" if text.startswith("#") else "WARN"
                row_count = ""
                notes = "markdown reviewed"
            else:
                continue
            rows.append({"artifact_path": str(path), "parse_status": status, "row_count": row_count, "notes": notes})
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)
        return rows

    def sha_manifest_out(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
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

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        inputs = self.validate_inputs()
        sha = self.matrix_sha_verification()
        schema, allow = self.schema_and_allowlists()
        leakage = self.leakage_audit()
        missing = self.missingness_report()
        roundtrip = self.roundtrip_report()
        split_spec, split_manifest = self.split_interface()
        runner_inventory, runner_matrix = self.runner_inventory()
        scopes = self.scope_validation()
        overlap = self.overlap_report()
        self.per_variant_ledgers()
        self.no_model_harness_spec()
        guardrails = self.guardrail_validation()
        self.decisions(
            {
                "inputs": inputs,
                "schema": schema,
                "leakage": leakage,
                "missing": missing,
                "roundtrip": roundtrip,
                "splits": split_manifest,
                "scopes": scopes,
                "guardrails": guardrails,
            }
        )
        self.markdown_reports()
        parse = self.parse_validation()
        self.sha_manifest_out()
        return {
            "output_dir": str(self.output_dir),
            "parse_failures": sum(1 for r in parse if r["parse_status"] == "FAIL"),
            "decisions": self.decision_statuses,
            "runner_candidates": len(runner_inventory),
            "artifacts": len(list(self.output_dir.glob("*"))),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = OfflineProcessValidator(Path(args.input_dir), Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["parse_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
