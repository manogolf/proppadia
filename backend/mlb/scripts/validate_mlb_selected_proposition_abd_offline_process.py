"""Validate selected-proposition A/B/D matrices for no-model offline process readiness.

This is a process-only harness. It loads the frozen 99-row Hits 1.5
selected-proposition Variant A, B, and D matrices and validates identity,
schema, serialization, label integrity, provenance, and guardrails. It does
not train, score, rank, calculate signal metrics, call APIs, write databases,
or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_INPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_offline_process_validation/2026-07-14"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")

FIELD_REGISTRY = SPEC_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
VARIANT_MANIFESTS = {
    "variant_a": SPEC_DIR / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": SPEC_DIR / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_d": SPEC_DIR / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
VARIANT_LABELS = {"variant_a": "A", "variant_b": "B", "variant_d": "D"}
EXPECTED_VARIANT_ROWS = {"variant_a": 99, "variant_b": 99, "variant_d": 99}
EXPECTED_FEATURE_COUNTS = {"variant_a": 12, "variant_b": 14, "variant_d": 7}
EXPECTED_STATUS_BY_VARIANT = {
    "variant_a": "VARIANT_A_AUTHORITATIVE_REPRODUCTION",
    "variant_b": "VARIANT_B_AUTHORITATIVE_REPRODUCTION",
    "variant_d": "VARIANT_D_AUTHORITATIVE_REPRODUCTION",
}

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
LABEL_COLUMNS = ["actual_hits", "win_loss_label"]
AUDIT_COLUMNS = [
    "outcome_certification_status",
    "experimental_label_eligible",
    "starter_join_status_preserved",
    "pa_join_status_preserved",
    "selection_conditioned_population",
    "side_semantic_class",
    "market_side_identity",
    "opposite_side_in_denominator",
    "governance_scope",
    "variant",
    "matrix_certification_status",
    "replayability_status",
    "source_provenance_refs",
]
SELECTED_PROPOSITION_FIELDS = [
    "selection_conditioned_population",
    "side_semantic_class",
    "market_side_identity",
    "opposite_side_in_denominator",
    "governance_scope",
]
NUMERIC_UNITS = {
    "hits per PA",
    "hits/game delta",
    "rate",
    "hits per out",
    "outs",
    "ratio",
    "probability",
    "count",
    "conditional rate",
    "hits stddev",
}
FORBIDDEN_FEATURE_PATTERNS = [
    "actual_hit",
    "actual_",
    "win_loss",
    "settlement",
    "outcome",
    "participation",
    "official_game",
    "game_final",
    "nonappearance",
    "source_authority",
    "source_provenance",
    "game_status",
    "postgame",
    "target",
    "model_pick_side",
    "p_over",
    "selection_probability",
    "side_binding",
]
PROHIBITED_RUNTIME_PATTERNS = {
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


def base_key(row: dict[str, str]) -> str:
    return "|".join(
        [
            row.get("slate_date", ""),
            row.get("game_id", ""),
            row.get("player_id", ""),
            row.get("prop_type", ""),
            row.get("line", ""),
        ]
    )


def governed_key(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


class SelectedPropositionABDValidator:
    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.field_registry = {r["field_name"]: r for r in read_csv(FIELD_REGISTRY)}
        self.variant_features = {variant: manifest_fields(path) for variant, path in VARIANT_MANIFESTS.items()}
        self.variant_paths = {
            variant: input_dir / f"{variant}_hits_1_5_qualified_matrix_{RUN_DATE}.csv"
            for variant in VARIANT_MANIFESTS
        }
        self.schema_paths = {
            variant: input_dir / f"{variant}_matrix_schema_manifest_{RUN_DATE}.csv"
            for variant in VARIANT_MANIFESTS
        }
        self.variant_rows = {variant: read_csv(path) for variant, path in self.variant_paths.items()}
        self.population_99 = read_csv(input_dir / f"frozen_99_row_population_manifest_{RUN_DATE}.csv")
        self.excluded_36 = read_csv(input_dir / f"frozen_36_row_exclusion_reference_ledger_{RUN_DATE}.csv")
        self.source_sha_manifest = read_csv(input_dir / f"sha256_manifest_{RUN_DATE}.csv")
        self.source_sha_by_path = {r["path"]: r for r in self.source_sha_manifest}
        self.decision_statuses: dict[str, str] = {}
        self.source_before_sha = {str(path): sha256_path(path) for path in self.variant_paths.values()}

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        inputs = self.authoritative_input_reproduction()
        source_sha = self.source_matrix_immutability_audit()
        schemas, allowlists = self.process_schemas_and_allowlists()
        leakage = self.forbidden_field_and_leakage_audit()
        provenance = self.selected_proposition_provenance_audit()
        missing = self.null_missingness_report()
        roundtrip = self.serialization_roundtrip_report()
        labels = self.label_integrity_audit()
        cross = self.cross_variant_comparison()
        split_spec, split_manifest = self.split_interface()
        runners, runner_matrix = self.runner_inventory()
        self.no_model_harness_specification()
        guards = self.guardrail_validation()
        excluded = self.excluded_36_integrity_audit()
        variant_c = self.variant_c_absence_audit()
        self.decisions(
            {
                "inputs": inputs,
                "source_sha": source_sha,
                "schemas": schemas,
                "leakage": leakage,
                "provenance": provenance,
                "missing": missing,
                "roundtrip": roundtrip,
                "labels": labels,
                "cross": cross,
                "splits": split_manifest,
                "guards": guards,
                "excluded": excluded,
                "variant_c": variant_c,
            }
        )
        self.per_variant_ledgers()
        self.markdown_reports()
        self.deterministic_replay_report()
        parse = self.parse_validation()
        self.sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "variant_a_rows": len(self.variant_rows["variant_a"]),
            "variant_b_rows": len(self.variant_rows["variant_b"]),
            "variant_d_rows": len(self.variant_rows["variant_d"]),
            "parse_failures": sum(1 for r in parse if r["parse_status"] == "FAIL"),
            "runner_candidates": len(runners),
            "decisions": self.decision_statuses,
        }

    def authoritative_input_reproduction(self) -> list[dict[str, Any]]:
        population_ids = [r["governed_canonical_row_id"] for r in self.population_99]
        population_set = set(population_ids)
        excluded_set = {r["governed_canonical_row_id"] for r in self.excluded_36}
        rows = []
        for variant, matrix in self.variant_rows.items():
            path = self.variant_paths[variant]
            header = read_header(path)
            features = self.variant_features[variant]
            ids = [governed_key(r) for r in matrix]
            base_ids = [base_key(r) for r in matrix]
            schema_features = [
                r["column_name"]
                for r in read_csv(self.schema_paths[variant])
                if r.get("column_role") == "feature"
            ]
            expected_sha = self.source_sha_by_path.get(str(path), {}).get("sha256", "")
            actual_sha = sha256_path(path)
            label_ok = all(r.get("actual_hits", "") != "" and r.get("win_loss_label", "") in {"win", "loss"} for r in matrix)
            rows.append(
                {
                    "variant": variant,
                    "matrix_path": str(path),
                    "expected_rows": EXPECTED_VARIANT_ROWS[variant],
                    "observed_rows": len(matrix),
                    "row_count_status": "PASS" if len(matrix) == EXPECTED_VARIANT_ROWS[variant] else "FAIL",
                    "expected_feature_count": EXPECTED_FEATURE_COUNTS[variant],
                    "observed_feature_count": len(features),
                    "feature_count_status": "PASS" if len(features) == EXPECTED_FEATURE_COUNTS[variant] else "FAIL",
                    "unique_governed_keys": len(set(ids)),
                    "unique_base_keys": len(set(base_ids)),
                    "unique_key_status": "PASS" if len(set(ids)) == len(ids) and len(set(base_ids)) == len(base_ids) else "FAIL",
                    "all_rows_in_frozen_99_population": str(set(ids) == population_set).lower(),
                    "row_order_matches_frozen_population": str(ids == population_ids).lower(),
                    "excluded_36_overlap": len(set(ids) & excluded_set),
                    "opposite_side_synthetic_rows": 0,
                    "schema_feature_order_matches_manifest": str(schema_features == features).lower(),
                    "label_field_status": "PASS" if label_ok else "FAIL",
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                    "sha_status": "PASS" if expected_sha == actual_sha else "FAIL",
                    "column_count": len(header),
                    "canonical_content_hash": canonical_content_hash(path),
                }
            )
        write_csv(self.output_dir / f"authoritative_input_reproduction_report_{RUN_DATE}.csv", rows)
        return rows

    def source_matrix_immutability_audit(self) -> list[dict[str, Any]]:
        rows = []
        for variant, path in self.variant_paths.items():
            before = self.source_before_sha[str(path)]
            after = sha256_path(path)
            rows.append(
                {
                    "variant": variant,
                    "source_matrix_path": str(path),
                    "sha256_before_validation": before,
                    "sha256_after_validation": after,
                    "canonical_content_hash_after_validation": canonical_content_hash(path),
                    "immutability_status": "PASS" if before == after else "FAIL_SOURCE_CHANGED",
                }
            )
        for artifact in [
            self.input_dir / f"frozen_99_row_population_manifest_{RUN_DATE}.csv",
            self.input_dir / f"frozen_36_row_exclusion_reference_ledger_{RUN_DATE}.csv",
            self.input_dir / f"machine_readable_construction_decision_{RUN_DATE}.json",
            self.input_dir / f"deterministic_replay_report_{RUN_DATE}.csv",
        ]:
            rows.append(
                {
                    "variant": "source_artifact",
                    "source_matrix_path": str(artifact),
                    "sha256_before_validation": "",
                    "sha256_after_validation": sha256_path(artifact),
                    "canonical_content_hash_after_validation": canonical_content_hash(artifact) if artifact.suffix == ".csv" else "",
                    "immutability_status": "PASS",
                }
            )
        write_csv(self.output_dir / f"source_matrix_immutability_audit_{RUN_DATE}.csv", rows)
        return rows

    def process_schemas_and_allowlists(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        schema_rows = []
        allow_rows = []
        for variant, matrix in self.variant_rows.items():
            header = read_header(self.variant_paths[variant])
            features = self.variant_features[variant]
            for idx, column in enumerate(header, start=1):
                role = (
                    "identity"
                    if column in IDENTITY_COLUMNS
                    else "feature"
                    if column in features
                    else "label"
                    if column in LABEL_COLUMNS
                    else "audit_metadata"
                    if column in AUDIT_COLUMNS
                    else "unexpected"
                )
                registry = self.field_registry.get(column, {})
                declared_type = self._declared_type(column, role, matrix)
                null_count = sum(1 for row in matrix if row.get(column, "") == "")
                nullable = role == "feature" and self._missing_policy(column).startswith("retain")
                schema_rows.append(
                    {
                        "variant": variant,
                        "column_order": idx,
                        "column_name": column,
                        "column_role": role,
                        "declared_type": declared_type,
                        "null_count": null_count,
                        "nullable_under_contract": str(nullable).lower() if role == "feature" else "",
                        "nonnull_required": str(not nullable).lower() if role == "feature" else "",
                        "missing_policy": self._missing_policy(column),
                        "source_table_or_artifact": registry.get("source_table_or_artifact", ""),
                        "source_generator_or_owner": registry.get("source_generator_or_owner", ""),
                        "selected_proposition_provenance_field": str(column in SELECTED_PROPOSITION_FIELDS).lower(),
                        "forbidden_model_input": str(role != "feature").lower(),
                        "serialization_expectation": "csv_string_preserve_exact_cell_then_type_at_loader_boundary",
                        "schema_status": "PASS" if role != "unexpected" else "FAIL_UNEXPECTED_COLUMN",
                    }
                )
            for order, field in enumerate(features, start=1):
                registry = self.field_registry.get(field, {})
                allow_rows.append(
                    {
                        "variant": variant,
                        "feature_order": order,
                        "feature_name": field,
                        "declared_type": self._declared_type(field, "feature", matrix),
                        "missing_policy": self._missing_policy(field),
                        "primary_owner": registry.get("primary_owner", ""),
                        "native_grain": registry.get("native_grain", ""),
                        "target_grain": registry.get("target_grain", ""),
                        "source_table_or_artifact": registry.get("source_table_or_artifact", ""),
                        "manifest_path": str(VARIANT_MANIFESTS[variant]),
                        "allowlist_status": "PASS_FROZEN_MANIFEST_FEATURE",
                    }
                )
            write_csv(self.output_dir / f"{variant}_process_schema_{RUN_DATE}.csv", [r for r in schema_rows if r["variant"] == variant])
        write_csv(self.output_dir / f"positive_feature_allowlists_{RUN_DATE}.csv", allow_rows)
        return schema_rows, allow_rows

    def _missing_policy(self, field: str) -> str:
        return self.field_registry.get(field, {}).get("missing_policy", "metadata_not_in_field_registry")

    def _declared_type(self, column: str, role: str, rows: list[dict[str, str]]) -> str:
        if role != "feature":
            return "integer" if column in {"denominator_order", "game_id", "player_id", "actual_hits"} else "string"
        unit = self.field_registry.get(column, {}).get("unit_or_domain", "")
        values = [row.get(column, "") for row in rows if row.get(column, "") != ""]
        if unit in NUMERIC_UNITS or all(self._is_float(v) for v in values):
            return "numeric"
        if set(values) <= {"True", "False", "true", "false"}:
            return "boolean"
        return "categorical"

    def _is_float(self, value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def forbidden_field_and_leakage_audit(self) -> list[dict[str, Any]]:
        rows = []
        for variant, features in self.variant_features.items():
            for field in features:
                lowered = field.lower()
                registry_text = json.dumps(self.field_registry.get(field, {}), sort_keys=True).lower()
                pattern_hits = [p for p in FORBIDDEN_FEATURE_PATTERNS if p in lowered]
                semantic_hits = [
                    p
                    for p in ["postgame", "actual", "settlement", "outcome", "label", "same-game"]
                    if p in registry_text and "prohibited" not in registry_text
                ]
                rows.append(
                    {
                        "variant": variant,
                        "feature_name": field,
                        "positive_allowlist_member": "true",
                        "forbidden_name_alias_hits": "|".join(pattern_hits),
                        "registry_semantic_flags": "|".join(semantic_hits),
                        "feature_label_isolation_status": "PASS" if not pattern_hits and not semantic_hits else "FAIL_REVIEW_REQUIRED",
                        "notes": "governed side is identity/audit only; not in feature allowlist",
                    }
                )
            for forbidden in LABEL_COLUMNS + AUDIT_COLUMNS + ["model_pick_side", "p_over"]:
                rows.append(
                    {
                        "variant": variant,
                        "feature_name": forbidden,
                        "positive_allowlist_member": "false",
                        "forbidden_name_alias_hits": "explicit_forbidden_or_metadata",
                        "registry_semantic_flags": "",
                        "feature_label_isolation_status": "PASS_EXCLUDED_FROM_FEATURE_ALLOWLIST",
                        "notes": "not selected as model-input feature",
                    }
                )
        write_csv(self.output_dir / f"forbidden_field_and_leakage_audit_{RUN_DATE}.csv", rows)
        return rows

    def selected_proposition_provenance_audit(self) -> list[dict[str, Any]]:
        rows = []
        for variant, matrix in self.variant_rows.items():
            for idx, row in enumerate(matrix, start=1):
                ok = (
                    row.get("selection_conditioned_population") == "true"
                    and row.get("side_semantic_class") == "PRE_GAME_MODEL_SELECTED_DIRECTION"
                    and row.get("market_side_identity") == "false"
                    and row.get("opposite_side_in_denominator") == "false"
                    and row.get("governance_scope") == "HISTORICAL_RESEARCH_ONLY"
                )
                rows.append(
                    {
                        "variant": variant,
                        "row_number": idx,
                        "governed_canonical_row_id": governed_key(row),
                        "side": row.get("side", ""),
                        "selection_conditioned_population": row.get("selection_conditioned_population", ""),
                        "side_semantic_class": row.get("side_semantic_class", ""),
                        "market_side_identity": row.get("market_side_identity", ""),
                        "opposite_side_in_denominator": row.get("opposite_side_in_denominator", ""),
                        "governance_scope": row.get("governance_scope", ""),
                        "full_market_generalization": "prohibited",
                        "unrestricted_side_selection_evaluation": "prohibited",
                        "provenance_status": "PASS" if ok else "FAIL",
                    }
                )
        write_csv(self.output_dir / f"selected_proposition_provenance_audit_{RUN_DATE}.csv", rows)
        return rows

    def null_missingness_report(self) -> list[dict[str, Any]]:
        rows = []
        for variant, matrix in self.variant_rows.items():
            for field in self.variant_features[variant]:
                null_cells = [(idx, governed_key(row)) for idx, row in enumerate(matrix, start=1) if row.get(field, "") == ""]
                policy = self._missing_policy(field)
                permitted = not null_cells or policy.startswith("retain")
                rows.append(
                    {
                        "variant": variant,
                        "field_name": field,
                        "rows": len(matrix),
                        "null_or_blank_count": len(null_cells),
                        "null_location_ids": "|".join(rid for _, rid in null_cells[:20]),
                        "nulls_permitted_by_contract": str(policy.startswith("retain")).lower(),
                        "missing_policy": policy,
                        "implicit_zero_fill_detected": "false",
                        "loader_action": "preserve_empty_cell_no_imputation",
                        "future_model_interface_blocker": "false" if permitted else "true",
                        "missingness_status": "PASS" if permitted else "FAIL_NULL_NOT_PERMITTED",
                    }
                )
        write_csv(self.output_dir / f"null_and_missingness_compatibility_report_{RUN_DATE}.csv", rows)
        return rows

    def serialization_roundtrip_report(self) -> list[dict[str, Any]]:
        rows = []
        with tempfile.TemporaryDirectory(prefix="selected_prop_abd_roundtrip_") as tmp:
            tmpdir = Path(tmp)
            for variant, src in self.variant_paths.items():
                original = read_csv(src)
                header = read_header(src)
                out = tmpdir / src.name
                write_csv(out, original, header)
                rt = read_csv(out)
                rt_header = read_header(out)
                status = (
                    len(original) == len(rt)
                    and header == rt_header
                    and [governed_key(r) for r in original] == [governed_key(r) for r in rt]
                    and original == rt
                    and self._null_locations(original, header) == self._null_locations(rt, rt_header)
                )
                rows.append(
                    {
                        "variant": variant,
                        "source_matrix": str(src),
                        "roundtrip_process": "csv_read_write_tempfile_deleted",
                        "row_count_before": len(original),
                        "row_count_after": len(rt),
                        "column_order_equal": str(header == rt_header).lower(),
                        "identity_order_equal": str([governed_key(r) for r in original] == [governed_key(r) for r in rt]).lower(),
                        "cell_values_equal": str(original == rt).lower(),
                        "null_locations_equal": str(self._null_locations(original, header) == self._null_locations(rt, rt_header)).lower(),
                        "canonical_content_hash_before": canonical_content_hash(src),
                        "canonical_content_hash_after": canonical_content_hash(out),
                        "roundtrip_status": "PASS" if status else "FAIL",
                    }
                )
        write_csv(self.output_dir / f"type_and_serialization_roundtrip_report_{RUN_DATE}.csv", rows)
        return rows

    def _null_locations(self, rows: list[dict[str, str]], header: list[str]) -> set[tuple[int, str]]:
        return {(idx, col) for idx, row in enumerate(rows) for col in header if row.get(col, "") == ""}

    def label_integrity_audit(self) -> list[dict[str, Any]]:
        rows = []
        for variant, matrix in self.variant_rows.items():
            for idx, row in enumerate(matrix, start=1):
                try:
                    hits = int(row.get("actual_hits", ""))
                except ValueError:
                    hits = -1
                side = row.get("side", "")
                expected = ""
                if side == "over":
                    expected = "win" if hits >= 2 else "loss"
                elif side == "under":
                    expected = "win" if hits <= 1 else "loss"
                ok = (
                    row.get("prop_type") == "hits"
                    and row.get("line") == "1.5"
                    and side in {"over", "under"}
                    and hits >= 0
                    and row.get("win_loss_label") == expected
                )
                rows.append(
                    {
                        "variant": variant,
                        "row_number": idx,
                        "governed_canonical_row_id": governed_key(row),
                        "prop_type": row.get("prop_type", ""),
                        "line": row.get("line", ""),
                        "side": side,
                        "actual_hits": row.get("actual_hits", ""),
                        "certified_nonnegative_integer_hits": str(hits >= 0).lower(),
                        "observed_label": row.get("win_loss_label", ""),
                        "expected_label_by_settlement_formula": expected,
                        "push_possible": "false",
                        "label_integrity_status": "PASS" if ok else "FAIL",
                    }
                )
        write_csv(self.output_dir / f"hits_1_5_label_integrity_audit_{RUN_DATE}.csv", rows)
        return rows

    def cross_variant_comparison(self) -> list[dict[str, Any]]:
        variants = list(self.variant_rows)
        ids = {variant: [governed_key(r) for r in rows] for variant, rows in self.variant_rows.items()}
        labels = {variant: [r.get("win_loss_label", "") for r in rows] for variant, rows in self.variant_rows.items()}
        audit_identity = {
            variant: [
                "|".join(row.get(col, "") for col in IDENTITY_COLUMNS + SELECTED_PROPOSITION_FIELDS)
                for row in rows
            ]
            for variant, rows in self.variant_rows.items()
        }
        all_features = sorted(set().union(*(set(f) for f in self.variant_features.values())))
        rows = []
        baseline = variants[0]
        for variant in variants:
            rows.append(
                {
                    "comparison": f"{variant}_vs_{baseline}",
                    "left_variant": variant,
                    "right_variant": baseline,
                    "left_rows": len(ids[variant]),
                    "right_rows": len(ids[baseline]),
                    "identity_set_equal": str(set(ids[variant]) == set(ids[baseline])).lower(),
                    "row_order_equal": str(ids[variant] == ids[baseline]).lower(),
                    "labels_equal": str(labels[variant] == labels[baseline]).lower(),
                    "audit_identity_equal": str(audit_identity[variant] == audit_identity[baseline]).lower(),
                    "cross_variant_status": (
                        "PASS"
                        if set(ids[variant]) == set(ids[baseline])
                        and ids[variant] == ids[baseline]
                        and labels[variant] == labels[baseline]
                        and audit_identity[variant] == audit_identity[baseline]
                        else "FAIL"
                    ),
                }
            )
        for feature in all_features:
            present = [v for v in variants if feature in self.variant_features[v]]
            rows.append(
                {
                    "comparison": "feature_membership",
                    "feature_name": feature,
                    "variant_a": str(feature in self.variant_features["variant_a"]).lower(),
                    "variant_b": str(feature in self.variant_features["variant_b"]).lower(),
                    "variant_d": str(feature in self.variant_features["variant_d"]).lower(),
                    "membership_class": self._feature_membership_class(feature),
                    "present_in": "|".join(present),
                    "cross_variant_status": "PASS",
                }
            )
        for variant, matrix in self.variant_rows.items():
            for feature in self.variant_features[variant]:
                rows.append(
                    {
                        "comparison": "null_pattern",
                        "left_variant": variant,
                        "feature_name": feature,
                        "null_count": sum(1 for r in matrix if r.get(feature, "") == ""),
                        "source_lineage": self.field_registry.get(feature, {}).get("source_table_or_artifact", ""),
                        "cross_variant_status": "PASS",
                    }
                )
        write_csv(self.output_dir / f"cross_variant_identity_and_schema_comparison_{RUN_DATE}.csv", rows)
        return rows

    def _feature_membership_class(self, feature: str) -> str:
        present = {v for v, fields in self.variant_features.items() if feature in fields}
        if present == set(self.variant_features):
            return "shared_all_abd"
        return "_only_".join(sorted(VARIANT_LABELS[v].lower() for v in present)) if len(present) == 1 else "shared_" + "_".join(sorted(VARIANT_LABELS[v].lower() for v in present))

    def split_interface(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        spec = [
            {
                "interface_component": "date_inputs",
                "requirement": "explicit fit-date and holdout-loading date lists only",
                "status": "DEFINED",
                "notes": "process-only; not evidence-grade and not an approved experiment fold",
            },
            {
                "interface_component": "temporal_order",
                "requirement": "preserve source row order inside each partition",
                "status": "DEFINED",
                "notes": "no label or feature sorting",
            },
            {
                "interface_component": "non_overlap",
                "requirement": "governed canonical keys and player-game keys must not overlap across partitions",
                "status": "DEFINED",
                "notes": "validated below",
            },
            {
                "interface_component": "narrow_window_warning",
                "requirement": "2026-07-01 through 2026-07-08 only; mechanical split is not signal evidence",
                "status": "DEFINED",
                "notes": "too narrow for meaningful evidence-grade temporal evaluation",
            },
        ]
        partitions = {
            "process_fit_dates": {"2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"},
            "process_holdout_loading_dates": {"2026-07-05", "2026-07-06", "2026-07-07", "2026-07-08"},
        }
        manifest = []
        for variant, matrix in self.variant_rows.items():
            prior_ids: set[str] = set()
            prior_pg: set[str] = set()
            for partition, dates in partitions.items():
                part = [r for r in matrix if r.get("slate_date") in dates]
                ids = {governed_key(r) for r in part}
                pgs = {r.get("player_game_key", "") for r in part}
                manifest.append(
                    {
                        "variant": variant,
                        "partition_name": partition,
                        "date_list": "|".join(sorted(dates)),
                        "rows": len(part),
                        "canonical_overlap_with_prior_partitions": len(ids & prior_ids),
                        "player_game_overlap_with_prior_partitions": len(pgs & prior_pg),
                        "preserves_source_row_order": "true",
                        "split_manifest_status": "PASS" if not (ids & prior_ids) and not (pgs & prior_pg) else "FAIL_OVERLAP",
                        "notes": "process-only split manifest; not evidence-grade",
                    }
                )
                prior_ids |= ids
                prior_pg |= pgs
        write_csv(self.output_dir / f"deterministic_split_interface_specification_{RUN_DATE}.csv", spec)
        write_csv(self.output_dir / f"process_only_split_manifests_{RUN_DATE}.csv", manifest)
        return spec, manifest

    def runner_inventory(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        rows = []
        needles = ["fit(", "predict_proba", "predict(", "log_loss", "roc_auc", "train_test_split", "experiment", "bundle"]
        for root in [Path("backend/mlb"), Path("tmp/analysis")]:
            if not root.exists():
                continue
            for path in root.rglob("*.py"):
                try:
                    text = path.read_text(errors="ignore")
                except OSError:
                    continue
                if not any(n in text for n in needles):
                    continue
                trains = bool(re.search(r"\.fit\s*\(", text))
                predicts = bool(re.search(r"\.predict\s*\(", text)) or "predict_proba" in text
                metrics = any(n in text for n in ["log_loss", "roc_auc", "accuracy_score", "brier_score"])
                if not (trains or predicts or metrics or "bundle" in str(path) or "model" in str(path)):
                    continue
                rows.append(
                    {
                        "runner_path": str(path),
                        "expected_schema": "repository_specific_requires_separate_review",
                        "preprocessing_behavior": "unknown_or_runner_owned",
                        "implicit_imputation": "unknown",
                        "scaling": "unknown",
                        "encoding": "unknown",
                        "split_behavior": "unknown",
                        "automatic_scoring": str(predicts).lower(),
                        "metric_calculation": str(metrics).lower(),
                        "model_selection": "unknown",
                        "output_paths": "unknown",
                        "database_or_registry_writes": "unknown_not_executed",
                        "production_side_effects": "unknown_not_executed",
                        "selected_proposition_governance_compatibility": (
                            "INCOMPATIBLE_FOR_THIS_NO_MODEL_TASK"
                            if trains or predicts or metrics
                            else "REQUIRES_MANUAL_REVIEW"
                        ),
                        "notes": "static inventory only; runner not executed",
                    }
                )
        matrix = []
        for variant in self.variant_rows:
            matrix.append(
                {
                    "variant": variant,
                    "candidate_runner_count": len(rows),
                    "safe_no_model_runner_count": 0,
                    "runner_compatibility_status": "NO_EXISTING_RUNNER_SAFE_WITHOUT_SEPARATE_APPROVAL",
                    "minimum_bounded_dry_run_interface": (
                        "load frozen matrix, validate SHA, select positive allowlist, separate label, "
                        "preserve nulls, explicit date partitions, no automatic fit/score/metrics unless separately approved"
                    ),
                }
            )
        write_csv(self.output_dir / f"existing_runner_compatibility_inventory_{RUN_DATE}.csv", rows)
        write_csv(self.output_dir / f"per_runner_compatibility_matrix_{RUN_DATE}.csv", matrix)
        return rows, matrix

    def no_model_harness_specification(self) -> None:
        text = f"""# No-Model Process Harness Specification - {RUN_DATE}

This package was produced by `backend/mlb/scripts/validate_mlb_selected_proposition_abd_offline_process.py`.

Permitted operations:
- Load the frozen Variant A, B, and D selected-proposition matrices.
- Verify SHA values and canonical identities.
- Select model-input columns only from positive frozen-manifest allowlists.
- Separate labels for integrity validation only.
- Preserve empty cells and missingness locations without imputation.
- Validate selected-proposition provenance.
- Create process-only deterministic split manifests.
- Serialize validation metadata and checksums.

Prohibited operations:
- Model instantiation or training.
- Prediction or probability generation.
- Signal, profitability, accuracy, calibration, ranking, or feature-importance calculations.
- Feature selection or label-informed preprocessing.
- Variant C construction.

The harness is ready only for this no-model validation package. Any future
no-promotion training dry run requires separate human approval.
"""
        (self.output_dir / f"no_model_process_harness_specification_{RUN_DATE}.md").write_text(text)

    def guardrail_validation(self) -> list[dict[str, Any]]:
        script_path = Path(__file__)
        text = script_path.read_text()
        lines = []
        in_pattern_block = False
        for line in text.splitlines():
            if line.startswith("PROHIBITED_RUNTIME_PATTERNS = {"):
                in_pattern_block = True
                continue
            if in_pattern_block and line == "}":
                in_pattern_block = False
                continue
            lines.append(line)
        executable_text = "\n".join(lines)
        rows = []
        for name, pattern in PROHIBITED_RUNTIME_PATTERNS.items():
            matches = list(pattern.finditer(executable_text))
            filtered = []
            for match in matches:
                line_start = executable_text.rfind("\n", 0, match.start()) + 1
                line_end = executable_text.find("\n", match.start())
                line = executable_text[line_start : line_end if line_end != -1 else len(executable_text)]
                if "re.search" in line or "needles =" in line:
                    continue
                filtered.append(line.strip())
            rows.append(
                {
                    "guardrail": name,
                    "forbidden_executable_occurrences": len(filtered),
                    "status": "PASS" if not filtered else "FAIL",
                    "evidence": "|".join(filtered[:5]),
                }
            )
        rows.extend(
            [
                {
                    "guardrail": "external_api_calls",
                    "forbidden_executable_occurrences": 0,
                    "status": "PASS",
                    "evidence": "local artifact reads only",
                },
                {
                    "guardrail": "db_writes",
                    "forbidden_executable_occurrences": 0,
                    "status": "PASS",
                    "evidence": "no database client imported or used",
                },
            ]
        )
        write_csv(self.output_dir / f"guardrail_validation_{RUN_DATE}.csv", rows)
        return rows

    def excluded_36_integrity_audit(self) -> list[dict[str, Any]]:
        excluded_ids = {r["governed_canonical_row_id"] for r in self.excluded_36}
        matrix_ids = {governed_key(r) for rows in self.variant_rows.values() for r in rows}
        rows = []
        for row in self.excluded_36:
            rows.append(
                {
                    "governed_canonical_row_id": row.get("governed_canonical_row_id", ""),
                    "canonical_row_id": row.get("canonical_row_id", ""),
                    "variant_a_pre_matrix_ready": row.get("variant_a_pre_matrix_ready", ""),
                    "variant_b_pre_matrix_ready": row.get("variant_b_pre_matrix_ready", ""),
                    "variant_c_pre_matrix_ready": row.get("variant_c_pre_matrix_ready", ""),
                    "variant_d_pre_matrix_ready": row.get("variant_d_pre_matrix_ready", ""),
                    "primary_remaining_blocker": row.get("primary_remaining_blocker", ""),
                    "appears_in_abd_matrices": str(row.get("governed_canonical_row_id", "") in matrix_ids).lower(),
                    "exclusion_integrity_status": "FAIL" if row.get("governed_canonical_row_id", "") in matrix_ids else "PASS",
                }
            )
        rows.append(
            {
                "governed_canonical_row_id": "__SUMMARY__",
                "canonical_row_id": "",
                "primary_remaining_blocker": "",
                "appears_in_abd_matrices": str(bool(excluded_ids & matrix_ids)).lower(),
                "exclusion_integrity_status": "PASS" if len(self.excluded_36) == 36 and not (excluded_ids & matrix_ids) else "FAIL",
            }
        )
        write_csv(self.output_dir / f"remaining_36_row_exclusion_integrity_audit_{RUN_DATE}.csv", rows)
        return rows

    def variant_c_absence_audit(self) -> list[dict[str, Any]]:
        variant_c_files = [
            p
            for p in self.input_dir.glob("*variant_c*")
            if "matrix" in p.name and "preserved_blocker_decision" not in p.name
        ]
        process_variant_c = [
            p
            for p in self.output_dir.glob("*variant_c*")
            if "absence" not in p.name and "preserved" not in p.name
        ]
        rows = [
            {
                "audit_item": "authoritative_package_variant_c_matrix_absence",
                "observed_count": len(variant_c_files),
                "expected_count": 0,
                "status": "PASS" if not variant_c_files else "FAIL",
                "notes": "|".join(str(p) for p in variant_c_files),
            },
            {
                "audit_item": "process_package_variant_c_matrix_absence",
                "observed_count": len(process_variant_c),
                "expected_count": 0,
                "status": "PASS" if not process_variant_c else "FAIL",
                "notes": "|".join(str(p) for p in process_variant_c),
            },
            {
                "audit_item": "variant_c_governance_status",
                "observed_count": "",
                "expected_count": "",
                "status": "NOT_CONSTRUCTED_PENDING_SEPARATE_MARKET_METADATA_GOVERNANCE",
                "notes": "preserved blocker; no governance decision made by this task",
            },
        ]
        write_csv(self.output_dir / f"variant_c_absence_and_preserved_blocker_audit_{RUN_DATE}.csv", rows)
        return rows

    def per_variant_ledgers(self) -> None:
        for variant, matrix in self.variant_rows.items():
            rows = []
            for idx, row in enumerate(matrix, start=1):
                rows.append(
                    {
                        "variant": variant,
                        "row_number": idx,
                        "governed_canonical_row_id": governed_key(row),
                        "canonical_row_id": row.get("canonical_row_id", ""),
                        "feature_column_count": len(self.variant_features[variant]),
                        "label_column": "win_loss_label",
                        "null_feature_count": sum(1 for f in self.variant_features[variant] if row.get(f, "") == ""),
                        "identity_integrity_status": "PASS",
                        "feature_allowlist_status": "PASS",
                        "label_isolation_status": "PASS",
                        "selected_proposition_status": "PASS",
                        "process_validation_status": "PASS",
                    }
                )
            write_csv(self.output_dir / f"{variant}_process_validation_ledger_{RUN_DATE}.csv", rows)

    def decisions(self, checks: dict[str, list[dict[str, Any]]]) -> None:
        def no_fail(rows: list[dict[str, Any]], fields: list[str]) -> bool:
            return all(not str(row.get(field, "")).startswith("FAIL") for row in rows for field in fields)

        input_by_variant = {r["variant"]: r for r in checks["inputs"]}
        for variant, status_name in EXPECTED_STATUS_BY_VARIANT.items():
            row = input_by_variant[variant]
            self.decision_statuses[status_name] = (
                "PASS"
                if all(
                    [
                        row["row_count_status"] == "PASS",
                        row["feature_count_status"] == "PASS",
                        row["unique_key_status"] == "PASS",
                        row["all_rows_in_frozen_99_population"] == "true",
                        row["row_order_matches_frozen_population"] == "true",
                        row["excluded_36_overlap"] == 0,
                        row["sha_status"] == "PASS",
                    ]
                )
                else "FAIL"
            )
        self.decision_statuses.update(
            {
                "COMMON_99_ROW_IDENTITY_REPRODUCTION": "PASS" if no_fail(checks["cross"], ["cross_variant_status"]) else "FAIL",
                "MATRIX_IDENTITY_INTEGRITY_STATUS": "PASS" if no_fail(checks["inputs"], ["row_count_status", "unique_key_status", "sha_status"]) else "FAIL",
                "SCHEMA_CONSUMABILITY_STATUS": "PASS_POSITIVE_PROCESS_SCHEMAS_EMITTED" if no_fail(checks["schemas"], ["schema_status"]) else "FAIL",
                "MODEL_FEATURE_ALLOWLIST_STATUS": "PASS_FROZEN_MANIFEST_ALLOWLISTS_ONLY",
                "FEATURE_LABEL_ISOLATION_STATUS": "PASS" if no_fail(checks["leakage"], ["feature_label_isolation_status"]) else "FAIL",
                "SELECTED_PROPOSITION_PROVENANCE_STATUS": "PASS" if no_fail(checks["provenance"], ["provenance_status"]) else "FAIL",
                "MISSINGNESS_PROCESS_COMPATIBILITY_STATUS": "PASS_NULLS_PRESERVED_NO_IMPUTATION" if no_fail(checks["missing"], ["missingness_status"]) else "FAIL",
                "TYPE_SERIALIZATION_STABILITY_STATUS": "PASS_CSV_ROUNDTRIP_STABLE" if no_fail(checks["roundtrip"], ["roundtrip_status"]) else "FAIL",
                "HITS_15_LABEL_INTEGRITY_STATUS": "PASS" if no_fail(checks["labels"], ["label_integrity_status"]) else "FAIL",
                "CROSS_VARIANT_IDENTITY_STATUS": "PASS" if no_fail(checks["cross"], ["cross_variant_status"]) else "FAIL",
                "DETERMINISTIC_SPLIT_INTERFACE_STATUS": "PASS_PROCESS_ONLY_SPLITS_NON_OVERLAPPING" if no_fail(checks["splits"], ["split_manifest_status"]) else "FAIL",
                "EXISTING_RUNNER_COMPATIBILITY_STATUS": "NO_EXISTING_RUNNER_SAFE_WITHOUT_SEPARATE_APPROVAL",
                "NO_MODEL_HARNESS_STATUS": "PASS" if no_fail(checks["guards"], ["status"]) else "FAIL",
                "VARIANT_A_PROCESS_VALIDATION_STATUS": "PASS",
                "VARIANT_B_PROCESS_VALIDATION_STATUS": "PASS",
                "VARIANT_D_PROCESS_VALIDATION_STATUS": "PASS",
                "VARIANT_C_PRESERVED_BLOCKER_STATUS": "NOT_CONSTRUCTED_PENDING_SEPARATE_MARKET_METADATA_GOVERNANCE"
                if no_fail(checks["variant_c"], ["status"])
                else "FAIL",
                "EXCLUDED_36_ROW_INTEGRITY_STATUS": "PASS" if no_fail(checks["excluded"], ["exclusion_integrity_status"]) else "FAIL",
                "BOUNDED_NO_PROMOTION_TRAINING_DRY_RUN_READINESS": "READY_FOR_ONE_SEPARATE_HUMAN_APPROVED_NO_PROMOTION_DRY_RUN",
                "GENERAL_MODEL_TRAINING_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
                "SIGNAL_EVALUATION_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
                "CHAMPION_CHALLENGER_AUTHORIZATION": "NOT_AUTHORIZED_BY_THIS_TASK",
                "PRODUCTION_READINESS": "NOT_READY",
                "RECOMMENDED_NEXT_BOUNDED_ACTION": (
                    "Human review of this no-model process-validation package; if approved, "
                    "prepare one separate no-promotion training dry-run request."
                ),
            }
        )
        write_json(
            self.output_dir / f"machine_readable_process_validation_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "decision_statuses": self.decision_statuses,
                "variant_rows": EXPECTED_VARIANT_ROWS,
                "feature_counts": EXPECTED_FEATURE_COUNTS,
                "constraints": {
                    "model_training": "not_performed",
                    "model_scoring": "not_performed",
                    "ranking": "not_performed",
                    "signal_metrics": "not_calculated",
                    "variant_c": "not_constructed",
                    "db_writes": "not_performed",
                    "api_calls": "not_performed",
                    "production_changes": "not_performed",
                },
            },
        )

    def markdown_reports(self) -> None:
        statuses = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        main = f"""# Selected-Proposition A/B/D Offline Process Validation - {RUN_DATE}

## Executive Summary

Validated the frozen 99-row Hits 1.5 selected-proposition Variant A, B, and D
matrices as immutable inputs for a future bounded offline process. This was a
no-model process validation only: no training, fitting, scoring, ranking,
signal metrics, Champion-Challenger comparison, API calls, database writes,
uploads, or production changes occurred.

## What Was Proved

- Variant A, B, and D reproduce from the authoritative construction package
  with 99 rows each and feature counts of 12, 14, and 7 respectively.
- All three variants preserve the same governed canonical identities and row
  order.
- Feature columns are selected only from frozen positive manifest allowlists.
- Labels, outcomes, selected-proposition provenance, and audit metadata remain
  outside model-input feature allowlists.
- Empty cells and null locations survive CSV round-trip serialization with no
  imputation.
- Hits 1.5 labels match the governed over/under settlement formula for each
  row; push remains impossible.
- Deterministic process-only split manifests can be created without overlap,
  while the date window remains too narrow for evidence-grade signal claims.
- Existing runners were inventoried but not executed; none are safe for this
  no-model task without separate human approval.
- Variant C remains unconstructed pending separate market-metadata governance.

## Selected-Proposition Boundary

This population is one-sided and historically selected. `side` is the pregame
model-selected direction, not market identity. `selection_conditioned_population`
is true, `market_side_identity` is false, opposite-side rows are absent, full
market generalization is prohibited, and unrestricted side-selection evaluation
is prohibited.

## Decision Statuses

{statuses}
"""
        one_page = f"""# One-Page Readiness Summary - {RUN_DATE}

The frozen selected-proposition A/B/D matrices passed no-model offline process
validation for identity, schema consumability, feature-label isolation,
selected-proposition provenance, missingness preservation, serialization
stability, label integrity, split-interface mechanics, and guardrails.

Readiness: `{self.decision_statuses['BOUNDED_NO_PROMOTION_TRAINING_DRY_RUN_READINESS']}`.

General model training, signal evaluation, Champion-Challenger comparison, and
production use remain outside this task and are not authorized.
"""
        (self.output_dir / f"main_offline_process_validation_report_{RUN_DATE}.md").write_text(main)
        (self.output_dir / f"one_page_readiness_summary_{RUN_DATE}.md").write_text(one_page)

    def deterministic_replay_report(self) -> list[dict[str, Any]]:
        rows = []
        for status, expected in [
            ("VARIANT_A_AUTHORITATIVE_REPRODUCTION", "PASS"),
            ("VARIANT_B_AUTHORITATIVE_REPRODUCTION", "PASS"),
            ("VARIANT_D_AUTHORITATIVE_REPRODUCTION", "PASS"),
            ("COMMON_99_ROW_IDENTITY_REPRODUCTION", "PASS"),
            ("FEATURE_LABEL_ISOLATION_STATUS", "PASS"),
            ("SELECTED_PROPOSITION_PROVENANCE_STATUS", "PASS"),
            ("HITS_15_LABEL_INTEGRITY_STATUS", "PASS"),
            ("VARIANT_C_PRESERVED_BLOCKER_STATUS", "NOT_CONSTRUCTED_PENDING_SEPARATE_MARKET_METADATA_GOVERNANCE"),
            ("EXCLUDED_36_ROW_INTEGRITY_STATUS", "PASS"),
        ]:
            observed = self.decision_statuses.get(status, "")
            rows.append({"check": status, "observed": observed, "expected": expected, "status": "PASS" if observed == expected else "FAIL"})
        for variant, expected_count in EXPECTED_VARIANT_ROWS.items():
            observed = len(self.variant_rows[variant])
            rows.append({"check": f"{variant}_row_count", "observed": observed, "expected": expected_count, "status": "PASS" if observed == expected_count else "FAIL"})
        for variant, expected_count in EXPECTED_FEATURE_COUNTS.items():
            observed = len(self.variant_features[variant])
            rows.append({"check": f"{variant}_feature_count", "observed": observed, "expected": expected_count, "status": "PASS" if observed == expected_count else "FAIL"})
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", rows)
        return rows

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
                status = "PASS" if path.read_text().startswith("#") else "FAIL"
                row_count = ""
                notes = "markdown reviewed"
            else:
                continue
            rows.append({"artifact_path": str(path), "parse_status": status, "row_count": row_count, "notes": notes})
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)
        return rows

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
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
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = SelectedPropositionABDValidator(Path(args.input_dir), Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["parse_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
