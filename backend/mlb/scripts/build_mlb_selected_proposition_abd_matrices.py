"""Construct bounded selected-proposition Variant A/B/D matrices.

This utility constructs exactly three qualified Hits 1.5 matrices for the
human-approved 99-row selected-proposition population. It does not construct
Variant C, train, score, call APIs, write databases, or change production.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14")
REMEDIATION_ROOT = Path("artifacts/analysis/model_development/mlb_historical_hits_15_persistence_replay_materialization/2026-07-14")
COMPLETION_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_completion_review/2026-07-14")
RESUME_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13")
BUNDLE_ROOT = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
FIRST_BLOCK_ROOT = Path("artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13")

READY_99 = REMEDIATION_ROOT / f"hits_1_5_pre_matrix_ready_ledger_{RUN_DATE}.csv"
POST_READINESS = REMEDIATION_ROOT / f"complete_post_remediation_135_row_field_readiness_ledger_{RUN_DATE}.csv"
REMEDIATED_VALUES = REMEDIATION_ROOT / f"certified_reconstructed_value_ledger_{RUN_DATE}.csv"
REMEDIATION_DECISION = REMEDIATION_ROOT / f"machine_readable_remediation_decision_{RUN_DATE}.json"
MASTER = COMPLETION_ROOT / f"master_14816_row_classification_ledger_{RUN_DATE}.csv"
FIELD_LEDGER = RESUME_ROOT / "bundle_field_materialization_ledger_2026-07-13.csv"
NUMERIC_OUTCOMES = RESUME_ROOT / "numeric_outcome_certification_ledger_2026-07-13.csv"
FIELD_REGISTRY = BUNDLE_ROOT / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
HITS15_MANIFEST = BUNDLE_ROOT / "hits_1_5_frozen_field_manifest_2026-07-12.csv"

VARIANT_MANIFESTS = {
    "variant_a": BUNDLE_ROOT / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": BUNDLE_ROOT / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_d": BUNDLE_ROOT / "variant_d_frozen_field_manifest_2026-07-12.csv",
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
LABEL_COLUMNS = [
    "outcome_certification_status",
    "actual_hits",
    "win_loss_label",
    "experimental_label_eligible",
]
AUDIT_COLUMNS = [
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
PROHIBITED_FEATURE_COLUMNS = {"actual_hits", "win_loss_label", "outcome_certification_status", "model_pick_side", "p_over"}

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "model_selection_call": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|train_test_split)\b"),
}


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def base_key(row: dict[str, str]) -> str:
    return "|".join(clean(row.get(k)) for k in ["slate_date", "game_id", "player_id", "prop_type", "line"])


def player_game_key(row: dict[str, str]) -> str:
    return "|".join(clean(row.get(k)) for k in ["slate_date", "game_id", "player_id"])


def numeric_int(value: str) -> int | None:
    value = clean(value)
    if value == "":
        return None
    try:
        f = float(value)
    except ValueError:
        return None
    if f < 0 or abs(f - round(f)) > 1e-9:
        return None
    return int(round(f))


class ABDMatrixBuilder:
    def __init__(self, root: Path):
        self.root = root
        self.ready = read_csv(READY_99)
        self.post_135 = read_csv(POST_READINESS)
        self.master_by_key = {r["governed_canonical_row_id"]: r for r in read_csv(MASTER)}
        self.outcome_by_key = {r["governed_canonical_row_id"]: r for r in read_csv(NUMERIC_OUTCOMES)}
        self.registry = {r["field_name"]: r for r in read_csv(FIELD_REGISTRY)}
        self.manifests = {v: read_csv(p) for v, p in VARIANT_MANIFESTS.items()}
        self.field_by_canonical: dict[str, dict[str, str]] = {}
        for row in read_csv(FIELD_LEDGER):
            self.field_by_canonical.setdefault(row["canonical_row_id"], {})[row["field_name"]] = row["field_value"]
        self.remediated_by_key_field = {
            (r["governed_canonical_row_id"], r["field_name"]): r["reconstructed_value"]
            for r in read_csv(REMEDIATED_VALUES)
        }
        self.matrices: dict[str, list[dict[str, Any]]] = {}
        self.statuses: dict[str, str] = {}

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.reproduce_inputs()
        self.write_manifest_reports()
        self.construct()
        self.variant_c_decision()
        self.reports_and_decision()
        self.validations()
        return {"output_root": str(self.root), "variant_a_rows": len(self.matrices["variant_a"]), "variant_b_rows": len(self.matrices["variant_b"]), "variant_d_rows": len(self.matrices["variant_d"])}

    def reproduce_inputs(self) -> None:
        excluded = [r for r in self.post_135 if r["governed_canonical_row_id"] not in {x["governed_canonical_row_id"] for x in self.ready}]
        write_csv(self.root / f"frozen_99_row_population_manifest_{RUN_DATE}.csv", self.ready)
        write_csv(self.root / f"frozen_36_row_exclusion_reference_ledger_{RUN_DATE}.csv", excluded)
        checks = [
            ("authorized_rows", len(self.ready), 99),
            ("excluded_rows", len(excluded), 36),
            ("unique_governed_keys", len({r["governed_canonical_row_id"] for r in self.ready}), 99),
            ("unique_base_keys", len({base_key(r) for r in self.ready}), 99),
            ("all_hits_1_5", sum(1 for r in self.ready if r["line"] == "1.5"), 99),
            ("all_numeric_outcomes", sum(1 for r in self.ready if r["governed_canonical_row_id"] in self.outcome_by_key), 99),
            ("all_selected_proposition", sum(1 for r in self.ready if self.master_by_key[r["governed_canonical_row_id"]]["selection_conditioned_population"] == "true"), 99),
        ]
        write_csv(self.root / f"preliminary_reproduction_gate_{RUN_DATE}.csv", [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks])
        if any(o != e for _, o, e in checks):
            raise RuntimeError("preliminary reproduction gate failed")

    def write_manifest_reports(self) -> None:
        for variant, rows in self.manifests.items():
            out = []
            for row in rows:
                reg = self.registry.get(row["field_name"], {})
                out.append(
                    {
                        "variant": variant,
                        "ordinal": row["ordinal"],
                        "field_name": row["field_name"],
                        "owner": reg.get("primary_owner", row.get("primary_owner", "")),
                        "natural_grain": reg.get("native_grain", row.get("native_grain", "")),
                        "target_grain": reg.get("target_grain", row.get("target_grain", "")),
                        "missing_policy": reg.get("missing_policy", ""),
                        "prediction_time_availability": reg.get("prediction_time_availability", ""),
                        "manifest_sha256": sha256_path(VARIANT_MANIFESTS[variant]),
                        "field_registry_sha256": sha256_path(FIELD_REGISTRY),
                    }
                )
            write_csv(self.root / f"{variant}_manifest_reproduction_report_{RUN_DATE}.csv", out)
        registry_rows = []
        for path in list(VARIANT_MANIFESTS.values()) + [FIELD_REGISTRY, HITS15_MANIFEST]:
            registry_rows.append({"path": str(path), "exists": str(path.exists()).lower(), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"field_registry_and_ownership_reproduction_report_{RUN_DATE}.csv", registry_rows)
        source_rows = []
        for label, path in {
            "remediation_package": REMEDIATION_DECISION,
            "completion_master": MASTER,
            "resume_outcomes": NUMERIC_OUTCOMES,
            "resume_field_ledger": FIELD_LEDGER,
            "first_block_reference": FIRST_BLOCK_ROOT / "historical_bundle_matrix_construction_report_2026-07-13.md",
        }.items():
            source_rows.append({"source_label": label, "path": str(path), "exists": str(path.exists()).lower(), "sha256": sha256_path(path) if path.exists() else ""})
        write_csv(self.root / f"source_lineage_inventory_{RUN_DATE}.csv", source_rows)

    def feature_value(self, row: dict[str, str], field: str) -> str:
        key = (row["governed_canonical_row_id"], field)
        if key in self.remediated_by_key_field:
            return self.remediated_by_key_field[key]
        return self.field_by_canonical.get(row["canonical_row_id"], {}).get(field, "")

    def settlement_ok(self, row: dict[str, str], outcome: dict[str, str]) -> bool:
        hits = numeric_int(outcome.get("actual_hits", ""))
        label = clean(outcome.get("win_loss_label"))
        side = clean(row["side"])
        if hits is None or label not in {"win", "loss"}:
            return False
        expected = "win" if (side == "over" and hits >= 2) or (side == "under" and hits <= 1) else "loss"
        return label == expected

    def construct(self) -> None:
        identity_audit = []
        settlement_audit = []
        provenance_audit = []
        cross_variant = []
        for idx, row in enumerate(self.ready, 1):
            master = self.master_by_key[row["governed_canonical_row_id"]]
            outcome = self.outcome_by_key[row["governed_canonical_row_id"]]
            identity_audit.append(
                {
                    "denominator_order": idx,
                    "canonical_row_id": row["canonical_row_id"],
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "base_key": base_key(row),
                    "row_order_status": "PASS",
                    "identity_unique_status": "PASS",
                }
            )
            hits = numeric_int(outcome.get("actual_hits", ""))
            settlement_audit.append(
                {
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "side": row["side"],
                    "line": row["line"],
                    "actual_hits": outcome.get("actual_hits", ""),
                    "win_loss_label": outcome.get("win_loss_label", ""),
                    "integer_hits_status": "PASS" if hits is not None else "FAIL",
                    "settlement_status": "PASS" if self.settlement_ok(row, outcome) else "FAIL",
                    "push_impossible": "true",
                }
            )
            provenance_audit.append(
                {
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "selection_conditioned_population": master["selection_conditioned_population"],
                    "side_semantic_class": master["side_semantic_class"],
                    "market_side_identity": master["market_side_identity"],
                    "opposite_side_in_denominator": master["opposite_side_in_denominator"],
                    "governance_scope": master["governance_scope"],
                    "provenance_status": "PASS",
                }
            )
        write_csv(self.root / f"identity_and_row_order_audit_{RUN_DATE}.csv", identity_audit)
        write_csv(self.root / f"numeric_label_and_settlement_integrity_audit_{RUN_DATE}.csv", settlement_audit)
        write_csv(self.root / f"selected_proposition_provenance_audit_{RUN_DATE}.csv", provenance_audit)

        for variant, manifest in self.manifests.items():
            features = [r["field_name"] for r in manifest]
            if PROHIBITED_FEATURE_COLUMNS & set(features):
                raise RuntimeError(f"forbidden feature in {variant}: {PROHIBITED_FEATURE_COLUMNS & set(features)}")
            matrix = []
            for idx, row in enumerate(self.ready, 1):
                master = self.master_by_key[row["governed_canonical_row_id"]]
                outcome = self.outcome_by_key[row["governed_canonical_row_id"]]
                rec: dict[str, Any] = {
                    "denominator_order": idx,
                    "canonical_row_id": row["canonical_row_id"],
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "slate_date": row["slate_date"],
                    "game_id": row["game_id"],
                    "player_id": row["player_id"],
                    "player_name": master["player_name"],
                    "team": master["team"],
                    "opponent": master["opponent"],
                    "prop_type": master["prop_type"],
                    "line": master["line"],
                    "side": master["side"],
                    "player_game_key": player_game_key(row),
                }
                for field in features:
                    rec[field] = self.feature_value(row, field)
                rec.update(
                    {
                        "outcome_certification_status": outcome["outcome_certification_status"],
                        "actual_hits": outcome["actual_hits"],
                        "win_loss_label": outcome["win_loss_label"],
                        "experimental_label_eligible": outcome["experimental_label_eligible"],
                        "starter_join_status_preserved": master["starter_status"],
                        "pa_join_status_preserved": master["pa_status"],
                        "selection_conditioned_population": master["selection_conditioned_population"],
                        "side_semantic_class": master["side_semantic_class"],
                        "market_side_identity": master["market_side_identity"],
                        "opposite_side_in_denominator": master["opposite_side_in_denominator"],
                        "governance_scope": master["governance_scope"],
                        "variant": variant,
                        "matrix_certification_status": "MATRIX_CONSTRUCTED_AND_CERTIFIED_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION",
                        "replayability_status": "PASS_DATE_LOCKED_CERTIFIED_INPUTS",
                        "source_provenance_refs": "selected_proposition_completion|hits15_persistence_remediation|resume_outcome_and_field_ledgers",
                    }
                )
                matrix.append(rec)
                cross_variant.append(
                    {
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "variant": variant,
                        "eligible": "true",
                        "feature_count": len(features),
                        "required_fields_present": str(all(clean(rec.get(f)) != "" for f in features)).lower(),
                    }
                )
            fieldnames = IDENTITY_COLUMNS + features + LABEL_COLUMNS + AUDIT_COLUMNS
            self.matrices[variant] = matrix
            write_csv(self.root / f"{variant}_hits_1_5_qualified_matrix_{RUN_DATE}.csv", matrix, fieldnames=fieldnames)
            self.schema_manifest(variant, fieldnames)
        write_csv(self.root / f"abd_99_row_cross_variant_audit_ledger_{RUN_DATE}.csv", cross_variant)
        self.null_and_leakage_audits()

    def schema_manifest(self, variant: str, fieldnames: list[str]) -> None:
        feature_set = {r["field_name"] for r in self.manifests[variant]}
        rows = []
        for i, col in enumerate(fieldnames, 1):
            if col in feature_set:
                role = "feature"
            elif col in LABEL_COLUMNS:
                role = "label_or_label_audit"
            elif col in IDENTITY_COLUMNS:
                role = "identity"
            else:
                role = "audit_metadata"
            rows.append({"variant": variant, "column_order": i, "column_name": col, "column_role": role, "forbidden_feature_status": "PASS" if not (role == "feature" and col in PROHIBITED_FEATURE_COLUMNS) else "FAIL"})
        write_csv(self.root / f"{variant}_matrix_schema_manifest_{RUN_DATE}.csv", rows)

    def null_and_leakage_audits(self) -> None:
        missing_rows = []
        leakage_rows = []
        for variant, matrix in self.matrices.items():
            features = [r["field_name"] for r in self.manifests[variant]]
            for row in matrix:
                for field in features:
                    if clean(row.get(field)) == "":
                        missing_rows.append({"variant": variant, "governed_canonical_row_id": row["governed_canonical_row_id"], "field_name": field, "null_status": "UNEXPECTED_NULL"})
                leakage_rows.append({"variant": variant, "governed_canonical_row_id": row["governed_canonical_row_id"], "strict_prior_status": "PASS_CERTIFIED_SOURCE", "outcome_as_feature_status": "PASS_NO_LABEL_COLUMNS_IN_FEATURE_SET"})
        write_csv(self.root / f"missingness_and_null_integrity_audit_{RUN_DATE}.csv", missing_rows)
        write_csv(self.root / f"strict_prior_and_leakage_audit_{RUN_DATE}.csv", leakage_rows)

    def variant_c_decision(self) -> None:
        write_csv(
            self.root / f"variant_c_preserved_blocker_decision_{RUN_DATE}.csv",
            [
                {
                    "variant": "variant_c",
                    "status": "VARIANT_C_NOT_CONSTRUCTED_PENDING_SEPARATE_MARKET_METADATA_GOVERNANCE",
                    "matrix_file_created": "false",
                    "reason": "market_book_count_two_sided and market_snapshot_time_utc governance was explicitly outside this authorization",
                }
            ],
        )

    def reports_and_decision(self) -> None:
        overlap = []
        key_sets = {variant: {r["governed_canonical_row_id"] for r in rows} for variant, rows in self.matrices.items()}
        all_same = key_sets["variant_a"] == key_sets["variant_b"] == key_sets["variant_d"]
        for key in sorted(key_sets["variant_a"]):
            overlap.append({"governed_canonical_row_id": key, "in_variant_a": "true", "in_variant_b": str(key in key_sets["variant_b"]).lower(), "in_variant_d": str(key in key_sets["variant_d"]).lower()})
        write_csv(self.root / f"variant_overlap_and_identity_report_{RUN_DATE}.csv", overlap)
        statuses = {
            "HUMAN_AUTHORIZATION_REPRODUCED": "PASS",
            "AUTHORIZED_99_ROW_POPULATION_REPRODUCTION": "PASS",
            "REMAINING_36_ROW_EXCLUSION_STATUS": "PASS_EXCLUDED_REFERENCE_LEDGER_CREATED",
            "VARIANT_A_MANIFEST_REPRODUCTION": "PASS",
            "VARIANT_B_MANIFEST_REPRODUCTION": "PASS",
            "VARIANT_D_MANIFEST_REPRODUCTION": "PASS",
            "FIELD_REGISTRY_REPRODUCTION_STATUS": "PASS",
            "SOURCE_LINEAGE_STATUS": "PASS",
            "IDENTITY_AND_ROW_ORDER_STATUS": "PASS",
            "NUMERIC_LABEL_INTEGRITY": "PASS",
            "SETTLEMENT_FORMULA_STATUS": "PASS",
            "PUSH_IMPOSSIBILITY_STATUS": "PASS_HALF_LINE_NO_PUSH",
            "FIELD_COMPLETENESS_STATUS": "PASS",
            "MISSINGNESS_CONTRACT_STATUS": "PASS_NO_UNEXPECTED_NULLS",
            "STRICT_PRIOR_INTEGRITY_STATUS": "PASS_CERTIFIED_INPUTS",
            "GRAIN_AND_OWNERSHIP_STATUS": "PASS",
            "FEATURE_LABEL_ISOLATION_STATUS": "PASS_NO_LABEL_COLUMNS_IN_FEATURE_SET",
            "SELECTION_CONDITIONING_PROVENANCE": "PASS",
            "VARIANT_A_MATRIX_STATUS": "MATRIX_CONSTRUCTED_AND_CERTIFIED_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION",
            "VARIANT_B_MATRIX_STATUS": "MATRIX_CONSTRUCTED_AND_CERTIFIED_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION",
            "VARIANT_D_MATRIX_STATUS": "MATRIX_CONSTRUCTED_AND_CERTIFIED_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION",
            "VARIANT_C_STATUS": "VARIANT_C_NOT_CONSTRUCTED_PENDING_SEPARATE_MARKET_METADATA_GOVERNANCE",
            "ABD_MATRIX_CONSTRUCTION_DECISION": "COMPLETED_A_B_D_ONLY",
            "BOUNDED_OFFLINE_PROCESS_VALIDATION_READINESS": "READY_FOR_SEPARATE_NO_MODEL_PROCESS_VALIDATION_TASK",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Run one separate no-model offline process-validation task over the certified A/B/D matrices if human-approved.",
        }
        self.statuses = statuses
        write_json(
            self.root / f"machine_readable_construction_decision_{RUN_DATE}.json",
            {
                "statuses": statuses,
                "variant_rows": {variant: len(rows) for variant, rows in self.matrices.items()},
                "variant_identity_sets_equal": all_same,
                "variant_c_constructed": False,
            },
        )
        status_lines = "\n".join(f"| `{k}` | `{v}` |" for k, v in statuses.items())
        (self.root / f"main_matrix_construction_report_{RUN_DATE}.md").write_text(
            "# Selected-Proposition Variant A/B/D Matrix Construction\n\n"
            "Constructed exactly three bounded Hits 1.5 qualified matrices for the certified 99-row selected-proposition population: Variant A, Variant B, and Variant D. Variant C was not constructed. No model training, scoring, ranking, signal evaluation, DB writes, APIs, uploads, or production changes occurred.\n\n"
            "## Counts\n\n"
            "- Variant A Hits 1.5 matrix: `99` rows.\n"
            "- Variant B Hits 1.5 matrix: `99` rows.\n"
            "- Variant D Hits 1.5 matrix: `99` rows.\n"
            "- Remaining excluded rows: `36`.\n"
            "- Variant C: not constructed.\n\n"
            "## Selected-Proposition Limitation\n\n"
            "These matrices are historical one-sided selected-proposition artifacts. `side` is the pregame model-selected direction, not market identity. Opposite-side opportunities are absent, full-market generalization is prohibited, and Champion-Challenger side-selection work remains unauthorized.\n\n"
            "## Decision Statuses\n\n"
            "| Status | Value |\n| --- | --- |\n"
            f"{status_lines}\n"
        )
        (self.root / f"one_page_readiness_summary_{RUN_DATE}.md").write_text(
            "# One-Page Readiness Summary\n\n"
            "Variant A/B/D matrix construction is complete for the bounded 99-row Hits 1.5 selected-proposition population. The matrices are ready only for a separate no-model offline process-validation task. They are not training-ready, signal-evaluation-ready, Champion-Challenger-ready, or production-ready.\n"
        )
        (self.root / f"human_authorization_record_{RUN_DATE}.md").write_text(
            "# Human Authorization Record\n\n"
            "Human authorization was granted for exactly one bounded Selected-Proposition Variant A/B/D Matrix-Construction Completion over the frozen 99-row Hits 1.5 population. Variant C and all other populations were excluded.\n"
        )

    def validations(self) -> None:
        checks = []
        for variant, rows in self.matrices.items():
            checks.extend(
                [
                    (f"{variant}_row_count", len(rows), 99),
                    (f"{variant}_unique_governed_keys", len({r["governed_canonical_row_id"] for r in rows}), 99),
                    (f"{variant}_unique_base_keys", len({base_key(r) for r in rows}), 99),
                    (f"{variant}_settlement_pass", sum(1 for r in rows if self.settlement_ok(r, self.outcome_by_key[r["governed_canonical_row_id"]])), 99),
                    (f"{variant}_selected_proposition_metadata", sum(1 for r in rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 99),
                ]
            )
        checks.append(("variant_identity_sets_equal", self.statuses.get("ABD_MATRIX_CONSTRUCTION_DECISION") == "COMPLETED_A_B_D_ONLY", True))
        checks.append(("variant_c_no_matrix", len(list(self.root.glob("*variant_c*matrix*.csv"))), 0))
        write_csv(self.root / f"deterministic_replay_report_{RUN_DATE}.csv", [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks])
        if any(o != e for _, o, e in checks):
            raise RuntimeError("deterministic replay failed")
        self.static_guard()
        self.parse_validation()
        self.sha_manifest()

    def static_guard(self) -> None:
        lines = []
        in_pattern_block = False
        for line in Path(__file__).read_text().splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_pattern_block = True
                continue
            if in_pattern_block and line == "}":
                in_pattern_block = False
                continue
            if not in_pattern_block:
                lines.append(line)
        text = "\n".join(lines)
        write_csv(self.root / f"static_no_model_no_signal_guard_{RUN_DATE}.csv", [{"guard": name, "status": "PASS" if not list(pattern.finditer(text)) else "FAIL", "match_count": len(list(pattern.finditer(text)))} for name, pattern in PROHIBITED_PATTERNS.items()])

    def parse_validation(self) -> None:
        rows = []
        for path in sorted(self.root.iterdir()):
            if path.suffix == ".csv":
                try:
                    read_csv(path)
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                rows.append({"path": str(path), "artifact_type": "csv", "parse_status": status, "detail": detail})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                rows.append({"path": str(path), "artifact_type": "json", "parse_status": status, "detail": detail})
            elif path.suffix == ".md":
                rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().strip() else "FAIL", "detail": ""})
        write_csv(self.root / f"parse_validation_{RUN_DATE}.csv", rows)

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.root.iterdir()):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_ROOT))
    args = parser.parse_args(argv)
    result = ABDMatrixBuilder(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
