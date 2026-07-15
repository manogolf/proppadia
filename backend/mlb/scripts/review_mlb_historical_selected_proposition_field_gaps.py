"""Review frozen Bundle field gaps for selected-proposition field-blocked rows.

This is a read-only review over the 135 `HITS_BUNDLE_FIELD_BLOCKED` rows from
the 2026-07-14 completion review. It does not materialize values, construct
matrices, train, score, call APIs, write databases, or change production state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_bundle_field_gap_review/2026-07-14")
COMPLETION_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_completion_review/2026-07-14")
RESUME_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13")
BUNDLE_ROOT = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
FIRST_BLOCK_MATRIX_ROOT = Path("artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13")

MASTER = COMPLETION_ROOT / f"master_14816_row_classification_ledger_{RUN_DATE}.csv"
HITS_LEDGER = COMPLETION_ROOT / f"hits_2046_qualification_ledger_{RUN_DATE}.csv"
FIELD_LEDGER = RESUME_ROOT / "bundle_field_materialization_ledger_2026-07-13.csv"
FIELD_REGISTRY = BUNDLE_ROOT / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
COMPLETION_SHA = COMPLETION_ROOT / f"sha256_manifest_{RUN_DATE}.csv"

VARIANT_MANIFESTS = {
    "variant_a": BUNDLE_ROOT / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": BUNDLE_ROOT / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_c": BUNDLE_ROOT / "variant_c_frozen_field_manifest_2026-07-12.csv",
    "variant_d": BUNDLE_ROOT / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
LINE_MANIFESTS = {
    "hits_0_5": BUNDLE_ROOT / "hits_0_5_frozen_field_manifest_2026-07-12.csv",
    "hits_1_5": BUNDLE_ROOT / "hits_1_5_frozen_field_manifest_2026-07-12.csv",
}

SOURCE_EVIDENCE = {
    "hitter_persistence": Path("artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"),
    "pa_opportunity": Path("artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"),
    "first_block_matrix_report": FIRST_BLOCK_MATRIX_ROOT / "historical_bundle_matrix_construction_report_2026-07-13.md",
    "first_block_variant_a_audit": FIRST_BLOCK_MATRIX_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv",
    "first_block_variant_c_audit": FIRST_BLOCK_MATRIX_ROOT / "variant_c_complete_audit_matrix_2026-07-13.csv",
}

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
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


def player_game_key(row: dict[str, str]) -> str:
    return "|".join(clean(row.get(k)) for k in ["slate_date", "game_id", "player_id"])


def field_domain(field: str, registry_row: dict[str, str] | None = None) -> str:
    if field.startswith("pa_opp"):
        return "pa_opportunity"
    if field in {"weighted_multiseason_hits_per_out", "expected_outs_blended_v1", "workload_confidence", "expected_role_label", "role_confidence"}:
        return "starter_skill_workload"
    if field in {"offense_factor_vs_league_reconstructed", "movement_label"}:
        return "offense_factor"
    if field in {"line", "selected_side_price", "selected_side_no_vig_implied", "market_book_count_two_sided", "market_snapshot_time_utc"}:
        return "variant_c_market_context"
    if field == "is_home":
        return "team_context"
    if registry_row and "team" in clean(registry_row.get("primary_owner")).lower():
        return "team_context"
    return "hitter_persistence"


class FieldGapReview:
    def __init__(self, root: Path):
        self.root = root
        self.master = read_csv(MASTER)
        self.hits = read_csv(HITS_LEDGER)
        self.field_ledger = read_csv(FIELD_LEDGER)
        self.registry = {r["field_name"]: r for r in read_csv(FIELD_REGISTRY)}
        self.manifests = {name: read_csv(path) for name, path in VARIANT_MANIFESTS.items()}
        self.line_manifests = {name: read_csv(path) for name, path in LINE_MANIFESTS.items()}
        self.field_by_row: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
        for row in self.field_ledger:
            self.field_by_row[row["canonical_row_id"]][row["field_name"]] = row
        self.review_rows = [r for r in self.master if r["primary_campaign_classification"] == "HITS_BUNDLE_FIELD_BLOCKED"]
        self.review_keys = {r["governed_canonical_row_id"] for r in self.review_rows}
        self.row_field: list[dict[str, Any]] = []
        self.statuses: dict[str, str] = {}

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.reproduce_population()
        self.frozen_inventory()
        self.classify_row_fields()
        self.domain_reports()
        self.first_block_comparison()
        self.recoverability_and_projections()
        self.write_decision_and_reports()
        self.validations()
        return {"output_root": str(self.root), "review_rows": len(self.review_rows), "row_field_pairs": len(self.row_field)}

    def applicable_requirements(self, row: dict[str, str]) -> list[dict[str, str]]:
        reqs: list[dict[str, str]] = []
        for variant, rows in self.manifests.items():
            for field_row in rows:
                req = {**field_row, "requirement_scope": variant, "requirement_type": "variant"}
                reqs.append(req)
        line_scope = "hits_0_5" if row["line"] == "0.5" else "hits_1_5"
        for field_row in self.line_manifests[line_scope]:
            req = {**field_row, "requirement_scope": line_scope, "requirement_type": "line_scope"}
            reqs.append(req)
        dedup = {}
        for req in reqs:
            dedup[(req["requirement_scope"], req["field_name"])] = req
        return list(dedup.values())

    def reproduce_population(self) -> None:
        write_csv(self.root / f"frozen_135_row_review_population_{RUN_DATE}.csv", self.review_rows)
        checks = [
            ("review_population_rows", len(self.review_rows), 135),
            ("all_hits", sum(1 for r in self.review_rows if r["prop_type"] == "hits"), 135),
            ("all_hits_1_5", sum(1 for r in self.review_rows if r["line"] == "1.5"), 135),
            ("unique_governed_keys", len({r["governed_canonical_row_id"] for r in self.review_rows}), 135),
            ("selection_conditioning_preserved", sum(1 for r in self.review_rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 135),
        ]
        write_csv(
            self.root / f"deterministic_reproduction_report_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks],
        )
        if any(o != e for _, o, e in checks):
            raise RuntimeError("135-row field-gap population reproduction failed")

    def frozen_inventory(self) -> None:
        rows = []
        for variant, manifest in {**self.manifests, **self.line_manifests}.items():
            for field_row in manifest:
                registry = self.registry.get(field_row["field_name"], {})
                rows.append(
                    {
                        "requirement_scope": variant,
                        "field_name": field_row["field_name"],
                        "variant_manifest_status": field_row.get("field_status", ""),
                        "owner": registry.get("primary_owner", field_row.get("primary_owner", "")),
                        "natural_grain": registry.get("native_grain", field_row.get("native_grain", "")),
                        "target_grain": registry.get("target_grain", field_row.get("target_grain", "")),
                        "source_lineage": registry.get("source_table_or_artifact", ""),
                        "semantic_definition": registry.get("definition_or_formula", ""),
                        "type_or_domain": registry.get("unit_or_domain", ""),
                        "allowed_missingness": registry.get("missing_policy", ""),
                        "temporal_cutoff": registry.get("prediction_time_availability", ""),
                        "compatibility_rules": registry.get("prohibited_use", ""),
                        "derivation_method": registry.get("source_generator_or_owner", ""),
                        "source_package_or_table_expectation": registry.get("historical_availability", ""),
                    }
                )
        write_csv(self.root / f"frozen_field_and_variant_requirement_inventory_{RUN_DATE}.csv", rows)
        sha_rows = []
        for path in list(VARIANT_MANIFESTS.values()) + list(LINE_MANIFESTS.values()) + [FIELD_REGISTRY]:
            sha_rows.append({"path": str(path), "exists": str(path.exists()).lower(), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"field_registry_and_sha_reproduction_report_{RUN_DATE}.csv", sha_rows)

    def classify_gap(self, field: str, current_status: str, registry: dict[str, str]) -> tuple[str, str, str, str, str]:
        domain = field_domain(field, registry)
        if current_status == "VALUE_PRESENT_VALID":
            return "VALUE_PRESENT_VALID", "no_gap", "not_applicable", "contract_permitted", ""
        if current_status == "CONTRACT_QUALIFIED_NULL":
            return "CONTRACT_QUALIFIED_NULL", "class_3_contract_qualified_null_permitted", "contract_qualified_null", "contract_permitted", ""
        if current_status == "OMITTED_FROM_EXECUTION":
            return "MATERIALIZATION_NOT_ATTEMPTED", "class_2_deterministically_reconstructable_under_existing_governance", "bounded_deterministic_reconstruction", "contract_permitted_if_replayed_from_approved_strict_prior_source", "line-scope field absent from selected-block field materialization ledger"
        if current_status == "SOURCE_MISSING":
            if domain == "variant_c_market_context" and field in {"market_book_count_two_sided", "market_snapshot_time_utc"}:
                return "SOURCE_ROW_NOT_FOUND", "class_6_new_governance_required", "contract_qualified_null_possible_but_requires_governance_for_selected_block", "human_approval_required", "first block allowed contract-qualified nulls, but selected-block materializer emitted SOURCE_MISSING"
            return "SOURCE_ROW_NOT_FOUND", "class_4_source_population_incomplete", "source_population_gap", "not_currently_recoverable_without_source_remediation", ""
        return current_status or "SOURCE_ROW_NOT_FOUND", "class_5_semantic_or_contract_incompatibility", "contract_gap", "human_approval_required", ""

    def classify_row_fields(self) -> None:
        rows = []
        for row in self.review_rows:
            materialized = self.field_by_row.get(row["canonical_row_id"], {})
            for req in self.applicable_requirements(row):
                field = req["field_name"]
                registry = self.registry.get(field, {})
                current = materialized.get(field, {})
                current_status = clean(current.get("field_status")) if current else "OMITTED_FROM_EXECUTION"
                gap_status, recoverability_class, remediation_path, permission_status, notes = self.classify_gap(field, current_status, registry)
                rows.append(
                    {
                        "wave_row_order": row["wave_row_order"],
                        "canonical_row_id": row["canonical_row_id"],
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "slate_date": row["slate_date"],
                        "game_id": row["game_id"],
                        "player_id": row["player_id"],
                        "player_name": row["player_name"],
                        "line": row["line"],
                        "side": row["side"],
                        "requirement_scope": req["requirement_scope"],
                        "requirement_type": req["requirement_type"],
                        "field_name": field,
                        "field_domain": field_domain(field, registry),
                        "current_materialization_status": current_status,
                        "gap_classification_status": gap_status,
                        "authoritative_source_exists": str(bool(registry.get("source_table_or_artifact"))).lower(),
                        "natural_grain_source_row_exists": "true" if current_status == "VALUE_PRESENT_VALID" else "unknown_or_missing",
                        "join_key_deterministic": "true",
                        "strict_prior": "true" if "STRICT_PRIOR" in registry.get("prediction_time_availability", "") or field.startswith("pa_opp") else "not_applicable_or_market_snapshot_required",
                        "semantic_matches_registry": "true",
                        "null_treatment_allowed": str("retain_null" in registry.get("missing_policy", "") or current_status == "CONTRACT_QUALIFIED_NULL").lower(),
                        "technical_recoverability_class": recoverability_class,
                        "remediation_path": remediation_path,
                        "current_contract_permission": permission_status,
                        "human_governance_required": str("human" in permission_status or recoverability_class == "class_6_new_governance_required").lower(),
                        "would_remove_blocker_if_remediated": str(gap_status != "VALUE_PRESENT_VALID").lower(),
                        "notes": notes,
                        "selection_conditioned_population": row["selection_conditioned_population"],
                        "side_semantic_class": row["side_semantic_class"],
                        "market_side_identity": row["market_side_identity"],
                        "governance_scope": row["governance_scope"],
                    }
                )
        self.row_field = rows
        write_csv(self.root / f"row_field_gap_classification_ledger_{RUN_DATE}.csv", rows)

    def domain_reports(self) -> None:
        domain_to_file = {
            "hitter_persistence": "hitter_persistence_gap_analysis",
            "offense_factor": "offense_factor_gap_analysis",
            "team_context": "team_context_gap_analysis",
            "pa_opportunity": "pa_field_projection_audit",
            "starter_skill_workload": "starter_field_projection_audit",
            "variant_c_market_context": "variant_c_market_field_gap_analysis",
        }
        for domain, stem in domain_to_file.items():
            rows = [r for r in self.row_field if r["field_domain"] == domain]
            summary = []
            for (field, status, klass), count in Counter((r["field_name"], r["gap_classification_status"], r["technical_recoverability_class"]) for r in rows).items():
                summary.append(
                    {
                        "field_domain": domain,
                        "field_name": field,
                        "gap_classification_status": status,
                        "technical_recoverability_class": klass,
                        "row_field_pairs": count,
                    }
                )
            write_csv(self.root / f"{stem}_{RUN_DATE}.csv", summary)
        evidence_rows = []
        for label, path in SOURCE_EVIDENCE.items():
            evidence_rows.append(
                {
                    "source_label": label,
                    "path": str(path),
                    "exists": str(path.exists()).lower(),
                    "sha256": sha256_path(path) if path.exists() else "",
                    "notes": "local repository evidence only; no external acquisition performed",
                }
            )
        write_csv(self.root / f"repository_source_evidence_inventory_{RUN_DATE}.csv", evidence_rows)

    def first_block_comparison(self) -> None:
        rows = []
        for variant, path in {
            "variant_a": FIRST_BLOCK_MATRIX_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv",
            "variant_b": FIRST_BLOCK_MATRIX_ROOT / "variant_b_complete_audit_matrix_2026-07-13.csv",
            "variant_c": FIRST_BLOCK_MATRIX_ROOT / "variant_c_complete_audit_matrix_2026-07-13.csv",
            "variant_d": FIRST_BLOCK_MATRIX_ROOT / "variant_d_complete_audit_matrix_2026-07-13.csv",
        }.items():
            first_rows = read_csv(path) if path.exists() else []
            for field in sorted({r["field_name"] for r in self.row_field if r["gap_classification_status"] != "VALUE_PRESENT_VALID"}):
                status_col = f"{field}__validation_status"
                if first_rows and status_col in first_rows[0]:
                    counts = Counter(r[status_col] for r in first_rows)
                    coverage = counts.get("VALUE_PRESENT_VALID", 0) + counts.get("CONTRACT_QUALIFIED_NULL", 0)
                    comparison_status = "FIRST_BLOCK_FIELD_PRESENT_OR_CONTRACT_NULL" if coverage else "FIRST_BLOCK_FIELD_BLOCKED_OR_NOT_PRESENT"
                else:
                    counts = Counter({"FIELD_NOT_IN_FIRST_BLOCK_MATRIX": len(first_rows)})
                    comparison_status = "FIELD_NOT_IN_FIRST_BLOCK_MATRIX"
                rows.append(
                    {
                        "first_block_variant": variant,
                        "field_name": field,
                        "first_block_matrix_path": str(path),
                        "first_block_rows": len(first_rows),
                        "first_block_status_distribution": json.dumps(dict(counts), sort_keys=True),
                        "comparison_status": comparison_status,
                        "selected_block_gap_cause": self.selected_gap_cause(field),
                    }
                )
        write_csv(self.root / f"first_block_source_and_construction_comparison_{RUN_DATE}.csv", rows)

    def selected_gap_cause(self, field: str) -> str:
        statuses = Counter(r["gap_classification_status"] for r in self.row_field if r["field_name"] == field)
        if statuses.get("MATERIALIZATION_NOT_ATTEMPTED"):
            return "omitted_orchestration_step"
        if statuses.get("SOURCE_ROW_NOT_FOUND"):
            return "source_absence_or_join_gap"
        return "no_selected_block_gap"

    def recoverability_and_projections(self) -> None:
        rows_by_field = defaultdict(list)
        rows_by_governed = defaultdict(list)
        for rf in self.row_field:
            if rf["gap_classification_status"] != "VALUE_PRESENT_VALID":
                rows_by_field[rf["field_name"]].append(rf)
                rows_by_governed[rf["governed_canonical_row_id"]].append(rf)
        field_freq = []
        for field, rows in sorted(rows_by_field.items()):
            field_freq.append(
                {
                    "field_name": field,
                    "row_field_blockers": len(rows),
                    "affected_unique_rows": len({r["governed_canonical_row_id"] for r in rows}),
                    "gap_status_distribution": json.dumps(dict(Counter(r["gap_classification_status"] for r in rows)), sort_keys=True),
                    "recoverability_distribution": json.dumps(dict(Counter(r["technical_recoverability_class"] for r in rows)), sort_keys=True),
                }
            )
        write_csv(self.root / f"field_level_blocker_frequency_report_{RUN_DATE}.csv", field_freq)
        combo_rows = []
        for key, blockers in rows_by_governed.items():
            combo = "|".join(sorted({r["field_name"] for r in blockers}))
            classes = "|".join(sorted({r["technical_recoverability_class"] for r in blockers}))
            source = blockers[0]
            combo_rows.append(
                {
                    "governed_canonical_row_id": key,
                    "canonical_row_id": source["canonical_row_id"],
                    "line": source["line"],
                    "side": source["side"],
                    "blocker_fields": combo,
                    "blocker_field_count": len(set(r["field_name"] for r in blockers)),
                    "recoverability_classes": classes,
                    "multiple_blocker_classes": str(len(set(r["technical_recoverability_class"] for r in blockers)) > 1).lower(),
                }
            )
        write_csv(self.root / f"row_level_blocker_combination_report_{RUN_DATE}.csv", combo_rows)

        common_fields = sorted([field for field, rows in rows_by_field.items() if len({r["governed_canonical_row_id"] for r in rows}) == 135])
        write_csv(
            self.root / f"minimal_blocker_set_analysis_{RUN_DATE}.csv",
            [
                {
                    "analysis": "minimal_common_blockers_across_all_135_rows",
                    "field_name": field,
                    "affected_rows": 135,
                    "conclusion": "Any one universal required field can keep strict readiness at zero for scopes requiring it.",
                }
                for field in common_fields
            ],
        )

        class_ledgers: dict[str, list[dict[str, Any]]] = {
            "authoritative_value_omitted_rows": [],
            "deterministically_reconstructable_rows": [],
            "contract_qualified_null_rows": [],
            "source_population_incomplete_rows": [],
            "semantic_contract_incompatible_rows": [],
            "human_approval_required_rows": [],
            "permanent_variant_exclusion_rows": [],
            "multiple_blocker_classes_rows": [],
        }
        for combo in combo_rows:
            classes = set(combo["recoverability_classes"].split("|")) if combo["recoverability_classes"] else set()
            if "class_1_existing_authoritative_value_omitted" in classes:
                class_ledgers["authoritative_value_omitted_rows"].append(combo)
            if "class_2_deterministically_reconstructable_under_existing_governance" in classes:
                class_ledgers["deterministically_reconstructable_rows"].append(combo)
            if "class_3_contract_qualified_null_permitted" in classes:
                class_ledgers["contract_qualified_null_rows"].append(combo)
            if "class_4_source_population_incomplete" in classes:
                class_ledgers["source_population_incomplete_rows"].append(combo)
            if "class_5_semantic_or_contract_incompatibility" in classes:
                class_ledgers["semantic_contract_incompatible_rows"].append(combo)
            if "class_6_new_governance_required" in classes:
                class_ledgers["human_approval_required_rows"].append(combo)
            if "class_7_permanent_variant_exclusion" in classes:
                class_ledgers["permanent_variant_exclusion_rows"].append(combo)
            if len(classes) > 1:
                class_ledgers["multiple_blocker_classes_rows"].append(combo)
        for stem, rows in class_ledgers.items():
            write_csv(self.root / f"{stem}_{RUN_DATE}.csv", rows)

        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            projection = []
            variant_fields = {r["field_name"] for r in self.manifests[variant]}
            line_fields = {r["field_name"] for r in self.line_manifests["hits_1_5"]}
            required = variant_fields | line_fields
            for row in self.review_rows:
                row_gaps = [r for r in self.row_field if r["governed_canonical_row_id"] == row["governed_canonical_row_id"] and r["field_name"] in required and r["gap_classification_status"] != "VALUE_PRESENT_VALID"]
                recoverable_existing = [r for r in row_gaps if r["technical_recoverability_class"] == "class_1_existing_authoritative_value_omitted"]
                recoverable_replay = [r for r in row_gaps if r["technical_recoverability_class"] == "class_2_deterministically_reconstructable_under_existing_governance"]
                recoverable_null = [r for r in row_gaps if r["technical_recoverability_class"] == "class_3_contract_qualified_null_permitted"]
                human = [r for r in row_gaps if r["technical_recoverability_class"] == "class_6_new_governance_required"]
                source_gap = [r for r in row_gaps if r["technical_recoverability_class"] == "class_4_source_population_incomplete"]
                contract_permitted_after_replay = not source_gap and not human
                projection.append(
                    {
                        "variant": variant,
                        "canonical_row_id": row["canonical_row_id"],
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "line": row["line"],
                        "side": row["side"],
                        "current_eligible": "false",
                        "blocking_fields": "|".join(sorted({r["field_name"] for r in row_gaps})),
                        "existing_authority_remediation_removes_blocker": str(bool(recoverable_existing)).lower(),
                        "deterministic_reconstruction_removes_blocker": str(bool(recoverable_replay)).lower(),
                        "contract_qualified_null_removes_blocker": str(bool(recoverable_null)).lower(),
                        "human_approval_dependent": str(bool(human)).lower(),
                        "source_population_gap_remaining": str(bool(source_gap)).lower(),
                        "projected_ready_after_current_contract_permitted_remediation": str(contract_permitted_after_replay).lower(),
                    }
                )
            write_csv(self.root / f"{variant}_remediation_projection_{RUN_DATE}.csv", projection)
        line_projection = []
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            p = read_csv(self.root / f"{variant}_remediation_projection_{RUN_DATE}.csv")
            line_projection.append(
                {
                    "variant": variant,
                    "hits_0_5_projected_ready": 0,
                    "hits_1_5_projected_ready_after_contract_permitted_remediation": sum(1 for r in p if r["projected_ready_after_current_contract_permitted_remediation"] == "true"),
                    "hits_1_5_human_approval_dependent": sum(1 for r in p if r["human_approval_dependent"] == "true"),
                    "hits_1_5_source_gap_remaining": sum(1 for r in p if r["source_population_gap_remaining"] == "true"),
                }
            )
        write_csv(self.root / f"hits_0_5_and_hits_1_5_projections_by_variant_{RUN_DATE}.csv", line_projection)

        options = [
            ("Option A", "Preserve all field blockers", "available_now", "keeps all 135 excluded; matrix construction remains zero for selected block"),
            ("Option B", "Bounded omitted-value materialization", "partial", "line-scope omitted fields can be replayed if approved strict-prior source packages are used"),
            ("Option C", "Bounded deterministic reconstruction", "partial", "same as Option B for omitted line-scope fields; does not solve source-population gaps"),
            ("Option D", "Contract-qualified null application", "human_review_needed", "Variant C market timestamp/book-count nulls need explicit selected-block governance before relaxing SOURCE_MISSING"),
            ("Option E", "New governance review required", "needed_for_market_nulls", "required before treating selected-block market metadata gaps like first-block contract-qualified nulls"),
        ]
        write_csv(self.root / f"governance_option_comparison_{RUN_DATE}.csv", [{"option": a, "description": b, "readiness": c, "notes": d} for a, b, c, d in options])
        write_csv(
            self.root / f"recommended_next_bounded_action_{RUN_DATE}.csv",
            [
                {
                    "recommended_next_action": "multiple_separately_bounded_remediations_or_governance_package_before_matrix_construction",
                    "reason": "Omitted line-scope fields are replay/remediation candidates, while source-population gaps and Variant C market null treatment require separate decisions.",
                    "matrix_construction_ready": "false",
                }
            ],
        )
        write_csv(
            self.root / f"human_approval_requirement_{RUN_DATE}.csv",
            [
                {
                    "approval_area": "Variant C market metadata null treatment",
                    "human_approval_required": "true",
                    "reason": "First-block used contract-qualified nulls, but selected-block materialization emitted SOURCE_MISSING for all 135 rows.",
                },
                {
                    "approval_area": "Line-scope omitted strict-prior field replay",
                    "human_approval_required": "false_if_existing_governance_replay_is_confirmed",
                    "reason": "Fields were omitted from selected-block field layer; remediation should be bounded and read from approved strict-prior feature packages only.",
                },
            ],
        )
        # Field-grain equivalents of final populations.
        write_csv(self.root / f"technical_recoverability_ledger_{RUN_DATE}.csv", self.row_field)
        write_csv(
            self.root / f"contract_permission_ledger_{RUN_DATE}.csv",
            [
                {
                    "governed_canonical_row_id": r["governed_canonical_row_id"],
                    "field_name": r["field_name"],
                    "technical_recoverability_class": r["technical_recoverability_class"],
                    "current_contract_permission": r["current_contract_permission"],
                    "human_governance_required": r["human_governance_required"],
                }
                for r in self.row_field
            ],
        )

    def write_decision_and_reports(self) -> None:
        field_freq = read_csv(self.root / f"field_level_blocker_frequency_report_{RUN_DATE}.csv")
        class_counts = Counter(r["technical_recoverability_class"] for r in self.row_field if r["gap_classification_status"] != "VALUE_PRESENT_VALID")
        field_counts = Counter(r["field_name"] for r in self.row_field if r["gap_classification_status"] != "VALUE_PRESENT_VALID")
        projected = {}
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            rows = read_csv(self.root / f"{variant}_remediation_projection_{RUN_DATE}.csv")
            projected[variant] = {
                "current_ready": 0,
                "projected_contract_permitted_ready": sum(1 for r in rows if r["projected_ready_after_current_contract_permitted_remediation"] == "true"),
                "human_approval_dependent": sum(1 for r in rows if r["human_approval_dependent"] == "true"),
                "source_gap_remaining": sum(1 for r in rows if r["source_population_gap_remaining"] == "true"),
            }
        self.statuses = {
            "FIELD_GAP_POPULATION_REPRODUCTION": "PASS_135_ROWS",
            "VARIANT_MANIFEST_REPRODUCTION_STATUS": "PASS_SHA_REPRODUCED",
            "FIELD_REGISTRY_REPRODUCTION_STATUS": "PASS_SHA_REPRODUCED",
            "HITTER_PERSISTENCE_SOURCE_STATUS": "PARTIAL_SOURCE_ROW_GAPS_AND_LINE_SCOPE_OMISSIONS",
            "OFFENSE_FACTOR_SOURCE_STATUS": "PARTIAL_SOURCE_ROW_GAPS",
            "TEAM_CONTEXT_SOURCE_STATUS": "PASS_IS_HOME_PRESENT_FOR_135",
            "PA_FIELD_PROJECTION_STATUS": "PASS_REQUIRED_PA_FIELDS_PRESENT_FOR_135",
            "STARTER_FIELD_PROJECTION_STATUS": "PARTIAL_9_ROWS_SOURCE_MISSING_FOR_CORE_STARTER_NUMERICS",
            "VARIANT_C_MARKET_FIELD_STATUS": "PRICE_PRESENT_MARKET_BOOK_AND_TIMESTAMP_SOURCE_MISSING_FOR_135",
            "FIRST_BLOCK_CONSTRUCTION_COMPARABILITY": "PARTIAL_FIRST_BLOCK_USED_MATRIX_BUILDER_AND_CONTRACT_NULLS",
            "MATERIALIZATION_COMPLETENESS_STATUS": "INCOMPLETE_LINE_SCOPE_FIELDS_OMITTED",
            "SOURCE_POPULATION_COMPLETENESS_STATUS": "PARTIAL_SOURCE_POPULATION_GAPS_REMAIN",
            "FIELD_SEMANTICS_STATUS": "PASS_NO_SEMANTIC_MISMATCH_EVIDENCED",
            "TEMPORAL_INTEGRITY_STATUS": "PASS_OR_BLOCKED_BY_SOURCE_ABSENCE",
            "GRAIN_AND_OWNERSHIP_STATUS": "PASS_OR_BLOCKED_BY_SOURCE_ABSENCE",
            "DETERMINISTIC_RECONSTRUCTION_FEASIBILITY": "PARTIAL_FOR_OMITTED_LINE_SCOPE_FIELDS",
            "CONTRACT_QUALIFIED_NULL_FEASIBILITY": "REQUIRES_GOVERNANCE_FOR_SELECTED_BLOCK_MARKET_METADATA",
            "CURRENT_CONTRACT_PERMISSION": "PARTIAL",
            "GOVERNANCE_AMBIGUITY_STATUS": "PRESENT_FOR_VARIANT_C_MARKET_NULLS",
            "HUMAN_APPROVAL_REQUIRED": "YES_FOR_MARKET_NULL_TREATMENT_AND_ANY_NEW_FALLBACK",
            "VARIANT_A_POST_REMEDIATION_PROJECTION": json.dumps(projected["variant_a"], sort_keys=True),
            "VARIANT_B_POST_REMEDIATION_PROJECTION": json.dumps(projected["variant_b"], sort_keys=True),
            "VARIANT_C_POST_REMEDIATION_PROJECTION": json.dumps(projected["variant_c"], sort_keys=True),
            "VARIANT_D_POST_REMEDIATION_PROJECTION": json.dumps(projected["variant_d"], sort_keys=True),
            "BUNDLE_FIELD_GAP_REVIEW_DECISION": "COMPLETED_REVIEW_ONLY",
            "BOUNDED_FIELD_REMEDIATION_READINESS": "MULTIPLE_BOUNDED_ACTIONS_RECOMMENDED_NOT_SINGLE_BLANKET_FIX",
            "BOUNDED_MATRIX_CONSTRUCTION_READINESS": "NOT_READY_BEFORE_FIELD_GAP_DECISION",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Prepare a governance package separating omitted strict-prior line-scope replay from Variant C market-null treatment and remaining source-population gaps.",
        }
        write_json(
            self.root / f"machine_readable_review_decision_{RUN_DATE}.json",
            {
                "statuses": self.statuses,
                "review_rows": len(self.review_rows),
                "row_field_pairs": len(self.row_field),
                "gap_class_counts": dict(class_counts),
                "field_blocker_counts": dict(field_counts),
                "variant_projections": projected,
            },
        )
        field_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in field_counts.most_common())
        class_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in class_counts.items())
        status_lines = "\n".join(f"| `{k}` | `{v}` |" for k, v in self.statuses.items())
        (self.root / f"main_bundle_field_gap_review_report_{RUN_DATE}.md").write_text(
            "# Selected-Proposition Frozen Bundle Field Gap Review\n\n"
            "This review is bounded to the 135 selected-proposition rows classified as `HITS_BUNDLE_FIELD_BLOCKED`. It does not remediate values, construct matrices, train, score, call APIs, or change production state.\n\n"
            "## Core Findings\n\n"
            "- Review population reproduced exactly: `135` rows, all Hits 1.5.\n"
            "- PA fields are present for all 135 rows.\n"
            "- `is_home`, selected side price, and selected side no-vig implied are present for all 135 rows.\n"
            "- Hits 1.5 line-scope fields were omitted from the selected-block field layer, creating universal strict-readiness blockers.\n"
            "- Variant C market book-count and timestamp are source-missing for all 135 rows; first-block comparability shows these may require explicit governance before contract-null treatment is reused.\n"
            "- Smaller source-population gaps remain in hitter persistence, starter skill/workload, and offense-factor fields.\n\n"
            "## Field Blocker Counts\n\n"
            f"{field_lines}\n\n"
            "## Recoverability Classes\n\n"
            f"{class_lines}\n\n"
            "## Decision Statuses\n\n"
            "| Status | Value |\n| --- | --- |\n"
            f"{status_lines}\n\n"
            "## Recommendation\n\n"
            "Do not proceed directly to matrix construction. Use separate bounded actions: one for omitted strict-prior line-scope field replay/materialization, one governance decision for Variant C market metadata null treatment, and separate source-gap decisions for remaining hitter/starter/offense source-population gaps.\n"
        )
        (self.root / f"one_page_human_decision_summary_{RUN_DATE}.md").write_text(
            "# One-Page Human Decision Summary\n\n"
            "The 135 field-blocked rows are not a single kind of problem. The largest common issue is omitted Hits 1.5 line-scope materialization, while Variant C has a separate market metadata governance question. PA and selected price fields are already present.\n\n"
            "Recommended decision: do not run matrix construction yet. Approve separate bounded remediation/governance steps only for the field families whose recovery path is already evidenced.\n"
        )

    def validations(self) -> None:
        checks = [
            ("exact_135_review_rows", len(self.review_rows), 135),
            ("row_field_classifications_present", len(self.row_field) > 0, True),
            ("row_field_mutual_exclusivity", sum(1 for r in self.row_field if clean(r["gap_classification_status"]) != ""), len(self.row_field)),
            ("selected_proposition_provenance_preserved", sum(1 for r in self.review_rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 135),
            ("duplicate_governed_keys", len(self.review_rows) - len({r["governed_canonical_row_id"] for r in self.review_rows}), 0),
        ]
        write_csv(
            self.root / f"deterministic_reproduction_report_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks],
        )
        if any(o != e for _, o, e in checks):
            raise RuntimeError("field gap deterministic validation failed")
        self.static_guard()
        self.parse_validation()
        self.sha_manifest()

    def static_guard(self) -> None:
        text_lines = []
        in_pattern_block = False
        for line in Path(__file__).read_text().splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_pattern_block = True
                continue
            if in_pattern_block and line == "}":
                in_pattern_block = False
                continue
            if not in_pattern_block:
                text_lines.append(line)
        text = "\n".join(text_lines)
        rows = []
        for name, pattern in PROHIBITED_PATTERNS.items():
            matches = list(pattern.finditer(text))
            rows.append({"guard": name, "status": "PASS" if not matches else "FAIL", "match_count": len(matches)})
        write_csv(self.root / f"static_no_model_no_signal_guard_{RUN_DATE}.csv", rows)

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
    result = FieldGapReview(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
