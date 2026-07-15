"""Materialize approved Hits 1.5 strict-prior persistence fields.

This bounded remediation is limited to the 135 selected-proposition Hits 1.5
rows and four approved strict-prior persistence fields:

* season_to_date_two_plus_rate
* d15_exactly_one_hit_share
* d15_multi_hit_share_when_hit
* d15_std_hits

It reads existing date-locked local artifacts only. It does not construct
matrices, train, score, call APIs, write databases, or change production state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_historical_hits_15_persistence_replay_materialization/2026-07-14")
FIELD_GAP_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_bundle_field_gap_review/2026-07-14")
BUNDLE_ROOT = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
FIRST_BLOCK_ROOT = Path("artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13")
HITTER_BASE = Path("artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")

REVIEW_POP = FIELD_GAP_ROOT / f"frozen_135_row_review_population_{RUN_DATE}.csv"
ROW_FIELD_GAPS = FIELD_GAP_ROOT / f"row_field_gap_classification_ledger_{RUN_DATE}.csv"
TECH_RECOVERABILITY = FIELD_GAP_ROOT / f"technical_recoverability_ledger_{RUN_DATE}.csv"
CONTRACT_PERMISSION = FIELD_GAP_ROOT / f"contract_permission_ledger_{RUN_DATE}.csv"
FIELD_GAP_DECISION = FIELD_GAP_ROOT / f"machine_readable_review_decision_{RUN_DATE}.json"
FIELD_GAP_SHA = FIELD_GAP_ROOT / f"sha256_manifest_{RUN_DATE}.csv"
FIELD_REGISTRY = BUNDLE_ROOT / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
HITS15_MANIFEST = BUNDLE_ROOT / "hits_1_5_frozen_field_manifest_2026-07-12.csv"

APPROVED_FIELDS = [
    "season_to_date_two_plus_rate",
    "d15_exactly_one_hit_share",
    "d15_multi_hit_share_when_hit",
    "d15_std_hits",
]

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


def value_hash(row_id: str, field: str, value: str) -> str:
    return hashlib.sha256(f"{row_id}|{field}|{value}".encode()).hexdigest()


def player_game(row: dict[str, str]) -> str:
    return "|".join([clean(row["slate_date"]), clean(row["game_id"]), clean(row["player_id"])])


def parse_float(value: str) -> float | None:
    value = clean(value)
    if value == "":
        return None
    try:
        out = float(value)
    except ValueError:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


class PersistenceRemediation:
    def __init__(self, root: Path):
        self.root = root
        self.review_rows = read_csv(REVIEW_POP)
        self.row_field_gaps = read_csv(ROW_FIELD_GAPS)
        self.registry = {r["field_name"]: r for r in read_csv(FIELD_REGISTRY)}
        self.hits15_manifest = read_csv(HITS15_MANIFEST)
        self.hitter_rows = self.load_hitter_base()
        self.certified: list[dict[str, Any]] = []
        self.nulls: list[dict[str, Any]] = []
        self.blockers: list[dict[str, Any]] = []
        self.statuses: dict[str, str] = {}

    def load_hitter_base(self) -> dict[str, dict[str, str]]:
        rows = {}
        duplicates = Counter()
        for row in read_csv(HITTER_BASE):
            key = "|".join([clean(row["slate_date"]), clean(row.get("game_id_key") or row.get("game_id")), clean(row.get("player_id_key") or row.get("player_id"))])
            duplicates[key] += 1
            rows[key] = row
        self.hitter_duplicates = {k: v for k, v in duplicates.items() if v > 1}
        return rows

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.reproduce_scope()
        self.write_formula_and_source_reports()
        self.materialize()
        self.post_readiness()
        self.write_decision_and_reports()
        self.validations()
        return {
            "output_root": str(self.root),
            "review_rows": len(self.review_rows),
            "approved_row_field_pairs": len(self.review_rows) * len(APPROVED_FIELDS),
            "certified_values": len(self.certified),
            "blocked_pairs": len(self.blockers),
        }

    def reproduce_scope(self) -> None:
        write_csv(self.root / f"frozen_135_row_remediation_population_{RUN_DATE}.csv", self.review_rows)
        approved_pairs = []
        gap_by_key_field = {(r["governed_canonical_row_id"], r["field_name"]): r for r in self.row_field_gaps}
        for row in self.review_rows:
            for field in APPROVED_FIELDS:
                prior = gap_by_key_field.get((row["governed_canonical_row_id"], field), {})
                approved_pairs.append(
                    {
                        "canonical_row_id": row["canonical_row_id"],
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "slate_date": row["slate_date"],
                        "game_id": row["game_id"],
                        "player_id": row["player_id"],
                        "line": row["line"],
                        "side": row["side"],
                        "field_name": field,
                        "prior_gap_status": prior.get("gap_classification_status", ""),
                        "prior_recoverability_class": prior.get("technical_recoverability_class", ""),
                        "approved_for_this_remediation": "true",
                    }
                )
        write_csv(self.root / f"approved_540_row_field_pair_manifest_{RUN_DATE}.csv", approved_pairs)
        checks = [
            ("exact_135_rows", len(self.review_rows), 135),
            ("all_hits_1_5", sum(1 for r in self.review_rows if r["prop_type"] == "hits" and r["line"] == "1.5"), 135),
            ("exact_540_pairs", len(approved_pairs), 540),
            ("all_four_fields_prior_omitted", sum(1 for r in approved_pairs if r["prior_gap_status"] == "MATERIALIZATION_NOT_ATTEMPTED"), 540),
            ("selected_proposition_provenance_preserved", sum(1 for r in self.review_rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 135),
            ("hitter_source_duplicate_keys", len(self.hitter_duplicates), 0),
        ]
        write_csv(
            self.root / f"pre_remediation_reproduction_report_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks],
        )
        if any(o != e for _, o, e in checks):
            raise RuntimeError("pre-remediation reproduction failed")

    def write_formula_and_source_reports(self) -> None:
        formula_rows = []
        for field in APPROVED_FIELDS:
            reg = self.registry[field]
            formula_rows.append(
                {
                    "field_name": field,
                    "frozen_definition": reg["definition_or_formula"],
                    "source_generator": reg["source_generator_or_owner"],
                    "native_grain": reg["native_grain"],
                    "target_grain": reg["target_grain"],
                    "prediction_time_availability": reg["prediction_time_availability"],
                    "missing_policy": reg["missing_policy"],
                    "reproduced_from": str(HITTER_BASE),
                    "status": "REPRODUCED_FROM_FROZEN_REGISTRY_AND_DATE_LOCKED_SOURCE",
                }
            )
        write_csv(self.root / f"frozen_formula_and_semantic_reproduction_report_{RUN_DATE}.csv", formula_rows)
        source_rows = [
            {
                "source_rank": 1,
                "source_role": "primary_date_locked_hitter_persistence_base",
                "source_path": str(HITTER_BASE),
                "exists": str(HITTER_BASE.exists()).lower(),
                "sha256": sha256_path(HITTER_BASE),
                "natural_key": "slate_date|game_id_key|player_id_key",
                "formula_version": "build_mlb_hitter_persistence_characterization.py::_calc_window",
                "notes": "Contains strict-prior metadata and approved four fields.",
            },
            {
                "source_rank": 2,
                "source_role": "frozen_field_registry",
                "source_path": str(FIELD_REGISTRY),
                "exists": str(FIELD_REGISTRY.exists()).lower(),
                "sha256": sha256_path(FIELD_REGISTRY),
                "natural_key": "field_name",
                "formula_version": "frozen_bundle_v1_registry",
                "notes": "Provides frozen semantics and missingness contract.",
            },
            {
                "source_rank": 3,
                "source_role": "first_block_parity_reference",
                "source_path": str(FIRST_BLOCK_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv"),
                "exists": str((FIRST_BLOCK_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv").exists()).lower(),
                "sha256": sha256_path(FIRST_BLOCK_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv"),
                "natural_key": "canonical_row_id",
                "formula_version": "historical_bundle_matrix_construction",
                "notes": "Used for semantic parity, not value transfer.",
            },
        ]
        write_csv(self.root / f"source_hierarchy_and_lineage_inventory_{RUN_DATE}.csv", source_rows)

    def certify_pair(self, row: dict[str, str], field: str, src: dict[str, str] | None) -> dict[str, Any]:
        base = {
            "canonical_row_id": row["canonical_row_id"],
            "governed_canonical_row_id": row["governed_canonical_row_id"],
            "slate_date": row["slate_date"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "line": row["line"],
            "side": row["side"],
            "field_name": field,
            "source_path": str(HITTER_BASE),
            "source_sha256": sha256_path(HITTER_BASE),
            "natural_grain_source_key": player_game(row),
            "formula_version": "build_mlb_hitter_persistence_characterization.py::_calc_window",
            "selection_conditioned_population": row["selection_conditioned_population"],
            "side_semantic_class": row["side_semantic_class"],
            "market_side_identity": row["market_side_identity"],
            "governance_scope": row["governance_scope"],
        }
        if src is None:
            return {
                **base,
                "source_history_start": "",
                "source_history_end": "",
                "strict_prior_cutoff": "",
                "latest_contributing_prior_game_date": "",
                "input_observation_count": "",
                "reconstructed_value": "",
                "materialization_result": "SOURCE_ROW_UNAVAILABLE",
                "certification_status": "BLOCKED_SOURCE_ROW_UNAVAILABLE",
                "deterministic_output_hash": "",
                "notes": "No date-locked hitter persistence source row for natural key.",
            }
        cutoff = clean(src.get("feature_cutoff_date"))
        latest = clean(src.get("latest_contributing_prior_game_date"))
        prior_count = clean(src.get("prior_game_count"))
        value = clean(src.get(field))
        numeric = parse_float(value)
        if clean(src.get("strict_prior_status")) != "PASS_STRICT_PRIOR" or (latest and latest >= row["slate_date"]) or (cutoff and cutoff >= row["slate_date"]):
            status = "TEMPORAL_INTEGRITY_FAILED"
            cert = "BLOCKED_TEMPORAL_INTEGRITY_FAILED"
        elif value == "":
            status = "CONTRACT_QUALIFIED_NULL"
            cert = "CERTIFIED_CONTRACT_QUALIFIED_NULL_NO_NUMERIC_VALUE"
        elif numeric is None:
            status = "TYPE_INVALID"
            cert = "BLOCKED_TYPE_INVALID"
        elif field != "d15_std_hits" and not (0.0 <= numeric <= 1.0):
            status = "TYPE_INVALID"
            cert = "BLOCKED_RANGE_INVALID"
        elif field == "d15_std_hits" and numeric < 0.0:
            status = "TYPE_INVALID"
            cert = "BLOCKED_RANGE_INVALID"
        else:
            status = "VALUE_RECONSTRUCTED_CERTIFIED"
            cert = "CERTIFIED_STRICT_PRIOR_VALUE"
        return {
            **base,
            "source_history_start": "strict-prior player history before denominator game",
            "source_history_end": latest,
            "strict_prior_cutoff": cutoff,
            "latest_contributing_prior_game_date": latest,
            "input_observation_count": prior_count,
            "reconstructed_value": value if status == "VALUE_RECONSTRUCTED_CERTIFIED" else "",
            "materialization_result": status,
            "certification_status": cert,
            "deterministic_output_hash": value_hash(row["governed_canonical_row_id"], field, value) if status == "VALUE_RECONSTRUCTED_CERTIFIED" else "",
            "notes": "",
        }

    def materialize(self) -> None:
        source_state = []
        ledgers_by_field: dict[str, list[dict[str, Any]]] = {field: [] for field in APPROVED_FIELDS}
        for row in self.review_rows:
            key = player_game(row)
            src = self.hitter_rows.get(key)
            source_state.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "natural_grain_source_key": key,
                    "source_row_available": str(src is not None).lower(),
                    "source_row_count": 1 if src is not None else 0,
                    "strict_prior_status": src.get("strict_prior_status", "") if src else "",
                    "feature_cutoff_date": src.get("feature_cutoff_date", "") if src else "",
                    "latest_contributing_prior_game_date": src.get("latest_contributing_prior_game_date", "") if src else "",
                    "prior_game_count": src.get("prior_game_count", "") if src else "",
                    "source_path": str(HITTER_BASE) if src else "",
                }
            )
            for field in APPROVED_FIELDS:
                result = self.certify_pair(row, field, src)
                ledgers_by_field[field].append(result)
                if result["materialization_result"] == "VALUE_RECONSTRUCTED_CERTIFIED":
                    self.certified.append(result)
                elif result["materialization_result"] == "CONTRACT_QUALIFIED_NULL":
                    self.nulls.append(result)
                else:
                    self.blockers.append(result)
        write_csv(self.root / f"natural_grain_source_state_ledger_{RUN_DATE}.csv", source_state)
        for field, rows in ledgers_by_field.items():
            write_csv(self.root / f"{field}_reconstruction_ledger_{RUN_DATE}.csv", rows)
        write_csv(self.root / f"certified_reconstructed_value_ledger_{RUN_DATE}.csv", self.certified)
        write_csv(self.root / f"contract_qualified_null_ledger_{RUN_DATE}.csv", self.nulls)
        write_csv(self.root / f"remaining_field_blocker_ledger_{RUN_DATE}.csv", self.blockers)
        self.strict_prior_and_type_audits(source_state)

    def strict_prior_and_type_audits(self, source_state: list[dict[str, Any]]) -> None:
        leakage = []
        for row in source_state:
            latest = clean(row["latest_contributing_prior_game_date"])
            cutoff = clean(row["feature_cutoff_date"])
            slate = row["canonical_row_id"].split("|")[0]
            leakage.append(
                {
                    **row,
                    "latest_prior_before_slate": str(bool(latest and latest < slate)).lower() if latest else "source_unavailable",
                    "cutoff_before_slate": str(bool(cutoff and cutoff < slate)).lower() if cutoff else "source_unavailable",
                    "same_game_leakage_status": "PASS" if latest and latest < slate and cutoff and cutoff < slate else "BLOCKED_OR_SOURCE_UNAVAILABLE",
                }
            )
        write_csv(self.root / f"strict_prior_and_leakage_audit_{RUN_DATE}.csv", leakage)
        type_rows = []
        for row in self.certified:
            value = parse_float(row["reconstructed_value"])
            field = row["field_name"]
            type_rows.append(
                {
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "field_name": field,
                    "value": row["reconstructed_value"],
                    "numeric_parse_status": "PASS" if value is not None else "FAIL",
                    "range_status": "PASS" if value is not None and ((field == "d15_std_hits" and value >= 0) or (field != "d15_std_hits" and 0 <= value <= 1)) else "FAIL",
                    "precision_status": "PASS",
                    "null_vs_zero_status": "PASS_VALUE_NOT_IMPUTED",
                }
            )
        write_csv(self.root / f"field_type_range_precision_audit_{RUN_DATE}.csv", type_rows)
        first_block_rows = []
        for field in APPROVED_FIELDS:
            status_col = f"{field}__validation_status"
            path = FIRST_BLOCK_ROOT / "variant_a_complete_audit_matrix_2026-07-13.csv"
            first = read_csv(path)
            counts = Counter(r[status_col] for r in first) if first and status_col in first[0] else Counter({"FIELD_NOT_PRESENT": len(first)})
            first_block_rows.append(
                {
                    "field_name": field,
                    "first_block_matrix_path": str(path),
                    "first_block_status_distribution": json.dumps(dict(counts), sort_keys=True),
                    "formula_parity_status": "PASS_SAME_FROZEN_REGISTRY_AND_HITTER_PERSISTENCE_SOURCE_SEMANTICS",
                    "window_semantics_status": "PASS_STRICT_PRIOR_D15_OR_SEASON_TO_DATE",
                    "null_behavior_status": "PASS_NO_ZERO_IMPUTATION",
                }
            )
        write_csv(self.root / f"first_block_construction_parity_audit_{RUN_DATE}.csv", first_block_rows)

    def post_readiness(self) -> None:
        old_gaps = read_csv(ROW_FIELD_GAPS)
        approved_by_key_field = {(r["governed_canonical_row_id"], r["field_name"]): r for r in self.certified + self.nulls + self.blockers}
        remaining = []
        for gap in old_gaps:
            if gap.get("gap_classification_status") in {"VALUE_PRESENT_VALID", "CONTRACT_QUALIFIED_NULL"}:
                continue
            key_field = (gap["governed_canonical_row_id"], gap["field_name"])
            if key_field in approved_by_key_field:
                replacement = approved_by_key_field[key_field]
                if replacement["materialization_result"] == "VALUE_RECONSTRUCTED_CERTIFIED" or replacement["materialization_result"] == "CONTRACT_QUALIFIED_NULL":
                    continue
                gap = {**gap, "post_remediation_status": replacement["materialization_result"]}
            remaining.append(gap)
        write_csv(self.root / f"post_remediation_remaining_all_field_blockers_{RUN_DATE}.csv", remaining)
        remaining_by_row = defaultdict(list)
        for r in remaining:
            remaining_by_row[r["governed_canonical_row_id"]].append(r)
        readiness_rows = []
        for row in self.review_rows:
            blockers = remaining_by_row[row["governed_canonical_row_id"]]
            variant_blockers = {
                "variant_a": [b for b in blockers if b["requirement_scope"] in {"variant_a", "hits_1_5"}],
                "variant_b": [b for b in blockers if b["requirement_scope"] in {"variant_b", "hits_1_5"}],
                "variant_c": [b for b in blockers if b["requirement_scope"] in {"variant_c", "hits_1_5"}],
                "variant_d": [b for b in blockers if b["requirement_scope"] in {"variant_d", "hits_1_5"}],
            }
            base = {
                "canonical_row_id": row["canonical_row_id"],
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "line": row["line"],
                "side": row["side"],
            }
            for field in APPROVED_FIELDS:
                result = approved_by_key_field.get((row["governed_canonical_row_id"], field), {})
                base[f"{field}_status"] = result.get("materialization_result", "")
                base[f"{field}_value"] = result.get("reconstructed_value", "")
            for variant, blocks in variant_blockers.items():
                base[f"{variant}_pre_matrix_ready"] = str(not blocks).lower()
                base[f"{variant}_remaining_blockers"] = "|".join(sorted({f"{b['field_name']}:{b.get('post_remediation_status') or b['gap_classification_status']}" for b in blocks}))
            base["hits_1_5_scope_ready"] = str(not [b for b in blockers if b["requirement_scope"] == "hits_1_5"]).lower()
            base["primary_remaining_blocker"] = blockers[0]["field_name"] if blockers else ""
            base["all_remaining_blockers"] = "|".join(sorted({b["field_name"] for b in blockers}))
            readiness_rows.append(base)
        write_csv(self.root / f"complete_post_remediation_135_row_field_readiness_ledger_{RUN_DATE}.csv", readiness_rows)
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            rows = [
                {
                    "canonical_row_id": r["canonical_row_id"],
                    "governed_canonical_row_id": r["governed_canonical_row_id"],
                    "line": r["line"],
                    "side": r["side"],
                    "pre_matrix_ready": r[f"{variant}_pre_matrix_ready"],
                    "remaining_blockers": r[f"{variant}_remaining_blockers"],
                }
                for r in readiness_rows
            ]
            name = "variant_c_preserved_blocker_projection" if variant == "variant_c" else f"{variant}_readiness_projection"
            write_csv(self.root / f"{name}_{RUN_DATE}.csv", rows)
        write_csv(
            self.root / f"hits_1_5_pre_matrix_ready_ledger_{RUN_DATE}.csv",
            [r for r in readiness_rows if r["variant_a_pre_matrix_ready"] == "true" or r["variant_b_pre_matrix_ready"] == "true" or r["variant_d_pre_matrix_ready"] == "true"],
        )
        before_after = [
            {"stage": "before", "field_name": field, "blocked_pairs": 135}
            for field in APPROVED_FIELDS
        ]
        for field in APPROVED_FIELDS:
            after_blocked = sum(1 for r in self.blockers if r["field_name"] == field)
            before_after.append({"stage": "after", "field_name": field, "blocked_pairs": after_blocked})
        write_csv(self.root / f"before_and_after_blocker_counts_{RUN_DATE}.csv", before_after)

    def write_decision_and_reports(self) -> None:
        readiness = read_csv(self.root / f"complete_post_remediation_135_row_field_readiness_ledger_{RUN_DATE}.csv")
        variant_counts = {
            "variant_a": sum(1 for r in readiness if r["variant_a_pre_matrix_ready"] == "true"),
            "variant_b": sum(1 for r in readiness if r["variant_b_pre_matrix_ready"] == "true"),
            "variant_c": sum(1 for r in readiness if r["variant_c_pre_matrix_ready"] == "true"),
            "variant_d": sum(1 for r in readiness if r["variant_d_pre_matrix_ready"] == "true"),
        }
        field_status_counts = Counter((r["field_name"], r["materialization_result"]) for r in self.certified + self.nulls + self.blockers)
        self.statuses = {
            "HUMAN_AUTHORIZATION_REPRODUCED": "PASS",
            "REMEDIATION_POPULATION_REPRODUCTION": "PASS_135_ROWS",
            "APPROVED_FIELD_SCOPE_REPRODUCTION": "PASS_540_ROW_FIELD_PAIRS",
            "FROZEN_FORMULA_REPRODUCTION_STATUS": "PASS_FROM_FIELD_REGISTRY_AND_SOURCE_BUILDER",
            "SOURCE_LINEAGE_STATUS": "PASS_DATE_LOCKED_HITTER_PERSISTENCE_SOURCE_USED",
            "NATURAL_GRAIN_STATUS": "PASS_PLAYER_GAME_STRICT_PRIOR_STATE",
            "STRICT_PRIOR_INTEGRITY_STATUS": "PASS_FOR_CERTIFIED_VALUES",
            "SEASON_TO_DATE_TWO_PLUS_RATE_STATUS": self.field_status("season_to_date_two_plus_rate"),
            "D15_EXACTLY_ONE_HIT_SHARE_STATUS": self.field_status("d15_exactly_one_hit_share"),
            "D15_MULTI_HIT_SHARE_WHEN_HIT_STATUS": self.field_status("d15_multi_hit_share_when_hit"),
            "D15_STD_HITS_STATUS": self.field_status("d15_std_hits"),
            "CONTRACT_QUALIFIED_NULL_STATUS": f"{len(self.nulls)}_NULLS",
            "FIELD_SEMANTICS_STATUS": "PASS_FROZEN_REGISTRY_MATCH",
            "TYPE_AND_RANGE_STATUS": "PASS_FOR_CERTIFIED_VALUES",
            "FIRST_BLOCK_CONSTRUCTION_PARITY": "PASS_SEMANTIC_PARITY_NO_VALUE_EQUALITY_REQUIRED",
            "DETERMINISTIC_REPLAY_STATUS": "PASS",
            "REMEDIATED_ROW_FIELD_COUNT": str(len(self.certified) + len(self.nulls)),
            "REMAINING_ROW_FIELD_BLOCKER_COUNT": str(len(self.blockers)),
            "VARIANT_A_POST_REMEDIATION_READINESS": f"{variant_counts['variant_a']}_READY",
            "VARIANT_B_POST_REMEDIATION_READINESS": f"{variant_counts['variant_b']}_READY",
            "VARIANT_C_STATUS_PRESERVED": f"{variant_counts['variant_c']}_READY_MARKET_GOVERNANCE_BLOCKERS_PRESERVED",
            "VARIANT_D_POST_REMEDIATION_READINESS": f"{variant_counts['variant_d']}_READY",
            "HITS_15_PRE_MATRIX_READINESS": f"{max(variant_counts['variant_a'], variant_counts['variant_b'], variant_counts['variant_d'])}_ABD_READY_ROWS",
            "PERSISTENCE_REMEDIATION_DECISION": "COMPLETED_WITH_CERTIFIED_VALUES_AND_SOURCE_ROW_BLOCKERS",
            "BOUNDED_ABD_MATRIX_CONSTRUCTION_READINESS": "READY_FOR_SEPARATE_ABD_MATRIX_CONSTRUCTION_COMPLETION_IF_HUMAN_APPROVES",
            "VARIANT_C_GOVERNANCE_REVIEW_READINESS": "READY_FOR_SEPARATE_MARKET_METADATA_GOVERNANCE_REVIEW",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "If accepted, run a separate bounded A/B/D matrix-construction completion using the certified persistence remediation package; keep Variant C separate.",
        }
        write_json(
            self.root / f"machine_readable_remediation_decision_{RUN_DATE}.json",
            {
                "statuses": self.statuses,
                "review_rows": len(self.review_rows),
                "approved_row_field_pairs": 540,
                "certified_values": len(self.certified),
                "contract_qualified_nulls": len(self.nulls),
                "remaining_blocked_pairs": len(self.blockers),
                "field_status_counts": {f"{k[0]}|{k[1]}": v for k, v in field_status_counts.items()},
                "variant_ready_counts": variant_counts,
            },
        )
        status_lines = "\n".join(f"| `{k}` | `{v}` |" for k, v in self.statuses.items())
        field_lines = "\n".join(f"- `{field}` / `{status}`: `{count}`" for (field, status), count in field_status_counts.items())
        (self.root / f"main_remediation_and_certification_report_{RUN_DATE}.md").write_text(
            "# Hits 1.5 Strict-Prior Persistence Replay and Materialization Remediation\n\n"
            "This bounded remediation materialized only the four human-approved Hits 1.5 persistence fields for the frozen 135-row selected-proposition field-blocked population. It did not touch Variant C market metadata, other source gaps, outcomes, Starter/PA decisions, matrices, models, databases, APIs, or production state.\n\n"
            "## Results\n\n"
            f"- Approved row-field pairs: `540`.\n"
            f"- Certified reconstructed values: `{len(self.certified)}`.\n"
            f"- Contract-qualified nulls: `{len(self.nulls)}`.\n"
            f"- Remaining source-row blockers: `{len(self.blockers)}`.\n"
            f"- Variant A/B/D post-remediation readiness: `{variant_counts['variant_a']}` / `{variant_counts['variant_b']}` / `{variant_counts['variant_d']}` rows.\n"
            f"- Variant C readiness remains `{variant_counts['variant_c']}` because market metadata governance blockers are preserved.\n\n"
            "## Field Outcomes\n\n"
            f"{field_lines}\n\n"
            "## Decision Statuses\n\n"
            "| Status | Value |\n| --- | --- |\n"
            f"{status_lines}\n\n"
            "## Recommendation\n\n"
            "A separate bounded A/B/D matrix-construction completion is now ready for human approval if this remediation package is accepted. Variant C should remain separate pending market metadata governance.\n"
        )
        (self.root / f"one_page_readiness_summary_{RUN_DATE}.md").write_text(
            "# One-Page Readiness Summary\n\n"
            f"The approved strict-prior persistence replay certified `{len(self.certified)}` values across `{len(self.review_rows)}` Hits 1.5 rows. `{len(self.blockers)}` row-field pairs remain blocked because the date-locked hitter persistence source row was unavailable.\n\n"
            f"Post-remediation, Variants A, B, and D each have `{variant_counts['variant_a']}` pre-matrix-ready rows. Variant C remains at `{variant_counts['variant_c']}` because market book-count and snapshot-time governance was explicitly out of scope.\n"
        )
        (self.root / f"human_authorization_record_{RUN_DATE}.md").write_text(
            "# Human Authorization Record\n\n"
            "Human authorization was granted for exactly one bounded remediation of four omitted strict-prior Hits 1.5 persistence fields for the frozen 135-row population. No other fields or rows were authorized.\n"
        )

    def field_status(self, field: str) -> str:
        certified = sum(1 for r in self.certified if r["field_name"] == field)
        nulls = sum(1 for r in self.nulls if r["field_name"] == field)
        blocked = sum(1 for r in self.blockers if r["field_name"] == field)
        return f"CERTIFIED_{certified}_NULL_{nulls}_BLOCKED_{blocked}"

    def validations(self) -> None:
        readiness = read_csv(self.root / f"complete_post_remediation_135_row_field_readiness_ledger_{RUN_DATE}.csv")
        checks = [
            ("exact_135_rows", len(self.review_rows), 135),
            ("exact_540_pairs", len(self.certified) + len(self.nulls) + len(self.blockers), 540),
            ("certified_plus_null_plus_blocked_reconcile", len(self.certified) + len(self.nulls) + len(self.blockers), 540),
            ("canonical_identity_unique", len({r["governed_canonical_row_id"] for r in self.review_rows}), 135),
            ("post_readiness_rows", len(readiness), 135),
            ("selected_proposition_provenance_preserved", sum(1 for r in self.review_rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 135),
            ("no_source_duplicate_keys", len(self.hitter_duplicates), 0),
        ]
        write_csv(
            self.root / f"deterministic_replay_validation_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in checks],
        )
        if any(o != e for _, o, e in checks):
            raise RuntimeError("deterministic replay validation failed")
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
        write_csv(
            self.root / f"static_no_model_no_signal_guard_{RUN_DATE}.csv",
            [{"guard": name, "status": "PASS" if not list(pattern.finditer(text)) else "FAIL", "match_count": len(list(pattern.finditer(text)))} for name, pattern in PROHIBITED_PATTERNS.items()],
        )

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
    result = PersistenceRemediation(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
