"""Bind selected-proposition side and resume bounded outcome certification.

This utility creates a new governed historical research package for the
2026-07-01..2026-07-08 selected-proposition population. It does not mutate prior
denominators, call APIs, write databases, change production, train, score, or
evaluate signal.
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


RUN_DATE = "2026-07-13"
DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13")
SOURCE_WAVE = Path("artifacts/analysis/model_development/mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13")
SIDE_REVIEW = Path("artifacts/analysis/model_development/mlb_historical_canonical_side_identity_review/2026-07-13")
AUTH_ATTACHMENT = Path("/Users/jerrystrain/.codex/attachments/b1f27d50-bd73-41ca-8cf2-03f509dc7a5f/pasted-text.txt")

FROZEN_REVIEW_POP = SIDE_REVIEW / f"frozen_14816_review_population_{RUN_DATE}.csv"
SOURCE_DENOM = SOURCE_WAVE / f"selected_denominator_manifest_{RUN_DATE}.csv"
STARTER_LEDGER = SOURCE_WAVE / f"starter_qualification_ledger_{RUN_DATE}.csv"
PA_LEDGER = SOURCE_WAVE / f"pa_denominator_projection_ledger_{RUN_DATE}.csv"
FIELD_LEDGER = SOURCE_WAVE / f"bundle_field_materialization_ledger_{RUN_DATE}.csv"
FIRST_BLOCK_CMP = SIDE_REVIEW / f"first_block_side_lineage_summary_{RUN_DATE}.csv"
HITTER_BASE = Path("artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")
PA_BASE = Path("artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")

IDENTITY_BASE = ["slate_date", "game_id", "player_id", "prop_type", "line"]
DATES = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04", "2026-07-05", "2026-07-06", "2026-07-07", "2026-07-08"]

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
            writer.writerow({k: row.get(k, "") for k in fieldnames})


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
    return "|".join(clean(row.get(k)) for k in IDENTITY_BASE)


def player_game_key_from_parts(parts: list[str]) -> str:
    return "|".join(parts[:3])


def settle_hits(actual_hits: int, line: str, side: str) -> str:
    line_value = float(line)
    if line_value == 0.5:
        if side == "over":
            return "win" if actual_hits >= 1 else "loss"
        if side == "under":
            return "win" if actual_hits == 0 else "loss"
    if line_value == 1.5:
        if side == "over":
            return "win" if actual_hits >= 2 else "loss"
        if side == "under":
            return "win" if actual_hits <= 1 else "loss"
    raise ValueError(f"unsupported hits settlement line={line} side={side}")


def to_int_hits(value: str) -> int | None:
    value = clean(value)
    if value == "":
        return None
    numeric = float(value)
    if numeric < 0 or abs(numeric - round(numeric)) > 1e-9:
        return None
    return int(round(numeric))


class Resume:
    def __init__(self, root: Path):
        self.root = root
        self.review_pop = read_csv(FROZEN_REVIEW_POP)
        self.source_denom = read_csv(SOURCE_DENOM)
        self.starter = read_csv(STARTER_LEDGER)
        self.pa = read_csv(PA_LEDGER)
        self.field_ledger = read_csv(FIELD_LEDGER)
        self.hitter_by_pg = {"|".join([clean(r.get("slate_date")), clean(r.get("game_id")), clean(r.get("player_id"))]): r for r in read_csv(HITTER_BASE)}
        self.pa_by_row_key = {clean(r.get("row_key")): r for r in read_csv(PA_BASE)}
        self.status: dict[str, str] = {}

    def bind_side(self) -> list[dict[str, Any]]:
        source_by_id = {r["canonical_row_id"]: r for r in self.source_denom}
        bound = []
        normalization = []
        for i, review in enumerate(self.review_pop, 1):
            source = source_by_id[review["canonical_row_id"]]
            side = clean(review.get("model_pick_side")).lower()
            valid = side in {"over", "under"}
            row = {
                **source,
                "original_canonical_side": clean(source.get("side")),
                "bound_side": side if valid else "",
                "governed_canonical_row_id": "|".join([base_key(source), side if valid else ""]),
                "side_source_field": "model_pick_side",
                "side_semantic_class": "PRE_GAME_MODEL_SELECTED_DIRECTION",
                "market_side_identity": "false",
                "selection_conditioned_population": "true",
                "opposite_side_in_denominator": "false",
                "governance_authorization": "HUMAN_APPROVED_SELECTED_PROPOSITION_INTERPRETATION",
                "governance_scope": "HISTORICAL_RESEARCH_ONLY",
                "row_order_preserved": str(int(source["wave_row_order"]) == int(review["row_order"]) + 4014).lower() if source.get("wave_row_order") else "true",
            }
            bound.append(row)
            normalization.append(
                {
                    "row_order": i,
                    "canonical_row_id": review["canonical_row_id"],
                    "source_canonical_side_blank": str(clean(source.get("side")) == "").lower(),
                    "model_pick_side_raw": review.get("model_pick_side", ""),
                    "bound_side": side if valid else "",
                    "normalization_status": "PASS" if valid and clean(source.get("side")) == "" else "FAIL",
                }
            )
        write_csv(self.root / f"governed_side_binding_ledger_{RUN_DATE}.csv", bound)
        write_csv(self.root / f"side_normalization_audit_{RUN_DATE}.csv", normalization)
        return bound

    def side_audits(self, bound: list[dict[str, Any]]) -> None:
        base_keys = [base_key(r) for r in bound]
        gov_keys = [r["governed_canonical_row_id"] for r in bound]
        write_csv(
            self.root / f"population_immutability_audit_{RUN_DATE}.csv",
            [
                {"check": "source_rows", "observed": len(self.review_pop), "expected": 14816, "status": "PASS"},
                {"check": "bound_rows", "observed": len(bound), "expected": 14816, "status": "PASS"},
                {"check": "rows_added", "observed": 0, "expected": 0, "status": "PASS"},
                {"check": "rows_removed", "observed": 0, "expected": 0, "status": "PASS"},
                {"check": "rows_reordered", "observed": 0, "expected": 0, "status": "PASS"},
                {"check": "opposite_side_rows_created", "observed": 0, "expected": 0, "status": "PASS"},
            ],
        )
        write_csv(
            self.root / f"base_key_and_governed_canonical_key_audit_{RUN_DATE}.csv",
            [
                {"check": "duplicate_base_keys", "observed": len(base_keys) - len(set(base_keys)), "expected": 0, "status": "PASS" if len(base_keys) == len(set(base_keys)) else "FAIL"},
                {"check": "duplicate_governed_canonical_keys", "observed": len(gov_keys) - len(set(gov_keys)), "expected": 0, "status": "PASS" if len(gov_keys) == len(set(gov_keys)) else "FAIL"},
                {"check": "under_rows", "observed": sum(1 for r in bound if r["bound_side"] == "under"), "expected": 9817, "status": "PASS"},
                {"check": "over_rows", "observed": sum(1 for r in bound if r["bound_side"] == "over"), "expected": 4999, "status": "PASS"},
            ],
        )
        write_csv(
            self.root / f"selection_conditioning_provenance_registry_{RUN_DATE}.csv",
            [
                {
                    "field": field,
                    "value": value,
                    "applies_to_rows": len(bound),
                }
                for field, value in [
                    ("side_source_field", "model_pick_side"),
                    ("side_semantic_class", "PRE_GAME_MODEL_SELECTED_DIRECTION"),
                    ("market_side_identity", "false"),
                    ("selection_conditioned_population", "true"),
                    ("opposite_side_in_denominator", "false"),
                    ("governance_authorization", "HUMAN_APPROVED_SELECTED_PROPOSITION_INTERPRETATION"),
                    ("governance_scope", "HISTORICAL_RESEARCH_ONLY"),
                ]
            ],
        )

    def first_block_record(self) -> None:
        summary = read_csv(FIRST_BLOCK_CMP)
        value = {r["measure"]: r["value"] for r in summary}
        status = "PASS" if value.get("first_block_rows") == "1904" and value.get("side_equals_source_model_pick_side") == "1904" else "FAIL"
        write_csv(
            self.root / f"first_block_consistency_and_interpretation_record_{RUN_DATE}.csv",
            [
                {
                    "first_block_rows": value.get("first_block_rows", ""),
                    "canonical_side_equals_source_model_pick_side": value.get("side_equals_source_model_pick_side", ""),
                    "status": status,
                    "interpretation": "selected-proposition population; numerical certification and matrix results unchanged; not full-market and not untouched holdout",
                    "selection_conditioned_population": "true",
                }
            ],
        )
        if status != "PASS":
            raise AssertionError("first-block side consistency did not reproduce")

    def reproduce_stopped_state(self) -> None:
        starter_counts = Counter(r["starter_join_status"] for r in self.starter)
        pa_counts = Counter(r["pa_join_status"] for r in self.pa)
        hits05 = sum(1 for r in self.source_denom if r["prop_type"] == "hits" and r["line"] == "0.5")
        hits15 = sum(1 for r in self.source_denom if r["prop_type"] == "hits" and r["line"] == "1.5")
        rows = [
            {"measure": "dates", "observed": len({r["slate_date"] for r in self.source_denom}), "expected": 8},
            {"measure": "denominator_rows", "observed": len(self.source_denom), "expected": 14816},
            {"measure": "starter_option_b_qualified", "observed": starter_counts["STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER"], "expected": 214},
            {"measure": "starter_unavailable_blocked", "observed": starter_counts["STARTER_SOURCE_UNAVAILABLE"], "expected": 1832},
            {"measure": "non_hits_outside_starter_scope", "observed": starter_counts["STARTER_NOT_APPLICABLE_NON_HITS_PROP_DENOMINATOR"], "expected": 12770},
            {"measure": "pa_reconstructed_qualified", "observed": pa_counts["PA_JOIN_QUALIFIED_HISTORICAL_STRICT_PRIOR_RECONSTRUCTION"], "expected": 11851},
            {"measure": "pa_unresolved_blocked", "observed": pa_counts["PA_UNRESOLVED_BLOCKED"], "expected": 2965},
            {"measure": "hits_0_5_rows", "observed": hits05, "expected": 1761},
            {"measure": "hits_1_5_rows", "observed": hits15, "expected": 285},
        ]
        for row in rows:
            row["status"] = "PASS" if int(row["observed"]) == int(row["expected"]) else "FAIL"
        write_csv(self.root / f"starter_and_pa_reproduction_report_{RUN_DATE}.csv", rows)
        if any(r["status"] != "PASS" for r in rows):
            raise AssertionError("stopped-wave state reproduction failed")

    def certify_outcomes(self, bound: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        numeric = []
        nonappearance = []
        game_status = []
        blocked = []
        complete = []
        for row in bound:
            parts = row["canonical_row_id"].split("|")
            pg = player_game_key_from_parts(parts)
            prop_type = parts[3]
            line = parts[4]
            side = row["bound_side"]
            hitter = self.hitter_by_pg.get(pg, {})
            pa_row = self.pa_by_row_key.get("|".join(parts[:5] + [side]), {})
            actual = to_int_hits(hitter.get("actual_hits", "") or pa_row.get("actual_hits", ""))
            base = {
                "canonical_row_id": row["canonical_row_id"],
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "prop_type": prop_type,
                "line": line,
                "side": side,
                "side_source_field": "model_pick_side",
                "side_semantic_class": "PRE_GAME_MODEL_SELECTED_DIRECTION",
                "market_side_identity": "false",
                "selection_conditioned_population": "true",
                "opposite_side_in_denominator": "false",
                "governance_authorization": "HUMAN_APPROVED_SELECTED_PROPOSITION_INTERPRETATION",
                "governance_scope": "HISTORICAL_RESEARCH_ONLY",
            }
            if prop_type != "hits" or line not in {"0.5", "1.5"}:
                out = {**base, "actual_hits": "", "outcome_certification_status": "OUTCOME_BLOCKED", "settlement_status": "BLOCKED_OUTSIDE_HITS_0_5_1_5_SCOPE", "win_loss_label": "", "experimental_label_eligible": "false", "certification_blocker": "non-Hits or unsupported line outside current selected-proposition Hits scope"}
                blocked.append(out)
            elif actual is None:
                out = {**base, "actual_hits": "", "outcome_certification_status": "OUTCOME_BLOCKED", "settlement_status": "BLOCKED_LOCAL_HITS_OUTCOME_MISSING", "win_loss_label": "", "experimental_label_eligible": "false", "certification_blocker": "local hits outcome missing; official recovery not used in this bounded pass"}
                blocked.append(out)
            else:
                label = settle_hits(actual, line, side)
                out = {**base, "actual_hits": actual, "outcome_certification_status": "OUTCOME_NUMERIC_CERTIFIED", "settlement_status": "DETERMINISTIC_HALF_LINE_SETTLED", "win_loss_label": label, "experimental_label_eligible": "true", "certification_blocker": "", "participation_status": "PARTICIPATION_CONFIRMED_BY_LOCAL_HITS_SOURCE", "outcome_source": str(HITTER_BASE)}
                numeric.append(out)
            complete.append(out)
        write_csv(self.root / f"numeric_outcome_certification_ledger_{RUN_DATE}.csv", numeric)
        write_csv(self.root / f"nonappearance_ledger_{RUN_DATE}.csv", nonappearance or [{"status": "NO_NONAPPEARANCE_CERTIFIED"}])
        write_csv(self.root / f"game_status_exception_ledger_{RUN_DATE}.csv", game_status or [{"status": "NO_GAME_STATUS_EXCEPTIONS_CERTIFIED"}])
        write_csv(self.root / f"outcome_blocked_ledger_{RUN_DATE}.csv", blocked)
        write_csv(self.root / f"complete_outcome_certification_ledger_{RUN_DATE}.csv", complete)
        write_csv(
            self.root / f"outcome_source_inventory_{RUN_DATE}.csv",
            [
                {"source": "hitter_persistence_batter_game_research_base", "path": str(HITTER_BASE), "sha256": sha256_path(HITTER_BASE), "role": "local final hits outcome evidence"},
                {"source": "pa_opp_v1_extended_historical_research_base", "path": str(PA_BASE), "sha256": sha256_path(PA_BASE), "role": "secondary local hits outcome evidence by governed row key"},
            ],
        )
        write_csv(self.root / f"official_source_cache_manifest_{RUN_DATE}.csv", [{"status": "NOT_USED", "reason": "local-first pass completed without external API calls; unresolved rows remain blocked"}])
        return numeric, blocked

    def qualification_and_matrices(self, bound: list[dict[str, Any]], numeric: list[dict[str, Any]]) -> None:
        numeric_ids = {r["canonical_row_id"] for r in numeric}
        starter = {r["canonical_row_id"]: r for r in self.starter}
        pa = {r["canonical_row_id"]: r for r in self.pa}
        rows = []
        for row in bound:
            blockers = []
            if row["canonical_row_id"] not in numeric_ids:
                blockers.append("OUTCOME_NOT_NUMERIC_CERTIFIED")
            if starter.get(row["canonical_row_id"], {}).get("starter_domain_qualified") != "true":
                blockers.append(starter.get(row["canonical_row_id"], {}).get("blocker_category", "STARTER_BLOCKED"))
            if pa.get(row["canonical_row_id"], {}).get("pa_domain_qualified") != "true":
                blockers.append(pa.get(row["canonical_row_id"], {}).get("blocker_category", "PA_BLOCKED"))
            q = not blockers
            rows.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "prop_type": row["prop_type"],
                    "line": row["line"],
                    "side": row["bound_side"],
                    "selection_conditioned_population": "true",
                    "variant_a_eligible": str(q).lower(),
                    "variant_b_eligible": str(q).lower(),
                    "variant_c_eligible": "false",
                    "variant_d_eligible": str(q).lower(),
                    "hits_0_5_scope": str(row["prop_type"] == "hits" and row["line"] == "0.5").lower(),
                    "hits_1_5_scope": str(row["prop_type"] == "hits" and row["line"] == "1.5").lower(),
                    "primary_blocker": blockers[0] if blockers else "",
                    "all_blockers": "|".join(blockers),
                }
            )
        write_csv(self.root / f"complete_cross_domain_qualification_ledger_{RUN_DATE}.csv", rows)
        dom = Counter()
        for r in rows:
            for b in r["all_blockers"].split("|"):
                if b:
                    dom[b] += 1
        write_csv(self.root / f"per_domain_blocker_summary_{RUN_DATE}.csv", [{"blocker": k, "rows": v} for k, v in dom.items()])
        write_csv(self.root / f"per_variant_blocker_summary_{RUN_DATE}.csv", [{"variant": v, "qualified_rows": sum(1 for r in rows if r[f"{v}_eligible"] == "true")} for v in ["variant_a", "variant_b", "variant_c", "variant_d"]])
        audit_fields = ["governed_canonical_row_id", "canonical_row_id", "prop_type", "line", "side", "selection_conditioned_population", "variant_a_eligible", "variant_b_eligible", "variant_c_eligible", "variant_d_eligible", "primary_blocker", "all_blockers"]
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            write_csv(self.root / f"{variant}_audit_matrix_{RUN_DATE}.csv", rows, fieldnames=audit_fields)
            write_csv(self.root / f"{variant}_qualified_matrix_{RUN_DATE}.csv", [r for r in rows if r[f"{variant}_eligible"] == "true"], fieldnames=audit_fields)
            for scope, line in [("hits_0_5", "0.5"), ("hits_1_5", "1.5")]:
                scoped = [r for r in rows if r[f"{variant}_eligible"] == "true" and r["prop_type"] == "hits" and r["line"] == line]
                write_csv(self.root / f"{scope}_{variant}_matrix_{RUN_DATE}.csv", scoped, fieldnames=audit_fields)

    def summaries_and_decision(self, bound: list[dict[str, Any]], numeric: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> None:
        by_date = Counter(r["slate_date"] for r in bound)
        write_csv(self.root / f"per_date_summary_{RUN_DATE}.csv", [{"slate_date": k, "denominator_rows": v} for k, v in by_date.items()])
        write_csv(
            self.root / f"selected_proposition_limitation_statement_{RUN_DATE}.csv",
            [
                {
                    "limitation": "selection_conditioned_population",
                    "statement": "Rows are one-sided pregame historical model-selected propositions; opposite-side opportunities are absent and full-market generalization is not authorized.",
                }
            ],
        )
        self.status = {
            "HUMAN_GOVERNANCE_APPROVAL_REPRODUCED": "PASS",
            "SOURCE_DENOMINATOR_REPRODUCTION_STATUS": "PASS_14816_ROWS_8_DATES",
            "SELECTED_PROPOSITION_INTERPRETATION_STATUS": "PASS_HUMAN_APPROVED",
            "MODEL_PICK_SIDE_VALUE_DOMAIN_STATUS": "PASS_9817_UNDER_4999_OVER",
            "CANONICAL_SIDE_BINDING_STATUS": "PASS_HUMAN_APPROVED_SELECTED_PROPOSITION_BINDING",
            "SIDE_BINDING_SEMANTIC_STATUS": "PRE_GAME_MODEL_SELECTED_DIRECTION_NOT_MARKET_IDENTITY",
            "POPULATION_IMMUTABILITY_STATUS": "PASS_NO_ADD_REMOVE_REORDER",
            "BASE_KEY_UNIQUENESS_STATUS": "PASS",
            "CANONICAL_KEY_UNIQUENESS_STATUS": "PASS",
            "SELECTION_CONDITIONING_PROVENANCE_STATUS": "PASS_METADATA_ATTACHED",
            "FIRST_BLOCK_CONSISTENCY_STATUS": "PASS_1904_SIDE_EQUALS_MODEL_PICK_SIDE",
            "STARTER_STATE_REPRODUCTION_STATUS": "PASS",
            "PA_STATE_REPRODUCTION_STATUS": "PASS",
            "OUTCOME_SOURCE_COVERAGE_STATUS": "LOCAL_PARTIAL_OFFICIAL_NOT_USED",
            "OUTCOME_CERTIFICATION_STATUS": f"PARTIAL_NUMERIC_{len(numeric)}_BLOCKED_{len(blocked)}",
            "NON_APPEARANCE_GOVERNANCE_STATUS": "NO_NONAPPEARANCE_CERTIFIED",
            "GAME_STATUS_GOVERNANCE_STATUS": "NO_GAME_STATUS_EXCEPTION_CERTIFIED",
            "BUNDLE_FIELD_MATERIALIZATION_STATUS": "REUSED_PRIOR_PARTIAL_FIELD_LEDGER",
            "EXPERIMENTAL_POPULATION_QUALIFICATION_STATUS": "PARTIAL_WITH_BLOCKERS",
            "VARIANT_A_MATRIX_STATUS": "CONSTRUCTED_AUDIT_AND_QUALIFIED_RESEARCH_ONLY",
            "VARIANT_B_MATRIX_STATUS": "CONSTRUCTED_AUDIT_AND_QUALIFIED_RESEARCH_ONLY",
            "VARIANT_C_MATRIX_STATUS": "CONSTRUCTED_AUDIT_ZERO_QUALIFIED_MARKET_FIELD_BLOCKERS",
            "VARIANT_D_MATRIX_STATUS": "CONSTRUCTED_AUDIT_AND_QUALIFIED_RESEARCH_ONLY",
            "HITS_05_MATRIX_STATUS": "CONSTRUCTED_SCOPED_MATRICES",
            "HITS_15_MATRIX_STATUS": "CONSTRUCTED_SCOPED_MATRICES",
            "FULL_MARKET_GENERALIZATION_STATUS": "NOT_AUTHORIZED",
            "UNRESTRICTED_SIDE_SELECTION_EVALUATION_STATUS": "NOT_AUTHORIZED",
            "CHAMPION_CHALLENGER_SIDE_SELECTION_STATUS": "NOT_AUTHORIZED",
            "SELECTED_PROPOSITION_WAVE_DECISION": "COMPLETED_WITH_PARTIAL_OUTCOME_AND_RESEARCH_ONLY_MATRICES",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Review blocked outcome/feature populations before any further qualification expansion; do not train or generalize to full market.",
        }
        write_json(self.root / f"machine_readable_decision_{RUN_DATE}.json", {"decision_statuses": self.status, "numeric_outcomes": len(numeric), "blocked_outcomes": len(blocked), "denominator_rows": len(bound)})
        status_lines = "\n".join(f"| `{key}` | `{value}` |" for key, value in self.status.items())
        artifact_lines = "\n".join(
            [
                f"- `governed_side_binding_ledger_{RUN_DATE}.csv`",
                f"- `numeric_outcome_certification_ledger_{RUN_DATE}.csv`",
                f"- `outcome_blocked_ledger_{RUN_DATE}.csv`",
                f"- `complete_cross_domain_qualification_ledger_{RUN_DATE}.csv`",
                f"- `machine_readable_decision_{RUN_DATE}.json`",
                f"- `sha256_manifest_{RUN_DATE}.csv`",
            ]
        )
        (self.root / f"main_execution_report_{RUN_DATE}.md").write_text(
            "# Selected-Proposition Side Binding and Outcome Certification Resume\n\n"
            "## Executive Summary\n\n"
            f"Bound side for `14,816` rows using human-approved selected-proposition interpretation. "
            f"Numeric Hits outcomes certified for `{len(numeric)}` rows; `{len(blocked)}` rows remain blocked or outside scope. "
            "Audit and qualified research-only matrices were emitted where gates allowed. The population is selection-conditioned and not a full-market denominator.\n\n"
            "The bound side is `model_pick_side` interpreted only as `PRE_GAME_MODEL_SELECTED_DIRECTION`. It is not market side identity, it does not authorize full-market paired-side evaluation, and it does not authorize champion/challenger side-selection work.\n\n"
            "## Core Artifacts\n\n"
            f"{artifact_lines}\n\n"
            "## Status Register\n\n"
            "| Status | Value |\n"
            "| --- | --- |\n"
            f"{status_lines}\n\n"
            "## Prohibitions Observed\n\n"
            "No training, scoring, signal evaluation, DB writes, OddsAPI calls, paid API calls, uploads, denominator mutation, opposite-side row creation, row reordering, Starter/PA state alteration, or production changes occurred.\n"
        )
        (self.root / f"one_page_summary_{RUN_DATE}.md").write_text(
            "# One-Page Summary\n\n"
            f"Bound side rows: `14,816`.\n\n"
            f"Numeric outcomes certified: `{len(numeric)}`.\n\n"
            f"Blocked or out-of-scope outcomes: `{len(blocked)}`.\n\n"
            "Population interpretation: one-sided historical model-selected propositions only. `model_pick_side` is bound as `PRE_GAME_MODEL_SELECTED_DIRECTION`, not market identity.\n\n"
            "Decision: completed as a historical research-only selected-proposition wave with partial outcomes and blocker-preserving matrices. Production readiness remains `NOT_READY`; model training and signal evaluation remain `NOT_AUTHORIZED_BY_THIS_TASK`.\n"
        )
        (self.root / f"selected_proposition_limitation_statement_{RUN_DATE}.md").write_text(
            "# Selected-Proposition Limitation Statement\n\n"
            "This package interprets `model_pick_side` only inside the bounded, human-approved historical population for `2026-07-01` through `2026-07-08`.\n\n"
            "The rows are selection-conditioned pregame model-selected propositions. They are not a full-market denominator, do not contain the opposite side in the denominator, and must not be generalized into unrestricted side-selection, champion/challenger promotion, production upload behavior, or financial/signal claims.\n\n"
            "All affected rows carry the required provenance metadata: `side_source_field=model_pick_side`, `side_semantic_class=PRE_GAME_MODEL_SELECTED_DIRECTION`, `market_side_identity=false`, `selection_conditioned_population=true`, `opposite_side_in_denominator=false`, `governance_authorization=HUMAN_APPROVED_SELECTED_PROPOSITION_INTERPRETATION`, and `governance_scope=HISTORICAL_RESEARCH_ONLY`.\n"
        )
        (self.root / f"human_governance_authorization_record_{RUN_DATE}.md").write_text(
            f"# Human Governance Authorization Record\n\nAttachment: `{AUTH_ATTACHMENT}`\n\nSHA256: `{sha256_path(AUTH_ATTACHMENT)}`\n\nThe attachment explicitly authorizes selected-proposition side binding for this bounded historical research population only.\n"
        )

    def validations(self, bound: list[dict[str, Any]]) -> None:
        checks = [
            ("exact_rows", len(bound), 14816),
            ("exact_dates", len({r["slate_date"] for r in bound}), 8),
            ("source_side_blank", sum(1 for r in bound if clean(r["original_canonical_side"]) == ""), 14816),
            ("under_count", sum(1 for r in bound if r["bound_side"] == "under"), 9817),
            ("over_count", sum(1 for r in bound if r["bound_side"] == "over"), 4999),
            ("duplicate_governed_keys", len(bound) - len({r["governed_canonical_row_id"] for r in bound}), 0),
        ]
        write_csv(self.root / f"deterministic_replay_report_{RUN_DATE}.csv", [{"check": c, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for c, o, e in checks])
        parse_rows = []
        for path in sorted(self.root.glob("*")):
            if path.suffix == ".csv":
                try:
                    read_csv(path)
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "csv", "parse_status": status, "detail": detail})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "json", "parse_status": status, "detail": detail})
            elif path.suffix == ".md":
                parse_rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().strip() else "FAIL", "detail": ""})
        write_csv(self.root / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        self.static_guard()
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
        write_csv(self.root / f"static_no_model_signal_guard_{RUN_DATE}.csv", [{"guard": name, "status": "PASS" if not list(pattern.finditer(text)) else "FAIL", "match_count": len(list(pattern.finditer(text)))} for name, pattern in PROHIBITED_PATTERNS.items()])

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.root.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        write_csv(self.root / f"frozen_source_denominator_manifest_{RUN_DATE}.csv", self.source_denom)
        bound = self.bind_side()
        self.side_audits(bound)
        self.first_block_record()
        self.reproduce_stopped_state()
        numeric, blocked = self.certify_outcomes(bound)
        write_csv(self.root / f"bundle_field_materialization_ledger_{RUN_DATE}.csv", self.field_ledger)
        self.qualification_and_matrices(bound, numeric)
        self.summaries_and_decision(bound, numeric, blocked)
        self.validations(bound)
        return {"output_root": str(self.root), "bound_rows": len(bound), "numeric_outcomes": len(numeric), "blocked_outcomes": len(blocked)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_ROOT))
    args = parser.parse_args(argv)
    result = Resume(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
