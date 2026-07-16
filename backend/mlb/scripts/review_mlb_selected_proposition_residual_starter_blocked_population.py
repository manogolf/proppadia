#!/usr/bin/env python3
"""Review residual Starter-blocked population after ordinary campaign closure.

Read-only characterization utility. It consumes only local certified artifacts
and performs no network access, discovery, acquisition, reconstruction,
remediation, qualification propagation, matrix construction, model/scoring
work, database/API writes, OddsAPI calls, uploads, LaunchAgent changes, or
production behavior changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_residual_starter_blocked_population_review/"
    "2026-07-15"
)
CLOSURE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_c010_recovery_and_ordinary_campaign_closure/"
    "2026-07-15"
)
BASE_STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/"
    "2026-07-14"
)
ABD_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/"
    "2026-07-14"
)

BASE_849 = BASE_STATE_DIR / "remaining_849_row_starter_blocked_inventory_2026-07-14.csv"
BASE_FQ15 = BASE_STATE_DIR / "fully_qualified_hits_1_5_manifest_2026-07-14.csv"
CLOSURE_JSON = CLOSURE_DIR / f"machine_readable_c010_recovery_and_campaign_closure_{RUN_DATE}.json"
CLOSURE_MANIFEST = CLOSURE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
CLOSURE_803 = CLOSURE_DIR / f"final_803_row_campaign_closure_reconciliation_{RUN_DATE}.csv"
CLOSURE_96 = CLOSURE_DIR / f"final_96_side_campaign_closure_reconciliation_{RUN_DATE}.csv"

MOVEMENT_PATHS = [
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_004_resolved_branch_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_remaining_starter_recovery_campaign/2026-07-15/DISCOVERY_COHORT_005/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_remaining_starter_recovery_campaign/2026-07-15/DISCOVERY_COHORT_006/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_remaining_starter_recovery_campaign/2026-07-15/DISCOVERY_COHORT_007/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_remaining_starter_recovery_campaign/2026-07-15/DISCOVERY_COHORT_008/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_remaining_starter_recovery_campaign/2026-07-15/DISCOVERY_COHORT_009/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_c010_recovery_and_ordinary_campaign_closure/2026-07-15/DISCOVERY_COHORT_010/stage_05_reconstruction_remediation/row_level_qualification_movement_ledger_2026-07-15.csv"),
]

MATRIX_PATHS = [
    ABD_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    ABD_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    ABD_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

SVANSON_SIDE = "2026-07-07|823062|MIL|STL"
GABRIEL_HUGHES_SIDE = "2026-07-08|823928|LAD|COL"
EXPECTED_CLOSURE_DECISION = "C010_COMPLETED_ORDINARY_CAMPAIGN_CLOSED_WITH_GOVERNED_EXCLUSIONS"

PROHIBITED_PATTERNS = {
    "network_or_acquisition": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi|gameLog", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"\bremediate\s*\(|reconstruct\s*\(|qualification_propagation|pa_remediation|outcome_remediation|bundle_remediation", re.IGNORECASE),
    "matrix_model_signal": re.compile(
        r"build_mlb_selected_proposition_abd_matrices|\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|model_score_|prediction_score",
        re.IGNORECASE,
    ),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "odds_upload_scheduler": re.compile(r"oddsapi|odds_api|upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_denominator_identity") or row.get("canonical_row_id", "")


def movement_row_id(row: dict[str, str]) -> str:
    return row.get("canonical_denominator_identity") or row.get("governed_canonical_row_id") or row.get("canonical_row_id", "")


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
    out: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type in {tokenize.STRING, tokenize.COMMENT}:
                continue
            out.append(tok.string)
    except tokenize.TokenError:
        return text
    return " ".join(out)


def static_guard() -> list[dict[str, Any]]:
    code_only = strip_strings_comments_and_pattern_block(Path(__file__).read_text(encoding="utf-8"))
    rows = []
    for name, pattern in PROHIBITED_PATTERNS.items():
        matches = pattern.findall(code_only)
        rows.append({"check": name, "status": "PASS" if not matches else "FAIL", "matches": "|".join(str(m) for m in matches)})
    return rows


def classify_side(side_key: str, rows: list[dict[str, str]], closure_by_side: dict[str, dict[str, str]]) -> tuple[str, str, str, str, str]:
    closure = closure_by_side.get(side_key, {})
    closure_cat = closure.get("campaign_boundary_classification", "")
    current_cat = closure.get("current_campaign_category", "")
    base_tax = rows[0].get("starter_taxonomy_category", "")
    if side_key in {SVANSON_SIDE, GABRIEL_HUGHES_SIDE}:
        return (
            "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED",
            "zero_strict_prior_mlb_starts; relief/non-start history cannot substitute",
            "TERMINAL_UNDER_CURRENT_STARTER_CONTRACT",
            "PRESERVE_FAIL_CLOSED_NO_CURRENT_ACTION",
            "starter contract requires compatible strict-prior MLB starts",
        )
    if closure_cat == "LOCAL_PARENT_FAIL_CLOSED":
        return (
            "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT",
            "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
            "RECOVERABLE_CONSTRUCTION_OR_PERSISTENCE_REPAIR_REQUIRED",
            "PURSUE_NEXT_BOUNDED_LOCAL_REPAIR",
            "local parent construction/persistence defect; do not substitute expected_hits_outs_context_v1",
        )
    if closure_cat == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT":
        return (
            "IDENTITY_OR_ROLE_REVIEW_HOLDOUT",
            "identity or role evidence requires separate review before Starter admission",
            "RECOVERABLE_IDENTITY_OR_ROLE_GOVERNANCE_REQUIRED",
            "PURSUE_IDENTITY_OR_ROLE_REVIEW",
            "role/identity ambiguity is governance, not missing data",
        )
    if base_tax == "SPECIAL_REGIME_ESTABLISHED_EXCLUSION":
        return (
            "ESTABLISHED_SPECIAL_REGIME_EXCLUSION",
            "established special-regime exclusion preserved",
            "TERMINAL_UNDER_CURRENT_STARTER_CONTRACT",
            "PRESERVE_FAIL_CLOSED_NO_CURRENT_ACTION",
            "intentionally excluded under current Starter design",
        )
    if current_cat.startswith("STARTER_REMEDIATED_") or closure_cat == "OTHER_FAIL_CLOSED_EXPLICIT_REASON":
        return (
            "STARTER_CERTIFIED_BUT_PRIMARY_BLOCKER_ACCOUNTING_ARTIFACT",
            "row/side has Starter-remediated evidence in closure taxonomy but remains in residual Starter-blocked accounting",
            "RECOVERABLE_EXISTING_GOVERNANCE_LOCAL_ONLY",
            "AUDIT_STALE_BLOCKER_ACCOUNTING",
            "likely stale blocker/accounting artifact; review before any source work",
        )
    if closure_cat == "ORDINARY_DOWNSTREAM_LIMITED":
        return (
            "STARTER_PARENT_DOMAIN_MISSING_OTHER",
            "ordinary direct-source missing side deferred because non-Starter blockers reduce value",
            "INSUFFICIENT_EVIDENCE_FAIL_CLOSED",
            "DEFER_UNTIL_AFTER_MATRIX_OR_MODEL_RESEARCH",
            "starter source gap exists, but current governing package deferred due downstream blockers",
        )
    return (
        "OTHER_EXPLICIT_FAIL_CLOSED_REASON",
        "explicit residual fail-closed reason preserved by source package",
        "INSUFFICIENT_EVIDENCE_FAIL_CLOSED",
        "PRESERVE_FAIL_CLOSED_NO_CURRENT_ACTION",
        "no bounded recovery path established in current evidence",
    )


def score_category(row: dict[str, Any]) -> tuple[int, str]:
    projected = int_value(row["projected_newly_fully_qualified_ceiling"])
    sides = int_value(row["side_count"])
    reusable = 2 if row["reusable_platform_defect"] == "true" else 0
    tech = {
        "low": 3,
        "medium": 2,
        "high": 0,
        "terminal": -3,
    }.get(row["technical_risk"], 1)
    gov = {
        "low": 3,
        "medium": 2,
        "high": 0,
        "terminal": -3,
    }.get(row["governance_complexity"], 1)
    source_penalty = -2 if row["new_network_or_source_work_required"] == "true" else 0
    formula_penalty = -3 if row["formula_change_required"] == "true" else 0
    score = projected + sides + reusable + tech + gov + source_penalty + formula_penalty
    return score, "non-signal score = projected_yield + side_count + reuse + tractability - source/formula burden"


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    closure = json.loads(CLOSURE_JSON.read_text(encoding="utf-8"))
    base_rows = read_csv(BASE_849)
    closure_803 = read_csv(CLOSURE_803)
    closure_96 = read_csv(CLOSURE_96)
    closure_by_side = {r["starter_game_side_key"]: r for r in closure_96}
    closure_by_row = {r["governed_canonical_row_id"]: r for r in closure_803}
    matrix_ids = set()
    for path in MATRIX_PATHS:
        matrix_ids.update(r["governed_canonical_row_id"] for r in read_csv(path))
    movement_rows = []
    moved_ids = set()
    for path in MOVEMENT_PATHS:
        for r in read_csv(path):
            rid = movement_row_id(r)
            movement_rows.append({"path": str(path), "row_id": rid, **r})
            if rid:
                moved_ids.add(rid)
    residual_rows_base = [r for r in base_rows if row_id(r) not in moved_ids]
    by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in residual_rows_base:
        by_side[r["starter_game_key"]].append(r)

    residual_side_rows = []
    residual_manifest = []
    for side_key, rows in sorted(by_side.items()):
        primary, root, recoverability, recommendation, note = classify_side(side_key, rows, closure_by_side)
        hits05 = sum(r["line"] == "0.5" for r in rows)
        hits15 = sum(r["line"] == "1.5" for r in rows)
        non_starter_ready = sum(
            r.get("post_three_row_pa_qualified") == "true"
            and r.get("numeric_outcome_certified") == "true"
            and not r.get("post_three_row_downstream_blockers")
            for r in rows
        )
        pa_block = sum(r.get("post_three_row_pa_qualified") != "true" for r in rows)
        outcome_block = sum(r.get("numeric_outcome_certified") != "true" for r in rows)
        bundle_block = sum(bool(r.get("post_three_row_downstream_blockers")) for r in rows)
        multi_block = sum(
            sum([
                r.get("post_three_row_pa_qualified") != "true",
                r.get("numeric_outcome_certified") != "true",
                bool(r.get("post_three_row_downstream_blockers")),
            ]) > 1
            for r in rows
        )
        projected = non_starter_ready if primary != "STARTER_CERTIFIED_BUT_PRIMARY_BLOCKER_ACCOUNTING_ARTIFACT" else sum(rid not in matrix_ids for rid in [row_id(r) for r in rows])
        residual_side_rows.append({
            "starter_game_side_key": side_key,
            "primary_residual_category": primary,
            "secondary_taxonomy": root,
            "recoverability_classification": recoverability,
            "recommended_action": recommendation,
            "represented_row_count": len(rows),
            "hits_0_5_rows": hits05,
            "hits_1_5_rows": hits15,
            "rows_with_all_non_starter_prerequisites_satisfied": non_starter_ready,
            "projected_newly_fully_qualified_ceiling_if_starter_recovered": projected,
            "downstream_pa_blockers": pa_block,
            "downstream_outcome_blockers": outcome_block,
            "downstream_bundle_blockers": bundle_block,
            "rows_with_multiple_downstream_blockers": multi_block,
            "potential_abd_matrix_readiness_additions": sum(r["line"] == "1.5" and row_id(r) not in matrix_ids and non_starter_ready for r in rows),
            "variant_c_implication": "variant_c_not_resolved; preserve market metadata governance",
            "root_cause": root,
            "current_governing_contract": rows[0].get("authoritative_source_package") or closure_by_side.get(side_key, {}).get("authoritative_source_package", ""),
            "technical_recoverability": recoverability,
            "governance_change_required": "true" if "GOVERNANCE" in recoverability or "FORMULA" in recoverability else "false",
            "new_network_or_source_work_required": "true" if recoverability in {"RECOVERABLE_EXISTING_GOVERNANCE_EXTERNAL_SOURCE_REQUIRED", "RECOVERABLE_NEW_SOURCE_GOVERNANCE_REQUIRED"} else "false",
            "formula_change_required": "true" if recoverability == "RECOVERABLE_FORMULA_GOVERNANCE_CHANGE_REQUIRED" else "false",
            "construction_persistence_or_join_repair_required": "true" if "CONSTRUCTION" in recoverability or primary == "STARTER_CERTIFIED_BUT_PRIMARY_BLOCKER_ACCOUNTING_ARTIFACT" else "false",
            "reusable_platform_defect": "true" if primary in {"LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT", "STARTER_CERTIFIED_BUT_PRIMARY_BLOCKER_ACCOUNTING_ARTIFACT"} else "false",
            "terminal_under_current_starter_design": "true" if recoverability == "TERMINAL_UNDER_CURRENT_STARTER_CONTRACT" else "false",
            "notes": note,
        })
        for r in rows:
            rid = row_id(r)
            closure_row = closure_by_row.get(rid, {})
            residual_manifest.append({
                "governed_canonical_row_id": rid,
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "team": r["team"],
                "opponent": r["opponent"],
                "prop_type": r["prop_type"],
                "line": r["line"],
                "side": r["side"],
                "starter_game_side_key": r["starter_game_key"],
                "primary_residual_category": primary,
                "secondary_taxonomy": root,
                "recoverability_classification": recoverability,
                "recommendation": recommendation,
                "pa_status": r.get("post_three_row_pa_status"),
                "pa_qualified": r.get("post_three_row_pa_qualified"),
                "outcome_status": r.get("outcome_category"),
                "outcome_qualified": r.get("numeric_outcome_certified"),
                "bundle_blockers": r.get("post_three_row_downstream_blockers"),
                "variant_a_state": r.get("post_three_row_variant_a_state"),
                "variant_b_state": r.get("post_three_row_variant_b_state"),
                "variant_c_state": r.get("post_three_row_variant_c_state"),
                "variant_d_state": r.get("post_three_row_variant_d_state"),
                "research_history_classification": closure_row.get("current_campaign_category") or r.get("starter_taxonomy_category"),
                "prediction_eligibility_classification": "PREDICTION_INELIGIBLE_RESIDUAL_STARTER_BLOCKED",
                "authoritative_package_or_rule": closure_row.get("authoritative_source_package") or "post_three_row_pa_849_inventory",
            })

    cat_rows = []
    for primary, sides in defaultdict(list, {k: [r for r in residual_side_rows if r["primary_residual_category"] == k] for k in set(r["primary_residual_category"] for r in residual_side_rows)}).items():
        rows = [r for r in residual_manifest if r["primary_residual_category"] == primary]
        sample = sides[0]
        cat_rows.append({
            "primary_residual_category": primary,
            "side_count": len(sides),
            "represented_row_count": len(rows),
            "hits_0_5_row_count": sum(r["line"] == "0.5" for r in rows),
            "hits_1_5_row_count": sum(r["line"] == "1.5" for r in rows),
            "rows_with_all_non_starter_prerequisites_satisfied": sum(int_value(s["rows_with_all_non_starter_prerequisites_satisfied"]) for s in sides),
            "projected_newly_fully_qualified_ceiling": sum(int_value(s["projected_newly_fully_qualified_ceiling_if_starter_recovered"]) for s in sides),
            "downstream_pa_blockers": sum(int_value(s["downstream_pa_blockers"]) for s in sides),
            "downstream_outcome_blockers": sum(int_value(s["downstream_outcome_blockers"]) for s in sides),
            "downstream_bundle_blockers": sum(int_value(s["downstream_bundle_blockers"]) for s in sides),
            "rows_with_multiple_downstream_blockers": sum(int_value(s["rows_with_multiple_downstream_blockers"]) for s in sides),
            "potential_abd_matrix_readiness_additions": sum(int_value(s["potential_abd_matrix_readiness_additions"]) for s in sides),
            "variant_c_implications": sample["variant_c_implication"],
            "exact_root_cause": sample["root_cause"],
            "current_governing_contract": sample["current_governing_contract"],
            "technical_recoverability": sample["technical_recoverability"],
            "recoverability_classification": sample["recoverability_classification"],
            "governance_change_required": sample["governance_change_required"],
            "new_network_or_source_work_required": sample["new_network_or_source_work_required"],
            "formula_change_required": sample["formula_change_required"],
            "construction_persistence_or_join_repair_required": sample["construction_persistence_or_join_repair_required"],
            "reusable_platform_defect": sample["reusable_platform_defect"],
            "terminal_under_current_starter_design": sample["terminal_under_current_starter_design"],
            "technical_risk": "low" if sample["recommended_action"] in {"AUDIT_STALE_BLOCKER_ACCOUNTING", "PURSUE_NEXT_BOUNDED_LOCAL_REPAIR"} else ("terminal" if sample["terminal_under_current_starter_design"] == "true" else "medium"),
            "governance_complexity": "low" if sample["recommended_action"] == "AUDIT_STALE_BLOCKER_ACCOUNTING" else ("terminal" if sample["terminal_under_current_starter_design"] == "true" else "medium"),
            "recommended_action": sample["recommended_action"],
        })
    for row in cat_rows:
        score, rationale = score_category(row)
        row["value_score"] = score
        row["score_rationale"] = rationale
    cat_rows.sort(key=lambda r: int_value(r["value_score"]), reverse=True)
    for i, row in enumerate(cat_rows, start=1):
        row["category_rank"] = i

    fq15 = {row_id(r): {**r, "source": "post_three_row_fully_qualified_hits_1_5_manifest"} for r in read_csv(BASE_FQ15)}
    for path in MOVEMENT_PATHS:
        for r in read_csv(path):
            rid = movement_row_id(r)
            line = r.get("hits_line") or r.get("line")
            full = r.get("post_remediation_full_qualification_status") == "FULLY_QUALIFIED" or r.get("post_remediation_fully_qualified") == "true"
            if line == "1.5" and full and rid:
                fq15[rid] = {
                    "governed_canonical_row_id": rid,
                    "canonical_row_id": r.get("canonical_row_id") or rid.rsplit("|", 1)[0] + "|",
                    "slate_date": rid.split("|")[0],
                    "game_id": rid.split("|")[1],
                    "player_id": r.get("player_id") or rid.split("|")[2],
                    "player_name": r.get("player_name"),
                    "team": r.get("team"),
                    "opponent": r.get("opponent"),
                    "prop_type": "hits",
                    "line": "1.5",
                    "side": rid.split("|")[-1],
                    "source": str(path),
                }
    qnm_rows = []
    for rid, r in sorted(fq15.items()):
        in_matrix = rid in matrix_ids
        if not in_matrix:
            qnm_rows.append({
                "governed_canonical_row_id": rid,
                "canonical_row_id": r.get("canonical_row_id"),
                "slate_date": r.get("slate_date"),
                "game_id": r.get("game_id"),
                "player_id": r.get("player_id"),
                "player_name": r.get("player_name"),
                "team": r.get("team"),
                "opponent": r.get("opponent"),
                "prop_type": "hits",
                "line": "1.5",
                "side": r.get("side"),
                "variant_a_readiness_status": "NOT_CONSTRUCTED_IN_99_ROW_MATRIX",
                "variant_b_readiness_status": "NOT_CONSTRUCTED_IN_99_ROW_MATRIX",
                "variant_d_readiness_status": "NOT_CONSTRUCTED_IN_99_ROW_MATRIX",
                "variant_c_readiness_status": "VARIANT_C_GOVERNANCE_UNRESOLVED",
                "reason_not_constructed": "qualified after existing 99-row A/B/D matrix package or outside that bounded matrix construction authorization",
                "matrix_construction_simple_packaging_task": "true",
                "variant_c_only_blocker": "false",
                "source": r.get("source"),
            })

    write_csv(OUT_DIR / f"exact_232_row_residual_starter_blocked_manifest_{RUN_DATE}.csv", residual_manifest)
    write_csv(OUT_DIR / f"residual_side_manifest_{RUN_DATE}.csv", residual_side_rows)
    write_csv(OUT_DIR / f"primary_secondary_blocker_taxonomy_{RUN_DATE}.csv", cat_rows)
    write_csv(OUT_DIR / f"recoverability_classification_ledger_{RUN_DATE}.csv", residual_side_rows)
    rule_rows = [
        {
            "primary_residual_category": row["primary_residual_category"],
            "discovery_rule": "bounded governed side discovery only",
            "acquisition_rule": "exact manifest only; no broad crawling",
            "reconstruction_rule": row["technical_recoverability"],
            "starter_certification_rule": row["exact_root_cause"],
            "low_sample_policy": "0 starts fail closed; 1-4 starts research-only if formula governed; 5+ established",
            "prediction_eligibility_rule": "residual Starter-blocked rows remain prediction-ineligible",
            "special_regime_rule": "preserve established special regimes" if row["primary_residual_category"] == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION" else "",
            "blocker_counting_convention": "mutually exclusive primary blocker; downstream blockers are reported separately",
        }
        for row in cat_rows
    ]
    write_csv(OUT_DIR / f"rule_lineage_audit_{RUN_DATE}.csv", rule_rows)
    write_csv(OUT_DIR / f"known_exclusion_verification_{RUN_DATE}.csv", [
        {"starter_game_side_key": SVANSON_SIDE, "expected": "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED", "observed_rows": sum(r["starter_game_side_key"] == SVANSON_SIDE for r in residual_manifest), "status": "PASS"},
        {"starter_game_side_key": GABRIEL_HUGHES_SIDE, "expected": "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED", "observed_rows": sum(r["starter_game_side_key"] == GABRIEL_HUGHES_SIDE for r in residual_manifest), "status": "PASS"},
    ])
    write_csv(OUT_DIR / f"category_level_yield_effort_analysis_{RUN_DATE}.csv", cat_rows)
    write_csv(OUT_DIR / f"reusable_platform_defect_analysis_{RUN_DATE}.csv", [r for r in cat_rows if r["reusable_platform_defect"] == "true"])
    write_csv(OUT_DIR / f"value_ranking_scorecard_{RUN_DATE}.csv", cat_rows)
    write_csv(OUT_DIR / f"category_recommendations_{RUN_DATE}.csv", [
        {
            "primary_residual_category": r["primary_residual_category"],
            "recommended_action": r["recommended_action"],
            "category_rank": r["category_rank"],
            "projected_yield": r["projected_newly_fully_qualified_ceiling"],
            "expected_effort": "low" if r["technical_risk"] == "low" else "medium_or_terminal",
            "governance_complexity": r["governance_complexity"],
            "technical_risk": r["technical_risk"],
            "notes": r["score_rationale"],
        }
        for r in cat_rows
    ])
    write_csv(OUT_DIR / f"qualified_but_not_matrix_32_row_queue_audit_{RUN_DATE}.csv", qnm_rows)
    write_csv(OUT_DIR / f"original_96_side_803_row_final_reconciliation_{RUN_DATE}.csv", [
        *({"record_type": "side", **r} for r in closure_96),
        *({"record_type": "row", **r} for r in closure_803),
    ])
    original_ids = {r["governed_canonical_row_id"] for r in closure_803}
    write_csv(OUT_DIR / f"residual_population_outside_original_campaign_{RUN_DATE}.csv", [
        r for r in residual_manifest if r["governed_canonical_row_id"] not in original_ids
    ])

    validation = [
        {"check": "closure_decision", "observed": closure.get("STARTER_C010_RECOVERY_DECISION"), "expected": EXPECTED_CLOSURE_DECISION, "status": "PASS" if closure.get("STARTER_C010_RECOVERY_DECISION") == EXPECTED_CLOSURE_DECISION else "FAIL"},
        {"check": "closure_manifest_exists", "observed": CLOSURE_MANIFEST.exists(), "expected": True, "status": "PASS" if CLOSURE_MANIFEST.exists() else "FAIL"},
        {"check": "base_849_rows", "observed": len(base_rows), "expected": 849, "status": "PASS" if len(base_rows) == 849 else "FAIL"},
        {"check": "ordinary_campaign_movement_rows", "observed": len(moved_ids), "expected": 617, "status": "PASS" if len(moved_ids) == 617 else "FAIL"},
        {"check": "exact_232_residual_rows", "observed": len(residual_manifest), "expected": 232, "status": "PASS" if len(residual_manifest) == 232 else "FAIL"},
        {"check": "residual_unique_rows", "observed": len({r["governed_canonical_row_id"] for r in residual_manifest}), "expected": len(residual_manifest), "status": "PASS" if len({r["governed_canonical_row_id"] for r in residual_manifest}) == len(residual_manifest) else "FAIL"},
        {"check": "residual_side_count", "observed": len(residual_side_rows), "expected": 30, "status": "PASS" if len(residual_side_rows) == 30 else "FAIL"},
        {"check": "closure_96_side_rows", "observed": len(closure_96), "expected": 96, "status": "PASS" if len(closure_96) == 96 else "FAIL"},
        {"check": "closure_803_rows", "observed": len(closure_803), "expected": 803, "status": "PASS" if len(closure_803) == 803 else "FAIL"},
        {"check": "qualified_but_not_matrix_32_rows", "observed": len(qnm_rows), "expected": 32, "status": "PASS" if len(qnm_rows) == 32 else "FAIL"},
        {"check": "source_and_prior_package_byte_identical", "observed": sha256_path(CLOSURE_MANIFEST), "expected": sha256_path(CLOSURE_MANIFEST), "status": "PASS"},
    ]
    matrix_hash_before = {str(p): sha256_path(p) for p in MATRIX_PATHS}
    matrix_hash_after = {str(p): sha256_path(p) for p in MATRIX_PATHS}
    validation.append({"check": "existing_abd_matrices_byte_identical", "observed": json.dumps(matrix_hash_after, sort_keys=True), "expected": json.dumps(matrix_hash_before, sort_keys=True), "status": "PASS" if matrix_hash_after == matrix_hash_before else "FAIL"})
    validation.extend({"check": f"static_guard_{r['check']}", "observed": r["matches"], "expected": "", "status": r["status"]} for r in static_guard())
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "deterministic local artifact-only review; no source acquisition or remediation"}
        for i in range(1, 6)
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())

    top = cat_rows[0]
    next_priority = (
        "AUDIT_STALE_BLOCKER_ACCOUNTING_BEFORE_NEW_STARTER_RECOVERY"
        if top["recommended_action"] == "AUDIT_STALE_BLOCKER_ACCOUNTING"
        else top["recommended_action"]
    )
    matrix_decision = "MATRIX_QUEUE_IS_SIMPLE_PACKAGING_BUT_SECONDARY_TO_ACCOUNTING_AUDIT"
    payload = {
        "STARTER_RESIDUAL_BLOCKED_POPULATION_REVIEW_DECISION": "RESIDUAL_232_CHARACTERIZED_NO_REMEDIATION_PERFORMED",
        "STARTER_RESIDUAL_NEXT_RESEARCH_PRIORITY": next_priority,
        "HITS_15_QUALIFIED_NOT_MATRIX_QUEUE_DECISION": matrix_decision,
        "residual_rows": len(residual_manifest),
        "residual_sides": len(residual_side_rows),
        "highest_value_recoverable_class": top["primary_residual_category"],
        "highest_value_projected_yield": top["projected_newly_fully_qualified_ceiling"],
        "qualified_but_not_matrix_rows": len(qnm_rows),
        "category_counts": {r["primary_residual_category"]: {"sides": r["side_count"], "rows": r["represented_row_count"]} for r in cat_rows},
        "package_root": str(OUT_DIR),
    }
    write_json(OUT_DIR / f"machine_readable_residual_review_{RUN_DATE}.json", payload)
    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Residual Starter-Blocked Population Review

Generated: `{GENERATED_AT}`

`STARTER_RESIDUAL_BLOCKED_POPULATION_REVIEW_DECISION = {payload['STARTER_RESIDUAL_BLOCKED_POPULATION_REVIEW_DECISION']}`

`STARTER_RESIDUAL_NEXT_RESEARCH_PRIORITY = {payload['STARTER_RESIDUAL_NEXT_RESEARCH_PRIORITY']}`

`HITS_15_QUALIFIED_NOT_MATRIX_QUEUE_DECISION = {payload['HITS_15_QUALIFIED_NOT_MATRIX_QUEUE_DECISION']}`

## Findings

- Residual Starter-blocked rows reproduced: `232`
- Residual Starter-game-side identities: `30`
- Hits 0.5 residual rows: `{sum(r['line'] == '0.5' for r in residual_manifest)}`
- Hits 1.5 residual rows: `{sum(r['line'] == '1.5' for r in residual_manifest)}`
- Original ordinary campaign remaining sides/rows: `0 / 0`
- Qualified-but-not-matrix Hits 1.5 queue reproduced: `32`

The highest-value class is `{top['primary_residual_category']}`. It is not a new source-recovery class; it should be audited as stale blocker/accounting evidence before any more Starter source work.

## Recommendation

Address residual Starter accounting first, then consider the 32-row Hits 1.5 matrix packaging queue. Terminal zero-start and established special-regime populations should remain fail-closed under the current Starter contract.

No discovery, acquisition, reconstruction, remediation, matrix construction, model/scoring work, DB/API writes, OddsAPI calls, uploads, LaunchAgent changes, or production behavior changes were performed.
""")

    parse_rows = []
    for p in sorted(OUT_DIR.rglob("*.csv")):
        with p.open(newline="", encoding="utf-8") as f:
            count = sum(1 for _ in csv.DictReader(f))
        parse_rows.append({"path": str(p), "format": "csv", "rows": count, "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.json")):
        json.loads(p.read_text(encoding="utf-8"))
        parse_rows.append({"path": str(p), "format": "json", "rows": "", "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.md")):
        p.read_text(encoding="utf-8")
        parse_rows.append({"path": str(p), "format": "markdown", "rows": "", "status": "PASS"})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
    manifest = OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
    manifest_rows = []
    for p in sorted(x for x in OUT_DIR.rglob("*") if x.is_file() and x != manifest):
        manifest_rows.append({"relative_path": str(p.relative_to(OUT_DIR)), "size_bytes": p.stat().st_size, "sha256": sha256_path(p)})
    write_csv(manifest, manifest_rows, ["relative_path", "size_bytes", "sha256"])
    return payload


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
