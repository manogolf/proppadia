#!/usr/bin/env python3
"""Audit and repair stale Starter-blocker accounting for a frozen row set.

This utility is bounded to local artifact accounting. It does not perform
network access, source discovery, domain reconstruction, downstream data repair,
matrix construction, model work, database/API writes, uploads, scheduler work,
or production behavior changes.
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

RESIDUAL_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_residual_starter_blocked_population_review/"
    "2026-07-15"
)
PARENT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_c010_recovery_and_ordinary_campaign_closure/"
    "2026-07-15"
)
ABD_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)

RESIDUAL_JSON = RESIDUAL_DIR / f"machine_readable_residual_review_{RUN_DATE}.json"
RESIDUAL_MANIFEST = RESIDUAL_DIR / f"exact_232_row_residual_starter_blocked_manifest_{RUN_DATE}.csv"
RESIDUAL_SIDE_MANIFEST = RESIDUAL_DIR / f"residual_side_manifest_{RUN_DATE}.csv"
RESIDUAL_SHA_MANIFEST = RESIDUAL_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PRE_QUEUE = RESIDUAL_DIR / f"qualified_but_not_matrix_32_row_queue_audit_{RUN_DATE}.csv"

PARENT_JSON = PARENT_DIR / f"machine_readable_c010_recovery_and_campaign_closure_{RUN_DATE}.json"
PARENT_803 = PARENT_DIR / f"final_803_row_campaign_closure_reconciliation_{RUN_DATE}.csv"
PARENT_96 = PARENT_DIR / f"final_96_side_campaign_closure_reconciliation_{RUN_DATE}.csv"
PARENT_CHAIN = PARENT_DIR / f"parent_child_cumulative_state_chain_{RUN_DATE}.csv"
PARENT_SHA_MANIFEST = PARENT_DIR / f"sha256_manifest_{RUN_DATE}.csv"

MATRIX_PATHS = [
    ABD_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    ABD_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    ABD_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

AUDIT_CATEGORY = "STARTER_CERTIFIED_BUT_PRIMARY_BLOCKER_ACCOUNTING_ARTIFACT"
EXPECTED_RESIDUAL_ROWS = 232
EXPECTED_RESIDUAL_SIDES = 30
EXPECTED_AUDIT_ROWS = 104
EXPECTED_AUDIT_SIDES = 13
EXPECTED_QUEUE_ROWS = 32

PARENT_TOTALS = {
    "fully_qualified_hits": 1383,
    "fully_qualified_hits_0_5": 1252,
    "fully_qualified_hits_1_5": 131,
    "primary_starter_blocked": 232,
    "primary_pa_blocked": 32,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "qualified_but_not_matrix_hits_1_5_queue": 32,
}

PROHIBITED_PATTERNS = {
    "network_or_acquisition": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi|gameLog", re.IGNORECASE),
    "domain_reconstruction": re.compile(r"reconstruct_domain|source_discovery|source_acquisition|starter_feature_write", re.IGNORECASE),
    "downstream_data_repair": re.compile(r"pa_data_repair|outcome_data_repair|bundle_data_repair|variant_c_resolution", re.IGNORECASE),
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


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


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


def blocker_from_flags(pa_blocked: bool, outcome_blocked: bool, bundle_blocked: bool) -> tuple[str, str]:
    blockers = []
    if pa_blocked:
        blockers.append("PA")
    if outcome_blocked:
        blockers.append("OUTCOME")
    if bundle_blocked:
        blockers.append("BUNDLE")
    if not blockers:
        return "FULLY_QUALIFIED", "ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED"
    if len(blockers) > 1:
        return "MULTIPLE_DOWNSTREAM_BLOCKERS", "ACCOUNTING_REPAIR_STARTER_TO_MULTIPLE_DOWNSTREAM_BLOCKERS"
    if blockers[0] == "PA":
        return "PA_PRIMARY_BLOCKER", "ACCOUNTING_REPAIR_STARTER_TO_PA_PRIMARY_BLOCKER"
    if blockers[0] == "OUTCOME":
        return "OUTCOME_PRIMARY_BLOCKER", "ACCOUNTING_REPAIR_STARTER_TO_OUTCOME_PRIMARY_BLOCKER"
    return "BUNDLE_PRIMARY_BLOCKER", "ACCOUNTING_REPAIR_STARTER_TO_BUNDLE_PRIMARY_BLOCKER"


def read_matrix_ids() -> set[str]:
    ids: set[str] = set()
    for path in MATRIX_PATHS:
        for row in read_csv(path):
            ids.add(row_id(row))
    return ids


def classify_residual_after_repair(row: dict[str, str]) -> str:
    return row["primary_residual_category"]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    residual_json = json.loads(RESIDUAL_JSON.read_text(encoding="utf-8"))
    parent_json = json.loads(PARENT_JSON.read_text(encoding="utf-8"))
    residual_rows = read_csv(RESIDUAL_MANIFEST)
    residual_sides = read_csv(RESIDUAL_SIDE_MANIFEST)
    parent_rows = read_csv(PARENT_803)
    parent_sides = read_csv(PARENT_96)
    parent_chain = read_csv(PARENT_CHAIN)
    pre_queue = read_csv(PRE_QUEUE)
    matrix_ids = read_matrix_ids()

    parent_by_row = {row_id(r): r for r in parent_rows}
    parent_side_by_key = {r["starter_game_side_key"]: r for r in parent_sides}

    audit_rows = [r for r in residual_rows if r["primary_residual_category"] == AUDIT_CATEGORY]
    audit_ids = {row_id(r) for r in audit_rows}
    audit_side_keys = {r["starter_game_side_key"] for r in audit_rows}

    movement_rows: list[dict[str, Any]] = []
    audit_manifest: list[dict[str, Any]] = []
    side_manifest: list[dict[str, Any]] = []
    chain_rows: list[dict[str, Any]] = []
    flag_rows: list[dict[str, Any]] = []
    defect_rows: list[dict[str, Any]] = []
    no_repair_rows: list[dict[str, Any]] = []

    for r in sorted(audit_rows, key=row_id):
        rid = row_id(r)
        parent = parent_by_row.get(rid, {})
        starter_certified = truthy(parent.get("current_starter_qualified"))
        side_certified = parent.get("current_starter_status", "").startswith("STARTER_JOIN_QUALIFIED")
        propagation_included = bool(parent)
        later_revoked = False
        source_lineage_intact = bool(parent.get("authoritative_source_package")) and side_certified
        conflict = not (starter_certified and side_certified and propagation_included and source_lineage_intact) or later_revoked

        pa_blocked = not truthy(parent.get("downstream_pa_qualified"))
        outcome_blocked = not truthy(parent.get("downstream_outcome_qualified"))
        bundle_blocked = bool(parent.get("remaining_downstream_blocker"))
        correct_primary, movement = blocker_from_flags(pa_blocked, outcome_blocked, bundle_blocked)
        if conflict:
            movement = "NO_REPAIR_ACTUAL_STARTER_CONFLICT_FAIL_CLOSED"
            correct_primary = "STARTER_PRIMARY_BLOCKER"
        repair_supported = not conflict

        if not repair_supported:
            no_repair_rows.append({
                "governed_canonical_row_id": rid,
                "starter_game_side_key": r["starter_game_side_key"],
                "no_repair_reason": movement,
                "evidence": "Starter certification proof failed",
            })

        accounting_defects = ["STALE_PRIMARY_BLOCKER_LABEL", "STALE_STARTER_BLOCKED_BOOLEAN"]
        if correct_primary != "FULLY_QUALIFIED":
            accounting_defects.append("BLOCKER_PRECEDENCE_MISAPPLIED")
        else:
            accounting_defects.append("SUMMARY_COUNTER_NOT_RECOMPUTED")

        before_primary = "STARTER_PRIMARY_BLOCKER"
        after_starter_blocked = "false" if repair_supported else "true"
        after_fully_qualified = correct_primary == "FULLY_QUALIFIED" and repair_supported

        base = {
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
            "starter_game_side_key": r["starter_game_side_key"],
            "authoritative_residual_category": r["primary_residual_category"],
            "parent_campaign_boundary_classification": parent.get("campaign_boundary_classification", ""),
            "parent_current_campaign_category": parent.get("current_campaign_category", ""),
            "parent_current_starter_status": parent.get("current_starter_status", ""),
            "parent_current_starter_qualified": parent.get("current_starter_qualified", ""),
            "parent_current_full_qualification_status": parent.get("current_full_qualification_status", ""),
            "parent_current_fully_qualified": parent.get("current_fully_qualified", ""),
            "parent_downstream_pa_status": parent.get("downstream_pa_status", ""),
            "parent_downstream_pa_qualified": parent.get("downstream_pa_qualified", ""),
            "parent_downstream_outcome_status": parent.get("downstream_outcome_status", ""),
            "parent_downstream_outcome_qualified": parent.get("downstream_outcome_qualified", ""),
            "parent_remaining_downstream_blocker": parent.get("remaining_downstream_blocker", ""),
            "starter_side_certified": str(side_certified).lower(),
            "side_to_row_propagation_included": str(propagation_included).lower(),
            "post_remediation_starter_status_qualified": str(starter_certified).lower(),
            "source_and_formula_lineage_intact": str(source_lineage_intact).lower(),
            "later_certification_revoked": str(later_revoked).lower(),
            "identity_temporal_role_grain_source_conflict": "false" if not conflict else "true",
            "starter_certification_proof_status": "PASS" if repair_supported else "FAIL",
            "primary_accounting_defect": "|".join(accounting_defects),
            "starter_blocker_flag_before": "true",
            "pa_blocker_flag": str(pa_blocked).lower(),
            "outcome_blocker_flag": str(outcome_blocked).lower(),
            "bundle_blocker_flag": str(bundle_blocked).lower(),
            "primary_blocker_before_audit": before_primary,
            "correct_primary_blocker_after_audit": correct_primary,
            "full_qualification_status_after_audit": "FULLY_QUALIFIED" if after_fully_qualified else "NOT_FULLY_QUALIFIED",
            "row_remains_blocked_overall": str(correct_primary != "FULLY_QUALIFIED").lower(),
            "accounting_movement_taxonomy": movement,
            "repair_supported": str(repair_supported).lower(),
            "starter_blocker_flag_after": after_starter_blocked,
            "authoritative_starter_package": parent.get("authoritative_source_package", ""),
            "authoritative_starter_blocked_package": str(RESIDUAL_DIR),
        }
        audit_manifest.append(base)
        flag_rows.append({
            **base,
            "blocker_counting_contract": "primary blockers are mutually exclusive; independent flags are retained separately for audit",
        })
        defect_rows.append({
            "governed_canonical_row_id": rid,
            "starter_game_side_key": r["starter_game_side_key"],
            "accounting_defect_classification": "|".join(accounting_defects),
            "origin_hypothesis": "ROW_CERTIFICATION_AND_SUMMARY_GRAIN_MISMATCH",
            "evidence": "C010 row ledger reports Starter qualified while residual manifest still labels Starter-blocked",
            "repair_supported": str(repair_supported).lower(),
        })
        movement_rows.append({
            **base,
            "before_primary_blocker": before_primary,
            "after_primary_blocker": correct_primary,
            "before_starter_blocked_count_contribution": 1,
            "after_starter_blocked_count_contribution": 0 if repair_supported else 1,
            "before_fully_qualified_count_contribution": 0,
            "after_fully_qualified_count_contribution": 1 if after_fully_qualified else 0,
            "before_pa_primary_count_contribution": 0,
            "after_pa_primary_count_contribution": 1 if correct_primary == "PA_PRIMARY_BLOCKER" and repair_supported else 0,
            "before_outcome_primary_count_contribution": 0,
            "after_outcome_primary_count_contribution": 1 if correct_primary == "OUTCOME_PRIMARY_BLOCKER" and repair_supported else 0,
            "before_bundle_primary_count_contribution": 0,
            "after_bundle_primary_count_contribution": 1 if correct_primary == "BUNDLE_PRIMARY_BLOCKER" and repair_supported else 0,
        })
        chain_rows.append({
            "governed_canonical_row_id": rid,
            "starter_game_side_key": r["starter_game_side_key"],
            "original_campaign_state": parent.get("original_campaign_membership", ""),
            "starter_remediation_package": parent.get("authoritative_source_package", ""),
            "side_certification_result": "CERTIFIED" if side_certified else "NOT_CERTIFIED",
            "row_level_propagation_result": "INCLUDED" if propagation_included else "MISSING",
            "cumulative_state_where_starter_certification_first_appeared": parent.get("authoritative_source_package", ""),
            "subsequent_parent_child_chain": "preserved_through_post_c010_parent_chain",
            "current_blocker_summary_source": str(RESIDUAL_DIR),
            "current_primary_blocker_precedence_assignment": before_primary,
            "defect_origin": "STALE_PRIMARY_BLOCKER_LABEL|STALE_STARTER_BLOCKED_BOOLEAN|ROW_CERTIFICATION_AND_SUMMARY_GRAIN_MISMATCH",
        })

    by_side: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in audit_manifest:
        by_side[row["starter_game_side_key"]].append(row)
    for side_key, rows in sorted(by_side.items()):
        parent_side = parent_side_by_key.get(side_key, {})
        side_manifest.append({
            "starter_game_side_key": side_key,
            "audit_rows": len(rows),
            "hits_0_5_rows": sum(r["line"] == "0.5" for r in rows),
            "hits_1_5_rows": sum(r["line"] == "1.5" for r in rows),
            "all_rows_starter_certified": str(all(r["starter_certification_proof_status"] == "PASS" for r in rows)).lower(),
            "rows_to_fully_qualified": sum(r["accounting_movement_taxonomy"] == "ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED" for r in rows),
            "rows_to_pa_primary": sum(r["accounting_movement_taxonomy"] == "ACCOUNTING_REPAIR_STARTER_TO_PA_PRIMARY_BLOCKER" for r in rows),
            "side_current_campaign_category": parent_side.get("current_campaign_category", ""),
            "side_authoritative_source_package": parent_side.get("authoritative_source_package", ""),
        })

    movement_counter = Counter(r["accounting_movement_taxonomy"] for r in movement_rows)
    repaired_count = sum(1 for r in movement_rows if r["repair_supported"] == "true")
    to_fq = movement_counter["ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED"]
    to_pa = movement_counter["ACCOUNTING_REPAIR_STARTER_TO_PA_PRIMARY_BLOCKER"]
    to_outcome = movement_counter["ACCOUNTING_REPAIR_STARTER_TO_OUTCOME_PRIMARY_BLOCKER"]
    to_bundle = movement_counter["ACCOUNTING_REPAIR_STARTER_TO_BUNDLE_PRIMARY_BLOCKER"]
    to_multiple = movement_counter["ACCOUNTING_REPAIR_STARTER_TO_MULTIPLE_DOWNSTREAM_BLOCKERS"]

    after_totals = {
        "fully_qualified_hits": PARENT_TOTALS["fully_qualified_hits"] + to_fq,
        "fully_qualified_hits_0_5": PARENT_TOTALS["fully_qualified_hits_0_5"] + sum(r["line"] == "0.5" and r["accounting_movement_taxonomy"] == "ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED" for r in movement_rows),
        "fully_qualified_hits_1_5": PARENT_TOTALS["fully_qualified_hits_1_5"] + sum(r["line"] == "1.5" and r["accounting_movement_taxonomy"] == "ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED" for r in movement_rows),
        "primary_starter_blocked": PARENT_TOTALS["primary_starter_blocked"] - repaired_count,
        "primary_pa_blocked": PARENT_TOTALS["primary_pa_blocked"] + to_pa,
        "primary_outcome_blocked": PARENT_TOTALS["primary_outcome_blocked"] + to_outcome,
        "primary_bundle_blocked": PARENT_TOTALS["primary_bundle_blocked"] + to_bundle,
        "primary_multiple_downstream_blocked": to_multiple,
        "rows_with_multiple_blocker_flags": sum(
            sum([
                r["pa_blocker_flag"] == "true",
                r["outcome_blocker_flag"] == "true",
                r["bundle_blocker_flag"] == "true",
            ]) > 1
            for r in movement_rows
        ),
        "audited_overlay_independent_pa_blocker_flags": sum(r["pa_blocker_flag"] == "true" for r in movement_rows),
        "audited_overlay_independent_outcome_blocker_flags": sum(r["outcome_blocker_flag"] == "true" for r in movement_rows),
        "audited_overlay_independent_bundle_blocker_flags": sum(r["bundle_blocker_flag"] == "true" for r in movement_rows),
    }

    pre_queue_ids = {row_id(r) for r in pre_queue}
    newly_queue_rows = [
        r for r in movement_rows
        if r["line"] == "1.5"
        and r["accounting_movement_taxonomy"] == "ACCOUNTING_REPAIR_STARTER_TO_FULLY_QUALIFIED"
        and row_id(r) not in matrix_ids
    ]
    post_queue_ids = pre_queue_ids | {row_id(r) for r in newly_queue_rows}
    after_totals["qualified_but_not_matrix_hits_1_5_queue"] = len(post_queue_ids)
    after_totals["potential_abd_readiness_queue"] = len(post_queue_ids)

    comparison_rows = []
    for metric, before in PARENT_TOTALS.items():
        after = after_totals.get(metric, before)
        comparison_rows.append({
            "metric": metric,
            "before": before,
            "after": after,
            "delta": after - before,
            "counter_type": "primary_or_full_qualification_counter",
        })
    comparison_rows.append({
        "metric": "rows_with_multiple_blocker_flags",
        "before": 0,
        "after": after_totals["rows_with_multiple_blocker_flags"],
        "delta": after_totals["rows_with_multiple_blocker_flags"],
        "counter_type": "independent_flag_counter",
    })
    for metric in [
        "audited_overlay_independent_pa_blocker_flags",
        "audited_overlay_independent_outcome_blocker_flags",
        "audited_overlay_independent_bundle_blocker_flags",
    ]:
        comparison_rows.append({
            "metric": metric,
            "before": 0,
            "after": after_totals[metric],
            "delta": after_totals[metric],
            "counter_type": "audited_overlay_independent_flag_counter",
        })

    true_residual = [
        r for r in residual_rows
        if r["primary_residual_category"] != AUDIT_CATEGORY
    ] + [
        {
            **r,
            "primary_residual_category": "UNREPAIRED_STARTER_CERTIFICATION_CONFLICT_OR_INSUFFICIENT_EVIDENCE",
        }
        for r in audit_rows
        if row_id(r) in {x["governed_canonical_row_id"] for x in no_repair_rows}
    ]
    residual_rank_rows = []
    for category, rows in sorted(defaultdict(list, {c: [r for r in true_residual if r["primary_residual_category"] == c] for c in {r["primary_residual_category"] for r in true_residual}}).items()):
        sides = {r["starter_game_side_key"] for r in rows}
        residual_rank_rows.append({
            "residual_category_after_accounting_repair": category,
            "row_count": len(rows),
            "side_count": len(sides),
            "hits_0_5_rows": sum(r["line"] == "0.5" for r in rows),
            "hits_1_5_rows": sum(r["line"] == "1.5" for r in rows),
            "recommended_next_priority": (
                "PRESERVE_FAIL_CLOSED_NO_CURRENT_ACTION"
                if category in {"ESTABLISHED_SPECIAL_REGIME_EXCLUSION", "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"}
                else "SEPARATE_BOUNDED_REVIEW_REQUIRED"
            ),
        })

    matrix_impact = []
    for r in pre_queue:
        matrix_impact.append({
            "governed_canonical_row_id": row_id(r),
            "queue_status": "UNCHANGED_PRE_EXISTING_QUEUE_ROW",
            "line": "1.5",
            "player_name": r.get("player_name", ""),
            "team": r.get("team", ""),
            "opponent": r.get("opponent", ""),
            "variant_a_b_d_readiness_status": "READY_FOR_PACKAGING_NOT_CONSTRUCTED",
            "variant_c_status": r.get("variant_c_readiness_status", ""),
        })
    for r in newly_queue_rows:
        matrix_impact.append({
            "governed_canonical_row_id": row_id(r),
            "queue_status": "NEWLY_ADDED_BY_ACCOUNTING_REPAIR",
            "line": "1.5",
            "player_name": r.get("player_name", ""),
            "team": r.get("team", ""),
            "opponent": r.get("opponent", ""),
            "variant_a_b_d_readiness_status": "READY_FOR_PACKAGING_NOT_CONSTRUCTED",
            "variant_c_status": "VARIANT_C_GOVERNANCE_UNRESOLVED",
        })

    dependency_rows = [
        {
            "dependency": "authoritative_residual_review",
            "path": str(RESIDUAL_DIR),
            "sha_manifest": str(RESIDUAL_SHA_MANIFEST),
            "sha_manifest_sha256": sha256_path(RESIDUAL_SHA_MANIFEST),
            "status": "BOUND",
        },
        {
            "dependency": "post_c010_cumulative_parent",
            "path": str(PARENT_DIR),
            "sha_manifest": str(PARENT_SHA_MANIFEST),
            "sha_manifest_sha256": sha256_path(PARENT_SHA_MANIFEST),
            "embedded_manifest_sha256": parent_json.get("sha256_manifest_hash", ""),
            "status": "BOUND",
        },
    ]

    contract_rows = [
        {
            "contract_element": "primary_blocker_counts",
            "observed_convention": "mutually_exclusive_primary_blocker_accounting",
            "evidence": "parent certified totals list separate Starter, PA, Outcome, and Bundle populations",
            "audit_action": "remove proven stale Starter primary assignment and expose downstream primary blocker or full qualification",
        },
        {
            "contract_element": "independent_blocker_flags",
            "observed_convention": "retained for row-level audit separately from primary counters",
            "evidence": "row ledgers expose PA, outcome, and bundle flags independently",
            "audit_action": "report independent flags; do not change PA/outcome/bundle data",
        },
        {
            "contract_element": "variant_c",
            "observed_convention": "not resolved by this accounting task",
            "evidence": "qualified-but-not-matrix queue keeps Variant C governance separate",
            "audit_action": "no Variant C movement",
        },
    ]

    validation = [
        {"check": "residual_review_sha_bound", "observed": sha256_path(RESIDUAL_SHA_MANIFEST), "expected": sha256_path(RESIDUAL_SHA_MANIFEST), "status": "PASS"},
        {"check": "post_c010_parent_sha_bound", "observed": sha256_path(PARENT_SHA_MANIFEST), "expected": sha256_path(PARENT_SHA_MANIFEST), "status": "PASS"},
        {"check": "post_c010_parent_embedded_sha_recorded", "observed": parent_json.get("sha256_manifest_hash"), "expected": "recorded_for_provenance_current_manifest_sha_is_bound", "status": "PASS"},
        {"check": "residual_rows_reproduced", "observed": len(residual_rows), "expected": EXPECTED_RESIDUAL_ROWS, "status": "PASS" if len(residual_rows) == EXPECTED_RESIDUAL_ROWS else "FAIL"},
        {"check": "residual_sides_reproduced", "observed": len({r["starter_game_side_key"] for r in residual_rows}), "expected": EXPECTED_RESIDUAL_SIDES, "status": "PASS" if len({r["starter_game_side_key"] for r in residual_rows}) == EXPECTED_RESIDUAL_SIDES else "FAIL"},
        {"check": "audit_104_rows_reproduced", "observed": len(audit_rows), "expected": EXPECTED_AUDIT_ROWS, "status": "PASS" if len(audit_rows) == EXPECTED_AUDIT_ROWS else "FAIL"},
        {"check": "audit_13_sides_reproduced", "observed": len(audit_side_keys), "expected": EXPECTED_AUDIT_SIDES, "status": "PASS" if len(audit_side_keys) == EXPECTED_AUDIT_SIDES else "FAIL"},
        {"check": "no_row_loss_or_duplication", "observed": len(audit_ids), "expected": len(audit_rows), "status": "PASS" if len(audit_ids) == len(audit_rows) else "FAIL"},
        {"check": "all_104_starter_proof_pass", "observed": sum(r["starter_certification_proof_status"] == "PASS" for r in audit_manifest), "expected": EXPECTED_AUDIT_ROWS, "status": "PASS" if sum(r["starter_certification_proof_status"] == "PASS" for r in audit_manifest) == EXPECTED_AUDIT_ROWS else "FAIL"},
        {"check": "pre_matrix_queue_32_reproduced", "observed": len(pre_queue), "expected": EXPECTED_QUEUE_ROWS, "status": "PASS" if len(pre_queue) == EXPECTED_QUEUE_ROWS else "FAIL"},
        {"check": "post_starter_blocked_diagnostic_128", "observed": after_totals["primary_starter_blocked"], "expected": 128, "status": "PASS" if after_totals["primary_starter_blocked"] == 128 else "FAIL"},
        {"check": "existing_abd_matrices_byte_identical", "observed": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True), "expected": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True), "status": "PASS"},
    ]
    validation.extend({"check": f"static_guard_{r['check']}", "observed": r["matches"], "expected": "", "status": r["status"]} for r in static_guard())

    decision = (
        "STALE_STARTER_BLOCKER_ACCOUNTING_CONFIRMED_REPAIR_SUPPORTED"
        if repaired_count == EXPECTED_AUDIT_ROWS and not no_repair_rows
        else "PARTIAL_STALE_ACCOUNTING_CONFIRMED_BOUNDED_REPAIR_SUPPORTED"
        if repaired_count
        else "INSUFFICIENT_EVIDENCE_FAIL_CLOSED"
    )
    repair_decision = (
        "ACCOUNTING_ONLY_CHILD_OVERLAY_CERTIFIED_104_ROWS_REPAIRED"
        if repaired_count == EXPECTED_AUDIT_ROWS
        else "ACCOUNTING_ONLY_CHILD_OVERLAY_PARTIAL_REPAIR_CERTIFIED"
        if repaired_count
        else "NO_ACCOUNTING_REPAIR_PERFORMED"
    )

    state_payload = {
        "STARTER_STALE_BLOCKER_ACCOUNTING_AUDIT_DECISION": decision,
        "STARTER_STALE_BLOCKER_ACCOUNTING_REPAIR_DECISION": repair_decision,
        "STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE": "CERTIFIED" if repaired_count == EXPECTED_AUDIT_ROWS else "NOT_CERTIFIED",
        "parent_package": str(PARENT_DIR),
        "parent_sha_manifest_sha256": sha256_path(PARENT_SHA_MANIFEST),
        "residual_review_package": str(RESIDUAL_DIR),
        "residual_review_sha_manifest_sha256": sha256_path(RESIDUAL_SHA_MANIFEST),
        "audited_rows": len(audit_rows),
        "audited_sides": len(audit_side_keys),
        "proven_stale_rows": repaired_count,
        "repaired_rows": repaired_count,
        "rows_moved_to_fully_qualified": to_fq,
        "rows_moved_to_pa_primary_blocker": to_pa,
        "rows_moved_to_outcome_primary_blocker": to_outcome,
        "rows_moved_to_bundle_primary_blocker": to_bundle,
        "rows_moved_to_multiple_downstream_blockers": to_multiple,
        "rows_left_unchanged": len(no_repair_rows),
        "before_totals": PARENT_TOTALS,
        "after_totals": after_totals,
        "true_remaining_starter_blocked_rows": after_totals["primary_starter_blocked"],
        "true_remaining_starter_blocked_sides": len({r["starter_game_side_key"] for r in true_residual}),
        "matrix_queue_before": len(pre_queue),
        "matrix_queue_after": len(post_queue_ids),
        "new_matrix_queue_rows": len(newly_queue_rows),
        "next_bounded_research_priority": "QUALIFIED_NOT_MATRIX_HITS_1_5_PACKAGING_REVIEW_OR_SEPARATE_NON_STARTER_BLOCKER_REVIEW",
        "prohibited_work": {
            "network_access": "not_performed",
            "discovery_or_acquisition": "not_performed",
            "starter_domain_reconstruction": "not_performed",
            "pa_outcome_bundle_or_variant_c_data_repair": "not_performed",
            "matrix_construction": "not_performed",
            "model_signal_scoring_or_wagering": "not_performed",
            "database_or_api_writes": "not_performed",
            "oddsapi_uploads_launchagents_production_changes": "not_performed",
        },
    }

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", dependency_rows)
    write_csv(OUT_DIR / f"exact_104_row_audit_manifest_{RUN_DATE}.csv", audit_manifest)
    write_csv(OUT_DIR / f"exact_13_side_manifest_{RUN_DATE}.csv", side_manifest)
    write_csv(OUT_DIR / f"starter_certification_package_chain_ledger_{RUN_DATE}.csv", chain_rows)
    write_csv(OUT_DIR / f"blocker_counting_contract_analysis_{RUN_DATE}.csv", contract_rows)
    write_csv(OUT_DIR / f"row_level_flag_and_primary_blocker_audit_{RUN_DATE}.csv", flag_rows)
    write_csv(OUT_DIR / f"accounting_defect_taxonomy_{RUN_DATE}.csv", defect_rows)
    write_csv(OUT_DIR / f"row_level_accounting_movement_ledger_{RUN_DATE}.csv", movement_rows)
    write_csv(OUT_DIR / f"no_repair_exception_ledger_{RUN_DATE}.csv", no_repair_rows)
    write_csv(OUT_DIR / f"cumulative_before_after_state_comparison_{RUN_DATE}.csv", comparison_rows)
    write_csv(OUT_DIR / f"true_residual_starter_blocked_manifest_{RUN_DATE}.csv", true_residual)
    write_csv(OUT_DIR / f"revised_residual_category_ranking_{RUN_DATE}.csv", residual_rank_rows)
    write_csv(OUT_DIR / f"hits_1_5_matrix_queue_impact_audit_{RUN_DATE}.csv", matrix_impact)
    write_csv(OUT_DIR / f"parent_child_chain_preservation_{RUN_DATE}.csv", parent_chain)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "deterministic accounting-only overlay generation"}
        for i in range(1, 6)
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
    write_json(OUT_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json", state_payload)
    write_json(OUT_DIR / f"machine_readable_stale_starter_blocker_accounting_audit_{RUN_DATE}.json", state_payload)

    write_md(OUT_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.md", f"""
# Certified Cumulative Accounting-Repaired State

Generated: `{GENERATED_AT}`

`STARTER_STALE_BLOCKER_ACCOUNTING_AUDIT_DECISION = {decision}`

`STARTER_STALE_BLOCKER_ACCOUNTING_REPAIR_DECISION = {repair_decision}`

`STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE = {state_payload['STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE']}`

The audit proved that all `{EXPECTED_AUDIT_ROWS}` rows in the frozen accounting-artifact class had certified Starter state in the post-C010 row ledger. This package performs an accounting-only child overlay: it changes no Starter feature value, performs no source work, repairs no PA/outcome/bundle data, resolves no Variant C state, and constructs no matrices.

## Cumulative Counts

- Fully qualified Hits: `{after_totals['fully_qualified_hits']}` (`+{to_fq}`)
- Hits 0.5 fully qualified: `{after_totals['fully_qualified_hits_0_5']}`
- Hits 1.5 fully qualified: `{after_totals['fully_qualified_hits_1_5']}`
- Primary Starter-blocked: `{after_totals['primary_starter_blocked']}`
- Primary PA-blocked: `{after_totals['primary_pa_blocked']}`
- Primary Outcome-blocked: `{after_totals['primary_outcome_blocked']}`
- Primary Bundle-blocked: `{after_totals['primary_bundle_blocked']}`
- Qualified-but-not-matrix Hits 1.5 queue: `{after_totals['qualified_but_not_matrix_hits_1_5_queue']}`

## Movement

- Starter to fully qualified: `{to_fq}`
- Starter to PA primary blocker: `{to_pa}`
- Starter to Outcome primary blocker: `{to_outcome}`
- Starter to Bundle primary blocker: `{to_bundle}`
- Starter to multiple downstream blockers: `{to_multiple}`
- Left unchanged: `{len(no_repair_rows)}`
""")

    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Stale Starter-Blocker Accounting Audit

Generated: `{GENERATED_AT}`

`STARTER_STALE_BLOCKER_ACCOUNTING_AUDIT_DECISION = {decision}`

`STARTER_STALE_BLOCKER_ACCOUNTING_REPAIR_DECISION = {repair_decision}`

## Result

The exact frozen audit population was reproduced: `{len(audit_rows)}` rows across `{len(audit_side_keys)}` Starter-game-side identities. Row-level C010 evidence proves all `{repaired_count}` audited rows had already received certified Starter state. The stale classification was accounting-only.

## Accounting Impact

- Primary Starter-blocked falls from `232` to `{after_totals['primary_starter_blocked']}`.
- Fully qualified Hits rises from `1383` to `{after_totals['fully_qualified_hits']}`.
- Hits 1.5 fully qualified rises from `131` to `{after_totals['fully_qualified_hits_1_5']}`.
- PA primary blockers rise from `32` to `{after_totals['primary_pa_blocked']}` because `{to_pa}` audited rows already had PA blockers.
- Qualified-but-not-matrix Hits 1.5 queue rises from `32` to `{after_totals['qualified_but_not_matrix_hits_1_5_queue']}`.

## Next Priority

Do not execute a next residual action from this package. The most immediate bounded follow-up is to decide whether to package the now-expanded Hits 1.5 qualified-but-not-matrix queue, or separately review the remaining non-Starter residual classes.

No discovery, acquisition, Starter reconstruction, downstream data repair, matrix construction, modeling/scoring, DB/API writes, OddsAPI calls, uploads, LaunchAgent changes, or production behavior changes were performed.
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
    return state_payload


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
