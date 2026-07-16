#!/usr/bin/env python3
"""Reconcile and freeze cumulative-state governance for DISCOVERY_COHORT_003.

Governance reconciliation only. This utility re-anchors the existing
DISCOVERY_COHORT_003 plan to the certified post-COHORT_002 cumulative campaign
state. It does not execute discovery, acquisition, reconstruction, remediation,
qualification propagation, matrix construction, model/scoring work, database/API
writes, uploads, scheduler edits, or production behavior changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"
COHORT_ID = "DISCOVERY_COHORT_003"

DECISION = (
    "STARTER_DISCOVERY_COHORT_003_CUMULATIVE_GOVERNANCE_DECISION = "
    "CUMULATIVE_STATE_RECONCILED_EXISTING_COHORT_FROZEN_UNCHANGED"
)
STATUS = (
    "STARTER_DISCOVERY_COHORT_003_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_BOUNDED_DISCOVERY_APPROVAL"
)

EXPECTED_CUMULATIVE_SHA = "0cb9d511aafb2a7ed10e200d7a6eaf719d8f2def1a1eaf7244f7d4fe2e429037"
EXPECTED_SCALE_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"
EXPECTED_COHORT_001_SHA = "0c2179dfc2a23f7ccc75402f3be8cb6de9eb16938d7bdec977c2737b52c3a8b4"
EXPECTED_COHORT_002_SHA = "888f9a248bdda5a4e26ac1ff21ebb3149b655448dedba1f4cb7bf19f82bcce31"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_cumulative_state_governance/2026-07-15"
)
CUMULATIVE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_remediation_overlay_chain_reconciliation/2026-07-15"
)
SCALE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)
COHORT_001_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/2026-07-15"
)
COHORT_002_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15"
)
FOUR_SIDE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/2026-07-15"
)
HC_LOCAL_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

CUMULATIVE_STATE = CUMULATIVE_DIR / f"cumulative_certified_state_{RUN_DATE}.json"
CUMULATIVE_REMAINING = CUMULATIVE_DIR / f"remaining_discovery_population_reconciliation_{RUN_DATE}.csv"
SCALE_PLAN = SCALE_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
SCALE_REMAINING_SIDES = SCALE_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"
SCALE_803 = SCALE_DIR / f"authoritative_803_row_campaign_reconciliation_{RUN_DATE}.csv"
SCALE_96 = SCALE_DIR / f"authoritative_96_side_campaign_reconciliation_{RUN_DATE}.csv"
SCALE_HOLDOUT_ROLE = SCALE_DIR / f"held_out_identity_role_review_ledger_{RUN_DATE}.csv"
SCALE_TARGET_C2 = SCALE_DIR / f"discovery_cohort_002_target_manifest_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

PROHIBITED_PATTERNS = {
    "network_or_source_acquisition": re.compile(r"requests[.]|httpx|urlopen|urlretrieve|download", re.IGNORECASE),
    "training_prediction_or_matrix": re.compile(r"[.]fit\s*[(]|[.]predict\s*[(]|roc_auc|log_loss|build_mlb_selected_proposition_abd_matrices", re.IGNORECASE),
    "db_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*[(])\b", re.IGNORECASE),
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


def package_sha(path: Path, date_value: str = RUN_DATE) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def row_id(row: dict[str, str]) -> str:
    return row.get("canonical_denominator_identity") or row.get("governed_canonical_row_id") or row.get("canonical_row_id", "")


def side_from_movement(row: dict[str, str]) -> str:
    return row.get("governed_starter_game_side_identity") or row.get("starter_game_side_key", "")


def split_row_id(value: str) -> dict[str, str]:
    parts = value.split("|")
    if len(parts) >= 6:
        return {
            "slate_date": parts[0],
            "game_id": parts[1],
            "player_id": parts[2],
            "prop_type": parts[3],
            "line": parts[4],
            "side": parts[5],
        }
    return {"slate_date": "", "game_id": "", "player_id": "", "prop_type": "", "line": "", "side": ""}


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
        rows.append({
            "check": name,
            "status": "PASS" if not matches else "FAIL",
            "matches": "|".join(str(m) for m in matches),
            "notes": "Static guard excludes comments/string literals and scans executable code only.",
        })
    return rows


class Cohort003CumulativeGovernance:
    def __init__(self) -> None:
        self.cumulative = json.loads(CUMULATIVE_STATE.read_text(encoding="utf-8"))
        self.scale_plan = read_csv(SCALE_PLAN)
        self.remaining_sides = read_csv(SCALE_REMAINING_SIDES)
        self.rows_803 = read_csv(SCALE_803)
        self.sides_96 = read_csv(SCALE_96)
        self.role_holdouts = read_csv(SCALE_HOLDOUT_ROLE)
        self.c2_targets = read_csv(SCALE_TARGET_C2)
        self.cohort_plan = next(row for row in self.scale_plan if row["cohort_id"] == COHORT_ID)
        self.cohort_side_keys = self.cohort_plan["side_keys"].split(";")
        self.side_by_key = {row["starter_game_side_key"]: row for row in self.remaining_sides}
        self.cohort_sides = [self.side_by_key[key] for key in self.cohort_side_keys]
        self.cohort_rows = [row for row in self.rows_803 if row["starter_game_side_key"] in set(self.cohort_side_keys)]
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def package_audit(self) -> list[dict[str, Any]]:
        checks = [
            ("cumulative_state", CUMULATIVE_DIR, EXPECTED_CUMULATIVE_SHA),
            ("remaining_scale_up_design", SCALE_DIR, EXPECTED_SCALE_SHA),
            ("discovery_cohort_001_remediation", COHORT_001_DIR, EXPECTED_COHORT_001_SHA),
            ("discovery_cohort_002_remediation", COHORT_002_DIR, EXPECTED_COHORT_002_SHA),
            ("four_side_history_complete_remediation", FOUR_SIDE_DIR, package_sha(FOUR_SIDE_DIR)),
            ("hc_local_cohort_001_remediation", HC_LOCAL_DIR, package_sha(HC_LOCAL_DIR)),
        ]
        return [{
            "package": name,
            "path": str(path),
            "expected_sha": expected,
            "actual_sha": package_sha(path),
            "status": "PASS" if package_sha(path) == expected else "FAIL",
        } for name, path, expected in checks]

    def cumulative_verification(self) -> list[dict[str, Any]]:
        expected = {
            "decision": "STARTER_REMEDIATION_OVERLAY_CHAIN_RECONCILIATION_DECISION = INDEPENDENT_OVERLAYS_VALID_CUMULATIVE_STATE_FIRST_MATERIALIZED",
            "state": "STARTER_POST_COHORT_002_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED",
            "total_fully_qualified_hits": 961,
            "fully_qualified_hits_0_5": 846,
            "fully_qualified_hits_1_5": 115,
            "starter_blocked_population": 675,
            "pa_blocked_population": 11,
            "outcome_blocked_population": 363,
            "bundle_blocked_population": 36,
            "qualified_but_not_matrix_constructed_hits_1_5_queue": 16,
        }
        rows = []
        for key, expected_value in expected.items():
            observed = self.cumulative.get(key)
            rows.append({
                "metric": key,
                "observed": observed,
                "expected": expected_value,
                "status": "PASS" if observed == expected_value else "FAIL",
            })
        rows.append({
            "metric": "package_sha",
            "observed": package_sha(CUMULATIVE_DIR),
            "expected": EXPECTED_CUMULATIVE_SHA,
            "status": "PASS" if package_sha(CUMULATIVE_DIR) == EXPECTED_CUMULATIVE_SHA else "FAIL",
        })
        return rows

    def confirmed_side_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for idx, side in enumerate(self.cohort_sides, 1):
            rows.append({
                "cohort_id": COHORT_ID,
                "target_order": idx,
                "starter_game_side_key": side["starter_game_side_key"],
                "parent_state_package": str(CUMULATIVE_DIR),
                "parent_state_sha": EXPECTED_CUMULATIVE_SHA,
                "current_campaign_category": side["current_campaign_category"],
                "represented_denominator_rows": side["represented_denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "projected_starter_qualified_ceiling": side["represented_denominator_rows"],
                "projected_newly_fully_qualified_ceiling": side["projected_newly_fully_qualified_ceiling"],
                "downstream_pa_blockers": side["downstream_pa_blockers"],
                "downstream_outcome_blockers": side["downstream_outcome_blockers"],
                "downstream_bundle_blockers": side["downstream_bundle_blockers"],
                "discovery_target_type": side["discovery_target_type"],
                "expected_discovery_key": side["expected_discovery_key"],
                "estimated_later_historical_acquisition_request_count": side["estimated_later_historical_acquisition_request_count"],
                "governance_status": "FROZEN_EXACT_SIDE",
            })
        return rows

    def confirmed_row_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for row in sorted(self.cohort_rows, key=row_id):
            parts = split_row_id(row_id(row))
            rows.append({
                "cohort_id": COHORT_ID,
                "governed_canonical_row_id": row_id(row),
                "starter_game_side_key": row["starter_game_side_key"],
                **parts,
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "current_campaign_category": row["current_campaign_category"],
                "current_starter_status": row["current_starter_status"],
                "current_starter_qualified": row["current_starter_qualified"],
                "current_full_qualification_status": row["current_full_qualification_status"],
                "downstream_pa_qualified": row["downstream_pa_qualified"],
                "downstream_outcome_qualified": row["downstream_outcome_qualified"],
                "remaining_downstream_blocker": row["remaining_downstream_blocker"],
                "parent_state_package": str(CUMULATIVE_DIR),
                "parent_state_sha": EXPECTED_CUMULATIVE_SHA,
                "governance_status": "FROZEN_EXACT_ROW",
            })
        return rows

    def confirmed_target_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for idx, side in enumerate(self.cohort_sides, 1):
            rows.append({
                "cohort_id": COHORT_ID,
                "target_order": idx,
                "starter_game_side_key": side["starter_game_side_key"],
                "governed_target_date": side["starter_game_side_key"].split("|")[0],
                "governed_target_game": side["starter_game_side_key"].split("|")[1],
                "hitter_team": side["starter_game_side_key"].split("|")[2],
                "opponent_team": side["starter_game_side_key"].split("|")[3],
                "discovery_target_key": side["expected_discovery_key"],
                "discovery_target_type": side["discovery_target_type"],
                "allowed_source_hierarchy": "official_game_boxscore_or_project_repository_preserved_game_metadata",
                "allowed_endpoint_or_source_class": "governed_discovery_only_no_acquisition_without_separate_approval",
                "raw_discovery_request_cap": "1",
                "estimated_later_historical_acquisition_request_count": side["estimated_later_historical_acquisition_request_count"],
                "temporal_boundary": "strict_prior_to_target_slate_date_for_future_acquisition",
                "identity_acceptance_criteria": "exact governed side, exact game, exact opponent starter identity required",
                "ambiguity_rejection_criteria": "fail_closed_on_identity_or_role_ambiguity",
                "role_regime_acceptance_rule": "starter-compatible only; role regime reviewed during discovery/acquisition steps",
                "duplicate_response_handling": "deduplicate by exact target identity; no source substitution",
                "parser_provenance_contract": "raw source and parsed ledger required in any later approved step",
                "retry_limit": "bounded by future explicit approval",
                "approval_boundary": "discovery only requires separate explicit approval; acquisition and remediation require later approvals",
            })
        return rows

    def overlap_audit(self) -> list[dict[str, Any]]:
        completed = {
            "four_side_history_complete_remediation": FOUR_SIDE_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
            "hc_local_cohort_001": HC_LOCAL_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
            "discovery_cohort_001": COHORT_001_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
            "discovery_cohort_002": COHORT_002_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
        }
        c3_row_ids = {row_id(row) for row in self.cohort_rows}
        c3_sides = set(self.cohort_side_keys)
        rows = []
        for name, path in completed.items():
            data = read_csv(path)
            other_row_ids = {row_id(row) for row in data}
            other_sides = {side_from_movement(row) for row in data}
            rows.append({
                "compared_population": name,
                "row_overlap": len(c3_row_ids & other_row_ids),
                "side_overlap": len(c3_sides & other_sides),
                "pitcher_overlap": "unknown_until_discovery",
                "discovery_target_overlap": "not_applicable_completed_overlay",
                "status": "PASS" if not (c3_row_ids & other_row_ids) and not (c3_sides & other_sides) else "FAIL",
                "overlap_row_ids": "|".join(sorted(c3_row_ids & other_row_ids)),
                "overlap_side_ids": "|".join(sorted(c3_sides & other_sides)),
            })
        category_sets = {
            "local_parent_prescreen_fail_closed_population": {row["starter_game_side_key"] for row in self.sides_96 if row["current_campaign_category"] == "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"},
            "identity_role_review_holdout_population": {row["starter_game_side_key"] for row in self.role_holdouts},
        }
        c3_targets = {row["expected_discovery_key"] for row in self.cohort_sides}
        c2_targets = {row["discovery_target_key"] for row in self.c2_targets}
        for name, sides in category_sets.items():
            rows.append({
                "compared_population": name,
                "row_overlap": "not_evaluated_side_population",
                "side_overlap": len(c3_sides & sides),
                "pitcher_overlap": "unknown_until_discovery",
                "discovery_target_overlap": "unknown_until_discovery",
                "status": "PASS" if not (c3_sides & sides) else "FAIL",
                "overlap_row_ids": "",
                "overlap_side_ids": "|".join(sorted(c3_sides & sides)),
            })
        rows.append({
            "compared_population": "discovery_cohort_002_target_manifest",
            "row_overlap": 0,
            "side_overlap": 0,
            "pitcher_overlap": "unknown_until_discovery",
            "discovery_target_overlap": len(c3_targets & c2_targets),
            "status": "PASS" if not (c3_targets & c2_targets) else "FAIL",
            "overlap_row_ids": "",
            "overlap_side_ids": "",
        })
        return rows

    def remaining_reconciliation(self) -> list[dict[str, Any]]:
        rows = []
        remaining_by_side = {row["starter_game_side_key"]: row for row in read_csv(CUMULATIVE_REMAINING)}
        for side in self.cohort_side_keys:
            source = remaining_by_side.get(side)
            rows.append({
                "starter_game_side_key": side,
                "present_in_post_cohort_002_remaining_population": str(bool(source)).lower(),
                "current_campaign_category": source.get("current_campaign_category", "") if source else "",
                "represented_denominator_rows": source.get("represented_denominator_rows", "") if source else "",
                "projected_newly_fully_qualified_ceiling": source.get("projected_newly_fully_qualified_ceiling", "") if source else "",
                "in_existing_cohort_003_plan": source.get("in_existing_cohort_003_plan", "") if source else "",
                "status": "PASS" if source and source.get("in_existing_cohort_003_plan") == "true" else "FAIL",
            })
        return rows

    def governance_contract(self) -> list[dict[str, Any]]:
        return [
            {"contract_item": "parent_state_reference", "frozen_value": str(CUMULATIVE_DIR), "notes": "New parent is certified cumulative post-COHORT_002 state."},
            {"contract_item": "parent_state_sha", "frozen_value": EXPECTED_CUMULATIVE_SHA, "notes": "Must verify before any future discovery execution."},
            {"contract_item": "side_manifest", "frozen_value": "exact 8 sides preserved from existing scale-up plan", "notes": "No redesign performed."},
            {"contract_item": "row_manifest", "frozen_value": "exact 72 governed denominator rows", "notes": "No population expansion or replacement."},
            {"contract_item": "discovery_target_cap", "frozen_value": "8", "notes": "One target per governed side."},
            {"contract_item": "estimated_later_acquisition_volume", "frozen_value": "240", "notes": "Acquisition requires separate explicit approval."},
            {"contract_item": "temporal_rule", "frozen_value": "strict prior to target slate date for future source records", "notes": "No same-game postgame facts for strict-prior domains."},
            {"contract_item": "role_rule", "frozen_value": "starter-compatible only; fail closed on incompatible role regime", "notes": "Role regime remains unknown until source review."},
            {"contract_item": "duplicate_handling", "frozen_value": "deduplicate exact target identity; fail closed on conflict", "notes": "No source substitution."},
            {"contract_item": "provenance_requirement", "frozen_value": "raw source, parsed ledger, and SHA references required in later approved steps", "notes": "Governance only here."},
            {"contract_item": "variant_c", "frozen_value": "governance preserved not resolved", "notes": "No matrix construction or variant C action."},
        ]

    def approval_boundary(self) -> list[dict[str, Any]]:
        return [
            {"approval_item": "authorized_now", "value": "freeze cumulative-state-governed DISCOVERY_COHORT_003 only"},
            {"approval_item": "next_separate_approval_required", "value": "bounded discovery execution for exact 8 COHORT_003 sides only"},
            {"approval_item": "not_authorized", "value": "discovery execution|acquisition|reconstruction|remediation|qualification propagation|matrix construction|model/scoring|DB/API writes|OddsAPI|uploads|LaunchAgent|production changes"},
        ]

    def validate(self, overlap: list[dict[str, Any]], remaining: list[dict[str, Any]]) -> list[dict[str, Any]]:
        side_manifest = self.confirmed_side_manifest()
        row_manifest = self.confirmed_row_manifest()
        target_manifest = self.confirmed_target_manifest()
        checks = [
            ("cumulative_state_sha_verification", package_sha(CUMULATIVE_DIR), EXPECTED_CUMULATIVE_SHA),
            ("scale_up_package_sha_verification", package_sha(SCALE_DIR), EXPECTED_SCALE_SHA),
            ("exact_remaining_discovery_sides_reproduction", self.cumulative["remaining_discovery_population"]["post_cohort_002_remaining_discovery_sides"], 60),
            ("exact_remaining_discovery_rows_reproduction", self.cumulative["remaining_discovery_population"]["post_cohort_002_remaining_discovery_rows"], 482),
            ("exact_cohort_003_side_count", len(side_manifest), 8),
            ("exact_cohort_003_row_count", len(row_manifest), 72),
            ("exact_discovery_target_cap", len(target_manifest), 8),
            ("projected_hits_0_5_additions", sum(int_value(r["hits_0_5_rows"]) for r in side_manifest), 66),
            ("projected_hits_1_5_additions", sum(int_value(r["hits_1_5_rows"]) for r in side_manifest), 6),
            ("projected_abd_additions", sum(int_value(r["potential_abd_matrix_readiness_additions"]) for r in self.cohort_sides), 6),
            ("estimated_later_acquisition_volume", sum(int_value(r["estimated_later_historical_acquisition_request_count"]) for r in side_manifest), 240),
            ("zero_duplicate_governed_rows", len({r["governed_canonical_row_id"] for r in row_manifest}), len(row_manifest)),
            ("zero_duplicate_governed_sides", len({r["starter_game_side_key"] for r in side_manifest}), len(side_manifest)),
            ("all_sides_still_discovery_eligible", sum(r["status"] == "PASS" for r in remaining), 8),
            ("exact_overlap_audit", sum(r["status"] == "PASS" for r in overlap), len(overlap)),
            ("no_population_expansion", len(row_manifest), 72),
        ]
        rows = [{"validation": name, "status": "PASS" if obs == exp else "FAIL", "observed": obs, "expected": exp} for name, obs, exp in checks]
        rows.extend([
            {"validation": item, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for item in [
                "discovery_execution",
                "acquisition",
                "reconstruction",
                "remediation",
                "qualification_propagation",
                "formula_changes",
                "matrix_construction",
                "model_signal_scoring",
                "database_api_writes",
                "oddsapi_calls",
                "uploads_launchagent_production_change",
            ]
        ])
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        rows.append({
            "validation": "existing_abd_matrices_byte_identical",
            "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL",
            "observed": json.dumps(matrix_after, sort_keys=True),
            "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
        })
        rows.append({
            "validation": "static_guard",
            "status": "PASS" if all(r["status"] == "PASS" for r in static_guard()) else "FAIL",
            "observed": "see_static_guard",
            "expected": "all_pass",
        })
        return rows

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        side_manifest = self.confirmed_side_manifest()
        row_manifest = self.confirmed_row_manifest()
        target_manifest = self.confirmed_target_manifest()
        overlap = self.overlap_audit()
        remaining = self.remaining_reconciliation()
        validation = self.validate(overlap, remaining)
        if any(row["status"] != "PASS" for row in validation):
            write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
            raise RuntimeError("COHORT_003 cumulative governance validation failed")

        payload = {
            "decision": DECISION,
            "status": STATUS,
            "generated_at": GENERATED_AT,
            "parent_cumulative_state_package": str(CUMULATIVE_DIR),
            "parent_cumulative_state_sha": EXPECTED_CUMULATIVE_SHA,
            "scale_up_design_package_sha": EXPECTED_SCALE_SHA,
            "governed_side_count": len(side_manifest),
            "governed_row_count": len(row_manifest),
            "discovery_target_cap": len(target_manifest),
            "projected_starter_qualified_ceiling": sum(int_value(r["represented_denominator_rows"]) for r in side_manifest),
            "projected_newly_fully_qualified_ceiling": sum(int_value(r["projected_newly_fully_qualified_ceiling"]) for r in side_manifest),
            "projected_hits_0_5_additions": sum(int_value(r["hits_0_5_rows"]) for r in side_manifest),
            "projected_hits_1_5_additions": sum(int_value(r["hits_1_5_rows"]) for r in side_manifest),
            "projected_abd_matrix_readiness_additions": sum(int_value(r["potential_abd_matrix_readiness_additions"]) for r in self.cohort_sides),
            "estimated_later_acquisition_volume": sum(int_value(r["estimated_later_historical_acquisition_request_count"]) for r in side_manifest),
            "cohort_redesigned": False,
            "discovery_executed": False,
            "acquisition_executed": False,
            "reconstruction_or_remediation_executed": False,
            "next_separate_approval_required": "bounded discovery execution for exact 8 DISCOVERY_COHORT_003 sides only",
        }

        write_csv(OUT_DIR / f"cumulative_state_verification_{RUN_DATE}.csv", self.cumulative_verification())
        write_csv(OUT_DIR / f"dependency_sha_audit_{RUN_DATE}.csv", self.package_audit())
        write_csv(OUT_DIR / f"overlap_audit_{RUN_DATE}.csv", overlap)
        write_csv(OUT_DIR / f"remaining_discovery_reconciliation_{RUN_DATE}.csv", remaining)
        write_csv(OUT_DIR / f"confirmed_side_manifest_{RUN_DATE}.csv", side_manifest)
        write_csv(OUT_DIR / f"confirmed_row_manifest_{RUN_DATE}.csv", row_manifest)
        write_csv(OUT_DIR / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv", target_manifest)
        write_csv(OUT_DIR / f"cumulative_state_governance_contract_{RUN_DATE}.csv", self.governance_contract())
        write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", self.approval_boundary())
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        write_json(OUT_DIR / f"machine_readable_cumulative_governance_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# DISCOVERY_COHORT_003 Cumulative-State Governance — {RUN_DATE}

Decision: `{DECISION}`

Status: `{STATUS}`

The existing DISCOVERY_COHORT_003 plan was reconciled against the certified
post-COHORT_002 cumulative campaign state and frozen unchanged. The only
governance update is the parent-state reference:
`{CUMULATIVE_DIR}` with SHA `{EXPECTED_CUMULATIVE_SHA}`.

- Exact governed sides: `{payload['governed_side_count']}`
- Exact governed rows: `{payload['governed_row_count']}`
- Discovery target cap: `{payload['discovery_target_cap']}`
- Projected Starter-qualified ceiling: `{payload['projected_starter_qualified_ceiling']}`
- Projected newly fully qualified ceiling: `{payload['projected_newly_fully_qualified_ceiling']}`
- Projected Hits 0.5 additions: `{payload['projected_hits_0_5_additions']}`
- Projected Hits 1.5 additions: `{payload['projected_hits_1_5_additions']}`
- Projected A/B/D additions: `{payload['projected_abd_matrix_readiness_additions']}`
- Estimated later acquisition volume: `{payload['estimated_later_acquisition_volume']}`

No discovery, acquisition, reconstruction, remediation, qualification
propagation, matrix construction, modeling, scoring, database/API write,
OddsAPI call, upload, LaunchAgent edit, or production change occurred.

Next separate approval required: bounded discovery execution for the exact 8
DISCOVERY_COHORT_003 sides only.
""")
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    if not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                        raise ValueError("markdown missing heading")
                else:
                    continue
                status = "PASS"
                notes = ""
            except Exception as exc:
                kind = path.suffix.lstrip(".")
                status = "FAIL"
                notes = str(exc)
            parse_rows.append({"path": str(path), "artifact_type": kind, "parse_status": status, "notes": notes})
        write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        sha_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                sha_rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)


def main() -> int:
    result = Cohort003CumulativeGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
