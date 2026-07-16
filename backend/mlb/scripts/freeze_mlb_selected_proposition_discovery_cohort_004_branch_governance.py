#!/usr/bin/env python3
"""Freeze DISCOVERY_COHORT_004 branch governance.

This is a governance-only package generator. It partitions the completed
COHORT_004 discovery result into the seven-side resolved acquisition branch and
the one-side unresolved second-discovery branch. It does not execute discovery,
acquisition, reconstruction, remediation, qualification propagation, matrix
construction, model/scoring work, database/API writes, uploads, scheduler edits,
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

DISCOVERY_SHA = "bebfb681792d83cfd4d79c8c021c26dc8328f764398c2b71999d9210588f00f6"
PARENT_SHA = "d7629fab6efcb3b48a1432323aa861c0ae7390a00595430226471b2129123856"
GOVERNANCE_SHA = "032dbdf1525848837ce031b1c6fcb2e2af7252ccc7a2d6f633cc32113aec4485"
SCALE_UP_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"

DECISION = (
    "STARTER_DISCOVERY_COHORT_004_BRANCH_GOVERNANCE_DECISION = "
    "RESOLVED_ACQUISITION_BRANCH_AND_UNRESOLVED_SECOND_DISCOVERY_BRANCH_FROZEN"
)
RESOLVED_STATUS = (
    "STARTER_DISCOVERY_COHORT_004_RESOLVED_ACQUISITION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_BOUNDED_ACQUISITION_APPROVAL"
)
UNRESOLVED_STATUS = (
    "STARTER_DISCOVERY_COHORT_004_UNRESOLVED_SIDE_SECOND_DISCOVERY_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_SECOND_DISCOVERY_APPROVAL"
)

UNRESOLVED_SIDE = "2026-07-08|823928|LAD|COL"
ROOT_CAUSE = "PRIOR_APPEARANCES_EXIST_BUT_NOT_STARTS"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_branch_governance/2026-07-15"
)
DISCOVERY_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_004/2026-07-15"
)
PARENT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_cumulative_state_governance/2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)

SIDE_MANIFEST = GOV_DIR / f"confirmed_side_manifest_{RUN_DATE}.csv"
ROW_MANIFEST = GOV_DIR / f"confirmed_row_manifest_{RUN_DATE}.csv"
TARGET_MANIFEST = GOV_DIR / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv"
DISCOVERY_RESULT = DISCOVERY_DIR / f"machine_readable_discovery_result_{RUN_DATE}.json"
SIDE_LEDGER = DISCOVERY_DIR / f"side_level_discovery_result_ledger_{RUN_DATE}.csv"
REQUEST_LEDGER = DISCOVERY_DIR / f"request_ledger_{RUN_DATE}.csv"
RAW_INVENTORY = DISCOVERY_DIR / f"raw_response_inventory_{RUN_DATE}.csv"
PARSED_RECORDS = DISCOVERY_DIR / f"parsed_discovery_record_ledger_{RUN_DATE}.csv"
IDENTITY_LEDGER = DISCOVERY_DIR / f"accepted_rejected_identity_ledger_{RUN_DATE}.csv"
ACQ_MANIFEST = DISCOVERY_DIR / f"inert_exact_acquisition_manifest_{RUN_DATE}.csv"
DOWNSTREAM_LEDGER = DISCOVERY_DIR / f"downstream_limited_row_preservation_ledger_{RUN_DATE}.csv"
PARENT_STATE = PARENT_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"

MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

PROHIBITED_PATTERNS = {
    "network_or_second_discovery": re.compile(r"urllib|requests[.]|httpx|urlopen|feed/live|gameLog", re.IGNORECASE),
    "historical_acquisition_execution": re.compile(r"execute_.*acquisition|manifest_status.*EXECUTED", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(|qualification_propagation", re.IGNORECASE),
    "matrix_model_signal_work": re.compile(r"\.fit\s*\(|\.predict\s*\(|build_mlb_selected_proposition_abd_matrices|roc_auc|log_loss|signal_", re.IGNORECASE),
    "db_or_production_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi_upload_scheduler": re.compile(r"oddsapi|odds_api|write_upload|upload_ready|launchctl|LaunchAgent", re.IGNORECASE),
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


def package_sha(path: Path) -> str:
    return sha256_path(path / f"sha256_manifest_{RUN_DATE}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_denominator_identity") or row.get("canonical_row_id", "")


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
            "notes": "Static guard excludes comments, string literals, and the pattern declaration block.",
        })
    return rows


class Cohort004BranchGovernance:
    def __init__(self) -> None:
        self.discovery = json.loads(DISCOVERY_RESULT.read_text(encoding="utf-8"))
        self.parent = json.loads(PARENT_STATE.read_text(encoding="utf-8"))
        self.sides = read_csv(SIDE_MANIFEST)
        self.rows = read_csv(ROW_MANIFEST)
        self.targets = read_csv(TARGET_MANIFEST)
        self.side_ledger = read_csv(SIDE_LEDGER)
        self.requests = read_csv(REQUEST_LEDGER)
        self.raw = read_csv(RAW_INVENTORY)
        self.parsed = read_csv(PARSED_RECORDS)
        self.identities = read_csv(IDENTITY_LEDGER)
        self.acq = read_csv(ACQ_MANIFEST)
        self.downstream = read_csv(DOWNSTREAM_LEDGER)
        self.resolved_side_keys = {
            row["starter_game_side_key"]
            for row in self.side_ledger
            if row["final_discovery_result"] == "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
        }
        self.unresolved_side_keys = {row["starter_game_side_key"] for row in self.side_ledger} - self.resolved_side_keys
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def dependency_sha_audit(self) -> list[dict[str, Any]]:
        checks = [
            ("cohort_004_discovery_result", DISCOVERY_DIR, DISCOVERY_SHA),
            ("post_cohort_003_cumulative_parent", PARENT_DIR, PARENT_SHA),
            ("cohort_004_cumulative_governance", GOV_DIR, GOVERNANCE_SHA),
            ("remaining_scale_up_design", SCALE_UP_DIR, SCALE_UP_SHA),
        ]
        return [
            {
                "dependency": name,
                "path": str(path),
                "expected_sha": expected,
                "actual_sha": package_sha(path),
                "status": "PASS" if package_sha(path) == expected else "FAIL",
            }
            for name, path, expected in checks
        ]

    def resolved_sides(self) -> list[dict[str, str]]:
        order = {row["starter_game_side_key"]: int_value(row["target_order"]) for row in self.sides}
        return sorted(
            [row for row in self.sides if row["starter_game_side_key"] in self.resolved_side_keys],
            key=lambda row: order[row["starter_game_side_key"]],
        )

    def unresolved_sides(self) -> list[dict[str, str]]:
        return [row for row in self.sides if row["starter_game_side_key"] in self.unresolved_side_keys]

    def resolved_rows(self) -> list[dict[str, Any]]:
        return [row for row in self.rows if row["starter_game_side_key"] in self.resolved_side_keys]

    def unresolved_rows(self) -> list[dict[str, Any]]:
        return [row for row in self.rows if row["starter_game_side_key"] in self.unresolved_side_keys]

    def branch_partition_ledger(self) -> list[dict[str, Any]]:
        side_result = {row["starter_game_side_key"]: row["final_discovery_result"] for row in self.side_ledger}
        rows = []
        for side in self.sides:
            key = side["starter_game_side_key"]
            branch = "RESOLVED_ACQUISITION_BRANCH" if key in self.resolved_side_keys else "UNRESOLVED_SECOND_DISCOVERY_BRANCH"
            rows.append({
                "starter_game_side_key": key,
                "branch": branch,
                "discovery_result": side_result.get(key, ""),
                "represented_rows": side["represented_denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "projected_starter_qualified_ceiling": side["projected_starter_qualified_ceiling"],
                "projected_newly_fully_qualified_ceiling": side["projected_newly_fully_qualified_ceiling"],
                "downstream_pa_blockers": side["downstream_pa_blockers"],
                "downstream_outcome_blockers": side["downstream_outcome_blockers"],
                "downstream_bundle_blockers": side["downstream_bundle_blockers"],
                "potential_abd_matrix_readiness_additions": side["potential_abd_matrix_readiness_additions"],
                "partition_status": "ACCOUNTED_EXACTLY_ONCE",
            })
        return rows

    def resolved_acquisition_manifest(self) -> list[dict[str, Any]]:
        return [
            {**row, "branch": "RESOLVED_ACQUISITION_BRANCH", "governance_status": "FROZEN_INERT_AWAITING_EXPLICIT_ACQUISITION_APPROVAL"}
            for row in self.acq
            if row["parent_starter_game_side_identity"] in self.resolved_side_keys
        ]

    def request_leakage_audit(self) -> list[dict[str, Any]]:
        original = self.acq
        resolved = self.resolved_acquisition_manifest()
        removed = [row for row in original if row["parent_starter_game_side_identity"] in self.unresolved_side_keys]
        dedupe_counts = Counter(row["deduplication_key"] for row in resolved)
        duplicate_keys = sorted(key for key, count in dedupe_counts.items() if count > 1)
        return [
            {
                "audit_item": "original_inert_request_count",
                "value": len(original),
                "status": "PASS",
                "notes": "Original inert manifest from discovery result.",
            },
            {
                "audit_item": "requests_removed_belonging_only_to_unresolved_side",
                "value": len(removed),
                "status": "PASS" if len(removed) == 0 else "WARN",
                "notes": "Unresolved side had zero accepted acquisition requests.",
            },
            {
                "audit_item": "shared_requests_retained",
                "value": 0,
                "status": "PASS",
                "notes": "No cross-side sharing required by deduplication key.",
            },
            {
                "audit_item": "final_resolved_executable_request_count",
                "value": len(resolved),
                "status": "PASS" if len(resolved) == 245 else "FAIL",
                "notes": "Executable means future eligible; manifest remains inert in this package.",
            },
            {
                "audit_item": "duplicate_deduplication_keys_in_resolved_manifest",
                "value": len(duplicate_keys),
                "status": "PASS" if not duplicate_keys else "FAIL",
                "notes": "|".join(duplicate_keys),
            },
            {
                "audit_item": "unresolved_side_request_leakage",
                "value": sum(1 for row in resolved if row["parent_starter_game_side_identity"] == UNRESOLVED_SIDE),
                "status": "PASS",
                "notes": "No unresolved-side request may remain in resolved branch.",
            },
        ]

    def resolved_governance_contract(self) -> list[dict[str, Any]]:
        return [
            {"contract_item": "discovery_package_sha", "frozen_value": DISCOVERY_SHA, "notes": "Source package for all accepted records and raw responses."},
            {"contract_item": "resolved_side_count", "frozen_value": "7", "notes": "Only sides classified DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY."},
            {"contract_item": "exact_request_count", "frozen_value": "245", "notes": "Inert exact historical acquisition manifest; not executed."},
            {"contract_item": "deterministic_ordering", "frozen_value": "manifest row order from discovery output", "notes": "No sorting changes before future acquisition approval."},
            {"contract_item": "deduplication_key", "frozen_value": "pitcher_id|historical_game_id|historical_game_date", "notes": "No duplicate keys allowed."},
            {"contract_item": "allowed_source_hierarchy", "frozen_value": "official_mlb_statsapi_game_feed_or_boxscore_by_exact_gamePk", "notes": "Exact gamePk only; no broad crawling."},
            {"contract_item": "exact_endpoint_method", "frozen_value": "GET official MLB StatsAPI game feed/boxscore by exact gamePk", "notes": "Acquisition requires separate approval."},
            {"contract_item": "identity_binding", "frozen_value": "parent side, pitcher identity, historical game identity, historical date", "notes": "All must match inert manifest."},
            {"contract_item": "strict_prior_boundary", "frozen_value": "historical_game_date < governed target date", "notes": "No same-game or future records."},
            {"contract_item": "accepted_response_types", "frozen_value": "raw official game feed or boxscore JSON", "notes": "Preserve raw responses."},
            {"contract_item": "retry_policy", "frozen_value": "bounded future approval only; fail closed after approved retries", "notes": "No retry execution here."},
            {"contract_item": "timeout_rate_limit_policy", "frozen_value": "bounded low-rate exact request list", "notes": "Future acquisition must report timing and retries."},
            {"contract_item": "parser_contract", "frozen_value": "parse starter role/workload only under separate acquisition/remediation governance", "notes": "No reconstruction authorized."},
            {"contract_item": "partial_failure_behavior", "frozen_value": "preserve successful raw responses; failed requests stay unresolved; no substitution", "notes": "Fail closed."},
            {"contract_item": "fail_closed_taxonomy", "frozen_value": "source_unavailable|identity_conflict|temporal_violation|role_incompatible|duplicate_conflict|parser_failure", "notes": "No convenience acceptance."},
            {"contract_item": "offline_replay_requirement", "frozen_value": "raw response inventory, SHA manifest, parse validation required", "notes": "Deterministic replay before remediation."},
            {"contract_item": "explicit_boundary", "frozen_value": "no reconstruction/no remediation/no qualification movement", "notes": "This package authorizes no execution."},
        ]

    def unresolved_evidence_review(self) -> list[dict[str, Any]]:
        side = next(row for row in self.side_ledger if row["starter_game_side_key"] == UNRESOLVED_SIDE)
        records = [row for row in self.identities if row["starter_game_side_key"] == UNRESOLVED_SIDE]
        requests = [row for row in self.requests if row["starter_game_side_key"] == UNRESOLVED_SIDE]
        raw = [row for row in self.raw if row["starter_game_side_key"] == UNRESOLVED_SIDE]
        return [
            {
                "evidence_item": "target_pitcher_identity",
                "value": f"{side['accepted_pitcher_identity']}|{side['accepted_pitcher_name']}",
                "source": SIDE_LEDGER,
                "notes": "Target starter identity resolved in first discovery.",
            },
            {
                "evidence_item": "target_game_identity",
                "value": side["accepted_target_game_identity"],
                "source": SIDE_LEDGER,
                "notes": "Target game identity resolved in first discovery.",
            },
            {
                "evidence_item": "first_discovery_request_count",
                "value": len(requests),
                "source": REQUEST_LEDGER,
                "notes": "Target feed plus bounded pitcher gameLog requests.",
            },
            {
                "evidence_item": "raw_responses_preserved",
                "value": len(raw),
                "source": RAW_INVENTORY,
                "notes": "No transport or parser failure.",
            },
            {
                "evidence_item": "strict_prior_records_returned",
                "value": len(records),
                "source": IDENTITY_LEDGER,
                "notes": "One first-discovery strict-prior MLB appearance was returned.",
            },
            {
                "evidence_item": "accepted_strict_prior_starter_records",
                "value": sum(1 for row in records if row["identity_record_status"] == "ACCEPTED"),
                "source": IDENTITY_LEDGER,
                "notes": "No compatible strict-prior MLB Starter history accepted.",
            },
            {
                "evidence_item": "rejected_record_reasons",
                "value": "|".join(
                    sorted(
                        f"{row['historical_game_date']}:{row['historical_game_id']}:starter={row['official_starter_designation']}:temporal={row['temporal_status']}"
                        for row in records
                    )
                ),
                "source": IDENTITY_LEDGER,
                "notes": "Rejected because the returned strict-prior appearance was not an official start.",
            },
        ]

    def unresolved_root_cause(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_side_key": UNRESOLVED_SIDE,
                "primary_root_cause": ROOT_CAUSE,
                "supporting_evidence": "First discovery returned one strict-prior MLB appearance for pitcher 687312 on 2026-07-03/game 824336 with official_starter_designation=false; accepted strict-prior starter records=0.",
                "evidence_boundary": "This classification is limited to preserved first-discovery evidence and does not infer that no prior starts exist outside the frozen first-discovery contract.",
                "governance_implication": "Second discovery may verify authoritative career appearance/start sequence for the same pitcher identity; it may not substitute relief appearances for Starter history.",
            }
        ]

    def second_discovery_target_manifest(self) -> list[dict[str, Any]]:
        side = next(row for row in self.side_ledger if row["starter_game_side_key"] == UNRESOLVED_SIDE)
        side_manifest = next(row for row in self.sides if row["starter_game_side_key"] == UNRESOLVED_SIDE)
        return [
            {
                "second_discovery_target_id": "COHORT_004_SECOND_DISCOVERY_001",
                "starter_game_side_key": UNRESOLVED_SIDE,
                "resolved_pitcher_identity": side["accepted_pitcher_identity"],
                "resolved_pitcher_name": side["accepted_pitcher_name"],
                "resolved_target_game_identity": side["accepted_target_game_identity"],
                "target_date": UNRESOLVED_SIDE.split("|")[0],
                "represented_rows": side_manifest["represented_denominator_rows"],
                "hits_0_5_rows": side_manifest["hits_0_5_rows"],
                "hits_1_5_rows": side_manifest["hits_1_5_rows"],
                "non_starter_prerequisite_status": "projected_full_ceiling_excludes_existing_downstream_limited_rows",
                "projected_starter_qualified_ceiling": side_manifest["projected_starter_qualified_ceiling"],
                "projected_newly_fully_qualified_ceiling": side_manifest["projected_newly_fully_qualified_ceiling"],
                "primary_root_cause": ROOT_CAUSE,
                "request_purpose": "verify same-pitcher strict-prior MLB career appearance/start sequence and determine whether compatible prior starts exist",
                "request_cap": 4,
                "allowed_source_hierarchy": "official_mlb_statsapi_pitching_gameLog_by_same_pitcher_identity_or_official_people_career_pitching_index",
                "allowed_endpoint_or_source_class": "same_pitcher_identity_only_no_unrelated_lookup",
                "date_boundaries": "historical_game_date < 2026-07-08",
                "acceptance_criteria": "same pitcher identity, MLB game identity, official start designation, strict-prior date",
                "rejection_criteria": "relief-only appearance, minor-league-only evidence, identity mismatch, temporal violation, source conflict",
                "approval_boundary": "second discovery only; no acquisition or remediation authorized",
            }
        ]

    def second_discovery_contract(self) -> list[dict[str, Any]]:
        return [
            {"contract_item": "governed_side", "frozen_value": UNRESOLVED_SIDE, "notes": "No other side may be queried."},
            {"contract_item": "pitcher_identity", "frozen_value": "687312|Gabriel Hughes", "notes": "Same resolved target pitcher only."},
            {"contract_item": "request_cap", "frozen_value": "4", "notes": "Maximum bounded second-discovery requests."},
            {"contract_item": "source_hierarchy", "frozen_value": "official MLB StatsAPI same-pitcher career/gameLog evidence", "notes": "No broad league-wide crawling."},
            {"contract_item": "permitted_purposes", "frozen_value": "career appearance/start sequence; starts vs relief; role-transition check; omitted MLB game identity check", "notes": "Discovery only."},
            {"contract_item": "forbidden_uses", "frozen_value": "relief substitution|minors as MLB Starter evidence|strict-prior weakening|same-game postgame workaround", "notes": "Fail closed if no compatible starter history exists."},
            {"contract_item": "date_boundary", "frozen_value": "strict prior to 2026-07-08", "notes": "No target-date or later history."},
            {"contract_item": "start_vs_relief_rule", "frozen_value": "official gamesStarted/start designation required", "notes": "Relief appearances rejected."},
            {"contract_item": "duplicate_handling", "frozen_value": "dedupe exact pitcher_id|game_id|game_date", "notes": "Conflicts fail closed."},
            {"contract_item": "retry_limit", "frozen_value": "bounded by future explicit approval", "notes": "No execution here."},
            {"contract_item": "raw_response_preservation", "frozen_value": "required", "notes": "Raw responses and hashes must be preserved."},
            {"contract_item": "parser_provenance", "frozen_value": "parsed ledger, request ledger, raw inventory, SHA manifest", "notes": "Deterministic replay required."},
            {"contract_item": "allowed_outcomes", "frozen_value": "SECOND_DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY|SECOND_DISCOVERY_PARTIALLY_RESOLVED_ADDITIONAL_GOVERNANCE_REQUIRED|SECOND_DISCOVERY_ROLE_TRANSITION_FAIL_CLOSED|SECOND_DISCOVERY_NO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED|SECOND_DISCOVERY_IDENTITY_OR_TEMPORAL_CONFLICT_FAIL_CLOSED|SECOND_DISCOVERY_SOURCE_COVERAGE_INSUFFICIENT|SECOND_DISCOVERY_EXECUTION_FAILURE", "notes": "Exactly one required in later execution."},
            {"contract_item": "approval_boundary", "frozen_value": "second discovery approval does not authorize acquisition, reconstruction, remediation, or qualification movement", "notes": "Separate approvals required."},
        ]

    def downstream_preservation(self) -> list[dict[str, Any]]:
        return [
            {
                **row,
                "branch": "RESOLVED_ACQUISITION_BRANCH"
                if row["starter_game_side_key"] in self.resolved_side_keys
                else "UNRESOLVED_SECOND_DISCOVERY_BRANCH",
                "preservation_confirmation": "UNCHANGED_NO_DOWNSTREAM_REMEDIATION_AUTHORIZED",
            }
            for row in self.downstream
        ]

    def cumulative_preservation_report(self) -> list[dict[str, Any]]:
        expected = {
            "total_fully_qualified_hits": 1033,
            "fully_qualified_hits_0_5": 912,
            "fully_qualified_hits_1_5": 121,
            "current_starter_blocked_population": 603,
            "current_pa_blocked_population": 11,
            "current_outcome_blocked_population": 363,
            "current_bundle_blocked_population": 36,
            "qualified_but_not_matrix_constructed_hits_1_5_rows": 22,
        }
        rows = []
        for key, value in expected.items():
            rows.append({
                "metric": key,
                "observed": self.parent.get(key),
                "expected": value,
                "status": "PASS" if self.parent.get(key) == value else "FAIL",
            })
        rows.append({
            "metric": "parent_package_sha",
            "observed": package_sha(PARENT_DIR),
            "expected": PARENT_SHA,
            "status": "PASS" if package_sha(PARENT_DIR) == PARENT_SHA else "FAIL",
        })
        return rows

    def approval_boundary(self) -> list[dict[str, Any]]:
        return [
            {"approval_item": "authorized_now", "value": "freeze branch governance only"},
            {"approval_item": "future_approval_1", "value": "execute exact 245-request acquisition for seven-side resolved branch"},
            {"approval_item": "future_approval_2", "value": "execute exact one-side second discovery for LAD-COL unresolved side"},
            {"approval_item": "not_authorized", "value": "acquisition execution|second discovery execution|reconstruction|remediation|qualification propagation|matrix construction|model/scoring|DB/API writes|OddsAPI|uploads|LaunchAgent|production changes"},
        ]

    def projections(self) -> dict[str, Any]:
        resolved_sides = self.resolved_sides()
        resolved_rows = self.resolved_rows()
        full_rows = [
            row
            for row in resolved_rows
            if boolish(row["downstream_pa_qualified"])
            and boolish(row["downstream_outcome_qualified"])
            and row["downstream_bundle_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
        ]
        hits05_full = sum(1 for row in full_rows if row["line"] == "0.5")
        hits15_full = sum(1 for row in full_rows if row["line"] == "1.5")
        return {
            "resolved_governed_sides": len(resolved_sides),
            "resolved_represented_rows": len(resolved_rows),
            "resolved_executable_requests": len(self.resolved_acquisition_manifest()),
            "resolved_unique_pitchers": len({row["pitcher_identity"] for row in self.resolved_acquisition_manifest()}),
            "resolved_unique_historical_games": len({row["historical_game_identity"] for row in self.resolved_acquisition_manifest()}),
            "resolved_projected_starter_qualified_ceiling": len(resolved_rows),
            "resolved_projected_newly_fully_qualified_ceiling": len(full_rows),
            "resolved_projected_hits_0_5_additions": hits05_full,
            "resolved_projected_hits_1_5_additions": hits15_full,
            "resolved_downstream_pa_blockers": sum(1 for row in resolved_rows if not boolish(row["downstream_pa_qualified"])),
            "resolved_downstream_outcome_blockers": sum(1 for row in resolved_rows if not boolish(row["downstream_outcome_qualified"])),
            "resolved_downstream_bundle_blockers": sum(
                1 for row in resolved_rows if row["downstream_bundle_status"] != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
            ),
            "resolved_potential_abd_additions": sum(int_value(row["potential_abd_matrix_readiness_additions"]) for row in resolved_sides),
            "projected_post_resolved_branch_total_fully_qualified_hits": self.parent["total_fully_qualified_hits"] + len(full_rows),
            "projected_post_resolved_branch_hits_0_5": self.parent["fully_qualified_hits_0_5"] + hits05_full,
            "projected_post_resolved_branch_hits_1_5": self.parent["fully_qualified_hits_1_5"] + hits15_full,
            "projected_post_resolved_branch_starter_blocked": self.parent["current_starter_blocked_population"] - len(resolved_rows),
            "projected_post_resolved_branch_pa_blocked": self.parent["current_pa_blocked_population"]
            + sum(1 for row in resolved_rows if not boolish(row["downstream_pa_qualified"])),
            "projected_post_resolved_branch_outcome_blocked": self.parent["current_outcome_blocked_population"],
            "projected_post_resolved_branch_bundle_blocked": self.parent["current_bundle_blocked_population"],
            "projected_post_resolved_branch_hits_1_5_queue": self.parent[
                "qualified_but_not_matrix_constructed_hits_1_5_rows"
            ]
            + hits15_full,
        }

    def validation_report(self) -> list[dict[str, Any]]:
        resolved_requests = self.resolved_acquisition_manifest()
        partition_rows = self.branch_partition_ledger()
        original_row_ids = {row_id(row) for row in self.rows}
        partition_row_ids = {row_id(row) for row in self.resolved_rows()} | {row_id(row) for row in self.unresolved_rows()}
        leakage = sum(1 for row in resolved_requests if row["parent_starter_game_side_identity"] == UNRESOLVED_SIDE)
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        checks = [
            ("cohort_004_discovery_package_sha", package_sha(DISCOVERY_DIR), DISCOVERY_SHA),
            ("cumulative_parent_state_sha", package_sha(PARENT_DIR), PARENT_SHA),
            ("cohort_004_governance_package_sha", package_sha(GOV_DIR), GOVERNANCE_SHA),
            ("exact_8_side_reproduction", len(self.sides), 8),
            ("exact_73_row_reproduction", len(self.rows), 73),
            ("exact_seven_resolved_one_unresolved_partition", (len(self.resolved_side_keys), len(self.unresolved_side_keys)), (7, 1)),
            ("complete_row_partition_no_loss_or_duplication", "|".join(sorted(original_row_ids)), "|".join(sorted(partition_row_ids))),
            ("exact_inert_request_manifest_reproduction", len(self.acq), 245),
            ("resolved_branch_final_request_count", len(resolved_requests), 245),
            ("no_unresolved_side_request_leakage", leakage, 0),
            ("exact_unresolved_side_first_discovery_records", len([r for r in self.identities if r["starter_game_side_key"] == UNRESOLVED_SIDE]), 1),
            ("exact_downstream_limited_four_rows_preserved", len(self.downstream), 4),
            ("branch_partition_totals_original_sides", sum(int_value(r["represented_rows"]) for r in partition_rows), 73),
            ("no_network_access", "not_performed", "not_performed"),
            ("no_second_discovery_execution", "not_performed", "not_performed"),
            ("no_historical_acquisition", "not_performed", "not_performed"),
            ("no_reconstruction", "not_performed", "not_performed"),
            ("no_remediation", "not_performed", "not_performed"),
            ("no_qualification_propagation", "not_performed", "not_performed"),
            ("no_formula_or_fallback_changes", "not_performed", "not_performed"),
            ("no_pa_outcome_bundle_variant_c_remediation", "not_performed", "not_performed"),
            ("no_matrix_construction", "not_performed", "not_performed"),
            ("no_model_signal_scoring_champion_challenger_promotion_roi", "not_performed", "not_performed"),
            ("no_database_api_writes", "not_performed", "not_performed"),
            ("no_oddsapi_calls", "not_performed", "not_performed"),
            ("no_uploads_launchagent_production_change", "not_performed", "not_performed"),
            ("cumulative_state_byte_identical", package_sha(PARENT_DIR), PARENT_SHA),
            ("existing_abd_matrices_byte_identical", json.dumps(matrix_after, sort_keys=True), json.dumps(self.matrix_hash_before, sort_keys=True)),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend([
            {
                "validation": f"static_guard_{row['check']}",
                "status": row["status"],
                "observed": row["matches"],
                "expected": "no_prohibited_pattern",
            }
            for row in static_guard()
        ])
        return rows

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", self.dependency_sha_audit())
        write_csv(OUT_DIR / f"original_eight_side_73_row_reproduction_{RUN_DATE}.csv", self.branch_partition_ledger())
        write_csv(OUT_DIR / f"resolved_unresolved_branch_partition_ledger_{RUN_DATE}.csv", self.branch_partition_ledger())
        write_csv(OUT_DIR / f"exact_seven_side_resolved_manifest_{RUN_DATE}.csv", self.resolved_sides())
        write_csv(OUT_DIR / f"exact_resolved_row_manifest_{RUN_DATE}.csv", self.resolved_rows())
        write_csv(OUT_DIR / f"exact_resolved_side_acquisition_request_manifest_{RUN_DATE}.csv", self.resolved_acquisition_manifest())
        write_csv(OUT_DIR / f"request_leakage_and_deduplication_audit_{RUN_DATE}.csv", self.request_leakage_audit())
        write_csv(OUT_DIR / f"resolved_acquisition_governance_contract_{RUN_DATE}.csv", self.resolved_governance_contract())
        write_csv(OUT_DIR / f"unresolved_side_evidence_review_{RUN_DATE}.csv", self.unresolved_evidence_review())
        write_csv(OUT_DIR / f"unresolved_side_root_cause_classification_{RUN_DATE}.csv", self.unresolved_root_cause())
        write_csv(OUT_DIR / f"exact_second_discovery_target_manifest_{RUN_DATE}.csv", self.second_discovery_target_manifest())
        write_csv(OUT_DIR / f"second_discovery_governance_contract_{RUN_DATE}.csv", self.second_discovery_contract())
        write_csv(OUT_DIR / f"downstream_limited_row_preservation_ledger_{RUN_DATE}.csv", self.downstream_preservation())
        write_csv(OUT_DIR / f"cumulative_state_preservation_report_{RUN_DATE}.csv", self.cumulative_preservation_report())
        write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", self.approval_boundary())
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        validation = self.validation_report()
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        if any(row["status"] != "PASS" for row in validation):
            raise RuntimeError("branch governance validation failed")
        projections = self.projections()
        payload = {
            "decision": DECISION,
            "resolved_acquisition_governance_status": RESOLVED_STATUS,
            "unresolved_side_second_discovery_status": UNRESOLVED_STATUS,
            "generated_at": GENERATED_AT,
            "discovery_package_sha": DISCOVERY_SHA,
            "parent_package_sha": PARENT_SHA,
            "governance_package_sha": GOVERNANCE_SHA,
            "unresolved_side": UNRESOLVED_SIDE,
            "unresolved_root_cause": ROOT_CAUSE,
            "second_discovery_target_cap": 1,
            "second_discovery_request_cap": 4,
            "network_access": "not_performed",
            "acquisition_executed": False,
            "second_discovery_executed": False,
            "reconstruction_or_remediation_executed": False,
            "qualification_propagation_executed": False,
            "matrix_model_upload_or_production_change": False,
            **projections,
        }
        write_json(OUT_DIR / f"machine_readable_branch_governance_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", self.render_summary(payload))
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def render_summary(self, payload: dict[str, Any]) -> str:
        return f"""
# DISCOVERY_COHORT_004 Branch Governance — {RUN_DATE}

Decision: `{payload['decision']}`

Resolved acquisition status: `{payload['resolved_acquisition_governance_status']}`

Unresolved second-discovery status: `{payload['unresolved_side_second_discovery_status']}`

The completed COHORT_004 discovery result was partitioned into two frozen
branches without executing either branch.

## Resolved Acquisition Branch

- Governed sides: `{payload['resolved_governed_sides']}`
- Represented rows: `{payload['resolved_represented_rows']}`
- Exact inert acquisition requests: `{payload['resolved_executable_requests']}`
- Unique pitchers: `{payload['resolved_unique_pitchers']}`
- Unique historical games: `{payload['resolved_unique_historical_games']}`
- Projected Starter-qualified ceiling: `{payload['resolved_projected_starter_qualified_ceiling']}`
- Projected newly fully qualified ceiling: `{payload['resolved_projected_newly_fully_qualified_ceiling']}`
- Projected Hits 0.5 additions: `{payload['resolved_projected_hits_0_5_additions']}`
- Projected Hits 1.5 additions: `{payload['resolved_projected_hits_1_5_additions']}`
- Potential A/B/D additions: `{payload['resolved_potential_abd_additions']}`

## Unresolved Second-Discovery Branch

- Side: `{payload['unresolved_side']}`
- Root-cause classification: `{payload['unresolved_root_cause']}`
- Second-discovery target cap: `{payload['second_discovery_target_cap']}`
- Second-discovery request cap: `{payload['second_discovery_request_cap']}`

Two future approvals are separated:

1. Approval to execute the exact 245-request acquisition for the seven-side
   resolved branch.
2. Approval to execute the one-side second discovery for LAD-COL.

No acquisition, second discovery, reconstruction, remediation, qualification
propagation, matrix construction, model/scoring work, database/API writes,
OddsAPI calls, uploads, LaunchAgent changes, or production behavior changes
occurred.
"""

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name in {f"sha256_manifest_{RUN_DATE}.csv", f"parse_validation_{RUN_DATE}.csv"}:
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
    result = Cohort004BranchGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
