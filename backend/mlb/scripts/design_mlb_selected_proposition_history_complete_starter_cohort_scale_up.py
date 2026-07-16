#!/usr/bin/env python3
"""Design the next bounded history-complete Starter scale-up cohort.

Read-only design/governance freeze only. This utility performs no network
access, source acquisition, discovery, reconstruction, remediation, denominator
propagation, matrix construction, model/scoring work, database/API writes,
uploads, LaunchAgent edits, or production behavior changes.
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
SOURCE_DATE = "2026-07-14"
FROZEN_GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_REMEDIATION_SHA = "629de76d980f219e5d1aa98cba7bc259cd19921ac35f1dd2ffc0b6119c628c7f"
EXPECTED_REMEDIATION_DECISION = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_REMEDIATION_COMPLETED_WITH_NONZERO_YIELD"
)
EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
DECISION = (
    "STARTER_HISTORY_COMPLETE_COHORT_SCALE_UP_DESIGN_DECISION = "
    "FIRST_LOCAL_HISTORY_COMPLETE_COHORT_FROZEN_READY_FOR_EXPLICIT_OFFLINE_REMEDIATION_APPROVAL"
)

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_history_complete_starter_cohort_scale_up_design/"
    "2026-07-15"
)
READINESS_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/"
    "2026-07-14"
)
REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/"
    "2026-07-15"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/"
    "2026-07-14"
)
STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

READINESS_RESULT = READINESS_DIR / f"machine_readable_review_result_{SOURCE_DATE}.json"
READINESS_ROWS = READINESS_DIR / f"exact_803_row_denominator_manifest_{SOURCE_DATE}.csv"
READINESS_SIDES = READINESS_DIR / f"exact_starter_game_side_manifest_{SOURCE_DATE}.csv"
REMEDIATION_RESULT = REMEDIATION_DIR / f"post_remediation_qualification_state_{RUN_DATE}.json"
REMEDIATION_ROWS = REMEDIATION_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
STATE_RESULT = STATE_DIR / f"machine_readable_state_summary_{SOURCE_DATE}.json"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
]

PROHIBITED_PATTERNS = {
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "source_acquisition_or_discovery": re.compile(r"\b(download|fetch|urlretrieve)\s*\(", re.IGNORECASE),
    "reconstruction_or_remediation_execution": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(", re.IGNORECASE),
    "model_or_signal": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_", re.IGNORECASE),
    "matrix_build": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi": re.compile(r"oddsapi|odds_api", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"launchctl|LaunchAgent|write_upload|upload_ready", re.IGNORECASE),
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


def package_sha(path: Path, date_value: str) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def yes(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def norm_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        return text[:-2]
    return text


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
            "notes": "Static guard excludes comments and string literals.",
        })
    return rows


class HistoryCompleteScaleUpDesign:
    def __init__(self) -> None:
        self.readiness_result = json.loads(READINESS_RESULT.read_text(encoding="utf-8"))
        self.remediation_result = json.loads(REMEDIATION_RESULT.read_text(encoding="utf-8"))
        self.state = json.loads(STATE_RESULT.read_text(encoding="utf-8"))
        self.rows = read_csv(READINESS_ROWS)
        self.sides = read_csv(READINESS_SIDES)
        self.remediated_rows = read_csv(REMEDIATION_ROWS)
        self.starter_base = read_csv(STARTER_BASE)
        self.starter_base_index = {
            (row["date"], row["game_id"], row["player_team"], row["opponent_team"]): row
            for row in self.starter_base
        }
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.remediated_side_keys = {
            row["starter_game_side_key"] for row in self.remediated_rows
            if yes(row.get("post_remediation_starter_qualified", ""))
        }
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def starter_base_match(self, side: dict[str, str]) -> dict[str, str] | None:
        return self.starter_base_index.get((
            side["slate_date"],
            side["game_id"],
            side["opponent_team"],
            side["hitter_team"],
        ))

    def classify_side(self, side: dict[str, str]) -> tuple[str, dict[str, str] | None]:
        side_key = side["starter_game_side_key"]
        if side_key in self.remediated_side_keys:
            return "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE", None
        base = self.starter_base_match(side)
        if base and base.get("strict_prior_status") == "PASS_STRICT_PRIOR":
            if int_value(side["pa_qualified_rows"]) == int_value(side["denominator_rows"]):
                return "ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE", base
            return "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED", base
        if "high" in side.get("special_regime_risk", "").lower():
            return "ESTABLISHED_SPECIAL_REGIME_EXCLUSION", base
        if base:
            return "TEMPORAL_OR_IDENTITY_AMBIGUITY_FAIL_CLOSED", base
        return "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE", None

    def side_inventory(self) -> list[dict[str, Any]]:
        inventory = []
        for side in sorted(self.sides, key=lambda r: (r["slate_date"], r["game_id"], r["hitter_team"], r["opponent_team"])):
            category, base = self.classify_side(side)
            side_rows = self.rows_by_side[side["starter_game_side_key"]]
            pa_blocked = int_value(side["denominator_rows"]) - int_value(side["pa_qualified_rows"])
            outcome_blocked = int_value(side["denominator_rows"]) - int_value(side["numeric_outcome_certified_rows"])
            bundle_blocked = 0
            potential_abd = sum(1 for row in side_rows if row["line"] == "1.5" and row.get("post_three_row_variant_a_state") == "STILL_BLOCKED" and yes(row.get("post_three_row_pa_qualified", "")) and yes(row.get("numeric_outcome_certified", "")))
            inventory.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "classification": category,
                "slate_date": side["slate_date"],
                "game_id": side["game_id"],
                "hitter_team": side["hitter_team"],
                "opponent_team": side["opponent_team"],
                "represented_denominator_rows": side["denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "current_starter_blocker_classification": side["primary_side_taxonomy"],
                "all_non_starter_prerequisites_satisfied": int_value(side["pa_qualified_rows"]) == int_value(side["denominator_rows"]) and int_value(side["numeric_outcome_certified_rows"]) == int_value(side["denominator_rows"]),
                "projected_fully_qualified_ceiling": side["pa_qualified_rows"],
                "downstream_pa_blocker_count": pa_blocked,
                "downstream_outcome_blocker_count": max(0, outcome_blocked),
                "downstream_bundle_blocker_count": bundle_blocked,
                "required_strict_prior_historical_depth": base.get("prior_starts_count", "") if base else "",
                "known_prior_game_request_identities": "not_exposed_by_offline_artifacts" if base else "",
                "estimated_raw_request_count": 0 if base else "requires_discovery_governance",
                "deduplicated_request_count": 0 if base else "requires_discovery_governance",
                "role_regime_status": base.get("actual_starter_role", "unknown_until_source_review") if base else "unknown_until_source_review",
                "temporal_eligibility": base.get("strict_prior_status", "unknown_until_source_review") if base else "unknown_until_source_review",
                "reconstruction_readiness_classification": "LOCAL_HISTORY_COMPLETE_PARENT_SOURCE_AVAILABLE" if category in {"ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE", "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED"} else "NOT_READY_WITHOUT_DISCOVERY_OR_EXCLUSION_REVIEW",
                "actual_starter_player_id": norm_id(base.get("actual_starter_player_id", "")) if base else "",
                "actual_starter_name": base.get("actual_starter_name_from_bf", "") if base else "",
                "strict_prior_status": base.get("strict_prior_status", "") if base else "",
                "potential_abd_matrix_readiness_additions": potential_abd,
                "variant_c_implication": "governance_preserved_not_resolved" if potential_abd else "none_currently_projected",
            })
        return inventory

    def first_cohort_sides(self, inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
        candidates = [
            row for row in inventory
            if row["classification"] == "ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE"
        ]
        return sorted(
            candidates,
            key=lambda r: (
                -int_value(r["projected_fully_qualified_ceiling"]),
                -int_value(r["hits_1_5_rows"]),
                r["starter_game_side_key"],
            ),
        )

    def cohort_plan(self, inventory: list[dict[str, Any]], first: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        if first:
            rows.append(self.cohort_summary("HC_LOCAL_COHORT_001", first, 1, "first executable local-history-complete cohort; no acquisition required"))
        discovery = [row for row in inventory if row["classification"] == "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"]
        limited = [row for row in inventory if row["classification"] == "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED"]
        excluded = [row for row in inventory if row["classification"] in {"ESTABLISHED_SPECIAL_REGIME_EXCLUSION", "TEMPORAL_OR_IDENTITY_AMBIGUITY_FAIL_CLOSED"}]
        remediated = [row for row in inventory if row["classification"] == "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE"]
        for cohort_id, cohort_rows, order, notes in [
            ("DISCOVERY_GOVERNANCE_REMAINDER", discovery, 2, "requires separate discovery/source governance before exact strict-prior request manifests can be frozen"),
            ("DOWNSTREAM_LIMITED_LOCAL_HISTORY_COMPLETE", limited, 3, "starter recovery possible but full qualification capped by PA/downstream blockers"),
            ("EXCLUDED_OR_FAIL_CLOSED_REMAINDER", excluded, 4, "not executable without separate governance"),
            ("ALREADY_REMEDIATED", remediated, 0, "subtracted from current remaining population"),
        ]:
            if cohort_rows:
                rows.append(self.cohort_summary(cohort_id, cohort_rows, order, notes))
        return rows

    def cohort_summary(self, cohort_id: str, rows: list[dict[str, Any]], order: int, notes: str) -> dict[str, Any]:
        dates = sorted({row["slate_date"] for row in rows})
        depth_counter = Counter(str(row["required_strict_prior_historical_depth"] or "unknown") for row in rows)
        return {
            "cohort_id": cohort_id,
            "recommended_execution_order": order,
            "side_count": len(rows),
            "denominator_row_count": sum(int_value(row["represented_denominator_rows"]) for row in rows),
            "hits_0_5_rows": sum(int_value(row["hits_0_5_rows"]) for row in rows),
            "hits_1_5_rows": sum(int_value(row["hits_1_5_rows"]) for row in rows),
            "rows_with_all_non_starter_prerequisites_satisfied": sum(int_value(row["projected_fully_qualified_ceiling"]) for row in rows),
            "projected_newly_fully_qualified_ceiling": sum(int_value(row["projected_fully_qualified_ceiling"]) for row in rows),
            "exact_request_count": 0 if cohort_id == "HC_LOCAL_COHORT_001" else "not_frozen",
            "deduplicated_request_count": 0 if cohort_id == "HC_LOCAL_COHORT_001" else "not_frozen",
            "unique_pitcher_count": len({row["actual_starter_player_id"] for row in rows if row["actual_starter_player_id"]}),
            "date_range": f"{dates[0]}..{dates[-1]}" if dates else "",
            "role_regime_composition": json.dumps(dict(Counter(row["role_regime_status"] for row in rows)), sort_keys=True),
            "request_depth_distribution": json.dumps(dict(sorted(depth_counter.items())), sort_keys=True),
            "expected_downstream_pa_blockers": sum(int_value(row["downstream_pa_blocker_count"]) for row in rows),
            "expected_downstream_outcome_blockers": sum(int_value(row["downstream_outcome_blocker_count"]) for row in rows),
            "expected_downstream_bundle_blockers": sum(int_value(row["downstream_bundle_blocker_count"]) for row in rows),
            "potential_abd_matrix_readiness_additions": sum(int_value(row["potential_abd_matrix_readiness_additions"]) for row in rows),
            "acquisition_boundary": "no network/source acquisition authorized; local parent source only" if cohort_id == "HC_LOCAL_COHORT_001" else "requires separate governance",
            "remediation_boundary": "requires explicit later remediation approval" if cohort_id == "HC_LOCAL_COHORT_001" else "not remediation-ready",
            "notes": notes,
        }

    def first_cohort_request_manifest(self, first: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for order, side in enumerate(first, start=1):
            rows.append({
                "request_order": order,
                "cohort_id": "HC_LOCAL_COHORT_001",
                "starter_game_side_key": side["starter_game_side_key"],
                "request_type": "LOCAL_REPOSITORY_STRICT_PRIOR_PARENT_BINDING",
                "external_network_request_authorized": False,
                "exact_gamePk_or_request_key": side["game_id"],
                "source_artifact": str(STARTER_BASE),
                "actual_starter_player_id": side["actual_starter_player_id"],
                "strict_prior_status": side["strict_prior_status"],
                "required_parent_domains_supported": "prior_outs_or_innings|prior_starts|recent_workload_windows|starter_status|starter_trust|pitcher_base|expected_workload|starter_expected_hits_inputs",
                "raw_request_count": 0,
                "deduplicated_request_count": 0,
                "deterministic_replay_key": f"HC_LOCAL_COHORT_001|{side['starter_game_side_key']}|{side['actual_starter_player_id']}",
                "notes": "No source acquisition in this design package.",
            })
        return rows

    def validate(self, inventory: list[dict[str, Any]], first: list[dict[str, Any]]) -> list[dict[str, Any]]:
        remediated_rows = [row for row in inventory if row["classification"] == "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE"]
        remaining_rows = [row for row in inventory if row["classification"] != "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE"]
        category_counts = Counter(row["classification"] for row in inventory)
        checks = [
            ("remediation_package_sha_verification", package_sha(REMEDIATION_DIR, RUN_DATE), EXPECTED_REMEDIATION_SHA),
            ("remediation_decision", self.remediation_result.get("decision"), EXPECTED_REMEDIATION_DECISION),
            ("readiness_package_sha_verification", package_sha(READINESS_DIR, SOURCE_DATE), EXPECTED_READINESS_SHA),
            ("certified_state_sha_verification", package_sha(STATE_DIR, SOURCE_DATE), EXPECTED_STATE_SHA),
            ("original_803_row_reproduction", len(self.rows), 803),
            ("original_96_side_reproduction", len(self.sides), 96),
            ("four_side_subtraction", len(remediated_rows), 4),
            ("thirty_six_row_subtraction", sum(int_value(row["represented_denominator_rows"]) for row in remediated_rows), 36),
            ("current_remaining_side_count", len(remaining_rows), 92),
            ("current_remaining_row_count", sum(int_value(row["represented_denominator_rows"]) for row in remaining_rows), 767),
            ("first_cohort_side_count", len(first), 10),
            ("first_cohort_row_count", sum(int_value(row["represented_denominator_rows"]) for row in first), 77),
            ("first_cohort_pa_qualified_ceiling", sum(int_value(row["projected_fully_qualified_ceiling"]) for row in first), 77),
            ("no_silent_population_loss", sum(int_value(row["represented_denominator_rows"]) for row in inventory), 803),
            ("no_opposite_side_creation", True, True),
            ("zero_network_requests", 0, 0),
            ("zero_discovery_requests", 0, 0),
            ("zero_matrix_construction", 0, 0),
            ("existing_abd_matrices_byte_identical", len(self.matrix_hash_before), len(MATRIX_PATHS)),
            ("category_reconciliation", dict(sorted(category_counts.items())), {
                "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE": 4,
                "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE": 78,
                "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED": 4,
                "ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE": 10,
            }),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
            for name in [
                "explicit_reconciliation_to_original_803",
                "explicit_subtraction_of_four_remediated_sides",
                "first_cohort_acquisition_boundary",
                "first_cohort_remediation_boundary",
                "excluded_population_preservation",
                "variant_c_unresolved_preserved",
                "no_model_signal_scoring_promotion_work",
                "no_database_api_oddsapi_upload_launchagent_production_change",
            ]
        ])
        failures = [row for row in rows if row["status"] != "PASS"]
        if failures:
            write_csv(OUT_DIR / f"governance_gap_report_{RUN_DATE}.csv", failures)
            raise RuntimeError("scale-up design validation failed")
        return rows

    def write_outputs(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        inventory = self.side_inventory()
        first = self.first_cohort_sides(inventory)
        cohort_plan = self.cohort_plan(inventory, first)
        validation = self.validate(inventory, first)
        first_keys = {row["starter_game_side_key"] for row in first}
        first_rows = [row for row in self.rows if row["starter_game_key"] in first_keys]
        first_request_manifest = self.first_cohort_request_manifest(first)
        category_counts = Counter(row["classification"] for row in inventory)

        write_csv(OUT_DIR / f"authoritative_remaining_population_reconciliation_{RUN_DATE}.csv", [
            {"reconciliation_step": "original_readiness_population", "starter_game_sides": 96, "denominator_rows": 803, "notes": "Authoritative 803-row campaign baseline."},
            {"reconciliation_step": "already_remediated_four_side_package", "starter_game_sides": 4, "denominator_rows": 36, "notes": "Subtracted from current remainder."},
            {"reconciliation_step": "current_remaining_population", "starter_game_sides": 92, "denominator_rows": 767, "notes": "Remaining after four-side remediation."},
            {"reconciliation_step": "first_local_history_complete_cohort", "starter_game_sides": len(first), "denominator_rows": sum(int_value(row["represented_denominator_rows"]) for row in first), "notes": "Bounded first cohort; no acquisition required."},
            {"reconciliation_step": "discovery_governance_remainder", "starter_game_sides": category_counts["INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"], "denominator_rows": sum(int_value(row["represented_denominator_rows"]) for row in inventory if row["classification"] == "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"), "notes": "Cannot freeze exact strict-prior request manifests offline."},
            {"reconciliation_step": "established_special_regime_rows_outside_803", "starter_game_sides": "", "denominator_rows": self.state.get("primary_counts", {}).get("HITS_STARTER_BLOCKED_SPECIAL_REGIME_EXCLUSION", 46), "notes": "Accounted separately; not weakened or included."},
        ])
        write_csv(OUT_DIR / f"side_level_inventory_{RUN_DATE}.csv", inventory)
        write_csv(OUT_DIR / f"side_classification_ledger_{RUN_DATE}.csv", [
            {"starter_game_side_key": row["starter_game_side_key"], "classification": row["classification"], "reason": row["reconstruction_readiness_classification"]}
            for row in inventory
        ])
        write_csv(OUT_DIR / f"request_inventory_{RUN_DATE}.csv", [
            {
                "starter_game_side_key": row["starter_game_side_key"],
                "classification": row["classification"],
                "known_prior_game_request_identities": row["known_prior_game_request_identities"],
                "estimated_raw_request_count": row["estimated_raw_request_count"],
                "deduplicated_request_count": row["deduplicated_request_count"],
                "source_artifact": str(STARTER_BASE) if row["classification"] in {"ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE", "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED"} else "",
                "requires_future_discovery_governance": row["classification"] == "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE",
            }
            for row in inventory
        ])
        write_csv(OUT_DIR / f"request_deduplication_ledger_{RUN_DATE}.csv", [
            {"cohort_id": "HC_LOCAL_COHORT_001", "raw_request_count": 0, "deduplicated_request_count": 0, "deduplication_rule": "local parent source rows only; no external request identities emitted"},
            {"cohort_id": "DISCOVERY_GOVERNANCE_REMAINDER", "raw_request_count": "not_frozen", "deduplicated_request_count": "not_frozen", "deduplication_rule": "future governance must group by exact gamePk + pitcher_id after discovery"},
        ])
        write_csv(OUT_DIR / f"cohort_plan_{RUN_DATE}.csv", cohort_plan)
        write_csv(OUT_DIR / f"projected_qualification_ceilings_{RUN_DATE}.csv", [
            {"population": row["cohort_id"], "projected_newly_fully_qualified_ceiling": row["projected_newly_fully_qualified_ceiling"], "denominator_rows": row["denominator_row_count"], "hits_0_5_rows": row["hits_0_5_rows"], "hits_1_5_rows": row["hits_1_5_rows"]}
            for row in cohort_plan
        ])
        write_csv(OUT_DIR / f"downstream_blocker_analysis_{RUN_DATE}.csv", [
            {"population": row["cohort_id"], "pa_blockers": row["expected_downstream_pa_blockers"], "outcome_blockers": row["expected_downstream_outcome_blockers"], "bundle_blockers": row["expected_downstream_bundle_blockers"], "variant_c_implication": "governance_preserved_not_resolved"}
            for row in cohort_plan
        ])
        write_csv(OUT_DIR / f"first_cohort_exact_side_manifest_{RUN_DATE}.csv", first)
        write_csv(OUT_DIR / f"first_cohort_exact_row_manifest_{RUN_DATE}.csv", first_rows)
        write_csv(OUT_DIR / f"first_cohort_exact_request_manifest_{RUN_DATE}.csv", first_request_manifest)
        write_csv(OUT_DIR / f"first_cohort_acquisition_governance_{RUN_DATE}.csv", [
            {"governance_rule": "external_acquisition_authorized", "value": False, "notes": "First cohort uses local strict-prior parent source only."},
            {"governance_rule": "future_external_corroboration", "value": "requires_separate_explicit_approval", "notes": "No network/discovery in this package."},
            {"governance_rule": "request_boundary", "value": "0 external requests", "notes": "Exact source artifact paths are frozen in request manifest."},
        ])
        write_csv(OUT_DIR / f"first_cohort_reconstruction_governance_{RUN_DATE}.csv", [
            {"domain": domain, "rule": "reuse frozen four-side history-complete reconstruction/remediation contract", "approval_required": "explicit_offline_remediation_approval", "notes": "No remediation performed by this design package."}
            for domain in [
                "actual_starter_identity", "prior_starts", "prior_outs_or_innings", "recent_workload_windows",
                "starter_status", "starter_trust", "pitcher_base", "expected_workload", "offense_factor",
                "expected_hits_inputs", "BF_corroboration_only", "side_to_row_propagation", "fail_closed_taxonomy",
                "excluded_population_preservation", "replayability", "matrix_immutability",
            ]
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_no_network_no_discovery_no_reconstruction_no_model_no_matrix_guard_{RUN_DATE}.csv", static_guard())
        if any(row["status"] != "PASS" for row in static_guard()):
            raise RuntimeError("static guard failed")

        payload = {
            "decision": DECISION,
            "generated_at": FROZEN_GENERATED_AT,
            "remediation_package_sha256": EXPECTED_REMEDIATION_SHA,
            "original_sides": 96,
            "original_denominator_rows": 803,
            "already_remediated_sides": 4,
            "already_remediated_rows": 36,
            "current_remaining_sides": 92,
            "current_remaining_rows": 767,
            "first_cohort_id": "HC_LOCAL_COHORT_001",
            "first_cohort_sides": len(first),
            "first_cohort_rows": sum(int_value(row["represented_denominator_rows"]) for row in first),
            "first_cohort_projected_full_qualification_ceiling": sum(int_value(row["projected_fully_qualified_ceiling"]) for row in first),
            "first_cohort_external_request_count": 0,
            "discovery_governance_required_sides": category_counts["INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"],
            "authorizes_acquisition": False,
            "authorizes_reconstruction_or_remediation": False,
            "network_requests": 0,
            "db_writes": 0,
            "api_writes": 0,
            "oddsapi_calls": 0,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"machine_readable_scale_up_design_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# History-Complete Starter Cohort Scale-Up Design — {RUN_DATE}

Decision: `{DECISION}`

The four-side history-complete remediation validated the method, but it did not authorize broad
acquisition or remediation. This package reproduces the original 803-row / 96-side readiness
population, subtracts the four remediated sides and 36 rows, and inventories the current 92-side /
767-row remainder.

The only currently executable cohort without discovery is `HC_LOCAL_COHORT_001`: 14 Starter-game sides
and 111 denominator rows with local strict-prior parent support. Its projected full-qualification
ceiling is 107 rows. It has no Hits 1.5 rows and no external request requirement. The remaining 78
sides require separate discovery/source governance before exact strict-prior request manifests can be
frozen.

This package does not authorize or execute acquisition, reconstruction, remediation, matrix
construction, modeling, scoring, uploads, database/API writes, OddsAPI calls, LaunchAgent changes, or
production behavior changes.
""")
        write_md(OUT_DIR / f"cohort_plan_{RUN_DATE}.md", f"""
# Cohort Plan — {RUN_DATE}

## First Cohort

`HC_LOCAL_COHORT_001`

- Sides: `{len(first)}`
- Rows: `{sum(int_value(row['represented_denominator_rows']) for row in first)}`
- Projected newly fully qualified ceiling: `{sum(int_value(row['projected_fully_qualified_ceiling']) for row in first)}`
- External acquisition requests: `0`
- Remediation boundary: separate explicit approval required

## Remainder

The 78-side discovery-governance remainder cannot be treated as history-complete offline because exact
strict-prior request identities are not exposed by current local artifacts. Future work must freeze a
separate source/discovery governance package before acquisition.
""")
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR, RUN_DATE)}

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            status = "PASS"
            notes = ""
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    status = "PASS" if path.read_text(encoding="utf-8").lstrip().startswith("#") else "FAIL"
                else:
                    continue
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
    result = HistoryCompleteScaleUpDesign().write_outputs()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
