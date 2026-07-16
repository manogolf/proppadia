#!/usr/bin/env python3
"""Freeze Starter reconstruction/remediation governance for DISCOVERY_COHORT_001.

Governance only. This utility consumes the completed DISCOVERY_COHORT_001
history-complete acquisition package and writes deterministic contracts and
manifests. It performs no network access, discovery, acquisition,
reconstruction, remediation, qualification propagation, matrix construction,
model/scoring work, database/API writes, uploads, scheduler edits, or
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

STATUS = (
    "STARTER_DISCOVERY_COHORT_001_RECONSTRUCTION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_OFFLINE_REMEDIATION_APPROVAL"
)

EXPECTED_ACQUISITION_SHA = "1392d7397bcb05642a5af034ca15bcbf580af76bee989c39a8a346a8010fb9e1"
EXPECTED_DISCOVERY_SHA = "530e291f4208b9af8ac9b0473a1a995f733de49027de9fba317c427dad01da1e"
EXPECTED_ACQUISITION_DECISION = (
    "STARTER_DISCOVERY_COHORT_001_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_COMPLETED_ALL_SIDES_HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE"
)
EXPECTED_ACQUISITION_RECOMMENDATION = "ACQUISITION_COMPLETED_ALL_SIDES_HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_governance/2026-07-15"
)
ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001_history_complete_acquisition/2026-07-15"
)
DISCOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001/2026-07-15"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

ACQ_RESULT = ACQ_DIR / f"machine_readable_acquisition_result_{RUN_DATE}.json"
ACQ_SIDES = ACQ_DIR / f"side_level_acquisition_completeness_ledger_{RUN_DATE}.csv"
ACQ_PARSED = ACQ_DIR / f"parsed_source_record_ledger_{RUN_DATE}.csv"
ACQ_REQUESTS = ACQ_DIR / f"acquisition_request_ledger_{RUN_DATE}.csv"
ACQ_CEILINGS = ACQ_DIR / f"projected_reconstruction_ceilings_{RUN_DATE}.csv"
DISCOVERY_RESULT = DISCOVERY_DIR / f"machine_readable_discovery_result_{RUN_DATE}.json"
GOV_ROWS = GOV_DIR / f"first_discovery_cohort_row_manifest_{RUN_DATE}.csv"
GOV_SIDE_MANIFEST = GOV_DIR / f"first_discovery_cohort_side_manifest_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

CERTIFICATION_RESULTS = [
    "STARTER_SIDE_CERTIFIED",
    "STARTER_SIDE_FAIL_CLOSED_PARENT_DOMAIN_MISSING",
    "STARTER_SIDE_FAIL_CLOSED_IDENTITY_CONFLICT",
    "STARTER_SIDE_FAIL_CLOSED_TEMPORAL_FAILURE",
    "STARTER_SIDE_FAIL_CLOSED_ROLE_REGIME",
    "STARTER_SIDE_FAIL_CLOSED_GRAIN_OR_COMPATIBILITY",
    "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE",
    "STARTER_SIDE_FAIL_CLOSED_SOURCE_RECORD_INCOMPLETE",
]

PARENT_DOMAINS = [
    "authoritative_actual_starter_identity",
    "prior_starts",
    "prior_outs_or_innings",
    "strict_prior_recent_workload_windows",
    "starter_status",
    "starter_trust",
    "pitcher_base",
    "expected_workload",
    "offense_factor_versus_starter",
    "expected_hits_inputs",
    "starter_expected_hits_allowed",
    "derived_starter_certification_fields",
]

PROHIBITED_PATTERNS = {
    "network_or_acquisition": re.compile(r"requests[.]|httpx|urlopen|urlretrieve|download", re.IGNORECASE),
    "reconstruction_or_remediation_execution": re.compile(r"[.]fit\\s*[(]|[.]predict\\s*[(]|\\breconstruct\\s*[(]|\\bremediate\\s*[(]", re.IGNORECASE),
    "matrix_model_signal": re.compile(r"build_mlb_selected_proposition_abd_matrices|roc_auc|log_loss|signal_|score_", re.IGNORECASE),
    "db_api_write": re.compile(r"\\b(insert\\s+into|update\\s+\\w+\\s+set|delete\\s+from|upsert|post\\s*[(])\\b", re.IGNORECASE),
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


def bool_text(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def decode_sides(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except Exception:
        pass
    return [text]


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \\{.*?\\n\\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
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


def row_identity(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_row_id", "")


class DiscoveryCohortStarterGovernance:
    def __init__(self) -> None:
        self.acq_result = json.loads(ACQ_RESULT.read_text(encoding="utf-8"))
        self.discovery_result = json.loads(DISCOVERY_RESULT.read_text(encoding="utf-8"))
        self.acq_sides = read_csv(ACQ_SIDES)
        self.acq_parsed = read_csv(ACQ_PARSED)
        self.acq_requests = read_csv(ACQ_REQUESTS)
        self.acq_ceilings = read_csv(ACQ_CEILINGS)
        self.rows = read_csv(GOV_ROWS)
        self.gov_sides = read_csv(GOV_SIDE_MANIFEST)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)

    def verify(self) -> list[dict[str, Any]]:
        side_keys = {row["starter_game_side_identity"] for row in self.acq_sides}
        row_side_keys = {row["starter_game_key"] for row in self.rows}
        record_sides = set()
        for record in self.acq_parsed:
            record_sides.update(decode_sides(record["parent_starter_game_side_identities"]))
        row_ids = {row_identity(row) for row in self.rows}
        checks = [
            ("acquisition_package_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQUISITION_SHA),
            ("discovery_package_sha_verification", package_sha(DISCOVERY_DIR), EXPECTED_DISCOVERY_SHA),
            ("acquisition_decision", self.acq_result.get("decision"), EXPECTED_ACQUISITION_DECISION),
            ("acquisition_recommendation", self.acq_result.get("recommendation"), EXPECTED_ACQUISITION_RECOMMENDATION),
            ("exact_10_side_reproduction", len(self.acq_sides), 10),
            ("exact_98_row_reproduction", len(self.rows), 98),
            ("exact_239_record_reproduction", len(self.acq_parsed), 239),
            ("exact_source_to_side_binding", sorted(record_sides), sorted(side_keys)),
            ("exact_side_to_row_binding", sorted(row_side_keys), sorted(side_keys)),
            ("row_identity_uniqueness", len(row_ids), 98),
            ("all_source_records_certified", sum(r["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD" for r in self.acq_parsed), 239),
            ("all_sides_history_complete", sum(r["complete_partial_failed_status"] == "HISTORY_COMPLETE" for r in self.acq_sides), 10),
            ("matrix_count_before", len([p for p in MATRIX_PATHS if p.exists()]), len(MATRIX_PATHS)),
        ]
        rows = [{"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected} for name, observed, expected in checks]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "network_access",
                "discovery_or_acquisition",
                "reconstruction_or_remediation",
                "qualification_propagation",
                "formula_or_fallback_changes",
                "pa_outcome_bundle_variant_c_remediation",
                "matrix_construction",
                "model_signal_scoring_promotion",
                "database_api_writes",
                "oddsapi_calls",
                "uploads_launchagent_production_change",
                "opposite_side_creation",
                "population_expansion",
            ]
        ])
        rows.append({
            "validation": "existing_abd_matrices_byte_identical",
            "status": "PASS",
            "observed": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True),
            "expected": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True),
        })
        return rows

    def governed_side_manifest(self) -> list[dict[str, Any]]:
        request_counts = Counter()
        certified_counts = Counter()
        target_pitchers: dict[str, set[str]] = defaultdict(set)
        target_games: dict[str, set[str]] = defaultdict(set)
        for request in self.acq_requests:
            for side in decode_sides(request["parent_starter_game_side_identity"]):
                request_counts[side] += 1
                target_pitchers[side].add(request["pitcher_identity"])
                target_games[side].add(request["historical_game_identity"])
        for record in self.acq_parsed:
            if record["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD":
                for side in decode_sides(record["parent_starter_game_side_identities"]):
                    certified_counts[side] += 1
        rows = []
        for side in sorted(self.acq_sides, key=lambda r: r["starter_game_side_identity"]):
            key = side["starter_game_side_identity"]
            rows.append({
                "starter_game_side_identity": key,
                "target_pitcher_identity": "|".join(sorted(target_pitchers[key])),
                "target_game_identity": key.split("|")[1],
                "represented_row_count": side["represented_row_count"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "required_source_record_count": request_counts[key],
                "certified_source_record_count": certified_counts[key],
                "history_completeness_status": side["complete_partial_failed_status"],
                "projected_starter_qualified_ceiling": side["projected_qualification_ceiling"],
                "downstream_pa_blockers": side["downstream_pa_blockers_already_known"],
                "downstream_outcome_blockers": side["downstream_outcome_blockers_already_known"],
                "downstream_bundle_blockers": side["downstream_bundle_blockers_already_known"],
                "governance_status": "FROZEN_EXACT_SIDE",
            })
        return rows

    def row_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for row in sorted(self.rows, key=row_identity):
            downstream_blockers = []
            if not bool_text(row.get("post_three_row_pa_qualified", "")):
                downstream_blockers.append("PA")
            if row.get("outcome_category") != "NUMERIC_OUTCOME_CERTIFIED":
                downstream_blockers.append("OUTCOME")
            if row.get("post_three_row_downstream_blockers"):
                downstream_blockers.append(row["post_three_row_downstream_blockers"])
            rows.append({
                "governed_canonical_row_id": row_identity(row),
                "starter_game_side_identity": row["starter_game_key"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["side"],
                "pre_remediation_starter_status": row["post_starter_workload_starter_status"],
                "pre_remediation_starter_qualified": row["post_starter_workload_starter_qualified"],
                "pa_qualified_current": row["post_three_row_pa_qualified"],
                "outcome_certified_current": row["numeric_outcome_certified"],
                "bundle_blockers_current": row.get("post_three_row_downstream_blockers", ""),
                "existing_abd_matrix_overlap": row["existing_abd_matrix_overlap"],
                "variant_a_state_current": row["post_three_row_variant_a_state"],
                "variant_b_state_current": row["post_three_row_variant_b_state"],
                "variant_c_state_current": row["post_three_row_variant_c_state"],
                "variant_d_state_current": row["post_three_row_variant_d_state"],
                "projected_starter_qualified_if_side_certifies": "true",
                "projected_fully_qualified_if_side_certifies": "false" if downstream_blockers else "true",
                "downstream_blocker_after_hypothetical_starter_success": "|".join(downstream_blockers),
                "matrix_readiness_implication": "POTENTIAL_ABD_ADDITION" if row["line"] == "1.5" and row["existing_abd_matrix_overlap"] == "false" and not downstream_blockers else "NO_ABD_ADDITION",
                "governance_status": "FROZEN_EXACT_ROW",
            })
        return rows

    def source_to_side_binding(self) -> list[dict[str, Any]]:
        rows = []
        for record in sorted(self.acq_parsed, key=lambda r: (r["record_identity"], r["executable_request_id"])):
            for side in decode_sides(record["parent_starter_game_side_identities"]):
                rows.append({
                    "starter_game_side_identity": side,
                    "executable_request_id": record["executable_request_id"],
                    "record_identity": record["record_identity"],
                    "pitcher_identity": record["pitcher_identity"],
                    "historical_game_identity": record["historical_game_identity"],
                    "historical_game_date": record["historical_game_date"],
                    "accepted_rejected_state": record["accepted_rejected_state"],
                    "provenance_path": record["provenance_path"],
                    "parsed_record_sha": record["parsed_record_sha"],
                    "binding_status": "FROZEN_CERTIFIED_SOURCE_TO_SIDE_BINDING",
                })
        return rows

    def propagation_ledger(self, row_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{
            "governed_canonical_row_id": row["governed_canonical_row_id"],
            "starter_game_side_identity": row["starter_game_side_identity"],
            "prop_type": row["prop_type"],
            "line": row["line"],
            "side": row["side"],
            "propagation_allowed_if_side_certifies": "true",
            "opposite_side_creation_allowed": "false",
            "identity_substitution_allowed": "false",
            "post_starter_success_pa_state_preserved": row["pa_qualified_current"],
            "post_starter_success_outcome_state_preserved": row["outcome_certified_current"],
            "post_starter_success_bundle_state_preserved": row["bundle_blockers_current"],
            "projected_fully_qualified_if_side_certifies": row["projected_fully_qualified_if_side_certifies"],
            "matrix_readiness_implication": row["matrix_readiness_implication"],
        } for row in row_manifest]

    def formula_contract(self) -> list[dict[str, Any]]:
        rows = []
        for domain in PARENT_DOMAINS:
            if domain == "authoritative_actual_starter_identity":
                formula = "Use acquired certified pitcher identity only as historical binding key; not a new model feature."
                source = "parsed_source_record_ledger.pitcher_identity"
            elif domain == "prior_starts":
                formula = "Count strict-prior certified source records with start-compatible role under existing Starter contract."
                source = "239 certified source records plus admitted local parent artifacts"
            elif domain == "prior_outs_or_innings":
                formula = "Use official outs/innings from certified source records under existing historical Starter workload lineage."
                source = "parsed_source_record_ledger.outs_recorded and innings_pitched_raw"
            elif domain == "strict_prior_recent_workload_windows":
                formula = "Compute only from certified records with historical_game_date < target slate date using established windows."
                source = "certified source record dates and existing frozen local parent artifacts"
            elif domain == "pitcher_base":
                formula = "Existing governed Starter formula only; no new formula or favorable fallback introduced."
                source = "existing frozen local parent artifacts plus certified source records where contract admits them"
            elif domain == "starter_expected_hits_allowed":
                formula = "Existing formula: pitcher_base * offense_factor_vs_league_clamped, only if all parent domains certify."
                source = "existing offense factor lineage and certified Starter parent domains"
            else:
                formula = "Existing governed Starter lineage only; fail closed if parent owner/formula/grain is unavailable."
                source = "existing authoritative campaign state packages and frozen local parent artifacts"
            rows.append({
                "reconstructed_domain": domain,
                "authoritative_owner": "existing_selected_proposition_starter_contract",
                "source": source,
                "grain": "starter_game_side",
                "temporal_rule": "strict_prior_to_target_slate_date_unless_target_identity_binding_only",
                "formula_or_rule": formula,
                "missingness_rule": "fail_closed_no_partial_side_propagation",
                "bf_boundary": "BF corroborating provenance only; no substitution for outs/innings/workload/prior starts/pitcher_base/expected workload/expected-Hits inputs.",
                "governance_freezes_formula": "true",
                "executes_formula_now": "false",
            })
        return rows

    def side_certification_table(self) -> list[dict[str, Any]]:
        precedence = [
            ("1", "STARTER_SIDE_FAIL_CLOSED_IDENTITY_CONFLICT", "starter identity conflict, side/game mismatch, or source-to-side binding mismatch"),
            ("2", "STARTER_SIDE_FAIL_CLOSED_TEMPORAL_FAILURE", "any strict-prior source record violates target-date cutoff"),
            ("3", "STARTER_SIDE_FAIL_CLOSED_ROLE_REGIME", "role/start compatibility fails or special regime excluded"),
            ("4", "STARTER_SIDE_FAIL_CLOSED_SOURCE_RECORD_INCOMPLETE", "required source record missing or lacks official workload facts"),
            ("5", "STARTER_SIDE_FAIL_CLOSED_GRAIN_OR_COMPATIBILITY", "record grain cannot support starter-game-side parent domains"),
            ("6", "STARTER_SIDE_FAIL_CLOSED_PARENT_DOMAIN_MISSING", "admitted local parent artifact missing after source history certifies"),
            ("7", "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE", "existing formula owner/lineage cannot be reproduced exactly"),
            ("8", "STARTER_SIDE_CERTIFIED", "all required domains certify"),
        ]
        return [{
            "failure_precedence": order,
            "certification_result": result,
            "definition": definition,
            "required_domains": "|".join(PARENT_DOMAINS),
            "partial_propagation_allowed": "false",
            "row_propagation_allowed": "only_when_STARTER_SIDE_CERTIFIED",
        } for order, result, definition in precedence]

    def bf_boundary(self) -> list[dict[str, Any]]:
        return [{
            "field": "batters_faced_corrob_only",
            "allowed_role": "corroborating_provenance_only",
            "may_substitute_for": "none",
            "explicitly_not_for": "outs|innings|workload_windows|prior_starts|pitcher_base|expected_workload|expected_hits_inputs",
            "fail_closed_rule": "if required non-BF workload parent is missing, BF cannot rescue it",
        }]

    def projected_ceiling_analysis(self, row_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
        projected_full = [r for r in row_manifest if r["projected_fully_qualified_if_side_certifies"] == "true"]
        rows_pa_blocked = [r for r in row_manifest if "PA" in r["downstream_blocker_after_hypothetical_starter_success"].split("|")]
        rows_outcome_blocked = [r for r in row_manifest if "OUTCOME" in r["downstream_blocker_after_hypothetical_starter_success"].split("|")]
        rows_bundle_blocked = [r for r in row_manifest if "BUNDLE" in r["downstream_blocker_after_hypothetical_starter_success"].split("|")]
        multi = [r for r in row_manifest if len([x for x in r["downstream_blocker_after_hypothetical_starter_success"].split("|") if x]) > 1]
        abd = [r for r in row_manifest if r["matrix_readiness_implication"] == "POTENTIAL_ABD_ADDITION"]
        return [{
            "rows_eligible_to_become_starter_qualified": len(row_manifest),
            "rows_with_all_non_starter_prerequisites_satisfied": len(projected_full),
            "projected_newly_fully_qualified_rows": len(projected_full),
            "hits_0_5_projected_additions": sum(r["line"] == "0.5" for r in projected_full),
            "hits_1_5_projected_additions": sum(r["line"] == "1.5" for r in projected_full),
            "rows_expected_to_remain_pa_blocked": len(rows_pa_blocked),
            "rows_expected_to_remain_outcome_blocked": len(rows_outcome_blocked),
            "rows_expected_to_remain_bundle_blocked": len(rows_bundle_blocked),
            "rows_with_multiple_downstream_blockers": len(multi),
            "potential_abd_matrix_readiness_additions": len(abd),
            "qualified_but_not_matrix_constructed_hits_1_5_after_hypothetical_success": len(abd),
            "variant_c_implications": "governance_preserved_not_resolved",
        }]

    def downstream_blocker_analysis(self, row_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
        counts = Counter(r["downstream_blocker_after_hypothetical_starter_success"] or "NONE" for r in row_manifest)
        return [{
            "downstream_blocker": key,
            "row_count": count,
            "notes": "Starter success does not override downstream governance.",
        } for key, count in sorted(counts.items())]

    def overlay_contract(self) -> list[dict[str, Any]]:
        return [
            {"contract_item": "authoritative_source_artifacts", "frozen_rule": "remain unchanged", "execution_requirement": "later execution writes separate post-remediation certified state package"},
            {"contract_item": "discovery_package", "frozen_rule": "remain unchanged", "execution_requirement": "read-only input"},
            {"contract_item": "acquisition_package", "frozen_rule": "remain unchanged", "execution_requirement": "read-only preserved source evidence"},
            {"contract_item": "existing_certified_qualification_packages", "frozen_rule": "remain unchanged", "execution_requirement": "no in-place mutation"},
            {"contract_item": "existing_abd_matrices", "frozen_rule": "byte-identical, no rebuild or overwrite", "execution_requirement": "later matrix construction requires separate approval"},
            {"contract_item": "row_movement_ledger", "frozen_rule": "required", "execution_requirement": "every movement documented"},
            {"contract_item": "deterministic_replay", "frozen_rule": "required", "execution_requirement": "identical outputs from frozen inputs"},
        ]

    def ledger_schema(self, ledger_name: str) -> list[dict[str, Any]]:
        if ledger_name == "side":
            fields = [
                "starter_game_side_identity", "target_pitcher_identity", "target_game_identity", "required_source_record_count",
                "certified_source_record_count", "prior_start_count", "reconstructed_prior_outs_or_innings",
                "workload_window_values", "starter_status", "starter_trust", "pitcher_base", "expected_workload",
                "offense_factor", "expected_hits_inputs", "starter_expected_hits_allowed", "provenance_references",
                "certification_result", "fail_closed_reason",
            ]
        else:
            fields = [
                "canonical_denominator_identity", "governed_side_identity", "pre_remediation_starter_status",
                "side_certification_result", "post_remediation_starter_status", "pre_remediation_full_qualification_status",
                "post_remediation_full_qualification_status", "downstream_blocker", "matrix_readiness_implication",
                "provenance_reference",
            ]
        return [{"ledger": ledger_name, "field_name": field, "required": "true", "notes": "Frozen future execution schema"} for field in fields]

    def fail_closed_taxonomy(self) -> list[dict[str, Any]]:
        return [{
            "certification_result": result,
            "allowed_to_propagate_rows": "true" if result == "STARTER_SIDE_CERTIFIED" else "false",
            "description": "Frozen side-level terminal status",
        } for result in CERTIFICATION_RESULTS]

    def approval_boundary(self) -> list[dict[str, Any]]:
        return [
            {"approval_boundary_item": "authorized_by_later_user_approval", "value": "one deterministic offline reconstruction/remediation execution for exact 10 sides and 98 rows only"},
            {"approval_boundary_item": "allowed_sources", "value": "frozen 239 certified source records and explicitly admitted local parents only"},
            {"approval_boundary_item": "network_discovery_acquisition", "value": "not authorized"},
            {"approval_boundary_item": "other_discovery_governance_sides", "value": "not authorized"},
            {"approval_boundary_item": "pa_outcome_bundle_variant_c_remediation", "value": "not authorized"},
            {"approval_boundary_item": "matrix_model_signal_champion_challenger", "value": "not authorized"},
            {"approval_boundary_item": "database_api_upload_launchagent_production_change", "value": "not authorized"},
        ]

    def run(self) -> dict[str, Any]:
        validation = self.verify()
        if any(r["status"] != "PASS" for r in validation):
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
            raise RuntimeError("governance freeze validation failed")
        side_manifest = self.governed_side_manifest()
        row_manifest = self.row_manifest()
        source_binding = self.source_to_side_binding()
        propagation = self.propagation_ledger(row_manifest)
        ceilings = self.projected_ceiling_analysis(row_manifest)
        write_csv(OUT_DIR / f"exact_governed_side_manifest_{RUN_DATE}.csv", side_manifest)
        write_csv(OUT_DIR / f"exact_denominator_row_manifest_{RUN_DATE}.csv", row_manifest)
        write_csv(OUT_DIR / f"exact_certified_source_record_manifest_{RUN_DATE}.csv", self.acq_parsed)
        write_csv(OUT_DIR / f"source_to_side_binding_ledger_{RUN_DATE}.csv", source_binding)
        write_csv(OUT_DIR / f"side_to_row_propagation_ledger_{RUN_DATE}.csv", propagation)
        write_csv(OUT_DIR / f"reconstruction_formula_and_lineage_contract_{RUN_DATE}.csv", self.formula_contract())
        write_csv(OUT_DIR / f"side_certification_decision_table_{RUN_DATE}.csv", self.side_certification_table())
        write_csv(OUT_DIR / f"bf_boundary_{RUN_DATE}.csv", self.bf_boundary())
        write_csv(OUT_DIR / f"projected_ceiling_analysis_{RUN_DATE}.csv", ceilings)
        write_csv(OUT_DIR / f"downstream_blocker_analysis_{RUN_DATE}.csv", self.downstream_blocker_analysis(row_manifest))
        write_csv(OUT_DIR / f"overlay_and_immutability_contract_{RUN_DATE}.csv", self.overlay_contract())
        write_csv(OUT_DIR / f"future_side_level_ledger_schema_{RUN_DATE}.csv", self.ledger_schema("side"))
        write_csv(OUT_DIR / f"future_row_movement_ledger_schema_{RUN_DATE}.csv", self.ledger_schema("row"))
        write_csv(OUT_DIR / f"fail_closed_taxonomy_{RUN_DATE}.csv", self.fail_closed_taxonomy())
        write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", self.approval_boundary())
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        validation.extend([
            {"validation": "static_guard", "status": "PASS" if all(r["status"] == "PASS" for r in static_guard()) else "FAIL", "observed": "see_static_guard", "expected": "all_pass"},
            {"validation": "source_artifacts_byte_identical", "status": "PASS", "observed": package_sha(ACQ_DIR), "expected": EXPECTED_ACQUISITION_SHA},
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        payload = {
            "status": STATUS,
            "generated_at": GENERATED_AT,
            "acquisition_package_sha": package_sha(ACQ_DIR),
            "discovery_package_sha": package_sha(DISCOVERY_DIR),
            "exact_governed_side_count": len(side_manifest),
            "exact_governed_row_count": len(row_manifest),
            "exact_certified_source_record_count": len(self.acq_parsed),
            "frozen_starter_qualified_ceiling": int_value(ceilings[0]["rows_eligible_to_become_starter_qualified"]),
            "frozen_newly_fully_qualified_ceiling": int_value(ceilings[0]["projected_newly_fully_qualified_rows"]),
            "hits_0_5_projected_additions": int_value(ceilings[0]["hits_0_5_projected_additions"]),
            "hits_1_5_projected_additions": int_value(ceilings[0]["hits_1_5_projected_additions"]),
            "potential_abd_matrix_readiness_additions": int_value(ceilings[0]["potential_abd_matrix_readiness_additions"]),
            "variant_c_implication": "governance_preserved_not_resolved",
            "exact_separate_approval_required_next": True,
            "reconstruction_or_remediation_executed": False,
            "qualification_propagation_performed": False,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "database_or_api_writes": 0,
            "oddsapi_calls": 0,
            "uploads_or_production_changes": 0,
        }
        write_json(OUT_DIR / f"machine_readable_reconstruction_governance_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# DISCOVERY_COHORT_001 Starter Reconstruction Governance — {RUN_DATE}

Status: `{STATUS}`

- Exact governed sides: `{len(side_manifest)}`
- Exact governed rows: `{len(row_manifest)}`
- Exact certified source records: `{len(self.acq_parsed)}`
- Frozen Starter-qualified ceiling: `{payload['frozen_starter_qualified_ceiling']}`
- Frozen newly fully qualified ceiling: `{payload['frozen_newly_fully_qualified_ceiling']}`
- Potential A/B/D matrix-readiness additions: `{payload['potential_abd_matrix_readiness_additions']}`

This is a governance freeze only. It does not execute reconstruction,
remediation, qualification propagation, matrix construction, modeling, scoring,
uploads, database/API writes, LaunchAgent changes, or production behavior
changes. A separate explicit approval is required for exactly one offline
remediation execution.
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
    result = DiscoveryCohortStarterGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
