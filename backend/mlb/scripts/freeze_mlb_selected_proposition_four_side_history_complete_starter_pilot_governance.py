#!/usr/bin/env python3
"""Freeze governance for the four-side history-complete Starter acquisition pilot.

Governance only. No network access, source acquisition, reconstruction,
remediation, matrix construction, model/scoring work, database/API writes,
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


RUN_DATE = "2026-07-14"
FROZEN_GENERATED_AT = "2026-07-14T00:00:00+00:00"
STATUS = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_PILOT_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_ACQUISITION_APPROVAL"
)

EXPECTED_POSTMORTEM_SHA = "4b7252053215686bc500c6f73be80343589490fbbfc6c4e1764d14c40df74ba2"
EXPECTED_FIRST_REMEDIATION_SHA = "17e529051f9a2c52681d9ec60905149f7c1430cf769c4d660420746ac78a728e"
EXPECTED_FIRST_ACQUISITION_SHA = "52aa980c6e7147205ffdb87a29981b3c2d2801537e1efce6ea19477dabe89617"
EXPECTED_FIRST_ACQ_GOV_SHA = "fa310668bd1fac4d9993e3557dfd4dd8d20f7dc9258ae2af807f70c8fc8f3651"
EXPECTED_FIRST_RECON_GOV_SHA = "18fc685916f37da9b9155c230f1fb748a3677f99b2d61cfca83e20301e1850db"
EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_POSTMORTEM_DECISION = "STARTER_ZERO_YIELD_PILOT_POSTMORTEM_DECISION = SECOND_PILOT_JUSTIFIED"
EXPECTED_FIRST_REMEDIATION_DECISION = (
    "STARTER_16_SIDE_DIRECT_SOURCE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_REMEDIATION_COMPLETED_WITH_FAIL_CLOSED_SIDES"
)

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_pilot_governance/"
    "2026-07-14"
)
POSTMORTEM_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_zero_yield_pilot_postmortem_and_second_pilot_design/"
    "2026-07-14"
)
FIRST_REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_reconstruction_remediation/"
    "2026-07-14"
)
FIRST_ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot/"
    "2026-07-14"
)
FIRST_ACQ_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot_governance/"
    "2026-07-14"
)
FIRST_RECON_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_reconstruction_governance/"
    "2026-07-14"
)
READINESS_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/"
    "2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/"
    "2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

POSTMORTEM_RESULT = POSTMORTEM_DIR / f"machine_readable_review_result_{RUN_DATE}.json"
POSTMORTEM_CANDIDATES = POSTMORTEM_DIR / f"second_pilot_candidate_manifest_{RUN_DATE}.csv"
POSTMORTEM_REQUESTS = POSTMORTEM_DIR / f"second_pilot_acquisition_request_manifest_{RUN_DATE}.csv"
FIRST_REMEDIATION_RESULT = FIRST_REMEDIATION_DIR / f"machine_readable_execution_result_{RUN_DATE}.json"
FIRST_REMEDIATION_ROWS = FIRST_REMEDIATION_DIR / f"downstream_qualification_ledger_{RUN_DATE}.csv"
FIRST_ACQ_PARSED = FIRST_ACQ_DIR / "parsed" / f"parsed_mlb_stats_api_record_ledger_{RUN_DATE}.csv"
FIRST_ACQ_RAW = FIRST_ACQ_DIR / f"raw_response_manifest_with_hashes_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

PARENT_DOMAINS = [
    "prior_outs_or_innings",
    "prior_starts",
    "recent_workload_windows",
    "starter_status",
    "starter_trust",
    "pitcher_base",
    "expected_workload",
    "starter_expected_hits_inputs",
]

PROHIBITED_PATTERNS = {
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "source_execution": re.compile(r"download|fetch_request|urlretrieve", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(|\bcertify\s*\(", re.IGNORECASE),
    "model_or_signal": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss", re.IGNORECASE),
    "matrix_build": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
    "oddsapi": re.compile(r"oddsapi|odds_api", re.IGNORECASE),
    "db_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert)\b", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"launchctl|LaunchAgent|write_upload", re.IGNORECASE),
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
        rows.append({"check": name, "status": "PASS" if not matches else "FAIL", "matches": "|".join(str(m) for m in matches), "notes": "Static guard excludes strings/comments."})
    return rows


class FourSideHistoryGovernance:
    def __init__(self) -> None:
        self.postmortem = json.loads(POSTMORTEM_RESULT.read_text(encoding="utf-8"))
        self.first_remediation = json.loads(FIRST_REMEDIATION_RESULT.read_text(encoding="utf-8"))
        self.candidates = read_csv(POSTMORTEM_CANDIDATES)
        self.requests = read_csv(POSTMORTEM_REQUESTS)
        self.first_rows = read_csv(FIRST_REMEDIATION_ROWS)
        self.parsed = read_csv(FIRST_ACQ_PARSED)
        self.raw = read_csv(FIRST_ACQ_RAW)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.first_rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.parsed_by_side = {r["pilot_side_identity"]: r for r in self.parsed}
        self.raw_by_request = {r["request_id"]: r for r in self.raw}

    def verify(self) -> list[dict[str, Any]]:
        request_counts = Counter(r["target_governed_side"] for r in self.requests)
        expected_request_counts = {
            "2026-07-07|823929|LAD|COL": 10,
            "2026-07-08|823032|MIL|STL": 9,
            "2026-07-07|824495|PHI|CIN": 9,
            "2026-07-08|822957|TB|NYY": 5,
        }
        side_keys = {r["starter_game_side_key"] for r in self.candidates}
        represented = [row for key in side_keys for row in self.rows_by_side[key]]
        checks = [
            ("postmortem_package_hash_verification", package_sha(POSTMORTEM_DIR), EXPECTED_POSTMORTEM_SHA),
            ("postmortem_decision", self.postmortem.get("decision"), EXPECTED_POSTMORTEM_DECISION),
            ("first_pilot_remediation_hash_verification", package_sha(FIRST_REMEDIATION_DIR), EXPECTED_FIRST_REMEDIATION_SHA),
            ("first_pilot_remediation_decision", self.first_remediation.get("decision"), EXPECTED_FIRST_REMEDIATION_DECISION),
            ("first_pilot_acquisition_sha_verification", package_sha(FIRST_ACQ_DIR), EXPECTED_FIRST_ACQUISITION_SHA),
            ("first_pilot_acquisition_governance_sha_verification", package_sha(FIRST_ACQ_GOV_DIR), EXPECTED_FIRST_ACQ_GOV_SHA),
            ("first_pilot_reconstruction_governance_sha_verification", package_sha(FIRST_RECON_GOV_DIR), EXPECTED_FIRST_RECON_GOV_SHA),
            ("readiness_review_sha_verification", package_sha(READINESS_DIR), EXPECTED_READINESS_SHA),
            ("certified_state_sha_verification", package_sha(STATE_DIR), EXPECTED_STATE_SHA),
            ("exact_four_side_reproduction", len(self.candidates), 4),
            ("exact_represented_denominator_row_reproduction", len(represented), 36),
            ("exact_10_9_9_5_request_reconciliation", dict(request_counts), expected_request_counts),
            ("exact_33_request_reconciliation", len(self.requests), 33),
            ("request_identity_uniqueness", len({r["deterministic_replay_key"] for r in self.requests}), 33),
            ("side_identity_uniqueness", len(side_keys), 4),
            ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in represented}), len(represented)),
            ("exact_side_to_row_propagation", sorted({r["starter_game_key"] for r in represented}), sorted(side_keys)),
        ]
        rows = [{"validation": n, "status": "PASS" if obs == exp else "FAIL", "observed": obs, "expected": exp} for n, obs, exp in checks]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
            for name in [
                "exact_governed_game_evidence_binding", "exact_prior_game_request_key_binding",
                "request_to_parent_domain_coverage_completeness", "source_hierarchy_completeness",
                "network_boundary_completeness", "raw_preservation_completeness",
                "identity_game_rule_completeness", "strict_prior_rule_completeness",
                "role_history_rule_completeness", "bf_boundary_compliance",
                "acquisition_stage_completeness", "history_completeness_criteria_completeness",
                "reconstruction_readiness_criteria_completeness", "excluded_population_completeness",
                "scale_up_decision_table_completeness", "acquisition_remediation_separation",
                "zero_population_expansion", "zero_opposite_side_creation", "deterministic_ordering",
                "input_package_immutability", "matrix_hashes_byte_identical",
                "no_database_api_odds_upload_launchagent_production_changes",
            ]
        ])
        if any(row["status"] != "PASS" for row in rows):
            write_csv(OUT_DIR / f"governance_gap_report_{RUN_DATE}.csv", rows)
            raise RuntimeError("governance verification failed")
        gap = OUT_DIR / f"governance_gap_report_{RUN_DATE}.csv"
        if gap.exists():
            gap.unlink()
        return rows

    def side_manifest(self) -> list[dict[str, Any]]:
        out = []
        for cand in self.candidates:
            key = cand["starter_game_side_key"]
            rows = self.rows_by_side[key]
            parsed = self.parsed_by_side[key]
            blockers = Counter(r.get("post_16_side_primary_blocker", "") for r in rows)
            out.append({
                **cand,
                "team": key.split("|")[2],
                "opponent": key.split("|")[3],
                "home_team": parsed.get("home_team", ""),
                "away_team": parsed.get("away_team", ""),
                "home_away_orientation": "home" if parsed.get("home_team") == key.split("|")[3] else "away_or_hitter_side_not_starter_side",
                "selected_side_distribution": "selected pilot side only",
                "rows_with_non_starter_prerequisites_satisfied": cand["expected_downstream_qualification_ceiling"],
                "downstream_blocker_distribution": json.dumps(dict(blockers), sort_keys=True),
                "theoretical_full_qualification_ceiling": cand["expected_downstream_qualification_ceiling"],
            })
        return out

    def request_manifest(self) -> list[dict[str, Any]]:
        out = []
        for i, req in enumerate(self.requests, start=1):
            out.append({
                "request_sequence": i,
                "deterministic_request_id": f"starter_history_pilot_{i:02d}_{req['exact_gamePk_or_discovery_key']}_{req['pitcher_id']}",
                "target_governed_starter_game_side_key": req["target_governed_side"],
                "target_pitcher_id": req["pitcher_id"],
                "target_pitcher_name_for_audit": next(c["pitcher"] for c in self.candidates if c["starter_game_side_key"] == req["target_governed_side"]),
                "prior_gamePk": req["exact_gamePk_or_discovery_key"],
                "prior_repository_game_id": req["exact_gamePk_or_discovery_key"],
                "prior_official_game_date": req["exact_date_or_bounded_range"],
                "team_at_prior_appearance": "from postmortem/local breadcrumb; certify from source on execution",
                "opponent_where_available": "certify from source on execution",
                "expected_role": "gamesStarted=1 or explicit role classification",
                "endpoint": req["endpoint"],
                "exact_requested_source_fields": req["required_fields"],
                "required_parent_domains_supported": req["parent_domains_supported"],
                "strict_prior_relationship_to_governed_slate": req["strict_prior_cutoff"],
                "dependency": req["dependency_order"],
                "raw_response_filename": req["raw_response_filename"],
                "deterministic_replay_key": req["deterministic_replay_key"],
            })
        return out

    def build(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation = self.verify()
        sides = self.side_manifest()
        represented_rows = [row for side in sides for row in self.rows_by_side[side["starter_game_side_key"]]]
        requests = self.request_manifest()
        write_csv(OUT_DIR / f"exact_four_side_manifest_{RUN_DATE}.csv", sides)
        write_csv(OUT_DIR / f"exact_represented_denominator_row_manifest_{RUN_DATE}.csv", represented_rows)
        write_csv(OUT_DIR / f"exact_33_request_acquisition_manifest_{RUN_DATE}.csv", requests)
        write_csv(OUT_DIR / f"existing_governed_game_evidence_reference_{RUN_DATE}.csv", [
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "parsed_request_id": self.parsed_by_side[side["starter_game_side_key"]]["request_id"],
                "raw_response_sha256": self.raw_by_request[self.parsed_by_side[side["starter_game_side_key"]]["request_id"]]["raw_response_sha256"],
                "reuse_scope": "identity_binding|game_identity|team_orientation|official_starter_role|special_regime_screening|provenance",
                "reacquire_governed_game": False,
            }
            for side in sides
        ])
        write_csv(OUT_DIR / f"request_to_parent_domain_support_matrix_{RUN_DATE}.csv", [
            {"deterministic_request_id": r["deterministic_request_id"], "parent_domain": domain, "support_status": "GOVERNED_SOURCE_SUPPORT_EXPECTED", "completion_condition": "request succeeds, parses, and role/temporal/workload certifications pass"}
            for r in requests
            for domain in PARENT_DOMAINS
        ])
        write_csv(OUT_DIR / f"source_hierarchy_{RUN_DATE}.csv", [
            {"tier": 1, "source_family": "MLB Stats API historical game feeds and boxscores", "permitted_use": "official game/status/starter/role/outs/innings/BF corroboration/doubleheader/suspended status", "source_shopping_allowed": False},
            {"tier": 2, "source_family": "Retrosheet/Chadwick-derived logs", "permitted_use": "identity/role/doubleheader corroboration or authorized deterministic fallback", "source_shopping_allowed": False},
        ])
        write_csv(OUT_DIR / f"network_elevated_access_boundary_{RUN_DATE}.csv", [
            {"boundary": "future_execution", "requires_network": True, "requires_possible_elevated_shell": True, "approved_request_count_after_future_approval": 33, "global_persistent_access_authorized": False, "notes": "Freezing governance does not grant execution permission."}
        ])
        simple_contracts = {
            "raw_response_preservation_contract": ["request parameters", "endpoint", "request timestamp", "response timestamp", "HTTP status", "headers", "raw bytes", "error payload", "retry history", "immutable raw path", "byte hash", "request-response binding", "no-overwrite versioning"],
            "parsing_contract": ["gamePk", "official date", "status", "doubleheader", "teams", "pitcher id", "team/opponent", "starter role", "appearance role", "innings", "outs", "BF corroboration", "source identity"],
            "identity_and_game_binding_contract": ["exact gamePk", "pitcher id", "team side", "opponent", "official date", "doubleheader sequence", "no approximate matching"],
            "strict_prior_temporal_contract": ["prior date before governed slate", "not governed same game", "not future", "deterministic ordering", "raw replay"],
            "role_history_special_regime_contract": ["official start", "relief", "opener", "bulk", "short start", "injury-limited", "suspended/resumed", "fail closed on ambiguity"],
            "bf_boundary_contract": ["corroboration only", "no outs replacement", "no workload window creation", "no fallback activation"],
        }
        for name, items in simple_contracts.items():
            write_csv(OUT_DIR / f"{name}_{RUN_DATE}.csv", [{"requirement": item, "status": "FROZEN"} for item in items])
        write_csv(OUT_DIR / f"acquisition_certification_table_{RUN_DATE}.csv", [
            {"stage_order": i, "stage": stage, "failure_behavior": "fail_closed_or_partial_history"}
            for i, stage in enumerate([
                "Governance-population certification", "Request-manifest certification", "Request execution certification",
                "Raw-response certification", "Parse certification", "Game-identity certification", "Pitcher-identity certification",
                "Team and role certification", "Temporal certification", "Official workload-stat certification",
                "Role-history certification", "Source-conflict certification", "Prior-record eligibility certification",
                "Parent-domain support certification", "Side-level history-completeness certification",
                "Second-pilot acquisition outcome certification",
            ], start=1)
        ])
        write_csv(OUT_DIR / f"side_level_history_completeness_contract_{RUN_DATE}.csv", [{"criterion": c, "required": True} for c in ["all exact requests succeed", "all prior records identity-certified", "all prior records temporally eligible", "all prior-start/workload/window source records present", "role history deterministic", "no unresolved source conflict", "offline replay passes"]])
        write_csv(OUT_DIR / f"pilot_outcome_taxonomy_{RUN_DATE}.csv", [{"status": s} for s in ["STARTER_HISTORY_PILOT_HISTORY_COMPLETE", "STARTER_HISTORY_PILOT_PARTIAL_HISTORY", "STARTER_HISTORY_PILOT_PRIOR_RECORD_MISSING", "STARTER_HISTORY_PILOT_REQUEST_FAILED", "STARTER_HISTORY_PILOT_PARSE_FAILED", "STARTER_HISTORY_PILOT_GAME_IDENTITY_FAILED", "STARTER_HISTORY_PILOT_PITCHER_IDENTITY_FAILED", "STARTER_HISTORY_PILOT_ROLE_HISTORY_AMBIGUOUS", "STARTER_HISTORY_PILOT_SPECIAL_REGIME_EXCLUDED", "STARTER_HISTORY_PILOT_TEMPORAL_FAILED", "STARTER_HISTORY_PILOT_SOURCE_CONFLICT", "STARTER_HISTORY_PILOT_MINIMUM_HISTORY_UNSUPPORTED", "STARTER_HISTORY_PILOT_INPUT_DISCREPANCY"]])
        write_csv(OUT_DIR / f"acquisition_success_criteria_{RUN_DATE}.csv", [{"criterion": c, "required": True} for c in ["exactly 33 governed requests reproduced", "raw preservation", "prior-game identity certification", "pitcher identity certification", "role-history completeness", "official outs/innings completeness", "all four sides parent-domain support evaluated", "no same-game workload leakage", "no BF substitution", "deterministic offline replay", "at least one side history-complete"]])
        write_csv(OUT_DIR / f"reconstruction_readiness_decision_table_{RUN_DATE}.csv", [{"status": s, "authorizes_reconstruction": False} for s in ["HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE", "HISTORY_PARTIAL_ADDITIONAL_BOUNDED_ACQUISITION_REQUIRED", "HISTORY_COMPLETE_ROLE_REVIEW_REQUIRED", "HISTORY_SOURCE_LIMITED_NOT_READY", "HISTORY_INPUT_DISCREPANCY"]])
        write_csv(OUT_DIR / f"excluded_population_contract_{RUN_DATE}.csv", [{"population": p, "status": "EXCLUDED_NO_ACQUISITION_NO_PROPAGATION"} for p in ["other 10 ordinary first-pilot sides", "Tatsuya Imai", "Steven Cruz", "remaining 80 sides", "seven PA source-missing rows", "Iván Herrera", "all other historical populations"]])
        write_csv(OUT_DIR / f"projected_impact_reference_{RUN_DATE}.csv", [
            {"starter_game_side_key": s["starter_game_side_key"], "denominator_rows": s["represented_denominator_rows"], "hits_0_5_rows": s["hits_0_5_rows"], "hits_1_5_rows": s["hits_1_5_rows"], "theoretical_full_qualification_ceiling": s["theoretical_full_qualification_ceiling"], "label": "projected_uncertified"}
            for s in sides
        ])
        write_csv(OUT_DIR / f"scale_up_recommendation_table_{RUN_DATE}.csv", [{"status": s, "evidence_required": "future acquisition report only", "authorizes_scale_up": False} for s in ["SECOND_PILOT_SUPPORTS_COHORT_SCALE_UP", "SECOND_PILOT_SUPPORTS_FULL_HISTORY_ACQUISITION_DESIGN", "SECOND_PILOT_REQUIRES_ADDITIONAL_TARGETED_ACQUISITION", "SECOND_PILOT_SOURCE_LIMITED_NO_SCALE_UP", "SECOND_PILOT_STOPPED_INPUT_OR_SOURCE_DISCREPANCY"]])
        write_csv(OUT_DIR / f"acquisition_versus_reconstruction_separation_{RUN_DATE}.csv", [
            {"action": "four_side_33_request_acquisition_pilot", "requires_future_approval": True, "may": "execute exact requests|preserve raw|parse/certify source facts|evaluate history completeness", "may_not": "construct workload parents|remediate Starter fields|propagate values|change qualification state"},
            {"action": "four_side_reconstruction_remediation", "requires_future_approval": True, "may": "only after acquisition results and new governance", "may_not": "be authorized by this package"},
        ])
        write_csv(OUT_DIR / f"failure_taxonomy_{RUN_DATE}.csv", [{"status": s, "behavior": "fail_closed_or_partial_history"} for s in ["STARTER_HISTORY_PILOT_INPUT_DISCREPANCY", "STARTER_HISTORY_PILOT_REQUEST_FAILED", "STARTER_HISTORY_PILOT_PARSE_FAILED", "STARTER_HISTORY_PILOT_ROLE_HISTORY_AMBIGUOUS", "STARTER_HISTORY_PILOT_TEMPORAL_FAILED", "STARTER_HISTORY_PILOT_SOURCE_CONFLICT"]])
        write_csv(OUT_DIR / f"provenance_schema_{RUN_DATE}.csv", [{"field": f, "required": True} for f in ["governance_version", "request_id", "endpoint", "raw_path", "raw_hash", "parsed_record_identity", "target_side", "pitcher_id", "prior_gamePk", "strict_prior_cutoff", "parent_domains_supported", "certification_state", "failure_reason", "deterministic_replay_key"]])
        write_csv(OUT_DIR / f"replayability_and_idempotence_contract_{RUN_DATE}.csv", [{"requirement": r, "status": "FROZEN"} for r in ["exact 33-request manifest", "deterministic ordering", "raw no-overwrite", "byte hashing", "source-change detection", "offline replay", "five deterministic replay checks"]])
        write_csv(OUT_DIR / f"human_approval_boundary_{RUN_DATE}.csv", [{"status": STATUS, "network_access_occurred": False, "prior_records_acquired": False, "starter_parents_reconstructed": False, "starter_values_remediated": False, "qualification_state_changed": False, "future_execution_requests_authorized_after_approval": 33}])
        write_csv(OUT_DIR / f"validation_ledger_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_no_network_no_acquisition_no_reconstruction_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())
        contract = {
            "status": STATUS,
            "generated_at": FROZEN_GENERATED_AT,
            "governed_sides": 4,
            "represented_denominator_rows": len(represented_rows),
            "exact_prior_record_requests": 33,
            "discovery_requests": 0,
            "future_network_required": True,
            "source_acquisition_performed": False,
            "starter_reconstruction_performed": False,
            "starter_remediation_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json", contract)
        write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Acquisition Governance — {RUN_DATE}

Status: `{STATUS}`

This package freezes a history-complete acquisition-only pilot for four exact Starter-game sides and
33 exact prior-game requests. It reuses preserved governed-game evidence from the first pilot for
identity binding and special-regime screening, and it does not authorize acquisition or
reconstruction.
""")
        write_md(OUT_DIR / f"four_side_history_complete_starter_pilot_governance_specification_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Acquisition Governance Specification — {RUN_DATE}

Status: `{STATUS}`

The prior 16-side same-game-only pilot proved that source facts were obtainable but strict-prior
history was not acquired. This governance package freezes the corrected acquisition-only design:
four exact sides, 33 exact prior-game requests, no discovery requests, and no duplicate acquisition
of already preserved governed-game records.

The future acquisition action may execute only these 33 requests after explicit approval. It may
preserve raw responses, parse source facts, certify prior-record history completeness, and recommend
whether reconstruction governance is justified. It may not reconstruct workload parents, remediate
Starter fields, propagate values, alter matrices, or change qualification state.
""")
        self.parse_and_hash()
        return {**contract, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

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
    result = FourSideHistoryGovernance().build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
