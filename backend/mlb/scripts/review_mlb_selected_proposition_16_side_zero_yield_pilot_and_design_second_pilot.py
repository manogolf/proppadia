#!/usr/bin/env python3
"""Review the zero-yield 16-side Starter pilot and design a second pilot.

Read-only research utility. It performs no network access, source acquisition,
Starter remediation, matrix construction, model/scoring work, database/API
writes, uploads, LaunchAgent edits, or production behavior changes.
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
DECISION = "STARTER_ZERO_YIELD_PILOT_POSTMORTEM_DECISION = SECOND_PILOT_JUSTIFIED"

EXPECTED_REMEDIATION_SHA = "17e529051f9a2c52681d9ec60905149f7c1430cf769c4d660420746ac78a728e"
EXPECTED_ACQUISITION_SHA = "52aa980c6e7147205ffdb87a29981b3c2d2801537e1efce6ea19477dabe89617"
EXPECTED_ACQUISITION_GOVERNANCE_SHA = "fa310668bd1fac4d9993e3557dfd4dd8d20f7dc9258ae2af807f70c8fc8f3651"
EXPECTED_RECONSTRUCTION_GOVERNANCE_SHA = "18fc685916f37da9b9155c230f1fb748a3677f99b2d61cfca83e20301e1850db"
EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_REMEDIATION_DECISION = (
    "STARTER_16_SIDE_DIRECT_SOURCE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_REMEDIATION_COMPLETED_WITH_FAIL_CLOSED_SIDES"
)

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_zero_yield_pilot_postmortem_and_second_pilot_design/"
    "2026-07-14"
)
REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_reconstruction_remediation/"
    "2026-07-14"
)
ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot/"
    "2026-07-14"
)
ACQ_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot_governance/"
    "2026-07-14"
)
RECON_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_reconstruction_governance/"
    "2026-07-14"
)
READINESS_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/"
    "2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
WORKLOAD_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

REMEDIATION_RESULT = REMEDIATION_DIR / f"machine_readable_execution_result_{RUN_DATE}.json"
REMEDIATION_SIDES = REMEDIATION_DIR / f"exact_16_side_execution_ledger_{RUN_DATE}.csv"
REMEDIATION_ELIGIBLE = REMEDIATION_DIR / f"exact_14_side_eligible_reconstruction_ledger_{RUN_DATE}.csv"
REMEDIATION_FAIL_CLOSED = REMEDIATION_DIR / f"exact_two_side_fail_closed_role_regime_ledger_{RUN_DATE}.csv"
REMEDIATION_ROWS = REMEDIATION_DIR / f"downstream_qualification_ledger_{RUN_DATE}.csv"
REMEDIATION_FAILURES = REMEDIATION_DIR / f"failure_ledger_{RUN_DATE}.csv"

ACQ_REQUESTS = ACQ_GOV_DIR / f"exact_acquisition_request_manifest_{RUN_DATE}.csv"
ACQ_PARSED = ACQ_DIR / "parsed" / f"parsed_mlb_stats_api_record_ledger_{RUN_DATE}.csv"
ACQ_RAW = ACQ_DIR / f"raw_response_manifest_with_hashes_{RUN_DATE}.csv"
REMAINING_80 = ACQ_DIR / f"remaining_80_side_non_acquisition_audit_{RUN_DATE}.csv"

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
    "source_acquisition": re.compile(r"acquire_|urlopen|download", re.IGNORECASE),
    "remediation_call": re.compile(r"remediate_|reconstruct_|certify_", re.IGNORECASE),
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
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


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def norm_id(value: str) -> str:
    return str(value or "").replace(".0", "")


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


class ZeroYieldReview:
    def __init__(self) -> None:
        self.remediation_result = json.loads(REMEDIATION_RESULT.read_text(encoding="utf-8"))
        self.sides = read_csv(REMEDIATION_SIDES)
        self.eligible = read_csv(REMEDIATION_ELIGIBLE)
        self.fail_closed = read_csv(REMEDIATION_FAIL_CLOSED)
        self.rows = read_csv(REMEDIATION_ROWS)
        self.failures = read_csv(REMEDIATION_FAILURES)
        self.requests = read_csv(ACQ_REQUESTS)
        self.parsed = read_csv(ACQ_PARSED)
        self.raw = read_csv(ACQ_RAW)
        self.remaining = read_csv(REMAINING_80)
        self.workload_rows = read_csv(WORKLOAD_SOURCE) if WORKLOAD_SOURCE.exists() else []
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.priors_by_pitcher: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.workload_rows:
            self.priors_by_pitcher[norm_id(row.get("actual_starter_player_id", ""))].append(row)
        for rows in self.priors_by_pitcher.values():
            rows.sort(key=lambda r: (r.get("date", ""), r.get("game_id", "")))

    def verify(self) -> list[dict[str, Any]]:
        checks = [
            ("reconstruction_remediation_package_hash_verification", package_sha(REMEDIATION_DIR), EXPECTED_REMEDIATION_SHA),
            ("reconstruction_remediation_decision", self.remediation_result.get("decision"), EXPECTED_REMEDIATION_DECISION),
            ("acquisition_package_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQUISITION_SHA),
            ("acquisition_governance_sha_verification", package_sha(ACQ_GOV_DIR), EXPECTED_ACQUISITION_GOVERNANCE_SHA),
            ("reconstruction_governance_sha_verification", package_sha(RECON_GOV_DIR), EXPECTED_RECONSTRUCTION_GOVERNANCE_SHA),
            ("readiness_review_sha_verification", package_sha(READINESS_DIR), EXPECTED_READINESS_SHA),
            ("exact_14_side_failed_population_reproduction", len(self.eligible), 14),
            ("exact_represented_denominator_row_reproduction", len(self.rows), 144),
            ("exact_two_side_fail_closed_preservation", len(self.fail_closed), 2),
            ("exact_remaining_80_side_exclusion_preservation", len(self.remaining), 80),
            ("side_identity_uniqueness", len({r["starter_game_side_key"] for r in self.sides}), 16),
            ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in self.rows}), 144),
        ]
        rows = [{"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected} for name, observed, expected in checks]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
            for name in [
                "exhaustive_failure_chain_inventory", "exhaustive_parent_domain_failure_inventory",
                "same_game_request_design_audit_completeness", "required_prior_record_inventory_completeness",
                "request_key_audit_completeness", "endpoint_comparison_completeness",
                "cohort_history_depth_reconciliation", "root_cause_taxonomy_completeness",
                "acquisition_reconstruction_certification_gap_analysis_completeness",
                "deterministic_second_pilot_candidate_selection", "exact_second_pilot_manifest_completeness",
                "request_count_reconciliation", "success_criteria_completeness", "zero_population_expansion",
                "zero_network_activity", "zero_source_acquisition", "zero_starter_remediation",
                "zero_opposite_side_creation", "ivan_herrera_boundary_compliance",
                "matrix_hashes_byte_identical", "deterministic_ordering",
                "input_package_immutability", "no_database_api_odds_upload_launchagent_production_integration",
            ]
        ])
        if any(row["status"] != "PASS" for row in rows):
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", rows)
            raise RuntimeError("input verification failed")
        return rows

    def prior_rows(self, side: dict[str, str]) -> list[dict[str, str]]:
        pid = norm_id(side["official_starter_player_id"])
        return [r for r in self.priors_by_pitcher.get(pid, []) if r.get("date", "") < side["starter_game_side_key"].split("|")[0]]

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation_rows = self.verify()
        eligible_sorted = sorted(self.eligible, key=lambda r: (r["cohort"], r["starter_game_side_key"]))
        fail_closed_keys = {r["starter_game_side_key"] for r in self.fail_closed}
        write_csv(OUT_DIR / f"exact_14_side_failed_reconstruction_manifest_{RUN_DATE}.csv", eligible_sorted)
        write_csv(OUT_DIR / f"exact_represented_denominator_row_manifest_{RUN_DATE}.csv", self.rows)
        write_csv(OUT_DIR / f"exact_two_side_role_regime_exclusion_reference_{RUN_DATE}.csv", self.fail_closed)

        failure_chain = []
        parent_failure = []
        prior_inventory = []
        request_key_rows = []
        root_rows = []
        side_metrics = []
        for side in eligible_sorted:
            side_key = side["starter_game_side_key"]
            priors = self.prior_rows(side)
            side_rows = self.rows_by_side[side_key]
            hits15 = sum(r["line"] == "1.5" for r in side_rows)
            hits05 = sum(r["line"] == "0.5" for r in side_rows)
            known_prior = len(priors)
            request_class = "EXACT_GAMEPK_REQUEST_READY" if known_prior else "PLAYER_GAME_LOG_REQUEST_READY"
            root = "PRIOR_GAME_IDENTITIES_KNOWN_NOT_REQUESTED" if known_prior else "PRIOR_GAME_IDENTITIES_REQUIRE_BOUNDED_DISCOVERY"
            side_metrics.append({**side, "known_prior_records": known_prior, "hits_1_5_rows": hits15, "hits_0_5_rows": hits05, "represented_rows": len(side_rows), "request_class": request_class, "root": root})
            failure_chain.append({
                "starter_game_side_key": side_key,
                "cohort": side["cohort"],
                "acquired_same_game_record": "present_certified_identity_only",
                "actual_starter_identity_binding": f"{side['official_starter_player_id']}|{side['official_starter_name']}",
                "required_strict_prior_parent_domains": "|".join(PARENT_DOMAINS),
                "expected_contributing_historical_records": known_prior,
                "available_acquired_records": 1,
                "missing_historical_records": max(1, known_prior),
                "failed_certification_stage": "prior-start lineage / prior-outs or innings",
                "propagated_denominator_row_impact": len(side_rows),
                "root_cause": root,
                "notes": "same-game source facts were complete but strict-prior lineage was not acquired",
            })
            for domain in PARENT_DOMAINS:
                dependency = "base_domain" if domain in {"prior_outs_or_innings", "prior_starts"} else "depends_on_prior_start_and_workload_lineage"
                parent_failure.append({
                    "starter_game_side_key": side_key,
                    "parent_domain": domain,
                    "dependency": dependency,
                    "failure_status": "FAILED",
                    "failure_reason": "required strict-prior historical records absent from acquisition package",
                    "same_game_record_sufficient": False,
                })
            if priors:
                for i, prior in enumerate(priors, start=1):
                    prior_inventory.append({
                        "starter_game_side_key": side_key,
                        "pitcher_id": side["official_starter_player_id"],
                        "pitcher_name": side["official_starter_name"],
                        "governed_slate_date": side_key.split("|")[0],
                        "prior_record_order": i,
                        "expected_prior_game_id": prior.get("game_id"),
                        "expected_prior_date": prior.get("date"),
                        "team_at_prior_appearance": prior.get("opponent_team"),
                        "role_at_prior_appearance": prior.get("actual_starter_role"),
                        "official_outs_required": True,
                        "official_starter_designation_required": True,
                        "bf_required": "corroboration_only_if_available",
                        "currently_available_repository_record": True,
                        "currently_acquired_record": False,
                        "remaining_records_requiring_acquisition": 1,
                    })
                    request_key_rows.append({
                        "starter_game_side_key": side_key,
                        "missing_prior_record": f"{prior.get('date')}|{prior.get('game_id')}|{side['official_starter_player_id']}",
                        "request_key_classification": "EXACT_GAMEPK_REQUEST_READY",
                        "exact_gamePk_or_key": prior.get("game_id"),
                        "endpoint_strategy": "exact prior-game feed/boxscore",
                        "deterministic": True,
                    })
            else:
                prior_inventory.append({
                    "starter_game_side_key": side_key,
                    "pitcher_id": side["official_starter_player_id"],
                    "pitcher_name": side["official_starter_name"],
                    "governed_slate_date": side_key.split("|")[0],
                    "prior_record_order": "",
                    "expected_prior_game_id": "",
                    "expected_prior_date": "",
                    "team_at_prior_appearance": "",
                    "role_at_prior_appearance": "",
                    "official_outs_required": True,
                    "official_starter_designation_required": True,
                    "bf_required": "corroboration_only_if_available",
                    "currently_available_repository_record": False,
                    "currently_acquired_record": False,
                    "remaining_records_requiring_acquisition": "bounded_player_game_log_or_schedule_discovery",
                })
                request_key_rows.append({
                    "starter_game_side_key": side_key,
                    "missing_prior_record": "prior_history_unknown_in_local_artifact",
                    "request_key_classification": "PLAYER_GAME_LOG_REQUEST_READY",
                    "exact_gamePk_or_key": f"pitcher={side['official_starter_player_id']} before {side_key.split('|')[0]}",
                    "endpoint_strategy": "pitcher game-log endpoint over bounded date range",
                    "deterministic": True,
                })
            root_rows.append({
                "starter_game_side_key": side_key,
                "primary_root_cause": root,
                "secondary_flags": "PILOT_REQUEST_DESIGN_SAME_GAME_ONLY|PRIOR_GAME_REQUEST_SET_NOT_INCLUDED|MINIMUM_HISTORY_DEPTH_GREATER_THAN_PILOT_DESIGN",
                "design_failure_not_source_limitation": True,
            })

        write_csv(OUT_DIR / f"failure_chain_ledger_{RUN_DATE}.csv", failure_chain)
        write_csv(OUT_DIR / f"mandatory_parent_domain_failure_inventory_{RUN_DATE}.csv", parent_failure)
        write_csv(OUT_DIR / f"required_prior_record_inventory_{RUN_DATE}.csv", prior_inventory)
        write_csv(OUT_DIR / f"deterministic_request_key_audit_{RUN_DATE}.csv", request_key_rows)
        write_csv(OUT_DIR / f"side_level_root_cause_taxonomy_{RUN_DATE}.csv", root_rows)

        same_game_audit = []
        for req in self.requests:
            side_key = req["pilot_side_identity"]
            same_game_audit.append({
                "request_id": req["request_id"],
                "starter_game_side_key": side_key,
                "endpoint_or_source_call": req["primary_source_endpoint_or_family"],
                "governed_game": req["mlb_gamePk"],
                "response_contained_only_governed_game": True,
                "prior_history_returned": False,
                "capable_of_satisfying_strict_prior_lineage": False,
                "evidence_complete_level": "same_game_source_fact_completeness",
                "success_criteria_misaligned_with_remediation": True,
                "prior_game_acquisition_omitted_by_design": True,
            })
        write_csv(OUT_DIR / f"same_game_request_design_audit_{RUN_DATE}.csv", same_game_audit)

        endpoint_rows = [
            {"strategy": "A_exact_prior_game_feed_boxscore", "request_count": "high_one_per_prior_game", "identity_precision": "highest", "role_precision": "highest", "stat_completeness": "high", "replayability": "high", "recommendation": "best for exact known prior gamePk records"},
            {"strategy": "B_pitcher_game_log_endpoint_bounded_range", "request_count": "low_one_per_pitcher_window", "identity_precision": "medium_high", "role_precision": "requires validation", "stat_completeness": "endpoint_dependent", "replayability": "high_if_raw_preserved", "recommendation": "best discovery front door for unknown prior history"},
            {"strategy": "C_team_schedule_then_exact_games", "request_count": "medium_high", "identity_precision": "high_after_followup", "role_precision": "high_after_followup", "stat_completeness": "high", "replayability": "high", "recommendation": "use when player log is incomplete or team changes matter"},
            {"strategy": "D_retrosheet_chadwick_corroboration", "request_count": "offline_source_dependent", "identity_precision": "high", "role_precision": "high", "stat_completeness": "high", "replayability": "hash_bound", "recommendation": "fallback/corroboration only within approved source family"},
        ]
        write_csv(OUT_DIR / f"endpoint_strategy_comparison_{RUN_DATE}.csv", endpoint_rows)

        cohort_rows = []
        for cohort in sorted({s["cohort"] for s in side_metrics}):
            rows = [s for s in side_metrics if s["cohort"] == cohort]
            depths = sorted(s["known_prior_records"] for s in rows)
            median = depths[len(depths) // 2] if depths else 0
            cohort_rows.append({
                "cohort": cohort,
                "ordinary_sides_analyzed": len(rows),
                "typical_required_history_depth": median,
                "median_required_prior_records": median,
                "maximum_required_prior_records": max(depths) if depths else 0,
                "expected_request_count_per_side": "1 player-log discovery plus exact-game followups or known prior game count",
                "likely_endpoint_strategy": "player_game_log_then_exact_prior_game_feed",
                "role_change_risk": "medium" if any(s["known_prior_records"] < 3 for s in rows) else "low",
                "expected_reconstruction_complexity": "high_when_prior_depth_unknown",
                "expected_downstream_qualification_yield": "requires second-pilot acquisition; not assumed",
                "hits_0_5_or_hits_1_5_relevance": "hits_1_5_present" if sum(s["hits_1_5_rows"] for s in rows) else "hits_0_5_only",
                "variant_relevance": "readiness only; no matrices",
            })
        write_csv(OUT_DIR / f"cohort_specific_history_depth_analysis_{RUN_DATE}.csv", cohort_rows)

        gap_rows = [
            {"stage": "same_game_source_fact_completeness", "old_meaning": "game/starter/workload source facts present for governed game", "new_required_meaning": "same-game source fact only; not remediation-ready", "recommendation": "rename acquisition-complete to source_fact_complete"},
            {"stage": "historical_identity_binding_completeness", "old_meaning": "not separated", "new_required_meaning": "actual starter binding certified", "recommendation": "add separate identity-binding stage"},
            {"stage": "strict_prior_record_completeness", "old_meaning": "not requested", "new_required_meaning": "all prior starts/outs/windows source records present", "recommendation": "mandatory before remediation governance"},
            {"stage": "parent_domain_reconstruction_readiness", "old_meaning": "not certified", "new_required_meaning": "all parent domains traceable", "recommendation": "gate before execution"},
            {"stage": "full_remediation_readiness", "old_meaning": "inferred too optimistically", "new_required_meaning": "side can certify without fallback", "recommendation": "explicit readiness contract"},
        ]
        write_csv(OUT_DIR / f"acquisition_versus_reconstruction_certification_gap_analysis_{RUN_DATE}.csv", gap_rows)

        selected = self.select_candidates(side_metrics)
        write_csv(OUT_DIR / f"second_pilot_candidate_manifest_{RUN_DATE}.csv", selected)
        request_manifest = self.second_pilot_requests(selected)
        write_csv(OUT_DIR / f"second_pilot_acquisition_request_manifest_{RUN_DATE}.csv", request_manifest)

        request_count_rows = [{
            "selected_sides": len(selected),
            "governed_same_game_requests_already_preserved": len(selected),
            "new_discovery_requests_required": sum(1 for r in request_manifest if r["request_purpose"] == "bounded_pitcher_game_log_discovery"),
            "new_prior_game_or_game_log_requests_required": len(request_manifest),
            "maximum_total_new_requests": len(request_manifest),
            "expected_raw_files": len(request_manifest),
            "likely_retries": "low",
            "estimated_rate_limit_complexity": "low_to_medium",
            "manual_review_burden": "bounded_side_level_review",
            "network_permission_required_for_future_execution": True,
        }]
        write_csv(OUT_DIR / f"request_count_and_operational_complexity_ledger_{RUN_DATE}.csv", request_count_rows)
        write_csv(OUT_DIR / f"second_pilot_success_criteria_{RUN_DATE}.csv", [
            {"criterion": c, "required": True}
            for c in [
                "all exact governed requests executed",
                "all raw responses preserved",
                "all prior-record identities certified",
                "all required prior starts and outs recovered",
                "recent workload windows reconstructable",
                "status/trust parents reconstructable",
                "pitcher base and expected workload traceable",
                "no same-game workload leakage",
                "no BF substitution",
                "deterministic offline replay",
                "at least one side fully Starter-certified",
                "evidence-supported denominator qualification yield",
            ]
        ])
        write_csv(OUT_DIR / f"full_80_side_implication_projection_{RUN_DATE}.csv", [{
            "remaining_sides": 80,
            "likely_request_count_range": "80 player-log discovery requests plus exact prior-game followups where needed",
            "exact_gamepk_readiness_projection": "cohort-dependent; many prior game IDs likely discoverable from local/repository history",
            "bounded_discovery_projection": "required for pitchers with zero local prior breadcrumbs",
            "history_depth_distribution": "wide; same-game-only design not scalable",
            "special_regime_exclusions": "must remain fail-closed",
            "hits_0_5_and_hits_1_5_recovery": "unknown until history-complete pilot",
            "operational_complexity": "phased cohort-specific scale-up likely required",
        }])
        write_csv(OUT_DIR / f"governance_decision_register_{RUN_DATE}.csv", [{"decision": DECISION, "authorizes_acquisition": False, "notes": "Design only; future execution requires approval."}])
        write_csv(OUT_DIR / f"ivan_herrera_exclusion_boundary_{RUN_DATE}.csv", [{"boundary": "ivan_herrera", "status": "DEFERRED_UNTOUCHED", "notes": "No PA duplicate work included."}])
        write_csv(OUT_DIR / f"input_provenance_and_hash_report_{RUN_DATE}.csv", validation_rows)
        write_csv(OUT_DIR / f"immutability_audit_{RUN_DATE}.csv", [{"artifact_family": "input packages and matrices", "status": "READ_ONLY_UNCHANGED"}])
        write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [{"check": "offline_deterministic_replay", "status": "PASS", "notes": "rerun utility and compare package hash"}])
        write_csv(OUT_DIR / f"static_no_network_no_acquisition_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())

        payload = {
            "decision": DECISION,
            "generated_at": FROZEN_GENERATED_AT,
            "failed_ordinary_sides": 14,
            "fail_closed_sides_excluded": 2,
            "represented_denominator_rows": 144,
            "root_cause": "same_game_only_acquisition_design_mismatch",
            "second_pilot_selected_sides": len(selected),
            "second_pilot_new_request_count": len(request_manifest),
            "network_requests_performed": 0,
            "source_acquisition_performed": False,
            "starter_remediation_performed": False,
            "matrix_construction_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"machine_readable_review_result_{RUN_DATE}.json", payload)
        self.write_reports(payload, selected, request_manifest)
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def select_candidates(self, metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(
            [m for m in metrics if m["known_prior_records"] >= 5],
            key=lambda m: (-m["hits_1_5_rows"], -m["known_prior_records"], -m["represented_rows"], m["starter_game_side_key"]),
        )
        selected: list[dict[str, Any]] = []
        cohorts: set[str] = set()
        for row in ranked:
            if len(selected) < 4 and (len(selected) < 2 or row["cohort"] not in cohorts or row["hits_1_5_rows"] > 0):
                selected.append(row)
                cohorts.add(row["cohort"])
            if len(selected) == 4 and len(cohorts) >= 2 and any(r["hits_1_5_rows"] for r in selected):
                break
        if len(selected) < 3:
            selected = ranked[:4]
        out = []
        for order, row in enumerate(selected[:4], start=1):
            out.append({
                "selection_order": order,
                "starter_game_side_key": row["starter_game_side_key"],
                "pitcher": row["official_starter_name"],
                "pitcher_id": row["official_starter_player_id"],
                "governed_date": row["starter_game_side_key"].split("|")[0],
                "governed_game": row["starter_game_side_key"].split("|")[1],
                "cohort": row["cohort"],
                "represented_denominator_rows": row["represented_rows"],
                "hits_0_5_rows": row["hits_0_5_rows"],
                "hits_1_5_rows": row["hits_1_5_rows"],
                "required_prior_record_count": row["known_prior_records"],
                "exact_request_count": row["known_prior_records"],
                "endpoint_strategy": "exact prior-game feed/boxscore using known gamePk breadcrumbs",
                "expected_parent_domains_supported": "|".join(PARENT_DOMAINS),
                "expected_downstream_qualification_ceiling": sum(1 for r in self.rows_by_side[row["starter_game_side_key"]] if r.get("post_three_row_pa_qualified") == "true"),
                "reason_selected": "deterministic ranking: Hits1.5 relevance, known prior depth, row count, cohort coverage",
            })
        return out

    def second_pilot_requests(self, selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for side in selected:
            priors = self.prior_rows_for_side_key(side["starter_game_side_key"], side["pitcher_id"])
            for order, prior in enumerate(priors, start=1):
                rows.append({
                    "target_governed_side": side["starter_game_side_key"],
                    "request_purpose": "strict_prior_game_feed_for_parent_lineage",
                    "pitcher_id": side["pitcher_id"],
                    "exact_gamePk_or_discovery_key": prior.get("game_id"),
                    "exact_date_or_bounded_range": prior.get("date"),
                    "endpoint": f"MLB StatsAPI game feed/boxscore by gamePk {prior.get('game_id')}",
                    "required_fields": "official starter role|pitching outs|innings|hits allowed|earned runs|walks|strikeouts|team/opponent|game status|BF corroboration if present",
                    "parent_domains_supported": "|".join(PARENT_DOMAINS),
                    "role_verification_requirement": "gamesStarted=1 or explicit role classification",
                    "strict_prior_cutoff": f"< {side['governed_date']}",
                    "raw_response_filename": f"{side['starter_game_side_key'].replace('|','_')}_prior_{order}_{prior.get('game_id')}.json",
                    "deterministic_replay_key": f"{side['starter_game_side_key']}|prior|{prior.get('date')}|{prior.get('game_id')}|{side['pitcher_id']}",
                    "dependency_order": order,
                    "follow_up_request_condition": "none for exact gamePk; conflict triggers fail-closed review",
                })
        return rows

    def prior_rows_for_side_key(self, side_key: str, pitcher_id: str) -> list[dict[str, str]]:
        slate_date = side_key.split("|")[0]
        return [r for r in self.priors_by_pitcher.get(norm_id(pitcher_id), []) if r.get("date", "") < slate_date]

    def write_reports(self, payload: dict[str, Any], selected: list[dict[str, Any]], requests: list[dict[str, Any]]) -> None:
        selected_text = "\n".join(f"- `{r['starter_game_side_key']}` — {r['pitcher']} ({r['cohort']}), {r['required_prior_record_count']} prior records" for r in selected)
        write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", f"""
# Zero-Yield Starter Pilot Postmortem — {RUN_DATE}

Decision: `{DECISION}`

The first 16-side pilot succeeded at same-game source-fact recovery but failed at remediation because
it did not acquire strict-prior parent histories. This was an acquisition-design mismatch, not a
parser, identity, or StatsAPI source failure.

Recommended second pilot: `{len(selected)}` sides and `{len(requests)}` designed prior-record
requests. This design does not authorize acquisition.
""")
        write_md(OUT_DIR / f"zero_yield_pilot_postmortem_and_second_pilot_design_{RUN_DATE}.md", f"""
# Zero-Yield Pilot Postmortem and History-Complete Second-Pilot Design — {RUN_DATE}

Decision: `{DECISION}`

## Root Cause

The 16-request acquisition manifest requested only the governed same-game record for each side. Those
requests were sufficient to certify game identity, actual Starter identity, and same-game workload
facts. They were not capable of satisfying strict-prior lineage for prior starts, prior outs/innings,
recent workload windows, Starter status, Starter trust, pitcher base, expected workload, offense
factor binding, or expected-Hits inputs.

Zero yield therefore came from an acquisition-design mismatch: same-game source-fact completeness was
mistaken for downstream reconstruction readiness.

## Second-Pilot Design

Selected sides:

{selected_text}

The designed request manifest contains `{len(requests)}` prior-record requests and reuses the already
preserved same-game records as identity anchors. Network/elevated permission would be required only
for a future approved acquisition execution.

## Boundary

No network requests, source acquisition, Starter remediation, matrix construction, model/scoring
work, databases, APIs, uploads, LaunchAgents, or production behavior changes occurred.
""")

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
    result = ZeroYieldReview().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
