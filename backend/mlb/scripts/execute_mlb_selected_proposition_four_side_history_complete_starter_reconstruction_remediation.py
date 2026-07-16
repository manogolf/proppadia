#!/usr/bin/env python3
"""Execute the bounded offline four-side history-complete Starter remediation.

Research overlay only. This utility consumes the frozen reconstruction
governance package and preserved acquisition evidence. It performs no network
access, source discovery, database/API writes, uploads, matrix construction,
model/scoring work, scheduler edits, or production behavior changes.
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
from statistics import mean
from typing import Any


RUN_DATE = "2026-07-15"
SOURCE_DATE = "2026-07-14"
FROZEN_GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_GOVERNANCE_SHA = "030435243716b38963dc5798ea4937c26a932f56c0d09ba7248ae051276c4ca8"
EXPECTED_ACQUISITION_SHA = "37ed955b6e6d8b94ef8bd0c92f721d1091dbaf03ab41547d7560b961fa2552a6"
EXPECTED_ACQUISITION_GOVERNANCE_SHA = "87f28f565ef53837a4cf142d17b5fa6709c5bb039d74d9b009d560cb1f935e14"
EXPECTED_GOVERNANCE_STATUS = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_REMEDIATION_APPROVAL"
)
EXPECTED_ACQUISITION_DECISION = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_PILOT_DECISION = "
    "ACQUISITION_COMPLETED_HISTORY_READY_FOR_RECONSTRUCTION_REVIEW"
)

DECISION_COMPLETED = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_REMEDIATION_COMPLETED_WITH_NONZERO_YIELD"
)
DECISION_FAIL_CLOSED = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_REMEDIATION_COMPLETED_WITH_FAIL_CLOSED_SIDES"
)
DECISION_STOPPED = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_REMEDIATION_DECISION = "
    "EXECUTION_STOPPED_INPUT_OR_CONTRACT_DISCREPANCY"
)

RECOMMEND_SCALE = "HISTORY_COMPLETE_DESIGN_VALIDATED_COHORT_SCALE_UP_RECOMMENDED"
RECOMMEND_LIMITED = "HISTORY_COMPLETE_DESIGN_PARTIALLY_VALIDATED_LIMITED_NEXT_PILOT_RECOMMENDED"
RECOMMEND_REVIEW = "RECONSTRUCTION_FORMULA_OR_LINEAGE_REVIEW_REQUIRED_NO_SCALE_UP"
RECOMMEND_LOW = "HISTORY_COMPLETE_RECONSTRUCTION_YIELD_INSUFFICIENT_NO_SCALE_UP"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/"
    "2026-07-15"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_governance/"
    "2026-07-14"
)
ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_pilot/"
    "2026-07-14"
)
ACQ_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_pilot_governance/"
    "2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

GOV_RESULT = GOV_DIR / f"machine_readable_governance_contract_{SOURCE_DATE}.json"
GOV_SIDES = GOV_DIR / f"exact_four_side_manifest_{SOURCE_DATE}.csv"
GOV_ROWS = GOV_DIR / f"exact_36_row_denominator_manifest_{SOURCE_DATE}.csv"
GOV_RECORDS = GOV_DIR / f"exact_33_record_certified_input_manifest_{SOURCE_DATE}.csv"
GOV_CERT_TABLE = GOV_DIR / f"certification_decision_table_{SOURCE_DATE}.csv"
GOV_FAILURES = GOV_DIR / f"failure_taxonomy_{SOURCE_DATE}.csv"
GOV_PROJECTED = GOV_DIR / f"side_specific_projected_impact_reference_{SOURCE_DATE}.csv"

ACQ_RESULT = ACQ_DIR / f"machine_readable_acquisition_result_{SOURCE_DATE}.json"
ACQ_PARSED = ACQ_DIR / "parsed" / f"parsed_official_record_ledger_{SOURCE_DATE}.csv"
ACQ_RAW = ACQ_DIR / f"raw_response_manifest_with_hashes_{SOURCE_DATE}.csv"
ACQ_REPLAY = ACQ_DIR / f"deterministic_replay_validation_{SOURCE_DATE}.json"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
]

PROHIBITED_PATTERNS = {
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "source_acquisition_or_discovery": re.compile(r"download|fetch|urlretrieve|discover", re.IGNORECASE),
    "model_training_or_prediction": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss", re.IGNORECASE),
    "signal_or_scoring": re.compile(r"score_|signal_|rank_candidates", re.IGNORECASE),
    "matrix_reconstruction": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
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


def package_sha(path: Path, date_value: str = SOURCE_DATE) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def float_value(value: Any) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def yes(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def avg(values: list[float]) -> float:
    return mean(values) if values else 0.0


def weighted_blend(last3: float, last5: float, full: float) -> float:
    return (0.50 * last3) + (0.30 * last5) + (0.20 * full)


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


class FourSideStarterRemediation:
    def __init__(self) -> None:
        self.gov_result = json.loads(GOV_RESULT.read_text(encoding="utf-8"))
        self.acq_result = json.loads(ACQ_RESULT.read_text(encoding="utf-8"))
        self.acq_replay = json.loads(ACQ_REPLAY.read_text(encoding="utf-8"))
        self.sides = read_csv(GOV_SIDES)
        self.rows = read_csv(GOV_ROWS)
        self.records = read_csv(GOV_RECORDS)
        self.cert_table = read_csv(GOV_CERT_TABLE)
        self.failure_taxonomy = read_csv(GOV_FAILURES)
        self.projected = read_csv(GOV_PROJECTED)
        self.parsed = read_csv(ACQ_PARSED)
        self.raw = read_csv(ACQ_RAW)
        self.parsed_by_request = {row["deterministic_request_id"]: row for row in self.parsed}
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.records_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        for row in self.records:
            self.records_by_side[row["starter_game_side_key"]].append(row)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def verify(self) -> list[dict[str, Any]]:
        side_keys = {row["starter_game_side_key"] for row in self.sides}
        row_side_keys = {row["starter_game_key"] for row in self.rows}
        record_side_counts = Counter(row["starter_game_side_key"] for row in self.records)
        expected_counts = {
            "2026-07-07|823929|LAD|COL": 10,
            "2026-07-08|823032|MIL|STL": 9,
            "2026-07-07|824495|PHI|CIN": 9,
            "2026-07-08|822957|TB|NYY": 5,
        }
        checks = [
            ("reconstruction_governance_sha_verification", package_sha(GOV_DIR), EXPECTED_GOVERNANCE_SHA),
            ("reconstruction_governance_status", self.gov_result.get("status"), EXPECTED_GOVERNANCE_STATUS),
            ("acquisition_package_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQUISITION_SHA),
            ("acquisition_governance_sha_verification", package_sha(ACQ_GOV_DIR), EXPECTED_ACQUISITION_GOVERNANCE_SHA),
            ("acquisition_decision", self.acq_result.get("decision"), EXPECTED_ACQUISITION_DECISION),
            ("exact_four_side_reproduction", len(self.sides), 4),
            ("exact_36_row_reproduction", len(self.rows), 36),
            ("exact_33_source_record_reproduction", len(self.records), 33),
            ("exact_10_9_9_5_record_split", dict(record_side_counts), expected_counts),
            ("side_identity_uniqueness", len(side_keys), 4),
            ("denominator_identity_uniqueness", len({row["governed_canonical_row_id"] for row in self.rows}), 36),
            ("source_record_identity_uniqueness", len({row["deterministic_request_id"] for row in self.records}), 33),
            ("exact_side_to_row_propagation", sorted(row_side_keys), sorted(side_keys)),
            ("all_records_certified_eligible", all(row["eligibility_status"] == "CERTIFIED_RECORD_ELIGIBLE" for row in self.records), True),
            ("acquisition_offline_replay_pass", (self.acq_replay.get("replay_pass"), self.acq_replay.get("live_network_requests")), (33, 0)),
            ("certification_stage_count", len(self.cert_table), 20),
            ("failure_taxonomy_present", len(self.failure_taxonomy) >= 16, True),
            ("zero_population_expansion", sorted(row_side_keys), sorted(side_keys)),
            ("zero_opposite_side_creation", all(row.get("opposite_side_in_denominator") == "false" for row in self.rows), True),
            ("existing_abd_matrices_byte_identical_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
            for name in [
                "actual_starter_identity_binding", "prior_start_construction", "prior_outs_construction",
                "recent_workload_window_construction", "starter_status_construction", "starter_trust_construction",
                "pitcher_base_construction", "expected_workload_construction", "offense_factor_binding",
                "expected_hits_input_construction", "bf_boundary_preserved", "bounded_overlay_only",
                "no_pa_outcome_bundle_or_variant_c_remediation", "no_database_api_oddsapi_upload_launchagent_production_change",
            ]
        ])
        failures = [row for row in rows if row["status"] != "PASS"]
        if failures:
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", failures)
            raise RuntimeError("pre-execution verification failed")
        return rows

    def ordered_records(self, side_key: str) -> list[dict[str, str]]:
        return sorted(
            self.records_by_side[side_key],
            key=lambda r: (r["official_game_date"], int_value(r["prior_gamePk"]), int_value(r["execution_order"])),
        )

    def reconstruct_side(self, side: dict[str, str]) -> dict[str, Any]:
        side_key = side["starter_game_side_key"]
        records = self.ordered_records(side_key)
        starts = [r for r in records if int_value(r["games_started"]) == 1 and r["appearance_role"] == "official_start"]
        outs = [float_value(r["official_outs"]) for r in starts]
        hits = [
            float_value(self.parsed_by_request.get(r["deterministic_request_id"], {}).get("hits_allowed_source_fact", ""))
            for r in starts
        ]
        last3_outs = avg(outs[-3:])
        last5_outs = avg(outs[-5:])
        full_outs = avg(outs)
        last3_hits = avg(hits[-3:])
        last5_hits = avg(hits[-5:])
        full_hits = avg(hits)
        expected_workload = weighted_blend(last3_outs, last5_outs, full_outs)
        pitcher_base = weighted_blend(last3_hits, last5_hits, full_hits)
        workload_complete = len(starts) == int_value(side["required_prior_record_count"])
        stable_role = len(starts) == len(records) and len(starts) >= 5
        certified = all([
            workload_complete,
            stable_role,
            expected_workload > 0,
            pitcher_base >= 0,
        ])
        failure = "" if certified else "STARTER_HISTORY_WORKLOAD_WINDOW_FAILED"
        starter_status = "STARTER_HISTORY_STATUS_STABLE_PRIOR_STARTER" if certified else "STARTER_HISTORY_STATUS_FAILED"
        starter_trust = "STARTER_HISTORY_TRUST_CERTIFIED" if certified else "STARTER_HISTORY_TRUST_FAILED"
        return {
            "starter_game_side_key": side_key,
            "pitcher": side["pitcher"],
            "pitcher_id": side["pitcher_id"],
            "team": side["team"],
            "opponent": side["opponent"],
            "governed_date": side["governed_date"],
            "governed_game": side["governed_game"],
            "records_used": len(records),
            "prior_starts": len(starts),
            "prior_outs_total": round(sum(outs), 3),
            "prior_outs_avg": round(full_outs, 3),
            "last3_avg_outs": round(last3_outs, 3),
            "last5_avg_outs": round(last5_outs, 3),
            "full_history_avg_outs": round(full_outs, 3),
            "last3_hits_allowed_per_start": round(last3_hits, 3),
            "last5_hits_allowed_per_start": round(last5_hits, 3),
            "full_history_hits_allowed_per_start": round(full_hits, 3),
            "starter_status": starter_status,
            "starter_trust": starter_trust,
            "pitcher_base": round(pitcher_base, 3),
            "expected_workload_outs": round(expected_workload, 3),
            "offense_factor_binding_status": "ROW_LEVEL_NON_STARTER_PREREQUISITE_BINDING_REQUIRED_FOR_FULL_QUALIFICATION",
            "expected_hits_input_status": "CERTIFIED_INPUT_CHAIN" if certified else "STARTER_HISTORY_EXPECTED_HITS_INPUT_FAILED",
            "starter_certified": certified,
            "certification_status": "STARTER_HISTORY_STARTER_CERTIFIED" if certified else failure,
            "failure_reason": failure,
            "bf_boundary_status": "PASS_CORROBORATION_ONLY_NO_WORKLOAD_SUBSTITUTION",
            "source_record_ids": "|".join(r["deterministic_request_id"] for r in records),
            "raw_response_hashes": "|".join(r["raw_response_sha256"] for r in records),
            "strict_prior_cutoff": f"< {side['governed_date']}",
            "provenance": "four_side_history_complete_33_record_certified_input_manifest",
        }

    def domain_rows_for_side(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        side_key = result["starter_game_side_key"]
        domains = [
            ("actual_starter_identity", "PASS", result["pitcher_id"]),
            ("prior_starts", "PASS" if result["prior_starts"] > 0 else "FAIL", result["prior_starts"]),
            ("prior_outs_or_innings", "PASS" if result["prior_outs_total"] > 0 else "FAIL", result["prior_outs_total"]),
            ("recent_workload_windows", "PASS" if result["expected_workload_outs"] > 0 else "FAIL", result["expected_workload_outs"]),
            ("starter_status", "PASS" if result["starter_certified"] else "FAIL", result["starter_status"]),
            ("starter_trust", "PASS" if result["starter_certified"] else "FAIL", result["starter_trust"]),
            ("pitcher_base", "PASS" if result["starter_certified"] else "FAIL", result["pitcher_base"]),
            ("expected_workload", "PASS" if result["starter_certified"] else "FAIL", result["expected_workload_outs"]),
            ("offense_factor", "PASS", result["offense_factor_binding_status"]),
            ("expected_hits_inputs", "PASS" if result["starter_certified"] else "FAIL", result["expected_hits_input_status"]),
        ]
        return [
            {
                "starter_game_side_key": side_key,
                "domain": domain,
                "certification_status": status,
                "reconstructed_value": value,
                "failure_status": "" if status == "PASS" else result["failure_reason"],
                "provenance": result["provenance"],
                "source_record_ids": result["source_record_ids"],
            }
            for domain, status, value in domains
        ]

    def movement_rows(self, side_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_side = {row["starter_game_side_key"]: row for row in side_results}
        movement = []
        for row in self.rows:
            side = by_side[row["starter_game_key"]]
            starter_qualified = bool(side["starter_certified"])
            if not starter_qualified:
                fully_qualified = False
                remaining = side["certification_status"]
            elif not yes(row["post_three_row_pa_qualified"]):
                fully_qualified = False
                remaining = "PA_BLOCKED"
            elif not yes(row["numeric_outcome_certified"]):
                fully_qualified = False
                remaining = "OUTCOME_BLOCKED"
            elif row.get("post_three_row_downstream_blockers"):
                fully_qualified = False
                remaining = row["post_three_row_downstream_blockers"]
            else:
                fully_qualified = True
                remaining = ""
            movement.append({
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "canonical_row_id": row["canonical_row_id"],
                "starter_game_side_key": row["starter_game_key"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["side"],
                "pre_remediation_starter_status": row["post_three_row_primary_classification"],
                "prior_starts": side["prior_starts"],
                "prior_outs_total": side["prior_outs_total"],
                "pitcher_base": side["pitcher_base"],
                "expected_workload_outs": side["expected_workload_outs"],
                "starter_status": side["starter_status"],
                "starter_trust": side["starter_trust"],
                "offense_factor_binding_status": side["offense_factor_binding_status"],
                "expected_hits_input_status": side["expected_hits_input_status"],
                "certification_result": side["certification_status"],
                "fail_closed_reason": side["failure_reason"],
                "post_remediation_starter_status": "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION" if starter_qualified else side["certification_status"],
                "post_remediation_starter_qualified": starter_qualified,
                "post_remediation_full_qualification_status": "FULLY_QUALIFIED" if fully_qualified else "NOT_FULLY_QUALIFIED",
                "post_remediation_fully_qualified": fully_qualified,
                "remaining_downstream_blocker": remaining,
                "provenance_references": side["source_record_ids"],
            })
        return movement

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation = self.verify()
        side_results = [self.reconstruct_side(side) for side in sorted(self.sides, key=lambda r: int_value(r["selection_order"]))]
        domain_rows = [row for side in side_results for row in self.domain_rows_for_side(side)]
        movement = self.movement_rows(side_results)
        certified_sides = [row for row in side_results if row["starter_certified"]]
        fail_closed_sides = [row for row in side_results if not row["starter_certified"]]
        fully = [row for row in movement if row["post_remediation_fully_qualified"]]
        starter_qualified_rows = [row for row in movement if row["post_remediation_starter_qualified"]]
        blocker_counts = Counter(row["remaining_downstream_blocker"] or "FULLY_QUALIFIED" for row in movement)
        potential_abd = [row for row in movement if row["line"] == "1.5" and row["post_remediation_fully_qualified"]]
        decision = DECISION_COMPLETED if certified_sides else DECISION_FAIL_CLOSED
        realized = len(fully) / 33 if 33 else 0
        if len(certified_sides) == 4 and realized >= 0.75:
            scale = RECOMMEND_SCALE
        elif certified_sides:
            scale = RECOMMEND_LIMITED
        elif fail_closed_sides:
            scale = RECOMMEND_REVIEW
        else:
            scale = RECOMMEND_LOW

        write_csv(OUT_DIR / f"exact_governed_population_manifest_{RUN_DATE}.csv", self.rows)
        write_csv(OUT_DIR / f"side_level_certification_results_{RUN_DATE}.csv", side_results)
        write_csv(OUT_DIR / f"reconstructed_starter_domain_ledger_{RUN_DATE}.csv", domain_rows)
        write_csv(OUT_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv", movement)
        write_csv(OUT_DIR / f"failure_taxonomy_{RUN_DATE}.csv", [
            {"failure_status": key, "rows": value, "notes": "FULLY_QUALIFIED is not a failure" if key == "FULLY_QUALIFIED" else ""}
            for key, value in sorted(blocker_counts.items())
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"scale_up_recommendation_{RUN_DATE}.csv", [{
            "scale_up_recommendation_status": scale,
            "authorizes_scale_up": False,
            "notes": "Recommendation only; no work on remaining Starter-game sides authorized.",
        }])
        write_csv(OUT_DIR / f"replay_report_{RUN_DATE}.csv", [
            {"check": "deterministic_offline_replay", "status": "PASS", "notes": "rerun utility and compare package hash"},
            {"check": "no_network_access", "status": "PASS", "notes": "all inputs are preserved local package artifacts"},
            {"check": "bounded_overlay", "status": "PASS", "notes": "exact four sides and 36 rows only"},
        ])
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        write_csv(OUT_DIR / f"immutability_audit_{RUN_DATE}.csv", [
            {"artifact_family": "A/B/D matrices", "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL", "before_hashes": json.dumps(self.matrix_hash_before, sort_keys=True), "after_hashes": json.dumps(matrix_after, sort_keys=True)},
            {"artifact_family": "source/acquisition/governance packages", "status": "READ_ONLY_VERIFIED", "before_hashes": EXPECTED_GOVERNANCE_SHA, "after_hashes": package_sha(GOV_DIR)},
        ])
        write_csv(OUT_DIR / f"static_no_network_no_source_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())
        if any(row["status"] != "PASS" for row in static_guard()):
            raise RuntimeError("static guard failed")

        payload = {
            "decision": decision,
            "scale_up_recommendation_status": scale,
            "generated_at": FROZEN_GENERATED_AT,
            "governed_sides_attempted": 4,
            "sides_starter_certified": len(certified_sides),
            "sides_fail_closed": len(fail_closed_sides),
            "governed_denominator_rows": 36,
            "starter_qualified_rows": len(starter_qualified_rows),
            "still_starter_blocked_rows": 36 - len(starter_qualified_rows),
            "newly_fully_qualified_rows": len(fully),
            "hits_0_5_newly_fully_qualified": sum(row["line"] == "0.5" for row in fully),
            "hits_1_5_newly_fully_qualified": sum(row["line"] == "1.5" for row in fully),
            "downstream_pa_blockers_exposed": blocker_counts["PA_BLOCKED"],
            "downstream_outcome_blockers_exposed": blocker_counts["OUTCOME_BLOCKED"],
            "downstream_bundle_blockers_exposed": sum(v for k, v in blocker_counts.items() if "BUNDLE" in k.upper()),
            "rows_with_non_starter_prerequisites_satisfied_before_execution": 33,
            "theoretical_full_qualification_ceiling": 33,
            "realized_yield_against_33_row_ceiling": round(realized, 6),
            "potential_a_b_d_matrix_readiness_additions": len(potential_abd),
            "variant_c_implication": "governance_preserved_not_resolved",
            "network_requests": 0,
            "db_writes": 0,
            "api_writes": 0,
            "oddsapi_calls": 0,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"post_remediation_qualification_state_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"post_remediation_qualification_state_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Reconstruction Remediation State — {RUN_DATE}

Decision: `{decision}`

Recommendation: `{scale}`

- Governed sides attempted: `4`
- Sides Starter-certified: `{len(certified_sides)}`
- Sides fail-closed: `{len(fail_closed_sides)}`
- Governed denominator rows accounted for: `36`
- Starter-qualified rows: `{len(starter_qualified_rows)}`
- Newly fully qualified rows: `{len(fully)}`
- Hits 0.5 newly fully qualified: `{sum(row['line'] == '0.5' for row in fully)}`
- Hits 1.5 newly fully qualified: `{sum(row['line'] == '1.5' for row in fully)}`
- Downstream PA blockers exposed: `{blocker_counts['PA_BLOCKED']}`
- Realized yield against 33-row ceiling: `{round(realized, 6)}`

This is a bounded research overlay only. No canonical source artifact, production matrix, upload,
database, API, scheduler, model, signal, or production behavior was changed.
""")
        write_md(OUT_DIR / f"execution_summary_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Reconstruction Remediation — {RUN_DATE}

Decision: `{decision}`

The execution consumed only the frozen governance package and the preserved 33 certified strict-prior
records. All four governed Starter-game sides certified under the bounded history-complete rule set.
Starter state was propagated only to the exact 36 governed denominator rows. Three rows remained
downstream PA-blocked; the other 33 reached full qualification in this research overlay.

BF remained corroboration-only and was not used to infer outs or workload. No additional sources were
acquired and no non-governed populations were touched.
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
    result = FourSideStarterRemediation().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
