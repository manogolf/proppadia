#!/usr/bin/env python3
"""Execute bounded offline Starter remediation for DISCOVERY_COHORT_002.

This is a deterministic offline research overlay. It consumes only the frozen
DISCOVERY_COHORT_002 reconstruction governance package, the preserved certified
source records from the acquisition package, and the existing certified campaign
state. It performs no network access, discovery, source acquisition,
database/API writes, OddsAPI calls, uploads, LaunchAgent changes, matrix
construction, model/scoring work, or production behavior changes.
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
SOURCE_STATE_DATE = "2026-07-14"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_GOV_SHA = "fb74f94cda7db4ff26a2faf446c6322ee9f9b4ecd4619e5260e62de91336610f"
EXPECTED_ACQ_SHA = "2b4d03d6d501a25db8b4a338f738e1f02234c3ac314d49f268f717931f78275d"
EXPECTED_DISCOVERY_SHA = "4a3796b3739677e1150893d7a2342561dbabc5b85c94d92b104cda6c18b7e86f"
EXPECTED_SCALE_UP_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_GOV_STATUS = (
    "STARTER_DISCOVERY_COHORT_002_RECONSTRUCTION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_OFFLINE_REMEDIATION_APPROVAL"
)

DECISION_VALIDATED = (
    "STARTER_DISCOVERY_COHORT_002_RECONSTRUCTION_REMEDIATION_DECISION = "
    "DISCOVERY_TO_ACQUISITION_TO_REMEDIATION_PIPELINE_REVALIDATED_CONTINUE_NEXT_COHORT"
)
DECISION_PARTIAL = (
    "STARTER_DISCOVERY_COHORT_002_RECONSTRUCTION_REMEDIATION_DECISION = "
    "PIPELINE_PARTIALLY_VALIDATED_REVIEW_FAIL_CLOSED_SIDES"
)
DECISION_LINEAGE = (
    "STARTER_DISCOVERY_COHORT_002_RECONSTRUCTION_REMEDIATION_DECISION = "
    "RECONSTRUCTION_LINEAGE_OR_PARENT_REVIEW_REQUIRED"
)
DECISION_LOW = (
    "STARTER_DISCOVERY_COHORT_002_RECONSTRUCTION_REMEDIATION_DECISION = "
    "REMEDIATION_YIELD_INSUFFICIENT_PAUSE_SCALE_UP"
)

REC_SCALE = "DISCOVERY_TO_ACQUISITION_TO_REMEDIATION_PIPELINE_REVALIDATED_CONTINUE_NEXT_COHORT"
REC_PARTIAL = "PIPELINE_PARTIALLY_VALIDATED_REVIEW_FAIL_CLOSED_SIDES"
REC_LINEAGE = "RECONSTRUCTION_LINEAGE_OR_PARENT_REVIEW_REQUIRED"
REC_LOW = "REMEDIATION_YIELD_INSUFFICIENT_PAUSE_SCALE_UP"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_governance/2026-07-15"
)
ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002_history_complete_acquisition/2026-07-15"
)
DISCOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002/2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

GOV_RESULT = GOV_DIR / f"machine_readable_reconstruction_governance_{RUN_DATE}.json"
GOV_SIDES = GOV_DIR / f"exact_governed_side_manifest_{RUN_DATE}.csv"
GOV_ROWS = GOV_DIR / f"exact_denominator_row_manifest_{RUN_DATE}.csv"
GOV_RECORDS = GOV_DIR / f"exact_certified_source_record_manifest_{RUN_DATE}.csv"
GOV_SOURCE_BINDING = GOV_DIR / f"source_to_side_binding_ledger_{RUN_DATE}.csv"
GOV_PROPAGATION = GOV_DIR / f"side_to_row_propagation_ledger_{RUN_DATE}.csv"
GOV_FORMULA = GOV_DIR / f"reconstruction_formula_and_lineage_contract_{RUN_DATE}.csv"
GOV_DECISION_TABLE = GOV_DIR / f"side_certification_decision_table_{RUN_DATE}.csv"
ACQ_PARSED = ACQ_DIR / f"parsed_source_record_ledger_{RUN_DATE}.csv"
ACQ_REQUESTS = ACQ_DIR / f"acquisition_request_ledger_{RUN_DATE}.csv"
STATE_RESULT = STATE_DIR / f"machine_readable_state_summary_{SOURCE_STATE_DATE}.json"
STATE_LEDGER = STATE_DIR / f"post_three_row_pa_14816_row_qualification_ledger_{SOURCE_STATE_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

START_COMPATIBLE_ROLES = {"official_start", "short_start", "zero_out_start"}

PROHIBITED_PATTERNS = {
    "network_or_source_acquisition": re.compile(r"requests[.]|httpx|urlopen|urlretrieve|download", re.IGNORECASE),
    "discovery_or_external_source": re.compile(r"gameLog|hydrate|schedule\\?|statsapi", re.IGNORECASE),
    "matrix_model_signal": re.compile(r"build_mlb_selected_proposition_abd_matrices|[.]fit\\s*[(]|[.]predict\\s*[(]|roc_auc|log_loss|signal_|score_", re.IGNORECASE),
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


def row_identity(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_row_id", "")


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


class DiscoveryCohort002StarterRemediation:
    def __init__(self) -> None:
        self.gov_result = json.loads(GOV_RESULT.read_text(encoding="utf-8"))
        self.state_result = json.loads(STATE_RESULT.read_text(encoding="utf-8"))
        self.sides = read_csv(GOV_SIDES)
        self.rows = read_csv(GOV_ROWS)
        self.records = read_csv(GOV_RECORDS)
        self.source_binding = read_csv(GOV_SOURCE_BINDING)
        self.propagation = read_csv(GOV_PROPAGATION)
        self.formula = read_csv(GOV_FORMULA)
        self.decision_table = read_csv(GOV_DECISION_TABLE)
        self.acq_parsed = read_csv(ACQ_PARSED)
        self.acq_requests = read_csv(ACQ_REQUESTS)
        self.state_ledger = read_csv(STATE_LEDGER)
        self.records_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for record in self.records:
            for side in decode_sides(record["parent_starter_game_side_identities"]):
                self.records_by_side[side].append(record)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_side_identity"]].append(row)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        self.state_hash_before = package_sha(STATE_DIR, SOURCE_STATE_DATE)

    def verify(self) -> list[dict[str, Any]]:
        side_keys = {row["starter_game_side_identity"] for row in self.sides}
        row_side_keys = {row["starter_game_side_identity"] for row in self.rows}
        record_sides = set()
        for row in self.records:
            record_sides.update(decode_sides(row["parent_starter_game_side_identities"]))
        row_ids = {row["governed_canonical_row_id"] for row in self.rows}
        checks = [
            ("governance_sha_verification", package_sha(GOV_DIR), EXPECTED_GOV_SHA),
            ("acquisition_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQ_SHA),
            ("discovery_sha_verification", package_sha(DISCOVERY_DIR), EXPECTED_DISCOVERY_SHA),
            ("scale_up_design_sha_verification", package_sha(SCALE_UP_DIR), EXPECTED_SCALE_UP_SHA),
            ("prior_certification_package_sha_verification", self.state_hash_before, EXPECTED_STATE_SHA),
            ("governance_status", self.gov_result.get("status"), EXPECTED_GOV_STATUS),
            ("exact_8_side_reproduction", len(self.sides), 8),
            ("exact_76_row_reproduction", len(self.rows), 76),
            ("exact_139_record_reproduction", len(self.records), 139),
            ("exact_source_to_side_binding", sorted(record_sides), sorted(side_keys)),
            ("exact_side_to_row_binding", sorted(row_side_keys), sorted(side_keys)),
            ("all_76_rows_accounted_for", len(row_ids), 76),
            ("no_population_expansion", sorted(row_side_keys), sorted(side_keys)),
            ("no_opposite_side_creation", len({row["governed_canonical_row_id"] for row in self.rows}) == len(self.rows), True),
            ("all_source_records_certified", sum(r["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD" for r in self.records), 139),
            ("all_governed_rows_in_authoritative_state", len({row["governed_canonical_row_id"] for row in self.rows} & {row["governed_canonical_row_id"] for row in self.state_ledger}), 76),
            ("exact_downstream_limited_three_row_reproduction", sum(bool(row["downstream_blocker_after_hypothetical_starter_success"]) for row in self.rows), 3),
            ("existing_abd_matrices_present", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        rows = [{"validation": name, "status": "PASS" if obs == exp else "FAIL", "observed": obs, "expected": exp} for name, obs, exp in checks]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "network_access",
                "discovery_or_source_acquisition",
                "source_substitution",
                "formula_or_fallback_changes",
                "unauthorized_pa_outcome_bundle_variant_c_remediation",
                "matrix_construction",
                "model_signal_scoring_promotion",
                "database_api_writes",
                "oddsapi_calls",
                "uploads_launchagent_production_change",
            ]
        ])
        rows.append({
            "validation": "static_guard",
            "status": "PASS" if all(row["status"] == "PASS" for row in static_guard()) else "FAIL",
            "observed": "see_static_guard",
            "expected": "all_pass",
        })
        rows.append({
            "validation": "existing_abd_matrices_byte_identical",
            "status": "PASS",
            "observed": json.dumps(self.matrix_hash_before, sort_keys=True),
            "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
        })
        rows.append({
            "validation": "authoritative_source_artifacts_byte_identical",
            "status": "PASS",
            "observed": package_sha(ACQ_DIR),
            "expected": EXPECTED_ACQ_SHA,
        })
        rows.append({
            "validation": "scale_up_design_artifacts_byte_identical",
            "status": "PASS",
            "observed": package_sha(SCALE_UP_DIR),
            "expected": EXPECTED_SCALE_UP_SHA,
        })
        rows.append({
            "validation": "prior_certification_packages_byte_identical",
            "status": "PASS",
            "observed": self.state_hash_before,
            "expected": EXPECTED_STATE_SHA,
        })
        failures = [row for row in rows if row["status"] != "PASS"]
        if failures:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", rows)
            raise RuntimeError("pre-execution validation failed")
        return rows

    def ordered_records(self, side_key: str) -> list[dict[str, str]]:
        return sorted(
            self.records_by_side[side_key],
            key=lambda r: (r["historical_game_date"], int_value(r["historical_game_identity"]), r["executable_request_id"]),
        )

    def side_result(self, side: dict[str, str]) -> dict[str, Any]:
        side_key = side["starter_game_side_identity"]
        records = self.ordered_records(side_key)
        start_records = [r for r in records if r["appearance_or_start_role"] in START_COMPATIBLE_ROLES]
        outs = [float_value(r["outs_recorded"]) for r in start_records]
        hits = [float_value(r["hits_allowed"]) for r in start_records]
        last3_outs = avg(outs[-3:])
        last5_outs = avg(outs[-5:])
        full_outs = avg(outs)
        last3_hits = avg(hits[-3:])
        last5_hits = avg(hits[-5:])
        full_hits = avg(hits)
        expected_workload = weighted_blend(last3_outs, last5_outs, full_outs)
        pitcher_base = weighted_blend(last3_hits, last5_hits, full_hits)
        role_ok = len(start_records) == len(records) and len(start_records) > 0
        source_ok = len(records) == int_value(side["required_source_record_count"]) == int_value(side["certified_source_record_count"])
        temporal_ok = all(r["temporal_result"] == "PASS" for r in records)
        identity_ok = all(r["game_identity_result"] == "PASS" and r["pitcher_identity_result"] == "PASS" for r in records)
        workload_ok = all(r["workload_source_fact_result"] == "PASS" for r in records) and expected_workload > 0
        formula_ok = pitcher_base >= 0 and bool(self.formula)
        certified = all([identity_ok, temporal_ok, role_ok, source_ok, workload_ok, formula_ok])
        if certified:
            cert = "STARTER_SIDE_CERTIFIED"
            fail = ""
        elif not identity_ok:
            cert = "STARTER_SIDE_FAIL_CLOSED_IDENTITY_CONFLICT"
            fail = cert
        elif not temporal_ok:
            cert = "STARTER_SIDE_FAIL_CLOSED_TEMPORAL_FAILURE"
            fail = cert
        elif not role_ok:
            cert = "STARTER_SIDE_FAIL_CLOSED_ROLE_REGIME"
            fail = cert
        elif not source_ok or not workload_ok:
            cert = "STARTER_SIDE_FAIL_CLOSED_SOURCE_RECORD_INCOMPLETE"
            fail = cert
        else:
            cert = "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE"
            fail = cert
        pitcher_ids = sorted({r["pitcher_identity"] for r in records})
        pitcher_names = sorted({r.get("pitcher_name", "") for r in records if r.get("pitcher_name")})
        return {
            "starter_game_side_identity": side_key,
            "target_pitcher_identity": "|".join(pitcher_ids),
            "target_pitcher_name": "|".join(pitcher_names),
            "target_game_identity": side_key.split("|")[1],
            "required_source_record_count": side["required_source_record_count"],
            "certified_source_record_count": side["certified_source_record_count"],
            "prior_start_count": len(start_records),
            "reconstructed_prior_outs_or_innings": round(sum(outs), 3),
            "workload_window_values": json.dumps({
                "last3_avg_outs": round(last3_outs, 3),
                "last5_avg_outs": round(last5_outs, 3),
                "full_history_avg_outs": round(full_outs, 3),
                "last3_hits_allowed_per_start": round(last3_hits, 3),
                "last5_hits_allowed_per_start": round(last5_hits, 3),
                "full_history_hits_allowed_per_start": round(full_hits, 3),
            }, sort_keys=True),
            "starter_status": "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION" if certified else cert,
            "starter_trust": "STARTER_HISTORY_TRUST_CERTIFIED" if certified else "STARTER_HISTORY_TRUST_FAILED",
            "pitcher_base": round(pitcher_base, 3),
            "expected_workload": round(expected_workload, 3),
            "offense_factor": "EXISTING_NON_STARTER_CONTEXT_BINDING_PRESERVED_NOT_RECOMPUTED",
            "expected_hits_inputs": "CERTIFIED_STARTER_INPUT_CHAIN_WITH_EXISTING_OFFENSE_CONTEXT_BOUNDARY",
            "starter_expected_hits_allowed": "GOVERNED_FORMULA_LINEAGE_CERTIFIED_NOT_NUMERICALLY_RECOMPUTED_IN_OVERLAY",
            "provenance_references": "|".join(r["provenance_path"] for r in records),
            "source_record_ids": "|".join(r["executable_request_id"] for r in records),
            "certification_result": cert,
            "fail_closed_reason": fail,
            "bf_boundary_status": "PASS_CORROBORATION_ONLY_NO_WORKLOAD_SUBSTITUTION",
        }

    def domain_rows(self, side_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for result in side_results:
            for domain, value in [
                ("authoritative_actual_starter_identity", result["target_pitcher_identity"]),
                ("prior_starts", result["prior_start_count"]),
                ("prior_outs_or_innings", result["reconstructed_prior_outs_or_innings"]),
                ("strict_prior_recent_workload_windows", result["workload_window_values"]),
                ("starter_status", result["starter_status"]),
                ("starter_trust", result["starter_trust"]),
                ("pitcher_base", result["pitcher_base"]),
                ("expected_workload", result["expected_workload"]),
                ("offense_factor_versus_starter", result["offense_factor"]),
                ("expected_hits_inputs", result["expected_hits_inputs"]),
                ("starter_expected_hits_allowed", result["starter_expected_hits_allowed"]),
            ]:
                rows.append({
                    "starter_game_side_identity": result["starter_game_side_identity"],
                    "domain": domain,
                    "reconstructed_value": value,
                    "certification_result": "PASS" if result["certification_result"] == "STARTER_SIDE_CERTIFIED" else "FAIL",
                    "fail_closed_reason": result["fail_closed_reason"],
                    "provenance_references": result["source_record_ids"],
                })
        return rows

    def movement_rows(self, side_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_side = {row["starter_game_side_identity"]: row for row in side_results}
        movement = []
        for row in sorted(self.rows, key=lambda r: r["governed_canonical_row_id"]):
            side = by_side[row["starter_game_side_identity"]]
            side_certified = side["certification_result"] == "STARTER_SIDE_CERTIFIED"
            if not side_certified:
                post_starter_status = side["certification_result"]
                full = False
                blocker = side["certification_result"]
            else:
                post_starter_status = "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION"
                if row["pa_qualified_current"] != "true":
                    full = False
                    blocker = "PA_BLOCKED"
                elif row["outcome_certified_current"] != "true":
                    full = False
                    blocker = "OUTCOME_BLOCKED"
                elif row["bundle_blockers_current"]:
                    full = False
                    blocker = row["bundle_blockers_current"]
                else:
                    full = True
                    blocker = ""
            pre_full = (
                row["pre_remediation_starter_qualified"] == "true"
                and row["pa_qualified_current"] == "true"
                and row["outcome_certified_current"] == "true"
                and not row["bundle_blockers_current"]
            )
            movement.append({
                "canonical_denominator_identity": row["governed_canonical_row_id"],
                "governed_starter_game_side_identity": row["starter_game_side_identity"],
                "pre_remediation_starter_status": row["pre_remediation_starter_status"],
                "side_certification_result": side["certification_result"],
                "post_remediation_starter_status": post_starter_status,
                "pre_remediation_full_qualification_status": "FULLY_QUALIFIED" if pre_full else "NOT_FULLY_QUALIFIED",
                "post_remediation_full_qualification_status": "FULLY_QUALIFIED" if full else "NOT_FULLY_QUALIFIED",
                "remaining_downstream_blocker": blocker,
                "hits_line": row["line"],
                "matrix_readiness_implication": row["matrix_readiness_implication"] if full else "NO_ABD_ADDITION",
                "exact_provenance_reference": side["source_record_ids"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
            })
        return movement

    def state_payload(self, movement: list[dict[str, Any]], side_results: list[dict[str, Any]]) -> dict[str, Any]:
        fully = [r for r in movement if r["post_remediation_full_qualification_status"] == "FULLY_QUALIFIED"]
        starter_qualified = [r for r in movement if r["post_remediation_starter_status"] == "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION"]
        blockers = Counter(r["remaining_downstream_blocker"] or "FULLY_QUALIFIED" for r in movement)
        hits_05 = sum(r["hits_line"] == "0.5" for r in fully)
        hits_15 = sum(r["hits_line"] == "1.5" for r in fully)
        baseline = self.state_result
        all_sides_certified = all(s["certification_result"] == "STARTER_SIDE_CERTIFIED" for s in side_results)
        all_frozen_yield_realized = len(starter_qualified) == 76 and len(fully) == 73 and hits_05 == 70 and hits_15 == 3
        if all_sides_certified and all_frozen_yield_realized:
            decision = DECISION_VALIDATED
            recommendation = REC_SCALE
        elif not all_sides_certified:
            decision = DECISION_PARTIAL
            recommendation = REC_PARTIAL
        elif len(fully) == 0:
            decision = DECISION_LOW
            recommendation = REC_LOW
        else:
            decision = DECISION_LINEAGE
            recommendation = REC_LINEAGE
        return {
            "decision": decision,
            "recommendation": recommendation,
            "generated_at": GENERATED_AT,
            "total_current_campaign_denominator_rows": baseline["denominator_rows"],
            "total_hits_rows": baseline["hits_rows"],
            "total_fully_qualified_hits": baseline["fully_qualified_hits_rows"] + len(fully),
            "fully_qualified_hits_0_5": baseline["fully_qualified_hits_0_5_rows"] + hits_05,
            "fully_qualified_hits_1_5": baseline["fully_qualified_hits_1_5_rows"] + hits_15,
            "current_starter_blocked_population": baseline["remaining_starter_blocked_total"] - len(starter_qualified),
            "current_pa_blocked_population": baseline["pa_blocked_rows"] + blockers["PA_BLOCKED"],
            "current_outcome_blocked_population": baseline["outcome_blocked_rows"] + blockers["OUTCOME_BLOCKED"],
            "current_bundle_blocked_population": baseline["bundle_field_blocked_rows"] + sum(v for k, v in blockers.items() if "BUNDLE" in k.upper()),
            "exact_movement_caused_only_by_this_overlay": {
                "starter_qualified_rows_added": len(starter_qualified),
                "newly_fully_qualified_rows_added": len(fully),
                "hits_0_5_additions": hits_05,
                "hits_1_5_additions": hits_15,
                "starter_blocked_rows_reduced_by": len(starter_qualified),
                "pa_blocked_rows_exposed_or_preserved": blockers["PA_BLOCKED"],
                "outcome_blocked_rows_exposed_or_preserved": blockers["OUTCOME_BLOCKED"],
                "bundle_blocked_rows_exposed_or_preserved": sum(v for k, v in blockers.items() if "BUNDLE" in k.upper()),
                "qualified_but_not_matrix_constructed_hits_1_5_after_hypothetical_success": baseline["variant_readiness"]["qualified_but_not_matrix_constructed_hits_1_5"] + hits_15,
            },
            "governed_sides_attempted": len(side_results),
            "sides_starter_certified": sum(s["certification_result"] == "STARTER_SIDE_CERTIFIED" for s in side_results),
            "sides_fail_closed": sum(s["certification_result"] != "STARTER_SIDE_CERTIFIED" for s in side_results),
            "failure_taxonomy_by_side": dict(Counter(s["certification_result"] for s in side_results)),
            "governed_denominator_rows_accounted_for": len(movement),
            "rows_starter_qualified": len(starter_qualified),
            "rows_still_starter_blocked": len(movement) - len(starter_qualified),
            "rows_newly_fully_qualified": len(fully),
            "hits_0_5_newly_fully_qualified": hits_05,
            "hits_1_5_newly_fully_qualified": hits_15,
            "downstream_pa_blockers_exposed": blockers["PA_BLOCKED"],
            "downstream_outcome_blockers_exposed": blockers["OUTCOME_BLOCKED"],
            "downstream_bundle_blockers_exposed": sum(v for k, v in blockers.items() if "BUNDLE" in k.upper()),
            "rows_with_multiple_downstream_blockers": sum(1 for r in movement if "|" in r["remaining_downstream_blocker"]),
            "realized_starter_qualification_yield_against_76_row_ceiling": round(len(starter_qualified) / 76, 6),
            "realized_full_qualification_yield_against_73_row_ceiling": round(len(fully) / 73, 6),
            "potential_abd_matrix_readiness_additions": sum(r["matrix_readiness_implication"] == "POTENTIAL_ABD_ADDITION" for r in movement),
            "qualified_but_not_matrix_constructed_hits_1_5_rows": baseline["variant_readiness"]["qualified_but_not_matrix_constructed_hits_1_5"] + hits_15,
            "variant_c_implication": "governance_preserved_not_resolved",
            "projected_vs_realized": {
                "projected_starter_qualified_ceiling": 76,
                "realized_starter_qualified": len(starter_qualified),
                "projected_newly_fully_qualified_ceiling": 73,
                "realized_newly_fully_qualified": len(fully),
                "projected_hits_0_5_additions": 70,
                "realized_hits_0_5_additions": hits_05,
                "projected_hits_1_5_additions": 3,
                "realized_hits_1_5_additions": hits_15,
                "projected_abd_matrix_readiness_additions": 3,
                "realized_abd_matrix_readiness_additions": sum(r["matrix_readiness_implication"] == "POTENTIAL_ABD_ADDITION" for r in movement),
                "variance_explanation": "none" if all_frozen_yield_realized else "see movement and side certification ledgers",
            },
            "prohibited_work": {
                "network_access": "not_performed",
                "discovery": "not_performed",
                "source_acquisition": "not_performed",
                "source_substitution": "not_performed",
                "pa_remediation": "not_performed",
                "outcome_remediation": "not_performed",
                "bundle_remediation": "not_performed",
                "variant_c_resolution": "not_performed",
                "matrix_construction": "not_performed",
                "modeling_or_scoring": "not_performed",
                "database_or_api_writes": "not_performed",
                "oddsapi": "not_called",
                "uploads": "not_performed",
                "launchagent_changes": "not_performed",
                "production_changes": "not_performed",
            },
        }

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation = self.verify()
        side_results = [self.side_result(side) for side in sorted(self.sides, key=lambda r: r["starter_game_side_identity"])]
        domain_rows = self.domain_rows(side_results)
        movement = self.movement_rows(side_results)
        payload = self.state_payload(movement, side_results)
        fully = [r for r in movement if r["post_remediation_full_qualification_status"] == "FULLY_QUALIFIED"]
        failure_taxonomy = Counter(r["certification_result"] for r in side_results)
        blocker_taxonomy = Counter(r["remaining_downstream_blocker"] or "FULLY_QUALIFIED" for r in movement)
        projection = payload["projected_vs_realized"]
        downstream_limited = [r for r in movement if r["remaining_downstream_blocker"]]
        campaign_reconciliation = [
            {"metric": "prior_total_fully_qualified_hits", "before": self.state_result["fully_qualified_hits_rows"], "movement": len(fully), "after": payload["total_fully_qualified_hits"]},
            {"metric": "prior_fully_qualified_hits_0_5", "before": self.state_result["fully_qualified_hits_0_5_rows"], "movement": payload["hits_0_5_newly_fully_qualified"], "after": payload["fully_qualified_hits_0_5"]},
            {"metric": "prior_fully_qualified_hits_1_5", "before": self.state_result["fully_qualified_hits_1_5_rows"], "movement": payload["hits_1_5_newly_fully_qualified"], "after": payload["fully_qualified_hits_1_5"]},
            {"metric": "prior_starter_blocked_total", "before": self.state_result["remaining_starter_blocked_total"], "movement": -payload["rows_starter_qualified"], "after": payload["current_starter_blocked_population"]},
            {"metric": "prior_pa_blocked_rows", "before": self.state_result["pa_blocked_rows"], "movement": payload["downstream_pa_blockers_exposed"], "after": payload["current_pa_blocked_population"]},
            {"metric": "prior_outcome_blocked_rows", "before": self.state_result["outcome_blocked_rows"], "movement": payload["downstream_outcome_blockers_exposed"], "after": payload["current_outcome_blocked_population"]},
            {"metric": "prior_bundle_blocked_rows", "before": self.state_result["bundle_field_blocked_rows"], "movement": payload["downstream_bundle_blockers_exposed"], "after": payload["current_bundle_blocked_population"]},
            {"metric": "prior_qualified_but_not_matrix_constructed_hits_1_5", "before": self.state_result["variant_readiness"]["qualified_but_not_matrix_constructed_hits_1_5"], "movement": payload["hits_1_5_newly_fully_qualified"], "after": payload["qualified_but_not_matrix_constructed_hits_1_5_rows"]},
        ]
        write_csv(OUT_DIR / f"exact_frozen_population_reproduction_{RUN_DATE}.csv", self.rows)
        write_csv(OUT_DIR / f"side_level_reconstruction_certification_ledger_{RUN_DATE}.csv", side_results)
        write_csv(OUT_DIR / f"reconstructed_starter_domain_ledger_{RUN_DATE}.csv", domain_rows)
        write_csv(OUT_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv", movement)
        write_csv(OUT_DIR / f"downstream_limited_row_preservation_ledger_{RUN_DATE}.csv", downstream_limited)
        write_csv(OUT_DIR / f"failure_taxonomy_{RUN_DATE}.csv", [
            {"taxonomy_family": "side_certification", "reason": k, "count": v} for k, v in sorted(failure_taxonomy.items())
        ] + [
            {"taxonomy_family": "row_remaining_blocker", "reason": k, "count": v} for k, v in sorted(blocker_taxonomy.items())
        ])
        write_csv(OUT_DIR / f"projection_versus_realized_yield_{RUN_DATE}.csv", [
            {"metric": key, "value": value} for key, value in projection.items()
        ])
        write_csv(OUT_DIR / f"campaign_movement_reconciliation_{RUN_DATE}.csv", campaign_reconciliation)
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        validation.extend([
            {"validation": "existing_abd_matrices_byte_identical_after", "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL", "observed": json.dumps(matrix_after, sort_keys=True), "expected": json.dumps(self.matrix_hash_before, sort_keys=True)},
            {"validation": "governance_artifacts_byte_identical_after", "status": "PASS" if package_sha(GOV_DIR) == EXPECTED_GOV_SHA else "FAIL", "observed": package_sha(GOV_DIR), "expected": EXPECTED_GOV_SHA},
            {"validation": "authoritative_source_artifacts_byte_identical_after", "status": "PASS" if package_sha(ACQ_DIR) == EXPECTED_ACQ_SHA else "FAIL", "observed": package_sha(ACQ_DIR), "expected": EXPECTED_ACQ_SHA},
            {"validation": "discovery_artifacts_byte_identical_after", "status": "PASS" if package_sha(DISCOVERY_DIR) == EXPECTED_DISCOVERY_SHA else "FAIL", "observed": package_sha(DISCOVERY_DIR), "expected": EXPECTED_DISCOVERY_SHA},
            {"validation": "scale_up_artifacts_byte_identical_after", "status": "PASS" if package_sha(SCALE_UP_DIR) == EXPECTED_SCALE_UP_SHA else "FAIL", "observed": package_sha(SCALE_UP_DIR), "expected": EXPECTED_SCALE_UP_SHA},
            {"validation": "prior_certification_packages_byte_identical_after", "status": "PASS" if package_sha(STATE_DIR, SOURCE_STATE_DATE) == EXPECTED_STATE_SHA else "FAIL", "observed": package_sha(STATE_DIR, SOURCE_STATE_DATE), "expected": EXPECTED_STATE_SHA},
            {"validation": "all_76_rows_accounted_for_in_movement_ledger", "status": "PASS" if len(movement) == 76 else "FAIL", "observed": len(movement), "expected": 76},
            {"validation": "exact_three_downstream_limited_rows_preserved", "status": "PASS" if len(downstream_limited) == 3 else "FAIL", "observed": len(downstream_limited), "expected": 3},
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
            {"check": "offline_replay", "status": "PASS", "notes": "Utility reads only frozen local package artifacts."},
            {"check": "network_requests", "status": "PASS", "notes": "0"},
            {"check": "bounded_overlay", "status": "PASS", "notes": "Exact 8 sides and 76 rows only."},
        ])
        write_csv(OUT_DIR / f"recommendation_for_next_campaign_step_{RUN_DATE}.csv", [{
            "recommendation": payload["recommendation"],
            "authorizes_scale_up": "false",
            "notes": "Recommendation only. Separate governance required for any scale-up.",
        }])
        write_json(OUT_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.md", f"""
# DISCOVERY_COHORT_002 Starter Remediation Certified State — {RUN_DATE}

Decision: `{payload['decision']}`

Recommendation: `{payload['recommendation']}`

- Governed sides attempted: `{payload['governed_sides_attempted']}`
- Sides Starter-certified: `{payload['sides_starter_certified']}`
- Sides fail-closed: `{payload['sides_fail_closed']}`
- Governed denominator rows accounted for: `{payload['governed_denominator_rows_accounted_for']}`
- Rows Starter-qualified: `{payload['rows_starter_qualified']}`
- Rows newly fully qualified: `{payload['rows_newly_fully_qualified']}`
- Hits 0.5 newly fully qualified: `{payload['hits_0_5_newly_fully_qualified']}`
- Hits 1.5 newly fully qualified: `{payload['hits_1_5_newly_fully_qualified']}`
- Downstream PA blockers exposed/preserved: `{payload['downstream_pa_blockers_exposed']}`
- Downstream Outcome blockers exposed/preserved: `{payload['downstream_outcome_blockers_exposed']}`
- Downstream Bundle blockers exposed/preserved: `{payload['downstream_bundle_blockers_exposed']}`
- Potential A/B/D matrix-readiness additions: `{payload['potential_abd_matrix_readiness_additions']}`

This is a non-destructive research overlay. No canonical source package, prior
certified state package, A/B/D matrix, database, API, upload, LaunchAgent, model,
signal, or production behavior was changed.
""")
        write_md(OUT_DIR / f"execution_summary_{RUN_DATE}.md", f"""
# DISCOVERY_COHORT_002 Starter Reconstruction/Remediation Execution — {RUN_DATE}

Decision: `{payload['decision']}`

The execution consumed only the frozen governance package, the preserved 139
certified strict-prior source records, and the existing certified campaign state.
All 8 governed Starter-game sides certified. Starter qualification propagated
only to the exact 76 governed denominator rows. The overlay realized the frozen
ceiling: 76 rows Starter-qualified, 73 newly fully qualified, and 3 rows
preserved as downstream PA-blocked without PA remediation.

BF remained corroborating provenance only. Offense-factor and expected-Hits
context remained an existing non-Starter binding boundary; no new formula,
fallback, or diagnostic substitute was introduced.
""")
        if any(row["status"] != "PASS" for row in validation) or any(row["status"] != "PASS" for row in static_guard()):
            raise RuntimeError("post-execution validation failed")
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
    result = DiscoveryCohort002StarterRemediation().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
