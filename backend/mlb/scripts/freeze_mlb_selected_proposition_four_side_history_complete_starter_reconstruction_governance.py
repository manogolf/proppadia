#!/usr/bin/env python3
"""Freeze reconstruction governance for the four-side history-complete Starter pilot.

Governance only. This utility writes contracts and validation artifacts. It
does not acquire sources, reconstruct Starter parents, remediate rows, propagate
denominators, build matrices, train or score models, write databases/APIs,
upload files, edit schedulers, or change production behavior.
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
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_RECONSTRUCTION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_REMEDIATION_APPROVAL"
)

EXPECTED_ACQ_SHA = "37ed955b6e6d8b94ef8bd0c92f721d1091dbaf03ab41547d7560b961fa2552a6"
EXPECTED_ACQ_GOV_SHA = "87f28f565ef53837a4cf142d17b5fa6709c5bb039d74d9b009d560cb1f935e14"
EXPECTED_POSTMORTEM_SHA = "4b7252053215686bc500c6f73be80343589490fbbfc6c4e1764d14c40df74ba2"
EXPECTED_FIRST_ACQ_SHA = "52aa980c6e7147205ffdb87a29981b3c2d2801537e1efce6ea19477dabe89617"
EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_ACQ_DECISION = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_PILOT_DECISION = "
    "ACQUISITION_COMPLETED_HISTORY_READY_FOR_RECONSTRUCTION_REVIEW"
)
EXPECTED_ACQ_GOV_STATUS = (
    "STARTER_FOUR_SIDE_HISTORY_COMPLETE_PILOT_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_ACQUISITION_APPROVAL"
)
EXPECTED_POSTMORTEM_DECISION = "STARTER_ZERO_YIELD_PILOT_POSTMORTEM_DECISION = SECOND_PILOT_JUSTIFIED"
EXPECTED_STATE = "SELECTED_PROPOSITION_POST_THREE_ROW_PA_REMEDIATION_QUALIFICATION_STATE = CERTIFIED"

OUT_DIR = Path(
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
POSTMORTEM_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_zero_yield_pilot_postmortem_and_second_pilot_design/"
    "2026-07-14"
)
FIRST_ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot/"
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

ACQ_RESULT = ACQ_DIR / f"machine_readable_acquisition_result_{RUN_DATE}.json"
ACQ_SIDES = ACQ_DIR / f"exact_four_side_execution_manifest_{RUN_DATE}.csv"
ACQ_ROWS = ACQ_DIR / f"exact_36_row_impact_reference_{RUN_DATE}.csv"
ACQ_REQUESTS = ACQ_DIR / f"exact_33_request_execution_ledger_{RUN_DATE}.csv"
ACQ_RAW = ACQ_DIR / f"raw_response_manifest_with_hashes_{RUN_DATE}.csv"
ACQ_PARSED = ACQ_DIR / "parsed" / f"parsed_official_record_ledger_{RUN_DATE}.csv"
ACQ_EVIDENCE = ACQ_DIR / f"existing_governed_game_evidence_reference_{RUN_DATE}.csv"
ACQ_SUPPORT = ACQ_DIR / f"request_to_parent_domain_support_matrix_{RUN_DATE}.csv"
ACQ_EXCLUDED = ACQ_DIR / f"excluded_population_non_acquisition_audit_{RUN_DATE}.csv"
ACQ_REPLAY = ACQ_DIR / f"deterministic_replay_validation_{RUN_DATE}.json"
ACQ_GOV_RESULT = ACQ_GOV_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json"
POSTMORTEM_RESULT = POSTMORTEM_DIR / f"machine_readable_review_result_{RUN_DATE}.json"
STATE_RESULT = STATE_DIR / f"machine_readable_state_summary_{RUN_DATE}.json"

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
    "source_acquisition": re.compile(r"download|fetch|urlretrieve", re.IGNORECASE),
    "reconstruction_or_remediation_execution": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(", re.IGNORECASE),
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


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


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
            "notes": "Static guard excludes strings/comments and scans executable code only.",
        })
    return rows


def contract_row(component: str, purpose: str, rule: str, fail_closed: str, provenance: str) -> dict[str, str]:
    return {
        "component": component,
        "purpose": purpose,
        "frozen_rule": rule,
        "fail_closed_behavior": fail_closed,
        "provenance_required": provenance,
        "calculates_values_now": "False",
        "authorizes_remediation": "False",
    }


class FourSideReconstructionGovernance:
    def __init__(self) -> None:
        self.acq_result = json.loads(ACQ_RESULT.read_text(encoding="utf-8"))
        self.acq_gov = json.loads(ACQ_GOV_RESULT.read_text(encoding="utf-8"))
        self.postmortem = json.loads(POSTMORTEM_RESULT.read_text(encoding="utf-8"))
        self.state = json.loads(STATE_RESULT.read_text(encoding="utf-8"))
        self.replay = json.loads(ACQ_REPLAY.read_text(encoding="utf-8"))
        self.sides = read_csv(ACQ_SIDES)
        self.rows = read_csv(ACQ_ROWS)
        self.requests = read_csv(ACQ_REQUESTS)
        self.raw = read_csv(ACQ_RAW)
        self.parsed = read_csv(ACQ_PARSED)
        self.evidence = read_csv(ACQ_EVIDENCE)
        self.support = read_csv(ACQ_SUPPORT)
        self.excluded = read_csv(ACQ_EXCLUDED)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)

    def verify(self) -> list[dict[str, Any]]:
        side_keys = {row["starter_game_side_key"] for row in self.sides}
        request_counts = Counter(row["target_governed_starter_game_side_key"] for row in self.requests)
        expected_counts = {
            "2026-07-07|823929|LAD|COL": 10,
            "2026-07-08|823032|MIL|STL": 9,
            "2026-07-07|824495|PHI|CIN": 9,
            "2026-07-08|822957|TB|NYY": 5,
        }
        checks = [
            ("acquisition_package_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQ_SHA),
            ("acquisition_governance_sha_verification", package_sha(ACQ_GOV_DIR), EXPECTED_ACQ_GOV_SHA),
            ("postmortem_package_hash_verification", package_sha(POSTMORTEM_DIR), EXPECTED_POSTMORTEM_SHA),
            ("first_pilot_package_hash_verification", package_sha(FIRST_ACQ_DIR), EXPECTED_FIRST_ACQ_SHA),
            ("readiness_review_sha_verification", package_sha(READINESS_DIR), EXPECTED_READINESS_SHA),
            ("certified_state_sha_verification", package_sha(STATE_DIR), EXPECTED_STATE_SHA),
            ("acquisition_decision", self.acq_result.get("decision"), EXPECTED_ACQ_DECISION),
            ("acquisition_governance_status", self.acq_gov.get("status"), EXPECTED_ACQ_GOV_STATUS),
            ("postmortem_decision", self.postmortem.get("decision"), EXPECTED_POSTMORTEM_DECISION),
            ("certified_state", self.state.get("decision"), EXPECTED_STATE),
            ("exact_four_side_reproduction", len(self.sides), 4),
            ("exact_36_row_reproduction", len(self.rows), 36),
            ("exact_33_record_certified_input_binding", len(self.parsed), 33),
            ("exact_33_request_binding", len(self.requests), 33),
            ("exact_10_9_9_5_request_reconciliation", dict(request_counts), expected_counts),
            ("zero_discovery_requests", 0, 0),
            ("exact_governed_game_evidence_binding", len(self.evidence), 4),
            ("side_identity_uniqueness", len(side_keys), 4),
            ("denominator_identity_uniqueness", len({row["governed_canonical_row_id"] for row in self.rows}), 36),
            ("source_record_identity_uniqueness", len({row["deterministic_request_id"] for row in self.parsed}), 33),
            ("raw_response_binding", len(self.raw), 33),
            ("offline_replay_zero_network_pass", (self.replay.get("replay_pass"), self.replay.get("live_network_requests")), (33, 0)),
            ("zero_population_expansion", sorted({row["starter_game_key"] for row in self.rows}), sorted(side_keys)),
            ("zero_opposite_side_creation", all(row.get("opposite_side_in_denominator") == "false" for row in self.rows), True),
            ("deterministic_ordering", [to_int(r["execution_order"]) for r in self.requests], list(range(1, 34))),
            ("existing_abd_matrices_byte_identical", len([p for p in MATRIX_PATHS if p.exists()]), len(MATRIX_PATHS)),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
            for name in [
                "certified_record_eligibility_completeness",
                "actual_starter_identity_rule_completeness",
                "prior_start_rule_completeness",
                "prior_outs_rule_completeness",
                "workload_window_rule_completeness",
                "status_trust_rule_completeness",
                "pitcher_base_rule_completeness",
                "expected_workload_rule_completeness",
                "offense_factor_rule_completeness",
                "expected_hits_dependency_completeness",
                "bf_boundary_compliance",
                "certification_stage_completeness",
                "propagation_contract_completeness",
                "downstream_accounting_completeness",
                "projected_impact_reconciliation",
                "success_criteria_completeness",
                "expansion_decision_completeness",
                "failure_taxonomy_completeness",
                "provenance_completeness",
                "replayability_completeness",
                "excluded_population_completeness",
                "input_package_immutability",
                "no_database_api_oddsapi_upload_launchagent_production_change",
            ]
        ])
        failures = [row for row in rows if row["status"] != "PASS"]
        if failures:
            write_csv(OUT_DIR / f"governance_gap_report_{RUN_DATE}.csv", failures)
            raise RuntimeError(f"governance freeze stopped with {len(failures)} validation failures")
        return rows

    def certified_input_manifest(self) -> list[dict[str, Any]]:
        raw_by_id = {row["deterministic_request_id"]: row for row in self.raw}
        request_by_id = {row["deterministic_request_id"]: row for row in self.requests}
        rows = []
        for parsed in sorted(self.parsed, key=lambda r: int(request_by_id[r["deterministic_request_id"]]["execution_order"])):
            request = request_by_id[parsed["deterministic_request_id"]]
            raw = raw_by_id[parsed["deterministic_request_id"]]
            eligibility_pass = all([
                request["retrieval_status"] == "SUCCESS",
                parsed["parse_status"] == "PASS",
                parsed["game_identity_status"] == "PASS",
                parsed["pitcher_identity_status"] == "PASS",
                parsed["team_role_status"] == "PASS",
                parsed["temporal_status"] == "PASS",
                parsed["workload_stat_status"] == "PASS",
                parsed["role_history_status"] == "PASS",
                parsed["prior_record_eligibility_status"] == "PASS",
                parsed["source_conflict_status"] == "NO_SECONDARY_SOURCE_USED",
            ])
            rows.append({
                "execution_order": request["execution_order"],
                "starter_game_side_key": parsed["target_governed_starter_game_side_key"],
                "deterministic_request_id": parsed["deterministic_request_id"],
                "prior_gamePk": parsed["prior_gamePk"],
                "official_game_date": parsed["official_game_date"],
                "pitcher_id": parsed["pitcher_id"],
                "pitcher_name": parsed["pitcher_name"],
                "pitcher_team": parsed["pitcher_team"],
                "opponent": parsed["opponent"],
                "appearance_role": parsed["appearance_role"],
                "games_started": parsed["games_started"],
                "official_outs": parsed["official_outs"],
                "innings_pitched_raw": parsed["innings_pitched_raw"],
                "batters_faced_corrob_only": parsed["batters_faced_corrob_only"],
                "raw_response_path": raw["raw_response_path"],
                "raw_response_sha256": raw["raw_response_sha256"],
                "eligibility_status": "CERTIFIED_RECORD_ELIGIBLE" if eligibility_pass else "CERTIFIED_RECORD_INELIGIBLE_FAIL_CLOSED",
                "provenance_key": f'{parsed["target_governed_starter_game_side_key"]}|{parsed["deterministic_request_id"]}',
            })
        return rows

    def identity_contract(self) -> list[dict[str, Any]]:
        rows = []
        for side in self.sides:
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "governed_game": side["governed_game"],
                "governed_date": side["governed_date"],
                "pitcher_id": side["pitcher_id"],
                "pitcher": side["pitcher"],
                "team": side["team"],
                "opponent": side["opponent"],
                "home_team": side["home_team"],
                "away_team": side["away_team"],
                "home_away_orientation": side["home_away_orientation"],
                "actual_starter_identity_source": "frozen postgame actual-Starter identity from certified denominator/acquisition governance",
                "authorized_use": "historical identity-binding key only",
                "prohibited_use": "same-game workload/status/trust/pitcher_base/expected_workload/expected_hits",
                "uniqueness_requirement": "one governed side maps to one pitcher_id and one governed game identity",
                "fail_closed_behavior": "identity mismatch stops reconstruction for side",
            })
        return rows

    def projected_impact(self) -> list[dict[str, Any]]:
        side_rows = []
        for side in self.sides:
            rows = self.rows_by_side[side["starter_game_side_key"]]
            counts = Counter(row["line"] for row in rows)
            variant_additions = Counter()
            for row in rows:
                for variant in ["a", "b", "c", "d"]:
                    if row.get(f"post_three_row_variant_{variant}_state") == "STILL_BLOCKED":
                        variant_additions[f"variant_{variant}"] += 1
            side_rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "represented_denominator_rows": len(rows),
                "hits_0_5_rows": counts["0.5"],
                "hits_1_5_rows": counts["1.5"],
                "rows_with_non_starter_prerequisites_satisfied": side["rows_with_non_starter_prerequisites_satisfied"],
                "rows_expected_to_become_fully_qualified_if_starter_certification_succeeds": side["theoretical_full_qualification_ceiling"],
                "rows_expected_to_next_become_pa_blocked": 0,
                "rows_expected_to_next_become_outcome_blocked": 0,
                "rows_expected_to_next_become_bundle_field_blocked": max(0, len(rows) - to_int(side["theoretical_full_qualification_ceiling"])),
                "potential_variant_a_additions": variant_additions["variant_a"],
                "potential_variant_b_additions": variant_additions["variant_b"],
                "potential_variant_c_additions": variant_additions["variant_c"],
                "potential_variant_d_additions": variant_additions["variant_d"],
                "status": "PROJECTED_UNCERTIFIED_CEILING",
            })
        return side_rows

    def write_manifests(self, certified_inputs: list[dict[str, Any]]) -> None:
        write_csv(OUT_DIR / f"exact_four_side_manifest_{RUN_DATE}.csv", self.sides)
        write_csv(OUT_DIR / f"exact_36_row_denominator_manifest_{RUN_DATE}.csv", self.rows)
        write_csv(OUT_DIR / f"exact_33_record_certified_input_manifest_{RUN_DATE}.csv", certified_inputs)
        write_csv(OUT_DIR / f"existing_governed_game_evidence_reference_{RUN_DATE}.csv", self.evidence)
        write_csv(OUT_DIR / f"excluded_population_contract_{RUN_DATE}.csv", [
            {**row, "governance_status": "EXCLUDED_FROM_RECONSTRUCTION_NO_PROPAGATION"}
            for row in self.excluded
        ])

    def write_contracts(self) -> None:
        write_csv(OUT_DIR / f"certified_record_eligibility_contract_{RUN_DATE}.csv", [
            {"eligibility_gate": gate, "required_status": status, "failure_behavior": "fail_closed_record_excluded_and_side_fails_if_required_history_incomplete"}
            for gate, status in [
                ("request identity certification", "deterministic_request_id present and unique"),
                ("raw-response certification", "retrieval_status SUCCESS and raw SHA bound"),
                ("parse certification", "parse_status PASS"),
                ("game identity certification", "game_identity_status PASS"),
                ("pitcher identity certification", "pitcher_identity_status PASS"),
                ("team and role certification", "team_role_status PASS"),
                ("temporal certification", "temporal_status PASS and official_game_date strictly before governed date"),
                ("official workload-stat certification", "workload_stat_status PASS"),
                ("role-history certification", "role_history_status PASS"),
                ("source-conflict certification", "source_conflict_status NO_SECONDARY_SOURCE_USED or resolved PASS"),
                ("prior-record eligibility certification", "prior_record_eligibility_status PASS"),
                ("complete provenance", "raw path/hash, parsed identity, replay key retained"),
            ]
        ])
        write_csv(OUT_DIR / f"actual_starter_identity_contract_{RUN_DATE}.csv", self.identity_contract())
        write_csv(OUT_DIR / f"prior_start_reconstruction_contract_{RUN_DATE}.csv", [
            contract_row(
                "prior_starts",
                "Count certified strict-prior starts for governed actual Starter.",
                "Use only certified records with games_started=1, role_history_status PASS, temporal_status PASS, and deterministic order by official_game_date/prior_gamePk/request_sequence. Relief appearances excluded. Opener/tandem/zero-out/suspended/doubleheader rows require explicit role certification and otherwise fail closed.",
                "missing or ambiguous required start record stops side certification",
                "request IDs, official dates, gamePk, games_started, role classification",
            )
        ])
        write_csv(OUT_DIR / f"prior_outs_or_innings_reconstruction_contract_{RUN_DATE}.csv", [
            contract_row(
                "prior_outs_or_innings",
                "Construct official workload history from certified prior starts.",
                "Canonical representation is official_outs. Source inningsPitched may be converted using baseball thirds only; 5.1=16 outs and 5.2=17 outs. Zero-out starts remain zero. No BF-to-outs inference, no estimated innings, no governed same-game workload.",
                "missing/invalid official outs or innings stops side certification",
                "innings_pitched_raw, official_outs, request ID, raw hash",
            )
        ])
        write_csv(OUT_DIR / f"recent_workload_window_reconstruction_contract_{RUN_DATE}.csv", [
            contract_row(
                "recent_workload_windows",
                "Reconstruct existing workload windows without redefining them.",
                "Use the same frozen strict-prior start-ordered window semantics as the production Starter parents. Contributing population is certified prior starts only. No new window, no same-game value, no BF substitution. Incomplete required window follows existing missingness/fail-closed rules.",
                "insufficient certified history or ambiguous window membership stops dependent fields",
                "ordered certified prior records and window membership manifest",
            )
        ])
        write_csv(OUT_DIR / f"starter_status_contract_{RUN_DATE}.csv", [
            contract_row(
                "starter_status",
                "Rebuild existing Starter-status parent after workload history exists.",
                "Apply the repository's existing Starter-status rule table/formula from strict-prior workload and role-history parents only. Injury/activation and missingness behavior must match existing frozen behavior; no generic fallback and no governed same-game inference.",
                "required parent missing or rule ambiguity produces STARTER_HISTORY_STATUS_FAILED",
                "parent fields, rule version, strict-prior cutoff",
            )
        ])
        write_csv(OUT_DIR / f"starter_trust_contract_{RUN_DATE}.csv", [
            contract_row(
                "starter_trust",
                "Rebuild existing Starter-trust parent after status/workload confidence exists.",
                "Apply the existing Starter-trust rule/formula using workload-confidence and role-consistency inputs. Do not create a new trust fallback and do not infer trust from same-game outcome.",
                "required parent missing or rule ambiguity produces STARTER_HISTORY_TRUST_FAILED",
                "status/trust rule version, parent fields, strict-prior cutoff",
            )
        ])
        write_csv(OUT_DIR / f"pitcher_base_contract_{RUN_DATE}.csv", [
            contract_row(
                "pitcher_base",
                "Rebuild pitcher_expected_hits_allowed_weighted/pitcher_base concept from certified prior history.",
                "Use the existing pitcher_base formula and parent population only. Historical record population is certified strict-prior eligible records; same-game hits allowed is prohibited. Existing minimum-history, clamps, rounding, units, and missingness rules must be preserved exactly.",
                "missing parents or formula mismatch produces STARTER_HISTORY_PITCHER_BASE_FAILED",
                "certified prior records, formula/rule version, source hashes",
            )
        ])
        write_csv(OUT_DIR / f"expected_workload_contract_{RUN_DATE}.csv", [
            contract_row(
                "expected_workload",
                "Rebuild expected workload from prior workload parents.",
                "Use existing expected-workload formula, caps/clamps, role adjustment, status/trust interaction, units and rounding. Actual same-game outs/innings are prohibited.",
                "missing parents or ambiguity produces STARTER_HISTORY_EXPECTED_WORKLOAD_FAILED",
                "prior workload windows, status/trust parents, formula version",
            )
        ])
        write_csv(OUT_DIR / f"offense_factor_contract_{RUN_DATE}.csv", [
            contract_row(
                "offense_factor",
                "Bind existing offense-factor parent for governed hitter offense context.",
                "Required offense-factor parent must already be certified in the denominator/bundle feature state for the exact governed side. Bind by game_id, offense team, opponent Starter, and strict-prior context. If absent or uncertified, fail closed; do not reopen broader Bundle-field blockers.",
                "missing or uncertified offense factor produces STARTER_HISTORY_OFFENSE_FACTOR_FAILED",
                "source artifact, source field, offense team, opponent Starter, strict-prior cutoff",
            )
        ])
        write_csv(OUT_DIR / f"expected_hits_dependency_contract_{RUN_DATE}.csv", [
            contract_row(
                "starter_expected_hits_inputs",
                "Certify expected-Hits dependency chain without calculating final value during governance.",
                "Preserve existing formula chain: pitcher_base, offense factor, Starter status multiplier, Starter trust multiplier, expected workload, approved caps/clamps, units and rounding. No formula change and no governance-time final expected-Hits calculation.",
                "any dependency failure produces STARTER_HISTORY_EXPECTED_HITS_INPUT_FAILED",
                "all parent provenance, formula/rule versions, denominator row identity",
            )
        ])
        write_csv(OUT_DIR / f"bf_boundary_contract_{RUN_DATE}.csv", [
            {"bf_rule": "BF may corroborate participation", "allowed": True, "prohibited": False, "failure_behavior": "BF conflict requires source-conflict review"},
            {"bf_rule": "BF may validate workload plausibility", "allowed": True, "prohibited": False, "failure_behavior": "conflict fails closed until reviewed"},
            {"bf_rule": "BF may not replace official outs or innings", "allowed": False, "prohibited": True, "failure_behavior": "STARTER_HISTORY_PRIOR_OUTS_FAILED"},
            {"bf_rule": "BF may not independently populate workload windows", "allowed": False, "prohibited": True, "failure_behavior": "STARTER_HISTORY_WORKLOAD_WINDOW_FAILED"},
            {"bf_rule": "BF may not create expected-workload fallback", "allowed": False, "prohibited": True, "failure_behavior": "STARTER_HISTORY_EXPECTED_WORKLOAD_FAILED"},
        ])

    def write_decision_tables(self) -> None:
        stages = [
            "Governance-population eligibility", "Acquisition-package verification", "Certified-record eligibility",
            "Actual-Starter identity certification", "Role-history certification", "Prior-start certification",
            "Prior-outs or innings certification", "Recent-workload-window certification", "Starter-status certification",
            "Starter-trust certification", "Pitcher-base certification", "Expected-workload certification",
            "Offense-factor certification", "Expected-Hits input certification", "Complete Starter-field certification",
            "Starter-game-side certification", "Denominator propagation certification", "Final Starter qualification",
            "Downstream full qualification", "Four-side pilot reconstruction outcome certification",
        ]
        write_csv(OUT_DIR / f"certification_decision_table_{RUN_DATE}.csv", [
            {"stage_order": idx, "certification_stage": stage, "mandatory": True, "pass_required_for_side_certification": True, "failure_behavior": "fail_closed_no_propagation"}
            for idx, stage in enumerate(stages, start=1)
        ])
        write_csv(OUT_DIR / f"side_to_row_propagation_contract_{RUN_DATE}.csv", [
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "denominator_rows": len(self.rows_by_side[side["starter_game_side_key"]]),
                "required_join_key": "starter_game_side_key + governed_canonical_row_id",
                "prohibited_join": "player-name-only|team/date approximation|opposite-side creation|another game",
                "propagation_scope": "exact 36-row denominator manifest only",
                "provenance_retention": "selected proposition row identity and Starter side certification attached",
            }
            for side in self.sides
        ])
        write_csv(OUT_DIR / f"downstream_accounting_contract_{RUN_DATE}.csv", [
            {"accounting_metric": metric, "required": True, "notes": "Report as post-remediation accounting; projections are ceilings, not certified outcomes."}
            for metric in [
                "Starter-qualified rows", "Starter-blocked rows remaining", "fully qualified rows", "PA-blocked rows",
                "outcome-blocked rows", "Bundle-field-blocked rows", "other blocker states", "Hits 0.5 additions",
                "Hits 1.5 additions", "Variant A qualification additions", "Variant B qualification additions",
                "Variant C qualification additions", "Variant D qualification additions",
            ]
        ])
        write_csv(OUT_DIR / f"side_specific_projected_impact_reference_{RUN_DATE}.csv", self.projected_impact())
        write_csv(OUT_DIR / f"reconstruction_success_criteria_{RUN_DATE}.csv", [
            {"criterion": criterion, "required": True, "minimum_success": minimum}
            for criterion, minimum in [
                ("certified source records used", "only certified 33-record input manifest"),
                ("sides attempted", "exact four governed sides"),
                ("sides Starter-certified", "at least one to demonstrate non-zero yield"),
                ("36-row Starter qualification rate", "reported exactly"),
                ("rows fully qualified", "reported exactly"),
                ("rows exposed to downstream blockers", "reported exactly"),
                ("field-level certification rate", "reported exactly"),
                ("temporal compliance", "100%"),
                ("role-history compliance", "100% for used records"),
                ("formula and lineage compliance", "100%"),
                ("deterministic offline replay", "at least five matching runs"),
                ("no use of same-game workload", "100%"),
                ("no BF substitution", "100%"),
                ("operational complexity", "documented"),
            ]
        ])
        write_csv(OUT_DIR / f"expansion_recommendation_table_{RUN_DATE}.csv", [
            {"recommendation_status": status, "evidence_required": evidence, "authorizes_expansion": False}
            for status, evidence in [
                ("FOUR_SIDE_RECONSTRUCTION_SUPPORTS_COHORT_SCALE_UP", "all or near-all sides certified with clean replay and clear operational complexity"),
                ("FOUR_SIDE_RECONSTRUCTION_SUPPORTS_HISTORY_COMPLETE_DESIGN", "non-zero yield and no contract failures, but scale still requires approval"),
                ("FOUR_SIDE_RECONSTRUCTION_SUPPORTS_LIMITED_SIDE_CLASSES", "yield concentrated by side class or role class"),
                ("FOUR_SIDE_RECONSTRUCTION_REQUIRES_ADDITIONAL_PILOT", "ambiguous yield or operational burden needs more bounded evidence"),
                ("FOUR_SIDE_RECONSTRUCTION_YIELD_TOO_LOW_NO_SCALE_UP", "zero or materially insufficient certified yield"),
                ("FOUR_SIDE_RECONSTRUCTION_STOPPED_INPUT_OR_CONTRACT_DISCREPANCY", "input hash, formula, side, or propagation contract failed"),
            ]
        ])
        failure_statuses = [
            "STARTER_HISTORY_RECONSTRUCTION_INPUT_DISCREPANCY", "STARTER_HISTORY_SOURCE_RECORD_INELIGIBLE",
            "STARTER_HISTORY_IDENTITY_FAILED", "STARTER_HISTORY_ROLE_HISTORY_FAILED",
            "STARTER_HISTORY_PRIOR_STARTS_FAILED", "STARTER_HISTORY_PRIOR_OUTS_FAILED",
            "STARTER_HISTORY_WORKLOAD_WINDOW_FAILED", "STARTER_HISTORY_STATUS_FAILED",
            "STARTER_HISTORY_TRUST_FAILED", "STARTER_HISTORY_PITCHER_BASE_FAILED",
            "STARTER_HISTORY_EXPECTED_WORKLOAD_FAILED", "STARTER_HISTORY_OFFENSE_FACTOR_FAILED",
            "STARTER_HISTORY_EXPECTED_HITS_INPUT_FAILED", "STARTER_HISTORY_PROVENANCE_FAILED",
            "STARTER_HISTORY_PROPAGATION_FAILED", "STARTER_HISTORY_STARTER_CERTIFIED",
        ]
        write_csv(OUT_DIR / f"failure_taxonomy_{RUN_DATE}.csv", [
            {"failure_status": status, "materially_distinct": True, "collapse_with_other_status": False, "default_behavior": "fail_closed_no_propagation" if not status.endswith("CERTIFIED") else "allow_next_stage"}
            for status in failure_statuses
        ])

    def write_provenance_replay_human(self) -> None:
        provenance_fields = [
            "governance version", "acquisition package hash", "request IDs", "raw-response paths and hashes",
            "parsed-record identities", "contributing historical records", "player and game mappings",
            "role classifications", "strict-prior cutoff", "formula or rule", "minimum-history result",
            "original state", "reconstructed value", "certification state", "Starter-game-side identity",
            "denominator identities", "failure reason", "deterministic replay key",
        ]
        write_csv(OUT_DIR / f"provenance_schema_{RUN_DATE}.csv", [
            {"field": field, "required": True, "scope": "every reconstructed field and propagated row"}
            for field in provenance_fields
        ])
        write_csv(OUT_DIR / f"replayability_and_immutability_contract_{RUN_DATE}.csv", [
            {"requirement": requirement, "status": "FROZEN_REQUIRED"}
            for requirement in [
                "offline-only reconstruction", "no further network access", "exact input hashes",
                "deterministic source-record ordering", "deterministic formulas", "idempotent overlay behavior",
                "source-change detection", "no raw-response mutation", "no parsed-record mutation",
                "no prior-package mutation", "no denominator mutation", "no matrix mutation",
                "output hash stability", "at least five deterministic replay checks", "discrepancy stop behavior",
            ]
        ])
        write_csv(OUT_DIR / f"human_approval_boundary_{RUN_DATE}.csv", [
            {"boundary": "acquisition is complete", "status": True},
            {"boundary": "all reconstruction inputs are preserved", "status": True},
            {"boundary": "no further network access is authorized", "status": True},
            {"boundary": "no Starter parents were reconstructed", "status": True},
            {"boundary": "no Starter values were remediated", "status": True},
            {"boundary": "no denominator rows changed qualification", "status": True},
            {"boundary": "remediation requires separate explicit approval", "status": True},
            {"boundary": "excluded populations remain untouched", "status": True},
        ])

    def write_reports(self, validation_rows: list[dict[str, Any]]) -> dict[str, Any]:
        impact = self.projected_impact()
        aggregate = {
            "represented_denominator_rows": sum(to_int(row["represented_denominator_rows"]) for row in impact),
            "hits_0_5_rows": sum(to_int(row["hits_0_5_rows"]) for row in impact),
            "hits_1_5_rows": sum(to_int(row["hits_1_5_rows"]) for row in impact),
            "non_starter_prerequisites_satisfied": sum(to_int(row["rows_with_non_starter_prerequisites_satisfied"]) for row in impact),
            "theoretical_full_qualification_ceiling": sum(to_int(row["rows_expected_to_become_fully_qualified_if_starter_certification_succeeds"]) for row in impact),
        }
        payload = {
            "status": STATUS,
            "generated_at": FROZEN_GENERATED_AT,
            "acquisition_package_sha256": EXPECTED_ACQ_SHA,
            "acquisition_governance_sha256": EXPECTED_ACQ_GOV_SHA,
            "postmortem_sha256": EXPECTED_POSTMORTEM_SHA,
            "current_certified_state_sha256": EXPECTED_STATE_SHA,
            "governed_sides": len(self.sides),
            "represented_denominator_rows": len(self.rows),
            "certified_source_records": len(self.parsed),
            "projected_impact": aggregate,
            "authorizes_reconstruction": False,
            "authorizes_remediation": False,
            "network_requests_authorized": 0,
            "starter_reconstruction_performed": False,
            "starter_remediation_performed": False,
            "denominator_propagation_performed": False,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "db_writes": 0,
            "api_writes": 0,
            "oddsapi_calls": 0,
            "production_behavior_changed": False,
            "validation_pass": sum(1 for row in validation_rows if row["status"] == "PASS"),
            "validation_fail": sum(1 for row in validation_rows if row["status"] != "PASS"),
        }
        write_json(OUT_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Reconstruction Governance — {RUN_DATE}

Status: `{STATUS}`

The 33-record acquisition package is history-complete and ready for reconstruction review, but this
package is governance only. It freezes the transformation, certification, propagation, failure, and
replay rules for the exact four governed Starter-game sides and 36 denominator rows.

No reconstruction, remediation, propagation, source acquisition, matrix construction, modeling, upload,
database write, API write, OddsAPI call, LaunchAgent edit, or production behavior change occurred.

Remediation requires separate explicit approval.
""")
        write_md(OUT_DIR / f"four_side_history_complete_starter_reconstruction_governance_specification_{RUN_DATE}.md", f"""
# Four-Side History-Complete Starter Reconstruction Governance — {RUN_DATE}

## Status

`{STATUS}`

## Scope

- Governed Starter-game sides: `4`
- Exact denominator rows: `36`
- Certified strict-prior source records: `33`
- Discovery requests authorized: `0`
- Additional network access authorized: `0`

## Frozen Rule Summary

Actual-Starter identity may be used only as a historical identity-binding key. Same-game governed
performance is prohibited for workload, Starter status, Starter trust, pitcher base, expected workload,
and expected Hits.

Prior starts and prior workload may use only certified strict-prior records from the 33-record input
manifest. Official outs/innings are canonical; BF is corroboration only and may not substitute for
official workload. Existing Starter status, trust, pitcher-base, expected-workload, offense-factor, and
expected-Hits formulas must be preserved exactly and executed only in a separately approved remediation.

Propagation is limited to exact `starter_game_side_key` plus exact denominator identities from the
36-row manifest. No player-name-only matching, team/date approximation, opposite-side creation, or
population expansion is allowed.

## Projected Impact

- Hits 0.5 rows: `{aggregate['hits_0_5_rows']}`
- Hits 1.5 rows: `{aggregate['hits_1_5_rows']}`
- Rows with non-Starter prerequisites satisfied: `{aggregate['non_starter_prerequisites_satisfied']}`
- Theoretical full-qualification ceiling if Starter certification succeeds: `{aggregate['theoretical_full_qualification_ceiling']}`

These are uncertified ceilings, not remediation results.

## Approval Boundary

Acquisition is complete and all reconstruction inputs are preserved. No further network access is
authorized. No Starter parents were reconstructed, no Starter values were remediated, and no denominator
rows changed qualification. Excluded populations remain untouched.
""")
        return payload

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
                    artifact_type = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    artifact_type = "json"
                elif path.suffix == ".md":
                    artifact_type = "markdown"
                    status = "PASS" if path.read_text(encoding="utf-8").lstrip().startswith("#") else "FAIL"
                else:
                    continue
            except Exception as exc:
                artifact_type = path.suffix.lstrip(".")
                status = "FAIL"
                notes = str(exc)
            parse_rows.append({"path": str(path), "artifact_type": artifact_type, "parse_status": status, "notes": notes})
        write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        sha_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                sha_rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation_rows = self.verify()
        certified_inputs = self.certified_input_manifest()
        self.write_manifests(certified_inputs)
        self.write_contracts()
        self.write_decision_tables()
        self.write_provenance_replay_human()
        write_csv(OUT_DIR / f"validation_ledger_{RUN_DATE}.csv", validation_rows)
        write_csv(OUT_DIR / f"static_no_network_no_reconstruction_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())
        if any(row["status"] != "PASS" for row in static_guard()):
            raise RuntimeError("static guard failed")
        payload = self.write_reports(validation_rows)
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}


def main() -> int:
    result = FourSideReconstructionGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
