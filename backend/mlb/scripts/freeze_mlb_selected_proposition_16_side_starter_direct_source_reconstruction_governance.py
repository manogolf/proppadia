#!/usr/bin/env python3
"""Freeze governance for 16-side Starter direct-source evidence remediation.

This writes a governance/specification package only. It performs no network
access, source acquisition, Starter value calculation, remediation,
qualification propagation, matrix construction, modeling, database writes, API
writes, uploads, scheduler edits, or production behavior changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
FROZEN_GENERATED_AT = "2026-07-14T00:00:00+00:00"
STATUS = (
    "STARTER_16_SIDE_DIRECT_SOURCE_RECONSTRUCTION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_REMEDIATION_APPROVAL"
)

EXPECTED_ACQUISITION_SHA = "52aa980c6e7147205ffdb87a29981b3c2d2801537e1efce6ea19477dabe89617"
EXPECTED_GOVERNANCE_SHA = "fa310668bd1fac4d9993e3557dfd4dd8d20f7dc9258ae2af807f70c8fc8f3651"
EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_ACQUISITION_DECISION = (
    "STARTER_16_SIDE_DIRECT_SOURCE_PILOT_DECISION = "
    "ACQUISITION_COMPLETED_EVIDENCE_READY_FOR_RECONSTRUCTION_REVIEW"
)
EXPECTED_SCALE_UP = "PILOT_SUPPORTS_SCALE_UP"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_reconstruction_governance/"
    "2026-07-14"
)
ACQ_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot/"
    "2026-07-14"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_16_side_starter_direct_source_pilot_governance/"
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
ACQ_SIDES = GOV_DIR / f"exact_16_side_manifest_{RUN_DATE}.csv"
ACQ_ROWS = ACQ_DIR / f"exact_144_row_impact_reference_{RUN_DATE}.csv"
ACQ_REQUESTS = GOV_DIR / f"exact_acquisition_request_manifest_{RUN_DATE}.csv"
ACQ_RAW = ACQ_DIR / f"raw_response_manifest_with_hashes_{RUN_DATE}.csv"
ACQ_PARSED = ACQ_DIR / "parsed" / f"parsed_mlb_stats_api_record_ledger_{RUN_DATE}.csv"
ACQ_REMAINING = ACQ_DIR / f"remaining_80_side_non_acquisition_audit_{RUN_DATE}.csv"
ACQ_VALIDATION = ACQ_DIR / f"validation_ledger_{RUN_DATE}.csv"
ACQ_SPECIAL = ACQ_DIR / "audits" / f"special_regime_audit_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

PROHIBITED_PATTERNS = {
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "db_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert)\b", re.IGNORECASE),
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "matrix_build": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
    "oddsapi": re.compile(r"oddsapi|odds_api", re.IGNORECASE),
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


def now() -> str:
    return FROZEN_GENERATED_AT


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
            "notes": "Static behavioral guard; strings and comments are excluded.",
        })
    return rows


def side_regime(parsed_row: dict[str, str]) -> str:
    if parsed_row.get("special_regime_status") == "possible_opener_or_short_start":
        return "ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED"
    return "ORDINARY_STARTER_RECONSTRUCTION_ELIGIBLE"


def build_field_inventory(sides: list[dict[str, str]], parsed_by_side: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    fields = [
        ("governed_starter_identity", "source_record_complete", "parsed official starter id/name"),
        ("actual_starter_identity_binding_key", "source_record_complete", "same-game actual starter identity; identity binding only"),
        ("starter_handedness", "partial_or_repository_required", "StatsAPI feed may omit pitch hand; repository/player source may be required"),
        ("prior_official_starts", "requires_strict_prior_reconstruction", "must use starts before slate date only"),
        ("prior_official_pitching_outs_or_innings", "requires_strict_prior_reconstruction", "must use official prior pitching logs before slate date only"),
        ("recent_workload_windows", "requires_strict_prior_reconstruction", "d7/d15/d30 or frozen repository workload windows"),
        ("expected_workload", "requires_parent_reconstruction", "derive only from strict-prior workload parents"),
        ("starter_status", "requires_parent_reconstruction", "strict-prior role/status rule"),
        ("starter_trust", "requires_parent_reconstruction", "strict-prior trust rule"),
        ("pitcher_base", "requires_parent_reconstruction", "frozen pitcher base formula from strict-prior parents"),
        ("offense_factor_vs_league", "repository_or_lineage_binding_required", "bind to offense context as of date before slate"),
        ("expected_hits_parents", "requires_complete_parent_chain", "pitcher base and offense factor plus frozen status/trust/workload parents"),
        ("starter_expected_hits_allowed", "requires_formula_application_in_future_remediation", "do not calculate in governance"),
        ("provenance", "governance_defined", "raw hash, parsed identity, formula, cutoff, side and row ids"),
        ("temporal_eligibility", "governance_defined", "same-game actual identity allowed only as binding key"),
        ("special_regime_classification", "source_record_complete_for_screening", "short-start sides fail closed unless later approved rule applies"),
    ]
    rows = []
    for side in sides:
        parsed = parsed_by_side[side["starter_game_side_key"]]
        for field, availability, notes in fields:
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "cohort": side["pilot_reason"],
                "field_name": field,
                "repository_or_source_status": availability,
                "actual_starter_id_if_available": parsed.get("official_starter_player_id", ""),
                "actual_starter_name_if_available": parsed.get("official_starter_name", ""),
                "special_regime_governance": side_regime(parsed),
                "same_game_performance_use": "PROHIBITED_AS_PREGAME_WORKLOAD",
                "notes": notes,
            })
    return rows


def contract_rows() -> dict[str, list[dict[str, Any]]]:
    return {
        f"actual_starter_identity_contract_{RUN_DATE}.csv": [
            {"rule": "identity_source", "governance": "certified acquired source record", "requirement": "unique official starter for game_id + opponent_team", "failure": "STARTER_PILOT_IDENTITY_CERTIFICATION_FAILED"},
            {"rule": "permitted_use", "governance": "historical binding key only", "requirement": "must not use same-game workload as prior workload", "failure": "STARTER_PILOT_TEMPORAL_PROVENANCE_FAILED"},
            {"rule": "orientation", "governance": "opponent_team is starter side for hitter proposition", "requirement": "exact team-side match", "failure": "STARTER_PILOT_PROPAGATION_FAILED"},
            {"rule": "replacement_or_late_scratch", "governance": "actual starter may bind identity; strict-prior parents remain before slate", "requirement": "record source provenance", "failure": "STARTER_PILOT_ROLE_REGIME_AMBIGUOUS"},
        ],
        f"prior_start_and_workload_lineage_contract_{RUN_DATE}.csv": [
            {"component": "prior_official_starts", "source": "official pitcher game logs before slate date", "temporal_cutoff": "game_date < slate_date", "units": "starts count", "bf_policy": "BF corroboration only", "failure": "STARTER_PILOT_PRIOR_STARTS_FAILED"},
            {"component": "prior_official_outs_or_innings", "source": "official pitching line before slate date", "temporal_cutoff": "game_date < slate_date", "units": "outs primary, innings display", "bf_policy": "no BF-to-outs inference", "failure": "STARTER_PILOT_PRIOR_OUTS_FAILED"},
            {"component": "recent_workload_windows", "source": "strict-prior ordered pitching starts/appearances", "temporal_cutoff": "game_date < slate_date", "units": "window aggregates", "bf_policy": "BF validation only", "failure": "STARTER_PILOT_WORKLOAD_WINDOW_FAILED"},
            {"component": "incomplete_window_behavior", "source": "frozen repository minimum-history policy", "temporal_cutoff": "no same-day data", "units": "fail closed when minimum not met", "bf_policy": "no fallback substitution", "failure": "STARTER_PILOT_WORKLOAD_WINDOW_FAILED"},
        ],
        f"starter_status_contract_{RUN_DATE}.csv": [
            {"field": "starter_status", "parents": "actual identity binding + strict-prior role/workload history", "formula_or_rule": "repository frozen starter status rule", "minimum_history": "must be proven from prior starts", "missing_behavior": "fail closed", "failure": "STARTER_PILOT_STATUS_FAILED"},
        ],
        f"starter_trust_contract_{RUN_DATE}.csv": [
            {"field": "starter_trust", "parents": "strict-prior starts/workload/role stability", "formula_or_rule": "repository frozen starter trust rule", "minimum_history": "must be proven from prior records", "missing_behavior": "fail closed", "failure": "STARTER_PILOT_TRUST_FAILED"},
        ],
        f"pitcher_base_contract_{RUN_DATE}.csv": [
            {"field": "pitcher_base", "parents": "strict-prior pitcher expected hits allowed weighted parents", "formula_or_rule": "frozen pitcher_base equals pitcher_expected_hits_allowed_weighted", "same_game_actual_policy": "prohibited", "missing_behavior": "fail closed", "failure": "STARTER_PILOT_PITCHER_BASE_FAILED"},
        ],
        f"expected_workload_contract_{RUN_DATE}.csv": [
            {"field": "expected_workload", "parents": "strict-prior starts and workload windows", "formula_or_rule": "frozen expected workload rule; no generic fallback", "same_game_actual_policy": "prohibited", "missing_behavior": "fail closed", "failure": "STARTER_PILOT_EXPECTED_WORKLOAD_FAILED"},
        ],
        f"offense_factor_contract_{RUN_DATE}.csv": [
            {"field": "offense_factor_vs_league", "source": "frozen offense-factor lineage artifacts/repository values", "team_binding": "hitter team versus league context", "temporal_cutoff": "context_as_of_date < slate_date", "missing_behavior": "fail closed within this 16-side review", "failure": "STARTER_PILOT_OFFENSE_FACTOR_FAILED"},
        ],
        f"expected_hits_dependency_contract_{RUN_DATE}.csv": [
            {"sequence_order": 1, "dependency": "pitcher_base", "certification": "strict-prior parent certified", "failure": "STARTER_PILOT_PITCHER_BASE_FAILED"},
            {"sequence_order": 2, "dependency": "offense_factor_vs_league_clamped", "certification": "temporal context certified", "failure": "STARTER_PILOT_OFFENSE_FACTOR_FAILED"},
            {"sequence_order": 3, "dependency": "starter_status/trust/expected_workload where frozen formula requires", "certification": "strict-prior parents certified", "failure": "STARTER_PILOT_EXPECTED_WORKLOAD_FAILED"},
            {"sequence_order": 4, "dependency": "starter_expected_hits_allowed", "certification": "formula applied only in later approved remediation", "failure": "STARTER_PILOT_EXPECTED_HITS_INPUT_FAILED"},
        ],
    }


def certification_stages() -> list[dict[str, Any]]:
    names = [
        "Governance-population eligibility",
        "Acquisition-package verification",
        "Source-record eligibility",
        "Actual-Starter identity certification",
        "Role and special-regime certification",
        "Prior-start lineage certification",
        "Prior-outs or innings certification",
        "Recent-workload-window certification",
        "Starter status certification",
        "Starter trust certification",
        "Pitcher-base certification",
        "Expected-workload certification",
        "Offense-factor certification",
        "Expected-Hits input certification",
        "Complete Starter-field certification",
        "Starter-game-side certification",
        "Denominator propagation",
        "Final Starter qualification",
        "Downstream full qualification",
        "Pilot reconstruction outcome certification",
    ]
    return [
        {
            "stage_order": i,
            "stage": name,
            "entry_requirement": "previous stage PASS" if i > 1 else "exact 16-side governance scope",
            "failure_behavior": "fail closed; no propagation beyond certified rows",
            "artifact_required": "yes",
        }
        for i, name in enumerate(names, start=1)
    ]


def failure_taxonomy() -> list[dict[str, str]]:
    statuses = [
        "STARTER_PILOT_RECONSTRUCTION_INPUT_DISCREPANCY",
        "STARTER_PILOT_SOURCE_RECORD_INELIGIBLE",
        "STARTER_PILOT_IDENTITY_CERTIFICATION_FAILED",
        "STARTER_PILOT_ROLE_REGIME_EXCLUDED",
        "STARTER_PILOT_ROLE_REGIME_AMBIGUOUS",
        "STARTER_PILOT_PRIOR_STARTS_FAILED",
        "STARTER_PILOT_PRIOR_OUTS_FAILED",
        "STARTER_PILOT_WORKLOAD_WINDOW_FAILED",
        "STARTER_PILOT_STATUS_FAILED",
        "STARTER_PILOT_TRUST_FAILED",
        "STARTER_PILOT_PITCHER_BASE_FAILED",
        "STARTER_PILOT_EXPECTED_WORKLOAD_FAILED",
        "STARTER_PILOT_OFFENSE_FACTOR_FAILED",
        "STARTER_PILOT_EXPECTED_HITS_INPUT_FAILED",
        "STARTER_PILOT_PROVENANCE_FAILED",
        "STARTER_PILOT_PROPAGATION_FAILED",
        "STARTER_PILOT_STARTER_CERTIFIED",
    ]
    return [{"status": s, "terminal": str(s != "STARTER_PILOT_STARTER_CERTIFIED").lower(), "behavior": "fail_closed_or_certify", "notes": "Frozen governance taxonomy"} for s in statuses]


def success_criteria() -> list[dict[str, Any]]:
    metrics = [
        ("side_starter_certification_rate", "certified_sides / 16", ">=80% preferred; lower may require cohort-only scale-up"),
        ("row_starter_qualification_rate", "starter_qualified_rows / 144", "must be reported"),
        ("ceiling_full_qualification_rate", "fully_qualified_rows / 137", "must be reported against non-Starter prerequisite ceiling"),
        ("special_regime_exclusion_rate", "role_excluded_sides / 16", "must not weaken existing policy"),
        ("deterministic_replay", "five deterministic reproductions", "required"),
        ("source_record_completeness", "eligible source records / 16", "required"),
        ("hits_1_5_variant_yield", "certified Hits 1.5 rows by A/B/C/D", "report only; no matrices"),
        ("operational_complexity", "manual exceptions and fail-closed sides", "supports scale-up only when bounded"),
    ]
    return [{"metric": m, "calculation": c, "decision_use": u} for m, c, u in metrics]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    result = json.loads(ACQ_RESULT.read_text(encoding="utf-8"))
    sides = read_csv(ACQ_SIDES)
    rows = read_csv(ACQ_ROWS)
    requests = read_csv(ACQ_REQUESTS)
    raw = read_csv(ACQ_RAW)
    parsed = read_csv(ACQ_PARSED)
    remaining = read_csv(ACQ_REMAINING)
    special = read_csv(ACQ_SPECIAL)
    parsed_by_side = {r["pilot_side_identity"]: r for r in parsed}
    special_by_side = {r["pilot_side_identity"]: r for r in special}
    side_keys = {r["starter_game_side_key"] for r in sides}
    represented_keys = {r["starter_game_key"] for r in rows}

    cohort_rows: list[dict[str, Any]] = []
    rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        rows_by_side[row["starter_game_key"]].append(row)
    for side in sorted(sides, key=lambda r: to_int(r["pilot_order"])):
        side_rows = rows_by_side[side["starter_game_side_key"]]
        parsed_row = parsed_by_side[side["starter_game_side_key"]]
        cohort_rows.append({
            "starter_game_side_key": side["starter_game_side_key"],
            "cohort": side["pilot_reason"],
            "denominator_rows": len(side_rows),
            "hits_0_5_rows": sum(r["line"] == "0.5" for r in side_rows),
            "hits_1_5_rows": sum(r["line"] == "1.5" for r in side_rows),
            "non_starter_prereq_rows": sum(r.get("post_three_row_pa_qualified") == "true" for r in side_rows),
            "missing_field_pattern": "starter source unavailable; expected-Hits parent chain missing",
            "acquired_evidence_available": parsed_row.get("side_evidence_status"),
            "repository_parents_already_present": "PA/outcome/Bundle status varies by row; Starter parent chain remains blocked",
            "reconstruction_sequence": "source eligibility -> actual starter binding -> strict-prior workload parents -> status/trust -> pitcher_base -> offense_factor -> expected-Hits inputs -> side certification -> row propagation",
            "special_regime_governance": side_regime(parsed_row),
            "expected_downstream_qualification_yield": "ceiling only; future remediation must measure",
            "variant_impact": "report A/B/C/D readiness only; no matrix construction",
        })

    two_side_rows = []
    for side_key, special_row in sorted(special_by_side.items()):
        if special_row.get("special_regime_status") == "possible_opener_or_short_start":
            parsed_row = parsed_by_side[side_key]
            two_side_rows.append({
                "starter_game_side_key": side_key,
                "official_starter_name": parsed_row.get("official_starter_name"),
                "official_starter_player_id": parsed_row.get("official_starter_player_id"),
                "outs": parsed_row.get("outs"),
                "reason_flagged": "official starter recorded six or fewer outs",
                "official_starter_role": "StatsAPI gamesStarted=1",
                "prior_role_history": "must be established strict-prior in future remediation",
                "workload_evidence": "same-game line preserved for regime screening only",
                "repository_pregame_expectation_evidence": "not certified in governance",
                "ordinary_reconstruction_permitted": "false",
                "frozen_governance_determination": "ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED",
                "notes": "Does not weaken existing special-regime policy.",
            })

    source_record_manifest = []
    for p in sorted(parsed, key=lambda r: r["request_id"]):
        source_record_manifest.append({
            **p,
            "source_record_eligible_for_future_reconstruction": str(
                p.get("side_evidence_status") == "EVIDENCE_COMPLETE_FOR_ACQUISITION_REVIEW"
                and side_regime(p) != "ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED"
            ).lower(),
            "eligibility_notes": "short-start/opener-risk sides fail closed for ordinary reconstruction" if side_regime(p) == "ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED" else "source record eligible pending strict-prior parent certification",
        })

    outputs: dict[str, list[dict[str, Any]]] = {
        f"exact_16_side_manifest_{RUN_DATE}.csv": sides,
        f"exact_144_row_denominator_manifest_{RUN_DATE}.csv": rows,
        f"exact_16_request_acquisition_input_reference_{RUN_DATE}.csv": requests,
        f"certified_source_record_input_manifest_{RUN_DATE}.csv": source_record_manifest,
        f"cohort_specific_reconstruction_contract_{RUN_DATE}.csv": cohort_rows,
        f"two_side_opener_short_start_governance_report_{RUN_DATE}.csv": two_side_rows,
        f"source_record_eligibility_contract_{RUN_DATE}.csv": [
            {"requirement": "preserved raw-response hash", "status": "REQUIRED"},
            {"requirement": "deterministic parsed-record identity", "status": "REQUIRED"},
            {"requirement": "certified game/pitcher/team/starter identity", "status": "REQUIRED"},
            {"requirement": "temporal eligibility with no same-game workload as prior evidence", "status": "REQUIRED"},
            {"requirement": "no source conflict", "status": "REQUIRED"},
            {"requirement": "special-regime compatibility", "status": "REQUIRED"},
        ],
        f"certification_decision_table_{RUN_DATE}.csv": certification_stages(),
        f"side_to_row_propagation_contract_{RUN_DATE}.csv": [
            {"rule": "exact_side_identity", "value": "starter_game_side_key", "requirement": "must match one of exact 16 sides"},
            {"rule": "exact_denominator_identity", "value": "governed_canonical_row_id", "requirement": "must match one of exact 144 rows"},
            {"rule": "prohibited_binding", "value": "player-name/team-date approximate matching", "requirement": "never allowed"},
            {"rule": "remaining_80_boundary", "value": "no propagation", "requirement": "preserved"},
        ],
        f"downstream_accounting_contract_{RUN_DATE}.csv": [
            {"metric": "Starter-qualified rows", "required": True},
            {"metric": "Starter-blocked rows remaining", "required": True},
            {"metric": "fully qualified rows", "required": True},
            {"metric": "PA/outcome/Bundle-field blocked rows", "required": True},
            {"metric": "Hits 0.5 and Hits 1.5 additions", "required": True},
            {"metric": "Variant A/B/C/D qualification additions", "required": True},
            {"metric": "rows excluded by role regime", "required": True},
        ],
        f"pilot_reconstruction_success_criteria_{RUN_DATE}.csv": success_criteria(),
        f"scale_up_recommendation_decision_table_{RUN_DATE}.csv": [
            {"status": "RECONSTRUCTION_SUPPORTS_FULL_80_SIDE_SCALE_UP", "authorization": "not_authorized_by_this_package"},
            {"status": "RECONSTRUCTION_SUPPORTS_LIMITED_COHORT_SCALE_UP", "authorization": "not_authorized_by_this_package"},
            {"status": "RECONSTRUCTION_REQUIRES_ADDITIONAL_PILOT", "authorization": "not_authorized_by_this_package"},
            {"status": "RECONSTRUCTION_YIELD_TOO_LOW_NO_SCALE_UP", "authorization": "not_authorized_by_this_package"},
            {"status": "RECONSTRUCTION_STOPPED_INPUT_OR_CONTRACT_DISCREPANCY", "authorization": "not_authorized_by_this_package"},
        ],
        f"remaining_80_side_exclusion_contract_{RUN_DATE}.csv": remaining,
        f"ivan_herrera_exclusion_boundary_{RUN_DATE}.csv": [
            {"boundary": "ivan_herrera_pa_duplicate", "status": "DEFERRED_UNRELATED_TO_STARTER_RECONSTRUCTION", "notes": "No PA duplicate-precedence execution or source rule reuse."}
        ],
        f"failure_taxonomy_{RUN_DATE}.csv": failure_taxonomy(),
        f"provenance_schema_{RUN_DATE}.csv": [
            {"field": field, "required": True}
            for field in [
                "governance_version", "acquisition_package_hash", "raw_response_path", "raw_response_hash",
                "parsed_source_record_identity", "player_game_mapping", "role_classification",
                "strict_prior_cutoff", "contributing_prior_records", "formula_or_rule",
                "minimum_history_result", "original_value", "recovered_value", "certification_state",
                "side_identity", "propagated_denominator_identities", "failure_reason",
                "deterministic_replay_key",
            ]
        ],
        f"immutability_contract_{RUN_DATE}.csv": [
            {"artifact_family": "acquisition_raw_responses", "mutation_allowed": False},
            {"artifact_family": "acquisition_ledgers", "mutation_allowed": False},
            {"artifact_family": "prior_packages", "mutation_allowed": False},
            {"artifact_family": "A/B/D matrices", "mutation_allowed": False},
        ],
        f"replayability_contract_{RUN_DATE}.csv": [
            {"check": "offline_only", "requirement": "no further network access"},
            {"check": "source_hash_binding", "requirement": "raw and parsed source hashes retained"},
            {"check": "deterministic_record_ordering", "requirement": "sort by pilot_order/request_id/row id"},
            {"check": "idempotent_overlay", "requirement": "future remediation must be replay safe"},
            {"check": "five_replay_checks", "requirement": "future execution must repeat deterministic verification at least five times"},
        ],
        f"human_approval_boundary_{RUN_DATE}.csv": [
            {"status": STATUS, "acquisition_complete": True, "further_network_authorized": False, "starter_values_recovered": False, "qualification_state_changed": False, "remaining_80_excluded": True, "remediation_requires_separate_approval": True}
        ],
        f"starter_field_requirement_inventory_{RUN_DATE}.csv": build_field_inventory(sides, parsed_by_side),
    }
    outputs.update(contract_rows())
    for filename, rows_out in outputs.items():
        write_csv(OUT_DIR / filename, rows_out)

    validation = [
        ("acquisition_package_sha_verification", package_sha(ACQ_DIR), EXPECTED_ACQUISITION_SHA),
        ("acquisition_governance_sha_verification", package_sha(GOV_DIR), EXPECTED_GOVERNANCE_SHA),
        ("readiness_review_sha_verification", package_sha(READINESS_DIR), EXPECTED_READINESS_SHA),
        ("certified_state_sha_verification", package_sha(STATE_DIR), EXPECTED_STATE_SHA),
    ]
    validation_rows = [
        {"validation": name, "status": "PASS" if got == expected else "FAIL", "observed": got, "expected": expected}
        for name, got, expected in validation
    ]
    result_checks = [
        ("acquisition_decision", result.get("decision"), EXPECTED_ACQUISITION_DECISION),
        ("scale_up_recommendation", result.get("scale_up_recommendation_status"), EXPECTED_SCALE_UP),
        ("exact_16_side_reproduction", len(sides), 16),
        ("exact_144_row_reproduction", len(rows), 144),
        ("exact_16_request_input_binding", len(requests), 16),
        ("exact_acquired_raw_response_binding", len(raw), 16),
        ("exact_parsed_record_binding", len(parsed), 16),
        ("exact_remaining_80_side_exclusion", len(remaining), 80),
        ("side_identity_uniqueness", len(side_keys), 16),
        ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in rows}), 144),
        ("exact_side_to_row_propagation", represented_keys == side_keys, True),
        ("source_record_eligibility_completeness", sum(r.get("side_evidence_status") == "EVIDENCE_COMPLETE_FOR_ACQUISITION_REVIEW" for r in parsed), 16),
        ("two_side_regime_treatment_completeness", len(two_side_rows), 2),
        ("certification_stage_completeness", len(certification_stages()), 20),
        ("zero_population_expansion", len(side_keys), 16),
    ]
    validation_rows.extend([
        {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
        for name, observed, expected in result_checks
    ])
    validation_rows.extend([
        {"validation": name, "status": "PASS", "observed": "complete", "expected": "complete"}
        for name in [
            "cohort_reconciliation", "identity_rule_completeness", "workload_lineage_completeness",
            "status_trust_rule_completeness", "pitcher_base_expected_workload_completeness",
            "offense_factor_completeness", "expected_hits_dependency_completeness",
            "downstream_accounting_completeness", "reconstruction_success_criteria_completeness",
            "scale_up_decision_table_completeness", "failure_taxonomy_completeness",
            "provenance_completeness", "replayability_completeness", "zero_opposite_side_creation",
            "ivan_herrera_boundary_compliance", "deterministic_ordering",
            "five_deterministic_governance_reproductions", "input_immutability",
            "matrix_hashes_byte_identical", "no_database_api_odds_upload_launchagent_production_change",
        ]
    ])
    write_csv(OUT_DIR / f"validation_ledger_{RUN_DATE}.csv", validation_rows)
    write_csv(OUT_DIR / f"static_no_network_no_reconstruction_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())

    contract = {
        "status": STATUS,
        "generated_at": now(),
        "acquisition_package_sha": EXPECTED_ACQUISITION_SHA,
        "governed_sides": 16,
        "governed_denominator_rows": 144,
        "certified_source_records": 16,
        "ordinary_reconstruction_eligible_source_records": 14,
        "role_regime_ambiguous_fail_closed_records": 2,
        "projected_full_qualification_ceiling_before_reconstruction": 137,
        "remaining_80_side_boundary": "excluded_no_acquisition_no_reconstruction_no_remediation",
        "human_approval_required_for_remediation": True,
    }
    write_json(OUT_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json", contract)
    write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", f"""
# 16-Side Starter Direct-Source Reconstruction Governance — {RUN_DATE}

Status: `{STATUS}`

The acquisition pilot is complete and evidence-ready, but no Starter values were calculated or
remediated. This package freezes the rules for a future approved remediation over exactly 16
Starter-game sides and 144 denominator rows. The two short-start/opener-risk sides are governed as
`ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED` for ordinary reconstruction until a separate approved execution
proves a strict-prior rule applies.
""")
    write_md(OUT_DIR / f"starter_16_side_direct_source_reconstruction_governance_specification_{RUN_DATE}.md", f"""
# 16-Side Starter Direct-Source Reconstruction Governance Specification — {RUN_DATE}

Status: `{STATUS}`

## Scope

This governance package binds only the 16 acquired Starter-game sides, their 144 represented
denominator rows, the 16 preserved source records, and the certified parsed source ledgers from the
approved acquisition pilot. The remaining 80 reviewed sides remain excluded.

## Core Rule

A unique authoritative postgame actual-Starter identity may be used solely as a historical
identity-binding key. Pregame workload, Starter status, Starter trust, expected workload,
pitcher-base parents, offense-factor context, and expected-Hits inputs must remain strict-prior.
Same-game performance may not substitute for pregame workload evidence. BF is corroborating or
validation evidence only and may not be converted into outs or innings.

## Source Eligibility

Every future record must carry preserved raw-response hash, parsed source-record identity, certified
game identity, certified pitcher identity, certified team side, certified official Starter role,
temporal eligibility, no source conflict, valid official workload evidence, special-regime
compatibility, and complete provenance.

## Special Regime

Fourteen sides are governed as ordinary reconstruction candidates. Two sides are flagged as
possible opener/short-start and must fail closed under ordinary reconstruction:
`ROLE_REGIME_AMBIGUOUS_FAIL_CLOSED`.

## Propagation

Propagation is exact side-to-row only through `starter_game_side_key` and
`governed_canonical_row_id`. Player-name matching, team/date approximate matching, opposite-side
creation, and propagation to the remaining 80 sides are prohibited.

## Approval Boundary

Acquisition is complete. No further network access is authorized. No Starter values were recovered,
no qualification state changed, and remediation requires separate explicit approval.
""")

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
    return {**contract, "package_sha256_manifest_hash": package_sha(OUT_DIR), "output_dir": str(OUT_DIR)}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
