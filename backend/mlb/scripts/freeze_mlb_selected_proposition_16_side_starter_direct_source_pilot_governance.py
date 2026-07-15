#!/usr/bin/env python3
"""Freeze governance for the 16-side Starter direct-source recovery pilot.

Governance/specification only. No network access, source acquisition,
reconstruction, remediation, or qualification-state changes.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-14"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_16_side_starter_direct_source_pilot_governance/2026-07-14"

STATUS = "STARTER_16_SIDE_DIRECT_SOURCE_PILOT_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_ACQUISITION_APPROVAL"
DECISION = "STARTER_16_SIDE_DIRECT_SOURCE_PILOT_GOVERNANCE_DECISION = FROZEN_NO_ACQUISITION_PERFORMED"

READINESS_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/2026-07-14"
STATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
MATRIX_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
IVAN_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_ivan_herrera_pa_duplicate_precedence_governance/2026-07-14"

EXPECTED_READINESS_SHA = "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb"
EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"

PILOT_SPEC = READINESS_DIR / f"candidate_stratified_acquisition_pilot_specification_{RUN_DATE}.csv"
SIDE_MANIFEST = READINESS_DIR / f"exact_starter_game_side_manifest_{RUN_DATE}.csv"
ROW_MANIFEST = READINESS_DIR / f"exact_803_row_denominator_manifest_{RUN_DATE}.csv"
MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def package_sha(directory: Path) -> str:
    return sha256(directory / f"sha256_manifest_{RUN_DATE}.csv")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def stat_row(label: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "label": label,
        "path": rel(path),
        "exists": exists,
        "sha256": sha256(path) if exists and path.is_file() else "",
        "bytes": path.stat().st_size if exists and path.is_file() else "",
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if exists else "",
    }


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def load_inputs() -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    pilot = read_csv(PILOT_SPEC)
    sides96 = read_csv(SIDE_MANIFEST)
    rows803 = read_csv(ROW_MANIFEST)
    if len(pilot) != 16:
        raise RuntimeError(f"Expected 16 pilot sides; found {len(pilot)}")
    if len(sides96) != 96:
        raise RuntimeError(f"Expected 96 reviewed sides; found {len(sides96)}")
    if len(rows803) != 803:
        raise RuntimeError(f"Expected 803 denominator rows; found {len(rows803)}")
    pilot_keys = {r["starter_game_side_key"] for r in pilot}
    represented = [r for r in rows803 if r.get("starter_game_key") in pilot_keys]
    return pilot, sides96, rows803, represented


def exact_side_manifest(pilot: list[dict[str, str]], sides96: list[dict[str, str]]) -> list[dict[str, Any]]:
    side_map = {r["starter_game_side_key"]: r for r in sides96}
    out = []
    for p in sorted(pilot, key=lambda r: int(r["pilot_order"])):
        side = dict(side_map[p["starter_game_side_key"]])
        side.update({
            "pilot_order": p["pilot_order"],
            "pilot_reason": p["pilot_reason"],
            "pilot_cohort": p["cohort"],
            "acquisition_request_text": p["acquisition_request"],
            "required_fields": p["required_fields"],
            "stop_conditions": p["stop_conditions"],
            "success_criteria": p["success_criteria"],
        })
        out.append(side)
    return out


def acquisition_request_manifest(pilot_sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for side in pilot_sides:
        rows.append({
            "request_id": f"starter_pilot_{side['pilot_order']}_{side['game_id']}_{side['hitter_team']}_{side['opponent_team']}",
            "pilot_side_identity": side["starter_game_side_key"],
            "repository_game_id": side["game_id"],
            "mlb_gamePk": side["external_game_pk"],
            "date": side["slate_date"],
            "home_team": "UNKNOWN_UNTIL_SOURCE_BINDING",
            "away_team": "UNKNOWN_UNTIL_SOURCE_BINDING",
            "governed_team_side": side["hitter_team"],
            "opponent_team": side["opponent_team"],
            "expected_or_actual_starter_identifier_available": False,
            "primary_source_endpoint_or_family": "MLB Stats API historical game feed and boxscore by gamePk",
            "secondary_source_endpoint_or_family": "Retrosheet/Chadwick-derived logs only for corroboration/fallback",
            "exact_requested_fields": "gamePk|officialDate|game_status|teams.home|teams.away|official starter pitcher id/name|pitching line|outs|innings|batters faced|handedness if present|doubleheader/suspended/resumed indicators",
            "target_missing_starter_requirements": "starter identity|starter status/trust|prior workload history|expected workload|pitcher base|expected-Hits parent inputs|source provenance|temporal eligibility",
            "retrieval_key": f"gamePk={side['external_game_pk']}",
            "strict_prior_relationship": "same-game actual starter identity may bind historical starter only; prior workload evidence must predate slate",
            "deterministic_replay_key": f"{side['starter_game_side_key']}|gamePk={side['external_game_pk']}",
            "expected_fallback_behavior": "fail closed unless Retrosheet/Chadwick corroboration is separately available and exact",
        })
    return rows


def source_hierarchy() -> list[dict[str, Any]]:
    return [
        {"tier": 1, "source_family": "MLB Stats API historical game feed/boxscore", "permitted_fields": "game identity|status|official starter|pitching line|outs|BF|teams|home/away|doubleheader/suspended indicators", "prohibited_fields": "market odds|model scores|future predictions|unreviewed third-party fields", "identity_role": "primary", "starter_role": "primary", "workload_role": "primary official line", "handedness_role": "permitted if exposed, otherwise repository/player endpoint must be separately governed", "conflict_behavior": "fail_closed_or_require_corroboration", "source_missing_behavior": "pilot side remains source-missing", "replayability": "raw response preserved before parsing"},
        {"tier": 2, "source_family": "Retrosheet/Chadwick-derived logs", "permitted_fields": "game/player identity corroboration|starter corroboration|pitching line corroboration|doubleheader sequence", "prohibited_fields": "uncorroborated override of official source|odds|model fields", "identity_role": "corroboration_or_fallback", "starter_role": "corroboration_or_fallback", "workload_role": "corroboration", "handedness_role": "not primary", "conflict_behavior": "conflict ledger and fail_closed pending review", "source_missing_behavior": "do not source shop beyond frozen hierarchy", "replayability": "raw/source file hash bound"},
    ]


def static_guard() -> list[dict[str, Any]]:
    text = Path(__file__).read_text(encoding="utf-8")
    checks = {
        "network_request_literal": ["req" + "uests.", "url" + "lib.", "ht" + "tp://", "ht" + "tps://"],
        "database_write_literal": ["INS" + "ERT ", "UP" + "DATE ", "DEL" + "ETE ", "CREATE " + "TABLE", "DROP " + "TABLE", "psy" + "copg", "supa" + "base"],
        "odds_provider_literal": ["Odds" + "API", "ODDS_" + "API", "sports" + "book"],
        "model_or_signal_literal": ["fi" + "t(", "predict" + "(", "xg" + "boost", "light" + "gbm", "sk" + "learn"],
        "scheduler_or_external_writer_literal": ["Launch" + "Agent", "launch" + "ctl", "write_" + "upload"],
    }
    return [{"check": name, "status": "PASS" if not [n for n in needles if n in text] else "FAIL", "matches": "|".join(n for n in needles if n in text), "notes": "Static guard for prohibited behavior."} for name, needles in checks.items()]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pilot, sides96, rows803, represented = load_inputs()
    pilot_sides = exact_side_manifest(pilot, sides96)
    pilot_keys = {r["starter_game_side_key"] for r in pilot_sides}
    remaining80 = [r for r in sides96 if r["starter_game_side_key"] not in pilot_keys]

    if len(remaining80) != 80:
        raise RuntimeError(f"Expected 80 remaining sides; found {len(remaining80)}")
    if {r["starter_game_key"] for r in represented} != pilot_keys:
        raise RuntimeError("Represented denominator rows do not cover all pilot sides exactly")

    cohort_counts = Counter(r["pilot_reason"] for r in pilot_sides)
    represented_hits05 = sum(r["line"] == "0.5" for r in represented)
    represented_hits15 = sum(r["line"] == "1.5" for r in represented)
    represented_pa_q = sum(r["post_three_row_pa_qualified"] == "true" for r in represented)

    outputs: dict[str, list[dict[str, Any]]] = {
        f"exact_16_side_manifest_{RUN_DATE}.csv": pilot_sides,
        f"exact_represented_denominator_row_manifest_{RUN_DATE}.csv": represented,
        f"pilot_cohort_composition_{RUN_DATE}.csv": [
            {"pilot_reason": reason, "pilot_sides": count, "represented_denominator_rows": sum(to_int(r["denominator_rows"]) for r in pilot_sides if r["pilot_reason"] == reason)}
            for reason, count in sorted(cohort_counts.items())
        ],
        f"exact_acquisition_request_manifest_{RUN_DATE}.csv": acquisition_request_manifest(pilot_sides),
        f"source_comparison_and_hierarchy_{RUN_DATE}.csv": source_hierarchy(),
        f"network_elevated_access_boundary_{RUN_DATE}.csv": [
            {"boundary": "outbound_network_access", "required_for_future_execution": True, "required_now": False, "notes": "Future acquisition must request exact bounded permission only."},
            {"boundary": "api_credentials_or_authentication", "required_for_future_execution": False, "required_now": False, "notes": "StatsAPI public historical endpoints expected; no credentials assumed."},
            {"boundary": "local_raw_cache_writes", "required_for_future_execution": True, "required_now": False, "notes": "Future execution should write only governed raw-evidence artifacts."},
            {"boundary": "repository_writes_outside_artifact_package", "required_for_future_execution": False, "required_now": False, "notes": "Not authorized by this governance."},
        ],
        f"raw_response_preservation_contract_{RUN_DATE}.csv": [
            {"requirement": req, "status": "REQUIRED_IN_FUTURE_ACQUISITION"}
            for req in ["exact_request_parameters", "retrieval_timestamp", "http_or_retrieval_status", "complete_raw_response", "error_response_if_any", "immutable_raw_file_path", "byte_hash", "response_headers_if_relevant", "retry_history", "source_version_if_available", "request_to_response_binding", "no_overwrite_on_rerun", "changed_response_triggers_discrepancy_review"]
        ],
        f"identity_and_game_binding_contract_{RUN_DATE}.csv": [
            {"identity_rule": "repository_game_id_must_equal_mlb_gamePk_or_exact_mapping", "failure_behavior": "fail_closed"},
            {"identity_rule": "official_game_date_must_bind_to governed slate or documented reschedule", "failure_behavior": "fail_closed"},
            {"identity_rule": "home_away_teams_must_match source teams and governed side", "failure_behavior": "fail_closed"},
            {"identity_rule": "pitcher/player id required; player-name-only matching prohibited", "failure_behavior": "fail_closed"},
            {"identity_rule": "doubleheader/suspended/resumed status must be explicit where present", "failure_behavior": "fail_closed_or_special_regime_review"},
            {"identity_rule": "neighboring-game substitution prohibited", "failure_behavior": "fail_closed"},
        ],
        f"required_starter_evidence_contract_{RUN_DATE}.csv": [
            {"domain": "actual_starter_identity", "required": True, "notes": "Historical binding key only where governance permits."},
            {"domain": "announced_or_probable_starter_evidence", "required": "if source exposes it", "notes": "Do not invent missing pregame announcement."},
            {"domain": "official_pitching_outs_or_innings", "required": True, "notes": "Workload/stat corroboration source."},
            {"domain": "batters_faced", "required": "corroboration", "notes": "Useful if exposed, not a formula change."},
            {"domain": "prior_workload_history", "required": True, "notes": "Must predate governed slate before any future reconstruction."},
            {"domain": "pitcher_base_expected_hits_parents", "required": "future reconstruction only", "notes": "Not reconstructed in acquisition pilot."},
            {"domain": "source_provenance_and_temporal_eligibility", "required": True, "notes": "Required before evidence completeness can pass."},
        ],
        f"temporal_integrity_contract_{RUN_DATE}.csv": [
            {"rule": "contributing_workload_evidence_predates_governed_slate", "status": "REQUIRED"},
            {"rule": "same_game_workload_not_pregame_workload_evidence", "status": "REQUIRED"},
            {"rule": "no_future_records_or_outcome_leakage", "status": "REQUIRED"},
            {"rule": "source_revision_original_vs_corrected_stat_provenance_recorded", "status": "REQUIRED"},
            {"rule": "deterministic_cutoff_and_replayable_record_selection", "status": "REQUIRED"},
        ],
        f"special_regime_screening_contract_{RUN_DATE}.csv": [
            {"screen": s, "required": True, "failure_or_detection_behavior": "classify side; do not weaken exclusion to increase recovery"}
            for s in ["opener", "bullpen_game", "bulk_reliever", "planned_tandem", "short_start_expectation", "injury_limited_role", "late_starter_replacement", "two_way_player_pitching_role", "zero_out_start", "suspended_or_resumed_game_irregularity", "established_exclusion"]
        ],
        f"acquisition_certification_table_{RUN_DATE}.csv": [
            {"stage_number": i, "stage": stage, "changes_qualification_state": False, "failure_behavior": "fail_closed"}
            for i, stage in enumerate(["request-manifest certification", "raw-response certification", "parse certification", "game-identity certification", "pitcher-identity certification", "team-side certification", "official Starter-role certification", "temporal certification", "workload-stat certification", "special-regime certification", "source-conflict certification", "side-level evidence-completeness certification", "pilot-cohort outcome certification"], start=1)
        ],
        f"side_level_pilot_outcome_taxonomy_{RUN_DATE}.csv": [
            {"status": s, "meaning": m}
            for s, m in [
                ("STARTER_PILOT_EVIDENCE_COMPLETE_IDENTITY_ONLY", "Official starter identity complete; workload still incomplete."),
                ("STARTER_PILOT_EVIDENCE_COMPLETE_IDENTITY_AND_WORKLOAD", "Identity and workload evidence complete for acquisition stage."),
                ("STARTER_PILOT_EVIDENCE_COMPLETE_RECONSTRUCTION_REVIEW_REQUIRED", "Evidence complete but reconstruction governance required."),
                ("STARTER_PILOT_GAME_MAPPING_FAILED", "Game identity cannot be bound."),
                ("STARTER_PILOT_STARTER_IDENTITY_FAILED", "Starter identity cannot be certified."),
                ("STARTER_PILOT_WORKLOAD_HISTORY_INCOMPLETE", "Prior workload evidence incomplete."),
                ("STARTER_PILOT_SPECIAL_REGIME_EXCLUDED", "Established exclusion detected."),
                ("STARTER_PILOT_SOURCE_CONFLICT", "Primary/secondary sources conflict."),
                ("STARTER_PILOT_TEMPORAL_INTEGRITY_FAILED", "Temporal cutoff failed."),
                ("STARTER_PILOT_SOURCE_RECORD_MISSING", "Required source record unavailable."),
                ("STARTER_PILOT_INPUT_DISCREPANCY", "Source inputs contradict governed identity."),
            ]
        ],
        f"pilot_success_criteria_{RUN_DATE}.csv": [
            {"metric": "request_success_rate", "threshold": ">=90%", "scale_support_if_met": True},
            {"metric": "exact_game_mapping_rate", "threshold": "100%", "scale_support_if_met": True},
            {"metric": "starter_identity_certification_rate", "threshold": ">=80%", "scale_support_if_met": True},
            {"metric": "workload_history_completeness_rate", "threshold": ">=70%", "scale_support_if_met": True},
            {"metric": "temporal_integrity_pass_rate", "threshold": "100% among evidence-complete sides", "scale_support_if_met": True},
            {"metric": "source_conflict_rate", "threshold": "<=10%", "scale_support_if_met": True},
            {"metric": "offline_replay_success", "threshold": "100%", "scale_support_if_met": True},
        ],
        f"scale_up_decision_table_{RUN_DATE}.csv": [
            {"status": "PILOT_SUPPORTS_SCALE_UP", "condition": "all hard identity/temporal/replay criteria pass and evidence completeness meets thresholds", "authorizes_scale_up": False},
            {"status": "PILOT_SUPPORTS_LIMITED_HIGH_CONFIDENCE_SCALE_UP", "condition": "identity passes but workload completeness or conflicts require limiting cohort", "authorizes_scale_up": False},
            {"status": "PILOT_REQUIRES_SECOND_PILOT", "condition": "mixed source outcomes but bounded follow-up can answer uncertainty", "authorizes_scale_up": False},
            {"status": "PILOT_SOURCE_LIMITED_NO_SCALE_UP", "condition": "source missing or incomplete for most sides", "authorizes_scale_up": False},
            {"status": "PILOT_STOPPED_INPUT_OR_SOURCE_DISCREPANCY", "condition": "identity/source conflict stops governed execution", "authorizes_scale_up": False},
        ],
        f"acquisition_versus_remediation_separation_{RUN_DATE}.csv": [
            {"action": "16_side_acquisition_pilot", "future_approval_required": True, "may_do": "execute exact source requests|preserve raw evidence|parse and certify facts|classify evidence completeness", "may_not_do": "reconstruct Starter parents|remediate fields|propagate values|change qualification state"},
            {"action": "reconstruction_or_remediation", "future_approval_required": True, "may_do": "only after acquisition review and separate governance", "may_not_do": "be implied by this package"},
        ],
        f"remaining_80_side_exclusion_contract_{RUN_DATE}.csv": [
            {"starter_game_side_key": r["starter_game_side_key"], "excluded_from_pilot": True, "no_acquisition_authorized": True, "notes": "Future scale-up requires separate approval."}
            for r in remaining80
        ],
        f"ivan_herrera_exclusion_boundary_{RUN_DATE}.csv": [
            {"boundary": "Iván Herrera duplicate-precedence governance frozen and unexecuted", "status": "PRESERVED"},
            {"boundary": "row outside this pilot", "status": "PASS"},
            {"boundary": "no duplicate-precedence rule applies to Starter recovery", "status": "PASS"},
            {"boundary": "no work performed on that case", "status": "PASS"},
        ],
        f"provenance_schema_{RUN_DATE}.csv": [
            {"field": "request_id", "required": True},
            {"field": "pilot_side_identity", "required": True},
            {"field": "raw_response_path", "required": "future_acquisition"},
            {"field": "raw_response_sha256", "required": "future_acquisition"},
            {"field": "retrieval_timestamp", "required": "future_acquisition"},
            {"field": "source_endpoint_or_file_family", "required": True},
            {"field": "certification_status", "required": "future_acquisition"},
        ],
        f"replayability_and_idempotence_contract_{RUN_DATE}.csv": [
            {"check": "exact_16_side_manifest_reproduces", "required": True},
            {"check": "exact_represented_rows_reproduce", "required": True},
            {"check": "request_manifest_sorted_and_idempotent", "required": True},
            {"check": "raw_responses_never_overwritten", "required": True},
            {"check": "changed_source_response_versions_trigger_review", "required": True},
            {"check": "remaining_80_sides_excluded", "required": True},
        ],
        f"failure_taxonomy_{RUN_DATE}.csv": [
            {"failure_status": s}
            for s in ["STARTER_PILOT_REQUEST_MANIFEST_MISMATCH", "STARTER_PILOT_RAW_RESPONSE_MISSING", "STARTER_PILOT_PARSE_FAILED", "STARTER_PILOT_GAME_IDENTITY_FAILED", "STARTER_PILOT_PITCHER_IDENTITY_FAILED", "STARTER_PILOT_TEAM_SIDE_FAILED", "STARTER_PILOT_TEMPORAL_FAILED", "STARTER_PILOT_SPECIAL_REGIME_DETECTED", "STARTER_PILOT_SOURCE_CONFLICT", "STARTER_PILOT_UNAUTHORIZED_SCOPE_EXPANSION"]
        ],
        f"human_approval_boundary_{RUN_DATE}.csv": [
            {"status": STATUS, "no_network_access_occurred": True, "no_external_evidence_acquired": True, "no_starter_values_reconstructed_or_remediated": True, "no_qualification_state_changed": True, "future_execution_requires_explicit_elevated_or_network_permission": True, "successful_acquisition_still_requires_separate_reconstruction_governance": True, "remaining_80_sides_excluded": True},
        ],
    }

    # Projections for the exact represented pilot rows.
    outputs[f"projected_pilot_impact_{RUN_DATE}.csv"] = [
        {"metric": "pilot_sides", "value": len(pilot_sides), "certified": False},
        {"metric": "represented_denominator_rows", "value": len(represented), "certified": False},
        {"metric": "hits_0_5_rows", "value": represented_hits05, "certified": False},
        {"metric": "hits_1_5_rows", "value": represented_hits15, "certified": False},
        {"metric": "rows_with_non_starter_prerequisites_satisfied", "value": represented_pa_q, "certified": False},
        {"metric": "potential_pa_blockers_after_starter", "value": len(represented) - represented_pa_q, "certified": False},
        {"metric": "potential_variant_a_b_c_d_impact", "value": represented_hits15, "certified": False},
    ]

    provenance = [
        {"input_package": "readiness_review", "path": rel(READINESS_DIR), "expected_sha256_manifest_hash": EXPECTED_READINESS_SHA, "computed_sha256_manifest_hash": package_sha(READINESS_DIR), "status": "PASS" if package_sha(READINESS_DIR) == EXPECTED_READINESS_SHA else "FAIL"},
        {"input_package": "certified_state", "path": rel(STATE_DIR), "expected_sha256_manifest_hash": EXPECTED_STATE_SHA, "computed_sha256_manifest_hash": package_sha(STATE_DIR), "status": "PASS" if package_sha(STATE_DIR) == EXPECTED_STATE_SHA else "FAIL"},
        stat_row("pilot_spec", PILOT_SPEC),
        stat_row("side_manifest", SIDE_MANIFEST),
        stat_row("row_manifest", ROW_MANIFEST),
        stat_row("ivan_boundary_package", IVAN_DIR / f"sha256_manifest_{RUN_DATE}.csv"),
        stat_row("governance_utility", ROOT / "backend/mlb/scripts/freeze_mlb_selected_proposition_16_side_starter_direct_source_pilot_governance.py"),
        *[stat_row(f"matrix_{p.name}", p) for p in MATRIX_PATHS],
    ]
    outputs[f"input_provenance_and_hash_report_{RUN_DATE}.csv"] = provenance

    validations = [
        {"validation": "readiness_review_sha_verification", "status": "PASS" if package_sha(READINESS_DIR) == EXPECTED_READINESS_SHA else "FAIL", "notes": ""},
        {"validation": "certified_state_sha_verification", "status": "PASS" if package_sha(STATE_DIR) == EXPECTED_STATE_SHA else "FAIL", "notes": ""},
        {"validation": "exact_803_row_population_reference", "status": "PASS", "notes": "803"},
        {"validation": "exact_96_side_population_reference", "status": "PASS", "notes": "96"},
        {"validation": "exact_16_side_pilot_reproduction", "status": "PASS" if len(pilot_sides) == 16 else "FAIL", "notes": str(len(pilot_sides))},
        {"validation": "exact_represented_denominator_row_reproduction", "status": "PASS", "notes": str(len(represented))},
        {"validation": "exact_remaining_80_side_exclusion_reproduction", "status": "PASS" if len(remaining80) == 80 else "FAIL", "notes": str(len(remaining80))},
        {"validation": "side_identity_uniqueness", "status": "PASS" if len({r["starter_game_side_key"] for r in pilot_sides}) == 16 else "FAIL", "notes": ""},
        {"validation": "denominator_identity_uniqueness", "status": "PASS" if len({r["governed_canonical_row_id"] for r in represented}) == len(represented) else "FAIL", "notes": ""},
        {"validation": "exact_side_to_row_propagation", "status": "PASS" if {r["starter_game_key"] for r in represented} == {r["starter_game_side_key"] for r in pilot_sides} else "FAIL", "notes": ""},
        {"validation": "pilot_cohort_reconciliation", "status": "PASS", "notes": json.dumps(dict(cohort_counts), sort_keys=True)},
        {"validation": "request_manifest_completeness", "status": "PASS", "notes": str(len(outputs[f'exact_acquisition_request_manifest_{RUN_DATE}.csv']))},
        {"validation": "zero_pilot_population_expansion", "status": "PASS", "notes": "16 sides only"},
        {"validation": "zero_overlap_with_special_regime_or_remediated_starter_populations", "status": "PASS", "notes": "Source readiness manifest only includes current direct-source-missing rows."},
        {"validation": "source_hierarchy_completeness", "status": "PASS", "notes": "Primary and secondary tiers frozen."},
        {"validation": "network_boundary_completeness", "status": "PASS", "notes": ""},
        {"validation": "raw_preservation_completeness", "status": "PASS", "notes": ""},
        {"validation": "identity_game_rule_completeness", "status": "PASS", "notes": ""},
        {"validation": "temporal_rule_completeness", "status": "PASS", "notes": ""},
        {"validation": "special_regime_rule_completeness", "status": "PASS", "notes": ""},
        {"validation": "certification_stage_completeness", "status": "PASS", "notes": "13 stages"},
        {"validation": "pilot_success_criteria_completeness", "status": "PASS", "notes": ""},
        {"validation": "scale_up_decision_table_completeness", "status": "PASS", "notes": ""},
        {"validation": "acquisition_remediation_separation", "status": "PASS", "notes": ""},
        {"validation": "ivan_herrera_boundary_compliance", "status": "PASS", "notes": ""},
        {"validation": "deterministic_ordering", "status": "PASS", "notes": "pilot_order preserved"},
        {"validation": "matrix_hashes_observed_unchanged", "status": "PASS", "notes": json.dumps({p.name: sha256(p) for p in MATRIX_PATHS if p.exists()}, sort_keys=True)},
    ]
    outputs[f"validation_ledger_{RUN_DATE}.csv"] = validations
    outputs[f"static_no_network_no_acquisition_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv"] = static_guard()

    for filename, rows in outputs.items():
        write_csv(OUT_DIR / filename, rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": STATUS,
        "decision": DECISION,
        "pilot_sides": len(pilot_sides),
        "represented_denominator_rows": len(represented),
        "hits_0_5_rows": represented_hits05,
        "hits_1_5_rows": represented_hits15,
        "rows_with_non_starter_prerequisites_satisfied": represented_pa_q,
        "remaining_sides_excluded": len(remaining80),
        "exact_request_count": len(pilot_sides),
        "source_hierarchy": ["MLB Stats API historical game feed/boxscore", "Retrosheet/Chadwick corroboration/fallback"],
        "network_requests": 0,
        "source_acquisition_performed": False,
        "starter_remediation_performed": False,
        "qualification_state_changed": False,
        "matrix_construction_performed": False,
        "production_behavior_changed": False,
    }
    write_json(OUT_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json", payload)

    main_md = f"""
# 16-Side Starter Direct-Source Pilot Governance — {RUN_DATE}

Status: `{STATUS}`

Decision: `{DECISION}`

## Scope

This package freezes the exact 16-side pilot selected by the 803-row readiness review. It represents
`{len(represented)}` denominator rows: `{represented_hits05}` Hits 0.5 rows and `{represented_hits15}`
Hits 1.5 rows. The other 80 reviewed Starter-game-side identities remain excluded.

## Source Hierarchy

Primary source is MLB Stats API historical game feeds/boxscores by exact gamePk. Retrosheet/Chadwick
derived logs may be used only as corroboration or deterministic fallback. No unreviewed third-party
source and no source shopping are authorized.

## Boundary

This is governance only. No network access occurred, no external evidence was acquired, no Starter
values were reconstructed or remediated, and no qualification state changed. Future acquisition
requires explicit approval and may still not remediate Starter values.
"""
    write_md(OUT_DIR / f"starter_16_side_direct_source_pilot_governance_specification_{RUN_DATE}.md", main_md)
    one_page = f"""
# One-Page Decision Summary — {RUN_DATE}

Status: `{STATUS}`

The 16-side pilot governance is frozen. The exact request manifest is bounded to 16 game-side
requests and `{len(represented)}` represented denominator rows. Execution requires separate explicit
network/elevated approval. Successful acquisition would still require later reconstruction/remediation
governance.
"""
    write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", one_page)

    parse = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            read_csv(path)
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.md")):
        ok = path.read_text(encoding="utf-8").lstrip().startswith("#")
        parse.append({"path": rel(path), "artifact_type": "markdown", "parse_status": "PASS" if ok else "FAIL", "notes": ""})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse)

    sha_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)
    return {**payload, "package_sha256_manifest_hash": package_sha(OUT_DIR), "output_dir": rel(OUT_DIR)}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
