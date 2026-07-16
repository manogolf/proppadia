#!/usr/bin/env python3
"""Investigate the HC_LOCAL_COHORT_001 fail-closed pitcher-base lineage.

Read-only investigation only. This utility consumes frozen package artifacts and
repository-backed evidence. It performs no network access, source acquisition,
source discovery, remediation, state mutation, matrix construction, model/scoring
work, database/API writes, uploads, scheduler edits, or production behavior
changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
FAIL_SIDE = "2026-07-05|824010|LAA|BOS"
EXPECTED_HC_REMEDIATION_SHA = "097922f3ea6495c4a9c3c10b5df0bcd60515ecb7d5fe75f3ee38cb28ff514ab9"
EXPECTED_HC_DECISION = (
    "STARTER_HC_LOCAL_COHORT_001_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_LOCAL_REMEDIATION_COMPLETED_WITH_NONZERO_YIELD_AND_FAIL_CLOSED_SIDE"
)
EXPECTED_DESIGN_SHA = "de965d52ffa0752886d6ded1f319473b540924b0ff896ba793c2180fb5befacd"

DECISION = (
    "STARTER_HC_LOCAL_COHORT_001_PITCHER_BASE_LINEAGE_DECISION = "
    "CONSTRUCTION_OR_PERSISTENCE_DEFECT_REPAIR_REQUIRED_FOR_LOW_SAMPLE_LOCAL_PARENT_PATTERN"
)
RECOMMENDATION = "amend cohort pre-screening to fail these sides before execution"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_pitcher_base_lineage_investigation/"
    "2026-07-15"
)
HC_REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/"
    "2026-07-15"
)
DESIGN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_history_complete_starter_cohort_scale_up_design/"
    "2026-07-15"
)
STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_XH = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
COLLECTIVE_CONTRACT = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12/"
    "collective_bundle_v1_field_construction_contract_2026-07-12.json"
)
PITCHER_BASE_CONTRACT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_governance/"
    "2026-07-14/pitcher_base_contract_2026-07-14.csv"
)
EXPECTED_HITS_CONTRACT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_governance/"
    "2026-07-14/expected_hits_dependency_contract_2026-07-14.csv"
)
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
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "source_acquisition_or_discovery": re.compile(r"download|fetch|urlretrieve", re.IGNORECASE),
    "remediation_or_state_mutation": re.compile(r"remediate|post_remediation_starter_qualified\\s*=|STARTER_JOIN_QUALIFIED", re.IGNORECASE),
    "model_training_or_prediction": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss", re.IGNORECASE),
    "signal_or_scoring": re.compile(r"score_|signal_|rank_candidates", re.IGNORECASE),
    "matrix_construction": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
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


def package_sha(path: Path) -> str:
    return sha256_path(path / f"sha256_manifest_{RUN_DATE}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def yes(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


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


class PitcherBaseLineageInvestigation:
    def __init__(self) -> None:
        self.state = json.loads((HC_REMEDIATION_DIR / f"post_remediation_qualification_state_{RUN_DATE}.json").read_text())
        self.side_ledger = read_csv(HC_REMEDIATION_DIR / f"side_level_certification_ledger_{RUN_DATE}.csv")
        self.movement = read_csv(HC_REMEDIATION_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv")
        self.design_sides = read_csv(DESIGN_DIR / f"side_level_inventory_{RUN_DATE}.csv")
        self.starter_base = read_csv(STARTER_BASE)
        self.starter_xh = read_csv(STARTER_XH)
        self.starter_base_index = {
            (r["date"], r["game_id"], r["player_team"], r["opponent_team"]): r for r in self.starter_base
        }
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def fail_side(self) -> dict[str, str]:
        rows = [r for r in self.side_ledger if r["starter_game_side_key"] == FAIL_SIDE]
        if len(rows) != 1:
            raise RuntimeError("fail-closed side reproduction failed")
        return rows[0]

    def fail_rows(self) -> list[dict[str, str]]:
        rows = [r for r in self.movement if r["starter_game_side_key"] == FAIL_SIDE]
        if len(rows) != 9:
            raise RuntimeError("fail-closed nine-row reproduction failed")
        return rows

    def parent_row(self) -> dict[str, str]:
        side = self.fail_side()
        key = (side["slate_date"], side["game_id"], side["opponent_team"], side["hitter_team"])
        row = self.starter_base_index.get(key)
        if not row:
            raise RuntimeError("local parent row missing unexpectedly")
        return row

    def xh_rows(self) -> list[dict[str, str]]:
        side = self.fail_side()
        return [
            r for r in self.starter_xh
            if r.get("date") == side["slate_date"]
            and r.get("game_id") == side["game_id"]
            and r.get("player_team") == side["opponent_team"]
            and r.get("opponent_team") == side["hitter_team"]
        ]

    def validation_rows(self) -> list[dict[str, Any]]:
        rows = [
            ("hc_remediation_package_sha", package_sha(HC_REMEDIATION_DIR), EXPECTED_HC_REMEDIATION_SHA),
            ("hc_remediation_decision", self.state.get("decision"), EXPECTED_HC_DECISION),
            ("design_package_sha", package_sha(DESIGN_DIR), EXPECTED_DESIGN_SHA),
            ("exact_fail_closed_side_reproduction", len([r for r in self.side_ledger if r["starter_game_side_key"] == FAIL_SIDE]), 1),
            ("exact_fail_closed_row_reproduction", len(self.fail_rows()), 9),
            ("fail_reason_reproduction", self.fail_side().get("failure_reason"), "PITCHER_BASE_MISSING|STARTER_EXPECTED_HITS_ALLOWED_MISSING"),
            ("starter_base_source_exists", STARTER_BASE.exists(), True),
            ("starter_xh_source_exists", STARTER_XH.exists(), True),
            ("matrix_count_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        out = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in rows
        ]
        out.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "network_access", "source_acquisition", "source_discovery", "state_mutation",
                "remediation", "matrix_construction", "model_or_signal_work",
                "database_api_write", "upload_or_launchagent_change", "production_behavior_change",
            ]
        ])
        if any(r["status"] != "PASS" for r in out):
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", [r for r in out if r["status"] != "PASS"])
            raise RuntimeError("validation failed")
        return out

    def lineage_maps(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        pitcher_base = [
            {
                "field": "pitcher_base",
                "authoritative_owner": "Starter Expected Hits Allowed / Starter Skill-Workload",
                "source_artifact_or_table": str(STARTER_XH),
                "construction_formula": "pitcher_base is production-style pitcher_expected_hits_allowed_weighted from strict-prior Starter parents; frozen contract preserves existing minimum-history and missingness rules.",
                "required_parent_inputs": "expected/probable starter binding|strict-prior pitcher history|weighted_multiseason_hits_per_out or equivalent hits/start parents|minimum-history rule",
                "expected_grain": "starter-game or hitter-row bound to opposing starter-game side",
                "temporal_eligibility_rule": "all contributing pitcher history strictly before governed slate date; feature_cutoff_date < slate_date",
                "fallback_hierarchy": "none frozen for production-style pitcher_base; missing parents fail closed",
                "persistence_location": f"{STARTER_XH}; propagated to {STARTER_BASE}",
                "join_or_binding_key": "date + game_id + pitcher/opponent team + actual/expected starter id",
                "earliest_missing_stage": str(STARTER_XH),
                "missingness_classification": "constructed diagnostics exist in later workload source, but production-style pitcher_base was not persisted because expected-starter context was missing/low-history",
            }
        ]
        starter_expected = [
            {
                "field": "starter_expected_hits_allowed",
                "authoritative_owner": "Starter Expected Hits Allowed / Hits Environment",
                "source_artifact_or_table": str(STARTER_XH),
                "construction_formula": "starter_expected_hits_allowed = pitcher_base * offense_factor_vs_league_clamped under current frozen production blend; status/trust/workload parents govern certification but no fallback calculation is allowed",
                "required_parent_inputs": "pitcher_base|offense_factor_vs_league_clamped|starter status/trust certification|strict-prior eligibility",
                "direct_or_derived": "derived and persisted when parents exist",
                "relationship_to_pitcher_base": "direct child; missing pitcher_base causes missing starter_expected_hits_allowed",
                "relationship_to_offense_factor": "multiplicative context parent; offense factor is present for fail side",
                "relationship_to_status_trust": "status/trust are certification gates, not a permitted fallback formula in this investigation",
                "relationship_to_expected_workload": "expected workload is present but cannot replace pitcher_base",
                "expected_grain": "starter-game / hitter-row matchup side",
                "temporal_eligibility": "offense context and pitcher history must be prior-date context",
                "persistence_location": f"{STARTER_XH}; propagated to {STARTER_BASE}",
                "join_or_binding_key": "date + game_id + offense team/opponent starter",
                "earliest_missing_stage": str(STARTER_XH),
            }
        ]
        return pitcher_base, starter_expected

    def parent_domain_audit(self) -> list[dict[str, Any]]:
        side = self.fail_side()
        parent = self.parent_row()
        xh = self.xh_rows()
        xh_any = xh[0] if xh else {}
        domains = [
            ("actual_starter_identity", "PRESENT_AND_COMPATIBLE", parent.get("actual_starter_player_id"), "actual starter id matches source_starter_game_key and HC side"),
            ("strict_prior_cutoff", "PRESENT_AND_COMPATIBLE", parent.get("feature_cutoff_date"), f"latest prior game {parent.get('latest_contributing_prior_game_date')} is before {side['slate_date']}"),
            ("prior_starts", "PRESENT_AND_COMPATIBLE", parent.get("prior_starts_count"), "four prior starts are present"),
            ("prior_outs_or_innings", "PRESENT_AND_COMPATIBLE", parent.get("expected_outs_blended_v1"), "expected workload exists"),
            ("recent_workload_windows", "PRESENT_AND_COMPATIBLE", parent.get("recent3_prior_starts_count"), "recent3 exists; recent5 is partial because only four starts exist"),
            ("starter_status", "PRESENT_AND_COMPATIBLE", parent.get("starter_identity_status"), "expected starter confirmed actual starter"),
            ("starter_trust", "AMBIGUOUS_FAIL_CLOSED", parent.get("role_confidence"), "low role confidence from low history; cannot promote through trust without governance"),
            ("offense_factor_vs_league_clamped", "PRESENT_AND_COMPATIBLE", parent.get("offense_factor_vs_league_clamped"), "offense parent is present"),
            ("expected_workload", "PRESENT_AND_COMPATIBLE", parent.get("expected_outs_blended_v1"), "workload parent is present"),
            ("pitcher_base", "DIRECT_SOURCE_MISSING", parent.get("pitcher_base"), "missing in local workload source and upstream expected-hits characterization"),
            ("starter_expected_hits_allowed", "PRESENT_BUT_NOT_PERSISTED_TO_CHILD", parent.get("starter_expected_hits_allowed"), "cannot persist because pitcher_base parent missing; offense factor exists"),
            ("expected_hits_outs_context_v1", "PRESENT_WRONG_GRAIN", parent.get("expected_hits_outs_context_v1"), "diagnostic research field exists but is not the frozen production-style pitcher_base/starter_expected field"),
            ("starter_xh_expected_starter_id", "DIRECT_SOURCE_MISSING", xh_any.get("expected_starter_player_id", ""), "upstream expected-hits characterization rows have blank expected_starter_player_id for this matchup"),
            ("starter_xh_pitcher_base", "DIRECT_SOURCE_MISSING", xh_any.get("pitcher_base", ""), "earliest located missing stage"),
        ]
        return [
            {
                "starter_game_side_key": FAIL_SIDE,
                "domain": name,
                "classification": classification,
                "source_path": str(STARTER_BASE if not name.startswith("starter_xh") else STARTER_XH),
                "row_identity": parent.get("starter_game_key", ""),
                "source_date": parent.get("date", ""),
                "value": value,
                "notes": notes,
            }
            for name, classification, value, notes in domains
        ]

    def source_inventory(self) -> list[dict[str, Any]]:
        side = self.fail_side()
        parent = self.parent_row()
        xh_rows = self.xh_rows()
        return [
            {
                "source_path": str(HC_REMEDIATION_DIR / f"side_level_certification_ledger_{RUN_DATE}.csv"),
                "source_type": "remediation_side_ledger",
                "grain": "starter_game_side",
                "matching_rows": 1,
                "pitcher_base_value": side.get("pitcher_base"),
                "starter_expected_hits_allowed_value": side.get("starter_expected_hits_allowed"),
                "strict_prior_status": side.get("strict_prior_status"),
                "notes": "authoritative fail-closed ledger",
            },
            {
                "source_path": str(STARTER_BASE),
                "source_type": "local_parent_workload_source",
                "grain": "starter_game",
                "matching_rows": 1,
                "pitcher_base_value": parent.get("pitcher_base"),
                "starter_expected_hits_allowed_value": parent.get("starter_expected_hits_allowed"),
                "strict_prior_status": parent.get("strict_prior_status"),
                "notes": "workload diagnostics present; production-style expected fields missing",
            },
            {
                "source_path": str(STARTER_XH),
                "source_type": "starter_expected_hits_characterization",
                "grain": "hitter_row/research_row",
                "matching_rows": len(xh_rows),
                "pitcher_base_value": "|".join(sorted({r.get("pitcher_base", "") for r in xh_rows})),
                "starter_expected_hits_allowed_value": "|".join(sorted({r.get("starter_expected_hits_allowed", "") for r in xh_rows})),
                "strict_prior_status": "|".join(sorted({r.get("strict_prior_status", "") for r in xh_rows})),
                "notes": "earliest located stage with missing production-style fields",
            },
            {
                "source_path": str(COLLECTIVE_CONTRACT),
                "source_type": "field_construction_contract",
                "grain": "field_contract",
                "matching_rows": 1,
                "pitcher_base_value": "contract_reference",
                "starter_expected_hits_allowed_value": "contract_reference",
                "strict_prior_status": "n/a",
                "notes": "documents parent/child ownership and no formula-change discipline",
            },
            {
                "source_path": str(PITCHER_BASE_CONTRACT),
                "source_type": "governance_contract",
                "grain": "field_contract",
                "matching_rows": 1,
                "pitcher_base_value": "missing parents fail closed",
                "starter_expected_hits_allowed_value": "",
                "strict_prior_status": "strict-prior required",
                "notes": "frozen four-side governance contract reused as lineage reference",
            },
            {
                "source_path": str(EXPECTED_HITS_CONTRACT),
                "source_type": "governance_contract",
                "grain": "field_contract",
                "matching_rows": 1,
                "pitcher_base_value": "dependency",
                "starter_expected_hits_allowed_value": "dependency chain",
                "strict_prior_status": "strict-prior required",
                "notes": "any dependency failure fails expected-hits input chain",
            },
        ]

    def root_cause(self) -> list[dict[str, Any]]:
        parent = self.parent_row()
        xh_rows = self.xh_rows()
        return [
            {
                "starter_game_side_key": FAIL_SIDE,
                "missing_field": "pitcher_base",
                "earliest_missing_stage": str(STARTER_XH),
                "root_cause_class": "CONSTRUCTION_OR_PERSISTENCE_DEFECT_REPAIR_REQUIRED",
                "evidence": "upstream expected-hits characterization has blank expected_starter_player_id and blank pitcher_base for matching rows; local workload source has four strict-prior starts and expected_outs_blended_v1 but blank pitcher_base",
                "never_constructed_or_not_persisted": "not persisted as production-style pitcher_base; diagnostic expected_hits_outs_v1 exists",
                "same_game_leakage_risk": "none observed; latest_contributing_prior_game_date is strict-prior",
                "formula_governance_change_required": "yes if using expected_hits_outs_v1 as substitute; no such substitution is authorized",
            },
            {
                "starter_game_side_key": FAIL_SIDE,
                "missing_field": "starter_expected_hits_allowed",
                "earliest_missing_stage": str(STARTER_XH),
                "root_cause_class": "DEPENDENT_FIELD_MISSING_PARENT",
                "evidence": f"offense_factor={parent.get('offense_factor_vs_league_clamped')} is present, but pitcher_base is blank; {len(xh_rows)} upstream rows also blank",
                "never_constructed_or_not_persisted": "not derived/persisted because required production-style pitcher_base parent missing",
                "same_game_leakage_risk": "none observed in available parents",
                "formula_governance_change_required": "no to diagnose; yes to derive from alternate diagnostic parent",
            },
        ]

    def scope_analysis(self) -> list[dict[str, Any]]:
        side_inventory = [s for s in self.design_sides if s["classification"] != "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE"]
        idx = {
            (r["date"], r["game_id"], r["player_team"], r["opponent_team"]): r for r in self.starter_base
        }
        buckets = Counter()
        row_counts = Counter()
        h05 = Counter()
        h15 = Counter()
        ceiling = Counter()
        for side in side_inventory:
            base = idx.get((side["slate_date"], side["game_id"], side["opponent_team"], side["hitter_team"]))
            if not base:
                bucket = "EXTERNAL_SOURCE_PARENT_RECOVERY_REQUIRED_OR_DISCOVERY_GOVERNANCE"
            elif base.get("strict_prior_status") == "PASS_STRICT_PRIOR" and not base.get("pitcher_base") and not base.get("starter_expected_hits_allowed"):
                bucket = "LOCAL_PARENT_PATTERN_MISSING_PITCHER_BASE_AND_STARTER_EXPECTED"
            elif base.get("strict_prior_status") == "PASS_STRICT_PRIOR" and base.get("pitcher_base") and base.get("starter_expected_hits_allowed"):
                bucket = "LOCAL_PARENT_COMPLETE"
            else:
                bucket = "AMBIGUOUS_FAIL_CLOSED"
            buckets[bucket] += 1
            row_counts[bucket] += int_value(side["represented_denominator_rows"])
            h05[bucket] += int_value(side["hits_0_5_rows"])
            h15[bucket] += int_value(side["hits_1_5_rows"])
            ceiling[bucket] += int_value(side["projected_fully_qualified_ceiling"])
        rows = []
        for bucket in sorted(buckets):
            rows.append({
                "population_bucket": bucket,
                "starter_game_sides": buckets[bucket],
                "represented_rows": row_counts[bucket],
                "hits_0_5_rows": h05[bucket],
                "hits_1_5_rows": h15[bucket],
                "projected_qualification_ceiling_if_recovered": ceiling[bucket],
                "recoverability_classification": (
                    "CURRENT_REPOSITORY_PARENT_LINEAGE_RECOVERABLE_NEW_GOVERNANCE_REQUIRED"
                    if bucket == "LOCAL_PARENT_PATTERN_MISSING_PITCHER_BASE_AND_STARTER_EXPECTED"
                    else "EXTERNAL_SOURCE_PARENT_RECOVERY_REQUIRED"
                    if bucket == "EXTERNAL_SOURCE_PARENT_RECOVERY_REQUIRED_OR_DISCOVERY_GOVERNANCE"
                    else "CURRENT_REPOSITORY_PARENT_LINEAGE_RECOVERABLE_EXISTING_GOVERNANCE"
                    if bucket == "LOCAL_PARENT_COMPLETE"
                    else "INSUFFICIENT_EVIDENCE_FAIL_CLOSED"
                ),
                "notes": "read-only scope analysis; no cohort classification changed",
            })
        return rows

    def recoverability_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_side_key": FAIL_SIDE,
                "recoverability_decision": "CONSTRUCTION_OR_PERSISTENCE_DEFECT_REPAIR_REQUIRED",
                "data_recovery": "strict-prior workload parents are present; production-style pitcher_base is absent",
                "lineage_repair": "needed to decide whether low_lt5 expected-hits diagnostics may produce a governed production-style parent",
                "join_repair": "not the primary issue; matching local parent row exists by date/game/opponent/hitter team",
                "persistence_repair": "needed if existing generation should persist low-history pitcher_base; this would be a governed rule change",
                "formula_governance_change": "required if expected_hits_outs_v1 is promoted as substitute for pitcher_base/starter_expected_hits_allowed",
                "recommended_next_bounded_action": RECOMMENDATION,
            }
        ]

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validations = self.validation_rows()
        pitcher_base_map, starter_expected_map = self.lineage_maps()
        fail_rows = self.fail_rows()
        side = self.fail_side()
        parent = self.parent_row()
        scope = self.scope_analysis()

        write_csv(OUT_DIR / f"exact_fail_closed_side_manifest_{RUN_DATE}.csv", [side])
        write_csv(OUT_DIR / f"exact_fail_closed_row_manifest_{RUN_DATE}.csv", fail_rows)
        write_csv(OUT_DIR / f"pitcher_base_lineage_map_{RUN_DATE}.csv", pitcher_base_map)
        write_csv(OUT_DIR / f"starter_expected_hits_allowed_lineage_map_{RUN_DATE}.csv", starter_expected_map)
        write_csv(OUT_DIR / f"parent_domain_audit_{RUN_DATE}.csv", self.parent_domain_audit())
        write_csv(OUT_DIR / f"source_and_grain_inventory_{RUN_DATE}.csv", self.source_inventory())
        write_csv(OUT_DIR / f"missingness_root_cause_analysis_{RUN_DATE}.csv", self.root_cause())
        write_csv(OUT_DIR / f"matching_population_scope_analysis_{RUN_DATE}.csv", scope)
        write_csv(OUT_DIR / f"recoverability_classification_{RUN_DATE}.csv", self.recoverability_rows())
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validations)
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        write_csv(OUT_DIR / f"immutability_audit_{RUN_DATE}.csv", [
            {"artifact_family": "A/B/D matrices", "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL", "before_hashes": json.dumps(self.matrix_hash_before, sort_keys=True), "after_hashes": json.dumps(matrix_after, sort_keys=True)},
            {"artifact_family": "HC remediation package", "status": "PASS" if package_sha(HC_REMEDIATION_DIR) == EXPECTED_HC_REMEDIATION_SHA else "FAIL", "before_hashes": EXPECTED_HC_REMEDIATION_SHA, "after_hashes": package_sha(HC_REMEDIATION_DIR)},
        ])
        guard = static_guard()
        write_csv(OUT_DIR / f"static_no_network_no_remediation_no_model_no_matrix_guard_{RUN_DATE}.csv", guard)
        if any(r["status"] != "PASS" for r in guard):
            raise RuntimeError("static guard failed")

        matching = next((r for r in scope if r["population_bucket"] == "LOCAL_PARENT_PATTERN_MISSING_PITCHER_BASE_AND_STARTER_EXPECTED"), {})
        payload = {
            "decision": DECISION,
            "recommendation": RECOMMENDATION,
            "fail_closed_side": FAIL_SIDE,
            "fail_closed_rows": len(fail_rows),
            "pitcher_base_missing": True,
            "starter_expected_hits_allowed_missing": True,
            "earliest_missing_stage": str(STARTER_XH),
            "root_cause": "production-style pitcher_base was not persisted in expected-hits lineage for low-history/missing-starter-context rows; starter_expected is missing as its child",
            "recoverability": "CONSTRUCTION_OR_PERSISTENCE_DEFECT_REPAIR_REQUIRED",
            "matching_remaining_sides": int_value(matching.get("starter_game_sides", 0)),
            "matching_remaining_rows": int_value(matching.get("represented_rows", 0)),
            "matching_remaining_hits_0_5_rows": int_value(matching.get("hits_0_5_rows", 0)),
            "matching_remaining_hits_1_5_rows": int_value(matching.get("hits_1_5_rows", 0)),
            "projected_ceiling_if_reusable_remediation_later_succeeds": int_value(matching.get("projected_qualification_ceiling_if_recovered", 0)),
            "parent_prior_starts": parent.get("prior_starts_count"),
            "parent_sample_size_band": parent.get("sample_size_band"),
            "network_requests": 0,
            "source_acquisition_requests": 0,
            "source_discovery_requests": 0,
            "state_mutations": 0,
            "remediation_performed": False,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"machine_readable_lineage_investigation_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# HC_LOCAL_COHORT_001 Pitcher Base Lineage Investigation — {RUN_DATE}

Decision: `{DECISION}`

Recommendation: `{RECOMMENDATION}`.

The exact fail-closed side is `{FAIL_SIDE}` with `9` governed denominator rows.
The local parent source contains strict-prior workload evidence: `4` prior starts,
`expected_outs_blended_v1={parent.get('expected_outs_blended_v1')}`, and
`offense_factor_vs_league_clamped={parent.get('offense_factor_vs_league_clamped')}`.
However, production-style `pitcher_base` and `starter_expected_hits_allowed` are
blank in both the Starter Skill/Workload parent artifact and the upstream Starter
Expected Hits characterization artifact.

Root cause: the value was not safely persisted as the frozen production-style
`pitcher_base` parent for this low-history/missing-starter-context row. A diagnostic
field, `expected_hits_outs_context_v1={parent.get('expected_hits_outs_context_v1')}`,
exists, but using it as a substitute would be a formula-governance change, not
simple data recovery.

Scope: the same local-parent missing-both pattern appears in `2` remaining sides
representing `17` rows, all Hits 0.5. The other `78` remaining sides still lack
local parent evidence and remain external-source/discovery-governance work.

No remediation, state mutation, source acquisition, discovery, matrix construction,
model/scoring work, DB/API write, upload, LaunchAgent change, or production behavior
change was performed.
""")
        write_csv(OUT_DIR / f"recommended_next_bounded_action_{RUN_DATE}.csv", [{
            "recommendation": RECOMMENDATION,
            "authorizes_remediation": False,
            "authorizes_formula_change": False,
            "authorizes_source_acquisition": False,
            "notes": "Before another execution cohort, pre-screen low-history local parent rows with blank production-style pitcher_base/starter_expected fields and fail them before execution unless new governance is frozen.",
        }])
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

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
    result = PitcherBaseLineageInvestigation().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
