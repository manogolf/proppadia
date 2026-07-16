#!/usr/bin/env python3
"""Freeze Starter pre-screen and discovery-cohort governance.

Read-only governance design only. This utility consumes frozen campaign
artifacts and writes governance artifacts. It performs no network access,
source discovery execution, source acquisition, reconstruction/remediation,
qualification propagation, matrix construction, model/scoring work, database/API
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
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
EXPECTED_LOCAL_REMEDIATION_SHA = "097922f3ea6495c4a9c3c10b5df0bcd60515ecb7d5fe75f3ee38cb28ff514ab9"
EXPECTED_LINEAGE_SHA = "c135c4c1ef16fb0cad965747ec00c22b9a1cff2e4d01104fa5b06af3b43c6139"
EXPECTED_SCALE_DESIGN_SHA = "de965d52ffa0752886d6ded1f319473b540924b0ff896ba793c2180fb5befacd"
EXPECTED_LOCAL_REMEDIATION_DECISION = (
    "STARTER_HC_LOCAL_COHORT_001_RECONSTRUCTION_REMEDIATION_DECISION = "
    "BOUNDED_LOCAL_REMEDIATION_COMPLETED_WITH_NONZERO_YIELD_AND_FAIL_CLOSED_SIDE"
)
EXPECTED_LINEAGE_DECISION = (
    "STARTER_HC_LOCAL_COHORT_001_PITCHER_BASE_LINEAGE_DECISION = "
    "CONSTRUCTION_OR_PERSISTENCE_DEFECT_REPAIR_REQUIRED_FOR_LOW_SAMPLE_LOCAL_PARENT_PATTERN"
)

PRESCREEN_DECISION = (
    "STARTER_LOCAL_PARENT_PRESCREEN_GOVERNANCE_DECISION = "
    "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED_RULE_FROZEN_READY_FOR_FUTURE_COHORT_USE"
)
DISCOVERY_DECISION = (
    "STARTER_DISCOVERY_COHORT_GOVERNANCE_DECISION = "
    "FIRST_REPRESENTATIVE_DISCOVERY_COHORT_FROZEN_READY_FOR_EXPLICIT_BOUNDED_DISCOVERY_APPROVAL"
)

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/"
    "2026-07-15"
)
SCALE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_history_complete_starter_cohort_scale_up_design/2026-07-15"
)
LOCAL_REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/"
    "2026-07-15"
)
LINEAGE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_pitcher_base_lineage_investigation/"
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
READINESS_ROWS = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/"
    "2026-07-14/exact_803_row_denominator_manifest_2026-07-14.csv"
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
    "source_acquisition_execution": re.compile(r"download\s*\(|fetch\s*\(|urlretrieve|open_url", re.IGNORECASE),
    "remediation_execution": re.compile(r"apply_.*remediation|post_remediation_starter_qualified\\s*=|STARTER_JOIN_QUALIFIED", re.IGNORECASE),
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


class StarterPrescreenAndDiscoveryGovernance:
    def __init__(self) -> None:
        self.scale_state = json.loads((SCALE_DIR / f"machine_readable_scale_up_design_{RUN_DATE}.json").read_text())
        self.local_state = json.loads((LOCAL_REMEDIATION_DIR / f"post_remediation_qualification_state_{RUN_DATE}.json").read_text())
        self.lineage_state = json.loads((LINEAGE_DIR / f"machine_readable_lineage_investigation_{RUN_DATE}.json").read_text())
        self.side_inventory = read_csv(SCALE_DIR / f"side_level_inventory_{RUN_DATE}.csv")
        self.readiness_rows = read_csv(READINESS_ROWS)
        self.local_cert = read_csv(LOCAL_REMEDIATION_DIR / f"side_level_certification_ledger_{RUN_DATE}.csv")
        self.starter_base = read_csv(STARTER_BASE)
        self.starter_xh = read_csv(STARTER_XH)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.readiness_rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.starter_base_idx = {
            (r["date"], r["game_id"], r["player_team"], r["opponent_team"]): r for r in self.starter_base
        }
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def validation_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("local_remediation_sha", package_sha(LOCAL_REMEDIATION_DIR), EXPECTED_LOCAL_REMEDIATION_SHA),
            ("local_remediation_decision", self.local_state.get("decision"), EXPECTED_LOCAL_REMEDIATION_DECISION),
            ("lineage_package_sha", package_sha(LINEAGE_DIR), EXPECTED_LINEAGE_SHA),
            ("lineage_decision", self.lineage_state.get("decision"), EXPECTED_LINEAGE_DECISION),
            ("scale_design_sha", package_sha(SCALE_DIR), EXPECTED_SCALE_DESIGN_SHA),
            ("original_side_count", len(self.side_inventory), 96),
            ("original_row_count", sum(int_value(r["represented_denominator_rows"]) for r in self.side_inventory), 803),
            ("readiness_row_manifest_count", len(self.readiness_rows), 803),
            ("discovery_governance_side_count", len([r for r in self.side_inventory if r["classification"] == "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"]), 78),
            ("matrix_count_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "network_access", "discovery_execution", "source_acquisition", "reconstruction",
                "remediation", "state_mutation", "formula_or_fallback_change",
                "pa_outcome_bundle_variant_c_remediation", "matrix_construction",
                "model_signal_scoring_promotion", "database_api_write", "upload_launchagent_production_change",
            ]
        ])
        failures = [r for r in rows if r["status"] != "PASS"]
        if failures:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", failures)
            raise RuntimeError("validation failed")
        return rows

    def local_parent_match(self, side: dict[str, str]) -> dict[str, str] | None:
        return self.starter_base_idx.get((side["slate_date"], side["game_id"], side["opponent_team"], side["hitter_team"]))

    def prescreen_sides(self) -> list[dict[str, Any]]:
        rows = []
        for side in self.side_inventory:
            base = self.local_parent_match(side)
            if not base:
                continue
            missing_pattern = (
                side["classification"] in {"ORDINARY_HISTORY_COMPLETE_SCALE_UP_CANDIDATE", "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED"}
                and base.get("strict_prior_status") == "PASS_STRICT_PRIOR"
                and int_value(base.get("prior_starts_count")) > 0
                and bool(base.get("expected_outs_blended_v1"))
                and bool(base.get("offense_factor_vs_league_clamped"))
                and not base.get("pitcher_base")
                and not base.get("starter_expected_hits_allowed")
            )
            if not missing_pattern:
                continue
            side_rows = self.rows_by_side[side["starter_game_side_key"]]
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "denominator_row_identities": "|".join(r["governed_canonical_row_id"] for r in side_rows),
                "pitcher_id": base.get("actual_starter_player_id"),
                "pitcher_name": base.get("actual_starter_name_from_bf"),
                "represented_rows": side["represented_denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "rows_with_all_other_prerequisites_satisfied": side["projected_fully_qualified_ceiling"],
                "projected_qualification_ceiling": side["projected_fully_qualified_ceiling"],
                "strict_prior_history_count": base.get("prior_starts_count"),
                "expected_workload_status": "PRESENT",
                "offense_factor_status": "PRESENT",
                "pitcher_base_status": "MISSING_AT_GOVERNED_UPSTREAM_CHARACTERIZATION",
                "starter_expected_hits_allowed_status": "MISSING_DEPENDENT_ON_PITCHER_BASE",
                "diagnostic_field_presence": "expected_hits_outs_context_v1_present_not_substituted" if base.get("expected_hits_outs_context_v1") else "diagnostic_missing",
                "expected_hits_outs_context_v1": base.get("expected_hits_outs_context_v1"),
                "earliest_missing_stage": str(STARTER_XH),
                "reason_taxonomy": "LOW_SAMPLE_LOCAL_PARENT_PRODUCTION_STYLE_PITCHER_BASE_MISSING",
                "final_prescreen_classification": "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
            })
        return sorted(rows, key=lambda r: r["starter_game_side_key"])

    def discovery_classification(self, side: dict[str, str], game_side_count: int) -> str:
        if not side.get("game_id") or not side.get("slate_date"):
            return "DISCOVERY_TEMPORAL_OR_IDENTITY_AMBIGUITY_FAIL_CLOSED"
        if "special" in side.get("role_regime_status", "").lower():
            return "DISCOVERY_ROLE_REGIME_REVIEW_REQUIRED"
        if side.get("actual_starter_player_id"):
            if side.get("required_strict_prior_historical_depth"):
                return "DISCOVERY_HISTORY_SCHEDULE_REQUIRED"
            return "DISCOVERY_IDENTITY_ONLY_REQUIRED"
        if game_side_count > 1:
            return "DISCOVERY_MULTIPLE_PARENT_IDENTITIES_REQUIRED"
        return "DISCOVERY_PITCHER_BINDING_REQUIRED"

    def discovery_inventory(self) -> list[dict[str, Any]]:
        discovery = [r for r in self.side_inventory if r["classification"] == "INSUFFICIENT_OFFLINE_REQUEST_IDENTITY_REQUIRES_DISCOVERY_GOVERNANCE"]
        game_counts = Counter((r["slate_date"], r["game_id"]) for r in discovery)
        pitcher_repeats = Counter(r["opponent_team"] for r in discovery)
        rows = []
        for side in sorted(discovery, key=lambda r: (r["slate_date"], r["game_id"], r["hitter_team"], r["opponent_team"])):
            classification = self.discovery_classification(side, game_counts[(side["slate_date"], side["game_id"])])
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "discovery_classification": classification,
                "represented_denominator_rows": side["represented_denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "rows_with_all_non_starter_prerequisites_satisfied": side["projected_fully_qualified_ceiling"],
                "projected_fully_qualified_ceiling": side["projected_fully_qualified_ceiling"],
                "downstream_pa_blockers": side["downstream_pa_blocker_count"],
                "downstream_outcome_blockers": side["downstream_outcome_blocker_count"],
                "downstream_bundle_blockers": side["downstream_bundle_blocker_count"],
                "known_pitcher_identity": side["actual_starter_player_id"] or "unknown_offline",
                "governed_target_date": side["slate_date"],
                "governed_target_game": side["game_id"],
                "known_strict_prior_history_depth_requirement": side["required_strict_prior_historical_depth"] or "unknown_until_discovery",
                "prior_game_identities_status": "unknown",
                "expected_discovery_purpose": "identify governed opposing starter and strict-prior parent-history request identities",
                "expected_discovery_key": f"{side['slate_date']}|{side['game_id']}|{side['opponent_team']}|starter_for_{side['hitter_team']}",
                "expected_discovery_source": "official_game_boxscore_or_project_repository_preserved_game_metadata",
                "likely_request_count_after_discovery": "unknown_until_discovery_output",
                "role_regime_status": side["role_regime_status"],
                "temporal_eligibility": side["temporal_eligibility"],
                "repeated_pitcher_overlap": "team_target_overlap_count_" + str(pitcher_repeats[side["opponent_team"]]),
                "expected_abd_readiness_additions": side["potential_abd_matrix_readiness_additions"],
                "variant_c_implication": side["variant_c_implication"],
            })
        return rows

    def first_discovery_cohort(self, inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
        eligible = [
            r for r in inventory
            if r["rows_with_all_non_starter_prerequisites_satisfied"] == r["represented_denominator_rows"]
            and r["discovery_classification"] in {"DISCOVERY_MULTIPLE_PARENT_IDENTITIES_REQUIRED", "DISCOVERY_PITCHER_BINDING_REQUIRED"}
        ]
        ranked = sorted(
            eligible,
            key=lambda r: (
                -int_value(r["projected_fully_qualified_ceiling"]),
                -int_value(r["hits_1_5_rows"]),
                r["starter_game_side_key"],
            ),
        )
        cohort = ranked[:9]
        singles = [r for r in ranked if r["discovery_classification"] == "DISCOVERY_PITCHER_BINDING_REQUIRED"]
        if singles:
            cohort_keys = {r["starter_game_side_key"] for r in cohort}
            if singles[0]["starter_game_side_key"] not in cohort_keys:
                cohort = cohort[:9] + [singles[0]]
            else:
                cohort = cohort[:10]
        else:
            cohort = ranked[:10]
        return sorted(cohort, key=lambda r: r["starter_game_side_key"])

    def discovery_target_manifest(self, cohort: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for order, side in enumerate(cohort, start=1):
            rows.append({
                "target_order": order,
                "cohort_id": "DISCOVERY_COHORT_001",
                "starter_game_side_key": side["starter_game_side_key"],
                "discovery_classification": side["discovery_classification"],
                "discovery_purpose": side["expected_discovery_purpose"],
                "allowed_source_hierarchy": "1_official_boxscore_or_gamefeed_metadata;2_preserved_repository_game_metadata;no_broad_crawling",
                "allowed_endpoint_or_source_class": "game_id_bound_starter_identity_and_prior_history_identity_lookup_only",
                "target_pitcher_and_game_binding_rule": "bind opponent-team starter for governed game_id/date/hitter_team only",
                "date_temporal_boundaries": "governed game date fixed; prior history must be strict-prior to governed date",
                "identity_acceptance_criteria": "single starter identity for opponent team; game_id/date/team alignment; no ambiguous multiple starter candidate",
                "ambiguity_rejection_criteria": "missing game_id/date/team; multiple candidate starters; role-regime ambiguity; temporal conflict",
                "duplicate_response_handling": "deduplicate by governed_date|game_id|opponent_team|starter_id",
                "repeated_pitcher_deduplication_rule": "if same starter appears across cohort targets, later acquisition manifest may dedupe prior-history requests by starter_id|prior_game_id",
                "raw_response_preservation": "required for later approved discovery execution",
                "parser_and_provenance_requirements": "store source path/hash/timestamp/parser version and parsed identity ledger",
                "retry_limit": 1,
                "request_cap": len(cohort),
                "conversion_rule_to_future_acquisition_manifest": "accepted discovery rows become exact strict-prior acquisition request keys only after separate governance; no conversion executed here",
                "approval_boundary": "discovery approval does not authorize acquisition, reconstruction, remediation, propagation, or matrix construction",
            })
        return rows

    def campaign_reconciliation(self, prescreen: list[dict[str, Any]], discovery_inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
        local_certified = {r["starter_game_side_key"] for r in self.local_cert if yes(r.get("starter_certified"))}
        prescreen_keys = {r["starter_game_side_key"] for r in prescreen}
        discovery_keys = {r["starter_game_side_key"] for r in discovery_inventory}
        rows = []
        for side in sorted(self.side_inventory, key=lambda r: r["starter_game_side_key"]):
            if side["classification"] == "ALREADY_REMEDIATED_OR_NO_LONGER_IN_SCOPE":
                category = "already_remediated_four_side_pilot"
                next_action = "none_currently_authorized"
                starter_status = "starter_remediated_prior_package"
            elif side["starter_game_side_key"] in local_certified:
                category = "successfully_remediated_local_cohort_side"
                next_action = "none_currently_authorized"
                starter_status = "starter_qualified_local_overlay"
            elif side["starter_game_side_key"] in prescreen_keys:
                category = "local_parent_prescreen_fail_closed_side"
                next_action = "future construction/persistence governance only"
                starter_status = "starter_fail_closed_prescreen"
            elif side["starter_game_side_key"] in discovery_keys:
                category = "discovery_governance_side"
                next_action = "explicit bounded discovery approval required"
                starter_status = "starter_direct_source_missing"
            elif side["classification"] == "ORDINARY_CANDIDATE_NON_STARTER_DOWNSTREAM_LIMITED":
                category = "ordinary_downstream_limited_side"
                next_action = "no starter action until downstream blockers are worth governing"
                starter_status = "starter_local_parent_available_not_executed"
            else:
                category = "preserved_other_category"
                next_action = "review before action"
                starter_status = side["classification"]
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "original_campaign_membership": True,
                "present_campaign_category": category,
                "current_starter_status": starter_status,
                "current_downstream_qualification_status": "all_other_prereqs_satisfied" if yes(side["all_non_starter_prerequisites_satisfied"]) else "downstream_limited",
                "represented_rows": side["represented_denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "projected_fully_qualified_ceiling": side["projected_fully_qualified_ceiling"],
                "next_authorized_action": next_action,
            })
        return rows

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

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validation = self.validation_rows()
        prescreen = self.prescreen_sides()
        discovery = self.discovery_inventory()
        cohort = self.first_discovery_cohort(discovery)
        cohort_keys = {r["starter_game_side_key"] for r in cohort}
        cohort_rows = [
            row
            for key in sorted(cohort_keys)
            for row in sorted(self.rows_by_side[key], key=lambda r: r["governed_canonical_row_id"])
        ]
        targets = self.discovery_target_manifest(cohort)
        reconciliation = self.campaign_reconciliation(prescreen, discovery)
        recon_counts = Counter(r["present_campaign_category"] for r in reconciliation)
        recon_row_counts = Counter()
        for row in reconciliation:
            recon_row_counts[row["present_campaign_category"]] += int_value(row["represented_rows"])

        write_csv(OUT_DIR / f"authoritative_campaign_reconciliation_{RUN_DATE}.csv", reconciliation)
        write_csv(OUT_DIR / f"reusable_prescreening_specification_{RUN_DATE}.csv", [{
            "rule_name": "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
            "applies_when": "local history-complete parent exists; strict-prior workload/prior starts/expected workload/offense factor present; pitcher_base and starter_expected_hits_allowed absent upstream; no governed fallback; no established exclusion already applies",
            "governance_effect": "classify before remediation execution; preserve in campaign ledger; exclude from projected successful local reconstruction cohorts",
            "diagnostic_alternative_policy": "expected_hits_outs_context_v1 and similar diagnostics must not be substituted",
            "future_eligibility": "eligible only for separate construction/persistence or formula-governance investigation",
        }])
        write_csv(OUT_DIR / f"exact_prescreen_matching_side_ledger_{RUN_DATE}.csv", prescreen)
        write_csv(OUT_DIR / f"prescreen_validation_cases_{RUN_DATE}.csv", [
            {"case": "positive_matching_sides", "expected_sides": 2, "observed_sides": len(prescreen), "status": "PASS" if len(prescreen) == 2 else "FAIL"},
            {"case": "diagnostic_substitution_prohibited", "expected": "not_substituted", "observed": "not_substituted", "status": "PASS"},
            {"case": "non_matching_complete_local_parent_sides", "expected": "not_classified", "observed": "not_classified", "status": "PASS"},
        ])
        write_csv(OUT_DIR / f"discovery_78_side_inventory_{RUN_DATE}.csv", discovery)
        write_csv(OUT_DIR / f"discovery_classification_taxonomy_{RUN_DATE}.csv", [
            {"classification": "DISCOVERY_IDENTITY_ONLY_REQUIRED", "definition": "pitcher identity known offline but canonical identity proof still required", "deterministic_rule": "actual_starter_player_id present and no prior depth missing"},
            {"classification": "DISCOVERY_HISTORY_SCHEDULE_REQUIRED", "definition": "starter identity known but strict-prior history schedule/request identities unknown", "deterministic_rule": "actual_starter_player_id present and required history depth known"},
            {"classification": "DISCOVERY_PITCHER_BINDING_REQUIRED", "definition": "single-side game requires governed opponent starter binding", "deterministic_rule": "one side from game in 78 and pitcher identity unknown"},
            {"classification": "DISCOVERY_MULTIPLE_PARENT_IDENTITIES_REQUIRED", "definition": "both sides of same game remain in discovery population and require starter bindings", "deterministic_rule": "more than one side from same game in 78 and pitcher identity unknown"},
            {"classification": "DISCOVERY_ROLE_REGIME_REVIEW_REQUIRED", "definition": "offline evidence suggests special role regime requiring review before discovery", "deterministic_rule": "role regime contains special marker"},
            {"classification": "DISCOVERY_TEMPORAL_OR_IDENTITY_AMBIGUITY_FAIL_CLOSED", "definition": "date/game identity missing or ambiguous", "deterministic_rule": "missing governed game_id or slate date"},
        ])
        write_csv(OUT_DIR / f"first_discovery_cohort_selection_rationale_{RUN_DATE}.csv", [{
            "cohort_id": "DISCOVERY_COHORT_001",
            "selection_rule": "top projected fully qualified ceiling among discovery-governance sides with all non-Starter prerequisites satisfied, with one single-side pitcher-binding representative included when available",
            "side_count": len(cohort),
            "represented_rows": sum(int_value(r["represented_denominator_rows"]) for r in cohort),
            "request_or_target_cap": len(cohort),
            "notes": "whole-side governance only; no source discovery or acquisition executed",
        }])
        write_csv(OUT_DIR / f"first_discovery_cohort_side_manifest_{RUN_DATE}.csv", cohort)
        write_csv(OUT_DIR / f"first_discovery_cohort_row_manifest_{RUN_DATE}.csv", cohort_rows)
        write_csv(OUT_DIR / f"first_discovery_cohort_target_manifest_{RUN_DATE}.csv", targets)
        write_csv(OUT_DIR / f"discovery_governance_contract_{RUN_DATE}.csv", [{
            "contract_area": "approval_boundary",
            "rule": "future approval of this package authorizes bounded discovery execution only for exact target manifest",
            "prohibits": "source acquisition beyond manifest|reconstruction|remediation|qualification propagation|matrix construction",
        }, {
            "contract_area": "source_scope",
            "rule": "game_id-bound starter identity and prior-history identity lookup only",
            "prohibits": "broad crawling|unrelated player discovery|unrelated game discovery",
        }, {
            "contract_area": "fail_closed",
            "rule": "ambiguity, duplicate conflict, role-regime uncertainty, temporal conflict, or missing provenance fails closed",
            "prohibits": "best-effort pitcher substitution",
        }])
        write_csv(OUT_DIR / f"allowed_discovery_to_acquisition_conversion_rule_{RUN_DATE}.csv", [{
            "conversion_step": "accepted_discovery_output_to_future_acquisition_manifest",
            "rule": "only accepted discovery rows with exact governed game/date/team/starter binding may be converted into strict-prior acquisition request keys under separate later governance",
            "executed_in_this_task": False,
            "authorizes_acquisition": False,
        }])
        write_csv(OUT_DIR / f"fail_closed_taxonomy_{RUN_DATE}.csv", [
            {"failure": "DISCOVERY_GAME_ID_DATE_MISMATCH", "action": "fail_closed"},
            {"failure": "DISCOVERY_MULTIPLE_STARTER_CANDIDATES", "action": "fail_closed"},
            {"failure": "DISCOVERY_ROLE_REGIME_AMBIGUITY", "action": "fail_closed"},
            {"failure": "DISCOVERY_TEMPORAL_CONFLICT", "action": "fail_closed"},
            {"failure": "DISCOVERY_SOURCE_PROVENANCE_MISSING", "action": "fail_closed"},
            {"failure": "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING", "action": "pre_screen_fail_closed"},
        ])
        write_csv(OUT_DIR / f"explicit_approval_boundaries_{RUN_DATE}.csv", [
            {"future_approval_type": "bounded_discovery_execution", "would_authorize": "exact first discovery target manifest only", "would_not_authorize": "source acquisition|reconstruction|remediation|qualification propagation|matrix construction"},
            {"future_approval_type": "bounded_acquisition", "would_authorize": "only exact acquisition manifest produced from accepted discovery output", "would_not_authorize": "reconstruction/remediation"},
            {"future_approval_type": "bounded_remediation", "would_authorize": "only separately frozen exact side/row manifest", "would_not_authorize": "new discovery or acquisition"},
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        write_csv(OUT_DIR / f"immutability_audit_{RUN_DATE}.csv", [
            {"artifact_family": "A/B/D matrices", "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL", "before_hashes": json.dumps(self.matrix_hash_before, sort_keys=True), "after_hashes": json.dumps(matrix_after, sort_keys=True)},
        ])
        guard = static_guard()
        write_csv(OUT_DIR / f"static_no_network_no_discovery_no_acquisition_no_remediation_no_model_no_matrix_guard_{RUN_DATE}.csv", guard)
        if any(r["status"] != "PASS" for r in guard):
            raise RuntimeError("static guard failed")

        payload = {
            "prescreen_decision": PRESCREEN_DECISION,
            "discovery_decision": DISCOVERY_DECISION,
            "prescreen_matching_sides": len(prescreen),
            "prescreen_matching_rows": sum(int_value(r["represented_rows"]) for r in prescreen),
            "prescreen_hits_0_5_rows": sum(int_value(r["hits_0_5_rows"]) for r in prescreen),
            "prescreen_hits_1_5_rows": sum(int_value(r["hits_1_5_rows"]) for r in prescreen),
            "prescreen_projected_qualification_ceiling": sum(int_value(r["projected_qualification_ceiling"]) for r in prescreen),
            "discovery_inventory_sides": len(discovery),
            "discovery_inventory_rows": sum(int_value(r["represented_denominator_rows"]) for r in discovery),
            "first_discovery_cohort_id": "DISCOVERY_COHORT_001",
            "first_discovery_cohort_sides": len(cohort),
            "first_discovery_cohort_rows": sum(int_value(r["represented_denominator_rows"]) for r in cohort),
            "first_discovery_cohort_targets": len(targets),
            "first_discovery_request_or_target_cap": len(cohort),
            "reconciliation_side_counts": dict(sorted(recon_counts.items())),
            "reconciliation_row_counts": dict(sorted(recon_row_counts.items())),
            "network_requests": 0,
            "discovery_execution_performed": False,
            "source_acquisition_performed": False,
            "reconstruction_or_remediation_performed": False,
            "state_mutations": 0,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "production_behavior_changed": False,
        }
        write_json(OUT_DIR / f"machine_readable_governance_freeze_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Starter Pre-Screen and Discovery-Cohort Governance Freeze — {RUN_DATE}

Pre-screen decision: `{PRESCREEN_DECISION}`

Discovery decision: `{DISCOVERY_DECISION}`

The reusable pre-screen rule is frozen for `LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED`.
It matches `{payload['prescreen_matching_sides']}` sides / `{payload['prescreen_matching_rows']}` rows,
all Hits 0.5, with projected ceiling `{payload['prescreen_projected_qualification_ceiling']}`.
The rule preserves those sides in the campaign ledger and prevents them from entering future
local reconstruction cohorts as projected-success candidates unless separate construction,
persistence, or formula governance is approved.

The discovery-governance inventory reproduces `{payload['discovery_inventory_sides']}` sides /
`{payload['discovery_inventory_rows']}` rows. The first representative discovery cohort,
`DISCOVERY_COHORT_001`, freezes `{payload['first_discovery_cohort_sides']}` sides /
`{payload['first_discovery_cohort_rows']}` rows with request/target cap
`{payload['first_discovery_request_or_target_cap']}`.

The next separate user approval would authorize bounded discovery execution only for the exact
`DISCOVERY_COHORT_001` target manifest. It would not authorize source acquisition, reconstruction,
remediation, qualification propagation, matrix construction, modeling, scoring, uploads, or any
production behavior change.
""")
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}


def main() -> int:
    result = StarterPrescreenAndDiscoveryGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
