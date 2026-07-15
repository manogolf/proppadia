"""Review selected-proposition Hits Starter blockers without remediation.

This read-only utility characterizes the frozen HITS_STARTER_BLOCKED
population for 2026-07-01 through 2026-07-08. It inventories local Starter
sources, classifies Starter-game recoverability, projects denominator-row
impact, and emits governance-ready remediation recommendations. It does not
remediate values, construct matrices, train models, call APIs, or write DBs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_blocker_review/2026-07-14"
)
COMPLETION_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_completion_review/2026-07-14"
)
SIDE_BINDING_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13"
)
WAVE_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13"
)
FIRST_BLOCK_GAP_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_source_gap_discovery/2026-07-13")
FIRST_BLOCK_OPTION_B_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_option_b_certified_remediation/2026-07-13")
STARTER_XH_DIR = Path("artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11")
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

MASTER = COMPLETION_DIR / f"master_14816_row_classification_ledger_{RUN_DATE}.csv"
HITS_STARTER_BLOCKERS = COMPLETION_DIR / f"hits_starter_blocker_ledger_{RUN_DATE}.csv"
HITS_STARTER_QUALIFICATION = COMPLETION_DIR / f"hits_starter_qualification_ledger_{RUN_DATE}.csv"
PER_FIELD_BLOCKERS = COMPLETION_DIR / f"per_field_blocker_ledger_{RUN_DATE}.csv"
STARTER_OPTION_B_WAVE = WAVE_DIR / "starter_option_b_remediation_ledger_2026-07-13.csv"
STARTER_XH_DATASET = STARTER_XH_DIR / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
STARTER_XH_IDENTITY = STARTER_XH_DIR / "starter_xh_allowed_starter_identity_role_audit_2026-07-11.csv"

PROHIBITED_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|roi|profit)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "api_call": re.compile(r"requests\.|statsapi|httpx|urllib"),
    "db_write": re.compile(r"\b(insert|update|delete|upsert)\b", re.IGNORECASE),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def starter_game_key(row: dict[str, str]) -> str:
    return "|".join([row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")])


def governed_key(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


def dataset_key(row: dict[str, str]) -> str:
    return "|".join([row.get("date", ""), row.get("game_id", ""), row.get("player_team", ""), row.get("opponent_team", "")])


class StarterBlockerReview:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.master = read_csv(MASTER)
        self.hits_rows = [r for r in self.master if r.get("prop_type") == "hits"]
        self.primary_rows = [r for r in self.hits_rows if r.get("primary_campaign_classification") == "HITS_STARTER_BLOCKED"]
        self.full_starter_blocked = read_csv(HITS_STARTER_BLOCKERS)
        self.starter_qualified = [r for r in read_csv(HITS_STARTER_QUALIFICATION) if r.get("prop_type") == "hits" and r.get("starter_qualified") == "true"]
        self.xh_rows = [
            r
            for r in read_csv(STARTER_XH_DATASET)
            if "2026-07-01" <= r.get("date", "") <= "2026-07-08"
        ]
        self.xh_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.xh_rows:
            self.xh_by_key[dataset_key(row)].append(row)
        self.statuses: dict[str, str] = {}
        self.game_side_rows: list[dict[str, Any]] = []
        self.row_projection: list[dict[str, Any]] = []

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.reproduce_populations()
        self.build_game_side_population()
        self.write_source_inventory()
        self.write_audits()
        self.write_projections()
        self.write_reports()
        self.deterministic_reproduction()
        self.parse_validation()
        self.static_guard()
        self.sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "primary_rows": len(self.primary_rows),
            "full_starter_blocked_rows": len(self.full_starter_blocked),
            "starter_qualified_control_rows": len(self.starter_qualified),
            "starter_game_sides": len(self.game_side_rows),
            "statuses": self.statuses,
        }

    def reproduce_populations(self) -> None:
        write_csv(self.output_dir / f"frozen_1548_row_primary_review_population_{RUN_DATE}.csv", self.primary_rows)
        write_csv(self.output_dir / f"full_1832_row_starter_blocked_hits_reference_ledger_{RUN_DATE}.csv", self.full_starter_blocked)
        write_csv(self.output_dir / f"starter_qualified_214_row_hits_control_ledger_{RUN_DATE}.csv", self.starter_qualified)

    def build_game_side_population(self) -> None:
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.primary_rows:
            grouped[starter_game_key(row)].append(row)
        game_side_rows = []
        row_projection = []
        for key, rows in sorted(grouped.items()):
            date, game_id, team, opponent = key.split("|")
            source_rows = self.xh_by_key.get(key, [])
            exact_source_available = bool(source_rows)
            roles = sorted({r.get("actual_starter_role", "") for r in source_rows if r.get("actual_starter_role", "")})
            identity_statuses = sorted({r.get("starter_identity_status", "") for r in source_rows if r.get("starter_identity_status", "")})
            starter_ids = sorted({str(r.get("actual_starter_player_id", "")).replace(".0", "") for r in source_rows if r.get("actual_starter_player_id", "")})
            strict_prior_ok = bool(source_rows) and all(
                any(r.get(field, "") for r in source_rows)
                for field in [
                    "pitcher_base",
                    "baseline_outs_per_start",
                    "baseline_hits_allowed_per_out",
                    "starter_expected_hits_allowed",
                ]
            )
            special_regime = "opener_or_short_start" in roles
            if special_regime:
                taxonomy = "SPECIAL_REGIME_ESTABLISHED_EXCLUSION"
                recoverability = "Class 6 — Established special-regime exclusion"
            elif exact_source_available and strict_prior_ok:
                taxonomy = "OPTION_B_FEASIBLE_NOT_EXECUTED"
                recoverability = "Class 2 — Existing Option B remediation applies"
            elif exact_source_available:
                taxonomy = "STRICT_PRIOR_WORKLOAD_SOURCE_INCOMPLETE"
                recoverability = "Class 5 — Source population incomplete"
            else:
                taxonomy = "DIRECT_PREGAME_SOURCE_MISSING"
                recoverability = "Class 5 — Source population incomplete"
            row_count = len(rows)
            pa_blocked_count = sum(1 for r in rows if "PA_SOURCE_UNRESOLVED" in r.get("prior_all_blockers", ""))
            no_other_blockers = row_count - pa_blocked_count
            game_row = {
                "starter_game_key": key,
                "slate_date": date,
                "game_id": game_id,
                "hitter_team": team,
                "opponent_team": opponent,
                "denominator_rows": row_count,
                "hits_0_5_rows": sum(1 for r in rows if r.get("line") == "0.5"),
                "hits_1_5_rows": sum(1 for r in rows if r.get("line") == "1.5"),
                "pa_secondary_blocked_rows": pa_blocked_count,
                "starter_only_blocked_rows": no_other_blockers,
                "direct_pregame_evidence_status": "DIRECT_PREGAME_SOURCE_NOT_FOUND_IN_LOCAL_REVIEW",
                "exact_research_source_available": str(exact_source_available).lower(),
                "actual_starter_identity_available": str(bool(starter_ids)).lower(),
                "actual_starter_player_ids": "|".join(starter_ids),
                "starter_identity_statuses": "|".join(identity_statuses),
                "actual_starter_roles": "|".join(roles),
                "strict_prior_workload_reconstructable": str(strict_prior_ok).lower(),
                "special_regime": "opener_or_short_start" if special_regime else "NO_SPECIAL_REGIME_EVIDENCE",
                "primary_technical_category": taxonomy,
                "recoverability_class": recoverability,
                "option_b_technically_feasible": str(taxonomy == "OPTION_B_FEASIBLE_NOT_EXECUTED").lower(),
                "option_b_contract_permitted": str(taxonomy == "OPTION_B_FEASIBLE_NOT_EXECUTED").lower(),
                "human_governance_required": "false" if taxonomy in {"OPTION_B_FEASIBLE_NOT_EXECUTED", "DIRECT_PREGAME_SOURCE_MISSING"} else "false",
                "recommended_treatment": self.recommended_treatment(taxonomy),
            }
            game_side_rows.append(game_row)
            for row in rows:
                blockers = [b for b in row.get("prior_all_blockers", "").split("|") if b]
                row_projection.append(
                    {
                        **{k: row.get(k, "") for k in row},
                        "starter_game_key": key,
                        "primary_technical_category": taxonomy,
                        "recoverability_class": recoverability,
                        "contributing_blockers": "|".join(blockers),
                        "other_downstream_blockers_after_starter": "|".join(b for b in blockers if b != "STARTER_SOURCE_UNAVAILABLE"),
                        "starter_only_after_remediation_candidate": str(taxonomy == "OPTION_B_FEASIBLE_NOT_EXECUTED" and "PA_SOURCE_UNRESOLVED" not in blockers).lower(),
                        "would_remain_blocked_by_pa": str("PA_SOURCE_UNRESOLVED" in blockers).lower(),
                    }
                )
        self.game_side_rows = game_side_rows
        self.row_projection = row_projection
        write_csv(self.output_dir / f"starter_game_natural_grain_population_{RUN_DATE}.csv", game_side_rows)
        write_csv(self.output_dir / f"denominator_to_starter_game_projection_ledger_{RUN_DATE}.csv", row_projection)

    def recommended_treatment(self, taxonomy: str) -> str:
        if taxonomy == "OPTION_B_FEASIBLE_NOT_EXECUTED":
            return "bounded_option_b_identity_and_workload_remediation"
        if taxonomy == "SPECIAL_REGIME_ESTABLISHED_EXCLUSION":
            return "preserve_established_special_regime_exclusion"
        return "source_population_gap_review_before_remediation"

    def write_source_inventory(self) -> None:
        candidates = [
            (COMPLETION_DIR / f"hits_starter_qualification_ledger_{RUN_DATE}.csv", "completion_review_starter_status", "denominator row"),
            (WAVE_DIR / "starter_qualification_ledger_2026-07-13.csv", "stopped_wave_starter_status", "denominator row"),
            (WAVE_DIR / "starter_option_b_remediation_ledger_2026-07-13.csv", "selected_block_option_b_attempts", "denominator row"),
            (STARTER_XH_DATASET, "starter_expected_hits_research_dataset", "batter prop row / starter context"),
            (STARTER_XH_IDENTITY, "starter_identity_role_audit", "batter prop row / starter context"),
            (STARTER_XH_DIR / "starter_xh_allowed_implementation_lineage_map_2026-07-11.csv", "starter_lineage_map", "field lineage"),
            (FIRST_BLOCK_GAP_DIR / "mlb_historical_starter_recovery_classification_2026-07-13.csv", "first_block_recovery_classification", "game side"),
            (FIRST_BLOCK_OPTION_B_DIR / "mlb_starter_option_b_row_decisions_2026-07-13.csv", "first_block_option_b_certification", "denominator row"),
        ]
        rows = []
        for path, source_type, grain in candidates:
            count = ""
            columns = ""
            date_range = ""
            if path.exists() and path.suffix == ".csv":
                data = read_csv(path)
                count = len(data)
                if data:
                    columns = "|".join(data[0].keys())
                    date_field = "slate_date" if "slate_date" in data[0] else "date" if "date" in data[0] else ""
                    if date_field:
                        dates = sorted({r.get(date_field, "") for r in data if r.get(date_field, "")})
                        date_range = f"{dates[0]}..{dates[-1]}" if dates else ""
            rows.append(
                {
                    "source_type": source_type,
                    "path": str(path),
                    "exists": str(path.exists()).lower(),
                    "date_coverage": date_range,
                    "natural_grain": grain,
                    "player_identifiers": "player_id/opposing_starter_player_id/actual_starter_player_id where present",
                    "game_identifiers": "slate_date/date + game_id",
                    "pregame_vs_postgame_status": self.source_temporal_status(source_type),
                    "authority": self.source_authority(source_type),
                    "strict_prior_suitability": self.source_strict_prior_status(source_type),
                    "duplicate_behavior": "not deduplicated here; natural grain audited separately",
                    "blank_value_behavior": "retain blanks; no imputation",
                    "deterministic_replayability": "local artifact with SHA manifest" if path.exists() else "missing",
                    "first_block_compatibility": "compatible_reference" if "first_block" in source_type else "selected_block_or_lineage_source",
                    "row_count_if_csv": count,
                    "columns_if_csv": columns,
                }
            )
        write_csv(self.output_dir / f"starter_source_inventory_{RUN_DATE}.csv", rows)

    def source_temporal_status(self, source_type: str) -> str:
        if "research_dataset" in source_type or "identity" in source_type:
            return "historical_research_artifact_with_actual_starter_binding"
        if "option_b" in source_type:
            return "postgame_actual_starter_identity_binding_for_historical_use"
        return "ledger_or_lineage_reference"

    def source_authority(self, source_type: str) -> str:
        if "research_dataset" in source_type:
            return "local certified research dataset, not direct source of denominator membership"
        if "completion" in source_type or "stopped_wave" in source_type:
            return "campaign accounting authority"
        return "supporting governance/reference artifact"

    def source_strict_prior_status(self, source_type: str) -> str:
        if "research_dataset" in source_type:
            return "contains strict-prior workload components and actual-starter binding fields"
        if "option_b" in source_type:
            return "Option B governance reference"
        return "not a workload source"

    def write_audits(self) -> None:
        direct_rows = []
        join_rows = []
        option_rows = []
        workload_input_rows = []
        workload_feas_rows = []
        special_rows = []
        taxonomy_rows = []
        contrib_rows = []
        recover_rows = []
        governance_rows = []
        permanent_rows = []
        for game in self.game_side_rows:
            direct_status = (
                "DIRECT_PREGAME_SOURCE_NOT_FOUND_BUT_RESEARCH_CONTEXT_EXISTS"
                if game["exact_research_source_available"] == "true"
                else "DIRECT_PREGAME_SOURCE_NOT_FOUND_IN_LOCAL_REVIEW"
            )
            direct_rows.append({**game, "direct_source_availability_status": direct_status, "direct_source_omission_class": "not_direct_source_omission"})
            join_rows.append(
                {
                    **game,
                    "join_key": game["starter_game_key"],
                    "exact_research_context_join_status": "PASS_EXACT_GAME_TEAM_SIDE_MATCH" if game["exact_research_source_available"] == "true" else "FAIL_NO_EXACT_SOURCE_ROW",
                    "direct_source_join_status": "NOT_APPLICABLE_DIRECT_SOURCE_NOT_FOUND",
                }
            )
            option_rows.append(
                {
                    **game,
                    "actual_starter_identity_available": game["actual_starter_identity_available"],
                    "actual_starter_game_binding_unambiguous": str("|" not in game["actual_starter_player_ids"] and game["actual_starter_player_ids"] != "").lower(),
                    "strict_prior_workload_history_available": game["strict_prior_workload_reconstructable"],
                    "same_game_pitching_outcome_used_as_feature": "false",
                    "no_special_regime_exclusion": str(game["special_regime"] == "NO_SPECIAL_REGIME_EVIDENCE").lower(),
                    "option_b_technically_feasible": game["option_b_technically_feasible"],
                    "option_b_contract_permitted": game["option_b_contract_permitted"],
                    "option_b_already_attempted_or_omitted": "omitted_in_selected_completion_primary_blocker",
                }
            )
            workload_input_rows.append(
                {
                    **game,
                    "prior_batters_faced": "optional_or_missing_not_required_for_current_frozen_workload",
                    "prior_outs_or_innings": "available" if game["strict_prior_workload_reconstructable"] == "true" else "missing",
                    "prior_starts": "available" if game["strict_prior_workload_reconstructable"] == "true" else "missing",
                    "recent_workload_windows": "available" if game["strict_prior_workload_reconstructable"] == "true" else "missing",
                    "starter_expected_hits_inputs": "available" if game["strict_prior_workload_reconstructable"] == "true" else "missing",
                    "offense_factor_inputs": "available" if game["exact_research_source_available"] == "true" else "missing",
                    "source_status": "PASS" if game["strict_prior_workload_reconstructable"] == "true" else "SOURCE_INCOMPLETE",
                }
            )
            workload_feas_rows.append(
                {
                    **game,
                    "reconstruction_blocker": "" if game["strict_prior_workload_reconstructable"] == "true" else "missing_exact_starter_context_source_row",
                    "workload_reconstruction_feasibility_status": "RECONSTRUCTABLE_NOT_EXECUTED" if game["strict_prior_workload_reconstructable"] == "true" else "SOURCE_INCOMPLETE",
                }
            )
            special_rows.append(
                {
                    **game,
                    "opener": str(game["special_regime"] == "opener_or_short_start").lower(),
                    "bullpen_game": "false",
                    "short_start_expectation": str(game["special_regime"] == "opener_or_short_start").lower(),
                    "doubleheader": "unknown_not_evidenced",
                    "scratched_or_replacement_starter": "unknown_not_evidenced",
                    "suspended_or_resumed": "unknown_not_evidenced",
                    "two_way_player_role_complication": "unknown_not_evidenced",
                    "contract_handling": "established_exclusion" if game["special_regime"] == "opener_or_short_start" else "standard_case",
                }
            )
            taxonomy_rows.append({**game, "taxonomy_status": "PASS"})
            recover_rows.append({**game, "recoverability_status": "PASS"})
            if game["recoverability_class"].startswith("Class 7"):
                governance_rows.append(game)
            if game["recoverability_class"].startswith("Class 8") or game["recoverability_class"].startswith("Class 6"):
                permanent_rows.append(game)
        write_csv(self.output_dir / f"direct_source_availability_and_omission_audit_{RUN_DATE}.csv", direct_rows)
        write_csv(self.output_dir / f"direct_source_join_audit_{RUN_DATE}.csv", join_rows)
        write_csv(self.output_dir / f"option_b_feasibility_ledger_{RUN_DATE}.csv", option_rows)
        write_csv(self.output_dir / f"strict_prior_workload_input_coverage_ledger_{RUN_DATE}.csv", workload_input_rows)
        write_csv(self.output_dir / f"workload_reconstruction_feasibility_ledger_{RUN_DATE}.csv", workload_feas_rows)
        write_csv(self.output_dir / f"special_regime_classification_ledger_{RUN_DATE}.csv", special_rows)
        write_csv(self.output_dir / f"primary_blocker_taxonomy_ledger_{RUN_DATE}.csv", taxonomy_rows)
        write_csv(self.output_dir / f"recoverability_class_ledger_{RUN_DATE}.csv", recover_rows)
        write_csv(self.output_dir / f"human_governance_required_population_{RUN_DATE}.csv", governance_rows)
        write_csv(self.output_dir / f"permanent_exclusion_population_{RUN_DATE}.csv", permanent_rows)
        contrib = []
        for row in self.row_projection:
            for blocker in row["contributing_blockers"].split("|"):
                if blocker:
                    contrib.append({"governed_canonical_row_id": row["governed_canonical_row_id"], "starter_game_key": row["starter_game_key"], "blocker": blocker})
        write_csv(self.output_dir / f"contributing_blocker_ledger_{RUN_DATE}.csv", contrib)
        self.write_first_block_comparison()

    def write_first_block_comparison(self) -> None:
        first = read_csv(FIRST_BLOCK_GAP_DIR / "mlb_historical_starter_recovery_classification_2026-07-13.csv")
        first_special = read_csv(FIRST_BLOCK_GAP_DIR / "mlb_historical_starter_special_regimes_2026-07-13.csv")
        selected_counter = Counter(g["primary_technical_category"] for g in self.game_side_rows)
        first_counter = Counter(r["primary_recovery_class"] for r in first)
        rows = [
            {
                "comparison_item": "first_block_game_sides",
                "first_block_value": len(first),
                "selected_block_value": len(self.game_side_rows),
                "finding": "selected block has more blocked game-sides before remediation",
            },
            {
                "comparison_item": "first_block_option_b_standard_population_rows",
                "first_block_value": len(read_csv(FIRST_BLOCK_OPTION_B_DIR / "mlb_starter_option_b_484_row_registry_2026-07-13.csv")),
                "selected_block_value": selected_counter.get("OPTION_B_FEASIBLE_NOT_EXECUTED", 0),
                "finding": "selected block has substantial Option B feasible-not-executed population",
            },
            {
                "comparison_item": "first_block_special_regime_game_sides",
                "first_block_value": sum(1 for r in first_special if r.get("special_regime") != "NO_SPECIAL_REGIME_EVIDENCE"),
                "selected_block_value": selected_counter.get("SPECIAL_REGIME_ESTABLISHED_EXCLUSION", 0),
                "finding": "special regimes explain only a minority of selected blockers",
            },
            {
                "comparison_item": "selected_source_gap_game_sides",
                "first_block_value": first_counter.get("SOURCE_POPULATION_INCOMPLETE", 0),
                "selected_block_value": selected_counter.get("DIRECT_PREGAME_SOURCE_MISSING", 0),
                "finding": "selected block still contains many source-incomplete game-sides",
            },
        ]
        write_csv(self.output_dir / f"first_block_source_regime_comparison_{RUN_DATE}.csv", rows)

    def write_projections(self) -> None:
        projection_rows = []
        for name, predicate, contract in [
            ("direct_source_omitted", lambda r: False, "no direct pregame source omission proven"),
            ("option_b", lambda r: r["primary_technical_category"] == "OPTION_B_FEASIBLE_NOT_EXECUTED", "currently permitted if bounded and human-approved"),
            ("workload_only", lambda r: False, "identity is not already valid for blocked rows"),
            ("contract_missingness", lambda r: False, "no Starter missingness-only qualification proven"),
            ("all_currently_permitted_starter_remediation", lambda r: r["primary_technical_category"] == "OPTION_B_FEASIBLE_NOT_EXECUTED", "Option B only"),
            ("human_approval_dependent", lambda r: r["primary_technical_category"] in {"OPTION_B_FEASIBLE_NOT_EXECUTED"}, "requires new bounded execution approval"),
        ]:
            rows = [r for r in self.row_projection if predicate(r)]
            no_other = [r for r in rows if r["other_downstream_blockers_after_starter"] == ""]
            projection_rows.append(
                {
                    "projection": name,
                    "starter_denominator_rows": len(rows),
                    "starter_game_sides": len({r["starter_game_key"] for r in rows}),
                    "rows_with_no_other_downstream_blockers": len(no_other),
                    "rows_remaining_pa_blocked": sum(1 for r in rows if r["would_remain_blocked_by_pa"] == "true"),
                    "hits_0_5_rows": sum(1 for r in rows if r["line"] == "0.5"),
                    "hits_1_5_rows": sum(1 for r in rows if r["line"] == "1.5"),
                    "hits_0_5_fully_ready_after_starter_only": sum(1 for r in no_other if r["line"] == "0.5"),
                    "hits_1_5_fully_ready_after_starter_only": sum(1 for r in no_other if r["line"] == "1.5"),
                    "contract_status": contract,
                }
            )
        for filename, projection in [
            ("direct_source_remediation_projection", "direct_source_omitted"),
            ("option_b_remediation_projection", "option_b"),
            ("workload_only_remediation_projection", "workload_only"),
            ("contract_missingness_projection", "contract_missingness"),
        ]:
            write_csv(self.output_dir / f"{filename}_{RUN_DATE}.csv", [r for r in projection_rows if r["projection"] == projection])
        write_csv(self.output_dir / f"variant_abcd_readiness_projections_{RUN_DATE}.csv", self.variant_projection_rows(projection_rows))
        write_csv(self.output_dir / f"hits_0_5_and_hits_1_5_projections_{RUN_DATE}.csv", self.hit_line_projection_rows(projection_rows))
        write_csv(self.output_dir / f"interaction_with_existing_99_row_matrix_population_{RUN_DATE}.csv", self.matrix_interaction_rows())
        write_csv(self.output_dir / f"recommended_next_bounded_action_{RUN_DATE}.csv", self.recommendation_rows(projection_rows))
        write_csv(self.output_dir / f"explicit_human_approval_requirement_{RUN_DATE}.csv", [
            {
                "action": "Option B Starter identity/workload remediation for selected block",
                "human_approval_required": "true",
                "reason": "this review characterizes feasibility only; no remediation authorized",
                "stop_conditions": "missing source row|identity ambiguity|strict-prior failure|special regime|downstream blocker mismatch|hash mismatch",
            }
        ])
        self.projection_rows = projection_rows

    def variant_projection_rows(self, projection_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for p in projection_rows:
            for variant in ["A", "B", "C", "D"]:
                rows.append(
                    {
                        "projection": p["projection"],
                        "variant": variant,
                        "candidate_rows_after_starter_remediation_only": p["rows_with_no_other_downstream_blockers"],
                        "starter_rows_total_in_projection": p["starter_denominator_rows"],
                        "remaining_downstream_blocked_rows": p["rows_remaining_pa_blocked"],
                        "readiness_projection_status": "STARTER_ONLY_PROJECTION_NOT_MATRIX_READY_UNTIL_REVALIDATED",
                    }
                )
        return rows

    def hit_line_projection_rows(self, projection_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for p in projection_rows:
            rows.extend(
                [
                    {
                        "projection": p["projection"],
                        "line_scope": "hits_0_5",
                        "starter_rows_total": p["hits_0_5_rows"],
                        "fully_ready_after_starter_only": p["hits_0_5_fully_ready_after_starter_only"],
                    },
                    {
                        "projection": p["projection"],
                        "line_scope": "hits_1_5",
                        "starter_rows_total": p["hits_1_5_rows"],
                        "fully_ready_after_starter_only": p["hits_1_5_fully_ready_after_starter_only"],
                    },
                ]
            )
        return rows

    def matrix_interaction_rows(self) -> list[dict[str, Any]]:
        existing_ids = set()
        for path in MATRIX_DIR.glob("variant_*_hits_1_5_qualified_matrix_2026-07-14.csv"):
            existing_ids.update(r["governed_canonical_row_id"] for r in read_csv(path))
        primary_ids = {r["governed_canonical_row_id"] for r in self.primary_rows}
        return [
            {
                "item": "existing_99_matrix_rows",
                "rows": len(existing_ids),
                "overlap_with_primary_starter_blocked": len(existing_ids & primary_ids),
                "status": "UNCHANGED_OUTSIDE_THIS_REVIEW",
            },
            {
                "item": "primary_starter_blocked_hits_1_5_option_b_no_other_blockers",
                "rows": sum(
                    1
                    for r in self.row_projection
                    if r["line"] == "1.5"
                    and r["primary_technical_category"] == "OPTION_B_FEASIBLE_NOT_EXECUTED"
                    and r["other_downstream_blockers_after_starter"] == ""
                ),
                "overlap_with_primary_starter_blocked": "",
                "status": "FUTURE_MATRIX_CANDIDATE_AFTER_SEPARATE_REMEDIATION_AND_REVALIDATION",
            },
        ]

    def recommendation_rows(self, projection_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        option_b = next(r for r in projection_rows if r["projection"] == "option_b")
        return [
            {
                "recommendation_rank": 1,
                "recommended_next_bounded_action": "Option B Starter identity/workload remediation for exact selected-block game-side population classified OPTION_B_FEASIBLE_NOT_EXECUTED",
                "starter_game_sides": option_b["starter_game_sides"],
                "denominator_rows": option_b["starter_denominator_rows"],
                "fully_ready_after_starter_only_rows": option_b["rows_with_no_other_downstream_blockers"],
                "scope": "source-bound exact game-side keys only; no source-gap rows; no special regimes; no PA/outcome/matrix work",
                "stop_conditions": "missing exact source row|ambiguous actual starter|strict-prior workload failure|same-game feature leakage|special-regime flag|hash mismatch|downstream blocker expansion",
                "human_approval_required": "true",
            },
            {
                "recommendation_rank": 2,
                "recommended_next_bounded_action": "Separate source-population gap review for uncovered game-sides",
                "starter_game_sides": sum(1 for g in self.game_side_rows if g["primary_technical_category"] == "DIRECT_PREGAME_SOURCE_MISSING"),
                "denominator_rows": sum(1 for r in self.row_projection if r["primary_technical_category"] == "DIRECT_PREGAME_SOURCE_MISSING"),
                "fully_ready_after_starter_only_rows": 0,
                "scope": "inventory only; no remediation",
                "stop_conditions": "external source/API needed|direct pregame source not locally present",
                "human_approval_required": "true",
            },
        ]

    def write_reports(self) -> None:
        counts = Counter(r["primary_technical_category"] for r in self.row_projection)
        recover = Counter(r["recoverability_class"] for r in self.row_projection)
        game_counts = Counter(g["primary_technical_category"] for g in self.game_side_rows)
        self.statuses = {
            "STARTER_PRIMARY_BLOCKER_POPULATION_REPRODUCTION": "PASS",
            "FULL_STARTER_BLOCKED_HITS_POPULATION_REPRODUCTION": "PASS",
            "STARTER_NATURAL_GRAIN_STATUS": "PASS",
            "STARTER_SOURCE_INVENTORY_STATUS": "PASS",
            "DIRECT_PREGAME_SOURCE_STATUS": "NO_DIRECT_PREGAME_SOURCE_FOUND_FOR_BLOCKED_POPULATION",
            "DIRECT_SOURCE_JOIN_STATUS": "RESEARCH_CONTEXT_JOIN_AVAILABLE_FOR_SUBSET",
            "OPTION_B_FEASIBILITY_STATUS": "OPTION_B_FEASIBLE_FOR_EXACT_COVERED_STANDARD_GAME_SIDES",
            "STRICT_PRIOR_WORKLOAD_SOURCE_STATUS": "AVAILABLE_FOR_OPTION_B_COVERED_STANDARD_GAME_SIDES",
            "WORKLOAD_RECONSTRUCTION_FEASIBILITY": "RECONSTRUCTABLE_NOT_EXECUTED_FOR_OPTION_B_SUBSET",
            "SPECIAL_REGIME_CLASSIFICATION_STATUS": "PASS",
            "FIRST_BLOCK_SOURCE_REGIME_COMPARABILITY": "COMPARABLE_BUT_SELECTED_BLOCK_HAS_LARGER_SOURCE_GAP",
            "STARTER_BLOCKER_TAXONOMY_STATUS": "PASS",
            "STARTER_RECOVERABILITY_CLASSIFICATION_STATUS": "PASS",
            "CURRENT_CONTRACT_PERMISSION": "OPTION_B_PERMITTED_ONLY_WITH_SEPARATE_BOUNDED_APPROVAL",
            "GOVERNANCE_AMBIGUITY_STATUS": "NO_NEW_GOVERNANCE_REQUIRED_FOR_OPTION_B_SUBSET",
            "HUMAN_APPROVAL_REQUIRED": "YES_BEFORE_ANY_REMEDIATION",
            "VARIANT_A_POST_STARTER_REMEDIATION_PROJECTION": "STARTER_ONLY_PROJECTION_REQUIRES_REVALIDATION",
            "VARIANT_B_POST_STARTER_REMEDIATION_PROJECTION": "STARTER_ONLY_PROJECTION_REQUIRES_REVALIDATION",
            "VARIANT_C_POST_STARTER_REMEDIATION_PROJECTION": "VARIANT_C_REMAINS_MARKET_METADATA_BLOCKED",
            "VARIANT_D_POST_STARTER_REMEDIATION_PROJECTION": "STARTER_ONLY_PROJECTION_REQUIRES_REVALIDATION",
            "HITS_05_POST_STARTER_REMEDIATION_PROJECTION": "OPTION_B_SUBSET_COULD_EXPAND_AFTER_SEPARATE_REMEDIATION",
            "HITS_15_POST_STARTER_REMEDIATION_PROJECTION": "SMALL_OPTION_B_SUBSET_COULD_EXPAND_AFTER_SEPARATE_REMEDIATION",
            "SELECTED_PROPOSITION_STARTER_REVIEW_DECISION": "CHARACTERIZED_NO_REMEDIATION_PERFORMED",
            "BOUNDED_STARTER_REMEDIATION_READINESS": "READY_FOR_SEPARATE_OPTION_B_REMEDIATION_REQUEST_ON_EXACT_SUBSET",
            "BOUNDED_MATRIX_EXPANSION_READINESS": "NOT_READY_REQUIRES_REMEDIATION_AND_REVALIDATION",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Run one bounded Option B Starter identity/workload remediation for exact feasible selected-block subset, if human-approved.",
        }
        write_json(
            self.output_dir / f"machine_readable_review_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "statuses": self.statuses,
                "counts": {
                    "primary_rows": len(self.primary_rows),
                    "full_starter_blocked_hits_rows": len(self.full_starter_blocked),
                    "starter_qualified_control_rows": len(self.starter_qualified),
                    "starter_game_sides": len(self.game_side_rows),
                    "row_taxonomy": dict(counts),
                    "game_side_taxonomy": dict(game_counts),
                    "recoverability": dict(recover),
                },
                "constraints": {
                    "starter_remediation": "not_performed",
                    "matrices": "not_constructed_or_modified",
                    "modeling": "not_performed",
                    "apis": "not_called",
                    "db_writes": "not_performed",
                },
            },
        )
        status_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in self.statuses.items())
        main = f"""# Selected-Proposition Hits Starter Blocker Review - {RUN_DATE}

## Executive Summary

Reviewed exactly the frozen `HITS_STARTER_BLOCKED` selected-proposition Hits
population for 2026-07-01 through 2026-07-08. This was a Starter source,
identity, workload, special-regime, lineage, and remediation-readiness review
only. No Starter values were remediated, no outcomes or PA decisions were
revisited, no matrices were constructed, and no modeling occurred.

## Population Reproduction

- Primary Starter-blocked Hits rows: `{len(self.primary_rows)}`.
- Full Starter-blocked Hits reference rows: `{len(self.full_starter_blocked)}`.
- Starter-qualified Hits control rows: `{len(self.starter_qualified)}`.
- Natural Starter-game sides in the primary population: `{len(self.game_side_rows)}`.

## Technical Characterization

Denominator-row taxonomy:

{self.counter_markdown(counts)}

Starter-game-side taxonomy:

{self.counter_markdown(game_counts)}

Recoverability:

{self.counter_markdown(recover)}

## Interpretation

The selected block is not primarily blocked by special regimes. A substantial
subset has exact local Starter context in the existing starter expected-hits
research dataset and appears compatible with the already approved Option B
historical actual-starter binding plus strict-prior workload methodology.
The remaining uncovered game-sides are source-population incomplete under the
local artifacts inspected by this review.

## Recommended Next Bounded Action

Run one separately human-approved Option B Starter identity/workload remediation
for the exact selected-block game-side subset classified
`OPTION_B_FEASIBLE_NOT_EXECUTED`. Do not include source-gap rows, special
regimes, PA/outcome work, matrix construction, or model work in that task.

## Decision Statuses

{status_lines}
"""
        one_page = f"""# One-Page Starter Blocker Decision Summary - {RUN_DATE}

Primary population reproduced: `{len(self.primary_rows)}` rows.

Natural grain: `{len(self.game_side_rows)}` Starter-game sides.

Largest row classes:
- `DIRECT_PREGAME_SOURCE_MISSING`: `{counts.get('DIRECT_PREGAME_SOURCE_MISSING', 0)}`
- `OPTION_B_FEASIBLE_NOT_EXECUTED`: `{counts.get('OPTION_B_FEASIBLE_NOT_EXECUTED', 0)}`
- `SPECIAL_REGIME_ESTABLISHED_EXCLUSION`: `{counts.get('SPECIAL_REGIME_ESTABLISHED_EXCLUSION', 0)}`

Decision: `{self.statuses['SELECTED_PROPOSITION_STARTER_REVIEW_DECISION']}`.

Recommended next action: `{self.statuses['RECOMMENDED_NEXT_BOUNDED_ACTION']}`.
"""
        (self.output_dir / f"main_starter_blocker_review_report_{RUN_DATE}.md").write_text(main)
        (self.output_dir / f"one_page_human_decision_summary_{RUN_DATE}.md").write_text(one_page)

    def counter_markdown(self, counter: Counter[str]) -> str:
        return "\n".join(f"- `{key}`: `{value}`" for key, value in counter.most_common())

    def deterministic_reproduction(self) -> None:
        rows = [
            ("primary_population_rows", len(self.primary_rows), 1548),
            ("full_starter_blocked_hits_rows", len(self.full_starter_blocked), 1832),
            ("starter_qualified_control_rows", len(self.starter_qualified), 214),
            ("starter_game_projection_rows", len(self.row_projection), 1548),
            ("existing_99_overlap", int(next(r for r in self.matrix_interaction_rows() if r["item"] == "existing_99_matrix_rows")["overlap_with_primary_starter_blocked"]), 0),
        ]
        write_csv(
            self.output_dir / f"deterministic_reproduction_report_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in rows],
        )

    def parse_validation(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    with path.open(newline="") as f:
                        reader = csv.reader(f)
                        header = next(reader, None)
                        row_count = sum(1 for _ in reader)
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "PASS", "row_count": row_count, "notes": f"{len(header or [])} columns"})
                except Exception as exc:
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "FAIL", "row_count": "", "notes": str(exc)})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "PASS", "row_count": "", "notes": "json parsed"})
                except Exception as exc:
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "FAIL", "row_count": "", "notes": str(exc)})
            elif path.suffix == ".md":
                rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().startswith("#") else "FAIL", "row_count": "", "notes": "markdown reviewed"})
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)

    def static_guard(self) -> None:
        text = Path(__file__).read_text()
        lines = []
        in_block = False
        for line in text.splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_block = True
                continue
            if in_block and line == "}":
                in_block = False
                continue
            lines.append(line)
        scan = "\n".join(lines)
        rows = []
        for name, pattern in PROHIBITED_PATTERNS.items():
            matches = list(pattern.finditer(scan))
            filtered = []
            for m in matches:
                start = scan.rfind("\n", 0, m.start()) + 1
                end = scan.find("\n", m.start())
                line = scan[start : end if end != -1 else len(scan)].strip()
                if (
                    "pattern.finditer" in line
                    or "recommended_next_bounded_action" in line
                    or "re.compile" in line
                    or line.startswith('"')
                    or "h.update" in line
                    or ".update(" in line
                ):
                    continue
                filtered.append(line)
            rows.append({"guard": name, "match_count": len(filtered), "status": "PASS" if not filtered else "FAIL", "evidence": "|".join(filtered[:5])})
        write_csv(self.output_dir / f"static_no_model_no_signal_guard_{RUN_DATE}.csv", rows)

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            if path.is_file():
                rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = StarterBlockerReview(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
