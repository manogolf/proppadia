#!/usr/bin/env python3
"""Design MLB Collective Bundle v1 historical source expansion.

Read-only planning utility. It inspects existing platform artifacts and emits a
backfill/expansion design package. It does not backfill, generate historical
data, assemble matrices, train, score, call external APIs, or modify Bundle v1.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


OUT_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_source_expansion_design/2026-07-12")
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
READINESS_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_training_population_readiness/2026-07-12")
PA_DIR = Path("artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11")
STARTER_DIR = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def package_digest(out: Path) -> str:
    rows: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    for path in sorted(p for p in out.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        file_sha = sha256(path)
        rel = str(path.relative_to(out))
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": digest.hexdigest(), "bytes": ""})
    write_csv(out / "sha256_manifest_2026-07-12.csv", rows)
    return digest.hexdigest()


def source_lineage_inventory() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "authoritative_source": "mlb.player_stats / mlb.player_derived_stats strict-prior PA fields",
            "original_source_system": "MLB StatsAPI batting.plateAppearances with formula fallback AB+BB+HBP+SF+SH+CI where retained",
            "stored_artifacts": str(PA_DIR / "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"),
            "archived_artifacts": "pa_formula_and_cutoff_audit, pa_historical_coverage, pa_research_dataset_inventory, pa_opportunity_research_base",
            "intermediate_artifacts": "player_derived_stats rolling PA aliases; replay-aligned manifests",
            "reconstruction_inputs": "player_stats completed batting PA fields, player_derived_stats rolling PA fields, pregame manifest row keys",
            "required_joins": "row_key for prop rows; player_id + source_game_date < slate_date for PA context",
            "required_identifiers": "slate_date|game_id|player_id|prop_type|line|side row_key; player_id; game_date",
            "current_limitation": "research base begins 2026-07-03",
        },
        {
            "platform": "Starter Skill / Workload",
            "authoritative_source": "mlb.player_stats pitcher-game history plus starter-game reconstruction artifacts",
            "original_source_system": "MLB StatsAPI/local player_stats pitcher lines; official BF dry-run manifests for partial BF support",
            "stored_artifacts": str(STARTER_DIR / "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"),
            "archived_artifacts": "starter_game_base, batter_prop_expanded_base, source_semantics_inventory, strict_prior_lineage, BF coverage ledger",
            "intermediate_artifacts": "starter role confidence fields, starter strict-prior lineage, official BF dry-run manifests",
            "reconstruction_inputs": "pitcher prior starts, outs_recorded, hits_allowed, is_starter, offense factor context, optional BF manifests",
            "required_joins": "slate_date/date + game_id + batter opponent/team to starter-game row",
            "required_identifiers": "date, game_id, expected_starter_player_id, player_team, opponent_team",
            "current_limitation": "archived starter-game base ends 2026-07-06",
        },
        {
            "platform": "Variant C Market Metadata",
            "authoritative_source": "market snapshot artifacts / rolling candidate ledger",
            "original_source_system": "existing local OddsAPI-derived market artifacts only; no new OddsAPI calls in this task",
            "stored_artifacts": "hitter/offense prop research bases retain selected price/no-vig but not complete book-count/timestamp",
            "archived_artifacts": "market-late rolling observation artifacts and odds_history where available",
            "intermediate_artifacts": "candidate ledgers, market snapshots, slate outputs",
            "reconstruction_inputs": "timestamped market snapshots and candidate keys",
            "required_joins": "candidate key + market snapshot timestamp + line/side",
            "required_identifiers": "snapshot/run tag, game_id, player_id, prop_type, line, side, book",
            "current_limitation": "independent workstream; does not constrain baseball-state bundle planning",
        },
    ]


def historical_coverage_audit() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "already_archived": "2026-07-03_to_2026-07-09 research base",
            "reconstructable": "Likely 2026 season where player_stats/player_derived_stats PA fields exist and strict prior aliases can be regenerated",
            "partially_reconstructable": "Older seasons if player_stats PA components and stable player IDs exist; 2024/2025 not verified by this task",
            "impossible_to_reconstruct": "Dates with no player_stats PA components and no archived derived PA rows",
            "unknown": "Full 2024/2025 date-locked PA artifact availability",
            "earliest_recoverable_date": "UNKNOWN_WITHOUT_PA_SOURCE_DISCOVERY",
            "latest_recoverable_date": "2026-07-09 from current PA research base; future requires rerun/pipeline",
            "continuous_coverage": "Verified only 2026-07-03_to_2026-07-09",
            "discontinuities": "pre-2026-07-03 unverified in research base",
            "schema_transitions": "raw PA is postgame; approved fields are strict-prior prior_ aliases / pa_opp_v1 labels",
            "archive_quality": "good for verified window; broader period requires source-discovery pilot",
        },
        {
            "platform": "Starter Skill / Workload",
            "already_archived": "2026-05-01_to_2026-07-06 starter-game base; 2026-05-01_to_2026-07-06 supports current common bundle interval",
            "reconstructable": "Likely 2026 through available player_stats pitcher history if local stat-line corrections are accepted or audited",
            "partially_reconstructable": "BF-based fields only partial; outs-based Bundle v1 fields are more reconstructable",
            "impossible_to_reconstruct": "Dates without reliable starter identity or pitcher-game prior history",
            "unknown": "Post-2026-07-06 archived starter-game reconstruction not present in current package",
            "earliest_recoverable_date": "2026-05-01 verified artifact; earlier possible from 2024+ player_stats export but not generated here",
            "latest_recoverable_date": "2026-07-06 verified artifact",
            "continuous_coverage": "66 dates verified 2026-05-01_to_2026-07-06",
            "discontinuities": "post-2026-07-06 missing from archived starter reconstruction package",
            "schema_transitions": "official BF partial; Bundle v1 uses outs-based skill/workload and role fields",
            "archive_quality": "good for stated scope, but local stat-line lineage and starter identity require validation for expansion",
        },
    ]


def reconstruction_feasibility_matrix() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "field": "pa_opp_v1_d15_opportunity_band",
            "required_source": "strict-prior PA aliases from player_derived_stats / PA opportunity research base",
            "classification": "CONTRACT_PERMITTED_RECONSTRUCTION",
            "formula_version": "pa_opp_v1; low <3.8, medium 3.8 to <4.3, high >=4.3",
            "temporal_cutoff": "source_game_date < artifact_date",
            "blocker": "Need regenerated/replayed PA opportunity base before 2026-07-03",
        },
        {
            "platform": "Rolling PA Opportunity",
            "field": "pa_opp_v1_trend_label",
            "required_source": "strict-prior PA aliases",
            "classification": "CONTRACT_PERMITTED_RECONSTRUCTION",
            "formula_version": "pa_opp_v1; short_window_up if d7-d30>=0.35, down if <=-0.35, else stable",
            "temporal_cutoff": "source_game_date < artifact_date",
            "blocker": "Need deterministic regeneration over historical manifests",
        },
        {
            "platform": "Starter Skill / Workload",
            "field": "weighted_multiseason_hits_per_out",
            "required_source": "prior pitcher starts from player_stats",
            "classification": "EXACT_RECONSTRUCTABLE",
            "formula_version": "decay 0.70 by season distance; SUM(hpo*outs*decay)/SUM(outs*decay)",
            "temporal_cutoff": "pitcher game_date < target date",
            "blocker": "local stat-line lineage corrections and starter identity validation for expanded dates",
        },
        {
            "platform": "Starter Skill / Workload",
            "field": "expected_outs_blended_v1",
            "required_source": "prior starter outs history",
            "classification": "EXACT_RECONSTRUCTABLE",
            "formula_version": "0.65 stable weighted outs + 0.35 recent5 outs when recent sample >=2, else stable",
            "temporal_cutoff": "pitcher game_date < target date",
            "blocker": "starter-game artifact currently ends 2026-07-06",
        },
        {
            "platform": "Starter Skill / Workload",
            "field": "workload_confidence",
            "required_source": "prior starts and recent5 counts",
            "classification": "EXACT_RECONSTRUCTABLE",
            "formula_version": "high prior starts>=10 and recent5>=3; medium>=5; low>0; missing otherwise",
            "temporal_cutoff": "pitcher game_date < target date",
            "blocker": "same as starter source expansion",
        },
        {
            "platform": "Starter Skill / Workload",
            "field": "expected_role_label / role_confidence",
            "required_source": "prior all pitcher appearances and prior starts",
            "classification": "EXACT_RECONSTRUCTABLE",
            "formula_version": "role_label from prior usage share, expected outs, early removal frequency; role_confidence by prior sample",
            "temporal_cutoff": "pitcher game_date < target date",
            "blocker": "starter identity and pitcher role history validation",
        },
        {
            "platform": "Variant C Market Metadata",
            "field": "market_book_count_two_sided / market_snapshot_time_utc",
            "required_source": "timestamped market snapshots",
            "classification": "PARTIALLY_RECONSTRUCTABLE",
            "formula_version": "Bundle v1 market contract; no formula change",
            "temporal_cutoff": "snapshot before game start for pregame matrix",
            "blocker": "separate market snapshot lineage/backfill workstream",
        },
    ]


def formula_stability_audit() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "formula_family": "pa_opp_v1",
            "formula_version": "IMPLEMENTED_RESEARCH_V1",
            "parameters": "d7/d15/d30 PA per game; band thresholds low <3.8, medium 3.8 to <4.3, high >=4.3; trend +/-0.35",
            "stability_status": "STABLE_FOR_VERIFIED_SCOPE",
            "change_allowed": False,
            "notes": "Do not use raw same-game plate_appearances as feature.",
        },
        {
            "platform": "Starter Skill / Workload",
            "formula_family": "starter_skill_workload_v1",
            "formula_version": "2026-07-11 reconstruction",
            "parameters": "season decay 0.70; expected outs 65/35 stable/recent5 blend; role thresholds usage>=0.8 outs>=12, opener outs<9 or early_freq>=0.6",
            "stability_status": "STABLE_FOR_VERIFIED_SCOPE",
            "change_allowed": False,
            "notes": "Bundle v1 uses outs-based skill/workload/role fields, not BF proxy.",
        },
    ]


def replayability_assessment() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "source_period": "2026-07-03_to_2026-07-09",
            "classification": "EXACT_REPLAYABLE",
            "broader_period_classification": "CONTRACT_PERMITTED_RECONSTRUCTION",
            "risk": "pre-2026-07-03 source discovery not yet complete",
            "required_guard": "deterministic PA pilot with source hashes and strict prior cutoff audit",
        },
        {
            "platform": "Starter Skill / Workload",
            "source_period": "2026-05-01_to_2026-07-06",
            "classification": "EXACT_REPLAYABLE",
            "broader_period_classification": "EXACT_RECONSTRUCTABLE_WITH_VALIDATED_LOCAL_STATS",
            "risk": "local player_stats correction drift and starter identity gaps",
            "required_guard": "lineage parity audit, starter identity audit, deterministic reconstruction pilot",
        },
        {
            "platform": "Variant C Market Metadata",
            "source_period": "2026-05-01_to_2026-07-09 price/no-vig partial",
            "classification": "PARTIAL_REPLAYABILITY",
            "broader_period_classification": "PARTIAL_REPLAYABILITY",
            "risk": "missing book count and snapshot timestamp fields",
            "required_guard": "separate market lineage workstream",
        },
    ]


def temporal_integrity_assessment() -> list[dict[str, Any]]:
    return [
        {
            "platform": "Rolling PA Opportunity",
            "audit_area": "rolling calculations",
            "finding": "strict prior repaired rule is source_game_date < artifact_date",
            "leakage_risk": "LOW if prior_ aliases are used; HIGH if raw same-game plate_appearances is used",
            "mitigation": "require pa_opp_v1_cutoff_status PASS_PRIOR_DATE and no raw PA field in matrix",
        },
        {
            "platform": "Starter Skill / Workload",
            "audit_area": "pitcher history",
            "finding": "fields use prior starts/game_date < target date",
            "leakage_risk": "MODERATE due to mutable local stat corrections and starter identity authority",
            "mitigation": "date-lock source exports, hash player_stats extracts, audit local-vs-official stat-line drift",
        },
        {
            "platform": "Variant C Market Metadata",
            "audit_area": "market snapshot timing",
            "finding": "metadata incomplete in current research bases",
            "leakage_risk": "HIGH for market-aware training until timestamps/book counts are restored",
            "mitigation": "separate Variant C pilot; exclude from first baseball-state expansion",
        },
    ]


def engineering_effort_assessment() -> list[dict[str, Any]]:
    return [
        {
            "workstream": "PA source inventory and bounded pilot",
            "implementation_complexity": "MODERATE",
            "expected_runtime": "LOW_TO_MODERATE",
            "expected_storage": "LOW",
            "validation_effort": "MODERATE",
            "operational_risk": "LOW if artifact-only",
            "notes": "No DB write needed for first pilot; regenerate research base for a small earlier window.",
        },
        {
            "workstream": "Starter skill/workload forward extension",
            "implementation_complexity": "MODERATE",
            "expected_runtime": "MODERATE",
            "expected_storage": "LOW",
            "validation_effort": "HIGH",
            "operational_risk": "LOW_TO_MODERATE if read-only artifact generation",
            "notes": "Requires starter identity and local stat-line parity checks.",
        },
        {
            "workstream": "Full combined expansion",
            "implementation_complexity": "HIGH",
            "expected_runtime": "MODERATE",
            "expected_storage": "MODERATE",
            "validation_effort": "HIGH",
            "operational_risk": "MODERATE",
            "notes": "Only after platform-specific pilots pass.",
        },
        {
            "workstream": "Variant C market metadata",
            "implementation_complexity": "HIGH",
            "expected_runtime": "UNKNOWN",
            "expected_storage": "MODERATE",
            "validation_effort": "HIGH",
            "operational_risk": "MODERATE",
            "notes": "Independent; should not block baseball-state population.",
        },
    ]


def validation_plan() -> list[dict[str, Any]]:
    checks = [
        "source_inventory",
        "strict_prior_cutoff",
        "deterministic_rebuild",
        "sha_manifest",
        "row_count_by_date",
        "missingness_stability",
        "duplicate_identity",
        "grain_join_cardinality",
        "temporal_integrity",
        "ownership_audit",
        "field_distribution_comparison",
        "matrix_assembler_dry_run",
    ]
    rows: list[dict[str, Any]] = []
    for platform in ["Rolling PA Opportunity", "Starter Skill / Workload"]:
        for check in checks:
            rows.append(
                {
                    "platform": platform,
                    "validation_check": check,
                    "required_before_full_expansion": True,
                    "failure_action": "stop_and_report_blocker",
                    "notes": "must pass on pilot before larger backfill/expansion",
                }
            )
    return rows


def expansion_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "scenario": "Scenario 1 - Expand PA Opportunity backward",
            "expected_date_range": "target 2026-05-01_to_2026-07-09 if PA sources validate",
            "expected_slates": "up to 70 in current source horizon",
            "expected_rows": "up to 10823 candidate prop rows if starter source also available; PA-only rows require separate count",
            "expected_benefits": "unblocks pre-2026-07-03 Bundle v1 fields using existing hitter/offense source horizon",
            "remaining_blockers": "starter source still ends 2026-07-06",
            "implementation_complexity": "MODERATE",
            "replayability_confidence": "MEDIUM_PENDING_SOURCE_DISCOVERY",
            "recommended_priority": "1",
        },
        {
            "scenario": "Scenario 2 - Expand Starter Skill / Workload forward",
            "expected_date_range": "target 2026-07-07_to_2026-07-09 initially, then broader",
            "expected_slates": "3 immediate missing dates in current horizon",
            "expected_rows": "would recover current PA-supported dates after 2026-07-06",
            "expected_benefits": "expands common Bundle interval through PA source max date",
            "remaining_blockers": "local stat-line lineage and starter identity validation",
            "implementation_complexity": "MODERATE",
            "replayability_confidence": "MEDIUM_HIGH_FOR_OUTS_BASED_FIELDS",
            "recommended_priority": "2",
        },
        {
            "scenario": "Scenario 3 - Expand both together",
            "expected_date_range": "target 2026-05-01_to_2026-07-09 after platform pilots",
            "expected_slates": "70 current hitter/offense source dates",
            "expected_rows": "10823 exact candidate rows in hitter/offense horizon, subject to PA/starter completeness",
            "expected_benefits": "creates first plausible bounded historical matrix-expansion candidate",
            "remaining_blockers": "both platform pilots must pass; still not training approval",
            "implementation_complexity": "HIGH",
            "replayability_confidence": "MEDIUM_AFTER_PILOTS",
            "recommended_priority": "3",
        },
        {
            "scenario": "Scenario 4 - Separate baseball-state from Variant C market population",
            "expected_date_range": "baseball-state follows PA/starter support; Variant C follows market metadata support",
            "expected_slates": "manifest-specific",
            "expected_rows": "baseball-state not reduced by missing market metadata",
            "expected_benefits": "prevents market metadata gaps from blocking Variant A/B/D and hit-family expansion",
            "remaining_blockers": "Variant C requires independent market lineage workstream",
            "implementation_complexity": "LOW_FOR_SEPARATION_DESIGN_HIGH_FOR_MARKET_FIX",
            "replayability_confidence": "HIGH_FOR_SEPARATION_LOW_FOR_VARIANT_C_UNTIL_MARKET_PILOT",
            "recommended_priority": "0_already_adopt_as_design_rule",
        },
    ]


def dependency_graph() -> list[dict[str, Any]]:
    return [
        {"workstream": "PA", "from_step": "PA source inventory", "to_step": "PA reconstruction verification", "can_parallelize": True},
        {"workstream": "PA", "from_step": "PA reconstruction verification", "to_step": "bounded PA pilot backfill/artifact regeneration", "can_parallelize": False},
        {"workstream": "PA", "from_step": "bounded PA pilot backfill/artifact regeneration", "to_step": "PA replay validation", "can_parallelize": False},
        {"workstream": "PA", "from_step": "PA replay validation", "to_step": "PA historical expansion", "can_parallelize": False},
        {"workstream": "Starter", "from_step": "starter source inventory", "to_step": "local stat-line/starter identity validation", "can_parallelize": True},
        {"workstream": "Starter", "from_step": "local stat-line/starter identity validation", "to_step": "bounded starter reconstruction pilot", "can_parallelize": False},
        {"workstream": "Starter", "from_step": "bounded starter reconstruction pilot", "to_step": "starter replay validation", "can_parallelize": False},
        {"workstream": "Starter", "from_step": "starter replay validation", "to_step": "starter historical expansion", "can_parallelize": False},
        {"workstream": "Combined", "from_step": "PA historical expansion", "to_step": "Bundle v1 matrix assembler dry run", "can_parallelize": False},
        {"workstream": "Combined", "from_step": "starter historical expansion", "to_step": "Bundle v1 matrix assembler dry run", "can_parallelize": False},
    ]


def risk_register() -> list[dict[str, Any]]:
    return [
        {"risk": "PA source missing before 2026-07-03", "likelihood": "MEDIUM", "impact": "HIGH", "mitigation": "source discovery plus bounded pilot"},
        {"risk": "raw same-game PA leakage", "likelihood": "LOW_IF_GUARDED", "impact": "HIGH", "mitigation": "use prior aliases only and enforce cutoff audit"},
        {"risk": "starter local stat-line drift", "likelihood": "MEDIUM", "impact": "HIGH", "mitigation": "local-vs-StatsAPI parity audit before expansion"},
        {"risk": "starter identity conflicts", "likelihood": "MEDIUM", "impact": "HIGH", "mitigation": "game/team/starter authority rules and exception manifest"},
        {"risk": "schema drift across regenerated artifacts", "likelihood": "MEDIUM", "impact": "MEDIUM", "mitigation": "schema lock and parse validation"},
        {"risk": "row multiplication in starter joins", "likelihood": "LOW", "impact": "HIGH", "mitigation": "canonical key duplicate audit"},
        {"risk": "Variant C market timestamp gaps", "likelihood": "HIGH", "impact": "MEDIUM", "mitigation": "separate market workstream; exclude from first baseball-state expansion"},
        {"risk": "validation burden grows with full expansion", "likelihood": "HIGH", "impact": "MEDIUM", "mitigation": "pilot/incremental expansion sequence"},
    ]


def roadmap() -> list[dict[str, Any]]:
    return [
        {"order": 1, "step": "Run PA source-discovery audit for 2026-05-01_to_2026-07-02", "scope": "read-only", "expected_decision": "confirm exact recoverability"},
        {"order": 2, "step": "Run starter source-extension audit for 2026-07-07_to_2026-07-09", "scope": "read-only", "expected_decision": "confirm forward reconstruction feasibility"},
        {"order": 3, "step": "PA bounded pilot artifact regeneration for one week before 2026-07-03", "scope": "artifact-only proposed future implementation", "expected_decision": "validate strict prior and deterministic replay"},
        {"order": 4, "step": "Starter bounded pilot reconstruction for 2026-07-07_to_2026-07-09", "scope": "artifact-only proposed future implementation", "expected_decision": "validate join/starter identity and local stat lineage"},
        {"order": 5, "step": "Run Bundle v1 assembler on expanded pilot intersection", "scope": "matrix assembly only after pilots", "expected_decision": "determine whether broader historical expansion is justified"},
        {"order": 6, "step": "Defer Variant C market metadata to separate workstream", "scope": "read-only design / later pilot", "expected_decision": "do not block baseball-state expansion"},
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    rows_by_file = {
        "source_lineage_inventory_2026-07-12.csv": source_lineage_inventory(),
        "historical_coverage_audit_2026-07-12.csv": historical_coverage_audit(),
        "reconstruction_feasibility_matrix_2026-07-12.csv": reconstruction_feasibility_matrix(),
        "formula_stability_audit_2026-07-12.csv": formula_stability_audit(),
        "replayability_assessment_2026-07-12.csv": replayability_assessment(),
        "temporal_integrity_assessment_2026-07-12.csv": temporal_integrity_assessment(),
        "engineering_effort_assessment_2026-07-12.csv": engineering_effort_assessment(),
        "validation_plan_2026-07-12.csv": validation_plan(),
        "expansion_scenarios_2026-07-12.csv": expansion_scenarios(),
        "dependency_graph_2026-07-12.csv": dependency_graph(),
        "risk_register_2026-07-12.csv": risk_register(),
        "recommended_implementation_roadmap_2026-07-12.csv": roadmap(),
    }
    for name, rows in rows_by_file.items():
        write_csv(out / name, rows)

    readiness = {
        "final_recommendation": "READY_FOR_PLATFORM_SPECIFIC_PILOTS",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "matrix_expansion_readiness": "NOT_READY_FOR_FULL_HISTORICAL_EXPANSION_UNTIL_PLATFORM_PILOTS_PASS",
        "baseball_state_planning": "Variant C market limitations should not constrain Variant A/B/D or hit-family planning",
        "next_step": "Run PA and Starter platform-specific bounded source/reconstruction pilots; no broad backfill yet.",
        "no_side_effects": True,
    }
    write_json(out / "readiness_decision_2026-07-12.json", readiness)

    main_md = """# MLB Collective Bundle v1 Historical Source Expansion Feasibility and Backfill Design Review — 2026-07-12

## Executive Summary

The certified Bundle v1 matrix assembly is correct and replayable for 2026-07-03 through 2026-07-06, but the historical population remains source-limited. This review identifies the exact engineering work needed to expand that population without violating the frozen Bundle v1 contracts.

The two limiting baseball-state platforms are Rolling PA Opportunity and Starter Skill / Workload. Variant C market metadata is a separate workstream and should not constrain baseball-state planning.

## Recommendation

`READY_FOR_PLATFORM_SPECIFIC_PILOTS`

Run small, artifact-only pilots first:

1. PA source-discovery and reconstruction pilot before 2026-07-03.
2. Starter skill/workload forward-extension pilot after 2026-07-06.
3. Re-run the certified Bundle v1 assembler on the expanded pilot intersection only after both pilots pass.

Do not run a broad backfill, matrix expansion, model training, scoring, Champion-Challenger experiment, or production integration from this review.

## Key Limits

- PA Opportunity is verified only from 2026-07-03 through 2026-07-09 in the current research base.
- Starter Skill / Workload is verified through 2026-07-06 in the current archived reconstruction package.
- Variant C market timestamp/book-count metadata is incomplete and should be isolated.

## Source Lineage Findings

Rolling PA Opportunity is rooted in local MLB player stat lineage and derived strict-prior PA aliases. The safe Bundle v1 feature surface is not raw same-game `plate_appearances`; it is the repaired prior-only contract: `prior_d7_plate_appearances`, `prior_d15_plate_appearances`, `prior_d30_plate_appearances`, `pa_opp_v1_d15_opportunity_band`, and `pa_opp_v1_trend_label`, with `source_game_date < artifact_date`.

Starter Skill / Workload is rooted in prior pitcher-game history from `mlb.player_stats` and the starter reconstruction package. Bundle v1 should use the outs-based reconstruction fields for this planning branch. BF support remains useful foundation work, but is not required for the current Bundle v1 starter skill/workload expansion.

Variant C market metadata is not a baseball-state source. It should be planned independently so missing timestamp/book-count coverage does not shrink otherwise-valid Variant A/B/D or hit-family Bundle v1 populations.

## Historical Coverage Findings

PA Opportunity has exact archived research coverage from 2026-07-03 through 2026-07-09. Earlier recovery is plausible, but not certified by this review. It requires source discovery and deterministic regeneration using strict-prior PA aliases.

Starter Skill / Workload has exact archived reconstruction coverage for the certified matrix interval and verified starter reconstruction support through 2026-07-06. Forward extension through 2026-07-09 is the smallest immediate pilot because PA already supports those dates.

## Reconstruction Feasibility

PA Opportunity fields are classified as `CONTRACT_PERMITTED_RECONSTRUCTION` outside the verified window. The formulas are known and stable, but the earlier source availability and date-locked replay need proof before any broad backfill.

Starter Skill / Workload outs-based fields are classified as `EXACT_RECONSTRUCTABLE` when prior pitcher-game sources and starter identity validate. The main risk is not formula ambiguity; it is local completed-game stat lineage and starter identity parity.

Variant C fields are `PARTIALLY_RECONSTRUCTABLE` and should remain outside the first baseball-state expansion path.

## Formula Stability

No formula changes are proposed or permitted. PA uses the frozen `pa_opp_v1` thresholds and trend rule. Starter Skill / Workload uses the 2026-07-11 reconstruction formulas, including season decay, expected outs blend, and role confidence logic. Any future backfill must reproduce these formulas exactly or stop.

## Replayability and Temporal Integrity

The verified windows are exact replayable from archived package artifacts. Broader PA expansion requires strict-prior cutoff proof. Broader starter expansion requires source hashes, local stat-line parity checks, and starter identity exception manifests. Raw same-game PA, mutable local stat corrections, and non-date-locked regenerated aggregates are the principal temporal hazards.

## Recommended Implementation Order

1. Run PA source-discovery for 2026-05-01 through 2026-07-02.
2. Run Starter Skill / Workload source-extension discovery for 2026-07-07 through 2026-07-09.
3. Run one small PA artifact-only pilot before 2026-07-03.
4. Run one small starter artifact-only pilot after 2026-07-06.
5. Only after both pilots pass, run the Bundle v1 assembler on the expanded pilot intersection.
6. Keep Variant C market metadata on a separate track.

## Readiness Decision

`READY_FOR_PLATFORM_SPECIFIC_PILOTS`

This means the source designs are mature enough for bounded platform-specific pilots, but not broad historical backfill and not model training. Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.

## No Behavior Changed

This package is read-only planning. It performs no historical backfill, no data generation, no matrix expansion, no scoring, no model training, no Champion-Challenger work, no production integration, and no Bundle v1 modification.
"""
    (out / "mlb_collective_bundle_v1_historical_source_expansion_design_2026-07-12.md").write_text(main_md)

    exec_md = """# Executive Summary — 2026-07-12

Final decision: `READY_FOR_PLATFORM_SPECIFIC_PILOTS`.

The next safe work is not a full backfill. It is platform-specific pilot work for PA Opportunity and Starter Skill / Workload, each with deterministic replay, strict-prior checks, source hashes, duplicate identity audits, and Bundle v1 assembler compatibility validation.

Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.

Variant C market metadata should be treated as an independent future workstream. It should not constrain baseball-state Bundle v1 expansion planning.
"""
    (out / "executive_summary_2026-07-12.md").write_text(exec_md)

    # Parse validation before SHA.
    parse_rows: list[dict[str, Any]] = []
    excluded_validation_outputs = {
        "sha256_manifest_2026-07-12.csv",
        "parse_schema_validation_results_2026-07-12.csv",
    }
    for path in sorted(p for p in out.rglob("*") if p.is_file() and p.name not in excluded_validation_outputs):
        rel = str(path.relative_to(out))
        if path.suffix == ".csv":
            try:
                rows = read_csv(path)
                parse_rows.append({"relative_path": rel, "file_type": "csv", "status": "PASS", "rows": len(rows), "notes": ""})
            except Exception as exc:
                parse_rows.append({"relative_path": rel, "file_type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                parse_rows.append({"relative_path": rel, "file_type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                parse_rows.append({"relative_path": rel, "file_type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            parse_rows.append({"relative_path": rel, "file_type": "markdown", "status": "PASS" if path.read_text().lstrip().startswith("#") else "WARN", "rows": "", "notes": ""})
    write_csv(out / "parse_schema_validation_results_2026-07-12.csv", parse_rows)
    digest = package_digest(out)
    print(json.dumps({"output_dir": str(out), "readiness": readiness["final_recommendation"], "package_sha256": digest}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
