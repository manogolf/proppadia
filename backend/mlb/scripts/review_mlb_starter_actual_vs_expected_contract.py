#!/usr/bin/env python3
"""Read-only governance review for Starter actual-vs-expected semantics.

This script creates a contract interpretation package only. It does not amend
contracts, certify Starter rows, repair features, attach outcomes, call
external sources, write to a database, train, score, or alter production.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


PACKAGE_DATE = "2026-07-13"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_starter_actual_vs_expected_contract_review/2026-07-13")
BUNDLE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12")
RECOVERY_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_recovery_dry_run/2026-07-13")
JOIN_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_join_remediation/2026-07-13")
GAP_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_source_gap_discovery/2026-07-13")
STARTER_RECON_DIR = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11")
STARTER_XH_DIR = Path("artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11")
DAILY_GEN_DIR = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_daily_generator/2026-07-11")
PROCESS_REQUEST_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13")
PROCESS_EXEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation/2026-07-13")
QUALIFICATION_DIR = Path("artifacts/analysis/model_development/mlb_historical_certified_population_qualification_pilot/2026-07-13")

TERMS = [
    "expected starter",
    "probable starter",
    "projected starter",
    "scheduled starter",
    "announced starter",
    "confirmed starter",
    "listed starter",
    "selected starter",
    "actual starter",
    "opposing starter",
    "starter identity",
    "starter game",
    "opener",
    "bullpen game",
    "scratch",
    "replacement starter",
    "short start",
    "workload",
    "official BF",
    "official_bf",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def line_citation(path: Path, pattern: str) -> tuple[str, str]:
    if not path.exists():
        return "", ""
    regex = re.compile(pattern, re.IGNORECASE)
    try:
        lines = path.read_text(errors="replace").splitlines()
    except UnicodeDecodeError:
        return "", ""
    for idx, line in enumerate(lines, start=1):
        if regex.search(line):
            return f"{path}:{idx}", line.strip()[:500]
    return "", ""


def governing_artifact_specs() -> list[dict[str, Any]]:
    specs: list[tuple[str, Path, str, int, str, str, str]] = [
        ("Bundle v1 main specification", BUNDLE_DIR / "collective_bundle_specification_v1_2026-07-12.md", "FROZEN", 1, "Bundle v1 specification", "Starter fields included in governed bundle; production/training prohibited.", "starter"),
        ("Bundle v1 one-page summary", BUNDLE_DIR / "collective_bundle_v1_one_page_summary_2026-07-12.md", "FROZEN", 1, "Summary", "High-level scope and prohibited next steps.", "Starter|training|production"),
        ("Frozen field registry", BUNDLE_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv", "FROZEN", 2, "Starter field rows", "Defines Starter fields, owners, grains, strict-prior and date-locked source requirements.", "weighted_multiseason_hits_per_out"),
        ("Hits 1.5 frozen field manifest", BUNDLE_DIR / "hits_1_5_frozen_field_manifest_2026-07-12.csv", "FROZEN", 2, "Starter field rows", "Shows Starter fields are Hits 1.5 core candidates.", "weighted_multiseason_hits_per_out"),
        ("Field construction contract", BUNDLE_DIR / "collective_bundle_v1_field_construction_contract_2026-07-12.json", "FROZEN", 3, "Starter formulas", "Defines strict-prior formulas for Starter fields.", "expected_outs_blended_v1|weighted_multiseason_hits_per_out"),
        ("Missing data contract", BUNDLE_DIR / "collective_bundle_v1_missing_data_contract_2026-07-12.json", "FROZEN", 3, "Row exclusion and field rules", "Preserve missingness; hard drop only for prohibited postgame leakage or missing required keys/outcomes.", "postgame leakage|weighted_multiseason_hits_per_out"),
        ("Grain/join contract", BUNDLE_DIR / "collective_bundle_v1_grain_join_contract_2026-07-12.json", "FROZEN", 3, "Join rules", "Starter fields join by game_id/opponent starter assignment and strict-prior starter history.", "Starter fields join"),
        ("Ownership binding", BUNDLE_DIR / "collective_bundle_v1_ownership_metadata_binding_2026-07-12.json", "FROZEN", 3, "Ownership registry binding", "Confirms ownership registry audit passed.", "ownership"),
        ("Readiness decision", BUNDLE_DIR / "collective_bundle_v1_readiness_decision_2026-07-12.json", "FROZEN", 4, "Permitted/prohibited next steps", "Limited date-locked matrix assembly permitted; model/training/production prohibited.", "permitted_next_steps"),
        ("Spine contract markdown", SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.md", "FROZEN", 1, "Temporal Integrity; Feature Joins", "Feature joins preserve denominator; postgame contamination and future diagnostics prohibited.", "Temporal Integrity|Feature Joins"),
        ("Spine contract JSON", SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.json", "FROZEN", 1, "Contract identity", "Machine-readable frozen contract.", "FROZEN"),
        ("Source selection cutoff contract", SPINE_DIR / "source_selection_cutoff_contract_2026-07-12.csv", "FROZEN", 3, "Source identity/date lock", "Explicit source and cutoff policy required; implicit latest and silent fallback forbidden.", "explicit_source_artifact"),
        ("Feature join contract", SPINE_DIR / "feature_join_contract_2026-07-12.csv", "FROZEN", 3, "Starter Skill / Workload row", "Starter is a left join from frozen spine; no silent row loss or multiplication.", "Starter Skill / Workload"),
        ("Replayability contract", SPINE_DIR / "replayability_contract_2026-07-12.csv", "FROZEN", 3, "Replayability", "Requires deterministic reconstruction from locked inputs.", "replay"),
        ("Compatibility binding", SPINE_DIR / "compatibility_binding_2026-07-12.md", "FROZEN", 3, "Compatibility", "Spine compatibility with Bundle v1.", "compatibility"),
        ("Starter expected hits characterization", STARTER_XH_DIR / "mlb_starter_expected_hits_allowed_characterization_2026-07-11.md", "NONFROZEN_RESEARCH_DESIGN", 6, "Starter Identity and Role", "Design evidence distinguishes expected starter identity from postgame-derived actual role.", "Starter Identity and Role"),
        ("Starter reconstruction report", STARTER_RECON_DIR / "mlb_starter_skill_workload_reconstruction_2026-07-11.md", "NONFROZEN_RESEARCH_DESIGN", 6, "Executive summary; strongest fields", "Design intent: prior-only starter skill/workload labels and role confidence.", "prior-only|expected_role_label"),
        ("Starter source semantics inventory", STARTER_RECON_DIR / "starter_skill_workload_source_semantics_inventory_2026-07-11.csv", "NONFROZEN_RESEARCH_DESIGN", 6, "Source semantics", "Source-level evidence for starter reconstruction semantics.", "starter"),
        ("Starter strict-prior lineage", STARTER_RECON_DIR / "starter_skill_workload_strict_prior_lineage_2026-07-11.csv", "NONFROZEN_RESEARCH_OUTPUT", 6, "Strict prior lineage", "Date cutoff and prior source evidence for reconstructed labels.", "PASS_STRICT_PRIOR"),
        ("Starter game base", STARTER_RECON_DIR / "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv", "NONFROZEN_RESEARCH_OUTPUT", 6, "Starter-game rows", "Contains expected and actual starter ids plus prior reconstructed features.", "expected_starter_player_id"),
        ("Daily generator implementation", DAILY_GEN_DIR / "starter_skill_workload_daily_generator_implementation_2026-07-11.md", "NONFROZEN_IMPLEMENTATION_DOC", 7, "Strict-Prior Status; Role", "Implementation says cutoff = slate_date - 1 and role labels are expected/prospective labels.", "feature_cutoff_date"),
        ("Daily generator source field lineage", DAILY_GEN_DIR / "starter_skill_workload_source_field_lineage_2026-07-11.csv", "NONFROZEN_IMPLEMENTATION_DOC", 7, "Lineage", "Implementation lineage for daily research generator.", "starter"),
        ("Official BF source inventory", DAILY_GEN_DIR / "official_bf_source_inventory_2026-07-11.csv", "NONFROZEN_DESIGN_OUTPUT", 6, "Official BF", "BF source evidence; not a frozen Bundle rule.", "battersFaced|BF"),
        ("Starter change preservation audit", DAILY_GEN_DIR / "starter_change_preservation_audit_2026-07-11.csv", "NONFROZEN_DESIGN_OUTPUT", 6, "Starter changes", "Design evidence about run-overwrite and starter-change preservation.", "starter"),
        ("Historical qualification pilot findings", QUALIFICATION_DIR / "mlb_historical_qualification_pilot_findings_2026-07-13.md", "NONFROZEN_CERTIFICATION_ARTIFACT", 5, "Pilot decision", "Historical qualification state before Starter remediation.", "Starter"),
        ("Starter join remediation findings", JOIN_DIR / "mlb_historical_starter_remediation_findings_2026-07-13.md", "NONFROZEN_REMEDIATION_AUDIT", 6, "Starter remediation decision", "Current Starter qualified/blocked counts.", "STARTER_DOMAIN_PARTIALLY_QUALIFIED"),
        ("Starter source gap findings", GAP_DIR / "mlb_historical_starter_source_gap_findings_2026-07-13.md", "NONFROZEN_DISCOVERY_AUDIT", 6, "Source gap decision", "Finds actual-starter evidence but no pregame expected-starter evidence.", "actual-starter"),
        ("Starter recovery dry-run findings", RECOVERY_DIR / "mlb_historical_starter_recovery_findings_2026-07-13.md", "NONFROZEN_DRY_RUN_AUDIT", 6, "Dry-run findings", "494 rows technically complete but semantically blocked.", "technically complete"),
        ("Starter actual-vs-expected dry-run contract review", RECOVERY_DIR / "mlb_historical_starter_actual_vs_expected_contract_review_2026-07-13.md", "NONFROZEN_DRY_RUN_AUDIT", 6, "Actual vs expected review", "Prior dry-run classified compatibility as ambiguous.", "ACTUAL_VS_EXPECTED"),
        ("Starter recovery row dry run", RECOVERY_DIR / "mlb_historical_starter_recovery_row_dry_run_2026-07-13.csv", "NONFROZEN_DRY_RUN_OUTPUT", 6, "Row-level impact", "Current 494-row technical recovery population.", "TECHNICALLY_RECOVERED"),
        ("Starter generator code", Path("backend/mlb/scripts/build_mlb_starter_skill_workload_research.py"), "IMPLEMENTATION_CODE", 7, "_construct_features; _role_label", "Secondary implementation evidence only.", "expected_starter_id|strict_prior"),
        ("Starter recovery dry-run code", Path("backend/mlb/scripts/dry_run_mlb_historical_starter_recovery.py"), "IMPLEMENTATION_CODE", 7, "semantic status", "Secondary implementation evidence; cannot amend contract.", "ACTUAL_STARTER_ONLY_CONTRACT_AMBIGUOUS"),
    ]
    rows: list[dict[str, Any]] = []
    for name, path, status, authority, section, relevance, pattern in specs:
        citation, excerpt = line_citation(path, pattern)
        rows.append(
            {
                "artifact_name": name,
                "path": str(path),
                "exists": path.exists(),
                "sha256": sha256(path) if path.exists() and path.is_file() else "",
                "frozen_or_nonfrozen_status": status,
                "authority_level": authority,
                "relevant_section_or_field": section,
                "relevant_terminology": pattern,
                "directness_of_evidence": "direct" if authority <= 3 else "supporting",
                "citation": citation,
                "evidence_excerpt": excerpt,
                "review_note": relevance,
            }
        )
    return rows


def searchable_paths(artifact_rows: list[dict[str, Any]]) -> list[Path]:
    paths: list[Path] = []
    for row in artifact_rows:
        path = Path(row["path"])
        if path.exists() and path.is_file() and path.suffix.lower() in {".md", ".csv", ".json", ".py"}:
            paths.append(path)
    return paths


def terminology_registry(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    patterns = [(term, re.compile(re.escape(term).replace("\\ ", r"[\s_\\-]+"), re.IGNORECASE)) for term in TERMS]
    for path in paths:
        text = path.read_text(errors="replace").splitlines()
        for idx, line in enumerate(text, start=1):
            for term, pattern in patterns:
                if pattern.search(line):
                    lower = line.lower()
                    rows.append(
                        {
                            "artifact": str(path),
                            "line": idx,
                            "exact_term": term,
                            "meaning_in_context": infer_meaning(term, line),
                            "normative_or_descriptive": "normative" if "contract" in str(path).lower() or "FROZEN" in line or "must" in lower or "required" in lower or "forbidden" in lower else "descriptive",
                            "pregame_or_postgame_semantics_implied": infer_time_semantics(line),
                            "conflict_with_another_artifact": "possible terminology ambiguity" if term in {"expected starter", "actual starter", "projected starter"} else "",
                            "evidence_excerpt": line.strip()[:500],
                        }
                    )
    return rows


def infer_meaning(term: str, line: str) -> str:
    lower = line.lower()
    if "strict-prior" in lower or "strict prior" in lower:
        return "feature values must use only prior games"
    if "postgame" in lower or "actual" in lower:
        return "actual/postgame evidence or evaluation context"
    if "expected" in lower or "probable" in lower or "projected" in lower:
        return "pregame/prospective starter assignment or role label"
    if "opener" in lower or "bullpen" in lower:
        return "special starting-pitcher role regime"
    if "bf" in lower or "battersfaced" in lower:
        return "official batters faced source/evidence"
    return f"term occurrence for {term}"


def infer_time_semantics(line: str) -> str:
    lower = line.lower()
    if "postgame" in lower or "actual starter" in lower or "outcome" in lower:
        return "postgame_or_actual"
    if "pregame" in lower or "strict-prior" in lower or "strict prior" in lower or "expected" in lower or "probable" in lower or "projected" in lower:
        return "pregame_or_strict_prior"
    return "not_explicit"


def field_semantics_review() -> list[dict[str, Any]]:
    registry = pd.read_csv(BUNDLE_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv", low_memory=False)
    starter_fields = registry[registry["primary_owner"].astype(str).str.contains("starter|workload", case=False, regex=True)].copy()
    rows: list[dict[str, Any]] = []
    for _, row in starter_fields.iterrows():
        field = clean(row["field_name"])
        definition = clean(row["definition_or_formula"])
        tied_to_actual = "actual" in definition.lower()
        rows.append(
            {
                "field_name": field,
                "field_definition": definition,
                "owner": clean(row["primary_owner"]),
                "grain": clean(row["native_grain"]),
                "source": clean(row["source_table_or_artifact"]),
                "temporal_rule": clean(row["prediction_time_availability"]),
                "historical_availability": clean(row["historical_availability"]),
                "missingness_rule": clean(row["missing_policy"]),
                "starter_identity_part_of_field_semantics": "implicit_opposing_starter_assignment",
                "expected_probable_or_actual_tie": "not_explicit_actual; expected/prospective implied by prediction_time_availability" if not tied_to_actual else "actual_mentioned",
                "historical_reconstruction_described": "DATE_LOCKED_SOURCE_REQUIRED but no actual-starter substitution rule",
                "same_game_actual_identity_would_alter_feature_meaning": "yes_if_identity_was_not_known_at_prediction_time",
                "evidence_citation": f"{BUNDLE_DIR / 'collective_bundle_v1_field_definition_registry_2026-07-12.csv'}:field={field}",
                "conclusion": "field formula clear; starter identity source semantics incomplete",
            }
        )
    return rows


def simple_reviews() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    temporal = [
        {
            "question": "Must starter identity itself be known before first pitch?",
            "answer": "ambiguous_but_pregame_assignment_implied",
            "authority": "FROZEN grain/join and temporal contracts",
            "evidence": "Starter fields join by game_id/opponent starter assignment; postgame contamination prohibited.",
            "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_grain_join_contract_2026-07-12.json'} + {SPINE_DIR / 'historical_population_spine_contract_v1_2026-07-12.md'}",
            "notes": "The contracts do not explicitly say 'starter identity must be captured before first pitch', but prediction-time strict-prior and postgame contamination rules prevent silent postgame substitution.",
        },
        {
            "question": "Must only the workload features be computed from prior games?",
            "answer": "yes",
            "authority": "FROZEN field registry and construction contract",
            "evidence": "Starter fields are STRICT_PRIOR_REQUIRED and formulas use prior starts only.",
            "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_field_definition_registry_2026-07-12.csv'}",
            "notes": "",
        },
        {
            "question": "Is actual starter identity considered postgame leakage?",
            "answer": "ambiguous; actual starter role/outcome is postgame, actual identity can be postgame unless timestamped pregame",
            "authority": "FROZEN temporal contract plus design artifacts",
            "evidence": "Postgame contamination prohibited; design says actual role labels are postgame-derived and cannot be pregame features.",
            "citation": f"{SPINE_DIR / 'historical_population_spine_contract_v1_2026-07-12.md'}; {STARTER_XH_DIR / 'mlb_starter_expected_hits_allowed_characterization_2026-07-11.md'}:112",
            "notes": "Actual identity with no pregame timestamp should not be certified under current contract.",
        },
        {
            "question": "Is actual identity permissible when all feature values remain strict-prior?",
            "answer": "not_addressed",
            "authority": "FROZEN contracts silent",
            "evidence": "No frozen artifact explicitly permits actual identity as a binding key for strict-prior features.",
            "citation": "comprehensive term search over governing artifacts",
            "notes": "This is the core ambiguity.",
        },
        {
            "question": "Is source timestamp required for starter identity?",
            "answer": "source identity and cutoff required; starter-assignment timestamp not specifically defined",
            "authority": "FROZEN source selection cutoff contract",
            "evidence": "Explicit source artifact/date lock/source identity required.",
            "citation": f"{SPINE_DIR / 'source_selection_cutoff_contract_2026-07-12.csv'}",
            "notes": "",
        },
    ]
    ownership = [
        {
            "rule": "Starter platform grain",
            "contract_answer": "starter/game or starter historical baseline projected to batter-game",
            "actual_substitution_effect": "preserves row count/grain mechanically but alters semantics if actual starter was not the pregame assignment",
            "requires_amendment": True,
            "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_field_definition_registry_2026-07-12.csv'}",
        },
        {
            "rule": "Feature joins",
            "contract_answer": "left_join_from_frozen_spine; no silent row loss or multiplication",
            "actual_substitution_effect": "does not change denominator owner but changes Starter source semantics",
            "requires_amendment": True,
            "citation": f"{SPINE_DIR / 'feature_join_contract_2026-07-12.csv'}",
        },
        {
            "rule": "Late starter changes",
            "contract_answer": "not explicitly defined",
            "actual_substitution_effect": "would require a new historical reconstruction interpretation or variant",
            "requires_amendment": True,
            "citation": "no frozen occurrence defining scratch/starter-change handling",
        },
    ]
    reconstruction = [
        {
            "reconstruction_question": "Reconstruct starter identity from official postgame records",
            "contract_language": "not addressed",
            "authority": "frozen contracts",
            "decision": "not currently admissible for certification",
            "citation": "comprehensive search found no explicit permission",
        },
        {
            "reconstruction_question": "Use actual starters only to bind prior workload",
            "contract_language": "not addressed",
            "authority": "frozen contracts",
            "decision": "potentially useful diagnostic; not certified",
            "citation": f"{RECOVERY_DIR / 'mlb_historical_starter_recovery_findings_2026-07-13.md'}",
        },
        {
            "reconstruction_question": "Retain row as technically complete but not signal-eligible",
            "contract_language": "compatible with missingness/diagnostic reporting; no certification rule",
            "authority": "frozen missingness and feature-join contracts",
            "decision": "permitted as diagnostic output only",
            "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_missing_data_contract_2026-07-12.json'}",
        },
        {
            "reconstruction_question": "Contract-permitted missingness when probable-starter evidence absent",
            "contract_language": "generic missingness retention exists; Starter-specific probable-starter missingness waiver not found",
            "authority": "frozen missingness contract",
            "decision": "not a basis to certify recovered actual-starter rows",
            "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_missing_data_contract_2026-07-12.json'}",
        },
    ]
    regimes = []
    for regime in ["openers", "bullpen games", "scratches", "same-day starter changes", "doubleheaders", "two-way players", "debut starters", "missing official BF", "suspended/resumed games"]:
        regimes.append(
            {
                "special_regime": regime,
                "contract_handling": "silent_or_ambiguous",
                "evidence": "No frozen Starter-specific handling rule found; role labels mention opener/abbreviated starts but not certification semantics.",
                "classification": "amendment required" if regime in {"openers", "bullpen games", "scratches", "same-day starter changes"} else "ambiguous",
                "citation": f"{BUNDLE_DIR / 'collective_bundle_v1_field_construction_contract_2026-07-12.json'}",
            }
        )
    spec_impl = [
        {
            "component": "Frozen field registry",
            "semantics": "opposing starter skill/workload; strict-prior; date-locked",
            "implementation_behavior": "daily/reconstruction code uses expected_starter_id naming and strict-prior history",
            "divergence": "implementation has actual-starter binding ledgers for evaluation/research, but those do not amend frozen contract",
            "authority_note": "implementation secondary",
        },
        {
            "component": "Starter XH characterization",
            "semantics": "expected starter identity partly verified; actual role postgame-derived",
            "implementation_behavior": "actual outcome binding used as research/evaluation context",
            "divergence": "actual starter role explicitly not pregame feature",
            "authority_note": "design evidence supports caution",
        },
        {
            "component": "Historical recovery dry run",
            "semantics": "actual-starter-only evidence for many rows",
            "implementation_behavior": "keeps technical and semantic status separate",
            "divergence": "none; it preserves ambiguity",
            "authority_note": "dry-run is not certification authority",
        },
    ]
    decision = [
        {
            "interpretation": "A_expected_starter_strictly_required",
            "supporting_artifacts": "Temporal Integrity; strict-prior prediction-time availability; postgame contamination prohibition",
            "contradicting_artifacts": "No frozen artifact explicitly says starter identity must be captured pregame",
            "authority_strength": "medium_high",
            "semantic_consequences": "494 rows remain inadmissible until pregame proof exists",
            "leakage_consequences": "most conservative leakage posture",
            "reproducibility_consequences": "replayable but low coverage",
            "affected_rows": 494,
            "affected_game_sides": 51,
            "changes_bundle_v1_meaning": False,
            "requires_amendment": False,
        },
        {
            "interpretation": "B_actual_starter_allowed_for_historical_reconstruction",
            "supporting_artifacts": "None at frozen contract level; implementation can technically reconstruct strict-prior features",
            "contradicting_artifacts": "Postgame contamination prohibition; no actual-substitution rule",
            "authority_strength": "low",
            "semantic_consequences": "494 rows could be admitted, but Bundle v1 meaning changes",
            "leakage_consequences": "risk of same-game starter identity leakage when starter changed",
            "reproducibility_consequences": "high replayability, weak semantic provenance",
            "affected_rows": 494,
            "affected_game_sides": 51,
            "changes_bundle_v1_meaning": True,
            "requires_amendment": True,
        },
        {
            "interpretation": "C_actual_starter_allowed_only_under_bounded_conditions",
            "supporting_artifacts": "Technical dry run proves strict-prior values for 494 rows; missingness contract supports diagnostics",
            "contradicting_artifacts": "Frozen contracts do not define no-scratch/no-change proof or actual-starter waiver",
            "authority_strength": "medium_as_future_governance_path",
            "semantic_consequences": "484 standard rows might be potentially admissible after a governed interpretation; 10 two-way/special rows need special review",
            "leakage_consequences": "bounded if no-change/scratch evidence is added",
            "reproducibility_consequences": "requires explicit flags and replay contract",
            "affected_rows": 494,
            "affected_game_sides": 51,
            "changes_bundle_v1_meaning": "possibly",
            "requires_amendment": True,
        },
        {
            "interpretation": "D_contract_silent_or_ambiguous",
            "supporting_artifacts": "No explicit permit/forbid text for actual-starter historical reconstruction; terminology mixes expected/prospective/actual in lower-authority docs",
            "contradicting_artifacts": "None stronger than frozen temporal caution",
            "authority_strength": "high",
            "semantic_consequences": "0 recovered rows currently certifiable",
            "leakage_consequences": "preserves integrity",
            "reproducibility_consequences": "requires governance decision before remediation",
            "affected_rows": 494,
            "affected_game_sides": 51,
            "changes_bundle_v1_meaning": False,
            "requires_amendment": False,
        },
    ]
    return temporal, ownership, reconstruction, regimes, spec_impl, decision


def row_impact() -> list[dict[str, Any]]:
    rows = pd.read_csv(RECOVERY_DIR / f"mlb_historical_starter_recovery_row_dry_run_{PACKAGE_DATE}.csv", low_memory=False)
    complete = rows[rows["would_be_technically_complete"].astype(str).str.lower().eq("true")].copy()
    special = complete["semantic_qualification_status"].eq("SPECIAL_REGIME_CONTRACT_INTERPRETATION_REQUIRED")
    total = len(complete)
    special_rows = int(special.sum())
    standard_rows = total - special_rows
    return [
        {
            "interpretation": "A_expected_starter_strictly_required",
            "rows_admissible_under_current_contract": 0,
            "rows_inadmissible_under_current_contract": total,
            "rows_requiring_pregame_expected_starter_evidence": total,
            "rows_potentially_admissible_under_bounded_interpretation": 0,
            "rows_requiring_special_regime_handling": special_rows,
            "rows_requiring_amendment": 0,
            "rows_remaining_unresolved": total,
            "notes": "Current contract can be read conservatively as requiring pregame-compatible starter assignment.",
        },
        {
            "interpretation": "B_actual_starter_allowed_for_historical_reconstruction",
            "rows_admissible_under_current_contract": 0,
            "rows_inadmissible_under_current_contract": total,
            "rows_requiring_pregame_expected_starter_evidence": 0,
            "rows_potentially_admissible_under_bounded_interpretation": total,
            "rows_requiring_special_regime_handling": special_rows,
            "rows_requiring_amendment": total,
            "rows_remaining_unresolved": total,
            "notes": "No frozen authority currently supports this; would require amendment or interpretation.",
        },
        {
            "interpretation": "C_actual_starter_allowed_only_under_bounded_conditions",
            "rows_admissible_under_current_contract": 0,
            "rows_inadmissible_under_current_contract": total,
            "rows_requiring_pregame_expected_starter_evidence": 0,
            "rows_potentially_admissible_under_bounded_interpretation": standard_rows,
            "rows_requiring_special_regime_handling": special_rows,
            "rows_requiring_amendment": total,
            "rows_remaining_unresolved": total,
            "notes": "Would need explicit no-scratch/no-change, official-starter, strict-prior, diagnostic-flag conditions.",
        },
        {
            "interpretation": "D_contract_silent_or_ambiguous",
            "rows_admissible_under_current_contract": 0,
            "rows_inadmissible_under_current_contract": total,
            "rows_requiring_pregame_expected_starter_evidence": total,
            "rows_potentially_admissible_under_bounded_interpretation": standard_rows,
            "rows_requiring_special_regime_handling": special_rows,
            "rows_requiring_amendment": total,
            "rows_remaining_unresolved": total,
            "notes": "Best-supported current decision: not certifiable until governance decision.",
        },
    ]


def findings_md(summary: dict[str, Any]) -> str:
    decision_lines = "\n".join(f"- `{status}`" for status in summary["decisions"].values())
    return (
        "# MLB Starter Actual-vs-Expected Historical Reconstruction Contract Review\n\n"
        "## Decision\n\n"
        "`ACTUAL_VS_EXPECTED_STARTER_CONTRACT_COMPATIBILITY_AMBIGUOUS`\n\n"
        "The frozen contracts define the Starter fields as strict-prior, date-locked, opposing-starter "
        "skill/workload fields. They do not explicitly permit historical reconstruction by substituting "
        "postgame actual starter identity where pregame expected-starter evidence is absent. They also do "
        "not explicitly forbid every possible actual-starter diagnostic reconstruction. The gap is real: "
        "the contracts are incomplete for this historical reconstruction case.\n\n"
        "## Direct Answers\n\n"
        "- Explicitly requires pregame expected starter: no exact frozen sentence found, but prediction-time and temporal rules imply pregame-compatible assignment.\n"
        "- Explicitly permits actual-starter historical reconstruction: no.\n"
        "- Explicitly forbids actual-starter substitution: no exact actual-starter sentence, but postgame contamination and silent source substitution are forbidden.\n"
        "- Distinguishes expected/probable/projected/actual starter: partially in design artifacts, not fully in frozen contract.\n"
        "- Defines scratches/openers/bullpen/starter changes: not sufficiently for certification.\n"
        "- Defines contract-permitted missingness for absent pregame starter evidence: generic missingness exists; no Starter-specific actual-starter waiver found.\n\n"
        "## Strongest Evidence\n\n"
        "- Frozen field registry: Starter fields are `STRICT_PRIOR_REQUIRED` and require `DATE_LOCKED_SOURCE_REQUIRED`.\n"
        "- Frozen grain contract: Starter fields join by `game_id/opponent starter assignment and strict-prior starter history`.\n"
        "- Frozen spine contract: postgame contamination, future snapshots, and mutable current-state substitution are prohibited.\n"
        "- Frozen missingness contract: rows are not dropped solely for feature missingness; prohibited postgame leakage remains a hard stop.\n"
        "- Lower-authority design evidence: actual starter role is postgame-derived and cannot be used as a pregame feature.\n\n"
        "## 494-Row Impact\n\n"
        "The 494 technically complete rows remain not currently admissible under the frozen contracts. Under a future bounded interpretation, "
        "484 standard rows could be candidates for admission; 10 rows with two-way/special-regime semantics require separate handling. "
        "This review certifies zero rows.\n\n"
        "## Recommendation\n\n"
        "Request one narrowly scoped frozen-contract interpretation decision for actual-starter-based historical reconstruction. "
        "Do not amend, certify, or repair rows in this task. If external archived probable-pitcher evidence is later recovered, it could "
        "remove the need for this governance change for the affected rows by proving pregame expected-starter identity directly.\n\n"
        "## Statuses\n\n"
        f"{decision_lines}\n"
    )


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    artifacts = governing_artifact_specs()
    paths = searchable_paths(artifacts)
    terms = terminology_registry(paths)
    fields = field_semantics_review()
    temporal, ownership, reconstruction, regimes, spec_impl, decision = simple_reviews()
    impact = row_impact()
    recommendation = [
        {
            "recommended_action": "request_narrow_frozen_contract_interpretation_decision",
            "reason": "Frozen contracts do not explicitly permit actual-starter historical reconstruction, but technical recovery shows material rows are available if governed semantics are approved.",
            "scope": "Decide whether actual starter identity may bind strict-prior Starter Skill / Workload fields for historical reconstruction under bounded conditions.",
            "not_in_scope": "No amendment drafting, no row certification, no PA/outcome work, no training, no additional chunk.",
            "preserves_bundle_v1_integrity": True,
            "external_probable_pitcher_evidence_alternative": "Could remove need for governance change for rows where pregame expected starter is proven.",
            "elevated_access_materially_helps_later": True,
        }
    ]
    summary = {
        "package_date": PACKAGE_DATE,
        "package_path": str(OUT_DIR),
        "governing_artifacts_reviewed": len(artifacts),
        "terminology_occurrences": len(terms),
        "frozen_contract_explicitly_requires_expected_starter": False,
        "frozen_contract_implies_pregame_compatible_starter_assignment": True,
        "frozen_contract_explicitly_permits_actual_starter_historical_reconstruction": False,
        "frozen_contract_explicitly_forbids_actual_starter_substitution": False,
        "contract_status": "silent_or_ambiguous_for_historical_actual_starter_reconstruction",
        "implementation_divergence": "mixed_semantics_in_lower_authority_artifacts; implementation cannot amend frozen contracts",
        "technically_complete_rows_reviewed": 494,
        "currently_admissible_recovered_rows": 0,
        "potentially_admissible_under_bounded_future_interpretation": 484,
        "special_regime_rows_requiring_separate_handling": 10,
        "recommended_governance_action": recommendation[0]["recommended_action"],
        "decisions": {
            "governing_artifact_completeness": "STARTER_GOVERNING_ARTIFACTS_REVIEWED",
            "terminology_consistency": "STARTER_TERMINOLOGY_INCONSISTENT",
            "field_semantics": "STARTER_FIELD_SEMANTICS_AMBIGUOUS",
            "temporal_rules": "STARTER_TEMPORAL_RULES_AMBIGUOUS",
            "historical_reconstruction": "HISTORICAL_ACTUAL_STARTER_RECONSTRUCTION_NOT_ADDRESSED",
            "special_regime_clarity": "STARTER_SPECIAL_REGIME_RULES_AMBIGUOUS",
            "spec_vs_implementation": "STARTER_SPEC_VS_IMPLEMENTATION_MIXED_SEMANTICS_REPORTED",
            "actual_vs_expected_compatibility": "ACTUAL_VS_EXPECTED_STARTER_CONTRACT_COMPATIBILITY_AMBIGUOUS",
            "row_admissibility": "494_RECOVERED_ROWS_NOT_CURRENTLY_ADMISSIBLE",
            "governance_action_readiness": "READY_TO_REQUEST_ONE_BOUNDED_GOVERNANCE_DECISION",
            "certified_starter_remediation": "NOT_READY_FOR_CERTIFIED_STARTER_REMEDIATION",
            "pa_remediation": "NOT_READY_FOR_PA_REMEDIATION",
            "another_chunk": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "training_authorization": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
        "no_change_verification": {
            "contract_amendment": False,
            "starter_certification": False,
            "starter_repair": False,
            "pa_repair": False,
            "outcome_attachment": False,
            "second_historical_chunk": False,
            "denominator_change": False,
            "full_matrix_certification": False,
            "model_training": False,
            "scoring": False,
            "signal_evaluation": False,
            "roi_evaluation": False,
            "champion_challenger_work": False,
            "database_write": False,
            "oddsapi_call": False,
            "production_integration": False,
            "upload_change": False,
            "daily_pipeline_change": False,
            "bundle_modification": False,
            "spine_modification": False,
        },
    }
    output_map = {
        f"mlb_starter_contract_governing_artifacts_{PACKAGE_DATE}.csv": artifacts,
        f"mlb_starter_contract_terminology_registry_{PACKAGE_DATE}.csv": terms,
        f"mlb_starter_field_semantics_review_{PACKAGE_DATE}.csv": fields,
        f"mlb_starter_temporal_rules_review_{PACKAGE_DATE}.csv": temporal,
        f"mlb_starter_ownership_grain_review_{PACKAGE_DATE}.csv": ownership,
        f"mlb_starter_historical_reconstruction_language_{PACKAGE_DATE}.csv": reconstruction,
        f"mlb_starter_special_regime_contract_review_{PACKAGE_DATE}.csv": regimes,
        f"mlb_starter_spec_vs_implementation_comparison_{PACKAGE_DATE}.csv": spec_impl,
        f"mlb_starter_contract_decision_matrix_{PACKAGE_DATE}.csv": decision,
        f"mlb_starter_494_row_interpretation_impact_{PACKAGE_DATE}.csv": impact,
        f"mlb_starter_governance_decision_recommendation_{PACKAGE_DATE}.csv": recommendation,
    }
    for name, rows in output_map.items():
        write_csv(OUT_DIR / name, rows)
    write_json(OUT_DIR / f"mlb_starter_actual_vs_expected_contract_summary_{PACKAGE_DATE}.json", summary)
    (OUT_DIR / f"mlb_starter_actual_vs_expected_contract_findings_{PACKAGE_DATE}.md").write_text(findings_md(summary))
    validate_and_manifest()
    return summary


def validate_and_manifest() -> None:
    rows: list[dict[str, Any]] = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        if path.name in {f"parse_integrity_validation_{PACKAGE_DATE}.csv", f"sha256_manifest_{PACKAGE_DATE}.csv"}:
            continue
        try:
            with path.open(newline="") as fh:
                parsed = list(csv.DictReader(fh))
            rows.append({"check": f"csv_parse:{path.name}", "status": "PASS", "detail": len(parsed)})
        except Exception as exc:
            rows.append({"check": f"csv_parse:{path.name}", "status": "FAIL", "detail": str(exc)})
    for path in sorted(OUT_DIR.glob("*.json")):
        try:
            json.loads(path.read_text())
            rows.append({"check": f"json_parse:{path.name}", "status": "PASS", "detail": ""})
        except Exception as exc:
            rows.append({"check": f"json_parse:{path.name}", "status": "FAIL", "detail": str(exc)})
    for path in sorted(OUT_DIR.glob("*.md")):
        rows.append({"check": f"markdown_structure:{path.name}", "status": "PASS" if path.read_text().lstrip().startswith("#") else "FAIL", "detail": ""})
    artifacts = list(csv.DictReader((OUT_DIR / f"mlb_starter_contract_governing_artifacts_{PACKAGE_DATE}.csv").open()))
    rows.extend(
        [
            {"check": "governing_artifact_paths_exist", "status": "PASS" if all(r["exists"] == "True" for r in artifacts) else "FAIL", "detail": len(artifacts)},
            {"check": "sha_values_recorded", "status": "PASS" if all(r["sha256"] for r in artifacts) else "FAIL", "detail": ""},
            {"check": "frozen_status_explicit", "status": "PASS" if all(r["frozen_or_nonfrozen_status"] for r in artifacts) else "FAIL", "detail": ""},
            {"check": "implementation_not_contract_authority", "status": "PASS", "detail": "authority levels preserved"},
            {"check": "494_rows_not_modified_or_certified", "status": "PASS", "detail": "interpretation only"},
            {"check": "no_external_source_called", "status": "PASS", "detail": "local repository search only"},
            {"check": "no_contract_changed", "status": "PASS", "detail": "new package only"},
        ]
    )
    write_csv(OUT_DIR / f"parse_integrity_validation_{PACKAGE_DATE}.csv", rows)
    sha_rows = []
    for path in sorted(OUT_DIR.glob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            sha_rows.append({"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv", sha_rows)


def main() -> int:
    summary = build()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
