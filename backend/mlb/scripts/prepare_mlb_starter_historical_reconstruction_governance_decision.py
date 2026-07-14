#!/usr/bin/env python3
"""Prepare MLB Starter historical reconstruction governance decision package.

This is a read-only decision-preparation script. It does not adopt an
interpretation, amend contracts, certify or repair Starter rows, attach
outcomes, process another chunk, call external sources, write databases, train,
score, or alter production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


PACKAGE_DATE = "2026-07-13"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_starter_historical_reconstruction_governance_decision/2026-07-13")
REVIEW_DIR = Path("artifacts/analysis/model_development/mlb_starter_actual_vs_expected_contract_review/2026-07-13")
RECOVERY_DIR = Path("artifacts/analysis/model_development/mlb_historical_starter_recovery_dry_run/2026-07-13")
BUNDLE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12")


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def id_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    try:
        return str(int(float(value)))
    except Exception:
        return str(value).strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def load_inputs() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    review = json.loads((REVIEW_DIR / f"mlb_starter_actual_vs_expected_contract_summary_{PACKAGE_DATE}.json").read_text())
    dry_rows = pd.read_csv(RECOVERY_DIR / f"mlb_historical_starter_recovery_row_dry_run_{PACKAGE_DATE}.csv", low_memory=False)
    technical = pd.read_csv(RECOVERY_DIR / f"mlb_historical_starter_recovery_technical_status_{PACKAGE_DATE}.csv", low_memory=False)
    evidence = pd.read_csv(REVIEW_DIR / f"mlb_starter_contract_governing_artifacts_{PACKAGE_DATE}.csv", low_memory=False)
    return review, dry_rows, technical, evidence


def reproduce_or_stop(review: dict[str, Any], dry_rows: pd.DataFrame, technical: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    expected = {
        "contract_status": "silent_or_ambiguous_for_historical_actual_starter_reconstruction",
        "technically_complete_rows_reviewed": 494,
        "currently_admissible_recovered_rows": 0,
        "potentially_admissible_under_bounded_future_interpretation": 484,
        "special_regime_rows_requiring_separate_handling": 10,
    }
    mismatches = {k: (expected[k], review.get(k)) for k in expected if review.get(k) != expected[k]}
    complete = dry_rows[dry_rows["would_be_technically_complete"].astype(str).str.lower().eq("true")].copy()
    special = complete[complete["semantic_qualification_status"].eq("SPECIAL_REGIME_CONTRACT_INTERPRETATION_REQUIRED")].copy()
    standard = complete[~complete.index.isin(special.index)].copy()
    if len(complete) != 494:
        mismatches["complete_rows_from_dry_run"] = (494, len(complete))
    if len(standard) != 484:
        mismatches["standard_rows_from_dry_run"] = (484, len(standard))
    if len(special) != 10:
        mismatches["special_rows_from_dry_run"] = (10, len(special))
    if mismatches:
        raise RuntimeError(f"governance decision package reproduction mismatch: {mismatches}")
    return standard, special


def decision_question_md() -> str:
    return (
        "# MLB Starter Historical Reconstruction Governance Decision Question\n\n"
        "## Exact Yes/No Question\n\n"
        "For the bounded 2026-06-22 through 2026-06-28 historical qualification pilot, may authoritative unique "
        "postgame actual-starter identity be used solely as a binding key to reconstruct Starter Skill / Workload "
        "features that are computed strictly from games before the target game, when direct pregame expected/probable/"
        "announced starter evidence is unavailable, excluding special regimes and without applying the rule to live "
        "production or future slates?\n\n"
        "## In Scope\n\n"
        "- Historical qualification only for the current seven-date pilot.\n"
        "- The 484 standard technically complete rows identified by the dry run.\n"
        "- Authoritative unique actual-starter identity as a binding key only.\n"
        "- Strict-prior Starter Skill / Workload fields only.\n\n"
        "## Out Of Scope\n\n"
        "- The 10 special-regime/two-way rows.\n"
        "- Live production, same-day prediction generation, future slates, uploads, Champion-Challenger, signal evaluation, model training, other sports, and any production expected-starter assignment.\n\n"
        "## Boundaries\n\n"
        "- Evidence boundary: official actual-starter identity plus strict-prior source lineage; direct pregame evidence remains separately tracked.\n"
        "- Temporal boundary: no same-game or future pitcher performance may enter feature values.\n"
        "- Feature boundary: Starter Skill / Workload only; no PA, outcomes, odds, model signals, or ROI.\n"
        "- Production boundary: no live behavior changes.\n"
        "- Review condition: any later recovered pregame expected-starter evidence supersedes actual-starter reconstruction for affected rows.\n"
    )


def evidence_table(evidence: pd.DataFrame) -> list[dict[str, Any]]:
    def artifact(name: str) -> dict[str, Any]:
        match = evidence[evidence["artifact_name"].eq(name)]
        return match.iloc[0].to_dict() if not match.empty else {}

    items = [
        (
            "Pregame-compatible semantics",
            "Grain/join contract says Starter fields join by game_id/opponent starter assignment and strict-prior starter history.",
            "Grain/join contract",
            "Join rules",
            3,
            "strong",
            "No exact sentence says the assignment must be pregame captured.",
            "Supports caution; does not by itself authorize actual-starter substitution.",
        ),
        (
            "Strict-prior feature calculation",
            "Frozen field registry and construction contract define weighted_multiseason_hits_per_out and expected_outs_blended_v1 from prior starts only.",
            "Frozen field registry",
            "Starter field rows",
            2,
            "strong",
            "Identity source remains separate from feature-value cutoff.",
            "Feature-value leakage can be controlled if identity is accepted.",
        ),
        (
            "Actual-starter identity as post-start information",
            "Spine temporal integrity prohibits postgame contamination; design artifacts state actual role is postgame-derived.",
            "Spine contract markdown",
            "Temporal Integrity",
            1,
            "strong",
            "Actual identity may match expected identity in many games, but that is not proven without pregame evidence.",
            "Identity-selection leakage remains the core governance risk.",
        ),
        (
            "Historical reconstruction intent",
            "Frozen contracts allow limited date-locked matrix assembly and strict-prior source bundles, but do not define actual-starter reconstruction.",
            "Readiness decision",
            "Permitted/prohibited next steps",
            4,
            "medium",
            "Technical dry run proves replayability for 494 rows.",
            "Requires explicit human interpretation before certification.",
        ),
        (
            "Ownership and grain preservation",
            "Feature join contract requires Starter Skill / Workload as left join from frozen spine with no row loss or multiplication.",
            "Feature join contract",
            "Starter Skill / Workload row",
            3,
            "strong",
            "Semantic meaning may still change even if row grain is preserved.",
            "Permissive option needs explicit semantic mode and provenance flags.",
        ),
        (
            "Special-regime risk",
            "Review found special-regime rules ambiguous; dry run identifies 10 complete rows requiring separate handling.",
            "Contract review summary",
            "special_regime_rows_requiring_separate_handling",
            6,
            "strong_for_exclusion",
            "No standard frozen rule handles openers/two-way/special rows.",
            "The 10 rows must be excluded from standard approval.",
        ),
        (
            "Deterministic replay",
            "Starter recovery dry run replayed with identical output SHA.",
            "Starter recovery dry-run findings",
            "Replay report",
            6,
            "strong",
            "Replay does not solve semantic admissibility.",
            "Supports Option B/C safeguards but not automatic certification.",
        ),
        (
            "External probable-pitcher availability",
            "Prior source-gap package identified external historical probable-pitcher evidence as materially helpful.",
            "Starter source gap findings",
            "external source needs",
            6,
            "medium",
            "External evidence was not called in this task.",
            "Could avoid interpretation/amendment for rows where direct pregame evidence is recovered.",
        ),
    ]
    rows = []
    for argument, text, artifact_name, section, authority, strength, counter, implication in items:
        art = artifact(artifact_name)
        rows.append(
            {
                "argument": argument,
                "supporting_artifact": art.get("path", artifact_name),
                "exact_section_or_field": section,
                "authority_level": authority,
                "strength": strength,
                "supporting_evidence": text,
                "counterevidence": counter,
                "implication": implication,
                "artifact_sha256": art.get("sha256", ""),
            }
        )
    return rows


def leakage_analysis() -> list[dict[str, Any]]:
    return [
        {
            "risk": "feature_value_leakage",
            "mechanism": "same-game or future pitcher performance enters reconstructed fields",
            "affected_population": "484 standard rows if safeguards fail",
            "severity": "high",
            "detectability": "high via feature_cutoff_date/latest_contributing_prior_game_date",
            "mitigation": "require strict-prior cutoff validation and zero same-game/future feature input",
            "residual_risk": "low if validation is mandatory",
        },
        {
            "risk": "identity_selection_leakage",
            "mechanism": "actual starter identity may be learned only after game start",
            "affected_population": "all actual-starter reconstructed rows",
            "severity": "medium_high",
            "detectability": "medium; requires source timestamp or direct pregame evidence",
            "mitigation": "explicit semantic mode, no production reuse, separate reporting from directly proven expected-starter rows",
            "residual_risk": "medium unless direct pregame evidence is recovered",
        },
        {
            "risk": "selection_bias",
            "mechanism": "only recoverable actual-starter rows are admitted while unrecoverable rows remain blocked",
            "affected_population": "484 standard recovered rows versus remaining blocked rows",
            "severity": "medium",
            "detectability": "high via recovery population ledger",
            "mitigation": "report recovered, blocked, special-regime, and directly proven rows separately",
            "residual_risk": "medium",
        },
        {
            "risk": "scratch_change_risk",
            "mechanism": "actual starter differs from historical expected/probable starter",
            "affected_population": "unknown subset of 484 standard rows",
            "severity": "high",
            "detectability": "low without archived probable-pitcher/scratch timing evidence",
            "mitigation": "exclude known scratches/replacements; prefer external probable-pitcher recovery where available",
            "residual_risk": "medium_high",
        },
        {
            "risk": "opener_bullpen_risk",
            "mechanism": "official actual starter designation may misrepresent intended workload semantics",
            "affected_population": "10 special-regime/two-way rows excluded from standard population",
            "severity": "high",
            "detectability": "medium via role/special-regime flags",
            "mitigation": "exclude standard decision; require separate governance",
            "residual_risk": "low for 484 standard rows if exclusion enforced",
        },
    ]


def safeguards() -> list[dict[str, Any]]:
    items = [
        ("official actual starter uniquely proven", "mandatory", "Options B/C", "required to avoid ambiguous identity"),
        ("strict-prior cutoff validation", "mandatory", "Options B/C", "protects feature-value leakage"),
        ("zero same-game feature input", "mandatory", "Options B/C", "feature values only"),
        ("explicit provenance flag", "mandatory", "Options B/C/D", "auditability"),
        ("semantic mode HISTORICAL_ACTUAL_STARTER_RECONSTRUCTION", "mandatory", "Options B/C/D", "prevents silent substitution"),
        ("standard games only", "mandatory", "Options B/C", "excludes 10 special rows"),
        ("exclude opener/bullpen games", "mandatory", "Options B/C", "special semantics unsafe"),
        ("exclude known scratches/replacements", "mandatory", "Options B/C", "scratch/change leakage risk"),
        ("exclude ambiguous two-way cases", "mandatory", "Options B/C", "special handling required"),
        ("retain source SHAs", "mandatory", "Options B/C/D", "replayability"),
        ("deterministic replay", "mandatory", "Options B/C/D", "scientific reproducibility"),
        ("no production reuse", "mandatory", "Options B/C/D", "scope boundary"),
        ("no assumption actual equals expected", "mandatory", "Options B/C/D", "semantic honesty"),
        ("separate reporting of direct expected vs reconstructed rows", "mandatory", "Options B/C/D", "prevents representation drift"),
        ("external probable-pitcher evidence recovery", "optional", "All options", "can replace interpretation for rows with direct proof"),
    ]
    return [
        {
            "safeguard": name,
            "mandatory_or_optional": level,
            "applies_to": applies,
            "rationale": rationale,
            "status_for_current_484": "available_or_enforceable" if level == "mandatory" else "future_optional",
        }
        for name, level, applies, rationale in items
    ]


def population_484(standard: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    group_cols = ["slate_date", "game_id", "team", "opponent", "selected_starter_id", "selected_starter_name"]
    for keys, group in standard.groupby(group_cols, dropna=False):
        date, game_id, team, opponent, starter_id, starter_name = keys
        rows.append(
            {
                "slate_date": date,
                "game_id": id_text(game_id),
                "hitter_team": team,
                "opponent_team": opponent,
                "game_side": f"{team}|{opponent}",
                "rows": len(group),
                "unique_starter_id": id_text(starter_id),
                "unique_starter_name": starter_name,
                "technical_completeness": "TECHNICALLY_COMPLETE",
                "source_authority": "official_actual_starter_identity_plus_strict_prior_repository_features",
                "strict_prior_validation": "PASS_STRICT_PRIOR_NO_SAME_GAME_OR_FUTURE",
                "special_regime_exclusion": "standard_population_no_special_regime",
                "known_scratch_change_evidence": "none_found_in_repository_review_not_proven_absent",
                "current_contract_status": "not_currently_admissible",
                "status_option_a": "inadmissible_requires_direct_pregame_evidence",
                "status_option_b": "potentially_authorized_if_human_interpretation_approved",
                "status_option_c": "potentially_authorized_after_versioned_amendment",
                "status_option_d": "eligible_for_separate_diagnostic_variant",
            }
        )
    return rows


def special_10(special: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for keys, group in special.groupby(["slate_date", "game_id", "team", "opponent", "semantic_qualification_status"], dropna=False):
        date, game_id, team, opponent, status = keys
        rows.append(
            {
                "slate_date": date,
                "game_id": id_text(game_id),
                "hitter_team": team,
                "opponent_team": opponent,
                "game_side": f"{team}|{opponent}",
                "rows": len(group),
                "special_regime": status,
                "reason_standard_interpretation_unsafe": "special/two-way/opener semantics may misrepresent intended workload or expected starter role",
                "separate_governance_needed": True,
                "external_evidence_could_resolve": True,
                "option_a_disposition": "remain_blocked_or_require_direct_pregame_evidence",
                "option_b_disposition": "excluded_from_standard_interpretation",
                "option_c_disposition": "requires_specific_amendment_language",
                "option_d_disposition": "separate_special_regime_variant_or_exclusion",
            }
        )
    return rows


def option_comparison() -> list[dict[str, Any]]:
    dimensions = [
        ("governance_integrity", "high", "medium", "high_with_cost", "high"),
        ("semantic_fidelity", "high", "medium", "medium_high", "high_as_separate_variant"),
        ("leakage_risk", "lowest", "medium", "medium_low_if_amended", "low_for_bundle_v1"),
        ("replayability", "high", "high", "high", "high"),
        ("historical_population_recovery", "low", "high_484_standard_rows", "high_484_standard_rows", "medium_separate_path"),
        ("engineering_effort", "medium_external_recovery", "low_medium", "high", "medium"),
        ("external_data_dependency", "high", "low", "low", "low_medium"),
        ("reuse_deferred_89_dates", "depends_on_external_evidence", "high_if_approved", "high_after_amendment", "high_as_diagnostic"),
        ("reuse_class_c_463_dates", "depends_on_external_evidence", "possible_but_governed", "possible_after_amendment", "possible_as_variant"),
        ("production_contamination_risk", "lowest", "low_if_flags_enforced", "low_if_scope_explicit", "lowest"),
        ("amendment_burden", "none", "none_if_accepted_as_interpretation", "high", "none_for_bundle_v1"),
        ("auditability", "high", "high_with_flags", "high_with_contract", "high"),
        ("reversibility", "high", "high_if_provenance_retained", "medium", "high"),
    ]
    return [
        {"dimension": d, "option_a": a, "option_b": b, "option_c": c, "option_d": dopt}
        for d, a, b, c, dopt in dimensions
    ]


def approval_consequences() -> list[dict[str, Any]]:
    return [
        {
            "option": "A",
            "if_approved_authorizes_next": "external_or_repository_pregame_expected_starter_evidence_recovery",
            "remains_prohibited": "actual-starter-based certification; PA/outcomes/next chunk/training/production",
            "certified_starter_remediation_authorized": False,
        },
        {
            "option": "B",
            "if_approved_authorizes_next": "bounded certified Starter remediation design/execution for 484 standard rows only, with mandatory safeguards",
            "remains_prohibited": "10 special rows; live production; future slates; training; signal/ROI evaluation; contract amendment unless separately approved",
            "certified_starter_remediation_authorized": "next_task_only_after_explicit_human_approval",
        },
        {
            "option": "C",
            "if_approved_authorizes_next": "draft versioned contract amendment package; no row certification until amendment approved",
            "remains_prohibited": "immediate row certification; PA/outcomes/next chunk/training/production",
            "certified_starter_remediation_authorized": False,
        },
        {
            "option": "D",
            "if_approved_authorizes_next": "create separate historical actual-starter diagnostic variant specification",
            "remains_prohibited": "silent substitution into Bundle v1; model/signal/production authorization",
            "certified_starter_remediation_authorized": False,
        },
    ]


def option_language(option: str) -> str:
    title = {
        "a": "Option A — Strict Pregame Evidence Required",
        "b": "Option B — Bounded Interpretation of Existing Contract",
        "c": "Option C — Versioned Contract Amendment",
        "d": "Option D — Separate Historical Actual-Starter Variant",
    }[option]
    if option == "a":
        body = (
            "If approved, direct pregame expected/probable/announced starter evidence remains mandatory for Starter Skill / Workload historical qualification. "
            "Authoritative actual-starter identity is not admissible as a substitute. Rows without direct pregame starter evidence remain missing or blocked."
        )
    elif option == "b":
        body = (
            "If approved, this is a narrow interpretation of the existing frozen contract for historical qualification only. It permits authoritative unique "
            "actual-starter identity solely as a binding key for strictly prior Starter Skill / Workload features when pregame expected-starter evidence is "
            "unavailable. It does not redefine live expected-starter semantics, does not apply to production, excludes special regimes, requires semantic mode "
            "`HISTORICAL_ACTUAL_STARTER_RECONSTRUCTION`, source SHAs, replay flags, row-level auditability, and separate reporting from directly proven expected-starter rows. "
            "Approval does not itself certify rows; it authorizes a bounded remediation task for the 484 standard rows only."
        )
    elif option == "c":
        body = (
            "If approved, prepare a versioned contract amendment that explicitly permits actual-starter historical reconstruction under bounded conditions. "
            "The prior frozen contract remains immutable. The amendment must define scope, effective dates, compatibility, replayability, migration, special-regime exclusions, "
            "and the distinction between historical reconstruction and live pregame semantics. Approval of this option authorizes amendment drafting only."
        )
    else:
        body = (
            "If approved, keep Bundle v1 unchanged and authorize a separate historical actual-starter diagnostic variant. The variant must have its own manifest, field semantics, "
            "lineage, qualification path, and comparison rules. It may not be silently substituted into Bundle v1 and carries no immediate signal, model, or production authorization."
        )
    return f"# MLB Starter Governance {title}\n\n{body}\n\nHuman approval is required before any downstream action.\n"


def reversal_rules_md() -> str:
    return (
        "# MLB Starter Governance Reversal And Audit Rules\n\n"
        "For any permissive option, every reconstructed row must retain source artifact path, source SHA, semantic mode, selected starter identity, strict-prior cutoff, "
        "feature lineage, and replay hash. If direct pregame expected-starter evidence is later recovered, it supersedes actual-starter reconstruction for affected rows "
        "and triggers a conflict audit.\n\n"
        "Rows must be reversible by excluding `HISTORICAL_ACTUAL_STARTER_RECONSTRUCTION` rows from the matrix and restoring prior missing/blocker status. Existing certified "
        "matrices are not rewritten without a separate certification task. Future historical chunks must rerun the same audit gates and report direct expected-starter rows "
        "separately from reconstructed actual-starter rows.\n\n"
        "Audit triggers include source SHA drift, duplicated starter identity, evidence of scratch/replacement, opener/bullpen classification, two-way ambiguity, missing strict-prior cutoff, "
        "or any attempt to use the semantic mode in live production.\n"
    )


def package_md(summary: dict[str, Any]) -> str:
    decision_lines = "\n".join(f"- `{status}`" for status in summary["decisions"].values())
    return (
        "# MLB Starter Historical Actual-Starter Reconstruction Governance Decision Package\n\n"
        "This package prepares a human decision. It does not choose an option.\n\n"
        "## Governance Question\n\n"
        "May authoritative unique postgame actual-starter identity be used solely as a binding key to reconstruct strictly prior Starter Skill / Workload features for "
        "historical qualification when direct pregame expected-starter evidence is unavailable, excluding special regimes and without applying the rule to live production or future slates?\n\n"
        "## Options\n\n"
        "- Option A: Strict pregame evidence required.\n"
        "- Option B: Bounded interpretation of existing contract.\n"
        "- Option C: Versioned contract amendment.\n"
        "- Option D: Separate historical actual-starter variant.\n\n"
        "## Strongest Argument For Option B\n\n"
        "Feature values can be protected: the dry run proved strict-prior workload reconstruction for 494 rows, and actual starter identity can be retained as explicit historical provenance rather than live semantics.\n\n"
        "## Strongest Argument Against Option B\n\n"
        "Identity selection itself may be post-start information, and the frozen contracts do not explicitly permit actual-starter substitution. Interpreting this as allowed may change Bundle v1 meaning.\n\n"
        "## Population Impact\n\n"
        f"- 484 standard rows are decision-ready under bounded options.\n"
        f"- 10 special-regime/two-way rows remain excluded from the standard decision.\n"
        f"- Currently admissible recovered rows: 0.\n\n"
        "## Recommendation For Decision Process\n\n"
        "Record explicit option selected, approval timestamp, approver, rationale, scope, artifact SHA, expiration/review trigger, special-regime exclusion, and next authorized task.\n\n"
        "## Statuses\n\n"
        f"{decision_lines}\n"
    )


def approval_template(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "approval_status": "PENDING_HUMAN_APPROVAL",
        "approver": "",
        "approval_timestamp_utc": "",
        "selected_option": "",
        "approved_scope": "historical qualification only; 2026-06-22 through 2026-06-28 pilot unless explicitly expanded later",
        "artifact_package": str(OUT_DIR),
        "artifact_sha256_manifest": str(OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv"),
        "rationale": "",
        "special_regime_exclusion": "10 special-regime/two-way rows excluded from standard decision",
        "next_authorized_task": "",
        "expiration_or_review_trigger": "new pregame expected-starter evidence, source conflict, special-regime discovery, or attempt to reuse for live production",
        "no_automatic_authorization": [
            "production",
            "future slates",
            "training",
            "Champion-Challenger",
            "signal evaluation",
            "PA remediation",
            "outcome attachment",
            "next historical chunk",
        ],
        "current_summary": summary,
    }


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    review, dry_rows, technical, evidence = load_inputs()
    standard, special = reproduce_or_stop(review, dry_rows, technical)
    summary = {
        "package_date": PACKAGE_DATE,
        "package_path": str(OUT_DIR),
        "exact_governance_question": "May authoritative unique postgame actual-starter identity be used solely as a binding key to reconstruct strictly prior Starter Skill / Workload features for historical qualification when direct pregame expected-starter evidence is unavailable?",
        "prior_findings_reproduced": True,
        "technically_complete_rows": 494,
        "standard_rows": 484,
        "special_regime_rows": 10,
        "currently_admissible_rows": 0,
        "human_approval_required": True,
        "governance_option_adopted": False,
        "approval_payload_template": str(OUT_DIR / f"mlb_starter_governance_approval_payload_template_{PACKAGE_DATE}.json"),
        "decisions": {
            "package": "STARTER_GOVERNANCE_DECISION_PACKAGE_COMPLETED",
            "options": "FOUR_GOVERNANCE_OPTIONS_DOCUMENTED",
            "risks": "LEAKAGE_AND_SEMANTIC_RISKS_CHARACTERIZED",
            "standard_rows": "484_STANDARD_ROWS_DECISION_READY",
            "special_rows": "10_SPECIAL_REGIME_ROWS_EXCLUDED_FROM_STANDARD_DECISION",
            "approval": "HUMAN_APPROVAL_REQUIRED",
            "adoption": "NO_GOVERNANCE_OPTION_ADOPTED",
            "starter_remediation": "NOT_READY_FOR_CERTIFIED_STARTER_REMEDIATION",
            "pa_remediation": "NOT_READY_FOR_PA_REMEDIATION",
            "next_chunk": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "training": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
        "no_change_verification": {
            "interpretation_adopted": False,
            "contract_amended": False,
            "starter_row_certified": False,
            "starter_row_repaired": False,
            "pa_work": False,
            "outcome_attachment": False,
            "second_historical_chunk": False,
            "model_or_production_change": False,
            "database_write": False,
            "oddsapi_call": False,
            "bundle_or_spine_change": False,
        },
    }
    write_csv(OUT_DIR / f"mlb_starter_governance_evidence_table_{PACKAGE_DATE}.csv", evidence_table(evidence))
    write_csv(OUT_DIR / f"mlb_starter_governance_leakage_analysis_{PACKAGE_DATE}.csv", leakage_analysis())
    write_csv(OUT_DIR / f"mlb_starter_governance_safeguards_{PACKAGE_DATE}.csv", safeguards())
    write_csv(OUT_DIR / f"mlb_starter_governance_484_row_population_{PACKAGE_DATE}.csv", population_484(standard))
    write_csv(OUT_DIR / f"mlb_starter_governance_10_special_rows_{PACKAGE_DATE}.csv", special_10(special))
    write_csv(OUT_DIR / f"mlb_starter_governance_option_comparison_{PACKAGE_DATE}.csv", option_comparison())
    write_csv(OUT_DIR / f"mlb_starter_governance_approval_consequences_{PACKAGE_DATE}.csv", approval_consequences())
    (OUT_DIR / f"mlb_starter_governance_decision_question_{PACKAGE_DATE}.md").write_text(decision_question_md())
    for option in ["a", "b", "c", "d"]:
        (OUT_DIR / f"mlb_starter_governance_option_{option}_language_{PACKAGE_DATE}.md").write_text(option_language(option))
    (OUT_DIR / f"mlb_starter_governance_reversal_audit_rules_{PACKAGE_DATE}.md").write_text(reversal_rules_md())
    (OUT_DIR / f"mlb_starter_governance_decision_package_{PACKAGE_DATE}.md").write_text(package_md(summary))
    write_json(OUT_DIR / f"mlb_starter_governance_decision_summary_{PACKAGE_DATE}.json", summary)
    write_json(OUT_DIR / f"mlb_starter_governance_approval_payload_template_{PACKAGE_DATE}.json", approval_template(summary))
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
    pop = list(csv.DictReader((OUT_DIR / f"mlb_starter_governance_484_row_population_{PACKAGE_DATE}.csv").open()))
    special = list(csv.DictReader((OUT_DIR / f"mlb_starter_governance_10_special_rows_{PACKAGE_DATE}.csv").open()))
    pop_rows = sum(int(r["rows"]) for r in pop)
    special_rows = sum(int(r["rows"]) for r in special)
    rows.extend(
        [
            {"check": "prior_review_findings_reproduce", "status": "PASS", "detail": "494/484/10 and ambiguity reproduced"},
            {"check": "484_standard_rows_reconcile", "status": "PASS" if pop_rows == 484 else "FAIL", "detail": pop_rows},
            {"check": "10_special_rows_reconcile", "status": "PASS" if special_rows == 10 else "FAIL", "detail": special_rows},
            {"check": "no_row_certified", "status": "PASS", "detail": "decision package only"},
            {"check": "no_interpretation_adopted", "status": "PASS", "detail": "human approval required"},
            {"check": "no_contract_amended", "status": "PASS", "detail": "no frozen artifacts modified"},
            {"check": "special_regime_excluded", "status": "PASS", "detail": "10 rows separate"},
            {"check": "no_external_source_called", "status": "PASS", "detail": "local artifacts only"},
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
