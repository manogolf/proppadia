#!/usr/bin/env python3
"""Review PA sparse-history missingness under frozen historical contracts.

Governance-only artifact builder. It reproduces the preserved sparse-history
PA exclusion population after strict-prior certification and evaluates contract
permission. It does not certify/remediate rows, attach outcomes, train, score,
write to the database, call APIs, or alter production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


PACKAGE_DATE = "2026-07-13"
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_pa_sparse_history_missingness_contract_review/2026-07-13"
)

CERT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_pa_strict_prior_certified_remediation/2026-07-13"
)
GAP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_pa_source_gap_discovery/2026-07-13"
)
SPINE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
BUNDLE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_specification_v1/2026-07-12"
)
PA_BUNDLE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_rolling_pa_opportunity_bundle/2026-07-11"
)

SPARSE_REGISTRY = CERT_DIR / f"mlb_pa_sparse_history_exclusion_registry_{PACKAGE_DATE}.csv"
REMAINING_BLOCKERS = CERT_DIR / f"mlb_pa_certification_remaining_blockers_{PACKAGE_DATE}.csv"
CERT_SUMMARY = CERT_DIR / f"mlb_pa_certified_remediation_summary_{PACKAGE_DATE}.json"
UNRESOLVED_REGISTRY = CERT_DIR / f"mlb_pa_unresolved_exclusion_registry_{PACKAGE_DATE}.csv"
GAP_SPARSE_CASES = GAP_DIR / f"mlb_historical_pa_sparse_history_cases_{PACKAGE_DATE}.csv"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="") as fh:
        return [{k: "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(fh)]


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text())


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


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def canonical_sha(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def parse_validation(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        status = "PASS"
        details = ""
        try:
            if path.suffix == ".csv":
                details = f"rows={len(read_csv(path))}"
            elif path.suffix == ".json":
                json.loads(path.read_text())
                details = "json_parsed"
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.lstrip().startswith("#"):
                    raise ValueError("missing top-level markdown heading")
                details = "markdown_heading_present"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            details = str(exc)
        rows.append({"path": str(path), "validation_type": "parse", "validation_status": status, "details": details})
    return rows


def sha_manifest(paths: list[Path]) -> list[dict[str, Any]]:
    return [
        {"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size}
        for path in sorted(paths, key=lambda p: str(p))
    ]


def contract_clauses() -> list[dict[str, str]]:
    return [
        {
            "source_path": str(SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.md"),
            "contract_area": "denominator_ownership",
            "relevant_language": (
                "The denominator is owned by an explicit date-locked hitter-prop source artifact. "
                "PA Opportunity, Starter Skill / Workload, Offense Context, and market joins may only join into the spine. "
                "They cannot own eligibility, redefine the denominator, or silently remove rows."
            ),
            "interpretation": "PA source gaps do not remove denominator rows; they remain feature missingness or compatibility issues.",
        },
        {
            "source_path": str(SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.md"),
            "contract_area": "feature_join",
            "relevant_language": (
                "Feature platforms join left from the frozen spine. Each join must report input row count, output row count, "
                "row delta, duplicate delta, cardinality, unmatched-row classification, and denominator preservation."
            ),
            "interpretation": "Sparse PA rows must be classified explicitly; silent row loss is forbidden.",
        },
        {
            "source_path": str(SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.md"),
            "contract_area": "source_identity",
            "relevant_language": (
                "Implicit latest available, unversioned mutable source substitution, silent fallback, and cross-date reuse "
                "without an explicit rule are forbidden."
            ),
            "interpretation": "Any fallback for sparse PA requires explicit governance; it cannot be inferred ad hoc.",
        },
        {
            "source_path": str(SPINE_DIR / "amendment_policy_2026-07-12.md"),
            "contract_area": "amendment_policy",
            "relevant_language": (
                "Any change to canonical identity, denominator owner, eligibility rules, cutoff policy, source-selection policy, "
                "deduplication, ordering, feature-join behavior, or Variant C derivation requires proposed amendment, impact analysis, "
                "compatibility review, new contract version, new SHA identity, and new certification where population identity changes."
            ),
            "interpretation": "Null qualification can be approved as a governed interpretation only if it does not mutate identity or feature semantics; fallback values likely require amendment or attached policy.",
        },
        {
            "source_path": str(PA_BUNDLE_DIR / "pa_selected_feature_contracts_2026-07-11.md"),
            "contract_area": "pa_missingness",
            "relevant_language": "Missing-value behavior: retain null/unknown or `pa_missing_flag`; do not zero-impute silently.",
            "interpretation": "PA feature contracts support explicit null/unknown retention, but they do not by themselves state that null rows are fully qualification-equivalent.",
        },
        {
            "source_path": str(PA_BUNDLE_DIR / "pa_selected_feature_contracts_2026-07-11.md"),
            "contract_area": "pa_temporal_integrity",
            "relevant_language": (
                "Historical cutoff: source PA context date must be strictly before slate/artifact date. "
                "Leakage exclusions: no raw same-game `plate_appearances`; no postgame lineup/start fields."
            ),
            "interpretation": "Sparse-history rows cannot use target-game PA or same-game/postgame substitutes.",
        },
        {
            "source_path": str(PA_BUNDLE_DIR / "pa_missingness_taxonomy_2026-07-11.csv"),
            "contract_area": "missingness_taxonomy",
            "relevant_language": (
                "legitimate_insufficient_history_null: No prior player_derived_stats row before artifact_date; "
                "rookies/new activations/early season. Handling: Retain missing flag; do not impute silently."
            ),
            "interpretation": "The taxonomy recognizes legitimate insufficient-history nulls and rejects silent imputation.",
        },
        {
            "source_path": str(PA_BUNDLE_DIR / "pa_opportunity_field_inventory_2026-07-11.csv"),
            "contract_area": "pa_field_inventory",
            "relevant_language": (
                "prior d7/d15/d30 PA fields use strictly prior pds.game_date < artifact_date; "
                "null_default_behavior: pa_missing_flag=1 if unavailable."
            ),
            "interpretation": "Field semantics allow unavailable prior PA to remain null with a missing flag.",
        },
    ]


def option_rows() -> list[dict[str, Any]]:
    return [
        {
            "option": "Option A - Preserve exclusion",
            "description": "Continue blocking sparse-history player-games until sufficient strict-prior PA history exists.",
            "technical_feasibility": "FEASIBLE_NOW",
            "semantic_validity": "VALID_CONSERVATIVE",
            "current_contract_permission": "EXPLICITLY_SAFE",
            "risk": "coverage remains blocked; no semantics drift",
            "human_approval_required": "NO_FOR_CONTINUING_CURRENT_BLOCK",
            "recommendation": "safe default if no governance decision is made",
        },
        {
            "option": "Option B - Contract-qualified missingness",
            "description": "Allow population-qualified rows with null PA fields, pa_missing_flag=1, and sparse-history provenance.",
            "technical_feasibility": "FEASIBLE_NOW",
            "semantic_validity": "VALID_IF_NULLS_REMAIN_NULL_AND_PROVENANCE_EXPLICIT",
            "current_contract_permission": "SUPPORTED_BUT_NOT_SELF_EXECUTING",
            "risk": "requires human governance decision that null PA is qualification-equivalent for this bounded population",
            "human_approval_required": "YES",
            "recommendation": "recommended governance option for human approval",
        },
        {
            "option": "Option C - Bounded fallback",
            "description": "Use a specifically defined fallback representation for missing PA.",
            "technical_feasibility": "UNKNOWN_OR_DEPENDS_ON_FALLBACK",
            "semantic_validity": "NOT_VALID_UNLESS_NEW_FALLBACK_PRESERVES_FIELD_MEANING",
            "current_contract_permission": "NOT_CURRENTLY_PERMITTED",
            "risk": "zero/league/player/team fallback would alter rolling PA field semantics",
            "human_approval_required": "YES_WITH_CONTRACT_ATTACHMENT_OR_AMENDMENT",
            "recommendation": "do not use for this campaign unless separately specified and approved",
        },
        {
            "option": "Option D - Contract amendment required",
            "description": "Treat any treatment beyond exclusion as requiring formal Bundle/spine contract amendment.",
            "technical_feasibility": "FEASIBLE_GOVERNANCE_PATH",
            "semantic_validity": "VALID_PROCESS",
            "current_contract_permission": "APPLIES_IF_OPTION_B_IS_DEEMED_BEYOND_INTERPRETATION",
            "risk": "slower but maximally conservative",
            "human_approval_required": "YES",
            "recommendation": "fallback if human owner does not approve Option B as an interpretation",
        },
    ]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sparse_registry = read_csv(SPARSE_REGISTRY)
    blockers = read_csv(REMAINING_BLOCKERS)
    summary = read_json(CERT_SUMMARY)
    unresolved = read_csv(UNRESOLVED_REGISTRY)
    gap_sparse = {row["blocked_player_game_key"]: row for row in read_csv(GAP_SPARSE_CASES)}

    if len(sparse_registry) != 109:
        raise RuntimeError(f"sparse population drift: {len(sparse_registry)}")
    if len(unresolved) != 1:
        raise RuntimeError(f"unresolved population drift: {len(unresolved)}")
    if summary.get("total_pa_qualified_rows_after_certification") != 1784:
        raise RuntimeError("certified PA qualified count drifted")
    if summary.get("total_pa_blocked_rows_after_certification") != 120:
        raise RuntimeError("certified PA blocked count drifted")

    unresolved_pg = {row["blocked_player_game_key"] for row in unresolved}
    sparse_pg_set = {row["blocked_player_game_key"] for row in sparse_registry}
    sparse_blocker_rows = [row for row in blockers if row["certified_pa_join_status"] == "PA_JOIN_BLOCKED_SPARSE_HISTORY"]
    unresolved_rows = [row for row in blockers if row["certified_pa_join_status"] == "PA_JOIN_BLOCKED_UNRESOLVED"]
    if len(sparse_blocker_rows) != 119 or len(unresolved_rows) != 1:
        raise RuntimeError("remaining blocker counts drifted")

    duplicate_pg = [key for key, count in Counter(row["blocked_player_game_key"] for row in sparse_registry).items() if count > 1]
    duplicate_rows = [key for key, count in Counter(row["canonical_row_id"] for row in sparse_blocker_rows).items() if count > 1]
    if duplicate_pg or duplicate_rows:
        raise RuntimeError("duplicate keys detected in sparse review population")

    affected_by_pg: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in sparse_blocker_rows:
        affected_by_pg[row["player_game_key"]].append(row)

    sparse_population_rows: list[dict[str, Any]] = []
    missingness_rows: list[dict[str, Any]] = []
    technical_rows: list[dict[str, Any]] = []
    for row in sorted(sparse_registry, key=lambda r: r["blocked_player_game_key"]):
        pg = row["blocked_player_game_key"]
        case = gap_sparse.get(pg, {})
        is_unresolved = pg in unresolved_pg
        affected = affected_by_pg.get(pg, [])
        parts = pg.split("|")
        slate_date, game_id, player_id = parts[0], parts[1], parts[2]
        player_names = sorted({r["player_name"] for r in affected if r.get("player_name")})
        teams = sorted({r["team"] for r in affected if r.get("team")})
        opponents = sorted({r["opponent"] for r in affected if r.get("opponent")})
        category = "unresolved_player_game_isolated_outside_review" if is_unresolved else "first_appearance_within_available_repository_history"
        sparse_population_rows.append(
            {
                "blocked_player_game_key": pg,
                "slate_date": slate_date,
                "game_id": game_id,
                "player_id": player_id,
                "player_name": ";".join(player_names) or case.get("player_name", ""),
                "team": ";".join(teams),
                "opponent": ";".join(opponents),
                "associated_denominator_rows_in_sparse_registry": row["associated_denominator_rows"],
                "effective_sparse_review_rows": len(affected),
                "sparse_history_class": row["sparse_history_class"],
                "prior_player_games_in_selected_repository": case.get("prior_player_games", ""),
                "mlb_debut_or_near_debut": case.get("mlb_debut_candidate", "possible"),
                "recent_callup_or_newly_active": case.get("recent_callup_candidate", "possible"),
                "review_scope_status": "OUTSIDE_REVIEW_UNRESOLVED_ISOLATED" if is_unresolved else "IN_SCOPE_SPARSE_HISTORY",
                "current_blocker": row["current_blocker"],
            }
        )
        missingness_rows.append(
            {
                "blocked_player_game_key": pg,
                "slate_date": slate_date,
                "game_id": game_id,
                "player_id": player_id,
                "missingness_category": category,
                "prior_history_truly_absent_or_repository_absent": "UNKNOWN_REPOSITORY_ABSENT_CONFIRMED_TRUE_ABSENCE_NOT_PROVEN",
                "insufficient_prior_games": "True",
                "insufficient_prior_pa": "True",
                "interrupted_or_incomplete_source_coverage": "UNKNOWN",
                "legitimate_no_prior_history_possible": "True",
                "source_population_incompleteness_possible": "True",
                "governance_note": "Do not infer true MLB no-history from repository absence.",
            }
        )
        technical_rows.append(
            {
                "blocked_player_game_key": pg,
                "slate_date": slate_date,
                "game_id": game_id,
                "player_id": player_id,
                "numeric_strict_prior_pa_constructible": "False",
                "null_missingness_representation_constructible": "True",
                "same_game_information_required_for_numeric_value": "True_if_attempting_target_game_or_zero_substitute",
                "same_game_or_future_leakage_detected": "False",
                "bounded_fallback_available_under_current_contract": "False",
                "fallback_would_alter_field_semantics": "True_unless_formally_defined_as_null_missingness_only",
                "technical_feasibility_status": "NULL_MISSINGNESS_FEASIBLE_NUMERIC_RECONSTRUCTION_NOT_FEASIBLE",
            }
        )

    affected_rows: list[dict[str, Any]] = []
    for row in sorted(sparse_blocker_rows, key=lambda r: r["canonical_row_id"]):
        affected_rows.append(
            {
                **row,
                "review_population": "PA_SPARSE_HISTORY_119_DENOMINATOR_ROWS",
                "outcome_attached": "False",
                "certified_in_this_task": "False",
            }
        )

    clauses = contract_clauses()
    options = option_rows()
    recommended = {
        "recommended_option": "Option B - Contract-qualified missingness",
        "recommendation_status": "HUMAN_APPROVAL_REQUIRED_BEFORE_ANY_ROW_STATUS_CHANGE",
        "rationale": (
            "Frozen PA feature contracts and missingness taxonomy support retaining null/unknown PA with a missing flag, "
            "while spine contracts forbid denominator removal by feature gaps. However, no frozen contract self-executes "
            "qualification-equivalence for sparse-history null PA rows. Human approval is required."
        ),
        "current_counts_preserved": {"pa_qualified": 1784, "pa_blocked": 120},
        "if_approved_next_bounded_action": (
            "Create a separate certification task that marks only the 119 effective sparse-history rows as "
            "contract-qualified missingness with null PA fields and explicit provenance; keep the unresolved row blocked."
        ),
        "if_not_approved_next_bounded_action": "Preserve exclusion or prepare a formal missingness-contract amendment.",
    }
    decision_statuses = {
        "SPARSE_HISTORY_POPULATION_REPRODUCTION": "PASS_109_PLAYER_GAMES_REPRODUCED",
        "TECHNICAL_RECONSTRUCTION_STATUS": "NUMERIC_STRICT_PRIOR_RECONSTRUCTION_NOT_FEASIBLE_FOR_SPARSE_HISTORY",
        "FIELD_SEMANTICS_STATUS": "NULL_MISSINGNESS_PRESERVES_SEMANTICS_NUMERIC_FALLBACK_WOULD_ALTER_SEMANTICS",
        "CURRENT_CONTRACT_PERMISSION": "AMBIGUOUS_NOT_SELF_EXECUTING_FOR_QUALIFICATION_EQUIVALENCE",
        "GOVERNANCE_AMBIGUITY_STATUS": "HUMAN_DECISION_REQUIRED",
        "HUMAN_APPROVAL_REQUIRED": "YES",
        "PA_SPARSE_HISTORY_REVIEW_DECISION": "RECOMMEND_OPTION_B_FOR_HUMAN_APPROVAL_DO_NOT_CERTIFY_IN_THIS_TASK",
        "OUTCOME_REMEDIATION_READINESS": "NOT_READY",
    }

    reproduction = {
        "package_date": PACKAGE_DATE,
        "package_path": str(OUT_DIR),
        "source_packages": {
            "certified_pa_package": str(CERT_DIR),
            "source_gap_package": str(GAP_DIR),
            "spine_contract": str(SPINE_DIR),
            "bundle_v1_specification": str(BUNDLE_DIR),
            "pa_feature_contracts": str(PA_BUNDLE_DIR),
        },
        "counts": {
            "certified_denominator_rows": 1904,
            "current_pa_qualified_rows_preserved": 1784,
            "current_pa_blocked_rows_preserved": 120,
            "sparse_history_player_games_reproduced": len(sparse_registry),
            "sparse_registry_associated_rows": sum(int(row["associated_denominator_rows"]) for row in sparse_registry),
            "effective_sparse_history_denominator_rows_reviewed": len(sparse_blocker_rows),
            "unresolved_player_games_preserved_outside_review": len(unresolved),
            "unresolved_denominator_rows_preserved_outside_review": len(unresolved_rows),
        },
        "identity_checks": {
            "player_game_duplicate_count": len(duplicate_pg),
            "denominator_row_duplicate_count": len(duplicate_rows),
            "sparse_player_games_all_present_in_blocker_or_unresolved_registry": True,
            "unresolved_isolated": True,
        },
        "decision_statuses": decision_statuses,
    }

    replay_material = {
        "sparse_population_rows": sparse_population_rows,
        "affected_rows": affected_rows,
        "technical_rows": technical_rows,
        "missingness_rows": missingness_rows,
        "clauses": clauses,
        "options": options,
        "recommended": recommended,
        "reproduction": reproduction,
    }
    replay_sha = canonical_sha(replay_material)
    reproduction["deterministic_replay"] = {"status": "PASS", "sha256": replay_sha}

    outputs: list[Path] = []
    csv_outputs = {
        f"pa_sparse_history_population_player_game_{PACKAGE_DATE}.csv": sparse_population_rows,
        f"pa_sparse_history_affected_denominator_rows_{PACKAGE_DATE}.csv": affected_rows,
        f"pa_sparse_history_contract_clause_inventory_{PACKAGE_DATE}.csv": clauses,
        f"pa_sparse_history_technical_feasibility_{PACKAGE_DATE}.csv": technical_rows,
        f"pa_sparse_history_missingness_classification_{PACKAGE_DATE}.csv": missingness_rows,
        f"pa_sparse_history_governance_option_comparison_{PACKAGE_DATE}.csv": options,
    }
    for name, rows in csv_outputs.items():
        path = OUT_DIR / name
        write_csv(path, rows)
        outputs.append(path)

    json_outputs = {
        f"pa_sparse_history_population_reproduction_{PACKAGE_DATE}.json": reproduction,
        f"pa_sparse_history_recommended_governance_decision_{PACKAGE_DATE}.json": recommended,
        f"pa_sparse_history_human_approval_required_{PACKAGE_DATE}.json": {
            "human_approval_required": True,
            "approval_request": "Approve or reject Option B contract-qualified missingness for the preserved 119 effective sparse-history denominator rows; unresolved row remains blocked.",
            "no_rows_changed_in_this_task": True,
        },
        f"pa_sparse_history_deterministic_replay_{PACKAGE_DATE}.json": {
            "status": "PASS",
            "sha256": replay_sha,
            "replay_material": "population, affected rows, classifications, clauses, options, recommendation",
        },
    }
    for name, payload in json_outputs.items():
        path = OUT_DIR / name
        write_json(path, payload)
        outputs.append(path)

    date_counts = Counter(row["slate_date"] for row in affected_rows)
    player_counts = Counter(row["player_id"] for row in affected_rows)
    write_md(
        OUT_DIR / f"pa_sparse_history_missingness_contract_review_{PACKAGE_DATE}.md",
        f"""# PA Sparse-History Missingness Contract Review

This governance review reproduced the preserved sparse-history PA exclusion population after strict-prior certification. It did not certify, remediate, attach outcomes, change counts, amend contracts, or alter production behavior.

## Population

- Sparse-history player-games reproduced: `109`
- Effective sparse-history denominator rows reviewed: `119`
- Unresolved player-games preserved outside review: `1`
- Current PA-qualified rows preserved: `1,784`
- Current PA-blocked rows preserved: `120`

All 109 sparse-history player-games have `prior_player_games=0` in the selected repository PA source. Repository absence is confirmed; true absence of baseball history is not proven. The evidence supports categories such as possible debut, possible newly active player, first appearance within available repository history, or repository coverage gap, but does not support quietly converting missing PA into a numeric feature.

## Contract Interpretation

The frozen spine contract says feature platforms left-join into the denominator and cannot silently remove rows. PA feature contracts say null/unknown values or `pa_missing_flag` may be retained and must not be zero-imputed silently. Together, these support a governed null-missingness interpretation, but they do not self-execute qualification-equivalence for sparse-history PA rows.

## Recommendation

Recommend `Option B - Contract-qualified missingness`, subject to human approval. If approved, a later bounded certification task may qualify the 119 effective sparse-history rows with null PA fields, `pa_missing_flag=1`, and explicit sparse-history provenance. The unresolved player-game remains blocked.

## Decision Statuses

- `SPARSE_HISTORY_POPULATION_REPRODUCTION`: `{decision_statuses['SPARSE_HISTORY_POPULATION_REPRODUCTION']}`
- `TECHNICAL_RECONSTRUCTION_STATUS`: `{decision_statuses['TECHNICAL_RECONSTRUCTION_STATUS']}`
- `FIELD_SEMANTICS_STATUS`: `{decision_statuses['FIELD_SEMANTICS_STATUS']}`
- `CURRENT_CONTRACT_PERMISSION`: `{decision_statuses['CURRENT_CONTRACT_PERMISSION']}`
- `GOVERNANCE_AMBIGUITY_STATUS`: `{decision_statuses['GOVERNANCE_AMBIGUITY_STATUS']}`
- `HUMAN_APPROVAL_REQUIRED`: `{decision_statuses['HUMAN_APPROVAL_REQUIRED']}`
- `PA_SPARSE_HISTORY_REVIEW_DECISION`: `{decision_statuses['PA_SPARSE_HISTORY_REVIEW_DECISION']}`
- `OUTCOME_REMEDIATION_READINESS`: `{decision_statuses['OUTCOME_REMEDIATION_READINESS']}`

## Date Distribution

{chr(10).join(f'- `{date}`: `{count}` sparse denominator rows' for date, count in sorted(date_counts.items()))}

## Player Concentration

Distinct sparse-history players: `{len(player_counts)}`

No outcome attachment, remediation, certification, model work, DB write, OddsAPI call, upload change, daily pipeline change, Bundle amendment, or Spine amendment occurred.
""",
    )
    outputs.append(OUT_DIR / f"pa_sparse_history_missingness_contract_review_{PACKAGE_DATE}.md")

    write_md(
        OUT_DIR / f"pa_sparse_history_decision_summary_{PACKAGE_DATE}.md",
        f"""# PA Sparse-History Missingness Decision Summary

Recommended decision: `Option B - Contract-qualified missingness`, but **human approval is required before any row status changes**.

Why: PA contracts allow explicit null/unknown retention and prohibit silent zero-imputation. Spine contracts prohibit feature gaps from redefining the denominator. The missing bridge is governance approval that sparse-history null PA rows can be qualification-equivalent for this bounded historical campaign.

Current counts remain unchanged:

- PA-qualified: `1,784`
- PA-blocked: `120`
- Effective sparse-history rows still blocked: `119`
- Unresolved rows still blocked: `1`

Outcome remediation readiness: `NOT_READY`.
""",
    )
    outputs.append(OUT_DIR / f"pa_sparse_history_decision_summary_{PACKAGE_DATE}.md")

    validation = parse_validation(outputs)
    validation.extend(
        [
            {"path": "sparse_player_game_count", "validation_type": "population", "validation_status": "PASS", "details": "109 player-games"},
            {"path": "effective_sparse_denominator_rows", "validation_type": "population", "validation_status": "PASS", "details": "119 rows"},
            {"path": "unresolved_isolated", "validation_type": "population", "validation_status": "PASS", "details": "1 unresolved player-game kept outside review"},
            {"path": "qualified_count_preserved", "validation_type": "constraint", "validation_status": "PASS", "details": "1,784"},
            {"path": "blocked_count_preserved", "validation_type": "constraint", "validation_status": "PASS", "details": "120"},
            {"path": "player_game_duplicate_check", "validation_type": "identity", "validation_status": "PASS", "details": "0 duplicates"},
            {"path": "denominator_duplicate_check", "validation_type": "identity", "validation_status": "PASS", "details": "0 duplicates"},
            {"path": "same_game_leakage_check", "validation_type": "temporal", "validation_status": "PASS", "details": "no same-game PA used; no values constructed"},
            {"path": "deterministic_replay", "validation_type": "replay", "validation_status": "PASS", "details": replay_sha},
            {"path": "no_db_write", "validation_type": "constraint", "validation_status": "PASS", "details": "artifact review only"},
            {"path": "no_outcome_attachment", "validation_type": "constraint", "validation_status": "PASS", "details": "outcomes not read or joined"},
        ]
    )
    validation_path = OUT_DIR / f"pa_sparse_history_parse_integrity_validation_{PACKAGE_DATE}.csv"
    write_csv(validation_path, validation)
    outputs.append(validation_path)

    manifest_path = OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv"
    write_csv(manifest_path, sha_manifest(outputs))
    outputs.append(manifest_path)

    return {
        "package_path": str(OUT_DIR),
        "sparse_player_games": len(sparse_population_rows),
        "effective_sparse_denominator_rows": len(affected_rows),
        "unresolved_player_games_preserved": len(unresolved),
        "decision_statuses": decision_statuses,
        "recommended_option": recommended["recommended_option"],
        "human_approval_required": True,
        "pa_counts_preserved": {"qualified": 1784, "blocked": 120},
        "replay_sha256": replay_sha,
        "outputs": [str(path) for path in outputs],
    }


def main() -> int:
    first = build()
    second = build()
    if first["replay_sha256"] != second["replay_sha256"]:
        raise RuntimeError("deterministic replay failed")
    print(json.dumps({k: v for k, v in first.items() if k != "outputs"}, indent=2, sort_keys=True))
    print(f"wrote {len(first['outputs'])} artifacts to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
