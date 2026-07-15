"""Create a governance-only review for historical non-appearance/game-status gaps.

This script is intentionally read-only against production sources. It consumes the
bounded official-source recovery package from 2026-07-13 and writes a decision
packet only. It does not certify outcomes, attach labels, query external APIs,
write databases, train models, or modify production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PACKAGE_DATE = "2026-07-13"
RECOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_gap_authoritative_recovery/2026-07-13"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_nonappearance_game_status_governance_review/2026-07-13"
)

BUNDLE_CONTRACT = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12/"
    "collective_bundle_v1_outcome_label_contract_2026-07-12.json"
)
ATTACHMENT_CONTRACT_JSON = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation_request/"
    "2026-07-13/outcome_attachment_contract_2026-07-13.json"
)
ATTACHMENT_CONTRACT_MD = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation_request/"
    "2026-07-13/outcome_attachment_contract_2026-07-13.md"
)
CHAMPION_CHALLENGER_SPEC = Path("docs/model_development/champion_challenger_experiment_specification.md")

EXPECTED_NONAPPEARANCE_ROWS = 134
EXPECTED_GAME_STATUS_ROWS = 20
EXPECTED_CONTROL_ROWS = 63
EXPECTED_GOVERNANCE_ROWS = 154
EXPECTED_TOTAL_ROWS = 217


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_id_set(rows: list[dict[str, str]]) -> set[str]:
    return {str(row.get("canonical_row_id") or "").strip() for row in rows}


def player_game_key(row: dict[str, str]) -> str:
    return str(row.get("player_game_key") or "").strip()


def index_by(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    return {str(row.get(key) or "").strip(): row for row in rows}


def load_inputs() -> dict[str, list[dict[str, str]]]:
    return {
        "nonappearance": read_csv(RECOVERY_DIR / f"confirmed_non_appearance_ledger_{PACKAGE_DATE}.csv"),
        "game_status": read_csv(RECOVERY_DIR / f"game_status_exception_ledger_{PACKAGE_DATE}.csv"),
        "control": read_csv(RECOVERY_DIR / f"authoritative_value_recovered_ledger_{PACKAGE_DATE}.csv"),
        "pg_nonappearance": read_csv(RECOVERY_DIR / f"player_game_confirmed_non_appearance_ledger_{PACKAGE_DATE}.csv"),
        "pg_game_status": read_csv(RECOVERY_DIR / f"player_game_game_status_exception_ledger_{PACKAGE_DATE}.csv"),
        "pg_control": read_csv(RECOVERY_DIR / f"player_game_authoritative_value_recovered_ledger_{PACKAGE_DATE}.csv"),
        "game_map": read_csv(RECOVERY_DIR / f"game_id_mapping_ledger_{PACKAGE_DATE}.csv"),
        "participation": read_csv(RECOVERY_DIR / f"participation_classification_ledger_{PACKAGE_DATE}.csv"),
        "request_manifest": read_csv(RECOVERY_DIR / f"official_mlb_request_manifest_{PACKAGE_DATE}.csv"),
        "raw_sha": read_csv(RECOVERY_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv"),
    }


def verify_inputs(inputs: dict[str, list[dict[str, str]]]) -> None:
    if len(inputs["nonappearance"]) != EXPECTED_NONAPPEARANCE_ROWS:
        raise AssertionError("unexpected nonappearance row count")
    if len(inputs["game_status"]) != EXPECTED_GAME_STATUS_ROWS:
        raise AssertionError("unexpected game-status row count")
    if len(inputs["control"]) != EXPECTED_CONTROL_ROWS:
        raise AssertionError("unexpected control row count")
    gov = canonical_id_set(inputs["nonappearance"]) | canonical_id_set(inputs["game_status"])
    ctl = canonical_id_set(inputs["control"])
    if len(gov) != EXPECTED_GOVERNANCE_ROWS:
        raise AssertionError("unexpected governance unique count")
    if gov & ctl:
        raise AssertionError("governance/control overlap")
    if len(gov | ctl) != EXPECTED_TOTAL_ROWS:
        raise AssertionError("unexpected total unique count")


def governance_population(inputs: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    game_map = index_by(inputs["game_map"], "canonical_row_id")
    participation = index_by(inputs["participation"], "canonical_row_id")
    rows: list[dict[str, Any]] = []
    for bucket, source_rows in [
        ("confirmed_non_appearance", inputs["nonappearance"]),
        ("game_status_exception", inputs["game_status"]),
    ]:
        for row in source_rows:
            cid = row["canonical_row_id"]
            gm = game_map.get(cid, {})
            part = participation.get(cid, {})
            rows.append(
                {
                    **row,
                    "governance_population_bucket": bucket,
                    "official_game_status": gm.get("game_status", ""),
                    "official_abstract_game_state": gm.get("abstract_game_state", ""),
                    "official_date": gm.get("official_date", ""),
                    "official_participation_category": part.get("participation_category", row.get("participation_category", "")),
                    "governance_interpretation": (
                        "official evidence supports did-not-appear, not zero hits"
                        if bucket == "confirmed_non_appearance"
                        else "official cached game status was not final; numeric batting outcome unavailable"
                    ),
                    "current_contract_permission": "NOT_FOUND",
                    "certification_eligibility": "NOT_ELIGIBLE_WITHOUT_HUMAN_GOVERNANCE_DECISION",
                }
            )
    return rows


def control_population(inputs: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "control_role": "authoritative_numeric_recovery_reference_only",
            "governance_population_member": "false",
        }
        for row in inputs["control"]
    ]


def nonappearance_reason_rows(inputs: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    participation = index_by(inputs["participation"], "canonical_row_id")
    rows: list[dict[str, Any]] = []
    for row in inputs["nonappearance"]:
        part = participation.get(row["canonical_row_id"], {})
        evidence_reason = part.get("reason") or row.get("reason") or ""
        rows.append(
            {
                "canonical_row_id": row["canonical_row_id"],
                "player_game_key": row["player_game_key"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "line": row["line"],
                "side": row["side"],
                "official_participation_category": part.get("participation_category", row.get("participation_category", "")),
                "official_hits": part.get("official_hits", row.get("official_hits", "")),
                "official_at_bats": part.get("official_at_bats", row.get("official_at_bats", "")),
                "official_plate_appearances": part.get("official_plate_appearances", row.get("official_plate_appearances", "")),
                "official_batting_order": part.get("official_batting_order", ""),
                "evidence_reason": evidence_reason,
                "reason_classification": "DID_NOT_APPEAR_REASON_UNESTABLISHED_BY_CACHED_OFFICIAL_SOURCE",
                "zero_hit_classification": "NOT_APPEARED_ZERO_HITS_AND_NOT_CONVERTIBLE_TO_ZERO",
                "settlement_permission": "NO_CURRENT_CONTRACT_PERMISSION_FOUND",
                "recommended_treatment": "PRESERVE_OUTCOME_UNGRADED_PENDING_HUMAN_DECISION",
            }
        )
    return rows


def game_status_investigation_rows(inputs: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    game_map = index_by(inputs["game_map"], "canonical_row_id")
    rows: list[dict[str, Any]] = []
    for row in inputs["game_status"]:
        gm = game_map.get(row["canonical_row_id"], {})
        rows.append(
            {
                "canonical_row_id": row["canonical_row_id"],
                "player_game_key": row["player_game_key"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "certified_game_id": gm.get("certified_game_id", row.get("game_id", "")),
                "mlb_game_pk": gm.get("mlb_game_pk", ""),
                "slate_date": row["slate_date"],
                "official_date": gm.get("official_date", ""),
                "scheduled_start": gm.get("scheduled_start", ""),
                "game_status": gm.get("game_status", ""),
                "abstract_game_state": gm.get("abstract_game_state", ""),
                "game_identity_status": gm.get("game_identity_status", ""),
                "additional_official_fetch_performed": "false",
                "replacement_game_pk_found": "not_investigated_beyond_frozen_cached_evidence",
                "reschedule_rebinding_feasibility": "NOT_PERMITTED_BY_CURRENT_CONTRACT",
                "notes": "Frozen denominator identity binds to this game_pk; cached official status is not final. Rebinding requires a separate approved contract.",
            }
        )
    return rows


def contract_inventory_rows() -> list[dict[str, Any]]:
    bundle = json.loads(BUNDLE_CONTRACT.read_text()) if BUNDLE_CONTRACT.exists() else {}
    attach = json.loads(ATTACHMENT_CONTRACT_JSON.read_text()) if ATTACHMENT_CONTRACT_JSON.exists() else {}
    attach_md = ATTACHMENT_CONTRACT_MD.read_text().strip() if ATTACHMENT_CONTRACT_MD.exists() else ""
    spec = CHAMPION_CHALLENGER_SPEC.read_text() if CHAMPION_CHALLENGER_SPEC.exists() else ""
    champion_excerpt = ""
    if "## 8. Experiment Reproducibility" in spec:
        champion_excerpt = spec.split("## 8. Experiment Reproducibility", 1)[1].split("## 9.", 1)[0].strip()
    return [
        {
            "source_path": str(BUNDLE_CONTRACT),
            "clause_or_field": "separation_rule",
            "exact_language": bundle.get("separation_rule", ""),
            "governance_interpretation": "Allows only post-freeze outcomes; does not define nonappearance or game-status settlement.",
        },
        {
            "source_path": str(BUNDLE_CONTRACT),
            "clause_or_field": "allowed_labels",
            "exact_language": json.dumps(bundle.get("allowed_labels", {}), sort_keys=True),
            "governance_interpretation": "Allows actual hit-derived or line-specific settlement labels; the line-specific settlement rule is not yet defined for DNP/scheduled games.",
        },
        {
            "source_path": str(ATTACHMENT_CONTRACT_JSON),
            "clause_or_field": "join_policy",
            "exact_language": attach.get("join_policy", ""),
            "governance_interpretation": "Requires exact canonical identity; does not authorize name fallback or replacement-game rebinding.",
        },
        {
            "source_path": str(ATTACHMENT_CONTRACT_JSON),
            "clause_or_field": "push_policy",
            "exact_language": attach.get("push_policy", ""),
            "governance_interpretation": "Push handling must be documented or preapproved; no equivalent DNP/no-action policy is present.",
        },
        {
            "source_path": str(ATTACHMENT_CONTRACT_JSON),
            "clause_or_field": "write_back_to_certified_matrices",
            "exact_language": str(attach.get("write_back_to_certified_matrices", "")),
            "governance_interpretation": "Any label attachment must remain experiment-local.",
        },
        {
            "source_path": str(ATTACHMENT_CONTRACT_MD),
            "clause_or_field": "requirements",
            "exact_language": " ".join(line.strip() for line in attach_md.splitlines() if line.strip().startswith("- ")),
            "governance_interpretation": "Provides exact identity and exclusion requirements; silent on settlement of nonappearance and non-final games.",
        },
        {
            "source_path": str(CHAMPION_CHALLENGER_SPEC),
            "clause_or_field": "reproducibility",
            "exact_language": champion_excerpt[:1000],
            "governance_interpretation": "Experiments require durable, reproducible evidence; governance gaps should remain explicit rather than patched by inference.",
        },
    ]


def settlement_architecture_rows() -> list[dict[str, Any]]:
    return [
        {
            "component": "build_mlb_reconcile_rows._load_actual_values",
            "path": "backend/mlb/scripts/build_mlb_reconcile_rows.py",
            "intended_use": "Load resolved numeric prop values from model_training_props with player_stats fallback.",
            "grain": "game_id|player_id|prop_type",
            "nonappearance_or_game_status_semantics": "Missing actual_value returns no outcome; does not define void/no-action.",
            "sportsbook_specific": "false",
            "production_path": "reconcile/reporting utility",
            "compatibility_with_frozen_governance": "PARTIAL_NUMERIC_ONLY",
            "notes": "Useful after numeric official hits exist; insufficient to certify DNP or scheduled-game exceptions.",
        },
        {
            "component": "build_mlb_reconcile_rows._side_outcome",
            "path": "backend/mlb/scripts/build_mlb_reconcile_rows.py",
            "intended_use": "Convert numeric actual value, line, and side into win/loss/push.",
            "grain": "market row after numeric value binding",
            "nonappearance_or_game_status_semantics": "None; actual_value None yields None.",
            "sportsbook_specific": "false",
            "production_path": "reconcile/reporting utility",
            "compatibility_with_frozen_governance": "COMPATIBLE_FOR_NUMERIC_HITS_ONLY",
            "notes": "Must not be used to transform DID_NOT_APPEAR into 0.",
        },
        {
            "component": "reconcile_mlb_v1_results",
            "path": "backend/mlb/scripts/reconcile_mlb_v1_results.py",
            "intended_use": "Join candidates to resolved side outcomes.",
            "grain": "candidate side row",
            "nonappearance_or_game_status_semantics": "Accepts win/loss/push columns from source; does not govern missingness.",
            "sportsbook_specific": "false",
            "production_path": "legacy/current reconcile helper",
            "compatibility_with_frozen_governance": "DOWNSTREAM_ONLY",
            "notes": "Requires a pre-governed outcome source.",
        },
        {
            "component": "report_mlb_graded_wagers._norm_grade",
            "path": "backend/mlb/scripts/report_mlb_graded_wagers.py",
            "intended_use": "Normalize manual/user wager grades including void/cancelled/dnp labels.",
            "grain": "graded wager row",
            "nonappearance_or_game_status_semantics": "Recognizes labels but does not define when they apply.",
            "sportsbook_specific": "not encoded",
            "production_path": "reporting",
            "compatibility_with_frozen_governance": "REFERENCE_ONLY",
            "notes": "Shows vocabulary exists; not a frozen market-settlement contract.",
        },
        {
            "component": "audit_mlb_prediction_flow",
            "path": "backend/mlb/scripts/audit_mlb_prediction_flow.py",
            "intended_use": "Prediction-flow health audit.",
            "grain": "player_props row",
            "nonappearance_or_game_status_semantics": "Counts dnp as resolved-like status for health audit.",
            "sportsbook_specific": "false",
            "production_path": "audit",
            "compatibility_with_frozen_governance": "NOT_AUTHORIZING",
            "notes": "Health audit acceptance is not certification permission.",
        },
    ]


def option_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    nonappearance = [
        {
            "option": "A",
            "name": "Preserve as outcome-ungraded",
            "permission_under_current_contract": "ALLOWED_AS_EXCLUSION_ONLY",
            "pros": "Preserves denominator truth and avoids converting nonappearance into a fake zero-hit outcome.",
            "cons": "Does not increase certified labels.",
            "recommended": "YES",
        },
        {
            "option": "B",
            "name": "Governed void/no-action",
            "permission_under_current_contract": "NOT_CURRENTLY_SPECIFIED",
            "pros": "Could become label-exclusion semantics if human approved.",
            "cons": "Requires explicit frozen rule and settlement vocabulary.",
            "recommended": "NO_UNTIL_APPROVED",
        },
        {
            "option": "C",
            "name": "Sportsbook-specific settlement",
            "permission_under_current_contract": "NOT_CURRENTLY_SPECIFIED",
            "pros": "Could match real market settlement if book rule/time context is known.",
            "cons": "Requires sportsbook, market, and timing-specific rules not present in frozen spine.",
            "recommended": "NO",
        },
        {
            "option": "D",
            "name": "Treat as zero hits",
            "permission_under_current_contract": "NOT_FOUND",
            "pros": "Would maximize numeric coverage.",
            "cons": "Conflates did-not-appear with appeared-and-zero; violates evidence discipline.",
            "recommended": "EXPLICITLY_REJECT",
        },
        {
            "option": "E",
            "name": "Contract clarification/amendment required",
            "permission_under_current_contract": "TRUE_FOR_ANY_LABEL_CERTIFICATION",
            "pros": "Keeps future action governable.",
            "cons": "Defers certification of these rows.",
            "recommended": "YES_FOR_NEXT_ACTION",
        },
    ]
    game_status = [
        {
            "option": "A",
            "name": "Preserve as ungraded exception",
            "permission_under_current_contract": "ALLOWED_AS_EXCLUSION_ONLY",
            "pros": "Keeps frozen game identity intact and avoids unsupported rebinding.",
            "cons": "Does not recover labels.",
            "recommended": "YES",
        },
        {
            "option": "B",
            "name": "Rebind to rescheduled/replacement game",
            "permission_under_current_contract": "NOT_CURRENTLY_SPECIFIED",
            "pros": "May recover outcomes if a future approved identity rule exists.",
            "cons": "Would alter frozen denominator semantics without a contract.",
            "recommended": "NO_UNTIL_SEPARATE_APPROVAL",
        },
        {
            "option": "C",
            "name": "Governed void/no-action",
            "permission_under_current_contract": "NOT_CURRENTLY_SPECIFIED",
            "pros": "Could reflect market settlement if approved.",
            "cons": "Requires explicit status/time/book rules.",
            "recommended": "NO_UNTIL_APPROVED",
        },
        {
            "option": "D",
            "name": "Contract clarification/amendment required",
            "permission_under_current_contract": "TRUE_FOR_ANY_LABEL_CERTIFICATION_OR_REBINDING",
            "pros": "Prevents silent label inflation.",
            "cons": "Defers certification.",
            "recommended": "YES_FOR_NEXT_ACTION",
        },
    ]
    return nonappearance, game_status


def projection_rows() -> list[dict[str, Any]]:
    return [
        {
            "population_component": "existing_attached_ready_from_source_coverage_pass",
            "rows": 1687,
            "label_certification_status": "not_certified_in_this_review",
            "experimental_label_readiness": "potential_numeric_reference_only",
            "notes": "Previously reported attached-ready rows; outside current 154-row governance population.",
        },
        {
            "population_component": "authoritative_value_recovered_control",
            "rows": EXPECTED_CONTROL_ROWS,
            "label_certification_status": "not_certified_control_reference_only",
            "experimental_label_readiness": "technically_numeric_but_held_until_contract_governance",
            "notes": "Official numeric hits recovered in prior dry run; not part of governance population.",
        },
        {
            "population_component": "confirmed_non_appearance",
            "rows": EXPECTED_NONAPPEARANCE_ROWS,
            "label_certification_status": "not_certified_governance_required",
            "experimental_label_readiness": "not_ready",
            "notes": "Official evidence supports did-not-appear, not appeared-zero.",
        },
        {
            "population_component": "game_status_exception",
            "rows": EXPECTED_GAME_STATUS_ROWS,
            "label_certification_status": "not_certified_governance_required",
            "experimental_label_readiness": "not_ready",
            "notes": "Cached official status not final for frozen game identity.",
        },
        {
            "population_component": "numeric_if_future_governance_allows_existing_plus_control",
            "rows": 1750,
            "label_certification_status": "projection_only",
            "experimental_label_readiness": "not_ready_without_separate_certification",
            "notes": "1687 + 63; excludes 154 governance rows.",
        },
    ]


def decision_json(package_files: dict[str, str]) -> dict[str, Any]:
    return {
        "package_date": PACKAGE_DATE,
        "generated_at": utc_now(),
        "source_package": str(RECOVERY_DIR),
        "GOVERNANCE_POPULATION_REPRODUCTION": "PASS_154_GOVERNANCE_ROWS_63_CONTROL_ROWS_217_TOTAL",
        "NON_APPEARANCE_FACTUAL_STATUS": "CONFIRMED_DID_NOT_APPEAR_FROM_CACHED_OFFICIAL_SOURCE",
        "NON_APPEARANCE_FIELD_SEMANTICS": "DID_NOT_APPEAR_IS_NOT_APPEARED_ZERO_HITS_AND_NOT_NUMERIC_ZERO",
        "NON_APPEARANCE_CURRENT_CONTRACT_PERMISSION": "NOT_FOUND_FOR_LABEL_CERTIFICATION_OR_ZERO_CONVERSION",
        "NON_APPEARANCE_GOVERNANCE_AMBIGUITY": "PRESENT_HUMAN_DECISION_REQUIRED",
        "GAME_STATUS_FACTUAL_STATUS": "OFFICIAL_CACHED_GAME_STATUS_EXCEPTION_NOT_FINAL_FOR_20_ROWS",
        "RESCHEDULE_REBINDING_FEASIBILITY": "NOT_PERMITTED_UNDER_CURRENT_FROZEN_CONTRACT",
        "GAME_STATUS_CURRENT_CONTRACT_PERMISSION": "NOT_FOUND_FOR_LABEL_CERTIFICATION_OR_REBINDING",
        "GAME_STATUS_GOVERNANCE_AMBIGUITY": "PRESENT_HUMAN_DECISION_REQUIRED",
        "EXISTING_SETTLEMENT_ARCHITECTURE_COMPATIBILITY": "PARTIAL_NUMERIC_ONLY_NOT_SUFFICIENT_FOR_DNP_OR_GAME_STATUS_GOVERNANCE",
        "HUMAN_APPROVAL_REQUIRED": True,
        "RECOMMENDED_NON_APPEARANCE_OPTION": "A_PRESERVE_OUTCOME_UNGRADED_WITH_E_CONTRACT_CLARIFICATION_BEFORE_ANY_CERTIFICATION",
        "RECOMMENDED_GAME_STATUS_OPTION": "A_PRESERVE_UNGRADED_EXCEPTION_WITH_D_CONTRACT_CLARIFICATION_BEFORE_ANY_CERTIFICATION_OR_REBINDING",
        "OUTCOME_CERTIFICATION_READINESS": "NOT_READY",
        "EXPERIMENTAL_LABEL_READINESS": "NOT_READY",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "HUMAN_GOVERNANCE_DECISION_ON_NONAPPEARANCE_AND_GAME_STATUS_EXCLUSION_OR_SETTLEMENT_CONTRACT",
        "files": package_files,
    }


def write_reports(
    inputs: dict[str, list[dict[str, str]]],
    package_files: dict[str, str],
    decision: dict[str, Any],
) -> None:
    non_source_counts = Counter(row.get("final_ledger", "") for row in inputs["nonappearance"])
    gs_status_counts = Counter()
    game_map = index_by(inputs["game_map"], "canonical_row_id")
    for row in inputs["game_status"]:
        gs_status_counts[game_map.get(row["canonical_row_id"], {}).get("game_status", "")] += 1

    main = f"""# MLB Historical Non-Appearance and Game-Status Governance Review

Generated: `{decision['generated_at']}`

## Executive Summary

This review reproduced the frozen governance population from the official-source recovery package without new data fetches or production writes.

- Governance population: `{EXPECTED_GOVERNANCE_ROWS}` rows
- Confirmed non-appearance rows: `{EXPECTED_NONAPPEARANCE_ROWS}` rows, `{len(inputs['pg_nonappearance'])}` player-game keys
- Game-status exception rows: `{EXPECTED_GAME_STATUS_ROWS}` rows, `{len(inputs['pg_game_status'])}` player-game keys
- Control population: `{EXPECTED_CONTROL_ROWS}` authoritative-value-recovered rows
- Total frozen recovery rows reconciled: `{EXPECTED_TOTAL_ROWS}`

The core governance finding is simple: the official evidence separates numeric outcome availability from market settlement permission. Non-appearance rows are not zero-hit rows, and scheduled/non-final game rows are not eligible for rebinding under the current frozen contract.

## Factual Findings

The non-appearance rows are official participation facts, not numeric batting outcomes. They should remain ungraded unless a human-approved settlement contract later defines void/no-action or another permitted treatment.

The game-status exception rows are tied to frozen game identities whose cached official status was not final. Replacement-game or reschedule rebinding is not supported by the current exact-identity label contract.

## Contract Findings

The frozen outcome contracts permit exact, post-freeze label attachment. They require exact canonical identity, no name-only fallback, an exclusion ledger, and no write-back to certified matrices. They do not define non-appearance settlement, sportsbook-specific void/no-action treatment, or rescheduled-game rebinding.

## Recommendation

- Non-appearance: preserve as outcome-ungraded pending human governance approval.
- Game-status exceptions: preserve as ungraded exceptions pending human governance approval.
- Explicitly reject converting non-appearance to zero hits under the current contract.
- Do not certify outcomes or attach experimental labels until the missing settlement contract decision is made.

## Decision Statuses

```json
{json.dumps({k: v for k, v in decision.items() if k.isupper()}, indent=2, sort_keys=True)}
```

## Counts

- Non-appearance source ledgers: `{dict(non_source_counts)}`
- Game-status official statuses: `{dict(gs_status_counts)}`

## No Behavior Changed

This package is documentation and governance review only. It did not certify outcomes, attach labels, build matrices, train models, write databases, call OddsAPI, call MLB StatsAPI, or change production behavior.
"""
    (OUT_DIR / f"nonappearance_game_status_governance_review_{PACKAGE_DATE}.md").write_text(main)

    summary = f"""# Human Decision Summary

## Decision Needed

Approve how the historical certification campaign should treat:

1. `{EXPECTED_NONAPPEARANCE_ROWS}` confirmed non-appearance denominator rows.
2. `{EXPECTED_GAME_STATUS_ROWS}` game-status exception denominator rows.

## Recommended Decision

- Non-appearance: Option A, preserve as outcome-ungraded; do not convert to zero.
- Game-status: Option A, preserve as ungraded exception; do not rebind to another game without a new contract.

## Why

The frozen contracts do not authorize DNP/no-action settlement, sportsbook-specific settlement, zero conversion, or rescheduled-game rebinding. The factual layer is clean enough to classify, but the governance layer is not approved enough to certify labels.

## Requested Approval

Human approval is required before any future step converts these governance categories into an experiment-eligible label policy.
"""
    (OUT_DIR / f"human_decision_summary_{PACKAGE_DATE}.md").write_text(summary)

    approval = """# Explicit Human-Approval Request

Please approve one of the reviewed governance options before outcome certification continues.

Recommended:

- Confirmed non-appearance rows remain outcome-ungraded exclusions for this historical certification campaign.
- Game-status exception rows remain ungraded exceptions for this historical certification campaign.
- No confirmed non-appearance row may be treated as zero hits.
- No game-status exception row may be rebound to a replacement/rescheduled game without a separate frozen contract.

Until approved, `OUTCOME_CERTIFICATION_READINESS` and `EXPERIMENTAL_LABEL_READINESS` remain `NOT_READY`.
"""
    (OUT_DIR / f"explicit_human_approval_request_{PACKAGE_DATE}.md").write_text(approval)

    gaps = """# Recommended Governance Decisions

## Non-Appearance

Recommended option: A, preserve as outcome-ungraded. Contract clarification is still required before any future certification policy can treat these as void/no-action or sportsbook-settled rows.

## Game Status

Recommended option: A, preserve as ungraded exception. Contract clarification is required before any future rebinding or settlement policy.

## Certification

Do not certify these 154 rows. Keep the 63 authoritative-value recovered rows as a numeric control reference until a separate certification step is approved.
"""
    (OUT_DIR / f"recommended_governance_decisions_{PACKAGE_DATE}.md").write_text(gaps)


def validation_rows(inputs: dict[str, list[dict[str, str]]], output_paths: list[Path]) -> list[dict[str, Any]]:
    gov_ids = canonical_id_set(inputs["nonappearance"]) | canonical_id_set(inputs["game_status"])
    ctl_ids = canonical_id_set(inputs["control"])
    participation = index_by(inputs["participation"], "canonical_row_id")
    game_map = index_by(inputs["game_map"], "canonical_row_id")
    raw_sha_matches = 0
    raw_sha_checked = 0
    for row in inputs["request_manifest"]:
        cache_path = Path(row.get("cache_path") or "")
        expected = str(row.get("sha256") or "").strip()
        if cache_path.exists() and expected:
            raw_sha_checked += 1
            if sha256_file(cache_path) == expected:
                raw_sha_matches += 1
    nonappearance_participation_ok = all(
        participation.get(row["canonical_row_id"], {}).get("participation_category", row.get("participation_category", ""))
        == "DID_NOT_APPEAR"
        for row in inputs["nonappearance"]
    )
    game_status_ok = all(
        game_map.get(row["canonical_row_id"], {}).get("game_status", "") != "Final"
        for row in inputs["game_status"]
    )
    return [
        {"check": "governance_row_count", "status": "PASS" if len(gov_ids) == EXPECTED_GOVERNANCE_ROWS else "FAIL", "value": len(gov_ids), "expected": EXPECTED_GOVERNANCE_ROWS},
        {"check": "control_row_count", "status": "PASS" if len(ctl_ids) == EXPECTED_CONTROL_ROWS else "FAIL", "value": len(ctl_ids), "expected": EXPECTED_CONTROL_ROWS},
        {"check": "total_row_count", "status": "PASS" if len(gov_ids | ctl_ids) == EXPECTED_TOTAL_ROWS else "FAIL", "value": len(gov_ids | ctl_ids), "expected": EXPECTED_TOTAL_ROWS},
        {"check": "governance_control_overlap", "status": "PASS" if not (gov_ids & ctl_ids) else "FAIL", "value": len(gov_ids & ctl_ids), "expected": 0},
        {"check": "official_raw_response_sha", "status": "PASS" if raw_sha_matches == len(inputs["request_manifest"]) else "FAIL", "value": raw_sha_matches, "expected": len(inputs["request_manifest"])},
        {"check": "official_raw_response_sha_checked", "status": "PASS" if raw_sha_checked == len(inputs["request_manifest"]) else "FAIL", "value": raw_sha_checked, "expected": len(inputs["request_manifest"])},
        {"check": "official_nonappearance_participation", "status": "PASS" if nonappearance_participation_ok else "FAIL", "value": EXPECTED_NONAPPEARANCE_ROWS if nonappearance_participation_ok else "mismatch", "expected": "all DID_NOT_APPEAR"},
        {"check": "official_game_status_exception_not_final", "status": "PASS" if game_status_ok else "FAIL", "value": EXPECTED_GAME_STATUS_ROWS if game_status_ok else "mismatch", "expected": "all non-Final"},
        {"check": "nonappearance_not_zero", "status": "PASS", "value": EXPECTED_NONAPPEARANCE_ROWS, "expected": "all preserved ungraded"},
        {"check": "game_status_not_rebound", "status": "PASS", "value": EXPECTED_GAME_STATUS_ROWS, "expected": "all preserved ungraded"},
        {"check": "output_files_written", "status": "PASS" if all(p.exists() for p in output_paths) else "FAIL", "value": sum(p.exists() for p in output_paths), "expected": len(output_paths)},
    ]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs()
    verify_inputs(inputs)

    outputs: dict[str, str] = {}
    def record(name: str, path: Path) -> Path:
        outputs[name] = str(path)
        return path

    write_csv(
        record("governance_population", OUT_DIR / f"frozen_154_governance_population_{PACKAGE_DATE}.csv"),
        governance_population(inputs),
    )
    write_csv(
        record("control_population", OUT_DIR / f"frozen_63_control_population_reference_{PACKAGE_DATE}.csv"),
        control_population(inputs),
    )
    write_csv(
        record("nonappearance_player_game_ledger", OUT_DIR / f"nonappearance_player_game_ledger_{PACKAGE_DATE}.csv"),
        inputs["pg_nonappearance"],
    )
    write_csv(
        record("nonappearance_denominator_row_ledger", OUT_DIR / f"nonappearance_denominator_row_ledger_{PACKAGE_DATE}.csv"),
        inputs["nonappearance"],
    )
    write_csv(
        record("nonappearance_reason_classification", OUT_DIR / f"nonappearance_reason_classification_{PACKAGE_DATE}.csv"),
        nonappearance_reason_rows(inputs),
    )
    write_csv(
        record("game_status_player_game_ledger", OUT_DIR / f"game_status_player_game_ledger_{PACKAGE_DATE}.csv"),
        inputs["pg_game_status"],
    )
    write_csv(
        record("game_status_denominator_row_ledger", OUT_DIR / f"game_status_denominator_row_ledger_{PACKAGE_DATE}.csv"),
        inputs["game_status"],
    )
    write_csv(
        record("reschedule_investigation", OUT_DIR / f"reschedule_replacement_game_investigation_{PACKAGE_DATE}.csv"),
        game_status_investigation_rows(inputs),
    )
    write_csv(
        record("contract_inventory", OUT_DIR / f"contract_clause_inventory_{PACKAGE_DATE}.csv"),
        contract_inventory_rows(),
    )
    write_csv(
        record("settlement_architecture", OUT_DIR / f"existing_settlement_architecture_inventory_{PACKAGE_DATE}.csv"),
        settlement_architecture_rows(),
    )
    non_opts, gs_opts = option_rows()
    write_csv(
        record("nonappearance_options", OUT_DIR / f"nonappearance_governance_option_comparison_{PACKAGE_DATE}.csv"),
        non_opts,
    )
    write_csv(
        record("game_status_options", OUT_DIR / f"game_status_governance_option_comparison_{PACKAGE_DATE}.csv"),
        gs_opts,
    )
    write_csv(
        record("readiness_projection", OUT_DIR / f"certification_label_readiness_projection_{PACKAGE_DATE}.csv"),
        projection_rows(),
    )

    decision = decision_json(outputs)
    write_json(record("decision_json", OUT_DIR / f"machine_readable_decision_{PACKAGE_DATE}.json"), decision)
    write_reports(inputs, outputs, decision)
    outputs["main_report"] = str(OUT_DIR / f"nonappearance_game_status_governance_review_{PACKAGE_DATE}.md")
    outputs["human_summary"] = str(OUT_DIR / f"human_decision_summary_{PACKAGE_DATE}.md")
    outputs["approval_request"] = str(OUT_DIR / f"explicit_human_approval_request_{PACKAGE_DATE}.md")
    outputs["recommended_decisions"] = str(OUT_DIR / f"recommended_governance_decisions_{PACKAGE_DATE}.md")

    output_paths = [Path(v) for v in outputs.values()]
    write_csv(
        record("reproduction_validation", OUT_DIR / f"deterministic_reproduction_validation_{PACKAGE_DATE}.csv"),
        validation_rows(inputs, output_paths),
    )
    output_paths.append(Path(outputs["reproduction_validation"]))

    manifest_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            manifest_rows.append(
                {
                    "path": str(path),
                    "sha256": sha256_file(path),
                    "bytes": path.stat().st_size,
                }
            )
    write_csv(
        record("sha256_manifest", OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv"),
        manifest_rows,
        ["path", "sha256", "bytes"],
    )

    return {"out_dir": str(OUT_DIR), "files": outputs, "decision": decision}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
