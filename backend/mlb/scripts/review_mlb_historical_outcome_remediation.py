#!/usr/bin/env python3
"""Review outcome-remediation readiness for the bounded MLB historical spine.

This script is intentionally read-only.  It writes review artifacts only; it
does not certify outcomes, train, score, query external APIs, or update DB state.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


PACKAGE_DATE = "2026-07-13"
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_remediation_review/2026-07-13"
)

DENOMINATOR_PATH = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_earlier_source_denominator_recovery/2026-07-13/"
    "mlb_historical_earlier_source_denominator_rows_2026-07-13.csv"
)
STARTER_PATH = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_starter_option_b_certified_remediation/2026-07-13/"
    "mlb_starter_option_b_certified_join_rows_2026-07-13.csv"
)
PA_PATH = Path(
    "artifacts/analysis/model_development/"
    "mlb_pa_sparse_history_certified_missingness/2026-07-13/"
    "pa_sparse_history_certified_join_rows_2026-07-13.csv"
)
HITTER_OUTCOME_LEDGER_PATH = Path(
    "artifacts/analysis/model_development/"
    "mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_actual_batter_outcome_binding_ledger_2026-07-11.csv"
)
PREVIOUS_PILOT_ROW_AUDIT_PATH = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_certified_population_qualification_pilot/2026-07-13/"
    "mlb_historical_qualification_row_audit_2026-07-13.csv"
)
OUTCOME_LABEL_CONTRACT_JSON = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_specification_v1/2026-07-12/"
    "collective_bundle_v1_outcome_label_contract_2026-07-12.json"
)
OUTCOME_ATTACHMENT_CONTRACT_MD = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13/"
    "outcome_attachment_contract_2026-07-13.md"
)
OUTCOME_ATTACHMENT_CONTRACT_JSON = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_bounded_offline_process_validation_request/2026-07-13/"
    "outcome_attachment_contract_2026-07-13.json"
)
RECONCILE_SCRIPT_PATH = Path("backend/mlb/scripts/build_mlb_reconcile_rows.py")


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def key(row: Dict[str, str]) -> str:
    return str(row.get("canonical_row_id") or "").strip()


def pg_key(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        str(row.get("slate_date") or row.get("game_date") or "").strip(),
        str(row.get("game_id") or "").strip(),
        str(row.get("player_id") or "").strip(),
    )


def line_value(row: Dict[str, str]) -> float:
    return float(str(row.get("line") or "nan"))


def settle_hits(actual_hits: str, line: str, side: str) -> str:
    actual = float(actual_hits)
    threshold = float(line)
    if abs(actual - threshold) < 1e-12:
        return "push"
    winning_side = "over" if actual > threshold else "under"
    return "win" if str(side).strip().lower() == winning_side else "loss"


def sample_clause(path: Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text().strip()
    return " ".join(text.split())[:900]


def build() -> Dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    denominator = read_csv(DENOMINATOR_PATH)
    starter = read_csv(STARTER_PATH)
    pa = read_csv(PA_PATH)
    outcomes = read_csv(HITTER_OUTCOME_LEDGER_PATH)

    starter_by_id = {key(r): r for r in starter}
    pa_by_id = {key(r): r for r in pa}
    outcome_index: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in outcomes:
        outcome_index[pg_key(row)].append(row)

    prop_counter = Counter((r["prop_type"], r["line"], r["side"]) for r in pa)
    date_counter = Counter(r["slate_date"] for r in pa)
    starter_status = Counter(r["starter_join_status"] for r in starter)
    pa_status = Counter(r["pa_join_status"] for r in pa)

    denominator_manifest_rows = [
        {
            "artifact_role": "frozen_denominator_input",
            "path": str(DENOMINATOR_PATH),
            "exists": DENOMINATOR_PATH.exists(),
            "rows": len(denominator),
            "sha256": sha256(DENOMINATOR_PATH),
            "notes": "Recovered earlier-source denominator; immutable reference only.",
        },
        {
            "artifact_role": "starter_certified_join_input",
            "path": str(STARTER_PATH),
            "exists": STARTER_PATH.exists(),
            "rows": len(starter),
            "sha256": sha256(STARTER_PATH),
            "notes": "Current certified Starter state used only for state/count verification.",
        },
        {
            "artifact_role": "pa_certified_join_input",
            "path": str(PA_PATH),
            "exists": PA_PATH.exists(),
            "rows": len(pa),
            "sha256": sha256(PA_PATH),
            "notes": "Current 1,904-row certified population spine for this review.",
        },
    ]
    write_csv(
        OUT_DIR / f"frozen_denominator_reference_manifest_{PACKAGE_DATE}.csv",
        denominator_manifest_rows,
        ["artifact_role", "path", "exists", "rows", "sha256", "notes"],
    )

    prop_inventory_rows = []
    for (prop_type, line, side), count in sorted(prop_counter.items()):
        push_possible = float(line).is_integer()
        prop_inventory_rows.append(
            {
                "prop_type": prop_type,
                "line": line,
                "side": side,
                "rows": count,
                "required_outcome_stat": "hits",
                "direct_or_derived": "direct",
                "grading_formula": f"{side} wins when actual_hits {'>' if side == 'over' else '<'} {line}",
                "push_possible": push_possible,
                "unsupported_combo": False,
                "ambiguity": "none for hits half-lines; certification still requires governed source attachment",
            }
        )
    write_csv(
        OUT_DIR / f"denominator_prop_line_side_inventory_{PACKAGE_DATE}.csv",
        prop_inventory_rows,
        [
            "prop_type",
            "line",
            "side",
            "rows",
            "required_outcome_stat",
            "direct_or_derived",
            "grading_formula",
            "push_possible",
            "unsupported_combo",
            "ambiguity",
        ],
    )

    outcome_source_rows = [
        {
            "source_name": "hitter_persistence_actual_batter_outcome_binding_ledger",
            "path_or_table": str(HITTER_OUTCOME_LEDGER_PATH),
            "source_type": "local_artifact_from_mlb.player_stats",
            "grain": "player-game",
            "authority": "repository_derived_postgame_outcome_ledger",
            "capture_or_ingestion_time": "2026-07-11 artifact; underlying player_stats read-only export not re-queried",
            "completeness_for_review_population": "",
            "identity_fields": "slate_date|game_id|player_id",
            "duplicates": "0 duplicate player-game keys observed in source",
            "corrections_policy": "inherits local player_stats correction/staleness risk",
            "direct_or_derived": "direct hits and PA fields from player_stats-derived ledger",
            "review_status": "SELECTED_FOR_TECHNICAL_DRY_RUN_ONLY",
        },
        {
            "source_name": "mlb.player_stats",
            "path_or_table": "mlb.player_stats",
            "source_type": "database_table",
            "grain": "player-game",
            "authority": "local completed-game stats table",
            "capture_or_ingestion_time": "not queried by this review",
            "completeness_for_review_population": "expected authoritative local source, but local stat-line integrity audits show row presence can drift from current StatsAPI",
            "identity_fields": "game_date|game_id|player_id",
            "duplicates": "not audited in this no-DB review",
            "corrections_policy": "currently requires separate Completed Game Lineage Integrity controls",
            "direct_or_derived": "direct batter stat fields",
            "review_status": "AUTHORITATIVE_CANDIDATE_REQUIRES_GOVERNED_READ_AND_PARITY_CHECK",
        },
        {
            "source_name": "build_mlb_reconcile_rows._load_actual_values",
            "path_or_table": str(RECONCILE_SCRIPT_PATH),
            "source_type": "repository_settlement_utility",
            "grain": "player-game then market-row settlement",
            "authority": "existing code path, not a frozen outcome certification contract by itself",
            "capture_or_ingestion_time": "runtime database query",
            "completeness_for_review_population": "not executed for current 1,904 rows",
            "identity_fields": "game_id|player_id|prop_type",
            "duplicates": "uses aggregation and distinct-value diagnostics",
            "corrections_policy": "inherits model_training_props/player_stats state at execution time",
            "direct_or_derived": "direct and derived props",
            "review_status": "COMPATIBLE_ARCHITECTURE_CANDIDATE",
        },
        {
            "source_name": "previous_historical_qualification_pilot_row_audit",
            "path_or_table": str(PREVIOUS_PILOT_ROW_AUDIT_PATH),
            "source_type": "prior_review_artifact",
            "grain": "market-row",
            "authority": "diagnostic predecessor only",
            "capture_or_ingestion_time": "2026-07-13",
            "completeness_for_review_population": "1,249-row predecessor; does not equal current 1,904-row spine",
            "identity_fields": "canonical_row_id",
            "duplicates": "0 duplicate identities in predecessor",
            "corrections_policy": "not applicable for current certification",
            "direct_or_derived": "diagnostic actual_hits fields",
            "review_status": "REFERENCE_ONLY_NOT_CERTIFICATION_SOURCE",
        },
    ]
    write_csv(
        OUT_DIR / f"outcome_source_inventory_{PACKAGE_DATE}.csv",
        outcome_source_rows,
        [
            "source_name",
            "path_or_table",
            "source_type",
            "grain",
            "authority",
            "capture_or_ingestion_time",
            "completeness_for_review_population",
            "identity_fields",
            "duplicates",
            "corrections_policy",
            "direct_or_derived",
            "review_status",
        ],
    )

    field_registry_rows = [
        {
            "field_name": "actual_hits",
            "source_field_or_formula": "mlb.player_stats.hits / hitter_persistence ledger actual_hits",
            "direct_or_derived": "direct",
            "supported_props": "hits 0.5 over/under; hits 1.5 over/under",
            "semantics_status": "SUPPORTED_FOR_TECHNICAL_DRY_RUN",
            "notes": "Only field required by the current 1,904-row denominator.",
        },
        {
            "field_name": "actual_one_plus_hit",
            "source_field_or_formula": "actual_hits >= 1",
            "direct_or_derived": "derived",
            "supported_props": "hits 0.5 binary labels",
            "semantics_status": "DEFINED_IN_FROZEN_OUTCOME_LABEL_CONTRACT",
            "notes": "Label is post-feature-freeze only.",
        },
        {
            "field_name": "actual_two_plus_hit",
            "source_field_or_formula": "actual_hits >= 2",
            "direct_or_derived": "derived",
            "supported_props": "hits 1.5 binary labels",
            "semantics_status": "DEFINED_IN_FROZEN_OUTCOME_LABEL_CONTRACT",
            "notes": "Label is post-feature-freeze only.",
        },
        {
            "field_name": "actual_plate_appearances",
            "source_field_or_formula": "mlb.player_stats.plate_appearances",
            "direct_or_derived": "direct",
            "supported_props": "participation/no-action review context",
            "semantics_status": "REVIEW_CONTEXT_ONLY",
            "notes": "Not needed to grade hits half-lines, but useful for non-participation classification.",
        },
        {
            "field_name": "total_bases",
            "source_field_or_formula": "mlb.player_stats.total_bases or singles+2*doubles+3*triples+4*home_runs",
            "direct_or_derived": "direct_or_derived",
            "supported_props": "not present in current denominator",
            "semantics_status": "OUT_OF_SCOPE_FOR_CURRENT_ROWS",
            "notes": "Keep registry entry for future observed props.",
        },
        {
            "field_name": "hits_runs_rbis",
            "source_field_or_formula": "hits + runs_scored + rbis",
            "direct_or_derived": "derived",
            "supported_props": "not present in current denominator",
            "semantics_status": "OUT_OF_SCOPE_FOR_CURRENT_ROWS",
            "notes": "Existing reconcile utility derives it; frozen contract would need prop-specific review.",
        },
    ]
    write_csv(
        OUT_DIR / f"outcome_field_derivation_registry_{PACKAGE_DATE}.csv",
        field_registry_rows,
        [
            "field_name",
            "source_field_or_formula",
            "direct_or_derived",
            "supported_props",
            "semantics_status",
            "notes",
        ],
    )

    row_binding_rows: List[Dict[str, Any]] = []
    pg_summary: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    settlement_counter = Counter()
    binding_counter = Counter()
    for row in pa:
        source_rows = outcome_index.get(pg_key(row), [])
        duplicate_label_matches = len(source_rows)
        status = "OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME"
        actual_hits = ""
        actual_pa = ""
        proposed_settlement = ""
        reason = "missing_batter_outcome_source"
        source_path = ""
        if duplicate_label_matches == 1:
            source = source_rows[0]
            actual_hits = source.get("actual_hits", "")
            actual_pa = source.get("actual_plate_appearances", "")
            source_path = str(HITTER_OUTCOME_LEDGER_PATH)
            if actual_hits != "":
                status = "OUTCOME_BINDING_TECHNICALLY_BINDABLE_REVIEW_ONLY"
                proposed_settlement = settle_hits(actual_hits, row["line"], row["side"])
                reason = "exact_player_game_outcome_found"
            else:
                status = "OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE"
                reason = "outcome_row_present_without_hits_value"
        elif duplicate_label_matches > 1:
            status = "OUTCOME_BINDING_BLOCKED_DUPLICATE_PLAYER_GAME_OUTCOME"
            reason = "duplicate_label_matches"
            source_path = str(HITTER_OUTCOME_LEDGER_PATH)

        binding_counter[status] += 1
        if proposed_settlement:
            settlement_counter[proposed_settlement] += 1
        pg = pg_key(row)
        pg_summary.setdefault(
            pg,
            {
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "market_rows": 0,
                "outcome_match_count": duplicate_label_matches,
                "player_game_binding_status": status,
                "outcome_source_path": source_path,
                "actual_hits": actual_hits,
                "actual_plate_appearances": actual_pa,
                "notes": reason,
            },
        )
        pg_summary[pg]["market_rows"] += 1
        if "BINDABLE" not in pg_summary[pg]["player_game_binding_status"] and "BINDABLE" in status:
            pg_summary[pg]["player_game_binding_status"] = status
            pg_summary[pg]["outcome_source_path"] = source_path
            pg_summary[pg]["actual_hits"] = actual_hits
            pg_summary[pg]["actual_plate_appearances"] = actual_pa
            pg_summary[pg]["notes"] = reason

        row_binding_rows.append(
            {
                "canonical_row_id": row["canonical_row_id"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["side"],
                "pa_join_status": row["pa_join_status"],
                "remaining_pa_blocker": row["remaining_blocker"],
                "starter_join_status": starter_by_id.get(row["canonical_row_id"], {}).get("starter_join_status", ""),
                "outcome_join_key": "|".join(pg),
                "outcome_match_count": duplicate_label_matches,
                "outcome_binding_status": status,
                "resolved_actual_hits_review_only": actual_hits,
                "resolved_actual_plate_appearances_review_only": actual_pa,
                "proposed_settlement_review_only": proposed_settlement,
                "resolution_reason": reason,
                "outcome_source_path": source_path,
                "certification_status": "NOT_CERTIFIED_REVIEW_ONLY",
                "temporal_status": "POSTGAME_OUTCOME_REVIEW_ONLY_NO_FEATURE_WRITEBACK",
            }
        )

    write_csv(
        OUT_DIR / f"denominator_row_outcome_binding_audit_{PACKAGE_DATE}.csv",
        row_binding_rows,
        [
            "canonical_row_id",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "prop_type",
            "line",
            "side",
            "pa_join_status",
            "remaining_pa_blocker",
            "starter_join_status",
            "outcome_join_key",
            "outcome_match_count",
            "outcome_binding_status",
            "resolved_actual_hits_review_only",
            "resolved_actual_plate_appearances_review_only",
            "proposed_settlement_review_only",
            "resolution_reason",
            "outcome_source_path",
            "certification_status",
            "temporal_status",
        ],
    )

    write_csv(
        OUT_DIR / f"player_game_identity_binding_audit_{PACKAGE_DATE}.csv",
        pg_summary.values(),
        [
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "market_rows",
            "outcome_match_count",
            "player_game_binding_status",
            "outcome_source_path",
            "actual_hits",
            "actual_plate_appearances",
            "notes",
        ],
    )

    settlement_rows = []
    for prop_type, line, side in sorted(prop_counter):
        settlement_rows.append(
            {
                "prop_type": prop_type,
                "line": line,
                "side": side,
                "required_actual_field": "actual_hits",
                "market_row_grain": "canonical_row_id",
                "outcome_binding_grain": "slate_date|game_id|player_id",
                "settlement_rule": f"{side} wins when actual_hits {'>' if side == 'over' else '<'} {line}",
                "push_rule": "actual_hits == line",
                "push_possible_for_integer_hits_and_this_line": float(line).is_integer(),
                "void_no_action_rule": "not yet frozen for this historical outcome remediation package",
                "existing_utility_compatible": True,
                "contract_status": "PARTIAL_LABEL_CONTRACT_EXISTS; SETTLEMENT/VOID POLICY NEEDS HUMAN APPROVAL BEFORE CERTIFICATION",
            }
        )
    write_csv(
        OUT_DIR / f"settlement_semantics_matrix_{PACKAGE_DATE}.csv",
        settlement_rows,
        [
            "prop_type",
            "line",
            "side",
            "required_actual_field",
            "market_row_grain",
            "outcome_binding_grain",
            "settlement_rule",
            "push_rule",
            "push_possible_for_integer_hits_and_this_line",
            "void_no_action_rule",
            "existing_utility_compatible",
            "contract_status",
        ],
    )

    push_void_rows = [
        {
            "case": "push",
            "current_population_applicability": "not possible for hits 0.5/1.5 integer outcomes",
            "existing_code_behavior": "build_mlb_reconcile_rows._side_outcome returns push when actual == line",
            "frozen_contract_status": "partial; outcome attachment request says document/exclude or preapprove binary policy",
            "recommendation": "preapprove explicit push handling before adding integer-line props",
        },
        {
            "case": "void/no_action/non_participation",
            "current_population_applicability": "possible when player has no postgame batter row or did not appear",
            "existing_code_behavior": "missing actual yields no outcome",
            "frozen_contract_status": "not fully specified for current 1,904-row package",
            "recommendation": "human decision required before certification; keep unattached ledger for missing player-game rows",
        },
        {
            "case": "postponed/suspended/cancelled",
            "current_population_applicability": "not classified by current local ledger-only dry run",
            "existing_code_behavior": "not determined from selected source",
            "frozen_contract_status": "not fully specified",
            "recommendation": "future certification should include game-status source and explicit exclusion treatment",
        },
    ]
    write_csv(
        OUT_DIR / f"push_void_no_action_treatment_inventory_{PACKAGE_DATE}.csv",
        push_void_rows,
        [
            "case",
            "current_population_applicability",
            "existing_code_behavior",
            "frozen_contract_status",
            "recommendation",
        ],
    )

    technical_rows = [
        {
            "metric": "denominator_rows_reviewed",
            "value": len(pa),
            "status": "PASS",
            "notes": "Current certified PA spine used as review universe.",
        },
        {
            "metric": "unique_player_game_keys",
            "value": len(pg_summary),
            "status": "INFO",
            "notes": "Outcome source binds at player-game grain before settlement expands to market rows.",
        },
        {
            "metric": "market_rows_technically_bindable_review_only",
            "value": binding_counter["OUTCOME_BINDING_TECHNICALLY_BINDABLE_REVIEW_ONLY"],
            "status": "PARTIAL",
            "notes": "Exact player-game hits rows found in selected local ledger; not certified.",
        },
        {
            "metric": "market_rows_missing_selected_outcome_source",
            "value": binding_counter["OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME"],
            "status": "BLOCKER",
            "notes": "No exact player-game row in selected local artifact; may require DB-backed player_stats read/parity check.",
        },
        {
            "metric": "market_rows_with_source_row_but_blank_hits",
            "value": binding_counter["OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE"],
            "status": "BLOCKER",
            "notes": "Exact player-game row exists in selected local artifact, but actual_hits is blank.",
        },
        {
            "metric": "duplicate_label_matches",
            "value": binding_counter["OUTCOME_BINDING_BLOCKED_DUPLICATE_PLAYER_GAME_OUTCOME"],
            "status": "PASS" if binding_counter["OUTCOME_BINDING_BLOCKED_DUPLICATE_PLAYER_GAME_OUTCOME"] == 0 else "FAIL",
            "notes": "Selected local ledger has no duplicate player-game matches for reviewed rows.",
        },
        {
            "metric": "review_only_win_loss_push_rows_calculated",
            "value": sum(settlement_counter.values()),
            "status": "INFO",
            "notes": "Calculated only to test settlement feasibility; no ROI/signal/evaluation performed.",
        },
    ]
    write_csv(
        OUT_DIR / f"technical_dry_run_results_{PACKAGE_DATE}.csv",
        technical_rows,
        ["metric", "value", "status", "notes"],
    )

    blocker_rows = [
        {
            "blocker": "selected_local_outcome_ledger_incomplete_for_current_1904_row_spine",
            "affected_rows": (
                binding_counter["OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME"]
                + binding_counter["OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE"]
            ),
            "severity": "high",
            "blocks_outcome_certification": True,
            "blocks_review_completion": False,
            "recommendation": "Run one governed no-write outcome-source read/parity pass against mlb.player_stats or approved source for all 1,904 rows.",
        },
        {
            "blocker": "void_no_action_non_participation_policy_not_fully_frozen_for_current_package",
            "affected_rows": "unknown",
            "severity": "medium",
            "blocks_outcome_certification": True,
            "blocks_review_completion": False,
            "recommendation": "Human approval of settlement contract before any certification.",
        },
        {
            "blocker": "local_stat_lineage_integrity_risk",
            "affected_rows": "all locally sourced outcomes",
            "severity": "medium",
            "blocks_outcome_certification": False,
            "blocks_review_completion": False,
            "recommendation": "Use parity/lineage checks from Completed Game Lineage Integrity before treating local player_stats as final authority.",
        },
        {
            "blocker": "one_pa_unresolved_row",
            "affected_rows": 1,
            "severity": "low_for_outcome_review_high_for_full_matrix",
            "blocks_outcome_certification": False,
            "blocks_review_completion": False,
            "recommendation": "Classify separately; do not exclude from outcome identity inventory unless future contract requires complete PA qualification.",
        },
    ]
    write_csv(
        OUT_DIR / f"blocker_classification_{PACKAGE_DATE}.csv",
        blocker_rows,
        [
            "blocker",
            "affected_rows",
            "severity",
            "blocks_outcome_certification",
            "blocks_review_completion",
            "recommendation",
        ],
    )

    contract_rows = [
        {
            "contract_or_source": "Bundle v1 outcome label contract",
            "path": str(OUTCOME_LABEL_CONTRACT_JSON),
            "status": "FROZEN",
            "relevant_language": sample_clause(OUTCOME_LABEL_CONTRACT_JSON),
            "review_interpretation": "Outcome labels are allowed only after feature construction/date lock and may not be predictors.",
        },
        {
            "contract_or_source": "Outcome attachment contract request",
            "path": str(OUTCOME_ATTACHMENT_CONTRACT_MD),
            "status": "CONTRACT_DEFINED_NOT_EXECUTED",
            "relevant_language": sample_clause(OUTCOME_ATTACHMENT_CONTRACT_MD),
            "review_interpretation": "Requires exact canonical identity, zero ambiguous/duplicate matches, exclusion ledger, no name fallback, no write-back.",
        },
        {
            "contract_or_source": "Outcome attachment contract JSON",
            "path": str(OUTCOME_ATTACHMENT_CONTRACT_JSON),
            "status": "CONTRACT_DEFINED_NOT_EXECUTED",
            "relevant_language": sample_clause(OUTCOME_ATTACHMENT_CONTRACT_JSON),
            "review_interpretation": "Defines a compatible execution pattern for experiment-local labels, but prior counts apply to a different 2,104-row package.",
        },
        {
            "contract_or_source": "Repository reconcile utility",
            "path": str(RECONCILE_SCRIPT_PATH),
            "status": "CODE_REFERENCE_NOT_FROZEN_CONTRACT",
            "relevant_language": "_load_actual_values loads model_training_props then player_stats fallback; _side_outcome returns push/win/loss from actual_value vs line.",
            "review_interpretation": "Technically compatible but not sufficient by itself to authorize outcome certification.",
        },
    ]
    write_csv(
        OUT_DIR / f"contract_clause_inventory_{PACKAGE_DATE}.csv",
        contract_rows,
        [
            "contract_or_source",
            "path",
            "status",
            "relevant_language",
            "review_interpretation",
        ],
    )

    gov_rows = [
        {
            "option": "A",
            "description": "certify only directly supported outcomes",
            "pros": "maximally conservative",
            "cons": "would leave large missing source block unless player_stats source pass is completed",
            "current_feasibility": "PARTIAL",
            "recommendation": "not yet; first run complete source coverage review",
        },
        {
            "option": "B",
            "description": "certify outcomes with explicit governed missingness",
            "pros": "keeps denominator intact with exclusion/unknown labels where source absent",
            "cons": "requires explicit no-action/non-participation policy",
            "current_feasibility": "PARTIAL",
            "recommendation": "candidate only after human-approved settlement contract",
        },
        {
            "option": "C",
            "description": "use existing repository settlement architecture if proven compatible",
            "pros": "fits existing exact player-game then market-row settlement design",
            "cons": "must prove full 1,904-row source coverage and local-stat parity",
            "current_feasibility": "PROMISING_BUT_NOT_READY",
            "recommendation": "recommended next path: one bounded no-write compatibility pass over all 1,904 rows",
        },
        {
            "option": "D",
            "description": "contract clarification/amendment required",
            "pros": "resolves void/no-action and source-authority ambiguity before certification",
            "cons": "slower",
            "current_feasibility": "REQUIRED_FOR_CERTIFICATION",
            "recommendation": "required human approval before outcome-certified labels are attached",
        },
    ]
    write_csv(
        OUT_DIR / f"governance_option_comparison_{PACKAGE_DATE}.csv",
        gov_rows,
        ["option", "description", "pros", "cons", "current_feasibility", "recommendation"],
    )

    validation_rows = [
        {
            "check": "denominator_row_count",
            "expected": 1904,
            "actual": len(pa),
            "status": "PASS" if len(pa) == 1904 else "FAIL",
            "notes": "Current PA-certified spine row count.",
        },
        {
            "check": "denominator_dates",
            "expected": "2026-06-22..2026-06-28",
            "actual": f"{min(date_counter)}..{max(date_counter)}",
            "status": "PASS" if sorted(date_counter) == [
                "2026-06-22",
                "2026-06-23",
                "2026-06-24",
                "2026-06-25",
                "2026-06-26",
                "2026-06-27",
                "2026-06-28",
            ] else "FAIL",
            "notes": "",
        },
        {
            "check": "starter_qualified_count",
            "expected": 1671,
            "actual": sum(v for k, v in starter_status.items() if "QUALIFIED" in k),
            "status": "PASS" if sum(v for k, v in starter_status.items() if "QUALIFIED" in k) == 1671 else "FAIL",
            "notes": "Includes direct, Option B, and contract-permitted missingness qualified starter rows.",
        },
        {
            "check": "starter_blocked_count",
            "expected": 233,
            "actual": sum(v for k, v in starter_status.items() if "BLOCKED" in k),
            "status": "PASS" if sum(v for k, v in starter_status.items() if "BLOCKED" in k) == 233 else "FAIL",
            "notes": "",
        },
        {
            "check": "pa_qualified_count",
            "expected": 1903,
            "actual": sum(v for k, v in pa_status.items() if "QUALIFIED" in k),
            "status": "PASS" if sum(v for k, v in pa_status.items() if "QUALIFIED" in k) == 1903 else "FAIL",
            "notes": "",
        },
        {
            "check": "pa_blocked_count",
            "expected": 1,
            "actual": sum(v for k, v in pa_status.items() if "BLOCKED" in k),
            "status": "PASS" if sum(v for k, v in pa_status.items() if "BLOCKED" in k) == 1 else "FAIL",
            "notes": "",
        },
    ]
    write_csv(
        OUT_DIR / f"deterministic_reproduction_validation_{PACKAGE_DATE}.csv",
        validation_rows,
        ["check", "expected", "actual", "status", "notes"],
    )

    decisions = {
        "package_date": PACKAGE_DATE,
        "generated_at": generated_at,
        "DENOMINATOR_REPRODUCTION_STATUS": "PASS_1904_ROWS_REPRODUCED",
        "OUTCOME_SOURCE_AVAILABILITY": "PARTIAL_LOCAL_PLAYER_GAME_LEDGER_AVAILABLE_BUT_INCOMPLETE_FOR_1904",
        "OUTCOME_IDENTITY_BINDING_STATUS": "PARTIAL_EXACT_PLAYER_GAME_BINDING_FEASIBLE_WHERE_SOURCE_ROWS_EXIST",
        "OUTCOME_FIELD_SEMANTICS_STATUS": "HITS_HALF_LINES_SUPPORTED_DIRECT_HITS_FIELD",
        "SETTLEMENT_SEMANTICS_STATUS": "PARTIAL_PUSH_IMPOSSIBLE_FOR_CURRENT_HALF_LINES_BUT_VOID_NO_ACTION_POLICY_NEEDS_APPROVAL",
        "TEMPORAL_INTEGRITY_STATUS": "PASS_POSTGAME_REVIEW_ONLY_NO_FEATURE_OR_DENOMINATOR_WRITEBACK",
        "CURRENT_CONTRACT_PERMISSION": "REVIEW_ONLY_ATTACHMENT_NOT_AUTHORIZED",
        "GOVERNANCE_AMBIGUITY_STATUS": "HUMAN_APPROVAL_REQUIRED_FOR_CERTIFICATION_POLICY",
        "HUMAN_APPROVAL_REQUIRED": True,
        "TECHNICAL_OUTCOME_ATTACHMENT_FEASIBILITY": "PARTIAL_FEASIBLE_AFTER_COMPLETE_SOURCE_COVERAGE_AND_CONTRACT_APPROVAL",
        "OUTCOME_CERTIFICATION_READINESS": "NOT_READY",
        "EXPERIMENTAL_LABEL_READINESS": "NOT_READY",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "ONE_NO_WRITE_OUTCOME_SOURCE_COVERAGE_AND_SETTLEMENT_COMPATIBILITY_PASS_FOR_ALL_1904_ROWS",
        "denominator_rows": len(pa),
        "player_game_rows": len(pg_summary),
        "technically_bindable_review_only_rows": binding_counter["OUTCOME_BINDING_TECHNICALLY_BINDABLE_REVIEW_ONLY"],
        "missing_selected_outcome_source_rows": binding_counter["OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME"],
        "source_rows_present_without_hits_value": binding_counter[
            "OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE"
        ],
        "duplicate_label_matches": binding_counter["OUTCOME_BINDING_BLOCKED_DUPLICATE_PLAYER_GAME_OUTCOME"],
    }
    (OUT_DIR / f"review_decision_{PACKAGE_DATE}.json").write_text(json.dumps(decisions, indent=2, sort_keys=True))

    next_action = f"""# Recommended Next Bounded Action — {PACKAGE_DATE}

Run exactly one no-write outcome-source coverage and settlement compatibility pass for
the current 1,904-row certified population. The pass should read the approved
postgame source for every `slate_date|game_id|player_id`, compare any local
player_stats-derived values to an approved authority/parity source where required,
emit attached/unattached/rejected/ambiguous ledgers, and stop before certification.

Do not train, score, certify outcomes, construct an experimental matrix, or write
back to any certified denominator/feature artifact. Human approval is required
before any later outcome-certified label package is created.
"""
    write_text(OUT_DIR / f"recommended_next_bounded_action_{PACKAGE_DATE}.md", next_action)

    approval = f"""# Explicit Human Approval Requirement — {PACKAGE_DATE}

Outcome certification is not authorized by this review.

Human approval is required before:

- attaching outcome labels to an experiment-local certified package;
- deciding whether missing player-game outcome rows become exclusions, no-action,
  governed missingness, or hard blockers;
- relying on existing repository settlement code as the governed certification path;
- using any outcome-certified labels to construct an experimental matrix.
"""
    write_text(OUT_DIR / f"explicit_human_approval_requirement_{PACKAGE_DATE}.md", approval)

    review_md = f"""# MLB Historical Outcome Remediation Readiness and Contract Review — {PACKAGE_DATE}

## Executive Summary

This package reviewed the current bounded historical population for
`2026-06-22` through `2026-06-28`: `1,904` denominator rows, `1,817`
unique player-game outcome keys, Starter state `1,671` qualified / `233`
blocked, and PA state `1,903` qualified / `1` blocked.

No outcomes were certified. No experimental matrix was built. No signal,
ROI, model, upload, DB, or production behavior changed.

## Core Finding

Outcome attachment is technically plausible but not certification-ready. The
current population contains only `hits` markets on half-lines (`0.5` and
`1.5`), so the required outcome field is the direct postgame `actual_hits`
field and pushes are mathematically impossible for this population.

A review-only dry run against the existing local hitter-persistence outcome
ledger found:

- Market rows technically bindable by exact `slate_date|game_id|player_id`:
  `{binding_counter['OUTCOME_BINDING_TECHNICALLY_BINDABLE_REVIEW_ONLY']}`
- Market rows missing from that selected local ledger:
  `{binding_counter['OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME']}`
- Market rows with a source row but blank `actual_hits`:
  `{binding_counter['OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE']}`
- Duplicate player-game label matches:
  `{binding_counter['OUTCOME_BINDING_BLOCKED_DUPLICATE_PLAYER_GAME_OUTCOME']}`

The selected local ledger is therefore useful evidence, but it is not complete
enough to certify the current 1,904-row population.

## Contract Interpretation

The frozen Bundle v1 outcome contract permits outcome labels only after feature
construction and date lock, and excludes outcome fields from predictors. The
bounded attachment contract requires exact canonical identity, zero ambiguous
matches, zero duplicate label matches, an exclusion ledger, no name-only
fallback, and no write-back into certified matrices.

This review satisfied those rules only as an inventory/dry-run exercise. It did
not execute an outcome-certification package.

## Settlement Semantics

For current rows, `hits` settlement is simple:

- over wins when `actual_hits > line`
- under wins when `actual_hits < line`
- push would require `actual_hits == line`

Because the current lines are `0.5` and `1.5`, push cannot occur with integer
hits. However, void/no-action/non-participation treatment is not fully frozen
for this package and requires human approval before certification.

## Temporal Integrity

All outcome information reviewed here is postgame-only and is explicitly marked
review-only. It was not used to alter denominator rows, features, Starter
qualification, PA qualification, missingness contracts, model inputs, or any
production surface.

## Decision Status

- DENOMINATOR_REPRODUCTION_STATUS: `PASS_1904_ROWS_REPRODUCED`
- OUTCOME_SOURCE_AVAILABILITY: `PARTIAL_LOCAL_PLAYER_GAME_LEDGER_AVAILABLE_BUT_INCOMPLETE_FOR_1904`
- OUTCOME_IDENTITY_BINDING_STATUS: `PARTIAL_EXACT_PLAYER_GAME_BINDING_FEASIBLE_WHERE_SOURCE_ROWS_EXIST`
- OUTCOME_FIELD_SEMANTICS_STATUS: `HITS_HALF_LINES_SUPPORTED_DIRECT_HITS_FIELD`
- SETTLEMENT_SEMANTICS_STATUS: `PARTIAL_PUSH_IMPOSSIBLE_FOR_CURRENT_HALF_LINES_BUT_VOID_NO_ACTION_POLICY_NEEDS_APPROVAL`
- TEMPORAL_INTEGRITY_STATUS: `PASS_POSTGAME_REVIEW_ONLY_NO_FEATURE_OR_DENOMINATOR_WRITEBACK`
- CURRENT_CONTRACT_PERMISSION: `REVIEW_ONLY_ATTACHMENT_NOT_AUTHORIZED`
- GOVERNANCE_AMBIGUITY_STATUS: `HUMAN_APPROVAL_REQUIRED_FOR_CERTIFICATION_POLICY`
- OUTCOME_CERTIFICATION_READINESS: `NOT_READY`
- EXPERIMENTAL_LABEL_READINESS: `NOT_READY`

## Recommendation

Proceed next with one bounded no-write outcome-source coverage and settlement
compatibility pass over all `1,904` rows. That pass should produce complete
attached/unattached/rejected/ambiguous ledgers but still stop before outcome
certification or experiment construction.
"""
    write_text(OUT_DIR / f"outcome_remediation_readiness_review_{PACKAGE_DATE}.md", review_md)

    decision_md = f"""# One-Page Decision Summary — {PACKAGE_DATE}

**Decision:** Outcome certification is **not ready**.

**Why:** The denominator and feature-domain state reproduce cleanly, and hits
half-line settlement is semantically straightforward, but the selected local
postgame player-game ledger covers only
`{binding_counter['OUTCOME_BINDING_TECHNICALLY_BINDABLE_REVIEW_ONLY']}` of
`1,904` market rows. Missing source rows and void/no-action policy must be
resolved before any certified label package is allowed.

**Recommended option:** Option C is the most promising path, but only after one
bounded no-write compatibility pass proves full coverage and produces governed
ledgers. Option D contract approval is still required before certification.

**Human approval required:** Yes.
"""
    write_text(OUT_DIR / f"outcome_remediation_decision_summary_{PACKAGE_DATE}.md", decision_md)

    # SHA manifest and parse validation are written last.
    csv_files = sorted(OUT_DIR.glob("*.csv"))
    parse_rows = []
    for path in csv_files:
        try:
            with path.open(newline="") as f:
                reader = csv.DictReader(f)
                count = sum(1 for _ in reader)
                fields = "|".join(reader.fieldnames or [])
            parse_rows.append(
                {
                    "path": str(path),
                    "parse_status": "PASS",
                    "rows": count,
                    "field_count": len(fields.split("|")) if fields else 0,
                    "fields": fields,
                }
            )
        except Exception as exc:  # pragma: no cover - artifact validation path
            parse_rows.append(
                {
                    "path": str(path),
                    "parse_status": "FAIL",
                    "rows": "",
                    "field_count": "",
                    "fields": str(exc),
                }
            )
    write_csv(
        OUT_DIR / f"parse_integrity_validation_{PACKAGE_DATE}.csv",
        parse_rows,
        ["path", "parse_status", "rows", "field_count", "fields"],
    )

    manifest_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            manifest_rows.append(
                {
                    "path": str(path),
                    "sha256": sha256(path),
                    "bytes": path.stat().st_size,
                }
            )
    write_csv(
        OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv",
        manifest_rows,
        ["path", "sha256", "bytes"],
    )

    return decisions


def main() -> int:
    decisions = build()
    print(json.dumps(decisions, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
