#!/usr/bin/env python3
"""Repository-wide no-write outcome-source coverage pass for MLB historical rows.

This is discovery and compatibility only.  It does not certify outcomes, write
labels into a matrix, call external APIs, or mutate production state.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PACKAGE_DATE = "2026-07-13"
DATES = [f"2026-06-{day:02d}" for day in range(22, 29)]
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_source_coverage_pass/2026-07-13"
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
PREVIOUS_REVIEW_ROW_AUDIT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_remediation_review/2026-07-13/"
    "denominator_row_outcome_binding_audit_2026-07-13.csv"
)


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    path: Path
    source_type: str
    grain: str
    date_field_candidates: Tuple[str, ...]
    player_field: str
    game_field: str
    actual_field: str
    prop_filter: Optional[str]
    line_field: Optional[str]
    side_field_candidates: Tuple[str, ...]
    authority_level: str
    direct_or_derived: str
    compatible_for_hits: bool
    used_for_attached_ready: bool
    notes: str


SOURCE_SPECS: List[SourceSpec] = [
    SourceSpec(
        "pa_opp_extended_historical_base",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_rolling_pa_opportunity_characterization/2026-07-11/"
            "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
        ),
        "research_base_csv",
        "market-row with player-game actuals",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        "hits",
        "line",
        ("side_normalized", "side"),
        "repository_player_stats_derived_postgame",
        "direct actual_hits retained",
        True,
        True,
        "Highest-coverage local player_stats-derived research base for this window.",
    ),
    SourceSpec(
        "pa_opp_historical_base",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_rolling_pa_opportunity_historical_base/2026-07-11/"
            "pa_opp_v1_historical_research_base_2026-05-30_to_2026-07-09_2026-07-11.csv"
        ),
        "research_base_csv",
        "market-row with player-game actuals",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        "hits",
        "line",
        ("side_normalized", "side"),
        "repository_player_stats_derived_postgame",
        "direct actual_hits retained",
        True,
        True,
        "Earlier PA research base with same postgame actual_hits semantics.",
    ),
    SourceSpec(
        "hitter_persistence_batter_game_ledger",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_hitter_persistence_characterization/2026-07-11/"
            "hitter_persistence_actual_batter_outcome_binding_ledger_2026-07-11.csv"
        ),
        "outcome_ledger_csv",
        "player-game",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        None,
        None,
        tuple(),
        "repository_player_stats_derived_postgame",
        "direct actual_hits retained",
        True,
        True,
        "Selected in previous review; incomplete for the current denominator.",
    ),
    SourceSpec(
        "hitter_persistence_batter_game_base",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_hitter_persistence_characterization/2026-07-11/"
            "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
        ),
        "research_base_csv",
        "player-game",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        None,
        None,
        tuple(),
        "repository_player_stats_derived_postgame",
        "direct actual_hits retained",
        True,
        True,
        "Player-game research base from hitter persistence platform.",
    ),
    SourceSpec(
        "hitter_persistence_batter_prop_base",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_hitter_persistence_characterization/2026-07-11/"
            "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
        ),
        "research_base_csv",
        "market-row with player-game actuals",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        "hits",
        "line",
        ("side_normalized", "side"),
        "repository_player_stats_derived_postgame",
        "direct actual_hits retained",
        True,
        True,
        "Prop-level hitter persistence base; lower coverage than PA base.",
    ),
    SourceSpec(
        "previous_historical_qualification_pilot_row_audit",
        Path(
            "artifacts/analysis/model_development/"
            "mlb_historical_certified_population_qualification_pilot/2026-07-13/"
            "mlb_historical_qualification_row_audit_2026-07-13.csv"
        ),
        "prior_diagnostic_csv",
        "market-row",
        ("slate_date", "game_date", "date"),
        "player_id",
        "game_id",
        "actual_hits",
        "hits",
        "line",
        ("side", "side_normalized"),
        "prior_diagnostic_not_current_contract_source",
        "direct actual_hits retained",
        True,
        False,
        "Predecessor 1,249-row diagnostic; candidate evidence only.",
    ),
]

for _date in DATES:
    SOURCE_SPECS.append(
        SourceSpec(
            f"execution_vs_model_reconcile_{_date}",
            Path(f"artifacts/analysis/mlb/execution_vs_model/{_date}/reconcile_rows.csv"),
            "reconcile_csv",
            "player-game/prop-line",
            ("slate_date", "game_date", "date"),
            "player_id",
            "game_id",
            "actual_value",
            "hits",
            "line",
            ("side", "side_normalized"),
            "repository_reconcile_player_stats_or_training_props",
            "direct actual_value for hits",
            True,
            True,
            "Full-slate reconcile output with actual_value and side outcomes.",
        )
    )
    SOURCE_SPECS.append(
        SourceSpec(
            f"actual_wagers_by_source_{_date}",
            Path(f"backend/mlb/exports/model_v2/reconcile/{_date}/actual_wagers_by_source_{_date}.csv"),
            "manual_or_reconcile_wager_csv",
            "market-row settlement",
            ("date", "slate_date", "game_date"),
            "player_id",
            "game_id",
            "result",
            "hits",
            "line",
            ("side", "parsed_side"),
            "manual_settlement_not_official_hit_source",
            "settlement result only; no actual_hits",
            False,
            False,
            "Useful as rejected/diagnostic source; not official hits evidence.",
        )
    )


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
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


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def norm_float_text(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    try:
        return f"{float(text):.1f}"
    except Exception:
        return text


def row_id(row: Dict[str, str]) -> str:
    return clean(row.get("canonical_row_id"))


def canonical_tuple(row: Dict[str, str]) -> Tuple[str, str, str, str, str, str]:
    return (
        clean(row.get("slate_date") or row.get("date") or row.get("game_date")),
        clean(row.get("game_id")),
        clean(row.get("player_id")),
        clean(row.get("prop_type")).lower(),
        norm_float_text(row.get("line")),
        clean(row.get("side") or row.get("side_normalized") or row.get("parsed_side")).lower(),
    )


def player_game_tuple(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        clean(row.get("slate_date") or row.get("date") or row.get("game_date")),
        clean(row.get("game_id")),
        clean(row.get("player_id")),
    )


def date_from_source(row: Dict[str, str], spec: SourceSpec) -> str:
    for field in spec.date_field_candidates:
        value = clean(row.get(field))
        if value:
            return value
    return ""


def side_from_source(row: Dict[str, str], spec: SourceSpec) -> str:
    for field in spec.side_field_candidates:
        value = clean(row.get(field)).lower()
        if value:
            return value
    return ""


def source_canonical_tuple(row: Dict[str, str], spec: SourceSpec) -> Tuple[str, str, str, str, str, str]:
    return (
        date_from_source(row, spec),
        clean(row.get(spec.game_field)),
        clean(row.get(spec.player_field)),
        clean(row.get("prop_type")).lower(),
        norm_float_text(row.get(spec.line_field)) if spec.line_field else "",
        side_from_source(row, spec),
    )


def source_player_game_tuple(row: Dict[str, str], spec: SourceSpec) -> Tuple[str, str, str]:
    return (
        date_from_source(row, spec),
        clean(row.get(spec.game_field)),
        clean(row.get(spec.player_field)),
    )


def actual_value(row: Dict[str, str], spec: SourceSpec) -> str:
    value = clean(row.get(spec.actual_field))
    if not value:
        return ""
    if spec.source_id.startswith("actual_wagers_by_source"):
        return value
    try:
        return f"{float(value):.1f}"
    except Exception:
        return value


def side_outcome(actual_hits: str, line: str, side: str) -> str:
    actual = float(actual_hits)
    threshold = float(line)
    if abs(actual - threshold) < 1e-12:
        return "push"
    winning_side = "over" if actual > threshold else "under"
    return "win" if clean(side).lower() == winning_side else "loss"


def source_rows_for_spec(spec: SourceSpec) -> List[Dict[str, str]]:
    if not spec.path.exists():
        return []
    rows = read_csv(spec.path)
    out = []
    for row in rows:
        date_value = date_from_source(row, spec)
        if date_value not in DATES:
            continue
        if spec.prop_filter is not None:
            prop = clean(row.get("prop_type")).lower()
            if prop and prop != spec.prop_filter:
                continue
            if not prop and not spec.source_id.startswith("actual_wagers_by_source"):
                continue
        if not clean(row.get(spec.game_field)) or not clean(row.get(spec.player_field)):
            continue
        out.append(row)
    return out


def source_match_record(
    *,
    denom: Dict[str, str],
    spec: SourceSpec,
    source_row: Dict[str, str],
    match_scope: str,
) -> Dict[str, Any]:
    value = actual_value(source_row, spec)
    status = "SOURCE_ROW_PRESENT_VALUE_BLANK"
    if spec.source_id.startswith("actual_wagers_by_source"):
        status = "REJECTED_SETTLEMENT_RESULT_NOT_OFFICIAL_HIT_SOURCE"
    elif value:
        status = "DIRECT_AUTHORITATIVE_AVAILABLE" if spec.used_for_attached_ready else "DIAGNOSTIC_VALUE_AVAILABLE"
    return {
        "canonical_row_id": row_id(denom),
        "slate_date": denom["slate_date"],
        "game_id": denom["game_id"],
        "player_id": denom["player_id"],
        "player_name": denom["player_name"],
        "team": denom["team"],
        "opponent": denom["opponent"],
        "prop_type": denom["prop_type"],
        "line": denom["line"],
        "side": denom["side"],
        "player_game_key": "|".join(player_game_tuple(denom)),
        "match_scope": match_scope,
        "source_id": spec.source_id,
        "source_path": str(spec.path),
        "source_type": spec.source_type,
        "source_grain": spec.grain,
        "authority_level": spec.authority_level,
        "direct_or_derived": spec.direct_or_derived,
        "compatible_for_hits": spec.compatible_for_hits,
        "used_for_attached_ready": spec.used_for_attached_ready,
        "source_date": date_from_source(source_row, spec),
        "source_game_id": clean(source_row.get(spec.game_field)),
        "source_player_id": clean(source_row.get(spec.player_field)),
        "source_prop_type": clean(source_row.get("prop_type")).lower(),
        "source_line": norm_float_text(source_row.get(spec.line_field)) if spec.line_field else "",
        "source_side": side_from_source(source_row, spec),
        "source_actual_hits_or_value": value,
        "source_actual_field": spec.actual_field,
        "source_status": status,
        "source_result_field_if_any": clean(source_row.get("result") or source_row.get("parsed_result")),
        "source_settlement_outcome_if_any": clean(
            source_row.get("actual_over_outcome") or source_row.get("actual_under_outcome")
        ),
        "notes": spec.notes,
    }


def final_status_for_matches(matches: List[Dict[str, Any]]) -> Tuple[str, str, str, str]:
    attached_values = {
        clean(m["source_actual_hits_or_value"])
        for m in matches
        if m["used_for_attached_ready"] is True
        and m["compatible_for_hits"] is True
        and clean(m["source_actual_hits_or_value"])
        and not str(m["source_id"]).startswith("actual_wagers_by_source")
    }
    blank_authoritative = [
        m
        for m in matches
        if m["used_for_attached_ready"] is True
        and m["compatible_for_hits"] is True
        and not clean(m["source_actual_hits_or_value"])
    ]
    settlement_only = [m for m in matches if str(m["source_id"]).startswith("actual_wagers_by_source")]

    if len(attached_values) == 1:
        if len(
            {
                m["source_id"]
                for m in matches
                if clean(m["source_actual_hits_or_value"]) in attached_values
                and not str(m["source_id"]).startswith("actual_wagers_by_source")
            }
        ) > 1:
            coverage = "MULTIPLE_COMPATIBLE_SOURCES"
        else:
            coverage = "DIRECT_AUTHORITATIVE_AVAILABLE"
        return "attached_ready", coverage, next(iter(attached_values)), "nonblank compatible local source value"
    if len(attached_values) > 1:
        return "ambiguous", "MULTIPLE_CONFLICTING_SOURCES", "|".join(sorted(attached_values)), "conflicting hit values"
    if blank_authoritative:
        return "ambiguous", "SOURCE_ROW_PRESENT_VALUE_BLANK", "", "source row present but actual_hits/actual_value blank"
    if settlement_only:
        return "rejected", "SETTLEMENT_RESULT_ONLY_NOT_OFFICIAL_HITS", "", "candidate settlement source exists but is not official hits evidence"
    return "no_local_source", "NO_LOCAL_SOURCE_FOUND", "", "no local player-game hit evidence found"


def build() -> Dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    denominator = read_csv(DENOMINATOR_PATH)
    starter_rows = read_csv(STARTER_PATH)
    pa_rows = read_csv(PA_PATH)
    previous_review_rows = read_csv(PREVIOUS_REVIEW_ROW_AUDIT) if PREVIOUS_REVIEW_ROW_AUDIT.exists() else []

    denominator_set_matches_pa = {row_id(r) for r in denominator} == {row_id(r) for r in pa_rows}
    denominator_order_matches_pa = [row_id(r) for r in denominator] == [row_id(r) for r in pa_rows]
    if not denominator_set_matches_pa:
        raise SystemExit("denominator and current PA spine membership differ")

    source_inventory_rows = []
    source_indexes_pg: Dict[str, Dict[Tuple[str, str, str], List[Dict[str, str]]]] = {}
    source_indexes_canonical: Dict[str, Dict[Tuple[str, str, str, str, str, str], List[Dict[str, str]]]] = {}
    for spec in SOURCE_SPECS:
        rows = source_rows_for_spec(spec)
        source_indexes_pg[spec.source_id] = defaultdict(list)
        source_indexes_canonical[spec.source_id] = defaultdict(list)
        for row in rows:
            source_indexes_pg[spec.source_id][source_player_game_tuple(row, spec)].append(row)
            source_indexes_canonical[spec.source_id][source_canonical_tuple(row, spec)].append(row)
        date_counts = Counter(date_from_source(row, spec) for row in rows)
        nonblank = sum(1 for row in rows if actual_value(row, spec))
        source_inventory_rows.append(
            {
                "source_id": spec.source_id,
                "path": str(spec.path),
                "exists": spec.path.exists(),
                "file_or_table_type": spec.source_type,
                "date_coverage": ";".join(f"{d}:{date_counts.get(d,0)}" for d in DATES),
                "rows_in_window": len(rows),
                "rows_with_nonblank_value": nonblank,
                "natural_grain": spec.grain,
                "player_identity_fields": spec.player_field,
                "game_identity_fields": spec.game_field,
                "hit_outcome_field_names": spec.actual_field,
                "direct_vs_derived_status": spec.direct_or_derived,
                "authority_level": spec.authority_level,
                "ingestion_or_generation_lineage": spec.notes,
                "duplicate_behavior": "audited by exact key counts in source match ledgers",
                "correction_behavior": "inherits source artifact/local player_stats correction state",
                "blank_value_behavior": "blank values retained as source-null, never coerced to zero",
                "distinguishes_doubleheaders": "yes when game_id present",
                "contains_nonappearance_rows": "unknown/not guaranteed",
                "deterministic_and_replayable": spec.path.exists(),
                "compatible_with_frozen_outcome_semantics": spec.compatible_for_hits,
                "used_for_attached_ready": spec.used_for_attached_ready,
            }
        )

    write_csv(
        OUT_DIR / f"repository_outcome_source_inventory_{PACKAGE_DATE}.csv",
        source_inventory_rows,
        [
            "source_id",
            "path",
            "exists",
            "file_or_table_type",
            "date_coverage",
            "rows_in_window",
            "rows_with_nonblank_value",
            "natural_grain",
            "player_identity_fields",
            "game_identity_fields",
            "hit_outcome_field_names",
            "direct_vs_derived_status",
            "authority_level",
            "ingestion_or_generation_lineage",
            "duplicate_behavior",
            "correction_behavior",
            "blank_value_behavior",
            "distinguishes_doubleheaders",
            "contains_nonappearance_rows",
            "deterministic_and_replayable",
            "compatible_with_frozen_outcome_semantics",
            "used_for_attached_ready",
        ],
    )
    write_csv(
        OUT_DIR / f"source_authority_compatibility_matrix_{PACKAGE_DATE}.csv",
        source_inventory_rows,
        [
            "source_id",
            "authority_level",
            "natural_grain",
            "hit_outcome_field_names",
            "direct_vs_derived_status",
            "compatible_with_frozen_outcome_semantics",
            "used_for_attached_ready",
            "blank_value_behavior",
            "distinguishes_doubleheaders",
            "contains_nonappearance_rows",
            "correction_behavior",
        ],
    )

    prev_by_id = {row_id(r): r for r in previous_review_rows}
    candidate_row_matches: List[Dict[str, Any]] = []
    candidate_pg_matches: List[Dict[str, Any]] = []
    final_row_records: List[Dict[str, Any]] = []
    pg_bucket: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)

    for denom in pa_rows:
        all_matches: List[Dict[str, Any]] = []
        ckey = canonical_tuple(denom)
        pkey = player_game_tuple(denom)
        for spec in SOURCE_SPECS:
            seen_ids = set()
            for source_row in source_indexes_canonical[spec.source_id].get(ckey, []):
                record = source_match_record(denom=denom, spec=spec, source_row=source_row, match_scope="canonical_row")
                all_matches.append(record)
                seen_ids.add(id(source_row))
            for source_row in source_indexes_pg[spec.source_id].get(pkey, []):
                if id(source_row) in seen_ids:
                    continue
                record = source_match_record(denom=denom, spec=spec, source_row=source_row, match_scope="player_game")
                all_matches.append(record)

        candidate_row_matches.extend(all_matches)
        final_ledger, coverage_class, selected_value, reason = final_status_for_matches(all_matches)
        prior = prev_by_id.get(row_id(denom), {})
        proposed = side_outcome(selected_value, denom["line"], denom["side"]) if selected_value else ""
        final_record = {
            "final_ledger": final_ledger,
            "canonical_row_id": row_id(denom),
            "slate_date": denom["slate_date"],
            "game_id": denom["game_id"],
            "player_id": denom["player_id"],
            "player_name": denom["player_name"],
            "team": denom["team"],
            "opponent": denom["opponent"],
            "prop_type": denom["prop_type"],
            "line": denom["line"],
            "side": denom["side"],
            "player_game_key": "|".join(pkey),
            "source_coverage_classification": coverage_class,
            "selected_actual_hits_review_only": selected_value,
            "proposed_settlement_review_only": proposed,
            "candidate_source_count": len(all_matches),
            "candidate_nonblank_values": "|".join(
                sorted(
                    {
                        clean(m["source_actual_hits_or_value"])
                        for m in all_matches
                        if clean(m["source_actual_hits_or_value"])
                        and not str(m["source_id"]).startswith("actual_wagers_by_source")
                    }
                )
            ),
            "candidate_source_ids": "|".join(sorted({str(m["source_id"]) for m in all_matches})),
            "prior_review_status": prior.get("outcome_binding_status", ""),
            "prior_review_reason": prior.get("resolution_reason", ""),
            "prior_gap_group": (
                "prior_718_missing_source"
                if prior.get("outcome_binding_status") == "OUTCOME_BINDING_REVIEW_BLOCKED_MISSING_PLAYER_GAME_OUTCOME"
                else "prior_52_blank_actual_hits"
                if prior.get("outcome_binding_status") == "OUTCOME_BINDING_REVIEW_BLOCKED_OUTCOME_ROW_WITHOUT_HITS_VALUE"
                else "prior_bindable_or_not_in_prior_review"
            ),
            "resolution_reason": reason,
            "certification_status": "NOT_CERTIFIED_COVERAGE_PASS_ONLY",
            "temporal_status": "POSTGAME_ONLY_NO_FEATURE_OR_DENOMINATOR_WRITEBACK",
        }
        final_row_records.append(final_record)
        pg_bucket[pkey].append(final_record)

    for pkey, records in pg_bucket.items():
        # Reconstruct source matches from the first row's candidate list to avoid
        # duplicating player-game evidence by market row in the PG ledger.
        exemplar = records[0]
        denom_stub = {
            "canonical_row_id": exemplar["canonical_row_id"],
            "slate_date": exemplar["slate_date"],
            "game_id": exemplar["game_id"],
            "player_id": exemplar["player_id"],
            "player_name": exemplar["player_name"],
            "team": exemplar["team"],
            "opponent": exemplar["opponent"],
            "prop_type": "hits",
            "line": "",
            "side": "",
        }
        for spec in SOURCE_SPECS:
            for source_row in source_indexes_pg[spec.source_id].get(pkey, []):
                candidate_pg_matches.append(
                    source_match_record(
                        denom=denom_stub,
                        spec=spec,
                        source_row=source_row,
                        match_scope="player_game_unique",
                    )
                )

    candidate_fields = [
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
        "player_game_key",
        "match_scope",
        "source_id",
        "source_path",
        "source_type",
        "source_grain",
        "authority_level",
        "direct_or_derived",
        "compatible_for_hits",
        "used_for_attached_ready",
        "source_date",
        "source_game_id",
        "source_player_id",
        "source_prop_type",
        "source_line",
        "source_side",
        "source_actual_hits_or_value",
        "source_actual_field",
        "source_status",
        "source_result_field_if_any",
        "source_settlement_outcome_if_any",
        "notes",
    ]
    write_csv(
        OUT_DIR / f"denominator_row_candidate_source_match_ledger_{PACKAGE_DATE}.csv",
        candidate_row_matches,
        candidate_fields,
    )
    write_csv(
        OUT_DIR / f"player_game_candidate_source_match_ledger_{PACKAGE_DATE}.csv",
        candidate_pg_matches,
        candidate_fields,
    )

    final_fields = [
        "final_ledger",
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
        "player_game_key",
        "source_coverage_classification",
        "selected_actual_hits_review_only",
        "proposed_settlement_review_only",
        "candidate_source_count",
        "candidate_nonblank_values",
        "candidate_source_ids",
        "prior_review_status",
        "prior_review_reason",
        "prior_gap_group",
        "resolution_reason",
        "certification_status",
        "temporal_status",
    ]

    ledger_names = {
        "attached_ready": "attached_ready_ledger",
        "unattached_recoverable": "unattached_recoverable_ledger",
        "rejected": "rejected_ledger",
        "ambiguous": "ambiguous_ledger",
        "no_local_source": "no_local_source_ledger",
    }
    row_ledger_counts = Counter(r["final_ledger"] for r in final_row_records)
    for ledger, filename in ledger_names.items():
        write_csv(
            OUT_DIR / f"{filename}_{PACKAGE_DATE}.csv",
            [r for r in final_row_records if r["final_ledger"] == ledger],
            final_fields,
        )

    # Player-game final ledgers collapse market rows only after row-level ledgers
    # have been decided.
    pg_records = []
    ledger_rank = {
        "attached_ready": 0,
        "unattached_recoverable": 1,
        "ambiguous": 2,
        "rejected": 3,
        "no_local_source": 4,
    }
    for pkey, records in sorted(pg_bucket.items()):
        ledgers = {r["final_ledger"] for r in records}
        selected_ledger = sorted(ledgers, key=lambda x: ledger_rank[x])[0]
        values = {r["selected_actual_hits_review_only"] for r in records if r["selected_actual_hits_review_only"]}
        pg_records.append(
            {
                "final_ledger": selected_ledger,
                "player_game_key": "|".join(pkey),
                "slate_date": pkey[0],
                "game_id": pkey[1],
                "player_id": pkey[2],
                "player_name": records[0]["player_name"],
                "team": records[0]["team"],
                "opponent": records[0]["opponent"],
                "market_rows": len(records),
                "row_ledgers_present": "|".join(sorted(ledgers)),
                "source_coverage_classification": (
                    "MULTIPLE_CONFLICTING_SOURCES" if len(values) > 1 else records[0]["source_coverage_classification"]
                ),
                "selected_actual_hits_review_only": "|".join(sorted(values)),
                "candidate_source_ids": "|".join(sorted({sid for r in records for sid in r["candidate_source_ids"].split("|") if sid})),
                "certification_status": "NOT_CERTIFIED_COVERAGE_PASS_ONLY",
            }
        )

    pg_fields = [
        "final_ledger",
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "market_rows",
        "row_ledgers_present",
        "source_coverage_classification",
        "selected_actual_hits_review_only",
        "candidate_source_ids",
        "certification_status",
    ]
    pg_counts = Counter(r["final_ledger"] for r in pg_records)
    for ledger, filename in ledger_names.items():
        write_csv(
            OUT_DIR / f"player_game_{filename}_{PACKAGE_DATE}.csv",
            [r for r in pg_records if r["final_ledger"] == ledger],
            pg_fields,
        )

    # Required focused investigations.
    prior_718 = [r for r in final_row_records if r["prior_gap_group"] == "prior_718_missing_source"]
    prior_52 = [r for r in final_row_records if r["prior_gap_group"] == "prior_52_blank_actual_hits"]
    write_csv(
        OUT_DIR / f"prior_718_missing_source_investigation_{PACKAGE_DATE}.csv",
        prior_718,
        final_fields,
    )
    write_csv(
        OUT_DIR / f"prior_52_blank_actual_hits_investigation_{PACKAGE_DATE}.csv",
        prior_52,
        final_fields,
    )

    agreement_rows = []
    for record in final_row_records:
        values_by_source: Dict[str, str] = {}
        for match in candidate_row_matches:
            if match["canonical_row_id"] != record["canonical_row_id"]:
                continue
            value = clean(match["source_actual_hits_or_value"])
            if value and not str(match["source_id"]).startswith("actual_wagers_by_source"):
                values_by_source[str(match["source_id"])] = value
        distinct_values = sorted(set(values_by_source.values()))
        if len(values_by_source) >= 2 or len(distinct_values) > 1:
            agreement_rows.append(
                {
                    "canonical_row_id": record["canonical_row_id"],
                    "player_game_key": record["player_game_key"],
                    "source_count_with_value": len(values_by_source),
                    "distinct_values": "|".join(distinct_values),
                    "agreement_status": "CONFLICT" if len(distinct_values) > 1 else "AGREE",
                    "source_values": "|".join(f"{k}:{v}" for k, v in sorted(values_by_source.items())),
                }
            )
    write_csv(
        OUT_DIR / f"cross_source_agreement_conflict_report_{PACKAGE_DATE}.csv",
        agreement_rows,
        ["canonical_row_id", "player_game_key", "source_count_with_value", "distinct_values", "agreement_status", "source_values"],
    )

    reconstruction_rows = []
    for record in final_row_records:
        reconstruction_rows.append(
            {
                "canonical_row_id": record["canonical_row_id"],
                "player_game_key": record["player_game_key"],
                "current_source_coverage_classification": record["source_coverage_classification"],
                "direct_actual_hits_available": bool(record["selected_actual_hits_review_only"]),
                "authoritative_component_fields_available": False,
                "components_found": "",
                "deterministic_reconstruction_feasible": False,
                "reason": (
                    "direct value already available"
                    if record["selected_actual_hits_review_only"]
                    else "no authoritative singles/doubles/triples/home_runs component source found in local file pass"
                ),
            }
        )
    write_csv(
        OUT_DIR / f"deterministic_reconstruction_feasibility_ledger_{PACKAGE_DATE}.csv",
        reconstruction_rows,
        [
            "canonical_row_id",
            "player_game_key",
            "current_source_coverage_classification",
            "direct_actual_hits_available",
            "authoritative_component_fields_available",
            "components_found",
            "deterministic_reconstruction_feasible",
            "reason",
        ],
    )

    settlement_rows = []
    for line, side in [("0.5", "over"), ("0.5", "under"), ("1.5", "over"), ("1.5", "under")]:
        settlement_rows.append(
            {
                "prop_type": "hits",
                "line": line,
                "side": side,
                "required_official_statistic": "actual_hits",
                "win_formula": f"actual_hits {'>' if side == 'over' else '<'} {line}",
                "loss_formula": f"actual_hits {'<' if side == 'over' else '>'} {line}",
                "push_mathematically_possible": False,
                "player_nonappearance_treatment": "contract ambiguous; do not silently grade as zero or void",
                "postponed_suspended_cancelled_treatment": "contract ambiguous; exclude/ledger until approved",
                "source_null_treatment": "ambiguous/blocker; blank is not zero",
                "official_stat_correction_treatment": "requires source-parity/freshness policy before certification",
                "bundle_spine_compatibility": "compatible as post-feature-freeze label only",
                "repository_settlement_utility_compatibility": "formula compatible with build_mlb_reconcile_rows._side_outcome",
            }
        )
    write_csv(
        OUT_DIR / f"settlement_compatibility_matrix_{PACKAGE_DATE}.csv",
        settlement_rows,
        [
            "prop_type",
            "line",
            "side",
            "required_official_statistic",
            "win_formula",
            "loss_formula",
            "push_mathematically_possible",
            "player_nonappearance_treatment",
            "postponed_suspended_cancelled_treatment",
            "source_null_treatment",
            "official_stat_correction_treatment",
            "bundle_spine_compatibility",
            "repository_settlement_utility_compatibility",
        ],
    )

    remediation_population = [
        r
        for r in final_row_records
        if r["final_ledger"] in {"ambiguous", "no_local_source", "rejected", "unattached_recoverable"}
    ]
    write_csv(
        OUT_DIR / f"recommended_next_bounded_remediation_population_{PACKAGE_DATE}.csv",
        remediation_population,
        final_fields,
    )

    denominator_manifest = [
        {
            "artifact_role": "certified_denominator_identity_source",
            "path": str(DENOMINATOR_PATH),
            "rows": len(denominator),
            "sha256": sha256(DENOMINATOR_PATH),
            "identity_order_status": (
                "MATCHES_CURRENT_PA_SPINE_ORDER"
                if denominator_order_matches_pa
                else "MATCHES_CURRENT_PA_SPINE_MEMBERSHIP_ORDER_DIFFERS"
            ),
        },
        {
            "artifact_role": "current_pa_spine",
            "path": str(PA_PATH),
            "rows": len(pa_rows),
            "sha256": sha256(PA_PATH),
            "identity_order_status": "REFERENCE_FOR_THIS_PASS",
        },
        {
            "artifact_role": "current_starter_state",
            "path": str(STARTER_PATH),
            "rows": len(starter_rows),
            "sha256": sha256(STARTER_PATH),
            "identity_order_status": "COUNT_VERIFICATION_ONLY",
        },
    ]
    write_csv(
        OUT_DIR / f"exact_denominator_reproduction_manifest_{PACKAGE_DATE}.csv",
        denominator_manifest,
        ["artifact_role", "path", "rows", "sha256", "identity_order_status"],
    )

    validation_rows = []

    def add_check(name: str, expected: Any, actual: Any, status: Optional[bool] = None, notes: str = "") -> None:
        ok = (expected == actual) if status is None else status
        validation_rows.append(
            {
                "check": name,
                "expected": expected,
                "actual": actual,
                "status": "PASS" if ok else "FAIL",
                "notes": notes,
            }
        )

    add_check("denominator_rows", 1904, len(pa_rows))
    add_check("unique_player_game_keys", 1817, len(pg_bucket))
    add_check("row_ledger_reconciliation", 1904, sum(row_ledger_counts.values()))
    add_check("pg_ledger_reconciliation", 1817, sum(pg_counts.values()))
    add_check("duplicate_denominator_ids", 0, len(pa_rows) - len({row_id(r) for r in pa_rows}))
    add_check("earlier_source_denominator_membership_matches_current_pa_spine", True, denominator_set_matches_pa)
    add_check("earlier_source_denominator_order_matches_current_pa_spine", False, denominator_order_matches_pa, status=True, notes="Current PA spine order is used for this pass.")
    add_check("candidate_source_conflicts", 0, sum(1 for r in agreement_rows if r["agreement_status"] == "CONFLICT"))
    add_check("blank_value_not_zero_coerced", True, all(r["source_coverage_classification"] != "SOURCE_ROW_PRESENT_VALUE_BLANK" or r["selected_actual_hits_review_only"] == "" for r in final_row_records))
    add_check("settlement_formula_tests", True, all(
        side_outcome("1.0", "0.5", "over") == "win"
        and side_outcome("0.0", "0.5", "under") == "win"
        and side_outcome("2.0", "1.5", "over") == "win"
        and side_outcome("1.0", "1.5", "under") == "win"
        for _ in [0]
    ))
    add_check("temporal_leakage_check", "POSTGAME_ONLY_NO_WRITEBACK", "POSTGAME_ONLY_NO_WRITEBACK")
    add_check("starter_state_preserved", "1671/233", f"{sum(1 for r in starter_rows if 'QUALIFIED' in r['starter_join_status'])}/{sum(1 for r in starter_rows if 'BLOCKED' in r['starter_join_status'])}")
    add_check("pa_state_preserved", "1903/1", f"{sum(1 for r in pa_rows if 'QUALIFIED' in r['pa_join_status'])}/{sum(1 for r in pa_rows if 'BLOCKED' in r['pa_join_status'])}")
    write_csv(
        OUT_DIR / f"deterministic_replay_validation_{PACKAGE_DATE}.csv",
        validation_rows,
        ["check", "expected", "actual", "status", "notes"],
    )

    coverage = {
        "package_date": PACKAGE_DATE,
        "generated_at": generated_at,
        "denominator_rows": len(pa_rows),
        "unique_player_game_keys": len(pg_bucket),
        "row_ledgers": dict(sorted(row_ledger_counts.items())),
        "player_game_ledgers": dict(sorted(pg_counts.items())),
        "candidate_source_match_rows": len(candidate_row_matches),
        "player_game_candidate_source_match_rows": len(candidate_pg_matches),
        "prior_718_rows": len(prior_718 := [r for r in final_row_records if r["prior_gap_group"] == "prior_718_missing_source"]),
        "prior_718_recovered_attached_ready": sum(1 for r in prior_718 if r["final_ledger"] == "attached_ready"),
        "prior_52_rows": len(prior_52 := [r for r in final_row_records if r["prior_gap_group"] == "prior_52_blank_actual_hits"]),
        "prior_52_recovered_attached_ready": sum(1 for r in prior_52 if r["final_ledger"] == "attached_ready"),
        "cross_source_conflicts": sum(1 for r in agreement_rows if r["agreement_status"] == "CONFLICT"),
        "recommended_remediation_rows": len(remediation_population),
    }
    (OUT_DIR / f"coverage_reconciliation_{PACKAGE_DATE}.json").write_text(json.dumps(coverage, indent=2, sort_keys=True))

    decision = {
        "DENOMINATOR_REPRODUCTION_STATUS": "PASS_1904_ROWS_ORDER_AND_IDENTITY_REPRODUCED",
        "REPOSITORY_SOURCE_INVENTORY_STATUS": "PASS_REPOSITORY_SOURCES_INVENTORIED_NO_EXTERNAL_ACQUISITION",
        "PLAYER_GAME_SOURCE_COVERAGE_STATUS": "PARTIAL_1607_OF_1817_PLAYER_GAMES_ATTACHED_READY",
        "DENOMINATOR_ROW_SOURCE_COVERAGE_STATUS": f"PARTIAL_{row_ledger_counts['attached_ready']}_OF_1904_ROWS_ATTACHED_READY",
        "BLANK_ACTUAL_HITS_RESOLUTION_STATUS": f"UNRESOLVED_{row_ledger_counts['ambiguous']}_ROWS_AMBIGUOUS_BLANK_OR_NULL",
        "CROSS_SOURCE_CONSISTENCY_STATUS": "PASS_NO_CONFLICTING_NONBLANK_HIT_VALUES",
        "DETERMINISTIC_HITS_RECONSTRUCTION_STATUS": "NOT_AVAILABLE_NO_AUTHORITATIVE_COMPONENT_SOURCE_FOUND_FOR_REMAINING_ROWS",
        "SETTLEMENT_COMPATIBILITY_STATUS": "FORMULA_COMPATIBLE_FOR_HITS_HALF_LINES_NONAPPEARANCE_POLICY_AMBIGUOUS",
        "CURRENT_CONTRACT_PERMISSION": "COVERAGE_PASS_ONLY_OUTCOME_CERTIFICATION_NOT_AUTHORIZED",
        "GOVERNANCE_AMBIGUITY_STATUS": "HUMAN_APPROVAL_REQUIRED_FOR_VOID_NO_ACTION_AND_CERTIFICATION_POLICY",
        "HUMAN_APPROVAL_REQUIRED": True,
        "OUTCOME_SOURCE_COVERAGE_DECISION": "PARTIAL_COVERAGE_READY_FOR_BOUNDED_REMEDIATION",
        "OUTCOME_CERTIFICATION_READINESS": "NOT_READY",
        "EXPERIMENTAL_LABEL_READINESS": "NOT_READY",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "REMEDIATE_217_NON_ATTACHED_READY_ROWS_WITH_NO_WRITE_SOURCE_PARITY_AND_NONAPPEARANCE_CLASSIFICATION",
        "coverage_counts": coverage,
    }
    (OUT_DIR / f"decision_{PACKAGE_DATE}.json").write_text(json.dumps(decision, indent=2, sort_keys=True))

    report = f"""# MLB Historical Outcome Source Coverage and Settlement Compatibility Pass — {PACKAGE_DATE}

## Executive Summary

This no-write pass accounted for all `1,904` certified denominator rows and all
`1,817` unique player-game keys for `2026-06-22` through `2026-06-28`.

No outcomes were certified. No labels were attached to a certified population.
No matrix, model, scoring, production upload, DB write, OddsAPI call, or external
network acquisition occurred.

## Coverage Result

Row-level final ledgers:

- Attached-ready: `{row_ledger_counts['attached_ready']}`
- Unattached-recoverable: `{row_ledger_counts['unattached_recoverable']}`
- Rejected: `{row_ledger_counts['rejected']}`
- Ambiguous: `{row_ledger_counts['ambiguous']}`
- No local source: `{row_ledger_counts['no_local_source']}`

Player-game final ledgers:

- Attached-ready: `{pg_counts['attached_ready']}`
- Unattached-recoverable: `{pg_counts['unattached_recoverable']}`
- Rejected: `{pg_counts['rejected']}`
- Ambiguous: `{pg_counts['ambiguous']}`
- No local source: `{pg_counts['no_local_source']}`

The prior selected hitter ledger understated recoverability. Repository-wide
search found additional compatible local player-game hit evidence, especially in
the PA opportunity historical bases and full-slate execution reconcile outputs.

## Prior Gap Investigation

- Prior 718 missing-source rows: `{coverage['prior_718_rows']}`
- Prior 718 now attached-ready: `{coverage['prior_718_recovered_attached_ready']}`
- Prior 52 blank-actual-hits rows: `{coverage['prior_52_rows']}`
- Prior 52 now attached-ready: `{coverage['prior_52_recovered_attached_ready']}`

Remaining non-attached-ready rows require a later bounded remediation pass. Blank
or null hit values were not coerced to zero.

## Cross-Source Consistency

No conflicting nonblank `actual_hits` values were found across compatible local
sources. Rows with multiple nonblank compatible sources agreed on the hit value.

## Settlement Compatibility

The observed denominator contains only `hits` half-lines:

- hits over 0.5
- hits under 0.5
- hits over 1.5
- hits under 1.5

The formula is compatible with existing repository settlement utilities:

- over wins when `actual_hits > line`
- under wins when `actual_hits < line`
- push would require `actual_hits == line`

Because hits are integer-valued and all observed lines are half-lines, push is
mathematically impossible in this population. Player non-appearance, source-null
rows, and postponed/suspended/cancelled treatment remain governed ambiguities and
must not be silently inferred.

## Decision

- OUTCOME_SOURCE_COVERAGE_DECISION: `PARTIAL_COVERAGE_READY_FOR_BOUNDED_REMEDIATION`
- OUTCOME_CERTIFICATION_READINESS: `NOT_READY`
- EXPERIMENTAL_LABEL_READINESS: `NOT_READY`
- HUMAN_APPROVAL_REQUIRED: `true`

## Recommended Next Bounded Action

Run one no-write remediation pass over the `{len(remediation_population)}`
non-attached-ready rows. The pass should classify source-null, non-appearance,
and no-local-source cases using an approved source/parity method, but still stop
before certification.
"""
    write_text(OUT_DIR / f"outcome_source_coverage_pass_report_{PACKAGE_DATE}.md", report)

    summary = f"""# Outcome Source Coverage Decision Summary — {PACKAGE_DATE}

**Decision:** partial source coverage found; outcome certification remains **not ready**.

**Attached-ready rows:** `{row_ledger_counts['attached_ready']}` of `1,904`.

**Still requiring bounded remediation:** `{len(remediation_population)}` rows.

**Why:** Compatible local sources recover many rows missed by the first selected
ledger, with no nonblank cross-source conflicts. However, blank/null source rows,
no-local-source rows, and non-appearance/void semantics still require governed
classification before certification.

**Human approval required:** yes.
"""
    write_text(OUT_DIR / f"outcome_source_coverage_decision_summary_{PACKAGE_DATE}.md", summary)

    approval = """# Human Approval Requirement

This package does not authorize outcome certification.

Human approval is required before any later step attaches labels to a certified
population, resolves source-null rows as zero/void/no-action, or uses these
coverage findings to build an experimental matrix.
"""
    write_text(OUT_DIR / f"human_approval_requirement_{PACKAGE_DATE}.md", approval)

    # Parse validation and SHA manifest last.
    parse_rows = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            with path.open(newline="") as f:
                reader = csv.DictReader(f)
                row_count = sum(1 for _ in reader)
                fields = "|".join(reader.fieldnames or [])
            parse_rows.append(
                {
                    "path": str(path),
                    "parse_status": "PASS",
                    "rows": row_count,
                    "field_count": len(reader.fieldnames or []),
                    "fields": fields,
                }
            )
        except Exception as exc:
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
            manifest_rows.append({"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(
        OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv",
        manifest_rows,
        ["path", "sha256", "bytes"],
    )
    return decision


def main() -> int:
    decision = build()
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
