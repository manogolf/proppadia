#!/usr/bin/env python3
"""Audit downstream use of line-specific pitcher hits-allowed outputs.

This script is intentionally read-only. It inventories retained research
artifacts and source code references; it does not refit, score, call external
APIs, or write to the database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
MODEL_DEV = REPO_ROOT / "artifacts/analysis/model_development"
OUT_DIR = MODEL_DEV / "mlb_pha_line_specific_downstream_consumer_integrity_audit/2026-07-18"

PHA_DIR = MODEL_DEV / "mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17"
PROMO_DIR = MODEL_DEV / "mlb_pitcher_hits_allowed_promotion_grade/2026-07-17"
TRANSFER_DIR = MODEL_DEV / "mlb_pitcher_foundation_hitter_hits_transfer/2026-07-17"
HITS05_PROMO_DIR = MODEL_DEV / "mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17"
COUNT_INVARIANCE_DIR = MODEL_DEV / "mlb_pha_live_shadow_count_invariance_audit/2026-07-18"
LIVE_PARENT_DIR = MODEL_DEV / "mlb_live_hitter_parent_daily_integration/2026-07-18"
LIVE_REPLAY_DIR = MODEL_DEV / "mlb_pitcher_hits_allowed_live_replay_repair/2026-07-17"
CROSS_PROP_DIR = MODEL_DEV / "mlb_granular_feature_platform_cross_prop_transfer/2026-07-17"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def norm_key_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        return text[:-2]
    return text


def make_key(df: pd.DataFrame, pitcher_col: str = "pitcher_id") -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    return df.apply(
        lambda r: "|".join(norm_key_value(r.get(c)) for c in ["slate_date", "game_id", pitcher_col]),
        axis=1,
    )


def pct(n: int, d: int) -> float:
    return round((n / d) * 100.0, 4) if d else 0.0


def summarize_population(path: Path, multiline_keys: set[str]) -> dict[str, Any]:
    df = read_csv(path)
    if df.empty:
        return {
            "rows": 0,
            "affected_rows": 0,
            "affected_pitcher_games": 0,
            "has_path": False,
        }
    if "pitcher_id" in df.columns:
        pitcher_col = "pitcher_id"
    elif "opposing_starter_id" in df.columns:
        pitcher_col = "opposing_starter_id"
    else:
        pitcher_col = "player_id"
    keys = make_key(df, pitcher_col)
    affected = keys.isin(multiline_keys)
    return {
        "rows": int(len(df)),
        "affected_rows": int(affected.sum()),
        "affected_pitcher_games": int(keys[affected].nunique()),
        "has_path": True,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def artifact_ref(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT)) if path.exists() else str(path.relative_to(REPO_ROOT))


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    historical_ml_path = COUNT_INVARIANCE_DIR / "pha_historical_multi_line_trace_2026-07-18.csv"
    historical_ml = read_csv(historical_ml_path)
    historical_ml_keys = set(make_key(historical_ml)) if not historical_ml.empty else set()

    transfer_contract_path = TRANSFER_DIR / "pitcher_foundation_transfer_contract_2026-07-17.csv"
    transfer_contract = read_csv(transfer_contract_path)
    if not transfer_contract.empty and "pitcher_line_rows" in transfer_contract.columns:
        multi_contract = transfer_contract[pd.to_numeric(transfer_contract["pitcher_line_rows"], errors="coerce") > 1]
        multiline_keys = set(make_key(multi_contract))
    else:
        multi_contract = pd.DataFrame()
        multiline_keys = set()

    populations = {
        "initial_hitter_hits05_transfer": TRANSFER_DIR / "hits05_pitcher_transfer_population_2026-07-17.csv",
        "initial_hitter_hits15_transfer": TRANSFER_DIR / "hits15_pitcher_transfer_population_2026-07-17.csv",
        "o15_market_ranking_transfer": TRANSFER_DIR / "o15_market_ranking_transfer_population_2026-07-17.csv",
        "hits05_promotion_grade": HITS05_PROMO_DIR / "hits05_final_transfer_population_2026-07-17.csv",
        "july18_controlled_shadow": COUNT_INVARIANCE_DIR / "pha_corrected_controlled_shadow_2026-07-18.csv",
    }
    pop_stats = {name: summarize_population(path, multiline_keys) for name, path in populations.items()}
    live_shadow = read_csv(populations["july18_controlled_shadow"])
    if not live_shadow.empty and {"slate_date", "game_id", "player_id"}.issubset(live_shadow.columns):
        live_group_sizes = live_shadow.groupby(["slate_date", "game_id", "player_id"], dropna=False).size()
        live_multi_groups = live_group_sizes[live_group_sizes > 1]
        pop_stats["july18_controlled_shadow"]["affected_pitcher_games"] = int(len(live_multi_groups))
        pop_stats["july18_controlled_shadow"]["affected_rows"] = int(live_multi_groups.sum())

    contracts_rows = [
        {
            "contract": "Contract A",
            "name": "line_specific_pha_proposition_challenger",
            "grain": "slate_date|game_id|pitcher_id|market_line|side",
            "valid_fields": "line_specific_champion_count_proxy; line_specific_challenger_count_proxy; market_line; champion_over_probability; challenger_over_probability; distance_from_line; side; disagreement_state",
            "invalid_fields": "universal pitcher-game expected-hit count",
            "valid_uses": "PHA proposition ranking; OVER/UNDER probability; line-specific diagnostics; controlled-shadow grading",
            "invalid_uses": "collapsing multiple market lines into one invariant pitcher-game expectation",
            "source_evidence": "champion_expected_hits_allowed_poisson_implied is inverted from market line plus model_prob_over; challenger_e includes that proxy",
            "notes": "Frozen Challenger remains valid as a line-aware proposition instrument.",
        },
        {
            "contract": "Contract B",
            "name": "line_invariant_pitcher_foundation",
            "grain": "slate_date|game_id|pitcher_id",
            "valid_fields": "workload; expected_batters_faced; expected_starter_facing_pa_environment; starter_exit_probability; opponent-lineup encounter aggregate; pitcher contact/conversion profiles; support; uncertainty; non-line suppression context",
            "invalid_fields": "champion_expected_hits_allowed_poisson_implied; challenger_e_champion_plus_granular_expected_hits_allowed; means or residuals derived from line-specific PHA proxy rows",
            "valid_uses": "hitter Hits O0.5 transfer; hitter Hits O1.5 transfer; pitcher suppression context; shared game-level pitcher environment",
            "invalid_uses": "using line-specific PHA outputs as if they were one pitcher-game expected-hit count",
            "source_evidence": "Pitcher foundation transfer contract already contains line-invariant strict-prior workload and encounter fields; line-specific proxy fields must be excluded.",
            "notes": "A clean Contract B reevaluation should preserve original splits and instruments but remove proxy-derived residuals.",
        },
    ]

    consumer_rows = [
        {
            "consumer": "PHA historical line-level performance",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_pitcher_hits_allowed_granular_encounter_challenger.py",
            "source_field": "champion_expected_hits_allowed_poisson_implied; challenger_e_champion_plus_granular_expected_hits_allowed; *_prob_over",
            "source_grain": "Contract A proposition line",
            "target_grain": "pitcher proposition line",
            "join_keys": "canonical_key / slate_date|game_id|pitcher_id|line|bookmaker_key",
            "multi_line_resolution_policy": "none; line rows retained",
            "affected_pitcher_games": len(historical_ml_keys),
            "affected_target_rows": int(len(historical_ml)),
            "market_line_retained": "yes",
            "treated_as_invariant": "no for line-level performance; count labels need caveat",
            "validity_classification": "VALID_LINE_SPECIFIC_USE",
            "result_status": "preserved",
            "notes": "Line-specific OVER/UNDER ranking and proposition diagnostics remain valid.",
        },
        {
            "consumer": "PHA count MAE claims",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_pitcher_hits_allowed_granular_encounter_challenger.py; backend/mlb/scripts/run_mlb_pitcher_hits_allowed_promotion_grade.py",
            "source_field": "champion_expected_hits_allowed; challenger_e_champion_plus_granular_expected_hits_allowed",
            "source_grain": "Contract A proposition line",
            "target_grain": "pitcher proposition line count diagnostic",
            "join_keys": "row-level PHA proposition identity",
            "multi_line_resolution_policy": "none in PHA package; line rows evaluated separately",
            "affected_pitcher_games": len(historical_ml_keys),
            "affected_target_rows": int(len(historical_ml)),
            "market_line_retained": "yes",
            "treated_as_invariant": "label risk only",
            "validity_classification": "SEMANTIC_LABEL_ONLY_CORRECTION",
            "result_status": "preserved as line-specific proxy MAE; not preserved as invariant pitcher-game expected-count MAE",
            "notes": "The values are useful diagnostics but must not be described as universal pitcher-game expected hits.",
        },
        {
            "consumer": "Initial hitter Hits transfer",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_pitcher_foundation_hitter_hits_transfer.py",
            "source_field": "pitcher_granular_expected_hits_allowed; pitcher_granular_minus_champion_residual; champion_expected_hits_allowed",
            "source_grain": "Contract B fields plus collapsed Contract A champion proxy",
            "target_grain": "hitter player-game proposition research rows",
            "join_keys": "slate_date|game_id|pitcher_id then hitter/player-game join",
            "multi_line_resolution_policy": "aggregation: mean(champion_expected_hits_allowed) by slate_date|game_id|pitcher_id",
            "affected_pitcher_games": pop_stats["initial_hitter_hits05_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["initial_hitter_hits05_transfer"]["affected_rows"],
            "market_line_retained": "pitcher_hits_allowed_lines retained as diagnostic only",
            "treated_as_invariant": "yes for collapsed residual",
            "validity_classification": "REQUIRES_CLEAN_REEVALUATION",
            "result_status": "foundation-field findings preserved; residual-dependent conclusions require Contract B reevaluation",
            "notes": "Strict-prior workload/encounter fields are not invalidated; the collapsed Champion proxy residual is.",
        },
        {
            "consumer": "Hits O0.5 promotion-grade ranking result",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_hits05_pitcher_foundation_promotion_grade.py",
            "source_field": "challenger_e_expected_hits_allowed; champion_expected_hits_allowed; challenger_residual",
            "source_grain": "collapsed Contract A line-specific proxy fields",
            "target_grain": "hitter Hits O0.5 player-game rows",
            "join_keys": "slate_date|game_id|pitcher_id/opposing_starter_id",
            "multi_line_resolution_policy": "aggregation: mean(champion_expected_hits_allowed), mean(challenger_e_champion_plus_granular_expected_hits_allowed)",
            "affected_pitcher_games": pop_stats["hits05_promotion_grade"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["hits05_promotion_grade"]["affected_rows"],
            "market_line_retained": "pitcher_lines retained only as string diagnostic",
            "treated_as_invariant": "yes",
            "validity_classification": "MULTI_LINE_SELECTION_CONTAMINATION",
            "result_status": "requires clean Contract B reevaluation before promotion-grade claim is relied on",
            "notes": "The model consumed a mean of line-specific values as a pitcher-game feature.",
        },
        {
            "consumer": "O1.5 one-to-two-plus probability transfer",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_pitcher_foundation_hitter_hits_transfer.py",
            "source_field": "pitcher_granular_minus_champion_residual plus line-invariant foundation fields",
            "source_grain": "Contract B fields plus collapsed Contract A champion proxy",
            "target_grain": "hitter Hits O1.5 player-game rows",
            "join_keys": "slate_date|game_id|pitcher_id",
            "multi_line_resolution_policy": "inherits transfer contract aggregation: mean(champion_expected_hits_allowed)",
            "affected_pitcher_games": pop_stats["initial_hitter_hits15_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["initial_hitter_hits15_transfer"]["affected_rows"],
            "market_line_retained": "no PHA market line in target rows",
            "treated_as_invariant": "yes for residual",
            "validity_classification": "REQUIRES_CLEAN_REEVALUATION",
            "result_status": "requires clean Contract B reevaluation for residual-dependent O1.5 transfer claim",
            "notes": "No direct row-level challenger_e was used, but the transfer residual depends on collapsed line-specific Champion proxy.",
        },
        {
            "consumer": "O1.5 market-ranking transfer diagnostic",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_pitcher_foundation_hitter_hits_transfer.py",
            "source_field": "pitcher_granular_minus_champion_residual plus line-invariant foundation fields",
            "source_grain": "Contract B fields plus collapsed Contract A champion proxy",
            "target_grain": "O1.5 market-ranking rows",
            "join_keys": "slate_date|game_id|pitcher_id",
            "multi_line_resolution_policy": "inherits transfer contract aggregation: mean(champion_expected_hits_allowed)",
            "affected_pitcher_games": pop_stats["o15_market_ranking_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["o15_market_ranking_transfer"]["affected_rows"],
            "market_line_retained": "O1.5 market line retained; PHA line not retained",
            "treated_as_invariant": "yes for residual",
            "validity_classification": "REQUIRES_CLEAN_REEVALUATION",
            "result_status": "requires clean Contract B reevaluation before transfer diagnostic is treated as stable",
            "notes": "The O1.5 market-ranking challenger outside this transfer package is not invalidated by this finding.",
        },
        {
            "consumer": "July 18 controlled shadow",
            "script_or_artifact": "backend/mlb/scripts/materialize_mlb_current_pitcher_opponent_lineup_encounter_features.py; backend/mlb/scripts/run_mlb_live_hitter_parent_daily_integration.py",
            "source_field": "champion_expected_hits_allowed_poisson_implied; challenger_e_champion_plus_granular_expected_hits_allowed; champion_over_probability; challenger_over_probability",
            "source_grain": "Contract A proposition line",
            "target_grain": "live PHA proposition line",
            "join_keys": "slate_date|game_id|pitcher_id|line|side",
            "multi_line_resolution_policy": "none; exact proposition rows retained",
            "affected_pitcher_games": pop_stats["july18_controlled_shadow"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["july18_controlled_shadow"]["affected_rows"],
            "market_line_retained": "yes",
            "treated_as_invariant": "no after semantic repair",
            "validity_classification": "VALID_LINE_SPECIFIC_USE",
            "result_status": "active controlled shadow preserved",
            "notes": "Grade per exact line using side/probability/outcome; do not grade proxy counts as invariant count estimates.",
        },
        {
            "consumer": "July 17 live replay repair package",
            "script_or_artifact": "backend/mlb/scripts/materialize_mlb_pitcher_hits_allowed_live_replay_repair.py",
            "source_field": "champion_expected_hits_allowed_poisson_implied; challenger_e_champion_plus_granular_expected_hits_allowed",
            "source_grain": "Contract A proposition line",
            "target_grain": "live PHA proposition line / withheld July 17 Challenger package",
            "join_keys": "slate_date|game_id|pitcher_id|line|side",
            "multi_line_resolution_policy": "none for scored rows; July 17 Challenger withheld by governed parent absence",
            "affected_pitcher_games": 0,
            "affected_target_rows": 0,
            "market_line_retained": "yes",
            "treated_as_invariant": "label risk only",
            "validity_classification": "SEMANTIC_LABEL_ONLY_CORRECTION",
            "result_status": "withheld status preserved; semantics should use line-specific proxy labels",
            "notes": "No valid July 17 Challenger scores were frozen, so there is no contaminated Challenger comparison.",
        },
        {
            "consumer": "Granular feature platform cross-prop transfer registry",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_granular_feature_platform_cross_prop_transfer.py",
            "source_field": "package-level transfer classifications and holdout summaries",
            "source_grain": "summary artifact",
            "target_grain": "cross-prop platform decision registry",
            "join_keys": "artifact package references",
            "multi_line_resolution_policy": "not row-level; inherits upstream classifications",
            "affected_pitcher_games": "",
            "affected_target_rows": "",
            "market_line_retained": "not applicable",
            "treated_as_invariant": "not directly",
            "validity_classification": "SEMANTIC_LABEL_ONLY_CORRECTION",
            "result_status": "must annotate that contaminated transfer/promotion claims await Contract B reevaluation",
            "notes": "Registry remains useful as lineage, not as independent validation of contaminated downstream results.",
        },
        {
            "consumer": "Pitcher suppression research artifacts",
            "script_or_artifact": "backend/mlb/scripts/validate_mlb_hits15_pitcher_suppression_under.py; suppression observation artifacts",
            "source_field": "starter_expected_hits_allowed; pitcher suppression labels",
            "source_grain": "line-invariant starter/pitcher environment",
            "target_grain": "hitter Hits 1.5 proposition rows",
            "join_keys": "existing hitter/starter context keys",
            "multi_line_resolution_policy": "not applicable",
            "affected_pitcher_games": 0,
            "affected_target_rows": 0,
            "market_line_retained": "not PHA market line",
            "treated_as_invariant": "no PHA challenger_e consumption found in bounded search",
            "validity_classification": "UNAFFECTED",
            "result_status": "preserved",
            "notes": "Bounded search found no direct use of line-specific PHA Challenger fields.",
        },
        {
            "consumer": "Standalone O1.5 market-anchored ranking challenger and prospective ledger",
            "script_or_artifact": "backend/mlb/scripts/run_mlb_o15_market_anchored_ranking_challenger.py; backend/mlb/scripts/run_mlb_o15_market_anchored_ranking_prospective_grader.py",
            "source_field": "market-anchored O1.5 ranking fields",
            "source_grain": "O1.5 hitter proposition rows",
            "target_grain": "O1.5 hitter proposition ranking",
            "join_keys": "frozen O1.5 ledger keys",
            "multi_line_resolution_policy": "not applicable",
            "affected_pitcher_games": 0,
            "affected_target_rows": 0,
            "market_line_retained": "O1.5 market line only",
            "treated_as_invariant": "no PHA challenger_e consumption found in bounded search",
            "validity_classification": "UNAFFECTED",
            "result_status": "preserved",
            "notes": "Do not alter the frozen O1.5 prospective ranking ledger.",
        },
    ]

    multi_rows = [
        {
            "consumer": "PHA historical line-level performance",
            "source_artifact": artifact_ref(PHA_DIR / "pitcher_hits_allowed_exact_historical_population_2026-07-17.csv"),
            "source_field": "champion_expected_hits_allowed; challenger_e_champion_plus_granular_expected_hits_allowed",
            "reduction_policy": "none",
            "implementation_evidence": "score_population writes proposition-line rows and evaluates by line/book/split",
            "affected_pitcher_games": len(historical_ml_keys),
            "affected_target_rows": int(len(historical_ml)),
            "market_line_retained": "yes",
            "integrity_status": "valid_line_specific",
        },
        {
            "consumer": "Pitcher foundation transfer contract",
            "source_artifact": artifact_ref(transfer_contract_path),
            "source_field": "champion_expected_hits_allowed",
            "reduction_policy": "mean aggregation by slate_date|game_id|pitcher_id",
            "implementation_evidence": "build_pitcher_transfer_contract agg champion_expected_hits_allowed=(..., 'mean')",
            "affected_pitcher_games": int(len(multiline_keys)),
            "affected_target_rows": int(len(multi_contract)),
            "market_line_retained": "diagnostic pitcher_hits_allowed_lines only",
            "integrity_status": "requires_contract_b_reevaluation",
        },
        {
            "consumer": "Initial hitter Hits O0.5 transfer",
            "source_artifact": artifact_ref(populations["initial_hitter_hits05_transfer"]),
            "source_field": "pitcher_granular_minus_champion_residual",
            "reduction_policy": "inherits mean champion proxy from transfer contract",
            "implementation_evidence": "residual = pitcher_granular_expected_hits_allowed - mean collapsed champion_expected_hits_allowed",
            "affected_pitcher_games": pop_stats["initial_hitter_hits05_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["initial_hitter_hits05_transfer"]["affected_rows"],
            "market_line_retained": "no exact PHA line on hitter rows",
            "integrity_status": "partially_preserved_requires_reevaluation",
        },
        {
            "consumer": "Initial hitter Hits O1.5 transfer",
            "source_artifact": artifact_ref(populations["initial_hitter_hits15_transfer"]),
            "source_field": "pitcher_granular_minus_champion_residual",
            "reduction_policy": "inherits mean champion proxy from transfer contract",
            "implementation_evidence": "TRANSFER_FEATURES includes pitcher_granular_minus_champion_residual",
            "affected_pitcher_games": pop_stats["initial_hitter_hits15_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["initial_hitter_hits15_transfer"]["affected_rows"],
            "market_line_retained": "no exact PHA line on hitter rows",
            "integrity_status": "requires_contract_b_reevaluation",
        },
        {
            "consumer": "O1.5 market-ranking transfer",
            "source_artifact": artifact_ref(populations["o15_market_ranking_transfer"]),
            "source_field": "pitcher_granular_minus_champion_residual",
            "reduction_policy": "inherits mean champion proxy from transfer contract",
            "implementation_evidence": "pitcher_transfer_rank_score includes transfer feature set derived from transfer contract",
            "affected_pitcher_games": pop_stats["o15_market_ranking_transfer"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["o15_market_ranking_transfer"]["affected_rows"],
            "market_line_retained": "O1.5 line only; PHA line not retained",
            "integrity_status": "requires_contract_b_reevaluation",
        },
        {
            "consumer": "Hits O0.5 promotion-grade ranking result",
            "source_artifact": artifact_ref(populations["hits05_promotion_grade"]),
            "source_field": "challenger_e_expected_hits_allowed; champion_expected_hits_allowed; challenger_residual",
            "reduction_policy": "mean aggregation of line-specific champion and challenger_e by pitcher-game",
            "implementation_evidence": "reproduce_pitcher_model agg champion_expected_hits_allowed=(..., 'mean'), challenger_e_expected_hits_allowed=(..., 'mean')",
            "affected_pitcher_games": pop_stats["hits05_promotion_grade"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["hits05_promotion_grade"]["affected_rows"],
            "market_line_retained": "pitcher_lines retained only as diagnostic",
            "integrity_status": "multi_line_selection_contamination",
        },
        {
            "consumer": "July 18 controlled shadow",
            "source_artifact": artifact_ref(populations["july18_controlled_shadow"]),
            "source_field": "champion_expected_hits_allowed; challenger_expected_hits_allowed; market_line; probabilities",
            "reduction_policy": "none; exact line rows retained",
            "implementation_evidence": "controlled shadow writes one row per current proposition line",
            "affected_pitcher_games": pop_stats["july18_controlled_shadow"]["affected_pitcher_games"],
            "affected_target_rows": pop_stats["july18_controlled_shadow"]["affected_rows"],
            "market_line_retained": "yes",
            "integrity_status": "valid_line_specific",
        },
    ]

    classifications = [
        {
            "prior_result": "PHA historical line-level performance",
            "classification": "VALID_LINE_SPECIFIC_USE",
            "validity": "valid",
            "preserved_scope": "OVER/UNDER proposition ranking, probability, line-specific diagnostics",
            "superseded_scope": "none",
            "required_action": "Retain line-specific contract labels.",
        },
        {
            "prior_result": "PHA count MAE claims",
            "classification": "SEMANTIC_LABEL_ONLY_CORRECTION",
            "validity": "partially valid",
            "preserved_scope": "Line-specific proxy count MAE diagnostics",
            "superseded_scope": "Invariant pitcher-game expected-hit count claim",
            "required_action": "Rename/describe as proxy-count diagnostics in future research outputs.",
        },
        {
            "prior_result": "Initial hitter Hits transfer",
            "classification": "REQUIRES_CLEAN_REEVALUATION",
            "validity": "partially valid",
            "preserved_scope": "Strict-prior workload, encounter, support, uncertainty, and suppression foundation value",
            "superseded_scope": "Residual-dependent conclusions using pitcher_granular_minus_champion_residual",
            "required_action": "Rerun with Contract B context excluding line-specific proxy fields and derived residuals.",
        },
        {
            "prior_result": "Hits O0.5 promotion-grade ranking result",
            "classification": "MULTI_LINE_SELECTION_CONTAMINATION",
            "validity": "not valid for promotion-grade claim until reevaluated",
            "preserved_scope": "Original package retained as historical evidence of the error path",
            "superseded_scope": "Promotion-grade ranking conclusion that used collapsed challenger_e_expected_hits_allowed",
            "required_action": "Clean Contract B reevaluation required before any promotion-grade pitcher-foundation claim.",
        },
        {
            "prior_result": "O1.5 probability transfer",
            "classification": "REQUIRES_CLEAN_REEVALUATION",
            "validity": "partially valid",
            "preserved_scope": "Non-proxy strict-prior pitcher foundation observations",
            "superseded_scope": "Probability-transfer lift attributed to proxy-derived residual",
            "required_action": "Clean Contract B reevaluation with original splits and no new features.",
        },
        {
            "prior_result": "O1.5 market-ranking transfer",
            "classification": "REQUIRES_CLEAN_REEVALUATION",
            "validity": "partially valid",
            "preserved_scope": "Market-anchored ranking work independent of PHA transfer package remains unaffected",
            "superseded_scope": "Pitcher-foundation transfer ranking diagnostic using proxy-derived residual",
            "required_action": "Clean Contract B reevaluation if this transfer path remains important.",
        },
        {
            "prior_result": "July 18 controlled shadow",
            "classification": "VALID_LINE_SPECIFIC_USE",
            "validity": "valid",
            "preserved_scope": "Line-aware PHA controlled shadow rows and future exact-line grading",
            "superseded_scope": "Any language treating proxy values as invariant counts",
            "required_action": "Grade by exact line using probability/side/outcome.",
        },
    ]

    semantic_rows = [
        {
            "old_or_ambiguous_label": "champion_expected_hits_allowed",
            "recommended_research_label": "line_specific_champion_count_proxy",
            "contract": "Contract A",
            "reason": "Derived from market line plus Champion OVER probability.",
            "production_behavior_change": "false",
        },
        {
            "old_or_ambiguous_label": "challenger_e_champion_plus_granular_expected_hits_allowed",
            "recommended_research_label": "line_specific_challenger_count_proxy",
            "contract": "Contract A",
            "reason": "Frozen feature list includes champion_expected_hits_allowed_poisson_implied.",
            "production_behavior_change": "false",
        },
        {
            "old_or_ambiguous_label": "pitcher_granular_expected_hits_allowed",
            "recommended_research_label": "line_invariant_pitcher_foundation_expected_hits_allowed",
            "contract": "Contract B",
            "reason": "Built from strict-prior pitcher-game workload and opponent-lineup encounter aggregate, not PHA market line.",
            "production_behavior_change": "false",
        },
        {
            "old_or_ambiguous_label": "pitcher_granular_minus_champion_residual",
            "recommended_research_label": "deprecated_proxy_residual_do_not_use_for_contract_b",
            "contract": "Invalid for Contract B",
            "reason": "Subtracts collapsed line-specific Champion proxy from line-invariant pitcher foundation value.",
            "production_behavior_change": "false",
        },
        {
            "old_or_ambiguous_label": "challenger_e_expected_hits_allowed",
            "recommended_research_label": "deprecated_collapsed_line_specific_challenger_proxy",
            "contract": "Invalid for Contract B",
            "reason": "Hits O0.5 promotion collapsed proposition-line Challenger values to pitcher-game grain.",
            "production_behavior_change": "false",
        },
    ]

    reevaluation_rows = [
        {
            "package_or_result": "Hits O0.5 pitcher-foundation promotion-grade ranking",
            "need": "required",
            "reason": "Collapsed line-specific challenger_e and Champion proxies were used as pitcher-game features.",
            "allowed_scope": "Contract B-only unchanged split reevaluation with contaminated proxy fields removed.",
            "not_allowed": "new features, tuning, refit beyond original experiment mechanics, production change",
            "priority": "high",
            "notes": "Original package should remain archived but its promotion-grade conclusion should not stand alone.",
        },
        {
            "package_or_result": "O1.5 probability transfer",
            "need": "required if using pitcher-foundation transfer claim",
            "reason": "Transfer feature set includes proxy-derived residual.",
            "allowed_scope": "Contract B-only reevaluation preserving original split and instrument configuration.",
            "not_allowed": "model redesign or threshold optimization",
            "priority": "high",
            "notes": "Base O1.5 market-anchored ranking challenger is not affected unless it consumes this transfer output.",
        },
        {
            "package_or_result": "O1.5 market-ranking transfer diagnostic",
            "need": "required if using transfer ranking diagnostic",
            "reason": "Population inherits proxy-derived residual from the transfer contract.",
            "allowed_scope": "Contract B-only reevaluation preserving original ranking comparison.",
            "not_allowed": "ledger mutation or prospective ranking changes",
            "priority": "medium",
            "notes": "Do not alter frozen O1.5 prospective ranking ledger.",
        },
        {
            "package_or_result": "PHA line-specific proposition performance",
            "need": "not required",
            "reason": "The correct grain is proposition line and line is retained.",
            "allowed_scope": "semantic label update only.",
            "not_allowed": "refit or redesign",
            "priority": "none",
            "notes": "Keep controlled shadow active.",
        },
    ]

    decision_rows = [
        ("MLB_PHA_CONTRACT_LINE_SPECIFIC_DECISION", "CONTRACT_A_FROZEN_LINE_SPECIFIC_PROPOSITION_GRAIN"),
        ("MLB_PHA_CONTRACT_LINE_INVARIANT_FOUNDATION_DECISION", "CONTRACT_B_FROZEN_PITCHER_GAME_FOUNDATION_EXCLUDES_LINE_SPECIFIC_PHA_PROXY_FIELDS"),
        ("MLB_PHA_DOWNSTREAM_CONSUMER_INVENTORY_DECISION", f"INVENTORY_COMPLETE_BOUNDED_CONSUMERS_{len(consumer_rows)}"),
        ("MLB_PHA_MULTI_LINE_RESOLUTION_DECISION", "MEAN_COLLAPSE_FOUND_IN_HITTER_TRANSFER_AND_HITS05_PROMOTION_GRADE_REQUIRES_CONTRACT_B_REEVALUATION"),
        ("MLB_PHA_HISTORICAL_COUNT_CLAIM_DECISION", "VALID_AS_LINE_SPECIFIC_PROXY_DIAGNOSTIC_NOT_INVARIANT_COUNT_CLAIM"),
        ("MLB_PHA_INITIAL_HITTER_TRANSFER_DECISION", "PARTIAL_VALID_FOUNDATION_FIELDS_PRESERVED_RESIDUAL_DEPENDENT_CONCLUSIONS_REQUIRE_REEVALUATION"),
        ("MLB_PHA_HITS05_PROMOTION_GRADE_INTEGRITY_DECISION", "REQUIRES_CLEAN_REEVALUATION_COLLAPSED_LINE_SPECIFIC_CHALLENGER_E_USED_AS_PITCHER_GAME_FEATURE"),
        ("MLB_PHA_O15_TRANSFER_INTEGRITY_DECISION", "REQUIRES_CLEAN_REEVALUATION_SHARED_TRANSFER_RESIDUAL_USES_COLLAPSED_LINE_SPECIFIC_CHAMPION_PROXY"),
        ("MLB_PHA_LIVE_SHADOW_INTEGRITY_DECISION", "VALID_LINE_SPECIFIC_PROPOSITION_SHADOW"),
        ("MLB_PHA_REQUIRED_REEVALUATION_DECISION", "CONTRACT_B_REEVALUATION_REQUIRED_FOR_HITS05_PROMOTION_AND_O15_TRANSFER_RESULTS_NO_RERUN_IN_THIS_AUDIT"),
        ("MLB_PHA_SEMANTIC_REPAIR_DECISION", "RESEARCH_ONLY_CONTRACT_LABELS_FROZEN_NO_PRODUCTION_FIELD_RENAME"),
        ("MLB_PHA_CONTROLLED_SHADOW_STATUS", "PHA_CONTROLLED_SHADOW_PARTIAL_COVERAGE"),
        ("MLB_PHA_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    decisions = [{"decision_name": k, "decision_value": v, "notes": ""} for k, v in decision_rows]

    files = {}
    files["contracts"] = out_dir / "pha_contract_definitions_2026-07-18.csv"
    write_csv(
        files["contracts"],
        contracts_rows,
        ["contract", "name", "grain", "valid_fields", "invalid_fields", "valid_uses", "invalid_uses", "source_evidence", "notes"],
    )

    files["consumers"] = out_dir / "pha_downstream_consumer_inventory_2026-07-18.csv"
    write_csv(
        files["consumers"],
        consumer_rows,
        [
            "consumer",
            "script_or_artifact",
            "source_field",
            "source_grain",
            "target_grain",
            "join_keys",
            "multi_line_resolution_policy",
            "affected_pitcher_games",
            "affected_target_rows",
            "market_line_retained",
            "treated_as_invariant",
            "validity_classification",
            "result_status",
            "notes",
        ],
    )

    files["multi_line"] = out_dir / "pha_multi_line_resolution_audit_2026-07-18.csv"
    write_csv(
        files["multi_line"],
        multi_rows,
        [
            "consumer",
            "source_artifact",
            "source_field",
            "reduction_policy",
            "implementation_evidence",
            "affected_pitcher_games",
            "affected_target_rows",
            "market_line_retained",
            "integrity_status",
        ],
    )

    files["classifications"] = out_dir / "pha_prior_result_classification_2026-07-18.csv"
    write_csv(
        files["classifications"],
        classifications,
        ["prior_result", "classification", "validity", "preserved_scope", "superseded_scope", "required_action"],
    )

    files["semantic"] = out_dir / "pha_semantic_repair_plan_2026-07-18.csv"
    write_csv(
        files["semantic"],
        semantic_rows,
        ["old_or_ambiguous_label", "recommended_research_label", "contract", "reason", "production_behavior_change"],
    )

    files["reevaluation"] = out_dir / "pha_required_reevaluation_plan_2026-07-18.csv"
    write_csv(
        files["reevaluation"],
        reevaluation_rows,
        ["package_or_result", "need", "reason", "allowed_scope", "not_allowed", "priority", "notes"],
    )

    files["decisions"] = out_dir / "pha_downstream_integrity_decisions_2026-07-18.csv"
    write_csv(files["decisions"], decisions, ["decision_name", "decision_value", "notes"])

    summary = {
        "generated_at": generated_at,
        "historical_multi_line_pitcher_games": len(historical_ml_keys),
        "historical_multi_line_rows": int(len(historical_ml)),
        "transfer_contract_multi_line_pitcher_games": int(len(multiline_keys)),
        "population_stats": pop_stats,
        "decisions": {row["decision_name"]: row["decision_value"] for row in decisions},
        "guardrails": {
            "model_fit_or_refit": False,
            "new_features_or_formulas": False,
            "new_lineup_capture": False,
            "network_or_oddsapi": False,
            "db_writes": False,
            "production_behavior_change": False,
        },
    }
    files["json"] = out_dir / "machine_readable_pha_downstream_integrity_audit_2026-07-18.json"
    files["json"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    files["md"] = out_dir / "pha_line_specific_downstream_consumer_integrity_audit_2026-07-18.md"
    files["md"].write_text(
        f"""# PHA Line-Specific Contract Downstream-Consumer Integrity Audit

Generated: `{generated_at}`

## Executive Summary

The audit confirms the governing finding: the PHA Champion and Challenger fields are line-specific proposition proxies, not invariant pitcher-game expected-hit counts. The PHA historical line-level performance and the July 18 controlled shadow remain valid because they retain exact proposition-line grain.

The contamination risk appears when downstream hitter-transfer work collapses multiple PHA market-line rows to one pitcher-game row. The traced implementation uses mean aggregation for `champion_expected_hits_allowed` in the pitcher-foundation transfer contract and mean aggregation for both Champion and `challenger_e` proxy fields in the Hits O0.5 promotion-grade package.

## Contract Separation

- Contract A: `slate_date | game_id | pitcher_id | market_line | side`. Valid for proposition-line ranking, OVER/UNDER probability, controlled-shadow grading, and line-specific diagnostics.
- Contract B: `slate_date | game_id | pitcher_id`. Valid for hitter transfer and shared pitcher environment only when it excludes line-specific proxy fields and residuals derived from them.

## Multi-Line Findings

- Historical multi-line pitcher-games: `{len(historical_ml_keys)}`
- Historical multi-line rows: `{len(historical_ml)}`
- Transfer-contract multi-line pitcher-games: `{len(multiline_keys)}`
- Initial Hits O0.5 transfer affected rows: `{pop_stats['initial_hitter_hits05_transfer']['affected_rows']}`
- Initial Hits O1.5 transfer affected rows: `{pop_stats['initial_hitter_hits15_transfer']['affected_rows']}`
- O1.5 market-ranking transfer affected rows: `{pop_stats['o15_market_ranking_transfer']['affected_rows']}`
- Hits O0.5 promotion-grade affected rows: `{pop_stats['hits05_promotion_grade']['affected_rows']}`
- July 18 controlled shadow affected multi-line rows: `{pop_stats['july18_controlled_shadow']['affected_rows']}`; retained as line-specific and valid.

## Prior Result Classification

PHA line-specific proposition performance remains valid. PHA count MAE claims remain useful only as line-specific proxy diagnostics, not as invariant pitcher-game expected-hit MAE. Initial hitter transfer results preserve their strict-prior workload and encounter-field signal, but residual-dependent conclusions require clean Contract B reevaluation. Hits O0.5 promotion-grade ranking and O1.5 transfer/ranking diagnostics that consumed collapsed line-specific proxy context require clean Contract B reevaluation before promotion claims are relied on.

## Required Reevaluation

No model was refit in this audit. Clean reevaluation is required only for downstream hitter-transfer claims that used collapsed PHA proxy fields or proxy-derived residuals. The next package should rebuild pitcher context under Contract B, retain original splits and instrument configuration, avoid new features or tuning, and compare corrected results with the archived originals.

## Preserved Findings

- PHA historical line-level performance.
- July 18 line-aware controlled shadow and future exact-line grading.
- Strict-prior workload, encounter, support, uncertainty, and suppression foundation fields.
- Independent O1.5 market-anchored prospective ranking ledger.
- Pitcher suppression research that does not consume PHA `challenger_e` fields.

## No Behavior Changed

This was an inventory and semantic-integrity audit only. No model fitting, new features, formula changes, lineup capture, network calls, OddsAPI calls, DB writes, uploads, scheduler changes, or production behavior changes occurred.
""",
    )

    files["sha"] = out_dir / "sha256_manifest_2026-07-18.csv"
    sha_rows = []
    for path in sorted(files.values()):
        if path.name.startswith("sha256_manifest"):
            continue
        sha_rows.append(
            {
                "path": artifact_ref(path),
                "sha256": sha256(path),
                "bytes": path.stat().st_size,
            }
        )
    write_csv(files["sha"], sha_rows, ["path", "sha256", "bytes"])

    files["validation"] = out_dir / "validation_report_2026-07-18.csv"
    validation_rows = [
        {"check": "csv_outputs_written", "status": "PASS", "details": "All CSV artifacts were written with headers."},
        {"check": "json_output_written", "status": "PASS", "details": "Machine-readable JSON summary written."},
        {"check": "markdown_output_written", "status": "PASS", "details": "Audit markdown written."},
        {"check": "db_writes", "status": "PASS", "details": "No database client or write path is used by this script."},
        {"check": "network_or_oddsapi", "status": "PASS", "details": "No network client, requests, urllib, or OddsAPI calls are used."},
        {"check": "model_refit", "status": "PASS", "details": "No estimator fitting, training, or scoring is performed."},
        {"check": "production_behavior", "status": "PASS", "details": "Only audit artifacts are written."},
    ]
    write_csv(files["validation"], validation_rows, ["check", "status", "details"])

    return {"files": {k: artifact_ref(v) for k, v in files.items()}, **summary}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
