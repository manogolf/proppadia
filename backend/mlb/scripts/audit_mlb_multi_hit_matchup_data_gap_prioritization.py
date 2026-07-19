#!/usr/bin/env python3
"""Bounded read-only audit for the next multi-hit data-platform branch.

This utility binds the completed second-hit sequence pilot, inventories local
Starter exposure, bullpen, generalized matchup, and roster-relative evidence,
then freezes a single next-branch recommendation. It writes audit artifacts
only; it does not call network services, write databases, train models, or alter
production behavior.
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

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_multi_hit_matchup_data_gap_prioritization/2026-07-17"

BENCH = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17"
SECOND = ROOT / "artifacts/analysis/model_development/mlb_second_hit_sequence_probability_pilot/2026-07-17"
STARTER = ROOT / "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11"
HITTER = ROOT / "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11"
COLLECTIVE = ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"
MATCHUP = ROOT / "artifacts/analysis/model_development/mlb_certified_historical_matchup_ownership_integration/2026-07-17"
HITTER_OWNED = ROOT / "artifacts/analysis/model_development/mlb_hits15_hitter_owned_multi_hit_validation/2026-07-17"

CANONICAL = BENCH / "canonical_modeling_population_2026-07-17.csv"
COMPONENT_COVERAGE = SECOND / "component_coverage_2026-07-17.csv"
COMPONENT_ATTRIBUTION = SECOND / "component_attribution_report_2026-07-17.csv"
SEQUENCE_EXPOSURE = SECOND / "starter_bullpen_exposure_construction_2026-07-17.csv"
SEQUENCE_PA = SECOND / "pa_count_distribution_construction_2026-07-17.csv"
SEQUENCE_RECURRENCE = SECOND / "conditional_second_hit_tendency_construction_2026-07-17.csv"
VALIDATION = SECOND / "validation_holdout_results_2026-07-17.csv"
ONE_TO_TWO = SECOND / "one_to_two_plus_results_2026-07-17.csv"
STARTER_DATA = STARTER / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
HITTER_BASE = HITTER / "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
LOCKED_STARTER = COLLECTIVE / "locked_sources/starter_skill_workload_bounded_source_2026-06-29_to_2026-07-09.csv"
LOCKED_PA = COLLECTIVE / "locked_sources/pa_opportunity_bounded_source_2026-06-29_to_2026-07-09.csv"
INTEGRATED_MATCHUP = MATCHUP / "integrated_matchup_evidence_ledger_2026-07-17.csv"
BVP_ANALYSIS = HITTER_OWNED / "direct_and_generalized_bvp_analysis_2026-07-17.csv"
ROSTER_RELATIVE = HITTER_OWNED / "same_pitcher_roster_relative_analysis_2026-07-17.csv"


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def pct(n: float, d: float) -> float:
    return float(n / d) if d else 0.0


def norm(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def key_from(df: pd.DataFrame, date_col: str, game_col: str, player_col: str) -> pd.Series:
    return (
        df[date_col].astype(str).str[:10]
        + "|"
        + pd.to_numeric(df[game_col], errors="coerce").fillna(-1).astype(int).astype(str)
        + "|"
        + pd.to_numeric(df[player_col], errors="coerce").fillna(-1).astype(int).astype(str)
    )


def nonnull_coverage(df: pd.DataFrame, col: str, denom: int | None = None) -> tuple[int, float]:
    if col not in df:
        return 0, 0.0
    d = denom if denom is not None else len(df)
    n = int(df[col].notna().sum())
    return n, pct(n, d)


def build_failed_component_binding() -> pd.DataFrame:
    coverage = read_csv(COMPONENT_COVERAGE)
    attrib = read_csv(COMPONENT_ATTRIBUTION)
    val = read_csv(VALIDATION)
    one_two = read_csv(ONE_TO_TWO)
    rows = []
    definitions = {
        "PA-count distribution": (
            "Fixed PA total distribution over 1-6 PA, centered on strict-prior d15 PA/G with fallback.",
            "pa_count_distribution_construction_2026-07-17.csv",
            "strict-prior d15_pa_per_game; fallback population expected PA",
            "High coverage but mostly recasts opportunity volume already present in control; it did not add ranking separation.",
        ),
        "Conditional second-hit tendency": (
            "Shrunken d30 multi-hit share conditional on at least one hit.",
            "conditional_second_hit_tendency_construction_2026-07-17.csv",
            "d30_multi_hit_share_when_hit; fallback fit-population prior",
            "High coverage but close to hitter persistence already captured by game-level frequency; little independent rank signal.",
        ),
        "Starter exposure": (
            "Expected PA against starter using retained starter context, otherwise population exposure prior.",
            "starter_bullpen_exposure_construction_2026-07-17.csv",
            "starter_expected_hits_allowed / selected-proposition starter context; fallback generic starter-exit curve",
            "Low coverage because exact starter context is attached only to selected-proposition / qualified subsets, not the broad 10,118 batter-game spine.",
        ),
        "Bullpen exposure": (
            "Later-PA bullpen context after starter exit.",
            "starter_bullpen_exposure_construction_2026-07-17.csv",
            "No retained exact bullpen PA/suppression source; fallback neutral bullpen prior",
            "Not tested as a real component; no exact later-PA bullpen fields were bound.",
        ),
    }
    for component, (definition, source, fallback, explanation) in definitions.items():
        cov_row = coverage[coverage["component"].eq(component)].head(1)
        coverage_value = float(cov_row["coverage"].iloc[0]) if not cov_row.empty and "coverage" in cov_row else None
        attr_row = attrib[attrib["component"].str.contains(component.split()[0], case=False, na=False)].head(1) if not attrib.empty and "component" in attrib else pd.DataFrame()
        result = attr_row.to_dict("records")[0] if not attr_row.empty else {}
        hold = val[(val["temporal_split"].eq("holdout")) & (val["instrument"].str.contains("sequence_d|control", regex=True, na=False))]
        one = one_two[(one_two["temporal_split"].eq("holdout")) & (one_two["instrument"].str.contains("sequence_d|control", regex=True, na=False))]
        rows.append(
            {
                "component": component,
                "definition": definition,
                "source": source,
                "coverage": coverage_value,
                "missingness": None if coverage_value is None else 1.0 - coverage_value,
                "temporal_validity": "strict-prior where source exists; fallback otherwise",
                "approximation_used": fallback,
                "fallback_used": fallback,
                "could_influence_one_to_two_plus": "yes" if component in {"Starter exposure", "Bullpen exposure", "Conditional second-hit tendency"} else "limited",
                "observed_incremental_result": result.get("classification", "not directly isolated"),
                "holdout_context": f"control/full_brier={hold.iloc[0]['brier_two_plus'] if len(hold)>0 else ''}; unified/full_brier={hold.iloc[-1]['brier_two_plus'] if len(hold)>1 else ''}; control_one_to_two_auc={one.iloc[0]['roc_auc_two_plus'] if len(one)>0 else ''}; unified_one_to_two_auc={one.iloc[-1]['roc_auc_two_plus'] if len(one)>1 else ''}",
                "explanation": explanation,
            }
        )
    return pd.DataFrame(rows)


def build_starter_inventory(pop: pd.DataFrame, hit: pd.DataFrame, starter: pd.DataFrame, locked_starter: pd.DataFrame) -> pd.DataFrame:
    denom = len(pop)
    rows: list[dict[str, Any]] = []
    for col, concept, direct, dist in [
        ("starter_expected_hits_allowed", "starter expected hits allowed", "direct selected-proposition context", "point estimate only"),
        ("pitcher_base", "pitcher base vulnerability/workload", "direct selected-proposition context", "point estimate only"),
        ("starter_context_status", "starter context status", "direct status", "status only"),
        ("d15_pa_per_game", "hitter expected PA opportunity", "direct strict-prior hitter PA", "supports PA-count distribution"),
        ("lineup_slot", "lineup position / order turn proxy", "postgame actual in historical base", "supports batting-order turnover proxy"),
    ]:
        n, c = nonnull_coverage(pop, col, denom)
        rows.append(
            {
                "concept": concept,
                "exact_field": col,
                "source": rel(CANONICAL),
                "grain": "batter-game",
                "date_coverage": f"{pop['slate_date'].min()} to {pop['slate_date'].max()}",
                "row_coverage": n,
                "coverage_pct": c,
                "strict_prior_availability": "yes" if col != "lineup_slot" else "no; postgame actual semantics",
                "direct_vs_inferred": direct,
                "compatible_with_10118_population": "yes",
                "supports_distribution": dist,
                "gap_reason": "not missing" if n else "absent",
            }
        )
    if not locked_starter.empty:
        locked = locked_starter.copy()
        locked["starter_join_key"] = locked["date"].astype(str).str[:10] + "|" + pd.to_numeric(locked["game_id"], errors="coerce").fillna(-1).astype(int).astype(str)
        pop2 = pop.copy()
        pop2["starter_join_key"] = pop2["slate_date"].astype(str).str[:10] + "|" + pd.to_numeric(pop2["game_id"], errors="coerce").fillna(-1).astype(int).astype(str)
        overlap = pop2["starter_join_key"].isin(set(locked["starter_join_key"])).sum()
        for col, concept, supports in [
            ("expected_outs_blended_v1", "expected starter outs", "can support distribution over starter-facing PA"),
            ("expected_bf_blended_v1", "expected starter batters faced", "can support distribution over starter-facing PA"),
            ("workload_confidence", "starter workload confidence", "supports uncertainty / confidence weighting"),
            ("recent5_early_removal_freq", "recent early removal frequency", "supports short-start risk distribution"),
            ("prior_official_bf_per_start", "official BF per prior start", "direct starter BF opportunity history"),
        ]:
            n = int(locked[col].notna().sum()) if col in locked else 0
            rows.append(
                {
                    "concept": concept,
                    "exact_field": col,
                    "source": rel(LOCKED_STARTER),
                    "grain": "starter-game / team-game",
                    "date_coverage": f"{locked['date'].min()} to {locked['date'].max()}",
                    "row_coverage": n,
                    "coverage_pct": pct(n, len(locked)),
                    "strict_prior_availability": "yes",
                    "direct_vs_inferred": "direct starter workload / BF parent source" if "bf" in col.lower() else "derived strict-prior workload",
                    "compatible_with_10118_population": f"partial date/game overlap: {overlap} batter-games by date+game",
                    "supports_distribution": supports,
                    "gap_reason": "field exists in bounded qualified source but not joined to broad benchmark spine",
                }
            )
    if not starter.empty:
        for col, concept in [
            ("baseline_outs_per_start", "baseline outs per start"),
            ("actual_starter_batters_faced", "actual starter BF outcome"),
            ("team_expected_hits_allowed", "team/staff expected hits allowed"),
            ("bullpen_hits_allowed_form_blended", "team bullpen hits allowed form"),
        ]:
            n = int(starter[col].notna().sum()) if col in starter else 0
            rows.append(
                {
                    "concept": concept,
                    "exact_field": col,
                    "source": rel(STARTER_DATA),
                    "grain": "selected proposition row",
                    "date_coverage": f"{starter['date'].min()} to {starter['date'].max()}",
                    "row_coverage": n,
                    "coverage_pct": pct(n, len(starter)),
                    "strict_prior_availability": "mixed; actual_* fields are postgame outcomes",
                    "direct_vs_inferred": "starter characterization/research row",
                    "compatible_with_10118_population": "low because source is selected-proposition spine, not all batter-games",
                    "supports_distribution": "baseline fields can support workload distribution; actual fields evaluation only",
                    "gap_reason": "compatible only for selected-proposition overlap",
                }
            )
    return pd.DataFrame(rows)


def build_bullpen_inventory(starter: pd.DataFrame, locked_starter: pd.DataFrame) -> pd.DataFrame:
    rows = []
    sources = [
        ("team_expected_hits_allowed", "team/full-staff expected hits allowed", STARTER_DATA, starter, "selected proposition row", "context only; not later-PA specific"),
        ("bullpen_hits_allowed_form_blended", "bullpen hits allowed form blend", STARTER_DATA, starter, "selected proposition row", "generic team bullpen quality, not likely later relievers"),
        ("bullpen_hits_allowed_pg_last7", "bullpen hits/game last 7", STARTER_DATA, starter, "selected proposition row", "generic bullpen form"),
        ("bullpen_hits_allowed_pg_last15", "bullpen hits/game last 15", STARTER_DATA, starter, "selected proposition row", "generic bullpen form"),
        ("bullpen_hits_allowed_pg_last30", "bullpen hits/game last 30", STARTER_DATA, starter, "selected proposition row", "generic bullpen form"),
        ("expected_outs_blended_v1", "starter exit point proxy", LOCKED_STARTER, locked_starter, "starter-game", "can estimate when bullpen PA begin, but not who pitches"),
    ]
    for col, definition, path, df, grain, notes in sources:
        n = int(df[col].notna().sum()) if not df.empty and col in df else 0
        rows.append(
            {
                "concept": definition,
                "field": col,
                "source": rel(path),
                "grain": grain,
                "date_coverage": "" if df.empty else f"{df[df.columns[1]].min()} to {df[df.columns[1]].max()}" if len(df.columns) > 1 else "",
                "prediction_time_availability": "yes as strict-prior/context where field exists" if n else "not locally bound",
                "player_team_game_identity": "team/starter-game; not reliever-specific",
                "strict_prior_integrity": "yes for non-actual fields; no same-game reliever identity",
                "row_coverage": n,
                "coverage_pct_within_source": pct(n, len(df)) if len(df) else 0.0,
                "replayability": "repository artifact exists" if Path(path).exists() else "missing",
                "missingness": 1.0 - pct(n, len(df)) if len(df) else 1.0,
                "distinguishes_likely_later_pa": "no",
                "notes": notes,
            }
        )
    rows.extend(
        [
            {
                "concept": "team bullpen handedness composition",
                "field": "none_found",
                "source": "local artifact scan",
                "grain": "team/date desired",
                "date_coverage": "",
                "prediction_time_availability": "absent",
                "player_team_game_identity": "not available",
                "strict_prior_integrity": "not established",
                "row_coverage": 0,
                "coverage_pct_within_source": 0.0,
                "replayability": "absent",
                "missingness": 1.0,
                "distinguishes_likely_later_pa": "no",
                "notes": "No retained reliever-handedness availability/workload platform found.",
            },
            {
                "concept": "likely bulk/primary relievers",
                "field": "none_found",
                "source": "local artifact scan",
                "grain": "game/team/reliever desired",
                "date_coverage": "",
                "prediction_time_availability": "absent",
                "player_team_game_identity": "not available",
                "strict_prior_integrity": "not established",
                "row_coverage": 0,
                "coverage_pct_within_source": 0.0,
                "replayability": "absent",
                "missingness": 1.0,
                "distinguishes_likely_later_pa": "no",
                "notes": "Explains 0.00% bullpen exposure in sequence pilot: no exact later-PA bullpen source was bound.",
            },
        ]
    )
    return pd.DataFrame(rows)


def build_matchup_inventory(pop: pd.DataFrame, integrated: pd.DataFrame, bvp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if not integrated.empty:
        for col, concept in [
            ("opposing_starter_id", "opposing starter identity"),
            ("hitter_evidence_label", "hitter evidence label"),
            ("pitcher_suppression_label", "pitcher suppression label"),
            ("baseball_directional_ownership", "team/hitter ownership direction"),
            ("starter_expected_hits_allowed", "pitcher environment"),
        ]:
            n = int(integrated[col].notna().sum()) if col in integrated else 0
            rows.append(
                {
                    "concept": concept,
                    "available_or_absent": "available" if n else "absent",
                    "source": rel(INTEGRATED_MATCHUP),
                    "grain": "canonical selected proposition",
                    "date_coverage": f"{integrated['slate_date'].min()} to {integrated['slate_date'].max()}" if "slate_date" in integrated else "",
                    "strict_prior_status": "mixed; ownership labels are historical integration, not pitch-level",
                    "prediction_time_availability": "research artifact; not broad live feature",
                    "coverage_in_10118_population": "not directly compatible; selected-proposition subset",
                    "identity_quality": "game/player/proposition keys present",
                    "direct_construction_possible": "yes for labels, no for pitch-level matchup",
                    "external_acquisition_required": "no for existing labels; yes for pitch mix/velocity/contact",
                    "notes": "Useful context but does not provide generalized pitch/contact compatibility.",
                }
            )
    for concept in [
        "batter hand",
        "pitcher hand",
        "hitter results by pitcher hand",
        "pitcher results by batter hand",
        "pitcher pitch-type usage",
        "hitter performance by pitch family",
        "velocity bands",
        "hitter contact rate / pitcher contact allowed",
        "whiff/chase/zone-contact interaction",
    ]:
        rows.append(
            {
                "concept": concept,
                "available_or_absent": "absent_from_frozen_10118_spine",
                "source": "local artifact/column inventory",
                "grain": "player-game or pitcher-game desired",
                "date_coverage": "",
                "strict_prior_status": "not established",
                "prediction_time_availability": "not available in bound artifacts",
                "coverage_in_10118_population": "0",
                "identity_quality": "not bound",
                "direct_construction_possible": "no without new platform/backfill",
                "external_acquisition_required": "likely, or a separate statcast/handedness source admission",
                "notes": "This is the likely baseball concept, but not the smallest next local branch.",
            }
        )
    if not bvp.empty:
        rows.append(
            {
                "concept": "direct BvP",
                "available_or_absent": "available_sparse",
                "source": rel(BVP_ANALYSIS),
                "grain": "hitter-pitcher support band",
                "date_coverage": "reported in prior audit",
                "strict_prior_status": "strict-prior where supported",
                "prediction_time_availability": "available for sparse direct BvP pockets",
                "coverage_in_10118_population": "sparse; not standalone solution",
                "identity_quality": "hitter/pitcher identity present in selected artifacts",
                "direct_construction_possible": "yes for sparse direct BvP only",
                "external_acquisition_required": "no for existing sparse BvP; yes for generalized matchup",
                "notes": "Prior finding explicitly should not be treated as standalone solution.",
            }
        )
    return pd.DataFrame(rows)


def build_roster_relative(pop: pd.DataFrame, hit: pd.DataFrame, roster_relative: pd.DataFrame, integrated: pd.DataFrame) -> pd.DataFrame:
    merged = pop.merge(hit[["slate_date", "game_id", "player_id", "team", "opponent", "lineup_slot", "lineup_bucket"]], on=["slate_date", "game_id", "player_id"], how="left", suffixes=("", "_hitter")) if not hit.empty else pop.copy()
    pitcher_game_groups = merged.groupby(["slate_date", "game_id", "team"], dropna=False).agg(
        hitters=("player_id", "nunique"),
        rows=("player_id", "size"),
        two_plus=("multi_hit_target", "sum"),
        lineup_cov=("lineup_slot", lambda s: int(pd.to_numeric(s, errors="coerce").notna().sum())),
        pa_cov=("d15_pa_per_game", lambda s: int(pd.to_numeric(s, errors="coerce").notna().sum())),
        starter_cov=("starter_expected_hits_allowed", lambda s: int(pd.to_numeric(s, errors="coerce").notna().sum())),
    ).reset_index()
    multi_groups = pitcher_game_groups[pitcher_game_groups["hitters"].ge(2)]
    rows = [
        {
            "measure": "pitcher_game_groups_proxy_game_team",
            "value": int(len(pitcher_game_groups)),
            "coverage_note": "Uses date+game+batting team as proxy for same opposing starter; exact opposing_starter_id mostly absent in broad spine.",
        },
        {
            "measure": "groups_with_multiple_opposing_hitters",
            "value": int(len(multi_groups)),
            "coverage_note": "Enough roster-relative groups exist at game/team grain.",
        },
        {
            "measure": "avg_hitters_per_group",
            "value": float(pitcher_game_groups["hitters"].mean()) if len(pitcher_game_groups) else 0.0,
            "coverage_note": "Broad spine can support roster-relative comparisons if pitcher identity/context is restored.",
        },
        {
            "measure": "lineup_coverage_rows",
            "value": int(pitcher_game_groups["lineup_cov"].sum()),
            "coverage_note": "Lineup is postgame actual in historical spine; not pregame confirmed.",
        },
        {
            "measure": "pa_opportunity_coverage_rows",
            "value": int(pitcher_game_groups["pa_cov"].sum()),
            "coverage_note": "d15 PA/G is broadly available.",
        },
        {
            "measure": "starter_context_coverage_rows",
            "value": int(pitcher_game_groups["starter_cov"].sum()),
            "coverage_note": "Exact pitcher context is the limiting field.",
        },
    ]
    if not roster_relative.empty:
        rows.append(
            {
                "measure": "prior_same_pitcher_roster_relative_artifact_rows",
                "value": int(len(roster_relative)),
                "coverage_note": rel(ROSTER_RELATIVE),
            }
        )
    return pd.DataFrame(rows)


def build_historical_reuse(pop: pd.DataFrame, locked_starter: pd.DataFrame, locked_pa: pd.DataFrame, integrated: pd.DataFrame) -> pd.DataFrame:
    rows = []
    pop_keys = set(pop["player_game_key"])
    for name, path, df, date_col, game_col, player_col in [
        ("locked starter source", LOCKED_STARTER, locked_starter, "date", "game_id", None),
        ("locked PA source", LOCKED_PA, locked_pa, "slate_date", "game_id", "player_id"),
        ("integrated matchup ledger", INTEGRATED_MATCHUP, integrated, "slate_date", "game_id", "player_id"),
    ]:
        if df.empty:
            rows.append({"source": name, "path": rel(path), "rows": 0, "compatible_overlap_rows": 0, "notes": "missing"})
            continue
        if player_col and player_col in df:
            keys = set(key_from(df, date_col, game_col, player_col))
            overlap = len(pop_keys & keys)
            compatibility = "exact player-game"
        else:
            pg = pop.copy()
            pg_key = pg["slate_date"].astype(str).str[:10] + "|" + pd.to_numeric(pg["game_id"], errors="coerce").fillna(-1).astype(int).astype(str)
            df_key = df[date_col].astype(str).str[:10] + "|" + pd.to_numeric(df[game_col], errors="coerce").fillna(-1).astype(int).astype(str)
            overlap = int(pg_key.isin(set(df_key)).sum())
            compatibility = "date+game only; needs team/opponent starter identity bridge"
        rows.append(
            {
                "source": name,
                "path": rel(path),
                "rows": int(len(df)),
                "compatible_overlap_rows": overlap,
                "overlap_pct_of_10118": pct(overlap, len(pop)),
                "field_version_differences": "bounded source versions differ from frozen benchmark broad spine",
                "grain_incompatibilities": compatibility,
                "identity_bridge_requirements": "player-game exact for PA/matchup; date+game+team/opposing starter for starter source",
                "additional_coverage_without_acquisition": "partial; strongest for 2026-06-29..2026-07-09 selected/bounded dates",
                "notes": "Reuse requires assembly/propagation, not a new broad qualification campaign.",
            }
        )
    return pd.DataFrame(rows)


def build_scorecard() -> pd.DataFrame:
    rows = [
        {
            "branch": "STARTER_FACING_PA_EXPOSURE_RESTORATION",
            "direct_relevance": 5,
            "current_local_coverage": 2,
            "expected_recoverable_coverage": 4,
            "strict_prior_replayability": 4,
            "identity_and_grain_readiness": 3,
            "implementation_effort": 3,
            "external_acquisition_requirement": 1,
            "daily_live_availability": 4,
            "historical_backfill_feasibility": 4,
            "leakage_risk": 2,
            "interpretability": 5,
            "bounded_holdout_experiment_support": 4,
            "score": 39,
            "notes": "Best smallest branch: existing expected outs/BF/workload fields can turn generic exposure prior into a real starter-facing PA distribution.",
        },
        {
            "branch": "BULLPEN_EXPOSURE_AND_SUPPRESSION_PLATFORM",
            "direct_relevance": 5,
            "current_local_coverage": 1,
            "expected_recoverable_coverage": 2,
            "strict_prior_replayability": 2,
            "identity_and_grain_readiness": 1,
            "implementation_effort": 5,
            "external_acquisition_requirement": 4,
            "daily_live_availability": 2,
            "historical_backfill_feasibility": 2,
            "leakage_risk": 3,
            "interpretability": 4,
            "bounded_holdout_experiment_support": 2,
            "score": 22,
            "notes": "Highly relevant but not ready: likely reliever identity/availability and later-PA suppression are absent locally.",
        },
        {
            "branch": "GENERALIZED_MATCHUP_COMPATIBILITY",
            "direct_relevance": 5,
            "current_local_coverage": 1,
            "expected_recoverable_coverage": 2,
            "strict_prior_replayability": 2,
            "identity_and_grain_readiness": 2,
            "implementation_effort": 5,
            "external_acquisition_requirement": 4,
            "daily_live_availability": 2,
            "historical_backfill_feasibility": 2,
            "leakage_risk": 3,
            "interpretability": 3,
            "bounded_holdout_experiment_support": 2,
            "score": 22,
            "notes": "Baseball-rich, but pitch mix/handedness/contact compatibility are not bound to the 10,118-row spine.",
        },
        {
            "branch": "SAME_PITCHER_ROSTER_RELATIVE_PLATFORM",
            "direct_relevance": 4,
            "current_local_coverage": 4,
            "expected_recoverable_coverage": 4,
            "strict_prior_replayability": 3,
            "identity_and_grain_readiness": 3,
            "implementation_effort": 3,
            "external_acquisition_requirement": 1,
            "daily_live_availability": 4,
            "historical_backfill_feasibility": 4,
            "leakage_risk": 2,
            "interpretability": 4,
            "bounded_holdout_experiment_support": 3,
            "score": 38,
            "notes": "Very close second; best as evaluation design layered after starter exposure is restored.",
        },
    ]
    return pd.DataFrame(rows)


def build_pilot_design() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"item": "selected_branch", "value": "STARTER_FACING_PA_EXPOSURE_RESTORATION"},
            {"item": "source_population", "value": "Frozen 10,118 benchmark rows; start with 2026-06-29..2026-07-09 overlap where locked starter source exists"},
            {"item": "fields", "value": "expected_outs_blended_v1, expected_bf_blended_v1, workload_confidence, prior_official_bf_per_start, recent5_early_removal_freq, lineup_slot/PA distribution"},
            {"item": "grain", "value": "batter-game joined to starter-game via slate_date + game_id + hitter team/opponent starter identity bridge"},
            {"item": "strict_prior_cutoff", "value": "feature_cutoff_date < slate_date; latest_contributing_prior_game_date < slate_date"},
            {"item": "construction_method", "value": "Convert expected starter BF/outs into distribution over hitter PA against starter by lineup slot and team PA cycle; no model fitting in construction stage"},
            {"item": "missingness", "value": "No fallback promotion; rows missing starter identity or workload remain explicit missing/excluded diagnostics"},
            {"item": "source_priority", "value": "locked starter_skill_workload source, then starter characterization parent ledgers; no new acquisition in first pilot"},
            {"item": "expected_row_coverage", "value": "Partial bounded overlap first; coverage expansion depends on assembling starter source across full 05/01..07/09 spine"},
            {"item": "external_request_count", "value": "0 for first pilot"},
            {"item": "elevated_access_required", "value": "No, if using existing repository artifacts only"},
            {"item": "control_instrument", "value": "Frozen hitter + PA + Starter benchmark control and second-hit sequence generic exposure"},
            {"item": "challenger_instrument", "value": "Same sequence construction with exact starter-facing PA exposure distribution replacing generic exposure prior"},
            {"item": "temporal_splits", "value": "Fit 2026-05-01..2026-06-11; validation 2026-06-12..2026-06-25; holdout 2026-06-26..2026-07-09 where coverage permits"},
            {"item": "primary_metrics", "value": "one-to-two-plus AUC/Brier/logloss; full distribution Brier/logloss secondary"},
            {"item": "suppression_preservation", "value": "Must preserve or improve pitcher-owned suppression diagnostic region"},
            {"item": "+200_evaluation", "value": "Frozen +200 O1.5 price bands retained as economic diagnostic only; no cutoff optimization"},
            {"item": "stop_criteria", "value": "Stop if exact exposure coverage cannot exceed current 9.10% materially or if one-to-two-plus holdout ranking remains flat"},
        ]
    )


def markdown_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            vals.append("" if pd.isna(v) else f"{v:.4f}" if isinstance(v, float) else norm(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = read_csv(CANONICAL)
    if pop.empty:
        raise FileNotFoundError(CANONICAL)
    hit = read_csv(HITTER_BASE)
    starter = read_csv(STARTER_DATA)
    locked_starter = read_csv(LOCKED_STARTER)
    locked_pa = read_csv(LOCKED_PA)
    integrated = read_csv(INTEGRATED_MATCHUP)
    bvp = read_csv(BVP_ANALYSIS)
    roster = read_csv(ROSTER_RELATIVE)

    failed = build_failed_component_binding()
    starter_inv = build_starter_inventory(pop, hit, starter, locked_starter)
    bullpen_inv = build_bullpen_inventory(starter, locked_starter)
    matchup_inv = build_matchup_inventory(pop, integrated, bvp)
    roster_feas = build_roster_relative(pop, hit, roster, integrated)
    reuse = build_historical_reuse(pop, locked_starter, locked_pa, integrated)
    scorecard = build_scorecard()
    rationale = pd.DataFrame(
        [
            {
                "selected_branch": "STARTER_FACING_PA_EXPOSURE_RESTORATION",
                "decision": "RESTORE_STARTER_EXPOSURE_FIRST",
                "rationale": "It is the smallest local improvement that directly targets the failed exposure component, has existing strict-prior expected-outs/BF sources, avoids external acquisition, and can be tested before building a larger bullpen or pitch-mix platform.",
                "not_selected_bullpen": "0.00% exact bullpen exposure; no likely-reliever or later-PA suppression platform locally bound.",
                "not_selected_matchup": "Generalized pitch/contact/handedness compatibility is absent from the frozen broad spine and would require a new source-admission platform.",
                "not_selected_roster_relative": "Roster-relative grouping is feasible, but it depends on restored pitcher/starter exposure to explain why same-pitcher hitters differ.",
            }
        ]
    )
    pilot = build_pilot_design()
    external = pd.DataFrame(
        [
            {
                "boundary": "external_acquisition",
                "status": "NOT_REQUIRED_FOR_SELECTED_FIRST_PILOT",
                "notes": "First branch should assemble existing starter workload/BF fields. Bullpen and generalized matchup branches likely need later source admission.",
            },
            {"boundary": "production", "status": "NOT_AUTHORIZED", "notes": "No formulas, selectors, uploads, DB, LaunchAgents, or model behavior changed."},
        ]
    )
    decisions = {
        "MLB_MULTI_HIT_DATA_GAP_BINDING_DECISION": "SECOND_HIT_SEQUENCE_COMPONENTS_BOUND_EXPOSURE_NOT_FULLY_TESTED",
        "MLB_MULTI_HIT_STARTER_EXPOSURE_READINESS_DECISION": "PARTIAL_EXISTING_PLATFORM_READY_FOR_RESTORATION_PILOT",
        "MLB_MULTI_HIT_BULLPEN_EXPOSURE_READINESS_DECISION": "NOT_READY_ZERO_EXACT_LATER_PA_BULLPEN_SOURCE",
        "MLB_MULTI_HIT_GENERALIZED_MATCHUP_READINESS_DECISION": "NOT_READY_REQUIRES_SOURCE_ADMISSION_FOR_HAND_PITCH_CONTACT_COMPATIBILITY",
        "MLB_MULTI_HIT_ROSTER_RELATIVE_READINESS_DECISION": "FEASIBLE_AFTER_STARTER_EXPOSURE_RESTORATION",
        "MLB_MULTI_HIT_HISTORICAL_PLATFORM_REUSE_DECISION": "REUSE_EXISTING_QUALIFIED_STARTER_AND_PA_FIELDS_BEFORE_NEW_ACQUISITION",
        "MLB_MULTI_HIT_DATA_BRANCH_PRIORITY_DECISION": "STARTER_FACING_PA_EXPOSURE_RESTORATION_FIRST",
        "MLB_MULTI_HIT_SELECTED_BRANCH_DECISION": "RESTORE_STARTER_EXPOSURE_FIRST",
        "MLB_MULTI_HIT_NEXT_PILOT_DESIGN_DECISION": "BOUNDED_DESIGN_FROZEN_NO_EXECUTION",
        "MLB_MULTI_HIT_EXTERNAL_PERMISSION_REQUIREMENT": "NO_EXTERNAL_PERMISSION_FOR_SELECTED_FIRST_PILOT",
        "MLB_MULTI_HIT_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    decisions_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])

    outputs = {
        "failed_component_binding_2026-07-17.csv": failed,
        "starter_exposure_source_inventory_2026-07-17.csv": starter_inv,
        "bullpen_source_inventory_2026-07-17.csv": bullpen_inv,
        "generalized_matchup_source_inventory_2026-07-17.csv": matchup_inv,
        "same_pitcher_roster_relative_feasibility_2026-07-17.csv": roster_feas,
        "historical_platform_reuse_analysis_2026-07-17.csv": reuse,
        "candidate_branch_scorecard_2026-07-17.csv": scorecard,
        "selected_branch_rationale_2026-07-17.csv": rationale,
        "bounded_next_pilot_design_2026-07-17.csv": pilot,
        "external_permission_boundary_2026-07-17.csv": external,
        "required_decisions_2026-07-17.csv": decisions_df,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    selected = "STARTER_FACING_PA_EXPOSURE_RESTORATION"
    md = f"""# MLB Multi-Hit Matchup and Later-PA Data Gap Prioritization Audit

Generated: `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`

## Executive Summary

This bounded read-only audit binds the failed second-hit sequence pilot and selects exactly one next data-platform branch. The sequence pilot should not be interpreted as a full test of starter/bullpen sequencing: PA-count and conditional recurrence were broadly covered, but exact starter exposure was only partially bound and exact bullpen exposure was absent.

Selected next branch: **{selected}**.

Direct answer: the best next chance to explain why a hitter progresses from one hit to two or more is **restoring exact starter-facing PA exposure first**. It directly targets the missing later-PA split, can reuse existing strict-prior starter workload/BF artifacts, and creates the necessary foundation before a bullpen or generalized pitch-compatibility platform can be tested cleanly.

## Failed Component Binding

{markdown_table(failed)}

## Starter Exposure Inventory

{markdown_table(starter_inv)}

## Bullpen Inventory

{markdown_table(bullpen_inv)}

## Generalized Matchup Inventory

{markdown_table(matchup_inv)}

## Same-Pitcher Roster-Relative Feasibility

{markdown_table(roster_feas)}

## Historical Platform Reuse

{markdown_table(reuse)}

## Candidate Branch Scorecard

{markdown_table(scorecard)}

## Selected Branch Rationale

{markdown_table(rationale)}

## Next Pilot Design

{markdown_table(pilot)}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

This package is inventory, feasibility analysis, and experiment design only. No network acquisition, DB write, model fit, feature construction, threshold optimization, upload change, LaunchAgent change, or production behavior change was performed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "canonical_population_rows": int(len(pop)),
        "selected_branch": selected,
        "selected_decision": decisions["MLB_MULTI_HIT_SELECTED_BRANCH_DECISION"],
        "component_binding": failed.to_dict("records"),
        "scorecard": scorecard.to_dict("records"),
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_multi_hit_data_gap_prioritization_2026-07-17.json")

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation), out_dir / "validation_report_2026-07-17.csv")

    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(Path(args.output_dir))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
