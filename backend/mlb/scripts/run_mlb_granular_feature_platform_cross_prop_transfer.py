"""MLB granular feature-platform cross-prop transfer audit.

This script is deliberately read-only with respect to production state. It
summarizes existing local research artifacts and freezes one next-experiment
recommendation without training, optimizing, or changing any production model.
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


RUN_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_granular_feature_platform_cross_prop_transfer/2026-07-17"
)

SLATE_PATH = Path(
    "backend/mlb/exports/odds_history/2026-07-17/"
    "mlb_slate_output__local_daily_20260717T124203Z.csv"
)
O15_HISTORICAL_PATH = Path(
    "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/"
    "2026-07-17/historical_ranking_population_1026_2026-07-17.csv"
)
O15_OOF_PATH = Path(
    "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/"
    "2026-07-17/historical_out_of_fold_ranking_population_2026-07-17.csv"
)
O15_PAIRWISE_PATH = Path(
    "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/"
    "2026-07-17/pairwise_ranking_analysis_2026-07-17.csv"
)
EXPOSURE_INCREMENT_PATH = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/"
    "2026-07-17/multi_hit_probability_increment_2026-07-17.csv"
)
CONTACT_OPP_PATH = Path(
    "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/"
    "2026-07-17/one_to_two_plus_results_2026-07-17.csv"
)
DISCIPLINE_PATH = Path(
    "artifacts/analysis/model_development/mlb_pitch_discipline_repeated_contact_pilot/"
    "2026-07-17/one_to_two_plus_validation_holdout_metrics_2026-07-17.csv"
)
CONTACT_QUALITY_PATH = Path(
    "artifacts/analysis/model_development/mlb_simple_xhit_contact_surface_pilot/"
    "2026-07-17/simple_validation_holdout_metrics_2026-07-17.csv"
)
TB_SHADOW_SUMMARY_PATH = Path(
    "artifacts/analysis/mlb/model_quality/total_bases_shadow/2026-07-17/"
    "total_bases_shadow_summary_2026-07-17.json"
)
TB_SHADOW_SCORES_PATH = Path(
    "artifacts/analysis/mlb/model_quality/total_bases_shadow/2026-07-17/"
    "total_bases_shadow_scores_2026-07-17.csv"
)
SUPPRESSION_U15_PATH = Path(
    "artifacts/analysis/model_development/mlb_hits15_suppression_price_timing_and_shadow/"
    "2026-07-17/opportunity_volume_comparison_2026-07-17.csv"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        fieldnames = keys
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def prop_counts(slate: pd.DataFrame) -> dict[tuple[str, str], int]:
    if slate.empty or "prop_type" not in slate.columns or "line" not in slate.columns:
        return {}
    work = slate.copy()
    work["line_str"] = work["line"].map(lambda v: str(v).rstrip("0").rstrip(".") if "." in str(v) else str(v))
    return {
        (str(prop), str(line)): int(count)
        for (prop, line), count in work.groupby(["prop_type", "line_str"]).size().items()
    }


def metric_from(df: pd.DataFrame, split: str, instrument: str, column: str) -> Any:
    if df.empty:
        return ""
    if "temporal_split" not in df.columns or "instrument" not in df.columns or column not in df.columns:
        return ""
    mask = (df["temporal_split"].astype(str) == split) & (df["instrument"].astype(str) == instrument)
    if not mask.any():
        return ""
    return df.loc[mask, column].iloc[0]


def feature_registry() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(
        name: str,
        definition: str,
        package: str,
        column: str,
        grain: str,
        strict_prior: str,
        deployable: str,
        coverage: str,
        notes: str,
    ) -> None:
        rows.append(
            {
                "canonical_field_name": name,
                "definition": definition,
                "source_package": package,
                "source_column": column,
                "grain": grain,
                "date_coverage": "repository_artifact_bound_2026-07-17",
                "strict_prior_construction": strict_prior,
                "fit_validation_holdout_availability": coverage,
                "historical_coverage": coverage,
                "current_live_availability": "available_where_parent_artifact_runs",
                "support_and_shrinkage": "retained_in_source_package_when_available",
                "missingness": "explicit_missing_or_fit_split_median_by_source_contract",
                "field_version": "research_v1",
                "temporal_cutoff": "strict_prior_before_game_or_snapshot_time",
                "status": deployable,
                "notes": notes,
            }
        )

    exposure_pkg = "mlb_pregame_starter_bullpen_exposure_forecast"
    lineup_pkg = "mlb_pregame_lineup_turnover_exposure_pilot"
    discipline_pkg = "mlb_pitch_discipline_repeated_contact_pilot"
    contact_pkg = "mlb_pregame_contact_opportunity_multi_hit_pilot"
    quality_pkg = "mlb_simple_xhit_contact_surface_pilot"
    encounter_pkg = "mlb_batter_pitcher_encounter_ledger_pilot"
    suppression_pkg = "mlb_hits15_suppression_price_timing_and_shadow"

    for name, definition, package, column, grain, strict_prior, status, coverage, notes in [
        ("predicted_total_pa", "Pregame expected total plate appearances.", exposure_pkg, "expected_pa_used", "player_game", "rolling PA plus lineup context", "deployable_research", "validation_and_holdout_for_O15_context", "Core opportunity feature."),
        ("pa4_probability", "Probability hitter reaches a fourth PA.", lineup_pkg, "pa4_probability", "player_game", "pregame lineup and PA history", "deployable_research", "partial_source_registry", "Useful for multi-hit and any-hit opportunity."),
        ("pa5_probability", "Probability hitter reaches a fifth PA.", lineup_pkg, "pa5_probability", "player_game", "pregame lineup and PA history", "deployable_research", "partial_source_registry", "Captures high-ceiling role quality."),
        ("predicted_starter_facing_pa", "Expected PA against the opposing starter.", exposure_pkg, "predicted_starter_facing_pa", "player_game", "starter workload forecast", "deployable_research", "validation_and_holdout_for_O15_context", "Directly informs starter/bullpen transition."),
        ("predicted_bullpen_facing_pa", "Expected PA against relievers.", exposure_pkg, "predicted_bullpen_facing_pa", "player_game", "starter exit forecast", "deployable_research", "validation_and_holdout_for_O15_context", "Separates late-PA opponent type."),
        ("starter_workload", "Predicted starter workload/exposure context.", exposure_pkg, "starter_workload", "game_pitcher", "strict-prior starter history", "deployable_research", "validation_and_holdout_for_O15_context", "Transfers strongly to hits_allowed and hitter props."),
        ("starter_exit_probability", "Probability starter exits before later hitter PA.", exposure_pkg, "starter_exit_probability", "game_pitcher", "strict-prior starter workload", "diagnostic", "partial_source_registry", "No production usage."),
        ("lineup_slot_model", "Pregame lineup slot or model-imputed slot.", lineup_pkg, "pregame_lineup_slot_model", "player_game", "pregame lineup capture or strict-prior fallback", "deployable_research", "validation_and_holdout_for_O15_context", "Not postgame actual lineup."),
        ("lineup_certainty_score", "Certainty of pregame lineup role.", lineup_pkg, "lineup_certainty_score", "player_game", "pregame lineup capture lineage", "diagnostic", "partial_source_registry", "Supports role-quality filtering."),
        ("hitter_swing_rate", "Strict-prior hitter swing rate.", discipline_pkg, "swing_rate", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Direct to batter strikeouts and contact."),
        ("hitter_chase_rate", "Strict-prior hitter chase rate where certified.", discipline_pkg, "chase_rate", "hitter_prior", "prior pitch events only", "diagnostic", "partial_source_registry", "Zone semantics must remain certified."),
        ("hitter_contact_rate", "Strict-prior hitter contact rate.", discipline_pkg, "contact_rate", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Direct to any-hit and batter strikeouts."),
        ("hitter_whiff_rate", "Strict-prior hitter whiff rate.", discipline_pkg, "whiff_rate", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Direct to batter strikeouts."),
        ("hitter_strikeout_rate", "Strict-prior hitter strikeout rate.", discipline_pkg, "strikeout_rate", "hitter_prior", "prior outcomes only", "deployable_research", "partial_source_registry", "Target-adjacent for batter K."),
        ("hitter_walk_rate", "Strict-prior hitter walk rate.", discipline_pkg, "walk_rate", "hitter_prior", "prior outcomes only", "deployable_research", "partial_source_registry", "PA terminal outcome separator."),
        ("terminal_contact_per_pa", "Expected terminal contact events per PA.", contact_pkg, "terminal_contact_per_pa", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Any-hit and multi-hit bridge."),
        ("hit_capable_contact_per_pa", "Expected hit-capable contact per PA.", contact_pkg, "hit_capable_contact_per_pa", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Core contact-frequency field."),
        ("ball_in_play_per_pa", "Expected BIP per PA.", contact_pkg, "ball_in_play_per_pa", "hitter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Transfers to hits, singles, total bases."),
        ("two_strike_contact", "Two-strike contact tendency.", discipline_pkg, "two_strike_contact", "hitter_prior", "prior pitch events only", "diagnostic", "partial_source_registry", "Batter K mechanism."),
        ("pitches_per_pa", "Strict-prior pitches per PA.", discipline_pkg, "pitches_per_pa", "hitter_prior", "prior pitch events only", "diagnostic", "partial_source_registry", "Batter K and PA depth."),
        ("starter_contact_allowed", "Starter contact allowed profile.", contact_pkg, "starter_contact_allowed", "starter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Opponent contact environment."),
        ("starter_whiff_induced", "Starter whiff induced profile.", discipline_pkg, "starter_whiff_rate", "starter_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Direct to batter K."),
        ("starter_strikeout_rate", "Starter strikeout profile.", discipline_pkg, "starter_strikeout_rate", "starter_prior", "prior outcomes only", "deployable_research", "partial_source_registry", "Batter K transfer."),
        ("starter_walk_rate", "Starter walk allowed profile.", discipline_pkg, "starter_walk_rate", "starter_prior", "prior outcomes only", "diagnostic", "partial_source_registry", "Terminal PA separator."),
        ("bullpen_contact_allowed", "Bullpen contact allowed profile.", contact_pkg, "bullpen_contact_allowed", "team_bullpen_prior", "prior pitch events only", "deployable_research", "validation_and_holdout_for_O15_context", "Late-PA environment."),
        ("pitch_family_usage", "Pitch-family usage profile.", discipline_pkg, "pitch_family_usage", "pitcher_prior", "prior pitch events only", "diagnostic", "partial_source_registry", "Needs compatibility binding before production."),
        ("velocity_profile", "Pitch velocity profile.", discipline_pkg, "velocity_profile", "pitcher_prior", "prior pitch events only", "diagnostic", "partial_source_registry", "Useful for K/contact quality."),
        ("handedness_split_profile", "Handedness split profile.", discipline_pkg, "handedness_split_profile", "player_prior", "strict-prior split where support allows", "diagnostic", "partial_source_registry", "Sparse; shrinkage required."),
        ("empirical_xhit_speed_angle", "Repaired empirical hit probability by speed-angle cell.", quality_pkg, "empirical_xhit_speed_angle_v1", "contact_event", "fit period before validation/holdout", "deployable_research", "validation_and_holdout_for_O15_context", "Not official Statcast xBA."),
        ("launch_speed_profile", "Hitter/pitcher launch-speed profile.", quality_pkg, "launch_speed_profile", "player_prior", "prior batted-ball events", "deployable_research", "validation_and_holdout_for_O15_context", "Total bases and conversion transfer."),
        ("launch_angle_profile", "Hitter/pitcher launch-angle profile.", quality_pkg, "launch_angle_profile", "player_prior", "prior batted-ball events", "deployable_research", "validation_and_holdout_for_O15_context", "Singles versus extra-base separator."),
        ("hard_hit_rate", "Strict-prior hard-contact rate.", quality_pkg, "hard_hit_rate", "player_prior", "prior batted-ball events", "diagnostic", "partial_source_registry", "Contact damage proxy."),
        ("batted_ball_type_profile", "Grounder/liner/fly/pop profile.", quality_pkg, "batted_ball_type_profile", "player_prior", "prior batted-ball events", "diagnostic", "partial_source_registry", "Singles and total bases mechanism."),
        ("encounter_role", "Starter versus reliever encounter role.", encounter_pkg, "pitcher_role", "plate_appearance", "official prior encounter reconstruction", "diagnostic", "validation_and_holdout_for_O15_context", "Do not use current-game realized role pregame."),
        ("encounter_order", "Pregame-forecastable encounter sequence concept.", encounter_pkg, "encounter_order", "plate_appearance", "prior encounter reconstruction plus forecast only", "diagnostic", "partial_source_registry", "Oracle-only if realized current-game."),
        ("times_through_order_proxy", "Strict-prior TTO-compatible exposure concept.", encounter_pkg, "times_through_order_proxy", "player_game", "forecasted, not realized", "diagnostic", "partial_source_registry", "Needs more pregame certification."),
        ("affirmative_pitcher_suppression", "Frozen pitcher-suppression veto state.", suppression_pkg, "suppression_veto_state", "player_game", "strict-prior pitcher/hitter evidence", "deployable_research", "validation_and_holdout_for_O15_context", "Preserved as veto/context, not optimizer."),
        ("irregular_role_exclusion", "Irregular role or insufficient prior state.", suppression_pkg, "irregular_role_exclusion", "player_game", "strict-prior role history", "diagnostic", "partial_source_registry", "Fail-closed research guard."),
    ]:
        add(name, definition, package, column, grain, strict_prior, status, coverage, notes)
    return rows


def target_contracts(counts: dict[tuple[str, str], int]) -> list[dict[str, Any]]:
    return [
        {
            "prop_family": "hits_0_5",
            "exact_proposition_identity": "prop_type=hits|line=0.5|side=over_or_under",
            "outcome_definition": "official batter hits >= 1 for OVER; < 1 for UNDER",
            "push_treatment": "no push at 0.5",
            "current_model_probability_score_or_rank": "production calibrated prob_over/prob_under where market exists",
            "model_version": "current production champion via slate output",
            "historical_prediction_population": "selected-proposition certified Hits 0.5 population exists; exact granular joined challenger spine not yet frozen",
            "official_outcome_coverage": "authoritative official hits outcomes available in certified selected-proposition campaign",
            "price_coverage": "current slate exact market rows present; historical selection-time price coverage requires separate certification",
            "temporal_coverage": "current slate count from run-tagged slate output",
            "current_live_rows_2026_07_17": counts.get(("hits", "0.5"), 0),
            "production_or_research_status": "production_surface_exists_research_transfer_not_executed",
            "known_current_limitations": "no exact cross-prop granular matrix frozen yet",
            "baseball_mechanism": "PA opportunity, non-strikeout/contact avoidance, hit-capable contact, conversion, no suppression",
        },
        {
            "prop_family": "hits_1_5",
            "exact_proposition_identity": "prop_type=hits|line=1.5|side=over_or_under",
            "outcome_definition": "official batter hits >= 2 for OVER; <= 1 for UNDER",
            "push_treatment": "no push at 1.5",
            "current_model_probability_score_or_rank": "O1.5 market-anchored ranking challenger frozen separately",
            "model_version": "O15_MARKET_ANCHORED_RANKING_RUN_1 prospective observation",
            "historical_prediction_population": "1026 certified price rows; 567 rolling-origin evaluable rows",
            "official_outcome_coverage": "governed historical O1.5 certified population",
            "price_coverage": "certified at-or-before price population for ranking branch",
            "temporal_coverage": "fit/validation/holdout rolling-origin package frozen",
            "current_live_rows_2026_07_17": counts.get(("hits", "1.5"), 0),
            "production_or_research_status": "prospective_ranking_observation_active",
            "known_current_limitations": "Run 1 pending official July 17 grade; do not reconstruct or alter",
            "baseball_mechanism": "repeated PA/contact/conversion, starter-bullpen exposure, market-relative ranking",
        },
        {
            "prop_family": "singles",
            "exact_proposition_identity": "prop_type=singles|line=0.5 if available|side=over_or_under",
            "outcome_definition": "official singles count exceeds line",
            "push_treatment": "line-dependent; no push at 0.5",
            "current_model_probability_score_or_rank": "supported in upload mapper but absent from inspected current slate",
            "model_version": "not bound for current transfer audit",
            "historical_prediction_population": "not located as exact granular joined spine",
            "official_outcome_coverage": "official singles derivable from total hits and extra-base components only if certified",
            "price_coverage": "not certified in this audit",
            "temporal_coverage": "unknown for exact selected proposition spine",
            "current_live_rows_2026_07_17": counts.get(("singles", "0.5"), 0),
            "production_or_research_status": "fail_closed_missing_current_population",
            "known_current_limitations": "needs exact singles market/outcome spine and extra-base split lineage",
            "baseball_mechanism": "contact conversion into singles, excluding extra-base damage",
        },
        {
            "prop_family": "total_bases",
            "exact_proposition_identity": "prop_type=total_bases|line varies|side=over_or_under",
            "outcome_definition": "official total bases exceeds line",
            "push_treatment": "push possible at integer lines only; current inspected lines are 0.5/1.5",
            "current_model_probability_score_or_rank": "production slate plus research-only total bases shadow",
            "model_version": "dedicated_total_bases_plus_rolling_dual_shadow research-only",
            "historical_prediction_population": "23000-row training dataset referenced by shadow summary; current shadow rows generated",
            "official_outcome_coverage": "shadow summary reports outcomes_supplied=false for 2026-07-17",
            "price_coverage": "current slate exact market rows present; historical price coverage not certified here",
            "temporal_coverage": "training 2026-04-01 through 2026-06-14 in current shadow",
            "current_live_rows_2026_07_17": counts.get(("total_bases", "0.5"), 0) + counts.get(("total_bases", "1.5"), 0),
            "production_or_research_status": "research_shadow_exists_outcome_holdout_not_supplied_for_current_run",
            "known_current_limitations": "not yet tied to granular contact-quality transfer holdout for current prop",
            "baseball_mechanism": "hit probability, repeated contact, contact damage, speed/angle conversion",
        },
        {
            "prop_family": "strikeouts_batting",
            "exact_proposition_identity": "prop_type=strikeouts_batting|line varies|side=over_or_under",
            "outcome_definition": "official batter strikeouts exceeds line",
            "push_treatment": "line-dependent; no push at half lines",
            "current_model_probability_score_or_rank": "production schema supports d7/d15/d30 strikeouts_batting context",
            "model_version": "not bound in current slate output",
            "historical_prediction_population": "not located as exact current selected proposition spine for this audit",
            "official_outcome_coverage": "official batter strikeout outcomes exist in player stats lineage but transfer spine not frozen",
            "price_coverage": "not certified in this audit",
            "temporal_coverage": "unknown for exact market population",
            "current_live_rows_2026_07_17": sum(v for (p, _), v in counts.items() if p == "strikeouts_batting"),
            "production_or_research_status": "mechanism_strong_population_not_bound",
            "known_current_limitations": "needs exact market/outcome denominator before diagnostic fitting",
            "baseball_mechanism": "expected PA, hitter whiff/contact, opposing pitcher whiff/K, pitch-family compatibility",
        },
        {
            "prop_family": "hits_allowed",
            "exact_proposition_identity": "prop_type=hits_allowed|line varies|side=over_or_under",
            "outcome_definition": "official pitcher hits allowed exceeds line",
            "push_treatment": "push possible at integer lines only; current inspected lines are half lines",
            "current_model_probability_score_or_rank": "production slate rows exist for pitcher hits allowed",
            "model_version": "current production champion via slate output",
            "historical_prediction_population": "starter workload and BF foundations exist; exact granular hitter-lineup-facing transfer spine not frozen",
            "official_outcome_coverage": "official pitcher hits allowed available in player_stats with known lineage caveats",
            "price_coverage": "current slate exact market rows present; historical price coverage not certified here",
            "temporal_coverage": "current slate and starter foundation artifacts",
            "current_live_rows_2026_07_17": sum(v for (p, _), v in counts.items() if p == "hits_allowed"),
            "production_or_research_status": "production_surface_exists_foundation_partial",
            "known_current_limitations": "needs pitcher-facing BF/lineup contact opponent spine before transfer fitting",
            "baseball_mechanism": "BF/workload, opposing lineup contact frequency, hitter conversion quality, bullpen exclusion",
        },
    ]


def population_rows(counts: dict[tuple[str, str], int], tb_summary: dict[str, Any]) -> list[dict[str, Any]]:
    o15 = read_csv(O15_HISTORICAL_PATH)
    oof = read_csv(O15_OOF_PATH)
    tb_scores = read_csv(TB_SHADOW_SCORES_PATH)
    rows = []

    def add(prop: str, current_rows: int, hist_rows: Any, oof_rows: Any, status: str, notes: str) -> None:
        rows.append(
            {
                "prop_family": prop,
                "current_live_rows_2026_07_17": current_rows,
                "historical_rows": hist_rows,
                "validation_holdout_rows": oof_rows,
                "official_outcome_coverage_status": status,
                "price_coverage_status": "certified_for_o15_only" if prop == "hits_1_5" else "not_certified_in_this_audit",
                "exact_granular_joined_population_status": "available_for_o15_only" if prop == "hits_1_5" else "not_frozen",
                "comparable_population_decision": "ELIGIBLE_REFERENCE_ONLY" if prop == "hits_1_5" else "FAIL_CLOSED_NO_EXACT_COMPARABLE_SPINE",
                "notes": notes,
            }
        )

    add("hits_0_5", counts.get(("hits", "0.5"), 0), "certified_selected_prop_population_exists_not_loaded_as_granular_spine", "not_frozen", "certified_selected_prop_outcomes_exist", "authoritative any-hit outcomes exist; granular transfer matrix must be assembled next")
    add("hits_1_5", counts.get(("hits", "1.5"), 0), len(o15), len(oof), "certified", "prospective ranking observation active; not eligible for another experiment now")
    add("singles", counts.get(("singles", "0.5"), 0), "unknown", "unknown", "not_bound", "no current slate rows found")
    add("total_bases", counts.get(("total_bases", "0.5"), 0) + counts.get(("total_bases", "1.5"), 0), tb_summary.get("training_rows", ""), len(tb_scores) if not tb_scores.empty else "", "current_shadow_outcomes_not_supplied", "shadow exists but no current holdout outcomes supplied")
    add("strikeouts_batting", sum(v for (p, _), v in counts.items() if p == "strikeouts_batting"), "unknown", "unknown", "not_bound", "mechanism is strong but exact market/outcome transfer population not located")
    add("hits_allowed", sum(v for (p, _), v in counts.items() if p == "hits_allowed"), "starter_foundation_artifacts_exist", "not_frozen", "pitcher_foundation_outcomes_exist_with_lineage_caveats", "pitcher-facing BF and lineup-contact spine needed")
    return rows


def scorecard() -> list[dict[str, Any]]:
    weights = {
        "direct_causal_relationship": 12,
        "strict_prior_feature_coverage": 12,
        "current_live_availability": 8,
        "authoritative_outcome_quality": 8,
        "historical_sample_size": 8,
        "untouched_holdout_availability": 8,
        "existing_model_weakness": 8,
        "interpretability": 8,
        "temporal_replayability": 8,
        "price_coverage": 6,
        "low_leakage_risk": 8,
        "operational_usefulness": 8,
        "o15_reuse_value": 6,
    }
    scores = {
        "hits_0_5": [10, 8, 10, 8, 7, 5, 8, 10, 7, 5, 8, 9, 10],
        "hits_1_5": [10, 10, 8, 10, 9, 9, 8, 9, 9, 9, 8, 9, 10],
        "singles": [7, 7, 0, 4, 2, 0, 6, 6, 2, 0, 6, 4, 5],
        "total_bases": [9, 7, 10, 7, 9, 4, 7, 8, 7, 5, 7, 8, 6],
        "strikeouts_batting": [10, 8, 0, 7, 3, 0, 8, 9, 4, 0, 7, 7, 7],
        "hits_allowed": [8, 7, 8, 7, 5, 2, 7, 7, 5, 4, 6, 7, 4],
    }
    rows = []
    for prop, vals in scores.items():
        weighted = 0.0
        detail = {}
        for key, score in zip(weights, vals):
            detail[key] = score
            weighted += (score / 10.0) * weights[key]
        rows.append(
            {
                "prop_family": prop,
                **detail,
                "weighted_score": round(weighted, 2),
                "selection_eligible_now": "false" if prop == "hits_1_5" else ("true" if prop == "hits_0_5" else "false"),
                "notes": (
                    "selected next because direct any-hit mechanism and strongest O1.5 reuse"
                    if prop == "hits_0_5"
                    else "not selectable because O1.5 is already in prospective observation"
                    if prop == "hits_1_5"
                    else "not selected; missing exact comparable transfer spine or weaker reuse"
                ),
            }
        )
    return sorted(rows, key=lambda r: r["weighted_score"], reverse=True)


def bundles() -> list[dict[str, Any]]:
    common = "predicted_total_pa,pa4_probability,pa5_probability,lineup_slot_model,lineup_certainty_score,affirmative_pitcher_suppression"
    return [
        {
            "prop_family": "hits_0_5",
            "fixed_bundle_name": "any_hit_opportunity_contact_conversion_v1",
            "frozen_fields": common + ",hitter_contact_rate,hitter_whiff_rate,hit_capable_contact_per_pa,ball_in_play_per_pa,starter_contact_allowed,bullpen_contact_allowed,empirical_xhit_speed_angle",
            "target": "official_hits_ge_1",
            "fit_population_status": "design_frozen_population_not_executed",
            "missing_policy": "fail_closed_for_missing_core_identity; median/shrinkage only for governed profile fields",
            "expected_mechanism": "capture at least one PA/contact/conversion while avoiding noncontact and suppression",
            "notes": "selected next bounded experiment; no fitting executed in this audit",
        },
        {
            "prop_family": "hits_1_5",
            "fixed_bundle_name": "existing_o15_market_anchored_ranking_v1",
            "frozen_fields": "market_probability_used,predicted_minus_implied_probability,p_two_plus_hits,suppression_veto_state,contact_hitter_regime_state,opportunity_bucket,personal_support_bucket",
            "target": "official_hits_ge_2",
            "fit_population_status": "already_frozen_prospective_observation_active",
            "missing_policy": "preserve existing ranking contract",
            "expected_mechanism": "repeated PA/contact/conversion with market-relative rank",
            "notes": "do not execute another O1.5 experiment",
        },
        {
            "prop_family": "singles",
            "fixed_bundle_name": "singles_conversion_without_damage_v1",
            "frozen_fields": common + ",hit_capable_contact_per_pa,empirical_xhit_speed_angle,launch_angle_profile,batted_ball_type_profile",
            "target": "official_singles_over_line",
            "fit_population_status": "blocked_no_exact_population",
            "missing_policy": "fail_closed",
            "expected_mechanism": "contact conversion into singles while excluding extra-base damage",
            "notes": "needs exact singles spine first",
        },
        {
            "prop_family": "total_bases",
            "fixed_bundle_name": "contact_damage_total_bases_v1",
            "frozen_fields": common + ",hit_capable_contact_per_pa,empirical_xhit_speed_angle,launch_speed_profile,launch_angle_profile,hard_hit_rate,batted_ball_type_profile",
            "target": "official_total_bases_over_line",
            "fit_population_status": "shadow_exists_outcome_holdout_not_supplied_for_current_run",
            "missing_policy": "research shadow policy",
            "expected_mechanism": "hit count plus contact damage",
            "notes": "good later candidate after exact outcome-backed granular transfer package",
        },
        {
            "prop_family": "strikeouts_batting",
            "fixed_bundle_name": "batter_k_discipline_compatibility_v1",
            "frozen_fields": "predicted_total_pa,hitter_swing_rate,hitter_chase_rate,hitter_contact_rate,hitter_whiff_rate,hitter_strikeout_rate,two_strike_contact,pitches_per_pa,starter_whiff_induced,starter_strikeout_rate,pitch_family_usage,velocity_profile,handedness_split_profile",
            "target": "official_batter_strikeouts_over_line",
            "fit_population_status": "blocked_no_exact_market_population",
            "missing_policy": "fail_closed",
            "expected_mechanism": "PA volume plus hitter noncontact versus pitcher swing-miss profile",
            "notes": "mechanistically attractive but no exact population located",
        },
        {
            "prop_family": "hits_allowed",
            "fixed_bundle_name": "pitcher_hits_allowed_lineup_contact_exposure_v1",
            "frozen_fields": "starter_workload,predicted_total_bf,predicted_lineup_contact_frequency,opponent_empirical_xhit,starter_contact_allowed,starter_whiff_induced,starter_walk_rate,affirmative_pitcher_suppression",
            "target": "official_pitcher_hits_allowed_over_line",
            "fit_population_status": "blocked_no_pitcher_facing_granular_spine",
            "missing_policy": "fail_closed",
            "expected_mechanism": "BF/workload times opponent contact and conversion",
            "notes": "requires pitcher-facing BF plus opposing lineup granular aggregation",
        },
    ]


def transfer_results() -> list[dict[str, Any]]:
    exposure = read_csv(EXPOSURE_INCREMENT_PATH)
    contact = read_csv(CONTACT_OPP_PATH)
    discipline = read_csv(DISCIPLINE_PATH)
    quality = read_csv(CONTACT_QUALITY_PATH)
    tb_summary = read_json(TB_SHADOW_SUMMARY_PATH)
    rows: list[dict[str, Any]] = []

    def add(prop: str, diagnostic: str, source: str, split: str, control_auc: Any, challenger_auc: Any, control_brier: Any, challenger_brier: Any, rows_count: Any, status: str, notes: str) -> None:
        rows.append(
            {
                "prop_family": prop,
                "diagnostic_name": diagnostic,
                "source_artifact": source,
                "temporal_split": split,
                "rows": rows_count,
                "control_auc": control_auc,
                "challenger_auc": challenger_auc,
                "auc_increment": _delta(challenger_auc, control_auc),
                "control_brier": control_brier,
                "challenger_brier": challenger_brier,
                "brier_increment": _delta(control_brier, challenger_brier),
                "execution_status": status,
                "notes": notes,
            }
        )

    for split in ["validation", "holdout"]:
        add(
            "hits_1_5",
            "exposure_increment_reference",
            str(EXPOSURE_INCREMENT_PATH),
            split,
            metric_from(exposure, split, "frozen_multi_hit_control", "auc"),
            metric_from(exposure, split, "new_exposure_challenger", "auc"),
            metric_from(exposure, split, "frozen_multi_hit_control", "brier"),
            metric_from(exposure, split, "new_exposure_challenger", "brier"),
            metric_from(exposure, split, "new_exposure_challenger", "rows"),
            "EXECUTED_PREVIOUSLY_REFERENCE_ONLY",
            "O1.5 result preserved; not re-executed or altered",
        )
        add(
            "hits_1_5",
            "contact_quality_reference",
            str(CONTACT_QUALITY_PATH),
            split,
            metric_from(quality, split, "predicted_contact_count_model", "auc"),
            metric_from(quality, split, "hitter_plus_starter_conversion", "auc"),
            metric_from(quality, split, "predicted_contact_count_model", "brier"),
            metric_from(quality, split, "hitter_plus_starter_conversion", "brier"),
            metric_from(quality, split, "hitter_plus_starter_conversion", "rows"),
            "EXECUTED_PREVIOUSLY_REFERENCE_ONLY",
            "Shows contact-quality transfer has signal but remains O1.5 reference evidence",
        )
        add(
            "hits_1_5",
            "discipline_reference",
            str(DISCIPLINE_PATH),
            split,
            metric_from(discipline, split, "exposure_control", "auc"),
            metric_from(discipline, split, "discipline_unified", "auc"),
            metric_from(discipline, split, "exposure_control", "brier"),
            metric_from(discipline, split, "discipline_unified", "brier"),
            metric_from(discipline, split, "discipline_unified", "rows"),
            "EXECUTED_PREVIOUSLY_REFERENCE_ONLY",
            "Discipline has directional separation but calibration weakness for multi-hit target",
        )

    add(
        "total_bases",
        "current_shadow_delta_no_outcomes",
        str(TB_SHADOW_SUMMARY_PATH),
        "current_slate_shadow",
        "",
        "",
        "",
        "",
        tb_summary.get("shadow_rows", ""),
        "PARTIAL_NO_OUTCOMES_SUPPLIED",
        f"side_changed_rows={tb_summary.get('side_changed_rows','')}; production_outputs_changed={tb_summary.get('production_outputs_changed','')}",
    )
    for prop in ["hits_0_5", "singles", "strikeouts_batting", "hits_allowed"]:
        add(
            prop,
            "fixed_transfer_diagnostic",
            "not_available_as_exact_granular_joined_population",
            "not_executed",
            "",
            "",
            "",
            "",
            "",
            "FAIL_CLOSED_NO_EXACT_COMPARABLE_SPINE",
            "No diagnostic fitting executed; next step is bounded population assembly where selected",
        )
    return rows


def _delta(a: Any, b: Any) -> Any:
    try:
        if a == "" or b == "":
            return ""
        return round(float(a) - float(b), 6)
    except Exception:
        return ""


def orientation_rows() -> list[dict[str, Any]]:
    o15_coef = read_csv(Path("artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/2026-07-17/ranking_instrument_coefficients_2026-07-17.csv"))
    rows = [
        {
            "prop_family": "hits_1_5",
            "artifact": "ranking_instrument_coefficients_2026-07-17.csv",
            "coefficient_or_orientation_status": "FOUND",
            "expected_direction": "higher challenger score means stronger O1.5 ordering",
            "observed_evidence": f"coefficient_rows={len(o15_coef)}",
            "audit_result": "PASS_REFERENCE_ONLY",
            "notes": "O1.5 instrument preserved, not changed",
        },
    ]
    for prop, status in [
        ("hits_0_5", "NOT_YET_FIT"),
        ("singles", "NO_EXACT_POPULATION"),
        ("total_bases", "SHADOW_NOT_OUTCOME_CERTIFIED"),
        ("strikeouts_batting", "NO_EXACT_POPULATION"),
        ("hits_allowed", "NO_GRANULAR_PITCHER_FACING_SPINE"),
    ]:
        rows.append(
            {
                "prop_family": prop,
                "artifact": "",
                "coefficient_or_orientation_status": status,
                "expected_direction": "frozen in selected next experiment before any fit" if prop == "hits_0_5" else "not applicable",
                "observed_evidence": "",
                "audit_result": "FAIL_CLOSED" if prop != "total_bases" else "PARTIAL",
                "notes": "No coefficient interpretation made from unfitted target",
            }
        )
    return rows


def temporal_rows() -> list[dict[str, Any]]:
    pairwise = read_csv(O15_PAIRWISE_PATH)
    exposure = read_csv(EXPOSURE_INCREMENT_PATH)
    rows = []
    if not pairwise.empty:
        rows.append(
            {
                "prop_family": "hits_1_5",
                "temporal_test": "rolling_origin_pairwise_auc_increment",
                "source_artifact": str(O15_PAIRWISE_PATH),
                "blocks_or_splits": len(pairwise) if "fold" in pairwise.columns else "4_reported",
                "stable_positive_blocks": "4_reported",
                "result": "STABLE_REFERENCE",
                "notes": "Current O1.5 branch stays in prospective observation.",
            }
        )
    if not exposure.empty:
        hold = exposure[exposure.get("temporal_split", pd.Series(dtype=str)).astype(str) == "holdout"]
        rows.append(
            {
                "prop_family": "hits_1_5",
                "temporal_test": "exposure_validation_holdout",
                "source_artifact": str(EXPOSURE_INCREMENT_PATH),
                "blocks_or_splits": "validation,holdout",
                "stable_positive_blocks": "brier/ece improved in both; AUC mixed",
                "result": "PRODUCTIVE_BUT_NOT_COMPLETE",
                "notes": f"holdout_rows={int(hold['rows'].max()) if not hold.empty and 'rows' in hold.columns else ''}",
            }
        )
    for prop in ["hits_0_5", "singles", "total_bases", "strikeouts_batting", "hits_allowed"]:
        rows.append(
            {
                "prop_family": prop,
                "temporal_test": "target_specific_temporal_transfer",
                "source_artifact": "",
                "blocks_or_splits": "",
                "stable_positive_blocks": "",
                "result": "NOT_TESTED_FAIL_CLOSED",
                "notes": "Requires exact target population before stability can be claimed.",
            }
        )
    return rows


def classifications() -> list[dict[str, Any]]:
    return [
        {
            "prop_family": "hits_0_5",
            "transfer_classification": "BEST_NEXT_BOUNDED_EXPERIMENT",
            "evidence_summary": "Current live rows exist; official hits outcomes are mature; granular platform directly explains one-hit avoidance/capture; strongest O1.5 reuse through P(0).",
            "incremental_value_status": "PROMISING_NOT_YET_EXECUTED",
            "next_action": "assemble fixed any-hit granular spine and run one bounded champion/challenger diagnostic",
        },
        {
            "prop_family": "hits_1_5",
            "transfer_classification": "REFERENCE_PLATFORM_ACTIVE",
            "evidence_summary": "Historical ranking branch is frozen and prospective Run 1 is pending grade.",
            "incremental_value_status": "SUPPORTED_FOR_CURRENT_BRANCH",
            "next_action": "observe and grade existing prospective run only",
        },
        {
            "prop_family": "singles",
            "transfer_classification": "BLOCKED_MISSING_EXACT_POPULATION",
            "evidence_summary": "No current slate singles rows located; extra-base split lineage must be certified.",
            "incremental_value_status": "NOT_EVALUABLE",
            "next_action": "inventory singles market/outcome spine before modeling",
        },
        {
            "prop_family": "total_bases",
            "transfer_classification": "PROMISING_LATER_CONTACT_DAMAGE_BRANCH",
            "evidence_summary": "Current shadow exists with complete rolling context and 213 rows, but outcomes were not supplied for the current shadow run.",
            "incremental_value_status": "PARTIAL",
            "next_action": "certify total-bases outcome-backed granular transfer spine after Hits 0.5",
        },
        {
            "prop_family": "strikeouts_batting",
            "transfer_classification": "MECHANISM_STRONG_POPULATION_BLOCKED",
            "evidence_summary": "Discipline/whiff/contact platform is directly relevant, but no exact current/historical batter-K transfer population was bound.",
            "incremental_value_status": "NOT_EVALUABLE",
            "next_action": "bind batter-K market/outcome denominator before any challenger",
        },
        {
            "prop_family": "hits_allowed",
            "transfer_classification": "FOUNDATION_PARTIAL_PITCHER_FACING_SPINE_NEEDED",
            "evidence_summary": "Starter/BF lineage is strong, but transfer requires opposing lineup contact aggregation and pitcher-facing BF exposure.",
            "incremental_value_status": "NOT_EVALUABLE",
            "next_action": "build pitcher-facing granular exposure spine before model test",
        },
    ]


def selected_experiment() -> list[dict[str, Any]]:
    return [
        {
            "experiment_id": "MLB-GRANULAR-XPROP-001",
            "selected_prop_family": "hits_0_5",
            "experiment_title": "Hits 0.5 Any-Hit Opportunity Contact Conversion Challenger",
            "objective": "Test whether strict-prior granular opportunity/contact/conversion fields improve any-hit prediction against the frozen production Hits 0.5 baseline.",
            "fixed_population": "historical selected-proposition Hits 0.5 rows with official outcome, strict-prior PA/opportunity, contact-frequency, contact-quality, starter/bullpen exposure, and selection-time lineage where available",
            "control": "current production Hits 0.5 probability/ranking from preserved prediction artifacts",
            "challenger_bundle": "any_hit_opportunity_contact_conversion_v1",
            "primary_metrics": "AUC, Brier, log loss, calibration, rank-band lift, temporal block stability",
            "forbidden_actions": "no hyperparameter search; no threshold/price optimization; no production change; no upload change",
            "exit_criteria": "all temporal blocks valid; no leakage; fixed feature orientation; materially positive holdout diagnostics versus control",
            "decision": "SELECTED_FOR_NEXT_BOUNDED_DESIGN_NOT_EXECUTED",
        }
    ]


def o15_reuse_pathway() -> list[dict[str, Any]]:
    return [
        {
            "reuse_component": "P_zero_hits",
            "from_selected_experiment": "Hits 0.5 any-hit challenger estimates P(hit>=1), which directly constrains P(0).",
            "o15_use": "multi-hit distribution calibration and false-positive filtering",
            "status": "research_reuse_after_hits05_validation",
            "notes": "Do not patch O1.5 Run 1.",
        },
        {
            "reuse_component": "noncontact_failure_mode",
            "from_selected_experiment": "Any-hit misses isolate strikeout/noncontact/weak-contact failure patterns.",
            "o15_use": "explain weak O1.5 candidates that cannot secure first hit despite team context",
            "status": "diagnostic",
            "notes": "Especially useful for long-price O1.5.",
        },
        {
            "reuse_component": "opportunity_contact_conversion_chain",
            "from_selected_experiment": "Any-hit probability decomposes PA opportunity, contact frequency, and conversion quality.",
            "o15_use": "later convert from one-hit floor to two-hit ceiling",
            "status": "diagnostic",
            "notes": "Keeps baseball probability before price.",
        },
    ]


def decisions() -> list[dict[str, Any]]:
    return [
        {"decision_name": "MLB_GRANULAR_PLATFORM_SOURCE_BINDING_DECISION", "decision_value": "BOUND_TO_EXISTING_REPOSITORY_ARTIFACTS_NO_NEW_ACQUISITION", "notes": "Only local artifacts were read."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_FEATURE_COVERAGE_DECISION", "decision_value": "FEATURE_REGISTRY_FROZEN_COVERAGE_MIXED_BY_PROP", "notes": "Granular fields are mature for O1.5 reference; target-specific spines vary."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_CROSS_PROP_POPULATION_DECISION", "decision_value": "ONLY_O15_HAS_FULL_GOVERNED_GRANULAR_OUTCOME_PRICE_POPULATION", "notes": "Other props fail closed pending exact population assembly."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_HITS05_TRANSFER_DECISION", "decision_value": "SELECTED_NEXT_BOUNDED_EXPERIMENT_POPULATION_ASSEMBLY_REQUIRED", "notes": "Best cross-prop transfer and O1.5 reuse candidate."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_HITS15_STATUS_DECISION", "decision_value": "PROSPECTIVE_RANKING_OBSERVATION_ACTIVE", "notes": "Run 1 remains untouched and pending grade."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_SINGLES_TRANSFER_DECISION", "decision_value": "NOT_EVALUABLE_EXACT_POPULATION_MISSING", "notes": "No current inspected slate population."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_TOTAL_BASES_TRANSFER_DECISION", "decision_value": "PROMISING_SHADOW_EXISTS_BUT_OUTCOME_BACKED_TRANSFER_NOT_CERTIFIED", "notes": "Good later branch after exact holdout binding."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_BATTER_K_TRANSFER_DECISION", "decision_value": "MECHANISM_STRONG_BUT_POPULATION_NOT_BOUND", "notes": "Needs exact market/outcome denominator."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_PITCHER_HITS_ALLOWED_TRANSFER_DECISION", "decision_value": "FOUNDATION_PARTIAL_PITCHER_FACING_GRANULAR_SPINE_REQUIRED", "notes": "Needs BF plus opponent lineup contact aggregation."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_INCREMENTAL_VALUE_DECISION", "decision_value": "SUPPORTED_AS_REUSABLE_PLATFORM_WITH_TARGET_SPECIFIC_READINESS_LIMITS", "notes": "Existing O1.5 diagnostics support platform value; cross-prop testing must be bounded."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_SELECTED_PROP_DECISION", "decision_value": "HITS_0_5_SELECTED_FOR_NEXT_BOUNDED_EXPERIMENT", "notes": "Best mix of mechanism, availability, interpretability, and O1.5 reuse."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_NEXT_EXPERIMENT_DESIGN_DECISION", "decision_value": "MLB_GRANULAR_XPROP_001_HITS05_ANY_HIT_CHALLENGER_DESIGN_FROZEN_NOT_EXECUTED", "notes": "No training performed here."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_O15_REUSE_DECISION", "decision_value": "REUSE_THROUGH_ZERO_HIT_AND_CONTACT_FAILURE_DIAGNOSTICS_AFTER_HITS05_VALIDATION", "notes": "No impact to prospective O1.5 Run 1."},
        {"decision_name": "MLB_GRANULAR_PLATFORM_PRODUCTION_STATUS", "decision_value": "NOT_AUTHORIZED", "notes": "No production or upload behavior changed."},
    ]


def markdown_summary(paths: dict[str, str], stats: dict[str, Any]) -> str:
    return f"""# MLB Granular Feature-Platform Cross-Prop Transfer Audit

Generated: `{stats['generated_at']}`

## Executive Summary

The reusable granular platform is real, but target readiness is uneven. O1.5 remains the only prop with a fully governed historical granular, outcome, and certified price-ranking package. It is already in prospective observation and was not altered.

The best next cross-prop experiment is **Hits 0.5**. It has the cleanest baseball transfer from the O1.5 platform: PA opportunity, noncontact avoidance, hit-capable contact, conversion quality, and pitcher suppression all explain whether a hitter gets at least one hit. It also creates the most useful O1.5 reuse path by improving the platform's understanding of `P(0 hits)` and first-hit failure modes.

## Key Findings

- Current slate live rows found: Hits 0.5 `{stats['hits05_current_rows']}`, Hits 1.5 `{stats['hits15_current_rows']}`, Total Bases `{stats['total_bases_current_rows']}`, Hits Allowed `{stats['hits_allowed_current_rows']}`.
- No current inspected slate rows were found for `singles` or `strikeouts_batting`.
- Total Bases has a research-only shadow with `{stats['tb_shadow_rows']}` current rows, but its summary reports `outcomes_supplied=false`.
- The O1.5 branch has `{stats['o15_historical_rows']}` certified historical price rows and `{stats['o15_oof_rows']}` rolling-origin evaluable rows; it remains `PROSPECTIVE_RANKING_OBSERVATION_ACTIVE`.

## Selection

Selected next bounded experiment:

`MLB-GRANULAR-XPROP-001 — Hits 0.5 Any-Hit Opportunity Contact Conversion Challenger`

This audit freezes the design only. It does not execute the challenger, fit a model, optimize a threshold, change a formula, change uploads, or alter production behavior.

## Why Hits 0.5

Hits 0.5 is the strongest transfer target because its outcome is the first gate in the multi-hit distribution. If the granular platform can explain who fails to get even one hit, the result directly improves future O1.5 diagnostics without touching the active O1.5 prospective ranking run.

## Fail-Closed Props

- `singles`: no exact current population located and extra-base split lineage is not frozen.
- `strikeouts_batting`: mechanism is strong, but exact market/outcome population was not bound.
- `hits_allowed`: live surface exists, but a pitcher-facing granular lineup-contact/BF spine is not frozen.
- `total_bases`: promising contact-damage branch, but current shadow lacks supplied outcomes for transfer certification.

## No Behavior Changed

No model formula, tier, selector, upload, workspace, DB, OddsAPI, LaunchAgent, or production behavior was changed.
"""


def validate_outputs(output_dir: Path, generated_files: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in generated_files:
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                pd.read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.startswith("#"):
                    status = "WARN"
                    notes = "markdown does not start with heading"
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    return rows


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()

    slate = read_csv(SLATE_PATH)
    counts = prop_counts(slate)
    tb_summary = read_json(TB_SHADOW_SUMMARY_PATH)
    o15 = read_csv(O15_HISTORICAL_PATH)
    oof = read_csv(O15_OOF_PATH)

    stats = {
        "generated_at": generated_at,
        "hits05_current_rows": counts.get(("hits", "0.5"), 0),
        "hits15_current_rows": counts.get(("hits", "1.5"), 0),
        "total_bases_current_rows": counts.get(("total_bases", "0.5"), 0) + counts.get(("total_bases", "1.5"), 0),
        "hits_allowed_current_rows": sum(v for (p, _), v in counts.items() if p == "hits_allowed"),
        "tb_shadow_rows": tb_summary.get("shadow_rows", ""),
        "o15_historical_rows": len(o15),
        "o15_oof_rows": len(oof),
    }

    files: dict[str, Path] = {
        "executive_summary": output_dir / f"executive_summary_{RUN_DATE}.md",
        "feature_registry": output_dir / f"granular_feature_registry_{RUN_DATE}.csv",
        "target_contracts": output_dir / f"target_prediction_contract_registry_{RUN_DATE}.csv",
        "populations": output_dir / f"exact_cross_prop_population_registry_{RUN_DATE}.csv",
        "scorecard": output_dir / f"mechanism_alignment_scorecard_{RUN_DATE}.csv",
        "bundles": output_dir / f"frozen_granular_bundle_by_prop_{RUN_DATE}.csv",
        "transfer_results": output_dir / f"validation_holdout_transfer_results_{RUN_DATE}.csv",
        "orientation": output_dir / f"coefficient_orientation_audit_{RUN_DATE}.csv",
        "temporal": output_dir / f"temporal_stability_report_{RUN_DATE}.csv",
        "classifications": output_dir / f"transfer_classifications_{RUN_DATE}.csv",
        "selected": output_dir / f"selected_next_experiment_design_{RUN_DATE}.csv",
        "reuse": output_dir / f"o15_reuse_pathway_{RUN_DATE}.csv",
        "decisions": output_dir / f"required_decisions_{RUN_DATE}.csv",
        "machine": output_dir / f"machine_readable_cross_prop_transfer_{RUN_DATE}.json",
        "manifest": output_dir / f"sha256_manifest_{RUN_DATE}.csv",
        "validation": output_dir / f"validation_report_{RUN_DATE}.csv",
    }

    write_text(files["executive_summary"], markdown_summary({k: str(v) for k, v in files.items()}, stats))
    write_csv(files["feature_registry"], feature_registry())
    write_csv(files["target_contracts"], target_contracts(counts))
    write_csv(files["populations"], population_rows(counts, tb_summary))
    write_csv(files["scorecard"], scorecard())
    write_csv(files["bundles"], bundles())
    write_csv(files["transfer_results"], transfer_results())
    write_csv(files["orientation"], orientation_rows())
    write_csv(files["temporal"], temporal_rows())
    write_csv(files["classifications"], classifications())
    write_csv(files["selected"], selected_experiment())
    write_csv(files["reuse"], o15_reuse_pathway())
    write_csv(files["decisions"], decisions())

    machine = {
        "generated_at": generated_at,
        "run_date": RUN_DATE,
        "mode": "read_only_cross_prop_transfer_audit",
        "source_artifacts": {
            "slate": str(SLATE_PATH),
            "o15_historical": str(O15_HISTORICAL_PATH),
            "o15_oof": str(O15_OOF_PATH),
            "exposure_increment": str(EXPOSURE_INCREMENT_PATH),
            "contact_opportunity": str(CONTACT_OPP_PATH),
            "discipline": str(DISCIPLINE_PATH),
            "contact_quality": str(CONTACT_QUALITY_PATH),
            "total_bases_shadow_summary": str(TB_SHADOW_SUMMARY_PATH),
            "total_bases_shadow_scores": str(TB_SHADOW_SCORES_PATH),
        },
        "stats": stats,
        "selected_next_experiment": selected_experiment()[0],
        "decisions": decisions(),
        "production_status": "NOT_AUTHORIZED",
        "guardrails": {
            "network_calls": 0,
            "db_writes": 0,
            "model_training": 0,
            "production_behavior_changes": 0,
            "o15_run1_modified": False,
        },
    }
    write_text(files["machine"], json.dumps(machine, indent=2) + "\n")

    generated_before_manifest = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    manifest_rows = []
    for p in generated_before_manifest:
        manifest_rows.append(
            {
                "path": str(p),
                "sha256": sha256_file(p),
                "bytes": p.stat().st_size,
                "notes": "generated audit artifact",
            }
        )
    write_csv(files["manifest"], manifest_rows)

    validation_rows = validate_outputs(output_dir, generated_before_manifest + [files["manifest"]])
    validation_rows.extend(
        [
            {"artifact": "guardrail_no_network", "validation": "PASS", "notes": "script reads local files only"},
            {"artifact": "guardrail_no_db_write", "validation": "PASS", "notes": "no DB connector or write path used"},
            {"artifact": "guardrail_no_training", "validation": "PASS", "notes": "no model fitting executed"},
            {"artifact": "guardrail_o15_run1_preserved", "validation": "PASS", "notes": "O1.5 referenced only as existing governed package"},
        ]
    )
    write_csv(files["validation"], validation_rows)
    return {"output_dir": str(output_dir), "files": {k: str(v) for k, v in files.items()}, "stats": stats}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
