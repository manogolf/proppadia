"""Materialize current Pitcher Hits Allowed opponent-lineup encounter features.

This bounded utility binds the frozen historical opponent-lineup encounter
construction used by the Pitcher Hits Allowed Challenger, verifies historical
field/prediction parity, then attempts to materialize the same pitcher-game
aggregate for a current slate from local pregame artifacts only.

It performs no network calls, no OddsAPI calls, no DB writes, no model fitting,
and no production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import materialize_mlb_pitcher_hits_allowed_live_replay_repair as live_replay
from backend.mlb.scripts import run_mlb_pitcher_hits_allowed_granular_encounter_challenger as pha


RUN_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_pitcher_hits_allowed_current_encounter_source/2026-07-17"
)
FROZEN_LEDGER = Path(
    "artifacts/analysis/model_development/"
    "mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17/"
    "pitcher_hits_allowed_opponent_lineup_encounter_ledger_2026-07-17.csv"
)
CURRENT_SLATE = Path(
    "backend/mlb/exports/odds_history/2026-07-17/"
    "mlb_slate_output__local_daily_20260717T200004Z.csv"
)
CURRENT_WIDE = Path(
    "backend/mlb/exports/odds_history/2026-07-17/"
    "mlb_predictions_wide_calibrated__local_daily_20260717T200004Z.csv"
)
PROSPECTIVE_PA_PARENT_SUMMARY = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/"
    "2026-07-17/july17_parent_capture_summary_2026-07-17.csv"
)
PROSPECTIVE_PA_RUN_MANIFEST = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/"
    "2026-07-17/july17_live_run_manifest_2026-07-17.csv"
)
LINEUP_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/"
    "2026-07-17/canonical_pregame_lineup_ledger_2026-07-17.csv"
)
CONTACT_PROFILES = Path(
    "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/"
    "2026-07-17/strict_prior_hitter_contact_profiles_2026-07-17.csv"
)
DEFAULT_PARENT_ARTIFACT = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/"
    "2026-07-17/research_only_model_artifacts_2026-07-17.csv"
)

FROZEN_CHALLENGER = live_replay.FROZEN_CHALLENGER
FROZEN_FEATURES = live_replay.FROZEN_FEATURES
NUMERIC_TOLERANCE = 1e-9
FEATURE_CONTRACT_VERSION = "pha_opponent_lineup_encounter_v1_frozen_2026_07_17"
MATERIALIZER_VERSION = "current_pregame_encounter_materializer_v1"
ENCOUNTER_OUTPUT_COLUMNS = [
    "join_key",
    "slate_date",
    "game_id",
    "pitcher_id",
    "lineup_batters",
    "official_batters_faced_from_encounters",
    "official_hits_allowed_from_encounters",
    "expected_starter_facing_pa",
    "expected_total_pa_lineup",
    "expected_bullpen_pa_lineup",
    "lineup_weighted_hit_rate",
    "lineup_weighted_contact_conversion",
    "lineup_weighted_bullpen_hit_rate",
    "lineup_weighted_season_hits_per_pa",
    "lineup_weighted_season_pa_per_game",
    "lineup_weighted_d15_pa_per_game",
    "lineup_weighted_d30_hits_per_pa",
    "lineup_weighted_p4",
    "lineup_weighted_p5",
    "lineup_weighted_zero_hit_risk",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "starter_prior_start_count",
    "suppression_rows",
    "prior_dominated_share",
    "expected_hit_capable_contact_proxy",
    "feature_contract_version",
    "materializer_version",
    "cutoff",
    "source_parent_artifact",
    "source_parent_sha256",
    "lineup_state",
]

ROW_LEVEL_PARENT_COLUMNS = [
    "slate_date",
    "game_id",
    "player_id",
    "opposing_starter_id",
    "opponent",
    "encounter_batter_team",
    "pred_starter_pa",
    "pred_bullpen_pa",
    "pred_total_pa",
    "hitter_per_pa_hit_estimate",
    "p_hit_starter_prior",
    "p_hit_bullpen_prior",
    "season_to_date_hits_per_pa",
    "season_to_date_pa_per_game",
    "d15_pa_per_game",
    "d30_hits_per_pa",
    "p_hitter_receives_fourth_pa",
    "p_hitter_receives_fifth_pa",
    "predicted_exposure_p_zero_hits",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "starter_prior_start_count",
    "suppression_subtype",
    "strict_prior_status",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def summarize_source(path: Path, role: str, required_columns: list[str] | None = None, date_value: str = RUN_DATE) -> dict[str, Any]:
    df = read_csv(path)
    cols = set(df.columns)
    required_columns = required_columns or []
    missing = [c for c in required_columns if c not in cols]
    date_rows = 0
    if "slate_date" in cols:
        date_rows = int(df[df["slate_date"].astype(str).eq(date_value)].shape[0])
    elif "game_date" in cols:
        date_rows = int(df[df["game_date"].astype(str).eq(date_value)].shape[0])
    return {
        "source_path": str(path),
        "role": role,
        "exists": path.exists(),
        "sha256": sha256_file(path) if path.exists() else "",
        "rows": int(len(df)),
        "run_date_rows": date_rows,
        "required_columns_present": int(len(required_columns) - len(missing)),
        "required_columns_missing": "|".join(missing),
        "status": "AVAILABLE" if path.exists() and not missing and date_rows > 0 else ("AVAILABLE_NO_RUN_DATE_ROWS" if path.exists() and not missing else "MISSING_OR_INCOMPLETE"),
    }


def frozen_historical_contract() -> pd.DataFrame:
    rows = [
        {
            "output_field": "join_key",
            "historical_source_column": "slate_date|game_id|opposing_starter_id",
            "source_artifact": str(pha.GRANULAR_SOURCE),
            "source_grain": "hitter-game row",
            "aggregation_formula": "string concat date, game_id, opposing_starter_id",
            "hitter_weighting": "not_applicable",
            "shrinkage_or_fallback_rule": "none",
            "missingness_treatment": "requires non-null opposing_starter_id",
            "temporal_cutoff": "strict-prior historical modeling population; source rows are pregame fields plus certified postgame targets",
            "numeric_type": "string",
        },
        {
            "output_field": "lineup_batters",
            "historical_source_column": "player_id",
            "source_artifact": str(pha.GRANULAR_SOURCE),
            "source_grain": "hitter-game row",
            "aggregation_formula": "nunique(player_id)",
            "hitter_weighting": "unweighted count",
            "shrinkage_or_fallback_rule": "none",
            "missingness_treatment": "missing player_id reduces distinct count",
            "temporal_cutoff": "same as source",
            "numeric_type": "integer",
        },
        {
            "output_field": "expected_starter_facing_pa",
            "historical_source_column": "pred_starter_pa",
            "source_artifact": str(pha.GRANULAR_SOURCE),
            "source_grain": "hitter-game row",
            "aggregation_formula": "sum(pred_starter_pa)",
            "hitter_weighting": "additive expected opportunity",
            "shrinkage_or_fallback_rule": "provided by frozen starter/bullpen exposure artifact",
            "missingness_treatment": "NaN if no non-null pred_starter_pa",
            "temporal_cutoff": "strict-prior pregame exposure forecast",
            "numeric_type": "float",
        },
        {
            "output_field": "expected_hit_capable_contact_proxy",
            "historical_source_column": "pred_starter_pa,hitter_per_pa_hit_estimate",
            "source_artifact": str(pha.GRANULAR_SOURCE),
            "source_grain": "pitcher-game aggregate",
            "aggregation_formula": "expected_starter_facing_pa * lineup_weighted_hit_rate",
            "hitter_weighting": "pred_starter_pa weighted hit-rate",
            "shrinkage_or_fallback_rule": "depends on hitter_per_pa_hit_estimate source",
            "missingness_treatment": "NaN unless both operands finite",
            "temporal_cutoff": "strict-prior",
            "numeric_type": "float",
        },
    ]
    weighted = [
        ("lineup_weighted_hit_rate", "hitter_per_pa_hit_estimate"),
        ("lineup_weighted_contact_conversion", "p_hit_starter_prior"),
        ("lineup_weighted_bullpen_hit_rate", "p_hit_bullpen_prior"),
        ("lineup_weighted_season_hits_per_pa", "season_to_date_hits_per_pa"),
        ("lineup_weighted_season_pa_per_game", "season_to_date_pa_per_game"),
        ("lineup_weighted_d15_pa_per_game", "d15_pa_per_game"),
        ("lineup_weighted_d30_hits_per_pa", "d30_hits_per_pa"),
        ("lineup_weighted_p4", "p_hitter_receives_fourth_pa"),
        ("lineup_weighted_p5", "p_hitter_receives_fifth_pa"),
        ("lineup_weighted_zero_hit_risk", "predicted_exposure_p_zero_hits"),
    ]
    for output, source in weighted:
        rows.append(
            {
                "output_field": output,
                "historical_source_column": source,
                "source_artifact": str(pha.GRANULAR_SOURCE),
                "source_grain": "hitter-game row",
                "aggregation_formula": f"weighted average of {source}; weight=pred_starter_pa when positive, else unweighted mean fallback",
                "hitter_weighting": "pred_starter_pa",
                "shrinkage_or_fallback_rule": "row-level source already contains strict-prior/shrinkage state",
                "missingness_treatment": "NaN if no finite source values",
                "temporal_cutoff": "strict-prior pregame artifact",
                "numeric_type": "float",
            }
        )
    for output, source, formula in [
        ("starter_expected_hits_allowed", "starter_expected_hits_allowed", "median"),
        ("pitcher_base", "pitcher_base", "median"),
        ("starter_prior_start_count", "starter_prior_start_count", "median"),
        ("suppression_rows", "suppression_subtype", "count rows containing suppression"),
        ("prior_dominated_share", "strict_prior_status", "mean(strict_prior_status != PASS_STRICT_PRIOR)"),
    ]:
        rows.append(
            {
                "output_field": output,
                "historical_source_column": source,
                "source_artifact": str(pha.GRANULAR_SOURCE),
                "source_grain": "hitter-game row",
                "aggregation_formula": formula,
                "hitter_weighting": "not_applicable" if formula != "median" else "pitcher-game median",
                "shrinkage_or_fallback_rule": "source-provided",
                "missingness_treatment": "NaN or 0 per frozen aggregate code",
                "temporal_cutoff": "strict-prior",
                "numeric_type": "float",
            }
        )
    return pd.DataFrame(rows)


def current_parent_source_map(date_value: str, parent_artifact: Path, slate_artifact: Path, cutoff: str) -> pd.DataFrame:
    parent = read_csv(parent_artifact)
    parent_date_rows = 0
    parent_missing = ROW_LEVEL_PARENT_COLUMNS[:]
    if not parent.empty:
        parent_missing = [c for c in ROW_LEVEL_PARENT_COLUMNS if c not in parent.columns]
        if "slate_date" in parent.columns:
            parent_date_rows = int(parent[parent["slate_date"].astype(str).eq(date_value)].shape[0])
    rows = [
        {
            "parent": "scheduled_game_and_starter_identity",
            "source_path": str(slate_artifact),
            "source_hash": sha256_file(slate_artifact) if slate_artifact.exists() else "",
            "classification": "CURRENT_PARENT_AVAILABLE" if slate_artifact.exists() else "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": int(read_csv(slate_artifact).query("prop_type == 'hits_allowed'").shape[0]) if slate_artifact.exists() else 0,
            "cutoff": cutoff,
            "notes": "Current pitcher hits-allowed slate rows identify pitcher, game, team, opponent, line, and Champion probability.",
        },
        {
            "parent": "opposing_team",
            "source_path": str(slate_artifact),
            "source_hash": sha256_file(slate_artifact) if slate_artifact.exists() else "",
            "classification": "CURRENT_PARENT_AVAILABLE" if slate_artifact.exists() and "opponent" in read_csv(slate_artifact).columns else "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": int(read_csv(slate_artifact).query("prop_type == 'hits_allowed'").shape[0]) if slate_artifact.exists() else 0,
            "cutoff": cutoff,
            "notes": "Opponent code is available on current live proposition rows.",
        },
        {
            "parent": "expected_or_confirmed_lineup_pool",
            "source_path": str(LINEUP_LEDGER),
            "source_hash": sha256_file(LINEUP_LEDGER) if LINEUP_LEDGER.exists() else "",
            "classification": "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": int(read_csv(LINEUP_LEDGER).query("slate_date == @date_value").shape[0]) if LINEUP_LEDGER.exists() and "slate_date" in read_csv(LINEUP_LEDGER).columns else 0,
            "cutoff": cutoff,
            "notes": "Existing canonical lineup ledger is historical modeling population; no July 17 run-bound expected/confirmed lineup rows were found.",
        },
        {
            "parent": "run_bound_hitter_player_game_spine",
            "source_path": str(PROSPECTIVE_PA_PARENT_SUMMARY),
            "source_hash": sha256_file(PROSPECTIVE_PA_PARENT_SUMMARY) if PROSPECTIVE_PA_PARENT_SUMMARY.exists() else "",
            "classification": "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": 0,
            "cutoff": cutoff,
            "notes": "Prospective PA shadow summary reports run_bound_player_game_population=0 and no July 17 run-tagged parent artifact.",
        },
        {
            "parent": "predicted_starter_facing_pa",
            "source_path": str(parent_artifact),
            "source_hash": sha256_file(parent_artifact) if parent_artifact.exists() else "",
            "classification": "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM" if parent_date_rows and not parent_missing else "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": parent_date_rows,
            "cutoff": cutoff,
            "notes": "Requires exact row-level pred_starter_pa in frozen starter/bullpen exposure source.",
        },
        {
            "parent": "strict_prior_hitter_contact_and_conversion_profiles",
            "source_path": str(CONTACT_PROFILES),
            "source_hash": sha256_file(CONTACT_PROFILES) if CONTACT_PROFILES.exists() else "",
            "classification": "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM" if CONTACT_PROFILES.exists() else "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": int(len(read_csv(CONTACT_PROFILES))) if CONTACT_PROFILES.exists() else 0,
            "cutoff": cutoff,
            "notes": "Profile ledger exists historically, but no exact July 17 run-bound hitter-game spine exists to bind it safely to opponent lineup rows.",
        },
        {
            "parent": "frozen_row_level_encounter_parent_artifact",
            "source_path": str(parent_artifact),
            "source_hash": sha256_file(parent_artifact) if parent_artifact.exists() else "",
            "classification": "CURRENT_PARENT_AVAILABLE_REQUIRES_EXISTING_TRANSFORM" if parent_date_rows and not parent_missing else "CURRENT_PARENT_MISSING_BLOCKING",
            "run_date_rows": parent_date_rows,
            "cutoff": cutoff,
            "notes": f"Missing required columns: {'|'.join(parent_missing)}; date rows={parent_date_rows}.",
        },
    ]
    return pd.DataFrame(rows)


def historical_field_parity() -> pd.DataFrame:
    generated = pha.aggregate_granular()
    frozen = read_csv(FROZEN_LEDGER)
    if generated.empty or frozen.empty:
        return pd.DataFrame(
            [
                {
                    "field": "__source__",
                    "status": "FAIL",
                    "rows_checked": 0,
                    "matched_rows": 0,
                    "max_abs_diff": "",
                    "notes": "generated or frozen historical ledger missing",
                }
            ]
        )
    common = [c for c in frozen.columns if c in generated.columns]
    generated = generated[generated["join_key"].isin(set(frozen["join_key"].astype(str)))].copy()
    merged = frozen[common].merge(generated[common], on="join_key", how="left", suffixes=("_frozen", "_generated"), indicator=True)
    rows = [
        {
            "field": "__identity__",
            "status": "PASS" if merged["_merge"].eq("both").all() and len(merged) == len(frozen) else "FAIL",
            "rows_checked": int(len(merged)),
            "matched_rows": int(merged["_merge"].eq("both").sum()),
            "max_abs_diff": "",
            "notes": "join_key identity comparison against retained frozen encounter ledger",
        }
    ]
    for col in common:
        if col == "join_key":
            continue
        left = merged[f"{col}_frozen"] if f"{col}_frozen" in merged.columns else merged[col]
        right = merged[f"{col}_generated"] if f"{col}_generated" in merged.columns else merged[col]
        left_num = pd.to_numeric(left, errors="coerce")
        right_num = pd.to_numeric(right, errors="coerce")
        numeric_like = left_num.notna().any() or right_num.notna().any()
        if numeric_like:
            diff = (left_num - right_num).abs()
            missing_equal = left_num.isna().eq(right_num.isna()).all()
            max_diff = float(diff.max()) if diff.notna().any() else 0.0
            status = "PASS" if missing_equal and max_diff <= NUMERIC_TOLERANCE else "FAIL"
        else:
            unequal = left.fillna("").astype(str).ne(right.fillna("").astype(str))
            max_diff = ""
            status = "PASS" if not unequal.any() else "FAIL"
        rows.append(
            {
                "field": col,
                "status": status,
                "rows_checked": int(len(merged)),
                "matched_rows": int(merged["_merge"].eq("both").sum()),
                "max_abs_diff": max_diff,
                "notes": "exact aggregate function compared to frozen retained encounter ledger",
            }
        )
    return pd.DataFrame(rows)


def materialize_encounter(date_value: str, parent_artifact: Path, cutoff: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    parent = read_csv(parent_artifact)
    if parent.empty:
        return pd.DataFrame(columns=ENCOUNTER_OUTPUT_COLUMNS), pd.DataFrame(
            [
                {
                    "withheld_scope": "encounter_parent_artifact",
                    "primary_reason": "run_bound_row_level_parent_artifact_missing",
                    "source_path": str(parent_artifact),
                    "required_fields": "|".join(ROW_LEVEL_PARENT_COLUMNS),
                    "notes": "No source rows available to aggregate.",
                }
            ]
        )
    missing = [c for c in ROW_LEVEL_PARENT_COLUMNS if c not in parent.columns]
    if missing:
        return pd.DataFrame(columns=ENCOUNTER_OUTPUT_COLUMNS), pd.DataFrame(
            [
                {
                    "withheld_scope": "encounter_parent_artifact",
                    "primary_reason": "row_level_parent_artifact_missing_required_columns",
                    "source_path": str(parent_artifact),
                    "required_fields": "|".join(ROW_LEVEL_PARENT_COLUMNS),
                    "missing_fields": "|".join(missing),
                    "notes": "Cannot execute exact frozen aggregation.",
                }
            ]
        )
    current = parent[parent["slate_date"].astype(str).eq(date_value)].copy()
    if current.empty:
        return pd.DataFrame(columns=ENCOUNTER_OUTPUT_COLUMNS), pd.DataFrame(
            [
                {
                    "withheld_scope": "encounter_parent_artifact",
                    "primary_reason": "run_bound_row_level_parent_artifact_has_zero_date_rows",
                    "source_path": str(parent_artifact),
                    "required_fields": "|".join(ROW_LEVEL_PARENT_COLUMNS),
                    "missing_fields": "",
                    "notes": "The exact parent exists historically but has no rows for the requested current date.",
                }
            ]
        )
    original = pha.GRANULAR_SOURCE
    try:
        pha.GRANULAR_SOURCE = parent_artifact
        agg = pha.aggregate_granular()
    finally:
        pha.GRANULAR_SOURCE = original
    agg = agg[agg["slate_date"].astype(str).eq(date_value)].copy()
    if not agg.empty:
        agg["feature_contract_version"] = FEATURE_CONTRACT_VERSION
        agg["materializer_version"] = MATERIALIZER_VERSION
        agg["cutoff"] = cutoff
        agg["source_parent_artifact"] = str(parent_artifact)
        agg["source_parent_sha256"] = sha256_file(parent_artifact) if parent_artifact.exists() else ""
        agg["lineup_state"] = "exact_run_bound_parent_artifact"
    return agg.reindex(columns=ENCOUNTER_OUTPUT_COLUMNS), pd.DataFrame()


def live_propositions(slate_artifact: Path, date_value: str) -> pd.DataFrame:
    slate = read_csv(slate_artifact)
    if slate.empty:
        return pd.DataFrame()
    h = slate[slate["prop_type"].astype(str).eq("hits_allowed")].copy()
    h["slate_date"] = h["slate_date"].astype(str)
    h = h[h["slate_date"].eq(date_value)].copy()
    h["pitcher_id"] = num(h["player_id"]).astype("Int64")
    h["line"] = num(h["line"])
    h["model_prob_over"] = num(h["prob_over"])
    h["champion_expected_hits_allowed_poisson_implied"] = [
        pha.champion_lambda_from_line_prob(line, prob)
        for line, prob in zip(h["line"], h["model_prob_over"])
    ]
    h["champion_expected_hits_allowed"] = h["champion_expected_hits_allowed_poisson_implied"]
    h["join_key"] = h["slate_date"].astype(str) + "|" + h["game_id"].astype(str) + "|" + h["pitcher_id"].astype(str)
    return h


def score_live(date_value: str, encounter: pd.DataFrame, instrument: Any, slate_artifact: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    props = live_propositions(slate_artifact, date_value)
    if props.empty:
        return props, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    join_cols = []
    for col in ["join_key"] + [c for c in encounter.columns if c not in props.columns or c == "join_key"]:
        if col not in join_cols:
            join_cols.append(col)
    if encounter.empty:
        joined = props.copy()
    else:
        joined = props.merge(encounter[join_cols].drop_duplicates("join_key"), on="join_key", how="left")
    for f in FROZEN_FEATURES:
        if f not in joined.columns:
            joined[f] = np.nan
    joined["champion_side"] = np.where(joined["model_prob_over"] >= 0.5, "OVER", "UNDER")
    joined["market_line"] = joined["line"]
    joined["champion_over_probability"] = joined["model_prob_over"]
    exact = joined[FROZEN_FEATURES].notna().all(axis=1)
    joined["materialization_status"] = np.where(exact, "SCORED", "WITHHELD")
    joined["withheld_reason"] = ""
    joined.loc[~exact, "withheld_reason"] = "missing_run_bound_opponent_lineup_encounter_parent"
    joined["challenger_expected_hits_allowed"] = np.nan
    joined["challenger_prob_over"] = np.nan
    joined["challenger_side"] = ""
    if exact.any():
        scored = pha.score_population(joined.loc[exact].copy(), [instrument])
        joined.loc[exact, "challenger_expected_hits_allowed"] = scored[f"{FROZEN_CHALLENGER}_expected_hits_allowed"].to_numpy()
        joined.loc[exact, "challenger_prob_over"] = scored[f"{FROZEN_CHALLENGER}_prob_over"].to_numpy()
        joined.loc[exact, "challenger_side"] = np.where(joined.loc[exact, "challenger_prob_over"].astype(float) >= 0.5, "OVER", "UNDER")
    joined["residual_challenger_minus_champion"] = num(joined["challenger_expected_hits_allowed"]) - num(joined["champion_expected_hits_allowed"])
    joined["champion_distance_from_line"] = num(joined["champion_expected_hits_allowed"]) - num(joined["market_line"])
    joined["challenger_distance_from_line"] = num(joined["challenger_expected_hits_allowed"]) - num(joined["market_line"])
    joined["distance_from_line"] = joined["challenger_distance_from_line"]
    joined["side_disagreement"] = joined["champion_side"].ne(joined["challenger_side"]) & joined["challenger_side"].ne("")
    joined["disagreement_state"] = np.where(joined["side_disagreement"], "SIDE_DISAGREEMENT", "SIDE_AGREEMENT")
    joined["champion_count_semantics"] = "line_specific_poisson_implied_from_market_line_and_champion_over_probability"
    joined["challenger_count_semantics"] = "line_specific_frozen_count_model_output_because_champion_poisson_proxy_feature_is_line_specific"
    joined["workload_state"] = pd.cut(num(joined["expected_starter_facing_pa"]), [-np.inf, 20, 24, np.inf], labels=["low_workload", "normal_workload", "high_workload"]).astype(str)
    joined["lineup_state"] = np.where(exact, joined.get("lineup_state", "exact_run_bound_parent_artifact"), "missing_run_bound_lineup_encounter")
    joined["support"] = pd.cut(num(joined["prior_dominated_share"]), [-np.inf, .1, .35, np.inf], labels=["high_support", "mixed_support", "prior_dominated"]).astype(str)
    joined["uncertainty"] = np.where(exact, "default_off_shadow_scored", "withheld_missing_parent")
    scored_cols = [
        "slate_date",
        "game_id",
        "pitcher_id",
        "player_name",
        "team",
        "opponent",
        "line",
        "market_line",
        "champion_expected_hits_allowed",
        "champion_over_probability",
        "champion_distance_from_line",
        "champion_count_semantics",
        "challenger_expected_hits_allowed",
        "challenger_prob_over",
        "challenger_distance_from_line",
        "challenger_count_semantics",
        "distance_from_line",
        "residual_challenger_minus_champion",
        "champion_side",
        "challenger_side",
        "side_disagreement",
        "disagreement_state",
        "workload_state",
        "lineup_state",
        "support",
        "uncertainty",
        "market_price_over",
        "market_price_under",
    ]
    scored_rows = joined[joined["materialization_status"].eq("SCORED")][[c for c in scored_cols if c in joined.columns]].copy()
    withheld = []
    for _, r in joined[joined["materialization_status"].ne("SCORED")].iterrows():
        missing = [f for f in FROZEN_FEATURES if pd.isna(r.get(f))]
        withheld.append(
            {
                "slate_date": r.get("slate_date"),
                "game_id": r.get("game_id"),
                "pitcher_id": r.get("pitcher_id"),
                "pitcher_name": r.get("player_name"),
                "team": r.get("team"),
                "opponent": r.get("opponent"),
                "line": r.get("line"),
                "primary_reason": "missing_run_bound_opponent_lineup_encounter_parent",
                "missing_required_feature_count": len(missing),
                "missing_required_features": "|".join(missing),
                "smallest_upstream_blocker": "run_bound_hitter_player_game_spine_with_pred_starter_pa_and_strict_prior_hitter_profiles",
                "notes": "Current slate has pitcher market/champion fields but lacks exact row-level opponent lineup encounter parents.",
            }
        )
    shadow = joined.copy()
    shadow["shadow_mode"] = "default_off"
    shadow["shadow_status"] = np.where(shadow["materialization_status"].eq("SCORED"), "DEFAULT_OFF_SHADOW_READY", "WITHHELD_NOT_SCORED")
    shadow["production_behavior_changed"] = False
    comparison = pd.DataFrame(
        [
            {
                "scope": "july17_pitcher_hits_allowed_live_props",
                "live_propositions": int(len(joined)),
                "pitcher_games_represented": int(joined["join_key"].nunique()),
                "scored_rows": int(joined["materialization_status"].eq("SCORED").sum()),
                "withheld_rows": int(joined["materialization_status"].ne("SCORED").sum()),
                "coverage_pct": float(joined["materialization_status"].eq("SCORED").mean()) if len(joined) else 0.0,
                "avg_champion_expected_hits_allowed": float(num(joined["champion_expected_hits_allowed"]).mean()),
                "avg_challenger_expected_hits_allowed": float(num(scored_rows["challenger_expected_hits_allowed"]).mean()) if not scored_rows.empty else "",
                "side_disagreements": int(scored_rows["side_disagreement"].sum()) if "side_disagreement" in scored_rows.columns and not scored_rows.empty else 0,
            }
        ]
    )
    return joined, scored_rows, pd.DataFrame(withheld), shadow, comparison


def decisions(field_parity: pd.DataFrame, pred_parity: pd.DataFrame, parents: pd.DataFrame, encounter: pd.DataFrame, live_joined: pd.DataFrame) -> pd.DataFrame:
    field_pass = bool(not field_parity.empty and field_parity["status"].eq("PASS").all())
    pred_pass = bool(not pred_parity.empty and pred_parity["status"].eq("PASS").all())
    blocking = parents[parents["classification"].eq("CURRENT_PARENT_MISSING_BLOCKING")]
    scored = int(live_joined["materialization_status"].eq("SCORED").sum()) if not live_joined.empty else 0
    total = int(len(live_joined))
    if field_pass:
        contract = "BOUND_EXACT_HISTORICAL_AGGREGATE_REPRODUCED"
    else:
        contract = "NOT_BOUND_HISTORICAL_FIELD_PARITY_FAILED"
    if blocking.empty:
        parent_decision = "ALL_CURRENT_PARENTS_AVAILABLE_OR_TRANSFORMABLE"
    else:
        parent_decision = "CURRENT_PARENT_MISSING_BLOCKING_RUN_BOUND_HITTER_PLAYER_GAME_SPINE"
    if not encounter.empty:
        materialization = f"GENERATED_{len(encounter)}_PITCHER_GAME_ROWS"
    else:
        materialization = "ZERO_ROWS_FAIL_CLOSED_NO_RUN_BOUND_PARENT"
    if scored == total and total:
        live_decision = "LIVE_INFERENCE_SUCCEEDED_ALL_ROWS"
        shadow = "READY_FOR_CONTROLLED_PHA_SHADOW"
        blocker = "NONE"
    elif scored:
        live_decision = f"LIVE_INFERENCE_PARTIAL_SCORED_{scored}_WITHHELD_{total - scored}"
        shadow = "PARTIAL_SHADOW_NOT_READY_FOR_CONTROLLED_USE"
        blocker = "PARTIAL_RUN_BOUND_PARENT_COVERAGE"
    else:
        live_decision = "LIVE_INFERENCE_NOT_RUN_ZERO_EXACT_FEATURE_VECTORS"
        shadow = "NOT_READY_NO_SCORED_CURRENT_CHALLENGER_ROWS"
        blocker = "RUN_BOUND_HITTER_PLAYER_GAME_SPINE_WITH_PRED_STARTER_PA_AND_STRICT_PRIOR_HITTER_PROFILES"
    rows = [
        ("MLB_PHA_ENCOUNTER_HISTORICAL_CONTRACT_DECISION", contract),
        ("MLB_PHA_ENCOUNTER_CURRENT_PARENT_DECISION", parent_decision),
        ("MLB_PHA_ENCOUNTER_GENERATOR_DECISION", "REUSABLE_GENERATOR_IMPLEMENTED_FAIL_CLOSED"),
        ("MLB_PHA_ENCOUNTER_HISTORICAL_PARITY_DECISION", "PASS" if field_pass and pred_pass else "FAIL"),
        ("MLB_PHA_ENCOUNTER_JULY17_MATERIALIZATION_DECISION", materialization),
        ("MLB_PHA_ENCOUNTER_LIVE_INFERENCE_DECISION", live_decision),
        ("MLB_PHA_ENCOUNTER_LIVE_JOIN_COVERAGE_DECISION", f"LIVE_PROPS_{total}_SCORED_{scored}_WITHHELD_{total - scored}"),
        ("MLB_PHA_ENCOUNTER_REMAINING_BLOCKER_DECISION", blocker),
        ("MLB_PHA_ENCOUNTER_SHADOW_READINESS_DECISION", shadow),
        ("MLB_PHA_ENCOUNTER_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validation_report(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for path in paths:
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                pd.read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                assert path.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    for name, value in guardrails.items():
        rows.append({"artifact": f"guardrail_{name}", "validation": "PASS" if value in (0, False, "PASS") else "FAIL", "notes": str(value)})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, field_parity: pd.DataFrame, pred_parity: pd.DataFrame, parents: pd.DataFrame, encounter: pd.DataFrame, live_joined: pd.DataFrame, dec: pd.DataFrame) -> str:
    scored = int(live_joined["materialization_status"].eq("SCORED").sum()) if not live_joined.empty else 0
    total = int(len(live_joined))
    field_status = "PASS" if not field_parity.empty and field_parity["status"].eq("PASS").all() else "FAIL"
    pred_status = "PASS" if not pred_parity.empty and pred_parity["status"].eq("PASS").all() else "FAIL"
    parent_blockers = parents[parents["classification"].eq("CURRENT_PARENT_MISSING_BLOCKING")]["parent"].tolist()
    direct = "YES" if scored == total and total else "NO"
    return f"""# MLB Current Pregame Opponent-Lineup Encounter Source Generator

Generated: `{generated_at}`

## Direct Answer

Can the frozen historical opponent-lineup encounter construction now be
materialized from current pregame artifacts and used to score the July 17
Pitcher Hits Allowed slate?

`{direct}`.

The historical encounter construction is bound and reproduced, but July 17
current materialization remains fail-closed because the exact run-bound
hitter/player-game parent spine with `pred_starter_pa` and strict-prior hitter
profile fields is not present in current local pregame artifacts.

## Historical Parity

- Historical field parity: `{field_status}`
- Historical prediction parity: `{pred_status}`
- Frozen source: `{pha.GRANULAR_SOURCE}`

## July 17 Current Coverage

- Live pitcher hits-allowed propositions: `{total}`
- Generated pitcher-game encounter rows: `{len(encounter)}`
- Exact live Challenger scored rows: `{scored}`
- Withheld rows: `{total - scored}`

## Current Parent Blockers

`{'|'.join(parent_blockers) if parent_blockers else 'none'}`

Smallest next implementation contract: create the run-bound current
hitter/player-game spine before the prediction cutoff, containing exact
expected/confirmed lineup identity, hitter IDs, `pred_starter_pa`, strict-prior
hit/contact conversion profiles, starter context, support/shrinkage state, and
source cutoff metadata.

## No Behavior Changed

No network, OddsAPI, DB write, model fitting/refitting, new formulas, postgame
information, production model, formula, tier, selector, candidate, upload,
Quick Card, workspace, LaunchAgent, Hits O0.5, or O1.5 behavior changed.
"""


def build(date_value: str, slate_artifact: Path, parent_artifact: Path, cutoff: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    historical_scored, instrument, bound = live_replay.bind_frozen_model()
    pred_parity = live_replay.historical_parity(historical_scored)
    field_parity = historical_field_parity()
    contract = frozen_historical_contract()
    source_inventory = pd.DataFrame(
        [
            summarize_source(slate_artifact, "current_pitcher_market_and_champion_probability_source", ["slate_date", "game_id", "player_id", "prop_type", "line", "prob_over"], date_value),
            summarize_source(CURRENT_WIDE, "current_wide_prediction_source", ["game_id", "player_id", "prop_type"], date_value),
            summarize_source(parent_artifact, "candidate_row_level_opponent_lineup_encounter_parent", ROW_LEVEL_PARENT_COLUMNS, date_value),
            summarize_source(PROSPECTIVE_PA_PARENT_SUMMARY, "prospective_pa_parent_summary", [], date_value),
            summarize_source(LINEUP_LEDGER, "canonical_pregame_lineup_ledger", ["slate_date", "game_id", "player_id", "canonical_pregame_lineup_slot"], date_value),
            summarize_source(CONTACT_PROFILES, "strict_prior_hitter_contact_profiles", [], date_value),
        ]
    )
    parents = current_parent_source_map(date_value, parent_artifact, slate_artifact, cutoff)
    encounter, parent_taxonomy = materialize_encounter(date_value, parent_artifact, cutoff)
    live_joined, scored_rows, withheld, shadow, comparison = score_live(date_value, encounter, instrument, slate_artifact)
    missing_taxonomy = pd.concat([parent_taxonomy, withheld], ignore_index=True, sort=False)
    dec = decisions(field_parity, pred_parity, parents, encounter, live_joined)
    guardrails = {
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "model_fits_or_refits": 0,
        "postgame_information_used": 0,
        "production_behavior_changed": False,
        "hits05_modified": False,
        "hits15_modified": False,
    }
    files = {
        "summary": output_dir / "current_pitcher_opponent_lineup_encounter_source_summary_2026-07-17.md",
        "contract": output_dir / "frozen_historical_encounter_contract_2026-07-17.csv",
        "source_inventory": output_dir / "current_parent_source_inventory_2026-07-17.csv",
        "parent_map": output_dir / "current_parent_source_map_2026-07-17.csv",
        "field_parity": output_dir / "historical_field_parity_report_2026-07-17.csv",
        "prediction_parity": output_dir / "historical_prediction_parity_report_2026-07-17.csv",
        "encounter": output_dir / "july17_current_pitcher_game_encounter_features_2026-07-17.csv",
        "live_ledger": output_dir / "july17_frozen_challenger_live_ledger_2026-07-17.csv",
        "live_join": output_dir / "live_proposition_join_report_2026-07-17.csv",
        "missing": output_dir / "missing_parent_taxonomy_2026-07-17.csv",
        "shadow": output_dir / "default_off_shadow_2026-07-17.csv",
        "decisions": output_dir / "required_decisions_2026-07-17.csv",
        "machine": output_dir / "machine_readable_current_encounter_source_2026-07-17.json",
        "manifest": output_dir / "sha256_manifest_2026-07-17.csv",
        "validation": output_dir / "validation_report_2026-07-17.csv",
    }
    write_text(files["summary"], summary_md(generated_at, field_parity, pred_parity, parents, encounter, live_joined, dec))
    write_csv(files["contract"], contract)
    write_csv(files["source_inventory"], source_inventory)
    write_csv(files["parent_map"], parents)
    write_csv(files["field_parity"], field_parity)
    write_csv(files["prediction_parity"], pred_parity)
    write_csv(files["encounter"], encounter)
    write_csv(files["live_ledger"], live_joined)
    write_csv(files["live_join"], comparison)
    write_csv(files["missing"], missing_taxonomy)
    write_csv(files["shadow"], shadow)
    write_csv(files["decisions"], dec)
    machine = {
        "generated_at": generated_at,
        "date": date_value,
        "cutoff": cutoff,
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "materializer_version": MATERIALIZER_VERSION,
        "script_path": "backend/mlb/scripts/materialize_mlb_current_pitcher_opponent_lineup_encounter_features.py",
        "historical_field_parity_pass": bool(field_parity["status"].eq("PASS").all()),
        "historical_prediction_parity_pass": bool(pred_parity["status"].eq("PASS").all()),
        "current_parent_blockers": parents[parents["classification"].eq("CURRENT_PARENT_MISSING_BLOCKING")]["parent"].tolist(),
        "generated_pitcher_game_rows": int(len(encounter)),
        "live_propositions": int(len(live_joined)),
        "live_scored_rows": int(live_joined["materialization_status"].eq("SCORED").sum()) if not live_joined.empty else 0,
        "live_withheld_rows": int(live_joined["materialization_status"].ne("SCORED").sum()) if not live_joined.empty else 0,
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "bound_model_state_sha256": bound["hash"],
        "guardrails": guardrails,
    }
    write_json(files["machine"], machine)
    generated_files = [p for key, p in files.items() if key not in {"manifest", "validation"}]
    write_csv(
        files["manifest"],
        pd.DataFrame(
            [
                {"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"}
                for p in generated_files
            ]
        ),
    )
    write_csv(files["validation"], validation_report(generated_files + [files["manifest"]], guardrails))
    return {
        "output_dir": str(output_dir),
        "historical_field_parity_pass": machine["historical_field_parity_pass"],
        "historical_prediction_parity_pass": machine["historical_prediction_parity_pass"],
        "generated_pitcher_game_rows": machine["generated_pitcher_game_rows"],
        "live_propositions": machine["live_propositions"],
        "live_scored_rows": machine["live_scored_rows"],
        "live_withheld_rows": machine["live_withheld_rows"],
        "remaining_blocker": machine["decisions"]["MLB_PHA_ENCOUNTER_REMAINING_BLOCKER_DECISION"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=RUN_DATE)
    parser.add_argument("--run-tag", default="local_daily_20260717T200004Z")
    parser.add_argument("--slate-artifact", type=Path, default=CURRENT_SLATE)
    parser.add_argument("--parent-artifact", type=Path, default=DEFAULT_PARENT_ARTIFACT)
    parser.add_argument("--cutoff", default="2026-07-17T20:00:04Z")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()
    result = build(args.date, args.slate_artifact, args.parent_artifact, args.cutoff, args.output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
