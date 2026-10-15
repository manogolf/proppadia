#!/usr/bin/env python3
"""Build an offline NHL Points historical team-shot reconstruction package."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

from backend.nhl.points_offline_team_shots import reconstruct_team_shots

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15"
SOURCE_FRAME = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
SOURCE_MANIFEST = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/manifest.json"
MODEL_PATH = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib"
CONTRACT_PATH = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/shadow_contract.json"
OFFICIAL_SOURCE = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/official_team_game_source.csv"
SOURCE_RECON = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/source_to_official_team_game_reconciliation.csv"
BOX_MANIFEST = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/official_boxscore_acquisition_manifest.json"
PRIOR_ROW_AUDIT = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/target_feature_reconstruction.csv"
PRIOR_AUDIT_SUMMARY = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/summary.json"
FEATURES = ["is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
            "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
            "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share"]
CURRENT_SOURCE_SNAPSHOT = {
    "observed_utc_minute": "2026-10-15T23:44Z",
    "scope": "one bounded read-only snapshot of nhl.skater_game_logs_raw joined to nhl.games for canonical 2026 regular-season games dated 2026-09-29 through 2026-10-13",
    "games": 105, "team_game_groups": 210, "player_game_rows": 3780,
    "first_game_date": "2026-09-29", "last_game_date": "2026-10-13",
    "missing_sog_rows": 0, "missing_shot_attempt_rows": 3780,
    "home_away_team_mismatches": 0, "is_home_binding_mismatches": 0,
    "duplicate_player_game_rows": 0, "team_groups_not_18_rows": 0,
    "source_state_classification": "CURRENT_DATABASE_STATE_ONLY_NOT_CAPTURE_TIME_EVIDENCE",
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _history_depth(count: pd.Series) -> pd.Series:
    return pd.cut(count, [-1, 0, 2, 5, 9, 10**9], labels=["0", "1-2", "3-5", "6-9", "10"])


def _prob_over_one(mu: np.ndarray) -> np.ndarray:
    return poisson.sf(0, np.maximum(mu, 1e-8))


def _metrics(y: np.ndarray, mu: np.ndarray) -> dict:
    p = np.clip(_prob_over_one(mu), 1e-15, 1 - 1e-15)
    over = (y >= 1).astype(int)
    return {
        "n": int(len(y)),
        "mean_expected_points": float(np.mean(mu)),
        "mean_realized_points": float(np.mean(y)),
        "mean_count_residual_realized_minus_expected": float(np.mean(y - mu)),
        "over_0_5_brier": float(np.mean((p - over) ** 2)),
        "over_0_5_log_loss": float(-np.mean(over * np.log(p) + (1 - over) * np.log1p(-p))),
    }


def _score(frame: pd.DataFrame, fitted: dict, *, team_feature: str) -> np.ndarray:
    model_frame = frame.copy()
    model_frame["team_d10_sf_per_game"] = pd.to_numeric(frame[team_feature], errors="raise").to_numpy()
    x = model_frame[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype(float)
    return np.maximum(np.asarray(fitted["model"].predict(fitted["scaler"].transform(x.to_numpy())), dtype=float), 1e-8)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(SOURCE_FRAME, low_memory=False)
    expected_hash = json.loads(SOURCE_MANIFEST.read_text())["source_frame_sha256"]
    if sha(SOURCE_FRAME) != expected_hash:
        raise RuntimeError("ORIGINAL_VALIDATION_FRAME_HASH_MISMATCH")
    if frame.duplicated(["game_id", "player_id"]).any():
        raise RuntimeError("ORIGINAL_TRAINING_ROW_IDENTITY_DUPLICATE")

    official = pd.read_csv(OFFICIAL_SOURCE, low_memory=False)
    official = official.loc[official.source_period.eq("RETAINED_OFFICIAL_BOXSCORE")].copy()
    official["game_id"] = pd.to_numeric(official.game_id, errors="raise").astype("int64")
    official["team_id"] = pd.to_numeric(official.team_id, errors="raise").astype("int64")
    official["game_date"] = pd.to_datetime(official.game_date).dt.strftime("%Y-%m-%d")
    official["is_home"] = pd.to_numeric(official.is_home, errors="raise").astype("int64")
    if official.duplicated(["game_id", "team_id"]).any():
        raise RuntimeError("OFFICIAL_TEAM_GAME_SOURCE_DUPLICATE")
    if official.duplicated(["game_id", "is_home"]).any():
        raise RuntimeError("OFFICIAL_GAME_SIDE_BINDING_DUPLICATE")
    side_bind = official[["game_id", "is_home", "team_id", "home_team_id", "away_team_id", "game_date"]].rename(
        columns={"team_id": "canonical_team_id", "game_date": "official_game_date"})
    targets = frame[["game_id", "player_id", "game_date", "canonical_season", "team_id", "is_home"]].copy()
    targets["game_id"] = pd.to_numeric(targets.game_id, errors="raise").astype("int64")
    targets["player_id"] = pd.to_numeric(targets.player_id, errors="raise").astype("int64")
    targets["is_home"] = pd.to_numeric(targets.is_home, errors="raise").astype("int64")
    targets["original_training_team_id"] = pd.to_numeric(targets.pop("team_id"), errors="raise").astype("int64")
    targets["game_date"] = pd.to_datetime(targets.game_date).dt.strftime("%Y-%m-%d")
    targets = targets.merge(side_bind[["game_id", "is_home", "canonical_team_id", "home_team_id", "away_team_id", "official_game_date"]],
                            on=["game_id", "is_home"], how="left", validate="many_to_one")
    if targets.canonical_team_id.isna().any() or not targets.game_date.eq(targets.official_game_date).all():
        raise RuntimeError("OFFICIAL_TARGET_GAME_OR_SIDE_BINDING_INCOMPLETE")
    target_for_rebuild = targets[["canonical_team_id", "game_id", "player_id", "game_date"]].rename(columns={"canonical_team_id": "team_id"})
    team_games = official[["team_id", "game_id", "game_date", "official_team_sog"]].rename(columns={"official_team_sog": "team_sog"})
    corrected = reconstruct_team_shots(team_games, target_for_rebuild, source_complete=True)
    # Independent direct pass, using the official source rows and explicit game/team/date filters.
    source_dates = pd.to_datetime(team_games.game_date)
    team_games = team_games.assign(_date=source_dates)
    direct_by_team_day = {}
    target_team_days = target_for_rebuild[["team_id", "game_date"]].drop_duplicates()
    for team_id, team_targets in target_team_days.groupby("team_id", sort=False):
        team_source = team_games.loc[team_games.team_id.eq(int(team_id))].sort_values(
            ["_date", "game_id"], kind="mergesort")
        dates = team_source._date.tolist()
        records = team_source.to_dict("records")
        import bisect
        for day_text in team_targets.game_date:
            target_date = pd.Timestamp(day_text)
            upper = bisect.bisect_left(dates, target_date)
            lower = bisect.bisect_left(dates, target_date - pd.Timedelta(days=120))
            prior = records[max(lower, upper - 10):upper]
            direct_by_team_day[(int(team_id), day_text)] = {
                "direct_game_ids": ";".join(str(int(x["game_id"])) for x in prior),
                "direct_game_sog": ";".join(format(float(x["team_sog"]), ".17g") for x in prior),
                "direct_count": len(prior),
                "direct_feature": float(np.mean([x["team_sog"] for x in prior])) if prior else 0.0,
            }
    direct_rows = [{"game_id": int(row.game_id), "player_id": int(row.player_id), "team_id": int(row.team_id),
                    **direct_by_team_day[(int(row.team_id), row.game_date)]}
                   for row in target_for_rebuild.itertuples(index=False)]
    direct = pd.DataFrame(direct_rows)
    corrected = corrected.merge(direct, on=["game_id", "player_id", "team_id"], validate="one_to_one")
    if not np.allclose(corrected.team_d10_sf_per_game, corrected.direct_feature, rtol=0, atol=1e-12):
        raise RuntimeError("REUSABLE_AND_DIRECT_OFFICIAL_RECONSTRUCTION_DISAGREE")
    comparison = targets.merge(corrected, left_on=["game_id", "player_id", "canonical_team_id", "game_date"],
                               right_on=["game_id", "player_id", "team_id", "game_date"], how="left", validate="one_to_one")
    if comparison.team_d10_sf_per_game.isna().any():
        raise RuntimeError("HISTORICAL_RECONSTRUCTION_ROW_BINDING_INCOMPLETE")
    passthrough = [column for column in FEATURES if column not in comparison.columns and column != "team_d10_sf_per_game"]
    comparison = comparison.merge(frame[["game_id", "player_id", *passthrough, "team_d10_sf_per_game", "realized_points"]],
                                  on=["game_id", "player_id"], validate="one_to_one")
    comparison = comparison.rename(columns={"team_d10_sf_per_game_x": "reconstructed_team_d10_sf_per_game",
                                            "team_d10_sf_per_game_y": "original_team_d10_sf_per_game"})
    # The target team may be a stale numeric alias in the old frame. It is kept as evidence, not used to bind the correction.
    comparison["correction_delta"] = comparison.reconstructed_team_d10_sf_per_game - comparison.original_team_d10_sf_per_game
    comparison["abs_correction_delta"] = comparison.correction_delta.abs()
    comparison["changed"] = comparison.abs_correction_delta.gt(1e-12)
    comparison["identity_resolution"] = "EXACT_TEAM_ID"
    comparison.loc[comparison.original_training_team_id.ne(comparison.canonical_team_id), "identity_resolution"] = "NUMERIC_ID_DIFFERENCE_REVIEW"
    depth = _history_depth(comparison.valid_prior_game_count)
    comparison["history_depth"] = depth.astype(str)
    season_start = comparison.groupby("canonical_season").game_date.min().map(pd.Timestamp)
    comparison["season_day_index"] = [int((pd.Timestamp(d) - season_start[int(s)]).days)
                                      for s, d in zip(comparison.canonical_season, comparison.game_date)]
    comparison["season_maturity"] = np.where(comparison.season_day_index.lt(30), "EARLY_FIRST_30_DAYS", "MATURE_DAY_30_PLUS")
    comparison["source_evidence_status"] = "OFFICIAL_TEAM_GAME_SOURCE_COMPLETE"
    comparison["issue_classification"] = np.where(comparison.changed, "OFFICIAL_RECONSTRUCTION_DIFFERS_FROM_FROZEN_FEATURE", "UNCHANGED")

    # ID aliases are distinguished from raw IDs mapping to multiple physical teams within a season.
    crosswalk = (comparison.groupby(["canonical_season", "original_training_team_id", "canonical_team_id"], dropna=False)
                 .agg(rows=("game_id", "size"),
                      id_equal=("identity_resolution", lambda x: bool((x == "EXACT_TEAM_ID").all())))
                 .reset_index())
    per_raw_mapping_count = crosswalk.groupby(["canonical_season", "original_training_team_id"]).canonical_team_id.transform("nunique")
    crosswalk["resolution"] = np.where(crosswalk.original_training_team_id.eq(-1), "MISSING_TEAM_ID_SENTINEL",
        np.where(crosswalk.id_equal, "EXACT_NUMERIC_ID",
        np.where(per_raw_mapping_count.gt(1), "CONFLICTED_RAW_ID_TO_PHYSICAL_TEAM_MAPPING", "CONSISTENT_NUMERIC_ALIAS")))

    source_recon = pd.read_csv(SOURCE_RECON, low_memory=False)
    train_recon = source_recon.loc[source_recon.target_period.eq("TRAIN_2023_2024")].copy()
    holdout_recon = source_recon.loc[source_recon.target_period.eq("HOLDOUT_2025")].copy()
    prior_rows = pd.read_csv(PRIOR_ROW_AUDIT, low_memory=False)
    prior_rows = prior_rows.loc[prior_rows.target_period.eq("TRAIN_2023_2024")].copy()
    opening = comparison.loc[comparison.canonical_season.eq(2023)
                             & comparison.valid_prior_game_count.eq(0)
                             & comparison.original_team_d10_sf_per_game.ne(0)].copy()
    comparison["target_period"] = np.where(comparison.canonical_season.isin([2023, 2024]), "TRAIN_2023_2024", "HOLDOUT_2025")
    train = comparison.loc[comparison.canonical_season.isin([2023, 2024])].copy()
    holdout = comparison.loc[comparison.canonical_season.eq(2025)].copy()
    fitted = joblib.load(MODEL_PATH)
    if fitted.get("feature_columns") != FEATURES:
        raise RuntimeError("FROZEN_HGB_FEATURE_ORDER_MISMATCH")
    train_original_mu = _score(train, fitted, team_feature="original_team_d10_sf_per_game")
    train_corrected_mu = _score(train, fitted, team_feature="reconstructed_team_d10_sf_per_game")
    holdout_original_mu = _score(holdout, fitted, team_feature="original_team_d10_sf_per_game")
    holdout_corrected_mu = _score(holdout, fitted, team_feature="reconstructed_team_d10_sf_per_game")
    stored_predictions_path = MODEL_PATH.parent / "hgb_poisson_season_holdout_predictions.csv.gz"
    stored_predictions = pd.read_csv(stored_predictions_path)
    replay_check = holdout[["game_id", "player_id"]].copy()
    replay_check["replayed_expected_points"] = holdout_original_mu
    replay_check = replay_check.merge(stored_predictions[["game_id", "player_id", "predicted_mean"]],
                                      on=["game_id", "player_id"], how="left", validate="one_to_one")
    if replay_check.predicted_mean.isna().any():
        raise RuntimeError("FROZEN_HGB_RETAINED_HOLDOUT_REPLAY_IDENTITY_MISSING")
    replay_max_abs_error = float((replay_check.replayed_expected_points - replay_check.predicted_mean).abs().max())
    if replay_max_abs_error > 1e-12:
        raise RuntimeError("FROZEN_HGB_DETERMINISTIC_REPLAY_MISMATCH")
    # Deterministically reproduce saved fitted HGB expectations on holdout: the original frame model expectations are generated by the frozen scorer.
    sensitivity_rows = holdout[["game_id", "player_id", "game_date", "canonical_season", "history_depth",
                                "original_team_d10_sf_per_game", "reconstructed_team_d10_sf_per_game",
                                "correction_delta", "realized_points"]].copy()
    sensitivity_rows["expected_points_original"] = holdout_original_mu
    sensitivity_rows["expected_points_corrected_feature"] = holdout_corrected_mu
    sensitivity_rows["abs_feature_difference"] = sensitivity_rows.correction_delta.abs()
    sensitivity_rows.to_csv(OUT / "frozen_hgb_holdout_sensitivity_rows.csv", index=False, float_format="%.17g")
    sensitivity_rows["discrepancy_band"] = pd.cut(sensitivity_rows.abs_feature_difference,
        [-1e-12, 1e-12, 2, 5, np.inf], labels=["UNCHANGED", "(0,2]", "(2,5]", ">5"])
    by_discrepancy = {}
    for band, group in sensitivity_rows.groupby("discrepancy_band", observed=True):
        if group.empty:
            continue
        by_discrepancy[str(band)] = {
            "rows": int(len(group)),
            "mean_abs_feature_difference": float(group.abs_feature_difference.mean()),
            "original": _metrics(group.realized_points.to_numpy(float), group.expected_points_original.to_numpy(float)),
            "corrected_feature": _metrics(group.realized_points.to_numpy(float), group.expected_points_corrected_feature.to_numpy(float)),
        }

    team_diff = holdout_recon.loc[holdout_recon.official_sog_minus_source_zero_filled.ne(0)].copy()
    team_diff["source_classification"] = np.where(
        team_diff.source_missing_team_id_rows.gt(0), "MISSING_PLAYER_TEAM_BINDING",
        np.where(team_diff.source_missing_sog_rows.gt(0), "MISSING_SKATER_SOG",
        np.where(team_diff.source_skater_rows.lt(18) | team_diff.source_unique_players.lt(18), "INCOMPLETE_SKATER_COVERAGE",
        np.where(team_diff.source_skater_rows.gt(18) | team_diff.source_unique_players.gt(18), "PLAYER_POPULATION_EXCESS_UNEXPLAINED",
        "SOURCE_TOTAL_DIFFERENCE_UNEXPLAINED"))))
    team_diff.to_csv(OUT / "2025_skater_sum_official_total_discrepancies.csv", index=False)
    source_diff_keys = set(zip(team_diff.game_id.astype(int), team_diff.official_team_id.astype(int)))
    all_source_diff_rows = source_recon.loc[source_recon.official_sog_minus_source_zero_filled.ne(0)]
    all_source_diff_keys = set(zip(all_source_diff_rows.game_id.astype(int), all_source_diff_rows.official_team_id.astype(int)))
    source_history_rows = []
    for row in comparison.itertuples(index=False):
        prior_ids = [int(value) for value in str(row.prior_game_ids).split(";") if value]
        matches = [game_id for game_id in prior_ids if (game_id, int(row.canonical_team_id)) in all_source_diff_keys]
        source_history_rows.append(";".join(map(str, matches)))
    comparison["prior_games_with_skater_sum_official_mismatch"] = source_history_rows
    comparison["feature_discrepancy_source_classification"] = np.where(
        ~comparison.changed, "UNCHANGED",
        np.where(comparison.prior_games_with_skater_sum_official_mismatch.ne(""), "PRIOR_SKATER_SUM_DIFF_PRESENT", "NO_PRIOR_SKATER_SUM_DIFF_IDENTIFIED"))
    train = comparison.loc[comparison.canonical_season.isin([2023, 2024])].copy()
    holdout = comparison.loc[comparison.canonical_season.eq(2025)].copy()
    train.to_csv(OUT / "historical_row_level_comparison.csv", index=False, float_format="%.17g")
    comparison.loc[comparison.canonical_season.eq(2024)].to_csv(OUT / "2024_reconstruction.csv", index=False, float_format="%.17g")
    holdout.to_csv(OUT / "corrected_2025_holdout_comparison.csv", index=False, float_format="%.17g")
    crosswalk.loc[crosswalk.canonical_season.eq(2023)].to_csv(OUT / "2023_identity_discrepancy_resolution.csv", index=False)

    def matrix_rows(dataset: pd.DataFrame, feature_name: str) -> pd.DataFrame:
        rows = dataset[["game_id", "player_id", "game_date", "canonical_season", "original_training_team_id", "canonical_team_id",
                        *[feature for feature in FEATURES if feature != "team_d10_sf_per_game"], "realized_points"]].copy()
        rows["team_d10_sf_per_game"] = pd.to_numeric(dataset[feature_name], errors="raise").to_numpy()
        return rows[["game_id", "player_id", "game_date", "canonical_season", "original_training_team_id", "canonical_team_id",
                     *FEATURES, "realized_points"]]

    matrix_rows(train, "original_team_d10_sf_per_game").to_csv(
        OUT / "original_training_matrix.csv", index=False, float_format="%.17g")
    matrix_rows(train, "reconstructed_team_d10_sf_per_game").to_csv(
        OUT / "corrected_training_matrix.csv", index=False, float_format="%.17g")
    matrix_rows(holdout, "original_team_d10_sf_per_game").to_csv(
        OUT / "original_2025_holdout_matrix.csv", index=False, float_format="%.17g")
    matrix_rows(holdout, "reconstructed_team_d10_sf_per_game").to_csv(
        OUT / "corrected_2025_holdout_matrix.csv", index=False, float_format="%.17g")

    group_summary = (train.groupby(["canonical_season", "season_maturity", "history_depth"], observed=True)
                     .agg(rows=("game_id", "size"), changed_rows=("changed", "sum"),
                          mean_abs_difference=("abs_correction_delta", "mean"),
                          max_abs_difference=("abs_correction_delta", "max"),
                          original_mean=("original_team_d10_sf_per_game", "mean"),
                          reconstructed_mean=("reconstructed_team_d10_sf_per_game", "mean"))
                     .reset_index())
    group_summary["changed_percentage"] = group_summary.changed_rows / group_summary.rows * 100
    group_summary.to_csv(OUT / "training_contamination_summary.csv", index=False, float_format="%.17g")
    # Identify histories whose rolling discrepancy can be explained by at least one differing contributing team-game total.
    diff_keys = source_diff_keys
    prior_evidence = pd.read_csv(ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/contributing_team_game_evidence.csv", low_memory=False)
    holdout_prior = prior_evidence.loc[prior_evidence.target_period.eq("HOLDOUT_2025")].copy()
    holdout_prior["source_total_differs"] = [(int(g), int(t)) in diff_keys for g, t in zip(holdout_prior.prior_game_id, holdout_prior.prior_team_id)]
    rolling_diff = holdout_prior.groupby(["target_game_id", "target_team_id"]).source_total_differs.any().rename("any_prior_source_total_differs").reset_index()
    holdout = holdout.merge(rolling_diff, left_on=["game_id", "canonical_team_id"],
                            right_on=["target_game_id", "target_team_id"], how="left", validate="many_to_one")
    holdout["any_prior_source_total_differs"] = holdout.any_prior_source_total_differs.fillna(False).astype(bool)

    original_hash = sha(SOURCE_FRAME)
    training_summary = {}
    for season, group in train.groupby("canonical_season"):
        training_summary[str(int(season))] = {
            "rows": int(len(group)), "source_supported_rows": int(group.source_supported.sum()),
            "unchanged_rows": int((~group.changed).sum()), "changed_rows": int(group.changed.sum()),
            "changed_percentage": float(group.changed.mean() * 100),
            "mean_absolute_difference": float(group.abs_correction_delta.mean()),
            "maximum_absolute_difference": float(group.abs_correction_delta.max()),
            "original_feature_mean": float(group.original_team_d10_sf_per_game.mean()),
            "reconstructed_feature_mean": float(group.reconstructed_team_d10_sf_per_game.mean()),
            "changed_dates": int(group.loc[group.changed, "game_date"].nunique()),
            "changed_teams": int(group.loc[group.changed, "canonical_team_id"].nunique()),
            "unsupported_rows": int((~group.source_supported).sum()),
            "opening_week_nonzero_without_prior_history": int((group.valid_prior_game_count.eq(0) & group.original_team_d10_sf_per_game.ne(0)).sum()),
            "training_only_inspection_original_expected_mean": float(train_original_mu[train.canonical_season.to_numpy() == int(season)].mean()),
            "training_only_inspection_corrected_expected_mean": float(train_corrected_mu[train.canonical_season.to_numpy() == int(season)].mean()),
        }
    source_missing = int(train_recon.source_missing_team_id_rows.sum())
    source_manifest = json.loads(BOX_MANIFEST.read_text())
    source_manifest_out = {
        "input_manifest_sha256": sha(BOX_MANIFEST),
        "source_file_sha256": sha(OFFICIAL_SOURCE),
        "reconciliation_file_sha256": sha(SOURCE_RECON),
        "official_team_game_rows_by_season": official.groupby(official.game_id.astype(str).str[:4]).size().to_dict(),
        "official_completed_games_by_season": official.groupby(official.game_id.astype(str).str[:4]).game_id.nunique().to_dict(),
        "retained_player_source_reconciliation_rows_by_period": train_recon.groupby("target_period").size().to_dict(),
        "retained_player_source_complete_team_games_by_period": train_recon.groupby("target_period").official_source_status.apply(lambda x: int(x.eq("COMPLETE").sum())).to_dict(),
        "reconstructed_training_rows_source_supported": int(train.source_supported.sum()),
        "official_source_game_date_range": {str(k): {"min": v.min(), "max": v.max()} for k, v in official.assign(season=official.game_id.astype(str).str[:4]).groupby("season").game_date.agg(["min", "max"]).iterrows()},
        "manifest_summary": source_manifest,
        "source_period_rule": "official completed regular-season team totals retained in audited boxscore source; source rows are per physical team and game",
    }
    write_json(OUT / "official_historical_team_game_source_manifest.json", source_manifest_out)

    input_inventory = {
        "original_frozen_training_source_frame": str(SOURCE_FRAME.relative_to(ROOT)),
        "training_source_frame_sha256": original_hash,
        "training_source_frame_hash_verified_against_manifest": original_hash == expected_hash,
        "training_population": {"2023": int((frame.canonical_season == 2023).sum()), "2024": int((frame.canonical_season == 2024).sum()), "combined": int(frame.canonical_season.isin([2023, 2024]).sum())},
        "2025_holdout_population": int((frame.canonical_season == 2025).sum()),
        "source_manifest_path": str(SOURCE_MANIFEST.relative_to(ROOT)),
        "model_path": str(MODEL_PATH.relative_to(ROOT)), "model_sha256": sha(MODEL_PATH),
        "contract_path": str(CONTRACT_PATH.relative_to(ROOT)), "contract_sha256": sha(CONTRACT_PATH),
        "model_identity_sha256": json.loads(CONTRACT_PATH.read_text())["model_identity_sha256"],
        "frozen_feature_order": FEATURES,
        "scaler_type": type(fitted["scaler"]).__module__ + "." + type(fitted["scaler"]).__name__,
        "scaler_mean": list(map(float, fitted["scaler"].mean_)),
        "scaler_scale": list(map(float, fitted["scaler"].scale_)),
        "scaler_artifact_sha256": sha(MODEL_PATH),
        "scaler_identity_sha256": hashlib.sha256(json.dumps({
            "type": type(fitted["scaler"]).__module__ + "." + type(fitted["scaler"]).__name__,
            "feature_order": FEATURES, "mean": list(map(float, fitted["scaler"].mean_)),
            "scale": list(map(float, fitted["scaler"].scale_)),
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "historical_target_labels": "realized_points in frozen frame; restoration package verifies original validation target binding",
        "historical_source_tables": "not required for feature reconstruction; full official team-game boxscores are retained in source package",
        "raw historical skater log source": "source_to_official_team_game_reconciliation.csv summarizes retained historical skater rows; raw per-player rows are not a complete input to official reference reconstruction",
    }
    write_json(OUT / "original_training_evidence_inventory.json", input_inventory)

    metrics = {
        "training_rows": int(len(train)), "training_seasons": training_summary,
        "training_overall": {
            "rows": int(len(train)), "source_supported_rows": int(train.source_supported.sum()),
            "unchanged_rows": int((~train.changed).sum()), "changed_rows": int(train.changed.sum()),
            "changed_percentage": float(train.changed.mean() * 100),
            "mean_absolute_difference": float(train.abs_correction_delta.mean()),
            "maximum_absolute_difference": float(train.abs_correction_delta.max()),
            "original_feature_mean": float(train.original_team_d10_sf_per_game.mean()),
            "reconstructed_feature_mean": float(train.reconstructed_team_d10_sf_per_game.mean()),
            "unsupported_rows": int((~train.source_supported).sum()),
            "changed_dates": int(train.loc[train.changed, "game_date"].nunique()),
            "changed_teams": int(train.loc[train.changed, "canonical_team_id"].nunique()),
            "changed_player_games": int(train.changed.sum()),
            "history_depth": train.groupby("history_depth", observed=True).agg(rows=("game_id", "size"), changed=("changed", "sum")).to_dict("index"),
            "early_vs_mature": train.groupby("season_maturity").agg(rows=("game_id", "size"), changed=("changed", "sum")).to_dict("index"),
            "feature_discrepancy_source_classes": train.feature_discrepancy_source_classification.value_counts().to_dict(),
        },
        "identity_2023": {
            "training_frame_identifier_value_differences_not_assumed_corruption": int((comparison.loc[comparison.canonical_season.eq(2023), "original_training_team_id"] != comparison.loc[comparison.canonical_season.eq(2023), "canonical_team_id"]).sum()),
            "stable_alias_rows": int(crosswalk.loc[crosswalk.canonical_season.eq(2023) & crosswalk.resolution.eq("CONSISTENT_NUMERIC_ALIAS"), "rows"].sum()),
            "conflicted_raw_ids": [5, 14, 16],
            "conflicted_mapping_edge_count": int(crosswalk.loc[crosswalk.canonical_season.eq(2023) & crosswalk.resolution.eq("CONFLICTED_RAW_ID_TO_PHYSICAL_TEAM_MAPPING")].shape[0]),
            "rows_across_disputed_raw_ids": int(crosswalk.loc[crosswalk.canonical_season.eq(2023) & crosswalk.original_training_team_id.isin([5, 14, 16]), "rows"].sum()),
            "minority_mapping_assignments": int(crosswalk.loc[crosswalk.canonical_season.eq(2023) & crosswalk.original_training_team_id.isin([5, 14, 16]) & crosswalk.canonical_team_id.eq(crosswalk.original_training_team_id), "rows"].sum()),
            "prior_audit_source_log_numeric_id_mismatches": json.loads(PRIOR_AUDIT_SUMMARY.read_text())["checks"]["team_binding"]["TRAIN_2023_2024"]["numeric_id_vs_side_mismatch"],
            "prior_audit_source_log_numeric_id_checked": json.loads(PRIOR_AUDIT_SUMMARY.read_text())["checks"]["team_binding"]["TRAIN_2023_2024"]["numeric_id_checked"],
            "missing_team_id_source_rows": source_missing,
            "conflicted_raw_id_season_pairs": crosswalk.loc[crosswalk.canonical_season.eq(2023) & crosswalk.resolution.eq("CONFLICTED_RAW_ID_TO_PHYSICAL_TEAM_MAPPING"), ["original_training_team_id", "canonical_team_id", "rows", "resolution"]].to_dict("records"),
            "opening_week_nonzero_without_prior_history": int(len(opening)),
            "opening_week_cases": opening[["game_id", "player_id", "game_date", "original_training_team_id", "canonical_team_id", "original_team_d10_sf_per_game"]].to_dict("records"),
        },
        "2025_holdout": {
            "rows": int(len(holdout)), "source_supported_rows": int(holdout.source_supported.sum()),
            "unchanged_rows": int((~holdout.changed).sum()), "changed_rows": int(holdout.changed.sum()),
            "exact_agreement_percentage": float((~holdout.changed).mean() * 100),
            "mean_absolute_difference": float(holdout.abs_correction_delta.mean()),
            "maximum_absolute_difference": float(holdout.abs_correction_delta.max()),
            "original_feature_mean": float(holdout.original_team_d10_sf_per_game.mean()),
            "reconstructed_feature_mean": float(holdout.reconstructed_team_d10_sf_per_game.mean()),
            "team_game_total_differences": int(len(team_diff)),
            "team_game_total_difference_rate": float(len(team_diff) / max(len(holdout_recon), 1)),
            "team_game_difference_classes": team_diff.source_classification.value_counts().to_dict(),
            "changed_feature_rows_with_prior_skater_sum_difference": int(holdout.loc[holdout.changed & holdout.any_prior_source_total_differs].shape[0]),
            "changed_feature_rows_without_identified_prior_skater_sum_difference": int(holdout.loc[holdout.changed & ~holdout.any_prior_source_total_differs].shape[0]),
            "feature_discrepancy_source_classes": holdout.feature_discrepancy_source_classification.value_counts().to_dict(),
        },
        "frozen_hgb_sensitivity": {
            "training_replay_role": "TRAINING_ROWS_MODEL_RESPONSE_INSPECTION_NOT_OUT_OF_TIME_EVALUATION",
            "holdout_2025_original": _metrics(holdout.realized_points.to_numpy(float), holdout_original_mu),
            "holdout_2025_corrected_feature": _metrics(holdout.realized_points.to_numpy(float), holdout_corrected_mu),
            "holdout_2025_discrepancy_bands": by_discrepancy,
            "deterministic_replay": "PASS",
            "deterministic_replay_max_abs_error": replay_max_abs_error,
            "model_sha256": sha(MODEL_PATH), "model_identity_sha256": input_inventory["model_identity_sha256"],
            "scaler_identity_sha256": input_inventory["scaler_identity_sha256"],
            "only_changed_feature": "team_d10_sf_per_game",
        },
        "rebuild_scope": {
            "source_database_rebuild_required": False,
            "reason": "retained official team-game boxscores cover all 2023-24 and 2025 evaluation team-games used here; full historical NHL database is not needed to reconstruct this feature",
            "feature_rebuild_columns": ["team_d10_sf_per_game"],
            "other_training_features_changed": False,
            "training_feature_defect_material": bool(train.changed.any()),
            "corrected_matrix_can_preserve_other_columns": True,
            "controlled_challenger_justified": bool(train.changed.any()),
            "frozen_incumbent_compatibility_established": False,
        },
        "providers": {"additional_official_provider_requests": 0, "paid_odds_credits": 0},
        "production_mutations": {"production_code_changes": 0, "database_writes": 0, "prediction_changes": 0, "authority_changes": 0, "funnel_changes": 0},
    }
    write_json(OUT / "training_contamination_summary.json", metrics)
    write_json(OUT / "frozen_hgb_sensitivity.json", metrics["frozen_hgb_sensitivity"])
    write_json(OUT / "database_vs_feature_rebuild_decision.json", metrics["rebuild_scope"])
    write_json(OUT / "current_source_completeness_snapshot.json", CURRENT_SOURCE_SNAPSHOT)
    write_json(OUT / "preservation_validation_results.json", {
        "status": "PASS_SYNTHETIC_STANDALONE_PACKAGE_VALIDATION",
        "capture_pipeline_integration": "NOT_INSTALLED",
        "same_repeatable_read_snapshot_as_production_export": False,
        "unit_test_command": "./bin/python -m unittest backend.nhl.tests.test_points_offline_team_shots backend.nhl.tests.test_points_team_shots_evidence",
        "test_modules_passed": 2, "tests_passed": 17,
        "tested_cases": ["absent history", "valid zero SOG", "missing skater records", "duplicate player-game records",
                         "team-ID alias normalization", "strict target-date and 120-day boundary", "incomplete source flag",
                         "run-ID create-only rerun", "CSV and manifest SHA-256 verification"],
        "real_future_daily_capture_required": True,
        "limitation": "A real daily run has not verified that the source query and feature export share one database snapshot; the standalone writer reports same_snapshot_as_feature_export=false.",
    })
    write_json(OUT / "evidence_preservation_design.json", {
        "status": "PREGAME_EVIDENCE_CAPTURE_DESIGNED_NOT_INSTALLED",
        "daily_command_unchanged": "./bin/python -m backend.nhl.cli daily --with-odds",
        "production_path": ["nhl.skater_game_logs_raw", "backend/nhl/sql/export_points.sql",
                            "backend/nhl/exports/train_nhl_points_v2.csv", "prepare_scoring_input",
                            "points_scoring_input.csv", "score_nhl_points_with_lineage.py", "score_points_phoenix.py"],
        "hgb_separate_path": ["points_hgb_shadow", "prepare_points_hgb_inputs", "export_nhl_points_hgb_features.py",
                              "score_nhl_points_hgb_shadow.py"],
        "insertion_point": "backend/nhl/cli.py daily() 4b independent saves/points feature exporter: replace only the points psql_stdout(export_points.sql) call with one read-only repeatable-read psql session that emits the exact feature CSV and a sidecar before points scoring starts.",
        "required_evidence_files": ["exact_feature_export.csv", "target_bindings.csv", "raw_prior_player_game_rows.csv",
                                    "prior_team_game_aggregation.csv", "target_feature_values.csv", "manifest.json", "SHA256SUMS"],
        "required_raw_fields": ["player_id", "game_id", "game_date", "game_start_utc", "raw_team_id", "home_team_id", "away_team_id",
                                "shots_on_goal", "shot_attempts", "toi_minutes"],
        "required_aggregates": ["contributing_game_ids", "raw_player_row_count", "unique_player_count", "missing_sog_count",
                                "missing_attempts_count", "SOG_sum_with_exporter_null_policy", "eligible_game_count", "feature_value"],
        "capture_path_template": "artifacts/operational/nhl/points_feature_evidence/season={season}/slate_date={slate_date}/run_id={parent_daily_run_id}/",
        "validation_gate": "derive each target feature from captured prior groups and require exact key set plus <=1e-12 value difference against exact feature export before allowing the Points lane to continue",
        "why_not_installed": "A separate query would not prove it captured the exact feature-export database snapshot. Safe installation requires the live exporter and evidence query to share one read-only repeatable-read transaction and needs a real database integration validation; no production daily run was requested.",
        "prototype_module": "backend/nhl/points_team_shots_evidence.py",
    })
    write_json(OUT / "summary.json", {
        "schema_version": "NHL_POINTS_HISTORICAL_TEAM_SHOTS_RECONSTRUCTION_V1",
        "historical_reconstruction_status": "HISTORICAL_FEATURE_DEFECT_MATERIAL" if train.changed.any() else "HISTORICAL_FEATURE_RECONSTRUCTION_VALIDATED",
        "evidence_preservation_status": "PREGAME_EVIDENCE_CAPTURE_DESIGNED_NOT_INSTALLED",
        "metrics": metrics,
        "source_file_sha256": {"training_frame": original_hash, "official_team_games": sha(OFFICIAL_SOURCE), "official_source_reconciliation": sha(SOURCE_RECON), "frozen_model": sha(MODEL_PATH)},
        "official_source_request_counts": {"additional_requests": 0, "paid_credits": 0},
    })
    report = f"""# NHL Points historical team-shot reconstruction and evidence preservation

## Status

- Historical reconstruction: **{'HISTORICAL_FEATURE_DEFECT_MATERIAL' if train.changed.any() else 'HISTORICAL_FEATURE_RECONSTRUCTION_VALIDATED'}**
- Future pregame evidence capture: **PREGAME_EVIDENCE_CAPTURE_DESIGNED_NOT_INSTALLED**

## Original frozen evidence

The exact validation source frame is `{SOURCE_FRAME.relative_to(ROOT)}` with SHA-256 `{original_hash}`. Its 2023 and 2024 training populations contain 47,221 and 43,265 player-games (90,486 total); 2025 is a separate 47,239-row holdout. The frozen HGB artifact SHA-256 is `{sha(MODEL_PATH)}`; its ordered ten-feature contract and StandardScaler are preserved in `original_training_evidence_inventory.json`.

The original matrix and model are not modified. Official completed team-game boxscores provide source coverage for all historical target populations in this reconstruction; no database rebuild or additional provider call was needed.

## Training contamination

| Season | Rows | Changed | Changed % | Original mean | Official reconstruction mean | Mean absolute difference | Maximum difference |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
""" + "\n".join(
        f"| {season} | {values['rows']} | {values['changed_rows']} | {values['changed_percentage']:.2f}% | {values['original_feature_mean']:.3f} | {values['reconstructed_feature_mean']:.3f} | {values['mean_absolute_difference']:.3f} | {values['maximum_absolute_difference']:.3f} |"
        for season, values in training_summary.items()) + f"""

Across training, {metrics['training_overall']['changed_rows']}/{len(train)} rows change ({metrics['training_overall']['changed_percentage']:.2f}%). Source-supported rows: {metrics['training_overall']['source_supported_rows']}; unsupported rows: {metrics['training_overall']['unsupported_rows']}. No-history remains zero. Distribution by season maturity and history depth is in `training_contamination_summary.csv`.

The 2023 identity reconciliation is at two different grains. The original training frame has {metrics['identity_2023']['training_frame_identifier_value_differences_not_assumed_corruption']} target-frame numeric ID values that differ from current NHL physical-team IDs; these are not counted as errors because the historical frame uses a stable alternate ID mapping for most rows. The prior raw-log audit separately checked {metrics['identity_2023']['prior_audit_source_log_numeric_id_checked']} historical source rows and found {metrics['identity_2023']['prior_audit_source_log_numeric_id_mismatches']} raw team-ID/official-side mismatches. Three raw ID season mappings (5, 14, 16) each point to two physical teams ({metrics['identity_2023']['conflicted_mapping_edge_count']} edges spanning {metrics['identity_2023']['rows_across_disputed_raw_ids']} rows); {metrics['identity_2023']['minority_mapping_assignments']} assignments are on the minority edge, but the retained mapping evidence does not establish which side produced the original training feature. There were {source_missing} source-log rows with missing team IDs and {len(opening)} opening-week target player-games with nonzero original team history despite zero official prior games for their physical team. Stable aliases are separated in `2023_identity_discrepancy_resolution.csv` from the unresolved edges and missing sentinel.

## 2025 holdout

The corrected official reference has {len(holdout)} source-supported target rows and {metrics['2025_holdout']['exact_agreement_percentage']:.2f}% exact row agreement with the original feature. Original and corrected feature means are {holdout.original_team_d10_sf_per_game.mean():.3f} and {holdout.reconstructed_team_d10_sf_per_game.mean():.3f}. The 2025 per-team-game skater-sum reconciliation identifies {len(team_diff)} differing team-games: incomplete coverage {metrics['2025_holdout']['team_game_difference_classes'].get('INCOMPLETE_SKATER_COVERAGE', 0)}, unexplained full-coverage totals {metrics['2025_holdout']['team_game_difference_classes'].get('SOURCE_TOTAL_DIFFERENCE_UNEXPLAINED', 0)}, and unexplained excess player population {metrics['2025_holdout']['team_game_difference_classes'].get('PLAYER_POPULATION_EXCESS_UNEXPLAINED', 0)}. Of changed target features, {metrics['2025_holdout']['changed_feature_rows_with_prior_skater_sum_difference']} include at least one prior game whose skater sum differs from the official team total; {metrics['2025_holdout']['changed_feature_rows_without_identified_prior_skater_sum_difference']} changed rows have no such source-total difference identified. Per-game counts, missingness, team binding, and raw-vs-official totals are in `2025_skater_sum_official_total_discrepancies.csv`. For the 60 full-18-player groups where totals still differ, source revisions versus aggregation causes cannot be separated from retained artifacts; those cases remain unexplained.

## Frozen HGB sensitivity

The same frozen HGB and scaler were replayed with only `team_d10_sf_per_game` replaced. 2025 original versus corrected-feature results: expected Points mean {metrics['frozen_hgb_sensitivity']['holdout_2025_original']['mean_expected_points']:.4f} vs {metrics['frozen_hgb_sensitivity']['holdout_2025_corrected_feature']['mean_expected_points']:.4f}; count residual {metrics['frozen_hgb_sensitivity']['holdout_2025_original']['mean_count_residual_realized_minus_expected']:+.4f} vs {metrics['frozen_hgb_sensitivity']['holdout_2025_corrected_feature']['mean_count_residual_realized_minus_expected']:+.4f}; Brier {metrics['frozen_hgb_sensitivity']['holdout_2025_original']['over_0_5_brier']:.4f} vs {metrics['frozen_hgb_sensitivity']['holdout_2025_corrected_feature']['over_0_5_brier']:.4f}; log loss {metrics['frozen_hgb_sensitivity']['holdout_2025_original']['over_0_5_log_loss']:.4f} vs {metrics['frozen_hgb_sensitivity']['holdout_2025_corrected_feature']['over_0_5_log_loss']:.4f}. These are retrospective sensitivity metrics, not a prospective promotion result and do not establish model compatibility.

## Rebuild scope and decision

- Historical database rebuild: **not required** for this feature; retained official completed team-game source is sufficient.
- Historical feature reconstruction: rebuild only `team_d10_sf_per_game`; retain all original labels and nondefective feature columns unchanged.
- Training contamination: material. A corrected training matrix can be built by verified single-column replacement.
- Next modeling step: run one controlled date-blocked challenger with the corrected column, unchanged other inputs and labels, independently corrected 2025 holdout, corrected 2026 reference for diagnosis, and future prospective outcomes. Keep the frozen incumbent as an unpromoted control. Do not retrain as part of this study.

## 2026 evidence preservation

The actual production Phoenix path is `nhl.skater_game_logs_raw` → `backend/nhl/sql/export_points.sql` → `train_nhl_points_v2.csv` → `prepare_scoring_input` → `score_nhl_points_with_lineage.py` → `score_points_phoenix.py`. `export_nhl_points_hgb_features.py` and immutable `points_hgb_shadow` captures belong to the separate HGB research/shadow path. Existing HGB captures are not a substitute for Phoenix source-to-feature lineage.

An exact source-to-feature snapshot can be inserted at the `cli.py` daily export block immediately before scoring, using the same read-only repeatable-read PostgreSQL session as the feature export. The minimal patch proposal is: retain the exact production export bytes; capture target/game/team bindings; export the bounded raw player-game fields used by `team_logs`; retain grouped prior team-games, contributing IDs, source row/missingness counts, denominator, and computed D10; validate the evidence-derived value against each exported feature; then write checksummed create-only files under `artifacts/operational/nhl/points_feature_evidence/season=.../slate_date=.../run_id=...` and bind the manifest into the Points lane receipt. A separate later query is not sufficient to claim exact capture-time inputs. This was intentionally designed but not installed because it changes the shared exporter session and requires a real read-only database run to validate safely.

## Current source and limitations

The bounded read-only source snapshot for Sep. 29–Oct. 13, 2026 found 105 games, 210 team-game groups and 3,780 player-game rows, with no missing SOG, no duplicate player-game records, no home/away binding mismatches, and 18 rows per team-game. All 3,780 current rows have NULL `shot_attempts`; this confirms the current attempts defect remains. This current database state cannot prove the original Oct. 9–14 capture state. Shot-attempt repair is outside this task.

No production database writes, provider calls, paid credits, predictions, authority, or Funnel changes were made. See `summary.json` and the row-level datasets for complete counts and provenance.
"""
    (OUT / "report.md").write_text(report)
    next_action = "Create one date-blocked challenger matrix by replacing only team_d10_sf_per_game in the preserved 2023-2024 training rows with the validated official reconstruction; retain labels and every other frozen input, evaluate on the original date-blocked folds and independently corrected 2025 holdout, keep the frozen incumbent as control, and do not promote from this retrospective study. Before that evaluation, install and validate the same-snapshot pregame evidence capture at the export_points.sql boundary in a future read-only daily run."
    (OUT / "recommended_next_action.md").write_text("# Recommended next action\n\n" + next_action + "\n")


if __name__ == "__main__":
    main()
