#!/usr/bin/env python3
"""Build a deterministic, offline NHL Points PP-TOI research dataset.

This consumes only the retained Oct. 16 replay, verified recovery candidates,
and frozen HGB validation input. It never connects to PostgreSQL, fits/scores a
model, or changes canonical data.
"""
from __future__ import annotations

import argparse
import bisect
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
ARTIFACT_ROOT = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16"
DEFAULT_OUT = ROOT / "artifacts/analysis/nhl/pp_toi_verified_feature_preparation_2026-10-16"
TARGETS = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
REPLAY = ARTIFACT_ROOT / "hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL.csv"
REPLAY_SUMMARY = ARTIFACT_ROOT / "hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL_summary.json"
CANDIDATES = ARTIFACT_ROOT / "pp_toi_recovery_candidates_plus_32_official_overlay_COUNTERFACTUAL.csv"
RAW_SNAPSHOT = ROOT / "artifacts/analysis/nhl/raw_foundation_integrity_2026-10-16/current_raw_snapshot.csv"
FEATURE_EXPORTER = ROOT / "backend/nhl/scripts/export_nhl_points_hgb_features.py"
FROZEN_CONTRACT = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/shadow_contract.json"

FEATURES = [
    "is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
    "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
    "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share",
]
IDENTITY = ["game_date", "game_id", "player_id"]
VERIFIED = {"VERIFIED_CORRECT", "VERIFIED_INCORRECT"}
EXPECTED_SHA256 = {
    "frozen_training_input": "45605af7f472481af4d5a33a103b679ab4284db5d043f82842757297a3534892",
    "counterfactual_replay": "e6e7a71e7b71981f290586c0ffb2f2a49e0187c8118d228fecb9a1671294dd25",
    "counterfactual_replay_summary": "610efb2ba6323747421f11c6a046a9633910c5325cf0980c14c915dce365a85f",
    "verified_candidate_overlay": "6d37ed9793931e4b4615ef0b60bdb3b561ca9dfd1012c8341d97b2845259f2ce",
    "read_only_raw_snapshot": "fadf53ffaaefe518affe06b58f6ba7dd709dd070a3d025d1f3dc0fa2115fcc68",
}
EXPECTED_ROWS = {"frozen": 90_486, "qualified": 21_599, "baseline_exceptions": 326}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def qualify_replay_rows(replay: pd.DataFrame) -> pd.Series:
    """Use the retained replay's exact all-contributors-verified rule."""
    prior = pd.to_numeric(replay["prior_player_games_in_120d"], errors="coerce")
    verified = pd.to_numeric(replay["verified_reconstruction_rows_in_window"], errors="coerce")
    return replay["status"].eq("REPLAYED") & prior.gt(0) & verified.eq(prior)


def outcome_class(points: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(points, errors="raise")
    return numeric.map(lambda n: "3+" if n >= 3 else str(int(n)))


def make_strata(population: pd.DataFrame, cohort: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    populations = {"FROZEN_TRAINING_POPULATION": population, "FULLY_VERIFIED_COHORT": cohort}
    for population_name, frame in populations.items():
        work = frame.copy()
        work["outcome_class"] = outcome_class(work["realized_points"])
        work["season_month"] = work["game_date"].astype(str).str[:7]
        window_games = pd.to_numeric(work["prior_player_games_in_120d"], errors="coerce")
        work["prior_window_games_band"] = pd.cut(
            window_games, bins=[0, 2, 5, 9, 10], labels=["1-2", "3-5", "6-9", "10"],
            include_lowest=True, right=True,
        ).astype("string")
        lifetime = pd.to_numeric(work["player_lifetime_games_prior"], errors="coerce")
        work["lifetime_games_band"] = pd.cut(
            lifetime, bins=[-1, 9, 24, 49, 99, np.inf],
            labels=["0-9", "10-24", "25-49", "50-99", "100+"], right=True,
        ).astype("string")
        dimensions = [
            ("season", work["canonical_season"].astype(str)),
            ("season_month", work["season_month"]),
            ("prior_window_games", work["prior_window_games_band"]),
            ("lifetime_games_prior", work["lifetime_games_band"]),
            ("outcome_class", work["outcome_class"]),
        ]
        for dimension, values in dimensions:
            grouped = work.assign(_stratum=values).groupby("_stratum", dropna=False, sort=True)
            for key, subset in grouped:
                counts = subset["outcome_class"].value_counts()
                rows.append({
                    "population": population_name,
                    "dimension": dimension,
                    "stratum": "UNKNOWN" if pd.isna(key) else str(key),
                    "row_count": int(len(subset)),
                    "population_share": float(len(subset) / len(work)) if len(work) else None,
                    "baseline_exact_match_rows": int(subset["current_raw_replay_matches_frozen_feature"].eq(True).sum()),
                    "baseline_exception_rows": int(subset["current_raw_replay_matches_frozen_feature"].eq(False).sum()),
                    "outcome_0_rows": int(counts.get("0", 0)),
                    "outcome_1_rows": int(counts.get("1", 0)),
                    "outcome_2_rows": int(counts.get("2", 0)),
                    "outcome_3plus_rows": int(counts.get("3+", 0)),
                })
    return pd.DataFrame(rows)


def build_lineage(cohort: pd.DataFrame, candidates: pd.DataFrame, raw: pd.DataFrame,
                  history_identity_keys: set[tuple[int, int]]) -> pd.DataFrame:
    verified = candidates.loc[candidates["classification"].isin(VERIFIED)].copy()
    if verified.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_VERIFIED_CANDIDATE_IDENTITY")
    hash_columns = ["source_pbp_sha256", "source_boxscore_sha256", "source_shiftcharts_sha256"]
    if verified[hash_columns].isna().any().any():
        raise ValueError("VERIFIED_CANDIDATE_SOURCE_HASH_MISSING")
    verified["game_date"] = pd.to_datetime(verified["game_date"], errors="raise").dt.normalize()
    candidate_by_key = {(int(r.game_id), int(r.player_id)): r
                        for r in verified.itertuples(index=False)}
    raw = raw.copy()
    raw["game_id"] = pd.to_numeric(raw["game_id"], errors="raise").astype("int64")
    raw["player_id"] = pd.to_numeric(raw["player_id"], errors="raise").astype("int64")
    raw["game_date"] = pd.to_datetime(raw["game_date"], errors="raise").dt.normalize()
    raw = raw.loc[[key in history_identity_keys for key in zip(raw.game_id, raw.player_id)]].copy()
    raw["pp_toi_minutes"] = pd.to_numeric(raw["pp_toi_minutes"], errors="coerce")
    if raw.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_RAW_PLAYER_GAME_IDENTITY")
    by_player: dict[int, tuple[list[pd.Timestamp], list[object]]] = {}
    for pid, group in raw.groupby("player_id", sort=False):
        group = group.sort_values(["game_date", "game_id"], kind="mergesort")
        by_player[int(pid)] = (group["game_date"].tolist(), list(group.itertuples(index=False)))

    lines: list[dict[str, object]] = []
    for target in cohort.itertuples(index=False):
        target_date = pd.Timestamp(target.game_date)
        dates, records = by_player.get(int(target.player_id), ([], []))
        lo = bisect.bisect_left(dates, target_date - pd.Timedelta(days=120))
        hi = bisect.bisect_left(dates, target_date)  # exclusive target date
        history = records[max(lo, hi - 10):hi]
        if len(history) != int(target.prior_player_games_in_120d):
            raise ValueError("QUALIFIED_HISTORY_COUNT_DOES_NOT_MATCH_REPLAY")
        if len(history) != int(target.verified_reconstruction_rows_in_window):
            raise ValueError("QUALIFIED_VERIFIED_COUNT_DOES_NOT_MATCH_REPLAY")
        for order, source in enumerate(history, start=1):
            candidate = candidate_by_key.get((int(source.game_id), int(source.player_id)))
            if candidate is None or candidate.classification not in VERIFIED:
                raise ValueError("QUALIFIED_RAW_CONTRIBUTOR_LACKS_VERIFIED_CANDIDATE")
            if pd.isna(candidate.reconstructed_pp_toi_minutes) or pd.isna(source.pp_toi_minutes):
                raise ValueError("QUALIFIED_CONTRIBUTOR_PP_TOI_MISSING")
            if not np.isclose(float(candidate.current_pp_toi_minutes), float(source.pp_toi_minutes), rtol=0, atol=1e-10):
                raise ValueError("CANDIDATE_CURRENT_VALUE_DIFFERS_FROM_RAW_SNAPSHOT")
            days_before = int((target_date - source.game_date).days)
            if not (0 < days_before <= 120):
                raise ValueError("STRICT_PRIOR_120_DAY_WINDOW_VIOLATION")
            if candidate.validation_status != "RECONCILED_FINAL_SOURCE_AND_DUAL_METHODS":
                raise ValueError("VERIFIED_SOURCE_RECONCILIATION_STATUS_INVALID")
            lines.append({
                "target_game_date": target.game_date,
                "target_game_id": int(target.game_id),
                "target_player_id": int(target.player_id),
                "contributor_order_oldest_to_newest": order,
                "contributor_rank_newest": len(history) - order + 1,
                "contributor_game_date": source.game_date.date().isoformat(),
                "contributor_game_id": int(source.game_id),
                "contributor_player_id": int(source.player_id),
                "days_before_target": days_before,
                "contributor_classification": candidate.classification,
                "validation_status": candidate.validation_status,
                "current_pp_toi_minutes": float(source.pp_toi_minutes),
                "verified_pp_toi_minutes": float(candidate.reconstructed_pp_toi_minutes),
                "source_pbp_sha256": candidate.source_pbp_sha256,
                "source_boxscore_sha256": candidate.source_boxscore_sha256,
                "source_shiftcharts_sha256": candidate.source_shiftcharts_sha256,
            })
    return pd.DataFrame(lines)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def run(out_dir: Path) -> dict[str, object]:
    source_paths = {
        "frozen_training_input": TARGETS,
        "counterfactual_replay": REPLAY,
        "counterfactual_replay_summary": REPLAY_SUMMARY,
        "verified_candidate_overlay": CANDIDATES,
        "read_only_raw_snapshot": RAW_SNAPSHOT,
        "frozen_feature_exporter": FEATURE_EXPORTER,
        "frozen_hgb_contract": FROZEN_CONTRACT,
        "dataset_builder": Path(__file__).resolve(),
    }
    for name, path in source_paths.items():
        require(path.is_file(), f"SOURCE_ARTIFACT_MISSING:{name}")
    input_hashes = {name: sha256(path) for name, path in source_paths.items()}
    for name, expected in EXPECTED_SHA256.items():
        require(input_hashes[name] == expected, f"SOURCE_HASH_MISMATCH:{name}")

    frozen_all = pd.read_csv(TARGETS)
    frozen = frozen_all.loc[pd.to_numeric(frozen_all["canonical_season"], errors="raise").isin([2023, 2024])].copy()
    replay = pd.read_csv(REPLAY)
    candidates = pd.read_csv(CANDIDATES)
    raw = pd.read_csv(RAW_SNAPSHOT)
    key = ["game_date", "game_id", "player_id"]
    for frame, label in [(frozen, "FROZEN"), (replay, "REPLAY")]:
        frame["game_date"] = pd.to_datetime(frame["game_date"], errors="raise").dt.date.astype(str)
        for col in ("game_id", "player_id"):
            frame[col] = pd.to_numeric(frame[col], errors="raise").astype("int64")
        require(not frame.duplicated(key).any(), f"DUPLICATE_{label}_IDENTITY")
    require(len(frozen) == EXPECTED_ROWS["frozen"], "FROZEN_POPULATION_ROW_COUNT_MISMATCH")
    joined = frozen.merge(replay, on=key, how="left", validate="one_to_one", suffixes=("", "_replay"), indicator=True)
    require(len(joined) == len(frozen) and joined["_merge"].eq("both").all(), "FROZEN_REPLAY_IDENTITY_COVERAGE_MISMATCH")
    joined = joined.drop(columns="_merge")
    qualified_mask = qualify_replay_rows(joined)
    cohort = joined.loc[qualified_mask].copy()
    require(len(cohort) == EXPECTED_ROWS["qualified"], "QUALIFIED_ROW_COUNT_MISMATCH")
    require(cohort["mean_pp_toi_last10_after_verified_source_values"].notna().all(), "CORRECTED_PP_TOI_FEATURE_INCOMPLETE")

    exceptions = joined.loc[joined["current_raw_replay_matches_frozen_feature"].eq(False)].copy()
    require(len(exceptions) == EXPECTED_ROWS["baseline_exceptions"], "BASELINE_EXCEPTION_COUNT_MISMATCH")
    exception_qualified = qualify_replay_rows(exceptions)
    require(int(exception_qualified.sum()) == 80 and int((~exception_qualified).sum()) == 246,
            "BASELINE_EXCEPTION_QUALIFICATION_PARTITION_MISMATCH")
    exact_cohort = cohort.loc[cohort["current_raw_replay_matches_frozen_feature"].eq(True)].copy()
    require(len(exact_cohort) == 21_519, "EXACT_BASELINE_REPRODUCTION_COUNT_MISMATCH")

    # Reconstruct source-backed lineage from already retained verified rows;
    # no source acquisition or database read occurs here.
    candidates["game_id"] = pd.to_numeric(candidates["game_id"], errors="raise").astype("int64")
    candidates["player_id"] = pd.to_numeric(candidates["player_id"], errors="raise").astype("int64")
    all_history_keys = set(zip(
        pd.to_numeric(frozen_all.game_id, errors="raise").astype("int64"),
        pd.to_numeric(frozen_all.player_id, errors="raise").astype("int64"),
    ))
    lineage = build_lineage(cohort, candidates, raw, all_history_keys)
    require(lineage[["source_pbp_sha256", "source_boxscore_sha256", "source_shiftcharts_sha256"]]
            .apply(lambda column: column.astype(str).str.fullmatch(r"[0-9a-f]{64}").all()).all(),
            "SOURCE_HASH_FORMAT_INVALID")
    lineage_counts = lineage.groupby(["target_game_date", "target_game_id", "target_player_id"], sort=False).size()
    cohort_index = pd.MultiIndex.from_frame(cohort[["game_date", "game_id", "player_id"]].rename(
        columns={"game_date": "target_game_date", "game_id": "target_game_id", "player_id": "target_player_id"}))
    counts_aligned = lineage_counts.reindex(cohort_index)
    require(counts_aligned.notna().all(), "TARGET_LINEAGE_MISSING")
    require(np.array_equal(counts_aligned.to_numpy(dtype=int), cohort["prior_player_games_in_120d"].to_numpy(dtype=int)),
            "TARGET_LINEAGE_CARDINALITY_MISMATCH")
    source_means = lineage.groupby(["target_game_date", "target_game_id", "target_player_id"], sort=False)["verified_pp_toi_minutes"].mean().reindex(cohort_index)
    replay_after = cohort.set_index(key).loc[:, "mean_pp_toi_last10_after_verified_source_values"]
    replay_after.index = pd.MultiIndex.from_frame(cohort[["game_date", "game_id", "player_id"]].rename(
        columns={"game_date": "target_game_date", "game_id": "target_game_id", "player_id": "target_player_id"}))
    require(np.allclose(source_means.to_numpy(), replay_after.to_numpy(), rtol=0, atol=1e-10),
            "LINEAGE_RECONSTRUCTED_FEATURE_DOES_NOT_MATCH_REPLAY")
    raw_means = lineage.groupby(["target_game_date", "target_game_id", "target_player_id"], sort=False)["current_pp_toi_minutes"].mean().reindex(cohort_index)
    replay_before = cohort.set_index(key).loc[:, "mean_pp_toi_last10_before"]
    replay_before.index = replay_after.index
    require(np.allclose(raw_means.to_numpy(), replay_before.to_numpy(), rtol=0, atol=1e-10),
            "LINEAGE_RAW_FEATURE_DOES_NOT_MATCH_REPLAY")

    cohort["outcome_class"] = outcome_class(cohort["realized_points"])
    require(cohort["outcome_class"].eq(outcome_class(cohort["outcome_category"])).all(), "OUTCOME_CLASS_SOURCE_MISMATCH")
    cohort["mean_pp_toi_last10_frozen"] = cohort["mean_pp_toi_last10"]
    cohort["mean_pp_toi_last10"] = cohort["mean_pp_toi_last10_after_verified_source_values"]
    cohort["counterfactual_delta_vs_frozen"] = cohort["mean_pp_toi_last10"] - cohort["mean_pp_toi_last10_frozen"]
    cohort["counterfactual_delta_vs_current_raw_replay"] = cohort["mean_pp_toi_last10"] - cohort["mean_pp_toi_last10_before"]
    cohort["pp_toi_qualification_status"] = "FULLY_SOURCE_VERIFIED_HISTORY"
    cohort.loc[~cohort["current_raw_replay_matches_frozen_feature"].eq(True), "pp_toi_qualification_status"] = \
        "FULLY_SOURCE_VERIFIED_BASELINE_EXCEPTION_326"
    cohort["baseline_compatibility_status"] = np.where(
        cohort["current_raw_replay_matches_frozen_feature"].eq(True),
        "EXACT_CURRENT_RAW_REPLAY_MATCH", "BASELINE_EXCEPTION_RETAINED_SEPARATELY")
    cohort["qualification_logic_id"] = "ALL_NONEMPTY_LAST10_CONTRIBUTORS_SOURCE_VERIFIED_V1"
    cohort["qualification_verified_contributor_count"] = cohort["verified_reconstruction_rows_in_window"].astype("int64")
    cohort["source_candidate_sha256"] = input_hashes["verified_candidate_overlay"]
    cohort["counterfactual_replay_sha256"] = input_hashes["counterfactual_replay"]
    cohort["frozen_training_input_sha256"] = input_hashes["frozen_training_input"]

    keep = [
        "game_date", "game_id", "player_id", "canonical_season", "team_id", "history_contract",
        *FEATURES, "mean_pp_toi_last10_frozen", "outcome_class", "realized_points", "goals", "assists",
        "outcome_category", "over_0_5", "over_1_5", "over_2_5",
        "pp_toi_qualification_status", "baseline_compatibility_status", "qualification_logic_id",
        "prior_player_games_in_120d", "verified_reconstruction_rows_in_window",
        "qualification_verified_contributor_count", "current_raw_replay_matches_frozen_feature",
        "mean_pp_toi_last10_before", "counterfactual_delta_vs_frozen",
        "counterfactual_delta_vs_current_raw_replay", "source_candidate_sha256",
        "counterfactual_replay_sha256", "frozen_training_input_sha256",
    ]
    feature_data = cohort[keep].copy()
    require(len(feature_data) == EXPECTED_ROWS["qualified"], "OUTPUT_COHORT_ROW_COUNT_MISMATCH")
    require(not feature_data.duplicated(["game_date", "game_id", "player_id"]).any(), "OUTPUT_DUPLICATE_IDENTITY")
    require(feature_data[FEATURES + ["realized_points", "team_id"]].notna().all().all(), "FEATURE_OR_OUTCOME_COMPLETENESS_FAILURE")
    # The only HGB feature allowed to differ from the frozen input is PP TOI.
    for feature in FEATURES:
        if feature != "mean_pp_toi_last10":
            require(np.array_equal(feature_data[feature].to_numpy(), cohort[feature].to_numpy(), equal_nan=True),
                    f"NON_PP_TOI_FEATURE_CHANGED:{feature}")

    # Keep every prior mismatch in a standalone file with explicit qualification.
    exceptions["qualification_status"] = np.where(
        qualify_replay_rows(exceptions), "FULLY_VERIFIED_HISTORY_BASELINE_EXCEPTION",
        "PARTIAL_VERIFICATION_BASELINE_EXCEPTION_EXCLUDED_FROM_FEATURE_DATASET")
    exceptions["verified_contributor_count"] = pd.to_numeric(
        exceptions["verified_reconstruction_rows_in_window"], errors="coerce").fillna(0).astype("int64")
    exception_keep = [*key, "canonical_season", "team_id", "mean_pp_toi_last10", "mean_pp_toi_last10_before",
                      "mean_pp_toi_last10_after_verified_source_values", "prior_player_games_in_120d",
                      "verified_reconstruction_rows_in_window", "verified_contributor_count",
                      "qualification_status", "status"]
    exception_data = exceptions[exception_keep].copy()
    strata = make_strata(joined, cohort)

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "feature_dataset": out_dir / "points_hgb_verified_pp_toi_research_features.csv",
        "strict_prior_lineage": out_dir / "strict_prior_verified_contributor_lineage.csv",
        "baseline_exceptions": out_dir / "baseline_exceptions_326.csv",
        "population_strata": out_dir / "research_population_strata.csv",
    }
    feature_data.to_csv(paths["feature_dataset"], index=False, float_format="%.17g")
    lineage.to_csv(paths["strict_prior_lineage"], index=False, float_format="%.17g")
    exception_data.to_csv(paths["baseline_exceptions"], index=False, float_format="%.17g")
    strata.to_csv(paths["population_strata"], index=False, float_format="%.17g")
    output_hashes = {name: sha256(path) for name, path in paths.items()}
    pp_delta = feature_data["counterfactual_delta_vs_frozen"]
    replay_delta = feature_data["counterfactual_delta_vs_current_raw_replay"]
    manifest: dict[str, object] = {
        "schema_version": "NHL_POINTS_VERIFIED_PP_TOI_RESEARCH_DATASET_V1",
        "status": "READY_WITH_EXPLICIT_BASELINE_EXCEPTIONS",
        "research_only": True,
        "production_authority": False,
        "database_mutation": False,
        "model_training_scoring_or_promotion": False,
        "pending_32_row_correction_applied": False,
        "frozen_population_rows": len(joined),
        "qualified_fully_source_verified_rows": len(feature_data),
        "qualified_rows_with_exact_current_raw_baseline_reproduction": len(exact_cohort),
        "exact_baseline_reproduction_percent_of_qualified_population": round(len(exact_cohort) / len(cohort) * 100, 8),
        "fully_verified_baseline_exceptions": int(exception_qualified.sum()),
        "partial_verification_baseline_exceptions_excluded": int((~exception_qualified).sum()),
        "baseline_exceptions_total": len(exception_data),
        "counterfactual_changed_vs_frozen_feature_rows": int(pp_delta.abs().gt(1e-12).sum()),
        "counterfactual_changed_vs_current_raw_replay_rows": int(replay_delta.abs().gt(1e-12).sum()),
        "mean_absolute_counterfactual_delta_vs_frozen": float(pp_delta.abs().mean()),
        "median_absolute_counterfactual_delta_vs_frozen": float(pp_delta.abs().median()),
        "p90_absolute_counterfactual_delta_vs_frozen": float(pp_delta.abs().quantile(0.90)),
        "maximum_absolute_counterfactual_delta_vs_frozen": float(pp_delta.abs().max()),
        "verified_contributor_lineage_rows": len(lineage),
        "counterfactual_feature_reproduced_from_lineage_rows": len(cohort),
        "counterfactual_feature_reproduction_percent": 100.0,
        "verified_contributor_history_min_max": [int(lineage.groupby(["target_game_date", "target_game_id", "target_player_id"]).size().min()),
                                                  int(lineage.groupby(["target_game_date", "target_game_id", "target_player_id"]).size().max())],
        "strict_prior_violations": 0,
        "outside_120_day_window_rows": 0,
        "history_selection": "strict target game_date exclusion; inclusive target_date-120d lower bound; latest 10 verified player-games ordered by game_date, game_id",
        "feature_contract": "NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED",
        "hgb_feature_order": FEATURES,
        "only_changed_model_feature": "mean_pp_toi_last10",
        "source_artifact_paths": {name: path.relative_to(ROOT).as_posix() for name, path in source_paths.items()},
        "source_artifact_sha256": input_hashes,
        "output_artifact_sha256": output_hashes,
        "output_row_counts": {name: len(pd.read_csv(path)) for name, path in paths.items()},
        "source_expectations": EXPECTED_SHA256,
        "integrity_checks": {
            "frozen_population_exact_row_identity_coverage": True,
            "frozen_replay_duplicate_prevention": True,
            "qualified_dataset_duplicate_prevention": True,
            "all_hgb_features_and_outcomes_complete": True,
            "verified_source_hashes_present_and_well_formed": True,
            "strict_prior_latest_ten_120_day_lineage_verified": True,
            "lineage_recomputed_counterfactual_matches_retained_replay": True,
            "only_mean_pp_toi_last10_changed_among_hgb_features": True,
            "baseline_exceptions_retained_separately_and_flagged": True,
        },
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    readme = [
        "# Verified NHL Points PP-TOI Research Dataset",
        "",
        "Research-only, offline counterfactual. No source acquisition, database write, model fit/score, or promotion occurred. The pending 32-row correction was not applied to canonical data; the retained counterfactual replay was used as-is.",
        "",
        f"Qualified feature rows: **{len(feature_data):,}** of the immutable **{len(joined):,}** frozen population.",
        f"Exact current-raw replay equality to the frozen PP-TOI feature: **{len(exact_cohort):,}/{len(cohort):,} ({manifest['exact_baseline_reproduction_percent_of_qualified_population']:.2f}%)**. The **{len(exception_data):,}** baseline exceptions are separate; {int(exception_qualified.sum())} are fully source-verified but explicitly flagged in the feature dataset, and {int((~exception_qualified).sum())} are excluded from it.",
        "The verified counterfactual feature is independently recomputed from the retained contributor lineage for **100% of the 21,599 qualified rows**. This is corrected-value reproducibility; it is distinct from exact current-raw reproduction of the frozen baseline.",
        "",
        "`points_hgb_verified_pp_toi_research_features.csv` retains the 21,599 qualified rows. `mean_pp_toi_last10` is replaced by the retained verified counterfactual value; the frozen value is retained as `mean_pp_toi_last10_frozen`. Other HGB V1 model features are copied unchanged. Rows with baseline compatibility exceptions are labeled and must not be silently included in exact-baseline comparisons.",
        "",
        "`strict_prior_verified_contributor_lineage.csv` retains each source-verified contributor identity, original/reconstructed PP-TOI, reconciliation status, and PBP/boxscore/shiftchart hashes. It includes only strict-prior rows in the inclusive 120-day window and the latest ten for each target.",
        "",
        "`baseline_exceptions_326.csv` preserves all 326 known current-raw replay versus frozen-feature mismatches. The 246 partially verified exceptions are not admitted to the feature dataset. `research_population_strata.csv` includes frozen-population and qualified-cohort counts for season, season-month, prior-window-game count, lifetime-game count, and points outcome strata.",
        "",
        "The manifest records source and output SHA-256 values, row counts, feature order, qualification/reproduction counts, and invariants. Rebuild with `./bin/python backend/nhl/scripts/build_nhl_points_verified_pp_toi_research_dataset.py --out-dir artifacts/analysis/nhl/pp_toi_verified_feature_preparation_2026-10-16`.",
        "",
        "This dataset is for controlled research only. It does not authorize model training, scoring, promotion, or canonical data changes.",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(readme))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    manifest = run(args.out_dir)
    print(json.dumps({
        "status": manifest["status"],
        "qualified_rows": manifest["qualified_fully_source_verified_rows"],
        "baseline_exceptions": manifest["baseline_exceptions_total"],
        "exact_baseline_reproduction_percent": manifest["exact_baseline_reproduction_percent_of_qualified_population"],
        "output_artifact_sha256": manifest["output_artifact_sha256"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
