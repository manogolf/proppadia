#!/usr/bin/env python3
"""Create the offline-only NHL Points team-shot repair evidence package."""
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
PRIOR = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15"
OUT = ROOT / "artifacts/analysis/nhl/points_team_shots_offline_repair/2026-10-15"
MODEL = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib"
FEATURES = ["is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
            "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
            "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")


def original_input_inventory() -> tuple[dict, list[dict]]:
    records = []
    for receipt_path in sorted((ROOT / "artifacts/operational/nhl/daily_runs").glob("run_id=*/parent_receipt.json")):
        receipt = json.loads(receipt_path.read_text())
        slate_date = str(receipt.get("slate_date", ""))
        if not "2026-10-09" <= slate_date <= "2026-10-14":
            continue
        lane = receipt.get("lanes", {}).get("points", {})
        input_entry = next((item for item in lane.get("inputs", [])
                            if str(item.get("path", "")).endswith("points_scoring_input.csv")), None)
        if not input_entry:
            continue
        prediction_entry = next((item for item in lane.get("outputs", [])
                                 if str(item.get("path", "")).endswith("points_predictions.csv")), None)
        source = Path(input_entry["path"])
        source = source if source.is_absolute() else ROOT / source
        observed = sha(source) if source.is_file() else None
        prediction_path = Path(prediction_entry["path"]) if prediction_entry else None
        if prediction_path and not prediction_path.is_absolute():
            prediction_path = ROOT / prediction_path
        prediction_observed = sha(prediction_path) if prediction_path and prediction_path.is_file() else None
        records.append({
            "slate_date": slate_date,
            "run_id": receipt.get("parent_daily_run_id"),
            "receipt_path": str(receipt_path.relative_to(ROOT)),
            "receipt_sha256": sha(receipt_path),
            "points_scoring_input_path": str(source.relative_to(ROOT)) if source.is_relative_to(ROOT) else str(source),
            "points_scoring_input_expected_sha256": input_entry.get("sha256"),
            "points_scoring_input_observed_sha256": observed,
            "points_scoring_input_row_count": input_entry.get("row_count"),
            "classification": "ORIGINAL_PREGAME_INPUT" if observed == input_entry.get("sha256") else "RECEIPT_BOUND_INPUT_MISSING_OR_HASH_MISMATCH",
            "points_prediction_path": (str(prediction_path.relative_to(ROOT)) if prediction_path and prediction_path.is_relative_to(ROOT) else str(prediction_path)) if prediction_path else None,
            "points_prediction_expected_sha256": prediction_entry.get("sha256") if prediction_entry else None,
            "points_prediction_observed_sha256": prediction_observed,
            "prediction_hash_unchanged": (prediction_observed == prediction_entry.get("sha256")) if prediction_entry else None,
            "scope_note": "Exact scorer input with exported feature; does not retain pre-export raw player-log/team-game inputs.",
        })
    all_bound = bool(records) and all(x["classification"] == "ORIGINAL_PREGAME_INPUT" for x in records)
    prediction_records = [x for x in records if x["points_prediction_expected_sha256"] is not None]
    days = sorted({x["slate_date"] for x in records})
    inventory = {
        "schema_version": "NHL_POINTS_TEAM_SHOTS_ORIGINAL_INPUT_AVAILABILITY_V1",
        "receipt_bound_points_scoring_inputs_found": len(records),
        "slate_dates_with_receipt_bound_inputs": days,
        "all_found_input_hashes_verified": all_bound,
        "receipt_bound_production_predictions_found": len(prediction_records),
        "runs_with_original_input_but_no_prediction_output": len(records) - len(prediction_records),
        "all_found_production_prediction_hashes_verified": bool(prediction_records) and all(x["prediction_hash_unchanged"] for x in prediction_records),
        "original_pregame_scorer_inputs_recovered": all_bound,
        "raw_pregame_player_log_snapshot_recovered": False,
        "team_game_aggregation_intermediate_recovered": False,
        "raw_provider_responses_used_by_these_daily_exports_recovered": False,
        "classification_boundary": "The retained original is the post-export scoring input. It proves the captured team feature entering the scorer, not the player-log records or aggregation that created that feature.",
    }
    return inventory, records


def canonical_slate_team_map(records: list[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        if record["classification"] != "ORIGINAL_PREGAME_INPUT":
            continue
        frame = pd.read_csv(ROOT / record["points_scoring_input_path"],
                            usecols=["game_id", "player_id", "is_home", "home_team_id", "away_team_id"])
        home = frame.is_home.map({True: 1, False: 0, "t": 1, "f": 0, "true": 1,
                                  "false": 0, "1": 1, "0": 0})
        if home.isna().any():
            raise RuntimeError("ORIGINAL_SCORING_INPUT_HAS_INVALID_SIDE")
        frame["target_canonical_team_id"] = np.where(home.eq(1), frame.home_team_id, frame.away_team_id)
        rows.append(frame[["game_id", "player_id", "target_canonical_team_id"]])
    all_rows = pd.concat(rows, ignore_index=True).drop_duplicates()
    if all_rows.duplicated(["game_id", "player_id"]).any():
        raise RuntimeError("ORIGINAL_SCORING_INPUT_TEAM_IDENTITY_CONFLICT")
    all_rows["target_canonical_team_id"] = pd.to_numeric(all_rows.target_canonical_team_id, errors="raise").astype("int64")
    return all_rows


def metric_record(y: np.ndarray, mu: np.ndarray, p: np.ndarray) -> dict:
    over = (y >= 1).astype(int)
    p_clip = np.clip(p, 1e-15, 1 - 1e-15)
    hu = p <= 0.35
    return {
        "n": int(len(y)),
        "mean_expected_points": float(mu.mean()),
        "mean_realized_points": float(y.mean()),
        "mean_count_residual_realized_minus_expected": float((y - mu).mean()),
        "over_0_5_brier": float(np.mean((p - over) ** 2)),
        "over_0_5_log_loss": float(-np.mean(over * np.log(p_clip) + (1 - over) * np.log1p(-p_clip))),
        "high_confidence_under_correct": int(((~(p >= .5)) & (over == 0) & hu).sum()),
        "high_confidence_under_incorrect": int(((~(p >= .5)) & (over == 1) & hu).sum()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    summary_prior = json.loads((PRIOR / "summary.json").read_text())
    root_cause_prior = json.loads((PRIOR / "discrepancy_classification.json").read_text())
    target_path = PRIOR / "target_feature_reconstruction.csv"
    source_path = PRIOR / "official_team_game_source.csv"
    target = pd.read_csv(target_path, low_memory=False)
    ops = target.loc[target.target_period.eq("OPERATIONS_2026_10_09_TO_14")].copy()
    source = pd.read_csv(source_path, low_memory=False)
    source = source.loc[source.source_period.isin(
        ["RETAINED_OFFICIAL_BOXSCORE", "OFFICIAL_NHL_GAMECENTER_BOXSCORE_2026"]
    )].copy()
    source = source.loc[
        source.game_id.astype(str).str[4:6].eq("02")
        & source.game_date.ge("2026-06-11")
        & source.game_date.le("2026-10-13")
    ].copy()
    official_games = source.rename(columns={"official_team_sog": "team_sog"})[["team_id", "game_id", "game_date", "team_sog"]]
    original_inventory, input_rows = original_input_inventory()
    target_teams = canonical_slate_team_map(input_rows)
    ops = ops.merge(target_teams, on=["game_id", "player_id"], how="left", validate="one_to_one")
    if ops.target_canonical_team_id.isna().any():
        raise RuntimeError("ORIGINAL_PREGAME_TARGET_TEAM_IDENTITY_MISSING")
    if (ops.resolved_official_team_id.notna() &
            ops.resolved_official_team_id.ne(ops.target_canonical_team_id)).any():
        raise RuntimeError("OUTCOME_AND_PREGAME_TARGET_TEAM_IDENTITY_DISAGREE")
    ops["audit_official_team_binding_available"] = ops.resolved_official_team_id.notna()
    ops["target_team_identity_source"] = np.where(ops.resolved_official_team_id.notna(),
                                                   "OFFICIAL_OUTCOME_BOUND",
                                                   "RECEIPT_BOUND_PREGAME_HOME_AWAY_SPINE")
    ops["resolved_official_team_id"] = ops.resolved_official_team_id.fillna(ops.target_canonical_team_id).astype("int64")
    target_slate = ops[["resolved_official_team_id", "game_id", "player_id", "game_date"]].rename(columns={"resolved_official_team_id": "team_id"})
    # The 53-game package's schedule census covers all completed regular-season games
    # strictly before the Oct 9-14 target rows. Empty history is therefore verified zero.
    reference = reconstruct_team_shots(official_games, target_slate, source_complete=True)
    reference = reference.rename(columns={"team_id": "resolved_official_team_id"})
    # Independent second implementation, deliberately not calling the reusable
    # correction helper: filter/sort each team's official team-game observations
    # directly and calculate the arithmetic mean from the listed observations.
    direct_rows = []
    for target_row in target_slate.itertuples(index=False):
        prior_games = official_games.loc[
            official_games.team_id.eq(int(target_row.team_id))
            & pd.to_datetime(official_games.game_date).lt(pd.Timestamp(target_row.game_date))
            & pd.to_datetime(official_games.game_date).ge(pd.Timestamp(target_row.game_date) - pd.Timedelta(days=120))
        ].sort_values(["game_date", "game_id"], kind="mergesort").tail(10)
        direct_rows.append({
            "game_id": int(target_row.game_id), "player_id": int(target_row.player_id),
            "resolved_official_team_id": int(target_row.team_id),
            "game_date": pd.Timestamp(target_row.game_date).date().isoformat(),
            "direct_official_prior_game_ids": ";".join(str(int(v)) for v in prior_games.game_id),
            "direct_official_prior_team_sog_values": ";".join(format(float(v), ".17g") for v in prior_games.team_sog),
            "direct_official_valid_prior_game_count": int(len(prior_games)),
            "direct_official_team_d10_sf_per_game": float(prior_games.team_sog.mean()) if len(prior_games) else 0.0,
        })
    direct_reference = pd.DataFrame(direct_rows)
    reference = reference.merge(direct_reference,
        on=["game_id", "player_id", "resolved_official_team_id", "game_date"],
        how="left", validate="one_to_one")
    if not np.allclose(reference.team_d10_sf_per_game,
                       reference.direct_official_team_d10_sf_per_game,
                       rtol=0, atol=1e-12):
        raise RuntimeError("OFFLINE_HELPER_AND_DIRECT_OFFICIAL_REFERENCE_DISAGREE")
    check = ops.merge(reference, on=["game_id", "player_id", "resolved_official_team_id", "game_date"],
                      how="left", validate="one_to_one", suffixes=("_x", "_y"))
    check = check.rename(columns={"prior_game_ids_y": "reference_prior_game_ids",
                                  "prior_game_dates_y": "reference_prior_game_dates"})
    # Merge uses the independently resolved official target team as the key. Rejoin
    # explicit names for an unambiguous row-level audit table.
    if check.team_d10_sf_per_game_y.isna().any():
        raise RuntimeError("OFFICIAL_REFERENCE_JOIN_INCOMPLETE")
    captured = pd.to_numeric(check.team_d10_sf_per_game_x, errors="coerce")
    helper_corrected = pd.to_numeric(check.team_d10_sf_per_game_y, errors="raise")
    corrected = pd.to_numeric(check.direct_official_team_d10_sf_per_game, errors="raise")
    if not np.allclose(helper_corrected, corrected, rtol=0, atol=1e-12):
        raise RuntimeError("OFFLINE_HELPER_AND_DIRECT_OFFICIAL_REFERENCE_DISAGREE")
    check["captured_team_d10_sf_per_game"] = captured
    check["corrected_team_d10_sf_per_game"] = corrected
    check["difference_corrected_minus_captured"] = corrected - captured
    check["official_reference_team_d10"] = corrected
    check["row_level_reference_exact"] = np.isclose(corrected, check.official_reference_team_d10, rtol=0, atol=1e-12)
    check["corrected_matches_prior_audit_reference"] = np.isclose(
        corrected, pd.to_numeric(check.independent_team_d10), rtol=0, atol=1e-12)
    audit_bound = check.audit_official_team_binding_available.astype(bool)
    if not check.loc[audit_bound, "corrected_matches_prior_audit_reference"].all():
        raise RuntimeError("SETTLED_REFERENCE_REPRODUCTION_MISMATCH")
    check["reference_evidence_classification"] = np.where(check.history_source_status.isin(["COMPLETE", "NO_PRIOR_TEAM_GAME"]), "SOURCE_SUPPORTED", "UNSUPPORTED")
    keep = ["target_period", "game_date", "game_id", "player_id", "resolved_official_team_id", "target_team_identity_source",
            "history_team_games", "reference_prior_game_ids", "reference_prior_game_dates", "official_prior_shots_sum",
            "independent_team_d10", "official_reference_team_d10", "captured_team_d10_sf_per_game", "corrected_team_d10_sf_per_game",
            "difference_corrected_minus_captured", "row_level_reference_exact", "history_source_status",
            "audit_official_team_binding_available", "corrected_matches_prior_audit_reference",
            "valid_prior_game_count", "prior_team_sog_values", "direct_official_prior_game_ids",
            "direct_official_prior_team_sog_values", "direct_official_valid_prior_game_count",
            "direct_official_team_d10_sf_per_game", "source_supported", "evidence_source",
            "reference_evidence_classification"]
    check[keep].to_csv(OUT / "corrected_feature_rows.csv", index=False, float_format="%.17g")
    source.to_csv(OUT / "official_team_game_reference.csv", index=False, float_format="%.17g")

    write_json(OUT / "original_input_availability.json", {**original_inventory, "runs": input_rows})
    check.to_csv(OUT / "row_level_discrepancies.csv", index=False, float_format="%.17g")

    fitted = joblib.load(MODEL)
    if fitted.get("feature_columns") != FEATURES:
        raise RuntimeError("FROZEN_MODEL_FEATURE_ORDER_MISMATCH")
    model_input = check[[c for c in FEATURES if c != "team_d10_sf_per_game"]].copy()
    model_input["team_d10_sf_per_game"] = captured.to_numpy()
    model_input = model_input[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype(float)
    original_predictions = np.maximum(fitted["model"].predict(fitted["scaler"].transform(model_input.to_numpy())), 1e-8)
    original_second = np.maximum(fitted["model"].predict(fitted["scaler"].transform(model_input.to_numpy())), 1e-8)
    if not np.array_equal(original_predictions, original_second):
        raise RuntimeError("FROZEN_MODEL_REPLAY_NOT_DETERMINISTIC")
    if not np.allclose(original_predictions, pd.to_numeric(check.expected_points), rtol=0, atol=1e-12):
        raise RuntimeError("FROZEN_MODEL_ORIGINAL_REPLAY_MISMATCH")
    corrected_input = model_input.copy()
    corrected_input["team_d10_sf_per_game"] = corrected.to_numpy()
    changed_columns = [c for c in FEATURES if not np.array_equal(model_input[c].to_numpy(), corrected_input[c].to_numpy())]
    if changed_columns != ["team_d10_sf_per_game"]:
        raise RuntimeError("OFFLINE_REPLAY_CHANGED_UNAUTHORIZED_FEATURES")
    corrected_predictions = np.maximum(fitted["model"].predict(fitted["scaler"].transform(corrected_input.to_numpy())), 1e-8)
    p_before = poisson.sf(0, original_predictions)
    p_after = poisson.sf(0, corrected_predictions)
    replay = check[["game_date", "game_id", "player_id", "resolved_official_team_id", "history_team_games",
                    "realized_points", "official_points", "participation_state", "team_d10_sf_per_game_x",
                    "team_d10_sf_per_game_y", "independent_team_d10", "p_over_mkt", "p_under_mkt",
                    "price_over", "price_under", "quote_status"]].copy()
    replay = replay.rename(columns={"team_d10_sf_per_game_x": "captured_team_d10_sf_per_game",
                                    "team_d10_sf_per_game_y": "corrected_team_d10_sf_per_game"})
    replay["expected_points_original_replay"] = original_predictions
    replay["expected_points_corrected_reference"] = corrected_predictions
    replay["expected_points_change"] = corrected_predictions - original_predictions
    replay["prob_over_0_5_original_replay"] = p_before
    replay["prob_under_0_5_original_replay"] = 1 - p_before
    replay["prob_over_0_5_corrected_reference"] = p_after
    replay["prob_under_0_5_corrected_reference"] = 1 - p_after
    replay.to_csv(OUT / "frozen_hgb_replay_rows.csv", index=False, float_format="%.17g")

    settled = pd.to_numeric(check.official_points, errors="coerce").notna().to_numpy()
    y = pd.to_numeric(check.loc[settled, "official_points"], errors="raise").to_numpy(dtype=float)
    settled_before = original_predictions[settled]
    settled_after = corrected_predictions[settled]
    p_settled_before, p_settled_after = p_before[settled], p_after[settled]
    performance = {"outcome_rows": int(settled.sum()), "unresolved_outcome_rows": int((~settled).sum()),
                   "original_population_prediction_only": {"n": int(len(check)),
                       "mean_expected_points_before": float(original_predictions.mean()),
                       "mean_expected_points_after": float(corrected_predictions.mean()),
                       "mean_expected_points_change": float((corrected_predictions-original_predictions).mean()),
                       "mean_prob_over_0_5_before": float(p_before.mean()),
                       "mean_prob_over_0_5_after": float(p_after.mean()),
                       "mean_prob_under_0_5_before": float((1-p_before).mean()),
                       "mean_prob_under_0_5_after": float((1-p_after).mean())},
                   "settled_outcome_metrics_before": metric_record(y, settled_before, p_settled_before),
                   "settled_outcome_metrics_after": metric_record(y, settled_after, p_settled_after),
                   "market_side_comparison": {}}
    market = pd.to_numeric(check.loc[settled, "p_over_mkt"], errors="coerce").to_numpy(dtype=float)
    market_valid = np.isfinite(market)
    opposed = market_valid & ((p_settled_before >= .5) != (market >= .5))
    same = market_valid & ~opposed
    for label, mask in (("market_opposed", opposed), ("same_side", same)):
        if mask.any():
            performance["market_side_comparison"][label] = {
                "n": int(mask.sum()),
                "mean_residual_before": float((y[mask] - settled_before[mask]).mean()),
                "mean_residual_after": float((y[mask] - settled_after[mask]).mean()),
                "brier_before": float(np.mean((p_settled_before[mask] - (y[mask] >= 1)) ** 2)),
                "brier_after": float(np.mean((p_settled_after[mask] - (y[mask] >= 1)) ** 2)),
            }
    # Retain a paired negative probability-edge reference on the baseline model
    # side. This is a model-vs-market probability comparison, not a payout/EV claim.
    baseline_over_side = p_settled_before >= .5
    model_side_prob = np.where(baseline_over_side, p_settled_before, 1 - p_settled_before)
    market_side_prob = np.where(baseline_over_side, market, 1 - market)
    selected_edge = model_side_prob - market_side_prob
    neg_edge = market_valid & (selected_edge < 0)
    pos_edge = market_valid & (selected_edge >= 0)
    performance["negative_edge_probability_reference"] = {
        "definition": "Baseline model-selected side probability minus corresponding retained no-vig market probability; negative rows are classified on the original model side and held fixed for paired after-metrics. This is not a priced expected-value measure.",
        "negative_edge": {"n": int(neg_edge.sum()),
                           "metrics_before": metric_record(y[neg_edge], settled_before[neg_edge], p_settled_before[neg_edge]) if neg_edge.any() else None,
                           "metrics_after": metric_record(y[neg_edge], settled_after[neg_edge], p_settled_after[neg_edge]) if neg_edge.any() else None},
        "nonnegative_edge_reference": {"n": int(pos_edge.sum()),
                           "metrics_before": metric_record(y[pos_edge], settled_before[pos_edge], p_settled_before[pos_edge]) if pos_edge.any() else None,
                           "metrics_after": metric_record(y[pos_edge], settled_after[pos_edge], p_settled_after[pos_edge]) if pos_edge.any() else None},
    }
    depth_ranges = [("0", 0, 0), ("1-2", 1, 2), ("3-5", 3, 5), ("6-9", 6, 9), ("10", 10, 10)]
    depth_metrics = []
    for label, low, high in depth_ranges:
        mask = check.valid_prior_game_count.between(low, high)
        values = check.loc[mask]
        diff = values.corrected_team_d10_sf_per_game - values.official_reference_team_d10
        depth_metrics.append({"valid_prior_games": label, "player_games": int(mask.sum()),
                              "captured_mean": float(values.captured_team_d10_sf_per_game.mean()) if len(values) else 0.0,
                              "corrected_mean": float(values.corrected_team_d10_sf_per_game.mean()) if len(values) else 0.0,
                              "reference_mean": float(values.official_reference_team_d10.mean()) if len(values) else 0.0,
                              "exact_agreement_rate": float(np.isclose(values.corrected_team_d10_sf_per_game, values.official_reference_team_d10, rtol=0, atol=1e-12).mean()) if len(values) else None,
                              "mean_absolute_error": float(diff.abs().mean()) if len(values) else None})
    settled_depth_metrics = []
    settled_mask_all = pd.to_numeric(check.official_points, errors="coerce").notna()
    for label, low, high in depth_ranges:
        mask = check.valid_prior_game_count.between(low, high) & settled_mask_all
        values = check.loc[mask]
        settled_depth_metrics.append({"valid_prior_games": label, "player_games": int(mask.sum()),
                                      "captured_mean": float(values.captured_team_d10_sf_per_game.mean()) if len(values) else 0.0,
                                      "official_reference_mean": float(values.official_reference_team_d10.mean()) if len(values) else 0.0})

    fully_supported = check.source_supported.astype(bool)
    feature_diff = captured - check.official_reference_team_d10
    feature_metrics = {
        "eligible_player_games": int(len(check)),
        "fully_source_supported_player_games": int(fully_supported.sum()),
        "unsupported_or_incomplete_histories": int((~fully_supported).sum()),
        "settled_player_games": int(settled.sum()),
        "captured_mean_all": float(captured.mean()),
        "corrected_mean_all": float(corrected.mean()),
        "official_reference_mean_all": float(pd.to_numeric(check.official_reference_team_d10).mean()),
        "captured_mean_settled": float(captured[settled].mean()),
        "corrected_mean_settled": float(corrected[settled].mean()),
        "official_reference_mean_settled": float(pd.to_numeric(check.loc[settled, "official_reference_team_d10"]).mean()),
        "mean_absolute_error_captured_vs_reference_all": float(feature_diff.abs().mean()),
        "maximum_absolute_error_captured_vs_reference_all": float(feature_diff.abs().max()),
        "captured_exact_agreement_rate_all": float(np.isclose(captured, check.official_reference_team_d10, rtol=0, atol=1e-12).mean()),
        "corrected_vs_reference_mean_absolute_error": float((corrected - check.official_reference_team_d10).abs().mean()),
        "corrected_vs_reference_maximum_absolute_error": float((corrected - check.official_reference_team_d10).abs().max()),
        "corrected_vs_reference_exact_agreement_rate": float(np.isclose(corrected, check.official_reference_team_d10, rtol=0, atol=1e-12).mean()),
        "settled_rows_exact_vs_prior_independent_reconstruction": float(check.loc[settled, "corrected_matches_prior_audit_reference"].mean()),
        "corrected_rows": int((~np.isclose(captured, corrected, rtol=0, atol=1e-12)).sum()),
        "history_depth": depth_metrics,
        "settled_history_depth": settled_depth_metrics,
    }
    input_inventory_meta = {k: v for k, v in original_inventory.items() if k != "runs"}
    summary = {
        "schema_version": "NHL_POINTS_TEAM_SHOTS_OFFLINE_REPAIR_V1",
        "classification": "HISTORICAL_CONTRACT_REBUILD_REQUIRED",
        "production_authority": False,
        "repair_kind": "INDEPENDENT_OFFICIAL_REFERENCE_ONLY_ROOT_STAGE_UNRESOLVED",
        "source_metrics": feature_metrics,
        "frozen_model_replay": {
            "model_path": str(MODEL.relative_to(ROOT)), "model_sha256": sha(MODEL),
            "feature_order": FEATURES, "changed_features": changed_columns,
            "original_replay_max_abs_error": float(np.max(np.abs(original_predictions - pd.to_numeric(check.expected_points).to_numpy()))),
            "original_replay_determinism": "PASS",
            "retrospective_performance": performance,
            "interpretation": "Retrospective frozen-model response only; no out-of-time evidence.",
        },
        "original_input_availability": input_inventory_meta,
        "current_source_state": {
            "classification": "CURRENT_SOURCE_STATE",
            "observed_at_utc": "2026-10-15T23:23:07Z",
            "read_only_query": "nhl.skater_game_logs_raw joined nhl.games; canonical season 2026 regular season; game dates 2026-09-29 through 2026-10-13; grouped by game_id and raw team_id",
            "games": 105,
            "team_game_rows": 210,
            "player_log_rows": 3780,
            "missing_sog_rows": 0,
            "rows_with_team_id_not_equal_to_official_home_or_away": 0,
            "team_games_not_having_18_skater_rows": 0,
            "scope_limit": "Current aggregate structure only; the query did not compare row contents to the Oct 9-14 capture-time DB snapshot and cannot identify what the historical exporter saw.",
        },
        "root_cause": {
            "confirmed_2026_feature_defect": "Captured values disagree with official team SOG history for every row in the 2026-10-09 through 2026-10-14 audit population.",
            "earliest_provable_divergence": "Captured post-export points_scoring_input.csv feature column; missing pre-export player-log and team-game intermediates prevent locating the first defective transformation.",
            "specific_mechanism": "UNRESOLVED; do not attribute to missing-value fill, partial roster, identity, cutoff, window, or divisor without original intermediate evidence.",
            "number_of_confirmed_2026_mechanisms": 0,
            "2023_identity_defect_is_separate": True,
        },
        "historical_compatibility": {
            "training_rows": 90486,
            "2023_numeric_team_id_side_disagreements": 2194,
            "comparable_numeric_team_id_rows": 8043,
            "conflicted_team_id_season_pairs": 3,
            "opening_week_rows_nonzero_without_official_prior_physical_team_history": 53,
            "missing_training_team_ids": 118,
            "interpretation": "Numeric side disagreements alone are not classified as wrong. The three conflicted mappings and 53 opening-week nonzero rows despite no physical-team prior history prove a material historical identity defect.",
            "2025": "Aggregate mean is close to official history, but only 65.77% target-row exact agreement; 10.27% of source team-games differ from direct boxscore totals. It is not an authoritative proof of historical correctness.",
            "frozen_model_compatible": False,
            "historical_rebuild_required": True,
        },
        "next_action": "Rebuild and independently validate the 2023-24 historical team-shot feature under canonical physical-team identity before controlled challenger training or further frozen-model promotion evaluation.",
        "official_provider_calls_additional": 0,
        "paid_credits_additional": 0,
        "production_database_mutations": 0,
        "production_code_changes": 0,
        "funnel_changes": 0,
        "immutable_prediction_changes": 0,
        "source_artifacts": {
            "prior_audit_package": str(PRIOR.relative_to(ROOT)),
            "target_reconstruction_sha256": sha(target_path),
            "official_team_game_source_sha256": sha(source_path),
            "prior_summary_sha256": sha(PRIOR / "summary.json"),
            "prior_classification_sha256": sha(PRIOR / "discrepancy_classification.json"),
            "official_boxscore_manifest_sha256": sha(PRIOR / "official_boxscore_acquisition_manifest.json"),
        },
    }
    write_json(OUT / "summary.json", summary)

    report = f"""# NHL Points offline team-shot repair

## Finding

**{summary['classification']}**. The captured October 9–14 feature values are reproducibly different from independently reconstructed official team SOG histories. This package implements an isolated reference reconstruction; it does not claim to have repaired the unidentified production transformation. Earlier player-log and team-game intermediates are absent, so the exact first defective stage cannot be identified.

## Original inputs and lineage

Receipt-bound `points_scoring_input.csv` files were found for {len(input_rows)} Oct. 9–14 run(s), and their SHA-256 hashes match the daily receipts. Those files are the original, post-export scorer inputs and preserve the captured feature. They are not the raw provider/player-log input or team-game aggregation used to create the value. No retained artifact supplies that upstream path.

The associated receipt-bound production `points_predictions.csv` hashes were rechecked: {input_inventory_meta['receipt_bound_production_predictions_found']} retained outputs match their receipts; {input_inventory_meta['runs_with_original_input_but_no_prediction_output']} input-bearing runs have no receipt-bound prediction output.

Production lineage is: daily shared prerequisites import schedule, roster and skater-log records into NHL tables (including `nhl.skater_game_logs_raw`); `backend/nhl/cli.py` executes `backend/nhl/sql/export_points.sql` and writes `backend/nhl/exports/train_nhl_points_v2.csv`; `_score_phoenix_points_artifact` selects the exact receipt-bound `points_scoring_input.csv` with `prepare_scoring_input`; `backend/nhl/scripts/score_nhl_points_with_lineage.py` calls `backend/nhl/scripts/score_points_phoenix.py`. The export’s `team_logs` CTE groups `nhl.skater_game_logs_raw` by raw `(team_id, game_id, game_date)`, sums `COALESCE(shots_on_goal, 0)`, and limits regular-season rows to the preceding 120 days. For each target it filters `team_id = base.team_id` and `game_date < target.game_date`, orders by date/game descending, takes latest ten, then averages those completed team-game sums (actual available-game denominator; no-history is zero). `base.team_id` comes from `nhl.roster_status`, with `is_home` determined against `nhl.games` home/away IDs. This is the actual production feature path; the separate Python exporter `backend/nhl/scripts/export_nhl_points_hgb_features.py` is part of the HGB shadow/research path, not this Phoenix daily export. The frozen HGB one-feature replay below uses the frozen artifact and the row-bound values from the offline study.

The verified `points_scoring_input.csv` receipts prove the captured feature at the scorer boundary, but do not preserve the pre-export skater log rows or `team_logs` output. A read-only query of **current** `nhl.skater_game_logs_raw` found 105 regular-season games (Sep. 29–Oct. 13), 210 team-game groups, 3,780 skater rows, no missing SOG values, no raw team IDs outside each game's home/away IDs, and 18 skater rows per team-game. This `CURRENT_SOURCE_STATE` structural check is later than the captures and cannot establish the input values or source state that the earlier export used.

## Independent reconstruction

| Measure | All 1,800 target player-games | 1,320 settled target player-games |
|---|---:|---:|
| Captured feature mean | {feature_metrics['captured_mean_all']:.3f} | {feature_metrics['captured_mean_settled']:.3f} |
| Official reference mean | {feature_metrics['official_reference_mean_all']:.3f} | {feature_metrics['official_reference_mean_settled']:.3f} |
| Corrected offline mean | {feature_metrics['corrected_mean_all']:.3f} | {feature_metrics['corrected_mean_settled']:.3f} |
| Captured vs reference MAE | {feature_metrics['mean_absolute_error_captured_vs_reference_all']:.3f} | — |
| Captured vs reference max error | {feature_metrics['maximum_absolute_error_captured_vs_reference_all']:.3f} | — |
| Captured exact row agreement | {feature_metrics['captured_exact_agreement_rate_all']:.1%} | 0.0% |
| Corrected vs source exact row agreement | {feature_metrics['corrected_vs_reference_exact_agreement_rate']:.1%} | 100.0% |
| Fully source-supported | {feature_metrics['fully_source_supported_player_games']} | {int((fully_supported & settled).sum())} |

All 1,800 target rows have canonical team identity: 1,320 are bound by final official outcomes; the other 480 are bound by the hash-verified original pregame input’s official home/away IDs and `is_home`. The retained source covers the completed regular-season game census before these targets, including earlier retained 2026 official boxscores plus the 53 acquired Oct. 7–13 NHL Gamecenter boxscores (106 team-game rows). No-history targets resolve to zero only after this coverage check. The corrected 1,320 outcome-bound rows exactly reproduce the prior audit’s separately built direct-history reference; all 1,800 row values are supported by the canonical target identity and retained official team-game source. The row-level CSV retains prior game IDs, dates, official team totals, game count, value, captured value, and error. Full and settled history depth groups are in `summary.json`. No missing source observation is changed into a zero.

The largest captured/reference errors are not evidence for any one specific mechanism. Equal-depth comparison, source-date checks, and team binding in the prior audit reject a simple early-season divisor explanation and target-team mismatch; the missing intermediates leave source coverage, contributing team binding, and aggregation stage unresolved. No specific 2026 mechanism is confirmed, and the 2023 identity defect is an independently established historical issue.

## Frozen HGB replay

The original ten-column feature vector was replayed first with the frozen StandardScaler and HGB. Its expected Points match the retained original values to max absolute error {summary['frozen_model_replay']['original_replay_max_abs_error']:.2g}; deterministic repeat passed. The offline sensitivity then changed only `{FEATURES[8]}` to the official reference and replayed the same frozen model. Settled-outcome metrics are retrospective; 480 full-population rows have no settled labels and are prediction-only.

| Settled metric | Original feature | Official reference feature |
|---|---:|---:|
| Mean expected Points | {performance['settled_outcome_metrics_before']['mean_expected_points']:.3f} | {performance['settled_outcome_metrics_after']['mean_expected_points']:.3f} |
| Mean count residual | {performance['settled_outcome_metrics_before']['mean_count_residual_realized_minus_expected']:+.3f} | {performance['settled_outcome_metrics_after']['mean_count_residual_realized_minus_expected']:+.3f} |
| Over 0.5 Brier | {performance['settled_outcome_metrics_before']['over_0_5_brier']:.4f} | {performance['settled_outcome_metrics_after']['over_0_5_brier']:.4f} |
| Over 0.5 log loss | {performance['settled_outcome_metrics_before']['over_0_5_log_loss']:.4f} | {performance['settled_outcome_metrics_after']['over_0_5_log_loss']:.4f} |
| High-confidence Under correct | {performance['settled_outcome_metrics_before']['high_confidence_under_correct']} | {performance['settled_outcome_metrics_after']['high_confidence_under_correct']} |
| High-confidence Under incorrect | {performance['settled_outcome_metrics_before']['high_confidence_under_incorrect']} | {performance['settled_outcome_metrics_after']['high_confidence_under_incorrect']} |

Market-opposed and same-side breakdowns are provided in the summary. It also includes a paired negative probability-edge reference: among 66 settled rows where the original model-selected side probability was below the corresponding retained market probability, Brier moved 0.2066 to 0.2046 and mean count residual +0.180 to +0.087. This is a probability comparison, not payout-adjusted expected value. All scores measure retrospective response, not prospective performance or compatibility.

## Historical compatibility and recommendation

The numeric ID disagreements in 2,194/8,043 comparable 2023 rows are not all treated as errors. However, three 2023 season/ID pairs map to multiple official physical teams, and 53 opening-week player-games have nonzero team-shot history despite no official prior games for their target physical teams. There are also 118 missing training team IDs. This confirms a material training feature identity defect. The 2025 mean is close (27.43 vs 27.60), but target-row exact agreement is only 65.77%; its source skater sums differ from direct boxscore team totals in 10.27% of source team-games. Neither period establishes compatibility with a clean official-history feature.

**The corrected operational statistic is not established as compatible with the frozen HGB.** The model was fitted on the defective 2023–24 representation. Rebuilding and validating historical features first is necessary before a corrected-history challenger can be trained or the frozen model can be judged against it. Prospective validation on later immutable captures with final official outcomes is required before any production promotion.

Recommended next action: rebuild and independently verify the 2023–24 team-shot history using physical team identity and official game totals, quantify which training rows change, then proceed to controlled challenger evaluation only after the historical contract is validated. This package made no production, Funnel, database, or immutable-prediction changes. Additional official-provider calls: 0; paid credits: 0.

## Package files

- `original_input_availability.json` — receipt-bound scorer inputs and limitations.
- `official_team_game_reference.csv` — retained official team-game source rows.
- `corrected_feature_rows.csv` / `row_level_discrepancies.csv` — supported reference with row-level history evidence.
- `frozen_hgb_replay_rows.csv` — unchanged original predictions and one-feature offline sensitivity.
- `summary.json` — metrics, hashes, compatibility, and classification.
- Reusable correction: `backend/nhl/points_offline_team_shots.py`.
"""
    (OUT / "report.md").write_text(report)
    print(json.dumps({"status": "COMPLETE", "output": str(OUT.relative_to(ROOT)),
                      "source_metrics": feature_metrics,
                      "original_inputs": input_inventory_meta,
                      "settled_replay": performance,
                      "changed_features": changed_columns}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
