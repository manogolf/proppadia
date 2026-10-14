"""Immutable prospective SOG D10/D20 divergence correction shadow."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

MODEL_IDENTITY = "NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1"
CONTRACT = "NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1"
THRESHOLD = 0.5
LINES = (1.5, 2.5, 3.5)
RATE_FIELDS = ("d10_sog_per60", "d20_sog_per60", "d5_sog_per60")
TOI_FIELDS = ("d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg")


def _num(frame: pd.DataFrame, field: str) -> pd.Series:
    if field not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    values = pd.to_numeric(frame[field], errors="coerce")
    return values.where(np.isfinite(values))


def _coalesce(*values: pd.Series) -> pd.Series:
    result = values[0].copy()
    for value in values[1:]:
        result = result.where(result.notna(), value)
    return result


def build_predictions(features: pd.DataFrame, production: pd.DataFrame, *,
                      slate_date: str, run_id: str, feature_cutoff_utc: str,
                      canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None = None) -> pd.DataFrame:
    """Build shadow rows from the exact production-scored player-game set."""
    keys = ["game_id", "player_id"]
    if features.duplicated(keys).any() or production.duplicated(keys).any():
        raise ValueError("SOG_DIVERGENCE_DUPLICATE_PLAYER_GAME")
    required = {"game_id", "player_id", "expected_sog"}
    if required - set(production.columns):
        raise ValueError("SOG_DIVERGENCE_PRODUCTION_SCHEMA_INVALID")
    prod_cols = keys + [c for c in ("expected_sog", "p_over_1_5", "p_over_2_5", "p_over_3_5", "poisson_source") if c in production]
    joined = production[prod_cols].merge(features, on=keys, how="left", validate="one_to_one", suffixes=("_production", ""), indicator=True)
    if not joined["_merge"].eq("both").all():
        raise ValueError("SOG_DIVERGENCE_PRODUCTION_ROW_MISSING_FROM_BOUND_FEATURES")
    if not set(pd.to_numeric(joined.game_id).astype(int)).issubset(set(map(int, canonical_game_ids))):
        raise ValueError("SOG_DIVERGENCE_NONCANONICAL_GAME")
    d10, d20, d5 = (_num(joined, field) for field in RATE_FIELDS)
    toi = _coalesce(_num(joined, "d10_toi_min_avg"), _num(joined, "d20_toi_min_avg"), _num(joined, "d5_toi_min_avg"),
                    _num(joined, "szn_toi_per_game_5on5") + _num(joined, "szn_toi_per_game_pp"),
                    _num(joined, "season_5on5_icetime_per_game") / 60 + _num(joined, "season_5on4_icetime_per_game") / 60)
    baseline_rate = _coalesce(d10, d20, d5)
    production_lambda = _num(joined, "expected_sog")
    expected_lambda = baseline_rate * toi / 60
    valid = production_lambda.notna() & expected_lambda.notna() & toi.notna()
    if not valid.all():
        raise ValueError("SOG_DIVERGENCE_PRODUCTION_EXPOSURE_UNAVAILABLE")
    if not production_lambda.sub(expected_lambda).abs().le(1e-10).all():
        raise ValueError("SOG_DIVERGENCE_PRODUCTION_LAMBDA_REPLAY_FAILED")
    divergence = d10 - d20
    d20_valid = d20.notna()
    correction = d10.notna() & d20_valid & divergence.abs().gt(THRESHOLD)
    # The production scorer selects D10 first; preserve its fallback hierarchy otherwise.
    d10_selected = d10.notna()
    correction &= d10_selected
    corrected_rate = baseline_rate.copy()
    corrected_rate.loc[correction] = ((d10.loc[correction] + d20.loc[correction]) / 2)
    reason = pd.Series("DIVERGENCE_WITHIN_THRESHOLD", index=joined.index, dtype=object)
    reason.loc[~d20_valid] = "D20_UNAVAILABLE_NO_CORRECTION"
    reason.loc[~d10_selected & d20_valid] = "PRODUCTION_FALLBACK_RATE_NO_CORRECTION"
    reason.loc[correction] = "MIDPOINT_CORRECTION_APPLIED"
    shadow_lambda = corrected_rate * toi / 60
    out = joined[[c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "season") if c in joined]].copy()
    out.insert(0, "slate_date", slate_date)
    out["parent_daily_run_id"] = run_id
    out["eligible_pregame_game_ids"] = json.dumps(sorted(set(pd.to_numeric(features.game_id).astype(int))))
    out["started_excluded_game_ids"] = json.dumps(sorted(set(map(int, started_excluded_game_ids or []))))
    out["feature_cutoff_utc"] = feature_cutoff_utc
    out["production_model_family"] = "poisson_baseline"
    out["production_model_version"] = "baseline_v1"
    out["production_d10_sog_per60"] = d10
    out["d20_sog_per60"] = d20
    out["divergence"] = divergence
    out["correction_threshold"] = THRESHOLD
    out["correction_applied"] = correction
    out["correction_reason"] = reason
    out["production_selected_toi_minutes"] = toi
    out["production_rate_per60"] = baseline_rate
    out["corrected_rate_per60"] = corrected_rate
    out["production_lambda"] = production_lambda
    out["shadow_lambda"] = shadow_lambda
    out["lambda_delta"] = shadow_lambda - production_lambda
    line_parts = []
    for line, threshold, pcol in zip(LINES, (2, 3, 4), ("p_over_1_5", "p_over_2_5", "p_over_3_5")):
        p_prod = _num(joined, pcol) if pcol in joined else production_lambda.map(lambda x: _poisson_tail(float(x), threshold))
        p_shadow = shadow_lambda.map(lambda x: _poisson_tail(float(x), threshold))
        out[f"production_{pcol}"] = p_prod
        out[f"shadow_{pcol}"] = p_shadow
        out[f"{pcol}_delta"] = p_shadow - p_prod
        part = out.copy()
        part["line"] = line
        part["production_p_over"] = p_prod
        part["shadow_p_over"] = p_shadow
        part["probability_delta"] = p_shadow - p_prod
        part["p_under"] = 1 - p_shadow
        part["selected_side"] = np.where(p_shadow.ge(.5), "OVER", "UNDER")
        line_parts.append(part)
    # Exact identity for rows on which no correction applies.
    inactive = ~correction
    if not shadow_lambda.loc[inactive].sub(production_lambda.loc[inactive]).abs().le(1e-12).all():
        raise ValueError("SOG_DIVERGENCE_INACTIVE_LAMBDA_IDENTITY_FAILED")
    for pcol in ("p_over_1_5", "p_over_2_5", "p_over_3_5"):
        if not out.loc[inactive, f"shadow_{pcol}"].sub(out.loc[inactive, f"production_{pcol}" ]).abs().le(1e-12).all():
            raise ValueError("SOG_DIVERGENCE_INACTIVE_PROBABILITY_IDENTITY_FAILED")
    if not ((out.shadow_p_over_1_5 >= out.shadow_p_over_2_5) & (out.shadow_p_over_2_5 >= out.shadow_p_over_3_5)).all():
        raise ValueError("SOG_DIVERGENCE_THRESHOLD_COHERENCE_FAILED")
    return pd.concat(line_parts, ignore_index=True)


def _write_sums(path: Path) -> None:
    (path / "SHA256SUMS").write_text("".join(
        f"{sha256_file(item)}  {item.name}\n" for item in sorted(path.iterdir())
        if item.is_file() and item.name != "SHA256SUMS"))


def capture(*, feature_path: Path, production_path: Path, output_root: Path,
            run_id: str, slate_date: str, season: int, feature_sha256: str,
            feature_cutoff_utc: str, canonical_game_ids: list[int],
            started_excluded_game_ids: list[int] | None, scorer_path: Path) -> dict[str, Any]:
    if sha256_file(feature_path) != feature_sha256:
        raise ValueError("SOG_DIVERGENCE_FEATURE_SHA_MISMATCH")
    feature = pd.read_csv(feature_path)
    production = pd.read_csv(production_path)
    kwargs = dict(slate_date=slate_date, run_id=run_id, feature_cutoff_utc=feature_cutoff_utc,
                  canonical_game_ids=canonical_game_ids, started_excluded_game_ids=started_excluded_game_ids)
    frame = build_predictions(feature, production, **kwargs)
    replay = build_predictions(feature, production, **kwargs)
    out_root = Path(output_root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
    final = out_root / f"model={MODEL_IDENTITY}"
    stage = out_root / f".{MODEL_IDENTITY}.incomplete"
    out_root.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"SOG_DIVERGENCE_CAPTURE_ALREADY_EXISTS:{final}")
    stage.mkdir(exist_ok=False)
    prediction_path = stage / "predictions.csv"
    frame.to_csv(prediction_path, index=False, float_format="%.17g")
    replay_path = stage / "replay_predictions.csv"
    replay.to_csv(replay_path, index=False, float_format="%.17g")
    if sha256_file(prediction_path) != sha256_file(replay_path):
        raise ValueError("SOG_DIVERGENCE_DETERMINISTIC_REPLAY_FAILED")
    replay_path.unlink()
    feature_game_ids = sorted(set(pd.to_numeric(feature.game_id).astype(int)))
    manifest = {
        "contract": CONTRACT, "model_identity": MODEL_IDENTITY, "research_role": "PROSPECTIVE_SHADOW",
        "season": season, "slate_date": slate_date, "parent_daily_run_id": run_id,
        "correction_threshold": THRESHOLD, "midpoint_weights": {"d10": 0.5, "d20": 0.5},
        "correction_contract": "if abs(d10-d20)>0.5 use midpoint; otherwise d10; missing D20 leaves production rate unchanged",
        "production_model_identity": {"family": "poisson_baseline", "version": "baseline_v1"},
        "production_scorer_path": str(Path(scorer_path).resolve()), "production_scorer_sha256": sha256_file(scorer_path),
        "production_feature_input_path": str(Path(feature_path).resolve()), "production_feature_input_sha256": feature_sha256,
        "production_prediction_path": str(Path(production_path).resolve()), "production_prediction_sha256": sha256_file(production_path),
        "canonical_game_ids": sorted(set(map(int, canonical_game_ids))),
        "canonical_game_set_hash": canonical_game_set_hash(canonical_game_ids),
        "eligible_game_ids": feature_game_ids,
        "started_excluded_game_ids": sorted(set(map(int, started_excluded_game_ids or []))),
        "feature_cutoff_utc": feature_cutoff_utc, "prediction_row_count": len(frame),
        "player_game_count": len(frame) // 3, "line_count": 3,
        "correction_applied_player_game_count": int(frame.drop_duplicates(["game_id", "player_id"]).correction_applied.sum()),
        "unchanged_player_game_count": int((~frame.drop_duplicates(["game_id", "player_id"]).correction_applied).sum()),
        "d20_unavailable_count": int(frame.drop_duplicates(["game_id", "player_id"]).d20_sog_per60.isna().sum()),
        "threshold_coherence_crossing_count": 0, "deterministic_replay": "PASS",
        "prediction_sha256": sha256_file(prediction_path), "status": "COMPLETE",
    }
    (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    # Validate package contents before publishing its completion marker.
    readback = pd.read_csv(prediction_path)
    readback_keys = readback[["game_id", "player_id", "line"]].apply(pd.to_numeric, errors="coerce")
    frame_keys = frame[["game_id", "player_id", "line"]].apply(pd.to_numeric, errors="coerce")
    if (len(readback) != len(frame)
            or not np.array_equal(readback_keys.to_numpy(), frame_keys.to_numpy())
            or not np.allclose(readback.shadow_lambda, frame.shadow_lambda, rtol=0, atol=1e-15)
            or not np.allclose(readback.shadow_p_over, frame.shadow_p_over, rtol=0, atol=1e-15)):
        raise ValueError("SOG_DIVERGENCE_CAPTURE_READBACK_FAILED")
    (stage / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE", "parent_daily_run_id": run_id}) + "\n")
    _write_sums(stage)
    stage.rename(final)
    verify_package(final)
    manifest["path"] = str(final.resolve())
    manifest["manifest_sha256"] = sha256_file(final / "manifest.json")
    return manifest


def _probability_metrics(y: pd.Series, p: pd.Series) -> dict[str, Any]:
    eps = 1e-15
    p = p.clip(eps, 1-eps)
    return {"brier": float(((p-y)**2).mean()),
            "log_loss": float((-(y*p.map(math.log)+(1-y)*(1-p).map(math.log))).mean())}


def grade_slate(*, slate_date: str, reconciliation_package: Path,
                capture_root: Path) -> dict[str, Any]:
    """Grade retained immutable captures against finalized canonical outcomes."""
    outcome_path = Path(reconciliation_package) / "canonical_skater_outcomes.csv"
    if not outcome_path.is_file():
        return {"status": "NO_FINAL_OUTCOMES"}
    outcomes = pd.read_csv(outcome_path)
    outcomes = outcomes[outcomes.slate_date.astype(str).eq(slate_date)]
    if "participation_state" in outcomes:
        outcomes = outcomes[outcomes.participation_state.eq("PARTICIPATED")]
    if "official_final" in outcomes:
        outcomes = outcomes[outcomes.official_final.astype(str).str.lower().eq("true")]
    grades: list[dict[str, Any]] = []
    for package in sorted(Path(capture_root).glob(f"season=*/slate_date={slate_date}/run_id=*/model={MODEL_IDENTITY}")):
        verify_package(package)
        manifest = json.loads((package / "manifest.json").read_text())
        pred = pd.read_csv(package / "predictions.csv")
        value = "shots_on_goal" if "shots_on_goal" in outcomes else "official_sog"
        joined = pred.merge(outcomes[["game_id", "player_id", value]], on=["game_id", "player_id"], how="left", validate="many_to_one")
        y_count = pd.to_numeric(joined[value], errors="coerce")
        settled = y_count.notna() & y_count.ge(0)
        rows = joined[settled].copy()
        y_count = y_count[settled]
        lam = pd.to_numeric(rows.shadow_lambda)
        player_rows = rows.drop_duplicates(["game_id", "player_id"])
        item: dict[str, Any] = {"model_identity": MODEL_IDENTITY, "research_role": "PROSPECTIVE_SHADOW",
            "slate_date": slate_date, "run_id": manifest["parent_daily_run_id"],
            "prediction_sha256": manifest["prediction_sha256"], "outcome_sha256": sha256_file(outcome_path),
            "settled_player_games": int(rows[["game_id", "player_id"]].drop_duplicates().shape[0]), "settled_line_rows": len(rows),
            "unsettled_line_rows": int((~settled).sum()), "mean_lambda": float(lam.mean()) if len(rows) else None,
            "mean_residual": float((lam-y_count).mean()) if len(rows) else None,
            "count_mae": float((lam-y_count).abs().mean()) if len(rows) else None,
            "count_rmse": float(((lam-y_count).pow(2).mean())**0.5) if len(rows) else None,
            "divergence_diagnostics": {"corrected_rows": int(player_rows.correction_applied.sum()),
                "unchanged_rows": int((~player_rows.correction_applied).sum()),
                "material_d10_gt_d20": int((player_rows.divergence > THRESHOLD).sum()),
                "material_d10_lt_d20": int((player_rows.divergence < -THRESHOLD).sum()),
                "d20_unavailable": int(player_rows.d20_sog_per60.isna().sum())}}
        prod_count = pd.to_numeric(rows.production_lambda)
        item["paired_count_mae_difference_vs_production"] = float((lam-y_count).abs().mean()-(prod_count-y_count).abs().mean()) if len(rows) else None
        by_line = {}
        for line in LINES:
            part = rows[rows.line.eq(line)]
            y = (pd.to_numeric(part[value]) > line).astype(float)
            ps = pd.to_numeric(part.shadow_p_over)
            pp = pd.to_numeric(part.production_p_over)
            sl = _probability_metrics(y, ps) if len(part) else {"brier": None, "log_loss": None}
            pl = _probability_metrics(y, pp) if len(part) else {"brier": None, "log_loss": None}
            from sklearn.metrics import accuracy_score, average_precision_score, roc_auc_score
            sl.update({"n": len(part), "accuracy": float(accuracy_score(y, ps.ge(.5))) if len(part) else None,
                "predicted_over_frequency": float(ps.mean()) if len(part) else None,
                "realized_over_frequency": float(y.mean()) if len(part) else None,
                "calibration_bias": float(ps.mean()-y.mean()) if len(part) else None,
                "auc": float(roc_auc_score(y, ps)) if y.nunique()==2 else None,
                "average_precision": float(average_precision_score(y, ps)) if y.nunique()==2 else None,
                "paired_brier_difference_vs_production": sl["brier"]-pl["brier"] if len(part) else None,
                "paired_log_loss_difference_vs_production": sl["log_loss"]-pl["log_loss"] if len(part) else None,
                "side_disagreements": int(ps.ge(.5).ne(pp.ge(.5)).sum()),
                "wins_gained": int(((ps.ge(.5)==y.eq(1)) & (pp.ge(.5)!=y.eq(1))).sum()),
                "wins_lost": int(((ps.ge(.5)!=y.eq(1)) & (pp.ge(.5)==y.eq(1))).sum())})
            by_line[str(line)] = sl
        item["by_line"] = by_line
        delta = rows[["divergence", "shadow_lambda", value]].dropna()
        item["residual_slope_vs_divergence"] = (float(np.polyfit(delta.divergence, delta.shadow_lambda-delta[value], 1)[0]) if len(delta)>=3 and delta.divergence.nunique()>1 else None)
        grades.append(item)
    return {"status": "COMPLETE" if grades else "NO_IMMUTABLE_SHADOW_CAPTURE", "role": "PROSPECTIVE_SHADOW", "grades": grades}
