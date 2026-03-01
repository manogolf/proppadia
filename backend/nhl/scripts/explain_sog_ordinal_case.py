#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import date, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.nhl.scripts.calibrate_sog_segmented_recency import (
    _apply_calibration,
    _fit_models,
    _latest_truth_date,
    _load_slate_bucket_map,
    _load_training,
    _parse_lines,
)
from backend.nhl.scripts.score_sog_denali_pairings_ordinal_lgbm import (
    _alias_sog_count_cols,
    _ensure_required_feature_defaults,
    _fill_missing_pairings_cov_cols,
    load_features_list,
    prep_X,
)

DEFAULT_MODEL_ROOT = (
    "backend/nhl/models/latest/shots_on_goal/"
    "sog_player_denali_pairings_ordinal_v1__no_shiftcounts"
)
DEFAULT_FEATURE_META = (
    "backend/nhl/models/latest/shots_on_goal/"
    "sog_player_denali_pairings_ordinal_v1__no_shiftcounts/ge_2/metadata.json"
)
DEFAULT_PRED_CSV = "backend/nhl/data/processed/sog_predictions_wide_calibrated.csv"
DEFAULT_CALIBRATION_MODEL_FAMILY = "denali_blend"
DEFAULT_CALIBRATION_MODEL_VERSION = "phoenix_v2"
DEFAULT_CALIBRATION_LINES = "1.5,2.5,3.5"
DEFAULT_CALIBRATION_LOOKBACK_DAYS = 120
DEFAULT_CALIBRATION_SEGMENT_MIN_ROWS = 120
DEFAULT_CALIBRATION_BLEND_ALPHA = 0.65
DEFAULT_CALIBRATION_DECAY_HALF_LIFE_DAYS = 21.0

THRESHOLDS = (
    ("ge_2", "p_over_1_5"),
    ("ge_3", "p_over_2_5"),
    ("ge_4", "p_over_3_5"),
)

FOCUS_FEATURES = (
    "d5_sog_per60",
    "d10_sog_per60",
    "d20_sog_per60",
    "attempts_d10_per60",
    "rest_days",
    "b2b_flag",
    "pace_index",
    "pace_matchup_index",
    "team_d10_sf_per_game",
    "opp_d10_sf_allowed_per_game",
    "opp_d10_sf_per60",
    "team_d10_sa_per60",
    "opp_d10_sa_per60",
    "role_pp_share",
    "szn_toi_per_game_5on5",
    "szn_toi_per_game_pp",
    "season_5on5_icetime_per_game",
    "season_5on4_icetime_per_game",
    "d10_pairings_available",
    "d20_pairings_available",
    "d10_top_mate_overlap_share_avg",
    "d20_top_mate_repeat_rate",
)


def _fmt_num(value: object, digits: int = 4) -> str:
    try:
        num = float(value)
    except Exception:
        return str(value)
    if math.isnan(num) or math.isinf(num):
        return str(value)
    return f"{num:.{digits}f}"


def _fmt_prob(value: object) -> str:
    try:
        num = float(value)
    except Exception:
        return str(value)
    return f"{num:.4f} ({num * 100:.1f}%)"


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-float(x)))


def _load_metadata(path: Path) -> dict:
    return json.loads(path.read_text())


def _select_feature_row(df: pd.DataFrame, player_id: int, game_id: int | None) -> int:
    matches = df.index[df["player_id"].astype(str) == str(player_id)].tolist()
    if game_id is not None:
        matches = [idx for idx in matches if str(df.at[idx, "game_id"]) == str(game_id)]
    if not matches:
        raise SystemExit(
            f"No feature row found for player_id={player_id}"
            + (f" game_id={game_id}" if game_id is not None else "")
        )
    if len(matches) > 1:
        raise SystemExit(
            f"Multiple feature rows found for player_id={player_id}"
            + (f" game_id={game_id}" if game_id is not None else "")
            + "; pass --game-id to disambiguate."
        )
    return matches[0]


def _lookup_existing_prediction(pred_csv: Path, player_id: int, game_id: int | None) -> dict[str, str] | None:
    if not pred_csv.exists():
        return None
    with pred_csv.open() as fh:
        rows = list(csv.DictReader(fh))
    matches = [r for r in rows if str(r.get("player_id")) == str(player_id)]
    if game_id is not None:
        matches = [r for r in matches if str(r.get("game_id")) == str(game_id)]
    if not matches:
        return None
    return matches[0]


def _ordered_feature_snapshot(raw_row: pd.Series) -> list[tuple[str, object]]:
    out = []
    for key in FOCUS_FEATURES:
        if key in raw_row.index:
            out.append((key, raw_row[key]))
    return out


def _contrib_table(model, x_row: pd.DataFrame, feats: list[str], top_n: int) -> dict[str, object]:
    raw_score = float(model.predict(x_row, raw_score=True)[0])
    probability = float(model.predict(x_row)[0])
    contrib = np.asarray(model.predict(x_row, pred_contrib=True))[0]
    feature_contribs = contrib[:-1]
    base_value = float(contrib[-1])

    rows = []
    for feature, value, shap in zip(feats, x_row.iloc[0].tolist(), feature_contribs):
        rows.append(
            {
                "feature": feature,
                "value": value,
                "contribution": float(shap),
                "abs_contribution": abs(float(shap)),
            }
        )

    rows_sorted = sorted(rows, key=lambda r: r["abs_contribution"], reverse=True)
    positive = [r for r in rows_sorted if r["contribution"] > 0][:top_n]
    negative = [r for r in rows_sorted if r["contribution"] < 0][:top_n]

    return {
        "raw_score": raw_score,
        "probability": probability,
        "base_value": base_value,
        "base_probability": _sigmoid(base_value),
        "positive": positive,
        "negative": negative,
    }


def _maybe_calibrated_probs(raw_row: pd.Series, fresh_probs: dict[str, float], args: argparse.Namespace) -> tuple[dict[str, float] | None, str | None]:
    try:
        lines = _parse_lines(args.calibration_lines)
        slate_date = date.fromisoformat(str(raw_row["game_date"]))
        if args.asof_date.strip():
            asof = date.fromisoformat(args.asof_date.strip())
        else:
            latest_truth = _latest_truth_date()
            asof = min(latest_truth, slate_date - timedelta(days=1))
        train_from = asof - timedelta(days=max(1, int(args.calibration_lookback_days)) - 1)

        train_df = _load_training(
            model_family=args.calibration_model_family,
            model_version=args.calibration_model_version,
            lines=lines,
            train_from=train_from,
            train_to=asof,
        )
        if train_df.empty:
            return None, "calibration training window returned no rows"

        pred_df = pd.DataFrame(
            [
                {
                    "player_id": int(raw_row["player_id"]),
                    "game_id": int(raw_row["game_id"]),
                    "game_date": slate_date,
                    **fresh_probs,
                }
            ]
        )
        bucket_map = _load_slate_bucket_map(slate_date)
        if bucket_map.empty:
            pred_df["expected_sog_bucket"] = "missing"
        else:
            pred_df = pred_df.merge(bucket_map, on=["player_id", "game_id"], how="left")
            pred_df["expected_sog_bucket"] = pred_df["expected_sog_bucket"].fillna("missing").astype(str)

        fit_pack = _fit_models(
            train_df=train_df,
            lines=lines,
            asof=asof,
            segment_min_rows=max(1, int(args.calibration_segment_min_rows)),
            half_life_days=float(args.calibration_decay_half_life_days),
        )
        _apply_calibration(
            pred_df=pred_df,
            fit=fit_pack,
            lines=lines,
            blend_alpha=float(args.calibration_blend_alpha),
        )
        calibrated = {
            "p_over_1_5": float(pred_df.iloc[0]["p_over_1_5"]),
            "p_over_2_5": float(pred_df.iloc[0]["p_over_2_5"]),
            "p_over_3_5": float(pred_df.iloc[0]["p_over_3_5"]),
        }
        return calibrated, None
    except Exception as exc:
        return None, str(exc)


def main() -> None:
    ap = argparse.ArgumentParser(description="Explain an NHL SOG ordinal case (ge_2 / ge_3 / ge_4).")
    ap.add_argument("--features-csv", required=True, help="Path to sog_features_*.csv")
    ap.add_argument("--player-id", required=True, type=int, help="Target NHL player_id")
    ap.add_argument("--game-id", type=int, default=None, help="Optional game_id to disambiguate")
    ap.add_argument("--model-root", default=DEFAULT_MODEL_ROOT)
    ap.add_argument("--feature-meta", default=DEFAULT_FEATURE_META)
    ap.add_argument("--pred-csv", default=DEFAULT_PRED_CSV)
    ap.add_argument("--top-n", type=int, default=10, help="Top positive/negative feature contributions to print")
    ap.add_argument("--calibration-model-family", default=DEFAULT_CALIBRATION_MODEL_FAMILY)
    ap.add_argument("--calibration-model-version", default=DEFAULT_CALIBRATION_MODEL_VERSION)
    ap.add_argument("--calibration-lines", default=DEFAULT_CALIBRATION_LINES)
    ap.add_argument("--calibration-lookback-days", type=int, default=DEFAULT_CALIBRATION_LOOKBACK_DAYS)
    ap.add_argument("--calibration-segment-min-rows", type=int, default=DEFAULT_CALIBRATION_SEGMENT_MIN_ROWS)
    ap.add_argument("--calibration-blend-alpha", type=float, default=DEFAULT_CALIBRATION_BLEND_ALPHA)
    ap.add_argument("--calibration-decay-half-life-days", type=float, default=DEFAULT_CALIBRATION_DECAY_HALF_LIFE_DAYS)
    ap.add_argument("--asof-date", default="", help="Optional calibration as-of date (YYYY-MM-DD)")
    args = ap.parse_args()

    feature_csv = Path(args.features_csv)
    model_root = Path(args.model_root)
    feature_meta = Path(args.feature_meta)
    pred_csv = Path(args.pred_csv)

    df_raw = pd.read_csv(feature_csv)
    row_idx = _select_feature_row(df_raw, args.player_id, args.game_id)
    raw_row = df_raw.iloc[row_idx]

    df = df_raw.copy()
    df = _fill_missing_pairings_cov_cols(df)
    df = _alias_sog_count_cols(df)
    feats = load_features_list(str(feature_meta))
    df = _ensure_required_feature_defaults(df, feats, in_path=feature_csv)
    X = prep_X(df, feats)
    x_row = X.iloc[[row_idx]]

    metadata = {name: _load_metadata(model_root / name / "metadata.json") for name, _ in THRESHOLDS}
    models = {name: joblib.load(model_root / name / "lgbm.joblib") for name, _ in THRESHOLDS}

    print(f"[case] player_id={args.player_id} game_id={raw_row.get('game_id')}")
    print(f"[case] features_csv={feature_csv}")
    print(f"[case] model_root={model_root}")
    print()

    print("[feature snapshot]")
    for key, value in _ordered_feature_snapshot(raw_row):
        print(f"  {key}: {_fmt_num(value)}")
    print()

    existing = _lookup_existing_prediction(pred_csv, args.player_id, args.game_id)
    if existing:
        print(f"[existing predictions] source={pred_csv}")
        for _, pred_col in THRESHOLDS:
            if pred_col in existing:
                print(f"  {pred_col}: {_fmt_prob(existing[pred_col])}")
        print()

    fresh_probs = {}
    for name, pred_col in THRESHOLDS:
        prob = float(models[name].predict(x_row)[0])
        fresh_probs[pred_col] = prob

    print("[fresh threshold probabilities]")
    for name, pred_col in THRESHOLDS:
        meta = metadata[name]
        print(
            f"  {name} ({meta.get('definition')} | base_rate_valid={_fmt_num(meta.get('base_rate_valid'))}): "
            f"{pred_col}={_fmt_prob(fresh_probs[pred_col])}"
        )
    print()

    calibrated_probs, calibration_error = _maybe_calibrated_probs(raw_row, fresh_probs, args)
    if calibrated_probs:
        print("[calibrated threshold probabilities]")
        for _, pred_col in THRESHOLDS:
            print(f"  {pred_col}={_fmt_prob(calibrated_probs[pred_col])}")
        print()
    else:
        print(f"[calibration unavailable] {calibration_error}")
        print()

    if existing:
        diffs = []
        compare_probs = calibrated_probs or fresh_probs
        for _, pred_col in THRESHOLDS:
            try:
                existing_prob = float(existing.get(pred_col, "nan"))
            except Exception:
                continue
            delta = compare_probs[pred_col] - existing_prob
            if abs(delta) > 1e-6:
                diffs.append((pred_col, existing_prob, compare_probs[pred_col], delta))
        if diffs:
            compare_label = "calibrated output" if calibrated_probs else "fresh scorer output"
            print(f"[warning] existing wide CSV differs from {compare_label}")
            for pred_col, old, new, delta in diffs:
                print(
                    f"  {pred_col}: existing={_fmt_prob(old)} compare={_fmt_prob(new)} delta={delta:+.4f}"
                )
            print()

    ge4 = _contrib_table(models["ge_4"], x_row, feats, args.top_n)
    print("[ge_4 explanation]")
    print(f"  raw_score: {_fmt_num(ge4['raw_score'])}")
    print(f"  probability: {_fmt_prob(ge4['probability'])}")
    print(f"  base_value (logit): {_fmt_num(ge4['base_value'])}")
    print(f"  base_probability: {_fmt_prob(ge4['base_probability'])}")
    print()

    print("[ge_4 top positive contributors]")
    for item in ge4["positive"]:
        print(
            f"  + {item['feature']}: contrib={_fmt_num(item['contribution'])} value={_fmt_num(item['value'])}"
        )
    print()

    print("[ge_4 top negative contributors]")
    for item in ge4["negative"]:
        print(
            f"  - {item['feature']}: contrib={_fmt_num(item['contribution'])} value={_fmt_num(item['value'])}"
        )


if __name__ == "__main__":
    main()
