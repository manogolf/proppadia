#!/usr/bin/env python3
"""Score NHL SOG wide probabilities from a strengthened Poisson base (base_v2 shadow)."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.scripts.score_sog_poisson_baseline import (
    _bucket_series,
    _coalesce,
    _poisson_tail,
    _to_numeric,
)
from backend.shared.db.pg import pg_fetchall


DEFAULT_OUT = "backend/nhl/data/processed/sog_predictions_wide_base_v2_shadow.csv"

HISTORY_SQL = """
SELECT
  f.game_date::date AS game_date,
  f.season::int AS season,
  f.player_id::bigint AS player_id,
  f.game_id::bigint AS game_id,
  f.team_id::bigint AS team_id,
  f.opponent_id::bigint AS opponent_id,
  f.is_home,
  s.shots_on_goal::int AS shots_on_goal,
  f.d5_sog_per60::float8 AS d5_sog_per60,
  f.d10_sog_per60::float8 AS d10_sog_per60,
  f.d20_sog_per60::float8 AS d20_sog_per60,
  f.team_d10_sf_per_game::float8 AS team_d10_sf_per_game,
  f.opp_d10_sf_allowed_per_game::float8 AS opp_d10_sf_allowed_per_game,
  f.pace_matchup_index::float8 AS pace_matchup_index,
  f.role_pp_share::float8 AS role_pp_share,
  f.rest_days::float8 AS rest_days,
  CASE WHEN f.b2b_flag THEN 1.0 ELSE 0.0 END::float8 AS b2b_flag,
  f.last10_team_sog_share::float8 AS last10_team_sog_share,
  f.team_num_sog_last10::float8 AS team_num_sog_last10,
  f.team_num_event_last10::float8 AS team_num_event_last10,
  f.d5_toi_min_avg::float8 AS d5_toi_min_avg,
  f.d10_toi_min_avg::float8 AS d10_toi_min_avg,
  f.d20_toi_min_avg::float8 AS d20_toi_min_avg,
  f.szn_toi_per_game_5on5::float8 AS szn_toi_per_game_5on5,
  f.szn_toi_per_game_pp::float8 AS szn_toi_per_game_pp,
  f.season_5on5_icetime_per_game::float8 AS season_5on5_icetime_per_game,
  f.season_5on4_icetime_per_game::float8 AS season_5on4_icetime_per_game
FROM nhl.training_features_nhl_sog_enriched_pregame_v2 f
JOIN nhl.skater_game_logs_raw s
  ON s.game_id = f.game_id
 AND s.player_id = f.player_id
WHERE f.season = %s
  AND f.game_date < %s::date
  AND (%s::date IS NULL OR f.game_date >= %s::date)
  AND s.shots_on_goal IS NOT NULL
ORDER BY f.game_date, f.game_id, f.player_id
"""

FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "team_environment": (
        "team_d10_sf_per_game",
        "last10_team_sog_share",
        "team_sog_rate_last10",
    ),
    "opponent_suppression": (
        "opp_d10_sf_allowed_per_game",
    ),
    "game_state": (
        "pace_matchup_index",
        "is_home",
        "rest_days",
        "b2b_flag",
    ),
    "role_usage": (
        "role_pp_share",
    ),
}


@dataclass
class ScaleStat:
    median: float
    scale: float


@dataclass
class RidgeModel:
    features: list[str]
    intercept: float
    coefs: dict[str, float]
    scales: dict[str, ScaleStat]


def _infer_season(slate_date: str) -> int:
    y, m, _ = (int(x) for x in str(slate_date).split("-"))
    return y if m >= 9 else (y - 1)


def _infer_single_slate_date(df: pd.DataFrame, explicit: str) -> str:
    if str(explicit).strip():
        return str(explicit).strip()
    dates = sorted({str(x) for x in df.get("game_date", pd.Series(dtype=str)).dropna().astype(str).tolist()})
    if len(dates) == 1:
        return dates[0]
    raise SystemExit("[base_v2 scorer] --slate-date is required when input has multiple/no game_date values")


def _prep_common(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rate = _coalesce(
        _to_numeric(out, "d10_sog_per60"),
        _to_numeric(out, "d20_sog_per60"),
        _to_numeric(out, "d5_sog_per60"),
    )
    toi = _coalesce(
        _to_numeric(out, "d10_toi_min_avg"),
        _to_numeric(out, "d20_toi_min_avg"),
        _to_numeric(out, "d5_toi_min_avg"),
        _to_numeric(out, "szn_toi_per_game_5on5") + _to_numeric(out, "szn_toi_per_game_pp"),
        (_to_numeric(out, "season_5on5_icetime_per_game") / 60.0)
        + (_to_numeric(out, "season_5on4_icetime_per_game") / 60.0),
    )
    out["rate_base"] = pd.to_numeric(rate, errors="coerce").clip(lower=0.0)
    out["toi_base"] = pd.to_numeric(toi, errors="coerce").clip(lower=0.0)
    out["lambda_base"] = ((out["rate_base"] * out["toi_base"]) / 60.0).where(
        out["rate_base"].notna() | out["toi_base"].notna(),
        0.0,
    ).clip(lower=0.0)

    team_sog_last10 = _to_numeric(out, "team_num_sog_last10")
    team_evt_last10 = _to_numeric(out, "team_num_event_last10")
    out["team_sog_rate_last10"] = (team_sog_last10 / team_evt_last10).where(team_evt_last10 > 0, np.nan)
    return out


def _load_history(*, season: int, cutoff_date: str, from_date: str) -> pd.DataFrame:
    rows = pg_fetchall(
        HISTORY_SQL,
        (
            int(season),
            str(cutoff_date),
            (str(from_date).strip() or None),
            (str(from_date).strip() or None),
        ),
    )
    hist = pd.DataFrame(rows or [])
    if hist.empty:
        return hist
    return _prep_common(hist)


def _robust_scale_stats(s: pd.Series) -> ScaleStat:
    v = pd.to_numeric(s, errors="coerce").astype(float)
    med = float(v.median()) if v.notna().any() else 0.0
    q25 = float(v.quantile(0.25)) if v.notna().any() else med
    q75 = float(v.quantile(0.75)) if v.notna().any() else med
    iqr = q75 - q25
    if not math.isfinite(iqr) or iqr < 1e-6:
        mad = float((v - med).abs().median()) if v.notna().any() else 0.0
        scale = max(1.4826 * mad, 1.0)
    else:
        scale = iqr
    return ScaleStat(median=med, scale=float(scale))


def _scaled_feature(df: pd.DataFrame, feature: str, stat: ScaleStat) -> tuple[pd.Series, pd.Series]:
    raw = _to_numeric(df, feature).astype(float)
    missing = raw.isna()
    x = ((raw.fillna(stat.median) - stat.median) / max(stat.scale, 1e-6)).clip(lower=-6.0, upper=6.0)
    return x.astype(float), missing


def _fit_model(
    hist: pd.DataFrame,
    *,
    ridge_alpha: float,
    half_life_days: float,
    cutoff_date: str,
) -> RidgeModel:
    feat_list = [f for group in FEATURE_GROUPS.values() for f in group]
    work = hist.copy()
    work["shots_on_goal"] = pd.to_numeric(work["shots_on_goal"], errors="coerce")
    work = work[(work["shots_on_goal"].notna()) & (work["lambda_base"].notna())].copy()
    work["lambda_base"] = pd.to_numeric(work["lambda_base"], errors="coerce").clip(lower=0.0)
    if work.empty:
        raise RuntimeError("no training rows after null filtering")

    scales = {f: _robust_scale_stats(work[f]) for f in feat_list}
    x_cols: list[np.ndarray] = []
    for f in feat_list:
        x, _ = _scaled_feature(work, f, scales[f])
        x_cols.append(x.to_numpy(dtype=float))

    X = np.column_stack([np.ones(len(work), dtype=float)] + x_cols)
    y = np.log((work["shots_on_goal"].to_numpy(dtype=float) + 0.5) / (work["lambda_base"].to_numpy(dtype=float) + 0.5))

    if float(half_life_days) > 0:
        asof = datetime.strptime(str(cutoff_date), "%Y-%m-%d").date()
        gd = pd.to_datetime(work["game_date"], errors="coerce").dt.date
        ages = np.array([(asof - d).days if pd.notna(d) else 0 for d in gd], dtype=float)
        ages = np.clip(ages, a_min=0.0, a_max=None)
        w = np.exp(-math.log(2.0) * ages / float(half_life_days))
    else:
        w = np.ones(len(work), dtype=float)

    sqrt_w = np.sqrt(np.clip(w, a_min=1e-9, a_max=None))
    Xw = X * sqrt_w[:, None]
    yw = y * sqrt_w

    xtx = Xw.T @ Xw
    pen = np.eye(xtx.shape[0], dtype=float) * float(max(ridge_alpha, 0.0))
    pen[0, 0] = 0.0
    xty = Xw.T @ yw
    beta = np.linalg.solve(xtx + pen, xty)

    return RidgeModel(
        features=feat_list,
        intercept=float(beta[0]),
        coefs={f: float(beta[i + 1]) for i, f in enumerate(feat_list)},
        scales=scales,
    )


def _apply_model(
    df: pd.DataFrame,
    model: RidgeModel,
    *,
    min_multiplier: float,
    max_multiplier: float,
    min_coverage_weight: float,
) -> pd.DataFrame:
    out = df.copy()

    feature_contrib: dict[str, pd.Series] = {}
    missing_mask: dict[str, pd.Series] = {}
    for f in model.features:
        x, miss = _scaled_feature(out, f, model.scales[f])
        feature_contrib[f] = x * float(model.coefs[f])
        missing_mask[f] = miss
        out[f"x_{f}"] = x

    group_logs: dict[str, pd.Series] = {}
    for g, feats in FEATURE_GROUPS.items():
        acc = pd.Series(0.0, index=out.index, dtype=float)
        for f in feats:
            acc = acc + feature_contrib[f]
        group_logs[g] = acc

    n_features = max(1, len(model.features))
    n_present = pd.Series(0.0, index=out.index, dtype=float)
    for f in model.features:
        n_present = n_present + (~missing_mask[f]).astype(float)
    coverage = (n_present / float(n_features)).clip(lower=0.0, upper=1.0)
    shrink = float(min_coverage_weight) + (1.0 - float(min_coverage_weight)) * coverage

    log_adj_raw = pd.Series(float(model.intercept), index=out.index, dtype=float)
    for f in model.features:
        log_adj_raw = log_adj_raw + feature_contrib[f]
    log_adj = log_adj_raw * shrink

    out["base_v2_coverage"] = coverage
    out["base_v2_log_adj_raw"] = log_adj_raw
    out["base_v2_log_adj"] = log_adj
    out["base_v2_multiplier_raw"] = np.exp(log_adj)
    out["base_v2_multiplier"] = out["base_v2_multiplier_raw"].clip(
        lower=float(min_multiplier),
        upper=float(max_multiplier),
    )

    out["base_v2_log_team_environment"] = group_logs["team_environment"] * shrink
    out["base_v2_log_opponent_suppression"] = group_logs["opponent_suppression"] * shrink
    out["base_v2_log_game_state"] = group_logs["game_state"] * shrink
    out["base_v2_log_role_usage"] = group_logs["role_usage"] * shrink

    out["m_team_environment"] = np.exp(out["base_v2_log_team_environment"]).clip(lower=0.7, upper=1.4)
    out["m_opponent_suppression"] = np.exp(out["base_v2_log_opponent_suppression"]).clip(lower=0.7, upper=1.4)
    out["m_game_state"] = np.exp(out["base_v2_log_game_state"]).clip(lower=0.7, upper=1.4)
    out["m_role_usage"] = np.exp(out["base_v2_log_role_usage"]).clip(lower=0.7, upper=1.4)

    out["expected_sog_base"] = pd.to_numeric(out["lambda_base"], errors="coerce").clip(lower=0.0)
    out["expected_sog_v2"] = (out["expected_sog_base"] * out["base_v2_multiplier"]).clip(lower=0.0)
    return out


def _score_probs(df: pd.DataFrame, lambda_col: str, prefix: str) -> None:
    out = pd.to_numeric(df[lambda_col], errors="coerce").clip(lower=0.0)
    for line, threshold in ((1.5, 2), (2.5, 3), (3.5, 4)):
        tag = str(line).replace(".", "_")
        df[f"p_{prefix}_over_{tag}"] = out.apply(lambda v: _poisson_tail(float(v), threshold)).astype(float)


def _combined_metrics(df: pd.DataFrame, prefix: str) -> dict[str, Any]:
    rows: list[dict[str, float]] = []
    for line, threshold in ((1.5, 2), (2.5, 3), (3.5, 4)):
        tag = str(line).replace(".", "_")
        p = pd.to_numeric(df.get(f"p_{prefix}_over_{tag}"), errors="coerce")
        y = (pd.to_numeric(df.get("shots_on_goal"), errors="coerce") >= threshold).astype(float)
        m = p.notna() & y.notna()
        if not m.any():
            continue
        pp = p[m].astype(float)
        yy = y[m].astype(float)
        rows.append(
            {
                "line": line,
                "n": int(len(pp)),
                "avg_p": float(pp.mean()),
                "hit_rate": float(yy.mean()),
                "brier": float(((pp - yy) ** 2).mean()),
            }
        )
    if not rows:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None, "by_line": []}
    n_total = int(sum(r["n"] for r in rows))
    avg_p = float(sum(r["avg_p"] * r["n"] for r in rows) / n_total)
    hit = float(sum(r["hit_rate"] * r["n"] for r in rows) / n_total)
    brier = float(sum(r["brier"] * r["n"] for r in rows) / n_total)
    return {
        "n": n_total,
        "avg_p": round(avg_p, 4),
        "hit_rate": round(hit, 4),
        "gap": round(avg_p - hit, 4),
        "brier": round(brier, 4),
        "by_line": [
            {
                "line": r["line"],
                "n": int(r["n"]),
                "avg_p": round(float(r["avg_p"]), 4),
                "hit_rate": round(float(r["hit_rate"]), 4),
                "gap": round(float(r["avg_p"] - r["hit_rate"]), 4),
                "brier": round(float(r["brier"]), 4),
            }
            for r in rows
        ],
    }


def _split_holdout(hist: pd.DataFrame, holdout_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted({str(x) for x in hist["game_date"].dropna().astype(str).tolist()})
    if len(dates) <= int(holdout_days):
        raise RuntimeError(f"need > {holdout_days} history dates; found {len(dates)}")
    holdout = set(dates[-int(holdout_days):])
    train = hist[~hist["game_date"].astype(str).isin(holdout)].copy()
    test = hist[hist["game_date"].astype(str).isin(holdout)].copy()
    return train, test


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Score NHL SOG probabilities using a strengthened Poisson base_v2 shadow model.")
    ap.add_argument("--in", dest="in_path", required=True, help="Input feature CSV (single slate).")
    ap.add_argument("--out", dest="out_path", default=DEFAULT_OUT)
    ap.add_argument("--slate-date", default="", help="YYYY-MM-DD; default infer from input game_date.")
    ap.add_argument("--season", type=int, default=0, help="Default infer from slate date.")
    ap.add_argument("--history-from-date", default="", help="Optional inclusive history lower bound.")
    ap.add_argument("--history-to-date", default="", help="Optional exclusive history upper bound; default slate date.")
    ap.add_argument("--ridge-alpha", type=float, default=25.0)
    ap.add_argument("--half-life-days", type=float, default=45.0)
    ap.add_argument("--min-train-rows", type=int, default=5000)
    ap.add_argument("--min-multiplier", type=float, default=0.75)
    ap.add_argument("--max-multiplier", type=float, default=1.30)
    ap.add_argument("--min-coverage-weight", type=float, default=0.50)
    ap.add_argument("--eval-holdout-days", type=int, default=0, help="Optional holdout eval on history before scoring live rows.")
    ap.add_argument("--summary-json", default="", help="Optional path to write JSON summary.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    live = pd.read_csv(in_path)
    if live.empty:
        raise SystemExit(f"[base_v2 scorer] empty input CSV: {in_path}")
    live = _prep_common(live)

    slate_date = _infer_single_slate_date(live, args.slate_date)
    season = int(args.season) if int(args.season or 0) > 0 else _infer_season(slate_date)
    history_to_date = str(args.history_to_date).strip() or str(slate_date)
    history_from_date = str(args.history_from_date).strip()

    hist = _load_history(
        season=season,
        cutoff_date=history_to_date,
        from_date=history_from_date,
    )
    if hist.empty:
        raise SystemExit(
            f"[base_v2 scorer] no history rows for season={season} "
            f"from={history_from_date or 'season_start'} to<{history_to_date}"
        )
    if len(hist) < int(args.min_train_rows):
        raise SystemExit(
            f"[base_v2 scorer] train rows {len(hist)} below --min-train-rows={int(args.min_train_rows)}"
        )

    eval_summary: dict[str, Any] = {}
    if int(args.eval_holdout_days) > 0:
        tr, ho = _split_holdout(hist, int(args.eval_holdout_days))
        fit_eval = _fit_model(
            tr,
            ridge_alpha=float(args.ridge_alpha),
            half_life_days=float(args.half_life_days),
            cutoff_date=str(history_to_date),
        )
        ho_scored = _apply_model(
            ho,
            fit_eval,
            min_multiplier=float(args.min_multiplier),
            max_multiplier=float(args.max_multiplier),
            min_coverage_weight=float(args.min_coverage_weight),
        )
        _score_probs(ho_scored, "expected_sog_base", "base")
        _score_probs(ho_scored, "expected_sog_v2", "base_v2")
        eval_summary = {
            "holdout_days": int(args.eval_holdout_days),
            "rows_train": int(len(tr)),
            "rows_holdout": int(len(ho)),
            "base": _combined_metrics(ho_scored, "base"),
            "base_v2": _combined_metrics(ho_scored, "base_v2"),
        }

    fit = _fit_model(
        hist,
        ridge_alpha=float(args.ridge_alpha),
        half_life_days=float(args.half_life_days),
        cutoff_date=str(history_to_date),
    )

    scored = _apply_model(
        live,
        fit,
        min_multiplier=float(args.min_multiplier),
        max_multiplier=float(args.max_multiplier),
        min_coverage_weight=float(args.min_coverage_weight),
    )
    _score_probs(scored, "expected_sog_base", "base")
    _score_probs(scored, "expected_sog_v2", "base_v2")

    out = pd.DataFrame()
    for c in ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season"]:
        if c in scored.columns:
            out[c] = scored[c]
    out["expected_sog"] = scored["expected_sog_v2"].astype(float)
    out["expected_sog_base"] = scored["expected_sog_base"].astype(float)
    out["expected_sog_v2"] = scored["expected_sog_v2"].astype(float)
    out["expected_sog_bucket"] = _bucket_series(out["expected_sog"])
    out["poisson_source"] = np.where(
        _to_numeric(scored, "d10_sog_per60").notna() & _to_numeric(scored, "d10_toi_min_avg").notna(),
        "d10",
        "fallback",
    )

    out["p_over_1_5"] = scored["p_base_v2_over_1_5"].astype(float)
    out["p_over_2_5"] = scored["p_base_v2_over_2_5"].astype(float)
    out["p_over_3_5"] = scored["p_base_v2_over_3_5"].astype(float)
    out["p_0_1"] = (1.0 - out["p_over_1_5"]).clip(0, 1)
    out["p_2"] = (out["p_over_1_5"] - out["p_over_2_5"]).clip(0, 1)
    out["p_3"] = (out["p_over_2_5"] - out["p_over_3_5"]).clip(0, 1)
    out["p_4p"] = out["p_over_3_5"].clip(0, 1)

    out["p_base_over_1_5"] = scored["p_base_over_1_5"].astype(float)
    out["p_base_over_2_5"] = scored["p_base_over_2_5"].astype(float)
    out["p_base_over_3_5"] = scored["p_base_over_3_5"].astype(float)
    out["p_base_v2_over_1_5"] = scored["p_base_v2_over_1_5"].astype(float)
    out["p_base_v2_over_2_5"] = scored["p_base_v2_over_2_5"].astype(float)
    out["p_base_v2_over_3_5"] = scored["p_base_v2_over_3_5"].astype(float)
    out["base_v2_multiplier"] = scored["base_v2_multiplier"].astype(float)
    out["base_v2_multiplier_raw"] = scored["base_v2_multiplier_raw"].astype(float)
    out["base_v2_coverage"] = scored["base_v2_coverage"].astype(float)
    out["m_team_environment"] = scored["m_team_environment"].astype(float)
    out["m_opponent_suppression"] = scored["m_opponent_suppression"].astype(float)
    out["m_game_state"] = scored["m_game_state"].astype(float)
    out["m_role_usage"] = scored["m_role_usage"].astype(float)
    out["base_v2_log_adj"] = scored["base_v2_log_adj"].astype(float)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    coef_by_group = {}
    for g, feats in FEATURE_GROUPS.items():
        coef_by_group[g] = {f: round(float(fit.coefs.get(f, 0.0)), 6) for f in feats}

    summary = {
        "ok": True,
        "model": "poisson_base_v2_shadow_ridge",
        "slate_date": str(slate_date),
        "season": int(season),
        "rows_live": int(len(out)),
        "rows_history": int(len(hist)),
        "history_from_date": (history_from_date or None),
        "history_to_date_exclusive": str(history_to_date),
        "ridge_alpha": float(args.ridge_alpha),
        "half_life_days": float(args.half_life_days),
        "min_multiplier": float(args.min_multiplier),
        "max_multiplier": float(args.max_multiplier),
        "min_coverage_weight": float(args.min_coverage_weight),
        "coverage": {
            "rows_d10_source": int((out["poisson_source"] == "d10").sum()),
            "rows_fallback_source": int((out["poisson_source"] == "fallback").sum()),
            "coverage_mean": round(float(pd.to_numeric(out["base_v2_coverage"], errors="coerce").mean()), 4),
        },
        "coefficients": {
            "intercept": round(float(fit.intercept), 6),
            "by_group": coef_by_group,
        },
        "eval_holdout": eval_summary,
        "out_csv": str(out_path),
    }

    if str(args.summary_json).strip():
        summary_path = Path(str(args.summary_json).strip())
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["summary_json"] = str(summary_path)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
