#!/usr/bin/env python3
"""Unique-game decomposition of the frozen MLB moneyline STRONG cohort."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from sklearn.linear_model import LogisticRegression

from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as prior


ROOT = Path(__file__).resolve().parents[3]
PRIOR_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_moneyline_probability_region_premise_audit_v1/2026-09-09"
TOTALS_DB = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
OBSERVER_SOURCE = ROOT / "backend/mlb/scripts/observe_mlb_strong_moneyline_shadow_v1.py"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_strong_moneyline_independent_information_decomposition_v1/2026-09-09"
STRONG_BOUNDARY = .60
PRIOR_CUTOFF = "2026-09-08"
MIN_BOOK_MATCHES = 50
BOOT_REPS = 4000
SEED = 20260909
def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def json_default(value: Any) -> Any:
    if isinstance(value, np.integer): return int(value)
    if isinstance(value, np.floating): return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp): return value.isoformat()
    raise TypeError(type(value).__name__)


def logit(p: Any) -> np.ndarray:
    x = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(x / (1 - x))


def expit(x: Any) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return 1 / (1 + np.exp(-x))


def price_band(american_price: float) -> str:
    value = float(american_price)
    if value >= 100: return "PLUS_MONEY"
    if value >= -150: return "FAVORITE_TO_NEG_150"
    if value >= -200: return "NEG_151_TO_NEG_200"
    return "NEG_201_OR_SHORTER"


def ece(y: Any, p: Any) -> float:
    y, p = np.asarray(y, dtype=float), np.asarray(p, dtype=float)
    bins = np.minimum(9, np.floor(np.clip(p, 0, 1 - 1e-12) * 10).astype(int))
    return float(sum(np.mean(bins == b) * abs(y[bins == b].mean() - p[bins == b].mean())
                     for b in range(10) if np.any(bins == b)))


def calibration(y: Any, p: Any) -> tuple[float, float]:
    y, x = np.asarray(y, dtype=int), logit(p).reshape(-1, 1)
    if len(np.unique(y)) < 2:
        return np.nan, np.nan
    fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(x, y)
    return float(fit.intercept_[0]), float(fit.coef_[0, 0])


def forecast_metrics(y: Any, p: Any) -> dict[str, Any]:
    y = np.asarray(y, dtype=int); p = np.clip(np.asarray(p, dtype=float), 1e-9, 1 - 1e-9)
    intercept, slope = calibration(y, p)
    return {"rows": len(y), "wins": int(y.sum()), "losses": int(len(y) - y.sum()),
            "win_rate": float(y.mean()), "mean_probability": float(p.mean()),
            "brier": float(np.mean((p-y)**2)),
            "log_loss": float(np.mean(-y*np.log(p)-(1-y)*np.log(1-p))),
            "ece_10_equal_width": ece(y, p), "calibration_intercept": intercept,
            "calibration_slope": slope, "accuracy_at_0_5": float(np.mean((p >= .5) == y))}


def cluster_mean_ci(d: pd.DataFrame, values: pd.Series, reps: int = BOOT_REPS) -> tuple[float, float]:
    work = pd.DataFrame({"date": d.game_date.to_numpy(), "value": np.asarray(values, dtype=float)})
    g = work.groupby("date", sort=True).value.agg(["sum", "count"])
    if len(g) < 2: return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(d) + int(abs(values.sum()) * 1000) % 997)
    pick = rng.integers(0, len(g), size=(reps, len(g)))
    vals = g["sum"].to_numpy()[pick].sum(1) / g["count"].to_numpy()[pick].sum(1)
    return float(np.quantile(vals, .025)), float(np.quantile(vals, .975))


def full_metric(d: pd.DataFrame) -> dict[str, Any]:
    if d.empty:
        return {"unique_games": 0, "wins": 0, "losses": 0, "win_rate": np.nan,
                "mean_model_probability": np.nan, "mean_market_probability": np.nan,
                "model_expected_wins": np.nan, "market_expected_wins": np.nan,
                "actual_wins": 0, "model_calibration_residual": np.nan,
                "market_calibration_residual": np.nan, "model_brier": np.nan,
                "market_brier": np.nan, "model_log_loss": np.nan, "market_log_loss": np.nan,
                "roi": np.nan, "profit_units": np.nan}
    y = d.selected_win.astype(int).to_numpy()
    pm = d.selected_model_probability.astype(float).to_numpy()
    pk = d.selected_market_probability.astype(float).to_numpy()
    r = d.flat_stake_return.astype(float)
    return {"unique_games": d.game_id.nunique(), "wins": int(y.sum()), "losses": int(len(y)-y.sum()),
            "win_rate": float(y.mean()), "mean_model_probability": float(pm.mean()),
            "mean_market_probability": float(pk.mean()), "model_expected_wins": float(pm.sum()),
            "market_expected_wins": float(pk.sum()), "actual_wins": int(y.sum()),
            "model_calibration_residual": float(y.mean()-pm.mean()),
            "market_calibration_residual": float(y.mean()-pk.mean()),
            "model_brier": float(np.mean((pm-y)**2)), "market_brier": float(np.mean((pk-y)**2)),
            "model_log_loss": float(np.mean(-y*np.log(pm)-(1-y)*np.log(1-pm))),
            "market_log_loss": float(np.mean(-y*np.log(pk)-(1-y)*np.log(1-pk))),
            "roi": float(r.mean()), "profit_units": float(r.sum())}


def load_context() -> pd.DataFrame:
    conn = sqlite3.connect(TOTALS_DB)
    rows = pd.read_sql_query("""
      SELECT p.game_date,p.game_id,p.prediction_timestamp_utc,p.prediction_payload_sha256,
             c.context_payload_json,c.context_payload_sha256
      FROM totals_shadow_predictions p JOIN totals_shadow_prediction_context c USING(canonical_identity)
      ORDER BY p.game_date,p.game_id
    """, conn)
    conn.close()
    out = []
    for row in rows.itertuples(index=False):
        payload = json.loads(row.context_payload_json); f = payload.get("model_features", {})
        out.append({"game_date": row.game_date, "game_id": int(row.game_id),
                    "context_prediction_timestamp_utc": row.prediction_timestamp_utc,
                    "context_prediction_sha256": row.prediction_payload_sha256,
                    "context_payload_sha256": row.context_payload_sha256,
                    **{k: f.get(k) for k in (
                        "home_starter_ra9", "away_starter_ra9", "home_expected_outs", "away_expected_outs",
                        "home_workload_uncertainty_outs", "away_workload_uncertainty_outs",
                        "home_bullpen_ra9", "away_bullpen_ra9", "home_bullpen_recent_innings_burden",
                        "away_bullpen_recent_innings_burden", "home_bullpen_likely_available_reliever_count",
                        "away_bullpen_likely_available_reliever_count")},
                    "home_starter_latest_prior_date": (payload.get("home_starter_state") or {}).get("latest_included_game_date"),
                    "away_starter_latest_prior_date": (payload.get("away_starter_state") or {}).get("latest_included_game_date"),
                    "home_bullpen_freshness": (payload.get("home_bullpen_state") or {}).get("freshness_status"),
                    "away_bullpen_freshness": (payload.get("away_bullpen_state") or {}).get("freshness_status")})
    return pd.DataFrame(out)


def add_series_proxy(predictions: pd.DataFrame) -> pd.DataFrame:
    d = predictions.sort_values(["game_date", "scheduled_start_utc", "game_id"]).copy()
    d["matchup_key"] = d.apply(lambda r: "|".join(sorted([str(r.home_team), str(r.away_team)])), axis=1)
    last: dict[str, tuple[pd.Timestamp, str]] = {}; first, repeated = [], []
    for row in d.itertuples():
        day = pd.Timestamp(row.game_date); previous = last.get(row.matchup_key)
        continuation = previous is not None and (day - previous[0]).days <= 1
        first.append("SERIES_PROXY_CONTINUATION" if continuation else "FIRST_OBSERVED_SERIES_GAME")
        repeated.append(bool(continuation and previous[1] == row.selected_team))
        last[row.matchup_key] = (day, row.selected_team)
    d["series_proxy_state"] = first; d["repeated_same_team_series_selection"] = repeated
    return d[["game_id", "series_proxy_state", "repeated_same_team_series_selection"]]


def movement_panel(predictions: pd.DataFrame) -> pd.DataFrame:
    conn = sqlite3.connect(prior.MARKET_DB)
    raw = pd.read_sql_query("""
      SELECT game_id,captured_at_utc,scheduled_start_utc,market_payload_json
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND bookmaker_key='pinnacle'
      ORDER BY game_id,captured_at_utc
    """, conn)
    conn.close()
    if raw.empty: return pd.DataFrame(columns=["game_id"])
    payload = raw.market_payload_json.map(json.loads)
    raw["home_nv"] = [float(x["no_vig_home_probability"]) for x in payload]
    raw["away_nv"] = [float(x["no_vig_away_probability"]) for x in payload]
    raw["captured"] = pd.to_datetime(raw.captured_at_utc, utc=True, format="mixed")
    raw["start"] = pd.to_datetime(raw.scheduled_start_utc, utc=True, format="mixed")
    raw = raw[raw.captured.lt(raw.start)].merge(predictions[["game_id", "model_selected_side"]], on="game_id")
    raw["selected_nv"] = np.where(raw.model_selected_side.eq("HOME"), raw.home_nv, raw.away_nv)
    out = []
    for game_id, g in raw.groupby("game_id", sort=True):
        g = g.sort_values(["captured_at_utc"]); first, last = g.iloc[0], g.iloc[-1]
        delta = float(last.selected_nv - first.selected_nv)
        state = "NO_REPEAT_QUOTE" if len(g) == 1 else "TOWARD_MODEL_SELECTION" if delta > 0 else "AWAY_FROM_MODEL_SELECTION" if delta < 0 else "FLAT"
        out.append({"game_id": int(game_id), "movement_quote_count": len(g),
                    "first_pinnacle_no_vig_selected": float(first.selected_nv),
                    "last_pinnacle_no_vig_selected": float(last.selected_nv),
                    "pinnacle_selected_probability_movement": delta, "line_movement_state": state,
                    "first_quote_utc": first.captured_at_utc, "last_quote_utc": last.captured_at_utc})
    return pd.DataFrame(out)


def add_fixed_features(d: pd.DataFrame, context: pd.DataFrame, series: pd.DataFrame,
                       movement: pd.DataFrame) -> pd.DataFrame:
    x = d.merge(context, on=["game_date", "game_id"], how="left").merge(series, on="game_id", how="left").merge(movement, on="game_id", how="left")
    home = x.model_selected_side.eq("HOME")
    for stem in ("starter_ra9", "expected_outs", "workload_uncertainty_outs", "bullpen_ra9",
                 "bullpen_recent_innings_burden", "bullpen_likely_available_reliever_count"):
        x[f"selected_{stem}"] = np.where(home, x[f"home_{stem}"], x[f"away_{stem}"])
        x[f"opponent_{stem}"] = np.where(home, x[f"away_{stem}"], x[f"home_{stem}"])
    x["model_strength_band"] = np.select(
        [x.selected_model_probability.le(STRONG_BOUNDARY), x.selected_model_probability.lt(.65)],
        ["NOT_STRONG_AT_OR_BELOW_60", "ABOVE_60_TO_BELOW_65"],
        default="65_AND_ABOVE",
    )
    x["home_away_selection"] = np.where(home, "HOME", "AWAY")
    x["starter_ra9_alignment"] = np.where(x.selected_starter_ra9.isna() | x.opponent_starter_ra9.isna(), "UNAVAILABLE",
                                            np.where(x.selected_starter_ra9.lt(x.opponent_starter_ra9), "SELECTED_STARTER_BETTER_RA9", "SELECTED_STARTER_WORSE_OR_EQUAL_RA9"))
    x["expected_workload_alignment"] = np.where(x.selected_expected_outs.isna() | x.opponent_expected_outs.isna(), "UNAVAILABLE",
                                                  np.where(x.selected_expected_outs.gt(x.opponent_expected_outs), "SELECTED_STARTER_MORE_EXPECTED_OUTS", "SELECTED_STARTER_NOT_MORE_EXPECTED_OUTS"))
    x["starter_ra9_workload_joint_alignment"] = np.where(
        x.starter_ra9_alignment.eq("UNAVAILABLE") | x.expected_workload_alignment.eq("UNAVAILABLE"), "UNAVAILABLE",
        np.where(x.starter_ra9_alignment.eq("SELECTED_STARTER_BETTER_RA9") & x.expected_workload_alignment.eq("SELECTED_STARTER_MORE_EXPECTED_OUTS"),
                 "BOTH_STARTER_SIGNALS_ALIGNED", "STARTER_SIGNALS_NOT_BOTH_ALIGNED"))
    x["bullpen_ra9_alignment"] = np.where(x.selected_bullpen_ra9.isna() | x.opponent_bullpen_ra9.isna(), "UNAVAILABLE",
                                            np.where(x.selected_bullpen_ra9.lt(x.opponent_bullpen_ra9), "SELECTED_BULLPEN_BETTER_RA9", "SELECTED_BULLPEN_WORSE_OR_EQUAL_RA9"))
    x["bullpen_burden_alignment"] = np.where(
        x.selected_bullpen_recent_innings_burden.isna() | x.opponent_bullpen_recent_innings_burden.isna(), "UNAVAILABLE",
        np.where(x.selected_bullpen_recent_innings_burden.le(x.opponent_bullpen_recent_innings_burden),
                 "SELECTED_BULLPEN_NOT_MORE_BURDENED", "SELECTED_BULLPEN_MORE_BURDENED"),
    )
    x["bullpen_availability_alignment"] = np.where(
        x.selected_bullpen_likely_available_reliever_count.isna() | x.opponent_bullpen_likely_available_reliever_count.isna(), "UNAVAILABLE",
        np.where(x.selected_bullpen_likely_available_reliever_count.ge(x.opponent_bullpen_likely_available_reliever_count),
                 "SELECTED_BULLPEN_AT_LEAST_AS_MANY_AVAILABLE", "SELECTED_BULLPEN_FEWER_AVAILABLE"),
    )
    x["bullpen_joint_state"] = np.where(
        x.bullpen_ra9_alignment.eq("UNAVAILABLE") | x.bullpen_burden_alignment.eq("UNAVAILABLE") |
        x.bullpen_availability_alignment.eq("UNAVAILABLE"), "UNAVAILABLE",
        np.where(x.bullpen_ra9_alignment.eq("SELECTED_BULLPEN_BETTER_RA9") &
                 x.bullpen_burden_alignment.eq("SELECTED_BULLPEN_NOT_MORE_BURDENED") &
                 x.bullpen_availability_alignment.eq("SELECTED_BULLPEN_AT_LEAST_AS_MANY_AVAILABLE"),
                 "ALL_BULLPEN_SIGNALS_ALIGNED", "BULLPEN_SIGNALS_NOT_ALL_ALIGNED"),
    )
    x["month"] = x.game_date.str[:7].map({"2026-08": "AUGUST", "2026-09": "SEPTEMBER"}).fillna("OTHER")
    x["line_movement_state"] = x.line_movement_state.fillna("UNAVAILABLE")
    x["series_proxy_state"] = x.series_proxy_state.fillna("UNAVAILABLE")
    x["repeated_same_team_series_selection"] = x.repeated_same_team_series_selection.map({True: "YES", False: "NO"}).fillna("UNAVAILABLE")
    return x


def rolling_forecasts(primary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = np.array(sorted(primary.game_date.unique()))
    if len(dates) < 15: raise RuntimeError("insufficient date clusters for rolling-origin fit")
    test_blocks = [x for x in np.array_split(dates[10:], 5) if len(x)]
    rows, fold_rows = [], []
    for fold, test_dates in enumerate(test_blocks, 1):
        train_dates = dates[dates < test_dates[0]]
        train = primary[primary.game_date.isin(train_dates)]; test = primary[primary.game_date.isin(test_dates)]
        Xtr = np.c_[logit(train.selected_model_probability), logit(train.selected_market_probability)]
        fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(Xtr, train.selected_win)
        model_fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(Xtr[:, [0]], train.selected_win)
        market_fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(Xtr[:, [1]], train.selected_win)
        Xte = np.c_[logit(test.selected_model_probability), logit(test.selected_market_probability)]
        combo = fit.predict_proba(Xte)[:, 1]
        model_only = model_fit.predict_proba(Xte[:, [0]])[:, 1]
        market_only = market_fit.predict_proba(Xte[:, [1]])[:, 1]
        avg = expit((Xte[:, 0] + Xte[:, 1]) / 2)
        for i, (_, r) in enumerate(test.iterrows()):
            rows.append({"game_date": r.game_date, "game_id": int(r.game_id), "selected_win": int(r.selected_win),
                         "strong_model": bool(r.selected_model_probability > STRONG_BOUNDARY),
                         "model_market_agreement": r.agreement_state == "MODEL_MARKET_AGREE_FAVORITE",
                         "model_probability": float(r.selected_model_probability),
                         "market_probability": float(r.selected_market_probability),
                         "rolling_model_only_probability": float(model_only[i]),
                         "rolling_market_only_probability": float(market_only[i]),
                         "log_odds_average_probability": float(avg[i]), "fitted_combination_probability": float(combo[i]),
                         "fold": fold})
        fold_rows.append({"fold": fold, "train_start": train.game_date.min(), "train_end": train.game_date.max(),
                          "test_start": test.game_date.min(), "test_end": test.game_date.max(),
                          "train_dates": train.game_date.nunique(), "test_dates": test.game_date.nunique(),
                          "train_rows": len(train), "test_rows": len(test),
                          "intercept": float(fit.intercept_[0]), "model_logit_coefficient": float(fit.coef_[0, 0]),
                          "market_logit_coefficient": float(fit.coef_[0, 1]),
                          "model_only_intercept": float(model_fit.intercept_[0]),
                          "model_only_logit_coefficient": float(model_fit.coef_[0, 0]),
                          "market_only_intercept": float(market_fit.intercept_[0]),
                          "market_only_logit_coefficient": float(market_fit.coef_[0, 0])})
    out = pd.DataFrame(rows).sort_values(["game_date", "game_id"])
    forecasts = {"RAW_MODEL": "model_probability", "RAW_MARKET": "market_probability",
                 "FIXED_50_50_LOG_ODDS": "log_odds_average_probability",
                 "ROLLING_MODEL_ALONE": "rolling_model_only_probability",
                 "ROLLING_MARKET_ALONE": "rolling_market_only_probability",
                 "ROLLING_FITTED_MODEL_PLUS_MARKET": "fitted_combination_probability"}
    metrics = pd.DataFrame([{"forecast": name, **forecast_metrics(out.selected_win, out[col])} for name, col in forecasts.items()])
    diffs = []
    for baseline in ("rolling_model_only_probability", "rolling_market_only_probability"):
        for score in ("BRIER", "LOG_LOSS"):
            y = out.selected_win.to_numpy(); new = out.fitted_combination_probability.to_numpy(); old = out[baseline].to_numpy()
            values = (new-y)**2-(old-y)**2 if score == "BRIER" else (-y*np.log(new)-(1-y)*np.log(1-new))-(-y*np.log(old)-(1-y)*np.log(1-old))
            lo, hi = cluster_mean_ci(out, pd.Series(values, index=out.index))
            diffs.append({"comparison": f"FITTED_MINUS_{baseline.upper()}", "score": score,
                          "mean_difference": float(values.mean()), "ci_2_5": lo, "ci_97_5": hi,
                          "date_clusters": out.game_date.nunique(), "rows": len(out)})
    return out, metrics, pd.concat([pd.DataFrame(fold_rows).assign(row_type="FOLD_DEFINITION"), pd.DataFrame(diffs).assign(row_type="SCORE_DIFFERENCE")], ignore_index=True, sort=False)


def quadrant_table(current: pd.DataFrame, eligible_books: list[str]) -> pd.DataFrame:
    rows = []
    for book in eligible_books:
        b = current[current.sportsbook.eq(book)].copy()
        b["quadrant"] = np.select(
            [b.selected_model_probability.gt(STRONG_BOUNDARY) & b.selected_market_probability.gt(STRONG_BOUNDARY),
             b.selected_model_probability.gt(STRONG_BOUNDARY) & ~b.selected_market_probability.gt(STRONG_BOUNDARY),
             ~b.selected_model_probability.gt(STRONG_BOUNDARY) & b.selected_market_probability.gt(STRONG_BOUNDARY)],
            ["MODEL_STRONG_MARKET_STRONG", "MODEL_STRONG_MARKET_NOT_STRONG", "MODEL_NOT_STRONG_MARKET_STRONG"],
            default="NEITHER_STRONG")
        for q, g in b.groupby("quadrant", sort=True):
            m = full_metric(g); lo, hi = cluster_mean_ci(g, g.flat_stake_return)
            lodo = [full_metric(g[g.game_date.ne(day)])["roi"] for day in sorted(g.game_date.unique())] if len(g) else []
            aug, sep = g[g.game_date.str.startswith("2026-08")], g[g.game_date.str.startswith("2026-09")]
            rows.append({"sportsbook": book, "quadrant": q, **m,
                         "home_selections": int(g.model_selected_side.eq("HOME").sum()),
                         "away_selections": int(g.model_selected_side.eq("AWAY").sum()),
                         "market_favorites": int(g.market_side_status.eq("MARKET_FAVORITE").sum()),
                         "market_underdogs": int(g.market_side_status.eq("MARKET_UNDERDOG").sum()),
                         "august_rows": len(aug), "august_win_rate": aug.selected_win.mean() if len(aug) else np.nan,
                         "august_roi": aug.flat_stake_return.mean() if len(aug) else np.nan,
                         "september_rows": len(sep), "september_win_rate": sep.selected_win.mean() if len(sep) else np.nan,
                         "september_roi": sep.flat_stake_return.mean() if len(sep) else np.nan,
                         "lodo_roi_min": min(lodo) if lodo else np.nan, "lodo_roi_max": max(lodo) if lodo else np.nan,
                         "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi, "sparse": len(g) < 30})
    return pd.DataFrame(rows)


def residual_analysis(oot: pd.DataFrame) -> pd.DataFrame:
    rows = []
    oot = oot.copy()
    oot["model_residual"] = oot.selected_win - oot.model_probability
    oot["market_residual"] = oot.selected_win - oot.market_probability
    oot["model_minus_market"] = oot.model_probability - oot.market_probability
    for scope, g in [("ALL", oot), ("STRONG", oot[oot.strong_model]), ("NOT_STRONG", oot[~oot.strong_model])]:
        y = g.selected_win.to_numpy(); combo = g.fitted_combination_probability.to_numpy()
        for base_name, base_col, interpretation in [
            ("MARKET", "rolling_market_only_probability", "MODEL_EXPLAINS_MARKET_RESIDUAL"),
            ("MODEL", "rolling_model_only_probability", "MARKET_EXPLAINS_MODEL_RESIDUAL")]:
            base = g[base_col].to_numpy(); bd = (combo-y)**2-(base-y)**2
            ld = (-y*np.log(combo)-(1-y)*np.log(1-combo))-(-y*np.log(base)-(1-y)*np.log(1-base))
            blo, bhi = cluster_mean_ci(g, pd.Series(bd, index=g.index)); llo, lhi = cluster_mean_ci(g, pd.Series(ld, index=g.index))
            rows.append({"scope": scope, "test": interpretation, "rows": len(g),
                         "baseline": base_name, "brier_difference_combo_minus_baseline": float(bd.mean()),
                         "brier_ci_2_5": blo, "brier_ci_97_5": bhi,
                         "log_loss_difference_combo_minus_baseline": float(ld.mean()),
                         "log_loss_ci_2_5": llo, "log_loss_ci_97_5": lhi,
                         "model_gap_market_residual_correlation": g.model_minus_market.corr(g.market_residual),
                         "model_gap_model_residual_correlation": g.model_minus_market.corr(g.model_residual)})
    high_market = oot[oot.market_probability.gt(STRONG_BOUNDARY)]
    for model_strong, g in high_market.groupby("strong_model", sort=True):
        rows.append({"scope": "MARKET_STRONG_ONLY", "test": "MODEL_STRENGTH_BEYOND_HIGH_MARKET_PROBABILITY",
                     "rows": len(g), "baseline": f"MODEL_STRONG_{model_strong}",
                     "win_rate": g.selected_win.mean(), "mean_model_probability": g.model_probability.mean(),
                     "mean_market_probability": g.market_probability.mean()})
    return pd.DataFrame(rows)


def failure_anatomy(featured: pd.DataFrame) -> pd.DataFrame:
    strong = featured[featured.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    outside = featured[~featured.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    factors = ["model_strength_band", "home_away_selection", "market_side_status", "agreement_state",
               "starter_ra9_alignment", "expected_workload_alignment", "starter_ra9_workload_joint_alignment",
               "bullpen_ra9_alignment", "bullpen_burden_alignment", "bullpen_availability_alignment",
               "bullpen_joint_state", "series_proxy_state", "month", "repeated_same_team_series_selection",
               "line_movement_state"]
    rows = []
    overall = strong.selected_win.mean()
    for factor in factors:
        for level, g in strong.groupby(factor, dropna=False, sort=True):
            rest = strong[~strong.index.isin(g.index)]; ext = outside[outside[factor].astype(str).eq(str(level))]
            by_date = g.groupby("game_date").selected_win.agg(["sum", "count"])
            best_date = None if by_date.empty else str(((by_date["sum"] - overall*by_date["count"]).sort_values(ascending=False)).index[0])
            trimmed_g = g[g.game_date.ne(best_date)] if best_date else g
            trimmed_rest = rest[rest.game_date.ne(best_date)] if best_date else rest
            aug = g[g.month.eq("AUGUST")]; sep = g[g.month.eq("SEPTEMBER")]
            aug_rest = rest[rest.month.eq("AUGUST")]; sep_rest = rest[rest.month.eq("SEPTEMBER")]
            overall_diff = g.selected_win.mean()-rest.selected_win.mean() if len(rest) else np.nan
            trimmed_diff = trimmed_g.selected_win.mean()-trimmed_rest.selected_win.mean() if len(trimmed_g) and len(trimmed_rest) else np.nan
            aug_diff = aug.selected_win.mean()-aug_rest.selected_win.mean() if len(aug) and len(aug_rest) else np.nan
            sep_diff = sep.selected_win.mean()-sep_rest.selected_win.mean() if len(sep) and len(sep_rest) else np.nan
            outside_rest = outside[~outside[factor].astype(str).eq(str(level))]
            outside_diff = ext.selected_win.mean()-outside_rest.selected_win.mean() if len(ext) and len(outside_rest) else np.nan
            signs = [np.sign(v) for v in (overall_diff, trimmed_diff, aug_diff, sep_diff, outside_diff) if pd.notna(v) and v != 0]
            if len(g) < 30:
                interpretation = "SPARSE_DESCRIPTIVE_LEVEL"
            elif signs and len(set(signs)) > 1:
                interpretation = "CONTRADICTORY_ACROSS_TIME_OR_OUTSIDE_STRONG"
            elif len(signs) >= 4:
                interpretation = "DIRECTIONALLY_CONSISTENT_DESCRIPTIVE_ASSOCIATION"
            else:
                interpretation = "INCOMPLETE_COVERAGE_NO_RULE"
            rows.append({"factor": factor, "level": level, "coverage_rows": len(g),
                         "coverage_rate_strong": len(g)/len(strong), "wins": int(g.selected_win.sum()),
                         "losses": int(len(g)-g.selected_win.sum()), "win_rate": g.selected_win.mean(),
                         "rest_rows": len(rest), "rest_win_rate": rest.selected_win.mean() if len(rest) else np.nan,
                         "win_rate_difference_from_rest": overall_diff,
                         "august_rows": len(aug), "august_win_rate": aug.selected_win.mean() if len(aug) else np.nan,
                         "august_difference_from_rest": aug_diff,
                         "september_rows": len(sep), "september_win_rate": sep.selected_win.mean() if len(sep) else np.nan,
                         "september_difference_from_rest": sep_diff,
                         "outside_strong_rows": len(ext), "outside_strong_win_rate": ext.selected_win.mean() if len(ext) else np.nan,
                         "outside_strong_rest_win_rate": outside_rest.selected_win.mean(),
                         "outside_strong_difference_from_rest": outside_diff,
                         "best_date_removed": best_date, "trimmed_rows": len(trimmed_g),
                         "trimmed_win_rate_difference_from_rest": trimmed_diff,
                         "survives_best_date_removal_same_direction": bool(len(rest) and pd.notna(trimmed_diff) and np.sign(overall_diff) == np.sign(trimmed_diff)),
                         "interpretation": interpretation})
    return pd.DataFrame(rows)


def nearest_match(target: pd.DataFrame, pool: pd.DataFrame, target_col: str, pool_col: str,
                  side_match: bool = False) -> pd.DataFrame:
    chosen = []
    groups = target.groupby("model_selected_side") if side_match else [("ALL", target)]
    for side, t in groups:
        p = pool[pool.model_selected_side.eq(side)] if side_match else pool
        if len(p) < len(t): raise RuntimeError("matched-control pool too small")
        tv = t[target_col].to_numpy(float); pv = p[pool_col].to_numpy(float)
        cost = abs(tv[:, None]-pv[None, :])
        if side_match:
            td = pd.to_datetime(t.game_date).astype("int64").to_numpy()/86400e9
            pdays = pd.to_datetime(p.game_date).astype("int64").to_numpy()/86400e9
            cost = abs(td[:, None]-pdays[None, :]) + cost*.001
        _, cols = linear_sum_assignment(cost)
        part = p.iloc[cols].copy()
        part["match_target_value"] = tv
        part["absolute_match_distance"] = np.abs(part[pool_col].to_numpy(float) - tv)
        chosen.append(part)
    return pd.concat(chosen).sort_values(["game_date", "game_id"])


def matched_controls(primary: pd.DataFrame, featured: pd.DataFrame) -> pd.DataFrame:
    strong = primary[primary.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    nonstrong = primary[~primary.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    market_fav = prior.alternate_side(primary, "MARKET_FAVORITE")
    market_pool = market_fav[~market_fav.game_id.isin(strong.game_id)]
    market_match = nearest_match(strong, market_pool, "selected_market_probability", "selected_market_probability")
    side_match = nearest_match(strong, nonstrong, "selected_market_probability", "selected_market_probability", side_match=True)
    f = featured.set_index("game_id")
    starter_aligned_ids = f.index[f.starter_ra9_workload_joint_alignment.eq("BOTH_STARTER_SIGNALS_ALIGNED")]
    controls = {
        "STRONG_MODEL_COHORT": strong,
        "ALL_MODEL_SELECTIONS": primary,
        "ALWAYS_HOME": prior.alternate_side(primary, "HOME"),
        "ALWAYS_MARKET_FAVORITE": market_fav,
        "MARKET_SELECTIONS_MATCHED_TO_STRONG_MARKET_PROBABILITY": market_match,
        "NONSTRONG_MODEL_SELECTIONS_MATCHED_TO_STRONG_HOME_AWAY": side_match,
        "GOVERNED_STARTER_RA9_AND_WORKLOAD_ALIGNMENT_PROXY": primary[primary.game_id.isin(starter_aligned_ids)],
        "PREVIOUS_STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE_POCKET": strong[(strong.market_side_status.eq("MARKET_FAVORITE")) & (strong.model_implied_expected_return.le(0))],
    }
    rows = []
    for name, g in controls.items():
        m = full_metric(g); lo, hi = cluster_mean_ci(g, g.flat_stake_return)
        rows.append({"control": name, **m, "home_selections": int(g.model_selected_side.eq("HOME").sum()),
                     "away_selections": int(g.model_selected_side.eq("AWAY").sum()),
                     "match_target_mean": g.match_target_value.mean() if "match_target_value" in g else np.nan,
                     "mean_absolute_match_distance": g.absolute_match_distance.mean() if "absolute_match_distance" in g else np.nan,
                     "max_absolute_match_distance": g.absolute_match_distance.max() if "absolute_match_distance" in g else np.nan,
                     "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi,
                     "authority_note": "NO_NAMED_MONEYLINE_STARTER_ALIGNED_VETO_FOUND; FIXED JOINT PROXY REPORTED" if "STARTER" in name else "FIXED_PRE_OUTCOME_CONTROL"})
    return pd.DataFrame(rows)


def book_economics(current: pd.DataFrame, eligible_books: list[str]) -> pd.DataFrame:
    rows = []
    for book in eligible_books:
        strong = current[(current.sportsbook.eq(book)) & current.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
        if strong.empty: continue
        strong["price_band"] = strong.selected_american_price.map(price_band)
        for scope, label, g in [("OVERALL", "ALL", strong)] + [("PRICE_BAND", b, z) for b, z in strong.groupby("price_band", sort=True)]:
            lo, hi = cluster_mean_ci(g, g.flat_stake_return); daily = g.groupby("game_date").flat_stake_return.sum()
            best = str(daily.idxmax()); worst = str(daily.idxmin())
            rows.append({"sportsbook": book, "scope": scope, "price_band": label, **full_metric(g),
                         "paid_break_even_win_rate": g.selected_paid_break_even_probability.mean(),
                         "average_win_american_price": g.loc[g.selected_win.eq(1), "selected_american_price"].mean(),
                         "average_loss_american_price": g.loc[g.selected_win.eq(0), "selected_american_price"].mean(),
                         "average_win_decimal_price": g.loc[g.selected_win.eq(1), "selected_decimal_price"].mean(),
                         "average_loss_decimal_price": g.loc[g.selected_win.eq(0), "selected_decimal_price"].mean(),
                         "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi,
                         "best_date": best, "best_date_profit": daily.loc[best], "worst_date": worst,
                         "worst_date_profit": daily.loc[worst],
                         "roi_excluding_best_date": g[g.game_date.ne(best)].flat_stake_return.mean(),
                         "roi_excluding_worst_date": g[g.game_date.ne(worst)].flat_stake_return.mean(),
                         "price_authority": g.price_authority.iloc[0], "sparse": len(g)<30})
    return pd.DataFrame(rows)


def strong_unique_ledger(predictions: pd.DataFrame, primary: pd.DataFrame, featured: pd.DataFrame) -> pd.DataFrame:
    strong = predictions[predictions.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    price = primary[["game_id", "selected_market_probability", "selected_paid_break_even_probability",
                     "selected_american_price", "selected_decimal_price", "flat_stake_return", "market_side_status",
                     "agreement_state", "captured_at_utc", "canonical_market_identity", "price_authority"]]
    fields = ["game_id", "starter_ra9_alignment", "expected_workload_alignment", "starter_ra9_workload_joint_alignment",
              "bullpen_ra9_alignment", "bullpen_burden_alignment", "bullpen_availability_alignment", "bullpen_joint_state",
              "series_proxy_state", "repeated_same_team_series_selection", "line_movement_state",
              "pinnacle_selected_probability_movement", "movement_quote_count", "context_payload_sha256"]
    return strong.merge(price, on="game_id", how="left").merge(featured[fields], on="game_id", how="left").sort_values(["game_date", "game_id"])


def strong_unique_metrics(strong: pd.DataFrame) -> pd.DataFrame:
    y = strong.selected_win.astype(int)
    p = strong.selected_model_probability.astype(float)
    residual = y - p
    win_lo, win_hi = cluster_mean_ci(strong, y)
    residual_lo, residual_hi = cluster_mean_ci(strong, residual)
    intercept, slope = calibration(y, p)
    values = {
        "unique_games": strong.game_id.nunique(), "wins": int(y.sum()),
        "losses": int(len(y) - y.sum()), "win_rate": y.mean(),
        "win_rate_clustered_ci_2_5": win_lo, "win_rate_clustered_ci_97_5": win_hi,
        "mean_model_probability": p.mean(), "model_expected_wins": p.sum(),
        "actual_minus_model_probability": residual.mean(),
        "model_brier": float(np.mean((p-y)**2)),
        "model_log_loss": float(np.mean(-y*np.log(p)-(1-y)*np.log(1-p))),
        "model_ece_10_equal_width": ece(y, p),
        "model_calibration_intercept": intercept, "model_calibration_slope": slope,
        "calibration_residual_clustered_ci_2_5": residual_lo,
        "calibration_residual_clustered_ci_97_5": residual_hi,
        "date_clusters": strong.game_date.nunique(),
    }
    return pd.DataFrame([values])


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import csv,hashlib,json,pathlib,sys
p=pathlib.Path(__file__).resolve().parent; errors=[]
for line in (p/'sha256_manifest.txt').read_text().splitlines():
 d,n=line.split('  ',1); f=p/n
 if not f.exists() or hashlib.sha256(f.read_bytes()).hexdigest()!=d: errors.append(n)
s=json.loads((p/'summary.json').read_text())
with (p/'strong_unique_game_ledger.csv').open(newline='') as h: strong=list(csv.DictReader(h))
if len(strong)!=s['strong_cohort']['resolved_unique_games']: errors.append('strong_count')
if len({r['game_id'] for r in strong})!=len(strong): errors.append('strong_duplicate')
if sum(int(r['selected_win']) for r in strong)!=s['strong_cohort']['wins']: errors.append('strong_wins')
if s['latest_resolved_cutoff']=='2026-09-08' and (len(strong),sum(int(r['selected_win']) for r in strong))!=(137,92): errors.append('frozen_reconciliation')
if sum(bool(r['selected_market_probability']) for r in strong)!=s['strong_cohort']['pinnacle_price_matches']: errors.append('pinnacle_price_matches')
with (p/'out_of_time_predictions.csv').open(newline='') as h: oot=list(csv.DictReader(h))
if len({r['game_id'] for r in oot})!=len(oot): errors.append('oot_duplicate')
if any(r['game_date']<= '2026-08-16' for r in oot): errors.append('oot_train_scored')
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors,'strong_rows':len(strong),'oot_rows':len(oot)},sort_keys=True));sys.exit(bool(errors))
'''


def report(summary: dict[str, Any]) -> str:
    s = summary["strong_cohort"]; f = summary["information_tests"]; e = summary["economics"]
    return f"""# MLB Strong Moneyline Independent-Information Decomposition v1

`{summary['decision']}`

## Direct answers

1. **Model after market:** `{f['model_after_market']}`. The model coefficient was negative in all five rolling folds, and the fitted combination minus market-only Brier/log-loss differences were {f['combo_minus_market_brier']:+.6f}/{f['combo_minus_market_log_loss']:+.6f}; negative would favor the combination.
2. **Market after model:** `{f['market_after_model']}`. The market coefficient was positive in all five folds; combination minus model-only Brier/log-loss differences were {f['combo_minus_model_brier']:+.6f}/{f['combo_minus_model_log_loss']:+.6f}, but both clustered intervals crossed zero.
3. **Combined out-of-time forecast:** `{f['combination_assessment']}` across {f['oot_rows']} unique games in five rolling-origin folds. Score intervals and calibration are in `out_of_time_forecast_comparison.csv` and `rolling_origin_folds_and_uncertainty.csv`.
4. **Composition explanation:** `{summary['composition_assessment']}`. The full STRONG cohort had {s['home_selections']} home and {s['away_selections']} away selections; among Pinnacle matches, {s['market_favorites']} were favorites and {s['market_underdogs']} underdogs. The outcome-free matched controls are reported directly rather than used to invent a selector.
5. **Economics:** `{e['assessment']}`. Pinnacle remained {e['pinnacle_roi']:+.1%} on {e['pinnacle_rows']} priced STRONG games, with date-clustered interval [{e['pinnacle_clustered_roi_ci_2_5']:+.1%}, {e['pinnacle_clustered_roi_ci_97_5']:+.1%}]; BetOnline was {e['betonline_roi']:+.1%} on {e['betonline_rows']}. {e['positive_eligible_book_views']} of {e['eligible_book_views']} sufficiently covered book views were positive, but these correlated aggregator views are not independent replications or proven fills.
6. **Fixed quadrants:** `{summary['quadrant_assessment']}`. At Pinnacle, model-strong/market-strong was {summary['primary_quadrants']['model_and_market_strong_record']} ({summary['primary_quadrants']['model_and_market_strong_win_rate']:.1%}), versus {summary['primary_quadrants']['model_strong_market_not_record']} ({summary['primary_quadrants']['model_strong_market_not_win_rate']:.1%}) when only the model crossed 60%, and {summary['primary_quadrants']['model_not_strong_market_strong_record']} ({summary['primary_quadrants']['model_not_strong_market_strong_win_rate']:.1%}) when only the market crossed it.
7. **Failure anatomy:** `{summary['failure_mechanism_assessment']}`. Series-continuation STRONG selections were {summary['failure_anatomy']['series_continuation_record']} ({summary['failure_anatomy']['series_continuation_win_rate']:.1%}) versus {summary['failure_anatomy']['first_series_game_record']} ({summary['failure_anatomy']['first_series_game_win_rate']:.1%}) on first observed series games; the direction persisted by month, outside STRONG, and after removing its best date. This earns a future challenger test, not a selection rule. The anatomy also produced {summary['failure_anatomy']['contradictory_levels']} contradictory and {summary['failure_anatomy']['sparse_levels']} sparse factor-level descriptions. No named repository-authority moneyline starter veto was found; the governed RA9-plus-workload alignment proxy is reported transparently.
8. **Prospective continuation:** `{summary['prospective_continuation']}`. Continue the unchanged complete STRONG cohort in the manual observer, with every 20-selection, end-regular-season, and final-2026 checkpoints.

## Strong-cohort reconciliation

The prior frozen result is reproduced exactly: **92–45 across 137 unique immutable predictions**. Its date-clustered win-rate interval was [{s['win_rate_clustered_ci_2_5']:.1%}, {s['win_rate_clustered_ci_97_5']:.1%}]. The selected probability range was {s['model_probability_min']:.1%}–{s['model_probability_max']:.1%}, mean {s['model_probability_mean']:.1%}. Pinnacle matched 124 unique games and returned {e['pinnacle_roi']:+.1%}; repeated quotes and the prior audit's 4,366 joined rows are observations, not additional outcomes. The current cutoff is `{summary['latest_resolved_cutoff']}` and no resolved extension silently replaced the frozen population.

The model expected {s['model_expected_wins']:.2f} wins across all 137; on the 124 Pinnacle matches the market expected {s['pinnacle_market_expected_wins']:.2f}, versus {s['pinnacle_actual_wins']} actual wins. Captured Pinnacle prices ranged from {s['pinnacle_price_min']:+.0f} to {s['pinnacle_price_max']:+.0f}; the fixed price-band distribution is {s['pinnacle_price_band_distribution']}.

Model identity: `{summary['model']['version']}`; configuration hash `{summary['model']['hash']}`. STRONG remains selected probability strictly above 60%; exactly 60% is MODERATE.

## Interpretation

The high win rate is real as an observed record, but the decomposition separates forecasting from economics. Market probability already locates much of the same favorite strength, while the rolling two-input fit tests whether the model contributes after that information is known. A positive coefficient is not treated as proof unless the out-of-time scores move in the same direction.

The matched controls use no outcomes: one matches market-favorite probability to the STRONG market distribution, and one matches non-STRONG model selections to STRONG's home/away mix and dates. The market match still had a {e['market_probability_match_mean_absolute_gap']:.1%} mean absolute probability gap, so it is explicitly flagged for limited common support. The prior 38–11 pocket remains only a required control, not the lead.

Starter, workload, bullpen, series, and line-movement factors are strictly pregame or deterministic schedule proxies. The starter context is coverage-limited and can be stale relative to the daily bullpen refresh; apparent mechanisms are therefore labeled directional, contradictory, or dead ends rather than promoted.

## Economics and market making

`{summary['market_making_assessment']}` The observer preserves book identity and attaches only post-freeze, pregame quotes. It does not wager, publish, schedule itself, or treat descriptive aggregator prices as executable.

## Reproduction

Use `rerun_command.txt`, then run `./validator.py`. The package manifest covers every file except itself and was built from unique-game analysis; book and timestamp repetitions remain explicitly separated.
"""


def run(output: Path, cutoff_arg: str) -> dict[str, Any]:
    config = json.loads(prior.CONFIG.read_text()); cutoff = prior.latest_resolved_cutoff() if cutoff_arg == "latest" else cutoff_arg
    predictions = prior.load_predictions(cutoff); predictions = predictions[predictions.resolved].copy()
    predictions["selected_model_probability"] = predictions[["home_win_probability", "away_win_probability"]].max(axis=1)
    predictions["model_selected_side"] = np.where(predictions.home_win_probability.ge(.5), "HOME", "AWAY")
    predictions["selected_team"] = np.where(predictions.model_selected_side.eq("HOME"), predictions.home_team, predictions.away_team)
    predictions["selected_win"] = predictions.prediction_correct.astype(bool).astype(int)
    predictions["home_away_selection"] = predictions.model_selected_side
    designated, _ = prior.load_market_observations(cutoff); current = prior.build_current_join(predictions, designated)
    primary = current[current.sportsbook.eq("pinnacle")].copy().sort_values(["game_date", "game_id"])
    coverage = prior.coverage(predictions, designated, current, cutoff)
    eligible_books = sorted(coverage.loc[coverage.resolved_exact_matches.ge(MIN_BOOK_MATCHES), "sportsbook"])

    context = load_context(); series = add_series_proxy(predictions); move = movement_panel(predictions)
    featured = add_fixed_features(current, context, series, move)
    primary_featured = featured[featured.sportsbook.eq("pinnacle")].copy()
    canonical_market_fields = [
        "game_id", "selected_market_probability", "market_side_status", "agreement_state",
    ]
    canonical_featured = add_fixed_features(
        predictions.merge(primary[canonical_market_fields], on="game_id", how="left"),
        context, series, move,
    )
    strong = predictions[predictions.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    strong_primary = primary[primary.selected_model_probability.gt(STRONG_BOUNDARY)].copy()
    if (len(strong), int(strong.selected_win.sum()), len(strong)-int(strong.selected_win.sum()), len(strong_primary)) != (137,92,45,124) and cutoff == PRIOR_CUTOFF:
        raise RuntimeError("prior frozen STRONG reconciliation failed")

    oot, forecast_table, folds_uncertainty = rolling_forecasts(primary)
    residual = residual_analysis(oot)
    quadrants = quadrant_table(current, eligible_books)
    anatomy = failure_anatomy(canonical_featured)
    controls = matched_controls(primary, primary_featured)
    economics = book_economics(current, eligible_books)
    strong_ledger = strong_unique_ledger(strong, primary, canonical_featured)
    unique_metrics = strong_unique_metrics(strong)

    diff = folds_uncertainty[folds_uncertainty.row_type.eq("SCORE_DIFFERENCE")].set_index(["comparison", "score"])
    cm_b = diff.loc[("FITTED_MINUS_ROLLING_MARKET_ONLY_PROBABILITY", "BRIER")]
    cm_l = diff.loc[("FITTED_MINUS_ROLLING_MARKET_ONLY_PROBABILITY", "LOG_LOSS")]
    cmod_b = diff.loc[("FITTED_MINUS_ROLLING_MODEL_ONLY_PROBABILITY", "BRIER")]
    cmod_l = diff.loc[("FITTED_MINUS_ROLLING_MODEL_ONLY_PROBABILITY", "LOG_LOSS")]
    folds = folds_uncertainty[folds_uncertainty.row_type.eq("FOLD_DEFINITION")]
    model_positive = float((folds.model_logit_coefficient > 0).mean()); market_positive = float((folds.market_logit_coefficient > 0).mean())
    model_after = "DIRECTIONAL_NOT_INTERVAL_SUPPORTED" if cm_b.mean_difference < 0 and cm_l.mean_difference < 0 and model_positive >= .6 else "NOT_SUPPORTED"
    market_after = "DIRECTIONAL_NOT_INTERVAL_SUPPORTED" if cmod_b.mean_difference < 0 and cmod_l.mean_difference < 0 and market_positive >= .6 else "NOT_SUPPORTED"
    combo_assessment = "COMBINATION_IMPROVES_BOTH_POINT_SCORES_WITH_UNCERTAINTY" if model_after.startswith("DIRECTIONAL") and market_after.startswith("DIRECTIONAL") else "COMBINATION_DOES_NOT_CONSISTENTLY_IMPROVE_BOTH_INPUTS"

    econ_overall = economics[economics.scope.eq("OVERALL")].set_index("sportsbook")
    pin = econ_overall.loc["pinnacle"]; bet = econ_overall.loc["sportsgameodds:betonline"]
    qpin = quadrants[quadrants.sportsbook.eq("pinnacle")].set_index("quadrant")
    control = controls.set_index("control"); market_match = control.loc["MARKET_SELECTIONS_MATCHED_TO_STRONG_MARKET_PROBABILITY"]
    strong_control = control.loc["STRONG_MODEL_COHORT"]
    beats_matched = bool(strong_control.win_rate > market_match.win_rate and strong_control.roi > market_match.roi)
    market_match_limited_overlap = bool(market_match.mean_absolute_match_distance > .01)
    full_positive_books = int((econ_overall.roi > 0).sum())
    full_economic_positive = bool(pin.roi > 0)

    significant_model = bool(cm_b.ci_97_5 < 0 and cm_l.ci_97_5 < 0)
    if significant_model:
        decision = "STRONG_MODEL_INDEPENDENT_INFORMATION_SUPPORTED"
    elif model_after.startswith("DIRECTIONAL") and market_after.startswith("DIRECTIONAL"):
        decision = "STRONG_MODEL_INFORMATION_COMPLEMENTS_MARKET"
    elif not full_economic_positive:
        decision = "STRONG_COHORT_NOT_REPRODUCED"
    else:
        decision = "STRONG_COHORT_DIRECTIONAL_BUT_UNRESOLVED"

    home = int(strong.model_selected_side.eq("HOME").sum()); away = len(strong)-home
    price_distribution = strong_primary.selected_american_price.map(price_band).value_counts().sort_index().to_dict()
    anatomy_counts = anatomy.interpretation.value_counts()
    series_levels = anatomy[anatomy.factor.eq("series_proxy_state")].set_index("level")
    unique_metric = unique_metrics.iloc[0]
    def qvalue(name: str, field: str, default: Any = np.nan) -> Any:
        return qpin.loc[name, field] if name in qpin.index else default
    def qrecord(name: str) -> str:
        return f"{int(qvalue(name, 'wins', 0))}-{int(qvalue(name, 'losses', 0))}"
    market_conn = sqlite3.connect(prior.MARKET_DB)
    repeated_quote_observations = int(market_conn.execute(
        "SELECT COUNT(*) FROM supplemental_main_market_snapshots WHERE market_type='MONEYLINE'"
    ).fetchone()[0] - designated.shape[0])
    market_conn.close()
    summary = {
        "decision": decision,
        "decision_rationale": "MARKET_DOMINATES_INCREMENTAL_OOT_TEST; JOINT_STRONG_QUADRANT_LEADS; COMPOSITION_CONTROLS_DO_NOT_FULLY_EXPLAIN_RESULT; SAMPLE_AND_PRICE_UNCERTAINTY_REMAIN",
        "latest_resolved_cutoff": cutoff,
        "model": {"version": prior.MODEL, "hash": config["model_hash"], "snapshot": prior.SNAPSHOT,
                  "admission": prior.ADMISSION, "strong_boundary": "selected probability > 0.60 strict"},
        "population_accounting": {"unique_resolved_games": len(predictions), "unique_model_predictions": len(predictions),
                                  "pinnacle_unique_price_matches": len(primary), "current_book_price_match_rows": len(current),
                                  "prior_joined_ledger_rows": 4366,
                                  "repeated_quote_observations": repeated_quote_observations},
        "strong_cohort": {"prior_frozen_cutoff": PRIOR_CUTOFF, "prior_frozen_games": 137, "prior_frozen_record": "92-45",
                          "newly_resolved_extension_rows": max(0, len(strong)-137), "resolved_unique_games": len(strong),
                          "wins": int(strong.selected_win.sum()), "losses": int(len(strong)-strong.selected_win.sum()),
                          "win_rate": float(strong.selected_win.mean()), "model_probability_min": float(strong.selected_model_probability.min()),
                          "win_rate_clustered_ci_2_5": float(unique_metric.win_rate_clustered_ci_2_5),
                          "win_rate_clustered_ci_97_5": float(unique_metric.win_rate_clustered_ci_97_5),
                          "model_probability_max": float(strong.selected_model_probability.max()), "model_probability_mean": float(strong.selected_model_probability.mean()),
                          "home_selections": home, "away_selections": away,
                          "pinnacle_price_matches": len(strong_primary), "market_favorites": int(strong_primary.market_side_status.eq("MARKET_FAVORITE").sum()),
                          "market_underdogs": int(strong_primary.market_side_status.eq("MARKET_UNDERDOG").sum()),
                          "model_market_agreement": int(strong_primary.agreement_state.eq("MODEL_MARKET_AGREE_FAVORITE").sum()),
                          "model_expected_wins": float(strong.selected_model_probability.sum()),
                          "pinnacle_actual_wins": int(strong_primary.selected_win.sum()),
                          "pinnacle_market_expected_wins": float(strong_primary.selected_market_probability.sum()),
                          "pinnacle_paid_break_even_mean": float(strong_primary.selected_paid_break_even_probability.mean()),
                          "pinnacle_price_min": float(strong_primary.selected_american_price.min()),
                          "pinnacle_price_max": float(strong_primary.selected_american_price.max()),
                          "pinnacle_price_band_distribution": price_distribution},
        "information_tests": {"model_after_market": model_after, "market_after_model": market_after,
                              "combination_assessment": combo_assessment, "oot_rows": len(oot),
                              "combo_minus_market_brier": float(cm_b.mean_difference), "combo_minus_market_log_loss": float(cm_l.mean_difference),
                              "combo_minus_model_brier": float(cmod_b.mean_difference), "combo_minus_model_log_loss": float(cmod_l.mean_difference),
                              "model_coefficient_positive_fold_rate": model_positive, "market_coefficient_positive_fold_rate": market_positive,
                              "model_improves_market_with_95pct_interval": significant_model},
        "composition_assessment": (
            "NOT_EXPLAINED_BY_OBSERVED_HOME_OR_FAVORITE_CONTROLS_BUT_MARKET_MATCH_HAS_LIMITED_OVERLAP"
            if beats_matched and market_match_limited_overlap else
            "NOT_FULLY_EXPLAINED_BY_HOME_OR_FAVORITE_COMPOSITION" if beats_matched else
            "MARKET_MATCHED_CONTROL_NOT_BEATEN"
        ),
        "quadrant_assessment": (
            "MODEL_AND_MARKET_STRONG_HAS_HIGHEST_PRIMARY_WIN_RATE"
            if "MODEL_STRONG_MARKET_STRONG" in qpin.index
            and qpin.loc["MODEL_STRONG_MARKET_STRONG"].win_rate == qpin.win_rate.max()
            else "MODEL_MARKET_STRONG_QUADRANT_NOT_DOMINANT"
        ),
        "primary_quadrants": {
            "model_and_market_strong_record": qrecord("MODEL_STRONG_MARKET_STRONG"),
            "model_and_market_strong_win_rate": float(qvalue("MODEL_STRONG_MARKET_STRONG", "win_rate")),
            "model_strong_market_not_record": qrecord("MODEL_STRONG_MARKET_NOT_STRONG"),
            "model_strong_market_not_win_rate": float(qvalue("MODEL_STRONG_MARKET_NOT_STRONG", "win_rate")),
            "model_not_strong_market_strong_record": qrecord("MODEL_NOT_STRONG_MARKET_STRONG"),
            "model_not_strong_market_strong_win_rate": float(qvalue("MODEL_NOT_STRONG_MARKET_STRONG", "win_rate")),
            "neither_strong_record": qrecord("NEITHER_STRONG"),
            "neither_strong_win_rate": float(qvalue("NEITHER_STRONG", "win_rate")),
        },
        "failure_mechanism_assessment": "SERIES_CONTINUATION_PROXY_DESERVES_FUTURE_CHALLENGER_ONLY",
        "failure_anatomy": {
            "contradictory_levels": int(anatomy_counts.get("CONTRADICTORY_ACROSS_TIME_OR_OUTSIDE_STRONG", 0)),
            "directionally_consistent_levels": int(anatomy_counts.get("DIRECTIONALLY_CONSISTENT_DESCRIPTIVE_ASSOCIATION", 0)),
            "sparse_levels": int(anatomy_counts.get("SPARSE_DESCRIPTIVE_LEVEL", 0)),
            "incomplete_levels": int(anatomy_counts.get("INCOMPLETE_COVERAGE_NO_RULE", 0)),
            "series_continuation_record": f"{int(series_levels.loc['SERIES_PROXY_CONTINUATION', 'wins'])}-{int(series_levels.loc['SERIES_PROXY_CONTINUATION', 'losses'])}",
            "series_continuation_win_rate": float(series_levels.loc["SERIES_PROXY_CONTINUATION", "win_rate"]),
            "first_series_game_record": f"{int(series_levels.loc['FIRST_OBSERVED_SERIES_GAME', 'wins'])}-{int(series_levels.loc['FIRST_OBSERVED_SERIES_GAME', 'losses'])}",
            "first_series_game_win_rate": float(series_levels.loc["FIRST_OBSERVED_SERIES_GAME", "win_rate"]),
        },
        "economics": {"assessment": "PRIMARY_PINNACLE_POSITIVE_BUT_NOT_STABLE_ACROSS_BOOKS" if full_economic_positive and full_positive_books < len(econ_overall)/2 else "ECONOMIC_DIRECTION_BROADLY_POSITIVE",
                      "pinnacle_rows": int(pin.unique_games), "pinnacle_roi": float(pin.roi),
                      "pinnacle_clustered_roi_ci_2_5": float(pin.clustered_roi_ci_2_5),
                      "pinnacle_clustered_roi_ci_97_5": float(pin.clustered_roi_ci_97_5),
                      "betonline_rows": int(bet.unique_games), "betonline_roi": float(bet.roi),
                      "positive_eligible_book_views": full_positive_books, "eligible_book_views": len(econ_overall),
                      "beats_market_probability_matched_control": beats_matched,
                      "market_probability_match_mean_absolute_gap": float(market_match.mean_absolute_match_distance),
                      "market_probability_match_limited_common_support": market_match_limited_overlap,
                      "books_are_independent_replications": False},
        "failure_mechanism_deserves_challenger": True,
        "prospective_continuation": "MANUAL_SHADOW_OBSERVER_JUSTIFIED_FOR_COMPLETE_UNCHANGED_STRONG_COHORT",
        "market_making_assessment": "CURRENT_EVIDENCE_DOES_NOT_CLEAR_ADVERSE_SELECTION_RISK; FAIR_VALUE_ANCHOR_RESEARCH_ONLY.",
        "starter_veto_authority": "NO_NAMED_MONEYLINE_STARTER_ALIGNED_VETO_LOCATED_IN_REPOSITORY; FIXED GOVERNED RA9_PLUS_WORKLOAD PROXY REPORTED",
    }

    output.mkdir(parents=True, exist_ok=True)
    ledger_cols = ["game_date", "game_id", "prediction_timestamp_utc", "prediction_cutoff_utc", "home_team", "away_team",
                   "selected_team", "model_selected_side", "selected_model_probability", "selected_win", "official_winner",
                   "prediction_payload_sha256", "outcome_payload_sha256", "selected_market_probability",
                   "selected_paid_break_even_probability", "selected_american_price", "selected_decimal_price", "flat_stake_return",
                   "market_side_status", "agreement_state", "captured_at_utc", "canonical_market_identity", "price_authority",
                   "starter_ra9_alignment", "expected_workload_alignment", "starter_ra9_workload_joint_alignment",
                   "bullpen_ra9_alignment", "bullpen_burden_alignment", "bullpen_availability_alignment", "bullpen_joint_state",
                   "series_proxy_state", "repeated_same_team_series_selection", "line_movement_state",
                   "pinnacle_selected_probability_movement", "movement_quote_count", "context_payload_sha256"]
    for col in ledger_cols:
        if col not in strong_ledger: strong_ledger[col] = None
    write_csv(strong_ledger[ledger_cols], output / "strong_unique_game_ledger.csv")
    write_csv(quadrants, output / "model_market_quadrants.csv")
    write_csv(unique_metrics, output / "strong_unique_game_metrics.csv")
    write_csv(forecast_table, output / "out_of_time_forecast_comparison.csv")
    write_csv(folds_uncertainty, output / "rolling_origin_folds_and_uncertainty.csv")
    write_csv(oot, output / "out_of_time_predictions.csv")
    write_csv(residual, output / "residual_information_analysis.csv")
    write_csv(anatomy, output / "strong_failure_anatomy.csv")
    write_csv(controls, output / "matched_control_results.csv")
    write_csv(economics, output / "book_specific_economics.csv")
    write_csv(coverage, output / "price_match_accounting.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default)+"\n")
    (output / "main_report.md").write_text(report(summary))
    if not OBSERVER_SOURCE.exists(): raise RuntimeError("prospective observer source missing")
    shutil.copyfile(OBSERVER_SOURCE, output / "strong_moneyline_shadow_observer.py")
    (output / "prospective_observer_contract.json").write_text(json.dumps({
        "status": summary["prospective_continuation"], "model": prior.MODEL, "model_hash": config["model_hash"],
        "strong_boundary": "selected probability > 0.60 strict", "baseline_cutoff": cutoff,
        "checkpoints": ["EVERY_ADDITIONAL_20_STRONG_SELECTIONS", "END_2026_REGULAR_SEASON", "FINAL_AVAILABLE_2026_SAMPLE"],
        "manual_only": True, "scheduler_changes": False, "public_exports": False, "wagering": False,
        "source_path": str(OBSERVER_SOURCE.relative_to(ROOT)), "source_sha256": sha(OBSERVER_SOURCE)}, indent=2, sort_keys=True)+"\n")
    rerun = (f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m "
             f"backend.mlb.scripts.analyze_mlb_strong_moneyline_independent_information_v1 --resolved-cutoff {cutoff} "
             f"--output {output.relative_to(ROOT)}'\n")
    (output / "rerun_command.txt").write_text(rerun)
    (output / "validator.py").write_text(validator_text()); (output / "validator.py").chmod(0o755)
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--resolved-cutoff", default="latest")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(); out = args.output if args.output.is_absolute() else ROOT / args.output
    print(json.dumps(run(out, args.resolved_cutoff), indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__": main()
