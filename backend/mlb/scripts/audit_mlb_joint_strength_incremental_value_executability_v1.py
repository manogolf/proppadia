#!/usr/bin/env python3
"""Joint-strength incremental-value and executable-price audit (MLB moneyline v1)."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as prior
from backend.mlb.scripts import audit_mlb_across_board_apparent_ev_provenance_economic_value_v1 as across
from backend.mlb.scripts import analyze_mlb_strong_moneyline_independent_information_v1 as strong


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09"
OBSERVER_SOURCE = ROOT / "backend/mlb/scripts/observe_mlb_moneyline_strength_classes_shadow_v1.py"
BOUNDARY = strong.STRONG_BOUNDARY
PRIOR_FREEZE = "2026-09-08"
ORIGINAL_END = "2026-08-13"
BOOT_REPS = 4000
SEED = 20260909
CLASS_ORDER = ["JOINT_STRONG_SAME_SIDE", "MODEL_STRONG_ONLY", "MARKET_STRONG_ONLY", "STRONG_CONFLICT", "NEITHER_STRONG"]
BETONLINE = "sportsgameodds:betonline"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating,)): return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp,)): return value.isoformat()
    raise TypeError(type(value).__name__)


def logit(p: Iterable[float]) -> np.ndarray:
    x = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(x / (1 - x))


def expit(x: Iterable[float]) -> np.ndarray:
    z = np.asarray(x, dtype=float)
    return 1 / (1 + np.exp(-z))


def side_value(frame: pd.DataFrame, side: pd.Series, home_col: str, away_col: str) -> np.ndarray:
    return np.where(side.eq("HOME"), pd.to_numeric(frame[home_col]), pd.to_numeric(frame[away_col]))


def classify(frame: pd.DataFrame) -> pd.DataFrame:
    d = frame.copy()
    d["model_strong_side"] = np.select(
        [d.home_win_probability.gt(BOUNDARY), d.away_win_probability.gt(BOUNDARY)], ["HOME", "AWAY"], default="NONE")
    d["market_strong_side"] = np.select(
        [d.no_vig_home_probability.gt(BOUNDARY), d.no_vig_away_probability.gt(BOUNDARY)], ["HOME", "AWAY"], default="NONE")
    model_strong = d.model_strong_side.ne("NONE")
    market_strong = d.market_strong_side.ne("NONE")
    same = d.model_strong_side.eq(d.market_strong_side)
    d["strength_class"] = np.select(
        [model_strong & market_strong & same,
         model_strong & market_strong & ~same,
         model_strong & ~market_strong,
         market_strong & ~model_strong],
        ["JOINT_STRONG_SAME_SIDE", "STRONG_CONFLICT", "MODEL_STRONG_ONLY", "MARKET_STRONG_ONLY"],
        default="NEITHER_STRONG")
    d["home_win"] = np.where(d.model_selected_side.eq("HOME"), d.selected_win, 1 - d.selected_win).astype(int)
    d["market_favorite_side"] = np.where(d.no_vig_home_probability.ge(.5), "HOME", "AWAY")
    d["series_proxy_state"] = d.get("series_proxy_state", "UNAVAILABLE")
    return d


def evaluated(frame: pd.DataFrame, side: pd.Series) -> pd.DataFrame:
    d = frame.copy()
    d["evaluated_side"] = side.to_numpy()
    d["evaluated_team"] = np.where(d.evaluated_side.eq("HOME"), d.home_team, d.away_team)
    d["evaluated_win"] = np.where(d.evaluated_side.eq("HOME"), d.home_win, 1 - d.home_win).astype(int)
    d["evaluated_model_probability"] = side_value(d, d.evaluated_side, "home_win_probability", "away_win_probability")
    d["evaluated_market_probability"] = side_value(d, d.evaluated_side, "no_vig_home_probability", "no_vig_away_probability")
    d["evaluated_american_price"] = side_value(d, d.evaluated_side, "home_american_price", "away_american_price")
    d["evaluated_decimal_price"] = side_value(d, d.evaluated_side, "home_decimal_price", "away_decimal_price")
    d["evaluated_paid_break_even"] = side_value(d, d.evaluated_side, "home_implied_probability", "away_implied_probability")
    d["flat_stake_return"] = np.where(d.evaluated_win.eq(1), d.evaluated_decimal_price - 1, -1)
    d["evaluated_home"] = d.evaluated_side.eq("HOME").astype(int)
    d["evaluated_market_favorite"] = d.evaluated_side.eq(d.market_favorite_side).astype(int)
    return d


def cluster_ci(dates: Iterable[str], values: Iterable[float]) -> tuple[float, float]:
    x = pd.DataFrame({"date": np.asarray(dates), "value": np.asarray(values, dtype=float)}).dropna()
    g = x.groupby("date", sort=True).value.agg(["sum", "count"])
    if len(g) < 2: return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(x))
    pick = rng.integers(0, len(g), size=(BOOT_REPS, len(g)))
    means = g["sum"].to_numpy()[pick].sum(1) / g["count"].to_numpy()[pick].sum(1)
    return float(np.quantile(means, .025)), float(np.quantile(means, .975))


def econ(d: pd.DataFrame) -> dict[str, Any]:
    if d.empty:
        return {"unique_games": 0, "wins": 0, "losses": 0, "win_rate": np.nan,
                "average_american_price": np.nan, "average_decimal_price": np.nan,
                "paid_break_even_probability": np.nan, "market_expected_wins": np.nan,
                "model_expected_wins": np.nan, "actual_wins": 0, "profit_units": np.nan,
                "roi": np.nan, "roi_ci_2_5": np.nan, "roi_ci_97_5": np.nan,
                "best_date": None, "worst_date": None, "roi_excluding_best_date": np.nan,
                "roi_excluding_worst_date": np.nan}
    lo, hi = cluster_ci(d.game_date, d.flat_stake_return)
    daily = d.groupby("game_date", sort=True).flat_stake_return.sum()
    best, worst = str(daily.idxmax()), str(daily.idxmin())
    return {"unique_games": int(d.game_id.nunique()), "wins": int(d.evaluated_win.sum()),
            "losses": int(len(d)-d.evaluated_win.sum()), "win_rate": float(d.evaluated_win.mean()),
            "average_american_price": float(d.evaluated_american_price.mean()),
            "average_decimal_price": float(d.evaluated_decimal_price.mean()),
            "paid_break_even_probability": float(d.evaluated_paid_break_even.mean()),
            "market_expected_wins": float(d.evaluated_market_probability.sum()),
            "model_expected_wins": float(d.evaluated_model_probability.sum()),
            "actual_wins": int(d.evaluated_win.sum()), "profit_units": float(d.flat_stake_return.sum()),
            "roi": float(d.flat_stake_return.mean()), "roi_ci_2_5": lo, "roi_ci_97_5": hi,
            "best_date": best, "worst_date": worst,
            "roi_excluding_best_date": float(d[d.game_date.ne(best)].flat_stake_return.mean()),
            "roi_excluding_worst_date": float(d[d.game_date.ne(worst)].flat_stake_return.mean())}


def primary_ledger(predictions: pd.DataFrame, designated: pd.DataFrame) -> pd.DataFrame:
    pin = designated[designated.sportsbook.eq("pinnacle")].copy()
    series = strong.add_series_proxy(predictions)
    d = classify(pin.merge(series, on="game_id", how="left", validate="one_to_one"))
    side = np.where(d.strength_class.eq("MARKET_STRONG_ONLY"), d.market_strong_side,
                    np.where(d.model_strong_side.ne("NONE"), d.model_strong_side, d.model_selected_side))
    d = evaluated(d, pd.Series(side, index=d.index))
    d["date_block"] = pd.to_datetime(d.game_date).dt.strftime("%Y-W%W")
    d["month"] = d.game_date.str[:7]
    d["price_band"] = d.evaluated_american_price.map(strong.price_band)
    return d.sort_values(["game_date", "game_id"]).reset_index(drop=True)


def test_panels(ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    market = ledger[ledger.market_strong_side.ne("NONE") & ~ledger.strength_class.eq("STRONG_CONFLICT")].copy()
    a = evaluated(market, market.market_strong_side)
    a["comparison_group"] = np.where(a.strength_class.eq("JOINT_STRONG_SAME_SIDE"),
                                     "MODEL_ALSO_STRONG_SAME_TEAM", "MODEL_NOT_STRONG_MARKET_TEAM")
    model = ledger[ledger.model_strong_side.ne("NONE") & ~ledger.strength_class.eq("STRONG_CONFLICT")].copy()
    b = evaluated(model, model.model_strong_side)
    b["comparison_group"] = np.where(b.strength_class.eq("JOINT_STRONG_SAME_SIDE"),
                                     "MARKET_ALSO_STRONG_SAME_TEAM", "MARKET_NOT_STRONG_MODEL_TEAM")
    rows = []
    for test, panel in (("TEST_A_WITHIN_MARKET_STRONG", a), ("TEST_B_WITHIN_MODEL_STRONG", b)):
        for group, g in panel.groupby("comparison_group", sort=True):
            rows.append({"test": test, "group": group, **econ(g),
                         "home_sides": int(g.evaluated_home.sum()), "market_favorites": int(g.evaluated_market_favorite.sum()),
                         "mean_quote_lead_minutes": float(g.quote_lead_minutes.mean())})
    return a, b, pd.DataFrame(rows)


def matched_pairs(panel: pd.DataFrame, test: str, treated_label: str, control_label: str) -> pd.DataFrame:
    treated = panel[panel.comparison_group.eq(treated_label)].copy().reset_index(drop=True)
    control = panel[panel.comparison_group.eq(control_label)].copy().reset_index(drop=True)
    definitions = {
        "MARKET_PROBABILITY_MATCHED": (["evaluated_market_probability"], []),
        "PRICE_MATCHED": (["evaluated_paid_break_even"], []),
        "DATE_BLOCK_MATCHED": (["evaluated_market_probability"], ["date_block"]),
        "HOME_FAVORITE_COMPOSITION_MATCHED": (["evaluated_market_probability"], ["evaluated_side", "evaluated_market_favorite"]),
        "FULL_PREDECLARED_MATCH": (["evaluated_market_probability", "evaluated_paid_break_even", "quote_lead_minutes"], ["date_block", "evaluated_side"]),
    }
    rows = []
    for label, (continuous, exact) in definitions.items():
        if treated.empty or control.empty: continue
        tv = treated[continuous].astype(float).to_numpy(); cv = control[continuous].astype(float).to_numpy()
        pooled = np.vstack([tv, cv]); scale = np.nanstd(pooled, axis=0); scale[scale == 0] = 1
        cost = np.sqrt((((tv[:, None, :] - cv[None, :, :]) / scale) ** 2).sum(2))
        for col in exact:
            cost += (treated[col].astype(str).to_numpy()[:, None] != control[col].astype(str).to_numpy()[None, :]) * 1e6
        ri, ci = linear_sum_assignment(cost)
        keep = cost[ri, ci] < 1e6
        t, c = treated.iloc[ri[keep]].copy(), control.iloc[ci[keep]].copy()
        if t.empty: continue
        wd = t.evaluated_win.to_numpy(float) - c.evaluated_win.to_numpy(float)
        rd = t.flat_stake_return.to_numpy(float) - c.flat_stake_return.to_numpy(float)
        lo, hi = cluster_ci(t.game_date, rd)
        overlap_lo = max(t.evaluated_market_probability.min(), c.evaluated_market_probability.min())
        overlap_hi = min(t.evaluated_market_probability.max(), c.evaluated_market_probability.max())
        rows.append({"test": test, "match": label, "pairs": len(t),
                     "available_treated": len(treated), "available_controls": len(control),
                     "treated_win_rate": float(t.evaluated_win.mean()), "control_win_rate": float(c.evaluated_win.mean()),
                     "paired_win_rate_difference": float(wd.mean()), "treated_roi": float(t.flat_stake_return.mean()),
                     "control_roi": float(c.flat_stake_return.mean()), "paired_roi_difference": float(rd.mean()),
                     "paired_roi_diff_ci_2_5": lo, "paired_roi_diff_ci_97_5": hi,
                     "mean_abs_market_probability_gap": float(np.mean(abs(t.evaluated_market_probability.to_numpy()-c.evaluated_market_probability.to_numpy()))),
                     "mean_abs_paid_break_even_gap": float(np.mean(abs(t.evaluated_paid_break_even.to_numpy()-c.evaluated_paid_break_even.to_numpy()))),
                     "mean_abs_quote_lead_gap_minutes": float(np.mean(abs(t.quote_lead_minutes.to_numpy()-c.quote_lead_minutes.to_numpy()))),
                     "market_probability_common_support_low": float(overlap_lo), "market_probability_common_support_high": float(overlap_hi),
                     "exact_match_failures": int(min(len(treated), len(control)) - len(t)),
                     "support_note": "LIMITED_CONTROL_POOL" if len(t) < len(treated) else "FULL_TREATED_SUPPORT"})
    return pd.DataFrame(rows)


def control_table(ledger: pd.DataFrame, a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    cohorts = {
        "ALL_MARKET_STRONG_SIDES": a,
        "MARKET_STRONG_MODEL_NOT_STRONG": a[a.comparison_group.eq("MODEL_NOT_STRONG_MARKET_TEAM")],
        "MODEL_AND_MARKET_STRONG": a[a.comparison_group.eq("MODEL_ALSO_STRONG_SAME_TEAM")],
        "ALWAYS_MARKET_FAVORITE": evaluated(ledger, ledger.market_favorite_side),
        "FULL_MODEL_STRONG": b,
        "FULL_UNFILTERED_MODEL_SELECTIONS": evaluated(ledger, ledger.model_selected_side),
    }
    rows = []
    for label, g in cohorts.items(): rows.append({"control": label, **econ(g)})
    return pd.DataFrame(rows)


def forecast_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    p = np.clip(np.asarray(p, float), 1e-9, 1-1e-9); y = np.asarray(y, int)
    cal_i, cal_s = strong.calibration(y, p)
    return {"rows": len(y), "brier": float(np.mean((p-y)**2)),
            "log_loss": float(np.mean(-y*np.log(p)-(1-y)*np.log(1-p))),
            "ece": strong.ece(y, p), "calibration_intercept": cal_i, "calibration_slope": cal_s,
            "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else np.nan}


def rolling_information(ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    d = ledger.copy().sort_values(["game_date", "game_id"])
    d["joint_indicator"] = d.strength_class.eq("JOINT_STRONG_SAME_SIDE").astype(int)
    dates = np.array(sorted(d.game_date.unique())); blocks = [x for x in np.array_split(dates[10:], 5) if len(x)]
    specs = {
        "MARKET_ALONE": lambda x: np.c_[logit(x.no_vig_home_probability)],
        "MARKET_PLUS_CONTINUOUS_MODEL": lambda x: np.c_[logit(x.no_vig_home_probability), logit(x.home_win_probability)],
        "MARKET_PLUS_FIXED_STRONG_AGREEMENT": lambda x: np.c_[logit(x.no_vig_home_probability), x.joint_indicator],
        "MARKET_MODEL_PREDECLARED_INTERACTION": lambda x: np.c_[logit(x.no_vig_home_probability), logit(x.home_win_probability), logit(x.no_vig_home_probability)*logit(x.home_win_probability)],
        "MODEL_ALONE": lambda x: np.c_[logit(x.home_win_probability)],
    }
    preds, coefs = [], []
    for fold, td in enumerate(blocks, 1):
        tr, te = d[d.game_date.lt(td[0])], d[d.game_date.isin(td)]
        for name, fn in specs.items():
            fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(fn(tr), tr.home_win)
            p = fit.predict_proba(fn(te))[:, 1]
            for (_, r), value in zip(te.iterrows(), p):
                preds.append({"game_date": r.game_date, "game_id": int(r.game_id), "fold": fold,
                              "forecast": name, "home_win": int(r.home_win), "probability": float(value),
                              "market_strong": r.market_strong_side != "NONE", "strength_class": r.strength_class})
            coefs.append({"forecast": name, "fold": fold, "train_start": tr.game_date.min(), "train_end": tr.game_date.max(),
                          "test_start": te.game_date.min(), "test_end": te.game_date.max(), "train_rows": len(tr), "test_rows": len(te),
                          "intercept": float(fit.intercept_[0]), "coefficients": json.dumps([float(v) for v in fit.coef_[0]])})
    pred = pd.DataFrame(preds)
    rows, diffs = [], []
    for scope, z in (("ALL", pred), ("MARKET_STRONG", pred[pred.market_strong])):
        wide = z.pivot(index=["game_date", "game_id", "home_win"], columns="forecast", values="probability").reset_index()
        y = wide.home_win.to_numpy()
        for name in specs:
            rows.append({"scope": scope, "forecast": name, **forecast_metrics(y, wide[name].to_numpy())})
            if name != "MARKET_ALONE":
                base, alt = wide.MARKET_ALONE.to_numpy(), wide[name].to_numpy()
                for score in ("BRIER", "LOG_LOSS"):
                    vals = (alt-y)**2-(base-y)**2 if score == "BRIER" else (-y*np.log(alt)-(1-y)*np.log(1-alt))-(-y*np.log(base)-(1-y)*np.log(1-base))
                    lo, hi = cluster_ci(wide.game_date, vals)
                    diffs.append({"scope": scope, "forecast": name, "score": score, "difference_minus_market": float(vals.mean()),
                                  "ci_2_5": lo, "ci_97_5": hi, "rows": len(wide), "date_clusters": wide.game_date.nunique()})
    return pd.DataFrame(rows), pd.DataFrame(diffs), pd.DataFrame(coefs)


def attach_fixed_cohort(quotes: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    fixed = ledger[["game_id", "strength_class", "model_strong_side", "market_strong_side", "model_selected_side",
                    "series_proxy_state"]].rename(columns={
                        "strength_class": "pinnacle_fixed_strength_class",
                        "model_strong_side": "pinnacle_fixed_model_strong_side",
                        "market_strong_side": "pinnacle_fixed_market_strong_side",
                        "model_selected_side": "pinnacle_fixed_model_selected_side",
                    })
    d = classify(quotes.drop(columns=["series_proxy_state"], errors="ignore").merge(fixed, on="game_id", how="inner", validate="many_to_one"))
    side = np.where(d.pinnacle_fixed_strength_class.eq("MARKET_STRONG_ONLY"), d.pinnacle_fixed_market_strong_side,
                    np.where(d.pinnacle_fixed_model_strong_side.ne("NONE"), d.pinnacle_fixed_model_strong_side, d.pinnacle_fixed_model_selected_side))
    # Restore the primary Pinnacle side definitions after classify created target-book variants.
    d["fixed_evaluated_side"] = side
    d["home_win"] = np.where(d.model_selected_side.eq("HOME"), d.selected_win, 1-d.selected_win).astype(int)
    return evaluated(d, d.fixed_evaluated_side)


def book_economics(designated: pd.DataFrame, ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    own_rows = []
    for book in ("pinnacle", BETONLINE):
        b = classify(designated[designated.sportsbook.eq(book)].copy())
        if b.empty: continue
        side = np.where(b.strength_class.eq("MARKET_STRONG_ONLY"), b.market_strong_side,
                        np.where(b.model_strong_side.ne("NONE"), b.model_strong_side, b.model_selected_side))
        b = evaluated(b, pd.Series(side, index=b.index)); b["price_band"] = b.evaluated_american_price.map(strong.price_band)
        for cls in CLASS_ORDER:
            g = b[b.strength_class.eq(cls)]; own_rows.append({"sportsbook": book, "cohort_authority": "TARGET_BOOK_OWN_CLASS", "strength_class": cls, "scope": "ALL", **econ(g)})
            for band, z in g.groupby("price_band", sort=True):
                own_rows.append({"sportsbook": book, "cohort_authority": "TARGET_BOOK_OWN_CLASS", "strength_class": cls, "scope": f"PRICE_BAND:{band}", **econ(z)})
        decisive = {
            "DECISIVE_ALL_MARKET_STRONG": evaluated(b[b.market_strong_side.ne("NONE")], b.loc[b.market_strong_side.ne("NONE"), "market_strong_side"]),
            "DECISIVE_ALL_MODEL_STRONG": evaluated(b[b.model_strong_side.ne("NONE")], b.loc[b.model_strong_side.ne("NONE"), "model_strong_side"]),
        }
        for label, g in decisive.items():
            g["price_band"] = g.evaluated_american_price.map(strong.price_band)
            own_rows.append({"sportsbook": book, "cohort_authority": "TARGET_BOOK_OWN_CLASS", "strength_class": label, "scope": "ALL", **econ(g)})
            for band, z in g.groupby("price_band", sort=True):
                own_rows.append({"sportsbook": book, "cohort_authority": "TARGET_BOOK_OWN_CLASS", "strength_class": label, "scope": f"PRICE_BAND:{band}", **econ(z)})
    transfer = attach_fixed_cohort(designated, ledger)
    fixed_rows = []
    for book in ("pinnacle", BETONLINE):
        b = transfer[transfer.sportsbook.eq(book)].copy(); b["price_band"] = b.evaluated_american_price.map(strong.price_band)
        for cls in CLASS_ORDER:
            g = b[b.pinnacle_fixed_strength_class.eq(cls)]
            fixed_rows.append({"sportsbook": book, "cohort_authority": "FIXED_PINNACLE_CLASS_AND_SIDE", "strength_class": cls, "scope": "ALL", **econ(g)})
            for band, z in g.groupby("price_band", sort=True):
                fixed_rows.append({"sportsbook": book, "cohort_authority": "FIXED_PINNACLE_CLASS_AND_SIDE", "strength_class": cls, "scope": f"PRICE_BAND:{band}", **econ(z)})
        for label, classes in {
            "DECISIVE_ALL_MARKET_STRONG": ["JOINT_STRONG_SAME_SIDE", "MARKET_STRONG_ONLY"],
            "DECISIVE_ALL_MODEL_STRONG": ["JOINT_STRONG_SAME_SIDE", "MODEL_STRONG_ONLY"],
        }.items():
            g = b[b.pinnacle_fixed_strength_class.isin(classes)].copy()
            fixed_rows.append({"sportsbook": book, "cohort_authority": "FIXED_PINNACLE_CLASS_AND_SIDE", "strength_class": label, "scope": "ALL", **econ(g)})
            for band, z in g.groupby("price_band", sort=True):
                fixed_rows.append({"sportsbook": book, "cohort_authority": "FIXED_PINNACLE_CLASS_AND_SIDE", "strength_class": label, "scope": f"PRICE_BAND:{band}", **econ(z)})
    return pd.DataFrame(own_rows + fixed_rows), transfer


def stage_quotes(quotes: pd.DataFrame, ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = attach_fixed_cohort(quotes, ledger).sort_values(["sportsbook", "game_id", "captured_dt", "canonical_market_identity"])
    stages = []
    for (book, gid), g in d.groupby(["sportsbook", "game_id"], sort=True):
        g = g.sort_values(["captured_dt", "canonical_market_identity"])
        candidates: dict[str, pd.DataFrame] = {
            "FIRST_TRUSTWORTHY_PREGAME": g.head(1),
            "NEAREST_AFTER_IMMUTABLE_MODEL": g[g.captured_dt.ge(pd.to_datetime(g.prediction_timestamp_utc.iloc[0], utc=True))].head(1),
            "DESIGNATED_DAILY_MARKET_SNAPSHOT": g.tail(1),
            "NEAREST_VALID_WITHIN_30_MINUTES": g[g.quote_lead_minutes.le(30)].tail(1),
            "LATEST_TRUSTWORTHY_PREGAME": g.tail(1),
        }
        for stage, z in candidates.items():
            if len(z): stages.append(z.assign(price_stage=stage))
    s = pd.concat(stages, ignore_index=True) if stages else pd.DataFrame()
    order = ["FIRST_TRUSTWORTHY_PREGAME", "NEAREST_AFTER_IMMUTABLE_MODEL", "DESIGNATED_DAILY_MARKET_SNAPSHOT", "NEAREST_VALID_WITHIN_30_MINUTES", "LATEST_TRUSTWORTHY_PREGAME"]
    rows = []
    for book in ("pinnacle", BETONLINE):
        for cls in CLASS_ORDER:
            eligible = ledger[ledger.strength_class.eq(cls)].game_id.nunique()
            previous = None
            for stage in order:
                g = s[(s.sportsbook.eq(book)) & s.pinnacle_fixed_strength_class.eq(cls) & s.price_stage.eq(stage)].copy()
                current_ids = set(g.game_id)
                paired_move = np.nan; pairs = 0
                if previous is not None and not g.empty:
                    pair = previous[["game_id", "evaluated_paid_break_even"]].merge(g[["game_id", "evaluated_paid_break_even"]], on="game_id", suffixes=("_prior", "_current"))
                    pairs = len(pair); paired_move = float((pair.evaluated_paid_break_even_current-pair.evaluated_paid_break_even_prior).mean()) if pairs else np.nan
                rows.append({"sportsbook": book, "strength_class": cls, "stage": stage, "eligible_fixed_games": eligible,
                             "coverage": len(g), "coverage_rate": len(g)/eligible if eligible else np.nan,
                             **econ(g), "mean_implied_probability": g.evaluated_paid_break_even.mean() if len(g) else np.nan,
                             "mean_no_vig_probability": g.evaluated_market_probability.mean() if len(g) else np.nan,
                             "paired_with_previous_stage": pairs, "paid_break_even_movement_from_previous": paired_move,
                             "unavailable_not_substituted": eligible-len(current_ids),
                             "realized_return_state": "UNAVAILABLE" if g.empty else "PRESERVED_POSITIVE_IN_SAMPLE" if g.flat_stake_return.mean() > 0 else "NOT_PRESERVED_OR_PRICED_AWAY"})
                previous = g
    return s, pd.DataFrame(rows)


def movement_summary(stages: pd.DataFrame) -> pd.DataFrame:
    stages = stages[stages.sportsbook.isin(["pinnacle", BETONLINE])].copy()
    first = stages[stages.price_stage.eq("FIRST_TRUSTWORTHY_PREGAME")]
    last = stages[stages.price_stage.eq("LATEST_TRUSTWORTHY_PREGAME")]
    p = first.merge(last, on=["sportsbook", "game_id", "pinnacle_fixed_strength_class"], suffixes=("_first", "_last"))
    if p.empty: return pd.DataFrame()
    p["paid_break_even_movement"] = p.evaluated_paid_break_even_last-p.evaluated_paid_break_even_first
    p["no_vig_movement"] = p.evaluated_market_probability_last-p.evaluated_market_probability_first
    p["model_side_no_vig_movement"] = np.where(
        p.fixed_evaluated_side_first.eq(p.model_selected_side_first), p.no_vig_movement, -p.no_vig_movement)
    p["movement_direction"] = np.select([p.no_vig_movement.gt(0), p.no_vig_movement.lt(0)], ["TOWARD_EVALUATED_SIDE", "AWAY_FROM_EVALUATED_SIDE"], default="FLAT")
    rows = []
    for (book, cls), g in p.groupby(["sportsbook", "pinnacle_fixed_strength_class"], sort=True):
        rows.append({"sportsbook": book, "strength_class": cls, "paired_games": len(g),
                     "mean_paid_break_even_movement": g.paid_break_even_movement.mean(), "mean_no_vig_movement": g.no_vig_movement.mean(),
                     "mean_model_side_no_vig_movement": g.model_side_no_vig_movement.mean(),
                     "became_more_expensive_count": int(g.paid_break_even_movement.gt(0).sum()),
                     "became_less_expensive_count": int(g.paid_break_even_movement.lt(0).sum()),
                     "moved_toward_model_count": int(g.model_side_no_vig_movement.gt(0).sum()),
                     "moved_away_from_model_count": int(g.model_side_no_vig_movement.lt(0).sum()),
                     "toward_side_win_rate": g.loc[g.no_vig_movement.gt(0), "evaluated_win_last"].mean(),
                     "away_side_win_rate": g.loc[g.no_vig_movement.lt(0), "evaluated_win_last"].mean(),
                     "first_quote_roi": g.flat_stake_return_first.mean(), "latest_quote_roi": g.flat_stake_return_last.mean()})
    # Descriptive cross-book synchronized first/latest direction; no fillability inference.
    pin = p[p.sportsbook.eq("pinnacle")][["game_id", "no_vig_movement"]]
    bol = p[p.sportsbook.eq(BETONLINE)][["game_id", "no_vig_movement"]]
    pair = pin.merge(bol, on="game_id", suffixes=("_pinnacle", "_betonline"))
    for cls in ["ALL_FIXED_CLASSES", *CLASS_ORDER]:
        pin_cls = p[p.sportsbook.eq("pinnacle")]
        bol_cls = p[p.sportsbook.eq(BETONLINE)]
        if cls != "ALL_FIXED_CLASSES":
            pin_cls = pin_cls[pin_cls.pinnacle_fixed_strength_class.eq(cls)]
            bol_cls = bol_cls[bol_cls.pinnacle_fixed_strength_class.eq(cls)]
        pair = pin_cls[["game_id", "no_vig_movement"]].merge(
            bol_cls[["game_id", "no_vig_movement"]], on="game_id", suffixes=("_pinnacle", "_betonline"))
        rows.append({"sportsbook": "pinnacle_vs_betonline", "strength_class": cls, "paired_games": len(pair),
                     "mean_paid_break_even_movement": np.nan, "mean_no_vig_movement": np.nan,
                     "mean_model_side_no_vig_movement": np.nan,
                     "became_more_expensive_count": np.nan, "became_less_expensive_count": np.nan,
                     "moved_toward_model_count": np.nan, "moved_away_from_model_count": np.nan,
                     "toward_side_win_rate": np.nan, "away_side_win_rate": np.nan,
                     "first_quote_roi": np.nan, "latest_quote_roi": np.nan,
                     "same_direction_rate": float((np.sign(pair.no_vig_movement_pinnacle)==np.sign(pair.no_vig_movement_betonline)).mean()) if len(pair) else np.nan,
                     "movement_correlation": pair.no_vig_movement_pinnacle.corr(pair.no_vig_movement_betonline) if len(pair)>1 else np.nan,
                     "interpretation": "DESCRIPTIVE_CAPTURED_MOVEMENT_NOT_EXECUTION_OR_LEAD_LAG_PROOF"})
    return pd.DataFrame(rows)


def stability(ledger: pd.DataFrame) -> pd.DataFrame:
    median = str(sorted(ledger.game_date.unique())[len(ledger.game_date.unique())//2])
    scopes = {
        "ORIGINAL_CHARACTERIZED_PERIOD": ledger.game_date.le(ORIGINAL_END),
        "LATER_AUGUST_CONFIRMATION": ledger.game_date.gt(ORIGINAL_END) & ledger.game_date.lt("2026-09-01"),
        "PRIOR_SEPTEMBER_EXTENSION": ledger.game_date.str.startswith("2026-09"),
        "NEWLY_RESOLVED_AFTER_PRIOR_FREEZE": ledger.game_date.gt(PRIOR_FREEZE),
        "AUGUST": ledger.game_date.str.startswith("2026-08"), "SEPTEMBER": ledger.game_date.str.startswith("2026-09"),
        "DATE_FIRST_HALF": ledger.game_date.lt(median), "DATE_SECOND_HALF": ledger.game_date.ge(median),
    }
    rows = []
    for scope, mask in scopes.items():
        for cls in CLASS_ORDER:
            g = ledger[mask & ledger.strength_class.eq(cls)]
            rows.append({"axis": "FIXED_PERIOD", "level": scope, "strength_class": cls, **econ(g)})
    for cls in CLASS_ORDER:
        g = ledger[ledger.strength_class.eq(cls)]
        for day in sorted(g.game_date.unique()): rows.append({"axis": "LEAVE_ONE_DATE_OUT", "level": str(day), "strength_class": cls, **econ(g[g.game_date.ne(day)])})
        for state in sorted(g.series_proxy_state.dropna().unique()): rows.append({"axis": "LEAVE_ONE_SERIES_PROXY_GROUP_OUT", "level": str(state), "strength_class": cls, **econ(g[g.series_proxy_state.ne(state)])})
    return pd.DataFrame(rows)


def series_diagnostic(predictions: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    full = predictions.merge(strong.add_series_proxy(predictions), on="game_id", how="left", validate="one_to_one")
    full = full[full.selected_model_probability.gt(BOUNDARY)].copy()
    d = evaluated(ledger[ledger.model_strong_side.ne("NONE")], ledger.loc[ledger.model_strong_side.ne("NONE"), "model_strong_side"]).copy()
    d["continuation"] = d.series_proxy_state.eq("SERIES_PROXY_CONTINUATION").astype(int)
    d["joint"] = d.strength_class.eq("JOINT_STRONG_SAME_SIDE").astype(int)
    rows = []
    for state, g in full.groupby("series_proxy_state", sort=True):
        rows.append({"row_type": "FULL_MODEL_STRONG_FIXED_PRIOR_AXIS", "series_proxy_state": state,
                     "unique_games": len(g), "wins": int(g.selected_win.sum()), "losses": int(len(g)-g.selected_win.sum()),
                     "win_rate": g.selected_win.mean(), "claim_limit": "FIXED_PRIOR_SERIES_PROXY; INCLUDES GAMES WITHOUT PINNACLE_PRICE"})
    for state, g in d.groupby("series_proxy_state", sort=True): rows.append({"row_type": "PINNACLE_PRICED_RAW", "series_proxy_state": state, **econ(g)})
    X = np.c_[logit(d.evaluated_market_probability), logit(d.evaluated_paid_break_even), d.evaluated_home, d.joint, d.continuation]
    fit = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000).fit(X, d.evaluated_win)
    rows.append({"row_type": "ADJUSTED_LOGISTIC_DIAGNOSTIC", "series_proxy_state": "CONTINUATION_COEFFICIENT",
                 "unique_games": len(d), "coefficient": float(fit.coef_[0, 4]), "odds_ratio": float(np.exp(fit.coef_[0, 4])),
                 "controls": "market_probability+paid_price+home_status+joint_strength", "claim_limit": "FULL_SAMPLE_DIAGNOSTIC_NOT_CAUSAL_OR_SELECTION_RULE"})
    return pd.DataFrame(rows)


def betonline_missingness(ledger: pd.DataFrame, transfer: pd.DataFrame) -> pd.DataFrame:
    have = set(transfer[transfer.sportsbook.eq(BETONLINE)].game_id)
    d = ledger.copy(); d["betonline_exact_price_available"] = d.game_id.isin(have)
    rows = []
    for available, g in d.groupby("betonline_exact_price_available", sort=True):
        rows.append({"betonline_exact_price_available": bool(available), "games": len(g),
                     "mean_max_market_strength": g[["no_vig_home_probability", "no_vig_away_probability"]].max(axis=1).mean(),
                     "mean_max_model_strength": g[["home_win_probability", "away_win_probability"]].max(axis=1).mean(),
                     "mean_pinnacle_quote_lead_minutes": g.quote_lead_minutes.mean(), "date_min": g.game_date.min(), "date_max": g.game_date.max(),
                     **{f"class_{c}": int(g.strength_class.eq(c).sum()) for c in CLASS_ORDER}})
    return pd.DataFrame(rows)


def render_report(s: dict[str, Any]) -> str:
    return f"""# MLB joint-strength incremental value and executability audit v1

## Decision

**{s['decision']}**

The 56-20 joint result is reproduced. Test A is 56-20 versus 31-20: its market-side universe correctly includes one additional game where a non-strong model leaned opposite the market-strong team; the prior model-selection-oriented quadrant was 31-19. The model did not improve rolling out-of-time market-only probability scores and matched outcome/ROI comparisons remain uncertain. The defensible interpretation is independent agreement with directional selection value that is not resolved as incremental economic value.

## Plain-language answers

Did the model contribute useful independent confirmation? **Not conclusively.** The joint cohort selected a better-observed subgroup of already-strong market favorites, but market probability remains the stronger information source and controls do not establish a model increment with interval support. The result primarily identifies teams the market already knew were strong, with a still-unresolved directional agreement signal.

Where and when was the price available? The +10.3% result is a **Pinnacle captured-price result** at the designated daily snapshot. The nearest captured post-prediction Pinnacle quote covered {s['postprediction_joint_pinnacle_games']} games and returned {s['postprediction_joint_pinnacle_roi']:+.1%}. The exact same fixed cohort has only {s['betonline_joint_exact_games']} designated BetOnline prices, returning {s['betonline_joint_roi_text']}; only {s['postprediction_joint_betonline_games']} were captured after the immutable prediction and they returned {s['postprediction_joint_betonline_roi']:+.1%}. This is too sparse and time-concentrated to establish transfer. Near-start (within 30 minutes) joint coverage is {s['near_start_joint_pinnacle']} Pinnacle games and {s['near_start_joint_betonline']} BetOnline games. A captured quote is descriptive evidence, not proof of availability or fillability to this user.

## Decisive findings

* Test A: model+market strong was {s['test_a_joint_record']} ({s['test_a_joint_win_rate']:.1%}) versus {s['test_a_control_record']} ({s['test_a_control_win_rate']:.1%}) for market-strong/model-not-strong.
* Test B reproduces 56-20 versus 26-22 for unchanged model-strong selections.
* Joint Pinnacle ROI was {s['pinnacle_joint_roi']:.1%}; its date-clustered 95% interval was [{s['pinnacle_joint_ci_low']:.1%}, {s['pinnacle_joint_ci_high']:.1%}].
* Model-after-market scoring: {s['probability_information_conclusion']}. The continuous model coefficient was negative in {s['continuous_model_negative_folds']}/5 rolling folds; the market coefficient was positive in {s['market_positive_folds']}/5.
* Market-probability matching: {s['matching_conclusion']} (paired ROI difference {s['matched_roi_difference']:+.1%}, 95% date-clustered interval [{s['matched_roi_ci_low']:+.1%}, {s['matched_roi_ci_high']:+.1%}]).
* BetOnline transfer: {s['betonline_transfer_conclusion']}.
* BetOnline missingness: exact overlap exists for 66/449 primary games and is confined to August 7–11. Available games had mean maximum market strength {s['betonline_available_market_strength']:.1%} versus {s['betonline_missing_market_strength']:.1%} when missing; calendar/source coverage dominates, so strength-dependent missingness is not established.
* Near-start survival: {s['near_start_conclusion']}.
* Joint line movement: Pinnacle paid break-even rose {s['joint_pinnacle_pbe_movement']:+.2%} from first to latest captured quote. BetOnline/Pinnacle joint movement direction agreement was {s['joint_crossbook_same_direction_text']} across {s['joint_crossbook_pairs']} exact games, descriptive only.
* Movement was not an outcome signal: joint games moving toward the selected side won {s['joint_toward_win_rate']:.1%}, versus {s['joint_away_win_rate']:.1%} moving away. Model-only prices were effectively flat toward the model ({s['model_only_model_side_movement']:+.2%}); market-only prices moved {s['market_only_model_side_movement']:+.2%} toward the model on average.
* Stability: joint strength was {s['august_joint_record']} ({s['august_joint_roi']:+.1%}) in August and {s['september_joint_record']} ({s['september_joint_roi']:+.1%}) in September; there were {s['new_after_freeze_games']} newly resolved games after the prior freeze, so nested updates are not independent confirmation.
* Series position: {s['series_conclusion']}. The adjusted continuation odds ratio was {s['series_continuation_adjusted_odds_ratio']:.2f} on the priced subset.
* Market making: **No.** Nothing here supports a market-making claim, production change, publication change, or wagering action.

## Guardrails and interpretation

The model STRONG boundary remains strictly greater than 0.60; exactly 0.60 is not strong. Market strength uses the same strict no-vig boundary. Conflicts remain a separate class; none can normally occur for coherent complementary two-way probabilities, but the class is retained and validated. Unique games, books, and quote timestamps remain separate. Matching is outcome-blind and reports incomplete common support. Fits are diagnostic, rolling-origin, and never score training games. No combined model is promoted.

Pinnacle and BetOnline are never substituted. The BetOnline transfer cohort is fixed by the primary Pinnacle class and same evaluated team, preventing retrospective book shopping. First, post-prediction, designated, within-30-minute, and latest stages are reported separately; missing stages are not filled.

The prior first-series 37-13 versus continuation 55-32 split is preserved in the model-strong unique-game diagnostic. Its adjusted coefficient controls for market probability, paid price, home/away, and joint status, but remains descriptive rather than a new filter.

## Continuing evidence

`observe_mlb_moneyline_strength_classes_shadow_v1.py` is a manual, append-only observer for every future game and every fixed class. It writes immutable predictions first, then separate sportsbook quotes, then outcomes. It is not scheduled, published, uploaded, or connected to wagering.
"""


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import hashlib, json
from pathlib import Path
import pandas as pd
p=Path(__file__).resolve().parent
errors=[]
s=json.loads((p/'summary.json').read_text())
l=pd.read_csv(p/'unique_game_strength_class_ledger.csv')
if l.game_id.nunique()!=len(l): errors.append('ledger_not_unique')
if set(l.strength_class)-set(['JOINT_STRONG_SAME_SIDE','MODEL_STRONG_ONLY','MARKET_STRONG_ONLY','STRONG_CONFLICT','NEITHER_STRONG']): errors.append('unknown_class')
if s['strong_boundary']!='probability > 0.60 strict': errors.append('boundary_changed')
if s['joint_record']!='56-20': errors.append('joint_not_reproduced')
if s['model_only_record']!='26-22': errors.append('model_only_not_reproduced')
for row in (p/'sha256_manifest.txt').read_text().splitlines():
    expected,name=row.split('  ',1)
    if hashlib.sha256((p/name).read_bytes()).hexdigest()!=expected: errors.append('hash:'+name)
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors,'unique_games':len(l)},sort_keys=True))
raise SystemExit(bool(errors))
'''


def run(output: Path, cutoff_arg: str) -> dict[str, Any]:
    cutoff = prior.latest_resolved_cutoff() if cutoff_arg == "latest" else cutoff_arg
    if cutoff != PRIOR_FREEZE: raise ValueError(f"v1 audit is frozen at {PRIOR_FREEZE}; got {cutoff}")
    predictions, quotes, designated = across.load_population(cutoff)
    output.mkdir(parents=True, exist_ok=True)
    ledger = primary_ledger(predictions, designated)
    a, b, decisive = test_panels(ledger)
    matched = pd.concat([
        matched_pairs(a, "TEST_A_WITHIN_MARKET_STRONG", "MODEL_ALSO_STRONG_SAME_TEAM", "MODEL_NOT_STRONG_MARKET_TEAM"),
        matched_pairs(b, "TEST_B_WITHIN_MODEL_STRONG", "MARKET_ALSO_STRONG_SAME_TEAM", "MARKET_NOT_STRONG_MODEL_TEAM")], ignore_index=True)
    controls = control_table(ledger, a, b)
    forecasts, score_diffs, fold_coeffs = rolling_information(ledger)
    books, transfer = book_economics(designated, ledger)
    stages, paths = stage_quotes(quotes, ledger)
    movement = movement_summary(stages)
    stable = stability(ledger)
    series = series_diagnostic(predictions, ledger)
    missing = betonline_missingness(ledger, transfer)

    joint = a[a.comparison_group.eq("MODEL_ALSO_STRONG_SAME_TEAM")]; market_only = a[a.comparison_group.eq("MODEL_NOT_STRONG_MARKET_TEAM")]
    model_only = b[b.comparison_group.eq("MARKET_NOT_STRONG_MODEL_TEAM")]
    pin_joint = books[(books.sportsbook.eq("pinnacle")) & books.cohort_authority.eq("FIXED_PINNACLE_CLASS_AND_SIDE") & books.strength_class.eq("JOINT_STRONG_SAME_SIDE") & books.scope.eq("ALL")].iloc[0]
    bol_joint = books[(books.sportsbook.eq(BETONLINE)) & books.cohort_authority.eq("FIXED_PINNACLE_CLASS_AND_SIDE") & books.strength_class.eq("JOINT_STRONG_SAME_SIDE") & books.scope.eq("ALL")].iloc[0]
    near_pin = paths[(paths.sportsbook.eq("pinnacle")) & paths.strength_class.eq("JOINT_STRONG_SAME_SIDE") & paths.stage.eq("NEAREST_VALID_WITHIN_30_MINUTES")].iloc[0]
    near_bol = paths[(paths.sportsbook.eq(BETONLINE)) & paths.strength_class.eq("JOINT_STRONG_SAME_SIDE") & paths.stage.eq("NEAREST_VALID_WITHIN_30_MINUTES")].iloc[0]
    post_pin = paths[(paths.sportsbook.eq("pinnacle")) & paths.strength_class.eq("JOINT_STRONG_SAME_SIDE") & paths.stage.eq("NEAREST_AFTER_IMMUTABLE_MODEL")].iloc[0]
    post_bol = paths[(paths.sportsbook.eq(BETONLINE)) & paths.strength_class.eq("JOINT_STRONG_SAME_SIDE") & paths.stage.eq("NEAREST_AFTER_IMMUTABLE_MODEL")].iloc[0]
    oot_market = score_diffs[(score_diffs.scope.eq("ALL")) & score_diffs.forecast.eq("MARKET_PLUS_CONTINUOUS_MODEL")]
    mp_match = matched[(matched.test.eq("TEST_A_WITHIN_MARKET_STRONG")) & matched.match.eq("MARKET_PROBABILITY_MATCHED")].iloc[0]
    pin_move = movement[(movement.sportsbook.eq("pinnacle")) & movement.strength_class.eq("JOINT_STRONG_SAME_SIDE")].iloc[0]
    pin_model_only_move = movement[(movement.sportsbook.eq("pinnacle")) & movement.strength_class.eq("MODEL_STRONG_ONLY")].iloc[0]
    pin_market_only_move = movement[(movement.sportsbook.eq("pinnacle")) & movement.strength_class.eq("MARKET_STRONG_ONLY")].iloc[0]
    cross_move = movement[(movement.sportsbook.eq("pinnacle_vs_betonline")) & movement.strength_class.eq("JOINT_STRONG_SAME_SIDE")].iloc[0]
    miss_yes = missing[missing.betonline_exact_price_available.eq(True)].iloc[0]
    miss_no = missing[missing.betonline_exact_price_available.eq(False)].iloc[0]
    august_joint = stable[(stable.axis.eq("FIXED_PERIOD")) & stable.level.eq("AUGUST") & stable.strength_class.eq("JOINT_STRONG_SAME_SIDE")].iloc[0]
    september_joint = stable[(stable.axis.eq("FIXED_PERIOD")) & stable.level.eq("SEPTEMBER") & stable.strength_class.eq("JOINT_STRONG_SAME_SIDE")].iloc[0]
    new_joint = stable[(stable.axis.eq("FIXED_PERIOD")) & stable.level.eq("NEWLY_RESOLVED_AFTER_PRIOR_FREEZE") & stable.strength_class.eq("JOINT_STRONG_SAME_SIDE")].iloc[0]
    series_adjusted = series[series.row_type.eq("ADJUSTED_LOGISTIC_DIAGNOSTIC")].iloc[0]
    probability_adds = bool((oot_market.difference_minus_market < 0).all() and (oot_market.ci_97_5 < 0).all())
    combo_fold = fold_coeffs[fold_coeffs.forecast.eq("MARKET_PLUS_CONTINUOUS_MODEL")]
    combo_values = [json.loads(x) for x in combo_fold.coefficients]
    match_supported = bool(mp_match.paired_win_rate_difference > 0 and mp_match.paired_roi_diff_ci_2_5 > 0)
    decision = "MODEL_ADDS_VALUE_WITHIN_MARKET_STRONG" if probability_adds and match_supported else "MODEL_MARKET_AGREEMENT_DIRECTIONAL_BUT_UNRESOLVED"
    if len(joint) != 76 or int(joint.evaluated_win.sum()) != 56: decision = "JOINT_STRENGTH_NOT_REPRODUCED"
    summary = {
        "audit": "MLB_JOINT_STRENGTH_INCREMENTAL_VALUE_EXECUTABILITY_V1", "resolved_cutoff": cutoff,
        "decision": decision, "strong_boundary": "probability > 0.60 strict", "joint_record": f"{int(joint.evaluated_win.sum())}-{len(joint)-int(joint.evaluated_win.sum())}",
        "model_only_record": f"{int(model_only.evaluated_win.sum())}-{len(model_only)-int(model_only.evaluated_win.sum())}",
        "test_a_joint_record": f"{int(joint.evaluated_win.sum())}-{len(joint)-int(joint.evaluated_win.sum())}", "test_a_joint_win_rate": joint.evaluated_win.mean(),
        "test_a_control_record": f"{int(market_only.evaluated_win.sum())}-{len(market_only)-int(market_only.evaluated_win.sum())}", "test_a_control_win_rate": market_only.evaluated_win.mean(),
        "pinnacle_joint_roi": pin_joint.roi, "pinnacle_joint_ci_low": pin_joint.roi_ci_2_5, "pinnacle_joint_ci_high": pin_joint.roi_ci_97_5,
        "betonline_joint_exact_games": int(bol_joint.unique_games), "betonline_joint_roi": bol_joint.roi,
        "betonline_joint_record": f"{int(bol_joint.wins)}-{int(bol_joint.losses)}",
        "betonline_joint_roi_text": "unavailable" if pd.isna(bol_joint.roi) else f"{bol_joint.roi:.1%}",
        "near_start_joint_pinnacle": int(near_pin.coverage), "near_start_joint_betonline": int(near_bol.coverage),
        "postprediction_joint_pinnacle_games": int(post_pin.coverage), "postprediction_joint_pinnacle_roi": post_pin.roi,
        "postprediction_joint_betonline_games": int(post_bol.coverage), "postprediction_joint_betonline_roi": post_bol.roi,
        "prediction_rows": len(predictions), "pinnacle_exact_game_rows": len(ledger),
        "pinnacle_missing_game_rows": len(predictions)-len(ledger),
        "probability_information_conclusion": "NO_INTERVAL_SUPPORTED_IMPROVEMENT_OVER_MARKET_ONLY" if not probability_adds else "INTERVAL_SUPPORTED_IMPROVEMENT",
        "continuous_model_negative_folds": sum(v[1] < 0 for v in combo_values),
        "market_positive_folds": sum(v[0] > 0 for v in combo_values),
        "matching_conclusion": "DIRECTIONAL_NOT_INTERVAL_SUPPORTED" if mp_match.paired_win_rate_difference > 0 and not match_supported else "INTERVAL_SUPPORTED" if match_supported else "NOT_DIRECTIONALLY_BETTER",
        "matched_roi_difference": mp_match.paired_roi_difference, "matched_roi_ci_low": mp_match.paired_roi_diff_ci_2_5,
        "matched_roi_ci_high": mp_match.paired_roi_diff_ci_97_5,
        "joint_pinnacle_pbe_movement": pin_move.mean_paid_break_even_movement,
        "joint_toward_win_rate": pin_move.toward_side_win_rate, "joint_away_win_rate": pin_move.away_side_win_rate,
        "model_only_model_side_movement": pin_model_only_move.mean_model_side_no_vig_movement,
        "market_only_model_side_movement": pin_market_only_move.mean_model_side_no_vig_movement,
        "joint_crossbook_pairs": int(cross_move.paired_games),
        "joint_crossbook_same_direction_text": "unavailable" if pd.isna(cross_move.same_direction_rate) else f"{cross_move.same_direction_rate:.1%}",
        "betonline_available_market_strength": miss_yes.mean_max_market_strength,
        "betonline_missing_market_strength": miss_no.mean_max_market_strength,
        "august_joint_record": f"{int(august_joint.wins)}-{int(august_joint.losses)}", "august_joint_roi": august_joint.roi,
        "september_joint_record": f"{int(september_joint.wins)}-{int(september_joint.losses)}", "september_joint_roi": september_joint.roi,
        "new_after_freeze_games": int(new_joint.unique_games),
        "series_continuation_adjusted_odds_ratio": series_adjusted.odds_ratio,
        "betonline_transfer_conclusion": "INSUFFICIENT_TIME_CONCENTRATED_EXACT_PRICE_EVIDENCE" if int(bol_joint.unique_games)<30 else "POSITIVE" if bol_joint.roi>0 else "NOT_POSITIVE",
        "near_start_conclusion": "INSUFFICIENT_EXACT_WITHIN_30_MINUTE_COVERAGE" if int(near_pin.coverage)<30 else "REPORTED_WITHOUT_FILLABILITY_CLAIM",
        "series_conclusion": "DESCRIPTIVE_PORTION_ONLY; ADJUSTED DIAGNOSTIC DOES_NOT_AUTHORIZE_A_FILTER",
        "supports_market_making": False, "price_authority": "captured pregame sportsbook quotes; no substitutions",
        "constraints": {"refit": False, "threshold_change": False, "production_change": False, "wagering": False, "book_shopping": False}}

    ledger_cols = ["game_date", "game_id", "scheduled_start_utc", "home_team", "away_team", "home_win_probability", "away_win_probability", "home_win",
                   "model_strong_side", "market_strong_side", "strength_class", "evaluated_side", "evaluated_team", "evaluated_win",
                   "evaluated_model_probability", "evaluated_market_probability", "evaluated_american_price", "evaluated_decimal_price", "evaluated_paid_break_even",
                   "flat_stake_return", "captured_at_utc", "provider_market_updated_at_utc", "quote_lead_minutes", "canonical_market_identity", "prediction_payload_sha256",
                   "market_payload_sha256", "series_proxy_state"]
    write_csv(ledger[ledger_cols], output / "unique_game_strength_class_ledger.csv")
    write_csv(decisive, output / "decisive_test_a_test_b.csv")
    write_csv(matched, output / "matched_control_results.csv")
    write_csv(controls, output / "required_control_cohorts.csv")
    write_csv(forecasts, output / "out_of_time_information_comparison.csv")
    write_csv(score_diffs, output / "out_of_time_score_differences.csv")
    write_csv(fold_coeffs, output / "rolling_origin_folds_and_coefficients.csv")
    write_csv(books, output / "book_specific_economics.csv")
    write_csv(paths, output / "fixed_timestamp_price_path.csv")
    write_csv(movement, output / "line_movement_summary.csv")
    write_csv(stable, output / "temporal_stability.csv")
    write_csv(series, output / "fixed_series_proxy_diagnostic.csv")
    write_csv(missing, output / "betonline_missingness.csv")
    write_csv(pd.DataFrame([
        {"population": "ALL_RESOLVED_IMMUTABLE_PREDICTIONS", "games": len(predictions), "classified": False, "reason": "NO_SINGLE_BOOK_REQUIRED"},
        {"population": "PRIMARY_PINNACLE_EXACT_GAME_CLASSIFICATION", "games": len(ledger), "classified": True, "reason": "LATEST_TRUSTWORTHY_PREGAME_QUOTE"},
        {"population": "RESOLVED_WITHOUT_PRIMARY_PINNACLE_PRICE", "games": len(predictions)-len(ledger), "classified": False, "reason": "NO_PRICE_INVENTED"},
    ]), output / "coverage_accounting.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default)+"\n")
    (output / "main_report.md").write_text(render_report(summary))
    if not OBSERVER_SOURCE.exists(): raise RuntimeError("quadrant observer source is missing")
    shutil.copyfile(OBSERVER_SOURCE, output / "moneyline_strength_classes_shadow_observer.py")
    (output / "observer_contract.json").write_text(json.dumps({"manual_only": True, "append_only": True, "baseline_cutoff": PRIOR_FREEZE,
        "all_fixed_classes": CLASS_ORDER, "model_prediction_first": True, "sportsbook_quotes_separate": True,
        "outcomes_later": True, "scheduler": False, "publishing": False, "wagering": False,
        "source_path": str(OBSERVER_SOURCE.relative_to(ROOT)), "source_sha256": sha(OBSERVER_SOURCE)}, indent=2, sort_keys=True)+"\n")
    rerun = f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m backend.mlb.scripts.audit_mlb_joint_strength_incremental_value_executability_v1 --resolved-cutoff {cutoff} --output {output.relative_to(ROOT)}'\n"
    (output / "rerun_command.txt").write_text(rerun)
    (output / "observer_rerun_command.txt").write_text(".venv/bin/python -m backend.mlb.scripts.observe_mlb_moneyline_strength_classes_shadow_v1 --through-date 2026-09-09 --checkpoint ORDINARY\n")
    (output / "validator.py").write_text(validator_text()); (output / "validator.py").chmod(0o755)
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--resolved-cutoff", default=PRIOR_FREEZE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(); output = args.output if args.output.is_absolute() else ROOT / args.output
    print(json.dumps(run(output, args.resolved_cutoff), indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__": main()
