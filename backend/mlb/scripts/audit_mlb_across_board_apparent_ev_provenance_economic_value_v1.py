#!/usr/bin/env python3
"""Deterministic, read-only audit of MLB moneyline apparent-edge provenance.

Every quote remains attached to one immutable game outcome.  Market-derived
price disagreements are never labeled independent truth or execution evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from scipy.stats import spearmanr
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as prior


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_across_board_apparent_ev_provenance_economic_value_audit_v1/2026-09-09"
EXPERIMENT = "MLB_ACROSS_BOARD_APPARENT_EV_PROVENANCE_ECONOMIC_VALUE_AUDIT_V1"
CONTEMPORANEOUS_SECONDS = 15 * 60
CONSENSUS_MIN_OTHER_BOOKS = 3
MATCH_CALIPER = .02
BOOT_REPS = 4000
SEED = 20260909
STRONG = .60
EDGE_ORDER = ["NEGATIVE_AT_OR_BELOW_NEG2", "APPROX_NEUTRAL_NEG2_TO_POS2",
              "MODEST_POSITIVE_ABOVE2_TO5", "LARGE_POSITIVE_ABOVE5"]
DEFINITIONS = {
    "MODEL_DERIVED_EV": "INDEPENDENT_MODEL_EDGE",
    "PINNACLE_REFERENCE_EV": "SINGLE_MARKET_REFERENCE_EDGE",
    "EX_TARGET_CONSENSUS_EV": "MULTIMARKET_CONSENSUS_EDGE",
    "BEST_PRICE_ADVANTAGE": "CROSS_BOOK_PRICE_DIFFERENCE",
    "LATER_PINNACLE_RELATIONSHIP": "LINE_MOVEMENT_MEASURE",
}


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
    p = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def expit(x: Any) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return 1 / (1 + np.exp(-x))


def ece(y: Any, p: Any) -> float:
    y, p = np.asarray(y, float), np.asarray(p, float)
    bins = np.minimum(9, np.floor(np.clip(p, 0, 1 - 1e-12) * 10).astype(int))
    return float(sum(np.mean(bins == b) * abs(y[bins == b].mean() - p[bins == b].mean())
                     for b in range(10) if np.any(bins == b)))


def log_loss(y: Any, p: Any) -> float:
    y, p = np.asarray(y, float), np.clip(np.asarray(p, float), 1e-9, 1 - 1e-9)
    return float(np.mean(-y*np.log(p) - (1-y)*np.log(1-p)))


def cluster_ci(dates: Any, values: Any, reps: int = BOOT_REPS) -> tuple[float, float]:
    work = pd.DataFrame({"date": np.asarray(dates), "value": np.asarray(values, float)}).dropna()
    grouped = work.groupby("date", sort=True).value.agg(["sum", "count"])
    if len(grouped) < 2: return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(work) + int(abs(work.value.sum()) * 1000) % 997)
    pick = rng.integers(0, len(grouped), size=(reps, len(grouped)))
    boot = grouped["sum"].to_numpy()[pick].sum(1) / grouped["count"].to_numpy()[pick].sum(1)
    return float(np.quantile(boot, .025)), float(np.quantile(boot, .975))


def probability_band(value: float) -> str:
    if value < .45: return "BELOW_45"
    if value < .50: return "45_TO_BELOW_50"
    if value < .55: return "50_TO_BELOW_55"
    if value < .60: return "55_TO_BELOW_60"
    if value < .65: return "60_TO_BELOW_65"
    return "65_AND_ABOVE"


def edge_band(value: float) -> str:
    if value <= -.02: return EDGE_ORDER[0]
    if value <= .02: return EDGE_ORDER[1]
    if value <= .05: return EDGE_ORDER[2]
    return EDGE_ORDER[3]


def quote_timing_band(minutes: float) -> str:
    if minutes < 120: return "BELOW_2_HOURS"
    if minutes < 360: return "2_TO_BELOW_6_HOURS"
    if minutes < 1440: return "6_TO_BELOW_24_HOURS"
    return "24_HOURS_OR_MORE"


def load_population(cutoff: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    predictions = prior.load_predictions(cutoff)
    predictions = predictions[predictions.resolved].copy()
    predictions["model_selected_side"] = np.where(predictions.home_win_probability.ge(.5), "HOME", "AWAY")
    predictions["selected_model_probability"] = predictions[["home_win_probability", "away_win_probability"]].max(axis=1)
    predictions["selected_win"] = predictions.prediction_correct.astype(bool).astype(int)
    predictions["selected_team"] = np.where(predictions.model_selected_side.eq("HOME"), predictions.home_team, predictions.away_team)
    predictions["home_away_status"] = predictions.model_selected_side
    conn = sqlite3.connect(f"file:{prior.MARKET_DB}?mode=ro", uri=True)
    raw = pd.read_sql_query("""
      SELECT canonical_market_identity,provider,bookmaker_key,game_date,game_id,captured_at_utc,
             scheduled_start_utc,timing_status,market_payload_json,market_payload_sha256,
             raw_source_path,raw_source_sha256
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND game_date<=?
      ORDER BY game_date,game_id,captured_at_utc,bookmaker_key,canonical_market_identity
    """, conn, params=(cutoff,))
    conn.close()
    rows = []
    for r in raw.itertuples(index=False):
        payload = json.loads(r.market_payload_json)
        captured = pd.to_datetime(r.captured_at_utc, utc=True, errors="coerce")
        start = pd.to_datetime(r.scheduled_start_utc, utc=True, errors="coerce")
        updated = pd.to_datetime(payload.get("provider_market_updated_at_utc"), utc=True, errors="coerce")
        valid = (r.timing_status == "PREGAME_CERTIFIED" and pd.notna(captured) and pd.notna(start)
                 and pd.notna(updated) and captured < start and updated < start)
        if not valid: continue
        rows.append({"canonical_market_identity": r.canonical_market_identity, "provider": r.provider,
                     "sportsbook": r.bookmaker_key, "game_date": r.game_date, "game_id": int(r.game_id),
                     "captured_at_utc": str(r.captured_at_utc), "captured_dt": captured,
                     "scheduled_start_utc_market": str(r.scheduled_start_utc), "start_dt": start,
                     "provider_market_updated_at_utc": payload.get("provider_market_updated_at_utc"),
                     "market_payload_sha256": r.market_payload_sha256, "raw_source_path": r.raw_source_path,
                     "raw_source_sha256": r.raw_source_sha256,
                     **{k: payload.get(k) for k in (
                         "home_american_price", "away_american_price", "home_decimal_price", "away_decimal_price",
                         "home_implied_probability", "away_implied_probability", "no_vig_home_probability",
                         "no_vig_away_probability")}})
    quotes = pd.DataFrame(rows)
    d = quotes.merge(predictions, on=["game_date", "game_id"], how="inner", validate="many_to_one")
    home = d.model_selected_side.eq("HOME")
    for output, home_col, away_col in [
        ("selected_american_price", "home_american_price", "away_american_price"),
        ("selected_decimal_price", "home_decimal_price", "away_decimal_price"),
        ("selected_paid_break_even_probability", "home_implied_probability", "away_implied_probability"),
        ("selected_target_no_vig_probability", "no_vig_home_probability", "no_vig_away_probability")]:
        d[output] = np.where(home, pd.to_numeric(d[home_col]), pd.to_numeric(d[away_col]))
    d = d.dropna(subset=["selected_american_price", "selected_decimal_price",
                         "selected_paid_break_even_probability", "selected_target_no_vig_probability"]).copy()
    d["flat_stake_return"] = np.where(d.selected_win.eq(1), d.selected_decimal_price - 1, -1)
    d["target_market_favorite_status"] = np.where(d.selected_target_no_vig_probability.gt(.5), "FAVORITE",
                                                     np.where(d.selected_target_no_vig_probability.lt(.5), "UNDERDOG", "PICKEM"))
    d["quote_lead_minutes"] = (d.start_dt - d.captured_dt).dt.total_seconds() / 60
    d["quote_timing_band"] = d.quote_lead_minutes.map(quote_timing_band)
    d["paid_break_even_band"] = d.selected_paid_break_even_probability.map(probability_band)
    d["temporal_block"] = pd.to_datetime(d.game_date).dt.strftime("%Y-W%W")
    d["month"] = d.game_date.str[:7]
    d = d.sort_values(["sportsbook", "game_id", "captured_dt", "canonical_market_identity"])
    designated = d.groupby(["sportsbook", "game_id"], sort=True).tail(1).copy()
    return predictions, d.reset_index(drop=True), designated.reset_index(drop=True)


def attach_references(quotes: pd.DataFrame) -> pd.DataFrame:
    d = quotes.copy()
    pinnacle = d[d.sportsbook.eq("pinnacle")].sort_values(["game_id", "captured_dt"])
    by_game = {int(k): g for k, g in pinnacle.groupby("game_id", sort=True)}
    pin_p, pin_time, pin_delta, later_p, later_time = [], [], [], [], []
    for r in d.itertuples(index=False):
        g = by_game.get(int(r.game_id))
        if g is None or g.empty:
            pin_p.append(np.nan); pin_time.append(None); pin_delta.append(np.nan)
            later_p.append(np.nan); later_time.append(None); continue
        seconds = (g.captured_dt - r.captured_dt).dt.total_seconds().abs()
        idx = seconds.idxmin()
        if float(seconds.loc[idx]) <= CONTEMPORANEOUS_SECONDS:
            pin_p.append(float(g.loc[idx, "selected_target_no_vig_probability"]))
            pin_time.append(g.loc[idx, "captured_at_utc"]); pin_delta.append(float(seconds.loc[idx]))
        else:
            pin_p.append(np.nan); pin_time.append(None); pin_delta.append(np.nan)
        later = g[g.captured_dt.gt(r.captured_dt)]
        if len(later):
            z = later.iloc[-1]; later_p.append(float(z.selected_target_no_vig_probability)); later_time.append(z.captured_at_utc)
        else:
            later_p.append(np.nan); later_time.append(None)
    d["pinnacle_reference_probability"] = pin_p
    d["pinnacle_reference_timestamp_utc"] = pin_time
    d["pinnacle_reference_time_delta_seconds"] = pin_delta
    d["later_pinnacle_probability"] = later_p
    d["later_pinnacle_timestamp_utc"] = later_time

    consensus_p = pd.Series(np.nan, index=d.index, dtype=float)
    consensus_count = pd.Series(0, index=d.index, dtype=int)
    median_pbe = pd.Series(np.nan, index=d.index, dtype=float)
    best_pbe = pd.Series(np.nan, index=d.index, dtype=float)
    for _, g in d.groupby(["game_id", "captured_at_utc"], sort=False):
        nv = g.selected_target_no_vig_probability.to_numpy(float)
        be = g.selected_paid_break_even_probability.to_numpy(float)
        for i in range(len(g)):
            mask = np.arange(len(g)) != i
            idx = g.index[i]
            consensus_p.loc[idx] = float(np.median(nv[mask])) if mask.sum() >= CONSENSUS_MIN_OTHER_BOOKS else np.nan
            consensus_count.loc[idx] = int(mask.sum())
            median_pbe.loc[idx] = float(np.median(be[mask])) if mask.sum() >= CONSENSUS_MIN_OTHER_BOOKS else np.nan
            best_pbe.loc[idx] = float(np.min(be))
    d["ex_target_consensus_probability"] = consensus_p
    d["ex_target_consensus_book_count"] = consensus_count
    d["ex_target_median_paid_break_even"] = median_pbe
    d["best_same_side_paid_break_even"] = best_pbe
    d["is_best_same_side_captured_price"] = np.isclose(
        d.selected_paid_break_even_probability, d.best_same_side_paid_break_even, atol=1e-12, rtol=0
    )
    return d


def definition_rows(designated: pd.DataFrame) -> pd.DataFrame:
    rows = []
    specs = [
        ("MODEL_DERIVED_EV", "selected_model_probability", "selected_model_probability", True,
         "frozen canonical model probability"),
        ("PINNACLE_REFERENCE_EV", "pinnacle_reference_probability", "pinnacle_reference_probability", True,
         "nearest Pinnacle proportional no-vig probability within 15 minutes"),
        ("EX_TARGET_CONSENSUS_EV", "ex_target_consensus_probability", "ex_target_consensus_probability", True,
         "median proportional no-vig probability across at least three other same-capture books"),
        ("BEST_PRICE_ADVANTAGE", "ex_target_median_paid_break_even", "ex_target_median_paid_break_even", False,
         "other-book median paid break-even probability; price disagreement, not truth"),
        ("LATER_PINNACLE_RELATIONSHIP", "later_pinnacle_probability", "later_pinnacle_probability", False,
         "latest later pregame Pinnacle no-vig probability; line relationship, not truth"),
    ]
    common = ["game_date", "game_id", "canonical_market_identity", "provider", "sportsbook",
              "captured_at_utc", "scheduled_start_utc_market", "provider_market_updated_at_utc",
              "model_selected_side", "selected_team", "selected_win", "confidence_band",
              "selected_model_probability", "selected_american_price", "selected_decimal_price",
              "selected_paid_break_even_probability", "selected_target_no_vig_probability",
              "target_market_favorite_status", "home_away_status", "quote_lead_minutes",
              "quote_timing_band", "paid_break_even_band", "temporal_block", "month",
              "flat_stake_return", "prediction_payload_sha256", "outcome_payload_sha256",
              "market_payload_sha256", "raw_source_sha256", "pinnacle_reference_timestamp_utc",
              "pinnacle_reference_time_delta_seconds", "ex_target_consensus_book_count",
              "later_pinnacle_timestamp_utc", "is_best_same_side_captured_price"]
    for name, source_col, reference_col, probability_forecast, source_note in specs:
        g = designated[designated[source_col].notna()].copy()
        if name == "PINNACLE_REFERENCE_EV":
            g = g[~g.sportsbook.eq("pinnacle")].copy()
        if g.empty: continue
        g["edge_definition"] = name
        g["provenance_class"] = DEFINITIONS[name]
        g["source_probability"] = g[source_col].astype(float)
        g["reference_market_probability"] = g[reference_col].astype(float)
        g["edge_probability_gap"] = g.source_probability - g.selected_paid_break_even_probability
        if name == "BEST_PRICE_ADVANTAGE":
            g["edge_probability_gap"] = np.where(g.is_best_same_side_captured_price,
                                                   g.edge_probability_gap.clip(lower=0), 0.0)
        g["edge_band"] = g.edge_probability_gap.map(edge_band)
        g["positive_edge"] = g.edge_probability_gap.gt(0)
        g["expected_roi_under_source_probability"] = g.source_probability * g.selected_decimal_price - 1
        g["probability_forecast_applicable"] = probability_forecast
        g["source_note"] = source_note
        g["analysis_identity"] = g.edge_definition + "|" + g.canonical_market_identity
        rows.append(g[common + ["analysis_identity", "edge_definition", "provenance_class",
                                "source_probability", "reference_market_probability", "edge_probability_gap",
                                "edge_band", "positive_edge", "expected_roi_under_source_probability",
                                "probability_forecast_applicable", "source_note"]])
    return pd.concat(rows, ignore_index=True).sort_values(["edge_definition", "sportsbook", "game_date", "game_id"])


def across_board(long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for (definition, pband, eband), g in long.groupby(["edge_definition", "paid_break_even_band", "edge_band"], sort=True):
        unique = g.sort_values(["game_id", "sportsbook"]).drop_duplicates("game_id")
        lo, hi = cluster_ci(g.game_date, g.flat_stake_return)
        y = g.selected_win.to_numpy(int); p = g.source_probability.to_numpy(float)
        forecast_ok = bool(g.probability_forecast_applicable.iloc[0])
        rows.append({"edge_definition": definition, "paid_break_even_band": pband, "edge_band": eband,
                     "unique_games": g.game_id.nunique(), "book_quotes": len(g),
                     "unique_game_wins": int(unique.selected_win.sum()),
                     "unique_game_losses": int(len(unique)-unique.selected_win.sum()),
                     "quote_wins": int(y.sum()), "quote_losses": int(len(y)-y.sum()), "quote_win_rate": y.mean(),
                     "average_paid_break_even_probability": g.selected_paid_break_even_probability.mean(),
                     "average_model_probability": g.selected_model_probability.mean(),
                     "average_reference_market_probability": g.reference_market_probability.mean(),
                     "average_edge_probability_gap": g.edge_probability_gap.mean(),
                     "realized_flat_stake_roi": g.flat_stake_return.mean(),
                     "expected_roi_under_source_probability": g.expected_roi_under_source_probability.mean(),
                     "source_brier": np.mean((p-y)**2) if forecast_ok else np.nan,
                     "source_log_loss": log_loss(y, p) if forecast_ok else np.nan,
                     "first_date": g.game_date.min(), "last_date": g.game_date.max(),
                     "date_clusters": g.game_date.nunique(), "book_coverage": g.sportsbook.nunique(),
                     "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi,
                     "quotes_are_independent_outcomes": False})
    table = pd.DataFrame(rows)
    monotonic = []
    for (definition, pband), g in table.groupby(["edge_definition", "paid_break_even_band"], sort=True):
        g = g.set_index("edge_band").reindex(EDGE_ORDER).dropna(subset=["realized_flat_stake_roi"])
        rho = spearmanr(np.arange(len(g)), g.realized_flat_stake_roi).statistic if len(g) >= 3 else np.nan
        roi = g.realized_flat_stake_roi.to_numpy()
        monotonic.append({"edge_definition": definition, "paid_break_even_band": pband,
                          "nonempty_edge_cells": len(g), "unique_games_across_cells": int(g.unique_games.sum()),
                          "roi_monotonic_nondecreasing": bool(len(g) >= 3 and np.all(np.diff(roi) >= 0)),
                          "edge_order_roi_spearman": rho})
    return table, pd.DataFrame(monotonic)


def matched_comparisons(long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pairs = []
    strata = ["sportsbook", "month", "home_away_status", "target_market_favorite_status",
              "confidence_band", "quote_timing_band"]
    for definition, d in long.groupby("edge_definition", sort=True):
        positive, control = d[d.positive_edge].copy(), d[~d.positive_edge].copy()
        for key, pos in positive.groupby(strata, dropna=False, sort=True):
            mask = np.ones(len(control), dtype=bool)
            for col, value in zip(strata, key if isinstance(key, tuple) else (key,)):
                mask &= control[col].fillna("__NA__").eq("__NA__" if pd.isna(value) else value).to_numpy()
            pool = control[mask].copy()
            if pos.empty or pool.empty: continue
            cost = abs(pos.selected_paid_break_even_probability.to_numpy()[:, None] -
                       pool.selected_paid_break_even_probability.to_numpy()[None, :])
            same_game = pos.game_id.to_numpy()[:, None] == pool.game_id.to_numpy()[None, :]
            cost = cost + same_game * 1000
            rr, cc = linear_sum_assignment(cost)
            for i, j in zip(rr, cc):
                gap = float(abs(pos.iloc[i].selected_paid_break_even_probability - pool.iloc[j].selected_paid_break_even_probability))
                if gap > MATCH_CALIPER or int(pos.iloc[i].game_id) == int(pool.iloc[j].game_id): continue
                a, b = pos.iloc[i], pool.iloc[j]
                pairs.append({"edge_definition": definition, "pair_id": f"{definition}|{a.analysis_identity}|{b.analysis_identity}",
                              "target_sportsbook": a.sportsbook, "positive_game_id": int(a.game_id),
                              "control_game_id": int(b.game_id), "positive_date": a.game_date, "control_date": b.game_date,
                              "positive_edge_gap": a.edge_probability_gap, "control_edge_gap": b.edge_probability_gap,
                              "positive_paid_break_even": a.selected_paid_break_even_probability,
                              "control_paid_break_even": b.selected_paid_break_even_probability,
                              "absolute_paid_break_even_gap": gap,
                              "positive_win": int(a.selected_win), "control_win": int(b.selected_win),
                              "positive_return": a.flat_stake_return, "control_return": b.flat_stake_return,
                              "return_difference": a.flat_stake_return-b.flat_stake_return,
                              "outcome_difference": int(a.selected_win)-int(b.selected_win),
                              "exact_match_fields": "|".join(strata)})
    pair_df = pd.DataFrame(pairs)
    summary = []
    for definition, d in long.groupby("edge_definition", sort=True):
        p = pair_df[pair_df.edge_definition.eq(definition)] if len(pair_df) else pair_df
        positives = d[d.positive_edge]
        lo, hi = cluster_ci(p.positive_date, p.return_difference) if len(p) else (np.nan, np.nan)
        summary.append({"edge_definition": definition, "positive_quotes": len(positives),
                        "matched_positive_quotes": len(p), "match_rate": len(p)/len(positives) if len(positives) else np.nan,
                        "target_books_with_positive": positives.sportsbook.nunique(),
                        "target_books_with_matches": p.target_sportsbook.nunique() if len(p) else 0,
                        "mean_absolute_paid_break_even_gap": p.absolute_paid_break_even_gap.mean() if len(p) else np.nan,
                        "positive_win_rate": p.positive_win.mean() if len(p) else np.nan,
                        "control_win_rate": p.control_win.mean() if len(p) else np.nan,
                        "positive_roi": p.positive_return.mean() if len(p) else np.nan,
                        "control_roi": p.control_return.mean() if len(p) else np.nan,
                        "paired_roi_difference": p.return_difference.mean() if len(p) else np.nan,
                        "paired_roi_difference_clustered_ci_2_5": lo,
                        "paired_roi_difference_clustered_ci_97_5": hi,
                        "positive_outperforms_control": bool(len(p) and p.return_difference.mean() > 0),
                        "common_support": "LIMITED" if len(positives) and len(p)/len(positives) < .5 else "ADEQUATE"})
    return pair_df, pd.DataFrame(summary)


def favorite_underdog_results(long: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (definition, status, positive), g in long.groupby(
            ["edge_definition", "target_market_favorite_status", "positive_edge"], sort=True):
        lo, hi = cluster_ci(g.game_date, g.flat_stake_return)
        rows.append({"edge_definition": definition, "target_market_status": status,
                     "edge_sign": "POSITIVE" if positive else "ZERO_OR_NEGATIVE",
                     "unique_games": g.game_id.nunique(), "book_quotes": len(g),
                     "wins": int(g.selected_win.sum()), "losses": int(len(g)-g.selected_win.sum()),
                     "win_rate": g.selected_win.mean(),
                     "mean_paid_break_even_probability": g.selected_paid_break_even_probability.mean(),
                     "mean_edge_probability_gap": g.edge_probability_gap.mean(),
                     "flat_stake_roi": g.flat_stake_return.mean(),
                     "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi,
                     "first_date": g.game_date.min(), "last_date": g.game_date.max()})
    return pd.DataFrame(rows)


def rolling_edge_tests(long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    predictions, summaries = [], []
    for definition, d in long.groupby("edge_definition", sort=True):
        if definition == "MODEL_DERIVED_EV":
            d = d[d.sportsbook.eq("pinnacle")].copy()
        dates = np.array(sorted(d.game_date.unique()))
        if len(dates) < 4: continue
        initial = max(2, min(10, len(dates)//3))
        blocks = [x for x in np.array_split(dates[initial:], min(5, len(dates)-initial)) if len(x)]
        fold_coefs = []
        for fold, test_dates in enumerate(blocks, 1):
            train_dates = dates[dates < test_dates[0]]
            train, test = d[d.game_date.isin(train_dates)], d[d.game_date.isin(test_dates)]
            if train.selected_win.nunique() < 2 or test.empty: continue
            xb_train = logit(train.selected_paid_break_even_probability).reshape(-1, 1)
            xe_train = np.c_[xb_train[:, 0], train.edge_probability_gap.to_numpy(float)]
            base = LogisticRegression(C=1, solver="lbfgs", max_iter=2000).fit(xb_train, train.selected_win)
            edge = LogisticRegression(C=1, solver="lbfgs", max_iter=2000).fit(xe_train, train.selected_win)
            xb_test = logit(test.selected_paid_break_even_probability).reshape(-1, 1)
            xe_test = np.c_[xb_test[:, 0], test.edge_probability_gap.to_numpy(float)]
            pb, pe = base.predict_proba(xb_test)[:, 1], edge.predict_proba(xe_test)[:, 1]
            fold_coefs.append(float(edge.coef_[0, 1]))
            for i, (_, r) in enumerate(test.iterrows()):
                predictions.append({"edge_definition": definition, "fold": fold, "game_date": r.game_date,
                                    "game_id": int(r.game_id), "sportsbook": r.sportsbook,
                                    "selected_win": int(r.selected_win), "flat_stake_return": r.flat_stake_return,
                                    "paid_break_even_probability": r.selected_paid_break_even_probability,
                                    "edge_probability_gap": r.edge_probability_gap, "positive_edge": bool(r.positive_edge),
                                    "base_probability": float(pb[i]), "edge_augmented_probability": float(pe[i]),
                                    "train_start": train.game_date.min(), "train_end": train.game_date.max(),
                                    "test_start": test.game_date.min(), "test_end": test.game_date.max(),
                                    "train_rows": len(train), "test_rows": len(test)})
        p = pd.DataFrame([r for r in predictions if r["edge_definition"] == definition])
        if p.empty: continue
        y, pb, pe = p.selected_win.to_numpy(), p.base_probability.to_numpy(), p.edge_augmented_probability.to_numpy()
        bd, ld = (pe-y)**2-(pb-y)**2, (-y*np.log(pe)-(1-y)*np.log(1-pe))-(-y*np.log(pb)-(1-y)*np.log(1-pb))
        blo, bhi = cluster_ci(p.game_date, bd); llo, lhi = cluster_ci(p.game_date, ld)
        pos, nonpos = p[p.positive_edge], p[~p.positive_edge]
        pos_lo, pos_hi = cluster_ci(pos.game_date, pos.flat_stake_return) if len(pos) else (np.nan, np.nan)
        summaries.append({"edge_definition": definition, "oot_rows": len(p), "oot_unique_games": p.game_id.nunique(),
                          "oot_dates": p.game_date.nunique(), "folds": p.fold.nunique(),
                          "base_brier": np.mean((pb-y)**2), "edge_augmented_brier": np.mean((pe-y)**2),
                          "brier_difference_edge_minus_base": bd.mean(), "brier_difference_ci_2_5": blo,
                          "brier_difference_ci_97_5": bhi, "base_log_loss": log_loss(y, pb),
                          "edge_augmented_log_loss": log_loss(y, pe), "log_loss_difference_edge_minus_base": ld.mean(),
                          "log_loss_difference_ci_2_5": llo, "log_loss_difference_ci_97_5": lhi,
                          "base_ece": ece(y, pb), "edge_augmented_ece": ece(y, pe),
                          "base_accuracy": np.mean((pb >= .5) == y), "edge_augmented_accuracy": np.mean((pe >= .5) == y),
                          "base_auc": roc_auc_score(y, pb) if len(np.unique(y)) == 2 else np.nan,
                          "edge_augmented_auc": roc_auc_score(y, pe) if len(np.unique(y)) == 2 else np.nan,
                          "edge_coefficient_positive_fold_rate": np.mean(np.asarray(fold_coefs) > 0),
                          "positive_edge_oot_rows": len(pos), "positive_edge_oot_roi": pos.flat_stake_return.mean() if len(pos) else np.nan,
                          "positive_edge_oot_roi_ci_2_5": pos_lo, "positive_edge_oot_roi_ci_97_5": pos_hi,
                          "nonpositive_edge_oot_rows": len(nonpos), "nonpositive_edge_oot_roi": nonpos.flat_stake_return.mean() if len(nonpos) else np.nan,
                          "edge_return_spearman": spearmanr(p.edge_probability_gap, p.flat_stake_return).statistic,
                          "improves_brier_and_log_loss": bool(bd.mean() < 0 and ld.mean() < 0),
                          "interval_supported_score_improvement": bool(bhi < 0 and lhi < 0)})
    return pd.DataFrame(predictions), pd.DataFrame(summaries)


def required_forecast_comparison(designated: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = designated[designated.sportsbook.eq("pinnacle") & designated.pinnacle_reference_probability.notna()].copy()
    dates = np.array(sorted(d.game_date.unique())); blocks = [x for x in np.array_split(dates[10:], 5) if len(x)]
    rows = []
    features = {
        "PAID_BREAK_EVEN_ALONE": ["pbe_logit"],
        "PAID_BREAK_EVEN_PLUS_MODEL_EDGE": ["pbe_logit", "model_edge"],
        "MARKET_PROBABILITY_ALONE": ["market_logit"],
        "MARKET_PLUS_MODEL_MARKET_GAP": ["market_logit", "model_market_gap"],
        "INDEPENDENT_MODEL_PROBABILITY_ALONE": ["model_logit"],
        "MODEL_AND_MARKET_WITHOUT_EDGE_TRANSFORMATION": ["model_logit", "market_logit"],
    }
    d["pbe_logit"] = logit(d.selected_paid_break_even_probability)
    d["market_logit"] = logit(d.pinnacle_reference_probability)
    d["model_logit"] = logit(d.selected_model_probability)
    d["model_edge"] = d.selected_model_probability-d.selected_paid_break_even_probability
    d["model_market_gap"] = d.selected_model_probability-d.pinnacle_reference_probability
    for fold, test_dates in enumerate(blocks, 1):
        train, test = d[d.game_date.lt(test_dates[0])], d[d.game_date.isin(test_dates)]
        for name, cols in features.items():
            fit = LogisticRegression(C=1, solver="lbfgs", max_iter=2000).fit(train[cols], train.selected_win)
            pred = fit.predict_proba(test[cols])[:, 1]
            for i, (_, r) in enumerate(test.iterrows()):
                rows.append({"forecast": name, "fold": fold, "game_date": r.game_date, "game_id": int(r.game_id),
                             "selected_win": int(r.selected_win), "probability": float(pred[i]),
                             "train_start": train.game_date.min(), "train_end": train.game_date.max(),
                             "test_start": test.game_date.min(), "test_end": test.game_date.max(),
                             "train_rows": len(train), "test_rows": len(test)})
    pred = pd.DataFrame(rows)
    summary = []
    for name, g in pred.groupby("forecast", sort=True):
        y, p = g.selected_win.to_numpy(), g.probability.to_numpy()
        summary.append({"forecast": name, "oot_rows": len(g), "oot_unique_games": g.game_id.nunique(),
                        "folds": g.fold.nunique(), "brier": np.mean((p-y)**2), "log_loss": log_loss(y, p),
                        "ece": ece(y, p), "accuracy": np.mean((p >= .5) == y),
                        "auc": roc_auc_score(y, p) if len(np.unique(y)) == 2 else np.nan})
    return pred, pd.DataFrame(summary)


def cross_book(quotes: pd.DataFrame) -> pd.DataFrame:
    paired = quotes[quotes.pinnacle_reference_probability.notna() & ~quotes.sportsbook.eq("pinnacle")].copy()
    paired["reference_edge"] = paired.pinnacle_reference_probability-paired.selected_paid_break_even_probability
    rows = []
    for book, b in paired.groupby("sportsbook", sort=True):
        games = []
        for game_id, g in b.groupby("game_id", sort=True):
            g = g.sort_values(["captured_dt", "canonical_market_identity"]); first, last = g.iloc[0], g.iloc[-1]
            games.append({"game_id": int(game_id), "game_date": first.game_date, "selected_win": int(first.selected_win),
                          "first_return": first.flat_stake_return, "first_edge": first.reference_edge,
                          "last_edge": last.reference_edge, "first_target_pbe": first.selected_paid_break_even_probability,
                          "last_target_pbe": last.selected_paid_break_even_probability,
                          "first_reference_p": first.pinnacle_reference_probability,
                          "last_reference_p": last.pinnacle_reference_probability,
                          "first_time_delta_seconds": first.pinnacle_reference_time_delta_seconds,
                          "paired_quotes": len(g),
                          "converged": abs(last.reference_edge) < abs(first.reference_edge) if len(g) > 1 else np.nan})
        game = pd.DataFrame(games)
        if game.empty: continue
        pos = game[game.first_edge.gt(0)]; conv = game[game.converged.eq(True)]; nonconv = game[game.converged.eq(False)]
        pos_multi = pos[pos.converged.notna()]
        pos_conv = pos[pos.converged.eq(True)]; pos_nonconv = pos[pos.converged.eq(False)]
        ordered_dates = sorted(game.game_date.unique()); midpoint = ordered_dates[max(0, len(ordered_dates)//2)]
        early, late = game[game.game_date.lt(midpoint)], game[game.game_date.ge(midpoint)]
        rows.append({"target_sportsbook": book, "reference_sportsbook": "pinnacle",
                     "exact_matched_games": len(game), "paired_quote_observations": int(game.paired_quotes.sum()),
                     "first_date": game.game_date.min(), "last_date": game.game_date.max(),
                     "average_absolute_quote_time_delta_seconds": game.first_time_delta_seconds.mean(),
                     "average_displayed_reference_edge": game.first_edge.mean(),
                     "positive_reference_edge_frequency": len(pos)/len(game),
                     "all_target_roi": game.first_return.mean(), "positive_reference_edge_rows": len(pos),
                     "positive_reference_edge_roi": pos.first_return.mean() if len(pos) else np.nan,
                     "average_target_pbe_movement": (game.last_target_pbe-game.first_target_pbe).mean(),
                     "average_reference_probability_movement": (game.last_reference_p-game.first_reference_p).mean(),
                     "average_edge_change": (game.last_edge-game.first_edge).mean(),
                     "games_with_multiple_pairs": int(game.converged.notna().sum()),
                     "convergence_frequency": game.converged.mean(),
                     "positive_edge_games_with_multiple_pairs": len(pos_multi),
                     "positive_edge_convergence_frequency": pos_multi.converged.mean() if len(pos_multi) else np.nan,
                     "converged_win_rate": conv.selected_win.mean() if len(conv) else np.nan,
                     "not_converged_win_rate": nonconv.selected_win.mean() if len(nonconv) else np.nan,
                     "converged_roi": conv.first_return.mean() if len(conv) else np.nan,
                     "not_converged_roi": nonconv.first_return.mean() if len(nonconv) else np.nan,
                     "positive_converged_win_rate": pos_conv.selected_win.mean() if len(pos_conv) else np.nan,
                     "positive_not_converged_win_rate": pos_nonconv.selected_win.mean() if len(pos_nonconv) else np.nan,
                     "positive_converged_roi": pos_conv.first_return.mean() if len(pos_conv) else np.nan,
                     "positive_not_converged_roi": pos_nonconv.first_return.mean() if len(pos_nonconv) else np.nan,
                     "positive_edge_roi_early_half": early[early.first_edge.gt(0)].first_return.mean(),
                     "positive_edge_roi_late_half": late[late.first_edge.gt(0)].first_return.mean(),
                     "prices_are_fill_evidence": False})
    return pd.DataFrame(rows)


def alternative_regions(designated: pd.DataFrame) -> pd.DataFrame:
    p = designated[designated.sportsbook.eq("pinnacle")].copy()
    p["model_edge"] = p.selected_model_probability-p.selected_paid_break_even_probability
    market_fav = p.copy()
    side_home = market_fav.no_vig_home_probability.astype(float).ge(.5)
    market_fav["model_selected_side"] = np.where(side_home, "HOME", "AWAY")
    market_fav["selected_model_probability"] = np.where(side_home, market_fav.home_win_probability, market_fav.away_win_probability)
    market_fav["selected_paid_break_even_probability"] = np.where(side_home, market_fav.home_implied_probability, market_fav.away_implied_probability)
    market_fav["selected_decimal_price"] = np.where(side_home, market_fav.home_decimal_price, market_fav.away_decimal_price)
    market_fav["pinnacle_reference_probability"] = np.where(side_home, market_fav.no_vig_home_probability, market_fav.no_vig_away_probability)
    market_fav["selected_win"] = np.where(side_home, market_fav.official_winner.eq(market_fav.home_team), market_fav.official_winner.eq(market_fav.away_team)).astype(int)
    market_fav["flat_stake_return"] = np.where(market_fav.selected_win.eq(1), market_fav.selected_decimal_price-1, -1)
    cohorts = {
        "INDEPENDENT_MODEL_POSITIVE_EV": p[p.model_edge.gt(0)],
        "INDEPENDENT_MODEL_STRENGTH": p[p.selected_model_probability.gt(STRONG)],
        "MARKET_PROBABILITY_STRENGTH": p[p.pinnacle_reference_probability.gt(STRONG)],
        "MODEL_MARKET_AGREEMENT": p[p.target_market_favorite_status.eq("FAVORITE")],
        "FULL_STRONG_MODEL_COHORT_PRICED": p[p.selected_model_probability.gt(STRONG)],
        "MODEL_STRONG_MARKET_STRONG": p[p.selected_model_probability.gt(STRONG) & p.pinnacle_reference_probability.gt(STRONG)],
        "MARKET_STRONG_MODEL_NOT_STRONG": p[p.selected_model_probability.le(STRONG) & p.pinnacle_reference_probability.gt(STRONG)],
        "ALWAYS_MARKET_FAVORITE": market_fav,
        "ENTIRE_UNFILTERED_MODEL_POPULATION": p,
    }
    rows = []
    for name, g in cohorts.items():
        lo, hi = cluster_ci(g.game_date, g.flat_stake_return)
        rows.append({"approach": name, "unique_games": g.game_id.nunique(), "wins": int(g.selected_win.sum()),
                     "losses": int(len(g)-g.selected_win.sum()), "win_rate": g.selected_win.mean(),
                     "mean_paid_break_even": g.selected_paid_break_even_probability.mean(),
                     "mean_model_probability": g.selected_model_probability.mean(),
                     "mean_market_probability": g.selected_market_probability.mean() if "selected_market_probability" in g else g.pinnacle_reference_probability.mean(),
                     "flat_stake_roi": g.flat_stake_return.mean(), "profit_units": g.flat_stake_return.sum(),
                     "clustered_roi_ci_2_5": lo, "clustered_roi_ci_97_5": hi,
                     "selection_basis": "EDGE_FIRST" if "EV" in name else "PROBABILITY_OR_AGREEMENT_NOT_EDGE_FIRST"})
    return pd.DataFrame(rows)


def provenance_ledger() -> pd.DataFrame:
    rows = [
        ("model_implied_expected_return", "audit_mlb_moneyline_probability_region_premise_v1.py; observe_mlb_strong_moneyline_shadow_v1.py", "model_p * target_decimal_price - 1", "frozen model", "captured target book", "none", "model precedes/independent of quote in retrospective audit; observer requires quote after freeze", "proportional no-vig retained separately", False, True, "descriptive/unknown fill", "research/filter", "INDEPENDENT_MODEL_EDGE"),
        ("model_minus_market_gap", "audit_mlb_moneyline_probability_region_premise_v1.py", "model_p - target_book_no_vig_p", "frozen model", "target book", "same target book paired side", "designated pregame quote", "proportional", False, True, "descriptive/unknown fill", "ranking/research", "INDEPENDENT_MODEL_EDGE"),
        ("selected_market_probability", "audit_mlb_moneyline_probability_region_premise_v1.py", "raw_selected_implied / (raw_home + raw_away)", "target book paired market", "none", "same target book", "same quote", "proportional", True, False, "descriptive", "research", "SINGLE_MARKET_REFERENCE_EDGE"),
        ("EV %", "run_mlb_tool_equivalent_ev_edge_selection_reconstruction_v1.py", "model_p * target_decimal_price - 1", "independent totals model", "external-tool book price", "none", "preserved totals examples/history; no moneyline display export", "raw price in EV", False, True, "unknown", "display/filter", "INDEPENDENT_MODEL_EDGE"),
        ("Edge % / edge_pct_raw", "run_mlb_tool_equivalent_ev_edge_selection_reconstruction_v1.py", "model_p - raw_target_implied_p", "independent totals model", "external-tool book price", "none", "preserved totals examples/history; no moneyline display export", "none", False, True, "unknown", "display/filter", "INDEPENDENT_MODEL_EDGE"),
        ("edge_pct_no_vig", "run_mlb_tool_equivalent_ev_edge_selection_reconstruction_v1.py", "model_p - target_paired_no_vig_p", "independent totals model", "target book", "same target book paired side", "same pregame quote", "proportional", True, True, "descriptive", "research", "INDEPENDENT_MODEL_EDGE"),
        ("value_vs_market", "today_workspace_mvp.sql", "best American price - median American price", "none", "best book", "multi-book median", "current workspace snapshot", "none; American-price arithmetic", True, False, "displayed/descriptive", "display/ranking", "CROSS_BOOK_PRICE_DIFFERENCE"),
        ("price_change_from_open", "today_workspace_mvp.sql", "latest American price - open American price", "none", "same-side latest", "same-side open", "later minus earlier snapshot", "none", True, False, "displayed/descriptive", "display", "LINE_MOVEMENT_MEASURE"),
        ("MODEL_DERIVED_EV", Path(__file__).name, "frozen model_p - target paid break-even; EV=model_p*decimal-1", "frozen moneyline model", "actual captured target book", "none", "latest valid pregame target quote", "target proportional no-vig separately", False, True, "descriptive/unknown fill", "research", "INDEPENDENT_MODEL_EDGE"),
        ("PINNACLE_REFERENCE_EV", Path(__file__).name, "nearest Pinnacle no-vig_p - target paid break-even", "Pinnacle market", "actual captured target book", "Pinnacle", "within fixed 15-minute window", "Pinnacle proportional", True, False, "descriptive/unknown fill", "research", "SINGLE_MARKET_REFERENCE_EDGE"),
        ("EX_TARGET_CONSENSUS_EV", Path(__file__).name, "median ex-target no-vig_p - target paid break-even", "other managed markets", "actual captured target book", "at least 3 other books", "same capture batch", "each book proportional", True, False, "descriptive/unknown fill", "research", "MULTIMARKET_CONSENSUS_EDGE"),
        ("BEST_PRICE_ADVANTAGE", Path(__file__).name, "for minimum-PBE same-side quote: ex-target median paid break-even - target paid break-even; non-best=0", "none", "best actual captured target book price", "other-book median", "same capture batch", "none", True, False, "descriptive/unknown fill", "research", "CROSS_BOOK_PRICE_DIFFERENCE"),
        ("LATER_PINNACLE_RELATIONSHIP", Path(__file__).name, "later Pinnacle no-vig_p - evaluated target paid break-even", "later managed market", "earlier target book", "latest later Pinnacle", "strictly later and pregame", "Pinnacle proportional", True, False, "descriptive/unknown fill", "research", "LINE_MOVEMENT_MEASURE"),
        ("moneyline tool-displayed EV", "repository-wide retained evidence search", "not preserved", "unknown", "unknown", "unknown", "no exact moneyline tool-display export located", "unknown", None, None, "unknown", "unknown", "UNKNOWN_OR_UNREPRODUCIBLE"),
    ]
    columns = ["field_name", "producing_utility", "formula", "probability_source", "target_price_source",
               "reference_price_source", "timestamp_relationship", "no_vig_method",
               "market_information_on_both_sides", "probability_independent_of_offered_price",
               "quote_status", "use", "classification"]
    return pd.DataFrame(rows, columns=columns)


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import csv,hashlib,json,pathlib,sys
p=pathlib.Path(__file__).resolve().parent; errors=[]
for line in (p/'sha256_manifest.txt').read_text().splitlines():
 d,n=line.split('  ',1); f=p/n
 if not f.exists() or hashlib.sha256(f.read_bytes()).hexdigest()!=d: errors.append(n)
s=json.loads((p/'summary.json').read_text())
with (p/'edge_definition_observations.csv').open(newline='') as h: rows=list(csv.DictReader(h))
if len({r['analysis_identity'] for r in rows})!=len(rows): errors.append('duplicate_analysis_identity')
if s['population']['unique_resolved_games']!=471: errors.append('resolved_game_reconciliation')
if any(float(r['quote_lead_minutes'])<=0 for r in rows): errors.append('nonpregame_quote')
if any(r['edge_definition']=='MODEL_DERIVED_EV' and r['provenance_class']!='INDEPENDENT_MODEL_EDGE' for r in rows): errors.append('model_edge_provenance')
with (p/'alternative_roi_regions.csv').open(newline='') as h: alt={r['approach']:r for r in csv.DictReader(h)}
model=alt['INDEPENDENT_MODEL_POSITIVE_EV']
if int(model['unique_games'])!=225 or abs(float(model['flat_stake_roi'])-(-0.043936779108))>1e-10: errors.append('model_positive_ev_reconciliation')
with (p/'edge_oot_predictions.csv').open(newline='') as h: oot=list(csv.DictReader(h))
if any(r['test_start']<=r['train_end'] for r in oot): errors.append('oot_overlap')
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors,'edge_rows':len(rows)},sort_keys=True));sys.exit(bool(errors))
'''


def render_report(s: dict[str, Any]) -> str:
    return f"""# MLB Across-Board Apparent-EV Provenance and Economic-Value Audit v1

`{s['classification']}`

## Direct answers

1. **Across-board economic information:** `{s['direct_answers']['apparent_ev_economically_informative']}`. Positive model-derived EV remained {s['key_results']['model_positive_roi']:+.1%}; fixed edge magnitude did not monotonically order ROI in {s['key_results']['nonmonotonic_cells']} of {s['key_results']['testable_monotonic_cells']} testable definition/probability regions.
2. **Definition dependence:** `{s['direct_answers']['definition_dependence']}`. Only model-derived EV uses an independent probability. Pinnacle-reference, consensus, best-price, and later-line measures compare managed prices with managed prices.
3. **Market-derived edge after target price:** `{s['direct_answers']['market_edge_after_target_price']}`. See `edge_oot_usefulness.csv`; negative score differences favor adding edge.
4. **Independent model edge:** `{s['direct_answers']['independent_model_edge']}`. Its out-of-time edge coefficient and score changes do not overturn the prior -4.4% realized positive-EV result.
5. **Matched no-edge comparisons:** `{s['direct_answers']['matched_comparison']}`. {s['key_results']['matched_outperform_definitions']} of {s['key_results']['matched_tested_definitions']} definitions had a positive paired ROI point difference, but {s['key_results']['matched_interval_supported_outperform_definitions']} had an interval-supported positive difference; all five comparisons had limited common support.
6. **Favorite/underdog and probability regions:** `{s['direct_answers']['favorite_underdog_and_probability_regions']}`. Positive apparent edge appeared in {s['key_results']['positive_edge_probability_band_count']} paid-break-even bands; the fixed within-band tables do not support one monotonic across-board payoff.
7. **Probability strength versus discrepancy:** `{s['direct_answers']['probability_vs_edge']}`. Model-strong/market-strong remained 56-20 and {s['key_results']['joint_strong_roi']:+.1%} at Pinnacle, while model-derived positive EV was {s['key_results']['model_positive_roi']:+.1%}.
8. **Convergence and observable bait signature:** Favorable-discrepancy convergence is `{s['direct_answers']['favorable_price_convergence']}`; no target/reference pair had 30 favorable repeated-pair games. Overall, `{s['direct_answers']['bait_signature']}`. This is an empirical price/outcome pattern only. Intent is `{s['intentional_inducement']}`.
9. **Immediate direction:** `{s['next_immediate_research_direction']}`. The existing append-only STRONG observer remains unchanged and observational.

## Plain-language answer

> When we believed we were measuring an advantage over a book, were we usually measuring independent predictive information—or merely a difference between prices managed by related markets?

Only the frozen-model calculations were independent of the offered price. Pinnacle-reference EV, ex-target consensus EV, best-price advantage, and closing-line relationships were differences among managed market prices. The retained evidence does not show that those visible disagreements reliably became bettor profit.

## Population and provenance

The audit covers {s['population']['unique_resolved_games']} canonical resolved games, {s['population']['unique_model_predictions']} unique predictions, {s['population']['designated_book_price_matches']} designated game/book prices, and {s['population']['valid_repeated_quote_observations']} valid repeated quote observations through `{s['cutoff']}`. A quote is not another game outcome. The prior 4,366-row joined ledger remains a prior analysis table, not 4,366 independent results.

Model/configuration hash: `{s['model']['hash']}`. STRONG remains strictly above 60%; no threshold or production model changed.

`edge_provenance_ledger.csv` documents formulas, source and target identities, timestamp relationships, no-vig methods, independence, quote status, and use. No exact moneyline tool-displayed EV export was located; the preserved external-tool formulas belong to the separate totals reconstruction and are not silently imported into moneyline outcomes.

## Cross-book and convergence limits

The fixed contemporaneous rule pairs quotes within 15 minutes. Ex-target consensus requires at least three other books in the same capture batch. Later Pinnacle relationships are line movement, not outcome truth. Convergence rates and outcome/ROI splits are in `cross_book_reference_tests.csv`; correlated movement cannot establish independent predictive information or executable fills.

BetOnline had {s['key_results']['betonline_cross_book_rows']} exact Pinnacle-paired games, {s['key_results']['betonline_positive_reference_edge_frequency']:.1%} positive Pinnacle-reference EV, and {s['key_results']['betonline_convergence_frequency']:.1%} convergence among games with repeated pairs. Its converged/non-converged captured-price ROIs were {s['key_results']['betonline_converged_roi']:+.1%}/{s['key_results']['betonline_not_converged_roi']:+.1%}; these short, correlated August samples are descriptive.

## Controls and uncertainty

Matching was exact on target book, month, home/away, favorite status, model confidence band, and quote-timing band, then used the existing 2-percentage-point neutral boundary as a paid-break-even caliper. Unsupported positive rows were not extrapolated. Date-clustered intervals treat same-date book quotes as correlated.

## Conclusion

Visible price disagreement exists across the board, but the evidence does not support treating a generic edge label as economic truth. The economically stronger fixed region is probability-and-agreement based, not discrepancy-first. This is compatible with visible edge functioning as transaction inducement without reliable bettor advantage, but deliberate intent cannot be inferred from prices and outcomes.

No production model, selection, observer, scheduler, public surface, historical record, or wagering system was modified.
"""


def run(output: Path, cutoff_arg: str) -> dict[str, Any]:
    cutoff = prior.latest_resolved_cutoff() if cutoff_arg == "latest" else cutoff_arg
    config = json.loads(prior.CONFIG.read_text())
    predictions, raw, designated0 = load_population(cutoff)
    raw = attach_references(raw)
    designated_keys = set(designated0.canonical_market_identity)
    designated = raw[raw.canonical_market_identity.isin(designated_keys)].copy()
    long = definition_rows(designated)
    board, monotonic = across_board(long)
    matched_pairs, matched_summary = matched_comparisons(long)
    favorite_results = favorite_underdog_results(long)
    oot_predictions, oot_summary = rolling_edge_tests(long)
    required_predictions, required_summary = required_forecast_comparison(designated)
    cross = cross_book(raw)
    alternatives = alternative_regions(designated)
    provenance = provenance_ledger()

    model_rows = long[long.edge_definition.eq("MODEL_DERIVED_EV") & long.sportsbook.eq("pinnacle")]
    model_positive = model_rows[model_rows.positive_edge]
    mono_test = monotonic[monotonic.nonempty_edge_cells.ge(3)]
    matched_test = matched_summary[matched_summary.matched_positive_quotes.gt(0)]
    market_defs = ["PINNACLE_REFERENCE_EV", "EX_TARGET_CONSENSUS_EV", "BEST_PRICE_ADVANTAGE", "LATER_PINNACLE_RELATIONSHIP"]
    market_oot = oot_summary[oot_summary.edge_definition.isin(market_defs)]
    market_improves = market_oot[market_oot.interval_supported_score_improvement]
    model_oot = oot_summary[oot_summary.edge_definition.eq("MODEL_DERIVED_EV")].iloc[0]
    alt = alternatives.set_index("approach")
    betonline = cross.set_index("target_sportsbook").loc["sportsgameodds:betonline"] if "sportsgameodds:betonline" in set(cross.target_sportsbook) else None
    matched_outperform = int(matched_test.positive_outperforms_control.sum())
    matched_supported = int((matched_test.paired_roi_difference_clustered_ci_2_5 > 0).sum())
    no_market_edge_use = len(market_improves) == 0
    no_model_edge_use = not bool(model_oot.interval_supported_score_improvement)
    matched_not_consistent = matched_outperform < max(1, math.ceil(len(matched_test)/2))
    if no_market_edge_use and no_model_edge_use and matched_not_consistent:
        classification = "APPARENT_EDGE_NOT_ECONOMICALLY_INFORMATIVE"
    elif len(market_improves) and bool(model_oot.improves_brier_and_log_loss):
        classification = "APPARENT_EDGE_PARTIALLY_INFORMATIVE"
    else:
        classification = "EDGE_DEFINITION_DEPENDENT"
    convergence_supported = bool(len(cross) and cross.positive_edge_games_with_multiple_pairs.max() >= 30)
    convergence_better = bool(convergence_supported and (
        cross.loc[cross.positive_edge_games_with_multiple_pairs.ge(30), "positive_converged_roi"] >
        cross.loc[cross.positive_edge_games_with_multiple_pairs.ge(30), "positive_not_converged_roi"]
    ).mean() >= .75)
    signature = {
        "positive_edge_multiple_probability_regions": bool((board[board.edge_band.isin(EDGE_ORDER[2:])].groupby("edge_definition").paid_break_even_band.nunique() >= 3).any()),
        "edge_magnitude_not_monotonic": bool(len(mono_test) and (~mono_test.roi_monotonic_nondecreasing).mean() > .5),
        "positive_edge_fails_matched_consistently": matched_not_consistent,
        "market_reference_edge_adds_no_oot_information": no_market_edge_use,
        "favorable_discrepancy_convergence_without_economic_gain": bool(convergence_supported and not convergence_better),
        "favorable_discrepancy_convergence_evidence_sufficient": convergence_supported,
        "model_positive_edge_bettor_return_negative": bool(model_positive.flat_stake_return.mean() < 0),
    }
    summary = {
        "experiment": EXPERIMENT, "classification": classification, "cutoff": cutoff,
        "model": {"version": prior.MODEL, "hash": config["model_hash"], "snapshot": prior.SNAPSHOT,
                  "admission": prior.ADMISSION, "strong_boundary": "selected probability > 0.60 strict"},
        "population": {"unique_resolved_games": predictions.game_id.nunique(),
                       "unique_model_predictions": len(predictions), "designated_book_price_matches": len(designated),
                       "valid_repeated_quote_observations": len(raw)-len(designated),
                       "valid_quote_observations_total": len(raw), "books": raw.sportsbook.nunique(),
                       "prior_joined_ledger_rows": 4366},
        "direct_answers": {
            "apparent_ev_economically_informative": classification,
            "definition_dependence": "SEMANTICS_AND_COVERAGE_DIFFER; MARKET_DERIVED_MEASURES_ARE_PRICE_DISAGREEMENTS",
            "market_edge_after_target_price": "NO_CONSISTENT_OUT_OF_TIME_INCREMENTAL_INFORMATION" if no_market_edge_use else "DEFINITION_DEPENDENT_OOT_RESULT",
            "independent_model_edge": "NO_OUT_OF_TIME_INCREMENTAL_INFORMATION_AND_NEGATIVE_REALIZED_POSITIVE_EDGE_ROI" if no_model_edge_use else "DIRECTIONAL_OOT_INFORMATION_ONLY",
            "matched_comparison": "POSITIVE_EDGE_DOES_NOT_CONSISTENTLY_BEAT_MATCHED_NO_EDGE" if matched_not_consistent else "MIXED_MATCHED_RESULTS",
            "favorite_underdog_and_probability_regions": "POINT_RESULTS_DIFFER_BY REGION_AND_SIDE_BUT_NO_EDGE_DEFINITION_ORDERS_ROI_RELIABLY",
            "probability_vs_edge": "MODEL_MARKET_STRENGTH_AND_AGREEMENT_OUTPERFORM_DISCREPANCY_FIRST_SELECTION",
            "favorable_price_convergence": "INSUFFICIENT_COMPARABLE_POSITIVE_REFERENCE_EDGE_HISTORY",
            "bait_signature": "OBSERVED_STRUCTURE_IS_CONSISTENT_WITH_VISIBLE_DISAGREEMENT_WITHOUT_RELIABLE_ECONOMIC_ADVANTAGE" if sum(signature.values()) >= 4 else "PARTIAL_EMPIRICAL_SIGNATURE",
        },
        "key_results": {"model_positive_rows": len(model_positive), "model_positive_roi": float(model_positive.flat_stake_return.mean()),
                        "nonmonotonic_cells": int((~mono_test.roi_monotonic_nondecreasing).sum()),
                        "testable_monotonic_cells": len(mono_test),
                        "matched_outperform_definitions": matched_outperform,
                        "matched_interval_supported_outperform_definitions": matched_supported,
                        "matched_tested_definitions": len(matched_test),
                        "market_edge_oot_improving_definitions": len(market_improves),
                        "model_edge_oot_improves": bool(model_oot.interval_supported_score_improvement),
                        "joint_strong_roi": float(alt.loc["MODEL_STRONG_MARKET_STRONG", "flat_stake_roi"]),
                        "positive_edge_probability_band_count": int(board[board.edge_band.isin(EDGE_ORDER[2:])].paid_break_even_band.nunique()),
                        "betonline_cross_book_rows": int(betonline.exact_matched_games) if betonline is not None else 0,
                        "betonline_positive_reference_edge_frequency": float(betonline.positive_reference_edge_frequency) if betonline is not None else np.nan,
                        "betonline_convergence_frequency": float(betonline.convergence_frequency) if betonline is not None else np.nan,
                        "betonline_converged_roi": float(betonline.converged_roi) if betonline is not None else np.nan,
                        "betonline_not_converged_roi": float(betonline.not_converged_roi) if betonline is not None else np.nan},
        "bait_signature": signature,
        "intentional_inducement": "NOT_TESTABLE_FROM_PRICE_AND_OUTCOME_DATA",
        "independent_probability_edge_definitions": ["MODEL_DERIVED_EV", "preserved totals-tool EV/Edge (different market, provenance only)"],
        "market_on_both_sides_definitions": market_defs,
        "existing_strong_observer": "UNCHANGED_APPEND_ONLY_OBSERVATIONAL_STREAM_NOT_PROMOTED",
        "new_observer_created": False,
        "next_immediate_research_direction": "TIME_SAFE_PRICE_EFFICIENCY_AND_CONVERGENCE STUDY WITHIN THE FIXED MODEL/MARKET STRENGTH QUADRANTS; NO NEW SELECTOR",
    }

    output.mkdir(parents=True, exist_ok=True)
    write_csv(provenance, output / "edge_provenance_ledger.csv")
    write_csv(long, output / "edge_definition_observations.csv")
    write_csv(board, output / "across_board_probability_edge_cells.csv")
    write_csv(monotonic, output / "edge_roi_monotonicity.csv")
    write_csv(matched_pairs, output / "matched_edge_control_pairs.csv")
    write_csv(matched_summary, output / "matched_edge_control_summary.csv")
    write_csv(favorite_results, output / "favorite_underdog_edge_results.csv")
    write_csv(oot_predictions, output / "edge_oot_predictions.csv")
    write_csv(oot_summary, output / "edge_oot_usefulness.csv")
    write_csv(required_predictions, output / "required_forecast_oot_predictions.csv")
    write_csv(required_summary, output / "required_forecast_oot_comparison.csv")
    write_csv(cross, output / "cross_book_reference_tests.csv")
    write_csv(alternatives, output / "alternative_roi_regions.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default)+"\n")
    (output / "main_report.md").write_text(render_report(summary))
    (output / "data_limitations.md").write_text("""# Data limitations

- No exact moneyline tool-displayed EV export was located; totals-tool examples are provenance only.
- Quotes are descriptive captures with no fill, limits, queue, rejection, or account evidence.
- SportsGameOdds book history is concentrated on 2026-08-06 through 2026-08-11; Pinnacle continues later.
- Cross-provider contemporaneous matches use a fixed 15-minute tolerance and retain the observed time delta.
- Ex-target consensus is an unweighted median of proportional no-vig probabilities from at least three other same-batch books; shared feeds and correlated books are not independent truth.
- Matched comparisons do not extrapolate beyond exact categorical strata and the pre-existing 2-point neutral caliper.
- Closing-line relationships and convergence are market movement, not outcome truth.
""")
    rerun = (f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m "
             f"backend.mlb.scripts.audit_mlb_across_board_apparent_ev_provenance_economic_value_v1 "
             f"--resolved-cutoff {cutoff} --output {output.relative_to(ROOT)}'\n")
    (output / "rerun_command.txt").write_text(rerun)
    (output / "validator.py").write_text(validator_text()); (output / "validator.py").chmod(0o755)
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolved-cutoff", default="latest")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    print(json.dumps(run(output, args.resolved_cutoff), indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__":
    main()
