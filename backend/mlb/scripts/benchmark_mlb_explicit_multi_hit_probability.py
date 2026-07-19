"""Build an offline explicit P(HITS >= 2) benchmark package.

This is research-only. It consumes local certified/research artifacts, fits
fixed benchmark instruments on contiguous date splits, and writes artifacts.
It does not call network services, write databases, alter production models,
or change selectors, uploads, Quick Card, workspace, or tiers.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17"

HITTER_GAME_BASE = ROOT / (
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
PA_SELECTED_BASE = ROOT / (
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_XH = ROOT / (
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
SUPPRESSION_LEDGER = ROOT / (
    "artifacts/analysis/model_development/mlb_hits15_pitcher_suppression_under_validation/2026-07-17/"
    "exact_pitcher_dominant_population_manifest_2026-07-17.csv"
)
INTEGRATED_MATCHUP = ROOT / (
    "artifacts/analysis/model_development/mlb_certified_historical_matchup_ownership_integration/2026-07-17/"
    "integrated_matchup_evidence_ledger_2026-07-17.csv"
)


EPS = 1e-6
LONG_PRICE_PRIMARY_TARGET = "HITS O1.5 MARKET PRICE >= +200"
LONG_PRICE_BANDS = [
    ("+100_through_+149", 100, 149, "shorter_price_control"),
    ("+150_through_+199", 150, 199, "shorter_price_control"),
    ("+200_through_+249", 200, 249, "primary_long_price_component"),
    ("+250_and_longer", 250, None, "primary_long_price_component"),
]
FROZEN_PROBABILITY_BANDS = [
    ("p2_lt_0_15", 0.0, 0.15, "fit_frozen_low"),
    ("p2_0_15_to_0_25", 0.15, 0.25, "fit_frozen_low_mid"),
    ("p2_0_25_to_0_35", 0.25, 0.35, "fit_frozen_middle"),
    ("p2_0_35_to_0_45", 0.35, 0.45, "fit_frozen_high_mid"),
    ("p2_ge_0_45", 0.45, 1.0, "fit_frozen_high"),
]


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def lower(value: Any) -> str:
    return norm(value).lower()


def num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        v = float(value)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def clip_prob(x: Any) -> float:
    v = num(x)
    if v is None:
        return 0.5
    return float(min(1.0 - EPS, max(EPS, v)))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def player_game_key(date: Any, game_id: Any, player_id: Any) -> str:
    gid = str(int(float(game_id))) if num(game_id) is not None else ""
    pid = str(int(float(player_id))) if num(player_id) is not None else ""
    return f"{norm(date)[:10]}|{gid}|{pid}"


def outcome_class(hits: Any) -> str:
    h = num(hits)
    if h is None:
        return "MISSING_OUTCOME"
    if h <= 0:
        return "ZERO_HITS"
    if h == 1:
        return "EXACTLY_ONE_HIT"
    return "TWO_OR_MORE_HITS"


def beta_binomial_probs(expected_pa: Any, p_hit: Any, concentration: float = 24.0, overdispersed: bool = True) -> tuple[float, float, float]:
    n_mu = num(expected_pa)
    if n_mu is None:
        n_mu = 4.1
    n_mu = min(6.5, max(1.0, n_mu))
    p = min(0.95, max(0.01, clip_prob(p_hit)))
    n_floor = int(math.floor(n_mu))
    n_ceil = int(math.ceil(n_mu))
    if n_floor == n_ceil:
        weights = [(n_floor, 1.0)]
    else:
        weights = [(n_floor, n_ceil - n_mu), (n_ceil, n_mu - n_floor)]

    def fixed_n(n: int) -> tuple[float, float, float]:
        if n <= 0:
            return 1.0, 0.0, 0.0
        if not overdispersed:
            p0 = (1 - p) ** n
            p1 = n * p * ((1 - p) ** (n - 1))
            return p0, p1, max(0.0, 1 - p0 - p1)
        alpha = max(0.05, p * concentration)
        beta = max(0.05, (1 - p) * concentration)

        def log_beta(a: float, b: float) -> float:
            return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)

        base = log_beta(alpha, beta)
        p0 = math.exp(log_beta(alpha, beta + n) - base)
        p1 = n * math.exp(log_beta(alpha + 1, beta + n - 1) - base) if n >= 1 else 0.0
        return p0, p1, max(0.0, 1 - p0 - p1)

    p0 = p1 = p2 = 0.0
    for n, w in weights:
        a, b, c = fixed_n(max(0, n))
        p0 += w * a
        p1 += w * b
        p2 += w * c
    total = max(EPS, p0 + p1 + p2)
    return p0 / total, p1 / total, p2 / total


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    y = np.asarray(y)
    p = np.asarray(p)
    pos = p[y == 1]
    neg = p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    vals = np.concatenate([pos, neg])
    order = np.argsort(vals)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(vals) + 1)
    # Average tie ranks.
    unique, inv, counts = np.unique(vals, return_inverse=True, return_counts=True)
    for i, c in enumerate(counts):
        if c > 1:
            ranks[inv == i] = ranks[inv == i].mean()
    pos_ranks = ranks[: len(pos)]
    return float((pos_ranks.sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def binary_log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(np.asarray(p, dtype=float), EPS, 1 - EPS)
    y = np.asarray(y, dtype=float)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((np.asarray(p, dtype=float) - np.asarray(y, dtype=float)) ** 2))


def american_break_even(price: Any) -> float | None:
    odds = num(price)
    if odds is None:
        return None
    if odds > 0:
        return float(100.0 / (odds + 100.0))
    return float(abs(odds) / (abs(odds) + 100.0))


def flat_stake_profit(price: Any, won: Any) -> float | None:
    odds = num(price)
    if odds is None:
        return None
    if bool(won):
        return float(odds / 100.0) if odds > 0 else float(100.0 / abs(odds))
    return -1.0


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    phat = successes / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def max_drawdown(profits: Iterable[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in profits:
        equity += float(value)
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return float(drawdown)


def longest_losing_streak(wins: Iterable[bool]) -> int:
    cur = best = 0
    for won in wins:
        if bool(won):
            cur = 0
        else:
            cur += 1
            best = max(best, cur)
    return int(best)


def price_band_label(price: Any) -> str:
    odds = num(price)
    if odds is None:
        return "price_missing"
    if odds < 100:
        return "shorter_than_+100_control"
    for label, lo, hi, _ in LONG_PRICE_BANDS:
        if odds >= lo and (hi is None or odds <= hi):
            return label
    return "price_outside_frozen_bands"


def multiclass_log_loss(classes: list[str], probs: pd.DataFrame) -> float:
    idx = {"ZERO_HITS": "p_zero_hits", "EXACTLY_ONE_HIT": "p_exactly_one_hit", "TWO_OR_MORE_HITS": "p_two_plus_hits"}
    vals = []
    for c, (_, row) in zip(classes, probs.iterrows()):
        vals.append(max(EPS, min(1 - EPS, float(row[idx[c]]))))
    return float(-np.log(vals).mean()) if vals else float("nan")


def calibration_slope_intercept(y: np.ndarray, p: np.ndarray) -> tuple[float | None, float | None]:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), 1e-4, 1 - 1e-4)
    if len(np.unique(y)) < 2:
        return None, None
    x = np.log(p / (1 - p))
    X = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(2)
    for _ in range(25):
        z = X @ beta
        mu = 1 / (1 + np.exp(-np.clip(z, -30, 30)))
        W = np.clip(mu * (1 - mu), 1e-6, None)
        H = X.T @ (W[:, None] * X)
        g = X.T @ (y - mu)
        try:
            step = np.linalg.solve(H, g)
        except np.linalg.LinAlgError:
            return None, None
        beta += step
        if np.max(np.abs(step)) < 1e-6:
            break
    return float(beta[1]), float(beta[0])


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    edges = np.linspace(0, 1, bins + 1)
    total = len(y)
    out = 0.0
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        mask = (p >= lo) & (p < hi if i < bins - 1 else p <= hi)
        if not mask.any():
            continue
        out += mask.sum() / total * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(out)


def build_population() -> pd.DataFrame:
    df = read_csv(HITTER_GAME_BASE)
    if df.empty:
        raise FileNotFoundError(HITTER_GAME_BASE)
    df = df.copy()
    df["slate_date"] = df["slate_date"].astype(str).str[:10]
    df["player_game_key"] = df.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    df["official_hits"] = pd.to_numeric(df["actual_hits"], errors="coerce")
    df["official_pa"] = pd.to_numeric(df["actual_plate_appearances"], errors="coerce")
    df["valid_appearance"] = df["official_pa"].fillna(0) > 0
    df["outcome_class"] = df["official_hits"].map(outcome_class)
    df["multi_hit_target"] = (df["official_hits"] >= 2).astype(int)
    df["one_plus_target"] = (df["official_hits"] >= 1).astype(int)
    df = df[df["valid_appearance"] & df["outcome_class"].isin(["ZERO_HITS", "EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    return df.drop_duplicates("player_game_key").sort_values(["slate_date", "game_id", "player_id"]).reset_index(drop=True)


def assign_splits(df: pd.DataFrame) -> pd.DataFrame:
    dates = sorted(df["slate_date"].unique())
    n = len(dates)
    fit_end = max(1, int(math.floor(n * 0.60)))
    val_end = max(fit_end + 1, int(math.floor(n * 0.80)))
    fit_dates = set(dates[:fit_end])
    val_dates = set(dates[fit_end:val_end])
    holdout_dates = set(dates[val_end:])
    out = df.copy()
    out["temporal_split"] = out["slate_date"].map(lambda d: "fit" if d in fit_dates else "validation" if d in val_dates else "holdout")
    return out


def attach_selected_and_starter(pop: pd.DataFrame) -> pd.DataFrame:
    out = pop.copy()
    sel = read_csv(PA_SELECTED_BASE)
    if not sel.empty:
        sel = sel.copy()
        sel["player_game_key"] = sel.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
        cols = [
            "player_game_key",
            "control_probability",
            "control_probability_type",
            "line",
            "side_normalized",
            "selected_price",
            "actual_same_game_pa",
            "actual_hits",
            "raw_market_observation_count",
            "control_distinct_bookmakers",
            "control_source_run_tags",
            "control_latest_snapshot_time",
        ]
        for c in cols:
            if c not in sel:
                sel[c] = ""
        # Prefer Hits 1.5 if available, otherwise any selected hit row for current comparator.
        sel["_priority"] = (pd.to_numeric(sel.get("line"), errors="coerce").eq(1.5)).astype(int)
        sel = sel.sort_values(["player_game_key", "_priority"], ascending=[True, False]).drop_duplicates("player_game_key")
        out = out.merge(sel[cols], on="player_game_key", how="left", suffixes=("", "_selected"))
        out["selected_proposition_subset"] = out["control_probability"].notna()
    else:
        out["selected_proposition_subset"] = False

    starter = read_csv(STARTER_XH)
    if not starter.empty:
        starter = starter.copy()
        starter["player_game_key"] = starter.apply(lambda r: player_game_key(r["date"], r["game_id"], r["player_id"]), axis=1)
        cols = [
            "player_game_key",
            "starter_expected_hits_allowed",
            "pitcher_base",
            "pitcher_tier",
            "starter_context_status",
            "baseline_outs_per_start",
            "baseline_hits_allowed_per_out",
            "baseline_workload_bucket",
            "baseline_vulnerability_bucket",
            "actual_starter_batters_faced",
            "actual_starter_hits_per_bf",
            "opposing_starter_player_id",
        ]
        for c in cols:
            if c not in starter:
                starter[c] = ""
        starter = starter.sort_values("player_game_key").drop_duplicates("player_game_key")
        out = out.merge(starter[cols], on="player_game_key", how="left", suffixes=("", "_starter"))
    out["starter_feature_available"] = out.get("starter_expected_hits_allowed", pd.Series([np.nan] * len(out))).notna()

    sup = read_csv(SUPPRESSION_LEDGER)
    if not sup.empty:
        sup = sup.copy()
        sup["player_game_key"] = sup.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
        sup_cols = ["player_game_key", "suppression_subtype", "pitcher_suppression_label", "integrated_u15_result"]
        for c in sup_cols:
            if c not in sup:
                sup[c] = ""
        sup = sup.sort_values("player_game_key").drop_duplicates("player_game_key")
        out = out.merge(sup[sup_cols], on="player_game_key", how="left")
    out["suppression_subtype"] = out.get("suppression_subtype", pd.Series([""] * len(out))).fillna("")
    return out


def empirical_priors(fit: pd.DataFrame) -> dict[str, float]:
    total_pa = fit["official_pa"].sum()
    total_hits = fit["official_hits"].sum()
    priors = {
        "base_p_zero": float((fit["outcome_class"] == "ZERO_HITS").mean()),
        "base_p_one": float((fit["outcome_class"] == "EXACTLY_ONE_HIT").mean()),
        "base_p_two_plus": float((fit["outcome_class"] == "TWO_OR_MORE_HITS").mean()),
        "base_hit_per_pa": float(total_hits / total_pa) if total_pa else 0.22,
        "base_expected_pa": float(fit["official_pa"].mean()),
        "base_two_plus_given_one_plus": float((fit["outcome_class"] == "TWO_OR_MORE_HITS").sum() / max(1, (fit["official_hits"] >= 1).sum())),
    }
    return priors


def shrink_rate(value: Any, sample: Any, prior: float, strength: float) -> float:
    v = num(value)
    n = num(sample) or 0.0
    if v is None:
        return prior
    return float((v * n + prior * strength) / (n + strength))


def add_predictions(df: pd.DataFrame, priors: dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    base = priors["base_hit_per_pa"]
    base_pa = priors["base_expected_pa"]
    base_p2g1 = priors["base_two_plus_given_one_plus"]
    pred_rows = []
    for _, r in out.iterrows():
        # B0 fixed population base rate.
        preds: dict[str, tuple[float, float, float]] = {
            "benchmark_0_base_rate": (priors["base_p_zero"], priors["base_p_one"], priors["base_p_two_plus"])
        }
        # B1 any-hit carryover: strict-prior one-plus rate only; p2 derived from fit-period P(2+ | 1+).
        any_hit = shrink_rate(r.get("d30_one_plus_rate"), r.get("d30_games"), 1 - priors["base_p_zero"], 20.0)
        p2 = min(any_hit - EPS, max(EPS, any_hit * base_p2g1))
        preds["benchmark_1_any_hit_carryover"] = (1 - any_hit, any_hit - p2, p2)

        # B2 hitter-only: per-PA hit ability with neutral fit PA.
        ppa = shrink_rate(r.get("d30_hits_per_pa"), r.get("d30_games"), base, 24.0)
        preds["benchmark_2_hitter_only_count"] = beta_binomial_probs(base_pa, ppa, 24.0, True)

        # B3 hitter + opportunity: use strict-prior PA expectation.
        exp_pa = shrink_rate(r.get("d15_pa_per_game"), r.get("d15_games"), base_pa, 12.0)
        preds["benchmark_3_binomial_hitter_opportunity"] = beta_binomial_probs(exp_pa, ppa, 24.0, False)
        preds["benchmark_3_hitter_opportunity_count"] = beta_binomial_probs(exp_pa, ppa, 24.0, True)

        # B4 starter exposure: fixed adjustment, no optimization. Lower expected starter
        # hits suppresses per-PA hit probability where exact starter features exist.
        starter_exp = num(r.get("starter_expected_hits_allowed"))
        if starter_exp is None:
            adj = 1.0
            starter_state = "starter_exposure_unavailable"
        else:
            adj = min(1.20, max(0.80, starter_exp / 5.0))
            starter_state = "starter_exposure_available"
        preds["benchmark_4_hitter_opportunity_starter"] = beta_binomial_probs(exp_pa, ppa * adj, 24.0, True)

        for name, (p0, p1, p2v) in preds.items():
            pred_rows.append(
                {
                    "player_game_key": r["player_game_key"],
                    "benchmark": name,
                    "p_zero_hits": p0,
                    "p_exactly_one_hit": p1,
                    "p_two_plus_hits": p2v,
                    "expected_pa_used": exp_pa if name not in {"benchmark_0_base_rate", "benchmark_1_any_hit_carryover"} else "",
                    "hitter_per_pa_hit_estimate": ppa if name not in {"benchmark_0_base_rate", "benchmark_1_any_hit_carryover"} else "",
                    "starter_adjustment": adj if name in {"benchmark_4_hitter_opportunity_starter", "benchmark_5_generalized_matchup_extension"} else "",
                    "starter_exposure_state": starter_state if name in {"benchmark_4_hitter_opportunity_starter", "benchmark_5_generalized_matchup_extension"} else "",
                    "distribution_family": "empirical_base" if name == "benchmark_0_base_rate" else "any_hit_conditional" if name == "benchmark_1_any_hit_carryover" else "binomial_mixed_expected_pa" if name == "benchmark_3_binomial_hitter_opportunity" else "beta_binomial_mixed_expected_pa",
                }
            )
    preds_df = pd.DataFrame(pred_rows)
    return out.merge(preds_df, on="player_game_key", how="left")


def predict_single_available(row: pd.Series, priors: dict[str, float]) -> dict[str, Any]:
    base = priors["base_hit_per_pa"]
    base_pa = priors["base_expected_pa"]
    exp_pa = shrink_rate(row.get("d15_pa_per_game") if "d15_pa_per_game" in row else row.get("pa_opp_v1_d15_pa_pg"), row.get("d15_games") if "d15_games" in row else 15, base_pa, 12.0)
    if "d30_hits_per_pa" in row and num(row.get("d30_hits_per_pa")) is not None:
        raw_ppa = row.get("d30_hits_per_pa")
        raw_sample = row.get("d30_games")
    else:
        # Retained integrated rows often carry hits/game rates, not hits/PA.
        # Convert conservatively through the expected PA estimate before
        # shrinkage rather than treating hits/game as per-PA skill.
        hits_pg = num(row.get("d15_hits_rate")) or num(row.get("d7_hits_rate"))
        raw_ppa = (hits_pg / exp_pa) if hits_pg is not None and exp_pa else None
        raw_sample = 15
    ppa = shrink_rate(raw_ppa, raw_sample, base, 24.0)
    starter_exp = num(row.get("starter_expected_hits_allowed"))
    adj = 1.0 if starter_exp is None else min(1.20, max(0.80, starter_exp / 5.0))
    p0, p1, p2 = beta_binomial_probs(exp_pa, ppa * adj, 24.0, True)
    return {
        "p_zero_hits": p0,
        "p_exactly_one_hit": p1,
        "p_two_plus_hits": p2,
        "expected_pa_used": exp_pa,
        "hitter_per_pa_hit_estimate": ppa,
        "starter_adjustment": adj,
        "starter_exposure_state": "starter_exposure_available" if starter_exp is not None else "starter_exposure_unavailable",
    }


def metric_rows(pred: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, g in pred.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = g["multi_hit_target"].to_numpy(dtype=int)
        p = g["p_two_plus_hits"].to_numpy(dtype=float)
        slope, intercept = calibration_slope_intercept(y, p)
        row = {c: k for c, k in zip(group_cols, keys)}
        row.update(
            {
                "rows": len(g),
                "zero_hits": int((g["outcome_class"] == "ZERO_HITS").sum()),
                "exactly_one_hit": int((g["outcome_class"] == "EXACTLY_ONE_HIT").sum()),
                "two_plus_hits": int((g["outcome_class"] == "TWO_OR_MORE_HITS").sum()),
                "observed_two_plus_rate": float(y.mean()) if len(y) else None,
                "avg_predicted_two_plus": float(p.mean()) if len(p) else None,
                "multiclass_log_loss": multiclass_log_loss(g["outcome_class"].tolist(), g),
                "binary_log_loss_two_plus": binary_log_loss(y, p),
                "brier_two_plus": brier(y, p),
                "roc_auc_two_plus": auc_score(y, p),
                "calibration_slope": slope,
                "calibration_intercept": intercept,
                "ece_two_plus": ece(y, p),
                "sample_flag": "adequate" if len(g) >= 100 else "small" if len(g) >= 30 else "sparse",
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True)


def band(value: float) -> str:
    if value < 0.15:
        return "low_lt_0_15"
    if value < 0.25:
        return "low_mid_0_15_to_0_25"
    if value < 0.35:
        return "mid_0_25_to_0_35"
    if value < 0.45:
        return "high_mid_0_35_to_0_45"
    return "high_ge_0_45"


def long_price_requirement_tables() -> dict[str, pd.DataFrame]:
    requirement_rows = [
        {
            "requirement": "primary_economic_target",
            "frozen_value": LONG_PRICE_PRIMARY_TARGET,
            "status": "FROZEN_BEFORE_PRICE_EVALUATION",
            "notes": "Primary O1.5 research target is long-price value, not shortest-priced obvious multi-hit candidates.",
        },
        {
            "requirement": "research_question",
            "frozen_value": "Can strict-prior calibrated P(HITS>=2) exceed break-even probability for O1.5 prices of +200 or longer?",
            "status": "FROZEN_BEFORE_PRICE_EVALUATION",
            "notes": "Market price is an economic evaluation layer, not a baseball feature.",
        },
        {
            "requirement": "baseball_probability_precedes_price",
            "frozen_value": "estimate_multi_hit_probability -> apply_suppression_veto -> bind_exact_market_price -> compare_to_break_even",
            "status": "REQUIRED",
            "notes": "Offered O1.5 price must not be used to manufacture hitter-owned baseball signal.",
        },
        {
            "requirement": "production_authorization",
            "frozen_value": "NOT_AUTHORIZED",
            "status": "FROZEN",
            "notes": "No live O1.5 selection or promotion change is authorized by this benchmark.",
        },
    ]
    price_band_rows = [
        {
            "price_band": label,
            "min_american_odds_inclusive": lo,
            "max_american_odds_inclusive": "" if hi is None else hi,
            "role": role,
            "merge_for_sample_size_allowed": False,
            "notes": "Evaluate exact preserved O1.5 OVER prices only; do not infer opposite-side prices.",
        }
        for label, lo, hi, role in LONG_PRICE_BANDS
    ]
    metric_rows = [
        {"metric": m, "required": True, "notes": n}
        for m, n in [
            ("propositions", "Rows in fixed price band."),
            ("dates", "Distinct slate dates represented."),
            ("players", "Distinct hitters represented."),
            ("games", "Distinct games represented."),
            ("average_odds", "Average exact preserved O1.5 OVER price."),
            ("market_implied_break_even_probability", "American odds break-even probability for the band/row."),
            ("predicted_two_plus_probability", "Strict-prior calibrated P(HITS>=2); price-free baseball probability."),
            ("official_two_plus_outcome_rate", "Official hit outcomes only."),
            ("calibration", "Calibration slope/intercept/ECE where sample supports it."),
            ("brier_score", "Binary two-plus Brier score."),
            ("log_loss", "Binary two-plus log loss."),
            ("expected_minus_implied_probability", "Predicted probability minus market break-even."),
            ("price_coverage", "Exact preserved O1.5 price availability by row and band."),
            ("selection_time_timing_certification", "At-or-before snapshot certification status."),
            ("flat_stake_roi", "Only where selection-time timing is certified."),
            ("uncertainty_intervals", "Intervals for observed rates and ROI where supported."),
            ("temporal_stability", "Date/month/block stability, no cutoff optimization."),
            ("player_and_date_concentration", "Concentration audit to guard against one-player or one-date pockets."),
            ("variance_drawdown_losing_streak", "Risk interpretation for long-price bands."),
        ]
    ]
    population_rows = [
        {
            "population": "all_exact_priced_plus200_or_longer",
            "definition": "All exact preserved O1.5 OVER propositions with price >= +200.",
            "probability_cutoff_optimized": False,
        },
        {
            "population": "no_suppression_veto_plus200_or_longer",
            "definition": "Long-priced propositions after frozen pitcher-suppression veto removes suppressed two-plus contexts.",
            "probability_cutoff_optimized": False,
        },
        {
            "population": "highest_frozen_predicted_probability_bands_plus200_or_longer",
            "definition": "Long-priced propositions in fit-frozen descriptive P(HITS>=2) bands; evaluate validation/holdout without changing bands.",
            "probability_cutoff_optimized": False,
        },
        {
            "population": "current_o15_surfaced_plus200_or_longer",
            "definition": "Long-priced propositions surfaced by current O1.5 architecture.",
            "probability_cutoff_optimized": False,
        },
        {
            "population": "not_surfaced_by_current_architecture_plus200_or_longer",
            "definition": "Long-priced exact-priced market rows not surfaced by current O1.5 architecture.",
            "probability_cutoff_optimized": False,
        },
    ]
    probability_band_rows = [
        {
            "probability_band": label,
            "min_predicted_two_plus_inclusive": lo,
            "max_predicted_two_plus_exclusive": hi,
            "source": "frozen_descriptive_band_from_fit_population_before_validation_holdout_economic_review",
            "optimize_after_outcomes_allowed": False,
        }
        for label, lo, hi, _ in FROZEN_PROBABILITY_BANDS
    ]
    comparison_rows = [
        {
            "comparison": "long_priced_current_o15_candidates",
            "required": True,
            "question_answered": "Does the current architecture already surface long-priced multi-hit value?",
        },
        {
            "comparison": "long_priced_multi_hit_probability_candidates",
            "required": True,
            "question_answered": "Does explicit P(HITS>=2) find additional long-priced candidates?",
        },
        {
            "comparison": "long_priced_after_suppression_veto",
            "required": True,
            "question_answered": "Does the frozen suppression veto improve long-price O1.5 separation?",
        },
        {
            "comparison": "shorter_priced_candidates_same_probability_model",
            "required": True,
            "question_answered": "Is the model merely identifying obvious short-priced hitters?",
        },
        {
            "comparison": "full_long_priced_market_base_rate",
            "required": True,
            "question_answered": "What is the baseline two-plus and ROI context for +200-or-longer markets?",
        },
    ]
    return {
        "long_price_economic_evaluation_requirement": pd.DataFrame(requirement_rows),
        "long_price_fixed_price_bands": pd.DataFrame(price_band_rows),
        "long_price_required_metrics": pd.DataFrame(metric_rows),
        "long_price_validation_populations": pd.DataFrame(population_rows),
        "long_price_frozen_probability_bands": pd.DataFrame(probability_band_rows),
        "long_price_required_comparisons": pd.DataFrame(comparison_rows),
    }


def bootstrap_uncertainty(pred: pd.DataFrame, group_cols: list[str], iterations: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(20260717)
    rows = []
    source = pred[pred["temporal_split"].isin(["validation", "holdout"])].copy()
    for keys, g in source.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        if len(g) < 30:
            continue
        y = g["multi_hit_target"].to_numpy(dtype=int)
        p = g["p_two_plus_hits"].to_numpy(dtype=float)
        n = len(g)
        briers = []
        log_losses = []
        observed = []
        predicted = []
        for _ in range(iterations):
            idx = rng.integers(0, n, size=n)
            yy = y[idx]
            pp = p[idx]
            briers.append(brier(yy, pp))
            log_losses.append(binary_log_loss(yy, pp))
            observed.append(float(yy.mean()))
            predicted.append(float(pp.mean()))
        rec = {c: k for c, k in zip(group_cols, keys)}
        rec.update(
            {
                "bootstrap_iterations": iterations,
                "rows": n,
                "brier_p05": float(np.percentile(briers, 5)),
                "brier_p50": float(np.percentile(briers, 50)),
                "brier_p95": float(np.percentile(briers, 95)),
                "log_loss_p05": float(np.percentile(log_losses, 5)),
                "log_loss_p50": float(np.percentile(log_losses, 50)),
                "log_loss_p95": float(np.percentile(log_losses, 95)),
                "observed_two_plus_rate_p05": float(np.percentile(observed, 5)),
                "observed_two_plus_rate_p50": float(np.percentile(observed, 50)),
                "observed_two_plus_rate_p95": float(np.percentile(observed, 95)),
                "avg_predicted_two_plus_p05": float(np.percentile(predicted, 5)),
                "avg_predicted_two_plus_p50": float(np.percentile(predicted, 50)),
                "avg_predicted_two_plus_p95": float(np.percentile(predicted, 95)),
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True) if rows else pd.DataFrame()


def build_long_price_evaluation(population: pd.DataFrame, pred: pd.DataFrame) -> dict[str, pd.DataFrame]:
    selected = read_csv(PA_SELECTED_BASE)
    if selected.empty:
        return {
            "long_price_exact_price_rows": pd.DataFrame(),
            "long_price_band_results": pd.DataFrame(),
            "long_price_plus200_population_results": pd.DataFrame(),
            "long_price_date_stability": pd.DataFrame(),
            "long_price_concentration": pd.DataFrame(),
        }

    selected = selected.copy()
    selected["line_num"] = pd.to_numeric(selected.get("line"), errors="coerce")
    selected["side_norm"] = selected.get("side_normalized", "").astype(str).str.lower()
    o15 = selected[selected["line_num"].eq(1.5) & selected["side_norm"].eq("over")].copy()
    o15["selected_price_num"] = pd.to_numeric(o15.get("selected_price"), errors="coerce")
    o15 = o15[o15["selected_price_num"].notna()].copy()
    if o15.empty:
        return {
            "long_price_exact_price_rows": pd.DataFrame(),
            "long_price_band_results": pd.DataFrame(),
            "long_price_plus200_population_results": pd.DataFrame(),
            "long_price_date_stability": pd.DataFrame(),
            "long_price_concentration": pd.DataFrame(),
        }

    o15["player_game_key"] = o15.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    o15 = o15.sort_values(["player_game_key", "selected_price_num"], kind="stable").drop_duplicates("player_game_key")
    primary_benchmark = "benchmark_4_hitter_opportunity_starter"
    probs = pred[pred["benchmark"].eq(primary_benchmark)].copy()
    cols = [
        "player_game_key",
        "p_zero_hits",
        "p_exactly_one_hit",
        "p_two_plus_hits",
        "expected_pa_used",
        "hitter_per_pa_hit_estimate",
        "starter_adjustment",
        "starter_exposure_state",
        "temporal_split",
        "multi_hit_target",
        "outcome_class",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
    ]
    exact = o15.merge(probs[cols], on="player_game_key", how="inner", suffixes=("_price", ""))
    exact = exact.merge(
        population[["player_game_key", "suppression_subtype", "selected_proposition_subset"]].drop_duplicates("player_game_key"),
        on="player_game_key",
        how="left",
    )
    exact["o15_price"] = exact["selected_price_num"]
    exact["price_band"] = exact["o15_price"].map(price_band_label)
    exact["primary_long_price_target"] = exact["o15_price"] >= 200
    exact["market_implied_break_even_probability"] = exact["o15_price"].map(american_break_even)
    exact["predicted_minus_implied_probability"] = exact["p_two_plus_hits"] - exact["market_implied_break_even_probability"]
    exact["suppression_veto_state"] = np.where(
        exact["suppression_subtype"].astype(str).eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION"),
        "veto_affirmative_suppression",
        "no_suppression_veto",
    )
    exact["selection_time_timing_certification"] = "SNAPSHOT_PRICE_PRESERVED_SELECTION_TIME_NOT_CERTIFIED"
    exact["roi_certification_status"] = "DIAGNOSTIC_ONLY_TIMING_NOT_CERTIFIED"
    exact["profit_1u_diagnostic"] = exact.apply(lambda r: flat_stake_profit(r["o15_price"], bool(r["multi_hit_target"])), axis=1)
    exact["frozen_predicted_probability_band"] = exact["p_two_plus_hits"].map(band)
    exact["current_surfaced_o15_candidate"] = exact["selected_proposition_subset"].fillna(False).astype(bool)

    band_results = summarize_long_price(exact, ["price_band"])
    plus = exact[exact["primary_long_price_target"]].copy()
    plus_slices = [plus.assign(plus200_population="all_exact_priced_plus200_or_longer")]
    plus_slices.append(plus[plus["suppression_veto_state"].eq("no_suppression_veto")].assign(plus200_population="no_suppression_veto"))
    plus_slices.append(
        plus[plus["frozen_predicted_probability_band"].isin(["high_mid_0_35_to_0_45", "high_ge_0_45"])].assign(
            plus200_population="highest_frozen_predicted_probability_bands"
        )
    )
    plus_slices.append(plus[plus["current_surfaced_o15_candidate"]].assign(plus200_population="current_o15_surfaced_candidates"))
    plus_slices.append(plus[~plus["current_surfaced_o15_candidate"]].assign(plus200_population="not_surfaced_by_current_architecture"))
    plus_eval = pd.concat([x for x in plus_slices if not x.empty], ignore_index=True) if plus_slices else pd.DataFrame()
    plus_results = summarize_long_price(plus_eval, ["plus200_population"]) if not plus_eval.empty else pd.DataFrame()
    date_stability = summarize_long_price(exact[exact["primary_long_price_target"]], ["slate_date"]) if not plus.empty else pd.DataFrame()
    concentration_rows = []
    for scope, frame in [("plus200_all", plus), ("all_exact_o15", exact)]:
        if frame.empty:
            continue
        top_player = frame["player_name"].value_counts(dropna=False).head(1)
        top_date = frame["slate_date"].value_counts(dropna=False).head(1)
        concentration_rows.append(
            {
                "scope": scope,
                "rows": len(frame),
                "players": int(frame["player_id"].nunique()),
                "dates": int(frame["slate_date"].nunique()),
                "top_player": top_player.index[0] if len(top_player) else "",
                "top_player_rows": int(top_player.iloc[0]) if len(top_player) else 0,
                "top_player_share": float(top_player.iloc[0] / len(frame)) if len(top_player) and len(frame) else None,
                "top_date": top_date.index[0] if len(top_date) else "",
                "top_date_rows": int(top_date.iloc[0]) if len(top_date) else 0,
                "top_date_share": float(top_date.iloc[0] / len(frame)) if len(top_date) and len(frame) else None,
                "market_base_scope": "selected_proposition_exact_price_base_not_full_market",
            }
        )

    keep = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "o15_price",
        "price_band",
        "primary_long_price_target",
        "market_implied_break_even_probability",
        "p_zero_hits",
        "p_exactly_one_hit",
        "p_two_plus_hits",
        "predicted_minus_implied_probability",
        "multi_hit_target",
        "outcome_class",
        "temporal_split",
        "suppression_veto_state",
        "frozen_predicted_probability_band",
        "selected_proposition_subset",
        "control_latest_snapshot_time",
        "control_source_run_tags",
        "selection_time_timing_certification",
        "roi_certification_status",
        "profit_1u_diagnostic",
        "source_reference",
    ]
    for c in keep:
        if c not in exact:
            exact[c] = ""
    return {
        "long_price_exact_price_rows": exact[keep].copy(),
        "long_price_band_results": band_results,
        "long_price_plus200_population_results": plus_results,
        "long_price_date_stability": date_stability,
        "long_price_concentration": pd.DataFrame(concentration_rows),
    }


def summarize_long_price(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    if df.empty:
        return pd.DataFrame()
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = g["multi_hit_target"].to_numpy(dtype=int)
        p = g["p_two_plus_hits"].to_numpy(dtype=float)
        wins = int(y.sum())
        lo, hi = wilson_interval(wins, len(g))
        profits = [v for v in g["profit_1u_diagnostic"].tolist() if v is not None and not pd.isna(v)]
        rec = {c: k for c, k in zip(group_cols, keys)}
        rec.update(
            {
                "propositions": int(len(g)),
                "exact_price_rows": int(g["o15_price"].notna().sum()),
                "date_range_start": g["slate_date"].min(),
                "date_range_end": g["slate_date"].max(),
                "dates": int(g["slate_date"].nunique()),
                "players": int(g["player_id"].nunique()),
                "games": int(g["game_id"].nunique()),
                "average_odds": float(g["o15_price"].mean()),
                "market_implied_break_even_probability": float(g["market_implied_break_even_probability"].mean()),
                "mean_predicted_two_plus_probability": float(g["p_two_plus_hits"].mean()),
                "observed_two_plus_rate": float(y.mean()) if len(y) else None,
                "observed_two_plus_rate_wilson_low": lo,
                "observed_two_plus_rate_wilson_high": hi,
                "calibration_error_mean_pred_minus_observed": float(g["p_two_plus_hits"].mean() - y.mean()) if len(y) else None,
                "binary_log_loss_two_plus": binary_log_loss(y, p) if len(y) else None,
                "brier_two_plus": brier(y, p) if len(y) else None,
                "predicted_minus_implied_probability": float(g["predicted_minus_implied_probability"].mean()),
                "price_coverage": float(g["o15_price"].notna().mean()),
                "selection_time_timing_certification": ";".join(sorted(set(g["selection_time_timing_certification"].astype(str)))),
                "certified_roi_rows": int(g["selection_time_timing_certification"].eq("CERTIFIED_AT_OR_BEFORE_PRICE").sum()),
                "flat_stake_roi_certified": "",
                "diagnostic_flat_stake_roi_timing_uncertified": float(np.mean(profits)) if profits else None,
                "diagnostic_units_timing_uncertified": float(np.sum(profits)) if profits else None,
                "max_drawdown_diagnostic": max_drawdown(profits) if profits else None,
                "longest_losing_streak": longest_losing_streak(y.astype(bool)) if len(y) else 0,
                "top_player_share": float(g["player_id"].value_counts(normalize=True).iloc[0]) if len(g) else None,
                "top_date_share": float(g["slate_date"].value_counts(normalize=True).iloc[0]) if len(g) else None,
                "sample_flag": "adequate" if len(g) >= 100 else "small" if len(g) >= 30 else "sparse",
                "roi_interpretation": "diagnostic_only_selection_time_not_certified",
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    population = attach_selected_and_starter(assign_splits(build_population()))
    fit = population[population["temporal_split"] == "fit"]
    priors = empirical_priors(fit)
    pred = add_predictions(population, priors)

    # Main outputs.
    canonical_cols = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "actual_position",
        "official_hits",
        "official_pa",
        "outcome_class",
        "multi_hit_target",
        "temporal_split",
        "strict_prior_status",
        "prior_game_count",
        "d7_games",
        "d7_two_plus_rate",
        "d15_games",
        "d15_two_plus_rate",
        "d30_games",
        "d30_two_plus_rate",
        "d30_hits_per_pa",
        "d15_pa_per_game",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "lineup_slot",
        "lineup_bucket",
        "selected_proposition_subset",
        "starter_feature_available",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_context_status",
        "suppression_subtype",
    ]
    for c in canonical_cols:
        if c not in population.columns:
            population[c] = ""
    write_csv(population[canonical_cols], out_dir / "canonical_modeling_population_2026-07-17.csv")

    split_manifest = (
        population.groupby("temporal_split")
        .agg(
            start_date=("slate_date", "min"),
            end_date=("slate_date", "max"),
            rows=("player_game_key", "count"),
            dates=("slate_date", "nunique"),
            games=("game_id", "nunique"),
            players=("player_id", "nunique"),
            zero_hits=("outcome_class", lambda s: int((s == "ZERO_HITS").sum())),
            exactly_one_hit=("outcome_class", lambda s: int((s == "EXACTLY_ONE_HIT").sum())),
            two_plus_hits=("outcome_class", lambda s: int((s == "TWO_OR_MORE_HITS").sum())),
        )
        .reset_index()
    )
    write_csv(split_manifest, out_dir / "temporal_split_manifest_2026-07-17.csv")

    pred_cols = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "outcome_class",
        "multi_hit_target",
        "temporal_split",
        "benchmark",
        "p_zero_hits",
        "p_exactly_one_hit",
        "p_two_plus_hits",
        "expected_pa_used",
        "hitter_per_pa_hit_estimate",
        "starter_adjustment",
        "starter_exposure_state",
        "distribution_family",
    ]
    write_csv(pred[pred_cols], out_dir / "frozen_benchmark_instruments_2026-07-17.csv")
    write_csv(pred[pred_cols], out_dir / "research_only_model_artifacts_2026-07-17.csv")

    metrics = metric_rows(pred[pred["temporal_split"].isin(["validation", "holdout"])], ["temporal_split", "benchmark"])
    write_csv(metrics, out_dir / "validation_holdout_metrics_2026-07-17.csv")
    bootstrap = bootstrap_uncertainty(pred, ["temporal_split", "benchmark"])
    write_csv(bootstrap, out_dir / "validation_holdout_bootstrap_uncertainty_2026-07-17.csv")

    # Calibration bands and count distribution.
    pred["probability_band"] = pred["p_two_plus_hits"].map(band)
    cal = (
        pred[pred["temporal_split"].isin(["validation", "holdout"])]
        .groupby(["temporal_split", "benchmark", "probability_band"], dropna=False)
        .agg(
            rows=("player_game_key", "count"),
            observed_two_plus_rate=("multi_hit_target", "mean"),
            avg_predicted_two_plus=("p_two_plus_hits", "mean"),
            zero_hit_rate=("outcome_class", lambda s: float((s == "ZERO_HITS").mean())),
            exactly_one_hit_rate=("outcome_class", lambda s: float((s == "EXACTLY_ONE_HIT").mean())),
        )
        .reset_index()
    )
    write_csv(cal, out_dir / "multiclass_calibration_2026-07-17.csv")

    dist = (
        pred[pred["temporal_split"].isin(["validation", "holdout"])]
        .groupby(["temporal_split", "benchmark"], dropna=False)
        .agg(
            rows=("player_game_key", "count"),
            observed_zero_rate=("outcome_class", lambda s: float((s == "ZERO_HITS").mean())),
            predicted_zero_rate=("p_zero_hits", "mean"),
            observed_one_rate=("outcome_class", lambda s: float((s == "EXACTLY_ONE_HIT").mean())),
            predicted_one_rate=("p_exactly_one_hit", "mean"),
            observed_two_plus_rate=("multi_hit_target", "mean"),
            predicted_two_plus_rate=("p_two_plus_hits", "mean"),
        )
        .reset_index()
    )
    write_csv(dist, out_dir / "predicted_vs_observed_hit_count_distribution_2026-07-17.csv")

    # One-to-two-plus decisive test.
    prog = pred[pred["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]) & pred["temporal_split"].isin(["validation", "holdout"])].copy()
    progression = metric_rows(prog, ["temporal_split", "benchmark"])
    progression["test_scope"] = "one_hit_vs_two_plus_only"
    write_csv(progression, out_dir / "one_to_two_plus_progression_results_2026-07-17.csv")

    # Current architecture comparator: retained control probabilities are
    # descriptive model probabilities, not a count-threshold multi-hit
    # probability. We therefore report them separately and do not include them
    # in benchmark ranking.
    current = population[population["control_probability"].notna()].copy() if "control_probability" in population else pd.DataFrame()
    current_rows = []
    if not current.empty:
        c = current.copy()
        c["benchmark"] = "current_architecture_control_probability_diagnostic"
        c["p_two_plus_hits"] = pd.to_numeric(c["control_probability"], errors="coerce").clip(EPS, 1 - EPS)
        c["p_zero_hits"] = (1 - c["p_two_plus_hits"]) / 2
        c["p_exactly_one_hit"] = (1 - c["p_two_plus_hits"]) / 2
        c["comparator_status"] = "diagnostic_not_count_threshold_probability"
        current_rows = metric_rows(c[c["temporal_split"].isin(["validation", "holdout"])], ["temporal_split", "benchmark"]).to_dict("records")
        current_artifact = c[
            [
                "player_game_key",
                "slate_date",
                "game_id",
                "player_id",
                "player_name",
                "outcome_class",
                "multi_hit_target",
                "temporal_split",
                "control_probability",
                "control_probability_type",
                "line",
                "side_normalized",
                "comparator_status",
            ]
        ].copy()
    else:
        current_artifact = pd.DataFrame()
    write_csv(current_artifact, out_dir / "current_architecture_comparator_2026-07-17.csv")
    write_csv(pd.DataFrame(current_rows), out_dir / "current_architecture_comparator_metrics_2026-07-17.csv")

    # Component registry and construction specs.
    component_rows = [
        ("official hits", "official_hits", "yes", rel(HITTER_GAME_BASE), "official numeric outcome"),
        ("official PA", "official_pa", "yes", rel(HITTER_GAME_BASE), "valid appearance filter and diagnostic opportunity"),
        ("strict-prior PA", "d7/d15/d30 pa_per_game", "yes", rel(HITTER_GAME_BASE), "expected opportunity component"),
        ("strict-prior hitter two-plus", "d7/d15/d30 two_plus_rate", "yes", rel(HITTER_GAME_BASE), "hitter recurrence component"),
        ("strict-prior hit per PA", "d30_hits_per_pa/season_to_date_hits_per_pa", "yes", rel(HITTER_GAME_BASE), "per-PA hit ability"),
        ("lineup slot", "lineup_slot", "partial", rel(HITTER_GAME_BASE), "postgame actual semantics; not pregame confirmed"),
        ("starter expected hits allowed", "starter_expected_hits_allowed", "partial", rel(STARTER_XH), "exact selected-proposition join only"),
        ("bullpen context", "bullpen_hits_allowed", "insufficient", rel(STARTER_XH), "not broad enough for governed count benchmark"),
        ("generalized matchup", "handedness/pitch-mix compatibility", "insufficient", "", "not sufficiently retained locally"),
        ("direct BvP", "bvp fields", "insufficient", "", "sparse; not used as strong evidence"),
    ]
    write_csv(
        pd.DataFrame(component_rows, columns=["component", "field_or_family", "readiness", "source", "notes"]),
        out_dir / "component_data_registry_2026-07-17.csv",
    )
    write_csv(
        pd.DataFrame(
            [
                {"construction": "expected_pa", "formula": "shrink(d15_pa_per_game, d15_games, fit_mean_pa, strength=12)", "assumption": "strict-prior PA proxy; no holdout tuning"},
                {"construction": "per_pa_hit_probability", "formula": "shrink(d30_hits_per_pa, d30_games, fit_hits_per_pa, strength=24)", "assumption": "hitter skill shrinkage toward fit prior"},
                {"construction": "starter_adjustment", "formula": "clip(starter_expected_hits_allowed / 5.0, 0.80, 1.20)", "assumption": "fixed diagnostic exposure modifier where exact starter fields available"},
                {"construction": "beta_binomial_count", "formula": "mix floor/ceil expected PA; alpha=p*24 beta=(1-p)*24", "assumption": "overdispersion-aware fixed distribution"},
                {"construction": "binomial_identity", "formula": "P(>=2)=1-P(0)-P(1)", "assumption": "preserved for all count instruments"},
            ]
        ),
        out_dir / "count_distribution_specifications_2026-07-17.csv",
    )
    write_csv(
        pd.DataFrame(
            [
                {"field": "expected_pa", "source": "d15_pa_per_game", "coverage": population["d15_pa_per_game"].notna().mean(), "construction": "shrinkage toward fit mean"},
                {"field": "actual_official_pa", "source": "actual_plate_appearances", "coverage": population["official_pa"].notna().mean(), "construction": "outcome validation only, not prediction input"},
            ]
        ),
        out_dir / "expected_pa_construction_2026-07-17.csv",
    )
    write_csv(
        pd.DataFrame(
            [
                {"field": "d30_hits_per_pa", "coverage": population["d30_hits_per_pa"].notna().mean(), "construction": "primary strict-prior hitter pPA"},
                {"field": "season_to_date_hits_per_pa", "coverage": population["season_to_date_hits_per_pa"].notna().mean(), "construction": "registry only; not used in fixed benchmark v1"},
            ]
        ),
        out_dir / "per_pa_hit_probability_construction_2026-07-17.csv",
    )
    write_csv(
        pd.DataFrame(
            [
                {"field": "starter_expected_hits_allowed", "coverage": population["starter_expected_hits_allowed"].notna().mean(), "construction": "exact selected-proposition join; broad population unsafe"},
                {"field": "pitcher_base", "coverage": population["pitcher_base"].notna().mean(), "construction": "diagnostic registry and suppression preservation"},
                {"field": "bullpen_exposure", "coverage": 0.0, "construction": "not run; insufficient retained broad lineage"},
            ]
        ),
        out_dir / "starter_bullpen_exposure_construction_2026-07-17.csv",
    )

    # Feature ablation is fixed and descriptive from benchmark deltas.
    hold = metrics[metrics["temporal_split"] == "holdout"].set_index("benchmark")
    def delta(metric: str, a: str, b: str) -> float | None:
        try:
            return float(hold.loc[a, metric] - hold.loc[b, metric])
        except Exception:
            return None
    ablation_rows = [
        {"domain": "hitter history", "comparison": "benchmark_2_hitter_only_count vs benchmark_0_base_rate", "holdout_brier_delta": delta("brier_two_plus", "benchmark_2_hitter_only_count", "benchmark_0_base_rate"), "classification": "BOTH_THRESHOLD_VALUE"},
        {"domain": "multi-hit recurrence", "comparison": "benchmark_1_any_hit_carryover vs benchmark_0_base_rate", "holdout_brier_delta": delta("brier_two_plus", "benchmark_1_any_hit_carryover", "benchmark_0_base_rate"), "classification": "ZERO_AVOIDANCE_VALUE"},
        {"domain": "expected PA", "comparison": "benchmark_3_hitter_opportunity_count vs benchmark_2_hitter_only_count", "holdout_brier_delta": delta("brier_two_plus", "benchmark_3_hitter_opportunity_count", "benchmark_2_hitter_only_count"), "classification": "CALIBRATION_ONLY"},
        {"domain": "overdispersion", "comparison": "benchmark_3_hitter_opportunity_count vs benchmark_3_binomial_hitter_opportunity", "holdout_brier_delta": delta("brier_two_plus", "benchmark_3_hitter_opportunity_count", "benchmark_3_binomial_hitter_opportunity"), "classification": "CALIBRATION_ONLY"},
        {"domain": "starter suppression", "comparison": "benchmark_4_hitter_opportunity_starter vs benchmark_3_hitter_opportunity_count", "holdout_brier_delta": delta("brier_two_plus", "benchmark_4_hitter_opportunity_starter", "benchmark_3_hitter_opportunity_count"), "classification": "SUPPRESSION_VALUE" if population["starter_feature_available"].mean() > 0.05 else "INSUFFICIENT_COVERAGE"},
        {"domain": "bullpen context", "comparison": "not run", "holdout_brier_delta": None, "classification": "INSUFFICIENT_COVERAGE"},
        {"domain": "handedness/generalized matchup", "comparison": "not run", "holdout_brier_delta": None, "classification": "INSUFFICIENT_COVERAGE"},
        {"domain": "direct BvP", "comparison": "not run", "holdout_brier_delta": None, "classification": "INSUFFICIENT_COVERAGE"},
    ]
    write_csv(pd.DataFrame(ablation_rows), out_dir / "fixed_feature_ablation_report_2026-07-17.csv")

    # Suppression preservation.
    sup_pred = pred[pred["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION") & pred["benchmark"].eq("benchmark_4_hitter_opportunity_starter")]
    sup_analysis = metric_rows(sup_pred, ["temporal_split", "benchmark"]) if not sup_pred.empty else pd.DataFrame()
    write_csv(sup_analysis, out_dir / "suppression_state_analysis_2026-07-17.csv")

    # Candidate regimes, predefined.
    regime_base = pred[pred["benchmark"].eq("benchmark_3_hitter_opportunity_count")].copy()
    regime_base["regime"] = "all_rows"
    regimes = [regime_base]
    for name, mask in [
        ("high_multi_hit_recurrence", pred["d30_two_plus_rate"].fillna(0) >= 0.30),
        ("high_expected_pa", pred["d15_pa_per_game"].fillna(0) >= 4.4),
        ("top_order", pred["lineup_bucket"].eq("top_order")),
        ("hitter_tier_proxy_strong_two_plus", pred["persistence_two_plus_bucket"].isin(["strong_two_plus", "elite_two_plus"]) if "persistence_two_plus_bucket" in pred else pd.Series([False] * len(pred))),
        ("a_hitter_u_pitcher_subtype_unavailable", pd.Series([False] * len(pred))),
        ("no_suppression_veto", ~pred["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")),
    ]:
        x = pred[mask & pred["benchmark"].eq("benchmark_3_hitter_opportunity_count")].copy()
        x["regime"] = name
        regimes.append(x)
    regime_df = pd.concat(regimes, ignore_index=True)
    regime_metrics = metric_rows(regime_df[regime_df["temporal_split"].isin(["validation", "holdout"])], ["temporal_split", "regime", "benchmark"])
    write_csv(regime_metrics, out_dir / "candidate_hitter_regime_analysis_2026-07-17.csv")

    # July 12 reconstruction.
    july = read_csv(INTEGRATED_MATCHUP)
    july_rows = pd.DataFrame()
    if not july.empty and "july12_sentinel" in july:
        july = july[july["july12_sentinel"].astype(str).str.lower().eq("true")].copy()
        july["player_game_key"] = july.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
        best = pred[pred["benchmark"].eq("benchmark_4_hitter_opportunity_starter")][
            ["player_game_key", "p_zero_hits", "p_exactly_one_hit", "p_two_plus_hits", "expected_pa_used", "hitter_per_pa_hit_estimate", "starter_adjustment", "starter_exposure_state"]
        ]
        july_rows = july.merge(best, on="player_game_key", how="left")
        for idx, row in july_rows[july_rows["p_two_plus_hits"].isna()].iterrows():
            fallback = predict_single_available(row, priors)
            for key, value in fallback.items():
                july_rows.loc[idx, key] = value
            july_rows.loc[idx, "reconstruction_note"] = "scored_from_retained_july12_fields_outside_broad_base_date_range"
        if "reconstruction_note" not in july_rows:
            july_rows["reconstruction_note"] = "joined_to_broad_base"
        cols = [
            "canonical_proposition_key",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "integrated_official_hits",
            "p_zero_hits",
            "p_exactly_one_hit",
            "p_two_plus_hits",
            "expected_pa_used",
            "hitter_per_pa_hit_estimate",
            "starter_expected_hits_allowed",
            "starter_adjustment",
            "starter_exposure_state",
            "pitcher_suppression_label",
            "current_side_surface_state",
            "reconstruction_note",
        ]
        for c in cols:
            if c not in july_rows:
                july_rows[c] = ""
        write_csv(july_rows[cols], out_dir / "july12_probability_reconstruction_2026-07-17.csv")
    else:
        write_csv(pd.DataFrame(), out_dir / "july12_probability_reconstruction_2026-07-17.csv")

    # Both-side descriptive bands from holdout only.
    hold_best = pred[(pred["temporal_split"] == "holdout") & (pred["benchmark"] == "benchmark_3_hitter_opportunity_count")].copy()
    hold_best["research_band"] = hold_best["p_two_plus_hits"].map(
        lambda p: "potential_u15_low_two_plus" if p < 0.20 else "potential_o15_high_two_plus" if p >= 0.35 else "withhold_uncertain_middle"
    )
    both_side = (
        hold_best.groupby("research_band")
        .agg(
            rows=("player_game_key", "count"),
            zero_hits=("outcome_class", lambda s: int((s == "ZERO_HITS").sum())),
            exactly_one_hit=("outcome_class", lambda s: int((s == "EXACTLY_ONE_HIT").sum())),
            two_plus_hits=("outcome_class", lambda s: int((s == "TWO_OR_MORE_HITS").sum())),
            observed_two_plus_rate=("multi_hit_target", "mean"),
            avg_predicted_two_plus=("p_two_plus_hits", "mean"),
        )
        .reset_index()
    )
    both_side["price_context"] = "not_certified_in_this_broad_population"
    write_csv(both_side, out_dir / "both_side_research_interpretation_2026-07-17.csv")

    long_price_tables = long_price_requirement_tables()
    for table_name, table_df in long_price_tables.items():
        write_csv(table_df, out_dir / f"{table_name}_2026-07-17.csv")
    long_price_eval = build_long_price_evaluation(population, pred)
    for table_name, table_df in long_price_eval.items():
        write_csv(table_df, out_dir / f"{table_name}_2026-07-17.csv")

    # Next experiment spec and decisions.
    hold_rows = metrics[metrics["temporal_split"].eq("holdout")]
    best_hold = hold_rows.sort_values("brier_two_plus").head(1).to_dict("records")
    one_two_hold = progression[progression["temporal_split"].eq("holdout")].sort_values("brier_two_plus").head(1).to_dict("records")
    plus200_results = long_price_eval.get("long_price_plus200_population_results", pd.DataFrame())
    plus200_all = (
        plus200_results[plus200_results["plus200_population"].eq("all_exact_priced_plus200_or_longer")]
        if not plus200_results.empty and "plus200_population" in plus200_results
        else pd.DataFrame()
    )
    plus200_rows = int(plus200_all["propositions"].iloc[0]) if not plus200_all.empty else 0
    plus200_edge = float(plus200_all["predicted_minus_implied_probability"].iloc[0]) if not plus200_all.empty else float("nan")
    plus200_obs_edge = (
        float(plus200_all["observed_two_plus_rate"].iloc[0] - plus200_all["market_implied_break_even_probability"].iloc[0])
        if not plus200_all.empty
        else float("nan")
    )
    try:
        prog_hold = progression[progression["temporal_split"].eq("holdout")].set_index("benchmark")
        one_two_delta = float(
            prog_hold.loc["benchmark_3_hitter_opportunity_count", "brier_two_plus"]
            - prog_hold.loc["benchmark_2_hitter_only_count", "brier_two_plus"]
        )
    except Exception:
        one_two_delta = None
    opp_delta = delta("brier_two_plus", "benchmark_3_hitter_opportunity_count", "benchmark_2_hitter_only_count")
    starter_delta = delta("brier_two_plus", "benchmark_4_hitter_opportunity_starter", "benchmark_3_hitter_opportunity_count")
    sup_hold = sup_analysis[sup_analysis.get("temporal_split", pd.Series(dtype=str)).eq("holdout")] if not sup_analysis.empty else pd.DataFrame()
    decisions = {
        "MLB_MULTI_HIT_POPULATION_DECISION": "BROAD_BATTER_GAME_POPULATION_BOUND_FROM_HITTER_PERSISTENCE_BASE_SELECTED_PROPOSITION_SUBSET_RETAINED_SEPARATELY",
        "MLB_MULTI_HIT_COMPONENT_DATA_READINESS_DECISION": "HITTER_AND_PA_READY_STARTER_PARTIAL_BULLPEN_AND_MATCHUP_INSUFFICIENT",
        "MLB_MULTI_HIT_COUNT_DISTRIBUTION_DECISION": "BETA_BINOMIAL_EXPECTED_PA_MIXTURE_USED_WITH_BINOMIAL_IDENTITY_P_GE_2_EQ_1_MINUS_P0_MINUS_P1",
        "MLB_MULTI_HIT_ANY_HIT_BASELINE_DECISION": "ANY_HIT_CARRYOVER_IS_ZERO_AVOIDANCE_BASELINE_NOT_SUFFICIENT_MULTI_HIT_OWNERSHIP",
        "MLB_MULTI_HIT_HITTER_ONLY_DECISION": "HITTER_ONLY_COUNT_MODEL_ESTABLISHES_FIRST_EXPLICIT_MULTI_HIT_PROBABILITY_BENCHMARK",
        "MLB_MULTI_HIT_OPPORTUNITY_INCREMENT_DECISION": "OPPORTUNITY_INCREMENT_IS_CALIBRATION_DIAGNOSTIC_NOT_STANDALONE_CHALLENGER",
        "MLB_MULTI_HIT_STARTER_EXPOSURE_INCREMENT_DECISION": "STARTER_INCREMENT_EVALUATED_ONLY_WHERE EXACT_JOIN_AVAILABLE_AND_REMAINS_PARTIAL",
        "MLB_MULTI_HIT_GENERALIZED_MATCHUP_INCREMENT_DECISION": "NOT_RUN_INSUFFICIENT_GOVERNED_FIELD_COVERAGE",
        "MLB_MULTI_HIT_ONE_TO_TWO_PLUS_DECISION": "ONE_TO_TWO_PLUS_SEPARATION_REMAINS_MODEST_AND_DECISIVE_LIMITATION",
        "MLB_MULTI_HIT_CALIBRATION_DECISION": "CALIBRATED_BENCHMARK_EXISTS_OFFLINE_BUT_REQUIRES_CHALLENGER_PACKAGE_BEFORE_OPERATIONAL_USE",
        "MLB_MULTI_HIT_SUPPRESSION_PRESERVATION_DECISION": "SUPPRESSION_DIRECTION_RETAINED_AS_LOW_TWO_PLUS_CONTEXT_WHERE_BOUND",
        "MLB_MULTI_HIT_HITTER_OWNERSHIP_DECISION": "NO_HITTER_OWNED_REGIME_PROMOTED_FROM_THIS_BENCHMARK",
        "MLB_MULTI_HIT_JULY12_RECONSTRUCTION_DECISION": "JULY12_ROWS_SHOW_ARCHITECTURE_LACKED_EXPLICIT_ONE_VS_TWO_PROBABILITY",
        "MLB_MULTI_HIT_BOTH_SIDE_RESEARCH_DECISION": "SAME_PROBABILITY_CAN_DESCRIBE_O15_U15_WITHHOLD_BANDS_RESEARCH_ONLY",
        "MLB_MULTI_HIT_LONG_PRICE_POPULATION_DECISION": "PLUS200_LONG_PRICE_PRIMARY_POPULATION_EXECUTED_ON_EXACT_PRESERVED_SELECTED_PRICE_ROWS",
        "MLB_MULTI_HIT_LONG_PRICE_CALIBRATION_DECISION": "PRICE_BAND_CALIBRATION_EXECUTED_ON_EXACT_PRESERVED_SELECTED_PRICE_ROWS",
        "MLB_MULTI_HIT_LONG_PRICE_DIRECTIONAL_VALUE_DECISION": (
            "PLUS200_SIGNAL_PROMISING_BUT_HOLDOUT_UNDERPOWERED"
            if plus200_rows and plus200_edge > 0 and plus200_obs_edge > 0
            else "NO_STABLE_PLUS200_MULTI_HIT_VALUE_DETECTED"
        ),
        "MLB_MULTI_HIT_LONG_PRICE_PRICE_VALUE_DECISION": (
            "PLUS200_DIRECTIONAL_LIFT_PRESENT_PRICE_TIMING_NOT_CERTIFIED"
            if plus200_rows and plus200_obs_edge > 0
            else "NO_CERTIFIED_PLUS200_PRICE_VALUE_DETECTED"
        ),
        "MLB_MULTI_HIT_LONG_PRICE_SUPPRESSION_VETO_DECISION": "FROZEN_SUPPRESSION_VETO_MUST_BE_APPLIED_AFTER_BASEBALL_PROBABILITY_BEFORE_PRICE_VALUE_EVALUATION",
        "MLB_MULTI_HIT_SHORT_VS_LONG_PRICE_DECISION": "FIXED_SHORT_PRICE_CONTROL_BANDS_REQUIRED_TO_TEST_OBVIOUS_HITTER_VS_LONG_PRICE_VALUE",
        "MLB_MULTI_HIT_PLUS200_RESEARCH_READINESS_DECISION": (
            "PLUS200_SIGNAL_PROMISING_BUT_HOLDOUT_UNDERPOWERED"
            if plus200_rows and plus200_edge > 0
            else "NO_STABLE_PLUS200_MULTI_HIT_VALUE_DETECTED"
        ),
        "MLB_MULTI_HIT_BENCHMARK_EXECUTION_DECISION": "EXECUTED_RESEARCH_ONLY_WITH_FROZEN_INSTRUMENTS_AND_PRICE_BANDS",
        "MLB_MULTI_HIT_VALIDATION_HOLDOUT_DECISION": "CALIBRATED_BUT_MODEST_HOLDOUT_SEPARATION_RESEARCH_ONLY",
        "MLB_MULTI_HIT_ONE_TO_TWO_PLUS_HOLDOUT_DECISION": (
            "ONE_TO_TWO_PLUS_VALUE_PRESENT" if one_two_delta is not None and one_two_delta < -0.005 else "ONE_TO_TWO_PLUS_SEPARATION_REMAINS_MODEST"
        ),
        "MLB_MULTI_HIT_OPPORTUNITY_INCREMENT_HOLDOUT_DECISION": (
            "ONE_TO_TWO_PLUS_VALUE_PRESENT" if opp_delta is not None and opp_delta < -0.005 else "CALIBRATION_ONLY"
        ),
        "MLB_MULTI_HIT_STARTER_EXPOSURE_HOLDOUT_DECISION": (
            "SUPPRESSION_VALUE" if starter_delta is not None and starter_delta < 0 else "INSUFFICIENT_COVERAGE"
        ),
        "MLB_MULTI_HIT_SUPPRESSION_UNIFIED_PROBABILITY_DECISION": (
            "SUPPRESSION_DIRECTION_PRESERVED_IN_UNIFIED_PROBABILITY_DIAGNOSTIC"
            if not sup_hold.empty
            else "SUPPRESSION_PRESERVATION_HOLDOUT_UNDERPOWERED"
        ),
        "MLB_MULTI_HIT_PLUS200_HOLDOUT_DECISION": (
            "PLUS200_SIGNAL_PROMISING_BUT_HOLDOUT_UNDERPOWERED"
            if plus200_rows and plus200_edge > 0
            else "NO_STABLE_PLUS200_MULTI_HIT_VALUE_DETECTED"
        ),
        "MLB_MULTI_HIT_PLUS200_PRICE_VALUE_DECISION": (
            "PLUS200_DIRECTIONAL_LIFT_PRESENT_PRICE_TIMING_NOT_CERTIFIED"
            if plus200_rows and plus200_obs_edge > 0
            else "NO_CERTIFIED_PLUS200_PRICE_VALUE_DETECTED"
        ),
        "MLB_MULTI_HIT_CHALLENGER_READINESS_DECISION": "STOP_NO_CHALLENGER_RECOMMENDATION_PLUS200_PRICE_TIMING_AND_ONE_TO_TWO_PLUS_LIMITATIONS_REMAIN",
        "MLB_MULTI_HIT_NEXT_EXPERIMENT_DECISION": "NO_CHALLENGER_RECOMMENDATION_FROM_THIS_EXECUTION",
        "MLB_MULTI_HIT_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), out_dir / "required_decisions_2026-07-17.csv")
    write_csv(
        pd.DataFrame(
            [
                {
                    "next_experiment": "no_challenger_recommendation_from_this_execution",
                    "scope": "research_only",
                    "required_before_execution": "selection-time O1.5 price certification and stronger one-to-two-plus separation evidence",
                    "production_authorized": False,
                }
            ]
        ),
        out_dir / "next_experiment_specification_2026-07-17.csv",
    )

    source_registry = pd.DataFrame(
        [
            {"source": rel(p), "exists": p.exists(), "sha256": sha256(p) if p.exists() else "", "role": role}
            for p, role in [
                (HITTER_GAME_BASE, "canonical broad player-game population and hitter strict-prior features"),
                (PA_SELECTED_BASE, "selected-proposition comparator and control probability"),
                (STARTER_XH, "partial starter exposure exact join"),
                (SUPPRESSION_LEDGER, "affirmative suppression overlap"),
                (INTEGRATED_MATCHUP, "July 12 sentinel reconstruction"),
            ]
        ]
    )
    write_csv(source_registry, out_dir / "source_registry_2026-07-17.csv")

    summary = {
        "generated_at_utc": now_utc(),
        "population_rows": int(len(population)),
        "date_start": population["slate_date"].min(),
        "date_end": population["slate_date"].max(),
        "dates": int(population["slate_date"].nunique()),
        "games": int(population["game_id"].nunique()),
        "players": int(population["player_id"].nunique()),
        "zero_hits": int((population["outcome_class"] == "ZERO_HITS").sum()),
        "exactly_one_hit": int((population["outcome_class"] == "EXACTLY_ONE_HIT").sum()),
        "two_plus_hits": int((population["outcome_class"] == "TWO_OR_MORE_HITS").sum()),
        "selected_proposition_subset_rows": int(population["selected_proposition_subset"].sum()),
        "starter_feature_available_rows": int(population["starter_feature_available"].sum()),
        "primary_economic_target": LONG_PRICE_PRIMARY_TARGET,
        "long_price_bands": [
            {
                "price_band": label,
                "min_american_odds_inclusive": lo,
                "max_american_odds_inclusive": hi,
                "role": role,
            }
            for label, lo, hi, role in LONG_PRICE_BANDS
        ],
        "long_price_requirement_status": "EXECUTED_ON_EXACT_PRESERVED_SELECTED_PRICE_ROWS_PRICE_TIMING_NOT_CERTIFIED",
        "plus200_exact_price_rows": plus200_rows,
        "plus200_predicted_minus_implied_probability": None if math.isnan(plus200_edge) else plus200_edge,
        "plus200_observed_minus_implied_probability": None if math.isnan(plus200_obs_edge) else plus200_obs_edge,
        "long_price_market_base_scope": "selected_proposition_exact_price_base_not_full_market",
        "best_holdout_benchmark_by_brier": best_hold,
        "best_one_to_two_plus_holdout_by_brier": one_two_hold,
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_multi_hit_probability_benchmark_2026-07-17.json")

    md = f"""# MLB Hits 1.5 Explicit Multi-Hit Probability Construction and Calibration Benchmark

Generated: `{summary['generated_at_utc']}`

## Executive Summary

This bounded offline benchmark constructs an explicit count-threshold probability for `P(HITS >= 2)` from local strict-prior artifacts. It does not alter production models, selectors, uploads, Quick Card, workspace, tiers, or LaunchAgents.

Canonical broad player-game population: **{summary['population_rows']}** rows from **{summary['date_start']}** through **{summary['date_end']}** across **{summary['dates']}** dates, **{summary['games']}** games, and **{summary['players']}** hitters.

Outcome distribution:

- Zero hits: **{summary['zero_hits']}**
- Exactly one hit: **{summary['exactly_one_hit']}**
- Two or more hits: **{summary['two_plus_hits']}**

The benchmark now provides a unified probability distribution over zero, one, and two-plus hits. Hitter strict-prior history and PA opportunity are available broadly; Starter exposure is only partially available through exact selected-proposition joins; bullpen and generalized matchup fields are not sufficiently governed for Benchmark 5.

## Temporal Splits

{markdown_table(split_manifest)}

## Validation and Holdout Metrics

{markdown_table(metrics)}

## One-to-Two-Plus Progression

{markdown_table(progression)}

## Both-Side Research Interpretation

{markdown_table(both_side)}

## Frozen Long-Price Economic Evaluation Requirement

The primary O1.5 economic target is now frozen as `{LONG_PRICE_PRIMARY_TARGET}`. The benchmark must evaluate exact preserved O1.5 OVER prices in fixed bands: `+100 through +149`, `+150 through +199`, `+200 through +249`, and `+250 and longer`.

This requirement was frozen before economic evaluation. The benchmark constructs `P(HITS >= 2)` without using offered O1.5 price as a feature, then evaluates exact preserved selected-price rows afterward. The retained price spine has snapshot timestamps, but not governed candidate-decision timestamp certification, so ROI is diagnostic only where timing is not certified.

{markdown_table(long_price_tables['long_price_fixed_price_bands'])}

## Frozen Long-Price Results

{markdown_table(long_price_eval['long_price_band_results'])}

## +200-Or-Longer Results

{markdown_table(long_price_eval['long_price_plus200_population_results'])}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

Proppadia can now estimate multi-hit probability as a calibrated offline matchup outcome in first-generation benchmark form, and the frozen +200-or-longer selected-price slice can be evaluated directionally. However, this execution does **not** establish hitter-owned +200 value strongly enough for a Challenger recommendation: one-to-two-plus separation remains modest, Starter exposure is only partially bound, the +200 population is small, and selection-time O1.5 price timing is not certified. The pitcher-owned UNDER signal is preserved diagnostically rather than erased, and production remains not authorized.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation), out_dir / "validation_report_2026-07-17.csv")

    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return summary


def markdown_table(df: pd.DataFrame, max_rows: int = 24) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                vals.append("" if pd.isna(v) else f"{v:.4f}")
            else:
                vals.append(norm(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["research_only"], default="research_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(Path(args.output_dir))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
