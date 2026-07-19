#!/usr/bin/env python3
"""Bounded MLB empirical contact-quality conversion pilot.

This offline research utility builds a fixed empirical surface for
P(official hit | hit-capable contact) from local MLB feed hitData, then uses
strict-prior hitter and pitcher contact-quality profiles to estimate multi-hit
probabilities. The empirical surface is research-only and is not official
Statcast xBA.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, LaunchAgent changes, threshold search, price optimization,
hyperparameter search, or holdout tuning are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_empirical_contact_quality_conversion_pilot/2026-07-17"

CONTACT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"
CONTACT_LEDGER = CONTACT_ROOT / "canonical_contact_outcome_ledger_2026-07-17.csv"
CONTACT_POP = CONTACT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717
FIT_END = pd.Timestamp("2026-06-11")
K_SURFACE = 40
K_PROFILE = 35


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def support_class(n: int) -> str:
    if n >= 80:
        return "HIGH_PERSONAL_SUPPORT"
    if n >= 40:
        return "MODERATE_PERSONAL_SUPPORT"
    if n >= 10:
        return "LOW_PERSONAL_SUPPORT"
    if n > 0:
        return "PRIOR_DOMINATED"
    return "MISSING"


def shrink(raw: float, n: int, prior: float, k: int = K_PROFILE) -> tuple[float, float]:
    if not math.isfinite(raw):
        raw = prior
    weight = n / (n + k) if n + k else 0.0
    return float(raw * weight + prior * (1 - weight)), float(weight)


def poisson_probs(lam: float) -> tuple[float, float, float]:
    lam = max(float(lam), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = max(0.0, 1.0 - p0 - p1)
    s = p0 + p1 + p2
    return p0 / s, p1 / s, p2 / s


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def surface_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    speed = pd.to_numeric(out["launch_speed"], errors="coerce")
    angle = pd.to_numeric(out["launch_angle"], errors="coerce")
    x = pd.to_numeric(out["hit_coordinates_x"], errors="coerce")
    y = pd.to_numeric(out["hit_coordinates_y"], errors="coerce")
    out["speed_band"] = pd.cut(speed, [-np.inf, 70, 80, 90, 100, np.inf], labels=["lt70", "70_80", "80_90", "90_100", "100plus"]).astype(str).replace("nan", "missing")
    out["angle_band"] = pd.cut(angle, [-np.inf, 0, 10, 25, 50, np.inf], labels=["lt0", "0_10", "10_25", "25_50", "50plus"]).astype(str).replace("nan", "missing")
    out["coord_x_band"] = pd.cut(x, [-np.inf, 80, 160, np.inf], labels=["x_left", "x_mid", "x_right"]).astype(str).replace("nan", "x_missing")
    out["coord_y_band"] = pd.cut(y, [-np.inf, 80, 160, np.inf], labels=["y_shallow", "y_mid", "y_deep"]).astype(str).replace("nan", "y_missing")
    out["trajectory_band"] = out["batted_ball_type"].fillna("missing").astype(str)
    out["sweet_spot"] = ((angle >= 8) & (angle <= 32)).astype(int)
    out["hard_hit_derived"] = (speed >= 95).astype(int)
    return out


SURFACE_KEYS = ["speed_band", "angle_band", "trajectory_band", "coord_x_band", "coord_y_band"]
FALLBACK_1 = ["speed_band", "angle_band", "trajectory_band"]
FALLBACK_2 = ["speed_band", "angle_band"]


def build_surface(contact: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    fit = contact[(contact["game_date_dt"] <= FIT_END) & contact["hit_capable_contact"].eq(1)].copy()
    prior = float(fit["official_hit"].mean())
    rows = []
    for keys, level in [(SURFACE_KEYS, "full"), (FALLBACK_1, "speed_angle_trajectory"), (FALLBACK_2, "speed_angle")]:
        grouped = fit.groupby(keys).agg(contact_events=("official_hit", "count"), official_hits=("official_hit", "sum")).reset_index()
        grouped["surface_level"] = level
        grouped["empirical_xhit_contact_v1"] = (grouped["official_hits"] + prior * K_SURFACE) / (grouped["contact_events"] + K_SURFACE)
        rows.append(grouped)
    surface = pd.concat(rows, ignore_index=True, sort=False)
    spec = {
        "surface_name": "empirical_xhit_contact_v1",
        "model_family": "fixed smoothed empirical cell surface",
        "fit_period": "2026-05-01 through 2026-06-11",
        "target": "official_hit among hit-capable contacts",
        "features": SURFACE_KEYS,
        "fallback_levels": ["speed_angle_trajectory", "speed_angle", "global_prior"],
        "smoothing": f"(hits + global_prior * {K_SURFACE}) / (contacts + {K_SURFACE})",
        "global_fit_hit_rate": prior,
        "fit_contact_events": int(len(fit)),
        "fit_official_hits": int(fit["official_hit"].sum()),
        "not_official_statcast_xba": True,
    }
    return surface, spec


def apply_surface(contact: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    out = contact.copy()
    out["empirical_xhit_contact_v1"] = np.nan
    out["surface_support"] = 0
    out["surface_level"] = ""
    for keys, level in [(SURFACE_KEYS, "full"), (FALLBACK_1, "speed_angle_trajectory"), (FALLBACK_2, "speed_angle")]:
        surf = surface[surface["surface_level"].eq(level)][keys + ["contact_events", "empirical_xhit_contact_v1"]].copy()
        merged = out[keys].merge(surf, on=keys, how="left")
        mask = out["empirical_xhit_contact_v1"].isna() & merged["empirical_xhit_contact_v1"].notna()
        out.loc[mask, "empirical_xhit_contact_v1"] = merged.loc[mask, "empirical_xhit_contact_v1"].to_numpy()
        out.loc[mask, "surface_support"] = merged.loc[mask, "contact_events"].to_numpy()
        out.loc[mask, "surface_level"] = level
    out["empirical_xhit_contact_v1"] = out["empirical_xhit_contact_v1"].fillna(float(spec["global_fit_hit_rate"]))
    out["surface_level"] = out["surface_level"].replace("", "global_prior")
    return out


def binary_metric(y: pd.Series, p: pd.Series, split: str, instrument: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    yy = y.astype(int).to_numpy()
    pp = np.clip(pd.to_numeric(p, errors="coerce").fillna(float(pd.to_numeric(p, errors="coerce").mean())).to_numpy(), EPS, 1 - EPS)
    out = {
        "temporal_split": split,
        "instrument": instrument,
        "rows": int(len(yy)),
        "positives": int(yy.sum()),
        "observed_rate": float(yy.mean()) if len(yy) else "",
        "avg_predicted": float(pp.mean()) if len(pp) else "",
        "brier": float(np.mean((pp - yy) ** 2)) if len(yy) else "",
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])) if len(yy) else "",
        "auc": float(roc_auc_score(yy, pp)) if len(set(yy)) > 1 else "",
        "ece": expected_calibration_error(yy, pp) if len(yy) else "",
    }
    try:
        x = np.log(pp / (1 - pp))
        slope, intercept = np.polyfit(x, yy, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        out["calibration_slope"] = ""
        out["calibration_intercept"] = ""
    if extra:
        out.update(extra)
    return out


def surface_validation(contact: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    bands = []
    for split, date_filter in [
        ("fit", contact["game_date_dt"] <= FIT_END),
        ("validation", (contact["game_date_dt"] >= pd.Timestamp("2026-06-12")) & (contact["game_date_dt"] <= pd.Timestamp("2026-06-25"))),
        ("holdout", (contact["game_date_dt"] >= pd.Timestamp("2026-06-26")) & (contact["game_date_dt"] <= pd.Timestamp("2026-07-09"))),
    ]:
        g = contact[date_filter & contact["hit_capable_contact"].eq(1)].copy()
        rows.append(binary_metric(g["official_hit"], g["empirical_xhit_contact_v1"], split, "empirical_xhit_contact_v1", {"contact_events": len(g)}))
        if len(g):
            g["frozen_xhit_band"] = pd.cut(g["empirical_xhit_contact_v1"], bins=[0, .15, .25, .35, .45, 1], labels=["0_15", "15_25", "25_35", "35_45", "45plus"], include_lowest=True)
            for band, b in g.groupby("frozen_xhit_band", observed=True):
                bands.append({"temporal_split": split, "band": str(band), "rows": len(b), "observed_hit_rate": float(b["official_hit"].mean()), "avg_predicted_xhit": float(b["empirical_xhit_contact_v1"].mean())})
            for dim in ["speed_band", "angle_band", "trajectory_band", "starter_reliever_role", "batter_hand", "pitcher_hand", "surface_level"]:
                for bucket, b in g.groupby(dim, observed=True):
                    bands.append({"temporal_split": split, "band": f"{dim}={bucket}", "rows": len(b), "observed_hit_rate": float(b["official_hit"].mean()), "avg_predicted_xhit": float(b["empirical_xhit_contact_v1"].mean())})
    return pd.DataFrame(rows), pd.DataFrame(bands)


def build_profiles(pop: pd.DataFrame, contact: pd.DataFrame, global_prior: float) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    pop["batter_key"] = pd.to_numeric(pop["player_id"], errors="coerce").astype("Int64").astype(str)
    pop["starter_key"] = pd.to_numeric(pop["opposing_starter_id"], errors="coerce").astype("Int64").astype(str)
    contact = contact[contact["hit_capable_contact"].eq(1)].copy()
    contact["batter_key"] = pd.to_numeric(contact["batter_id"], errors="coerce").astype("Int64").astype(str)
    contact["pitcher_key"] = pd.to_numeric(contact["pitcher_id"], errors="coerce").astype("Int64").astype(str)
    rows = []
    hitter_rows = []
    starter_rows = []
    bullpen_rows = []
    for date, day in pop.groupby("slate_date_dt", dropna=False):
        prior = contact[contact["game_date_dt"] < pd.Timestamp(date)] if pd.notna(date) else contact.iloc[0:0]
        hitter = prior.groupby("batter_key").agg(
            support=("empirical_xhit_contact_v1", "count"),
            mean_xhit=("empirical_xhit_contact_v1", "mean"),
            q25_xhit=("empirical_xhit_contact_v1", lambda s: s.quantile(.25)),
            q50_xhit=("empirical_xhit_contact_v1", "median"),
            q75_xhit=("empirical_xhit_contact_v1", lambda s: s.quantile(.75)),
            hard_hit_rate=("hard_hit_derived", "mean"),
            sweet_spot_rate=("sweet_spot", "mean"),
            launch_speed_mean=("launch_speed", lambda s: pd.to_numeric(s, errors="coerce").mean()),
            launch_angle_mean=("launch_angle", lambda s: pd.to_numeric(s, errors="coerce").mean()),
        ).to_dict("index")
        pitcher = prior.groupby("pitcher_key").agg(
            support=("empirical_xhit_contact_v1", "count"),
            mean_xhit_allowed=("empirical_xhit_contact_v1", "mean"),
            hard_hit_allowed=("hard_hit_derived", "mean"),
            sweet_spot_allowed=("sweet_spot", "mean"),
        ).to_dict("index")
        reliever = prior[prior["starter_reliever_role"].eq("RELIEVER_FACING_PA")]
        rel_n = int(len(reliever))
        rel_mean, rel_weight = shrink(float(reliever["empirical_xhit_contact_v1"].mean()) if rel_n else np.nan, rel_n, global_prior)
        rel_hard, _ = shrink(float(reliever["hard_hit_derived"].mean()) if rel_n else np.nan, rel_n, float(prior["hard_hit_derived"].mean()) if len(prior) else 0.35)
        rel_sweet, _ = shrink(float(reliever["sweet_spot"].mean()) if rel_n else np.nan, rel_n, float(prior["sweet_spot"].mean()) if len(prior) else 0.35)
        for _, r in day.iterrows():
            hk = r["batter_key"]
            pk = r["starter_key"]
            hp = hitter.get(hk, {})
            pp = pitcher.get(pk, {})
            h_n = int(hp.get("support", 0) or 0)
            p_n = int(pp.get("support", 0) or 0)
            h_xhit, h_w = shrink(float(hp.get("mean_xhit", np.nan)), h_n, global_prior)
            p_xhit, p_w = shrink(float(pp.get("mean_xhit_allowed", np.nan)), p_n, global_prior)
            hitter_plus_starter = (h_xhit * (h_w + 0.001) + p_xhit * (p_w + 0.001)) / (h_w + p_w + 0.002)
            source_bullpen = (h_xhit * (h_w + 0.001) + rel_mean * (rel_weight + 0.001)) / (h_w + rel_weight + 0.002)
            rows.append({
                "player_game_key": r["player_game_key"],
                "hitter_xhit_support": h_n,
                "starter_xhit_support": p_n,
                "bullpen_xhit_support": rel_n,
                "hitter_xhit_support_class": support_class(h_n),
                "starter_xhit_support_class": support_class(p_n),
                "bullpen_xhit_support_class": support_class(rel_n),
                "hitter_empirical_xhit_per_contact": h_xhit,
                "starter_empirical_xhit_allowed_per_contact": p_xhit,
                "bullpen_empirical_xhit_allowed_per_contact": rel_mean,
                "hitter_personal_weight": h_w,
                "starter_personal_weight": p_w,
                "hitter_plus_starter_conversion": hitter_plus_starter,
                "source_aware_starter_conversion": hitter_plus_starter,
                "source_aware_bullpen_conversion": source_bullpen,
                "hitter_hard_hit_rate": shrink(float(hp.get("hard_hit_rate", np.nan)), h_n, 0.35)[0],
                "hitter_sweet_spot_rate": shrink(float(hp.get("sweet_spot_rate", np.nan)), h_n, 0.35)[0],
                "starter_hard_hit_allowed": shrink(float(pp.get("hard_hit_allowed", np.nan)), p_n, 0.35)[0],
                "starter_sweet_spot_allowed": shrink(float(pp.get("sweet_spot_allowed", np.nan)), p_n, 0.35)[0],
            })
            hitter_rows.append({"player_game_key": r["player_game_key"], "player_id": r["player_id"], "player_name": r["player_name"], "support": h_n, "support_class": support_class(h_n), "mean_empirical_xhit": h_xhit, "q25_xhit": hp.get("q25_xhit", ""), "q50_xhit": hp.get("q50_xhit", ""), "q75_xhit": hp.get("q75_xhit", ""), "hard_hit_rate": shrink(float(hp.get("hard_hit_rate", np.nan)), h_n, 0.35)[0], "sweet_spot_rate": shrink(float(hp.get("sweet_spot_rate", np.nan)), h_n, 0.35)[0], "personal_weight": h_w})
            starter_rows.append({"player_game_key": r["player_game_key"], "pitcher_id": r.get("opposing_starter_id"), "pitcher_name": r.get("opposing_starter_name"), "support": p_n, "support_class": support_class(p_n), "mean_empirical_xhit_allowed": p_xhit, "hard_hit_allowed": shrink(float(pp.get("hard_hit_allowed", np.nan)), p_n, 0.35)[0], "sweet_spot_allowed": shrink(float(pp.get("sweet_spot_allowed", np.nan)), p_n, 0.35)[0], "personal_weight": p_w})
            bullpen_rows.append({"player_game_key": r["player_game_key"], "support": rel_n, "support_class": support_class(rel_n), "mean_empirical_xhit_allowed": rel_mean, "hard_hit_allowed": rel_hard, "sweet_spot_allowed": rel_sweet, "personal_weight": rel_weight})
    return pop.merge(pd.DataFrame(rows), on="player_game_key", how="left"), pd.DataFrame(hitter_rows), pd.DataFrame(starter_rows), pd.DataFrame(bullpen_rows)


def apply_game_instruments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    total_hcc = pd.to_numeric(out["pred_hit_capable_contact_count_c"], errors="coerce").fillna(2.5)
    starter_pa = pd.to_numeric(out["turnover_starter_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_starter_pa"], errors="coerce")).fillna(2.4)
    bullpen_pa = pd.to_numeric(out["turnover_bullpen_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_bullpen_pa"], errors="coerce")).fillna(1.6)
    starter_hcc_rate = pd.to_numeric(out["contact_pred_hit_capable_contact_rate"], errors="coerce").fillna(0.65)
    bullpen_hcc_rate = pd.to_numeric(out["contact_pred_bullpen_hit_capable_contact_rate"], errors="coerce").fillna(0.65)
    starter_contacts = starter_pa * starter_hcc_rate
    bullpen_contacts = bullpen_pa * bullpen_hcc_rate
    conversions = {
        "hitter_conversion_profile": total_hcc * out["hitter_empirical_xhit_per_contact"],
        "hitter_plus_starter_conversion": total_hcc * out["hitter_plus_starter_conversion"],
        "source_aware_conversion": starter_contacts * out["source_aware_starter_conversion"] + bullpen_contacts * out["source_aware_bullpen_conversion"],
    }
    for name, lam in conversions.items():
        vals = [poisson_probs(v) for v in lam]
        out[f"{name}_p_zero_hits"] = [v[0] for v in vals]
        out[f"{name}_p_exactly_one_hit"] = [v[1] for v in vals]
        out[f"{name}_p_two_plus_hits"] = [v[2] for v in vals]
    # Oracle ladder.
    actual_count = pd.to_numeric(out["hit_capable_contact_count"], errors="coerce").fillna(0)
    prior_conv = pd.to_numeric(out["source_aware_conversion_p_two_plus_hits"], errors="coerce")
    out["oracle_a_actual_count_predicted_conversion_p_two_plus_hits"] = [poisson_probs(c * p)[2] for c, p in zip(actual_count, out["hitter_plus_starter_conversion"])]
    actual_hard_rate = pd.to_numeric(out["actual_hard_hit_count"], errors="coerce").fillna(0) / actual_count.replace(0, np.nan)
    actual_quality_conv = (out["hitter_plus_starter_conversion"] * (1 + 0.35 * (actual_hard_rate.fillna(out["hitter_hard_hit_rate"]) - out["hitter_hard_hit_rate"]))).clip(0.05, 0.75)
    out["oracle_b_predicted_count_actual_quality_p_two_plus_hits"] = [poisson_probs(c * p)[2] for c, p in zip(total_hcc, actual_quality_conv)]
    out["oracle_c_predicted_count_predicted_quality_p_two_plus_hits"] = out["source_aware_conversion_p_two_plus_hits"]
    out["oracle_d_actual_count_actual_quality_p_two_plus_hits"] = [poisson_probs(c * p)[2] for c, p in zip(actual_count, actual_quality_conv)]
    return out


def game_metric(df: pd.DataFrame, prob_col: str, instrument: str, split: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    y = g["two_plus_binary"].astype(int)
    p = g[prob_col]
    out = binary_metric(y, p, split, instrument)
    out.update({
        "wins_two_plus": int(y.sum()),
        "losses_exactly_one": int(len(y) - y.sum()),
        "avg_predicted_two_plus": out.pop("avg_predicted"),
        "observed_two_plus_rate": out.pop("observed_rate"),
    })
    return out


def build_game_metrics(df: pd.DataFrame) -> pd.DataFrame:
    instruments = {
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "predicted_contact_count_model": "source_aware_contact_challenger_p_two_plus_hits",
        "hitter_conversion_profile": "hitter_conversion_profile_p_two_plus_hits",
        "hitter_plus_starter_conversion": "hitter_plus_starter_conversion_p_two_plus_hits",
        "source_aware_conversion": "source_aware_conversion_p_two_plus_hits",
        "unified_contact_quantity_x_conversion": "source_aware_conversion_p_two_plus_hits",
        "oracle_a_actual_count_predicted_conversion": "oracle_a_actual_count_predicted_conversion_p_two_plus_hits",
        "oracle_b_predicted_count_actual_quality": "oracle_b_predicted_count_actual_quality_p_two_plus_hits",
        "oracle_c_predicted_count_predicted_quality": "oracle_c_predicted_count_predicted_quality_p_two_plus_hits",
        "oracle_d_actual_count_actual_quality": "oracle_d_actual_count_actual_quality_p_two_plus_hits",
    }
    rows = []
    for split in ["validation", "holdout"]:
        for name, col in instruments.items():
            rows.append(game_metric(df, col, name, split))
    return pd.DataFrame(rows)


def probability_bands(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for inst, col in {
        "source_aware_conversion": "source_aware_conversion_p_two_plus_hits",
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
    }.items():
        fit = df[(df["temporal_split"].eq("fit")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)][col].dropna()
        if fit.empty:
            continue
        edges = sorted(set([float("-inf"), *np.quantile(fit, [.25, .5, .75]).tolist(), float("inf")]))
        labels = [f"fit_q{i+1}" for i in range(len(edges) - 1)]
        for split in ["validation", "holdout"]:
            g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
            g["band"] = pd.cut(g[col], edges, labels=labels, include_lowest=True)
            for band, b in g.groupby("band", observed=True):
                rows.append({"temporal_split": split, "instrument": inst, "frozen_probability_band": str(band), "rows": len(b), "observed_two_plus_rate": float(b["two_plus_binary"].mean()), "avg_predicted_two_plus": float(b[col].mean())})
    return pd.DataFrame(rows)


def bootstrap(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    rows = []
    for name, col in {"frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits", "source_aware_conversion": "source_aware_conversion_p_two_plus_hits"}.items():
        briers, aucs = [], []
        for _ in range(250):
            sample = hold.sample(len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = sample["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(sample[col].astype(float).to_numpy(), EPS, 1 - EPS)
            briers.append(float(np.mean((p - y) ** 2)))
            aucs.append(float(roc_auc_score(y, p)) if len(set(y)) > 1 else np.nan)
        rows.append({"instrument": name, "brier_p05": float(np.nanquantile(briers, .05)), "brier_p50": float(np.nanquantile(briers, .5)), "brier_p95": float(np.nanquantile(briers, .95)), "auc_p05": float(np.nanquantile(aucs, .05)), "auc_p50": float(np.nanquantile(aucs, .5)), "auc_p95": float(np.nanquantile(aucs, .95))})
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & df["suppression_subtype"].notna() & (df["confirmatory_contact_eval"] == True)]
        rows.append({"temporal_split": split, "rows": len(g), "avg_pred_contact_count": float(g["pred_hit_capable_contact_count_c"].mean()) if len(g) else "", "avg_pred_xhit_per_contact": float(g["source_aware_starter_conversion"].mean()) if len(g) else "", "avg_pred_two_plus": float(g["source_aware_conversion_p_two_plus_hits"].mean()) if len(g) else "", "observed_two_plus_rate": float(g["two_plus_binary"].mean()) if len(g) else "", "suppression_preserved": bool(g["source_aware_conversion_p_two_plus_hits"].mean() < .30) if len(g) else ""})
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["confirmatory_contact_eval"] == True)].copy()
    rows = []
    for game_id, g in hold.groupby("game_id"):
        if len(g) < 4:
            continue
        pred = g.sort_values("source_aware_conversion_p_two_plus_hits", ascending=False).iloc[0]
        actual = g.sort_values("official_hits", ascending=False).iloc[0]
        pairs = correct_hits = correct_one_two = 0
        gg = g[["source_aware_conversion_p_two_plus_hits", "official_hits", "two_plus_binary", "one_to_two_population"]].dropna().reset_index(drop=True)
        for i in range(len(gg)):
            for j in range(i + 1, len(gg)):
                if gg.loc[i, "official_hits"] != gg.loc[j, "official_hits"]:
                    pairs += 1
                    correct_hits += int((gg.loc[i, "source_aware_conversion_p_two_plus_hits"] > gg.loc[j, "source_aware_conversion_p_two_plus_hits"]) == (gg.loc[i, "official_hits"] > gg.loc[j, "official_hits"]))
                if bool(gg.loc[i, "one_to_two_population"]) and bool(gg.loc[j, "one_to_two_population"]) and gg.loc[i, "two_plus_binary"] != gg.loc[j, "two_plus_binary"]:
                    correct_one_two += int((gg.loc[i, "source_aware_conversion_p_two_plus_hits"] > gg.loc[j, "source_aware_conversion_p_two_plus_hits"]) == (gg.loc[i, "two_plus_binary"] > gg.loc[j, "two_plus_binary"]))
        rows.append({"game_id": game_id, "hitters": len(g), "top_predicted_player": pred["player_name"], "top_actual_player": actual["player_name"], "top_agreement": pred["player_game_key"] == actual["player_game_key"], "pairwise_hit_ordering_accuracy": correct_hits / pairs if pairs else "", "one_to_two_pairwise_correct_pairs": correct_one_two})
    out = pd.DataFrame(rows)
    if not out.empty:
        out["top_agreement_rate"] = out["top_agreement"].mean()
    return out


def second_source(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    two = df[df["outcome_class"].eq("TWO_OR_MORE_HITS") & df["two_plus_hit_source_class"].notna() & (df["confirmatory_contact_eval"] == True)]
    for split in ["validation", "holdout"]:
        for cls, g in two[two["temporal_split"].eq(split)].groupby("two_plus_hit_source_class"):
            rows.append({"temporal_split": split, "second_hit_source": cls, "rows": len(g), "avg_pred_contact_count": float(g["pred_hit_capable_contact_count_c"].mean()), "avg_hitter_conversion": float(g["hitter_empirical_xhit_per_contact"].mean()), "avg_starter_conversion": float(g["source_aware_starter_conversion"].mean()), "avg_bullpen_conversion": float(g["source_aware_bullpen_conversion"].mean()), "avg_pred_two_plus": float(g["source_aware_conversion_p_two_plus_hits"].mean()), "observed_two_plus_rate": 1.0})
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    target = price[price["price_band"].eq("+200_through_+249")].copy() if not price.empty else pd.DataFrame()
    if target.empty:
        return pd.DataFrame()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in m.groupby("temporal_split", dropna=False):
        rows.append({"temporal_split": split, "rows": len(g), "avg_support": float(pd.to_numeric(g["hitter_xhit_support"], errors="coerce").mean()), "support_classes": "|".join(sorted(g["hitter_xhit_support_class"].dropna().astype(str).unique())), "avg_pred_contact_count": float(g["pred_hit_capable_contact_count_c"].mean()), "avg_pred_conversion_quality": float(g["source_aware_starter_conversion"].mean()), "avg_pred_two_plus": float(g["source_aware_conversion_p_two_plus_hits"].mean()), "observed_two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()), "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan,index=g.index)), errors="coerce").mean()), "timing_certification": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique()))})
    return pd.DataFrame(rows)


def date_stability(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for date, g in df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].groupby("slate_date"):
        y = g["two_plus_binary"].astype(int)
        p = g["source_aware_conversion_p_two_plus_hits"]
        rows.append({"slate_date": date, "rows": len(g), "observed_two_plus_rate": float(y.mean()), "avg_predicted": float(p.mean()), "brier": float(((p-y)**2).mean()), "sample_flag": "SPARSE" if len(g) < 20 else "OK"})
    return pd.DataFrame(rows)


def concentration(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)]
    player = hold.groupby(["player_id", "player_name"]).size().reset_index(name="rows").sort_values("rows", ascending=False)
    pitcher = hold.groupby(["opposing_starter_id", "opposing_starter_name"]).size().reset_index(name="rows").sort_values("rows", ascending=False)
    date = hold.groupby("slate_date").size().reset_index(name="rows").sort_values("rows", ascending=False)
    return pd.DataFrame([
        {"dimension": "player", "top_identity": player.iloc[0]["player_name"], "top_rows": int(player.iloc[0]["rows"]), "top_share": float(player.iloc[0]["rows"] / len(hold))},
        {"dimension": "pitcher", "top_identity": pitcher.iloc[0]["opposing_starter_name"], "top_rows": int(pitcher.iloc[0]["rows"]), "top_share": float(pitcher.iloc[0]["rows"] / len(hold))},
        {"dimension": "date", "top_identity": date.iloc[0]["slate_date"], "top_rows": int(date.iloc[0]["rows"]), "top_share": float(date.iloc[0]["rows"] / len(hold))},
    ])


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    contact = read_csv(CONTACT_LEDGER)
    contact = surface_features(contact)
    contact["game_date_dt"] = pd.to_datetime(contact["game_date"], errors="coerce")
    contact = contact[contact["hit_capable_contact"].eq(1)].copy()
    contact["official_hit_on_contact"] = contact["official_hit"].astype(int)
    contact["contact_out"] = contact["bip_out"].astype(int)
    contact["nonstandard_contact_result"] = contact["official_pa_result"].isin(["field_error", "fielders_choice", "fielders_choice_out", "sac_fly"]).astype(int)
    surface, spec = build_surface(contact)
    contact = apply_surface(contact, surface, spec)
    surface_metrics, surface_bands = surface_validation(contact)
    pop = read_csv(CONTACT_POP)
    global_prior = float(spec["global_fit_hit_rate"])
    pop, hitter_profiles, starter_profiles, bullpen_profiles = build_profiles(pop, contact, global_prior)
    pop = apply_game_instruments(pop)
    game_metrics = build_game_metrics(pop)
    bands = probability_bands(pop)
    boot = bootstrap(pop)
    suppress = suppression(pop)
    roster = roster_relative(pop)
    source = second_source(pop)
    plus = plus200(pop)
    stability = date_stability(pop)
    conc = concentration(pop)
    support = pop[["player_game_key", "hitter_xhit_support", "starter_xhit_support", "bullpen_xhit_support", "hitter_xhit_support_class", "starter_xhit_support_class", "bullpen_xhit_support_class", "hitter_personal_weight", "starter_personal_weight"]]
    instruments = pd.DataFrame([
        {"instrument": "control", "definition": "frozen predicted-exposure multi-hit control unchanged"},
        {"instrument": "hitter_conversion_profile", "definition": "predicted hit-capable contacts times strict-prior hitter empirical xHit per contact"},
        {"instrument": "hitter_plus_starter_conversion", "definition": "support-weighted blend of hitter xHit and starter xHit allowed"},
        {"instrument": "source_aware_conversion", "definition": "starter-facing and bullpen-facing predicted contacts times source-specific conversion probabilities"},
        {"instrument": "oracle_ladder_a", "definition": "actual contact count + strict-prior predicted conversion quality"},
        {"instrument": "oracle_ladder_b", "definition": "predicted contact count + actual current-game hard-hit quality"},
        {"instrument": "oracle_ladder_d", "definition": "actual contact count + actual current-game hard-hit quality"},
    ])
    hold = game_metrics[game_metrics["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_exposure_control", "brier"])
    challenger_brier = float(hold.loc["source_aware_conversion", "brier"])
    control_auc = float(hold.loc["frozen_exposure_control", "auc"])
    challenger_auc = float(hold.loc["source_aware_conversion", "auc"])
    surface_holdout = surface_metrics[surface_metrics["temporal_split"].eq("holdout")].iloc[0]
    suppression_ok = bool(suppress[suppress["temporal_split"].eq("holdout")]["suppression_preserved"].iloc[0])
    if not suppression_ok:
        next_decision = "NO HITTER_OWNED CHALLENGER READY"
    elif challenger_brier < control_brier and challenger_auc > control_auc:
        next_decision = "STRICT_PRIOR_CONTACT_CONVERSION_ADDS_MULTI_HIT_VALUE"
    elif challenger_brier < control_brier:
        next_decision = "CONTACT_QUANTITY_LIMITS_DEPLOYABLE_VALUE"
    elif float(surface_holdout["auc"]) < 0.58:
        next_decision = "CONTACT_QUALITY_FORECAST_NOT_READY"
    elif float(hold.loc["oracle_b_predicted_count_actual_quality", "auc"]) > challenger_auc + 0.05:
        next_decision = "OFFICIAL_STATCAST_EXPECTED_METRICS_REQUIRED_NEXT"
    else:
        next_decision = "ORACLE_VALUE_PRIMARILY_REALIZED_GAME_INFORMATION"
    decisions = pd.DataFrame([
        {"decision": "MLB_XHIT_CONTACT_LEDGER_DECISION", "value": "CANONICAL_HIT_CAPABLE_CONTACT_LEDGER_CREATED"},
        {"decision": "MLB_XHIT_EMPIRICAL_SURFACE_DECISION", "value": "EMPIRICAL_XHIT_CONTACT_V1_FIXED_SMOOTHED_SURFACE_FIT"},
        {"decision": "MLB_XHIT_SURFACE_VALIDATION_DECISION", "value": "SURFACE_VALIDATED_DIRECTLY_NO_OFFICIAL_STATCAST_XBA_CLAIM"},
        {"decision": "MLB_XHIT_HITTER_PROFILE_DECISION", "value": "STRICT_PRIOR_HITTER_XHIT_PROFILES_BUILT_WITH_SHRINKAGE"},
        {"decision": "MLB_XHIT_PITCHER_PROFILE_DECISION", "value": "STRICT_PRIOR_STARTER_AND_GLOBAL_BULLPEN_XHIT_ALLOWED_PROFILES_BUILT"},
        {"decision": "MLB_XHIT_SOURCE_AWARE_DECISION", "value": "SOURCE_AWARE_CONVERSION_EVALUATED"},
        {"decision": "MLB_XHIT_UNIFIED_MULTI_HIT_DECISION", "value": "UNIFIED_CONTACT_QUANTITY_X_CONVERSION_EVALUATED"},
        {"decision": "MLB_XHIT_ORACLE_GAP_DECISION", "value": "ORACLE_LADDER_EXECUTED_ORACLE_REMAINS_NONDEPLOYABLE"},
        {"decision": "MLB_XHIT_ONE_TO_TWO_PLUS_HOLDOUT_DECISION", "value": next_decision},
        {"decision": "MLB_XHIT_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_PRESERVED" if suppression_ok else "SUPPRESSION_NOT_PRESERVED"},
        {"decision": "MLB_XHIT_ROSTER_RELATIVE_DECISION", "value": "ROSTER_RELATIVE_CONVERSION_DIAGNOSTIC_RETAINED"},
        {"decision": "MLB_XHIT_SECOND_HIT_SOURCE_DECISION", "value": "SECOND_HIT_SOURCE_CONVERSION_DIAGNOSTIC_RETAINED"},
        {"decision": "MLB_XHIT_PLUS200_DECISION", "value": "PLUS200_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"},
        {"decision": "MLB_XHIT_NEXT_RESEARCH_DECISION", "value": next_decision},
        {"decision": "MLB_XHIT_EXTERNAL_METRIC_REQUIREMENT", "value": "OFFICIAL_STATCAST_EXPECTED_METRICS_NOT_USED_LOCAL_EMPIRICAL_SURFACE_ONLY"},
        {"decision": "MLB_XHIT_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])
    outputs = {
        "canonical_contact_ledger_2026-07-17.csv": contact,
        "empirical_xhit_surface_specification_2026-07-17.csv": pd.DataFrame([spec]),
        "empirical_xhit_surface_cells_2026-07-17.csv": surface,
        "contact_surface_validation_2026-07-17.csv": surface_metrics,
        "contact_surface_probability_bands_2026-07-17.csv": surface_bands,
        "hitter_profile_ledger_2026-07-17.csv": hitter_profiles,
        "starter_profile_ledger_2026-07-17.csv": starter_profiles,
        "bullpen_profile_ledger_2026-07-17.csv": bullpen_profiles,
        "support_and_shrinkage_report_2026-07-17.csv": support,
        "frozen_instruments_2026-07-17.csv": instruments,
        "oracle_gap_ladder_2026-07-17.csv": game_metrics[game_metrics["instrument"].astype(str).str.startswith("oracle")],
        "validation_holdout_metrics_2026-07-17.csv": game_metrics,
        "frozen_probability_band_progression_2026-07-17.csv": bands,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "date_stability_2026-07-17.csv": stability,
        "hitter_pitcher_park_concentration_2026-07-17.csv": conc,
        "suppression_preservation_2026-07-17.csv": suppress,
        "roster_relative_results_2026-07-17.csv": roster,
        "second_hit_source_results_2026-07-17.csv": source,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "next_branch_decision_2026-07-17.csv": decisions[decisions["decision"].eq("MLB_XHIT_NEXT_RESEARCH_DECISION")],
        "research_only_model_artifacts_2026-07-17.csv": pop,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    manifest = []
    for path in [CONTACT_LEDGER, CONTACT_POP, LONG_PRICE]:
        manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    machine = {
        "generated_at_utc": now_utc(),
        "surface_holdout_auc": float(surface_holdout["auc"]),
        "surface_holdout_brier": float(surface_holdout["brier"]),
        "holdout_control_brier": control_brier,
        "holdout_source_aware_conversion_brier": challenger_brier,
        "holdout_control_auc": control_auc,
        "holdout_source_aware_conversion_auc": challenger_auc,
        "next_decision": next_decision,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_empirical_contact_quality_conversion_2026-07-17.json")
    direct = "No. The strict-prior contact-quality profiles did not produce a deployable multi-hit improvement over the frozen control; hit conversion remains mostly dependent on realized in-game contact/quality and likely unavailable park/defense or official expected-metric information." if next_decision != "STRICT_PRIOR_CONTACT_CONVERSION_ADDS_MULTI_HIT_VALUE" else "Yes. The strict-prior empirical contact-quality profiles improved holdout multi-hit performance, but remain research-only."
    write_md(f"""# MLB Strict-Prior Empirical Contact-Quality Conversion Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The experiment fit `empirical_xhit_contact_v1`, a fixed smoothed empirical
surface for `P(official hit | hit-capable contact)`, using fit-period contacts
only. It is not official Statcast xBA.

Holdout contact-surface quality:

| metric | value |
|---|---:|
| Brier | {machine['surface_holdout_brier']:.6f} |
| AUC | {machine['surface_holdout_auc']:.6f} |

Holdout one-hit versus two-plus:

| instrument | brier | auc |
|---|---:|---:|
| frozen exposure control | {control_brier:.6f} | {control_auc:.6f} |
| source-aware conversion | {challenger_brier:.6f} | {challenger_auc:.6f} |

## Direct Answer

{direct}

## Production Status

`MLB_XHIT_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
    validation_report(out_dir)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
