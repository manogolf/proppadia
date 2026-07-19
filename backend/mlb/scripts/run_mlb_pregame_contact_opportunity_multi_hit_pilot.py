#!/usr/bin/env python3
"""Bounded MLB pregame contact-opportunity multi-hit pilot.

This research-only utility asks whether Proppadia can forecast repeated
hit-capable contact opportunities before first pitch. It reuses the frozen
PA-hazard pilot population and temporal splits, fails closed to exact PA/hit
reconciled rows for confirmatory evaluation, and preserves oracle diagnostics
as explicitly nondeployable.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, LaunchAgent changes, threshold search, price optimization, or holdout
tuning are performed.
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
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"

PA_HAZARD_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pa_hit_hazard_multi_hit_pilot/2026-07-17"
POP_PATH = PA_HAZARD_ROOT / "research_only_model_artifacts_2026-07-17.csv"
PA_LEDGER_PATH = PA_HAZARD_ROOT / "canonical_pa_outcome_ledger_2026-07-17.csv"
PA_RECON_PATH = PA_HAZARD_ROOT / "hitter_game_pa_hit_reconciliation_2026-07-17.csv"
PA_METRICS_PATH = PA_HAZARD_ROOT / "validation_holdout_metrics_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717

OFFICIAL_BIP_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "double_play",
    "fielders_choice_out",
    "field_error",
    "sac_fly",
    "sac_bunt",
    "fielders_choice",
    "sac_fly_double_play",
}
HIT_CAPABLE_CONTACT_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "double_play",
    "fielders_choice_out",
    "field_error",
    "sac_fly",
    "fielders_choice",
    "sac_fly_double_play",
}
NON_PA_TERMINAL_EVENTS = {
    "caught_stealing_2b",
    "caught_stealing_3b",
    "caught_stealing_home",
    "pickoff_1b",
    "pickoff_caught_stealing_2b",
}


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


def clip_prob(x: Any, lo: float = 0.001, hi: float = 0.999) -> float:
    try:
        val = float(x)
    except Exception:
        val = 0.2
    if not math.isfinite(val):
        val = 0.2
    return float(min(max(val, lo), hi))


def shrink(raw: float, n: int, prior: float, k: int = 40) -> float:
    if not math.isfinite(raw):
        raw = prior
    return float((raw * n + prior * k) / (n + k)) if n + k else prior


def support_class(n: int) -> str:
    if n >= 80:
        return "HIGH_PERSONAL_SUPPORT"
    if n >= 40:
        return "MODERATE_PERSONAL_SUPPORT"
    if n >= 10:
        return "LOW_PERSONAL_SUPPORT"
    if n > 0:
        return "POPULATION_PRIOR_DOMINATED"
    return "MISSING"


def poisson_two_plus(lam: float) -> float:
    lam = max(float(lam), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    return float(max(0.0, 1.0 - p0 - p1))


def poisson_count_bins(lam: float) -> list[float]:
    lam = max(float(lam), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = (lam**2 / 2.0) * p0
    p3 = (lam**3 / 6.0) * p0
    p4p = max(0.0, 1.0 - p0 - p1 - p2 - p3)
    s = p0 + p1 + p2 + p3 + p4p
    return [p0 / s, p1 / s, p2 / s, p3 / s, p4p / s]


def binary_metric(y: pd.Series, p: pd.Series, label: str, split: str, target: str) -> dict[str, Any]:
    yy = y.astype(int).to_numpy()
    pp = np.clip(pd.to_numeric(p, errors="coerce").fillna(float(pd.to_numeric(p, errors="coerce").mean())).to_numpy(), EPS, 1 - EPS)
    return {
        "temporal_split": split,
        "target": target,
        "instrument": label,
        "rows": int(len(yy)),
        "positives": int(yy.sum()),
        "observed_rate": float(yy.mean()) if len(yy) else "",
        "avg_predicted": float(pp.mean()) if len(pp) else "",
        "brier": float(np.mean((pp - yy) ** 2)) if len(yy) else "",
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])) if len(yy) else "",
        "auc": float(roc_auc_score(yy, pp)) if len(set(yy)) > 1 else "",
        "ece": expected_calibration_error(yy, pp) if len(yy) else "",
    }


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def one_to_two_metric(df: pd.DataFrame, prob: str, instrument: str, split: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    y = g["two_plus_binary"].astype(int).to_numpy()
    p = np.clip(pd.to_numeric(g[prob], errors="coerce").fillna(float(pd.to_numeric(g[prob], errors="coerce").mean())).to_numpy(), EPS, 1 - EPS)
    out = {
        "temporal_split": split,
        "instrument": instrument,
        "rows": int(len(g)),
        "wins_two_plus": int(y.sum()),
        "losses_exactly_one": int(len(y) - y.sum()),
        "observed_two_plus_rate": float(y.mean()) if len(y) else "",
        "avg_predicted_two_plus": float(p.mean()) if len(p) else "",
        "brier": float(np.mean((p - y) ** 2)) if len(y) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(y) else "",
        "auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else "",
        "ece": expected_calibration_error(y, p) if len(y) else "",
    }
    try:
        x = np.log(p / (1 - p))
        slope, intercept = np.polyfit(x, y, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        out["calibration_slope"] = ""
        out["calibration_intercept"] = ""
    return out


def count_accuracy(df: pd.DataFrame, pred: str, actual: str, split: str, instrument: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & (df["confirmatory_contact_eval"] == True)].copy()
    err = pd.to_numeric(g[pred], errors="coerce") - pd.to_numeric(g[actual], errors="coerce")
    return {
        "temporal_split": split,
        "instrument": instrument,
        "actual_target": actual,
        "rows": int(err.notna().sum()),
        "mae": float(err.abs().mean()),
        "rmse": float(np.sqrt((err**2).mean())),
        "bias": float(err.mean()),
        "median_absolute_error": float(err.abs().median()),
    }


def multiclass_quality(df: pd.DataFrame, prefix: str, actual_col: str, split: str, instrument: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & (df["confirmatory_contact_eval"] == True)].copy()
    probs = g[[f"{prefix}_p0", f"{prefix}_p1", f"{prefix}_p2", f"{prefix}_p3", f"{prefix}_p4p"]].astype(float).clip(EPS, 1 - EPS)
    probs = probs.div(probs.sum(axis=1), axis=0)
    actual = pd.to_numeric(g[actual_col], errors="coerce").fillna(0).astype(int).clip(0, 4)
    y = np.zeros((len(actual), 5))
    y[np.arange(len(actual)), actual.to_numpy()] = 1
    briers = ((probs.to_numpy() - y) ** 2).mean(axis=0)
    return {
        "temporal_split": split,
        "instrument": instrument,
        "actual_target": actual_col,
        "rows": int(len(g)),
        "multiclass_log_loss": float(log_loss(actual.to_numpy(), probs.to_numpy(), labels=[0, 1, 2, 3, 4])),
        "brier_class_0": float(briers[0]),
        "brier_class_1": float(briers[1]),
        "brier_class_2": float(briers[2]),
        "brier_class_3": float(briers[3]),
        "brier_class_4plus": float(briers[4]),
    }


def bind_contact_outcomes(pa: pd.DataFrame) -> pd.DataFrame:
    out = pa.copy()
    event = out["official_pa_result"].astype(str)
    out["is_non_pa_terminal_event"] = event.isin(NON_PA_TERMINAL_EVENTS).astype(int)
    out["canonical_batter_pa"] = (out["is_non_pa_terminal_event"] == 0).astype(int)
    out["official_bip_event"] = event.isin(OFFICIAL_BIP_EVENTS).astype(int)
    out["terminal_contact_pa"] = out["official_bip_event"]
    out["hit_capable_contact"] = event.isin(HIT_CAPABLE_CONTACT_EVENTS).astype(int)
    out["home_run"] = event.eq("home_run").astype(int)
    out["walk"] = event.isin(["walk", "intent_walk"]).astype(int)
    out["hbp"] = event.eq("hit_by_pitch").astype(int)
    out["sacrifice"] = event.isin(["sac_fly", "sac_bunt", "sac_fly_double_play"]).astype(int)
    out["catcher_interference"] = event.eq("catcher_interf").astype(int)
    out["non_contact_terminal_pa"] = ((out["canonical_batter_pa"] == 1) & (out["terminal_contact_pa"] == 0)).astype(int)
    out["other_non_contact_pa"] = ((out["canonical_batter_pa"] == 1) & (out["terminal_contact_pa"] == 0) & (out["strikeout"] == 0) & (out["walk"] == 0) & (out["hbp"] == 0)).astype(int)
    return out


def aggregate_contact_targets(pa: pd.DataFrame) -> pd.DataFrame:
    agg = pa.groupby(["game_date", "game_id", "batter_id"]).agg(
        total_pa=("canonical_batter_pa", "sum"),
        official_bip_count=("official_bip_event", "sum"),
        terminal_contact_pa_count=("terminal_contact_pa", "sum"),
        hit_capable_contact_count=("hit_capable_contact", "sum"),
        strikeouts=("strikeout", "sum"),
        walks=("walk", "sum"),
        hbp=("hbp", "sum"),
        home_runs=("home_run", "sum"),
        other_non_contact_pa=("other_non_contact_pa", "sum"),
        non_contact_terminal_pa=("non_contact_terminal_pa", "sum"),
        contact_outs=("bip_out", "sum"),
        official_hits=("official_hit", "sum"),
        non_pa_terminal_events=("is_non_pa_terminal_event", "sum"),
        catcher_interference=("catcher_interference", "sum"),
        sacrifices=("sacrifice", "sum"),
    ).reset_index().rename(columns={"game_date": "slate_date", "batter_id": "player_id"})
    agg["pa_identity_check"] = agg["total_pa"] - agg["terminal_contact_pa_count"] - agg["non_contact_terminal_pa"]
    return agg


def pa_mismatch_audit(recon: pd.DataFrame, pa: pd.DataFrame) -> pd.DataFrame:
    event_counts = pa.pivot_table(
        index=["game_date", "game_id", "batter_id"],
        columns="official_pa_result",
        values="pa_key",
        aggfunc="count",
        fill_value=0,
    ).reset_index().rename(columns={"game_date": "slate_date", "batter_id": "player_id"})
    out = recon[recon["reconciliation_status"].ne("PASS")].copy()
    out = out.merge(event_counts, on=["slate_date", "game_id", "player_id"], how="left")
    out["pa_delta_ledger_minus_official"] = pd.to_numeric(out["ledger_pa_count"], errors="coerce") - pd.to_numeric(out["official_pa"], errors="coerce")
    non_pa_cols = [c for c in NON_PA_TERMINAL_EVENTS if c in out.columns]
    if non_pa_cols:
        out["non_pa_terminal_events_found"] = out[non_pa_cols].sum(axis=1)
    else:
        out["non_pa_terminal_events_found"] = 0
    out["primary_cause"] = np.select(
        [
            (out["pa_delta_ledger_minus_official"] > 0) & (out["non_pa_terminal_events_found"] > 0),
            (out["pa_delta_ledger_minus_official"] > 0) & (out["non_pa_terminal_events_found"] == 0),
            out["pa_delta_ledger_minus_official"] < 0,
        ],
        [
            "non_pa_baserunning_or_pickoff_terminal_event_included",
            "official_definition_difference_or_duplicate_pa_boundary",
            "benchmark_source_has_extra_pa_or_feed_missing_pa_boundary",
        ],
        default="unresolved_pa_denominator_difference",
    )
    keep_cols = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "official_pa",
        "ledger_pa_count",
        "official_hits",
        "ledger_hits_count",
        "pa_delta_ledger_minus_official",
        "non_pa_terminal_events_found",
        "primary_cause",
    ]
    return out[keep_cols]


def build_profiles(pop: pd.DataFrame, pa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_pa = pa[pa["canonical_batter_pa"].eq(1)].copy()
    train_pa["game_date_dt"] = pd.to_datetime(train_pa["game_date"], errors="coerce")
    train_pa["batter_key"] = train_pa["batter_id"].astype("Int64").astype(str)
    train_pa["pitcher_key"] = train_pa["pitcher_id"].astype("Int64").astype(str)
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    pop["batter_key"] = pop["player_id"].astype("Int64").astype(str)
    pop["starter_key"] = pd.to_numeric(pop["opposing_starter_id"], errors="coerce").astype("Int64").astype(str)
    priors = {
        "terminal_contact": float(train_pa["terminal_contact_pa"].mean()),
        "hit_capable_contact": float(train_pa["hit_capable_contact"].mean()),
        "official_hit": float(train_pa["official_hit"].mean()),
        "strikeout": float(train_pa["strikeout"].mean()),
        "walk_hbp": float((train_pa["walk"] + train_pa["hbp"]).mean()),
    }
    hit_cap = train_pa[train_pa["hit_capable_contact"].eq(1)]
    priors["hit_on_hit_capable_contact"] = float(hit_cap["official_hit"].mean()) if len(hit_cap) else 0.32
    rows: list[dict[str, Any]] = []
    hitter_rows: list[dict[str, Any]] = []
    pitcher_rows: list[dict[str, Any]] = []
    bullpen_rows: list[dict[str, Any]] = []
    for date, day in pop.groupby("slate_date_dt", dropna=False):
        prior = train_pa[train_pa["game_date_dt"] < pd.Timestamp(date)] if pd.notna(date) else train_pa.iloc[0:0]
        hitter = prior.groupby("batter_key").agg(
            pa_count=("canonical_batter_pa", "count"),
            terminal_contact_rate=("terminal_contact_pa", "mean"),
            hit_capable_contact_rate=("hit_capable_contact", "mean"),
            hit_rate=("official_hit", "mean"),
            strikeout_rate=("strikeout", "mean"),
            walk_hbp_rate=("walk_hbp", "mean"),
        ).to_dict("index")
        pitcher = prior.groupby("pitcher_key").agg(
            pa_count=("canonical_batter_pa", "count"),
            terminal_contact_allowed=("terminal_contact_pa", "mean"),
            hit_capable_contact_allowed=("hit_capable_contact", "mean"),
            hit_allowed=("official_hit", "mean"),
            strikeout_rate=("strikeout", "mean"),
            walk_hbp_rate=("walk_hbp", "mean"),
        ).to_dict("index")
        prior_hc = prior[prior["hit_capable_contact"].eq(1)]
        hitter_hc = prior_hc.groupby("batter_key").agg(hcc_count=("official_hit", "count"), hit_on_hcc=("official_hit", "mean")).to_dict("index") if len(prior_hc) else {}
        pitcher_hc = prior_hc.groupby("pitcher_key").agg(hcc_count=("official_hit", "count"), hit_on_hcc_allowed=("official_hit", "mean")).to_dict("index") if len(prior_hc) else {}
        bullpen_prior = prior[prior["starter_reliever_role"].eq("RELIEVER_FACING_PA")]
        bullpen_hc = bullpen_prior[bullpen_prior["hit_capable_contact"].eq(1)]
        bp_n = len(bullpen_prior)
        bullpen_terminal = shrink(float(bullpen_prior["terminal_contact_pa"].mean()) if bp_n else np.nan, bp_n, priors["terminal_contact"])
        bullpen_hcc = shrink(float(bullpen_prior["hit_capable_contact"].mean()) if bp_n else np.nan, bp_n, priors["hit_capable_contact"])
        bullpen_hit_on_hcc = shrink(float(bullpen_hc["official_hit"].mean()) if len(bullpen_hc) else np.nan, len(bullpen_hc), priors["hit_on_hit_capable_contact"])
        for _, r in day.iterrows():
            hk = r["batter_key"]
            pk = r["starter_key"]
            hp = hitter.get(hk, {})
            pp = pitcher.get(pk, {})
            hh = hitter_hc.get(hk, {})
            ph = pitcher_hc.get(pk, {})
            h_n = int(hp.get("pa_count", 0) or 0)
            p_n = int(pp.get("pa_count", 0) or 0)
            hh_n = int(hh.get("hcc_count", 0) or 0)
            ph_n = int(ph.get("hcc_count", 0) or 0)
            h_terminal = shrink(float(hp.get("terminal_contact_rate", np.nan)), h_n, priors["terminal_contact"])
            p_terminal = shrink(float(pp.get("terminal_contact_allowed", np.nan)), p_n, priors["terminal_contact"])
            h_hcc = shrink(float(hp.get("hit_capable_contact_rate", np.nan)), h_n, priors["hit_capable_contact"])
            p_hcc = shrink(float(pp.get("hit_capable_contact_allowed", np.nan)), p_n, priors["hit_capable_contact"])
            h_hit_hcc = shrink(float(hh.get("hit_on_hcc", np.nan)), hh_n, priors["hit_on_hit_capable_contact"])
            p_hit_hcc = shrink(float(ph.get("hit_on_hcc_allowed", np.nan)), ph_n, priors["hit_on_hit_capable_contact"])
            starter_terminal = (h_terminal + p_terminal) / 2.0
            starter_hcc = (h_hcc + p_hcc) / 2.0
            bullpen_terminal_p = (h_terminal + bullpen_terminal) / 2.0
            bullpen_hcc_p = (h_hcc + bullpen_hcc) / 2.0
            hit_on_hcc = (h_hit_hcc + p_hit_hcc) / 2.0
            bullpen_hit_on_hcc_p = (h_hit_hcc + bullpen_hit_on_hcc) / 2.0
            rows.append({
                "player_game_key": r["player_game_key"],
                "hitter_contact_pa_support": h_n,
                "pitcher_contact_pa_support": p_n,
                "hitter_hit_capable_support": hh_n,
                "pitcher_hit_capable_support": ph_n,
                "hitter_contact_support_class": support_class(h_n),
                "pitcher_contact_support_class": support_class(p_n),
                "hitter_hit_capable_support_class": support_class(hh_n),
                "pitcher_hit_capable_support_class": support_class(ph_n),
                "contact_pred_strikeout_rate": shrink(float(hp.get("strikeout_rate", np.nan)), h_n, priors["strikeout"]),
                "contact_pred_walk_hbp_rate": shrink(float(hp.get("walk_hbp_rate", np.nan)), h_n, priors["walk_hbp"]),
                "contact_pred_terminal_contact_rate": starter_terminal,
                "contact_pred_hit_capable_contact_rate": starter_hcc,
                "contact_pred_bullpen_terminal_contact_rate": bullpen_terminal_p,
                "contact_pred_bullpen_hit_capable_contact_rate": bullpen_hcc_p,
                "contact_pred_hit_on_hit_capable_contact": hit_on_hcc,
                "contact_pred_bullpen_hit_on_hit_capable_contact": bullpen_hit_on_hcc_p,
            })
            hitter_rows.append({"player_game_key": r["player_game_key"], "player_id": r["player_id"], "pa_support": h_n, "hit_capable_contact_support": hh_n, "terminal_contact_rate": h_terminal, "hit_capable_contact_rate": h_hcc, "strikeout_rate": shrink(float(hp.get("strikeout_rate", np.nan)), h_n, priors["strikeout"]), "walk_hbp_rate": shrink(float(hp.get("walk_hbp_rate", np.nan)), h_n, priors["walk_hbp"]), "support_class": support_class(h_n)})
            pitcher_rows.append({"player_game_key": r["player_game_key"], "pitcher_id": r.get("opposing_starter_id"), "pa_support": p_n, "hit_capable_contact_support": ph_n, "terminal_contact_allowed": p_terminal, "hit_capable_contact_allowed": p_hcc, "support_class": support_class(p_n)})
            bullpen_rows.append({"player_game_key": r["player_game_key"], "bullpen_prior_pa": bp_n, "terminal_contact_allowed": bullpen_terminal, "hit_capable_contact_allowed": bullpen_hcc, "hit_on_hit_capable_contact": bullpen_hit_on_hcc, "support_class": support_class(bp_n)})
    return pop.merge(pd.DataFrame(rows), on="player_game_key", how="left"), pd.DataFrame(hitter_rows), pd.DataFrame(pitcher_rows), pd.DataFrame(bullpen_rows)


def apply_contact_instruments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    total_pa = pd.to_numeric(out["turnover_total_pa"], errors="coerce").fillna(pd.to_numeric(out["pred_total_pa"], errors="coerce")).fillna(4.0)
    starter_pa = pd.to_numeric(out["turnover_starter_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_starter_pa"], errors="coerce")).fillna(2.4)
    bullpen_pa = pd.to_numeric(out["turnover_bullpen_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_bullpen_pa"], errors="coerce")).fillna(1.6)
    out["pred_contact_count_a"] = total_pa * pd.to_numeric(out["contact_pred_terminal_contact_rate"], errors="coerce")
    out["pred_hit_capable_contact_count_c"] = total_pa * pd.to_numeric(out["contact_pred_hit_capable_contact_rate"], errors="coerce")
    out["pred_source_aware_contact_count_d"] = starter_pa * pd.to_numeric(out["contact_pred_terminal_contact_rate"], errors="coerce") + bullpen_pa * pd.to_numeric(out["contact_pred_bullpen_terminal_contact_rate"], errors="coerce")
    out["pred_source_aware_hit_capable_contact_count_d"] = starter_pa * pd.to_numeric(out["contact_pred_hit_capable_contact_rate"], errors="coerce") + bullpen_pa * pd.to_numeric(out["contact_pred_bullpen_hit_capable_contact_rate"], errors="coerce")
    for prefix, col in [
        ("contact_count_distribution_b", "pred_contact_count_a"),
        ("hit_capable_contact_distribution_c", "pred_hit_capable_contact_count_c"),
        ("source_aware_contact_distribution_d", "pred_source_aware_contact_count_d"),
        ("source_aware_hit_capable_distribution_d", "pred_source_aware_hit_capable_contact_count_d"),
    ]:
        probs = [poisson_count_bins(v) for v in pd.to_numeric(out[col], errors="coerce").fillna(0)]
        out[f"{prefix}_p0"] = [v[0] for v in probs]
        out[f"{prefix}_p1"] = [v[1] for v in probs]
        out[f"{prefix}_p2"] = [v[2] for v in probs]
        out[f"{prefix}_p3"] = [v[3] for v in probs]
        out[f"{prefix}_p4p"] = [v[4] for v in probs]
    out["pred_contact_count_ge2"] = 1 - out["contact_count_distribution_b_p0"] - out["contact_count_distribution_b_p1"]
    out["pred_contact_count_ge3"] = out["contact_count_distribution_b_p3"] + out["contact_count_distribution_b_p4p"]
    out["pred_contact_count_ge4"] = out["contact_count_distribution_b_p4p"]
    out["pred_hit_capable_contact_count_ge2"] = 1 - out["hit_capable_contact_distribution_c_p0"] - out["hit_capable_contact_distribution_c_p1"]
    out["pred_hit_capable_contact_count_ge3"] = out["hit_capable_contact_distribution_c_p3"] + out["hit_capable_contact_distribution_c_p4p"]
    hcc_hit = pd.to_numeric(out["contact_pred_hit_on_hit_capable_contact"], errors="coerce").fillna(0.32)
    bp_hcc_hit = pd.to_numeric(out["contact_pred_bullpen_hit_on_hit_capable_contact"], errors="coerce").fillna(hcc_hit)
    out["contact_challenger_p_two_plus_hits"] = [poisson_two_plus(lam * p) for lam, p in zip(out["pred_hit_capable_contact_count_c"], hcc_hit)]
    source_lam_hit = starter_pa * pd.to_numeric(out["contact_pred_hit_capable_contact_rate"], errors="coerce").fillna(0.65) * hcc_hit + bullpen_pa * pd.to_numeric(out["contact_pred_bullpen_hit_capable_contact_rate"], errors="coerce").fillna(0.65) * bp_hcc_hit
    out["source_aware_contact_challenger_p_two_plus_hits"] = [poisson_two_plus(v) for v in source_lam_hit]
    out["oracle_actual_total_pa_only_p_two_plus_hits"] = [poisson_two_plus(pa * p) for pa, p in zip(pd.to_numeric(out["actual_total_pa_target"], errors="coerce").fillna(total_pa), pd.to_numeric(out["direct_pa_hit_rate"], errors="coerce").fillna(0.22))]
    out["oracle_actual_contact_count_p_two_plus_hits"] = [poisson_two_plus(c * p) for c, p in zip(pd.to_numeric(out["hit_capable_contact_count"], errors="coerce").fillna(0), hcc_hit)]
    out["oracle_actual_contact_count_plus_hit_on_contact_p_two_plus_hits"] = out["oracle_actual_contact_count_p_two_plus_hits"]
    return out


def build_count_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split in ["validation", "holdout"]:
        rows.append(count_accuracy(df, "pred_contact_count_a", "terminal_contact_pa_count", split, "contact_forecast_a_expected_terminal_contact"))
        rows.append(count_accuracy(df, "pred_hit_capable_contact_count_c", "hit_capable_contact_count", split, "contact_forecast_c_hit_capable_contact"))
        rows.append(count_accuracy(df, "pred_source_aware_contact_count_d", "terminal_contact_pa_count", split, "contact_forecast_d_source_aware_terminal_contact"))
        rows.append(multiclass_quality(df, "contact_count_distribution_b", "terminal_contact_pa_count", split, "contact_forecast_b_terminal_contact_distribution"))
        rows.append(multiclass_quality(df, "hit_capable_contact_distribution_c", "hit_capable_contact_count", split, "contact_forecast_c_hit_capable_distribution"))
        sub = df[(df["temporal_split"].eq(split)) & (df["confirmatory_contact_eval"] == True)]
        rows.append(binary_metric((pd.to_numeric(sub["terminal_contact_pa_count"], errors="coerce") >= 2), sub["pred_contact_count_ge2"], "contact_forecast_b", split, "terminal_contact_count_ge2"))
        rows.append(binary_metric((pd.to_numeric(sub["terminal_contact_pa_count"], errors="coerce") >= 3), sub["pred_contact_count_ge3"], "contact_forecast_b", split, "terminal_contact_count_ge3"))
        rows.append(binary_metric((pd.to_numeric(sub["terminal_contact_pa_count"], errors="coerce") >= 4), sub["pred_contact_count_ge4"], "contact_forecast_b", split, "terminal_contact_count_ge4"))
        rows.append(binary_metric((pd.to_numeric(sub["hit_capable_contact_count"], errors="coerce") >= 2), sub["pred_hit_capable_contact_count_ge2"], "contact_forecast_c", split, "hit_capable_contact_count_ge2"))
        rows.append(binary_metric((pd.to_numeric(sub["hit_capable_contact_count"], errors="coerce") >= 3), sub["pred_hit_capable_contact_count_ge3"], "contact_forecast_c", split, "hit_capable_contact_count_ge3"))
    return pd.DataFrame(rows)


def build_one_to_two(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    instruments = {
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "prior_pa_hazard_bip_decomposition": "bip_decomposition_p_two_plus_hits",
        "prior_pa_hazard_unified_sequence": "unified_pa_sequence_p_two_plus_hits",
        "contact_count_challenger": "contact_challenger_p_two_plus_hits",
        "source_aware_contact_challenger": "source_aware_contact_challenger_p_two_plus_hits",
        "oracle_actual_total_pa_only": "oracle_actual_total_pa_only_p_two_plus_hits",
        "oracle_actual_contact_count": "oracle_actual_contact_count_p_two_plus_hits",
        "oracle_actual_contact_quality": "oracle_contact_quality_p_two_plus_hits",
    }
    for split in ["validation", "holdout"]:
        for name, col in instruments.items():
            rows.append(one_to_two_metric(df, col, name, split))
    return pd.DataFrame(rows)


def bootstrap_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    rows = []
    for name, col in {
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "contact_count_challenger": "contact_challenger_p_two_plus_hits",
        "source_aware_contact_challenger": "source_aware_contact_challenger_p_two_plus_hits",
    }.items():
        briers, aucs = [], []
        for _ in range(250):
            sample = hold.sample(n=len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = sample["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(sample[col].astype(float).to_numpy(), EPS, 1 - EPS)
            briers.append(float(np.mean((p - y) ** 2)))
            aucs.append(float(roc_auc_score(y, p)) if len(set(y)) > 1 else np.nan)
        rows.append({"instrument": name, "brier_p05": float(np.nanquantile(briers, .05)), "brier_p50": float(np.nanquantile(briers, .5)), "brier_p95": float(np.nanquantile(briers, .95)), "auc_p05": float(np.nanquantile(aucs, .05)), "auc_p50": float(np.nanquantile(aucs, .5)), "auc_p95": float(np.nanquantile(aucs, .95))})
    return pd.DataFrame(rows)


def build_oracle_gap(one_to_two: pd.DataFrame) -> pd.DataFrame:
    hold = one_to_two[one_to_two["temporal_split"].eq("holdout")].set_index("instrument")
    control_auc = float(hold.loc["frozen_exposure_control", "auc"])
    rows = []
    for inst in [
        "oracle_actual_total_pa_only",
        "oracle_actual_contact_count",
        "oracle_actual_contact_quality",
        "contact_count_challenger",
        "source_aware_contact_challenger",
    ]:
        r = hold.loc[inst]
        rows.append({
            "instrument": inst,
            "holdout_brier": r["brier"],
            "holdout_auc": r["auc"],
            "auc_lift_vs_control": float(r["auc"]) - control_auc if r["auc"] != "" else "",
            "brier_delta_vs_control": float(r["brier"]) - float(hold.loc["frozen_exposure_control", "brier"]),
            "deployability": "oracle_nondeployable" if inst.startswith("oracle") else "legitimate_pregame",
        })
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & df["suppression_subtype"].notna() & (df["confirmatory_contact_eval"] == True)]
        if g.empty:
            rows.append({
                "temporal_split": split,
                "rows": 0,
                "avg_predicted_pa": "",
                "avg_predicted_contact_opportunities": "",
                "avg_predicted_hit_capable_contact_opportunities": "",
                "avg_predicted_two_plus_probability": "",
                "observed_two_plus_rate": "",
                "suppression_preserved": "",
                "status": "NO_SUPPRESSION_ROWS_IN_SPLIT",
            })
            continue
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "avg_predicted_pa": float(pd.to_numeric(g["turnover_total_pa"], errors="coerce").mean()),
            "avg_predicted_contact_opportunities": float(g["pred_contact_count_a"].mean()),
            "avg_predicted_hit_capable_contact_opportunities": float(g["pred_hit_capable_contact_count_c"].mean()),
            "avg_predicted_two_plus_probability": float(g["source_aware_contact_challenger_p_two_plus_hits"].mean()),
            "observed_two_plus_rate": float(g["two_plus_binary"].mean()),
            "suppression_preserved": bool(g["source_aware_contact_challenger_p_two_plus_hits"].mean() < 0.30),
            "status": "EVALUATED",
        })
    return pd.DataFrame(rows)


def hitter_owned(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["suppression_subtype"].isna()) & (df["confirmatory_contact_eval"] == True)].copy()
    if hold.empty:
        return pd.DataFrame()
    hold["pred_contact_band"] = pd.qcut(hold["pred_hit_capable_contact_count_c"], q=4, labels=["low", "mid_low", "mid_high", "high"], duplicates="drop")
    rows = []
    for band, g in hold.groupby("pred_contact_band", observed=True):
        rows.append({
            "temporal_split": "holdout",
            "band": str(band),
            "rows": len(g),
            "observed_two_plus_rate": float(g["two_plus_binary"].mean()),
            "avg_predicted_contact_opportunities": float(g["pred_contact_count_a"].mean()),
            "avg_predicted_hit_capable_contact": float(g["pred_hit_capable_contact_count_c"].mean()),
            "avg_predicted_hit_on_contact": float(g["contact_pred_hit_on_hit_capable_contact"].mean()),
            "avg_strikeout_rate": float(g["contact_pred_strikeout_rate"].mean()),
            "interpretation": "higher_projected_hit_capable_contact" if str(band) == "high" else "lower_projected_hit_capable_contact",
        })
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["confirmatory_contact_eval"] == True)].copy()
    rows = []
    for game_id, g in hold.groupby("game_id"):
        if len(g) < 4:
            continue
        pred_contact_top = g.sort_values("pred_hit_capable_contact_count_c", ascending=False).iloc[0]
        actual_contact_top = g.sort_values("hit_capable_contact_count", ascending=False).iloc[0]
        pred_pa_top = g.sort_values("turnover_total_pa", ascending=False).iloc[0]
        pred_hits_top = g.sort_values("source_aware_contact_challenger_p_two_plus_hits", ascending=False).iloc[0]
        pairs = correct_contact = correct_hits = 0
        gg = g[["pred_hit_capable_contact_count_c", "hit_capable_contact_count", "source_aware_contact_challenger_p_two_plus_hits", "official_hits"]].dropna().reset_index(drop=True)
        for i in range(len(gg)):
            for j in range(i + 1, len(gg)):
                if gg.loc[i, "hit_capable_contact_count"] != gg.loc[j, "hit_capable_contact_count"]:
                    pairs += 1
                    correct_contact += int((gg.loc[i, "pred_hit_capable_contact_count_c"] > gg.loc[j, "pred_hit_capable_contact_count_c"]) == (gg.loc[i, "hit_capable_contact_count"] > gg.loc[j, "hit_capable_contact_count"]))
                if gg.loc[i, "official_hits"] != gg.loc[j, "official_hits"]:
                    correct_hits += int((gg.loc[i, "source_aware_contact_challenger_p_two_plus_hits"] > gg.loc[j, "source_aware_contact_challenger_p_two_plus_hits"]) == (gg.loc[i, "official_hits"] > gg.loc[j, "official_hits"]))
        rows.append({
            "game_id": game_id,
            "hitters": len(g),
            "top_predicted_contact_player": pred_contact_top["player_name"],
            "top_actual_contact_player": actual_contact_top["player_name"],
            "top_predicted_pa_player": pred_pa_top["player_name"],
            "top_predicted_two_plus_player": pred_hits_top["player_name"],
            "top_contact_agreement": pred_contact_top["player_game_key"] == actual_contact_top["player_game_key"],
            "pairwise_contact_ordering_accuracy": correct_contact / pairs if pairs else "",
            "pairwise_hit_ordering_accuracy": correct_hits / pairs if pairs else "",
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        out["top_contact_agreement_rate"] = out["top_contact_agreement"].mean()
    return out


def second_hit_source(df: pd.DataFrame) -> pd.DataFrame:
    two = df[df["outcome_class"].eq("TWO_OR_MORE_HITS") & df["two_plus_hit_source_class"].notna() & (df["confirmatory_contact_eval"] == True)]
    rows = []
    for split in ["validation", "holdout"]:
        for cls, g in two[two["temporal_split"].eq(split)].groupby("two_plus_hit_source_class"):
            rows.append({
                "temporal_split": split,
                "second_hit_source": cls,
                "rows": len(g),
                "avg_pred_contact_count": float(g["pred_contact_count_a"].mean()),
                "avg_pred_hit_capable_contact_count": float(g["pred_hit_capable_contact_count_c"].mean()),
                "avg_actual_hit_capable_contact_count": float(g["hit_capable_contact_count"].mean()),
                "avg_source_aware_two_plus": float(g["source_aware_contact_challenger_p_two_plus_hits"].mean()),
            })
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    target = price[price["price_band"].eq("+200_through_+249")].copy() if not price.empty else pd.DataFrame()
    if target.empty:
        return pd.DataFrame()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in m.groupby("temporal_split", dropna=False):
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "avg_predicted_contact_count": float(g["pred_contact_count_a"].mean()),
            "avg_predicted_hit_capable_contact_count": float(g["pred_hit_capable_contact_count_c"].mean()),
            "avg_predicted_two_plus": float(g["source_aware_contact_challenger_p_two_plus_hits"].mean()),
            "observed_two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()),
            "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan, index=g.index)), errors="coerce").mean()),
            "diagnostic_roi": float(pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan, index=g.index)), errors="coerce").mean()),
            "timing_certification": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique())),
        })
    return pd.DataFrame(rows)


def probability_band_progression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    instruments = {
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "contact_count_challenger": "contact_challenger_p_two_plus_hits",
        "source_aware_contact_challenger": "source_aware_contact_challenger_p_two_plus_hits",
    }
    for inst, col in instruments.items():
        fit = df[(df["temporal_split"].eq("fit")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)][col].dropna()
        if fit.empty:
            continue
        edges = sorted(set([float("-inf"), *np.quantile(fit, [0.25, 0.5, 0.75]).tolist(), float("inf")]))
        labels = [f"fit_q{i+1}" for i in range(len(edges) - 1)]
        for split in ["validation", "holdout"]:
            g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
            g["frozen_probability_band"] = pd.cut(g[col], bins=edges, labels=labels, include_lowest=True)
            for band, b in g.groupby("frozen_probability_band", observed=True):
                rows.append({
                    "temporal_split": split,
                    "instrument": inst,
                    "frozen_probability_band": str(band),
                    "rows": len(b),
                    "wins_two_plus": int(b["two_plus_binary"].sum()),
                    "observed_two_plus_rate": float(b["two_plus_binary"].mean()),
                    "avg_predicted_two_plus": float(b[col].mean()),
                    "sample_flag": "SPARSE" if len(b) < 50 else "OK",
                })
    return pd.DataFrame(rows)


def date_stability(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for inst, col in {
        "frozen_exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "contact_count_challenger": "contact_challenger_p_two_plus_hits",
        "source_aware_contact_challenger": "source_aware_contact_challenger_p_two_plus_hits",
    }.items():
        g = df[(df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
        for (split, date), d in g.groupby(["temporal_split", "slate_date"]):
            y = d["two_plus_binary"].astype(int)
            p = pd.to_numeric(d[col], errors="coerce")
            rows.append({
                "temporal_split": split,
                "slate_date": date,
                "instrument": inst,
                "rows": len(d),
                "observed_two_plus_rate": float(y.mean()),
                "avg_predicted_two_plus": float(p.mean()),
                "brier": float(((p - y) ** 2).mean()),
                "sample_flag": "SPARSE" if len(d) < 20 else "OK",
            })
    return pd.DataFrame(rows)


def concentration(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    if hold.empty:
        return pd.DataFrame()
    player = hold.groupby(["player_id", "player_name"]).size().reset_index(name="rows").sort_values("rows", ascending=False)
    date = hold.groupby("slate_date").size().reset_index(name="rows").sort_values("rows", ascending=False)
    return pd.DataFrame([
        {"scope": "holdout_one_to_two", "dimension": "player", "total_rows": len(hold), "top_identity": player.iloc[0]["player_name"], "top_rows": int(player.iloc[0]["rows"]), "top_share": float(player.iloc[0]["rows"] / len(hold))},
        {"scope": "holdout_one_to_two", "dimension": "date", "total_rows": len(hold), "top_identity": date.iloc[0]["slate_date"], "top_rows": int(date.iloc[0]["rows"]), "top_share": float(date.iloc[0]["rows"] / len(hold))},
    ])


def contact_slice_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    base = df[(df["temporal_split"].isin(["validation", "holdout"])) & (df["confirmatory_contact_eval"] == True)].copy()
    base["lineup_position_bucket"] = base.get("lineup_bucket", pd.Series("unknown", index=base.index)).fillna("unknown")
    base["hitter_strikeout_class"] = pd.cut(pd.to_numeric(base["contact_pred_strikeout_rate"], errors="coerce"), bins=[-1, 0.18, 0.26, 1], labels=["low_k", "mid_k", "high_k"])
    base["starter_exposure_bucket"] = pd.cut(pd.to_numeric(base["turnover_starter_pa"], errors="coerce"), bins=[-1, 2.0, 3.0, 10], labels=["low_starter_exposure", "mid_starter_exposure", "high_starter_exposure"])
    base["support_bucket"] = base["hitter_contact_support_class"].fillna("unknown")
    base["handedness_bucket"] = base.get("handedness_compatibility", pd.Series("unknown", index=base.index)).fillna("unknown")
    for split in ["validation", "holdout"]:
        s = base[base["temporal_split"].eq(split)]
        for dim in ["lineup_position_bucket", "hitter_strikeout_class", "starter_exposure_bucket", "support_bucket", "handedness_bucket"]:
            for bucket, g in s.groupby(dim, observed=True):
                if len(g) == 0:
                    continue
                rows.append({
                    "temporal_split": split,
                    "slice_dimension": dim,
                    "slice_bucket": str(bucket),
                    "rows": len(g),
                    "avg_actual_contact_count": float(g["terminal_contact_pa_count"].mean()),
                    "avg_pred_contact_count": float(g["pred_contact_count_a"].mean()),
                    "contact_mae": float((g["pred_contact_count_a"] - g["terminal_contact_pa_count"]).abs().mean()),
                    "one_to_two_rows": int(g["one_to_two_population"].sum()),
                    "one_to_two_observed_rate": float(g.loc[g["one_to_two_population"] == True, "two_plus_binary"].mean()) if g["one_to_two_population"].sum() else "",
                    "one_to_two_avg_pred": float(g.loc[g["one_to_two_population"] == True, "source_aware_contact_challenger_p_two_plus_hits"].mean()) if g["one_to_two_population"].sum() else "",
                    "sample_flag": "SPARSE" if len(g) < 50 else "OK",
                })
    return pd.DataFrame(rows)


def semantic_binding(pa: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for event, g in pa.groupby("official_pa_result"):
        rows.append({
            "official_pa_result": event,
            "rows": len(g),
            "official_bip_count_includes": bool(event in OFFICIAL_BIP_EVENTS),
            "terminal_contact_pa_includes": bool(event in OFFICIAL_BIP_EVENTS),
            "hit_capable_contact_includes": bool(event in HIT_CAPABLE_CONTACT_EVENTS),
            "non_pa_terminal_excluded_from_confirmatory_pa": bool(event in NON_PA_TERMINAL_EVENTS),
            "official_hit": bool(g["official_hit"].max()),
            "home_run": bool(event == "home_run"),
            "sacrifice": bool(event in ["sac_fly", "sac_bunt", "sac_fly_double_play"]),
            "catcher_interference": bool(event == "catcher_interf"),
            "notes": "home_run retained as hit-capable contact" if event == "home_run" else "",
        })
    return pd.DataFrame(rows).sort_values("rows", ascending=False)


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
    base_pop = read_csv(POP_PATH)
    pa = bind_contact_outcomes(read_csv(PA_LEDGER_PATH))
    recon = read_csv(PA_RECON_PATH)
    target = aggregate_contact_targets(pa)
    mismatch = pa_mismatch_audit(recon, pa)
    pop = base_pop.merge(target, on=["slate_date", "game_id", "player_id"], how="left", suffixes=("", "_contact"))
    pop = pop.merge(recon[["player_game_key", "reconciliation_status"]], on="player_game_key", how="left")
    pop["confirmatory_contact_eval"] = pop["reconciliation_status"].eq("PASS")
    pop, hitter_profiles, pitcher_profiles, bullpen_profiles = build_profiles(pop, pa)
    pop = apply_contact_instruments(pop)
    count_validation = build_count_validation(pop)
    one_two = build_one_to_two(pop)
    oracle_gap = build_oracle_gap(one_two)
    suppress = suppression(pop)
    hitter_interp = hitter_owned(pop)
    roster = roster_relative(pop)
    source = second_hit_source(pop)
    plus = plus200(pop)
    boot = bootstrap_uncertainty(pop)
    bands = probability_band_progression(pop)
    stability = date_stability(pop)
    conc = concentration(pop)
    slices = contact_slice_summary(pop)
    semantic = semantic_binding(pa)

    hold = one_two[one_two["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_exposure_control", "brier"])
    contact_brier = float(hold.loc["contact_count_challenger", "brier"])
    source_brier = float(hold.loc["source_aware_contact_challenger", "brier"])
    control_auc = float(hold.loc["frozen_exposure_control", "auc"])
    contact_auc = float(hold.loc["contact_count_challenger", "auc"])
    source_auc = float(hold.loc["source_aware_contact_challenger", "auc"])
    oracle_contact_auc = float(hold.loc["oracle_actual_contact_count", "auc"])
    oracle_quality_auc = float(hold.loc["oracle_actual_contact_quality", "auc"])
    suppression_preserved = bool(suppress[suppress["temporal_split"].eq("holdout")]["suppression_preserved"].iloc[0])
    if not suppression_preserved:
        next_decision = "NO HITTER_OWNED CHALLENGER READY"
    elif source_brier < control_brier and source_auc > control_auc:
        next_decision = "PREGAME_CONTACT_COUNT_ADDS_MULTI_HIT_VALUE"
    elif contact_brier < control_brier or source_brier < control_brier:
        next_decision = "CONTACT_COUNT_FORECAST_CALIBRATION_ONLY"
    elif oracle_quality_auc > oracle_contact_auc + 0.03:
        next_decision = "CONTACT_QUALITY_FORECAST_REQUIRED_NEXT"
    elif oracle_contact_auc > control_auc + 0.08:
        next_decision = "ORACLE_CONTACT_COUNT_VALUE_NOT_FORECASTABLE"
    else:
        next_decision = "NO HITTER_OWNED CHALLENGER READY"
    oracle_decision = "CONTACT_QUALITY_DRIVES_ORACLE_VALUE" if oracle_quality_auc > oracle_contact_auc + 0.03 else "CONTACT_COUNT_DRIVES_ORACLE_VALUE"
    decisions = pd.DataFrame([
        {"decision": "MLB_CONTACT_OPPORTUNITY_DEFINITION_DECISION", "value": "FROZEN_TERMINAL_CONTACT_AND_HIT_CAPABLE_CONTACT_DEFINED"},
        {"decision": "MLB_CONTACT_PA_RECONCILIATION_DECISION", "value": "FAIL_CLOSED_TO_9905_PA_RECONCILED_ROWS"},
        {"decision": "MLB_CONTACT_PREGAME_FIELD_READINESS_DECISION", "value": "STRICT_PRIOR_FIELDS_AVAILABLE_WITH_SUPPORT_SHRINKAGE"},
        {"decision": "MLB_CONTACT_COUNT_FORECAST_DECISION", "value": "PREGAME_CONTACT_COUNT_FORECAST_EVALUATED"},
        {"decision": "MLB_HIT_CAPABLE_CONTACT_FORECAST_DECISION", "value": "HIT_CAPABLE_CONTACT_FORECAST_EVALUATED"},
        {"decision": "MLB_CONTACT_SOURCE_AWARE_DECISION", "value": "SOURCE_AWARE_CONTACT_FORECAST_EVALUATED"},
        {"decision": "MLB_CONTACT_ORACLE_GAP_DECISION", "value": oracle_decision},
        {"decision": "MLB_CONTACT_ONE_TO_TWO_PLUS_HOLDOUT_DECISION", "value": next_decision},
        {"decision": "MLB_CONTACT_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_PRESERVED" if suppression_preserved else "SUPPRESSION_NOT_PRESERVED"},
        {"decision": "MLB_CONTACT_HITTER_OWNERSHIP_DECISION", "value": "HITTER_OWNED_CONTACT_SEPARATION_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_CONTACT_ROSTER_RELATIVE_DECISION", "value": "ROSTER_RELATIVE_CONTACT_ORDERING_DIAGNOSTIC"},
        {"decision": "MLB_CONTACT_SECOND_HIT_SOURCE_DECISION", "value": "SECOND_HIT_SOURCE_CONTACT_ERRORS_RETAINED_DIAGNOSTIC"},
        {"decision": "MLB_CONTACT_PLUS200_DECISION", "value": "PLUS200_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"},
        {"decision": "MLB_CONTACT_NEXT_RESEARCH_DECISION", "value": next_decision},
        {"decision": "MLB_CONTACT_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])
    instruments = pd.DataFrame([
        {"instrument": "control", "definition": "prior frozen exposure multi-hit control unchanged"},
        {"instrument": "contact_forecast_a", "definition": "expected PA times strict-prior terminal-contact probability"},
        {"instrument": "contact_forecast_b", "definition": "Poisson count distribution over 0/1/2/3/4+ terminal contacts"},
        {"instrument": "contact_forecast_c", "definition": "expected PA times strict-prior hit-capable-contact probability"},
        {"instrument": "contact_forecast_d", "definition": "starter-facing and bullpen-facing contact hazards combined by predicted exposure"},
        {"instrument": "unified_legitimate_challenger", "definition": "predicted hit-capable contact count times strict-prior hit-on-hit-capable-contact conversion"},
    ])
    outputs = {
        "exact_bip_contact_semantic_binding_2026-07-17.csv": semantic,
        "pa_mismatch_audit_2026-07-17.csv": mismatch,
        "canonical_contact_outcome_ledger_2026-07-17.csv": pa,
        "strict_prior_hitter_contact_profiles_2026-07-17.csv": hitter_profiles,
        "strict_prior_pitcher_contact_profiles_2026-07-17.csv": pitcher_profiles,
        "strict_prior_bullpen_contact_profiles_2026-07-17.csv": bullpen_profiles,
        "profile_support_ledger_2026-07-17.csv": pop[["player_game_key", "hitter_contact_pa_support", "pitcher_contact_pa_support", "hitter_hit_capable_support", "pitcher_hit_capable_support", "hitter_contact_support_class", "pitcher_contact_support_class", "hitter_hit_capable_support_class", "pitcher_hit_capable_support_class"]],
        "frozen_contact_instruments_2026-07-17.csv": instruments,
        "contact_count_forecast_validation_2026-07-17.csv": count_validation,
        "oracle_gap_decomposition_2026-07-17.csv": oracle_gap,
        "one_to_two_plus_results_2026-07-17.csv": one_two,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "frozen_probability_band_progression_2026-07-17.csv": bands,
        "date_stability_2026-07-17.csv": stability,
        "hitter_date_concentration_2026-07-17.csv": conc,
        "contact_slice_summary_2026-07-17.csv": slices,
        "suppression_preservation_2026-07-17.csv": suppress,
        "hitter_owned_interpretation_2026-07-17.csv": hitter_interp,
        "roster_relative_results_2026-07-17.csv": roster,
        "second_hit_source_results_2026-07-17.csv": source,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "next_branch_decision_2026-07-17.csv": decisions[decisions["decision"].eq("MLB_CONTACT_NEXT_RESEARCH_DECISION")],
        "required_decisions_2026-07-17.csv": decisions,
        "research_only_model_artifacts_2026-07-17.csv": pop,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    manifest = []
    for p in [POP_PATH, PA_LEDGER_PATH, PA_RECON_PATH, PA_METRICS_PATH, LONG_PRICE]:
        manifest.append({"artifact_role": "input", "path": rel(p), "sha256": sha256(p)})
    for p in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(p), "sha256": sha256(p)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    machine = {
        "generated_at_utc": now_utc(),
        "confirmatory_rows": int(pop["confirmatory_contact_eval"].sum()),
        "pa_mismatch_rows": int(len(mismatch)),
        "holdout_control_brier": control_brier,
        "holdout_contact_challenger_brier": contact_brier,
        "holdout_source_aware_contact_brier": source_brier,
        "holdout_control_auc": control_auc,
        "holdout_contact_challenger_auc": contact_auc,
        "holdout_source_aware_contact_auc": source_auc,
        "holdout_oracle_contact_auc": oracle_contact_auc,
        "holdout_oracle_quality_auc": oracle_quality_auc,
        "next_decision": next_decision,
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_contact_opportunity_pilot_2026-07-17.json")
    direct = "No. The legitimate pregame contact-count forecast did not recover the oracle BIP/contact-quality advantage; the large oracle edge is mostly information that becomes visible only after game contact volume and quality are known." if next_decision != "PREGAME_CONTACT_COUNT_ADDS_MULTI_HIT_VALUE" else "Yes. The legitimate pregame contact-count forecast improved holdout multi-hit ranking, but it remains research-only."
    write_md(f"""# MLB Pregame Contact-Opportunity Count and Multi-Hit Oracle-Gap Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The pilot froze terminal-contact and hit-capable-contact outcomes, audited the 145 PA-denominator mismatch rows, and failed closed to `{machine['confirmatory_rows']}` PA-reconciled rows for confirmatory evaluation.

Holdout one-hit versus two-plus:

| instrument | brier | auc |
|---|---:|---:|
| frozen exposure control | {control_brier:.6f} | {control_auc:.6f} |
| contact-count challenger | {contact_brier:.6f} | {contact_auc:.6f} |
| source-aware contact challenger | {source_brier:.6f} | {source_auc:.6f} |
| oracle actual contact count | {float(hold.loc['oracle_actual_contact_count', 'brier']):.6f} | {oracle_contact_auc:.6f} |
| oracle contact quality | {float(hold.loc['oracle_actual_contact_quality', 'brier']):.6f} | {oracle_quality_auc:.6f} |

## Direct Answer

{direct}

## Production Status

`MLB_CONTACT_PRODUCTION_STATUS = NOT_AUTHORIZED`

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
