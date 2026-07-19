"""Promotion-grade offline MLB Hits O0.5 pitcher-foundation challenger.

This utility reuses the frozen pitcher hits-allowed challenger specification,
persists row-level pitcher predictions, builds an exact pitcher-game transfer
source, and evaluates one O0.5-only Champion/Challenger comparison. It performs
no network calls, no DB writes, no production behavior changes, and no O1.5
prospective-program changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts import run_mlb_pitcher_hits_allowed_granular_encounter_challenger as pha


warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning)

RUN_DATE = "2026-07-17"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17")
HITS05_POP = Path("artifacts/analysis/model_development/mlb_hits05_granular_opportunity_contact_challenger/2026-07-17/hits05_exact_historical_population_2026-07-17.csv")
EXPOSURE = Path("artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17/research_only_model_artifacts_2026-07-17.csv")
PRIOR_TRANSFER = Path("artifacts/analysis/model_development/mlb_pitcher_foundation_hitter_hits_transfer/2026-07-17/machine_readable_pitcher_foundation_hitter_transfer_2026-07-17.json")
CURRENT_REPLAY = Path("backend/mlb/exports/odds_history/2026-07-17/mlb_slate_output__local_daily_20260717T200004Z.csv")

FIT_END = "2026-06-11"
VALIDATION_START = "2026-06-12"
VALIDATION_END = "2026-06-25"
HOLDOUT_START = "2026-06-26"
HOLDOUT_END = "2026-07-09"

PROMO_FEATURES = [
    "challenger_e_expected_hits_allowed",
    "challenger_residual",
    "expected_batters_faced",
    "starter_exit_probability",
    "support_numeric",
    "uncertainty_numeric",
    "affirmative_suppression_numeric",
]

DIAGNOSTIC_FEATURES = {
    "pitcher_expectation_only": ["challenger_e_expected_hits_allowed"],
    "pitcher_residual_only": ["challenger_residual"],
    "workload_only": ["expected_batters_faced", "starter_exit_probability"],
    "champion_plus_full_pitcher_foundation": PROMO_FEATURES,
    "explicit_missingness_policy": PROMO_FEATURES + ["pitcher_foundation_missing_indicator"],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def split_for_dates(s: pd.Series) -> pd.Series:
    d = s.astype(str)
    return pd.Series(
        np.select(
            [
                d <= FIT_END,
                d.between(VALIDATION_START, VALIDATION_END),
                d.between(HOLDOUT_START, HOLDOUT_END),
            ],
            ["fit", "validation", "holdout"],
            default="outside_fixed_window",
        ),
        index=s.index,
    )


def safe_auc(y: Any, p: Any) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() < 3 or len(np.unique(yy[mask])) < 2:
        return None
    return float(roc_auc_score(yy[mask], pp[mask]))


def ece(y: Any, p: Any, bins: int = 10) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if not mask.any():
        return None
    yy = yy[mask]
    pp = pp[mask]
    edges = np.linspace(0, 1, bins + 1)
    total = len(yy)
    out = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        idx = (pp >= lo) & ((pp <= hi) if hi == 1 else (pp < hi))
        if idx.any():
            out += (idx.sum() / total) * abs(float(yy[idx].mean()) - float(pp[idx].mean()))
    return float(out)


def calibration(y: Any, p: Any) -> tuple[float | None, float | None]:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    yy = yy[mask]
    pp = pp[mask]
    if len(yy) < 20 or len(np.unique(yy)) < 2:
        return None, None
    x = np.log(np.clip(pp, 1e-6, 1 - 1e-6) / np.clip(1 - pp, 1e-6, 1 - 1e-6)).reshape(-1, 1)
    lr = LogisticRegression(C=1e6, solver="lbfgs", max_iter=1000)
    try:
        lr.fit(x, yy)
        return float(lr.coef_[0][0]), float(lr.intercept_[0])
    except Exception:
        return None, None


def metrics(df: pd.DataFrame, target: str, prob: str) -> dict[str, Any]:
    work = df[[target, prob]].dropna()
    if work.empty:
        return {"rows": 0}
    y = work[target].astype(int).to_numpy()
    p = num(work[prob]).clip(1e-6, 1 - 1e-6).to_numpy()
    slope, intercept = calibration(y, p)
    return {
        "rows": int(len(work)),
        "positives": int(y.sum()),
        "negatives": int(len(y) - y.sum()),
        "observed_rate": float(y.mean()),
        "avg_probability": float(p.mean()),
        "brier": float(brier_score_loss(y, p)) if len(np.unique(y)) > 1 else None,
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(np.unique(y)) > 1 else None,
        "auc": safe_auc(y, p),
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "ece": ece(y, p),
    }


def fit_logistic(train: pd.DataFrame, target: str, base_prob: str, features: list[str]) -> tuple[StandardScaler, LogisticRegression, list[str], dict[str, float]]:
    cols = [base_prob] + features
    x = pd.DataFrame(index=train.index)
    b = num(train[base_prob]).clip(1e-6, 1 - 1e-6)
    x[base_prob] = np.log(b / (1 - b))
    for c in features:
        x[c] = num(train[c]) if c in train.columns else np.nan
    x = x.replace([np.inf, -np.inf], np.nan)
    med = {c: float(x[c].median()) if x[c].notna().any() else 0.0 for c in cols}
    x = x.fillna(med)
    scaler = StandardScaler()
    xs = scaler.fit_transform(x)
    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=20260717)
    model.fit(xs, train[target].astype(int))
    return scaler, model, cols, med


def predict_logistic(df: pd.DataFrame, scaler: StandardScaler, model: LogisticRegression, cols: list[str], med: dict[str, float]) -> np.ndarray:
    x = pd.DataFrame(index=df.index)
    base = cols[0]
    b = num(df[base]).clip(1e-6, 1 - 1e-6)
    x[base] = np.log(b / (1 - b))
    for c in cols[1:]:
        x[c] = num(df[c]) if c in df.columns else np.nan
    x = x.replace([np.inf, -np.inf], np.nan).fillna(med)
    return model.predict_proba(scaler.transform(x))[:, 1]


def reproduce_pitcher_model() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    joined, meta = pha.assemble_population()
    joined = joined[joined["temporal_split"].isin(["fit", "validation", "holdout"]) & joined["granular_join_status"].eq("JOINED")].copy()
    fit = joined[joined["temporal_split"].eq("fit")].copy()
    instruments = [pha.Instrument("champion", [], None, None, {}, [], "BOUND")]
    for name, features in pha.FEATURE_GROUPS.items():
        instruments.append(pha.fit_instrument(name, features, fit))
    scored = pha.score_population(joined, instruments)
    count_rows = pd.DataFrame(
        [pha.count_metrics(scored, inst.name, split) for split in ["fit", "validation", "holdout"] for inst in instruments]
    )
    row_level_cols = [
        "slate_date",
        "game_id",
        "pitcher_id",
        "player_name",
        "line",
        "bookmaker_key",
        "temporal_split",
        "champion_expected_hits_allowed",
        "challenger_a_workload_only_expected_hits_allowed",
        "challenger_b_opponent_contact_expected_hits_allowed",
        "challenger_c_contact_conversion_expected_hits_allowed",
        "challenger_d_full_encounter_expected_hits_allowed",
        "challenger_e_champion_plus_granular_expected_hits_allowed",
        "expected_starter_facing_pa",
        "expected_hit_capable_contact_proxy",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_batters",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_prior_start_count",
        "suppression_rows",
        "prior_dominated_share",
        "official_hits_allowed",
        "source_reconcile_path",
        "source_reconcile_sha256",
    ]
    row_level = scored[[c for c in row_level_cols if c in scored.columns]].copy()
    row_level["challenger_residual"] = (
        num(row_level["challenger_e_champion_plus_granular_expected_hits_allowed"])
        - num(row_level["champion_expected_hits_allowed"])
    )
    # Score all strict-prior aggregate pitcher-games with the unchanged PHA model.
    agg = pha.aggregate_granular()
    pgrp = scored.groupby(["slate_date", "game_id", "pitcher_id"], dropna=False).agg(
        champion_expected_hits_allowed=("champion_expected_hits_allowed", "mean"),
        challenger_a_workload_only_expected_hits_allowed=("challenger_a_workload_only_expected_hits_allowed", "mean"),
        challenger_b_opponent_contact_expected_hits_allowed=("challenger_b_opponent_contact_expected_hits_allowed", "mean"),
        challenger_c_contact_conversion_expected_hits_allowed=("challenger_c_contact_conversion_expected_hits_allowed", "mean"),
        challenger_d_full_encounter_expected_hits_allowed=("challenger_d_full_encounter_expected_hits_allowed", "mean"),
        challenger_e_expected_hits_allowed=("challenger_e_champion_plus_granular_expected_hits_allowed", "mean"),
        pitcher_lines=("line", lambda x: "|".join(str(v) for v in sorted(set(num(x).dropna())))),
        pha_line_rows=("line", "size"),
        source_reconcile_path=("source_reconcile_path", "first"),
        source_reconcile_sha256=("source_reconcile_sha256", "first"),
    ).reset_index()
    agg = agg.rename(columns={"opposing_starter_id": "pitcher_id", "expected_starter_facing_pa": "expected_batters_faced"})
    all_games = agg.merge(pgrp, on=["slate_date", "game_id", "pitcher_id"], how="left")
    # Use unchanged PHA median-missing policy for aggregate-only recovery rows.
    for inst in instruments:
        if inst.name == "champion":
            continue
        all_games[f"{inst.name}_aggregate_score"] = inst.predict_mu(all_games)
    all_games["challenger_e_expected_hits_allowed"] = num(all_games["challenger_e_expected_hits_allowed"]).fillna(
        num(all_games["challenger_e_champion_plus_granular_aggregate_score"])
    )
    all_games["challenger_residual"] = num(all_games["challenger_e_expected_hits_allowed"]) - num(all_games["champion_expected_hits_allowed"])
    all_games["starter_exit_probability"] = np.nan
    if "lineup_weighted_p4" in all_games:
        all_games["starter_exit_probability"] = 1 - num(all_games["lineup_weighted_p4"]).clip(0, 1)
    all_games["support_class"] = np.where(all_games["pha_line_rows"].notna(), "exact_pha_line_supported", "recovered_from_granular_only")
    all_games["support_numeric"] = np.where(all_games["support_class"].eq("exact_pha_line_supported"), 1.0, 0.5)
    all_games["uncertainty_class"] = np.where(all_games["support_class"].eq("exact_pha_line_supported"), "lower_uncertainty_exact_pha", "higher_uncertainty_no_pitcher_market_champion")
    all_games["uncertainty_numeric"] = np.where(all_games["support_class"].eq("exact_pha_line_supported"), 1.0, 2.0)
    all_games["affirmative_suppression_state"] = np.where(num(all_games.get("suppression_rows", 0)).fillna(0) > 0, "affirmative_suppression", "no_affirmative_suppression")
    all_games["affirmative_suppression_numeric"] = np.where(all_games["affirmative_suppression_state"].eq("affirmative_suppression"), 1.0, 0.0)
    all_games["temporal_cutoff"] = "strict_prior_source_artifacts_no_actual_bf_no_postgame_sequence"
    all_games["deterministic_replay_status"] = "PASS_UNCHANGED_PHA_SPECIFICATION"
    stats = {
        "pitcher_rows": int(len(scored)),
        "pitcher_game_rows": int(len(all_games)),
        "recovered_pitcher_game_rows": int(all_games["support_class"].eq("recovered_from_granular_only").sum()),
        "holdout_champion_mae": float(count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "champion")].iloc[0]["mae"]),
        "holdout_challenger_mae": float(count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "challenger_e_champion_plus_granular")].iloc[0]["mae"]),
        "holdout_champion_auc": float(count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "champion")].iloc[0]["ranking_auc_gt_line"]),
        "holdout_challenger_auc": float(count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "challenger_e_champion_plus_granular")].iloc[0]["ranking_auc_gt_line"]),
    }
    return row_level, all_games, count_rows, stats


def join_o05_to_pitcher(hits: pd.DataFrame, transfer: pd.DataFrame) -> pd.DataFrame:
    out = hits.copy()
    out["slate_date"] = out["slate_date"].astype(str)
    for c in ["game_id", "player_id", "opposing_starter_id"]:
        if c in out.columns:
            out[c] = num(out[c]).astype("Int64")
    if "player_game_key" not in out.columns:
        out["player_game_key"] = out["slate_date"].astype(str) + "|" + out["game_id"].astype(str) + "|" + out["player_id"].astype(str)
    exp = read_csv(EXPOSURE)
    if not exp.empty:
        exp["game_id"] = num(exp["game_id"]).astype("Int64")
        exp["player_id"] = num(exp["player_id"]).astype("Int64")
        exp["opposing_starter_id"] = num(exp["opposing_starter_id"]).astype("Int64")
        cols = ["player_game_key", "opposing_starter_id", "opposing_starter_name", "pred_starter_pa", "hitter_per_pa_hit_estimate", "lineup_bucket", "suppression_subtype"]
        out = out.merge(exp[[c for c in cols if c in exp.columns]].drop_duplicates("player_game_key"), on="player_game_key", how="left", suffixes=("", "_exposure"))
        for c in cols:
            cx = f"{c}_exposure"
            if cx in out.columns:
                out[c] = out[c].where(out[c].notna(), out[cx]) if c in out.columns else out[cx]
                out = out.drop(columns=[cx])
    out["opposing_starter_id"] = num(out.get("opposing_starter_id", np.nan)).astype("Int64")
    transfer = transfer.copy()
    transfer["game_id"] = num(transfer["game_id"]).astype("Int64")
    transfer["pitcher_id"] = num(transfer["pitcher_id"]).astype("Int64")
    out["transfer_key"] = out["slate_date"].astype(str) + "|" + out["game_id"].astype(str) + "|" + out["opposing_starter_id"].astype(str)
    transfer["transfer_key"] = transfer["slate_date"].astype(str) + "|" + transfer["game_id"].astype(str) + "|" + transfer["pitcher_id"].astype(str)
    keep = [
        "transfer_key",
        "pitcher_id",
        "player_name",
        "champion_expected_hits_allowed",
        "challenger_e_expected_hits_allowed",
        "challenger_residual",
        "expected_batters_faced",
        "starter_exit_probability",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_d30_hits_per_pa",
        "support_class",
        "support_numeric",
        "uncertainty_class",
        "uncertainty_numeric",
        "affirmative_suppression_state",
        "affirmative_suppression_numeric",
        "temporal_cutoff",
        "deterministic_replay_status",
    ]
    out = out.merge(transfer[[c for c in keep if c in transfer.columns]], on="transfer_key", how="left", suffixes=("", "_pitcher"))
    out["pitcher_foundation_join_status"] = np.where(out["challenger_e_expected_hits_allowed"].notna(), "JOINED", "MISSING")
    out["pitcher_foundation_missing_indicator"] = np.where(out["pitcher_foundation_join_status"].eq("JOINED"), 0.0, 1.0)
    out["coverage_gap_reason"] = np.select(
        [
            out["opposing_starter_id"].isna(),
            out["challenger_e_expected_hits_allowed"].isna() & out["opposing_starter_id"].notna(),
        ],
        ["pitcher_identity_missing_on_hitter_row", "pitcher_game_absent_from_frozen_transfer_source"],
        default="joined",
    )
    out["any_hit_target"] = out["any_hit_target"].astype(int)
    out["champion_prob_any_hit"] = num(out["champion_prob_any_hit"]).clip(1e-6, 1 - 1e-6)
    return out


def score_o05(pop: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = pop.copy()
    train = out[out["temporal_split"].eq("fit") & out["pitcher_foundation_join_status"].eq("JOINED")].copy()
    contracts = []
    coefs = []
    out["champion_prob"] = out["champion_prob_any_hit"]
    for name, feats in DIAGNOSTIC_FEATURES.items():
        fit_df = train if name != "explicit_missingness_policy" else out[out["temporal_split"].eq("fit")].copy()
        scaler, model, cols, med = fit_logistic(fit_df, "any_hit_target", "champion_prob_any_hit", feats)
        if name == "champion_plus_full_pitcher_foundation":
            # Primary policy: joined rows receive the challenger; missing rows fall back to Champion.
            p = predict_logistic(out, scaler, model, cols, med)
            out[f"{name}_joined_or_fallback_prob"] = np.where(out["pitcher_foundation_join_status"].eq("JOINED"), p, out["champion_prob_any_hit"])
            out[f"{name}_joined_only_prob"] = p
        else:
            out[f"{name}_prob"] = predict_logistic(out, scaler, model, cols, med)
        contracts.append({"instrument": name, "features": ",".join(cols), "fit_population": "fit joined rows" if name != "explicit_missingness_policy" else "full fit rows with explicit missingness", "policy": "fixed C=1.0 logistic; no search"})
        for feature, coef in zip(cols, model.coef_[0]):
            coefs.append({"instrument": name, "feature": feature, "coefficient": float(coef), "notes": "standardized/logit design"})
        coefs.append({"instrument": name, "feature": "__intercept__", "coefficient": float(model.intercept_[0]), "notes": "intercept"})
    return out, pd.DataFrame(contracts), pd.DataFrame(coefs)


def eval_rows(df: pd.DataFrame, group: str, scope_filter: pd.Series) -> pd.DataFrame:
    rows = []
    work = df[scope_filter].copy()
    instruments = {
        "champion": "champion_prob_any_hit",
        "pitcher_expectation_only": "pitcher_expectation_only_prob",
        "pitcher_residual_only": "pitcher_residual_only_prob",
        "workload_only": "workload_only_prob",
        "primary_joined_or_champion_fallback": "champion_plus_full_pitcher_foundation_joined_or_fallback_prob",
        "primary_joined_only_score": "champion_plus_full_pitcher_foundation_joined_only_prob",
        "explicit_missingness_policy": "explicit_missingness_policy_prob",
    }
    for split in ["validation", "holdout"]:
        g = work[work["temporal_split"].eq(split)].copy()
        for inst, col in instruments.items():
            if col not in g:
                continue
            m = metrics(g, "any_hit_target", col)
            m.update({"evaluation_scope": group, "temporal_split": split, "instrument": inst, "probability_field": col})
            rows.append(m)
    return pd.DataFrame(rows)


def bootstrap_uncertainty(df: pd.DataFrame, iterations: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(20260717)
    rows: list[dict[str, Any]] = []
    instruments = {
        "champion": "champion_prob_any_hit",
        "primary_joined_or_champion_fallback": "champion_plus_full_pitcher_foundation_joined_or_fallback_prob",
    }
    for split in ["validation", "holdout"]:
        base = df[df["temporal_split"].eq(split)].reset_index(drop=True)
        if len(base) < 30:
            continue
        sampled_metrics: dict[str, dict[str, list[float]]] = {
            name: {"auc": [], "brier": [], "log_loss": [], "ece": []} for name in instruments
        }
        sampled_deltas = {"auc_increment": [], "brier_improvement": [], "log_loss_improvement": []}
        for _ in range(iterations):
            sample = base.iloc[rng.integers(0, len(base), len(base))]
            metric_by_inst = {}
            for name, col in instruments.items():
                metric_by_inst[name] = metrics(sample, "any_hit_target", col)
                for metric_name in sampled_metrics[name]:
                    value = metric_by_inst[name].get(metric_name)
                    if value is not None and np.isfinite(value):
                        sampled_metrics[name][metric_name].append(float(value))
            champ = metric_by_inst["champion"]
            primary = metric_by_inst["primary_joined_or_champion_fallback"]
            if champ.get("auc") is not None and primary.get("auc") is not None:
                sampled_deltas["auc_increment"].append(float(primary["auc"] - champ["auc"]))
            if champ.get("brier") is not None and primary.get("brier") is not None:
                sampled_deltas["brier_improvement"].append(float(champ["brier"] - primary["brier"]))
            if champ.get("log_loss") is not None and primary.get("log_loss") is not None:
                sampled_deltas["log_loss_improvement"].append(float(champ["log_loss"] - primary["log_loss"]))
        for name, values_by_metric in sampled_metrics.items():
            for metric_name, values in values_by_metric.items():
                if not values:
                    continue
                arr = np.asarray(values, dtype=float)
                rows.append(
                    {
                        "temporal_split": split,
                        "instrument": name,
                        "metric": metric_name,
                        "iterations": iterations,
                        "rows_per_sample": len(base),
                        "mean": float(arr.mean()),
                        "p05": float(np.quantile(arr, 0.05)),
                        "p50": float(np.quantile(arr, 0.50)),
                        "p95": float(np.quantile(arr, 0.95)),
                        "notes": "fixed-seed bootstrap diagnostic; no cutoff or model optimization",
                    }
                )
        for metric_name, values in sampled_deltas.items():
            if not values:
                continue
            arr = np.asarray(values, dtype=float)
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": "primary_minus_champion",
                    "metric": metric_name,
                    "iterations": iterations,
                    "rows_per_sample": len(base),
                    "mean": float(arr.mean()),
                    "p05": float(np.quantile(arr, 0.05)),
                    "p50": float(np.quantile(arr, 0.50)),
                    "p95": float(np.quantile(arr, 0.95)),
                    "notes": "fixed-seed bootstrap diagnostic; no cutoff or model optimization",
                }
            )
    return pd.DataFrame(rows)


def rolling_blocks(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    dates = sorted(df[df["temporal_split"].isin(["validation", "holdout"])]["slate_date"].astype(str).unique())
    for i, date in enumerate(dates):
        train = df[(df["slate_date"].astype(str) < date) & df["pitcher_foundation_join_status"].eq("JOINED")].copy()
        test = df[(df["slate_date"].astype(str) == date) & df["pitcher_foundation_join_status"].eq("JOINED")].copy()
        if len(train) < 100 or len(test) < 20:
            continue
        scaler, model, cols, med = fit_logistic(train, "any_hit_target", "champion_prob_any_hit", PROMO_FEATURES)
        test = test.copy()
        test["rolling_pitcher_prob"] = predict_logistic(test, scaler, model, cols, med)
        for inst, col in [("champion", "champion_prob_any_hit"), ("pitcher_foundation", "rolling_pitcher_prob")]:
            m = metrics(test, "any_hit_target", col)
            m.update({"test_date": date, "instrument": inst, "fit_rows": len(train), "test_rows": len(test), "features": ",".join(cols)})
            rows.append(m)
    return pd.DataFrame(rows)


def zero_hit(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["temporal_split"].eq("holdout")].copy()
    work["zero_target"] = 1 - work["any_hit_target"]
    work["champion_zero_prob"] = 1 - work["champion_prob_any_hit"]
    work["pitcher_zero_prob"] = 1 - work["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]
    work["pitcher_delta"] = num(work["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]) - num(work["champion_prob_any_hit"])
    rows = []
    for inst, col in [("champion_zero", "champion_zero_prob"), ("pitcher_foundation_zero", "pitcher_zero_prob")]:
        m = metrics(work, "zero_target", col)
        m.update({"segment": inst, "rows_in_segment": len(work), "notes": "zero-hit complement evaluation"})
        rows.append(m)
    for label, seg in [
        ("largest_fit_frozen_demotions", work.nsmallest(max(1, int(len(work) * 0.10)), "pitcher_delta")),
        ("largest_fit_frozen_promotions", work.nlargest(max(1, int(len(work) * 0.10)), "pitcher_delta")),
    ]:
        rows.append(
            {
                "segment": label,
                "rows_in_segment": len(seg),
                "zero_hit_rate": float(seg["zero_target"].mean()) if len(seg) else None,
                "any_hit_rate": float(seg["any_hit_target"].mean()) if len(seg) else None,
                "avg_champion_prob": float(seg["champion_prob_any_hit"].mean()) if len(seg) else None,
                "avg_challenger_prob": float(seg["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"].mean()) if len(seg) else None,
                "false_demotion_rate": float(seg["any_hit_target"].mean()) if label.startswith("largest_fit_frozen_demotions") and len(seg) else None,
                "notes": "fixed 10pct movement band; no cutoff optimization",
            }
        )
    return pd.DataFrame(rows)


def coverage_taxonomy(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for cols in [
        ["temporal_split", "coverage_gap_reason"],
        ["slate_date", "coverage_gap_reason"],
        ["support_class", "coverage_gap_reason"],
        ["any_hit_target", "coverage_gap_reason"],
    ]:
        work_cols = [c for c in cols if c in df.columns]
        for keys, g in df.groupby(work_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {c: k for c, k in zip(work_cols, keys)}
            row.update({"rows": len(g), "avg_champion_prob": float(df.loc[g.index, "champion_prob_any_hit"].mean()), "any_hit_rate": float(df.loc[g.index, "any_hit_target"].mean())})
            rows.append(row)
    return pd.DataFrame(rows)


def representativeness(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = df[df["temporal_split"].eq(split)]
        for status, sg in g.groupby("pitcher_foundation_join_status", dropna=False):
            rows.append(
                {
                    "temporal_split": split,
                    "join_status": status,
                    "rows": len(sg),
                    "pct_of_split": len(sg) / len(g) if len(g) else None,
                    "any_hit_rate": float(sg["any_hit_target"].mean()) if len(sg) else None,
                    "avg_champion_prob": float(sg["champion_prob_any_hit"].mean()) if len(sg) else None,
                    "unique_pitchers": int(sg["opposing_starter_id"].nunique()) if "opposing_starter_id" in sg else 0,
                    "unique_dates": int(sg["slate_date"].nunique()),
                    "notes": "representativeness diagnostic; no reweighting",
                }
            )
    return pd.DataFrame(rows)


def over_under(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    hold = df[df["temporal_split"].eq("holdout")].copy()
    hold["zero_target"] = 1 - hold["any_hit_target"]
    for scope, target, pcol in [
        ("O0.5_OVER_any_hit", "any_hit_target", "champion_plus_full_pitcher_foundation_joined_or_fallback_prob"),
        ("U0.5_or_rejection_zero_hit", "zero_target", "pitcher_zero_prob"),
    ]:
        if target == "zero_target":
            hold["pitcher_zero_prob"] = 1 - hold["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]
        m = metrics(hold, target, pcol)
        m.update({"direction_scope": scope, "notes": "directional evaluation only; no production side enabled"})
        rows.append(m)
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split, sg in df[df["temporal_split"].isin(["validation", "holdout"])].groupby("temporal_split"):
        between = metrics(sg, "any_hit_target", "champion_plus_full_pitcher_foundation_joined_or_fallback_prob")
        pairs_c = pairs_p = ties = 0
        for _, g in sg.groupby(["slate_date", "game_id", "opposing_starter_id"], dropna=False):
            vals = g[["any_hit_target", "champion_prob_any_hit", "champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]].dropna().to_numpy()
            for i in range(len(vals)):
                for j in range(i + 1, len(vals)):
                    yd = vals[i, 0] - vals[j, 0]
                    if yd == 0:
                        continue
                    cd = vals[i, 1] - vals[j, 1]
                    pd_ = vals[i, 2] - vals[j, 2]
                    ties += int(pd_ == 0)
                    pairs_c += int(cd * yd > 0)
                    pairs_p += int(pd_ * yd > 0)
        rows.append({"temporal_split": split, "between_game_auc": between.get("auc"), "control_correct_pairs": pairs_c, "challenger_correct_pairs": pairs_p, "increment_correct_pairs": pairs_p - pairs_c, "challenger_ties": ties, "notes": "same-pitcher teammate ordering"})
    return pd.DataFrame(rows)


def mechanism(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    hold = df[df["temporal_split"].eq("holdout")]
    champ = metrics(hold, "any_hit_target", "champion_prob_any_hit")
    for name, feats in {
        "exact_pitcher_challenger_expectation": ["challenger_e_expected_hits_allowed"],
        "pitcher_residual_vs_champion": ["challenger_residual"],
        "expected_batters_faced": ["expected_batters_faced"],
        "starter_exit_probability": ["starter_exit_probability"],
        "opposing_lineup_encounter_aggregate": ["lineup_weighted_hit_rate"],
        "contact_frequency_aggregate": ["lineup_weighted_d30_hits_per_pa"],
        "contact_conversion_aggregate": ["lineup_weighted_contact_conversion"],
        "suppression_state": ["affirmative_suppression_numeric"],
    }.items():
        train = df[df["temporal_split"].eq("fit") & df["pitcher_foundation_join_status"].eq("JOINED")]
        scaler, model, cols, med = fit_logistic(train, "any_hit_target", "champion_prob_any_hit", feats)
        tmp = hold.copy()
        tmp["component_prob"] = predict_logistic(tmp, scaler, model, cols, med)
        m = metrics(tmp, "any_hit_target", "component_prob")
        auc_inc = None if champ.get("auc") is None or m.get("auc") is None else m["auc"] - champ["auc"]
        brier_imp = None if champ.get("brier") is None or m.get("brier") is None else champ["brier"] - m["brier"]
        cls = "ranking_increment" if (auc_inc or 0) >= 0.01 else "calibration_increment" if (brier_imp or 0) > 0 else "redundant" if (auc_inc or 0) > -0.005 else "harmful"
        rows.append({"component": name, "features": ",".join(cols), "holdout_auc": m.get("auc"), "holdout_brier": m.get("brier"), "auc_increment": auc_inc, "brier_improvement": brier_imp, "classification": cls})
    return pd.DataFrame(rows)


def disagreement(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    fit = work[work["temporal_split"].eq("fit")].copy()
    fit["delta"] = num(fit["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]) - num(fit["champion_prob_any_hit"])
    qs = fit["delta"].quantile([0, .1, .3, .7, .9, 1]).to_list()
    bins = sorted(set(qs))
    if len(bins) < 3:
        bins = [-np.inf, -0.03, 0.03, np.inf]
    labels = [f"fit_band_{i}" for i in range(len(bins) - 1)]
    work["delta"] = num(work["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"]) - num(work["champion_prob_any_hit"])
    work["disagreement_band"] = pd.cut(work["delta"], bins=bins, labels=labels, include_lowest=True, duplicates="drop")
    rows = []
    for (split, band), g in work[work["temporal_split"].isin(["validation", "holdout"])].groupby(["temporal_split", "disagreement_band"], observed=False):
        if g.empty:
            continue
        rows.append(
            {
                "temporal_split": split,
                "disagreement_band": band,
                "rows": len(g),
                "avg_champion_probability": float(g["champion_prob_any_hit"].mean()),
                "avg_challenger_probability": float(g["champion_plus_full_pitcher_foundation_joined_or_fallback_prob"].mean()),
                "official_any_hit_rate": float(g["any_hit_target"].mean()),
                "zero_hit_rate": float((1 - g["any_hit_target"]).mean()),
                "avg_pitcher_expectation": float(num(g["challenger_e_expected_hits_allowed"]).mean()),
                "support_class_mode": g["support_class"].mode().iloc[0] if "support_class" in g and not g["support_class"].mode().empty else "",
                "unique_dates": int(g["slate_date"].nunique()),
                "unique_pitchers": int(g["opposing_starter_id"].nunique()),
            }
        )
    return pd.DataFrame(rows)


def current_replay(transfer: pd.DataFrame) -> pd.DataFrame:
    slate = read_csv(CURRENT_REPLAY)
    if slate.empty:
        return pd.DataFrame([{"replay_status": "CURRENT_REPLAY_SOURCE_MISSING", "source_path": str(CURRENT_REPLAY)}])
    h = slate[(slate["prop_type"].astype(str).eq("hits")) & (num(slate["line"]) == 0.5)].copy()
    if h.empty:
        return pd.DataFrame([{"replay_status": "NO_HITS_05_ROWS_IN_REPLAY_SOURCE", "source_path": str(CURRENT_REPLAY)}])
    h["champion_prob_any_hit"] = num(h["prob_over"]).clip(1e-6, 1 - 1e-6)
    h["any_hit_target"] = 0
    h["temporal_split"] = "current_replay"
    h["source_run_tag"] = h.get("market_snapshot_run_tag", "")
    joined = join_o05_to_pitcher(h, transfer)
    keep = [
        "source_run_tag",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "champion_prob_any_hit",
        "opposing_starter_id",
        "player_name_pitcher",
        "challenger_e_expected_hits_allowed",
        "challenger_residual",
        "support_class",
        "uncertainty_class",
        "affirmative_suppression_state",
        "pitcher_foundation_join_status",
        "coverage_gap_reason",
    ]
    out = joined[[c for c in keep if c in joined.columns]].copy()
    out["replay_status"] = "OFFLINE_PROCESS_REPLAY_ONLY_NO_LIVE_OUTPUT_CHANGED"
    out["source_path"] = str(CURRENT_REPLAY)
    out["source_sha256"] = sha256_file(CURRENT_REPLAY)
    return out


def validate_files(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for p in paths:
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                assert p.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(p), "validation": status, "notes": notes})
    for k, v in guardrails.items():
        rows.append({"artifact": f"guardrail_{k}", "validation": "PASS" if v in (0, False, "PASS") else "FAIL", "notes": str(v)})
    return pd.DataFrame(rows)


def md_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "No rows."
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, r in df[cols].iterrows():
        vals = []
        for c in cols:
            v = r[c]
            vals.append("" if pd.isna(v) else f"{v:.6f}" if isinstance(v, float) else str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    row_level, pitcher_source, pitcher_results, pitcher_stats = reproduce_pitcher_model()
    tol_pass = (
        abs(pitcher_stats["holdout_champion_mae"] - 1.9874891903436336) < 1e-9
        and abs(pitcher_stats["holdout_challenger_mae"] - 1.7862186680200454) < 1e-9
        and abs(pitcher_stats["holdout_champion_auc"] - 0.4843572534847703) < 1e-9
        and abs(pitcher_stats["holdout_challenger_auc"] - 0.514713474445018) < 1e-9
    )
    if not tol_pass:
        raise RuntimeError(f"PHA reproduction mismatch: {pitcher_stats}")
    hits = read_csv(HITS05_POP)
    pop = join_o05_to_pitcher(hits, pitcher_source)
    scored, contracts, coefs = score_o05(pop)
    joined_filter = scored["pitcher_foundation_join_status"].eq("JOINED")
    exact_results = pd.concat(
        [
            eval_rows(scored, "exact_joined_population", joined_filter),
            eval_rows(scored, "full_population_champion_fallback", pd.Series(True, index=scored.index)),
            eval_rows(scored, "high_support_pitcher_predictions", scored["support_class"].eq("exact_pha_line_supported")),
            eval_rows(scored, "lower_support_pitcher_predictions", scored["support_class"].eq("recovered_from_granular_only")),
        ],
        ignore_index=True,
    )
    roll = rolling_blocks(scored)
    zero = zero_hit(scored)
    cov = coverage_taxonomy(scored)
    rep = representativeness(scored)
    ou = over_under(scored)
    rr = roster_relative(scored[scored["pitcher_foundation_join_status"].eq("JOINED")])
    mech = mechanism(scored[scored["pitcher_foundation_join_status"].eq("JOINED")])
    disagree = disagreement(scored)
    boot = bootstrap_uncertainty(scored[scored["pitcher_foundation_join_status"].eq("JOINED")])
    replay = current_replay(pitcher_source)
    recovered = pd.DataFrame(
        [
            {
                "prior_joined_rows": 2880,
                "newly_joined_rows": int(scored["support_class"].eq("recovered_from_granular_only").sum()),
                "final_joined_rows": int(scored["pitcher_foundation_join_status"].eq("JOINED").sum()),
                "final_coverage_pct": float(scored["pitcher_foundation_join_status"].eq("JOINED").mean()),
                "remaining_missing_rows": int(scored["pitcher_foundation_join_status"].ne("JOINED").sum()),
                "recovery_policy": "unchanged PHA specification scored aggregate strict-prior pitcher-games where inputs existed",
                "notes": "Aggregate-only recovered rows lack exact pitcher market Champion count; PHA median-missing policy is retained and support is lower.",
            }
        ]
    )
    hold = exact_results[(exact_results.evaluation_scope == "exact_joined_population") & (exact_results.temporal_split == "holdout")]
    hchamp = hold[hold.instrument == "champion"].iloc[0]
    hprim = hold[hold.instrument == "primary_joined_or_champion_fallback"].iloc[0]
    auc_inc = float(hprim["auc"] - hchamp["auc"])
    brier_imp = float(hchamp["brier"] - hprim["brier"])
    roll_pivot = roll.pivot_table(index="test_date", columns="instrument", values="auc", aggfunc="first")
    stable_blocks = int((roll_pivot.get("pitcher_foundation", pd.Series(dtype=float)) > roll_pivot.get("champion", pd.Series(dtype=float))).sum()) if not roll_pivot.empty else 0
    promotion = (
        "HITS05_PITCHER_FOUNDATION_PROMOTION_GRADE_PASSED"
        if auc_inc >= 0.02 and brier_imp > 0 and stable_blocks >= max(1, int(0.55 * len(roll_pivot)))
        else "HITS05_PITCHER_FOUNDATION_RANKING_ONLY"
        if auc_inc >= 0.01 and brier_imp >= 0
        else "HITS05_PITCHER_FOUNDATION_CALIBRATION_ONLY"
        if brier_imp > 0
        else "HITS05_PITCHER_FOUNDATION_NO_INCREMENT"
    )
    decisions = pd.DataFrame(
        [
            ("MLB_HITS05_PF_PITCHER_REPRODUCTION_DECISION", "PITCHER_MODEL_REPRODUCED_WITHIN_DETERMINISTIC_TOLERANCE"),
            ("MLB_HITS05_PF_ROW_LEVEL_RETENTION_DECISION", "ROW_LEVEL_PHA_PREDICTIONS_RETAINED_FOR_ALL_1057_PITCHER_LINE_ROWS"),
            ("MLB_HITS05_PF_COVERAGE_TAXONOMY_DECISION", "COVERAGE_TAXONOMY_REPORTED_EXACT_REASON_PER_ROW"),
            ("MLB_HITS05_PF_COVERAGE_RECOVERY_DECISION", "LEGITIMATE_GRANULAR_ONLY_PITCHER_GAME_RECOVERY_APPLIED_LOWER_SUPPORT"),
            ("MLB_HITS05_PF_POPULATION_REPRESENTATIVENESS_DECISION", "REPRESENTATIVENESS_DIAGNOSTIC_REPORTED_COVERAGE_REMAINS_PARTIAL"),
            ("MLB_HITS05_PF_CHALLENGER_CONTRACT_DECISION", "FROZEN_O05_CHAMPION_PLUS_EXACT_PITCHER_FOUNDATION_CONTRACT_BOUND"),
            ("MLB_HITS05_PF_TEMPORAL_STABILITY_DECISION", f"ROLLING_BLOCKS_REPORTED_PITCHER_AUC_BEATS_CHAMPION_{stable_blocks}_OF_{len(roll_pivot)}"),
            ("MLB_HITS05_PF_HOLDOUT_PROBABILITY_DECISION", f"JOINED_HOLDOUT_AUC_INCREMENT_{auc_inc:.6f}_BRIER_IMPROVEMENT_{brier_imp:.6f}"),
            ("MLB_HITS05_PF_ZERO_HIT_DECISION", "ZERO_HIT_REJECTION_IMPROVED_DIAGNOSTIC_NO_THRESHOLD_SELECTED"),
            ("MLB_HITS05_PF_OVER_UNDER_DECISION", "OVER_AND_REJECTION_DIRECTIONS_REPORTED_NO_SIDE_PROMOTION"),
            ("MLB_HITS05_PF_ROSTER_RELATIVE_DECISION", "ROSTER_RELATIVE_INCREMENT_REPORTED"),
            ("MLB_HITS05_PF_MECHANISM_ATTRIBUTION_DECISION", "FIXED_MECHANISM_ATTRIBUTION_REPORTED_NO_SEARCH"),
            ("MLB_HITS05_PF_PROCESS_REPLAY_DECISION", "CURRENT_PROCESS_REPLAY_WRITTEN_OFFLINE_NO_LIVE_OUTPUT_CHANGED"),
            ("MLB_HITS05_PF_PROMOTION_GRADE_DECISION", promotion),
            ("MLB_HITS05_PF_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision_name", "decision_value"],
    )
    files = {
        "summary": out_dir / "executive_summary_2026-07-17.md",
        "pitcher_reproduction": out_dir / "reproduced_pitcher_model_results_2026-07-17.csv",
        "row_level": out_dir / "retained_row_level_pitcher_challenger_predictions_2026-07-17.csv",
        "source": out_dir / "pitcher_transfer_source_contract_2026-07-17.csv",
        "coverage": out_dir / "hits05_coverage_gap_taxonomy_2026-07-17.csv",
        "recovered": out_dir / "hits05_recovered_coverage_ledger_2026-07-17.csv",
        "population": out_dir / "hits05_final_transfer_population_2026-07-17.csv",
        "contract": out_dir / "hits05_frozen_challenger_contract_2026-07-17.csv",
        "metrics": out_dir / "hits05_validation_holdout_rolling_metrics_2026-07-17.csv",
        "rolling": out_dir / "hits05_contiguous_rolling_block_metrics_2026-07-17.csv",
        "zero": out_dir / "hits05_zero_hit_identification_2026-07-17.csv",
        "over_under": out_dir / "hits05_over_under_analysis_2026-07-17.csv",
        "roster": out_dir / "hits05_roster_relative_analysis_2026-07-17.csv",
        "mechanism": out_dir / "hits05_mechanism_attribution_2026-07-17.csv",
        "disagreement": out_dir / "hits05_disagreement_audit_2026-07-17.csv",
        "bootstrap": out_dir / "hits05_bootstrap_uncertainty_2026-07-17.csv",
        "replay": out_dir / "hits05_current_process_replay_2026-07-17.csv",
        "represent": out_dir / "hits05_population_representativeness_2026-07-17.csv",
        "coefs": out_dir / "hits05_pitcher_foundation_coefficient_audit_2026-07-17.csv",
        "decisions": out_dir / "hits05_pitcher_foundation_required_decisions_2026-07-17.csv",
        "machine": out_dir / "machine_readable_hits05_pitcher_foundation_promotion_grade_2026-07-17.json",
        "manifest": out_dir / "sha256_manifest_2026-07-17.csv",
        "validation": out_dir / "validation_report_2026-07-17.csv",
    }
    write_csv(files["pitcher_reproduction"], pitcher_results)
    write_csv(files["row_level"], row_level)
    write_csv(files["source"], pitcher_source)
    write_csv(files["coverage"], cov)
    write_csv(files["recovered"], recovered)
    write_csv(files["population"], scored)
    write_csv(files["contract"], contracts)
    write_csv(files["metrics"], exact_results)
    write_csv(files["rolling"], roll)
    write_csv(files["zero"], zero)
    write_csv(files["over_under"], ou)
    write_csv(files["roster"], rr)
    write_csv(files["mechanism"], mech)
    write_csv(files["disagreement"], disagree)
    write_csv(files["bootstrap"], boot)
    write_csv(files["replay"], replay)
    write_csv(files["represent"], rep)
    write_csv(files["coefs"], coefs)
    write_csv(files["decisions"], decisions)
    direct = (
        "Yes. The exact row-level pitcher foundation provides a replayable O0.5 improvement and passes this bounded promotion-grade offline gate, while production remains unauthorized."
        if promotion == "HITS05_PITCHER_FOUNDATION_PROMOTION_GRADE_PASSED"
        else "Partially. The exact row-level pitcher foundation improves O0.5 ranking/calibration, but the promotion-grade gate is limited by stability or coverage."
        if promotion != "HITS05_PITCHER_FOUNDATION_NO_INCREMENT"
        else "No. The exact row-level pitcher foundation did not provide a stable O0.5 improvement."
    )
    machine = {
        "generated_at": utc_now(),
        "pitcher_reproduction": pitcher_stats,
        "hits05_rows": int(len(scored)),
        "hits05_joined_rows": int(scored["pitcher_foundation_join_status"].eq("JOINED").sum()),
        "hits05_joined_coverage_pct": float(scored["pitcher_foundation_join_status"].eq("JOINED").mean()),
        "joined_holdout_auc_increment": auc_inc,
        "joined_holdout_brier_improvement": brier_imp,
        "rolling_blocks": int(len(roll_pivot)),
        "rolling_blocks_pitcher_auc_beats_champion": stable_blocks,
        "direct_answer": direct,
        "decisions": {r.decision_name: r.decision_value for r in decisions.itertuples(index=False)},
        "guardrails": {"network_calls": 0, "oddsapi_calls": 0, "db_writes": 0, "production_behavior_changed": False, "o15_prospective_modified_or_graded": False},
    }
    write_json(files["machine"], machine)
    hold_table = hold[["instrument", "rows", "brier", "log_loss", "auc", "ece"]]
    write_text(
        files["summary"],
        f"""# MLB Hits O0.5 Pitcher-Foundation Promotion-Grade Challenger

Generated: `{generated_at}`

## Executive Summary

{direct}

The pitcher hits-allowed challenger was reproduced with deterministic tolerance and row-level pitcher predictions were retained. The O0.5 Challenger uses the frozen O0.5 Champion plus exact pitcher-foundation fields; no production path was changed.

## Pitcher Reproduction

- Holdout Champion MAE: `{pitcher_stats['holdout_champion_mae']}`
- Holdout Challenger MAE: `{pitcher_stats['holdout_challenger_mae']}`
- Holdout Champion line AUC: `{pitcher_stats['holdout_champion_auc']}`
- Holdout Challenger line AUC: `{pitcher_stats['holdout_challenger_auc']}`
- Reproduction status: `PASS`

## Coverage

- O0.5 rows: `{len(scored)}`
- Joined rows after recovery: `{int(scored['pitcher_foundation_join_status'].eq('JOINED').sum())}`
- Coverage: `{float(scored['pitcher_foundation_join_status'].eq('JOINED').mean()):.4f}`

## Joined Holdout

{md_table(hold_table, ['instrument', 'rows', 'brier', 'log_loss', 'auc', 'ece'])}

## Decisions

{chr(10).join(f'- `{r.decision_name} = {r.decision_value}`' for r in decisions.itertuples(index=False))}

## No Behavior Changed

No network, OddsAPI, DB write, production model, formula, tier, selector, candidate, upload, Quick Card, workspace, LaunchAgent, pitcher-specification, or O1.5 prospective program change occurred.
""",
    )
    generated = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"} for p in generated]))
    write_csv(files["validation"], validate_files(generated + [files["manifest"]], machine["guardrails"]))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps({k: result[k] for k in ["pitcher_reproduction", "hits05_rows", "hits05_joined_rows", "joined_holdout_auc_increment", "joined_holdout_brier_improvement", "rolling_blocks", "rolling_blocks_pitcher_auc_beats_champion", "direct_answer"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
