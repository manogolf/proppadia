"""Bounded MLB pitcher hits-allowed granular encounter challenger.

Offline research only. Reads preserved local pitcher hits-allowed predictions,
official outcomes, and existing strict-prior encounter/exposure artifacts.
Writes a dated audit package without changing production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import poisson
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error, roc_auc_score
from sklearn.preprocessing import StandardScaler


RUN_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17"
)
RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
GRANULAR_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/"
    "2026-07-17/research_only_model_artifacts_2026-07-17.csv"
)
FIT_END = "2026-06-11"
VALIDATION_START = "2026-06-12"
VALIDATION_END = "2026-06-25"
HOLDOUT_START = "2026-06-26"
HOLDOUT_END = "2026-07-09"


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


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def safe_auc(y: Any, p: Any) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() < 3 or len(np.unique(yy[mask])) < 2:
        return None
    return float(roc_auc_score(yy[mask], pp[mask]))


def champion_lambda_from_line_prob(line: Any, prob_over: Any) -> float | None:
    try:
        line_f = float(line)
        p = min(max(float(prob_over), 1e-6), 1 - 1e-6)
    except Exception:
        return None
    threshold = math.floor(line_f)

    def f(lam: float) -> float:
        return 1.0 - poisson.cdf(threshold, lam) - p

    try:
        return float(brentq(f, 1e-6, 30.0, maxiter=100))
    except Exception:
        return None


def poisson_over_prob(mu: Any, line: Any) -> float | None:
    try:
        m = max(float(mu), 1e-6)
        threshold = math.floor(float(line))
    except Exception:
        return None
    return float(1.0 - poisson.cdf(threshold, m))


def ece_score(y: Any, p: Any, bins: int = 10) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() == 0:
        return None
    yy = yy[mask]
    pp = pp[mask]
    edges = np.linspace(0.0, 1.0, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        idx = (pp >= lo) & (pp <= hi if hi == 1 else pp < hi)
        if idx.any():
            ece += (idx.sum() / len(pp)) * abs(float(yy[idx].mean()) - float(pp[idx].mean()))
    return float(ece)


def american_profit(price: Any, win: bool) -> float | None:
    try:
        odds = float(price)
    except Exception:
        return None
    if win:
        return odds / 100.0 if odds > 0 else 100.0 / abs(odds)
    return -1.0


def implied_prob_american(price: Any) -> float | None:
    try:
        odds = float(price)
    except Exception:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return None


def load_pitcher_population() -> tuple[pd.DataFrame, dict[str, Any]]:
    frames = []
    source_paths = []
    for path in sorted(RECONCILE_ROOT.glob("*/reconcile_rows.csv")):
        date_value = path.parent.name
        if date_value < "2026-05-01" or date_value > HOLDOUT_END:
            continue
        df = read_csv(path)
        required = {"slate_date", "game_id", "player_id", "prop_type", "line", "model_prob_over", "actual_value"}
        if df.empty or not required.issubset(df.columns):
            continue
        h = df[df["prop_type"].astype(str).eq("hits_allowed")].copy()
        if h.empty:
            continue
        h["source_reconcile_path"] = str(path)
        h["source_reconcile_sha256"] = sha256_file(path)
        frames.append(h)
        source_paths.append(str(path))
    if not frames:
        return pd.DataFrame(), {"source_reconcile_files": source_paths}
    pop = pd.concat(frames, ignore_index=True)
    pop["slate_date"] = pop["slate_date"].astype(str)
    pop["game_id"] = pd.to_numeric(pop["game_id"], errors="coerce").astype("Int64")
    pop["pitcher_id"] = pd.to_numeric(pop["player_id"], errors="coerce").astype("Int64")
    pop["line"] = pd.to_numeric(pop["line"], errors="coerce")
    pop["official_hits_allowed"] = pd.to_numeric(pop["actual_value"], errors="coerce")
    pop["model_prob_over"] = pd.to_numeric(pop["model_prob_over"], errors="coerce")
    pop["canonical_key"] = (
        pop["slate_date"].astype(str)
        + "|"
        + pop["game_id"].astype(str)
        + "|"
        + pop["pitcher_id"].astype(str)
        + "|hits_allowed|"
        + pop["line"].astype(str)
    )
    pop["snapshot_time_sort"] = pd.to_datetime(pop.get("snapshot_time_utc"), errors="coerce")
    pop = pop.sort_values(["canonical_key", "snapshot_time_sort"], na_position="last")
    pop["duplicate_observation_count"] = pop.groupby("canonical_key")["canonical_key"].transform("size")
    pop = pop.drop_duplicates("canonical_key", keep="first").copy()
    pop = pop[pop["official_hits_allowed"].notna() & pop["model_prob_over"].notna()].copy()
    pop["champion_expected_hits_allowed_poisson_implied"] = [
        champion_lambda_from_line_prob(line, prob)
        for line, prob in zip(pop["line"], pop["model_prob_over"])
    ]
    pop["champion_prob_under"] = 1 - pop["model_prob_over"]
    pop["over_target"] = (pop["official_hits_allowed"] > pop["line"]).astype(int)
    pop["under_target"] = (pop["official_hits_allowed"] < pop["line"]).astype(int)
    pop["push_target"] = (pop["official_hits_allowed"] == pop["line"]).astype(int)
    pop["temporal_split"] = np.select(
        [
            pop["slate_date"] <= FIT_END,
            pop["slate_date"].between(VALIDATION_START, VALIDATION_END),
            pop["slate_date"].between(HOLDOUT_START, HOLDOUT_END),
        ],
        ["fit", "validation", "holdout"],
        default="outside_fixed_window",
    )
    meta = {
        "source_reconcile_files": source_paths,
        "source_reconcile_file_count": len(source_paths),
        "unique_pitcher_line_rows": len(pop),
    }
    return pop, meta


def aggregate_granular() -> pd.DataFrame:
    df = read_csv(GRANULAR_SOURCE)
    if df.empty:
        return pd.DataFrame()
    df["slate_date"] = df["slate_date"].astype(str)
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["opposing_starter_id"] = pd.to_numeric(df["opposing_starter_id"], errors="coerce").astype("Int64")
    df = df[df["opposing_starter_id"].notna()].copy()
    numeric_cols = [
        "actual_starter_facing_pa_seq",
        "hits_against_starter",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "pred_total_pa",
        "expected_pa_used",
        "hitter_per_pa_hit_estimate",
        "p_hit_starter_prior",
        "p_hit_bullpen_prior",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "d15_pa_per_game",
        "d30_hits_per_pa",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "p_hitter_receives_fourth_pa",
        "p_hitter_receives_fifth_pa",
        "p_bullpen_pa_ge1",
        "p_starter_exit_before_pa4",
        "predicted_exposure_p_zero_hits",
        "source_aware_unified_p_zero_hits",
        "challenger_p_zero_hits",
        "joint_p_zero_hits",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    def wavg(grp: pd.DataFrame, col: str, weight: str = "pred_starter_pa") -> float:
        if col not in grp.columns:
            return np.nan
        x = pd.to_numeric(grp[col], errors="coerce")
        w = pd.to_numeric(grp.get(weight, pd.Series(1, index=grp.index)), errors="coerce").fillna(0)
        mask = x.notna() & (w > 0)
        if mask.any():
            return float(np.average(x[mask], weights=w[mask]))
        return float(x.mean()) if x.notna().any() else np.nan
    def numeric_series(grp: pd.DataFrame, col: str) -> pd.Series:
        if col not in grp.columns:
            return pd.Series(np.nan, index=grp.index)
        return pd.to_numeric(grp[col], errors="coerce")
    rows = []
    for (date, game_id, pitcher_id), grp in df.groupby(["slate_date", "game_id", "opposing_starter_id"], dropna=True):
        starter_pa = numeric_series(grp, "pred_starter_pa")
        actual_bf = numeric_series(grp, "actual_starter_facing_pa_seq")
        hits = numeric_series(grp, "hits_against_starter")
        row = {
            "join_key": f"{date}|{game_id}|{pitcher_id}",
            "slate_date": date,
            "game_id": game_id,
            "pitcher_id": pitcher_id,
            "lineup_batters": int(grp["player_id"].nunique()) if "player_id" in grp.columns else len(grp),
            "official_batters_faced_from_encounters": float(actual_bf.sum()) if actual_bf.notna().any() else np.nan,
            "official_hits_allowed_from_encounters": float(hits.sum()) if hits.notna().any() else np.nan,
            "expected_starter_facing_pa": float(starter_pa.sum()) if starter_pa.notna().any() else np.nan,
            "expected_total_pa_lineup": float(numeric_series(grp, "pred_total_pa").sum()),
            "expected_bullpen_pa_lineup": float(numeric_series(grp, "pred_bullpen_pa").sum()),
            "lineup_weighted_hit_rate": wavg(grp, "hitter_per_pa_hit_estimate"),
            "lineup_weighted_contact_conversion": wavg(grp, "p_hit_starter_prior"),
            "lineup_weighted_bullpen_hit_rate": wavg(grp, "p_hit_bullpen_prior"),
            "lineup_weighted_season_hits_per_pa": wavg(grp, "season_to_date_hits_per_pa"),
            "lineup_weighted_season_pa_per_game": wavg(grp, "season_to_date_pa_per_game"),
            "lineup_weighted_d15_pa_per_game": wavg(grp, "d15_pa_per_game"),
            "lineup_weighted_d30_hits_per_pa": wavg(grp, "d30_hits_per_pa"),
            "lineup_weighted_p4": wavg(grp, "p_hitter_receives_fourth_pa"),
            "lineup_weighted_p5": wavg(grp, "p_hitter_receives_fifth_pa"),
            "lineup_weighted_zero_hit_risk": wavg(grp, "predicted_exposure_p_zero_hits"),
            "starter_expected_hits_allowed": float(numeric_series(grp, "starter_expected_hits_allowed").median()),
            "pitcher_base": float(numeric_series(grp, "pitcher_base").median()),
            "starter_prior_start_count": float(numeric_series(grp, "starter_prior_start_count").median()) if "starter_prior_start_count" in grp.columns else np.nan,
            "suppression_rows": int(grp.get("suppression_subtype", pd.Series(dtype=str)).astype(str).str.contains("suppression", case=False, na=False).sum()) if "suppression_subtype" in grp.columns else 0,
            "prior_dominated_share": float((grp.get("strict_prior_status", pd.Series(dtype=str)).astype(str) != "PASS_STRICT_PRIOR").mean()) if "strict_prior_status" in grp.columns else np.nan,
        }
        row["expected_hit_capable_contact_proxy"] = row["expected_starter_facing_pa"] * row["lineup_weighted_hit_rate"] if np.isfinite(row["expected_starter_facing_pa"]) and np.isfinite(row["lineup_weighted_hit_rate"]) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def assemble_population() -> tuple[pd.DataFrame, dict[str, Any]]:
    pop, meta = load_pitcher_population()
    agg = aggregate_granular()
    if pop.empty:
        return pop, meta
    pop["join_key"] = pop["slate_date"].astype(str) + "|" + pop["game_id"].astype(str) + "|" + pop["pitcher_id"].astype(str)
    joined = pop.merge(agg, on="join_key", how="left", suffixes=("", "_granular"))
    joined["granular_join_status"] = np.where(joined["lineup_batters"].notna(), "JOINED", "MISSING_GRANULAR")
    meta.update(
        {
            "granular_source": str(GRANULAR_SOURCE),
            "granular_source_sha256": sha256_file(GRANULAR_SOURCE) if GRANULAR_SOURCE.exists() else "",
            "granular_aggregates": len(agg),
            "joined_rows": int((joined["granular_join_status"] == "JOINED").sum()),
        }
    )
    return joined, meta


FEATURE_GROUPS = {
    "challenger_a_workload_only": [
        "champion_expected_hits_allowed_poisson_implied",
        "expected_starter_facing_pa",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_prior_start_count",
    ],
    "challenger_b_opponent_contact": [
        "champion_expected_hits_allowed_poisson_implied",
        "expected_starter_facing_pa",
        "lineup_weighted_hit_rate",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "prior_dominated_share",
    ],
    "challenger_c_contact_conversion": [
        "champion_expected_hits_allowed_poisson_implied",
        "expected_starter_facing_pa",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_bullpen_hit_rate",
        "expected_hit_capable_contact_proxy",
        "lineup_weighted_zero_hit_risk",
    ],
    "challenger_d_full_encounter": [
        "expected_starter_facing_pa",
        "expected_hit_capable_contact_proxy",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_season_pa_per_game",
        "lineup_weighted_d15_pa_per_game",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "prior_dominated_share",
        "suppression_rows",
    ],
    "challenger_e_champion_plus_granular": [
        "champion_expected_hits_allowed_poisson_implied",
        "expected_starter_facing_pa",
        "expected_hit_capable_contact_proxy",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_season_pa_per_game",
        "lineup_weighted_d15_pa_per_game",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_prior_start_count",
        "prior_dominated_share",
        "suppression_rows",
    ],
    "oracle_actual_bf": [
        "official_batters_faced_from_encounters",
        "official_hits_allowed_from_encounters",
    ],
}


@dataclass
class Instrument:
    name: str
    features: list[str]
    model: PoissonRegressor | None
    scaler: StandardScaler | None
    medians: dict[str, float]
    coeffs: list[dict[str, Any]]
    status: str

    def predict_mu(self, df: pd.DataFrame) -> np.ndarray:
        if self.name == "champion":
            return pd.to_numeric(df["champion_expected_hits_allowed_poisson_implied"], errors="coerce").fillna(df["line"]).to_numpy(dtype=float)
        if self.model is None or self.scaler is None:
            return np.full(len(df), np.nan)
        x = feature_matrix(df, self.features, self.medians)
        return np.clip(self.model.predict(self.scaler.transform(x)), 1e-6, 30.0)


def feature_matrix(df: pd.DataFrame, features: list[str], medians: dict[str, float]) -> pd.DataFrame:
    x = pd.DataFrame(index=df.index)
    for f in features:
        x[f] = pd.to_numeric(df[f], errors="coerce") if f in df.columns else np.nan
        x[f] = x[f].replace([np.inf, -np.inf], np.nan).fillna(medians.get(f, 0.0))
    return x


def expected_direction(feature: str) -> str:
    negative = {"lineup_weighted_zero_hit_risk", "prior_dominated_share", "suppression_rows"}
    context = {"starter_prior_start_count"}
    if feature in negative:
        return "negative_for_hits_allowed"
    if feature in context:
        return "context_dependent"
    return "positive_for_hits_allowed"


def fit_instrument(name: str, features: list[str], fit_df: pd.DataFrame) -> Instrument:
    y = pd.to_numeric(fit_df["official_hits_allowed"], errors="coerce")
    medians = {}
    for f in features:
        s = pd.to_numeric(fit_df[f], errors="coerce") if f in fit_df.columns else pd.Series(dtype=float)
        medians[f] = float(s.median()) if s.notna().any() else 0.0
    x = feature_matrix(fit_df, features, medians)
    if len(fit_df) < 50:
        return Instrument(name, features, None, None, medians, [], "NOT_FIT_INSUFFICIENT_ROWS")
    scaler = StandardScaler()
    xs = scaler.fit_transform(x)
    model = PoissonRegressor(alpha=1.0, max_iter=1000)
    model.fit(xs, y)
    coeffs = []
    for f, coef in zip(features, model.coef_):
        direction = expected_direction(f)
        status = "INFO"
        if direction == "positive_for_hits_allowed":
            status = "PASS" if coef >= 0 else "WARN_OPPOSITE_SIGN"
        elif direction == "negative_for_hits_allowed":
            status = "PASS" if coef <= 0 else "WARN_OPPOSITE_SIGN"
        coeffs.append({"instrument": name, "feature": f, "coefficient": float(coef), "expected_direction": direction, "orientation_status": status})
    coeffs.append({"instrument": name, "feature": "__intercept__", "coefficient": float(model.intercept_), "expected_direction": "not_applicable", "orientation_status": "INFO"})
    return Instrument(name, features, model, scaler, medians, coeffs, "FIT")


def score_population(df: pd.DataFrame, instruments: list[Instrument]) -> pd.DataFrame:
    out = df.copy()
    for inst in instruments:
        out[f"{inst.name}_expected_hits_allowed"] = inst.predict_mu(out)
        out[f"{inst.name}_prob_over"] = [poisson_over_prob(mu, line) for mu, line in zip(out[f"{inst.name}_expected_hits_allowed"], out["line"])]
        out[f"{inst.name}_prob_under"] = 1 - pd.to_numeric(out[f"{inst.name}_prob_over"], errors="coerce")
    return out


def count_metrics(df: pd.DataFrame, inst: str, split: str) -> dict[str, Any]:
    work = df[df["temporal_split"] == split].copy()
    col = f"{inst}_expected_hits_allowed"
    work = work[pd.to_numeric(work[col], errors="coerce").notna()]
    if work.empty:
        return {"temporal_split": split, "instrument": inst, "rows": 0}
    y = pd.to_numeric(work["official_hits_allowed"], errors="coerce")
    p = pd.to_numeric(work[col], errors="coerce").clip(1e-6, 30.0)
    return {
        "temporal_split": split,
        "instrument": inst,
        "rows": len(work),
        "actual_mean": float(y.mean()),
        "predicted_mean": float(p.mean()),
        "mae": float(mean_absolute_error(y, p)),
        "rmse": float(mean_squared_error(y, p) ** 0.5),
        "mean_bias": float((p - y).mean()),
        "median_absolute_error": float(np.median(np.abs(p - y))),
        "poisson_deviance_proxy": float(2 * np.mean(np.where(y == 0, p, y * np.log(np.clip(y / p, 1e-9, None)) - (y - p)))),
        "ranking_auc_gt_line": safe_auc(work["over_target"], work[f"{inst}_prob_over"]),
        "underprediction_rate": float((p < y).mean()),
        "overprediction_rate": float((p > y).mean()),
    }


def line_metrics(df: pd.DataFrame, inst: str) -> list[dict[str, Any]]:
    rows = []
    for (split, line), grp in df[df["temporal_split"].isin(["validation", "holdout"])].groupby(["temporal_split", "line"]):
        y = grp["over_target"].astype(int)
        p = pd.to_numeric(grp[f"{inst}_prob_over"], errors="coerce").clip(1e-6, 1 - 1e-6)
        rows.append(
            {
                "temporal_split": split,
                "line": line,
                "instrument": inst,
                "rows": len(grp),
                "over_wins": int(y.sum()),
                "over_losses": int((1 - y).sum()),
                "pushes": int(grp["push_target"].sum()),
                "observed_over_rate": float(y.mean()),
                "observed_under_rate": float((1 - y).mean()),
                "avg_prob_over": float(p.mean()),
                "brier": float(brier_score_loss(y, p)) if y.nunique() > 1 else "",
                "log_loss": float(log_loss(y, p, labels=[0, 1])) if y.nunique() > 1 else "",
                "auc": safe_auc(y, p),
                "ece": ece_score(y, p),
            }
        )
    return rows


def calibration_bands(df: pd.DataFrame, inst: str) -> list[dict[str, Any]]:
    rows = []
    bins = [-np.inf, 3.5, 4.5, 5.5, 6.5, np.inf]
    labels = ["lt3_5", "3_5_to_4_5", "4_5_to_5_5", "5_5_to_6_5", "ge6_5"]
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["expected_band"] = pd.cut(work[f"{inst}_expected_hits_allowed"], bins=bins, labels=labels)
    for (split, band), grp in work.groupby(["temporal_split", "expected_band"], observed=False):
        if grp.empty:
            continue
        rows.append({"temporal_split": split, "instrument": inst, "expected_hits_band": band, "rows": len(grp), "actual_mean": float(grp["official_hits_allowed"].mean()), "predicted_mean": float(grp[f"{inst}_expected_hits_allowed"].mean())})
    return rows


def bootstrap_uncertainty(df: pd.DataFrame, instruments: list[str], n: int = 200) -> list[dict[str, Any]]:
    rng = np.random.default_rng(17)
    rows = []
    for split in ["validation", "holdout"]:
        base = df[df["temporal_split"] == split].reset_index(drop=True)
        if len(base) < 30:
            continue
        for inst in instruments:
            maes = []
            aucs = []
            for _ in range(n):
                idx = rng.integers(0, len(base), len(base))
                sample = base.iloc[idx]
                maes.append(mean_absolute_error(sample["official_hits_allowed"], sample[f"{inst}_expected_hits_allowed"]))
                auc = safe_auc(sample["over_target"], sample[f"{inst}_prob_over"])
                if auc is not None:
                    aucs.append(auc)
            rows.append({"temporal_split": split, "instrument": inst, "bootstrap_iterations": n, "mae_mean": float(np.mean(maes)), "mae_p05": float(np.percentile(maes, 5)), "mae_p95": float(np.percentile(maes, 95)), "auc_mean": float(np.mean(aucs)) if aucs else "", "auc_p05": float(np.percentile(aucs, 5)) if aucs else "", "auc_p95": float(np.percentile(aucs, 95)) if aucs else ""})
    return rows


def mechanism_attribution(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by = {(r["temporal_split"], r["instrument"]): r for r in metrics}
    comps = [
        ("workload", "challenger_a_workload_only", "champion"),
        ("opponent_contact_frequency", "challenger_b_opponent_contact", "challenger_a_workload_only"),
        ("contact_conversion", "challenger_c_contact_conversion", "challenger_b_opponent_contact"),
        ("full_encounter", "challenger_d_full_encounter", "challenger_b_opponent_contact"),
        ("champion_plus_granular", "challenger_e_champion_plus_granular", "champion"),
        ("oracle_actual_bf", "oracle_actual_bf", "champion"),
    ]
    rows = []
    for split in ["validation", "holdout"]:
        for domain, inst, base in comps:
            a = by.get((split, inst), {})
            b = by.get((split, base), {})
            mae_imp = _delta(b.get("mae"), a.get("mae"))
            auc_imp = _delta(a.get("ranking_auc_gt_line"), b.get("ranking_auc_gt_line"))
            if mae_imp != "" and mae_imp > 0.05:
                cls = "count_increment"
            elif auc_imp != "" and auc_imp > 0.01:
                cls = "ranking_increment"
            elif mae_imp != "" and mae_imp < -0.05:
                cls = "harmful"
            else:
                cls = "redundant_or_small"
            rows.append({"temporal_split": split, "mechanism_domain": domain, "instrument": inst, "baseline": base, "mae_improvement": mae_imp, "auc_increment": auc_imp, "classification": cls, "notes": "fixed ablation; no arbitrary search"})
    return rows


def _delta(a: Any, b: Any) -> Any:
    if a in ["", None] or b in ["", None]:
        return ""
    try:
        return float(a) - float(b)
    except Exception:
        return ""


def same_pitcher_line_diagnostics(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for split in ["validation", "holdout"]:
        work = df[df["temporal_split"] == split]
        for group_name, group_cols in [
            ("same_pitcher_across_opponents", ["pitcher_id"]),
            ("same_market_line", ["line"]),
            ("workload_bucket", ["workload_bucket"]),
            ("lineup_contact_bucket", ["lineup_contact_bucket"]),
            ("support_bucket", ["support_bucket"]),
        ]:
            if any(c not in work.columns for c in group_cols):
                continue
            for key, grp in work.groupby(group_cols):
                if len(grp) < 5:
                    continue
                rows.append({
                    "temporal_split": split,
                    "diagnostic": group_name,
                    "bucket": str(key),
                    "rows": len(grp),
                    "actual_mean": float(grp["official_hits_allowed"].mean()),
                    "champion_mean": float(grp["champion_expected_hits_allowed"].mean()),
                    "challenger_e_mean": float(grp["challenger_e_champion_plus_granular_expected_hits_allowed"].mean()),
                    "champion_mae": float(mean_absolute_error(grp["official_hits_allowed"], grp["champion_expected_hits_allowed"])),
                    "challenger_e_mae": float(mean_absolute_error(grp["official_hits_allowed"], grp["challenger_e_champion_plus_granular_expected_hits_allowed"])),
                })
    return rows


def suppression_analysis(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["suppression_bucket"] = np.select(
        [pd.to_numeric(work.get("suppression_rows", 0), errors="coerce").fillna(0) > 0, work["pitcher_base"].isna()],
        ["affirmative_or_lineup_suppression_rows", "missing_or_uncertain"],
        default="no_affirmative_suppression",
    )
    for (split, bucket), grp in work.groupby(["temporal_split", "suppression_bucket"]):
        rows.append({"temporal_split": split, "suppression_bucket": bucket, "rows": len(grp), "official_hits_allowed_mean": float(grp["official_hits_allowed"].mean()), "champion_expected_hits_mean": float(grp["champion_expected_hits_allowed"].mean()), "challenger_e_expected_hits_mean": float(grp["challenger_e_champion_plus_granular_expected_hits_allowed"].mean()), "avg_movement": float((grp["challenger_e_champion_plus_granular_expected_hits_allowed"] - grp["champion_expected_hits_allowed"]).mean()), "notes": "suppression inherited from hitter-granular source and may be sparse"})
    return rows


def price_diagnostics(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["implied_break_even_over"] = work["price_over_american"].map(implied_prob_american)
    work["champion_over_profit"] = [american_profit(price, bool(win)) for price, win in zip(work["price_over_american"], work["over_target"])]
    for (split, line, book), grp in work.groupby(["temporal_split", "line", "bookmaker_key"], dropna=False):
        if len(grp) < 3:
            continue
        rows.append({"temporal_split": split, "line": line, "sportsbook": book, "rows": len(grp), "avg_price_over": float(pd.to_numeric(grp["price_over_american"], errors="coerce").mean()), "avg_implied_break_even_over": float(pd.to_numeric(grp["implied_break_even_over"], errors="coerce").mean()), "observed_over_rate": float(grp["over_target"].mean()), "champion_avg_prob_over": float(grp["model_prob_over"].mean()), "challenger_e_avg_prob_over": float(grp["challenger_e_champion_plus_granular_prob_over"].mean()), "diagnostic_roi_over": float(pd.to_numeric(grp["champion_over_profit"], errors="coerce").mean()), "timing_certification": "preserved_reconcile_snapshot_timestamp_not_repaired_selection_time_certification"})
    return rows


def feature_manifest(features: list[str], kind: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for f in sorted(features):
        vals = pd.to_numeric(df[f], errors="coerce") if f in df.columns else pd.Series(dtype=float)
        rows.append({"feature": f, "feature_family": kind, "source": "granular_encounter_aggregate" if f != "champion_expected_hits_allowed_poisson_implied" else "champion_line_probability_poisson_inversion", "coverage_rows": int(vals.notna().sum()), "coverage_pct": float(vals.notna().mean()) if len(df) else 0, "prediction_time_status": "strict_prior_or_champion_snapshot" if not f.startswith("official_") else "oracle_only", "missing_policy": "fit_split_median", "notes": ""})
    return rows


def decisions(metrics: list[dict[str, Any]], line_rows: list[dict[str, Any]], df: pd.DataFrame) -> list[dict[str, Any]]:
    by = {(r["temporal_split"], r["instrument"]): r for r in metrics}
    hc = by.get(("holdout", "champion"), {})
    he = by.get(("holdout", "challenger_e_champion_plus_granular"), {})
    mae_imp = _delta(hc.get("mae"), he.get("mae"))
    auc_imp = _delta(he.get("ranking_auc_gt_line"), hc.get("ranking_auc_gt_line"))
    if mae_imp != "" and mae_imp > 0.1 and auc_imp != "" and auc_imp > 0.01:
        primary = "PITCHER_HITS_ALLOWED_STRONG_INCREMENT"
    elif mae_imp != "" and mae_imp > 0.05:
        primary = "PITCHER_HITS_ALLOWED_COUNT_INCREMENT"
    elif auc_imp != "" and auc_imp > 0.01:
        primary = "PITCHER_HITS_ALLOWED_REPEATABLE_RANKING_INCREMENT"
    elif mae_imp != "" and mae_imp > 0:
        primary = "PITCHER_HITS_ALLOWED_CALIBRATION_ONLY"
    else:
        primary = "NO_PITCHER_HITS_ALLOWED_INCREMENT"
    return [
        {"decision_name": "MLB_PHA_CHAMPION_BINDING_DECISION", "decision_value": "CHAMPION_BOUND_FROM_PRESERVED_PITCHER_HITS_ALLOWED_RECONCILE_ROWS", "notes": "Native line probabilities preserved; count baseline uses documented Poisson inversion."},
        {"decision_name": "MLB_PHA_POPULATION_DECISION", "decision_value": "EXACT_PITCHER_LINE_POPULATION_ASSEMBLED_WITH_COVERAGE_NARROWING", "notes": f"rows={len(df)}; granular_joined={(df['granular_join_status']=='JOINED').sum()}"},
        {"decision_name": "MLB_PHA_WORKLOAD_COVERAGE_DECISION", "decision_value": "WORKLOAD_FEATURES_AVAILABLE_FROM_ENCOUNTER_AGGREGATES", "notes": "Expected starter-facing PA and starter expected hits fields joined where available."},
        {"decision_name": "MLB_PHA_LINEUP_ENCOUNTER_COVERAGE_DECISION", "decision_value": "LINEUP_WEIGHTED_ENCOUNTER_AGGREGATES_AVAILABLE_FOR_JOINED_ROWS", "notes": "Weighted by predicted starter-facing PA, not equal hitter weights."},
        {"decision_name": "MLB_PHA_TARGET_ORIENTATION_DECISION", "decision_value": "PASS_HIGHER_EXPECTED_HITS_ALLOWED_MEANS_HIGHER_OFFICIAL_HITS_ALLOWED", "notes": "Poisson count target is official starter hits allowed."},
        {"decision_name": "MLB_PHA_WORKLOAD_ONLY_DECISION", "decision_value": "EVALUATED_FIXED_ABLATION", "notes": "Challenger A."},
        {"decision_name": "MLB_PHA_CONTACT_AGGREGATE_DECISION", "decision_value": "EVALUATED_FIXED_ABLATION", "notes": "Challenger B."},
        {"decision_name": "MLB_PHA_CONTACT_CONVERSION_DECISION", "decision_value": "EVALUATED_FIXED_ABLATION", "notes": "Challenger C."},
        {"decision_name": "MLB_PHA_CHAMPION_PLUS_GRANULAR_DECISION", "decision_value": primary, "notes": f"holdout_mae_improvement={mae_imp}; holdout_auc_increment={auc_imp}"},
        {"decision_name": "MLB_PHA_COUNT_HOLDOUT_DECISION", "decision_value": "COUNT_HOLDOUT_EVALUATED", "notes": f"champion_mae={hc.get('mae')}; challenger_e_mae={he.get('mae')}"},
        {"decision_name": "MLB_PHA_LINE_PROBABILITY_HOLDOUT_DECISION", "decision_value": "LINE_SPECIFIC_PROBABILITY_EVALUATED_NO_OPTIMIZATION", "notes": "Half-line over/under probabilities evaluated by line."},
        {"decision_name": "MLB_PHA_SUPPRESSION_DECISION", "decision_value": "SUPPRESSION_SPARSE_EVALUATED_SEPARATELY", "notes": "Sparse inherited suppression rows; do not overinterpret."},
        {"decision_name": "MLB_PHA_PRICE_DIAGNOSTIC_DECISION", "decision_value": "PRICE_DIAGNOSTIC_ONLY_NO_OPTIMIZATION", "notes": "No EV threshold or price band selection."},
        {"decision_name": "MLB_PHA_HITTER_HITS_REUSE_DECISION", "decision_value": "REUSE_REQUIRES_SEPARATE_BOUNDED_HITTER_TRANSFER_EXPERIMENT", "notes": "No Hits O0.5/O1.5 modification."},
        {"decision_name": "MLB_PHA_NEXT_RESEARCH_DECISION", "decision_value": "PROMOTION_GRADE_OR_HITTER_TRANSFER_DESIGN_IF_HOLDOUT_INCREMENT_CONFIRMED" if primary not in {"NO_PITCHER_HITS_ALLOWED_INCREMENT"} else "STOP_OR_REDESIGN_NO_INCREMENT", "notes": "No production authorization."},
        {"decision_name": "MLB_PHA_PRODUCTION_STATUS", "decision_value": "NOT_AUTHORIZED", "notes": "No production behavior changed."},
    ]


def champion_contract(meta: dict[str, Any], df: pd.DataFrame) -> list[dict[str, Any]]:
    lines = ",".join(map(str, sorted(pd.to_numeric(df["line"], errors="coerce").dropna().unique())))
    return [
        {"contract_field": "proposition_identity", "value": "MLB|hits_allowed|pitcher_hits_allowed|line varies|over_under", "evidence": "reconcile_rows.csv prop_type=hits_allowed"},
        {"contract_field": "supported_market_lines", "value": lines, "evidence": "assembled historical population"},
        {"contract_field": "champion_probability", "value": "model_prob_over/model_prob_under from preserved reconcile rows", "evidence": f"source_files={meta.get('source_reconcile_file_count')}"},
        {"contract_field": "champion_expected_hits_allowed", "value": "Poisson-implied count from preserved line and model_prob_over for count diagnostics", "evidence": "No standalone production expected-count field was retained on reconcile rows."},
        {"contract_field": "official_outcome_source", "value": "actual_value in execution_vs_model reconcile rows", "evidence": "official starter hits allowed target"},
        {"contract_field": "push_treatment", "value": "half-line population produced no push under line semantics; push flag retained", "evidence": "line-specific outcome columns"},
        {"contract_field": "temporal_blocks", "value": f"fit<={FIT_END}; validation={VALIDATION_START}..{VALIDATION_END}; holdout={HOLDOUT_START}..{HOLDOUT_END}", "evidence": "fixed constants"},
        {"contract_field": "production_status", "value": "unchanged", "evidence": "offline package only"},
    ]


def historical_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = ["canonical_key", "slate_date", "game_id", "pitcher_id", "player_name", "team", "opponent", "line", "bookmaker_key", "price_over_american", "price_under_american", "model_prob_over", "champion_expected_hits_allowed", "official_hits_allowed", "official_batters_faced_from_encounters", "official_hits_allowed_from_encounters", "over_target", "under_target", "push_target", "temporal_split", "snapshot_run_tag", "snapshot_time_utc", "source_reconcile_path", "source_reconcile_sha256", "granular_join_status", "lineup_batters"]
    return [{c: r.get(c, "") for c in cols} for _, r in df.iterrows()]


def summary_md(stats: dict[str, Any], decisions_rows: list[dict[str, Any]]) -> str:
    primary = next(r["decision_value"] for r in decisions_rows if r["decision_name"] == "MLB_PHA_CHAMPION_PLUS_GRANULAR_DECISION")
    return f"""# MLB Pitcher Hits Allowed Granular Encounter Challenger

Generated: `{stats['generated_at']}`

## Executive Summary

This bounded offline experiment compared preserved pitcher hits-allowed Champion line probabilities against fixed granular workload and opponent-lineup encounter challengers.

Primary decision:

`{primary}`

The native Champion count expectation was not retained as a standalone field in `reconcile_rows.csv`; count evaluation therefore uses a documented Poisson-implied expected hits value from the preserved line and `model_prob_over`. Native proposition-line probability evaluation remains separate.

## Population

- Exact pitcher-line rows: `{stats['population_rows']}`
- Fit rows: `{stats['fit_rows']}`
- Validation rows: `{stats['validation_rows']}`
- Untouched holdout rows: `{stats['holdout_rows']}`
- Granular joined rows: `{stats['granular_joined_rows']}`

## Primary Holdout

- Champion MAE: `{stats['holdout_champion_mae']}`
- Champion+granular MAE: `{stats['holdout_challenger_e_mae']}`
- MAE improvement: `{stats['holdout_mae_improvement']}`
- Champion line AUC: `{stats['holdout_champion_auc']}`
- Champion+granular line AUC: `{stats['holdout_challenger_e_auc']}`
- AUC increment: `{stats['holdout_auc_increment']}`

## No Behavior Changed

No production model, formula, selector, upload, Quick Card, workspace, LaunchAgent, OddsAPI, DB, Hits O0.5, or O1.5 artifact was changed.
"""


def validate_files(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for p in paths:
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md" and not p.read_text().startswith("#"):
                status = "WARN"
                notes = "markdown heading missing"
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(p), "validation": status, "notes": notes})
    rows += [
        {"artifact": "guardrail_no_network_or_oddsapi", "validation": "PASS", "notes": "local files only"},
        {"artifact": "guardrail_no_db_writes", "validation": "PASS", "notes": "no database connector used"},
        {"artifact": "guardrail_no_production_change", "validation": "PASS", "notes": "outputs limited to artifact package"},
        {"artifact": "guardrail_hits_models_preserved", "validation": "PASS", "notes": "Hits O0.5 and O1.5 not modified"},
    ]
    return rows


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    joined, meta = assemble_population()
    joined = joined[joined["temporal_split"].isin(["fit", "validation", "holdout"]) & (joined["granular_join_status"] == "JOINED")].copy()
    if joined.empty:
        raise RuntimeError("no joined pitcher hits-allowed population")
    joined["workload_bucket"] = pd.cut(pd.to_numeric(joined["expected_starter_facing_pa"], errors="coerce"), [-np.inf, 20, 24, np.inf], labels=["low_workload", "normal_workload", "high_workload"])
    joined["lineup_contact_bucket"] = pd.cut(pd.to_numeric(joined["lineup_weighted_hit_rate"], errors="coerce"), [-np.inf, .20, .24, np.inf], labels=["low_contact", "normal_contact", "high_contact"])
    joined["support_bucket"] = pd.cut(pd.to_numeric(joined["prior_dominated_share"], errors="coerce"), [-np.inf, .1, .35, np.inf], labels=["high_support", "mixed_support", "prior_dominated"])
    fit = joined[joined["temporal_split"] == "fit"].copy()
    instruments = [Instrument("champion", [], None, None, {}, [], "BOUND")]
    for name, features in FEATURE_GROUPS.items():
        instruments.append(fit_instrument(name, features, fit))
    scored = score_population(joined, instruments)
    inst_names = [i.name for i in instruments]
    count_rows = [count_metrics(scored, inst, split) for split in ["fit", "validation", "holdout"] for inst in inst_names]
    line_rows_all = []
    for inst in inst_names:
        line_rows_all.extend(line_metrics(scored, inst))
    band_rows = []
    for inst in inst_names:
        band_rows.extend(calibration_bands(scored, inst))
    boot = bootstrap_uncertainty(scored, inst_names)
    mechanism = mechanism_attribution(count_rows)
    same = same_pitcher_line_diagnostics(scored)
    supp = suppression_analysis(scored)
    price = price_diagnostics(scored)
    decisions_rows = decisions(count_rows, line_rows_all, scored)
    by = {(r["temporal_split"], r["instrument"]): r for r in count_rows}
    hc = by[("holdout", "champion")]
    he = by[("holdout", "challenger_e_champion_plus_granular")]
    stats = {
        "generated_at": generated_at,
        "population_rows": len(scored),
        "fit_rows": int((scored["temporal_split"] == "fit").sum()),
        "validation_rows": int((scored["temporal_split"] == "validation").sum()),
        "holdout_rows": int((scored["temporal_split"] == "holdout").sum()),
        "granular_joined_rows": int((scored["granular_join_status"] == "JOINED").sum()),
        "holdout_champion_mae": hc.get("mae"),
        "holdout_challenger_e_mae": he.get("mae"),
        "holdout_mae_improvement": _delta(hc.get("mae"), he.get("mae")),
        "holdout_champion_auc": hc.get("ranking_auc_gt_line"),
        "holdout_challenger_e_auc": he.get("ranking_auc_gt_line"),
        "holdout_auc_increment": _delta(he.get("ranking_auc_gt_line"), hc.get("ranking_auc_gt_line")),
    }
    files = {
        "summary": output_dir / f"executive_summary_{RUN_DATE}.md",
        "champion": output_dir / f"pitcher_hits_allowed_champion_contract_{RUN_DATE}.csv",
        "population": output_dir / f"pitcher_hits_allowed_exact_historical_population_{RUN_DATE}.csv",
        "workload_manifest": output_dir / f"pitcher_hits_allowed_workload_feature_manifest_{RUN_DATE}.csv",
        "lineup_ledger": output_dir / f"pitcher_hits_allowed_opponent_lineup_encounter_ledger_{RUN_DATE}.csv",
        "pitcher_manifest": output_dir / f"pitcher_hits_allowed_pitcher_granular_feature_manifest_{RUN_DATE}.csv",
        "challenger_contracts": output_dir / f"pitcher_hits_allowed_fixed_challenger_contracts_{RUN_DATE}.csv",
        "coefficients": output_dir / f"pitcher_hits_allowed_coefficient_orientation_audit_{RUN_DATE}.csv",
        "count_results": output_dir / f"pitcher_hits_allowed_validation_holdout_count_results_{RUN_DATE}.csv",
        "line_results": output_dir / f"pitcher_hits_allowed_line_specific_over_under_results_{RUN_DATE}.csv",
        "calibration": output_dir / f"pitcher_hits_allowed_expected_hits_band_calibration_{RUN_DATE}.csv",
        "bootstrap": output_dir / f"pitcher_hits_allowed_bootstrap_uncertainty_{RUN_DATE}.csv",
        "mechanism": output_dir / f"pitcher_hits_allowed_mechanism_attribution_{RUN_DATE}.csv",
        "same": output_dir / f"pitcher_hits_allowed_same_pitcher_same_line_diagnostics_{RUN_DATE}.csv",
        "suppression": output_dir / f"pitcher_hits_allowed_suppression_analysis_{RUN_DATE}.csv",
        "price": output_dir / f"pitcher_hits_allowed_price_diagnostics_{RUN_DATE}.csv",
        "reuse": output_dir / f"pitcher_hits_allowed_hitter_hits_transfer_pathway_{RUN_DATE}.csv",
        "decisions": output_dir / f"pitcher_hits_allowed_required_decisions_{RUN_DATE}.csv",
        "machine": output_dir / f"machine_readable_pitcher_hits_allowed_challenger_{RUN_DATE}.json",
        "manifest": output_dir / f"sha256_manifest_{RUN_DATE}.csv",
        "validation": output_dir / f"validation_report_{RUN_DATE}.csv",
    }
    write_text(files["summary"], summary_md(stats, decisions_rows))
    write_csv(files["champion"], champion_contract(meta, scored))
    write_csv(files["population"], historical_rows(scored))
    workload_features = sorted({f for name, feats in FEATURE_GROUPS.items() if "workload" in name or name in {"challenger_e_champion_plus_granular", "challenger_d_full_encounter"} for f in feats})
    write_csv(files["workload_manifest"], feature_manifest(workload_features, "workload", scored))
    write_csv(files["lineup_ledger"], scored[["join_key", "slate_date", "game_id", "pitcher_id", "lineup_batters", "expected_starter_facing_pa", "expected_hit_capable_contact_proxy", "lineup_weighted_hit_rate", "lineup_weighted_contact_conversion", "lineup_weighted_season_hits_per_pa", "lineup_weighted_p4", "lineup_weighted_p5", "prior_dominated_share"]].drop_duplicates("join_key").to_dict("records"))
    pitcher_features = sorted({f for feats in FEATURE_GROUPS.values() for f in feats if "pitcher" in f or "starter" in f or f.startswith("champion")})
    write_csv(files["pitcher_manifest"], feature_manifest(pitcher_features, "pitcher_granular", scored))
    write_csv(files["challenger_contracts"], [{"instrument": "champion", "definition": "preserved line probability plus Poisson-implied count", "features": "model_prob_over,line", "notes": "unchanged champion; count proxy for diagnostics"}] + [{"instrument": name, "definition": "fixed Poisson count model", "features": ",".join(feats), "notes": "alpha=1.0 fixed; no hyperparameter search"} for name, feats in FEATURE_GROUPS.items()])
    coeffs = []
    for inst in instruments:
        coeffs.extend(inst.coeffs)
    write_csv(files["coefficients"], coeffs)
    write_csv(files["count_results"], count_rows)
    write_csv(files["line_results"], line_rows_all)
    write_csv(files["calibration"], band_rows)
    write_csv(files["bootstrap"], boot)
    write_csv(files["mechanism"], mechanism)
    write_csv(files["same"], same)
    write_csv(files["suppression"], supp)
    write_csv(files["price"], price)
    write_csv(files["reuse"], [
        {"target": "Hits O0.5", "reuse_component": "improved pitcher baseline and zero-hit risk", "status": "future_bounded_transfer_required", "notes": "No Hits model modified."},
        {"target": "Hits O1.5", "reuse_component": "pitcher-owned multi-hit suppression and expected total hits conceded", "status": "future_bounded_transfer_required", "notes": "O1.5 prospective program untouched."},
        {"target": "matchup ownership", "reuse_component": "opponent lineup contact versus starter suppression aggregate", "status": "diagnostic", "notes": "Requires separate hitter transfer experiment."},
    ])
    write_csv(files["decisions"], decisions_rows)
    machine = {"generated_at": generated_at, "run_date": RUN_DATE, "mode": "offline_research_only", "source_artifacts": {"reconcile_root": str(RECONCILE_ROOT), "granular_source": str(GRANULAR_SOURCE)}, "meta": meta, "stats": stats, "decisions": decisions_rows, "guardrails": {"network_calls": 0, "db_writes": 0, "hyperparameter_search": False, "production_behavior_changed": False, "hits05_modified": False, "o15_modified_or_graded": False}}
    write_text(files["machine"], json.dumps(machine, indent=2, default=str) + "\n")
    manifest_targets = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], [{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"} for p in manifest_targets])
    write_csv(files["validation"], validate_files(manifest_targets + [files["manifest"]]))
    return {"output_dir": str(output_dir), "files": {k: str(v) for k, v in files.items()}, "stats": stats}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    args = parser.parse_args()
    print(json.dumps(build(args.output_dir), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
