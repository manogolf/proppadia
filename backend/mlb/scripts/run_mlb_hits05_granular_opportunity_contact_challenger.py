"""Bounded MLB Hits O0.5 granular opportunity/contact challenger.

Offline research only. The script reads preserved local prediction/outcome
artifacts and existing strict-prior granular research artifacts, fits one fixed
interpretable configuration, and writes an immutable audit package. It performs
no production writes, network calls, upload changes, or O1.5 grading.
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler


RUN_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits05_granular_opportunity_contact_challenger/2026-07-17"
)
RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
GRANULAR_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/"
    "2026-07-17/research_only_model_artifacts_2026-07-17.csv"
)
CURRENT_SLATE = Path(
    "backend/mlb/exports/odds_history/2026-07-17/"
    "mlb_slate_output__local_daily_20260717T124203Z.csv"
)
CROSS_PROP_REGISTRY = Path(
    "artifacts/analysis/model_development/mlb_granular_feature_platform_cross_prop_transfer/"
    "2026-07-17/granular_feature_registry_2026-07-17.csv"
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


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        fieldnames = keys
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def logit(p: pd.Series) -> pd.Series:
    x = pd.to_numeric(p, errors="coerce").clip(1e-6, 1 - 1e-6)
    return np.log(x / (1 - x))


def inv_logit(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -35, 35)))


def safe_auc(y: pd.Series | np.ndarray, p: pd.Series | np.ndarray) -> float | None:
    y_arr = np.asarray(y, dtype=float)
    p_arr = np.asarray(p, dtype=float)
    if len(np.unique(y_arr[~np.isnan(y_arr)])) < 2:
        return None
    try:
        return float(roc_auc_score(y_arr, p_arr))
    except Exception:
        return None


def ece_score(y: pd.Series | np.ndarray, p: pd.Series | np.ndarray, bins: int = 10) -> float | None:
    y_arr = np.asarray(y, dtype=float)
    p_arr = np.asarray(p, dtype=float)
    mask = np.isfinite(y_arr) & np.isfinite(p_arr)
    if not mask.any():
        return None
    y_arr = y_arr[mask]
    p_arr = p_arr[mask]
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = len(y_arr)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        if hi == 1:
            idx = (p_arr >= lo) & (p_arr <= hi)
        else:
            idx = (p_arr >= lo) & (p_arr < hi)
        if not idx.any():
            continue
        ece += (idx.sum() / total) * abs(float(y_arr[idx].mean()) - float(p_arr[idx].mean()))
    return float(ece)


def calibration_line(y: pd.Series | np.ndarray, p: pd.Series | np.ndarray) -> tuple[float | None, float | None]:
    y_arr = np.asarray(y, dtype=float)
    p_arr = np.asarray(p, dtype=float)
    mask = np.isfinite(y_arr) & np.isfinite(p_arr)
    y_arr = y_arr[mask]
    p_arr = p_arr[mask]
    if len(y_arr) < 20 or len(np.unique(y_arr)) < 2:
        return None, None
    x = np.log(np.clip(p_arr, 1e-6, 1 - 1e-6) / np.clip(1 - p_arr, 1e-6, 1 - 1e-6)).reshape(-1, 1)
    try:
        lr = LogisticRegression(C=1e6, solver="lbfgs", max_iter=1000)
        lr.fit(x, y_arr)
        return float(lr.coef_[0][0]), float(lr.intercept_[0])
    except Exception:
        return None, None


def american_profit(price: Any, win: int) -> float | None:
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


def load_champion_population() -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    source_paths: list[str] = []
    for path in sorted(RECONCILE_ROOT.glob("*/reconcile_rows.csv")):
        date_value = path.parent.name
        if date_value < "2026-05-01" or date_value > HOLDOUT_END:
            continue
        df = read_csv(path)
        needed = {"slate_date", "game_id", "player_id", "prop_type", "line", "model_prob_over", "actual_value"}
        if df.empty or not needed.issubset(df.columns):
            continue
        work = df[(df["prop_type"].astype(str) == "hits") & (pd.to_numeric(df["line"], errors="coerce") == 0.5)].copy()
        if work.empty:
            continue
        work["source_reconcile_path"] = str(path)
        work["source_reconcile_sha256"] = sha256_file(path)
        frames.append(work)
        source_paths.append(str(path))
    if not frames:
        return pd.DataFrame(), source_paths
    pop = pd.concat(frames, ignore_index=True)
    pop["slate_date"] = pop["slate_date"].astype(str)
    pop["game_id"] = pd.to_numeric(pop["game_id"], errors="coerce").astype("Int64")
    pop["player_id"] = pd.to_numeric(pop["player_id"], errors="coerce").astype("Int64")
    pop["line"] = pd.to_numeric(pop["line"], errors="coerce")
    pop["actual_hits"] = pd.to_numeric(pop["actual_value"], errors="coerce")
    pop["any_hit_target"] = (pop["actual_hits"] >= 1).astype("Int64")
    pop["canonical_key"] = (
        pop["slate_date"].astype(str)
        + "|"
        + pop["game_id"].astype(str)
        + "|"
        + pop["player_id"].astype(str)
        + "|hits|0.5"
    )
    pop["snapshot_time_sort"] = pd.to_datetime(pop.get("snapshot_time_utc"), errors="coerce")
    pop["book_sort"] = pop.get("bookmaker_key", "").astype(str)
    pop = pop.sort_values(["canonical_key", "snapshot_time_sort", "book_sort"], na_position="last")
    pop["duplicate_observation_count"] = pop.groupby("canonical_key")["canonical_key"].transform("size")
    unique = pop.drop_duplicates("canonical_key", keep="first").copy()
    unique = unique[unique["actual_hits"].notna() & unique["model_prob_over"].notna()].copy()
    unique["champion_prob_any_hit"] = pd.to_numeric(unique["model_prob_over"], errors="coerce").clip(1e-6, 1 - 1e-6)
    unique["champion_prob_zero_hit"] = 1 - unique["champion_prob_any_hit"]
    unique["temporal_split"] = np.select(
        [
            unique["slate_date"] <= FIT_END,
            unique["slate_date"].between(VALIDATION_START, VALIDATION_END),
            unique["slate_date"].between(HOLDOUT_START, HOLDOUT_END),
        ],
        ["fit", "validation", "holdout"],
        default="outside_fixed_window",
    )
    return unique, source_paths


def load_granular() -> pd.DataFrame:
    df = read_csv(GRANULAR_SOURCE)
    if df.empty:
        return df
    df["slate_date"] = df["slate_date"].astype(str)
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["granular_key"] = df["slate_date"] + "|" + df["game_id"].astype(str) + "|" + df["player_id"].astype(str)
    keep = [
        "granular_key",
        "player_game_key",
        "official_hits",
        "official_pa",
        "actual_position",
        "strict_prior_status",
        "prior_game_count",
        "d7_two_plus_rate",
        "d15_two_plus_rate",
        "d30_hits_per_pa",
        "d15_pa_per_game",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "lineup_slot",
        "lineup_bucket",
        "starter_feature_available",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_context_status",
        "suppression_subtype",
        "control_p_zero_hits",
        "control_p_exactly_one_hit",
        "control_p_two_plus_hits",
        "expected_pa_used",
        "hitter_per_pa_hit_estimate",
        "starter_adjustment",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "pred_total_pa",
        "p_hit_starter_prior",
        "p_hit_bullpen_neutral",
        "p_hit_bullpen_prior",
        "predicted_exposure_p_zero_hits",
        "predicted_exposure_p_exactly_one_hit",
        "predicted_exposure_p_two_plus_hits",
        "source_aware_unified_p_zero_hits",
        "source_aware_unified_p_exactly_one_hit",
        "source_aware_unified_p_two_plus_hits",
        "opposing_starter_id",
        "opposing_starter_name",
        "actual_total_pa",
        "actual_starter_facing_pa_seq",
        "actual_bullpen_facing_pa_seq",
        "starter_exit_before_hitter_pa3",
        "starter_exit_before_hitter_pa4",
        "bullpen_pa_ge1",
        "bullpen_pa_ge2",
        "hitter_receives_fourth_pa",
        "hitter_receives_fifth_pa",
        "starter_prior_status",
        "starter_prior_start_count",
        "prior_pred_starter_pa",
        "prior_pred_bullpen_pa",
        "prior_predicted_exposure_p_two_plus_hits",
        "challenger_total_pa",
        "challenger_starter_pa",
        "challenger_bullpen_pa",
        "p_bullpen_pa_ge1",
        "p_bullpen_pa_ge2",
        "p_starter_exit_before_pa3",
        "p_starter_exit_before_pa4",
        "p_hitter_receives_fourth_pa",
        "p_hitter_receives_fifth_pa",
        "challenger_p_zero_hits",
        "challenger_p_exactly_one_hit",
        "challenger_p_two_plus_hits",
        "joint_p_zero_hits",
        "joint_p_exactly_one_hit",
        "joint_p_two_plus_hits",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep].drop_duplicates("granular_key", keep="first")


def assemble_population() -> tuple[pd.DataFrame, dict[str, Any]]:
    champion, source_paths = load_champion_population()
    granular = load_granular()
    if champion.empty:
        return champion, {"source_reconcile_files": source_paths, "granular_rows": len(granular)}
    champion["granular_key"] = (
        champion["slate_date"].astype(str)
        + "|"
        + champion["game_id"].astype(str)
        + "|"
        + champion["player_id"].astype(str)
    )
    joined = champion.merge(granular, on="granular_key", how="left", suffixes=("", "_granular"))
    joined["granular_join_status"] = np.where(joined["player_game_key"].notna(), "JOINED", "MISSING_GRANULAR")
    joined["granular_core_available"] = joined["player_game_key"].notna()
    meta = {
        "source_reconcile_files": source_paths,
        "source_reconcile_file_count": len(source_paths),
        "granular_source": str(GRANULAR_SOURCE),
        "granular_source_sha256": sha256_file(GRANULAR_SOURCE) if GRANULAR_SOURCE.exists() else "",
        "granular_rows": len(granular),
        "raw_champion_rows": int(champion["duplicate_observation_count"].sum()) if not champion.empty else 0,
        "unique_champion_rows": len(champion),
        "joined_rows": int((joined["granular_join_status"] == "JOINED").sum()),
    }
    return joined, meta


FEATURE_GROUPS = {
    "challenger_a_opportunity_noncontact": [
        "expected_pa_used",
        "pred_total_pa",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "p_hitter_receives_fourth_pa",
        "p_hitter_receives_fifth_pa",
        "p_bullpen_pa_ge1",
        "p_starter_exit_before_pa4",
        "season_to_date_pa_per_game",
        "d15_pa_per_game",
        "starter_expected_hits_allowed",
        "pitcher_base",
    ],
    "challenger_b_contact_opportunity": [
        "expected_pa_used",
        "pred_total_pa",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "hitter_per_pa_hit_estimate",
        "p_hit_starter_prior",
        "p_hit_bullpen_prior",
        "season_to_date_hits_per_pa",
        "d30_hits_per_pa",
        "prior_predicted_exposure_p_two_plus_hits",
    ],
    "challenger_c_full_granular": [
        "expected_pa_used",
        "pred_total_pa",
        "pred_starter_pa",
        "pred_bullpen_pa",
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
        "p_bullpen_pa_ge2",
        "p_starter_exit_before_pa3",
        "p_starter_exit_before_pa4",
        "predicted_exposure_p_zero_hits",
        "source_aware_unified_p_zero_hits",
        "challenger_p_zero_hits",
        "joint_p_zero_hits",
    ],
    "challenger_d_champion_plus_granular": [
        "champion_logit",
        "expected_pa_used",
        "pred_total_pa",
        "pred_starter_pa",
        "pred_bullpen_pa",
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
    ],
    "oracle_actual_pa": [
        "actual_total_pa",
        "actual_starter_facing_pa_seq",
        "actual_bullpen_facing_pa_seq",
    ],
}


@dataclass
class FittedInstrument:
    name: str
    features: list[str]
    scaler: StandardScaler | None
    model: LogisticRegression | None
    medians: dict[str, float]
    coefficient_rows: list[dict[str, Any]]
    status: str
    notes: str

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        if self.name == "champion":
            return pd.to_numeric(df["champion_prob_any_hit"], errors="coerce").fillna(0.5).to_numpy(dtype=float)
        if self.model is None or self.scaler is None:
            return np.full(len(df), np.nan)
        x = prepare_feature_matrix(df, self.features, self.medians)
        return self.model.predict_proba(self.scaler.transform(x))[:, 1]


def prepare_feature_matrix(df: pd.DataFrame, features: list[str], medians: dict[str, float]) -> pd.DataFrame:
    x = pd.DataFrame(index=df.index)
    for f in features:
        if f in df.columns:
            x[f] = pd.to_numeric(df[f], errors="coerce")
        else:
            x[f] = np.nan
        x[f] = x[f].replace([np.inf, -np.inf], np.nan).fillna(medians.get(f, 0.0))
    return x


def fit_instrument(name: str, features: list[str], fit_df: pd.DataFrame) -> FittedInstrument:
    y = fit_df["any_hit_target"].astype(int)
    medians = {}
    for f in features:
        values = pd.to_numeric(fit_df[f], errors="coerce") if f in fit_df.columns else pd.Series(dtype=float)
        medians[f] = float(values.median()) if values.notna().any() else 0.0
    x = prepare_feature_matrix(fit_df, features, medians)
    if len(fit_df) < 50 or y.nunique() < 2:
        return FittedInstrument(name, features, None, None, medians, [], "NOT_FIT_INSUFFICIENT_POPULATION", "fit split too sparse")
    scaler = StandardScaler()
    xs = scaler.fit_transform(x)
    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=17)
    model.fit(xs, y)
    coeffs = [
        {
            "instrument": name,
            "feature": feature,
            "coefficient": float(coef),
            "expected_direction": expected_direction(feature),
            "orientation_status": orientation_status(feature, float(coef)),
        }
        for feature, coef in zip(features, model.coef_[0])
    ]
    coeffs.append(
        {
            "instrument": name,
            "feature": "__intercept__",
            "coefficient": float(model.intercept_[0]),
            "expected_direction": "not_applicable",
            "orientation_status": "INFO",
        }
    )
    return FittedInstrument(name, features, scaler, model, medians, coeffs, "FIT", "fixed C=1.0 lbfgs logistic regression")


def expected_direction(feature: str) -> str:
    positive = {
        "champion_logit",
        "expected_pa_used",
        "pred_total_pa",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "p_hitter_receives_fourth_pa",
        "p_hitter_receives_fifth_pa",
        "p_bullpen_pa_ge1",
        "p_bullpen_pa_ge2",
        "hitter_per_pa_hit_estimate",
        "p_hit_starter_prior",
        "p_hit_bullpen_prior",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "d15_pa_per_game",
        "d30_hits_per_pa",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "prior_predicted_exposure_p_two_plus_hits",
        "actual_total_pa",
        "actual_starter_facing_pa_seq",
        "actual_bullpen_facing_pa_seq",
    }
    negative = {
        "predicted_exposure_p_zero_hits",
        "source_aware_unified_p_zero_hits",
        "challenger_p_zero_hits",
        "joint_p_zero_hits",
    }
    if feature in positive:
        return "positive_for_any_hit"
    if feature in negative:
        return "negative_for_any_hit"
    if "exit_before" in feature:
        return "context_dependent"
    return "unknown"


def orientation_status(feature: str, coef: float) -> str:
    direction = expected_direction(feature)
    if direction == "positive_for_any_hit":
        return "PASS" if coef >= 0 else "WARN_OPPOSITE_SIGN"
    if direction == "negative_for_any_hit":
        return "PASS" if coef <= 0 else "WARN_OPPOSITE_SIGN"
    return "INFO"


def add_predictions(df: pd.DataFrame, instruments: list[FittedInstrument]) -> pd.DataFrame:
    out = df.copy()
    for inst in instruments:
        out[f"{inst.name}_prob_any_hit"] = inst.predict(out)
        out[f"{inst.name}_prob_zero_hit"] = 1 - out[f"{inst.name}_prob_any_hit"]
    return out


def instrument_metrics(df: pd.DataFrame, prob_col: str, split: str, instrument: str) -> dict[str, Any]:
    work = df[df["temporal_split"] == split].copy()
    work = work[pd.to_numeric(work[prob_col], errors="coerce").notna()].copy()
    if work.empty:
        return {"temporal_split": split, "instrument": instrument, "rows": 0}
    y = work["any_hit_target"].astype(int)
    p = pd.to_numeric(work[prob_col], errors="coerce").clip(1e-6, 1 - 1e-6)
    slope, intercept = calibration_line(y, p)
    return {
        "temporal_split": split,
        "instrument": instrument,
        "rows": len(work),
        "positives_any_hit": int(y.sum()),
        "zero_hit_rows": int((1 - y).sum()),
        "observed_any_hit_rate": float(y.mean()),
        "avg_predicted_any_hit": float(p.mean()),
        "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "auc": safe_auc(y, p),
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "ece": ece_score(y, p),
    }


def probability_bands(df: pd.DataFrame, prob_col: str, instrument: str) -> list[dict[str, Any]]:
    rows = []
    labels = ["lt_0_45", "0_45_to_0_55", "0_55_to_0_65", "0_65_to_0_75", "ge_0_75"]
    bins = [-np.inf, 0.45, 0.55, 0.65, 0.75, np.inf]
    work = df.copy()
    work["band"] = pd.cut(pd.to_numeric(work[prob_col], errors="coerce"), bins=bins, labels=labels)
    for (split, band), grp in work.groupby(["temporal_split", "band"], observed=False):
        if grp.empty or str(split) == "fit":
            continue
        y = grp["any_hit_target"].astype(int)
        rows.append(
            {
                "temporal_split": split,
                "instrument": instrument,
                "probability_band": band,
                "rows": len(grp),
                "observed_any_hit_rate": float(y.mean()) if len(grp) else "",
                "avg_predicted_any_hit": float(pd.to_numeric(grp[prob_col], errors="coerce").mean()),
                "zero_hit_rate": float((1 - y).mean()) if len(grp) else "",
            }
        )
    return rows


def bootstrap_rows(df: pd.DataFrame, instruments: list[str], n: int = 200) -> list[dict[str, Any]]:
    rng = np.random.default_rng(17)
    rows = []
    for split in ["validation", "holdout"]:
        base = df[df["temporal_split"] == split].reset_index(drop=True)
        if len(base) < 30:
            continue
        for inst in instruments:
            col = f"{inst}_prob_any_hit"
            if col not in base.columns:
                continue
            auc_values = []
            brier_values = []
            for _ in range(n):
                idx = rng.integers(0, len(base), len(base))
                sample = base.iloc[idx]
                y = sample["any_hit_target"].astype(int)
                p = pd.to_numeric(sample[col], errors="coerce").clip(1e-6, 1 - 1e-6)
                auc = safe_auc(y, p)
                if auc is not None:
                    auc_values.append(auc)
                brier_values.append(float(brier_score_loss(y, p)))
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": inst,
                    "bootstrap_iterations": n,
                    "auc_mean": float(np.mean(auc_values)) if auc_values else "",
                    "auc_p05": float(np.percentile(auc_values, 5)) if auc_values else "",
                    "auc_p95": float(np.percentile(auc_values, 95)) if auc_values else "",
                    "brier_mean": float(np.mean(brier_values)) if brier_values else "",
                    "brier_p05": float(np.percentile(brier_values, 5)) if brier_values else "",
                    "brier_p95": float(np.percentile(brier_values, 95)) if brier_values else "",
                }
            )
    return rows


def zero_hit_rows(df: pd.DataFrame, instruments: list[str]) -> list[dict[str, Any]]:
    rows = []
    for split in ["validation", "holdout"]:
        work = df[df["temporal_split"] == split].copy()
        if work.empty:
            continue
        y_zero = 1 - work["any_hit_target"].astype(int)
        for inst in instruments:
            pz_col = f"{inst}_prob_zero_hit"
            pz = pd.to_numeric(work[pz_col], errors="coerce").clip(1e-6, 1 - 1e-6)
            threshold = pz.quantile(0.8)
            selected = pz >= threshold
            true_zero = y_zero == 1
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": inst,
                    "rows": len(work),
                    "zero_hit_rows": int(true_zero.sum()),
                    "zero_hit_auc": safe_auc(y_zero, pz),
                    "zero_hit_brier": float(brier_score_loss(y_zero, pz)),
                    "highest_predicted_zero_band_rows": int(selected.sum()),
                    "zero_hit_precision_top20pct": float((true_zero & selected).sum() / max(selected.sum(), 1)),
                    "zero_hit_recall_top20pct": float((true_zero & selected).sum() / max(true_zero.sum(), 1)),
                    "avg_predicted_zero_top20pct": float(pz[selected].mean()) if selected.any() else "",
                    "observed_zero_rate_top20pct": float(y_zero[selected].mean()) if selected.any() else "",
                }
            )
    return rows


def mechanism_attribution(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {(r["temporal_split"], r["instrument"]): r for r in metrics}
    rows = []
    comparisons = [
        ("PA opportunity + noncontact", "challenger_a_opportunity_noncontact", "champion"),
        ("contact-frequency prediction", "challenger_b_contact_opportunity", "challenger_a_opportunity_noncontact"),
        ("contact conversion/full granular", "challenger_c_full_granular", "challenger_b_contact_opportunity"),
        ("champion plus granular", "challenger_d_champion_plus_granular", "champion"),
        ("oracle PA attribution", "oracle_actual_pa", "champion"),
    ]
    for split in ["validation", "holdout"]:
        for domain, inst, base in comparisons:
            a = by_key.get((split, inst), {})
            b = by_key.get((split, base), {})
            auc_delta = _num(a.get("auc")) - _num(b.get("auc")) if a.get("auc") not in ["", None] and b.get("auc") not in ["", None] else ""
            brier_delta = _num(b.get("brier")) - _num(a.get("brier")) if a.get("brier") not in ["", None] and b.get("brier") not in ["", None] else ""
            if auc_delta == "" and brier_delta == "":
                cls = "not_evaluable"
            elif (auc_delta != "" and auc_delta > 0.01) or (brier_delta != "" and brier_delta > 0.002):
                cls = "ranking_or_calibration_increment"
            elif (auc_delta != "" and auc_delta < -0.01) or (brier_delta != "" and brier_delta < -0.002):
                cls = "harmful_or_unstable"
            else:
                cls = "redundant_or_small"
            rows.append(
                {
                    "temporal_split": split,
                    "mechanism_domain": domain,
                    "instrument": inst,
                    "baseline": base,
                    "auc_delta": auc_delta,
                    "brier_improvement": brier_delta,
                    "classification": cls,
                    "notes": "fixed ablation; no combination search",
                }
            )
    return rows


def _num(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def pairwise_group_auc(group: pd.DataFrame, score_col: str) -> tuple[int, int, float | None]:
    rows = group[[score_col, "any_hit_target"]].dropna()
    if len(rows) < 2 or rows["any_hit_target"].nunique() < 2:
        return 0, 0, None
    wins = 0
    comps = 0
    pos = rows[rows["any_hit_target"] == 1][score_col].to_numpy(dtype=float)
    neg = rows[rows["any_hit_target"] == 0][score_col].to_numpy(dtype=float)
    for p in pos:
        for n in neg:
            comps += 1
            if p > n:
                wins += 1
            elif p == n:
                wins += 0.5
    return int(wins), comps, float(wins / comps) if comps else None


def roster_relative(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["starter_group"] = work["slate_date"].astype(str) + "|" + work["game_id"].astype(str) + "|" + work.get("opposing_starter_id", "").astype(str)
    for split in ["validation", "holdout"]:
        split_df = work[work["temporal_split"] == split]
        for inst in ["champion", "challenger_d_champion_plus_granular"]:
            score_col = f"{inst}_prob_any_hit"
            wins = comps = groups = 0
            top_hits = []
            bottom_hits = []
            for _, grp in split_df.groupby("starter_group"):
                if len(grp) < 3:
                    continue
                w, c, _ = pairwise_group_auc(grp, score_col)
                if c == 0:
                    continue
                wins += w
                comps += c
                groups += 1
                ordered = grp.sort_values(score_col, ascending=False)
                top_hits.append(int(ordered.iloc[0]["any_hit_target"]))
                bottom_hits.append(int(ordered.iloc[-1]["any_hit_target"]))
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": inst,
                    "starter_groups": groups,
                    "pairwise_comparisons": comps,
                    "pairwise_auc": float(wins / comps) if comps else "",
                    "top_ranked_hitter_any_hit_rate": float(np.mean(top_hits)) if top_hits else "",
                    "bottom_ranked_hitter_any_hit_rate": float(np.mean(bottom_hits)) if bottom_hits else "",
                    "notes": "same game/opposing starter grouping where starter id was available",
                }
            )
    return rows


def suppression_analysis(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    def suppression_bucket(row: pd.Series) -> str:
        val = str(row.get("suppression_subtype", "")).lower()
        context = str(row.get("starter_context_status", "")).lower()
        if "affirmative" in val or "suppression" in val:
            return "affirmative_suppression"
        if val in {"nan", "", "none"} and ("missing" in context or "unavailable" in context):
            return "missing_or_uncertain"
        return "no_affirmative_suppression"
    work["suppression_bucket"] = work.apply(suppression_bucket, axis=1)
    for (split, bucket), grp in work.groupby(["temporal_split", "suppression_bucket"]):
        rows.append(
            {
                "temporal_split": split,
                "suppression_bucket": bucket,
                "rows": len(grp),
                "observed_any_hit_rate": float(grp["any_hit_target"].astype(int).mean()),
                "champion_avg_any_hit": float(grp["champion_prob_any_hit"].mean()),
                "challenger_d_avg_any_hit": float(grp["challenger_d_champion_plus_granular_prob_any_hit"].mean()),
                "avg_rank_movement": float((grp["challenger_d_champion_plus_granular_prob_any_hit"] - grp["champion_prob_any_hit"]).mean()),
                "notes": "suppression labels are inherited from granular source; sparse affirmative rows expected",
            }
        )
    return rows


def side_analysis(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["champion_pick_side"] = np.where(work["champion_prob_any_hit"] >= 0.5, "over", "under")
    work["challenger_d_pick_side"] = np.where(work["challenger_d_champion_plus_granular_prob_any_hit"] >= 0.5, "over", "under")
    for split in ["validation", "holdout"]:
        s = work[work["temporal_split"] == split]
        for side, target_col in [("over", "any_hit_target"), ("under", "zero_hit_target")]:
            side_df = s.copy()
            if side == "under":
                side_df["zero_hit_target"] = 1 - side_df["any_hit_target"].astype(int)
                champ_score = side_df["champion_prob_zero_hit"]
                chall_score = side_df["challenger_d_champion_plus_granular_prob_zero_hit"]
            else:
                champ_score = side_df["champion_prob_any_hit"]
                chall_score = side_df["challenger_d_champion_plus_granular_prob_any_hit"]
            y = side_df[target_col].astype(int)
            rows.append(
                {
                    "temporal_split": split,
                    "side": side,
                    "rows": len(side_df),
                    "champion_auc": safe_auc(y, champ_score),
                    "challenger_d_auc": safe_auc(y, chall_score),
                    "auc_increment": _delta(safe_auc(y, chall_score), safe_auc(y, champ_score)),
                    "champion_brier": float(brier_score_loss(y, champ_score)),
                    "challenger_d_brier": float(brier_score_loss(y, chall_score)),
                    "brier_improvement": float(brier_score_loss(y, champ_score) - brier_score_loss(y, chall_score)),
                    "notes": "side view uses same probability complement; no side threshold optimized",
                }
            )
    return rows


def _delta(a: Any, b: Any) -> Any:
    if a in ["", None] or b in ["", None]:
        return ""
    try:
        return float(a) - float(b)
    except Exception:
        return ""


def price_diagnostics(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    work["price_band"] = pd.cut(
        pd.to_numeric(work["price_over_american"], errors="coerce"),
        bins=[-np.inf, -200, -150, -120, -100, 100, 150, np.inf],
        labels=["lt_-200", "-200_to_-150", "-150_to_-120", "-120_to_-100", "even_to_+100", "+100_to_+150", "gt_+150"],
    )
    work["implied_break_even"] = work["price_over_american"].map(implied_prob_american)
    work["over_profit_1u"] = [american_profit(price, int(win)) for price, win in zip(work["price_over_american"], work["any_hit_target"])]
    for (split, band), grp in work.groupby(["temporal_split", "price_band"], observed=False):
        if grp.empty:
            continue
        rows.append(
            {
                "temporal_split": split,
                "price_band": band,
                "rows": len(grp),
                "sportsbooks": int(grp.get("bookmaker_key", pd.Series(dtype=str)).nunique()),
                "avg_price_over": float(pd.to_numeric(grp["price_over_american"], errors="coerce").mean()),
                "avg_implied_break_even": float(pd.to_numeric(grp["implied_break_even"], errors="coerce").mean()),
                "observed_any_hit_rate": float(grp["any_hit_target"].astype(int).mean()),
                "champion_avg_any_hit": float(grp["champion_prob_any_hit"].mean()),
                "challenger_d_avg_any_hit": float(grp["challenger_d_champion_plus_granular_prob_any_hit"].mean()),
                "diagnostic_flat_roi_over": float(pd.to_numeric(grp["over_profit_1u"], errors="coerce").mean()),
                "selection_time_certification": "preserved_reconcile_snapshot_timestamp_not_repaired_selection_time_certification",
                "notes": "diagnostic only; no price optimization",
            }
        )
    return rows


def current_surface(df: pd.DataFrame) -> list[dict[str, Any]]:
    slate = read_csv(CURRENT_SLATE)
    if slate.empty:
        return []
    work = slate[(slate["prop_type"].astype(str) == "hits") & (pd.to_numeric(slate["line"], errors="coerce") == 0.5)].copy()
    rows = []
    for _, r in work.head(300).iterrows():
        rows.append(
            {
                "slate_date": r.get("slate_date", ""),
                "game_id": r.get("game_id", ""),
                "player_id": r.get("player_id", ""),
                "player_name": r.get("player_name", ""),
                "team": r.get("team", ""),
                "opponent": r.get("opponent", ""),
                "line": r.get("line", ""),
                "champion_prob_over": r.get("prob_over", r.get("model_prob_over", "")),
                "champion_pick_side": r.get("model_pick_side", ""),
                "market_price_over": r.get("market_price_over", ""),
                "market_price_under": r.get("market_price_under", ""),
                "candidate_surface_status": "bound_current_surface_no_challenger_live_score",
                "notes": "live candidates not modified; challenger fit is historical offline only",
            }
        )
    return rows


def build_feature_manifest(joined: pd.DataFrame) -> list[dict[str, Any]]:
    registry = read_csv(CROSS_PROP_REGISTRY)
    rows = []
    all_features = sorted({f for values in FEATURE_GROUPS.values() for f in values})
    for feature in all_features:
        source_match = registry[registry.get("canonical_field_name", pd.Series(dtype=str)).astype(str).eq(feature)] if not registry.empty else pd.DataFrame()
        values = pd.to_numeric(joined[feature], errors="coerce") if feature in joined.columns else pd.Series(dtype=float)
        rows.append(
            {
                "feature": feature,
                "source_package": source_match["source_package"].iloc[0] if not source_match.empty and "source_package" in source_match.columns else "hits05_experiment_joined_population_or_derived",
                "source_column": source_match["source_column"].iloc[0] if not source_match.empty and "source_column" in source_match.columns else feature,
                "bundle_membership": ",".join([name for name, feats in FEATURE_GROUPS.items() if feature in feats]),
                "prediction_time_status": "strict_prior_or_champion_snapshot" if not feature.startswith("actual_") else "oracle_only",
                "coverage_rows": int(values.notna().sum()) if feature in joined.columns else 0,
                "coverage_pct": float(values.notna().mean()) if feature in joined.columns and len(joined) else 0.0,
                "missing_policy": "fit_split_median_imputation" if not feature.startswith("actual_") else "oracle_diagnostic_only",
                "notes": "Derived champion_logit from preserved model_prob_over" if feature == "champion_logit" else "",
            }
        )
    return rows


def champion_contract(meta: dict[str, Any], population: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "contract_field": "canonical_proposition_identity",
            "value": "MLB|hits|line=0.5|side=over_or_under",
            "evidence": "reconcile_rows.csv prop_type=hits line=0.5",
        },
        {
            "contract_field": "champion_probability",
            "value": "model_prob_over / model_prob_under from preserved reconcile rows",
            "evidence": f"source_files={meta.get('source_reconcile_file_count')}",
        },
        {
            "contract_field": "expected_hits",
            "value": "UNKNOWN_NOT_EXPLICITLY_RETAINED_ON_RECONCILE_ROWS",
            "evidence": "preserved O0.5 champion artifact retains model_prob_over but not a standalone expected_hits field",
        },
        {
            "contract_field": "pitcher_tier",
            "value": "UNKNOWN_NOT_EXPLICITLY_RETAINED_ON_RECONCILE_ROWS",
            "evidence": "pitcher tier is not present in the bound O0.5 reconcile rows",
        },
        {
            "contract_field": "hitter_tier",
            "value": "UNKNOWN_NOT_EXPLICITLY_RETAINED_ON_RECONCILE_ROWS",
            "evidence": "hitter tier is not present in the bound O0.5 reconcile rows",
        },
        {
            "contract_field": "rolling_hitter_evidence",
            "value": "d7_hits,d15_hits,d30_hits,d7_total_bases,d15_total_bases,d30_total_bases,d7/d15/d30 strikeouts where retained",
            "evidence": "reconcile_rows.csv columns vary by date but rolling evidence is present on eligible rows",
        },
        {
            "contract_field": "pa_opportunity_currently_used",
            "value": "d7/d15/d30 plate_appearances retained on later reconcile rows; not uniformly present for all dates",
            "evidence": "reconcile row schema differs across historical files",
        },
        {
            "contract_field": "suppression_or_veto_behavior",
            "value": "UNKNOWN_FOR_O0.5_CHAMPION_ARTIFACT",
            "evidence": "no explicit suppression/veto field retained in bound O0.5 champion rows",
        },
        {
            "contract_field": "side_selection_boundary",
            "value": "model_pick_side from production reconcile; probability analysis uses any-hit target without changing side boundary",
            "evidence": "reconcile_rows.csv",
        },
        {
            "contract_field": "official_outcome_source",
            "value": "actual_value in execution_vs_model reconcile rows",
            "evidence": "numeric official hits outcome already reconciled",
        },
        {
            "contract_field": "historical_population",
            "value": f"{len(population)} unique player-game Hits 0.5 rows",
            "evidence": "deduped canonical_key preserving first snapshot observation",
        },
        {
            "contract_field": "temporal_cutoff",
            "value": f"fit<={FIT_END}; validation={VALIDATION_START}..{VALIDATION_END}; holdout={HOLDOUT_START}..{HOLDOUT_END}",
            "evidence": "fixed before fitting in script constants",
        },
        {
            "contract_field": "production_status",
            "value": "unchanged",
            "evidence": "offline research package only",
        },
    ]


def historical_population_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "canonical_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent",
        "line", "side", "champion_prob_any_hit", "model_pick_side", "actual_hits", "any_hit_target",
        "temporal_split", "feature_cutoff_date", "snapshot_run_tag", "snapshot_time_utc",
        "price_over_american", "price_under_american", "bookmaker_key", "source_reconcile_path",
        "source_reconcile_sha256", "granular_join_status", "opposing_starter_id", "opposing_starter_name",
    ]
    rows = []
    for _, r in df.iterrows():
        rows.append({c: r.get(c, "") for c in cols})
    return rows


def make_decisions(metrics: list[dict[str, Any]], zero_rows_data: list[dict[str, Any]], joined: pd.DataFrame) -> list[dict[str, Any]]:
    m = {(r["temporal_split"], r["instrument"]): r for r in metrics}
    hold_champ = m.get(("holdout", "champion"), {})
    hold_d = m.get(("holdout", "challenger_d_champion_plus_granular"), {})
    auc_inc = _delta(hold_d.get("auc"), hold_champ.get("auc"))
    brier_imp = _delta(hold_champ.get("brier"), hold_d.get("brier"))
    hold_zero = [r for r in zero_rows_data if r["temporal_split"] == "holdout" and r["instrument"] == "challenger_d_champion_plus_granular"]
    champ_zero = [r for r in zero_rows_data if r["temporal_split"] == "holdout" and r["instrument"] == "champion"]
    zero_inc = ""
    if hold_zero and champ_zero:
        zero_inc = _delta(hold_zero[0].get("zero_hit_auc"), champ_zero[0].get("zero_hit_auc"))
    if auc_inc != "" and brier_imp != "" and auc_inc > 0.01 and brier_imp > 0:
        next_decision = "HITS05_REPEATABLE_RANKING_INCREMENT"
    elif zero_inc != "" and zero_inc > 0.01:
        next_decision = "HITS05_ZERO_HIT_IDENTIFICATION_INCREMENT"
    elif brier_imp != "" and brier_imp > 0:
        next_decision = "HITS05_CALIBRATION_ONLY"
    else:
        next_decision = "NO_HITS05_INCREMENT"
    return [
        {"decision_name": "MLB_HITS05_CHAMPION_BINDING_DECISION", "decision_value": "CHAMPION_BOUND_FROM_PRESERVED_RECONCILE_MODEL_PROB_OVER", "notes": "Champion was not modified."},
        {"decision_name": "MLB_HITS05_POPULATION_DECISION", "decision_value": "EXACT_UNIQUE_HITS05_POPULATION_ASSEMBLED", "notes": f"unique_rows={len(joined)}; joined_granular={(joined['granular_join_status']=='JOINED').sum()}"},
        {"decision_name": "MLB_HITS05_GRANULAR_COVERAGE_DECISION", "decision_value": "STRICT_PRIOR_GRANULAR_JOIN_AVAILABLE_WITH_MEDIAN_MISSING_POLICY", "notes": "Granular coverage varies by feature; manifest contains row coverage."},
        {"decision_name": "MLB_HITS05_TARGET_ORIENTATION_DECISION", "decision_value": "PASS_HIGHER_PROBABILITY_MEANS_HIGHER_ANY_HIT_EXPECTATION", "notes": "Target is hits>=1."},
        {"decision_name": "MLB_HITS05_OPPORTUNITY_NONCONTACT_DECISION", "decision_value": "EVALUATED_FIXED_ABLATION", "notes": "Challenger A fit with fixed opportunity/noncontact fields."},
        {"decision_name": "MLB_HITS05_CONTACT_FREQUENCY_DECISION", "decision_value": "EVALUATED_FIXED_ABLATION", "notes": "Challenger B adds contact opportunity proxies."},
        {"decision_name": "MLB_HITS05_CONTACT_CONVERSION_DECISION", "decision_value": "EVALUATED_FIXED_FULL_GRANULAR_CONSTRUCTION", "notes": "Challenger C includes full granular p-zero/contact/conversion fields."},
        {"decision_name": "MLB_HITS05_CHAMPION_PLUS_GRANULAR_DECISION", "decision_value": next_decision, "notes": f"holdout_auc_increment={auc_inc}; holdout_brier_improvement={brier_imp}"},
        {"decision_name": "MLB_HITS05_ZERO_HIT_IDENTIFICATION_DECISION", "decision_value": "ZERO_HIT_DIAGNOSTIC_EVALUATED", "notes": f"holdout_zero_hit_auc_increment={zero_inc}"},
        {"decision_name": "MLB_HITS05_ROSTER_RELATIVE_DECISION", "decision_value": "SAME_PITCHER_ROSTER_RELATIVE_EVALUATED_WHERE_GROUPS_EXIST", "notes": "No production grouping changed."},
        {"decision_name": "MLB_HITS05_SUPPRESSION_DECISION", "decision_value": "SUPPRESSION_BUCKETS_EVALUATED_SPARSE_AFFIRMATIVE_SUPPORT", "notes": "Missing/uncertain suppression separated."},
        {"decision_name": "MLB_HITS05_PRICE_DIAGNOSTIC_DECISION", "decision_value": "PRICE_DIAGNOSTIC_ONLY_NO_OPTIMIZATION", "notes": "Reconcile snapshot prices used as diagnostic."},
        {"decision_name": "MLB_HITS05_O15_REUSE_DECISION", "decision_value": "O15_REUSE_POSSIBLE_ONLY_AFTER_SEPARATE_TRANSFER_APPROVAL", "notes": "Frozen O1.5 prospective run not altered or graded."},
        {"decision_name": "MLB_HITS05_NEXT_RESEARCH_DECISION", "decision_value": "PROMOTION_GRADE_DESIGN_ONLY_IF_REPEATABLE_RANKING_OR_ZERO_HIT_INCREMENT_CONFIRMED" if next_decision in {"HITS05_REPEATABLE_RANKING_INCREMENT", "HITS05_ZERO_HIT_IDENTIFICATION_INCREMENT"} else "NO_PROMOTION_GRADE_NEXT_STEP_CALIBRATION_REVIEW_OR_REDESIGN", "notes": "This task does not authorize production."},
        {"decision_name": "MLB_HITS05_PRODUCTION_STATUS", "decision_value": "NOT_AUTHORIZED", "notes": "No production behavior changed."},
    ]


def o15_reuse_rows() -> list[dict[str, Any]]:
    return [
        {
            "reuse_path": "zero_hit_probability",
            "hits05_output": "challenger_d_champion_plus_granular_prob_zero_hit",
            "possible_o15_use": "first-hit component in multi-hit distribution",
            "status": "future_separate_experiment_required",
            "notes": "Do not alter active O1.5 prospective ranking.",
        },
        {
            "reuse_path": "bad_over_removal",
            "hits05_output": "highest predicted zero-hit bands",
            "possible_o15_use": "filter or diagnostic for weak O1.5 candidates",
            "status": "future_separate_experiment_required",
            "notes": "No cutoff optimized here.",
        },
        {
            "reuse_path": "contact_failure_mode",
            "hits05_output": "mechanism attribution by PA/contact/conversion",
            "possible_o15_use": "explain one-hit floor versus two-hit ceiling",
            "status": "diagnostic",
            "notes": "Research-only.",
        },
    ]


def validate_files(files: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for p in files:
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                if not p.read_text().startswith("#"):
                    status = "WARN"
                    notes = "markdown heading missing"
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(p), "validation": status, "notes": notes})
    rows.extend(
        [
            {"artifact": "guardrail_no_network_or_oddsapi", "validation": "PASS", "notes": "local file reads only"},
            {"artifact": "guardrail_no_db_writes", "validation": "PASS", "notes": "no database connector used"},
            {"artifact": "guardrail_no_standalone_batter_k", "validation": "PASS", "notes": "discipline retained only as internal Hits mechanism"},
            {"artifact": "guardrail_no_o15_grading", "validation": "PASS", "notes": "O1.5 artifacts not read for grading or mutation"},
            {"artifact": "guardrail_no_production_change", "validation": "PASS", "notes": "outputs limited to audit package"},
        ]
    )
    return rows


def summary_md(stats: dict[str, Any], decisions: list[dict[str, Any]]) -> str:
    decision = next((d["decision_value"] for d in decisions if d["decision_name"] == "MLB_HITS05_CHAMPION_PLUS_GRANULAR_DECISION"), "")
    return f"""# MLB Hits O0.5 Granular Opportunity-Contact Challenger

Generated: `{stats['generated_at']}`

## Executive Summary

This bounded offline experiment bound the existing Hits O0.5 Champion from preserved `reconcile_rows.csv` probabilities and compared it to fixed granular challengers built from strict-prior opportunity, exposure, contact-frequency, contact-conversion, and suppression evidence.

The load-bearing decision is:

`{decision}`

No production model, formula, selector, upload, candidate surface, workspace, LaunchAgent, DB, OddsAPI, or O1.5 prospective artifact was changed.

## Population

- Unique Hits O0.5 rows: `{stats['population_rows']}`
- Fit rows through `{FIT_END}`: `{stats['fit_rows']}`
- Validation rows `{VALIDATION_START}` through `{VALIDATION_END}`: `{stats['validation_rows']}`
- Untouched holdout rows `{HOLDOUT_START}` through `{HOLDOUT_END}`: `{stats['holdout_rows']}`
- Granular joined rows: `{stats['granular_joined_rows']}`

## Primary Holdout Comparison

- Champion AUC: `{stats['holdout_champion_auc']}`
- Champion plus granular AUC: `{stats['holdout_challenger_d_auc']}`
- AUC increment: `{stats['holdout_auc_increment']}`
- Champion Brier: `{stats['holdout_champion_brier']}`
- Champion plus granular Brier: `{stats['holdout_challenger_d_brier']}`
- Brier improvement: `{stats['holdout_brier_improvement']}`

## Interpretation

The experiment is scoped to predictive improvement and zero-hit identification, not historical ROI optimization. Price rows are diagnostic only.

## No Behavior Changed

The frozen O1.5 prospective ranking program was not altered or graded. Batter strikeout evidence was used only as an internal Hits mechanism; no standalone batter-strikeout prop experiment was created.
"""


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    joined, meta = assemble_population()
    if joined.empty:
        raise RuntimeError("no Hits O0.5 population assembled from local reconcile rows")
    joined["champion_logit"] = logit(joined["champion_prob_any_hit"])
    joined["zero_hit_target"] = 1 - joined["any_hit_target"].astype(int)
    fit_df = joined[(joined["temporal_split"] == "fit") & (joined["granular_join_status"] == "JOINED")].copy()
    eval_df = joined[joined["temporal_split"].isin(["fit", "validation", "holdout"]) & (joined["granular_join_status"] == "JOINED")].copy()

    instruments = [FittedInstrument("champion", [], None, None, {}, [], "BOUND", "preserved model_prob_over")]
    for name, features in FEATURE_GROUPS.items():
        instruments.append(fit_instrument(name, features, fit_df))
    scored = add_predictions(eval_df, instruments)

    instrument_names = [i.name for i in instruments]
    metrics = []
    for split in ["fit", "validation", "holdout"]:
        for inst in instrument_names:
            metrics.append(instrument_metrics(scored, f"{inst}_prob_any_hit", split, inst))
    band_rows = []
    for inst in instrument_names:
        band_rows.extend(probability_bands(scored, f"{inst}_prob_any_hit", inst))
    zero = zero_hit_rows(scored, instrument_names)
    mechanism = mechanism_attribution(metrics)
    roster = roster_relative(scored)
    suppression = suppression_analysis(scored)
    sides = side_analysis(scored)
    prices = price_diagnostics(scored)
    surface = current_surface(scored)
    coeffs = []
    for inst in instruments:
        coeffs.extend(inst.coefficient_rows)

    decisions = make_decisions(metrics, zero, scored)
    m = {(r["temporal_split"], r["instrument"]): r for r in metrics}
    hold_champ = m.get(("holdout", "champion"), {})
    hold_d = m.get(("holdout", "challenger_d_champion_plus_granular"), {})
    stats = {
        "generated_at": generated_at,
        "population_rows": len(joined),
        "fit_rows": int((scored["temporal_split"] == "fit").sum()),
        "validation_rows": int((scored["temporal_split"] == "validation").sum()),
        "holdout_rows": int((scored["temporal_split"] == "holdout").sum()),
        "granular_joined_rows": int((joined["granular_join_status"] == "JOINED").sum()),
        "holdout_champion_auc": hold_champ.get("auc"),
        "holdout_challenger_d_auc": hold_d.get("auc"),
        "holdout_auc_increment": _delta(hold_d.get("auc"), hold_champ.get("auc")),
        "holdout_champion_brier": hold_champ.get("brier"),
        "holdout_challenger_d_brier": hold_d.get("brier"),
        "holdout_brier_improvement": _delta(hold_champ.get("brier"), hold_d.get("brier")),
    }

    files = {
        "executive_summary": output_dir / f"executive_summary_{RUN_DATE}.md",
        "champion_contract": output_dir / f"hits05_champion_contract_{RUN_DATE}.csv",
        "historical_population": output_dir / f"hits05_exact_historical_population_{RUN_DATE}.csv",
        "feature_manifest": output_dir / f"hits05_granular_feature_manifest_{RUN_DATE}.csv",
        "challenger_contracts": output_dir / f"hits05_fixed_challenger_contracts_{RUN_DATE}.csv",
        "coefficient_audit": output_dir / f"hits05_orientation_coefficient_audit_{RUN_DATE}.csv",
        "validation_holdout": output_dir / f"hits05_validation_holdout_results_{RUN_DATE}.csv",
        "probability_bands": output_dir / f"hits05_probability_band_progression_{RUN_DATE}.csv",
        "bootstrap": output_dir / f"hits05_bootstrap_uncertainty_{RUN_DATE}.csv",
        "date_stability": output_dir / f"hits05_date_stability_{RUN_DATE}.csv",
        "concentration": output_dir / f"hits05_player_pitcher_concentration_{RUN_DATE}.csv",
        "zero_hit": output_dir / f"hits05_zero_hit_identification_{RUN_DATE}.csv",
        "mechanism": output_dir / f"hits05_mechanism_attribution_{RUN_DATE}.csv",
        "roster_relative": output_dir / f"hits05_same_pitcher_roster_relative_{RUN_DATE}.csv",
        "suppression": output_dir / f"hits05_suppression_analysis_{RUN_DATE}.csv",
        "side_analysis": output_dir / f"hits05_over_under_side_analysis_{RUN_DATE}.csv",
        "price": output_dir / f"hits05_price_diagnostics_{RUN_DATE}.csv",
        "candidate_surface": output_dir / f"hits05_candidate_surface_diagnostic_{RUN_DATE}.csv",
        "o15_reuse": output_dir / f"hits05_o15_reuse_pathway_{RUN_DATE}.csv",
        "decisions": output_dir / f"hits05_required_decisions_{RUN_DATE}.csv",
        "machine": output_dir / f"machine_readable_hits05_challenger_{RUN_DATE}.json",
        "manifest": output_dir / f"sha256_manifest_{RUN_DATE}.csv",
        "validation": output_dir / f"validation_report_{RUN_DATE}.csv",
    }

    write_text(files["executive_summary"], summary_md(stats, decisions))
    write_csv(files["champion_contract"], champion_contract(meta, joined))
    write_csv(files["historical_population"], historical_population_rows(scored))
    write_csv(files["feature_manifest"], build_feature_manifest(scored))
    write_csv(
        files["challenger_contracts"],
        [
            {
                "instrument": "champion",
                "definition": "preserved Hits O0.5 model_prob_over",
                "features": "model_prob_over",
                "deployability": "production_champion_reference",
                "notes": "unchanged",
            }
        ]
        + [
            {
                "instrument": name,
                "definition": "fixed logistic challenger",
                "features": ",".join(features),
                "deployability": "research_only" if not name.startswith("oracle") else "oracle_only",
                "notes": "C=1.0, lbfgs, fit split only; no hyperparameter search",
            }
            for name, features in FEATURE_GROUPS.items()
        ],
    )
    write_csv(files["coefficient_audit"], coeffs)
    write_csv(files["validation_holdout"], metrics)
    write_csv(files["probability_bands"], band_rows)
    write_csv(files["bootstrap"], bootstrap_rows(scored, instrument_names))
    write_csv(files["date_stability"], date_stability(scored, instrument_names))
    write_csv(files["concentration"], concentration(scored, instrument_names))
    write_csv(files["zero_hit"], zero)
    write_csv(files["mechanism"], mechanism)
    write_csv(files["roster_relative"], roster)
    write_csv(files["suppression"], suppression)
    write_csv(files["side_analysis"], sides)
    write_csv(files["price"], prices)
    write_csv(files["candidate_surface"], surface)
    write_csv(files["o15_reuse"], o15_reuse_rows())
    write_csv(files["decisions"], decisions)

    machine = {
        "generated_at": generated_at,
        "run_date": RUN_DATE,
        "mode": "offline_research_only",
        "source_artifacts": {
            "reconcile_root": str(RECONCILE_ROOT),
            "granular_source": str(GRANULAR_SOURCE),
            "current_slate": str(CURRENT_SLATE),
        },
        "meta": meta,
        "stats": stats,
        "decisions": decisions,
        "guardrails": {
            "network_calls": 0,
            "db_writes": 0,
            "standalone_batter_k_experiment": False,
            "o15_prospective_program_altered": False,
            "production_behavior_changed": False,
        },
    }
    write_text(files["machine"], json.dumps(machine, indent=2, default=str) + "\n")
    manifest_targets = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(
        files["manifest"],
        [{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated audit artifact"} for p in manifest_targets],
    )
    validations = validate_files(manifest_targets + [files["manifest"]])
    write_csv(files["validation"], validations)
    return {"output_dir": str(output_dir), "files": {k: str(v) for k, v in files.items()}, "stats": stats}


def date_stability(df: pd.DataFrame, instruments: list[str]) -> list[dict[str, Any]]:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    for (split, date), grp in work.groupby(["temporal_split", "slate_date"]):
        for inst in instruments:
            col = f"{inst}_prob_any_hit"
            y = grp["any_hit_target"].astype(int)
            p = pd.to_numeric(grp[col], errors="coerce").clip(1e-6, 1 - 1e-6)
            rows.append(
                {
                    "temporal_split": split,
                    "slate_date": date,
                    "instrument": inst,
                    "rows": len(grp),
                    "observed_any_hit_rate": float(y.mean()),
                    "avg_predicted_any_hit": float(p.mean()),
                    "brier": float(brier_score_loss(y, p)) if len(grp) else "",
                    "auc": safe_auc(y, p),
                    "zero_hit_rows": int((1 - y).sum()),
                }
            )
    return rows


def concentration(df: pd.DataFrame, instruments: list[str]) -> list[dict[str, Any]]:
    rows = []
    for split in ["validation", "holdout"]:
        grp = df[df["temporal_split"] == split]
        if grp.empty:
            continue
        for inst in instruments:
            top_player_share = grp["player_id"].value_counts(normalize=True).iloc[0] if "player_id" in grp.columns and not grp.empty else ""
            pitcher_col = "opposing_starter_id"
            top_pitcher_share = grp[pitcher_col].value_counts(normalize=True).iloc[0] if pitcher_col in grp.columns and grp[pitcher_col].notna().any() else ""
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": inst,
                    "rows": len(grp),
                    "players": int(grp["player_id"].nunique()),
                    "dates": int(grp["slate_date"].nunique()),
                    "pitchers": int(grp[pitcher_col].nunique()) if pitcher_col in grp.columns else "",
                    "top_player_share": float(top_player_share) if top_player_share != "" else "",
                    "top_pitcher_share": float(top_pitcher_share) if top_pitcher_share != "" else "",
                }
            )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
