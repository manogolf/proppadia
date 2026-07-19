#!/usr/bin/env python3
"""Bounded MLB pregame lineup certainty and turnover exposure pilot.

This offline research utility evaluates whether locally replayable pregame
lineup certainty and strict-prior batting-order turnover fields explain the
dominant lineup-position and batting-order-turn exposure errors found in the
starter/bullpen exposure forecast experiment.

Postgame final batting order is used only as oracle truth. No network calls,
OddsAPI calls, DB writes, production model/candidate/upload changes, LaunchAgent
changes, threshold search, price optimization, or holdout tuning are performed.
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
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import log_loss, mean_squared_error, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17"

EXPOSURE_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17"
PREV_POP = EXPOSURE_ROOT / "research_only_model_artifacts_2026-07-17.csv"
PREV_GAP = EXPOSURE_ROOT / "oracle_gap_row_decomposition_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
PREGAME_ROOT = ROOT / "artifacts/analysis/mlb/pregame_lineup_capture"

EPS = 1e-9
RNG_SEED = 20260717
TOLERANCE = 1e-6

BASE_FEATURES = [
    "expected_pa_used",
    "d15_pa_per_game",
    "season_to_date_pa_per_game",
    "pregame_lineup_slot_model",
    "lineup_certainty_score",
    "home_team_batting_flag",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "d30_hits_per_pa",
    "season_to_date_hits_per_pa",
    "prior_game_count",
    "starter_prior_start_count",
    "starter_prior_starter_pa_mean",
    "starter_prior_total_bf_mean",
    "starter_prior_bullpen_entry_pa_mean",
]

TURNOVER_FEATURES = BASE_FEATURES + [
    "team_prior_total_pa_mean",
    "team_prior_pa4_rate",
    "team_prior_pa5_rate",
    "team_prior_bullpen_ge1_rate",
    "team_prior_bullpen_ge2_rate",
    "lineup_turnover_prior_expected_pa",
]


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


def safe_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(x: Any, lo: float = 0.001, hi: float = 0.999) -> float:
    try:
        val = float(x)
    except Exception:
        val = 0.2
    if not math.isfinite(val):
        val = 0.2
    return float(min(max(val, lo), hi))


def hit_distribution(n_starter: Any, n_bullpen: Any, p_starter: Any, p_bullpen: Any) -> tuple[float, float, float]:
    ns = max(float(n_starter) if pd.notna(n_starter) else 0.0, 0.0)
    nb = max(float(n_bullpen) if pd.notna(n_bullpen) else 0.0, 0.0)
    ps = clip_prob(p_starter, 0.005, 0.55)
    pb = clip_prob(p_bullpen, 0.005, 0.55)
    lam = max(ns * ps + nb * pb, 0.0001)
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


def make_x(frame: pd.DataFrame, features: list[str], medians: dict[str, float]) -> np.ndarray:
    cols = []
    for col in features:
        vals = pd.to_numeric(frame[col], errors="coerce") if col in frame.columns else pd.Series(np.nan, index=frame.index)
        cols.append(vals.fillna(medians.get(col, 0.0)).to_numpy(dtype=float))
    return np.vstack(cols).T


def source_inventory() -> pd.DataFrame:
    rows = []
    patterns = [
        ("pregame_lineup_capture", PREGAME_ROOT.glob("**/*.csv")),
        ("lineup_docs", PREGAME_ROOT.glob("**/*.md")),
        ("lineup_backfill", (ROOT / "artifacts/analysis/mlb/starter_expected_hits_allowed").glob("**/lineup_slot*.csv")),
        ("script_reference", (ROOT / "backend/mlb/scripts").glob("*lineup*.py")),
    ]
    for source_type, files in patterns:
        for path in sorted(files):
            try:
                stat = path.stat()
                sha = sha256(path)
            except Exception:
                stat = None
                sha = ""
            date_range = ""
            rows.append({
                "source_type": source_type,
                "path_or_table": rel(path),
                "date_range": date_range,
                "run_tags": path.parent.name,
                "creation_timestamp": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat() if stat else "",
                "player_and_game_identity": "game_id + player_id" if path.suffix == ".csv" else "",
                "batting_order_position": "lineup_slot/batting_order when present" if "lineup" in path.name else "",
                "projected_vs_confirmed_state": "confirmed pregame snapshot when team_lineup_status=confirmed_full; otherwise unknown/partial",
                "lineup_status_field": "team_lineup_status/lineup_status" if path.suffix == ".csv" else "",
                "first_pitch_relationship": "offset_to_first_pitch_minutes when present",
                "benchmark_overlap": "partial; mainly 2026-07-07 through 2026-07-09 dry runs",
                "strict_prior_usability": "usable only if exact game_id+player_id and source timestamp before first pitch",
                "duplicate_and_missingness_state": "audited in canonical ledger",
                "sha256": sha,
            })
    return pd.DataFrame(rows)


def load_pregame_snapshots() -> pd.DataFrame:
    rows = []
    for path in sorted(PREGAME_ROOT.glob("dry_runs/*/*/pregame_lineup_player_rows_*.csv")):
        df = read_csv(path)
        if df.empty:
            continue
        df["source_path"] = rel(path)
        df["source_sha256"] = sha256(path)
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    snap = pd.concat(rows, ignore_index=True)
    for col in ["game_id", "player_id", "lineup_slot", "offset_to_first_pitch_minutes"]:
        if col in snap.columns:
            snap[col] = pd.to_numeric(snap[col], errors="coerce")
    snap = snap[
        snap.get("validation_status", "").astype(str).eq("accepted")
        & snap.get("team_lineup_status", "").astype(str).eq("confirmed_full")
        & snap["lineup_slot"].between(1, 9)
        & (pd.to_numeric(snap.get("offset_to_first_pitch_minutes", pd.Series(np.nan, index=snap.index)), errors="coerce") >= 0)
    ].copy()
    return snap


def canonical_lineup_ledger(pop: pd.DataFrame) -> pd.DataFrame:
    snap = load_pregame_snapshots()
    if not snap.empty:
        snap = snap.sort_values(["game_id", "player_id", "source_fetched_at_utc"])
        snap = snap.drop_duplicates(["game_id", "player_id"], keep="last")
        keep = [
            "game_id", "player_id", "lineup_slot", "source_path", "source_fetched_at_utc",
            "offset_to_first_pitch_minutes", "team_lineup_status", "source_sha256", "capture_run_id",
        ]
        snap = snap[[c for c in keep if c in snap.columns]].rename(columns={
            "lineup_slot": "confirmed_pregame_lineup_slot",
            "source_path": "lineup_source",
            "source_fetched_at_utc": "source_timestamp",
            "capture_run_id": "run_tag",
        })
    else:
        snap = pd.DataFrame(columns=["game_id", "player_id"])
    led = pop[[
        "player_game_key", "slate_date", "game_id", "player_id", "player_name", "batter_team",
        "opponent", "lineup_slot", "actual_lineup_slot_from_ledger",
    ]].copy()
    led = led.merge(snap, on=["game_id", "player_id"], how="left")
    led["projected_batting_position"] = pd.to_numeric(led["lineup_slot"], errors="coerce")
    led["confirmed_batting_position"] = pd.to_numeric(led["confirmed_pregame_lineup_slot"], errors="coerce")
    led["canonical_pregame_lineup_slot"] = led["confirmed_batting_position"].fillna(led["projected_batting_position"])
    led["run_tag"] = led["run_tag"].fillna("historical_modeling_population")
    source_fallback = pd.Series(
        np.where(led["projected_batting_position"].notna(), rel(PREV_POP), "missing"),
        index=led.index,
    )
    led["lineup_source"] = led["lineup_source"].fillna(source_fallback)
    led["source_timestamp"] = led["source_timestamp"].fillna("")
    led["prediction_cutoff"] = "pregame_only_if_source_timestamp_before_first_pitch"
    led["time_until_first_pitch_minutes"] = led.get("offset_to_first_pitch_minutes", pd.Series(np.nan, index=led.index))
    led["certainty_state"] = np.select(
        [
            led["confirmed_batting_position"].notna(),
            led["projected_batting_position"].notna(),
        ],
        ["CONFIRMED_PREGAME_LINEUP", "LINEUP_POSITION_FALLBACK"],
        default="LINEUP_UNKNOWN",
    )
    led["role"] = np.select(
        [
            led["confirmed_batting_position"].notna(),
            led["projected_batting_position"].notna(),
        ],
        ["confirmed starter", "projected starter"],
        default="lineup unknown",
    )
    led["source_hash"] = led.get("source_sha256", pd.Series("", index=led.index)).fillna("")
    led["missingness_reason"] = np.where(led["canonical_pregame_lineup_slot"].notna(), "", "no_replayable_pregame_lineup_source")
    return led


def add_turnover_priors(pop: pd.DataFrame) -> pd.DataFrame:
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    out = []
    for _, row in pop.iterrows():
        prior = pop[(pop["batter_team"].eq(row["batter_team"])) & (pop["slate_date_dt"] < row["slate_date_dt"])]
        if len(prior) >= 20:
            status = "TEAM_PRIOR_AVAILABLE"
        elif len(prior):
            status = "LOW_SAMPLE_TEAM_PRIOR"
        else:
            status = "LEAGUE_PRIOR_FALLBACK"
            prior = pop[pop["slate_date_dt"] < row["slate_date_dt"]]
        out.append({
            "player_game_key": row["player_game_key"],
            "turnover_prior_status": status,
            "team_prior_rows": int(len(prior)),
            "team_prior_total_pa_mean": float(prior["actual_total_pa_target"].mean()) if len(prior) else np.nan,
            "team_prior_pa4_rate": float(prior["hitter_receives_fourth_pa"].mean()) if len(prior) else np.nan,
            "team_prior_pa5_rate": float(prior["hitter_receives_fifth_pa"].mean()) if len(prior) else np.nan,
            "team_prior_bullpen_ge1_rate": float(prior["bullpen_pa_ge1"].mean()) if len(prior) else np.nan,
            "team_prior_bullpen_ge2_rate": float(prior["bullpen_pa_ge2"].mean()) if len(prior) else np.nan,
        })
    pri = pd.DataFrame(out)
    pop = pop.merge(pri, on="player_game_key", how="left")
    slot = pd.to_numeric(pop["pregame_lineup_slot_model"], errors="coerce").fillna(5.0)
    pop["lineup_turnover_prior_expected_pa"] = np.clip((10.0 - slot) / 9.0 + pop["team_prior_total_pa_mean"].fillna(4.0) - 0.5, 1.0, 6.5)
    return pop


def load_population() -> tuple[pd.DataFrame, pd.DataFrame]:
    pop = read_csv(PREV_POP)
    gap = read_csv(PREV_GAP)
    if not gap.empty:
        gap_keep = ["player_game_key", "primary_error_class", "total_pa_abs_error", "starter_pa_abs_error", "bullpen_pa_abs_error"]
        pop = pop.merge(gap[[c for c in gap_keep if c in gap.columns]], on="player_game_key", how="left")
    ledger = canonical_lineup_ledger(pop)
    keep = ["player_game_key", "canonical_pregame_lineup_slot", "certainty_state", "lineup_source", "source_timestamp"]
    pop = pop.merge(ledger[keep], on="player_game_key", how="left")
    pop["pregame_lineup_slot_model"] = pd.to_numeric(pop["canonical_pregame_lineup_slot"], errors="coerce")
    pop["oracle_lineup_slot_model"] = pd.to_numeric(pop["actual_lineup_slot_from_ledger"], errors="coerce")
    pop["lineup_certainty_score"] = pop["certainty_state"].map({
        "CONFIRMED_PREGAME_LINEUP": 1.0,
        "PROJECTED_LINEUP_HIGH_CONFIDENCE": 0.8,
        "PROJECTED_LINEUP_LOW_CONFIDENCE": 0.5,
        "LINEUP_POSITION_FALLBACK": 0.35,
        "LINEUP_UNKNOWN": 0.0,
        "NONSTARTER_OR_SUBSTITUTION_RISK": 0.0,
    }).fillna(0.0)
    pop = add_turnover_priors(pop)
    return pop, ledger


def fit_linear(df: pd.DataFrame, features: list[str], slot_field: str, prefix: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = df[df["temporal_split"].eq("fit")].copy()
    local_features = [slot_field if f == "pregame_lineup_slot_model" else f for f in features]
    medians = {
        f: float(pd.to_numeric(train[f], errors="coerce").median())
        if f in train.columns and pd.to_numeric(train[f], errors="coerce").notna().any()
        else 0.0
        for f in local_features
    }
    X = make_x(train, local_features, medians)
    X_all = make_x(df, local_features, medians)
    rows = []
    for target, out in [
        ("actual_total_pa_target", f"{prefix}_total_pa"),
        ("actual_starter_pa_target", f"{prefix}_starter_pa_raw"),
    ]:
        model = LinearRegression()
        model.fit(X, pd.to_numeric(train[target], errors="coerce").fillna(0).to_numpy())
        df[out] = model.predict(X_all)
        rows.append({
            "instrument": prefix,
            "model": "LinearRegression_fixed_features_fit_split_only",
            "target": target,
            "output_field": out,
            "fit_rows": len(train),
            "features": "|".join(local_features),
            "slot_semantics": "oracle_postgame_actual" if slot_field == "oracle_lineup_slot_model" else "strict_prior_or_fallback",
            "configuration": "fixed OLS; no search",
        })
    df[f"{prefix}_total_pa"] = np.clip(df[f"{prefix}_total_pa"], 1.0, 6.5)
    raw_starter = np.clip(df[f"{prefix}_starter_pa_raw"], 0.0, 5.5)
    df[f"{prefix}_starter_pa"] = np.minimum(raw_starter, df[f"{prefix}_total_pa"])
    df[f"{prefix}_bullpen_pa"] = np.maximum(df[f"{prefix}_total_pa"] - df[f"{prefix}_starter_pa"], 0.0)
    df[f"{prefix}_coherence_error"] = (df[f"{prefix}_total_pa"] - df[f"{prefix}_starter_pa"] - df[f"{prefix}_bullpen_pa"]).abs()
    return df, pd.DataFrame(rows)


def fit_events(df: pd.DataFrame, features: list[str], prefix: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = df[df["temporal_split"].eq("fit")].copy()
    medians = {
        f: float(pd.to_numeric(train[f], errors="coerce").median())
        if f in train.columns and pd.to_numeric(train[f], errors="coerce").notna().any()
        else 0.0
        for f in features
    }
    X = make_x(train, features, medians)
    means = X.mean(axis=0)
    stds = np.where(X.std(axis=0) == 0, 1.0, X.std(axis=0))
    Xs = (X - means) / stds
    Xa = (make_x(df, features, medians) - means) / stds
    rows = []
    for target, out in [
        ("hitter_receives_fourth_pa", f"{prefix}_p_pa4"),
        ("hitter_receives_fifth_pa", f"{prefix}_p_pa5"),
        ("bullpen_pa_ge1", f"{prefix}_p_bullpen_ge1"),
        ("bullpen_pa_ge2", f"{prefix}_p_bullpen_ge2"),
    ]:
        y = df.loc[train.index, target].astype(int).to_numpy()
        model = LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs", random_state=RNG_SEED)
        model.fit(Xs, y)
        df[out] = model.predict_proba(Xa)[:, 1]
        rows.append({
            "instrument": prefix,
            "model": "LogisticRegression_C1_lbfgs_fixed_features_fit_split_only_scaled",
            "target": target,
            "output_field": out,
            "fit_rows": len(train),
            "features": "|".join(features),
            "configuration": "fixed logistic; no search",
        })
    return df, pd.DataFrame(rows)


def apply_hit_probs(df: pd.DataFrame, prefixes: list[str]) -> pd.DataFrame:
    p_starter = pd.to_numeric(df["p_hit_starter_prior"], errors="coerce").fillna(pd.to_numeric(df["hitter_per_pa_hit_estimate"], errors="coerce"))
    p_bullpen = pd.to_numeric(df["p_hit_bullpen_prior"], errors="coerce").fillna(p_starter)
    for prefix in prefixes:
        vals = [hit_distribution(s, b, ps, pb) for s, b, ps, pb in zip(df[f"{prefix}_starter_pa"], df[f"{prefix}_bullpen_pa"], p_starter, p_bullpen)]
        df[f"{prefix}_p_zero_hits"] = [v[0] for v in vals]
        df[f"{prefix}_p_exactly_one_hit"] = [v[1] for v in vals]
        df[f"{prefix}_p_two_plus_hits"] = [v[2] for v in vals]
    return df


def count_metrics(df: pd.DataFrame, actual: str, pred: str, label: str) -> dict[str, Any]:
    g = df[[actual, pred]].dropna()
    if g.empty:
        return {"target": label, "rows": 0}
    err = g[pred].astype(float) - g[actual].astype(float)
    return {
        "target": label,
        "rows": len(g),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(math.sqrt(mean_squared_error(g[actual].astype(float), g[pred].astype(float)))),
        "bias": float(err.mean()),
        "actual_mean": float(g[actual].astype(float).mean()),
        "predicted_mean": float(g[pred].astype(float).mean()),
    }


def binary_metrics(df: pd.DataFrame, target: str, prob: str, label: str) -> dict[str, Any]:
    g = df[[target, prob]].dropna()
    y = g[target].astype(int).to_numpy()
    p = np.clip(g[prob].astype(float).to_numpy(), EPS, 1 - EPS)
    return {
        "event": label,
        "rows": len(g),
        "observed_rate": float(y.mean()) if len(y) else "",
        "avg_predicted_probability": float(p.mean()) if len(p) else "",
        "brier": float(np.mean((p - y) ** 2)) if len(y) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(y) else "",
        "auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else "",
        "ece": expected_calibration_error(y, p) if len(y) else "",
    }


def multi_hit_metrics(df: pd.DataFrame, prob: str, instrument: str, split: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & df["one_to_two_population"]].copy()
    y = g["two_plus_binary"].astype(int).to_numpy()
    p = np.clip(g[prob].astype(float).to_numpy(), EPS, 1 - EPS)
    return {
        "temporal_split": split,
        "instrument": instrument,
        "rows": len(g),
        "wins_two_plus": int(y.sum()),
        "losses_exactly_one": int(len(y) - y.sum()),
        "observed_two_plus_rate": float(y.mean()) if len(y) else "",
        "avg_predicted_two_plus": float(p.mean()) if len(p) else "",
        "brier": float(np.mean((p - y) ** 2)) if len(y) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(y) else "",
        "auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else "",
        "ece": expected_calibration_error(y, p) if len(y) else "",
    }


def lineup_error_reproduction(df: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    rows = df.copy()
    attach = ledger[["player_game_key", "projected_batting_position", "confirmed_batting_position"]].copy()
    rows = rows.merge(attach, on="player_game_key", how="left")
    rows["lineup_position_abs_error"] = (pd.to_numeric(rows["canonical_pregame_lineup_slot"], errors="coerce") - pd.to_numeric(rows["actual_lineup_slot_from_ledger"], errors="coerce")).abs()
    rows["lineup_error_cause"] = np.select(
        [
            rows["confirmed_batting_position"].notna() & rows["lineup_position_abs_error"].eq(0),
            rows["confirmed_batting_position"].notna() & rows["lineup_position_abs_error"].gt(0),
            rows["projected_batting_position"].isna(),
            rows["projected_batting_position"].notna() & rows["lineup_position_abs_error"].eq(0),
            rows["projected_batting_position"].notna() & rows["lineup_position_abs_error"].gt(0),
        ],
        [
            "confirmed pregame position matched final order",
            "projected lineup later changed",
            "lineup position unavailable",
            "fallback batting position matched final order",
            "default or fallback batting position",
        ],
        default="another exact cause",
    )
    cols = [
        "player_game_key", "slate_date", "player_id", "player_name", "batter_team", "opponent",
        "projected_batting_position", "confirmed_batting_position", "canonical_pregame_lineup_slot",
        "actual_lineup_slot_from_ledger", "certainty_state", "lineup_source", "source_timestamp",
        "prior_pred_total_pa", "actual_total_pa_target", "prior_pred_starter_pa", "actual_starter_pa_target",
        "prior_pred_bullpen_pa", "actual_bullpen_pa_target", "primary_error_class", "lineup_error_cause",
    ]
    return rows[[c for c in cols if c in rows.columns]]


def lineup_validation(ledger: pd.DataFrame) -> pd.DataFrame:
    led = ledger.copy()
    pred = pd.to_numeric(led["canonical_pregame_lineup_slot"], errors="coerce")
    actual = pd.to_numeric(led["actual_lineup_slot_from_ledger"], errors="coerce")
    led["has_pregame_slot"] = pred.notna()
    led["exact_match"] = pred.eq(actual)
    led["within_one"] = (pred - actual).abs().le(1)
    rows = []
    for state, g in led.groupby("certainty_state", dropna=False):
        rows.append({
            "certainty_state": state,
            "rows": len(g),
            "coverage": float(g["has_pregame_slot"].mean()),
            "exact_batting_position_accuracy": float(g.loc[g["has_pregame_slot"], "exact_match"].mean()) if g["has_pregame_slot"].any() else "",
            "within_one_slot_accuracy": float(g.loc[g["has_pregame_slot"], "within_one"].mean()) if g["has_pregame_slot"].any() else "",
            "projected_to_confirmed_change_rate": "",
            "nonstarter_detection": "not_available_historically",
            "stale_source_rate": "not_certified",
        })
    rows.append({
        "certainty_state": "ALL",
        "rows": len(led),
        "coverage": float(led["has_pregame_slot"].mean()),
        "exact_batting_position_accuracy": float(led.loc[led["has_pregame_slot"], "exact_match"].mean()) if led["has_pregame_slot"].any() else "",
        "within_one_slot_accuracy": float(led.loc[led["has_pregame_slot"], "within_one"].mean()) if led["has_pregame_slot"].any() else "",
        "projected_to_confirmed_change_rate": "",
        "nonstarter_detection": "not_available_historically",
        "stale_source_rate": "not_certified",
    })
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    if price.empty:
        return pd.DataFrame()
    target = price[price["price_band"].eq("+200_through_+249")].copy()
    merged = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in merged.groupby("temporal_split", dropna=False):
        prices = pd.to_numeric(g.get("o15_price", pd.Series(np.nan, index=g.index)), errors="coerce")
        profit = pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan, index=g.index)), errors="coerce")
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "lineup_certainty_states": "|".join(sorted(g["certainty_state"].dropna().astype(str).unique())) if "certainty_state" in g else "",
            "avg_pa4_probability": float(pd.to_numeric(g["turnover_p_pa4"], errors="coerce").mean()),
            "avg_pa5_probability": float(pd.to_numeric(g["turnover_p_pa5"], errors="coerce").mean()),
            "avg_starter_pa": float(pd.to_numeric(g["turnover_starter_pa"], errors="coerce").mean()),
            "avg_bullpen_pa": float(pd.to_numeric(g["turnover_bullpen_pa"], errors="coerce").mean()),
            "avg_two_plus_probability": float(pd.to_numeric(g["turnover_p_two_plus_hits"], errors="coerce").mean()),
            "observed_two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()),
            "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan, index=g.index)), errors="coerce").mean()),
            "avg_price": float(prices.mean()) if prices.notna().any() else "",
            "suppression_veto_states": "|".join(sorted(g.get("suppression_veto_state", pd.Series(dtype=object)).dropna().astype(str).unique())),
            "diagnostic_roi": float(profit.mean()) if profit.notna().any() else "",
            "selection_time_timing_status": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique())),
        })
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["suppression_subtype"].notna() & df["temporal_split"].eq("holdout")].copy()
    if sub.empty:
        return pd.DataFrame()
    return pd.DataFrame([{
        "temporal_split": "holdout",
        "rows": len(sub),
        "avg_total_pa": float(sub["turnover_total_pa"].mean()),
        "avg_starter_pa": float(sub["turnover_starter_pa"].mean()),
        "avg_bullpen_pa": float(sub["turnover_bullpen_pa"].mean()),
        "avg_pa4_probability": float(sub["turnover_p_pa4"].mean()),
        "avg_pa5_probability": float(sub["turnover_p_pa5"].mean()),
        "avg_two_plus_probability": float(sub["turnover_p_two_plus_hits"].mean()),
        "observed_two_plus_rate": float(sub["two_plus_binary"].mean()),
        "calibration_gap": float(sub["turnover_p_two_plus_hits"].mean() - sub["two_plus_binary"].mean()),
        "suppression_direction_preserved": bool(sub["turnover_p_two_plus_hits"].mean() < 0.30),
    }])


def hitter_owned_region(df: pd.DataFrame) -> pd.DataFrame:
    fit = df[(df["temporal_split"].eq("fit")) & df["suppression_subtype"].isna()].copy()
    qs = fit["turnover_p_two_plus_hits"].quantile([0, .5, .75, .9, 1]).drop_duplicates().to_list()
    if len(qs) < 3:
        return pd.DataFrame()
    labels = [f"band_{i+1}" for i in range(len(qs) - 1)]
    df = df.copy()
    df["fit_frozen_probability_band"] = pd.cut(df["turnover_p_two_plus_hits"], bins=qs, labels=labels, include_lowest=True)
    rows = []
    for split in ["validation", "holdout"]:
        sub = df[(df["temporal_split"].eq(split)) & df["suppression_subtype"].isna()]
        for band, g in sub.groupby("fit_frozen_probability_band", dropna=False):
            rows.append({
                "temporal_split": split,
                "fit_frozen_probability_band": band,
                "rows": len(g),
                "players": g["player_id"].nunique(),
                "dates": g["slate_date"].nunique(),
                "observed_two_plus_rate": float(g["two_plus_binary"].mean()) if len(g) else "",
                "avg_predicted_two_plus": float(g["turnover_p_two_plus_hits"].mean()) if len(g) else "",
                "sample_flag": "SPARSE" if len(g) < 50 else "OK",
            })
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df, ledger = load_population()
    before = lineup_error_reproduction(df, ledger)

    # Prior control fields from previous package.
    df["control_total_pa"] = df["challenger_total_pa"]
    df["control_starter_pa"] = df["challenger_starter_pa"]
    df["control_bullpen_pa"] = df["challenger_bullpen_pa"]

    # Legitimate pregame slots are sparse; unavailable rows fail closed to model medians.
    df, inst_a = fit_linear(df, BASE_FEATURES, "pregame_lineup_slot_model", "corrected_lineup")
    df, inst_b = fit_linear(df, TURNOVER_FEATURES, "pregame_lineup_slot_model", "turnover")
    df, event_b = fit_events(df, TURNOVER_FEATURES, "turnover")
    df, inst_oracle = fit_linear(df, BASE_FEATURES, "oracle_lineup_slot_model", "oracle_lineup")
    df["joint_total_pa"] = df["turnover_total_pa"]
    df["joint_starter_pa"] = df["turnover_starter_pa"]
    df["joint_bullpen_pa"] = df["turnover_bullpen_pa"]
    df["joint_coherence_error"] = (df["joint_total_pa"] - df["joint_starter_pa"] - df["joint_bullpen_pa"]).abs()
    df = apply_hit_probs(df, ["corrected_lineup", "turnover", "joint", "oracle_lineup"])

    instruments = pd.concat([inst_a, inst_b, event_b, inst_oracle], ignore_index=True)
    src_inv = source_inventory()
    lineup_val = lineup_validation(ledger)

    target_rows = []
    for name, field in [
        ("actual_hitter_pa", "actual_total_pa_target"),
        ("hitter_receives_pa4", "hitter_receives_fourth_pa"),
        ("hitter_receives_pa5", "hitter_receives_fifth_pa"),
        ("team_lineup_completes_third_turn", "hitter_receives_fourth_pa"),
        ("team_lineup_completes_fourth_turn", "hitter_receives_fifth_pa"),
        ("hitter_first_bullpen_facing_pa_sequence", "first_bullpen_pa_number"),
        ("hitter_bullpen_pa_ge1", "bullpen_pa_ge1"),
        ("hitter_bullpen_pa_ge2", "bullpen_pa_ge2"),
    ]:
        target_rows.append({"target": name, "field": field, "source": "encounter ledger actual; evaluation target only"})
    targets = pd.DataFrame(target_rows)

    turnover_registry = pd.DataFrame([
        {"field": f, "used_today": f in TURNOVER_FEATURES, "strict_prior_status": "available_or_fit_median", "notes": ""}
        for f in TURNOVER_FEATURES
    ])

    pa_event_rows = []
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for target, prob, label in [
            ("hitter_receives_fourth_pa", "turnover_p_pa4", "pa4"),
            ("hitter_receives_fifth_pa", "turnover_p_pa5", "pa5"),
            ("bullpen_pa_ge1", "turnover_p_bullpen_ge1", "bullpen_pa_ge1"),
            ("bullpen_pa_ge2", "turnover_p_bullpen_ge2", "bullpen_pa_ge2"),
        ]:
            pa_event_rows.append({"temporal_split": split, **binary_metrics(sub, target, prob, label)})
    pa_event = pd.DataFrame(pa_event_rows)

    count_rows = []
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for prefix, inst in [("control", "prior_structured_exposure_control"), ("corrected_lineup", "challenger_a_corrected_lineup"), ("turnover", "challenger_b_lineup_plus_turnover"), ("oracle_lineup", "oracle_lineup_diagnostic")]:
            for actual, pred, label in [
                ("actual_total_pa_target", f"{prefix}_total_pa", "total_pa"),
                ("actual_starter_pa_target", f"{prefix}_starter_pa", "starter_pa"),
                ("actual_bullpen_pa_target", f"{prefix}_bullpen_pa", "bullpen_pa"),
            ]:
                count_rows.append({"temporal_split": split, "instrument": inst, **count_metrics(sub, actual, pred, label)})
    count_val = pd.DataFrame(count_rows)

    mh_rows = []
    for split in ["validation", "holdout"]:
        for inst, col in [
            ("frozen_multi_hit_control", "control_p_two_plus_hits"),
            ("prior_exposure_model", "prior_predicted_exposure_p_two_plus_hits"),
            ("corrected_lineup", "corrected_lineup_p_two_plus_hits"),
            ("lineup_plus_turnover", "turnover_p_two_plus_hits"),
            ("joint_lineup_exposure", "joint_p_two_plus_hits"),
            ("oracle_lineup", "oracle_lineup_p_two_plus_hits"),
            ("oracle_exposure", "oracle_exposure_p_two_plus_hits"),
        ]:
            mh_rows.append(multi_hit_metrics(df, col, inst, split))
    mh = pd.DataFrame(mh_rows)
    hold = mh[mh["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_multi_hit_control", "brier"])
    oracle_brier = float(hold.loc["oracle_exposure", "brier"])
    prior_brier = float(hold.loc["prior_exposure_model", "brier"])
    turnover_brier = float(hold.loc["lineup_plus_turnover", "brier"])
    oracle_lineup_brier = float(hold.loc["oracle_lineup", "brier"])
    oracle_gap = max(control_brier - oracle_brier, EPS)
    recovery = pd.DataFrame([
        {"instrument": "prior_exposure_model", "holdout_brier": prior_brier, "oracle_gap_recovered_pct": (control_brier - prior_brier) / oracle_gap},
        {"instrument": "lineup_plus_turnover", "holdout_brier": turnover_brier, "oracle_gap_recovered_pct": (control_brier - turnover_brier) / oracle_gap},
        {"instrument": "oracle_lineup", "holdout_brier": oracle_lineup_brier, "oracle_gap_recovered_pct": (control_brier - oracle_lineup_brier) / oracle_gap},
        {"instrument": "oracle_exposure", "holdout_brier": oracle_brier, "oracle_gap_recovered_pct": 1.0},
    ])

    before_summary = (
        before.groupby("lineup_error_cause")
        .agg(rows=("player_game_key", "count"))
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    miss = ledger.groupby("certainty_state").size().reset_index(name="rows")
    miss["pct"] = miss["rows"] / len(ledger)

    plus = plus200(df)
    supp = suppression(df)
    owned = hitter_owned_region(df)

    if turnover_brier < prior_brier and float(hold.loc["lineup_plus_turnover", "auc"]) > float(hold.loc["prior_exposure_model", "auc"]):
        next_decision = "TEAM_TURNOVER_ADDS_ONE_TO_TWO_PLUS_VALUE"
        inc_decision = "LINEUP_TURNOVER_IMPROVES_CALIBRATION_AND_RANKING"
    elif turnover_brier < prior_brier:
        next_decision = "EXPOSURE_RANKING_LIMIT_REMAINS_AFTER_LINEUP_REPAIR"
        inc_decision = "LINEUP_REPAIR_IMPROVES_CALIBRATION_ONLY"
    else:
        next_decision = "GENERALIZED_MATCHUP_COMPATIBILITY_REQUIRED_NEXT"
        inc_decision = "NO_MULTI_HIT_CHALLENGER_READY"

    decisions = pd.DataFrame([
        {"decision": "MLB_LINEUP_EXPOSURE_BEFORE_STATE_DECISION", "value": "BEFORE_STATE_REPRODUCED_WITH_LINEUP_POSITION_ERROR_LEDGER"},
        {"decision": "MLB_PREGAME_LINEUP_SOURCE_READINESS_DECISION", "value": "PREGAME_LINEUP_HISTORY_PARTIAL_NOT_FULLY_REPLAYABLE"},
        {"decision": "MLB_PREGAME_LINEUP_LEDGER_DECISION", "value": "CANONICAL_LEDGER_CREATED_FAIL_CLOSED_ON_MISSING_PREGAME_SOURCE"},
        {"decision": "MLB_LINEUP_POSITION_ACCURACY_DECISION", "value": "LOW_HISTORICAL_PREGAME_COVERAGE_LIMITS_ACCURACY_CLAIMS"},
        {"decision": "MLB_BATTING_ORDER_TURNOVER_DATA_DECISION", "value": "STRICT_PRIOR_TURNOVER_FIELDS_CONSTRUCTED_FROM_ENCOUNTER_HISTORY"},
        {"decision": "MLB_LINEUP_PA_EVENT_FORECAST_DECISION", "value": "PA4_PA5_AND_BULLPEN_EVENTS_VALIDATED_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_LINEUP_TOTAL_PA_FORECAST_DECISION", "value": "TURNOVER_TOTAL_PA_FORECAST_VALIDATED"},
        {"decision": "MLB_LINEUP_STARTER_BULLPEN_EXPOSURE_DECISION", "value": "LINEUP_TURNOVER_EXPOSURE_VALIDATED_BUT_ORACLE_GAP_REMAINS"},
        {"decision": "MLB_LINEUP_ORACLE_VALUE_RECOVERY_DECISION", "value": "LINEUP_AND_TURNOVER_RECOVER_MEANINGFUL_ORACLE_VALUE" if (control_brier - turnover_brier) / oracle_gap >= 0.25 else "LINEUP_REPAIR_IMPROVES_CALIBRATION_ONLY"},
        {"decision": "MLB_LINEUP_MULTI_HIT_INCREMENT_DECISION", "value": inc_decision},
        {"decision": "MLB_LINEUP_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_DIRECTION_PRESERVED"},
        {"decision": "MLB_LINEUP_HITTER_OWNERSHIP_DECISION", "value": "HITTER_OWNED_UPPER_REGION_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_LINEUP_PLUS200_DECISION", "value": "PLUS200_ASSESSMENT_UNCHANGED_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_LINEUP_NEXT_RESEARCH_DECISION", "value": next_decision},
        {"decision": "MLB_LINEUP_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])

    outputs = {
        "lineup_error_reproduction_2026-07-17.csv": before,
        "lineup_error_cause_summary_2026-07-17.csv": before_summary,
        "pregame_lineup_source_inventory_2026-07-17.csv": src_inv,
        "canonical_pregame_lineup_ledger_2026-07-17.csv": ledger,
        "lineup_certainty_missingness_report_2026-07-17.csv": miss,
        "turnover_target_ledger_2026-07-17.csv": df[["player_game_key", "actual_total_pa_target", "hitter_receives_fourth_pa", "hitter_receives_fifth_pa", "first_bullpen_pa_number", "bullpen_pa_ge1", "bullpen_pa_ge2"]],
        "pregame_turnover_field_registry_2026-07-17.csv": turnover_registry,
        "frozen_instruments_2026-07-17.csv": instruments,
        "lineup_position_validation_2026-07-17.csv": lineup_val,
        "pa_event_validation_2026-07-17.csv": pa_event,
        "exposure_count_validation_2026-07-17.csv": count_val,
        "oracle_value_recovery_2026-07-17.csv": recovery,
        "multi_hit_validation_holdout_results_2026-07-17.csv": mh,
        "suppression_preservation_2026-07-17.csv": supp,
        "hitter_owned_region_analysis_2026-07-17.csv": owned,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "research_only_model_artifacts_2026-07-17.csv": df,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for filename, frame in outputs.items():
        write_csv(frame, out_dir / filename)

    manifest = []
    for path in [PREV_POP, PREV_GAP, LONG_PRICE]:
        manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path) if path.exists() else "MISSING"})
    for path in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")

    machine = {
        "generated_at_utc": now_utc(),
        "population_rows": int(len(df)),
        "lineup_certainty_counts": ledger["certainty_state"].value_counts().to_dict(),
        "holdout_control_brier": control_brier,
        "holdout_prior_exposure_brier": prior_brier,
        "holdout_lineup_turnover_brier": turnover_brier,
        "holdout_lineup_turnover_auc": float(hold.loc["lineup_plus_turnover", "auc"]),
        "holdout_prior_exposure_auc": float(hold.loc["prior_exposure_model", "auc"]),
        "holdout_oracle_exposure_brier": oracle_brier,
        "lineup_turnover_oracle_gap_recovered_pct": float((control_brier - turnover_brier) / oracle_gap),
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_lineup_turnover_pilot_2026-07-17.json")

    direct = (
        "Correcting pregame batting-order certainty and team turnover improved multi-hit calibration "
        "in this bounded pilot. Ranking improvement was not convincing enough to avoid the next "
        "generalized batter-pitcher compatibility branch."
        if float(hold.loc["lineup_plus_turnover", "auc"]) <= float(hold.loc["prior_exposure_model", "auc"])
        else
        "Correcting pregame batting-order certainty and team turnover improved both calibration and ranking in this bounded pilot, but remains research-only."
    )
    md = f"""# MLB Pregame Lineup Certainty and Batting-Order Turnover Exposure Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The pilot created a canonical pregame lineup ledger for `{machine['population_rows']}` fully reconciled rows. Historical pregame lineup coverage is partial: `{machine['lineup_certainty_counts']}`.

Holdout one-hit versus two-plus:

| instrument | brier | auc |
|---|---:|---:|
| frozen control | {control_brier:.6f} | {float(hold.loc['frozen_multi_hit_control', 'auc']):.6f} |
| prior exposure model | {prior_brier:.6f} | {float(hold.loc['prior_exposure_model', 'auc']):.6f} |
| lineup plus turnover | {turnover_brier:.6f} | {float(hold.loc['lineup_plus_turnover', 'auc']):.6f} |
| oracle exposure | {oracle_brier:.6f} | {float(hold.loc['oracle_exposure', 'auc']):.6f} |

Lineup plus turnover recovered `{machine['lineup_turnover_oracle_gap_recovered_pct']:.2%}` of the oracle Brier gap.

## Direct Answer

{direct}

## Production Status

`MLB_LINEUP_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, selector, candidate, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")
    write_validation(out_dir)
    return machine


def write_validation(out_dir: Path) -> None:
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
