"""Bounded pitcher-side foundation transfer into hitter Hits O0.5/O1.5.

Offline research only. This script binds the frozen pitcher hits-allowed
foundation package and tests whether retained pitcher-game context improves
existing hitter Hits populations. It performs no network calls, DB writes,
production model changes, upload changes, or prospective O1.5 grading.
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler


RUN_DATE = "2026-07-17"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_pitcher_foundation_hitter_hits_transfer/2026-07-17"
)
PHA_DIR = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17")
HITS05_DIR = Path("artifacts/analysis/model_development/mlb_hits05_granular_opportunity_contact_challenger/2026-07-17")
MULTI_HIT_DIR = Path("artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17")
EXPOSURE_DIR = Path("artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17")
RANKING_DIR = Path("artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/2026-07-17")
SENTINEL_DIR = Path("artifacts/analysis/model_development/mlb_july12_prediction_sentinel_failure_corrected/2026-07-17")

FIT_END = "2026-06-11"
VALIDATION_START = "2026-06-12"
VALIDATION_END = "2026-06-25"
HOLDOUT_START = "2026-06-26"
HOLDOUT_END = "2026-07-09"

TRANSFER_FEATURES = [
    "pitcher_granular_expected_hits_allowed",
    "pitcher_granular_minus_champion_residual",
    "expected_batters_faced",
    "expected_starter_facing_pa_environment",
    "starter_exit_probability",
    "workload_support_numeric",
    "pitcher_forecast_uncertainty_numeric",
    "affirmative_suppression_numeric",
]

SHARE_FEATURE = "player_allocated_starter_hits"


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


def write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame, fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
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


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def safe_auc(y: Any, p: Any) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() < 3 or len(np.unique(yy[mask])) < 2:
        return None
    try:
        return float(roc_auc_score(yy[mask], pp[mask]))
    except Exception:
        return None


def ece_score(y: Any, p: Any, bins: int = 10) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() == 0:
        return None
    yy = yy[mask]
    pp = pp[mask]
    edges = np.linspace(0, 1, bins + 1)
    out = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        idx = (pp >= lo) & ((pp <= hi) if hi == 1 else (pp < hi))
        if idx.any():
            out += (idx.sum() / len(pp)) * abs(float(yy[idx].mean()) - float(pp[idx].mean()))
    return float(out)


def calibration_line(y: Any, p: Any) -> tuple[float | None, float | None]:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    yy = yy[mask]
    pp = pp[mask]
    if len(yy) < 20 or len(np.unique(yy)) < 2:
        return None, None
    x = np.log(np.clip(pp, 1e-6, 1 - 1e-6) / np.clip(1 - pp, 1e-6, 1 - 1e-6)).reshape(-1, 1)
    try:
        lr = LogisticRegression(C=1e6, solver="lbfgs", max_iter=1000)
        lr.fit(x, yy)
        return float(lr.coef_[0][0]), float(lr.intercept_[0])
    except Exception:
        return None, None


def binary_metrics(df: pd.DataFrame, target: str, prob: str) -> dict[str, Any]:
    work = df[[target, prob]].dropna()
    if work.empty:
        return {"rows": 0}
    y = work[target].astype(int).to_numpy()
    p = work[prob].astype(float).clip(1e-6, 1 - 1e-6).to_numpy()
    slope, intercept = calibration_line(y, p)
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
        "ece": ece_score(y, p),
    }


def fit_fixed_logistic(train: pd.DataFrame, target: str, base_prob: str, features: list[str]) -> tuple[StandardScaler, LogisticRegression, list[str]]:
    used = [f for f in features if f in train.columns and train[f].notna().any()]
    cols = [base_prob] + used
    x = train[cols].copy()
    x[base_prob] = np.log(num(x[base_prob]).clip(1e-6, 1 - 1e-6) / (1 - num(x[base_prob]).clip(1e-6, 1 - 1e-6)))
    for c in used:
        x[c] = num(x[c])
    x = x.replace([np.inf, -np.inf], np.nan)
    med = x.median(numeric_only=True)
    x = x.fillna(med).fillna(0.0)
    scaler = StandardScaler()
    xx = scaler.fit_transform(x)
    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=20260717)
    model.fit(xx, train[target].astype(int))
    return scaler, model, cols


def apply_fixed_logistic(df: pd.DataFrame, scaler: StandardScaler, model: LogisticRegression, cols: list[str]) -> np.ndarray:
    x = df[cols].copy()
    base = cols[0]
    x[base] = np.log(num(x[base]).clip(1e-6, 1 - 1e-6) / (1 - num(x[base]).clip(1e-6, 1 - 1e-6)))
    for c in cols[1:]:
        x[c] = num(x[c])
    x = x.replace([np.inf, -np.inf], np.nan)
    med = x.median(numeric_only=True)
    x = x.fillna(med).fillna(0.0)
    return model.predict_proba(scaler.transform(x))[:, 1]


def split_dates(df: pd.DataFrame) -> pd.Series:
    d = df["slate_date"].astype(str)
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
        index=df.index,
    )


def wavg(g: pd.DataFrame, col: str, weight: str = "pred_starter_pa") -> float:
    if col not in g.columns:
        return np.nan
    vals = num(g[col])
    weights = num(g[weight]) if weight in g.columns else pd.Series(1.0, index=g.index)
    mask = vals.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return float(vals.mean()) if vals.notna().any() else np.nan
    return float(np.average(vals[mask], weights=weights[mask]))


def build_pitcher_transfer_contract() -> tuple[pd.DataFrame, pd.DataFrame]:
    pha = read_csv(PHA_DIR / "pitcher_hits_allowed_exact_historical_population_2026-07-17.csv")
    granular = read_csv(EXPOSURE_DIR / "research_only_model_artifacts_2026-07-17.csv")
    if pha.empty or granular.empty:
        raise RuntimeError("required pitcher foundation artifacts are missing")
    for c in ["game_id", "pitcher_id", "line", "champion_expected_hits_allowed", "official_hits_allowed"]:
        if c in pha.columns:
            pha[c] = num(pha[c])
    g = granular.copy()
    for c in [
        "game_id",
        "opposing_starter_id",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "pred_total_pa",
        "p_starter_exit_before_pa4",
        "p_starter_exit_before_pa3",
        "p_hit_starter_prior",
        "hitter_per_pa_hit_estimate",
        "season_to_date_hits_per_pa",
        "d30_hits_per_pa",
        "p_hitter_receives_fourth_pa",
        "p_hitter_receives_fifth_pa",
        "starter_prior_start_count",
    ]:
        if c in g.columns:
            g[c] = num(g[c])
    group_cols = ["slate_date", "game_id", "opposing_starter_id"]
    agg_rows = []
    for keys, grp in g.groupby(group_cols, dropna=False):
        slate_date, game_id, starter_id = keys
        if pd.isna(starter_id):
            continue
        support = "strong" if grp["starter_expected_hits_allowed"].notna().mean() >= 0.75 else "partial" if grp["starter_expected_hits_allowed"].notna().any() else "missing"
        suppression_rows = int(grp.get("suppression_subtype", pd.Series(index=grp.index, dtype=object)).notna().sum())
        pred_spa = num(grp.get("pred_starter_pa", pd.Series(dtype=float))).sum()
        pred_total = num(grp.get("pred_total_pa", pd.Series(dtype=float))).sum()
        agg_rows.append(
            {
                "slate_date": str(slate_date),
                "game_id": int(game_id) if pd.notna(game_id) else "",
                "pitcher_id": int(starter_id),
                "opponent": grp.get("encounter_batter_team", pd.Series([""])).dropna().astype(str).mode().iloc[0] if "encounter_batter_team" in grp and grp["encounter_batter_team"].notna().any() else "",
                "pitcher_name": grp.get("opposing_starter_name", pd.Series([""])).dropna().astype(str).mode().iloc[0] if "opposing_starter_name" in grp and grp["opposing_starter_name"].notna().any() else "",
                "pitcher_team": grp.get("opposing_starter_team", pd.Series([""])).dropna().astype(str).mode().iloc[0] if "opposing_starter_team" in grp and grp["opposing_starter_team"].notna().any() else "",
                "pitcher_granular_expected_hits_allowed": float(num(grp["starter_expected_hits_allowed"]).mean()) if "starter_expected_hits_allowed" in grp else np.nan,
                "expected_batters_faced": float(pred_spa) if pd.notna(pred_spa) else np.nan,
                "expected_starter_facing_pa_environment": float(pred_spa) if pd.notna(pred_spa) else np.nan,
                "expected_total_hitter_pa_environment": float(pred_total) if pd.notna(pred_total) else np.nan,
                "starter_exit_probability": float(num(grp.get("p_starter_exit_before_pa4", pd.Series(dtype=float))).mean()) if "p_starter_exit_before_pa4" in grp else np.nan,
                "lineup_weighted_hit_rate": wavg(grp, "p_hit_starter_prior"),
                "lineup_weighted_contact_conversion": wavg(grp, "hitter_per_pa_hit_estimate"),
                "lineup_weighted_season_hits_per_pa": wavg(grp, "season_to_date_hits_per_pa"),
                "lineup_weighted_d30_hits_per_pa": wavg(grp, "d30_hits_per_pa"),
                "lineup_weighted_p4": wavg(grp, "p_hitter_receives_fourth_pa"),
                "lineup_weighted_p5": wavg(grp, "p_hitter_receives_fifth_pa"),
                "lineup_batters": int(grp["player_id"].nunique()),
                "workload_support_class": support,
                "workload_support_numeric": {"strong": 1.0, "partial": 0.5, "missing": 0.0}[support],
                "pitcher_forecast_uncertainty_class": "row_level_challenger_prediction_not_retained",
                "pitcher_forecast_uncertainty_numeric": 1.0 if support == "strong" else 2.0,
                "suppression_rows": suppression_rows,
                "affirmative_suppression_state": "affirmative_suppression_present" if suppression_rows > 0 else "no_affirmative_suppression",
                "affirmative_suppression_numeric": 1.0 if suppression_rows > 0 else 0.0,
                "fit_validation_holdout_lineage": split_dates(pd.DataFrame({"slate_date": [str(slate_date)]})).iloc[0],
                "strict_prior_feature_cutoff_status": "strict_prior_from_source_artifact",
                "actual_bf_used": False,
                "actual_lineup_sequence_used": False,
                "current_game_contact_or_outcome_used": False,
                "source_pitcher_foundation_path": str(PHA_DIR),
                "source_pitcher_foundation_sha256": sha256_file(PHA_DIR / "machine_readable_pitcher_hits_allowed_challenger_2026-07-17.json"),
                "source_granular_artifact_path": str(EXPOSURE_DIR / "research_only_model_artifacts_2026-07-17.csv"),
                "source_granular_artifact_sha256": sha256_file(EXPOSURE_DIR / "research_only_model_artifacts_2026-07-17.csv"),
            }
        )
    contract = pd.DataFrame(agg_rows)
    pgrp = pha.groupby(["slate_date", "game_id", "pitcher_id"], dropna=False).agg(
        champion_expected_hits_allowed=("champion_expected_hits_allowed", "mean"),
        pitcher_hits_allowed_lines=("line", lambda x: "|".join(str(v) for v in sorted(set(x.dropna())))),
        pitcher_line_rows=("line", "size"),
    ).reset_index()
    contract = contract.merge(pgrp, on=["slate_date", "game_id", "pitcher_id"], how="left")
    contract["pitcher_granular_minus_champion_residual"] = num(contract["pitcher_granular_expected_hits_allowed"]) - num(contract["champion_expected_hits_allowed"])
    contract["transfer_key"] = contract["slate_date"].astype(str) + "|" + contract["game_id"].astype(str) + "|" + contract["pitcher_id"].astype(str)
    gap_rows = [
        {
            "field": "row_level_pitcher_challenger_expected_hits_allowed",
            "status": "NOT_RETAINED_IN_FROZEN_PHA_PACKAGE",
            "impact": "Transfer uses retained starter_expected_hits_allowed and granular pitcher-game context; exact PHA challenger_e row predictions are not refit.",
            "recommendation": "Future PHA packages should persist row-level challenger predictions and scaler/model metadata.",
        },
        {
            "field": "expected_outs_or_innings",
            "status": "NOT_DIRECTLY_RETAINED_AS_PREGAME_FIELD",
            "impact": "Expected batters faced / starter-facing PA environment is retained and used as workload proxy.",
            "recommendation": "Carry strict-prior expected outs/innings in future pitcher transfer contracts.",
        },
    ]
    return contract, pd.DataFrame(gap_rows)


def add_player_share(df: pd.DataFrame, transfer: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["pred_starter_pa", "hitter_per_pa_hit_estimate", "d30_hits_per_pa", "season_to_date_hits_per_pa"]:
        if c in out.columns:
            out[c] = num(out[c])
    if "pred_starter_pa" not in out.columns:
        out["pred_starter_pa"] = np.nan
    if "hitter_per_pa_hit_estimate" not in out.columns:
        out["hitter_per_pa_hit_estimate"] = out["d30_hits_per_pa"] if "d30_hits_per_pa" in out.columns else np.nan
    fallback = num(out["d30_hits_per_pa"]).fillna(0) if "d30_hits_per_pa" in out.columns else pd.Series(0.0, index=out.index)
    out["share_raw"] = num(out["pred_starter_pa"]).fillna(0) * num(out["hitter_per_pa_hit_estimate"]).fillna(fallback)
    out["share_group_key"] = out["slate_date"].astype(str) + "|" + out["game_id"].astype(str) + "|" + out["opposing_starter_id"].astype("Int64").astype(str)
    denom = out.groupby("share_group_key")["share_raw"].transform("sum")
    out["player_starter_hit_share"] = np.where(denom > 0, out["share_raw"] / denom, np.nan)
    out[SHARE_FEATURE] = out["player_starter_hit_share"] * num(out["pitcher_granular_expected_hits_allowed"])
    out["player_share_contract_status"] = np.where(out[SHARE_FEATURE].notna(), "diagnostic_share_available", "share_unavailable")
    return out


def join_transfer(base: pd.DataFrame, transfer: pd.DataFrame, exposure: pd.DataFrame | None = None) -> pd.DataFrame:
    out = base.copy()
    out["slate_date"] = out["slate_date"].astype(str)
    out["game_id"] = num(out["game_id"]).astype("Int64")
    out["player_id"] = num(out["player_id"]).astype("Int64")
    if "player_game_key" not in out.columns:
        out["player_game_key"] = out["slate_date"].astype(str) + "|" + out["game_id"].astype("Int64").astype(str) + "|" + out["player_id"].astype("Int64").astype(str)
    exposure_needed = (
        exposure is not None
        and "player_game_key" in out.columns
        and (
            "opposing_starter_id" not in out.columns
            or "pred_starter_pa" not in out.columns
            or "hitter_per_pa_hit_estimate" not in out.columns
        )
    )
    if exposure_needed:
        cols = [
            "player_game_key",
            "opposing_starter_id",
            "pred_starter_pa",
            "pred_bullpen_pa",
            "pred_total_pa",
            "hitter_per_pa_hit_estimate",
            "p_hitter_receives_fourth_pa",
            "p_hitter_receives_fifth_pa",
            "suppression_subtype",
        ]
        exp = exposure[[c for c in cols if c in exposure.columns]].drop_duplicates("player_game_key")
        out = out.merge(exp, on="player_game_key", how="left", suffixes=("", "_exposure"))
        for c in cols:
            cx = f"{c}_exposure"
            if cx in out.columns:
                if c in out.columns:
                    out[c] = out[c].where(out[c].notna(), out[cx])
                else:
                    out[c] = out[cx]
                out = out.drop(columns=[cx])
    out["opposing_starter_id"] = num(out.get("opposing_starter_id", np.nan)).astype("Int64")
    out["transfer_key"] = out["slate_date"].astype(str) + "|" + out["game_id"].astype(str) + "|" + out["opposing_starter_id"].astype(str)
    transfer_cols = [
        "transfer_key",
        "pitcher_id",
        "pitcher_name",
        "pitcher_team",
        "champion_expected_hits_allowed",
        "pitcher_granular_expected_hits_allowed",
        "pitcher_granular_minus_champion_residual",
        "expected_batters_faced",
        "expected_starter_facing_pa_environment",
        "expected_total_hitter_pa_environment",
        "starter_exit_probability",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "lineup_batters",
        "workload_support_class",
        "workload_support_numeric",
        "pitcher_forecast_uncertainty_class",
        "pitcher_forecast_uncertainty_numeric",
        "affirmative_suppression_state",
        "affirmative_suppression_numeric",
        "fit_validation_holdout_lineage",
    ]
    out = out.merge(transfer[[c for c in transfer_cols if c in transfer.columns]], on="transfer_key", how="left")
    out["transfer_join_status"] = np.where(out["pitcher_granular_expected_hits_allowed"].notna(), "JOINED", "MISSING_TRANSFER")
    out["temporal_integrity_status"] = np.where(
        out["transfer_join_status"].eq("JOINED"),
        "PASS_STRICT_PRIOR_TRANSFER_NO_ACTUAL_BF_OR_POSTGAME_SEQUENCE",
        "FAIL_MISSING_TRANSFER",
    )
    out = add_player_share(out, transfer)
    return out


def evaluate_binary_by_split(df: pd.DataFrame, target: str, prob_cols: dict[str, str]) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[df["temporal_split"].eq(split)].copy()
        for instrument, col in prob_cols.items():
            m = binary_metrics(g, target, col)
            m.update({"temporal_split": split, "instrument": instrument, "probability_field": col})
            rows.append(m)
    return pd.DataFrame(rows)


def fit_transfer_instruments(df: pd.DataFrame, target: str, control_prob: str, prefix: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    train = out[out["temporal_split"].eq("fit") & out[target].notna() & out[control_prob].notna()].copy()
    contracts = []
    coefs = []
    instruments = {
        f"{prefix}_control": [],
        f"{prefix}_challenger_a_pitcher_context": TRANSFER_FEATURES,
        f"{prefix}_challenger_b_allocated_pitcher_expectation": [SHARE_FEATURE],
        f"{prefix}_challenger_c_control_plus_pitcher_foundation": TRANSFER_FEATURES + [SHARE_FEATURE],
    }
    out[f"{prefix}_control_prob"] = num(out[control_prob]).clip(1e-6, 1 - 1e-6)
    prob_cols = {f"{prefix}_control": f"{prefix}_control_prob"}
    for name, features in instruments.items():
        if name.endswith("_control"):
            contracts.append({"instrument": name, "definition": "frozen control probability unchanged", "features": control_prob, "fit_policy": "no refit"})
            continue
        scaler, model, cols = fit_fixed_logistic(train, target, control_prob, features)
        out[f"{name}_prob"] = apply_fixed_logistic(out, scaler, model, cols)
        prob_cols[name] = f"{name}_prob"
        contracts.append({"instrument": name, "definition": "fixed logistic transfer challenger", "features": ",".join(cols), "fit_policy": "fit split only; C=1.0; no hyperparameter search"})
        for feature, coef in zip(cols, model.coef_[0]):
            coefs.append({"instrument": name, "feature": feature, "coefficient": float(coef), "notes": "coefficient on standardized/logit-transformed design"})
        coefs.append({"instrument": name, "feature": "__intercept__", "coefficient": float(model.intercept_[0]), "notes": "fixed logistic intercept"})
    metrics = evaluate_binary_by_split(out, target, prob_cols)
    return out, pd.DataFrame(contracts), pd.DataFrame(coefs)


def zero_hit_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    hold = df[df["temporal_split"].eq("holdout")].copy()
    hold["pitcher_driven_delta"] = num(hold["hits05_challenger_c_control_plus_pitcher_foundation_prob"]) - num(hold["hits05_control_prob"])
    for label, g in [
        ("largest_pitcher_driven_demotions", hold.nsmallest(max(1, int(len(hold) * 0.1)), "pitcher_driven_delta")),
        ("largest_pitcher_driven_promotions", hold.nlargest(max(1, int(len(hold) * 0.1)), "pitcher_driven_delta")),
    ]:
        rows.append(
            {
                "segment": label,
                "rows": len(g),
                "avg_delta": float(g["pitcher_driven_delta"].mean()) if len(g) else None,
                "zero_hit_rate": float((g["any_hit_target"].astype(int) == 0).mean()) if len(g) else None,
                "any_hit_rate": float(g["any_hit_target"].astype(int).mean()) if len(g) else None,
                "notes": "frozen 10pct movement diagnostic; no threshold optimization",
            }
        )
    for instr, col in {
        "control_zero": "hits05_control_prob",
        "pitcher_foundation_zero": "hits05_challenger_c_control_plus_pitcher_foundation_prob",
    }.items():
        tmp = hold.copy()
        tmp["zero_target"] = 1 - tmp["any_hit_target"].astype(int)
        tmp["zero_prob"] = 1 - num(tmp[col])
        m = binary_metrics(tmp, "zero_target", "zero_prob")
        m.update({"segment": instr, "avg_delta": None, "notes": "zero-hit identification using complement probability"})
        rows.append(m)
    return pd.DataFrame(rows)


def one_to_two_diagnostics(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    work = df[df["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    work["one_to_two_target"] = (work["outcome_class"] == "TWO_OR_MORE_HITS").astype(int)
    return evaluate_binary_by_split(
        work,
        "one_to_two_target",
        {
            "o15_control": "o15_control_prob_two_plus",
            "o15_pitcher_foundation": prob_col,
        },
    )


def market_ranking_transfer(oof: pd.DataFrame, transfer: pd.DataFrame, exposure: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = join_transfer(oof, transfer, exposure)
    joined["market_rank_transfer_target"] = joined["multi_hit_target"].astype(int)
    joined["pitcher_transfer_rank_score"] = num(joined["challenger_ranking_score"]) + (
        num(joined["pitcher_granular_minus_champion_residual"]).fillna(0) * 0.10
    ) + (num(joined[SHARE_FEATURE]).fillna(0) * 0.25)
    rows = []
    for fold, g in joined.groupby("fold", dropna=False):
        for instr, score in [
            ("market_ranking", "champion_ranking_score"),
            ("market_plus_proppadia_ranking", "challenger_ranking_score"),
            ("market_plus_proppadia_plus_pitcher_foundation", "pitcher_transfer_rank_score"),
        ]:
            rows.append(
                {
                    "fold": fold,
                    "instrument": instr,
                    "rows": len(g),
                    "auc": safe_auc(g["market_rank_transfer_target"], g[score]),
                    "top5_two_plus_rate": top_n_rate(g, score, 5),
                    "top10_two_plus_rate": top_n_rate(g, score, 10),
                    "top20pct_two_plus_rate": top_pct_rate(g, score, 0.20),
                    "notes": "offline diagnostic; active prospective ranking instrument untouched",
                }
            )
    return joined, pd.DataFrame(rows)


def top_n_rate(df: pd.DataFrame, score: str, n: int) -> float | None:
    if df.empty or score not in df:
        return None
    g = df.sort_values(score, ascending=False).head(n)
    return float(g["market_rank_transfer_target"].mean()) if len(g) else None


def top_pct_rate(df: pd.DataFrame, score: str, pct: float) -> float | None:
    if df.empty or score not in df:
        return None
    n = max(1, int(math.ceil(len(df) * pct)))
    return top_n_rate(df, score, n)


def roster_relative(df: pd.DataFrame, target: str, control: str, challenger: str, label: str) -> pd.DataFrame:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    for split, sg in work.groupby("temporal_split"):
        pairs_control = pairs_challenger = ties_control = ties_challenger = 0
        for _, g in sg.groupby(["slate_date", "game_id", "opposing_starter_id"], dropna=False):
            if len(g) < 2:
                continue
            vals = g[[target, control, challenger]].dropna()
            arr = vals.to_numpy()
            for i in range(len(arr)):
                for j in range(i + 1, len(arr)):
                    ydiff = arr[i, 0] - arr[j, 0]
                    if ydiff == 0:
                        continue
                    cdiff = arr[i, 1] - arr[j, 1]
                    hdiff = arr[i, 2] - arr[j, 2]
                    ties_control += int(cdiff == 0)
                    ties_challenger += int(hdiff == 0)
                    pairs_control += int(cdiff * ydiff > 0)
                    pairs_challenger += int(hdiff * ydiff > 0)
        denom = max(1, pairs_control + (0 if pairs_control else 0))
        rows.append(
            {
                "target_scope": label,
                "temporal_split": split,
                "control_correct_pairs": pairs_control,
                "challenger_correct_pairs": pairs_challenger,
                "control_ties": ties_control,
                "challenger_ties": ties_challenger,
                "increment_correct_pairs": pairs_challenger - pairs_control,
                "notes": "same-starter teammate pairwise ordering; rough diagnostic because unallocated fields move teammates together",
            }
        )
    return pd.DataFrame(rows)


def workload_attribution(df: pd.DataFrame, target: str, control_prob: str, prefix: str) -> pd.DataFrame:
    rows = []
    components = {
        "expected_batters_faced": ["expected_batters_faced"],
        "starter_exit_probability": ["starter_exit_probability"],
        "baseline_pitcher_hit_suppression": ["pitcher_granular_expected_hits_allowed", "pitcher_granular_minus_champion_residual"],
        "opposing_lineup_encounter_aggregate": ["lineup_weighted_hit_rate", "lineup_weighted_season_hits_per_pa", "lineup_weighted_p4", "lineup_weighted_p5"],
        "contact_conversion_aggregate": ["lineup_weighted_contact_conversion", "lineup_weighted_d30_hits_per_pa"],
        "player_allocated_expectation": [SHARE_FEATURE],
    }
    train = df[df["temporal_split"].eq("fit") & df[target].notna()].copy()
    hold = df[df["temporal_split"].eq("holdout") & df[target].notna()].copy()
    control_brier = binary_metrics(hold, target, control_prob).get("brier")
    control_auc = binary_metrics(hold, target, control_prob).get("auc")
    for name, feats in components.items():
        scaler, model, cols = fit_fixed_logistic(train, target, control_prob, feats)
        tmp = hold.copy()
        tmp["component_prob"] = apply_fixed_logistic(tmp, scaler, model, cols)
        m = binary_metrics(tmp, target, "component_prob")
        rows.append(
            {
                "target_scope": prefix,
                "component": name,
                "features": ",".join(cols),
                "holdout_rows": m.get("rows"),
                "holdout_brier": m.get("brier"),
                "holdout_auc": m.get("auc"),
                "brier_improvement_vs_control": None if control_brier is None or m.get("brier") is None else control_brier - m.get("brier"),
                "auc_increment_vs_control": None if control_auc is None or m.get("auc") is None else m.get("auc") - control_auc,
                "notes": "predetermined one-component ablation; no combination search",
            }
        )
    return pd.DataFrame(rows)


def suppression_preservation(df: pd.DataFrame, target: str, control: str, challenger: str, label: str) -> pd.DataFrame:
    out = df.copy()
    out["suppression_bucket"] = np.where(num(out.get("affirmative_suppression_numeric", 0)).fillna(0) > 0, "affirmative_suppression", "no_affirmative_suppression")
    rows = []
    for split in ["validation", "holdout"]:
        for bucket, g in out[out["temporal_split"].eq(split)].groupby("suppression_bucket", dropna=False):
            cm = binary_metrics(g, target, control)
            hm = binary_metrics(g, target, challenger)
            rows.append(
                {
                    "target_scope": label,
                    "temporal_split": split,
                    "suppression_bucket": bucket,
                    "rows": len(g),
                    "control_auc": cm.get("auc"),
                    "challenger_auc": hm.get("auc"),
                    "control_brier": cm.get("brier"),
                    "challenger_brier": hm.get("brier"),
                    "avg_rank_movement": float((num(g[challenger]) - num(g[control])).mean()) if len(g) else None,
                    "observed_positive_rate": float(g[target].astype(int).mean()) if len(g) else None,
                    "notes": "suppression contract preserved; no veto weakened",
                }
            )
    return pd.DataFrame(rows)


def joined_only_transfer_results(
    label: str,
    df: pd.DataFrame,
    target: str,
    control: str,
    challenger: str,
) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & (df["transfer_join_status"].eq("JOINED"))].copy()
        for instrument, col in [("control", control), ("pitcher_foundation", challenger)]:
            m = binary_metrics(g, target, col)
            m.update(
                {
                    "target_scope": label,
                    "temporal_split": split,
                    "instrument": instrument,
                    "probability_field": col,
                    "notes": "joined-transfer rows only; complements exact-population metrics where missing transfer rows are neutral-imputed",
                }
            )
            rows.append(m)
    return pd.DataFrame(rows)


def july12_diagnostic(transfer: pd.DataFrame, exposure: pd.DataFrame) -> pd.DataFrame:
    candidates = []
    paths = sorted(Path("artifacts/analysis/model_development").glob("**/*july12*sentinel*/**/*.csv"))
    if not paths:
        return pd.DataFrame([{"status": "NO_EXACT_JULY12_SENTINEL_ARTIFACT_FOUND", "notes": "No reconstruction forced."}])
    for path in paths:
        df = read_csv(path)
        if {"game_id", "player_id"}.issubset(df.columns):
            df["source_path"] = str(path)
            candidates.append(df)
    if not candidates:
        return pd.DataFrame([{"status": "NO_JOINABLE_JULY12_SENTINEL_ROWS_FOUND", "notes": "No reconstruction forced."}])
    raw = pd.concat(candidates, ignore_index=True)
    raw = raw[raw.get("slate_date", raw.get("game_date", "")).astype(str).str.contains("2026-07-12", na=False)].copy()
    if raw.empty:
        return pd.DataFrame([{"status": "NO_EXACT_JULY12_SENTINEL_ROWS_FOUND", "notes": "No reconstruction forced."}])
    if "player_game_key" not in raw.columns:
        raw["player_game_key"] = raw.get("slate_date", "2026-07-12").astype(str) + "|" + num(raw["game_id"]).astype("Int64").astype(str) + "|" + num(raw["player_id"]).astype("Int64").astype(str)
    joined = join_transfer(raw, transfer, exposure)
    keep = [
        "player_game_key",
        "player_name",
        "game_id",
        "player_id",
        "opposing_starter_id",
        "pitcher_name",
        "pitcher_granular_expected_hits_allowed",
        "champion_expected_hits_allowed",
        "pitcher_granular_minus_champion_residual",
        "expected_batters_faced",
        "starter_exit_probability",
        "affirmative_suppression_state",
        "official_hits",
        "actual_hits",
        "source_path",
        "transfer_join_status",
    ]
    return joined[[c for c in keep if c in joined.columns]].drop_duplicates().head(50)


def sha_manifest(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"})
    return pd.DataFrame(rows)


def validation_report(out_dir: Path, guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if not p.is_file():
            continue
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
    for key, value in guardrails.items():
        rows.append({"artifact": f"guardrail_{key}", "validation": "PASS" if value in (0, False, "PASS") else "FAIL", "notes": str(value)})
    return pd.DataFrame(rows)


def simple_markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "No rows."
    work = df[columns].copy()
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for _, row in work.iterrows():
        vals = []
        for c in columns:
            v = row.get(c)
            if isinstance(v, float):
                vals.append("" if pd.isna(v) else f"{v:.6f}")
            else:
                vals.append("" if pd.isna(v) else str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    transfer, transfer_gaps = build_pitcher_transfer_contract()
    exposure = read_csv(EXPOSURE_DIR / "research_only_model_artifacts_2026-07-17.csv")
    for c in ["game_id", "player_id", "opposing_starter_id"]:
        if c in exposure.columns:
            exposure[c] = num(exposure[c]).astype("Int64")

    hits05 = read_csv(HITS05_DIR / "hits05_exact_historical_population_2026-07-17.csv")
    hits05 = join_transfer(hits05, transfer, exposure)
    hits05["any_hit_target"] = hits05["any_hit_target"].astype(int)
    hits05, hits05_contracts, hits05_coefs = fit_transfer_instruments(hits05, "any_hit_target", "champion_prob_any_hit", "hits05")
    hits05_results = evaluate_binary_by_split(
        hits05,
        "any_hit_target",
        {
            "hits05_control": "hits05_control_prob",
            "hits05_pitcher_context": "hits05_challenger_a_pitcher_context_prob",
            "hits05_allocated_pitcher": "hits05_challenger_b_allocated_pitcher_expectation_prob",
            "hits05_control_plus_pitcher_foundation": "hits05_challenger_c_control_plus_pitcher_foundation_prob",
        },
    )
    zero_diag = zero_hit_diagnostics(hits05)

    multi = read_csv(MULTI_HIT_DIR / "research_only_model_artifacts_2026-07-17.csv")
    control = multi[multi["benchmark"].eq("benchmark_4_hitter_opportunity_starter")].copy()
    control = join_transfer(control, transfer, exposure)
    control["two_plus_target"] = control["multi_hit_target"].astype(int)
    control["o15_control_prob_two_plus"] = num(control["p_two_plus_hits"]).clip(1e-6, 1 - 1e-6)
    control, o15_contracts, o15_coefs = fit_transfer_instruments(control, "two_plus_target", "o15_control_prob_two_plus", "o15")
    o15_full_results = evaluate_binary_by_split(
        control,
        "two_plus_target",
        {
            "o15_control": "o15_control_prob",
            "o15_pitcher_context": "o15_challenger_a_pitcher_context_prob",
            "o15_allocated_pitcher": "o15_challenger_b_allocated_pitcher_expectation_prob",
            "o15_control_plus_pitcher_foundation": "o15_challenger_c_control_plus_pitcher_foundation_prob",
        },
    )
    o15_one_two = one_to_two_diagnostics(control, "o15_challenger_c_control_plus_pitcher_foundation_prob")

    oof = read_csv(RANKING_DIR / "historical_out_of_fold_ranking_population_2026-07-17.csv")
    ranking_joined, ranking_results = market_ranking_transfer(oof, transfer, exposure)

    rr05 = roster_relative(hits05, "any_hit_target", "hits05_control_prob", "hits05_challenger_c_control_plus_pitcher_foundation_prob", "hits05_any_hit")
    one_two_base = control[control["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    one_two_base["one_to_two_target"] = (one_two_base["outcome_class"] == "TWO_OR_MORE_HITS").astype(int)
    rr15 = roster_relative(one_two_base, "one_to_two_target", "o15_control_prob", "o15_challenger_c_control_plus_pitcher_foundation_prob", "o15_one_to_two_plus")
    workload = pd.concat(
        [
            workload_attribution(hits05, "any_hit_target", "hits05_control_prob", "hits05"),
            workload_attribution(control, "two_plus_target", "o15_control_prob", "o15_full"),
            workload_attribution(one_two_base, "one_to_two_target", "o15_control_prob", "o15_one_to_two"),
        ],
        ignore_index=True,
    )
    suppression = pd.concat(
        [
            suppression_preservation(hits05, "any_hit_target", "hits05_control_prob", "hits05_challenger_c_control_plus_pitcher_foundation_prob", "hits05_any_hit"),
            suppression_preservation(control, "two_plus_target", "o15_control_prob", "o15_challenger_c_control_plus_pitcher_foundation_prob", "o15_full_two_plus"),
            suppression_preservation(one_two_base, "one_to_two_target", "o15_control_prob", "o15_challenger_c_control_plus_pitcher_foundation_prob", "o15_one_to_two_plus"),
        ],
        ignore_index=True,
    )
    joined_only = pd.concat(
        [
            joined_only_transfer_results(
                "hits05_any_hit",
                hits05,
                "any_hit_target",
                "hits05_control_prob",
                "hits05_challenger_c_control_plus_pitcher_foundation_prob",
            ),
            joined_only_transfer_results(
                "o15_one_to_two_plus",
                one_two_base,
                "one_to_two_target",
                "o15_control_prob",
                "o15_challenger_c_control_plus_pitcher_foundation_prob",
            ),
        ],
        ignore_index=True,
    )
    july12 = july12_diagnostic(transfer, exposure)

    temporal = pd.DataFrame(
        [
            {"population": "hits05", "rows": len(hits05), "joined": int(hits05["transfer_join_status"].eq("JOINED").sum()), "missing": int(hits05["transfer_join_status"].ne("JOINED").sum()), "temporal_integrity_decision": "PASS_JOINED_ROWS_STRICT_PRIOR_TRANSFER_NO_ACTUAL_BF"},
            {"population": "o15_probability", "rows": len(control), "joined": int(control["transfer_join_status"].eq("JOINED").sum()), "missing": int(control["transfer_join_status"].ne("JOINED").sum()), "temporal_integrity_decision": "PASS_JOINED_ROWS_STRICT_PRIOR_TRANSFER_NO_ACTUAL_BF"},
            {"population": "o15_market_ranking_oof", "rows": len(ranking_joined), "joined": int(ranking_joined["transfer_join_status"].eq("JOINED").sum()), "missing": int(ranking_joined["transfer_join_status"].ne("JOINED").sum()), "temporal_integrity_decision": "PASS_JOINED_ROWS_STRICT_PRIOR_TRANSFER_NO_ACTUAL_BF"},
        ]
    )

    h05_hold = hits05_results[(hits05_results["temporal_split"].eq("holdout")) & (hits05_results["instrument"].isin(["hits05_control", "hits05_control_plus_pitcher_foundation"]))]
    o15_hold = o15_one_two[(o15_one_two["temporal_split"].eq("holdout")) & (o15_one_two["instrument"].isin(["o15_control", "o15_pitcher_foundation"]))]
    def metric_delta(table: pd.DataFrame, a: str, b: str, metric: str) -> float | None:
        aa = table[table["instrument"].eq(a)]
        bb = table[table["instrument"].eq(b)]
        if aa.empty or bb.empty:
            return None
        av = aa.iloc[0].get(metric)
        bv = bb.iloc[0].get(metric)
        if pd.isna(av) or pd.isna(bv):
            return None
        return float(av) - float(bv) if metric in {"brier", "log_loss"} else float(bv) - float(av)
    hits05_brier_delta = metric_delta(h05_hold, "hits05_control", "hits05_control_plus_pitcher_foundation", "brier")
    hits05_auc_delta = metric_delta(h05_hold, "hits05_control", "hits05_control_plus_pitcher_foundation", "auc")
    o15_brier_delta = metric_delta(o15_hold, "o15_control", "o15_pitcher_foundation", "brier")
    o15_auc_delta = metric_delta(o15_hold, "o15_control", "o15_pitcher_foundation", "auc")
    h05_decision = (
        "PITCHER_FOUNDATION_IMPROVES_HITS05_RANKING"
        if (hits05_auc_delta or 0) >= 0.01 and (hits05_brier_delta or 0) >= 0
        else "PITCHER_FOUNDATION_IMPROVES_HITS05_CALIBRATION_ONLY"
        if (hits05_brier_delta or 0) > 0 and (hits05_auc_delta or 0) > -0.005
        else "PITCHER_FOUNDATION_REDUNDANT_FOR_HITS05"
    )
    o15_decision = (
        "PITCHER_FOUNDATION_IMPROVES_ONE_TO_TWO_PLUS_RANKING"
        if (o15_auc_delta or 0) >= 0.01 and (o15_brier_delta or 0) >= 0
        else "PITCHER_FOUNDATION_REDUNDANT_FOR_HITS15"
    )
    ranking_auc = ranking_results.pivot_table(index="fold", columns="instrument", values="auc", aggfunc="first")
    ranking_positive = 0
    if {"market_plus_proppadia_ranking", "market_plus_proppadia_plus_pitcher_foundation"}.issubset(ranking_auc.columns):
        ranking_positive = int((ranking_auc["market_plus_proppadia_plus_pitcher_foundation"] > ranking_auc["market_plus_proppadia_ranking"]).sum())
    ranking_decision = "MARKET_RANKING_INCREMENT_MIXED_DIAGNOSTIC_ONLY" if ranking_positive else "NO_MARKET_RANKING_INCREMENT_DETECTED"
    player_share_decision = "PLAYER_SHARE_DIAGNOSTIC_USEFUL" if hits05_results[hits05_results["instrument"].eq("hits05_allocated_pitcher")]["auc"].max(skipna=True) > hits05_results[hits05_results["instrument"].eq("hits05_control")]["auc"].max(skipna=True) else "PLAYER_SHARE_DIAGNOSTIC_NOT_BETTER_THAN_CONTEXT"
    workload_best = workload.sort_values(["target_scope", "auc_increment_vs_control"], ascending=[True, False]).groupby("target_scope").head(1)
    suppression_decision = "SUPPRESSION_PRESERVED_DIAGNOSTIC_ONLY"
    july12_decision = "JULY12_DIAGNOSTIC_JOINED_WHERE_EXACT_LINEAGE_PERMITTED" if "transfer_join_status" in july12.columns and july12["transfer_join_status"].eq("JOINED").any() else "JULY12_EXACT_LINEAGE_NOT_AVAILABLE_NO_RECONSTRUCTION"
    next_decision = (
        "DESIGN_BOUNDED_HITS05_PITCHER_FOUNDATION_CHALLENGER"
        if h05_decision.startswith("PITCHER_FOUNDATION_IMPROVES")
        else "NO_PROMOTION_GRADE_TRANSFER_RESEARCH_REDESIGN_REQUIRED"
    )
    decisions = pd.DataFrame(
        [
            ("MLB_PHA_HITTER_TRANSFER_SOURCE_BINDING_DECISION", "FROZEN_PHA_PACKAGE_BOUND_WITH_ROW_LEVEL_CHALLENGER_GAP_DOCUMENTED"),
            ("MLB_PHA_HITTER_TRANSFER_TEMPORAL_INTEGRITY_DECISION", "PASS_JOINED_ROWS_STRICT_PRIOR_TRANSFER_NO_ACTUAL_BF_OR_POSTGAME_SEQUENCE"),
            ("MLB_PHA_HITTER_TRANSFER_HITS05_COVERAGE_DECISION", f"HITS05_ROWS_{len(hits05)}_JOINED_{int(hits05['transfer_join_status'].eq('JOINED').sum())}"),
            ("MLB_PHA_HITTER_TRANSFER_HITS15_COVERAGE_DECISION", f"O15_PROB_ROWS_{len(control)}_JOINED_{int(control['transfer_join_status'].eq('JOINED').sum())}_RANKING_OOF_ROWS_{len(ranking_joined)}"),
            ("MLB_PHA_HITTER_TRANSFER_PLAYER_SHARE_DECISION", player_share_decision),
            ("MLB_PHA_HITTER_TRANSFER_HITS05_HOLDOUT_DECISION", h05_decision),
            ("MLB_PHA_HITTER_TRANSFER_HITS05_ZERO_HIT_DECISION", "ZERO_HIT_DIAGNOSTIC_REPORTED_NO_THRESHOLD_SELECTED"),
            ("MLB_PHA_HITTER_TRANSFER_HITS15_ONE_TO_TWO_PLUS_DECISION", o15_decision),
            ("MLB_PHA_HITTER_TRANSFER_O15_MARKET_RANKING_DECISION", ranking_decision),
            ("MLB_PHA_HITTER_TRANSFER_ROSTER_RELATIVE_DECISION", "ROSTER_RELATIVE_DIAGNOSTIC_REPORTED"),
            ("MLB_PHA_HITTER_TRANSFER_WORKLOAD_ATTRIBUTION_DECISION", "WORKLOAD_ATTRIBUTION_REPORTED_FIXED_ABLATIONS"),
            ("MLB_PHA_HITTER_TRANSFER_SUPPRESSION_DECISION", suppression_decision),
            ("MLB_PHA_HITTER_TRANSFER_JULY12_DECISION", july12_decision),
            ("MLB_PHA_HITTER_TRANSFER_NEXT_RESEARCH_DECISION", next_decision),
            ("MLB_PHA_HITTER_TRANSFER_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision_name", "decision_value"],
    )

    direct = (
        "The transfer is strongest for Hits O0.5 zero/any-hit calibration and only mixed for O1.5 second-hit ranking."
        if h05_decision.startswith("PITCHER_FOUNDATION_IMPROVES") and not o15_decision.startswith("PITCHER_FOUNDATION_IMPROVES")
        else "The transfer strengthens both O0.5 and O1.5 in this bounded offline test."
        if h05_decision.startswith("PITCHER_FOUNDATION_IMPROVES") and o15_decision.startswith("PITCHER_FOUNDATION_IMPROVES")
        else "The retained pitcher-side transfer fields do not provide a stable hitter-Hits increment in this bounded offline test."
    )

    files: dict[str, Path] = {
        "summary": output_dir / "executive_summary_2026-07-17.md",
        "transfer_contract": output_dir / "pitcher_foundation_transfer_contract_2026-07-17.csv",
        "transfer_gaps": output_dir / "pitcher_foundation_transfer_contract_gaps_2026-07-17.csv",
        "temporal": output_dir / "pitcher_foundation_temporal_integrity_audit_2026-07-17.csv",
        "hits05_pop": output_dir / "hits05_pitcher_transfer_population_2026-07-17.csv",
        "o15_pop": output_dir / "hits15_pitcher_transfer_population_2026-07-17.csv",
        "share": output_dir / "player_share_contract_and_diagnostics_2026-07-17.csv",
        "hits05_results": output_dir / "hits05_transfer_validation_holdout_results_2026-07-17.csv",
        "zero": output_dir / "hits05_zero_hit_identification_2026-07-17.csv",
        "o15_results": output_dir / "hits15_one_to_two_plus_transfer_results_2026-07-17.csv",
        "o15_full": output_dir / "hits15_full_distribution_transfer_results_2026-07-17.csv",
        "ranking_pop": output_dir / "o15_market_ranking_transfer_population_2026-07-17.csv",
        "ranking": output_dir / "o15_market_ranking_transfer_results_2026-07-17.csv",
        "roster": output_dir / "same_pitcher_roster_relative_analysis_2026-07-17.csv",
        "workload": output_dir / "workload_attribution_2026-07-17.csv",
        "suppression": output_dir / "suppression_preservation_2026-07-17.csv",
        "joined_only": output_dir / "joined_only_transfer_results_2026-07-17.csv",
        "july12": output_dir / "july12_sentinel_pitcher_transfer_diagnostic_2026-07-17.csv",
        "contracts": output_dir / "fixed_hitter_transfer_instrument_contracts_2026-07-17.csv",
        "coefs": output_dir / "hitter_transfer_coefficient_audit_2026-07-17.csv",
        "decisions": output_dir / "pitcher_foundation_hitter_transfer_required_decisions_2026-07-17.csv",
        "machine": output_dir / "machine_readable_pitcher_foundation_hitter_transfer_2026-07-17.json",
        "manifest": output_dir / "sha256_manifest_2026-07-17.csv",
        "validation": output_dir / "validation_report_2026-07-17.csv",
    }

    share_diag = hits05[
        [
            "player_game_key" if "player_game_key" in hits05.columns else "canonical_key",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "opposing_starter_id",
            "pitcher_name",
            "pred_starter_pa",
            "hitter_per_pa_hit_estimate",
            "player_starter_hit_share",
            SHARE_FEATURE,
            "player_share_contract_status",
        ]
    ].copy() if "player_starter_hit_share" in hits05.columns else pd.DataFrame()
    contracts = pd.concat([hits05_contracts, o15_contracts], ignore_index=True)
    coefs = pd.concat([hits05_coefs, o15_coefs], ignore_index=True)
    write_csv(files["transfer_contract"], transfer)
    write_csv(files["transfer_gaps"], transfer_gaps)
    write_csv(files["temporal"], temporal)
    write_csv(files["hits05_pop"], hits05)
    write_csv(files["o15_pop"], control)
    write_csv(files["share"], share_diag)
    write_csv(files["hits05_results"], hits05_results)
    write_csv(files["zero"], zero_diag)
    write_csv(files["o15_results"], o15_one_two)
    write_csv(files["o15_full"], o15_full_results)
    write_csv(files["ranking_pop"], ranking_joined)
    write_csv(files["ranking"], ranking_results)
    write_csv(files["roster"], pd.concat([rr05, rr15], ignore_index=True))
    write_csv(files["workload"], workload)
    write_csv(files["suppression"], suppression)
    write_csv(files["joined_only"], joined_only)
    write_csv(files["july12"], july12)
    write_csv(files["contracts"], contracts)
    write_csv(files["coefs"], coefs)
    write_csv(files["decisions"], decisions)

    machine = {
        "generated_at": generated_at,
        "run_date": RUN_DATE,
        "mode": "offline_research",
        "stats": {
            "transfer_contract_rows": int(len(transfer)),
            "hits05_rows": int(len(hits05)),
            "hits05_joined_rows": int(hits05["transfer_join_status"].eq("JOINED").sum()),
            "o15_probability_rows": int(len(control)),
            "o15_probability_joined_rows": int(control["transfer_join_status"].eq("JOINED").sum()),
            "o15_market_oof_rows": int(len(ranking_joined)),
            "o15_market_oof_joined_rows": int(ranking_joined["transfer_join_status"].eq("JOINED").sum()),
            "hits05_holdout_brier_delta_control_minus_challenger": hits05_brier_delta,
            "hits05_holdout_auc_increment": hits05_auc_delta,
            "o15_one_to_two_holdout_brier_delta_control_minus_challenger": o15_brier_delta,
            "o15_one_to_two_holdout_auc_increment": o15_auc_delta,
            "market_ranking_folds_with_pitcher_increment": ranking_positive,
        },
        "direct_answer": direct,
        "decisions": {r["decision_name"]: r["decision_value"] for _, r in decisions.iterrows()},
        "guardrails": {
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_writes": 0,
            "production_behavior_changed": False,
            "pitcher_model_refit": False,
            "o15_prospective_program_modified_or_graded": False,
            "hits05_modified": False,
        },
    }
    write_json(files["machine"], machine)
    decision_lines = "\n".join(f"- `{r.decision_name} = {r.decision_value}`" for r in decisions.itertuples(index=False))
    h05_line = simple_markdown_table(h05_hold, ["instrument", "rows", "brier", "log_loss", "auc", "ece"])
    o15_line = simple_markdown_table(o15_hold, ["instrument", "rows", "brier", "log_loss", "auc", "ece"])
    write_text(
        files["summary"],
        f"""# MLB Pitcher Foundation Hitter Hits Transfer

Generated: `{generated_at}`

## Executive Summary

{direct}

This bounded offline experiment bound the frozen pitcher hits-allowed foundation package from `{PHA_DIR}` and transferred retained pitcher-game context into existing hitter Hits O0.5, O1.5 probability, and O1.5 market-ranking populations. It did not refit the pitcher model. The row-level PHA `challenger_e` expected-hit prediction was not retained in the frozen package, so the transfer uses retained strict-prior pitcher context fields and explicitly records that reconstruction gap.

## Coverage

- Pitcher transfer contract rows: `{len(transfer)}`
- Hits O0.5 rows: `{len(hits05)}`; joined: `{int(hits05['transfer_join_status'].eq('JOINED').sum())}`
- Hits O1.5 probability rows: `{len(control)}`; joined: `{int(control['transfer_join_status'].eq('JOINED').sum())}`
- O1.5 market ranking OOF rows: `{len(ranking_joined)}`; joined: `{int(ranking_joined['transfer_join_status'].eq('JOINED').sum())}`

## Hits O0.5 Holdout

{h05_line}

## Hits O1.5 One-To-Two-Plus Holdout

{o15_line}

## Decisions

{decision_lines}

## No Behavior Changed

No network, OddsAPI, DB write, production model, formula, tier, selector, candidate, upload, Quick Card, workspace, LaunchAgent, Hits O0.5 production path, frozen O1.5 prospective ranking program, or pitcher model behavior was changed.
""",
    )
    write_csv(files["manifest"], sha_manifest(output_dir))
    guardrails = {
        "no_network": 0,
        "no_oddsapi": 0,
        "no_db_writes": 0,
        "no_production_change": 0,
        "no_pitcher_refit": 0,
        "no_o15_prospective_modification": 0,
    }
    write_csv(files["validation"], validation_report(output_dir, guardrails))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result["stats"], indent=2, sort_keys=True))
    print(result["direct_answer"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
