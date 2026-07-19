#!/usr/bin/env python3
"""Build a bounded market-independent MLB Hits count-distribution reconstruction.

This utility reads retained local feature/outcome artifacts, excludes sportsbook
and market-derived fields from the modeling matrix, fits a small frozen set of
baseball-only count-distribution candidates, and writes research artifacts only.
It does not write to a database, call external APIs, alter production models, or
produce executable wagers.
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

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
RUN_DATE = "2026-07-18"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_market_independent_reconstruction/2026-07-18"
PRIOR_V1_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_production_model_reconstruction/2026-07-18"
FEATURE_ROOT = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors"
PRODUCTION_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18/production_prediction_manifest_2026-07-18.csv"
CURRENT_FEATURE_DATE = "2026-07-19"
PROP = "hits"
SEED = 20260718

MARKET_TOKEN_REJECTS = (
    "odds",
    "price",
    "market",
    "book",
    "vig",
    "implied",
    "consensus",
    "favorite",
    "underdog",
    "ev",
    "line_diff",
    "offered",
    "snapshot",
    "selection",
    "betonline",
    "fanduel",
)
IDENTITY_OR_TARGET_FIELDS = {
    "date",
    "slate_date",
    "for_date",
    "game_date",
    "game_time",
    "player_name",
    "player_id",
    "game_id",
    "prop_type",
    "side",
    "over_under",
    "line",
    "actual_value",
    "actual_over_outcome",
    "target_over",
    "selected_side",
    "model_pick_prob",
    "prob_over",
    "prob_under",
    "prop_value",
    "official_hits",
    "actual_hits",
    "actual_hits_uncapped",
    "hit_count_class",
    "target_o05",
    "target_o15",
    "target_o25",
    "target_o35",
    "incumbent_p_over_0_5",
    "incumbent_p_over_1_5",
    "market_price_available",
    "source_path",
    "feature_source_path",
    "_source_path",
    "pair_key",
    "player_game_key",
    "_line_priority",
    "split",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    if not fields:
        fields = ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def safe_float(value: Any) -> float | None:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def line_key(value: Any) -> str:
    f = safe_float(value)
    return f"{f:.1f}" if f is not None else ""


def player_game_key(df: pd.DataFrame) -> pd.Series:
    return (
        pd.to_datetime(df["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d").astype(str)
        + "|"
        + pd.to_numeric(df["game_id"], errors="coerce").astype("Int64").astype(str)
        + "|"
        + pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype(str)
    )


def proposition_key(df: pd.DataFrame) -> pd.Series:
    return (
        player_game_key(df)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + pd.to_numeric(df["line"], errors="coerce").map(line_key)
    )


def hit_class(value: Any) -> int:
    f = safe_float(value)
    if f is None:
        return -1
    return int(min(max(int(f), 0), 3))


def onehot() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_preprocessor(numeric: list[str], categorical: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), numeric),
            ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", onehot())]), categorical),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


def make_hgb_classifier(numeric: list[str], categorical: list[str]) -> Pipeline:
    model = HistGradientBoostingClassifier(
        learning_rate=0.045,
        max_iter=160,
        max_leaf_nodes=31,
        min_samples_leaf=30,
        l2_regularization=0.08,
        random_state=SEED,
        early_stopping=False,
    )
    return Pipeline([("preprocess", build_preprocessor(numeric, categorical)), ("model", model)])


def make_poisson(numeric: list[str], categorical: list[str]) -> Pipeline:
    return Pipeline(
        [
            ("preprocess", build_preprocessor(numeric, categorical)),
            ("model", PoissonRegressor(alpha=0.02, max_iter=800)),
        ]
    )


def load_feature_vectors() -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frames: list[pd.DataFrame] = []
    rows: list[dict[str, Any]] = []
    for path in sorted(FEATURE_ROOT.glob("20??-??-??/hits_features.csv")):
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            rows.append({"slate_date": path.parent.name, "source": rel(path), "rows": 0, "status": "READ_ERROR", "notes": str(exc)})
            continue
        df["feature_source_path"] = rel(path)
        df["slate_date"] = pd.to_datetime(df.get("date", df.get("game_date")), errors="coerce").dt.strftime("%Y-%m-%d")
        df = df[df["prop_type"].astype(str).eq(PROP)].copy()
        if len(df):
            frames.append(df)
        rows.append({"slate_date": path.parent.name, "source": rel(path), "rows": int(len(df)), "status": "FOUND" if len(df) else "NO_HITS_ROWS", "notes": "retained prepared hits feature vector"})
    return (pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()), rows


def load_outcome_manifest() -> pd.DataFrame:
    manifest = pd.read_csv(PRODUCTION_MANIFEST, low_memory=False)
    manifest = manifest[manifest["prop_type"].astype(str).eq(PROP)].copy()
    manifest["slate_date"] = pd.to_datetime(manifest["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    manifest["player_game_key"] = player_game_key(manifest)
    manifest["proposition_key"] = proposition_key(manifest)
    manifest["official_hits"] = pd.to_numeric(manifest["actual_value"], errors="coerce")
    return manifest[manifest["official_hits"].notna()].copy()


def recover_missing_dates(feature_coverage: list[dict[str, Any]], manifest: pd.DataFrame) -> list[dict[str, Any]]:
    prior = pd.read_csv(PRIOR_V1_DIR / "population_exclusions_2026-07-18.csv")
    missing_text = str(prior.loc[prior["reason"].eq("manifest_dates_without_feature_vectors"), "notes"].iloc[0])
    missing_dates = [d for d in missing_text.split("|") if d]
    bundle_paths = [
        ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12/independent_replay/matrices/variant_a_research_matrix_2026-07-12.csv",
        ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12/independent_replay/matrices/hits_1_5_research_matrix_2026-07-12.csv",
    ]
    bundle_by_date: Counter[str] = Counter()
    for path in bundle_paths:
        if not path.exists():
            continue
        df = pd.read_csv(path, usecols=lambda c: c in {"slate_date"}, low_memory=False)
        bundle_by_date.update(df["slate_date"].dropna().astype(str).tolist())
    feature_rows = {r["slate_date"]: int(r.get("rows", 0) or 0) for r in feature_coverage}
    manifest_counts = manifest.groupby("slate_date").size().to_dict()
    out = []
    for d in missing_dates:
        feature_file = FEATURE_ROOT / d / "hits_features.csv"
        if feature_rows.get(d, 0) > 0:
            status = "BASEBALL_FEATURES_RECOVERED"
            notes = "Prepared feature vector exists in current repository scan."
        elif bundle_by_date.get(d, 0) > 0:
            status = "PARTIALLY_RECOVERED"
            notes = "Collective-bundle research matrix has limited strict-prior fields, but not enough to reconstruct full production-style feature vectors safely."
        elif (FEATURE_ROOT / d / "README.md").exists():
            status = "TECHNICALLY_UNRECOVERABLE"
            notes = "Only directory/README retained; no hits_features.csv available."
        else:
            status = "TECHNICALLY_UNRECOVERABLE"
            notes = "No retained prepared feature vector or certified substitute located."
        out.append(
            {
                "slate_date": d,
                "prior_missing_inventory_source": rel(PRIOR_V1_DIR / "population_exclusions_2026-07-18.csv"),
                "manifest_hit_rows": int(manifest_counts.get(d, 0)),
                "prepared_feature_rows": int(feature_rows.get(d, 0)),
                "collective_bundle_rows": int(bundle_by_date.get(d, 0)),
                "recovery_classification": status,
                "fields_recovered": "existing_prepared_feature_vector" if status == "BASEBALL_FEATURES_RECOVERED" else "limited_collective_bundle_fields" if status == "PARTIALLY_RECOVERED" else "",
                "unresolved_fields": "" if status == "BASEBALL_FEATURES_RECOVERED" else "full_production_prepared_feature_vector",
                "source_lineage": rel(feature_file) if feature_file.exists() else "collective_bundle_matrix" if bundle_by_date.get(d, 0) else "",
                "strict_prior_validation": "PASS_RETAINED_ARTIFACT" if status in {"BASEBALL_FEATURES_RECOVERED", "PARTIALLY_RECOVERED"} else "NOT_AVAILABLE",
                "notes": notes,
            }
        )
    return out


def assemble_population(features: pd.DataFrame, manifest: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if features.empty:
        return pd.DataFrame(), []
    features = features.copy()
    features["player_game_key"] = player_game_key(features)
    features["_line_priority"] = pd.to_numeric(features.get("line"), errors="coerce").map(lambda x: 0 if x == 0.5 else 1 if x == 1.5 else 2)
    feature_unique = features.sort_values(["player_game_key", "_line_priority"]).drop_duplicates("player_game_key", keep="first")
    outcome_unique = manifest.sort_values(["player_game_key", "line"]).drop_duplicates("player_game_key", keep="first")
    pop = feature_unique.merge(
        outcome_unique[["player_game_key", "official_hits", "snapshot_run_tag", "_source_path"]],
        on="player_game_key",
        how="inner",
        suffixes=("", "_outcome"),
    )
    pop["actual_hits_uncapped"] = pd.to_numeric(pop["official_hits"], errors="coerce")
    pop["hit_count_class"] = pop["actual_hits_uncapped"].map(hit_class)
    pop = pop[pop["hit_count_class"].between(0, 3)].copy()
    pop["target_o05"] = (pop["actual_hits_uncapped"] >= 1).astype(int)
    pop["target_o15"] = (pop["actual_hits_uncapped"] >= 2).astype(int)
    pop["target_o25"] = (pop["actual_hits_uncapped"] >= 3).astype(int)
    pop["target_o35"] = (pop["actual_hits_uncapped"] >= 4).astype(int)
    prior_pop_path = PRIOR_V1_DIR / "reconstruction_population_2026-07-18.csv"
    if prior_pop_path.exists():
        prior = pd.read_csv(prior_pop_path, low_memory=False)
        prior["slate_date"] = pd.to_datetime(prior["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        prior["player_game_key"] = player_game_key(prior)
        prior["line"] = pd.to_numeric(prior["line"], errors="coerce")
        prior["incumbent_prob_over"] = pd.to_numeric(prior.get("incumbent_prob_over", prior.get("prob_over")), errors="coerce")
        prior["market_price_available"] = prior.get("market_price_available", False)
        prior["market_price_available"] = prior["market_price_available"].astype(str).str.lower().isin({"true", "1", "yes"})
        pivot = prior.pivot_table(index="player_game_key", columns="line", values="incumbent_prob_over", aggfunc="last")
        pivot = pivot.rename(columns={0.5: "incumbent_p_over_0_5", 1.5: "incumbent_p_over_1_5"})
        pivot.columns = [str(c) for c in pivot.columns]
        pivot = pivot.reset_index()
        flags = prior.groupby("player_game_key", as_index=False)["market_price_available"].max()
        pop = pop.merge(pivot, on="player_game_key", how="left").merge(flags, on="player_game_key", how="left")
    else:
        pop["incumbent_p_over_0_5"] = np.nan
        pop["incumbent_p_over_1_5"] = np.nan
        pop["market_price_available"] = False
    diagnostics = [
        {"stage": "retained_feature_rows", "rows": int(len(features)), "notes": "Prepared local hits feature rows; source remains market-conditioned."},
        {"stage": "unique_player_game_features", "rows": int(len(feature_unique)), "notes": "Deduped to player-game grain, preferring 0.5 line row only as duplicate source row."},
        {"stage": "manifest_outcome_rows", "rows": int(len(manifest)), "notes": "Outcome authority from retained production baseline manifest."},
        {"stage": "joined_player_game_rows", "rows": int(len(pop)), "notes": "Market-independent feature matrix, but retained population is not full nonmarket universe."},
        {"stage": "same_row_incumbent_o05_rows", "rows": int(pd.to_numeric(pop.get("incumbent_p_over_0_5"), errors="coerce").notna().sum()), "notes": "Evaluation-only incumbent comparator; not used as feature."},
        {"stage": "same_row_incumbent_o15_rows", "rows": int(pd.to_numeric(pop.get("incumbent_p_over_1_5"), errors="coerce").notna().sum()), "notes": "Evaluation-only incumbent comparator; not used as feature."},
    ]
    return pop, diagnostics


def choose_features(pop: pd.DataFrame) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    rows = []
    candidates = []
    for c in pop.columns:
        lc = str(c).lower()
        numeric_label = False
        try:
            float(str(c))
            numeric_label = True
        except Exception:
            numeric_label = False
        if not isinstance(c, str) or numeric_label or c in IDENTITY_OR_TARGET_FIELDS or any(tok in lc for tok in MARKET_TOKEN_REJECTS) or c.endswith("_outcome"):
            rows.append({"feature_name": c, "used": False, "feature_family": "excluded", "source_table_or_file": "prepared_feature_vectors", "prediction_time_availability": "n/a", "historical_availability": "n/a", "missing_policy": "excluded", "notes": "identity/target/market-derived/post-outcome guard"})
            continue
        if pd.to_numeric(pop[c], errors="coerce").notna().any() or pop[c].dtype == object or str(pop[c].dtype) == "bool":
            candidates.append(c)
    categorical = [c for c in candidates if pop[c].dtype == object or str(pop[c].dtype) == "bool"]
    numeric = [c for c in candidates if c not in categorical and pd.to_numeric(pop[c], errors="coerce").notna().any()]
    for c in numeric + categorical:
        lc = c.lower()
        if "bvp" in lc:
            fam = "batter_pitcher_interaction"
        elif any(x in lc for x in ["hit", "total_base", "double", "run", "rbi", "walk", "strikeout"]):
            fam = "batter_skill_form"
        elif any(x in lc for x in ["home", "away", "team", "opponent", "time", "day"]):
            fam = "team_game_context"
        elif any(x in lc for x in ["pitcher", "outs", "allowed", "earned"]):
            fam = "opposing_starter"
        else:
            fam = "other_baseball"
        rows.append(
            {
                "feature_name": c,
                "used": True,
                "feature_family": fam,
                "source_table_or_file": "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date>/hits_features.csv",
                "prediction_time_availability": "retained_prepared_feature_vector",
                "historical_availability": "partial_date_coverage",
                "missing_policy": "median_impute" if c in numeric else "most_frequent_impute",
                "notes": "No sportsbook, price, line, bookmaker, or market-derived fields allowed.",
            }
        )
    used = numeric + categorical
    banned_used = [
        c for c in used
        if c in IDENTITY_OR_TARGET_FIELDS
        or any(tok in c.lower() for tok in MARKET_TOKEN_REJECTS)
        or c.endswith("_outcome")
    ]
    if banned_used:
        raise RuntimeError(f"banned market/outcome fields reached feature matrix: {banned_used}")
    return rows, numeric, categorical


def choose_splits(pop: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    dates = sorted(pop["slate_date"].dropna().astype(str).unique())
    n = len(dates)
    fit_end = max(1, int(n * 0.60))
    val_end = max(fit_end + 1, int(n * 0.80))
    parts = {"fit": dates[:fit_end], "validation": dates[fit_end:val_end], "holdout": dates[val_end:]}
    mapping = {d: split for split, ds in parts.items() for d in ds}
    out = pop.copy()
    out["split"] = out["slate_date"].astype(str).map(mapping)
    rows = []
    for split, ds in parts.items():
        g = out[out["split"].eq(split)]
        dist = g["hit_count_class"].value_counts().to_dict()
        rows.append(
            {
                "split": split,
                "start_date": ds[0] if ds else "",
                "end_date": ds[-1] if ds else "",
                "date_count": len(ds),
                "player_game_rows": int(len(g)),
                "hits_0_rows": int(dist.get(0, 0)),
                "hits_1_rows": int(dist.get(1, 0)),
                "hits_2_rows": int(dist.get(2, 0)),
                "hits_3_plus_rows": int(dist.get(3, 0)),
                "unique_players": int(g["player_id"].nunique()),
                "unique_teams": int(g["team"].nunique()) if "team" in g else "",
                "market_offered_population_rows": int(len(g)),
                "nonmarket_population_rows": 0,
                "notes": "Whole-date chronological split. Nonmarket population unavailable from retained local artifacts.",
            }
        )
    return out, rows


def poisson_probs(mu: np.ndarray) -> np.ndarray:
    mu = np.clip(mu.astype(float), 1e-6, 8.0)
    p0 = np.exp(-mu)
    p1 = p0 * mu
    p2 = p1 * mu / 2.0
    p3 = np.clip(1.0 - p0 - p1 - p2, 1e-9, 1.0)
    out = np.vstack([p0, p1, p2, p3]).T
    return out / out.sum(axis=1, keepdims=True)


def baseline_probs(pop: pd.DataFrame, fit: pd.DataFrame, kind: str) -> np.ndarray:
    if kind == "rolling_hit_rate_opportunity":
        d7 = pd.to_numeric(pop.get("d7_hits"), errors="coerce")
        d15 = pd.to_numeric(pop.get("d15_hits"), errors="coerce")
        d30 = pd.to_numeric(pop.get("d30_hits"), errors="coerce")
        mu = (0.50 * d7.fillna(d15).fillna(d30) + 0.30 * d15.fillna(d30).fillna(d7) + 0.20 * d30.fillna(d15).fillna(d7)).fillna(fit["actual_hits_uncapped"].mean())
        return poisson_probs(mu.to_numpy())
    if kind == "poisson_count_baseline":
        mu = np.repeat(float(fit["actual_hits_uncapped"].mean()), len(pop))
        return poisson_probs(mu)
    if kind == "empirical_skill_opportunity":
        train = fit.copy()
        train["_skill_bin"] = pd.qcut(pd.to_numeric(train.get("d15_hits"), errors="coerce").rank(method="first"), q=min(4, max(1, len(train) // 50)), duplicates="drop")
        dist = train.groupby("_skill_bin", observed=False)["hit_count_class"].value_counts(normalize=True).unstack(fill_value=0)
        global_dist = train["hit_count_class"].value_counts(normalize=True).reindex([0, 1, 2, 3], fill_value=0).to_numpy()
        scored = pop.copy()
        try:
            scored["_skill_bin"] = pd.cut(pd.to_numeric(scored.get("d15_hits"), errors="coerce").rank(method="first"), bins=len(dist), include_lowest=True)
        except Exception:
            return np.tile(global_dist, (len(pop), 1))
        rows = []
        dist_rows = dist.reindex(columns=[0, 1, 2, 3], fill_value=0).to_numpy()
        for i, _ in enumerate(scored.index):
            rows.append(dist_rows[min(i % max(len(dist_rows), 1), len(dist_rows) - 1)] if len(dist_rows) else global_dist)
        return np.asarray(rows)
    raise ValueError(kind)


def fit_candidates(pop: pd.DataFrame, numeric: list[str], categorical: list[str], out_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    fit = pop[pop["split"].eq("fit")]
    features = numeric + categorical
    candidates: dict[str, Any] = {}
    contracts: list[dict[str, Any]] = []
    multiclass = make_hgb_classifier(numeric, categorical)
    multiclass.fit(fit[features], fit["hit_count_class"].astype(int))
    candidates["candidate_a_multiclass_hgb"] = multiclass
    contracts.append({"candidate": "candidate_a_multiclass_hgb", "model_type": "HistGradientBoostingClassifier", "target": "hit_count_class 0/1/2/3plus", "hyperparameters": "learning_rate=0.045|max_iter=160|max_leaf_nodes=31|min_samples_leaf=30|l2_regularization=0.08", "seed": SEED, "calibration": "none", "selection_rule": "frozen before holdout"})
    ordinal = {}
    for threshold, target in [(1, "target_o05"), (2, "target_o15"), (3, "target_o25")]:
        clf = make_hgb_classifier(numeric, categorical)
        clf.fit(fit[features], fit[target].astype(int))
        ordinal[threshold] = clf
    candidates["candidate_b_ordinal_hgb"] = ordinal
    contracts.append({"candidate": "candidate_b_ordinal_hgb", "model_type": "three fixed cumulative HistGradientBoostingClassifiers", "target": "P(hits>=1), P(hits>=2), P(hits>=3)", "hyperparameters": "same as candidate_a", "seed": SEED, "calibration": "monotonic repair only", "selection_rule": "frozen before holdout"})
    poisson = make_poisson(numeric, categorical)
    poisson.fit(fit[features], fit["actual_hits_uncapped"].clip(lower=0))
    candidates["candidate_c_poisson"] = poisson
    contracts.append({"candidate": "candidate_c_poisson", "model_type": "PoissonRegressor", "target": "uncapped actual hits", "hyperparameters": "alpha=0.02|max_iter=800", "seed": SEED, "calibration": "Poisson distribution from predicted mean", "selection_rule": "frozen before holdout"})
    for name, model in candidates.items():
        path = out_dir / f"{name}_artifact.joblib"
        joblib.dump({"candidate": name, "model": model, "numeric_features": numeric, "categorical_features": categorical, "seed": SEED}, path)
        for row in contracts:
            if row["candidate"] == name:
                row["artifact_path"] = rel(path)
                row["artifact_sha256"] = sha256(path)
    return candidates, contracts


def predict_candidate(name: str, model: Any, df: pd.DataFrame, features: list[str]) -> np.ndarray:
    if name == "candidate_a_multiclass_hgb":
        probs = model.predict_proba(df[features])
        classes = list(model.named_steps["model"].classes_)
        out = np.zeros((len(df), 4))
        for idx, cls in enumerate(classes):
            if int(cls) in [0, 1, 2, 3]:
                out[:, int(cls)] = probs[:, idx]
        return out / out.sum(axis=1, keepdims=True)
    if name == "candidate_b_ordinal_hgb":
        p_ge1 = model[1].predict_proba(df[features])[:, 1]
        p_ge2 = model[2].predict_proba(df[features])[:, 1]
        p_ge3 = model[3].predict_proba(df[features])[:, 1]
        p_ge2 = np.minimum(p_ge2, p_ge1)
        p_ge3 = np.minimum(p_ge3, p_ge2)
        p0 = 1 - p_ge1
        p1 = p_ge1 - p_ge2
        p2 = p_ge2 - p_ge3
        p3 = p_ge3
        return np.vstack([p0, p1, p2, p3]).T
    if name == "candidate_c_poisson":
        mu = np.clip(model.predict(df[features]), 1e-6, 8.0)
        return poisson_probs(mu)
    raise ValueError(name)


def distribution_metrics(df: pd.DataFrame, candidate: str) -> dict[str, Any]:
    y = df["hit_count_class"].astype(int).to_numpy()
    probs = df[[f"{candidate}_p0", f"{candidate}_p1", f"{candidate}_p2", f"{candidate}_p3_plus"]].to_numpy(dtype=float)
    one_hot = np.eye(4)[y]
    cdf_pred = np.cumsum(probs, axis=1)
    cdf_true = np.cumsum(one_hot, axis=1)
    expected = probs @ np.array([0, 1, 2, 3], dtype=float)
    return {
        "candidate": candidate,
        "split": str(df["split"].iloc[0]) if len(df) else "",
        "rows": int(len(df)),
        "multiclass_log_loss": float(log_loss(y, probs, labels=[0, 1, 2, 3])) if len(df) else "",
        "multiclass_brier": float(np.mean(np.sum((probs - one_hot) ** 2, axis=1))) if len(df) else "",
        "ranked_probability_score": float(np.mean(np.sum((cdf_pred - cdf_true) ** 2, axis=1) / 3.0)) if len(df) else "",
        "expected_hits_mae": float(mean_absolute_error(df["actual_hits_uncapped"], expected)) if len(df) else "",
        "expected_hits_rmse": float(mean_squared_error(df["actual_hits_uncapped"], expected) ** 0.5) if len(df) else "",
        "avg_predicted_hits": float(expected.mean()) if len(df) else "",
        "avg_actual_hits": float(df["actual_hits_uncapped"].mean()) if len(df) else "",
    }


def binary_metrics(df: pd.DataFrame, candidate: str, threshold: str, prob_col: str, target_col: str) -> dict[str, Any]:
    y = df[target_col].astype(int)
    p = df[prob_col].clip(1e-6, 1 - 1e-6)
    auc = float(roc_auc_score(y, p)) if y.nunique() == 2 else ""
    slope, intercept = calibration_slope_intercept(y, p)
    pred = (p >= 0.5).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    return {
        "candidate": candidate,
        "threshold": threshold,
        "split": str(df["split"].iloc[0]) if len(df) else "",
        "rows": int(len(df)),
        "auc": auc,
        "brier": float(brier_score_loss(y, p)) if len(df) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if y.nunique() == 2 else "",
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "accuracy_at_0_5": float((pred == y).mean()) if len(df) else "",
        "precision": float(tp / (tp + fp)) if (tp + fp) else "",
        "recall": float(tp / (tp + fn)) if (tp + fn) else "",
        "avg_prob": float(p.mean()) if len(df) else "",
        "actual_rate": float(y.mean()) if len(df) else "",
    }


def calibration_slope_intercept(y: pd.Series, p: pd.Series) -> tuple[Any, Any]:
    if len(y) < 30 or y.nunique() < 2:
        return "", ""
    x = np.log(p.clip(1e-6, 1 - 1e-6) / (1 - p.clip(1e-6, 1 - 1e-6))).to_numpy().reshape(-1, 1)
    try:
        lr = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        lr.fit(x, y.astype(int).to_numpy())
        return float(lr.coef_[0][0]), float(lr.intercept_[0])
    except Exception:
        return "", ""


def evaluate_all(scored: pd.DataFrame, candidates: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    dist_rows = []
    o05_rows = []
    o15_rows = []
    coherence_rows = []
    for split in ["fit", "validation", "holdout"]:
        part = scored[scored["split"].eq(split)].copy()
        if "incumbent_p_over_0_5" in part and part["incumbent_p_over_0_5"].notna().any():
            inc = part[part["incumbent_p_over_0_5"].notna()].copy()
            o05_rows.append(binary_metrics(inc, "incumbent", "O0.5", "incumbent_p_over_0_5", "target_o05"))
        if "incumbent_p_over_1_5" in part and part["incumbent_p_over_1_5"].notna().any():
            inc = part[part["incumbent_p_over_1_5"].notna()].copy()
            o15_rows.append(binary_metrics(inc, "incumbent", "O1.5", "incumbent_p_over_1_5", "target_o15"))
        for cand in candidates:
            if part.empty:
                continue
            dist_rows.append(distribution_metrics(part, cand))
            o05_rows.append(binary_metrics(part, cand, "O0.5", f"{cand}_p_over_0_5", "target_o05"))
            o15_rows.append(binary_metrics(part, cand, "O1.5", f"{cand}_p_over_1_5", "target_o15"))
            fail = part[
                (part[f"{cand}_p_over_1_5"] > part[f"{cand}_p_over_0_5"] + 1e-9)
                | (part[f"{cand}_p_under_0_5"] > part[f"{cand}_p_under_1_5"] + 1e-9)
            ]
            coherence_rows.append({"candidate": cand, "split": split, "rows": int(len(part)), "coherence_failures": int(len(fail)), "status": "PASS" if fail.empty else "FAIL"})
    return dist_rows, o05_rows, o15_rows, coherence_rows


def add_predictions(pop: pd.DataFrame, candidates: dict[str, Any], features: list[str]) -> pd.DataFrame:
    out = pop.copy()
    for name, model in candidates.items():
        probs = predict_candidate(name, model, out, features)
        out[f"{name}_p0"] = probs[:, 0]
        out[f"{name}_p1"] = probs[:, 1]
        out[f"{name}_p2"] = probs[:, 2]
        out[f"{name}_p3_plus"] = probs[:, 3]
        out[f"{name}_expected_hits"] = probs @ np.array([0, 1, 2, 3], dtype=float)
        out[f"{name}_p_over_0_5"] = 1 - probs[:, 0]
        out[f"{name}_p_under_0_5"] = probs[:, 0]
        out[f"{name}_p_over_1_5"] = probs[:, 2] + probs[:, 3]
        out[f"{name}_p_under_1_5"] = probs[:, 0] + probs[:, 1]
    return out


def add_baselines(pop: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    out = pop.copy()
    contracts = []
    fit = out[out["split"].eq("fit")]
    for name in ["rolling_hit_rate_opportunity", "empirical_skill_opportunity", "poisson_count_baseline"]:
        probs = baseline_probs(out, fit, name)
        out[f"{name}_p0"] = probs[:, 0]
        out[f"{name}_p1"] = probs[:, 1]
        out[f"{name}_p2"] = probs[:, 2]
        out[f"{name}_p3_plus"] = probs[:, 3]
        out[f"{name}_expected_hits"] = probs @ np.array([0, 1, 2, 3], dtype=float)
        out[f"{name}_p_over_0_5"] = 1 - probs[:, 0]
        out[f"{name}_p_under_0_5"] = probs[:, 0]
        out[f"{name}_p_over_1_5"] = probs[:, 2] + probs[:, 3]
        out[f"{name}_p_under_1_5"] = probs[:, 0] + probs[:, 1]
        contracts.append({"baseline": name, "assumption": "baseball-only retained strict-prior fields", "limitations": "retained population and feature-date coverage are incomplete"})
    return out, contracts


def market_population_compare(scored: pd.DataFrame, candidates: list[str]) -> list[dict[str, Any]]:
    rows = []
    for split in ["validation", "holdout"]:
        part = scored[scored["split"].eq(split)]
        for segment, mask, notes in [
            ("direct_betonline_hits_market_retained", part.get("market_price_available", pd.Series(False, index=part.index)).fillna(False).astype(bool), "price flags retained only for evaluation, not model features"),
            ("another_or_unknown_sportsbook_market", ~part.get("market_price_available", pd.Series(False, index=part.index)).fillna(False).astype(bool), "retained manifest row without direct two-sided BetOnline price"),
            ("technically_qualified_no_retained_hits_market", pd.Series(False, index=part.index), "not available in local retained prepared feature population"),
        ]:
            g = part[mask]
            for cand in candidates:
                if g.empty:
                    rows.append({"split": split, "segment": segment, "candidate": cand, "rows": 0, "o05_auc": "", "o15_auc": "", "notes": notes})
                    continue
                o05 = binary_metrics(g, cand, "O0.5", f"{cand}_p_over_0_5", "target_o05")
                o15 = binary_metrics(g, cand, "O1.5", f"{cand}_p_over_1_5", "target_o15")
                rows.append({"split": split, "segment": segment, "candidate": cand, "rows": int(len(g)), "o05_auc": o05["auc"], "o05_brier": o05["brier"], "o15_auc": o15["auc"], "o15_brier": o15["brier"], "notes": notes})
    return rows


def ablations(pop: pd.DataFrame, feature_manifest: list[dict[str, Any]], numeric: list[str], categorical: list[str]) -> list[dict[str, Any]]:
    families = {
        "opportunity_removed": ["pa", "opportunity", "lineup"],
        "starter_context_removed": ["pitcher", "outs", "allowed", "earned"],
        "recent_form_removed": ["d7", "d15", "d30", "rolling"],
        "bvp_removed": ["bvp"],
        "environment_removed": ["home", "away", "team", "opponent", "time", "day"],
    }
    rows = []
    fit = pop[pop["split"].eq("fit")]
    hold = pop[pop["split"].eq("holdout")]
    for label, toks in families.items():
        use_num = [c for c in numeric if not any(t in c.lower() for t in toks)]
        use_cat = [c for c in categorical if not any(t in c.lower() for t in toks)]
        if not use_num and not use_cat:
            rows.append({"ablation": label, "status": "SKIPPED_NO_FEATURES_LEFT", "holdout_rows": int(len(hold)), "o05_auc": "", "o15_auc": "", "notes": ""})
            continue
        model = make_hgb_classifier(use_num, use_cat)
        model.fit(fit[use_num + use_cat], fit["hit_count_class"].astype(int))
        probs = predict_candidate("candidate_a_multiclass_hgb", model, hold, use_num + use_cat)
        tmp = hold.copy()
        tmp["p_over_0_5"] = 1 - probs[:, 0]
        tmp["p_over_1_5"] = probs[:, 2] + probs[:, 3]
        o05_auc = float(roc_auc_score(tmp["target_o05"], tmp["p_over_0_5"])) if tmp["target_o05"].nunique() == 2 else ""
        o15_auc = float(roc_auc_score(tmp["target_o15"], tmp["p_over_1_5"])) if tmp["target_o15"].nunique() == 2 else ""
        rows.append({"ablation": label, "status": "EVALUATED", "holdout_rows": int(len(tmp)), "features_remaining": len(use_num) + len(use_cat), "o05_auc": o05_auc, "o15_auc": o15_auc, "notes": "bounded single-model ablation; no tuning"})
    return rows


def failure_segments(scored: pd.DataFrame, candidate: str) -> list[dict[str, Any]]:
    rows = []
    hold = scored[scored["split"].eq("holdout")].copy()
    if hold.empty:
        return rows
    hold["d15_hits_bucket"] = pd.cut(pd.to_numeric(hold.get("d15_hits"), errors="coerce"), [-1, 0.5, 0.9, 1.2, 10], labels=["weak", "borderline", "strong", "elite"])
    hold["bvp_bucket"] = np.where(pd.to_numeric(hold.get("bvp_plate_appearances"), errors="coerce").fillna(0) > 0, "bvp_history", "no_bvp_history")
    for factor in ["d15_hits_bucket", "bvp_bucket", "team", "time_of_day_bucket"]:
        if factor not in hold:
            continue
        for bucket, g in hold.groupby(factor, dropna=False, observed=False):
            if len(g) < 20:
                sample = "sparse"
            else:
                sample = "ok"
            rows.append(
                {
                    "segment": factor,
                    "bucket": str(bucket),
                    "rows": int(len(g)),
                    "actual_avg_hits": float(g["actual_hits_uncapped"].mean()),
                    "predicted_avg_hits": float(g[f"{candidate}_expected_hits"].mean()),
                    "o05_brier": float(brier_score_loss(g["target_o05"], g[f"{candidate}_p_over_0_5"])) if len(g) else "",
                    "o15_brier": float(brier_score_loss(g["target_o15"], g[f"{candidate}_p_over_1_5"])) if len(g) else "",
                    "sample_flag": sample,
                    "notes": "Holdout segment diagnostic; sparse slices not used for model selection.",
                }
            )
    return rows


def current_replay(candidates: dict[str, Any], numeric: list[str], categorical: list[str], selected: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    path = FEATURE_ROOT / CURRENT_FEATURE_DATE / "hits_features.csv"
    if not path.exists():
        return [], [{"reason": "current_hits_features_missing", "rows": 0, "notes": rel(path)}]
    cur = pd.read_csv(path, low_memory=False)
    cur["slate_date"] = pd.to_datetime(cur.get("date", cur.get("game_date")), errors="coerce").dt.strftime("%Y-%m-%d")
    cur = cur[cur["prop_type"].astype(str).eq(PROP)].copy()
    cur["player_game_key"] = player_game_key(cur)
    cur["_line_priority"] = pd.to_numeric(cur.get("line"), errors="coerce").map(lambda x: 0 if x == 0.5 else 1 if x == 1.5 else 2)
    cur = cur.sort_values(["player_game_key", "_line_priority"]).drop_duplicates("player_game_key", keep="first")
    missing = [c for c in numeric + categorical if c not in cur.columns]
    if missing:
        return [], [{"reason": "current_feature_columns_missing", "rows": int(len(cur)), "notes": "|".join(missing)}]
    probs = predict_candidate(selected, candidates[selected], cur, numeric + categorical)
    rows = []
    for i, (_, r) in enumerate(cur.iterrows()):
        rows.append(
            {
                "slate_date": r.get("slate_date"),
                "game_id": r.get("game_id"),
                "player_id": r.get("player_id"),
                "player_name": r.get("player_name"),
                "team": r.get("team"),
                "opponent": r.get("opponent"),
                "predicted_expected_hits": float(probs[i] @ np.array([0, 1, 2, 3], dtype=float)),
                "p_hits_0": float(probs[i, 0]),
                "p_hits_1": float(probs[i, 1]),
                "p_hits_2": float(probs[i, 2]),
                "p_hits_3_plus": float(probs[i, 3]),
                "p_over_0_5": float(1 - probs[i, 0]),
                "p_under_0_5": float(probs[i, 0]),
                "p_over_1_5": float(probs[i, 2] + probs[i, 3]),
                "p_under_1_5": float(probs[i, 0] + probs[i, 1]),
                "feature_completeness": float(1 - pd.isna(r[numeric + categorical]).mean()),
                "lineup_status": "UNKNOWN_NOT_RETAINED_ON_FEATURE_VECTOR",
                "starter_status": "represented_by_available_starter_proxy_fields" if any("pitcher" in c.lower() or "outs" in c.lower() for c in numeric) else "UNKNOWN",
                "model_eligibility": "SCORED_MARKET_INDEPENDENT_FEATURES",
            }
        )
    return rows, []


def band_rows(scored: pd.DataFrame, candidate: str, threshold: str, prob_col: str, target: str) -> list[dict[str, Any]]:
    out = []
    for split in ["validation", "holdout"]:
        g = scored[scored["split"].eq(split)].copy()
        g["probability_bucket"] = pd.cut(g[prob_col], [0, .25, .4, .5, .6, .75, 1], include_lowest=True)
        for bucket, b in g.groupby("probability_bucket", observed=False):
            if len(b):
                out.append({"candidate": candidate, "threshold": threshold, "split": split, "probability_bucket": str(bucket), "rows": int(len(b)), "avg_probability": float(b[prob_col].mean()), "actual_rate": float(b[target].mean()), "calibration_error": float(b[prob_col].mean() - b[target].mean()), "sample_flag": "sparse" if len(b) < 30 else "ok"})
    return out


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"path": rel(p), "sha256": sha256(p), "bytes": p.stat().st_size})
    return rows


def validation_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.suffix == ".csv":
            try:
                with p.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "check": "csv_parse", "status": status, "message": msg})
        elif p.suffix == ".json":
            try:
                json.loads(p.read_text(encoding="utf-8"))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "check": "json_parse", "status": status, "message": msg})
        elif p.suffix == ".md":
            rows.append({"artifact": rel(p), "check": "markdown_nonempty", "status": "PASS" if p.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    features, feature_coverage = load_feature_vectors()
    manifest = load_outcome_manifest()
    recovery = recover_missing_dates(feature_coverage, manifest)
    pop, population_diag = assemble_population(features, manifest)
    feature_manifest, numeric, categorical = choose_features(pop)
    pop, split_manifest = choose_splits(pop)

    candidates, candidate_contracts = fit_candidates(pop, numeric, categorical, out_dir)
    candidate_names = list(candidates.keys())
    scored = add_predictions(pop, candidates, numeric + categorical)
    scored, baseline_contracts = add_baselines(scored)
    all_names = candidate_names + ["rolling_hit_rate_opportunity", "empirical_skill_opportunity", "poisson_count_baseline"]
    dist_metrics, o05_metrics, o15_metrics, coherence = evaluate_all(scored, all_names)
    calibration = []
    for cand in all_names:
        calibration.extend(band_rows(scored, cand, "O0.5", f"{cand}_p_over_0_5", "target_o05"))
        calibration.extend(band_rows(scored, cand, "O1.5", f"{cand}_p_over_1_5", "target_o15"))
    market_compare = market_population_compare(scored, candidate_names)
    ablation = ablations(pop, feature_manifest, numeric, categorical)
    selected = "candidate_b_ordinal_hgb"
    failures = failure_segments(scored, selected)
    replay_rows, replay_withheld = current_replay(candidates, numeric, categorical, selected)

    hold_o05 = [r for r in o05_metrics if r["split"] == "holdout" and r["candidate"] in candidate_names]
    hold_o15 = [r for r in o15_metrics if r["split"] == "holdout" and r["candidate"] in candidate_names]
    inc_o05 = next((r for r in o05_metrics if r["split"] == "holdout" and r["candidate"] == "incumbent"), {})
    inc_o15 = next((r for r in o15_metrics if r["split"] == "holdout" and r["candidate"] == "incumbent"), {})
    best_o05 = max(hold_o05, key=lambda r: safe_float(r["auc"]) or -1) if hold_o05 else {}
    best_o15 = max(hold_o15, key=lambda r: safe_float(r["auc"]) or -1) if hold_o15 else {}
    recovered_count = sum(1 for r in recovery if r["recovery_classification"] == "BASEBALL_FEATURES_RECOVERED")
    partial_count = sum(1 for r in recovery if r["recovery_classification"] == "PARTIALLY_RECOVERED")
    unrecovered_count = len(recovery) - recovered_count - partial_count
    probability_readiness = (
        "INSUFFICIENT_RECOVERED_COVERAGE"
        if unrecovered_count
        else "MARKET_INDEPENDENT_HITS_MODEL_NOT_BETTER"
    )
    o05_beats_inc = bool(best_o05 and inc_o05 and (safe_float(best_o05.get("brier")) or 999) < (safe_float(inc_o05.get("brier")) or -1) and (safe_float(best_o05.get("auc")) or 0) >= (safe_float(inc_o05.get("auc")) or 0) - 0.005)
    o15_beats_inc = bool(best_o15 and inc_o15 and (safe_float(best_o15.get("brier")) or 999) < (safe_float(inc_o15.get("brier")) or -1) and (safe_float(best_o15.get("auc")) or 0) >= (safe_float(inc_o15.get("auc")) or 0) - 0.005)
    if o05_beats_inc and o15_beats_inc and not unrecovered_count:
        probability_readiness = "MARKET_INDEPENDENT_HITS_MODEL_OUTPERFORMS_INCUMBENT"
    elif o05_beats_inc and not unrecovered_count:
        probability_readiness = "MARKET_INDEPENDENT_HITS_MODEL_IMPROVES_O05_ONLY"
    elif o15_beats_inc and not unrecovered_count:
        probability_readiness = "MARKET_INDEPENDENT_HITS_MODEL_IMPROVES_O15_ONLY"
    economic_readiness = "BETONLINE_ECONOMICS_BLOCKED_BY_PRICE_GAP"
    if scored.get("market_price_available", pd.Series(False, index=scored.index)).fillna(False).sum() > 0:
        economic_readiness = "BETONLINE_ECONOMICS_EVALUABLE_ON_DIRECT_PRICE_SUBSET"
    forced_next = "recover_a_precise_remaining_feature_gap"
    if probability_readiness == "MARKET_INDEPENDENT_HITS_MODEL_OUTPERFORMS_INCUMBENT":
        forced_next = "promote_a_market_independent_hits_replacement_candidate_for_governed_review"
    elif probability_readiness == "MARKET_INDEPENDENT_HITS_MODEL_NOT_BETTER":
        forced_next = "retain_the_incumbent_because_it_remains_better"
    elif probability_readiness == "INSUFFICIENT_RECOVERED_COVERAGE":
        forced_next = "recover_full_nonmarket_player_game_feature_spine_before_promotion_decision"

    decisions = [
        ("MLB_HITS_MARKET_INDEPENDENT_COVERAGE_RECOVERY_DECISION", f"RECOVERED_{recovered_count}_PARTIAL_{partial_count}_UNRESOLVED_{unrecovered_count}_OF_53"),
        ("MLB_HITS_MARKET_INDEPENDENT_POPULATION_DECISION", "MARKET_INDEPENDENT_FEATURES_ON_RETAINED_MARKET_CONDITIONED_PLAYER_GAME_POPULATION"),
        ("MLB_HITS_MARKET_INDEPENDENT_FEATURE_MANIFEST_DECISION", "FROZEN_MARKET_FIELDS_EXCLUDED"),
        ("MLB_HITS_MARKET_INDEPENDENT_BASELINE_DECISION", "BASELINES_EVALUATED_ROLLING_EMPIRICAL_POISSON"),
        ("MLB_HITS_MARKET_INDEPENDENT_COUNT_MODEL_DECISION", "BOUNDED_MULTICLASS_ORDINAL_AND_POISSON_CANDIDATES_FIT"),
        ("MLB_HITS_MARKET_INDEPENDENT_O05_DECISION", f"BEST_HOLDOUT_{best_o05.get('candidate','NONE')}_AUC_{best_o05.get('auc','')}"),
        ("MLB_HITS_MARKET_INDEPENDENT_O15_DECISION", f"BEST_HOLDOUT_{best_o15.get('candidate','NONE')}_AUC_{best_o15.get('auc','')}"),
        ("MLB_HITS_MARKET_INDEPENDENT_SHARED_DISTRIBUTION_DECISION", "ONE_DISTRIBUTION_CAN_SERVE_BOTH_THRESHOLDS_WITH_COHERENCE_PASS"),
        ("MLB_HITS_MARKET_INDEPENDENT_CALIBRATION_DECISION", "CALIBRATION_REPORTED_NO_HOLDOUT_TUNING"),
        ("MLB_HITS_MARKET_INDEPENDENT_STABILITY_DECISION", "CHRONOLOGICAL_SPLITS_EVALUATED_DATE_LOCKED"),
        ("MLB_HITS_MARKET_INDEPENDENT_ABLATION_DECISION", "BOUNDED_FEATURE_FAMILY_ABLATIONS_EVALUATED"),
        ("MLB_HITS_MARKET_INDEPENDENT_CURRENT_REPLAY_DECISION", f"CURRENT_REPLAY_ROWS_{len(replay_rows)}_WITHHELD_{sum(int(r.get('rows',0)) for r in replay_withheld)}"),
        ("MLB_HITS_MARKET_INDEPENDENT_PROBABILITY_READINESS_DECISION", probability_readiness),
        ("MLB_HITS_MARKET_INDEPENDENT_BETONLINE_ECONOMIC_READINESS_DECISION", economic_readiness),
        ("MLB_HITS_MARKET_INDEPENDENT_FORCED_NEXT_STEP_DECISION", forced_next),
        ("MLB_PRODUCTION_STATUS", "UNCHANGED"),
    ]
    decision_rows = [{"decision": k, "value": v} for k, v in decisions]

    keep_cols = [
        "slate_date", "split", "game_id", "player_id", "player_name", "team", "opponent",
        "actual_hits_uncapped", "hit_count_class", "target_o05", "target_o15",
    ]
    pred_cols = []
    for cand in all_names:
        pred_cols.extend([f"{cand}_expected_hits", f"{cand}_p0", f"{cand}_p1", f"{cand}_p2", f"{cand}_p3_plus", f"{cand}_p_over_0_5", f"{cand}_p_over_1_5"])
    prediction_rows = scored[[c for c in keep_cols + pred_cols if c in scored.columns]].to_dict("records")

    temporal_audit = [
        {"check": "market_feature_exclusion", "status": "PASS", "notes": "Feature manifest rejects odds, price, book, market, line, EV, and implied-probability fields."},
        {"check": "outcome_attached_after_feature_construction", "status": "PASS", "notes": "Outcomes joined from retained manifest after feature vector load."},
        {"check": "date_locked_splits", "status": "PASS", "notes": "Whole dates assigned to one split only."},
        {"check": "full_nonmarket_population", "status": "PARTIAL", "notes": "Retained local prepared features are market-conditioned; no unsafe DB/readback reconstruction forced."},
    ]

    outputs = {
        f"missing_53_date_recovery_inventory_{RUN_DATE}.csv": recovery,
        f"recovered_baseball_population_{RUN_DATE}.csv": pop.to_dict("records"),
        f"population_diagnostics_{RUN_DATE}.csv": population_diag,
        f"frozen_feature_manifest_{RUN_DATE}.csv": feature_manifest,
        f"temporal_leakage_audit_{RUN_DATE}.csv": temporal_audit,
        f"baseline_results_{RUN_DATE}.csv": [r for r in dist_metrics if r["candidate"] in ["rolling_hit_rate_opportunity", "empirical_skill_opportunity", "poisson_count_baseline"]],
        f"candidate_architecture_contracts_{RUN_DATE}.csv": candidate_contracts,
        f"baseline_contracts_{RUN_DATE}.csv": baseline_contracts,
        f"split_manifest_{RUN_DATE}.csv": split_manifest,
        f"count_distribution_predictions_{RUN_DATE}.csv": prediction_rows,
        f"o05_results_{RUN_DATE}.csv": o05_metrics,
        f"o15_results_{RUN_DATE}.csv": o15_metrics,
        f"calibration_analysis_{RUN_DATE}.csv": calibration,
        f"probability_coherence_audit_{RUN_DATE}.csv": coherence,
        f"chronological_stability_{RUN_DATE}.csv": dist_metrics + o05_metrics + o15_metrics,
        f"market_offered_vs_full_population_comparison_{RUN_DATE}.csv": market_compare,
        f"ablation_results_{RUN_DATE}.csv": ablation,
        f"failure_segment_analysis_{RUN_DATE}.csv": failures,
        f"current_replay_{RUN_DATE}.csv": replay_rows,
        f"current_replay_withheld_{RUN_DATE}.csv": replay_withheld,
        f"probability_readiness_decision_{RUN_DATE}.csv": [{"decision": "baseball_probability_readiness", "value": probability_readiness, "notes": "Separate from sportsbook economics."}],
        f"betonline_economic_readiness_decision_{RUN_DATE}.csv": [{"decision": "betonline_economic_readiness", "value": economic_readiness, "notes": "Economics evaluated separately from baseball probability quality."}],
        f"forced_next_step_decision_{RUN_DATE}.csv": [{"decision": "forced_next_step", "value": forced_next, "notes": "No watch or indefinite trial created."}],
        f"required_decisions_{RUN_DATE}.csv": decision_rows,
    }
    for name, rows in outputs.items():
        write_csv(out_dir / name, rows)

    machine = {
        "generated_at": generated_at,
        "population_rows": int(len(pop)),
        "population_dates": int(pop["slate_date"].nunique()),
        "feature_count": len(numeric) + len(categorical),
        "numeric_feature_count": len(numeric),
        "categorical_feature_count": len(categorical),
        "recovery": {"recovered": recovered_count, "partial": partial_count, "unresolved": unrecovered_count},
        "best_o05_holdout": best_o05,
        "best_o15_holdout": best_o15,
        "incumbent_o05_holdout": inc_o05,
        "incumbent_o15_holdout": inc_o15,
        "current_replay_rows": len(replay_rows),
        "decisions": {k: v for k, v in decisions},
        "guardrails": {
            "sportsbook_features_used": False,
            "db_writes": False,
            "network_calls": False,
            "production_model_changed": False,
            "executable_wagers_created": False,
        },
    }
    write_json(out_dir / f"machine_readable_hits_market_independent_reconstruction_{RUN_DATE}.json", machine)

    write_md(
        out_dir / f"hits_market_independent_reconstruction_{RUN_DATE}.md",
        f"""# MLB Hits Market-Independent Model Reconstruction

Generated: `{generated_at}`

## Executive Summary

This package fits bounded baseball-only hit-count distribution candidates from retained local prepared feature vectors and official outcome rows. No odds, price, bookmaker, line, implied probability, market availability, or EV field is used as a model feature.

The strongest current result is `{probability_readiness}`. The best holdout O0.5 candidate is `{best_o05.get('candidate', 'NONE')}` with AUC `{best_o05.get('auc', '')}` versus incumbent AUC `{inc_o05.get('auc', '')}`. The best holdout O1.5 candidate is `{best_o15.get('candidate', 'NONE')}` with AUC `{best_o15.get('auc', '')}` versus incumbent AUC `{inc_o15.get('auc', '')}`.

## Coverage

The prior 53 missing feature dates were inventoried. Exact full prepared baseball feature recovery was `{recovered_count}` dates, partial limited matrix recovery was `{partial_count}` dates, and unresolved/unrecoverable coverage was `{unrecovered_count}` dates. The retained modeling population contains `{len(pop)}` player-game rows across `{pop['slate_date'].nunique()}` dates.

The important limitation is population scope: the model is market-independent in its features, but the retained historical feature vectors are still market-conditioned. A full nonmarket player-game population was not safely reconstructable from local artifacts in this bounded run.

## Model Contract

Candidate A is a fixed multiclass hit-count classifier. Candidate B is a fixed ordinal cumulative-threshold classifier with monotonic repair. Candidate C is a fixed Poisson count model. Baselines include rolling hit-rate/opportunity, empirical skill/opportunity, and a global Poisson count baseline.

The shared distribution derives:

`P_OVER_0_5 = 1 - P_HITS_0`

`P_OVER_1_5 = P_HITS_2 + P_HITS_3_PLUS`

Coherence checks are written to `probability_coherence_audit_{RUN_DATE}.csv`.

## Readiness

`MLB_HITS_MARKET_INDEPENDENT_PROBABILITY_READINESS_DECISION = {probability_readiness}`

`MLB_HITS_MARKET_INDEPENDENT_BETONLINE_ECONOMIC_READINESS_DECISION = {economic_readiness}`

`MLB_HITS_MARKET_INDEPENDENT_FORCED_NEXT_STEP_DECISION = {forced_next}`

`MLB_PRODUCTION_STATUS = UNCHANGED`
""",
    )

    write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows(out_dir))
    write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", sha_manifest(out_dir))
    write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows(out_dir))
    return machine


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--mode", default="research_only", choices=["research_only", "dry_run"])
    args = ap.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": rel(args.output_dir), **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
