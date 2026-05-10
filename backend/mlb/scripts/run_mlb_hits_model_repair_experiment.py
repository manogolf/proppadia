#!/usr/bin/env python3
"""Run a non-production hits model repair experiment.

This script tests whether adding subgroup/context features and splitting hits
models by line improves probability calibration. It does not write DB rows,
deploy models, create betting rules, or alter production model artifacts.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sqlalchemy import create_engine, text


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_SUMMARY = Path("backend/mlb/exports/model_diagnostics/hits_model_repair_experiment_summary.md")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_model_repair_experiment.csv")
DEFAULT_OUT_BAD_ZONE = Path("backend/mlb/exports/model_diagnostics/hits_bad_zone_before_after.csv")
DEFAULT_PREPARED_FEATURE_ROOT = Path("backend/mlb/exports/model_diagnostics/prepared_feature_vectors")

HOLDOUT_FROM = "2026-04-09"
HOLDOUT_TO = "2026-05-08"
DEFAULT_TRAIN_FROM = "2024-01-01"

RECENCY_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
RECENCY_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]
PROB_BINS = [0.0, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]

NUMERIC_FEATURES = [
    "line_num",
    "rolling_result_avg_7",
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "recency_deviation",
    "is_home_num",
]
CATEGORICAL_FEATURES = [
    "rolling_result_avg_7_bucket",
    "d7_hits_bucket",
    "d15_hits_bucket",
    "d30_hits_bucket",
    "is_home_bucket",
    "team_context",
    "opponent_context",
    "rr7_x_d15",
    "rr7_x_d30",
    "rr7_x_home",
    "d7_x_d30",
]


@dataclass(frozen=True)
class Variant:
    name: str
    family: str
    line_scope: float | None = None
    min_train_rows: int = 250
    min_holdout_rows: int = 25


VARIANTS = [
    Variant("B_logistic_subgroup_features", "logistic", None),
    Variant("B_gbt_subgroup_features", "gbt", None),
    Variant("C_logistic_hits_line_0_5", "logistic", 0.5),
    Variant("C_logistic_hits_line_1_5", "logistic", 1.5),
    Variant("D_gbt_hits_line_0_5", "gbt", 0.5),
    Variant("D_gbt_hits_line_1_5", "gbt", 1.5),
]


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _num(value: Any) -> float:
    out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(out) if pd.notna(out) else math.nan


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if pd.isna(pd.to_datetime(date, errors="coerce")):
            continue
        if date < from_date or date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_hits_reconcile(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    required = {
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "model_prob_over",
        "model_prob_under",
        "model_pick_side",
        "model_pick_prob",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
        "pnl_model_pick_1u",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-repair] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits reconcile rows found for holdout.")
    return pd.concat(frames, ignore_index=True)


def _table_columns(engine, table: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'mlb'
          AND table_name = :table
        """
    )
    with engine.connect() as conn:
        return {str(r[0]) for r in conn.execute(sql, {"table": table}).fetchall()}


def _select_optional(alias: str, columns: Sequence[str], available: set[str]) -> list[str]:
    return [f"{alias}.{col} AS {alias}_{col}" for col in columns if col in available]


def _fetch_training_features(engine, train_from: str, holdout_to: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    mt_cols = _table_columns(engine, "model_training_props")
    pds_cols = _table_columns(engine, "player_derived_stats")
    optional_mt = _select_optional(
        "mt",
        [
            "team",
            "opponent",
            "team_id",
            "opponent_team_id",
            "opponent_encoded",
            "is_home",
            "rolling_result_avg_7",
            "player_handedness",
            "batter_hand",
            "bats",
            "bat_side",
            "batting_order",
            "batting_order_spot",
            "lineup_spot",
            "starting_pitcher_hand",
            "opposing_pitcher_hand",
            "pitcher_hand",
            "pitcher_throws",
            "opposing_pitcher_throws",
            "starting_pitcher_throws",
        ],
        mt_cols,
    )
    optional_pds = _select_optional("pds", ["d7_hits", "d15_hits", "d30_hits"], pds_cols)
    optional_sql = ""
    if optional_mt or optional_pds:
        optional_sql = ",\n          " + ",\n          ".join([*optional_mt, *optional_pds])

    sql = text(
        f"""
        SELECT
          mt.game_date,
          mt.game_id,
          mt.player_id,
          mt.player_name,
          mt.prop_type,
          NULLIF(btrim(mt.prop_value::text), '')::numeric AS prop_value,
          NULLIF(btrim(mt.result::text), '')::numeric AS result,
          pfp.features AS pfp_features,
          pfp.feature_set_tag,
          pfp.model_tag
          {optional_sql}
        FROM mlb.model_training_props mt
        LEFT JOIN mlb.player_derived_stats pds
          ON pds.player_id = mt.player_id
         AND pds.game_id = mt.game_id
         AND pds.game_date = mt.game_date
        LEFT JOIN mlb.prop_features_precomputed pfp
          ON pfp.player_id = mt.player_id
         AND pfp.game_id = mt.game_id
         AND pfp.game_date = mt.game_date
         AND pfp.prop_type = mt.prop_type
        WHERE mt.prop_type = 'hits'
          AND mt.game_date BETWEEN :train_from AND :holdout_to
          AND btrim(mt.prop_value::text) <> ''
          AND btrim(mt.result::text) <> ''
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"train_from": train_from, "holdout_to": holdout_to})
    meta = {
        "model_training_props_optional_found": sorted(c.removeprefix("mt.") for c in optional_mt),
        "player_derived_stats_optional_found": sorted(c.removeprefix("pds.") for c in optional_pds),
        "rows_fetched": int(len(df)),
    }
    return df, meta


def _fetch_context_features(engine, from_date: str, to_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT
          pfp.game_date,
          pfp.game_id,
          pfp.player_id,
          NULL::text AS player_name,
          'hits'::text AS prop_type,
          NULL::numeric AS prop_value,
          NULL::numeric AS result,
          pfp.features AS pfp_features,
          pfp.feature_set_tag,
          pfp.model_tag,
          pds.d7_hits AS pds_d7_hits,
          pds.d15_hits AS pds_d15_hits,
          pds.d30_hits AS pds_d30_hits
        FROM mlb.prop_features_precomputed pfp
        LEFT JOIN mlb.player_derived_stats pds
          ON pds.player_id = pfp.player_id
         AND pds.game_id = pfp.game_id
         AND pds.game_date = pfp.game_date
        WHERE pfp.prop_type = 'hits'
          AND pfp.game_date BETWEEN :from_date AND :to_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"from_date": from_date, "to_date": to_date})


def _load_prepared_feature_vectors(root: Path, dates: Sequence[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    missing: list[str] = []
    required = {"date", "player_id", "game_id", "prop_type", "line", "rolling_result_avg_7", "d7_hits"}
    for date in sorted(set(str(d) for d in dates if str(d))):
        path = root / date / "hits_features.csv"
        if not path.exists():
            missing.append(str(path))
            continue
        df = pd.read_csv(path, low_memory=False)
        absent = sorted(required - set(df.columns))
        if absent:
            raise SystemExit(f"Prepared hits feature vector file missing columns {absent}: {path}")
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        df["source_prepared_feature_file"] = str(path)
        frames.append(df)
    if missing:
        sample = "\n".join(f"  - {p}" for p in missing[:20])
        extra = "" if len(missing) <= 20 else f"\n  ... {len(missing) - 20} more"
        raise SystemExit(
            "Missing prepared hits feature vector files. Backfill these before rerunning the repair experiment:\n"
            f"{sample}{extra}\n\n"
            "Expected path pattern:\n"
            "  backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date>/hits_features.csv\n\n"
            "Example one-date backfill:\n"
            "  source backend/.env && SUPABASE_DB_URL=\"$SUPABASE_DB_URL\" .venv/bin/python "
            "backend/mlb/scripts/build_mlb_predictions_wide.py "
            "--slate-date <date> --prop-types hits "
            "--odds-snapshot-in backend/mlb/exports/odds_history/<date>/<snapshot>.json "
            "--output tmp/mlb_predictions_wide_hits_feature_debug_<date>.csv "
            "--feature-debug-out-dir backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date> "
            "--require-min-rows 1"
        )
    if not frames:
        raise SystemExit("No prepared hits feature vector files loaded.")
    out = pd.concat(frames, ignore_index=True)
    meta = {
        "prepared_feature_root": str(root),
        "prepared_feature_files": int(len(frames)),
        "prepared_feature_rows_raw": int(len(out)),
    }
    return out, meta


def _parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _bucket(series: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(series, errors="coerce"), bins=RECENCY_BINS, labels=RECENCY_LABELS, right=False)


def _prep_feature_frame(raw: pd.DataFrame, *, require_label: bool = True) -> pd.DataFrame:
    out = raw.copy()
    parsed = out["pfp_features"].map(_parse_json_obj) if "pfp_features" in out.columns else pd.Series([{}] * len(out))
    for feature in ["rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits"]:
        candidates = [feature, f"mt_{feature}", f"pds_{feature}"]
        values = pd.Series(np.nan, index=out.index, dtype="float64")
        for col in candidates:
            if col in out.columns:
                values = values.fillna(pd.to_numeric(out[col], errors="coerce"))
        mask = values.isna()
        if mask.any():
            values.loc[mask] = parsed[mask].map(lambda obj, key=feature: obj.get(key)).pipe(pd.to_numeric, errors="coerce")
        out[feature] = values
    if out["rolling_result_avg_7"].isna().any() and out["d7_hits"].notna().any():
        out["rolling_result_avg_7"] = out["rolling_result_avg_7"].fillna(out["d7_hits"])

    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    prop_values = out["prop_value"] if "prop_value" in out.columns else pd.Series(np.nan, index=out.index)
    result_values = out["result"] if "result" in out.columns else pd.Series(np.nan, index=out.index)
    out["line_num"] = pd.to_numeric(prop_values, errors="coerce")
    out["result_num"] = pd.to_numeric(result_values, errors="coerce")
    out["y_over"] = np.where(out["result_num"] > out["line_num"], 1, np.where(out["result_num"] < out["line_num"], 0, np.nan))
    out["recency_deviation"] = pd.to_numeric(out["d7_hits"], errors="coerce") - pd.to_numeric(out["d30_hits"], errors="coerce")

    if "mt_is_home" in out.columns:
        home_raw = out["mt_is_home"]
    elif "is_home" in out.columns:
        home_raw = out["is_home"]
    else:
        home_raw = parsed.map(lambda obj: obj.get("is_home") if isinstance(obj, dict) else np.nan)
    home_text = home_raw.map(lambda v: _clean(v).lower())
    out["is_home_num"] = np.where(home_text.isin({"1", "true", "t", "home", "yes"}), 1.0, np.where(home_text.isin({"0", "false", "f", "away", "no"}), 0.0, np.nan))
    out["is_home_bucket"] = np.where(out["is_home_num"].eq(1.0), "home", np.where(out["is_home_num"].eq(0.0), "away", "unknown"))

    for feature in ["rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits"]:
        out[f"{feature}_bucket"] = _bucket(out[feature]).astype("object").fillna("missing").astype(str)

    def first_col(row: pd.Series, cols: Sequence[str]) -> str:
        for col in cols:
            if col in row.index:
                val = _clean(row.get(col)).upper()
                if val:
                    return val
        obj = _parse_json_obj(row.get("pfp_features")) if "pfp_features" in row.index else {}
        for col in cols:
            key = col.removeprefix("mt_")
            val = _clean(obj.get(key)).upper()
            if val:
                return val
        return "unknown"

    out["team_context"] = out.apply(lambda r: first_col(r, ["mt_team", "team", "mt_team_id", "team_id"]), axis=1)
    out["opponent_context"] = out.apply(
        lambda r: first_col(r, ["mt_opponent", "opponent", "mt_opponent_team_id", "opponent_team_id", "mt_opponent_encoded"]),
        axis=1,
    )
    out["rr7_x_d15"] = out["rolling_result_avg_7_bucket"] + "|" + out["d15_hits_bucket"]
    out["rr7_x_d30"] = out["rolling_result_avg_7_bucket"] + "|" + out["d30_hits_bucket"]
    out["rr7_x_home"] = out["rolling_result_avg_7_bucket"] + "|" + out["is_home_bucket"]
    out["d7_x_d30"] = out["d7_hits_bucket"] + "|" + out["d30_hits_bucket"]

    if require_label:
        out = out[out["y_over"].isin([0, 1]) & out["line_num"].notna()].copy()
    out = out.sort_values(["date_key", "game_id_key", "player_id_key", "line_num", "feature_set_tag", "model_tag"]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key", "line_num", "result_num"] if require_label else ["date_key", "game_id_key", "player_id_key"],
        keep="last",
    )
    keep = [
        "date_key",
        "game_id_key",
        "player_id_key",
        "player_name",
        "line_num",
        "result_num",
        "y_over",
        *NUMERIC_FEATURES,
        *CATEGORICAL_FEATURES,
    ]
    keep = list(dict.fromkeys([c for c in keep if c in out.columns]))
    if not require_label:
        keep = [c for c in keep if c not in {"line_num", "result_num", "y_over"}]
    return out[keep].copy()


def _prep_prepared_feature_frame(raw: pd.DataFrame) -> pd.DataFrame:
    out = raw.copy()
    rename = {"date": "game_date"}
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns and v not in out.columns})
    for feature in ["rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits"]:
        if feature not in out.columns:
            raise SystemExit(f"Prepared feature vectors missing required feature column: {feature}")
        out[feature] = pd.to_numeric(out[feature], errors="coerce")
    if out["rolling_result_avg_7"].isna().any():
        raise SystemExit("Prepared feature vectors contain null rolling_result_avg_7; no fallback is allowed.")
    if out["d7_hits"].isna().any():
        raise SystemExit("Prepared feature vectors contain null d7_hits; no fallback is allowed.")

    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["line_num"] = pd.to_numeric(out["line"], errors="coerce")
    out["recency_deviation"] = pd.to_numeric(out["d7_hits"], errors="coerce") - pd.to_numeric(out["d30_hits"], errors="coerce")

    home_raw = out["is_home"] if "is_home" in out.columns else pd.Series(np.nan, index=out.index)
    home_text = home_raw.map(lambda v: _clean(v).lower())
    out["is_home_num"] = np.where(home_text.isin({"1", "true", "t", "home", "yes"}), 1.0, np.where(home_text.isin({"0", "false", "f", "away", "no"}), 0.0, np.nan))
    out["is_home_bucket"] = np.where(out["is_home_num"].eq(1.0), "home", np.where(out["is_home_num"].eq(0.0), "away", "unknown"))
    for feature in ["rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits"]:
        out[f"{feature}_bucket"] = _bucket(out[feature]).astype("object").fillna("missing").astype(str)
    out["team_context"] = out["team"].map(lambda v: _clean(v).upper() or "unknown") if "team" in out.columns else "unknown"
    out["opponent_context"] = out["opponent"].map(lambda v: _clean(v).upper() or "unknown") if "opponent" in out.columns else "unknown"
    out["rr7_x_d15"] = out["rolling_result_avg_7_bucket"] + "|" + out["d15_hits_bucket"]
    out["rr7_x_d30"] = out["rolling_result_avg_7_bucket"] + "|" + out["d30_hits_bucket"]
    out["rr7_x_home"] = out["rolling_result_avg_7_bucket"] + "|" + out["is_home_bucket"]
    out["d7_x_d30"] = out["d7_hits_bucket"] + "|" + out["d30_hits_bucket"]

    keep = [
        "date_key",
        "game_id_key",
        "player_id_key",
        "line_num",
        "player_name",
        "source_prepared_feature_file",
        *NUMERIC_FEATURES,
        *CATEGORICAL_FEATURES,
    ]
    keep = list(dict.fromkeys([c for c in keep if c in out.columns]))
    return out[keep].sort_values(["date_key", "game_id_key", "player_id_key", "line_num"]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key", "line_num"],
        keep="last",
    )


def _prepare_holdout(reconcile: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    feature_cols = [
        "date_key",
        "game_id_key",
        "player_id_key",
        "line_num",
        *NUMERIC_FEATURES,
        *CATEGORICAL_FEATURES,
    ]
    feature_cols = list(dict.fromkeys([c for c in feature_cols if c in features.columns]))
    feature_keys = ["date_key", "game_id_key", "player_id_key", "line_num"]
    merged = work.merge(features[feature_cols].drop_duplicates(feature_keys), how="left", on=feature_keys)
    merged["line_num"] = pd.to_numeric(merged["line"], errors="coerce")
    return merged


def _one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:  # older sklearn
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def _make_model(family: str) -> Pipeline:
    numeric = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())])
    categorical = Pipeline([("imputer", SimpleImputer(strategy="constant", fill_value="missing")), ("onehot", _one_hot_encoder())])
    pre = ColumnTransformer(
        [
            ("num", numeric, NUMERIC_FEATURES),
            ("cat", categorical, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    if family == "gbt":
        clf = GradientBoostingClassifier(random_state=7, n_estimators=120, learning_rate=0.04, max_depth=2, subsample=0.85)
    else:
        clf = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
    return Pipeline([("preprocess", pre), ("model", clf)])


def _valid_metric_rows(df: pd.DataFrame, prob_col: str, outcome_col: str) -> pd.DataFrame:
    out = df.copy()
    out[prob_col] = pd.to_numeric(out[prob_col], errors="coerce")
    out["win"] = out[outcome_col].map(lambda v: _clean(v).lower()).map({"win": 1.0, "loss": 0.0})
    return out[out[prob_col].notna() & out["win"].isin([0.0, 1.0])].copy()


def _score_metrics(probs: pd.Series, wins: pd.Series) -> dict[str, float]:
    y = pd.to_numeric(wins, errors="coerce").astype(float)
    p = pd.to_numeric(probs, errors="coerce").clip(1e-6, 1 - 1e-6)
    valid = p.notna() & y.notna()
    if not valid.any():
        return {"brier": np.nan, "log_loss": np.nan, "auc": np.nan}
    yv = y[valid].to_numpy()
    pv = p[valid].to_numpy()
    auc = np.nan
    if len(np.unique(yv)) == 2:
        auc = float(roc_auc_score(yv, pv))
    return {
        "brier": float(brier_score_loss(yv, pv)),
        "log_loss": float(log_loss(yv, pv, labels=[0, 1])),
        "auc": auc,
    }


def _side_eval_rows(scored: pd.DataFrame, variant: str, over_prob_col: str, scope: str) -> pd.DataFrame:
    pieces = []
    for side in ("over", "under"):
        prob = pd.to_numeric(scored[over_prob_col], errors="coerce")
        if side == "under":
            prob = 1.0 - prob
        part = pd.DataFrame(
            {
                "variant": variant,
                "scope": scope,
                "date": scored["date_key"],
                "player_name": scored["player_name"],
                "line": scored["line_num"],
                "side": side,
                "prob": prob,
                "outcome": scored[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
                "pnl": pd.to_numeric(scored[f"pnl_{side}_1u"], errors="coerce"),
                "rolling_result_avg_7": pd.to_numeric(scored["rolling_result_avg_7"], errors="coerce"),
                "d7_hits": pd.to_numeric(scored["d7_hits"], errors="coerce"),
                "d15_hits": pd.to_numeric(scored["d15_hits"], errors="coerce"),
                "d30_hits": pd.to_numeric(scored["d30_hits"], errors="coerce"),
            }
        )
        pieces.append(part)
    out = pd.concat(pieces, ignore_index=True)
    out = out[out["outcome"].isin({"win", "loss"}) & out["prob"].notna()].copy()
    out["win"] = out["outcome"].eq("win").astype(float)
    out["prob_bucket"] = pd.cut(out["prob"], bins=PROB_BINS, labels=PROB_LABELS, right=False)
    return out


def _overall_row(side_rows: pd.DataFrame, variant: str, scope: str, notes: str = "") -> dict[str, Any]:
    bets = int(len(side_rows))
    wins = int(side_rows["win"].sum()) if bets else 0
    profit = float(pd.to_numeric(side_rows["pnl"], errors="coerce").fillna(0.0).sum()) if bets else np.nan
    metrics = _score_metrics(side_rows["prob"], side_rows["win"])
    return {
        "variant": variant,
        "scope": scope,
        "metric_type": "overall_side_probability",
        "prob_bucket": "ALL",
        "side": "ALL",
        "bets": bets,
        "wins": wins,
        "win_rate": wins / bets if bets else np.nan,
        "avg_model_prob": float(side_rows["prob"].mean()) if bets else np.nan,
        "calibration_error": float(side_rows["win"].mean() - side_rows["prob"].mean()) if bets else np.nan,
        "brier": metrics["brier"],
        "log_loss": metrics["log_loss"],
        "auc": metrics["auc"],
        "profit_units": profit,
        "roi": profit / bets if bets else np.nan,
        "notes": notes,
    }


def _bucket_rows(side_rows: pd.DataFrame, variant: str, scope: str) -> list[dict[str, Any]]:
    rows = []
    for keys, group in side_rows.groupby(["prob_bucket", "side"], observed=True, dropna=False):
        prob_bucket, side = keys
        row = _overall_row(group, variant, scope)
        row["metric_type"] = "probability_bucket_by_side"
        row["prob_bucket"] = str(prob_bucket)
        row["side"] = str(side)
        rows.append(row)
    for key, group in side_rows.groupby("prob_bucket", observed=True, dropna=False):
        row = _overall_row(group, variant, scope)
        row["metric_type"] = "probability_bucket"
        row["prob_bucket"] = str(key)
        row["side"] = "ALL"
        rows.append(row)
    return rows


def _model_pick_metrics(scored: pd.DataFrame, variant: str, over_prob_col: str, scope: str) -> dict[str, Any]:
    work = scored.copy()
    p_over = pd.to_numeric(work[over_prob_col], errors="coerce")
    work["pick_side_experiment"] = np.where(p_over >= 0.5, "over", "under")
    work["pick_prob_experiment"] = np.where(p_over >= 0.5, p_over, 1.0 - p_over)
    work["pick_win"] = np.where(
        work["pick_side_experiment"].eq("over"),
        work["actual_over_outcome"].map(lambda v: _clean(v).lower()).map({"win": 1.0, "loss": 0.0}),
        work["actual_under_outcome"].map(lambda v: _clean(v).lower()).map({"win": 1.0, "loss": 0.0}),
    )
    work["pick_pnl"] = np.where(work["pick_side_experiment"].eq("over"), pd.to_numeric(work["pnl_over_1u"], errors="coerce"), pd.to_numeric(work["pnl_under_1u"], errors="coerce"))
    work = work[work["pick_prob_experiment"].notna() & pd.Series(work["pick_win"]).isin([0.0, 1.0])].copy()
    bets = int(len(work))
    wins = int(pd.Series(work["pick_win"]).sum()) if bets else 0
    profit = float(pd.to_numeric(work["pick_pnl"], errors="coerce").fillna(0.0).sum()) if bets else np.nan
    return {
        "variant": variant,
        "scope": scope,
        "metric_type": "model_pick_secondary_roi",
        "prob_bucket": "ALL",
        "side": "MODEL_PICK",
        "bets": bets,
        "wins": wins,
        "win_rate": wins / bets if bets else np.nan,
        "avg_model_prob": float(pd.Series(work["pick_prob_experiment"]).mean()) if bets else np.nan,
        "calibration_error": float(pd.Series(work["pick_win"]).mean() - pd.Series(work["pick_prob_experiment"]).mean()) if bets else np.nan,
        "brier": np.nan,
        "log_loss": np.nan,
        "auc": np.nan,
        "profit_units": profit,
        "roi": profit / bets if bets else np.nan,
        "notes": "ROI is secondary diagnostic only; no betting rules are derived.",
    }


def _bad_zone_metrics(side_rows: pd.DataFrame, variant: str, scope: str) -> list[dict[str, Any]]:
    definitions = {
        "under_0_5_mid_low_rr7_0_50_0_75": (
            side_rows["side"].eq("under")
            & pd.to_numeric(side_rows["line"], errors="coerce").eq(0.5)
            & pd.to_numeric(side_rows["rolling_result_avg_7"], errors="coerce").ge(0.50)
            & pd.to_numeric(side_rows["rolling_result_avg_7"], errors="coerce").lt(0.75)
        ),
        "under_0_5_mid_low_rr7_0_50_0_75_high_conf_ge_0_60": (
            side_rows["side"].eq("under")
            & pd.to_numeric(side_rows["line"], errors="coerce").eq(0.5)
            & pd.to_numeric(side_rows["rolling_result_avg_7"], errors="coerce").ge(0.50)
            & pd.to_numeric(side_rows["rolling_result_avg_7"], errors="coerce").lt(0.75)
            & pd.to_numeric(side_rows["prob"], errors="coerce").ge(0.60)
        ),
    }
    rows: list[dict[str, Any]] = []
    for bad_zone, mask in definitions.items():
        group = side_rows[mask].copy()
        bets = int(len(group))
        wins = int(group["win"].sum()) if bets else 0
        metrics = _score_metrics(group["prob"], group["win"])
        rows.append(
            {
                "variant": variant,
                "scope": scope,
                "bad_zone": bad_zone,
                "bets": bets,
                "wins": wins,
                "actual_win_rate": wins / bets if bets else np.nan,
                "avg_model_prob": float(group["prob"].mean()) if bets else np.nan,
                "calibration_error": float(group["win"].mean() - group["prob"].mean()) if bets else np.nan,
                "brier": metrics["brier"],
                "log_loss": metrics["log_loss"],
                "auc": metrics["auc"],
                "prob_std": float(group["prob"].std(ddof=0)) if bets else np.nan,
                "prob_p05": float(group["prob"].quantile(0.05)) if bets else np.nan,
                "prob_p95": float(group["prob"].quantile(0.95)) if bets else np.nan,
            }
        )
    return rows


def _baseline_scored(holdout: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    scored = holdout.copy()
    scored["baseline_prob_over"] = pd.to_numeric(scored["model_prob_over"], errors="coerce")
    side_rows = _side_eval_rows(scored, "A_baseline_current_hits_model", "baseline_prob_over", "all_hits_lines")
    rows = [_overall_row(side_rows, "A_baseline_current_hits_model", "all_hits_lines")]
    rows.extend(_bucket_rows(side_rows, "A_baseline_current_hits_model", "all_hits_lines"))
    rows.append(
        {
            **_model_pick_metrics(scored, "A_baseline_current_hits_model", "baseline_prob_over", "all_hits_lines"),
            "notes": "Uses existing production predictions as control.",
        }
    )
    bad_rows = _bad_zone_metrics(side_rows, "A_baseline_current_hits_model", "all_hits_lines")
    return side_rows, pd.DataFrame(rows), bad_rows


def _run_variant(variant: Variant, train: pd.DataFrame, holdout: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    train_part = train.copy()
    holdout_part = holdout.copy()
    scope = "all_hits_lines"
    if variant.line_scope is not None:
        scope = f"line_{str(variant.line_scope).replace('.', '_')}"
        train_part = train_part[pd.to_numeric(train_part["line_num"], errors="coerce").eq(variant.line_scope)].copy()
        holdout_part = holdout_part[pd.to_numeric(holdout_part["line_num"], errors="coerce").eq(variant.line_scope)].copy()
    if len(train_part) < variant.min_train_rows or len(holdout_part) < variant.min_holdout_rows:
        note = f"skipped: train_rows={len(train_part)}, holdout_rows={len(holdout_part)} below minimum"
        row = {
            "variant": variant.name,
            "scope": scope,
            "metric_type": "skipped",
            "prob_bucket": "ALL",
            "side": "ALL",
            "bets": 0,
            "wins": 0,
            "win_rate": np.nan,
            "avg_model_prob": np.nan,
            "calibration_error": np.nan,
            "brier": np.nan,
            "log_loss": np.nan,
            "auc": np.nan,
            "profit_units": np.nan,
            "roi": np.nan,
            "notes": note,
        }
        return pd.DataFrame(), [row], []

    model = _make_model(variant.family)
    X_train = train_part[NUMERIC_FEATURES + CATEGORICAL_FEATURES].copy()
    y_train = train_part["y_over"].astype(int)
    model.fit(X_train, y_train)
    scored = holdout_part.copy()
    scored[f"{variant.name}_prob_over"] = model.predict_proba(scored[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[:, 1]
    side_rows = _side_eval_rows(scored, variant.name, f"{variant.name}_prob_over", scope)
    rows = [
        _overall_row(
            side_rows,
            variant.name,
            scope,
            notes=f"train_rows={len(train_part)}; holdout_rows={len(holdout_part)}; family={variant.family}",
        )
    ]
    rows.extend(_bucket_rows(side_rows, variant.name, scope))
    rows.append(_model_pick_metrics(scored, variant.name, f"{variant.name}_prob_over", scope))
    bad_rows = _bad_zone_metrics(side_rows, variant.name, scope)
    return side_rows, rows, bad_rows


def _write_summary(
    path: Path,
    experiment: pd.DataFrame,
    bad_zone: pd.DataFrame,
    meta: dict[str, Any],
) -> None:
    def md_table(df: pd.DataFrame) -> str:
        if df.empty:
            return "_No rows._"
        display = df.copy()
        for col in display.columns:
            if pd.api.types.is_numeric_dtype(display[col]):
                display[col] = display[col].map(lambda v: "" if pd.isna(v) else f"{float(v):.4f}")
            else:
                display[col] = display[col].map(lambda v: "" if pd.isna(v) else str(v))
        cols = list(display.columns)
        lines = [
            "| " + " | ".join(cols) + " |",
            "| " + " | ".join(["---"] * len(cols)) + " |",
        ]
        for _, row in display.iterrows():
            lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in cols) + " |")
        return "\n".join(lines)

    path.parent.mkdir(parents=True, exist_ok=True)
    overall = experiment[experiment["metric_type"].eq("overall_side_probability")].copy()
    model_pick = experiment[experiment["metric_type"].eq("model_pick_secondary_roi")].copy()
    bad_main = bad_zone[bad_zone["bad_zone"].eq("under_0_5_mid_low_rr7_0_50_0_75")].copy()
    bad_high_conf = bad_zone[bad_zone["bad_zone"].eq("under_0_5_mid_low_rr7_0_50_0_75_high_conf_ge_0_60")].copy()
    baseline_bad = bad_main[bad_main["variant"].eq("A_baseline_current_hits_model")]
    baseline_error = float(baseline_bad["calibration_error"].iloc[0]) if not baseline_bad.empty else np.nan
    bad_main["abs_error_delta_vs_baseline"] = bad_main["calibration_error"].abs() - abs(baseline_error)
    best_bad = bad_main.sort_values("abs_error_delta_vs_baseline", na_position="last").head(5)
    baseline_high_conf = bad_high_conf[bad_high_conf["variant"].eq("A_baseline_current_hits_model")]
    baseline_high_conf_error = (
        float(baseline_high_conf["calibration_error"].iloc[0]) if not baseline_high_conf.empty else np.nan
    )
    bad_high_conf["abs_error_delta_vs_baseline"] = (
        bad_high_conf["calibration_error"].abs() - abs(baseline_high_conf_error)
    )
    lines = [
        "# Hits Model Repair Experiment",
        "",
        "Non-production diagnostic experiment. No model artifact was deployed, no frontend changed, and no betting rules were created.",
        "",
        "## Setup",
        f"- Train window starts: `{meta.get('train_from')}`",
        f"- Holdout window: `{meta.get('holdout_from')}` to `{meta.get('holdout_to')}`",
        f"- Training rows fetched: `{meta.get('rows_fetched')}`",
        f"- Holdout reconcile rows: `{meta.get('holdout_rows')}`",
        f"- Holdout prepared feature rows: `{meta.get('holdout_feature_rows_after_prep')}`",
        f"- Prepared feature root: `{meta.get('prepared_feature_root', '')}`",
        f"- Prepared feature files loaded: `{meta.get('prepared_feature_files', '')}`",
        f"- Features found from player_derived_stats: `{', '.join(meta.get('player_derived_stats_optional_found', [])) or 'none'}`",
        f"- rolling_result_avg_7 source note: `{meta.get('rolling_result_avg_7_source_note', 'prepared_feature_vectors')}`",
        "",
        "## Variants",
        "- A: existing production hits probabilities as baseline control.",
        "- B: hits models with subgroup buckets, recency deviation, home/away, team/opponent context, and interaction terms.",
        "- C: separate line-specific logistic models for hits 0.5 and 1.5 where sample supports it.",
        "- D: gradient boosted tree comparison with the same subgroup features.",
        "",
        "## Overall Calibration",
        md_table(
            overall[["variant", "scope", "bets", "avg_model_prob", "win_rate", "calibration_error", "brier", "log_loss", "auc"]]
            .sort_values(["scope", "brier"], na_position="last")
        ),
        "",
        "## Model Pick Secondary Diagnostic",
        md_table(
            model_pick[["variant", "scope", "bets", "win_rate", "avg_model_prob", "calibration_error", "profit_units", "roi"]]
            .sort_values(["scope", "roi"], ascending=[True, False], na_position="last")
        ),
        "",
        "## Bad Zone: Hits Under 0.5, rolling_result_avg_7 0.50-0.75",
        md_table(
            best_bad[
                [
                    "variant",
                    "scope",
                    "bets",
                    "avg_model_prob",
                    "actual_win_rate",
                    "calibration_error",
                    "brier",
                    "log_loss",
                    "prob_std",
                    "abs_error_delta_vs_baseline",
                ]
            ]
        ),
        "",
        "## High-Confidence Bad Zone: Hits Under 0.5, rolling_result_avg_7 0.50-0.75, predicted probability >= 0.60",
        md_table(
            bad_high_conf[
                [
                    "variant",
                    "scope",
                    "bets",
                    "avg_model_prob",
                    "actual_win_rate",
                    "calibration_error",
                    "brier",
                    "log_loss",
                    "prob_std",
                    "abs_error_delta_vs_baseline",
                ]
            ].sort_values("abs_error_delta_vs_baseline", na_position="last")
        ),
        "",
        "## Success Read",
        "- Success means materially smaller absolute bad-zone calibration error, no worse overall hits Brier/log loss, and non-flat probability outputs.",
        "- ROI is included only as a secondary diagnostic in the CSV, not as an optimization target.",
    ]
    path.write_text("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> None:
    paths = _discover_reconcile_files(args.reconcile_root, args.holdout_from, args.holdout_to)
    if not paths:
        raise SystemExit(f"No reconcile_rows.csv files found for {args.holdout_from} to {args.holdout_to}.")
    reconcile = _load_hits_reconcile(paths)
    reconcile_dates = sorted(set(reconcile["game_date"].map(_date_key)))
    engine = create_engine(_db_url())
    raw_train_features, meta = _fetch_training_features(engine, args.train_from, args.holdout_to)
    raw_holdout_features, prepared_meta = _load_prepared_feature_vectors(args.prepared_feature_root, reconcile_dates)
    meta.update(prepared_meta)
    train_features = _prep_feature_frame(raw_train_features, require_label=True)
    holdout_features = _prep_prepared_feature_frame(raw_holdout_features)

    train = train_features[train_features["date_key"].lt(args.holdout_from)].copy()
    holdout = _prepare_holdout(reconcile, holdout_features)
    holdout = holdout[holdout[NUMERIC_FEATURES + CATEGORICAL_FEATURES].notna().any(axis=1)].copy()
    if holdout.empty:
        raise SystemExit("No holdout reconcile rows matched feature vectors.")

    all_rows: list[dict[str, Any]] = []
    all_bad_rows: list[dict[str, Any]] = []
    _, baseline_rows, baseline_bad = _baseline_scored(holdout)
    all_rows.extend(baseline_rows.to_dict("records"))
    all_bad_rows.extend(baseline_bad)

    for variant in VARIANTS:
        _, rows, bad_rows = _run_variant(variant, train, holdout)
        all_rows.extend(rows)
        all_bad_rows.extend(bad_rows)

    experiment = pd.DataFrame(all_rows)
    bad_zone = pd.DataFrame(all_bad_rows)
    for out in [args.out_csv, args.bad_zone_csv, args.out_summary]:
        out.parent.mkdir(parents=True, exist_ok=True)
    experiment.to_csv(args.out_csv, index=False)
    bad_zone.to_csv(args.bad_zone_csv, index=False)
    meta.update(
        {
            "train_from": args.train_from,
            "holdout_from": args.holdout_from,
            "holdout_to": args.holdout_to,
            "holdout_rows": int(len(holdout)),
            "train_rows_after_prep": int(len(train)),
            "holdout_feature_rows_after_prep": int(len(holdout_features)),
            "rolling_result_avg_7_source_note": "loaded directly from prepared feature vector export; no holdout fallback or feature reconstruction used",
        }
    )
    _write_summary(args.out_summary, experiment, bad_zone, meta)

    overall = experiment[experiment["metric_type"].eq("overall_side_probability")]
    print(f"Wrote {args.out_csv}")
    print(f"Wrote {args.bad_zone_csv}")
    print(f"Wrote {args.out_summary}")
    print(overall[["variant", "scope", "bets", "win_rate", "avg_model_prob", "calibration_error", "brier", "log_loss", "auc"]].to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run non-production hits model repair experiment.")
    parser.add_argument("--reconcile-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--train-from", default=DEFAULT_TRAIN_FROM)
    parser.add_argument("--holdout-from", default=HOLDOUT_FROM)
    parser.add_argument("--holdout-to", default=HOLDOUT_TO)
    parser.add_argument("--out-summary", type=Path, default=DEFAULT_OUT_SUMMARY)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    parser.add_argument("--bad-zone-csv", type=Path, default=DEFAULT_OUT_BAD_ZONE)
    parser.add_argument("--prepared-feature-root", type=Path, default=DEFAULT_PREPARED_FEATURE_ROOT)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
