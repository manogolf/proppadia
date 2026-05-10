#!/usr/bin/env python3
"""Audit a direct hitless-outcome target for hits 0.5 UNDER.

This trains a temporary validation-only classifier for under_win
(actual_value == 0) to test whether hitless outcomes can be ranked directly.
It does not create a final model artifact or alter production logic.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text


DEFAULT_TRAIN_AUDIT = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_RESIDUAL_MAPPER = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_mapper_audit.csv")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_direct_target_audit.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_direct_target_summary.json")
DEFAULT_FROM_DATE = "2026-04-09"
DEFAULT_TO_DATE = "2026-05-08"

EXCLUDE_EXACT = {
    "actual_value",
    "residual",
    "under_win",
    "player_id",
    "game_id",
    "player_id_key",
    "game_id_key",
    "game_date",
    "date",
    "player_name",
    "prop_type",
    "prop_type_norm",
    "side",
    "source_reconcile_file",
    "joined_to_player_derived_stats",
}
EXCLUDE_SUBSTRINGS = (
    "outcome",
    "pnl",
    "profit",
    "odds",
    "price",
    "model_prob",
    "implied",
    "fair",
    "bookmaker",
    "market",
)


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _feature_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        low = col.strip().lower()
        if low in EXCLUDE_EXACT:
            continue
        if any(part in low for part in EXCLUDE_SUBSTRINGS):
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if series.notna().any():
            cols.append(col)
    return ["line"] + sorted(c for c in cols if c != "line")


def _make_model(model_type: str, random_state: int) -> Pipeline:
    if model_type == "random_forest":
        clf = RandomForestClassifier(
            n_estimators=300,
            min_samples_leaf=20,
            random_state=random_state,
            n_jobs=-1,
        )
    else:
        clf = HistGradientBoostingClassifier(
            learning_rate=0.06,
            max_iter=250,
            min_samples_leaf=30,
            l2_regularization=0.01,
            random_state=random_state,
        )
    return Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", clf)])


def _load_train(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Missing training audit CSV: {path}")
    df = pd.read_csv(path, low_memory=False)
    required = {"game_date", "prop_type", "line", "actual_value"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{path} missing required columns: {missing}")
    out = df.copy()
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    if "joined_to_player_derived_stats" in out.columns:
        joined = out["joined_to_player_derived_stats"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    else:
        joined = pd.Series(True, index=out.index)
    out = out[
        out["prop_type_norm"].eq("hits")
        & out["line"].eq(0.5)
        & out["actual_value"].notna()
        & joined
    ].copy()
    out["under_win"] = out["actual_value"].eq(0).astype(int)
    return out


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        parsed = pd.to_datetime(date, errors="coerce")
        if pd.isna(parsed):
            continue
        if from_date <= date <= to_date:
            files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_validation_reconcile(root: Path, from_date: str, to_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    required = {
        "game_date",
        "player_id",
        "game_id",
        "player_name",
        "prop_type",
        "line",
        "actual_value",
        "price_under_american",
        "price_over_american",
    }
    for path in _discover_reconcile_files(root, from_date, to_date):
        cols = pd.read_csv(path, nrows=0).columns
        missing = sorted(required - set(cols))
        if missing:
            print(f"[hits-05-under-direct] skip {path}: missing {missing}")
            continue
        keep = list(dict.fromkeys(list(required) + ["pnl_under_1u", "actual_under_outcome"]))
        keep = [c for c in keep if c in cols]
        df = pd.read_csv(path, usecols=keep, low_memory=False)
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible validation reconcile rows found.")
    out = pd.concat(frames, ignore_index=True)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out = out[out["prop_type_norm"].eq("hits") & out["line"].eq(0.5) & out["actual_value"].notna()].copy()
    out["under_win"] = out["actual_value"].eq(0).astype(int)
    return out


def _load_player_derived_stats(engine, game_ids: list[int], chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with engine.connect() as conn:
        for chunk in _chunks(game_ids, chunk_size):
            part = pd.read_sql(
                text("SELECT * FROM mlb.player_derived_stats WHERE game_id = ANY(:game_ids)"),
                conn,
                params={"game_ids": chunk},
            )
            if not part.empty:
                frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["player_id", "game_id"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    return out.drop_duplicates(["player_id", "game_id"], keep="last")


def _join_validation_features(validation: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
    work = validation.copy()
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    game_ids = sorted({int(v) for v in work.loc[work["game_id_key"].notna(), "game_id_key"].tolist()})
    pds = _load_player_derived_stats(create_engine(_db_url()), game_ids, chunk_size)
    pds = pds.rename(columns={c: f"pds_{c}" for c in pds.columns if c not in {"player_id", "game_id"}})
    pds["player_id_key"] = pd.to_numeric(pds["player_id"], errors="coerce").astype("Int64")
    pds["game_id_key"] = pd.to_numeric(pds["game_id"], errors="coerce").astype("Int64")
    pds = pds.drop(columns=["player_id", "game_id"], errors="ignore")
    joined = work.merge(pds, on=["player_id_key", "game_id_key"], how="left", indicator="pds_join_status")
    joined["joined_to_player_derived_stats"] = joined["pds_join_status"].eq("both")
    return joined.drop(columns=["pds_join_status"])


def _profit_from_price(price: Any, win: Any) -> float | None:
    if pd.isna(win):
        return None
    price_num = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(price_num):
        return None
    if not bool(win):
        return -1.0
    if price_num > 0:
        return float(price_num / 100.0)
    if price_num < 0:
        return float(100.0 / abs(price_num))
    return None


def _score_probability(model: Pipeline, X: pd.DataFrame) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        return model.predict_proba(X)[:, 1]
    return model.decision_function(X)


def _rank_deciles(scored: pd.DataFrame) -> pd.DataFrame:
    out = scored.copy()
    out["rank_score"] = out["under_win_score"]
    out = out.sort_values(["game_date", "rank_score"], ascending=[True, False])
    out["rank_position"] = out.groupby("game_date")["rank_score"].rank(method="first", ascending=False).astype(int)
    out["rank_percentile"] = out.groupby("game_date")["rank_score"].rank(method="average", pct=True, ascending=True)
    ranked = out["rank_percentile"].rank(method="first")
    out["rank_bucket"] = pd.qcut(ranked, q=min(10, len(out)), labels=False, duplicates="drop") + 1
    out["rank_bucket"] = out["rank_bucket"].astype(int)
    return out


def _bucket_metrics(scored: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket, group in scored.groupby("rank_bucket"):
        bets = int(len(group))
        wins = int(group["under_win"].sum())
        profit = float(pd.to_numeric(group["pnl_under_1u"], errors="coerce").fillna(0.0).sum())
        rows.append(
            {
                "rank_bucket": int(bucket),
                "validation_sample_size": bets,
                "validation_win_rate": float(wins / bets) if bets else None,
                "validation_roi": float(profit / bets) if bets else None,
                "validation_profit_units": profit,
                "avg_under_win_score": float(group["under_win_score"].mean(skipna=True)),
                "avg_actual_value": float(group["actual_value"].mean(skipna=True)),
                "avg_price_under_american": float(pd.to_numeric(group["price_under_american"], errors="coerce").mean(skipna=True)),
                "avg_price_over_american": float(pd.to_numeric(group["price_over_american"], errors="coerce").mean(skipna=True)),
            }
        )
    return rows


def _spearman(frame: pd.DataFrame, a: str, b: str) -> float | None:
    val = pd.to_numeric(frame[a], errors="coerce").corr(pd.to_numeric(frame[b], errors="coerce"), method="spearman")
    if pd.isna(val):
        return None
    return float(val)


def _feature_importances(model: Pipeline, X: pd.DataFrame, y: pd.Series, feature_cols: list[str], sample_rows: int, random_state: int) -> list[dict[str, Any]]:
    clf = model.named_steps["model"]
    raw = getattr(clf, "feature_importances_", None)
    if raw is not None:
        pairs = sorted(zip(feature_cols, raw), key=lambda item: float(item[1]), reverse=True)
        return [{"feature": f, "importance": float(v), "method": "native"} for f, v in pairs[:20]]
    if sample_rows <= 0:
        return []
    sample = X.sample(n=min(sample_rows, len(X)), random_state=random_state)
    target = y.loc[sample.index]
    result = permutation_importance(
        model,
        sample,
        target,
        n_repeats=3,
        random_state=random_state,
        scoring="roc_auc",
        n_jobs=1,
    )
    pairs = sorted(zip(feature_cols, result.importances_mean), key=lambda item: float(item[1]), reverse=True)
    return [{"feature": f, "importance": float(v), "method": "permutation_auc"} for f, v in pairs[:20]]


def _load_residual_comparison(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"available": False, "reason": f"missing {path}"}
    df = pd.read_csv(path)
    return {
        "available": True,
        "source": str(path),
        "buckets": df.to_dict(orient="records"),
        "top_bucket": df.sort_values("rank_bucket").tail(1).to_dict(orient="records")[0] if not df.empty else None,
        "bottom_bucket": df.sort_values("rank_bucket").head(1).to_dict(orient="records")[0] if not df.empty else None,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    train = _load_train(Path(args.training_audit_csv))
    validation = _join_validation_features(
        _load_validation_reconcile(Path(args.reconcile_root), args.from_date, args.to_date),
        args.chunk_size,
    )
    validation = validation[validation["joined_to_player_derived_stats"]].copy()

    feature_cols = _feature_columns(train)
    for col in feature_cols:
        if col not in validation.columns:
            validation[col] = np.nan
    X_train = train[feature_cols].apply(pd.to_numeric, errors="coerce")
    y_train = train["under_win"].astype(int)
    X_val = validation[feature_cols].apply(pd.to_numeric, errors="coerce")
    y_val = validation["under_win"].astype(int)

    model = _make_model(args.model_type, args.random_state)
    model.fit(X_train, y_train)
    validation["under_win_score"] = _score_probability(model, X_val)
    validation["pnl_under_1u"] = pd.to_numeric(validation.get("pnl_under_1u"), errors="coerce")
    if validation["pnl_under_1u"].isna().any():
        fallback = pd.Series(
            [_profit_from_price(p, w) for p, w in zip(validation["price_under_american"], validation["under_win"])],
            index=validation.index,
        )
        validation["pnl_under_1u"] = validation["pnl_under_1u"].where(validation["pnl_under_1u"].notna(), fallback)

    scored = _rank_deciles(validation)
    bucket_rows = _bucket_metrics(scored)
    auc = float(roc_auc_score(y_val, scored["under_win_score"])) if y_val.nunique() == 2 else None
    rank_corr = _spearman(scored, "rank_bucket", "under_win")
    top = max(bucket_rows, key=lambda r: r["rank_bucket"]) if bucket_rows else {}
    bottom = min(bucket_rows, key=lambda r: r["rank_bucket"]) if bucket_rows else {}
    overall_profit = float(scored["pnl_under_1u"].fillna(0.0).sum())
    overall_bets = int(len(scored))
    overall_wins = int(scored["under_win"].sum())

    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    out_cols = [
        "game_date",
        "player_name",
        "player_id",
        "game_id",
        "prop_type",
        "line",
        "actual_value",
        "under_win",
        "price_under_american",
        "price_over_american",
        "pnl_under_1u",
        "under_win_score",
        "rank_position",
        "rank_percentile",
        "rank_bucket",
        "source_reconcile_file",
    ]
    scored[[c for c in out_cols if c in scored.columns]].to_csv(out_csv, index=False)

    summary = {
        "training_audit_csv": str(args.training_audit_csv),
        "reconcile_root": str(args.reconcile_root),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "model_type": args.model_type,
        "train_rows": int(len(train)),
        "validation_rows": int(len(scored)),
        "feature_count": int(len(feature_cols)),
        "validation_auc": auc,
        "rank_bucket_vs_actual_under_win_spearman": rank_corr,
        "top_decile_win_rate": top.get("validation_win_rate"),
        "bottom_decile_win_rate": bottom.get("validation_win_rate"),
        "overall_win_rate": float(overall_wins / overall_bets) if overall_bets else None,
        "overall_roi": float(overall_profit / overall_bets) if overall_bets else None,
        "bucket_rows": bucket_rows,
        "top_20_feature_importances": _feature_importances(
            model,
            X_train,
            y_train,
            feature_cols,
            args.importance_sample_rows,
            args.random_state,
        ),
        "residual_sign_flip_comparison": _load_residual_comparison(Path(args.residual_mapper_audit_csv)),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit direct-target hits 0.5 UNDER ranking.")
    parser.add_argument("--training-audit-csv", default=str(DEFAULT_TRAIN_AUDIT))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--to-date", default=DEFAULT_TO_DATE)
    parser.add_argument("--residual-mapper-audit-csv", default=str(DEFAULT_RESIDUAL_MAPPER))
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--model-type", choices=["hist_gradient_boosting", "random_forest"], default="hist_gradient_boosting")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--importance-sample-rows", type=int, default=10000)
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "validation_rows={validation_rows} auc={validation_auc:.4f} top_decile_wr={top_decile_win_rate:.4f} "
        "bottom_decile_wr={bottom_decile_win_rate:.4f} rank_corr={rank_bucket_vs_actual_under_win_spearman:.4f}".format(
            **summary
        )
    )


if __name__ == "__main__":
    main()
