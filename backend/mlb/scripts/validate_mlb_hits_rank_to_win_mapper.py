#!/usr/bin/env python3
"""Validate a frozen rank-percentile-to-win mapper for the hits residual ranker.

The mapper is built from 2024-2025 training rows only, then applied to the
already-ranked 2026 validation rows. This is validation/reporting only; it does
not alter production models or current probability outputs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


DEFAULT_TRAIN_AUDIT = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_MODEL = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
DEFAULT_FEATURES = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_features.json")
DEFAULT_VALIDATION_RANKED = Path("backend/mlb/exports/model_v2/ranking/validation/hits_residual_ranked_2026.csv")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/validation/hits_rank_mapper_validation.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/ranking/validation/hits_rank_mapper_summary.json")
MIN_SAMPLE_SIZE = 50


def _load_feature_columns(features_path: Path, artifact: Any) -> list[str]:
    payload = json.loads(features_path.read_text(encoding="utf-8"))
    feature_cols = list(payload.get("feature_columns") or [])
    if not feature_cols and isinstance(artifact, dict):
        feature_cols = list(artifact.get("feature_columns") or [])
    if not feature_cols:
        raise SystemExit("No feature columns found in feature artifact.")
    return feature_cols


def _rank_bucket(rank_percentile: pd.Series) -> pd.Series:
    pct = pd.to_numeric(rank_percentile, errors="coerce")
    bucket = np.ceil(pct * 10.0)
    bucket = np.clip(bucket, 1, 10)
    return pd.Series(bucket, index=rank_percentile.index).astype("Int64")


def _normalize_line(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return None
    return float(round(float(val), 3))


def _actual_win(actual_value: Any, line: Any, side: str) -> bool | None:
    actual = pd.to_numeric(pd.Series([actual_value]), errors="coerce").iloc[0]
    point = pd.to_numeric(pd.Series([line]), errors="coerce").iloc[0]
    if pd.isna(actual) or pd.isna(point) or actual == point:
        return None
    if side == "over":
        return bool(actual > point)
    return bool(actual < point)


def _profit_from_price(price: Any, actual_win: Any) -> float | None:
    win = actual_win
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


def _score_train_rows(train: pd.DataFrame, model: Any, feature_cols: list[str]) -> pd.DataFrame:
    work = train.copy()
    work["game_date"] = pd.to_datetime(work["game_date"], errors="coerce").dt.date.astype(str)
    work["prop_type_norm"] = work["prop_type"].astype(str).str.strip().str.lower()
    work["line"] = pd.to_numeric(work["line"], errors="coerce")
    work["actual_value"] = pd.to_numeric(work["actual_value"], errors="coerce")
    work["residual"] = pd.to_numeric(work["residual"], errors="coerce")
    if "joined_to_player_derived_stats" in work.columns:
        joined = work["joined_to_player_derived_stats"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    else:
        joined = pd.Series(True, index=work.index)
    work = work[
        work["prop_type_norm"].eq("hits")
        & work["actual_value"].notna()
        & work["line"].notna()
        & work["residual"].notna()
        & joined
    ].copy()
    for col in feature_cols:
        if col not in work.columns:
            work[col] = np.nan
    X = work[feature_cols].apply(pd.to_numeric, errors="coerce")
    work["predicted_residual"] = model.predict(X)
    frames: list[pd.DataFrame] = []
    for side in ("over", "under"):
        side_df = work.copy()
        side_df["side"] = side
        side_df["rank_score"] = side_df["predicted_residual"] if side == "over" else -side_df["predicted_residual"]
        side_df["actual_win"] = [_actual_win(a, l, side) for a, l in zip(side_df["actual_value"], side_df["line"])]
        frames.append(side_df)
    side_rows = pd.concat(frames, ignore_index=True)
    side_rows = side_rows[side_rows["actual_win"].notna()].copy()
    side_rows["line_bucket"] = side_rows["line"].map(_normalize_line)
    side_rows = side_rows.sort_values(["game_date", "prop_type_norm", "side", "rank_score"], ascending=[True, True, True, False])
    group_cols = ["game_date", "prop_type_norm", "side"]
    side_rows["rank_percentile"] = side_rows.groupby(group_cols)["rank_score"].rank(
        method="average", pct=True, ascending=True
    )
    side_rows["rank_bucket"] = _rank_bucket(side_rows["rank_percentile"])
    return side_rows


def _build_mapper(train_side_rows: pd.DataFrame) -> pd.DataFrame:
    mapper = (
        train_side_rows.groupby(["prop_type_norm", "side", "line_bucket", "rank_bucket"], dropna=False)
        .agg(
            empirical_win_rate=("actual_win", "mean"),
            sample_size=("actual_win", "size"),
            train_avg_rank_score=("rank_score", "mean"),
            train_avg_actual_residual=("residual", "mean"),
        )
        .reset_index()
        .rename(columns={"prop_type_norm": "prop_type"})
    )
    return mapper


def _load_reconcile_pnl_for_validation(validation: pd.DataFrame) -> pd.DataFrame:
    if "source_reconcile_file" not in validation.columns:
        return validation
    source_files = sorted(str(p) for p in validation["source_reconcile_file"].dropna().unique())
    frames: list[pd.DataFrame] = []
    needed = {
        "game_date",
        "player_id",
        "game_id",
        "prop_type",
        "line",
        "pnl_over_1u",
        "pnl_under_1u",
    }
    for source in source_files:
        path = Path(source)
        if not path.exists():
            continue
        cols = pd.read_csv(path, nrows=0).columns
        usecols = [c for c in needed if c in cols]
        if not {"game_date", "player_id", "game_id", "prop_type", "line"}.issubset(usecols):
            continue
        df = pd.read_csv(path, usecols=usecols, low_memory=False)
        if "pnl_over_1u" not in df.columns:
            df["pnl_over_1u"] = np.nan
        if "pnl_under_1u" not in df.columns:
            df["pnl_under_1u"] = np.nan
        frames.append(df)
    if not frames:
        return validation
    pnl = pd.concat(frames, ignore_index=True)
    for df in (validation, pnl):
        df["game_date_key"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date.astype(str)
        df["player_id_key"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df["game_id_key"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
        df["line_key"] = pd.to_numeric(df["line"], errors="coerce").round(3)
        df["prop_type_key"] = df["prop_type"].astype(str).str.strip().str.lower()
    pnl = pnl.drop_duplicates(["game_date_key", "player_id_key", "game_id_key", "prop_type_key", "line_key"])
    return validation.merge(
        pnl[
            [
                "game_date_key",
                "player_id_key",
                "game_id_key",
                "prop_type_key",
                "line_key",
                "pnl_over_1u",
                "pnl_under_1u",
            ]
        ],
        on=["game_date_key", "player_id_key", "game_id_key", "prop_type_key", "line_key"],
        how="left",
    )


def _prepare_validation(validation_path: Path) -> pd.DataFrame:
    if not validation_path.exists():
        raise SystemExit(f"Missing validation ranked CSV: {validation_path}")
    val = pd.read_csv(validation_path, low_memory=False)
    required = {"game_date", "player_id", "game_id", "prop_type", "side", "line", "rank_percentile", "actual_win"}
    missing = sorted(required - set(val.columns))
    if missing:
        raise SystemExit(f"{validation_path} missing required columns: {missing}")
    val["prop_type"] = val["prop_type"].astype(str).str.strip().str.lower()
    val["side"] = val["side"].astype(str).str.strip().str.lower()
    val["line"] = pd.to_numeric(val["line"], errors="coerce")
    val["line_bucket"] = val["line"].map(_normalize_line)
    val["rank_bucket"] = _rank_bucket(val["rank_percentile"])
    val["actual_win"] = val["actual_win"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "win"})
    val = _load_reconcile_pnl_for_validation(val)
    val["pnl_side_1u"] = np.where(
        val["side"].eq("over"),
        pd.to_numeric(val.get("pnl_over_1u"), errors="coerce"),
        pd.to_numeric(val.get("pnl_under_1u"), errors="coerce"),
    )
    if val["pnl_side_1u"].isna().any():
        fallback = [
            _profit_from_price(price, win)
            for price, win in zip(val.get("price", pd.Series(np.nan, index=val.index)), val["actual_win"])
        ]
        fallback = pd.Series(fallback, index=val.index)
        val["pnl_side_1u"] = val["pnl_side_1u"].where(val["pnl_side_1u"].notna(), fallback)
    return val


def _metric_row(df: pd.DataFrame, label: str, group: str = "overall") -> dict[str, Any]:
    bets = int(len(df))
    wins = int(df["actual_win"].sum()) if bets else 0
    profit = float(pd.to_numeric(df["pnl_side_1u"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    return {
        "group": group,
        "value": label,
        "bets": bets,
        "wins": wins,
        "win_rate": float(wins / bets) if bets else None,
        "profit_units": profit,
        "roi": float(profit / bets) if bets else None,
    }


def _group_metrics(df: pd.DataFrame, column: str, group: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value, part in df.groupby(column, dropna=False):
        rows.append(_metric_row(part, str(value), group))
    return rows


def run(args: argparse.Namespace) -> dict[str, Any]:
    train_path = Path(args.training_audit_csv)
    model_path = Path(args.model)
    features_path = Path(args.features)
    validation_path = Path(args.validation_ranked_csv)
    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    for path in (train_path, model_path, features_path, validation_path):
        if not path.exists():
            raise SystemExit(f"Missing input: {path}")

    artifact = joblib.load(model_path)
    model = artifact["model"] if isinstance(artifact, dict) and "model" in artifact else artifact
    feature_cols = _load_feature_columns(features_path, artifact)

    train = pd.read_csv(train_path, low_memory=False)
    train_side_rows = _score_train_rows(train, model, feature_cols)
    mapper = _build_mapper(train_side_rows)

    validation = _prepare_validation(validation_path)
    mapped = validation.merge(
        mapper,
        on=["prop_type", "side", "line_bucket", "rank_bucket"],
        how="left",
    )
    mapped["mapped"] = mapped["empirical_win_rate"].notna()
    mapped["passes_sample_size"] = pd.to_numeric(mapped["sample_size"], errors="coerce").ge(args.min_sample_size)
    evaluated = mapped[mapped["mapped"] & mapped["passes_sample_size"]].copy()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    mapped.to_csv(out_csv, index=False)

    metrics = [_metric_row(evaluated, "all")]
    metrics.extend(_group_metrics(evaluated, "side", "by_side"))
    metrics.extend(_group_metrics(evaluated, "line_bucket", "by_line"))
    metrics.extend(_group_metrics(evaluated, "rank_bucket", "by_rank_bucket"))
    hits_05_under = evaluated[
        evaluated["prop_type"].eq("hits") & evaluated["side"].eq("under") & evaluated["line_bucket"].eq(0.5)
    ]
    metrics.append(_metric_row(hits_05_under, "hits_0.5_under", "focus"))

    summary = {
        "training_audit_csv": str(train_path),
        "model": str(model_path),
        "features": str(features_path),
        "validation_ranked_csv": str(validation_path),
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "min_sample_size": int(args.min_sample_size),
        "train_side_rows": int(len(train_side_rows)),
        "mapper_rows": int(len(mapper)),
        "validation_side_rows": int(len(validation)),
        "rows_mapped_before_sample_filter": int(mapped["mapped"].sum()),
        "rows_mapped_after_sample_filter": int(len(evaluated)),
        "unmapped_rows": int((~mapped["mapped"]).sum()),
        "low_sample_mapped_rows": int((mapped["mapped"] & ~mapped["passes_sample_size"]).sum()),
        "pnl_source": "pnl_over_1u/pnl_under_1u when recoverable from source_reconcile_file; price/outcome fallback otherwise",
        "metrics": metrics,
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate frozen hits rank-to-win empirical mapper.")
    parser.add_argument("--training-audit-csv", default=str(DEFAULT_TRAIN_AUDIT))
    parser.add_argument("--model", default=str(DEFAULT_MODEL))
    parser.add_argument("--features", default=str(DEFAULT_FEATURES))
    parser.add_argument("--validation-ranked-csv", default=str(DEFAULT_VALIDATION_RANKED))
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--min-sample-size", type=int, default=MIN_SAMPLE_SIZE)
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    overall = next((m for m in summary["metrics"] if m["group"] == "overall"), {})
    print(
        "mapped={rows_mapped_after_sample_filter}/{validation_side_rows} "
        "win_rate={win_rate:.4f} roi={roi:.4f}".format(
            win_rate=overall.get("win_rate") or 0.0,
            roi=overall.get("roi") or 0.0,
            **summary,
        )
    )


if __name__ == "__main__":
    main()
