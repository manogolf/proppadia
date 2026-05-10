#!/usr/bin/env python3
"""Score 2026 hits validation rows with the v0 residual ranker.

This script is validation-only. It reads outcome-backed reconcile rows, joins
mlb.player_derived_stats, scores the residual ranker, and emits side-specific
rank rows for later evaluation/upload mapping experiments.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_MODEL = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
DEFAULT_FEATURES = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_features.json")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/validation/hits_residual_ranked_2026.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/ranking/validation/hits_residual_ranked_2026_summary.json")
DEFAULT_FROM_DATE = "2026-04-09"
DEFAULT_TO_DATE = "2026-05-08"


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


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        parsed = pd.to_datetime(date, errors="coerce")
        if pd.isna(parsed):
            continue
        if date < from_date or date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_reconcile(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    required = {"game_date", "player_id", "game_id", "player_name", "prop_type", "line", "actual_value"}
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-ranker-validation] skip {path}: missing {missing}")
            continue
        df["source_reconcile_file"] = str(path)
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible reconcile_rows.csv files found.")
    out = pd.concat(frames, ignore_index=True)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce").dt.date.astype(str)
    return out


def _load_player_derived_stats(engine, game_ids: list[int], chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with engine.connect() as conn:
        for chunk in _chunks(game_ids, chunk_size):
            part = pd.read_sql(
                text(
                    """
                    SELECT *
                    FROM mlb.player_derived_stats
                    WHERE game_id = ANY(:game_ids)
                    """
                ),
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


def _outcome_to_win(value: Any, actual_value: Any, line: Any, side: str) -> bool | None:
    text_value = str(value if value is not None else "").strip().lower()
    if text_value in {"win", "won", "true", "1"}:
        return True
    if text_value in {"loss", "lost", "false", "0"}:
        return False
    actual = pd.to_numeric(pd.Series([actual_value]), errors="coerce").iloc[0]
    point = pd.to_numeric(pd.Series([line]), errors="coerce").iloc[0]
    if pd.isna(actual) or pd.isna(point):
        return None
    if actual == point:
        return None
    if side == "over":
        return bool(actual > point)
    return bool(actual < point)


def _line_distribution(df: pd.DataFrame) -> list[dict[str, Any]]:
    counts = (
        pd.to_numeric(df["line"], errors="coerce")
        .round(3)
        .value_counts(dropna=False)
        .sort_index()
        .reset_index()
    )
    counts.columns = ["line", "rows"]
    total = max(1, int(len(df)))
    counts["pct"] = counts["rows"].map(lambda v: float(v) / total)
    return counts.to_dict(orient="records")


def _spearman(pred: pd.Series, actual: pd.Series) -> float | None:
    corr = pd.to_numeric(pred, errors="coerce").corr(pd.to_numeric(actual, errors="coerce"), method="spearman")
    if pd.isna(corr):
        return None
    return float(corr)


def _decile_table(side_rows: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side, group in side_rows.groupby("side", dropna=False):
        work = group.copy()
        work["rank_percentile"] = pd.to_numeric(work["rank_percentile"], errors="coerce")
        work["actual_win"] = work["actual_win"].astype("boolean")
        work = work[work["rank_percentile"].notna() & work["actual_win"].notna()].copy()
        if work.empty:
            continue
        work["rank_percentile_decile"] = pd.qcut(
            work["rank_percentile"].rank(method="first"),
            q=min(10, len(work)),
            labels=False,
            duplicates="drop",
        ) + 1
        summary = (
            work.groupby("rank_percentile_decile")
            .agg(
                rows=("actual_win", "size"),
                avg_rank_percentile=("rank_percentile", "mean"),
                avg_rank_score=("rank_score", "mean"),
                actual_win_rate=("actual_win", "mean"),
                avg_actual_residual=("actual_residual", "mean"),
            )
            .reset_index()
            .sort_values("rank_percentile_decile")
        )
        summary.insert(0, "side", side)
        rows.extend(summary.to_dict(orient="records"))
    return rows


def run(args: argparse.Namespace) -> dict[str, Any]:
    model_path = Path(args.model)
    features_path = Path(args.features)
    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    if not model_path.exists():
        raise SystemExit(f"Missing model artifact: {model_path}")
    if not features_path.exists():
        raise SystemExit(f"Missing feature JSON: {features_path}")

    artifact = joblib.load(model_path)
    model = artifact["model"] if isinstance(artifact, dict) and "model" in artifact else artifact
    feature_payload = json.loads(features_path.read_text(encoding="utf-8"))
    feature_cols = list(feature_payload.get("feature_columns") or artifact.get("feature_columns") or [])
    if not feature_cols:
        raise SystemExit("No feature columns found in features artifact.")

    reconcile_files = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_reconcile(reconcile_files)
    hits_all = reconcile[reconcile["prop_type_norm"].eq("hits")].copy()
    hits = hits_all[hits_all["actual_value"].notna() & hits_all["line"].notna()].copy()
    hits["player_id_key"] = pd.to_numeric(hits["player_id"], errors="coerce").astype("Int64")
    hits["game_id_key"] = pd.to_numeric(hits["game_id"], errors="coerce").astype("Int64")
    hits["actual_residual"] = hits["actual_value"] - hits["line"]

    game_ids = sorted({int(v) for v in hits.loc[hits["game_id_key"].notna(), "game_id_key"].tolist()})
    engine = create_engine(_db_url())
    pds = _load_player_derived_stats(engine, game_ids, args.chunk_size)
    pds = pds.rename(columns={c: f"pds_{c}" for c in pds.columns if c not in {"player_id", "game_id"}})
    pds["player_id_key"] = pd.to_numeric(pds["player_id"], errors="coerce").astype("Int64")
    pds["game_id_key"] = pd.to_numeric(pds["game_id"], errors="coerce").astype("Int64")
    pds = pds.drop(columns=["player_id", "game_id"], errors="ignore")

    joined = hits.merge(pds, on=["player_id_key", "game_id_key"], how="left", indicator="pds_join_status")
    joined["joined_to_player_derived_stats"] = joined["pds_join_status"].eq("both")
    joined = joined.drop(columns=["pds_join_status"])

    scored = joined[joined["joined_to_player_derived_stats"]].copy()
    for col in feature_cols:
        if col not in scored.columns:
            scored[col] = np.nan
    X = scored[feature_cols].apply(pd.to_numeric, errors="coerce")
    scored["predicted_residual"] = model.predict(X)

    side_frames: list[pd.DataFrame] = []
    for side in ("over", "under"):
        side_df = scored.copy()
        side_df["side"] = side
        side_df["rank_score"] = side_df["predicted_residual"] if side == "over" else -side_df["predicted_residual"]
        outcome_col = f"actual_{side}_outcome"
        price_col = f"price_{side}_american"
        side_df["actual_outcome"] = side_df[outcome_col] if outcome_col in side_df.columns else None
        side_df["price"] = side_df[price_col] if price_col in side_df.columns else None
        side_df["actual_win"] = [
            _outcome_to_win(outcome, actual, line, side)
            for outcome, actual, line in zip(side_df["actual_outcome"], side_df["actual_value"], side_df["line"])
        ]
        side_frames.append(side_df)
    side_rows = pd.concat(side_frames, ignore_index=True)
    side_rows = side_rows.sort_values(["game_date", "prop_type", "side", "rank_score"], ascending=[True, True, True, False])
    group_cols = ["game_date", "prop_type", "side"]
    side_rows["rank_position"] = side_rows.groupby(group_cols)["rank_score"].rank(method="first", ascending=False).astype(int)
    side_rows["rank_percentile"] = side_rows.groupby(group_cols)["rank_score"].rank(method="average", pct=True, ascending=True)

    out_cols = [
        "game_date",
        "player_name",
        "player_id",
        "game_id",
        "prop_type",
        "side",
        "line",
        "price",
        "actual_value",
        "actual_residual",
        "actual_outcome",
        "actual_win",
        "predicted_residual",
        "rank_score",
        "rank_position",
        "rank_percentile",
        "price_over_american",
        "price_under_american",
        "actual_over_outcome",
        "actual_under_outcome",
        "source_reconcile_file",
    ]
    out_cols = [c for c in out_cols if c in side_rows.columns]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    side_rows[out_cols].to_csv(out_csv, index=False)

    rows_with_keys = int((hits["player_id_key"].notna() & hits["game_id_key"].notna()).sum())
    joined_rows = int(joined["joined_to_player_derived_stats"].sum())
    summary = {
        "model": str(model_path),
        "features": str(features_path),
        "reconcile_root": str(args.reconcile_root),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "validation_rows_loaded": int(len(hits)),
        "rows_with_player_id_game_id": rows_with_keys,
        "joined_rows": joined_rows,
        "join_coverage_pct": float(joined_rows / rows_with_keys) if rows_with_keys else None,
        "scored_rows": int(len(scored)),
        "side_rows": int(len(side_rows)),
        "date_min": str(pd.to_datetime(hits["game_date"], errors="coerce").min().date()) if not hits.empty else None,
        "date_max": str(pd.to_datetime(hits["game_date"], errors="coerce").max().date()) if not hits.empty else None,
        "line_distribution": _line_distribution(hits),
        "spearman_predicted_residual_vs_actual_residual": _spearman(
            scored["predicted_residual"], scored["actual_residual"]
        ),
        "decile_table_by_side": _decile_table(side_rows),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score hits residual ranker on 2026 validation rows.")
    parser.add_argument("--model", default=str(DEFAULT_MODEL))
    parser.add_argument("--features", default=str(DEFAULT_FEATURES))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--to-date", default=DEFAULT_TO_DATE)
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--chunk-size", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "validation_rows={validation_rows_loaded} joined={joined_rows} scored={scored_rows} "
        "coverage={join_coverage_pct:.2%} spearman={spearman_predicted_residual_vs_actual_residual:.4f}".format(
            **summary
        )
    )


if __name__ == "__main__":
    main()
