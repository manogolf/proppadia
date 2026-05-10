#!/usr/bin/env python3
"""Audit historical hits features for a residual-ranker dataset.

This script does not train a model. It joins historical hits reconcile rows to
mlb.player_derived_stats and reports whether that join is viable as the base
feature dataset for a future residual ranking model.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_RECONCILE_CSVS = [
    Path("tmp/mlb_reconcile_rows_historical_bestbook_2024.csv"),
    Path("tmp/mlb_reconcile_rows_historical_bestbook_2025.csv"),
]
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_SUMMARY_JSON = Path(
    "backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit_summary.json"
)
PDS_KEY_COLUMNS = {"id", "player_id", "game_id", "game_date", "team", "is_home", "created_at", "updated_at"}


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


def _read_reconcile(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    required = {"game_date", "player_id", "game_id", "player_name", "prop_type", "line", "actual_value"}
    for path in paths:
        if not path.exists():
            raise SystemExit(f"Missing reconcile CSV: {path}")
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            raise SystemExit(f"{path} missing required columns: {missing}")
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        raise SystemExit("No reconcile CSVs provided.")
    out = pd.concat(frames, ignore_index=True)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    return out


def _load_player_derived_stats(engine, game_ids: list[int], chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with engine.connect() as conn:
        for chunk in _chunks(game_ids, chunk_size):
            rows = pd.read_sql(
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
            if not rows.empty:
                frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["player_id", "game_id"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    return out.drop_duplicates(["player_id", "game_id"], keep="last")


def _missing_rate(series: pd.Series) -> float | None:
    if series is None or len(series) == 0:
        return None
    return float(series.isna().mean())


def _describe_numeric(series: pd.Series) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {
            "count": 0,
            "mean": None,
            "std": None,
            "min": None,
            "p25": None,
            "median": None,
            "p75": None,
            "max": None,
        }
    return {
        "count": int(values.size),
        "mean": float(values.mean()),
        "std": float(values.std(ddof=0)),
        "min": float(values.min()),
        "p25": float(values.quantile(0.25)),
        "median": float(values.median()),
        "p75": float(values.quantile(0.75)),
        "max": float(values.max()),
    }


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


def _numeric_feature_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col in PDS_KEY_COLUMNS:
            continue
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().any():
            cols.append(col)
    return sorted(cols)


def run(args: argparse.Namespace) -> dict[str, Any]:
    reconcile_paths = [Path(p) for p in args.reconcile_csv]
    raw = _read_reconcile(reconcile_paths)
    hits_all = raw[raw["prop_type_norm"].eq("hits")].copy()
    hits = hits_all[hits_all["actual_value"].notna() & hits_all["line"].notna()].copy()
    hits["player_id_key"] = pd.to_numeric(hits["player_id"], errors="coerce").astype("Int64")
    hits["game_id_key"] = pd.to_numeric(hits["game_id"], errors="coerce").astype("Int64")
    hits["has_join_keys"] = hits["player_id_key"].notna() & hits["game_id_key"].notna()
    hits["residual"] = hits["actual_value"] - hits["line"]

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

    pds_feature_cols_prefixed = [
        c
        for c in joined.columns
        if c.startswith("pds_") and c not in {"pds_id", "pds_game_date", "pds_team", "pds_is_home", "pds_created_at", "pds_updated_at"}
    ]
    numeric_feature_cols = _numeric_feature_columns(joined[pds_feature_cols_prefixed])

    out_cols = [
        "game_date",
        "player_id",
        "game_id",
        "player_name",
        "prop_type",
        "line",
        "actual_value",
        "residual",
        "joined_to_player_derived_stats",
        "source_reconcile_file",
    ] + numeric_feature_cols
    out_cols = [c for c in out_cols if c in joined.columns]

    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    joined[out_cols].to_csv(out_csv, index=False)

    rows_with_keys = int(hits["has_join_keys"].sum())
    joined_rows = int(joined["joined_to_player_derived_stats"].sum())
    feature_missing = {}
    for col in ("pds_d7_hits", "pds_d15_hits", "pds_d30_hits"):
        feature_missing[col.removeprefix("pds_")] = {
            "column_present": col in joined.columns,
            "missing_rate_all_rows": _missing_rate(joined[col]) if col in joined.columns else None,
            "missing_rate_joined_rows": (
                _missing_rate(joined.loc[joined["joined_to_player_derived_stats"], col])
                if col in joined.columns
                else None
            ),
        }

    sample_cols = [
        "game_date",
        "player_name",
        "player_id",
        "game_id",
        "line",
        "actual_value",
        "residual",
        "pds_d7_hits",
        "pds_d15_hits",
        "pds_d30_hits",
    ]
    sample_cols = [c for c in sample_cols if c in joined.columns]
    sample_rows = joined.loc[joined["joined_to_player_derived_stats"], sample_cols].head(args.sample_rows)

    summary = {
        "inputs": [str(p) for p in reconcile_paths],
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "total_hits_reconcile_rows": int(len(hits_all)),
        "filtered_hits_rows_actual_and_line": int(len(hits)),
        "rows_with_player_id_game_id": rows_with_keys,
        "rows_joined_to_player_derived_stats": joined_rows,
        "join_coverage_pct": float(joined_rows / rows_with_keys) if rows_with_keys else None,
        "date_min": str(pd.to_datetime(hits["game_date"], errors="coerce").min().date()) if not hits.empty else None,
        "date_max": str(pd.to_datetime(hits["game_date"], errors="coerce").max().date()) if not hits.empty else None,
        "line_distribution": _line_distribution(hits),
        "residual_distribution": _describe_numeric(hits["residual"]),
        "feature_missing_rates": feature_missing,
        "available_numeric_player_derived_feature_columns": numeric_feature_cols,
        "available_numeric_feature_count": int(len(numeric_feature_cols)),
        "sample_joined_rows": sample_rows.replace({np.nan: None}).to_dict(orient="records"),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit hits residual-ranker feature joins.")
    parser.add_argument(
        "--reconcile-csv",
        action="append",
        default=None,
        help="Historical reconcile CSV. Repeatable. Defaults to 2024 and 2025 historical bestbook files.",
    )
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--sample-rows", type=int, default=10)
    args = parser.parse_args()
    if args.reconcile_csv is None:
        args.reconcile_csv = [str(p) for p in DEFAULT_RECONCILE_CSVS]
    return args


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "hits_rows={filtered_hits_rows_actual_and_line} joined={rows_joined_to_player_derived_stats} "
        "coverage={join_coverage_pct:.2%} numeric_features={available_numeric_feature_count}".format(**summary)
    )


if __name__ == "__main__":
    main()
